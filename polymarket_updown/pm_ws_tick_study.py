"""BTC subscribed WS event recorder and per-message TWAP diagnostics. No orders."""
import asyncio
import dataclasses
from datetime import datetime, timezone
import gzip
import json
import math
import os
from pathlib import Path
import queue
import shutil
import threading
import time

ROOT=Path('.')
CONFIG=ROOT/'params/pm_ws_tick_study.json'
OUT=ROOT/'state/pm_edge_research'


def serial(value):
    if value is None or isinstance(value,(str,int,bool)):return value
    if isinstance(value,float):return value if math.isfinite(value) else None
    if isinstance(value,dict):return {str(k):serial(v) for k,v in value.items()}
    if isinstance(value,(list,tuple,set)):return [serial(v) for v in value]
    if dataclasses.is_dataclass(value):return serial(dataclasses.asdict(value))
    if hasattr(value,'model_dump'):return serial(value.model_dump())
    if hasattr(value,'__dict__'):return serial(vars(value))
    return str(value)


class TickStudy:
    def __init__(self,bot,module,config=CONFIG,out=OUT):
        self.bot,self.module,self.config,self.out=bot,module,Path(config),Path(out)
        self.out.mkdir(parents=True,exist_ok=True)
        self.q=queue.Queue(maxsize=50000)
        self.received=self.written=self.dropped=self.errors=0
        self.sequence=0;self.signal={};self.refresh_tasks={};self.last_refresh={}
        self.capture_sequence=0
        self.snapshots={};self.window_records=set();self.storage_stopped=False
        self.reference_received=0;self.snapshot_count=0;self.storage_checked_at=0
        self.reference_last_received=0.;self.clob_last_received=0.
        self.label_task=None;self.label_errors=0
        try:self.pending_labels=json.loads((self.out/'pending_labels.json').read_text())
        except (OSError,ValueError):self.pending_labels={}
        try:self.venue_labels=json.loads((self.out/'venue_labels.json').read_text())
        except (OSError,ValueError):self.venue_labels={}
        self.policy={};self.config_at=0;self.signaled=set();self.closed=False
        self.segment=f'{os.getpid()}_{time.time_ns()}'
        self.worker=threading.Thread(target=self._write,daemon=True,name='pm-ws-tick-writer')
        self.worker.start()
        self.emit(dict(kind='start',received_at=time.time(),note='SDK-decoded subscribed BTC events; not raw websocket frames'))

    def emit(self,record):
        self.capture_sequence+=1
        record=dict(record,capture_id=self.segment,capture_sequence=self.capture_sequence,
                    dropped_before_emit=self.dropped)
        if self.storage_stopped:
            self.dropped+=1;return
        try:self.q.put_nowait(record)
        except queue.Full:self.dropped+=1

    def stats(self):
        return dict(received=self.received,written=self.written,dropped=self.dropped,
                    writer_errors=self.errors,queued=self.q.qsize(),mode=self.policy.get('mode'),
                    study_end_utc=self.policy.get('end_utc'),last_error=getattr(self,'last_error',None))

    def _config(self,now):
        if now-self.config_at>=1:
            self.config_at=now
            try:self.policy=json.loads(self.config.read_text())
            except Exception as e:self.policy={};self.errors+=1;self.last_error=str(e)
        return self.policy

    def _enabled(self,now):
        p=self._config(now)
        if self.storage_stopped:return False
        if p.get('mode') in ('all','closing'):
            end=datetime.fromisoformat(p['end_utc']).timestamp()
            start=datetime.fromisoformat(p['start_utc']).timestamp() if p.get('start_utc') else 0
            return start<=now<end
        return p.get('mode')=='signal'

    def on_reference(self,row,received_at):
        if not self._enabled(received_at):return
        self.reference_received+=1
        self.reference_last_received=received_at
        self.emit(dict(kind='reference_tick',received_at=received_at,
            reference_sequence=self.reference_received,
            observation={key:row.get(key) for key in ('feed_id','observations_ts',
                'benchmark_price','collector_received_at','sequence','publisher_epoch')}))

    def start_labels(self):
        self.label_task=asyncio.create_task(self._labels_loop())

    async def _labels_loop(self):
        while not self.closed:
            now=time.time()
            pending=[int(a) for a in self.pending_labels if int(a)+420<now][:8]
            for anchor in pending:
                try:
                    label=await asyncio.to_thread(fetch_venue_label,anchor)
                    if label:
                        self.venue_labels[str(anchor)]=label
                        self.pending_labels.pop(str(anchor),None)
                        self.emit(dict(kind='venue_label',received_at=label['acquired_at'],**label))
                except Exception as exc:self.label_errors+=1;self.last_error='venue label: '+str(exc)[:120]
                # Unresolved/temporarily unavailable markets must not starve newer labels.
                key=str(anchor)
                if key in self.pending_labels:
                    value=self.pending_labels.pop(key);self.pending_labels[key]=value
            await asyncio.sleep(30)

    def snapshot(self,w,now):
        """Bootstrap + periodic copies of depth after the named event sequence."""
        anchor=w['window_anchor']
        if now-self.snapshots.get(anchor,0)<5:return
        self.snapshots[anchor]=now;books={}
        for side in ('up','down'):
            token=str(w.get(side+'_token'));b=self.bot.bookfeed._books.get(token)
            if b is None:continue
            books[side]=dict(token_id=token,book_received_at=b.get('ts'),
                touch_mismatch=b.get('touch_mismatch',False),
                bids=sorted((float(p),float(q)) for p,q in b.get('bids',{}).items()),
                asks=sorted((float(p),float(q)) for p,q in b.get('asks',{}).items()))
        self.emit(dict(kind='book_snapshot',received_at=now,anchor=anchor,
            available_at=time.time(),after_sequence=self.sequence,books=books))
        self.snapshot_count+=1

    async def refresh(self,w):
        anchor=w['window_anchor']
        try:
            result=await asyncio.to_thread(self.module.twap_lock_estimate,anchor)
            if result:
                est,feed_ts,coverage=result
                if not w.get('price_open'):return
                self.signal[anchor]=dict(estimate=est,gap=(est/w['price_open']-1)*10000,
                    feed_ts=feed_ts,coverage=coverage,refreshed_at=time.time())
        except Exception as e:
            self.signal.pop(anchor,None);self.last_error=str(e);self.errors+=1

    def evaluate(self,w,token,now):
        """Called for every affected token on each WS event, including after entries."""
        m=self.module;anchor=w['window_anchor'];elapsed=now-anchor
        if not m.TWAP_EDGE_T0_S<=elapsed<m.TWAP_EDGE_T1_S:return None
        task=self.refresh_tasks.get(anchor)
        if now-self.last_refresh.get(anchor,0)>=.25 and (task is None or task.done()):
            self.last_refresh[anchor]=now
            self.refresh_tasks[anchor]=asyncio.create_task(self.refresh(w))
        result=dict(anchor=anchor,token_id=token,elapsed_s=elapsed,reason='signal_missing',qualified=False)
        signal=self.signal.get(anchor)
        if not signal:return result
        age=now-signal['refreshed_at'];result.update(signal,signal_age_ms=age*1000)
        if age>1 or now-float(signal['feed_ts'])>8:
            result['reason']='signal_stale';return result
        side='Up' if signal['gap']>=0 else 'Down'
        result['side']=side
        if str(w.get('up_token') if side=='Up' else w.get('down_token'))!=token:
            result['reason']='opposite_token';return result
        levels=self.bot.bookfeed.levels(token)
        if levels is None:result['reason']='depth_unusable';return result
        from pm_twap_presend import valid_levels,edge_cap
        try:
            bids,asks=valid_levels(levels)
            ask,bid=min(p for p,s in asks),max(p for p,s in bids)
            policy=w.get('twap_entry_quality') or getattr(self.bot,'_twap_effective_quality',{})
            if not policy:
                result['reason']='policy_missing';return result
            ok,why=m.twap_quality.evaluate(policy,signal['gap'],ask,bid)
            result.update(ask=ask,bid=bid,spread=ask-bid)
            if not ok:result['reason']=why;return result
            ceiling=min(.85,float(policy.get('shadow_max_ask',w.get('twap_edge_max_px',m.TWAP_EDGE_MAX_PX))))
            floor=float(policy.get('shadow_min_ask',m.TWAP_EDGE_MIN_PX))
            if ask<floor:result['reason']='ask_below_floor';return result
            sample=dict(w.get('twap_calibration') or {},elapsed=elapsed,gap=signal['gap'],estimate=signal['estimate'],
                coverage=signal['coverage'],feed_ts=signal['feed_ts'],ask=ask,quality_bid=bid)
            from pm_twap_presend import admission_probability
            sample.update(anchor=anchor,source='projected_twap',quality_policy=policy)
            p,calibration_status,enforced=admission_probability(self.bot,m,sample,now)
            if p is None:result['reason']='probability_missing';return result
            cap,_=edge_cap(p,ask,policy,m.twap_quality,m.POLY_FEE_RATE,
                max(.05,float(policy.get('shadow_min_edge',m.TWAP_EDGE_MIN_EDGE))),ceiling,.02 if enforced else 0.)
            # $1 depth diagnostics; explicitly separate from bot's configured ticket.
            shares,spent,vwap,frac=m.sweep_ladder(asks,1.,cap)
            result.update(probability=p,cap=cap,one_dollar_vwap=vwap,one_dollar_fill_fraction=frac,
                qualified=(frac or 0)>=.95,reason='pass' if (frac or 0)>=.95 else 'depth_thin',
                calibration_status=calibration_status,calibration_enforced=enforced,
                book_received_at=self.bot.bookfeed._books[token]['ts'],policy_version=policy.get('version'),
                prior_entry=bool(w.get('twap_edge_done')))
        except Exception as e:result['reason']=str(e)
        return result

    def on_event(self,event,received_at=None):
        now=received_at if received_at is not None else time.time();self.received+=1;self.sequence+=1
        if not self._enabled(now):return
        pl=getattr(event,'payload',None);typ=getattr(event,'type',None)
        tokens={str(getattr(pl,'token_id',''))}
        tokens.update(str(getattr(pc,'token_id','')) for pc in getattr(pl,'price_changes',[]) or [])
        tokens.discard('')
        evaluations=[];anchors=set()
        for w in list(self.bot.windows.values()):
            if 0<=now-w['window_anchor']<300 and tokens.intersection({str(w.get('up_token')),str(w.get('down_token'))}):
                if self.policy.get('mode')=='closing' and not float(self.policy.get('capture_start_s',235))<=now-w['window_anchor']<300:
                    continue
                anchors.add(w['window_anchor'])
                if w['window_anchor'] not in self.window_records:
                    self.window_records.add(w['window_anchor'])
                    self.emit(dict(kind='window',received_at=now,anchor=w['window_anchor'],
                        price_open=w.get('price_open'),slug=w.get('slug'),
                        up_token=str(w.get('up_token')),down_token=str(w.get('down_token')),
                        policy=w.get('twap_entry_quality') or getattr(self.bot,'_twap_effective_quality',{}),
                        note='Opening reference observed at this receipt; original acquisition time unknown'))
                    if str(w['window_anchor']) not in self.venue_labels:
                        self.pending_labels[str(w['window_anchor'])]=w.get('slug')
                for token in sorted(tokens.intersection({str(w.get('up_token')),str(w.get('down_token'))})):
                    r=self.evaluate(w,token,now)
                    if r:evaluations.append(r)
                if any(r.get('qualified') for r in evaluations) or any(l.get('book')=='twap_edge' and (l.get('stake_usd') or 0)>0 for l in w.get('legs',[])):
                    self.signaled.add(w['window_anchor'])
                if self.policy.get('mode')=='closing':self.snapshot(w,now)
        if self.policy.get('mode')=='closing' and not anchors:return
        if self.policy.get('mode')=='signal' and not anchors.intersection(self.signaled):return
        self.clob_last_received=now
        self.emit(dict(kind='event',sequence=self.sequence,received_at=now,event_type=typ,
            available_at=time.time(),
            payload=pl,subscribed_tokens=list(self.bot.bookfeed._tokens),anchors=sorted(anchors),
            evaluations=evaluations))
        # Remove old state without retaining hours of token/anchor objects.
        for anchor in list(self.signal):
            if now-anchor>600:self.signal.pop(anchor,None);self.last_refresh.pop(anchor,None);self.refresh_tasks.pop(anchor,None);self.signaled.discard(anchor)
        for anchor in list(self.snapshots):
            if now-anchor>600:self.snapshots.pop(anchor,None);self.window_records.discard(anchor)

    def _write(self):
        handle=None;hour=None;last_flush=0
        while not self.closed or not self.q.empty():
            try:r=self.q.get(timeout=.5)
            except queue.Empty:
                if handle:handle.flush()
                self._health()
                continue
            try:
                if self.storage_stopped:self.dropped+=1;continue
                self._storage_check()
                if self.storage_stopped:self.dropped+=1;continue
                new_hour=datetime.fromtimestamp(r['received_at'],timezone.utc).strftime('%Y%m%d_%H')
                if new_hour!=hour:
                    if handle:handle.close()
                    hour=new_hour;handle=gzip.open(self.out/(hour+'_'+self.segment+'.jsonl.gz'),'at',encoding='utf-8')
                handle.write(json.dumps(serial(r),allow_nan=False,separators=(',',':'))+'\n');self.written+=1
                if time.time()-last_flush>1:
                    handle.flush();last_flush=time.time()
                    self._health()
            except Exception as e:self.errors+=1;self.last_error=str(e)
            finally:self.q.task_done()
        if handle:handle.close()

    def _health(self):
        try:
            value=dict(self.stats(),reference_received=self.reference_received,
                snapshots=self.snapshot_count,storage_stopped=self.storage_stopped,
                pending_labels=len(self.pending_labels),venue_labels=len(self.venue_labels),
                label_errors=self.label_errors,
                reference_last_received=self.reference_last_received,
                clob_last_received=self.clob_last_received,
                output=str(self.out),updated_at=time.time(),capture_version=2)
            tmp=self.out/('health_'+self.segment+'.tmp')
            tmp.write_text(json.dumps(value,indent=2));tmp.replace(self.out/'health.json')
            for name,value in (('pending_labels',dict(self.pending_labels)),('venue_labels',dict(self.venue_labels))):
                tmp=self.out/(name+'_'+self.segment+'.tmp')
                tmp.write_text(json.dumps(value,indent=2));tmp.replace(self.out/(name+'.json'))
        except Exception as exc:self.errors+=1;self.last_error=str(exc)

    def _storage_check(self):
        now=time.time()
        if now-self.storage_checked_at<30:return
        self.storage_checked_at=now
        limit=int(self.policy.get('max_archive_bytes',8*1024**3))
        total=sum(p.stat().st_size for p in self.out.glob('*.gz'))
        if total>=limit or shutil.disk_usage(self.out).free<2*1024**3:
            self.storage_stopped=True;self.last_error='Capture storage limit reached; existing archives retained'
            self._health()

    def close(self):
        self.closed=True
        if self.label_task:self.label_task.cancel()
        self.worker.join(timeout=5);self._health()


def fetch_venue_label(anchor):
    """Public, resolved venue outcome; never substitute a spot-price label."""
    import requests
    slug=f'btc-updown-5m-{int(anchor)}'
    response=requests.get('https://gamma-api.polymarket.com/markets/slug/'+slug,timeout=5)
    response.raise_for_status();row=response.json()
    if row.get('slug')!=slug or not row.get('closed') or str(row.get('umaResolutionStatus','')).lower()!='resolved':
        return None
    outcomes=row.get('outcomes') or [];prices=row.get('outcomePrices') or []
    if isinstance(outcomes,str):outcomes=json.loads(outcomes)
    if isinstance(prices,str):prices=json.loads(prices)
    if len(outcomes)!=2 or len(prices)!=2 or set(outcomes)!={'Up','Down'}:return None
    prices=[float(p) for p in prices]
    winners=[o for o,p in zip(outcomes,prices) if p==1.]
    if len(winners)!=1 or sorted(prices)!=[0.,1.]:return None
    return dict(anchor=int(anchor),slug=slug,outcome=winners[0],acquired_at=time.time(),
                source='gamma_resolved_market',market={k:row.get(k) for k in
                ('id','slug','closed','umaResolutionStatus','outcomes','outcomePrices')})
