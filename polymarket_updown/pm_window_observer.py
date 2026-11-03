"""Read-only per-window quote/gap observations. Never submits orders."""
import os
import sys
import asyncio
import csv
import json
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LOG = ROOT/'state/pm_window_observations.jsonl'
SUMMARY = ROOT/'reports/pm_window_observation_summary.csv'
_last = 0
_task = None
_last_summary = 0


def tier(module, gap):
    return module.tier_for(abs(gap))[0] or 'SUB-WEAK' if gap is not None else None


def append(record):
    with LOG.open('a') as f:
        f.write(json.dumps(record, allow_nan=False)+'\n')


async def quote(bot, module, token):
    started = time.time()
    try:
        # Copy the already consumed WS book synchronously. REST previously took
        # several seconds and made every prospective model sample unusable.
        feed = getattr(bot, 'bookfeed', None)
        task = getattr(feed, '_task', None)
        cached = getattr(feed, '_books', {}).get(str(token), {})
        confirm = getattr(feed, 'confirmed_at', None)
        import pm_t240_live_probe
        source_received = (confirm(token, pm_t240_live_probe.loop_ok(bot)) if confirm else cached.get('ts', 0)) or 0
        import pm_rest_book_poller as rest
        if task is None or task.done() or not 0 <= started-source_received <= .1:
            return (rest.rest_quote(bot, token, started) or
                    dict(request_at=started, received_at=time.time(),
                         error_type='ws_missing_disconnected_or_stale'))
        levels = feed.levels(str(token))
        if levels is None:
            # One-sided books stay unavailable; only a touch/depth contradiction
            # means the WS copy is wrong and a fresh REST snapshot may stand in.
            return ((rest.rest_quote(bot, token, started) if cached.get('touch_mismatch') else None) or
                    dict(request_at=started, received_at=time.time(),
                         error_type='ws_depth_unavailable'))
        bids, asks = (list(levels[0]), list(levels[1]))
        received = time.time()
        bid = max((p for p,s in bids if 0<p<1 and s>0), default=None)
        ask = min((p for p,s in asks if 0<p<1 and s>0), default=None)
        sh, usd, avg, fraction = module.sweep_ladder(asks, 1., .99)
        return dict(request_at=started, received_at=received,
                    latency_ms=(received-started)*1000,
                    quote_source='ws', book_received_at=source_received,
                    exchange_timestamp_raw=None,
                    bid=bid, ask=ask, spread=ask-bid if ask and bid else None,
                    one_dollar_vwap=avg, one_dollar_fill_fraction=fraction,
                    bids=bids, asks=asks)
    except Exception as exc:
        return dict(request_at=started, received_at=time.time(), error_type=type(exc).__name__)


def summarize():
    labels_path = Path('./reports/twap_calibration_official_labels.json')
    labels = json.loads(labels_path.read_text()) if labels_path.exists() else {}
    windows = {}
    if not LOG.exists():
        return
    for line in LOG.read_text().splitlines():
        r = json.loads(line)
        windows.setdefault(r['anchor'], []).append(r)
    rows = []
    for anchor, samples in windows.items():
        label = labels.get(str(anchor))
        final_gap = (label['price_settle']/label['price_open']-1)*10000 if label else None
        for kind in ('spot', 'twap'):
            valid = [r for r in samples if r.get(kind+'_gap_bps') is not None]
            if not valid:
                continue
            transitions = []
            prev = None
            for r in valid:
                state = (r[kind+'_tier'], 'Up' if r[kind+'_gap_bps']>=0 else 'Down')
                if prev is not None and state != prev:
                    transitions.append(dict(elapsed_s=r['elapsed_s'], before=prev, after=state))
                prev = state
            first, last = valid[0], valid[-1]
            rows.append(dict(anchor=anchor, gap_type=kind, first_elapsed_s=first['elapsed_s'],
                             first_gap_bps=first[kind+'_gap_bps'], first_tier=first[kind+'_tier'],
                             last_elapsed_s=last['elapsed_s'], last_gap_bps=last[kind+'_gap_bps'],
                             last_tier=last[kind+'_tier'], final_gap_bps=final_gap,
                             first_direction_win=(final_gap>=0)==(first[kind+'_gap_bps']>=0) if final_gap is not None else None,
                             transitions=json.dumps(transitions), samples=len(valid)))
    if rows:
        tmp = SUMMARY.with_suffix('.tmp')
        with tmp.open('w') as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0])); w.writeheader(); w.writerows(rows)
        tmp.replace(SUMMARY)


async def observe(bot, module):
    global _last_summary
    try:
        now = time.time()
        for anchor, window in list(bot.windows.items()):
            elapsed = now-anchor
            if not 120 <= elapsed < 300:
                continue
            opening = window.get('price_open')
            if not opening:
                continue
            spot = await asyncio.to_thread(module.underlying_price_now)
            projected = await asyncio.to_thread(module.twap_lock_estimate, anchor) if elapsed>=240 else None
            up, down = await asyncio.gather(quote(bot,module,window['up_token']), quote(bot,module,window['down_token']))
            spot_gap = (spot/opening-1)*10000 if spot else None
            twap_gap = (projected[0]/opening-1)*10000 if projected else None
            record = dict(anchor=anchor, sampled_at=now, completed_at=time.time(), elapsed_s=elapsed,
                          spot_gap_bps=spot_gap, spot_tier=tier(module,spot_gap),
                          twap_gap_bps=twap_gap, twap_tier=tier(module,twap_gap),
                          projected_feed_ts=projected[1] if projected else None,
                          projected_coverage=projected[2] if projected else None,
                          up_quote=up, down_quote=down,
                          mode_decisions=[{k:leg.get(k) for k in ('book','tier','decision_bps','predicted_side','status','entry_price','twap_decision_ask')} for leg in window.get('legs',[])])
            await asyncio.to_thread(append, record)
            import pm_three_rule_live
            if pm_three_rule_live.configured():
                pm_three_rule_live.stage1_schedule(bot,module,window,record)
            else:
                import pm_t240_live_probe
                pm_t240_live_probe.schedule(bot,module,window,record)
        if time.time()-_last_summary>=120:
            _last_summary=time.time()
            # Separate process: parsing the whole observation log in a thread
            # holds the GIL and stalls the bot's event loop for seconds.
            proc=await asyncio.create_subprocess_exec(sys.executable,str(Path(__file__).resolve()),
                stdout=asyncio.subprocess.DEVNULL,stderr=asyncio.subprocess.DEVNULL)
            asyncio.create_task(proc.wait())
    except Exception as exc:
        print('PM-WINDOW-OBSERVER '+type(exc).__name__, flush=True)


def schedule(bot, module):
    global _last, _task
    import pm_three_rule_live
    if pm_three_rule_live.configured():pm_three_rule_live.schedule(bot,module)
    if module.ASSET!='BTC' or time.time()-_last<5 or (_task is not None and not _task.done()):
        return
    _last=time.time()
    _task=asyncio.create_task(observe(bot,module))


if __name__ == '__main__':
    os.nice(15) if hasattr(os,'nice') else None
    summarize()
