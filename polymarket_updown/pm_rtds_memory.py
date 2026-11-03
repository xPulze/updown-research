"""Receive collector ticks without waiting for PostgreSQL persistence."""
import asyncio,json,math,os,socket,threading,time,uuid
from pathlib import Path

ROOT=Path('./state')

def socket_path(symbol):return ROOT/('pm_rtds_'+symbol.split('/')[0]+'.sock')

class MemoryFeed:
 def __init__(self,symbol,seed=(),on_tick=None):
  self.symbol=symbol;self.rows={int(ts):float(price) for ts,price in seed}
  self.on_tick=on_tick;self.lock=threading.RLock();self.live=False;self.received_at=0.;self.last_sequence=None
  self.received=0;self.sequence_gaps=0;self.errors=0;self.sock=None;self.loop=None
 def start(self):
  path=socket_path(self.symbol);path.parent.mkdir(parents=True,exist_ok=True)
  if path.exists():path.unlink()
  self.sock=socket.socket(socket.AF_UNIX,socket.SOCK_DGRAM);self.sock.setblocking(False)
  self.sock.bind(str(path));os.chmod(path,0o600)
  self.loop=asyncio.get_running_loop();self.loop.add_reader(self.sock.fileno(),self.receive)
 def apply(self,r):
  if r.get('feed_id')!=self.symbol:return False
  now=time.time();ts=int(r['observations_ts']);price=float(r['benchmark_price'])
  if not math.isfinite(price) or price<=0 or not -1<=now-ts<=10:return False
  with self.lock:
   seq=r.get('sequence');epoch=r.get('publisher_epoch')
   if getattr(self,'epoch',None)==epoch and seq is not None and self.last_sequence is not None and seq>self.last_sequence+1:self.sequence_gaps+=seq-self.last_sequence-1
   self.epoch=epoch;self.last_sequence=seq;self.rows[ts]=price;self.live=True;self.received_at=now;self.received+=1
   for old in list(self.rows):
    if old<now-3600:self.rows.pop(old,None)
  if self.on_tick:self.on_tick()
  recorder=getattr(self,'on_observation',None)
  if recorder:
   try:recorder(dict(r),now)
   except Exception:self.errors+=1
  return True
 def receive(self):
  while True:
   try:payload,address=self.sock.recvfrom(65536)
   except BlockingIOError:return
   try:
    row=json.loads(payload)
    if row.get('kind')=='quote_request' and address and getattr(self,'on_query',None):
     self.sock.sendto(json.dumps(self.on_query()).encode(),address)
    else:self.apply(row)
   except Exception:self.errors+=1
 def range(self,lo,hi):
  with self.lock:
   if not self.live or time.time()-self.received_at>10:return []
   return sorted((ts,p) for ts,p in self.rows.items() if lo<ts<=hi)
 def latest(self,max_age=8):
  rows=self.range(time.time()-max_age-1,time.time())
  return rows[-1] if rows and time.time()-rows[-1][0]<=max_age else None
 def stats(self):return dict(source='collector_before_db',received=self.received,receive_age_s=round(time.time()-self.received_at,3) if self.received_at else None,sequence_gaps=self.sequence_gaps,errors=self.errors)
 def close(self):
  if self.sock:
   self.loop.remove_reader(self.sock.fileno());self.sock.close();socket_path(self.symbol).unlink(missing_ok=True)


def read_quotes(symbol='btc/usd',timeout=.5):
 """Read current engine memory directly; no filesystem snapshots or DB reads."""
 with socket.socket(socket.AF_UNIX,socket.SOCK_DGRAM) as client:
  client.settimeout(timeout)
  client.bind('\0pm_quote_'+uuid.uuid4().hex)
  client.sendto(b'{"kind":"quote_request"}',str(socket_path(symbol)))
  return json.loads(client.recv(65536))
