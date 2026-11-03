"""Durable EDGE execution diagnostics; no credentials and no order submission."""
import json,os,time
from pathlib import Path
PATH=Path('./state/pm_edge_execution_audit.jsonl')
def append(event,record):
 PATH.parent.mkdir(parents=True,exist_ok=True)
 row=dict(record,event=event,audit_at=time.time())
 with PATH.open('a') as f:
  f.write(json.dumps(row,allow_nan=False,default=str,separators=(',',':'))+'\n');f.flush();os.fsync(f.fileno())
async def match_timestamp(client,order_id,token,anchor):
 import asyncio
 try:
  await asyncio.sleep(1)
  raw=await client._ctx.secure_clob.get_json('/data/trades',params={'asset_id':str(token),'after':str(int(anchor))})
  rows=[{k:r.get(k) for k in ('id','taker_order_id','match_time','last_update','status','price','size','side')} for r in raw.get('data',[]) if r.get('taker_order_id')==order_id]
  append('exchange_match_timestamp',dict(order_id=order_id,anchor=anchor,trades=rows,timestamp_precision='reported wire values; whole-second match_time is not exact millisecond latency'))
 except Exception as e:append('exchange_timestamp_error',dict(order_id=order_id,anchor=anchor,error_type=type(e).__name__))
