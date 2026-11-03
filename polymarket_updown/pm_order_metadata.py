"""Short-lived per-executor SDK metadata cache. Never caches quotes or orders."""
import asyncio,copy,time
from dataclasses import replace
class MetadataCache:
 def __init__(self):self.values={};self.tasks={};self.last={}
 def key(self,path,params):return (path,tuple(sorted((params or {}).items())))
 def ttl(self,path):return 15 if path=='/tick-size' else 60
 def allowed(self,path):return path in ('/tick-size','/neg-risk') or path.startswith(('/markets-by-token/','/clob-markets/'))
 def invalidate_tick(self,token=None):
  for k in list(self.values):
   if k[0]=='/tick-size' and (token is None or dict(k[1]).get('token_id')==str(token)):self.values.pop(k,None)
 def schedule(self,client,tokens,anchor):
  now=time.monotonic()
  for token in tokens:
   token=str(token);old=self.tasks.get(token)
   if now-self.last.get(token,-100)>=6 and (old is None or old.done()):
    self.last[token]=now;self.tasks[token]=asyncio.create_task(self.prefetch(client,token,anchor))
  for token,at in list(self.last.items()):
   task=self.tasks.get(token)
   if now-at>120 and (task is None or task.done()):self.last.pop(token,None);self.tasks.pop(token,None)
  for k,(at,v) in list(self.values.items()):
   if now-at>120:self.values.pop(k,None)
 async def prefetch(self,client,token,anchor):
  from polymarket._internal.actions.orders import market_data as md
  trace=[];proxy=MetadataTransport(client._ctx.clob,self,trace,force=True);ctx=replace(client._ctx,clob=proxy)
  try:
   tick,neg,condition=await asyncio.gather(md.fetch_tick_size(ctx,token_id=token),md.fetch_neg_risk(ctx,token_id=token),md.resolve_condition_by_token(ctx,token_id=token))
   await md.fetch_platform_fee_info(ctx,condition_id=condition)
   status='ready'
  except Exception as e:status=type(e).__name__
  try:
   from pm_execution_audit import append
   append('metadata_prefetch',dict(anchor=anchor,token_id=token,status=status,requests=trace))
  except Exception:pass
class MetadataTransport:
 def __init__(self,transport,cache,trace,force=False,cache_only=False):self.transport=transport;self.cache=cache;self.trace=trace;self.force=force;self.cache_only=cache_only
 def __getattr__(self,name):return getattr(self.transport,name)
 async def get_json(self,path,params=None,**kwargs):
  start=time.perf_counter();now=time.monotonic();k=self.cache.key(path,params);entry=self.cache.values.get(k);hit=not self.force and self.cache.allowed(path) and entry is not None and 0<=now-entry[0]<self.cache.ttl(path)
  try:
   if hit:return copy.deepcopy(entry[1])
   if self.cache_only:raise ValueError('Prepared order metadata unavailable: '+path)
   value=await self.transport.get_json(path,params=params,**kwargs)
   if self.cache.allowed(path):self.cache.values[k]=(time.monotonic(),copy.deepcopy(value))
   return value
  finally:self.trace.append(dict(path=path,cache_hit=bool(hit),cache_age_ms=(now-entry[0])*1000 if hit else None,elapsed_ms=(time.perf_counter()-start)*1000))
async def prepare(client,cache,trace,cache_only=False,**kwargs):
 if not hasattr(client,'_ctx'):
  if cache_only:raise ValueError('Cache-only signing unsupported for client')
  return await client._prepare_and_sign_market_order(**kwargs)
 clone=copy.copy(client);clone._ctx=replace(client._ctx,clob=MetadataTransport(client._ctx.clob,cache,trace,cache_only=cache_only))
 return await clone._prepare_and_sign_market_order(**kwargs)
