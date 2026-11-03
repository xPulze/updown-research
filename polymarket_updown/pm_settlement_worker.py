"""BTC settlement workers and display-only provisional window results.

Never closes a trade, releases risk, or credits cash from a provisional label.
"""
import asyncio
import copy
import json
import math
import os
from pathlib import Path
import tempfile
import time

ROOT = Path('.')
STATE = ROOT / 'state/pm_settlement_worker.json'
AUDIT = ROOT / 'state/pm_settlement_worker_audit.jsonl'


def _persist(state, event=None):
    STATE.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(dir=STATE.parent, prefix='.pm_settlement_')
    try:
        with os.fdopen(fd, 'w') as f:
            json.dump(state, f, allow_nan=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(name, STATE)
    finally:
        if os.path.exists(name):
            os.unlink(name)
    if event:
        with AUDIT.open('a') as f:
            f.write(json.dumps(event, allow_nan=False) + '\n')
            f.flush()
            os.fsync(f.fileno())


def provisional(windows, now, window_s):
    results = {}
    for anchor, w in list(windows.items()):
        if now < anchor + window_s:
            continue
        nxt = windows.get(anchor + window_s)
        opening = w.get('price_open')
        boundary = nxt.get('price_open') if nxt else None
        if any(isinstance(x, bool) or not isinstance(x, (float, int))
               or not math.isfinite(x) or x <= 0 for x in (opening, boundary)):
            continue
        estimate = 'Up' if boundary >= opening else 'Down'
        official = w.get('pm_outcome')
        results[str(anchor)] = dict(
            outcome=estimate, official_outcome=official,
            matches_official=(estimate == official) if official in ('Up', 'Down') else None,
            opening=opening, estimated_close=boundary,
            source='next_window_published_open', next_anchor=anchor + window_s,
            observed_at=now, provisional=True,
            label='Provisional estimate; official PM result pending',
        )
    return results


async def _stage(bot, module, name, operation):
    state = bot._pm_settlement_state
    health = state['stages'].setdefault(name, {})
    started = time.perf_counter()
    tick_start = state.get('decision_ticks', 0)
    health.update(running=True, started_at=time.time(), error=None)
    try:
        return await operation
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        health['error'] = str(exc)
        module._err('background.' + name, exc)
    finally:
        health.update(running=False, finished_at=time.time(),
                      duration_ms=(time.perf_counter() - started) * 1000,
                      decision_ticks_during_stage=state.get('decision_ticks', 0)-tick_start)
        event = dict(event='background_stage', stage=name, at=time.time(), **health)
        # File writes and fsync never run on the decision event loop.
        async with bot._pm_settlement_publish_lock:
            await asyncio.to_thread(_persist, copy.deepcopy(state), event)


async def _bookkeeping(bot, module):
    now = time.time()
    if now - getattr(bot, '_pm_recon_ts', 0) >= 15:
        bot._pm_recon_ts = now
        await _stage(bot, module, 'official_reconciliation',
                     module._offload(module.reconcile_pending_pm_outcomes))
    for anchor, w in list(bot.windows.items()):
        if time.time() >= w['expiry'] and not w['settled']:
            await _stage(bot, module, 'window_settlement', module._offload(bot.settle, w))
    if time.time() - getattr(bot, '_deferred_sweep_ts', 0) >= module.DEFERRED_SETTLE_EVERY_S:
        bot._deferred_sweep_ts = time.time()
        await _stage(bot, module, 'deferred_settlement', bot._settle_deferred())


async def _redemptions(bot, module):
    # The original queue draining implementation is retained on Bot. It can now
    # wait on-chain without holding up settlement, provisional labels or ticks.
    await _stage(bot, module, 'redemption', bot._background_redemption_run())


async def _publish_provisional(bot, rows):
    state = bot._pm_settlement_state
    changes = []
    for anchor, result in rows.items():
        prior = state['provisional'].get(anchor)
        if prior is None or any(prior.get(k) != result[k] for k in
                                ('outcome', 'opening', 'estimated_close', 'official_outcome')):
            state['provisional'][anchor] = result
            changes.append(dict(anchor=int(anchor), **result))
    # Retain estimates across restarts; bounded history, not a trading ledger.
    keys = sorted(state['provisional'], key=int)
    for key in keys[:-2016]:
        del state['provisional'][key]
    state['at'] = time.time()
    async with bot._pm_settlement_publish_lock:
        await asyncio.to_thread(_persist, copy.deepcopy(state), dict(
            event='provisional_update', at=time.time(), changes=changes))


async def _guard(bot, module, name, operation):
    try:
        await operation
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        module._err('background_worker.' + name, exc)
        print('PM-BACKGROUND-ERROR ' + name + ' ' + type(exc).__name__, flush=True)


def schedule(bot, module):
    """Non-awaiting scheduler: at most one task per independent worker."""
    if module.ASSET != 'BTC':
        return
    now = time.time()
    if not hasattr(bot, '_pm_settlement_tasks'):
        try:
            state = json.loads(STATE.read_text())
        except (OSError, ValueError):
            state = {}
        bot._pm_settlement_state = dict(
            pid=os.getpid(), started_at=now, at=now, stages={},
            provisional=state.get('provisional', {}))
        bot._pm_settlement_tasks = {}
        bot._pm_settlement_last = {}
        bot._pm_settlement_publish_lock = asyncio.Lock()
    bot._pm_settlement_state['decision_scheduler_at'] = now
    bot._pm_settlement_state['decision_ticks'] = bot._pm_settlement_state.get('decision_ticks', 0) + 1
    for name, interval, build in (
        ('bookkeeping', 3, lambda: _bookkeeping(bot, module)),
        ('redemption', 3, lambda: _redemptions(bot, module)),
        ('provisional', 3, lambda: _publish_provisional(
            bot, provisional(bot.windows, now, module.WINDOW_S))),
    ):
        task = bot._pm_settlement_tasks.get(name)
        if task is not None and not task.done():
            continue
        if now - bot._pm_settlement_last.get(name, 0) < interval:
            continue
        bot._pm_settlement_last[name] = now
        bot._pm_settlement_tasks[name] = asyncio.create_task(
            _guard(bot, module, name, build()))
    bot._schedule_redemption_recovery(now)


def inject(state):
    """Add labels only. Existing official fields, metrics and P&L are untouched."""
    try:
        data = json.loads(STATE.read_text())
    except (OSError, ValueError):
        return state
    state['settlement_workers'] = {k: v for k, v in data.items() if k != 'provisional'}
    for row in state.get('windows', []):
        # Never let a provisional estimate compete with a durable PM verdict.
        for key in ('provisional_outcome', 'provisional_hit', 'provisional_reference'):
            row.pop(key, None)
        if row.get('pm_outcome') in ('Up', 'Down'):
            continue
        result = data.get('provisional', {}).get(str(row.get('window_anchor')))
        if result and row.get('staked') and row.get('status') != 'CLOSED':
            row['provisional_outcome'] = result['outcome']
            row['provisional_hit'] = row.get('predicted_side') == result['outcome']
            row['provisional_reference'] = result
    return state


async def shutdown(bot):
    tasks = list(getattr(bot, '_pm_settlement_tasks', {}).values())
    for task in tasks:
        if not task.done():
            task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
