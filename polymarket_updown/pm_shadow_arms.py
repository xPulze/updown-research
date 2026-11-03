"""Read-only shadow arms for the BTC TWAP-EDGE entry rule. Never submits orders.

Each arm re-runs the live EDGE admission (quality gap/spread, ask band, p,
edge_cap, probability tiers) on the same in-memory WS book and projected TWAP,
with one parameter changed. `control` has no override and must agree with the
live decisions. Also records a per-window settlement label from the bot's own
tick memory so arm results can be scored without the sparse official file.
"""
import json
import math
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT/'params/pm_shadow_arms.json'
ARMS_LOG = ROOT/'state/pm_shadow_arms.jsonl'
LABEL_LOG = ROOT/'state/pm_window_labels.jsonl'
HARD_CEILING = .85
# Pre-14-Oct-2026 live table, replayed by arms with legacy_p for audit of the 1-2bp fix.
LEGACY_P = {240: (0.612, 0.703, 0.821, 0.910, 0.939, 0.978, 0.995),
            250: (0.630, 0.848, 0.880, 0.948, 0.971, 0.993, 0.998),
            260: (0.712, 0.876, 0.912, 0.975, 0.993, 0.995, 1.000)}
GAP_EDGES = (0.0, 0.25, 0.5, 1.0, 2.0, 3.0, 5.0)


def legacy_p(elapsed_s, gap_abs):
    row = LEGACY_P.get((int(elapsed_s)//10)*10)
    if row is None:
        return None
    return row[sum(gap_abs >= e for e in GAP_EDGES)-1]
_state = {}
_cfg = {'mtime': None, 'value': None}


def load_config(path=CONFIG):
    try:
        mtime = path.stat().st_mtime
        if _cfg['mtime'] != mtime:
            raw = json.loads(path.read_text())
            arms = []
            for a in raw.get('arms', []):
                arms.append(dict(name=str(a['name']),
                                 min_gap_weekday=a.get('min_gap_weekday'),
                                 max_ask=a.get('max_ask'),
                                 legacy_p=bool(a.get('legacy_p'))))
            _cfg.update(mtime=mtime, value=dict(enabled=bool(raw.get('enabled')), arms=arms))
        return _cfg['value']
    except (OSError, ValueError, KeyError, TypeError):
        return dict(enabled=False, arms=[])


def append(path, record):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('a') as f:
        f.write(json.dumps(record, allow_nan=False, default=str)+'\n')


def arm_policy(base, arm, weekend):
    """Live weekend rule replaces the gap with .25; weekday gap may be overridden."""
    policy = dict(base)
    if weekend:
        policy.update(min_gap_bps=.25)
    elif arm.get('min_gap_weekday') is not None:
        policy.update(min_gap_bps=float(arm['min_gap_weekday']))
    if arm.get('max_ask') is not None:
        # May exceed the live cap (to replay the previous .85 rule) but never .85.
        policy.update(shadow_max_ask=min(float(arm['max_ask']), HARD_CEILING))
    return policy


def evaluate_arm(arm, ctx, quality, edge_cap, sweep_ladder, fee, p_fn, stake_usd=1.0):
    """Mirror of Bot._twap_edge_impl admission. Returns (entered, why, details)."""
    policy = arm_policy(ctx['base_policy'], arm, ctx['weekend'])
    gap, touch, bid, elapsed = ctx['gap'], ctx['touch'], ctx['bid'], ctx['elapsed']
    ok, reason = quality.evaluate(policy, gap, touch, bid)
    if not ok:
        return False, reason, {}
    p = p_fn(elapsed, abs(gap))
    if p is None:
        return False, 'no probability', {}
    min_px = policy.get('shadow_min_ask', .35)
    max_px = policy.get('shadow_max_ask', .85)
    min_edge = policy.get('shadow_min_edge', .10)
    edge = p-touch-fee*touch*(1-touch)
    if not (min_px <= touch <= max_px):
        return False, f'ask {touch:.3f} outside {min_px:.2f}-{max_px:.2f}', dict(p=p)
    if edge <= min_edge:
        return False, f'edge {edge:+.3f} <= {min_edge:.2f}', dict(p=p, edge=edge)
    try:
        cap, _ = edge_cap(p, touch, policy, quality, fee, max(.05, min_edge), min(.85, max_px), 0.)
    except ValueError as exc:
        return False, str(exc), dict(p=p, edge=edge)
    tier_ok, tier_reason, _ = quality.evaluate_probability(policy, p, cap, fee)
    if not tier_ok:
        return False, tier_reason, dict(p=p, edge=edge, cap=cap)
    shares, spent, avg, fraction = sweep_ladder(ctx['rungs'], stake_usd, cap)
    return True, 'enter', dict(p=p, edge=edge, cap=cap, policy_max_ask=max_px, fill_vwap=avg, fill_fraction=fraction,
                               shares=shares)


def realized_twap(anchor, rows):
    """Closing-minute TWAP as a 1s step function over [T+240, T+300]; None when thin."""
    start, end = int(anchor)+240, int(anchor)+300
    rows = sorted((int(t), float(p)) for t, p in rows if math.isfinite(float(p)) and float(p) > 0)
    if not rows or rows[0][0] > start:
        return None
    covered = len({t for t, _ in rows if start <= t < end})
    if covered/60. < .9:
        return None
    total, i, current = 0., 0, rows[0][1]
    for stamp in range(start, end):
        while i < len(rows) and rows[i][0] <= stamp:
            current = rows[i][1]
            i += 1
        total += current
    return total/60., min(1., covered/60.)


def _window_state(anchor):
    st = _state.setdefault(anchor, dict(arms={}, final=False, labelled=False))
    for old in [a for a in _state if a < anchor-3600]:
        _state.pop(old, None)
    return st


def tick(bot, module):
    """Called every bot tick; cheap, in-memory, exceptions never reach the caller."""
    try:
        _tick(bot, module)
    except Exception as exc:
        print('PM-SHADOW-ARMS '+type(exc).__name__+': '+str(exc)[:80], flush=True)


def _tick(bot, module):
    cfg = load_config()
    if module.ASSET != 'BTC' or not cfg['enabled'] or not cfg['arms']:
        return
    if module._RTDS_MEMORY is None or getattr(bot, 'bookfeed', None) is None:
        return
    if module.TWAP_CALIBRATION_ENFORCE:
        return  # arms mirror the legacy-probability branch only
    now = time.time()
    for anchor, w in list(bot.windows.items()):
        elapsed = now-anchor
        st = _window_state(anchor)
        if module.TWAP_EDGE_T0_S <= elapsed < module.TWAP_EDGE_T1_S:
            _evaluate_window(bot, module, cfg, w, anchor, elapsed, now, st)
        elif elapsed >= module.TWAP_EDGE_T1_S and not st['final'] and st['arms']:
            st['final'] = True
            for name, s in st['arms'].items():
                if not s.get('entered'):
                    append(ARMS_LOG, dict(kind='no_entry', arm=name, anchor=anchor, why=s.get('why'),
                                          last=s.get('last')))
        if 302 <= elapsed < 420 and not st['labelled'] and w.get('price_open'):
            rows = module._RTDS_MEMORY.range(anchor+210, anchor+300)
            res = realized_twap(anchor, rows)
            if res is not None:
                st['labelled'] = True
                append(LABEL_LOG, dict(anchor=anchor, price_open=w['price_open'], settle_twap=res[0],
                                       coverage=res[1], source='bot_rtds_memory',
                                       outcome='Up' if res[0] >= w['price_open'] else 'Down'))


def _evaluate_window(bot, module, cfg, w, anchor, elapsed, now, st):
    from datetime import datetime, timezone
    from zoneinfo import ZoneInfo
    opening = w.get('price_open')
    if not opening:
        return
    pending = [a for a in cfg['arms'] if not st['arms'].get(a['name'], {}).get('entered')]
    if not pending:
        return
    r = module.twap_lock_estimate(anchor)
    if r is None:
        return
    est, feed_ts, coverage = r
    gap = (est/opening-1.)*1e4
    side = 'Up' if gap >= 0 else 'Down'
    token = w.get('up_token') if side == 'Up' else w.get('down_token')
    if token is None:
        return
    touch, rungs = module.fade_live_touch(bot.bookfeed, token)
    lv = bot.bookfeed.levels(token)
    if lv:
        rungs = [(float(p), float(s)) for p, s in lv[1]
                 if math.isfinite(float(p)) and 0 < float(p) < 1 and float(s) > 0]
        touch = module.fade_clob_touch(rungs)
    bid = module.twap_quality.best_bid(lv)
    weekend = datetime.fromtimestamp(anchor, timezone.utc).astimezone(ZoneInfo('Asia/Kolkata')).weekday() >= 5
    base = w.get('twap_entry_quality')
    if not base:
        return  # the live path has not built this window's policy yet
    # Live weekend override is applied per arm inside arm_policy, so strip it here.
    base = dict(base)
    ctx = dict(base_policy=base, weekend=weekend, gap=gap, touch=touch, bid=bid, rungs=rungs, elapsed=int(elapsed))
    book_ts = getattr(bot.bookfeed, '_books', {}).get(str(token), {}).get('ts')
    for arm in pending:
        s = st['arms'].setdefault(arm['name'], {})
        entered, why, details = evaluate_arm(
            arm, ctx, module.twap_quality, _edge_cap(), module.sweep_ladder, module.POLY_FEE_RATE,
            legacy_p if arm.get('legacy_p') else module.twap_edge_p)
        s['why'] = why
        s['last'] = dict(elapsed=int(elapsed), gap=gap, side=side, ask=touch, bid=bid)
        if entered:
            s['entered'] = True
            append(ARMS_LOG, dict(kind='entry', arm=arm['name'], anchor=anchor, elapsed=elapsed,
                                  side=side, gap=gap, ask=touch, bid=bid, weekend_ist=weekend,
                                  ws_age_ms=(now-book_ts)*1000 if book_ts else None,
                                  projection_coverage=coverage, **details))


def _edge_cap():
    import sys
    sys.path.insert(0, str(ROOT/'scripts'))
    from pm_twap_presend import edge_cap
    return edge_cap
