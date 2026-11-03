"""Shared, deterministic EDGE admission rules for runtime and validation."""
import hashlib
import json
import math
from datetime import datetime
from zoneinfo import ZoneInfo


def for_anchor(policy, anchor):
    result = dict(policy)
    weekday_gap = float(policy.get('weekday_min_gap_bps', policy.get('min_gap_bps', 0)))
    result['weekday_min_gap_bps'] = weekday_gap
    result['min_gap_bps'] = weekday_gap
    if policy.get('weekend_min_gap_bps') is not None:
        if datetime.fromtimestamp(anchor, ZoneInfo('Asia/Kolkata')).weekday() >= 5:
            result['min_gap_bps'] = float(policy['weekend_min_gap_bps'])
    return result


def signature(policy):
    rules = {key: policy.get(key) for key in (
        'enabled', 'valid', 'max_spread', 'shadow_min_ask', 'shadow_max_ask',
        'shadow_min_edge', 'win_rate_tiers', 'entry_t0_s', 'entry_t1_s',
        'weekend_min_gap_bps')}
    rules['min_gap_bps'] = policy.get('weekday_min_gap_bps', policy.get('min_gap_bps'))
    # Execution changes invalidate previously qualified policy fingerprints.
    rules['execution_max_slippage'] = policy.get('execution_max_slippage', .01)
    rules['execution_version'] = 'price_bound_postsign_v1'
    return hashlib.sha256(json.dumps(rules, sort_keys=True).encode()).hexdigest()[:16]


def admit(probability, sample, policy, quality, fee=.07, cost=.02):
    """Return the same worst-fill cap and tier in both validation and runtime."""
    from pm_twap_presend import edge_cap
    policy = for_anchor(policy, sample['anchor'])
    elapsed = float(sample['elapsed'])
    if not float(policy.get('entry_t0_s', 240)) <= elapsed < float(policy.get('entry_t1_s', 262)):
        raise ValueError('TWAP entry window expired')
    ask = float(sample['ask'])
    if not math.isfinite(ask) or ask < float(policy.get('shadow_min_ask', .35)):
        raise ValueError('TWAP ask below policy floor')
    ceiling = min(.85, float(policy.get('shadow_max_ask', .90)))
    if ask > math.floor(ceiling * 100 + 1e-9) / 100 + 1e-9:
        raise ValueError('TWAP ask above policy ceiling')
    ok, why = quality.evaluate(policy, sample['gap'], ask, sample.get('quality_bid'))
    if not ok:
        raise ValueError(why)
    return edge_cap(probability, ask, policy, quality, fee,
                    max(.05, float(policy.get('shadow_min_edge', .10))),
                    ceiling, cost)
