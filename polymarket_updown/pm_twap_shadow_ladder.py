"""BTC TWAP paper capital; isolated from venue wallet and funded ledgers."""
import math

def capital_and_stake(base, rows, step=10., ceiling=250., stake_per_tier=1.):
    cash=float(base)
    if not all(math.isfinite(float(v)) for v in (cash, step, ceiling, stake_per_tier)) or cash<=0 or step<=0 or stake_per_tier<=0:
        return 0.,0.
    for row in rows:
        price,shares,hit=row
        price=float(price);shares=float(shares)
        if not math.isfinite(price) or not math.isfinite(shares) or not 0<price<1 or shares<0:
            raise ValueError('invalid paper fill')
        cash-=shares*price*(1+.07*(1-price))
        if hit is True:
            cash+=shares
        # Unresolved trades reserve the full cost; no provisional payout.
    cash=max(0.,cash)
    stake=max(1.,float(math.ceil(cash/step))*stake_per_tier) if cash>=1. else 0.
    if ceiling>0:stake=min(stake,ceiling)
    # Ticket excludes fee; reserve a conservative max entry fee.
    if stake*1.07>cash:stake=0.
    return cash,stake


def candidate_policy(base, path=None):
    """Fail closed on invalid optional BTC shadow candidate configuration."""
    from pathlib import Path
    import pm_twap_entry_quality as quality
    policy = quality.load(path or Path(__file__).parent / 'params/twap_edge_shadow_candidate.json')
    try:
        lo, hi, edge = (float(policy[k]) for k in ('shadow_min_ask', 'shadow_max_ask', 'shadow_min_edge'))
        if not policy.get('valid') or not policy.get('enabled') or not all(math.isfinite(v) for v in (lo, hi, edge)) or not 0 < lo < hi < 1 or not 0 < edge < 1:
            raise ValueError('invalid shadow candidate')
        if not policy.get('win_rate_tiers') or policy['win_rate_tiers'][-1]['max_price'] < hi:
            raise ValueError('missing price tier')
        return policy
    except (ValueError, KeyError, TypeError):
        return dict(base, enabled=True, valid=False, status='invalid_shadow_candidate')
