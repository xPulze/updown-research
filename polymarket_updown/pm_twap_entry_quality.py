"""TWAP projected-gap/spread admission policy; forward evaluation only."""
import json
import math
from pathlib import Path

PATH = Path(__file__).resolve().parent / 'params/twap_entry_quality.json'


def load(path=PATH):
    try:
        policy = json.loads(Path(path).read_text())
        if not isinstance(policy.get('enabled'), bool):
            raise ValueError('enabled must be boolean')
        for key in ['min_gap_bps', 'max_spread']:
            if isinstance(policy[key], bool) or not math.isfinite(float(policy[key])):
                raise ValueError('invalid ' + key)
        if not 0 <= float(policy['min_gap_bps']) <= 10 or not 0 < float(policy['max_spread']) <= .10:
            raise ValueError('threshold outside supported range')
        previous_price, previous_wr = 0., 0.
        for tier in policy.get('win_rate_tiers', []):
            price, wr = tier['max_price'], tier['min_win_rate']
            if (isinstance(price, bool) or isinstance(wr, bool)
                    or not previous_price < float(price) < 1
                    or not previous_wr <= float(wr) < 1):
                raise ValueError('invalid win-rate tier')
            previous_price, previous_wr = float(price), float(wr)
        return dict(policy, valid=True)
    except FileNotFoundError:
        return dict(enabled=False, valid=True, status='not_configured')
    except (OSError, ValueError, KeyError, TypeError):
        return dict(enabled=True, valid=False, status='invalid_policy')


def evaluate(policy, gap_bps, ask, bid):
    if not policy.get('enabled'):
        return True, 'quality_off'
    if not policy.get('valid'):
        return False, 'quality_policy_invalid'
    try:
        gap, ask, bid = float(gap_bps), float(ask), float(bid)
    except (TypeError, ValueError):
        return False, 'quality_quote_missing'
    if not all(math.isfinite(x) for x in [gap, ask, bid]) or not 0 < bid <= ask < 1:
        return False, 'quality_quote_invalid'
    if abs(gap) + 1e-9 < float(policy['min_gap_bps']):
        return False, 'quality_gap_small'
    if ask - bid > float(policy['max_spread']) + 1e-9:
        return False, 'quality_spread_wide'
    return True, 'quality_pass'


def best_bid(levels):
    bids = [float(p) for p, size in levels[0]
            if math.isfinite(float(p)) and 0 < float(p) < 1 and float(size) > 0] if levels else []
    return max(bids) if bids else None


def evaluate_probability(policy, probability, max_buy_price, fee_rate):
    """Use the worst authorized fill price, including a tier crossed by the cap."""
    if not policy.get('enabled') or not policy.get('win_rate_tiers'):
        return True, 'tier_off', {}
    if not policy.get('valid'):
        return False, 'quality_policy_invalid', {}
    try:
        p, price, fee = float(probability), float(max_buy_price), float(fee_rate)
        if not all(math.isfinite(x) for x in (p, price, fee)) or not (0 <= p <= 1 and 0 < price < 1 and fee >= 0):
            raise ValueError('invalid probability or price')
    except (TypeError, ValueError):
        return False, 'tier_probability_invalid', {}
    for tier in policy['win_rate_tiers']:
        if price <= float(tier['max_price']) + 1e-9:
            minimum = float(tier['min_win_rate'])
            break_even = price + fee * price * (1 - price)
            details = dict(min_win_rate=minimum, max_buy_price=price,
                           tier_max_price=float(tier['max_price']), break_even_win_rate=break_even)
            if p + 1e-12 < minimum or p <= break_even:
                return False, 'tier_win_rate_below_minimum', details
            return True, 'tier_pass', details
    return False, 'tier_price_unsupported', {}
