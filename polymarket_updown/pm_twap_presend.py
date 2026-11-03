"""Fresh TWAP EDGE admission for shadow and funded entry; never submits orders."""
import asyncio
import math
import time


def valid_levels(levels):
    bids, asks = levels
    def clean(rows):
        return [(float(p), float(s)) for p, s in rows
                if math.isfinite(float(p)) and math.isfinite(float(s))
                and 0 < float(p) < 1 and float(s) > 0]
    bids, asks = clean(bids), clean(asks)
    if not bids or not asks or max(p for p,s in bids) > min(p for p,s in asks):
        raise ValueError('missing or crossed depth')
    return bids, asks


async def fresh_depth(bot, token, module):
    feed = getattr(bot, 'bookfeed', None)
    book = getattr(feed, '_books', {}).get(str(token), {})
    task = getattr(feed, '_task', None)
    now = time.time()
    age = now - book.get('ts', 0)
    if task is not None and not task.done() and 0 <= age <= .100:
        levels = feed.levels(token)
        if levels is not None:
            try:
                return valid_levels(levels), dict(source='ws', received_at=book['ts'], age_ms=age*1000)
            except (TypeError, ValueError):
                pass
    started = time.time()
    response = await asyncio.wait_for(bot.client.get_order_book(token_id=str(token)), timeout=1.)
    received = time.time()
    # A slow request cannot authorize a late-window entry.
    if received-started > .5:
        raise ValueError('REST quote request too slow')
    return valid_levels(module._levels(response)), dict(source='rest', received_at=received,
            age_ms=0., request_ms=(received-started)*1000)


def edge_cap(probability, ask, policy, quality, fee=.07, minimum=.05, ceiling=.85, cost=0.):
    """Highest valid limit, bounded to at most one cent above the fresh ask."""
    values = (probability, ask, fee, minimum, ceiling, cost)
    if not all(math.isfinite(float(v)) for v in values) or not 0 <= probability <= 1 or not 0 < ask <= ceiling < 1:
        raise ValueError('invalid probability/price bounds')
    slippage = float(policy.get('execution_max_slippage', .01))
    if not math.isfinite(slippage) or not 0 <= slippage <= .01:
        raise ValueError('invalid execution slippage bound')
    ceiling = min(ceiling, ask + slippage)
    for cents in range(int(math.floor(ceiling*100+1e-9)), 0, -1):
        cap = cents/100
        if cap+1e-9 < ask:
            break
        ok, _, details = quality.evaluate_probability(policy, probability, cap, fee)
        if ok and probability-cap-fee*cap*(1-cap)-cost > minimum+1e-9:
            return cap, details
    raise ValueError('no price preserves required fee-net edge and tier')


async def prepare(bot, window, side, token, policy, module):
    started = time.time()
    estimate = await asyncio.to_thread(module.twap_lock_estimate, window['window_anchor'])
    if estimate is None:
        raise ValueError('TWAP estimate stale or thin')
    est, feed_ts, coverage = estimate
    opening = float(window['price_open'])
    if not all(math.isfinite(float(v)) for v in (est, opening, coverage)) or opening <= 0 or est <= 0:
        raise ValueError('invalid TWAP estimate')
    gap = (est/opening-1)*10000
    if ('Up' if gap >= 0 else 'Down') != side:
        raise ValueError('TWAP direction reversed')
    levels, quote = await fresh_depth(bot, token, module)
    now = time.time(); elapsed = now-window['window_anchor']
    if not module.TWAP_EDGE_T0_S <= elapsed < module.TWAP_EDGE_T1_S:
        raise ValueError('TWAP entry window expired')
    if now-started > 1. or now-float(feed_ts) > 8 or float(feed_ts) > now:
        raise ValueError('TWAP pre-send signal stale')
    bids, asks = levels
    ask, bid = min(p for p,s in asks), max(p for p,s in bids)
    ok, why = module.twap_quality.evaluate(policy, gap, ask, bid)
    if not ok:
        raise ValueError(why)
    minimum = max(.05, float(policy.get('shadow_min_edge', module.TWAP_EDGE_MIN_EDGE)))
    ceiling = min(.85, float(policy.get('shadow_max_ask', window.get('twap_edge_max_px', module.TWAP_EDGE_MAX_PX))))
    floor = float(policy.get('shadow_min_ask', module.TWAP_EDGE_MIN_PX))
    if ask < floor:
        raise ValueError('TWAP ask below policy floor')
    sample = dict(window.get('twap_calibration') or {}, anchor=window['window_anchor'],
                  source='projected_twap', quality_policy=policy,
                  elapsed=elapsed, gap=gap, estimate=est,
                  coverage=coverage, feed_ts=feed_ts, ask=ask, quality_bid=bid,
                  observed_at=now, quote_source=quote['source'], quote_age_ms=quote['age_ms'])
    probability, status, enforced = admission_probability(bot, module, sample, now)
    if probability is None:
        raise ValueError('TWAP probability unavailable: '+status)
    from pm_twap_policy import admit
    # Supply resolved defaults so historical validation and runtime share the rules.
    resolved = dict(policy, shadow_min_ask=floor, shadow_max_ask=ceiling,
                    shadow_min_edge=minimum, entry_t0_s=module.TWAP_EDGE_T0_S,
                    entry_t1_s=module.TWAP_EDGE_T1_S)
    cap, tier = admit(probability, sample, resolved, module.twap_quality,
                      module.POLY_FEE_RATE, .02 if enforced else 0.)
    sample.update(probability=probability, calibration_status=status,
                  calibration_enforced=enforced,
                  calibrated_probability=probability if enforced else sample.get('calibrated_probability'),
                  win_rate_tier=tier, presend_at=now,
                  max_price=cap, fee_net_edge_at_cap=probability-cap-module.POLY_FEE_RATE*cap*(1-cap)-(.02 if enforced else 0.))
    return dict(ask=ask, cap=cap, asks=asks, gap=gap, estimate=est, elapsed=elapsed,
                sample=sample, quote=quote, probability=probability)


async def final_check(bot, window, side, token, policy, module, prepared, stake):
    """After signing: approve only the unchanged signed limit, or abort the POST."""
    refreshed = await prepare(bot, window, side, token, policy, module)
    now = time.time()
    signed_cap = float(prepared['cap'])
    if now - float(prepared['sample']['observed_at']) > 1.:
        raise ValueError('TWAP signed decision expired')
    if signed_cap > refreshed['cap'] + 1e-9 or refreshed['ask'] > signed_cap + 1e-9:
        raise ValueError('TWAP signed cap outside refreshed price/edge bounds')
    if not 0 <= now - refreshed['quote']['received_at'] <= .100:
        raise ValueError('TWAP final quote stale')
    _, _, vwap, fraction = module.sweep_ladder(refreshed['asks'], stake, signed_cap)
    if vwap is None or not math.isfinite(float(vwap)) or not math.isfinite(float(fraction)) or fraction < .95:
        raise ValueError('TWAP final depth insufficient at signed cap')
    return dict(checked_at=now, ask=refreshed['ask'], signed_cap=signed_cap,
                probability=refreshed['probability'], gap=refreshed['gap'],
                feed_ts=refreshed['sample']['feed_ts'],
                quote_received_at=refreshed['quote']['received_at'],
                quote_source=refreshed['quote']['source'],
                fill_fraction=fraction, vwap=vwap,
                calibration_status=refreshed['sample']['calibration_status'])


def admission_probability(bot, module, sample, now):
    """Funded bots must qualify even when shadow calibration enforcement is off."""
    funded = bool(getattr(bot, 'live', False))
    enabled = module.TWAP_CONTINUOUS_CALIBRATION
    enforced = funded or (enabled and module.TWAP_CALIBRATION_ENFORCE)
    if enforced:
        if not enabled:
            raise ValueError('TWAP funded entry requires continuous calibration')
        probability, status = module.twap_cal.predict(
            getattr(bot, '_twap_model', {}), sample, now)
        if probability is None:
            raise ValueError('TWAP probability unavailable: ' + status)
    else:
        probability, status = module.twap_edge_p(sample['elapsed'], abs(sample['gap'])), 'legacy'
    return probability, status, enforced
