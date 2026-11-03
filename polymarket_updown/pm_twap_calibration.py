"""Rolling, chronological TWAP calibration. Pure maths shared by replay/runtime.

No orders, strategy sizing or original trade records are changed here.
"""
import hashlib
import json
import math
import os
import tempfile
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parent
DATA = ROOT / 'analysis/twap_continuous_calibration'
SAMPLES = ROOT / 'reports/twap_calibration_samples.jsonl'
MODEL = ROOT / 'reports/twap_calibration_model.json'
LABELS = ROOT / 'reports/twap_calibration_official_labels.json'
AUDIT = ROOT / 'reports/twap_calibration_audit.jsonl'
VERSION = 'rolling_twap_v3_price_bound'
LOOKBACK_S = 7 * 86400
REFRESH_S = 900
MAX_MODEL_AGE_S = 3600
MIN_TRAIN = 200
MIN_VALIDATION = 40
MIN_VALIDATION_TRADES = 20
RIDGE = 10.0


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile('w', dir=path.parent, delete=False) as f:
        json.dump(value, f, allow_nan=False)
        name = f.name
    os.replace(name, path)


def append_json(path, value):
    with open(path, 'a') as f:
        f.write(json.dumps(value, allow_nan=False) + '\n')


LAST_REFUSAL = {}


def projected_close(anchor, decision_ts, ticks):
    """Wrapper that records why a projection was refused (diagnostics only)."""
    out = _projected_close(anchor, decision_ts, ticks)
    if out is None:
        LAST_REFUSAL.update(_refusal_reason(anchor, decision_ts, ticks))
    else:
        LAST_REFUSAL.clear()
    return out


def _refusal_reason(anchor, decision_ts, ticks):
    start = int(anchor) + 240
    rows = sorted(int(t[0]) for t in ticks if start - 30 < t[0] <= decision_ts)
    if not rows:
        return dict(reason='no_ticks', n=0)
    if rows[0] > start:
        return dict(reason='no_seed', n=len(rows), first_after_open_s=rows[0] - start)
    return dict(reason='stale_or_thin', n=len(rows), newest_age_s=round(decision_ts - rows[-1], 2))


def _projected_close(anchor, decision_ts, ticks):
    """Causal step-function projection; ticks already available at decision_ts.

    Rows are (observation timestamp, price[, collector receipt timestamp]).
    No close/future tick enters the projection. Missing seed/coverage refuses.
    """
    start, end = int(anchor) + 240, int(anchor) + 300
    if not start <= decision_ts < end:
        return None
    rows = sorted((int(t[0]), float(t[1])) for t in ticks
                  if start - 30 < t[0] <= decision_ts
                  and (len(t) < 3 or t[2] is None or t[2] <= decision_ts)
                  and math.isfinite(float(t[1])) and float(t[1]) > 0)
    if not rows or rows[0][0] > start:
        return None
    newest = rows[-1][0]
    if decision_ts - newest > 4:
        return None
    if newest <= start:
        # At T+240, feed latency leaves zero known closing seconds. Project
        # all 60 seconds from the fresh seed, with actual preceding coverage.
        coverage = len({t for t, _ in rows if newest - 20 < t <= newest}) / 20.
        if coverage < .8:
            return None
        return rows[-1][1], newest, min(1., coverage)
    coverage = len({t for t, _ in rows if start <= t <= newest}) / max(1, newest - start)
    if coverage < .8:
        return None
    total, i, current = 0., 0, rows[0][1]
    for stamp in range(start, newest):
        while i < len(rows) and rows[i][0] <= stamp:
            current = rows[i][1]
            i += 1
        total += current
    total += rows[-1][1] * (end - newest)
    return total / 60., newest, min(1., coverage)


def legacy_probability(elapsed, gap):
    table = {240: (.612, .703, .821, .910, .939, .978, .995),
             250: (.630, .848, .880, .948, .971, .993, .998),
             260: (.712, .876, .912, .975, .993, .995, 1.)}
    row = table.get(int(elapsed) // 10 * 10)
    if row is None:
        return None
    idx = sum(abs(gap) >= b for b in (0., .25, .5, 1., 2., 3., 5.)) - 1
    return row[idx]


def features(row):
    ask = max(.01, min(.99, float(row['ask'])))
    return [1., math.log(ask / (1. - ask)),
            math.log1p(min(20., abs(float(row['gap'])))),
            1. if row['side'] == 'Up' else -1.,
            (float(row['elapsed']) - 240.) / 20.]


def sigmoid(x):
    return 1. / (1. + np.exp(-np.clip(x, -25, 25)))


def fit(rows, now):
    x = np.asarray([features(r) for r in rows])
    y = np.asarray([r['won'] for r in rows], dtype=float)
    counts = Counter(r['anchor'] for r in rows)
    weights = np.asarray([2 ** (-(now - r['anchor']) / (3 * 86400)) /
                          counts[r['anchor']] for r in rows])
    # Shrink toward the market quote, not the optimistic old gap table.
    prior = np.asarray([0., 1., 0., 0., 0.])
    beta = prior.copy()
    for _ in range(40):
        p = sigmoid(x @ beta)
        h = x.T @ (x * (weights * p * (1 - p))[:, None]) + RIDGE * np.eye(5)
        g = x.T @ (weights * (p - y)) + RIDGE * (beta - prior)
        step = np.linalg.solve(h, g)
        beta -= step
        if np.max(np.abs(step)) < 1e-7:
            break
    return beta.tolist()


def probability(beta, row):
    return float(sigmoid(np.dot(features(row), beta)))


def logloss(rows, probabilities):
    # Windows, rather than correlated repeated snapshots, carry equal weight.
    counts = Counter(r['anchor'] for r in rows)
    terms = []
    for r, p in zip(rows, probabilities):
        p = max(.001, min(.999, p))
        loss = -math.log(p if r['won'] else 1 - p)
        terms.append(loss / counts[r['anchor']])
    return sum(terms) / len(counts)


def trade_summary(rows, probs, max_ask=.65, fee=.07, extra_cost=.02, policy=None, bucket_windows=None):
    chosen = {}
    rejected = Counter()
    if policy is not None:
        import pm_twap_entry_quality as quality
        from pm_twap_policy import admit
    for r, p in sorted(zip(rows, probs), key=lambda z: z[0]['observed_at']):
        if r['anchor'] in chosen:
            continue
        if policy is not None:
            try:
                bucket = str(int(r['elapsed']) // 10 * 10)
                if bucket_windows is not None and bucket_windows.get(bucket, 0) < 50:
                    raise ValueError('elapsed_bucket_unqualified')
                cap, _ = admit(p, r, policy, quality, fee, extra_cost)
                price = float(r.get('fill_price') or 0)
                fraction = float(r.get('fill_frac') or 0)
                if not math.isfinite(price) or not math.isfinite(fraction) or not 0 < price <= cap + 1e-9 or fraction < .95:
                    raise ValueError('recorded_depth_missing_or_outside_cap')
            except (ValueError, TypeError, KeyError) as exc:
                rejected[str(exc)] += 1
                continue
            chosen[r['anchor']] = r
        else:
            # Compatibility for offline callers without a runtime policy snapshot.
            ask = r['ask']
            if (r.get('fill_frac', 1.) >= .95 and .35 <= ask <= max_ask
                    and p - ask - fee * ask * (1 - ask) - extra_cost > .10):
                chosen.setdefault(r['anchor'], r)
    returns = [float(r['won']) / r.get('fill_price', r['ask']) - 1
               - fee * (1 - r.get('fill_price', r['ask']))
               - extra_cost / r.get('fill_price', r['ask']) for r in chosen.values()]
    mean = float(np.mean(returns)) if returns else 0.
    se = float(np.std(returns, ddof=1) / math.sqrt(len(returns))) if len(returns) > 1 else 0.
    return {'n': len(returns), 'wins': sum(r['won'] for r in chosen.values()),
            'flat_one_pnl': sum(returns), 'mean_return': mean,
            'lower_90': mean - 1.282 * se if len(returns) > 1 else -1.,
            'rejected_samples': dict(rejected)}


def eligible(rows, asof):
    return [r for r in rows if asof - LOOKBACK_S <= r['anchor'] < asof - 420
            and r.get('label_available_at', float('inf')) <= asof
            and r.get('source') == 'projected_twap'
            and r.get('won') in (0, 1)
            and 240 <= r['elapsed'] < 262 and .35 <= r['ask'] <= .90
            and r.get('coverage', 0) >= .8]


def calibrate(rows, asof, max_ask=.65, fee=.07, policy=None):
    usable = eligible(rows, asof)
    split = asof - 86400
    train = [r for r in usable if r['anchor'] < split and r['label_available_at'] <= split]
    validation = [r for r in usable if r['anchor'] >= split]
    nt, nv = len({r['anchor'] for r in train}), len({r['anchor'] for r in validation})
    state = {'version': VERSION, 'trained_at': asof, 'enabled': False,
             'status': 'insufficient_samples', 'train_windows': nt,
             'validation_windows': nv, 'max_ask': max_ask, 'fee': fee,
             'lookback_days': 7, 'validation_days': 1, 'half_life_days': 3,
             'execution_stress_per_share': .02}
    if policy is not None:
        from pm_twap_policy import signature
        state.update(admission_policy=policy, admission_policy_id=signature(policy))
        if not policy.get('valid'):
            state['status'] = 'invalid_admission_policy'
            return state
    if nt < MIN_TRAIN or nv < MIN_VALIDATION:
        return state
    beta = fit(train, split)
    p = [probability(beta, r) for r in validation]
    losses = {'calibrated': logloss(validation, p),
              'market': logloss(validation, [r['ask'] for r in validation]),
              'legacy': logloss(validation, [legacy_probability(r['elapsed'], r['gap']) for r in validation])}
    bucket_windows = {str(b): len({r['anchor'] for r in train
                          if int(r['elapsed']) // 10 * 10 == b}) for b in (240, 250, 260)}
    trades = trade_summary(validation, p, max_ask, fee, policy=policy,
                           bucket_windows=bucket_windows if policy is not None else None)
    state.update(losses=losses, validation_trades=trades,
                 validation_beta=beta, status='no_validated_edge')
    checks = {
        'beats_market': losses['calibrated'] + .005 < losses['market'],
        'beats_legacy': losses['calibrated'] + .005 < losses['legacy'],
        'enough_validation_trades': trades['n'] >= MIN_VALIDATION_TRADES,
        'positive_return_lower_bound': trades['lower_90'] > 0,
    }
    state.update(validation_checks=checks,
                 rejection_reasons=[key for key, ok in checks.items() if not ok],
                 validation_min_edge=max(.05, float(policy.get('shadow_min_edge', .10))) if policy is not None else .10,
                 validation_min_trades=MIN_VALIDATION_TRADES)
    passed = all(checks.values())
    if passed:
        # Once qualified on a later day, fit the live model on all known labels.
        state.update(enabled=True, status='under_forward_evaluation', beta=fit(usable, asof),
                     bucket_windows={str(b): len({r['anchor'] for r in usable
                                                  if int(r['elapsed']) // 10 * 10 == b})
                                     for b in (240, 250, 260)})
    state['model_id'] = hashlib.sha256(json.dumps(state, sort_keys=True).encode()).hexdigest()[:16]
    return state


def predict(state, row, now):
    if not state or not state.get('enabled'):
        return None, (state or {}).get('status', 'calibration_pending')
    if not 0 <= now - state['trained_at'] <= MAX_MODEL_AGE_S:
        return None, 'calibration_stale'
    if state.get('version') != VERSION:
        return None, 'calibration_version_mismatch'
    if state.get('admission_policy_id'):
        from pm_twap_policy import signature
        if signature(row.get('quality_policy') or {}) != state['admission_policy_id']:
            return None, 'calibration_policy_mismatch'
    bucket = str(int(row['elapsed']) // 10 * 10)
    if state.get('bucket_windows', {}).get(bucket, 0) < 50:
        return None, 'elapsed_bucket_unqualified'
    if row.get('source') != 'projected_twap' or row.get('coverage', 0) < .8:
        return None, 'invalid_twap_estimate'
    if not 240 <= row['elapsed'] < 262 or not .35 <= row['ask'] <= .90:
        return None, 'outside_calibrated_support'
    return probability(state['beta'], row), 'calibrated'


def read_json(path, default):
    try:
        return json.loads(Path(path).read_text())
    except (OSError, ValueError):
        return default


def read_samples():
    historical = read_json(DATA / 'training_samples.json', [])
    try:
        with open(SAMPLES) as f:
            for line in f:
                try:
                    historical.append(json.loads(line))
                except ValueError:
                    continue
    except FileNotFoundError:
        pass
    # A restart must not multiply the weight of the same sample bucket.
    unique = {(r['anchor'], int(r['elapsed']) // 10): r for r in historical}
    return list(unique.values())


def refresh(reference_reader, max_ask=.65, fee=.07, policy=None):
    now = time.time()
    rows = [r for r in read_samples() if now - LOOKBACK_S <= r['anchor'] < now]
    labels = read_json(LABELS, {})
    pending = sorted({r['anchor'] for r in rows if r['anchor'] + 420 < now
                      and 'won' not in r and str(r['anchor']) not in labels}, reverse=True)[:48]
    def lookup(anchor):
        ref = reference_reader(anchor)
        if ref['price_open'] is not None and ref['price_settle'] is not None:
            return str(anchor), dict(ref, label_available_at=time.time())
        return None
    with ThreadPoolExecutor(max_workers=4) as pool:
        for result in pool.map(lookup, pending):
            if result:
                labels[result[0]] = result[1]
    atomic_json(LABELS, labels)
    labelled = []
    for row in rows:
        label = labels.get(str(row['anchor']))
        if label:
            # Refuse training if the recorded opening reference isn't official.
            if abs(row['price_open'] - label['price_open']) > 1e-6:
                continue
            outcome = 'Up' if label['price_settle'] >= label['price_open'] else 'Down'
            row = dict(row, won=int(row['side'] == outcome),
                       label_available_at=label['label_available_at'])
        labelled.append(row)
    state = calibrate(labelled, now, max_ask, fee, policy)
    state['labelled_samples'] = len(eligible(labelled, now))
    atomic_json(MODEL, state)
    append_json(AUDIT, state)
    return state
