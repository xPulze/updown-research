"""Expiring per-asset swingfade overlay; no trading or configuration writes on import."""
import json
import math
import time
from pathlib import Path

CONFIG_PATH = Path(__file__).resolve().parent / 'reports/btc_swingfade_trial.json'
ETH_CONFIG_PATH = Path(__file__).resolve().parent / 'reports/eth_swingfade_trial.json'


def config_path(asset):
    return {'BTC': CONFIG_PATH, 'ETH': ETH_CONFIG_PATH}.get(asset)


def active_trial(asset, now=None):
    path = config_path(asset)
    if path is None:
        return None
    now = time.time() if now is None else now
    try:
        cfg = json.loads(path.read_text())
        if cfg['starts_at'] <= now < cfg['expires_at']:
            return cfg
    except (OSError, ValueError, KeyError, TypeError):
        pass
    return None


def trial_band(asset, default_lo, default_hi, now=None):
    cfg = active_trial(asset, now)
    return (0.15, 0.17) if cfg else (default_lo, default_hi)


def evaluate_moves(rows, side, end):
    """Compare completed 5s mean-price buckets, exactly 10s apart."""
    if side not in ('Up', 'Down'):
        return False, 'trial: invalid fade side'
    sign = 1 if side == 'Up' else -1
    values = {(r['source'], int(r['bucket'])): r['price'] for r in rows}
    moves = {}
    for source in ('pm', 'kraken'):
        old, new = values.get((source, end - 10)), values.get((source, end))
        if any(v is None or not math.isfinite(float(v)) or v <= 0 for v in (old, new)):
            return False, f'trial: {source} missing/stale 10s buckets'
        moves[source] = sign * (new / old - 1) * 10000
    ok = all(v > 0 for v in moves.values())
    return ok, 'trial both10 ' + ' '.join(f'{s}={v:+.4f}bps' for s, v in moves.items())


def confirmation(engine, asset, side, now=None):
    if not active_trial(asset, now):
        return True, 'trial inactive'
    from sqlalchemy import text
    now = time.time() if now is None else now
    end = ((int(now) - 4) // 5) * 5 + 4
    try:
        with engine.connect() as c:
            rows = c.execute(text('''
                SELECT 'pm' AS source, (observations_ts/5)*5+4 AS bucket,
                       avg(benchmark_price) AS price
                FROM chainlink_prices
                WHERE feed_id=:feed_id
                  AND ((observations_ts BETWEEN :lo AND :hi)
                    OR (observations_ts BETWEEN :oldlo AND :oldhi))
                GROUP BY 2
                UNION ALL
                SELECT 'kraken' AS source, (observations_ts/5)*5+4 AS bucket,
                       avg(price) AS price
                FROM pm_reference_ticks
                WHERE source='kraken' AND asset=:asset
                  AND ((observations_ts BETWEEN :lo AND :hi)
                    OR (observations_ts BETWEEN :oldlo AND :oldhi))
                GROUP BY 2
            '''), {'lo': end-4, 'hi': end, 'oldlo': end-14, 'oldhi': end-10,
                   'feed_id': asset.lower() + '/usd', 'asset': asset}).mappings().all()
        return evaluate_moves(rows, side, end)
    except Exception as exc:
        return False, f'trial feed read failed: {type(exc).__name__}'


def trial_expired(asset, now=None):
    """Fail closed after the trial even if the scheduled toggle restore is delayed."""
    path = config_path(asset)
    if path is None:
        return False
    try:
        cfg = json.loads(path.read_text())
        return (time.time() if now is None else now) >= cfg['expires_at']
    except (OSError, ValueError, KeyError, TypeError):
        return False


def trial_blocks_live(asset, now=None):
    cfg = active_trial(asset, now)
    return bool(cfg and cfg.get('shadow_only')) or trial_expired(asset, now)
