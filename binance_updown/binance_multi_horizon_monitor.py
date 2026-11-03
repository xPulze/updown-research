"""
Phase 1 read-only monitor for Binance BTCUSDT "Higher/Lower" event contracts
at horizons OTHER than 5-min (currently: 10-min and 15-min), one process per
horizon (run with --horizon 10 or --horizon 15).

Extends the validated 5-min approach (scripts/binance_5min_stake_alert_monitor.py,
T+3 decision / T+5 expiry) using the delay-calibration analysis in
scripts/predict_10_15min_delay_calibration.py and
MEMORY project_binance_10_15min_delay_calibration.md: accuracy at a decision
point is a function of the FRACTION of the window elapsed (matches the
Brownian-motion arcsine law almost exactly across 5/10/15-min), so the same
60%-elapsed rule that validated T+3-of-5 generalizes to T+6-of-10 and
T+9-of-15.

Deliberately MUCH simpler than the 5-min monitor: no stake sizing, no Kelly,
no payout curve, no ADX overlay, no NY-open tightening, no early/dwell/late/
value legs. None of that is validated yet for these horizons (in particular,
the 5-min payout curve MARKET_Z_KNOTS took days of real app screenshots to
fit, and no equivalent exists yet for 10/15-min). This script only tracks:
  - price at window open (T)
  - price at the decision point (T+D) -> partial move (bps), tier, side
  - price at expiry (T+H) -> actual outcome, correct/wrong

Advisory only. No orders placed. Manual staking in the Binance app.
"""
import argparse
import json
import os
import sys
import time
from collections import deque
from datetime import datetime, timedelta, timezone

import requests
import psycopg2

sys.path.insert(0, ".")
import config as _C

SYMBOL = "BTCUSDT"
BASE = "https://api.binance.com"
POLL_SECONDS = 3
TICK_BUFFER_MINUTES = 20
STATE_DIR = "./reports"

# Decision offset = 60% of the window elapsed, validated against 45 days of
# 1-min candles in predict_10_15min_delay_calibration.py (78.1%/78.8% overall
# baseline accuracy for 10/15-min respectively, matching the already-live
# 5-min product's T+3/5 = 76.9%). Not assumed — this is the empirically (and
# theoretically, via the arcsine law) validated fraction, re-derived per
# horizon rather than hardcoded as "always 3 minutes".
DECISION_FRACTION = 0.6

# Tier floors (bps) x backtest accuracy at each horizon's decision point,
# taken directly from predicted_10_15min_delay_calibration.xlsx
# (h10_d6_tiers / h15_d9_tiers sheets). Same floor scheme (2/4/6/10/20/30) as
# the 5-min monitor's TIERS list for cross-dashboard consistency. Where two
# adjacent raw buckets inverted (noise, thin samples e.g. n=355-545), the
# LOWER (more conservative) of the two accuracies was kept for the merged
# floor rather than averaging or assuming monotonicity.
TIERS_BY_HORIZON = {
    10: [
        (30.0, "EXTREME", 0.984),
        (20.0, "VERY STRONG", 0.966),
        (10.0, "STRONG", 0.897),       # conservative: min(10-15=.913, 15-20=.897)
        (6.0, "MODERATE-STRONG", 0.813),  # conservative: min(6-8=.813, 8-10=.831)
        (4.0, "MODERATE", 0.741),
        (2.0, "WEAK", 0.660),
    ],
    15: [
        (30.0, "EXTREME", 0.966),
        (20.0, "VERY STRONG", 0.937),
        (10.0, "STRONG", 0.860),       # conservative: min(10-15=.860, 15-20=.906)
        (6.0, "MODERATE-STRONG", 0.783),  # conservative: min(6-8=.804, 8-10=.783)
        (4.0, "MODERATE", 0.738),
        (2.0, "WEAK", 0.628),
    ],
}

# No-stake floor (bps), re-derived (not assumed) per horizon on the same
# 45-day 1-min candle history as TIERS_BY_HORIZON above (2026-07-08 analysis,
# see MEMORY project_binance_10_15min_delay_calibration.md): WEAK (2-4bps)
# and MODERATE (4-6bps) both sit well below the MODERATE-STRONG+ population
# on BOTH horizons -- 10-min cumulative accuracy jumps 78.1%->89.3% at the
# >=6bps floor (54.3% of windows kept), 15-min jumps 78.8%->87.6% (63.3%
# kept). Same floor as the live 5-min product's WEAK+MODERATE no-stake rule
# (see binance_5min_stake_alert_monitor.py NO_STAKE_TIERS). Advisory only --
# Phase 1 has no real stake sizing yet -- but flagged so the dashboard can
# show a NO STAKE capsule consistent with the 5-min page.
NO_STAKE_FLOOR_BPS = 6.0

TABLE_NAME = "binance_multi_horizon_windows"

_PG_CONN = None


def _pg_conn():
    global _PG_CONN
    try:
        if _PG_CONN is None or _PG_CONN.closed:
            _PG_CONN = psycopg2.connect(
                host=_C.DB_HOST, dbname=_C.DB_NAME, user=_C.DB_USER, password=_C.DB_PASSWORD
            )
        return _PG_CONN
    except Exception as e:
        print(f"[{datetime.now(timezone.utc).isoformat()}] DB connect failed: {e}", flush=True)
        return None


def ensure_table():
    pg = _pg_conn()
    if pg is None:
        return
    with pg.cursor() as cur:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                horizon_min         INTEGER NOT NULL,
                anchor              TIMESTAMPTZ NOT NULL,
                decision_offset_min INTEGER,
                price_open          DOUBLE PRECISION,
                price_decision      DOUBLE PRECISION,
                bps_decision        DOUBLE PRECISION,
                side                TEXT,
                tier                TEXT,
                tier_backtest_acc   DOUBLE PRECISION,
                decided_at          TIMESTAMPTZ,
                price_expiry        DOUBLE PRECISION,
                actual_dir          TEXT,
                correct             TEXT,
                status              TEXT NOT NULL DEFAULT 'PENDING',
                created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
                PRIMARY KEY (horizon_min, anchor)
            );
        """)
    pg.commit()


def upsert_window(w):
    """Best-effort upsert of the full window row. Never raises."""
    pg = _pg_conn()
    if pg is None:
        return
    try:
        with pg.cursor() as cur:
            cur.execute(f"""
                INSERT INTO {TABLE_NAME} (
                    horizon_min, anchor, decision_offset_min, price_open,
                    price_decision, bps_decision, side, tier, tier_backtest_acc,
                    decided_at, price_expiry, actual_dir, correct, status
                ) VALUES (
                    %(horizon_min)s, %(anchor)s, %(decision_offset_min)s, %(price_open)s,
                    %(price_decision)s, %(bps_decision)s, %(side)s, %(tier)s, %(tier_acc)s,
                    %(decided_at)s, %(price_expiry)s, %(actual_dir)s, %(correct)s, %(status)s
                )
                ON CONFLICT (horizon_min, anchor) DO UPDATE SET
                    price_decision = EXCLUDED.price_decision,
                    bps_decision = EXCLUDED.bps_decision,
                    side = EXCLUDED.side, tier = EXCLUDED.tier,
                    tier_backtest_acc = EXCLUDED.tier_backtest_acc,
                    decided_at = EXCLUDED.decided_at,
                    price_expiry = EXCLUDED.price_expiry,
                    actual_dir = EXCLUDED.actual_dir,
                    correct = EXCLUDED.correct,
                    status = EXCLUDED.status;
            """, {
                "horizon_min": w["horizon_min"], "anchor": w["anchor"],
                "decision_offset_min": w["decision_offset_min"], "price_open": w["price_open"],
                "price_decision": w["price_decision"], "bps_decision": w["bps_decision"],
                "side": w["side"], "tier": w["tier"], "tier_acc": w["tier_acc"],
                "decided_at": w["decided_at"], "price_expiry": w["price_expiry"],
                "actual_dir": w["actual_dir"], "correct": w["correct"], "status": w["status"],
            })
        pg.commit()
    except Exception as e:
        print(f"[{datetime.now(timezone.utc).isoformat()}] upsert failed for {w.get('anchor')}: {e}", flush=True)


def recover_pending_from_db(horizon_min):
    """On restart, reload any window still short of EXPIRED so a restart
    landing between decision and expiry doesn't silently drop the outcome."""
    pg = _pg_conn()
    windows = {}
    if pg is None:
        return windows
    try:
        with pg.cursor() as cur:
            cur.execute(f"""
                SELECT anchor, decision_offset_min, price_open, price_decision, bps_decision,
                       side, tier, tier_backtest_acc, decided_at, status
                FROM {TABLE_NAME}
                WHERE horizon_min = %s AND status != 'EXPIRED'
                  AND anchor > now() - interval '1 hour'
            """, (horizon_min,))
            rows = cur.fetchall()
        for (anchor, doff, price_open, price_decision, bps_decision, side, tier, tier_acc,
             decided_at, status) in rows:
            windows[anchor.isoformat()] = {
                "horizon_min": horizon_min, "anchor": anchor, "decision_offset_min": doff,
                "price_open": price_open, "price_decision": price_decision,
                "bps_decision": bps_decision, "side": side, "tier": tier, "tier_acc": tier_acc,
                "decided_at": decided_at, "price_expiry": None, "actual_dir": None,
                "correct": None, "status": status,
            }
    except Exception as e:
        print(f"[{datetime.now(timezone.utc).isoformat()}] recover failed: {e}", flush=True)
    return windows


def get_stats(horizon_min, hours=None):
    pg = _pg_conn()
    if pg is None:
        return {"n_decided": 0, "n_correct": 0, "n_wrong": 0, "accuracy_pct": None}
    try:
        clause = "AND anchor > now() - interval %(h)s" if hours else ""
        params = {"horizon": horizon_min}
        if hours:
            params["h"] = f"{hours} hours"
        with pg.cursor() as cur:
            cur.execute(f"""
                SELECT correct, COUNT(*) FROM {TABLE_NAME}
                WHERE horizon_min = %(horizon)s AND status = 'EXPIRED' AND correct IS NOT NULL {clause}
                GROUP BY correct
            """, params)
            rows = dict(cur.fetchall())
        n_correct = rows.get("CORRECT", 0)
        n_wrong = rows.get("WRONG", 0)
        n_tie = rows.get("TIE", 0)
        n_decided = n_correct + n_wrong + n_tie
        return {
            "n_decided": n_decided, "n_correct": n_correct, "n_wrong": n_wrong,
            "accuracy_pct": round(100.0 * n_correct / (n_correct + n_wrong), 1) if (n_correct + n_wrong) else None,
        }
    except Exception as e:
        print(f"[{datetime.now(timezone.utc).isoformat()}] stats query failed: {e}", flush=True)
        return {"n_decided": 0, "n_correct": 0, "n_wrong": 0, "accuracy_pct": None}


def tier_for(abs_bps, tiers):
    for floor, label, acc in tiers:
        if abs_bps >= floor:
            return label, acc
    return None, None


def get_price():
    r = requests.get(f"{BASE}/api/v3/ticker/price", params={"symbol": SYMBOL}, timeout=5)
    r.raise_for_status()
    return float(r.json()["price"])


def get_klines_1m(limit=100):
    """1-min OHLC klines from Binance's public REST (same call the 5-min
    monitor uses) — horizon-independent, feeds the dashboard's 1-min candle
    chart for every product."""
    r = requests.get(f"{BASE}/api/v3/klines",
                      params={"symbol": SYMBOL, "interval": "1m", "limit": limit}, timeout=5)
    r.raise_for_status()
    rows = r.json()
    return [{"t": int(row[0]), "o": float(row[1]), "h": float(row[2]),
             "l": float(row[3]), "c": float(row[4])} for row in rows]


def get_hourly_range_24h():
    """Last 24 fully-CLOSED 1-hour candles (drops the still-forming current
    hour) — feeds the dashboard's Master 24h Range / Price Range cards.
    Horizon-independent, identical to the 5-min monitor's version."""
    r = requests.get(f"{BASE}/api/v3/klines",
                      params={"symbol": SYMBOL, "interval": "1h", "limit": 25}, timeout=5)
    r.raise_for_status()
    rows = r.json()
    closed = rows[:-1][-24:]
    return [{"t": int(row[0]), "o": float(row[1]), "h": float(row[2]),
             "l": float(row[3]), "c": float(row[4])} for row in closed]


def floor_to_n_min(t, n):
    minute_bucket = (t.minute // n) * n
    return t.replace(minute=minute_bucket, second=0, microsecond=0)


def write_state(path, horizon_min, decision_offset_min, ticks, windows, cumulative, recent_24h,
                candles_1m, hourly_range_24h):
    ordered = sorted(windows.values(), key=lambda w: w["anchor"], reverse=True)[:60]
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "symbol": SYMBOL,
        "horizon_min": horizon_min,
        "candles_1m": candles_1m,
        "hourly_range_24h": hourly_range_24h,
        "decision_offset_min": decision_offset_min,
        "ticks": list(ticks),
        "windows": [{
            "anchor": w["anchor"].isoformat() if hasattr(w["anchor"], "isoformat") else w["anchor"],
            "price_open": w["price_open"],
            "price_decision": w["price_decision"],
            "bps_decision": round(w["bps_decision"], 2) if w["bps_decision"] is not None else None,
            "side": w["side"], "tier": w["tier"], "tier_backtest_acc": w["tier_acc"],
            "no_stake": (abs(w["bps_decision"]) < NO_STAKE_FLOOR_BPS) if w["bps_decision"] is not None else None,
            "price_expiry": w["price_expiry"], "actual_dir": w["actual_dir"],
            "correct": w["correct"], "status": w["status"],
        } for w in ordered],
        "cumulative": cumulative,
        "recent_24h": recent_24h,
    }
    tmp_path = path + ".tmp"
    with open(tmp_path, "w") as f:
        json.dump(payload, f)
    os.replace(tmp_path, path)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--horizon", type=int, required=True, choices=[10, 15])
    args = parser.parse_args()

    horizon_min = args.horizon
    decision_offset_min = round(horizon_min * DECISION_FRACTION)
    tiers = TIERS_BY_HORIZON[horizon_min]
    state_path = f"{STATE_DIR}/binance_{horizon_min}min_live_state.json"

    print(f"[{datetime.now(timezone.utc).isoformat()}] Multi-horizon monitor started — "
          f"{SYMBOL}, horizon={horizon_min}min, decision at T+{decision_offset_min} "
          f"({DECISION_FRACTION*100:.0f}% elapsed), expiry T+{horizon_min}. "
          f"Phase 1: directional accuracy only, no stake sizing. State: {state_path}", flush=True)

    ensure_table()
    windows = recover_pending_from_db(horizon_min)

    tick_buffer_len = int(TICK_BUFFER_MINUTES * 60 / POLL_SECONDS)
    ticks = deque(maxlen=tick_buffer_len)

    last_stats_refresh = 0
    cumulative = get_stats(horizon_min)
    recent_24h = get_stats(horizon_min, hours=24)

    # 1-min candle chart (60-min rolling window, same construction as the
    # 5-min monitor's — horizon-independent, just BTC price bars) and the
    # 24h hourly range cards. Refreshed on their own slower cadences.
    CANDLE_WINDOW_MIN = 60
    closed_candles_1m = []
    live_candle = None
    live_candle_minute_ms = None
    KLINE_RESYNC_SECONDS = 20
    last_kline_resync = 0
    HOURLY_REFRESH_SECONDS = 60
    last_hourly_refresh = 0
    hourly_range_24h = []

    prev_anchor = None

    while True:
        try:
            now = datetime.now(timezone.utc)
            price = get_price()
            ticks.append({"t": now.isoformat(), "p": price})

            minute_start = now.replace(second=0, microsecond=0)
            minute_ms = int(minute_start.timestamp() * 1000)
            if live_candle_minute_ms != minute_ms:
                if live_candle is not None:
                    closed_candles_1m.append(live_candle)
                    closed_candles_1m = closed_candles_1m[-(CANDLE_WINDOW_MIN * 2):]
                live_candle = {"t": minute_ms, "o": price, "h": price, "l": price, "c": price}
                live_candle_minute_ms = minute_ms
            else:
                live_candle["h"] = max(live_candle["h"], price)
                live_candle["l"] = min(live_candle["l"], price)
                live_candle["c"] = price

            if time.time() - last_kline_resync > KLINE_RESYNC_SECONDS:
                try:
                    fetched = get_klines_1m(limit=CANDLE_WINDOW_MIN + 5)
                    closed_candles_1m = [c for c in fetched if c["t"] < live_candle_minute_ms]
                except Exception as e:
                    print(f"[{now.isoformat()}] kline resync failed: {e}", flush=True)
                last_kline_resync = time.time()

            if time.time() - last_hourly_refresh > HOURLY_REFRESH_SECONDS:
                try:
                    hourly_range_24h = get_hourly_range_24h()
                except Exception as e:
                    print(f"[{now.isoformat()}] hourly range refresh failed: {e}", flush=True)
                last_hourly_refresh = time.time()

            anchor_dt = floor_to_n_min(now, horizon_min)
            anchor_key = anchor_dt.isoformat()

            if anchor_key not in windows and (prev_anchor is None or anchor_dt != prev_anchor):
                windows[anchor_key] = {
                    "horizon_min": horizon_min, "anchor": anchor_dt,
                    "decision_offset_min": decision_offset_min,
                    "price_open": price, "price_decision": None, "bps_decision": None,
                    "side": None, "tier": None, "tier_acc": None, "decided_at": None,
                    "price_expiry": None, "actual_dir": None, "correct": None,
                    "status": "PENDING",
                }
                prev_anchor = anchor_dt

            for key, w in list(windows.items()):
                w_anchor = w["anchor"] if hasattr(w["anchor"], "tzinfo") else datetime.fromisoformat(w["anchor"])
                decision_time = w_anchor + timedelta(minutes=w["decision_offset_min"])
                expiry_time = w_anchor + timedelta(minutes=w["horizon_min"])

                if w["status"] == "PENDING" and now >= decision_time:
                    bps = (price - w["price_open"]) / w["price_open"] * 10000.0
                    side = "HIGHER" if bps > 0 else ("LOWER" if bps < 0 else None)
                    tier, tier_acc = tier_for(abs(bps), tiers) if side else (None, None)
                    w.update({
                        "price_decision": price, "bps_decision": bps, "side": side,
                        "tier": tier, "tier_acc": tier_acc, "decided_at": now,
                        "status": "DECIDED",
                    })
                    upsert_window(w)
                    print(f"[{now.isoformat()}] T+{w['decision_offset_min']} DECISION "
                          f"anchor={key} bps={bps:+.2f} side={side} tier={tier}", flush=True)

                if w["status"] in ("PENDING", "DECIDED") and now >= expiry_time:
                    actual_dir = "HIGHER" if price > w["price_open"] else ("LOWER" if price < w["price_open"] else "TIE")
                    if w["status"] == "DECIDED" and w["side"]:
                        correct = "TIE" if actual_dir == "TIE" else ("CORRECT" if actual_dir == w["side"] else "WRONG")
                    else:
                        correct = None
                    w.update({
                        "price_expiry": price, "actual_dir": actual_dir, "correct": correct,
                        "status": "EXPIRED" if w["status"] == "DECIDED" else "INCOMPLETE",
                    })
                    upsert_window(w)
                    print(f"[{now.isoformat()}] EXPIRY anchor={key} actual={actual_dir} "
                          f"result={correct}", flush=True)

            # prune windows older than 2h from memory (already persisted)
            cutoff = now - timedelta(hours=2)
            for key in [k for k, w in windows.items()
                        if (w["anchor"] if hasattr(w["anchor"], "tzinfo") else datetime.fromisoformat(w["anchor"])) < cutoff]:
                del windows[key]

            if time.time() - last_stats_refresh > 30:
                cumulative = get_stats(horizon_min)
                recent_24h = get_stats(horizon_min, hours=24)
                last_stats_refresh = time.time()

            candles_1m = (closed_candles_1m + [live_candle] if live_candle else closed_candles_1m)[-CANDLE_WINDOW_MIN:]
            write_state(state_path, horizon_min, decision_offset_min, ticks, windows, cumulative, recent_24h,
                        candles_1m, hourly_range_24h)

        except Exception as e:
            print(f"[{datetime.now(timezone.utc).isoformat()}] loop error: {e}", flush=True)

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
