"""Replay the live LATE trigger over every stored Binance five-minute window.

This deliberately imports the live constants and pricing functions so a replay
cannot silently drift from the monitor. It reports the raw redesigned trigger,
then applies today's operational gates, including the manual-fill ask cap.
"""
import math
import sys
from collections import defaultdict
from datetime import timedelta

sys.path.insert(0, ".")

import psycopg2
import config as C
from scripts import binance_5min_stake_alert_monitor as live


def wilson(wins, n, z=1.96):
    if not n:
        return (None, None)
    p = wins / n
    den = 1 + z * z / n
    centre = (p + z * z / (2 * n)) / den
    margin = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / den
    return centre - margin, centre + margin


def replay_window(anchor, price_t, ticks):
    expiry = anchor + timedelta(minutes=5)
    cross_ts = None
    cross_sign = None
    for ts, price in ticks:
        delta = price - price_t
        sign = 1 if delta > 0 else (-1 if delta < 0 else 0)
        in_band = live.LATE_DELTA_MIN_USD <= abs(delta) <= live.LATE_DELTA_MAX_USD
        if not in_band or sign == 0:
            cross_ts = cross_sign = None
            continue
        if cross_ts is None or cross_sign != sign:
            cross_ts, cross_sign = ts, sign
            continue
        if (ts - cross_ts).total_seconds() >= live.LATE_HOLD_SECONDS:
            seconds_left = (expiry - ts).total_seconds()
            if seconds_left >= live.LATE_MIN_ACTION_SECONDS:
                bps = delta / price_t * 10000.0
                payout = live.assumed_payout_for_time(abs(bps), seconds_left)
                return ts, sign, bps, seconds_left, payout, live.implied_ask(payout)
    return None


def main():
    pg = psycopg2.connect(host=C.DB_HOST, dbname=C.DB_NAME, user=C.DB_USER, password=C.DB_PASSWORD)
    with pg.cursor() as cur:
        cur.execute("""
            SELECT anchor, price_t, actual_dir, adx
            FROM binance_5min_windows
            WHERE status = 'EXPIRED' AND price_t IS NOT NULL
              AND actual_dir IN ('HIGHER', 'LOWER')
            ORDER BY anchor
        """)
        windows = cur.fetchall()
        if not windows:
            print("No settled windows found.")
            return
        first = windows[0][0] + timedelta(minutes=5) - timedelta(seconds=live.LATE_WINDOW_START_SEC)
        last = windows[-1][0] + timedelta(minutes=5)
        cur.execute("""
            SELECT ts, price FROM binance_5min_ticks
            WHERE ts >= %s AND ts <= %s ORDER BY ts
        """, (first, last))
        ticks = cur.fetchall()
    pg.close()

    by_anchor = defaultdict(list)
    for ts, price in ticks:
        anchor = ts.replace(second=0, microsecond=0) - timedelta(minutes=ts.minute % 5)
        if ts >= anchor + timedelta(minutes=5) - timedelta(seconds=live.LATE_WINDOW_START_SEC):
            by_anchor[anchor].append((ts, float(price)))

    rows = []
    for anchor, price_t, actual_dir, adx in windows:
        fire = replay_window(anchor, float(price_t), by_anchor.get(anchor, ()))
        if not fire:
            continue
        ts, sign, bps, seconds_left, payout, ask = fire
        side = "HIGHER" if sign > 0 else "LOWER"
        raw_ok = side == actual_dir
        ask_ok = ask is not None and ask <= live.MAX_MANUAL_ASK
        adx_ok = not live.adx_unfavourable(float(adx) if adx is not None else None)
        ny_ok = not (live.ny_open_tightening(ts) and abs(bps) < live.NY_OPEN_MIN_STAKE_BPS)
        rows.append(dict(anchor=anchor, correct=raw_ok, payout=payout, ask=ask,
                         ask_ok=ask_ok, gated=ask_ok and adx_ok and ny_ok))

    def show(label, sample):
        n = len(sample)
        wins = sum(r["correct"] for r in sample)
        losses = n - wins
        lo, hi = wilson(wins, n)
        pnl = sum(r["payout"] if r["correct"] else -1.0 for r in sample)
        avg_ask = sum(r["ask"] for r in sample) / n if n else 0
        print(f"{label:<34} n={n:5d}  {wins}W/{losses}L  "
              f"WR={100*wins/n:5.1f}%  95%CI={100*lo:4.1f}-{100*hi:4.1f}%  "
              f"ask={avg_ask:.3f}  EV={100*pnl/n:+6.2f}%" if n else f"{label:<34} n=0")

    print(f"Stored settled windows: {len(windows):,} ({windows[0][0]} to {windows[-1][0]})")
    print(f"Windows with late tick coverage: {sum(bool(by_anchor.get(w[0])) for w in windows):,}")
    print(f"Live rule: ${live.LATE_DELTA_MIN_USD:g}-${live.LATE_DELTA_MAX_USD:g} band, "
          f"{live.LATE_HOLD_SECONDS}s hold, >= {live.LATE_MIN_ACTION_SECONDS}s left")
    print(f"Manual-fill cap: ask <= {live.MAX_MANUAL_ASK:.2f}\n")
    show("Raw redesigned trigger", rows)
    show(f"After ask <= {live.MAX_MANUAL_ASK:.2f}", [r for r in rows if r["ask_ok"]])
    show("After all reconstructable gates", [r for r in rows if r["gated"]])
    print("\nBy month, after all gates:")
    months = sorted({r["anchor"].strftime("%Y-%m") for r in rows})
    for month in months:
        show(month, [r for r in rows if r["gated"] and r["anchor"].strftime("%Y-%m") == month])


if __name__ == "__main__":
    main()
