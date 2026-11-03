"""
Re-runnable analysis backing a proposed LATE-window stake trigger for
binance_5min_stake_alert_monitor.py (added 2026-07-03, see project memory).
Unlike the mid-window dwell trigger (binance_5min_dwell_backtest.py), which
only ever tries to fire EARLIER than T+3, this tests firing a SECOND,
independent stake opportunity AFTER T+3 -- in the last stretch before expiry
-- regardless of whether the window already staked (or was NO STAKE) at T+3.

Motivation: a real trade's tick history showed a ~$17 (~3bps) move about 37s
before expiry that the monitor never looked at, since it only decides a
window once (T+2/dwell/T+3) and never re-checks before T+5.

Sample size caveat (same posture as DWELL_SECONDS/DWELL_FLOOR_BPS): tick-level
history (binance_5min_ticks) only exists since 2026-07-01, ~2-3 days / ~560
windows as of this writing. Treat any constants chosen from this run as a
starting point to revisit weekly as more tick history accumulates, not a
settled conclusion -- identical posture to the existing dwell trigger.

Usage: python3 scripts/binance_5min_late_trigger_backtest.py
"""
import sys
sys.path.insert(0, ".")
import psycopg2
from datetime import timedelta
import config as _C

LATE_WINDOW_START_CANDIDATES = [90, 60, 45, 30]   # seconds before expiry to start watching
LATE_FLOOR_BPS_CANDIDATES = [2.0, 3.0, 4.0, 5.0, 6.0]
LATE_HOLD_SECONDS_CANDIDATES = [5, 8, 10, 15, 20]

pg = psycopg2.connect(host=_C.DB_HOST, dbname=_C.DB_NAME, user=_C.DB_USER, password=_C.DB_PASSWORD)

with pg.cursor() as cur:
    cur.execute("""
        SELECT anchor, price_t, tier, side, stake_ok, bps_t3, actual_dir, correct
        FROM binance_5min_windows
        WHERE status = 'EXPIRED' AND actual_dir IS NOT NULL AND price_t IS NOT NULL
        ORDER BY anchor
    """)
    windows = cur.fetchall()

print(f"Candidate windows (all EXPIRED, no tier/stake filter -- late trigger must fire on "
      f"NO-STAKE windows too): {len(windows)}\n")
if len(windows) < 100:
    print("*** CAUTION: small sample -- tick history (binance_5min_ticks) only goes back to "
          "2026-07-01. Treat any constants below as a starting point, not a settled "
          "conclusion -- same posture as DWELL_SECONDS/DWELL_FLOOR_BPS. Re-run as more "
          "history accumulates. ***\n")

MAX_START = max(LATE_WINDOW_START_CANDIDATES)
candidates = []
for anchor, price_t, tier, side, stake_ok, bps_t3, actual_dir, correct in windows:
    expiry = anchor + timedelta(minutes=5)
    late_start = expiry - timedelta(seconds=MAX_START)
    with pg.cursor() as cur:
        cur.execute("""
            SELECT ts, price FROM binance_5min_ticks
            WHERE ts >= %s AND ts <= %s ORDER BY ts
        """, (late_start, expiry))
        ticks = cur.fetchall()
    if not ticks:
        continue
    bps_series = [(ts, (price - price_t) / price_t * 10000.0) for ts, price in ticks]
    candidates.append({
        "anchor": anchor, "price_t": price_t, "expiry": expiry,
        "t3_tier": tier, "t3_side": side, "t3_stake_ok": bool(stake_ok) if stake_ok is not None else False,
        "t3_staked": tier not in (None, "NONE", "WEAK") and bool(stake_ok),
        "actual_dir": actual_dir, "bps_series": bps_series,
    })

print(f"Of these, {len(candidates)} have tick coverage in the last {MAX_START}s before expiry.\n")

header = (f"{'start':>6} | {'floor':>5} | {'hold':>4} | {'n_fired':>7} | {'acc':>6} | "
          f"{'avg_s_left':>10} | {'avg|bps|':>8} | {'agree_T3':>8} | "
          f"{'no-stake_n/acc':>15} | {'staked_n/acc':>13}")
print(header)
print("-" * len(header))

results = []
for start_sec in LATE_WINDOW_START_CANDIDATES:
    for floor_bps in LATE_FLOOR_BPS_CANDIDATES:
        for hold_sec in LATE_HOLD_SECONDS_CANDIDATES:
            if hold_sec >= start_sec:
                continue
            n_fired = 0
            n_correct = 0
            seconds_left_list = []
            abs_bps_list = []
            n_agree_t3 = 0
            n_no_stake = n_no_stake_correct = 0
            n_staked = n_staked_correct = 0

            for w in candidates:
                window_start = w["expiry"] - timedelta(seconds=start_sec)
                series = [(ts, bps) for ts, bps in w["bps_series"] if ts >= window_start]
                cross_idx = None
                for i, (ts, bps) in enumerate(series):
                    if abs(bps) >= floor_bps:
                        cross_idx = i
                        break
                if cross_idx is None:
                    continue
                cross_ts, cross_bps = series[cross_idx]
                cross_sign = 1 if cross_bps > 0 else -1
                hold_end = cross_ts + timedelta(seconds=hold_sec)
                if hold_end > w["expiry"]:
                    continue
                stable = True
                last_bps = cross_bps
                fire_ts = None
                for ts, bps in series[cross_idx:]:
                    if ts > hold_end:
                        fire_ts = ts
                        break
                    sign = 1 if bps > 0 else (-1 if bps < 0 else 0)
                    if sign != 0 and sign != cross_sign:
                        stable = False
                        break
                    last_bps = bps
                    fire_ts = ts
                if not stable or abs(last_bps) < floor_bps:
                    continue

                predicted_side = "HIGHER" if cross_sign > 0 else "LOWER"
                n_fired += 1
                seconds_left_list.append((w["expiry"] - fire_ts).total_seconds())
                abs_bps_list.append(abs(last_bps))
                is_correct = predicted_side == w["actual_dir"]
                if is_correct:
                    n_correct += 1
                if w["t3_side"] is not None and predicted_side == w["t3_side"]:
                    n_agree_t3 += 1
                if w["t3_staked"]:
                    n_staked += 1
                    if is_correct:
                        n_staked_correct += 1
                else:
                    n_no_stake += 1
                    if is_correct:
                        n_no_stake_correct += 1

            if n_fired == 0:
                continue
            acc = 100 * n_correct / n_fired
            avg_left = sum(seconds_left_list) / len(seconds_left_list)
            avg_bps = sum(abs_bps_list) / len(abs_bps_list)
            agree_pct = 100 * n_agree_t3 / n_fired
            no_stake_str = f"{n_no_stake}/{100*n_no_stake_correct/n_no_stake:.0f}%" if n_no_stake else "0/n-a"
            staked_str = f"{n_staked}/{100*n_staked_correct/n_staked:.0f}%" if n_staked else "0/n-a"
            print(f"{start_sec:6d} | {floor_bps:5.1f} | {hold_sec:4d} | {n_fired:7d} | "
                  f"{acc:5.1f}% | {avg_left:10.1f} | {avg_bps:8.2f} | {agree_pct:7.1f}% | "
                  f"{no_stake_str:>15} | {staked_str:>13}")
            results.append({
                "start_sec": start_sec, "floor_bps": floor_bps, "hold_sec": hold_sec,
                "n_fired": n_fired, "acc": acc, "n_no_stake": n_no_stake,
                "no_stake_acc": (100*n_no_stake_correct/n_no_stake) if n_no_stake else None,
            })

print(f"\nTotal combos with >=1 fire: {len(results)}")
print("\nColumns 'no-stake_n/acc' and 'staked_n/acc' split fires by whether the window was "
      "already staked at T+3 -- a combo that fires mostly on NO-STAKE windows with decent "
      "accuracy is genuinely rescuing missed opportunities; one that mostly fires on "
      "already-staked windows is just reinforcing (still useful, but less novel).")
print("\nPick LATE_WINDOW_START_SEC / LATE_FLOOR_BPS / LATE_HOLD_SECONDS from the printed "
      "table above -- favor combos with n_fired large enough to trust (dozens, not single "
      "digits) and accuracy meaningfully above 50%. Do not hand-pick the single "
      "highest-accuracy row if its n is tiny (that's noise, not signal).")

pg.close()
