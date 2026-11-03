"""
Re-runnable analysis backing the mid-window dwell trigger in
binance_5min_stake_alert_monitor.py (see project memory 'Mid-window dwell
trigger', added 2026-07-03). Tests, against the FULL population of windows
(not just ones already known to qualify by T+3 -- that biased first pass),
whether firing early on a DWELL_FLOOR_BPS crossing that holds its sign for a
candidate dwell period matches or beats waiting for the fixed T+3 checkpoint.

Re-run this periodically as more binance_5min_ticks history accumulates --
the constants shipped in the monitor (DWELL_FLOOR_BPS=4.0, DWELL_SECONDS=30)
were chosen from a ~4-day sample and are a starting point, not a settled
conclusion.
"""
import sys
sys.path.insert(0, ".")
import psycopg2
from datetime import timedelta
import config as _C

LOWEST_FLOOR = 4.0  # MODERATE's floor -- the lowest stake-eligible threshold
DWELL_CANDIDATES_SEC = [10, 20, 30, 40, 50]

pg = psycopg2.connect(host=_C.DB_HOST, dbname=_C.DB_NAME, user=_C.DB_USER, password=_C.DB_PASSWORD)

with pg.cursor() as cur:
    cur.execute("""
        SELECT anchor, price_t, tier, bps_t2, bps_t3, actual_dir, correct, side
        FROM binance_5min_windows
        WHERE status='EXPIRED' AND bps_t2 IS NOT NULL AND bps_t3 IS NOT NULL
          AND actual_dir IS NOT NULL
    """)
    windows = cur.fetchall()

# candidates: windows where the T+2 reading was BELOW the lowest floor (i.e.
# not already caught by the existing T+2-exact early path) -- these are the
# ones a dwell mechanism would newly affect.
candidates = []
for anchor, price_t, tier, bps_t2, bps_t3, actual_dir, correct, side in windows:
    if abs(bps_t2) < LOWEST_FLOOR:
        candidates.append({
            "anchor": anchor, "price_t": price_t, "tier": tier, "bps_t3": bps_t3,
            "actual_dir": actual_dir, "t3_staked": tier not in (None, "NONE", "WEAK"),
            "t3_correct": (correct == "CORRECT") if correct in ("CORRECT", "WRONG") else None,
        })

print(f"Candidate windows (T+2 reading below {LOWEST_FLOOR}bps, i.e. not already early-decided): {len(candidates)} of {len(windows)}")
n_t3_staked = sum(1 for w in candidates if w["t3_staked"])
n_t3_staked_correct = sum(1 for w in candidates if w["t3_staked"] and w["t3_correct"])
print(f"Of these, {n_t3_staked} end up staked by T+3 (tier != WEAK/NONE), "
      f"accuracy {100*n_t3_staked_correct/n_t3_staked:.1f}%\n")

for w in candidates:
    t2 = w["anchor"] + timedelta(minutes=2)
    t3 = w["anchor"] + timedelta(minutes=3)
    with pg.cursor() as cur:
        cur.execute("SELECT ts, price FROM binance_5min_ticks WHERE ts >= %s AND ts <= %s ORDER BY ts", (t2, t3))
        ticks = cur.fetchall()
    w["t3_ts"] = t3
    w["bps_series"] = [(ts, (price - w["price_t"]) / w["price_t"] * 10000.0) for ts, price in ticks]

print("dwell(s) | n_fired_early | acc_of_early_fires | avg_time_saved(s) | of fires, % that were true stakeworthy by T+3")
for dwell in DWELL_CANDIDATES_SEC:
    n_fired = 0
    n_fired_correct = 0
    n_fired_was_staked_at_t3 = 0
    time_saved_list = []
    for w in candidates:
        bps_series = w["bps_series"]
        t3 = w["t3_ts"]
        cross_idx = None
        for i, (ts, bps) in enumerate(bps_series):
            if abs(bps) >= LOWEST_FLOOR:
                cross_idx = i
                break
        if cross_idx is None:
            continue
        cross_ts, cross_bps = bps_series[cross_idx]
        cross_sign = 1 if cross_bps > 0 else -1
        predicted_side = "HIGHER" if cross_sign > 0 else "LOWER"
        dwell_end = cross_ts + timedelta(seconds=dwell)
        if dwell_end > t3:
            continue
        stable = True
        last_bps = cross_bps
        for ts, bps in bps_series[cross_idx:]:
            if ts > dwell_end:
                break
            sign = 1 if bps > 0 else (-1 if bps < 0 else 0)
            if sign != 0 and sign != cross_sign:
                stable = False
                break
            last_bps = bps
        if not (stable and abs(last_bps) >= LOWEST_FLOOR):
            continue
        n_fired += 1
        if predicted_side == w["actual_dir"]:
            n_fired_correct += 1
        if w["t3_staked"]:
            n_fired_was_staked_at_t3 += 1
        time_saved_list.append((t3 - dwell_end).total_seconds())

    acc = 100 * n_fired_correct / n_fired if n_fired else float("nan")
    avg_saved = sum(time_saved_list) / len(time_saved_list) if time_saved_list else 0.0
    pct_real = 100 * n_fired_was_staked_at_t3 / n_fired if n_fired else float("nan")
    print(f"{dwell:8d} | {n_fired:5d}        | {acc:6.1f}%             | {avg_saved:8.1f}          | {pct_real:5.1f}%")

pg.close()
