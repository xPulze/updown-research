"""
Independent verification of a generic "time-decay" narrative for 5-min BTC
Up/Down windows (claims like "T+2:00-4:00 zone, 0.10%+ move -> 70/30 odds",
"final 10-50s -> 85-95%+ accuracy") against our OWN real tick data, not
just reasoning about how such markets "should" behave.

Reconstructs windows from binance_5min_ticks (~2s resolution, since
2026-06-29 -- our project's own tier system uses 1-min `candles` snapshots
at fixed T+2/T+3/T+5, this script instead checks the claimed elapsed-time
zones directly, at whatever granularity the raw ticks allow).

For each window and each claimed time zone, finds the tick closest to that
elapsed offset, computes the bps move from window-open, and checks whether
that move's SIGN matches the window's actual final (T+5) outcome --
"accuracy" in the same sense the shared narrative uses it.
"""
import sys
sys.path.insert(0, ".")
from datetime import timedelta
import psycopg2
import config as _C

pg = psycopg2.connect(host=_C.DB_HOST, dbname=_C.DB_NAME, user=_C.DB_USER, password=_C.DB_PASSWORD)
with pg.cursor() as cur:
    cur.execute("SELECT ts, price FROM binance_5min_ticks ORDER BY ts")
    tick_rows = cur.fetchall()
pg.close()

print(f"Loaded {len(tick_rows)} ticks, {tick_rows[0][0]} to {tick_rows[-1][0]}")

# index ticks by minute-bucket for fast nearest-lookup
import bisect
tick_times = [t for t, p in tick_rows]
tick_prices = [p for t, p in tick_rows]


def price_at_or_after(target_ts):
    i = bisect.bisect_left(tick_times, target_ts)
    if i >= len(tick_times):
        return None
    return tick_prices[i]


def floor_to_5min(t):
    return t.replace(second=0, microsecond=0, minute=(t.minute // 5) * 5)


anchors = sorted(set(floor_to_5min(t) for t in tick_times))
windows = []
for anchor in anchors:
    open_price = price_at_or_after(anchor)
    close_price = price_at_or_after(anchor + timedelta(minutes=5))
    if open_price is None or close_price is None:
        continue
    # require the window to actually be complete (a tick exists at/after T+5)
    if tick_times[bisect.bisect_left(tick_times, anchor + timedelta(minutes=5))] > anchor + timedelta(minutes=5, seconds=30):
        continue
    final_bps = (close_price - open_price) / open_price * 10000.0
    if final_bps == 0:
        continue
    final_dir = 1 if final_bps > 0 else -1
    windows.append({"anchor": anchor, "open": open_price, "final_dir": final_dir})

print(f"Reconstructed {len(windows)} complete windows\n")

# Claimed zones (elapsed seconds from window open) from the shared narrative,
# plus a magnitude threshold to test their specific claims at each zone.
ZONES = [
    ("T+0-30s (claimed: coin-flip ~50%)", 15, None),
    ("T+30s-2:00, 0.02-0.05% move (claimed: ~55/45)", 90, (2.0, 5.0)),
    ("T+2:00-4:00, 0.10%+ move (claimed: ~70/30)", 180, (10.0, None)),
    ("T+4:00-4:50 (final 10-50s) (claimed: 85-95%+)", 260, None),
    ("T+4:50-5:00 (final 10s) (claimed: near-certain)", 295, None),
]

for label, elapsed_s, bps_range in ZONES:
    n = 0
    n_correct = 0
    for w in windows:
        check_ts = w["anchor"] + timedelta(seconds=elapsed_s)
        p = price_at_or_after(check_ts)
        if p is None:
            continue
        bps = (p - w["open"]) / w["open"] * 10000.0
        if bps_range is not None:
            lo, hi = bps_range
            if abs(bps) < lo or (hi is not None and abs(bps) >= hi):
                continue
        if bps == 0:
            continue
        direction = 1 if bps > 0 else -1
        n += 1
        if direction == w["final_dir"]:
            n_correct += 1
    acc = 100 * n_correct / n if n else float("nan")
    print(f"{label}\n  n={n}  accuracy={acc:.1f}%\n")
