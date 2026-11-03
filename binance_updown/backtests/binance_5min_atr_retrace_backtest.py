"""
Focused replay for one question ("how to eliminate this
loser" re: the 2026-07-06 02:30 UTC STRONG dwell loss, followed by "re run
... for the border line case"): that loss fired on a RETRACING move (12.7bps
at T+2 shrinking to 11.1bps by T+3, same direction) sitting right at the
edge of the already-shipped ATR_RATIO_MIN=2.0 filter (~1.8-2.0x ATR). Does
tightening the ATR-ratio bar specifically for RETRACING T+3/dwell fires
improve accuracy enough to be worth the lost volume, or was this just a
one-off near-miss?

Reconstructs 5-min windows from the full ~18-month `candles` history (1-min
OHLC + adx), same method as binance_5min_new_logic_backtest.py. Limitation
shared with compute_retroactive_gated_stats()'s own mirror (see its
t3_sign_flip comment): only the T+2/T+3 checkpoints are visible here, not
live ticks — so this can only test the SIGN-FLIP half of the T3 backward
filter and the T2->T3 magnitude-shrink definition of "retracing", not the
full tick-level retrace-off-extreme. Scope matches ATR_RATIO_FILTER_TIERS /
T3_BACKWARD_FILTER_TIERS exactly (STRONG, VERY STRONG, EXTREME,
MODERATE-STRONG), decided_early=False (T+3/dwell) only.

Usage: python3 scripts/binance_5min_atr_retrace_backtest.py [days]
"""
import sys
sys.path.insert(0, ".")
from datetime import timedelta
import psycopg2
import pandas as pd
import pandas_ta as ta
import config as _C
from scripts.binance_5min_stake_alert_monitor import (
    NO_STAKE_TIERS, ADX_UNFAVOURABLE_EXEMPT_TIERS, LOW_ADX_ONLY_TIERS,
    ny_open_tightening, NY_OPEN_MIN_STAKE_BPS, tier_for, adx_unfavourable,
    T3_BACKWARD_FILTER_TIERS, ATR_RATIO_FILTER_TIERS, ATR_RATIO_MIN,
)

_pos_args = [a for a in sys.argv[1:] if not a.startswith("--")]
DAYS = int(_pos_args[0]) if _pos_args else 548  # ~18 months, matches other studies here

SCOPE_TIERS = T3_BACKWARD_FILTER_TIERS & ATR_RATIO_FILTER_TIERS  # {STRONG, VERY STRONG, EXTREME, MODERATE-STRONG}

pg = psycopg2.connect(host=_C.DB_HOST, dbname=_C.DB_NAME, user=_C.DB_USER, password=_C.DB_PASSWORD)
with pg.cursor() as cur:
    cur.execute("""
        SELECT time, open, high, low, close, adx FROM candles
        WHERE time >= now() - (%(days)s || ' days')::interval
        ORDER BY time
    """, {"days": DAYS})
    rows = cur.fetchall()
pg.close()

df = pd.DataFrame(rows, columns=["time", "open", "high", "low", "close", "adx"]).set_index("time")
atr_series = df.ta.atr(length=14)
df["atr_bps"] = (atr_series / df["close"] * 10000.0)
by_time = {t: r for t, r in zip(df.index, df.itertuples(index=False))}


def floor_to_5min(t):
    return t.replace(second=0, microsecond=0, minute=(t.minute // 5) * 5)


anchors = sorted(set(floor_to_5min(t) for t in df.index))

cases = []
for anchor in anchors:
    t0 = by_time.get(anchor)
    t2 = by_time.get(anchor + timedelta(minutes=1))
    t3 = by_time.get(anchor + timedelta(minutes=2))
    t5 = by_time.get(anchor + timedelta(minutes=4))
    if not (t0 and t2 and t3 and t5):
        continue
    price_T = t0.open
    if not price_T:
        continue
    bps_T2 = (t2.close - price_T) / price_T * 10000.0
    bps_T3 = (t3.close - price_T) / price_T * 10000.0
    bps_T5 = (t5.close - price_T) / price_T * 10000.0
    actual_dir = "HIGHER" if bps_T5 > 0 else ("LOWER" if bps_T5 < 0 else "FLAT")
    atr_bps = t3.atr_bps
    if atr_bps is None or pd.isna(atr_bps) or atr_bps <= 0:
        continue

    abs_bps = abs(bps_T3)
    label, acc = tier_for(abs_bps)
    if label is None or label not in SCOPE_TIERS:
        continue
    ny_tight = ny_open_tightening(anchor)
    non_gate_ok = (
        label not in NO_STAKE_TIERS
        and not (label not in ADX_UNFAVOURABLE_EXEMPT_TIERS and adx_unfavourable(t3.adx))
        and not (label in LOW_ADX_ONLY_TIERS and t3.adx is not None and t3.adx >= LOW_ADX_ONLY_TIERS[label])
        and not (ny_tight and abs_bps < NY_OPEN_MIN_STAKE_BPS)
    )
    if not non_gate_ok:
        continue
    # T3 sign-flip mirror only (tick-level retrace-off-extreme not
    # reconstructable from 1-min candles — see module docstring).
    sign_flip = (bps_T2 > 0) != (bps_T3 > 0)
    if sign_flip:
        continue

    side = "HIGHER" if bps_T3 > 0 else "LOWER"
    correct = (side == actual_dir)
    retracing = (abs(bps_T3) < abs(bps_T2)) and ((bps_T2 > 0) == (bps_T3 > 0))
    atr_ratio = abs_bps / atr_bps
    cases.append({
        "anchor": anchor, "tier": label, "correct": correct,
        "retracing": retracing, "atr_ratio": atr_ratio,
    })

print(f"Reconstructed {len(cases)} T+3/dwell {sorted(SCOPE_TIERS)} decisions "
      f"(post non-gate + sign-flip filter, pre-ATR-ratio) over {DAYS} days "
      f"({anchors[0]} .. {anchors[-1]})")


def summarize(pop, label):
    n = len(pop)
    if n == 0:
        print(f"  {label:32s} n=0")
        return
    correct = sum(1 for c in pop if c["correct"])
    print(f"  {label:32s} n={n:5d}  acc={100*correct/n:5.1f}%")


print(f"\n=== Baseline (current shipped bar, ATR_RATIO_MIN={ATR_RATIO_MIN:g}) ===")
kept_now = [c for c in cases if c["atr_ratio"] >= ATR_RATIO_MIN]
excl_now = [c for c in cases if c["atr_ratio"] < ATR_RATIO_MIN]
summarize(cases, "All (pre-ATR-ratio)")
summarize(kept_now, "Kept (>= current bar)")
summarize(excl_now, "Excluded (< current bar)")
summarize([c for c in kept_now if c["retracing"]], "Kept & retracing")
summarize([c for c in kept_now if not c["retracing"]], "Kept & NOT retracing")

print(f"\n=== Retracing-only subset: accuracy/volume by ATR-ratio bar ===")
retracing_all = [c for c in cases if c["retracing"]]
print(f"({len(retracing_all)} retracing cases total, pre-ATR-ratio, out of {len(cases)})")
for bar in [1.5, 1.75, 2.0, 2.25, 2.5, 2.75, 3.0, 3.5, 4.0]:
    kept = [c for c in retracing_all if c["atr_ratio"] >= bar]
    excl_n = len(retracing_all) - len(kept)
    if kept:
        acc = 100 * sum(1 for c in kept if c["correct"]) / len(kept)
        print(f"  bar={bar:4.2f}x  kept n={len(kept):5d} acc={acc:5.1f}%  "
              f"(excludes {excl_n} more retracing cases vs current bar)")
    else:
        print(f"  bar={bar:4.2f}x  kept n=0")

print(f"\n=== Non-retracing subset (sanity check bar shouldn't move for these) ===")
non_retracing_all = [c for c in cases if not c["retracing"]]
for bar in [2.0, 2.5, 3.0]:
    kept = [c for c in non_retracing_all if c["atr_ratio"] >= bar]
    if kept:
        acc = 100 * sum(1 for c in kept if c["correct"]) / len(kept)
        print(f"  bar={bar:4.2f}x  kept n={len(kept):5d} acc={acc:5.1f}%")

print(f"\n=== By tier, at current bar ({ATR_RATIO_MIN:g}x), retracing vs not ===")
for tier in sorted(SCOPE_TIERS):
    tier_kept = [c for c in kept_now if c["tier"] == tier]
    summarize([c for c in tier_kept if c["retracing"]], f"{tier} retracing")
    summarize([c for c in tier_kept if not c["retracing"]], f"{tier} not-retracing")
