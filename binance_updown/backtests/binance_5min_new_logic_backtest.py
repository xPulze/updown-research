"""
Re-runnable full-pipeline backtest of the reworked gate + Kelly + reweighted-
ladder logic (see project memory '"Profit ramp" rework', added 2026-07-03).
Reconstructs 5-min windows from raw 1-min `candles` (14+ months of history,
unlike `binance_5min_windows` which only goes back to 2026-07-01) and replays
the exact production functions (tier_for, adx_confirmed/unfavourable,
ny_open_tightening, required_edge_acc, stake_pct_for, Kelly sizing) walking
chronologically with the same stateful gate hysteresis as
refresh_auto_gates()/compute_retroactive_gated_stats().

Usage: python3 scripts/binance_5min_new_logic_backtest.py [days] [--time-aware]
  days: lookback window in days (default 14)
  --time-aware: (added 2026-07-03) also reconstruct the T+2 early-decision
    path (decided_early / decision_bps, mirroring production and
    compute_retroactive_gated_stats()) and price every decision with
    assumed_payout_for_time(abs_bps, seconds_remaining) instead of the flat
    per-tier assumed_payout_for(label). seconds_remaining is 180 for
    T+2-early decisions and 120 for T+3 — the only two checkpoints a 1-min
    candle reconstruction can see (the dwell/late-trigger sub-minute paths
    need tick data and are validated separately in
    binance_5min_late_trigger_backtest.py).

Re-run this whenever the gate/Kelly/ladder constants change, or periodically
to re-check the numbers as more live data accumulates past 2026-07-01 in
binance_5min_windows itself (which would let a similar check run off real
persisted decisions instead of a candles reconstruction).
"""
import sys
sys.path.insert(0, ".")
from datetime import timedelta
import psycopg2
import config as _C
from scripts.binance_5min_stake_alert_monitor import (
    BACKTEST_ACC_BY_TIER, AUTO_GATE_TIERS, AUTO_GATE_MIN_N, AUTO_GATE_DISABLE_MARGIN,
    AUTO_GATE_LOOKBACK_HOURS, required_edge_acc, NO_STAKE_TIERS, ny_open_tightening,
    NY_OPEN_MIN_STAKE_BPS, stake_pct_for, KELLY_FRACTIONAL_MULT, KELLY_MAX_STAKE_PCT,
    STAKE_PCT_LADDER, tier_for, adx_confirmed, adx_unfavourable, assumed_payout_for,
    ny_open_tier_acc, LOW_ADX_ONLY_TIERS, assumed_payout_for_time,
    ADX_UNFAVOURABLE_EXEMPT_TIERS,
)

TIME_AWARE = "--time-aware" in sys.argv
# --early-only: decomposition mode — reconstruct the T+2-early decision path
# (like --time-aware) but keep the FLAT per-tier payout. Isolates how much of
# the --time-aware delta comes from modeling the early path more faithfully
# (production already has it; the default baseline doesn't model it) vs from
# the payout formula itself.
EARLY_ONLY = "--early-only" in sys.argv
_pos_args = [a for a in sys.argv[1:] if not a.startswith("--")]
DAYS = int(_pos_args[0]) if _pos_args else 14

pg = psycopg2.connect(host=_C.DB_HOST, dbname=_C.DB_NAME, user=_C.DB_USER, password=_C.DB_PASSWORD)
with pg.cursor() as cur:
    cur.execute("""
        SELECT time, close, open, adx FROM candles
        WHERE time >= now() - (%(days)s || ' days')::interval
        ORDER BY time
    """, {"days": DAYS})
    candle_rows = cur.fetchall()
pg.close()

by_time = {t: (close, open_, adx) for t, close, open_, adx in candle_rows}


def floor_to_5min(t):
    return t.replace(second=0, microsecond=0, minute=(t.minute // 5) * 5)


anchors = sorted(set(floor_to_5min(t) for t, *_ in candle_rows))

windows = []
for anchor in anchors:
    t0 = by_time.get(anchor)
    t2 = by_time.get(anchor + timedelta(minutes=1))   # close = price at anchor+2m
    t3 = by_time.get(anchor + timedelta(minutes=2))   # close = price at anchor+3m
    t5 = by_time.get(anchor + timedelta(minutes=4))   # close = price at anchor+5m
    if not (t0 and t2 and t3 and t5):
        continue
    price_T = t0[1]  # open of anchor's own candle
    price_T2 = t2[0]
    price_T3 = t3[0]
    price_T5 = t5[0]
    adx_at_t3 = t3[2]
    if not price_T:
        continue
    bps_T2 = (price_T2 - price_T) / price_T * 10000.0
    bps_T3 = (price_T3 - price_T) / price_T * 10000.0
    bps_T5 = (price_T5 - price_T) / price_T * 10000.0
    actual_dir = "HIGHER" if bps_T5 > 0 else ("LOWER" if bps_T5 < 0 else "FLAT")

    # Replicate the production early-decision rule (same pattern as
    # compute_retroactive_gated_stats(): a window that already crosses a
    # stake-eligible tier's floor at T+2 is decided on THAT reading, with
    # 180s remaining; otherwise it's decided at T+3 with 120s remaining).
    # Only exercised in --time-aware mode so the default run stays
    # byte-identical to the recorded baseline.
    decided_early = False
    decision_bps = bps_T3
    seconds_remaining = 120.0
    if TIME_AWARE or EARLY_ONLY:
        early_label, _ = tier_for(abs(bps_T2))
        if early_label is not None and early_label not in NO_STAKE_TIERS:
            decided_early = True
            decision_bps = bps_T2
            seconds_remaining = 180.0

    abs_bps = abs(decision_bps)
    label, acc = tier_for(abs_bps)
    in_ny_open = ny_open_tightening(anchor)
    if in_ny_open and acc is not None:
        acc = ny_open_tier_acc(abs_bps, acc)
    side = "HIGHER" if decision_bps > 0 else ("LOWER" if decision_bps < 0 else None)
    correct = (side == actual_dir) if side else None
    if TIME_AWARE:
        payout = assumed_payout_for_time(abs_bps, seconds_remaining, fallback_label=label) if label else None
    else:
        payout = assumed_payout_for(label) if label else None
    windows.append({
        "anchor": anchor, "tier": label, "bps_t3": decision_bps, "side": side,
        "adx": adx_at_t3,
        "adx_unfavourable": adx_unfavourable(adx_at_t3), "adx_confirmed": adx_confirmed(adx_at_t3),
        "correct": correct, "ny_tight": in_ny_open,
        "assumed_payout": payout, "decided_early": decided_early,
    })

print(f"Reconstructed {len(windows)} windows from {anchors[0]} to {anchors[-1]}")

tier_history = {}
gate_state = {t: True for t in AUTO_GATE_TIERS}

n_staked = n_correct = n_wrong = 0
staked_frac = 0.0
pnl_frac = 0.0
by_tier = {}
gate_flips = []

for w in windows:
    anchor = w["anchor"]; tier = w["tier"]; bps_t3 = w["bps_t3"]
    non_gate_ok = (
        tier is not None and tier != "NONE" and tier not in NO_STAKE_TIERS
        and not (tier not in ADX_UNFAVOURABLE_EXEMPT_TIERS and w["adx_unfavourable"])
        and not (tier in LOW_ADX_ONLY_TIERS and w["adx"] is not None and w["adx"] >= LOW_ADX_ONLY_TIERS[tier])
        and not (w["ny_tight"] and abs(bps_t3) < NY_OPEN_MIN_STAKE_BPS)
    )
    gate_on = True
    n_prior = 0
    rolling_acc = None
    if tier in AUTO_GATE_TIERS:
        hist = tier_history.get(tier, [])
        cutoff = anchor - timedelta(hours=AUTO_GATE_LOOKBACK_HOURS)
        hist_recent = [c for (t, c) in hist if t >= cutoff]
        n_prior = len(hist_recent)
        backtest_acc = BACKTEST_ACC_BY_TIER.get(tier)
        if n_prior >= AUTO_GATE_MIN_N and backtest_acc is not None:
            rolling_acc = sum(hist_recent) / n_prior
            was_enabled = gate_state[tier]
            if was_enabled and rolling_acc < backtest_acc - AUTO_GATE_DISABLE_MARGIN:
                gate_state[tier] = False
                gate_flips.append((anchor, tier, "OFF", rolling_acc))
            elif not was_enabled and rolling_acc >= required_edge_acc(backtest_acc):
                gate_state[tier] = True
                gate_flips.append((anchor, tier, "ON", rolling_acc))
        gate_on = gate_state[tier]
    retro_stake_ok = non_gate_ok and gate_on

    pct = 0.0
    if retro_stake_ok:
        backtest_acc = BACKTEST_ACC_BY_TIER.get(tier)
        b = w["assumed_payout"]
        if n_prior >= AUTO_GATE_MIN_N and rolling_acc is not None and backtest_acc is not None and b:
            if rolling_acc >= required_edge_acc(backtest_acc):
                f_full = max(0.0, rolling_acc - (1.0 - rolling_acc) / b)
                pct = min(KELLY_MAX_STAKE_PCT, KELLY_FRACTIONAL_MULT * f_full)
        else:
            pct = stake_pct_for(abs(bps_t3))
            if pct > 0 and w["adx_confirmed"]:
                idx = STAKE_PCT_LADDER.index(pct)
                if idx < len(STAKE_PCT_LADDER) - 1:
                    pct = STAKE_PCT_LADDER[idx + 1]
        if pct <= 0:
            retro_stake_ok = False

    if retro_stake_ok and w["correct"] is not None:
        n_staked += 1
        staked_frac += pct
        t = by_tier.setdefault(tier, {"n": 0, "correct": 0, "staked_frac": 0.0, "pnl_frac": 0.0})
        t["n"] += 1
        t["staked_frac"] += pct
        if w["correct"]:
            n_correct += 1
            pnl_frac += pct * w["assumed_payout"]
            t["correct"] += 1
            t["pnl_frac"] += pct * w["assumed_payout"]
        else:
            n_wrong += 1
            pnl_frac -= pct
            t["pnl_frac"] -= pct

    if w["correct"] is not None and tier in AUTO_GATE_TIERS:
        tier_history.setdefault(tier, []).append((anchor, w["correct"]))

mode_str = ("TIME-AWARE payout (T+2-early reconstructed)" if TIME_AWARE
            else ("flat payout + T+2-early reconstructed (decomposition)" if EARLY_ONLY
                  else "flat per-tier payout (baseline)"))
print(f"\n=== Last {DAYS} days, reworked logic (gate + reweighted ladder/Kelly) — {mode_str} ===")
if TIME_AWARE or EARLY_ONLY:
    n_early = sum(1 for w in windows if w.get("decided_early"))
    print(f"Windows decided early at T+2 (180s remaining): {n_early} of {len(windows)}")
if n_staked:
    print(f"Staked: {n_staked}  Correct: {n_correct}  Wrong: {n_wrong}  Accuracy: {100*n_correct/n_staked:.1f}%")
else:
    print("no stakes")
print(f"Total staked (sum of fractions): {staked_frac*100:.1f}%")
print(f"Net P&L (fraction of capital): {pnl_frac*100:+.2f}%")
print(f"\nBy tier:")
for tier, t in sorted(by_tier.items(), key=lambda kv: -kv[1]['pnl_frac']):
    acc = 100*t['correct']/t['n'] if t['n'] else 0
    print(f"  {tier:16s} n={t['n']:4d}  acc={acc:5.1f}%  staked={t['staked_frac']*100:6.1f}%  pnl={t['pnl_frac']*100:+7.2f}%")
print(f"\nGate flips ({len(gate_flips)} total, showing last 20):")
for anchor, tier, direction, acc in gate_flips[-20:]:
    print(f"  {anchor}  {tier:16s} -> {direction}  (rolling_acc={acc*100:.1f}%)")
