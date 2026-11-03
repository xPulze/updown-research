#!/usr/bin/env python3
"""ETH T3 live-staking guard (2026-09-25).

ETH T3 has no proven edge (Sept holdout ~0/negative, see memory
project_pm_eth_t3_no_taker_or_maker_edge_20260924) and at 2-3 trades/day it can
never be validated by waiting for n. So it runs as a CAPPED bet with a dynamic
kill rule instead:

  1. HEALTH (every window, not just staked): every leg-0 T3 window row that
     passes the live gate's FEATURE rules (predicted-side ask in the T3 band,
     |bps|>=4, velocity>0) is scored at $1 with the bot's own fee formula,
     whether or not we could fill it. Same ~2.6/day, but immune to queue/fill
     luck and it keeps scoring while staking is off. A lower CUSUM over that
     stream (S = max(0, S - x - K)) trips at S > H.
     Calibrated by bootstrap on the Jul-Sep cohort (sd 0.8): median trip after
     ~27 candidates at true EV -0.25, ~53 at -0.10, ~100 at 0.
  2. LIVE BUDGET (real fills only, recon_ok not null): cumulative real P&L since
     the arm epoch <= -BUDGET, or LOSS_STREAK straight real losses.

A trip sets `ETH:t3_live=0` (the bot re-reads it every tick: no restart) and
LATCHES `ETH:t3_guard_halted=1`. The guard never re-arms. To resume:
`eth_t3_guard.py --arm` (new epoch, CUSUM restarts at 0).

    eth_t3_guard.py            evaluate once (cron, every 5 min)
    eth_t3_guard.py --status   print state, change nothing
    eth_t3_guard.py --arm      arm live + fresh epoch
    eth_t3_guard.py --halt     manual stop
"""
import json
import os
import sys
from datetime import datetime, timezone

import psycopg2

ROOT = os.path.dirname(os.path.abspath(__file__))
TABLE = "polymarket_updown_trades_eth"
FEE = 0.07
ASK_LO, ASK_HI = 0.35, 0.68        # == POLYMARKET_T3_ASK_FLOOR/CEIL
MIN_BPS = 4.0
K, H = 0.10, 6.0                   # CUSUM allowance / threshold, in $ per $1 ticket
BUDGET_USD = 5.0
LOSS_STREAK = 6
STATE_PATH = os.path.join(ROOT, "reports", "eth_t3_guard_state.json")
K_LIVE, K_HALT, K_EPOCH, K_WHY = ("ETH:t3_live", "ETH:t3_guard_halted",
                                  "ETH:t3_guard_epoch", "ETH:t3_guard_reason")


def _env():
    for line in open(os.path.join(ROOT, ".env")):
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.replace("export ", "", 1).split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip("'\""))


def connect():
    _env()
    return psycopg2.connect(host=os.environ["DB_HOST"], port=os.environ.get("DB_PORT", 5432),
                            user=os.environ["DB_USER"], password=os.environ["DB_PASSWORD"],
                            dbname=os.environ["DB_NAME"])


def cfg_get(cur, k, d=None):
    cur.execute("SELECT value FROM polymarket_config WHERE key=%s", (k,))
    r = cur.fetchone()
    return r[0] if r else d


def cfg_set(cur, k, v):
    cur.execute("""INSERT INTO polymarket_config (key, value) VALUES (%s, %s)
                   ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value, updated_at=NOW()""",
                (k, str(v)))


def ticket_pnl(ask, win):
    """Per-$1 P&L, same fee shape as the bot (t3 breakeven formula)."""
    fee = FEE * (1.0 - ask)
    return ((1.0 - ask) / ask - fee) if win else (-1.0 - fee)


def cohort(cur, epoch):
    cur.execute(f"""SELECT window_start_utc, predicted_side, decision_bps, velocity_frac,
                           ask_up, ask_down, actual_outcome FROM {TABLE}
                    WHERE is_t3 AND leg=0 AND actual_outcome IS NOT NULL
                      AND window_start_utc >= %s ORDER BY window_start_utc""", (epoch,))
    out = []
    for ws, side, bps, vel, au, ad, oc in cur.fetchall():
        ask = au if side == "Up" else ad
        if None in (ask, bps, vel) or side not in ("Up", "Down"):
            continue
        if ASK_LO <= ask <= ASK_HI and abs(bps) >= MIN_BPS and vel > 0:
            out.append((ws, ticket_pnl(ask, side == oc)))
    return out


def live_real(cur, epoch):
    cur.execute(f"""SELECT realized_pnl_usd FROM {TABLE}
                    WHERE book='t3' AND staked AND recon_ok IS NOT NULL
                      AND realized_pnl_usd IS NOT NULL AND decided_at >= %s
                    ORDER BY decided_at""", (epoch,))
    return [float(r[0]) for r in cur.fetchall()]


def evaluate(cur):
    epoch = cfg_get(cur, K_EPOCH)
    if not epoch:
        return None
    coh = cohort(cur, epoch)
    s, s_max, peak_at = 0.0, 0.0, None
    for i, (_, x) in enumerate(coh):
        s = max(0.0, s - x - K)
        if s > s_max:
            s_max = s
        if s > H and peak_at is None:
            peak_at = i + 1
    xs = [x for _, x in coh]
    live = live_real(cur, epoch)
    streak = 0
    for p in reversed(live):
        if p < 0:
            streak += 1
        else:
            break
    st = {
        "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"), "epoch": epoch,
        "cohort_n": len(xs), "cohort_wr": round(sum(x > 0 for x in xs) / len(xs), 3) if xs else None,
        "cohort_ev": round(sum(xs) / len(xs), 4) if xs else None, "cusum": round(s, 3),
        "cusum_max": round(s_max, 3), "cusum_h": H,
        "live_n": len(live), "live_pnl": round(sum(live), 2), "live_loss_streak": streak,
        "budget_usd": BUDGET_USD,
        "live_flag": cfg_get(cur, K_LIVE), "halted": cfg_get(cur, K_HALT, "0"),
    }
    why = None
    if s > H:
        why = f"CUSUM {s:.2f} > {H} after {len(xs)} candidates (EV {st['cohort_ev']})"
    elif st["live_pnl"] <= -BUDGET_USD:
        why = f"live budget: real P&L {st['live_pnl']} <= -{BUDGET_USD}"
    elif streak >= LOSS_STREAK:
        why = f"{streak} straight real losses"
    st["trip"] = why
    return st


def main():
    arg = sys.argv[1] if len(sys.argv) > 1 else ""
    conn = connect()
    conn.autocommit = True
    cur = conn.cursor()
    if arg == "--arm":
        cfg_set(cur, K_EPOCH, datetime.now(timezone.utc).isoformat())
        cfg_set(cur, K_HALT, "0")
        cfg_set(cur, K_WHY, "")
        cfg_set(cur, "ETH:t3_capital_usd", BUDGET_USD)
        cfg_set(cur, K_LIVE, "1")
        print("ARMED ETH T3 live, budget", BUDGET_USD)
    elif arg == "--halt":
        cfg_set(cur, K_LIVE, "0")
        cfg_set(cur, K_HALT, "1")
        cfg_set(cur, K_WHY, "manual")
        print("HALTED")
    st = evaluate(cur)
    if st is None:
        print("guard: no epoch set (run --arm)")
        return
    if arg != "--status" and st["trip"] and st["halted"] != "1":
        cfg_set(cur, K_LIVE, "0")
        cfg_set(cur, K_HALT, "1")
        cfg_set(cur, K_WHY, st["trip"])
        st["halted"], st["live_flag"] = "1", "0"
        print(f"[{st['ts']}] ETH T3 GUARD TRIP -> staking OFF: {st['trip']}", flush=True)
    elif st["halted"] == "1" and st["live_flag"] not in (None, "0") and arg != "--status":
        cfg_set(cur, K_LIVE, "0")      # someone re-armed by hand without --arm
        st["live_flag"] = "0"
        print(f"[{st['ts']}] guard: latched halt re-enforced", flush=True)
    with open(STATE_PATH, "w") as f:
        json.dump(st, f, indent=1)
    print(json.dumps(st))


if __name__ == "__main__":
    main()
