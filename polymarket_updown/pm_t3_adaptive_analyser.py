#!/usr/bin/env python3
"""PM T3 adaptive-threshold analyser (BTC + ETH) — SHADOW / RECOMMEND ONLY.

Replaces the fixed 4bps T3 move floor with a DYNAMIC, session-aware one:

    min_bps(session) = clamp(K * noise(session), 1.5, 8.0)     ask band stays .35-.68

  Sessions (UTC)  asia 00-07 · london 07-12 · ldn_ny overlap 12-16 · ny 16-21 · late 21-24.
                  Threshold rises into the London/NY active windows and falls in quiet
                  Asia/weekend hours, because the noise it is scaled by does.
  Noise           robust level of |decision_bps| over ALL primary T3 windows of that
                  session in the lookback (median after winsorising at med+3*MAD), so a
                  single spike in the 3 days can't set the threshold.
  Weekend logic   day-type matched lookback: on Sat/Sun (UTC, same calendar as
                  dow_stake_ok) the last 3 WEEKEND days; on a weekday the last 3 WEEKDAYS.
  Velocity        live asset rules unchanged (BTC vel>=0 below 6bps, ETH vel>0).
  Scoring         every primary (leg-0) T3 window, filled or not, $1 at the decision ask
                  with the bot fee formula (eth_t3_guard.ticket_pnl) — guard cohort shape.
                  Real primary fills have run AT OR ABOVE this replay (09-27: BTC real
                  +0.115/ticket n=36, replay-real gap -0.10); the loser fills were
                  revision legs, which the first-principles gate no longer trades.

  Walk-forward 08-05..09-27 (thresholds rebuilt at every session boundary from prior
  data only): BTC K=0.8 n=252 +$40.53 vs live 4bps n=162 +$23.17, both halves green;
  ETH K=0.6-0.8 +$8-9 vs +$5.42 — marginal. Data after 2026-09-27 is out-of-sample.

  pm_t3_adaptive_analyser.py              recommend now -> state + history (cron */15)
  pm_t3_adaptive_analyser.py --backtest   walk-forward, all K in K_GRID, per session

Output: reports/pm_t3_adaptive_state.json, reports/pm_t3_adaptive_history.jsonl
LIVE since 2026-09-27: polymarket_updown_bot.t3_min_move_bps() reads assets.<A>.tables
when polymarket_config `t3_adaptive_live` (BTC) / `ETH:t3_adaptive_live` = 1; stale
(>45 min) / missing / flag off -> fixed 4bps. Guards untouched.
"""
import json
import math
import os
import sys
from datetime import datetime, timedelta, timezone

from eth_t3_guard import ROOT, connect, ticket_pnl

TABLES = {"BTC": "polymarket_updown_trades", "ETH": "polymarket_updown_trades_eth"}
DATA_START = "2026-07-30"          # both assets on Chainlink from here
LOOKBACK_DAYS = 3
LIVE = (4.0, 0.35, 0.68)           # == t3_book_ok first-principles gate
K = 0.8                            # min_bps = K * session noise (BTC walk-forward best)
K_GRID = (0.6, 0.8, 1.0, 1.2, 1.5, 2.0)
FLOOR_BPS, CAP_BPS = 1.5, 8.0
NOISE_MIN_WINDOWS = 30             # per session in the lookback, else live 4bps
Z = 1.0
HAIRCUT_DAYS = 30
STATE_PATH = os.path.join(ROOT, "reports", "pm_t3_adaptive_state.json")
HIST_PATH = os.path.join(ROOT, "reports", "pm_t3_adaptive_history.jsonl")


def is_weekend(ts):
    return ts.astimezone(timezone.utc).weekday() >= 5


def load(cur, asset):
    cur.execute(f"""SELECT window_start_utc, predicted_side, decision_bps, velocity_frac,
                           ask_up, ask_down, actual_outcome, decided_at, price_open,
                           last_tick_bps, fillable_sh, fillable_opp_sh, no_stake_reason
                    FROM {TABLES[asset]}
                    WHERE is_t3 AND leg=0 AND actual_outcome IS NOT NULL
                      AND window_start_utc >= %s ORDER BY window_start_utc""", (DATA_START,))
    out = []
    for ws, side, bps, vel, au, ad, oc, da, po, lt, ff, fo, nsr in cur.fetchall():
        if side not in ("Up", "Down") or bps is None:
            continue
        ask = au if side == "Up" else ad
        if ask is None or not 0.0 < ask < 1.0:
            continue
        out.append({"ws": ws, "day": ws.astimezone(timezone.utc).date(), "we": is_weekend(ws),
                    "ask": ask, "mv": abs(bps), "vel": vel,
                    "x": ticket_pnl(ask, side == oc), "win": side == oc,
                    "sgn": 1 if side == "Up" else -1, "decided_at": da, "po": po, "lt": lt,
                    "nsr": nsr or "",
                    # Fill Δ = called-side minus opposite-side taker depth at decision
                    # (blank = 0, the dash/swingfade convention); None = never measured
                    "fd": (None if ff is None and fo is None
                           else float(ff or 0.0) - float(fo or 0.0))})
    return out


def _edge_stats(rs):
    """n, WR, avg ask, edge = WR - avg ask (price-adjusted, NO fee — context only),
    EV/ticket AFTER the bot fee and its SE. Verdicts use EV (2026-09-29): edge ignores
    the ~7%*(1-ask) fee (~3pp at ask .55), so a fairly priced cell read as break-even
    when it loses (ETH study: follow side edge ~0, EV -0.01..-0.08)."""
    n = len(rs)
    if not n:
        return {"n": 0}
    wr = sum(r["win"] for r in rs) / n
    ask = sum(r["ask"] for r in rs) / n
    ev = sum(r["x"] for r in rs) / n
    sd = math.sqrt(sum((r["x"] - ev) ** 2 for r in rs) / (n - 1)) if n > 1 else 1.0
    return {"n": n, "wr": round(wr, 3), "ask": round(ask, 3), "edge": round(wr - ask, 4),
            "se": round(math.sqrt(max(wr * (1 - wr), 1e-9) / n), 4),
            "ev": round(ev, 4), "ev_se": round(sd / math.sqrt(n), 4)}


MDE_K = 2.8     # minimum detectable effect = 2.8 SE (80% power, two-sided 5%)


def _diff(k, b):
    """kept-vs-blocked comparison on EV after fee: diff, SE, z and the smallest
    difference this sample could detect (MDE). A 3-day window has n~150, SE ~0.1-0.2
    $/ticket -> MDE ~0.3-0.5, far above the <=~0.05 effects history shows (09-29)."""
    if not (k.get("n", 0) > 1 and b.get("n", 0) > 1):
        return {"ev_diff": None, "ev_diff_se": None, "z": None, "mde": None}
    d = k["ev"] - b["ev"]
    se = math.sqrt(k["ev_se"] ** 2 + b["ev_se"] ** 2)
    return {"ev_diff": round(d, 4), "ev_diff_se": round(se, 4),
            "z": round(d / se, 2) if se else None, "mde": round(MDE_K * se, 4)}


def lasttick_check(cur, asset, rows, now):
    """FORWARD check of the T3 last-tick gate (2026-09-28, shadow in the bot).

    Scored on EVERY in-band window of the 3-day lookback (ask LIVE[1]..LIVE[2], no
    move/velocity filter), not just the ~2/day the live gate trades: same mechanism,
    ~25x the sample. PRICE-ADJUSTED — edge = WR - avg ask — because blocked windows
    trade cheaper (the market prices the reversal; 09-28 first read: blocked ask 0.457
    vs kept 0.589), so raw WR overstates the gate. diff = edge(kept) - edge(blocked);
    z = diff / SE. The gate earns arming only when diff is clearly > 0 forward.
    Rows before the bot stamped `last_tick_bps` are rebuilt at decided_at (~T+187)
    and counted `rebuilt`; trust `stored` as they accumulate."""
    lb, days = lookback(rows, now, is_weekend(now))
    fid = f"{asset.lower()}/usd"
    thr = {}
    kept, blocked = [], []
    tr_kept = tr_blocked = n_stored = n_rebuilt = n_missing = 0
    for r in lb:
        if not LIVE[1] <= r["ask"] <= LIVE[2]:
            continue
        lt = r["lt"]
        if lt is not None:
            n_stored += 1
        elif r["decided_at"] is not None and r["po"]:
            cur.execute("""SELECT benchmark_price FROM chainlink_prices
                            WHERE feed_id=%s AND observations_ts <= %s
                            ORDER BY observations_ts DESC LIMIT 1""",
                        (fid, int(r["decided_at"].timestamp())))
            px = cur.fetchone()
            if not px:
                n_missing += 1
                continue
            lt = (float(px[0]) - float(r["po"])) / float(r["po"]) * 1e4
            n_rebuilt += 1
        else:
            n_missing += 1
            continue
        keep = lt * r["sgn"] > 0
        (kept if keep else blocked).append(r)
        # context only: how many of these the live adaptive gate would actually trade
        t0 = r["ws"].astimezone(timezone.utc)
        t0 = t0.replace(hour=max(b for b in SESSION_START_H if b <= t0.hour),
                        minute=0, second=0, microsecond=0)
        if t0 not in thr:
            thr[t0] = thresholds(rows, t0, K, weekend=r["we"])[0]
        if passes(asset, r, (thr[t0][session(r["ws"])]["min_bps"], LIVE[1], LIVE[2])):
            if keep:
                tr_kept += 1
            else:
                tr_blocked += 1
    k, b = _edge_stats(kept), _edge_stats(blocked)
    return {"lookback_days": [str(d) for d in days], "population": "all in-band windows",
            "kept": k, "blocked": b, "baseline": _edge_stats(kept + blocked), **_diff(k, b),
            "traded_kept": tr_kept, "traded_blocked": tr_blocked,
            "stored": n_stored, "rebuilt": n_rebuilt, "missing": n_missing}


# Cumulative FORWARD test of the last-tick gate (2026-09-29). The 3-day check above
# caps n at ~145 (SE ~±9pp forever) and is re-read every 15 min — optional stopping.
# This one never rolls: stored-only rows (bot-stamped last_tick_bps, nothing rebuilt)
# from LT_FWD_EPOCH (latency + last-tick fixes live), in-band 0.35-0.68 at the
# decision ask, tradeable book. Pre-registered looks on the FIRST N windows only, so
# the verdict of a look is frozen once it is reached. Metric = EV after fee
# (kept - blocked), switched from fee-less edge on 09-29 BEFORE the first look (n=60):
#   look 1 at 400  (~6 days)  - detects a ~17pp diff at z>=3
#   look 2 at 1100 (~17 days) - detects a ~10pp diff at z>=3
# z>=3 (family-wise: last-tick + 5 Fill Δ ranges + K are all under test) -> ARM.
LT_FWD_EPOCH = datetime(2026, 9, 28, 18, 15, tzinfo=timezone.utc)
LT_LOOKS = (400, 1100)
LT_Z_ARM = 3.0
UNTRADEABLE = ("illiquid", "one-sided", "ask too high", "ask —")


def _lt_split(rs):
    kept = [r for r in rs if r["lt"] * r["sgn"] > 0]
    blocked = [r for r in rs if r["lt"] * r["sgn"] <= 0]
    k, b = _edge_stats(kept), _edge_stats(blocked)
    return {"n": len(rs), "kept": k, "blocked": b, "baseline": _edge_stats(rs), **_diff(k, b)}


def lasttick_forward(rows):
    rs = [r for r in rows if r["ws"] >= LT_FWD_EPOCH and r["lt"] is not None
          and LIVE[1] <= r["ask"] <= LIVE[2]
          and not r["nsr"].startswith(UNTRADEABLE)]
    rs.sort(key=lambda r: r["ws"])
    looks, verdict = [], None
    for i, n_look in enumerate(LT_LOOKS):
        if len(rs) < n_look:
            break
        s = _lt_split(rs[:n_look])
        z = s["z"]
        if z is not None and z >= LT_Z_ARM:
            v = "ARM — gate helps"
        elif z is not None and z <= -LT_Z_ARM:
            v = "DO NOT ARM — gate hurts"
        elif i + 1 < len(LT_LOOKS):
            v = f"inconclusive — continue to {LT_LOOKS[i + 1]}"
        else:
            v = "NO EDGE — drop the gate"
        looks.append({"at_n": n_look, "through": rs[n_look - 1]["ws"].isoformat(),
                      **s, "verdict": v})
        verdict = v
        if not v.startswith("inconclusive"):
            break
    nxt = next((n for n in LT_LOOKS if n > len(rs)), None)
    return {"epoch": LT_FWD_EPOCH.isoformat(), "looks_at": list(LT_LOOKS),
            "z_arm": LT_Z_ARM, "running": _lt_split(rs), "looks": looks,
            "next_look": nxt, "verdict": verdict or f"accumulating {len(rs)}/{nxt}"}


FD_BUCKETS = ((float("-inf"), -1000.0), (-1000.0, 0.0), (0.0, 1000.0),
              (1000.0, 3000.0), (3000.0, float("inf")))
FD_MIN_N = 15          # a range needs this many 3-day in-band windows to be judged
FD_BLOCK_Z = 1.0       # block a range only when EV + FD_BLOCK_Z*SE < 0 (EV after fee)


def _fd_label(lo, hi):
    f = lambda v: ("−∞" if v == float("-inf") else "+∞" if v == float("inf")
                   else f"{v:+.0f}")
    return f"{f(lo)}..{f(hi)}"


def filldelta_table(rows_lb):
    """Per Fill Δ range over in-band windows: price-adjusted edge and whether the
    rule blocks it. Blocking needs n >= FD_MIN_N AND edge clearly < 0."""
    out = []
    for lo, hi in FD_BUCKETS:
        xs = [r for r in rows_lb if r["fd"] is not None and LIVE[1] <= r["ask"] <= LIVE[2]
              and lo <= r["fd"] < hi]
        st = _edge_stats(xs)
        blocked = bool(st["n"] >= FD_MIN_N and st["ev"] + FD_BLOCK_Z * st["ev_se"] < 0)
        out.append({"lo": lo if math.isfinite(lo) else None,
                    "hi": hi if math.isfinite(hi) else None,
                    "label": _fd_label(lo, hi), **st, "blocked": blocked})
    return out


def fd_blocked(table, fd):
    if fd is None:
        return False
    for b in table:
        lo = float("-inf") if b["lo"] is None else b["lo"]
        hi = float("inf") if b["hi"] is None else b["hi"]
        if lo <= fd < hi:
            return b["blocked"]
    return False


def filldelta_check(rows, now):
    """Dynamic T3 Fill Δ gate ("so the analyser can dynamically
    set this gate"). Same 3-day same-type lookback as the thresholds; the bot reads
    `blocked_ranges` (shadow unless config t3_filldelta_gate_live=1)."""
    lb, days = lookback(rows, now, is_weekend(now))
    tab = filldelta_table(lb)
    return {"lookback_days": [str(d) for d in days], "buckets": tab,
            "baseline": _edge_stats([r for r in lb if LIVE[1] <= r["ask"] <= LIVE[2]]),
            "blocked_ranges": [[b["lo"], b["hi"]] for b in tab if b["blocked"]],
            "rule": f"block range if n>={FD_MIN_N} and EV+{FD_BLOCK_Z}*SE<0 "
                    f"(EV per $1 after fee, in-band {LIVE[1]}-{LIVE[2]})"}


def vel_ok(asset, r):
    if asset == "BTC":      # live: 4-6bps shoulder needs vel>=0; >=6bps free
        return r["mv"] >= 6.0 or (r["vel"] is not None and r["vel"] >= 0)
    return r["vel"] is not None and r["vel"] > 0     # ETH H2


def passes(asset, r, cell):
    mb, lo, hi = cell
    return lo <= r["ask"] <= hi and r["mv"] >= mb and vel_ok(asset, r)


def session(ts):
    """UTC trading sessions. London/NY overlap is its own bucket: it is the
    noisiest block of the day (BTC weekday median |move| 5.6bps vs 3.3 London)."""
    h = ts.astimezone(timezone.utc).hour
    if h < 7:
        return "asia"
    if h < 12:
        return "london"
    if h < 16:
        return "ldn_ny"
    if h < 21:
        return "ny"
    return "late"


SESSIONS = ("asia", "london", "ldn_ny", "ny", "late")
SESSION_START_H = (0, 7, 12, 16, 21)


def session_start(ts):
    ts = ts.astimezone(timezone.utc)
    h = max(b for b in SESSION_START_H if b <= ts.hour)
    return ts.replace(hour=h, minute=0, second=0, microsecond=0)


def lookback(rows, now, weekend):
    """Last LOOKBACK_DAYS distinct UTC days of the same day-type, strictly before now
    (today's resolved windows count as one of the days)."""
    days = []
    for r in reversed(rows):
        if r["ws"] >= now - timedelta(minutes=5) or r["we"] != weekend:
            continue
        if r["day"] not in days:
            if len(days) == LOOKBACK_DAYS:
                break
            days.append(r["day"])
    keep = set(days)
    return [r for r in rows if r["day"] in keep and r["we"] == weekend
            and r["ws"] < now - timedelta(minutes=5)], sorted(days)


def noise_level(moves):
    """Robust noise of a session: median |move| over ALL its windows (not just the
    staked ones), winsorised at median + 3*MAD so a single news spike in the 3 days
    can't drag the threshold up for the whole session."""
    if len(moves) < NOISE_MIN_WINDOWS:
        return None
    xs = sorted(moves)
    med = xs[len(xs) // 2]
    mad = sorted(abs(v - med) for v in xs)[len(xs) // 2]
    cap = med + 3 * 1.4826 * mad
    w = sorted(min(v, cap) for v in xs)
    return w[len(w) // 2]


def cell_stats(xs, haircut=0.0):
    n = len(xs)
    if n == 0:
        return {"n": 0}
    m = sum(xs) / n
    sd = math.sqrt(sum((v - m) ** 2 for v in xs) / (n - 1)) if n > 1 else 1.0
    return {"n": n, "wr": round(sum(v > 0 for v in xs) / n, 3), "ev": round(m, 4),
            "pnl": round(sum(xs), 2), "lcb": round(m - haircut - Z * sd / math.sqrt(n), 4)}


def thresholds(rows, now, k=K, weekend=None):
    """{session: min_bps} for a day-type (default: now's), from the last 3 same-type
    days. min_bps = clamp(k * session noise, FLOOR, CAP); falls back to the live
    4bps for a session with too few windows."""
    if weekend is None:
        weekend = is_weekend(now)
    lb, days = lookback(rows, now, weekend)
    out = {}
    for s in SESSIONS:
        nl = noise_level([r["mv"] for r in lb if session(r["ws"]) == s])
        mb = LIVE[0] if nl is None else min(CAP_BPS, max(FLOOR_BPS, k * nl))
        out[s] = {"noise_bps": None if nl is None else round(nl, 2), "min_bps": round(mb, 2)}
    return out, weekend, days, len(lb)


def recommend(asset, rows, now, k=K):
    th, weekend, days, n_lb = thresholds(rows, now, k)
    cur_s = session(now)
    # informational: how the recommended per-session gate did on the lookback itself
    lb, _ = lookback(rows, now, weekend)
    xs = [r["x"] for r in lb if passes(asset, r, (th[session(r["ws"])]["min_bps"], LIVE[1], LIVE[2]))]
    return {"asset": asset, "at": now.isoformat(timespec="seconds"),
            "day_type": "weekend" if weekend else "weekday", "session_now": cur_s,
            "min_bps_now": th[cur_s]["min_bps"], "ask_band": [LIVE[1], LIVE[2]], "k": k,
            "lookback_days": [str(d) for d in days], "lookback_windows": n_lb,
            "sessions": th, "lookback_score": cell_stats(xs)}


def measured_haircut(cur, asset, rows, now):
    """mean(replay x) - mean(real pnl per $) over the same windows' real T3 fills."""
    cur.execute(f"""SELECT window_start_utc, realized_pnl_usd / NULLIF(stake_usd, 0)
                    FROM {TABLES[asset]} WHERE book='t3' AND staked AND recon_ok IS NOT NULL
                      AND realized_pnl_usd IS NOT NULL AND leg=0
                      AND window_start_utc >= %s AND window_start_utc < %s""",
                (now - timedelta(days=HAIRCUT_DAYS), now))
    real = {ws: float(p) for ws, p in cur.fetchall() if p is not None}
    rep = {r["ws"]: r["x"] for r in rows if r["ws"] in real}
    common = [ws for ws in real if ws in rep]
    if len(common) < 5:
        return 0.0, len(common)
    gap = sum(rep[w] - real[w] for w in common) / len(common)
    return max(0.0, gap), len(common)


def run_now():
    conn = connect()
    cur = conn.cursor()
    now = datetime.now(timezone.utc)
    out = {"generated_at": now.isoformat(timespec="seconds"), "mode": "read by bot when t3_adaptive_live=1",
           "assets": {}}
    for asset in TABLES:
        rows = load(cur, asset)
        rec = recommend(asset, rows, now)
        rec["fill_haircut"], rec["fill_haircut_n"] = measured_haircut(cur, asset, rows, now)
        # both day-types, so a window that crosses into/out of the weekend between
        # runs still finds its table. The bot reads THIS (t3_min_move_bps).
        # The CURRENT session's threshold is frozen at the session start, exactly as
        # backtest() computes it; recomputing intraday replayed worse (09-28: n=248
        # +$37.91 vs frozen n=252 +$40.53). Other sessions refreeze once they start.
        t0 = session_start(now)
        rec["tables"] = {}
        for dt in ("weekday", "weekend"):
            we = dt == "weekend"
            tab = {s: v["min_bps"] for s, v in thresholds(rows, now, K, weekend=we)[0].items()}
            tab[session(now)] = thresholds(rows, t0, K, weekend=we)[0][session(now)]["min_bps"]
            rec["tables"][dt] = tab
        rec["frozen_at"] = t0.isoformat(timespec="seconds")
        rec["min_bps_now"] = rec["tables"][rec["day_type"]][session(now)]
        try:
            rec["lasttick_check"] = lasttick_check(cur, asset, rows, now)
        except Exception as e:
            conn.rollback()
            rec["lasttick_check"] = {"error": f"{type(e).__name__}: {e}"}
        try:
            rec["lasttick_forward"] = lasttick_forward(rows)
        except Exception as e:
            rec["lasttick_forward"] = {"error": f"{type(e).__name__}: {e}"}
        try:
            rec["filldelta"] = filldelta_check(rows, now)
        except Exception as e:
            rec["filldelta"] = {"error": f"{type(e).__name__}: {e}", "blocked_ranges": []}
        out["assets"][asset] = rec
    with open(STATE_PATH, "w") as f:
        json.dump(out, f, indent=1, default=str)
    with open(HIST_PATH, "a") as f:
        f.write(json.dumps(out, default=str) + "\n")
    for a, r in out["assets"].items():
        sess = " ".join(f"{s}={v['min_bps']}({v['noise_bps']})" for s, v in r["sessions"].items())
        print(f"[{out['generated_at']}] {a} {r['day_type']}/{r['session_now']} min_bps_now="
              f"{r['min_bps_now']} | min(noise): {sess} | lookback {r['lookback_days']}")
        def _f(s):
            return (f"n={s['n']} WR={s['wr'] * 100:.0f}% ask={s['ask']:.2f} "
                    f"EV={s['ev']:+.3f}±{s['ev_se']:.3f}" if s.get("n") else "n=0")

        def _dd(x):
            return (f"EV diff={x['ev_diff']:+.3f} ±{x['ev_diff_se']:.3f} z={x['z']} "
                    f"(detectable >={x['mde']:.2f})" if x.get("ev_diff") is not None
                    else "diff=—")
        lc = r.get("lasttick_check") or {}
        if "error" in lc:
            print(f"    {a} last-tick gate 3d: ERROR {lc['error']}")
        elif lc:
            _d = _dd(lc)
            print(f"    {a} last-tick gate 3d (all in-band, EV after fee): BASELINE "
                  f"{_f(lc['baseline'])} | KEPT {_f(lc['kept'])}"
                  f" | BLOCKED {_f(lc['blocked'])} | {_d} | traded-cohort kept/blocked "
                  f"{lc['traded_kept']}/{lc['traded_blocked']} | rows stored={lc['stored']} "
                  f"rebuilt={lc['rebuilt']} missing={lc['missing']}")
        lf = r.get("lasttick_forward") or {}
        if "error" in lf:
            print(f"    {a} last-tick FORWARD: ERROR {lf['error']}")
        elif lf:
            ru = lf["running"]
            _d = _dd(ru)
            print(f"    {a} last-tick FORWARD since {lf['epoch'][:16]} (stored only, "
                  f"looks at {lf['looks_at']}, arm z>={lf['z_arm']}): n={ru['n']} KEPT "
                  f"{_f(ru['kept'])} | BLOCKED {_f(ru['blocked'])} | {_d} | "
                  f"VERDICT: {lf['verdict']}")
        fdc = r.get("filldelta") or {}
        if fdc.get("buckets"):
            print(f"    {a} fill-Δ 3d: " + " | ".join(
                f"{b['label']} n={b['n']}" + (f" EV {b['ev']:+.3f}±{b['ev_se']:.3f}"
                                              if b["n"] else "") + (" BLOCK" if b["blocked"] else "")
                for b in fdc["buckets"]))
        elif fdc.get("error"):
            print(f"    {a} fill-Δ 3d: ERROR {fdc['error']}")


def _score(xs):
    s = cell_stats(xs)
    if not s["n"]:
        return "n=   0"
    h = len(xs) // 2
    return (f"n={s['n']:4d} WR={s['wr']*100:5.1f}% P&L=${s['pnl']:+7.2f} EV={s['ev']:+.3f} "
            f"halves ${sum(xs[:h]):+.2f}/${sum(xs[h:]):+.2f}")


def backtest():
    """Walk-forward: thresholds recomputed at every session boundary from data
    strictly before it, applied to that session's windows only."""
    conn = connect()
    cur = conn.cursor()
    bounds = (0, 7, 12, 16, 21)
    for asset in TABLES:
        rows = load(cur, asset)
        start = datetime(2026, 8, 5, tzinfo=timezone.utc)
        end = rows[-1]["ws"]
        seg_starts = []
        d = start
        while d < end:
            for h in bounds:
                seg_starts.append(d + timedelta(hours=h))
            d += timedelta(days=1)
        # one threshold table per segment start, shared by every k (noise is k-free)
        noise = []
        for t in seg_starts:
            th, _, _, _ = thresholds(rows, t, 1.0)
            noise.append(th[session(t)]["noise_bps"])
        segs = []
        idx = 0
        for n_i, t in enumerate(seg_starts):
            t_end = seg_starts[n_i + 1] if n_i + 1 < len(seg_starts) else end + timedelta(hours=1)
            while idx < len(rows) and rows[idx]["ws"] < t:
                idx += 1
            j = idx
            while j < len(rows) and rows[j]["ws"] < t_end:
                j += 1
            segs.append((t, noise[n_i], rows[idx:j]))
        print(f"\n===== {asset}  (walk-forward {start.date()} -> {end.date()}, "
              f"{len(segs)} session segments)")
        res = {"live": {"we": [], "wd": []}}
        for k in K_GRID:
            res[k] = {"we": [], "wd": []}
        by_sess = {}
        for t, nl, seg in segs:
            for r in seg:
                key = "we" if r["we"] else "wd"
                if passes(asset, r, LIVE):
                    res["live"][key].append(r["x"])
                    by_sess.setdefault(("live", key, session(r["ws"])), []).append(r["x"])
                for k in K_GRID:
                    mb = LIVE[0] if nl is None else min(CAP_BPS, max(FLOOR_BPS, k * nl))
                    if passes(asset, r, (mb, LIVE[1], LIVE[2])):
                        res[k][key].append(r["x"])
                        if k == K:
                            by_sess.setdefault(("k", key, session(r["ws"])), []).append(r["x"])
        for name in ["live"] + list(K_GRID):
            lab = "LIVE 4bps" if name == "live" else f"k={name:<4}"
            print(f"  {lab:9s} weekend {_score(res[name]['we'])}")
            print(f"  {'':9s} weekday {_score(res[name]['wd'])}")
            print(f"  {'':9s} ALL     {_score(res[name]['we'] + res[name]['wd'])}")
        print(f"  -- per session, live vs k={K}")
        for key in ("wd", "we"):
            for s in SESSIONS:
                a = by_sess.get(("live", key, s), [])
                b = by_sess.get(("k", key, s), [])
                print(f"   {key} {s:7s} live {_score(a)[:40]} | k={K} {_score(b)[:40]}")


def backtest_filldelta():
    """Walk-forward of the DYNAMIC Fill Δ gate: at every session boundary the
    blocked ranges are rebuilt from the prior 3 same-type days only, then applied
    to that session. Compared on the adaptive-gate TRADED cohort and on all in-band
    windows. The 09-27 lesson: re-picking cells from 3-day samples lost walk-forward
    — so this must beat 'no gate' in BOTH halves before arming."""
    conn = connect()
    cur = conn.cursor()
    for asset in TABLES:
        rows = load(cur, asset)
        start = datetime(2026, 8, 1, tzinfo=timezone.utc)   # fill depth from 07-28 + 3d
        end = rows[-1]["ws"]
        res = {"traded": {"base": [], "gated": [], "cut": []},
               "inband": {"base": [], "gated": [], "cut": []}}
        blocks_seen = {}
        d = start
        idx = 0
        while d < end:
            for h in SESSION_START_H:
                t = d + timedelta(hours=h)
                t_end = t + timedelta(hours=(SESSION_START_H + (24,))[
                    SESSION_START_H.index(h) + 1] - h)
                lb, _ = lookback(rows, t, is_weekend(t))
                tab = filldelta_table(lb)
                for b in tab:
                    if b["blocked"]:
                        blocks_seen[b["label"]] = blocks_seen.get(b["label"], 0) + 1
                th = thresholds(rows, t, K)[0][session(t)]["min_bps"]
                while idx < len(rows) and rows[idx]["ws"] < t:
                    idx += 1
                j = idx
                while j < len(rows) and rows[j]["ws"] < t_end:
                    r = rows[j]
                    j += 1
                    if r["fd"] is None or not LIVE[1] <= r["ask"] <= LIVE[2]:
                        continue
                    cut = fd_blocked(tab, r["fd"])
                    for key, ok in (("inband", True),
                                    ("traded", passes(asset, r, (th, LIVE[1], LIVE[2])))):
                        if not ok:
                            continue
                        res[key]["base"].append(r["x"])
                        (res[key]["cut"] if cut else res[key]["gated"]).append(r["x"])
            d += timedelta(days=1)
        print(f"\n===== {asset} dynamic Fill Δ gate, walk-forward {start.date()} -> {end.date()}")
        print(f"  ranges blocked (session-count): {blocks_seen or 'none'}")
        for key in ("traded", "inband"):
            R = res[key]
            print(f"  {key:7s} no gate {_score(R['base'])}")
            print(f"  {'':7s} gated   {_score(R['gated'])}")
            print(f"  {'':7s} removed {_score(R['cut'])}")


if __name__ == "__main__":
    if "--backtest-fd" in sys.argv:
        backtest_filldelta()
    elif "--backtest" in sys.argv:
        backtest()
    else:
        run_now()
