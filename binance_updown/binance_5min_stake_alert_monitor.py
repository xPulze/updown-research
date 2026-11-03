"""
Read-only monitor for the 3-minute-delay / 5-minute-expiry continuation signal,
validated earlier in reports/predicted_vs_actual_5min_24h.xlsx (delay3_* sheets).

This places NO orders and touches no API key — it only reads Binance's PUBLIC
REST market data (no auth required) for BTCUSDT, tracks each 5-minute UTC clock
window, and records the partial move at T+2 and T+3, plus the actual outcome at
T+5. It prints alerts at T+3 (the validated decision point) and continuously
writes the full rolling window history to a JSON file that the your-dashboard-host
/binance dashboard page reads and renders as a live table.

  |partial move| at T+3      backtest accuracy (n, 14-day test)
  0-2 bps     -> no alert (56.7%, coin-flip, n=982)
  2-4 bps     -> WEAK        (69.1%, n=770)
  4-6 bps     -> MODERATE    (82.0%, n=649)
  6-10 bps    -> STRONG      (87.7-88.3%, n=473+307)
  10-20 bps   -> VERY STRONG (89.6-95.2%, n=414+189)
  20+ bps     -> EXTREME     (94.5-96.8%, n=145+95)

Direction: if price already moved UP, bet "Higher" (continuation). If price
already moved DOWN, bet "Lower". T+2 is recorded for visibility/comparison only
— the validated decision point is T+3.

ADX confirmation (added 2026-06-29, validated on >=4bps moves over the same
14-day backtest, n=2272 baseline accuracy 87.9%):
  ADX 15-20  -> 89.9% (n=493)   "confirmed"
  ADX 25-30  -> 89.5% (n=324)   "confirmed"
  ADX 30-35  -> 91.0% (n=233)   "confirmed"
  ADX 20-25  -> 85.8% (n=458)   worse than baseline
ADX is computed from live 1-min klines (pandas_ta, length=14), refreshed every
~20s — far slower-moving than tick price, so this cadence is enough.

NO-STAKE rule (added 2026-06-29, the owner's call): WEAK tier (2-4bps) and ADX in
the unfavourable [20,25) band are both hard "no stake" calls now — the alert
prints "NO STAKE" with the reason instead of "STAKE <side>", and the directional
lean is still shown (labeled "lean=") for visibility but is not a recommendation.

Liquidation imbalance (added 2026-06-29, INFO-ONLY — not a filter): checked
against the same >=4bps baseline and found NO edge (agree/disagree/neutral
with the stake side all landed within ~1-2pp of the 87.9% baseline, in both
the 300s and 900s windows). Displayed for visibility only, no confirmation
badge, sourced live from liq_state.snapshot_windows() (shared in-memory buffer
fed by liq_service.py from Postgres trading_database.liquidations — the same
multi-exchange BTC liquidation feed the live trading bot uses).

You place the actual stake by hand in the Binance app — this script only tells
you when, which side, and a suggested $ stake (see STAKE_PCT_TIERS below), and
gives you the full live T+2/T+3/actual table.

Suggested stake sizing (added 2026-07-01, the owner provisioned 1000 USDT bankroll,
BANKROLL_USDT / BINANCE_STAKE_BANKROLL_USDT env var): fractional-Kelly-inspired
ladder, 1.5% / 2.5% / 3.5% / 5% of bankroll by tier, +1 rung if ADX-confirmed.
Deliberately far below full Kelly — see the comment above STAKE_PCT_TIERS for
why. This is advisory only; you still place the stake by hand.
"""
import json
import os
import sys
import tempfile
import time
import requests
import pandas as pd
import pandas_ta_classic as ta
import psycopg2
from collections import deque
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

sys.path.insert(0, ".")
import config as _C  # noqa: E402 — DB creds for the tick-history mirror
# NOTE (2026-07-04): liq_state is no longer imported here. Its in-memory
# buffer only lazy-loads Postgres ONCE per process and is otherwise fed by
# dashboard.py's live push() — which never runs in this process, so a few
# minutes after every monitor restart snapshot_windows() saw an empty
# window and liq_imbalance_300s/900s persisted as 0.0 (or a degenerate ±1
# right after a restart, when only startup-loaded events were in range).
# The monitor now queries the liquidations table directly at decision time
# (liq_imbalance_from_pg below).

SYMBOL = "BTCUSDT"
BASE = "https://api.binance.com"
IST = ZoneInfo("Asia/Kolkata")


def ist_hm(dt):
    """UTC datetime -> 'HH:MM IST' string, for Telegram alerts only (console/
    dashboard stay UTC — the owner asked specifically for Telegram in IST)."""
    return dt.astimezone(IST).strftime("%H:%M") + " IST"
POLL_SECONDS = 2
ADX_REFRESH_SECONDS = 20

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"


def _tg_send_raw(chat_id, text: str) -> None:
    try:
        requests.post(TELEGRAM_API,
                      data={"chat_id": chat_id, "text": text, "parse_mode": "HTML"},
                      timeout=5)
    except Exception as e:
        print(f"[tg] send to {chat_id} failed: {e}", flush=True)


def tg(text: str) -> None:
    """Fire-and-forget Telegram message to the admin chat only. Never raises —
    alert failure must not interrupt the monitor loop. Use tg_broadcast()/
    tg_personalized() below for user-facing alerts that should also reach
    every viewer who has linked their own Telegram (/binance's 'Set up
    Telegram Alerts' button, added 2026-07-06) — this stays admin-only for
    ops messages (kill-bar trips, auto-gate flips) that assume the owner's own
    context (env var names, "manual restart" language)."""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    _tg_send_raw(TELEGRAM_CHAT_ID, text)


def _linked_telegram_recipients():
    """Every user who has linked their own Telegram chat via /binance's 'Set
    up Telegram Alerts' button (binance_5min_user_capital.telegram_chat_id).
    Best-effort — a DB hiccup here must not interrupt the monitor loop."""
    try:
        pg = _pg_conn()
        if pg is None:
            return []
        with pg.cursor() as cur:
            cur.execute("SELECT username, telegram_chat_id FROM binance_5min_user_capital "
                        "WHERE telegram_chat_id IS NOT NULL")
            rows = cur.fetchall()
        return [{"username": r[0], "chat_id": r[1]} for r in rows]
    except Exception as e:
        print(f"[tg] recipient lookup failed: {e}", flush=True)
        return []


def tg_broadcast(text: str) -> None:
    """User-facing alert, sent verbatim to the admin chat and every linked
    viewer's own chat. Same content everyone already gets today via the
    admin chat, just fanned out — for signals where personalizing the dollar
    figure isn't worth the added risk (value/late legs, early-exit warning);
    those messages already carry a "check the app's own To Win" disclaimer
    covering the case where the figure doesn't match the reader's own stake."""
    tg(text)
    if not TELEGRAM_TOKEN:
        return
    for r in _linked_telegram_recipients():
        _tg_send_raw(r["chat_id"], text)


def tg_personalized(build_msg) -> None:
    """Like tg_broadcast, but `build_msg(capital)` is called once per
    recipient (admin included) with THEIR OWN compounding capital, mirroring
    how BANKROLL_USDT already tracks TELEGRAM_BANKROLL_USERNAME's capital —
    so every linked viewer's stake/result figures match their own dashboard
    instead of showing someone else's dollar amount. Reserved for the two
    highest-stakes messages (main stake alert + its result)."""
    tg(build_msg(BANKROLL_USDT))
    if not TELEGRAM_TOKEN:
        return
    for r in _linked_telegram_recipients():
        cap = get_user_capital(r["username"])
        if cap is None:
            continue
        try:
            _tg_send_raw(r["chat_id"], build_msg(cap))
        except Exception as e:
            print(f"[tg] personalized build/send for {r['username']} failed: {e}", flush=True)
EARLY_OFFSET_MIN = 2       # T+2 (recorded for visibility only)
DECISION_OFFSET_MIN = 3    # T+3 (the validated decision point)
EXPIRY_OFFSET_MIN = 5       # T+5
ALERT_MIN_BPS = 2.0         # below this, don't even print a WEAK alert

# Early-exit warning (added 2026-07-05, the owner: "if the prediction is going
# wrong... give an early exit alert so some loss can be saved"). Once a
# window has been staked, if the live price reverses back through price_T
# and continues this many bps PAST it on the wrong side, that's a strong
# signal the T+3 continuation call is failing mid-flight — fire once
# (console + Telegram + dashboard) so the owner can manually exit the position
# before expiry instead of riding it to a full loss. Not itself validated
# against a backtest (there's nothing to backtest — it's a manual-exit aid,
# not a new stake decision), so the bar is set well above normal noise
# around price_T (ALERT_MIN_BPS=2.0 is the floor for even considering a
# stake) rather than tuned to any target hit rate.
EARLY_EXIT_MIN_REVERSAL_BPS = 4.0

# Fast-move early warning (added 2026-07-06, the owner: "whenever the price delta
# is more than 20 the exchange either gives too little $ or the buy in gets
# blocked... we need to be quick enough when these explosions happen"). A
# raw-$ ping, independent of tier/stake/side logic — no side/stake
# recommendation attached; the real tier decision still comes from the normal
# T+2/T+3/dwell/late/value pipeline.
#
# RE-CALIBRATED 2026-07-07 ("remove the noisy alerts and only alert
# for accuracy >=95%"). A flat $20-any-time rule was firing on ~98% of
# windows (150 pings/12h) but a $20-40 crossing early in a window is barely
# better than a coinflip (68-75% direction-matches-final-settle) — there's
# still 4+ minutes left for price to wander back. Full-history backtest
# (1,577 settled windows, 06-29 tick archive) of "does the crossing's
# direction match the window's actual settle" split by seconds-left-at-
# crossing found NO threshold up to $150 ever reliably clears 95% with
# >=150s still on the clock — that regime is suppressed outright, size alone
# doesn't fix an early ping. Accuracy only clears 95% once time is short
# enough that there's little room left to reverse:
#   150-240s left: never reliably >=95% (tops out ~91-95% around $100-145)
#   90-150s left:  $60  -> 96.2% (n=132)
#   30-90s left:   $50  -> 97.6% (n=124)
#   <30s left:     $30  -> 96.9% (n=32), climbing to 100% by $40+
# See required_fast_move_delta() below for the bucket lookup. NOT a stake
# signal even at 95%+ direction accuracy — this is exactly the big-delta/
# near-expiry zone already known (LATE trigger redesign, 2026-07-04) to
# collapse real payout to $1-5 per $90 once the book has priced the obvious
# side in, so accuracy alone doesn't imply it's profitable to stake.
FAST_MOVE_ALERT_ENABLED = os.getenv("BINANCE_FAST_MOVE_ALERT_ENABLED", "1") not in ("0", "false", "False", "")

# (seconds_left_floor, required_delta_usd) — first matching row (by descending
# seconds-left floor) wins. secs_left >= 150 has no row -> never fires.
FAST_MOVE_DELTA_BANDS = [
    (90.0, 60.0),   # 90-150s left
    (30.0, 50.0),   # 30-90s left
    (0.0, 30.0),    # <30s left
]


def required_fast_move_delta(secs_left):
    """USD delta needed for a >=95%-accuracy fast-move ping at this point in
    the window, or None if this much time left can't reach 95% at any size
    (see FAST_MOVE_DELTA_BANDS comment)."""
    if secs_left is None or secs_left >= 150.0:
        return None
    for floor, delta in FAST_MOVE_DELTA_BANDS:
        if secs_left >= floor:
            return delta
    return None

# Mid-window dwell trigger (added 2026-07-03) — for windows that DON'T
# already qualify for a tier at the T+2 snapshot (that case is handled by
# the T+2-exact early path, see make_stake_decision()), check continuously
# between T+2 and T+3 whether the move crosses the lowest stake-eligible
# floor (MODERATE, 4bps) and HOLDS the same sign for DWELL_SECONDS before
# firing early. Backtested against the full window population (not just
# ones known to already qualify by T+3, to catch false positives that
# stabilize briefly then fizzle): 30s dwell gave 48 early fires at 89.6%
# accuracy (vs 81.7% baseline for this same population waiting to T+3),
# saving ~16s on average, with 85% of those fires being trades that would
# have been staked at T+3 anyway. Shorter dwells (10-20s) catch more but at
# a real (if small) accuracy cost; longer dwells barely save any time. This
# is a thinner, noisier sample (~4 days of tick history) than the T+2-exact
# path, so treat 30s as a starting point, not a permanently fixed constant —
# revisit once more data accumulates.
DWELL_FLOOR_BPS = 4.0       # MODERATE's floor -- lowest stake-eligible threshold
DWELL_SECONDS = 30

# Late-window trigger — a SECOND, independent stake opportunity per window
# in the last LATE_WINDOW_START_SEC before expiry. REDESIGNED 2026-07-04
# (was: cumulative move crosses 6bps + 10s hold, 97.4% backtest accuracy).
# Why the redesign: the owner's real app observations showed the To Win on
# BIG-delta near-expiry stakes collapses to $1-5 per $90 (the book prices an
# already-obvious side near $1 — our calibrated model, fit mostly on
# mid-window points, estimated $7-18 on the same fires). At a real ~3%
# payout the old 94.8%-accuracy fires were breakeven-to-negative EV despite
# the headline accuracy. The RETURN lives at SMALL deltas, where the market
# is still genuinely uncertain: tick replay (1,447 windows / 5 days,
# scratchpad late_delta_study.py) of "delta enters [$15,$20] band, same
# sign held 20s, fire 45-90s before expiry" gives 81.6% accuracy at ~0.38
# model payout = +12.7% EV/trade, n=76 (~15 fires/day, ~3 losers/day) —
# vs the old rule's +10.1% EV at MODEL payout, which the owner's real numbers
# say was fiction. 10s hold doubles the fires but drops accuracy to 77.6%
# (~8 losers/day) — 20s chosen per feedback_minimize_losses. Adding
# T+3-magnitude filters made EV NEGATIVE (a $15-20 delta that used to be
# bigger is a fading move) — deliberately not gated on T+3 tier. The band
# is $-based, not bps-based, because the app's payout economics follow the
# $ delta while bps floors scale with BTC's price. In-sample caveat as
# always: constants from the same 5 days they were scored on; re-validate
# as ticks accumulate.
LATE_TRIGGER_ENABLED = os.getenv("BINANCE_LATE_TRIGGER_ENABLED", "0") not in ("0", "false", "False", "")
LATE_WINDOW_START_SEC = 90
LATE_DELTA_MIN_USD = 15.0   # band floor — below this it's a coin flip (63-72%)
LATE_DELTA_MAX_USD = 20.0   # band cap — above this the real To Win dies off
LATE_HOLD_SECONDS = 20
LATE_TRIGGER_BACKTEST_ACC = 0.798
# Historical redesigned fires through 2026-08-29 under the current ADX/NY
# gates and the user-requested ask<=0.85 cap: 260W/66L, 79.8% WR and -2.47%
# model EV per stake. The high headline WR is below breakeven at the average
# price paid, so the newly re-enabled signal remains at a fixed conservative
# fraction; do not Kelly-size it without positive out-of-sample EV.
LATE_STAKE_PCT = 0.01

# Binance Event Contracts are filled manually and, in practice, the Buy
# button/order is no longer actionable once the favoured-side ask moves above
# 0.85. The exchange does not expose that ask via API, so apply the cap to the
# model-implied ask (1 / (1 + payout)) for every suggested-stake leg.
MAX_MANUAL_ASK = float(os.getenv("BINANCE_MAX_MANUAL_ASK", "0.85"))
# Minimum seconds remaining at fire time for the alert to be worth sending
# (added 2026-07-03, after the FIRST live fire came with only 6s left).
# Raised 30->45 on 2026-07-04 after the owner confirmed the real "Buy Up" button
# was DISABLED with 35s still on the clock on a real ~8.3bps window (order
# book had no ask-side liquidity left — the move was decisive enough that no
# one would sell the losing side). This means the order cutoff is NOT the
# flat ~5s previously assumed (that was only ever a confirmed lower bound,
# from a single 0:02 observation, never verified as the true edge) — it can
# be liquidity-driven and arrive earlier on exactly the large, one-sided
# moves this trigger targets. 45s is a safety margin pending more real
# lockout observations, not a re-derived constant — a fire that can't be
# acted on is worse than no fire at all (alert noise). Crossings whose hold
# would complete with less than this remaining simply don't fire.
LATE_MIN_ACTION_SECONDS = 45

# Live kill bar (added 2026-07-05): the redesigned band opened its first
# half-day at 2/5 (-$197) — a ~1-in-20 outcome if the 81.6% replay accuracy
# is real, which is a warning, not yet proof the replay was wrong. Rather
# than killing a day-old redesign on n=5, this arms an explicit tripwire:
# once the post-redesign leg has LATE_KILL_MIN_FIRES decided stakes, if
# live accuracy sits below LATE_KILL_ACC (chosen under the ~74% breakeven
# at the band's real ~36% payout, with margin so variance alone can't trip
# it), the trigger disables itself and alerts — no silent decay. Only rows
# fired at/after LATE_REDESIGN_TS count: earlier rows are the old 6bps
# rule, a different trigger entirely (see project memory — don't mix).
# Latches off once tripped; re-enabling is a manual call (same no-auto-
# resume posture as the go-live hardening).
LATE_KILL_MIN_FIRES = int(os.getenv("BINANCE_LATE_KILL_MIN_FIRES", "15"))
LATE_KILL_ACC = float(os.getenv("BINANCE_LATE_KILL_ACC", "0.65"))
LATE_REDESIGN_TS = datetime(2026, 7, 4, 17, 36, tzinfo=timezone.utc)

# Paper mode (added 2026-07-05, the owner: "disable until it proves itself"):
# the trigger keeps running its state machine and RECORDS every fire in
# binance_5min_late_legs, but no fire ever stakes (stake_ok forced False,
# reason "paper mode") and no Telegram stake alert goes out. Fully
# disabling via LATE_TRIGGER_ENABLED would stop the record entirely and
# nothing could ever clear the trigger; paper mode keeps building the live
# sample money-free. The dashboard reads late_paper_mode from the state
# JSON and retro-treats ALL late legs (including the pre-paper staked ones)
# as no-stake in counts/P&L — same display-level retro pattern as
# NO_STAKE_TIERS, raw rows untouched. Proving bar (manual re-enable): >=15
# decided paper fires that WOULD have staked, at accuracy comfortably above
# the ~74% breakeven at the band's real ~36% payout.
LATE_PAPER_MODE = os.getenv("BINANCE_LATE_PAPER_MODE", "0") not in ("0", "false", "False", "")

# VALUE trigger (added 2026-07-04) — the THIRD stake signal per window,
# between the primary decision and the late leg's 90s window. Idea (the owner's):
# after the direction is confirmed, the market often RETRACES toward the
# strike while keeping its sign — the direction call still stands, but the
# order book re-prices that side much closer to even money, so the same bet
# suddenly pays 30-50% instead of ~25%. Fire at that value moment.
# Validated on 450 real decided windows (tick history since Jul 1), firing
# when calibrated est. payout >= target with the sign stable 10s, decision..
# expiry-90s scan range:
#   all tiers @30%:      208 fires, 78.8% WR, 44 losers  (retraces on weak
#   MODERATE tiers @30%: 158 fires, 75.3% WR, 39 losers   tiers are too often
#                                                          real reversals)
#   STRONG+ @30%, 10s:    31 fires, 93.5% WR,  2 losers, avg payout 37.5%,
#                         EV +28.7%/trade — the highest per-trade EV of any
#                         leg in the system; chosen per feedback_minimize_losses.
# Same small-sample caveat as the other triggers: revisit as data accumulates.
VALUE_TRIGGER_ENABLED = os.getenv("BINANCE_VALUE_TRIGGER_ENABLED", "0") not in ("0", "false", "False", "")
VALUE_PAYOUT_TARGET = 0.30
VALUE_DWELL_SECONDS = 10
VALUE_TIERS = {"STRONG", "VERY STRONG", "EXTREME"}
VALUE_STAKE_PCT = 0.01  # flat, same no-Kelly-until-sample posture as LATE_STAKE_PCT

# ---------------------------------------------------------------------------
# SWING_FADE (added 2026-07-26) — the FOURTH signal, and the only leg in this
# file whose edge survived measurement. It BUYS THE LOSING SIDE: after T+3,
# when the market has moved enough that the opposite contract is cheap, take
# the cheap side and hold to expiry. It is a fade, not a continuation bet, so
# it is deliberately NOT gated on tier/ADX/ATR — those all select for
# continuation and would invert it.
#
# Where the rule comes from: Polymarket's SWINGFADE book, ported onto the
# Binance 5-min clock (PM windows are also 5 minutes, so PM's "elapsed 180s"
# is exactly Binance T+3). Validated on 14 days of REAL two-sided quotes
# captured in polymarket_updown_t2probe / re_flip_shadow (250,236 snapshots,
# 4,086 BTC windows), with outcomes settled on the BINANCE feed — per-window,
# first qualifying snapshot, one fire per window:
#   cheap ask 0.03-0.235, elapsed >= 180s : n=425, 86W, WR 20.2% vs 15.4%
#       breakeven, EV +31.7%, bootstrap 95% CI [+0.12, +0.78] per $1, p=0.004
#   the same band restricted to PM's own 180-220s : n=106, EV +38.8%, p=0.047
# The BROAD band is the stronger cell — narrowing to 180-220s cuts n by 4x and
# WEAKENS significance. That is why FADE_ELAPSED_MAX_SEC is the whole window
# rather than PM's 220s cap. Cells tightened further on adx_slope/excursion
# score higher still but are re-fits on the data the rule was chosen from;
# they are deliberately NOT encoded here.
#
# THREE caveats that must survive any future edit of this block:
#  1. The asks above are POLYMARKET's, from a CLOB. Binance Event Contracts
#     are house-quoted and publish NO orderbook API — there is no Binance ask
#     anywhere in this system. cheap_ask_now() below is therefore a MODEL
#     price (1 - assumed market price of the favoured side), not an observed
#     one. If a real Binance quote capture ever lands, re-derive the band on
#     it before trusting these numbers.
#  2. Binance and Chainlink (which PM settles on) disagree on 4.87% of
#     windows — a hard floor on how well this transfers.
#  3. The spread check that gates the whole idea passed on PM: the two sides
#     sum to 1.0108 mean / 1.0100 median, and only 0.83% of snapshots exceed
#     1.034, which is where this edge would break even. Binance's own spread
#     is unmeasured.
FADE_TRIGGER_ENABLED = os.getenv("BINANCE_FADE_TRIGGER_ENABLED", "1") not in ("0", "false", "False", "")

# FADE-ONLY MODE (added 2026-07-26, the owner's call after the measurement below).
# When False, every CONTINUATION leg — the primary T+2/dwell/T+3 decision, the
# LATE leg and the VALUE leg — goes alert-only: windows still tier, alert to
# console, persist, and accumulate accuracy history, they just never stake.
# SWING_FADE is untouched and keeps firing.
#
# Why: on post-refit data (>= 2026-07-05 12:00, n=3,045 windows / 21 days) the
# continuation side has no edge left to trade. Per tier, EV vs the price
# actually paid: EXTREME +6.6% (n=40), VERY STRONG +1.8% (n=80), STRONG +0.19%
# (n=1,199 — flat on the only sample big enough to trust), MODERATE-STRONG
# -0.70%, MODERATE -3.71%, WEAK -4.29%. Confirmed independently on 4,023
# windows of REAL two-sided quotes: no ask threshold from 0.60 to 1.00 produces
# positive EV (all p >= 0.72). The book is efficiently priced; the fade is the
# only leg that measured positive.
#
# This is a DELIBERATE STOP, not a bug and not a gate to "fix". Turning it back
# on is a decision to trade a leg that has been measured flat — do that only
# with new evidence, not because the dashboard looks quiet. Alert-only is
# chosen over a hard disable so the accuracy record keeps building for exactly
# that future re-evaluation (same posture as NO_STAKE_TIERS).
CONTINUATION_STAKE_ENABLED = os.getenv("BINANCE_CONTINUATION_STAKE_ENABLED", "1") not in ("0", "false", "False", "")
FADE_ELAPSED_MIN_SEC = float(os.getenv("BINANCE_FADE_ELAPSED_MIN_SEC", "180"))
FADE_CHEAP_ASK_MIN = 0.03
# PM ships 0.235. TIGHTENED to 0.16 for Binance — and this one IS fit on
# Binance replay, not ported, so it carries a weaker provenance than the rest
# of this block and is called out as such. Reason it had to move: at PM's
# 0.235 the leg fires 118x/day, and FILLS ON THIS PRODUCT ARE MANUAL. A
# signal nobody can act on is noise, and the same "a fire that can't be acted
# on is worse than no fire" rule already governs LATE_MIN_ACTION_SECONDS.
#
# Volume/EV surface over 7,703 windows / 26 days (excursion cap x ask cap,
# spread-inclusive asks, one fire per window). EV is POSITIVE in 19 of the 20
# cells swept, so the direction is robust rather than a lone survivor — but
# the magnitude AT THE SELECTED CELL is optimistically biased by having
# chosen it, and should not be quoted as the expected forward return:
#     exc<=4  ask<=0.120 :  14.3/day  WR 15.3%  EV +90.1%  p=0.0000
#     exc<=4  ask<=0.160 :  22.7/day  WR 15.4%  EV +35.7%  p=0.0017   <- shipped
#     exc<=5  ask<=0.200 :  54.3/day  WR 18.5%  EV +34.6%  p=0.0000
#     exc<=8  ask<=0.235 : 117.9/day  WR 16.7%  EV +17.5%  p=0.0001   (PM's band)
# 0.120 scores best but sits at the largest moves, which is exactly where the
# 2026-07-04 buy-button lockout was observed — best EV, worst fillability. The
# 0.160 cell is the compromise: actionable volume, still p<0.01, and not at
# the extreme of the move distribution.
FADE_CHEAP_ASK_MAX = float(os.getenv("BINANCE_FADE_CHEAP_ASK_MAX", "0.16"))
# Sign must hold this long before firing — same anti-noise device as the other
# legs. 10s matches VALUE_DWELL_SECONDS; not independently tuned (tuning it on
# the same 14 days would be the re-fit this block already refuses).
FADE_DWELL_SECONDS = 10.0
# Flat 1% of bankroll. NOT Kelly: Kelly at a 20% win rate against a ~5.5x
# payout would size far larger, on an n=425 in-sample estimate, which is
# exactly what feedback_full_run_validation_discipline forbids. On the $200
# cap this is a $2 ticket — deliberately small until live sample exists.
FADE_STAKE_PCT = float(os.getenv("BINANCE_FADE_STAKE_PCT", "0.01"))
# A fade needs less lead time than a continuation bet (it is the side the
# market WANTS to sell — see fill_probability's note), but a fire nobody can
# act on is still noise. 20s vs LATE's 45s.
FADE_MIN_ACTION_SECONDS = float(os.getenv("BINANCE_FADE_MIN_ACTION_SECONDS", "20"))
# Held to expiry, no exit — matches the policy validated for PM swingfade
# (HOLD beat the bid-0.30 stop by +9,453 bps/trade over 47 fires, bootstrap
# P(HOLD>SL) 98.7%). There is deliberately no fade early-exit path.
#
# PATH GATES — carried over from PM's SHIPPED swingfade_ok(), not invented
# here and not re-fit. Without them this leg fires on ~87% of windows (238/day
# in replay) because a continuously-scanned 120s stretch always finds some
# tick inside the ask band; PM's own rule qualifies ~42% of candidate windows.
# The excursion caps say "the move that made the other side cheap was ORDERLY"
# — a window that already spiked 8bps and came back is not a swing to fade,
# it is a trend. crossings != 2 is PM's own 69-trade cell whose neighbours
# flip sign (most likely noise, kept because it is part of the rule as
# shipped); set BINANCE_FADE_CROSS_EXCLUDE="" to disable just that clause.
# PM ships 8.0. Tightened to 4.0 on the same volume/EV surface documented at
# FADE_CHEAP_ASK_MAX — same weaker, fit-on-Binance provenance.
FADE_MAX_EXCURSION_BPS = float(os.getenv("BINANCE_FADE_MAX_EXC_BPS", "4.0"))
_x = os.getenv("BINANCE_FADE_CROSS_EXCLUDE", "2").strip()
FADE_CROSS_EXCLUDE = int(_x) if _x else None


# Two-sided spread charged on the cheap side. The naive model cheap ask is
# 1 - p_favoured, which silently assumes BOTH sides sum to exactly 1.00 — a
# costless book that does not exist anywhere. Measured on 246,494 real
# two-sided snapshots of a comparable 5-min BTC binary (Polymarket, 14 days):
# ask_up + ask_down = 1.0108 mean, 1.0100 median, p99 1.0300. 0.011 is that
# median excess, charged in full to the side we buy (conservative: it is
# really split across both sides).
#
# This is NOT cosmetic. It is the difference between a fade edge of +31% and
# one of roughly +15%, and the whole idea breaks even at a total spread of
# 1.034. Binance's own spread has never been measured; if it is wider than
# PM's, this constant is the single number to correct.
FADE_SPREAD_ADJ = float(os.getenv("BINANCE_FADE_SPREAD_ADJ", "0.011"))


def cheap_ask_now(abs_bps, seconds_remaining, include_spread=True):
    """Estimated price we would actually PAY for the LOSING side right now:
    (1 - model price of the favoured side) + FADE_SPREAD_ADJ. Binance
    publishes no orderbook, so the base is derived from the same
    MARKET_Z_KNOTS screenshot fit the rest of this file prices with.

    The spread term matters for correctness, not just display — the band
    [FADE_CHEAP_ASK_MIN, FADE_CHEAP_ASK_MAX] was validated against PM asks
    that ALREADY included a spread, so band membership has to be tested on a
    spread-inclusive price or the two are not the same quantity.

    Known blind spot: the fit is clamped flat above 10bps (no favoured-side
    screenshot beyond 9.2bps exists), so the base cannot fall below ~0.060 no
    matter how large the move. The 0.03-0.06 slice of the band is therefore
    UNREACHABLE in practice — real Binance would quote it, we just cannot see
    it. Fires cluster in 0.07-0.235."""
    if abs_bps is None or seconds_remaining is None or seconds_remaining <= 0:
        return None
    b = assumed_payout_for_time(abs_bps, seconds_remaining)
    if b is None:
        return None
    p_favoured = 1.0 / (1.0 + b)          # invert payout -> implied price
    cheap = 1.0 - p_favoured
    if include_spread:
        cheap += FADE_SPREAD_ADJ
    return max(0.0, min(1.0, cheap))


def fill_probability(ask, seconds_remaining):
    """MODELLED probability that a manual order at `ask` actually fills, with
    `seconds_remaining` on the clock. Returns (probability, label).

    THIS IS A PRIOR, NOT A MEASUREMENT. Fills on this product are manual and
    no fill attempt is recorded anywhere in the system, so there is zero
    observed fill data to fit. It is emitted so every signal carries an
    explicit, falsifiable fillability claim that logged attempts can later
    grade — not because the number is known.

    Shape comes from the two things we do know:
      - The one hard observation (2026-07-04): the real "Buy Up" button was
        DISABLED with 35s still on the clock on a ~8.3bps move, because the
        ask side had no liquidity left. Nobody sells the losing side of an
        already-decided window. So P(fill) must fall as ask -> 1 and as the
        clock runs out. See feedback_binance_order_cutoff_correction.
      - The cheap side is the side the market wants to sell, so it should be
        the more fillable one. NOTE this mechanism is NOT confirmed: on
        Polymarket the opposite holds (the expensive side carries 4-50x more
        depth — median $9,006 fillable at ask 0.95-1.00 vs $93 at 0.10-0.15),
        but PM is a CLOB and Binance is house-quoted, so PM depth does not
        transfer. Treat the cheap-side optimism here as the weakest part of
        this function and the first thing to correct from real attempts."""
    if ask is None or seconds_remaining is None:
        return None, "unknown"
    # price term: flat below 0.80 (the market is still genuinely uncertain,
    # someone will take the other side), then decays to near-zero as the
    # outcome becomes obvious.
    if ask <= 0.80:
        price_term = 1.0
    else:
        price_term = max(0.10, 1.0 - (ask - 0.80) / 0.20 * 0.90)
    # time term: full below 90s-to-go, tapering as the book thins into expiry.
    if seconds_remaining >= 90.0:
        time_term = 1.0
    elif seconds_remaining >= 20.0:
        time_term = 0.25 + (seconds_remaining - 20.0) / 70.0 * 0.75
    else:
        time_term = 0.05
    p = max(0.0, min(1.0, price_term * time_term))
    label = ("likely" if p >= 0.75 else "even" if p >= 0.45
             else "doubtful" if p >= 0.20 else "unlikely")
    return p, label

# T+2 backward-dwell filter (added 2026-07-04) — confirmation for the
# T+2-exact early path that costs ZERO waiting (and therefore zero To-Win):
# instead of holding the fire for N seconds AFTER T+2 (rejected — payout
# decays with elapsed time, see assumed_payout_for_time), it checks the
# ticks ALREADY SEEN at the T+2 snapshot. Both real $450 early-path losers
# (2026-07-04 03:30 and 16:00 UTC) fired AT the exact extreme of a move
# that had held its tier floor for only 17-19.5s before T+2, then fully
# reversed by T+3 (the original T+3 rule would have staked nothing).
# Tick replay, 1,440 reconstructed windows / 5 days, STRONG+ floor,
# NY-open block mirrored: requiring the floor already held >=30s before
# T+2 AND price within 5bps of its favorable extreme cut losers 52 -> 32
# (-38%) at 89.8% -> 93.0% accuracy, EV +67.8 -> +64.4 stake-units (within
# noise), with both live losers never firing at all (reversed by T+3, so
# the deferral to the dwell/T+3 paths skipped them entirely — which also
# silences the VALUE leg, since it only fires after a staked primary).
# Windows that fail this filter are NOT rejected — they just fall through
# to the existing mid-window dwell / T+3 paths, exactly as if the T+2
# snapshot hadn't tier-qualified. 20s would have been knife-edge (0.5s
# above the 16:00 loser's 19.5s); 30s chosen for margin. In-sample caveat:
# thresholds picked on the same 5 days they were evaluated on — re-validate
# as binance_5min_ticks accumulates (scratchpad backward_filter_test2.py).
T2_FILTER_ENABLED = os.getenv("BINANCE_T2_BACKWARD_FILTER_ENABLED", "0") not in ("0", "false", "False", "")
T2_BACKWARD_DWELL_SECONDS = 30.0  # floor must have been held this long pre-T+2
T2_MAX_RETRACE_BPS = 5.0          # max giveback from the favorable extreme
# The study measured the backward hold against the flat STRONG entry floor
# (8bps) for ALL STRONG+ fires — NOT each tier's own floor (a 12bps move
# only needs to have HELD >=8bps, not >=10). MODERATE/MODERATE-STRONG
# (4-8bps) T+2 fires are out of the validated scope and pass through
# unfiltered (they're 0.5-1% stakes with their own low-ADX gate).
T2_FILTER_MIN_BPS = 8.0           # filter scope + the backward hold bar

# T+3/dwell backward filter (added 2026-07-05, "prune out bad cases" —
# STRONG's real payout is thin enough that breakeven accuracy runs 90-94%
# depending on timing, so a full-stake loss costs far more than a thin win
# earns). Reconstructed STRONG/VERY STRONG/EXTREME T+3 decisions over the
# full 18-month `candles` history: baseline 93.25% (n=47,111), robust across
# all 4 quarters (92.4-94.7%). Excluding windows that either (a) flipped
# sign between T+2 and T+3, or (b) shrank from T+2 to T+3 AND retraced more
# than T3_MAX_RETRACE_BPS off their own [T+2,T+3] favorable extreme, raises
# the KEPT population to ~94.5% (clearing the ~94% T+3-timing breakeven bar)
# while the EXCLUDED ~22% sits at ~88-90% (clearly below breakeven) — held
# in all 4 quarters individually (kept 93.9-95.6% vs excluded 87.1-90.2%).
# Unlike t2_backward_filter, this does NOT defer to a later checkpoint —
# T+3/dwell are already the last main-decision checkpoints, so a failed
# check here is a genuine no-stake for that window (the LATE/VALUE triggers
# remain separate, independently-gated rescue mechanisms untouched by this).
# MODERATE-STRONG added 2026-07-05 alongside the ATR-ratio filter below —
# the SAME fading-move check also improves it on its own (within its
# existing ADX<15 scope: 86.3% -> 88.5%), validated the same way.
T3_BACKWARD_FILTER_ENABLED = os.getenv("BINANCE_T3_BACKWARD_FILTER_ENABLED", "0") not in ("0", "false", "False", "")
T3_BACKWARD_FILTER_TIERS = {"STRONG", "VERY STRONG", "EXTREME", "MODERATE-STRONG"}
T3_MAX_RETRACE_BPS = 3.0

# ATR-ratio filter (added 2026-07-05, "look deeper... 98% accuracy" — ADX
# turned out to be the wrong lens; what actually separates winners from
# losers is how big THIS move is relative to how volatile the market has
# recently been, not whether it's currently trending). Reconstructed the
# full 18-month `candles` history with Wilder's ATR(14) expressed as bps of
# price: on top of the fading-move filter above, requiring
# abs(decision_bps)/ATR_bps >= ATR_RATIO_MIN raises STRONG/VERY STRONG/
# EXTREME to 98.4% (n=15,352, 31.5% of pool) and MODERATE-STRONG (within its
# existing ADX<15 scope) to 98.2% (n=224, 10.3% of that pool) — both held
# in EVERY one of 4 independent quarters (98.38-98.53% and 95.65-100%
# respectively), not a fluke. Volume (candles.volume) was also tested as a
# third dimension — weaker than ATR alone and added nothing once stacked on
# top of the ATR ratio, so it's deliberately not part of this filter.
ATR_RATIO_FILTER_ENABLED = os.getenv("BINANCE_ATR_RATIO_FILTER_ENABLED", "0") not in ("0", "false", "False", "")
ATR_RATIO_FILTER_TIERS = {"STRONG", "VERY STRONG", "EXTREME", "MODERATE-STRONG"}
ATR_RATIO_MIN = 2.0
ATR_PERIOD = 14

# T+2(early)-STRONG disable (added 2026-07-06) — see make_stake_decision's
# comment at the point this is checked for the full loser-analysis writeup.
# Defaults ON: the path runs below breakeven both before and after
# t2_backward_filter shipped, so this is a straight stop, not experimental.
T2_EARLY_STRONG_DISABLED = os.getenv("BINANCE_T2_EARLY_STRONG_DISABLED", "1") not in ("0", "false", "False", "")

# Minimum expected-return floor (added 2026-07-05, "if we have only 2 or 3
# dollar return no point sending the signal") — applied to ALL THREE signal
# types (main STAKE, LATE, VALUE): below this expected $ payout on
# BANKROLL_USDT, the signal is treated as full no-stake, not just a
# silenced Telegram alert.
#
# LOWERED 5.0 -> 1.0 on 2026-07-26. The $5 floor silently darkened the entire
# book: ZERO stakes fired between 2026-07-07 and 2026-07-26 while 25-79
# STRONG+ windows qualified per day. Cause is arithmetic, not a bug — the
# floor is an ABSOLUTE $ test applied to
#     BANKROLL_USDT x stake_pct x assumed_payout,
# and every real user is capped at $200 (binance_5min_user_capital, see
# project_binance_capital_cap_200). On $200, STRONG's real payout (median
# 0.088 post-refit) at its ladder rung yields a median expected return of
# $1.42 — a $5 floor passes 2.0% of STRONG+ windows, $2 passes 15.4%, $1
# passes 97.7%. The floor and the $200 cap were mutually unsatisfiable.
# 1.0 is chosen as the largest floor that still lets the book run at the
# capital that actually exists; it is a liveness constant, NOT an edge claim
# (see the payout-floor finding recorded against BANKROLL_USDT below).
# SECOND PASS, same day: 1.0 was still a kill switch. At the $200 cap a STRONG
# window sizes to ~$8.20 and the real T+3 payout for 8-13bps is 0.064 (the
# market z-curve clamps at z=1.5562 -> p=0.940 above 10bps), so expected return
# is $0.52 — under a $1 floor. Only EXTREME ($20 stake) cleared it. The first
# fix moved the number without fixing the DEFECT, which is that an ABSOLUTE $
# floor is meaningless against a return that scales with bankroll: it is
# permissive at $1000 and a total block at $200, with no warning either way.
#
# Now expressed as a FRACTION of bankroll, resolved at decision time (bankroll
# is refreshed from the user's capital every 60s, so an import-time constant
# would go stale). 0.2% of bankroll: $0.40 at the $200 cap, $2 at $1000 —
# proportional at any capital, and it can never again silently darken the book
# because someone changed the cap. The absolute env var still wins if set, for
# a hard manual override.
MIN_EXPECTED_RETURN_FRAC = float(os.getenv("BINANCE_MIN_EXPECTED_RETURN_FRAC", "0.002"))
_MIN_EXP_RET_ABS = os.getenv("BINANCE_MIN_EXPECTED_RETURN_USDT", "").strip()
MIN_EXPECTED_RETURN_USDT = float(_MIN_EXP_RET_ABS) if _MIN_EXP_RET_ABS else None


def min_expected_return():
    """Minimum expected $ profit for a signal to be worth acting on, at the
    CURRENT bankroll. Absolute env override wins; otherwise a fixed fraction
    of bankroll (see MIN_EXPECTED_RETURN_FRAC).

    IMPORTANT, and the reason this is a floor rather than an edge test: on
    post-refit data the T+3 continuation leg has essentially no edge left
    (STRONG +0.19% over n=1,199 — see the note on BANKROLL_USDT). This
    function decides whether a signal is worth a human's attention, NOT
    whether it is profitable. Do not read a passing window as a good bet."""
    if MIN_EXPECTED_RETURN_USDT is not None:
        return MIN_EXPECTED_RETURN_USDT
    return max(0.10, MIN_EXPECTED_RETURN_FRAC * BANKROLL_USDT)


def implied_ask(payout):
    """Return the contract ask implied by a profit/stake payout ratio."""
    if payout is None or payout < 0:
        return None
    return 1.0 / (1.0 + payout)


def manual_ask_block_reason(payout):
    """Explain an unfillable model ask, or return None when it is actionable."""
    ask = implied_ask(payout)
    if ask is not None and ask > MAX_MANUAL_ASK:
        return f"model ask {ask:.3f} > {MAX_MANUAL_ASK:.2f} manual-fill cap"
    return None

MAX_HISTORY = 60            # rows kept in the live state file (~5h at 5min/window)
TICK_BUFFER_MINUTES = 12    # how much raw tick history to keep for the live chart
TICK_BUFFER_LEN = int(TICK_BUFFER_MINUTES * 60 / POLL_SECONDS)

STATE_PATH = "./reports/binance_5min_live_state.json"
TICK_DB_FLUSH_SECONDS = 10  # batch ticks before writing to Postgres, like liq_service's batch writer

_PG_CONN = None


def _pg_conn():
    global _PG_CONN
    try:
        if _PG_CONN is None or _PG_CONN.closed:
            _PG_CONN = psycopg2.connect(
                host=_C.DB_HOST, dbname=_C.DB_NAME, user=_C.DB_USER, password=_C.DB_PASSWORD
            )
            _PG_CONN.autocommit = True  # mixed read/write poll loop; avoid parking an open transaction for days
        return _PG_CONN
    except Exception as e:
        print(f"[{datetime.now(timezone.utc).isoformat()}] tick DB connect failed: {e}", flush=True)
        return None


def flush_ticks_to_db(buffer):
    """Best-effort batch insert of buffered (ts, price) ticks. Never raises —
    a DB hiccup must not interrupt the live monitor loop."""
    if not buffer:
        return
    try:
        pg = _pg_conn()
        if pg is None:
            return
        with pg.cursor() as cur:
            cur.executemany(
                "INSERT INTO binance_5min_ticks (ts, price) VALUES (%s, %s) "
                "ON CONFLICT (ts) DO NOTHING",
                buffer,
            )
        pg.commit()
    except Exception as e:
        print(f"[{datetime.now(timezone.utc).isoformat()}] tick DB flush error "
              f"({len(buffer)} rows): {e}", flush=True)


def ensure_windows_table():
    """Best-effort CREATE TABLE IF NOT EXISTS — table is also created out-of-band,
    this is just so a fresh environment doesn't silently no-op forever."""
    try:
        pg = _pg_conn()
        if pg is None:
            return
        with pg.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS binance_5min_windows (
                    anchor              TIMESTAMPTZ PRIMARY KEY,
                    price_t             DOUBLE PRECISION,
                    price_t2            DOUBLE PRECISION,
                    bps_t2              DOUBLE PRECISION,
                    price_t3            DOUBLE PRECISION,
                    bps_t3              DOUBLE PRECISION,
                    tier                TEXT,
                    tier_acc            DOUBLE PRECISION,
                    side                TEXT,
                    adx                 DOUBLE PRECISION,
                    adx_confirmed       BOOLEAN,
                    adx_unfavourable    BOOLEAN,
                    stake_ok            BOOLEAN,
                    stake_usdt          DOUBLE PRECISION,
                    stake_pct           DOUBLE PRECISION,
                    liq_imbalance_300s  DOUBLE PRECISION,
                    liq_imbalance_900s  DOUBLE PRECISION,
                    price_t5            DOUBLE PRECISION,
                    bps_t5              DOUBLE PRECISION,
                    actual_dir          TEXT,
                    correct             TEXT,
                    status              TEXT NOT NULL,
                    decided_at          TIMESTAMPTZ,
                    expired_at          TIMESTAMPTZ,
                    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
                );
                CREATE INDEX IF NOT EXISTS idx_binance_5min_windows_status
                    ON binance_5min_windows(status);
                ALTER TABLE binance_5min_windows
                    ADD COLUMN IF NOT EXISTS assumed_payout DOUBLE PRECISION,
                    ADD COLUMN IF NOT EXISTS est_return_usdt DOUBLE PRECISION,
                    ADD COLUMN IF NOT EXISTS decided_early BOOLEAN,
                    ADD COLUMN IF NOT EXISTS decision_trigger TEXT,
                    ADD COLUMN IF NOT EXISTS weak_edge BOOLEAN,
                    ADD COLUMN IF NOT EXISTS t0_lean_side TEXT;
            """)
        pg.commit()
    except Exception as e:
        print(f"[{datetime.now(timezone.utc).isoformat()}] windows table ensure failed: {e}", flush=True)


def persist_window_decision(w_anchor, w):
    """Upsert the T+3 decision fields. Best-effort, never raises — a DB hiccup
    must not interrupt the live monitor loop or block the T+3 alert."""
    try:
        pg = _pg_conn()
        if pg is None:
            return
        with pg.cursor() as cur:
            cur.execute("""
                INSERT INTO binance_5min_windows (
                    anchor, price_t, price_t2, bps_t2, price_t3, bps_t3,
                    tier, tier_acc, side, adx, adx_confirmed, adx_unfavourable, atr_bps,
                    stake_ok, stake_usdt, stake_pct, assumed_payout, decided_early, decision_trigger,
                    weak_edge, liq_imbalance_300s, liq_imbalance_900s, t0_lean_side,
                    status, decided_at, updated_at
                ) VALUES (
                    %(anchor)s, %(price_t)s, %(price_t2)s, %(bps_t2)s, %(price_t3)s, %(bps_t3)s,
                    %(tier)s, %(tier_acc)s, %(side)s, %(adx)s, %(adx_confirmed)s, %(adx_unfavourable)s, %(atr_bps)s,
                    %(stake_ok)s, %(stake_usdt)s, %(stake_pct)s, %(assumed_payout)s, %(decided_early)s, %(decision_trigger)s,
                    %(weak_edge)s, %(liq_300)s, %(liq_900)s, %(t0_lean)s,
                    'DECIDED', now(), now()
                )
                ON CONFLICT (anchor) DO UPDATE SET
                    price_t2=EXCLUDED.price_t2, bps_t2=EXCLUDED.bps_t2,
                    price_t3=EXCLUDED.price_t3, bps_t3=EXCLUDED.bps_t3,
                    tier=EXCLUDED.tier, tier_acc=EXCLUDED.tier_acc, side=EXCLUDED.side,
                    adx=EXCLUDED.adx, adx_confirmed=EXCLUDED.adx_confirmed,
                    adx_unfavourable=EXCLUDED.adx_unfavourable, atr_bps=EXCLUDED.atr_bps,
                    stake_ok=EXCLUDED.stake_ok,
                    stake_usdt=EXCLUDED.stake_usdt, stake_pct=EXCLUDED.stake_pct,
                    assumed_payout=EXCLUDED.assumed_payout, decided_early=EXCLUDED.decided_early,
                    decision_trigger=EXCLUDED.decision_trigger, weak_edge=EXCLUDED.weak_edge,
                    liq_imbalance_300s=EXCLUDED.liq_imbalance_300s,
                    liq_imbalance_900s=EXCLUDED.liq_imbalance_900s,
                    t0_lean_side=coalesce(EXCLUDED.t0_lean_side, binance_5min_windows.t0_lean_side),
                    status='DECIDED',
                    decided_at=now(), updated_at=now()
            """, {
                "anchor": w_anchor, "price_t": w.get("price_T"),
                "price_t2": w.get("price_T2"), "bps_t2": w.get("bps_T2"),
                "price_t3": w.get("price_T3"), "bps_t3": w.get("bps_T3"),
                "tier": w.get("tier"), "tier_acc": w.get("tier_acc"), "side": w.get("side"),
                "adx": w.get("adx"), "adx_confirmed": w.get("adx_confirmed"),
                "adx_unfavourable": w.get("adx_unfavourable"), "atr_bps": w.get("atr_bps"),
                "stake_ok": w.get("stake_ok"),
                "stake_usdt": w.get("stake_usdt"), "stake_pct": w.get("stake_pct"),
                "assumed_payout": w.get("assumed_payout"),
                "decided_early": w.get("decided_early", False),
                "decision_trigger": w.get("decision_trigger"),
                "weak_edge": w.get("weak_edge", False),
                "liq_300": w.get("liq_imbalance_300s"),
                "liq_900": w.get("liq_imbalance_900s"),
                "t0_lean": w.get("t0_lean_side"),
            })
        pg.commit()
    except Exception as e:
        print(f"[{datetime.now(timezone.utc).isoformat()}] window decision DB write failed "
              f"(anchor={w_anchor}): {e}", flush=True)


def persist_window_expiry(w_anchor, w):
    """Fill in the T+5 outcome fields for a row already inserted at T+3."""
    try:
        pg = _pg_conn()
        if pg is None:
            return
        correct = w.get("correct")
        correct_str = "TIE" if correct == "TIE" else ("CORRECT" if correct is True else ("WRONG" if correct is False else None))
        with pg.cursor() as cur:
            cur.execute("""
                UPDATE binance_5min_windows
                SET price_t5=%(price_t5)s, bps_t5=%(bps_t5)s, actual_dir=%(actual_dir)s,
                    correct=%(correct)s, est_return_usdt=%(est_return_usdt)s,
                    status='EXPIRED', expired_at=now(), updated_at=now()
                WHERE anchor=%(anchor)s
            """, {
                "anchor": w_anchor, "price_t5": w.get("price_T5"), "bps_t5": w.get("bps_T5"),
                "actual_dir": w.get("actual_dir"), "correct": correct_str,
                "est_return_usdt": w.get("est_return_usdt"),
            })
        pg.commit()
    except Exception as e:
        print(f"[{datetime.now(timezone.utc).isoformat()}] window expiry DB write failed "
              f"(anchor={w_anchor}): {e}", flush=True)


def ensure_late_legs_table():
    """Best-effort CREATE TABLE IF NOT EXISTS for the late-trigger legs
    (added 2026-07-03). Deliberately a separate append-only table, NOT new
    columns on binance_5min_windows — that table's anchor PK encodes a hard
    one-decision-per-window contract that get_cumulative_stats()/
    compute_retroactive_gated_stats()/the dashboard all rely on. `anchor` is
    a plain indexed column, no FK: same precedent as binance_5min_ticks
    (relates by timestamp, no constraint), and a hard FK would add write-time
    contention in the poll loop for no real integrity benefit (the parent row
    always exists by construction — a late leg can only fire after T+3
    already upserted it)."""
    try:
        pg = _pg_conn()
        if pg is None:
            return
        with pg.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS binance_5min_late_legs (
                    id                     BIGSERIAL PRIMARY KEY,
                    anchor                 TIMESTAMPTZ NOT NULL,
                    fired_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
                    seconds_before_expiry  DOUBLE PRECISION,
                    price_at_fire          DOUBLE PRECISION,
                    bps_at_fire            DOUBLE PRECISION,
                    side                   TEXT,
                    assumed_payout         DOUBLE PRECISION,
                    stake_ok               BOOLEAN,
                    stake_pct              DOUBLE PRECISION,
                    stake_usdt             DOUBLE PRECISION,
                    no_stake_reason        TEXT,
                    adx                    DOUBLE PRECISION,
                    adx_unfavourable       BOOLEAN,
                    t3_side                TEXT,
                    t3_stake_ok            BOOLEAN,
                    agrees_with_t3         BOOLEAN,
                    price_t5               DOUBLE PRECISION,
                    bps_t5                 DOUBLE PRECISION,
                    actual_dir             TEXT,
                    correct                TEXT,
                    est_return_usdt        DOUBLE PRECISION,
                    status                 TEXT NOT NULL DEFAULT 'FIRED',
                    updated_at             TIMESTAMPTZ NOT NULL DEFAULT now()
                );
                CREATE INDEX IF NOT EXISTS idx_binance_5min_late_legs_anchor
                    ON binance_5min_late_legs(anchor);
                CREATE INDEX IF NOT EXISTS idx_binance_5min_late_legs_status
                    ON binance_5min_late_legs(status);
                ALTER TABLE binance_5min_late_legs
                    ADD COLUMN IF NOT EXISTS leg_type TEXT NOT NULL DEFAULT 'LATE';
                -- SWING_FADE (2026-07-26) reuses this table as leg_type='FADE'.
                -- entry_ask is the modelled cheap-side price actually paid (a
                -- fade's payout is derived from it, not from a tier), and
                -- fill_prob is the modelled fillability claim recorded at fire
                -- time so logged manual attempts can grade it later. Both are
                -- NULL for LATE/VALUE rows.
                ALTER TABLE binance_5min_late_legs
                    ADD COLUMN IF NOT EXISTS entry_ask DOUBLE PRECISION;
                ALTER TABLE binance_5min_late_legs
                    ADD COLUMN IF NOT EXISTS fill_prob DOUBLE PRECISION;
            """)
        pg.commit()
    except Exception as e:
        print(f"[{datetime.now(timezone.utc).isoformat()}] late-legs table ensure failed: {e}", flush=True)


def persist_extra_leg_fire(w_anchor, w, leg_type="LATE", prefix="late"):
    """Append-only insert at extra-leg (late/value) fire time. Best-effort,
    never raises. At most one FIRED row per (anchor, leg_type) is guaranteed
    by the in-memory {prefix}_leg_fired one-shot guard plus
    recover_stuck_extra_legs() on restart."""
    try:
        pg = _pg_conn()
        if pg is None:
            return
        with pg.cursor() as cur:
            cur.execute("""
                INSERT INTO binance_5min_late_legs (
                    anchor, seconds_before_expiry, price_at_fire, bps_at_fire, side,
                    assumed_payout, stake_ok, stake_pct, stake_usdt, no_stake_reason,
                    adx, adx_unfavourable, t3_side, t3_stake_ok, agrees_with_t3, status, leg_type,
                    entry_ask, fill_prob
                ) VALUES (
                    %(anchor)s, %(secs)s, %(price)s, %(bps)s, %(side)s,
                    %(payout)s, %(stake_ok)s, %(stake_pct)s, %(stake_usdt)s, %(reason)s,
                    %(adx)s, %(adx_unfav)s, %(t3_side)s, %(t3_stake_ok)s, %(agrees)s, 'FIRED', %(leg_type)s,
                    %(entry_ask)s, %(fill_prob)s
                )
            """, {
                "anchor": w_anchor, "secs": w.get(f"{prefix}_seconds_before_expiry"),
                "price": w.get(f"{prefix}_price_at_fire"), "bps": w.get(f"{prefix}_bps_at_fire"),
                "side": w.get(f"{prefix}_side"), "payout": w.get(f"{prefix}_assumed_payout"),
                "stake_ok": w.get(f"{prefix}_stake_ok"), "stake_pct": w.get(f"{prefix}_stake_pct"),
                "stake_usdt": w.get(f"{prefix}_stake_usdt"), "reason": w.get(f"{prefix}_no_stake_reason"),
                "adx": w.get(f"{prefix}_adx"), "adx_unfav": w.get(f"{prefix}_adx_unfavourable"),
                "t3_side": w.get("side"), "t3_stake_ok": w.get("stake_ok"),
                "agrees": (w.get(f"{prefix}_side") == w.get("side")) if w.get("side") else None,
                "leg_type": leg_type,
                "entry_ask": w.get(f"{prefix}_entry_ask"),
                "fill_prob": w.get(f"{prefix}_fill_prob"),
            })
        pg.commit()
    except Exception as e:
        print(f"[{datetime.now(timezone.utc).isoformat()}] {leg_type} leg fire DB write failed "
              f"(anchor={w_anchor}): {e}", flush=True)


# Backwards-compatible alias (late leg was the first extra leg)
def persist_late_leg_fire(w_anchor, w):
    persist_extra_leg_fire(w_anchor, w, leg_type="LATE", prefix="late")


def persist_extra_leg_expiry(w_anchor, w, leg_type="LATE", prefix="late"):
    """Fill in the T+5 outcome for a FIRED extra leg. anchor isn't this
    table's PK, but the one-FIRED-row-per-(anchor,leg_type) guarantee makes
    the WHERE unambiguous."""
    try:
        pg = _pg_conn()
        if pg is None:
            return
        correct = w.get(f"{prefix}_correct")
        correct_str = "TIE" if correct == "TIE" else ("CORRECT" if correct is True else ("WRONG" if correct is False else None))
        with pg.cursor() as cur:
            cur.execute("""
                UPDATE binance_5min_late_legs
                SET price_t5=%(price_t5)s, bps_t5=%(bps_t5)s, actual_dir=%(actual_dir)s,
                    correct=%(correct)s, est_return_usdt=%(est)s,
                    status='EXPIRED', updated_at=now()
                WHERE anchor=%(anchor)s AND status='FIRED' AND leg_type=%(leg_type)s
            """, {
                "anchor": w_anchor, "price_t5": w.get("price_T5"), "bps_t5": w.get("bps_T5"),
                "actual_dir": w.get("actual_dir"), "correct": correct_str,
                "est": w.get(f"{prefix}_est_return_usdt"), "leg_type": leg_type,
            })
        pg.commit()
    except Exception as e:
        print(f"[{datetime.now(timezone.utc).isoformat()}] {leg_type} leg expiry DB write failed "
              f"(anchor={w_anchor}): {e}", flush=True)


def persist_late_leg_expiry(w_anchor, w):
    persist_extra_leg_expiry(w_anchor, w, leg_type="LATE", prefix="late")


def recover_stuck_late_legs():
    """On startup, resolve or reload extra legs (LATE and VALUE) left in
    status='FIRED' by a restart landing between fire time and T+5 — mirrors
    recover_stuck_decided_windows() (the primary leg hit exactly this bug
    class once already; see its docstring). Returns anchor -> state dict
    (prefixed late_*/value_* keys by leg_type) to merge into the in-memory
    windows dict for legs still pending; anything past T+5 is resolved here
    from binance_5min_ticks."""
    reloaded = {}
    try:
        pg = _pg_conn()
        if pg is None:
            return reloaded
        with pg.cursor() as cur:
            cur.execute("""
                SELECT anchor, seconds_before_expiry, price_at_fire, bps_at_fire, side,
                       assumed_payout, stake_ok, stake_pct, stake_usdt, adx, adx_unfavourable,
                       leg_type, entry_ask, fill_prob
                FROM binance_5min_late_legs
                WHERE status = 'FIRED'
            """)
            rows = cur.fetchall()
    except Exception as e:
        print(f"[{datetime.now(timezone.utc).isoformat()}] stuck extra-leg recovery query failed: {e}", flush=True)
        return reloaded

    now = datetime.now(timezone.utc)
    # leg_type -> in-memory key prefix. FADE added 2026-07-26; anything
    # unrecognised falls back to "late" (the original, and the DEFAULT of the
    # leg_type column, so pre-column rows land there correctly).
    _PREFIX_BY_LEG = {"VALUE": "value", "FADE": "fade", "LATE": "late"}
    for (anchor, secs, price_at_fire, bps_at_fire, side, payout, stake_ok,
         stake_pct, stake_usdt, adx_val, adx_unfav, leg_type,
         entry_ask, fill_prob) in rows:
        prefix = _PREFIX_BY_LEG.get(leg_type, "late")
        expiry_time = anchor + timedelta(minutes=EXPIRY_OFFSET_MIN)
        late_state = {
            f"{prefix}_leg_fired": True, f"{prefix}_side": side,
            f"{prefix}_seconds_before_expiry": secs, f"{prefix}_price_at_fire": price_at_fire,
            f"{prefix}_bps_at_fire": bps_at_fire, f"{prefix}_assumed_payout": payout,
            f"{prefix}_stake_ok": stake_ok, f"{prefix}_stake_pct": stake_pct,
            f"{prefix}_stake_usdt": stake_usdt, f"{prefix}_adx": adx_val,
            f"{prefix}_adx_unfavourable": adx_unfav, f"{prefix}_no_stake_reason": None,
            f"{prefix}_correct": None, f"{prefix}_est_return_usdt": None,
            f"{prefix}_entry_ask": entry_ask, f"{prefix}_fill_prob": fill_prob,
        }
        if now >= expiry_time:
            try:
                pg = _pg_conn()
                with pg.cursor() as cur:
                    cur.execute("""
                        SELECT ts, price FROM binance_5min_ticks
                        WHERE ts < %(expiry)s ORDER BY ts DESC LIMIT 1
                    """, {"expiry": expiry_time})
                    tick_row = cur.fetchone()
                if tick_row is None:
                    print(f"[{now.isoformat()}] stuck late leg anchor={anchor} has no tick data "
                          f"before expiry — leaving as FIRED for manual review", flush=True)
                    continue
                # Last tick BEFORE expiry, not first at/after — see
                # get_settlement_price's docstring for why that matters.
                price_t5 = tick_row[1]
                # bps from window open isn't needed to settle a late leg —
                # only the side vs final direction relative to the strike
                # (price_T). Fetch price_T from the windows table.
                with pg.cursor() as cur:
                    cur.execute("SELECT price_t FROM binance_5min_windows WHERE anchor=%s", (anchor,))
                    pt_row = cur.fetchone()
                if pt_row is None or pt_row[0] is None:
                    print(f"[{now.isoformat()}] stuck late leg anchor={anchor} has no parent "
                          f"window price_t — leaving as FIRED for manual review", flush=True)
                    continue
                price_t = pt_row[0]
                actual_bps = (price_t5 - price_t) / price_t * 10000.0
                outcome = "HIGHER" if actual_bps > 0 else ("LOWER" if actual_bps < 0 else "FLAT")
                if outcome == "FLAT":
                    correct = "TIE"
                else:
                    correct = (side == outcome)
                w = dict(late_state)
                w.update({"price_T5": price_t5, "bps_T5": actual_bps, "actual_dir": outcome,
                          f"{prefix}_correct": correct})
                if stake_ok and payout is not None and stake_usdt:
                    if correct is True:
                        w[f"{prefix}_est_return_usdt"] = round(stake_usdt * payout, 2)
                    elif correct is False:
                        w[f"{prefix}_est_return_usdt"] = round(-stake_usdt, 2)
                    else:
                        w[f"{prefix}_est_return_usdt"] = 0.0
                persist_extra_leg_expiry(anchor, w, leg_type=leg_type or "LATE", prefix=prefix)
                print(f"[{now.isoformat()}] recovered stuck {leg_type or 'LATE'} leg "
                      f"anchor={anchor.strftime('%Y-%m-%d %H:%M:%S')} "
                      f"-> actual={outcome} correct={correct}", flush=True)
            except Exception as e:
                print(f"[{now.isoformat()}] stuck late-leg recovery failed for anchor={anchor}: {e}", flush=True)
        else:
            reloaded[anchor] = late_state
            print(f"[{now.isoformat()}] reloaded in-flight late leg anchor={anchor.strftime('%Y-%m-%d %H:%M:%S')} "
                  f"(T+5 still pending)", flush=True)
    return reloaded


def get_late_leg_cumulative_stats(leg_type="LATE"):
    """All-time extra-leg stats (LATE or VALUE) from binance_5min_late_legs
    (survives restarts), parallel to get_cumulative_stats() for the primary.
    Includes est_net_return_frac/total_staked_frac (2026-07-04) — same fix
    as the primary leg's bankroll-independence bug: est_net_pnl_usdt/
    total_staked_usdt are tied to whatever BANKROLL_USDT the monitor process
    uses (the owner's), so a multi-user dashboard must scale off the
    capital-independent fraction (stake_pct-weighted) instead, exactly like
    get_cumulative_stats() already does — see project memory's "per-user $
    bug fix" note for what happens if this is skipped."""
    try:
        pg = _pg_conn()
        if pg is None:
            return None
        with pg.cursor() as cur:
            cur.execute("""
                SELECT count(*) FILTER (WHERE stake_ok),
                       count(*) FILTER (WHERE stake_ok AND correct = 'CORRECT'),
                       count(*) FILTER (WHERE stake_ok AND correct = 'WRONG'),
                       count(*) FILTER (WHERE NOT stake_ok),
                       coalesce(sum(est_return_usdt) FILTER (WHERE stake_ok), 0),
                       coalesce(sum(stake_usdt) FILTER (WHERE stake_ok), 0),
                       min(fired_at)
                FROM binance_5min_late_legs
                WHERE status = 'EXPIRED' AND leg_type = %s
            """, (leg_type,))
            (staked, correct, wrong, skipped, est_pnl, total_staked, since) = cur.fetchone()
            cur.execute("""
                SELECT stake_pct, assumed_payout, correct
                FROM binance_5min_late_legs
                WHERE status = 'EXPIRED' AND leg_type = %s AND stake_ok
            """, (leg_type,))
            frac_rows = cur.fetchall()
        est_net_return_frac = 0.0
        total_staked_frac = 0.0
        for stake_pct, payout, corr in frac_rows:
            if stake_pct is None:
                continue
            total_staked_frac += stake_pct
            if payout is not None:
                if corr == "CORRECT":
                    est_net_return_frac += stake_pct * payout
                elif corr == "WRONG":
                    est_net_return_frac -= stake_pct
        decided = (correct or 0) + (wrong or 0)
        return {
            "staked": staked or 0,
            "correct": correct or 0,
            "accuracy_pct": round(100.0 * correct / decided, 1) if decided else None,
            "skipped": skipped or 0,
            "est_net_pnl_usdt": round(float(est_pnl), 2),
            "total_staked_usdt": round(float(total_staked), 2),
            "est_net_return_frac": est_net_return_frac,
            "total_staked_frac": total_staked_frac,
            "since": since.isoformat() if since else None,
        }
    except Exception as e:
        print(f"[{datetime.now(timezone.utc).isoformat()}] late-leg cumulative stats query failed: {e}", flush=True)
        return None


# In-process kill-bar state (see LATE_KILL_* constants). DB-backed via the
# query below so it survives restarts — a restart re-derives "killed" from
# the same rows and latches again (the Telegram alert re-fires once per
# process in that case, which is acceptable: a tripped tripwire SHOULD nag).
_late_kill_state = {"killed": False, "checked_at": None, "n": 0, "acc": None}


def late_trigger_killed(now):
    """True once the redesigned late leg's post-LATE_REDESIGN_TS live record
    trips the kill bar (>= LATE_KILL_MIN_FIRES decided stakes at accuracy
    below LATE_KILL_ACC). Cached 60s so the per-tick fire gate doesn't
    hammer Postgres; latches on permanently for the process once tripped."""
    st = _late_kill_state
    if st["killed"]:
        return True
    if st["checked_at"] is not None and (now - st["checked_at"]).total_seconds() < 60:
        return False
    st["checked_at"] = now
    pg = None
    try:
        pg = _pg_conn()
        if pg is None:
            return False
        with pg.cursor() as cur:
            cur.execute("""
                SELECT count(*) FILTER (WHERE correct = 'CORRECT'), count(*)
                FROM binance_5min_late_legs
                WHERE leg_type = 'LATE' AND stake_ok AND status = 'EXPIRED'
                  AND correct IN ('CORRECT', 'WRONG')
                  AND fired_at >= %s
            """, (LATE_REDESIGN_TS,))
            wins, decided = cur.fetchone()
    except Exception as e:
        try:
            if pg is not None:
                pg.rollback()
        except Exception:
            pass
        print(f"[{datetime.now(timezone.utc).isoformat()}] late kill-bar query failed: {e}", flush=True)
        return False
    st["n"] = decided or 0
    st["acc"] = (wins / decided) if decided else None
    if decided and decided >= LATE_KILL_MIN_FIRES and (wins / decided) < LATE_KILL_ACC:
        st["killed"] = True
        msg = (f"🔴 <b>LATE trigger disabled by its live kill bar</b> — "
               f"{wins}/{decided} correct ({100.0 * wins / decided:.1f}%) since the "
               f"2026-07-04 17:36 redesign, below the {LATE_KILL_ACC * 100:.0f}% bar "
               f"at n>={LATE_KILL_MIN_FIRES}. No further LATE fires this process; "
               f"re-enabling is a manual call (BINANCE_LATE_KILL_* env + restart).")
        print(msg, flush=True)
        tg(msg)
        return True
    return False


# In-process kill-bar state for WEAK-EDGE (see WEAK_EDGE_KILL_* constants).
# Same DB-backed-so-it-survives-restarts, latches-permanently-once-tripped
# pattern as _late_kill_state above.
_weak_edge_kill_state = {"killed": False, "checked_at": None, "n": 0, "acc": None}


def weak_edge_killed(now):
    """True once WEAK-EDGE's live record trips the kill bar (>=
    WEAK_EDGE_KILL_MIN_FIRES decided stakes at accuracy below
    WEAK_EDGE_KILL_ACC). Cached 60s so the per-tick gate doesn't hammer
    Postgres; latches on permanently for the process once tripped."""
    st = _weak_edge_kill_state
    if st["killed"]:
        return True
    if st["checked_at"] is not None and (now - st["checked_at"]).total_seconds() < 60:
        return False
    st["checked_at"] = now
    pg = None
    try:
        pg = _pg_conn()
        if pg is None:
            return False
        with pg.cursor() as cur:
            cur.execute("""
                SELECT count(*) FILTER (WHERE correct = 'CORRECT'), count(*)
                FROM binance_5min_windows
                WHERE tier = 'WEAK-EDGE' AND stake_ok AND status = 'EXPIRED'
                  AND correct IN ('CORRECT', 'WRONG')
            """)
            wins, decided = cur.fetchone()
    except Exception as e:
        try:
            if pg is not None:
                pg.rollback()
        except Exception:
            pass
        print(f"[{datetime.now(timezone.utc).isoformat()}] weak-edge kill-bar query failed: {e}", flush=True)
        return False
    st["n"] = decided or 0
    st["acc"] = (wins / decided) if decided else None
    if decided and decided >= WEAK_EDGE_KILL_MIN_FIRES and (wins / decided) < WEAK_EDGE_KILL_ACC:
        st["killed"] = True
        msg = (f"🔴 <b>WEAK-EDGE disabled by its live kill bar</b> — "
               f"{wins}/{decided} correct ({100.0 * wins / decided:.1f}%), below the "
               f"{WEAK_EDGE_KILL_ACC * 100:.0f}% bar at n>={WEAK_EDGE_KILL_MIN_FIRES}. "
               f"No further WEAK-EDGE fires this process; re-enabling is a manual "
               f"call (BINANCE_WEAK_EDGE_KILL_* env + restart).")
        print(msg, flush=True)
        tg(msg)
        return True
    return False


def reconstruct_boundary_price(anchor):
    """True window-open price reconstructed from persisted DB ticks around the
    :00 boundary (added 2026-07-03): last tick at/before the anchor and first
    tick after it, linearly interpolated at the boundary. Returns None unless
    BOTH surrounding ticks exist within 6s of the anchor — a wider gap means
    the monitor was actually down over the boundary and any estimate would be
    a guess (the exact fabrication this function exists to avoid)."""
    try:
        pg = _pg_conn()
        if pg is None:
            return None
        with pg.cursor() as cur:
            cur.execute("""
                (SELECT ts, price FROM binance_5min_ticks WHERE ts <= %(a)s ORDER BY ts DESC LIMIT 1)
                UNION ALL
                (SELECT ts, price FROM binance_5min_ticks WHERE ts > %(a)s ORDER BY ts ASC LIMIT 1)
                ORDER BY ts
            """, {"a": anchor})
            rows = cur.fetchall()
        if len(rows) != 2:
            return None
        (t0, p0), (t1, p1) = rows
        if t0 > anchor or t1 <= anchor:
            return None
        if (anchor - t0).total_seconds() > 6 or (t1 - anchor).total_seconds() > 6:
            return None
        gap = (t1 - t0).total_seconds()
        if gap <= 0:
            return None
        frac = (anchor - t0).total_seconds() / gap
        return p0 + (p1 - p0) * frac
    except Exception as e:
        print(f"[{datetime.now(timezone.utc).isoformat()}] boundary price reconstruction failed "
              f"(anchor={anchor}): {e}", flush=True)
        return None


def preload_recent_ticks():
    """Refill the in-memory tick buffer from binance_5min_ticks on startup
    (added 2026-07-03, the owner noticed the live chart collapsed to ~45s of data
    after a restart — the deque is in-memory and was starting empty even
    though every tick is already persisted). Returns a list of
    {"t": iso, "p": price} dicts, oldest first, capped at TICK_BUFFER_LEN."""
    out = []
    try:
        pg = _pg_conn()
        if pg is None:
            return out
        with pg.cursor() as cur:
            cur.execute("""
                SELECT ts, price FROM binance_5min_ticks
                WHERE ts >= now() - (%s || ' minutes')::interval
                ORDER BY ts ASC
            """, (TICK_BUFFER_MINUTES,))
            rows = cur.fetchall()
        out = [{"t": ts.isoformat(), "p": p} for ts, p in rows[-TICK_BUFFER_LEN:]]
        if out:
            print(f"[{datetime.now(timezone.utc).isoformat()}] preloaded {len(out)} ticks "
                  f"({out[0]['t']} .. {out[-1]['t']}) into the chart buffer", flush=True)
    except Exception as e:
        print(f"[{datetime.now(timezone.utc).isoformat()}] tick preload failed: {e}", flush=True)
    return out


def recover_stuck_decided_windows():
    """On startup, resolve or reload any windows left in status='DECIDED' by
    a previous restart landing between T+3 and T+5 (added 2026-07-03 —
    found 9 such windows stuck forever this way, including 3 real stakes,
    one nearly a full day old: the in-memory `windows` dict previously
    always started as `{}` on restart, and only the CURRENT 5-min anchor
    ever gets (re-)added to it going forward, so anything mid-flight at
    restart time was silently dropped and never reached EXPIRED in the DB).
    Returns a dict of anchor -> window-state to merge into the in-memory
    `windows` dict for anything that hasn't reached T+5 yet; anything
    already past T+5 is resolved immediately here using the closest
    `binance_5min_ticks` price at/after the expiry timestamp."""
    reloaded = {}
    try:
        pg = _pg_conn()
        if pg is None:
            return reloaded
        with pg.cursor() as cur:
            cur.execute("""
                SELECT anchor, price_t, price_t2, bps_t2, price_t3, bps_t3,
                       tier, tier_acc, side, adx, adx_confirmed, adx_unfavourable,
                       stake_ok, stake_usdt, stake_pct, assumed_payout, decided_early,
                       decision_trigger, liq_imbalance_300s, liq_imbalance_900s
                FROM binance_5min_windows
                WHERE status = 'DECIDED'
            """)
            rows = cur.fetchall()
    except Exception as e:
        print(f"[{datetime.now(timezone.utc).isoformat()}] stuck-window recovery query failed: {e}", flush=True)
        return reloaded

    now = datetime.now(timezone.utc)
    for (anchor, price_t, price_t2, bps_t2, price_t3, bps_t3, tier, tier_acc, side,
         adx, adx_confirmed_val, adx_unfav, stake_ok, stake_usdt, stake_pct,
         assumed_payout, decided_early, decision_trigger, liq_300, liq_900) in rows:
        expiry_time = anchor + timedelta(minutes=EXPIRY_OFFSET_MIN)
        if now >= expiry_time:
            try:
                pg = _pg_conn()
                with pg.cursor() as cur:
                    cur.execute("""
                        SELECT price FROM binance_5min_ticks
                        WHERE ts < %(expiry)s ORDER BY ts DESC LIMIT 1
                    """, {"expiry": expiry_time})
                    tick_row = cur.fetchone()
                if tick_row is None:
                    print(f"[{now.isoformat()}] stuck window anchor={anchor} has no tick data "
                          f"before expiry — leaving as DECIDED for manual review", flush=True)
                    continue
                # Last tick BEFORE expiry, not first at/after — see
                # get_settlement_price's docstring for why that matters.
                price_t5 = tick_row[0]
                actual_bps = (price_t5 - price_t) / price_t * 10000.0
                outcome = "HIGHER" if actual_bps > 0 else ("LOWER" if actual_bps < 0 else "FLAT")
                if outcome == "FLAT":
                    correct = "TIE"
                elif side:
                    correct = (side == outcome)
                else:
                    correct = "TIE"
                w = {"price_T5": price_t5, "bps_T5": actual_bps, "actual_dir": outcome, "correct": correct}
                if stake_ok and side and assumed_payout is not None and stake_usdt:
                    if correct is True:
                        w["est_return_usdt"] = round(stake_usdt * assumed_payout, 2)
                    elif correct is False:
                        w["est_return_usdt"] = round(-stake_usdt, 2)
                    else:
                        w["est_return_usdt"] = 0.0
                else:
                    w["est_return_usdt"] = None
                persist_window_expiry(anchor, w)
                print(f"[{now.isoformat()}] recovered stuck window anchor={anchor.strftime('%Y-%m-%d %H:%M:%S')} "
                      f"-> actual={outcome} correct={correct} (resolved from a past restart gap)", flush=True)
            except Exception as e:
                print(f"[{now.isoformat()}] stuck window recovery failed for anchor={anchor}: {e}", flush=True)
        else:
            reloaded[anchor] = {
                "price_T": price_t, "status": "DECIDED",
                "price_T2": price_t2, "bps_T2": bps_t2,
                "price_T3": price_t3, "bps_T3": bps_t3,
                "tier": tier, "tier_acc": tier_acc, "side": side,
                "price_T5": None, "bps_T5": None, "actual_dir": None, "correct": None,
                "adx": adx, "adx_confirmed": adx_confirmed_val, "adx_unfavourable": adx_unfav,
                "stake_ok": stake_ok, "stake_usdt": stake_usdt, "stake_pct": stake_pct,
                "assumed_payout": assumed_payout, "est_return_usdt": None,
                "decided_early": decided_early, "decision_trigger": decision_trigger,
                "ny_open_tightened": None, "no_stake_reason": None,
                "liq_imbalance_300s": liq_300, "liq_imbalance_900s": liq_900,
                "dwell_cross_ts": None, "dwell_cross_sign": None,
            }
            print(f"[{now.isoformat()}] reloaded in-flight window anchor={anchor.strftime('%Y-%m-%d %H:%M:%S')} "
                  f"(T+5 still pending, {(expiry_time - now).total_seconds():.0f}s left)", flush=True)
    return reloaded


def get_cumulative_stats():
    """All-time stats across every restart, sourced from binance_5min_windows
    (not the in-memory windows dict, which resets on every process restart).
    Staked/correct/accuracy/no-stake counts come from
    compute_retroactive_gated_stats() (added 2026-07-03) — i.e. recomputed as
    if today's per-tier auto-gate had been running the whole time, not the
    raw historical mix.

    Est. net P&L was originally scoped to `binance_5min_pnl_reset` instead
    (a manual cutoff to hide pre-fix history dragging the number negative).
    Dropped that same day once it became clear the reset created its own,
    worse confusion: "96 correct" next to a $ figure that silently only
    covered ~8 trades since the reset point. The retroactive gate already
    solves the original problem the reset was for (it excludes trades that
    wouldn't be staked under current rules, wherever they fall in history),
    so P&L now uses the exact same `retro_stake_map` as the count fields —
    same population, no separate cutoff. `binance_5min_pnl_reset` is left
    in place unused rather than dropped, in case a future logic change wants
    the same "start counting from here" mechanism again.

    Returns (stats_dict_or_None, stake_map, pct_map) — added 2026-07-06 so the
    dashboard's "All" range can give the per-row table the same all-time
    retro map the headline numbers already use, instead of falling back to
    each row's own frozen historical stake_ok for anything older than the
    12h/24h maps (that fallback was showing old, now-excluded losers — e.g.
    a T+2-early STRONG loss from days ago — as "staked" in the table while
    the corrected headline no longer counted them, a real inconsistency
    the owner caught by comparing the two)."""
    try:
        pg = _pg_conn()
        if pg is None:
            return None, {}, {}
        retro, stake_map, pct_map = compute_retroactive_gated_stats(hours=None, return_map=True)
        retro = retro or {}
        with pg.cursor() as cur:
            cur.execute("""
                SELECT anchor, stake_pct, assumed_payout, correct, stake_usdt, est_return_usdt,
                       capped_stake_pct, recomputed_real_payout, capped_stake_usdt, capped_est_return_usdt
                FROM binance_5min_windows
                WHERE status = 'EXPIRED'
            """)
            rows = cur.fetchall()
            cur.execute("SELECT min(anchor) FROM binance_5min_windows WHERE status = 'EXPIRED'")
            (since,) = cur.fetchone()

        est_net_return_frac = 0.0
        total_staked_frac = 0.0
        est_net_pnl_usdt = 0.0
        total_staked_usdt = 0.0
        LEGACY_BANKROLL_USDT = 1000.0  # see comment below
        for (anchor, stake_pct, assumed_payout, correct, stake_usdt, est_return_usdt,
             capped_stake_pct, recomputed_real_payout, capped_stake_usdt, capped_est_return_usdt) in rows:
            if not stake_map.get(anchor.isoformat()):
                continue
            if stake_pct is None and stake_usdt is not None:
                # Rows from before per-user stake_pct tracking existed
                # (added 2026-07-02) only ever recorded the raw $ amount.
                # BANKROLL_USDT was a flat $1000 default for all of them
                # (confirmed: stake_usdt/1000 exactly reproduces the old
                # static ladder's 1.5/2.5/3.5/5% values with no remainder)
                # — derive the equivalent fraction so these ~37 real
                # trades (all from 2026-07-01) aren't silently dropped from
                # the fraction-based sum the dashboard actually displays.
                stake_pct = stake_usdt / LEGACY_BANKROLL_USDT
            # Payout-cap backfill (2026-07-05, see project memory 'payout cap
            # replay'): rows staked before the cap shipped have capped_*/
            # recomputed_* columns backfilled to what they would have been
            # under the current (capped) sizing — prefer those so All-time
            # reflects the fix across its whole history, not just trades
            # staked after the deploy. Rows without a backfill (LATE/VALUE
            # legs, or anything staked after the cap shipped, since live
            # stake_pct/stake_usdt are already correctly capped going
            # forward) fall back to the raw columns unchanged.
            eff_stake_pct = capped_stake_pct if capped_stake_pct is not None else stake_pct
            eff_payout = recomputed_real_payout if recomputed_real_payout is not None else assumed_payout
            eff_stake_usdt = capped_stake_usdt if capped_stake_usdt is not None else stake_usdt
            eff_return_usdt = capped_est_return_usdt if capped_est_return_usdt is not None else est_return_usdt
            if eff_stake_pct is not None:
                total_staked_frac += eff_stake_pct
                if eff_payout is not None:
                    if correct == "CORRECT":
                        est_net_return_frac += eff_stake_pct * eff_payout
                    elif correct == "WRONG":
                        est_net_return_frac -= eff_stake_pct
            if eff_stake_usdt is not None:
                total_staked_usdt += eff_stake_usdt
            if eff_return_usdt is not None:
                est_net_pnl_usdt += eff_return_usdt

        return {
            "staked": retro.get("staked", 0),
            "correct": retro.get("correct", 0),
            "wrong": retro.get("wrong", 0),
            "ties": retro.get("ties", 0),
            "accuracy_pct": retro.get("accuracy_pct"),
            "skipped": retro.get("skipped", 0),
            "no_stake_correct": retro.get("no_stake_correct", 0),
            "no_stake_accuracy_pct": retro.get("no_stake_accuracy_pct"),
            "est_net_pnl_usdt": round(est_net_pnl_usdt, 2),
            "total_staked_usdt": round(total_staked_usdt, 2),
            "est_net_return_frac": est_net_return_frac,
            "total_staked_frac": total_staked_frac,
            "pnl_reset_at": None,
            "since": since.isoformat() if since else None,
        }, stake_map, pct_map
    except Exception as e:
        print(f"[{datetime.now(timezone.utc).isoformat()}] cumulative stats query failed: {e}", flush=True)
        return None, {}, {}


def get_user_capital(username):
    """Current compounding capital for `username` from binance_5min_user_capital
    (same table the dashboard's manual-update form writes to). None if unset —
    caller should fall back to a hardcoded default rather than stake $0."""
    try:
        pg = _pg_conn()
        if pg is None:
            return None
        with pg.cursor() as cur:
            cur.execute(
                "SELECT capital_usdt FROM binance_5min_user_capital WHERE username = %s",
                (username,),
            )
            row = cur.fetchone()
        return float(row[0]) if row else None
    except Exception as e:
        print(f"[{datetime.now(timezone.utc).isoformat()}] user capital query failed "
              f"(username={username}): {e}", flush=True)
        return None


TIERS = [
    (30.0, "EXTREME", 0.968),
    (20.0, "VERY STRONG", 0.952),
    (10.0, "STRONG", 0.896),
    (8.0, "STRONG", 0.883),
    (6.0, "MODERATE-STRONG", 0.877),
    (4.0, "MODERATE", 0.820),
    (2.0, "WEAK", 0.691),
]

# Suggested stake sizing (added 2026-07-01, the owner provisioned 1000 USDT).
# Deliberately far below full Kelly: at these win rates a full/half-Kelly
# calc would suggest 60-90%+ of bankroll per window, which is not sane given
# (a) Binance's actual Event Contract payout ratio has never been confirmed
# (assumed conservative ~0.80, not even-money), (b) top tiers rest on thin
# backtest samples (n=95-189), (c) fee/slippage drag on the edge is unchecked,
# (d) capital preservation outranks max EV per the standing "do not lose"
# directive. Ladder is a fixed pct-of-bankroll per tier, one rung higher if
# ADX-confirms (see feedback_minimize_losses). WEAK / unfavourable-ADX stay
# $0 via should_stake() regardless of everything below.
#
# Reweighted 2026-07-03 ("profit ramp" rework) by REAL historical $ edge per
# tier (all-time, backfilled from binance_5min_windows), not equal
# increments down the tier list as before: STRONG +7.4% (n=71, the largest,
# most statistically solid sample), VERY STRONG +5.0% (n=10), EXTREME +3.3%
# (n=5) all showed genuine positive real-money return. MODERATE-STRONG
# -17.0% (n=29) and MODERATE -26.8% (n=41) showed genuine, real historical
# losses — and by construction their assumed payout sits at only their own
# breakeven point (see MIN_EDGE_MARGIN above), so they carry the thinnest
# possible cushion even in the best case. Bumped STRONG's two sub-bands up,
# cut MODERATE/MODERATE-STRONG's baseline sharply (not to zero — the dynamic
# gate is still the mechanism that fully pauses them, per the owner's call to
# keep it dynamic rather than hard-exclude; this is their baseline when the
# gate says they're eligible, not an additional gate).
# DEFAULT LOWERED 1000 -> 200 on 2026-07-26. This is the reference capital the
# $ figures and the MIN_EXPECTED_RETURN_USDT floor are computed against, and it
# was five times larger than any capital that actually exists: every row in
# binance_5min_user_capital is capped at $200 (project_binance_capital_cap_200),
# so the gate was being evaluated on a bankroll nobody holds. Aligning it makes
# the displayed stake/return honest and makes the floor above mean what it says.
# NOTE this changes the BASIS of new $ figures only — historical rows keep the
# stake_usdt/est_return_usdt they were written with, so cumulative P&L spanning
# 2026-07-26 mixes a $1000-basis past with a $200-basis present.
#
# Recorded here because it is the reason sizing is no longer worth tuning: on
# post-refit data (>= 2026-07-05 12:00, n=3,045 windows / 21 days) the T+3
# continuation leg has no edge left to size. EV by payout floor is NEGATIVE and
# gets worse as the floor rises (no floor -1.13%, b>=0.10 -1.75%, b>=0.15
# -3.84%), and per tier: EXTREME +6.6% (n=40), VERY STRONG +1.8% (n=80),
# STRONG +0.19% (n=1,199 — flat on a solid sample), MODERATE-STRONG -0.70%,
# MODERATE -3.71%, WEAK -4.29%. An earlier monotone "payout floor fixes it"
# result was an artifact of pre-2026-07-04 rows written under the old,
# over-generous payout model — do not re-derive it without the date filter.
BANKROLL_USDT = float(os.getenv("BINANCE_STAKE_BANKROLL_USDT", "200"))
TELEGRAM_BANKROLL_USERNAME = os.getenv("BINANCE_TELEGRAM_BANKROLL_USER", "admin")
STAKE_PCT_LADDER = [0.005, 0.01, 0.03, 0.04, 0.05]
STAKE_PCT_TIERS = [
    (30.0, 0.05),    # EXTREME -- real +3.3%
    (20.0, 0.05),    # VERY STRONG -- real +5.0%
    (10.0, 0.04),    # STRONG (10-20bps) -- real +7.4% combined, bumped from 0.035
    (8.0, 0.03),     # STRONG (8-10bps) -- bumped from 0.025
    (6.0, 0.01),     # MODERATE-STRONG (6-8bps) -- real -17.0%, cut from 0.025
    (4.0, 0.0),      # MODERATE (4-6bps) -- alert-only since 2026-07-05 (see NO_STAKE_TIERS)
    (2.0, 0.0),      # WEAK -> no stake
]

# Validated "ADX-confirmed" bands (>=4bps base) — 20-25 intentionally excluded.
ADX_CONFIRMED_BANDS = [(15.0, 20.0), (25.0, 30.0), (30.0, 35.0)]
# ADX 20-25 tested WORSE than the unfiltered baseline (85.8% vs 87.9%, n=458) —
# explicitly unfavourable, not just "unconfirmed". ADX>=35 also tested worse
# (overextended trend, high reversal risk). Combined with WEAK tier
# (2-4bps, 69.1% backtest accuracy), all are now hard "no stake" calls.
ADX_UNFAVOURABLE_BANDS = [(20.0, 25.0), (35.0, float("inf"))]
# Scoped 2026-07-05 ("isolate the winners" in the no-stake bucket) —
# the bands above were validated on the whole >=4bps population pooled
# across tiers (n=458/2272), never re-checked per-tier. Reconstructed the
# full 18-month `candles` history split by tier: STRONG (n=38,153),
# VERY STRONG (n=6,563) and EXTREME (n=4,053) show NO gap in these bands
# (within ~1-2pp of every other ADX band, holding across all 4 quarters of
# the 18 months) — the pooled finding was being driven by MODERATE-STRONG,
# which DOES show a real, consistent gap (84.5% vs 87.0% confirmed, n=17,071)
# and stays gated. Confirmed live: 101 STRONG + 11 VERY STRONG + 1 EXTREME
# window were blocked by this rule in just the first 5 days of live data,
# at 92.1%/90.9%/100% accuracy — comparable to or better than each tier's
# own currently-staked population.
ADX_UNFAVOURABLE_EXEMPT_TIERS = {"STRONG", "VERY STRONG", "EXTREME"}
# MODERATE added 2026-07-05 (the owner's call — supersedes the 07-03 "keep it
# dynamic, not hard-exclude" note above): full Jul 1-5 audit with ALL current
# gates retro-applied (backward filter, NY floor, LOW_ADX_ONLY_TIERS) still
# left MODERATE contributing 3 of the 6 residual losers for 1.3% of profit
# (+$74 of $5,647; live 80% vs its 85% auto-gate bar). Alert-only now:
# windows still tier, alert and accumulate history (clean data for a future
# re-enable) — they just never stake. Applies retroactively to displayed
# history/P&L via compute_retroactive_gated_stats(), which reads this set.
# VERY STRONG + MODERATE-STRONG added 2026-07-05 (the owner's call, pending more
# data) — after enabling the T3-backward + ATR-ratio filters (backfilled,
# not yet live), their small live samples moved the WRONG way (VERY STRONG
# 95.8%->92.9% n=14, MODERATE-STRONG 91.7%->85.7% n=7), too small to trust
# either direction. Same alert-only treatment as MODERATE above: windows
# still tier, alert and accumulate history, they just never stake — revisit
# once more live data accumulates. STRONG/EXTREME unaffected (95.1%/100%,
# essentially unchanged by the same filters at their larger sample sizes).
NO_STAKE_TIERS = {"WEAK", "MODERATE", "VERY STRONG", "MODERATE-STRONG"}

# Low-ADX-only gate for MODERATE/MODERATE-STRONG (added 2026-07-03). The
# universal ADX bands above were validated on the whole >=4bps population,
# not per-tier — the owner noticed MODERATE fires most often when the market is
# genuinely directionless, and checking that directly (14-day candles
# reconstruction) confirmed both of these tiers don't fit the universal
# bands at all: MODERATE's accuracy is actually WORST in the "confirmed"
# 25-30 band (73.6%) and BEST at low ADX (0-15: 84.1%, tightened to ADX<15
# + outside NY-open: 85.9%, n=78); MODERATE-STRONG shows the same shape
# (0-15: 92.2%, ex-NY: 93.6% n=47, vs 82-88% in the 20-35 range) — the
# opposite pattern from STRONG+, where mid-range ADX bands help. Both
# clear their own required_edge_acc bar restricted to ADX<15 (MODERATE
# 85.9% vs 85.0% bar; MODERATE-STRONG 93.6% vs 90.7% bar) where the
# unfiltered blend does not. Costs volume (~13% of MODERATE's windows,
# ~10% of MODERATE-STRONG's) for tiers that go from marginal/negative to
# genuinely +EV.
LOW_ADX_ONLY_TIERS = {"MODERATE": 15.0, "MODERATE-STRONG": 15.0}

# ---------------------------------------------------------------------------
# WEAK-EDGE: opt-in high-risk carve-out of the WEAK tier (added 2026-07-04).
# 30-day conditional-accuracy study of the 3.5-4.0bps sub-band (see project
# memory "WEAK 3.5-4.0bps band conditional-accuracy study"): the band's
# blended 73.5% accuracy is exactly the market's breakeven, but restricted to
# a QUIET tape it runs meaningfully hotter, robust across both independent
# 15-day halves:
#   ADX < 15              -> 85.7% (n=49)
#   regime == RANGE       -> 82.6% (n=69)
#   combined ADX<15|RANGE -> 81.0% (n=84, ~2.8/day, EV +$10.1/$100 at the
#                            band's ~36% payout; survives a 20% payout haircut)
# Economic logic: a 3.5-4bps 3-minute move in a quiet, low-vol market is an
# outsized impulse relative to noise and continues; the same move in a loud
# tape is noise (vol-expand 62.5%, high-ATR 64.4%, NY 16-21 UTC 66.0%).
#
# HIGH RISK, deliberately opt-in everywhere: WEAK stays in NO_STAKE_TIERS;
# a qualifying window is only rebranded to the WEAK-EDGE pseudo-tier when
# ALL of: env flag on, |decision bps| in [3.5, 4.0), and (ADX < 15 or RANGE
# regime) — and only at the T+3 checkpoint (the early T+2/dwell paths never
# reach WEAK-band moves; the study's decision point was T+3). Dashboard
# users additionally need a per-user opt-in checkbox
# (binance_5min_user_capital.weak_edge_optin) before the stake is shown to
# them. The filter is half-split robust but still derived in-sample on the
# same 30 days — the per-tier auto-gate below (WEAK-EDGE is a member) is the
# live safety net, per feedback_full_run_validation_discipline.
WEAK_EDGE_ENABLED = os.getenv("BINANCE_WEAK_EDGE_ENABLED", "0") not in ("0", "false", "False", "")
# Live kill bar (added 2026-07-05, after WEAK-EDGE went 0-for-2 live —
# feedback_verify_both_toggles_before_off.md — while the only "off" lever
# was a manual env var + restart nobody was watching). Mirrors LATE's
# LATE_KILL_MIN_FIRES/LATE_KILL_ACC pattern exactly: once >=MIN_FIRES
# decided WEAK-EDGE stakes exist, if live accuracy is below KILL_ACC the
# tier disables itself for the rest of this process (no more STAKE alerts —
# NO STAKE windows were already console-only, never Telegram, since
# 2026-07-01). ACC default of 0.76 is the owner's own stated bar ("kill if live
# acc <~76%" — the +EV floor at this band's real ~36% payout). Latches on
# permanently once tripped; re-enabling is a manual call, same no-auto-
# resume posture as LATE and the go-live hardening generally.
WEAK_EDGE_KILL_MIN_FIRES = int(os.getenv("BINANCE_WEAK_EDGE_KILL_MIN_FIRES", "10"))
WEAK_EDGE_KILL_ACC = float(os.getenv("BINANCE_WEAK_EDGE_KILL_ACC", "0.76"))
WEAK_EDGE_LABEL = "WEAK-EDGE"
WEAK_EDGE_BAND_LO = 3.5
WEAK_EDGE_BAND_HI = 4.0
WEAK_EDGE_ADX_MAX = 15.0
WEAK_EDGE_BACKTEST_ACC = 0.810   # 30-day conditional accuracy of ADX<15|RANGE
# Flat 0.5% of bankroll (MODERATE's baseline rung) — deliberately NOT
# Kelly-sized: assumed_payout_for(WEAK-EDGE) would be (1-0.81)/0.81 = 23.5%,
# but the market prices this BAND at its blended 73.5% (~36% payout)
# regardless of our filter, so Kelly against the too-thin 23.5% proxy would
# zero out a genuinely +EV stake. Flat & small until live sample says more.
WEAK_EDGE_STAKE_PCT = 0.005
# RANGE regime mirrors the backtest's vectorized definition (predict scripts:
# adx<18 AND NOT vol-expanding AND ATR contracting), computed from the same
# 1-min klines the ADX refresh already fetches. vol_N = close.pct_change()
# rolling(N).std(); atr_pct_slope = ewm(3)-ewm(8) of atr_pct (predict_bot
# definitions). Live ADX here is pandas_ta's, not the backtest's own calc —
# close but not identical; acceptable since ADX<15 (the dominant leg of the
# condition) uses the same live ADX already validated for LOW_ADX_ONLY_TIERS.
WEAK_EDGE_RANGE_ADX_MAX = 18.0
WEAK_EDGE_VOL_EXPAND_RATIO = 1.15
# Updated by the ADX-refresh block in main(); asof guards staleness so a
# frozen kline feed can't leave is_range=True pinned for hours.
RANGE_REGIME_STATE = {"is_range": False, "asof": None}
RANGE_REGIME_MAX_AGE_SECONDS = 120.0


def weak_edge_market_ok(adx_value):
    """True when the quiet-tape condition (ADX<15 OR RANGE regime) holds right
    now — the WEAK-EDGE qualifying condition beyond the bps band itself."""
    if adx_value is not None and adx_value < WEAK_EDGE_ADX_MAX:
        return True
    st = RANGE_REGIME_STATE
    if st["is_range"] and st["asof"] is not None:
        age = (datetime.now(timezone.utc) - st["asof"]).total_seconds()
        if age <= RANGE_REGIME_MAX_AGE_SECONDS:
            return True
    return False


def refresh_range_regime(klines_df, adx_value):
    """Recompute RANGE_REGIME_STATE from the (already-fetched) 1-min klines.
    Drops the still-open last row — the backtest definition used closed
    candles only. Best-effort: any failure leaves is_range=False."""
    try:
        df = klines_df.iloc[:-1]
        close = df["close"].astype(float)
        ret = close.pct_change()
        vol_fast = float(ret.rolling(7).std().iloc[-1])
        vol_slow = float(ret.rolling(30).std().iloc[-1])
        expanding = bool(vol_fast > 0 and vol_slow > 0
                         and (vol_fast / vol_slow) > WEAK_EDGE_VOL_EXPAND_RATIO)
        tr = pd.concat([
            df["high"].astype(float) - df["low"].astype(float),
            (df["high"].astype(float) - close.shift()).abs(),
            (df["low"].astype(float) - close.shift()).abs(),
        ], axis=1).max(axis=1)
        atr_pct = tr.ewm(alpha=1 / 14, adjust=False).mean() / close * 100
        slope = float((atr_pct.ewm(span=3, adjust=False).mean()
                       - atr_pct.ewm(span=8, adjust=False).mean()).iloc[-1])
        is_range = (adx_value is not None and adx_value < WEAK_EDGE_RANGE_ADX_MAX
                    and not expanding and slope < 0)
        RANGE_REGIME_STATE["is_range"] = bool(is_range)
        RANGE_REGIME_STATE["asof"] = datetime.now(timezone.utc)
    except Exception as e:
        RANGE_REGIME_STATE["is_range"] = False
        RANGE_REGIME_STATE["asof"] = None
        print(f"[{datetime.now(timezone.utc).isoformat()}] range-regime refresh failed: {e}", flush=True)


# Adaptive auto on/off gate per tier (added 2026-07-03). the owner noticed staked
# accuracy dropped ~10pt overall; traced to MODERATE (82.0% backtest -> 50.0%
# live, n=9) and MODERATE-STRONG (87.7% -> 70.0%, n=10) collapsing same-day
# while STRONG stayed at 100%. Rather than a one-time manual tighten, each
# tier's OWN rolling live accuracy (from binance_5min_windows, survives
# restarts) is compared against ITS OWN backtest accuracy, not a flat bar
# shared across tiers — deliberately, because payout differs by tier: a
# smaller confirmed move (MODERATE) means the market is less certain / prices
# it closer to even money, so it pays out MORE per win (e.g. ~20-25% seen in
# practice) and does not need as high a hit-rate to stay +EV as STRONG+,
# where the market has already priced in high confidence and the payout is
# thin (~8-13% seen in practice), demanding near-backtest accuracy just to
# break even. Anchoring the gate to each tier's own backtest_acc (used here
# as a proxy for the market's implied breakeven probability) means MODERATE
# isn't held to a stricter bar than its economics require — it only gets
# switched off when it falls short of ITS OWN normal range, not some
# one-size-fits-all threshold.
#   disable when rolling_acc < backtest_acc - AUTO_GATE_DISABLE_MARGIN
#   re-enable when rolling_acc >= backtest_acc + MIN_EDGE_MARGIN
# (re-enable bar is ABOVE backtest_acc, not below it — see MIN_EDGE_MARGIN
# below for why; this plus the disable margin gives a wide hysteresis band,
# no flapping).
# MIN_N guards against judging off tiny samples — EXTREME/VERY STRONG rarely
# hit 8 staked+decided windows in the lookback, so they stay ungated in
# practice without needing a separate carve-out.
#
# MIN_EDGE_MARGIN (added 2026-07-03, "profit ramp" rework): backtest_acc is
# each tier's OWN breakeven point by construction (assumed_payout is derived
# from it), so "rolling_acc >= backtest_acc" alone means zero edge, not a
# real one — worked through concretely with the owner: at 82% WR and $150
# stakes, 18 losses = -$2,700, requiring wins to average +21.95% just to
# break even, which is exactly MODERATE's assumed payout (22.0%, from its
# own 82.0% backtest_acc) with no cushion at all. The OLD enable threshold
# (backtest_acc - 3pp) let a paused tier resume staking while still
# *below* its own breakeven — fixed to require a REAL cushion above
# breakeven instead. Also gates Kelly sizing itself (see
# kelly_fraction_for()) so it doesn't size up on a razor-thin, likely-noise
# edge just barely over zero.
# WEAK-EDGE included (2026-07-04) so the experimental high-risk carve-out
# gets the same self-monitoring as everything else (disable below 71%,
# re-enable at 84%); at ~2.8 fires/day it rarely reaches MIN_N in 12h, so in
# practice it stays ungated like EXTREME — but a genuine cold streak within
# one day still trips it. Kelly sizing is exempted for it (see
# WEAK_EDGE_STAKE_PCT's comment + stake_fraction()).
AUTO_GATE_TIERS = {"EXTREME", "VERY STRONG", "STRONG", "MODERATE-STRONG", "MODERATE", "WEAK-EDGE"}
AUTO_GATE_LOOKBACK_HOURS = 12
AUTO_GATE_MIN_N = 8
AUTO_GATE_DISABLE_MARGIN = 0.10
MIN_EDGE_MARGIN = 0.03
MIN_EDGE_MARGIN_CEILING = 0.99  # see required_edge_acc()
AUTO_GATE_REFRESH_SECONDS = 300


def required_edge_acc(backtest_acc):
    """backtest_acc + MIN_EDGE_MARGIN, capped at MIN_EDGE_MARGIN_CEILING —
    for tiers already near the top of the accuracy range (EXTREME's 96.8%
    backtest_acc + 3pp = 99.8%), an uncapped bar would demand near-perfect
    accuracy to ever resume staking, effectively a permanent lockout after
    one disable. Capping keeps the "real cushion above breakeven" intent
    without that failure mode."""
    return min(backtest_acc + MIN_EDGE_MARGIN, MIN_EDGE_MARGIN_CEILING)

_tier_acc_lists = {}
for _floor, _label, _acc in TIERS:
    _tier_acc_lists.setdefault(_label, []).append(_acc)
BACKTEST_ACC_BY_TIER = {label: sum(accs) / len(accs) for label, accs in _tier_acc_lists.items()}
# Lowest bps floor per tier label (STRONG spans two rungs -> 8.0). Used by the
# retro walk's display sizing: dwell-trigger rows persist only bps_t2/bps_t3,
# not the mid-window tick bps the decision actually fired on, so a STRONG row
# can carry bps_t2=4.1 — below every stakeable ladder rung. The tier label
# itself proves the fire bps reached the tier floor, so sizing uses at least
# that (the conservative rung for multi-rung tiers).
TIER_MIN_FLOOR = {}
for _floor, _label, _acc in TIERS:
    TIER_MIN_FLOOR[_label] = min(TIER_MIN_FLOOR.get(_label, _floor), _floor)
# WEAK-EDGE isn't in TIERS (tier_for must keep returning WEAK for the band —
# the rebrand happens in make_stake_decision only when the filter passes);
# registered here so the auto-gate/rolling-accuracy machinery covers it.
BACKTEST_ACC_BY_TIER[WEAK_EDGE_LABEL] = WEAK_EDGE_BACKTEST_ACC

# tier -> {"enabled": bool, "acc": float|None, "n": int|None}
AUTO_GATE_STATUS = {t: {"enabled": True, "acc": None, "n": None} for t in AUTO_GATE_TIERS}


def restore_gate_enabled_from_state_file():
    """Seed AUTO_GATE_STATUS's enabled flags from the last-written state
    file at startup, instead of defaulting every tier to enabled (added
    2026-07-05, investigating why MODERATE/MODERATE-STRONG's 2026-07-03
    disable never stuck). AUTO_GATE_STATUS is a bare in-memory dict reset on
    every process start; refresh_auto_gates() only flips a tier when its
    trailing-12h rolling sample clears AUTO_GATE_MIN_N (8) — a low-volume
    tier restarted while disabled routinely finds n<8 on its first
    post-restart check (see get_rolling_tier_accuracy's `if n is not None
    and n >= AUTO_GATE_MIN_N` guard: neither branch fires, so `now_enabled =
    was_enabled` just keeps the hardcoded True default), silently resuming
    staking with no AUTO-ENABLED log and no real recovery. Only `enabled` is
    restored — acc/n are re-derived fresh by the very next refresh_auto_
    gates() call regardless, so there's nothing stale to carry forward
    there."""
    try:
        with open(STATE_PATH) as f:
            payload = json.load(f)
    except (OSError, ValueError):
        return
    persisted = payload.get("auto_gate_status") or {}
    for tier, st in persisted.items():
        if tier in AUTO_GATE_STATUS and isinstance(st, dict) and "enabled" in st:
            AUTO_GATE_STATUS[tier]["enabled"] = bool(st["enabled"])


def liq_imbalance_from_pg(now, windows=(300, 900)):
    """Liquidation imbalance per trailing window, straight from the
    liquidations table (multi-exchange feed liq_service.py mirrors into
    Postgres). Returns {window_seconds: imbalance} with the same convention
    the old liq_state-based code persisted: (short_btc - long_btc) / total,
    so positive = shorts liquidated = forced BUYING (bounce fuel), 0.0 when
    the window is empty. NOTE: liquidations.ts is TEXT — bounds must be ISO
    strings with the 'T' separator (space-separated silently matches 0 rows).
    """
    out = {w: 0.0 for w in windows}
    pg = _pg_conn()
    if pg is None:
        return out
    try:
        lo = (now - timedelta(seconds=max(windows))).isoformat()
        with pg.cursor() as cur:
            cur.execute(
                "SELECT ts, side, size_btc FROM liquidations WHERE ts >= %s",
                (lo,),
            )
            rows = cur.fetchall()
        for w in windows:
            lo_w = (now - timedelta(seconds=w)).isoformat()
            long_btc = short_btc = 0.0
            for ts, side, sz in rows:
                if ts >= lo_w:
                    if side == "long":
                        long_btc += sz or 0.0
                    else:
                        short_btc += sz or 0.0
            tot = long_btc + short_btc
            out[w] = (short_btc - long_btc) / tot if tot > 0 else 0.0
    except Exception as e:
        try:
            pg.rollback()
        except Exception:
            pass
        print(f"[{datetime.now(timezone.utc).isoformat()}] liq_imbalance_from_pg failed: {e}", flush=True)
    return out


def get_rolling_tier_accuracy(tier, lookback_hours=AUTO_GATE_LOOKBACK_HOURS):
    """Live (not backtest) accuracy of this tier's NON-GATE-ELIGIBLE+DECIDED
    windows over the trailing lookback_hours, sourced from binance_5min_windows
    so it survives restarts and reflects actual conditions, not backtest.

    Judged on non-gate eligibility (ADX conditions, NY-open — everything
    should_stake() checks EXCEPT the auto-gate itself), not the historical
    `stake_ok` column (fixed 2026-07-03, "update the off mode"). Two bugs
    that fix closes: (1) once a tier is auto-disabled, every NEW decision
    for it gets stake_ok=False purely because the gate says so — filtering
    on stake_ok=true was self-referential, so the rolling pool could only
    shrink as old true-stakes aged out of the window, never refresh with
    fresh post-disable performance, meaning a disabled tier could get
    permanently stuck (no path to ever re-clear required_edge_acc). (2) for
    MODERATE/MODERATE-STRONG specifically, old stake_ok=true rows decided
    before the LOW_ADX_ONLY_TIERS gate existed included high-ADX trades
    (their worst regime) — diluting the rolling accuracy the gate judges
    itself on with data the tier can no longer even trade under current
    rules."""
    try:
        pg = _pg_conn()
        if pg is None:
            return None, None
        with pg.cursor() as cur:
            cur.execute("""
                SELECT anchor, bps_t3, adx, adx_unfavourable, correct
                FROM binance_5min_windows
                WHERE tier = %(tier)s AND correct IN ('CORRECT', 'WRONG')
                  AND anchor >= now() - (%(hrs)s || ' hours')::interval
            """, {"tier": tier, "hrs": lookback_hours})
            rows = cur.fetchall()
        adx_max = LOW_ADX_ONLY_TIERS.get(tier)
        adx_exempt = tier in ADX_UNFAVOURABLE_EXEMPT_TIERS
        eligible = [
            correct == "CORRECT" for anchor, bps_t3, adx_val, adx_unfav, correct in rows
            if (adx_exempt or not adx_unfav)
            and (adx_max is None or (adx_val is not None and adx_val < adx_max))
            and not (bps_t3 is not None and abs(bps_t3) < NY_OPEN_MIN_STAKE_BPS and ny_open_tightening(anchor))
        ]
        n = len(eligible)
        if not n:
            return 0, None
        return n, (sum(eligible) / n)
    except Exception as e:
        print(f"[{datetime.now(timezone.utc).isoformat()}] rolling tier accuracy query failed "
              f"(tier={tier}): {e}", flush=True)
        return None, None


def refresh_auto_gates():
    """Re-evaluate each AUTO_GATE_TIERS tier's rolling live accuracy against
    its own backtest accuracy and flip AUTO_GATE_STATUS on/off with
    hysteresis. Logs (console + Telegram) on any state transition."""
    for tier in AUTO_GATE_TIERS:
        backtest_acc = BACKTEST_ACC_BY_TIER.get(tier)
        n, acc = get_rolling_tier_accuracy(tier)
        prev = AUTO_GATE_STATUS[tier]
        was_enabled = prev["enabled"]
        now_enabled = was_enabled
        if n is not None and n >= AUTO_GATE_MIN_N and backtest_acc is not None:
            if was_enabled and acc < (backtest_acc - AUTO_GATE_DISABLE_MARGIN):
                now_enabled = False
            elif not was_enabled and acc >= required_edge_acc(backtest_acc):
                now_enabled = True
        AUTO_GATE_STATUS[tier] = {"enabled": now_enabled, "acc": acc, "n": n}
        if now_enabled != was_enabled:
            acc_str = f"{acc * 100:.1f}%" if acc is not None else "n/a"
            bt_str = f"{backtest_acc * 100:.1f}%" if backtest_acc is not None else "n/a"
            if now_enabled:
                msg = (f"[{tier}] AUTO-ENABLED — rolling live accuracy recovered to {acc_str} "
                       f"(n={n}, last {AUTO_GATE_LOOKBACK_HOURS}h) vs backtest {bt_str}")
            else:
                msg = (f"[{tier}] AUTO-DISABLED — rolling live accuracy dropped to {acc_str} "
                       f"(n={n}, last {AUTO_GATE_LOOKBACK_HOURS}h) vs backtest {bt_str}. "
                       f"Staking paused for this tier until it recovers.")
            print(f"[{datetime.now(timezone.utc).isoformat()}] {msg}", flush=True)
            tg(f"{'🟢' if now_enabled else '🔴'} <b>{tier} auto-gate "
               f"{'ENABLED' if now_enabled else 'DISABLED'}</b>\n{msg}")

# NY-open tightening (added 2026-07-02, the owner noticed lower W/R at NY open).
# 18-month backtest (157k windows from 1-min candles, NY-clock so DST-safe)
# confirmed: on WEEKDAYS between 09:30 and 12:00 America/New_York the T+3
# continuation edge degrades hard for sub-10bps moves (4-6bps 81.3%->70.1%,
# 6-8bps 87.1%->75.8%, 8-10bps 90.0%->78.2%), moderately for 10-20bps
# (93.9%->87.7%), and barely for >=20bps (96.9%->94.9%, 98.4%->98.1%).
# Weekends show NO dip in the same clock window (89.6% vs 89.0% baseline),
# so this is the US equity open, not an hour-of-day artifact — gate is
# weekday-only. Rule: in the window, moves <10bps are hard NO STAKE;
# 10-20bps stakes one rung smaller; >=20bps unchanged. 12:00-13:00 NY stays
# mildly soft for 6-8bps (~79-82% vs 87.1%) but is left ungated for now.
NY_TZ = ZoneInfo("America/New_York")
NY_OPEN_GATE_START_MIN = 9 * 60 + 30   # 09:30 New York
NY_OPEN_GATE_END_MIN = 12 * 60         # 12:00 New York
# Raised 10 -> 20 on 2026-07-04 (loser elimination pass): the 10-20bps
# in-window band is the weakest staked accuracy anywhere in the system
# (87.7% over the 18-month backtest vs >=94.9% for everything else that
# stakes), and both live NY-window losers on record (07-02 14:05 at
# 10.6bps, 07-02 14:35 at 18.1bps — full V-reversals after the decision)
# sat in exactly that band, while all five live in-window stakes >=20bps
# won. Blocking 10-20 sacrifices a thin +EV (~+0.14/stake at 87.7% acc,
# ~0.30 payout) for materially fewer losers, per feedback_minimize_losses.
# Side effect: the old "10-20bps stakes one rung smaller" zone is now
# empty (hard-blocked instead) — that rung-cut code path is dormant, not
# wrong. Env-overridable for quick revert without a code change.
NY_OPEN_MIN_STAKE_BPS = float(os.getenv("BINANCE_NY_MIN_STAKE_BPS", "20"))
# (floor_bps, accuracy) measured 09:30-12:00 NY weekdays over the same
# 18-month backtest — replaces the all-hours TIERS accuracy in alerts/
# dashboard while the tightening window is active, so displayed confidence
# matches the regime the stake is actually taken in.
NY_OPEN_TIER_ACC = [
    (30.0, 0.981),
    (20.0, 0.949),
    (10.0, 0.877),
    (8.0, 0.782),
    (6.0, 0.758),
    (4.0, 0.701),
    (2.0, 0.636),
]


def ny_open_tightening(when_utc):
    """True while the NY-open tightening window is active: weekdays
    09:30-12:00 America/New_York (DST-aware)."""
    ny = when_utc.astimezone(NY_TZ)
    if ny.weekday() >= 5:
        return False
    minute = ny.hour * 60 + ny.minute
    return NY_OPEN_GATE_START_MIN <= minute < NY_OPEN_GATE_END_MIN


def ny_open_tier_acc(abs_bps, fallback_acc):
    for floor, acc in NY_OPEN_TIER_ACC:
        if abs_bps >= floor:
            return acc
    return fallback_acc


def tier_for(abs_bps):
    for floor, label, acc in TIERS:
        if abs_bps >= floor:
            return label, acc
    return None, None


# T=0 informational lean (added 2026-07-04): NOT a stake signal. Per
# scripts/predict_net_streak_reversal.py's validated result, fading the net
# color of the trailing 5 closed 1-min candles gets 51.8% accuracy (n=4018,
# p=0.011) predicting the NEXT 5-min direction — real but far too thin to
# survive fees/spread, unlike the T+3 continuation edge this monitor stakes
# on. Every other from-scratch (T=0) method tried (rule heuristics, trained
# ML, a 10k-combo ADX x ATR grid search, the bot's own regime classifier)
# landed at ~50-55%, indistinguishable from chance — see
# [[project_binance_5min_continuation_monitor]] Finding 1. This function
# exists to show an early, honestly-labeled low-confidence lean at window
# open while T+2/T+3 confirmation is pending — never gate stake_ok/tier on it.
T0_LEAN_ACCURACY = 0.518


def t0_lean_side(closed_candles):
    """Fade the net body (close-open) of the trailing 5 CLOSED 1-min candles
    ending at window open. Returns "HIGHER"/"LOWER", or None if fewer than 5
    closed candles are available yet (e.g. just after a restart)."""
    if len(closed_candles) < 5:
        return None
    net_body = sum(c["c"] - c["o"] for c in closed_candles[-5:])
    if net_body == 0:
        return None
    return "LOWER" if net_body > 0 else "HIGHER"


def adx_confirmed(adx_value):
    if adx_value is None:
        return False
    return any(lo <= adx_value < hi for lo, hi in ADX_CONFIRMED_BANDS)


def adx_unfavourable(adx_value):
    if adx_value is None:
        return False
    return any(lo <= adx_value < hi for lo, hi in ADX_UNFAVOURABLE_BANDS)


def should_stake(label, adx_value, abs_bps=None, when_utc=None):
    """False -> explicit no-stake call: the global FADE-ONLY switch, WEAK tier,
    a tier auto-gated off by its own rolling live accuracy, the unfavourable
    ADX[20,25)/ADX>=35 bands (STRONG/VERY STRONG/EXTREME exempt — see
    ADX_UNFAVOURABLE_EXEMPT_TIERS), a <10bps move inside the NY-open
    tightening window, or (MODERATE/MODERATE-STRONG only) ADX above their
    LOW_ADX_ONLY_TIERS bar."""
    if not CONTINUATION_STAKE_ENABLED:
        return False
    if label in NO_STAKE_TIERS:
        return False
    if label in AUTO_GATE_TIERS and not AUTO_GATE_STATUS.get(label, {}).get("enabled", True):
        return False
    if label not in ADX_UNFAVOURABLE_EXEMPT_TIERS and adx_unfavourable(adx_value):
        return False
    if label in LOW_ADX_ONLY_TIERS and adx_value is not None and adx_value >= LOW_ADX_ONLY_TIERS[label]:
        return False
    if (abs_bps is not None and when_utc is not None
            and abs_bps < NY_OPEN_MIN_STAKE_BPS and ny_open_tightening(when_utc)):
        return False
    return True


def no_stake_reason(label, adx_value, abs_bps=None, when_utc=None):
    """Human-readable reason matching should_stake()'s checks, in order."""
    if not CONTINUATION_STAKE_ENABLED:
        return ("FADE-ONLY mode: the T+3 continuation book is alert-only "
                "(measured no edge — STRONG +0.19% over n=1,199 post-refit)")
    if label in NO_STAKE_TIERS:
        return f"{label} tier is alert-only (no stake)"
    if label in AUTO_GATE_TIERS and not AUTO_GATE_STATUS.get(label, {}).get("enabled", True):
        st = AUTO_GATE_STATUS[label]
        acc_str = f"{st['acc'] * 100:.1f}%" if st["acc"] is not None else "n/a"
        return f"auto-disabled: {label} rolling live accuracy {acc_str} (n={st['n']}) below its own backtest bar"
    if label not in ADX_UNFAVOURABLE_EXEMPT_TIERS and adx_unfavourable(adx_value):
        return "unfavourable ADX band (20-25 or 35+)"
    if label in LOW_ADX_ONLY_TIERS and adx_value is not None and adx_value >= LOW_ADX_ONLY_TIERS[label]:
        return f"{label} requires ADX < {LOW_ADX_ONLY_TIERS[label]:.0f} (directionless market only), currently {adx_value:.1f}"
    if (abs_bps is not None and when_utc is not None
            and abs_bps < NY_OPEN_MIN_STAKE_BPS and ny_open_tightening(when_utc)):
        return "NY-open window (<10bps, 09:30-12:00 NY weekdays)"
    return None


def stake_pct_for(abs_bps):
    for floor, pct in STAKE_PCT_TIERS:
        if abs_bps >= floor:
            return pct
    return 0.0


# Kelly-informed sizing (added 2026-07-03) — on top of the fixed ladder above.
# Confirmed mechanism (see project memory): each contract share redeems $1 if
# correct / $0 if wrong, and the price you pay reflects the crowd's implied
# probability. We still can't read that live price (no API for this product),
# but each tier's own backtest_acc is the best available proxy for it, which
# gives an assumed payout b = (1-backtest_acc)/backtest_acc for that tier —
# same proxy already used by the auto-gate above. Comparing the tier's
# ROLLING LIVE accuracy (same 12h/AUTO_GATE_MIN_N bar as the auto-gate, reused
# from AUTO_GATE_STATUS so gate and sizing always agree) to that assumed
# payout gives a real per-tier Kelly fraction: positive only while the tier
# is currently running hotter than its own historical/backtest rate, zero (no
# stake) at or below it, since betting AT exactly the assumed market price has
# zero edge by construction — this is deliberately not a floor-guaranteed
# stake. Heavily damped (KELLY_FRACTIONAL_MULT) vs full Kelly, which at these
# win rates implies absurd fractions (e.g. ~59% of bankroll for STRONG running
# at 95%+) — same "far below full Kelly" philosophy as the original ladder,
# see feedback_minimize_losses. Falls back to the static ladder above when
# there isn't yet enough rolling sample (same insufficient-data case where the
# auto-gate also declines to act).
KELLY_FRACTIONAL_MULT = 0.08
KELLY_MAX_STAKE_PCT = 0.05  # same hard cap as the old ladder's top rung

# Final stake-size multiplier (added 2026-07-05, the owner: "now that we have
# fine-tuned it to have 96%+ accuracy... increase the stake size by a
# multiple of 2") — applied as the LAST step in stake_fraction(), after
# Kelly/ladder sizing, the ADX-confirm bump, NY-tightening, and the payout
# cap have all already run. Effectively doubles KELLY_MAX_STAKE_PCT too
# (5% -> 10% per trade), since the cap is baked into the pre-multiplier
# value — deliberate, not a bug (confirmed wanting the ceiling to move
# with it). Even doubled, this is still nowhere near full Kelly: at 0.08 of
# full Kelly (see KELLY_FRACTIONAL_MULT's comment — full Kelly for STRONG at
# 95%+ implies ~59% of bankroll), doubling the final result is equivalent to
# ~0.16 of full Kelly, still heavily damped. NOTE: Kelly sizing already
# scales up automatically as rolling live accuracy improves (f = p -
# (1-p)/b grows with p) — this multiplier is a DELIBERATE additional
# increase on top of that, not a correction for Kelly failing to track the
# accuracy gains.
STAKE_SIZE_MULTIPLIER = 2.0


def assumed_payout_for(label):
    """Assumed payout ratio b (profit per $1 staked if correct) for `label`,
    e.g. b=0.20 means a correct $100 stake returns $20 profit ($120 back).
    Derived from the tier's own backtest_acc as a proxy for the live order
    price (1/price - 1) since there's no API to read the real one — see
    project memory's "Mechanism confirmed" note. None for an unknown tier."""
    backtest_acc = BACKTEST_ACC_BY_TIER.get(label)
    if backtest_acc is None or backtest_acc <= 0:
        return None
    return (1.0 - backtest_acc) / backtest_acc


# Time-aware payout (added 2026-07-03). The flat assumed_payout_for() above
# treats every decision as if it fired exactly at the T+3 checkpoint (120s
# before expiry) — but decisions actually fire at T+2 (180s left), mid-window
# dwell (~120-180s), T+3 (120s), and now the late trigger (~10-90s), and ~20
# real app observations confirmed the market's actual price for the SAME bps
# move varies a lot with time remaining (a 4bps move at 200s left priced
# ~0.69; the same-size move at 30s left priced ~0.95).
#
# Calibration: interpolate z(bps) = Phi^-1(backtest_acc) directly through the
# 7 already-validated TIERS points (measured at tau=120s over 14-18 months)
# rather than fitting one global sigma — a single Brownian sigma was tried
# first and failed its own sanity check (per-tier implied sigmas spread 4x,
# 0.37 at 2bps to 1.48 at 30bps: big partial moves continue disproportionately
# more often than a random walk predicts, i.e. real momentum, so no single
# diffusion constant fits). Interpolating z keeps the validated bps->accuracy
# curve EXACT at tau=120s and applies the standard Brownian sqrt-time scaling
# only across the one dimension (time remaining) that the backtest never
# varied: z(bps, tau) = z_120(bps) * sqrt(120/tau).
#
# Known honest limitation (checked against the motivating real trade): this
# does NOT explain the 47.2%-actual vs 12.4%-assumed STRONG anecdote — that
# decision fired exactly AT tau=120s where this model reproduces the flat
# number by construction. Its value is for genuinely different-tau decisions
# (T+2-early, the late trigger), not a universal fix for market-price noise.
CALIBRATION_TAU_SECONDS = 120.0
# (bps, z) knots at tau=120s, from TIERS: z = norm.ppf(backtest_acc).
# Hardcoded rather than importing scipy in the hot loop — values are pure
# functions of TIERS' accuracies, regenerate if TIERS ever changes:
#   from scipy.stats import norm; [(f, norm.ppf(a)) for f, _, a in TIERS]
Z_KNOTS_120S = [
    (2.0, 0.4987),   # WEAK 0.691
    (4.0, 0.9154),   # MODERATE 0.820
    (6.0, 1.1601),   # MODERATE-STRONG 0.877
    (8.0, 1.1901),   # STRONG(8-10) 0.883
    (10.0, 1.2591),  # STRONG(10-20) 0.896
    (20.0, 1.6646),  # VERY STRONG 0.952
    (30.0, 1.8522),  # EXTREME 0.968
]
# Below the first knot there's no validated accuracy (sub-2bps was coin-flip
# in the backtest) — clamp z at the 2bps value rather than extrapolating to 0.
# Above the last knot, clamp at the 30bps value (same conservative posture).

# Market-price calibration (added 2026-07-03, same day as the model). The
# uncalibrated model prices at OUR backtest accuracy, but 14 real app
# observations (the owner's screenshots) + 1 real settled trade showed the market
# consistently prices LESS confidently than our backtest — under-estimated
# the real payout in 14/14 points, mean -11pp. That gap is the strategy's
# actual edge, but it made the displayed "Expected To Win"/est-return
# systematically lower than the app's real numbers. Least-squares fit of
# p_market = Phi(K * z_backtest * sqrt(120/tau)) over those observations:
# K=0.68, mean |err| 13.0pp -> 9.1pp, bias eliminated (5/14 under vs 14/14).
# SCOPE: this calibrates the *display/est-P&L* payout only. Kelly sizing
# deliberately keeps the UNcalibrated flat assumed_payout_for(label)
# (= each tier's own breakeven) — sizing off the richer market payout would
# loosen the conservative posture, exactly what feedback_minimize_losses
# says not to do. Refit K as more (tau, bps, price) observations accumulate.
MARKET_Z_CALIB = 0.68

# Refit 2026-07-04 (evening): 110 fresh app screenshots (4 windows, taus
# 2-293s, deltas -$56..+$58, Up tab throughout so both the favored side's
# ask AND the losing side's price were observed). The single-K form was
# hiding two opposite errors by move size: sub-4bps moves were OVERpriced
# (the 2bps z-clamp forces p~0.59 where the market really sits ~0.50 —
# five dead-even observations all priced 0.48-0.52) and 6-10bps moves were
# badly UNDERpriced (model p~0.77-0.80 vs the market's real 0.86-0.96,
# -10pp bias; the market's confidence keeps climbing 4->8bps where our
# backtest z-curve plateaus). Fix: fit the MARKET's own z-curve directly,
# piecewise-linear in bps, anchored at z(0)=0, clamped flat above 10bps
# (no favored-side data beyond 9.2bps; the flat clamp matches the observed
# dead-payout zone: big delta near expiry -> p ~0.95-0.99). Same
# sqrt(120/tau) time scaling as before. Fit quality: mean |err| 8.1pp ->
# 5.0pp on the 81 favored-side points, bias -1.4pp -> -0.3pp; validated
# out-of-fit on the 21 losing-side points (4.4pp) and 5 dead-even points
# (nails 0.50). Fit script: scratchpad fit_market_zcurve.py; raw points:
# binance_screenshot_points.csv (same scratchpad), keep for future refits.
# Refit 2026-07-05: +57 fresh points (11:40/11:45 IST Jul-5 windows, taus
# 6-292s, deltas -$22..+$30, incl. losing-side and Orders-Not-Allowed
# near-expiry rows) on top of the 07-04 110-point set -> 127 favored-side
# fit rows. The Saturday Asia-morning tape prices 2-4bps moves MUCH more
# confidently than Friday's US-evening tape did (a $14-20 move trades
# 0.76-0.90 vs 0.55-0.65) — the 4bps knot jumps 0.45->1.11 and 2bps
# 0.39->0.50; the 8-10bps clamp eases 1.62->1.56. Combined mae 6.0pp,
# bias +0.2pp (old K=0.68 model: 9.8pp on the same rows). Known residuals:
# regime variance between quiet-Asia and US-session tapes (single curve
# averages them), and <45s bucket bias +6.6pp (sqrt(120/tau) overstates
# certainty for small-bps near expiry — the app keeps paying ~2x the
# model's payout there; conservative direction for displayed To Win).
MARKET_Z_KNOTS = [
    (0.0, 0.0),
    (2.0, 0.5047),
    (4.0, 1.1131),
    (6.0, 1.1139),
    (8.0, 1.5561),
    (10.0, 1.5562),
]


def _market_z(abs_bps):
    """Piecewise-linear interpolation of the MARKET's implied z through
    MARKET_Z_KNOTS (2026-07-04 screenshot refit), clamped flat above the
    last knot."""
    knots = MARKET_Z_KNOTS
    if abs_bps <= knots[0][0]:
        return knots[0][1]
    if abs_bps >= knots[-1][0]:
        return knots[-1][1]
    for (b0, z0), (b1, z1) in zip(knots, knots[1:]):
        if b0 <= abs_bps <= b1:
            frac = (abs_bps - b0) / (b1 - b0)
            return z0 + frac * (z1 - z0)
    return knots[-1][1]  # unreachable, defensive


def _z_at_120s(abs_bps):
    """Piecewise-linear interpolation of z through Z_KNOTS_120S, clamped at
    both ends."""
    knots = Z_KNOTS_120S
    if abs_bps <= knots[0][0]:
        return knots[0][1]
    if abs_bps >= knots[-1][0]:
        return knots[-1][1]
    for (b0, z0), (b1, z1) in zip(knots, knots[1:]):
        if b0 <= abs_bps <= b1:
            frac = (abs_bps - b0) / (b1 - b0)
            return z0 + frac * (z1 - z0)
    return knots[-1][1]  # unreachable, defensive


def _norm_cdf(z):
    """Standard normal CDF via erf — avoids a scipy import in the monitor's
    hot loop (math.erf is C-level and exact enough here)."""
    import math
    return 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))


def assumed_payout_for_time(abs_bps, seconds_remaining, fallback_label=None):
    """Time-aware assumed payout ratio b for a move of `abs_bps` decided with
    `seconds_remaining` until expiry, calibrated to the MARKET's observed
    pricing via MARKET_Z_KNOTS (2026-07-04 110-screenshot refit — this
    function's job is to predict the app's real "To Win", not our own
    breakeven). Falls back to the flat per-tier number (or None) when inputs
    are missing/degenerate — never lets a bad time value produce a wild
    payout."""
    import math
    if (abs_bps is None or seconds_remaining is None or seconds_remaining <= 0
            or abs_bps <= 0):
        return assumed_payout_for(fallback_label) if fallback_label is not None else None
    z = _market_z(abs_bps) * math.sqrt(CALIBRATION_TAU_SECONDS / seconds_remaining)
    p_win = _norm_cdf(z)
    # p_win -> 1.0 as tau -> 0 for any nonzero bps; floor the payout at a
    # tiny epsilon instead of returning exactly 0 so downstream EV math
    # (stake * payout) stays well-defined.
    if p_win >= 0.9999:
        p_win = 0.9999
    if p_win <= 0.5:
        # never assume worse than even-money for a move in our favor
        p_win = 0.5
    return (1.0 - p_win) / p_win


def assumed_payout_for_time_legacy(abs_bps, seconds_remaining, fallback_label=None):
    """The pre-2026-07-04-refit calibration (single MARKET_Z_CALIB scalar on
    the backtest z-curve). Kept ONLY for the VALUE trigger: its 0.30 payout
    target + 93.5%-WR backtest were validated against THIS model's numbers,
    and the refit moves payout estimates enough to materially change which
    retraces fire (the new curve needs a deeper retrace before showing
    >=30%). Do not silently re-point the trigger at the new curve — re-run
    the VALUE tick replay against it first, then retune VALUE_PAYOUT_TARGET
    and retire this function."""
    import math
    if (abs_bps is None or seconds_remaining is None or seconds_remaining <= 0
            or abs_bps <= 0):
        return assumed_payout_for(fallback_label) if fallback_label is not None else None
    z_120 = _z_at_120s(abs_bps)
    z = MARKET_Z_CALIB * z_120 * math.sqrt(CALIBRATION_TAU_SECONDS / seconds_remaining)
    p_win = _norm_cdf(z)
    if p_win >= 0.9999:
        p_win = 0.9999
    if p_win <= 0.5:
        p_win = 0.5
    return (1.0 - p_win) / p_win


def kelly_fraction_for(label):
    """Damped-Kelly stake fraction for `label`, or None if there isn't enough
    rolling live sample to trust it (caller should fall back to the static
    ladder in that case). Requires rolling accuracy to clear the tier's own
    breakeven (backtest_acc) by MIN_EDGE_MARGIN before sizing anything —
    added 2026-07-03 as part of the "profit ramp" rework: `p > backtest_acc`
    alone is zero real edge (backtest_acc IS the breakeven point, since
    assumed_payout is derived from it), so staking right at that line was
    committing capital to what's essentially noise, not a real edge."""
    b = assumed_payout_for(label)
    backtest_acc = BACKTEST_ACC_BY_TIER.get(label)
    st = AUTO_GATE_STATUS.get(label)
    if (b is None or backtest_acc is None or st is None or st.get("n") is None
            or st["n"] < AUTO_GATE_MIN_N or st.get("acc") is None):
        return None
    p = st["acc"]
    if p < required_edge_acc(backtest_acc):
        return 0.0
    f_full = p - (1.0 - p) / b if b > 0 else 0.0
    return max(0.0, f_full)


def stake_fraction(label, adx_value, abs_bps, when_utc=None, seconds_remaining=None):
    """Final stake fraction of bankroll (post ADX-confirm bump), independent
    of any one user's capital — 2026-07-02: capital now varies per dashboard
    user (user A vs user B, etc.), so this is the thing that gets persisted/
    displayed; multiplying by a specific user's capital happens downstream
    (Telegram uses BANKROLL_USDT for the owner, the dashboard uses whichever user
    is logged in).

    Payout cap (added 2026-07-05, "risk to reward too low" — the owner noticed
    STRONG+ stakes showing $90 risked for $1-5 To Win): both the Kelly path
    and the static-ladder fallback size off assumed_payout_for(label), each
    tier's OWN flat backtest-implied breakeven payout — but the REAL
    time-aware market payout at the actual decision point (seconds_remaining
    can be well under the 120s T+3 assumes, e.g. dwell/late fires) is often
    thinner, most severely for STRONG (real ran ~half the flat number).
    Replayed against 207 real historical stakes: capping the final pct by
    min(1, real_payout/flat_payout) — never loosening, since real can only
    shrink the ratio below 1 — cut real-$ losses ~38% ($619) and left VERY
    STRONG/EXTREME (whose real payout already exceeds the flat number)
    untouched. Applied as a final multiplier rather than inside
    kelly_fraction_for() so it also covers the static-ladder path, matching
    exactly what was validated."""
    if abs_bps is None or not should_stake(label, adx_value, abs_bps, when_utc):
        return 0.0
    # WEAK-EDGE: flat, small, no Kelly and no ADX-confirm bump — see
    # WEAK_EDGE_STAKE_PCT's comment (Kelly's per-tier breakeven proxy is
    # wrong for a filtered slice of a band the market prices as a blend, and
    # the ADX-confirm bands were never validated on this carve-out).
    if label == WEAK_EDGE_LABEL:
        return WEAK_EDGE_STAKE_PCT
    f_kelly = kelly_fraction_for(label)
    if f_kelly is not None:
        pct = min(KELLY_MAX_STAKE_PCT, KELLY_FRACTIONAL_MULT * f_kelly)
    else:
        pct = stake_pct_for(abs_bps)
    # NY-open tightening for 10-20bps: the tier loses ~6pp of accuracy in the
    # window (93.9% -> 87.7%) so it drops one rung, and the ADX-confirm bump
    # is suppressed — in-window all ADX bands cluster at 87-90% for this tier
    # (confirmed 15-20 = 87.0%, no better than unfiltered), so the bump would
    # size up on a signal that carries no information there.
    ny_tightened = (when_utc is not None and abs_bps < 20.0
                    and ny_open_tightening(when_utc))
    if pct > 0 and adx_confirmed(adx_value) and not ny_tightened:
        if f_kelly is not None:
            pct = min(KELLY_MAX_STAKE_PCT, pct * 1.3)
        else:
            idx = STAKE_PCT_LADDER.index(pct)
            if idx < len(STAKE_PCT_LADDER) - 1:
                pct = STAKE_PCT_LADDER[idx + 1]
    if pct > 0 and ny_tightened:
        if f_kelly is not None:
            pct = pct * 0.7
        else:
            idx = STAKE_PCT_LADDER.index(pct)
            pct = STAKE_PCT_LADDER[max(idx - 1, 0)]
    if pct > 0 and seconds_remaining is not None:
        flat_b = assumed_payout_for(label)
        real_b = assumed_payout_for_time(abs_bps, seconds_remaining)
        if flat_b and real_b is not None and real_b < flat_b:
            pct = pct * (real_b / flat_b)
        # Real-payout breakeven floor ("higher payout
        # signals only" after 31 staked/28 correct (90.3%) still net -$126).
        # The shrink above only ever downsizes a stake, never kills it, so a
        # window whose REAL time-aware payout is genuinely thin (a move
        # that's already mostly priced in by the time it fires) still gets
        # staked at some reduced size. Root cause confirmed against a real
        # screenshot: one STRONG window's real payout ran 0.065 against a
        # flat-assumed 0.112 — its rolling live accuracy (92.3%, n=39)
        # clears the FLAT breakeven (89.6%) but not a breakeven built off
        # its own REAL payout (93.9%), i.e. it looks profitable on the
        # backtest number and isn't on the number the market actually pays.
        # Zero the stake outright (not a shrink) when rolling accuracy can't
        # clear breakeven computed off THIS decision's real payout, same
        # MIN_EDGE_MARGIN cushion as every other edge check in this file.
        if pct > 0 and real_b is not None and real_b > 0:
            st = AUTO_GATE_STATUS.get(label)
            if (st and st.get("n") is not None and st["n"] >= AUTO_GATE_MIN_N
                    and st.get("acc") is not None):
                real_breakeven = 1.0 / (1.0 + real_b)
                if st["acc"] < required_edge_acc(real_breakeven):
                    pct = 0.0
    # Final multiplier (2026-07-05) — see STAKE_SIZE_MULTIPLIER's comment.
    # Applied last, after every other adjustment above.
    pct = pct * STAKE_SIZE_MULTIPLIER
    return pct


def compute_retroactive_gated_stats(hours=None, return_map=False):
    """Recompute staked/correct/accuracy/no-stake counts AS IF the current
    per-tier auto-gate (rolling 12h same-tier accuracy vs its own backtest
    bar — same rule as refresh_auto_gates()) AND the LOW_ADX_ONLY_TIERS gate
    (added 2026-07-03) had been running the whole time, instead of the raw
    historical mix (the dashboard was still showing mostly pre-gate history
    since MODERATE/MODERATE-STRONG only got auto-gated a couple hours before
    this, and every further deploy that same day kept interrupting the
    window cycle before a real post-gate sample could accumulate — see
    project memory). Walks history in chronological order maintaining each
    tier's own trailing 12h window so a row is judged only on what was known
    strictly before it (no lookahead). Deliberately does NOT touch
    est_return_usdt/stake_usdt ($ P&L) — that stays on the separate
    `binance_5min_pnl_reset` mechanism, since we can't know what $ amount
    would really have been staked on a window that wasn't actually staked
    (no recorded stake_pct for it).

    return_map=True additionally returns a pct_map (anchor -> retro stake
    fraction, only for retro-staked rows): the walk's own reconstruction of
    what stake_fraction() would size under current rules with the rolling
    state known strictly before the row — Kelly when the walk's rolling
    sample clears the tier's edge bar, static ladder otherwise, same
    ADX-confirm bump / NY-tightening rung moves. DISPLAY-ONLY (dashboard
    stake column for retro-staked rows the live gate didn't stake, added
    2026-07-05 "stake amount is not showing"): P&L sums still exclude these
    rows for the docstring's original reason."""
    try:
        pg = _pg_conn()
        if pg is None:
            return (None, {}, {}) if return_map else None
        where_clause = "WHERE status = 'EXPIRED'"
        params = {}
        cutoff_ts = None
        if hours is not None:
            # Fetch an extra AUTO_GATE_LOOKBACK_HOURS of warm-up history
            # beyond the requested window (added 2026-07-05, "but this was
            # staked" — a window near the START of a narrow hours=N walk
            # only had rolling_acc built from whatever tier_history the walk
            # itself had accumulated so far, which is LESS than the live
            # gate saw at that moment (get_rolling_tier_accuracy() always
            # looks back a full 12h from the row's own decision time,
            # regardless of how far back this query's own window starts).
            # Found on a real case: the 12h "Recent" walk showed a STRONG
            # window as retro-staked because its own n_prior was only 7
            # (just under AUTO_GATE_MIN_N) that early into the walk, while
            # the live monitor — and the unbounded hours=None walk, which
            # has no such truncation — both correctly saw n=9/Kelly-zeroed
            # from the fuller history and did NOT stake it. The extra buffer
            # rows below are walked (so tier_history/gate_state warm up
            # correctly) but never counted into stake_map/pct_map/stats —
            # cutoff_ts gates the OUTPUT, not the walk itself.
            where_clause += " AND anchor >= now() - (%(hrs)s || ' hours')::interval"
            params["hrs"] = hours + AUTO_GATE_LOOKBACK_HOURS
            cutoff_ts = datetime.now(timezone.utc) - timedelta(hours=hours)
        with pg.cursor() as cur:
            # ny_open_tightened isn't a persisted column (only lives in the
            # JSON state) — recomputed below from the anchor timestamp via
            # ny_open_tightening(), same as the live monitor does at decision time.
            cur.execute(f"""
                SELECT anchor, tier, bps_t3, bps_t2, decided_early, adx_unfavourable, adx, correct,
                       retro_t2_defer, decided_at, expired_at, atr_bps, decision_trigger, stake_pct
                FROM binance_5min_windows
                {where_clause}
                ORDER BY anchor
            """, params)
            rows = cur.fetchall()
    except Exception as e:
        # roll back so the shared connection isn't left in an aborted-
        # transaction state (which would otherwise fail every subsequent
        # query on this connection until the process restarts)
        try:
            if pg is not None:
                pg.rollback()
        except Exception:
            pass
        print(f"[{datetime.now(timezone.utc).isoformat()}] retroactive stats query failed: {e}", flush=True)
        return (None, {}) if return_map else None

    n_staked = n_correct = n_wrong = n_tie = 0
    no_stake_n = no_stake_correct = no_stake_wrong = 0
    stake_map = {}
    pct_map = {}
    tier_history = {}  # tier -> list of (anchor, is_correct), chronological
    # Stateful, mirrors AUTO_GATE_STATUS's hysteresis exactly (disable at
    # backtest_acc - AUTO_GATE_DISABLE_MARGIN, re-enable only at
    # backtest_acc + MIN_EDGE_MARGIN — see refresh_auto_gates()) rather than
    # a single stateless threshold, so this retroactive simulation matches
    # what the live gate would actually have done.
    gate_state = {t: True for t in AUTO_GATE_TIERS}

    for (anchor, tier, bps_t3, bps_t2, decided_early, adx_unfav, adx_val, correct, retro_t2_defer,
         decided_at, expired_at, atr_bps, decision_trigger, real_stake_pct) in rows:
        is_decided = correct in ("CORRECT", "WRONG")
        # Genuinely had money on it at the time ("still
        # losing in $" on a 95.2%/21-staked card that was net -$14.96) —
        # distinct from retro_stake_ok, which only asks "would today's rules
        # stake this." A row can pass every current gate/filter and still
        # have been sized to $0 live (Kelly-zeroed: rolling accuracy hadn't
        # cleared its own breakeven at that exact moment) — real, not a
        # display bug, but it means the "21 staked / 95.2%" headline was
        # counting 14 zero-dollar windows alongside the 7 that actually had
        # a stake, making the accuracy number look far more profitable than
        # the $ figure below it could ever show. Used below to gate the
        # STATS counts only — stake_map (which drives the table's "Staked
        # only" filter and the "~$" retro-sizing display) is untouched, so
        # a tier that's genuinely retro-eligible again after a gate
        # recovers still shows correctly there.
        really_staked_live = real_stake_pct is not None and real_stake_pct > 0
        ny_tight = ny_open_tightening(anchor)
        # Use the SAME bps the real decision was actually made on — bps_T2
        # for windows decided early, bps_T3 otherwise — not always bps_T3.
        # A window decided early on an 8.9bps move (correctly NY-blocked,
        # since NY-open hard-blocks <10bps) stays decided on that reading
        # even if the move grows past 10bps by T+3; re-deriving eligibility
        # from bps_T3 alone incorrectly retro-flagged such a window as
        # staked (found 2026-07-03: real stake_ok=False, NY-open-blocked at
        # T+2, but bps_T3 had grown to 16.3bps by expiry).
        decision_bps = bps_t2 if decided_early and bps_t2 is not None else bps_t3
        # T3 backward filter mirror (2026-07-05): only meaningfully testable
        # here for true-T+3 rows (decided_early=False) — dwell rows persist
        # bps_T2/bps_T3 only, not the mid-window tick bps actually fired on
        # (same limitation TIER_MIN_FLOOR's comment already documents), so a
        # sign-flip check against decision_bps==bps_t2 would be a no-op for
        # them. The retrace-off-extreme half needs tick data this walk
        # doesn't have — NOT mirrored here, so retro display slightly
        # understates the live filter's effect on dwell/T+3 rows until a
        # tick-replay backfill (matching retro_t2_defer's precedent) closes
        # the gap.
        t3_sign_flip = (
            T3_BACKWARD_FILTER_ENABLED and tier in T3_BACKWARD_FILTER_TIERS
            and not decided_early and bps_t2 is not None and bps_t3 is not None
            and (bps_t2 > 0) != (bps_t3 > 0)
        )
        # ATR-ratio filter mirror (2026-07-05) — see ATR_RATIO_MIN's comment.
        # Same dwell/T+3-only scope; atr_bps is only populated for rows
        # decided after this feature shipped (NULL for older rows, which
        # then pass through unfiltered here rather than false-failing).
        atr_thin = (
            ATR_RATIO_FILTER_ENABLED and tier in ATR_RATIO_FILTER_TIERS
            and not decided_early and atr_bps and decision_bps is not None
            and abs(decision_bps) / atr_bps < ATR_RATIO_MIN
        )
        non_gate_ok = (
            tier is not None and tier != "NONE" and tier not in NO_STAKE_TIERS
            # WEAK-EDGE excluded from the shared retro/All-time counts
            # ("I have disabled weak edge anyway, remove
            # that trade") — this is a GLOBAL exclusion, not per-viewer: the
            # All-time card is one shared computation written to the state
            # file once, not recomputed per dashboard session, so there's no
            # way to hide it only for opted-out viewers without a bigger
            # per-user refactor. Only ever 1 trade ($5) has used this tier
            # system-wide so far, so the impact on a second user (the one other
            # opted-in user) is negligible today — revisit if WEAK-EDGE
            # volume grows and this tradeoff stops being trivial.
            and tier != WEAK_EDGE_LABEL
            and not (tier not in ADX_UNFAVOURABLE_EXEMPT_TIERS and adx_unfav)
            and not (tier in LOW_ADX_ONLY_TIERS and adx_val is not None and adx_val >= LOW_ADX_ONLY_TIERS[tier])
            and not (ny_tight and decision_bps is not None and abs(decision_bps) < NY_OPEN_MIN_STAKE_BPS)
            and not t3_sign_flip
            and not atr_thin
            # T+2(early)-STRONG disable mirror (2026-07-06) — see
            # T2_EARLY_STRONG_DISABLED's comment/make_stake_decision. Must key
            # off decision_trigger == "T+2 (early)" specifically, NOT the
            # decided_early boolean — decided_early is True for BOTH the
            # T+2-exact path AND the "T+2-T+3 dwell" path (both pass
            # early=True to make_stake_decision), but only the exact-T+2
            # trigger is the one shown as underperforming (87.9% vs the
            # dwell path's 93.2%); using decided_early alone would wrongly
            # also retro-exclude the dwell path. Without this mirror the
            # retro walk (and therefore retroStakeMap/the "Staked only"
            # filter/scoreboard counts) kept showing T+2-exact STRONG rows as
            # staked even after the live gate stopped staking them going
            # forward — "retro twins must mirror live filters exactly" (see
            # project memory on the Recent-card Est. P&L bug, same lesson).
            and not (tier == "STRONG" and decision_trigger == "T+2 (early)" and T2_EARLY_STRONG_DISABLED)
            # retro_t2_defer (backfilled 2026-07-04 by tick replay): TRUE
            # means this row really fired on the T+2-exact early path
            # before the backward filter shipped, today's filter would
            # have deferred it, AND it no longer tier-qualified at T+3 —
            # i.e. under today's logic it would never have been staked.
            # Rows since the filter shipped are NULL (live path already
            # enforces the filter, nothing to recompute).
            and not retro_t2_defer
        )
        gate_on = True
        kelly_ok = True
        n_prior = 0
        rolling_acc = None
        backtest_acc = BACKTEST_ACC_BY_TIER.get(tier)
        if tier in AUTO_GATE_TIERS:
            hist = tier_history.get(tier, [])
            cutoff = anchor - timedelta(hours=AUTO_GATE_LOOKBACK_HOURS)
            hist_recent = [c for (t, c) in hist if t >= cutoff]
            n_prior = len(hist_recent)
            if n_prior >= AUTO_GATE_MIN_N and backtest_acc is not None:
                rolling_acc = sum(hist_recent) / n_prior
                was_enabled = gate_state[tier]
                if was_enabled and rolling_acc < backtest_acc - AUTO_GATE_DISABLE_MARGIN:
                    gate_state[tier] = False
                elif not was_enabled and rolling_acc >= required_edge_acc(backtest_acc):
                    gate_state[tier] = True
                # Mirrors kelly_fraction_for()'s own zero-condition: even
                # while the auto-gate stays "on" (its looser -10pp disable
                # margin), Kelly sizing separately requires clearing the
                # tighter required_edge_acc bar before producing ANY
                # nonzero stake. Without this the simulation over-counted
                # windows real should_stake()/Kelly would have zeroed out
                # (found 2026-07-03: a window with real stake_ok=False,
                # Kelly-zeroed, was incorrectly retro-flagged as staked —
                # the auto-gate and Kelly zero-point are two different
                # thresholds and both must be checked).
                # WEAK-EDGE is flat-sized (Kelly-exempt, see stake_fraction)
                # so the Kelly zero-point doesn't apply to it — only the
                # auto-gate hysteresis above does.
                if rolling_acc < required_edge_acc(backtest_acc) and tier != WEAK_EDGE_LABEL:
                    kelly_ok = False
            gate_on = gate_state[tier]
        retro_stake_ok = non_gate_ok and gate_on and kelly_ok
        # Everything below this line is OUTPUT (stake_map/pct_map/counts) —
        # gated on cutoff_ts so the extra warm-up buffer rows fetched above
        # only feed gate_state/tier_history, never the reported stats.
        in_display_window = cutoff_ts is None or anchor >= cutoff_ts
        if in_display_window:
            stake_map[anchor.isoformat()] = retro_stake_ok

        if retro_stake_ok and in_display_window:
            # Mirror stake_fraction() with the walk's own rolling state (see
            # docstring: display-only pct for retro-staked rows).
            if tier == WEAK_EDGE_LABEL:
                pct = WEAK_EDGE_STAKE_PCT
            else:
                # Computed unconditionally (2026-07-05 fix — the Kelly branch
                # below never set this, causing an UnboundLocalError the
                # first time a row's rolling sample actually cleared
                # AUTO_GATE_MIN_N within the extended-buffer walk) since the
                # payout-cap mirror further down needs it regardless of
                # which sizing path (Kelly vs ladder) was taken.
                eff_bps = abs(decision_bps) if decision_bps is not None else None
                tier_floor = TIER_MIN_FLOOR.get(tier)
                if tier_floor is not None and (eff_bps is None or eff_bps < tier_floor):
                    eff_bps = tier_floor  # dwell rows: see TIER_MIN_FLOOR
                f_kelly = None
                if (n_prior >= AUTO_GATE_MIN_N and backtest_acc is not None
                        and rolling_acc is not None
                        and rolling_acc >= required_edge_acc(backtest_acc)):
                    b = assumed_payout_for(tier)
                    if b:
                        f_kelly = max(0.0, rolling_acc - (1.0 - rolling_acc) / b)
                if f_kelly is not None:
                    pct = min(KELLY_MAX_STAKE_PCT, KELLY_FRACTIONAL_MULT * f_kelly)
                else:
                    pct = stake_pct_for(eff_bps) if eff_bps is not None else 0.0
                ny_small = ny_tight and decision_bps is not None and abs(decision_bps) < 20.0
                if pct > 0 and adx_confirmed(adx_val) and not ny_small:
                    if f_kelly is not None:
                        pct = min(KELLY_MAX_STAKE_PCT, pct * 1.3)
                    elif pct in STAKE_PCT_LADDER:
                        idx = STAKE_PCT_LADDER.index(pct)
                        if idx < len(STAKE_PCT_LADDER) - 1:
                            pct = STAKE_PCT_LADDER[idx + 1]
                if pct > 0 and ny_small:
                    if f_kelly is not None:
                        pct = pct * 0.7
                    elif pct in STAKE_PCT_LADDER:
                        pct = STAKE_PCT_LADDER[max(STAKE_PCT_LADDER.index(pct) - 1, 0)]
                # Payout cap mirror (2026-07-05) — see stake_fraction()'s
                # docstring for the validated fix; must mirror it here too so
                # the dashboard's retro-staked pct doesn't drift from what
                # the live gate would actually size (project memory: "retro
                # twins must mirror live filters exactly").
                if pct > 0 and eff_bps is not None and decided_at is not None:
                    expiry_ts = expired_at or (anchor + timedelta(minutes=5))
                    secs_left = (expiry_ts - decided_at).total_seconds()
                    if secs_left > 0:
                        flat_b = assumed_payout_for(tier)
                        real_b = assumed_payout_for_time(eff_bps, secs_left)
                        if flat_b and real_b is not None and real_b < flat_b:
                            pct = pct * (real_b / flat_b)
                # Final multiplier mirror (2026-07-05) — see
                # STAKE_SIZE_MULTIPLIER's comment in stake_fraction().
                pct = pct * STAKE_SIZE_MULTIPLIER
            if pct > 0:
                pct_map[anchor.isoformat()] = round(pct, 4)
        # Stats counts (staked/correct/wrong/ties -> the scoreboard's
        # "Staked windows"/accuracy headline) require really_staked_live on
        # top of retro_stake_ok — see that flag's comment. stake_map/pct_map
        # above are untouched (still just retro_stake_ok), so the table's
        # "Staked only" filter and "~$" retro-sizing column keep showing
        # every row that passes today's rules, whether or not it actually
        # had money on it at the time.
        if retro_stake_ok and really_staked_live and in_display_window:
            n_staked += 1
            if correct == "CORRECT":
                n_correct += 1
            elif correct == "WRONG":
                n_wrong += 1
            elif correct == "TIE":
                # TIE = stake returned, no win, no loss — counted in
                # n_staked (a stake WAS placed) but excluded from the
                # accuracy denominator below ("tie is a pass").
                n_tie += 1
        elif in_display_window:
            no_stake_n += 1
            if correct == "CORRECT":
                no_stake_correct += 1
            elif correct == "WRONG":
                no_stake_wrong += 1

        if is_decided:
            # Mirror get_rolling_tier_accuracy()'s ELIGIBLE filter exactly —
            # the live gate/Kelly judge each tier only on windows that pass
            # the non-gate checks (ADX-unfavourable, LOW_ADX_ONLY_TIERS,
            # NY-open small-bps by bps_T3). Appending every decided window
            # here (the original code) gave the retro walk a different —
            # larger, dirtier — rolling population than the live pipeline
            # uses: found 2026-07-04 when four really-staked, really-won
            # STRONG windows (16:05-17:25 UTC) were retro-flagged NO-STAKE
            # (unfiltered n=11, acc 90.9% < required bar) while the live
            # monitor had correctly staked them (eligible n=6 < MIN_N ->
            # static-ladder fallback, no bar applied). The dashboard's
            # Recent card was summing the 16:00 STRONG loss but none of
            # those wins — showing -$2.92 on a genuinely positive 12h.
            adx_max = LOW_ADX_ONLY_TIERS.get(tier)
            hist_eligible = (
                (tier in ADX_UNFAVOURABLE_EXEMPT_TIERS or not adx_unfav)
                and (adx_max is None or (adx_val is not None and adx_val < adx_max))
                and not (bps_t3 is not None and abs(bps_t3) < NY_OPEN_MIN_STAKE_BPS and ny_tight)
            )
            if hist_eligible:
                tier_history.setdefault(tier, []).append((anchor, correct == "CORRECT"))

    decided = n_correct + n_wrong
    no_stake_decided = no_stake_correct + no_stake_wrong
    stats = {
        "staked": n_staked,
        "correct": n_correct,
        "wrong": n_wrong,
        "ties": n_tie,
        "accuracy_pct": round(100.0 * n_correct / decided, 1) if decided else None,
        "skipped": no_stake_n,
        "no_stake_correct": no_stake_correct,
        "no_stake_accuracy_pct": round(100.0 * no_stake_correct / no_stake_decided, 1) if no_stake_decided else None,
    }
    return (stats, stake_map, pct_map) if return_map else stats


def get_price():
    r = requests.get(f"{BASE}/api/v3/ticker/price", params={"symbol": SYMBOL}, timeout=5)
    r.raise_for_status()
    return float(r.json()["price"])


def get_klines_1m(limit=100):
    r = requests.get(f"{BASE}/api/v3/klines",
                      params={"symbol": SYMBOL, "interval": "1m", "limit": limit}, timeout=5)
    r.raise_for_status()
    rows = r.json()
    df = pd.DataFrame(rows, columns=[
        "open_time", "open", "high", "low", "close", "volume", "close_time",
        "qav", "trades", "tbav", "tqav", "ignore",
    ])
    for c in ["open", "high", "low", "close"]:
        df[c] = df[c].astype(float)
    return df


def get_settlement_price(expiry_time, tick_buf, fallback_price):
    """Authoritative price at a window's expiry — the LAST tick strictly
    BEFORE expiry_time, not the first tick at/after it (added 2026-07-05,
    "data stream mismatch" — a window recorded WRONG on a +0.015bps outcome,
    essentially a dead tie; the owner found the real Binance app settled it WON
    and hypothesized "1 tick before the close". Confirmed against the raw
    ticks: the last one before the boundary (62718.83) was BELOW entry —
    a win, matching Binance exactly — while the first one at/after (62719.99,
    what we used) was marginally above. "First tick at/after expiry" is
    actually the OPENING tick of the NEXT 5-min window, not the closing tick
    of this one — a real off-by-one, not just poll-timing noise. Checked all
    7 other losers in the same population: none flip under this correction,
    since those are real multi-bps reversals, not near-ties — so this only
    changes razor-thin outcomes, which is exactly where it should matter.
    An earlier version of this function used Binance's own kline close
    instead, but that returned 62720.00 for this same window — STILL wrong —
    so kline-close is not a reliable stand-in here; the tick buffer is.
    Falls back to fallback_price if no tick before expiry_time is found in
    tick_buf — must never block or fail the expiry determination."""
    best_ts = None
    best_price = None
    for entry in reversed(tick_buf):
        try:
            ts = datetime.fromisoformat(entry["t"])
            p = entry["p"]
        except (KeyError, ValueError, TypeError):
            continue
        if ts < expiry_time:
            best_ts, best_price = ts, p
            break
    return best_price if best_price is not None else fallback_price


def get_adx_full(df, period=14):
    """ADX plus its two directional components (DI+/DI-) from the same
    df.ta.adx() call — used for the live ADX/DI+/DI- readout on the
    dashboard's candle chart, not just the bare ADX gating value."""
    adx_df = df.ta.adx(length=period)
    row = adx_df.iloc[-1]
    adx = row.get(f"ADX_{period}")
    di_plus = row.get(f"DMP_{period}")
    di_minus = row.get(f"DMN_{period}")
    return (
        float(adx) if pd.notna(adx) else None,
        float(di_plus) if pd.notna(di_plus) else None,
        float(di_minus) if pd.notna(di_minus) else None,
    )


def get_atr_bps(df, period=14):
    """Wilder's ATR(period) expressed as bps of the last close — a move-size-
    relative-to-recent-volatility measure, not an absolute price move. See
    ATR_RATIO_MIN's comment for the study behind why this matters more than
    ADX for pruning STRONG+/MODERATE-STRONG losers."""
    atr_df = df.ta.atr(length=period)
    val = atr_df.iloc[-1]
    last_close = float(df["close"].iloc[-1])
    if pd.isna(val) or not last_close:
        return None
    return float(val) / last_close * 10000.0


def get_hourly_range_24h():
    """Last 24 fully-CLOSED 1-hour candles (drops the still-forming current
    hour the API includes as its last row) — feeds the dashboard's rolling
    24h price-range list."""
    r = requests.get(f"{BASE}/api/v3/klines",
                      params={"symbol": SYMBOL, "interval": "1h", "limit": 25}, timeout=5)
    r.raise_for_status()
    rows = r.json()
    df = pd.DataFrame(rows, columns=[
        "open_time", "open", "high", "low", "close", "volume", "close_time",
        "qav", "trades", "tbav", "tqav", "ignore",
    ])
    for c in ["open", "high", "low", "close"]:
        df[c] = df[c].astype(float)
    closed = df.iloc[:-1].tail(24)
    return [
        {"t": int(r.open_time), "o": r.open, "h": r.high, "l": r.low, "c": r.close}
        for r in closed.itertuples()
    ]


def floor_to_5min(t):
    return t.replace(second=0, microsecond=0, minute=(t.minute // 5) * 5)


def write_state(windows, ticks, candles_1m, hourly_range_24h, cumulative=None, auto_gate_status=None,
                recent_gated_stats=None, retro_stake_map=None, late_leg_cumulative=None,
                value_leg_cumulative=None, fade_leg_cumulative=None, retro_stake_pct_map=None,
                recent_gated_stats_24h=None, retro_stake_map_24h=None, retro_stake_pct_map_24h=None,
                retro_stake_map_all=None, retro_stake_pct_map_all=None,
                current_adx=None, current_atr_bps=None, current_di_plus=None, current_di_minus=None):
    rows = []
    for anchor in sorted(windows.keys(), reverse=True)[:MAX_HISTORY]:
        w = windows[anchor]
        rows.append({
            "anchor": anchor.isoformat(),
            "t0_lean_side": w.get("t0_lean_side"),
            "price_T": w.get("price_T"),
            "price_T2": w.get("price_T2"),
            "bps_T2": w.get("bps_T2"),
            "price_T3": w.get("price_T3"),
            "bps_T3": w.get("bps_T3"),
            "tier": w.get("tier"),
            "tier_acc": w.get("tier_acc"),
            "side": w.get("side"),
            "price_T5": w.get("price_T5"),
            "bps_T5": w.get("bps_T5"),
            "actual_dir": w.get("actual_dir"),
            "correct": w.get("correct"),
            "status": w.get("status"),
            "adx": w.get("adx"),
            "adx_confirmed": w.get("adx_confirmed"),
            "adx_unfavourable": w.get("adx_unfavourable"),
            "atr_bps": w.get("atr_bps"),
            "stake_ok": w.get("stake_ok"),
            "stake_usdt": w.get("stake_usdt"),
            "stake_pct": w.get("stake_pct"),
            "assumed_payout": w.get("assumed_payout"),
            "est_return_usdt": w.get("est_return_usdt"),
            "decided_early": w.get("decided_early", False),
            "decision_trigger": w.get("decision_trigger"),
            "weak_edge": w.get("weak_edge", False),
            "ny_open_tightened": w.get("ny_open_tightened"),
            "no_stake_reason": w.get("no_stake_reason"),
            "liq_imbalance_300s": w.get("liq_imbalance_300s"),
            "liq_imbalance_900s": w.get("liq_imbalance_900s"),
            "late_leg_fired": w.get("late_leg_fired", False),
            "late_side": w.get("late_side"),
            "late_stake_ok": w.get("late_stake_ok"),
            "late_stake_usdt": w.get("late_stake_usdt"),
            "late_stake_pct": w.get("late_stake_pct"),
            "late_assumed_payout": w.get("late_assumed_payout"),
            "late_seconds_before_expiry": w.get("late_seconds_before_expiry"),
            "late_bps_at_fire": w.get("late_bps_at_fire"),
            "late_correct": w.get("late_correct"),
            "late_est_return_usdt": w.get("late_est_return_usdt"),
            "late_no_stake_reason": w.get("late_no_stake_reason"),
            "value_leg_fired": w.get("value_leg_fired", False),
            "value_side": w.get("value_side"),
            "value_stake_ok": w.get("value_stake_ok"),
            "value_stake_usdt": w.get("value_stake_usdt"),
            "value_stake_pct": w.get("value_stake_pct"),
            "value_assumed_payout": w.get("value_assumed_payout"),
            "value_seconds_before_expiry": w.get("value_seconds_before_expiry"),
            "value_bps_at_fire": w.get("value_bps_at_fire"),
            "value_correct": w.get("value_correct"),
            "value_est_return_usdt": w.get("value_est_return_usdt"),
            "fade_leg_fired": w.get("fade_leg_fired", False),
            "fade_side": w.get("fade_side"),
            "fade_stake_ok": w.get("fade_stake_ok"),
            "fade_stake_usdt": w.get("fade_stake_usdt"),
            "fade_stake_pct": w.get("fade_stake_pct"),
            "fade_entry_ask": w.get("fade_entry_ask"),
            "fade_assumed_payout": w.get("fade_assumed_payout"),
            "fade_fill_prob": w.get("fade_fill_prob"),
            "fade_seconds_before_expiry": w.get("fade_seconds_before_expiry"),
            "fade_bps_at_fire": w.get("fade_bps_at_fire"),
            "fade_correct": w.get("fade_correct"),
            "fade_est_return_usdt": w.get("fade_est_return_usdt"),
            "fade_no_stake_reason": w.get("fade_no_stake_reason"),
            "live_bps_now": w.get("live_bps_now"),
            "live_seconds_remaining": w.get("live_seconds_remaining"),
            "live_side_now": w.get("live_side_now"),
            "live_payout_now": w.get("live_payout_now"),
            "early_exit_alerted": w.get("early_exit_alerted", False),
            "early_exit_bps": w.get("early_exit_bps"),
            "early_exit_ts": w.get("early_exit_ts"),
            "fast_move_alerted_higher": w.get("fast_move_alerted_higher", False),
            "fast_move_alerted_lower": w.get("fast_move_alerted_lower", False),
            "fast_move_alert_usd": w.get("fast_move_alert_usd"),
            "fast_move_alert_ts": w.get("fast_move_alert_ts"),
            "fast_move_early_exit_alerted_higher": w.get("fast_move_early_exit_alerted_higher", False),
            "fast_move_early_exit_alerted_lower": w.get("fast_move_early_exit_alerted_lower", False),
        })
    payload = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "symbol": SYMBOL,
        "windows": rows,
        "ticks": list(ticks),
        "candles_1m": candles_1m,
        "hourly_range_24h": hourly_range_24h,
        "cumulative": cumulative,
        "auto_gate_status": auto_gate_status,
        "recent_gated_stats": recent_gated_stats,
        "retro_stake_map": retro_stake_map,
        "retro_stake_pct_map": retro_stake_pct_map,
        # 24h counterparts (added 2026-07-06 for the dashboard's 12h/24h/All
        # range dropdown) — same compute_retroactive_gated_stats() walk, just
        # hours=24 instead of the fixed AUTO_GATE_LOOKBACK_HOURS=12 above.
        "recent_gated_stats_24h": recent_gated_stats_24h,
        "retro_stake_map_24h": retro_stake_map_24h,
        "retro_stake_pct_map_24h": retro_stake_pct_map_24h,
        # All-time map (added 2026-07-06, same day as the 24h one above, once
        # it became clear "All" needed it too): get_cumulative_stats() already
        # computes this internally (hours=None) for the headline numbers —
        # exposing it here is free (no extra walk). Without it, the table's
        # per-row Stake column fell back to each row's own frozen historical
        # stake_ok for anything beyond the 12h/24h maps' coverage, so old
        # T+2-early/Kelly-zeroed losers kept showing as "staked" in the "All"
        # table view even after the headline correctly stopped counting them
        # — the exact "table shows 4 losers, summary says 2" mismatch the owner
        # caught.
        "retro_stake_map_all": retro_stake_map_all,
        "retro_stake_pct_map_all": retro_stake_pct_map_all,
        "current_adx": current_adx,
        "current_atr_bps": current_atr_bps,
        "current_di_plus": current_di_plus,
        "current_di_minus": current_di_minus,
        "late_leg_cumulative": late_leg_cumulative,
        "late_trigger_enabled": LATE_TRIGGER_ENABLED,
        "late_paper_mode": LATE_PAPER_MODE,
        "late_kill_bar": {
            "killed": _late_kill_state["killed"],
            "n": _late_kill_state["n"],
            "acc": _late_kill_state["acc"],
            "min_fires": LATE_KILL_MIN_FIRES,
            "kill_acc": LATE_KILL_ACC,
        },
        "value_leg_cumulative": value_leg_cumulative,
        "value_trigger_enabled": VALUE_TRIGGER_ENABLED,
        "fade_leg_cumulative": fade_leg_cumulative,
        "fade_trigger_enabled": FADE_TRIGGER_ENABLED,
        "continuation_stake_enabled": CONTINUATION_STAKE_ENABLED,
        "fade_band": {"ask_min": FADE_CHEAP_ASK_MIN, "ask_max": FADE_CHEAP_ASK_MAX,
                      "elapsed_min_s": FADE_ELAPSED_MIN_SEC, "stake_pct": FADE_STAKE_PCT},
        "weak_edge_enabled": WEAK_EDGE_ENABLED,
        "weak_edge_kill_bar": {
            "killed": _weak_edge_kill_state["killed"],
            "n": _weak_edge_kill_state["n"],
            "acc": _weak_edge_kill_state["acc"],
            "min_fires": WEAK_EDGE_KILL_MIN_FIRES,
            "kill_acc": WEAK_EDGE_KILL_ACC,
        },
    }
    d = os.path.dirname(STATE_PATH)
    fd, tmp_path = tempfile.mkstemp(dir=d, prefix=".binance_state_", suffix=".json")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(payload, f, indent=2)
        os.replace(tmp_path, STATE_PATH)
    except Exception:
        try:
            os.remove(tmp_path)
        except OSError:
            pass
        raise


def t2_backward_filter(w_anchor, price_T, bps_t2, tick_buf, now):
    """Backward-looking confirmation for the T+2-exact early path (see the
    T2_FILTER_ENABLED constant's comment for the study behind it). Walks the
    in-memory tick buffer over [w_anchor, now] — ticks already observed, so
    this adds no delay — and answers two questions about the move that just
    tier-qualified at T+2:
      1. Has it HELD its tier floor (same sign) continuously for at least
         T2_BACKWARD_DWELL_SECONDS leading into T+2? (Both real losers had
         only ~17-19s; a fresh spike that landed on the snapshot.)
      2. Is it still within T2_MAX_RETRACE_BPS of its favorable extreme?
         (A big giveback means we'd be firing into a fading move.)
    Returns (ok, reason) — reason is a short human string when deferring."""
    if abs(bps_t2) < T2_FILTER_MIN_BPS:
        return True, None  # MODERATE tiers: outside the validated scope
    side = 1 if bps_t2 > 0 else -1
    floor = T2_FILTER_MIN_BPS
    tail = 0.0
    ext = abs(bps_t2)
    newer_ts = None
    tail_unbroken = True  # walking newest -> oldest
    for entry in reversed(tick_buf):
        try:
            ts = datetime.fromisoformat(entry["t"])
        except (KeyError, ValueError):
            continue
        if ts > now:
            continue
        if ts < w_anchor:
            break
        fav = (entry["p"] - price_T) / price_T * 10000.0 * side
        if fav > ext:
            ext = fav
        if tail_unbroken:
            if fav >= floor:
                if newer_ts is not None:
                    tail += (newer_ts - ts).total_seconds()
                newer_ts = ts
            else:
                tail_unbroken = False  # keep scanning only for the extreme
    if tail < T2_BACKWARD_DWELL_SECONDS:
        return False, (f"floor {floor:g}bps held only {tail:.0f}s pre-T+2 "
                       f"(<{T2_BACKWARD_DWELL_SECONDS:.0f}s)")
    retrace = ext - abs(bps_t2)
    if retrace > T2_MAX_RETRACE_BPS:
        return False, (f"retraced {retrace:.1f}bps off the {ext:.1f}bps extreme "
                       f"(>{T2_MAX_RETRACE_BPS:g}bps)")
    return True, None


def t3_backward_filter(price_T, bps_t2, decision_bps, tick_buf, t2_time, now):
    """Backward-looking confirmation for T+3/dwell decisions on STRONG+/VERY
    STRONG/EXTREME — see T3_BACKWARD_FILTER_TIERS/T3_MAX_RETRACE_BPS's
    comment for the 18-month study behind this. Flags a decision as a
    fading move if either: (1) the sign flipped vs the T+2 reading, or
    (2) the move shrank from T+2 to now AND has retraced more than
    T3_MAX_RETRACE_BPS off its own favorable extreme within [T+2, now].
    Returns (ok, reason) — reason is a short human string when blocking."""
    if bps_t2 is None:
        return True, None
    sign_flip = (bps_t2 > 0) != (decision_bps > 0)
    if sign_flip:
        return False, f"sign flipped vs T+2 ({bps_t2:+.1f}bps -> {decision_bps:+.1f}bps)"
    grew = abs(decision_bps) >= abs(bps_t2)
    if grew:
        return True, None
    side = 1 if decision_bps > 0 else -1
    ext = abs(decision_bps)
    for entry in reversed(tick_buf):
        try:
            ts = datetime.fromisoformat(entry["t"])
        except (KeyError, ValueError):
            continue
        if ts > now:
            continue
        if ts < t2_time:
            break
        fav = (entry["p"] - price_T) / price_T * 10000.0 * side
        if fav > ext:
            ext = fav
    retrace = ext - abs(decision_bps)
    if retrace > T3_MAX_RETRACE_BPS:
        return False, (f"shrank + retraced {retrace:.1f}bps off the {ext:.1f}bps "
                       f"T+2-T+3 extreme (>{T3_MAX_RETRACE_BPS:g}bps)")
    return True, None


def make_stake_decision(w, w_anchor, now, price, price_T, decision_bps, current_adx, expiry_time,
                         early, trigger_label=None, ticks=None, current_atr_bps=None):
    """Runs the tier/ADX/gate/Kelly decision and fires the alert — shared by
    the T+2-exact early path, the mid-window dwell path, and the normal T+3
    path (added 2026-07-03, see project memory 'Early T+2 decision path' and
    'Mid-window dwell trigger'). `decision_bps` is bps_T2/current-tick-bps for
    the two early paths, bps_T3 otherwise; `early`/`trigger_label` control
    logging/labeling only, the decision logic itself is identical either way.

    Why the early paths exist: checked whether windows that already cross a
    tier's floor by T+2 are as reliable as waiting the extra minute for T+3.
    They are (within ~0-2.5pp) — but the extra minute lets the Binance order
    book reprice against the now-obvious move, which is the "slippage" the owner
    noticed on a STRONG window where 12 of its 15.3bps had already happened
    by T+2. For windows that only cross the threshold between T+2 and T+3,
    a stable-for-DWELL_SECONDS crossing mid-window gets similar treatment
    (backtested comparably safe, not just waiting the full extra minute)."""
    if trigger_label is None:
        trigger_label = "T+2 (early)" if early else "T+3"
    abs_bps = abs(decision_bps)
    label, acc = tier_for(abs_bps)
    # WEAK-EDGE rebrand (2026-07-04, opt-in high-risk carve-out — see the
    # WEAK_EDGE_* constants block): a WEAK window in the validated 3.5-4.0bps
    # sub-band during a quiet tape (ADX<15 or RANGE regime) becomes its own
    # stake-eligible pseudo-tier. Everything downstream (should_stake,
    # auto-gate, rolling accuracy, dashboard badge) keys off the new label.
    w["weak_edge"] = False
    if (WEAK_EDGE_ENABLED and label == "WEAK"
            and WEAK_EDGE_BAND_LO <= abs_bps < WEAK_EDGE_BAND_HI
            and weak_edge_market_ok(current_adx)
            and not weak_edge_killed(now)):
        label = WEAK_EDGE_LABEL
        acc = WEAK_EDGE_BACKTEST_ACC
        w["weak_edge"] = True
    in_ny_open = ny_open_tightening(now)
    if in_ny_open and acc is not None:
        acc = ny_open_tier_acc(abs_bps, acc)
    w["tier"] = label or "NONE"
    w["tier_acc"] = acc
    # Time-aware payout (2026-07-03): price the decision by how much time is
    # actually left when it fires (T+2-early=180s, dwell=120-180s, T+3=120s),
    # not the flat tier number that assumed every decision fires at T+3.
    # Falls back to the flat number if the time input is degenerate.
    seconds_remaining = (expiry_time - now).total_seconds()
    w["assumed_payout"] = (
        assumed_payout_for_time(abs_bps, seconds_remaining, fallback_label=label)
        if label is not None else None
    )
    w["side"] = "HIGHER" if decision_bps > 0 else ("LOWER" if decision_bps < 0 else None)
    w["status"] = "DECIDED"
    w["decided_early"] = early
    w["decision_trigger"] = trigger_label
    w["adx"] = current_adx
    w["adx_confirmed"] = adx_confirmed(current_adx)
    w["adx_unfavourable"] = adx_unfavourable(current_adx)
    w["ny_open_tightened"] = in_ny_open
    w["stake_ok"] = should_stake(label, current_adx, abs_bps, now) if label is not None else False
    w["stake_pct"] = (
        stake_fraction(label, current_adx, abs_bps, now, seconds_remaining)
        if label is not None else 0.0
    )
    # Kelly sizing can legitimately size a tier down to zero even when
    # should_stake() otherwise says yes (gate/ADX/NY checks all pass, but the
    # tier isn't currently beating its own backtest-implied breakeven) —
    # treat that the same as any other no-stake case downstream (cumulative
    # stats, alerts, dashboard), not as a "$0 stake".
    kelly_zeroed = bool(w["stake_ok"]) and w["stake_pct"] <= 0.0
    if kelly_zeroed:
        w["stake_ok"] = False
    # T+2(early)-STRONG disable ("analyse the losers and
    # gate them, we need 96% accuracy to be profitable"). Loser analysis on
    # all STRONG stakes: the T+2-exact early trigger runs 87.9% live
    # (29/33correct pre-t2_backward_filter-scope, 11/13 post-shipping the
    # filter on 2026-07-04 — the filter doesn't fix this path) vs 93.2% for
    # the T+3/dwell paths (82/88) — at or below STRONG's own flat-assumed
    # breakeven (~89-90%), and certainly below breakeven on its REAL payout
    # (see the real-payout floor in stake_fraction()). t2_backward_filter
    # already runs upstream of this call and still lets these through, so
    # this is a separate, harder stop: STRONG no longer stakes on this
    # specific trigger at all, only alerts. T+2-early stays live for other
    # tiers (VERY STRONG/EXTREME too thin-sampled to judge; MODERATE/
    # MODERATE-STRONG don't reach this path staked at all).
    t2_early_reason = None
    if (T2_EARLY_STRONG_DISABLED and w["stake_ok"] and label == "STRONG"
            and trigger_label == "T+2 (early)"):
        t2_early_reason = ("T+2(early) STRONG disabled: 87.9% live vs 93.2% "
                            "T+3/dwell, at/below breakeven")
        w["stake_ok"] = False
        w["stake_pct"] = 0.0
    # T+3/dwell backward filter (2026-07-05) — see T3_BACKWARD_FILTER_TIERS'
    # comment. Only applies to the dwell/T+3 checkpoints (the T+2-exact path
    # has its own t2_backward_filter, run upstream before this function is
    # even called) and only to the tiers the 18-month study covered.
    t3_filter_reason = None
    if (T3_BACKWARD_FILTER_ENABLED and w["stake_ok"] and label in T3_BACKWARD_FILTER_TIERS
            and trigger_label != "T+2 (early)" and w.get("bps_T2") is not None):
        fire_ok, t3_filter_reason = t3_backward_filter(
            price_T, w["bps_T2"], decision_bps, ticks or [],
            w_anchor + timedelta(minutes=EARLY_OFFSET_MIN), now)
        if not fire_ok:
            w["stake_ok"] = False
            w["stake_pct"] = 0.0
    # ATR-ratio filter (2026-07-05) — see ATR_RATIO_MIN's comment. Same
    # dwell/T+3-only scope as the backward filter above; independent check
    # (a move can pass the backward filter but still be small relative to
    # current volatility, or vice versa).
    w["atr_bps"] = current_atr_bps
    atr_filter_reason = None
    if (ATR_RATIO_FILTER_ENABLED and w["stake_ok"] and label in ATR_RATIO_FILTER_TIERS
            and trigger_label != "T+2 (early)" and current_atr_bps):
        bps_over_atr = abs_bps / current_atr_bps
        if bps_over_atr < ATR_RATIO_MIN:
            atr_filter_reason = (f"move only {bps_over_atr:.1f}x current ATR "
                                  f"(<{ATR_RATIO_MIN:g}x)")
            w["stake_ok"] = False
            w["stake_pct"] = 0.0
    w["stake_usdt"] = round(BANKROLL_USDT * w["stake_pct"], 2)
    ask_cap_reason = manual_ask_block_reason(w.get("assumed_payout"))
    if w["stake_ok"] and ask_cap_reason:
        w["stake_ok"] = False
        w["stake_pct"] = 0.0
        w["stake_usdt"] = 0.0
    # Minimum expected-return floor (added 2026-07-05, the owner: "if we have
    # only 2 or 3 dollar return no point sending the signal") — checked
    # LAST, after all other sizing (Kelly, payout cap, backward/ATR
    # filters), since each of those can legitimately shrink a stake for its
    # own reason; this just catches whatever's left once the $ payout on
    # the CONFIGURED bankroll (BANKROLL_USDT — Telegram's reference capital)
    # is too thin to bother acting on. Full no-stake treatment, same as any
    # other blocked case — not just a silenced alert.
    thin_return_reason = None
    if w["stake_ok"] and w.get("assumed_payout") is not None:
        expected_to_win = w["stake_usdt"] * w["assumed_payout"]
        if expected_to_win < min_expected_return():
            thin_return_reason = (f"expected return ${expected_to_win:.2f} "
                                   f"< ${min_expected_return():g} minimum")
            w["stake_ok"] = False
            w["stake_pct"] = 0.0
            w["stake_usdt"] = 0.0
    w["no_stake_reason"] = (
        "Kelly-implied edge <= 0 (rolling live accuracy at/below this tier's "
        "own backtest-implied breakeven)" if kelly_zeroed else
        (t2_early_reason if t2_early_reason else
         (f"T+3 backward filter: {t3_filter_reason}" if t3_filter_reason else
          (f"ATR-ratio filter: {atr_filter_reason}" if atr_filter_reason else
           (ask_cap_reason if ask_cap_reason else
            (f"thin return: {thin_return_reason}" if thin_return_reason else
            (no_stake_reason(label, current_adx, abs_bps, now)
             if label is not None and not w["stake_ok"] else None))))))
    )

    try:
        liq = liq_imbalance_from_pg(now, (300, 900))
        w["liq_imbalance_300s"] = liq.get(300, 0.0)
        w["liq_imbalance_900s"] = liq.get(900, 0.0)
    except Exception as e:
        print(f"[{now.isoformat()}] liq snapshot failed: {e}", flush=True)

    persist_window_decision(w_anchor, w)

    if label is not None and abs_bps >= ALERT_MIN_BPS:
        seconds_left = (expiry_time - now).total_seconds()
        adx_str = f"{current_adx:.1f}" if current_adx is not None else "n/a"
        confirm_str = " [ADX-CONFIRMED]" if w["adx_confirmed"] else ""
        ny_str = " [NY-OPEN TIGHTENED]" if w.get("ny_open_tightened") else ""
        early_str = f" [EARLY @{trigger_label}]" if early else ""
        if w["stake_ok"]:
            stake_amt = w["stake_usdt"]
            # Assumed total payout (stake + assumed profit) at this tier's
            # backtest-accuracy-implied payout — NOT the real Binance fill
            # (no API to read the live order price). Meant to be cross-
            # checked against the app's own "To Win" figure before
            # confirming: if the app's real number is meaningfully lower,
            # the live price has already repriced past what this tier's
            # edge justifies, and the trade should be skipped even though
            # this alert says stake (see project memory 2026-07-03, a real
            # $450 stake showing only a $4.40/0.98% payout on-screen).
            expected_to_win = stake_amt * (1 + w["assumed_payout"]) if w.get("assumed_payout") is not None else None
            to_win_str = f"${expected_to_win:.2f}" if expected_to_win is not None else "n/a"
            risk_line = (
                "⚠️ <b>HIGH RISK — experimental WEAK-EDGE filter</b> (3.5-4bps band, "
                "quiet-market only; 81% on 30 days of backtest, thin live sample; "
                "a loss costs 100% of stake)\n"
                if label == WEAK_EDGE_LABEL else ""
            )
            msg = (
                f"\n*** ALERT [{label}, ~{acc*100:.1f}% backtest accuracy, decided at {trigger_label}] "
                f"window={w_anchor.strftime('%H:%M:%S')} -> STAKE {w['side']} "
                f"${stake_amt:.0f} USDT (expected To Win: {to_win_str}) "
                f"(price_T={price_T:.2f} -> now={price:.2f}, move={decision_bps:+.1f}bps, "
                f"ADX={adx_str}{confirm_str}{ny_str}{early_str}) "
                f"expiry in ~{seconds_left:.0f}s ***\n"
            )
            print(msg, flush=True)
            stake_pct = w["stake_pct"]
            assumed_payout = w.get("assumed_payout")
            side = w["side"]

            def _build_stake_msg(capital):
                # Recomputed per-recipient (2026-07-06) so every linked viewer's
                # own Telegram shows a stake sized to THEIR capital, not
                # whoever TELEGRAM_BANKROLL_USERNAME happens to be — same
                # formula the dashboard/BANKROLL_USDT refresh already use.
                amt = round(stake_pct * capital, 2)
                win = amt * (1 + assumed_payout) if assumed_payout is not None else None
                win_str = f"${win:.2f}" if win is not None else "n/a"
                return (
                    f"🟢 <b>STAKE {side} — ${amt:.0f} USDT</b> [{label}]{early_str}\n"
                    f"{risk_line}"
                    f"Expected To Win: <b>{win_str}</b> — check this against the app's own "
                    f"\"To Win\" before confirming; if the app's real number is meaningfully "
                    f"lower, skip the trade\n"
                    f"Δ from strike: <b>${abs(price - price_T):.0f}</b> — the bigger this has "
                    f"grown by the time you stake, the lower the app's real To Win\n"
                    f"Window: {ist_hm(w_anchor)}  |  Expiry in ~{seconds_left:.0f}s\n"
                    f"Move: {decision_bps:+.1f} bps  |  ADX: {adx_str}{confirm_str}{ny_str}\n"
                    f"Accuracy: ~{acc*100:.1f}%  |  Price now: {price:.2f}"
                )
            tg_personalized(_build_stake_msg)
        else:
            reason = w.get("no_stake_reason") or "unknown"
            msg = (
                f"\n*** NO STAKE [{label}, ~{acc*100:.1f}% backtest accuracy, "
                f"reason: {reason}, decided at {trigger_label}] window={w_anchor.strftime('%H:%M:%S')} "
                f"lean={w['side']} (price_T={price_T:.2f} -> now={price:.2f}, "
                f"move={decision_bps:+.1f}bps, ADX={adx_str}) expiry in ~{seconds_left:.0f}s ***\n"
            )
            # console-only — the owner asked to stop sending Telegram
            # alerts for no-stake windows (2026-07-01)
            print(msg, flush=True)
    else:
        print(f"[{now.isoformat()}] window={w_anchor.strftime('%H:%M:%S')} "
              f"decision point reached ({trigger_label}), move={decision_bps:+.1f}bps -> below alert threshold, skip",
              flush=True)


def fire_late_leg(w, w_anchor, now, price, price_T, late_bps, current_adx, expiry_time):
    """Fire the one-per-window late-window stake leg (added 2026-07-03).
    Deliberately separate from make_stake_decision(): must NOT touch
    w['status']/'tier'/'side'/'stake_ok' etc — those belong to the T+3-family
    leg and feed persist_window_decision()'s anchor-keyed upsert. Writes only
    late_* keys and persists append-only via persist_late_leg_fire().
    Reuses the general market-regime gates (adx_unfavourable, NY-open) but
    not tier_for/should_stake/kelly — those are tuned/validated at the
    tau=120s tier checkpoints, not this trigger's own backtested accuracy
    (LATE_TRIGGER_BACKTEST_ACC)."""
    abs_bps = abs(late_bps)
    side = "HIGHER" if late_bps > 0 else "LOWER"
    seconds_remaining = (expiry_time - now).total_seconds()
    adx_unfav = adx_unfavourable(current_adx)
    in_ny_open = ny_open_tightening(now)
    delta_usd = abs(price - price_T)
    stake_ok = (not adx_unfav) and not (in_ny_open and abs_bps < NY_OPEN_MIN_STAKE_BPS)
    reason = None
    if adx_unfav:
        reason = "unfavourable ADX band (20-25 or 35+)"
    elif not stake_ok:
        reason = "NY-open window (<10bps, 09:30-12:00 NY weekdays)"
    elif LATE_PAPER_MODE:
        # Records the leg for the proving sample but never stakes it. The
        # "would have staked" wording is load-bearing: the re-enable review
        # counts exactly these rows (gate-blocked fires are a different
        # population and stay excluded from the proving bar).
        stake_ok = False
        reason = "paper mode — trigger unproven (would have staked)"
    stake_pct = LATE_STAKE_PCT if stake_ok else 0.0
    stake_usdt = round(BANKROLL_USDT * stake_pct, 2)
    late_payout = assumed_payout_for_time(abs_bps, seconds_remaining)
    ask_cap_reason = manual_ask_block_reason(late_payout)
    if stake_ok and ask_cap_reason:
        stake_ok = False
        stake_pct = 0.0
        stake_usdt = 0.0
        reason = ask_cap_reason
    # Minimum expected-return floor (2026-07-05) — see MIN_EXPECTED_RETURN_USDT.
    if stake_ok and late_payout is not None:
        expected_profit = stake_usdt * late_payout
        if expected_profit < min_expected_return():
            stake_ok = False
            stake_pct = 0.0
            stake_usdt = 0.0
            reason = (f"expected return ${expected_profit:.2f} "
                      f"< ${min_expected_return():g} minimum")

    w["late_leg_fired"] = True
    w["late_side"] = side
    w["late_seconds_before_expiry"] = seconds_remaining
    w["late_price_at_fire"] = price
    w["late_bps_at_fire"] = late_bps
    w["late_assumed_payout"] = late_payout
    w["late_stake_ok"] = stake_ok
    w["late_stake_pct"] = stake_pct
    w["late_stake_usdt"] = stake_usdt
    w["late_adx"] = current_adx
    w["late_adx_unfavourable"] = adx_unfav
    w["late_no_stake_reason"] = reason

    persist_late_leg_fire(w_anchor, w)

    t3_call = w.get("side") if w.get("stake_ok") else "NO STAKE"
    adx_str = f"{current_adx:.1f}" if current_adx is not None else "n/a"
    payout = w["late_assumed_payout"]
    expected_to_win = stake_usdt * (1 + payout) if (payout is not None and stake_usdt) else None
    to_win_str = f"${expected_to_win:.2f}" if expected_to_win is not None else "n/a"
    if stake_ok:
        agree_str = "agrees with" if w.get("side") == side else "differs from"
        print(f"\n*** LATE ALERT [LATE TRIGGER, ~{LATE_TRIGGER_BACKTEST_ACC*100:.1f}% backtest accuracy, "
              f"{seconds_remaining:.0f}s to expiry] window={w_anchor.strftime('%H:%M:%S')} "
              f"-> STAKE {side} ${stake_usdt:.0f} USDT (expected To Win: {to_win_str}) "
              f"({agree_str} T+3 call: {t3_call}) move={late_bps:+.1f}bps, ADX={adx_str} ***\n",
              flush=True)
        tg_broadcast(
            f"🌙 <b>LATE STAKE {side} — ${stake_usdt:.0f} USDT</b> [LATE TRIGGER, "
            f"~{seconds_remaining:.0f}s to expiry]\n"
            f"Expected To Win: <b>{to_win_str}</b> — check against the app's own \"To Win\" "
            f"before confirming; if the app's real number is meaningfully lower, skip\n"
            f"Δ from strike: <b>${delta_usd:.0f}</b> (fired inside the "
            f"${LATE_DELTA_MIN_USD:.0f}-{LATE_DELTA_MAX_USD:.0f} value band) — if it has "
            f"blown out past ~${LATE_DELTA_MAX_USD:.0f} by the time you stake, the app's "
            f"To Win will have collapsed: skip\n"
            f"T+3 call was: {t3_call}\n"
            f"Window: {ist_hm(w_anchor)}  |  Move: {late_bps:+.1f} bps  |  ADX: {adx_str}"
        )
    else:
        # console-only for no-stake, same convention as the primary leg
        print(f"\n*** LATE NO STAKE [LATE TRIGGER, reason: {reason}] "
              f"window={w_anchor.strftime('%H:%M:%S')} lean={side} "
              f"move={late_bps:+.1f}bps, ADX={adx_str}, {seconds_remaining:.0f}s to expiry ***\n",
              flush=True)


def fire_value_leg(w, w_anchor, now, price, price_T, bps_now, payout, expiry_time):
    """Fire the one-per-window VALUE stake leg (added 2026-07-04): the
    already-confirmed side has retraced toward the strike (sign intact) far
    enough that the market re-prices it at >= VALUE_PAYOUT_TARGET. Same
    isolation rules as fire_late_leg — only touches value_* keys."""
    side = w.get("side")
    seconds_remaining = (expiry_time - now).total_seconds()
    stake_pct = VALUE_STAKE_PCT
    stake_usdt = round(BANKROLL_USDT * stake_pct, 2)
    stake_ok = True
    reason = None
    ask_cap_reason = manual_ask_block_reason(payout)
    if ask_cap_reason:
        stake_ok = False
        stake_pct = 0.0
        stake_usdt = 0.0
        reason = ask_cap_reason
    # Minimum expected-return floor (2026-07-05) — see MIN_EXPECTED_RETURN_USDT.
    # Rarely trips for VALUE given its own >=VALUE_PAYOUT_TARGET (30%) floor
    # already implies a real minimum profit on a flat stake — kept for
    # consistency across all three signal types and as a backstop if either
    # constant changes later.
    if stake_ok and payout is not None:
        expected_profit = stake_usdt * payout
        if expected_profit < min_expected_return():
            stake_ok = False
            stake_pct = 0.0
            stake_usdt = 0.0
            reason = (f"expected return ${expected_profit:.2f} "
                      f"< ${min_expected_return():g} minimum")

    w["value_leg_fired"] = True
    w["value_side"] = side
    w["value_seconds_before_expiry"] = seconds_remaining
    w["value_price_at_fire"] = price
    w["value_bps_at_fire"] = bps_now
    w["value_assumed_payout"] = payout
    w["value_stake_ok"] = stake_ok
    w["value_stake_pct"] = stake_pct
    w["value_stake_usdt"] = stake_usdt
    w["value_adx"] = w.get("adx")
    w["value_adx_unfavourable"] = w.get("adx_unfavourable")
    w["value_no_stake_reason"] = reason

    persist_extra_leg_fire(w_anchor, w, leg_type="VALUE", prefix="value")

    if not stake_ok:
        print(f"\n*** VALUE NO STAKE [{w.get('tier')}, retrace re-entry, {seconds_remaining:.0f}s to expiry] "
              f"window={w_anchor.strftime('%H:%M:%S')} reason={reason} "
              f"move={bps_now:+.1f}bps, est. payout {payout*100:.0f}% ***\n",
              flush=True)
        return

    expected_to_win = stake_usdt * (1 + payout) if payout is not None else None
    to_win_str = f"${expected_to_win:.2f}" if expected_to_win is not None else "n/a"
    print(f"\n*** VALUE ALERT [{w.get('tier')}, retrace re-entry, {seconds_remaining:.0f}s to expiry] "
          f"window={w_anchor.strftime('%H:%M:%S')} -> STAKE {side} ${stake_usdt:.0f} USDT "
          f"(expected To Win: {to_win_str}) move={bps_now:+.1f}bps, est. payout {payout*100:.0f}% ***\n",
          flush=True)
    tg_broadcast(
        f"💎 <b>VALUE STAKE {side} — ${stake_usdt:.0f} USDT</b> [{w.get('tier')}, "
        f"~{seconds_remaining:.0f}s to expiry]\n"
        f"Confirmed direction retraced toward the strike (still {side}-side) — the market "
        f"is repricing this side near even money.\n"
        f"Expected To Win: <b>{to_win_str}</b> (est. payout ~{payout*100:.0f}%) — check the app's "
        f"own \"To Win\" before confirming\n"
        f"Δ from strike: <b>${abs(price - price_T):.0f}</b> — the bigger this has "
        f"grown by the time you stake, the lower the app's real To Win\n"
        f"Window: {ist_hm(w_anchor)}  |  Move now: {bps_now:+.1f} bps"
    )


def check_value_trigger(w, w_anchor, now, price, price_T, expiry_time):
    """Stable-condition state machine for the VALUE trigger — between the
    primary decision and the late leg's window (expiry-90s), watch for the
    calibrated estimated payout of the ALREADY-DECIDED side to reach
    VALUE_PAYOUT_TARGET while the move keeps the decided sign, held for
    VALUE_DWELL_SECONDS. STRONG+ tiers only, primary must have staked."""
    side = w.get("side")
    if side not in ("HIGHER", "LOWER"):
        return
    sign_dec = 1 if side == "HIGHER" else -1
    bps_now = (price - price_T) / price_T * 10000.0
    sign_now = 1 if bps_now > 0 else (-1 if bps_now < 0 else 0)
    # Deliberately the LEGACY calibration: the 0.30 target was validated
    # against it (see assumed_payout_for_time_legacy's docstring) — the
    # 2026-07-04 market-curve refit only changes displayed/est payouts.
    payout = assumed_payout_for_time_legacy(abs(bps_now), (expiry_time - now).total_seconds()) if sign_now == sign_dec else None
    if sign_now == sign_dec and payout is not None and payout >= VALUE_PAYOUT_TARGET:
        if w.get("value_cond_ts") is None:
            w["value_cond_ts"] = now
        elif (now - w["value_cond_ts"]).total_seconds() >= VALUE_DWELL_SECONDS:
            fire_value_leg(w, w_anchor, now, price, price_T, bps_now, payout, expiry_time)
    else:
        w["value_cond_ts"] = None


def fire_fade_leg(w, w_anchor, now, price, price_T, bps_now, cheap_ask, expiry_time):
    """Fire the one-per-window SWING_FADE leg (added 2026-07-26) — BUY THE
    LOSING SIDE at `cheap_ask` and hold to expiry.

    Payout arithmetic is INVERTED relative to every other leg in this file: we
    pay `cheap_ask` for a contract that settles at 1, so profit per $1 staked
    on a win is (1 - cheap_ask)/cheap_ask, e.g. ~5.7x at an ask of 0.15. That
    is what goes into assumed_payout, so the shared persistence and est-return
    maths downstream stay correct without special-casing.

    Only touches fade_* keys, same isolation rule as fire_late_leg /
    fire_value_leg."""
    move_side = "HIGHER" if bps_now > 0 else "LOWER"
    side = "LOWER" if move_side == "HIGHER" else "HIGHER"   # the fade
    seconds_remaining = (expiry_time - now).total_seconds()
    payout = (1.0 - cheap_ask) / cheap_ask if cheap_ask > 0 else None
    p_fill, fill_label = fill_probability(cheap_ask, seconds_remaining)

    stake_pct = FADE_STAKE_PCT
    stake_usdt = round(BANKROLL_USDT * stake_pct, 2)
    stake_ok = True
    reason = None
    # Same MIN_EXPECTED_RETURN_USDT backstop as the other legs. Effectively
    # never binds for a fade (a $2 ticket at ~5.7x pays ~$11 expected on a
    # win), kept so all four legs answer to one floor.
    if payout is not None:
        expected_profit = stake_usdt * payout
        if expected_profit < min_expected_return():
            stake_ok = False
            stake_pct = 0.0
            stake_usdt = 0.0
            reason = (f"expected return ${expected_profit:.2f} "
                      f"< ${min_expected_return():g} minimum")

    w["fade_leg_fired"] = True
    w["fade_side"] = side
    w["fade_seconds_before_expiry"] = seconds_remaining
    w["fade_price_at_fire"] = price
    w["fade_bps_at_fire"] = bps_now
    w["fade_entry_ask"] = cheap_ask
    w["fade_assumed_payout"] = payout
    w["fade_fill_prob"] = p_fill
    w["fade_stake_ok"] = stake_ok
    w["fade_stake_pct"] = stake_pct
    w["fade_stake_usdt"] = stake_usdt
    w["fade_adx"] = w.get("adx")
    w["fade_adx_unfavourable"] = w.get("adx_unfavourable")
    w["fade_no_stake_reason"] = reason

    persist_extra_leg_fire(w_anchor, w, leg_type="FADE", prefix="fade")

    if not stake_ok:
        print(f"\n*** FADE NO STAKE [SWING_FADE, {seconds_remaining:.0f}s to expiry] "
              f"window={w_anchor.strftime('%H:%M:%S')} reason={reason} ***\n", flush=True)
        return

    to_win = stake_usdt * payout if payout is not None else None
    to_win_str = f"${to_win:.2f}" if to_win is not None else "n/a"
    print(f"\n*** FADE ALERT [SWING_FADE, {seconds_remaining:.0f}s to expiry] "
          f"window={w_anchor.strftime('%H:%M:%S')} -> BUY {side} (the losing side) "
          f"${stake_usdt:.2f} USDT @ est. ask {cheap_ask:.3f} "
          f"(To Win {to_win_str}, fill {fill_label} {p_fill*100:.0f}%) "
          f"move={bps_now:+.1f}bps ***\n", flush=True)
    tg_broadcast(
        f"🔄 <b>FADE {side} — ${stake_usdt:.2f} USDT</b> [SWING_FADE, "
        f"~{seconds_remaining:.0f}s to expiry]\n"
        f"This BUYS THE LOSING SIDE and holds to expiry — it is a fade, not a "
        f"continuation bet. Expect to lose ~4 of 5; the ~{payout:.1f}x payout is "
        f"where the edge lives.\n"
        f"Est. entry ask: <b>{cheap_ask:.3f}</b> (To Win <b>{to_win_str}</b>) — this is a "
        f"MODEL price, Binance publishes no book. If the app's real ask is above "
        f"<b>{FADE_CHEAP_ASK_MAX:.3f}</b>, skip.\n"
        f"Fill estimate: <b>{fill_label} (~{p_fill*100:.0f}%)</b> — modelled, not measured\n"
        f"Window: {ist_hm(w_anchor)}  |  Move: {bps_now:+.1f} bps"
    )


def fade_path_stats(ticks, w_anchor, price_T, now):
    """Excursion + crossing-count of the path from window open to `now`, the
    two path gates PM's swingfade_ok() applies. Mirrors PM's definitions:
    excursions are max favourable/adverse bps from the open, and crossings
    counts sign changes of (price_now - price_open) with zero ticks ignored.

    Returns (up_exc_bps, dn_exc_bps, crossings) or (None, None, None) when the
    tick buffer doesn't cover the window open — in which case the caller must
    NOT fire, since an unmeasured gate is not a passed gate (the failure mode
    that hid PM's oppfade MIN_ORDER skip for two days)."""
    up = dn = 0.0
    crossings = 0
    last_sign = 0
    seen_open = False
    for t in ticks:
        try:
            ts = datetime.fromisoformat(t["t"])
        except (ValueError, TypeError, KeyError):
            continue
        if ts < w_anchor:
            continue
        if ts > now:
            break
        seen_open = True
        bps = (t["p"] - price_T) / price_T * 10000.0
        up = max(up, bps)
        dn = min(dn, bps)
        s = 1 if bps > 0 else (-1 if bps < 0 else 0)
        if s != 0:
            if last_sign != 0 and s != last_sign:
                crossings += 1
            last_sign = s
    if not seen_open:
        return None, None, None
    return up, abs(dn), crossings


def check_fade_trigger(w, w_anchor, now, price, price_T, expiry_time, ticks=None):
    """Stable-condition state machine for SWING_FADE. From FADE_ELAPSED_MIN_SEC
    (T+3) to expiry, watch for the LOSING side's modelled ask to sit inside
    [FADE_CHEAP_ASK_MIN, FADE_CHEAP_ASK_MAX] with the move holding its sign for
    FADE_DWELL_SECONDS, then buy that losing side once.

    Deliberately ungated on tier / ADX / ATR / stake_ok: every one of those
    filters selects for continuation, and this leg profits when continuation
    fails. It fires on windows the primary leg declined, which is the point."""
    seconds_remaining = (expiry_time - now).total_seconds()
    if seconds_remaining < FADE_MIN_ACTION_SECONDS:
        w["fade_cond_ts"] = None
        return
    bps_now = (price - price_T) / price_T * 10000.0
    sign_now = 1 if bps_now > 0 else (-1 if bps_now < 0 else 0)
    cheap = cheap_ask_now(abs(bps_now), seconds_remaining)
    in_band = (sign_now != 0 and cheap is not None
               and FADE_CHEAP_ASK_MIN <= cheap <= FADE_CHEAP_ASK_MAX)
    if not in_band:
        w["fade_cond_ts"] = None
        w["fade_cond_sign"] = None
        return
    # Path gates (PM's shipped rule) — checked only once the ask band is
    # already satisfied, so the tick walk stays off the hot path.
    up_exc, dn_exc, crossings = fade_path_stats(ticks or [], w_anchor, price_T, now)
    if up_exc is None:
        # tick buffer doesn't reach the window open — cannot evaluate the
        # gate, so do not fire (see fade_path_stats' docstring).
        w["fade_cond_ts"] = None
        return
    if up_exc > FADE_MAX_EXCURSION_BPS or dn_exc > FADE_MAX_EXCURSION_BPS:
        w["fade_cond_ts"] = None
        return
    if FADE_CROSS_EXCLUDE is not None and crossings == FADE_CROSS_EXCLUDE:
        w["fade_cond_ts"] = None
        return

    if w.get("fade_cond_ts") is None or w.get("fade_cond_sign") != sign_now:
        w["fade_cond_ts"] = now
        w["fade_cond_sign"] = sign_now
    elif (now - w["fade_cond_ts"]).total_seconds() >= FADE_DWELL_SECONDS:
        w["fade_up_exc_bps"] = up_exc
        w["fade_dn_exc_bps"] = dn_exc
        w["fade_crossings"] = crossings
        fire_fade_leg(w, w_anchor, now, price, price_T, bps_now, cheap, expiry_time)


def check_late_trigger(w, w_anchor, now, price, price_T, expiry_time, current_adx):
    """Stable-condition state machine for the late-window trigger — same
    hold-timer pattern as the mid-window dwell trigger, but over the last
    LATE_WINDOW_START_SEC before expiry, keyed on the $ delta BAND
    [LATE_DELTA_MIN_USD, LATE_DELTA_MAX_USD] rather than a bps floor (see
    the constants' comment for the 2026-07-04 redesign: big deltas near
    expiry pay $1-5 per $90 in the real app, the return lives in the small-
    delta band). Leaving the band in EITHER direction — collapsing below
    $15 or blowing out past $20 — resets the hold timer. Own state keys
    (the dwell keys are only valid between T+2 and T+3)."""
    bps_now = (price - price_T) / price_T * 10000.0
    sign_now = 1 if bps_now > 0 else (-1 if bps_now < 0 else 0)
    delta_usd = abs(price - price_T)
    if LATE_DELTA_MIN_USD <= delta_usd <= LATE_DELTA_MAX_USD and sign_now != 0:
        if w.get("late_cross_ts") is None or w.get("late_cross_sign") != sign_now:
            # first entry into the band, or the prior entry reversed sign --
            # restart the hold timer (leaving the band also resets, below)
            w["late_cross_ts"] = now
            w["late_cross_sign"] = sign_now
        elif (now - w["late_cross_ts"]).total_seconds() >= LATE_HOLD_SECONDS:
            # Only fire if there's still time to actually act on the alert —
            # see LATE_MIN_ACTION_SECONDS (first live fire came 6s before
            # expiry: unactionable by hand, ~0% payout left).
            if (expiry_time - now).total_seconds() >= LATE_MIN_ACTION_SECONDS:
                fire_late_leg(w, w_anchor, now, price, price_T, bps_now,
                              current_adx, expiry_time)
    else:
        w["late_cross_ts"] = None
        w["late_cross_sign"] = None


def main():
    print(f"[{datetime.now(timezone.utc).isoformat()}] Monitor started — {SYMBOL}, "
          f"recording T+{EARLY_OFFSET_MIN} and T+{DECISION_OFFSET_MIN}, decision/alert at "
          f"T+{DECISION_OFFSET_MIN}, expiry T+{EXPIRY_OFFSET_MIN}, "
          f"min alert threshold {ALERT_MIN_BPS}bps. Manual staking only — no orders placed. "
          f"State file: {STATE_PATH}", flush=True)

    ensure_windows_table()
    ensure_late_legs_table()

    # Recover anything left in status='DECIDED' by a previous restart landing
    # between T+3 and T+5 — see recover_stuck_decided_windows()'s docstring.
    windows = recover_stuck_decided_windows()
    ticks_preload = preload_recent_ticks()
    # Same for late legs left in status='FIRED' (fire -> T+5 gap): resolve
    # past-expiry ones now, merge still-pending ones into the windows dict so
    # they don't double-fire.
    for _anchor, _late_state in recover_stuck_late_legs().items():
        if _anchor in windows:
            windows[_anchor].update(_late_state)
        # if the parent window state wasn't itself reloaded (e.g. its main
        # leg already EXPIRED before the restart), there's nothing in memory
        # to merge into — the leg was either resolved above or will be
        # resolved by the next restart's recovery pass; the fired row itself
        # is safe in the DB either way.
    ticks = deque(ticks_preload, maxlen=TICK_BUFFER_LEN)
    last_heartbeat = 0
    last_adx_refresh = 0
    current_adx = None
    current_atr_bps = None
    current_di_plus = None
    current_di_minus = None
    tick_db_buffer = []
    last_tick_db_flush = time.time()

    # 1-minute candles for the dashboard's rolling 60-min candle chart. Closed
    # candles are resynced from Binance's authoritative kline history every
    # ADX_REFRESH_SECONDS (cheap — piggybacks on the existing ADX kline fetch,
    # no extra API call); the still-open final minute is built tick-by-tick on
    # every poll below, so it visibly grows/shrinks with the live price instead
    # of only updating once a minute.
    CANDLE_WINDOW_MIN = 60
    closed_candles_1m = []
    live_candle = None
    live_candle_minute_ms = None

    # Rolling last-24h hourly price range (low/high per closed hour) — changes
    # slowly, so it's refreshed on its own slower cadence, not every poll.
    HOURLY_REFRESH_SECONDS = 60
    last_hourly_refresh = 0
    hourly_range_24h = []

    # All-time staked/correct/accuracy, sourced from Postgres so it survives
    # process restarts (the in-memory `windows` dict above does not — see
    # feedback from 2026-07-01 about the scoreboard resetting to a
    # handful of datapoints after every restart despite 2 days of collection).
    CUMULATIVE_REFRESH_SECONDS = 30
    last_cumulative_refresh = 0
    cumulative_stats, retro_stake_map_all, retro_stake_pct_map_all = get_cumulative_stats()
    recent_gated_stats, retro_stake_map, retro_stake_pct_map = compute_retroactive_gated_stats(hours=12, return_map=True)
    recent_gated_stats_24h, retro_stake_map_24h, retro_stake_pct_map_24h = compute_retroactive_gated_stats(hours=24, return_map=True)
    late_leg_cumulative = get_late_leg_cumulative_stats()
    value_leg_cumulative = get_late_leg_cumulative_stats(leg_type="VALUE")
    fade_leg_cumulative = get_late_leg_cumulative_stats(leg_type="FADE")

    # Adaptive per-tier auto on/off gate — evaluate once before the loop
    # starts so a fresh restart doesn't stake blind for up to 5 minutes.
    # Restore enabled/disabled flags from the last state write FIRST (see
    # restore_gate_enabled_from_state_file's docstring) so a restart can't
    # silently un-disable a tier just because this instant's rolling sample
    # is thin — refresh_auto_gates() then re-evaluates from that baseline.
    last_auto_gate_refresh = 0
    restore_gate_enabled_from_state_file()
    refresh_auto_gates()

    # Telegram's $ stake is tied to TELEGRAM_BANKROLL_USERNAME's own compounding
    # capital (binance_5min_user_capital), refreshed periodically so manual
    # capital updates on the dashboard take effect without a monitor restart.
    global BANKROLL_USDT
    BANKROLL_REFRESH_SECONDS = 60
    last_bankroll_refresh = 0
    _cap = get_user_capital(TELEGRAM_BANKROLL_USERNAME)
    if _cap is not None:
        BANKROLL_USDT = _cap

    while True:
        try:
            now = datetime.now(timezone.utc)
            price = get_price()
            anchor = floor_to_5min(now)
            # Last tick BEFORE this one, kept for the boundary interpolation
            # below (must be read before the append).
            prev_tick = ticks[-1] if ticks else None
            ticks.append({"t": now.isoformat(), "p": price})
            tick_db_buffer.append((now, price))

            # Build/extend the live (currently-forming) 1-min candle on every
            # poll. On a minute rollover, archive the just-finished candle into
            # the closed list (best-effort; the periodic resync below corrects
            # it against the authoritative API shortly after) and start a new one.
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

            if time.time() - last_adx_refresh > ADX_REFRESH_SECONDS:
                try:
                    klines_df = get_klines_1m(limit=CANDLE_WINDOW_MIN + 5)
                    current_adx, current_di_plus, current_di_minus = get_adx_full(klines_df)
                    current_atr_bps = get_atr_bps(klines_df, period=ATR_PERIOD)
                    if WEAK_EDGE_ENABLED:
                        refresh_range_regime(klines_df, current_adx)
                    fetched = [
                        {"t": int(r.open_time), "o": r.open, "h": r.high, "l": r.low, "c": r.close}
                        for r in klines_df.itertuples()
                    ]
                    # drop the still-open minute the API includes — the live
                    # tick-built candle above is the up-to-date version of it.
                    closed_candles_1m = [c for c in fetched if c["t"] < live_candle_minute_ms]
                except Exception as e:
                    print(f"[{now.isoformat()}] ADX/candle refresh failed: {e}", flush=True)
                last_adx_refresh = time.time()

            if time.time() - last_hourly_refresh > HOURLY_REFRESH_SECONDS:
                try:
                    hourly_range_24h = get_hourly_range_24h()
                except Exception as e:
                    print(f"[{now.isoformat()}] hourly range refresh failed: {e}", flush=True)
                last_hourly_refresh = time.time()

            if time.time() - last_cumulative_refresh > CUMULATIVE_REFRESH_SECONDS:
                stats, stats_map_all, stats_pct_map_all = get_cumulative_stats()
                if stats is not None:
                    cumulative_stats = stats
                    retro_stake_map_all = stats_map_all
                    retro_stake_pct_map_all = stats_pct_map_all
                recent_stats, recent_map, recent_pct_map = compute_retroactive_gated_stats(hours=12, return_map=True)
                if recent_stats is not None:
                    recent_gated_stats = recent_stats
                    retro_stake_map = recent_map
                    retro_stake_pct_map = recent_pct_map
                recent_stats_24h, recent_map_24h, recent_pct_map_24h = compute_retroactive_gated_stats(hours=24, return_map=True)
                if recent_stats_24h is not None:
                    recent_gated_stats_24h = recent_stats_24h
                    retro_stake_map_24h = recent_map_24h
                    retro_stake_pct_map_24h = recent_pct_map_24h
                late_stats = get_late_leg_cumulative_stats()
                if late_stats is not None:
                    late_leg_cumulative = late_stats
                value_stats = get_late_leg_cumulative_stats(leg_type="VALUE")
                if value_stats is not None:
                    value_leg_cumulative = value_stats
                fade_stats = get_late_leg_cumulative_stats(leg_type="FADE")
                if fade_stats is not None:
                    fade_leg_cumulative = fade_stats
                last_cumulative_refresh = time.time()

            if time.time() - last_bankroll_refresh > BANKROLL_REFRESH_SECONDS:
                _cap = get_user_capital(TELEGRAM_BANKROLL_USERNAME)
                if _cap is not None:
                    BANKROLL_USDT = _cap
                last_bankroll_refresh = time.time()

            if time.time() - last_auto_gate_refresh > AUTO_GATE_REFRESH_SECONDS:
                refresh_auto_gates()
                last_auto_gate_refresh = time.time()

            if anchor not in windows:
                # If we're seeing this window for the first time more than one
                # poll interval after it actually opened (e.g. the script was
                # just restarted mid-window), we do NOT have its real price_T —
                # "now"'s price is not what the window opened at. Fabricating
                # price_T from the current price would let it instantly
                # fast-forward through T+2/T+3 with a fake zero-move "DECIDED"
                # state. Mark it INCOMPLETE instead: tracked for chart
                # continuity only, never staked/alerted/decided.
                age_seconds = (now - anchor).total_seconds()
                is_incomplete = age_seconds > POLL_SECONDS * 2
                # Restart-rescue (added 2026-07-03): a window that would be
                # INCOMPLETE (this process started mid-window, e.g. a deploy)
                # can still be tracked and decided NORMALLY if (a) the true
                # boundary price is reconstructable from the persisted DB
                # ticks (both surrounding ticks within 6s — no guessing) and
                # (b) the T+2 checkpoint hasn't passed yet, so every
                # remaining checkpoint will still be read at its correct
                # time. Before this, every deploy threw away the in-flight
                # window (and its fabricated price_T floated off the chart's
                # tick line, which is how the owner spotted it).
                reconstructed_T = None
                if is_incomplete and now < anchor + timedelta(minutes=EARLY_OFFSET_MIN):
                    reconstructed_T = reconstruct_boundary_price(anchor)
                    if reconstructed_T is not None:
                        is_incomplete = False
                # price_T interpolated to the exact :00 boundary (added
                # 2026-07-03, the owner noticed the chart's T marker sitting a
                # few seconds right of the gridline): the poll loop only
                # samples every POLL_SECONDS, so the first tick of a window
                # lands 0-2s after the true open — during a fast move that's
                # a few bps of strike error vs Binance's actual :00 sample.
                # Linear interpolation between the last pre-boundary tick and
                # this first post-boundary tick estimates the true boundary
                # price; falls back to the raw first tick if there's no
                # usable previous tick (startup, or a poll gap > 6s).
                price_T_est = price
                if reconstructed_T is not None:
                    price_T_est = reconstructed_T
                elif not is_incomplete and prev_tick is not None:
                    try:
                        t_prev = datetime.fromisoformat(prev_tick["t"])
                        gap = (now - t_prev).total_seconds()
                        if 0 < gap <= POLL_SECONDS * 3 and t_prev <= anchor:
                            frac = (anchor - t_prev).total_seconds() / gap
                            price_T_est = prev_tick["p"] + (price - prev_tick["p"]) * frac
                    except Exception:
                        pass
                windows[anchor] = {
                    "price_T": price_T_est, "status": "INCOMPLETE" if is_incomplete else "OPEN",
                    # Informational only, see t0_lean_side() docstring — never
                    # read by should_stake()/tier_for()/make_stake_decision().
                    "t0_lean_side": None if is_incomplete else t0_lean_side(closed_candles_1m),
                    "price_T2": None, "bps_T2": None,
                    "price_T3": None, "bps_T3": None,
                    "tier": None, "tier_acc": None, "side": None,
                    "price_T5": None, "bps_T5": None, "actual_dir": None, "correct": None,
                    "adx": None, "adx_confirmed": None, "adx_unfavourable": None, "stake_ok": None,
                    "atr_bps": None,
                    "stake_usdt": None, "stake_pct": None, "assumed_payout": None, "est_return_usdt": None,
                    "decided_early": False, "decision_trigger": None,
                    "liq_imbalance_300s": None, "liq_imbalance_900s": None,
                    # in-memory only, not persisted — tracks the mid-window
                    # dwell trigger's current stable crossing, if any
                    "dwell_cross_ts": None, "dwell_cross_sign": None,
                    # late-window trigger state (2026-07-03): crossing tracker
                    # (in-memory only) + the fired leg's own fields, persisted
                    # to binance_5min_late_legs (NOT binance_5min_windows)
                    "late_cross_ts": None, "late_cross_sign": None,
                    "late_leg_fired": False, "late_side": None,
                    "late_seconds_before_expiry": None, "late_price_at_fire": None,
                    "late_bps_at_fire": None, "late_assumed_payout": None,
                    "late_stake_ok": None, "late_stake_pct": None, "late_stake_usdt": None,
                    "late_adx": None, "late_adx_unfavourable": None,
                    "late_no_stake_reason": None, "late_correct": None,
                    "late_est_return_usdt": None,
                    # VALUE trigger state (2026-07-04): condition tracker
                    # (in-memory only) + the fired leg's fields, persisted to
                    # binance_5min_late_legs with leg_type='VALUE'
                    "value_cond_ts": None,
                    "value_leg_fired": False, "value_side": None,
                    "value_seconds_before_expiry": None, "value_price_at_fire": None,
                    "value_bps_at_fire": None, "value_assumed_payout": None,
                    "value_stake_ok": None, "value_stake_pct": None, "value_stake_usdt": None,
                    "value_adx": None, "value_adx_unfavourable": None,
                    "value_no_stake_reason": None, "value_correct": None,
                    "value_est_return_usdt": None,
                    # SWING_FADE state (2026-07-26): condition tracker
                    # (in-memory only) + the fired leg's fields, persisted to
                    # binance_5min_late_legs with leg_type='FADE'. fade_side is
                    # the side OPPOSITE the move; fade_entry_ask is the modelled
                    # cheap-side price paid, and fade_assumed_payout is derived
                    # from it as (1-ask)/ask, not from a tier.
                    "fade_cond_ts": None, "fade_cond_sign": None,
                    "fade_leg_fired": False, "fade_side": None,
                    "fade_seconds_before_expiry": None, "fade_price_at_fire": None,
                    "fade_bps_at_fire": None, "fade_entry_ask": None,
                    "fade_assumed_payout": None, "fade_fill_prob": None,
                    "fade_stake_ok": None, "fade_stake_pct": None, "fade_stake_usdt": None,
                    "fade_adx": None, "fade_adx_unfavourable": None,
                    "fade_no_stake_reason": None, "fade_correct": None,
                    "fade_est_return_usdt": None,
                    # Early-exit warning (2026-07-05): fires once per window,
                    # after the primary stake, if price reverses hard enough
                    # against the staked side to suggest the prediction is
                    # going wrong — see the check right after live_bps_now is
                    # computed in the main loop.
                    "early_exit_alerted": False, "early_exit_bps": None,
                    "early_exit_ts": None,
                }
                if is_incomplete:
                    print(f"[{now.isoformat()}] Resumed mid-window anchor={anchor.strftime('%H:%M:%S')} "
                          f"({age_seconds:.0f}s late, likely a restart) — marked INCOMPLETE, will not be staked",
                          flush=True)
                elif reconstructed_T is not None:
                    print(f"[{now.isoformat()}] Rescued mid-window anchor={anchor.strftime('%H:%M:%S')} "
                          f"({age_seconds:.0f}s late) — price_T={reconstructed_T:.2f} reconstructed from DB "
                          f"ticks at the boundary, window will be tracked/decided normally", flush=True)
                else:
                    interp_note = f" (interpolated to :00 boundary, first tick was {price:.2f} at +{age_seconds:.1f}s)" if price_T_est != price else ""
                    print(f"[{now.isoformat()}] New 5-min window anchor={anchor.strftime('%H:%M:%S')} "
                          f"price_T={price_T_est:.2f}{interp_note}", flush=True)

            # Check EVERY active window's checkpoints against the current price —
            # not just the window matching "now"'s anchor. A window started 3-5
            # minutes ago needs its T+3/T+5 checkpoints updated on THIS tick too,
            # otherwise it sits frozen at whatever status it had when it was the
            # "current" window and never reaches T+5.
            for w_anchor, w in windows.items():
                if w["status"] == "INCOMPLETE":
                    continue
                price_T = w["price_T"]
                early_time = w_anchor + timedelta(minutes=EARLY_OFFSET_MIN)
                decision_time = w_anchor + timedelta(minutes=DECISION_OFFSET_MIN)
                expiry_time = w_anchor + timedelta(minutes=EXPIRY_OFFSET_MIN)

                # Continuous live preview (added 2026-07-04, the owner: "get
                # values continuously for open stake for to win, value and
                # late"). Runs every poll tick for every active window,
                # independent of whether/what has fired — a live snapshot of
                # "what would this side pay right now" using the exact same
                # calibrated time-aware model as the real triggers. Ephemeral
                # (not persisted to Postgres, only the live JSON state) since
                # it's meaningless once the window settles.
                if w["status"] != "EXPIRED" and now < expiry_time:
                    live_bps_now = (price - price_T) / price_T * 10000.0
                    live_secs_remaining = (expiry_time - now).total_seconds()
                    w["live_bps_now"] = live_bps_now
                    w["live_seconds_remaining"] = live_secs_remaining
                    w["live_side_now"] = "HIGHER" if live_bps_now > 0 else ("LOWER" if live_bps_now < 0 else None)
                    w["live_payout_now"] = (
                        assumed_payout_for_time(abs(live_bps_now), live_secs_remaining)
                        if live_bps_now != 0 else None
                    )
                else:
                    w["live_bps_now"] = w["live_seconds_remaining"] = None
                    w["live_side_now"] = w["live_payout_now"] = None

                # Fast-move early warning — see required_fast_move_delta()'s
                # comment. Independent of tier/stake/side logic; fires only
                # once the required delta for THIS much time-left is crossed
                # (never, if >=150s still remain — see FAST_MOVE_DELTA_BANDS),
                # at most once per direction per window — pure lead-time
                # ping, not a stake recommendation.
                if FAST_MOVE_ALERT_ENABLED and w["status"] != "EXPIRED" and now < expiry_time and price_T:
                    secs_left = (expiry_time - now).total_seconds()
                    required_delta = required_fast_move_delta(secs_left)
                    delta_usd = price - price_T
                    fired_dir = None
                    if required_delta is not None:
                        if delta_usd >= required_delta and not w.get("fast_move_alerted_higher"):
                            w["fast_move_alerted_higher"] = True
                            fired_dir = "HIGHER"
                        elif delta_usd <= -required_delta and not w.get("fast_move_alerted_lower"):
                            w["fast_move_alerted_lower"] = True
                            fired_dir = "LOWER"
                    if fired_dir:
                        w["fast_move_alert_usd"] = delta_usd
                        w["fast_move_alert_ts"] = now.isoformat()
                        print(
                            f"[{now.isoformat()}] ⚡ FAST MOVE — window {w_anchor.strftime('%H:%M:%S')} "
                            f"price delta {delta_usd:+.2f} USD ({fired_dir}), ~{secs_left:.0f}s left "
                            f"to expiry. Get the bet in now before entry/odds degrade further.",
                            flush=True,
                        )
                        tg_broadcast(
                            f"⚡ FAST MOVE — window {ist_hm(w_anchor)} "
                            f"price delta {delta_usd:+.2f} USD ({fired_dir}), ~{secs_left:.0f}s left "
                            f"to expiry. Get the bet in now before entry/odds degrade further."
                        )

                # Fast-move early-exit warning (added 2026-07-06, the owner: "if
                # the price is going the wrong way give a early exit alert" —
                # direct follow-up to the fast-move ping above). The fast-move
                # ping has no tier/side attached, so the tier-based early-exit
                # warning below (keyed off stake_ok+side) never fires for a
                # manual bet placed off a fast-move ping alone — this covers
                # that gap. Same reversal bar as the tier-based version
                # (EARLY_EXIT_MIN_REVERSAL_BPS), just keyed off which
                # direction the fast-move alert fired instead. Fires at most
                # once per direction per window.
                for fm_dir, fm_fired_flag, fm_exit_flag in (
                    ("HIGHER", "fast_move_alerted_higher", "fast_move_early_exit_alerted_higher"),
                    ("LOWER", "fast_move_alerted_lower", "fast_move_early_exit_alerted_lower"),
                ):
                    if (w.get(fm_fired_flag) and not w.get(fm_exit_flag)
                            and now < expiry_time and w.get("live_bps_now") is not None):
                        live_bps_now = w["live_bps_now"]
                        reversed_hard = (
                            (fm_dir == "HIGHER" and live_bps_now <= -EARLY_EXIT_MIN_REVERSAL_BPS)
                            or (fm_dir == "LOWER" and live_bps_now >= EARLY_EXIT_MIN_REVERSAL_BPS)
                        )
                        if reversed_hard:
                            w[fm_exit_flag] = True
                            secs_left = (expiry_time - now).total_seconds()
                            print(
                                f"[{now.isoformat()}] 🚨 FAST-MOVE EARLY EXIT — window {w_anchor.strftime('%H:%M:%S')} "
                                f"was pinged {fm_dir} on a fast move, but price has now reversed to "
                                f"{live_bps_now:+.1f}bps from entry (~{secs_left:.0f}s left to expiry). "
                                f"Consider exiting that position now.",
                                flush=True,
                            )
                            tg_broadcast(
                                f"🚨 FAST-MOVE EARLY EXIT — window {ist_hm(w_anchor)} "
                                f"was pinged {fm_dir} on a fast move, but price has now reversed to "
                                f"{live_bps_now:+.1f}bps from entry (~{secs_left:.0f}s left to expiry). "
                                f"Consider exiting that position now."
                            )

                # Early-exit warning — see EARLY_EXIT_MIN_REVERSAL_BPS comment.
                # Only meaningful once a real stake exists (stake_ok+side),
                # only between the decision and expiry (before that there's no
                # position yet; after, it's already settled), and fires at
                # most once per window.
                if (w.get("stake_ok") and w.get("side") and not w.get("early_exit_alerted")
                        and now >= decision_time and w.get("live_bps_now") is not None):
                    live_bps_now = w["live_bps_now"]
                    reversed_hard = (
                        (w["side"] == "HIGHER" and live_bps_now <= -EARLY_EXIT_MIN_REVERSAL_BPS)
                        or (w["side"] == "LOWER" and live_bps_now >= EARLY_EXIT_MIN_REVERSAL_BPS)
                    )
                    if reversed_hard:
                        w["early_exit_alerted"] = True
                        w["early_exit_bps"] = live_bps_now
                        w["early_exit_ts"] = now.isoformat()
                        secs_left = w.get("live_seconds_remaining") or 0.0
                        msg = (
                            f"🚨 <b>EARLY EXIT WARNING</b> — window {w_anchor.strftime('%H:%M:%S')} "
                            f"staked {w['side']} (${w.get('stake_usdt') or 0:.0f} USDT), but price has "
                            f"reversed to {live_bps_now:+.1f}bps from entry ({ist_hm(now)}, "
                            f"~{secs_left:.0f}s left to expiry). Consider exiting the position now "
                            f"to cut the loss."
                        )
                        print(f"[{now.isoformat()}] {msg}", flush=True)
                        tg_broadcast(msg)

                if w["price_T2"] is None and now >= early_time:
                    w["price_T2"] = price
                    w["bps_T2"] = (price - price_T) / price_T * 10000.0

                    # Early-entry path (added 2026-07-03): if the move already
                    # qualifies for a stake-eligible tier by T+2, decide now
                    # instead of waiting the extra minute for T+3 — see
                    # make_stake_decision()'s docstring for why this is safe.
                    # Windows that only cross the threshold between T+2 and
                    # T+3 fall through untouched to the normal path below.
                    early_label, _ = tier_for(abs(w["bps_T2"]))
                    if early_label is not None and early_label not in NO_STAKE_TIERS:
                        fire_ok, defer_reason = True, None
                        if T2_FILTER_ENABLED:
                            fire_ok, defer_reason = t2_backward_filter(
                                w_anchor, price_T, w["bps_T2"], ticks, now)
                        if fire_ok:
                            make_stake_decision(w, w_anchor, now, price, price_T,
                                                 w["bps_T2"], current_adx, expiry_time, early=True,
                                                 ticks=ticks, current_atr_bps=current_atr_bps)
                        else:
                            # Not a rejection — the window stays OPEN and falls
                            # through to the mid-window dwell / T+3 paths below,
                            # exactly as if T+2 hadn't tier-qualified.
                            print(f"[{now.isoformat()}] T+2 early fire DEFERRED "
                                  f"anchor={w_anchor.strftime('%H:%M:%S')} "
                                  f"bps_T2={w['bps_T2']:+.2f} ({early_label}): "
                                  f"{defer_reason} — falling through to dwell/T+3",
                                  flush=True)

                # Mid-window dwell trigger (added 2026-07-03): for windows that
                # did NOT already qualify at the T+2 snapshot above, keep
                # checking every poll tick between T+2 and T+3 for a crossing
                # of DWELL_FLOOR_BPS that holds the same sign for
                # DWELL_SECONDS — see the constant's comment for the backtest
                # behind this. Only runs while still undecided and strictly
                # between T+2 and T+3; the T+3 fallback below is untouched.
                if w["status"] == "OPEN" and w["price_T2"] is not None and early_time <= now < decision_time:
                    bps_now = (price - price_T) / price_T * 10000.0
                    sign_now = 1 if bps_now > 0 else (-1 if bps_now < 0 else 0)
                    if abs(bps_now) >= DWELL_FLOOR_BPS and sign_now != 0:
                        if w["dwell_cross_ts"] is None or w["dwell_cross_sign"] != sign_now:
                            # first crossing, or the prior crossing reversed
                            # sign (invalidated) -- restart the dwell timer
                            w["dwell_cross_ts"] = now
                            w["dwell_cross_sign"] = sign_now
                        elif (now - w["dwell_cross_ts"]).total_seconds() >= DWELL_SECONDS:
                            make_stake_decision(w, w_anchor, now, price, price_T, bps_now,
                                                 current_adx, expiry_time, early=True,
                                                 trigger_label=f"T+2-T+3 dwell ({DWELL_SECONDS}s)",
                                                 ticks=ticks, current_atr_bps=current_atr_bps)
                    else:
                        # dropped back below the floor -- not stable, reset
                        w["dwell_cross_ts"] = None
                        w["dwell_cross_sign"] = None

                # Late-window trigger (2026-07-03): one extra, independent
                # stake opportunity in the last LATE_WINDOW_START_SEC before
                # expiry — fires on DECIDED windows whether the T+3-family
                # call was a stake or NO STAKE (that's the point: rescuing
                # late moves the fixed checkpoints can't see). One-shot per
                # window via late_leg_fired.
                late_trigger_start = expiry_time - timedelta(seconds=LATE_WINDOW_START_SEC)
                # Both LATE and VALUE are continuation bets (they stake WITH
                # the confirmed move), so FADE-ONLY mode stops them too — see
                # CONTINUATION_STAKE_ENABLED.
                if (LATE_TRIGGER_ENABLED and CONTINUATION_STAKE_ENABLED
                        and w["status"] == "DECIDED"
                        and not w.get("late_leg_fired") and late_trigger_start <= now < expiry_time
                        and not late_trigger_killed(now)):
                    check_late_trigger(w, w_anchor, now, price, price_T, expiry_time, current_adx)

                # VALUE trigger (2026-07-04): third stake signal — after a
                # STRONG+ decision that STAKED, watch for a sign-preserving
                # retrace that reprices our side at >= VALUE_PAYOUT_TARGET
                # (see the constants' comment for the validation). Scans
                # strictly BEFORE the late leg's territory, one-shot per
                # window via value_leg_fired.
                if (VALUE_TRIGGER_ENABLED and CONTINUATION_STAKE_ENABLED
                        and w["status"] == "DECIDED"
                        and w.get("stake_ok") and w.get("tier") in VALUE_TIERS
                        and not w.get("value_leg_fired") and now < late_trigger_start):
                    check_value_trigger(w, w_anchor, now, price, price_T, expiry_time)

                # SWING_FADE (2026-07-26): fourth signal — buy the LOSING side
                # cheap and hold to expiry. Unlike VALUE it does NOT require
                # the primary to have staked, and unlike LATE it runs the whole
                # post-T+3 stretch rather than a near-expiry slice (the broad
                # elapsed band is the stronger cell — see FADE_* constants).
                # One-shot per window via fade_leg_fired.
                if (FADE_TRIGGER_ENABLED and not w.get("fade_leg_fired")
                        and (now - w_anchor).total_seconds() >= FADE_ELAPSED_MIN_SEC
                        and now < expiry_time):
                    check_fade_trigger(w, w_anchor, now, price, price_T, expiry_time,
                                       ticks=ticks)

                if w["price_T3"] is None and now >= decision_time:
                    w["price_T3"] = price
                    bps_T3 = (price - price_T) / price_T * 10000.0
                    w["bps_T3"] = bps_T3

                    if w["status"] == "DECIDED":
                        # Already decided early at T+2 — just keep price_T3/
                        # bps_T3 current for the dashboard/backtest record,
                        # don't re-run the decision or re-fire an alert.
                        persist_window_decision(w_anchor, w)
                    else:
                        make_stake_decision(w, w_anchor, now, price, price_T,
                                             bps_T3, current_adx, expiry_time, early=False,
                                             ticks=ticks, current_atr_bps=current_atr_bps)

                if w["status"] == "DECIDED" and now >= expiry_time:
                    settlement_price = get_settlement_price(expiry_time, ticks, price)
                    actual_bps = (settlement_price - price_T) / price_T * 10000.0
                    outcome = "HIGHER" if actual_bps > 0 else ("LOWER" if actual_bps < 0 else "FLAT")
                    w["price_T5"] = settlement_price
                    w["bps_T5"] = actual_bps
                    w["actual_dir"] = outcome
                    # Per Binance's actual Rules text (confirmed 2026-07-03): an
                    # exact tie resolves 50-50, not as a loss for either side —
                    # treat outcome=="FLAT" as TIE regardless of which side was
                    # staked, not just the no-side case. Never observed in 250+
                    # historical windows (BTC/USDT granularity makes an exact
                    # price match astronomically rare), but cheap to get right.
                    if outcome == "FLAT":
                        w["correct"] = "TIE"
                    elif w["side"]:
                        w["correct"] = (w["side"] == outcome)
                    else:
                        w["correct"] = "TIE"
                    w["status"] = "EXPIRED"
                    # Estimated $ return for this window — NOT the real Binance
                    # fill (no API to read the actual price paid), but the same
                    # assumed-payout proxy already used for Kelly sizing
                    # (tier's own backtest_acc -> implied price). A correct
                    # stake nets stake*assumed_payout profit; a wrong one loses
                    # the full stake (confirmed from the owner's real trades: every
                    # loss is -100%, see project memory). None if not staked.
                    if w["stake_ok"] and w["side"] and w.get("assumed_payout") is not None and w.get("stake_usdt"):
                        if w["correct"] is True:
                            w["est_return_usdt"] = round(w["stake_usdt"] * w["assumed_payout"], 2)
                        elif w["correct"] is False:
                            w["est_return_usdt"] = round(-w["stake_usdt"], 2)
                        else:
                            w["est_return_usdt"] = 0.0
                    else:
                        w["est_return_usdt"] = None
                    persist_window_expiry(w_anchor, w)
                    print(f"[{now.isoformat()}] window={w_anchor.strftime('%H:%M:%S')} EXPIRED -> "
                          f"actual={outcome} ({actual_bps:+.1f}bps from price_T={price_T:.2f}) "
                          f"staked_side={w['side']} correct={w['correct']} "
                          f"est_return={w['est_return_usdt']}", flush=True)
                    if w["stake_ok"] and w["side"]:
                        result_emoji = "✅" if w["correct"] is True else ("❌" if w["correct"] is False else "➖")
                        outcome_str = ('CORRECT ✓' if w['correct'] is True
                                       else 'WRONG ✗' if w['correct'] is False else 'TIE')
                        result_stake_pct = w["stake_pct"]
                        result_assumed_payout = w.get("assumed_payout")
                        result_correct = w["correct"]
                        result_side = w["side"]

                        def _build_result_msg(capital):
                            # Personalized (2026-07-06) — see _build_stake_msg above;
                            # keeps a linked viewer's RESULT figure consistent with
                            # the STAKE figure they were alerted with for this window.
                            amt = round(result_stake_pct * capital, 2)
                            if result_correct is True and result_assumed_payout is not None:
                                ret = round(amt * result_assumed_payout, 2)
                            elif result_correct is False:
                                ret = round(-amt, 2)
                            else:
                                ret = 0.0
                            ret_str = f"{'+' if ret >= 0 else ''}{ret:.2f} USDT (est.)"
                            return (
                                f"{result_emoji} <b>RESULT</b> — window {ist_hm(w_anchor)}\n"
                                f"Staked: {result_side} (${amt:.0f} USDT)  |  Actual: {outcome}\n"
                                f"Move from T: {actual_bps:+.1f} bps  |  Price T+5: {price:.2f}\n"
                                f"Outcome: <b>{outcome_str}</b>"
                                f"  |  Est. return: {ret_str}"
                            )
                        tg_personalized(_build_result_msg)

                    # Resolve the VALUE leg (if one fired) against the same
                    # T+5 outcome.
                    if w.get("value_leg_fired"):
                        if outcome == "FLAT":
                            value_correct = "TIE"
                        else:
                            value_correct = (w.get("value_side") == outcome)
                        w["value_correct"] = value_correct
                        if (w.get("value_stake_ok") and w.get("value_stake_usdt")
                                and w.get("value_assumed_payout") is not None):
                            if value_correct is True:
                                w["value_est_return_usdt"] = round(w["value_stake_usdt"] * w["value_assumed_payout"], 2)
                            elif value_correct is False:
                                w["value_est_return_usdt"] = round(-w["value_stake_usdt"], 2)
                            else:
                                w["value_est_return_usdt"] = 0.0
                        persist_extra_leg_expiry(w_anchor, w, leg_type="VALUE", prefix="value")
                        if w.get("value_stake_ok"):
                            v_emoji = "✅" if value_correct is True else ("❌" if value_correct is False else "➖")
                            vret = w.get("value_est_return_usdt")
                            vret_str = f"{'+' if vret is not None and vret >= 0 else ''}{vret:.2f} USDT (est.)" if vret is not None else "n/a"
                            tg_broadcast(
                                f"💎{v_emoji} <b>VALUE LEG RESULT</b> — window {ist_hm(w_anchor)}\n"
                                f"Staked: {w['value_side']} (${w['value_stake_usdt']:.0f} USDT)  |  Actual: {outcome}\n"
                                f"Outcome: <b>{'CORRECT ✓' if value_correct is True else 'WRONG ✗' if value_correct is False else 'TIE'}</b>"
                                f"  |  Est. return: {vret_str}"
                            )

                    # Resolve the SWING_FADE leg (if one fired) against the
                    # same T+5 outcome. Held to expiry by design — there is no
                    # early-exit path for a fade (see the FADE_* block).
                    if w.get("fade_leg_fired"):
                        if outcome == "FLAT":
                            fade_correct = "TIE"
                        else:
                            fade_correct = (w.get("fade_side") == outcome)
                        w["fade_correct"] = fade_correct
                        if (w.get("fade_stake_ok") and w.get("fade_stake_usdt")
                                and w.get("fade_assumed_payout") is not None):
                            if fade_correct is True:
                                w["fade_est_return_usdt"] = round(w["fade_stake_usdt"] * w["fade_assumed_payout"], 2)
                            elif fade_correct is False:
                                w["fade_est_return_usdt"] = round(-w["fade_stake_usdt"], 2)
                            else:
                                w["fade_est_return_usdt"] = 0.0
                        persist_extra_leg_expiry(w_anchor, w, leg_type="FADE", prefix="fade")
                        if w.get("fade_stake_ok"):
                            f_emoji = "✅" if fade_correct is True else ("❌" if fade_correct is False else "➖")
                            fret = w.get("fade_est_return_usdt")
                            fret_str = f"{'+' if fret is not None and fret >= 0 else ''}{fret:.2f} USDT (est.)" if fret is not None else "n/a"
                            tg_broadcast(
                                f"🔄{f_emoji} <b>FADE LEG RESULT</b> — window {ist_hm(w_anchor)}\n"
                                f"Bought: {w['fade_side']} @ {w['fade_entry_ask']:.3f} "
                                f"(${w['fade_stake_usdt']:.2f} USDT)  |  Actual: {outcome}\n"
                                f"Outcome: <b>{'CORRECT ✓' if fade_correct is True else 'WRONG ✗' if fade_correct is False else 'TIE'}</b>"
                                f"  |  Est. return: {fret_str}"
                            )

                    # Resolve the late leg (if one fired) against the same
                    # T+5 outcome — separate persistence and a distinctly
                    # 🌙-labeled Telegram result so the two legs of one
                    # window are visually distinguishable.
                    if w.get("late_leg_fired"):
                        if outcome == "FLAT":
                            late_correct = "TIE"
                        else:
                            late_correct = (w.get("late_side") == outcome)
                        w["late_correct"] = late_correct
                        if (w.get("late_stake_ok") and w.get("late_stake_usdt")
                                and w.get("late_assumed_payout") is not None):
                            if late_correct is True:
                                w["late_est_return_usdt"] = round(w["late_stake_usdt"] * w["late_assumed_payout"], 2)
                            elif late_correct is False:
                                w["late_est_return_usdt"] = round(-w["late_stake_usdt"], 2)
                            else:
                                w["late_est_return_usdt"] = 0.0
                        persist_late_leg_expiry(w_anchor, w)
                        if w.get("late_stake_ok"):
                            late_emoji = "✅" if late_correct is True else ("❌" if late_correct is False else "➖")
                            lret = w.get("late_est_return_usdt")
                            lret_str = f"{'+' if lret is not None and lret >= 0 else ''}{lret:.2f} USDT (est.)" if lret is not None else "n/a"
                            tg_broadcast(
                                f"🌙{late_emoji} <b>LATE LEG RESULT</b> — window {ist_hm(w_anchor)}\n"
                                f"Staked: {w['late_side']} (${w['late_stake_usdt']:.0f} USDT)  |  Actual: {outcome}\n"
                                f"Outcome: <b>{'CORRECT ✓' if late_correct is True else 'WRONG ✗' if late_correct is False else 'TIE'}</b>"
                                f"  |  Est. return: {lret_str}"
                            )

            # prune old windows beyond what the dashboard needs
            cutoff = now - timedelta(hours=6)
            for k in [k for k in windows if k < cutoff]:
                del windows[k]

            candles_1m = (closed_candles_1m + [live_candle] if live_candle else closed_candles_1m)[-CANDLE_WINDOW_MIN:]
            write_state(windows, ticks, candles_1m, hourly_range_24h, cumulative_stats, AUTO_GATE_STATUS,
                        recent_gated_stats, retro_stake_map, late_leg_cumulative, value_leg_cumulative,
                        fade_leg_cumulative=fade_leg_cumulative,
                        retro_stake_pct_map=retro_stake_pct_map,
                        recent_gated_stats_24h=recent_gated_stats_24h,
                        retro_stake_map_24h=retro_stake_map_24h,
                        retro_stake_pct_map_24h=retro_stake_pct_map_24h,
                        retro_stake_map_all=retro_stake_map_all,
                        retro_stake_pct_map_all=retro_stake_pct_map_all,
                        current_adx=current_adx, current_atr_bps=current_atr_bps,
                        current_di_plus=current_di_plus, current_di_minus=current_di_minus)

            if time.time() - last_tick_db_flush > TICK_DB_FLUSH_SECONDS:
                flush_ticks_to_db(tick_db_buffer)
                tick_db_buffer = []
                last_tick_db_flush = time.time()

            if time.time() - last_heartbeat > 60:
                print(f"[{now.isoformat()}] heartbeat: price={price:.2f}, "
                      f"{len(windows)} active window(s)", flush=True)
                last_heartbeat = time.time()

        except requests.exceptions.RequestException as e:
            print(f"[{datetime.now(timezone.utc).isoformat()}] network error: {e}, retrying...", flush=True)
        except Exception as e:
            print(f"[{datetime.now(timezone.utc).isoformat()}] unexpected error: {e}", flush=True)

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
