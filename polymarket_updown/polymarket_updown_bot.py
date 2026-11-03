#!/usr/bin/env python3
"""
Polymarket "BTC Up or Down 5m" auto-trading bot  (dry-run capable).

Re-targets the proven Binance 5-min T+3 continuation edge
([[project_binance_5min_continuation_monitor]]) onto Polymarket's
`btc-updown-5m-{anchor}` binary markets, which — unlike Binance Event
Contracts — expose a real programmatic order API (CLOB) via the unified
`polymarket-client` SDK.

Decision model (unchanged from the Binance monitor, the validated edge):
  * Each market is one 5-minute ET window (xx:00, xx:05, ... UTC-aligned).
  * At T+3 (180s into the window) we bet the direction BTC has ALREADY moved
    from the window open, on the continuation assumption (76.9% base -> 96.8%
    for >=30bps moves). Move size picks the tier; ADX(14) confirms/vetoes.
  * We BUY the "Up" or "Down" outcome token accordingly.

What is NEW here (the "auto entry + exit redesign"):
  * Real executable venue: a binary token bought at ask `a` pays 1.0 if the
    side wins, 0.0 if it loses -> breakeven win-rate == `a`. So the stake gate
    is the natural real-payout floor: only trade when tier_acc - margin > ask.
  * Active TP + SL management within the 5-min window (owner's call):
      - TP: sell the token back to the book once its bid runs to TP_BID (lock
        a near-certain win instead of riding a late reversal to 0).
      - SL: cut when the token bid sags to SL_BID, OR when the underlying
        Binance tick reverses back through the window open against our side by
        more than SL_REVERSAL_BPS (the continuation thesis has broken).
      - Otherwise hold to window-end settlement (pays 0/1).

Safety: POLYMARKET_DRY_RUN defaults ON. In dry-run NO orders are placed;
fills/exits are simulated by WALKING THE LIVE ORDER BOOK for the requested
share size, so logged entry/exit prices include real slippage on these thin
windows. Flip POLYMARKET_DRY_RUN=0 (with a funded wallet key) to go live; the
executor is the only thing that changes.

Runs under the isolated 3.11 venv:  .venv-pm/bin/python  (SDK needs py>=3.11).
"""
import asyncio
import contextlib
import json
import math
import os
import re
from collections import deque
import tempfile
import threading
import time
import traceback
import warnings
from datetime import datetime, timezone

warnings.filterwarnings("ignore")

import pandas as pd
# pandas_ta registers the DataFrame `.ta` accessor that adx_now() uses. Imported
# EXPLICITLY here as of 2026-07-30 because it previously arrived only as a SIDE
# EFFECT: the accessor appeared when `from DBops import get_exchange_api_credentials`
# ran (DBops line 2 does `import pandas_ta as ta`), which happens during LIVE
# credential setup. So `.ta` existed in live mode and NOT in a plain import of this
# module — and in any run that never touched credentials, `df.ta` raised
# AttributeError, adx_now() swallowed it and returned None, and every ADX gate
# silently went NULL. Uses whatever pandas_ta is installed in THIS venv (.venv-pm);
# the main trading bot's copy is deliberately left alone.
import pandas_ta  # noqa: F401  (registers pd.DataFrame.ta)
import requests
import sqlalchemy
from sqlalchemy import text

from polymarket import AsyncPublicClient, AsyncSecureClient
from polymarket.streams import MarketSpec

import sys
sys.path.insert(0,".")
from config import DB_URL
import pm_twap_calibration as twap_cal
import pm_twap_entry_quality as twap_quality
from swingfade_trial import active_trial as swingfade_trial_active, trial_band as swingfade_trial_band, confirmation as swingfade_trial_confirmation, trial_blocks_live as swingfade_trial_blocks_live
from pm_fast_band_regime import (classify as fast_band_classify,
                                 compute_features as fast_band_features,
                                 public_dict as fast_band_public_dict,
                                 transition as fast_band_transition)

# --- Order-placement egress proxy (Mumbai relay, Dubai failover) -------------
# PM GEO-RESTRICTS order placement from this AWS region, so `POST /order` must
# egress through an allowed-region relay (tinyproxy on the Mumbai EIP). But ALL
# reads — the PM market WS book feed, PM REST snapshots, and Chainlink — must go
# DIRECT from this box: routing them through the relay broke the WS handshake
# (tinyproxy can't tunnel WSS) and blanked the live ask/queue. So we do NOT set
# HTTPS_PROXY process-global; instead we bake the proxy into ONLY the secure
# (order-placement) client by scoping the env to its construction. The SDK builds
# every httpx transport eagerly in AsyncSecureClient.create() and reuses them for
# the client's lifetime, so this env scope captures order placement while the
# public/data client (built outside the scope) stays direct.
#
# FAILOVER (2026-08-06): a second relay (Dubai) can be set as
# POLYMARKET_ORDER_PROXY_FALLBACK. If unset, behavior is unchanged from before.
# If set: LiveExecutor.create() health-checks the primary before first use, and
# LiveExecutor rebuilds its secure client through the fallback (once per process)
# if an order call fails with a connection-shaped error (the relay box itself is
# unreachable) — NOT on a legitimate PM API rejection, which still surfaces
# normally. See LiveExecutor._failover().
ORDER_PROXY = os.getenv("POLYMARKET_ORDER_PROXY", "").strip()
ORDER_PROXY_FALLBACK = os.getenv("POLYMARKET_ORDER_PROXY_FALLBACK", "").strip()


@contextlib.contextmanager
def _order_proxy_env(proxy=None):
    """Temporarily export HTTPS_PROXY/HTTP_PROXY = `proxy` (defaults to ORDER_PROXY)
    so any httpx client constructed inside the `with` block routes through it, then
    restore the prior env. No-op when there is no proxy to set."""
    proxy = ORDER_PROXY if proxy is None else proxy
    if not proxy:
        yield
        return
    prev = {k: os.environ.get(k) for k in ("HTTPS_PROXY", "HTTP_PROXY")}
    os.environ["HTTPS_PROXY"] = proxy
    os.environ["HTTP_PROXY"] = proxy
    try:
        yield
    finally:
        for k, v in prev.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def _proxy_tcp_alive(proxy_url, timeout=2.5):
    """Raw TCP reachability check for a tinyproxy relay — confirms the box is up
    and accepting connections, not that it correctly tunnels PM traffic. Used only
    to pick a starting proxy / detect a dead relay; a full order still validates
    the real path."""
    if not proxy_url:
        return False
    import socket
    from urllib.parse import urlparse
    u = urlparse(proxy_url)
    try:
        with socket.create_connection((u.hostname, u.port), timeout=timeout):
            return True
    except OSError:
        return False


def _is_proxy_failure(exc):
    """True if `exc` looks like the order-egress relay itself is unreachable (dead
    tinyproxy box, connection refused/reset/timeout) rather than a legitimate PM
    API rejection (bad price, insufficient balance, geo-block message, etc.) —
    the signal LiveExecutor uses to fail over instead of just reporting the error."""
    try:
        import httpx
        if isinstance(exc, (httpx.ConnectError, httpx.ConnectTimeout, httpx.ProxyError,
                             httpx.ReadTimeout, httpx.WriteTimeout, httpx.TransportError)):
            return True
    except ImportError:
        pass
    if isinstance(exc, (ConnectionRefusedError, ConnectionResetError, TimeoutError, OSError)):
        return True
    msg = str(exc).lower()
    name = type(exc).__name__.lower()
    if "transport" in name or "proxy" in name:
        return True
    return any(s in msg for s in (
        "connection refused", "connect timeout", "proxyerror", "connection reset",
        "temporary failure in name resolution", "no route to host", "timed out",
        "connect_tunnel_failed", "server disconnected", "request failed",
        "all connection attempts failed"))

ENGINE = sqlalchemy.create_engine(DB_URL, echo=False, pool_pre_ping=True)

# --- ASSET (2026-07-11) — defined first so config is scoped per-asset. Defaults
# to BTC so the existing BTC instance/dashboard/config rows are byte-for-byte
# unchanged; a sibling ETH instance sets POLYMARKET_ASSET=ETH and gets a fully
# isolated config namespace (its own live_mode/capital_usd, never inheriting BTC's).
ASSET = os.getenv("POLYMARKET_ASSET", "BTC").strip().upper()
# OPPFADE is retired on ETH.  Its per-book live toggle only controls order
# placement; without this strategy gate a disarmed book still emits modelled
# shadow stakes and silently reappears in the forward cohort.
ETH_OPPFADE_RETIRED = (ASSET == "ETH")


def _cfg_key(key):
    # BTC keeps bare keys (back-compat); other assets are namespaced so the
    # global polymarket_config table can't leak live_mode/capital across instances.
    return key if ASSET == "BTC" else f"{ASSET}:{key}"


# --- dash-settable config (key/value) — lets the owner set trading CAPITAL from the
# dashboard instead of editing the systemd env. Bot reads at startup. ----------
def _ensure_config_table():
    with ENGINE.begin() as c:
        c.execute(text("""
            CREATE TABLE IF NOT EXISTS polymarket_config (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )"""))


_TWAP_CONFIG_KEYS = frozenset(['twap_edge_shadow_candidate', 'twap_edge_shadow_weekend', 'twap_edge_max_px', 'twap_edge_shadow_ladder', 'twap_edge_stake_ladder_step_usd', 'twap_edge_stake_ladder_max_usd', 'twap_edge_shadow_stake_per_tier', 'twap_lock_shadow_ladder', 'twap_lock_stake_ladder_step_usd', 'twap_lock_stake_ladder_max_usd', 'twap_lock_shadow_stake_per_tier'])
_TWAP_CONFIG_CACHE = None


def get_config(key, default=None):
    if _TWAP_CONFIG_CACHE is not None and key in _TWAP_CONFIG_KEYS:
        if time.time()-_TWAP_CONFIG_CACHE['ts'] > 3:
            raise ValueError('TWAP configuration cache stale')
        return _TWAP_CONFIG_CACHE['values'].get(_cfg_key(key), default)
    try:
        _ensure_config_table()
        with ENGINE.connect() as c:
            v = c.execute(text("SELECT value FROM polymarket_config WHERE key=:k"),
                          {"k": _cfg_key(key)}).scalar()
        return v if v is not None else default
    except Exception:
        return default


def set_config(key, value):
    _ensure_config_table()
    with ENGINE.begin() as c:
        c.execute(text("""
            INSERT INTO polymarket_config (key, value) VALUES (:k, :v)
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value,
                updated_at = NOW()"""), {"k": _cfg_key(key), "v": str(value)})


def get_capital(default=100.0):
    """Trading capital / starting bankroll. Dash-set `capital_usd` wins, else the
    systemd env, else `default`."""
    v = get_config("capital_usd", None)
    if v is None:
        v = os.getenv("POLYMARKET_BANKROLL_USD")
    try:
        return float(v) if v is not None else float(default)
    except (TypeError, ValueError):
        return float(default)


CAPITAL_WALLET_FRACTION = 0.90


def cap_capital_to_wallet(wallet_usdc):
    """Reduce an over-funded configured bankroll to 90% of PM collateral.

    The cent value is floored, not rounded, so it can never exceed the safety
    fraction because of display precision. Returns ``(capital, changed)``.
    """
    try:
        wallet = float(wallet_usdc)
    except (TypeError, ValueError):
        return get_capital(), False
    current = get_capital()
    if wallet < 0 or current <= wallet:
        return current, False
    capped = math.floor(wallet * CAPITAL_WALLET_FRACTION * 100.0) / 100.0
    # capital_usd must stay positive; a sub-cent/empty wallet simply cannot
    # provide a meaningful bankroll and stake affordability remains $0.
    if capped <= 0:
        return current, False
    set_config("capital_usd", capped)
    return capped, True


def trading_enabled(default=True):
    """LIVE-READ master switch for STAKING only. The data COLLECTION (T+2->T+3 ask
    probe + eval legs + shadow fills) runs in the main loop irrespective of this —
    turning trading off keeps the process alive and the [T+2,T+3] ask dataset
    growing, it just places/records NO stakes. So "stop trading" no longer means
    "stop collecting" (no need to kill the service). Dash/config `trading_enabled`
    wins, else env POLYMARKET_TRADING_ENABLED, else default (on). Reversible with no
    restart (read every tick)."""
    v = get_config("trading_enabled", None)
    if v is None:
        v = os.getenv("POLYMARKET_TRADING_ENABLED")
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "on")


def _book_live(book, default):
    """Per-book LIVE/SHADOW switch — dash-settable, READ EVERY TICK (no restart).
    Config `<book>_live` in (1/on/…) → this book places REAL orders; anything else
    → SHADOW (still records modelled legs + P&L, never risks capital). This is
    layered UNDER the master live gate: a book only trades real when BOTH the bot
    is live (`self.live`, i.e. live_mode=1 + key + balance) AND its own toggle is
    on — so while the dash is in DRY-RUN every toggle is dormant. `default`
    preserves the prior env-driven behaviour when the key was never set (t3 live,
    fade follows FADE_SHADOW, flip shadow)."""
    allowed = os.getenv("POLYMARKET_LIVE_BOOKS", "").strip()
    if allowed and book not in {v.strip() for v in allowed.split(",")}:
        return False
    if book == "swingfade" and swingfade_trial_blocks_live(ASSET):
        return False
    v = get_config(f"{book}_live", None)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "on")


# ETH-only fast band regime controller. ``on`` is an execution veto layered
# below the operator switch; it never arms a manually disabled book. Unlike the
# retired ten-stake controller, it updates from the preceding market hour every
# five minutes and can recover in three favourable observations (~15 minutes).
ETH_BAND_REGIME_MODE = os.getenv(
    "POLYMARKET_ETH_BAND_REGIME", "on" if ASSET == "ETH" else "off"
).strip().lower()
ETH_BAND_REGIME_INTERVAL_S = int(os.getenv("POLYMARKET_ETH_BAND_REGIME_INTERVAL_S", "300"))
ETH_BAND_REGIME_ARM_CHECKS = int(os.getenv("POLYMARKET_ETH_BAND_REGIME_ARM_CHECKS", "3"))
ETH_BAND_REGIME_DISARM_CHECKS = int(os.getenv("POLYMARKET_ETH_BAND_REGIME_DISARM_CHECKS", "2"))
ETH_BAND_REGIME_COOLDOWN_S = int(os.getenv("POLYMARKET_ETH_BAND_REGIME_COOLDOWN_S", "1800"))
_BAND_REQUIRED_SCORE = {
    # Retained-tape causal replay (2026-08-25): score>=2 plus the hard
    # trend/feed/spread/shock veto retained all 6 eligible candidates (+$8.04).
    # Scores 3/4 suppressed profitable candidates without enough observations
    # to justify per-band tuning. Revisit once the collector has >=50 candidates.
    "swingband_21_25": 2, "swingband_25_30": 2,
    "swingband_30_40": 2, "swingband_40_50": 4,
}
_band_regime_cache = {"ts": 0.0, "bucket": None, "decisions": {},
                      "states": {}, "started_at": None, "market": {}}


def _load_fast_band_state():
    if _band_regime_cache["started_at"] is not None:
        return
    raw = get_config("fast_band_regime_state", None)
    try:
        saved = json.loads(raw) if raw else {}
    except (TypeError, ValueError):
        saved = {}
    _band_regime_cache["states"] = saved.get("states") or {}
    _band_regime_cache["bucket"] = saved.get("bucket")
    _band_regime_cache["started_at"] = float(saved.get("started_at") or time.time())


def _fast_band_minute_rows(now_ts):
    """One causal ETH midpoint observation per completed minute."""
    with ENGINE.connect() as c:
        rows = c.execute(text("""
            WITH venue_minute AS (
                SELECT date_trunc('minute', to_timestamp(observations_ts)) AS minute,
                       source, avg(price) AS price,
                       avg(CASE WHEN bid IS NOT NULL AND ask IS NOT NULL AND price > 0
                                THEN (ask-bid)/price*10000.0 END) AS spread_bps
                  FROM pm_reference_ticks
                 WHERE asset='ETH' AND observations_ts >= :lo
                   AND observations_ts < :completed
                 GROUP BY 1,2
            )
            SELECT extract(epoch FROM minute) AS ts, avg(price) AS price,
                   (max(price)-min(price))/avg(price)*10000.0 AS source_divergence_bps,
                   avg(spread_bps) AS spread_bps,
                   count(DISTINCT source) AS source_coverage
              FROM venue_minute GROUP BY minute ORDER BY minute
        """), {"lo": int(now_ts - 65*60),
                 "completed": int(now_ts // 60 * 60)}).mappings().all()
    return [dict(r) for r in rows]


def _fast_band_loss_marker(book):
    """Return a stable marker when the latest two settled stakes are losses.

    The marker lets the controller consume a loss pair once.  Without it, the
    same two database rows started another cooldown as soon as the book finished
    rearming, potentially leaving the book in a permanent cooldown/rearm loop.
    """
    table = "polymarket_updown_trades_eth"
    with ENGINE.connect() as c:
        rows = c.execute(text(f"""
            SELECT hit, window_anchor, leg, decided_at FROM {table}
             WHERE asset='ETH' AND book=:book AND staked IS TRUE
               AND status <> 'NO_FILL' AND hit IS NOT NULL
               AND decided_at >= to_timestamp(:started)
             ORDER BY decided_at DESC LIMIT 2
        """), {"book": book,
                 "started": _band_regime_cache["started_at"]}).mappings().all()
    if len(rows) != 2 or any(bool(r["hit"]) for r in rows):
        return None
    newest = rows[0]
    decided = newest["decided_at"]
    return "|".join((
        str(newest["window_anchor"]), str(newest["leg"]),
        decided.isoformat() if hasattr(decided, "isoformat") else str(decided),
    ))


def eth_band_regime_snapshot(force=False):
    if ASSET != "ETH":
        return {"mode": "off", "controller": "ETH-only fast market regime",
                "decisions": {}}
    _load_fast_band_state()
    now = time.monotonic()
    if (not force and _band_regime_cache["decisions"]
            and now - _band_regime_cache["ts"] < 30.0):
        return {"mode": ETH_BAND_REGIME_MODE,
                "controller": "fast_market_60m_5m",
                "market": _band_regime_cache["market"],
                "decisions": _band_regime_cache["decisions"]}
    wall_now = time.time()
    bucket = int(wall_now // ETH_BAND_REGIME_INTERVAL_S)
    advance = bucket != _band_regime_cache["bucket"]
    books = [s["book"] for s in SWINGBAND_SPECS]
    decisions = {}
    try:
        features = fast_band_features(_fast_band_minute_rows(wall_now))
        feature_public = fast_band_public_dict(features)
        for book in books:
            market = fast_band_classify(
                features, required_score=_BAND_REQUIRED_SCORE.get(book, 4))
            prior = _band_regime_cache["states"].get(book) or {}
            if advance or not prior:
                loss_marker = _fast_band_loss_marker(book)
                new_loss_pair = bool(
                    loss_marker and loss_marker != prior.get("loss_brake_marker"))
                d = fast_band_transition(
                    book, market, prior, now=wall_now,
                    consecutive_losses=2 if new_loss_pair else 0,
                    arm_checks=ETH_BAND_REGIME_ARM_CHECKS,
                    disarm_checks=ETH_BAND_REGIME_DISARM_CHECKS,
                    cooldown_seconds=ETH_BAND_REGIME_COOLDOWN_S)
                next_state = fast_band_public_dict(d)
                # Preserve the consumed marker through cooldown, blocked and
                # rearming states. Record a new marker only when that loss pair
                # actually trips the live-book performance brake.
                consumed = prior.get("loss_brake_marker")
                if new_loss_pair and d.state == "COOLDOWN":
                    consumed = loss_marker
                if consumed:
                    next_state["loss_brake_marker"] = consumed
                prior = next_state
                _band_regime_cache["states"][book] = prior
            decisions[book] = dict(prior, allowed=bool(prior.get("allowed")),
                                   market_score=market.score,
                                   required_score=market.required_score)
        _band_regime_cache["market"] = {
            "asof": datetime.fromtimestamp(wall_now, timezone.utc).isoformat(),
            "interval_s": ETH_BAND_REGIME_INTERVAL_S,
            "features": feature_public,
        }
        if advance:
            _band_regime_cache["bucket"] = bucket
            set_config("fast_band_regime_state", json.dumps({
                "started_at": _band_regime_cache["started_at"],
                "bucket": bucket, "states": _band_regime_cache["states"],
            }, separators=(",", ":")))
    except Exception as exc:
        _err("eth_fast_band_regime", exc)
        for book in books:
            decisions[book] = {"allowed": False, "state": "BLOCKED",
                               "reason": "fast regime unavailable",
                               "favourable_streak": 0,
                               "unfavourable_streak": 0,
                               "cooldown_until": 0.0}
    _band_regime_cache.update(ts=now, decisions=decisions)
    return {"mode": ETH_BAND_REGIME_MODE,
            "controller": "fast_market_60m_5m",
            "market": _band_regime_cache["market"],
            "decisions": decisions}


def eth_band_regime_allows(book):
    if ASSET != "ETH" or ETH_BAND_REGIME_MODE == "off":
        return True, "ETH band regime bypassed"
    d = eth_band_regime_snapshot()["decisions"].get(book)
    if not d:
        return False, "ETH band regime unavailable"
    if ETH_BAND_REGIME_MODE == "shadow":
        return True, "regime shadow: " + d["reason"]
    return bool(d["allowed"]), d["reason"]


# ---------------------------------------------------------------------------
# T3 CALENDAR GATE — no real T3 stake on Mon/Tue/Wed
# ---------------------------------------------------------------------------
# The skip set is supported for T3, but the 2026-08-02 cross-book audit found
# the opposite direction for swingfade: its current fixed gate scored 6/9 on
# Fri-Sun versus 3/7 Mon-Thu. Therefore this set is T3-only. Swingfade and the
# other independent books must not inherit a calendar rule calibrated on T3.
#
# 2026-09-13: calendar OFF for both assets. The 2026-08-28 Mon/Tue/Wed skip was
# calibrated on WEAK-heavy ask<=0.68 (that book loses weekdays). The live T3
# book is now STRONG / MODERATE-STRONG at 0.50-0.68, and that cohort's weekday
# tape is the edge (BTC Mon-Thu +$60.78 / n=96 vs Fri-Sun +$3.36 / n=15).
# Skipping Mon-Tue-Wed zeroed the new book (replay n=0). Set e.g. 5,6 to skip
# Sat/Sun only. UTC weekday(), Mon=0. Empty = no calendar veto.
#
# One set, one definition, read at module load. `POLYMARKET_T3_BOOK_SKIP_DOW` is
# still honoured as the deprecated name so an existing deployment keeps working.
PM_SKIP_DOW = frozenset(
    int(x) for x in os.getenv(
        "POLYMARKET_SKIP_DOW",
        os.getenv("POLYMARKET_T3_BOOK_SKIP_DOW", "")).split(",")
    if x.strip().isdigit())


def dow_stake_ok(window_start_utc=None):
    """(ok, reason) for the T3 calendar gate. Pure, so T3's live path, shadow
    scorer and backtests share one definition.

    Judged on the WINDOW START, not on wall clock: a 23:55 Sunday window decided
    at 00:01 Monday belongs to Sunday, and the two must not disagree. Callers
    without a window pass None and get wall clock, which is correct for a gate
    evaluated before any window exists (banner, dash status).

    FAIL-CLOSED on an unusable timestamp, for the reason swingfade_ok() documents:
    a gate that cannot tell what day it is must not read as permission to bet."""
    if not PM_SKIP_DOW:
        return True, "calendar gate off"
    when = window_start_utc if window_start_utc is not None else datetime.now(timezone.utc)
    if not isinstance(when, datetime):
        return False, f"window_start_utc unusable ({when!r})"
    try:
        # A naive timestamp is UTC by the field's own name — do NOT let astimezone()
        # read it as box-local. It happens to agree while this box runs UTC, which
        # is exactly the kind of silent dependency that breaks on a host move.
        if when.tzinfo is None:
            when = when.replace(tzinfo=timezone.utc)
        dow = when.astimezone(timezone.utc).weekday()
    except Exception:
        return False, f"window_start_utc unusable ({when!r})"
    if dow in PM_SKIP_DOW:
        return False, (f"dow {'MonTueWedThuFriSatSun'[dow*3:dow*3+3]} excluded "
                       f"(T3 no-stake calendar)")
    return True, "ok"


def book_stakes_now(book, default, when=None):
    """Does `book` place real orders now? T3 alone inherits its calendar veto;
    every other book is governed only by its own live/shadow toggle."""
    live = _book_live(book, default)
    return (live and btc_oracle_stakes_ok()
            and (book != "t3" or dow_stake_ok(when)[0]))


_btc_oracle_health_cache = {"ts": 0.0, "ok": False, "n": 0, "agree": 0}
BTC_ORACLE_HEALTH_N = int(os.getenv("POLYMARKET_BTC_ORACLE_HEALTH_N", "20"))
BTC_ORACLE_HEALTH_MIN_N = int(os.getenv("POLYMARKET_BTC_ORACLE_HEALTH_MIN_N", "10"))
BTC_ORACLE_HEALTH_MAX_MISMATCH = int(os.getenv(
    "POLYMARKET_BTC_ORACLE_HEALTH_MAX_MISMATCH", "1"))


def btc_oracle_stakes_ok():
    """Circuit-break BTC live execution when our reconstructed resolution feed
    drifts from official Polymarket verdicts. ETH never enters this branch.

    Cached because book_stakes_now() is called on every tick/book. Any DB error or
    insufficient resolved sample fails closed for BTC live money.
    """
    if ASSET != "BTC":
        return True
    now = time.time()
    if now - _btc_oracle_health_cache["ts"] < 60.0:
        return bool(_btc_oracle_health_cache["ok"])
    try:
        with ENGINE.connect() as c:
            rows = c.execute(text(f"""
                WITH verdicts AS (
                    SELECT DISTINCT ON (window_anchor)
                           window_anchor, pm_outcome
                      FROM {TRADES_TABLE}
                     WHERE asset='BTC' AND pm_outcome IN ('Up','Down')
                  ORDER BY window_anchor DESC
                     LIMIT :lim
                )
                SELECT v.pm_outcome, op.p_open, cp.p_close
                  FROM verdicts v
                  LEFT JOIN LATERAL (
                    SELECT avg(benchmark_price) p_open, count(*) n,
                           max(observations_ts)-min(observations_ts) span,
                           max(observations_ts) newest
                      FROM chainlink_prices
                     WHERE feed_id='btc/usd'
                       AND observations_ts > v.window_anchor - :win
                       AND observations_ts <= v.window_anchor
                  ) op ON op.n >= :min_obs AND op.span >= :min_span
                       AND op.newest >= v.window_anchor - 8
                  LEFT JOIN LATERAL (
                    SELECT avg(benchmark_price) p_close, count(*) n,
                           max(observations_ts)-min(observations_ts) span,
                           max(observations_ts) newest
                      FROM chainlink_prices
                     WHERE feed_id='btc/usd'
                       AND observations_ts > v.window_anchor + :window_s - :win
                       AND observations_ts <= v.window_anchor + :window_s
                  ) cp ON cp.n >= :min_obs AND cp.span >= :min_span
                       AND cp.newest >= v.window_anchor + :window_s - 8
            """), {"lim": BTC_ORACLE_HEALTH_N, "win": BTC_TWAP_WINDOW_S,
                    "min_obs": BTC_TWAP_MIN_OBS, "min_span": BTC_TWAP_MIN_SPAN_S,
                    "window_s": WINDOW_S}).fetchall()
        usable = [(pm, op, cp) for pm, op, cp in rows
                  if op is not None and cp is not None]
        agree = sum(1 for pm, op, cp in usable
                    if ("Up" if float(cp) >= float(op) else "Down") == pm)
        n = len(usable)
        ok = (n >= BTC_ORACLE_HEALTH_MIN_N
              and n - agree <= BTC_ORACLE_HEALTH_MAX_MISMATCH)
    except Exception as exc:
        _err("btc_oracle_health", exc)
        n = agree = 0
        ok = False
    _btc_oracle_health_cache.update(ts=now, ok=ok, n=n, agree=agree)
    return ok


def claim_cross_asset_window(window_anchor, book):
    """Atomically reserve one live BTC/ETH stake slot for a five-minute window.

    Both asset processes share this table, so simultaneous qualifying signals
    cannot each send an order.  Claims intentionally survive until the window
    expires; a successful stake must remain exclusive after it settles.
    """
    try:
        with ENGINE.begin() as c:
            c.execute(text("""
                CREATE TABLE IF NOT EXISTS polymarket_cross_asset_window_claim (
                    window_anchor BIGINT PRIMARY KEY,
                    asset TEXT NOT NULL,
                    book TEXT NOT NULL,
                    claimed_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
            """))
            row = c.execute(text("""
                INSERT INTO polymarket_cross_asset_window_claim
                    (window_anchor, asset, book)
                VALUES (:wa, :asset, :book)
                ON CONFLICT (window_anchor) DO NOTHING
                RETURNING window_anchor
            """), {"wa": int(window_anchor), "asset": ASSET,
                    "book": str(book)}).first()
        return row is not None
    except Exception as e:
        _err("claim_cross_asset_window", e)
        return False                         # coordination failure = no order


def release_cross_asset_window(window_anchor, book):
    """Release this process's claim after a killed/rejected order."""
    try:
        with ENGINE.begin() as c:
            c.execute(text("""
                DELETE FROM polymarket_cross_asset_window_claim
                 WHERE window_anchor=:wa AND asset=:asset AND book=:book
            """), {"wa": int(window_anchor), "asset": ASSET,
                    "book": str(book)})
    except Exception as e:
        _err("release_cross_asset_window", e)


# --- SHARED RESERVE () --------------------------------------
# The ledger below deliberately forbids one book BORROWING another's allocation —
# that is what makes a book's own drawdown its own stop. The RESERVE is the one
# sanctioned exception: capital held UNALLOCATED, which any live book may draw on
# in fixed increments once its own ledger can no longer fund a ticket.
#
# A draw is a permanent REASSIGNMENT (reserve down, book cap up) committed in one
# transaction — not a runtime loan. That matters: `available` is recomputed from
# config on every tick, so anything held only in memory would be double-counted on
# the next call and forgotten on restart. Because it lands in config, the audit
# trail is polymarket_config.updated_at and the dash renders it with no new table.
CROSSBOOK_TOPUP_USD = float(os.getenv("POLYMARKET_CROSSBOOK_TOPUP_USD", "5"))


def crossbook_topup(book, wallet_usdc=None):
    """Reassign up to CROSSBOOK_TOPUP_USD from the shared reserve into `book`.

    Returns the amount moved (0.0 if none). Only LIVE books with a positive
    allocation draw: a retired book must stay retired — oppfade is sitting at
    -$2.12 realized with cap 0 and must not be resurrected by the pool.
    """
    if CROSSBOOK_TOPUP_USD <= 0:
        return 0.0
    if not _book_live(book, False):
        return 0.0
    _rk, _bk = _cfg_key("crossbook_reserve_usd"), _cfg_key(f"{book}_capital_usd")
    try:
        with ENGINE.begin() as c:
            # Re-read and lock INSIDE the transaction. Two books can go dry on the
            # same tick; without FOR UPDATE both would draw against the same
            # balance and the reserve would go negative.
            rows = c.execute(text(
                "SELECT key, value FROM polymarket_config WHERE key IN (:rk, :bk) "
                "FOR UPDATE"), {"rk": _rk, "bk": _bk}).mappings().all()
            vals = {r["key"]: float(r["value"] or 0) for r in rows}
            reserve, cap = vals.get(_rk, 0.0), vals.get(_bk, 0.0)
            if cap <= 0:
                return 0.0
            draw = min(CROSSBOOK_TOPUP_USD, reserve)
            if wallet_usdc is not None:
                draw = min(draw, float(wallet_usdc))
            if draw < MIN_ORDER_USD:
                return 0.0        # not enough left to fund even one ticket
            draw = round(draw, 2)
            c.execute(text("UPDATE polymarket_config SET value=:v, updated_at=NOW() "
                           "WHERE key=:k"),
                      {"v": str(round(reserve - draw, 2)), "k": _rk})
            c.execute(text("UPDATE polymarket_config SET value=:v, updated_at=NOW() "
                           "WHERE key=:k"),
                      {"v": str(round(cap + draw, 2)), "k": _bk})
        print(f"[{datetime.now(timezone.utc):%H:%M:%S}] CROSSBOOK TOPUP {book} "
              f"ran dry -> +${draw:.2f} from reserve (reserve "
              f"${reserve - draw:.2f} left, {book} cap ${cap + draw:.2f})",
              flush=True)
        return draw
    except Exception as e:
        _err("crossbook_topup", e)
        return 0.0                # fail closed: no draw, book simply parks


def wallet_stake_ladder(book, wallet_usdc):
    """$1 of stake per $10 of PM cash, floored, min $1 (2026-10-01: $30 -> $3,
    $40 -> $4, and so on; drops back automatically as the wallet shrinks).

    Opt-in per book via `<book>_stake_ladder`=1. Replaces the flat
    `<book>_stake_usd` AND the env UNIFORM_STAKE_USD cap for that book, so the
    step-ups need no restart. `<book>_stake_ladder_step_usd` sets the $ of wallet
    per $1 of stake (default 10); `<book>_stake_ladder_max_usd` is an optional
    ceiling (0 = none). Returns None when off or the wallet is unknown, so the
    caller keeps its flat stake."""
    if str(get_config(f"{book}_stake_ladder", "0")).lower() not in ("1", "true", "on"):
        return None
    if wallet_usdc is None:
        return None
    step = float(get_config(f"{book}_stake_ladder_step_usd", 10) or 10)
    ceiling = float(get_config(f"{book}_stake_ladder_max_usd", 0) or 0)
    stake = max(1.0, float(math.floor(float(wallet_usdc) / step)))
    if ceiling > 0:
        stake = min(stake, ceiling)
    return stake


def crossbook_stake_for(book, wallet_usdc=None):
    """Flat live stake bounded by this book's durable capital ledger.

    Capital and the epoch are dashboard config, so restarts cannot reset losses or
    let one book borrow another's allocation.  Unsettled notional is reserved.
    A book that can no longer fund its ticket draws from the shared reserve once
    (see crossbook_topup) before giving up, so a live book parks only when the
    RESERVE is dry too — not merely when its own opening allocation is spent.
    Returns zero when the book cannot cover the exchange's minimum order.
    """
    if str(get_config("crossbook_enabled", "0")).lower() not in ("1", "true", "on"):
        return None                         # caller retains its legacy sizer
    try:
        cap = float(get_config(f"{book}_capital_usd", 0) or 0)
        stake = min(float(get_config(f"{book}_stake_usd", UNIFORM_STAKE_USD)
                          or UNIFORM_STAKE_USD), UNIFORM_STAKE_USD)
        laddered = wallet_stake_ladder(book, wallet_usdc)
        if laddered is not None:
            stake = laddered
        start = get_config("crossbook_start_utc", None)
        if cap <= 0 or not start:
            return 0.0
        with ENGINE.connect() as c:
            row = c.execute(text(f"""
                SELECT COALESCE(SUM(realized_pnl_usd),0),
                       COALESCE(SUM(stake_usd) FILTER
                         (WHERE realized_pnl_usd IS NULL AND staked),0)
                FROM {TRADES_TABLE}
                WHERE asset=:asset AND NOT dry_run AND book=:b
                  AND decided_at >= CAST(:s AS timestamptz)
            """), {"b": book, "s": start, "asset": ASSET}).one()
        available = cap + float(row[0] or 0) - float(row[1] or 0)
        # Dry: the book's own ledger can't fund the ticket. Draw from the shared
        # reserve ONCE and re-derive `available` from the new cap rather than
        # adding the draw to the old number — the top-up is committed to config,
        # so re-reading is the only version that stays true across restarts.
        if available < stake:
            if crossbook_topup(book, wallet_usdc) > 0:
                cap = float(get_config(f"{book}_capital_usd", 0) or 0)
                available = cap + float(row[0] or 0) - float(row[1] or 0)
        stake = min(stake, available)
        if wallet_usdc is not None:
            stake = min(stake, float(wallet_usdc))
        return max(0.0, round(stake, 2))
    except Exception as e:
        _err("crossbook_stake_for", e)
        return 0.0                         # fail closed


def t3_capital():
    """t3 (normal) book's share of the total dash bankroll. EXPLICIT share since
    2026-07-20 — it used to be the remainder (1 - fade - flip), so zeroing the other
    books silently handed t3 everything. Now every book names its own fraction and
    the shares are independent (they need not sum to 1; unallocated capital simply
    goes unstaked)."""
    return get_capital() * T3_CAPITAL_SHARE


def fade_capital():
    """Fade book's share of the total dash bankroll (hfade draws from this same
    fade pool — it is NOT a separately-capitalised book)."""
    return get_capital() * FADE_CAPITAL_SHARE


def flip_capital():
    """Flip book's share of the total dash bankroll. The flip book is SHADOW /
    modelled (2026-07-13): it gets its 1/3 notional and books modelled P&L
    (surfaced inline in the per-asset window tables), but never places a real
    order until the RE-flip edge is live-validated. See [[project_pm_re_flip_edge]]."""
    return get_capital() * FLIP_CAPITAL_SHARE


def oppfade_capital():
    """Cheap-opp fade book's share of the dash bankroll — the whole thing since
    2026-07-20. Compounds on the same 24h-daily rule as fade/flip (locked intraday,
    steps at 00:00 UTC on the prior day's realized P&L)."""
    return get_capital() * OPPFADE_CAPITAL_SHARE


# --- MULTI-USER data model (2026-07-10) — each user has their own
# encrypted PM wallet/key (exchange_api_credentials account_key =
# polymarket_user_<username>), their own capital, and enable/live flags. The
# DECISION/signal is SHARED per window; EXECUTION + bankroll are PER-USER.
# CUSTODIAL: the bot holds users' PM private keys — encrypted at rest, per-user
# isolated. Reuses the users/user_logins auth + the encrypted creds store,
# mirroring binance_5min_user_capital. Per-user auto-execution stays gated
# behind single-user live validation. ---------------------------------------
def _ensure_pm_users_table():
    with ENGINE.begin() as c:
        c.execute(text("""
            CREATE TABLE IF NOT EXISTS polymarket_user_capital (
                username     TEXT PRIMARY KEY,
                capital_usd  DOUBLE PRECISION NOT NULL DEFAULT 100,
                enabled      BOOLEAN NOT NULL DEFAULT FALSE,   -- auto-exec on/off
                live         BOOLEAN NOT NULL DEFAULT FALSE,   -- live-money vs dry
                created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )"""))


def pm_user_account_key(username):
    return "polymarket_user_" + str(username)


def upsert_pm_user(username, capital_usd=None, enabled=None, live=None):
    """Create/update a Polymarket user's capital + enable/live flags."""
    _ensure_pm_users_table()
    with ENGINE.begin() as c:
        c.execute(text("INSERT INTO polymarket_user_capital (username) VALUES (:u)"
                       " ON CONFLICT (username) DO NOTHING"), {"u": username})
        if capital_usd is not None:
            c.execute(text("UPDATE polymarket_user_capital SET capital_usd=:v,"
                           " updated_at=NOW() WHERE username=:u"),
                      {"v": float(capital_usd), "u": username})
        if enabled is not None:
            c.execute(text("UPDATE polymarket_user_capital SET enabled=:v,"
                           " updated_at=NOW() WHERE username=:u"),
                      {"v": bool(enabled), "u": username})
        if live is not None:
            c.execute(text("UPDATE polymarket_user_capital SET live=:v,"
                           " updated_at=NOW() WHERE username=:u"),
                      {"v": bool(live), "u": username})


def list_pm_users():
    """All Polymarket users with capital/flags + whether creds are stored."""
    _ensure_pm_users_table()
    try:
        from DBops import list_exchange_api_credentials_status
        cred_keys = {r.get("account_key") for r in
                     list_exchange_api_credentials_status()
                     if str(r.get("account_key", "")).startswith("polymarket_user_")}
    except Exception:
        cred_keys = set()
    out = []
    with ENGINE.connect() as c:
        for r in c.execute(text("SELECT username, capital_usd, enabled, live,"
                                " updated_at FROM polymarket_user_capital"
                                " ORDER BY username")):
            out.append({"username": r[0], "capital_usd": r[1], "enabled": r[2],
                        "live": r[3], "updated_at": r[4],
                        "has_creds": pm_user_account_key(r[0]) in cred_keys})
    return out


def get_pm_user_credentials(username):
    """Decrypt a user's PM wallet(api_key)/private-key(api_secret) from the
    encrypted store. Returns dict or None. Never logs the secret."""
    try:
        from DBops import get_exchange_api_credentials
        return get_exchange_api_credentials(
            account_key=pm_user_account_key(username),
            exchange_name="POLYMARKET", include_secrets=True)
    except Exception:
        return None


def save_pm_user_credentials(username, private_key, wallet=None):
    """Encrypt+store a user's PM key (mirrors the single-user creds flow)."""
    from DBops import upsert_exchange_api_credentials
    if not wallet:
        try:
            from eth_account import Account
            wallet = Account.from_key(private_key).address
        except Exception:
            wallet = ""
    return upsert_exchange_api_credentials(
        account_key=pm_user_account_key(username), exchange_name="POLYMARKET",
        api_key=wallet or username, api_secret=private_key,
        account_label=f"Polymarket user {username}")


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
def _resolve_dry_run():
    """Live mode: dash config `live_mode` (1=LIVE, 0=dry-run) wins, else env
    POLYMARKET_DRY_RUN. Lets the dashboard toggle go-live; the bot self-restarts
    (systemd Restart=always) when this changes to re-init the executor."""
    v = get_config("live_mode", None)
    if v is not None:
        return str(v).strip().lower() not in ("1", "true", "yes", "on")
    return os.getenv("POLYMARKET_DRY_RUN", "1") not in ("0", "false", "False", "")


DRY_RUN = _resolve_dry_run()
BANKROLL_USD = get_capital(float(os.getenv("POLYMARKET_BANKROLL_USD", "1000")))
STATE_PATH = os.getenv(
    "POLYMARKET_STATE_PATH", "./reports/polymarket_updown_state.json"
)
POLL_S = float(os.getenv("POLYMARKET_POLL_S", "3"))

# --- ASSET config (2026-07-11) — the bot is now asset-parameterized so a second
# instance can trade ETH (or another coin) alongside BTC with zero collision.
# All defaults are BTC, so the existing BTC service is byte-for-byte unchanged.
# A sibling instance sets POLYMARKET_ASSET / _SLUG_PREFIX / _TRADES_TABLE + its own
# STATE_PATH + bankroll. BTC and ETH both consume Polymarket's public Chainlink
# rebroadcast; neither needs a direct oracle subscription or exchange fallback.
SLUG_PREFIX = os.getenv("POLYMARKET_SLUG_PREFIX", "btc-updown-5m")
# Per-asset DB table keeps each instance's calibration/trade rows fully isolated
# (no shared unique-constraint to fight over the common 5-min window_anchor).
TRADES_TABLE = os.getenv("POLYMARKET_TRADES_TABLE", "polymarket_updown_trades")
# NO BINANCE. Every price, direction, momentum, trend and settlement read in this
# bot comes from Chainlink (the feed PM resolves on); order-book data comes from
# Polymarket. The exchange spot URL and symbol config were deleted 2026-07-30; the feed
# disagreement that motivated this is documented on get_klines_1m below.

WINDOW_S = 300
STATE_EXPORT_MIN_S = float(os.getenv("POLYMARKET_STATE_EXPORT_MIN_S", "3"))
STATE_EXPORT_DB_TTL_S = float(os.getenv("POLYMARKET_STATE_EXPORT_DB_TTL_S", "5"))
STATE_EXPORT_QUIET_S = (241, 263)     # entry window: no dashboard export (30s stale limit)
DECISION_OFFSET_S = int(os.getenv("POLYMARKET_DECISION_OFFSET_S", "180"))  # T+3
# T+1 -> T+3 SHADOW PROBE (calibration, pure logging): the live gate only ever
# evaluates the book at T+3 (180s), by which point the favorite has run to 0.9+
# and is priced out of the R:R cap. This samples every tick from T2_PROBE_OFFSET_S
# up to (not incl.) DECISION_OFFSET_S and records each distinct would-enter state,
# so we can measure WHEN in the window an entry is actually cheap enough BEFORE
# wiring an early-entry decision. Start lowered 120->60 (2026-07-12) to capture the
# ask at the T+1 marks (60/90/100s) — that's the missing EV measurement: direction
# @35bps+ is ~100% by 60-120s, but we didn't yet know what ask you'd pay that early.
# Writes to a SEPARATE table — never stakes, never touches the trades pipeline or
# the leg-index convention.
# Lowered 60->0 (2026-07-13) to capture the OPEN book from T+0: the flip edge
# (bet-reversal-of-prior-decisive-window) must be taken at the open ~0.505, and we
# were blind to how fast the book skews in the first 60s. Also now logs BOTH sides
# (bid_up/bid_down + ask_up/ask_down) so the sell/exit price is recorded, not just buy.
T2_PROBE = os.getenv("POLYMARKET_T2_PROBE", "1") not in ("0", "false", "False", "")
T2_PROBE_OFFSET_S = int(os.getenv("POLYMARKET_T2_PROBE_OFFSET_S", "0"))  # T+0 (was 60/120)
# Write the probe ROWS to polymarket_updown_t2probe? The tick LOOP is independent of
# this and must keep running whatever it is set to — it carries live swingfade
# first-touch state, not just calibration. See the switch inside _t2_probe() for why
# POLYMARKET_T2_PROBE=0 is the wrong lever and this one is the right one.
T2_PROBE_PERSIST = os.getenv("POLYMARKET_T2_PROBE_PERSIST", "1") not in (
    "0", "false", "False", "")
MIN_SHARES = 5.0        # (legacy) nominal share floor — no longer inflates sizing
# Minimum ORDER VALUE we'll place (2026-07-13): honour the EXACT stake
# fraction rather than inflating small stakes up to a 5-share ticket — a $1 / ~2sh
# market order fills fine on PM (proven live). A stake below this is SKIPPED (not
# rounded up), so the fraction is never violated upward. Lower it only if PM is
# confirmed to accept smaller orders.
MIN_ORDER_USD = float(os.getenv("POLYMARKET_MIN_ORDER_USD", "1.0"))
# One flat ticket across every visible BTC/ETH book. This is a hard ceiling:
# capital growth may never increase a qualifying order above one dollar.
UNIFORM_STAKE_USD = float(os.getenv("POLYMARKET_UNIFORM_STAKE_USD", "1.0"))
TICK = 0.01             # orderPriceMinTickSize

# --- Kelly / breakeven gate ---
KELLY_FRACTION = float(os.getenv("POLYMARKET_KELLY_FRACTION", "0.25"))
MIN_EDGE_MARGIN = float(os.getenv("POLYMARKET_MIN_EDGE_MARGIN", "0.03"))
# Per-trade risk as a FRACTION of the (compounding) bankroll — replaces the old
# fixed $ cap (removed 2026-07-10). Fractional-Kelly is taken up to this bound;
# raise it to make stake size (and thus visible compounding) grow faster off a
# small bankroll, at the cost of higher variance. NOTE: below ~$80 bankroll the
# 5-share exchange minimum ($5*ask) still floors every stake regardless of this.
STAKE_FRAC = float(os.getenv("POLYMARKET_STAKE_FRAC", "0.05"))
# Back-compat only (no longer caps sizing): some displays/params still read it.
_MAX_STAKE_ENV = os.getenv("POLYMARKET_MAX_STAKE_USD")
MAX_STAKE_USD = float(_MAX_STAKE_ENV) if _MAX_STAKE_ENV else STAKE_FRAC * BANKROLL_USD

# --- TWO-BOOK strategy () — the analysed/committed model, moved
# from dashboard-only analytics into actual (dry-run) EXECUTION. Guarded by
# POLYMARKET_TWO_BOOK (default OFF) so the running maker dry-run is untouched
# until this path is validated. When ON:
#   Book 1 (T+3):  TAKER, COMPOUNDED at T3_STAKE_FRAC, only cheap-ask (<=cap)
#       high-accuracy tiers, skip-on-reversal-signal, TP/SL exits. NO T+3 on
#       MODERATE. Bankroll = start + T+3 realized P&L.
#   Book 2 (fade): FLAT (off the START bankroll) at FADE_STAKE_FRAC, fires only
#       on the maker-fill REVERSAL signal, buys the CHEAP opposite, held to
#       settle. Flat sizing bounds the total-loss variance (compounding the fade
#       over-bets — see memory).
#   Maker: the fill PROBE stays (reversal detector), maker EXECUTION + P&L go.
#   P&L: BOTH books record NET OF the 0.07*p*(1-p) taker fee (both legs).
TWO_BOOK = os.getenv("POLYMARKET_TWO_BOOK", "0").strip().lower() in (
    "1", "true", "yes", "on")
# Books whose economics are a TAKER BUY HELD TO SETTLE: a win redeems on-chain at
# $1/share (no exit fee), a loss goes to 0, and the taker fee is paid on the ENTRY
# only. ONE list, used everywhere those economics are applied — fee-net P&L via
# taker_pnl(), the on-chain redeem enqueue, and the exit accounting in close_leg.
# It is a single list because every book has been added late and each time one of
# the tuples was missed: oppfade (fixed 2026-07-21 — understated the cash loss by
# the entry fee, ~6% of stake, DB $10.00 vs $10.63 actually debited) and swingfade
# (fixed 2026-07-25 — same fee gap, AND it was never enqueued for redeem, so a
# winning live leg relied entirely on Polymarket's auto_redeem_operator).
# ADD ANY NEW HOLD-TO-SETTLE BOOK HERE, not at the call sites.
TAKER_HOLD_BOOKS = ("t3", "flip", "oppfade", "swingfade",
                    "swingband_21_25", "swingband_25_30",
                    "swingband_30_40", "swingband_40_50", "twap_lock",
                    "twap_edge", "pm_t240_probe")
# DATA-ONLY collection mode (): trade NOTHING, record NO P&L,
# but keep the PM feeds flowing — BookFeed (bid/ask) + the maker-fill/reversal
# shadow probe + the T+3 evaluation legs (direction accuracy + observed asks) +
# Chainlink open/settle. Used while the two-book execution is being implemented
# so the calibration dataset keeps growing without the old maker trades.
DATA_ONLY = os.getenv("POLYMARKET_DATA_ONLY", "0").strip().lower() in (
    "1", "true", "yes", "on")

# Deployment kill switch: unlike POLYMARKET_DRY_RUN, this cannot be overridden by
# a dashboard `live_mode` row. Use it while BTC/ETH PM-rebroadcast results are
# being observed before either asset is allowed to place real orders.
FORCE_SHADOW = os.getenv("POLYMARKET_FORCE_SHADOW", "0").strip().lower() in (
    "1", "true", "yes", "on")
if FORCE_SHADOW:
    DRY_RUN = True

# HARD SHADOW LOCK for non-BTC assets (2026-07-14): BTC goes live first;
# every other asset (currently ETH) stays SHADOW regardless of the
# `<ASSET>:live_mode` config or a dashboard go-live toggle, until that asset's
# feed + go-live is explicitly approved. This is belt-and-suspenders over the
# service env (POLYMARKET_FORCE_SHADOW) so a stray config row can never put
# ETH into real orders. Lift per-asset by setting POLYMARKET_ALLOW_LIVE=1 in that
# asset's own service unit.
_ALLOW_NONBTC_LIVE = os.getenv("POLYMARKET_ALLOW_LIVE", "0").strip().lower() in (
    "1", "true", "yes", "on")
if ASSET != "BTC" and not _ALLOW_NONBTC_LIVE:
    DRY_RUN = True

T3_STAKE_FRAC = float(os.getenv("POLYMARKET_T3_STAKE_FRAC", "0.25"))    # compounded
FADE_STAKE_FRAC = float(os.getenv("POLYMARKET_FADE_STAKE_FRAC", "0.125"))  # compounded
# SEPARATE fade bankroll (2026-07-11): the fade now has its OWN capital that
# COMPOUNDS off its own realized P&L, decoupled from the t3/main pool — so the
# fade's total-loss variance can't touch the t3 bankroll and vice-versa. Sized at
# FADE_STAKE_FRAC of the growing fade bankroll (12.5% ≈ half-Kelly at the reachable
# opposite-ask band — captures most of the growth without the ruin risk of
# over-betting a book whose losses are -100% of stake).
# The dash-set bankroll (get_capital()) is the TOTAL; it is SPLIT across THREE
# books by FADE_CAPITAL_SHARE + FLIP_CAPITAL_SHARE (2026-07-13: "the fattest
# book we have is for flips — plug the third book and allocate 1/3 the capital to
# each"). Default 1/3 fade, 1/3 flip, 1/3 t3 (the remainder). So a $300 dash
# bankroll → t3 $100, fade $100, flip $100. (Was 2/3 t3, 1/3 fade before the flip
# book was plugged in — t3's real sizing therefore drops from 2/3 to 1/3.)
# DEFAULTS CORRECTED 2026-07-15 (caught it: "25% of 40 is 10", but the card
# modelled $4.17). The 1/3-each defaults above are STALE — the shipped allocation is
# 80/10/10 (flip 80 / fade 10 / t3 remainder 10, see
# [[project_pm_book_recalibration_allocation]]), set as env on the BOT unit only.
# The DASHBOARD unit sets nothing but DASH_PORT, so it fell back to these defaults
# and ran the OLD 1/3 split: flip notional $16.67 vs the bot's $40, i.e. the card
# modelled a $4.17 stake while the live book would place $10 — on the SAME card that
# prints "cap $40" (published by the bot). Defaults now match the shipped split so a
# process with no env is CORRECT rather than silently 2.4x light.
# NOTE the env on the bot unit still wins; if you retune the split, set it on BOTH
# units or neither, or the model and the live book diverge again.
# FADE book share — RETIRED to 0.00 on 2026-07-20 (the whole bankroll goes to
# the cheap-opp fade). The fade is the most-negative book on this market at every
# route tested (taker -0.07(1-a) at cheap ask, maker 0/16, live -$105); it was already
# scheduled to drop regardless of how the flip read landed.
FADE_CAPITAL_SHARE = float(os.getenv("POLYMARKET_FADE_CAPITAL_SHARE", "0.0"))
# T+3 share — now EXPLICIT and RETIRED to 0.00 (). It used to be the
# remainder (1 - FADE - FLIP), which meant zeroing the other books would have silently
# handed t3 the ENTIRE bankroll — the exact opposite of the instruction. Shares are now
# independent and must be read as absolute fractions: oppfade 1.00, everything else 0.
T3_CAPITAL_SHARE = float(os.getenv("POLYMARKET_T3_CAPITAL_SHARE", "0.0"))
# FLIP book share — RETIRED to 0.00 on 2026-07-20 () after the pre-registered
# OOS read (logs/re_flip_oos_read.log, cutoff 07-19). On the BTC-only line the staked
# flip rule scored +0.6σ / n=17 / +$2.52 against a CONTROL (always-flip, no price
# selection) of +0.8σ / n=80 — i.e. BELOW the zero-line — and the 0.40-0.60 band arm
# was +0.1σ / -$1.79. Every strong number in that log is ETH, which is
# Binance-fallback scored (~3s look-ahead) and unfundable. No BTC evidence of edge,
# so the book stops carrying capital. The flip DETECTION path is untouched and MUST
# stay on — _re_flip_shadow is what generates the cheap-opp fade's entries.
# REVERT: POLYMARKET_FLIP_CAPITAL_SHARE=0.80 (and re-cut OPPFADE back to 0.70).
FLIP_CAPITAL_SHARE = float(os.getenv("POLYMARKET_FLIP_CAPITAL_SHARE", "0.0"))
# Per-flip stake as a fraction of the flip book's notional (mirrors the dashboard
# shadow model's FLIP_STAKE_FRAC). Only used when the flip book is toggled LIVE
# (`flip_live`) — a real BUY per qualifying flip sized off flip_capital(); the
# flip book stays SHADOW (this is unused) by default.
# STAKE RAISED 0.125 -> 0.25 on 2026-07-15 ("the only bread earner right now",
# read off the flip card AFTER the 0.30 cap went in). Recording the counter-evidence,
# because stake_frac only MULTIPLIES an edge — it never creates one:
#   * The card's +$12.11 is the MODELLED book: 2 shadow legs (07-14 14:34 @0.28 and
#     20:44 @0.23), both winners. It is not dollars — see
#     [[feedback_pm_logged_stake_not_real_dollars]].
#   * REAL flips, all 7 ever placed: -$0.80 (4W/3L). REAL flips passing the 0.30 cap:
#     ONE trade (07-14 15:44 @0.30, $5) — a SETTLE_LOSS of -$5.25. So at this exact
#     cap the live book's entire record is -$5.25 while the model shows +$12.11.
#   * The +$12.11 only exists because the cap was set to 0.30 the same hour, off a
#     7-cell sweep whose 0.30 row is n=2. Fitted number -> recommends strategy.
# Modelled flip stake goes $5 -> $10/trade (0.25 x $40 notional); live would be
# flip_base x 0.25 (~$13). All books are SHADOW today (flip_live=0), so no capital
# moves. THE DANGEROUS STEP IS SWITCHING flip_live=1 — at that point this doubles
# real exposure on a cohort with one real trade, which lost.
# REVERT: POLYMARKET_FLIP_STAKE_FRAC=0.125 in the unit, or edit the default back.
FLIP_STAKE_FRAC = float(os.getenv("POLYMARKET_FLIP_STAKE_FRAC", "0.25"))
# CHEAP-OPP FADE book — SHADOW-ONLY (pre-registered 2026-07-16, frozen for a clean
# OOS read by 2026-07-20; see [[project_pm_cheap_opp_fade_prereg]]). The dashboard
# reads the frozen cohort from polymarket_updown_re_flip_shadow and, at the owner's
# request (2026-07-17), sizes each shadow bet with the SAME 24h-daily-compounding
# rule as the flip/fade books instead of the pre-reg's $1-flat. The ENTRY rule
# (which windows qualify + win/loss) is untouched, so the frozen statistical read
# — scripts/pm_cheap_opp_fade_oos.py, still $1-flat — is unaffected; only the
# displayed stake/P&L compounds. Defaults give a $1 first-day bet (100×0.10×0.10)
# for continuity with the pre-reg scale. Never risks real capital.
# CAPITAL_SHARE 0.10 -> 0.70 (07-17, split with the band) -> 1.00 (2026-07-20:
# "the entire cap is given to opp fade <= 0.16 ask"). This is now the ONLY book with
# capital: t3 / fade / flip are all 0.00 and the [0.25,0.30] band is deleted. Sizing
# is unrelated to the statistical read (scripts/pm_cheap_opp_fade_oos.py stays $1-flat
# — only the SIZE compounds here, never the entry rule).
OPPFADE_CAPITAL_SHARE = float(os.getenv("POLYMARKET_OPPFADE_CAPITAL_SHARE", "1.00"))
OPPFADE_STAKE_FRAC = float(os.getenv("POLYMARKET_OPPFADE_STAKE_FRAC", "0.10"))
# Fraction of the 00:00-UTC exchange capital that is USABLE for sizing (
# 2026-07-22): we deploy 2/3 of the wallet and hold 1/3 back. The 10% stake frac
# applies to this usable slice, so effective stake = daily_capital * 2/3 * 10%.
OPPFADE_USABLE_FRAC = float(os.getenv("POLYMARKET_OPPFADE_USABLE_FRAC", "0.6667"))
# Cohort qualify rule. 0.15 -> 0.16 on 2026-07-19, and on 07-20 the owner collapsed the
# last 0.15 (the scorer) onto it: "remove all 0.15 and change it to 0.16, let us not
# have confusion". ONE number across bot + dashboard + scorer — keep them in step.
# The pre-registration ran at 0.15 and its verdict (p=0.02, n=67) is recorded in the
# scorer's PROVENANCE docstring; 0.16 was picked after seeing the data and moves the
# edge -0.3pp, so it is the operating rule, NOT the pre-registered one.
OPPFADE_ASK_MAX = float(os.getenv("POLYMARKET_OPPFADE_ASK_MAX", "0.16"))
# Forward shadow candidate armed 2026-08-02 after the first 74 settled shadow
# entries: <=0.10 scored 1/32, while 0.10-0.16 inside 220-250s scored 4/18 and
# stayed 2/9 in each chronological half. Strictly greater than the floor.
OPPFADE_ASK_MIN = float(os.getenv("POLYMARKET_OPPFADE_ASK_MIN", "0.10"))
# RETRY-ON-KILL limit (2026-07-21). A FAK kill is NOT a neutral miss: over
# 176 cohort windows the killed cell wins 38.5% vs 16.4% for the cell that fills
# — the exchange refuses us precisely when the reversal is real. So on a kill we
# immediately re-fire ONCE at this wider limit.
#   Why 0.20 and not the 0.25/0.35 the ask eventually reaches: the round trip is
#   1.65s (15 live fills, range 1.55-1.79), so the retry lands ~3.3s after the
#   signal. At <=3s the winner ask is a median 0.140 (5/5 still <=0.16); at 4-5s
#   it is 0.170. The run to 0.29+ is a 13s+ phenomenon we never see. Sweeping the
#   limit 0.18 -> 0.30 on the 3.3s book adds 7 windows and ZERO extra winners
#   while the average fill worsens 3c — every cent above ~0.20 buys losers only.
# RAISED 0.20 -> 0.23 (2026-07-21) after the FIRST retry ever fired died
# anyway: 07-21 16:54, kill at cap 0.16, retry at 0.20 refused with the ask
# already at 0.21 — and that window WON. The sweep above says +EV stops ~0.20,
# but it measured the ask at 3.3s; a retry that arrives to find 0.21 has no
# chance at 0.20, so the choice is 0.23 or no retry at all on a fast book.
# WATCH THE EV BAR: a fill at 0.25 needs a 25% win rate to break even, above the
# 17-20% the main cohort carries. It is only justified because the KILLED cell is
# a different, better cohort (38.5% historically; 2 of 4 live kills won) — if
# retry legs stop clearing ~25% they are pure leakage. retry_of_kill=TRUE keeps
# them separable; never blend them into the +8.9pp read.
#
# 0.20 -> 0.23 -> 0.25 (2026-07-22). The 0.20 setting demonstrably cost a
# winner: leg 26151 (07-21 16:54) was killed with the ask at 0.21 — short by ONE
# CENT — and that window settled a WIN at 6.7x. Journal:
#   "OPPFADE-RETRY ... kill@0.16 -> re-fire @0.20 (ask at kill 0.21) -> KILLED AGAIN"
# Live retry population is still only n=3 (1 win), so 0.25 is a judgement call on a
# documented near-miss, NOT a measured result. Score it via the pre-registration
# below before moving it again.
# Set to 0 (or <= OPPFADE_ASK_MAX) to disable the retry entirely.
# NOT PRE-REGISTERED and NOT part of the validated +8.9pp cohort, which is defined
# by ask<=0.16. Retry legs carry retry_of_kill=TRUE — every read of this book MUST
# split on it or the two cohorts get silently blended.
OPPFADE_RETRY_MAX = float(os.getenv("POLYMARKET_OPPFADE_RETRY_MAX", "0.25"))
OPPFADE_ADX_ABS_MAX = float(os.getenv("POLYMARKET_OPPFADE_ADX_ABS_MAX", "3.5"))
# --- ASYMMETRIC ADX-SLOPE GATE + FLAT-BAND + RETRACE CAP (2026-07-22) ---
# adx_slope is the signed per-minute LSQ slope of ADX(7) across the window (see
# adx_slope()). ADX is direction-agnostic strength, so its slope carries exactly one
# bit: strengthening or exhausting. We BUY the opp side = a fade, so the two tails
# mean OPPOSITE things and the old symmetric |slope| < 3.5 was wrong by construction
# — it lumped the best cell in with the worst:
#   steeply FALLING adx = the move that made our side cheap is dying -> fade works
#                         (-3.35..-2.53 bucket: 38.5% WR, +$273 on flat $10)
#   RISING adx          = move accelerating into settle -> cheap side stays cheap
#                         (+1.57..+3.50: 7.7%/7.1% WR, -$59/-$78; largest avg peak
#                          excursion 6.0bps — the acceleration showing up directly)
# So: cap the TOP, leave the BOTTOM open (MIN keeps the old -3.5 as a sanity floor).
# BTC, 192 shadow windows 07-12..07-22, one stake per window, flat $10, no fee:
#   baseline |slope|<3.5              192 stakes  31 wins  16.1%  +$1050
#   +slope<1.4, retrace<5.5           155         29       18.7%  +$1287
#   +drop flat band |slope|<=0.5      126         26       20.6%  +$1360
# CAVEATS, read before trusting these numbers:
#  * IN-SAMPLE. 10 days, and the thresholds were drawn where THIS sample breaks.
#    The asymmetry has a mechanism; 1.4 does not — it is a fitted boundary.
#  * The FLAT BAND is the weakest of the three. Edge is NOT monotonic in slope
#    (-0.49..+0.48 = 9.4% WR / -$103, but +0.51..+1.48 = 17.9% / +$118), and a hole
#    punched in the middle of a distribution is the classic overfit shape. It is a
#    32-row cell. Kept behind its own var so it can be switched off alone.
#  * Set POLYMARKET_OPPFADE_ADX_FLAT_BAND=0 to disable the flat cut, or
#    POLYMARKET_OPPFADE_ADX_SLOPE_MAX=3.5 to revert to the old symmetric gate.
# JUDGE FORWARD on the shadow rows (adx_slope/retrace_frac are logged on every one).
OPPFADE_ADX_SLOPE_MAX = float(os.getenv("POLYMARKET_OPPFADE_ADX_SLOPE_MAX", "1.4"))
OPPFADE_ADX_SLOPE_MIN = float(os.getenv("POLYMARKET_OPPFADE_ADX_SLOPE_MIN",
                                        str(-OPPFADE_ADX_ABS_MAX)))
# |slope| <= this is skipped as "no trend signal either way". 0 disables the cut.
OPPFADE_ADX_FLAT_BAND = float(os.getenv("POLYMARKET_OPPFADE_ADX_FLAT_BAND", "0.5"))
# retrace_frac cap. Median is 1.70 and p95 is 4.80, so 5.5 binds on ~3% of rows
# (6 of 192, all losers) — near-inert by design, a tail guard not a selector.
OPPFADE_RETRACE_MAX = float(os.getenv("POLYMARKET_OPPFADE_RETRACE_MAX", "5.5"))
# --- LATE-WINDOW CUTOFF (2026-07-22): no stake in the last 35s ---
# Win rate decays monotonically with elapsed time and the tail is where it goes
# negative. Under the live gate, BTC+ETH shadow 07-12..07-22, flat $10:
#   elapsed 3:30-3:44   7 stakes  28.6% WR   +$59
#           3:45-3:59  32         25.0%     +$259
#           4:00-4:14  80         12.5%     +$272
#           4:15-4:29  92         10.9%     +$324
#   > 4:25 (the cut)   26 stakes   7.7% WR   -$86   (BTC 12: 8.3%/-$29; ETH 14: 7.1%/-$57)
# 4:25 is where the DISCARDED bucket turns negative — earlier cutoffs keep a higher
# WR but throw away real money (a 4:00 cut discards +$626). The decay is gradual, so
# this is a tail cut, not a cliff: it buys +$86 for 26 fewer stakes and 2 winners.
# Independent support: the Binance monitor disables at 35s left on a decisive move —
# same 35s tail, arrived at separately. n=26 in the cut bucket, so this is thin;
# both assets agree on the sign, which is the reason to take it.
OPPFADE_MIN_ELAPSED_S = int(os.getenv("POLYMARKET_OPPFADE_MIN_ELAPSED_S", "220"))
OPPFADE_MAX_ELAPSED_S = int(os.getenv("POLYMARKET_OPPFADE_MAX_ELAPSED_S", "245"))


# =====================================================================
# SWINGFADE — the swing-gated cheap-side book (2026-07-23)
# =====================================================================
# A SEPARATE staking category from `oppfade`, not a re-tune of it. Same primitive
# (buy the cheap side, hold to settle) but a different entry rule, a different ask
# cap and its own sizing ceiling, so the two must never be blended in any read.
#
# Entry rule = the six filters the owner applied to reports/pm_leg1_anytime_dedup.xlsx,
# scored on 3,193 BTC windows 07-11..07-23 (leg-1 only, no second leg):
#     elapsed 120-220s · ask 0.03-0.235 · crossings-of-open != 2
#     · up_excursion <= 8bps · dn_excursion <= 8bps · |adx_slope| <= 5
# n=520 at ask<=0.30 -> WR 28.7%, +$922 flat-$10. Restricted to ask<=0.235 (so a
# $50 order survives the 0.25 FAK cap) n=135, WR 21.5%, mean ask 0.164, 100% fill.
#
# *** PROVENANCE — READ BEFORE TRUSTING ANY NUMBER FROM THIS BOOK ***
# Every threshold above was chosen AFTER looking at these same 3,193 windows, in a
# spreadsheet, by eye. There is NO out-of-sample test and NO independent test set:
# ETH cannot serve as one because our ETH feed is Binance while PM settles on
# Chainlink (see feedback_never_analyse_eth). The strongest single filter is the
# elapsed band, which on its own is +$444 of the +$922 on 2.8x the trades; the
# crossings!=2 cut is a 69-trade cell whose neighbours (0,1,3) flip sign, i.e. it
# has no monotone shape and is most likely noise — it is kept because the owner chose
# it deliberately, behind its own var so it can be switched off alone.
# THIS BOOK IS A FORWARD TEST, NOT A VALIDATED EDGE. Judge it on rows written
# from 2026-07-23 onward and nothing earlier.
# Live forward rule tightened 2026-08-09: keep the asymmetric cheap-token payoff,
# but do not fund the 0.21-0.235 tail.  The gate and the FAK limit intentionally
# share this value, so a qualifying quote cannot execute outside the scored band.
SWINGFADE_ASK_MAX = float(os.getenv("POLYMARKET_SWINGFADE_ASK_MAX", "0.21"))
# Ask floor 0.15 both assets (2026-09-13). BTC: live era 4/7 +$15.95 at
# >=0.15 vs 0/12 -$12.76 below. ETH epoch tape inverts the old 0.03 collector:
# 0.03-0.08 is two 15:1 lottery wins hiding the dump; 0.08-0.15 is 0/12;
# 0.15-0.21 is the book (11 trades, +$21.70 modelled). ETH live took the
# 0.05-0.12 half and lost. Do not reopen 0.03-0.12.
SWINGFADE_ASK_MIN = float(os.getenv("POLYMARKET_SWINGFADE_ASK_MIN", "0.15"))
# One FAK, no retry/refill. Pin to the live CLOB touch (see fade_place).
# A killed order stays NO_FILL and a partial fill is kept as-is.
SWINGFADE_RETRY_MAX = SWINGFADE_ASK_MAX
# The leak is FAK-at-band-ceiling eating dumped rungs, not the hold-to-settle
# exit. ETH live quoted 0.12 and filled 0.17 on the only winner — that walk
# turned the 6 live tickets from +$1.96 at quote to -$0.49. Same touch+1c
# pin as BTC. Rollback: POLYMARKET_FADE_PIN_TOUCH=0.
FADE_PIN_TOUCH = os.getenv(
    "POLYMARKET_FADE_PIN_TOUCH", "1",
).strip().lower() not in ("0", "false", "no", "off", "")
FADE_TOUCH_PAYUP = float(os.getenv("POLYMARKET_FADE_TOUCH_PAYUP", "0.01"))
# 200, not 180. On $1 tickets, 180-199s is the first-flip dump: post-08-15 that
# bucket is 0/8 and -$8.46. 200-220s keeps the convex 0.15-0.21 winners
# (5-7, +$14.66). The RE window still opens at ~181s; we just do not stake the
# first 19s of it. Rollback: POLYMARKET_SWINGFADE_ELAPSED_MIN=180.
# WHAT THIS COSTS: of the 135 filtered backtest entries, 72 (53%) fall at 120-179s
# and are NOT reachable today. Splitting them:
#     120-179s : n=72  WR 18.1%  +$93   (mean ask 0.181)
#     180-220s : n=63  WR 25.4%  +$593  (mean ask 0.145)
# The reachable half is the stronger one and carries 86% of the P&L, so nothing
# important is lost — but do NOT read that as the rule being validated at 180+;
# the split was seen after the fact. To reach 120-179s the gate has to be wired
# into _t2_probe as well (it runs 0-179s and already has ask_up/ask_down; the
# excursion and adx inputs are sampled from window open so they are available
# there too). That is a separate change, deliberately not made blind.
# ETH retained-tape review 2026-08-30: narrow the shadow collector to 200-205s
# (6 settled fires, 3-3, 50.0% WR, +$22.39 fee-adjusted model P&L at $1).
# BTC historical real-trade review 2026-08-30: use 201-206s (11 settled real
# fires, 4-7, 36.4% WR, +$9.83 fee-adjusted P&L normalized to $1). This timing
# pocket did not reproduce in the smaller shadow cohort, so keep monitoring it.
# BTC LIVE 2026-09-17: elapsed 200-215 via systemd drop-in
# polymarket-updown-bot.service.d/31-swingfade-elapsed.conf (code default
# below is still 201-206 for rollback). Shadow replay of swingfade_ok on
# re_flip_shadow, $1 HTS: 201-206 cal n=4 +$2.43 / post-restart n=0;
# 200-215 cal n=9 +$9.27, chrono H1+$6.84/H2+$6.14, post-restart picks the
# t=208s ask-0.21 winner. ETH MUST stay 200-205 — widening 200-220 decays
# H2 and ETH's one live fire was NO_FILL, not a clock miss.
# See reports/PM_LIVE_GATES_2026-09-17.md.
SWINGFADE_ELAPSED_MIN = int(os.getenv(
    "POLYMARKET_SWINGFADE_ELAPSED_MIN", "201" if ASSET == "BTC" else "200"))
SWINGFADE_ELAPSED_MAX = int(os.getenv(
    "POLYMARKET_SWINGFADE_ELAPSED_MAX", "206" if ASSET == "BTC" else "205"))
# BTC $1 tickets with a pre-band min ask >=0.08 were 6-12 and +$15.54.
# ETH first-fires have no such signature and the floor empties the book; off.
SWINGFADE_MIN_PRE_ASK = float(os.getenv(
    "POLYMARKET_SWINGFADE_MIN_PRE_ASK", "0.08" if ASSET == "BTC" else "0"))
# BTC's symmetric 8bps excursion cap was selected on the BTC basis sheet.  It
# does not transfer to ETH: the settled ETH sweep through 2026-08-25 has no
# breakpoint at 8bps (10-15bps score better and every cap is red in H1).  Zero
# means the symmetric cap is disabled.  This is deliberately asset-specific;
# the independently selected excursion fields in ETH SWINGBAND_SPECS remain on.
SWINGFADE_EXC_MAX = float(os.getenv(
    "POLYMARKET_SWINGFADE_EXC_MAX", "8.0" if ASSET == "BTC" else "0.0"))
# --- FIRST-TOUCH condition (retired 2026-08-08) -----------------------------
# The basis sheet's "elapsed 120-220s" was NEVER a time filter. Its rows are the
# FIRST tick in the window whose cheap ask <= 0.30, one per window, so keeping the
# 120-220s ones ALSO requires that no cheap ask existed BEFORE the band. The live
# gate only ever asked "is the ask in band now", which is a 17.4x larger and
# completely different population — that gap, not any filter, is why the book was
# staked at a measured 21.5-25.4% WR and has traded at ~12%.
# Reconstructed on re_flip_shadow through this same swingfade_ok(), settled on
# Chainlink, one row per window, day-clustered:
#     FIRST-TOUCH ON   IS n=7  WR 57.1%  | OOS n=10 WR 50.0% | ALL n=17 WR 52.9%
#     FIRST-TOUCH OFF  IS n=41 WR 14.6%  | OOS n=17 WR  5.9% | ALL n=58 WR 12.1%
# against a ~17% breakeven. ALL t=1.98, binomial p=0.0006; the OFF side is what the
# book has actually been trading. Edge is monotone in the ceiling (0.20 -> +8.5pp,
# 0.25 -> +13.0pp, 0.30 -> +36.4pp), flat in the density floor, and beats
# same-keep-rate control cuts on ask (+12.0pp), adverse (+6.6pp) and elapsed (+0.6pp),
# so it is not a proxy for any of them. See project_swingfade_basis_reconciliation.
# *** n=17 total / n=10 OOS. This is NOT a validated edge — it is the ONE condition
# that survived reconciling the basis, and it holds its sign out of sample, which
# the basis never did. The case for arming it rests at least as much on the OFF side
# being clearly -EV as on the ON side being +EV. Judge at n>=60. ***
# Corrected, coverage-safe replay through 2026-08-08 reversed the original read:
# the actually observable first-touch cohort was 1/4, while FIRST-TOUCH OFF scored
# 19/86 before 08-02 and 4/16 forward against ~18% breakeven.  The old 17-row/52.9%
# result was dominated by pre-band coverage holes.  Default OFF; a config value can
# still restore it for research, but production deliberately sets that value to 0.
SWINGFADE_FIRST_TOUCH_CEIL = float(
    os.getenv("POLYMARKET_SWINGFADE_FIRST_TOUCH_CEIL", "0"))        # 0 disables
# Guard against re-importing the basis's OWN fatal bug: it read "no cheap tick before
# the band" off a tick series that simply had no rows there (t2probe stops at 179s),
# so 66% of its staked cohort was a coverage hole, not a price path. Live can only
# assert the absence if it actually WATCHED — require this many pre-band polls, else
# fail closed. _t2_probe runs from T+0 while flat, so a normal window sees ~60.
SWINGFADE_FIRST_TOUCH_MIN_TICKS = int(
    os.getenv("POLYMARKET_SWINGFADE_FIRST_TOUCH_MIN_TICKS", "20"))
# --- ADVERSE-ONLY excursion cap (2026-07-28, DEFAULT OFF) --------------
# The 8bps cap above is SYMMETRIC: it applies the same number to the excursion
# running AGAINST the side we buy and to the earlier swing the other way. Measured
# 2026-07-28 on 4,456 BTC windows those two legs behave oppositely — WR falls
# monotonically with the ADVERSE excursion (37.1% at <2bps -> 9.0% at 12-20bps) and
# RISES with the opposite one (21.9% -> 29.0%) — so one number cannot be right for
# both. On the 108 armed shadow windows (all other live filters on), replacing the
# symmetric cap with `adverse <= 4` moves WR 26.4% -> 29.7% and edge-over-breakeven
# +7.5pp -> +10.6pp while keeping 69% of fires; the opposite leg is inert (opp<=4/6/8
# all score the same as no gate). Controls: a same-keep-rate ask cut gets only
# +3.6pp, corr(adverse, ask) is just -0.35, and the second half of the sample is the
# stronger one. See [[project_pm_swingfade_excursion_axis]].
# *** THIS THRESHOLD WAS CHOSEN IN-SAMPLE over ~10 variants on n=108. It is NOT a
# validated edge. On the 19 real staked legs it would have blocked 4 — all losers,
# saving $41.28 of the -$108.48 — but a random 4-window block hits only losers 35%
# of the time, so that is support, not proof. ***
# 0 (default) = OFF, symmetric cap only. Dash config `swingfade_adverse_max` wins
# over the env var and is read every tick, so it is reversible with no restart.
# Tightened 4 -> 2 on 2026-08-08. With first-touch removed, adverse<=2 scored
# 7/24 historically and 3/6 forward versus ~18-20% breakeven; the 2-4bps remainder
# decayed to 1/10 forward. Dashboard config remains the runtime source of truth.
SWINGFADE_ADVERSE_MAX = float(os.getenv("POLYMARKET_SWINGFADE_ADVERSE_MAX", "2"))
SWINGFADE_ADX_ABS = float(os.getenv("POLYMARKET_SWINGFADE_ADX_ABS", "5.0"))
# Crossing counts to REFUSE, comma-separated. the owner's spreadsheet unticked 2.
# Set to "" to disable the cut entirely (recommended if it ever reads as noise).
SWINGFADE_CROSS_EXCLUDE = frozenset(
    int(x) for x in os.getenv("POLYMARKET_SWINGFADE_CROSS_EXCLUDE", "2").split(",")
    if x.strip().isdigit())
SWINGFADE_USABLE_FRAC = float(os.getenv("POLYMARKET_SWINGFADE_USABLE_FRAC", str(2/3)))
SWINGFADE_STAKE_FRAC = float(os.getenv("POLYMARKET_SWINGFADE_STAKE_FRAC", "0.10"))
# HARD per-trade ceiling. The 07-19 depth probe puts median size at the touch at
# ~$16 and only 77% of windows fully fillable at $50 — above that the sweep walks
# the book faster than the edge can pay for. Sizing is bank x 2/3 x 10% = bank/15,
# which survived 2,000 order-reshuffles at 0% ruin; bank/9 blew up to -99%.
SWINGFADE_STAKE_CEIL = float(os.getenv("POLYMARKET_SWINGFADE_STAKE_CEIL", "50"))

# --- PRICE-BAND SWING SHADOW BOOKS (2026-08-02) ----------------------------
# Four price-band books above Swing Fade's hard 0.21 cap. They began as
# forward-only research books, but each now has an independent dashboard LIVE
# switch. The gates were shortlisted on the 07-24..08-01
# 2,592-window sample. Bounds are non-overlapping: existing swingfade owns <=0.21;
# each band
# owns (lo, hi].  A shared per-window latch permits at most one of these four books
# to fire, preventing a falling ask from stacking correlated tickets in several
# bands.  On a single tick specs are evaluated cheapest-first.
# ETH is an all-shadow parity run whose other modelled entries are $1 tickets.
# Keep its band books on that same notional so the combined shadow P&L is an
# apples-to-apples strategy total. BTC retains the original $10 forward-test
# notional unless explicitly overridden.
SWINGBAND_SHADOW_STAKE_USD = min(float(os.getenv(
    "POLYMARKET_SWINGBAND_SHADOW_STAKE_USD", str(UNIFORM_STAKE_USD))),
    UNIFORM_STAKE_USD)
# ETH band specs (2026-08-19 oracle) are NOT BTC's. Replay of 2,417 settled
# ETH flip rows, first-match per window, $1 fee model, both chronological
# halves green:
#   21-25  190-210s  0.5<|slope|<=2          n=32 12-20 +$17.26
#   25-30  190-200s  0.5<|slope|<=2          n=28 12-16 +$12.27
#   30-40  200-210s  1<|slope|<=2 and slope<=0 (weakening)  n=36 21-15 +$20.90
#   40-50  190-210s  slope<=0, |slope|<=8, other<=8        n=20 12-8  +$5.64
# Shared cheapest-first latch of those four: n=89-class portfolio, both halves
# green. BTC 21-25 t1 cut 210 -> 205 on 2026-09-13: epoch first-fire 200-205s
# is 4/4-class +$9.22, 206-210s is 1/5 -$1.27. ETH 21-25 stays 190-210 (and
# remains SHADOW — 0/4 this epoch).
SWINGBAND_SPECS = (
    dict(book="swingband_21_25", lo=0.21, hi=0.25,
         t0=(200 if ASSET == "BTC" else 190),
         t1=(205 if ASSET == "BTC" else 210),
         slope=(5.0 if ASSET == "BTC" else 2.0),
         slope_min=(0.0 if ASSET == "BTC" else 0.5),
         slope_hi=None, cross_exclude=frozenset({2}),
         adverse=None, other=None),
    dict(book="swingband_25_30", lo=0.25, hi=0.30, t0=190,
         t1=(198 if ASSET == "BTC" else 200),
         slope=2.0,
         slope_min=(0.0 if ASSET == "BTC" else 0.5),
         slope_hi=None,
         cross_exclude=frozenset({2}),
         adverse=None, other=None),
    dict(book="swingband_30_40", lo=0.30, hi=0.40,
         t0=190,
         t1=(200 if ASSET == "BTC" else 210),
         slope=(2.0 if ASSET == "BTC" else 5.0),
         slope_min=(0.0 if ASSET == "BTC" else 0.5),
         slope_hi=(0.0 if ASSET == "BTC" else None),
         cross_exclude=frozenset({2}),
         adverse=4.0,
         other=None),
    dict(book="swingband_40_50", lo=0.40, hi=0.50, t0=190,
         t1=(200 if ASSET == "BTC" else 210),
         slope=8.0,
         slope_min=(2.0 if ASSET == "BTC" else 0.0),
         slope_hi=(None if ASSET == "BTC" else 0.0),
         cross_exclude=frozenset({2}),
         adverse=8.0,
         other=(12.0 if ASSET == "BTC" else 8.0)),
)


def swingband_shadow_match(*, elapsed_s, ask, adx_slope, crossings,
                           up_exc_bps, dn_exc_bps, flip_side,
                           min_pre_ask=None, vol_3min_bps=None):
    """Return the first matching SWINGBAND_SPECS item, else ``None``.

    Pure/fail-closed so forward SQL replays and the live collection path can share
    exact semantics.  These books buy opposite the flip; adverse excursion is the
    flip-side excursion and ``other`` is the opposite path leg.
    """
    _mc_ok, _ = mc_allows("fade", vol_3min_bps)
    if not _mc_ok:
        return None
    vals = (elapsed_s, ask, adx_slope, crossings, up_exc_bps, dn_exc_bps)
    try:
        if any(v is None or not math.isfinite(float(v)) for v in vals):
            return None
    except (TypeError, ValueError):
        return None
    if flip_side not in ("Up", "Down"):
        return None
    adverse = up_exc_bps if flip_side == "Up" else dn_exc_bps
    other = dn_exc_bps if flip_side == "Up" else up_exc_bps
    for spec in SWINGBAND_SPECS:
        if not (spec["lo"] < ask <= spec["hi"]):
            continue
        if not (spec["t0"] <= elapsed_s <= spec["t1"]):
            continue
        if abs(adx_slope) > spec["slope"]:
            continue
        # Optional lower absolute-slope bound is strict: slope_min=2 means
        # 2 < |slope| <= slope. Optional directional ceiling supports a
        # weakening-trend-only book (30-40 BTC: signed slope <= 0).
        if spec.get("slope_min", 0) and abs(adx_slope) <= spec["slope_min"]:
            continue
        if spec.get("slope_hi") is not None and adx_slope > spec["slope_hi"]:
            continue
        if crossings in spec["cross_exclude"]:
            continue
        if spec["adverse"] is not None and adverse > spec["adverse"]:
            continue
        if spec["other"] is not None and other > spec["other"]:
            continue
        # Same dumped-longshot score as swingfade: a band fire on a token that
        # already printed 2c before the band is a bounce, not a 0.21-0.50 fade.
        if SWINGFADE_MIN_PRE_ASK > 0:
            if min_pre_ask is None or (
                    isinstance(min_pre_ask, float) and not math.isfinite(min_pre_ask)):
                continue
            if min_pre_ask < SWINGFADE_MIN_PRE_ASK:
                continue
        return spec
    return None
# --- STAKE FLOOR + COMPOUNDING GATE (2026-07-26) -----------------------
# The book bets a FLAT $10 ticket and does NOT compound until it has grown past
# its own seed capital. Two reasons this is not the plain % sizer:
#   1. bank x 2/3 x 10% on a $57 daily cap prints a $3.80 ticket — too small to
#      pay for PM's 7% fee plus the touch spread, so the book bleeds on size
#      alone regardless of whether the entry rule has edge.
#   2. compounding a balance that is still UNDER its seed is compounding a
#      drawdown: it shrinks stake exactly when the book needs to earn back, and
#      the recovery becomes arithmetically impossible.
# So: below the seed the stake is PINNED at the floor (never the formula, even
# if the formula would pay more); at or above it the daily 00:00-UTC snapshot
# compounds normally under the capacity ceiling. The gate reads the DAILY-LOCKED
# capital, never the live balance, so the stake steps at midnight and holds all
# day — an intraday deposit or drawdown cannot resize a ticket mid-window.
SWINGFADE_MIN_STAKE = min(float(os.getenv(
    "POLYMARKET_SWINGFADE_MIN_STAKE", str(UNIFORM_STAKE_USD))),
    UNIFORM_STAKE_USD)
# Seed capital the book must CROSS before compounding switches on.
BANKROLL_SEED_USD = float(os.getenv("POLYMARKET_BANKROLL_SEED_USD", "200"))


def swingfade_stake_for(cap):
    """Stake for ONE swingfade ticket off `cap` (the 00:00-UTC daily capital
    snapshot). Flat SWINGFADE_MIN_STAKE until `cap` clears BANKROLL_SEED_USD,
    then bank x 2/3 x 10% capped at SWINGFADE_STAKE_CEIL. Single source of truth
    — the live sizer, the dash payload and the startup banner all call this."""
    if cap is None or cap <= BANKROLL_SEED_USD:
        return SWINGFADE_MIN_STAKE
    return min(UNIFORM_STAKE_USD, max(SWINGFADE_MIN_STAKE,
               min(round(cap * SWINGFADE_USABLE_FRAC * SWINGFADE_STAKE_FRAC, 2),
                   SWINGFADE_STAKE_CEIL)))
# Compatibility field for the dashboard/API. This is deliberately not configurable:
# a stale POLYMARKET_SWINGFADE_REFILL=1 must never reopen the second-order path.
SWINGFADE_REFILL = False
# FILLABLE-DELTA cap (2026-07-24, applied by request). Fire only when
# (flip-side taker depth - opp-side taker depth) < this cap; blank depth = 0. NOTE:
# on the 64-fire in-sample set this delta had NO signature (AUC ~0.50) and net WR
# fell 25.0%->22.9% once winners removed were counted — objection logged; kept
# config-settable (`swingfade_fill_delta_max`, read every tick) so it is reversible
# with no restart. <=0 disables it.
SWINGFADE_FILL_DELTA_MAX = float(
    os.getenv("POLYMARKET_SWINGFADE_FILL_DELTA_MAX", "1000"))


def fade_fill_delta_gate(fill_flip_sh, fill_opp_sh):
    """Shared depth gate for the active Swingfade and Swingband fade books."""
    cfg = get_config("swingfade_fill_delta_max", None)
    try:
        cap = float(cfg) if cfg is not None else SWINGFADE_FILL_DELTA_MAX
    except (TypeError, ValueError):
        cap = SWINGFADE_FILL_DELTA_MAX
    delta = (fill_flip_sh or 0.0) - (fill_opp_sh or 0.0)
    ok = cap <= 0 or delta < cap
    why = (None if ok else
           f"fill delta {delta:.0f} >= {cap:.0f} "
           f"(flip {fill_flip_sh or 0:.0f} - opp {fill_opp_sh or 0:.0f})")
    return ok, delta, cap, why


def _swingfade_adverse_max():
    """Effective adverse-excursion cap: dash config `swingfade_adverse_max` wins
    over the env constant, read every tick so the leg is reversible with no
    restart. ONE source of truth — the gate and the shadow log both call this, so
    a row can never be stamped with a cap that was not the one enforced."""
    v = get_config("swingfade_adverse_max", None)
    try:
        return float(v) if v is not None else SWINGFADE_ADVERSE_MAX
    except (TypeError, ValueError):
        return SWINGFADE_ADVERSE_MAX


def _swingfade_first_touch_ceil():
    """Effective first-touch ceiling: dash config `swingfade_first_touch_ceil` wins
    over the env constant, so the condition can be retired with 0 (or retuned) with
    no restart — same mechanic as _swingfade_adverse_max()."""
    v = get_config("swingfade_first_touch_ceil", None)
    try:
        return float(v) if v is not None else SWINGFADE_FIRST_TOUCH_CEIL
    except (TypeError, ValueError):
        return SWINGFADE_FIRST_TOUCH_CEIL


def swingfade_ok(*, elapsed_s, ask, up_exc_bps, dn_exc_bps, adx_slope, crossings,
                 flip_side=None, adverse_max=None,
                 pre_ticks=None, cheap_touch_before=None, min_pre_ask=None,
                 first_touch_ceil=None, vol_3min_bps=None):
    """(ok, reason) for the swingfade entry gate — pure, so the live path, the
    dashboard scorer and any backtest share ONE definition of the rule.

    Mirrors oppfade_adx_ok()'s contract deliberately: the research->prod audit's
    failure mode is a model scoring a book we are not actually running.

    FAIL-CLOSED on non-finite inputs. Every numeric test below is a `>` or a range
    check, and NaN compares False against all of them, so a NaN slipped through as
    silent PERMISSION: `nan > 8.0` is False, so the excursion cap passes; `nan in
    {2}` is False, so the crossings cut passes; the armed adverse cap passes too.
    That is not hypothetical -- `path_max_up_bps`/`cross_count` are NULL in
    re_flip_shadow for every row before 2026-07-28, so any scorer reading them via
    pandas (NULL -> NaN, not None) was scoring an ungated book while believing it
    had the gate on. Reject non-finite explicitly so missing data can never open
    the gate, in the live path or in a backtest."""
    def _bad(x):
        return x is None or (isinstance(x, float) and not math.isfinite(x))

    for _name, _v in (("ask", ask), ("elapsed_s", elapsed_s),
                      ("up_exc_bps", up_exc_bps), ("dn_exc_bps", dn_exc_bps),
                      ("adx_slope", adx_slope), ("crossings", crossings)):
        if _bad(_v):
            return False, f"{_name} missing/non-finite ({_v!r})"
    _mc_ok, _mc_why = mc_allows("fade", vol_3min_bps)
    if not _mc_ok:
        return False, _mc_why
    _sf_lo, _sf_hi = swingfade_trial_band(ASSET, SWINGFADE_ASK_MIN, SWINGFADE_ASK_MAX)
    if ask is None or not (_sf_lo <= ask <= _sf_hi):
        return False, f"ask {ask} outside [{_sf_lo:.2f},{_sf_hi:.3f}]"
    if elapsed_s is None or not (SWINGFADE_ELAPSED_MIN <= elapsed_s <= SWINGFADE_ELAPSED_MAX):
        return False, (f"elapsed {elapsed_s}s outside "
                       f"[{SWINGFADE_ELAPSED_MIN},{SWINGFADE_ELAPSED_MAX}]")
    if SWINGFADE_EXC_MAX > 0 and up_exc_bps > SWINGFADE_EXC_MAX:
        return False, f"up excursion {up_exc_bps} > {SWINGFADE_EXC_MAX}bps"
    if SWINGFADE_EXC_MAX > 0 and dn_exc_bps > SWINGFADE_EXC_MAX:
        return False, f"dn excursion {dn_exc_bps} > {SWINGFADE_EXC_MAX}bps"
    if adx_slope is None or abs(adx_slope) > SWINGFADE_ADX_ABS:
        return False, f"adx slope {adx_slope} outside +-{SWINGFADE_ADX_ABS}"
    if crossings is None or crossings in SWINGFADE_CROSS_EXCLUDE:
        return False, f"crossings {crossings} in excluded set {sorted(SWINGFADE_CROSS_EXCLUDE)}"
    # ADVERSE-ONLY cap, applied ON TOP of the symmetric one when armed. We buy the
    # side OPPOSITE the flip, so the excursion running against us is the one in the
    # flip's own direction; the other leg is the prior swing and is left free.
    # Default MUST be the config-aware value, not the raw env constant: every
    # scorer/backtest that calls this without the kwarg has to see the SAME cap the
    # live path is enforcing, or the dash and the analysis scripts silently score
    # the old gate while the bot runs the new one.
    _amax = _swingfade_adverse_max() if adverse_max is None else adverse_max
    if _amax and _amax > 0:
        if flip_side not in ("Up", "Down"):
            return False, f"adverse cap armed but flip_side={flip_side!r}"
        adverse = up_exc_bps if flip_side == "Up" else dn_exc_bps
        if adverse > _amax:
            return False, (f"adverse excursion {adverse:.1f} > {_amax}bps "
                           f"(buy {'Down' if flip_side == 'Up' else 'Up'})")
    # FIRST-TOUCH — the condition the 07-23 basis measured and the live gate never
    # had. Fires only if the cheap side stayed ABOVE the ceiling for the whole
    # pre-band stretch, and only if we actually WATCHED that stretch: an unobserved
    # window is not evidence of absence, it is the coverage hole that invalidated
    # 66% of the original cohort. Fail closed on both counts.
    _ceil = _swingfade_first_touch_ceil() if first_touch_ceil is None else first_touch_ceil
    if _ceil and _ceil > 0:
        # `min_pre_ask` is the live form (the latch stores the raw minimum so the hot
        # path never reads config); `cheap_touch_before` is the pre-resolved boolean
        # a backtest may already hold. Either satisfies the requirement.
        if cheap_touch_before is None and min_pre_ask is not None:
            cheap_touch_before = min_pre_ask <= _ceil
        if pre_ticks is None or cheap_touch_before is None:
            # A caller that does not supply the state is not asserting anything about
            # it. Refuse rather than assume — an older scorer/backtest silently
            # getting a free pass here is precisely the research->prod drift that
            # caused this condition to be missing for six days.
            return False, ("first-touch state not supplied (pass pre_ticks + "
                           "cheap_touch_before, or first_touch_ceil=0 to disable)")
        if _bad(pre_ticks) or pre_ticks < SWINGFADE_FIRST_TOUCH_MIN_TICKS:
            return False, (f"pre-band coverage {pre_ticks} < "
                           f"{SWINGFADE_FIRST_TOUCH_MIN_TICKS} ticks — cannot assert "
                           f"first touch")
        if cheap_touch_before:
            return False, f"cheap ask already <= {_ceil:.2f} before the arming band"
    # Dumped-longshot score. min_pre_ask is the cheapest ask we actually saw
    # before the arming band (t2 probe). A 2c token that later bounces to 0.15
    # is not the convex 0.15-0.21 book — it is a bounce on a contract the
    # market already priced as dead. Fail closed when the floor is armed.
    if SWINGFADE_MIN_PRE_ASK > 0:
        if _bad(min_pre_ask):
            return False, ("min_pre_ask missing — cannot score dumped cheap side "
                           f"(need pre-band coverage, floor {SWINGFADE_MIN_PRE_ASK:.2f})")
        if min_pre_ask < SWINGFADE_MIN_PRE_ASK:
            return False, (f"min_pre_ask {min_pre_ask:.3f} < "
                           f"{SWINGFADE_MIN_PRE_ASK:.2f} (dumped cheap side)")
    return True, ""


def sweep_ladder(rungs, budget_usd, max_price, min_price=0.0):
    """Model a FAK taker sweep against a REAL ask ladder.

    `rungs` = [(price, shares), ...] as logged in opp_ask_ladder. Walks cheapest
    first, spending up to `budget_usd`, refusing any rung above `max_price` —
    exactly what the exchange does with an FAK carrying that limit. Returns
    (shares, notional_usd, avg_px, filled_frac).

    This replaces the extrapolated slip curve used in backtest: live we have the
    actual book, so partial fills and the re-fire are modelled off real depth
    rather than off a single 07-19 probe of the <=0.16 cohort."""
    sh = 0.0
    spent = 0.0
    for px, sz in sorted(rungs or [], key=lambda t: float(t[0])):
        px = float(px)
        sz = float(sz)
        if px > max_price + 1e-9:
            break
        if px < min_price - 1e-9:
            continue
        room = budget_usd - spent
        if room <= 1e-9:
            break
        take = min(sz, room / px)
        if take <= 0:
            continue
        sh += take
        spent += take * px
    avg = (spent / sh) if sh > 0 else None
    frac = (spent / budget_usd) if budget_usd else 0.0
    return round(sh, 4), round(spent, 4), avg, round(min(frac, 1.0), 4)


def t3_ask_fillable(asks, stake_usd, best_ask, min_frac=None):
    """True iff a FAK of `stake_usd` fills `min_frac` at the quoted ask, no pay-up.

    The 0.90 continuation book often shows hundreds of shares a few cents up the
    ladder that never rest at the touch; a $1 ticket then prints fill_frac=0
    against thousands of shares 'at quote' once the sweep's cap is 0.99. Live
    $1 T3 fills in 0.50-0.68 cleared 100% at the quote (ETH depth 20-50sh, BTC
    cheap ~185sh vs ~2sh wanted). Callers must skip when this is False rather
    than walking toward 0.90 to manufacture a fill.
    """
    if min_frac is None:
        min_frac = T3_MIN_FILL_FRAC
    if (not asks or best_ask is None or stake_usd is None
            or stake_usd <= 0 or not (0.0 < best_ask < 1.0)):
        return False, 0.0
    _sh, _usd, _avg, frac = sweep_ladder(asks, stake_usd, best_ask)
    return bool(frac >= min_frac), frac


def fade_clob_touch(rungs):
    """Cheapest ask on a [(px, sz), ...] ladder, else None."""
    best = None
    for item in rungs or ():
        try:
            px = float(item[0])
        except (TypeError, ValueError, IndexError):
            continue
        if not math.isfinite(px) or px <= 0:
            continue
        if best is None or px < best:
            best = px
    return best


def fade_live_touch(bookfeed, token):
    """Fresh WS best-ask + rungs for a token. Fail closed on a missing book."""
    if bookfeed is None or token is None:
        return None, []
    try:
        lv = bookfeed.levels(token)
    except Exception as e:
        _err("fade_live_touch", e)
        return None, []
    if not lv:
        return None, []
    _bids, asks = lv
    rungs = []
    for px, sz in asks:
        try:
            p, s = float(px), float(sz)
        except (TypeError, ValueError):
            continue
        if s > 0 and p > 0 and math.isfinite(p) and math.isfinite(s):
            rungs.append((p, s))
    return fade_clob_touch(rungs), rungs


def fade_place(*, live_ask, band_lo, band_hi, inclusive_lo=True,
               pin_touch=None, payup=None):
    """FAK cap for a fade ticket. Does not change hold-to-settle.

    Pin (`FADE_PIN_TOUCH`, default on for BTC and ETH): abort when the live
    touch has left the scored band and cap the FAK at touch+payup, never the
    band ceiling. That is placement, not an exit: a 0.16 qualify used to sweep
    dumped 0.06 rungs under a 0.21 limit, and ETH live walked a 0.12 quote to
    0.17. Pass pin_touch=False to restore the legacy band-ceiling FAK.

    Returns (ok, cap, reason).
    """
    pin = FADE_PIN_TOUCH if pin_touch is None else bool(pin_touch)
    pay = FADE_TOUCH_PAYUP if payup is None else float(payup)
    try:
        lo = float(band_lo)
        hi = float(band_hi)
    except (TypeError, ValueError):
        return False, None, "band bounds missing"
    if not pin:
        return True, hi, f"band-cap {hi:.3f}"
    if live_ask is None:
        return False, None, "no live touch — fail closed"
    try:
        ask = float(live_ask)
    except (TypeError, ValueError):
        return False, None, "live touch invalid"
    if not math.isfinite(ask) or ask <= 0:
        return False, None, f"live touch {live_ask!r} invalid"
    in_band = (lo <= ask <= hi) if inclusive_lo else (lo < ask <= hi)
    if not in_band:
        lo_br = "[" if inclusive_lo else "("
        return False, None, (
            f"touch {ask:.3f} left band {lo_br}{lo:.2f},{hi:.2f}]")
    cap = min(round(ask + max(0.0, pay), 2), hi)
    if cap + 1e-9 < ask:
        return False, None, f"touch-pin cap {cap:.3f} < ask {ask:.3f}"
    return True, cap, f"touch-pin {ask:.3f} cap {cap:.3f}"


def oppfade_adx_ok(slope, retrace=None, elapsed_s=None):
    """(ok, reason) for the asymmetric adx-slope + flat-band + retrace + late gate.

    Pure and side-effect free so the LIVE gate and the dashboard's scorer can share
    one definition (the drift class the research->prod audit flagged: a model that
    scores a book we are not running). NULL retrace passes — pre-sampler rows have
    no feed coverage and were never gated on it."""
    if slope is None:
        return False, "no adx slope"
    if slope >= OPPFADE_ADX_SLOPE_MAX:
        return False, f"adx slope {slope:+.2f} >= {OPPFADE_ADX_SLOPE_MAX:+.2f} (trend building)"
    if slope <= OPPFADE_ADX_SLOPE_MIN:
        return False, f"adx slope {slope:+.2f} <= {OPPFADE_ADX_SLOPE_MIN:+.2f}"
    if OPPFADE_ADX_FLAT_BAND > 0 and abs(slope) <= OPPFADE_ADX_FLAT_BAND:
        return False, f"adx slope {slope:+.2f} inside flat band +-{OPPFADE_ADX_FLAT_BAND:.2f}"
    if retrace is not None and retrace >= OPPFADE_RETRACE_MAX:
        return False, f"retrace {retrace:.2f} >= {OPPFADE_RETRACE_MAX:.2f}"
    if elapsed_s is None:
        return False, "no elapsed time"
    if elapsed_s < OPPFADE_MIN_ELAPSED_S:
        return False, f"elapsed {elapsed_s}s < {OPPFADE_MIN_ELAPSED_S}s"
    if elapsed_s > OPPFADE_MAX_ELAPSED_S:
        return False, (f"elapsed {elapsed_s}s > {OPPFADE_MAX_ELAPSED_S}s "
                       f"(last {WINDOW_S - OPPFADE_MAX_ELAPSED_S}s of the window)")
    return True, ""


def stamp_oppfade_provenance(leg, *, seq, elapsed_s, ask_opp, adx_slope, retrace,
                             flip_tier, fillable_sh, touch_sh):
    """Record WHY this leg fired, on the leg itself (2026-07-22).

    Every oppfade row — filled, killed or skipped — carries the gate inputs as they
    were AT DECISION TIME. Before this, a live leg had no seq/elapsed_s, so it could
    not be joined back to the re_flip_shadow snapshot it decided on; when 11 of 30
    live legs turned out to be off-cohort there was no way to ask why, and depth was
    being stuffed into free text because no column existed for it.

    `cohort_qualified` is stamped HERE, live, against the frozen rule rather than
    re-derived later — a replay has to guess which seq we used, and guessing wrong is
    how two cohorts get silently blended. Split every forward read on this flag: an
    off-cohort leg is not evidence about the +8.9pp result, in either direction."""
    leg["decision_seq"] = seq
    leg["decision_elapsed_s"] = elapsed_s
    leg["ask_opp"] = None if ask_opp is None else round(ask_opp, 4)
    leg["adx_slope"] = adx_slope
    leg["retrace_frac"] = retrace
    leg["flip_tier"] = flip_tier
    leg["fillable_opp_sh"] = fillable_sh
    leg["touch_opp_sh"] = touch_sh
    _ok, _ = oppfade_adx_ok(adx_slope, retrace, elapsed_s)
    leg["cohort_qualified"] = bool(
        ask_opp is not None and OPPFADE_ASK_MIN < ask_opp <= OPPFADE_ASK_MAX
        and flip_tier not in D_STRONG_TIERS and _ok)
    return leg


# SQL form of the SAME gate for the dashboard scorer — one source of truth. Bind
# with OPPFADE_ADX_SQL_PARAMS; the caller's query must expose adx_slope/retrace_frac.
OPPFADE_ADX_SQL = (
    "adx_slope IS NOT NULL AND adx_slope < :adxmax AND adx_slope > :adxmin "
    "AND (:adxflat <= 0 OR abs(adx_slope) > :adxflat) "
    "AND (retrace_frac IS NULL OR retrace_frac < :retrmax) "
    "AND elapsed_s >= :minelapsed AND elapsed_s <= :maxelapsed")
OPPFADE_ADX_SQL_PARAMS = {"adxmax": OPPFADE_ADX_SLOPE_MAX,
                          "adxmin": OPPFADE_ADX_SLOPE_MIN,
                          "adxflat": OPPFADE_ADX_FLAT_BAND,
                          "retrmax": OPPFADE_RETRACE_MAX,
                          "minelapsed": OPPFADE_MIN_ELAPSED_S,
                          "maxelapsed": OPPFADE_MAX_ELAPSED_S}
OPPFADE_FREEZE_UTC = os.getenv("POLYMARKET_OPPFADE_FREEZE_UTC", "2026-07-16 12:00:00+00")
# --- RULE D: prune by (T+3 ORIGIN tier -> MATURED flip tier) — 2026-07-21 ---
# LOG-ONLY BY DEFAULT ("0"). Set POLYMARKET_OPPFADE_PRUNE_D=1 to ENFORCE, but only
# after the logged counterfactual shows the improvement forward. Every shadow row
# carries d_prune either way, so the A/B is readable straight from the table.
#
# D drops a would-be entry when EITHER:
#   (a) the move MATURED to STRONG / MODERATE-STRONG  — already the live gate. With
#       <1min to settle a strong move does not reverse, so the flip holds and our
#       fade loses: 1-for-14 (MOD-STRONG) and 0-for-8 (STRONG) historically.
#   (b) the T+3 signal was WEAK/<2bps AND it STILL reads <2bps at the qualifying
#       ask — a never-developed noise crossing. 30 windows, 4 wins (13.3%) at the
#       most EXPENSIVE mean ask in the book (0.130): full price for a coin flip.
# Historical read (BTC, ask<=0.16, |adx_slope|<3.5, one bet/window, $1 flat):
#   live gate  n=178 16.9% +5.8pp p=0.013 $0.599/window
#   rule D     n=157 17.8% +7.2pp p=0.005 $0.748/window   (OOS 18.4% -> 20.0%)
# (b) is the ONLY behavioural change vs today's gate; it is an in-sample cut on the
# same sample that produced the pre-registered p=0.02, which is exactly why it
# ships OFF. On 07-21 it would have skipped 2 real losers (-$12.52 of -$92.94) and
# forfeited 1 modelled winner that was a NO_FILL, i.e. cost nothing real.
OPPFADE_PRUNE_D = os.getenv("POLYMARKET_OPPFADE_PRUNE_D", "0") == "1"
D_STRONG_TIERS = ("STRONG", "MODERATE-STRONG")


def _oppfade_prune_d(w, label):
    """(skip, reason) under rule D. `label` = the MATURED flip tier for this row.

    Pure + side-effect free: called on every shadow row for logging, and again by
    the live gate. A window with no T+3 leg counts as a weak origin (tier NULL is
    the sub-2bps bucket) — that is how the historical read scored it.
    """
    if label in D_STRONG_TIERS:
        return True, f"matured {label}"
    t3_tier, t3_bps = w.get("t3_tier"), w.get("t3_bps")
    orig_weak = (t3_tier in (None, "WEAK")
                 or (t3_bps is not None and abs(t3_bps) <= 2.0))
    if orig_weak and label == "<2bps":
        return True, f"origin {t3_tier or '<2bps'} -> still <2bps"
    return False, ""
# Per-entry stake cap as a fraction of the daily-locked oppfade base. The book now
# holds the WHOLE bankroll, so STAKE_FRAC 0.10 is the only thing bounding a single
# longshot ticket; keep it small — this cohort's edge is a ~19% win rate on ~0.10
# tokens, i.e. 4 losers in 5 is the NORMAL path and the bankroll has to survive it.
# OPP-BUY [0.25,0.30] BAND — DELETED 2026-07-20 ("remove the .25 to .3 band
# completely"). It was a forward test frozen 07-17 at +1.66σ in-sample, under the
# 2.57σ null bar, and never validated. Its shadow rows stay in
# polymarket_updown_re_flip_shadow (the table logs every flip regardless of book), so
# the arm can be re-scored later from history without re-adding code.
# LIVE FLIP STAKE GATE (2026-07-13, enforcing validated research into prod).
# The flip book only carries edge on a CHEAP ask (R:R — a cheap flip token pays
# >1:1, clearing break-even even at coin-flip accuracy) that has FULLY CROSSED the
# window open (retrace_frac >= 1.0 = a DEVELOPED reversal, not a ~0bps noise
# crossing priced ~0.50). Ask-cap sweep ([[project_pm_re_flip_edge]]): <=0.50 ->
# +0.53/$1 (100% WR); the 0.55-0.65 band ZEROES the edge. These constants MIRROR
# the dashboard/analyzer STAKED cohort (FLIP_STAKE_MAX_ASK / FLIP_RETRACE_LO) so
# LIVE execution finally matches the shipped research model. Until now the cap
# lived ONLY in tooling, so the live book staked 0.51/0.62 near-coin-flips — the
# real 07-13 losers 5918 (@0.62) / 5925 (@0.51). A pure TIGHTENING: can only
# reduce staked flips (fewer losers), never open a new one.
#
# CAP TIGHTENED 0.50 -> 0.30 on 2026-07-15 (the owner's explicit call, "irrespective").
# RECORDING THE EVIDENCE HONESTLY, because it does not support the number:
#   Cap sweep on the flip cohort (BTC, all history):
#     cap  legs   WR     priced  edge     z      P&L
#     0.50   23  43.5%   0.438   -0.3pp  -0.03  +$1.15   <- the only row with a sample
#     0.45   10  20.0%   0.365  -16.5pp  -1.10  -$5.25
#     0.40    7  28.6%   0.331   -4.6pp  -0.26  +$1.24
#     0.35    4  50.0%   0.290  +21.0pp  +0.93  +$7.75
#     0.30    2 100.0%   0.255  +74.5pp  +2.42  +$12.11  <- TWO TRADES, both won
#   The 0.30 row is n=2 (07-14 14:34 @0.28 and 07-14 20:44 @0.23). The sweep is a
#   7-cell grid; per the null sim in [[project_pm_cheap_ask_006_prereg]] a grid that
#   size manufactures a beautiful winner on coin-flip data every run. P&L is also
#   non-monotone (0.45 loses, 0.40 gains, 0.35 gains) — the signature of noise, not
#   a threshold. This cap is a JUDGEMENT CALL, not a validated result.
# IT CANNOT BE VALIDATED, EVER, ON HISTORY: PM DELISTS the 5-min markets within ~1h
#   of resolution (gamma-api returns [] for any window >1h old — verified 07-15), so
#   the only book history that will ever exist is what we logged live: 3.31 days.
#   Forward rate at cap 0.30 is 0.60 trades/day => n=100 takes ~165 days.
# LIVE IMPACT: this also gates the LIVE flip book (bot :2861). All books are shadow
#   today (t3_live/fade_live/flip_live = 0), so the effect now is on the modelled
#   card only — but if flip_live is ever switched on, staking drops ~13/day -> 0.6/day.
# REVERT: POLYMARKET_FLIP_STAKE_MAX_ASK=0.50 in the unit, or edit the default back.
FLIP_STAKE_MAX_ASK = float(os.getenv("POLYMARKET_FLIP_STAKE_MAX_ASK", "0.30"))
FLIP_RETRACE_LO = float(os.getenv("POLYMARKET_FLIP_RETRACE_LO", "1.0"))
# FLIP ADX-BAND GATE — DISABLED 2026-07-15 (). Default LO=HI=0 => gate off.
#
# Shipped 07-14 on a BIMODAL read of re_flip_shadow: quiet (ADX<25 → 75.9% WR) and
# strong-trend (ADX≥50 → 82.4% WR) both win, 25–50 middle a coin-flip dead zone
# (48.7%). Re-measured 07-15 — the evidence does not survive:
#   * The bimodal shape DECAYED: on the staked cohort the gate governs (ask<=0.50,
#     n=50 total) the buckets now read 66.7% / 44.0% / 50.0%. The ADX>=50 arm — half
#     the story — fell 82.4% -> 50.0%.
#   * Raw WR was never the test. vs the ASK on the same rows: +1.68σ / -0.09σ /
#     +0.35σ. Nothing clears any bar, and the ask is ~0.45 in ALL THREE buckets, so
#     the book does not price ADX and neither do the outcomes.
#   * The skipped 25-50 band is NOT a trap: edge -0.8pp at z=-0.09 = priced fair.
#   * NULL SIM (5k reps, outcomes ~ Bernoulli(ask), same 6x5 band grid, n=50):
#     median max|z| found by PURE LUCK = 1.77, 95th pct = 2.82. Our best real cell
#     (ADX<25) = 1.68 — luck beats it 62% of the time. A 3-way split of n=50 over 30
#     band combinations manufactures a beautiful bimodal band on coin-flip data every
#     single run. That is what the 07-14 finding was.
#   * Consistent with [[project_pm_ask_is_efficient_closed]]: Strength ≡ ADX/50
#     (corr 0.997) and the Bybit macro grid was refuted 07-15 on this same variable.
#
# Re-enable ONLY via a pre-registered forward test (freeze the band BEFORE scoring,
# null sim first, |z|>=2.4). The ADX>=50 arm on the full cohort (+9.3pp, +2.34σ,
# n=115) is the one candidate — still short of the bar, and post-hoc sliced.
# Read by BOTH polymarket_updown_bot.py AND polymarket_dashboard.py (which also
# excludes skipped flips from the modelled flip P&L) — they import these constants,
# so both processes must be restarted for a change to take effect.
FLIP_ADX_LO = float(os.getenv("POLYMARKET_FLIP_ADX_LO", "0"))
FLIP_ADX_HI = float(os.getenv("POLYMARKET_FLIP_ADX_HI", "0"))
# FADE SHADOW MODE (2026-07-11): keep the fade book in shadow when going LIVE —
# it still evaluates, records legs, and books MODELLED P&L for measurement, but NEVER
# places a real order / risks real capital (the fade edge isn't live-validated yet).
# The t3 book trades real; the fade stays dry-run even when self.live. Default ON.
FADE_SHADOW = os.getenv("POLYMARKET_FADE_SHADOW", "1").strip().lower() in (
    "1", "true", "yes", "on")


# ONE-SHOT LIVE PLUMBING TEST (2026-07-13) — the flip book is normally pure
# SHADOW (and the fade book stays shadow when FADE_SHADOW=on), so no real order
# from either has ever exercised the executor.buy → fill → record → settle →
# redeem path end-to-end (the t3 gate rarely stakes, so we've been waiting on it
# for days). This arms a SINGLE real order on a normally-shadow book: config
# `<book>_live_oneshot` = 1/arm → the next qualifying signal on that book places
# ONE real (small, USDC-denominated) order, then _consume_oneshot flips the flag
# to 'done' so it can NEVER fire again without being re-armed (restart-safe, since
# the flag lives in polymarket_config). Read every tick → arm/disarm from the dash
# with no restart. Default OFF for BOTH books; the fade flag is wired but left off.
def _oneshot_armed(book):
    """True iff a one-shot LIVE test order is armed for `book` (flip/fade)."""
    v = get_config(f"{book}_live_oneshot", None)
    return (v is not None and
            str(v).strip().lower() in ("1", "true", "yes", "on", "arm"))


def _consume_oneshot(book):
    """Disarm after a real order LANDED — auto-off so it's strictly one-shot."""
    try:
        set_config(f"{book}_live_oneshot", "done")
    except Exception as e:
        print(f"[oneshot] disarm {book} failed: {type(e).__name__}: {e}", flush=True)


def _oneshot_stake_usd(book):
    """USDC size for the one-shot test order (NOT share-count limited — PM orders
    are USDC-denominated). Config `<book>_live_stake_usd` wins, else env, else $1
    (a safe minimum that should clear PM's order floor on the first try; lower it
    from the dash to go cheaper once the plumbing is proven)."""
    v = get_config(f"{book}_live_stake_usd", None)
    if v is None:
        v = os.getenv(f"POLYMARKET_{book.upper()}_LIVE_STAKE_USD", "1.0")
    try:
        return max(0.01, float(v))
    except (TypeError, ValueError):
        return 1.0


# --- SWINGFADE bounded live-test batch (2026-07-24) ------------------------
# The shadow "100% fill" is computed off the ask ladder AS SEEN AT DECISION — which
# is EXACTLY the optimistic view that showed oppfade as fillable right before it went
# 0-for-35 live (every filled order a loser, the winners left in NO_FILL by taker
# adverse selection). Shadow therefore CANNOT answer "do winners fill?"; only a real
# order can. This runs a BOUNDED batch of small ($1) real swingfade orders so the one
# metric that matters — fill rate ON WINNERS — can be measured for ≤ max×stake at
# risk. Fully dash-settable, read every tick; the counter auto-disarms swingfade_live
# once the batch is spent so it can never quietly become an open-ended live book.
def _swingfade_test():
    """(active, stake_usd, max_n, count) for the bounded $1 live-test batch.
    active only when `swingfade_test_max` > 0 AND `swingfade_test_count` < it."""
    try:
        mx = int(float(get_config("swingfade_test_max", 0) or 0))
    except (TypeError, ValueError):
        mx = 0
    if mx <= 0:
        return False, None, 0, 0
    try:
        cnt = int(float(get_config("swingfade_test_count", 0) or 0))
    except (TypeError, ValueError):
        cnt = 0
    try:
        stk = get_config("swingfade_test_stake_usd", None)
        stk = max(0.01, float(stk if stk is not None
                              else os.getenv("POLYMARKET_SWINGFADE_TEST_STAKE_USD", "1.0")))
    except (TypeError, ValueError):
        stk = 1.0
    return (cnt < mx), stk, mx, cnt


def _swingfade_test_tick(filled):
    """Count one live batch order that was SENT (fill OR no-fill — both consume the
    batch) and auto-disarm swingfade_live once the batch is spent."""
    try:
        cnt = int(float(get_config("swingfade_test_count", 0) or 0)) + 1
        set_config("swingfade_test_count", cnt)
        mx = int(float(get_config("swingfade_test_max", 0) or 0))
        print(f"[swingfade-test] live order {cnt}/{mx} sent "
              f"({'FILLED' if filled else 'NO_FILL'})", flush=True)
        if mx > 0 and cnt >= mx:
            set_config("swingfade_live", "0")
            print(f"[swingfade-test] batch spent ({cnt}/{mx}) — swingfade_live "
                  f"auto-disarmed to SHADOW", flush=True)
    except Exception as e:
        print(f"[swingfade-test] counter update failed: {type(e).__name__}: {e}",
              flush=True)
# ABSOLUTE per-trade STAKE CAPS (2026-07-11) — PM's btc-updown books are thin
# and slippage grows with size (live snap: $250 buy = +74bps, $1k = +400bps, $2.5k =
# +850bps on the ~0.74 side; the CHEAP side the fade buys slips ~3-4x worse per $).
# The cap bounds per-window slippage so compounding doesn't quietly start eating the
# edge — bankroll keeps growing but per-trade size PLATEAUS at what one 5-min window
# can absorb. 0 = uncapped. t3 tolerates more (hold-to-settle survives slippage);
# the fade buys the cheap side so it's capped tighter.
T3_MAX_STAKE_USD = float(os.getenv("POLYMARKET_T3_MAX_STAKE_USD", "250"))
FADE_MAX_STAKE_USD = float(os.getenv("POLYMARKET_FADE_MAX_STAKE_USD", "50"))
POLY_FEE_RATE = 0.07                                                    # crypto taker
# Execution: START on the TAKER plan for BOTH books (T+3 continuation maker =
# adverse selection, confirmed bad). The FADE may later be switched to MAKER —
# but only once the reversal probes prove maker fills are actually gettable on
# the opposite (2026-07-10). Toggle OFF = taker (default); when the probe
# data supports it, POLYMARKET_FADE_MAKER=1 makes the fade rest post-only and
# record $0 fee on maker fills (fade_pnl maker=True), taker fallback otherwise.
FADE_MAKER = os.getenv("POLYMARKET_FADE_MAKER", "0").strip().lower() in (
    "1", "true", "yes", "on")
# HIGH-FADE (the ~9:1 fade) — EXPERIMENTAL 3rd book (2026-07-10). The normal
# fade fires on cheap-ask (<=0.80) reversals → opposite ~0.20-0.50 (~2-5:1). This
# fires on EXPENSIVE-ask reversals (our side ~0.90, i.e. MODERATE / high-ask STRONG
# that the T+3 gates skip) → buys the CHEAP opposite (~0.10) → ~9:1 on a real
# reversal. Signal precision is unproven (MODERATE shadow ~20% on n=5), so it's
# gated behind POLYMARKET_HIGH_FADE (default OFF) + a SMALL flat size + only fires
# when the opposite is genuinely cheap (<= HFADE_MAX_OPP_ASK). book tag "hfade".
HIGH_FADE = os.getenv("POLYMARKET_HIGH_FADE", "0").strip().lower() in (
    "1", "true", "yes", "on")
HFADE_STAKE_FRAC = float(os.getenv("POLYMARKET_HFADE_STAKE_FRAC", "0.05"))   # flat, small
HFADE_MAX_OPP_ASK = float(os.getenv("POLYMARKET_HFADE_MAX_OPP_ASK", "0.20"))  # opposite must be cheap
# ...but NOT too cheap. Payoff-ratio finding (2026-07-10): the only winning fades
# sat at ~6:1 (opposite ~0.14 = an ACHIEVABLE reversal); everything deeper (0.02-
# 0.10 = 9:1-50:1) went 0 wins / -$127 — that reversal never happens in 120s. So
# only fade the REACHABLE band [HFADE_MIN_OPP_ASK, HFADE_MAX_OPP_ASK] (~0.12-0.20,
# 5-8:1); skip the deep-OTM graveyard.
HFADE_MIN_OPP_ASK = float(os.getenv("POLYMARKET_HFADE_MIN_OPP_ASK", "0.12"))
# FADE CONFIRMATION — REMOVED 2026-07-30 ("the 3 sec lead binance has is not
# usable here... remove any trace of binance in pm BTC"). This gate WAS the whole
# justification for the fade books: a fade buys the opposite at price a = the
# market-implied reversal prob, so it is +EV only if P(reversal|signal) > a + fees,
# and the only signal claimed to lift P(reversal) above a was the ~3s Binance lead
# turning against the T+3 side (FADE_BIN_CONFIRM_BPS=2, FADE_STRONG_CONFIRM_BPS=10,
# FADE_REQUIRE_BINANCE). With the lead gone there is NO confirming signal left, so
# fade/hfade have no edge story — they stay SHADOW (FADE_SHADOW=1) and must not be
# armed live until a Chainlink-native confirmation is validated from scratch.
# FADE SELECTIVITY (2026-07-11): "only 1-2 fades in 24h, don't fire blindly."
#  - FADE_MAX_PER_DAY: hard rolling-24h cap on fade+hfade fires (DB-backed, survives
#    restarts). Once hit, we take the T+3 continuation instead of fading.
FADE_MAX_PER_DAY = int(os.getenv("POLYMARKET_FADE_MAX_PER_DAY", "2"))
# FADE ADX FLOOR (2026-07-13 loser analysis): fades only pay when there's a
# strong move to snap back from — the winners had adx_short 61–82, the confirmed
# loser 43. Require the short-window ADX ≥ this on BOTH fade and hfade entries.
FADE_MIN_ADX_SHORT = float(os.getenv("POLYMARKET_FADE_MIN_ADX_SHORT", "55"))
# FADE SALVAGE EXIT (2026-07-10 — "early exit if it goes wrong, save $"):
# fades are normally held to settle, BUT if the reversal is failing — the fade
# token bid has dropped below FADE_SL_FRAC of the entry — sell at market to
# salvage the remaining premium instead of riding to $0. 1s polling while a fade
# is open makes the reaction fast. Set FADE_SL_FRAC=0 to disable (hold to settle).
# DEFAULT FLIPPED 0.5 -> 0 (2026-07-13): salvage-at-0.5×entry is structurally
# −EV for a cheap fade token (recovers ~0.5a while capping the convex upside; hold-
# to-settle wins whenever reversal prob p > a/2 ≈ 6-10%, observed 67-80%). PROVEN on
# the dry-run book: FADE_EXIT salvage cuts realized −$113 (the single biggest loss
# bucket); the same fades HELD TO SETTLE model +$78 (hfade) — a ~$176 swing. Fades
# are SHADOW (FADE_SHADOW=1) so this is modeling fidelity only, zero real capital.
# See [[project_fade_salvage_grace_audit]] / RESEARCH_TO_PROD_ENFORCEMENT_AUDIT.md.
FADE_SL_FRAC = float(os.getenv("POLYMARKET_FADE_SL_FRAC", "0"))
# SALVAGE GRACE (2026-07-13): don't arm the salvage for the first
# FADE_SALVAGE_GRACE_S seconds after the fill. A single first-tick bid dip was
# stopping fades out ~2s into an 80s+ window that then recovered to settle
# in-favour (the hfade …3912200 winner cut for -$0.53 vs +$4.10 held). In-band
# data: EVERY wrongly-cut winner bailed <15s; every >=15s bail genuinely failed.
# So we grace the first-tick noise, re-check at grace-expiry, then bail if the
# reversal is still failing. 0 = disable the grace (arm immediately, old behaviour).
FADE_SALVAGE_GRACE_S = float(os.getenv("POLYMARKET_FADE_SALVAGE_GRACE_S", "15"))
# DECISION / EXIT FEED — CHAINLINK, NOT CONFIGURABLE (2026-07-30). Both used to be
# env-switchable to "binance" to exploit the ~3s lead: DECISION_FEED could drive
# STAKING off Binance, and EXIT_REVERSAL_FEED defaulted to "binance" so the intra-
# window SL_REVERSAL fired ~3s early. The lead is not usable here — measured over
# 5,629 windows Binance disagrees with Chainlink on the cheap side 4.96% of the time
# and on the winner 7.99% (26.2% when the entry gap is <1bps, i.e. exactly where this
# book trades). Kept as a constant, not deleted, because the dashboard state json and
# the startup banner report which feed is in force.
DECISION_FEED = "polymarket_rtds_chainlink"
# NEAR-CLOSE fast-exit safety (— "get out when a trade is going wrong"):
# poll every FAST_POLL_S in the final NEAR_CLOSE_FAST_S so we react instantly, and
# in the last LATE_BAIL_S, if the CHAINLINK move shows the settle going AGAINST our
# side while we still hold, BAIL = cross (taker) to dump at market rather than settle
# to 0. LATE_BAIL_MIN_BPS = how decisively adverse it must be. This trigger was
# Binance-driven until 2026-07-30; on Chainlink it loses the ~3s head start but gains
# correctness — it is now reading the feed that actually decides the payout.
NEAR_CLOSE_FAST_S = int(os.getenv("POLYMARKET_NEAR_CLOSE_FAST_S", "60"))
FAST_POLL_S = float(os.getenv("POLYMARKET_FAST_POLL_S", "1"))
LATE_BAIL_S = int(os.getenv("POLYMARKET_LATE_BAIL_S", "15"))
LATE_BAIL_MIN_BPS = float(os.getenv("POLYMARKET_LATE_BAIL_MIN_BPS", "2"))
# HYBRID (2026-07-11): hold-to-settle drops the noise SL/SL_REVERSAL but KEEPS
# the late-window BAIL — the only early cut that fires on a REAL late reversal rather
# than intra-window noise, so it recovers reversal-losses (e.g. the 07-11 −$26 window
# that reverted in the last 120s) WITHOUT cutting winners (a winner has the feed
# AGREEING at the close → never fires).
HTS_BAIL_MIN_BPS = float(os.getenv("POLYMARKET_HTS_BAIL_MIN_BPS", "2"))
# BAIL BID CEILING (2026-07-11): only bail if the token BID has also fallen to
# <= this — i.e. the MARKET confirms we're losing, not a lone feed blip. Fixes the
# false-positive where a near-coinflip window bailed at bid 0.88 (market still 88% our
# side) on a +2.2bps tick that reverted, cutting a winner. When our bid is still high
# the market strongly disagrees with the blip → hold to settle.
BAIL_MAX_BID = float(os.getenv("POLYMARKET_BAIL_MAX_BID", "0.60"))
# SETTLE: the Chainlink close report can 404 for ~1-2s after the boundary (the DON
# hasn't published that second yet). DEFER settling (retry next tick) rather than
# substitute another feed — Binance DISAGREES on who won 7.99% of windows (26.2%
# under 1bps) and would mis-record the outcome and, LIVE, skip redeeming a real
# winner. (Found via the 18:40 window: CL=Down but a settle-instant 404 fell back to
# Binance=Up, recording the wrong outcome.) The Binance fallback was REMOVED
# 2026-07-30; this grace now only controls when a 404 starts counting as an error.
SETTLE_GRACE_S = float(os.getenv("POLYMARKET_SETTLE_GRACE_S", "20"))
# Deferred-settlement recovery sweep (_settle_deferred): how far past expiry a still-
# OPEN staked row must be before the DB sweep claims it. Must be comfortably clear of
# SETTLE_GRACE_S + the normal REST-retry path so the sweep never races the in-memory
# settle() for a window that is merely a few seconds late — it exists for rows
# stranded by a restart, a >6h prune, or a bug, not for ordinary settlement.
DEFERRED_SETTLE_AFTER_S = float(os.getenv("POLYMARKET_DEFERRED_SETTLE_AFTER_S", "600"))
# How often the sweep runs. Cheap (one indexed query, usually zero rows).
DEFERRED_SETTLE_EVERY_S = float(os.getenv("POLYMARKET_DEFERRED_SETTLE_EVERY_S", "300"))
# Upper age bound: the sweep is OPERATIONAL recovery, not historical repair. Without
# it, a sweep would reach back and book P&L into long-closed history the moment its
# filters happened to match — e.g. the stranded 2026-07-10 hfade row id 779 (OPEN,
# staked, dry_run=t), which today is excluded only because the live bot filters
# dry_run=False. Anything older than this is left for a human.
DEFERRED_SETTLE_MAX_AGE_S = float(
    os.getenv("POLYMARKET_DEFERRED_SETTLE_MAX_AGE_S", str(7 * 24 * 3600)))
# Per-stake reconciliation poll interval. list_positions() lags the FAK response,
# so the reconciler waits this long between reads (4 attempts). Total worst case
# ~12s — far inside a 5-min window, and it runs detached from the tick loop.
RECON_POLL_S = float(os.getenv("POLYMARKET_RECON_POLL_S", "3"))
# Throttle for refreshing the live reconciled USDC balance that drives oppfade
# sizing. The wallet only moves on window settles (~5 min apart), so a coarse
# REST refresh is plenty; keeps us off a per-tick network call near entry.
LIVE_USDC_REFRESH_S = float(os.getenv("POLYMARKET_LIVE_USDC_REFRESH_S", "20"))


def floor_shares(sh):
    """Recordable share count: FLOOR to 2dp, never round up (2026-07-25).

    `round(sh, 2)` can land ABOVE what we actually hold, and Polymarket then rejects
    the whole sell with `not enough balance / allowance`. Real case: a $1 fill at
    0.22 = 4.545453sh recorded as 4.55 -> every exit attempt rejected for the full
    window; the same $1 at 0.13 = 7.692307sh recorded as 7.69 -> sold fine. Whether
    a leg could be exited was being decided by which way the 2nd decimal rounded.
    Flooring strands <0.01 share (worth <$0.01) which simply settles on-chain.
    """
    if sh is None:
        return None
    return math.floor(float(sh) * 100.0) / 100.0


def _taker_fee(shares, p):
    """Polymarket crypto taker fee for `shares` at price p (peaks at p=0.50)."""
    return shares * POLY_FEE_RATE * p * (1 - p)


def taker_pnl(entry_ask, exit_reason, exit_px, shares, spread=0.0):
    """Recorded P&L crossing as a TAKER, NET OF FEE on both legs. Mirrors the
    validated dashboard `_taker_pnl`. entry_ask = fill price paid; a settle-win
    redeems at $1 (no taker fee); TP/SL sells back into the book (fee on exit)."""
    if entry_ask is None or shares is None:
        return None
    ask_in = min(0.99, entry_ask + spread)
    cost = shares * ask_in + _taker_fee(shares, ask_in)
    if exit_reason == "SETTLE_WIN":
        proceeds = shares * 1.0
    elif exit_reason == "SETTLE_LOSS":
        proceeds = 0.0
    elif exit_reason in ("TP", "SL", "SL_REVERSAL", "BAIL") and exit_px is not None:
        bo = max(0.0, exit_px - spread)
        proceeds = shares * bo - _taker_fee(shares, bo)
    elif exit_px is not None:
        proceeds = shares * exit_px
    else:
        return None
    return round(proceeds - cost, 4)


def fade_pnl(opp_fill_px, won, shares, maker=False):
    """Recorded P&L for a FADE leg (buy the cheap opposite, hold to settle).
    `won` True iff the original T+3 call reversed (opposite settles $1). The
    fade is MAKER-PREFERRED: when it fills as a resting post-only order the
    entry pays $0 fee (2026-07-10 — "for fades we may be able to get the
    maker fills"); a taker fallback pays the 0.07*p*(1-p) fee. Held to settle,
    so no exit fee (on-chain redemption isn't a trade)."""
    if opp_fill_px is None or shares is None or won is None:
        return None
    fee = 0.0 if maker else _taker_fee(shares, opp_fill_px)
    cost = shares * opp_fill_px + fee
    proceeds = shares * (1.0 if won else 0.0)
    return round(proceeds - cost, 4)


def hold_to_settle_pnl(book, entry_price, shares, won, fade_maker=False):
    """P&L for a position HELD to the window boundary, by book. Extracted from
    settle() 2026-07-30 so the deferred-recovery sweep (_settle_deferred) books a
    stuck row on the EXACT same economics as the live path — two copies of this
    branch would drift, and the fee terms here are the difference between the
    booked number and the wallet (see the SWINGFADE/OPPFADE fee notes below).

      fade/hfade  -> fade_pnl: maker fill pays no entry fee, taker fill does.
      TAKER_HOLD_BOOKS (t3/flip/oppfade/swingfade) -> taker_pnl: taker BUY, fee on
        the ENTRY only; a win redeems on-chain at $1/share with no exit fee.
      anything else -> the raw (payout - entry) x shares, fee-free.
    """
    payout = 1.0 if won else 0.0
    if TWO_BOOK and book in ("fade", "hfade"):
        return fade_pnl(entry_price, won, shares, maker=fade_maker)
    if TWO_BOOK and book in TAKER_HOLD_BOOKS:
        return taker_pnl(entry_price,
                         "SETTLE_WIN" if won else "SETTLE_LOSS", payout, shares)
    if entry_price is None or shares is None:
        return None
    return round((payout - entry_price) * shares, 4)


# --- TP / SL (bid == price we can SELL the token back into) ---
TP_BID = float(os.getenv("POLYMARKET_TP_BID", "0.90"))
SL_BID = float(os.getenv("POLYMARKET_SL_BID", "0.30"))
SL_REVERSAL_BPS = float(os.getenv("POLYMARKET_SL_REVERSAL_BPS", "3.0"))

# --- HOLD-TO-SETTLE for the T+3 continuation book (2026-07-11) ---
# 30-day Chainlink backtest: staked tiers are 91.3% DIRECTIONALLY correct
# (STRONG 92.9%, MOD-STRONG 88.1%, n=2,909) and the entry gate already requires
# ask < tier_acc, so every staked t3 trade is +EV IF HELD to the 5-min boundary.
# The bid-0.30 SL was converting correct calls into near-total losses on
# intra-window noise (hold-to-settle +112 vs realized -36 on the 07-10/11 run).
# When ON, t3 legs SKIP the intrabar SL / SL_REVERSAL and ride to settlement; the
# only early exits are a HIGH-bid TP (locks a near-certain win) and a LATE, feed-
# confirmed BAIL (disaster hatch, not intrabar noise).
# DEFAULT FLIPPED 0 -> 1 (2026-07-13), RESOLVED by scripts/t3_exit_policy_backtest.py:
#   • Large-sample Chainlink settle accuracy: STRONG 93.1% (n=189), MODERATE-STRONG
#     84.7% (n=111) → hold-to-settle is +EV whenever ask < tier WR, which
#     MAX_ENTRY_ASK=0.80 already guarantees (+0.24 to +0.37 per $1 at ask 0.67).
#   • Direct A/B on the 17 real staked legs: the intrabar STOP HURT 12/17 (shook out
#     settle-winners, e.g. ask 0.76 stopped −0.67/$ vs hold +0.30/$); hold beats stop
#     −0.045 vs −0.076 per $1. Whole-book dry-run reconciliation: realized −$63 vs
#     hold-to-settle +$93 on t3.
# Pairs with fractional-Kelly sizing (TODO) to bound the full-stake variance on the
# ~15% genuine misses; consider a ~0.55 ask floor (thinner n=11 evidence).
HOLD_TO_SETTLE = os.getenv("POLYMARKET_HOLD_TO_SETTLE", "1").strip().lower() in (
    "1", "true", "yes", "on")
HOLD_TP_BID = float(os.getenv("POLYMARKET_HOLD_TP_BID", "0.97"))

# --- entry liquidity / sanity guards ---
# A healthy mid-window book on these 0.01-tick markets is ~1-3c wide. A hollow
# book (only extreme quotes, huge spread) or an already-near-resolved ask
# (<0.05 / >0.95) is a no-trade: either no real liquidity or the Polymarket
# crowd violently disagrees with our continuation call -- do not punt into it.
MAX_SPREAD = float(os.getenv("POLYMARKET_MAX_SPREAD", "0.06"))
MIN_ASK = float(os.getenv("POLYMARKET_MIN_ASK", "0.05"))
MAX_ASK = float(os.getenv("POLYMARKET_MAX_ASK", "0.95"))

# --- MAX ENTRY ASK (risk:reward cap) ---
# Buying a token at a high ask has terrible R:R: at ask 0.83 the exit-on-win
# (TP bid) nets ~+0.10/share while a stop can lose ~0.55/share, so a few losers
# swamp many small winners even at a high win-rate. The first 9 dry-run stakes
# bore this out: the entry≥0.82 bucket was a NET LOSER (4 wins +$2.60, 2 losses
# −$5.45) while entry≤0.80 was all winners (+$3.85). Only stake when best_ask is
# at or below this cap. (Distinct from MAX_ASK 0.95, which is a liquidity/sanity
# bound.) Tune with more data — do NOT treat the default as validated.
MAX_ENTRY_ASK = float(os.getenv("POLYMARKET_MAX_ENTRY_ASK", "0.80"))

# --- MIN ENTRY ASK (adverse-selection / calibration floor) ---
# The mirror problem at the CHEAP end. A t3 dry-run book audit (40 staked legs
# 07-09..07-14, 2026-07-14) found realized accuracy tracks the ENTRY ASK,
# NOT the model's tier_acc (0.877): below ~0.72 the win rate collapses to a
# coinflip and the book bleeds. Entry-ask buckets (win-rate / P&L):
#     <0.60      50%  -$65.90        0.72-0.80  88%  -$9.10
#     0.60-0.72  50%  -$4.86         0.80-0.86  75%  +$5.51
# i.e. ~95% of the book's -$74 loss is sub-0.72 entries on the OLD WEAK-heavy
# book. Forward tape 2026-08-25..09-13 on card-qualified STRONG/MS inverts that:
# BTC 0.50-0.60 n=95, 88.4% won vs 51.1% priced, +$66.89 per $1. The 0.72 floor
# was the skip reason on 122/124 of those BTC cheap tickets. T3 now uses
# T3_ASK_FLOOR (0.50) inside t3_book_ok; this legacy floor only applies if
# T3_BOOK_ON is off. Keep it aligned with T3_ASK_FLOOR so a leftover path
# cannot resurrect 0.20-0.40 lotteries. Set 0 to disable.
MIN_ENTRY_ASK = float(os.getenv("POLYMARKET_MIN_ENTRY_ASK", "0.50"))

# --- live execution slippage caps (protect FAK market orders) ---
BUY_SLIP = float(os.getenv("POLYMARKET_BUY_SLIP", "0.03"))   # max_price = ask + this
SELL_SLIP = float(os.getenv("POLYMARKET_SELL_SLIP", "0.03"))  # min_price = bid - this
# OBSERVATION-only cap for opp_ask_ladder (2026-07-20). NOT an execution guard --
# nothing sizes or prices off it. The 07-19 probe logged the ladder to
# ask+BUY_SLIP, which made the depth question unanswerable above the guard: the
# opp-fade's EV breakeven is ask+0.089 (true rate = ask + the 8.9pp edge) while
# BUY_SLIP stops at ask+0.03, so ~$93 median depth inside the cap was a floor, not
# a measurement. Widen the LOOK to past breakeven so we can tell "no liquidity"
# from "liquidity outside our guard" before touching BUY_SLIP itself.
LADDER_OBS_SLIP = float(os.getenv("POLYMARKET_LADDER_OBS_SLIP", "0.15"))
MIN_USDC_TO_TRADE = float(os.getenv("POLYMARKET_MIN_USDC", "1.0"))

# --- MAKER execution (post-only, reprice toward mid; DEFAULT and only route) ---
# Polymarket's 2026 dynamic TAKER fee peaks at exactly p=0.50 -- where every BTC
# up/down window sits -- and Crypto is the top category (feeRate 0.07):
#   fee = shares * 0.07 * p * (1-p)  ->  ~$1.75/100sh at 0.50  ->  ~3.5%/side,
#   ~7% round-trip of notional, ~60x the Binance 11bps cost bar. That haircut
# eats the entire T+3 continuation edge. MAKERS PAY $0, so we only ever rest
# post-only limits and reprice toward mid; we NEVER cross. The SDK exposes just
# the Global CLOB (chain 137, clob.polymarket.com) -- there is no US venue to
# route the flat-0.10% taker schedule through -- so maker-on-Global is the only
# fee escape. Both legs (entry + exit) are maker.
MAKER_MODE = os.getenv("POLYMARKET_MAKER_MODE", "1") not in ("0", "false", "False", "")
TICK = float(os.getenv("POLYMARKET_TICK", "0.01"))              # 0.01-tick markets
MAKER_REPRICE_S = float(os.getenv("POLYMARKET_MAKER_REPRICE_S", "8"))   # dwell/leg
MAKER_MAX_REPRICES = int(os.getenv("POLYMARKET_MAKER_MAX_REPRICES", "3"))  # steps to mid
MAKER_POLL_S = float(os.getenv("POLYMARKET_MAKER_POLL_S", "2"))         # fill poll cadence

# --- LIVE order book over WS (drives maker placement: touch/mid/spread) ---
# Maker repricing is entirely book-driven, so we stream the CLOB market channel
# and maintain the book in memory instead of REST-snapshotting every dwell. The
# feed is best-effort: any staleness/error transparently falls back to the REST
# get_order_book path, so it is NEVER a single point of failure.
WS_BOOK = os.getenv("POLYMARKET_WS_BOOK", "1") not in ("0", "false", "False", "")
BOOK_STALE_S = float(os.getenv("POLYMARKET_BOOK_STALE_S", "10"))        # feed->REST cutover

# --- DRY-RUN maker fill-realism logger (queue-depth + trade-tape) ---
# Dry-run currently assumes maker orders fill at the touch instantly. That is
# optimistic: a resting order only fills after the VOLUME queued ahead of it
# clears. This shadow logger replays the reprice ladder read-only against the
# live WS book, using level size (queue ahead) + the trade tape (volume traded
# through our price) to estimate what would ACTUALLY fill -- additive telemetry,
# does NOT change recorded PnL. Calibrates the maker economics before go-live.
MAKER_FILL_LOG = os.getenv(
    "POLYMARKET_MAKER_FILL_LOG",
    "./reports/polymarket_maker_fills.jsonl")
SHADOW_FILL = os.getenv("POLYMARKET_SHADOW_FILL", "1") not in ("0", "false", "False", "")

# --- EARLY-MAKER PROBE (2026-07-15) ---------------------------------
# The maker route was tested only at T+3, resting into a book that had ALREADY
# repriced the move — 54% fill, and 46.6% of those filled because price traded
# THROUGH us (i.e. adverse selection: we get the fills we don't want). Open
# question: at ~T+60 the book still sits at ~0.50 with a ~1.1c spread and carries
# no information yet, so a rest there may fill against UNINFORMED flow instead.
# That is a market-making question, not a directional one, and the honest prize
# is half the spread (~0.57pp/fill) — NOT the taker fee saving, because on an
# efficient book EV with no fee is exactly 0, not positive.
# t2probe already gives per-second bid/ask early, but it has NO queue sizes, so it
# cannot say whether a resting order fills or gets picked off. This fires the same
# queue+tape replay `_shadow_fill_log` once per window at T+EARLY_MAKER_PROBE_S to
# collect exactly that. Pure telemetry: places no order, touches no PnL, gates
# nothing. Set POLYMARKET_EARLY_MAKER_PROBE_S=0 to disable.
EARLY_MAKER_PROBE_S = float(os.getenv("POLYMARKET_EARLY_MAKER_PROBE_S", "60"))
# Hard upper bound on how late the probe may fire. Without it, a window joined
# late (mid-window restart) fires at the first poll past the threshold — observed
# at probe_s=148 with the book already at 0.90/0.10, i.e. the SATURATED regime
# this probe exists to avoid. Past this tolerance the window is skipped entirely.
EARLY_MAKER_PROBE_MAX_LATE = float(
    os.getenv("POLYMARKET_EARLY_MAKER_PROBE_MAX_LATE", "20"))

# --- Strategy equity: the bankroll compounds off the RESEARCHED strategy (taker
# on maker-no-fill, skip on fill), not the illusory maker-model P&L. ---
POLY_FEE = 0.07                                              # crypto taker feeRate
STRAT_SPREAD = float(os.getenv("POLYMARKET_STRAT_SPREAD", "0.02"))


_SHADOW_FILL_CACHE = {"sig": None, "map": {}}


def _shadow_fill_map():
    """{(window_anchor, leg): fill_frac} from the entry shadow-fill records.
    Re-parsed only when the log changes (called from the event loop)."""
    try:
        st = os.stat(MAKER_FILL_LOG)
        sig = (st.st_mtime_ns, st.st_size)
    except OSError:
        sig = None
    if sig is not None and _SHADOW_FILL_CACHE["sig"] == sig:
        return dict(_SHADOW_FILL_CACHE["map"])
    m = _shadow_fill_map_uncached()
    _SHADOW_FILL_CACHE["sig"] = sig
    _SHADOW_FILL_CACHE["map"] = dict(m)
    return m


def _shadow_fill_map_uncached():
    m = {}
    try:
        with open(MAKER_FILL_LOG) as f:
            for line in f:
                try:
                    r = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if r.get("kind") != "entry":
                    continue
                try:
                    a = int(str(r.get("slug", "")).split("-")[-1])
                except (TypeError, ValueError):
                    continue
                m[(a, r.get("leg"))] = r.get("fill_frac")
    except FileNotFoundError:
        pass
    return m


def _taker_per_share(bid, reason, exit_px):
    """Per-share taker P&L: buy at ask (~bid+spread), exit per rule, both-leg fee."""
    if bid is None:
        return None
    ask = min(0.99, bid + STRAT_SPREAD)
    if reason == "SETTLE_WIN":
        out, exit_fee = 1.0, 0.0
    elif reason in ("TP", "SL", "SL_REVERSAL") and exit_px is not None:
        out = max(0.0, exit_px - STRAT_SPREAD)
        exit_fee = POLY_FEE * out * (1 - out)
    elif exit_px is not None:
        out, exit_fee = exit_px, POLY_FEE * exit_px * (1 - exit_px)
    else:
        return None
    return (out - ask) - POLY_FEE * ask * (1 - ask) - exit_fee


# --- Maker fillability gate on STAKING (uses queue depth + trade volume) ---
# A resting maker order can only fill what the book's queue-clearing volume
# allows over our reprice-ladder dwell. Modes (POLYMARKET_MAKER_FILL_GATE):
#   "shadow" (default) — compute + record the fill prediction and the would-block
#       decision, but DO NOT block or cap. Lets the dry-run collect block-rate +
#       prediction-vs-observed calibration without starving fill data.
#   "on"/"1"          — enforce: skip when even the min ticket can't fill, else
#       cap the stake to the predicted fillable size.
#   "off"/"0"         — disabled.
# Degrades gracefully in every mode: if the WS feed lacks enough data to judge,
# the gate is skipped (no telemetry, no block) rather than mis-judging.
MAKER_FILL_GATE = os.getenv("POLYMARKET_MAKER_FILL_GATE", "shadow").strip().lower()
GATE_ENFORCE = MAKER_FILL_GATE in ("on", "1", "true", "yes", "enforce")
GATE_ANY = MAKER_FILL_GATE not in ("off", "0", "false", "no", "")
VOL_WINDOW_S = float(os.getenv("POLYMARKET_VOL_WINDOW_S", "60"))   # trade-rate lookback

# --- multi-entry / re-entry within one 5-min window ---
# The T+3 continuation call is the first entry; if it exits early (esp. on a
# reversal) we re-evaluate continuously while flat and re-enter — including the
# opposite side — as long as the new move clears the same tier/liquidity/
# breakeven gates. Bounded because every market round-trip pays the spread.
MAX_ENTRIES_PER_WINDOW = int(os.getenv("POLYMARKET_MAX_ENTRIES_PER_WINDOW", "3"))
LATE_ENTRY_CUTOFF_S = int(os.getenv("POLYMARKET_LATE_ENTRY_CUTOFF_S", "270"))  # no new entry in last 30s
REENTRY_COOLDOWN_S = int(os.getenv("POLYMARKET_REENTRY_COOLDOWN_S", "15"))     # min gap between legs
# When the T+3 decision does NOT stake, keep re-deciding Up/Down through the rest
# of the window and LOG a row each time the direction/tier CHANGES (deduped) — so
# the revised call is visible in the table. Pure logging: never stakes, never
# alters behaviour (the existing re-entry loop still stakes if a revision qualifies).
LOG_T3_REVISIONS = os.getenv("POLYMARKET_LOG_T3_REVISIONS", "1").strip().lower() in (
    "1", "true", "yes", "on")
# RE-FLIP SHADOW: when the re-entry re-analysis FLIPS direction vs the T+3 call
# (price crossed back through the window open -> small move -> WEAK tier, which the
# staking gate rejects), log the flipped side + the OBSERVED ask on it. Backtest
# (Chainlink, 14d) shows those flips are ~70% directionally right (T+3 was ~30% on
# the same windows) but currently thrown away by the no-stake filter. This measures
# the real ask at flip time so we can score whether staking them is +EV. Pure,
# isolated logging: never stakes, never mutates trading state. [[project_grind_liq_fade_audit]]
RE_FLIP_SHADOW = os.getenv("POLYMARKET_RE_FLIP_SHADOW", "1").strip().lower() in (
    "1", "true", "yes", "on")

# ---------------------------------------------------------------------------
# Proven edge tables — copied verbatim from binance_5min_stake_alert_monitor.py
# (do NOT import that module; importing boots a second live monitor).
# ---------------------------------------------------------------------------
TIERS = [
    (30.0, "EXTREME", 0.968),
    (20.0, "VERY STRONG", 0.952),
    (10.0, "STRONG", 0.896),
    (8.0, "STRONG", 0.883),
    (6.0, "MODERATE-STRONG", 0.877),
    (4.0, "MODERATE", 0.820),
    (2.0, "WEAK", 0.691),
]
ADX_CONFIRMED_BANDS = [(15.0, 20.0), (25.0, 30.0), (30.0, 35.0)]
ADX_UNFAVOURABLE_BANDS = [(20.0, 25.0), (35.0, float("inf"))]
VALUE_TIERS = {"STRONG", "VERY STRONG", "EXTREME"}
# ADX unfavourable-band VETO on MODERATE-STRONG — DISABLED by default 2026-07-10
# (). The bands are a Binance heuristic that doesn't transfer: on Polymarket
# the "unfavourable" bands are 75-81% direction-accurate (≈ the "favourable" 83-86%),
# while the confirmed 30-35 band is the WORST (56%). The ask R:R cap + fill/skip-on-
# reversal gates now handle the real failure modes (bad R:R, reversals) far better
# than a blunt ADX block. Flip POLYMARKET_ADX_VETO=1 to restore the old veto.
ADX_VETO = os.getenv("POLYMARKET_ADX_VETO", "0") not in ("0", "false", "False", "")
# SHORT-period ADX (2026-07-10): ADX(14) lags the whole 5-min window; a
# 5-7 period ADX captures the intra-window trend far better. Logged alongside.
ADX_SHORT_PERIOD = int(os.getenv("POLYMARKET_ADX_SHORT_PERIOD", "7"))
# MODERATE-STRONG (6-8bps) enabled 2026-07-09 (): it lost on Binance only
# because those contracts are hold-to-expiry; here we cut early (TP/SL/reversal),
# so the tier is viable. Still subject to the breakeven + liquidity + ADX-
# unfavourable gates (it's not in VALUE_TIERS, so ADX 20-25/>=35 still vetoes it).
NO_STAKE_TIERS = {"WEAK", "MODERATE", "VERY STRONG", "EXTREME"}
# The tiers we actually stake = every label tier_for can emit minus the no-stake
# set (currently MODERATE-STRONG / STRONG). EXTREME is ~0.90: high WR, no payout,
# and the 0.90 book does not fill a $1 FAK at the quote. Used for the
# "qualified tiers" predicted-accuracy breakout so the no-stake eval legs
# don't dilute it.
STAKED_TIERS = {lbl for _, lbl, _ in TIERS} - NO_STAKE_TIERS

# First number after the word "ask" — the observed best ask on the side we called.
# Covers every shape plan_entry/_maybe_log_revision emit (verified against the live
# table): "ask 0.66 · queue 421sh", "illiquid book (bid 0.30/ask 0.97, spread 0.10)",
# "ask too high for R:R (0.83 > cap 0.80)", "ask too cheap (0.60 < floor 0.72)",
# "breakeven/size floor (ask 0.80 vs acc 0.877)", each optionally prefixed
# "revised T+227s: ". The value must be a standalone decimal: the old [^0-9]* skip
# stopped at the "3" of "hard T3 cap (0.75 > 0.68)", which failed the 0-1 check and
# silently dropped every cap-rejected (i.e. cheaper-ask) leg from calibration.
_ASK_RE = re.compile(r"\bask\b.*?(?<![\w.])([0-9]?\.[0-9]+)")


def _observed_ask(leg):
    """Best ask we'd have paid on this leg, or None if not recoverable. Staked legs
    carry the real fill; no-stake legs only have it embedded in the reason text
    (nothing stamps it to a column — see _cached_ask_queue). Used for the dash
    calibration line; do NOT use for P&L (the fill can differ from the quote)."""
    ep = leg.get("entry_price")
    if ep is not None:
        try:
            return float(ep)
        except (TypeError, ValueError):
            pass
    m = _ASK_RE.search(leg.get("no_stake_reason") or "")
    if not m:
        return None
    try:
        a = float(m.group(1))
    except ValueError:
        return None
    return a if 0.0 < a < 1.0 else None

# base pct-of-bankroll ladder by tier (mirrors STAKE_PCT_TIERS), used as an
# UPPER CAP on the Kelly result rather than the raw stake.
STAKE_PCT_TIERS = [
    (30.0, 0.05),
    (20.0, 0.05),
    (10.0, 0.04),
    (8.0, 0.03),
    (6.0, 0.01),
    (4.0, 0.0),
    (2.0, 0.0),
]


def load_credentials():
    """(private_key, wallet) from env override, else the encrypted
    exchange_api_credentials DB (same store as Bybit/Binance keys). Never logs
    the secret. Returns (None, wallet) when no key is available."""
    pk = os.getenv("POLYMARKET_PRIVATE_KEY")
    wallet = os.getenv("POLYMARKET_WALLET_ADDRESS")
    if pk:
        return pk, wallet
    try:
        from DBops import get_exchange_api_credentials
        row = get_exchange_api_credentials(
            account_key="polymarket_main", exchange_name="POLYMARKET")
        if row and row.get("api_secret"):
            return row["api_secret"], (wallet or row.get("api_key"))
    except Exception:
        pass
    return None, wallet


def load_relayer_key():
    """(relayer_api_key, address) for gasless deposit-wallet transactions. Env
    override, else dash-set config (`relayer_api_key`/`relayer_address`). Address
    defaults to the signer (derived from the key) when blank. (None, None) if unset."""
    k = os.getenv("POLYMARKET_RELAYER_API_KEY") or get_config("relayer_api_key", None)
    a = os.getenv("POLYMARKET_RELAYER_ADDRESS") or get_config("relayer_address", None)
    return (k or None), (a or None)


def tier_for(abs_bps):
    for floor, label, acc in TIERS:
        if abs_bps >= floor:
            return label, acc
    return None, None


def stake_pct_cap(abs_bps):
    for floor, pct in STAKE_PCT_TIERS:
        if abs_bps >= floor:
            return pct
    return 0.0


def adx_confirmed(v):
    return v is not None and any(lo <= v < hi for lo, hi in ADX_CONFIRMED_BANDS)


def adx_unfavourable(v):
    return v is not None and any(lo <= v < hi for lo, hi in ADX_UNFAVOURABLE_BANDS)


def should_stake(label, adx_value):
    """False -> explicit no-stake: WEAK/MODERATE(+) no-stake tiers, or an
    unfavourable ADX band (STRONG/VERY STRONG/EXTREME exempt)."""
    if label is None or label in NO_STAKE_TIERS:
        return False
    if ADX_VETO and label not in VALUE_TIERS and adx_unfavourable(adx_value):
        return False
    return True


# ---------------------------------------------------------------------------
# T3 BANK BOOK — the validated entry stack (2026-07-29)
# ---------------------------------------------------------------------------
# Implements PM_T3_BANKBOOK_FILTERS.md §2 verbatim. Derived from 866 deduped
# windows (2026-07-24 -> 07-29) and reconciled cell-for-cell against
# reports/pm_t3_tier_shadow_20260724_20260729.xlsx: weekday Stage1+2 reproduces
# n=80 / +$109.86 exactly, and the all-days view reproduces the workbook's own
# per-tier summary (MS $21.94 / STRONG $8.26 / MODERATE $10.69 / WEAK $0.38).
#
# WHY THIS BOOK STAKES TIERS should_stake() REFUSES: NO_STAKE_TIERS blocks WEAK
# and MODERATE, which are 77% of this book's weekday profit ($84.31 of $109.86).
# The workbook's "AVOID WEAK" note is computed on its Date:All filter, where WEAK
# is +$0.38/n=102 — but that number is the WEEKEND dragging it down (weekdays
# +$61.40/n=53, weekends -$61.02/n=49). Once Stage 1 runs, WEAK is the single
# largest contributor. So this gate REPLACES should_stake() for book="t3"; it does
# not layer on top of it. Confirmed by 2026-07-29 over the workbook's own
# "EXCLUDE WEAK" line, which contradicts its "do NOT trade weekends" line.
T3_BOOK_ON = os.getenv("POLYMARKET_T3_BOOK_ON", "1") not in ("0", "false", "False", "")
# Frozen first-principles replacement gate (2026-09-22).  Opt-in so rollback is
# one environment line and historical tooling can still replay the legacy gate.
T3_FIRST_PRINCIPLES = os.getenv(
    "POLYMARKET_T3_FIRST_PRINCIPLES", "0").strip().lower() in (
        "1", "true", "yes", "on")
T3_MAX_ASK_DROP = float(os.getenv("POLYMARKET_T3_MAX_ASK_DROP", "0.05"))


def t3_live_ask_ok(signal_ask, live_ask):
    """Fail closed when the executable continuation token has materially faded."""
    try:
        signal_ask = float(signal_ask)
        live_ask = float(live_ask)
    except (TypeError, ValueError):
        return False
    if not (math.isfinite(signal_ask) and math.isfinite(live_ask)):
        return False
    return T3_MAX_ASK_DROP <= 0 or live_ask >= signal_ask - T3_MAX_ASK_DROP - 1e-9
# BTC T3 shadow challenger (2026-08-31). OFF 2026-09-13: VERY STRONG/EXTREME
# print 98%+ accuracy at ask ~0.90, which is both unfillable as a $1 FAK at
# the quote and priced in (BTC EXTREME card-ask set -$0.01). Re-enable with
# POLYMARKET_BTC_T3_SHADOW_CHALLENGER=1; the branch still only fires while
# t3_live=0 and never places a real order.
BTC_T3_SHADOW_CHALLENGER = os.getenv(
    "POLYMARKET_BTC_T3_SHADOW_CHALLENGER", "0").strip().lower() not in (
        "0", "false", "no", "off", "")
BTC_T3_SHADOW_TIERS = {"VERY STRONG", "EXTREME"}
BTC_T3_SHADOW_STAKE_USD = float(os.getenv(
    "POLYMARKET_BTC_T3_SHADOW_STAKE_USD", "1.0"))
BTC_T3_SHADOW_ASK_WARN = float(os.getenv(
    "POLYMARKET_BTC_T3_SHADOW_ASK_WARN", "0.85"))


def t3_entries_stopped():
    """Operator retirement of native T3 entries; settlement and TWAP remain active."""
    return os.path.exists('./params/pm_t3_stopped.json')


def btc_t3_shadow_challenger_active():
    """True only for BTC while T3's own live switch remains explicitly off."""
    return (not t3_entries_stopped() and ASSET == "BTC" and BTC_T3_SHADOW_CHALLENGER
            and not _book_live("t3", False))
# Stage 2: tier -> (ask_pred cap or None, be_wr cap). Tiers absent are EXCLUDED
# entirely (WEAK / MODERATE / VERY STRONG / EXTREME / sub-2bps).
# 2026-09-13 tape (epoch 2026-08-25..09-13, $1 taker hold-to-settle):
#   card-qualified STRONG/MS 0.50-0.68 is the monetizable band
#     BTC n=111 WR 83.8% priced 53.1% +$64.14; ETH n=112 WR 71.4% +$34.14
#   WEAK/MODERATE modelled $1 HTS: BTC -$53 / ETH -$35
#   0.90+ EXTREME/VERY STRONG: high WR, ~fair vs the ask, $1 FAK does not
#     fill at the quote. Per-tier cap here is 0.68; T3_ASK_CEIL enforces it
#     on every T3 route so no legacy path can reopen 0.80-0.90.
T3_BOOK_TIER_RULES = {
    "MODERATE-STRONG": (0.68, 0.80),
    "STRONG":          (0.68, 0.90),
}
# Stage 1: skip Fri/Sat/Sun (UTC weekday(), Mon=0). Thursday is UNTESTED — the
# sample only held Mon/Tue/Wed — so the first Thursdays are observation, not signal.
# `PM_SKIP_DOW` is retained as the compatibility/env name, but its scope is T3 only.
# This alias keeps the rule visible at the T3 spec's own site without creating a
# second definition.
T3_BOOK_SKIP_DOW = PM_SKIP_DOW
# Stage 3. adx (NOT adx_short — non-monotone across quartiles, no mechanism) is the
# only one of the three with an a-priori mechanism ON BTC: the tiers price
# CONTINUATION, and low ADX means there is no trend to continue. Bottom quartile
# (9.6-18.9) is -$152.53 / edge -5.60pp / t=-2.08 across the full BTC file.
# ETH NEVER had its own version of this study — the 17.0 threshold was inherited
# from BTC by default, not independently derived. Reconstructed ADX(14) (same
# adx_now() formula, via a 90-day Coinbase-OHLC proxy since we have no paid
# Chainlink history — see project_chainlink_data_streams_cost_not_viable) on
# n=19,778 ETH windows 2026-08-22: adx<17 hits 90.5%, adx>=17 hits 90.4% —
# IDENTICAL, no discrimination at all, and within the three highest-volume
# tiers (WEAK/MODERATE/MODERATE-STRONG) adx>=17 is slightly WORSE (e.g. WEAK
# 81.7% vs 78.5%). On real ETH ask data the gate is also economically inert:
# dropping it alone moves PnL by only +$0.45 (n=11->12) vs +$10.90 for dropping
# T3_BOOK_MIN_ABS_BPS alone. Floored to 0 (no-op) for ETH; BTC's validated 17.0
# is untouched.
T3_BOOK_ADX_MIN = float(os.getenv(
    "POLYMARKET_T3_BOOK_ADX_MIN", "17.0" if ASSET == "BTC" else "0.0"))
# Forward WR filter (2026-08-15, BTC 3bps floor): validated on a 95-row design
# subsample only. Replayed 2026-08-22 against the full BTC tape (35,240 settled
# decisions, 07-11..08-20): stacked on T3_REVISION_ASK_MIN it CUTS model P&L
# (+$16.87/n=328 stacked vs +$42.73/n=1130 with revision-ask-min alone) — the
# floor throws away more profitable volume than it protects on BTC. Floored to
# 0 (no-op) for BTC.
# ETH's own cited "7,660-row audit, WR plateau at 4bps" could not be reproduced
# this session: replaying the CURRENT full ETH gate stack (adx>=17 included) on
# real logged ETH decisions found n=11, PnL -$3.78; dropping this floor alone
# (adx gate still on) gives n=47, PnL +$7.12; dropping both gives n=63,
# PnL +$13.49. Same direction as BTC's finding — stacked with everything else,
# the floor is net-negative on ETH's own real ask data too. Floored to 0
# (no-op) for ETH as well, 2026-08-22. Both assets' floors are now 0; keep the
# constant (rather than deleting it) so a future asset can reintroduce its own
# validated cut without re-deriving the mechanism. Configurable for a clean
# rollback/A-B.
T3_BOOK_MIN_ABS_BPS = float(os.getenv(
    "POLYMARKET_T3_BOOK_MIN_ABS_BPS", "0.0"))
# ETH live audit (2026-08-15): only the scheduled T+3 decision may spend the T3
# ticket. ETH revisions were 22/41 (53.7%) and -$5.63 vs 8/9 and +$3.55 primary.
# BTC keeps revisions so the 0.20-0.40 convex tickets can still fire; dumped
# sub-0.20 revisions are scored out by T3_REVISION_ASK_MIN.
T3_BOOK_PRIMARY_ONLY = os.getenv(
    "POLYMARKET_T3_BOOK_PRIMARY_ONLY", "1" if ASSET == "ETH" else "0"
).strip().lower() not in ("0", "false", "no", "off", "")
# BTC revisions below 0.20 are dumped longshots.  The old justification for
# leaving the upper end open was a nine-trade 0.20-0.40 cohort; the forward
# period subsequently showed that price, not the primary/revision label, is the
# stable risk boundary.  T3_BTC_ASK_CEIL below now applies the same maximum
# purchase price to both paths.  ETH keeps primary-only above and never reaches
# the BTC-only ceiling.
T3_REVISION_ASK_MIN = float(os.getenv(
    "POLYMARKET_T3_REVISION_ASK_MIN", "0.20"))
# INITIAL-LEG (is_t3=True) flat ask ceiling (2026-08-22, BTC only). Replaces the
# per-tier ask_cap (STRONG 0.90 / MODERATE-STRONG 0.80 / WEAK 0.80 / MODERATE
# uncapped) for the PRIMARY decision only. Root cause this targets: within every
# tier the ask on our predicted side has been drifting up week over week (WEAK
# median ask 0.76->0.92 wk29->wk34) while hit rate held or improved (72->90%) —
# the tier label was a proxy for R:R, ask IS the R:R driver, and the market is
# steadily re-pricing the T+3 call more expensive at the same accuracy. Holdout
# check (first half vs second half of the 07-11..08-20 tape) confirms a flat
# 0.68 ceiling beats the tier-based caps in BOTH halves: H1 +$11.71/n=76 vs
# tier-based +$10.55/n=162; H2 +$0.84/n=59 vs tier-based -$1.21/n=87 (tier-based
# was UNDERWATER in H2, this flips it positive). Not validated for revisions
# (is_t3=False keep their existing tier caps + T3_REVISION_ASK_MIN untouched)
# or for ETH (no equivalent study run — falls back to the tier table there).
# Expect n to keep shrinking over time as ask continues drifting up; this is a
# price-following filter, not a one-time fix — re-check periodically.
T3_INITIAL_ASK_CEIL = float(os.getenv(
    "POLYMARKET_T3_INITIAL_ASK_CEIL", "0.68")) or None
# BTC universal price/EV guard (2026-08-25).  The live-equivalent replay uses
# first qualifying decision per five-minute window, matching w["t3_staked"].
# On 711 current-gate windows, asks >=0.68 lost despite high raw accuracy because
# the payout could not fund misses.  Stacking ask<=0.68 with trailing vol<6 bps
# produced +$22.37 before 08-15 and +$2.70 after 08-15 (+$4.22 after 08-19),
# versus -$17.99 for the current post-08-15 gate.  Keep this separate from
# T3_INITIAL_ASK_CEIL so an env override cannot accidentally reopen revisions.
# Set to 0 only for an explicit BTC A/B rollback.  ETH is deliberately unchanged.
T3_BTC_ASK_CEIL = float(os.getenv(
    "POLYMARKET_T3_BTC_ASK_CEIL", "0.68")) or None
# Payable + fillable T3 ask band (2026-09-13). Live $1 T3 FAKs that actually
# filled sat at 0.50-0.74 (ETH 100% fill-frac at quote, depth 20-50sh; BTC
# cheap ~0.50 depth ~185sh). Asks >=0.90 print huge depth at worse rungs but
# do not fill at the touch (model fill-frac 0.77; "insufficient book depth
# 0% of $1 fillable to 0.99" with thousands of shares at quote). Floor 0.50
# drops the 0.20-0.40 lottery (ETH <0.50 qualified WR 30%). Ceiling matches
# T3_INITIAL_ASK_CEIL / T3_BTC_ASK_CEIL. The live REST ask is the fill price;
# reason-text 0.51 flickers with a NULL ask column are not staked.
T3_ASK_FLOOR = float(os.getenv("POLYMARKET_T3_ASK_FLOOR", "0.50"))
T3_ASK_CEIL = float(os.getenv(
    "POLYMARKET_T3_ASK_CEIL",
    str(T3_INITIAL_ASK_CEIL if T3_INITIAL_ASK_CEIL is not None else 0.68))) or None
# MARKET-CONDITION SWITCH (2026-08-20). Veto on top of existing book gates
# and live flags — never arms a shadow book. BTC and ETH use different cuts.
# BTC: T3 vol ceiling REMOVED 2026-08-22 (see below); fade on vol >= 10.
# ETH: T3 holds through moderate vol (8-12 primary still pays); fade on
# vol >= 12. Quiet ETH fade is 1-9. Rollback: POLYMARKET_MC_GATE=0.
MC_GATE_ENABLED = os.getenv("POLYMARKET_MC_GATE", "1").strip().lower() not in (
    "0", "false", "no", "off", "")
# T3 vol ceiling. Code default 12bps is the 2026-09-13 cut. Live 2026-09-17
# raised both assets to 20 via POLYMARKET_MC_T3_VOL_MAX (STRONG/MS is a 6-20bps
# displacement; post-restart BTC vol 12.8 and 14.0 were winners, vol 26 lost;
# ETH vel-on + vol 20 is 32/33 WR 97% on 08-25..now vs 27 at vol 12). Rollback:
# drop the env line (returns 12) or POLYMARKET_MC_T3_VOL_MAX=12.
MC_T3_VOL_MAX = float(os.getenv("POLYMARKET_MC_T3_VOL_MAX", "12.0"))
# SWINGBAND/SWINGFADE "fade" family MC gate (vol_3min_bps >= this before the
# quiet-tape veto clears). Replayed 2026-08-22 on real re_flip_shadow ask data
# for both assets (BOT.swingband_shadow_match, vol_3min_bps reconstructed via
# a Coinbase-OHLC proxy — no vol column exists in that table, see
# project_chainlink_data_streams_cost_not_viable): removing the gate raised
# BOTH n and total PnL on both assets — BTC n=5/+$7.56 -> n=61/+$48.27, ETH
# n=12/+$22.62 -> n=44/+$31.41. Sample is thin (n=5-61, far below the T3
# book's thousands-of-rows validation) and vol_3min_bps here is a
# reconstructed proxy, not real production data, so this is a WEAKER
# evidence base than the T3 MC-gate removal above — flagged as a deliberate
# risk call (2026-08-22), not a "validated" fix. mc_allows() still
# fails closed on missing vol_3min_bps regardless of this floor. swingfade
# stays SHADOW (its own MC-gate-off sensitivity was -$25.58/n=49, WR 8.2% —
# do not extend this reasoning to swingfade going live).
MC_FADE_VOL_MIN = float(os.getenv("POLYMARKET_MC_FADE_VOL_MIN", "0.0"))


def _mc_vol_bad(vol_3min_bps):
    return (vol_3min_bps is None
            or (isinstance(vol_3min_bps, float) and not math.isfinite(vol_3min_bps)))


def mc_regime(vol_3min_bps, *, t3_vol_max=None, fade_vol_min=None):
    """QUIET / EXPANDING / UNKNOWN from trailing 3-min vol (bps)."""
    t3_max = MC_T3_VOL_MAX if t3_vol_max is None else t3_vol_max
    fade_min = MC_FADE_VOL_MIN if fade_vol_min is None else fade_vol_min
    if _mc_vol_bad(vol_3min_bps):
        return "UNKNOWN"
    if vol_3min_bps < t3_max:
        return "QUIET"
    if vol_3min_bps >= fade_min:
        return "EXPANDING"
    return "QUIET"


def mc_allows(family, vol_3min_bps, *, enabled=None, t3_vol_max=None,
              fade_vol_min=None):
    """(ok, reason) for book family ``t3`` or ``fade``. Fail-closed on missing vol."""
    on = MC_GATE_ENABLED if enabled is None else enabled
    if not on:
        return True, "mc off"
    if _mc_vol_bad(vol_3min_bps):
        return False, "mc vol_3min missing (fail closed)"
    t3_max = MC_T3_VOL_MAX if t3_vol_max is None else t3_vol_max
    fade_min = MC_FADE_VOL_MIN if fade_vol_min is None else fade_vol_min
    regime = mc_regime(vol_3min_bps, t3_vol_max=t3_max, fade_vol_min=fade_min)
    fam = (family or "").strip().lower()
    if fam == "t3":
        if vol_3min_bps < t3_max:
            return True, f"mc {regime} vol={vol_3min_bps:.2f}<{t3_max:g} t3 on"
        return False, (f"mc {regime} vol={vol_3min_bps:.2f}>={t3_max:g} "
                       f"t3 off (continuation exhausted)")
    if fam == "fade":
        if vol_3min_bps >= fade_min:
            return True, f"mc {regime} vol={vol_3min_bps:.2f}>={fade_min:g} fade on"
        return False, (f"mc {regime} vol={vol_3min_bps:.2f}<{fade_min:g} "
                       f"fade off (quiet dump tape)")
    return False, f"mc unknown family {family!r}"


# A HOLE, not a ceiling. [0.74,0.80) is the band just under MAX_ENTRY_ASK=0.80 —
# "just barely affordable" = maximum adverse selection ([0.76,0.78) alone is
# -$59.63/n=29). The band immediately BELOW it is the best in the file
# ([0.65,0.72) = +$64.40/n=48), so collapsing this into a single cap throws away
# the best zone along with the worst. Set either bound equal to disable.
T3_BOOK_ASK_HOLE_LO = float(os.getenv("POLYMARKET_T3_BOOK_ASK_HOLE_LO", "0.74"))
T3_BOOK_ASK_HOLE_HI = float(os.getenv("POLYMARKET_T3_BOOK_ASK_HOLE_HI", "0.80"))
# §3 sizing. k=20% of the book's OWN compounding bank, floored at PM's $1 minimum
# order and capped by the depth ceiling. See size_position() for the full rationale.
T3_BOOK_STAKE_FRAC = float(os.getenv("POLYMARKET_T3_BOOK_STAKE_FRAC", "0.20"))
T3_BOOK_STAKE_CEIL = float(os.getenv("POLYMARKET_T3_BOOK_STAKE_CEIL", "50"))
T3_BOOK_MIN_STAKE = float(os.getenv("POLYMARKET_T3_BOOK_MIN_STAKE", "1"))
# T3 $100 MIDNIGHT-COMPOUNDING TRIAL (2026-09-23). Separate from the k=20%
# scheme above — off by default, dash-config-gated so it can be armed/disarmed
# without a restart once the 24h $1-flat forward check (started 2026-09-23) is
# reviewed. Derivation: divisor 5.67 = the September-only flat-$1 candidate
# tape's own max drawdown (4.536 stake-units) x 1.25 reserve -> stake = bankroll
# / 5.67, i.e. ~17.6% of the daily-locked bank. Depth cap $79.59 = median
# executable BTC T3 depth AT THE EXACT QUOTE from the 09-23 compounding/depth
# study (POLYMARKET_SELECTED_MODES_COMPOUNDING_AND_DEPTH_2026-09-23.md) — past
# this the compounding formula stops scaling the stake up and holds it flat,
# it does not walk the book for more. DD halt 6.80 stake-units = 1.5x that same
# September study's max DD, deliberately calibrated IN-SAMPLE on the regime
# being traded (see t3_compound_decision docstring) — known limitation, not
# an oversight; it stops the book for a human to re-look, it does not resume
# itself.
T3_COMPOUND_BASE_USD = float(os.getenv("POLYMARKET_T3_COMPOUND_BASE_USD", "100"))
T3_COMPOUND_DIVISOR = float(os.getenv("POLYMARKET_T3_COMPOUND_DIVISOR", "5.67"))
T3_COMPOUND_DEPTH_CAP_USD = float(os.getenv("POLYMARKET_T3_COMPOUND_DEPTH_CAP_USD", "79.59"))
T3_COMPOUND_MIN_STAKE = float(os.getenv("POLYMARKET_T3_COMPOUND_MIN_STAKE", "1"))
T3_COMPOUND_DD_HALT_UNITS = float(os.getenv("POLYMARKET_T3_COMPOUND_DD_HALT_UNITS", "6.80"))


def t3_compound_on():
    """Dash-config wins (flip live, no restart), else the systemd env, else off.
    Same precedence as get_capital() — this is what lets the 24h $1-flat -> $100
    compounding switch happen with a config write instead of a deploy."""
    v = get_config("t3_compound_on", None)
    if v is None:
        v = os.getenv("POLYMARKET_T3_COMPOUND_ON", "0")
    return str(v).strip().lower() in ("1", "true", "on", "yes")


def t3_compound_decision(bankroll, peak, *, divisor=T3_COMPOUND_DIVISOR,
                          depth_cap=T3_COMPOUND_DEPTH_CAP_USD,
                          min_stake=T3_COMPOUND_MIN_STAKE,
                          dd_halt_units=T3_COMPOUND_DD_HALT_UNITS):
    """Pure midnight-compounding stake decision for the T3 $100 bankroll trial.
    Pure like t3_book_ok() — the live path and any replay share one definition.

    `bankroll` is the daily-locked SETTLED capital (see Bot._daily_book_capital,
    which already staked-off-P&L for oppfade/fade/flip — reused here rather than
    re-deriving a midnight snapshot). `peak` is the running high-water mark of
    that same series, persisted by the caller across restarts.

    Drawdown is measured in units of THIS call's own stake (dd / stake), not raw
    dollars, so the halt threshold scales with the bank instead of tripping on
    the first loss once stakes have compounded up. The 6.80-unit default was
    calibrated on the same forward window it will police — it has not been
    tested out-of-regime, so a halt here means stop and look, not a proven trap
    door. Returns (stake_usd, new_peak, halt, halt_reason)."""
    new_peak = max(peak, bankroll)
    raw = bankroll / divisor if divisor > 0 else 0.0
    stake = min(raw, depth_cap)
    if stake < min_stake:
        stake = min_stake if bankroll >= min_stake else 0.0
    stake = math.floor(stake * 100) / 100.0
    dd = new_peak - bankroll
    if stake > 0:
        dd_units = dd / stake
    else:
        dd_units = float("inf") if dd > 0 else 0.0
    halt = dd_units >= dd_halt_units
    reason = ""
    if halt:
        reason = (f"dd ${dd:.2f} / stake ${stake:.2f} = {dd_units:.2f}x >= "
                  f"{dd_halt_units:.2f}x (1.5x the Sept-study 4.536-unit max DD)")
    return (0.0 if halt else stake), new_peak, halt, reason
# velocity_frac floor. Default 0 = reject a real negative (NULL still passes,
# spec §2 Stage 3). Set POLYMARKET_T3_BOOK_VELOCITY_MIN=off to compile the
# gate out. BTC live 2026-09-17: cheap STRONG/MS vel<0 is still +EV
# (n=64 WR 81.2% +$33.13 on 08-25..now; post-restart vel-off + vol20 is
# 4W/0L). ETH MUST stay at 0 — same cohort vel<0 is WR 56.8% / +$0.28, and
# post-restart vel-off is 2W/4L -$3.01 with H1→H2 decay. Do not copy BTC's
# env onto the ETH unit.
def _t3_velocity_min_from_env(raw):
    s = (raw if raw is not None else "0").strip().lower()
    if s in ("off", "none", "false", "no", ""):
        return None
    return float(s)


T3_BOOK_VELOCITY_MIN = _t3_velocity_min_from_env(
    os.getenv("POLYMARKET_T3_BOOK_VELOCITY_MIN", "0"))
# --- T3 EXECUTION () ---------------------------------------
# The entry FAK is a DOLLAR sweep of the real ask ladder, judged on the fraction
# of the TICKET it fills — the same shape the live swingband/swingfade books use
# via sweep_ladder(). It replaces `walk_fill(...).filled < MIN_SHARES`, which was
# an ask ceiling in disguise: walk_fill can never return more than the shares
# asked for, so at a $1 ticket `filled >= 5` required `ask <= 0.20`. Measured over
# the 7.6 days to 2026-08-06 that silently refused 230 of 233 gate-passing
# decisions as "insufficient book depth" — 92% of them with the ENTIRE $1 ticket
# resting at the quoted ask (median 122sh available against a 1.49sh request).
# T3_PAYUP is the spec's pay-up bound (PM_T3_BANKBOOK_FILTERS.md §3): fill 95% at
# the quote, then pay at most one cent. Replay says +1c buys 3 extra trades at
# +1.80bps mean slippage and walking further (to the hard 0.80) adds NOTHING —
# a ~1.5sh order never leaves the touch — so 0.80 stays as the outer bound only.
T3_PAYUP = float(os.getenv("POLYMARKET_T3_PAYUP", "0.01"))
T3_MIN_FILL_FRAC = float(os.getenv("POLYMARKET_T3_MIN_FILL_FRAC", "0.95"))
# T3 (BTC + ETH): skip the order if the ask has dropped this far below the decision ask by
# send time (0 disables). See the pre-send re-check at the T3 executor.buy.
T3_PRESEND_MAX_DROP = float(os.getenv("POLYMARKET_T3_PRESEND_MAX_DROP", "0.10"))
# T+3 latency (2026-09-28): bankroll reads throttled to this cadence and skipped,
# with the PM outcome sweep, within +-T3_QUIET_S of the decision.
BANKROLL_REFRESH_S = float(os.getenv("POLYMARKET_BANKROLL_REFRESH_S", "20"))
T3_QUIET_S = float(os.getenv("POLYMARKET_T3_QUIET_S", "10"))


def t3_book_stake_for(bank, wallet_usdc=None):
    """§3 sizing: clamp(k x own bank, $1 floor, $50 depth ceiling), then the wallet.
    ONE definition, called by size_position() and published to the dash — a strip
    that recomputes the formula inline is how the header drifts from what we bet
    (the same failure the SWINGFADE `swingfade_stake_for` export was written for).
    Deliberately unrounded: the caller divides by `ask` for shares."""
    stake = min(T3_BOOK_STAKE_FRAC * max(0.0, bank), T3_BOOK_STAKE_CEIL,
                UNIFORM_STAKE_USD)
    if stake < T3_BOOK_MIN_STAKE:
        stake = T3_BOOK_MIN_STAKE
    stake = min(stake, UNIFORM_STAKE_USD)
    if wallet_usdc is not None and stake > wallet_usdc:
        stake = wallet_usdc                      # never overspend the wallet
    return stake


def t3_breakeven_wr(ask):
    """Break-even win rate at `ask` under PM's 7%*stake*(1-price) fee, charged on
    BOTH wins and losses. EV=0 at p=be_wr. Rounded to 4dp because that is what the
    source workbook stores — matches its `be_wr` column on 835/835 rows, so the
    live gate and the research file cannot drift apart on a boundary case."""
    if ask is None or not (0.0 < ask < 1.0):
        return None
    win = 10.0 * (1.0 - ask) / ask - 0.7 * (1.0 - ask)
    loss = -10.0 - 0.7 * (1.0 - ask)
    return round((-loss) / (win - loss), 4)


# --- T3 adaptive session-noise move floor (2026-09-27) ----------------------
# pm_t3_adaptive_analyser.py (cron */15) writes, per asset, min |decision_bps| per UTC
# session (asia/london/ldn_ny/ny/late) for weekday and weekend = 0.8 x that session's
# robust noise over the last 3 same-type days. Replaces the fixed 4bps first-principles
# floor ONLY while polymarket_config `t3_adaptive_live`=1 (per-asset key, read live).
# Flag off / file missing / stale / session absent -> the fixed 4bps, never looser.
T3_FIXED_MIN_MOVE_BPS = 4.0
T3_ADAPTIVE_STATE = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                 "reports", "pm_t3_adaptive_state.json")
T3_ADAPTIVE_MAX_AGE_S = 45 * 60
T3_ADAPTIVE_CLAMP = (1.5, 8.0)
_t3_adapt_cache = {"flag_at": 0.0, "flag": False, "mtime": None, "state": None}


def t3_min_move_bps(window_start_utc):
    """(min_bps, source) for the first-principles T3 move floor of this window."""
    from pm_t3_adaptive_analyser import session as _t3_session
    now = time.time()
    c = _t3_adapt_cache
    if now - c["flag_at"] > 30:
        c["flag"] = str(get_config("t3_adaptive_live", "0")).strip() == "1"
        c["flag_at"] = now
    if not c["flag"]:
        return T3_FIXED_MIN_MOVE_BPS, "fixed"
    try:
        mt = os.path.getmtime(T3_ADAPTIVE_STATE)
        if c["mtime"] != mt:
            with open(T3_ADAPTIVE_STATE) as f:
                c["state"], c["mtime"] = json.load(f), mt
        st = c["state"]
        gen = datetime.fromisoformat(st["generated_at"])
        if (datetime.now(timezone.utc) - gen).total_seconds() > T3_ADAPTIVE_MAX_AGE_S:
            return T3_FIXED_MIN_MOVE_BPS, "fixed (adaptive state stale)"
        ws = window_start_utc if window_start_utc.tzinfo else window_start_utc.replace(tzinfo=timezone.utc)
        ws = ws.astimezone(timezone.utc)
        dt = "weekend" if ws.weekday() >= 5 else "weekday"
        sess = _t3_session(ws)
        v = float(st["assets"][ASSET]["tables"][dt][sess])
        if not math.isfinite(v):
            raise ValueError(v)
        v = min(T3_ADAPTIVE_CLAMP[1], max(T3_ADAPTIVE_CLAMP[0], v))
        return v, f"adaptive {dt}/{sess}"
    except Exception as e:
        return T3_FIXED_MIN_MOVE_BPS, f"fixed (adaptive unavailable: {type(e).__name__})"


def t3_book_ok(*, window_start_utc, tier, ask_pred, adx, velocity_frac,
               decision_bps, is_t3=True, vol_3min_bps=None,
               shadow_challenger=False):
    """(ok, reason) for the t3 bank book's entry stack. Pure, so the live path,
    any scorer and any backtest share ONE definition — the research->prod audit's
    named failure mode is a model scoring a book we are not actually running.

    FAIL-CLOSED on missing/non-finite inputs, deliberately, and for the reason
    swingfade_ok() documents: NaN compares False against every `>` and `<=` here,
    so a NULL slipped in as NaN would read as silent PERMISSION and quietly run an
    ungated book. `velocity_frac` is the ONE exception — the spec says treat NULL
    as pass, and it is genuinely absent on early rows."""
    def _bad(x):
        return x is None or (isinstance(x, float) and not math.isfinite(x))

    if not T3_BOOK_ON:
        return False, "t3 book disabled"
    # Shadow-only challenger: primary T+3 VERY STRONG/EXTREME calls, with the
    # observed ask retained as measurement rather than used as a hard veto.
    # This intentionally bypasses the live calendar/ADX/velocity/MC and 0.68 ask
    # stack so the shadow can establish forward economics.  Execution safety is
    # enforced by the caller: this branch is passed only while t3_live=0.
    if shadow_challenger:
        if ASSET != "BTC":
            return False, "shadow challenger is BTC-only"
        if not is_t3:
            return False, "shadow challenger is primary T3 only"
        if tier not in BTC_T3_SHADOW_TIERS:
            return False, f"shadow challenger tier {tier!r} excluded"
        if _bad(ask_pred) or not (0.0 < ask_pred < 1.0):
            return False, f"ask_pred missing/invalid ({ask_pred!r})"
        warning = (f"; ask warning {ask_pred:.3f}>{BTC_T3_SHADOW_ASK_WARN:.2f}"
                   if ask_pred > BTC_T3_SHADOW_ASK_WARN else "")
        return True, f"ok shadow challenger{warning}"
    # --- Stage 1: T3 calendar gate (see dow_stake_ok) ---
    # Missing is rejected HERE rather than delegated: dow_stake_ok(None) falls back
    # to wall clock, which is right for a caller that has no window but wrong for a
    # book scoring a specific one. t3 asserts it holds a window; hold it to that.
    if window_start_utc is None or not isinstance(window_start_utc, datetime):
        return False, "window_start_utc missing"
    _dow_ok, _dow_why = dow_stake_ok(window_start_utc)
    if not _dow_ok:
        return False, _dow_why
    if T3_FIRST_PRINCIPLES:
        # Exact frozen rules from first_principles_polymarket_study.py.  The
        # assets are intentionally different and revisions are excluded because
        # the historical replay used one primary T+3 decision per window.
        if not is_t3:
            return False, f"{ASSET} first-principles T3 is primary-only"
        if _bad(ask_pred) or not (T3_ASK_FLOOR <= ask_pred <= T3_ASK_CEIL):
            return False, (f"ask {ask_pred} outside first-principles band "
                           f"[{T3_ASK_FLOOR:.2f},{T3_ASK_CEIL:.2f}]")
        if _bad(decision_bps):
            return False, f"decision_bps missing/invalid ({decision_bps!r})"
        move = abs(float(decision_bps))
        min_move, min_src = t3_min_move_bps(window_start_utc)
        if ASSET == "BTC":
            if move < min_move:
                return False, f"BTC move {move:.2f} < {min_move:.2f}bps ({min_src})"
            if move < 6.0:
                if _bad(velocity_frac) or velocity_frac < 0:
                    return False, (f"BTC 4-6bps shoulder requires velocity>=0 "
                                   f"({velocity_frac!r})")
        elif ASSET == "ETH":
            if move < min_move:
                return False, f"ETH move {move:.2f} < {min_move:.2f}bps ({min_src})"
            # 2026-09-24 (H2): velocity==0 is the worst ETH bucket (~14% of
            # windows, pre-Sept EV -0.078/ticket); strictly positive only.
            if _bad(velocity_frac) or velocity_frac <= 0:
                return False, f"ETH requires velocity>0 ({velocity_frac!r})"
        else:
            return False, f"first-principles T3 unsupported asset {ASSET}"
        return True, f"ok {ASSET} first-principles T3 (move {move:.2f} >= {min_move:.2f} {min_src})"
    # Forward-validated WR filter. Use absolute displacement so Up and Down calls
    # are treated symmetrically; missing/non-finite values fail closed.
    if _bad(decision_bps) or abs(decision_bps) < T3_BOOK_MIN_ABS_BPS:
        return False, (f"abs decision_bps {decision_bps} < "
                       f"{T3_BOOK_MIN_ABS_BPS}")
    # --- Stage 2: tier x ask x breakeven ---
    if tier not in T3_BOOK_TIER_RULES:
        return False, f"tier {tier!r} not in book"
    if _bad(ask_pred) or not (0.0 < ask_pred < 1.0):
        return False, f"ask_pred missing/invalid ({ask_pred!r})"
    if T3_ASK_FLOOR > 0 and ask_pred < T3_ASK_FLOOR:
        return False, (f"ask {ask_pred:.3f} < floor {T3_ASK_FLOOR:.2f} "
                       f"(not a fillable cheap-STRONG ticket)")
    if (not is_t3) and T3_REVISION_ASK_MIN > 0 and ask_pred < T3_REVISION_ASK_MIN:
        return False, (f"revision ask {ask_pred:.3f} < "
                       f"{T3_REVISION_ASK_MIN:.2f} (keep convex 0.20+)")
    ask_cap, be_cap = T3_BOOK_TIER_RULES[tier]
    if is_t3 and T3_INITIAL_ASK_CEIL is not None:
        ask_cap = T3_INITIAL_ASK_CEIL
    if T3_BTC_ASK_CEIL is not None:
        ask_cap = min(ask_cap, T3_BTC_ASK_CEIL) if ask_cap is not None else T3_BTC_ASK_CEIL
    if T3_ASK_CEIL is not None:
        ask_cap = min(ask_cap, T3_ASK_CEIL) if ask_cap is not None else T3_ASK_CEIL
    if ask_cap is not None and ask_pred > ask_cap:
        return False, f"{tier} ask {ask_pred:.3f} > {ask_cap:.2f}"
    be = t3_breakeven_wr(ask_pred)
    if be is None or be > be_cap:
        return False, f"{tier} be_wr {be} > {be_cap:.2f}"
    # --- Stage 3: the new filters ---
    if _bad(adx) or adx < T3_BOOK_ADX_MIN:
        return False, f"adx {adx} < {T3_BOOK_ADX_MIN}"
    if (T3_BOOK_ASK_HOLE_LO < T3_BOOK_ASK_HOLE_HI
            and T3_BOOK_ASK_HOLE_LO <= ask_pred < T3_BOOK_ASK_HOLE_HI):
        return False, (f"ask {ask_pred:.3f} in dead band "
                       f"[{T3_BOOK_ASK_HOLE_LO:.2f},{T3_BOOK_ASK_HOLE_HI:.2f})")
    # velocity_frac: NULL passes (spec §2 Stage 3). A real value below the
    # floor fails. T3_BOOK_VELOCITY_MIN is None when the gate is compiled out.
    if (T3_BOOK_VELOCITY_MIN is not None
            and not _bad(velocity_frac)
            and velocity_frac < T3_BOOK_VELOCITY_MIN):
        return False, (f"velocity_frac {velocity_frac} < "
                       f"{T3_BOOK_VELOCITY_MIN}")
    _mc_ok, _mc_why = mc_allows("t3", vol_3min_bps)
    if not _mc_ok:
        return False, _mc_why
    return True, "ok"


# ---------------------------------------------------------------------------
# Telemetry — process liveness, event-loop lag, swallowed errors
# ---------------------------------------------------------------------------
# Written after the 2026-07-28 outage: the WS order book went dark for ~45 min
# (five straight windows logged `ask n/a`, swingfade skipped every one) and NOTHING
# in the bot or the dashboard could have shown it. The subscription was healthy;
# blocking `requests` calls were pinning the event loop so nobody drained the
# socket. Three things were missing and are added here.
_ERRS = {}          # site -> {"n", "last", "ts"} — swallowed exceptions, counted


def _err(site, e):
    """Count an exception the caller is about to swallow.

    75 of this module's 88 `except` handlers were silent. A handler that degrades
    to None is usually CORRECT — a missing price should skip a window, not crash
    the bot — but silent is not the same as harmless: the same `pass` covers
    "quiet market" and "the price feed has been down for an hour". Counting is
    enough to tell those apart without changing any control flow.
    """
    d = _ERRS.setdefault(site, {"n": 0, "last": None, "ts": None})
    d["n"] += 1
    d["last"] = f"{type(e).__name__}: {str(e)[:120]}"
    d["ts"] = time.time()


def _errs_snapshot(max_age_s=3600.0):
    """Sites that have errored in the last hour, worst first."""
    now = time.time()
    out = {k: {**v, "age_s": round(now - v["ts"], 1)}
           for k, v in _ERRS.items() if v["ts"] and now - v["ts"] <= max_age_s}
    return dict(sorted(out.items(), key=lambda kv: -kv[1]["n"]))


class _Health:
    """Rolling liveness for the trading loop.

    `lag_ms` is the important one: a watchdog coroutine sleeps 1.0s in a loop and
    reports how much LATE it woke. That overshoot is time the event loop spent
    unable to run anything — the direct measure of the failure above, and it is
    independent of whether tick() itself looks healthy. Any lag over BOOK_STALE_S
    means the order book WILL have gone stale, so those are counted separately as
    `lag_breaches`: a non-zero count is the book going dark, stated as a number.
    """
    WINDOW_S = 300.0

    def __init__(self):
        self._lock = threading.Lock()
        self._ticks = deque()          # (ts, ms)
        self._lags = deque()           # (ts, ms)
        self.tick_n = 0
        self.lag_breaches = 0
        self.last_tick_ts = 0.0
        self.started = time.time()

    def _trim(self, dq, now):
        cut = now - self.WINDOW_S
        while dq and dq[0][0] < cut:
            dq.popleft()

    def record_tick(self, ms):
        now = time.time()
        with self._lock:
            self.tick_n += 1
            self.last_tick_ts = now
            self._ticks.append((now, ms))
            self._trim(self._ticks, now)

    def record_lag(self, ms):
        now = time.time()
        with self._lock:
            self._lags.append((now, ms))
            self._trim(self._lags, now)
            breach = ms >= BOOK_STALE_S * 1000.0
            if breach:
                self.lag_breaches += 1
                n = self.lag_breaches
        if breach:
            # the counter alone can't say WHEN the book went dark; stamp each one
            # so it can be lined up against skips/fills in the journal
            print(f"[{datetime.now(timezone.utc):%H:%M:%S}] BOOK-BLACKOUT #{n} "
                  f"loop blocked {ms / 1000:.1f}s (cutoff {BOOK_STALE_S:.0f}s), "
                  f"ended {datetime.now(timezone.utc):%H:%M:%S} UTC, "
                  f"started ~{datetime.fromtimestamp(now - ms / 1000, timezone.utc):%H:%M:%S} UTC",
                  flush=True)

    def snapshot(self):
        now = time.time()
        with self._lock:
            self._trim(self._ticks, now)
            self._trim(self._lags, now)
            tv = [m for _, m in self._ticks]
            lv = [m for _, m in self._lags]
            tick_n = self.tick_n
            last_tick_ts = self.last_tick_ts
            lag_breaches = self.lag_breaches

        def p95(v):
            if not v:
                return None
            s = sorted(v)
            return round(s[min(len(s) - 1, int(len(s) * 0.95))], 1)

        return {
            "uptime_s": int(now - self.started),
            "ticks": tick_n,
            # age of the last COMPLETED tick — if this climbs the loop is wedged,
            # which no other field in the payload could previously reveal
            "tick_age_s": (round(now - last_tick_ts, 1)
                           if last_tick_ts else None),
            "tick_ms_p95": p95(tv),
            "tick_ms_max": round(max(tv), 1) if tv else None,
            "lag_ms_p95": p95(lv),
            "lag_ms_max": round(max(lv), 1) if lv else None,
            "lag_breaches": lag_breaches,
            "lag_budget_ms": int(BOOK_STALE_S * 1000),
            "window_s": int(self.WINDOW_S),
        }


HEALTH = _Health()


async def _loop_watchdog(period=1.0):
    """Sleep `period` forever and record how late each wake-up was."""
    while True:
        t0 = time.perf_counter()
        await asyncio.sleep(period)
        HEALTH.record_lag(max(0.0, (time.perf_counter() - t0 - period) * 1000.0))


# ---------------------------------------------------------------------------
# Blocking-call offload
# ---------------------------------------------------------------------------
async def _offload(fn, *a, **kw):
    """Run a BLOCKING helper on a worker thread instead of the event loop.

    Database-backed price/history reads and other synchronous helpers must not pin
    the event loop: doing so stops BookFeed._consume() draining its socket and can
    age the cached order book past BOOK_STALE_S.
    """
    return await asyncio.to_thread(fn, *a, **kw)


# ---------------------------------------------------------------------------
# 1-MINUTE BARS — built from the CHAINLINK collector, not Binance klines.
# ---------------------------------------------------------------------------
# Ported 2026-07-30 ("for pm BTC the data is from chainlink and the book data
# is from pm"). This was the LAST Binance dependency in the bot: ADX(14)/ADX(7),
# adx_confirmed, the intra-window adx_series slope and vol_3min_bps were all computed
# on Binance 1-min klines, on the argument that a trend gate is resolution-
# insensitive. It is still Binance data feeding live gates, so it goes.
#
# MEASURED before switching (1,000 overlapping minutes, 2026-07-30) — this is a real
# change to gate VALUES, not a no-op:
#   ADX_14: corr 0.979, mean(CL-BN) -0.31, mean|CL-BN| 1.51
#           threshold crossings disagree 6.0% of minutes at 17, 5.5% at 25, 0.9% at 50
#   ADX_7 : corr 0.970, mean(CL-BN) -0.60, mean|CL-BN| 2.15
#           disagree 2.4% at 17, 5.7% at 25, 2.0% at 55
#   vol_3min_bps: 10.44 (CL) vs 10.47 (BN)
# The bias is slightly LOWER on Chainlink, so an ADX FLOOR passes marginally less
# often. Anything calibrated on Binance ADX (the swingfade |slope|<=5 band, the t3
# bankbook adx>=17 hypothesis, FADE_MIN_ADX_SHORT=55) is now reading a feed shifted
# ~1.5-2 points, and pre-registered thresholds should be judged on forward fires from
# here, not spliced onto pre-07-30 history.
#
# Bars come from chainlink_prices (~52 obs/minute), so high/low are real intrabar
# extremes of the settlement feed. 31 of 29,651 historical bars have <30 obs; a
# thin/missing minute distorts that bar's range slightly rather than the series.
def get_klines_1m(limit=100):
    """1-min OHLC from the Chainlink collector. Same column contract as the old
    Binance kline frame (open_time seconds, open/high/low/close floats) so adx_now()
    and the vol_3min_bps caller are unchanged. Time-bounded so it reads ~`limit`
    minutes, never the whole 29k-bar table."""
    lo = int(time.time()) - int(limit + 5) * 60
    with ENGINE.begin() as c:
        df = pd.read_sql(text("""
            SELECT (observations_ts / 60) * 60 AS open_time,
                   (array_agg(benchmark_price ORDER BY observations_ts))[1] AS open,
                   max(benchmark_price) AS high,
                   min(benchmark_price) AS low,
                   (array_agg(benchmark_price ORDER BY observations_ts DESC))[1]
                       AS close,
                   count(*) AS obs
              FROM chainlink_prices
             WHERE feed_id = :fid AND observations_ts >= :lo
          GROUP BY 1
          ORDER BY 1
        """), c, params={"fid": PRICE_FEED_KEY, "lo": lo})
    for col in ("open", "high", "low", "close"):
        df[col] = df[col].astype(float)
    return df.tail(limit).reset_index(drop=True)


# binance_spot_price / open_price_for_anchor / binance_recent_momentum were DELETED
# 2026-07-30 ("totally delink binance from pm BTC... remove any trace of
# binance in pm BTC"). Their three jobs are gone or moved:
#   • spot price + kline open  -> price fallbacks; now Chainlink REST -> chainlink_
#     prices collector -> defer. A different feed may never stand in for a PM price.
#   • the ~3s LEAD (price_open_binance / *_binance columns / fade confirm / exit
#     reversal / late bail / maker timing) -> REMOVED outright, not ported: the lead
#     is not usable here, and every consumer of it was measurement scaffolding or a
#     gate keyed to a feed PM does not settle on.
#   • 6s momentum for velocity_frac -> chainlink_recent_momentum(), on the feed that
#     actually settles.


# ---------------------------------------------------------------------------
# Polymarket RTDS Chainlink rebroadcast — sole underlying-price transport.
# The collector subscribes to btc/usd and eth/usd and stores both in the existing
# `chainlink_prices` table. There is no direct Chainlink API or exchange fallback.
# ---------------------------------------------------------------------------
PRICE_FEED_KEY = f"{ASSET.lower()}/usd"
_RTDS_MEMORY = None


def chainlink_db_spot_price_at(ts, tol_s=8):
    """Last PM-rebroadcast report at/just-before `ts`, provided it is within
    `tol_s` of `ts`. The tolerance is what makes this safe for a BOUNDARY price:
    if the collector has a hole around `ts` we return None (caller defers) rather
    than silently resolving on a price from minutes earlier."""
    if _RTDS_MEMORY is not None and int(ts) >= int(time.time())-3500:
        rows = _RTDS_MEMORY.range(int(ts)-int(tol_s)-1, int(ts))
        return rows[-1][1] if rows else None
    try:
        with ENGINE.begin() as c:
            row = c.execute(text("""
                SELECT benchmark_price, observations_ts
                  FROM chainlink_prices
                 WHERE feed_id = :fid
                   AND observations_ts <= :ts
                   AND observations_ts >= :lo
              ORDER BY observations_ts DESC
                 LIMIT 1
            """), {"fid": PRICE_FEED_KEY, "ts": int(ts),
                   "lo": int(ts) - int(tol_s)}).fetchone()
        return float(row[0]) if row else None
    except Exception:
        return None


# Polymarket's BTC and ETH markets resolve from Chainlink's 30-second TWAP while
# RTDS exposes the component ~1Hz spot series. Reconstruct the complete trailing
# minute for both assets. Incomplete input fails closed rather than silently
# reverting to the wrong oracle.
BTC_TWAP_WINDOW_S = int(os.getenv("POLYMARKET_BTC_TWAP_WINDOW_S", "60"))
BTC_TWAP_MIN_OBS = int(os.getenv("POLYMARKET_BTC_TWAP_MIN_OBS", "40"))
BTC_TWAP_MIN_SPAN_S = int(os.getenv("POLYMARKET_BTC_TWAP_MIN_SPAN_S", "50"))


def chainlink_db_twap_at(ts, tol_s=8):
    ts = int(ts)
    if _RTDS_MEMORY is not None and ts >= int(time.time())-3500:
        rows = _RTDS_MEMORY.range(ts-BTC_TWAP_WINDOW_S, ts)
        if (len(rows) < BTC_TWAP_MIN_OBS or not rows
                or rows[-1][0] < ts-int(tol_s)
                or rows[-1][0]-rows[0][0] < BTC_TWAP_MIN_SPAN_S):
            return None
        return sum(p for _,p in rows)/len(rows)
    try:
        with ENGINE.begin() as c:
            row = c.execute(text("""
                SELECT avg(benchmark_price), count(*),
                       min(observations_ts), max(observations_ts)
                  FROM chainlink_prices
                 WHERE feed_id = :fid
                   AND observations_ts > :lo
                   AND observations_ts <= :ts
            """), {"fid": PRICE_FEED_KEY,
                    "lo": ts - BTC_TWAP_WINDOW_S, "ts": ts}).fetchone()
        if not row or row[0] is None:
            return None
        avg_price, n, oldest, newest = row
        if (int(n) < BTC_TWAP_MIN_OBS
                or int(newest) < ts - int(tol_s)
                or int(newest) - int(oldest) < BTC_TWAP_MIN_SPAN_S):
            return None
        return float(avg_price)
    except Exception:
        return None


T3_LASTTICK_MIN_BPS = float(os.getenv("POLYMARKET_T3_LASTTICK_MIN_BPS", "0.0"))


def latest_tick_price(max_age_s=8):
    """Newest RAW PM-feed tick (not the 60s TWAP), or None if older than max_age_s.
    The TWAP is the settlement quantity, but at T+3 it describes T+2..T+3 — ~30s
    stale as a PREDICTOR of the closing TWAP; the book prices off this tick."""
    if _RTDS_MEMORY is not None:
        row = _RTDS_MEMORY.latest(max_age_s)
        return row[1] if row else None
    try:
        with ENGINE.begin() as c:
            row = c.execute(text("""
                SELECT benchmark_price, observations_ts FROM chainlink_prices
                 WHERE feed_id = :fid ORDER BY observations_ts DESC LIMIT 1
            """), {"fid": PRICE_FEED_KEY}).fetchone()
        if not row or row[0] is None or time.time() - int(row[1]) > max_age_s:
            return None
        return float(row[0])
    except Exception:
        return None


def t3_price_diag(ts):
    """(age_s of newest PM tick, latest tick vs 60s TWAP in bps) at `ts` — T3-TIMING
    diagnostic: the decision move is a 60s TWAP, the book prices off the latest tick."""
    try:
        with ENGINE.begin() as c:
            row = c.execute(text("""
                SELECT (SELECT benchmark_price FROM chainlink_prices
                         WHERE feed_id = :fid AND observations_ts <= :ts
                         ORDER BY observations_ts DESC LIMIT 1),
                       (SELECT max(observations_ts) FROM chainlink_prices
                         WHERE feed_id = :fid AND observations_ts <= :ts)
            """), {"fid": PRICE_FEED_KEY, "ts": int(ts)}).fetchone()
        twap = chainlink_db_twap_at(ts)
        if not row or row[0] is None or not twap:
            return None, None
        return ts - int(row[1]), (float(row[0]) - twap) / twap * 1e4
    except Exception:
        return None, None


# TWAP-LOCK (2026-09-29). Settlement = mean of the ~1Hz PM-feed ticks over
# [T+240, T+300] vs price_open (the previous window's same 60s TWAP) — verified
# against Gamma eventMetadata finalPrice/priceToBeat to 0.04bps median, and it
# reproduces 99.55% of 2008 BTC outcomes. From T+240 the average locks in second
# by second, so late in the window the leader is near-certain. Validated on 862
# BTC windows of REAL Polymarket fills (data-api trades, first leader-side fill
# at/after T+E, fee-net, feed 2s stale): T+280..294 with |gap|>=0.5bps -> 100%
# WR, n=188-322 per offset, EV ~+0.9-1.2c per $1 (fills sit at ~0.99). The edge
# is THIN: one loss costs ~100 wins. Gate below is tighter than validated
# (>=1bps, ask floor) so a stale/wrong feed that disagrees with the book skips.
TWAP_LOCK_T0_S = int(os.getenv("POLYMARKET_TWAP_LOCK_T0_S", "280"))
TWAP_LOCK_T1_S = int(os.getenv("POLYMARKET_TWAP_LOCK_T1_S", "294"))
TWAP_LOCK_MIN_BPS = float(os.getenv("POLYMARKET_TWAP_LOCK_MIN_BPS", "1.0"))
TWAP_LOCK_MIN_PX = float(os.getenv("POLYMARKET_TWAP_LOCK_MIN_PX", "0.90"))
TWAP_LOCK_MAX_PX = float(os.getenv("POLYMARKET_TWAP_LOCK_MAX_PX", "0.99"))
TWAP_LOCK_MAX_TICK_AGE_S = float(os.getenv("POLYMARKET_TWAP_LOCK_MAX_TICK_AGE_S", "4"))
TWAP_LOCK_MIN_COVERAGE = float(os.getenv("POLYMARKET_TWAP_LOCK_MIN_COVERAGE", "0.8"))
TWAP_LOCK = os.getenv("POLYMARKET_TWAP_LOCK", "1").strip().lower() in (
    "1", "true", "yes", "on")


# TWAP-EDGE (2026-09-29). The settlement TWAP averages the last 60s, so it moves
# ~1/sqrt(3) as much as spot over that minute; a small lead at T+240 holds far
# more often than a spot-vol pricer assumes. P(leader wins | elapsed bucket,
# |gap| bucket), gap = projected TWAP vs price_open, feed 2s stale, fit on 2008
# BTC windows 09-22..09-29. Out-of-sample check (table fit days 4-7 ago, tested
# on real Polymarket taker-BUY fills of the last 3 days, first per window, fee-
# net): edge>0.10 -> n=166, +$0.22/$1, positive all 4 days; independent ask
# snapshots (pm_window_ask_samples) edge>0.10 n=7, WR 86% at avg ask 0.60.
# Leader side only (trailing side was -$0.01). NOT family-wise clean (4
# thresholds tried, z~2): capped $1 bet with a small ledger, not proven edge.
TWAP_EDGE_GAP_EDGES = (0.0, 0.25, 0.5, 1.0, 2.0, 3.0, 5.0)
TWAP_EDGE_P = {
    240: (0.612, 0.703, 0.821, 0.830, 0.939, 0.978, 0.995),  # 1-2bp 0.910->0.830 (14 Oct 2026: measured 48/58=83%, CI 71-90%; live 3/6)
    250: (0.630, 0.848, 0.880, 0.948, 0.971, 0.993, 0.998),
    260: (0.712, 0.876, 0.912, 0.975, 0.993, 0.995, 1.000),
}
TWAP_EDGE_T0_S = int(os.getenv("POLYMARKET_TWAP_EDGE_T0_S", "240"))
TWAP_EDGE_T1_S = int(os.getenv("POLYMARKET_TWAP_EDGE_T1_S", "262"))
TWAP_EDGE_MIN_EDGE = float(os.getenv("POLYMARKET_TWAP_EDGE_MIN_EDGE", "0.10"))
TWAP_EDGE_MIN_PX = float(os.getenv("POLYMARKET_TWAP_EDGE_MIN_PX", "0.35"))
TWAP_EDGE_PRESEND_MAX_DROP = float(os.getenv(
    "POLYMARKET_TWAP_EDGE_PRESEND_MAX_DROP", "0.10"))
TWAP_DOWN_POSTFILL_MAX_DROP = 0.10  # operator-approved Down-only early exit
TWAP_EDGE_MAX_PX = float(os.getenv("POLYMARKET_TWAP_EDGE_MAX_PX", "0.90"))
TWAP_EDGE = os.getenv("POLYMARKET_TWAP_EDGE", "1").strip().lower() in (
    "1", "true", "yes", "on")
TWAP_CONTINUOUS_CALIBRATION = os.getenv(
    "POLYMARKET_TWAP_CONTINUOUS_CALIBRATION", "1").lower() in ("1", "true", "yes", "on")
TWAP_CALIBRATION_ENFORCE = os.getenv(
    "POLYMARKET_TWAP_CALIBRATION_ENFORCE", "0").lower() in ("1", "true", "yes", "on")


def twap_edge_p(elapsed_s, gap_abs_bps):
    """Empirical leader win probability, or None outside the fitted range."""
    kb = (int(elapsed_s) // 10) * 10
    row = TWAP_EDGE_P.get(kb)
    if row is None:
        return None
    i = 0
    for j, lo in enumerate(TWAP_EDGE_GAP_EDGES):
        if gap_abs_bps >= lo:
            i = j
    return row[i]


def twap_edge_policy():
    """Single current policy snapshot used by entry and model validation."""
    policy = twap_quality.load()
    if ASSET == 'BTC' and str(get_config('twap_edge_shadow_candidate', '0')) == '1':
        from pm_twap_shadow_ladder import candidate_policy
        policy = candidate_policy(policy)
    policy = dict(policy,
        shadow_min_ask=float(policy.get('shadow_min_ask', TWAP_EDGE_MIN_PX)),
        shadow_max_ask=min(.85, float(policy.get('shadow_max_ask',
            get_config('twap_edge_max_px', TWAP_EDGE_MAX_PX)))),
        shadow_min_edge=max(.05, float(policy.get('shadow_min_edge', TWAP_EDGE_MIN_EDGE))),
        entry_t0_s=TWAP_EDGE_T0_S, entry_t1_s=TWAP_EDGE_T1_S)
    if ASSET == 'BTC' and str(get_config('twap_edge_shadow_weekend', '0')) == '1':
        policy.update(weekend_min_gap_bps=.25, weekend_timezone='Asia/Kolkata')
    return policy


def twap_lock_estimate(anchor):
    """Projected settlement TWAP for the window at `anchor`: the known part of
    [T+240, T+300] as a 1s step function of PM-feed ticks, plus the newest tick
    held for the remaining seconds. Returns (est, newest_ts, coverage) or None.
    Fails closed on a stale newest tick or thin tick coverage."""
    anchor = int(anchor)
    lo = anchor + WINDOW_S - 60
    if _RTDS_MEMORY is not None:
        rows = _RTDS_MEMORY.range(lo-30, min(int(time.time()), anchor+WINDOW_S))
        return twap_cal.projected_close(anchor, time.time(), rows)
    try:
        with ENGINE.begin() as c:
            rows = c.execute(text("""
                SELECT observations_ts, benchmark_price FROM chainlink_prices
                 WHERE feed_id = :fid AND observations_ts > :lo0
                   AND observations_ts <= :hi
                 ORDER BY observations_ts
            """), {"fid": PRICE_FEED_KEY, "lo0": lo - 30,
                    "hi": min(int(time.time()), anchor + WINDOW_S)}).fetchall()
    except Exception:
        return None
    return twap_cal.projected_close(anchor, time.time(), rows)


def chainlink_db_price_at(ts, tol_s=8):
    """Resolution-aligned reconstructed Chainlink TWAP for BTC or ETH."""
    return chainlink_db_twap_at(ts, tol_s=tol_s)


REFERENCE_CONSENSUS_WINDOW_S = int(os.getenv(
    "POLYMARKET_REFERENCE_CONSENSUS_WINDOW_S", "60"))
REFERENCE_CONSENSUS_MIN_OBS = int(os.getenv(
    "POLYMARKET_REFERENCE_CONSENSUS_MIN_OBS", "20"))
REFERENCE_CONSENSUS_MIN_SPAN_S = int(os.getenv(
    "POLYMARKET_REFERENCE_CONSENSUS_MIN_SPAN_S", "50"))


def three_source_reference_healthy(window_anchor, decision_ts=None):
    """Require complete PM/Coinbase/Kraken inputs without imposing direction.

    A swing band is a reversal book.  Requiring all feeds to point in the side
    being faded made an armed band impossible precisely when its setup existed.
    Directional agreement remains appropriate for continuation entries; band
    execution uses this health-only variant.
    """
    end, anchor = int(decision_ts or time.time()), int(window_anchor)
    missing = []
    if underlying_price_at(anchor) is None or chainlink_price_at(end) is None:
        missing.append("pm")
    for source in ("coinbase", "kraken"):
        if (independent_reference_twap_at(source, anchor) is None
                or independent_reference_twap_at(source, end) is None):
            missing.append(source)
    if missing:
        return False, "reference missing/stale: " + ",".join(missing)
    return True, "pm+coinbase+kraken healthy"


def independent_reference_twap_at(source, ts, tol_s=8):
    """Complete trailing midpoint average for Coinbase or Kraken, else None."""
    ts = int(ts)
    try:
        with ENGINE.connect() as c:
            row = c.execute(text("""
                SELECT avg(price), count(*), min(observations_ts),
                       max(observations_ts)
                  FROM pm_reference_ticks
                 WHERE source=:source AND asset=:asset
                   AND observations_ts > :lo AND observations_ts <= :ts
            """), {"source": source, "asset": ASSET,
                    "lo": ts - REFERENCE_CONSENSUS_WINDOW_S,
                    "ts": ts}).fetchone()
        if not row or row[0] is None:
            return None
        avg_price, n, oldest, newest = row
        if (int(n) < REFERENCE_CONSENSUS_MIN_OBS
                or int(newest) - int(oldest) < REFERENCE_CONSENSUS_MIN_SPAN_S
                or int(newest) < ts - int(tol_s)):
            return None
        return float(avg_price)
    except Exception as exc:
        _err(f"reference_twap.{source}", exc)
        return None


def three_source_consensus_ok(window_anchor, expected_side, decision_ts=None):
    """Fail-closed PM + Coinbase + Kraken direction agreement for live orders.

    The PM leg uses the resolution-aligned reconstruction selected above; the two
    independent legs use public USD bid/ask midpoint ticks. Shadow scoring is not
    gated so it continues measuring how much coverage the safety rule costs.
    """
    if expected_side not in ("Up", "Down"):
        return False, "expected side missing"
    end = int(decision_ts or time.time())
    anchor = int(window_anchor)
    directions = {}
    pm_open, pm_now = underlying_price_at(anchor), chainlink_price_at(end)
    if pm_open is not None and pm_now is not None:
        directions["pm"] = "Up" if pm_now >= pm_open else "Down"
    for source in ("coinbase", "kraken"):
        p0 = independent_reference_twap_at(source, anchor)
        p1 = independent_reference_twap_at(source, end)
        if p0 is not None and p1 is not None:
            directions[source] = "Up" if p1 >= p0 else "Down"
    missing = [s for s in ("pm", "coinbase", "kraken") if s not in directions]
    if missing:
        return False, "reference missing/stale: " + ",".join(missing)
    disagree = [s for s, side in directions.items() if side != expected_side]
    if disagree:
        detail = ",".join(f"{s}={directions[s]}" for s in directions)
        return False, f"reference disagreement ({detail})"
    return True, "pm+coinbase+kraken agree"


def chainlink_db_price_now(max_age_s=15):
    """Freshest PM-rebroadcast price, or None if the collector is stale."""
    return chainlink_db_price_at(int(time.time()), tol_s=max_age_s)


_cl_mom_cache = {"ts": 0.0, "val": (None, None)}


def chainlink_recent_momentum(secs=6):
    """Short-term momentum (dir, bps) over the last `secs` seconds, ON THE
    SETTLEMENT FEED. Replaces binance_recent_momentum() 2026-07-30.

    This is the ONLY surviving short-momentum read, and it exists because
    `velocity_frac` (recent-6s move / total move) is a real gate in t3_book_ok() —
    a negative value BLOCKS a stake. Deleting Binance without replacing it would
    have made velocity_frac permanently NULL, and NULL *passes* that gate, so the
    filter would have silently opened rather than being removed on purpose.

    Sourced from the chainlink_prices collector (~1 obs/sec), not REST: a momentum
    read needs two points in time and the /reports/latest endpoint only gives the
    newest one. Same 4dp precision as before — over 6s BTC often moves a single
    $0.01 tick (0.0015bps at $65k) and round(,2) collapsed that to exactly 0.0,
    which made velocity_frac 54% zeros and destroyed it as a continuous feature.
    """
    now = time.time()
    if now - _cl_mom_cache["ts"] < 1.0:
        return _cl_mom_cache["val"]
    val = (None, None)
    try:
        with ENGINE.begin() as c:
            rows = c.execute(text("""
                SELECT benchmark_price
                  FROM chainlink_prices
                 WHERE feed_id = :fid
                   AND observations_ts >= :lo AND observations_ts <= :hi
              ORDER BY observations_ts
            """), {"fid": PRICE_FEED_KEY, "lo": int(now) - int(secs) - 2,
                   "hi": int(now)}).fetchall()
        if len(rows) >= 2:
            p0, p1 = float(rows[0][0]), float(rows[-1][0])
            if p0:
                bps = (p1 - p0) / p0 * 1e4
                val = ("Up" if bps > 0 else ("Down" if bps < 0 else None),
                       round(bps, 4))
    except Exception:
        pass
    _cl_mom_cache.update(ts=now, val=val)
    return val


def chainlink_price_now():
    """Latest fresh BTC/ETH reference received from Polymarket RTDS."""
    return chainlink_db_price_now()


def chainlink_price_at(ts):
    """Chainlink benchmark price at/just-before unix `ts` — the resolution-exact
    price for a window open (anchor) or close (expiry). None if ts is future."""
    ts = int(ts)
    if ts > int(time.time()):
        return None
    return chainlink_db_price_at(ts)


# CHAINLINK-ONLY ("totally delink binance from pm BTC, that is
# the cause of wrong stakes"). Both accessors previously fell back to Binance spot /
# Binance klines on any Chainlink error, which silently swapped the oracle mid-flight
# — a ~$40 level offset that flips the decision direction AND the settled outcome.
# A missing/stale PM tick returns None so callers defer; another exchange can never
# silently stand in for the settlement reference.
def underlying_price_now():
    """Live underlying price from Polymarket's Chainlink rebroadcast."""
    return chainlink_price_now()


_PM_REFERENCE_CACHE = {}


def polymarket_window_reference(anchor, asset=None):
    """Official window priceToBeat/finalPrice; never substitute raw-feed averages.

    Missing metadata is retried after five seconds. Completed references are
    cached for an hour; an opening reference alone still refreshes for the close.
    """
    slug = f"{(asset or ASSET).lower()}-updown-{int(WINDOW_S) // 60}m-{int(anchor)}"
    now = time.monotonic()
    cached = _PM_REFERENCE_CACHE.get(slug)
    if cached and now < cached[0]:
        return dict(cached[1])
    result = {"price_open": None, "price_settle": None}
    try:
        response = requests.get(
            "https://gamma-api.polymarket.com/events/slug/" + slug, timeout=4)
        response.raise_for_status()
        metadata = response.json().get("eventMetadata") or {}
        for field, key in (("price_open", "priceToBeat"),
                           ("price_settle", "finalPrice")):
            value = metadata.get(key)
            if value is not None and not isinstance(value, bool):
                price = float(value)
                if math.isfinite(price) and price > 0:
                    result[field] = price
    except Exception:
        pass
    # Gamma publishes eventMetadata late. The live PM page obtains the same
    # Chainlink TWAP reference from this first-party UI endpoint before expiry.
    if result["price_open"] is None or (
            result["price_settle"] is None and time.time() >= int(anchor) + WINDOW_S):
        try:
            response = requests.get(
                "https://polymarket.com/api/crypto/crypto-price",
                params={
                    "symbol": (asset or ASSET).upper(),
                    "eventStartTime": datetime.fromtimestamp(
                        int(anchor), timezone.utc).isoformat().replace("+00:00", "Z"),
                    "endDate": datetime.fromtimestamp(
                        int(anchor) + WINDOW_S, timezone.utc).isoformat().replace("+00:00", "Z"),
                    "variant": "fiveminute", "twapEnabled": "true",
                    "twapLookbackSeconds": "60",
                }, timeout=4)
            response.raise_for_status()
            data = response.json()
            for field, key in (("price_open", "openPrice"),
                               ("price_settle", "closePrice")):
                if field == "price_settle" and (
                        data.get("completed") is not True
                        or time.time() < int(anchor) + WINDOW_S):
                    continue
                value = data.get(key)
                if result[field] is None and value is not None and not isinstance(value, bool):
                    price = float(value)
                    if math.isfinite(price) and price > 0:
                        result[field] = price
        except Exception:
            pass
    # A transient error must not erase a reference already published by PM.
    if cached:
        for field in result:
            if result[field] is None:
                result[field] = cached[1][field]
    if len(_PM_REFERENCE_CACHE) > 256:
        _PM_REFERENCE_CACHE.pop(next(iter(_PM_REFERENCE_CACHE)), None)
    ttl = 3600 if all(v is not None for v in result.values()) else 5
    _PM_REFERENCE_CACHE[slug] = (now + ttl, dict(result))
    return result


def underlying_price_at(ts):
    """PM's official opening price for a market window (fails closed)."""
    return polymarket_window_reference(int(ts))["price_open"]


def polymarket_resolved_outcome(slug):
    """Authoritative PM winner for a closed market, or None until resolved.

    Gamma publishes binary outcome token prices as 1/0 after resolution. This is
    deliberately independent of order placement, so it reconciles shadow and live
    stakes alike. The local RTDS price-derived direction remains audit telemetry.
    """
    try:
        r = requests.get(
            "https://gamma-api.polymarket.com/markets/slug/" + str(slug),
            # This runs in both the settlement path and the periodic backlog
            # sweep.  A slow Gamma request must not hold the bot for an entire
            # window (the old 10s timeout was multiplied by up to 12 rows).
            timeout=4,
        )
        r.raise_for_status()
        d = r.json()
        if str(d.get("umaResolutionStatus", "")).lower() != "resolved":
            return None
        labels = d.get("outcomes") or []
        prices = d.get("outcomePrices") or []
        if isinstance(labels, str):
            labels = json.loads(labels)
        if isinstance(prices, str):
            prices = json.loads(prices)
        winners = [str(label) for label, price in zip(labels, prices)
                   if float(price) >= 0.999]
        return winners[0] if len(winners) == 1 and winners[0] in ("Up", "Down") else None
    except Exception:
        return None


PM_RECON_START_TS = int(os.getenv("POLYMARKET_PM_RECON_START_TS", "1786370400"))


def twap_exit_audit(leg, outcome):
    """Compare the confirmed early sale with holding the same filled shares."""
    if (leg.get("exit_reason") != "TWAP_FILL_DROP"
            or leg.get("status") != "CLOSED" or outcome not in ("Up", "Down")
            or leg.get("realized_pnl_usd") is None):
        return {}
    payout = leg["shares"] if leg["predicted_side"] == outcome else 0.0
    hold = round(payout - leg["stake_usd"] - (leg.get("entry_fee_usd") or 0), 4)
    delta = round(leg["realized_pnl_usd"] - hold, 4)
    verdict = "HELPED" if delta > 0 else "HURT" if delta < 0 else "NEUTRAL"
    return {"twap_hold_pnl": hold, "twap_exit_delta": delta,
            "no_stake_reason": (
                f"Early exit {verdict}: {delta:+.4f} USD vs hold; "
                f"exit {leg['realized_pnl_usd']:+.4f}, hold {hold:+.4f}; PM {outcome}")}


def reconcile_pending_pm_outcomes(max_rows=12):
    """Backfill PM's official verdict for every recent shadow/live decision.

    This catches profitable TP/SL exits and hold-to-settle positions, plus
    NO_STAKE/WATCH rows whose in-memory window was lost across a restart. The
    latter used to remain NO_STAKE forever because the DB-driven deferred sweep
    intentionally selects only real OPEN stakes. Returns the number reconciled
    this pass.
    """
    try:
        with ENGINE.connect() as c:
            rows = c.execute(text(f"""
                SELECT id, slug, dry_run, predicted_side, actual_outcome, book,
                       entry_price, shares, exit_reason, status, window_anchor,
                       price_open, realized_pnl_usd, stake_usd, entry_fee_usd,
                       no_stake_reason
                  FROM {TRADES_TABLE}
                 WHERE asset=:asset
                   AND (staked IS TRUE
                        OR (dry_run IS TRUE AND status='SHADOW')
                        OR status IN ('NO_STAKE', 'WATCH', 'SETTLED_NOTRADE'))
                   AND window_anchor >= :floor
                   AND (pm_outcome IS NULL OR
                        (dry_run IS TRUE AND status='SHADOW'
                         AND realized_pnl_usd IS NULL))
                   AND window_anchor + :win < :now
              -- Reconcile the window the operator is currently watching first.
              -- Older missing verdicts must never hold up the latest expiry.
              ORDER BY window_anchor DESC
                 LIMIT :lim
            """), {"asset": ASSET, "floor": PM_RECON_START_TS,
                    "win": WINDOW_S, "now": int(time.time()),
                    "lim": int(max_rows)}).mappings().all()
    except Exception:
        return 0
    done = 0
    # Gamma occasionally takes several seconds to answer.  Lookups are
    # independent, so fetch them concurrently; DB writes remain serialized.
    from concurrent.futures import ThreadPoolExecutor
    def _resolution(row):
        return (polymarket_resolved_outcome(row["slug"]),
                polymarket_window_reference(row["window_anchor"]))

    with ThreadPoolExecutor(max_workers=min(4, max(1, len(rows)))) as pool:
        resolutions = list(pool.map(_resolution, rows))
    for row, (pm, reference) in zip(rows, resolutions):
        settle_px = reference["price_settle"]
        open_px = reference["price_open"]
        if pm is None:
            # Restart-safe counterpart of settle()'s provisional no-trade path.
            # An expired DB-only window has no in-memory object left to update,
            # but its exact RTDS open/close still gives the dashboard a useful
            # result while Gamma remains pending. Keep pm_outcome NULL so this row
            # remains eligible for authoritative reconciliation on later sweeps.
            if (row["status"] in ("NO_STAKE", "WATCH", "SETTLED_NOTRADE")
                    and open_px is not None and settle_px is not None):
                feed = "Up" if settle_px >= open_px else "Down"
                feed_hit = (row["predicted_side"] == feed
                            if feed in ("Up", "Down") else None)
                with ENGINE.begin() as c:
                    c.execute(text(f"""
                        UPDATE {TRADES_TABLE}
                           SET status='SETTLED_NOTRADE', actual_outcome=:outcome,
                               hit=:hit, price_settle=:settle_px, updated_at=now()
                         WHERE id=:id
                    """), {"outcome": feed, "hit": feed_hit,
                            "settle_px": settle_px, "id": row["id"]})
            elif (row["dry_run"] and row["status"] == "OPEN"
                    and open_px is not None and settle_px is not None
                    and row["entry_price"] is not None and row["shares"] is not None):
                feed = "Up" if settle_px >= open_px else "Down"
                if feed in ("Up", "Down"):
                    won = row["predicted_side"] == feed
                    pnl = hold_to_settle_pnl(
                        row["book"], float(row["entry_price"]),
                        float(row["shares"]), won)
                    with ENGINE.begin() as c:
                        c.execute(text(f"""
                            UPDATE {TRADES_TABLE}
                               SET status='CLOSED', actual_outcome=:outcome,
                                   hit=:hit, price_settle=:settle_px,
                                   exit_reason=:reason, exit_price=:payout,
                                   exit_at=to_timestamp(window_anchor + :win_s),
                                   realized_pnl_usd=:pnl, updated_at=now()
                             WHERE id=:id AND status='OPEN'
                        """), {"outcome": feed, "hit": won,
                                "settle_px": settle_px,
                                "reason": "SETTLE_WIN" if won else "SETTLE_LOSS",
                                "payout": 1.0 if won else 0.0, "pnl": pnl,
                                "win_s": WINDOW_S, "id": row["id"]})
            continue
        prior = row["actual_outcome"]
        agrees = prior == pm if prior in ("Up", "Down") else None
        won = row["predicted_side"] == pm
        updates = {
            "id": row["id"], "pm": pm, "agrees": agrees, "hit": won,
            "pnl": None, "reason": row["exit_reason"], "hold": False,
            "exit_px": None, "win_s": WINDOW_S, "settle_px": settle_px,
            "notrade": row["status"] in ("NO_STAKE", "WATCH", "SETTLED_NOTRADE"),
        }
        if (str(row["exit_reason"] or "").startswith("SETTLE_")
                or (row["dry_run"] and row["status"] == "SHADOW"
                    and row["book"] in TAKER_HOLD_BOOKS)
                or (HOLD_TO_SETTLE and row["dry_run"] and row["book"] == "t3")):
            updates["pnl"] = hold_to_settle_pnl(
                row["book"], float(row["entry_price"]), float(row["shares"]), won)
            updates["reason"] = "SETTLE_WIN" if won else "SETTLE_LOSS"
            updates["hold"] = True
            updates["exit_px"] = 1.0 if won else 0.0
        audit = twap_exit_audit(row, pm)
        updates.update(twap_hold_pnl=audit.get("twap_hold_pnl"),
                       twap_exit_delta=audit.get("twap_exit_delta"),
                       audit_note=audit.get("no_stake_reason"))
        with ENGINE.begin() as c:
            c.execute(text(f"""
                UPDATE {TRADES_TABLE}
                   SET pm_outcome=:pm, pm_recon_ok=:agrees,
                       pm_reconciled_at=now(), actual_outcome=:pm, hit=:hit,
                       status=CASE WHEN :hold THEN 'CLOSED'
                                   WHEN :notrade THEN 'SETTLED_NOTRADE'
                                   ELSE status END,
                       price_settle=COALESCE(price_settle, :settle_px),
                       realized_pnl_usd=COALESCE(:pnl, realized_pnl_usd),
                       exit_reason=:reason,
                       exit_price=CASE WHEN :hold THEN :exit_px ELSE exit_price END,
                       exit_at=CASE WHEN :hold
                                    THEN to_timestamp(window_anchor + :win_s)
                                    ELSE exit_at END,
                       twap_hold_pnl=COALESCE(:twap_hold_pnl, twap_hold_pnl),
                       twap_exit_delta=COALESCE(:twap_exit_delta, twap_exit_delta),
                       no_stake_reason=COALESCE(:audit_note, no_stake_reason),
                       updated_at=now()
                 WHERE id=:id
            """), updates)
        print(f"[{datetime.now(timezone.utc):%H:%M:%S}] PM-VERDICT "
              f"{row['slug']} {pm} · RTDS "
              f"{'AGREES' if agrees is True else ('DISAGREES' if agrees is False else 'N/A')}",
              flush=True)
        if audit:
            print(f"TWAP-EXIT-AUDIT {row['slug']} id={row['id']} "
                  f"{audit['no_stake_reason']}", flush=True)
        done += 1
    return done


def adx_now(df=None, period=14):
    if df is None:
        df = get_klines_1m(limit=100)
    try:
        adx_df = df.ta.adx(length=period)
        val = adx_df.iloc[-1].get(f"ADX_{period}")
        return float(val) if pd.notna(val) else None
    except Exception:
        return None


def adx_chg_pct(series):
    """Total ADX change across the window as a % of the FIRST sample, or None.

    The SCALE-FREE twin of adx_slope(): the same "is the trend exhausting or
    building" question, but self-normalising, so one threshold means the same thing
    at ADX 15 and ADX 60 (the sample spans 13-75). Logged from 2026-07-22 for the
    forward read on a dynamic band — NOTHING GATES ON IT YET. On 07-12..22 shadow
    the relationship is a HUMP, not a monotone: pooled BTC+ETH win rate by band was
    <-25%: 13.6% | -25..-18%: 30.0% | -18..-12%: 19.4% | -12..-7%: 17.6% |
    -7..-2%: 7.0% | -2..+3%: 11.4% | >+3%: 9.1%. Both tails are bad, which is why a
    dynamic gate would need an upper AND a lower bound. Judge 2026-08-01."""
    pts = [a for _, a in (series or []) if a is not None]
    if len(pts) < 2 or not pts[0]:
        return None
    return round((pts[-1] - pts[0]) / pts[0] * 100.0, 2)


def adx_slope(series):
    """Least-squares slope (ADX units per MINUTE) of the per-minute ADX(short)
    samples collected across a window: `series` is [(elapsed_s, adx7), ...].
    Falling ADX during a move = trend exhaustion (confirms a reversal/flip);
    rising = trend strengthening (the 'flip' is more likely a fake-out). None
    if <2 valid samples."""
    pts = [(s / 60.0, a) for s, a in (series or []) if a is not None]
    if len(pts) < 2:
        return None
    n = len(pts)
    sx = sum(x for x, _ in pts)
    sy = sum(y for _, y in pts)
    sxx = sum(x * x for x, _ in pts)
    sxy = sum(x * y for x, y in pts)
    den = n * sxx - sx * sx
    if den == 0:
        return None
    return round((n * sxy - sx * sy) / den, 3)


# ---------------------------------------------------------------------------
# Order-book pricing (source of truth for fills — get_price side naming is
# counterintuitive; the book is unambiguous). Walk levels for the requested
# size so simulated fills include slippage on thin windows.
# ---------------------------------------------------------------------------
def _levels(book):
    bids = sorted(((float(b.price), float(b.size)) for b in book.bids), reverse=True)
    asks = sorted((float(a.price), float(a.size)) for a in book.asks)
    return bids, asks


def walk_fill(levels, shares, max_price=None):
    """Average fill price to trade `shares` across price levels (list of
    (price, size)). Returns (avg_price, filled_shares). Partial if the book
    can't cover the full size or the remaining levels exceed `max_price`."""
    need = shares
    cost = 0.0
    got = 0.0
    for price, size in levels:
        if max_price is not None and price > max_price + 1e-9:
            break
        take = min(need, size)
        cost += take * price
        got += take
        need -= take
        if need <= 1e-9:
            break
    if got <= 0:
        return None, 0.0
    return cost / got, got


def round_tick(px):
    """Snap a price to the market tick and clamp to the tradeable [0.01, 0.99]."""
    return max(0.01, min(0.99, round(round(px / TICK) * TICK, 2)))


# ---------------------------------------------------------------------------
# DB
# ---------------------------------------------------------------------------
def ensure_table():
    with ENGINE.begin() as c:
        c.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {TRADES_TABLE} (
                id                SERIAL PRIMARY KEY,
                window_anchor     BIGINT NOT NULL,
                leg               INT NOT NULL DEFAULT 0,
                is_t3             BOOLEAN DEFAULT FALSE,
                window_start_utc  TIMESTAMPTZ,
                slug              TEXT,
                dry_run           BOOLEAN NOT NULL DEFAULT TRUE,
                decided_at        TIMESTAMPTZ,
                predicted_side    TEXT,
                tier              TEXT,
                tier_acc          DOUBLE PRECISION,
                decision_bps      DOUBLE PRECISION,
                adx               DOUBLE PRECISION,
                adx_confirmed     BOOLEAN,
                price_open        DOUBLE PRECISION,
                price_decision    DOUBLE PRECISION,
                staked            BOOLEAN DEFAULT FALSE,
                no_stake_reason   TEXT,
                token_id          TEXT,
                entry_price       DOUBLE PRECISION,
                shares            DOUBLE PRECISION,
                stake_usd         DOUBLE PRECISION,
                exit_reason       TEXT,
                exit_price        DOUBLE PRECISION,
                exit_at           TIMESTAMPTZ,
                price_settle      DOUBLE PRECISION,
                actual_outcome    TEXT,
                hit               BOOLEAN,
                realized_pnl_usd  DOUBLE PRECISION,
                status            TEXT DEFAULT 'WATCH',
                updated_at        TIMESTAMPTZ DEFAULT now(),
                UNIQUE (window_anchor, leg, dry_run)
            )
        """))
        # Additive columns (idempotent). BINANCE-EARLY look-ahead exploit: log the
        # LEADING-feed (Binance own open->now delta, level offset cancels) direction
        # ALONGSIDE the Chainlink one, + which the Chainlink settle proves right, to
        # measure the lead-time edge. `book` tags the two-book execution (t3/fade).
        for col, typ in [
            ("asset", "TEXT DEFAULT 'BTC'"),        # which coin this row is for
            # DYNAMIC-CLASSIFICATION instrumentation (2026-07-11, log-only): the
            # ingredients to later replace the STATIC bps->tier->stake map with a
            # regime- + value-aware rule. Logged on every T+3 leg (incl no-stake
            # tiers) + post-T+3 revisions. move_z = move vs current 3-min vol;
            # velocity_frac = recent-6s move / total move (developing vs exhausted);
            # ask_up/ask_down = both-side token asks (value / R:R).
            ("move_z", "DOUBLE PRECISION"),
            ("vol_3min_bps", "DOUBLE PRECISION"),
            ("velocity_frac", "DOUBLE PRECISION"),
            ("last_tick_bps", "DOUBLE PRECISION"),   # raw latest tick vs open (09-28)
            ("ask_up", "DOUBLE PRECISION"),
            ("ask_down", "DOUBLE PRECISION"),
            ("price_open_binance", "DOUBLE PRECISION"),
            ("decision_bps_binance", "DOUBLE PRECISION"),
            ("predicted_side_binance", "TEXT"),
            ("tier_binance", "TEXT"),
            ("hit_binance", "BOOLEAN"),
            ("book", "TEXT"),
            ("bin_mom_bps", "DOUBLE PRECISION"),   # recent ~3-6s Binance momentum (fade gate)
            ("fade_confirmed", "BOOLEAN"),         # did the lead confirm the reversal?
            ("adx_short", "DOUBLE PRECISION"),     # short-period ADX (intra-window trend)
            # intended (pre-fill) stake in USDC — lets the dash flag a PARTIAL fill
            # (actual stake_usd < intended) vs a full fill, independent of any later
            # capital change (2026-07-14). Currently set on live flip legs.
            ("intended_stake_usd", "DOUBLE PRECISION"),
            # RETRY-ON-KILL instrumentation (2026-07-21). The whole point of the
            # retry is to measure the one number no table has: where the opp ask
            # actually sits ~1.65s after a FAK kill. Everything prior was inferred
            # from shadow ticks 4-14s out, which priced the retry into a move it
            # never sees. retry_of_kill also keeps the wider-limit legs separable
            # from the validated ask<=0.16 cohort — never blend them.
            ("retry_of_kill", "BOOLEAN"),          # this leg is a post-kill re-fire
            ("retry_ask_at_kill", "DOUBLE PRECISION"),  # best opp ask read at kill
            ("retry_rt_s", "DOUBLE PRECISION"),    # retry order round trip (s)
            # ask-side depth the retry FAK could actually take: resting shares at or
            # under the retry cap, read from the same book snapshot as
            # retry_ask_at_kill (). 0 here IS the KILLED-AGAIN
            # cause — the wall (retry_ask_at_kill) moved above the cap so nothing
            # rested in range. Mirrors decision-time queue logging for FAK deaths.
            ("retry_queue_sh", "DOUBLE PRECISION"),
            # PER-STAKE EXCHANGE RECONCILIATION (2026-07-21: "the book on
            # exchange must be reconciled every stake"). Every real fill is checked
            # back against Polymarket's own position + collateral, because the
            # order RESPONSE is not proof of the book: on 07-20 a rejection wrote
            # nothing and the dash invented a +$23.55 win, and the response's
            # making_amount excludes the taker fee, so recorded stake ran ~6% under
            # the actual debit. recon_ok=FALSE means our row disagrees with the
            # exchange and the row is NOT to be trusted for P&L.
            ("entry_fee_usd", "DOUBLE PRECISION"),   # taker fee paid on entry
            ("recon_shares", "DOUBLE PRECISION"),    # shares the exchange shows
            ("recon_avg_px", "DOUBLE PRECISION"),    # exchange avg entry price
            ("recon_usdc_after", "DOUBLE PRECISION"),# collateral after the stake
            ("recon_ok", "BOOLEAN"),                 # exchange agrees with our row
            # POST-SETTLEMENT PM VERDICT — applies to shadow AND live stakes.
            # Distinct from recon_ok above, which can only verify a real position.
            ("pm_outcome", "TEXT"),
            ("pm_recon_ok", "BOOLEAN"),              # RTDS-derived side agrees with PM
            ("pm_reconciled_at", "TIMESTAMPTZ"),
            # DECISION PROVENANCE ("capture all the data we need
            # later to analyse the fails"). Until now a live leg could NOT be joined
            # to the shadow snapshot it actually decided on — no seq, no elapsed_s —
            # so when 11 of 30 live legs turned out to be OFF-cohort there was no way
            # to ask why. These stamp the gate inputs AT DECISION TIME, on the row
            # itself, so every post-mortem is a SELECT and never a replay.
            ("decision_seq", "INTEGER"),             # shadow seq we decided on
            ("decision_elapsed_s", "INTEGER"),       # seconds into the 300s window
            ("ask_opp", "DOUBLE PRECISION"),         # opp ask AT DECISION (not fill)
            ("adx_slope", "DOUBLE PRECISION"),       # signed LSQ slope, the gate input
            ("retrace_frac", "DOUBLE PRECISION"),
            ("flip_tier", "TEXT"),                   # NOT the t3 `tier` column
            ("fillable_opp_sh", "DOUBLE PRECISION"), # depth <= ask cap at decision
            ("touch_opp_sh", "DOUBLE PRECISION"),    # size at the touch at decision
            # FLIP-SIDE mirrors of the two above (2026-07-28). Stamped on EVERY eval
            # leg by _stamp_signal, so fillable_sh - fillable_opp_sh is a real number
            # on t3/RE rows and not just on the windows re_flip_shadow happened to
            # log. `flip` = the side the move called at decision; see _stamp_signal.
            ("ask_flip", "DOUBLE PRECISION"),        # flip-side ask AT DECISION
            ("fillable_sh", "DOUBLE PRECISION"),     # flip-side sweepable depth
            ("touch_sh", "DOUBLE PRECISION"),        # flip-side size at the touch
            # RAW ask ladders, both sides, out to LADDER_OBS_SLIP (2026-07-29).
            # fillable_*_sh collapse the book to ONE cap (BUY_SLIP = 3c); the t3
            # bank book's pay-up rule caps at +1c, and nothing in the scalar pair
            # recovers that. JSON [[px, sh], ...] cheapest-first, so depth at any
            # cent cap is derivable after the fact. Same shape/units as
            # re_flip_shadow.opp_ask_ladder, minus its flip-detected-only coverage.
            # TRAP: both are stamped ONCE, at _stamp_signal time. The oppfade /
            # swingfade paths re-stamp ask_opp/fillable_opp_sh/touch_opp_sh later
            # with their own snapshot; ask_ladder_opp is NOT re-stamped, so on those
            # legs it is the earlier book and may disagree with fillable_opp_sh.
            # Read ask_ladder_opp against ask_flip/ask_ladder, never against a
            # re-stamped opp scalar.
            ("ask_ladder", "TEXT"),                  # flip side (what t3/RE buys)
            ("ask_ladder_opp", "TEXT"),              # opp side (fade/swingfade)
            # THE ONE THAT MATTERS: was this leg inside the frozen cohort rule at the
            # moment we fired? Stamped live, so the forward read can split in/out of
            # cohort without re-deriving it from re_flip_shadow and hoping the seq
            # dedup matches. An off-cohort leg is NOT evidence about the +8.9pp read.
            ("cohort_qualified", "BOOLEAN"),
            # SWINGFADE gate inputs (2026-07-23). Stamped on `swingfade` legs only.
            # These are the three features the oppfade provenance set does NOT
            # already carry, and without them a forward read cannot tell whether a
            # leg was taken because the window was swinging or merely because it was
            # cheap — which is the entire distinction this book is testing.
            ("swing_crossings", "INTEGER"),          # beat-price crossings pre-entry
            ("swing_up_exc_bps", "DOUBLE PRECISION"),
            ("swing_dn_exc_bps", "DOUBLE PRECISION"),
            # notional recovered by the partial-fill re-fire, NULL when none fired.
            # Keep separate from stake_usd so the re-fire's contribution stays
            # measurable on its own — it is a distinct, unvalidated mechanic.
            ("swing_refill_usd", "DOUBLE PRECISION"),
            # ADVERSE-cap regime stamp (2026-07-28). swing_adverse_bps = the
            # excursion that ran AGAINST the side this leg bought; swing_adverse_max
            # = the cap in force when it was taken (0 = leg disarmed). Together they
            # make every leg self-describing: an armed period and a disarmed one are
            # separable without date-matching a config change.
            ("swing_adverse_bps", "DOUBLE PRECISION"),
            ("swing_adverse_max", "DOUBLE PRECISION"),
            # T3 EXECUTION SET (2026-08-06). Written on staked AND no-stake t3 legs
            # by _stamp_signal — see there for why a refusal has to carry its own
            # arithmetic. The pair that answers "was it really the book?" is
            # t3_target_sh vs t3_depth_quote_sh; the pair that answers "did we get
            # what we modelled?" is t3_sweep_avg vs t3_fill_px.
            ("t3_ticket_usd", "DOUBLE PRECISION"),    # $ we tried to spend
            ("t3_target_sh", "DOUBLE PRECISION"),     # ticket / quoted ask
            ("t3_cap_px", "DOUBLE PRECISION"),        # FAK limit: ask+payup, <=0.80
            ("t3_sweep_sh", "DOUBLE PRECISION"),      # shares the sweep models
            ("t3_sweep_usd", "DOUBLE PRECISION"),     # $ the sweep models spending
            ("t3_sweep_avg", "DOUBLE PRECISION"),     # modelled avg fill price
            ("t3_fill_frac", "DOUBLE PRECISION"),     # sweep_usd / ticket — THE gate
            ("t3_depth_quote_sh", "DOUBLE PRECISION"),# shares resting AT the quote
            ("t3_depth_1c_sh", "DOUBLE PRECISION"),   # ... through quote + $0.01
            ("t3_depth_cap_sh", "DOUBLE PRECISION"),  # ... through the hard 0.80
            ("t3_model_slip_bps", "DOUBLE PRECISION"),# modelled avg vs quoted ask
            # Only on legs where an order actually went to the exchange.
            ("t3_fill_px", "DOUBLE PRECISION"),       # realised avg fill
            ("t3_fill_sh", "DOUBLE PRECISION"),       # realised shares
            ("t3_fill_vs_model_bps", "DOUBLE PRECISION"),  # realised vs modelled
            ("twap_decision_ask", "DOUBLE PRECISION"),
            ("twap_fill_cut", "BOOLEAN"),
            ("twap_exit_error", "TEXT"),
            ("twap_hold_pnl", "DOUBLE PRECISION"),
            ("twap_exit_delta", "DOUBLE PRECISION"),
        ]:
            c.execute(text(f"ALTER TABLE {TRADES_TABLE} "
                           f"ADD COLUMN IF NOT EXISTS {col} {typ}"))


DB_COLS = [
    "asset",
    "window_anchor", "leg", "is_t3", "window_start_utc", "slug", "dry_run",
    "decided_at", "predicted_side", "tier", "tier_acc", "decision_bps", "adx",
    "adx_confirmed", "price_open", "price_decision", "staked", "no_stake_reason",
    "token_id", "entry_price", "shares", "stake_usd", "exit_reason",
    "exit_price", "exit_at", "price_settle", "actual_outcome", "hit",
    "realized_pnl_usd", "status",
    "price_open_binance", "decision_bps_binance", "predicted_side_binance",
    "tier_binance", "hit_binance", "book", "bin_mom_bps", "fade_confirmed",
    "adx_short",
    "move_z", "vol_3min_bps", "velocity_frac", "last_tick_bps", "ask_up", "ask_down",
    "intended_stake_usd",
    "retry_of_kill", "retry_ask_at_kill", "retry_rt_s", "retry_queue_sh",
    "entry_fee_usd", "recon_shares", "recon_avg_px", "recon_usdc_after",
    "recon_ok",
    "pm_outcome", "pm_recon_ok", "pm_reconciled_at",
    "decision_seq", "decision_elapsed_s", "ask_opp", "adx_slope", "retrace_frac",
    "flip_tier", "fillable_opp_sh", "touch_opp_sh", "cohort_qualified",
    "ask_flip", "fillable_sh", "touch_sh", "ask_ladder", "ask_ladder_opp",
    "swing_crossings", "swing_up_exc_bps", "swing_dn_exc_bps", "swing_refill_usd",
    "swing_adverse_bps", "swing_adverse_max",
    "t3_ticket_usd", "t3_target_sh", "t3_cap_px", "t3_sweep_sh", "t3_sweep_usd",
    "t3_sweep_avg", "t3_fill_frac", "t3_depth_quote_sh", "t3_depth_1c_sh",
    "t3_depth_cap_sh", "t3_model_slip_bps",
    "t3_fill_px", "t3_fill_sh", "t3_fill_vs_model_bps",
    "twap_decision_ask", "twap_fill_cut", "twap_exit_error",
    "twap_hold_pnl", "twap_exit_delta",
]

_KEY_COLS = ("window_anchor", "leg", "dry_run")


def upsert(leg):
    row = {k: leg.get(k) for k in DB_COLS}
    row["dry_run"] = DRY_RUN
    row["asset"] = ASSET
    set_clause = ", ".join(f"{c}=EXCLUDED.{c}" for c in DB_COLS if c not in _KEY_COLS)
    cols = ", ".join(DB_COLS)
    ph = ", ".join(f":{c}" for c in DB_COLS)
    with ENGINE.begin() as c:
        c.execute(text(f"""
            INSERT INTO {TRADES_TABLE} ({cols}, updated_at)
            VALUES ({ph}, now())
            ON CONFLICT (window_anchor, leg, dry_run)
            DO UPDATE SET {set_clause}, updated_at = now()
        """), row)


# --- T+2 -> T+3 shadow probe storage (separate table, additive; never touches
# the trades pipeline). One row per distinct would-enter STATE per window, keyed
# by (asset, window_anchor, seq). Join to {TRADES_TABLE} on window_anchor (is_t3)
# for the T+3 comparison + settlement outcome. ------------------------------
T2_PROBE_TABLE = "polymarket_updown_t2probe"
_T2_PROBE_COLS = [
    "asset", "window_anchor", "seq", "window_start_utc", "slug", "dry_run",
    "decided_at", "elapsed_s", "predicted_side", "tier", "tier_acc",
    "decision_bps", "price_open", "price_decision", "ask_up", "ask_down",
    "bid_up", "bid_down", "best_ask", "would_stake", "reason",
]


def ensure_t2_probe_table():
    with ENGINE.begin() as c:
        c.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {T2_PROBE_TABLE} (
                asset            TEXT NOT NULL DEFAULT 'BTC',
                window_anchor    BIGINT NOT NULL,
                seq              INT NOT NULL DEFAULT 0,
                window_start_utc TIMESTAMPTZ,
                slug             TEXT,
                dry_run          BOOLEAN NOT NULL DEFAULT TRUE,
                decided_at       TIMESTAMPTZ,
                elapsed_s        INT,
                predicted_side   TEXT,
                tier             TEXT,
                tier_acc         DOUBLE PRECISION,
                decision_bps     DOUBLE PRECISION,
                price_open       DOUBLE PRECISION,
                price_decision   DOUBLE PRECISION,
                ask_up           DOUBLE PRECISION,
                ask_down         DOUBLE PRECISION,
                bid_up           DOUBLE PRECISION,
                bid_down         DOUBLE PRECISION,
                best_ask         DOUBLE PRECISION,
                would_stake      BOOLEAN,
                reason           TEXT,
                updated_at       TIMESTAMPTZ DEFAULT now(),
                PRIMARY KEY (asset, window_anchor, seq)
            )
        """))
        # bid columns added 2026-07-13 (two-sided open-book capture) — ALTER for
        # the pre-existing table since CREATE ... IF NOT EXISTS won't add columns.
        c.execute(text(f"ALTER TABLE {T2_PROBE_TABLE} "
                       "ADD COLUMN IF NOT EXISTS bid_up DOUBLE PRECISION, "
                       "ADD COLUMN IF NOT EXISTS bid_down DOUBLE PRECISION"))


def upsert_t2_probe(rec):
    row = {k: rec.get(k) for k in _T2_PROBE_COLS}
    cols = ", ".join(_T2_PROBE_COLS)
    ph = ", ".join(f":{c}" for c in _T2_PROBE_COLS)
    set_clause = ", ".join(f"{c}=EXCLUDED.{c}" for c in _T2_PROBE_COLS
                           if c not in ("asset", "window_anchor", "seq"))
    with ENGINE.begin() as c:
        c.execute(text(f"""
            INSERT INTO {T2_PROBE_TABLE} ({cols}, updated_at)
            VALUES ({ph}, now())
            ON CONFLICT (asset, window_anchor, seq)
            DO UPDATE SET {set_clause}, updated_at = now()
        """), row)


# --- RE-FLIP SHADOW table (isolated; one row per distinct flipped-call state) ---
RE_FLIP_SHADOW_TABLE = "polymarket_updown_re_flip_shadow"
_RE_FLIP_COLS = [
    "asset", "window_anchor", "seq", "window_start_utc", "slug", "dry_run",
    "decided_at", "elapsed_s", "t3_side", "flip_side", "flip_tier", "flip_dbps",
    "price_open", "price_now", "ask_flip", "ask_opp", "best_ask",
    # flip CONTEXT (2026-07-13): the pre-flip excursion this flip reverses +
    # intra-window ADX(short) slope. tier alone (dbps @ crossing) can't tell a
    # 40bps move that reversed from a never-moved noise crossing; these can.
    "peak_excursion_bps", "retrace_frac", "adx_samples", "adx_slope",
    # REAL fillable depth on the flip token at decision time (2026-07-14): shares
    # resting at asks <= ask_flip + BUY_SLIP, i.e. what a FAK taker could actually
    # get. Lets the dash show "ask X · Ysh" and flag no-fill/partial vs a phantom
    # ask with no depth (the recurring "no orders found to match FAK" rejections).
    "fillable_sh",
    # Binance ~3-6s leading momentum at flip time (2026-07-14): to score whether a
    # "Binance disagrees with the flip side" gate is +EV. Historical rows are NULL
    # (bin_mom wasn't logged on the shadow path before) — this builds the sample.
    "bin_mom_bps",
    # opp-side book at decision (2026-07-16): the cheap side we'd BUY when
    # ask_opp is low. bid_opp + queue_opp (size a fresh maker BUY rests behind)
    # gate maker-fill viability; min_ask_opp (lowest opp ask over the hold) is
    # settle-backfilled and answers "would a resting maker BUY have been crossed".
    "bid_opp", "queue_opp",
    # TAKER depth on the OPP token (2026-07-19): shares resting at asks
    # <= ask_opp + BUY_SLIP — the mirror of fillable_sh for the side OPP-FADE
    # actually buys. Without it the fill question is unanswerable: fillable_sh is
    # the FLIP token, and bid_opp/queue_opp/min_ask_opp only cover the MAKER route.
    "fillable_opp_sh",
    # SWEEP SHAPE on the OPP token (2026-07-19). fillable_opp_sh alone conflates
    # "fills" with "fills at ask_opp": it sums every level up to ask_opp+BUY_SLIP,
    # and BUY_SLIP=0.03 on a 0.16 token is ~1875bps of headroom, so a PASS can hide
    # a punishing sweep. touch_opp_sh = size at the best ask; opp_ask_ladder = the
    # [[price,size],...] levels <= cap as a JSON TEXT blob. Stake-independent on
    # purpose — sizing lives in polymarket_dashboard._opp_sized_legs and the base
    # compounds, so a vwap baked at log time would not be comparable across the run.
    # With the ladder, VWAP for ANY stake is recomputable retroactively.
    "touch_opp_sh", "opp_ask_ladder",
    # T+3 ORIGIN + RULE-D COUNTERFACTUAL (2026-07-21). t3_tier/t3_bps are the
    # ORIGINAL signal at the T+3 decision (NULL tier = sub-2bps); flip_tier is
    # what the same move MATURED into by the time this row's ask qualified. The
    # pair is the whole subject of rule D, and stamping it here means the scorer
    # never has to join back to polymarket_updown_trades to reconstruct it.
    # d_prune = would rule D have skipped this row (see _oppfade_prune_d). It is
    # logged whether or not OPPFADE_PRUNE_D is enforcing, so the A/B is readable
    # from one table: with the flag OFF every d_prune=TRUE row is a trade we DID
    # take and D would not have.
    "t3_tier", "t3_bps", "d_prune",
    # RELATIVE adx decay (2026-07-22): the scale-free twin of adx_slope — see
    # adx_chg_pct(). Data-only, nothing gates on it; judge 2026-08-01. MUST be
    # listed here or the insert silently drops it (this list, not the payload
    # dict, is what upsert writes — it swallowed the column on the first try).
    "adx_chg_pct",
    # SWING PATH at the flip (2026-07-28) — the raw inputs to the swingfade
    # excursion gate, so that axis is never reconstructed again. `adverse` is
    # path_max_up_bps when flip_side='Up' (we buy Down) and path_max_dn_bps
    # otherwise. swingfade_adverse_max records the cap IN FORCE on this row:
    # NULL/0 = the adverse leg was disarmed, so armed and disarmed periods are
    # separable in one query and can never be silently blended.
    "path_max_up_bps", "path_max_dn_bps", "cross_count", "swingfade_adverse_max",
]


def ensure_re_flip_shadow_table():
    with ENGINE.begin() as c:
        c.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {RE_FLIP_SHADOW_TABLE} (
                asset            TEXT NOT NULL DEFAULT 'BTC',
                window_anchor    BIGINT NOT NULL,
                seq              INT NOT NULL DEFAULT 0,
                window_start_utc TIMESTAMPTZ,
                slug             TEXT,
                dry_run          BOOLEAN NOT NULL DEFAULT TRUE,
                decided_at       TIMESTAMPTZ,
                elapsed_s        INT,
                t3_side          TEXT,
                flip_side        TEXT,
                flip_tier        TEXT,
                flip_dbps        DOUBLE PRECISION,
                price_open       DOUBLE PRECISION,
                price_now        DOUBLE PRECISION,
                ask_flip         DOUBLE PRECISION,
                ask_opp          DOUBLE PRECISION,
                bid_opp          DOUBLE PRECISION,
                queue_opp        DOUBLE PRECISION,
                min_ask_opp      DOUBLE PRECISION,
                fillable_opp_sh  DOUBLE PRECISION,
                touch_opp_sh     DOUBLE PRECISION,
                opp_ask_ladder   TEXT,
                best_ask         DOUBLE PRECISION,
                settle_side      TEXT,
                settle_price     DOUBLE PRECISION,
                flip_won         BOOLEAN,
                peak_bid_flip    DOUBLE PRECISION,
                peak_excursion_bps DOUBLE PRECISION,
                retrace_frac       DOUBLE PRECISION,
                adx_samples        TEXT,
                adx_slope          DOUBLE PRECISION,
                updated_at       TIMESTAMPTZ DEFAULT now(),
                PRIMARY KEY (asset, window_anchor, seq)
            )
        """))
        # migrate the already-live table (added 2026-07-12): settle backfill makes
        # the table self-scoring (no fragile Chainlink join); peak_bid_flip records
        # the best exit bid seen on the flip side, to score "exit the flip early".
        for _col, _typ in (("settle_side", "TEXT"),
                           ("settle_price", "DOUBLE PRECISION"),
                           ("flip_won", "BOOLEAN"),
                           ("peak_bid_flip", "DOUBLE PRECISION"),
                           # flip-context cols (2026-07-13)
                           ("peak_excursion_bps", "DOUBLE PRECISION"),
                           ("retrace_frac", "DOUBLE PRECISION"),
                           ("adx_samples", "TEXT"),
                           ("adx_slope", "DOUBLE PRECISION"),
                           # real fillable book depth at the flip ask (2026-07-14)
                           ("fillable_sh", "DOUBLE PRECISION"),
                           # Binance leading momentum at flip time (2026-07-14)
                           ("bin_mom_bps", "DOUBLE PRECISION"),
                           # opp-side (the cheap side we'd BUY) book for the
                           # maker-fill test (2026-07-16): bid + queue-ahead at
                           # decision, and min opp ask over the hold.
                           ("bid_opp", "DOUBLE PRECISION"),
                           ("queue_opp", "DOUBLE PRECISION"),
                           ("min_ask_opp", "DOUBLE PRECISION"),
                           # TAKER depth on the OPP token at decision (2026-07-19)
                           ("fillable_opp_sh", "DOUBLE PRECISION"),
                           # sweep shape on the opp token (2026-07-19)
                           ("touch_opp_sh", "DOUBLE PRECISION"),
                           ("opp_ask_ladder", "TEXT"),
                           # T+3 origin + rule-D counterfactual (2026-07-21).
                           # NULL on every pre-restart row — the D read starts
                           # from the restart, it cannot be backfilled from here
                           # (join polymarket_updown_trades for history).
                           ("t3_tier", "TEXT"),
                           ("t3_bps", "DOUBLE PRECISION"),
                           ("d_prune", "BOOLEAN"),
                           # RELATIVE adx decay across the window, % of the FIRST
                           # sample: (adx_last - adx_first)/adx_first*100. The
                           # scale-free twin of adx_slope, logged for the 10-day
                           # forward review of a dynamic band (2026-07-22, judge
                           # 2026-08-01). Derivable from adx_samples, stored so the
                           # review is one query and so it survives a sampler that
                           # writes fewer raw samples. NOT GATED ON — data only.
                           ("adx_chg_pct", "DOUBLE PRECISION"),
                           # SWING PATH at the flip (2026-07-28). The two legs the
                           # swingfade excursion gate reads, stored raw so any
                           # future read of that axis is EXACT rather than
                           # reconstructed from chainlink_prices — the 07-28 study
                           # had to rebuild them and could only do so to +-1 tick.
                           # up/dn are cumulative-from-window-OPEN running maxima,
                           # cross_count is crossings of the beat price. Which one
                           # is ADVERSE depends on flip_side: adverse = up when
                           # flip_side='Up' (we buy Down), else dn.
                           ("path_max_up_bps", "DOUBLE PRECISION"),
                           ("path_max_dn_bps", "DOUBLE PRECISION"),
                           ("cross_count", "INTEGER"),
                           # effective adverse cap at decision time (0/NULL = the
                           # leg was disarmed on this row) — records WHICH REGIME
                           # each row was written under, so an armed period and a
                           # disarmed one can never be blended by accident.
                           ("swingfade_adverse_max", "DOUBLE PRECISION")):
            c.execute(text(
                f"ALTER TABLE {RE_FLIP_SHADOW_TABLE} "
                f"ADD COLUMN IF NOT EXISTS {_col} {_typ}"))


def backfill_re_flip_settle(window_anchor, settle_side, settle_price, peaks,
                            opp_mins=None):
    """Post-settlement self-scoring: stamp every flip row for the window with the
    Chainlink resolution + whether the flipped side won, and each seq's peak exit
    bid. Isolated logging — callers wrap in try/except so it can never disturb the
    real settlement of a staked leg."""
    with ENGINE.begin() as c:
        c.execute(text(f"""
            UPDATE {RE_FLIP_SHADOW_TABLE}
               SET settle_side=:ss, settle_price=:sp,
                   flip_won=(flip_side = :ss),
                   updated_at=now()
             WHERE asset=:a AND window_anchor=:wa
        """), {"ss": settle_side, "sp": settle_price,
               "a": ASSET, "wa": int(window_anchor)})
        for seq, pk in (peaks or {}).items():
            if pk is None:
                continue
            c.execute(text(f"""
                UPDATE {RE_FLIP_SHADOW_TABLE} SET peak_bid_flip=:pk
                 WHERE asset=:a AND window_anchor=:wa AND seq=:s
            """), {"pk": pk, "a": ASSET, "wa": int(window_anchor), "s": int(seq)})
        for seq, mn in (opp_mins or {}).items():
            if mn is None:
                continue
            c.execute(text(f"""
                UPDATE {RE_FLIP_SHADOW_TABLE} SET min_ask_opp=:mn
                 WHERE asset=:a AND window_anchor=:wa AND seq=:s
            """), {"mn": mn, "a": ASSET, "wa": int(window_anchor), "s": int(seq)})


def upsert_re_flip_shadow(rec):
    row = {k: rec.get(k) for k in _RE_FLIP_COLS}
    cols = ", ".join(_RE_FLIP_COLS)
    ph = ", ".join(f":{c}" for c in _RE_FLIP_COLS)
    set_clause = ", ".join(f"{c}=EXCLUDED.{c}" for c in _RE_FLIP_COLS
                           if c not in ("asset", "window_anchor", "seq"))
    with ENGINE.begin() as c:
        c.execute(text(f"""
            INSERT INTO {RE_FLIP_SHADOW_TABLE} ({cols}, updated_at)
            VALUES ({ph}, now())
            ON CONFLICT (asset, window_anchor, seq)
            DO UPDATE SET {set_clause}, updated_at = now()
        """), row)


def get_run_start():
    """Dashboard/bankroll cutover (config `run_start_utc`): only legs decided
    AT/AFTER this show in the table + count in the bankroll. Older rows STAY in
    the DB (needed for look-ahead analysis) — they're just not displayed/counted.
    None = no cutover."""
    return get_config("run_start_utc", None)


_DB_LEGS_CACHE = {}


def db_legs(max_age=0.0):
    """Persisted legs for the current dry_run mode, as dicts. Filtered to the
    fresh-run cutover (get_run_start) so the dashboard shows ONLY the fresh run;
    older rows remain in the table for look-ahead but are not displayed.

    `max_age` > 0 lets the dashboard export reuse a recent result. The export
    re-read ~16k rows x ~100 columns every cycle and held ~50% of the GIL, which
    stalled the event loop (and starved the REST book poller) through the entry
    window. Callers that size or decide on these rows keep the default (fresh).
    The cached list is shared: callers must treat the rows as read-only."""
    rs = get_run_start()
    key = (rs, DRY_RUN)
    hit = _DB_LEGS_CACHE.get(key)
    if max_age > 0 and hit is not None and time.monotonic() - hit[0] < max_age:
        return hit[1]
    try:
        with ENGINE.connect() as c:
            if rs:
                rows = c.execute(text(
                    f"SELECT * FROM {TRADES_TABLE} WHERE dry_run = :d "
                    "AND decided_at >= :rs"), {"d": DRY_RUN, "rs": rs}).mappings().all()
            else:
                rows = c.execute(text(
                    f"SELECT * FROM {TRADES_TABLE} WHERE dry_run = :d"
                ), {"d": DRY_RUN}).mappings().all()
        out = [dict(r) for r in rows]
        _DB_LEGS_CACHE.clear()
        _DB_LEGS_CACHE[key] = (time.monotonic(), out)
        return out
    except Exception:
        return []


# ---------------------------------------------------------------------------
# Live executor (only constructed when DRY_RUN=0 and a key is available).
# All order placement is isolated here; the Bot's dry-run path never touches it.
# ---------------------------------------------------------------------------
class LiveExecutor:
    def __init__(self, secure_client, wallet, private_key=None, active_proxy=None):
        self.c = secure_client
        self.wallet = wallet
        self._private_key = private_key   # kept only to rebuild self.c on failover
        self.active_proxy = active_proxy  # which relay self.c's egress is baked into
        self._failed_over = False         # latch: at most one failover per process

    @staticmethod
    async def _build_secure_client(private_key, proxy):
        # Pass wallet=None so the SDK DERIVES the funded proxy/deposit wallet from the
        # signer. Polymarket web/relayer accounts hold USDC in a derived DEPOSIT_WALLET
        # (e.g. 0x42CF47…), NOT the bare signer EOA — forcing the EOA as funder reads an
        # empty balance. An explicit funder can still be forced via POLYMARKET_FUNDER.
        funder = os.getenv("POLYMARKET_FUNDER") or None
        # Gasless deposit wallets REQUIRE a Relayer API Key (created in the PM UI →
        # Relayer API keys) to submit approvals/orders/redemptions. Build it if present.
        api_key = None
        rk, rk_addr = load_relayer_key()
        if rk:
            from polymarket.auth import RelayerApiKey
            from eth_account import Account
            addr = rk_addr or Account.from_key(private_key).address
            api_key = RelayerApiKey(key=rk, address=addr)
        # Bake the relay into the secure client's httpx transports (built eagerly
        # here) so ONLY order placement egresses through it; reads stay direct.
        # See _order_proxy_env / ORDER_PROXY at module top.
        with _order_proxy_env(proxy):
            sc = await AsyncSecureClient.create(private_key=private_key, wallet=funder,
                                                api_key=api_key)
        return sc

    @classmethod
    async def create(cls, private_key, wallet):
        proxy = ORDER_PROXY
        if proxy and not _proxy_tcp_alive(proxy):
            print(f"[order-proxy] PRIMARY {proxy} unreachable at startup", flush=True)
            if ORDER_PROXY_FALLBACK and _proxy_tcp_alive(ORDER_PROXY_FALLBACK):
                print(f"[order-proxy] starting on FALLBACK {ORDER_PROXY_FALLBACK} instead",
                      flush=True)
                proxy = ORDER_PROXY_FALLBACK
            else:
                print("[order-proxy] fallback unset/unreachable too — starting on "
                      "PRIMARY anyway (order placement will likely fail geo-check)",
                      flush=True)
        if proxy:
            print(f"[order-proxy] secure client egress via {proxy} "
                  "(reads stay direct)", flush=True)
        try:
            sc = await cls._build_secure_client(private_key, proxy)
        except Exception as e:
            alt = ORDER_PROXY_FALLBACK if (
                proxy != ORDER_PROXY_FALLBACK and ORDER_PROXY_FALLBACK) else None
            if alt and _is_proxy_failure(e):
                print(f"[order-proxy] PRIMARY build failed ({type(e).__name__}); "
                      f"trying FALLBACK {alt}", flush=True)
                proxy = alt
                sc = await cls._build_secure_client(private_key, proxy)
            else:
                raise
        return cls(sc, getattr(sc, "wallet", None) or wallet,
                    private_key=private_key, active_proxy=proxy)

    async def _failover(self):
        """Rebuild self.c through the OTHER configured relay. Returns True on a
        successful rebuild (caller should retry its order once), False if there's
        no alternate relay to try, we've already failed over once this process, or
        the rebuild itself failed (self.c is left untouched — caller's original
        error stands)."""
        if self._failed_over or not self._private_key:
            return False
        candidates = [p for p in (ORDER_PROXY, ORDER_PROXY_FALLBACK)
                      if p and p != self.active_proxy]
        if not candidates:
            return False
        new_proxy = candidates[0]
        print(f"[order-proxy] FAILOVER triggered: {self.active_proxy or 'direct'} "
              f"-> {new_proxy}", flush=True)
        try:
            sc = await self._build_secure_client(self._private_key, new_proxy)
        except Exception as e:
            print(f"[order-proxy] FAILOVER rebuild failed: {type(e).__name__}: {e}",
                  flush=True)
            return False
        self.c = sc
        self.active_proxy = new_proxy
        self._failed_over = True
        print(f"[order-proxy] FAILOVER complete, now egressing via {new_proxy}",
              flush=True)
        return True

    async def setup(self):
        # idempotent; deploys deposit wallet + sets trading approvals if needed
        await self.c.setup_trading_approvals()

    async def usdc_balance(self):
        """USDC (collateral) balance in dollars on the funded deposit wallet, else
        None. `balance` is in 6-dp base units (59719563 == $59.72)."""
        try:
            ba = await self.c.get_balance_allowance(asset_type="COLLATERAL")
        except Exception:
            return None
        v = getattr(ba, "balance", None)
        if v is None:
            return None
        try:
            return float(v) / 1e6
        except (TypeError, ValueError):
            return None

    async def position_avg(self, token_id):
        """(avg_price, shares) currently held on this token, else (None, 0)."""
        try:
            pg = self.c.list_positions()
            async for page in pg:
                for p in page.items:
                    if str(getattr(p, "token_id", "")) == str(token_id):
                        # PM's Position model field is `size` (not `shares`) — the
                        # old `shares` lookup silently returned 0, so every reconcile
                        # logged UNCONFIRMED / recon_shares=0 (2026-07-22).
                        sh = float(getattr(p, "size", None)
                                   or getattr(p, "shares", 0) or 0)
                        avg = getattr(p, "avg_price", None) or getattr(p, "average_price", None)
                        return (float(avg) if avg is not None else None), sh
        except Exception:
            pass
        return None, 0.0

    async def buy(self, token_id, stake_usd, ask, max_price=None, max_spend=None,
                  before_post=None):
        """Market BUY `stake_usd` of USDC, capped at ask+slip. Returns
        (avg_price, shares, ok, err).

        `max_price` pins the FAK limit outright instead of deriving it from
        ask+BUY_SLIP — for a book whose entry rule is a validated PRICE CAP
        rather than a slippage tolerance (see the flip book's FLIP_STAKE_MAX_ASK).

        Fill (shares/avg) is read straight off the order RESPONSE, not from a
        follow-up positions query: for a BUY, `taking_amount` = outcome shares
        received and `making_amount` = USDC paid. list_positions() lags the FAK
        fill by enough that querying it here returned 0 — mis-recording a real
        2.04sh live fill as 0sh/$0 (which would also skip hold-to-settle redeem).
        Falls back to position_avg only if the response carries no fill amounts.

        On a connection-shaped failure (the relay box itself is unreachable, not a
        PM API rejection) retries ONCE through the fallback relay — see
        LiveExecutor._failover()."""
        max_price = min(0.99, round(ask + BUY_SLIP, 2) if max_price is None
                        else round(max_price, 2))
        budget_args = {} if max_spend is None else {"max_spend": max_spend}
        self.last_buy_timing = {}
        resp = None
        for attempt in (1, 2):
            try:
                if max_spend is not None:
                    signing_at = time.time(); signing_clock = time.perf_counter()
                    import pm_order_metadata
                    cache = getattr(self,"_metadata_cache",None)
                    if cache is None:
                        cache = self._metadata_cache = pm_order_metadata.MetadataCache()
                    metadata_trace = []
                    signed = await pm_order_metadata.prepare(self.c,cache,metadata_trace,
                        token_id=str(token_id), side="BUY", amount=round(stake_usd, 2),
                        max_price=max_price, order_type="FAK", **budget_args)
                    self.last_buy_timing = dict(signing_started_at=signing_at,
                        signing_ms=(time.perf_counter()-signing_clock)*1000,
                        metadata_requests=metadata_trace)
                    if before_post is not None:
                        check_clock = time.perf_counter()
                        try:
                            self.last_buy_timing['final_check'] = await before_post()
                        except Exception as exc:
                            self.last_buy_timing.update(final_check_rejected=True,
                                final_check_ms=(time.perf_counter()-check_clock)*1000)
                            return None, 0.0, False, ('TWAP final check: '+str(exc))[:160]
                        self.last_buy_timing['final_check_ms'] = (time.perf_counter()-check_clock)*1000
                    self.last_buy_timing['post_started_at'] = time.time()
                    post_clock = time.perf_counter()
                    try:
                        resp = await self.c.post_order(signed)
                    finally:
                        self.last_buy_timing.update(response_at=time.time(),
                            post_response_ms=(time.perf_counter()-post_clock)*1000)
                    self.last_buy_timing.update(order_id=str(getattr(resp,'order_id','')),
                        exchange_status=str(getattr(resp,'status','')),
                        making_amount=str(getattr(resp,'making_amount',0)),
                        taking_amount=str(getattr(resp,'taking_amount',0)))
                else:
                    resp = await self.c.place_market_order(
                        token_id=str(token_id), side="BUY",
                        amount=round(stake_usd, 2), max_price=max_price, order_type="FAK", **budget_args)
                break
            except Exception as e:
                if attempt == 1 and _is_proxy_failure(e) and await self._failover():
                    continue
                return None, 0.0, False, f"{type(e).__name__}: {str(e)[:80]}"
        if not getattr(resp, "ok", False):
            return None, 0.0, False, f"{getattr(resp,'code','?')}: {getattr(resp,'message','')}"[:100]
        try:
            sh = float(getattr(resp, "taking_amount", 0) or 0)    # shares received
            usdc = float(getattr(resp, "making_amount", 0) or 0)  # USDC paid
        except (TypeError, ValueError):
            sh = usdc = 0.0
        if sh > 0:
            return (usdc / sh), sh, True, None
        # No fill on the response (killed/empty) — fall back to a positions read.
        avg, sh = await self.position_avg(token_id)
        return avg, sh, True, None

    async def sell_twap_cut(self, token_id, shares, bid):
        """All-or-none cut; account using response amounts, never the quote.

        An ambiguous transport result must not be blindly retried: the FOK may
        already have executed. The caller persists EXIT_REVIEW before sending.
        """
        try:
            resp = await self.c.place_market_order(
                token_id=str(token_id), side="SELL", shares=floor_shares(shares),
                min_price=max(0.01, round(bid - SELL_SLIP, 2)), order_type="FOK")
        except Exception as e:
            return None, 0.0, "review", f"{type(e).__name__}: {str(e)[:100]}"
        if not getattr(resp, "ok", False):
            return None, 0.0, "retry", str(getattr(resp, "message", "rejected"))[:100]
        if getattr(resp, "status", None) != "matched":
            return None, 0.0, "review", "sell accepted without matched confirmation"
        try:
            sold = float(getattr(resp, "making_amount", 0) or 0)
            cash = float(getattr(resp, "taking_amount", 0) or 0)
            px = cash / sold
            if (not all(math.isfinite(v) for v in (sold, cash, px))
                    or abs(sold - floor_shares(shares)) > 0.0001
                    or not 0 < px <= 1):
                raise ValueError("FOK fill amounts do not confirm full requested sale")
        except (TypeError, ValueError, ZeroDivisionError) as e:
            return None, 0.0, "review", str(e)
        return px, sold, "filled", None

    async def sell(self, token_id, shares, bid):
        """Market SELL `shares`, floor at bid-slip. Returns (ok, err). Retries once
        through the fallback relay on a connection-shaped failure — see buy()."""
        min_price = max(0.01, round(bid - SELL_SLIP, 2))
        resp = None
        for attempt in (1, 2):
            try:
                resp = await self.c.place_market_order(
                    token_id=str(token_id), side="SELL",
                    shares=round(shares, 2), min_price=min_price, order_type="FAK")
                break
            except Exception as e:
                if attempt == 1 and _is_proxy_failure(e) and await self._failover():
                    continue
                return False, f"{type(e).__name__}: {str(e)[:80]}"
        return bool(getattr(resp, "ok", False)), (
            None if getattr(resp, "ok", False)
            else f"{getattr(resp,'code','?')}: {getattr(resp,'message','')}"[:100])

    async def redeem(self, condition_id):
        """Redeem a SETTLED winning position → USDC (the payout half of hold-to-
        settle; winning outcome tokens redeem at $1, no taker fee). Returns
        (ok, err). Safe to call even if Polymarket's auto_redeem_operator already
        swept it — the CTF call just reverts and we report it (not fatal). Retries
        once through the fallback relay on a connection-shaped failure."""
        out = None
        for attempt in (1, 2):
            try:
                h = await self.c.redeem_positions(condition_id=str(condition_id))
                out = await h.wait()
                break
            except Exception as e:
                if attempt == 1 and _is_proxy_failure(e) and await self._failover():
                    continue
                return False, f"{type(e).__name__}: {str(e)[:100]}"
        # A successful SDK wait returns TransactionOutcome(hash, id) and has no
        # success/status field. Terminal failures raise above. Probing those
        # nonexistent fields classified every confirmed redemption as failed and
        # resubmitted it forever. This used to be BTC-only, with ETH kept on a
        # separate "legacy interpretation" out of caution; confirmed 2026-09-02
        # that the ETH branch had the identical bug (22k+ REDEEM-RETRY log
        # lines since 08-18, same windows retried 500-700+ times over days).
        txh = (getattr(out, "transaction_hash", None)
               or getattr(h, "transaction_hash", None))
        txid = (getattr(out, "transaction_id", None)
                or getattr(h, "transaction_id", None))
        if out is not None and (txh or txid):
            return True, txh or txid
        return False, f"unexpected outcome={str(out)[:100]}"

    # ----- MAKER (post-only) execution: rest, dwell, reprice toward mid -----
    async def _order_matched(self, order_id):
        """(size_matched, still_live) for `order_id`. A vanished/errored order
        is reported as not-live with unknown match (None) so the caller falls
        back to what it already accumulated."""
        try:
            o = await self.c.get_order(order_id=order_id)
        except Exception:
            return None, False
        try:
            m = float(getattr(o, "size_matched", 0) or 0)
        except (TypeError, ValueError):
            m = 0.0
        live = str(getattr(o, "status", "")).lower() in ("live", "delayed", "")
        return m, live

    async def _maker_leg(self, token_id, side, size, price):
        """Post ONE post-only limit leg at `price`, dwell up to MAKER_REPRICE_S
        polling for fills, then cancel the remainder. Returns
        (matched_shares, ok, err). ok=False means the post itself was rejected
        (e.g. post-only would have crossed) -> caller reprices."""
        try:
            resp = await self.c.place_limit_order(
                token_id=str(token_id), side=side, price=price,
                size=round(size, 2), post_only=True)
        except Exception as e:
            return 0.0, False, f"{type(e).__name__}: {str(e)[:80]}"
        if not getattr(resp, "ok", False):
            return 0.0, False, f"{getattr(resp,'code','?')}: {getattr(resp,'message','')}"[:80]
        oid = resp.order_id
        matched = 0.0
        waited = 0.0
        while waited < MAKER_REPRICE_S:
            await asyncio.sleep(MAKER_POLL_S)
            waited += MAKER_POLL_S
            m, live = await self._order_matched(oid)
            if m is not None:
                matched = m
                if m >= size - 1e-6:
                    break
            if not live:
                break
        try:
            await self.c.cancel_order(order_id=oid)
        except Exception:
            pass
        m, _ = await self._order_matched(oid)   # final reconcile after cancel
        if m is not None:
            matched = max(matched, m)
        return matched, True, None

    async def _maker_accumulate(self, token_id, side, shares, start_px, limit_px, step):
        """Fill `shares` across successive post-only legs, starting at start_px
        and stepping `step` per reprice toward mid, never past limit_px. Returns
        (vwap, filled, ok, err)."""
        filled = 0.0
        notional = 0.0
        price = round_tick(start_px)
        last_err = None
        for _ in range(MAKER_MAX_REPRICES + 1):
            if filled >= shares - 1e-6:
                break
            m, ok, err = await self._maker_leg(token_id, side, shares - filled, price)
            if not ok:
                last_err = err
            if m > 0:
                notional += price * m
                filled += m
            if (step > 0 and price >= limit_px) or (step < 0 and price <= limit_px):
                break
            nxt = price + step
            price = round_tick(min(limit_px, nxt) if step > 0 else max(limit_px, nxt))
        if filled <= 0:
            return None, 0.0, False, last_err or "maker unfilled"
        return notional / filled, filled, True, None

    async def buy_maker(self, token_id, shares, best_bid, best_ask):
        """Accumulate `shares` via post-only BUYs from the bid, repricing UP
        toward mid, capped 1 tick inside the ask. Returns (avg, filled, ok, err)."""
        ceil = round_tick(best_ask - TICK)
        return await self._maker_accumulate(
            token_id, "BUY", shares, best_bid, ceil, TICK)

    async def sell_maker(self, token_id, shares, best_bid, best_ask):
        """Offload `shares` via post-only SELLs from the ask, repricing DOWN
        toward mid, capped 1 tick inside the bid. Returns (avg, filled, ok, err)."""
        floor = round_tick(best_bid + TICK)
        return await self._maker_accumulate(
            token_id, "SELL", shares, best_ask, floor, -TICK)


# ---------------------------------------------------------------------------
# Live order book (WS market stream) — drives maker placement
# ---------------------------------------------------------------------------
class BookFeed:
    """Maintains live order books for a set of token_ids off the CLOB market WS
    channel, so maker repricing reacts to the touch moving instead of REST-
    snapshotting every dwell. Best-effort: a stale/missing/errored feed makes
    `levels()` return None and callers fall back to REST — never a single point
    of failure. Windows rotate every 5m; `ensure()` resubscribes when the active
    token set changes."""

    TRADE_TTL_S = 300.0               # keep recent trades ~one window

    def __init__(self, client):
        self.client = client
        self._tokens = ()             # currently-subscribed token set (sorted)
        self._handle = None
        self._task = None
        self._books = {}              # token_id(str) -> {"bids":{px:sz},"asks":{px:sz},"ts"}
        self._trades = {}             # token_id(str) -> deque[(ts, price, size)]
        self._sub_ts = 0.0            # when the current subscription started
        self.msgs = 0                 # telemetry: total events applied
        self.last_msg_at = 0.0        # when the socket last delivered ANY event
        self.trades_seen = 0          # telemetry: last_trade_price events captured
        self.last_source = "rest"     # what the most recent levels() served
        self.ws_hits = 0              # book_levels() served from the live feed
        self.rest_hits = 0            # ...and fell back to a REST snapshot
        self.onesided_hits = 0        # fresh WS book, but one-sided: REST read, not a feed failure

    def queue_at(self, token_id, price, side):
        """Resting size at exactly `price` on our side (`side` = the side we'd
        REST on: 'BUY' rests on the bid, 'SELL' rests on the ask). This is the
        volume queued AHEAD of a fresh maker order at that price. None if the
        token isn't tracked."""
        b = self._books.get(str(token_id))
        if not b:
            return None
        book_side = "bids" if str(side).upper() == "BUY" else "asks"
        return float(b[book_side].get(round_tick(price), 0.0))

    def fillable_estimate(self, token_id, best_bid, best_ask, side,
                          dwell_per_rung, max_rungs, vol_window_s):
        """Predict how many shares a maker order could fill across the WHOLE
        reprice ladder it would actually walk (touch -> toward mid), not just at
        the touch. For each rung price P the order rests at, expected fill over
        that rung's dwell = max(0, rate_through(P)/s × dwell_per_rung −
        queue_ahead(P)); summed across rungs. Higher rungs (toward mid) have
        thinner queues and capture more trade flow, so this is materially less
        conservative than a touch-only estimate.

        Returns (total_fillable, touch_queue, touch_rate, rungs) — where `rungs`
        is a list of (price, queue_ahead, rate_per_s, rung_fill) — or None when
        there isn't enough live data to judge (feed cold/stale or tape history
        younger than the volume window); caller then skips the gate."""
        if not WS_BOOK:
            return None
        b = self._books.get(str(token_id))
        if not b or b.get("touch_mismatch") or (time.time() - b["ts"]) > BOOK_STALE_S:
            return None
        if self._sub_ts <= 0 or (time.time() - self._sub_ts) < vol_window_s:
            return None                       # not enough tape history yet
        buy = str(side).upper() == "BUY"
        if buy:
            price, limit, step = round_tick(best_bid), round_tick(best_ask - TICK), TICK
        else:
            price, limit, step = round_tick(best_ask), round_tick(best_bid + TICK), -TICK
        since = time.time() - vol_window_s
        total = 0.0
        rungs = []
        touch_q = touch_r = None
        for _ in range(max_rungs + 1):
            qa = self.queue_at(token_id, price, side)
            qa = 0.0 if qa is None else qa
            vol = self.traded_through(token_id, price, side, since)
            rate = vol / vol_window_s if vol_window_s > 0 else 0.0
            rung_fill = max(0.0, rate * dwell_per_rung - qa)
            total += rung_fill
            rungs.append((round(price, 3), round(qa, 1), round(rate, 2), round(rung_fill, 1)))
            if touch_q is None:
                touch_q, touch_r = qa, rate
            if (step > 0 and price >= limit) or (step < 0 and price <= limit):
                break
            price = round_tick(min(limit, price + step) if step > 0
                               else max(limit, price - step))
        return total, touch_q, touch_r, rungs

    def traded_through(self, token_id, price, side, since_ts):
        """Cumulative traded volume that would fill a resting `side` order at
        `price` since `since_ts`: for a resting BUY, trades at price <= P; for a
        resting SELL, trades at price >= P. Side label on the tape is NOT trusted
        (this SDK inverts side naming) — price bound alone drives it."""
        dq = self._trades.get(str(token_id))
        if not dq:
            return 0.0
        p = round_tick(price)
        buy = str(side).upper() == "BUY"
        vol = 0.0
        for ts, tp, sz in dq:
            if ts < since_ts:
                continue
            if (buy and tp <= p + 1e-9) or ((not buy) and tp >= p - 1e-9):
                vol += sz
        return vol

    def confirmed_at(self, token_id, loop_ok=False):
        """Time up to which this token's book is known current.

        A token's `ts` only moves when ITS book changes, so a quiet but healthy
        book looks old. The socket delivers events in order: if any event was
        processed after `ts` and none changed this token, the book is unchanged
        up to that event. Only trusted while the socket task is alive, the
        touch agrees with the depth, and the caller vouches the event loop is
        not backlogged (`loop_ok`). Otherwise falls back to the plain `ts`."""
        b = self._books.get(str(token_id))
        if not b:
            return None
        ts = b.get("ts")
        last = self.last_msg_at
        if (not loop_ok or ts is None or b.get("touch_mismatch")
                or self._handle is None or self._task is None or self._task.done()
                or str(token_id) not in self._tokens or last < ts or last > time.time() + .001):
            return ts
        return last

    def levels(self, token_id):
        """(bids, asks) sorted like _levels() from the live book, or None if the
        token isn't tracked, the book is one-sided, or the snapshot is stale."""
        if not WS_BOOK:
            return None
        b = self._books.get(str(token_id))
        if not b or b.get("touch_mismatch") or (time.time() - b["ts"]) > BOOK_STALE_S:
            return None
        bids = sorted(((p, s) for p, s in b["bids"].items() if s > 0), reverse=True)
        asks = sorted((p, s) for p, s in b["asks"].items() if s > 0)
        if not bids or not asks:
            return None
        return bids, asks

    def one_sided_fresh(self, token_id):
        """True when the WS book is fresh and agrees with the touch but one side
        is empty, i.e. levels() is None for market reasons, not feed reasons."""
        b = self._books.get(str(token_id))
        if (not WS_BOOK or not b or b.get("touch_mismatch")
                or (time.time() - b["ts"]) > BOOK_STALE_S):
            return False
        return not (any(s > 0 for s in b["bids"].values())
                    and any(s > 0 for s in b["asks"].values()))

    def stats(self):
        """Health of the feed itself, for the dashboard. `age_s` is the age of the
        stalest CURRENTLY-SUBSCRIBED book — retired windows' tokens are still in
        _books and would otherwise dominate the max and read as a permanent
        outage. Once age_s passes BOOK_STALE_S every levels() call returns None
        and the whole book renders as `—`, which used to be indistinguishable
        from "Polymarket is quiet". `subscribed` says the socket is open, so
        subscribed=True with a rising age_s is the signature of the event loop
        being blocked rather than of a dead subscription."""
        now = time.time()
        ages = [now - self._books[t]["ts"] for t in self._tokens
                if self._books.get(t, {}).get("ts")]
        return {
            "subscribed": self._handle is not None,
            "tokens": len(self._tokens),
            "tracked": len(self._books),
            "msgs": self.msgs,
            "trades_seen": self.trades_seen,
            "last_source": self.last_source,
            "ws_hits": self.ws_hits,
            "rest_hits": self.rest_hits,
            "onesided_hits": self.onesided_hits,
            "ws_frac": (round(self.ws_hits / (self.ws_hits + self.rest_hits), 3)
                        if (self.ws_hits + self.rest_hits) else None),
            "sub_age_s": round(now - self._sub_ts, 1) if self._sub_ts else None,
            "age_s": round(max(ages), 1) if ages else None,
            "stale_after_s": BOOK_STALE_S,
        }

    def _prune(self, keep_s=900.0):
        """Drop books/tapes for tokens we no longer follow. Windows roll every 5m
        and nothing ever evicted the retired tokens, so _books grew by 2 full
        depth ladders every window — unbounded for the life of the process.
        Retired entries are kept `keep_s` past their last update so an in-flight
        _shadow_fill_log task (which outlives its window by at most one reprice
        ladder) still finds the book it is measuring."""
        cutoff = time.time() - keep_s
        for store in (self._books, self._trades):
            for t in [t for t in store
                      if t not in self._tokens
                      and (store[t]["ts"] if store is self._books
                           else (store[t][-1][0] if store[t] else 0)) < cutoff]:
                store.pop(t, None)

    async def ensure(self, token_ids):
        """(Re)subscribe when the active token set changes; no-op otherwise."""
        if not WS_BOOK:
            return
        want = tuple(sorted(str(t) for t in token_ids if t))
        now = time.time()
        broken = (self._task is None or self._task.done() or any(
            now - self._books.get(t, {}).get("mismatch_since", now) > 1.
            for t in want))
        if not want or (want == self._tokens and not broken):
            return
        if want == self._tokens and now - getattr(self, "_resync_at", 0.) < 5.:
            return
        self._resync_at = now
        await self.stop()
        # Require fresh subscription snapshots; never seed depth from touch.
        for token in want:
            self._books.pop(token, None)
        try:
            self._handle = await self.client.subscribe(MarketSpec(token_ids=list(want)))
        except Exception as e:
            _err("bookfeed.subscribe", e)
            print(f"[bookfeed] subscribe failed ({type(e).__name__}: "
                  f"{str(e)[:80]}); REST fallback", flush=True)
            self._handle = None
            return
        self._tokens = want
        self._sub_ts = time.time()
        self._prune()
        self._task = asyncio.create_task(self._consume())

    async def _consume(self):
        try:
            async for ev in self._handle:
                received_at = time.time()
                cache = getattr(self,'execution_metadata_cache',None)
                if cache is not None and getattr(ev,'type',None)=='tick_size_change':
                    cache.invalidate_tick(getattr(getattr(ev,'payload',None),'token_id',None))
                self._apply(ev)
                self.msgs += 1
                self.last_msg_at = received_at
                wake = getattr(self, 'market_event', None)
                if wake is not None:
                    off = received_at % WINDOW_S
                    if TWAP_EDGE_T0_S <= off < TWAP_EDGE_T1_S or TWAP_LOCK_T0_S <= off < TWAP_LOCK_T1_S:
                        wake.set()
                if getattr(self, 'tick_study', None) is not None:
                    try:
                        self.tick_study.on_event(ev, received_at)
                    except Exception as exc:
                        self.tick_study.errors += 1
                        self.tick_study.last_error = str(exc)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            cache = getattr(self,'execution_metadata_cache',None)
            if cache is not None: cache.invalidate_tick()
            _err("bookfeed.stream_ended", e)
            print(f"[bookfeed] stream ended ({type(e).__name__}: {str(e)[:80]}); "
                  "REST fallback until resubscribe", flush=True)

    def _apply(self, ev):
        typ = getattr(ev, "type", None)
        pl = getattr(ev, "payload", None)
        if pl is None:
            return
        if typ == "book":
            self._books[str(pl.token_id)] = {
                "bids": {float(l.price): float(l.size) for l in pl.bids},
                "asks": {float(l.price): float(l.size) for l in pl.asks},
                "ts": time.time(),
                "touch_mismatch": False,
            }
        elif typ == "price_change":
            touched = {}
            for pc in pl.price_changes:
                tid = str(pc.token_id)
                b = self._books.get(tid)
                if b is None:
                    # Deltas cannot reconstruct depth without an initial snapshot.
                    continue
                book_side = "bids" if str(pc.side).upper() == "BUY" else "asks"
                px, sz = float(pc.price), float(pc.size)
                if sz <= 0:
                    b[book_side].pop(px, None)
                else:
                    b[book_side][px] = sz
                b["ts"] = time.time()
                touched[tid] = pc
            # Advertised touch describes the completed message, not a partial
            # application of its deltas. Publish only after the whole batch.
            for tid, pc in touched.items():
                self._check_touch(self._books[tid], pc)
        elif typ == "best_bid_ask":
            b = self._books.get(str(pl.token_id))
            if b is not None:
                # Top-of-book messages have no sizes: do not fabricate depth or
                # renew its age. Reject cached depth that contradicts the quote.
                self._check_touch(b, pl)
        elif typ == "last_trade_price":
            tid = str(pl.token_id)
            dq = self._trades.get(tid)
            if dq is None:
                dq = self._trades[tid] = deque()
            now = time.time()
            try:
                dq.append((now, float(pl.price), float(pl.size or 0)))
                self.trades_seen += 1
            except (TypeError, ValueError):
                pass
            cutoff = now - self.TRADE_TTL_S
            while dq and dq[0][0] < cutoff:
                dq.popleft()

    @staticmethod
    def _check_touch(book, quote):
        checked = False
        mismatch = False
        for name, reverse in (("bids", True), ("asks", False)):
            value = getattr(quote, "best_bid" if reverse else "best_ask", None)
            if value is None:
                continue
            checked = True
            levels = [p for p, s in book[name].items() if s > 0]
            cached = (max(levels) if reverse else min(levels)) if levels else None
            advertised = float(value)
            if not math.isfinite(advertised):
                mismatch = True
                continue
            # The venue advertises an EMPTY side as best_bid 0 / best_ask 1.
            # That agrees with a book that has no levels on that side; it is
            # not a mismatch (and must not trigger a resubscription).
            if (advertised <= 0) if reverse else (advertised >= 1):
                mismatch |= cached is not None
                continue
            mismatch |= cached is None or abs(cached - advertised) > 1e-9
        if checked:
            book["touch_mismatch"] = mismatch
            if mismatch:
                book.setdefault("mismatch_since", time.time())
            else:
                book.pop("mismatch_since", None)

    async def stop(self):
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except BaseException:
                pass
            self._task = None
        if self._handle is not None:
            try:
                await self._handle.close()
            except Exception:
                pass
            self._handle = None
        self._tokens = ()


# ---------------------------------------------------------------------------
# Bot
# ---------------------------------------------------------------------------
class Bot:
    def __init__(self):
        self.windows = {}   # anchor -> dict
        self._twap_model = twap_cal.read_json(twap_cal.MODEL, {})
        self._twap_refresh_task = None
        self._twap_refresh_ts = 0.
        self.client = None
        self.executor = None
        self.bookfeed = None  # live WS order book; None until run() wires it
        self.bankroll = BANKROLL_USD  # compounded live: starting capital + realized P&L
        # TWO_BOOK: SEPARATE compounding bankrolls, each off its SHARE of the total
        # dash bankroll (2026-07-13). Each book NEVER touches another's P&L.
        # Compounding model:
        #   • T+3   — CONTINUOUS: bankroll_total = t3_capital() + t3 realized (steps
        #     on every settle).
        #   • FADE (+hfade pool) & FLIP — 24-HOUR DAILY: fade_base / flip_base =
        #     capital share + that book's P&L SETTLED before today's 00:00 UTC; FIXED
        #     intraday, steps only at UTC midnight (see _daily_book_capital).
        # Refreshed each tick from DB.
        self.bankroll_total = t3_capital()    # t3 compounded pool (continuous)
        self.fade_base = fade_capital()       # fade+hfade pool (24h daily)
        self.flip_base = flip_capital()       # flip pool (24h daily)
        self.oppfade_base = oppfade_capital() # cheap-opp fade pool (24h daily)
        # LIVE reconciled wallet balance — refreshed on a throttle in tick(); used
        # only as the affordability CAP on a stake (never overspend the wallet).
        self._live_usdc = None
        self._live_usdc_ts = 0.0
        self._pm_recon_ts = 0.0
        self._redeem_recovery_task = None
        self._redeem_inflight = set()
        self._redeem_recovery_health = {}
        # DAILY-LOCKED oppfade capital (2026-07-22): the exchange USDC balance
        # snapshotted at 00:00 UTC and held FIXED all day (NOT compounded per stake).
        # Usable = _daily_capital * OPPFADE_USABLE_FRAC (2/3); stake = 10% of usable.
        # Persisted in config so a mid-day restart reuses the day's snapshot.
        self._daily_capital = None
        self._daily_capital_day = None
        self.t3_pnl = 0.0                     # cumulative T+3 realized (display)
        self.fade_pnl = 0.0                   # cumulative fade realized (display)
        self.hfade_pnl = 0.0                  # cumulative high-fade realized (display)
        self.flip_pnl = 0.0                   # cumulative flip realized (display)
        self.oppfade_pnl = 0.0                # cumulative cheap-opp fade realized
        self.live = False   # True only if DRY_RUN=0, key present, approvals+balance OK
        # (condition_id, slug, attempt, not_before_monotonic) for live winners.
        # Redemption frequently races Polymarket's closed-market index by a few
        # seconds.  A one-shot queue used to drop those winners permanently after
        # "No market found for condition"; retain failures and retry with backoff.
        self._redeem_q = []
        # STAKING gate (live-read each tick): when True the loop still runs + COLLECTS
        # (T+2 probe / eval legs) but places no stakes. DATA_ONLY or trading_enabled()=off.
        self.collect_only = DATA_ONLY

    def _daily_book_capital(self, books, base):
        """24-HOUR (daily) compounding capital for a book — or book GROUP that shares
        one pool, e.g. ['fade','hfade']. = base share + that group's realized P&L
        SETTLED before TODAY's 00:00 UTC (scoped to the current run). It STEPS only at
        UTC midnight and is FIXED intraday, so the whole day sizes off one locked
        figure: e.g. $5 base + the +$6 booked by yesterday's close → $11 held all of
        today; at the next 00:00 it rolls forward again. Floored at 0."""
        rs = get_run_start()
        midnight = datetime.now(timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0)
        clause = " AND decided_at >= :rs" if rs else ""
        ph = ",".join(f":b{i}" for i in range(len(books)))
        binds = {f"b{i}": b for i, b in enumerate(books)}
        try:
            with ENGINE.connect() as c:
                pnl = float(c.execute(text(
                    "SELECT COALESCE(SUM(realized_pnl_usd),0) FROM "
                    f"{TRADES_TABLE} WHERE dry_run=:d AND book IN ({ph}) "
                    "AND realized_pnl_usd IS NOT NULL AND exit_at < :m" + clause),
                    {"d": DRY_RUN, "m": midnight, "rs": rs, **binds}).scalar() or 0.0)
        except Exception:
            pnl = 0.0
        return max(0.0, base + pnl)

    def t3_compound_stake_for(self):
        """Live wiring for t3_compound_decision() (2026-09-23 $100 trial).
        Bankroll reuses _daily_book_capital's proven midnight-lock mechanism
        instead of a new bespoke snapshot. Peak and halt state live in
        polymarket_config so a restart cannot forget a halt or reset the
        high-water mark — clearing `t3_compound_halted` is a deliberate manual
        step (stop-and-reevaluate, not auto-resume)."""
        if str(get_config("t3_compound_halted", "0")).strip().lower() in (
                "1", "true", "yes"):
            return 0.0
        bankroll = self._daily_book_capital(["t3"], T3_COMPOUND_BASE_USD)
        peak = float(get_config("t3_compound_peak_usd", T3_COMPOUND_BASE_USD)
                     or T3_COMPOUND_BASE_USD)
        stake, new_peak, halt, reason = t3_compound_decision(bankroll, peak)
        if new_peak != peak:
            set_config("t3_compound_peak_usd", new_peak)
        if halt:
            set_config("t3_compound_halted", "1")
            set_config("t3_compound_halt_reason", reason)
            print(f"[{datetime.now(timezone.utc):%H:%M:%S}] T3-COMPOUND HALT "
                  f"{reason}", flush=True)
            return 0.0
        if self._live_usdc is not None:
            stake = min(stake, float(self._live_usdc))
        return max(0.0, stake)

    def _two_book_bankrolls(self):
        """Per-book SIZING capital + realized P&L for display (2026-07-13 model):
          • T+3 — CONTINUOUS compounding: t3_capital() + t3 realized P&L (steps on
            every settle).
          • FADE (+hfade pool) & FLIP — 24-HOUR DAILY compounding: base share + that
            book's P&L SETTLED before today's 00:00 UTC; FIXED intraday, steps at UTC
            midnight (see _daily_book_capital).
        The returned t3/fade/hfade/flip/oppfade P&L are ALL-TIME (this run) for the
        dashboard. Returns (t3_bankroll, fade_daily, flip_daily, oppfade_daily,
        t3_pnl, fade_pnl, hfade_pnl, flip_pnl, oppfade_pnl)."""
        rs = get_run_start()
        cut = " AND decided_at >= :rs" if rs else ""
        params = {"d": DRY_RUN, "rs": rs}

        def _book(bk):   # all-time realized this run (DISPLAY P&L)
            try:
                with ENGINE.connect() as c:
                    return float(c.execute(text(
                        "SELECT COALESCE(SUM(realized_pnl_usd),0) FROM "
                        f"{TRADES_TABLE} WHERE dry_run=:d AND book=:bk "
                        "AND realized_pnl_usd IS NOT NULL" + cut),
                        {**params, "bk": bk}).scalar() or 0.0)
            except Exception:
                return 0.0
        t3, fade, hf, flip = _book("t3"), _book("fade"), _book("hfade"), _book("flip")
        opf = _book("oppfade")
        return (max(0.0, t3_capital() + t3),                          # T+3 continuous
                self._daily_book_capital(["fade", "hfade"], fade_capital()),  # fade+hfade 24h daily
                self._daily_book_capital(["flip"], flip_capital()),          # flip 24h daily
                self._daily_book_capital(["oppfade"], oppfade_capital()),    # oppfade 24h daily
                t3, fade, hf, flip, opf)

    def _realized_net(self):
        """All-time realized P&L of staked trades under the maker model (kept for
        the dashboard's maker-vs-strategy comparison; NOT the bankroll driver)."""
        try:
            with ENGINE.connect() as c:
                return float(c.execute(text(
                    "SELECT COALESCE(SUM(realized_pnl_usd),0) "
                    f"FROM {TRADES_TABLE} WHERE dry_run=:d AND staked=true"),
                    {"d": DRY_RUN}).scalar() or 0.0)
        except Exception:
            return 0.0

    def _real_lifetime(self):
        """Real-money performance for the CURRENT rule epoch.

        ``run_start_utc`` is the durable per-asset rule cutover used by every other
        dashboard card.  Applying it here prevents trades made by retired gates
        from labelling the current rules as losing.  Rows are never deleted: the
        unfiltered all-time result remains available under ``archive`` for audit.
        This is always hard-wired to real stakes (``dry_run=false``).
        """
        rs = get_run_start()
        out = {"net": 0.0, "wins": 0, "losses": 0, "n": 0,
               "start_bankroll": round(BANKROLL_USD, 2),
               "bankroll": round(BANKROLL_USD, 2), "by_book": {}, "trades": [],
               "epoch_start": rs,
               "archive": {"net": 0.0, "wins": 0, "losses": 0, "n": 0}}
        try:
            with ENGINE.connect() as c:
                rows = c.execute(text(
                    "SELECT window_anchor, leg, book, predicted_side, entry_price, "
                    "stake_usd, exit_reason, realized_pnl_usd, decided_at, exit_at "
                    f"FROM {TRADES_TABLE} WHERE dry_run=false AND staked=true "
                    "AND realized_pnl_usd IS NOT NULL ORDER BY decided_at")
                ).mappings().all()
        except Exception:
            return out
        archive_net = sum(float(r["realized_pnl_usd"]) for r in rows)
        out["archive"] = {
            "net": round(archive_net, 2),
            "wins": sum(1 for r in rows if float(r["realized_pnl_usd"]) > 0),
            "losses": sum(1 for r in rows if float(r["realized_pnl_usd"]) < 0),
            "n": len(rows),
        }
        if rs:
            try:
                rs_dt = datetime.fromisoformat(str(rs).replace("Z", "+00:00"))
                if rs_dt.tzinfo is None:
                    rs_dt = rs_dt.replace(tzinfo=timezone.utc)
                rows = [r for r in rows
                        if r["decided_at"] is not None and r["decided_at"] >= rs_dt]
            except (TypeError, ValueError):
                # A malformed cutover must fail closed for display: do not revive
                # old-rule losses merely because a config value was mistyped.
                rows = []
        net = 0.0
        by_book = {}
        for r in rows:
            p = float(r["realized_pnl_usd"])
            net += p
            if p > 0:
                out["wins"] += 1
            elif p < 0:
                out["losses"] += 1
            bk = r["book"] or "—"
            b = by_book.setdefault(bk, {"net": 0.0, "wins": 0, "losses": 0, "n": 0})
            b["net"] += p
            b["n"] += 1
            if p > 0:
                b["wins"] += 1
            elif p < 0:
                b["losses"] += 1
            out["trades"].append({
                "window_anchor": r["window_anchor"], "leg": r["leg"], "book": bk,
                "side": r["predicted_side"],
                "entry_price": (float(r["entry_price"])
                                if r["entry_price"] is not None else None),
                "stake_usd": (float(r["stake_usd"])
                              if r["stake_usd"] is not None else None),
                "exit_reason": r["exit_reason"], "pnl": round(p, 4),
                "decided_at": r["decided_at"].isoformat() if r["decided_at"] else None,
                "exit_at": r["exit_at"].isoformat() if r["exit_at"] else None,
            })
        for b in by_book.values():
            b["net"] = round(b["net"], 2)
        out["n"] = len(rows)
        out["net"] = round(net, 2)
        out["bankroll"] = round(BANKROLL_USD + net, 2)
        out["by_book"] = by_book
        out["trades"] = list(reversed(out["trades"]))[:40]   # most-recent first
        return out

    def _kelly_shares(self, bankroll, acc, ask):
        """Fixed-fraction shares: STAKE_FRAC of bankroll, floored at the 5-share
        min ticket, no $ cap (0 if unaffordable). `acc` unused (kept for sig)."""
        if ask <= 0 or ask >= 1 or bankroll <= 0:
            return 0.0
        stake = max(STAKE_FRAC * bankroll, MIN_SHARES * ask)
        if stake > bankroll + 1e-9:
            return 0.0
        return stake / ask

    def _strategy_bankroll(self):
        """Compounded capital under the committed strategy (TAKER on maker-no-fill,
        SKIP on fill), walked chronologically so each stake rides the growing
        bankroll. This is what the bankroll compounds on — the maker P&L is
        illusory (adverse selection), so we don't compound on it."""
        ff = _shadow_fill_map()
        try:
            rows = [r for r in db_legs()
                    if r.get("staked") and r.get("realized_pnl_usd") is not None]
        except Exception:
            return BANKROLL_USD
        rows.sort(key=lambda r: (str(r.get("window_start_utc") or ""), r.get("leg") or 0))
        B = BANKROLL_USD
        for r in rows:
            f = ff.get((r.get("window_anchor"), r.get("leg")))
            if f is not None and f > 0.5:
                continue                                    # maker fills -> SKIP
            per = _taker_per_share(r.get("entry_price"), r.get("exit_reason"),
                                   r.get("exit_price"))
            if per is None:
                continue
            ask = min(0.99, (r.get("entry_price") or 0) + STRAT_SPREAD)
            B += self._kelly_shares(B, r.get("tier_acc"), ask) * per
        return max(0.0, round(B, 2))

    async def market_tokens(self, anchor):
        slug = f"{SLUG_PREFIX}-{anchor}"
        m = await self.client.get_market(slug=slug)
        if not m:
            return None
        return {
            "slug": slug,
            "condition_id": str(m.condition_id) if m.condition_id else None,
            "up_token": m.outcomes.yes.token_id,     # "Up"
            "down_token": m.outcomes.no.token_id,    # "Down"
        }

    async def book_levels(self, token_id):
        # Prefer the live WS book; fall back to a REST snapshot when the feed
        # isn't warm/fresh for this token.
        if self.bookfeed is not None:
            lv = self.bookfeed.levels(token_id)
            if lv is not None:
                self.bookfeed.last_source = "ws"
                self.bookfeed.ws_hits += 1
                return lv
        if self.bookfeed is not None:
            self.bookfeed.last_source = "rest"
            # COUNT the fallbacks, don't just remember the last one. `last_source`
            # is a single-sample gauge: it reported whatever the most recent call
            # happened to hit, so a feed serving 5% of reads and one serving 100%
            # looked identical. The ratio is the number that actually says whether
            # the WS book is carrying the bot or quietly doing nothing.
            if self.bookfeed.one_sided_fresh(token_id):
                # A healthy feed whose book has an empty side (a decided market)
                # is not a WS failure; keep it out of the ws_frac denominator.
                self.bookfeed.onesided_hits += 1
            else:
                self.bookfeed.rest_hits += 1
        bk = await self.client.get_order_book(token_id=token_id)
        return _levels(bk)

    def new_window(self, anchor):
        return {
            "window_anchor": anchor,
            "window_start_utc": datetime.fromtimestamp(anchor, tz=timezone.utc),
            "expiry": anchor + WINDOW_S,
            "slug": None, "up_token": None, "down_token": None, "condition_id": None,
            "price_open": None,            # Chainlink (resolution baseline)
            # FIRST-TOUCH state, latched by _t2_probe over 0..SWINGFADE_ELAPSED_MIN.
            # sf_pre_ticks starts at 0 so a window we joined LATE (restart mid-window)
            # fails the coverage floor and the condition refuses to arm, rather than
            # reading "never saw a cheap ask" as "the ask was never cheap".
            "sf_pre_ticks": 0,
            "sf_min_pre_ask": None,
            "faded": False,                # ONE fade per window — no re-fading a failed reversal
            # ONE oppfade ATTEMPT per window (2026-07-20). Separate from
            # open_leg because a REJECTED order must consume the window too, and
            # open_leg means "a real position to manage" — parking a non-staked leg
            # there would send it through manage()/settlement and silence the
            # re-flip shadow logging that generates this book's own entries.
            "oppfade_done": False,
            # swingfade claims its window independently of oppfade — the two are
            # SEPARATE books on the same primitive and may both fire in one window.
            "swingfade_done": False,
            "swingfade_logged": False,   # one SWINGFADE-SKIP journal line per window
            # Shared latch for the four price-band SHADOW books. Only the earliest
            # qualifying band may claim a window, so one market cannot accumulate
            # several correlated counterfactual stakes as its ask traverses bands.
            "swingband_done": False,
            "adx": None, "adx_confirmed": None, "adx_short": None,
            "actual_outcome": None, "price_settle": None,
            # leg machinery
            "legs": [],            # every recorded leg (staked or T+3 eval)
            # open_leg = the one ACTIVELY-MANAGED staked leg (t3/fade/hfade), or None.
            # NOT "the only position": hold-to-settle books park in hold_legs below.
            "open_leg": None,
            # HOLD-TO-SETTLE legs (swingfade today). Split out 2026-07-29 so a t3
            # stake and a swingfade stake can co-exist in ONE window — the books are
            # independent and the owner wants one of each. Before this they shared
            # open_leg, and the re-flip block that swingfade lives inside only runs
            # while `open_leg is None`, so ANY t3 stake silently suppressed swingfade
            # for that window. They fire at the same instant (DECISION_OFFSET_S=180 ==
            # SWINGFADE_ELAPSED_MIN=180), so the collision was total, not occasional.
            # manage() already returns early for these books. settle() iterates
            # w["legs"] but gates finalization on being a POSITION, so it must check
            # hold_legs too — it did NOT when this slot was introduced, which hung
            # the 07-29 21:48 leg OPEN with no P&L and no redeem. See settle().
            "hold_legs": [],
            "staked_count": 0,     # staked legs opened this window (ALL books)
            "t3_staked": False,    # this window's ONE t3 stake is spent
            "t3_done": False,      # T+3 evaluation recorded
            "t3_no_stake": False,  # did the T+3 decision NOT stake? (enables revision logging)
            "last_eval_key": None, # last logged (side,tier) — dedupes post-T+3 revision rows
            # T+2 -> T+3 shadow probe (pure logging): seq counter + last recorded
            # would-enter state, so we only write a new row when the state CHANGES
            "t2_seq": 0,
            "t2_last_key": None,
            # RE-FLIP SHADOW: T+3 call side (flip is measured vs this) + dedup state
            "t3_side": None,
            # ORIGINAL T+3 signal strength — the "origin" half of rule D. tier is
            # None for a sub-2bps decision (tier_for returns no label there), which
            # rule D treats as a weak origin, same as the historical read.
            "t3_tier": None,
            "t3_bps": None,
            "reflip_seq": 0,
            "reflip_last_key": None,
            "reflip_cur_seq": None,   # seq of the currently-active flip (peak target)
            "reflip_peaks": {},       # seq -> best flip-side bid seen (exit-early score)
            "reflip_opp_min": {},     # seq -> lowest OPP-side ask seen (maker-fill test)
            # FLIP CONTEXT (2026-07-13): price PATH + intra-window ADX, tracked from
            # window OPEN (the excursion a flip reverses happens pre-T+3, before the
            # RE window). path_max_*_bps = peak |price-open| each direction; adx_series
            # = one ADX(short) sample/min for the intra-window slope.
            "path_max_up_bps": 0.0,
            "path_max_dn_bps": 0.0,
            # SWING CONTEXT (2026-07-23, swingfade): how many times price has
            # crossed the BEAT price (window open) so far. Sampled on the same
            # tick as path_max_*; counts sign changes of (price_now - price_open),
            # ignoring exact-zero ticks. This is the "is it swinging or trending"
            # input to swingfade_ok() and it is CAUSAL by construction — the value
            # read at a decision only ever reflects ticks already seen.
            "cross_count": 0,
            "cross_last_sign": 0,
            "adx_series": [],         # [(elapsed_s, adx7), ...]
            "adx_last_min": -1,       # last minute-index sampled (per-min dedupe)
            "vol_3min_bps": None,  # trailing realized 3-min move vol (regime), set at T+3
            "last_exit_ts": None,  # for re-entry cooldown
            "settled": False,
        }

    def _new_leg(self, w, idx, is_t3):
        return {
            "window_anchor": w["window_anchor"], "leg": idx, "is_t3": is_t3,
            "window_start_utc": w["window_start_utc"], "slug": w["slug"],
            "decided_at": datetime.now(timezone.utc),
            "predicted_side": None, "tier": None, "tier_acc": None,
            "decision_bps": None, "adx": w.get("adx"),
            "adx_confirmed": w.get("adx_confirmed"), "adx_short": w.get("adx_short"),
            "price_open": w["price_open"], "price_decision": None,
            "staked": False, "no_stake_reason": None, "token_id": None,
            "entry_price": None, "shares": None, "stake_usd": None,
            "exit_reason": None, "exit_price": None, "exit_at": None,
            "price_settle": None, "actual_outcome": None, "hit": None,
            "realized_pnl_usd": None, "status": "WATCH",
            # Binance-early (leading-feed) direction, logged for look-ahead measurement
            "book": None,
            "bin_mom_bps": None, "fade_confirmed": None,
            "retry_of_kill": None, "retry_ask_at_kill": None, "retry_rt_s": None,
            "retry_queue_sh": None,
            "entry_fee_usd": None, "recon_shares": None, "recon_avg_px": None,
            "recon_usdc_after": None, "recon_ok": None,
            "pm_outcome": None, "pm_recon_ok": None, "pm_reconciled_at": None,
        }

    async def _reconcile_stake(self, leg, token_id, expected_sh):
        """Reconcile ONE real stake against the exchange's own book (
        2026-07-21). Runs as a detached task — never block the tick loop.

        The order RESPONSE is not proof of what we hold: `making_amount` excludes
        the taker fee, and a rejection historically wrote nothing at all. So after
        every real fill we ask Polymarket what it thinks we own and what
        collateral is left, and stamp the answer on the row.

        list_positions() LAGS the FAK response (see executor.buy — an immediate
        read returned 0 shares on a real 2.04sh fill), so poll with backoff and
        treat a persistent 0 as UNCONFIRMED (recon_ok=False), not as a zero fill:
        the position is real either way and must never be silently written down."""
        try:
            avg = None
            sh = 0.0
            for _ in range(4):
                await asyncio.sleep(RECON_POLL_S)
                avg, sh = await self.executor.position_avg(token_id)
                if sh:
                    break
            bal = await self.executor.usdc_balance()
            # Wallet telemetry is live, unlike the staking bankroll. Publish the
            # post-stake PM collateral as soon as reconciliation reads it so the
            # dashboard updates after every real fill. This deliberately does NOT
            # call cap_capital_to_wallet(); configured bankroll reconciliation is
            # daily-only in tick() at the UTC compounding boundary below.
            if bal is not None:
                self._live_usdc = bal
                self._live_usdc_ts = time.time()
            leg["recon_shares"] = round(sh or 0.0, 2)
            leg["recon_avg_px"] = round(avg, 4) if avg is not None else None
            leg["recon_usdc_after"] = round(bal, 2) if bal is not None else None
            # tolerance: 1% of the expected fill or half a share, whichever wider
            tol = max(0.5, 0.01 * float(expected_sh or 0.0))
            leg["recon_ok"] = bool(sh) and abs(sh - float(expected_sh or 0.0)) <= tol
            upsert(leg)
            _tag = "OK" if leg["recon_ok"] else ("UNCONFIRMED" if not sh else "MISMATCH")
            print(f"[{datetime.now(timezone.utc):%H:%M:%S}] RECON {_tag} "
                  f"L{leg['leg']} {leg.get('book')} — ours {expected_sh:.2f}sh "
                  f"@{leg.get('entry_price')} (fee ${leg.get('entry_fee_usd')}) · "
                  f"exchange {sh:.2f}sh @{avg} · USDC ${leg['recon_usdc_after']}",
                  flush=True)
        except Exception as e:
            # reconciliation is an observer: it must never take the bot down
            print(f"[{datetime.now(timezone.utc):%H:%M:%S}] RECON FAILED "
                  f"L{leg.get('leg')}: {type(e).__name__}: {str(e)[:100]}",
                  flush=True)

    def _schedule_redemption_recovery(self, now):
        """One read-only recovery scan at a time, outside the decision tick."""
        if not (self.live and self.executor is not None):
            return
        task = getattr(self, '_redeem_recovery_task', None)
        if task is not None and not task.done():
            return
        if now - getattr(self, '_redeem_recovery_ts', 0.0) < 300.0:
            return
        self._redeem_recovery_ts = now
        self._redeem_recovery_task = asyncio.create_task(self._run_redemption_recovery())

    async def _run_redemption_recovery(self):
        started = time.perf_counter()
        self._redeem_recovery_health = dict(running=True, started_at=time.time())
        try:
            await self._recover_pending_redemptions()
        except Exception as exc:
            _err('redeem_recovery', exc)
            self._redeem_recovery_health['error'] = type(exc).__name__
        finally:
            self._redeem_recovery_health.update(
                running=False, finished_at=time.time(),
                duration_ms=round((time.perf_counter()-started)*1000, 1))
            print('REDEEM-RECOVERY-TIMING ' + json.dumps(self._redeem_recovery_health), flush=True)

    async def _recover_pending_redemptions(self):
        """Recover held tokens from one complete wallet snapshot per sweep.

        A failed or incomplete snapshot leaves recovery for the next sweep. This
        never changes fills/PnL and retains winner and loser token redemption.
        """
        if not (self.live and self.executor is not None):
            return
        def read_rows():
            with ENGINE.connect() as c:
                return [dict(r) for r in c.execute(text(
                    f"SELECT DISTINCT window_anchor, slug, token_id, book "
                    f"FROM {TRADES_TABLE} WHERE dry_run=false AND staked=true "
                    "AND status='CLOSED' AND token_id IS NOT NULL"
                )).mappings().all()]
        rows = await _offload(read_rows)
        rows = [r for r in rows if r.get('book') in TAKER_HOLD_BOOKS]
        self._redeem_recovery_health['historical_rows'] = len(rows)
        if not rows:
            return
        # position_avg() fetches EVERY page for each token. With hundreds of
        # historical tokens that made one tick last almost a whole window.
        queued = {str(item[0]) for item in self._redeem_q if item}
        queued.update(getattr(self, '_redeem_inflight', set()))
        held = {}
        pages = 0
        async for page in self.executor.c.list_positions():
            pages += 1
            for position in page.items:
                shares = float(getattr(position, 'size', None)
                               or getattr(position, 'shares', 0) or 0)
                if math.isfinite(shares) and shares > 0:
                    held[str(getattr(position, 'token_id', ''))] = shares
        self._redeem_recovery_health.update(position_pages=pages, held_tokens=len(held))
        for r in rows:
            shares = held.get(str(r['token_id']), 0)
            if not shares:
                continue
            try:
                mt = await self.market_tokens(int(r['window_anchor']))
            except Exception as exc:
                _err('redeem_recovery.market', exc)
                continue
            cid = str(mt.get('condition_id')) if mt and mt.get('condition_id') else None
            # Queue consumption can proceed while this read-only task awaits HTTP.
            current = {str(item[0]) for item in self._redeem_q if item}
            if (not cid or cid in queued or cid in current
                    or cid in getattr(self, '_redeem_inflight', set())):
                continue
            self._redeem_q.append((cid, r['slug'], 0, 0.0))
            queued.add(cid)
            print(f"[{datetime.now(timezone.utc):%H:%M:%S}] REDEEM-RECOVER "
                  f"{r['slug']} {shares:.2f}sh still held", flush=True)

    # ----- decision -----
    def size_position(self, tier_acc, ask, book="t3", book_gated=False,
                      shadow_challenger=False):
        """FIXED-FRACTION sizing (2026-07-13 model):
          • T+3 = T3_STAKE_FRAC (25%) of the COMPOUNDING t3 bankroll
            (self.bankroll_total), breakeven-gated on tier_acc vs ask.
          • FADE = FADE_STAKE_FRAC (12.5%) of the fade book's FIXED base
            (self.fade_base = fade_capital(), NON-compounding — realized P&L is
            tracked but never reinvested); no breakeven gate.
          • Single-book (legacy): STAKE_FRAC of self.bankroll.
        The EXACT fraction is honoured — stakes are NOT inflated up to a 5-share
        ticket; a stake below MIN_ORDER_USD is skipped (0,0) rather than rounded up.
        Returns (shares, stake_usd) or (0,0)."""
        if ask is None or ask <= 0 or ask >= 1:
            return 0.0, 0.0
        if shadow_challenger and book == "t3":
            # Flat $1 model ticket: sub-dollar rows round both their profit and
            # loss to $0.00 on the card and one transient zero bankroll even
            # produced a bogus zero-ticket "fill".  Shadow has no exchange/wallet
            # constraint, so keep its measurement notional deterministic.  This
            # branch never funds or authorizes a real order.
            stake_usd = max(0.0, BTC_T3_SHADOW_STAKE_USD)
            if stake_usd <= 0:
                return 0.0, 0.0
            return stake_usd / ask, stake_usd
        # T3 BANK BOOK sizing (PM_T3_BANKBOOK_FILTERS.md §3), used when the entry
        # came through t3_book_ok(). stake = clamp(k x bank, $1 floor, ceiling).
        #   • k = 20%: the fixed-fraction sweep peaks near 28%, but past 20% the
        #     drawdown climbs faster than the return (33% earns LESS than 25%).
        #   • $1 floor: PM's minimum order. 20% of a $1 bank is $0.20 and would
        #     never be sendable, so the book bets a flat $1 until its own bank
        #     clears $5 and the fraction takes over. Same shape as
        #     SWINGFADE_MIN_STAKE — a floor, not a target.
        #   • $50 ceiling: a DEPTH limit, not a risk preference. Median top of book
        #     is ~122sh ≈ $85 at ask 0.70; only 64% of windows fill $50 whole.
        # Bank is self.bankroll_total, the t3 book's own compounding pool — separate
        # from the swingfade/oppfade daily capital even though both spend the same
        # PM wallet. Deliberately NOT breakeven-gated on tier_acc: Stage 2's per-tier
        # be_wr caps already did that, and the legacy test would veto the whole book.
        if book_gated and book == "t3":
            # NO bank-affordability test, deliberately: T3_CAPITAL_SHARE is 0.0, so
            # bankroll_total starts at $0 and IS the book's P&L. Gating the $1 floor
            # on the bank covering it would mean the book could never place its first
            # bet. The books are separately ACCOUNTED but share one PM wallet (
            # 2026-07-29), so the real constraint is the wallet, not the book share.
            # The arithmetic lives in t3_book_stake_for() so the dash publishes the
            # number we actually bet instead of recomputing the formula inline.
            if t3_compound_on():
                stake_usd = self.t3_compound_stake_for()
            else:
                stake_usd = crossbook_stake_for("t3", self._live_usdc)
                if stake_usd is None:
                    stake_usd = t3_book_stake_for(self.bankroll_total, self._live_usdc)
            if stake_usd < MIN_ORDER_USD:
                return 0.0, 0.0                  # under PM's floor — unsendable
            return stake_usd / ask, stake_usd
        if TWO_BOOK and book == "hfade":
            bankroll, frac = max(0.0, self.fade_base), HFADE_STAKE_FRAC
        elif TWO_BOOK and book == "fade":
            bankroll, frac = max(0.0, self.fade_base), FADE_STAKE_FRAC
        elif TWO_BOOK:
            if tier_acc - MIN_EDGE_MARGIN <= ask:
                return 0.0, 0.0
            bankroll, frac = max(0.0, self.bankroll_total), T3_STAKE_FRAC
        else:
            if tier_acc - MIN_EDGE_MARGIN <= ask:
                return 0.0, 0.0
            bankroll, frac = max(0.0, self.bankroll), STAKE_FRAC
        stake_usd = frac * bankroll                  # EXACT fraction (no min-ticket inflation)
        # absolute per-trade cap (thin-book slippage guard)
        cap_usd = (T3_MAX_STAKE_USD if book == "t3"
                   else FADE_MAX_STAKE_USD if book in ("fade", "hfade") else 0.0)
        if cap_usd and stake_usd > cap_usd:
            stake_usd = cap_usd
        if stake_usd > bankroll + 1e-9:
            return 0.0, 0.0                          # can't afford it
        if stake_usd < MIN_ORDER_USD:
            return 0.0, 0.0                          # below PM order floor — skip, don't inflate
        return stake_usd / ask, stake_usd

    # _fade_binance_confirms() DELETED 2026-07-30. It required the leading Binance
    # momentum to be turning against the T+3 side by >= the surest-only bar, and it
    # was the only thing lifting P(reversal) above the market-implied fade price.
    # Nothing Chainlink-native replaces it yet, so the fade books keep no confirmation
    # signal at all — they remain SHADOW and must not be armed live on this basis.

    def _fade_daily_ok(self):
        """Rolling 24h cap: fire only the FADE_MAX_PER_DAY surest reversals/day, not
        blindly on every confirmation. DB-backed (fade+hfade legs decided in the last
        24h) so it survives restarts. Fails OPEN on a DB hiccup — the strong-confirm
        gate + reachable band are the real filters; the cap is a secondary safety."""
        if FADE_MAX_PER_DAY <= 0:
            return False
        try:
            with ENGINE.connect() as c:
                n = int(c.execute(text(
                    f"SELECT COUNT(*) FROM {TRADES_TABLE} WHERE dry_run=:d "
                    "AND book IN ('fade','hfade') AND staked=true "
                    "AND decided_at >= NOW() - INTERVAL '24 hours'"),
                    {"d": DRY_RUN}).scalar() or 0)
            return n < FADE_MAX_PER_DAY
        except Exception:
            return True

    def _reversal_pred(self, token, bid, ask):
        """Predict whether a resting maker BUY on our side would FILL over the
        reprice ladder = price coming down to us = reversal. None if no feed."""
        if self.bookfeed is None or not GATE_ANY:
            return None
        try:
            est = self.bookfeed.fillable_estimate(
                token, bid, ask, "BUY", MAKER_REPRICE_S, MAKER_MAX_REPRICES,
                VOL_WINDOW_S)
        except Exception:
            return None
        if est is None:
            return None
        fillable = est[0]
        return fillable >= MIN_SHARES

    async def _try_high_fade(self, w, now, p):
        """EXPERIMENTAL ~9:1 fade: on an EXPENSIVE-ask reversal (our side ~0.90 that
        the T+3 gates skip — MODERATE / high-ask STRONG), buy the CHEAP opposite
        (<= HFADE_MAX_OPP_ASK) flat-small, held to settle. Fires only when the
        reversal probe predicts our expensive side would fill (price falling)."""
        if w.get("open_leg") is not None or w.get("faded"):
            return                       # one fade per window — don't re-fade
        if not self._fade_daily_ok():
            return                       # 24h fade cap reached — surest-only
        side = p.get("side")
        our_token = p.get("token") or p.get("mod_token")
        our_bid = p.get("best_bid") if p.get("token") else p.get("mod_bid")
        our_ask = p.get("best_ask") if p.get("token") else p.get("mod_ask")
        if side is None or our_token is None or our_bid is None or our_ask is None:
            return
        # our side must be expensive enough that the opposite is cheap (~9:1 zone)
        if our_ask < 1 - HFADE_MAX_OPP_ASK - 0.01:
            return
        # ADX floor (2026-07-13 loser analysis): only fade a STRONG move
        if (w.get("adx_short") or 0.0) < FADE_MIN_ADX_SHORT:
            return
        if self._reversal_pred(our_token, our_bid, our_ask) is not True:
            return
        # The Binance-lead confirmation gate that used to sit here is gone (2026-07-30).
        # NOTE this DID enforce in this deployment: the service unit set
        # POLYMARKET_FADE_REQUIRE_BINANCE=1, so an un-confirmed hfade was skipped.
        # Removing it therefore makes hfade fire MORE often, with no confirmation at
        # all. Tolerable only because fade/hfade are shadow at 0% capital share
        # (POLYMARKET_FADE_CAPITAL_SHARE=0.0) — they must not be armed live until a
        # Chainlink-native confirmation is built and validated.
        opp = w["down_token"] if side == "Up" else w["up_token"]
        fade_side = "Down" if side == "Up" else "Up"
        try:
            ob, oa = await self.book_levels(int(opp))
        except Exception:
            return
        if not oa or not ob:
            return
        opp_ask, opp_bid = oa[0][0], ob[0][0]
        # only the REACHABLE band: too-cheap (deep-OTM) reversals never happen
        if not (HFADE_MIN_OPP_ASK <= opp_ask <= HFADE_MAX_OPP_ASK) or opp_ask - opp_bid > MAX_SPREAD:
            return
        fsh, _ = self.size_position(None, opp_ask, book="hfade")
        favg, ffilled = walk_fill(oa, fsh)
        if favg is None or ffilled < MIN_SHARES:
            return
        if self.live and book_stakes_now("fade", not FADE_SHADOW,
                                         w["window_start_utc"]) and \
                three_source_consensus_ok(
                    w["window_anchor"], fade_side)[0]:  # else model only
            a2, s2, ok, err = await self.executor.buy(opp, ffilled * favg, opp_ask)
            if not ok:
                return
            if a2 is not None and s2 > 0:
                favg, ffilled = a2, s2
        idx = len(w["legs"])
        leg = self._new_leg(w, idx, is_t3=False)
        await _offload(self._stamp_signal, leg, p)
        leg["predicted_side"] = fade_side
        leg["token_id"] = str(opp)
        leg["entry_price"] = round(favg, 4)
        leg["shares"] = round(ffilled, 2)
        leg["stake_usd"] = round(ffilled * favg, 2)
        leg["staked"] = True
        leg["status"] = "OPEN"
        leg["book"] = "hfade"
        leg["_fade_maker"] = False
        w["legs"].append(leg)
        w["open_leg"] = leg              # held; manage() skips hfade
        w["faded"] = True                # one fade per window
        w["staked_count"] += 1
        print(f"[{datetime.now(timezone.utc):%H:%M:%S}] HFADE {w['slug']} {fade_side}"
              f" vs {p.get('label')} {side} (our ask {our_ask:.2f}) -> {ffilled:.1f}sh"
              f" @ {favg:.3f} (~{1/favg:.0f}:1) {'LIVE' if self.live else 'DRYRUN'}",
              flush=True)
        upsert(leg)

    async def plan_entry(self, w, now, is_t3=True):
        """Evaluate the current continuation signal + all entry gates. Returns a
        dict with the signal and (ok, reason) plus fill details when it
        qualifies. Never places an order — pure decision."""
        p = {"ok": False, "reason": None, "side": None, "label": None,
             "acc": None, "dbps": None, "token": None, "best_ask": None,
             "best_bid": None, "shares": None, "avg": None, "price_now": None,
             "maker_fillable": None, "queue_ahead": None, "vol_rate": None,
             "gate_would_block": None}
        try:
            price_now = await _offload(underlying_price_now)
        except Exception as e:
            _err("price.now", e)
            p["reason"] = "no price"
            return p
        if price_now is None:
            # Chainlink-only since 2026-07-30: no price means no decision. There is
            # no second feed to reach for, so the window simply does not trade.
            p["reason"] = "no chainlink price"
            return p
        dbps = (price_now - w["price_open"]) / w["price_open"] * 1e4
        label, acc = tier_for(abs(dbps))
        side = "Up" if dbps > 0 else "Down"
        adx = w.get("adx")
        p.update({"dbps": round(dbps, 3), "label": label, "acc": acc,
                  "side": side, "price_now": price_now})
        # The Binance-early direction (Binance's own open->now delta, logged beside
        # the Chainlink call to measure the look-ahead edge) is REMOVED 2026-07-30.
        # It was measurement scaffolding: DECISION_FEED could have been switched to it,
        # never was, and the measurement it fed is what refuted the idea.

        # --- DYNAMIC-CLASSIFICATION features (log-only, computed for EVERY tier
        # incl no-stake) — to later replace the static bps->tier->stake map.
        vol3 = w.get("vol_3min_bps")
        p["vol_3min_bps"] = vol3
        p["move_z"] = round(dbps / vol3, 3) if vol3 else None   # move size vs regime
        try:
            # 6s momentum on the SETTLEMENT feed (was Binance 1s klines until
            # 2026-07-30). velocity_frac gates t3_book_ok, so this had to move
            # feeds rather than be deleted — see chainlink_recent_momentum().
            _cmd, _cmb = await _offload(chainlink_recent_momentum)   # last ~6s (cached)
            p["velocity_frac"] = (round(_cmb / dbps, 3)
                                  if (_cmb is not None and dbps) else None)
        except Exception:
            p["velocity_frac"] = None
        # LATEST RAW TICK vs open (signed bps, like dbps). Feeds the T3 last-tick
        # gate: the TWAP move lags ~30s, and 08-23..09-28 in-band T3 went 35% WR
        # (n=17) when this had already crossed back through the open vs 74% (n=42).
        _lt_px = await _offload(latest_tick_price)
        p["last_tick_bps"] = (round((_lt_px - w["price_open"]) / w["price_open"] * 1e4, 3)
                              if _lt_px is not None else None)
        # both-side token asks from the WS book cache (value / R:R) — cheap, no REST
        p["ask_up"] = self._cached_best_ask(w.get("up_token"))
        p["ask_down"] = self._cached_best_ask(w.get("down_token"))
        # SELL side (bid) too — needed to measure exit/cut price + true spread
        p["bid_up"] = self._cached_best_bid(w.get("up_token"))
        p["bid_down"] = self._cached_best_bid(w.get("down_token"))
        # BOTH-SIDE TAKER DEPTH at each side's own ask (2026-07-28). Computed here,
        # above every early return, so it lands on EVERY eval leg — no-stake ones
        # included. That is the whole point: the flip−opp fill delta used to exist
        # only where a flip was detected and logged to re_flip_shadow, so the t3/RE
        # rows that make up most of the window table had no pair to subtract and
        # rendered blank. Same WS-cache-only cost as the ask/bid reads above.
        p["touch_up_sh"], p["fill_up_sh"], p["ladder_up"] = (
            self._cached_sweep_depth(w.get("up_token"), p["ask_up"]))
        p["touch_down_sh"], p["fill_down_sh"], p["ladder_down"] = (
            self._cached_sweep_depth(w.get("down_token"), p["ask_down"]))

        # T3 BANK BOOK gate (2026-07-29). Evaluated here off the WS-cached ask so it
        # can override should_stake()'s tier veto — the book stakes WEAK and MODERATE,
        # which NO_STAKE_TIERS refuses and which carry 77% of its weekday profit. This
        # is a PROVISIONAL read: the definitive check re-runs below on the REAL best
        # ask, because the fill price is what the ask caps and the dead band are about.
        _t3b_ask = (p.get("ask_up") if p.get("side") == "Up" else p.get("ask_down"))
        _t3_shadow = btc_t3_shadow_challenger_active()
        p["t3_shadow_challenger"] = _t3_shadow
        if T3_BOOK_ON:
            _t3b_ok, _t3b_why = t3_book_ok(
                window_start_utc=w.get("window_start_utc"), tier=label,
                ask_pred=_t3b_ask, adx=adx, velocity_frac=p.get("velocity_frac"),
                decision_bps=dbps, is_t3=is_t3,
                vol_3min_bps=p.get("vol_3min_bps"),
                shadow_challenger=_t3_shadow)
        else:
            _t3b_ok, _t3b_why = False, "t3 book off"
        p["t3_book_ok"], p["t3_book_why"] = _t3b_ok, _t3b_why
        # A challenger decision may arrive while the WS cache has no ask yet.
        # Do not let that provisional miss become the final verdict: the REST
        # book refresh below is the authoritative quote for this shadow cohort.
        _t3_shadow_tier = (_t3_shadow and is_t3
                           and label in BTC_T3_SHADOW_TIERS)
        if (not _t3b_ok and not should_stake(label, adx)
                and not _t3_shadow_tier):
            is_no_stake_tier = (label is None or label in NO_STAKE_TIERS)
            # MODERATE is excluded on R:R (86% direction acc but small move ->
            # high token price). Fetch the book once (REST) to stash the fade-
            # probe inputs so try_enter can shadow the fade: rest a maker BUY on
            # our predicted side; if it FILLS (sellers hitting us = T+3 call
            # failing) we'd fade the cheap opposite. Reuses the existing probe.
            if label == "MODERATE":
                try:
                    tok = w["up_token"] if side == "Up" else w["down_token"]
                    b, a = await self.book_levels(tok)
                    if a and b:
                        p["mod_token"] = tok
                        p["mod_bid"] = b[0][0]
                        p["mod_ask"] = a[0][0]
                except Exception:
                    pass
            if is_no_stake_tier:
                # NOTE the market data that actually governs whether this (e.g.
                # RE-flip) leg is stakeable — the ask we'd pay + the queue ahead
                # of a maker rest — NOT the tier, which is already its own column
                # (restating it here wrongly reads as "skipped because of tier").
                tok = w["up_token"] if side == "Up" else w["down_token"]
                ask_ps, q_ps = self._cached_ask_queue(tok)
                if q_ps is not None:
                    p["queue_ahead"] = q_ps
                p["reason"] = (
                    (f"ask {ask_ps:.2f}" if ask_ps is not None else "ask —")
                    + " · " +
                    (f"queue {q_ps:.0f}sh" if q_ps is not None else "queue —"))
            else:
                p["reason"] = "unfavourable ADX"
            return p
        token = w["up_token"] if side == "Up" else w["down_token"]
        p["token"] = token
        try:
            bids, asks = await self.book_levels(token)
        except Exception:
            bids, asks = [], []
        # The challenger is measurement-only and buys nothing.  A visible ask is
        # sufficient to model its ticket even when the complementary bid side is
        # empty; production routes still require a two-sided book.
        if not asks or (not bids and not _t3_shadow):
            p["reason"] = "one-sided/empty book"
            return p
        best_ask = asks[0][0]
        best_bid = bids[0][0] if bids else None
        p["best_ask"] = best_ask
        p["best_bid"] = best_bid
        if (not _t3_shadow and
                (best_ask - best_bid > MAX_SPREAD
                 or not (MIN_ASK <= best_ask <= MAX_ASK))):
            p["reason"] = (f"illiquid book (bid {best_bid:.2f}/ask "
                           f"{best_ask:.2f}, spread {best_ask-best_bid:.2f})")
            return p
        if _t3_shadow and not (0.0 < best_ask < 1.0):
            p["reason"] = f"invalid shadow ask ({best_ask!r})"
            return p
        # T3 ask ceiling is an execution invariant, not merely a signal-time
        # quote filter. 0.80 used to let FAK orders walk into the 0.90 book
        # that does not fill at the quote. Apply T3_ASK_CEIL (0.68) before both
        # the bank-book and leftover legacy branches.
        _t3_shadow = bool(p.get("t3_shadow_challenger"))
        _t3_hard_ceil = (T3_ASK_CEIL if T3_ASK_CEIL is not None else MAX_ENTRY_ASK)
        if best_ask > _t3_hard_ceil and not _t3_shadow:
            p["reason"] = (f"ask too high for hard T3 cap ({best_ask:.2f} > "
                           f"{_t3_hard_ceil:.2f})")
            return p
        # A falling continuation-token ask between the cached decision and the
        # executable REST touch is signal invalidation, not beneficial slippage.
        # Post-boundary live audit: drops >5c were 0/6; retain as an execution
        # safety gate while the frozen directional rule runs at $1 probe size.
        if (T3_FIRST_PRINCIPLES and not _t3_shadow
                and not t3_live_ask_ok(_t3b_ask, best_ask)):
            _signal_label = (f"{_t3b_ask:.2f}" if isinstance(_t3b_ask, (int, float))
                             else repr(_t3b_ask))
            p["reason"] = (f"T3 signal invalidated: ask {_signal_label}->"
                           f"{best_ask:.2f}; max drop {T3_MAX_ASK_DROP:.2f}")
            return p
        # DEFINITIVE t3-book check, on the REAL best ask rather than the WS-cached
        # one the tier bypass above used. The hard 0.80 execution ceiling above is
        # common to every T3 route. When this gate passes it replaces the remaining
        # two legacy price gates below, which contradict the measured book:
        #   • MIN_ENTRY_ASK 0.72  — WEAK is specified to ≤0.75, and the best zone in
        #                           the file is [0.65,0.72), entirely under this floor
        #   • tier_acc breakeven  — superseded by the per-tier be_wr caps in Stage 2
        # These legacy gates are why the t3 book staked ZERO times in the 10 days to 07-29
        # while logging 1,611 eval legs. The liquidity/spread/depth gates above and
        # below are NOT bypassed — they are execution safety, not selection.
        if T3_BOOK_ON:
            # ONE T3 gate. Previously STRONG/EXTREME that failed t3_book_ok
            # (vol, calendar, ADX) fell through should_stake() into the legacy
            # MIN_ENTRY_ASK=0.72 path — which skipped the cheap tickets and
            # still allowed 0.80-0.90 via MAX_ENTRY_ASK. No T3 route may now
            # stake a cohort t3_book_ok refused.
            _ok2, _why2 = t3_book_ok(
                window_start_utc=w.get("window_start_utc"), tier=label,
                ask_pred=best_ask, adx=adx, velocity_frac=p.get("velocity_frac"),
                decision_bps=dbps, is_t3=is_t3,
                vol_3min_bps=p.get("vol_3min_bps"),
                shadow_challenger=_t3_shadow)
            p["t3_book_ok"], p["t3_book_why"] = _ok2, _why2
            if not _ok2:
                p["reason"] = f"t3 book: {_why2}"
                return p
            # LAST-TICK GATE (2026-09-28): the latest raw tick must still be on the
            # call's side of the open by > T3_LASTTICK_MIN_BPS. SHADOW unless config
            # `t3_lasttick_gate_live`=1 (read per decision, no restart); fail-closed
            # on a missing tick only when live. Every row carries last_tick_bps.
            if is_t3 and not _t3_shadow:
                _lt = p.get("last_tick_bps")
                _lt_ok = (_lt is not None
                          and _lt * (1 if side == "Up" else -1) > T3_LASTTICK_MIN_BPS)
                if not _lt_ok:
                    _lt_live = str(await _offload(
                        get_config, "t3_lasttick_gate_live", "0")).strip() == "1"
                    _lt_why = (f"latest tick {_lt if _lt is not None else '—'}bps vs "
                               f"{side} call {dbps:+.2f}bps (TWAP)")
                    if _lt_live:
                        p["reason"] = f"t3 last-tick gate: {_lt_why}"
                        return p
                    print(f"[{datetime.now(timezone.utc):%H:%M:%S}] T3-LASTTICK-SHADOW "
                          f"{w['slug']} would block: {_lt_why}", flush=True)
            shares, stake_usd = self.size_position(
                acc, best_ask, book="t3", book_gated=True,
                shadow_challenger=_t3_shadow)
            if shares <= 0:
                p["reason"] = f"t3 book size floor (ask {best_ask:.2f})"
                return p
        else:
            # LEGACY FALLBACK — only when the bank book is compiled out.
            _legacy_ask_cap = MAX_ENTRY_ASK
            if T3_ASK_CEIL is not None:
                _legacy_ask_cap = min(_legacy_ask_cap, T3_ASK_CEIL)
            if T3_BTC_ASK_CEIL is not None:
                _legacy_ask_cap = min(_legacy_ask_cap, T3_BTC_ASK_CEIL)
            if best_ask > _legacy_ask_cap:
                p["reason"] = (f"ask too high for R:R ({best_ask:.2f} > cap "
                               f"{_legacy_ask_cap:.2f}): risk a lot to make a little")
                return p
            if MIN_ENTRY_ASK > 0 and best_ask < MIN_ENTRY_ASK:
                p["reason"] = (f"ask too cheap ({best_ask:.2f} < floor "
                               f"{MIN_ENTRY_ASK:.2f}): market disagrees, tier miscalibrated")
                return p
            shares, stake_usd = self.size_position(acc, best_ask)
            if shares <= 0:
                p["reason"] = f"breakeven/size floor (ask {best_ask:.2f} vs acc {acc:.2f})"
                return p
        # Model the live FAK as a DOLLAR sweep of the real ask ladder and judge it
        # on the fraction of the TICKET filled. See T3_PAYUP for why the old
        # `filled < MIN_SHARES` test was an ask<=0.20 ceiling rather than a depth
        # check. `t3_cap` is BOTH the modelled limit and the limit the order
        # actually carries downstream — they were allowed to diverge before (the
        # model priced the touch, the FAK was free to sweep to 0.80), and all three
        # live t3 fills came in worse than modelled because of it: 0.20->0.24,
        # 0.15->0.26 and 0.19->0.59, the last turning a $1/5.26sh ticket into
        # 1.69sh. Every input and output here is stamped onto the row (staked or
        # not) by _stamp_signal, so a refusal is diagnosable from a SELECT.
        _t3_exec_ceiling = 0.99 if _t3_shadow else (
            T3_ASK_CEIL if T3_ASK_CEIL is not None else MAX_ENTRY_ASK)
        t3_cap = min(best_ask + T3_PAYUP, _t3_exec_ceiling)
        # Fillable at THIS ask, not at 0.90. Quote-only sweep must cover the
        # ticket before we allow a +1c pay-up on the live FAK.
        if not _t3_shadow:
            _q_ok, _q_frac = t3_ask_fillable(asks, stake_usd, best_ask)
            p["t3_quote_fill_frac"] = _q_frac
            if not _q_ok:
                p["reason"] = (
                    f"ask not fillable at quote ({_q_frac:.0%} of "
                    f"${stake_usd:.2f} at {best_ask:.2f}; need "
                    f"{T3_MIN_FILL_FRAC:.0%} with no pay-up)")
                return p
        filled, sweep_usd, avg, fill_frac = sweep_ladder(asks, stake_usd, t3_cap)
        p.update({
            "t3_ticket_usd": round(stake_usd, 4),
            "t3_target_sh": round(stake_usd / best_ask, 4) if best_ask else None,
            "t3_cap_px": round(t3_cap, 4),
            "t3_sweep_sh": filled,
            "t3_sweep_usd": sweep_usd,
            "t3_sweep_avg": round(avg, 6) if avg is not None else None,
            "t3_fill_frac": fill_frac,
            # depth at each rung of the policy ladder, so "how much worse would a
            # bigger ticket have been" is answerable off the row without re-parsing
            # ask_ladder. Quoted ask / +1c / the hard 0.80 ceiling.
            "t3_depth_quote_sh": round(
                sum(s for px, s in asks if px <= best_ask + 1e-9), 4),
            "t3_depth_1c_sh": round(
                sum(s for px, s in asks if px <= best_ask + T3_PAYUP + 1e-9), 4),
            "t3_depth_cap_sh": round(
                sum(s for px, s in asks if px <= _t3_exec_ceiling + 1e-9), 4),
            "t3_model_slip_bps": (round((avg - best_ask) / best_ask * 1e4, 3)
                                  if avg is not None and best_ask else None),
        })
        if avg is None or fill_frac < T3_MIN_FILL_FRAC:
            # Say WHICH constraint bound. The old message named the book when the
            # book was almost never the problem, and cost ~70x the book's frequency
            # before anyone could see that from the reason text alone.
            p["reason"] = (f"insufficient book depth ({fill_frac:.0%} of "
                           f"${stake_usd:.2f} fillable to {t3_cap:.2f}; "
                           f"{p['t3_depth_quote_sh']:.1f}sh at quote vs "
                           f"{p['t3_target_sh']:.2f}sh wanted)")
            return p
        # Maker fillability gate (queue depth + trade volume across the full
        # reprice ladder): entry is a BUY resting at the bid then stepping toward
        # mid. In ENFORCE mode, skip if even the min ticket can't fill and cap the
        # stake to fillable volume; in SHADOW mode, only record the prediction +
        # would-block decision. Degrades gracefully: est is None (feed cold / thin
        # tape) -> no gate, no telemetry.
        if (best_bid is not None and (MAKER_MODE or TWO_BOOK)
                and GATE_ANY and self.bookfeed is not None):
            est = self.bookfeed.fillable_estimate(
                token, best_bid, best_ask, "BUY",
                MAKER_REPRICE_S, MAKER_MAX_REPRICES, VOL_WINDOW_S)
            if est is not None:
                fillable, qa, rate, _rungs = est
                would_block = fillable < MIN_SHARES
                p["maker_fillable"] = round(fillable, 1)
                p["queue_ahead"] = round(qa, 1)
                p["vol_rate"] = round(rate, 2)
                p["gate_would_block"] = would_block
                # REVERSAL signal for TWO_BOOK routing: a resting maker BUY that
                # WOULD fill = price coming down to us = T+3 call failing = reversal
                # (would_block False). would_block True = momentum (runs away).
                p["reversal"] = (would_block is False)
                if MAKER_MODE and GATE_ENFORCE:
                    if would_block:
                        p["reason"] = (f"maker-unfillable (queue {qa:.0f}sh, "
                                       f"{rate:.2f}sh/s, est {fillable:.0f}sh ladder)")
                        return p
                    if fillable < filled:
                        filled = fillable        # cap stake to fillable volume
        # T3 CALENDAR VETO — last thing before "ok", so every t3-family route
        # (bank book AND the legacy MAX/MIN_ENTRY_ASK path below it) is covered by
        # ONE check. It has to land HERE and not at the order call: the entry sites
        # downstream record `staked=True` with a MODELLED fill when they don't send
        # an order, and those rows carry dry_run=false, so bankroll_realised()
        # (`dry_run=false AND staked=true`) would book phantom P&L for trades we
        # deliberately did not place. A NO_STAKE row with a reason is both honest
        # and queryable; a modelled "staked" row is neither.
        _dow_ok, _dow_why = dow_stake_ok(w.get("window_start_utc"))
        if not _dow_ok and not _t3_shadow:
            # Distinct from every other refusal on purpose: the selection gates all
            # passed and only the calendar refused. Without its own flag the forward
            # read cannot separate "no candidate" from "candidate, blocked by day",
            # which is the only number that can ever retire this rule.
            p["dow_blocked"] = True
            p["reason"] = f"calendar: {_dow_why}"
            return p
        p.update({"ok": True, "shares": filled, "avg": avg})
        return p

    def _stamp_signal(self, leg, p):
        leg["decision_bps"] = p["dbps"]
        leg["predicted_side"] = p["side"]
        leg["tier"] = p["label"]
        leg["tier_acc"] = p["acc"]
        leg["price_decision"] = p.get("price_now")
        # Binance-early (leading-feed) direction for look-ahead measurement
        # decision_bps_binance / predicted_side_binance / tier_binance are no longer
        # written (2026-07-30). Historical values stay in the table.
        # maker fillability telemetry (queue depth + trade volume)
        leg["maker_fillable"] = p.get("maker_fillable")
        leg["queue_ahead"] = p.get("queue_ahead")
        leg["vol_rate"] = p.get("vol_rate")
        leg["gate_would_block"] = p.get("gate_would_block")
        # dynamic-classification features (regime-normalized move, velocity, value)
        leg["move_z"] = p.get("move_z")
        leg["vol_3min_bps"] = p.get("vol_3min_bps")
        leg["velocity_frac"] = p.get("velocity_frac")
        leg["last_tick_bps"] = p.get("last_tick_bps")
        leg["ask_up"] = p.get("ask_up")
        leg["ask_down"] = p.get("ask_down")
        # FLIP / OPP ORIENTATION (2026-07-28). "flip" = the side the move is calling
        # right now (p["side"]); "opp" = the other one — the same orientation
        # re_flip_shadow and the oppfade provenance already use, so the columns stay
        # joinable and the Fill Δ (flip − opp taker depth) means one thing everywhere.
        # NOTE the buy side differs by book: t3/RE buys `flip`, fade/oppfade/swingfade
        # buys `opp`. Both asks are stamped so either book can be read off the row.
        # The oppfade/swingfade paths re-stamp ask_opp/fillable_opp_sh/touch_opp_sh
        # later with their own decision-time snapshot; that write wins by design.
        _s = p.get("side")
        if _s in ("Up", "Down"):
            _up = (_s == "Up")
            leg["ask_flip"] = p.get("ask_up") if _up else p.get("ask_down")
            leg["ask_opp"] = p.get("ask_down") if _up else p.get("ask_up")
            leg["fillable_sh"] = p.get("fill_up_sh") if _up else p.get("fill_down_sh")
            leg["fillable_opp_sh"] = (p.get("fill_down_sh") if _up
                                      else p.get("fill_up_sh"))
            leg["touch_sh"] = p.get("touch_up_sh") if _up else p.get("touch_down_sh")
            leg["touch_opp_sh"] = (p.get("touch_down_sh") if _up
                                   else p.get("touch_up_sh"))
            leg["ask_ladder"] = p.get("ladder_up") if _up else p.get("ladder_down")
            leg["ask_ladder_opp"] = (p.get("ladder_down") if _up
                                     else p.get("ladder_up"))
        # T3 EXECUTION PROVENANCE (2026-08-06). Stamped on EVERY leg plan_entry got
        # far enough to price — STAKED OR NOT — because the whole point is that a
        # refusal must be diagnosable without a replay. The old "insufficient book
        # depth" row recorded nothing about the ticket, so for three weeks it was
        # impossible to tell from the table that the book was rejecting its own
        # 1.5-share order against a 122-share touch. Ticket, target, cap, sweep,
        # fill fraction and depth at three price points are now all columns.
        for _k in ("t3_ticket_usd", "t3_target_sh", "t3_cap_px", "t3_sweep_sh",
                   "t3_sweep_usd", "t3_sweep_avg", "t3_fill_frac",
                   "t3_depth_quote_sh", "t3_depth_1c_sh", "t3_depth_cap_sh",
                   "t3_model_slip_bps"):
            leg[_k] = p.get(_k)
        # The per-leg bin_mom_bps / fade_confirmed stamp (added 2026-07-14 so the
        # fade selectivity gate could be replayed over un-traded windows) is REMOVED
        # 2026-07-30 along with the Binance lead it measured. Both columns are left
        # in the table holding their historical values; nothing writes them now.

    def _cached_best_ask(self, token):
        """Best ask from the WS book cache only (no REST) — cheap, for logging."""
        if token is None or self.bookfeed is None:
            return None
        try:
            lv = self.bookfeed.levels(token)
            if lv:
                _bids, asks = lv
                if asks:
                    return round(asks[0][0], 3)
        except Exception:
            pass
        return None

    def _cached_ask_depth(self, token):
        """(touch_sh, fillable_sh) on the ASK side from the WS book cache only.

        touch_sh   = size resting AT the best ask.
        fillable_sh= total shares resting at asks <= OPPFADE_ASK_MAX, i.e. what the
                     cheap-opp fade's FAK could actually take at its price cap.
        This is the number that answers the book's recurring "no orders found to
        match FAK" deaths (a fat quote with nothing resting inside the cap), so it
        is surfaced live on the chart legend. See [[project_pm_flip_fak_fill_failures]]."""
        if token is None or self.bookfeed is None:
            return None, None
        try:
            lv = self.bookfeed.levels(token)
            if lv:
                _bids, asks = lv
                if asks:
                    touch = round(float(asks[0][1]), 2)
                    fill = round(sum(float(s) for p, s in asks
                                     if float(p) <= OPPFADE_ASK_MAX), 2)
                    return touch, fill
        except Exception:
            pass
        return None, None

    def _cached_sweep_depth(self, token, ask):
        """(touch_sh, fillable_sh, ask_ladder) a FAK taker could sweep off `ask`.

        The mirror of the depth `_maybe_log_reflip` computes for re_flip_shadow, but
        priced off THIS leg's own ask instead of the oppfade price cap — so it means
        the same thing on a 0.9 t3 token as on a 0.16 opp token: "what could we have
        actually bought at the price we'd have paid". Extracted so EVERY eval leg can
        carry it, not just the windows where a flip happened to be detected: before
        this, `fillable_sh`/`fillable_opp_sh` existed only on re_flip_shadow rows, and
        the dashboard's Fill Δ was blank on every t3/RE row for want of the pair
        (2026-07-28). WS cache only, try/excepted — pure telemetry, must never
        disturb the decision path or block the loop on a REST fetch.

        `fillable_sh` is summed at ask + BUY_SLIP (3c), which is the EXECUTION cap and
        NOT the cap any pay-up model wants: PM_T3_BANKBOOK_FILTERS.md §4 caps the
        pay-up at +1 CENT, and on the 275 rows logged so far fillable_sh averages
        2907sh against 642sh at the touch — a 4.5x spread that cannot be interpolated
        down to +1c. So also stamp the raw ladder out to LADDER_OBS_SLIP (the same
        observation-only cap `opp_ask_ladder` already uses), which makes depth at ANY
        cent cap a measurement instead of an assumption, retroactively and without
        another schema change when the cap is retuned (2026-07-29)."""
        if token is None or ask is None or self.bookfeed is None:
            return None, None, None
        try:
            lv = self.bookfeed.levels(token)
            if lv:
                _bids, asks = lv
                if asks:
                    cap = ask + BUY_SLIP + 1e-9
                    obs_cap = ask + LADDER_OBS_SLIP + 1e-9
                    rungs = sorted(((float(px), float(sz)) for px, sz in asks
                                    if float(px) <= obs_cap), key=lambda t: t[0])
                    touch = round(float(asks[0][1]), 2)
                    fill = round(sum(sz for px, sz in rungs if px <= cap), 2)
                    ladder = (json.dumps([[round(px, 4), round(sz, 2)]
                                          for px, sz in rungs]) if rungs else None)
                    return touch, fill, ladder
        except Exception:
            pass
        return None, None, None

    def _cached_best_bid(self, token):
        """Best bid from the WS book cache only (no REST) — the price you'd SELL
        the token at. Mirrors _cached_best_ask; used for two-sided open-book logging."""
        if token is None or self.bookfeed is None:
            return None
        try:
            lv = self.bookfeed.levels(token)
            if lv:
                bids, _asks = lv
                if bids:
                    return round(bids[0][0], 3)
        except Exception:
            pass
        return None

    def _cached_ask_queue(self, token):
        """(best_ask, queue_ahead) for `token` from the WS book cache only (no
        REST). queue_ahead = resting size at the best bid = the volume a fresh
        maker BUY would sit BEHIND. Either may be None if the book isn't cached.
        Used to annotate no-stake legs with the market data that actually
        governs whether they're stakeable (ask + queue), not the tier."""
        if token is None or self.bookfeed is None:
            return None, None
        ask = q = None
        try:
            lv = self.bookfeed.levels(token)
            if lv:
                bids, asks = lv
                if asks:
                    ask = round(asks[0][0], 3)
                if bids:
                    q = self.bookfeed.queue_at(token, bids[0][0], "BUY")
        except Exception:
            pass
        return ask, q

    async def _maybe_log_revision(self, w, p, *, force=False, block_reason=None):
        """After a NO-STAKE T+3, log a NO_STAKE row each time the revised Up/Down
        (or tier) CHANGES through the rest of the window — so the revised call +
        its dynamic features are visible in the table. With ``force=True``, also
        log revisions blocked by a primary-only policy. Pure logging: deduped by
        (side,tier), never stakes, never changes trading state. Revision rows use
        their own book label so they cannot be pooled with primary T3 results."""
        if not (LOG_T3_REVISIONS and (force or w.get("t3_no_stake"))):
            return
        key = (p.get("side"), p.get("label"))
        if p.get("side") is None or key == w.get("last_eval_key"):
            return
        w["last_eval_key"] = key
        idx = len(w["legs"])
        leg = self._new_leg(w, idx, is_t3=False)
        await _offload(self._stamp_signal, leg, p)
        leg["status"] = "NO_STAKE"
        leg["book"] = "t3_revision_shadow"
        leg["no_stake_reason"] = (
            block_reason or
            (f"revised T+{int(time.time() - w['window_anchor'])}s: "
             f"{p.get('reason') or ''}").rstrip(": "))
        w["legs"].append(leg)
        upsert(leg)

    async def _re_flip_shadow(self, w, now):
        """SHADOW (isolated): when the RE re-analysis FLIPS direction vs the T+3
        call, record the flipped side + the OBSERVED ask on it. Backtest says these
        flips are ~70% right (T+3 ~30% on the same windows) but are thrown away by
        the WEAK/MODERATE no-stake filter; this captures the real flip-side ask so
        we can score whether staking them is +EV. Pure logging: never stakes, never
        mutates trading state. Deduped on (flip_side, tier); fires while flat in the
        RE window regardless of the entry cap. plan_entry is cheap here (a no-stake
        tier returns before any REST book fetch; asks come from the WS cache)."""
        t3_side = w.get("t3_side")
        if t3_side is None:
            return
        p = await self.plan_entry(w, now)
        fside = p.get("side")
        bid_flip = p.get("best_bid")
        if fside is None or fside == t3_side:
            # flip collapsed back to the T+3 side (or no signal). Clear the dedup
            # key so that a LATER excursion back to the same flip side is captured
            # as a fresh seq row — previously reflip_last_key persisted across the
            # collapse, so a second flip to a prior side was silently deduped away.
            w["reflip_last_key"] = None
            return
        key = (fside, p.get("label"))
        if key == w.get("reflip_last_key"):
            # same flip still active -> just track the best (highest) exit bid seen
            # on the flip side, so we can score "could we have exited this flip
            # early into a favorable book" vs holding to settlement.
            cur = w.get("reflip_cur_seq")
            if cur is not None:
                if bid_flip is not None:
                    pk = w["reflip_peaks"]
                    pk[cur] = max(pk.get(cur, 0.0), bid_flip)
                # maker-fill test: lowest opp-side ask seen while this flip is
                # active — did the offer ever come down to a restable level.
                _oa = p.get("ask_down") if fside == "Up" else p.get("ask_up")
                if _oa is not None:
                    om = w["reflip_opp_min"]
                    om[cur] = min(om.get(cur, _oa), _oa)
            return
        w["reflip_last_key"] = key
        ask_flip = p.get("ask_up") if fside == "Up" else p.get("ask_down")
        ask_opp = p.get("ask_down") if fside == "Up" else p.get("ask_up")
        # OPP-side book at decision (the cheap side we'd BUY): best bid + the size
        # a fresh maker BUY would rest behind. WS-cache only, try/excepted — pure
        # logging, must never disturb the shadow path. (2026-07-16)
        opp_token = w.get("down_token") if fside == "Up" else w.get("up_token")
        bid_opp = self._cached_best_bid(opp_token)
        _oa2, queue_opp = self._cached_ask_queue(opp_token)
        seq = w["reflip_seq"]
        w["reflip_seq"] = seq + 1
        w["reflip_cur_seq"] = seq
        if bid_flip is not None:
            w["reflip_peaks"][seq] = bid_flip
        if ask_opp is not None:
            w["reflip_opp_min"][seq] = ask_opp
        # FLIP CONTEXT: the excursion this flip reverses lives on the T+3 side
        # (flip_side != t3_side). peak = biggest move that direction saw; the
        # current move toward T+3 (negative once flipped) gives the retrace
        # fraction (>=1.0 = fully back through the open / crossed). A big peak
        # that fully retraces = a developed reversal, NOT the weak/<2bps noise
        # crossing the crossing-instant tier would call it.
        peak_exc = (w["path_max_up_bps"] if t3_side == "Up"
                    else w["path_max_dn_bps"])
        cur_t3 = None
        pn = p.get("price_now")
        if pn is not None and w["price_open"]:
            _mv = (pn - w["price_open"]) / w["price_open"] * 1e4
            cur_t3 = _mv if t3_side == "Up" else -_mv
        retrace = ((peak_exc - cur_t3) / peak_exc
                   if (peak_exc and cur_t3 is not None) else None)
        # REAL fillable depth on the flip token: shares resting at asks a FAK taker
        # could sweep (price <= ask_flip + BUY_SLIP). ~0 here = a phantom ask with no
        # book behind it → the "no orders found to match FAK" rejections. Isolated /
        # best-effort: WS-cached (book_levels prefers the warm feed), try/excepted so
        # it can never disturb the shadow logging.
        fillable_sh = None
        try:
            flip_token = w.get("up_token") if fside == "Up" else w.get("down_token")
            # WS cache ONLY (no REST fallback) — this runs on every flip; a blocking
            # REST book fetch here would slow the loop. plan_entry just read this
            # token's ask, so the feed is warm. None cache -> leave depth unknown.
            lv = (self.bookfeed.levels(flip_token)
                  if (self.bookfeed is not None and flip_token is not None) else None)
            if lv is not None and ask_flip is not None:
                _bids, _asks = lv
                cap = ask_flip + BUY_SLIP + 1e-9
                fillable_sh = round(sum(sz for px, sz in _asks if px <= cap), 2)
        except Exception:
            fillable_sh = None
        # Same, for the OPP token — the side OPP-FADE actually BUYS (2026-07-19).
        # fillable_sh above is the FLIP token, so it cannot answer "would our opp
        # buy have filled"; bid_opp/queue_opp only cover the MAKER route. This is
        # the taker depth: shares at asks <= ask_opp + BUY_SLIP. Same WS-cache-only,
        # try/excepted contract — pure logging, never disturbs the shadow path.
        fillable_opp_sh = None
        touch_opp_sh = None
        opp_ask_ladder = None
        _opp_rungs = []        # live ask ladder, used by swingfade's fill model
        try:
            lv_o = (self.bookfeed.levels(opp_token)
                    if (self.bookfeed is not None and opp_token is not None) else None)
            if lv_o is not None and ask_opp is not None:
                _ob, _oa = lv_o
                cap_o = ask_opp + BUY_SLIP + 1e-9
                # Observed ladder runs to the WIDE cap; fillable_opp_sh stays on
                # BUY_SLIP so it keeps meaning "what a FAK could sweep TODAY" and
                # stays comparable with the 07-19 rows. Only the LOOK widened.
                _obs_cap = ask_opp + LADDER_OBS_SLIP + 1e-9
                _rungs = sorted(((float(px), float(sz)) for px, sz in _oa
                                 if px <= _obs_cap), key=lambda t: t[0])
                fillable_opp_sh = round(
                    sum(sz for px, sz in _rungs if px <= cap_o), 2)
                # touch = size at the BEST ask. Thin touch + fat ladder is the
                # trap case: fillable says FILLS while the sweep pays up.
                _opp_rungs = list(_rungs)
                if _rungs:
                    touch_opp_sh = round(_rungs[0][1], 2)
                    opp_ask_ladder = json.dumps(
                        [[round(px, 4), round(sz, 2)] for px, sz in _rungs])
        except Exception:
            fillable_opp_sh = None
            touch_opp_sh = None
            opp_ask_ladder = None
        # bin_mom_bps on the flip shadow row was the Binance leading momentum at flip
        # time (2026-07-14), logged to score a "Binance disagrees with the flip side"
        # gate. REMOVED 2026-07-30 with the rest of the lead; the column stays but is
        # written NULL from here on, so any cohort read must split on the 07-30 vintage
        # rather than treat NULL as "no momentum".
        _bin_mom = None
        # computed once — logged below AND read by the live oppfade gate that follows
        _adx_sl = adx_slope(w["adx_series"])
        _flip_tier = p.get("label") or "<2bps"
        _d_prune, _d_reason = _oppfade_prune_d(w, _flip_tier)
        upsert_re_flip_shadow({
            "fillable_sh": fillable_sh,
            "fillable_opp_sh": fillable_opp_sh,
            "touch_opp_sh": touch_opp_sh,
            "opp_ask_ladder": opp_ask_ladder,
            "bin_mom_bps": _bin_mom,
            "asset": ASSET,
            "window_anchor": w["window_anchor"],
            "seq": seq,
            "window_start_utc": w["window_start_utc"],
            "slug": w["slug"],
            "dry_run": DRY_RUN,
            "decided_at": datetime.now(timezone.utc),
            "elapsed_s": int(now - w["window_anchor"]),
            "t3_side": t3_side,
            "flip_side": fside,
            # sub-WEAK crossing (|move| < 2bps) has no tier -> label None; store an
            # explicit "<2bps" bucket so the column is never NULL and the developed
            # WEAK/MODERATE re-fire rows (dedup key incl. label) split cleanly.
            "flip_tier": _flip_tier,
            "flip_dbps": p.get("dbps"),
            # rule-D origin + counterfactual (logged on EVERY row, enforced only
            # when OPPFADE_PRUNE_D=1) — see _oppfade_prune_d
            "t3_tier": w.get("t3_tier"),
            "t3_bps": w.get("t3_bps"),
            "d_prune": _d_prune,
            "price_open": w["price_open"],
            "price_now": p.get("price_now"),
            "ask_flip": ask_flip,
            "ask_opp": ask_opp,
            "bid_opp": bid_opp,
            "queue_opp": queue_opp,
            "best_ask": p.get("best_ask"),
            "peak_excursion_bps": round(peak_exc, 2) if peak_exc else 0.0,
            "retrace_frac": round(retrace, 3) if retrace is not None else None,
            "adx_samples": json.dumps(w["adx_series"]),
            "adx_slope": _adx_sl,
            # scale-free twin of adx_slope — see the adx_chg_pct migration note.
            # Data only; nothing gates on it until the 2026-08-01 review.
            "adx_chg_pct": adx_chg_pct(w["adx_series"]),
            # SWING PATH (2026-07-28): the raw excursion legs + crossings this
            # window carried into the flip, and the adverse cap in force. Written
            # on EVERY flip row, armed or not, so the axis stays measurable
            # whether or not the gate is enforcing.
            "path_max_up_bps": round(w.get("path_max_up_bps") or 0.0, 2),
            "path_max_dn_bps": round(w.get("path_max_dn_bps") or 0.0, 2),
            "cross_count": w.get("cross_count"),
            "swingfade_adverse_max": _swingfade_adverse_max(),
        })

        # ---------------- PRICE-BAND SWING BOOKS -----------------------------
        # Each band retains its independent rule and capital ledger.  Disarmed or
        # exhausted books continue to write SHADOW rows; armed books send one FAK
        # and park fills to settle. Both assets pin that FAK to the live touch
        # so a 0.45 qualify cannot walk the cap to the band ceiling. Hold-to-
        # settle is unchanged.
        _sb_elapsed = int(now - w["window_anchor"])
        _sb_spec = swingband_shadow_match(
            elapsed_s=_sb_elapsed, ask=ask_opp, adx_slope=_adx_sl,
            crossings=w.get("cross_count"),
            up_exc_bps=w.get("path_max_up_bps"),
            dn_exc_bps=w.get("path_max_dn_bps"), flip_side=fside,
            min_pre_ask=w.get("sf_min_pre_ask"),
            vol_3min_bps=w.get("vol_3min_bps"))
        _sb_ff_ok, _, _, _sb_ff_why = fade_fill_delta_gate(
            fillable_sh, fillable_opp_sh)
        if (ASSET in ("BTC", "ETH") and opp_token is not None
                and not w.get("swingband_done") and _sb_spec is not None
                and not _sb_ff_ok):
            w["swingband_done"] = True
            print(f"[{datetime.now(timezone.utc):%H:%M:%S}] SWINGBAND-SKIP "
                  f"{_sb_spec['book']} {w['slug']} · {_sb_ff_why}", flush=True)
            await self._log_fd_block(
                w, p, _sb_spec["book"], fside, ask_opp, opp_token,
                fillable_sh, fillable_opp_sh, _sb_ff_why, elapsed_s=_sb_elapsed)
        if (ASSET in ("BTC", "ETH") and opp_token is not None
                and not w.get("swingband_done") and _sb_spec is not None
                and _sb_ff_ok):
            _sb_side = "Down" if fside == "Up" else "Up"
            _sb_regime_ok, _sb_regime_why = eth_band_regime_allows(
                _sb_spec["book"])
            _sb_ref_ok, _sb_ref_why = three_source_reference_healthy(
                w["window_anchor"])
            _sb_live = (self.live and getattr(self, "executor", None) is not None
                        and book_stakes_now(_sb_spec["book"], False,
                                            w["window_start_utc"])
                        and _sb_regime_ok and _sb_ref_ok)
            if not self.live:
                _sb_live_why = "bot dry-run/shadow"
            elif not _book_live(_sb_spec["book"], False):
                _sb_live_why = "book switch off"
            elif not _sb_regime_ok:
                _sb_live_why = "regime veto: " + _sb_regime_why
            elif not _sb_ref_ok:
                _sb_live_why = "reference-health veto: " + _sb_ref_why
            else:
                _sb_live_why = "live execution unavailable"
            _sb_rungs = _opp_rungs
            _sb_touch = ask_opp
            if _sb_live:
                _sb_touch, _sb_rungs = fade_live_touch(self.bookfeed, opp_token)
                if _sb_touch is None:
                    _sb_touch, _sb_rungs = ask_opp, _opp_rungs
            _sb_ok, _sb_cap, _sb_place_why = fade_place(
                live_ask=_sb_touch, band_lo=_sb_spec["lo"],
                band_hi=_sb_spec["hi"], inclusive_lo=False)
            if not _sb_ok:
                if not w.get("swingband_place_logged"):
                    w["swingband_place_logged"] = True
                    print(f"[{datetime.now(timezone.utc):%H:%M:%S}] "
                          f"SWINGBAND-PLACE-SKIP {_sb_spec['book']} {w['slug']} "
                          f"· {_sb_place_why}", flush=True)
            else:
                w["swingband_done"] = True
                _sb_stake = max(MIN_ORDER_USD, SWINGBAND_SHADOW_STAKE_USD)
                _cb_stake = crossbook_stake_for(_sb_spec["book"], self._live_usdc)
                if _cb_stake is not None:
                    _sb_stake = _cb_stake
                    if _sb_stake < MIN_ORDER_USD:
                        _sb_live = False
                _sb_sh, _sb_usd, _sb_avg, _sb_frac = sweep_ladder(
                    _sb_rungs, _sb_stake, _sb_cap)
                _sb_leg = self._new_leg(w, len(w["legs"]), is_t3=False)
                await _offload(self._stamp_signal, _sb_leg, p)
                _sb_leg["predicted_side"] = _sb_side
                _sb_leg["token_id"] = str(opp_token)
                _sb_leg["book"] = _sb_spec["book"]
                _sb_leg["intended_stake_usd"] = round(_sb_stake, 2)
                if _sb_live:
                    _avg, _sh, _ok, _why = await self.executor.buy(
                        opp_token, _sb_stake, ask_opp, max_price=_sb_cap)
                    if _ok and _sh:
                        _sb_leg["entry_price"] = round(_avg or ask_opp, 4)
                        _sb_leg["shares"] = floor_shares(_sh)
                        _sb_leg["stake_usd"] = round(_sh * (_avg or ask_opp), 2)
                        _sb_leg["entry_fee_usd"] = round(
                            _taker_fee(_sh, _avg or ask_opp), 4)
                        _sb_leg["staked"] = True
                        _sb_leg["status"] = "OPEN"
                        _sb_leg["no_stake_reason"] = None
                        w["hold_legs"].append(_sb_leg)
                        w["staked_count"] += 1
                        asyncio.create_task(self._reconcile_stake(
                            _sb_leg, str(opp_token), _sh))
                    else:
                        _sb_leg["entry_price"] = round(ask_opp, 4)
                        _sb_leg["shares"] = 0.0
                        _sb_leg["stake_usd"] = 0.0
                        _sb_leg["staked"] = False
                        _sb_leg["status"] = "NO_FILL"
                        _sb_leg["no_stake_reason"] = (
                            f"live order rejected: {_why}")
                else:
                    _sb_leg["entry_price"] = round(
                        _sb_avg if _sb_avg else ask_opp, 4)
                    _sb_leg["shares"] = floor_shares(_sb_sh)
                    _sb_leg["stake_usd"] = round(_sb_usd, 2)
                    _sb_leg["staked"] = False
                    _sb_leg["status"] = "SHADOW"
                    _sb_leg["no_stake_reason"] = (
                        f"{_sb_spec['book']} SHADOW · band "
                        f"({_sb_spec['lo']:.2f},{_sb_spec['hi']:.2f}] · "
                        f"t={_sb_spec['t0']}-{_sb_spec['t1']}s · "
                        + (f"{_sb_spec.get('slope_min', 0):g}<|slope|<="
                           if _sb_spec.get('slope_min', 0) else "|slope|<=")
                        + f"{_sb_spec['slope']:g} · "
                        f"sweep {_sb_frac:.0%} of ${_sb_stake:.2f} @ "
                        f"{(_sb_avg or ask_opp):.4f} · {_sb_place_why}"
                        + (" · bot DRY-RUN" if not self.live
                           else " · " + _sb_live_why))
                stamp_oppfade_provenance(
                    _sb_leg, seq=seq, elapsed_s=_sb_elapsed, ask_opp=ask_opp,
                    adx_slope=_adx_sl, retrace=retrace, flip_tier=_flip_tier,
                    fillable_sh=fillable_opp_sh, touch_sh=touch_opp_sh)
                _sb_leg["swing_crossings"] = w.get("cross_count")
                _sb_leg["swing_up_exc_bps"] = round(
                    w.get("path_max_up_bps") or 0.0, 2)
                _sb_leg["swing_dn_exc_bps"] = round(
                    w.get("path_max_dn_bps") or 0.0, 2)
                _sb_leg["swing_adverse_bps"] = round(
                    (w.get("path_max_up_bps") if fside == "Up"
                     else w.get("path_max_dn_bps")) or 0.0, 2)
                w["legs"].append(_sb_leg)
                upsert(_sb_leg)
                print(f"[{datetime.now(timezone.utc):%H:%M:%S}] "
                      f"SWINGBAND-{'LIVE' if _sb_live else 'SHADOW'} "
                      f"{_sb_spec['book']} {w['slug']} "
                      f"{_sb_leg['predicted_side']} ask {ask_opp:.3f} · "
                      f"t={_sb_elapsed}s · cross={w.get('cross_count')} · "
                      f"exc {w.get('path_max_up_bps'):.1f}/"
                      f"{w.get('path_max_dn_bps'):.1f}bps · slope {_adx_sl} "
                      f"-> sweep {_sb_frac:.0%} · {_sb_place_why}",
                      flush=True)

        # LIVE CHEAP-OPP FADE ORDER (2026-07-20). The book carries the ENTIRE
        # bankroll now; every other book is at 0.00. We BUY the cheap OPPOSITE side of
        # the flip and HOLD TO SETTLE — no exit, no salvage (bail rules are refuted on
        # this market: the winners ARE the hard reversals and at +30s a winner and a
        # loser look identical).
        #
        # The gate below is the scored cohort at the single 0.16 cap the scorer now
        # shares (75 OOS windows, 20.0% vs ask 11.1%, +8.9pp, p=0.02). The
        # pre-registration itself ran at 0.15 (+9.2pp, p=0.02, n=67) — that
        # distinction lives in the scorer's PROVENANCE docstring and is the reason
        # this book is believable where the flip was not.
        #
        # ONE ATTEMPT PER WINDOW, at the earliest qualifying seq — matching the
        # scorer's dedup. `open_leg is None` covers a FILLED bet; `oppfade_done`
        # covers a rejected one (07-20: open_leg alone let a kill leave the window
        # open to re-fire at a later, unscored seq). Runs BEFORE the flip block
        # because that block returns early on its own skip gates.
        # ADX gate: asymmetric slope band + flat-band + retrace cap. See
        # oppfade_adx_ok() — one definition, shared with the dashboard scorer.
        _adx_ok, _adx_why = oppfade_adx_ok(_adx_sl, retrace,
                                           int(now - w["window_anchor"]))
        # Gate and LIVENESS are split (2026-07-26) so a DISARMED book still leaves a
        # row. Before this the whole block short-circuited on _book_live before
        # anything was written, so oppfade's forward performance had to be
        # reconstructed from re_flip_shadow after the fact. `_oppfade_ok` keeps its
        # exact old meaning; the shadow branch below picks up gate-and-not-live.
        _oppfade_gate = (not ETH_OPPFADE_RETIRED and ASSET in ("BTC", "ETH")
                         and w["open_leg"] is None and not w["oppfade_done"]
                         and opp_token is not None
                         and ask_opp is not None
                         and OPPFADE_ASK_MIN < ask_opp <= OPPFADE_ASK_MAX
                         and _flip_tier not in D_STRONG_TIERS
                         and _adx_ok)
        _oppfade_side = "Down" if fside == "Up" else "Up"
        _oppfade_live = (self.live and getattr(self, "executor", None) is not None
                         and _flip_tier != "<2bps"
                         and book_stakes_now("oppfade", False,
                                             w["window_start_utc"])
                         and three_source_consensus_ok(
                             w["window_anchor"], _oppfade_side)[0])
        _oppfade_ok = _oppfade_gate and _oppfade_live
        # Journal every ADX/retrace refusal that would OTHERWISE have staked, so the
        # forward A/B is readable from the log as well as from the shadow rows.
        if (not _adx_ok and self.live and ASSET in ("BTC", "ETH")
                and _book_live("oppfade", False)
                and w["open_leg"] is None and not w["oppfade_done"]
                and opp_token is not None
                and ask_opp is not None
                and OPPFADE_ASK_MIN < ask_opp <= OPPFADE_ASK_MAX
                and _flip_tier not in D_STRONG_TIERS):
            print(f"[{datetime.now(timezone.utc):%H:%M:%S}] OPPFADE-ADX SKIP "
                  f"{w['slug']} seq={seq} ask {ask_opp:.3f} · {_adx_why}", flush=True)
        # RULE D counterfactual (2026-07-21). With PRUNE_D off this prints the entry
        # D would have refused and we are about to take anyway — the forward A/B.
        # `d_prune` is on the shadow row too; the journal line is for eyeballing it
        # live. Only the origin->"<2bps" arm can reach here; the STRONG/MOD-STRONG
        # arm is already excluded by _oppfade_ok above.
        if _oppfade_ok and _d_prune:
            print(f"[{datetime.now(timezone.utc):%H:%M:%S}] OPPFADE-D "
                  f"{'SKIP' if OPPFADE_PRUNE_D else 'WOULD-SKIP'} {w['slug']} "
                  f"seq={seq} ask {ask_opp:.3f} · {_d_reason}", flush=True)
        if _oppfade_ok and not (OPPFADE_PRUNE_D and _d_prune):
            # DAILY-LOCKED sizing (2026-07-22): capital = the exchange USDC
            # balance snapshotted at 00:00 UTC (fixed all day, NOT compounded per
            # stake). Deploy 2/3 of it (OPPFADE_USABLE_FRAC), then stake 10% of that
            # usable slice → effective stake = daily_capital * 2/3 * 10%. This
            # replaced the daily-base formula, which double-counted losses baked
            # into capital_usd and summed unstaked/modelled (shadow) rows.
            _cap = (self._daily_capital if self._daily_capital is not None
                    else self._live_usdc if self._live_usdc is not None
                    else self.oppfade_base)
            _usable = _cap * OPPFADE_USABLE_FRAC
            stake_usd = round(_usable * OPPFADE_STAKE_FRAC, 2)
            _cb_stake = crossbook_stake_for("oppfade", self._live_usdc)
            _cb_active = _cb_stake is not None
            if _cb_active:
                stake_usd = _cb_stake
            # FLOOR at PM's $1 minimum (a sub-$1 fraction used to SILENTLY skip the
            # window — no order, no row, no log); never exceed the live wallet.
            if stake_usd < MIN_ORDER_USD and not _cb_active:
                stake_usd = MIN_ORDER_USD
            _wallet = self._live_usdc if self._live_usdc is not None else _cap
            if stake_usd > _wallet:
                stake_usd = round(_wallet, 2)
            # LOUD ALERT on the residual silent-skip (2026-07-22). The $1 floor above
            # is undone by the wallet clamp whenever the live USDC balance is under
            # MIN_ORDER_USD, which lands us back on the ORIGINAL bug: a fully
            # qualifying cohort window produces no order, no row and no log line.
            # That failure mode cost two 12.5x winners on 07-20 11:00 and 07-22 04:25
            # and was invisible for two days precisely because it printed nothing.
            # Never let this branch be silent again — if the book cannot bet its own
            # cohort, that is the single most expensive fact of the session.
            if stake_usd < MIN_ORDER_USD:
                print(f"[{datetime.now(timezone.utc):%H:%M:%S}] *** OPPFADE MISSED "
                      f"COHORT WINDOW *** {w['slug']} seq={seq} ask {ask_opp:.3f} "
                      f"— stake ${stake_usd:.2f} < ${MIN_ORDER_USD:.2f} min "
                      f"(wallet ${_wallet:.2f}, daily cap ${_cap:.2f}). "
                      f"QUALIFIED BUT NOT BET.", flush=True)
                # DURABLE, not just journal (2026-07-22). The journal is the reason
                # this failure hid for two days: it printed nothing, and nothing is
                # not queryable. A MISSED row means "the cohort said bet and we could
                # not", which is a DIFFERENT failure from "we bet and lost" and has
                # to be countable separately — the two 12.5x winners we never fired
                # on are worth more than every loss the book has taken.
                # staked=False so it can never touch P&L; settlement still stamps
                # `hit`, so a later read can price exactly what the miss cost.
                _miss = self._new_leg(w, len(w["legs"]), is_t3=False)
                await _offload(self._stamp_signal, _miss, p)
                _miss["predicted_side"] = "Down" if fside == "Up" else "Up"
                _miss["token_id"] = str(opp_token)
                _miss["entry_price"] = round(ask_opp, 4)
                _miss["intended_stake_usd"] = round(stake_usd, 2)
                _miss["staked"] = False
                _miss["status"] = "MISSED"
                _miss["book"] = "oppfade"
                _miss["no_stake_reason"] = (
                    f"MISSED COHORT WINDOW — stake ${stake_usd:.2f} < "
                    f"${MIN_ORDER_USD:.2f} min (wallet ${_wallet:.2f}, "
                    f"daily cap ${_cap:.2f}). Qualified but not bet.")
                stamp_oppfade_provenance(
                    _miss, seq=seq, elapsed_s=int(now - w["window_anchor"]),
                    ask_opp=ask_opp, adx_slope=_adx_sl, retrace=retrace,
                    flip_tier=_flip_tier, fillable_sh=fillable_opp_sh,
                    touch_sh=touch_opp_sh)
                w["legs"].append(_miss)
                w["oppfade_done"] = True   # claim the window; do not re-fire later
                upsert(_miss)
            if stake_usd >= MIN_ORDER_USD:
                # Consume the window on the ATTEMPT, fill or not. The scorer dedups
                # on the EARLIEST qualifying seq, so a retry at a later seq is an
                # entry the +8.9pp cohort never scored — same reasoning as the flip
                # book's no-resend-on-kill rule. REVERT: drop this line to retry.
                w["oppfade_done"] = True
                # Price cap, not a slippage tolerance: the cohort is DEFINED by
                # ask<=cap, so a fill anywhere at or under it is the bet we scored.
                # A fill above it is a different (unvalidated) cohort — refuse it.
                avg, sh, ok, err = await self.executor.buy(
                    opp_token, stake_usd, ask_opp, max_price=OPPFADE_ASK_MAX)
                # IMMEDIATE RETRY ON KILL (2026-07-21). ONE re-fire, wider
                # limit, no loop — a second kill is final. The first FAK is dead
                # (fill-and-kill never rests), so there is no double-fill risk.
                # We read the book BEFORE re-firing purely to record the ask at
                # kill; the order itself needs no quote (max_price is absolute).
                retry_of_kill = None
                retry_ask_at_kill = None
                retry_rt_s = None
                retry_queue_sh = None
                if not ok and OPPFADE_RETRY_MAX > OPPFADE_ASK_MAX:
                    retry_of_kill = True
                    try:
                        _lv = (self.bookfeed.levels(opp_token)
                               if self.bookfeed is not None else None)
                        if _lv and _lv[1]:
                            retry_ask_at_kill = round(
                                min(float(px) for px, _sz in _lv[1]), 4)
                            # depth the retry FAK could actually take: resting ask
                            # shares at/under the retry cap. 0 == KILLED-AGAIN cause.
                            retry_queue_sh = round(
                                sum(float(sz) for px, sz in _lv[1]
                                    if float(px) <= OPPFADE_RETRY_MAX), 2)
                    except Exception:
                        retry_ask_at_kill = None
                        retry_queue_sh = None
                    _t_retry = time.monotonic()
                    avg, sh, ok, _err2 = await self.executor.buy(
                        opp_token, stake_usd, ask_opp,
                        max_price=OPPFADE_RETRY_MAX)
                    retry_rt_s = round(time.monotonic() - _t_retry, 3)
                    _rq = ("depth n/a" if retry_queue_sh is None
                           else f"{retry_queue_sh:.0f}sh<=cap")
                    err = (f"{err} | RETRY@{OPPFADE_RETRY_MAX:.2f} "
                           f"(ask {retry_ask_at_kill}, {_rq}, {retry_rt_s}s): "
                           f"{'FILLED' if ok else _err2}")
                    print(f"[{datetime.now(timezone.utc):%H:%M:%S}] OPPFADE-RETRY "
                          f"{w['slug']} kill@{OPPFADE_ASK_MAX:.2f} -> re-fire "
                          f"@{OPPFADE_RETRY_MAX:.2f} (ask at kill "
                          f"{retry_ask_at_kill}, {_rq}, rt {retry_rt_s}s) -> "
                          f"{'FILLED' if ok else 'KILLED AGAIN'}", flush=True)
                if not ok:
                    # RECORD THE REJECTION (2026-07-20). A failed order used to write
                    # nothing at all — so the dash, seeing no real leg for the window,
                    # fell back to its MODELLED leg and reported the bet as if it had
                    # been placed. On 07-20 09:08:52 that printed a +$23.55 "win" for
                    # an order the exchange killed on FAK no-match. A non-staked leg
                    # here claims the window for the real book, so the model is
                    # dropped and the row renders NO FILL with the ask + depth we saw.
                    leg = self._new_leg(w, len(w["legs"]), is_t3=False)
                    await _offload(self._stamp_signal, leg, p)
                    leg["predicted_side"] = "Down" if fside == "Up" else "Up"
                    leg["token_id"] = str(opp_token)
                    leg["entry_price"] = round(ask_opp, 4)
                    leg["intended_stake_usd"] = round(stake_usd, 2)
                    leg["staked"] = False
                    leg["status"] = "NO_FILL"
                    leg["book"] = "oppfade"
                    leg["retry_of_kill"] = retry_of_kill
                    leg["retry_ask_at_kill"] = retry_ask_at_kill
                    leg["retry_rt_s"] = retry_rt_s
                    leg["retry_queue_sh"] = retry_queue_sh
                    stamp_oppfade_provenance(
                        leg, seq=seq, elapsed_s=int(now - w["window_anchor"]),
                        ask_opp=ask_opp, adx_slope=_adx_sl, retrace=retrace,
                        flip_tier=_flip_tier, fillable_sh=fillable_opp_sh,
                        touch_sh=touch_opp_sh)
                    # depth ALSO stays in the reason text for the dash capsule; the
                    # queryable copy now lives in fillable_opp_sh / touch_opp_sh.
                    _q = "depth not logged" if queue_opp is None else f"{queue_opp:.0f}sh"
                    leg["no_stake_reason"] = (f"live order rejected @ cap "
                                              f"{OPPFADE_ASK_MAX:.2f} · ask "
                                              f"{ask_opp:.3f} · {_q} at decision — "
                                              f"{str(err)[:80]}")
                    w["legs"].append(leg)
                    upsert(leg)
                    print(f"[{datetime.now(timezone.utc):%H:%M:%S}] OPPFADE-LIVE "
                          f"{w['slug']} opp-of-{fside} ${stake_usd:.2f} FAILED: {err}",
                          flush=True)
                else:
                    fill_px = avg if avg is not None else ask_opp
                    opp_side = "Down" if fside == "Up" else "Up"
                    leg = self._new_leg(w, len(w["legs"]), is_t3=False)
                    await _offload(self._stamp_signal, leg, p)
                    # we bought the side OPPOSITE the flip — record OUR side, so the
                    # dash/settlement score this leg on what we actually hold.
                    leg["predicted_side"] = opp_side
                    leg["token_id"] = str(opp_token)
                    leg["entry_price"] = round(fill_px, 4)
                    leg["shares"] = round(sh or 0.0, 2)
                    leg["stake_usd"] = round((sh or 0.0) * fill_px, 2)
                    leg["intended_stake_usd"] = round(stake_usd, 2)
                    leg["staked"] = True
                    leg["status"] = "OPEN"
                    leg["book"] = "oppfade"
                    leg["retry_of_kill"] = retry_of_kill
                    leg["retry_ask_at_kill"] = retry_ask_at_kill
                    leg["retry_rt_s"] = retry_rt_s
                    leg["retry_queue_sh"] = retry_queue_sh
                    stamp_oppfade_provenance(
                        leg, seq=seq, elapsed_s=int(now - w["window_anchor"]),
                        ask_opp=ask_opp, adx_slope=_adx_sl, retrace=retrace,
                        flip_tier=_flip_tier, fillable_sh=fillable_opp_sh,
                        touch_sh=touch_opp_sh)
                    # cash trail: stake_usd is NOTIONAL (shares x price); the wallet
                    # is debited notional + this fee. Keep them separate so the two
                    # never get conflated again — settlement P&L nets the fee via
                    # taker_pnl(), this column is what reconciles to the statement.
                    leg["entry_fee_usd"] = round(
                        _taker_fee(sh or 0.0, fill_px), 4)
                    w["legs"].append(leg)
                    w["open_leg"] = leg        # held to settle; manage() skips it
                    w["staked_count"] += 1
                    upsert(leg)
                    # reconcile this stake against the exchange (detached)
                    asyncio.create_task(self._reconcile_stake(
                        leg, opp_token, sh or 0.0))
                    print(f"[{datetime.now(timezone.utc):%H:%M:%S}] OPPFADE-LIVE "
                          f"{w['slug']} {opp_side} (opp of {fside} "
                          f"{p.get('label')}) -> {sh:.2f}sh @ {fill_px:.3f} "
                          f"(${leg['stake_usd']}) LIVE, held to settle", flush=True)

        # ---------------- OPPFADE SHADOW (2026-07-26) ----------------
        # Same gate as the live book, but the book is disarmed (or the bot is in dry
        # run), so the fill is MODELLED against the real ask ladder and the row is
        # written staked=False — it can never touch P&L, and settlement still stamps
        # `hit`, so the forward read is queryable without reconstructing anything.
        # Deliberately keeps the live gate's `open_leg is None` clause: this measures
        # what oppfade WOULD have done alongside the other books, not standalone.
        if (_oppfade_gate and not _oppfade_live
                and not (OPPFADE_PRUNE_D and _d_prune)):
            w["oppfade_done"] = True     # one shadow row per window, earliest seq
            _os_cap = (self._daily_capital if self._daily_capital is not None
                       else self._live_usdc if self._live_usdc is not None
                       else self.oppfade_base)
            _os_stake = round(_os_cap * OPPFADE_USABLE_FRAC * OPPFADE_STAKE_FRAC, 2)
            if _os_stake < MIN_ORDER_USD:
                _os_stake = MIN_ORDER_USD
            # Sweep at the cohort cap; a TOTAL kill re-fires once at the retry cap,
            # matching the live retry-on-kill rule. Partials are NOT re-fired — the
            # live oppfade path does not re-fire them either (that is swingfade's
            # SWINGFADE_REFILL rule, and it must not leak into this book's model).
            _om_sh, _om_usd, _om_avg, _om_frac = sweep_ladder(
                _opp_rungs, _os_stake, OPPFADE_ASK_MAX)
            _os_retry = False
            if not _om_sh and OPPFADE_RETRY_MAX > OPPFADE_ASK_MAX:
                _om_sh, _om_usd, _om_avg, _om_frac = sweep_ladder(
                    _opp_rungs, _os_stake, OPPFADE_RETRY_MAX,
                    min_price=OPPFADE_ASK_MAX)
                _os_retry = bool(_om_sh)
            _os_leg = self._new_leg(w, len(w["legs"]), is_t3=False)
            await _offload(self._stamp_signal, _os_leg, p)
            _os_leg["predicted_side"] = "Down" if fside == "Up" else "Up"
            _os_leg["token_id"] = str(opp_token)
            _os_leg["book"] = "oppfade"
            _os_leg["entry_price"] = round(_om_avg if _om_avg else ask_opp, 4)
            _os_leg["shares"] = floor_shares(_om_sh)      # match the live path
            _os_leg["stake_usd"] = round(_om_usd, 2)
            _os_leg["intended_stake_usd"] = round(_os_stake, 2)
            _os_leg["staked"] = False                     # SHADOW: never real P&L
            _os_leg["status"] = "SHADOW"
            _os_leg["retry_of_kill"] = _os_retry or None
            _os_leg["no_stake_reason"] = (
                f"oppfade SHADOW · modelled sweep {_om_frac:.0%} of "
                f"${_os_stake:.2f} @ avg {(_om_avg or ask_opp):.4f}"
                + (f" (re-fire @{OPPFADE_RETRY_MAX:.2f})" if _os_retry else "")
                + (" · bot DRY-RUN" if not self.live else " · book disarmed"))
            stamp_oppfade_provenance(
                _os_leg, seq=seq, elapsed_s=int(now - w["window_anchor"]),
                ask_opp=ask_opp, adx_slope=_adx_sl, retrace=retrace,
                flip_tier=_flip_tier, fillable_sh=fillable_opp_sh,
                touch_sh=touch_opp_sh)
            w["legs"].append(_os_leg)
            upsert(_os_leg)
            print(f"[{datetime.now(timezone.utc):%H:%M:%S}] OPPFADE-SHADOW "
                  f"{w['slug']} {_os_leg['predicted_side']} ask {ask_opp:.3f} "
                  f"· slope {_adx_sl} · sweep {_om_frac:.0%} of ${_os_stake:.2f} "
                  f"@ {(_om_avg or ask_opp):.4f}", flush=True)

        # ---------------- SWINGFADE (2026-07-23) ----------------
        # Independent staking category, own gate/cap/ceiling — see SWINGFADE_* and
        # swingfade_ok(). Runs on EVERY window regardless of self.live: when the bot
        # is in DRY RUN (or the book is disarmed) it writes a non-staked SHADOW leg
        # with the fill MODELLED off the real ask ladder, so the forward read is
        # available before a cent is risked. Never touches w["open_leg"] unless a
        # real order actually filled.
        _sf_elapsed = int(now - w["window_anchor"])
        _sf_amax = _swingfade_adverse_max()   # same value stamped on the shadow row
        _sf_ok, _sf_why = swingfade_ok(
            elapsed_s=_sf_elapsed, ask=ask_opp,
            up_exc_bps=w.get("path_max_up_bps"), dn_exc_bps=w.get("path_max_dn_bps"),
            adx_slope=_adx_sl, crossings=w.get("cross_count"),
            flip_side=fside, adverse_max=_sf_amax,
            pre_ticks=w.get("sf_pre_ticks"),
            min_pre_ask=w.get("sf_min_pre_ask"),
            vol_3min_bps=w.get("vol_3min_bps"))
        if _sf_ok and swingfade_trial_active(ASSET):
            _sf_ok, _sf_why = await _offload(
                swingfade_trial_confirmation, ENGINE, ASSET,
                "Down" if fside == "Up" else "Up")
            print(f"[{datetime.now(timezone.utc):%H:%M:%S}] SWINGFADE-TRIAL "
                  f"{w['slug']} seq={seq} pass={_sf_ok} {_sf_why}", flush=True)
        # FILLABLE-DELTA gate (2026-07-24, by request; objection noted at the
        # constant). Fire only when flip-minus-opp taker depth < cap; blank depth = 0.
        # Applies to LIVE and SHADOW alike so the forward test tracks the live rule.
        # `swingfade_fill_delta_max` dash config wins, else the env constant; <=0 off.
        if _sf_ok:
            _sf_ok, _, _, _sf_fill_why = fade_fill_delta_gate(
                fillable_sh, fillable_opp_sh)
            if not _sf_ok:
                _sf_why = _sf_fill_why
                if (ASSET in ("BTC", "ETH") and opp_token is not None
                        and not w["swingfade_done"]
                        and not w.get("swingfade_fdblock_logged")):
                    w["swingfade_fdblock_logged"] = True    # one row per window
                    await self._log_fd_block(
                        w, p, "swingfade", fside, ask_opp, opp_token,
                        fillable_sh, fillable_opp_sh, _sf_fill_why,
                        elapsed_s=_sf_elapsed)
        # Observability: journal the FIRST refusal per window. Without this a gate
        # that never fires is indistinguishable from a gate that is never reached —
        # which is exactly how the oppfade MIN_ORDER skip hid for two days and cost
        # two 12.5x winners. Once per window, so it cannot flood the log.
        if (ASSET in ("BTC", "ETH") and opp_token is not None
                and not w["swingfade_done"] and not _sf_ok
                and not w.get("swingfade_logged")):
            w["swingfade_logged"] = True
            print(f"[{datetime.now(timezone.utc):%H:%M:%S}] SWINGFADE-SKIP "
                  f"{w['slug']} seq={seq} t={_sf_elapsed}s ask "
                  f"{'n/a' if ask_opp is None else format(ask_opp, '.3f')} "
                  f"cross={w.get('cross_count')} exc "
                  f"{w.get('path_max_up_bps'):.1f}/{w.get('path_max_dn_bps'):.1f}bps "
                  f"slope={_adx_sl} · {_sf_why}", flush=True)
        if (ASSET in ("BTC", "ETH") and opp_token is not None
                and not w["swingfade_done"] and _sf_ok):
            _sf_side = "Down" if fside == "Up" else "Up"
            # Reference gate: swingfade is a reversal book like swing-band, so it
            # uses the direction-agnostic health check, not consensus-on-fade-side.
            # three_source_consensus_ok() requires PM+Coinbase+Kraken to already
            # agree with the FADE side by decision time — for a countertrend bet
            # that is nearly the trend's own direction until the reversal is fully
            # realized, so it passed 0/77 live-eligible fires since 08-11 (verified
            # by replay 2026-09-07). three_source_reference_healthy() (added for
            # swing-band, see its call above) keeps the fail-closed stale/missing-
            # feed protection without vetoing on direction.
            _sf_ref_ok, _sf_ref_why = three_source_reference_healthy(
                w["window_anchor"])
            _sf_live = (self.live and getattr(self, "executor", None) is not None
                        and book_stakes_now("swingfade", False,
                                            w["window_start_utc"])
                        and _sf_ref_ok)
            _sf_rungs = _opp_rungs
            _sf_touch = ask_opp
            if _sf_live:
                _sf_touch, _sf_rungs = fade_live_touch(self.bookfeed, opp_token)
                if _sf_touch is None:
                    _sf_touch, _sf_rungs = ask_opp, _opp_rungs
            _sf_lo, _sf_hi = swingfade_trial_band(
                ASSET, SWINGFADE_ASK_MIN, SWINGFADE_ASK_MAX)
            _sf_place_ok, _sf_fak_cap, _sf_place_why = fade_place(
                live_ask=_sf_touch, band_lo=_sf_lo,
                band_hi=_sf_hi, inclusive_lo=True)
            if not _sf_place_ok:
                if not w.get("swingfade_place_logged"):
                    w["swingfade_place_logged"] = True
                    print(f"[{datetime.now(timezone.utc):%H:%M:%S}] "
                          f"SWINGFADE-PLACE-SKIP {w['slug']} · {_sf_place_why}",
                          flush=True)
            else:
                w["swingfade_done"] = True          # one attempt per window, earliest seq
                # BOUNDED $1 LIVE-TEST batch: when a batch is configured, a live fire uses
                # the $1 test stake; a configured-but-SPENT batch must not keep placing
                # real money if the toggle lags a tick before the auto-disarm lands, so
                # treat a spent batch as SHADOW. See _swingfade_test().
                _sf_test_on, _sf_test_stake, _sf_test_max, _sf_test_cnt = _swingfade_test()
                if _sf_live and _sf_test_max > 0 and not _sf_test_on:
                    _sf_live = False
                # Sizing: same daily-locked capital as oppfade (snapshot at 00:00 UTC,
                # NOT compounded per stake). FLAT SWINGFADE_MIN_STAKE until the book has
                # crossed BANKROLL_SEED_USD, then bank x 2/3 x 10% = bank/15 capped at
                # SWINGFADE_STAKE_CEIL. The ceiling is the capacity limit, not a risk
                # preference — see the constant's comment. See swingfade_stake_for().
                _sf_cap = (self._daily_capital if self._daily_capital is not None
                           else self._live_usdc if self._live_usdc is not None
                           else self.oppfade_base)
                if _sf_live and _sf_test_on:
                    _sf_stake = _sf_test_stake          # bounded live-test: $1/order
                else:
                    _sf_stake = swingfade_stake_for(_sf_cap)
                _cb_stake = crossbook_stake_for("swingfade", self._live_usdc)
                _cb_active = _cb_stake is not None
                if _cb_active:
                    _sf_stake = _cb_stake
                if _sf_stake < MIN_ORDER_USD and not _cb_active:
                    _sf_stake = MIN_ORDER_USD
                if _cb_active and _sf_stake < MIN_ORDER_USD:
                    _sf_live = False                 # exhausted ledger: keep shadowing
                _sf_leg = self._new_leg(w, len(w["legs"]), is_t3=False)
                await _offload(self._stamp_signal, _sf_leg, p)
                _sf_leg["predicted_side"] = _sf_side
                _sf_leg["token_id"] = str(opp_token)
                _sf_leg["book"] = "swingfade"
                _sf_leg["intended_stake_usd"] = round(_sf_stake, 2)
                _sf_refill_sh = 0.0
                _sf_refill_usd = 0.0
                if _sf_live:
                    # REAL path: exactly ONE FAK at the live-touch cap. A total kill
                    # is a NO_FILL; a partial fill is accepted and HELD TO SETTLE.
                    # Do not flatten a dump fill — salvage was the leak.
                    avg, sh, ok, err = await self.executor.buy(
                        opp_token, _sf_stake, ask_opp, max_price=_sf_fak_cap)
                    if ok and sh:
                        _sf_leg["entry_price"] = round(avg or ask_opp, 4)
                        # shares FLOORED (never round up past the on-chain balance —
                        # see floor_shares); stake/fee stay on the RAW fill because
                        # that is what the wallet was actually debited.
                        _sf_leg["shares"] = floor_shares(sh)
                        _sf_leg["stake_usd"] = round(sh * (avg or ask_opp), 2)
                        _sf_leg["entry_fee_usd"] = round(
                            _taker_fee(sh, avg or ask_opp), 4)
                        _sf_leg["staked"] = True
                        _sf_leg["status"] = "OPEN"
                        # Parks in hold_legs, NOT open_leg (2026-07-29): swingfade is
                        # hold-to-settle, so it needs no manage() slot, and occupying
                        # open_leg is what used to block the t3 book from the same
                        # window. Behaviour here is otherwise unchanged.
                        w["hold_legs"].append(_sf_leg)
                        w["staked_count"] += 1
                        # reconcile this stake against the exchange (detached). Wired
                        # 2026-07-25: swingfade had been the ONLY live book without it,
                        # so recon_* was NULL on every real leg.
                        asyncio.create_task(self._reconcile_stake(
                            _sf_leg, opp_token, sh or 0.0))
                    else:
                        _sf_leg["entry_price"] = round(ask_opp, 4)
                        _sf_leg["staked"] = False
                        _sf_leg["status"] = "NO_FILL"
                        _sf_leg["no_stake_reason"] = (
                            f"swingfade order rejected @ cap {_sf_fak_cap:.3f} · "
                            f"ask {ask_opp:.3f} · {_sf_place_why} — "
                            f"{str(err)[:80]}")
                    # bounded live-test: a SENT order (fill or no-fill) consumes the
                    # batch and, when spent, auto-disarms swingfade_live back to shadow.
                    if _sf_test_on:
                        _swingfade_test_tick(filled=bool(_sf_leg.get("staked")))
                else:
                    # SHADOW path mirrors live: one sweep at the touch cap, with any
                    # partial left partial and no second sweep of the remaining ladder.
                    _m_sh, _m_usd, _m_avg, _m_frac = sweep_ladder(
                        _sf_rungs, _sf_stake, _sf_fak_cap)
                    _sf_leg["entry_price"] = round(
                        _m_avg if _m_avg else ask_opp, 4)
                    _sf_leg["shares"] = floor_shares(_m_sh)
                    _sf_leg["stake_usd"] = round(_m_usd, 2)
                    _sf_leg["staked"] = False
                    _sf_leg["status"] = "SHADOW"
                    _sf_leg["no_stake_reason"] = (
                        f"swingfade SHADOW · modelled sweep {_m_frac:.0%} of "
                        f"${_sf_stake:.2f} @ avg {(_m_avg or ask_opp):.4f} · "
                        f"{_sf_place_why}"
                        + (" · bot DRY-RUN" if not self.live
                           else " · book disarmed"))
                # provenance: the gate inputs AS READ, so a forward read never has to
                # guess which rule fired. cohort_qualified is True by construction here.
                stamp_oppfade_provenance(
                    _sf_leg, seq=seq, elapsed_s=_sf_elapsed, ask_opp=ask_opp,
                    adx_slope=_adx_sl, retrace=retrace, flip_tier=_flip_tier,
                    fillable_sh=fillable_opp_sh, touch_sh=touch_opp_sh)
                _sf_leg["swing_crossings"] = w.get("cross_count")
                _sf_leg["swing_up_exc_bps"] = round(
                    w.get("path_max_up_bps") or 0.0, 2)
                _sf_leg["swing_dn_exc_bps"] = round(
                    w.get("path_max_dn_bps") or 0.0, 2)
                _sf_leg["swing_refill_usd"] = round(_sf_refill_usd, 2) or None
                # Which regime this leg was taken under (2026-07-28). 0 = the adverse
                # leg was disarmed; >0 = enforcing at that cap. Stamped per leg so a
                # later read never has to date-match against a config change.
                _sf_leg["swing_adverse_max"] = _sf_amax or 0.0
                _sf_leg["swing_adverse_bps"] = round(
                    (w.get("path_max_up_bps") if fside == "Up"
                     else w.get("path_max_dn_bps")) or 0.0, 2)
                w["legs"].append(_sf_leg)
                upsert(_sf_leg)
                print(f"[{datetime.now(timezone.utc):%H:%M:%S}] SWINGFADE"
                      f"{'-LIVE' if _sf_live else '-SHADOW'} {w['slug']} "
                      f"{_sf_side} ask {ask_opp:.3f} · t={_sf_elapsed}s · "
                      f"cross={w.get('cross_count')} "
                      f"· exc {w.get('path_max_up_bps'):.1f}/"
                      f"{w.get('path_max_dn_bps'):.1f}bps "
                      f"(adverse {_sf_leg['swing_adverse_bps']:.1f} vs cap "
                      f"{_sf_amax or 0:.1f}) · slope {_adx_sl} -> "
                      f"{_sf_leg['status']} "
                      f"${_sf_leg.get('stake_usd') or 0:.2f} · {_sf_place_why}",
                      flush=True)

        # LIVE FLIP ORDER (2026-07-13): the shadow row above is ALWAYS
        # recorded. Separately, the flip book places a REAL taker BUY on the flip
        # side when EITHER (a) the `flip_live` book toggle is on — a continuous live
        # flip book, sized off flip_capital()×FLIP_STAKE_FRAC, one entry per window
        # (held to settle) — OR (b) a `flip_live_oneshot` is armed while the toggle
        # is off: ONE small USDC-sized plumbing order that auto-disarms. Both fire on
        # the FIRST distinct flip while flat and set open_leg so this runs once/window.
        # The calendar veto is applied to the one-shot as well as to the book, and
        # the one-shot's "toggle is off" precedence is read off the TOGGLE, not off
        # the vetoed value: otherwise a blocked Saturday would flip `flip_live` False
        # and thereby ARM an idle one-shot that the live book had been suppressing —
        # a calendar gate that lets real money through is worse than no gate.
        _flip_dow_ok = dow_stake_ok(w["window_start_utc"])[0]
        flip_live = _book_live("flip", False) and _flip_dow_ok
        flip_oneshot = (not _book_live("flip", False)) and _flip_dow_ok and _oneshot_armed("flip")
        if (self.live and getattr(self, "executor", None) is not None
                and w["open_leg"] is None and ask_flip is not None
                and (flip_live or flip_oneshot)):
            _consensus_ok, _consensus_why = three_source_consensus_ok(
                w["window_anchor"], fside)
            if not _consensus_ok:
                print(f"[{datetime.now(timezone.utc):%H:%M:%S}] FLIP-SKIP "
                      f"{w['slug']} · {_consensus_why}", flush=True)
                return
            # VALIDATED STAKE GATE (continuous flip book only; the one-shot plumbing
            # order intentionally bypasses it). Match the shipped research cohort:
            # cheap R:R ask AND a fully-crossed / developed flip. NULL retrace (no
            # feed coverage yet) does NOT qualify — same as the dashboard model.
            if flip_live and (ask_flip > FLIP_STAKE_MAX_ASK
                              or retrace is None or retrace < FLIP_RETRACE_LO):
                print(f"[{datetime.now(timezone.utc):%H:%M:%S}] FLIP-SKIP "
                      f"{w['slug']} {fside} ask={ask_flip:.3f} "
                      f"retrace={'-' if retrace is None else f'{retrace:.2f}'} "
                      f"(gate: ask<={FLIP_STAKE_MAX_ASK:.2f} & "
                      f"retrace>={FLIP_RETRACE_LO:.2f})", flush=True)
                return
            # ADX-BAND gate: skip the coin-flip middle (ADX7 in [LO,HI)); stake only
            # quiet (ADX<LO) or strong-trend (ADX>=HI) flips. Missing ADX = allowed.
            _adx_now = (w["adx_series"][-1][1]
                        if w.get("adx_series") else None)
            if (flip_live and _adx_now is not None
                    and FLIP_ADX_LO <= _adx_now < FLIP_ADX_HI):
                print(f"[{datetime.now(timezone.utc):%H:%M:%S}] FLIP-SKIP "
                      f"{w['slug']} {fside} ADX7={_adx_now:.1f} "
                      f"(gate: skip {FLIP_ADX_LO:.0f}-{FLIP_ADX_HI:.0f} dead zone)",
                      flush=True)
                return
            flip_token = w["up_token"] if fside == "Up" else w["down_token"]
            if flip_oneshot:
                stake_usd = _oneshot_stake_usd("flip")
            else:
                # 24h-daily-compounding flip book: FLIP_STAKE_FRAC of the flip pool's
                # daily-locked capital (self.flip_base), honouring the exact fraction.
                stake_usd = round(self.flip_base * FLIP_STAKE_FRAC, 2)
                if stake_usd < MIN_ORDER_USD:
                    return                       # below PM order floor — skip this flip
            # The flip's entry rule is a validated PRICE CAP, not a slippage
            # tolerance (2026-07-15): a fill at 0.50 pays 2x, so ANY fill
            # <= FLIP_STAKE_MAX_ASK is the acceptable win — send at the cap and
            # take whatever the book gives. FAK sweeps from the best ask upward,
            # so a 0.38 book still fills at 0.38; the cap only stops us walking
            # away from 0.41-0.50 fills that are +EV by the same sweep. The old
            # ask+BUY_SLIP rule was wrong at BOTH ends — it refused that headroom
            # (ask 0.38 -> limit 0.41) AND let an ask at the cap fill at 0.53,
            # inside the 0.53-0.58 dead zone. No resend on a no-match kill: the
            # limit is already the cap, so a retry buys no better price and would
            # fill preferentially when the flip-side price is falling BACK under
            # 0.50 — i.e. when the market is turning against the flip we called.
            # One-shot plumbing orders bypass the gate, so they keep ask+slip.
            flip_cap = FLIP_STAKE_MAX_ASK if flip_live else None
            avg, sh, ok, err = await self.executor.buy(
                flip_token, stake_usd, ask_flip, max_price=flip_cap)
            if not ok:
                print(f"[{datetime.now(timezone.utc):%H:%M:%S}] FLIP-LIVE "
                      f"{w['slug']} {fside} ${stake_usd:.2f} FAILED: {err}"
                      f"{' (still armed)' if flip_oneshot else ''}", flush=True)
            else:
                fill_px = avg if avg is not None else ask_flip
                idx = len(w["legs"])
                leg = self._new_leg(w, idx, is_t3=False)
                await _offload(self._stamp_signal, leg, p)
                leg["predicted_side"] = fside
                leg["token_id"] = str(flip_token)
                leg["entry_price"] = round(fill_px, 4)
                leg["shares"] = round(sh or 0.0, 2)
                leg["stake_usd"] = round((sh or 0.0) * fill_px, 2)
                # intended (pre-fill) size — a partial FAK fill leaves stake_usd below
                # this; the dash compares the two to label "partial $A/$B" vs full.
                leg["intended_stake_usd"] = round(stake_usd, 2)
                leg["staked"] = True
                leg["status"] = "OPEN"
                leg["book"] = "flip"
                w["legs"].append(leg)
                w["open_leg"] = leg          # held to settle; manage() skips flip
                w["staked_count"] += 1
                if flip_oneshot:
                    _consume_oneshot("flip")  # auto-off: never fire again unarmed
                upsert(leg)
                print(f"[{datetime.now(timezone.utc):%H:%M:%S}] FLIP-LIVE "
                      f"{w['slug']} {fside} ({p.get('label')}) -> {sh:.2f}sh "
                      f"@ {fill_px:.3f} (${leg['stake_usd']}) LIVE"
                      f"{' — one-shot consumed' if flip_oneshot else ' (flip book)'}"
                      f", held to settle", flush=True)

    async def _twap_lock(self, w, now):
        if not hasattr(self, '_twap_eval_lock'):
            self._twap_eval_lock = asyncio.Lock()
        if self._twap_eval_lock.locked():
            return
        async with self._twap_eval_lock:
            return await self._twap_lock_impl(w, now)

    async def _record_twap_skip(self, w, book, reason):
        """Persist a book's final rejection, without a stake or order claim."""
        idx = -1001 if book == 'twap_edge' else -1002
        if any(l.get('leg') == idx for l in w['legs']):
            return
        leg = self._new_leg(w, idx, is_t3=False)
        diag = w.get('twap_quote_diagnostic' if book == 'twap_edge' else 'twap_lock_diagnostic', {})
        leg.update(book=book, status='NO_STAKE', no_stake_reason=reason,
                   predicted_side=diag.get('side'), entry_price=diag.get('ask'),
                   decision_bps=diag.get('gap'), price_decision=diag.get('estimate'))
        w['legs'].append(leg)
        await _offload(upsert, leg)

    async def _twap_lock_impl(self, w, now):
        """TWAP-LOCK book: one hold-to-settle taker buy of the settlement-TWAP
        leader in [T0, T1). See twap_lock_estimate() for the mechanism and its
        validation. SHADOW unless `twap_lock_live` is on AND the bot is live;
        stake from the crossbook ledger (`twap_lock_capital_usd`/`_stake_usd`)."""
        if w.get("twap_lock_done") or ASSET != "BTC":
            return
        elapsed = int(now - w["window_anchor"])
        if not (TWAP_LOCK_T0_S <= elapsed < TWAP_LOCK_T1_S):
            return
        pb = w.get("price_open")
        r = await _offload(twap_lock_estimate, w["window_anchor"])
        if not pb or r is None:
            return                    # retry next tick while still in [T0, T1)
        est, newest_ts, coverage = r
        gap = (est / pb - 1.0) * 1e4
        side = "Up" if gap >= 0 else "Down"
        token = w["up_token"] if side == "Up" else w["down_token"]
        w['twap_lock_diagnostic'] = dict(side=side, gap=gap, estimate=est)
        if token is None:
            return
        lv = None
        touch, rungs = fade_live_touch(self.bookfeed, token)
        if touch is None and abs(gap) >= TWAP_LOCK_MIN_BPS:
            # WS cache is often cold/stale this late in the window; REST snapshot.
            try:
                lv = await self.book_levels(token)
                if lv:
                    rungs = [(float(p), float(s)) for p, s in lv[1]
                             if float(s) > 0 and float(p) > 0]
                    touch = fade_clob_touch(rungs)
            except Exception as e:
                _err("twap_lock.book", e)
        why = None
        if abs(gap) < TWAP_LOCK_MIN_BPS:
            why = f"|gap| {abs(gap):.2f} < {TWAP_LOCK_MIN_BPS:g}bps"
        elif touch is None:
            why = __import__('pm_quote_diagnostics').diagnose(lv, self.bookfeed, token)
        elif touch < TWAP_LOCK_MIN_PX:
            why = f"ask {touch:.3f} < floor {TWAP_LOCK_MIN_PX:.2f} (book disagrees)"
        elif touch > TWAP_LOCK_MAX_PX:
            why = f"ask {touch:.3f} > cap {TWAP_LOCK_MAX_PX:.2f}"
        if why is not None:
            # A too-small gap can still grow before T1; the price gates too. Log
            # once per window, decide at the last tick inside the band.
            if now + POLL_S < w["window_anchor"] + TWAP_LOCK_T1_S:
                return
            w["twap_lock_done"] = True
            await self._record_twap_skip(w, 'twap_lock', f't={elapsed}s gap {gap:+.2f}bps {side} · {why}')
            print(f"[{datetime.now(timezone.utc):%H:%M:%S}] TWAP-LOCK-SKIP "
                  f"{w['slug']} t={elapsed}s gap {gap:+.2f}bps {side} · {why}",
                  flush=True)
            return
        w["twap_lock_done"] = True
        await self._twap_take(
            w, "twap_lock", side, token, touch, rungs, TWAP_LOCK_MAX_PX, gap, est,
            elapsed, f"cov {coverage:.0%}")

    async def _twap_take(self, w, book, side, token, touch, rungs, cap, gap, est,
                         elapsed, note):
        """Shared one-shot taker buy + hold-to-settle leg for the TWAP books.
        Same live/shadow/crossbook path as the swingband books."""
        live = (self.live and getattr(self, "executor", None) is not None
                and book_stakes_now(book, False, w["window_start_utc"]))
        quote_observed_at = time.time()
        cached_book = getattr(getattr(self, "bookfeed", None), "_books", {}).get(str(token), {})
        cached_received_at = cached_book.get("ts")
        cached_asks = cached_book.get("asks", {})
        # Only attach WS receipt time if its current best ask matches this decision.
        cached_best = min((p for p, size in cached_asks.items() if size > 0), default=None)
        quote_book_received_at = cached_received_at if cached_best is not None and abs(cached_best-touch) < 1e-9 else None
        # Read venue cash for every TWAP decision; never use paper equity as cash.
        try:
            if getattr(self, "executor", None) is None:
                raise ValueError("PM balance reader unavailable")
            wallet = await self.executor.usdc_balance()
            if wallet is None or not math.isfinite(float(wallet)) or float(wallet) < MIN_ORDER_USD:
                raise ValueError("PM balance unavailable or below minimum ticket")
            self._live_usdc = float(wallet)
            self._live_usdc_ts = time.time()
        except Exception as exc:
            _err("twap_wallet_sizing", exc)
            return
        stake = MIN_ORDER_USD
        live_edge_ladder = live and book == "twap_edge" and str(get_config("twap_edge_stake_ladder", "0")) == "1"
        if live_edge_ladder:
            from pm_twap_shadow_ladder import capital_and_stake
            _, stake = capital_and_stake(self._live_usdc, [],
                float(get_config("twap_edge_stake_ladder_step_usd", "10")),
                float(get_config("twap_edge_stake_ladder_max_usd", "250")), 1.)
            cb = None
        else:
            cb = crossbook_stake_for(book, self._live_usdc)
        if cb is not None:
            stake = cb
            if stake < MIN_ORDER_USD:
                live = False
        paper_ladder = (DRY_RUN and not getattr(self, "live", False) and ASSET == "BTC" and book in ("twap_edge", "twap_lock")
                        and str(get_config(f"{book}_shadow_ladder", "0")) == "1")
        if paper_ladder:
            try:
                from pm_twap_shadow_ladder import capital_and_stake
                with ENGINE.connect() as conn:
                    paper_rows = conn.execute(text(
                        "SELECT entry_price, shares, hit FROM polymarket_updown_trades "
                        "WHERE dry_run IS TRUE AND book IN ('twap_edge','twap_lock') "
                        "AND hit IS NULL AND no_stake_reason LIKE 'TWAP-WALLET-SHADOW %' "
                        "ORDER BY decided_at, id")).all()
                capital, stake = capital_and_stake(
                    self._live_usdc, paper_rows,
                    float(get_config(f"{book}_stake_ladder_step_usd", "10")),
                    float(get_config(f"{book}_stake_ladder_max_usd", "250" if book == "twap_edge" else "0")),
                    float(get_config(f"{book}_shadow_stake_per_tier", "1")))
                live = False
                if stake < MIN_ORDER_USD:
                    print(f"TWAP-PAPER-LADDER-SKIP insufficient capital ${capital:.2f}", flush=True)
                    return
            except Exception as exc:
                _err("twap_shadow_ladder", exc)
                return
        if stake < MIN_ORDER_USD or stake * (1 + POLY_FEE_RATE) > self._live_usdc:
            print(f"TWAP-WALLET-SKIP stake ${stake:.2f} exceeds available wallet ${self._live_usdc:.2f} with fee reserve", flush=True)
            return
        if book == "twap_edge":
            try:
                import sys
                sys.path.insert(0, ".")
                import pm_twap_presend
                prepared = await pm_twap_presend.prepare(
                    self, w, side, str(token),
                    w.get('twap_entry_quality') or twap_quality.load(), sys.modules[__name__])
                touch, cap, rungs = prepared['ask'], prepared['cap'], prepared['asks']
                gap, est, elapsed = prepared['gap'], prepared['estimate'], prepared['elapsed']
                w['twap_calibration'] = prepared['sample']
                quote_observed_at = prepared['sample']['observed_at']
                quote_book_received_at = prepared['quote']['received_at']
                note = f"p {prepared['probability']:.3f} edge {prepared['sample']['fee_net_edge_at_cap']:+.3f}"
                print("TWAP-EDGE-PRESEND " + json.dumps(dict(
                    **dict(prepared['sample'], anchor=w['window_anchor'], side=side,
                           quote=prepared['quote']))), flush=True)
            except Exception as exc:
                w['twap_edge_why'] = "TWAP pre-send " + str(exc)
                print(f"TWAP-EDGE-PRESEND-SKIP {w['slug']} {w['twap_edge_why']}", flush=True)
                return
        sh_m, usd_m, avg_m, frac_m = sweep_ladder(rungs, stake, cap)
        if (paper_ladder or (live and book == "twap_edge")) and (avg_m is None or frac_m < .95):
            print(f"TWAP-PAPER-LADDER-SKIP insufficient depth ${stake:.2f}", flush=True)
            return
        leg = self._new_leg(w, len(w["legs"]), is_t3=False)
        leg["book"] = book
        leg["predicted_side"] = side
        leg["token_id"] = str(token)
        leg["decision_bps"] = round(gap, 3)
        # TWAP uses its own probability model. The common tier is a gap-size
        # label only; do not attach T3's historical accuracy to a TWAP entry.
        leg["tier"] = tier_for(abs(leg["decision_bps"]))[0]
        leg["price_decision"] = round(est, 4)
        leg["intended_stake_usd"] = round(stake, 2)
        leg["twap_decision_ask"] = touch
        leg["twap_quote_observed_at"] = quote_observed_at
        leg["twap_book_received_at"] = quote_book_received_at
        if live:
            # EDGE's common fresh-depth/signal admission above applies to both modes.
            execution_audit = None
            if book == "twap_edge":
                import pm_execution_audit
                execution_audit = dict(anchor=w['window_anchor'], leg=leg['leg'],
                    side=side, token_id=str(token), stake_usd=stake, cap=cap,
                    decision_ask=touch, quote_received_at=quote_book_received_at,
                    quote_checked_at=quote_observed_at, signal=w.get('twap_calibration'),
                    shadow_vwap=avg_m, shadow_fill_fraction=frac_m)
                try:
                    pm_execution_audit.append('before_submit',execution_audit)
                except Exception as exc:
                    _err('pm_execution_audit',exc)
                    return  # No live order without its durable intent audit.
            _avg, _sh, _ok, _why = await self.executor.buy(
                token, stake, touch, max_price=cap,
                **({"max_spend": round(stake * (1 + POLY_FEE_RATE), 2),
                    "before_post": lambda: pm_twap_presend.final_check(
                        self, w, side, str(token),
                        w.get('twap_entry_quality') or twap_quality.load(),
                        sys.modules[__name__], prepared, stake)} if book == "twap_edge" else {}))
            if execution_audit is not None:
                timing = getattr(self.executor,'last_buy_timing',{})
                try:
                    pm_execution_audit.append('order_response',dict(execution_audit,
                        timing=timing, ok=_ok, rejection_reason=_why,
                        fill_price=_avg, shares=_sh,
                        fill_notional=(_avg*_sh if _avg is not None and _sh else 0),
                        fill_fraction=(_avg*_sh/stake if _avg is not None and _sh else 0),
                        entry_fee_usd=(_taker_fee(_sh,_avg) if _avg is not None and _sh else 0),
                        slippage_cents=((_avg-touch)*100 if _avg is not None else None),
                        slippage_bps=((_avg/touch-1)*10000 if _avg is not None else None),
                        quote_to_post_ms=((timing['post_started_at']-quote_observed_at)*1000 if timing.get('post_started_at') else None)))
                except Exception as exc:
                    _err('pm_execution_audit_response',exc)  # Keep registering the real fill.
                if timing.get('order_id'):
                    asyncio.create_task(pm_execution_audit.match_timestamp(
                        self.executor.c,timing['order_id'],token,w['window_anchor']))
            if _ok and _sh:
                leg["entry_price"] = round(_avg or touch, 4)
                leg["shares"] = floor_shares(_sh)
                leg["stake_usd"] = round(_sh * (_avg or touch), 2)
                leg["entry_fee_usd"] = round(_taker_fee(_sh, _avg or touch), 4)
                leg["staked"] = True
                leg["status"] = "OPEN"
                w["hold_legs"].append(leg)
                w["staked_count"] += 1
                leg["twap_fill_cut"] = (
                    book == "twap_edge" and side == "Down"
                    and touch - (_avg or touch) >= TWAP_DOWN_POSTFILL_MAX_DROP - 1e-9)
            else:
                leg["entry_price"] = round(touch, 4)
                leg["shares"] = 0.0
                leg["stake_usd"] = 0.0
                leg["status"] = "NO_FILL"
                leg["no_stake_reason"] = f"live order rejected: {_why}"
        else:
            leg["entry_price"] = round(avg_m or touch, 4)
            leg["shares"] = floor_shares(sh_m)
            leg["stake_usd"] = round(usd_m, 2)
            leg["status"] = "SHADOW"
            leg["no_stake_reason"] = (
                f"{book} SHADOW · t={elapsed}s gap {gap:+.2f}bps · {note} · "
                f"sweep {frac_m:.0%} of ${stake:.2f} @ {(avg_m or touch):.4f}"
                + (" · bot DRY-RUN" if not self.live else " · book switch off"))
        if paper_ladder:
            leg["entry_fee_usd"] = round(_taker_fee(leg["shares"], leg["entry_price"]), 4)
            leg["no_stake_reason"] = "TWAP-WALLET-SHADOW " + leg["no_stake_reason"]
            leg["no_stake_reason"] += " · policy " + str(w.get('twap_entry_quality', {}).get('version', 'base') if book == "twap_edge" else "twap_lock_baseline_v1")
        w["legs"].append(leg)
        upsert(leg)
        if book == 'twap_edge' and w.get('twap_calibration'):
            try:
                await _offload(twap_cal.append_json, twap_cal.AUDIT,
                               dict(event='entry', anchor=w['window_anchor'],
                                    leg=leg['leg'], calibration=w['twap_calibration'],
                                    filled_price=leg.get('entry_price'), shares=leg.get('shares'),
                                    status=leg['status'], live=live,
                                    no_stake_reason=leg.get('no_stake_reason')))
            except Exception as e:
                _err('twap_calibration.entry_audit', e)
        if live and leg["status"] == "OPEN":
            if leg.get("twap_fill_cut"):
                await self._twap_cut(w, leg)
            if leg["status"] == "OPEN" and not leg.get("twap_fill_cut"):
                asyncio.create_task(self._reconcile_stake(leg, str(token), _sh))
        print(f"[{datetime.now(timezone.utc):%H:%M:%S}] "
              f"{book.upper().replace('_', '-')}-{'LIVE' if live else 'SHADOW'} "
              f"{w['slug']} {side} t={elapsed}s gap {gap:+.2f}bps ask {touch:.3f} "
              f"cap {cap:.3f} · {note} -> {leg['status']} ${leg['stake_usd']}",
              flush=True)

    async def _twap_cut(self, w, leg):
        """Sell flagged Down fills immediately; rejected FOKs remain tracked."""
        if (not self.live or leg.get("status") != "OPEN"
                or leg.get("book") != "twap_edge"
                or leg.get("predicted_side") != "Down"
                or not leg.get("twap_fill_cut")
                or time.time() >= leg["window_anchor"] + WINDOW_S):
            return
        try:
            bids, _ = _levels(await self.client.get_order_book(
                token_id=str(leg["token_id"])))
            bids = [(p, s) for p, s in bids
                    if math.isfinite(p) and math.isfinite(s) and 0 < p < 1 and s > 0]
            if not bids:
                raise ValueError("no executable sell bid")
        except Exception as e:
            leg["twap_exit_error"] = f"quote unavailable: {type(e).__name__}"
            upsert(leg)
            return
        if time.time() >= leg["window_anchor"] + WINDOW_S:
            return
        # A crash/timeout after sending leaves this durable review marker, rather
        # than retrying a possibly executed sell or booking a phantom payout.
        leg["status"] = "EXIT_REVIEW"
        leg["twap_exit_error"] = "sell submitted; confirmation pending"
        upsert(leg)
        px, sold, result, err = await self.executor.sell_twap_cut(
            leg["token_id"], leg["shares"], bids[0][0])
        if result == "filled":
            leg["exit_reason"] = "TWAP_FILL_DROP"
            leg["exit_price"] = round(px, 6)
            leg["exit_at"] = datetime.now(timezone.utc)
            leg["realized_pnl_usd"] = round(
                sold * px - _taker_fee(sold, px)
                - leg["stake_usd"] - (leg.get("entry_fee_usd") or 0), 4)
            leg["status"] = "CLOSED"
            leg["twap_exit_error"] = None
            leg["no_stake_reason"] = "Early fill-drop exit; settlement audit pending"
        else:
            leg["status"] = "OPEN" if result == "retry" else "EXIT_REVIEW"
            leg["twap_exit_error"] = err
            if result != "retry":
                self._twap_exit_review = True
                leg["no_stake_reason"] = f"TWAP exit requires review: {err}"
        upsert(leg)
        print(f"[{datetime.now(timezone.utc):%H:%M:%S}] TWAP-FILL-DROP "
              f"{w['slug']} L{leg['leg']} decision {leg.get('twap_decision_ask')} "
              f"fill {leg['entry_price']} -> {leg['status']} "
              f"exit {leg.get('exit_price')} pnl {leg.get('realized_pnl_usd')} "
              f"{err or ''}", flush=True)

    async def _retry_twap_cuts(self, now):
        """Recover pending cuts from DB, including after a restart."""
        if not self.live or now - getattr(self, "_twap_cut_poll", 0) < 5:
            return
        self._twap_cut_poll = now
        def read_pending():
            with ENGINE.connect() as c:
                return [dict(r) for r in c.execute(text(f"""
                    SELECT * FROM {TRADES_TABLE}
                     WHERE asset=:asset AND NOT dry_run AND staked
                       AND book='twap_edge' AND predicted_side='Down'
                       AND twap_fill_cut IS TRUE
                       AND (status='EXIT_REVIEW' OR
                            (status='OPEN' AND window_anchor + :win > :now))
                """), {"asset": ASSET, "win": WINDOW_S, "now": now}).mappings()]
        try:
            rows = await _offload(read_pending)
            self._twap_exit_review = any(r["status"] == "EXIT_REVIEW" for r in rows)
            for row in rows:
                if row["status"] == "EXIT_REVIEW":
                    continue
                w = self.windows.get(row["window_anchor"])
                leg = next((l for l in w["legs"] if l["leg"] == row["leg"]), None) if w else None
                await self._twap_cut(w or {"slug": row["slug"]}, leg or row)
        except Exception as e:
            _err("twap_cut.retry", e)

    async def _twap_edge(self, w, now):
        if not hasattr(self, '_twap_eval_lock'):
            self._twap_eval_lock = asyncio.Lock()
        if self._twap_eval_lock.locked():
            return
        async with self._twap_eval_lock:
            return await self._twap_edge_impl(w, now)

    async def _twap_edge_impl(self, w, now):
        """TWAP-EDGE book: at T+240..T+262 buy the settlement-TWAP leader when the
        rolling qualified probability beats ask, fees and execution stress.
        Unqualified models still collect candidates, but never enter."""
        if w.get("twap_edge_done") or ASSET != "BTC":
            return
        if getattr(self, "_twap_exit_review", False):
            w["twap_edge_why"] = "prior TWAP sell requires execution review"
            return
        elapsed = int(now - w["window_anchor"])
        if not (TWAP_EDGE_T0_S <= elapsed < TWAP_EDGE_T1_S):
            return
        pb = w.get("price_open")
        w["twap_edge_why"] = f"t={elapsed}s no price_open"
        if not pb:
            return
        w["twap_edge_why"] = f"t={elapsed}s feed stale/thin"
        r = await _offload(twap_lock_estimate, w["window_anchor"])
        if r is None:
            ref = twap_cal.LAST_REFUSAL
            if ref:
                w["twap_edge_why"] = f"t={elapsed}s feed stale/thin {ref.get('reason')} {ref}"
            return  # Never feed a raw spot gap into a settlement-TWAP model.
        est, _k, _cov = r
        gap = (est / pb - 1.0) * 1e4
        side = "Up" if gap >= 0 else "Down"
        token = w["up_token"] if side == "Up" else w["down_token"]
        w['twap_quote_diagnostic'] = dict(side=side, gap=gap, estimate=est)
        if token is None:
            return
        from pm_twap_policy import for_anchor
        quality_policy = w.setdefault('twap_entry_quality',
                                      for_anchor(twap_edge_policy(), w['window_anchor']))
        self._twap_effective_quality = quality_policy
        lv = None
        touch, rungs = fade_live_touch(self.bookfeed, token)
        if touch is None:
            try:
                lv = await self.book_levels(token)
                if lv:
                    rungs = [(float(a), float(b)) for a, b in lv[1]
                             if float(b) > 0 and float(a) > 0]
                    touch = fade_clob_touch(rungs)
            except Exception as e:
                _err("twap_edge.book", e)
        if quality_policy.get('enabled'):
            # Read bid and ask from the same fresh snapshot. Pre-send REST checks
            # the rule again, so an old tight WS spread cannot authorize a fill.
            if lv is None and self.bookfeed is not None:
                lv = self.bookfeed.levels(token)
            if lv is None:
                try:
                    lv = await self.book_levels(token)
                except Exception as e:
                    _err('twap_quality.book', e)
            if lv:
                rungs = [(float(p), float(s)) for p, s in lv[1]
                         if math.isfinite(float(p)) and 0 < float(p) < 1 and float(s) > 0]
                touch = fade_clob_touch(rungs)
        quality_bid = twap_quality.best_bid(lv)
        quality_ok, quality_reason = twap_quality.evaluate(quality_policy, gap, touch, quality_bid)
        from pm_quote_diagnostics import diagnose
        quote_state = diagnose(lv, self.bookfeed, token)
        w['twap_quote_diagnostic'] = dict(observed_at=time.time(), side=side, token=str(token), state=quote_state, ask=touch, bid=quality_bid, gap=gap, estimate=est)
        if quality_reason == 'quality_quote_missing':
            quality_reason = 'quality_' + quote_state
        if "twap_edge_max_px" not in w:
            try:
                w["twap_edge_max_px"] = float(
                    get_config("twap_edge_max_px", TWAP_EDGE_MAX_PX))
            except (TypeError, ValueError):
                w["twap_edge_max_px"] = TWAP_EDGE_MAX_PX
        max_px = quality_policy.get('shadow_max_ask', w["twap_edge_max_px"])
        min_px = quality_policy.get('shadow_min_ask', TWAP_EDGE_MIN_PX)
        min_edge = quality_policy.get('shadow_min_edge', TWAP_EDGE_MIN_EDGE)
        sample = dict(anchor=w['window_anchor'], observed_at=time.time(),
                      elapsed=elapsed, price_open=pb, estimate=est, gap=gap,
                      side=side, ask=touch, coverage=_cov, feed_ts=_k,
                      source='projected_twap', quality_bid=quality_bid,
                      quality_reason=quality_reason, quality_policy=quality_policy)
        if touch is not None:
            _, _, fill_price, fill_frac = sweep_ladder(
                rungs, MIN_ORDER_USD, min(max_px, touch+.01))
            sample.update(fill_price=fill_price or touch, fill_frac=fill_frac)
        if touch is not None and .01 < touch < .99:
            # Log each elapsed bucket independently of whether trading is enabled.
            bucket = elapsed // 10
            seen = w.setdefault('twap_calibration_buckets', set())
            if bucket not in seen:
                try:
                    await _offload(twap_cal.append_json, twap_cal.SAMPLES, sample)
                    seen.add(bucket)
                except Exception as e:
                    _err('twap_calibration.sample', e)
                    w['twap_edge_why'] = 'calibration sample write failed'
                    return
        if not quality_ok:
            w['twap_edge_why'] = quality_reason
            return
        model = getattr(self, '_twap_model', {})
        calibrated_p, calibration_status = None, 'disabled'
        if TWAP_CONTINUOUS_CALIBRATION:
            if touch is None:
                w['twap_edge_why'] = 'no ask'
                return
            calibrated_p, calibration_status = twap_cal.predict(model, sample, time.time())
        calibration_required = bool(getattr(self, 'live', False)) or (
            TWAP_CONTINUOUS_CALIBRATION and TWAP_CALIBRATION_ENFORCE)
        if calibration_required:
            p = calibrated_p
            if p is None:
                w['twap_edge_why'] = ('continuous calibration required for funded entry'
                                      if not TWAP_CONTINUOUS_CALIBRATION else calibration_status)
                return
        else:
            p = twap_edge_p(elapsed, abs(gap))
            if p is None:
                return
        w['twap_calibration'] = dict(sample, model_id=model.get('model_id'),
                                    probability=p, calibrated_probability=calibrated_p,
                                    calibration_status=calibration_status,
                                    calibration_enforced=calibration_required,
                                    calibration_enabled=TWAP_CONTINUOUS_CALIBRATION)
        cost = .02 if calibration_required else 0.
        edge = (None if touch is None
                else p - touch - POLY_FEE_RATE * touch * (1 - touch) - cost)
        why = None
        if touch is None:
            why = "no ask"
        elif not (min_px <= touch <= max_px):
            why = f"ask {touch:.3f} outside {min_px:.2f}-{max_px:.2f}"
        elif edge <= min_edge:
            why = f"edge {edge:+.3f} <= {min_edge:.2f}"
        if why is not None:
            # re-check every tick until T1; the call site logs the last look
            w["twap_edge_why"] = (f"t={elapsed}s {side} gap {gap:+.2f}bps "
                                  f"p {p:.3f} · {why}")
            return
        try:
            import sys
            sys.path.insert(0, ".")
            from pm_twap_policy import admit
            cap, _ = admit(p, sample, quality_policy, twap_quality, POLY_FEE_RATE, cost)
        except ValueError as exc:
            w['twap_edge_why'] = str(exc)
            return
        tier_ok, tier_reason, tier_details = twap_quality.evaluate_probability(
            quality_policy, p, cap, POLY_FEE_RATE)
        w['twap_calibration']['win_rate_tier'] = tier_details
        if not tier_ok:
            w['twap_edge_why'] = tier_reason
            return
        w["twap_edge_done"] = True
        await self._twap_take(
            w, "twap_edge", side, token, touch, rungs, cap, gap, est, elapsed,
            f"p {p:.3f} edge {edge:+.3f}")

    async def _t2_probe(self, w, now):
        """T+2 -> T+3 shadow probe: evaluate the continuation entry BEFORE the live
        T+3 decision and record each distinct would-enter state to the probe table.
        `plan_entry` is pure (never orders, never mutates the window); we also never
        stake, never touch open_leg/t3_done/staked_count/adx here. Dedupes on the
        entry-relevant state so a single window emits a few rows (transitions), not
        one per poll — capturing the moment(s) an entry would be 'sure' (would_stake)
        and the ask at that time. Fully isolated: callers wrap this in try/except."""
        p = await self.plan_entry(w, now)

        # FIRST-TOUCH LATCH — must run BEFORE the no-material-change dedup below, or
        # a quiet book (unchanged asks poll after poll) would be counted once and the
        # window would fail the coverage floor for want of ticks that did happen.
        # Counts every pre-band poll we actually saw and latches whether the CHEAP
        # side ever traded at/below the ceiling. This is the live half of the
        # SWINGFADE_FIRST_TOUCH_CEIL condition; see swingfade_ok().
        # Records the MINIMUM cheap-side ask seen, NOT a ceiling comparison: the
        # threshold is applied once at decision time in swingfade_ok(). get_config()
        # is an uncached DB round trip, and calling it here would put one on a ~1/s
        # path inside the event loop — the exact blocking-call shape that starved the
        # WS book for 45 minutes on 2026-07-28. Latching the raw minimum also means a
        # mid-window ceiling change re-reads correctly instead of using a stale latch.
        _ft_elapsed = now - w["window_anchor"]
        if _ft_elapsed < SWINGFADE_ELAPSED_MIN:
            _au, _ad = p.get("ask_up"), p.get("ask_down")
            if _au is not None or _ad is not None:
                w["sf_pre_ticks"] = w.get("sf_pre_ticks", 0) + 1
                _cheap = min(x for x in (_au, _ad) if x is not None)
                _prev = w.get("sf_min_pre_ask")
                if _prev is None or _cheap < _prev:
                    w["sf_min_pre_ask"] = _cheap

        # EARLY-MAKER PROBE — fires BEFORE the no-material-change dedup below, so a
        # quiet book (exactly the case we want to measure) can never skip it. Once
        # per window, telemetry only. See EARLY_MAKER_PROBE_S.
        _em_elapsed = now - w["window_anchor"]
        if (EARLY_MAKER_PROBE_S > 0 and SHADOW_FILL and self.bookfeed is not None
                and not w.get("early_maker_probed")
                and _em_elapsed >= EARLY_MAKER_PROBE_S):
            # Latch on the FIRST poll past the threshold whether or not we fire, so a
            # window we joined late (restart mid-window) is skipped outright instead
            # of being probed near T+3 -- that is the saturated book this probe
            # exists to avoid, and it would silently contaminate the dataset.
            w["early_maker_probed"] = True
            if _em_elapsed <= EARLY_MAKER_PROBE_S + EARLY_MAKER_PROBE_MAX_LATE:
                # Probe BOTH sides off w's tokens, NOT p["token"] -- p["token"] is
                # only populated once the signal QUALIFIES, so on the quiet early
                # book this probe exists to measure it is always None. Both sides
                # also give the quantity that actually matters: resting on both
                # costs bid_up+bid_down (~0.99) and pays exactly $1, so the sum is
                # the real maker capture (~the spread). One side alone just
                # measures whichever way BTC happened to drift.
                for _side, _tok, _bid, _ask in (
                        ("Up", w.get("up_token"), p.get("bid_up"), p.get("ask_up")),
                        ("Down", w.get("down_token"), p.get("bid_down"), p.get("ask_down"))):
                    if _tok is None or _bid is None or _ask is None:
                        continue
                    asyncio.create_task(self._shadow_fill_log(
                        _tok, "BUY", MIN_SHARES, _bid, _ask,
                        {"slug": w["slug"], "leg": -1, "kind": "early_maker",
                         "probe_s": round(_em_elapsed, 1), "side_probed": _side,
                         "tier": p.get("label"), "dbps": p.get("dbps"),
                         "window_anchor": w["window_anchor"],
                         "bid_up": p.get("bid_up"), "ask_up": p.get("ask_up"),
                         "bid_down": p.get("bid_down"), "ask_down": p.get("ask_down")}))

        def _r(x):
            return round(x, 2) if isinstance(x, (int, float)) else None
        key = (p.get("side"), p.get("label"), bool(p.get("ok")),
               _r(p.get("best_ask")), _r(p.get("ask_up")), _r(p.get("ask_down")),
               _r(p.get("bid_up")), _r(p.get("bid_down")))
        if key == w.get("t2_last_key"):
            return                                   # no material change -> skip
        w["t2_last_key"] = key
        seq = w["t2_seq"]
        w["t2_seq"] = seq + 1
        # PERSISTENCE SWITCH (2026-08-04): the T+90s early-entry question this
        # table was built to answer is CLOSED (not viable), so the ~25k rows/day it
        # writes are dead weight. Everything ABOVE this point still runs — the
        # first-touch latch (sf_pre_ticks / sf_min_pre_ask) and the early-maker probe
        # are LIVE state feeding swingfade_ok(), NOT calibration. Do NOT reach for
        # POLYMARKET_T2_PROBE=0 to stop the rows: that skips the whole _t2_probe call
        # at the tick site, sf_pre_ticks never increments, and swingfade_ok() then
        # fails closed on "pre-band coverage 0 < 20" — the book silently stops firing
        # and looks like it merely never qualified. This flag is the only safe knob.
        # NOTE: turning persistence off also ends pm_breach30_shadow, which
        # reconstructs the first 30s of each window from these rows.
        if not T2_PROBE_PERSIST:
            return
        upsert_t2_probe({
            "asset": ASSET,
            "window_anchor": w["window_anchor"],
            "seq": seq,
            "window_start_utc": w["window_start_utc"],
            "slug": w["slug"],
            "dry_run": DRY_RUN,
            "decided_at": datetime.now(timezone.utc),
            "elapsed_s": int(now - w["window_anchor"]),
            "predicted_side": p.get("side"),
            "tier": p.get("label"),
            "tier_acc": p.get("acc"),
            "decision_bps": p.get("dbps"),
            "price_open": w["price_open"],
            "price_decision": p.get("price_now"),
            "ask_up": p.get("ask_up"),
            "ask_down": p.get("ask_down"),
            "bid_up": p.get("bid_up"),
            "bid_down": p.get("bid_down"),
            "best_ask": p.get("best_ask"),
            "would_stake": bool(p.get("ok")),
            "reason": p.get("reason"),
        })

    async def try_enter(self, w, now, is_t3):
        """Open a new leg when the current signal qualifies (subject to the
        per-window cap). On the T+3 pass, also record a non-staked evaluation
        leg when it doesn't qualify, so the signal's accuracy is tracked even
        when we don't trade."""
        if t3_entries_stopped():
            w['t3_done'] = True
            w['t3_no_stake'] = False
            return
        p = await self.plan_entry(w, now, is_t3=is_t3)
        if is_t3:
            self._tmark("plan")
        idx = len(w["legs"])
        # T3 primary-only: a later plan refresh must remain observable, but can
        # never reach any staking route. Put this before collect-only, cap, fade
        # and continuation branches so the invariant is fail-closed.
        if T3_BOOK_ON and T3_BOOK_PRIMARY_ONLY and not is_t3:
            await self._maybe_log_revision(
                w, p, force=True,
                block_reason=f"{ASSET} T3 primary-only: revision shadow")
            return
        # ONE t3 stake per window (2026-07-29). The shared staked_count is no
        # longer the right cap: swingfade increments it too, so a swingfade fill used
        # to eat into the t3 book's allowance and vice versa. The two books are
        # independent and each gets exactly one shot — swingfade keeps its own
        # `swingfade_done` flag, and this is the t3 equivalent. The legacy
        # MAX_ENTRIES_PER_WINDOW re-entry budget still applies when the book is off.
        capped = (w["t3_staked"] if T3_BOOK_ON
                  else w["staked_count"] >= MAX_ENTRIES_PER_WINDOW)

        if self.collect_only:
            # collect-only: record the eval leg + fire the fill/reversal probe on
            # the would-be entry (and MODERATE), never trade / never record P&L.
            # Keeps direction-accuracy, observed-ask and reversal-signal data
            # flowing whether trading is off (trading_enabled=0) or DATA_ONLY.
            if is_t3 or p["ok"]:
                leg = self._new_leg(w, idx, is_t3=is_t3)
                await _offload(self._stamp_signal, leg, p)
                leg["status"] = "NO_STAKE"
                leg["no_stake_reason"] = (
                    ("data-only" if DATA_ONLY else "trading-off")
                    if p["ok"] else p["reason"])
                w["legs"].append(leg)
                upsert(leg)
                if SHADOW_FILL and self.bookfeed is not None:
                    if p["ok"] and p.get("token") is not None:
                        asyncio.create_task(self._shadow_fill_log(
                            p["token"], "BUY", p.get("shares") or MIN_SHARES,
                            p["best_bid"], p["best_ask"],
                            {"slug": w["slug"], "leg": idx, "kind": "entry",
                             "dir": p["side"], "tier": p["label"],
                             "assumed_px": round(p.get("best_bid") or 0, 3),
                             "gate_mode": MAKER_FILL_GATE,
                             "pred_fillable": p.get("maker_fillable"),
                             "queue_ahead": p.get("queue_ahead"),
                             "vol_rate": p.get("vol_rate"),
                             "gate_would_block": p.get("gate_would_block")}))
                    if p.get("mod_token") is not None:
                        asyncio.create_task(self._shadow_fill_log(
                            p["mod_token"], "BUY", MIN_SHARES, p["mod_bid"],
                            p["mod_ask"],
                            {"slug": w["slug"], "leg": idx, "kind": "moderate",
                             "dir": p["side"], "mod_ask": round(p["mod_ask"], 3)}))
            elif not is_t3:
                await self._maybe_log_revision(w, p)
            return

        if not p["ok"] or capped:
            if is_t3:
                leg = self._new_leg(w, idx, is_t3=True)
                await _offload(self._stamp_signal, leg, p)
                leg["status"] = "NO_STAKE"
                leg["no_stake_reason"] = (
                    "max entries" if (p["ok"] and capped) else p["reason"])
                w["legs"].append(leg)
                upsert(leg)
                # MODERATE fade shadow: probe whether our predicted side would
                # FILL as a maker (= T+3 call failing / reversal). Reuses the
                # existing fill probe; logged kind="moderate". The fade P&L is
                # derived at analysis: fade the cheap opposite iff it filled,
                # which wins iff the window's hit is False (reversal materialised).
                if (SHADOW_FILL and self.bookfeed is not None
                        and p.get("mod_token") is not None):
                    asyncio.create_task(self._shadow_fill_log(
                        p["mod_token"], "BUY", MIN_SHARES, p["mod_bid"], p["mod_ask"],
                        {"slug": w["slug"], "leg": idx, "kind": "moderate",
                         "dir": p["side"], "mod_ask": round(p["mod_ask"], 3)}))
            elif not capped:
                await self._maybe_log_revision(w, p)
            # EXPERIMENTAL ~9:1 high-fade: expensive-ask reversal -> buy cheap opposite
            # NOT on a calendar-blocked day: hfade records `staked=True` off a
            # modelled fill when it isn't live, and a calendar block newly routes
            # would-be t3 entries down this branch — so without this guard the rule
            # would MANUFACTURE phantom staked rows on exactly the days it exists to
            # keep us flat. hfade's own order call is calendar-gated regardless.
            if HIGH_FADE and TWO_BOOK and not capped and not p.get("dow_blocked"):
                await self._try_high_fade(w, now, p)
            return

        if TWO_BOOK:
            # MAKER-FILL MEASUREMENT (): fire the read-only fill probe on our
            # predicted-side token. It used to be stamped with the Binance momentum at
            # placement, to test whether Binance-ALIGNED maker fills are non-adverse —
            # i.e. whether the 3s lead could time a fee-free entry. That experiment is
            # retired (2026-07-30); the probe itself still measures fillability.
            if (SHADOW_FILL and self.bookfeed is not None
                    and p.get("token") is not None):
                asyncio.create_task(self._shadow_fill_log(
                    p["token"], "BUY", p.get("shares") or MIN_SHARES,
                    p["best_bid"], p["best_ask"],
                    {"slug": w["slug"], "leg": idx, "kind": "tb_maker",
                     "dir": p["side"], "tier": p["label"],
                     "reversal": p.get("reversal")}))
            # ---- ROUTE: reversal predicted -> FADE cheap opposite (flat, held to
            # settle); else -> TAKER the T+3 continuation (compounded, TP/SL). ----
            reversal = p.get("reversal")
            # The Binance-lead confirmation that used to veto a fade here is gone —
            # 2026-07-30. It WAS enforcing in this deployment (unit set
            # FADE_REQUIRE_BINANCE=1), so more fades now route here unconfirmed; see
            # the matching note in the hfade path. Shadow-only book.
            if reversal is True and (w.get("adx_short") or 0.0) < FADE_MIN_ADX_SHORT:
                reversal = False          # ADX floor: only fade a STRONG move
            if reversal is True and not self._fade_daily_ok():
                reversal = False          # 24h fade cap reached -> take T+3 instead
            if w.get("faded"):
                reversal = False          # one fade per window -> take T+3 instead
            if reversal is True:
                opp = w["down_token"] if p["side"] == "Up" else w["up_token"]
                fade_side = "Down" if p["side"] == "Up" else "Up"
                try:
                    ob, oa = await self.book_levels(int(opp))
                except Exception:
                    ob, oa = [], []
                if not oa or not ob:
                    return
                opp_ask, opp_bid = oa[0][0], ob[0][0]
                if not (MIN_ASK <= opp_ask <= MAX_ASK) or opp_ask - opp_bid > MAX_SPREAD:
                    return
                fsh, _ = self.size_position(None, opp_ask, book="fade")
                favg, ffilled = walk_fill(oa, fsh)
                if favg is None or ffilled < MIN_SHARES:
                    return
                # maker-preferred fade (FADE_MAKER) rests at the bid ($0 fee);
                # else taker at the walk-fill ask.
                entry_px = opp_bid if FADE_MAKER else favg
                # ONE-SHOT LIVE test override (default OFF): normally fade is model-
                # only when FADE_SHADOW is on, but an armed `fade_live_oneshot` lets
                # ONE small real fade order through to exercise the executor path,
                # then auto-disarms. Sizes to the tiny test USDC, not the book frac.
                # Calendar veto on BOTH, and the one-shot's precedence read off the
                # raw toggle — see the flip block for why the vetoed value must not
                # be what arms the one-shot.
                _fade_toggle = _book_live("fade", not FADE_SHADOW)
                _fade_dow_ok = dow_stake_ok(w["window_start_utc"])[0]
                _fade_live = _fade_toggle and _fade_dow_ok
                _fade_oneshot = (not _fade_toggle) and _fade_dow_ok and _oneshot_armed("fade")
                if _fade_oneshot:
                    _usd = _oneshot_stake_usd("fade")
                    ffilled = _usd / max(opp_bid if FADE_MAKER else favg, 1e-6)
                if self.live and (_fade_live or _fade_oneshot):  # else model only
                    _consensus_ok, _consensus_why = three_source_consensus_ok(
                        w["window_anchor"], fade_side)
                    if not _consensus_ok:
                        print(f"[{datetime.now(timezone.utc):%H:%M:%S}] FADE-SKIP "
                              f"{w['slug']} · {_consensus_why}", flush=True)
                        return
                    a2, s2, ok, err = (
                        await self.executor.buy_maker(opp, ffilled, opp_bid, opp_ask)
                        if FADE_MAKER else
                        await self.executor.buy(opp, ffilled * favg, opp_ask))
                    if not ok:
                        return
                    if a2 is not None and s2 > 0:
                        entry_px, ffilled = a2, s2
                    if _fade_oneshot:
                        _consume_oneshot("fade")    # auto-off: strictly one-shot
                leg = self._new_leg(w, idx, is_t3=is_t3)
                await _offload(self._stamp_signal, leg, p)
                leg["predicted_side"] = fade_side          # we BOUGHT the opposite
                leg["token_id"] = str(opp)
                leg["entry_price"] = round(entry_px, 4)
                leg["shares"] = round(ffilled, 2)
                leg["stake_usd"] = round(ffilled * entry_px, 2)
                leg["staked"] = True
                leg["status"] = "OPEN"
                leg["book"] = "fade"
                leg["_fade_maker"] = bool(FADE_MAKER)
                w["legs"].append(leg)
                w["open_leg"] = leg                         # held; manage() skips fade
                w["faded"] = True                           # one fade per window
                w["staked_count"] += 1
                print(f"[{datetime.now(timezone.utc):%H:%M:%S}] FADE  {w['slug']} "
                      f"{fade_side} vs {p['label']} {p['side']} -> {ffilled:.1f}sh "
                      f"@ {entry_px:.3f} ({'maker' if FADE_MAKER else 'taker'}) "
                      f"{'LIVE' if self.live else 'DRYRUN'}", flush=True)
                upsert(leg)
                return
            # else: TAKER T+3 continuation
            token, avg, filled = p["token"], p["avg"], p["shares"]
            # Default FALSE (2026-07-27): shadow must be fail-safe. t3 was the
            # only book whose fallback was LIVE, so deleting the `t3_live` config row
            # (as opposed to setting it 0) would have silently armed it on the next
            # tick — no restart, no banner change. Liveness now requires the row to
            # say so explicitly. No behaviour change while t3_live=0 exists.
            _t3_order_sent = (self.live and book_stakes_now(
                "t3", False, w["window_start_utc"])
                and three_source_consensus_ok(
                    w["window_anchor"], p["side"])[0])
            _t3_window_claimed = False
            if _t3_order_sent:
                _t3_window_claimed = await _offload(
                    claim_cross_asset_window, w["window_anchor"], "t3")
                if not _t3_window_claimed:
                    p["reason"] = "cross-asset window already claimed"
                    print(f"[{datetime.now(timezone.utc):%H:%M:%S}] T3-SKIP "
                          f"{w['slug']} · BTC/ETH window already claimed",
                          flush=True)
                    return
            # The FAK carries the SAME limit the fill was modelled against
            # (t3_cap_px = ask + T3_PAYUP, bounded by 0.80). It used to be handed
            # the bare 0.80 ceiling while the model priced the touch, which let the
            # exchange sweep far past the modelled price: every live t3 fill to date
            # came in worse than modelled, one of them 0.19 -> 0.59. Falling back to
            # MAX_ENTRY_ASK keeps a row missing the stamp from tightening silently.
            _t3_cap = p.get("t3_cap_px") or MAX_ENTRY_ASK
            # PRE-SEND ASK RE-CHECK (). A decision tick can take
            # seconds (tick_ms p95 ~4.5s), and when the ask has collapsed >=10c
            # under the decision ask by send time the market has already turned on
            # us: real BTC fills filled >=10c below the decision ask went 1/5,
            # EV -0.46/$ (vs +0.06..+0.35 otherwise; 09-28 14:30 and 17:15 losers
            # filled 0.46->0.36 and 0.49->0.32). A filled FAK can't be cancelled,
            # so re-read the book right before sending and walk away instead.
            # REST, not the WS cache: the cache only goes stale after BOOK_STALE_S
            # of silence, and the 17:15 decision read a 0.49/0.50 ladder that the
            # exchange filled at 0.32 — re-reading the same cache would miss it.
            self._tmark("send0")
            if _t3_order_sent and T3_PRESEND_MAX_DROP > 0:
                try:
                    _pb, _pa = _levels(await self.client.get_order_book(token_id=token))
                except Exception:
                    _pa = None
                _now_ask = _pa[0][0] if _pa else None
                _drop = (p["best_ask"] - _now_ask) if _now_ask is not None else None
                p["t3_presend_ask"] = _now_ask
                self._tmark("rest")
                print(f"[{datetime.now(timezone.utc):%H:%M:%S}] T3-PRESEND {w['slug']} "
                      f"decision ask {p['best_ask']:.2f} · REST ask "
                      f"{'—' if _now_ask is None else f'{_now_ask:.2f}'}", flush=True)
                if _drop is not None and _drop >= T3_PRESEND_MAX_DROP - 1e-9:
                    await _offload(release_cross_asset_window,
                                   w["window_anchor"], "t3")
                    p["reason"] = (f"t3 pre-send: ask fell {p['best_ask']:.2f}->"
                                   f"{_now_ask:.2f} (>= {T3_PRESEND_MAX_DROP:.2f})")
                    print(f"[{datetime.now(timezone.utc):%H:%M:%S}] T3-SKIP "
                          f"{w['slug']} · {p['reason']}", flush=True)
                    return
            if _t3_order_sent:  # else modelled only
                a2, s2, ok, err = await self.executor.buy(
                    token, filled * avg, p["best_ask"],
                    max_price=_t3_cap)
                self._tmark("fill")
                if not ok:
                    await _offload(release_cross_asset_window,
                                   w["window_anchor"], "t3")
                    return
                if a2 is not None and s2 > 0:
                    avg, filled = a2, s2
                else:
                    await _offload(release_cross_asset_window,
                                   w["window_anchor"], "t3")
                    return
            leg = self._new_leg(w, idx, is_t3=is_t3)
            await _offload(self._stamp_signal, leg, p)
            leg["token_id"] = str(token)
            leg["entry_price"] = round(avg, 4)
            # FLOOR, never round: `round(sh, 2)` can land above what we actually
            # hold and PM then rejects the whole sell for the life of the window.
            # Every other book already uses floor_shares(); t3 was the last holdout.
            leg["shares"] = floor_shares(filled)
            leg["stake_usd"] = round(filled * avg, 2)
            # REALISED vs MODELLED, stamped only when an order really went out, so
            # the execution-quality question ("is our fill worse than the book we
            # decided on, and by how much") is a column and not a reconstruction.
            if _t3_order_sent:
                leg["t3_fill_px"] = round(avg, 6)
                leg["t3_fill_sh"] = floor_shares(filled)
                if p.get("t3_sweep_avg"):
                    leg["t3_fill_vs_model_bps"] = round(
                        (avg - p["t3_sweep_avg"]) / p["t3_sweep_avg"] * 1e4, 3)
            leg["staked"] = True
            leg["status"] = "OPEN"
            leg["book"] = "t3"
            w["legs"].append(leg)
            w["open_leg"] = leg
            w["staked_count"] += 1
            w["t3_staked"] = True        # this window's ONE t3 stake is spent
            tag = "ENTER" if is_t3 else f"RE-ENTER#{w['staked_count']}"
            print(f"[{datetime.now(timezone.utc):%H:%M:%S}] {tag}[T3] {w['slug']} "
                  f"{p['side']} {p['label']} {p['dbps']:+.1f}bps -> {filled:.1f}sh "
                  f"@ {avg:.3f} taker {'LIVE' if self.live else 'DRYRUN'}", flush=True)
            upsert(leg)
            # The T3 continuation route was the only real taker path that did not
            # reconcile its acknowledged fill against the exchange position.
            # Only schedule this when an order was actually sent; modelled T3 legs
            # must remain free of exchange evidence.
            if _t3_order_sent:
                asyncio.create_task(self._reconcile_stake(
                    leg, str(token), filled))
            return

        token, avg, filled = p["token"], p["avg"], p["shares"]
        # Maker economics: we rest at the bid (never cross), so the modelled fill
        # price is the bid, not the taker walk-fill avg. Sizing above stays ask-
        # based on purpose -- that's the conservative (stricter) breakeven gate.
        if MAKER_MODE:
            avg = p["best_bid"]
        if self.live:
            _consensus_ok, _consensus_why = three_source_consensus_ok(
                w["window_anchor"], p["side"])
            if not _consensus_ok:
                print(f"[{datetime.now(timezone.utc):%H:%M:%S}] LIVE-SKIP "
                      f"{w['slug']} · {_consensus_why}", flush=True)
                return
            if MAKER_MODE:
                avg2, sh2, ok, err = await self.executor.buy_maker(
                    token, filled, p["best_bid"], p["best_ask"])
            else:
                avg2, sh2, ok, err = await self.executor.buy(
                    token, filled * avg, p["best_ask"],
                    max_price=MAX_ENTRY_ASK)
            if not ok:
                if is_t3:
                    leg = self._new_leg(w, idx, is_t3=True)
                    await _offload(self._stamp_signal, leg, p)
                    leg["status"] = "NO_STAKE"
                    leg["no_stake_reason"] = (
                        f"maker buy unfilled: {err}" if MAKER_MODE
                        else f"live buy failed: {err}")
                    w["legs"].append(leg)
                    upsert(leg)
                return
            if avg2 is not None and sh2 > 0:
                avg, filled = avg2, sh2

        leg = self._new_leg(w, idx, is_t3=is_t3)
        await _offload(self._stamp_signal, leg, p)
        leg["token_id"] = str(token)
        leg["entry_price"] = round(avg, 4)
        leg["shares"] = round(filled, 2)
        leg["stake_usd"] = round(filled * avg, 2)
        leg["staked"] = True
        leg["status"] = "OPEN"
        w["legs"].append(leg)
        w["open_leg"] = leg
        w["staked_count"] += 1
        w["t3_staked"] = True            # this window's ONE t3 stake is spent
        tag = "ENTER" if is_t3 else f"RE-ENTER#{w['staked_count']}"
        print(f"[{datetime.now(timezone.utc):%H:%M:%S}] {tag} {w['slug']} "
              f"{p['side']} {p['label']} {p['dbps']:+.1f}bps adx={w['adx']} -> "
              f"{filled:.1f}sh @ {avg:.3f} (${leg['stake_usd']}) "
              f"{'LIVE' if self.live else 'DRYRUN'}", flush=True)
        upsert(leg)
        # DRY-RUN: shadow-log realistic maker fill (queue depth + volume) vs the
        # optimistic bid-fill recorded above. Background task — never blocks tick.
        if SHADOW_FILL and MAKER_MODE and not self.live and self.bookfeed is not None:
            asyncio.create_task(self._shadow_fill_log(
                token, "BUY", filled, p["best_bid"], p["best_ask"],
                {"slug": w["slug"], "leg": idx, "kind": "entry",
                 "dir": p["side"], "tier": p["label"], "assumed_px": round(avg, 3),
                 "gate_mode": MAKER_FILL_GATE, "pred_fillable": p.get("maker_fillable"),
                 "queue_ahead": p.get("queue_ahead"), "vol_rate": p.get("vol_rate"),
                 "gate_would_block": p.get("gate_would_block")}))

    # ----- manage the single OPEN leg (one stake at a time) -----
    async def manage(self, w, now):
        leg = w["open_leg"]
        if leg is None:
            return
        if leg.get("book") in ("flip", "oppfade", "swingfade"):
            # flip (plumbing one-shot), oppfade (the cheap-opp fade book) and
            # swingfade are pure hold-to-settle: no TP/SL/reversal exit — ride to
            # the window boundary where settle() books P&L and enqueues the redeem
            # on a win.
            # oppfade MUST be here: it buys at ask<=0.16, so best_bid sits under
            # SL_BID (0.30) from the first tick and the generic path would dump it
            # at market seconds after entry AND clear open_leg (freeing the
            # one-stake-per-window guard). Bail rules are refuted on this book.
            # swingfade MUST be here for the SAME reason and was MISSING until
            # 2026-07-25: it buys at ask <= SWINGFADE_ASK_MAX, so the bid is
            # under SL_BID at the INSTANT OF FILL on 47/47 historical fires — the
            # generic path made it a fill-triggered exit, not a risk exit. The
            # 07-25 12:38 live leg was stopped 6.6s after entry (0.130 -> 0.110);
            # the two earlier live legs fired the same SL and only survived because
            # the sell was rejected. A/B over the re_flip_shadow replay (n=47, live
            # gate): HOLD +$35.99 vs SL -$8.44 per $1 staked (+9,453 bps/trade,
            # bootstrap P(HOLD>SL)=98.7%); breakeven WR 15.5% vs observed 27.7%.
            # See [[project_pm_swingfade_book]].
            return
        if HOLD_TO_SETTLE and leg.get("book") == "t3":
            # Literal hold-to-settlement: no TP, SL, reversal, or late bail. The
            # prior implementation still took profit at HOLD_TP_BID and could bail
            # near the close, so shadow legs closed before the five-minute boundary.
            # PM's resolved outcome now closes/reconciles the leg in settle().
            return
        if leg.get("book") in ("fade", "hfade"):
            # SALVAGE EXIT: normally held to settle, but if the reversal is failing
            # (fade token bid dropped below FADE_SL_FRAC of entry) sell at market to
            # save the remaining premium rather than ride to $0.
            if FADE_SL_FRAC <= 0:
                return
            # grace window: ignore first-tick noise for the first GRACE seconds
            # after the fill, then let the salvage arm on the next poll.
            if FADE_SALVAGE_GRACE_S > 0:
                held_s = (datetime.now(timezone.utc)
                          - leg["decided_at"]).total_seconds()
                if held_s < FADE_SALVAGE_GRACE_S:
                    return
            try:
                fb, fa = await self.book_levels(int(leg["token_id"]))
            except Exception:
                return
            fbid = fb[0][0] if fb else 0.0
            if fbid >= FADE_SL_FRAC * leg["entry_price"]:
                return                   # reversal thesis still alive — hold
            if self.live and not FADE_SHADOW:      # fade shadow: model only, no real order
                ok, err = await self.executor.sell(
                    int(leg["token_id"]), leg["shares"], fbid)
                if not ok:
                    return
                avg, _ = walk_fill(fb, leg["shares"])
                fbid = avg if avg is not None else fbid
            sh, entry = leg["shares"], leg["entry_price"]
            entry_fee = 0.0 if leg.get("_fade_maker") else _taker_fee(sh, entry)
            proceeds = sh * fbid - _taker_fee(sh, fbid)
            leg["exit_reason"] = "FADE_EXIT"
            leg["exit_price"] = round(fbid, 4)
            leg["exit_at"] = datetime.now(timezone.utc)
            leg["realized_pnl_usd"] = round(proceeds - (sh * entry + entry_fee), 4)
            leg["status"] = "CLOSED"
            w["open_leg"] = None
            print(f"[{datetime.now(timezone.utc):%H:%M:%S}] FADE_EXIT {w['slug']} "
                  f"L{leg['leg']} ({leg['book']}) bid {fbid:.3f} < "
                  f"{FADE_SL_FRAC:.0%}x entry {entry:.3f} -> salvage, "
                  f"pnl=${leg['realized_pnl_usd']}", flush=True)
            upsert(leg)
            return
        token = int(leg["token_id"])
        try:
            bids, asks = await self.book_levels(token)
            price_now = await _offload(underlying_price_now)
        except Exception:
            return
        if price_now is None:
            return          # Chainlink-only: no price -> no exit decision this tick
        best_bid = bids[0][0] if bids else 0.0
        best_ask = asks[0][0] if asks else round_tick(best_bid + TICK)
        side = leg["predicted_side"]

        def _rev(dbps):
            return ((side == "Up" and dbps < -SL_REVERSAL_BPS)
                    or (side == "Down" and dbps > SL_REVERSAL_BPS))

        # Reversal on the CHAINLINK move (open -> now). This used to be computed on
        # BOTH feeds, with EXIT_REVERSAL_FEED defaulting to "binance" so the trigger
        # fired ~3s early off Binance's own open->now delta. Binance-only as of
        # 2026-07-30: the exit loses that head start but is now measured on the feed
        # that decides the payout, so it can no longer fire on a divergence that never
        # reaches the settlement price.
        dbps_now = (price_now - w["price_open"]) / w["price_open"] * 1e4
        reversed_against = _rev(dbps_now)
        # HOLD-TO-SETTLE (t3 continuation only): the +EV is realized at the 5-min
        # boundary, so skip the noise-triggered SL / SL_REVERSAL and ride to
        # settlement — EXCEPT the late-window BAIL, which stays ACTIVE (it fires on a
        # REAL late reversal — the feed disagrees at the close — not intra-window
        # noise, so it recovers reversal-losses without cutting winners). A HIGH-bid TP
        # also fires (lock a near-certain win). Fade legs handled above.
        hts = HOLD_TO_SETTLE and leg.get("book") == "t3"
        bail_bps = HTS_BAIL_MIN_BPS if hts else LATE_BAIL_MIN_BPS
        late = (w["expiry"] - now) <= LATE_BAIL_S
        # only bail if the move is decisively adverse AND the token bid has fallen
        # (market confirms the turn) — a still-high bid means the market rejects it.
        feed_against = (late and abs(dbps_now) >= bail_bps
                        and ("Up" if dbps_now > 0 else "Down") != side
                        and best_bid <= BAIL_MAX_BID)
        reason = None
        if best_bid >= (HOLD_TP_BID if hts else TP_BID):
            reason = "TP"
        elif not hts and best_bid <= SL_BID:
            reason = "SL"
        elif not hts and reversed_against:
            reason = "SL_REVERSAL"
            print(f"[{datetime.now(timezone.utc):%H:%M:%S}] SL_REVERSAL {w['slug']} "
                  f"L{leg['leg']} chainlink {dbps_now:+.1f}bps vs our {side}",
                  flush=True)
        # LATE-WINDOW BAIL: in the final LATE_BAIL_S, if the Chainlink move is
        # decisively AGAINST our side, dump at market rather than settle to 0.
        # Active in BOTH modes (the hold-to-settle hybrid).
        elif feed_against:
            reason = "BAIL"
            print(f"[{datetime.now(timezone.utc):%H:%M:%S}] BAIL{'(hts)' if hts else ''} "
                  f"{w['slug']} L{leg['leg']} {int(w['expiry']-now)}s to close: chainlink "
                  f"{dbps_now:+.1f}bps => {'Up' if dbps_now>0 else 'Down'} vs our {side} "
                  f"— dumping @ bid {best_bid:.3f}", flush=True)
        if reason:
            leg["price_exit_btc"] = price_now   # BTC spot at exit, for chart marker
            if TWO_BOOK:
                # TWO_BOOK t3 exits (TP/SL/SL_REVERSAL/BAIL) cross as TAKER — sell
                # into the bid; fee accounted in close_leg via taker_pnl.
                if self.live:
                    ok, err = await self.executor.sell(token, leg["shares"], best_bid)
                    if not ok:
                        print(f"[{datetime.now(timezone.utc):%H:%M:%S}] SELL FAILED "
                              f"{w['slug']} L{leg['leg']} {reason}: {err} — retry",
                              flush=True)
                        return
                avg, _ = walk_fill(bids, leg["shares"])
                exit_px = avg if avg is not None else best_bid
                self.close_leg(w, leg, reason, exit_px)
                return
            if self.live and MAKER_MODE:
                # Post-only exit from the ask, repricing toward mid; tolerate
                # partial fills across ticks. If nothing fills we hold the
                # remainder to the next tick / settlement -- never cross.
                remaining = leg["shares"] - leg.get("_sold", 0.0)
                avg2, sh2, ok, err = await self.executor.sell_maker(
                    token, remaining, best_bid, best_ask)
                if sh2 > 0:
                    leg["_sold"] = leg.get("_sold", 0.0) + sh2
                    leg["_exit_notional"] = leg.get("_exit_notional", 0.0) + avg2 * sh2
                if leg.get("_sold", 0.0) < leg["shares"] - 1e-6:
                    print(f"[{datetime.now(timezone.utc):%H:%M:%S}] MAKER EXIT "
                          f"{w['slug']} L{leg['leg']} {reason}: filled "
                          f"{leg.get('_sold', 0.0):.1f}/{leg['shares']:.1f}sh, "
                          f"holding remainder ({err or 'partial'})", flush=True)
                    return
                exit_px = leg["_exit_notional"] / leg["_sold"]
                self.close_leg(w, leg, reason, exit_px)
                return
            if self.live:
                ok, err = await self.executor.sell(token, leg["shares"], best_bid)
                if not ok:
                    print(f"[{datetime.now(timezone.utc):%H:%M:%S}] SELL FAILED "
                          f"{w['slug']} L{leg['leg']} {reason}: {err} — retry next tick",
                          flush=True)
                    return
                avg, _ = walk_fill(bids, leg["shares"])
                exit_px = avg if avg is not None else best_bid
            elif MAKER_MODE:
                # dry-run maker: modelled fill resting at the ask ($0 fee)
                exit_px = best_ask
                if SHADOW_FILL and self.bookfeed is not None:
                    asyncio.create_task(self._shadow_fill_log(
                        token, "SELL", leg["shares"], best_bid, best_ask,
                        {"slug": w["slug"], "leg": leg["leg"], "kind": "exit",
                         "reason": reason, "assumed_px": round(exit_px, 3)}))
            else:
                avg, _ = walk_fill(bids, leg["shares"])
                exit_px = avg if avg is not None else best_bid
            self.close_leg(w, leg, reason, exit_px)

    def close_leg(self, w, leg, reason, exit_px):
        leg["exit_reason"] = reason
        leg["exit_price"] = round(exit_px, 4)
        leg["exit_at"] = datetime.now(timezone.utc)
        if TWO_BOOK and leg.get("book") in TAKER_HOLD_BOOKS:
            # TAKER economics, net of the 0.07*p*(1-p) fee on both legs.
            # Widened from `== "t3"` to TAKER_HOLD_BOOKS 2026-07-25: no-op today
            # (manage() returns early for every other book in the list, so only t3
            # reaches here) but it means a book that later gets an early exit
            # cannot silently book fee-free P&L the way swingfade's SL did.
            leg["realized_pnl_usd"] = taker_pnl(
                leg["entry_price"], reason, exit_px, leg["shares"])
        else:
            leg["realized_pnl_usd"] = round(
                (exit_px - leg["entry_price"]) * leg["shares"], 4)
        leg["status"] = "CLOSED"
        w["open_leg"] = None
        w["last_exit_ts"] = time.time()
        print(f"[{datetime.now(timezone.utc):%H:%M:%S}] EXIT  {w['slug']} "
              f"L{leg['leg']} {reason} @ {exit_px:.3f} pnl=${leg['realized_pnl_usd']}",
              flush=True)
        upsert(leg)

    async def _shadow_fill_log(self, token_id, side, shares, best_bid, best_ask, meta):
        """DRY-RUN fill-realism telemetry. Replays the maker reprice ladder
        read-only against the live WS book and estimates what would ACTUALLY
        fill using VOLUME: `queue_ahead` = resting size at our price (we join the
        back of it) and `traded_thru` = tape volume through our price since we
        rested. est_fill per leg = min(remaining, max(0, traded_thru -
        queue_ahead)); a book that trades through our price (crossed) fills the
        rest. Writes one JSONL record; does NOT touch recorded PnL. Runs as a
        background task so it never stalls the tick loop."""
        try:
            bf = self.bookfeed
            if bf is None or not WS_BOOK:
                return
            tid = str(token_id)
            buy = str(side).upper() == "BUY"
            if buy:
                price, limit, step = round_tick(best_bid), round_tick(best_ask - TICK), TICK
            else:
                price, limit, step = round_tick(best_ask), round_tick(best_bid + TICK), -TICK
            remaining = float(shares)
            est_filled = 0.0
            legs = []
            t0 = time.time()
            for _ in range(MAKER_MAX_REPRICES + 1):
                if remaining <= 1e-6:
                    break
                rest_ts = time.time()
                qa = bf.queue_at(tid, price, side)
                qa = 0.0 if qa is None else qa
                crossed = False
                leg_fill = 0.0
                waited = 0.0
                while waited < MAKER_REPRICE_S:
                    await asyncio.sleep(MAKER_POLL_S)
                    waited += MAKER_POLL_S
                    lv = bf.levels(tid)
                    if lv:
                        bids, asks = lv
                        opp = (asks[0][0] if buy else bids[0][0])
                        if (buy and opp <= price + 1e-9) or ((not buy) and opp >= price - 1e-9):
                            crossed = True
                    traded = bf.traded_through(tid, price, side, rest_ts)
                    leg_fill = remaining if crossed else min(remaining, max(0.0, traded - qa))
                    if leg_fill >= remaining - 1e-6:
                        break
                est_filled += leg_fill
                remaining -= leg_fill
                legs.append({
                    "price": round(price, 3), "queue_ahead": round(qa, 1),
                    "traded_thru": round(bf.traded_through(tid, price, side, rest_ts), 1),
                    "crossed": crossed, "est_fill": round(leg_fill, 2),
                    "dwell_s": round(waited, 1)})
                if (step > 0 and price >= limit) or (step < 0 and price <= limit):
                    break
                price = round_tick(min(limit, price + step) if step > 0
                                   else max(limit, price - step))
            rec = {
                "ts": datetime.now(timezone.utc).isoformat(), "token_id": tid,
                "side": side, "want_shares": round(float(shares), 2),
                "est_filled": round(est_filled, 2),
                "fill_frac": round(est_filled / shares, 3) if shares else None,
                "elapsed_s": round(time.time() - t0, 1),
                "trades_seen": bf.trades_seen, "legs": legs, **meta}
            try:
                with open(MAKER_FILL_LOG, "a") as f:
                    f.write(json.dumps(rec) + "\n")
            except Exception:
                pass
            print(f"[{datetime.now(timezone.utc):%H:%M:%S}] SHADOW-FILL "
                  f"{meta.get('slug', '')} L{meta.get('leg', '?')} {meta.get('kind', '')} "
                  f"{side} want={float(shares):.1f} est_fill={est_filled:.1f}sh "
                  f"({rec['fill_frac']}) {len(legs)}leg [queue+volume]", flush=True)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            _err("shadow_fill", e)
            print(f"[shadow-fill] error {type(e).__name__}: {str(e)[:80]}", flush=True)

    # ----- settlement at window end -----
    def settle(self, w):
        # Resolve on the CHAINLINK price at the exact window-CLOSE timestamp (== PM's
        # resolution). NO BINANCE FALLBACK, EVER (2026-07-30: "binance feed is
        # completely different and cant be a substitute for chainlink"). It used to
        # accept Binance once SETTLE_GRACE_S elapsed so a window "can't hang"; that
        # traded a hang for a WRONG BOOK — Binance disagrees on who won 7.99% of
        # windows (26.2% when the entry gap is <1bps, which is where this book lives),
        # so the fallback could record the wrong outcome and, live, skip redeeming a
        # real winner.
        #
        # One resolution-aligned source: Polymarket's Chainlink RTDS rebroadcast,
        # persisted by our collector. If it is empty the window stays UNSETTLED and
        # is retried every tick, then
        # swept by _settle_deferred() below. An unsettled window is recoverable; a
        # mis-settled one is not.
        reference = polymarket_window_reference(w["window_anchor"])
        settle_px = reference["price_settle"]
        open_px = reference["price_open"]
        if open_px is None or settle_px is None:
            feed_outcome = None
        elif settle_px >= open_px:
            feed_outcome = "Up"
        else:
            feed_outcome = "Down"

        # Redemption readiness is an on-chain fact, not a Gamma-indexing fact.
        # Queue every real position's condition as soon as its window expires;
        # redeem_positions() is outcome-agnostic and safely retries until the CTF
        # condition is resolved. This lets a winner return to USDC as soon as the
        # contract permits it even when Gamma remains stale for minutes. The flag
        # prevents settle()'s per-tick retry from duplicating the queue item.
        if (self.live and self.executor is not None and w.get("condition_id")
                and not w.get("redeem_enqueued_at_expiry")
                and any(leg.get("staked") and leg.get("status") == "OPEN"
                        for leg in w.get("legs") or ())):
            self._redeem_q.append(
                (w["condition_id"], w["slug"], 0, 0.0))
            w["redeem_enqueued_at_expiry"] = time.time()
            print(f"[{datetime.now(timezone.utc):%H:%M:%S}] REDEEM-ARM "
                  f"{w['slug']} at expiry (independent of Gamma)", flush=True)

        # PM's resolved 1/0 outcome is the authority. This normally appears about
        # a minute after expiry; defer rather than finalising from RTDS alone.
        # Shadow settlement must track the PM UI's fast RTDS/TWAP result. Calling
        # Gamma here first adds its 4s timeout to the ~3s poll cadence and turns a
        # result available in seconds into a 7-10s dashboard gap. The independent
        # 15s reconciliation sweep still fetches/stamps PM's official verdict.
        # Live money continues to require that authority before closing/redeeming.
        pm_outcome = (None if DRY_RUN
                      else polymarket_resolved_outcome(w["slug"]))
        if pm_outcome is None:
            # Gamma can lag several minutes even though the resolution-aligned
            # RTDS close is already durable.  For rows with NO POSITION, expose
            # that feed result immediately so the dashboard does not leave an
            # expired window looking live.  This is display/scoring only: no P&L,
            # redemption, or wallet state is touched, pm_outcome remains NULL,
            # and the official verdict below overwrites/reconciles it later.
            if feed_outcome in ("Up", "Down", "Tie"):
                for leg in w["legs"]:
                    if leg["status"] not in ("NO_STAKE", "WATCH"):
                        continue
                    leg["actual_outcome"] = feed_outcome
                    leg["price_settle"] = settle_px
                    if (leg["predicted_side"] is not None
                            and feed_outcome in ("Up", "Down")):
                        leg["hit"] = (leg["predicted_side"] == feed_outcome)
                    leg["status"] = "SETTLED_NOTRADE"
                    upsert(leg)
            # PM's UI publishes the RTDS/TWAP direction within seconds, while both
            # Gamma and CLOB can remain open/unresolved for several minutes. Shadow
            # stakes carry no wallet/redemption risk, so close them on that same
            # resolution feed now and let reconcile_pending_pm_outcomes() replace
            # the provisional outcome if PM's eventual official verdict differs.
            # Real-money positions still fail closed and wait for PM authority.
            if DRY_RUN and feed_outcome in ("Up", "Down"):
                outcome = feed_outcome
            else:
                if time.time() - w["expiry"] >= SETTLE_GRACE_S:
                    w["settle_deferred_since"] = (w.get("settle_deferred_since")
                                                   or time.time())
                return
        else:
            outcome = pm_outcome
        w["actual_outcome"] = outcome
        w["price_settle"] = settle_px
        w["pm_outcome"] = pm_outcome
        w["pm_recon_ok"] = (feed_outcome == pm_outcome
                             if feed_outcome in ("Up", "Down") else None)

        for leg in w["legs"]:
            leg["actual_outcome"] = outcome
            leg["price_settle"] = settle_px
            leg["pm_outcome"] = pm_outcome
            leg["pm_recon_ok"] = w["pm_recon_ok"]
            leg["pm_reconciled_at"] = datetime.now(timezone.utc)
            # hit_binance (the look-ahead scoring of the Binance-early call against
            # the Chainlink resolution) is no longer computed — 2026-07-30. Rows
            # decided before then keep their scores; nothing new is stamped.
            if leg["predicted_side"] is not None and outcome in ("Up", "Down"):
                leg["hit"] = (leg["predicted_side"] == outcome)
            # A POSITION is either the actively-managed open_leg OR one parked in
            # hold_legs (swingfade). The hold_legs split (2026-07-29) assumed this
            # gate keyed off status alone — it does not, it keys off open_leg
            # IDENTITY, so the first swingfade stake after that change (07-29 21:48,
            # id 46172) got its outcome/hit stamped but hung OPEN forever: no
            # exit_reason, no P&L, and — for a winner — no redeem enqueue. Identity
            # compare, not `in`, because dict `in` is equality-by-content.
            _held = leg is w["open_leg"] or any(
                leg is h for h in w.get("hold_legs") or ())
            if _held and leg["status"] == "OPEN":
                # held to expiry -> resolves on-chain. LIVE: winning tokens are
                # redeemed via the _redeem_q → executor.redeem() sweep below (or
                # Polymarket's auto_redeem_operator). pnl here is the correct
                # economic result once redeemed at $1/share.
                # Per-book fee economics live in hold_to_settle_pnl() (extracted
                # 2026-07-30 so _settle_deferred books stuck rows identically —
                # oppfade's and swingfade's entry-fee terms were each added here
                # after a wallet mismatch, and one drifting copy would reintroduce
                # exactly that class of bug).
                won = leg["hit"] is True
                payout = 1.0 if won else 0.0
                leg["exit_reason"] = "SETTLE_WIN" if won else "SETTLE_LOSS"
                leg["exit_price"] = payout
                leg["exit_at"] = datetime.now(timezone.utc)
                leg["realized_pnl_usd"] = hold_to_settle_pnl(
                    leg.get("book"), leg["entry_price"], leg["shares"], won,
                    fade_maker=leg.get("_fade_maker", False))
                leg["status"] = "CLOSED"
                # LIVE hold-to-settle payout: a winning REAL t3 position must be
                # redeemed on-chain to realise $1/share. Enqueue for the async loop
                # to redeem (fade legs are shadow/not real → skipped). No-op if
                # Polymarket's auto_redeem_operator already swept it.
                if (won and self.live and TWO_BOOK
                        and leg.get("book") in TAKER_HOLD_BOOKS
                        and w.get("condition_id")
                        and not w.get("redeem_enqueued_at_expiry")):
                    # swingfade added 2026-07-25 — it was NOT in this tuple, so the
                    # 07-25 08:33 winner (+$3.35) was never enqueued and depended on
                    # Polymarket's auto_redeem_operator to turn shares into USDC.
                    self._redeem_q.append(
                        (w["condition_id"], w["slug"], 0, 0.0))
                print(f"[{datetime.now(timezone.utc):%H:%M:%S}] SETTLE {w['slug']} "
                      f"L{leg['leg']} {leg['exit_reason']} "
                      f"pnl=${leg['realized_pnl_usd']}", flush=True)
            elif leg["status"] in ("NO_STAKE", "WATCH"):
                leg["status"] = "SETTLED_NOTRADE"
            audit = twap_exit_audit(leg, outcome)
            if audit:
                leg.update(audit)
                print(f"TWAP-EXIT-AUDIT {w['slug']} L{leg['leg']} "
                      f"{audit['no_stake_reason']}", flush=True)
            upsert(leg)
        # RE-FLIP SHADOW self-scoring (isolated — wrapped so it can NEVER disturb the
        # real settlement above). Stamp every flip row for this window with the
        # resolution (flip_won = flip_side==outcome) + each seq's peak exit bid. This
        # is the ONLY place the shadow gets scored; without it flips hang "pending"
        # forever and peak_bid_flip stays NULL.
        if outcome in ("Up", "Down"):
            try:
                backfill_re_flip_settle(w["window_anchor"], outcome, settle_px,
                                        w.get("reflip_peaks"),
                                        w.get("reflip_opp_min"))
            except Exception as e:
                print(f"[re_flip] backfill skipped {w['slug']}: "
                      f"{type(e).__name__}: {e}", flush=True)
        w["open_leg"] = None
        w["settled"] = True

    # ----- deferred-settlement recovery sweep (DB-driven) -----
    async def _settle_deferred(self, max_rows=10):
        """Book positions that expired but were never settled — the durability net
        under the Chainlink-only settle() above (2026-07-30: "we are
        reconciling the stake after the settlement, that could be a place to pick
        from").

        settle() now DEFERS instead of falling back to Binance, and the tick retries
        it for 6h — but that retry lives in memory, so it is lost on a restart, and
        the window is pruned after 6h. Both leave a real staked position stranded
        OPEN in the DB with no P&L and, if it WON, no on-chain redeem. That is
        exactly how leg 46172 hung on 2026-07-29 (different cause, same end state).

        This sweep reads the DB instead, so it recovers rows this process never saw.
        Chainlink-only, same two sources as settle(); a row it cannot resolve is left
        alone for the next pass. Fully additive: it only ever touches rows that are
        already past expiry and still OPEN, and it books through
        hold_to_settle_pnl() so the economics match the live path exactly.
        """
        cutoff = int(time.time() - DEFERRED_SETTLE_AFTER_S)
        try:
            with ENGINE.begin() as c:
                rows = c.execute(text(f"""
                    SELECT id, window_anchor, leg, slug, book, predicted_side,
                           entry_price, shares, price_open
                      FROM {TRADES_TABLE}
                     WHERE asset = :asset AND dry_run = :dry
                       AND status = 'OPEN' AND staked IS TRUE
                       AND window_anchor + :win <= :cutoff
                       AND window_anchor >= :floor
                  ORDER BY window_anchor DESC
                     LIMIT :lim
                """), {"asset": ASSET, "dry": DRY_RUN, "win": int(WINDOW_S),
                       "cutoff": cutoff, "lim": int(max_rows),
                       "floor": int(time.time() - DEFERRED_SETTLE_MAX_AGE_S)}).fetchall()
        except Exception as e:
            _err("settle_deferred.query", e)
            return
        if not rows:
            return
        for r in rows:
            (rid, anchor, legno, slug, book, side,
             entry_px, shares, price_open) = r
            # Skip anything the in-memory path still owns and has already settled —
            # a settled window's row is CLOSED, so an OPEN row here means the live
            # path did not book it, whatever its window object says.
            expiry = int(anchor) + int(WINDOW_S)
            reference = await _offload(polymarket_window_reference, int(anchor))
            settle_px = reference["price_settle"]
            open_px = price_open
            if open_px is None:                     # never stamped — recover it too
                open_px = reference["price_open"]
            pm_outcome = await _offload(polymarket_resolved_outcome, slug)
            if pm_outcome is None:
                print(f"[settle-defer] {slug} L{legno} has no resolved PM verdict "
                      f"yet — retrying next pass", flush=True)
                continue
            feed_outcome = ("Up" if settle_px >= reference["price_open"] else "Down") \
                if settle_px is not None and reference["price_open"] is not None else None
            outcome = pm_outcome
            if side not in ("Up", "Down"):
                print(f"[settle-defer] {slug} L{legno} outcome={outcome} "
                      f"side={side} — left OPEN for manual review", flush=True)
                continue
            won = (side == outcome)
            pnl = hold_to_settle_pnl(book, entry_px, shares, won)
            reason = "SETTLE_WIN" if won else "SETTLE_LOSS"
            try:
                with ENGINE.begin() as c:
                    c.execute(text(f"""
                        UPDATE {TRADES_TABLE}
                           SET status='CLOSED', exit_reason=:reason,
                               exit_price=:payout, exit_at=now(),
                               price_settle=:spx, actual_outcome=:outcome,
                               hit=:hit, realized_pnl_usd=:pnl,
                               pm_outcome=:outcome, pm_recon_ok=:pmok,
                               pm_reconciled_at=now(),
                               price_open=COALESCE(price_open, :opx),
                               updated_at=now()
                         WHERE id=:rid AND status='OPEN'
                    """), {"reason": reason, "payout": 1.0 if won else 0.0,
                           "spx": settle_px, "outcome": outcome, "hit": won,
                           "pmok": (feed_outcome == outcome
                                    if feed_outcome in ("Up", "Down") else None),
                           "pnl": pnl, "opx": open_px, "rid": int(rid)})
            except Exception as e:
                _err("settle_deferred.update", e)
                continue
            print(f"[{datetime.now(timezone.utc):%H:%M:%S}] SETTLE-DEFERRED {slug} "
                  f"L{legno} {reason} pnl=${pnl} (recovered from DB)", flush=True)
            # A recovered WINNER still holds shares that must be redeemed on-chain.
            if won and self.live and TWO_BOOK and book in TAKER_HOLD_BOOKS:
                try:
                    mt = await self.market_tokens(int(anchor))
                    if mt and mt.get("condition_id"):
                        self._redeem_q.append(
                            (mt["condition_id"], slug, 0, 0.0))
                    else:
                        print(f"[settle-defer] {slug} WON but no condition_id — "
                              f"redeem NOT enqueued, check the wallet", flush=True)
                except Exception as e:
                    _err("settle_deferred.redeem_lookup", e)

    # ----- state json for dashboard -----
    def write_state(self):
        # Merge ALL-TIME persisted legs with the live in-memory legs (the latter
        # win — they carry the freshest fields, e.g. price_exit_btc / current
        # window). So the summary + table reflect every stake ever, not just the
        # ~6h kept in memory (which resets on restart).
        merged = {(r["window_anchor"], r["leg"]): r for r in db_legs(STATE_EXPORT_DB_TTL_S)}
        for w in self.windows.values():
            for leg in w["legs"]:
                key = (leg["window_anchor"], leg["leg"])
                db_leg = merged.get(key)
                out = dict(leg)
                # Background PM reconciliation writes the durable row without
                # mutating this in-memory shadow dict. The old merge let memory
                # overwrite CLOSED/PM/P&L with SHADOW/pending/null on every tick.
                # Once DB has finalized a settlement, those fields are immutable
                # and authoritative for the dashboard export.
                if db_leg and (db_leg.get("pm_reconciled_at") is not None
                               or db_leg.get("realized_pnl_usd") is not None):
                    for field in ("status", "exit_reason", "exit_price", "exit_at",
                                  "price_settle", "actual_outcome", "hit",
                                  "realized_pnl_usd", "pm_outcome", "pm_recon_ok",
                                  "pm_reconciled_at"):
                        out[field] = db_leg.get(field)
                merged[key] = out
        legs = list(merged.values())
        # honor the fresh-run cutover on the in-memory legs too (db_legs already
        # filters; this drops any in-memory pre-cutover windows from the display)
        rs = get_run_start()
        rs_dt = None
        if rs:
            try:
                rs_dt = datetime.fromisoformat(str(rs).replace(" ", "T"))
            except Exception:
                rs_dt = None
        if rs_dt:
            def _after(l):
                d = l.get("decided_at")
                if d is None:
                    return True
                try:
                    if not hasattr(d, "tzinfo"):
                        d = datetime.fromisoformat(str(d).replace(" ", "T"))
                    return d >= rs_dt
                except Exception:
                    return True
            legs = [l for l in legs if _after(l)]
        legs.sort(key=lambda x: (x["window_anchor"], x.get("leg") or 0), reverse=True)
        staked = [l for l in legs if l.get("staked")]
        closed = [l for l in staked if l.get("realized_pnl_usd") is not None]
        windows_tracked = len({l["window_anchor"] for l in legs})
        # signal accuracy from the T+3 evaluation legs (staked or not)
        t3 = [l for l in legs if l.get("is_t3") and l.get("hit") is not None]
        t3_wins = sum(1 for l in t3 if l["hit"])
        # accuracy for the QUALIFIED (staked) tiers only — the figure that matters
        # while fades are in shadow and only these tiers place real T+3 trades.
        t3_qual = [l for l in t3 if l.get("tier") in STAKED_TIERS]
        t3_qual_wins = sum(1 for l in t3_qual if l["hit"])
        # CALIBRATION vs the book — accuracy is NOT edge. A 94%-accurate tier is
        # only worth staking if the market prices it BELOW 94%, and it does not:
        # the tier is a function of |decision_bps| and the ask tracks the same move
        # (corr 0.63), so the two are one variable. Qualified tiers quote ~0.96 mean
        # -> 3.7% of them clear MAX_ENTRY_ASK, and the few that do are the ones the
        # book singled out as doubtful (0.76-0.79 bucket: 78% priced, 50% won).
        # This delta, not the accuracy, says whether any of it is monetizable.
        # Measured on the ask-bearing subset ONLY, so the win-rate and the price
        # describe the SAME legs (mixing them silently compares different cohorts).
        # [[project_pm_ask_is_efficient_closed]]
        qual_ask = [(l, a) for l in t3_qual
                    if (a := _observed_ask(l)) is not None]
        qual_priced = qual_won = qual_z = None
        if qual_ask:
            _n = len(qual_ask)
            qual_priced = sum(a for _, a in qual_ask) / _n
            qual_won = sum(1 for l, _ in qual_ask if l["hit"]) / _n
            # z of the delta — a few points on n~100 is noise, and a bare green
            # "+1.6pp" reads as edge. Ship the significance next to it or the card
            # re-creates the error it exists to prevent. Nothing in this book has
            # ever cleared 2sigma; the dash greys anything under it.
            _se = (qual_won * (1 - qual_won) / _n) ** 0.5
            qual_z = ((qual_won - qual_priced) / _se) if _se > 0 else None
        # staked-trade win/loss economics (this is what drives P&L, distinct from
        # the direction-call accuracy above): a high win-rate can still lose money
        # if the average loss dwarfs the average win.
        wins = [l["realized_pnl_usd"] for l in closed if l["realized_pnl_usd"] > 0]
        losses = [l["realized_pnl_usd"] for l in closed if l["realized_pnl_usd"] < 0]
        nwin, nloss = len(wins), len(losses)
        avg_win = (sum(wins) / nwin) if nwin else None
        avg_loss = (sum(losses) / nloss) if nloss else None
        # per-tier running tally (all-time): direction accuracy + staked W/L/net,
        # plus the MODERATE cheap-entry (<=cap) shadow tally
        by_tier = {}
        for t in ("EXTREME", "VERY STRONG", "STRONG", "MODERATE-STRONG",
                  "MODERATE", "WEAK"):
            dl = [l for l in legs if l.get("tier") == t and l.get("is_t3")
                  and l.get("hit") is not None]
            sl = [l for l in legs if l.get("tier") == t and l.get("staked")
                  and l.get("realized_pnl_usd") is not None]
            ent = {
                "dir_n": len(dl), "dir_hit": sum(1 for l in dl if l["hit"]),
                "staked_n": len(sl),
                "wins": sum(1 for l in sl if l["realized_pnl_usd"] > 0),
                "losses": sum(1 for l in sl if l["realized_pnl_usd"] < 0),
                "net": round(sum(l["realized_pnl_usd"] for l in sl), 2) if sl else 0.0,
            }
            # NOTE: the price-aware calibration for these tier cards (ask_n/priced/
            # won_askset/edge_pp/z/be_wr/ev_per10/tot_per10) is computed
            # DASHBOARD-side in inject_tier_calibration(), not here — it needs only
            # durable table reads, so keeping it out of the bot means shipping a
            # dash change never requires restarting the live trading process.
            if t == "MODERATE":
                # Same cohort as dir_n above (T+3 decided legs), restricted to the
                # ones carrying an observed ask — so "N asks <= cap" is comparable
                # to the accuracy printed over it. The old version counted ANY leg
                # whose reason text mentioned an ask (revisions, illiquid-book and
                # undecided legs alike: 388 vs 68 decided) and tested the reason for
                # a "(<=cap)" marker that nothing emits, so it always read 0/N.
                ms = [a for l in dl if (a := _observed_ask(l)) is not None]
                ent["cap_seen"] = len(ms)
                ent["cap_pass"] = sum(1 for a in ms if a <= MAX_ENTRY_ASK)
            if ent["dir_n"] or ent["staked_n"]:
                by_tier[t] = ent
        # LIVE BOOK (display-only — no gate, sizing or order reads this). Top of
        # book for BOTH tokens straight from the WS cache: ask = the price we'd BUY
        # at, bid = the price we'd SELL at. `side` is the token we are actually on —
        # the open position if we hold one, else the T+3 call; None before the call
        # is made, where the dashboard falls back to the direction price has moved.
        # Cache-only (never REST) so an absent/stale book degrades to None instead
        # of adding latency to the trading loop.
        live_book = None
        try:
            _now = time.time()
            cw = self.windows.get(int(_now - (_now % WINDOW_S)))
            if cw is not None and cw.get("up_token"):
                _ol = cw.get("open_leg")
                _up_touch, _up_fill = self._cached_ask_depth(cw.get("up_token"))
                _dn_touch, _dn_fill = self._cached_ask_depth(cw.get("down_token"))
                live_book = {
                    "ts": int(_now),
                    "window_anchor": cw["window_anchor"],
                    "side": (_ol.get("predicted_side") if _ol else None) or cw.get("t3_side"),
                    # ask-side DEPTH per side (2026-07-22): touch = size at the best
                    # ask, fill = shares resting at asks <= OPPFADE_ASK_MAX (what the
                    # oppfade FAK can actually take). The dash legend shows the CHEAP
                    # (opp-fade) side's depth — the fill constraint, not the quote.
                    "ask_max": OPPFADE_ASK_MAX,
                    "ask_min": OPPFADE_ASK_MIN,
                    "elapsed_min": OPPFADE_MIN_ELAPSED_S,
                    "elapsed_max": OPPFADE_MAX_ELAPSED_S,
                    "up":   {"bid": self._cached_best_bid(cw.get("up_token")),
                             "ask": self._cached_best_ask(cw.get("up_token")),
                             "touch_sh": _up_touch, "fill_sh": _up_fill},
                    "down": {"bid": self._cached_best_bid(cw.get("down_token")),
                             "ask": self._cached_best_ask(cw.get("down_token")),
                             "touch_sh": _dn_touch, "fill_sh": _dn_fill},
                }
        except Exception:
            live_book = None
        # DAILY-LOCKED oppfade sizing figures for the dash cards (2026-07-22):
        # capital = 00:00-UTC exchange snapshot; usable = 2/3; stake = 10% of usable,
        # floored at the $1 PM minimum. Mirrors the live sizing in tick()/the gate.
        _oppfade_daily_cap = (self._daily_capital if self._daily_capital is not None
                              else oppfade_capital())
        _oppfade_usable = _oppfade_daily_cap * OPPFADE_USABLE_FRAC
        _oppfade_stake = max(MIN_ORDER_USD,
                             round(_oppfade_usable * OPPFADE_STAKE_FRAC, 2))
        payload = {
            "updated_at": datetime.now(timezone.utc).isoformat(),
            # Top-level identity is mandatory: BTC and ETH share a state schema,
            # and a missing discriminator made cross-asset dashboard mistakes
            # needlessly difficult to detect.
            "asset": ASSET,
            "dry_run": DRY_RUN,
            "live_book": live_book,
            # feed health, so an empty book panel is diagnosable without journals
            "book_feed": (self.bookfeed.stats() if self.bookfeed is not None else None),
            "memory_feed": (_RTDS_MEMORY.stats() if _RTDS_MEMORY is not None else None),
            "ws_tick_study": (self.bookfeed.tick_study.stats()
                if self.bookfeed is not None and getattr(self.bookfeed, 'tick_study', None) is not None else None),
            # ONE place that answers "is this bot actually working right now".
            # loop  — is the trading loop running and is the event loop free?
            # feed  — is the order book live, and how close to the stale cutoff?
            # errors— which guarded paths have been failing, and how often?
            # live/dry_run/asset are repeated here so the health strip is
            # self-contained and cannot disagree with the rest of the payload.
            "health": {
                "loop": HEALTH.snapshot(),
                "redemption_recovery": dict(getattr(self, "_redeem_recovery_health", {})),
                "feed": (self.bookfeed.stats() if self.bookfeed is not None
                         else {"subscribed": False}),
                "errors": _errs_snapshot(),
                "live": bool(self.live),
                "dry_run": DRY_RUN,
                "asset": ASSET,
                # Calendar fields describe T3 only. Kept under the legacy names
                # for dashboard compatibility; they must not be read as a global
                # or swingfade block.
                "skip_dow": sorted(PM_SKIP_DOW),
                "dow_blocked": not dow_stake_ok()[0],
                "dow_reason": dow_stake_ok()[1],
                "ts": time.time(),
            },
            "bankroll_usd": round(self.bankroll, 2),      # compounded (start + realized P&L)
            "bankroll_start_usd": BANKROLL_USD,
            # TWO-BOOK: the cumulative compounded pool across BOTH books + the split
            "two_book": ({
                "on": True,
                "total_capital": round(get_capital(), 2),          # dash bankroll (total, split below)
                "capital": round(t3_capital(), 2),                 # t3 share of total
                "fade_capital": round(fade_capital(), 2),          # fade share of total
                "fade_share": FADE_CAPITAL_SHARE,
                # FLIP book (shadow) — its 1/3 notional; modelled P&L (flip_pnl /
                # flip_base) is filled in dashboard-side from the RE-flip table.
                "flip_capital": round(flip_capital(), 2),          # flip share of total
                "flip_share": FLIP_CAPITAL_SHARE,
                # REAL 24h-daily-locked sizing base — flip share + flip P&L settled
                # before today's 00:00 UTC, the exact figure _place_flip stakes off
                # (flip_base x FLIP_STAKE_FRAC). Emitted so the dash card can show
                # the base we ACTUALLY size on in live mode instead of re-deriving
                # capital + all-time P&L, which is not day-locked and drifts from
                # the stake the moment a flip settles today. While the book is
                # SHADOW the dash overrides this with the modelled base.
                "flip_base": round(self.flip_base, 2),
                "bankroll_total": round(self.bankroll_total, 2),   # t3 bankroll (t3 share + t3 P&L)
                "t3_pnl": round(self.t3_pnl, 2),
                "fade_pnl": round(self.fade_pnl, 2),
                "hfade_pnl": round(self.hfade_pnl, 2),
                "hfade_on": HIGH_FADE,
                "total_pnl": round(self.t3_pnl + self.fade_pnl + self.hfade_pnl, 2),
                "fade_base": round(self.fade_base, 2),             # fade bankroll (fade share + fade P&L, compounded)
                "t3_frac": T3_STAKE_FRAC, "fade_frac": FADE_STAKE_FRAC,
                "fade_maker": FADE_MAKER, "decision_feed": DECISION_FEED,
                # per-book LIVE/SHADOW toggle state (dash-settable, read every tick).
                # A book trades real only when the master live gate AND this are on.
                "t3_live": _book_live("t3", False),   # fail-safe default, see manage()
                # T3 BANK BOOK (2026-07-29). Published on the same principle as the
                # SWINGFADE block below: the dash must render the rule this book is
                # ACTUALLY running. Before this the payload carried `t3_live` and
                # nothing else, so the header strip had no field to draw a bank-book
                # line from and printed only "t3 0%" — which is the CAPITAL SHARE and
                # is permanently 0 (the book compounds off its own P&L). An armed
                # book was therefore indistinguishable from a dead one, exactly how
                # the 07-29 non-deployment stayed hidden for a day (2026-07-30).
                "t3_book_on": T3_BOOK_ON and not t3_entries_stopped(),
                "t3_entries_stopped": t3_entries_stopped(),
                "t3_first_principles": T3_FIRST_PRINCIPLES,
                "t3_max_ask_drop": T3_MAX_ASK_DROP,
                "t3_compound_on": t3_compound_on(),
                "t3_compound_bankroll": round(self._daily_book_capital(
                    ["t3"], T3_COMPOUND_BASE_USD), 2) if t3_compound_on() else None,
                "t3_compound_halted": str(get_config(
                    "t3_compound_halted", "0")).strip().lower() in ("1", "true", "yes"),
                "t3_compound_halt_reason": get_config("t3_compound_halt_reason", None),
                "btc_t3_shadow_challenger": btc_t3_shadow_challenger_active(),
                "btc_t3_shadow_tiers": sorted(BTC_T3_SHADOW_TIERS),
                "btc_t3_shadow_stake_usd": BTC_T3_SHADOW_STAKE_USD,
                "btc_t3_shadow_ask_warn": BTC_T3_SHADOW_ASK_WARN,
                "mc_gate": MC_GATE_ENABLED,
                "mc_t3_vol_max": MC_T3_VOL_MAX,
                "mc_fade_vol_min": MC_FADE_VOL_MIN,
                "t3_book_adx_min": T3_BOOK_ADX_MIN,
                "t3_book_velocity_min": T3_BOOK_VELOCITY_MIN,
                "t3_book_min_abs_bps": T3_BOOK_MIN_ABS_BPS,
                "t3_book_primary_only": T3_BOOK_PRIMARY_ONLY,
                "t3_revision_ask_min": T3_REVISION_ASK_MIN,
                "t3_btc_ask_ceil": T3_BTC_ASK_CEIL,
                "t3_ask_floor": T3_ASK_FLOOR,
                "t3_ask_ceil": T3_ASK_CEIL,
                "t3_book_ask_hole_lo": T3_BOOK_ASK_HOLE_LO,
                "t3_book_ask_hole_hi": T3_BOOK_ASK_HOLE_HI,
                "t3_book_skip_dow": sorted(T3_BOOK_SKIP_DOW),
                "t3_book_tier_rules": {k: list(v) for k, v in T3_BOOK_TIER_RULES.items()},
                "t3_book_stake_frac": T3_BOOK_STAKE_FRAC,
                "t3_book_stake_ceil": T3_BOOK_STAKE_CEIL,
                "t3_book_min_stake": T3_BOOK_MIN_STAKE,
                # execution rule — same reason the gates above are published: the
                # dash must render the rule the book is ACTUALLY running.
                "t3_payup": T3_PAYUP,
                "t3_min_fill_frac": T3_MIN_FILL_FRAC,
                # the book's OWN compounding bank + the stake it would send now
                "t3_book_bank": round(max(0.0, self.bankroll_total), 2),
                "t3_book_stake": round(
                    (BTC_T3_SHADOW_STAKE_USD
                     if btc_t3_shadow_challenger_active()
                     else (crossbook_stake_for("t3", self._live_usdc)
                           if crossbook_stake_for("t3", self._live_usdc) is not None
                           else t3_book_stake_for(self.bankroll_total,
                                                  self._live_usdc))), 2),
                "fade_live": _book_live("fade", not FADE_SHADOW),
                "flip_live": _book_live("flip", False),
                # CHEAP-OPP FADE — the only book with capital since 2026-07-20.
                # Defaults OFF: the owner arms it manually from the dash (`oppfade_live`),
                # which is what turns the modelled book into real orders.
                "oppfade_live": _book_live("oppfade", False),
                "oppfade_retired": str(get_config("oppfade_retired", "0")).lower()
                                   in ("1", "true", "on"),
                # DAILY-LOCKED model (2026-07-22): capital = exchange USDC at
                # 00:00 UTC; usable = 2/3 of it; stake = 10% of usable. All figures
                # published so the dash cards render the live model, not the old
                # capital_usd×share base. See [[project_pm_cheap_opp_fade_prereg]].
                "oppfade_capital": round(_oppfade_daily_cap, 2),
                "oppfade_daily_capital": round(_oppfade_daily_cap, 2),
                "oppfade_usable_frac": OPPFADE_USABLE_FRAC,
                "oppfade_usable": round(_oppfade_usable, 2),
                "oppfade_stake": _oppfade_stake,
                "oppfade_share": OPPFADE_CAPITAL_SHARE,
                "oppfade_base": round(_oppfade_usable, 2),
                "oppfade_frac": OPPFADE_STAKE_FRAC,
                # Live funded-wallet USDC (None in dry-run — no executor exists, so
                # the go-live dialog falls back to the daily-locked capital).
                "wallet_usdc": (round(self._live_usdc, 2)
                                if self._live_usdc is not None else None),
                "oppfade_ask_max": OPPFADE_ASK_MAX,
                "oppfade_ask_min": OPPFADE_ASK_MIN,
                "oppfade_elapsed_min": OPPFADE_MIN_ELAPSED_S,
                "oppfade_elapsed_max": OPPFADE_MAX_ELAPSED_S,
                # asymmetric adx-slope gate (2026-07-22) — surfaced so the dash
                # shows the band the live book is actually running
                "oppfade_adx_slope_max": OPPFADE_ADX_SLOPE_MAX,
                "oppfade_adx_slope_min": OPPFADE_ADX_SLOPE_MIN,
                "oppfade_adx_flat_band": OPPFADE_ADX_FLAT_BAND,
                "oppfade_retrace_max": OPPFADE_RETRACE_MAX,
                "oppfade_pnl": round(self.oppfade_pnl, 2),
                # SWINGFADE (2026-07-23) — separate staking category, disarmed by
                # default. Publishing the whole gate so the dash renders the rule
                # this book is ACTUALLY running rather than a stale copy of it.
                "swingfade_live": _book_live("swingfade", False),
                "swingfade_ask_min": swingfade_trial_band(ASSET, SWINGFADE_ASK_MIN, SWINGFADE_ASK_MAX)[0],
                "swingfade_ask_max": swingfade_trial_band(ASSET, SWINGFADE_ASK_MIN, SWINGFADE_ASK_MAX)[1],
                "swingfade_trial": swingfade_trial_active(ASSET),
                "fade_pin_touch": FADE_PIN_TOUCH,
                "fade_touch_payup": FADE_TOUCH_PAYUP,
                "swingfade_retry_max": SWINGFADE_RETRY_MAX,
                "swingfade_elapsed_min": SWINGFADE_ELAPSED_MIN,
                "swingfade_elapsed_max": SWINGFADE_ELAPSED_MAX,
                "swingfade_min_pre_ask": SWINGFADE_MIN_PRE_ASK,
                "swingfade_exc_max": SWINGFADE_EXC_MAX,
                "swingfade_adverse_max": _swingfade_adverse_max(),
                "swingfade_first_touch_ceil": _swingfade_first_touch_ceil(),
                "swingfade_adx_abs": SWINGFADE_ADX_ABS,
                "swingfade_cross_exclude": sorted(SWINGFADE_CROSS_EXCLUDE),
                "swingfade_stake_ceil": SWINGFADE_STAKE_CEIL,
                "swingfade_refill": SWINGFADE_REFILL,
                # Same call the live sizer makes — a dash that recomputes the
                # formula inline is how the strip drifts from what we bet.
                "swingfade_stake": swingfade_stake_for(_oppfade_daily_cap),
                "swingfade_min_stake": SWINGFADE_MIN_STAKE,
                # Forward-only price-band books. Publishing the full immutable
                # specs makes the dashboard/state file an audit of what is being
                # collected; there is deliberately no live flag for these books.
                "swingband_shadow_only": not any(
                    _book_live(s["book"], False) for s in SWINGBAND_SPECS),
                # Per-mode flags are required by the dashboard: several bands can
                # be armed at once, so the old aggregate shadow_only boolean cannot
                # say which card represents real money.
                "swingband_live": {
                    s["book"]: _book_live(s["book"], False)
                    for s in SWINGBAND_SPECS
                },
                "swingband_regime": eth_band_regime_snapshot(),
                "crossbook_enabled": str(get_config("crossbook_enabled", "0")).lower()
                                     in ("1", "true", "on"),
                "crossbook_capital": {
                    b: float(get_config(f"{b}_capital_usd", 0) or 0)
                    for b in ("t3", "oppfade", "swingfade",
                              "swingband_21_25", "swingband_25_30",
                              "swingband_30_40", "swingband_40_50")},
                "swingband_shadow_stake": SWINGBAND_SHADOW_STAKE_USD,
                # Actual cross-book stake configured for each band. The dashboard
                # previously showed the legacy $10 shadow-model ticket even while
                # every band was LIVE at its real $1 stake.
                "swingband_stake": {
                    s["book"]: float(get_config(f"{s['book']}_stake_usd",
                                                SWINGBAND_SHADOW_STAKE_USD)
                             or SWINGBAND_SHADOW_STAKE_USD)
                    for s in SWINGBAND_SPECS
                },
                "swingband_specs": [
                    {k: (sorted(v) if isinstance(v, frozenset) else v)
                     for k, v in spec.items()}
                    for spec in SWINGBAND_SPECS
                ],
                "bankroll_seed_usd": BANKROLL_SEED_USD,
                "compounding_on": bool(_oppfade_daily_cap > BANKROLL_SEED_USD),
                "master_live": bool(self.live),
            } if TWO_BOOK else {"on": False}),
            # Current-rule real-money accounting. Historical rows remain preserved
            # under real_lifetime.archive (see _real_lifetime).
            "real_lifetime": self._real_lifetime(),
            "params": {
                "decision_offset_s": DECISION_OFFSET_S, "tp_bid": TP_BID,
                "sl_bid": SL_BID, "sl_reversal_bps": SL_REVERSAL_BPS,
                "max_entry_ask": MAX_ENTRY_ASK, "min_entry_ask": MIN_ENTRY_ASK,
                "kelly_fraction": KELLY_FRACTION, "min_edge_margin": MIN_EDGE_MARGIN,
                "stake_frac": STAKE_FRAC,
                "max_entries_per_window": MAX_ENTRIES_PER_WINDOW,
                "late_entry_cutoff_s": LATE_ENTRY_CUTOFF_S,
                "reentry_cooldown_s": REENTRY_COOLDOWN_S,
            },
            "twap_calibration": dict(getattr(self, '_twap_model', {}),
                                     continuous=TWAP_CONTINUOUS_CALIBRATION,
                                     enforce=TWAP_CALIBRATION_ENFORCE),
            "twap_entry_quality": getattr(self, "_twap_effective_quality", twap_quality.load()),
            "twap_strategy_flags": {"edge": TWAP_EDGE, "lock": TWAP_LOCK},
            "summary": {
                "windows_tracked": windows_tracked,
                "staked": len(staked),
                "staked_pct": round(100 * len(staked) / windows_tracked, 1) if windows_tracked else None,
                "reentries": len([l for l in staked if not l.get("is_t3")]),
                "net_pnl_usd": round(sum(l["realized_pnl_usd"] for l in closed), 2),
                "closed": len(closed),
                "wins": nwin, "losses": nloss,
                "staked_wr": round(100 * nwin / (nwin + nloss), 1) if (nwin + nloss) else None,
                "avg_win": round(avg_win, 3) if avg_win is not None else None,
                "avg_loss": round(avg_loss, 3) if avg_loss is not None else None,
                "decided": len(t3),
                "predicted_hit": t3_wins,
                "predicted_acc": round(t3_wins / len(t3), 4) if t3 else None,
                "decided_qual": len(t3_qual),
                "predicted_hit_qual": t3_qual_wins,
                "predicted_acc_qual": round(t3_qual_wins / len(t3_qual), 4) if t3_qual else None,
                # calibration of the qualified tiers against the book (ask-bearing
                # legs only — qual_won_askset is the win-rate of THESE legs, not of
                # decided_qual above, and is the only figure qual_priced may be
                # differenced against). qual_edge_pp > 0 = the accuracy is worth
                # money; <= 0 = the book already prices it and there is nothing to
                # capture. See [[project_pm_ask_is_efficient_closed]].
                "qual_ask_n": len(qual_ask),
                "qual_priced": round(qual_priced, 4) if qual_priced is not None else None,
                "qual_won_askset": round(qual_won, 4) if qual_won is not None else None,
                "qual_edge_pp": (round(100 * (qual_won - qual_priced), 1)
                                 if qual_priced is not None else None),
                "qual_z": round(qual_z, 2) if qual_z is not None else None,
                "by_tier": by_tier,
            },
            # every staked leg (all-time) + the 80 most recent legs for context
            "windows": [self._row_json(l) for l in sorted(
                {(l["window_anchor"], l["leg"]): l
                 for l in (legs[:80] + staked)}.values(),
                key=lambda x: (x["window_anchor"], x.get("leg") or 0),
                reverse=True)],
        }
        d = os.path.dirname(STATE_PATH)
        if d:
            os.makedirs(d, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=d or ".", suffix=".tmp")
        with os.fdopen(fd, "w") as f:
            json.dump(payload, f, indent=2, default=str)
        os.replace(tmp, STATE_PATH)

    @staticmethod
    def _row_json(leg):
        keys = ["window_anchor", "leg", "is_t3", "slug", "status",
                "predicted_side", "tier", "tier_acc", "decision_bps", "adx",
                "adx_confirmed", "staked", "no_stake_reason", "entry_price",
                "shares", "stake_usd", "exit_reason", "exit_price", "price_open",
                "price_decision", "price_settle", "actual_outcome", "hit",
                "realized_pnl_usd", "maker_fillable", "queue_ahead", "vol_rate",
                "gate_would_block", "price_exit_btc",
                "decision_bps_binance", "predicted_side_binance", "tier_binance",
                "hit_binance", "book", "bin_mom_bps", "fade_confirmed", "adx_short",
                "intended_stake_usd", "twap_decision_ask", "twap_fill_cut", "twap_exit_error",
                "twap_hold_pnl", "twap_exit_delta",
                # decision-time ask + both-side taker depth, so an UNSTAKED row can
                # still show the ask we'd have paid and a real Fill Δ (2026-07-28)
                "ask_flip", "ask_opp", "fillable_sh", "fillable_opp_sh",
                "touch_sh", "touch_opp_sh"]
        out = {k: leg.get(k) for k in keys}
        # timestamps as ISO strings (leg may be an in-memory dict with datetimes
        # or a DB row); tolerate str/None too
        for tk in ("window_start_utc", "decided_at", "exit_at"):
            v = leg.get(tk)
            out[tk] = v.isoformat() if hasattr(v, "isoformat") else v
        return out

    def _poll_interval(self):
        """Adaptive poll: FAST_POLL_S near the ENTRY decision (T+3), while MANAGING
        an open leg, and NEAR CLOSE — so entries fire promptly and losing trades
        can bail in time. Otherwise POLL_S."""
        now = time.time()
        anchor = int(now - (now % WINDOW_S))
        off = now - anchor
        # sample the T+2 -> T+3 probe window at FAST cadence (book is warm from WS;
        # cheap) so we don't miss the ask crossing the R:R cap
        # wake EXACTLY at T+3 rather than up to one poll after it (2026-09-28)
        if 0 < DECISION_OFFSET_S - off <= FAST_POLL_S:
            return DECISION_OFFSET_S - off + 0.02
        if TWAP_EDGE and TWAP_EDGE_T0_S-1 <= off < TWAP_EDGE_T1_S:
            return min(FAST_POLL_S, .25)
        if T2_PROBE and T2_PROBE_OFFSET_S <= off < DECISION_OFFSET_S:
            return FAST_POLL_S
        if abs(off - DECISION_OFFSET_S) <= 15:                 # around entry T+3
            return FAST_POLL_S
        for w in self.windows.values():
            if w.get("open_leg") is not None:                  # managing a position
                return FAST_POLL_S
            if not w.get("settled") and 0 <= (w["expiry"] - now) <= NEAR_CLOSE_FAST_S:
                return FAST_POLL_S                              # near close
        return POLL_S

    # ----- main loop -----
    async def _log_fd_block(self, w, p, book, fside, ask_opp, opp_token,
                            fill_flip_sh, fill_opp_sh, why, elapsed_s=None):
        """Record a swingfade/swingband candidate that passed every OTHER gate but
        was refused by fade_fill_delta_gate (2026-09-28). Before this the refusal
        only reached the journal — once per window, and only if it was the first
        refusal — so the gate could never be judged against what it blocked.
        NO_STAKE + a `fdblock_<book>` tag: settles (outcome/hit) like any eval leg,
        never counts on a book card or in P&L. entry_price = the touch it would
        have bought."""
        try:
            leg = self._new_leg(w, len(w["legs"]), is_t3=False)
            await _offload(self._stamp_signal, leg, p)
            leg["predicted_side"] = "Down" if fside == "Up" else "Up"   # fade side
            leg["token_id"] = str(opp_token) if opp_token is not None else None
            leg["book"] = f"fdblock_{book}"
            leg["entry_price"] = round(ask_opp, 4) if ask_opp is not None else None
            leg["ask_opp"] = ask_opp
            leg["fillable_sh"] = fill_flip_sh
            leg["fillable_opp_sh"] = fill_opp_sh
            leg["decision_elapsed_s"] = elapsed_s
            leg["staked"] = False
            leg["status"] = "NO_STAKE"
            leg["no_stake_reason"] = f"{book} blocked · {why}"
            w["legs"].append(leg)
            upsert(leg)
            print(f"[{datetime.now(timezone.utc):%H:%M:%S}] FD-BLOCK {book} "
                  f"{w['slug']} fade {leg['predicted_side']} ask "
                  f"{'n/a' if ask_opp is None else format(ask_opp, '.3f')} · {why}",
                  flush=True)
        except Exception as e:
            print(f"[fd-block-log] {type(e).__name__}: {str(e)[:80]}", flush=True)

    def _tmark(self, k):
        """T3-TIMING stage stamp (2026-09-28): T3 orders land ~5s after T+3 and
        we don't know where the time goes. One log line per T3 decision."""
        self._tm[k] = time.time()

    async def _refresh_twap_calibration(self):
        """Background fitting and PM labels never block a decision tick."""
        try:
            policy = await _offload(twap_edge_policy)
            self._twap_model = await _offload(
                twap_cal.refresh, polymarket_window_reference,
                policy['shadow_max_ask'], POLY_FEE_RATE, policy)
            print('TWAP-CALIBRATION ' + json.dumps(self._twap_model), flush=True)
        except Exception as e:
            # Predict refuses the last model automatically once its TTL expires.
            _err('twap_calibration.refresh', e)
            print(f'TWAP-CALIBRATION-ERROR {type(e).__name__}: {str(e)[:100]}', flush=True)

    async def _background_redemption_run(self):
        """One queue drainer; BTC schedules this outside its decision loop."""
        # Drain due on-chain redemptions for real hold-to-settle winners. Gamma's
        # `closed=true` market lookup can lag the Chainlink settlement by seconds,
        # so failure is not terminal: requeue with bounded exponential backoff.
        # Process only the queue length present at entry so a failed item cannot
        # spin repeatedly in one tick.
        _redeem_now = time.monotonic()
        for _ in range(len(self._redeem_q)):
            item = self._redeem_q.pop(0)
            if len(item) == 2:             # tolerate pre-change in-memory tuples
                cid, slug = item
                attempt, not_before = 0, 0.0
            else:
                cid, slug, attempt, not_before = item
            if not_before > _redeem_now:
                self._redeem_q.append((cid, slug, attempt, not_before))
                continue
            if not (self.live and self.executor is not None):
                self._redeem_q.append((cid, slug, attempt, not_before))
                continue
            self._redeem_inflight.add(str(cid))
            try:
                ok, info = await self.executor.redeem(cid)
            finally:
                self._redeem_inflight.discard(str(cid))
            print(f"[{datetime.now(timezone.utc):%H:%M:%S}] REDEEM {slug} "
                  f"{'✔ ' + str(info) if ok else '✗ ' + str(info)}", flush=True)
            if not ok:
                attempt += 1
                delay = min(300.0, 5.0 * (2 ** min(attempt - 1, 6)))
                self._redeem_q.append(
                    (cid, slug, attempt, time.monotonic() + delay))
                print(f"[{datetime.now(timezone.utc):%H:%M:%S}] REDEEM-RETRY "
                      f"{slug} attempt={attempt + 1} in {delay:.0f}s", flush=True)


    async def tick(self):
        import sys
        sys.path.insert(0, ".")
        import pm_window_observer
        pm_window_observer.schedule(self, sys.modules[__name__])
        import pm_shadow_arms  # read-only BTC EDGE shadow arms; never raises
        pm_shadow_arms.tick(self, sys.modules[__name__])
        import pm_settlement_worker
        pm_settlement_worker.schedule(self, sys.modules[__name__])
        now = time.time()
        await self._retry_twap_cuts(now)
        anchor = int(now - (now % WINDOW_S))
        self._tm = {"tick": now}
        if (ASSET == 'BTC' and TWAP_CONTINUOUS_CALIBRATION
                and now - self._twap_refresh_ts >= twap_cal.REFRESH_S
                and not 165 <= now - anchor <= 270
                and (self._twap_refresh_task is None or self._twap_refresh_task.done())):
            self._twap_refresh_ts = now
            self._twap_refresh_task = asyncio.create_task(self._refresh_twap_calibration())
        # T+3 QUIET ZONE: no slow housekeeping from T+3-10s to T+3+10s, so the
        # decision tick starts on time and runs clean (see BANKROLL_REFRESH_S).
        _near_t3 = (DECISION_OFFSET_S - T3_QUIET_S <= now - anchor
                    < DECISION_OFFSET_S + T3_QUIET_S)

        # PM's official resolution verdict applies to SHADOW and LIVE stakes.
        # Sweep closed/TP/SL rows too; fill reconciliation remains live-only.
        if ASSET != 'BTC' and now - self._pm_recon_ts >= 15.0 and not _near_t3:
            await _offload(reconcile_pending_pm_outcomes)
            self._pm_recon_ts = now

        # dash go-live toggle: if the desired mode changed, exit so systemd restarts
        # us cleanly in the new mode (re-inits the live executor / approvals). The
        # dashboard also resets run_start on toggle, so the table shows the fresh run.
        if (await _offload(_resolve_dry_run)) != DRY_RUN:
            print(f"[live-toggle] mode change: DRY_RUN {DRY_RUN} -> "
                  f"{_resolve_dry_run()}; exiting for restart", flush=True)
            try:
                await _offload(self.write_state)
            except Exception:
                pass
            os._exit(0)

        # LIVE-READ staking switch. Collection (T+2->T+3 ask probe + eval legs) runs
        # below REGARDLESS; this only gates NEW stakes. Flip `trading_enabled=0` in
        # config/dash to stop staking (e.g. before the book turns illiquid, or to just
        # collect the timing dataset) WITHOUT stopping the process/collection. Open
        # positions still get managed to exit; only fresh entries are suppressed.
        collect_only = DATA_ONLY or not await _offload(trading_enabled)
        if collect_only != self.collect_only:
            print(f"[trading-toggle] staking "
                  f"{'DISABLED — collect-only (T+2 probe keeps running)' if collect_only else 'ENABLED'}",
                  flush=True)
            self.collect_only = collect_only

        # compound: the bankroll rides the STRATEGY's equity (taker/skip), not the
        # illusory maker P&L — refresh it before sizing.
        # THROTTLED (2026-09-28): the two bankroll reads cost ~1-2s (3.3s under
        # load) and ran EVERY tick, so the T+3 decision started ~7s late (T3-TIMING).
        # Refresh every BANKROLL_REFRESH_S, and never in the T+3 quiet zone; P&L
        # only moves on settlement, so a <=20s-old bankroll sizes identically.
        if (not hasattr(self, "_bank_ts")
                or (now - self._bank_ts >= BANKROLL_REFRESH_S and not _near_t3)):
            self.bankroll = await _offload(self._strategy_bankroll)
            if TWO_BOOK:
                (self.bankroll_total, self.fade_base, self.flip_base, self.oppfade_base,
                 self.t3_pnl, self.fade_pnl, self.hfade_pnl, self.flip_pnl,
                 self.oppfade_pnl) = await _offload(self._two_book_bankrolls)
            self._bank_ts = now
        self._tmark("bank")

        # RECONCILED wallet balance (2026-07-22): PM's own COLLATERAL balance
        # is ground truth. Refreshed whenever an executor exists — including a
        # DRY-RUN read-only executor (see run()) — purely so the dashboard's "PM
        # balance" strip is populated. The stake-affordability cap and daily
        # capital snapshot below stay gated on `self.live`: a dry-run process must
        # never mutate the config a live process sizes real stakes from.
        _wallet_just_refreshed = False
        if (getattr(self, "executor", None) is not None and not _near_t3
                and now - self._live_usdc_ts > LIVE_USDC_REFRESH_S):
            try:
                _b = await self.executor.usdc_balance()
                if _b is not None:
                    self._live_usdc = _b
                    self._live_usdc_ts = now
                    _wallet_just_refreshed = True
            except Exception:
                pass
        if self.live and _wallet_just_refreshed:
            # DAILY-LOCKED oppfade capital: snapshot the exchange balance ONCE per
            # UTC day and hold it fixed (no per-stake compounding — steps only at
            # 00:00 UTC). Persisted in config so a mid-day restart reuses the day's
            # figure instead of re-reading a drawn-down balance.
            _today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            if self._daily_capital_day != _today:
                _stored_day = get_config("oppfade_daily_cap_day", None)
                if _stored_day == _today:
                    _sv = get_config("oppfade_daily_cap", None)
                    if _sv is not None:
                        self._daily_capital = float(_sv)
                        self._daily_capital_day = _today
                if (self._daily_capital_day != _today
                        and self._live_usdc is not None):
                    self._daily_capital = self._live_usdc
                    self._daily_capital_day = _today
                    await _offload(set_config, "oppfade_daily_cap", self._live_usdc)
                    await _offload(set_config, "oppfade_daily_cap_day", _today)
                    print(f"[{datetime.now(timezone.utc):%Y-%m-%d %H:%M}] OPPFADE "
                          f"daily capital snapshot: ${self._live_usdc:.2f} "
                          f"(usable {OPPFADE_USABLE_FRAC:.0%} → stake "
                          f"${self._live_usdc*OPPFADE_USABLE_FRAC*OPPFADE_STAKE_FRAC:.2f})",
                          flush=True)
            # Bankroll-to-wallet reconciliation belongs to the same UTC-day
            # boundary as compounding. Do it once on the first successful PM
            # balance refresh of the new UTC day—not on every intraday refresh.
            # A separate day stamp makes a restart shortly after midnight safe
            # even when the daily-cap snapshot was already persisted.
            if self._live_usdc is not None:
                _cap_day = get_config("capital_wallet_cap_day", None)
                if _cap_day != _today:
                    _capital, _capital_cut = await _offload(
                        cap_capital_to_wallet, self._live_usdc)
                    await _offload(set_config, "capital_wallet_cap_day", _today)
                    if _capital_cut:
                        print(f"[capital-safety] UTC daily compounding: configured "
                              f"bankroll exceeded PM balance ${self._live_usdc:.2f}; "
                              f"reduced to 90% = ${_capital:.2f}", flush=True)

        # settle & prune expired windows
        for a, w in list(self.windows.items()):
            if ASSET != 'BTC' and now >= w["expiry"] and not w["settled"]:
                await _offload(self.settle, w)
            if now - w["expiry"] > 6 * 3600:
                del self.windows[a]

        # DB-driven recovery for positions the in-memory path never booked (restart
        # during a Chainlink outage, >6h prune, or a settle bug). Wrapped so it can
        # never disturb the trading path.
        if ASSET != 'BTC' and now - getattr(self, "_deferred_sweep_ts", 0.0) >= DEFERRED_SETTLE_EVERY_S:
            self._deferred_sweep_ts = now
            try:
                await self._settle_deferred()
            except Exception as e:
                _err("settle_deferred", e)
                print(f"[settle-defer] sweep error {type(e).__name__}: "
                      f"{str(e)[:80]}", flush=True)

        if ASSET != 'BTC':
            self._schedule_redemption_recovery(time.time())
            await self._background_redemption_run()

        # Housekeeping may cross a boundary; never discover the old tick anchor.
        now = time.time()
        anchor = int(now - (now % WINDOW_S))
        # ensure current window exists + has tokens + open price + ADX
        if anchor not in self.windows:
            self.windows[anchor] = self.new_window(anchor)
        w = self.windows[anchor]

        if w["up_token"] is None:
            try:
                mt = await self.market_tokens(anchor)
                if mt:
                    w.update(mt)
            except Exception as e:
                _err("window.market_tokens", e)
        if w["price_open"] is None:
            # Entry directions and bps use PM's published priceToBeat only.
            # Missing metadata leaves the window unready; retry without using
            # a reconstructed RTDS average or raw boundary tick as a substitute.
            try:
                w["price_open"] = await _offload(underlying_price_at, anchor)
            except Exception as e:
                _err("price.pm_open", e)
                w["price_open"] = None
        # price_open_binance (the Binance own-feed open that anchored every *_binance
        # look-ahead column) is no longer fetched — REMOVED 2026-07-30.

        # Warm SDK metadata well before EDGE decisions; never caches prices.
        if self.live and self.executor is not None and w.get('up_token') and w.get('down_token'):
            import pm_order_metadata
            cache = getattr(self.executor,'_metadata_cache',None)
            if cache is None:
                cache = self.executor._metadata_cache = pm_order_metadata.MetadataCache()
            cache.schedule(self.executor.c,[w['up_token'],w['down_token']],anchor)
            if self.bookfeed is not None:
                self.bookfeed.execution_metadata_cache = cache

        # keep the live WS book subscribed to THIS window's up/down tokens
        # (resubscribes automatically as windows roll every 5m)
        if self.bookfeed is not None and w["up_token"] is not None:
            try:
                await self.bookfeed.ensure([w["up_token"], w["down_token"]])
            except Exception as e:
                _err("bookfeed.ensure", e)

        now = time.time()
        if int(now - (now % WINDOW_S)) != anchor:
            return  # metadata arrived after rollover; retry the current window
        ready = w["price_open"] is not None and w["up_token"] is not None
        elapsed = now - anchor
        self._tmark("win")
        if ready:
            # FLIP-CONTEXT SAMPLER (isolated logging; never stakes/mutates trading
            # state). Runs EVERY tick from window open so the pre-T+3 excursion (the
            # move a later flip reverses) is captured, plus one ADX(short) sample per
            # minute for the intra-window slope. Wrapped so an error here can never
            # disturb the trading path below.
            try:
                _pp = await _offload(underlying_price_now)
                if _pp is not None and w["price_open"]:
                    _mv = (_pp - w["price_open"]) / w["price_open"] * 1e4
                    if _mv > w["path_max_up_bps"]:
                        w["path_max_up_bps"] = _mv
                    if -_mv > w["path_max_dn_bps"]:
                        w["path_max_dn_bps"] = -_mv
                    # beat-price crossings for swingfade_ok(). Exact-zero ticks
                    # carry no sign and must not reset the run, or a price sitting
                    # on the open would inflate the count.
                    _sg = 1 if _mv > 0 else (-1 if _mv < 0 else 0)
                    if _sg:
                        if w["cross_last_sign"] and _sg != w["cross_last_sign"]:
                            w["cross_count"] += 1
                        w["cross_last_sign"] = _sg
                _min = int(elapsed // 60)
                if _min != w["adx_last_min"] and 0 <= elapsed < WINDOW_S:
                    w["adx_last_min"] = _min
                    _a7 = adx_now(await _offload(get_klines_1m, 100),
                                  period=ADX_SHORT_PERIOD)
                    if _a7 is not None:
                        w["adx_series"].append((int(elapsed), round(_a7, 2)))
            except Exception as e:
                _err("flip_ctx", e)
                print(f"[flip-ctx] {type(e).__name__}: {str(e)[:80]}", flush=True)
            self._tmark("flip")

            # refresh ADX once per window at/after T+3 (used by every leg)
            if w["adx"] is None and elapsed >= DECISION_OFFSET_S:
                df = await _offload(get_klines_1m, 100)   # reuse one fetch for both periods
                adx = adx_now(df)
                w["adx"] = round(adx, 2) if adx is not None else None
                w["adx_confirmed"] = adx_confirmed(adx)
                adx_s = adx_now(df, period=ADX_SHORT_PERIOD)  # short-period (intra-window)
                w["adx_short"] = round(adx_s, 2) if adx_s is not None else None
                # regime vol for dynamic classification: stdev of trailing 3-min
                # (3-bar) returns, in bps — normalizes the T+3 move to "big for now"
                try:
                    r3 = df["close"].astype(float).pct_change(3).dropna()
                    if len(r3) >= 5:
                        w["vol_3min_bps"] = round(float(r3.std()) * 1e4, 2)
                except Exception:
                    pass
                self._tmark("adx")

            # T+2 -> T+3 SHADOW PROBE (calibration; pure logging, never stakes).
            # Fires every tick in [T2_PROBE_OFFSET_S, DECISION_OFFSET_S) while flat
            # and before the live T+3 decision; dedupes on state internally. Fully
            # isolated — an error here must NEVER disturb the trading path below.
            if (T2_PROBE and w["open_leg"] is None and not w["t3_done"]
                    and T2_PROBE_OFFSET_S <= elapsed < DECISION_OFFSET_S):
                try:
                    await self._t2_probe(w, now)
                except Exception as e:
                    print(f"[t2-probe] {type(e).__name__}: {str(e)[:80]}",
                          flush=True)

            if w["open_leg"] is not None:
                # exactly one stake at a time: manage it (may exit -> flat)
                await self.manage(w, now)
            elif not w["t3_done"] and elapsed >= DECISION_OFFSET_S:
                # first (T+3) decision — always recorded for accuracy
                self._tmark("enter")
                await self.try_enter(w, now, is_t3=True)
                self._tmark("done")
                try:
                    _t0 = anchor + DECISION_OFFSET_S
                    _tm = self._tm
                    _st = " ".join(f"{k}={1000 * (v - _t0):+.0f}" for k, v in
                                   sorted(_tm.items(), key=lambda kv: kv[1]))
                    _age, _gap = await _offload(
                        t3_price_diag, int(_tm.get("plan", _tm["done"])))
                    print(f"[{datetime.now(timezone.utc):%H:%M:%S}] T3-TIMING "
                          f"{w['slug']} ms vs T+{DECISION_OFFSET_S}: {_st}"
                          + (f" · px_age {_age}s" if _age is not None else "")
                          + (f" · last-vs-twap {_gap:+.2f}bps" if _gap is not None else ""),
                          flush=True)
                except Exception:
                    pass
                w["t3_done"] = True
                # if T+3 did NOT stake, enable post-T+3 revision logging + seed the
                # dedupe key from the T+3 leg's (side,tier)
                w["t3_no_stake"] = w["open_leg"] is None
                if w["legs"]:
                    _lt = w["legs"][-1]
                    w["last_eval_key"] = (_lt.get("predicted_side"), _lt.get("tier"))
                    # T+3 continuation side — RE-flip is measured vs this
                    w["t3_side"] = _lt.get("predicted_side")
                    # ...and the ORIGINAL strength, for rule D's origin test
                    w["t3_tier"] = _lt.get("tier")
                    w["t3_bps"] = _lt.get("decision_bps")
            elif (w["t3_done"] and elapsed < LATE_ENTRY_CUTOFF_S
                  and w["staked_count"] < MAX_ENTRIES_PER_WINDOW
                  and (w["last_exit_ts"] is None
                       or now - w["last_exit_ts"] >= REENTRY_COOLDOWN_S)):
                # flat again within the window -> re-enter if a fresh signal
                # (incl. the reversal direction) clears all the gates
                await self.try_enter(w, now, is_t3=False)

            # RE-FLIP SHADOW (isolated logging; never stakes / mutates state). Fires
            # regardless of the entry cap/cooldown, so it captures the flipped
            # re-analysis + its ask even in capped windows.
            # The `open_leg is None` clause was REMOVED 2026-07-29: swingfade lives
            # inside this block, and gating it on a flat book meant a t3 stake killed
            # swingfade for the whole window (both fire at t=180s, so always). The
            # oppfade and flip one-shots inside re-check `open_leg is None`
            # themselves, so they keep the old flat-only behaviour — only swingfade,
            # which never had that clause, is freed to run alongside a t3 position.
            if (RE_FLIP_SHADOW and w["t3_done"]
                    and DECISION_OFFSET_S <= elapsed < LATE_ENTRY_CUTOFF_S):
                try:
                    await self._re_flip_shadow(w, now)
                except Exception as e:
                    print(f"[re-flip] {type(e).__name__}: {str(e)[:80]}",
                          flush=True)

            # TWAP-EDGE / TWAP-LOCK: independent of every other book's state.
            if TWAP_EDGE and TWAP_EDGE_T0_S <= elapsed < TWAP_EDGE_T1_S:
                try:
                    await self._twap_edge(w, now)
                except Exception as e:
                    w["twap_edge_done"] = True
                    print(f"[twap-edge] {type(e).__name__}: {str(e)[:80]}",
                          flush=True)
            if (TWAP_EDGE and elapsed >= TWAP_EDGE_T1_S
                    and not w.get("twap_edge_done")):
                w["twap_edge_done"] = True
                await self._record_twap_skip(w, 'twap_edge', w.get('twap_edge_why') or 'no tick in band')
                print(f"[{datetime.now(timezone.utc):%H:%M:%S}] TWAP-EDGE-SKIP "
                      f"{w['slug']} · {w.get('twap_edge_why') or 'no tick in band'}",
                      flush=True)
            # TWAP-LOCK: independent of every other book's position state.
            if TWAP_LOCK and TWAP_LOCK_T0_S <= elapsed < TWAP_LOCK_T1_S:
                try:
                    await self._twap_lock(w, now)
                except Exception as e:
                    w["twap_lock_done"] = True
                    print(f"[twap-lock] {type(e).__name__}: {str(e)[:80]}",
                          flush=True)

        task = getattr(self, '_state_export_task', None)
        # The export is dashboard-only. Rate-limit it, and keep it out of the entry
        # window so it cannot contend with quotes/decisions; the gap stays well
        # under the dashboard's 30s STATE_STALE_S.
        _off = time.time() % WINDOW_S
        _export_ok = (time.time() - getattr(self, '_state_export_at', 0.0) >= STATE_EXPORT_MIN_S
                      and not STATE_EXPORT_QUIET_S[0] <= _off < STATE_EXPORT_QUIET_S[1])
        if (task is None or task.done()) and _export_ok:
            self._state_export_at = time.time()
            if task is not None and not task.cancelled() and task.exception() is not None:
                _err('state_export', task.exception())
            import copy
            snapshot = copy.copy(self)
            snapshot.windows = copy.deepcopy(self.windows)
            self._state_export_task = asyncio.create_task(_offload(snapshot.write_state))

    async def _refresh_twap_config(self):
        global _TWAP_CONFIG_CACHE
        def read():
            with ENGINE.connect() as c:
                return dict(c.execute(text("SELECT key,value FROM polymarket_config WHERE key=ANY(:keys)"),
                    {'keys':list(_TWAP_CONFIG_KEYS)}).fetchall())
        values = await _offload(read)
        _TWAP_CONFIG_CACHE = dict(ts=time.time(), values=values)

    async def _twap_config_loop(self):
        while True:
            await asyncio.sleep(1)
            try:
                await self._refresh_twap_config()
            except Exception as exc:
                _err('twap_config_refresh', exc)

    async def _memory_twap_loop(self):
        while True:
            await self._market_event.wait()
            self._market_event.clear()
            if getattr(self, 'collect_only', True):
                continue
            now = time.time()
            for w in list(self.windows.values()):
                if not w.get('up_token') or not w.get('price_open'):
                    continue
                elapsed = now-w['window_anchor']
                try:
                    if TWAP_EDGE and TWAP_EDGE_T0_S <= elapsed < TWAP_EDGE_T1_S:
                        await self._twap_edge(w, now)
                    if TWAP_LOCK and TWAP_LOCK_T0_S <= elapsed < TWAP_LOCK_T1_S:
                        await self._twap_lock(w, time.time())
                except Exception as exc:
                    _err('twap_memory_event', exc)

    async def run(self):
        global _RTDS_MEMORY
        ensure_table()
        if ASSET == 'BTC':
            import sys
            sys.path.insert(0, '.')
            from pm_rtds_memory import MemoryFeed
            self._market_event = asyncio.Event()
            def seed_memory():
                with ENGINE.connect() as c:
                    return c.execute(text("SELECT observations_ts, benchmark_price FROM chainlink_prices WHERE feed_id=:fid AND observations_ts>=:lo ORDER BY observations_ts"),
                        {'fid':PRICE_FEED_KEY,'lo':int(time.time())-3600}).all()
            _RTDS_MEMORY = MemoryFeed(PRICE_FEED_KEY, await _offload(seed_memory), self._market_event.set)
            def current_quotes():
                now = time.time()
                w = self.windows.get(int(now-now%WINDOW_S))
                if w is None or self.bookfeed is None:
                    return dict(live_book=None, ts=now)
                def quote(token):
                    b = self.bookfeed._books.get(str(token), {})
                    received = b.get('ts')
                    return dict(ask=self._cached_best_ask(token), bid=self._cached_best_bid(token),
                        received_at=received, age_ms=(now-received)*1000 if received else None)
                up, down = quote(w.get('up_token')), quote(w.get('down_token'))
                source_ts = max(up.get('received_at') or 0, down.get('received_at') or 0)
                return dict(live_book=dict(ts=source_ts,window_anchor=w['window_anchor'],
                    side=w.get('t3_side'), up=up, down=down),
                    memory_feed=_RTDS_MEMORY.stats(),ts=now)
            _RTDS_MEMORY.on_query = current_quotes
            _RTDS_MEMORY.start()
            await self._refresh_twap_config()
            self._twap_config_task = asyncio.create_task(self._twap_config_loop())
            print('PM-MEMORY-FEED ready for pre-persistence RTDS ticks; no DB fallback for live signal', flush=True)
        if T2_PROBE:
            ensure_t2_probe_table()
        if RE_FLIP_SHADOW:
            ensure_re_flip_shadow_table()
        pk, wallet = load_credentials()
        cred_state = "key AVAILABLE" if pk else "NO key stored"
        print(f"Polymarket {ASSET} Up/Down bot starting | DRY_RUN={DRY_RUN} "
              f"FORCE_SHADOW={FORCE_SHADOW} | "
              f"bankroll=${BANKROLL_USD} stake_frac={STAKE_FRAC:.0%} (no $ cap) | "
              f"decision=T+{DECISION_OFFSET_S}s | "
              f"{('HOLD-TO-SETTLE t3 (no early exits)') if HOLD_TO_SETTLE else ('TP_bid='+format(TP_BID)+' SL_bid='+format(SL_BID)+' SL_rev='+format(SL_REVERSAL_BPS)+'bps')} "
              f"| creds: {cred_state}"
              f"{' wallet '+wallet if wallet else ''}", flush=True)
        # T3 CALENDAR GATE — printed unconditionally, including when it is OFF.
        # A gate that prints nothing is indistinguishable from a gate that is not
        # deployed; that is precisely how the t3 book's non-deployment stayed hidden
        # on 07-29, and this rule now governs EVERY book.
        _dows = "".join("MTWTFSS"[d] for d in sorted(PM_SKIP_DOW)) or "none"
        _dnow_ok, _dnow_why = dow_stake_ok()
        print(f"** T3 NO-STAKE CALENDAR ** skip dow {_dows} (UTC) · "
              f"today: {'BLOCKED — ' + _dnow_why if not _dnow_ok else 'open'}",
              flush=True)
        if not DRY_RUN and not pk:
            print("REFUSING LIVE: DRY_RUN=0 but no private key available "
                  "(set via /polymarket/creds or POLYMARKET_PRIVATE_KEY). "
                  "Staying read-only.", flush=True)
        elif not DRY_RUN and pk:
            try:
                self.executor = await LiveExecutor.create(pk, wallet)
                try:
                    await self.executor.setup()
                except Exception as e:
                    if _is_proxy_failure(e) and await self.executor._failover():
                        print("[order-proxy] setup retry on fallback after "
                              f"{type(e).__name__}", flush=True)
                        await self.executor.setup()
                    else:
                        raise
                bal = await self.executor.usdc_balance()
                if bal is None:
                    print("LIVE REFUSED: could not read USDC balance; staying "
                          "read-only.", flush=True)
                elif bal < MIN_USDC_TO_TRADE:
                    print(f"LIVE REFUSED: USDC balance ${bal:.2f} < "
                          f"${MIN_USDC_TO_TRADE}; staying read-only.", flush=True)
                else:
                    self.live = True
                    print(f"LIVE ENABLED ✔ USDC ${bal:.2f}, approvals set, "
                          f"wallet {self.executor.wallet}", flush=True)
            except Exception as e:
                print(f"LIVE SETUP FAILED ({type(e).__name__}: {str(e)[:120]}); "
                      "staying read-only.", flush=True)
        elif DRY_RUN and pk:
            # DRY-RUN with credentials on file: build the executor for a BALANCE
            # READ ONLY so the dashboard's "PM balance" strip has something to show
            # instead of a permanent "—". Deliberately skip .setup() — that calls
            # setup_trading_approvals(), an on-chain write — and never flip
            # self.live, so every order-placing path (all gated on `self.live and
            # self.executor`) stays inert. LiveExecutor.create() itself only writes
            # on-chain if the deposit wallet has never been deployed; for an
            # account with real reconciled fills already on record that step is a
            # no-op read (fetch_deployed -> True).
            try:
                # Build an authenticated reader without wallet deployment or approvals.
                with _order_proxy_env(""):
                    reader = await AsyncSecureClient._create(
                        private_key=pk, wallet=os.getenv("POLYMARKET_FUNDER") or None,
                        validate_credentials=True)
                self.executor = LiveExecutor(reader, getattr(reader, "wallet", None) or wallet)
                bal = await self.executor.usdc_balance()
                if bal is not None:
                    self._live_usdc = bal
                    self._live_usdc_ts = time.time()
                    print(f"DRY-RUN: PM balance ${bal:.2f} (read-only, no "
                          "approvals set, no orders placed)", flush=True)
                else:
                    print("DRY-RUN: could not read PM balance; dashboard will "
                          "show —.", flush=True)
            except Exception as e:
                self.executor = None
                print(f"DRY-RUN: PM balance read failed ({type(e).__name__}: "
                      f"{str(e)[:120]}); dashboard will show —.", flush=True)
        async with AsyncPublicClient() as client:
            self.client = client
            self.bookfeed = BookFeed(client) if WS_BOOK else None
            if ASSET == 'BTC' and self.bookfeed is not None:
                import sys
                sys.path.insert(0, '.')
                from pm_ws_tick_study import TickStudy
                self.bookfeed.tick_study = TickStudy(self, sys.modules[__name__])
                self.bookfeed.tick_study.start_labels()
                if _RTDS_MEMORY is not None:
                    _RTDS_MEMORY.on_observation = self.bookfeed.tick_study.on_reference
                self.bookfeed.market_event = self._market_event
                self._memory_twap_task = asyncio.create_task(self._memory_twap_loop())
                print('PM-WS-TICK-STUDY full subscribed stream recording enabled', flush=True)
            print(f"live WS order book: {'ON' if WS_BOOK else 'OFF (REST only)'} "
                  f"| maker mode: {'ON' if MAKER_MODE else 'OFF (taker)'}", flush=True)
            if TWO_BOOK:
                # book allocation is no longer "t3 vs fade" — print every book's actual
                # share so a retired book at 0.00 is visible at a glance on startup.
                print(f"** BOOK EXECUTION ** | bankroll ${get_capital():.0f} | "
                      f"OPP-FADE {OPPFADE_CAPITAL_SHARE:.0%} (cap ${oppfade_capital():.0f}, "
                      f"stake {OPPFADE_STAKE_FRAC:.0%}, "
                      f"{OPPFADE_ASK_MIN:.2f}<ask≤{OPPFADE_ASK_MAX:.2f}, "
                      f"t={OPPFADE_MIN_ELAPSED_S}-{OPPFADE_MAX_ELAPSED_S}s, "
                      f"retry@{OPPFADE_RETRY_MAX:.2f}"
                      f"{'' if OPPFADE_RETRY_MAX > OPPFADE_ASK_MAX else ' OFF'}, "
                      f"{'LIVE' if _book_live('oppfade', False) else 'shadow'}) · "
                      f"t3 {T3_CAPITAL_SHARE:.0%} · fade {FADE_CAPITAL_SHARE:.0%} · "
                      f"flip {FLIP_CAPITAL_SHARE:.0%} | decision feed: "
                      f"{DECISION_FEED} (PM rebroadcast only; no exchange fallback)",
                      flush=True)
                # The t3 SHARE above is 0% and always will be — the bank book funds
                # itself off its own compounding P&L (size_position(), book="t3"),
                # so the share line alone reads as "t3 is off" even when the book is
                # armed. That is exactly how the 07-29 non-deployment stayed hidden
                # for a day. Print the book's own state, same as SWINGFADE.
                if T3_BOOK_ON:
                    if btc_t3_shadow_challenger_active():
                        print("** BTC T3 SHADOW CHALLENGER ** primary T+3 only · "
                              f"tiers {','.join(sorted(BTC_T3_SHADOW_TIERS))} · "
                              f"flat ${BTC_T3_SHADOW_STAKE_USD:.2f} model ticket "
                              f"(strategy bank fallback while live t3 bank is $0) · "
                              f"ask>{BTC_T3_SHADOW_ASK_WARN:.2f} warns but does not block · "
                              "production calendar/ADX/velocity/MC/0.68 gates bypassed · "
                              "NO REAL ORDERS (t3_live=0)", flush=True)
                    _dows = "".join("MTWTFSS"[d] for d in sorted(T3_BOOK_SKIP_DOW)) or "none"
                    _abbr = {"MODERATE-STRONG": "MS", "STRONG": "ST",
                             "MODERATE": "MOD", "WEAK": "WK"}
                    _tiers = " ".join(
                        f"{_abbr.get(t, t)} ask≤"
                        f"{'any' if a is None else format(a, '.2f')}/be≤{b:.2f}"
                        for t, (a, b) in T3_BOOK_TIER_RULES.items())
                    print(f"** T3 BANK BOOK ** {_tiers} · "
                          f"ask {T3_ASK_FLOOR:.2f}–"
                          f"{T3_ASK_CEIL if T3_ASK_CEIL is not None else MAX_ENTRY_ASK:.2f} "
                          f"fillable-at-quote · skip dow {_dows} · "
                          f"mc {'ON' if MC_GATE_ENABLED else 'off'} "
                          f"t3 vol<{MC_T3_VOL_MAX:g} fade vol>={MC_FADE_VOL_MIN:g} · "
                          f"|move|≥{T3_BOOK_MIN_ABS_BPS:.1f}bps · "
                          f"{'primary T+3 only' if T3_BOOK_PRIMARY_ONLY else 're-entry allowed'} · "
                          f"revision ask≥{T3_REVISION_ASK_MIN:.2f} · "
                          f"adx≥{T3_BOOK_ADX_MIN:.0f} · "
                          + (f"ask hole [{T3_BOOK_ASK_HOLE_LO:.2f},"
                             f"{T3_BOOK_ASK_HOLE_HI:.2f}) · "
                             if T3_BOOK_ASK_HOLE_LO < T3_BOOK_ASK_HOLE_HI
                             else "ask hole OFF · ")
                          + ("velocity_frac OFF · " if T3_BOOK_VELOCITY_MIN is None
                             else f"velocity_frac≥{T3_BOOK_VELOCITY_MIN:g} (NULL passes) · ")
                          + "stake "
                          f"{T3_BOOK_STAKE_FRAC:.0%}×own bank "
                          f"(floor ${T3_BOOK_MIN_STAKE:.0f}, cap "
                          f"${T3_BOOK_STAKE_CEIL:.0f}) · "
                          # Execution is a gate like any other and belongs in the
                          # banner: the rule it replaces (filled>=5sh) was invisible
                          # here and refused 230 of 233 candidates for three weeks.
                          f"one FAK ≥{T3_MIN_FILL_FRAC:.0%} of ticket at quote "
                          f"(pay-up ≤ask+${T3_PAYUP:.2f}, hard cap "
                          f"{T3_ASK_CEIL if T3_ASK_CEIL is not None else MAX_ENTRY_ASK:.2f}) · "
                          f"{'LIVE' if _book_live('t3', False) else 'SHADOW'}"
                          f" | adx is CHAINLINK-derived since 07-30 and runs "
                          f"~0.3-0.6 below the binance series this ≥"
                          f"{T3_BOOK_ADX_MIN:.0f} was calibrated on — judge on "
                          f"forward fires only", flush=True)
                else:
                    print("** T3 BANK BOOK ** OFF "
                          "(POLYMARKET_T3_BOOK_ON=0) — no t3 entries", flush=True)
                print(f"** SWINGFADE ** ask {swingfade_trial_band(ASSET, SWINGFADE_ASK_MIN, SWINGFADE_ASK_MAX)[0]:.2f}-"
                      f"{swingfade_trial_band(ASSET, SWINGFADE_ASK_MIN, SWINGFADE_ASK_MAX)[1]:.3f} · t {SWINGFADE_ELAPSED_MIN}-"
                      f"{SWINGFADE_ELAPSED_MAX}s · min_pre_ask≥"
                      f"{SWINGFADE_MIN_PRE_ASK:.2f} · "
                      + (f"exc≤{SWINGFADE_EXC_MAX:.0f}bps · "
                         if SWINGFADE_EXC_MAX > 0 else "symmetric exc OFF · ")
                      + (f"ADVERSE≤{_swingfade_adverse_max():.0f}bps · "
                         if _swingfade_adverse_max() > 0 else "adverse OFF · ")
                      + f"|slope|≤{SWINGFADE_ADX_ABS:.0f} · cross∉"
                      f"{sorted(SWINGFADE_CROSS_EXCLUDE)} · "
                      # Print the first-touch condition like every other gate. It was
                      # absent from this banner for its first hours live, and an
                      # invisible gate is exactly how the research->prod drift this
                      # condition fixes went unnoticed for six days.
                      + (f"FIRST-TOUCH≤{_swingfade_first_touch_ceil():.2f} "
                         f"(≥{SWINGFADE_FIRST_TOUCH_MIN_TICKS} pre-ticks) · "
                         if _swingfade_first_touch_ceil() > 0 else "first-touch OFF · ")
                      + f"stake flat "
                      f"${SWINGFADE_MIN_STAKE:.0f} until bank > "
                      f"${BANKROLL_SEED_USD:.0f}, then "
                      f"{SWINGFADE_USABLE_FRAC:.0%}×{SWINGFADE_STAKE_FRAC:.0%} "
                      f"cap ${SWINGFADE_STAKE_CEIL:.0f} · "
                      + (f"FAK@touch+{FADE_TOUCH_PAYUP:.2f} hold-to-settle, no retry/refill · "
                         if FADE_PIN_TOUCH else
                         f"one FAK≤{SWINGFADE_ASK_MAX:.2f}, no retry/refill · ")
                      + f"{'LIVE' if _book_live('swingfade', False) else 'SHADOW'}"
                      f" | FORWARD TEST — in-sample rule, no OOS set; |slope| is "
                      f"CHAINLINK-derived since 07-30 (band was binance-calibrated) "
                      f"— judge on forward fires only", flush=True)
                _sb_banner = " · ".join(
                    f"{s['book'].removeprefix('swingband_')} "
                    f"({s['lo']:.2f},{s['hi']:.2f}]@{s['t0']}-{s['t1']}s "
                    + (f"{s.get('slope_min', 0):g}<|slope|≤{s['slope']:g}"
                       if s.get('slope_min', 0) else f"|slope|≤{s['slope']:g}")
                    + (f" slope≤{s['slope_hi']:g}"
                       if s.get('slope_hi') is not None else "")
                    + (f" adv≤{s['adverse']:g}" if s['adverse'] is not None else "")
                    + (f" other≤{s['other']:g}" if s['other'] is not None else "")
                    + (f" cross∉{sorted(s['cross_exclude'])}"
                       if s['cross_exclude'] else "") + ")"
                    for s in SWINGBAND_SPECS)
                _sb_modes = ", ".join(
                    f"{s['book'].removeprefix('swingband_')}="
                    f"{'LIVE' if _book_live(s['book'], False) else 'SHADOW'}"
                    for s in SWINGBAND_SPECS)
                print(f"** SWINGBAND BOOKS ** {_sb_banner} · cross-book $1 cap · "
                      f"shared one-signal/window lock · "
                      + ("FAK@touch hold-to-settle · " if FADE_PIN_TOUCH
                         else "FAK at band hi · ")
                      + f"{_sb_modes}", flush=True)
            if DATA_ONLY:
                print("** DATA-ONLY (no trades) **", flush=True)
            elif not trading_enabled():
                print("** STAKING OFF (trading_enabled=0) — collect-only; "
                      "T+2->T+3 probe + eval legs still recording **", flush=True)
            _wd = asyncio.create_task(_loop_watchdog())
            try:
                while True:
                    # Master mode is startup-bound because the live executor and
                    # approvals are initialized in run(). The dashboard writes the
                    # asset-scoped config; exit cleanly when it changes and let
                    # systemd Restart=always rebuild the bot in the requested mode.
                    if _resolve_dry_run() != DRY_RUN:
                        print("MASTER MODE CHANGED — restarting bot to apply ",
                              f"{'SHADOW' if _resolve_dry_run() else 'LIVE'}",
                              flush=True)
                        return
                    _t0 = time.perf_counter()
                    try:
                        await self.tick()
                    except Exception as e:
                        _err("tick", e)
                        traceback.print_exc()
                    HEALTH.record_tick((time.perf_counter() - _t0) * 1000.0)
                    await asyncio.sleep(self._poll_interval())
            finally:
                _wd.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await _wd
                if getattr(self, '_twap_config_task', None) is not None:
                    self._twap_config_task.cancel()
                    await asyncio.gather(self._twap_config_task, return_exceptions=True)
                if getattr(self, '_memory_twap_task', None) is not None:
                    self._memory_twap_task.cancel()
                    await asyncio.gather(self._memory_twap_task, return_exceptions=True)
                import pm_settlement_worker
                await pm_settlement_worker.shutdown(self)
                if getattr(self, '_redeem_recovery_task', None) is not None:
                    self._redeem_recovery_task.cancel()
                    await asyncio.gather(self._redeem_recovery_task, return_exceptions=True)
                if _RTDS_MEMORY is not None:
                    _RTDS_MEMORY.close()
                if getattr(self, '_state_export_task', None) is not None:
                    await asyncio.gather(self._state_export_task, return_exceptions=True)
                if self.bookfeed is not None:
                    await self.bookfeed.stop()
                    if getattr(self.bookfeed, 'tick_study', None) is not None:
                        await asyncio.to_thread(self.bookfeed.tick_study.close)


async def _main():
    import signal
    loop = asyncio.get_running_loop()
    task = asyncio.current_task()
    loop.add_signal_handler(signal.SIGTERM, task.cancel)
    try:
        await Bot().run()
    except asyncio.CancelledError:
        pass


if __name__ == "__main__":
    asyncio.run(_main())
