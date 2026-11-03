"""Fast, causal market-regime gate for Polymarket ETH fade bands.

The old controller waited for ten outcomes per book.  These features instead use
the preceding hour of one-minute independent-reference prices and can change
state every five minutes.  This module is deliberately asset-agnostic and has no
database/config imports; the ETH bot is the only caller allowed to feed it data.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from math import sqrt
from statistics import fmean
from typing import Iterable, Mapping, Sequence


ELIGIBLE_BOOKS = frozenset({
    "swingband_21_25", "swingband_25_30", "swingband_30_40",
})


@dataclass(frozen=True)
class Features:
    observations: int
    span_minutes: float
    trend_efficiency: float | None
    return_autocorr: float | None
    flip_rate: float | None
    volatility_bps: float | None
    shock_ratio: float | None
    source_divergence_bps: float | None
    spread_bps: float | None
    source_coverage: int


@dataclass(frozen=True)
class MarketDecision:
    favourable: bool
    hard_veto: bool
    score: int
    required_score: int
    reason: str
    features: Features


@dataclass(frozen=True)
class BookDecision:
    allowed: bool
    state: str
    reason: str
    favourable_streak: int
    unfavourable_streak: int
    cooldown_until: float


def _corr(xs: Sequence[float], ys: Sequence[float]) -> float | None:
    if len(xs) != len(ys) or len(xs) < 3:
        return None
    mx, my = fmean(xs), fmean(ys)
    dx, dy = [x - mx for x in xs], [y - my for y in ys]
    den = sqrt(sum(x*x for x in dx) * sum(y*y for y in dy))
    return sum(x*y for x, y in zip(dx, dy)) / den if den else 0.0


def compute_features(rows: Iterable[Mapping]) -> Features:
    """Compute causal features from minute rows ordered oldest-to-newest.

    Required keys are ``ts`` and ``price``. Optional keys are ``spread_bps``,
    ``source_divergence_bps`` and ``source_coverage``.
    """
    clean = []
    for row in rows:
        try:
            ts, price = float(row["ts"]), float(row["price"])
        except (KeyError, TypeError, ValueError):
            continue
        if price > 0:
            clean.append((ts, price, row))
    clean.sort(key=lambda x: x[0])
    if len(clean) < 2:
        return Features(len(clean), 0.0, None, None, None, None, None,
                        None, None, 0)
    prices = [x[1] for x in clean]
    returns = [(b/a - 1.0) for a, b in zip(prices, prices[1:]) if a > 0]
    moves = [abs(x) for x in returns]
    efficiency = abs(prices[-1] / prices[0] - 1.0) / sum(moves) if sum(moves) else 0.0
    signs = [1 if x > 0 else -1 if x < 0 else 0 for x in returns]
    nz = [x for x in signs if x]
    flip_rate = (sum(a != b for a, b in zip(nz, nz[1:])) / (len(nz)-1)
                 if len(nz) > 1 else None)
    autocorr = _corr(returns[:-1], returns[1:]) if len(returns) > 3 else None
    vol = sqrt(fmean([x*x for x in returns])) * 10000.0 if returns else None
    # RMS volatility in the newest 15 minutes relative to the preceding sample.
    recent, prior = returns[-15:], returns[:-15]
    rv_recent = sqrt(fmean([x*x for x in recent])) if recent else 0.0
    rv_prior = sqrt(fmean([x*x for x in prior])) if prior else 0.0
    shock = rv_recent / rv_prior if rv_prior > 0 else (1.0 if rv_recent == 0 else 99.0)
    latest = clean[-1][2]
    def optional(name):
        try:
            value = latest.get(name)
            return float(value) if value is not None else None
        except (TypeError, ValueError):
            return None
    coverage = int(optional("source_coverage") or 0)
    return Features(
        len(clean), (clean[-1][0] - clean[0][0]) / 60.0,
        efficiency, autocorr, flip_rate, vol, shock,
        optional("source_divergence_bps"), optional("spread_bps"), coverage)


def classify(features: Features, *, required_score: int = 3) -> MarketDecision:
    """Classify whether the current hour supports short-horizon fade entries."""
    missing = (features.observations < 45 or features.span_minutes < 44
               or features.source_coverage < 2)
    hard_reasons = []
    if missing:
        hard_reasons.append("reference warmup/coverage")
    if features.source_divergence_bps is None or features.source_divergence_bps > 5.0:
        hard_reasons.append("venue divergence")
    if features.spread_bps is None or features.spread_bps > 5.0:
        hard_reasons.append("reference spread")
    if features.shock_ratio is None or features.shock_ratio > 2.25:
        hard_reasons.append("volatility shock")
    if features.trend_efficiency is None or features.trend_efficiency > 0.62:
        hard_reasons.append("strong directional trend")
    checks = [
        (features.trend_efficiency is not None and features.trend_efficiency <= 0.45,
         "low trend efficiency"),
        (features.return_autocorr is not None and features.return_autocorr <= 0.05,
         "non-positive autocorrelation"),
        (features.flip_rate is not None and features.flip_rate >= 0.48,
         "frequent direction changes"),
        (features.volatility_bps is not None and 0.5 <= features.volatility_bps <= 18.0,
         "workable volatility"),
    ]
    score = sum(ok for ok, _ in checks)
    hard = bool(hard_reasons)
    favourable = not hard and score >= required_score
    good = ", ".join(label for ok, label in checks if ok) or "no fade conditions"
    reason = ("veto: " + ", ".join(hard_reasons)) if hard else (
        f"fade score {score}/4: {good}")
    return MarketDecision(favourable, hard, score, required_score, reason, features)


def transition(book: str, market: MarketDecision, prior: Mapping | None, *,
               now: float, consecutive_losses: int = 0,
               arm_checks: int = 3, disarm_checks: int = 2,
               cooldown_seconds: int = 1800) -> BookDecision:
    """Advance one book at most once per five-minute feature observation."""
    if book not in ELIGIBLE_BOOKS:
        return BookDecision(False, "INELIGIBLE", "structurally ineligible",
                            0, 0, 0.0)
    prior = dict(prior or {})
    was_live = prior.get("state") == "LIVE"
    cooldown_until = float(prior.get("cooldown_until") or 0.0)
    fav = int(prior.get("favourable_streak") or 0)
    unfav = int(prior.get("unfavourable_streak") or 0)
    if consecutive_losses >= 2 and was_live:
        cooldown_until = now + cooldown_seconds
        return BookDecision(False, "COOLDOWN", "two consecutive losses",
                            0, 0, cooldown_until)
    if cooldown_until > now:
        return BookDecision(False, "COOLDOWN", "performance cooldown",
                            0, 0, cooldown_until)
    if market.hard_veto:
        return BookDecision(False, "BLOCKED", market.reason, 0, unfav + 1, 0.0)
    if market.favourable:
        fav, unfav = fav + 1, 0
        allowed = was_live or fav >= arm_checks
        return BookDecision(allowed, "LIVE" if allowed else "BLOCKED",
                            market.reason if allowed else f"arming {fav}/{arm_checks}: {market.reason}",
                            fav, unfav, 0.0)
    fav, unfav = 0, unfav + 1
    allowed = was_live and unfav < disarm_checks
    return BookDecision(allowed, "LIVE" if allowed else "BLOCKED",
                        (f"holding live {unfav}/{disarm_checks}: " if allowed else "") + market.reason,
                        fav, unfav, 0.0)


def public_dict(value):
    return asdict(value)
