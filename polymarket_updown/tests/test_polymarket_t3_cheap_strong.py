"""Cheap STRONG/MS T3 book: fillable 0.50-0.68, no WEAK, no 0.90."""
from datetime import datetime, timezone

import polymarket_updown_bot as bot


THU = datetime(2026, 8, 27, 12, 0, tzinfo=timezone.utc)


def _gate(**overrides):
    args = dict(
        window_start_utc=THU,
        tier="STRONG",
        ask_pred=0.55,
        adx=25.0,
        velocity_frac=0.2,
        decision_bps=10.0,
        is_t3=True,
        vol_3min_bps=8.0,
    )
    args.update(overrides)
    return bot.t3_book_ok(**args)


def test_staked_tiers_are_strong_and_ms():
    assert bot.STAKED_TIERS == {"STRONG", "MODERATE-STRONG"}
    assert "WEAK" not in bot.T3_BOOK_TIER_RULES
    assert "EXTREME" not in bot.T3_BOOK_TIER_RULES


def test_cheap_strong_passes(monkeypatch):
    monkeypatch.setattr(bot, "T3_ASK_FLOOR", 0.50)
    monkeypatch.setattr(bot, "T3_ASK_CEIL", 0.68)
    monkeypatch.setattr(bot, "MC_T3_VOL_MAX", 12.0)
    ok, why = _gate(ask_pred=0.51, vol_3min_bps=11.9)
    assert ok, why


def test_ms_in_band_passes(monkeypatch):
    monkeypatch.setattr(bot, "T3_ASK_FLOOR", 0.50)
    monkeypatch.setattr(bot, "T3_ASK_CEIL", 0.68)
    monkeypatch.setattr(bot, "MC_T3_VOL_MAX", 12.0)
    ok, why = _gate(tier="MODERATE-STRONG", ask_pred=0.60, decision_bps=7.0)
    assert ok, why


def test_weak_is_excluded():
    ok, why = _gate(tier="WEAK", ask_pred=0.60, decision_bps=3.0)
    assert not ok
    assert "not in book" in why


def test_ask_below_floor_rejected(monkeypatch):
    monkeypatch.setattr(bot, "T3_ASK_FLOOR", 0.50)
    ok, why = _gate(ask_pred=0.40)
    assert not ok
    assert "floor" in why


def test_ask_at_090_rejected(monkeypatch):
    monkeypatch.setattr(bot, "T3_ASK_CEIL", 0.68)
    ok, why = _gate(ask_pred=0.90)
    assert not ok
    assert "ask" in why


def test_extreme_is_excluded():
    ok, why = _gate(tier="EXTREME", ask_pred=0.55, decision_bps=35.0)
    assert not ok
    assert "not in book" in why


def test_quote_fillable_at_cheap_ask():
    # $1 at 0.51 wants ~2sh; 10sh at the quote is fillable.
    ok, frac = bot.t3_ask_fillable([(0.51, 10.0)], 1.0, 0.51)
    assert ok and frac >= 0.95


def test_quote_unfillable_when_size_is_above_touch():
    # Depth sits at 0.90, not at the 0.51 quote — the 0.90-book failure mode.
    ok, frac = bot.t3_ask_fillable([(0.90, 5000.0)], 1.0, 0.51)
    assert not ok
    assert frac == 0.0


def test_shadow_challenger_default_off():
    assert bot.BTC_T3_SHADOW_CHALLENGER is False


def test_negative_velocity_rejected_by_default():
    ok, why = _gate(velocity_frac=-0.2)
    assert not ok
    assert "velocity_frac" in why


def test_negative_velocity_passes_when_gate_off(monkeypatch):
    monkeypatch.setattr(bot, "T3_BOOK_VELOCITY_MIN", None)
    ok, why = _gate(velocity_frac=-0.52, vol_3min_bps=5.37)
    assert ok, why


def test_null_velocity_still_passes_with_gate_on():
    ok, why = _gate(velocity_frac=None)
    assert ok, why


def test_vol_20_keeps_the_displacement_band(monkeypatch):
    monkeypatch.setattr(bot, "MC_T3_VOL_MAX", 20.0)
    ok, why = _gate(vol_3min_bps=14.02)
    assert ok, why
    ok, why = _gate(vol_3min_bps=20.0)
    assert not ok
    assert "t3 off" in why


def test_velocity_min_env_parser():
    assert bot._t3_velocity_min_from_env("0") == 0.0
    assert bot._t3_velocity_min_from_env("off") is None
    assert bot._t3_velocity_min_from_env("OFF") is None
    assert bot._t3_velocity_min_from_env("none") is None
