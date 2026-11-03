from datetime import datetime, timezone

import polymarket_updown_bot as bot


MON = datetime(2026, 8, 24, 12, 0, tzinfo=timezone.utc)


def _gate(**overrides):
    args = dict(
        window_start_utc=MON,
        tier="STRONG",
        ask_pred=0.60,
        adx=25.0,
        velocity_frac=0.2,
        decision_bps=8.0,
        is_t3=False,
        vol_3min_bps=5.0,
    )
    args.update(overrides)
    return bot.t3_book_ok(**args)


def test_btc_universal_price_guard_blocks_expensive_revision(monkeypatch):
    monkeypatch.setattr(bot, "ASSET", "BTC")
    monkeypatch.setattr(bot, "T3_BTC_ASK_CEIL", 0.68)
    ok, why = _gate(ask_pred=0.69)
    assert not ok
    assert "ask" in why


def test_btc_profit_gate_accepts_cheap_quiet_revision(monkeypatch):
    monkeypatch.setattr(bot, "ASSET", "BTC")
    monkeypatch.setattr(bot, "T3_BTC_ASK_CEIL", 0.68)
    monkeypatch.setattr(bot, "MC_T3_VOL_MAX", 6.0)
    ok, why = _gate(ask_pred=0.60, vol_3min_bps=5.99)
    assert ok, why


def test_btc_profit_gate_blocks_expanding_volatility(monkeypatch):
    monkeypatch.setattr(bot, "ASSET", "BTC")
    monkeypatch.setattr(bot, "T3_BTC_ASK_CEIL", 0.68)
    monkeypatch.setattr(bot, "MC_T3_VOL_MAX", 6.0)
    ok, why = _gate(vol_3min_bps=6.0)
    assert not ok
    assert "t3 off" in why


def test_eth_shares_fillable_ask_ceiling(monkeypatch):
    """ETH used to inherit no 0.68 cap and bought 0.69-0.80 WEAK. Both
    assets now share the fillable 0.50-0.68 band."""
    monkeypatch.setattr(bot, "ASSET", "ETH")
    monkeypatch.setattr(bot, "T3_BTC_ASK_CEIL", 0.68)
    monkeypatch.setattr(bot, "T3_INITIAL_ASK_CEIL", 0.68)
    monkeypatch.setattr(bot, "T3_ASK_CEIL", 0.68)
    monkeypatch.setattr(bot, "T3_ASK_FLOOR", 0.50)
    monkeypatch.setattr(bot, "T3_BOOK_PRIMARY_ONLY", False)
    monkeypatch.setattr(bot, "MC_T3_VOL_MAX", 12.0)
    ok, why = _gate(ask_pred=0.69, vol_3min_bps=8.0)
    assert not ok
    assert "ask" in why
    ok, why = _gate(ask_pred=0.60, vol_3min_bps=8.0)
    assert ok, why
