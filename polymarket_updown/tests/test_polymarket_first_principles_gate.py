from datetime import datetime, timezone

import polymarket_updown_bot as bot


NOW = datetime(2026, 9, 22, tzinfo=timezone.utc)


def gate(**kw):
    args = dict(window_start_utc=NOW, tier="MODERATE", ask_pred=.55,
                adx=1.0, velocity_frac=.1, decision_bps=4.5,
                is_t3=True, vol_3min_bps=999.0)
    args.update(kw)
    return bot.t3_book_ok(**args)


def test_btc_hard_move_passes_without_velocity_adx_or_vol(monkeypatch):
    monkeypatch.setattr(bot, "T3_FIRST_PRINCIPLES", True)
    monkeypatch.setattr(bot, "ASSET", "BTC")
    monkeypatch.setattr(bot, "T3_ASK_FLOOR", .35)
    monkeypatch.setattr(bot, "T3_ASK_CEIL", .68)
    ok, why = gate(decision_bps=6.0, velocity_frac=-1.0)
    assert ok, why


def test_btc_shoulder_requires_velocity(monkeypatch):
    monkeypatch.setattr(bot, "T3_FIRST_PRINCIPLES", True)
    monkeypatch.setattr(bot, "ASSET", "BTC")
    monkeypatch.setattr(bot, "T3_ASK_FLOOR", .35)
    monkeypatch.setattr(bot, "T3_ASK_CEIL", .68)
    assert gate(decision_bps=4.0, velocity_frac=0.0)[0]
    assert not gate(decision_bps=5.99, velocity_frac=-.01)[0]
    assert not gate(decision_bps=3.99, velocity_frac=1.0)[0]


def test_eth_rule_is_independent(monkeypatch):
    monkeypatch.setattr(bot, "T3_FIRST_PRINCIPLES", True)
    monkeypatch.setattr(bot, "ASSET", "ETH")
    monkeypatch.setattr(bot, "T3_ASK_FLOOR", .35)
    monkeypatch.setattr(bot, "T3_ASK_CEIL", .68)
    assert gate(decision_bps=4.0, velocity_frac=.01)[0]
    assert not gate(decision_bps=4.0, velocity_frac=0.0)[0]   # H2 2026-09-24
    assert not gate(decision_bps=8.0, velocity_frac=-.01)[0]


def test_first_principles_is_primary_only_and_price_bounded(monkeypatch):
    monkeypatch.setattr(bot, "T3_FIRST_PRINCIPLES", True)
    monkeypatch.setattr(bot, "ASSET", "BTC")
    monkeypatch.setattr(bot, "T3_ASK_FLOOR", .35)
    monkeypatch.setattr(bot, "T3_ASK_CEIL", .68)
    assert not gate(is_t3=False, decision_bps=8.0)[0]
    assert not gate(ask_pred=.34, decision_bps=8.0)[0]
    assert not gate(ask_pred=.69, decision_bps=8.0)[0]


def test_live_ask_revalidation(monkeypatch):
    monkeypatch.setattr(bot, "T3_MAX_ASK_DROP", 0.05)
    assert bot.t3_live_ask_ok(0.50, 0.45)
    assert not bot.t3_live_ask_ok(0.50, 0.44)
    assert not bot.t3_live_ask_ok(None, 0.50)
