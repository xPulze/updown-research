import asyncio
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import polymarket_updown_bot as pm


def test_t3_shadow_challenger_uses_flat_one_dollar_ticket(monkeypatch):
    monkeypatch.setattr(pm, "BTC_T3_SHADOW_STAKE_USD", 1.0)
    bot = pm.Bot.__new__(pm.Bot)

    shares, stake = bot.size_position(
        0.99, 0.97, book="t3", book_gated=True, shadow_challenger=True)

    assert stake == 1.0
    assert shares == 1.0 / 0.97


def test_redeem_accepts_sdk_transaction_outcome():
    outcome = SimpleNamespace(transaction_hash="0xconfirmed", transaction_id="tx-1")

    class Handle:
        async def wait(self):
            return outcome

    class Client:
        async def redeem_positions(self, **_kwargs):
            return Handle()

    ex = pm.LiveExecutor(Client(), "wallet")
    ok, info = asyncio.run(ex.redeem("condition"))
    assert ok is True
    assert info == "0xconfirmed"


def test_redeem_wait_exception_is_failure():
    class Handle:
        async def wait(self):
            raise RuntimeError("relayer rejected")

    class Client:
        async def redeem_positions(self, **_kwargs):
            return Handle()

    ex = pm.LiveExecutor(Client(), "wallet")
    ok, info = asyncio.run(ex.redeem("condition"))
    assert ok is False
    assert "relayer rejected" in info


def test_health_snapshot_is_thread_safe():
    health = pm._Health()
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = []
        for i in range(100):
            health.record_lag(i)
            health.record_tick(i)
            futures.append(pool.submit(health.snapshot))
        assert all(f.result()["ticks"] >= 1 for f in futures)
    assert health.snapshot()["ticks"] == 100


def test_eth_resolution_path_uses_twap(monkeypatch):
    monkeypatch.setattr(pm, "ASSET", "ETH")
    monkeypatch.setattr(pm, "chainlink_db_spot_price_at", lambda ts, tol_s=8: 123.0)
    monkeypatch.setattr(pm, "chainlink_db_twap_at", lambda ts, tol_s=8: 456.0)
    assert pm.chainlink_db_price_at(1) == 456.0


def test_btc_resolution_path_uses_twap(monkeypatch):
    monkeypatch.setattr(pm, "ASSET", "BTC")
    monkeypatch.setattr(pm, "chainlink_db_spot_price_at", lambda ts, tol_s=8: 123.0)
    monkeypatch.setattr(pm, "chainlink_db_twap_at", lambda ts, tol_s=8: 456.0)
    assert pm.chainlink_db_price_at(1) == 456.0


def test_three_source_consensus_requires_all_sources(monkeypatch):
    monkeypatch.setattr(pm, "chainlink_price_at",
                        lambda ts: 100.0 if ts == 1000 else 101.0)
    prices = {("coinbase", 1000): 200.0, ("coinbase", 1200): 201.0,
              ("kraken", 1000): 300.0, ("kraken", 1200): 301.0}
    monkeypatch.setattr(pm, "independent_reference_twap_at",
                        lambda source, ts: prices.get((source, ts)))
    assert pm.three_source_consensus_ok(1000, "Up", 1200)[0] is True
    prices[("kraken", 1200)] = 299.0
    ok, reason = pm.three_source_consensus_ok(1000, "Up", 1200)
    assert ok is False
    assert "kraken=Down" in reason


def test_three_source_consensus_fails_closed_on_missing_feed(monkeypatch):
    monkeypatch.setattr(pm, "chainlink_price_at", lambda ts: 100.0)
    monkeypatch.setattr(pm, "independent_reference_twap_at",
                        lambda source, ts: None if source == "kraken" else 100.0)
    ok, reason = pm.three_source_consensus_ok(1000, "Up", 1200)
    assert ok is False
    assert "kraken" in reason
