import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
import polymarket_updown_bot as pm


@pytest.mark.parametrize('side', ['Up', 'Down'])
@pytest.mark.parametrize('fresh,allowed', [(0.34, False), (0.35, True), (0.50, True), (None, False), ('error', False), (0.0, False), (float('nan'), False)])
def test_live_twap_rest_floor(monkeypatch, side, fresh, allowed):
    monkeypatch.setattr(pm, 'TWAP_EDGE_PRESEND_MAX_DROP', 0)
    monkeypatch.setattr(pm, 'book_stakes_now', lambda *a: True)
    monkeypatch.setattr(pm, 'crossbook_stake_for', lambda *a: 3.0)
    monkeypatch.setattr(pm, 'upsert', lambda leg: None)
    bot = pm.Bot.__new__(pm.Bot)
    bot.live = True
    bot._live_usdc = 30
    bot.executor = SimpleNamespace(buy=AsyncMock(return_value=(None, 0, False, 'no match')))
    response = SimpleNamespace(bids=[], asks=[] if fresh is None else [SimpleNamespace(price=fresh, size=10)])
    bot.client = SimpleNamespace(get_order_book=AsyncMock(side_effect=RuntimeError('REST failed')) if fresh == 'error' else AsyncMock(return_value=response))
    w = dict(window_anchor=1000, window_start_utc=datetime.now(timezone.utc), slug='test', legs=[], hold_legs=[], staked_count=0, price_open=100)
    asyncio.run(bot._twap_take(w, 'twap_edge', side, 'token', .5, [(.5, 10)], .51, 1, 101, 240, 'test'))
    assert bot.client.get_order_book.await_count == 1
    assert bot.executor.buy.await_count == int(allowed)
    assert w['legs'][0]['stake_usd'] == 0
    if not allowed:
        assert 'TWAP pre-send REST ask' in w['legs'][0]['no_stake_reason']


@pytest.mark.parametrize('side', ['Up', 'Down'])
@pytest.mark.parametrize('decision,fresh,allowed', [
    (.60, .50, False),  # exact ten-cent boundary, still above the floor
    (.60, .49, False),
    (.60, .501, True),
    (.62, .74, True),  # observed Oct 5: rising quote does not trigger this gate
    (.49, .43, True),  # observed Oct 5: six-cent drop
    (.58, .50, True),  # observed Oct 5: eight-cent drop
])
def test_live_twap_presend_drop(monkeypatch, side, decision, fresh, allowed):
    monkeypatch.setattr(pm, 'TWAP_EDGE_PRESEND_MAX_DROP', .10)
    monkeypatch.setattr(pm, 'book_stakes_now', lambda *a: True)
    monkeypatch.setattr(pm, 'crossbook_stake_for', lambda *a: 3.0)
    monkeypatch.setattr(pm, 'upsert', lambda leg: None)
    bot = pm.Bot.__new__(pm.Bot)
    bot.live = True
    bot._live_usdc = 30
    bot.executor = SimpleNamespace(buy=AsyncMock(return_value=(None, 0, False, 'no match')))
    bot.client = SimpleNamespace(get_order_book=AsyncMock(return_value=SimpleNamespace(
        bids=[], asks=[SimpleNamespace(price=fresh, size=10)])))
    w = dict(window_anchor=1000, window_start_utc=datetime.now(timezone.utc),
             slug='test', legs=[], hold_legs=[], staked_count=0, price_open=100)
    asyncio.run(bot._twap_take(w, 'twap_edge', side, 'token', decision,
                             [(decision, 10)], decision + .01, 1, 101, 240, 'test'))
    assert bot.executor.buy.await_count == int(allowed)
    if not allowed:
        assert 'TWAP pre-send: ask fell' in w['legs'][0]['no_stake_reason']
        assert w['legs'][0]['status'] == 'NO_FILL'
        assert not w['hold_legs']
        assert w['staked_count'] == 0
