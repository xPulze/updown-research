import asyncio
from types import SimpleNamespace
from unittest.mock import Mock, AsyncMock

import pytest
import polymarket_updown_bot as pm


@pytest.fixture(autouse=True)
def isolated_cache(monkeypatch):
    monkeypatch.setattr(pm, '_PM_REFERENCE_CACHE', {})
    monkeypatch.setattr(pm.twap_quality, 'load', lambda: {'enabled': False, 'valid': True})


def response(metadata):
    return SimpleNamespace(raise_for_status=lambda: None,
                           json=lambda: {'eventMetadata': metadata})


def test_official_open_differs_from_raw_reconstruction(monkeypatch):
    get = Mock(return_value=response({'priceToBeat': 82952.67554140369,
                                     'finalPrice': 82949.3668662837}))
    monkeypatch.setattr(pm.requests, 'get', get)
    monkeypatch.setattr(pm, 'chainlink_price_at', lambda *a: pytest.fail('raw fallback'))
    assert pm.underlying_price_at(1791446100) == 82952.67554140369
    assert pm.polymarket_window_reference(1791446100)['price_settle'] == 82949.3668662837
    assert get.call_count == 1
    assert 'btc-updown-5m-1791446100' in get.call_args.args[0]


@pytest.mark.parametrize('value', [None, 0, -1, 'nan', 'inf', True, 'invalid'])
def test_missing_or_invalid_official_open_refuses_fallback(monkeypatch, value):
    monkeypatch.setattr(pm.requests, 'get', lambda *a, **k: response({'priceToBeat': value}))
    monkeypatch.setattr(pm, 'chainlink_price_at', lambda *a: pytest.fail('raw fallback'))
    assert pm.underlying_price_at(1791446100) is None


def test_open_only_cache_refreshes_for_final_and_preserves_on_outage(monkeypatch):
    clock = [0]
    monkeypatch.setattr(pm.time, 'monotonic', lambda: clock[0])
    monkeypatch.setattr(pm.time, 'time', lambda: 1001)
    get = Mock(side_effect=[response({'priceToBeat': 100}), RuntimeError('offline'), response({}),
                           response({'priceToBeat': 100, 'finalPrice': 99})])
    monkeypatch.setattr(pm.requests, 'get', get)
    assert pm.underlying_price_at(1000) == 100
    clock[0] = 6
    assert pm.underlying_price_at(1000) == 100
    clock[0] = 12
    assert pm.polymarket_window_reference(1000)['price_settle'] == 99


def test_live_ui_open_before_gamma_metadata(monkeypatch):
    monkeypatch.setattr(pm.time, 'time', lambda: 1100)
    get = Mock(side_effect=[response({}), SimpleNamespace(
        raise_for_status=lambda: None,
        json=lambda: {'openPrice': 102, 'closePrice': 103, 'completed': False})])
    monkeypatch.setattr(pm.requests, 'get', get)
    reference = pm.polymarket_window_reference(1000, 'ETH')
    assert reference == {'price_open': 102, 'price_settle': None}
    params = get.call_args.kwargs['params']
    assert params['symbol'] == 'ETH'
    assert params['twapEnabled'] == 'true'
    assert params['twapLookbackSeconds'] == '60'
    assert params['eventStartTime'] == '1970-01-01T00:16:40Z'


def test_ui_close_requires_completed_flag(monkeypatch):
    monkeypatch.setattr(pm.time, 'time', lambda: 1400)
    get = Mock(side_effect=[response({}), SimpleNamespace(
        raise_for_status=lambda: None,
        json=lambda: {'openPrice': 102, 'closePrice': 103, 'completed': False})])
    monkeypatch.setattr(pm.requests, 'get', get)
    assert pm.polymarket_window_reference(1000)['price_settle'] is None


def test_entry_side_changes_using_official_open(monkeypatch):
    # Raw/reconstructed open 100 would say Up; PM open 102 says Down.
    monkeypatch.setattr(pm.requests, 'get', lambda *a, **k: response({'priceToBeat': 102}))
    monkeypatch.setattr(pm, 'twap_lock_estimate', lambda *a: (101, 1250, 1))
    monkeypatch.setattr(pm, 'twap_edge_p', lambda *a: .95)
    monkeypatch.setattr(pm, 'TWAP_CONTINUOUS_CALIBRATION', False)
    monkeypatch.setattr(pm.twap_cal, 'append_json', lambda *a: None)
    monkeypatch.setattr(pm, 'fade_live_touch', lambda *a: (.5, [(.5, 10)]))
    bot = pm.Bot.__new__(pm.Bot)
    bot.bookfeed = None
    bot._twap_take = AsyncMock()
    w = dict(window_anchor=1000, price_open=pm.underlying_price_at(1000),
             up_token='up', down_token='down', twap_edge_max_px=.9)
    asyncio.run(bot._twap_edge(w, 1250))
    args = bot._twap_take.call_args.args
    assert args[2:4] == ('Down', 'down')
    assert args[7] == pytest.approx((101 / 102 - 1) * 10000)


def test_detail_uses_official_open_and_final_without_shadow_rows(monkeypatch):
    dash = __import__("pytest").importorskip("polymarket_dashboard")  # dashboard not included in this release
    class Result:
        def mappings(self): return self
        def all(self): return []
    class Connection:
        def __enter__(self): return self
        def __exit__(self, *a): pass
        def execute(self, *a, **k): return Result()
    monkeypatch.setattr(dash, 'ENGINE', SimpleNamespace(connect=Connection))
    monkeypatch.setattr(dash, 'polymarket_window_reference',
                        lambda *a: {'price_open': 102, 'price_settle': 101})
    with dash.app.test_request_context('/leg_detail?asset=BTC&anchor=1000'):
        data = dash.leg_detail().get_json()
    assert (data['price_open'], data['settle_price'], data['settle_side']) == (102, 101, 'Down')
