from types import SimpleNamespace as NS

import polymarket_updown_bot as pm


def feed(monkeypatch):
    monkeypatch.setattr(pm, 'WS_BOOK', True)
    monkeypatch.setattr(pm.time, 'time', lambda: 100)
    f = pm.BookFeed(None)
    f._books['x'] = {'bids': {.4: 10}, 'asks': {.5: 10}, 'ts': 99}
    return f


def test_heartbeat_does_not_refresh_depth(monkeypatch):
    f = feed(monkeypatch)
    f._apply(NS(type='best_bid_ask', payload=NS(token_id='x', best_bid=.4, best_ask=.5)))
    assert f._books['x']['ts'] == 99
    monkeypatch.setattr(pm.time, 'time', lambda: 100 + pm.BOOK_STALE_S)
    assert f.levels('x') is None


def test_disagreeing_touch_refuses_cached_depth_and_snapshot_recovers(monkeypatch):
    f = feed(monkeypatch)
    f._apply(NS(type='best_bid_ask', payload=NS(token_id='x', best_bid=.4, best_ask=.7)))
    assert f.levels('x') is None
    assert f.fillable_estimate('x', .4, .5, 'BUY', 1, 1, 1) is None
    f._apply(NS(type='book', payload=NS(token_id='x', bids=[NS(price=.4, size=10)],
                                     asks=[NS(price=.7, size=20)])))
    assert f.levels('x')[1] == [(.7, 20)]


def test_delta_touch_disagreement_and_recovery(monkeypatch):
    f = feed(monkeypatch)
    change = NS(token_id='x', side='SELL', price=.6, size=10, best_bid=.4, best_ask=.6)
    f._apply(NS(type='price_change', payload=NS(price_changes=[change])))
    assert f.levels('x') is None  # phantom .5 ask remains in reconstructed book
    change = NS(token_id='x', side='SELL', price=.5, size=0, best_bid=.4, best_ask=.6)
    f._apply(NS(type='price_change', payload=NS(price_changes=[change])))
    assert f.levels('x')[1] == [(.6, 10)]


def test_delta_without_snapshot_is_ignored(monkeypatch):
    f = feed(monkeypatch)
    change = NS(token_id='new', side='SELL', price=.6, size=10)
    f._apply(NS(type='price_change', payload=NS(price_changes=[change])))
    assert 'new' not in f._books
