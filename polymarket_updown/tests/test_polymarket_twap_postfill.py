import asyncio
import time
from copy import deepcopy
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
import polymarket_updown_bot as pm


def make_bot(monkeypatch):
    monkeypatch.setattr(pm, 'book_stakes_now', lambda *a: True)
    monkeypatch.setattr(pm, 'crossbook_stake_for', lambda *a: 4)
    monkeypatch.setattr(pm, 'TWAP_EDGE_PRESEND_MAX_DROP', .10)
    writes=[]
    monkeypatch.setattr(pm, 'upsert', lambda leg: writes.append(deepcopy(leg)))
    b=pm.Bot.__new__(pm.Bot)
    b.live=True
    b._live_usdc=40
    b.executor=SimpleNamespace(
        buy=AsyncMock(return_value=(.24, 4/.24, True, None)),
        sell_twap_cut=AsyncMock(return_value=(.22, 16.66, 'filled', None)))
    b._reconcile_stake=AsyncMock()
    b.client=SimpleNamespace(get_order_book=AsyncMock(return_value=SimpleNamespace(
        bids=[SimpleNamespace(price=.23, size=100)],
        asks=[SimpleNamespace(price=.74, size=100)])))
    w=dict(window_anchor=int(time.time()), window_start_utc=datetime.now(timezone.utc),
           slug='test', legs=[], hold_legs=[], staked_count=0, price_open=100)
    return b,w,writes


@pytest.mark.parametrize('side,book,fill,live,cut', [
    ('Down','twap_edge',.24,True,True),  # actual Oct5 62c->24c
    ('Down','twap_edge',.52,True,True),  # exact10c
    ('Down','twap_edge',.521,True,False),
    ('Up','twap_edge',.24,True,False),
    ('Down','twap_lock',.24,True,False),
    ('Down','twap_edge',.24,False,False),
])
def test_fill_trigger_scope(monkeypatch,side,book,fill,live,cut):
    b,w,writes=make_bot(monkeypatch)
    b.live=live
    sh=4/fill
    b.executor.buy.return_value=(fill,sh,True,None)
    b.executor.sell_twap_cut.return_value=(.22,pm.floor_shares(sh),'filled',None)
    asyncio.run(b._twap_take(w,book,side,'token',.62,[(.62,100)],.63,-.7,99,250,'test'))
    leg=w['legs'][0]
    assert b.executor.sell_twap_cut.await_count==int(cut)
    if cut:
        assert [r['status'] for r in writes]==['OPEN','EXIT_REVIEW','CLOSED']
        assert leg['exit_reason']=='TWAP_FILL_DROP'
        assert leg['exit_price']==.22  # response fill, not quoted23c
        expected=pm.floor_shares(sh)*.22-pm._taker_fee(pm.floor_shares(sh),.22)-4-pm._taker_fee(sh,fill)
        assert abs(leg['realized_pnl_usd']-expected)<.00015
        assert b._reconcile_stake.await_count==0
        assert w['staked_count']==1


@pytest.mark.parametrize('result,status', [('retry','OPEN'),('review','EXIT_REVIEW')])
def test_failed_sell_not_closed(monkeypatch,result,status):
    b,w,writes=make_bot(monkeypatch)
    b.executor.sell_twap_cut.return_value=(None,0,result,'failed')
    asyncio.run(b._twap_take(w,'twap_edge','Down','token',.62,[(.62,100)],.63,-.7,99,250,'test'))
    leg=w['legs'][0]
    assert leg['status']==status
    assert leg['realized_pnl_usd'] is None
    assert leg['twap_fill_cut'] is True
    assert b._reconcile_stake.await_count==0
    # Uncertain submission is never automatically resubmitted.
    if result=='review':
        asyncio.run(b._twap_cut(w,leg))
        assert b.executor.sell_twap_cut.await_count==1
    else:
        b.executor.sell_twap_cut.return_value=(.22,16.66,'filled',None)
        asyncio.run(b._twap_cut(w,leg))
        assert leg['status']=='CLOSED'


def test_missing_bid_stays_open(monkeypatch):
    b,w,writes=make_bot(monkeypatch)
    b.client.get_order_book.return_value.bids=[]
    asyncio.run(b._twap_take(w,'twap_edge','Down','token',.62,[(.62,100)],.63,-.7,99,250,'test'))
    assert w['legs'][0]['status']=='OPEN'
    assert w['legs'][0]['twap_fill_cut']
    assert b.executor.sell_twap_cut.await_count==0


@pytest.mark.parametrize('response,exception,result', [
    (dict(ok=True,status='matched',making_amount=16.66,taking_amount=3.6652),None,'filled'),
    (dict(ok=False,message='no liquidity'),None,'retry'),
    (dict(ok=True,status='matched',making_amount=8,taking_amount=1.76),None,'review'),
    (dict(ok=True,status='matched',making_amount=0,taking_amount=0),None,'review'),
    (dict(ok=True,status='matched',making_amount=float('nan'),taking_amount=1),None,'review'),
    (dict(ok=True,status='delayed',making_amount=16.66,taking_amount=3.6652),None,'review'),
    (dict(ok=True,status='live',making_amount=16.66,taking_amount=3.6652),None,'review'),
    ({},TimeoutError('transport timeout'),'review'),
])
def test_fok_response_confirmation(response,exception,result):
    e=pm.LiveExecutor.__new__(pm.LiveExecutor)
    e.c=SimpleNamespace(place_market_order=AsyncMock(return_value=SimpleNamespace(**response),side_effect=exception))
    px,sh,state,err=asyncio.run(e.sell_twap_cut('token',16.66,.23))
    assert state==result
    assert e.c.place_market_order.await_count==1
    assert e.c.place_market_order.call_args.kwargs['order_type']=='FOK'
    if result=='filled':
        assert px==pytest.approx(.22)
        assert sh==16.66
    else:assert px is None


def test_pending_recovered_after_restart(monkeypatch):
    b,w,writes=make_bot(monkeypatch)
    row=dict(window_anchor=w['window_anchor'],leg=2,slug='test',status='OPEN',
             staked=True,book='twap_edge',predicted_side='Down',twap_fill_cut=True,
             token_id='token',shares=16.66,stake_usd=4,entry_fee_usd=.2128,entry_price=.24,
             twap_decision_ask=.62)
    monkeypatch.setattr(pm,'_offload',AsyncMock(return_value=[row]))
    b.windows={}
    asyncio.run(b._retry_twap_cuts(time.time()))
    assert b.executor.sell_twap_cut.await_count==1
    assert row['status']=='CLOSED'


@pytest.mark.parametrize('outcome,realized,hold,delta,verdict', [
    ('Up',-.75,-4.2128,3.4628,'HELPED'),
    ('Down',-.75,12.4472,-13.1972,'HURT'),
    ('Up',-4.2128,-4.2128,0,'NEUTRAL'),
])
def test_settlement_audit(outcome,realized,hold,delta,verdict):
    leg=dict(status='CLOSED',exit_reason='TWAP_FILL_DROP',predicted_side='Down',
             shares=16.66,stake_usd=4,entry_fee_usd=.2128,realized_pnl_usd=realized)
    a=pm.twap_exit_audit(leg,outcome)
    assert a['twap_hold_pnl']==pytest.approx(hold)
    assert a['twap_exit_delta']==pytest.approx(delta)
    assert verdict in a['no_stake_reason']
    assert f'PM {outcome}' in a['no_stake_reason']
    assert leg['realized_pnl_usd']==realized


@pytest.mark.parametrize('status,reason,outcome', [
    ('OPEN','TWAP_FILL_DROP','Up'),
    ('EXIT_REVIEW','TWAP_FILL_DROP','Up'),
    ('CLOSED','SETTLE_LOSS','Up'),
    ('CLOSED','TWAP_FILL_DROP',None),
])
def test_audit_only_confirmed_exit_and_official_resolution(status,reason,outcome):
    assert pm.twap_exit_audit(dict(status=status,exit_reason=reason,realized_pnl_usd=-1),outcome)=={}


def test_review_blocks_new_twap_entries(monkeypatch):
    b,w,writes=make_bot(monkeypatch)
    b._twap_exit_review=True
    asyncio.run(b._twap_edge(w,time.time()))
    assert b.executor.buy.await_count==0
    assert 'requires execution review' in w['twap_edge_why']


@pytest.mark.parametrize('gap,tier', [
    (-1.31,None),(-2,'WEAK'),(3.806,'WEAK'),(-4,'MODERATE'),
    (6.514,'MODERATE-STRONG'),(-8,'STRONG'),(20,'VERY STRONG'),(-30,'EXTREME'),
])
def test_twap_gap_tier_is_recorded_without_t3_accuracy(monkeypatch,gap,tier):
    b,w,writes=make_bot(monkeypatch)
    b.live=False
    asyncio.run(b._twap_take(w,'twap_edge','Down','token',.62,[(.62,100)],.63,gap,99,250,'test'))
    assert w['legs'][0]['tier']==tier
    assert writes[-1]['tier']==tier
    assert w['legs'][0]['tier_acc'] is None
    assert b.executor.buy.await_count==0
