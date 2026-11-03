import polymarket_updown_bot as bot


def decide(bankroll, peak, **kw):
    return bot.t3_compound_decision(bankroll, peak, **kw)


def test_normal_day_stakes_bankroll_over_divisor():
    stake, new_peak, halt, reason = decide(100.0, 100.0, divisor=5.67,
                                            depth_cap=79.59, min_stake=1.0,
                                            dd_halt_units=6.80)
    assert stake == 17.63
    assert new_peak == 100.0
    assert not halt and reason == ""


def test_depth_cap_holds_stake_flat_past_the_book_limit():
    stake, new_peak, halt, _ = decide(500.0, 500.0, divisor=5.67,
                                       depth_cap=79.59, min_stake=1.0,
                                       dd_halt_units=6.80)
    assert stake == 79.59                 # 500/5.67=88.18, capped at depth
    assert new_peak == 500.0
    assert not halt


def test_min_stake_floor_below_divisor_threshold():
    stake, _, halt, _ = decide(3.0, 100.0, divisor=5.67, depth_cap=79.59,
                                min_stake=1.0, dd_halt_units=6.80)
    # bankroll has already fallen from a 100 peak, dd_units would be huge
    # at the $1 floor stake -> this case must halt, not silently floor-stake
    assert halt
    assert stake == 0.0


def test_dd_halt_trips_at_1_5x_the_study_drawdown():
    # peak 500, bankroll fallen to 400: dd=100, stake=min(400/5.67,79.59)=70.55
    # dd_units = 100/70.55 = 1.417x -> below 6.80x, should NOT halt
    stake, new_peak, halt, _ = decide(400.0, 500.0, divisor=5.67,
                                       depth_cap=79.59, min_stake=1.0,
                                       dd_halt_units=6.80)
    assert not halt
    assert new_peak == 500.0
    assert stake > 0

    # peak 500, bankroll fallen to 60: stake=min(60/5.67,79.59)=10.58,
    # dd=440, dd_units=440/10.58=41.6x -> way past 6.80x, must halt at $0
    stake2, _, halt2, reason2 = decide(60.0, 500.0, divisor=5.67,
                                        depth_cap=79.59, min_stake=1.0,
                                        dd_halt_units=6.80)
    assert halt2
    assert stake2 == 0.0
    assert "6.80x" in reason2


def test_peak_never_moves_backward():
    stake, new_peak, halt, _ = decide(50.0, 200.0, divisor=5.67,
                                       depth_cap=79.59, min_stake=1.0,
                                       dd_halt_units=999.0)  # halt disabled
    assert new_peak == 200.0                     # peak holds, does not chase bankroll down
    assert stake >= 1.0


def test_zero_bankroll_no_stake_no_crash():
    stake, new_peak, halt, _ = decide(0.0, 100.0, divisor=5.67,
                                       depth_cap=79.59, min_stake=1.0,
                                       dd_halt_units=6.80)
    assert stake == 0.0
    assert halt          # dd=100 against a $0 stake is inf -> halts, doesn't divide by zero
