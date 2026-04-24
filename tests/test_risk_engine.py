import pytest
from risk_engine import round_tick, round_qty, calc_qty, get_stop_price
from config import BotConfig

def test_round_tick():
    assert round_tick(2300.123, 0.01) == 2300.12
    assert round_tick(2300.128, 0.01) == 2300.13

def test_round_qty():
    assert round_qty(1.234, 0.01) == 1.23
    assert round_qty(1.235, 0.01) == 1.24

def test_calc_qty():
    cfg = BotConfig(init_dep=1000.0, risk_pct=1.0, leverage=10, qty_step=0.01)
    # risk_usdt = 1000 * 1% = $10. dist = $100.
    # qty = risk/dist = 10/100 = 0.1 ETH (leverage NOT multiplied — correct dollar risk)
    qty = calc_qty(entry=2000.0, stop=1900.0, config=cfg)
    assert qty == pytest.approx(0.1)

def test_get_stop_price():
    cfg = BotConfig(tick_size=0.01, use_liq_stop=False)
    # Hybrid mode uses ATR if no swing
    stop = get_stop_price(is_long=True, entry=2000.0, atr=10.0, swing_low=1950.0, swing_high=2050.0,
                          prev_low=1980.0, band_dn=1970.0, pdl=1960.0, prev_high=2020.0,
                          band_up=2030.0, pdh=2040.0, config=cfg, atr_mult=1.0)
    assert stop < 2000.0

    stop_short = get_stop_price(is_long=False, entry=2000.0, atr=10.0, swing_low=1950.0, swing_high=2050.0,
                          prev_low=1980.0, band_dn=1970.0, pdl=1960.0, prev_high=2020.0,
                          band_up=2030.0, pdh=2040.0, config=cfg, atr_mult=1.0)
    assert stop_short > 2000.0
