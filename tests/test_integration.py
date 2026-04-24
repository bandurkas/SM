import pytest
import pandas as pd
from config import BotConfig
from paper_trader import PaperExecutionEngine
from trade_manager import TradeManager

def test_paper_trade_lifecycle():
    cfg = BotConfig(
        tick_size=0.01,
        partial_close_tp1_pct=0.5, partial_close_tp2_pct=0.0,
        use_dynamic_atr_trailing=False,
        use_dca=False,            # isolate: test lifecycle only, not DCA
        use_range_scalper=False,  # isolate: no range scalps in lifecycle test
    )
    exec_eng = PaperExecutionEngine(cfg, log_path="test_paper.csv")
    mgr = TradeManager(exec_eng, cfg)
    
    # Open pending long
    mgr.open_pending("long", retest_level=2000.0, stop=1950.0, tp1=2050.0, tp2=2100.0, tp3=2200.0, qty=1.0, expiry_bar=10, score=50)
    assert mgr.has_pending
    assert mgr.pending.order_id in exec_eng._orders
    
    # Bar 1: Price touches 2000, simulating fill
    row1 = pd.Series({"high": 2010.0, "low": 1990.0, "close": 2005.0, "atr": 10.0})
    exec_eng.simulate_bar(row1)
    mgr.update(row1, 1)
    
    assert not mgr.has_pending
    assert mgr.has_trade
    assert mgr.trade.side == "long"
    assert mgr.trade.weighted_avg_price == 2000.0
    
    # Bar 2: Price hits TP1 (2050)
    row2 = pd.Series({"high": 2060.0, "low": 2005.0, "close": 2055.0, "atr": 10.0})
    exec_eng.simulate_bar(row2)
    mgr.update(row2, 2)
    
    assert mgr.trade.tp1_hit
    assert mgr.trade.be_moved
    assert mgr.trade.stop == 2000.0  # Moved to BE
    # The code uses round_qty which might have small floating point diffs
    assert abs(mgr.trade.remaining_qty - 0.5) < 1e-5
    
    # Bar 3: Price hits new Stop (BE @ 2000)
    row3 = pd.Series({"high": 2050.0, "low": 1990.0, "close": 1995.0, "atr": 10.0})
    exec_eng.simulate_bar(row3)
    mgr.update(row3, 3)
    
    assert not mgr.has_trade
    # TP1 closed 50% at ~2050 (with 2bps slippage), stop at BE closed remainder at ~2000.
    # Net PnL: profit from TP1 portion > loss from BE stop → balance must exceed initial.
    assert exec_eng._balance > cfg.init_dep, (
        f"Expected profit trade: balance {exec_eng._balance:.4f} <= init {cfg.init_dep}"
    )
