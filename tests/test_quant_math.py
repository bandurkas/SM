import unittest
import sys
import os

# Add root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dca_engine import calculate_next_dca_level, calculate_next_dca_qty, calculate_range_scalp_exit
from config import BotConfig

class TestQuantMath(unittest.TestCase):
    def setUp(self):
        self.cfg = BotConfig()
        self.cfg.dca_step_multiplier = 1.1
        self.cfg.dca_base_step_atr = 1.0
        self.cfg.dca_martingale_factor = 1.5
        self.cfg.qty_step = 0.01

    def test_dca_level_progression(self):
        # Long position: levels should go DOWN
        base_price = 2500.0
        atr = 50.0
        # Level 0 (First DCA)
        l1 = calculate_next_dca_level("long", base_price, atr, 0, self.cfg)
        self.assertEqual(l1, 2450.0) # 2500 - (1.0 * 50)
        
        # Level 1 (Second DCA)
        l2 = calculate_next_dca_level("long", l1, atr, 1, self.cfg)
        # step = 1.0 * 50 * 1.1 = 55
        self.assertEqual(l2, 2395.0) # 2450 - 55

    def test_martingale_expansion(self):
        initial_qty = 0.1
        # Qty 2 = 0.1 * 1.5 = 0.15
        q2 = calculate_next_dca_qty(initial_qty, self.cfg)
        self.assertAlmostEqual(q2, 0.15)
        
        # Qty 3 = 0.15 * 1.5 = 0.225 -> rounded to 0.23 (if step 0.01)
        q3 = calculate_next_dca_qty(q2, self.cfg)
        self.assertAlmostEqual(q3, 0.23)

    def test_scalp_profitability_check(self):
        # Ensure scalp exit is always in profit relative to entry
        entry = 2500.0
        atr = 50.0
        self.cfg.rs_profit_target_atr = 0.5
        
        # Long scalp exit should be ABOVE entry
        exit_long = calculate_range_scalp_exit("long", entry, atr, self.cfg)
        self.assertTrue(exit_long > entry)
        self.assertEqual(exit_long, 2525.0) # 2500 + (0.5 * 50)

if __name__ == '__main__':
    unittest.main()
