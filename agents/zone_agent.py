class ZoneAgent:
    """
    Agent responsible for identifying institutional zones: Fair Value Gaps (FVG), 
    Order Blocks (OB), and Breaker Blocks.
    """
    def __init__(self, fvg_min_pct=0.001):
        self.fvg_min_pct = fvg_min_pct

    def detect_fvg(self, df):
        """
        Detects Fair Value Gaps (FVG) in a 3-candle sequence.
        """
        fvg_bullish = [] # List of price ranges [low, high]
        fvg_bearish = []

        if len(df) < 3:
            return fvg_bullish, fvg_bearish

        # Check last 3 candles
        c1 = df.iloc[-3]
        c2 = df.iloc[-2]
        c3 = df.iloc[-1]

        # Bullish FVG: c1 high < c3 low
        if c3['low'] > c1['high']:
            gap_size = (c3['low'] - c1['high']) / c1['high']
            if gap_size > self.fvg_min_pct:
                fvg_bullish.append([c1['high'], c3['low']])

        # Bearish FVG: c1 low > c3 high
        if c3['high'] < c1['low']:
            gap_size = (c1['low'] - c3['high']) / c1['low']
            if gap_size > self.fvg_min_pct:
                fvg_bearish.append([c3['high'], c1['low']])

        return fvg_bullish, fvg_bearish

    def detect_order_block(self, df):
        """
        Identifies Order Blocks (OB). 
        Bullish OB: Last down candle before a strong move up that breaks structure.
        Bearish OB: Last up candle before a strong move down that breaks structure.
        """
        # Simplified OB detection: looks for strong directional candles
        c1 = df.iloc[-2] # Potential OB candle
        c2 = df.iloc[-1] # Displacement candle
        
        bullish_ob = False
        bearish_ob = False
        
        # Bullish OB: c1 is red, c2 is a strong green candle engulfing c1
        if c1['close'] < c1['open'] and c2['close'] > c2['open'] and c2['close'] > c1['high']:
            bullish_ob = True
            
        # Bearish OB: c1 is green, c2 is a strong red candle engulfing c1
        if c1['close'] > c1['open'] and c2['close'] < c2['open'] and c2['close'] < c1['low']:
            bearish_ob = True
            
        return bullish_ob, bearish_ob

    def get_signal(self, df):
        fvg_bull, fvg_bear = self.detect_fvg(df)
        ob_bull, ob_bear = self.detect_order_block(df)
        
        score = 0
        details = []
        
        if fvg_bull:
            score += 1
            details.append(f"Bullish FVG — Бычий имбаланс (+1 балл) в зоне {fvg_bull[-1]}")
        if fvg_bear:
            score += 1
            details.append(f"Bearish FVG — Медвежий имбаланс (+1 балл) в зоне {fvg_bear[-1]}")
        if ob_bull:
            score += 1
            details.append("Bullish OB — Потенциальный бычий ордер-блок (+1 балл)")
        if ob_bear:
            score += 1
            details.append("Bearish OB — Потенциальный медвежий ордер-блок (+1 балл)")
            
        return score, details
