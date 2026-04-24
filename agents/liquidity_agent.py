class LiquidityAgent:
    """
    Agent responsible for identifying liquidity pools (EQH/EQL, PDH/PDL) and liquidity sweeps.
    """
    def __init__(self, tolerance=0.0005):
        self.tolerance = tolerance # Tolerance for EQH/EQL identification

    def detect_eqh_eql(self, df):
        """
        Detects Equal Highs (EQH) and Equal Lows (EQL).
        """
        highs = df[df['swing_high']]['high'].tolist() if 'swing_high' in df.columns else []
        lows = df[df['swing_low']]['low'].tolist() if 'swing_low' in df.columns else []
        
        eqh = False
        eql = False
        
        if len(highs) >= 2:
            last_two_highs = highs[-2:]
            if abs(last_two_highs[0] - last_two_highs[1]) / last_two_highs[0] < self.tolerance:
                eqh = True
                
        if len(lows) >= 2:
            last_two_lows = lows[-2:]
            if abs(last_two_lows[0] - last_two_lows[1]) / last_two_lows[0] < self.tolerance:
                eql = True
                
        return eqh, eql

    def detect_sweep(self, df):
        """
        Detects a liquidity sweep: price goes beyond a swing high/low but fails to close beyond it (wick only).
        """
        # Get previous major swing points (simplified)
        prev_highs = df[df['swing_high']]['high'].iloc[:-1] if any(df['swing_high']) else []
        prev_lows = df[df['swing_low']]['low'].iloc[:-1] if any(df['swing_low']) else []
        
        if not any(prev_highs) or not any(prev_lows):
            return False, False
            
        last_high = prev_highs.iloc[-1]
        last_low = prev_lows.iloc[-1]
        
        current_high = df['high'].iloc[-1]
        current_low = df['low'].iloc[-1]
        current_close = df['close'].iloc[-1]
        
        # Bullish Sweep (Sell-side liquidity sweep): Price drops below last low but closes above it
        sweep_bullish = current_low < last_low and current_close > last_low
        
        # Bearish Sweep (Buy-side liquidity sweep): Price rises above last high but closes below it
        sweep_bearish = current_high > last_high and current_close < last_high
        
        return sweep_bullish, sweep_bearish

    def get_signal(self, df):
        eqh, eql = self.detect_eqh_eql(df)
        sweep_bullish, sweep_bearish = self.detect_sweep(df)
        
        score = 0
        details = []
        
        if sweep_bullish:
            score += 1
            details.append("SSL Sweep — Снятие ликвидности снизу (+1 балл)")
        if sweep_bearish:
            score += 1
            details.append("BSL Sweep — Снятие ликвидности сверху (+1 балл)")
        if eqh:
            details.append("Equal Highs (EQH) — Магнит ликвидности сверху")
        if eql:
            details.append("Equal Lows (EQL) — Магнит ликвидности снизу")
            
        return score, details
