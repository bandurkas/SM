import pandas as pd
import numpy as np

class StructureAgent:
    """
    Agent responsible for identifying market structure: swings, HH/HL, LL/LH, and BOS/MSS.
    """
    def __init__(self, window=3):
        self.window = window

    def identify_swings(self, df):
        """
        Identifies swing highs and lows using a N-candle window.
        """
        df = df.copy()
        df['swing_high'] = False
        df['swing_low'] = False

        for i in range(self.window, len(df) - self.window):
            # Swing High
            if all(df['high'].iloc[i] > df['high'].iloc[i-j] for j in range(1, self.window + 1)) and \
               all(df['high'].iloc[i] > df['high'].iloc[i+j] for j in range(1, self.window + 1)):
                df.at[df.index[i], 'swing_high'] = True
            
            # Swing Low
            if all(df['low'].iloc[i] < df['low'].iloc[i-j] for j in range(1, self.window + 1)) and \
               all(df['low'].iloc[i] < df['low'].iloc[i+j] for j in range(1, self.window + 1)):
                df.at[df.index[i], 'swing_low'] = True
        
        return df

    def detect_bos(self, df):
        """
        Detects Break of Structure (BOS) or Market Structure Shift (MSS).
        BOS occurs when price closes (body) beyond the last confirmed swing.
        """
        # Get confirmed swings
        highs = df[df['swing_high']]['high']
        lows = df[df['swing_low']]['low']
        
        if not any(highs) or not any(lows):
            return False, False
            
        last_high = highs.iloc[-1]
        last_low = lows.iloc[-1]
        
        current_close = df['close'].iloc[-1]
        
        # Bullish BOS: Body close ABOVE last swing high
        bos_bullish = current_close > last_high
        
        # Bearish BOS: Body close BELOW last swing low
        bos_bearish = current_close < last_low
        
        return bos_bullish, bos_bearish

    def get_signal(self, df):
        df_swings = self.identify_swings(df)
        bullish, bearish = self.detect_bos(df_swings)
        
        score = 0
        details = []
        
        if bullish:
            score = 1
            details.append("Bullish BOS/MSS — Слом структуры вверх (+1 балл)")
        elif bearish:
            score = 1
            details.append("Bearish BOS/MSS — Слом структуры вниз (+1 балл)")
            
        return score, details

    def get_nearest_swing(self, df, is_long):
        """
        Returns the price of the nearest significant swing point,
        used as the basis for Stop-Loss calculation.
        - For LONG: returns nearest Swing Low (price to place SL below)
        - For SHORT: returns nearest Swing High (price to place SL above)
        """
        df_swings = self.identify_swings(df)

        if is_long:
            swing_lows = df_swings[df_swings['swing_low']]['low']
            if len(swing_lows) > 0:
                return swing_lows.iloc[-1]  # Most recent swing low
        else:
            swing_highs = df_swings[df_swings['swing_high']]['high']
            if len(swing_highs) > 0:
                return swing_highs.iloc[-1]  # Most recent swing high

        return None
