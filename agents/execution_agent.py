import os
import ccxt
import time
import threading
from dotenv import load_dotenv

load_dotenv()

# ── Configuration ───────────────────────────────────────────────────────────
SL_BUFFER_PCT    = float(os.getenv('SL_BUFFER_PCT',    '0.002'))   # 0.2% beyond swing
TP1_PCT          = float(os.getenv('TP1_PCT',          '0.03'))    # 3% first target
TRAILING_STEP    = float(os.getenv('TRAILING_STEP',    '0.005'))   # 0.5% trailing step
TRAILING_POLL_S  = int(os.getenv('TRAILING_POLL_S',    '30'))      # Poll every 30 s


class ExecutionAgent:
    def __init__(self):
        self.enabled         = os.getenv('AUTO_TRADING_ENABLED', 'False').lower() == 'true'
        self.market_type     = os.getenv('TRADE_MARKET_TYPE', 'swap')
        self.trade_amount_usdt = float(os.getenv('TRADE_AMOUNT_USDT', '10.0'))

        if self.enabled:
            api_key = os.getenv('HTX_ACCESS_KEY')
            secret  = os.getenv('HTX_SECRET_KEY')
            self.exchange = ccxt.htx({
                'apiKey': api_key,
                'secret': secret,
                'enableRateLimit': True,
                'options': {'defaultType': self.market_type}
            })
            print(f"🤖 Execution Agent ON. Market: {self.market_type}")
        else:
            print("🤖 Execution Agent: DRY-RUN mode.")
            self.exchange = None

    # ── Helpers ─────────────────────────────────────────────────────────────
    def _trade_symbol(self, symbol):
        return f"{symbol}:USDT" if self.market_type == 'swap' else symbol

    def _calc_sl_tp(self, entry_price, is_long, swing_price):
        """
        Calculates SL and TP1 prices.
        SL  = swing extremum ± SL_BUFFER_PCT buffer
        TP1 = entry ± TP1_PCT
        """
        if swing_price:
            if is_long:
                sl_price = swing_price * (1 - SL_BUFFER_PCT)
            else:
                sl_price = swing_price * (1 + SL_BUFFER_PCT)
        else:
            # Fallback: flat % SL if no swing found
            sl_price = entry_price * (1 - 0.015) if is_long else entry_price * (1 + 0.015)

        tp1_price = entry_price * (1 + TP1_PCT) if is_long else entry_price * (1 - TP1_PCT)
        return round(sl_price, 4), round(tp1_price, 4)

    # ── Place a conditional order (SL / TP) on HTX Futures ─────────────────
    def _place_conditional(self, trade_symbol, side, size, trigger_price, order_type, reduce_only=True):
        """
        Places a stop-market or take-profit-market order.
        order_type: 'stop_market' or 'take_profit_market'
        """
        try:
            params = {
                'stopPrice': trigger_price,
                'reduceOnly': reduce_only,
                'triggerType': 'last_price',
            }
            order = self.exchange.create_order(
                trade_symbol,
                order_type,
                side,
                size,
                trigger_price,
                params
            )
            print(f"📌 Conditional order placed: {order_type} @ {trigger_price}")
            return order
        except Exception as e:
            print(f"⚠️ Could not place conditional order ({order_type}): {e}")
            return None

    # ── Trailing Stop Manager (runs in background thread) ───────────────────
    def _trailing_stop_manager(self, trade_symbol, is_long, entry_price,
                                sl_order_id, size):
        """
        Monitors the position and trails the stop-loss upward (for longs) 
        every TRAILING_POLL_S seconds after TP1 is reached.
        Breakeven is set as soon as price reaches entry_price * (1 + TP1_PCT).
        """
        print(f"🔄 Trailing stop manager started for {trade_symbol}")
        breakeven_set  = False
        highest_price  = entry_price
        current_sl     = entry_price * (1 - SL_BUFFER_PCT) if is_long else entry_price * (1 + SL_BUFFER_PCT)
        tp1            = entry_price * (1 + TP1_PCT) if is_long else entry_price * (1 - TP1_PCT)

        while True:
            time.sleep(TRAILING_POLL_S)
            try:
                ticker    = self.exchange.fetch_ticker(trade_symbol)
                last_price = ticker['last']

                # Check if position is still open
                positions = self.exchange.fetch_positions([trade_symbol])
                pos_open = any(float(p['contracts']) > 0 for p in positions if p['symbol'] == trade_symbol)
                if not pos_open:
                    print(f"✅ Position {trade_symbol} closed. Trailing manager exiting.")
                    break

                if is_long:
                    # 1. Set breakeven after TP1 hit
                    if not breakeven_set and last_price >= tp1:
                        new_sl = round(entry_price * 1.001, 4)  # Slightly above entry
                        self._cancel_and_replace_sl(trade_symbol, 'sell', size, new_sl)
                        current_sl    = new_sl
                        breakeven_set = True
                        highest_price = last_price
                        print(f"✅ Breakeven set at {new_sl} for {trade_symbol}")

                    # 2. Trail the stop upward
                    elif breakeven_set and last_price > highest_price:
                        proposed_sl = round(last_price * (1 - TRAILING_STEP), 4)
                        if proposed_sl > current_sl:
                            self._cancel_and_replace_sl(trade_symbol, 'sell', size, proposed_sl)
                            current_sl    = proposed_sl
                            highest_price = last_price
                            print(f"🔼 Trailing stop moved to {proposed_sl} for {trade_symbol}")

                else:  # SHORT mirror logic
                    if not breakeven_set and last_price <= tp1:
                        new_sl = round(entry_price * 0.999, 4)
                        self._cancel_and_replace_sl(trade_symbol, 'buy', size, new_sl)
                        current_sl    = new_sl
                        breakeven_set = True
                        highest_price = last_price
                        print(f"✅ Breakeven set at {new_sl} for {trade_symbol}")

                    elif breakeven_set and last_price < highest_price:
                        proposed_sl = round(last_price * (1 + TRAILING_STEP), 4)
                        if proposed_sl < current_sl:
                            self._cancel_and_replace_sl(trade_symbol, 'buy', size, proposed_sl)
                            current_sl    = proposed_sl
                            highest_price = last_price
                            print(f"🔽 Trailing stop moved to {proposed_sl} for {trade_symbol}")

            except Exception as e:
                print(f"⚠️ Trailing manager error: {e}")

    def _cancel_and_replace_sl(self, trade_symbol, side, size, new_sl_price):
        """Cancels all open stop orders and places a new one at the given price."""
        try:
            open_orders = self.exchange.fetch_open_orders(trade_symbol)
            for o in open_orders:
                if o.get('type') in ('stop_market', 'stop', 'stop_loss'):
                    self.exchange.cancel_order(o['id'], trade_symbol)
        except Exception as e:
            print(f"⚠️ Cancel SL error: {e}")
        
        self._place_conditional(trade_symbol, side, size, new_sl_price, 'stop_market')

    # ── Main Entry Point ─────────────────────────────────────────────────────
    def execute_trade(self, symbol, is_long, current_price, swing_price=None):
        """
        Full trade lifecycle:
        1. Market entry order
        2. Stop-Loss (below swing + buffer)
        3. Take-Profit at TP1 (3%)
        4. Background trailing stop thread starts after execution
        Returns a result dict with all details, or None if failed/disabled.
        """
        if not self.enabled or not self.exchange:
            sl, tp1 = self._calc_sl_tp(current_price, is_long, swing_price)
            print(f"🛑 Dry Run: {'LONG' if is_long else 'SHORT'} {symbol} @ {current_price} | SL={sl} | TP={tp1}")
            return {
                'dry_run': True,
                'symbol': symbol,
                'side': 'LONG' if is_long else 'SHORT',
                'price': current_price,
                'sl': sl,
                'tp': tp1,
            }

        trade_symbol = self._trade_symbol(symbol)

        try:
            # — Load market info —
            markets = self.exchange.load_markets()
            if trade_symbol not in markets:
                print(f"❌ {trade_symbol} not found in HTX markets.")
                return None

            market = markets[trade_symbol]
            contract_size = market.get('contractSize', 1)

            # — Position sizing —
            size_in_crypto    = self.trade_amount_usdt / current_price
            size_in_contracts = size_in_crypto / contract_size
            formatted_size    = float(self.exchange.amount_to_precision(trade_symbol, size_in_contracts))

            min_amt = market['limits']['amount']['min']
            if formatted_size < min_amt:
                print(f"⚠️ Size {formatted_size} < min {min_amt}. Increase TRADE_AMOUNT_USDT.")
                return None

            # — Entry side —
            side = 'buy' if is_long else 'sell'
            print(f"⚡ {side.upper()} {formatted_size} {trade_symbol} @ Market…")

            # — Market order —
            order = self.exchange.create_market_order(trade_symbol, side, formatted_size)
            entry_price = order.get('average') or order.get('price') or current_price

            # — Compute SL / TP —
            sl_price, tp1_price = self._calc_sl_tp(float(entry_price), is_long, swing_price)
            sl_side  = 'sell' if is_long else 'buy'

            print(f"📐 Entry={entry_price}  SL={sl_price}  TP1={tp1_price}")

            # — Place SL —
            self._place_conditional(trade_symbol, sl_side, formatted_size, sl_price, 'stop_market')

            # — Place TP1 (limit or take_profit_market) —
            self._place_conditional(trade_symbol, sl_side, formatted_size, tp1_price, 'take_profit_market')

            # — Start trailing manager in background —
            thread = threading.Thread(
                target=self._trailing_stop_manager,
                args=(trade_symbol, is_long, float(entry_price), None, formatted_size),
                daemon=True
            )
            thread.start()

            result = {
                'id':      order['id'],
                'symbol':  trade_symbol,
                'side':    side.upper(),
                'amount':  formatted_size,
                'price':   entry_price,
                'cost':    float(formatted_size) * float(entry_price) * contract_size,
                'sl':      sl_price,
                'tp':      tp1_price,
                'status':  order['status'],
            }
            print(f"✅ Trade complete: {result}")
            return result

        except Exception as e:
            print(f"❌ Order Failed for {symbol}: {e}")
            return None
