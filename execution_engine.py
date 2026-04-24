"""
execution_engine.py — CCXT-based order placement and management for HTX.

All order actions go through this module so paper_trader.py can swap out
the implementation without touching the rest of the codebase.
"""

import logging
import time
from typing import Optional

import ccxt

from config import BotConfig, cfg as default_cfg

logger = logging.getLogger(__name__)


def _with_retry(fn, *args, max_retries: int = 3, base_delay: float = 1.0, **kwargs):
    """Retry on transient exchange errors (rate limit, network) with exponential backoff."""
    for attempt in range(max_retries):
        try:
            return fn(*args, **kwargs)
        except (ccxt.RateLimitExceeded, ccxt.NetworkError, ccxt.RequestTimeout) as exc:
            if attempt == max_retries - 1:
                raise
            delay = base_delay * (2 ** attempt)
            logger.warning(f"Exchange transient error ({type(exc).__name__}), retry {attempt + 1}/{max_retries} in {delay:.1f}s")
            time.sleep(delay)
    return None  # unreachable


class ExecutionEngine:
    """
    Thin wrapper around ccxt.htx for ETH/USDT:USDT perpetual swap.

    Stop-loss management strategy:
      HTX supports conditional (trigger) orders. We place a market stop order
      via create_order with 'stopPrice'. If that fails, the TradeManager falls
      back to software stop monitoring.
    """

    CONTRACT_SIZE = 0.01  # 1 HTX contract = 0.01 ETH

    def __init__(self, exchange: ccxt.htx, config: BotConfig = default_cfg):
        self.exchange = exchange
        self.cfg = config
        self.symbol = config.symbol

    def _contracts(self, qty_eth: float) -> int:
        """Convert ETH qty → whole contracts (min 1). HTX requires integer contracts."""
        return max(1, round(qty_eth / self.CONTRACT_SIZE))

    # ── Order placement ───────────────────────────────────────────────────────

    def place_limit_order(self, side: str, qty: float, price: float) -> dict:
        """
        Place a GTC limit order.
        side: "buy" | "sell"
        Returns the raw ccxt order dict.
        """
        contracts = self._contracts(qty)
        client_id = str(int(time.time() * 1000))
        logger.info(f"Placing limit {side} {contracts} contracts ({qty} ETH) @ {price} cid={client_id}")
        order = self.exchange.create_order(
            symbol=self.symbol,
            type="limit",
            side=side,
            amount=contracts,
            price=price,
            params={
                "timeInForce": "GTC", 
                "lever_rate": self.cfg.leverage, 
                "margin_mode": "cross",
                "client_order_id": client_id
            },
        )
        logger.info(f"Limit order placed: {order['id']}")
        return order

    def place_market_order(self, side: str, qty: float, reduce_only: bool = False) -> dict:
        """Place a market order. Pass reduce_only=True for all closing orders."""
        contracts = self._contracts(qty)
        # P0 FIX: Idempotency
        client_id = str(int(time.time() * 1000))
        
        logger.info(f"Placing market {side} {contracts} contracts ({qty} ETH) reduce_only={reduce_only} cid={client_id}")
        params: dict = {
            "lever_rate": self.cfg.leverage, 
            "margin_mode": "cross",
            "client_order_id": client_id
        }
        if reduce_only:
            params["reduce_only"] = 1
        order = self.exchange.create_order(
            symbol=self.symbol,
            type="market",
            side=side,
            amount=contracts,
            params=params,
        )
        logger.info(f"Market order placed: {order['id']}")
        return order

    def place_stop_market_order(self, side: str, qty: float, stop_price: float) -> Optional[dict]:
        """
        Place a stop-market (trigger) order on HTX USDT-M cross.

        Uses /linear-swap-api/v1/swap_cross_trigger_order directly because
        ccxt's create_order(type="stop") passes an invalid margin_mode param
        to HTX unified account (err 1067).

        Returns dict with prefixed id "TRG:<order_id>" so cancel_order can
        route to the trigger-cancel endpoint. Falls back to software stop on error.
        """
        try:
            contracts = self._contracts(qty)
            # P0 FIX: Idempotency
            client_id = str(int(time.time() * 1000))

            # Long position stop-loss: price falls → sell → trigger_type "le"
            # Short position stop-loss: price rises → buy  → trigger_type "ge"
            trigger_type = "le" if side == "sell" else "ge"
            contract_code = self.symbol.split("/")[0] + "-" + self.symbol.split("/")[1].split(":")[0]
            logger.info(f"Placing stop-market {side} {contracts} contracts trigger @ {stop_price} cid={client_id}")
            r = self.exchange.contractPrivatePostLinearSwapApiV1SwapCrossTriggerOrder({
                "contract_code": contract_code,
                "trigger_type": trigger_type,
                "trigger_price": str(stop_price),
                "order_price": str(stop_price),
                "order_price_type": "optimal_20",
                "volume": contracts,
                "direction": side,
                "offset": "both",
                "lever_rate": self.cfg.leverage,
                "reduce_only": 1,
                "client_order_id": client_id
            })
            if r.get("status") != "ok":
                logger.warning(f"Stop order rejected: {r}; will use software stop")
                return None
            oid = r.get("data", {}).get("order_id_str") or str(r.get("data", {}).get("order_id"))
            logger.info(f"Stop order placed: TRG:{oid}")
            return {"id": f"TRG:{oid}", "info": r}
        except ccxt.InsufficientFunds as exc:
            logger.error(f"Stop order rejected (InsufficientFunds) - Margin tied up elsewhere. Error: {exc.args}")
            return None
        except ccxt.RateLimitExceeded as exc:
            logger.error(f"Stop order rejected (RateLimit) - Update frequency too high. Error: {exc.args}")
            return None
        except ccxt.BaseError as exc:
            logger.warning(f"Stop order placement failed ({type(exc).__name__}: {exc}); will use software stop")
            return None
        except Exception as exc:
            logger.warning(f"Stop order placement error ({type(exc).__name__}: {exc}); will use software stop")
            return None

    # ── Order management ──────────────────────────────────────────────────────

    def cancel_order(self, order_id: str) -> bool:
        """Cancel an open order. Routes trigger orders (TRG: prefix) to the
        trigger-cancel endpoint; regular orders go through ccxt.
        Returns True if cancel command was accepted."""
        try:
            if isinstance(order_id, str) and order_id.startswith("TRG:"):
                real_id = order_id[4:]
                if not real_id or real_id == "None":
                    logger.error(f"cancel_order: malformed TRG id '{order_id}' — skipping")
                    return False
                contract_code = self.symbol.split("/")[0] + "-" + self.symbol.split("/")[1].split(":")[0]
                r = self.exchange.contractPrivatePostLinearSwapApiV1SwapCrossTriggerCancel({
                    "contract_code": contract_code,
                    "order_id": real_id,
                })
                if r.get("status") == "ok":
                    logger.info(f"Cancelled trigger order {order_id}")
                    return True
                logger.warning(f"Cancel trigger failed: {r}")
                return False
            self.exchange.cancel_order(order_id, self.symbol)
            logger.info(f"Cancelled order {order_id}")
            return True
        except ccxt.OrderNotFound:
            logger.warning(f"Order {order_id} not found when cancelling")
            return True # If not found, it's effectively cancelled (or filled)
        except ccxt.BaseError as exc:
            logger.error(f"Cancel failed for {order_id}: {exc}")
            return False
        except Exception as exc:
            logger.error(f"Cancel failed for {order_id}: {exc}")
            return False

    def cancel_order_confirmed(self, order_id: str, max_attempts: int = 3) -> bool:
        """P0: Guaranteed cancellation. Commands cancel then verifies via fetch_order."""
        for i in range(max_attempts):
            success = self.cancel_order(order_id)
            if not success and i < max_attempts - 1:
                time.sleep(1.0)
                continue
                
            # Verify status
            time.sleep(0.5) # allow exchange to process
            order = self.fetch_order(order_id)
            if order is None or order["status"] in ("cancelled", "filled"):
                return True
                
        logger.error(f"P0: CRITICAL - FAILED TO CANCEL ORDER {order_id} AFTER {max_attempts} ATTEMPTS")
        return False

    def amend_stop_order(self, stop_order_id: str, new_stop_price: float) -> bool:
        """
        Move an existing stop order to a new price (break-even move).
        HTX does not support amend in unified API — we cancel & replace.
        """
        self.cancel_order(stop_order_id)
        return True  # Caller must re-place stop via place_stop_market_order

    # ── Account / position queries ────────────────────────────────────────────

    def fetch_order(self, order_id: str) -> Optional[dict]:
        """
        Query a specific limit order status via HTX swap_cross_order_info.
        (CCXT generic fetch_order is unreliable for HTX unified linear swap.)

        Returns normalized dict:
          { id, status, filled_qty, remaining_qty, avg_fill_price }
          status: "open" | "filled" | "partial" | "cancelled"
        Returns None on API error — caller must fall back to position check.
        """
        try:
            contract_code = (
                self.symbol.split("/")[0] + "-" + self.symbol.split("/")[1].split(":")[0]
            )
            r = self.exchange.contractPrivatePostLinearSwapApiV1SwapCrossOrderInfo({
                "contract_code": contract_code,
                "order_id": order_id,
            })
            if r.get("status") != "ok":
                logger.warning(f"fetch_order {order_id}: api status={r.get('status')}")
                return None
            orders = r.get("data", [])
            if not orders:
                return None
            o = orders[0]
            # HTX status codes: 3=open, 4=partial_fill, 5=partial_cancelled, 6=filled, 7=cancelled
            htx_status = int(o.get("status", 0))
            if htx_status == 6:
                status = "filled"
            elif htx_status in (7, 5):
                status = "cancelled"
            elif htx_status == 4:
                status = "partial"
            else:
                status = "open"

            volume       = float(o.get("volume", 0))        # original contracts
            trade_volume = float(o.get("trade_volume", 0))  # filled contracts
            return {
                "id":              str(o.get("order_id", order_id)),
                "status":          status,
                "filled_qty":      round(trade_volume * self.CONTRACT_SIZE, 8),
                "remaining_qty":   round((volume - trade_volume) * self.CONTRACT_SIZE, 8),
                "avg_fill_price":  float(o.get("trade_avg_price") or 0.0),
            }
        except (ccxt.BaseError, Exception) as exc:
            logger.warning(f"fetch_order {order_id} failed ({type(exc).__name__}): {exc}")
            return None

    def get_open_orders(self) -> list[dict]:
        try:
            return _with_retry(self.exchange.fetch_open_orders, self.symbol)
        except ccxt.BaseError as exc:
            logger.error(f"fetch_open_orders failed: {exc}")
            return []

    def get_position(self) -> Optional[dict]:
        """Returns the current position dict or None if flat."""
        try:
            positions = _with_retry(self.exchange.fetch_positions, [self.symbol])
            for pos in positions:
                if pos.get("contracts", 0) != 0:
                    return pos
        except ccxt.BaseError as exc:
            logger.error(f"fetch_positions failed: {exc}")
        return None

    def get_balance(self) -> float:
        """Returns USDT available balance from the swap (unified) account."""
        try:
            # Support for HTX Unified Account
            res = self.exchange.privatePostLinearSwapV3UnifiedAccountInfo()
            for item in res.get("data", []):
                if item.get("margin_asset") == "USDT":
                    return float(item.get("margin_available", 0.0))
            return 0.0
        except Exception:
            try:
                bal = self.exchange.fetch_balance({"type": "swap"})
                return float(bal.get("USDT", {}).get("free", 0.0))
            except Exception:
                return 0.0
    def get_obi(self, depth: int = 20) -> float:
        """
        Calculate Order Book Imbalance (OBI).
        OBI = (bid_vol - ask_vol) / (bid_vol + ask_vol)
        Range: -1.0 (heavy sell side) to +1.0 (heavy buy side).
        """
        try:
            ob = _with_retry(self.exchange.fetch_order_book, self.symbol, limit=depth)
            if not ob: return 0.0
            bid_vol = sum(b[1] for b in ob["bids"][:depth])
            ask_vol = sum(a[1] for a in ob["asks"][:depth])
            if (bid_vol + ask_vol) == 0: return 0.0
            return (bid_vol - ask_vol) / (bid_vol + ask_vol)
        except Exception as exc:
            logger.error(f"fetch_order_book failed for OBI: {exc}")
            return 0.0

    def set_leverage(self, leverage: int) -> bool:
        try:
            self.exchange.set_leverage(leverage, self.symbol)
            logger.info(f"Leverage set to {leverage}x")
            return True
        except ccxt.BaseError as exc:
            logger.warning(f"set_leverage failed: {exc}")
            return False
