# MemPalace Diary — Session: Hardening & Strategy Optimization (2026-04-24)

## 🛠 Critical Fixes (P0)
- **Duplicate Notifications**: Implemented `notified_ids` table in SQLite. The bot now tracks every event/order ID reported to Telegram to prevent duplicates across restarts.
- **Heartbeat Spam**: Moved the heartbeat timer to the database (`bot_meta` table). Heartbeats now strictly follow the 6-hour interval regardless of system reboots.
- **DB Stability**: Centralized all SQLite operations in `persistence.py` to fix `AttributeError` and connection issues in `TradeManager`.

## 📈 Strategy Enhancements
- **ADX Filter**: Momentum and Trend-Ride setups now require `ADX > 20` (trend presence) and `ADX < 45` (avoiding overextension).
- **Pin-Bar Rejection**: Sweep & Reversal setups now require a rejection wick of at least **60%** of the total candle range.
- **Threshold**: Raised `auto_trade_threshold` to **50.0** for higher entry quality.

## 📊 Backtest Insights (ETH/USDT, ~20 days)
| Setup | Win Rate | PnL (Net) | Notes |
| :--- | :--- | :--- | :--- |
| **15m (Default)** | 22% | -$19.84 | Most stable baseline. |
| **30m (Increased TF)**| 43% | -$70.62 | Higher win rate but wider stops lead to larger losses. |
| **15m + 4h Trend** | 9% | -$112.55 | Strict HTF filter hurts reversals (buys the top of 4h moves). |

## 🚀 The Road to 70% Win Rate
**Next Session Objectives:**
1. **Ultra-Selectivity**: Implement a "Confluence" requirement where at least 3 indicators must align.
2. **Session Timing**: Add filters to trade only during London (08:00-16:00 UTC) and NY (13:00-21:00 UTC) sessions.
3. **Volume Surge**: Add requirement for volume to be >2x SMA on the signal bar.
4. **Take Profit Optimization**: Test shorter TP1 targets (0.3-0.5 ATR) to capture quick scalps and raise win rate.

**Current Deployment**: LIVE on VPS `168.231.118.173` | PM2: `ethbot` | Threshold: `50.0`
## [2026-04-24] Production Launch
- Status: LIVE on HTX
- ML: 0.75
- Fixed HTX Unified Account balance and numeric IDs.
