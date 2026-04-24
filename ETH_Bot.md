
# ETH Scalper Bot — Technical Documentation

## Overview

Strategy: ETH/USDT Scalper Pro v6  
Type: Intraday Automated Trading Bot  
Exchange: HTX (Huobi)  
Language: Python

---

# Strategy Logic

## Entry Setups

### 1. Sweep & Reversal
- Liquidity sweep detection
- Volume confirmation
- Reversal entry

### 2. VWAP Mean Reversion
- Price deviation from VWAP
- Return to band

### 3. Momentum Pullback
- EMA trend
- Pullback entry
- RSI confirmation

---

# Scoring System

Weights:

Sweep: 40  
VWAP MR: 30  
Momentum: 30  
Volume Bonus: 15  
VWAP Bonus: 10  
EMA Bonus: 10  

Auto Trade Threshold: 55

---

# Risk Management

Risk per trade: 0.5%  
Leverage: Configurable  

Position Size:

Qty = Risk / Stop Distance

---

# Take Profit

TP1 = 0.8R  
TP2 = 1.8R  
TP3 = 3R  

After TP1 → Move Stop to Break Even

---

# Architecture

```
ETH Bot
 ├── market_data.py
 ├── strategy_engine.py
 ├── risk_engine.py
 ├── execution_engine.py
 ├── trade_manager.py
 ├── backtest.py
 ├── paper_trader.py
 └── eth_bot.py
```

---

# Development Tasks

## Phase 1 — Market Data
- Fetch OHLCV
- EMA
- VWAP
- ATR
- RSI

## Phase 2 — Strategy Engine
- Sweep detection
- VWAP MR
- Momentum pullback
- Score calculation

## Phase 3 — Risk Engine
- Position size calculation
- Risk control

## Phase 4 — Execution Engine
- Place orders
- Cancel orders
- Move stop

## Phase 5 — Trade Manager
- Manage TP
- Manage SL
- Move BE

## Phase 6 — Bot Controller
- Main loop
- Signal processing

## Phase 7 — Backtesting
- Historical testing
- Metrics

## Phase 8 — Paper Trading
- Simulation

## Phase 9 — Production
- VPS deployment
- Docker
- Monitoring

---

# Tech Stack

Python  
ccxt  
pandas  
numpy  
ta  
websockets  
asyncio  

---

# Testing Plan

1. Backtest 6 months  
2. Backtest 1 year  
3. Paper trading 1 week  
4. Live small capital  

---

# Performance Target

Win rate: 55‑65%  
RR: 1.8  
Trades/day: 3‑8  
Expected return: 0.2‑1% daily

---

# Deployment

Docker  
VPS  
Auto restart  
Logging  

---

# Future Improvements

- ML scoring
- Multi‑timeframe
- Multi‑pair trading
- Risk optimization

---

END
