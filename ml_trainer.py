import os
import pandas as pd
import numpy as np
from catboost import CatBoostClassifier
from sklearn.model_selection import train_test_split
from market_data import fetch_ohlcv, calc_indicators
import logging
from config import cfg
from ml_features import FEATURE_COLS

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MLTrainer:
    def __init__(self, model_path: str = "sniper_model.cbm"):
        self.model_path = model_path
        self.features = []
        self.labels = []

    def prepare_dataset(self, main_df: pd.DataFrame, target_r: float = 1.5):
        """
        Simple labeling: look ahead and see if price hits (price + TP) before (price - SL).
        For ML, we simplify: target is 1 if next high >= TP price, else 0.
        """
        logger.info(f"Preparing dataset from {len(main_df)} bars...")
        
        # We need a better labeling logic for a real bot, 
        # but for this MVP we'll look at the next 5 bars.
        df = main_df.copy()
        
        # Features: RSI, ADX, BB position, etc.
        # (In a real scenario, we'd use the aligned multi-tf features from ml_features.py)
        # For now, let's use the primary TF features.
        
        X = df[FEATURE_COLS].values
        
        # Labeling
        y = []
        for i in range(len(df) - 5):
            entry_price = df["close"].iloc[i]
            atr = df["atr"].iloc[i]
            tp_price = entry_price + (atr * target_r)
            
            # Did high of any of next 5 bars hit tp?
            future_highs = df["high"].iloc[i+1 : i+6]
            if future_highs.max() >= tp_price:
                y.append(1)
            else:
                y.append(0)
        
        # Trim X to match y
        X = X[:len(y)]
        return X, np.array(y)

    def train(self, X, y):
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        
        model = CatBoostClassifier(
            iterations=500,
            learning_rate=0.05,
            depth=6,
            verbose=100,
            loss_function='Logloss'
        )
        
        logger.info("Training CatBoost model...")
        model.fit(X_train, y_train, eval_set=(X_test, y_test))
        
        model.save_model(self.model_path)
        logger.info(f"Model saved to {self.model_path}")
        
        # Metrics
        acc = model.score(X_test, y_test)
        logger.info(f"Model Accuracy: {acc:.2%}")

if __name__ == "__main__":
    # Example usage: collect 2000 bars and train
    from execution_engine import ExecutionEngine
    import ccxt
    
    exchange = ccxt.htx()
    df = fetch_ohlcv(exchange, cfg.symbol, "15m", limit=1000)
    df = calc_indicators(df, cfg)
    
    trainer = MLTrainer()
    X, y = trainer.prepare_dataset(df)
    if len(y) > 0:
        trainer.train(X, y)
