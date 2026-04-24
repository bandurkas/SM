import sqlite3
import json
import logging
import os
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

class StateDB:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.backup_path = db_path.replace(".db", ".json")
        self._init_db()

    def _init_db(self):
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS bot_state (
                        key TEXT PRIMARY KEY,
                        value TEXT,
                        updated_at TEXT
                    )
                """)
                conn.execute("CREATE TABLE IF NOT EXISTS bot_meta (key TEXT PRIMARY KEY, value TEXT)")
                conn.execute("CREATE TABLE IF NOT EXISTS notified_ids (id TEXT PRIMARY KEY)")
                conn.commit()
        except Exception as e:
            logger.error(f"Failed to initialize SQLite DB: {e}")

    def save_state(self, data: dict):
        try:
            serialized = json.dumps(data)
            now = datetime.now(timezone.utc).isoformat()
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    "INSERT OR REPLACE INTO bot_state (key, value, updated_at) VALUES (?, ?, ?)",
                    ("main", serialized, now)
                )
                conn.commit()
            
            # P1 Hardening: JSON backup
            try:
                with open(self.backup_path, "w") as f:
                    json.dump(data, f)
            except Exception as backup_exc:
                logger.warning(f"JSON backup failed: {backup_exc}")
                
        except Exception as e:
            logger.error(f"SQLite save_state failed: {e}")

    def load_state(self) -> dict:
        try:
            # 1. Try SQLite
            if os.path.exists(self.db_path):
                try:
                    with sqlite3.connect(self.db_path) as conn:
                        cursor = conn.execute("SELECT value FROM bot_state WHERE key = 'main'")
                        row = cursor.fetchone()
                        if row:
                            return json.loads(row[0])
                except Exception as sqlite_exc:
                    logger.error(f"SQLite load_state failed, trying backup: {sqlite_exc}")
            
            # 2. Fallback to JSON backup
            if os.path.exists(self.backup_path):
                with open(self.backup_path, "r") as f:
                    return json.load(f)
                    
        except Exception as e:
            logger.error(f"Total load_state failure: {e}")
        return {}

    def get_meta(self, key: str, default: str = "") -> str:
        try:
            with sqlite3.connect(self.db_path) as conn:
                res = conn.execute("SELECT value FROM bot_meta WHERE key=?", (key,)).fetchone()
                return res[0] if res else default
        except Exception:
            return default

    def set_meta(self, key: str, value: str):
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("INSERT OR REPLACE INTO bot_meta (key, value) VALUES (?, ?)", (key, value))
                conn.commit()
        except Exception as e:
            logger.error(f"SQLite set_meta failed: {e}")

    def was_notified(self, event_id: str) -> bool:
        try:
            with sqlite3.connect(self.db_path) as conn:
                res = conn.execute("SELECT 1 FROM notified_ids WHERE id=?", (event_id,)).fetchone()
                return res is not None
        except Exception:
            return False

    def mark_notified(self, event_id: str):
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("INSERT OR IGNORE INTO notified_ids (id) VALUES (?)", (event_id,))
                conn.commit()
        except Exception as e:
            logger.error(f"SQLite mark_notified failed: {e}")

def log_balance(data_dir: str, balance: float):
    """Log current balance to CSV for PnL tracking."""
    try:
        os.makedirs(data_dir, exist_ok=True)
        path = os.path.join(data_dir, "balance_history.csv")
        exists = os.path.exists(path)
        
        now = datetime.now(timezone.utc).isoformat(timespec="seconds")
        with open(path, "a") as f:
            if not exists:
                f.write("timestamp,balance\n")
            f.write(f"{now},{balance:.2f}\n")
    except Exception as e:
        logger.error(f"Failed to log balance: {e}")
