import sqlite3
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
import config


def _db_path(db: str) -> str:
    """Resolve a database name to its file path, ensuring DB_DIR exists."""
    os.makedirs(config.DB_DIR, exist_ok=True)
    return os.path.join(config.DB_DIR, f"{db}.db")


def get_connection(db: str = config.DEFAULT_DB):
    conn = sqlite3.connect(_db_path(db))
    conn.row_factory = sqlite3.Row
    return conn


def execute(sql: str, params: tuple = (), db: str = config.DEFAULT_DB):
    conn = get_connection(db)
    try:
        cursor = conn.cursor()
        cursor.execute(sql, params)
        conn.commit()
        return {"ok": True, "rows": [dict(r) for r in cursor.fetchall()]}
    except Exception as e:
        return {"ok": False, "error": str(e)}
    finally:
        conn.close()
