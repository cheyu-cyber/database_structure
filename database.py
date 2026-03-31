import sqlite3
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
import config


def get_connection():
    conn = sqlite3.connect(config.DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def execute(sql: str, params: tuple = ()):
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(sql, params)
        conn.commit()
        return {"ok": True, "rows": [dict(r) for r in cursor.fetchall()]}
    except Exception as e:
        return {"ok": False, "error": str(e)}
    finally:
        conn.close()
