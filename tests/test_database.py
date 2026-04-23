"""Unit tests for services/database.py."""

import os
import sqlite3


class TestDbPath:
    def test_returns_db_dir_plus_name(self, tmp_db_dir):
        from database import _db_path
        assert _db_path("mydb") == os.path.join(str(tmp_db_dir), "mydb.db")

    def test_creates_directory_if_missing(self, tmp_path, monkeypatch):
        import config
        new_dir = tmp_path / "subdir"
        monkeypatch.setattr(config, "DB_DIR", str(new_dir))
        from database import _db_path
        _db_path("anything")
        assert new_dir.is_dir()


class TestGetConnection:
    def test_returns_a_connection(self):
        import database
        conn = database.get_connection("testdb")
        assert isinstance(conn, sqlite3.Connection)
        conn.close()

    def test_row_factory_is_sqlite_row(self):
        import database
        conn = database.get_connection("testdb")
        assert conn.row_factory is sqlite3.Row
        conn.close()

    def test_default_db_works(self):
        import database
        conn = database.get_connection()
        assert conn is not None
        conn.close()


class TestExecute:
    def test_create_table(self):
        import database
        result = database.execute("CREATE TABLE t1 (id INTEGER, name TEXT)", db="testdb")
        assert result == {"ok": True, "rows": []}

    def test_insert_then_select(self):
        import database
        database.execute("CREATE TABLE t2 (id INTEGER, val TEXT)", db="testdb")
        database.execute("INSERT INTO t2 (id, val) VALUES (1, 'hello')", db="testdb")
        result = database.execute("SELECT * FROM t2", db="testdb")
        assert result["ok"] is True
        assert result["rows"] == [{"id": 1, "val": "hello"}]

    def test_select_empty(self):
        import database
        database.execute("CREATE TABLE t3 (x INTEGER)", db="testdb")
        result = database.execute("SELECT * FROM t3", db="testdb")
        assert result["ok"] is True
        assert result["rows"] == []

    def test_invalid_sql_returns_error(self):
        import database
        result = database.execute("THIS IS NOT SQL", db="testdb")
        assert result["ok"] is False
        assert "error" in result

    def test_parameterised_insert(self):
        import database
        database.execute("CREATE TABLE t4 (a TEXT)", db="testdb")
        result = database.execute(
            "INSERT INTO t4 (a) VALUES (?)", ("foo",), db="testdb"
        )
        assert result["ok"] is True

    def test_multiple_rows_returned(self):
        import database
        database.execute("CREATE TABLE t5 (id INTEGER)", db="testdb")
        for i in (1, 2, 3):
            database.execute("INSERT INTO t5 VALUES (?)", (i,), db="testdb")
        result = database.execute("SELECT * FROM t5 ORDER BY id", db="testdb")
        assert [r["id"] for r in result["rows"]] == [1, 2, 3]

    def test_default_db_is_used(self):
        import database
        result = database.execute("CREATE TABLE def_t (x INTEGER)")
        assert result["ok"] is True

    def test_writes_persist_across_connections(self):
        import database
        database.execute("CREATE TABLE persist (x INTEGER)", db="testdb")
        database.execute("INSERT INTO persist VALUES (42)", db="testdb")
        assert database.execute("SELECT x FROM persist", db="testdb")["rows"] == [{"x": 42}]
