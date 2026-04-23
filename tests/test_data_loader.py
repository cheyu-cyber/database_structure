"""Unit tests for services/data_loader.py."""

import csv

import httpx
import pytest
import respx


# ─────────────────────── infer_type ───────────────────────

class TestInferType:
    @pytest.mark.parametrize("value,expected", [
        ("42", "INTEGER"),
        ("-5", "INTEGER"),
        ("0", "INTEGER"),
        ("3.14", "REAL"),
        ("-2.5", "REAL"),
        ("hello", "TEXT"),
        ("", "TEXT"),
        ("abc123", "TEXT"),
    ])
    def test_cases(self, value, expected):
        from data_loader import infer_type
        assert infer_type(value) == expected


# ─────────────────────── infer_schema ───────────────────────

class TestInferSchema:
    def test_empty_rows(self):
        from data_loader import infer_schema
        col_types, rows = infer_schema(iter([]), ["id", "name"])
        assert col_types == {"id": "TEXT", "name": "TEXT"}
        assert rows == []

    def test_integer_columns(self):
        from data_loader import infer_schema
        rows = [{"id": "1", "count": "10"}, {"id": "2", "count": "20"}]
        col_types, _ = infer_schema(iter(rows), ["id", "count"])
        assert col_types == {"id": "INTEGER", "count": "INTEGER"}

    def test_widen_int_to_real(self):
        from data_loader import infer_schema
        rows = [{"val": "1"}, {"val": "2.5"}]
        col_types, _ = infer_schema(iter(rows), ["val"])
        assert col_types["val"] == "REAL"

    def test_widen_int_to_text(self):
        from data_loader import infer_schema
        rows = [{"val": "1"}, {"val": "hello"}]
        col_types, _ = infer_schema(iter(rows), ["val"])
        assert col_types["val"] == "TEXT"

    def test_widen_real_to_text(self):
        from data_loader import infer_schema
        rows = [{"val": "1.5"}, {"val": "hello"}]
        col_types, _ = infer_schema(iter(rows), ["val"])
        assert col_types["val"] == "TEXT"

    def test_preserves_all_rows(self):
        from data_loader import infer_schema
        rows = [{"a": "1"}, {"a": "2"}, {"a": "3"}]
        _, out = infer_schema(iter(rows), ["a"])
        assert len(out) == 3

    def test_empty_values_are_skipped(self):
        from data_loader import infer_schema
        rows = [{"val": ""}, {"val": "42"}]
        col_types, _ = infer_schema(iter(rows), ["val"])
        assert col_types["val"] == "INTEGER"


# ─────────────────────── load() ───────────────────────

class TestLoad:
    def _write_csv(self, tmp_path, name, header, rows):
        path = tmp_path / name
        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=header)
            writer.writeheader()
            for row in rows:
                writer.writerow(row)
        return str(path)

    def test_create_then_insert(self, tmp_path, capsys):
        import config
        csv_path = self._write_csv(tmp_path, "items.csv", ["id", "name"], [
            {"id": "1", "name": "Apple"},
            {"id": "2", "name": "Banana"},
        ])
        with respx.mock:
            respx.post(f"{config.URLS['validator']}/execute").mock(
                return_value=httpx.Response(200, json={"ok": True})
            )
            respx.get(f"{config.URLS['validator']}/schema").mock(
                return_value=httpx.Response(200, json={
                    "schema": {"items": [
                        {"name": "id", "type": "INTEGER"},
                        {"name": "name", "type": "TEXT"},
                    ]}
                })
            )
            from data_loader import load
            load("testdb", csv_path)
        out = capsys.readouterr().out
        assert "2 inserted" in out
        assert "0 errors" in out

    def test_empty_csv_exits(self, tmp_path):
        path = tmp_path / "empty.csv"
        path.write_text("")
        from data_loader import load
        with pytest.raises(SystemExit):
            load("testdb", str(path))

    def test_create_failure_exits(self, tmp_path):
        import config
        csv_path = self._write_csv(tmp_path, "fail.csv", ["a"], [{"a": "1"}])
        with respx.mock:
            respx.post(f"{config.URLS['validator']}/execute").mock(
                return_value=httpx.Response(200, json={"ok": False, "reason": "exists"})
            )
            from data_loader import load
            with pytest.raises(SystemExit):
                load("testdb", str(csv_path))

    def test_insert_failure_counted(self, tmp_path, capsys):
        import config
        csv_path = self._write_csv(tmp_path, "mixed.csv", ["a"], [
            {"a": "1"}, {"a": "2"},
        ])
        responses = iter([
            httpx.Response(200, json={"ok": True}),                      # CREATE
            httpx.Response(200, json={"ok": True}),                      # INSERT 1
            httpx.Response(200, json={"ok": False, "error": "boom"}),    # INSERT 2
        ])
        with respx.mock:
            respx.post(f"{config.URLS['validator']}/execute").mock(
                side_effect=lambda req: next(responses)
            )
            respx.get(f"{config.URLS['validator']}/schema").mock(
                return_value=httpx.Response(200, json={"schema": {
                    "mixed": [{"name": "a", "type": "INTEGER"}],
                }})
            )
            from data_loader import load
            load("testdb", csv_path)
        out = capsys.readouterr().out
        assert "1 inserted" in out
        assert "1 errors" in out
