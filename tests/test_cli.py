"""Unit tests for services/cli.py."""

from unittest.mock import MagicMock, patch

import httpx


# ─────────────────────── print_result ───────────────────────

class TestPrintResult:
    def test_ok_with_rows(self, capsys):
        from cli import print_result
        print_result({"ok": True, "rows": [{"id": 1, "name": "Alice"}]})
        assert "Alice" in capsys.readouterr().out

    def test_ok_multiple_rows(self, capsys):
        from cli import print_result
        print_result({"ok": True, "rows": [{"x": 1}, {"x": 2}]})
        out = capsys.readouterr().out
        assert "1" in out and "2" in out

    def test_ok_empty_rows(self, capsys):
        from cli import print_result
        print_result({"ok": True, "rows": []})
        assert "no rows" in capsys.readouterr().out

    def test_ok_no_rows_key(self, capsys):
        from cli import print_result
        print_result({"ok": True})
        assert "OK" in capsys.readouterr().out

    def test_error_with_reason(self, capsys):
        from cli import print_result
        print_result({"ok": False, "reason": "Table not found"})
        assert "Table not found" in capsys.readouterr().out

    def test_error_with_error_field(self, capsys):
        from cli import print_result
        print_result({"ok": False, "error": "syntax error"})
        assert "syntax error" in capsys.readouterr().out

    def test_error_unknown(self, capsys):
        from cli import print_result
        print_result({"ok": False})
        assert "Unknown error" in capsys.readouterr().out


# ─────────────────────── run() ───────────────────────

class TestRun:
    def test_exit_command(self, capsys):
        from cli import run
        with patch("builtins.input", side_effect=["exit"]):
            run()
        assert "Bye" in capsys.readouterr().out

    def test_empty_input_continues(self, capsys):
        from cli import run
        with patch("builtins.input", side_effect=["", "exit"]):
            run()
        assert "Bye" in capsys.readouterr().out

    def test_use_command_switches_db(self, capsys):
        from cli import run
        with patch("builtins.input", side_effect=["use mydb", "exit"]):
            run()
        assert "mydb" in capsys.readouterr().out

    def test_eof_exits_cleanly(self, capsys):
        from cli import run
        with patch("builtins.input", side_effect=EOFError):
            run()
        assert "Bye" in capsys.readouterr().out

    def test_keyboard_interrupt_exits_cleanly(self, capsys):
        from cli import run
        with patch("builtins.input", side_effect=KeyboardInterrupt):
            run()
        assert "Bye" in capsys.readouterr().out

    def test_query_sends_to_service(self, capsys):
        from cli import run
        mock_response = MagicMock()
        mock_response.json.return_value = {"ok": True, "rows": [{"id": 1}]}
        with patch("builtins.input", side_effect=["select users", "exit"]):
            with patch("httpx.post", return_value=mock_response) as post:
                run()
        assert post.called
        assert "1" in capsys.readouterr().out

    def test_connection_error_is_handled(self, capsys):
        from cli import run
        with patch("builtins.input", side_effect=["select users", "exit"]):
            with patch("httpx.post", side_effect=httpx.ConnectError("fail")):
                run()
        assert "not reachable" in capsys.readouterr().out

    def test_generic_exception_is_handled(self, capsys):
        from cli import run
        with patch("builtins.input", side_effect=["select users", "exit"]):
            with patch("httpx.post", side_effect=RuntimeError("boom")):
                run()
        assert "boom" in capsys.readouterr().out
