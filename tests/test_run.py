"""Unit tests for run.py — only the stand-alone helpers, not the orchestrator."""

from unittest.mock import MagicMock, patch

import httpx


class TestWaitForService:
    def test_ready_immediately(self):
        from run import wait_for_service
        with patch("httpx.get", return_value=MagicMock()):
            assert wait_for_service("http://localhost:9999", "test", retries=1) is True

    def test_never_ready_returns_false(self):
        from run import wait_for_service
        with patch("httpx.get", side_effect=httpx.ConnectError("down")):
            with patch("time.sleep"):
                assert wait_for_service("http://localhost:9999", "test", retries=2) is False

    def test_ready_after_a_few_retries(self):
        from run import wait_for_service
        attempts = {"n": 0}

        def fake_get(*args, **kwargs):
            attempts["n"] += 1
            if attempts["n"] < 3:
                raise httpx.ConnectError("not yet")
            return MagicMock()

        with patch("httpx.get", side_effect=fake_get):
            with patch("time.sleep"):
                assert wait_for_service("http://localhost:9999", "test", retries=5) is True
        assert attempts["n"] == 3
