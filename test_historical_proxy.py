from datetime import date, datetime, timezone
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
from urllib.parse import parse_qs

sys.path.insert(0, str(Path(__file__).resolve().parent))

from historical_proxy import _flatten_upstream_params, proxy_quotes_latest, proxy_stock_bars


class HistoricalProxyTests(unittest.TestCase):
    def test_session_backfill_uses_previous_session_without_lookahead(self):
        replay_now = datetime(2026, 5, 14, 13, 35, 16, tzinfo=timezone.utc)
        qs = parse_qs(
            "symbols=AAPL,NOK&timeframe=1Min"
            "&start=2026-05-14T04:00:00-04:00"
            "&end=2026-05-14T16:00:00-04:00"
            "&feed=iex"
        )

        params = _flatten_upstream_params(qs, date(2026, 5, 14), replay_now)

        self.assertEqual(params["start"], "2026-05-13T08:00:00.000Z")
        self.assertEqual(params["end"], "2026-05-14T13:35:16.000Z")

    def test_short_explicit_bar_poll_tracks_replay_clock(self):
        replay_now = datetime(2026, 5, 14, 13, 35, 16, tzinfo=timezone.utc)
        qs = parse_qs(
            "symbols=AAPL,NOK&timeframe=1Min"
            "&start=2026-05-15T03:32:16Z"
            "&end=2026-05-15T03:35:16Z"
            "&feed=iex"
        )

        params = _flatten_upstream_params(qs, date(2026, 5, 14), replay_now)

        self.assertEqual(params["start"], "2026-05-14T13:32:16.000Z")
        self.assertEqual(params["end"], "2026-05-14T13:35:16.000Z")

    def test_intraday_limit_only_recent_bars_includes_previous_session(self):
        replay_now = datetime(2026, 5, 14, 13, 35, 16, tzinfo=timezone.utc)
        qs = parse_qs("symbols=AAPL&timeframe=1Min&limit=1000&feed=iex")

        params = _flatten_upstream_params(qs, date(2026, 5, 14), replay_now)

        self.assertEqual(params["start"], "2026-05-13T08:00:00.000Z")
        self.assertEqual(params["end"], "2026-05-14T13:35:16.000Z")

    def test_stock_bars_replay_cache_reuses_successful_response(self):
        replay_now = datetime(2026, 5, 14, 13, 35, 16, tzinfo=timezone.utc)
        qs = parse_qs("symbols=AAPL&timeframe=1Min&limit=5&feed=iex")
        body = {"bars": {"AAPL": [{"t": "2026-05-14T13:35:00Z", "c": 1.0}]}}

        with tempfile.TemporaryDirectory() as tmp:
            with patch("historical_proxy.upstream_get_json", return_value=(200, body, "")) as upstream:
                self.assertEqual(
                    proxy_stock_bars(qs, date(2026, 5, 14), "https://example.test", "k", "s", replay_now, tmp),
                    (200, body),
                )
                self.assertEqual(
                    proxy_stock_bars(qs, date(2026, 5, 14), "https://example.test", "k", "s", replay_now, tmp),
                    (200, body),
                )

        self.assertEqual(upstream.call_count, 1)

    def test_quotes_replay_cache_reuses_successful_chunk_response(self):
        replay_now = datetime(2026, 5, 14, 13, 35, 16, tzinfo=timezone.utc)
        qs = parse_qs("symbols=AAPL&feed=iex")
        upstream_body = {
            "quotes": {
                "AAPL": [
                    {"t": "2026-05-14T13:35:15Z", "bp": 10.0, "ap": 10.1},
                ]
            }
        }
        expected = {"quotes": {"AAPL": {"t": "2026-05-14T13:35:15Z", "bp": 10.0, "ap": 10.1}}}

        with tempfile.TemporaryDirectory() as tmp:
            with patch("historical_proxy.upstream_get_json", return_value=(200, upstream_body, "")) as upstream:
                self.assertEqual(
                    proxy_quotes_latest(qs, date(2026, 5, 14), "https://example.test", "k", "s", replay_now, tmp),
                    (200, expected),
                )
                self.assertEqual(
                    proxy_quotes_latest(qs, date(2026, 5, 14), "https://example.test", "k", "s", replay_now, tmp),
                    (200, expected),
                )

        self.assertEqual(upstream.call_count, 1)


if __name__ == "__main__":
    unittest.main()
