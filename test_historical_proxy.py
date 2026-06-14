from datetime import date, datetime, timezone
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
from urllib.parse import parse_qs

sys.path.insert(0, str(Path(__file__).resolve().parent))

from historical_proxy import (
    _flatten_upstream_params,
    is_trading_day,
    iter_trading_days,
    map_replay_elapsed_to_utc,
    proxy_quotes_latest,
    proxy_stock_bars,
    proxy_stock_trades,
    total_replay_session_seconds,
)


class HistoricalProxyTests(unittest.TestCase):
    def test_trading_day_range_skips_weekends(self):
        self.assertEqual(
            iter_trading_days(date(2026, 5, 18), date(2026, 5, 22)),
            [
                date(2026, 5, 18),
                date(2026, 5, 19),
                date(2026, 5, 20),
                date(2026, 5, 21),
                date(2026, 5, 22),
            ],
        )
        self.assertFalse(is_trading_day(date(2026, 5, 16)))  # Saturday
        self.assertFalse(is_trading_day(date(2026, 5, 17)))  # Sunday

    def test_trading_day_range_skips_weekend_in_middle(self):
        # Fri .. Mon: Saturday/Sunday omitted.
        self.assertEqual(
            iter_trading_days(date(2026, 5, 15), date(2026, 5, 18)),
            [date(2026, 5, 15), date(2026, 5, 18)],
        )

    def test_map_replay_elapsed_skips_sat_sun_after_friday_close(self):
        elapsed = 6.5 * 3600  # full Fri regular session
        now = map_replay_elapsed_to_utc(date(2026, 5, 15), date(2026, 5, 18), None, elapsed)
        self.assertEqual(now.astimezone(timezone.utc), datetime(2026, 5, 18, 13, 30, tzinfo=timezone.utc))

    def test_total_replay_session_seconds_counts_regular_hours_only(self):
        total = total_replay_session_seconds(date(2026, 5, 18), date(2026, 5, 22), None)
        self.assertEqual(total, 5 * 6.5 * 3600)

    def test_map_replay_elapsed_skips_overnight_between_sessions(self):
        # One full Mon session (6.5h) plus 30 minutes into Tue.
        elapsed = 6.5 * 3600 + 30 * 60
        now = map_replay_elapsed_to_utc(date(2026, 5, 18), date(2026, 5, 22), None, elapsed)
        self.assertEqual(now.astimezone(timezone.utc), datetime(2026, 5, 19, 14, 0, tzinfo=timezone.utc))

    def test_map_replay_elapsed_clamps_after_last_session_close(self):
        now = map_replay_elapsed_to_utc(date(2026, 5, 18), date(2026, 5, 18), None, 1_000_000)
        self.assertEqual(now.astimezone(timezone.utc), datetime(2026, 5, 18, 20, 0, tzinfo=timezone.utc))

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

    def test_morning_session_preload_uses_session_backfill(self):
        replay_now = datetime(2026, 6, 1, 13, 35, 0, tzinfo=timezone.utc)  # 09:35 ET
        qs = parse_qs(
            "symbols=BROS&timeframe=1Min"
            "&start=2026-06-01T08:00:00Z"
            "&end=2026-06-01T13:35:00Z"
            "&feed=iex"
        )

        params = _flatten_upstream_params(qs, date(2026, 6, 1), replay_now)

        self.assertEqual(params["start"], "2026-05-29T08:00:00.000Z")
        self.assertEqual(params["end"], "2026-06-01T13:35:00.000Z")

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

    def test_stock_trades_replay_cache_reuses_successful_response(self):
        replay_now = datetime(2026, 5, 14, 13, 35, 16, tzinfo=timezone.utc)
        qs = parse_qs("symbols=AAPL&start=2026-05-14T13:35:00Z&end=2026-05-14T13:35:16Z&feed=iex")
        body = {"trades": {"AAPL": [{"t": "2026-05-14T13:35:10Z", "p": 10.0, "s": 100}]}}
        expected = {"trades": {"AAPL": body["trades"]["AAPL"]}, "next_page_token": None}

        with tempfile.TemporaryDirectory() as tmp:
            with patch("historical_proxy.upstream_get_json", return_value=(200, body, "")) as upstream:
                self.assertEqual(
                    proxy_stock_trades(qs, date(2026, 5, 14), "https://example.test", "k", "s", replay_now, tmp),
                    (200, expected),
                )
                self.assertEqual(
                    proxy_stock_trades(qs, date(2026, 5, 14), "https://example.test", "k", "s", replay_now, tmp),
                    (200, expected),
                )

        self.assertEqual(upstream.call_count, 1)


if __name__ == "__main__":
    unittest.main()
