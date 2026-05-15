from datetime import date, datetime, timezone
from pathlib import Path
import sys
import unittest
from urllib.parse import parse_qs

sys.path.insert(0, str(Path(__file__).resolve().parent))

from historical_proxy import _flatten_upstream_params


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


if __name__ == "__main__":
    unittest.main()
