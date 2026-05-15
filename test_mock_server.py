from datetime import datetime, timezone
import unittest

from mock_server import MockState


class MockServerTests(unittest.TestCase):
    def test_market_fill_uses_quote_side(self):
        state = MockState("100000", True)
        now = datetime(2026, 5, 14, 13, 36, tzinfo=timezone.utc)
        state.remember_market_quote("HPE", "33.88", "34.09", "2026-05-14T13:36:00Z")

        self.assertEqual(state.fill_price("HPE", "buy", now), 34.09)
        self.assertEqual(state.fill_price("HPE", "sell", now), 33.88)

    def test_market_fill_falls_back_to_mid_when_quote_side_missing(self):
        state = MockState("100000", True)
        now = datetime(2026, 5, 14, 13, 36, tzinfo=timezone.utc)
        state.remember_market_price("HPE", "34.00", "2026-05-14T13:36:00Z")

        self.assertEqual(state.fill_price("HPE", "buy", now), 34.0)
        self.assertEqual(state.fill_price("HPE", "sell", now), 34.0)


if __name__ == "__main__":
    unittest.main()
