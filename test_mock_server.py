from datetime import date, datetime, timezone
import unittest
from unittest.mock import patch

import mock_server
from mock_server import MockState, _mock_chart_series, _order_payload, _strategy_from_client_order_id


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

    def test_replay_sell_fill_rejects_quote_far_below_latest_bar(self):
        state = MockState("100000", True, alpaca_historical_et_date=date(2026, 5, 11))
        now = datetime(2026, 5, 11, 13, 31, 50, tzinfo=timezone.utc)
        state.remember_market_data(
            {
                "bars": {
                    "B": [
                        {
                            "o": 44.95,
                            "h": 45.11,
                            "l": 44.85,
                            "c": 45.07,
                            "vw": 45.038282,
                            "t": "2026-05-11T13:31:00Z",
                        }
                    ]
                }
            }
        )
        state.remember_market_quote("B", "41.6687", "41.70", "2026-05-11T13:31:50Z")

        self.assertEqual(state.fill_price("B", "sell", now), 44.85)

    def test_replay_buy_fill_rejects_quote_far_above_latest_bar(self):
        state = MockState("100000", True, alpaca_historical_et_date=date(2026, 5, 11))
        now = datetime(2026, 5, 11, 13, 31, 50, tzinfo=timezone.utc)
        state.remember_market_data(
            {
                "bars": {
                    "B": [
                        {
                            "o": 44.95,
                            "h": 45.11,
                            "l": 44.85,
                            "c": 45.07,
                            "vw": 45.038282,
                            "t": "2026-05-11T13:31:00Z",
                        }
                    ]
                }
            }
        )
        state.remember_market_quote("B", "48.90", "49.00", "2026-05-11T13:31:50Z")

        self.assertEqual(state.fill_price("B", "buy", now), 45.11)

    def test_replay_fill_keeps_quote_inside_bar_guard(self):
        state = MockState("100000", True, alpaca_historical_et_date=date(2026, 5, 11))
        now = datetime(2026, 5, 11, 13, 31, 50, tzinfo=timezone.utc)
        state.remember_market_data(
            {
                "bars": {
                    "B": [
                        {
                            "o": 44.95,
                            "h": 45.11,
                            "l": 44.85,
                            "c": 45.07,
                            "vw": 45.038282,
                            "t": "2026-05-11T13:31:00Z",
                        }
                    ]
                }
            }
        )
        state.remember_market_quote("B", "44.80", "45.16", "2026-05-11T13:31:50Z")

        self.assertEqual(state.fill_price("B", "sell", now), 44.8)
        self.assertEqual(state.fill_price("B", "buy", now), 45.16)

    def test_replay_clock_uses_speed_multiplier(self):
        wall_start = datetime(2026, 5, 26, 0, 0, 0, tzinfo=timezone.utc)
        wall_now = datetime(2026, 5, 26, 0, 0, 10, tzinfo=timezone.utc)
        replay_start = datetime(2026, 5, 11, 13, 30, 0, tzinfo=timezone.utc)
        with patch.object(mock_server, "_utc_now", side_effect=[wall_start, wall_now]):
            state = MockState(
                "100000",
                True,
                alpaca_historical_et_date=date(2026, 5, 11),
                replay_speed=3.0,
            )
            state.replay_started_utc = replay_start

            self.assertEqual(state.replay_now_utc(), datetime(2026, 5, 11, 13, 30, 30, tzinfo=timezone.utc))

    def test_replay_clock_can_snap_to_fixed_step(self):
        wall_start = datetime(2026, 5, 26, 0, 0, 0, tzinfo=timezone.utc)
        wall_now = datetime(2026, 5, 26, 0, 0, 11, tzinfo=timezone.utc)
        replay_start = datetime(2026, 5, 11, 13, 30, 0, tzinfo=timezone.utc)
        with patch.object(mock_server, "_utc_now", side_effect=[wall_start, wall_now]):
            state = MockState(
                "100000",
                True,
                alpaca_historical_et_date=date(2026, 5, 11),
                replay_speed=3.0,
                replay_step_seconds=30.0,
            )
            state.replay_started_utc = replay_start

            self.assertEqual(state.replay_now_utc(), datetime(2026, 5, 11, 13, 30, 30, tzinfo=timezone.utc))

    def test_strategy_is_parsed_from_bk_client_order_id(self):
        self.assertEqual(
            _strategy_from_client_order_id("bk-si-uber-1778508121000-b-deadbeef"),
            "steady_intraday",
        )
        self.assertEqual(
            _strategy_from_client_order_id("bk-mei-cifr-1778508440000-s-deadbeef"),
            "macd_early_impulse",
        )
        self.assertEqual(_strategy_from_client_order_id("codex-uber-1778508121000-buy-deadbeef"), "")

    def test_chart_series_filters_trade_events_by_strategy(self):
        state = MockState("100000", True)
        now = datetime(2026, 5, 14, 13, 36, tzinfo=timezone.utc)
        state.remember_market_price("UBER", "75.00", "2026-05-14T13:36:00Z")
        state.record_tracked_symbols(["UBER"])

        steady_order = _order_payload(
            "11111111-1111-4111-8111-111111111111",
            {
                "symbol": "UBER",
                "qty": "1",
                "side": "buy",
                "client_order_id": "bk-si-uber-1778508121000-b-deadbeef",
            },
            "filled",
            "1",
            "75.00",
            now,
        )
        macd_order = _order_payload(
            "22222222-2222-4222-8222-222222222222",
            {
                "symbol": "UBER",
                "qty": "1",
                "side": "sell",
                "client_order_id": "bk-mei-uber-1778508181000-s-feedface",
            },
            "filled",
            "1",
            "75.10",
            now,
        )
        state.apply_fill(steady_order)
        state.apply_fill(macd_order)

        code, body = _mock_chart_series(
            state,
            {"symbols": ["UBER"], "minutes": ["60"], "timeframe": ["1Min"], "strategy": ["steady_intraday"]},
        )

        self.assertEqual(code, 200)
        self.assertEqual(body["trade_strategies"], ["macd_early_impulse", "steady_intraday"])
        self.assertEqual(body["symbols_with_trades"], ["UBER"])
        self.assertEqual(body["trade_counts_by_symbol"], {"UBER": 1})
        self.assertEqual(len(body["trade_events"]), 1)
        self.assertEqual(body["trade_events"][0]["strategy"], "steady_intraday")
        self.assertEqual(body["trade_events"][0]["qty"], "1")
        self.assertEqual(body["trade_events"][0]["fill_stage"], "entry")

    def test_trade_events_mark_partial_and_full_sell_quantities(self):
        state = MockState("100000", True)
        now = datetime(2026, 5, 14, 13, 36, tzinfo=timezone.utc)
        state.remember_market_price("BB", "10.00", "2026-05-14T13:36:00Z")
        state.record_tracked_symbols(["BB"])

        buy_order = _order_payload(
            "11111111-1111-4111-8111-111111111111",
            {"symbol": "BB", "qty": "10", "side": "buy"},
            "filled",
            "10",
            "10.00",
            now,
        )
        partial_sell = _order_payload(
            "22222222-2222-4222-8222-222222222222",
            {"symbol": "BB", "qty": "4", "side": "sell"},
            "filled",
            "4",
            "10.50",
            now,
        )
        full_sell = _order_payload(
            "33333333-3333-4333-8333-333333333333",
            {"symbol": "BB", "qty": "6", "side": "sell"},
            "filled",
            "6",
            "11.00",
            now,
        )

        state.apply_fill(buy_order)
        state.apply_fill(partial_sell)
        state.apply_fill(full_sell)

        code, body = _mock_chart_series(
            state,
            {"symbols": ["BB"], "minutes": ["60"], "timeframe": ["1Min"]},
        )

        self.assertEqual(code, 200)
        events = body["trade_events"]
        self.assertEqual([ev["qty"] for ev in events], ["10", "4", "6"])
        self.assertEqual([ev["fill_stage"] for ev in events], ["entry", "partial", "full"])


if __name__ == "__main__":
    unittest.main()
