#!/usr/bin/env python3
"""
Alpaca REST mock scoped to what ``stocktrader/main.py`` can hit (via alpaca-py).

``main()`` builds a market-data stream and an executor. Only those paths pull
REST against this mock:

**Trading** (``http://127.0.0.1:<trading-port>``) — used only when
``EXECUTION_MODE=alpaca_paper`` (``AlpacaPaperExecutor`` in ``execution.py``):

- ``GET /v2/clock`` — regular session gate
- ``GET /v2/account`` — startup cash sync
- ``GET /v2/positions`` — startup reconcile
- ``GET /v2/orders`` — startup cancel of open orders for watched symbols
- ``GET /v2/orders/{uuid}`` — fill polling after submit
- ``GET /v2/assets`` — optional universe discovery (``strategy_selectors/select_market_universe.py``):
  proxies to Alpaca when upstream keys are set; otherwise returns a built-in active US equity list
- ``POST /v2/orders`` — buy / sell
- ``DELETE /v2/orders/{uuid}`` — cancel after timeout

**Market data** (``http://127.0.0.1:<data-port>``):

- ``GET /v2/stocks/quotes/latest`` — ``AlpacaRestPollingStream`` each poll, and
  ``AlpacaPaperExecutor._fresh_entry_price`` before a buy when stream mode.
  Without ``--alpaca-date``, returns synthetic quotes (monotonic ``t``, small
  spread) from ``--price`` / defaults.
- ``GET /v2/stocks/bars`` — ``AlpacaRestPollingStream`` when
  ``ALPACA_MARKET_DATA_MODE=rest``; without ``--alpaca-date``, simple synthetic OHLC
  from the same mock mids.
- ``GET /chart`` — browser line chart (Chart.js from a CDN) that polls
  ``GET /v1/mock/chart-series``. Symbol chips switch which single ticker is charted;
  buy/sell fill markers come from mock order fills.

Stream mode still uses Alpaca WebSockets for live bars/quotes; this mock does
not implement WS.

**Alpaca historical replay:** pass ``--alpaca-date YYYY-MM-DD`` (and upstream API
keys in ``.env`` or flags). Data routes proxy to Alpaca’s Data API with runtime
request times shifted onto that US/Eastern calendar day, starting at replay
clock time (``--alpaca-time HH:MM``, default ``09:30``) and then advancing with
server runtime. Trading routes stay local.

Run (local synthetic data)::

    python mock_server.py --access-log
    python mock_server.py --price INTC=35.5

Point stocktrader at the mock (see ``config.py`` / ``alpaca_client.py``)::

    ALPACA_TRADING_BASE_URL=http://127.0.0.1:19901
    ALPACA_DATA_BASE_URL=http://127.0.0.1:19902
    ALPACA_API_KEY=test
    ALPACA_SECRET_KEY=test

**Configuration file:** on startup, ``mock_server.py`` loads the first existing file among
``--env-file PATH`` (if given), ``.env`` next to ``mock_server.py``, then ``.env`` in the
current working directory. Variables are merged with ``os.environ.setdefault`` (they do
not override already-exported shell variables). See ``env.example`` in this directory for
supported keys (``ALPACA_MOCK_*``, ``ALPACA_UPSTREAM_*``, etc.).

Run (Alpaca-backed replay; keys typically in ``.env``)::

    python mock_server.py --alpaca-date 2024-05-01 --alpaca-time 09:35 --access-log
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import threading
import uuid
from collections import defaultdict
from datetime import date, datetime, time as time_of_day, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Iterable
from urllib.parse import parse_qs, urlparse
from zoneinfo import ZoneInfo

from historical_proxy import (
    flatten_passthrough_params,
    proxy_quotes_latest,
    proxy_stock_bars,
    replay_session_minutes,
    snap_datetime_to_target_et_date,
    upstream_get_json,
)
from mock_env import (
    env_bool,
    env_int,
    env_str,
    load_dotenv,
    preparse_env_file_arg,
)

log = logging.getLogger("alpaca_mock")
_NY = ZoneInfo("America/New_York")

# Paths reachable from stocktrader main.py (see module docstring).
_MAIN_TRADING_GET_PATHS = frozenset(
    {"/v2/clock", "/v2/account", "/v2/positions", "/v2/orders", "/v2/assets"}
)
_MAIN_TRADING_ORDER_UUID = re.compile(r"^/v2/orders/([0-9a-f-]{36})$", re.I)
_MAIN_DATA_PATHS = frozenset({"/v2/stocks/bars", "/v2/stocks/quotes/latest"})
_TERMINAL_ORDER_STATUSES = frozenset({"filled", "canceled", "expired", "rejected", "done_for_day"})
_PASSTHROUGH_HEADER = "X-Alpaca-Mock-Replay"
# Cap chart-series when using auto-detected symbols (many series + large bar payloads).
_MAX_CHART_TRACKED_SYMBOLS = 100
_MAX_TRADE_EVENTS = 5000


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _parse_iso(s: str | None) -> datetime | None:
    if not s:
        return None
    s = s.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _parse_hhmm(value: str | None) -> time_of_day | None:
    raw = (value or "").strip()
    if not raw:
        return None
    try:
        parts = raw.split(":")
        if len(parts) != 2:
            raise ValueError
        hour = int(parts[0])
        minute = int(parts[1])
        return time_of_day(hour, minute)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be HH:MM in 24-hour New York time") from exc


def _decimal_value(value: Any, default: str = "0") -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal(default)


def _money(value: Decimal) -> str:
    return f"{value.quantize(Decimal('0.01'))}"


def _decimal_str(value: Decimal) -> str:
    return format(value.normalize(), "f")


def _order_matches_status_filter(order_status: str, status_filter: str) -> bool:
    status = order_status.lower()
    requested = status_filter.lower()
    if requested == "all":
        return True
    if requested == "closed":
        return status in _TERMINAL_ORDER_STATUSES
    if requested == "open":
        return status not in _TERMINAL_ORDER_STATUSES
    return status == requested


_MOCK_ASSET_SYMBOLS_CACHE: list[str] | None = None


def _flat_qs(qs: dict[str, list[str]]) -> dict[str, str]:
    out: dict[str, str] = {}
    for key, vals in qs.items():
        if not vals:
            continue
        first = (vals[0] or "").strip()
        if first:
            out[key] = first
    return out


def _mock_builtin_asset_symbols() -> list[str]:
    global _MOCK_ASSET_SYMBOLS_CACHE
    if _MOCK_ASSET_SYMBOLS_CACHE is None:
        path = Path(__file__).resolve().parent / "mock_asset_universe.txt"
        if path.is_file():
            lines: list[str] = []
            for line in path.read_text(encoding="utf-8").splitlines():
                sym = line.strip().upper()
                if sym and not sym.startswith("#"):
                    lines.append(sym)
            _MOCK_ASSET_SYMBOLS_CACHE = list(dict.fromkeys(lines))
        else:
            _MOCK_ASSET_SYMBOLS_CACHE = [
                "AAPL",
                "MSFT",
                "GOOGL",
                "AMZN",
                "META",
                "NVDA",
                "TSLA",
                "BRK.B",
                "UNH",
                "JNJ",
            ]
    return _MOCK_ASSET_SYMBOLS_CACHE


def _mock_asset_payload_dict(symbol: str, exchange: str) -> dict[str, Any]:
    sid = str(uuid.uuid5(uuid.NAMESPACE_URL, f"mock-asset:{symbol.upper()}"))
    return {
        "id": sid,
        "class": "us_equity",
        "exchange": exchange,
        "symbol": symbol.upper(),
        "name": f"{symbol.upper()} (mock asset list)",
        "status": "active",
        "tradable": True,
        "marginable": True,
        "shortable": True,
        "easy_to_borrow": True,
        "fractionable": True,
    }


def _synthetic_active_us_equity_assets(flat_qs: dict[str, str]) -> list[dict[str, Any]]:
    status = (flat_qs.get("status") or "active").lower()
    asset_class = (flat_qs.get("asset_class") or "us_equity").lower()
    ex_filter = (flat_qs.get("exchange") or "").strip().upper() or None
    if status != "active" or asset_class != "us_equity":
        return []
    cycle = ("NASDAQ", "NYSE", "ARCA")
    out: list[dict[str, Any]] = []
    for i, sym in enumerate(_mock_builtin_asset_symbols()):
        ex = cycle[i % len(cycle)]
        if ex_filter and ex != ex_filter:
            continue
        out.append(_mock_asset_payload_dict(sym, ex))
    return out


def _trading_get_assets(state: MockState, qs: dict[str, list[str]]) -> tuple[int, Any]:
    flat = _flat_qs(qs)
    if state.upstream_api_key and state.upstream_secret_key:
        status, body, err = upstream_get_json(
            state.upstream_trading_url,
            "/v2/assets",
            flat,
            state.upstream_api_key,
            state.upstream_secret_key,
        )
        if status == 200 and isinstance(body, list):
            return 200, body
        if status == 200:
            return 200, body if body is not None else []
        return status, body if body is not None else {"message": err or "upstream assets error"}
    return 200, _synthetic_active_us_equity_assets(flat)


def _wants_passthrough(headers: Any) -> bool:
    return str(headers.get(_PASSTHROUGH_HEADER, "")).strip().lower() == "passthrough"


class MockState:
    def __init__(
        self,
        starting_cash: float,
        market_open: bool,
        *,
        alpaca_historical_et_date: date | None = None,
        alpaca_historical_et_time: time_of_day | None = None,
        upstream_data_url: str = "https://data.alpaca.markets",
        upstream_trading_url: str = "https://paper-api.alpaca.markets",
        upstream_api_key: str | None = None,
        upstream_secret_key: str | None = None,
    ) -> None:
        self.starting_cash = starting_cash
        self.cash = _decimal_value(starting_cash)
        self.market_open = market_open
        self.orders: dict[str, dict[str, Any]] = {}
        self.positions: dict[str, dict[str, Decimal]] = {}
        self.account_lock = threading.RLock()
        self.mock_prices: dict[str, float] = defaultdict(lambda: 100.0)
        self.latest_market_prices: dict[str, Decimal] = {}
        self.latest_market_price_times: dict[str, str] = {}
        self.sim_session_minutes: float = 0.0
        self.quote_tick_index: int = 0
        self.quote_lock = threading.Lock()
        self.quote_last_emit_utc: datetime | None = None
        self.alpaca_historical_et_date = alpaca_historical_et_date
        self.alpaca_historical_et_time = alpaca_historical_et_time
        self.replay_wall_started_utc = _utc_now()
        self.replay_started_utc = self._initial_replay_utc()
        self.upstream_data_url = (upstream_data_url or "https://data.alpaca.markets").rstrip("/")
        self.upstream_trading_url = (upstream_trading_url or "https://paper-api.alpaca.markets").rstrip("/")
        self.upstream_api_key = upstream_api_key
        self.upstream_secret_key = upstream_secret_key
        self._tracked_symbols_lock = threading.Lock()
        self.tracked_symbols: set[str] = set()
        self._trade_events_lock = threading.Lock()
        self.trade_events: list[dict[str, Any]] = []

    def record_tracked_symbols(self, symbols: Iterable[str]) -> None:
        """Remember tickers seen on data or trading routes (for /chart auto-symbols)."""
        syms = [str(s).strip().upper() for s in symbols if s is not None and str(s).strip()]
        if not syms:
            return
        with self._tracked_symbols_lock:
            self.tracked_symbols.update(syms)

    def _record_trade_fill(self, order: dict[str, Any], sym: str, side: str, price: float) -> None:
        sd = side if side in ("buy", "sell") else "buy"
        t_ev = str(order.get("filled_at") or order.get("updated_at") or _iso(_utc_now()))
        ev: dict[str, Any] = {
            "t": t_ev,
            "symbol": sym.upper(),
            "side": sd,
            "price": price,
            "order_id": str(order.get("id") or ""),
        }
        with self._trade_events_lock:
            self.trade_events.append(ev)
            if len(self.trade_events) > _MAX_TRADE_EVENTS:
                self.trade_events = self.trade_events[-_MAX_TRADE_EVENTS:]

    def _initial_replay_utc(self) -> datetime | None:
        if self.alpaca_historical_et_date is None:
            return None
        if self.alpaca_historical_et_time is not None:
            return datetime.combine(
                self.alpaca_historical_et_date,
                self.alpaca_historical_et_time,
                tzinfo=_NY,
            ).astimezone(timezone.utc)
        return snap_datetime_to_target_et_date(self.replay_wall_started_utc, self.alpaca_historical_et_date)

    def refresh_replay_clock_from_wall(self) -> None:
        if self.alpaca_historical_et_date is None:
            return
        self.sim_session_minutes = replay_session_minutes(self.alpaca_historical_et_date, self.replay_now_utc())

    def replay_now_utc(self) -> datetime:
        if self.alpaca_historical_et_date is None:
            return _utc_now()
        if self.replay_started_utc is None:
            return snap_datetime_to_target_et_date(_utc_now(), self.alpaca_historical_et_date)
        return self.replay_started_utc + (_utc_now() - self.replay_wall_started_utc)

    def replay_market_is_open(self) -> bool:
        if not self.market_open:
            return False
        if self.alpaca_historical_et_date is None:
            return True
        # Alpaca has no regular US equity session on weekends. Exchange holidays
        # are still delegated to upstream data availability.
        replay_et = self.replay_now_utc().astimezone(_NY)
        return replay_et.weekday() < 5 and time_of_day(9, 30) <= replay_et.time() < time_of_day(16, 0)

    def next_quote(self, symbol: str) -> tuple[str, float, float]:
        """Synthetic quote: monotonic UTC ``t``, mid, half-spread width (bps-sized)."""
        sym = symbol.upper()
        with self.quote_lock:
            self.quote_tick_index += 1
            tick = self.quote_tick_index
            candidate = _utc_now()
            if self.quote_last_emit_utc is not None:
                candidate = max(candidate, self.quote_last_emit_utc + timedelta(microseconds=1))
            dt = candidate
            self.quote_last_emit_utc = dt

        base_price = self.mid_price(sym, dt)
        tick_phase = (tick % 1000) / 1000.0
        if tick_phase < 0.1:
            drift = 0.0015
            noise = (tick % 2) * 0.0002
        else:
            drift = 0.0
            noise = ((tick % 3) - 1) * 0.0005
        price = base_price * (1.0 + drift + noise)
        spread_bps = 0.0003 + (tick % 8) * 0.0001
        spread = max(0.01, price * spread_bps)
        return _iso(dt), float(price), float(spread)

    def remember_market_price(self, symbol: str, price: Any, timestamp: Any = None) -> None:
        sym = str(symbol or "").upper()
        px = _decimal_value(price, "0")
        if not sym or px <= 0:
            return
        with self.account_lock:
            self.latest_market_prices[sym] = px
            if timestamp:
                self.latest_market_price_times[sym] = str(timestamp)

    def has_market_price(self, symbol: str) -> bool:
        sym = str(symbol or "").upper()
        with self.account_lock:
            px = self.latest_market_prices.get(sym)
        return px is not None and px > 0

    def remember_market_data(self, body: Any) -> None:
        if not isinstance(body, dict):
            return
        quotes = body.get("quotes")
        if isinstance(quotes, dict):
            for sym, row in quotes.items():
                if not isinstance(row, dict):
                    continue
                bid = _decimal_value(row.get("bp"), "0")
                ask = _decimal_value(row.get("ap"), "0")
                if bid > 0 and ask > 0:
                    self.remember_market_price(sym, (bid + ask) / Decimal("2"), row.get("t"))
                elif ask > 0:
                    self.remember_market_price(sym, ask, row.get("t"))
                elif bid > 0:
                    self.remember_market_price(sym, bid, row.get("t"))
        bars = body.get("bars")
        if isinstance(bars, dict):
            for sym, rows in bars.items():
                if not isinstance(rows, list):
                    continue
                latest: dict[str, Any] | None = None
                latest_dt: datetime | None = None
                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    row_dt = _parse_iso(str(row.get("t") or ""))
                    if latest is None or (row_dt is not None and (latest_dt is None or row_dt >= latest_dt)):
                        latest = row
                        latest_dt = row_dt
                if latest:
                    self.remember_market_price(sym, latest.get("c"), latest.get("t"))

    def mid_price(self, symbol: str, _at: datetime) -> float:
        sym = symbol.upper()
        with self.account_lock:
            px = self.latest_market_prices.get(sym)
        if px is not None and px > 0:
            return float(px)
        return float(self.mock_prices[sym])

    def account_payload(self) -> dict[str, Any]:
        with self.account_lock:
            cash = self.cash
            market_value = Decimal("0")
            for sym, pos in self.positions.items():
                qty = pos["qty"]
                px = _decimal_value(self.mid_price(sym, _utc_now()))
                market_value += qty * px
            equity = cash + market_value
            buying_power = cash
        c = _money(cash)
        e = _money(equity)
        bp = _money(buying_power)
        return {
            "id": str(uuid.uuid4()),
            "account_number": "MOCK-0001",
            "status": "ACTIVE",
            "currency": "USD",
            "cash": c,
            "buying_power": bp,
            "portfolio_value": e,
            "equity": e,
            "pattern_day_trader": False,
            "trading_blocked": False,
            "transfers_blocked": False,
            "account_blocked": False,
            "multiplier": "1",
            "shorting_enabled": True,
        }

    def position_payloads(self) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        with self.account_lock:
            snapshot = [(sym, pos["qty"], pos["avg_entry_price"]) for sym, pos in self.positions.items()]
        for sym, qty, avg_entry in snapshot:
            if qty == 0:
                continue
            current = _decimal_value(self.mid_price(sym, _utc_now()))
            market_value = qty * current
            cost_basis = qty * avg_entry
            unrealized = market_value - cost_basis
            out.append(
                {
                    "asset_id": str(uuid.uuid5(uuid.NAMESPACE_DNS, sym)),
                    "symbol": sym,
                    "exchange": "NASDAQ",
                    "asset_class": "us_equity",
                    "asset_marginable": True,
                    "qty": _decimal_str(qty),
                    "avg_entry_price": _decimal_str(avg_entry),
                    "side": "long" if qty >= 0 else "short",
                    "market_value": _money(market_value),
                    "cost_basis": _money(cost_basis),
                    "unrealized_pl": _money(unrealized),
                    "unrealized_plpc": _decimal_str(unrealized / cost_basis) if cost_basis else "0",
                    "current_price": _decimal_str(current),
                    "lastday_price": _decimal_str(current),
                    "change_today": "0",
                }
            )
        return out

    def apply_fill(self, order: dict[str, Any]) -> None:
        sym = str(order.get("symbol") or "").upper()
        if not sym:
            return
        qty = _decimal_value(order.get("filled_qty") or order.get("qty"), "0")
        px = _decimal_value(order.get("filled_avg_price"), "0")
        if qty <= 0 or px <= 0:
            return
        side = str(order.get("side") or "buy").lower()
        if side not in ("buy", "sell"):
            side = "buy"
        signed_qty = qty if side == "buy" else -qty
        self._record_trade_fill(order, sym, side, float(px))
        with self.account_lock:
            self.cash -= signed_qty * px
            pos = self.positions.get(sym)
            if pos is None:
                self.positions[sym] = {"qty": signed_qty, "avg_entry_price": px}
                return
            old_qty = pos["qty"]
            old_avg = pos["avg_entry_price"]
            new_qty = old_qty + signed_qty
            if new_qty == 0:
                self.positions.pop(sym, None)
                return
            if old_qty == 0 or (old_qty > 0 and signed_qty > 0) or (old_qty < 0 and signed_qty < 0):
                pos["avg_entry_price"] = ((old_qty * old_avg) + (signed_qty * px)) / new_qty
            pos["qty"] = new_qty


def _timeframe_seconds(timeframe: str) -> int | None:
    """Bar step in seconds (Alpaca-style: 1Min, 5Min, 1Sec, …). Unknown → 60."""
    m = re.match(r"^(\d+)(Sec|Min|Hour|Day|Week|Month)$", timeframe or "", re.I)
    if not m:
        return 60
    n, unit = int(m.group(1)), m.group(2).title()
    mult = {"Sec": 1, "Min": 60, "Hour": 3600, "Day": 86400, "Week": 604800, "Month": 2592000}
    return n * mult[unit]


def _synthetic_bars(
    symbols: list[str],
    timeframe: str,
    start: datetime | None,
    end: datetime | None,
    limit: int | None,
    state: MockState,
) -> dict[str, list[dict[str, Any]]]:
    step = _timeframe_seconds(timeframe) or 60
    end_dt = end or _utc_now()
    max_points = min(limit or 500, 500) if limit else 1000
    if start:
        start_dt = start
    elif limit:
        n = max(1, min(limit, 500))
        start_dt = end_dt - timedelta(seconds=step * (n - 1))
    else:
        start_dt = end_dt - timedelta(seconds=step * 30)

    out: dict[str, list[dict[str, Any]]] = {s: [] for s in symbols}
    for sym in symbols:
        t = start_dt
        i = 0
        while t <= end_dt and i < max_points:
            t_next = t + timedelta(seconds=step)
            o = state.mid_price(sym, t)
            c = state.mid_price(sym, min(t_next, end_dt))
            hi = max(o, c) * 1.001
            lo = min(o, c) * 0.999
            vol = 800.0 + 40.0 * abs(c - o) * 1000.0
            if i > 0 and abs(c - o) > 0.03:
                vol += 5000.0
            out[sym].append(
                {
                    "t": _iso(t),
                    "o": o,
                    "h": hi,
                    "l": lo,
                    "c": c,
                    "v": vol,
                    "n": 50.0,
                    "vw": (o + c) / 2,
                }
            )
            t = t_next
            i += 1
        if limit and len(out[sym]) > limit:
            out[sym] = out[sym][-limit:]
    return out


def _split_symbol_csv(raw: str | None) -> list[str]:
    if not raw:
        return []
    return [s.strip().upper() for s in str(raw).split(",") if s.strip()]


def _chart_symbols_from_qs(qs: dict[str, list[str]]) -> list[str]:
    return _split_symbol_csv((qs.get("symbols") or [""])[0])


def _chart_minutes_timeframe(qs: dict[str, list[str]]) -> tuple[int, str]:
    minutes_raw = (qs.get("minutes") or ["120"])[0]
    try:
        minutes = int(minutes_raw)
    except (TypeError, ValueError):
        minutes = 120
    minutes = max(5, min(minutes, 24 * 60))
    timeframe = (qs.get("timeframe") or ["1Min"])[0] or "1Min"
    return minutes, timeframe


def _chart_resolve_symbol_list(state: MockState, qs: dict[str, list[str]]) -> tuple[list[str], str, dict[str, Any]]:
    """Pick chart tickers: explicit ``symbols=`` query, else tickers seen from stocktrader, else defaults."""
    extra: dict[str, Any] = {}
    explicit = _chart_symbols_from_qs(qs)
    if explicit:
        return explicit, "query", extra
    with state._tracked_symbols_lock:
        tracked_all = sorted(state.tracked_symbols)
    if tracked_all:
        extra["tracked_symbol_count"] = len(tracked_all)
        if len(tracked_all) > _MAX_CHART_TRACKED_SYMBOLS:
            extra["chart_symbols_capped"] = True
            return tracked_all[:_MAX_CHART_TRACKED_SYMBOLS], "tracked", extra
        return tracked_all, "tracked", extra
    return ["AAPL", "MSFT"], "default", extra


def _trade_events_for_chart_window(
    state: MockState, symbols: list[str], start: datetime, end: datetime
) -> list[dict[str, Any]]:
    """Fills in ``[start, end]`` for symbols on the chart (replay or wall-clock timestamps)."""
    symset = {s.upper() for s in symbols}
    with state._trade_events_lock:
        snap = list(state.trade_events)
    out: list[dict[str, Any]] = []
    for ev in snap:
        sym = str(ev.get("symbol") or "").upper()
        if sym not in symset:
            continue
        dt = _parse_iso(str(ev.get("t") or ""))
        if dt is None:
            continue
        if dt < start or dt > end:
            continue
        sd = str(ev.get("side") or "buy").lower()
        if sd not in ("buy", "sell"):
            sd = "buy"
        try:
            price = float(ev.get("price"))
        except (TypeError, ValueError):
            continue
        out.append(
            {
                "t": str(ev.get("t") or ""),
                "symbol": sym,
                "side": sd,
                "price": price,
                "order_id": str(ev.get("order_id") or ""),
            }
        )
    out.sort(key=lambda e: e["t"])
    return out


def _mock_chart_series(state: MockState, qs: dict[str, list[str]]) -> tuple[int, dict[str, Any]]:
    """Bars for the chart UI: same upstream/synthetic rules as ``GET /v2/stocks/bars``."""
    minutes, timeframe = _chart_minutes_timeframe(qs)
    symbols, symbol_source, symbol_extra = _chart_resolve_symbol_list(state, qs)
    with state._tracked_symbols_lock:
        strip_full = sorted(state.tracked_symbols)
    if not strip_full:
        strip_full = ["AAPL", "MSFT"]
    chart_strip = strip_full[:_MAX_CHART_TRACKED_SYMBOLS]
    strip_meta: dict[str, Any] = {"chart_symbol_strip": chart_strip}
    if len(strip_full) > _MAX_CHART_TRACKED_SYMBOLS:
        strip_meta["chart_symbol_strip_total"] = len(strip_full)
    state.refresh_replay_clock_from_wall()
    end = state.replay_now_utc()
    start = end - timedelta(minutes=minutes)
    bar_qs: dict[str, list[str]] = {
        "symbols": [",".join(symbols)],
        "timeframe": [timeframe],
        "feed": ["iex"],
        "start": [_iso(start)],
        "end": [_iso(end)],
        "limit": ["2000"],
    }
    data_mode = "alpaca_replay" if state.alpaca_historical_et_date else "local_synthetic"
    meta: dict[str, Any] = {
        "replay_now_utc": _iso(end),
        "data_mode": data_mode,
        "minutes": minutes,
        "timeframe": timeframe,
        "symbols": symbols,
        "symbol_source": symbol_source,
        "trade_events": _trade_events_for_chart_window(state, symbols, start, end),
        **strip_meta,
        **symbol_extra,
    }
    if (
        state.alpaca_historical_et_date is not None
        and state.upstream_api_key
        and state.upstream_secret_key
    ):
        code, body = proxy_stock_bars(
            bar_qs,
            state.alpaca_historical_et_date,
            state.upstream_data_url,
            state.upstream_api_key,
            state.upstream_secret_key,
            state.replay_now_utc(),
        )
        if isinstance(body, dict):
            out = dict(meta)
            out.update(body)
            if code == 200:
                state.remember_market_data(body)
            return code, out
        return code, {**meta, "message": str(body)}
    bars = _synthetic_bars(symbols, timeframe, start, end, 2000, state)
    return 200, {**meta, "bars": bars, "next_page_token": None}


_CHART_PAGE_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Alpaca mock — live bars</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<style>
  body { font-family: system-ui, sans-serif; margin: 16px; background: #111; color: #e8e8e8; }
  #meta { font-size: 13px; opacity: 0.85; margin-bottom: 12px; }
  #err { color: #ff6b6b; margin-bottom: 8px; min-height: 1.2em; }
  #symbol-strip { display: flex; flex-wrap: wrap; gap: 8px; margin-bottom: 14px; align-items: center; }
  #symbol-strip button { cursor: pointer; padding: 6px 12px; border-radius: 8px; border: 1px solid #444;
    background: #222; color: #eee; font-size: 13px; }
  #symbol-strip button.on { background: #2a4a6a; border-color: #5ac8fa; }
  #symbol-strip .hint { font-size: 12px; opacity: 0.75; margin-left: 4px; }
  canvas { max-height: 68vh; }
  a { color: #7ecbff; }
</style>
</head>
<body>
  <h1>Mock data — bar chart</h1>
  <p>Tickers come from what the mock has seen (bars, quotes, orders). Use the chips to switch the chart to one symbol at a time.
    Green ▲ = buy fill, red ▼ = sell fill (snapped to nearest bar). Optional URL: <code>?symbols=AAPL,MSFT</code> (first is shown until you pick another chip),
    <code>minutes</code>, <code>timeframe</code>, <code>poll</code> (default 5s, min 5s, max 120s).</p>
  <div id="symbol-strip"></div>
  <div id="meta"></div>
  <div id="err"></div>
  <canvas id="c"></canvas>
  <p><a href="/v1/mock/status">/v1/mock/status</a></p>
<script>
(function () {
  const params = new URLSearchParams(location.search);
  const rawSym = params.get("symbols");
  const symbolsOverride = rawSym
    ? rawSym.split(",").map(function (s) { return s.trim().toUpperCase(); }).filter(Boolean)
    : [];
  const minutes = params.get("minutes") || "120";
  const timeframe = params.get("timeframe") || "1Min";
  let pollMs = parseInt(params.get("poll") || "5000", 10);
  if (!isFinite(pollMs)) pollMs = 5000;
  pollMs = Math.max(5000, Math.min(pollMs, 120000));

  const urlLocked = symbolsOverride.length > 0;
  let activeSymbol = null;
  const stripEl = document.getElementById("symbol-strip");

  function getStripList(j) {
    if (urlLocked) return symbolsOverride.slice();
    const a = j.chart_symbol_strip;
    if (a && a.length) return a.slice();
    return (j.symbols || []).slice();
  }

  function parseIsoMs(s) {
    const ms = Date.parse(s);
    return isNaN(ms) ? null : ms;
  }

  function formatEt(isoUtc) {
    if (!isoUtc) return "";
    try {
      const d = new Date(isoUtc);
      if (isNaN(d.getTime())) return String(isoUtc);
      return new Intl.DateTimeFormat("en-US", {
        timeZone: "America/New_York",
        month: "short",
        day: "numeric",
        hour: "numeric",
        minute: "2-digit",
        second: "2-digit",
        hour12: true,
        timeZoneName: "short"
      }).format(d);
    } catch (e) {
      return String(isoUtc);
    }
  }

  function closestLabelIndex(eventT, utcTimes) {
    const et = parseIsoMs(eventT);
    if (et === null || !utcTimes.length) return 0;
    let best = 0;
    let bestDiff = Infinity;
    for (let i = 0; i < utcTimes.length; i++) {
      const bt = parseIsoMs(utcTimes[i]);
      if (bt === null) continue;
      const d = Math.abs(bt - et);
      if (d < bestDiff) { bestDiff = d; best = i; }
    }
    return best;
  }

  function buildChartData(barsBySym, symList, tradeEvents) {
    const syms = (symList && symList.length) ? symList : Object.keys(barsBySym || {}).sort();
    const timeSet = new Set();
    syms.forEach(function (sym) {
      (barsBySym[sym] || []).forEach(function (row) { timeSet.add(row.t); });
    });
    const utcTimes = Array.from(timeSet).sort();
    const labels = utcTimes.map(formatEt);
    const colors = ["#5ac8fa", "#ff9500", "#34c759", "#ff375f", "#bf5af2", "#ffd60a"];
    const evs = tradeEvents || [];
    const datasets = [];
    syms.forEach(function (sym, i) {
      const rows = barsBySym[sym] || [];
      const m = {};
      rows.forEach(function (r) { m[r.t] = r.c; });
      const data = utcTimes.map(function (t) { return m[t] !== undefined ? m[t] : null; });
      const n = utcTimes.length;
      const buy = new Array(n).fill(false);
      const sell = new Array(n).fill(false);
      evs.forEach(function (ev) {
        if ((ev.symbol || "").toUpperCase() !== sym) return;
        const idx = closestLabelIndex(ev.t, utcTimes);
        if ((ev.side || "").toLowerCase() === "sell") sell[idx] = true;
        else buy[idx] = true;
      });
      const pointRadius = utcTimes.map(function (_, j) {
        return (buy[j] || sell[j]) ? 9 : 0;
      });
      const pointBackgroundColor = utcTimes.map(function (_, j) {
        if (buy[j] && sell[j]) return "#ffd60a";
        if (buy[j]) return "#34c759";
        if (sell[j]) return "#ff375f";
        return colors[i % colors.length];
      });
      const pointStyle = utcTimes.map(function (_, j) {
        if (buy[j] && sell[j]) return "star";
        if (sell[j]) return "triangle";
        return "triangle";
      });
      const pointRotation = utcTimes.map(function (_, j) {
        if (sell[j] && !buy[j]) return 180;
        return 0;
      });
      datasets.push({
        label: sym,
        data: data,
        borderColor: colors[i % colors.length],
        backgroundColor: "transparent",
        spanGaps: true,
        tension: 0.12,
        pointRadius: pointRadius,
        pointHoverRadius: 11,
        pointBorderWidth: 1,
        pointBorderColor: "#111",
        pointStyle: pointStyle,
        pointRotation: pointRotation,
        pointBackgroundColor: pointBackgroundColor,
        borderWidth: 1.5
      });
    });
    return { labels: labels, datasets: datasets, utcTimes: utcTimes };
  }

  function renderSymbolStrip(strip, j) {
    stripEl.innerHTML = "";
    if (!strip.length) {
      const sp = document.createElement("span");
      sp.className = "hint";
      sp.textContent = "No symbols yet — run stocktrader against the mock or open with ?symbols=TICKER";
      stripEl.appendChild(sp);
      return;
    }
    strip.forEach(function (s) {
      const b = document.createElement("button");
      b.type = "button";
      b.textContent = s;
      if (activeSymbol === s) b.classList.add("on");
      b.onclick = function () {
        activeSymbol = s;
        tick();
      };
      stripEl.appendChild(b);
    });
    if (urlLocked) {
      const h = document.createElement("span");
      h.className = "hint";
      h.textContent = "(URL fixed symbol list; chip picks which one to chart)";
      stripEl.appendChild(h);
    }
    const tot = j && j.chart_symbol_strip_total;
    if (tot && tot > strip.length) {
      const sp = document.createElement("span");
      sp.className = "hint";
      sp.textContent = "showing " + strip.length + " of " + tot + " tracked";
      stripEl.appendChild(sp);
    }
  }

  const ctx = document.getElementById("c").getContext("2d");
  let chart = new Chart(ctx, {
    type: "line",
    data: { labels: [], datasets: [] },
    options: {
      responsive: true,
      maintainAspectRatio: true,
      animation: false,
      interaction: { mode: "index", intersect: false },
      plugins: {
        legend: { labels: { color: "#ccc" } },
        tooltip: {
          callbacks: {
            afterLabel: function (ctx) {
              const ch = ctx.chart;
              const ds = ctx.dataset;
              const i = ctx.dataIndex;
              if (!ds.pointRadius || typeof ds.pointRadius === "number") return "";
              const r = ds.pointRadius[i];
              if (!r) return "";
              const utcBars = ch._barUtcTimes || [];
              const sym = ds.label || "";
              const hits = (jLast && jLast.trade_events) ? jLast.trade_events.filter(function (ev) {
                return (ev.symbol || "").toUpperCase() === sym.toUpperCase() &&
                  closestLabelIndex(ev.t, utcBars) === i;
              }) : [];
              if (!hits.length) return "";
              return hits.map(function (h) {
                return (h.side || "") + " " + (h.price != null ? h.price : "") + " @ " + formatEt(h.t || "");
              }).join("\\n");
            }
          }
        }
      },
      scales: {
        x: {
          ticks: { color: "#aaa", maxRotation: 45, minRotation: 0, autoSkip: true, maxTicksLimit: 24 },
          grid: { color: "rgba(255,255,255,0.06)" },
          title: { display: true, text: "Bar time (US/Eastern)", color: "#888" }
        },
        y: {
          ticks: { color: "#aaa" },
          grid: { color: "rgba(255,255,255,0.06)" },
          title: { display: true, text: "Close", color: "#888" }
        }
      }
    }
  });

  let jLast = null;

  async function tick() {
    const u = new URL("/v1/mock/chart-series", location.origin);
    if (urlLocked) {
      if (!activeSymbol && symbolsOverride.length) activeSymbol = symbolsOverride[0];
      if (activeSymbol) u.searchParams.set("symbols", activeSymbol);
    } else if (activeSymbol) {
      u.searchParams.set("symbols", activeSymbol);
    }
    u.searchParams.set("minutes", minutes);
    u.searchParams.set("timeframe", timeframe);
    const errEl = document.getElementById("err");
    try {
      const r = await fetch(u.toString(), { cache: "no-store" });
      const j = await r.json();
      if (!r.ok) {
        errEl.textContent = (j && j.message) ? j.message : ("HTTP " + r.status);
        return;
      }
      const strip = getStripList(j);
      if (!activeSymbol && strip.length) {
        activeSymbol = strip[0];
        return tick();
      }
      if (activeSymbol && strip.length && strip.indexOf(activeSymbol) === -1) {
        activeSymbol = strip[0];
        return tick();
      }
      jLast = j;
      errEl.textContent = "";
      const meta = document.getElementById("meta");
      const symList = j.symbols || Object.keys(j.bars || {}).sort();
      let capNote = "";
      if (j.chart_symbols_capped) capNote = " (capped, tracked=" + (j.tracked_symbol_count || "") + ")";
      const nTr = (j.trade_events && j.trade_events.length) ? j.trade_events.length : 0;
      meta.textContent =
        (j.data_mode || "") + "  source=" + (j.symbol_source || "") + capNote +
        "  replay_now (US/Eastern)=" + formatEt(j.replay_now_utc || "") +
        "  showing=" + (activeSymbol || "") +
        "  series=" + symList.length + "  fills_in_window=" + nTr +
        "  bars: " + symList.map(function (s) {
          var a = (j.bars && j.bars[s]) ? j.bars[s].length : 0;
          return s + "=" + a;
        }).join(" ");
      renderSymbolStrip(strip, j);
      const bars = j.bars || {};
      const cd = buildChartData(bars, symList, j.trade_events || []);
      chart.data.labels = cd.labels;
      chart.data.datasets = cd.datasets;
      chart._barUtcTimes = cd.utcTimes;
      chart.update("none");
    } catch (e) {
      errEl.textContent = String(e);
    }
  }

  tick();
  setInterval(tick, pollMs);
})();
</script>
</body>
</html>
"""


def _order_payload(
    order_id: str,
    body: dict[str, Any],
    status: str,
    filled_qty: str | None,
    filled_avg: str | None,
    now: datetime,
) -> dict[str, Any]:
    sym = (body.get("symbol") or "").upper()
    side = (body.get("side") or "buy").lower()
    qty = str(body.get("qty") or body.get("quantity") or "1")
    client_oid = body.get("client_order_id") or f"mock-{order_id[:8]}"
    ts = _iso(now)
    asset_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, sym or "UNKNOWN"))
    payload: dict[str, Any] = {
        "id": order_id,
        "client_order_id": client_oid,
        "created_at": ts,
        "updated_at": ts,
        "submitted_at": ts,
        "asset_id": asset_id,
        "symbol": sym,
        "asset_class": "us_equity",
        "qty": qty,
        "filled_qty": filled_qty if filled_qty is not None else "0",
        "filled_avg_price": filled_avg,
        "type": "market",
        "side": side,
        "time_in_force": (body.get("time_in_force") or "day").lower(),
        "status": status,
        "order_class": "simple",
        "extended_hours": bool(body.get("extended_hours", False)),
    }
    if status == "filled":
        payload["filled_at"] = ts
        payload["filled_qty"] = filled_qty or qty
        payload["filled_avg_price"] = filled_avg or "100.0"
    return payload


def _setup_access_logging() -> None:
    if log.handlers:
        return
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s [alpaca_mock] %(message)s", datefmt="%H:%M:%S"))
    log.addHandler(h)
    log.setLevel(logging.INFO)


def _access_line(server: str, command: str, raw_path: str, code: int, detail: str) -> None:
    parsed = urlparse(raw_path)
    q = f"?{parsed.query}" if parsed.query else ""
    log.info("%s | %s %s%s -> %d | %s", server, command, parsed.path, q, code, detail)


def _summary_trading(path: str, code: int, body: Any, *, empty: bool = False) -> str:
    if empty or code == 204:
        return "no body"
    if code >= 400:
        if isinstance(body, dict):
            return (body.get("message") or str(body))[:220]
        return str(body)[:220]
    if path == "/v2/clock" and isinstance(body, dict):
        return f"is_open={body.get('is_open')} timestamp={body.get('timestamp')}"
    if path == "/v2/account" and isinstance(body, dict):
        return f"cash={body.get('cash')} buying_power={body.get('buying_power')} equity={body.get('equity')}"
    if path == "/v2/positions" and isinstance(body, list):
        return f"positions={len(body)}"
    if path == "/v2/orders" and isinstance(body, list):
        return f"open_orders={len(body)}"
    if path == "/v2/assets" and isinstance(body, list):
        return f"assets={len(body)}"
    if path.startswith("/v2/orders/") and isinstance(body, dict):
        oid = str(body.get("id", ""))[:8]
        return (
            f"order id={oid}… symbol={body.get('symbol')} status={body.get('status')} "
            f"filled_qty={body.get('filled_qty')} filled_avg={body.get('filled_avg_price')}"
        )
    if path == "/v2/orders" and isinstance(body, dict) and body.get("id"):
        return (
            f"order_accept symbol={body.get('symbol')} side={body.get('side')} status={body.get('status')} "
            f"qty={body.get('qty')} filled_qty={body.get('filled_qty')} filled_avg={body.get('filled_avg_price')}"
        )
    if isinstance(body, dict):
        return f"keys={list(body.keys())[:8]}"
    if isinstance(body, list):
        return f"list n={len(body)}"
    return str(type(body).__name__)


def _summary_data(path: str, code: int, body: Any, state: MockState) -> str:
    if code >= 400:
        if isinstance(body, dict):
            return (body.get("message") or str(body))[:220]
        return str(body)[:220]
    if path == "/v1/mock/status" and isinstance(body, dict):
        return (
            f"sim_session_minutes={body.get('sim_session_minutes')} "
            f"synthetic_ts={body.get('synthetic_timestamp_utc')} mode={body.get('data_mode')} "
            f"tracked_n={body.get('tracked_symbol_count')}"
        )
    if path == "/chart":
        return "html chart"
    if path == "/v1/mock/chart-series" and isinstance(body, dict):
        bars = body.get("bars") or {}
        n = sum(len(v) for v in bars.values() if isinstance(v, list))
        te = body.get("trade_events") or []
        n_te = len(te) if isinstance(te, list) else 0
        return (
            f"src={body.get('symbol_source')} mode={body.get('data_mode')} replay={body.get('replay_now_utc')} "
            f"syms={body.get('symbols')} total_bars={n} trades={n_te}"
        )
    if path == "/v2/stocks/bars" and isinstance(body, dict):
        bars = body.get("bars") or {}
        parts: list[str] = []
        symbols = sorted(bars.keys())
        for sym in symbols[:8]:
            rows = bars[sym]
            if not rows:
                parts.append(f"{sym}=0bars")
                continue
            last = rows[-1]
            parts.append(
                f"{sym} n={len(rows)} last_t={last.get('t')} o={last.get('o')} c={last.get('c')} v={last.get('v'):.0f}"
                if last.get("v") is not None
                else f"{sym} n={len(rows)} last_t={last.get('t')} c={last.get('c')}"
            )
        if len(symbols) > 8:
            parts.append(f"... {len(symbols) - 8} more symbols")
        sm = f" sim_session_minutes_after_tick={state.sim_session_minutes:.4f}"
        return "; ".join(parts) + sm
    if path == "/v2/stocks/quotes/latest" and isinstance(body, dict):
        q = body.get("quotes") or {}
        parts = []
        symbols = sorted(q.keys())
        for sym in symbols[:12]:
            row = q[sym]
            mid = (float(row.get("bp", 0)) + float(row.get("ap", 0))) / 2.0
            parts.append(f"{sym} t={row.get('t')} mid≈{mid:.4f} bp={row.get('bp')} ap={row.get('ap')}")
        if len(symbols) > 12:
            parts.append(f"... {len(symbols) - 12} more symbols")
        return "; ".join(parts)
    if isinstance(body, dict):
        return f"keys={list(body.keys())[:10]}"
    return str(type(body).__name__)


class TradingHandler(BaseHTTPRequestHandler):
    state: MockState
    log_verbose: bool = False
    access_log: bool = False

    def log_message(self, fmt: str, *args: Any) -> None:
        if self.log_verbose:
            super().log_message(fmt, *args)

    def _send(self, code: int, body: Any | None, content_type: str = "application/json") -> None:
        data = b""
        if body is not None:
            data = json.dumps(body).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        if data:
            self.wfile.write(data)
        if self.access_log:
            _access_line("trading", self.command, self.path, code, _summary_trading(urlparse(self.path).path, code, body))

    def _send_empty(self, code: int) -> None:
        self.send_response(code)
        self.send_header("Content-Length", "0")
        self.end_headers()
        if self.access_log:
            _access_line("trading", self.command, self.path, code, _summary_trading(urlparse(self.path).path, code, None, empty=True))

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path
        qs = parse_qs(parsed.query)

        if path not in _MAIN_TRADING_GET_PATHS and not _MAIN_TRADING_ORDER_UUID.match(path):
            self._send(
                404,
                {
                    "message": (
                        f"unknown path {path}; this mock only implements main.py trading GET routes "
                        f"(clock, account, positions, orders, assets)"
                    )
                },
            )
            return

        if path == "/v2/assets":
            code, body = _trading_get_assets(self.state, qs)
            self._send(code, body)
            return

        if path == "/v2/clock":
            self.state.refresh_replay_clock_from_wall()
            if self.state.alpaca_historical_et_date is not None:
                now = self.state.replay_now_utc()
            else:
                now = _utc_now()
            self._send(
                200,
                {
                    "timestamp": _iso(now),
                    "is_open": self.state.replay_market_is_open(),
                    "next_open": _iso(now - timedelta(hours=1)),
                    "next_close": _iso(now + timedelta(hours=6)),
                },
            )
            return

        if path == "/v2/account":
            self._send(200, self.state.account_payload())
            return

        if path == "/v2/positions":
            with self.state.account_lock:
                self.state.record_tracked_symbols(list(self.state.positions.keys()))
            self._send(200, self.state.position_payloads())
            return

        if path == "/v2/orders":
            status_filter = ((qs.get("status") or ["open"])[0] or "open").lower()
            out = []
            order_syms: list[str] = []
            for o in self.state.orders.values():
                s = str(o.get("symbol") or "").strip().upper()
                if s:
                    order_syms.append(s)
                order_status = str(o.get("status") or "").lower()
                if _order_matches_status_filter(order_status, status_filter):
                    out.append(o)
            self.state.record_tracked_symbols(order_syms)
            self._send(200, out)
            return

        m = _MAIN_TRADING_ORDER_UUID.match(path)
        if m:
            oid = m.group(1).lower()
            o = self.state.orders.get(oid)
            if not o:
                self._send(404, {"code": 40410000, "message": "order not found"})
                return
            s = str(o.get("symbol") or "").strip().upper()
            if s:
                self.state.record_tracked_symbols([s])
            self._send(200, o)
            return

        raise AssertionError(f"unreachable routing for {path}")

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path != "/v2/orders":
            self._send(
                404,
                {
                    "message": (
                        f"unknown path {parsed.path}; this mock only implements "
                        "main.py POST /v2/orders (market orders)"
                    )
                },
            )
            return

        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b"{}"
        try:
            body = json.loads(raw.decode("utf-8") or "{}")
        except json.JSONDecodeError:
            self._send(400, {"code": 40010001, "message": "invalid json"})
            return

        now = self.state.replay_now_utc()
        oid = str(uuid.uuid4()).lower()
        sym = (body.get("symbol") or "").upper()
        if sym:
            self.state.record_tracked_symbols([sym])
        if (
            sym
            and not self.state.has_market_price(sym)
            and self.state.alpaca_historical_et_date is not None
            and self.state.upstream_api_key
            and self.state.upstream_secret_key
        ):
            code, quote_body = proxy_quotes_latest(
                {"symbols": [sym]},
                self.state.alpaca_historical_et_date,
                self.state.upstream_data_url,
                self.state.upstream_api_key,
                self.state.upstream_secret_key,
                now,
            )
            if code == 200:
                self.state.remember_market_data(quote_body)
        if self.state.alpaca_historical_et_date is not None and sym and not self.state.has_market_price(sym):
            self._send(
                422,
                {
                    "code": 42210000,
                    "message": f"no replay market price available for {sym}; refusing synthetic fill",
                },
            )
            return
        px = f"{self.state.mid_price(sym, now):.4f}"

        if self.state.replay_market_is_open() and (body.get("type") or "market").lower() == "market":
            st = "filled"
            o = _order_payload(oid, body, st, None, px, now)
            self.state.apply_fill(o)
        else:
            st = "accepted"
            o = _order_payload(oid, body, st, "0", None, now)

        self.state.orders[oid] = o
        self._send(200, o)

    def do_DELETE(self) -> None:
        parsed = urlparse(self.path)
        m = _MAIN_TRADING_ORDER_UUID.match(parsed.path)
        if not m:
            self._send(
                404,
                {"message": f"unknown path {parsed.path}; main.py only DELETE /v2/orders/{{uuid}}"},
            )
            return
        oid = m.group(1).lower()
        o = self.state.orders.get(oid)
        if not o:
            self._send(404, {"code": 40410000, "message": "order not found"})
            return
        now = _utc_now()
        o["status"] = "canceled"
        o["canceled_at"] = _iso(now)
        o["updated_at"] = _iso(now)
        self._send_empty(204)


class DataHandler(BaseHTTPRequestHandler):
    state: MockState
    log_verbose: bool = False
    access_log: bool = False

    def log_message(self, fmt: str, *args: Any) -> None:
        if self.log_verbose:
            super().log_message(fmt, *args)

    def _send(self, code: int, body: Any) -> None:
        data = json.dumps(body).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)
        if self.access_log:
            _access_line(
                "data",
                self.command,
                self.path,
                code,
                _summary_data(urlparse(self.path).path, code, body, self.state),
            )

    def _send_html(self, code: int, html: str) -> None:
        data = html.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)
        if self.access_log:
            _access_line(
                "data",
                self.command,
                self.path,
                code,
                _summary_data(urlparse(self.path).path, code, {"_html": True}, self.state),
            )

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path
        qs = parse_qs(parsed.query)

        if path == "/v1/mock/status":
            st = self.state
            st.refresh_replay_clock_from_wall()
            syn: str | None = None
            if st.alpaca_historical_et_date is not None:
                syn = _iso(st.replay_now_utc())
            with st._tracked_symbols_lock:
                ts_sorted = sorted(st.tracked_symbols)
            self._send(
                200,
                {
                    "sim_session_minutes": st.sim_session_minutes,
                    "data_mode": "alpaca_replay" if st.alpaca_historical_et_date else "local_synthetic",
                    "alpaca_historical_et_date": str(st.alpaca_historical_et_date)
                    if st.alpaca_historical_et_date
                    else None,
                    "alpaca_historical_et_time": st.alpaca_historical_et_time.strftime("%H:%M")
                    if st.alpaca_historical_et_time
                    else None,
                    "upstream_data_url": st.upstream_data_url if st.alpaca_historical_et_date else None,
                    "synthetic_timestamp_utc": syn,
                    "market_open_flag": st.replay_market_is_open(),
                    "quote_tick_index": st.quote_tick_index,
                    "tracked_symbol_count": len(ts_sorted),
                    "tracked_symbols_sample": ts_sorted[:200],
                },
            )
            return

        if path == "/chart":
            self._send_html(200, _CHART_PAGE_HTML)
            return

        if path == "/v1/mock/chart-series":
            code, body = _mock_chart_series(self.state, qs)
            self._send(code, body if isinstance(body, dict) else {"message": str(body)})
            return

        if path not in _MAIN_DATA_PATHS:
            self._send(
                404,
                {
                    "message": (
                        f"unknown path {path}; data mock serves /v2/stocks/bars, "
                        "/v2/stocks/quotes/latest, /chart, /v1/mock/chart-series, /v1/mock/status"
                    )
                },
            )
            return

        if path == "/v2/stocks/bars":
            self.state.record_tracked_symbols(_split_symbol_csv((qs.get("symbols") or [""])[0]))
            if (
                _wants_passthrough(self.headers)
                and self.state.upstream_api_key
                and self.state.upstream_secret_key
            ):
                params = flatten_passthrough_params(qs)
                code, body, err = upstream_get_json(
                    self.state.upstream_data_url,
                    "/v2/stocks/bars",
                    params,
                    self.state.upstream_api_key,
                    self.state.upstream_secret_key,
                )
                if body is None:
                    body = {"message": err or "upstream passthrough error"}
                self._send(code, body if isinstance(body, dict) else {"message": str(body)})
                return
            if (
                self.state.alpaca_historical_et_date is not None
                and self.state.upstream_api_key
                and self.state.upstream_secret_key
            ):
                self.state.refresh_replay_clock_from_wall()
                code, body = proxy_stock_bars(
                    qs,
                    self.state.alpaca_historical_et_date,
                    self.state.upstream_data_url,
                    self.state.upstream_api_key,
                    self.state.upstream_secret_key,
                    self.state.replay_now_utc(),
                )
                if code == 200:
                    self.state.remember_market_data(body)
                self._send(code, body if isinstance(body, dict) else {"message": str(body)})
                return
            symbols = (qs.get("symbols") or [""])[0].split(",")
            symbols = [s.strip().upper() for s in symbols if s.strip()]
            timeframe = (qs.get("timeframe") or ["1Min"])[0]
            start = _parse_iso((qs.get("start") or [None])[0])
            end = _parse_iso((qs.get("end") or [None])[0])
            limit_raw = (qs.get("limit") or [None])[0]
            limit = int(limit_raw) if limit_raw and limit_raw.isdigit() else None
            bars = _synthetic_bars(symbols, timeframe, start, end, limit, self.state)
            self._send(200, {"bars": bars, "next_page_token": None})
            return

        if path == "/v2/stocks/quotes/latest":
            self.state.record_tracked_symbols(_split_symbol_csv((qs.get("symbols") or [""])[0]))
            if (
                _wants_passthrough(self.headers)
                and self.state.upstream_api_key
                and self.state.upstream_secret_key
            ):
                params = flatten_passthrough_params(qs)
                code, body, err = upstream_get_json(
                    self.state.upstream_data_url,
                    "/v2/stocks/quotes/latest",
                    params,
                    self.state.upstream_api_key,
                    self.state.upstream_secret_key,
                )
                if body is None:
                    body = {"message": err or "upstream passthrough error"}
                self._send(code, body if isinstance(body, dict) else {"message": str(body)})
                return
            if (
                self.state.alpaca_historical_et_date is not None
                and self.state.upstream_api_key
                and self.state.upstream_secret_key
            ):
                self.state.refresh_replay_clock_from_wall()
                code, body = proxy_quotes_latest(
                    qs,
                    self.state.alpaca_historical_et_date,
                    self.state.upstream_data_url,
                    self.state.upstream_api_key,
                    self.state.upstream_secret_key,
                    self.state.replay_now_utc(),
                )
                if code == 200:
                    self.state.remember_market_data(body)
                self._send(code, body if isinstance(body, dict) else {"message": str(body)})
                return
            symbols = (qs.get("symbols") or [""])[0].split(",")
            symbols = [s.strip().upper() for s in symbols if s.strip()]
            quotes: dict[str, dict[str, Any]] = {}
            for sym in symbols:
                t, mid, spread = self.state.next_quote(sym)
                quotes[sym] = {
                    "t": t,
                    "bp": mid - spread / 2,
                    "ap": mid + spread / 2,
                    "bs": 100.0,
                    "as": 100.0,
                }
            self._send(200, {"quotes": quotes})
            return

        raise AssertionError(f"unreachable routing for {path}")


def _run(host: str, port: int, handler: type[BaseHTTPRequestHandler]) -> ThreadingHTTPServer:
    server = ThreadingHTTPServer((host, port), handler)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    return server


def main() -> None:
    env_path = preparse_env_file_arg(sys.argv)
    loaded_env_path = load_dotenv(Path(env_path) if env_path else None)

    p = argparse.ArgumentParser(
        description="Alpaca REST mock: routes used by stocktrader main.py (alpaca-py layout).",
    )
    p.add_argument(
        "--env-file",
        type=str,
        default=None,
        metavar="PATH",
        help="Optional .env path (default: .env next to mock_server.py, then cwd). Loaded before other flags.",
    )
    p.add_argument("--host", default=env_str("ALPACA_MOCK_HOST", "127.0.0.1") or "127.0.0.1")
    p.add_argument("--trading-port", type=int, default=env_int("ALPACA_MOCK_TRADING_PORT", 19901))
    p.add_argument("--data-port", type=int, default=env_int("ALPACA_MOCK_DATA_PORT", 19902))
    p.add_argument(
        "--cash",
        type=float,
        default=float(env_str("ALPACA_MOCK_CASH", "25000") or "25000"),
        metavar="USD",
        help=(
            "Starting cash / buying_power for the local paper-trading mock (GET /v2/account). "
            "Same role as a real Alpaca paper account balance. Default: 25000 unless overridden by "
            "ALPACA_MOCK_CASH in the environment or .env."
        ),
    )
    p.add_argument("--market-closed", action="store_true", help="Report clock.is_open=false")
    p.add_argument("--price", action="append", default=[], metavar="SYM=PRICE", help="Mock mid price (repeatable)")
    p.add_argument(
        "--alpaca-date",
        type=str,
        default=env_str("ALPACA_MOCK_ALPACA_DATE") or env_str("ALPACA_MOCK_DATE") or None,
        metavar="YYYY-MM-DD",
        help=(
            "Replay calendar day (US/Eastern): forward /v2/stocks/bars and /v2/stocks/quotes/latest to Alpaca's "
            "data API with request times snapped onto this date. Requires upstream credentials (not the mock client keys)."
        ),
    )
    p.add_argument(
        "--alpaca-time",
        type=_parse_hhmm,
        default=_parse_hhmm(env_str("ALPACA_MOCK_ALPACA_TIME") or env_str("ALPACA_MOCK_TIME") or "09:30"),
        metavar="HH:MM",
        help=(
            "Replay clock start time in New York time when --alpaca-date is active. "
            "Default: 09:30. Example: --alpaca-time 09:35."
        ),
    )
    p.add_argument(
        "--upstream-trading-url",
        type=str,
        default=env_str("ALPACA_UPSTREAM_TRADING_URL", "https://paper-api.alpaca.markets")
        or "https://paper-api.alpaca.markets",
        help="Alpaca Trading API base URL for GET /v2/assets when upstream keys are set (env ALPACA_UPSTREAM_TRADING_URL).",
    )
    p.add_argument(
        "--upstream-data-url",
        type=str,
        default=env_str("ALPACA_UPSTREAM_DATA_URL", "https://data.alpaca.markets") or "https://data.alpaca.markets",
        help="Alpaca Data API base URL when using --alpaca-date (default: env ALPACA_UPSTREAM_DATA_URL or production).",
    )
    p.add_argument(
        "--upstream-api-key",
        type=str,
        default=env_str("ALPACA_UPSTREAM_API_KEY", ""),
        help="Alpaca API key id for upstream data fetches (env ALPACA_UPSTREAM_API_KEY).",
    )
    p.add_argument(
        "--upstream-secret-key",
        type=str,
        default=env_str("ALPACA_UPSTREAM_SECRET_KEY", ""),
        help="Alpaca API secret for upstream data fetches (env ALPACA_UPSTREAM_SECRET_KEY).",
    )
    p.add_argument("-v", "--verbose", action="store_true", help="Stdlib HTTP request line logging (Apache-style)")
    p.add_argument(
        "--access-log",
        action="store_true",
        help="Log each HTTP response: trading|data, method, path, status, summary (clock, bars, quotes, orders, …)",
    )
    args = p.parse_args()
    if args.env_file and loaded_env_path is None:
        p.error(f"--env-file not found or not readable: {args.env_file}")

    if env_bool("ALPACA_MOCK_MARKET_CLOSED"):
        args.market_closed = True
    if env_bool("ALPACA_MOCK_ACCESS_LOG"):
        args.access_log = True
    if env_bool("ALPACA_MOCK_VERBOSE"):
        args.verbose = True
    for part in env_str("ALPACA_MOCK_PRICE").split(","):
        piece = part.strip()
        if "=" in piece:
            args.price.append(piece)

    if loaded_env_path is not None:
        print(f"Loaded environment from {loaded_env_path}", flush=True)

    alpaca_hist: date | None = None
    alpaca_hist_time: time_of_day | None = None
    if args.alpaca_date and str(args.alpaca_date).strip():
        key = str(args.upstream_api_key).strip()
        sec = str(args.upstream_secret_key).strip()
        if not key or not sec:
            p.error(
                "--alpaca-date requires upstream Alpaca credentials: set "
                "ALPACA_UPSTREAM_API_KEY and ALPACA_UPSTREAM_SECRET_KEY or pass "
                "--upstream-api-key and --upstream-secret-key"
            )
        try:
            alpaca_hist = date.fromisoformat(str(args.alpaca_date).strip())
        except ValueError:
            p.error("--alpaca-date must be YYYY-MM-DD")
        alpaca_hist_time = args.alpaca_time or time_of_day(9, 30)

    state = MockState(
        args.cash,
        market_open=not args.market_closed,
        alpaca_historical_et_date=alpaca_hist,
        alpaca_historical_et_time=alpaca_hist_time,
        upstream_data_url=args.upstream_data_url,
        upstream_trading_url=args.upstream_trading_url,
        upstream_api_key=str(args.upstream_api_key).strip() or None,
        upstream_secret_key=str(args.upstream_secret_key).strip() or None,
    )
    for item in args.price:
        if "=" not in item:
            continue
        k, v = item.split("=", 1)
        state.mock_prices[k.strip().upper()] = float(v)

    access_log = args.access_log
    if access_log:
        _setup_access_logging()

    TradingHandler.state = state
    TradingHandler.log_verbose = args.verbose
    TradingHandler.access_log = access_log
    DataHandler.state = state
    DataHandler.log_verbose = args.verbose
    DataHandler.access_log = access_log

    t_srv: ThreadingHTTPServer | None = None
    d_srv: ThreadingHTTPServer | None = None
    try:
        t_srv = _run(args.host, args.trading_port, TradingHandler)
        d_srv = _run(args.host, args.data_port, DataHandler)
    except Exception:
        if t_srv is not None:
            t_srv.shutdown()
            t_srv.server_close()
        raise

    alpaca_line = ""
    if alpaca_hist:
        weekend_note = ""
        if alpaca_hist.weekday() >= 5:
            weekend_note = "  Warning: replay date is a weekend; upstream equity data will usually be empty.\n"
        alpaca_line = (
            f"  Alpaca historical replay: ET date={alpaca_hist} time={alpaca_hist_time.strftime('%H:%M') if alpaca_hist_time else 'wall-clock'} | upstream data={args.upstream_data_url.rstrip('/')}\n"
            f"{weekend_note}"
        )
    print(
        f"Alpaca mock (main.py REST surface) listening:\n"
        f"  Trading: http://{args.host}:{args.trading_port}\n"
        f"           GET  /v2/clock /v2/account /v2/positions /v2/orders /v2/assets /v2/orders/{{id}}\n"
        f"           POST /v2/orders   DELETE /v2/orders/{{id}}\n"
        f"  Data:    http://{args.host}:{args.data_port}\n"
        f"           GET /v2/stocks/quotes/latest   GET /v2/stocks/bars (rest mode only)\n"
        f"           GET /chart  GET /v1/mock/chart-series  GET /v1/mock/status\n"
        f"{alpaca_line}"
        f"  Without --alpaca-date, bars/quotes are simple local synthetics from --price / defaults.\n"
        f"Set ALPACA_TRADING_BASE_URL and ALPACA_DATA_BASE_URL (EXECUTION_MODE / ALPACA_MARKET_DATA_MODE as needed).\n"
        f"  Response logging: {'on (--access-log)' if access_log else 'off (add --access-log to log each reply)'}",
        flush=True,
    )
    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        print("\nShutting down.", flush=True)
    finally:
        if t_srv is not None:
            t_srv.shutdown()
            t_srv.server_close()
        if d_srv is not None:
            d_srv.shutdown()
            d_srv.server_close()


if __name__ == "__main__":
    main()
