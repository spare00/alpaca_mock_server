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
- ``POST /v2/orders`` — buy / sell
- ``DELETE /v2/orders/{uuid}`` — cancel after timeout

**Market data** (``http://127.0.0.1:<data-port>``):

- ``GET /v2/stocks/quotes/latest`` — ``AlpacaRestPollingStream`` each poll, and
  ``AlpacaPaperExecutor._fresh_entry_price`` before a buy when stream mode
- ``GET /v2/stocks/bars`` — ``AlpacaRestPollingStream`` only when
  ``ALPACA_MARKET_DATA_MODE=rest`` (not used on the default stream path)

Stream mode still uses Alpaca WebSockets for live bars/quotes; this mock does
not implement WS.

**Intraday simulation** (optional): pass ``--scenario`` to a JSON file (see
``samples/intc_day_scenario.json``). Two clocks:

- **minute** (default when a scenario is loaded): a **simulated session minute**
  counter advances by ``--minutes-per-bars-tick`` (default 1) on each
  ``GET /v2/stocks/bars`` response — matching stocktrader's rest loop (quotes
  then bars). Each poll moves **one minute along the curve**, so 1m OHLC and
  quotes line up for short (seconds–minutes) trading logic. Optional JSON
  ``session_minutes`` (default 390) is the length of one synthetic RTH day on
  the curve before wrap.

- **wall**: ``u`` from wall time modulo ``--sim-cycle-seconds`` (legacy).

Run::

    python mock_server.py --scenario samples/intc_day_scenario.json
    python mock_server.py --scenario samples/rig_day_scenario.json --sim-clock wall --sim-cycle-seconds 3600

Point stocktrader at the mock (see ``config.py`` / ``alpaca_client.py``)::

    ALPACA_TRADING_BASE_URL=http://127.0.0.1:19901
    ALPACA_DATA_BASE_URL=http://127.0.0.1:19902
    ALPACA_API_KEY=test
    ALPACA_SECRET_KEY=test
"""
from __future__ import annotations

import argparse
import json
import math
import re
import threading
import time
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

# Paths reachable from stocktrader main.py (see module docstring).
_MAIN_TRADING_GET_PATHS = frozenset({"/v2/clock", "/v2/account", "/v2/positions", "/v2/orders"})
_MAIN_TRADING_ORDER_UUID = re.compile(r"^/v2/orders/([0-9a-f-]{36})$", re.I)
_MAIN_DATA_PATHS = frozenset({"/v2/stocks/bars", "/v2/stocks/quotes/latest"})


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


def _timeframe_seconds(timeframe: str) -> int | None:
    m = re.match(r"^(\d+)(Min|Hour|Day|Week|Month)$", timeframe or "")
    if not m:
        return 60
    n, unit = int(m.group(1)), m.group(2)
    mult = {"Min": 60, "Hour": 3600, "Day": 86400, "Week": 604800, "Month": 2592000}
    return n * mult[unit]


class MockState:
    def __init__(
        self,
        starting_cash: float,
        market_open: bool,
        *,
        sim_anchor_epoch: float | None = None,
        sim_cycle_seconds: float = 3600.0,
        scenario_points: dict[str, list[tuple[float, float]]] | None = None,
        sim_clock_mode: str = "wall",
        session_minutes_total: int = 390,
        minutes_per_bars_tick: float = 1.0,
    ) -> None:
        self.starting_cash = starting_cash
        self.market_open = market_open
        self.orders: dict[str, dict[str, Any]] = {}
        self.mock_prices: dict[str, float] = defaultdict(lambda: 100.0)
        self.sim_anchor_epoch = float(sim_anchor_epoch if sim_anchor_epoch is not None else time.time())
        self.sim_cycle_seconds = max(60.0, float(sim_cycle_seconds))
        self.scenario_points: dict[str, list[tuple[float, float]]] = scenario_points or {}
        self.sim_clock_mode = sim_clock_mode
        self.session_minutes_total = max(1, int(session_minutes_total))
        self.minutes_per_bars_tick = max(0.01, float(minutes_per_bars_tick))
        self.sim_session_minutes: float = 0.0
        self.sim_lock = threading.Lock()

    def virtual_u(self, at: datetime) -> float:
        t = _as_utc_timestamp(at)
        elapsed = t - self.sim_anchor_epoch
        u = (elapsed % self.sim_cycle_seconds) / self.sim_cycle_seconds
        return min(1.0, max(0.0, u))

    def mid_price(self, symbol: str, at: datetime) -> float:
        sym = symbol.upper()
        pts = self.scenario_points.get(sym)
        if pts:
            if self.sim_clock_mode == "minute":
                with self.sim_lock:
                    sm = float(self.sim_session_minutes)
                return _mid_from_session_minute(pts, sm, float(self.session_minutes_total))
            return _interp_points(pts, self.virtual_u(at))
        return float(self.mock_prices[sym])


def _as_utc_timestamp(dt: datetime) -> float:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).timestamp()


def _mid_from_session_minute(
    points: list[tuple[float, float]], session_minute: float, session_len: float
) -> float:
    """Map simulated RTH minute index to scenario u in [0,1)."""
    if session_len <= 0:
        return _interp_points(points, 0.0)
    m = session_minute % session_len
    u = m / session_len
    return _interp_points(points, u)


def _interp_points(points: list[tuple[float, float]], u: float) -> float:
    pts = sorted(points, key=lambda x: x[0])
    if not pts:
        return 100.0
    u = u % 1.0
    if u <= pts[0][0]:
        return float(pts[0][1])
    for i in range(len(pts) - 1):
        u0, p0 = pts[i]
        u1, p1 = pts[i + 1]
        if u0 <= u <= u1:
            if abs(u1 - u0) < 1e-12:
                return float(p0)
            return float(p0 + (p1 - p0) * (u - u0) / (u1 - u0))
    return float(pts[-1][1])


def _load_scenario(path: str) -> tuple[dict[str, list[tuple[float, float]]], float | None, int]:
    """Returns (symbol -> [(u, price), ...], cycle_seconds or None, session_minutes)."""
    with open(path, encoding="utf-8") as f:
        raw = json.load(f)
    cycle = raw.get("cycle_seconds")
    session_minutes = int(raw.get("session_minutes", 390))
    symbols_block = raw.get("symbols") or {}
    out: dict[str, list[tuple[float, float]]] = {}
    for sym, spec in symbols_block.items():
        sym_u = sym.strip().upper()
        pairs = spec.get("points") or []
        parsed: list[tuple[float, float]] = []
        for row in pairs:
            if isinstance(row, (list, tuple)) and len(row) >= 2:
                parsed.append((float(row[0]), float(row[1])))
        if parsed:
            out[sym_u] = parsed
    return out, float(cycle) if cycle is not None else None, session_minutes


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

    if state.sim_clock_mode == "minute":
        return _synthetic_bars_minute_clock(
            symbols, timeframe, start_dt, end_dt, limit, max_points, step, state
        )

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


def _synthetic_bars_minute_clock(
    symbols: list[str],
    _timeframe: str,
    start_dt: datetime,
    end_dt: datetime,
    limit: int | None,
    max_points: int,
    step: int,
    state: MockState,
) -> dict[str, list[dict[str, Any]]]:
    """One simulated session minute per bars request; 1m OHLC from scenario knots."""
    session_len = float(state.session_minutes_total)
    times: list[datetime] = []
    t = start_dt
    i = 0
    while t <= end_dt and i < max_points:
        times.append(t)
        t += timedelta(seconds=step)
        i += 1
    if limit and len(times) > limit:
        times = times[-limit:]

    out: dict[str, list[dict[str, Any]]] = {s: [] for s in symbols}
    with state.sim_lock:
        cur = int(math.floor(state.sim_session_minutes))
        n = len(times)
        for sym in symbols:
            pts = state.scenario_points.get(sym)
            for j, t_open in enumerate(times):
                bm = cur - (n - 1 - j)
                if pts:
                    o = _mid_from_session_minute(pts, float(bm), session_len)
                    c = _mid_from_session_minute(pts, float(bm + 1), session_len)
                else:
                    px = float(state.mock_prices[sym])
                    o = c = px
                hi = max(o, c) * 1.001
                lo = min(o, c) * 0.999
                vol = 800.0 + 40.0 * abs(c - o) * 1000.0
                if abs(c - o) > 0.03:
                    vol += 5000.0
                out[sym].append(
                    {
                        "t": _iso(t_open),
                        "o": o,
                        "h": hi,
                        "l": lo,
                        "c": c,
                        "v": vol,
                        "n": 50.0,
                        "vw": (o + c) / 2,
                    }
                )
        state.sim_session_minutes += state.minutes_per_bars_tick
    return out


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


class TradingHandler(BaseHTTPRequestHandler):
    state: MockState
    log_verbose: bool = False

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

    def _send_empty(self, code: int) -> None:
        self.send_response(code)
        self.send_header("Content-Length", "0")
        self.end_headers()

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
                        f"(clock, account, positions, orders)"
                    )
                },
            )
            return

        if path == "/v2/clock":
            now = _utc_now()
            self._send(
                200,
                {
                    "timestamp": _iso(now),
                    "is_open": self.state.market_open,
                    "next_open": _iso(now - timedelta(hours=1)),
                    "next_close": _iso(now + timedelta(hours=6)),
                },
            )
            return

        if path == "/v2/account":
            c = f"{self.state.starting_cash:.2f}"
            self._send(
                200,
                {
                    "id": str(uuid.uuid4()),
                    "account_number": "MOCK-0001",
                    "status": "ACTIVE",
                    "currency": "USD",
                    "cash": c,
                    "buying_power": c,
                    "portfolio_value": c,
                    "equity": c,
                    "pattern_day_trader": False,
                    "trading_blocked": False,
                    "transfers_blocked": False,
                    "account_blocked": False,
                    "multiplier": "1",
                    "shorting_enabled": True,
                },
            )
            return

        if path == "/v2/positions":
            self._send(200, [])
            return

        if path == "/v2/orders":
            status_filter = (qs.get("status") or [None])[0]
            out = []
            for o in self.state.orders.values():
                if status_filter and o.get("status") != status_filter:
                    continue
                if o.get("status") not in ("filled", "canceled", "expired", "rejected", "done_for_day"):
                    out.append(o)
            self._send(200, out)
            return

        m = _MAIN_TRADING_ORDER_UUID.match(path)
        if m:
            oid = m.group(1).lower()
            o = self.state.orders.get(oid)
            if not o:
                self._send(404, {"code": 40410000, "message": "order not found"})
                return
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

        now = _utc_now()
        oid = str(uuid.uuid4()).lower()
        sym = (body.get("symbol") or "").upper()
        px = f"{self.state.mid_price(sym, now):.4f}"

        if self.state.market_open and (body.get("type") or "market").lower() == "market":
            st = "filled"
            o = _order_payload(oid, body, st, None, px, now)
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

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path
        qs = parse_qs(parsed.query)

        if path not in _MAIN_DATA_PATHS:
            self._send(
                404,
                {
                    "message": (
                        f"unknown path {path}; this mock only implements main.py data GET "
                        "/v2/stocks/bars (rest mode) and /v2/stocks/quotes/latest"
                    )
                },
            )
            return

        if path == "/v2/stocks/bars":
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
            symbols = (qs.get("symbols") or [""])[0].split(",")
            symbols = [s.strip().upper() for s in symbols if s.strip()]
            now_dt = _utc_now()
            now = _iso(now_dt)
            quotes: dict[str, dict[str, Any]] = {}
            for sym in symbols:
                mid = self.state.mid_price(sym, now_dt)
                spread = max(0.01, mid * 0.0005)
                quotes[sym] = {
                    "t": now,
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
    p = argparse.ArgumentParser(
        description="Alpaca REST mock: routes used by stocktrader main.py (alpaca-py layout).",
    )
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--trading-port", type=int, default=19901)
    p.add_argument("--data-port", type=int, default=19902)
    p.add_argument("--cash", type=float, default=25_000.0, help="Account cash / buying_power in mock /account")
    p.add_argument("--market-closed", action="store_true", help="Report clock.is_open=false")
    p.add_argument("--price", action="append", default=[], metavar="SYM=PRICE", help="Mock mid price (repeatable)")
    p.add_argument(
        "--scenario",
        type=str,
        default=None,
        help="JSON file with intraday curves (see samples/rig_day_scenario.json)",
    )
    p.add_argument(
        "--sim-cycle-seconds",
        type=float,
        default=None,
        help="Seconds for one full u=[0,1] replay (overrides scenario JSON if set)",
    )
    p.add_argument(
        "--sim-anchor-epoch",
        type=float,
        default=None,
        help="Unix epoch anchor for simulation clock (default: server start)",
    )
    p.add_argument(
        "--sim-clock",
        choices=("minute", "wall", "auto"),
        default="auto",
        help="minute=advance one session minute per bars GET (default with --scenario); wall=time-based u",
    )
    p.add_argument(
        "--minutes-per-bars-tick",
        type=float,
        default=1.0,
        help="Simulated session minutes to advance after each GET /v2/stocks/bars (minute clock only)",
    )
    p.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args()

    scenario_points: dict[str, list[tuple[float, float]]] | None = None
    file_cycle: float | None = None
    session_minutes_total = 390
    if args.scenario:
        scenario_points, file_cycle, session_minutes_total = _load_scenario(args.scenario)
    cycle = args.sim_cycle_seconds if args.sim_cycle_seconds is not None else (file_cycle or 3600.0)

    if args.sim_clock == "auto":
        sim_clock_mode = "minute" if scenario_points else "wall"
    else:
        sim_clock_mode = args.sim_clock

    state = MockState(
        args.cash,
        market_open=not args.market_closed,
        sim_anchor_epoch=args.sim_anchor_epoch,
        sim_cycle_seconds=cycle,
        scenario_points=scenario_points,
        sim_clock_mode=sim_clock_mode,
        session_minutes_total=session_minutes_total,
        minutes_per_bars_tick=args.minutes_per_bars_tick,
    )
    for item in args.price:
        if "=" not in item:
            continue
        k, v = item.split("=", 1)
        state.mock_prices[k.strip().upper()] = float(v)

    TradingHandler.state = state
    TradingHandler.log_verbose = args.verbose
    DataHandler.state = state
    DataHandler.log_verbose = args.verbose

    t_srv = _run(args.host, args.trading_port, TradingHandler)
    d_srv = _run(args.host, args.data_port, DataHandler)

    sim_line = ""
    if scenario_points:
        sim_line = (
            f"  Intraday scenario: {args.scenario} | symbols={','.join(sorted(scenario_points))} | "
            f"clock={sim_clock_mode}"
            + (f" | session_minutes={session_minutes_total}" if sim_clock_mode == "minute" else f" | cycle={cycle:.0f}s")
            + "\n"
        )
    print(
        f"Alpaca mock (main.py REST surface) listening:\n"
        f"  Trading: http://{args.host}:{args.trading_port}\n"
        f"           GET  /v2/clock /v2/account /v2/positions /v2/orders /v2/orders/{{id}}\n"
        f"           POST /v2/orders   DELETE /v2/orders/{{id}}\n"
        f"  Data:    http://{args.host}:{args.data_port}\n"
        f"           GET /v2/stocks/quotes/latest   GET /v2/stocks/bars (rest mode only)\n"
        f"{sim_line}"
        f"Set ALPACA_TRADING_BASE_URL and ALPACA_DATA_BASE_URL (EXECUTION_MODE / ALPACA_MARKET_DATA_MODE as needed).",
        flush=True,
    )
    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        print("\nShutting down.", flush=True)
    finally:
        t_srv.shutdown()
        d_srv.shutdown()


if __name__ == "__main__":
    main()
