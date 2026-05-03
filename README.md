# alpaca_mock_server

Local HTTP mock of the Alpaca REST endpoints used by **stocktrader** (`stocktrader/main.py` via alpaca-py). WebSocket market data is not implemented here.

## Run

```bash
python mock_server.py --scenario samples/intc_may01_chart_scenario.json
python mock_server.py --scenario samples/intc_may01_chart_scenario.json --sim-clock wall --sim-cycle-seconds 3600
```

Add **`--access-log`** to print each HTTP reply (trading vs data port, status, and a short summary: clock fields, last bar OHLC, quote mids, order status, etc.). Use **`-v`** separately for stdlib-style request lines.

## Point stocktrader at the mock

Set (see stocktrader `config.py` / `alpaca_client.py`):

- `ALPACA_TRADING_BASE_URL=http://127.0.0.1:19901`
- `ALPACA_DATA_BASE_URL=http://127.0.0.1:19902`
- `ALPACA_API_KEY=test`
- `ALPACA_SECRET_KEY=test`

## What is mocked

**Trading** (`127.0.0.1:<trading-port>`) when `EXECUTION_MODE=alpaca_paper`:

- `GET /v2/clock`, `/v2/account`, `/v2/positions`, `/v2/orders`
- `GET /v2/orders/{uuid}`, `POST /v2/orders`, `DELETE /v2/orders/{uuid}`

**Market data** (`127.0.0.1:<data-port>`):

- `GET /v2/stocks/quotes/latest`
- `GET /v2/stocks/bars` (when `ALPACA_MARKET_DATA_MODE=rest`)

## Scenarios

Optional `--scenario` JSON; the repo includes one versioned example (`samples/intc_may01_chart_scenario.json` + `samples/INTC_2026-05-01.png`). Other files under `samples/` are **gitignored**—add your own scenarios or screenshots there locally. Timing flags `--minutes-per-bars-tick` / `--sim-clock` / `--sim-cycle-seconds` are documented in `mock_server.py`.

With **minute** clock (the default when a scenario is loaded), bar and quote timestamps and `GET /v2/clock`’s `timestamp` are aligned to a **synthetic US/Eastern RTH day** (09:30 + simulated session minute). That matches clients that filter on regular market hours (e.g. `regular_market_only`) while you run the stack at night. Set the calendar day explicitly with `--session-date YYYY-MM-DD` (interpreted in `America/New_York`).

Diagnostics (not part of Alpaca’s API): `GET http://127.0.0.1:<data-port>/v1/mock/status` returns `sim_session_minutes`, synthetic clock fields, and `market_open_flag`.

### Sub-minute / 1 Hz REST

By default each `GET /v2/stocks/bars` advances **one simulated session minute** and bar rows use the requested **timeframe** (e.g. `1Min`) as the step on the session curve.

To align **one real second per poll** with **one session second** on the curve (e.g. stocktrader polling every second):

1. Request bars with **`timeframe=1Sec`** (supported alongside `1Min`, `5Min`, …).
2. Start the mock with **`--seconds-per-bars-tick 1`** (same as `--minutes-per-bars-tick 0.016666666666666666…`).

Then each `1Sec` bars response advances `sim_session_minutes` by `1/60`, last-bar synthetic `t` steps by one second, and OHLC spans one session second on the scenario. For stocktrader's default `1Min` REST bars, the mock advances at least one synthetic minute per bars response so opening-range logic sees new minute bars.

Requires Python 3.9+ (stdlib only, including `zoneinfo` IANA data).
