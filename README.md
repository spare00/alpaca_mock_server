# alpaca_mock_server

Local HTTP mock of the Alpaca REST endpoints used by **stocktrader** (`stocktrader/main.py` via alpaca-py). WebSocket market data is not implemented here.

## Run

```bash
python mock_server.py --scenario samples/intc_day_scenario.json
python mock_server.py --scenario samples/rig_day_scenario.json --sim-clock wall --sim-cycle-seconds 3600
```

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

Optional `--scenario` JSON (see `samples/intc_day_scenario.json`). Simulated session timing and `--minutes-per-bars-tick` / `--sim-clock` / `--sim-cycle-seconds` are documented in `mock_server.py`.

Requires Python 3 (stdlib only).
