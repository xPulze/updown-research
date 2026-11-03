# updown: Binance and Polymarket 5-minute up/down tooling (retired)

Research code from two retired projects that tried to call the direction of 5-minute BTC/ETH windows.
It is published as an engineering and research reference, **not** as a profitable system.
Neither project is running, and neither is being maintained.

| Folder | What it is |
|---|---|
| `binance_updown/` | Advisory-only monitor for Binance's 5-minute Higher/Lower product. It reads public market data, tracks each UTC 5-minute window, and says when to stake, which side and how much. It places no orders and uses no API key. Also has multi-horizon monitors (10/15 min), backtests and a user manual. |
| `polymarket_updown/` | Execution bot for Polymarket 5-minute up/down markets (shadow and live modes), with a settlement worker, a price-feed (RTDS) memory engine, order-metadata and quote diagnostics, data collectors and tests. |

## Read this first
- Backtest accuracy figures in the code comments and docs come from specific historical samples. They are not live returns and were not enough to keep either project running.
- Prediction-market and event-contract prices already move toward the obvious outcome. Accuracy is not edge once the entry price is paid.
- Trading can lose all of your capital. Check the rules of each venue and your jurisdiction before using any of this.
- Nothing here is financial advice.

## Setup
```
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env     # fill in only what you need; everything is read from the environment
```
The Binance monitor runs with no credentials. Run it from the repository root:
`python3 binance_updown/binance_5min_stake_alert_monitor.py`. The Postgres tick mirror and Telegram alerts are optional.
Paths in the code are relative to the repository root, so run scripts from there.

The Polymarket bot defaults to dry-run (`POLYMARKET_DRY_RUN=1`).
It is a reference implementation extracted from a larger system, so expect to adapt it before it runs end to end.
It uses `config.DB_URL` (a SQLAlchemy URL) for its state.
Live trading needs `POLYMARKET_PRIVATE_KEY` and `POLYMARKET_WALLET_ADDRESS` in the environment, never in code.

## Layout notes
- Backtests under `binance_updown/backtests/` expect a Postgres table of tick history (`binance_5min_ticks`).
- `deploy/*.service` files are systemd examples that assume the code lives in `/opt/updown`.

## Test status
`polymarket_updown/tests` has 115 tests: 70 pass, 44 fail, and 1 is skipped (it needs the dashboard, which is not included).
The failures also occur in the original private codebase.
The tests were written against earlier versions of the bot's rules, and the rules kept changing until the project was retired.
Treat the tests as documentation of intent, not as a green CI signal.
Run them with `cd polymarket_updown && PYTHONPATH=. pytest tests`.
The Binance monitor has no test suite. It was only smoke-tested (it imports cleanly).
