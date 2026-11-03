#!/usr/bin/env python3
"""Free independent USD reference ticks for Polymarket BTC/ETH validation.

Collects Coinbase Exchange and Kraken public WebSocket trade ticks. One latest
tick per source/asset/second is retained: this is sufficient for 30/60-second
TWAP reconstruction without allowing high-volume bursts to overweight a venue.
No credentials and no order endpoints are used.
"""
import asyncio
import json
import os
import signal
import sys
import time
from datetime import datetime, timezone

import sqlalchemy
from sqlalchemy import text
import websockets

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DB_URL

ENGINE = sqlalchemy.create_engine(DB_URL, pool_pre_ping=True, pool_size=1,
                                  max_overflow=1, pool_recycle=300)
COINBASE_WS = "wss://ws-feed.exchange.coinbase.com"
KRAKEN_WS = "wss://ws.kraken.com/v2"
ASSETS = ("BTC", "ETH")
MAX_SOURCE_AGE_S = 10.0
SILENCE_S = 20.0
RETENTION_DAYS = 14
_run = True

UPSERT = text("""
    INSERT INTO pm_reference_ticks
        (source, asset, observations_ts, source_ts_ms, price, bid, ask, received_at)
    VALUES (:source, :asset, :observations_ts, :source_ts_ms, :price, :bid, :ask, NOW())
    ON CONFLICT (source, asset, observations_ts) DO UPDATE
       SET source_ts_ms=EXCLUDED.source_ts_ms,
           price=EXCLUDED.price,
           bid=EXCLUDED.bid,
           ask=EXCLUDED.ask,
           received_at=NOW()
     WHERE EXCLUDED.source_ts_ms >= pm_reference_ticks.source_ts_ms
""")


def stop(*_args):
    global _run
    _run = False


def ensure_schema():
    with ENGINE.begin() as c:
        c.execute(text("""
            CREATE TABLE IF NOT EXISTS pm_reference_ticks (
                source          TEXT NOT NULL,
                asset           TEXT NOT NULL,
                observations_ts BIGINT NOT NULL,
                source_ts_ms    BIGINT NOT NULL,
                price           DOUBLE PRECISION NOT NULL,
                bid             DOUBLE PRECISION,
                ask             DOUBLE PRECISION,
                received_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (source, asset, observations_ts)
            );
            CREATE INDEX IF NOT EXISTS idx_pm_reference_ticks_asset_ts
                ON pm_reference_ticks (asset, observations_ts);
        """))
        c.execute(text("ALTER TABLE pm_reference_ticks "
                       "ADD COLUMN IF NOT EXISTS bid DOUBLE PRECISION"))
        c.execute(text("ALTER TABLE pm_reference_ticks "
                       "ADD COLUMN IF NOT EXISTS ask DOUBLE PRECISION"))


def write_batch(rows):
    if not rows:
        return
    # Keep only the last tick in each venue/asset/second within this batch.
    latest = {(r["source"], r["asset"], r["observations_ts"]): r for r in rows}
    with ENGINE.begin() as c:
        c.execute(UPSERT, list(latest.values()))


async def db_writer(queue):
    last_prune = 0.0
    while _run or not queue.empty():
        rows = []
        try:
            rows.append(await asyncio.wait_for(queue.get(), timeout=1.0))
        except asyncio.TimeoutError:
            pass
        while len(rows) < 1000 and not queue.empty():
            rows.append(queue.get_nowait())
        if rows:
            try:
                await asyncio.to_thread(write_batch, rows)
            except Exception as exc:
                print(f"DB write failed: {type(exc).__name__}: {exc}", flush=True)
        now = time.time()
        if now - last_prune >= 3600:
            try:
                with ENGINE.begin() as c:
                    c.execute(text("DELETE FROM pm_reference_ticks "
                                   "WHERE observations_ts < :cut"),
                              {"cut": int(now - RETENTION_DAYS * 86400)})
            except Exception as exc:
                print(f"DB prune failed: {type(exc).__name__}: {exc}", flush=True)
            last_prune = now


def iso_ms(value):
    dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return int(dt.timestamp() * 1000)


async def coinbase_once(queue):
    async with websockets.connect(COINBASE_WS, ping_interval=15, ping_timeout=10,
                                  open_timeout=10, close_timeout=5) as ws:
        await ws.send(json.dumps({
            "type": "subscribe",
            "product_ids": ["BTC-USD", "ETH-USD"],
            "channels": ["ticker"],
        }))
        print("Coinbase WS connected", flush=True)
        last = time.monotonic()
        while _run:
            raw = await asyncio.wait_for(ws.recv(), timeout=SILENCE_S)
            msg = json.loads(raw)
            if msg.get("type") != "ticker" or msg.get("product_id") not in (
                    "BTC-USD", "ETH-USD"):
                continue
            ts_ms = iso_ms(msg["time"])
            if time.time() - ts_ms / 1000.0 > MAX_SOURCE_AGE_S:
                continue
            asset = msg["product_id"].split("-")[0]
            bid = float(msg.get("best_bid") or msg["price"])
            ask = float(msg.get("best_ask") or msg["price"])
            await queue.put({"source": "coinbase", "asset": asset,
                             "observations_ts": ts_ms // 1000,
                             "source_ts_ms": ts_ms, "price": (bid + ask) / 2.0,
                             "bid": bid, "ask": ask})
            last = time.monotonic()
            if time.monotonic() - last > SILENCE_S:
                raise TimeoutError("Coinbase accepted-tick silence")


async def kraken_once(queue):
    async with websockets.connect(KRAKEN_WS, ping_interval=15, ping_timeout=10,
                                  open_timeout=10, close_timeout=5) as ws:
        await ws.send(json.dumps({"method": "subscribe", "params": {
            "channel": "ticker", "symbol": ["BTC/USD", "ETH/USD"],
            "event_trigger": "bbo", "snapshot": True}}))
        print("Kraken WS connected", flush=True)
        while _run:
            raw = await asyncio.wait_for(ws.recv(), timeout=SILENCE_S)
            msg = json.loads(raw)
            if msg.get("channel") != "ticker":
                continue
            for tick in msg.get("data") or ():
                symbol = tick.get("symbol")
                if symbol not in ("BTC/USD", "ETH/USD"):
                    continue
                ts_ms = iso_ms(tick["timestamp"])
                if time.time() - ts_ms / 1000.0 > MAX_SOURCE_AGE_S:
                    continue
                bid, ask = float(tick["bid"]), float(tick["ask"])
                await queue.put({"source": "kraken", "asset": symbol[:3],
                                 "observations_ts": ts_ms // 1000,
                                 "source_ts_ms": ts_ms,
                                 "price": (bid + ask) / 2.0,
                                 "bid": bid, "ask": ask})


async def reconnecting(name, fn, queue):
    backoff = 1.0
    while _run:
        try:
            await fn(queue)
            backoff = 1.0
        except Exception as exc:
            if not _run:
                break
            print(f"{name} failed: {type(exc).__name__}: {str(exc)[:160]} "
                  f"— reconnect {backoff:.0f}s", flush=True)
            await asyncio.sleep(backoff)
            backoff = min(30.0, backoff * 2.0)


async def main_async():
    await asyncio.to_thread(ensure_schema)
    queue = asyncio.Queue(maxsize=10000)
    tasks = [asyncio.create_task(db_writer(queue)),
             asyncio.create_task(reconnecting("Coinbase", coinbase_once, queue)),
             asyncio.create_task(reconnecting("Kraken", kraken_once, queue))]
    while _run:
        await asyncio.sleep(1)
    await asyncio.gather(*tasks, return_exceptions=True)


def main():
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    print("PM independent reference collector starting: Coinbase + Kraken "
          "BTC/USD, ETH/USD", flush=True)
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
