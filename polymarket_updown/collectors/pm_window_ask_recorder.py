import os
#!/usr/bin/env python3
"""Polymarket 5-min up/down multi-offset ask recorder (BTC + ETH) — READ-ONLY.

Why (2026-09-29 ETH T3 redesign): the bots store asks only at the T+3 decision,
so entry TIMING was the one dimension the pre-registered ETH study could not
test. This samples both tokens' REST books of the current window at every
30s offset (T+30 .. T+270) for both assets, plus the three raw inputs of our
constructed price at that moment — Polymarket RTDS (table `chainlink_prices`, a
legacy name: no Chainlink subscription), Coinbase and Kraken (`pm_reference_ticks`)
— into `pm_window_ask_samples`. Stored raw because both source tables are pruned,
so the bot's PM+Coinbase+Kraken construction can be rebuilt per sample later. Outcomes are joined later
from polymarket_updown_trades(_eth).actual_outcome on window_start_utc.

Independent of both bots: public gamma + CLOB REST only, no keys, no orders.
    pm_window_ask_recorder.py            run forever (systemd pm-window-ask-recorder)
    pm_window_ask_recorder.py --once     sample the current offset once and exit
"""
import json
import sys
import time
from datetime import datetime, timezone

import psycopg2
import requests

DSN = dict(host="localhost", dbname="updown", user="updown",
           password=os.getenv("DB_PASSWORD", ""))
ASSETS = {"BTC": ("btc-updown-5m", "btc/usd"), "ETH": ("eth-updown-5m", "eth/usd")}
REF_MAX_AGE_S = 15      # a reference tick older than this is stored as NULL
WINDOW_S = 300
OFFSETS = tuple(range(30, 300, 30))           # T+30 .. T+270
DEPTH_BAND = 0.05                             # shares within 5c of the touch
LADDER_LEVELS = 5
GAMMA = "https://gamma-api.polymarket.com/markets/slug/"
CLOB = "https://clob.polymarket.com/book"
S = requests.Session()
S.headers["User-Agent"] = "pm-window-ask-recorder/1"

DDL = """
CREATE TABLE IF NOT EXISTS pm_window_ask_samples (
    id               bigserial PRIMARY KEY,
    asset            text NOT NULL,
    window_anchor    bigint NOT NULL,
    window_start_utc timestamptz NOT NULL,
    offset_s         integer NOT NULL,
    sampled_at       timestamptz NOT NULL,
    up_ask double precision, up_bid double precision,
    down_ask double precision, down_bid double precision,
    up_ask_sz double precision, down_ask_sz double precision,
    up_depth_5c double precision, down_depth_5c double precision,
    up_ladder text, down_ladder text,
    pm_price double precision, pm_ts timestamptz,
    cb_price double precision, cb_ts timestamptz,
    kr_price double precision, kr_ts timestamptz,
    UNIQUE (asset, window_anchor, offset_s)
);
ALTER TABLE pm_window_ask_samples ADD COLUMN IF NOT EXISTS cb_price double precision;
ALTER TABLE pm_window_ask_samples ADD COLUMN IF NOT EXISTS cb_ts timestamptz;
ALTER TABLE pm_window_ask_samples ADD COLUMN IF NOT EXISTS kr_price double precision;
ALTER TABLE pm_window_ask_samples ADD COLUMN IF NOT EXISTS kr_ts timestamptz;
CREATE INDEX IF NOT EXISTS pm_window_ask_samples_ws ON pm_window_ask_samples (window_start_utc);
"""

_tokens = {}      # (asset, anchor) -> (up_token, down_token)


def tokens(asset, anchor):
    key = (asset, anchor)
    if key not in _tokens:
        r = S.get(GAMMA + f"{ASSETS[asset][0]}-{anchor}", timeout=5)
        r.raise_for_status()
        m = r.json()
        outs = json.loads(m["outcomes"])
        ids = json.loads(m["clobTokenIds"])
        tk = dict(zip(outs, ids))
        _tokens[key] = (tk["Up"], tk["Down"])
        for k in [k for k in _tokens if k[1] < anchor - WINDOW_S]:
            del _tokens[k]
    return _tokens[key]


def book(token):
    r = S.get(CLOB, params={"token_id": token}, timeout=5)
    r.raise_for_status()
    b = r.json()
    asks = sorted((float(x["price"]), float(x["size"])) for x in b.get("asks", []))
    bids = sorted(((float(x["price"]), float(x["size"])) for x in b.get("bids", [])), reverse=True)
    if not asks:
        return dict(ask=None, bid=bids[0][0] if bids else None, sz=None, depth=None, ladder=None)
    a0 = asks[0][0]
    return dict(ask=a0, bid=bids[0][0] if bids else None, sz=asks[0][1],
                depth=sum(s for p, s in asks if p <= a0 + DEPTH_BAND + 1e-9),
                ladder=json.dumps(asks[:LADDER_LEVELS]))


def sample(cur, asset, anchor, off):
    up, dn = tokens(asset, anchor)
    bu, bd = book(up), book(dn)
    cur.execute("""SELECT benchmark_price, to_timestamp(observations_ts) FROM chainlink_prices
                    WHERE feed_id=%s AND observations_ts >= extract(epoch from now()) - %s
                    ORDER BY observations_ts DESC LIMIT 1""", (ASSETS[asset][1], REF_MAX_AGE_S))
    pm = cur.fetchone() or (None, None)
    ref = {}
    for src in ("coinbase", "kraken"):
        cur.execute("""SELECT price, to_timestamp(observations_ts) FROM pm_reference_ticks
                        WHERE source=%s AND asset=%s
                          AND observations_ts >= extract(epoch from now()) - %s
                        ORDER BY observations_ts DESC LIMIT 1""", (src, asset, REF_MAX_AGE_S))
        ref[src] = cur.fetchone() or (None, None)
    cur.execute("""INSERT INTO pm_window_ask_samples
        (asset, window_anchor, window_start_utc, offset_s, sampled_at, up_ask, up_bid, down_ask,
         down_bid, up_ask_sz, down_ask_sz, up_depth_5c, down_depth_5c, up_ladder, down_ladder,
         pm_price, pm_ts, cb_price, cb_ts, kr_price, kr_ts)
        VALUES (%s,%s,to_timestamp(%s),%s,now(),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (asset, window_anchor, offset_s) DO NOTHING""",
                (asset, anchor, anchor, off, bu["ask"], bu["bid"], bd["ask"], bd["bid"],
                 bu["sz"], bd["sz"], bu["depth"], bd["depth"], bu["ladder"], bd["ladder"],
                 pm[0], pm[1], *ref["coinbase"], *ref["kraken"]))


def log(msg):
    print(f"[{datetime.now(timezone.utc).isoformat(timespec='seconds')}] {msg}", flush=True)


def run(once=False):
    conn = psycopg2.connect(**DSN)
    conn.autocommit = True
    with conn.cursor() as c:
        c.execute(DDL)
    done = set()
    while True:
        now = time.time()
        anchor = int(now // WINDOW_S * WINDOW_S)
        el = now - anchor
        due = [o for o in OFFSETS if o <= el < o + 20 and (anchor, o) not in done]
        for off in due:
            for asset in ASSETS:
                try:
                    with conn.cursor() as c:
                        sample(c, asset, anchor, off)
                except Exception as e:
                    log(f"{asset} {anchor}+{off}: {type(e).__name__}: {e}")
                    if isinstance(e, psycopg2.Error) and conn.closed:
                        conn = psycopg2.connect(**DSN)
                        conn.autocommit = True
            done.add((anchor, off))
            if off == OFFSETS[-1]:
                log(f"window {anchor} sampled")
        done = {k for k in done if k[0] >= anchor - WINDOW_S}
        if once:
            if not due:
                off = max([o for o in OFFSETS if o <= el] or [OFFSETS[0]])
                with conn.cursor() as c:
                    for asset in ASSETS:
                        sample(c, asset, anchor, off)
                log(f"--once sampled {anchor}+{off}")
            return
        nxt = min([anchor + o for o in OFFSETS if anchor + o > now] + [anchor + WINDOW_S + OFFSETS[0]])
        time.sleep(max(0.2, min(5.0, nxt - time.time())))


if __name__ == "__main__":
    run(once="--once" in sys.argv)
