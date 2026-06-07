#!/usr/bin/env python3
"""
tradeline-server.py - TradeLine live server.

Two live sources, one WebSocket out to the browser:
  * CRYPTO  - 10 symbols from Binance (kline -> candle, unchanged behaviour)
  * FOREX   - 16 major pairs from London Strategic Edge (lse-data).
              LSE sends *ticks* (symbol + price), so we aggregate them into
              hourly OHLC candles here.

Both relay to browsers on ws://<server>:8000/ws as:
   {"type":"candle","symbol":"BTC/USD","candle":{t,o,h,l,c,v,closed}}
Closed candles are saved to Supabase (crypto_candles for crypto,
forex_candles for forex).

Requires:
  pip install --break-system-packages fastapi "uvicorn[standard]" websockets requests lse-data
"""
import asyncio
import json
import os
import time

import requests
import websockets
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

# ---- secrets from environment (set these on the server with `export`) ----
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
LSE_API_KEY = os.environ.get("LSE_API_KEY")

# Binance crypto symbol -> display
CRYPTO = {
    "btcusdt": "BTC/USD", "ethusdt": "ETH/USD", "solusdt": "SOL/USD",
    "xrpusdt": "XRP/USD", "bnbusdt": "BNB/USD", "adausdt": "ADA/USD",
    "dogeusdt": "DOGE/USD", "avaxusdt": "AVAX/USD", "dotusdt": "DOT/USD",
    "linkusdt": "LINK/USD",
}
# 16 major forex pairs (LSE display symbols)
FOREX = [
    "EUR/USD", "GBP/USD", "USD/JPY", "USD/CHF", "AUD/USD", "USD/CAD",
    "NZD/USD", "EUR/GBP", "EUR/JPY", "GBP/JPY", "CHF/JPY", "AUD/JPY",
    "CAD/JPY", "EUR/CHF", "EUR/AUD", "GBP/CHF",
]
TF = "1h"
HOUR = 3600
BINANCE_WS = "wss://stream.binance.com:9443/stream?streams=" + "/".join(
    f"{s}@kline_{TF}" for s in CRYPTO)

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["*"], allow_headers=["*"])

clients = set()
last_saved = {}                 # display symbol -> last saved candle open time
fx_candles = {}                 # display symbol -> current building candle


def save_to_supabase(table, disp, candle):
    try:
        r = requests.post(
            f"{SUPABASE_URL}/rest/v1/{table}",
            headers={
                "apikey": SUPABASE_KEY,
                "Authorization": f"Bearer {SUPABASE_KEY}",
                "Content-Type": "application/json",
                "Prefer": "resolution=merge-duplicates",
            },
            json={
                "symbol": disp, "timeframe": TF, "timestamp": candle["t"],
                "open": candle["o"], "high": candle["h"], "low": candle["l"],
                "close": candle["c"], "volume": candle.get("v", 0),
            },
            timeout=10,
        )
        if r.status_code not in (200, 201, 204):
            print("Supabase save failed:", table, disp, r.status_code, r.text[:100])
        else:
            print(f"  saved {disp} -> {table} @ {candle['t']} close={candle['c']}")
    except Exception as e:
        print("Supabase error:", disp, e)


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    clients.add(ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        clients.discard(ws)


async def broadcast(msg):
    dead = []
    text = json.dumps(msg)
    for ws in clients:
        try:
            await ws.send_text(text)
        except Exception:
            dead.append(ws)
    for ws in dead:
        clients.discard(ws)


# ---------- CRYPTO: Binance klines ----------
async def relay_crypto():
    while True:
        try:
            print(f"[crypto] connecting to Binance ({len(CRYPTO)} symbols)...")
            async with websockets.connect(BINANCE_WS, ping_interval=20) as bw:
                print("[crypto] streaming:", ", ".join(CRYPTO.values()))
                async for raw in bw:
                    data = json.loads(raw).get("data", {})
                    k = data.get("k")
                    if not k:
                        continue
                    disp = CRYPTO.get(k["s"].lower())
                    if not disp:
                        continue
                    candle = {
                        "t": int(k["t"] // 1000),
                        "o": float(k["o"]), "h": float(k["h"]),
                        "l": float(k["l"]), "c": float(k["c"]),
                        "v": float(k["v"]), "closed": k["x"],
                    }
                    await broadcast({"type": "candle", "symbol": disp, "candle": candle})
                    if k["x"] and last_saved.get(disp) != candle["t"]:
                        save_to_supabase("crypto_candles", disp, candle)
                        last_saved[disp] = candle["t"]
        except Exception as e:
            print("[crypto] dropped, reconnecting in 3s:", e)
            await asyncio.sleep(3)


# ---------- FOREX: LSE ticks -> hourly candles ----------
def on_fx_tick(disp, price):
    """Aggregate a tick into the current hourly candle for `disp`."""
    now = int(time.time())
    bucket = now - (now % HOUR)          # start of the current hour
    cur = fx_candles.get(disp)
    closed_candle = None
    if cur is None or cur["t"] != bucket:
        # close the previous candle (if any) before starting a new one
        if cur is not None:
            closed_candle = dict(cur, closed=True)
        cur = {"t": bucket, "o": price, "h": price, "l": price, "c": price, "v": 0, "closed": False}
        fx_candles[disp] = cur
    else:
        cur["h"] = max(cur["h"], price)
        cur["l"] = min(cur["l"], price)
        cur["c"] = price
    return cur, closed_candle


async def relay_forex(loop):
    if not LSE_API_KEY:
        print("[forex] LSE_API_KEY not set - skipping forex feed")
        return
    while True:
        try:
            import lse
            print(f"[forex] connecting to LSE ({len(FOREX)} pairs)...")
            client = lse.LSE(api_key=LSE_API_KEY)
            # lse-data streams ticks; tick.symbol / tick.price (per their docs)
            for tick in client.stream(FOREX):
                disp = getattr(tick, "symbol", None)
                price = getattr(tick, "price", None)
                if disp is None or price is None:
                    continue
                price = float(price)
                cur, closed = on_fx_tick(disp, price)
                # relay the live (building) candle
                fut = asyncio.run_coroutine_threadsafe(
                    broadcast({"type": "candle", "symbol": disp, "candle": dict(cur)}), loop)
                try:
                    fut.result(timeout=2)
                except Exception:
                    pass
                # save the just-closed candle once
                if closed and last_saved.get(disp) != closed["t"]:
                    save_to_supabase("forex_candles", disp, closed)
                    last_saved[disp] = closed["t"]
        except Exception as e:
            print("[forex] dropped, reconnecting in 5s:", e)
            time_sleep(5)


def time_sleep(n):
    import time as _t
    _t.sleep(n)


@app.on_event("startup")
async def startup():
    loop = asyncio.get_event_loop()
    asyncio.create_task(relay_crypto())
    # forex client is blocking/iterator-based -> run in a thread
    loop.run_in_executor(None, lambda: relay_forex(loop))


if __name__ == "__main__":
    import uvicorn
    print("TradeLine server on http://0.0.0.0:8000")
    uvicorn.run(app, host="0.0.0.0", port=8000)
