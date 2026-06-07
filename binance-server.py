#!/usr/bin/env python3
"""
binance-server.py - TradeLine live BTC server.

What it does, all in one always-on process:
  1. Connects to Binance's live BTC/USDT 1h kline WebSocket.
  2. Saves every candle update into your Supabase `crypto_candles` table.
  3. Relays every candle to any browser connected on ws://localhost:8000/ws.

Run it once and leave it running. It saves 24/7 whether your browser is
open or not - that's the whole point of doing the saving here and not in
the browser.

SETUP (one time):
    pip install fastapi "uvicorn[standard]" websockets requests

RUN:
    python binance-server.py

Then open the chart at http://localhost:3000/live-chart-final.html
(see the README for the http.server step).
"""
import asyncio
import json

import requests
import websockets
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

SUPABASE_URL = os.environ.get('SUPABASE_URL')# ---- your Supabase details (already filled in from your project) ----

SUPABASE_KEY = os.environ.get('SUPABASE_KEY')

SYMBOL = "BTCUSDT"
TF = "1h"
BINANCE_WS = f"wss://stream.binance.com:9443/ws/{SYMBOL.lower()}@kline_{TF}"

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["*"], allow_headers=["*"])

clients: set = set()


# ---- save one candle to Supabase ----
def save_to_supabase(candle):
    try:
        r = requests.post(
            f"{SUPABASE_URL}/rest/v1/crypto_candles",
            headers={
                "apikey": SUPABASE_KEY,
                "Authorization": f"Bearer {SUPABASE_KEY}",
                "Content-Type": "application/json",
                "Prefer": "resolution=merge-duplicates",  # upsert, no dupes
            },
            json={
                "symbol": "BTC/USD",
                "timeframe": TF,
                "timestamp": candle["t"],
                "open": candle["o"],
                "high": candle["h"],
                "low": candle["l"],
                "close": candle["c"],
                "volume": candle["v"],
            },
            timeout=10,
        )
        if r.status_code not in (200, 201, 204):
            print("Supabase save failed:", r.status_code, r.text[:200])
        else:
            print(f"  saved candle @ {candle['t']}  close={candle['c']}")
    except Exception as e:
        print("Supabase error:", e)


# ---- browser stream ----
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
    for ws in clients:
        try:
            await ws.send_text(json.dumps(msg))
        except Exception:
            dead.append(ws)
    for ws in dead:
        clients.discard(ws)


# ---- Binance live relay ----
async def relay():
    # only write a closed candle to the DB once
    last_saved_open = None
    while True:
        try:
            print("Connecting to Binance...")
            async with websockets.connect(BINANCE_WS, ping_interval=20) as bw:
                print("Connected. Streaming BTC/USDT 1h.")
                async for raw in bw:
                    k = json.loads(raw).get("k")
                    if not k:
                        continue
                    candle = {
                        "t": int(k["t"] // 1000),  # open time, seconds
                        "o": float(k["o"]), "h": float(k["h"]),
                        "l": float(k["l"]), "c": float(k["c"]),
                        "v": float(k["v"]), "closed": k["x"],
                    }
                    # push live to browsers every update
                    await broadcast({"type": "candle", "candle": candle})
                    # save to DB once per candle (when it closes)
                    if k["x"] and candle["t"] != last_saved_open:
                        save_to_supabase(candle)
                        last_saved_open = candle["t"]
        except Exception as e:
            print("Binance dropped, reconnecting in 3s:", e)
            await asyncio.sleep(3)


@app.on_event("startup")
async def startup():
    asyncio.create_task(relay())


if __name__ == "__main__":
    import uvicorn
    print("TradeLine server starting on http://localhost:8000")
    uvicorn.run(app, host="0.0.0.0", port=8000)
