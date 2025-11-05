# worker.py
"""
Worker that either:
- connects to Polygon WebSocket (if POLYGON_API_KEY set) and subscribes to trade/quote channels for symbols in WATCHLIST
- OR runs a simulator that generates fake ticks for WATCHLIST

Publishes:
- Redis key `latest:{SYMBOL}` -> JSON payload of last tick
- Redis list `ticks:{SYMBOL}` -> LPUSH of recent ticks (trim to 200)
"""

import os
import time
import json
import random
import redis
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
POLY_KEY = os.getenv("POLYGON_API_KEY")
WATCHLIST = [s.strip().upper() for s in os.getenv("WATCHLIST", "AAPL,MSFT,TSLA,RELIANCE.NS").split(",")]
TICK_LIST_LEN = int(os.getenv("TICK_LIST_LEN", "200"))

print("Worker starting. Redis:", REDIS_URL, "POLY_KEY set:", bool(POLY_KEY))
r = redis.from_url(REDIS_URL, decode_responses=True)

def publish_tick(sym, price, size=1, ev_type="T"):
    payload = {
        "symbol": sym,
        "price": float(price),
        "size": int(size),
        "ts": datetime.utcnow().isoformat() + "Z",
        "type": ev_type
    }
    key = f"latest:{sym}"
    listkey = f"ticks:{sym}"
    r.set(key, json.dumps(payload))
    r.lpush(listkey, json.dumps(payload))
    r.ltrim(listkey, 0, TICK_LIST_LEN-1)

def simulate_ticks():
    # simple random walk for each symbol
    base = {}
    for s in WATCHLIST:
        base[s] = 100.0 + random.random()*100
    while True:
        for s in WATCHLIST:
            last = base[s]
            # small random change
            change_pct = random.uniform(-0.01, 0.01)
            new = last * (1 + change_pct)
            base[s] = new
            publish_tick(s, round(new, 2), size=random.randint(1,100))
        time.sleep(1)

# Polygon WebSocket client path
if POLY_KEY:
    try:
        import websocket
        def on_open(ws):
            print("WS open; authenticating")
            ws.send(json.dumps({"action":"auth","params":POLY_KEY}))
            # subscribe to trade ticks for each symbol (T.<SYMBOL>)
            for s in WATCHLIST:
                # Polygon trades channel prefix is 'T.'; quotes 'Q.'
                sub = f"T.{s}"
                print("subscribing to", sub)
                ws.send(json.dumps({"action":"subscribe","params": sub}))

        def on_message(ws, message):
            try:
                data = json.loads(message)
                # data is often a list of messages
                if isinstance(data, list):
                    for msg in data:
                        # handle trade messages with ev field 'ev' == 'T'
                        ev_type = msg.get("ev")
                        if ev_type == "T":
                            sym = msg.get("sym") or msg.get("ticker")
                            price = msg.get("p") or msg.get("price")
                            size = msg.get("s") or msg.get("size") or 1
                            publish_tick(sym, price, size=size, ev_type="T")
                elif isinstance(data, dict):
                    # one-off dict responses
                    pass
            except Exception as e:
                print("msg parse err", e)

        def on_error(ws, err):
            print("WS error", err)

        def on_close(ws, code, reason):
            print("WS closed", code, reason)

        ws_url = "wss://socket.polygon.io/stocks"
        print("Connecting to Polygon WebSocket...")
        ws = websocket.WebSocketApp(ws_url, on_open=on_open, on_message=on_message, on_error=on_error, on_close=on_close)
        ws.run_forever()
    except Exception as e:
        print("Polygon WS failed or websocket-client not installed:", e)
        print("Falling back to simulator.")
        simulate_ticks()
else:
    # no API key -> run simulator
    simulate_ticks()
