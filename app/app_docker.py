# app_docker.py
"""
Streamlit app that reads latest ticks from Redis (published by worker).
Fallback to yfinance if Redis / feed not available.
Run inside Docker via docker-compose.
"""

import streamlit as st
import redis
import os
import json
import time
import pandas as pd
from datetime import datetime
import yfinance as yf

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
WATCHLIST = os.getenv("WATCHLIST", "AAPL,MSFT,TSLA,RELIANCE.NS").split(",")

st.set_page_config(page_title="Bloomberg-Lite (Docker)", layout="wide")
st.title("Bloomberg-Lite — Dockerized (dev)")

# connect to redis
try:
    r = redis.from_url(REDIS_URL, decode_responses=True)
    r.ping()
    redis_ok = True
except Exception as e:
    r = None
    redis_ok = False
    st.warning(f"Redis not available: {e}. App will use yfinance fallback.")

def get_latest_from_redis(symbol):
    """Expect worker to set a JSON string at key latest:{SYMBOL}"""
    key = f"latest:{symbol.upper()}"
    try:
        payload = r.get(key)
        if payload:
            return json.loads(payload)
    except Exception:
        return None
    return None

def fallback_yfinance(symbol):
    try:
        t = yf.Ticker(symbol)
        info = t.info
        hist = t.history(period="5d")
        last = None
        if not hist.empty:
            last = float(hist["Close"].iloc[-1])
        return {"source": "yfinance", "last": last, "info": {"shortName": info.get("shortName")}}
    except Exception:
        return {"source": "yfinance", "last": None, "info": {}}

# UI controls
st.sidebar.header("Controls")
live = st.sidebar.button("Start Live (poll Redis)")
stop = st.sidebar.button("Stop Live")
st.sidebar.write("WATCHLIST from env:", WATCHLIST)

# show market tape
st.subheader("Market Tape (latest ticks)")
tape_container = st.container()

def render_tape():
    rows = []
    for sym in WATCHLIST:
        sym = sym.strip().upper()
        entry = None
        if redis_ok:
            entry = get_latest_from_redis(sym)
        if not entry:
            entry = fallback_yfinance(sym)
        last = entry.get("last")
        src = entry.get("source", "unknown")
        rows.append({"symbol": sym, "last": last, "source": src})
    df = pd.DataFrame(rows)
    tape_container.table(df)

# live loop control via session state
if "live_running" not in st.session_state:
    st.session_state.live_running = False

if live:
    st.session_state.live_running = True

if stop:
    st.session_state.live_running = False

# single render or live loop
render_tape()
if st.session_state.live_running:
    st.info("Live polling Redis — updating every 1s. Press Stop Live to stop.")
    # live update loop (local dev only) — stops when user clicks Stop or interrupts
    progress = st.empty()
    while st.session_state.live_running:
        render_tape()
        progress.markdown(f"Last update: {datetime.utcnow().isoformat()}Z")
        time.sleep(1)
        # Streamlit will check session state changes on interaction (stop button)
        # Note: heavy loops are for local dev only
    progress.empty()

# small history view per symbol (from Redis list 'ticks:{SYMBOL}' or yfinance)
st.markdown("---")
st.subheader("Recent tick samples (per symbol)")

sym = st.selectbox("Choose symbol", options=[s.strip().upper() for s in WATCHLIST])
sample_box = st.empty()

# try read last 20 ticks from Redis list
if redis_ok:
    try:
        ticks = r.lrange(f"ticks:{sym}", 0, 19)
        ticks = [json.loads(x) for x in ticks] if ticks else []
        df = pd.DataFrame(ticks)
        if not df.empty:
            sample_box.dataframe(df)
        else:
            sample_box.info("No recent ticks in Redis for this symbol. Worker may not be publishing yet.")
    except Exception as e:
        sample_box.error(f"Error reading ticks: {e}")
else:
    # fallback: show last 10 rows of yfinance history
    t = yf.Ticker(sym)
    hist = t.history(period="1mo")
    sample_box.dataframe(hist.tail(10))
