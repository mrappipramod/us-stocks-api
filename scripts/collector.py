#!/usr/bin/env python3
"""
Collector for US stock data (all stocks or selected list).
Uses yfinance and a daily-updated ticker list from GitHub.
"""

import yfinance as yf
import json
import os
import time
import pandas as pd
from datetime import datetime

# ================== CONFIGURATION ==================

DATA_DIR = "data"                     # Where JSON files will be stored
HISTORY_PERIOD = "1mo"                # Historical data period: 1mo, 6mo, 1y, etc.
REQUEST_DELAY = 0.3                   # Seconds between API calls
MAX_RETRIES = 3

# URL for the ticker list (all US stocks)
TICKER_LIST_URL = "https://raw.githubusercontent.com/Ate329/top-us-stock-tickers/main/tickers/all.csv"

# Optional: filter by exchange (e.g., "NASDAQ", "NYSE", "AMEX")
# Set to None to include all.
EXCHANGE_FILTER = None                # e.g., "NASDAQ"

# ===================================================

def ensure_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)

def get_tickers():
    """Download and return the list of ticker symbols."""
    try:
        df = pd.read_csv(TICKER_LIST_URL)
        if EXCHANGE_FILTER:
            # Assumes CSV has a column like 'exchange' or 'primaryExchange'
            # The top-us-stock-tickers CSV has 'exchange' column.
            if 'exchange' in df.columns:
                df = df[df['exchange'] == EXCHANGE_FILTER]
            else:
                print("Warning: 'exchange' column not found; ignoring filter.")
        tickers = df['symbol'].dropna().tolist()
        print(f"✅ Loaded {len(tickers)} tickers from {TICKER_LIST_URL}")
        return tickers
    except Exception as e:
        print(f"❌ Failed to load ticker list: {e}")
        # Fallback to S&P 500 list
        fallback_url = "https://raw.githubusercontent.com/Ate329/top-us-stock-tickers/main/tickers/sp500.csv"
        df = pd.read_csv(fallback_url)
        return df['symbol'].dropna().tolist()

def fetch_stock_data(symbol):
    """Fetch quote and historical data for a single symbol."""
    for attempt in range(MAX_RETRIES):
        try:
            stock = yf.Ticker(symbol)
            info = stock.info

            quote = {
                "symbol": symbol,
                "price": info.get("regularMarketPrice"),
                "change": info.get("regularMarketChange"),
                "changePercent": info.get("regularMarketChangePercent"),
                "volume": info.get("regularMarketVolume"),
                "marketCap": info.get("marketCap"),
                "pe": info.get("trailingPE"),
                "high": info.get("dayHigh"),
                "low": info.get("dayLow"),
                "open": info.get("regularMarketOpen"),
                "prevClose": info.get("previousClose"),
                "timestamp": info.get("regularMarketTime"),
                "updated": datetime.now().isoformat()
            }

            # Historical OHLCV
            hist = stock.history(period=HISTORY_PERIOD)
            if hist.empty:
                history = []
            else:
                hist = hist.reset_index()
                hist['Date'] = hist['Date'].dt.isoformat()
                history = hist.to_dict(orient="records")

            return {"quote": quote, "history": history}

        except Exception as e:
            print(f"   Attempt {attempt+1}/{MAX_RETRIES} failed for {symbol}: {e}")
            time.sleep(2 ** attempt)   # exponential backoff

    print(f"   ⚠️  Skipping {symbol} after {MAX_RETRIES} retries")
    return None

def save_data(symbol, data):
    """Save a stock's data to a JSON file."""
    if not data:
        return
    ensure_dir(DATA_DIR)
    filename = os.path.join(DATA_DIR, f"{symbol}.json")
    with open(filename, "w") as f:
        json.dump(data, f, indent=2)

def collect_all():
    """Main collection loop."""
    print(f"🚀 Starting collection at {datetime.now()}")
    tickers = get_tickers()
    print(f"📋 Will process {len(tickers)} stocks.")

    success = 0
    for i, symbol in enumerate(tickers):
        print(f"  [{i+1}/{len(tickers)}] Fetching {symbol}...")
        data = fetch_stock_data(symbol)
        if data:
            save_data(symbol, data)
            success += 1
        time.sleep(REQUEST_DELAY)

    print(f"✅ Done. Collected data for {success} / {len(tickers)} stocks.")

if __name__ == "__main__":
    collect_all()
