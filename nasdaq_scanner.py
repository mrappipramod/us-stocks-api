#!/usr/bin/env python3
"""
NASDAQ Stock Scanner
====================
Reads nasdaq-listed.csv, fetches live data via your Cloudflare proxy
(/api/yahoo), scores every stock, and saves results to JSON.

Designed to run on a schedule (cron / GitHub Actions / Cloudflare Worker cron).

Usage:
  python nasdaq_scanner.py                    # scan all common stocks
  python nasdaq_scanner.py --limit 100        # scan first 100 (testing)
  python nasdaq_scanner.py --sector tech      # filter by keyword in name
  python nasdaq_scanner.py --tickers AAPL,NVDA,MSFT  # specific tickers only
  python nasdaq_scanner.py --min-score 25     # only save BUY+ results
  python nasdaq_scanner.py --concurrency 5    # parallel workers (default 3)
"""

import csv
import json
import time
import math
import argparse
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

try:
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
except ImportError:
    print("Missing dependency. Run: pip install requests")
    sys.exit(1)

# ── CONFIG ────────────────────────────────────────────────────────────────────

PROXY_BASE   = os.getenv("PROXY_BASE", "https://champ.iamnewuser.com/api/yahoo")
OUTPUT_DIR   = Path(os.getenv("OUTPUT_DIR", "./scanner_output"))
CSV_PATH     = Path(os.getenv("CSV_PATH", "nasdaq-listed.csv"))
REQUEST_DELAY= float(os.getenv("REQUEST_DELAY", "0.4"))   # seconds between calls
TIMEOUT      = int(os.getenv("TIMEOUT", "15"))             # seconds per request
MAX_RETRIES  = int(os.getenv("MAX_RETRIES", "3"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("scanner")

# ── FILTER: only scan common stocks (skip warrants, units, ETFs, rights) ─────

SKIP_KEYWORDS = [
    "warrant", "unit", " right", " rights", " etf", "trust", "notes",
    "preferred", "debenture", "depositary", "adr", "acquisition corp",
    "spac", "blank check", "2x", "3x", "leverage", "inverse",
]

def is_common_stock(name: str) -> bool:
    nl = name.lower()
    if "common stock" not in nl and "ordinary share" not in nl:
        return False
    return not any(kw in nl for kw in SKIP_KEYWORDS)


# ── HTTP SESSION with retry ────────────────────────────────────────────────────

def make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=MAX_RETRIES,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.mount("http://",  HTTPAdapter(max_retries=retry))
    return s


# ── PROXY FETCH ────────────────────────────────────────────────────────────────

def fetch(session: requests.Session, symbol: str, type_: str, range_: str = None) -> Optional[dict]:
    params = {"symbol": symbol, "type": type_}
    if range_:
        params["range"] = range_
    try:
        r = session.get(PROXY_BASE, params=params, timeout=TIMEOUT)
        r.raise_for_status()
        data = r.json()
        if isinstance(data, dict) and data.get("error"):
            log.debug(f"{symbol} {type_}: proxy error — {data['error']}")
            return None
        return data
    except requests.exceptions.Timeout:
        log.warning(f"{symbol} {type_}: timeout")
        return None
    except requests.exceptions.HTTPError as e:
        log.debug(f"{symbol} {type_}: HTTP {e.response.status_code}")
        return None
    except Exception as e:
        log.debug(f"{symbol} {type_}: {e}")
        return None


# ── DATA EXTRACTION ────────────────────────────────────────────────────────────

def unwrap(obj: dict, key: str):
    """Unwrap Yahoo Finance { raw: N, fmt: '...' } objects."""
    if not obj:
        return None
    v = obj.get(key)
    if v is None:
        return None
    if isinstance(v, dict) and "raw" in v:
        return v["raw"]
    if isinstance(v, (int, float)):
        return v
    return None


def extract_chart(raw: dict) -> dict:
    """Extract price + OHLCV from ?type=chart response."""
    result = {}
    try:
        cr   = raw["chart"]["result"][0]
        meta = cr.get("meta", {})
        q    = cr.get("indicators", {}).get("quote", [{}])[0]
        ts   = cr.get("timestamp", [])
        closes = [v for v in (q.get("close") or []) if v is not None]
        highs  = q.get("high")  or []
        lows   = q.get("low")   or []
        vols   = [v for v in (q.get("volume") or []) if v is not None]

        price      = meta.get("regularMarketPrice")
        prev_close = meta.get("chartPreviousClose") or meta.get("previousClose")
        result = {
            "price":       price,
            "prev_close":  prev_close,
            "price_chg":   round(price - prev_close, 4) if price and prev_close else None,
            "pct_chg":     round((price - prev_close) / prev_close * 100, 3) if price and prev_close else None,
            "week52_high": meta.get("fiftyTwoWeekHigh"),
            "week52_low":  meta.get("fiftyTwoWeekLow"),
            "long_name":   meta.get("longName"),
            "currency":    meta.get("currency", "USD"),
            "closes":      closes,
            "highs":       highs,
            "lows":        lows,
            "timestamps":  ts,
            "avg_volume":  round(sum(vols) / len(vols)) if vols else None,
            "last_volume": vols[-1] if vols else None,
        }
    except (KeyError, IndexError, TypeError):
        pass
    return result


def extract_fundamentals(raw: dict) -> dict:
    """Extract fundamentals from ?type=fundamentals response."""
    result = {}
    if not raw:
        return result
    try:
        qsr = raw.get("quoteSummary", {}).get("result", [{}])[0] or {}
        sd  = qsr.get("summaryDetail", {})
        ks  = qsr.get("defaultKeyStatistics", {})
        fd  = qsr.get("financialData", {})
        ap  = qsr.get("assetProfile", {})
        rt  = qsr.get("recommendationTrend", {})
        t0  = (rt.get("trend") or [{}])[0]

        result = {
            "is_fallback":     bool(qsr.get("_fallback")),
            "company_name":    ap.get("longName"),
            "sector":          ap.get("sector"),
            "industry":        ap.get("industry"),
            "market_cap":      unwrap(sd, "marketCap"),
            "trailing_pe":     unwrap(sd, "trailingPE"),
            "forward_pe":      unwrap(sd, "forwardPE") or unwrap(ks, "forwardPE"),
            "peg_ratio":       unwrap(ks, "pegRatio"),
            "price_to_book":   unwrap(ks, "priceToBook"),
            "beta":            unwrap(sd, "beta"),
            "div_yield":       unwrap(sd, "dividendYield"),
            "eps":             unwrap(ks, "trailingEps"),
            "forward_eps":     unwrap(ks, "forwardEps"),
            # Growth & margins — stored as ratios (0-1)
            "revenue_growth":  unwrap(fd, "revenueGrowth"),
            "earnings_growth": unwrap(fd, "earningsGrowth"),
            "gross_margin":    unwrap(fd, "grossMargins"),
            "op_margin":       unwrap(fd, "operatingMargins"),
            "net_margin":      unwrap(fd, "profitMargins"),
            "roe":             unwrap(fd, "returnOnEquity"),
            "roa":             unwrap(fd, "returnOnAssets"),
            # Balance sheet
            "debt_to_equity":  unwrap(fd, "debtToEquity"),
            "current_ratio":   unwrap(fd, "currentRatio"),
            "free_cashflow":   unwrap(fd, "freeCashflow"),
            "total_cash":      unwrap(fd, "totalCash"),
            "target_price":    unwrap(fd, "targetMeanPrice"),
            # Analyst consensus
            "rec_buy":   (t0.get("strongBuy") or 0) + (t0.get("buy") or 0),
            "rec_hold":  t0.get("hold") or 0,
            "rec_sell":  (t0.get("strongSell") or 0) + (t0.get("sell") or 0),
        }
    except (KeyError, IndexError, TypeError):
        pass
    return result


# ── TECHNICAL INDICATORS ───────────────────────────────────────────────────────

def calc_sma(closes: list, period: int) -> Optional[float]:
    valid = [c for c in closes if c is not None]
    if len(valid) < period:
        return round(sum(valid) / len(valid), 4) if valid else None
    return round(sum(valid[-period:]) / period, 4)


def calc_rsi(closes: list, period: int = 14) -> Optional[float]:
    valid = [c for c in closes if c is not None]
    if len(valid) < period + 1:
        return None
    gains = losses = 0.0
    for i in range(len(valid) - period, len(valid)):
        diff = valid[i] - valid[i - 1]
        if diff >= 0:
            gains += diff
        else:
            losses += abs(diff)
    if losses == 0:
        return 100.0
    rs = (gains / period) / (losses / period)
    return round(100 - (100 / (1 + rs)), 2)


def calc_atr(highs: list, lows: list, closes: list, period: int = 14) -> Optional[float]:
    trs = []
    start = max(1, len(highs) - period * 2)
    for i in range(start, len(highs)):
        if any(v is None for v in [highs[i], lows[i]]) or i == 0 or closes[i-1] is None:
            continue
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i]  - closes[i - 1]),
        )
        trs.append(tr)
    if not trs:
        return None
    return round(sum(trs[-period:]) / min(len(trs), period), 4)


def calc_technicals(chart: dict) -> dict:
    closes = chart.get("closes", [])
    highs  = chart.get("highs", [])
    lows   = chart.get("lows", [])
    return {
        "ma_20":  calc_sma(closes, 20),
        "ma_50":  calc_sma(closes, 50),
        "ma_200": calc_sma(closes, 200),
        "rsi_14": calc_rsi(closes, 14),
        "atr_14": calc_atr(highs, lows, closes, 14),
    }


# ── SCORING ENGINE ─────────────────────────────────────────────────────────────

def score_stock(price, ma50, ma200, rsi14,
                trailing_pe, forward_pe, peg,
                rev_growth, eps_growth,
                gross_margin, op_margin,
                debt_to_equity, current_ratio, fcf, roe,
                rec_buy, rec_hold, rec_sell, target_price,
                is_fallback) -> dict:

    def clamp(v, lo, hi): return max(lo, min(hi, v))

    # TREND (±25/30)
    trend = 0
    if price and ma50:   trend += 10 if price > ma50  else -6
    if price and ma200:  trend += 12 if price > ma200 else -8
    if ma50 and ma200:   trend += 8  if ma50  > ma200 else -5
    trend = clamp(trend, -25, 30)

    # MOMENTUM via RSI (±10)
    mom = 0
    if rsi14 is not None:
        if   rsi14 < 30: mom = 8
        elif rsi14 < 45: mom = 5
        elif rsi14 < 60: mom = 6
        elif rsi14 < 70: mom = 3
        else:            mom = -6
    mom = clamp(mom, -10, 10)

    # VALUATION (±15)
    val = 0
    pe = forward_pe or trailing_pe
    if pe:
        if   pe < 15: val += 10
        elif pe < 25: val += 6
        elif pe < 35: val += 3
        elif pe < 50: val -= 2
        else:         val -= 8
    if peg:
        if   peg < 1: val += 5
        elif peg < 2: val += 2
        else:         val -= 3
    val = clamp(val, -15, 15)

    # GROWTH (±20) — values come in as ratios (0-1)
    gr = 0
    if rev_growth is not None:
        if   rev_growth > 0.30: gr += 10
        elif rev_growth > 0.15: gr += 7
        elif rev_growth > 0.05: gr += 4
        elif rev_growth > 0:    gr += 1
        else:                   gr -= 5
    if eps_growth is not None:
        if   eps_growth > 0.30: gr += 7
        elif eps_growth > 0.15: gr += 5
        elif eps_growth > 0:    gr += 2
        else:                   gr -= 4
    gr = clamp(gr, -10, 20)

    # QUALITY (±15)
    q = 0
    if gross_margin:
        if   gross_margin > 0.60: q += 6
        elif gross_margin > 0.40: q += 4
        elif gross_margin > 0.20: q += 2
    if op_margin:
        if   op_margin > 0.20: q += 5
        elif op_margin > 0.10: q += 3
        elif op_margin < 0:    q -= 5
    if debt_to_equity is not None:
        if   debt_to_equity < 50:  q += 4
        elif debt_to_equity < 150: q += 1
        elif debt_to_equity > 300: q -= 5
    if current_ratio is not None:
        if   current_ratio > 2: q += 3
        elif current_ratio > 1: q += 1
        else:                   q -= 4
    if fcf and fcf > 0: q += 3
    if roe and roe > 0.20: q += 3
    q = clamp(q, -10, 15)

    # ANALYST (±10)
    an = 0
    rec_total = (rec_buy or 0) + (rec_hold or 0) + (rec_sell or 0)
    if rec_total > 0:
        bull_pct = (rec_buy or 0) / rec_total
        if   bull_pct > 0.75: an = 8
        elif bull_pct > 0.60: an = 5
        elif bull_pct > 0.40: an = 2
        else:                 an = -3
    if target_price and price and target_price > price * 1.15:
        an += 2
    an = clamp(an, -5, 10)

    # FALLBACK PENALTY
    penalty = 0
    if is_fallback:
        val = 0; gr = 0; q = 0; an = 0
        trend = math.floor(trend * 0.6)
        penalty = -3

    total = trend + mom + val + gr + q + an + penalty

    if   total >= 45: verdict = "STRONG BUY"
    elif total >= 25: verdict = "BUY"
    elif total >= 0:  verdict = "HOLD"
    else:             verdict = "AVOID"

    conviction = max(1, min(10, round((total + 30) / 9)))
    ts = trend + mom
    trend_dir = "BULLISH" if ts > 12 else "BEARISH" if ts < -5 else "SIDEWAYS"

    return {
        "scores": {
            "trend": trend, "momentum": mom, "valuation": val,
            "growth": gr, "quality": q, "analyst": an,
            "data_penalty": penalty, "total": total,
        },
        "verdict":    verdict,
        "conviction": conviction,
        "trend_dir":  trend_dir,
    }


# ── ENTRY LEVELS ───────────────────────────────────────────────────────────────

def calc_entry_levels(price, ma50, ma200, closes, atr14, w52l) -> dict:
    if not price:
        return {"aggressive_entry": None, "conservative_entry": None, "stop_loss": None}

    # support zones from recent price history
    recent = [c for c in (closes or [])[-60:] if c]
    s1 = s2 = None
    if recent:
        srt = sorted(recent)
        s1 = round(srt[int(len(srt) * 0.18)], 2)
        s2 = round(srt[int(len(srt) * 0.35)], 2)

    agg_candidates = [v for v in [ma50, s1, price * 0.93] if v and v < price]
    agg = round(max(agg_candidates) if agg_candidates else price * 0.93, 2)

    con_candidates = [v for v in [ma200, s2, price * 0.82] if v and v < agg]
    con = round(max(con_candidates) if con_candidates else price * 0.82, 2)

    stop = round(min(con - (atr14 or price * 0.04) * 2, con * 0.95), 2)

    return {"aggressive_entry": agg, "conservative_entry": con, "stop_loss": stop}


# ── SCAN ONE TICKER ────────────────────────────────────────────────────────────

def scan_ticker(session: requests.Session, symbol: str, name: str) -> dict:
    t0 = time.time()

    chart_raw = fetch(session, symbol, "chart", "1y")
    time.sleep(REQUEST_DELAY)
    fund_raw  = fetch(session, symbol, "fundamentals")

    if not chart_raw:
        return {"symbol": symbol, "name": name, "error": "chart fetch failed", "scanned_at": now_iso()}

    chart = extract_chart(chart_raw)
    fund  = extract_fundamentals(fund_raw) if fund_raw else {"is_fallback": True}
    tech  = calc_technicals(chart)

    price    = chart.get("price")
    ma50     = tech.get("ma_50")
    ma200    = tech.get("ma_200")
    rsi14    = tech.get("rsi_14")
    atr14    = tech.get("atr_14")
    closes   = chart.get("closes", [])
    w52l     = chart.get("week52_low")

    scored = score_stock(
        price=price, ma50=ma50, ma200=ma200, rsi14=rsi14,
        trailing_pe=fund.get("trailing_pe"), forward_pe=fund.get("forward_pe"),
        peg=fund.get("peg_ratio"),
        rev_growth=fund.get("revenue_growth"), eps_growth=fund.get("earnings_growth"),
        gross_margin=fund.get("gross_margin"), op_margin=fund.get("op_margin"),
        debt_to_equity=fund.get("debt_to_equity"), current_ratio=fund.get("current_ratio"),
        fcf=fund.get("free_cashflow"), roe=fund.get("roe"),
        rec_buy=fund.get("rec_buy", 0), rec_hold=fund.get("rec_hold", 0),
        rec_sell=fund.get("rec_sell", 0), target_price=fund.get("target_price"),
        is_fallback=fund.get("is_fallback", True),
    )

    entries = calc_entry_levels(price, ma50, ma200, closes, atr14, w52l)

    return {
        "symbol":       symbol,
        "name":         name,
        "scanned_at":   now_iso(),
        "elapsed_s":    round(time.time() - t0, 2),
        # Price
        "price":        price,
        "price_chg":    chart.get("price_chg"),
        "pct_chg":      chart.get("pct_chg"),
        "week52_high":  chart.get("week52_high"),
        "week52_low":   chart.get("week52_low"),
        "currency":     chart.get("currency"),
        # Technicals
        "ma_20":        tech.get("ma_20"),
        "ma_50":        ma50,
        "ma_200":       ma200,
        "rsi_14":       rsi14,
        "atr_14":       atr14,
        "avg_volume":   chart.get("avg_volume"),
        # Fundamentals
        "company_name": fund.get("company_name") or chart.get("long_name") or name,
        "sector":       fund.get("sector"),
        "industry":     fund.get("industry"),
        "market_cap":   fund.get("market_cap"),
        "trailing_pe":  fund.get("trailing_pe"),
        "forward_pe":   fund.get("forward_pe"),
        "peg_ratio":    fund.get("peg_ratio"),
        "price_to_book":fund.get("price_to_book"),
        "beta":         fund.get("beta"),
        "div_yield":    fund.get("div_yield"),
        "eps":          fund.get("eps"),
        "revenue_growth":  pct_or_none(fund.get("revenue_growth")),
        "earnings_growth": pct_or_none(fund.get("earnings_growth")),
        "gross_margin":    pct_or_none(fund.get("gross_margin")),
        "op_margin":       pct_or_none(fund.get("op_margin")),
        "net_margin":      pct_or_none(fund.get("net_margin")),
        "roe":             pct_or_none(fund.get("roe")),
        "debt_to_equity":  fund.get("debt_to_equity"),
        "current_ratio":   fund.get("current_ratio"),
        "free_cashflow":   fund.get("free_cashflow"),
        "total_cash":      fund.get("total_cash"),
        "target_price":    fund.get("target_price"),
        "rec_buy":         fund.get("rec_buy"),
        "rec_hold":        fund.get("rec_hold"),
        "rec_sell":        fund.get("rec_sell"),
        "is_fallback":     fund.get("is_fallback"),
        # Scoring
        **scored,
        # Entry levels
        **entries,
    }


def pct_or_none(v):
    """Convert ratio (0-1) to percentage, rounded to 2dp."""
    return round(v * 100, 2) if v is not None else None


def now_iso():
    return datetime.now(timezone.utc).isoformat()


# ── LOAD CSV ───────────────────────────────────────────────────────────────────

def load_tickers(csv_path: Path, limit: int = None, sector_keyword: str = None,
                 specific: list = None) -> list[tuple[str, str]]:
    tickers = []
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            sym  = row.get("Symbol", "").strip()
            name = row.get("Security Name", "").strip()
            if not sym or not name:
                continue
            if specific:
                if sym in specific:
                    tickers.append((sym, name))
                continue
            if not is_common_stock(name):
                continue
            if sector_keyword and sector_keyword.lower() not in name.lower():
                continue
            tickers.append((sym, name))

    if limit:
        tickers = tickers[:limit]
    log.info(f"Loaded {len(tickers)} tickers from {csv_path}")
    return tickers


# ── MAIN SCAN ──────────────────────────────────────────────────────────────────

def run_scan(tickers: list[tuple[str, str]], concurrency: int = 3,
             min_score: int = None) -> dict:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    results      = []
    errors       = []
    done         = 0
    total        = len(tickers)
    scan_start   = time.time()

    log.info(f"Starting scan of {total} tickers | concurrency={concurrency} | proxy={PROXY_BASE}")

    # Use a thread pool — each thread gets its own session
    def worker(args):
        sym, name = args
        session = make_session()
        return scan_ticker(session, sym, name)

    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = {pool.submit(worker, t): t for t in tickers}
        for fut in as_completed(futures):
            done += 1
            result = fut.result()
            if result.get("error"):
                errors.append(result)
                log.warning(f"[{done}/{total}] ✕ {result['symbol']} — {result['error']}")
            else:
                results.append(result)
                verdict = result.get("verdict", "?")
                score   = result.get("scores", {}).get("total", "?")
                log.info(f"[{done}/{total}] {result['symbol']:8s} {verdict:10s} score={score:+d} "
                         f"price=${result.get('price') or 0:.2f}")

    elapsed = round(time.time() - scan_start, 1)

    # Apply min_score filter for the "top picks" output
    all_results = sorted(results, key=lambda r: r.get("scores", {}).get("total", -999), reverse=True)

    filtered = all_results
    if min_score is not None:
        filtered = [r for r in all_results if r.get("scores", {}).get("total", -999) >= min_score]

    # Summary stats
    verdicts = [r.get("verdict") for r in all_results]
    summary = {
        "scanned_at":     now_iso(),
        "elapsed_seconds": elapsed,
        "total_scanned":  len(all_results),
        "total_errors":   len(errors),
        "strong_buy":     verdicts.count("STRONG BUY"),
        "buy":            verdicts.count("BUY"),
        "hold":           verdicts.count("HOLD"),
        "avoid":          verdicts.count("AVOID"),
        "proxy_base":     PROXY_BASE,
    }

    output = {
        "meta":    summary,
        "results": all_results,   # all scanned stocks, sorted by score
        "errors":  errors,
    }

    # Save files
    ts        = datetime.now().strftime("%Y%m%d_%H%M%S")
    full_path = OUTPUT_DIR / f"scan_{ts}.json"
    latest    = OUTPUT_DIR / "latest.json"

    # Top picks (BUY or better) — smaller file for the web app to load
    top_picks = {
        "meta":    summary,
        "results": [r for r in all_results if r.get("verdict") in ("STRONG BUY", "BUY")],
    }
    top_path   = OUTPUT_DIR / f"top_picks_{ts}.json"
    top_latest = OUTPUT_DIR / "top_picks_latest.json"

    with open(full_path,   "w") as f: json.dump(output,    f, indent=2)
    with open(latest,      "w") as f: json.dump(output,    f, indent=2)
    with open(top_path,    "w") as f: json.dump(top_picks, f, indent=2)
    with open(top_latest,  "w") as f: json.dump(top_picks, f, indent=2)

    log.info(f"\n{'='*60}")
    log.info(f"Scan complete in {elapsed}s")
    log.info(f"Scanned:    {summary['total_scanned']} stocks")
    log.info(f"Errors:     {summary['total_errors']}")
    log.info(f"STRONG BUY: {summary['strong_buy']}")
    log.info(f"BUY:        {summary['buy']}")
    log.info(f"HOLD:       {summary['hold']}")
    log.info(f"AVOID:      {summary['avoid']}")
    log.info(f"Output:     {full_path}")
    log.info(f"Top picks:  {top_latest}")
    log.info(f"{'='*60}\n")

    return output


# ── CLI ────────────────────────────────────────────────────────────────────────

def main():
    global PROXY_BASE, OUTPUT_DIR, CSV_PATH  # must be first before any reference

    parser = argparse.ArgumentParser(description="NASDAQ Stock Scanner via /api/yahoo proxy")
    parser.add_argument("--csv",         default=str(CSV_PATH),  help="Path to nasdaq-listed.csv")
    parser.add_argument("--limit",       type=int,               help="Max tickers to scan (for testing)")
    parser.add_argument("--sector",      type=str,               help="Filter by keyword in company name")
    parser.add_argument("--tickers",     type=str,               help="Comma-separated specific tickers")
    parser.add_argument("--min-score",   type=int,               help="Min score to include in output")
    parser.add_argument("--concurrency", type=int, default=3,    help="Parallel workers (default 3)")
    parser.add_argument("--proxy",       type=str,               help="Override proxy base URL")
    parser.add_argument("--output-dir",  type=str,               help="Override output directory")
    args = parser.parse_args()

    if args.proxy:      PROXY_BASE  = args.proxy
    if args.output_dir: OUTPUT_DIR  = Path(args.output_dir)
    if args.csv:        CSV_PATH    = Path(args.csv)

    specific = [t.strip().upper() for t in args.tickers.split(",")] if args.tickers else None

    tickers = load_tickers(CSV_PATH, limit=args.limit, sector_keyword=args.sector, specific=specific)
    if not tickers:
        log.error("No tickers found. Check your CSV path and filters.")
        sys.exit(1)

    run_scan(tickers, concurrency=args.concurrency, min_score=args.min_score)


if __name__ == "__main__":
    main()
