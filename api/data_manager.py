"""QuantX Data Manager — Waterfall data fetcher for backtests.
Priority: Local SQLite → IBKR → LongPort → Cloudflare R2 → Yahoo → FMP
"""

import os
import json
import sqlite3
import logging
from datetime import datetime, timedelta
from typing import Optional

log = logging.getLogger("quantx-data")

SOURCE_MESSAGES = {
    "local_cache": "Loading from local cache...",
    "ibkr": "Fetching from your IBKR account...",
    "longport": "Fetching from your LongPort account...",
    "r2": "Loading from QuantX data library...",
    "yahoo": "Fetching from Yahoo Finance...",
    "fmp": "Fetching from QuantX data server...",
    "none": "No data available for this symbol/timeframe.",
}

CACHE_TTL = {
    "1min": 3600, "5min": 3600, "15min": 14400, "30min": 14400,
    "1hour": 86400, "4hour": 86400, "1day": 86400 * 7, "1week": 86400 * 30,
}


# ── Local SQLite cache ────────────────────────────────────────────────────────

def init_data_cache(conn: sqlite3.Connection):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS data_cache (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            source TEXT NOT NULL,
            bar_count INTEGER DEFAULT 0,
            bars_json TEXT NOT NULL,
            fetched_at TEXT DEFAULT (datetime('now')),
            UNIQUE(symbol, timeframe)
        );
        CREATE INDEX IF NOT EXISTS idx_data_cache_sym ON data_cache(symbol, timeframe);
    """)
    conn.commit()


def load_from_local_cache(db_path, symbol, timeframe):
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT bars_json, source, fetched_at FROM data_cache WHERE symbol=? AND timeframe=?",
            (symbol, timeframe)).fetchone()
        conn.close()
        if not row:
            return None
        fetched_at = datetime.fromisoformat(row["fetched_at"])
        ttl = CACHE_TTL.get(timeframe, 86400)
        if (datetime.utcnow() - fetched_at).total_seconds() > ttl:
            log.info("Cache expired for %s/%s", symbol, timeframe)
            return None
        bars = json.loads(row["bars_json"])
        log.info("Cache hit: %s/%s — %d bars from %s", symbol, timeframe, len(bars), row["source"])
        return bars, "local_cache"
    except Exception as e:
        log.warning("Local cache read failed: %s", e)
        return None


def save_to_local_cache(db_path, symbol, timeframe, bars, source):
    try:
        conn = sqlite3.connect(db_path)
        conn.execute(
            """INSERT INTO data_cache (symbol, timeframe, source, bar_count, bars_json, fetched_at)
               VALUES (?, ?, ?, ?, ?, datetime('now'))
               ON CONFLICT(symbol, timeframe) DO UPDATE SET
                 source=excluded.source, bar_count=excluded.bar_count,
                 bars_json=excluded.bars_json, fetched_at=excluded.fetched_at""",
            (symbol, timeframe, source, len(bars), json.dumps(bars)))
        conn.commit()
        conn.close()
        log.info("Saved %d bars for %s/%s to local cache (source: %s)", len(bars), symbol, timeframe, source)
    except Exception as e:
        log.warning("Local cache write failed: %s", e)


# ── IBKR data fetch ──────────────────────────────────────────────────────────

def fetch_from_ibkr(symbol, timeframe, limit, host="127.0.0.1", port=7497, client_id=99):
    try:
        import asyncio
        try:
            asyncio.get_event_loop()
        except RuntimeError:
            asyncio.set_event_loop(asyncio.new_event_loop())
        from ib_insync import IB, Stock

        tf_map = {
            "1min": ("1 min", "1 D"), "5min": ("5 mins", "5 D"),
            "15min": ("15 mins", "10 D"), "30min": ("30 mins", "20 D"),
            "1hour": ("1 hour", "30 D"), "4hour": ("4 hours", "60 D"),
            "1day": ("1 day", "5 Y"), "1week": ("1 week", "10 Y"),
        }
        bar_size, duration = tf_map.get(timeframe, ("1 day", "5 Y"))
        if timeframe == "1day":
            duration = f"{max(1, min(20, limit // 252 + 1))} Y"

        ib = IB()
        ib.connect(host, port, clientId=client_id, timeout=10)
        if not ib.isConnected():
            return None

        if symbol.endswith(".HK"):
            contract = Stock(symbol.replace(".HK", ""), "SEHK", "HKD")
        elif symbol.endswith(".SI"):
            contract = Stock(symbol.replace(".SI", ""), "SGX", "SGD")
        elif symbol.endswith(".US"):
            contract = Stock(symbol.replace(".US", ""), "SMART", "USD")
        else:
            contract = Stock(symbol, "SMART", "USD")

        ib.qualifyContracts(contract)
        raw = ib.reqHistoricalData(contract, endDateTime="", durationStr=duration,
                                    barSizeSetting=bar_size, whatToShow="MIDPOINT",
                                    useRTH=True, formatDate=1)
        ib.disconnect()
        if not raw:
            return None
        bars = [{"date": str(b.date), "open": float(b.open), "high": float(b.high),
                 "low": float(b.low), "close": float(b.close),
                 "volume": float(b.volume) if b.volume != -1 else 0} for b in raw]
        if len(bars) > limit:
            bars = bars[-limit:]
        log.info("IBKR: %d bars for %s/%s", len(bars), symbol, timeframe)
        return bars
    except Exception as e:
        log.warning("IBKR fetch failed %s/%s: %s", symbol, timeframe, e)
        return None


# ── Yahoo Finance ─────────────────────────────────────────────────────────────

def fetch_from_yahoo(symbol, timeframe, limit):
    import requests
    from datetime import datetime, timezone
    try:
        yf_sym = symbol
        if symbol.endswith(".HK"):
            code = symbol.replace(".HK", "").zfill(4)
            yf_sym = f"{code}.HK"
        elif symbol.endswith(".US"):
            yf_sym = symbol.replace(".US", "")

        interval_map = {"1min": "1m", "5min": "5m", "15min": "15m", "30min": "30m",
                        "1hour": "1h", "4hour": "1h", "1day": "1d", "1week": "1wk"}
        interval = interval_map.get(timeframe, "1d")

        if timeframe in ("1min", "5min", "15min", "30min"):
            range_val = "5d"
        elif timeframe in ("1hour", "4hour"):
            range_val = "730d"
        else:
            years = max(1, min(25, limit // 252 + 1))
            range_val = f"{years}y"

        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{yf_sym}?range={range_val}&interval={interval}"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json',
            'Connection': 'keep-alive'
        }
        r = requests.get(url, headers=headers, timeout=15)
        if r.status_code != 200:
            log.warning("Yahoo API returned status %d for %s", r.status_code, yf_sym)
            return None
        
        data = r.json()
        if not data.get("chart") or not data["chart"].get("result"):
            return None
            
        result = data["chart"]["result"][0]
        timestamps = result.get("timestamp", [])
        if not timestamps:
            return None
            
        indicators = result.get("indicators", {})
        quote = indicators.get("quote", [{}])[0]
        adjclose_list = indicators.get("adjclose", [{}])[0].get("adjclose", [])
        
        bars = []
        for i in range(len(timestamps)):
            try:
                ts = timestamps[i]
                dt = datetime.fromtimestamp(ts, timezone.utc)
                date_str = dt.strftime('%Y-%m-%d %H:%M:%S' if interval != '1d' and interval != '1wk' else '%Y-%m-%d')
                
                o = quote.get("open", [])[i]
                h = quote.get("high", [])[i]
                l = quote.get("low", [])[i]
                c = quote.get("close", [])[i]
                v = quote.get("volume", [])[i]
                
                if o is None or h is None or l is None or c is None:
                    continue
                
                if adjclose_list and i < len(adjclose_list) and adjclose_list[i] is not None and c > 0:
                    factor = adjclose_list[i] / c
                    o = round(float(o * factor), 4)
                    h = round(float(h * factor), 4)
                    l = round(float(l * factor), 4)
                    c = round(float(adjclose_list[i]), 4)
                else:
                    o = round(float(o), 4)
                    h = round(float(h), 4)
                    l = round(float(l), 4)
                    c = round(float(c), 4)
                
                bars.append({
                    "date": date_str,
                    "open": o,
                    "high": h,
                    "low": l,
                    "close": c,
                    "volume": float(v) if v is not None else 0.0
                })
            except Exception:
                continue
                
        if len(bars) > limit:
            bars = bars[-limit:]
            
        log.info("Yahoo Chart API: %d bars for %s/%s (yf: %s)", len(bars), symbol, timeframe, yf_sym)
        return bars
    except Exception as e:
        log.warning("Yahoo fetch failed %s/%s: %s", symbol, timeframe, e)
        return None


# ── LongPort data fetch ───────────────────────────────────────────────────────

# LP bars per API call (hard limit)
_LP_PAGE_SIZE = 1000

# Maximum bars we'll paginate for, per timeframe — keeps fetch time reasonable
_LP_MAX_BARS = {
    "1min": 5000, "5min": 10000, "15min": 10000, "30min": 3000,
    "1hour": 3000, "4hour": 3000, "1day": 5500, "1week": 1000,
}

def fetch_from_longport(symbol: str, timeframe: str, limit: int,
                        app_key: str, app_secret: str, access_token: str,
                        _get_ctx_fn=None):
    """
    Fetch OHLCV bars from LongPort.
    _get_ctx_fn: optional callable(app_key, secret, token) -> pooled QuoteContext.
    When provided, reuses an existing connected context (no handshake overhead).
    When None, creates a fresh context and deletes it in finally.
    Strategy:
    - limit <= 1000: single candlesticks() call (~2s)
    - limit > 1000:  paginated history_candlesticks_by_offset
    """
    try:
        from longport.openapi import QuoteContext, Config, Period, AdjustType
    except ImportError:
        log.warning("longport SDK not installed — skipping LP fetch")
        return None

    period_map = {
        "1min":  Period.Min_1,
        "5min":  Period.Min_5,
        "15min": Period.Min_15,
        "30min": Period.Min_30,
        "1hour": Period.Min_60,
        "1day":  Period.Day,
        "1week": Period.Week,
    }
    period = period_map.get(timeframe)
    if period is None:
        log.warning("LongPort: unsupported timeframe %s", timeframe)
        return None

    # Get or create context
    own_ctx = False
    ctx = None
    try:
        if _get_ctx_fn is not None:
            ctx = _get_ctx_fn(app_key, app_secret, access_token)
        else:
            cfg = Config(app_key=app_key, app_secret=app_secret, access_token=access_token)
            ctx = QuoteContext(cfg)
            own_ctx = True

        # ── Fast path: single call for <= 1000 bars ──────────────────────────
        if limit <= 1000:
            candles = ctx.candlesticks(symbol, period, limit, AdjustType.ForwardAdjust)
            if not candles:
                return None
            bars = _lp_candles_to_bars(candles, limit)
            log.info("LongPort (single-call): %d bars for %s/%s", len(bars), symbol, timeframe)
            return bars if bars else None

        # ── Paginated path: > 1000 bars ──────────────────────────────────────
        all_candles = []
        anchor_time = None
        while len(all_candles) < limit:
            batch = ctx.history_candlesticks_by_offset(
                symbol, period, AdjustType.ForwardAdjust,
                forward=False, count=_LP_PAGE_SIZE, time=anchor_time,
            )
            if not batch:
                break
            all_candles = list(batch) + all_candles
            if len(batch) < _LP_PAGE_SIZE:
                break  # reached beginning of available history
            anchor_time = batch[0].timestamp

        if not all_candles:
            return None
        bars = _lp_candles_to_bars(all_candles, limit)
        log.info("LongPort (paginated): %d bars for %s/%s", len(bars), symbol, timeframe)
        return bars if bars else None

    except Exception as e:
        log.warning("LongPort fetch failed %s/%s: %s", symbol, timeframe, e)
        return None
    finally:
        if own_ctx and ctx is not None:
            try:
                del ctx
            except Exception:
                pass


def _lp_candles_to_bars(candles, limit: int) -> list:
    """Convert LP candle objects to bar dicts, trimmed to limit."""
    bars = []
    for c in candles:
        try:
            ts = c.timestamp
            date_str = (ts.strftime("%Y-%m-%d %H:%M:%S")
                        if hasattr(ts, "strftime") else str(ts)[:19])
            bars.append({
                "date":   date_str,
                "open":   float(c.open),
                "high":   float(c.high),
                "low":    float(c.low),
                "close":  float(c.close),
                "volume": float(c.volume) if c.volume is not None else 0.0
            })
        except Exception:
            continue
    return bars[-limit:] if len(bars) > limit else bars


# ── Main waterfall ────────────────────────────────────────────────────────────

def fetch_bars_waterfall_sync(symbol, timeframe, limit, db_path,
                               ibkr_config=None, lp_credentials=None,
                               skip_cache=False, _get_lp_ctx_fn=None):
    """Synchronous waterfall fetch — call from executor thread.
    Priority: Local SQLite → R2 → LongPort → Yahoo → FMP
    _get_lp_ctx_fn: optional callable(app_key, secret, token) -> pooled QuoteContext.
    When provided, LongPort fetches reuse an existing connection (no handshake).
    """
    symbol = symbol.upper().strip()
    base = {"symbol": symbol, "timeframe": timeframe, "bars": [],
            "source": None, "source_message": "", "bar_count": 0, "error": None}

    # 1. Local SQLite cache (instant — no network)
    if not skip_cache:
        cached = load_from_local_cache(db_path, symbol, timeframe)
        if cached:
            bars, src = cached
            if len(bars) >= min(limit, 50):
                return {**base, "bars": bars[-limit:], "source": src,
                        "source_message": SOURCE_MESSAGES["local_cache"],
                        "bar_count": len(bars)}

    # 2. R2 (QuantX CDN — covers major US/HK on 1day, single fast fetch)
    #    Save hits to SQLite so subsequent calls are instant from local cache.
    log.info("Trying R2 for %s/%s...", symbol, timeframe)
    try:
        from .backtest import load_from_r2
        r2_bars, _ = load_from_r2(symbol, timeframe)
        if r2_bars and len(r2_bars) >= 20:
            trimmed = r2_bars[-limit:] if len(r2_bars) > limit else r2_bars
            save_to_local_cache(db_path, symbol, timeframe, trimmed, "r2")
            return {**base, "bars": trimmed, "source": "r2",
                    "source_message": SOURCE_MESSAGES["r2"],
                    "bar_count": len(trimmed)}
    except Exception as e:
        log.warning("R2 failed for %s/%s: %s", symbol, timeframe, e)

    # 3. LongPort (student account — covers HK/SG/intraday not in R2)
    if lp_credentials and lp_credentials.get("app_key"):
        log.info("Trying LongPort for %s/%s (limit=%d)...", symbol, timeframe, limit)
        bars = fetch_from_longport(
            symbol, timeframe, limit,
            app_key=lp_credentials["app_key"],
            app_secret=lp_credentials["app_secret"],
            access_token=lp_credentials["access_token"],
            _get_ctx_fn=_get_lp_ctx_fn,
        )
        if bars and len(bars) >= 20:
            save_to_local_cache(db_path, symbol, timeframe, bars, "longport")
            return {**base, "bars": bars, "source": "longport",
                    "source_message": SOURCE_MESSAGES["longport"],
                    "bar_count": len(bars)}
        log.info("LongPort: no usable bars for %s/%s, falling through", symbol, timeframe)

    # 4. Yahoo Finance
    log.info("Trying Yahoo for %s/%s...", symbol, timeframe)
    bars = fetch_from_yahoo(symbol, timeframe, limit)
    if bars and len(bars) >= 20:
        save_to_local_cache(db_path, symbol, timeframe, bars, "yahoo")
        return {**base, "bars": bars, "source": "yahoo",
                "source_message": SOURCE_MESSAGES["yahoo"],
                "bar_count": len(bars)}

    # 5. FMP
    log.info("Trying FMP for %s/%s...", symbol, timeframe)
    try:
        from .backtest import _fetch_from_fmp
        bars, _ = _fetch_from_fmp(symbol, timeframe, limit)
        if bars and len(bars) >= 20:
            save_to_local_cache(db_path, symbol, timeframe, bars, "fmp")
            return {**base, "bars": bars, "source": "fmp",
                    "source_message": SOURCE_MESSAGES["fmp"],
                    "bar_count": len(bars)}
    except Exception as e:
        log.warning("FMP failed for %s/%s: %s", symbol, timeframe, e)

    # No data available
    is_intraday_hk_sg = (timeframe not in ("1day", "1week")
                         and (symbol.endswith(".HK") or symbol.endswith(".SI")))
    error = (
        f"No intraday data for {symbol}/{timeframe}. "
        "Connect LongPort for HK/SG intraday data."
        if is_intraday_hk_sg else
        f"No data found for {symbol}/{timeframe}. "
        "Check symbol format (e.g. 700.HK, D05.SI, AAPL.US)."
    )
    return {**base, "source": "none",
            "source_message": SOURCE_MESSAGES["none"], "error": error}


# ── Helpers ───────────────────────────────────────────────────────────────────

def get_cached_symbols(db_path):
    try:
        conn = sqlite3.connect(db_path)
        rows = conn.execute(
            "SELECT symbol, timeframe, source, bar_count, fetched_at FROM data_cache ORDER BY fetched_at DESC"
        ).fetchall()
        conn.close()
        return [{"symbol": r[0], "timeframe": r[1], "source": r[2], "bars": r[3], "fetched_at": r[4]} for r in rows]
    except Exception:
        return []


def clear_cached_symbol(db_path, symbol, timeframe=None):
    try:
        conn = sqlite3.connect(db_path)
        if timeframe:
            conn.execute("DELETE FROM data_cache WHERE symbol=? AND timeframe=?", (symbol, timeframe))
        else:
            conn.execute("DELETE FROM data_cache WHERE symbol=?", (symbol,))
        conn.commit()
        conn.close()
    except Exception as e:
        log.warning("Cache clear failed: %s", e)
