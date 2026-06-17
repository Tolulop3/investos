"""
NGX Price Engine — Gate 1
Fetches NGX stock prices from NGN Markets API when NGN_MARKETS_KEY is set.
Falls back gracefully to macro-only scoring if unavailable.

Gate system:
  Gate 0 (no key): macro-only, paper tracking
  Gate 1 (key exists): price data blended with macro, real resolution begins
  Gate 2 (30+ resolved picks): sector betas auto-calibrated
  Gate 3 (60+ picks, WR>55%): full actionable signals
"""

import os
import json
import time
import urllib.request
from datetime import datetime, timedelta

NGN_MARKETS_BASE = "https://api.ngnmarkets.com/v1"
PRICE_CACHE_FILE  = "ngx_price_cache.json"
CACHE_TTL_HOURS   = 12  # EOD data — cache for half a day


def _get_api_key():
    return os.environ.get("NGN_MARKETS_KEY", "").strip()


def gate_level():
    """Returns current NGX gate level (0, 1, 2, or 3)."""
    if not _get_api_key():
        return 0

    try:
        cache = json.load(open(PRICE_CACHE_FILE))
        resolved_count = cache.get("resolved_picks", 0)
        wr             = cache.get("win_rate", 0.0)
    except Exception:
        resolved_count, wr = 0, 0.0

    if resolved_count >= 60 and wr >= 55.0:
        return 3
    if resolved_count >= 30:
        return 2
    return 1


def fetch_ngx_price(ticker):
    """
    Fetch EOD price data for a single NGX ticker from NGN Markets.
    Returns dict: {close, prev_close, change_pct, volume, ma50, ma200, above_ma50, above_ma200}
    Returns None if unavailable.
    """
    key = _get_api_key()
    if not key:
        return None

    # Try cache first
    try:
        cache = json.load(open(PRICE_CACHE_FILE))
        prices = cache.get("prices", {})
        entry  = prices.get(ticker)
        if entry:
            cached_at = datetime.fromisoformat(entry.get("cached_at", "2000-01-01"))
            if (datetime.now() - cached_at).total_seconds() < CACHE_TTL_HOURS * 3600:
                return entry["data"]
    except Exception:
        pass

    # Fetch from NGN Markets
    # Try both ticker formats: GTCO and GTCO.LG
    ticker_clean = ticker.replace(".LG", "")

    endpoints = [
        f"{NGN_MARKETS_BASE}/quotes/{ticker_clean}",
        f"{NGN_MARKETS_BASE}/stocks/{ticker_clean}/quote",
        f"{NGN_MARKETS_BASE}/market/stock/{ticker_clean}",
    ]

    for url in endpoints:
        try:
            req = urllib.request.Request(url, headers={
                "Authorization": f"Bearer {key}",
                "X-Api-Key":      key,
                "User-Agent":     "InvestOS/4.0",
                "Accept":         "application/json",
            })
            with urllib.request.urlopen(req, timeout=8) as r:
                raw = json.loads(r.read().decode())

            # Normalize response — NGN Markets may use different field names
            close      = _extract(raw, ["close", "lastPrice", "price", "last", "c"])
            prev_close = _extract(raw, ["previousClose", "prevClose", "open", "prev_close"])
            volume     = _extract(raw, ["volume", "vol", "v"])
            ma50       = _extract(raw, ["ma50", "sma50", "moving_average_50"])
            ma200      = _extract(raw, ["ma200", "sma200", "moving_average_200"])

            if close is None:
                continue

            change_pct = ((close - prev_close) / prev_close * 100) if prev_close else 0.0
            above_ma50  = close > ma50  if ma50  else None
            above_ma200 = close > ma200 if ma200 else None

            data = {
                "close":        round(float(close), 2),
                "prev_close":   round(float(prev_close), 2) if prev_close else None,
                "change_pct":   round(change_pct, 2),
                "volume":       int(volume) if volume else None,
                "ma50":         round(float(ma50),  2) if ma50  else None,
                "ma200":        round(float(ma200), 2) if ma200 else None,
                "above_ma50":   above_ma50,
                "above_ma200":  above_ma200,
                "source":       "ngn_markets",
            }

            # Save to cache
            _save_to_cache(ticker, data)
            return data

        except urllib.error.HTTPError as e:
            if e.code == 404:
                continue  # try next endpoint format
            if e.code in (401, 403):
                print(f"  ⚠️  NGX API auth error ({e.code}) — check NGN_MARKETS_KEY")
                return None
            continue
        except Exception:
            continue

    return None


def fetch_all_ngx_prices(tickers, verbose=False):
    """Fetch prices for all tickers. Returns dict {ticker: price_data}."""
    if not _get_api_key():
        return {}

    results = {}
    ok, failed = 0, 0

    for ticker in tickers:
        data = fetch_ngx_price(ticker)
        if data:
            results[ticker] = data
            ok += 1
        else:
            failed += 1
        time.sleep(0.15)  # Rate limit: ~6 requests/second

    if verbose:
        print(f"  📊 NGX prices: {ok}/{len(tickers)} fetched | {failed} unavailable")

    return results


def blend_price_with_macro(macro_score, price_data, ticker):
    """
    Blend macro score with price-based signals.
    Returns adjusted score and list of price signal reasons.

    Price signals (Gate 1):
      +8  if above MA50  (short-term momentum)
      +12 if above MA200 (long-term trend)
      -8  if below MA50
      -12 if below MA200
      +4  if positive day change
      -4  if negative day > 1%
    """
    if not price_data:
        return macro_score, []

    adj     = 0
    reasons = []

    above_200 = price_data.get("above_ma200")
    above_50  = price_data.get("above_ma50")
    chg       = price_data.get("change_pct", 0) or 0

    if above_200 is True:
        adj += 12; reasons.append("📈 Above 200MA (long-term bull)")
    elif above_200 is False:
        adj -= 12; reasons.append("📉 Below 200MA (long-term bear)")

    if above_50 is True:
        adj += 8;  reasons.append("📈 Above 50MA (momentum)")
    elif above_50 is False:
        adj -= 8;  reasons.append("📉 Below 50MA")

    if chg > 0.5:
        adj += 4;  reasons.append(f"📊 Day +{chg:.1f}%")
    elif chg < -1.0:
        adj -= 4;  reasons.append(f"📊 Day {chg:.1f}%")

    # Blend: 60% macro (fundamental driver), 40% price (actual market)
    blended = round(0.60 * macro_score + 0.40 * (macro_score + adj), 1)
    blended = max(0, min(100, blended))

    return blended, reasons


def get_gate_status_label(gate):
    labels = {
        0: "MACRO_ONLY — add NGN_MARKETS_KEY to enable price data",
        1: "PRICE_DATA_ACTIVE — resolving against real prices",
        2: "CALIBRATING — sector betas being calibrated from outcomes",
        3: "FULL — actionable signals with calibrated model",
    }
    return labels.get(gate, "UNKNOWN")


def _extract(d, keys):
    """Try multiple keys to extract a numeric value from a dict."""
    if not isinstance(d, dict):
        return None
    for k in keys:
        v = d.get(k)
        if v is not None:
            try: return float(v)
            except (TypeError, ValueError): pass
        # Try nested: data.stock.close etc.
        for nested_key in d:
            if isinstance(d[nested_key], dict):
                v = d[nested_key].get(k)
                if v is not None:
                    try: return float(v)
                    except (TypeError, ValueError): pass
    return None


def _save_to_cache(ticker, data):
    try:
        cache = {}
        try:
            cache = json.load(open(PRICE_CACHE_FILE))
        except Exception:
            pass
        if "prices" not in cache:
            cache["prices"] = {}
        cache["prices"][ticker] = {
            "data":       data,
            "cached_at":  datetime.now().isoformat(),
        }
        json.dump(cache, open(PRICE_CACHE_FILE, "w"), indent=2)
    except Exception:
        pass
