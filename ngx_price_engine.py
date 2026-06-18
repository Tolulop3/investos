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

NGN_MARKETS_BASE = "https://api.ngnmarket.com/v1"
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
        f"{NGN_MARKETS_BASE}/market/{ticker_clean}",           # most likely based on /market/snapshot
        f"{NGN_MARKETS_BASE}/market/stock/{ticker_clean}",
        f"{NGN_MARKETS_BASE}/quotes/{ticker_clean}",
        f"{NGN_MARKETS_BASE}/stocks/{ticker_clean}",
    ]

    _first_attempt = not os.path.exists("ngx_api_debug.json")

    for url in endpoints:
        try:
            req = urllib.request.Request(url, headers={
                "Authorization": f"Bearer {key}",
                "User-Agent":    "InvestOS/4.0",
                "Accept":        "application/json",
            })
            with urllib.request.urlopen(req, timeout=8) as r:
                raw = json.loads(r.read().decode())
            # Diagnostic: save first successful raw response
            if _first_attempt:
                with open("ngx_api_debug.json", "w") as _f:
                    json.dump({"url": url, "ticker": ticker_clean, "response": raw}, _f, indent=2)
                print(f"  📋 NGX API debug: saved first response from {url}")
                _first_attempt = False

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
            if _first_attempt:
                try:
                    _err_body = e.read().decode()[:300]
                    with open("ngx_api_debug.json", "w") as _f:
                        json.dump({"url": url, "ticker": ticker_clean,
                                   "http_error": e.code, "body": _err_body}, _f, indent=2)
                    print(f"  📋 NGX API {e.code} at {url}: {_err_body[:120]}")
                except Exception:
                    pass
                _first_attempt = False
            if e.code == 404:
                continue  # try next endpoint format
            if e.code in (401, 403):
                return None  # auth failure — silent, outer loop handles messaging
            continue
        except Exception:
            continue

    return None


def fetch_all_ngx_prices(tickers, verbose=False):
    """
    Fetch prices for all tickers. Returns dict {ticker: price_data}.
    Also fetches market snapshot for NGX macro data.
    """
    if not _get_api_key():
        return {}

    # Always fetch market snapshot (1 call) — contains NGX breadth/ASI
    market = fetch_ngx_market_snapshot(verbose=verbose)
    if market:
        _save_to_cache("__market__", market)

    # Try bulk/list endpoint first
    results = _fetch_snapshot(tickers, verbose=verbose)
    if results:
        ok = len(results)
        if verbose:
            print(f"  📊 NGX stock prices: {ok}/{len(tickers)} fetched via bulk endpoint")
        return results

    # Fall back to individual ticker calls (try first 3 to detect auth failure fast)
    results = {}
    ok, failed = 0, 0
    _auth_failed = False

    for i, ticker in enumerate(tickers):
        data = fetch_ngx_price(ticker)
        if data:
            results[ticker] = data
            ok += 1
        else:
            failed += 1
            # After 3 failures with 0 successes, likely auth issue — stop early
            if i >= 2 and ok == 0:
                _auth_failed = True
                failed += len(tickers) - i - 1  # count remaining as failed
                break
        time.sleep(0.10)

    if verbose:
        if ok == 0 and failed > 0:
            print(f"  📊 NGX prices: 0/{len(tickers)} fetched | all unavailable")
            if _auth_failed:
                print(f"  ⚠️  NGX API returned 403 — auth format mismatch")
                print(f"     Check: verify the API key is active on ngnmarket.com dashboard")
            else:
                print(f"  ⚠️  NGX price fetch failed — check API endpoint or key")
            print(f"     Falling back to macro-only scoring (same as Gate 0)")
        else:
            print(f"  📊 NGX prices: {ok}/{len(tickers)} fetched | {failed} unavailable")

    return results


def fetch_ngx_market_snapshot(verbose=False):
    """
    Fetch NGX market-level snapshot (ASI, breadth, volume).
    Returns market dict with asi_change, adv_dec_ratio, etc.
    This endpoint works on the free plan.
    """
    key = _get_api_key()
    if not key:
        return {}
    url = f"{NGN_MARKETS_BASE}/market/snapshot"
    try:
        req = urllib.request.Request(url, headers={
            "Authorization": f"Bearer {key}",
            "User-Agent":    "InvestOS/4.0",
            "Accept":        "application/json",
        })
        with urllib.request.urlopen(req, timeout=12) as r:
            raw = json.loads(r.read().decode())
        if not raw.get("success"):
            return {}
        data = raw.get("data", {})
        breadth = data.get("breadth", {})
        result = {
            "asi":               data.get("asi"),
            "asi_change_pct":    data.get("asi_change_percent"),
            "adv_dec_ratio":     breadth.get("adv_dec_ratio"),
            "advancers":         breadth.get("advancers"),
            "decliners":         breadth.get("decliners"),
            "unchanged":         breadth.get("unchanged"),
            "volume":            data.get("volume"),
            "deals":             data.get("deals"),
            "date":              data.get("date", "")[:10],
            "calls_remaining":   raw.get("meta", {}).get("calls_remaining"),
        }
        if verbose:
            adv = breadth.get("advancers", 0)
            dec = breadth.get("decliners", 0)
            chg = data.get("asi_change_percent", 0)
            direction = "🟢" if chg > 0 else "🔴"
            print(f"  {direction} NGX ASI: {chg:+.2f}% | "
                  f"Breadth: {adv} adv / {dec} dec "
                  f"(ratio: {breadth.get('adv_dec_ratio', 0):.2f})")
        return result
    except Exception:
        return {}


def _fetch_snapshot(tickers, verbose=False):
    """
    Attempt to fetch individual stock prices via bulk/list endpoint.
    Returns dict {ticker: price_data} or {} if unavailable.
    """
    key = _get_api_key()
    if not key:
        return {}

    # Individual stock list endpoints — snapshot is market-level only
    snap_candidates = [
        f"{NGN_MARKETS_BASE}/market/equities",
        f"{NGN_MARKETS_BASE}/market/stocks",
        f"{NGN_MARKETS_BASE}/equities",
        f"{NGN_MARKETS_BASE}/stocks",
    ]
    url = snap_candidates[0]
    raw = None
    for _snap_url in snap_candidates:
        try:
            _req = urllib.request.Request(_snap_url, headers={
                "Authorization": f"Bearer {key}",
                "User-Agent":    "InvestOS/4.0",
                "Accept":        "application/json",
            })
            with urllib.request.urlopen(_req, timeout=12) as _r:
                raw = json.loads(_r.read().decode())
            url = _snap_url
            break
        except urllib.error.HTTPError as _e:
            if _e.code == 403:
                try:
                    _body = _e.read().decode()[:300]
                    with open("ngx_api_debug.json", "w") as _f:
                        import json as _j
                        _j.dump({"url": _snap_url, "http_error": 403, "body": _body}, _f, indent=2)
                except Exception:
                    pass
            continue
        except Exception:
            continue
    if raw is None:
        return {}
    try:

            # Save snapshot for diagnostics
        with open("ngx_api_debug.json", "w") as _f:
            json.dump({"url": url, "response_keys": list(raw.keys()) if isinstance(raw, dict) else type(raw).__name__,
                       "sample": raw[:2] if isinstance(raw, list) else raw}, _f, indent=2)

            # Normalize: snapshot may return list of stocks or dict keyed by ticker
        stocks = []
        if isinstance(raw, list):
            stocks = raw
        elif isinstance(raw, dict):
            # Could be {"data": [...]} or {"stocks": [...]} or {"market": [...]}
            for key_try in ["data", "stocks", "market", "quotes", "results"]:
                if key_try in raw and isinstance(raw[key_try], list):
                    stocks = raw[key_try]
                    break
            if not stocks:
                stocks = list(raw.values()) if raw else []

        results = {}
        ticker_set = {t.upper() for t in tickers}

        for stock in stocks:
            if not isinstance(stock, dict):
                continue
            # Extract ticker symbol — field may be "symbol", "ticker", "code", "stock"
            sym = None
            for sym_key in ["symbol", "ticker", "code", "stock", "name"]:
                sym = stock.get(sym_key, "")
                if sym:
                    sym = str(sym).upper().replace(".LG", "")
                    break

            if not sym or sym not in ticker_set:
                continue

            close      = _extract(stock, ["close", "lastPrice", "price", "last", "c", "current_price"])
            prev_close = _extract(stock, ["previousClose", "prevClose", "open", "prev_close", "previous_close"])
            volume     = _extract(stock, ["volume", "vol", "v"])
            ma50       = _extract(stock, ["ma50", "sma50", "moving_average_50", "ma_50"])
            ma200      = _extract(stock, ["ma200", "sma200", "moving_average_200", "ma_200"])

            if close is None:
                continue

            change_pct  = ((close - prev_close) / prev_close * 100) if prev_close else 0.0
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
                "source":       "ngn_market_snapshot",
            }
            _save_to_cache(sym, data)
            results[sym] = data

        return results

    except urllib.error.HTTPError as e:
        if verbose:
            try:
                _body = e.read().decode()[:200]
                print(f"  ⚠️  Snapshot HTTP {e.code}: {_body}")
                with open("ngx_api_debug.json", "w") as _f:
                    json.dump({"url": url, "http_error": e.code, "body": _body}, _f, indent=2)
            except Exception:
                pass
        return {}
    except Exception as e:
        if verbose:
            print(f"  ⚠️  Snapshot fetch failed: {e}")
        return {}


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
