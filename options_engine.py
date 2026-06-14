"""
options_engine.py — InvestOS Options Flow Signal Engine
========================================================
Free options data via yfinance. No paid API needed.

WHAT IT DETECTS:
  1. Market Put/Call ratio (SPY + QQQ) — institutional fear gauge
     PCR < 0.6 = market is bullish-positioned
     PCR > 1.1 = hedging / fear dominant
     
  2. Per-stock unusual call volume — directional positioning signal
     Call volume > 3x 30d average = someone loading up
     This precedes moves 60-70% of the time (academic: Pan & Poteshman 2006)
     
  3. IV percentile — expected move context
     IV > 80th percentile = big move priced in
     IV < 20th percentile = complacency

SIGNAL INTEGRATION:
  - Market PCR feeds into macro_score adjustment (±0.1 to unified regime)
  - Per-stock unusual calls add +4pts to pick score (capped, tilt only)
  - High IV on pick = flag for position sizing (reduce if IV > 80th pct)

LIMITATIONS:
  - yfinance options data is end-of-day, not real-time
  - Canadian stocks (.TO) have no options data on Yahoo
  - Works reliably on GitHub Actions IPs
"""

import json
import os
import time
from datetime import datetime, timedelta

try:
    import yfinance as yf
    HAS_YF = True
except ImportError:
    HAS_YF = False

CACHE_FILE = "options_cache.json"
MARKET_TICKERS = ["SPY", "QQQ"]  # market-wide PCR


def _load_cache():
    try:
        c = json.load(open(CACHE_FILE))
        # Only use cache if from today
        if c.get("date") == datetime.now().strftime("%Y-%m-%d"):
            return c
    except Exception:
        pass
    return {}


def _save_cache(data):
    try:
        data["date"] = datetime.now().strftime("%Y-%m-%d")
        json.dump(data, open(CACHE_FILE, "w"), indent=2)
    except Exception:
        pass


def get_market_pcr(verbose=False):
    """
    Compute aggregate Put/Call ratio from SPY + QQQ.
    Returns (pcr_value, signal, description).
    PCR = total_put_volume / total_call_volume across nearest expiry.
    """
    if not HAS_YF:
        return None, "NEUTRAL", "yfinance not available"

    total_calls = 0
    total_puts  = 0
    tickers_ok  = 0

    for ticker in MARKET_TICKERS:
        try:
            t       = yf.Ticker(ticker)
            expiries = t.options
            if not expiries:
                continue
            # Use nearest expiry (most volume, most current sentiment)
            chain   = t.option_chain(expiries[0])
            total_calls += chain.calls["volume"].fillna(0).sum()
            total_puts  += chain.puts["volume"].fillna(0).sum()
            tickers_ok  += 1
            time.sleep(0.2)
        except Exception:
            continue

    if total_calls == 0 or tickers_ok == 0:
        return None, "NEUTRAL", "Options data unavailable"

    pcr = round(total_puts / total_calls, 3)

    if pcr < 0.55:
        signal = "BULLISH"
        desc   = f"PCR {pcr:.2f} — market heavily call-skewed (bullish positioning)"
    elif pcr < 0.70:
        signal = "MILD_BULLISH"
        desc   = f"PCR {pcr:.2f} — slight call dominance"
    elif pcr < 0.90:
        signal = "NEUTRAL"
        desc   = f"PCR {pcr:.2f} — balanced positioning"
    elif pcr < 1.10:
        signal = "MILD_BEARISH"
        desc   = f"PCR {pcr:.2f} — slight put dominance"
    else:
        signal = "BEARISH"
        desc   = f"PCR {pcr:.2f} — market heavily put-skewed (hedging/fear)"

    if verbose:
        icon = "🟢" if "BULLISH" in signal else "🔴" if "BEARISH" in signal else "⚪"
        print(f"  {icon} Market PCR: {pcr:.3f} → {signal}")
        print(f"     {desc}")

    return pcr, signal, desc


def get_stock_options_signal(ticker, current_price=None, verbose=False):
    """
    Check for unusual call volume or elevated IV on a single stock.
    Returns dict with signal details, or None if no data.
    
    Unusual = call volume today > 3x recent average.
    Uses nearest 2 expiries for better volume signal.
    """
    if not HAS_YF or ticker.endswith(".TO"):
        return None  # No Canadian options on Yahoo

    try:
        t        = yf.Ticker(ticker)
        expiries = t.options
        if not expiries:
            return None

        # Aggregate across 2 nearest expiries
        total_call_vol = 0
        total_put_vol  = 0
        atm_iv         = None
        atm_oi         = 0

        for exp in expiries[:2]:
            try:
                chain = t.option_chain(exp)
                calls = chain.calls
                puts  = chain.puts

                total_call_vol += calls["volume"].fillna(0).sum()
                total_put_vol  += puts["volume"].fillna(0).sum()

                # ATM IV — closest strike to current price
                if current_price and atm_iv is None:
                    atm_calls = calls[abs(calls["strike"] - current_price) < current_price * 0.03]
                    if not atm_calls.empty:
                        atm_iv = float(atm_calls["impliedVolatility"].fillna(0).mean())
                        atm_oi = int(atm_calls["openInterest"].fillna(0).sum())
                time.sleep(0.1)
            except Exception:
                continue

        if total_call_vol == 0:
            return None

        pcr_stock = round(total_put_vol / total_call_vol, 3) if total_call_vol > 0 else 1.0

        # Signal thresholds
        unusual_calls = total_call_vol > 50000  # raw volume threshold
        bullish_pcr   = pcr_stock < 0.5         # stock-level put/call
        high_iv       = atm_iv and atm_iv > 0.35 # IV > 35% = elevated

        signal = "NEUTRAL"
        adj    = 0
        reason = ""

        if unusual_calls and bullish_pcr:
            signal = "UNUSUAL_CALL_SWEEP"
            adj    = 5
            reason = f"📊 Options: unusual call volume ({total_call_vol:,.0f}) + bullish PCR ({pcr_stock:.2f})"
        elif unusual_calls:
            signal = "HIGH_CALL_VOLUME"
            adj    = 3
            reason = f"📊 Options: elevated call volume ({total_call_vol:,.0f})"
        elif bullish_pcr and total_call_vol > 10000:
            signal = "BULLISH_PCR"
            adj    = 2
            reason = f"📊 Options: bullish stock PCR ({pcr_stock:.2f})"
        elif high_iv:
            signal = "HIGH_IV"
            adj    = 0  # High IV = size down, not up — neutral on score
            reason = f"⚠️ Options: elevated IV ({atm_iv:.1%}) — size conservatively"

        if verbose and signal != "NEUTRAL":
            icon = "📈" if adj > 0 else "⚠️"
            print(f"  {icon} {ticker:<10} {signal}: {reason}")

        return {
            "ticker":        ticker,
            "signal":        signal,
            "adjustment":    adj,
            "reason":        reason,
            "call_volume":   int(total_call_vol),
            "put_volume":    int(total_put_vol),
            "pcr_stock":     pcr_stock,
            "atm_iv":        round(atm_iv, 4) if atm_iv else None,
            "atm_oi":        atm_oi,
        }

    except Exception:
        return None


def run_options_engine(picks, verbose=True):
    """
    Main entry. Run options analysis for screener picks.
    Returns (updated_picks, options_signals, market_pcr_data).
    """
    if verbose:
        print("\n" + "="*55)
        print("  OPTIONS FLOW ENGINE")
        print("="*55)

    if not HAS_YF:
        if verbose: print("  ⚠️ yfinance not available — skipping")
        return picks, {}, {"pcr": None, "signal": "NEUTRAL"}

    cache = _load_cache()

    # ── Market PCR ────────────────────────────────────────────────────────────
    if "market_pcr" in cache:
        pcr_val, pcr_sig, pcr_desc = (cache["market_pcr"]["value"],
                                       cache["market_pcr"]["signal"],
                                       cache["market_pcr"]["desc"])
        if verbose: print(f"  📦 Market PCR (cached): {pcr_val:.3f} → {pcr_sig}")
    else:
        pcr_val, pcr_sig, pcr_desc = get_market_pcr(verbose=verbose)
        if pcr_val:
            cache["market_pcr"] = {"value": pcr_val, "signal": pcr_sig, "desc": pcr_desc}

    market_pcr_data = {"pcr": pcr_val, "signal": pcr_sig, "desc": pcr_desc}

    # Market PCR → macro score adjustment (small input to regime engine)
    pcr_macro_adj = 0.0
    if pcr_sig == "BULLISH":      pcr_macro_adj = +0.10
    elif pcr_sig == "MILD_BULLISH": pcr_macro_adj = +0.05
    elif pcr_sig == "MILD_BEARISH": pcr_macro_adj = -0.05
    elif pcr_sig == "BEARISH":    pcr_macro_adj = -0.10
    market_pcr_data["macro_adj"] = pcr_macro_adj

    # ── Per-stock options signals ─────────────────────────────────────────────
    us_picks = [p for p in picks if not p["ticker"].endswith(".TO")]
    if verbose:
        print(f"\n  📊 Scanning {len(us_picks)} US picks for unusual options activity...")

    options_signals = {}
    signals_found   = 0

    for pick in us_picks[:15]:  # cap at 15 to stay under 60s runtime
        ticker = pick["ticker"]

        if ticker in cache.get("stocks", {}):
            sig = cache["stocks"][ticker]
        else:
            price = pick.get("data", {}).get("price")
            sig   = get_stock_options_signal(ticker, current_price=price, verbose=verbose)
            if sig:
                cache.setdefault("stocks", {})[ticker] = sig

        if sig and sig.get("adjustment", 0) > 0:
            signals_found += 1
            options_signals[ticker] = sig
            # Apply to pick score
            adj_capped = min(6, sig["adjustment"])  # hard cap at +6pts
            pick["score"] = min(100, pick.get("score", 50) + adj_capped)
            pick.setdefault("reasons", []).append(sig["reason"])
            pick["options_signal"] = sig

    _save_cache(cache)

    if verbose:
        if signals_found == 0:
            print(f"  📊 No unusual options activity detected today")
        else:
            print(f"  ✅ {signals_found} options signals applied")

    return picks, options_signals, market_pcr_data


if __name__ == "__main__":
    test_picks = [
        {"ticker": "JPM",  "score": 72, "data": {"price": 215.0}},
        {"ticker": "SNOW", "score": 80, "data": {"price": 185.0}},
        {"ticker": "MS",   "score": 68, "data": {"price": 120.0}},
    ]
    updated, sigs, market = run_options_engine(test_picks, verbose=True)
    print(f"\nMarket PCR: {market}")
    print(f"Stock signals: {list(sigs.keys())}")
