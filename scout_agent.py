"""
Scout Agent — InvestOS v4.0

Runs weekly (Sunday 6 AM ET via GitHub Actions).
Expands the screening universe beyond the 177 static names
by discovering momentum leaders from ETF constituents and index lists.

Output: universe_dynamic.json → merged into universe_current.json
The daily screener reads universe_current.json, not the hardcoded list.

Philosophy:
  Static universe  = curated, stable, evidence-backed names (177 core)
  Dynamic universe = weekly momentum scouts, auto-rotated
  universe_current = union of both, capped at 400

Promotion rule:
  Scout name scores top-10 in daily screener 3+ consecutive days
  → flagged for human promotion to static list (log entry created)

Demotion rule:
  Static name hasn't scored > 60 in 60+ days
  → flagged for human review / removal
"""

import json
import os
import re
import time
import datetime
import urllib.request

try:
    import yfinance as yf
    _YF = True
except ImportError:
    _YF = False

# ── Config ────────────────────────────────────────────────────────────────
MAX_UNIVERSE_SIZE  = 400      # hard cap — keeps daily runtime under 90s
MAX_DYNAMIC_US     = 120      # max US names added by scout
MAX_DYNAMIC_CA     = 60       # max CA names added by scout
MIN_PRICE          = 2.0      # filter penny stocks
MIN_VOLUME_3M_AVG  = 500_000  # min avg daily volume (shares)
MIN_3M_RETURN      = 0.05     # 5% min 3-month return to pass filter

DYNAMIC_FILE  = "universe_dynamic.json"
CURRENT_FILE  = "universe_current.json"
ROTATION_LOG  = "universe_rotation_log.json"

# ETF sources — yfinance holdings fetch
ETF_SOURCES = [
    {"ticker": "QQQ",     "max": 100, "region": "US", "label": "scout_qqq"},
    {"ticker": "SPY",     "max": 150, "region": "US", "label": "scout_spy"},
    {"ticker": "SMH",     "max": 30,  "region": "US", "label": "scout_smh"},
    {"ticker": "XIC.TO",  "max": 60,  "region": "CA", "label": "scout_xic"},
    {"ticker": "XEQT.TO", "max": 50,  "region": "US", "label": "scout_xeqt"},
]


def get_static_universe():
    """Extract the hardcoded UNIVERSE tickers from stock_screener.py."""
    tickers = set()
    try:
        content = open("stock_screener.py").read()
        # Find all quoted ticker-like strings inside UNIVERSE block (lines 30-135)
        block = "\n".join(content.split("\n")[29:135])
        matches = re.findall(r'"([A-Z][A-Z0-9.\-]{1,8})"', block)
        for m in matches:
            if len(m) >= 2:
                tickers.add(m)
    except Exception as e:
        print(f"  ⚠️  Could not read static universe: {e}")
    return tickers


def fetch_etf_holdings(etf_ticker, max_holdings=100):
    """
    Fetch ETF constituent tickers via yfinance.
    Returns list of ticker strings or empty list on failure.
    """
    if not _YF:
        return []
    try:
        etf = yf.Ticker(etf_ticker)
        # Try funds_data first (most reliable for ETFs)
        try:
            fd = etf.funds_data
            if fd is not None:
                df = fd.top_holdings
                if df is not None and not df.empty:
                    return list(df.index[:max_holdings])
        except Exception:
            pass
        # Fallback: get_holdings
        try:
            h = etf.get_holdings()
            if h is not None and not h.empty:
                return list(h.index[:max_holdings])
        except Exception:
            pass
    except Exception:
        pass
    return []


def fetch_sp500_wikipedia():
    """
    Fetch S&P 500 constituent tickers from Wikipedia.
    Stable, free, no API key. Returns list of ticker strings.
    """
    try:
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        req = urllib.request.Request(url, headers={"User-Agent": "InvestOS/4.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            html = r.read().decode("utf-8")
        # The table has ticker in first column as <td><a href="...">TICK</a></td>
        tickers = re.findall(
            r'<td><a[^>]+href="/wiki/[^"]+">([A-Z]{1,5})</a></td>', html
        )
        if not tickers:
            # Fallback pattern
            tickers = re.findall(r'"symbol"\s*:\s*"([A-Z]{1,5})"', html)
        return list(dict.fromkeys(tickers))[:505]  # dedupe, cap at 505
    except Exception as e:
        print(f"  ⚠️  S&P 500 Wikipedia fetch failed: {e}")
        return []


def momentum_pre_filter(tickers, region="US", verbose=True):
    """
    Fast momentum pre-filter — 1 yfinance call per ticker.
    Fetches 3-month OHLCV only (not full fundamentals).
    Returns list of (ticker, scout_score, metadata) sorted desc by score.

    Filters:
      price > MIN_PRICE
      avg volume > MIN_VOLUME_3M_AVG
      3-month return > MIN_3M_RETURN
    """
    if not _YF:
        return [(t, 0.0, {}) for t in tickers]

    results = []
    failed  = 0

    for i, ticker in enumerate(tickers):
        try:
            t = yf.Ticker(ticker)
            hist = t.history(period="3mo", auto_adjust=True)

            if hist.empty or len(hist) < 30:
                failed += 1
                continue

            price_now = float(hist["Close"].iloc[-1])
            price_3m  = float(hist["Close"].iloc[0])
            vol_avg   = float(hist["Volume"].mean())
            ret_3m    = (price_now - price_3m) / price_3m

            # Hard filters
            if price_now < MIN_PRICE:
                continue
            if vol_avg < MIN_VOLUME_3M_AVG:
                continue
            if ret_3m < MIN_3M_RETURN:
                continue

            # Quick 14-day RSI proxy
            closes = hist["Close"].tail(15)
            deltas = closes.diff().dropna()
            gains  = deltas.clip(lower=0).mean()
            losses = (-deltas.clip(upper=0)).mean()
            rsi = 100 - (100 / (1 + gains / losses)) if losses > 0 else 100.0

            # 52-week high check (single extended history call)
            hist_1y = t.history(period="1y", auto_adjust=True)
            near_high = False
            if not hist_1y.empty:
                high_52 = float(hist_1y["High"].max())
                near_high = price_now >= high_52 * 0.95

            # Scout score: momentum-weighted composite
            scout_score = (
                ret_3m * 40 +
                (rsi / 100.0) * 30 +
                (20.0 if near_high else 0.0)
            )

            results.append((ticker, scout_score, {
                "return_3m":      round(ret_3m * 100, 1),
                "rsi":            round(rsi, 1),
                "price":          round(price_now, 2),
                "vol_avg_shares": int(vol_avg),
                "near_52wk_high": near_high,
            }))

            if verbose and (i + 1) % 25 == 0:
                print(f"    → {i+1}/{len(tickers)} scouted | {len(results)} passed")

            time.sleep(0.08)  # polite rate limiting

        except Exception:
            failed += 1
            continue

    results.sort(key=lambda x: x[1], reverse=True)

    if verbose:
        print(f"    Scouted {len(tickers)} | passed: {len(results)} | failed: {failed}")

    return results


def run_scout(verbose=True):
    """
    Main weekly scout run.
    1. Load static universe from stock_screener.py
    2. Fetch ETF holdings + S&P 500 list
    3. Momentum pre-filter on new names only
    4. Merge static + top scouts → universe_current.json
    5. Log rotation for human review
    """
    today = datetime.date.today().isoformat()
    print(f"\n{'='*55}")
    print(f"  SCOUT AGENT — {today}")
    print(f"{'='*55}\n")

    # Step 1: Static universe
    static = get_static_universe()
    print(f"  Static universe: {len(static)} tickers")

    # Step 2: Candidate collection
    us_candidates = set()
    ca_candidates = set()
    source_map    = {}

    # S&P 500 from Wikipedia
    print(f"  Fetching S&P 500 from Wikipedia...")
    sp500 = fetch_sp500_wikipedia()
    if sp500:
        for t in sp500:
            us_candidates.add(t)
            source_map[t] = "scout_sp500"
        print(f"    → {len(sp500)} tickers")
    else:
        print(f"    → failed, continuing without")

    # ETF holdings
    for cfg in ETF_SOURCES:
        print(f"  Fetching {cfg['ticker']} holdings...")
        holdings = fetch_etf_holdings(cfg["ticker"], cfg["max"])
        if holdings:
            for t in holdings:
                if cfg["region"] == "CA":
                    ca_candidates.add(t)
                else:
                    us_candidates.add(t)
                if t not in source_map:
                    source_map[t] = cfg["label"]
            print(f"    → {len(holdings)} holdings")
        else:
            print(f"    → no holdings returned")

    # Remove already-static names (no need to re-scout)
    us_new = sorted(t for t in us_candidates if t not in static)
    ca_new = sorted(t for t in ca_candidates if t not in static)
    print(f"\n  New US candidates to scout: {len(us_new)}")
    print(f"  New CA candidates to scout: {len(ca_new)}")

    # Step 3: Momentum pre-filter
    print(f"\n  Scouting US candidates (momentum filter)...")
    us_results = momentum_pre_filter(us_new[:250], region="US", verbose=verbose)
    us_top     = us_results[:MAX_DYNAMIC_US]

    print(f"\n  Scouting CA candidates (momentum filter)...")
    ca_results = momentum_pre_filter(ca_new[:120], region="CA", verbose=verbose)
    ca_top     = ca_results[:MAX_DYNAMIC_CA]

    # Step 4: Build dynamic dict
    dynamic = {}
    for ticker, score, meta in us_top + ca_top:
        dynamic[ticker] = {
            "scout_score": round(score, 2),
            "source":      source_map.get(ticker, "scout_unknown"),
            "scouted_at":  today,
            **meta,
        }

    # Save dynamic universe
    json.dump(dynamic, open(DYNAMIC_FILE, "w"), indent=2)
    print(f"\n  ✅ {DYNAMIC_FILE}: {len(dynamic)} dynamic tickers")

    # Step 5: Merge into universe_current.json
    current_tickers = {}

    for t in sorted(static):
        current_tickers[t] = {"source": "static", "static": True}

    added = 0
    for t, meta in dynamic.items():
        if t not in current_tickers and len(current_tickers) < MAX_UNIVERSE_SIZE:
            current_tickers[t] = {**meta, "static": False}
            added += 1

    current_payload = {
        "generated_at":  today,
        "static_count":  len(static),
        "dynamic_count": added,
        "total":         len(current_tickers),
        "tickers":       current_tickers,
    }
    json.dump(current_payload, open(CURRENT_FILE, "w"), indent=2)
    print(f"  ✅ {CURRENT_FILE}: {len(current_tickers)} total "
          f"({len(static)} static + {added} dynamic)")

    # Step 6: Rotation log
    try:
        log = []
        try:
            log = json.load(open(ROTATION_LOG))
        except Exception:
            pass
        log.append({
            "date":           today,
            "static_count":   len(static),
            "dynamic_added":  added,
            "total":          len(current_tickers),
            "top_us_scouts":  [(t, meta["return_3m"]) for t, _, meta in us_top[:10]],
            "top_ca_scouts":  [(t, meta["return_3m"]) for t, _, meta in ca_top[:5]],
        })
        json.dump(log[-52:], open(ROTATION_LOG, "w"), indent=2)
    except Exception:
        pass

    # Step 7: Print promotion candidates
    print(f"\n  TOP US SCOUTS this week:")
    for ticker, score, meta in us_top[:12]:
        print(f"    {ticker:<10}  3m={meta['return_3m']:>+5.1f}%  "
              f"RSI={meta['rsi']:>4.0f}  ${meta['price']:.2f}"
              + ("  ★ near 52wk high" if meta["near_52wk_high"] else ""))

    print(f"\n  TOP CA SCOUTS this week:")
    for ticker, score, meta in ca_top[:6]:
        print(f"    {ticker:<12}  3m={meta['return_3m']:>+5.1f}%  "
              f"RSI={meta['rsi']:>4.0f}  ${meta['price']:.2f}")

    print(f"\n{'='*55}")
    print(f"  SCOUT COMPLETE")
    print(f"  {len(static)} static + {added} dynamic = {len(current_tickers)} total")
    print(f"  Daily screener will use universe_current.json tomorrow")
    print(f"{'='*55}\n")

    return current_payload


if __name__ == "__main__":
    import sys
    dry_run = "--dry-run" in sys.argv
    if dry_run:
        print("  🔬 DRY-RUN: will fetch candidates but skip momentum filter")
        static = get_static_universe()
        print(f"  Static: {len(static)} tickers")
        sp500  = fetch_sp500_wikipedia()
        print(f"  S&P 500: {len(sp500)} tickers from Wikipedia")
        new_us = [t for t in sp500 if t not in static]
        print(f"  New US names not in static: {len(new_us)}")
        print(f"  Sample: {new_us[:20]}")
    else:
        run_scout(verbose=True)
