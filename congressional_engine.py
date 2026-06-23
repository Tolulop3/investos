"""
congressional_engine.py — InvestOS Congressional Trading Signal Engine
=======================================================================
Tracks US Congress member stock trades (both Senate + House).
Data sources (free, no auth):
  Senate: senate-stock-watcher-data S3 aggregate
  House:  house-stock-watcher-data S3 aggregate

Signal logic:
  CLUSTER_BUY   3+ members bought same ticker in 30d, combined >$50k  → +HIGH
  PAIR_BUY      2 members bought same ticker in 30d                    → +MODERATE  
  SINGLE_BUY    1 member, $50k+, committee-relevant                   → +WATCH
  CLUSTER_SELL  3+ members sold same ticker in 30d                    → -MODERATE
  ETF_SIGNAL    5+ members buying stocks in same sector               → sector signal

ETF implications:
  Defense stocks cluster   → ITA / CIBR signal
  Energy stocks cluster    → XEG.TO / XLE signal
  Tech/Semi cluster        → SMH / QQQ signal
  Healthcare cluster       → XLV / IBB signal
  Finance cluster          → XLF / ZEB.TO signal
  Gold/Materials cluster   → GLD / XGD.TO signal

Output:
  - congressional_signals.json (for InvestOS + AllocOS)
  - Per-ticker score boost (±pts, capped at ±12)
  - Per-sector ETF signal (for AllocOS rotation)
  - Printed report in daily log
"""

import json
import os
import urllib.request
from datetime import datetime, timedelta
from collections import defaultdict

# ── ENDPOINTS ────────────────────────────────────────────────────────────────
SENATE_URL = (
    "https://senate-stock-watcher-data.s3-us-east-2.amazonaws.com"
    "/aggregate/all_transactions.json"
)
HOUSE_URL = (
    "https://house-stock-watcher-data.s3-us-east-2.amazonaws.com"
    "/data/all_transactions.json"
)

CACHE_FILE  = "congressional_cache.json"
SIGNAL_FILE = "congressional_signals.json"
CACHE_TTL_HOURS = 12   # refresh twice daily max

# ── AMOUNT RANGES → midpoint estimate ────────────────────────────────────────
AMOUNT_MAP = {
    "$1,001 - $15,000":     8000,
    "$15,001 - $50,000":   32500,
    "$50,001 - $100,000":  75000,
    "$100,001 - $250,000": 175000,
    "$250,001 - $500,000": 375000,
    "$500,001 - $1,000,000": 750000,
    "Over $1,000,000":    1500000,
    "Over $5,000,000":    7500000,
}

# ── SECTOR → ETF MAPPING ─────────────────────────────────────────────────────
# If cluster of stocks in a sector is bought → fire ETF signal
SECTOR_ETF_MAP = {
    "DEFENSE":     ["ITA", "CIBR", "XAR"],
    "TECH":        ["SMH", "QQQ", "XLK", "HQU.TO"],
    "ENERGY":      ["XLE", "XEG.TO", "XOP", "OIH"],
    "HEALTHCARE":  ["XLV", "IBB", "XBI", "XHC.TO"],
    "FINANCE":     ["XLF", "ZEB.TO", "KRE", "XFN.TO"],
    "GOLD":        ["GLD", "GDX", "GDXJ", "XGD.TO"],
    "MATERIALS":   ["XLB", "XMA.TO"],
    "UTILITIES":   ["XLU", "ZUT.TO"],
    "REALESTATE":  ["VNQ", "XRE.TO", "XLRE"],
    "CONSUMER":    ["XLY", "XLP"],
    "SEMI":        ["SMH", "SOXX", "SOXL"],
    "CYBER":       ["CIBR", "HACK"],
}

# Stock → sector mapping for ETF implication
STOCK_SECTOR = {
    # Defense
    "LMT": "DEFENSE", "RTX": "DEFENSE", "NOC": "DEFENSE",
    "GD": "DEFENSE", "BA": "DEFENSE", "HII": "DEFENSE",
    "L3H": "DEFENSE", "LDOS": "DEFENSE", "SAIC": "DEFENSE",
    "CACI": "DEFENSE", "PLTR": "DEFENSE",
    # Tech
    "MSFT": "TECH", "AAPL": "TECH", "GOOGL": "TECH",
    "META": "TECH", "AMZN": "TECH", "NVDA": "SEMI",
    "AMD": "SEMI", "INTC": "SEMI", "QCOM": "SEMI",
    "AVGO": "SEMI", "TSM": "SEMI", "AMAT": "SEMI",
    # Cyber
    "CRWD": "CYBER", "PANW": "CYBER", "ZS": "CYBER",
    "FTNT": "CYBER", "NET": "CYBER",
    # Energy
    "XOM": "ENERGY", "CVX": "ENERGY", "COP": "ENERGY",
    "SLB": "ENERGY", "HAL": "ENERGY", "OXY": "ENERGY",
    "PSX": "ENERGY", "VLO": "ENERGY", "MPC": "ENERGY",
    "EOG": "ENERGY", "PXD": "ENERGY",
    # Healthcare
    "JNJ": "HEALTHCARE", "PFE": "HEALTHCARE", "ABBV": "HEALTHCARE",
    "MRK": "HEALTHCARE", "LLY": "HEALTHCARE", "AMGN": "HEALTHCARE",
    "GILD": "HEALTHCARE", "REGN": "HEALTHCARE", "BIIB": "HEALTHCARE",
    "UNH": "HEALTHCARE", "CVS": "HEALTHCARE",
    # Finance
    "JPM": "FINANCE", "BAC": "FINANCE", "WFC": "FINANCE",
    "GS": "FINANCE", "MS": "FINANCE", "C": "FINANCE",
    "BLK": "FINANCE", "BX": "FINANCE", "KKR": "FINANCE",
    # Gold/Materials
    "GLD": "GOLD", "NEM": "GOLD", "AEM": "GOLD",
    "WPM": "GOLD", "ABX": "GOLD", "FNV": "GOLD",
    # Retail/Consumer
    "AMZN": "CONSUMER", "WMT": "CONSUMER", "TGT": "CONSUMER",
    "HD": "CONSUMER", "LOW": "CONSUMER",
}

# ── HIGH-VALUE SENATORS (committee relevance) ─────────────────────────────────
# These senators have committee assignments that give them advance knowledge
HIGH_VALUE_SENATORS = {
    # Armed Services Committee
    "Jack Reed", "Roger Wicker", "Jeanne Shaheen", "Kirsten Gillibrand",
    "Richard Blumenthal", "Mazie Hirono", "Tim Kaine", "Angus King",
    "Martin Heinrich", "Elizabeth Warren", "Tommy Tuberville",
    "Tom Cotton", "Mike Rounds", "Joni Ernst", "Thom Tillis",
    "Dan Sullivan", "Deb Fischer", "Josh Hawley", "Kevin Cramer",
    "Bill Hagerty",
    # Finance Committee
    "Ron Wyden", "Debbie Stabenow", "Maria Cantwell", "Robert Menendez",
    "Mike Crapo", "Chuck Grassley", "John Cornyn", "John Thune",
    # Intelligence Committee
    "Mark Warner", "Marco Rubio",
    # Banking Committee
    "Sherrod Brown", "Pat Toomey",
}


def _parse_amount(amount_str):
    """Convert amount range string to midpoint estimate."""
    if not amount_str:
        return 0
    for key, val in AMOUNT_MAP.items():
        if key.lower() in amount_str.lower():
            return val
    return 8000  # default to minimum if unrecognised


def _fetch_with_cache(url, cache_key):
    """Fetch JSON from URL with TTL cache."""
    cache = {}
    try:
        with open(CACHE_FILE) as _cf:
            cache = json.load(_cf)
    except Exception:
        pass

    entry = cache.get(cache_key, {})
    cached_at = entry.get("fetched_at", "")
    if cached_at:
        age_hours = (datetime.now() - datetime.fromisoformat(cached_at)).total_seconds() / 3600
        if age_hours < CACHE_TTL_HOURS:
            return entry.get("data", [])

    try:
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "InvestOS/4.1 (personal investment tool)"}
        )
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.loads(r.read().decode())
        cache[cache_key] = {
            "fetched_at": datetime.now().isoformat(),
            "data": data,
        }
        with open(CACHE_FILE, "w") as _cf:
            json.dump(cache, _cf, indent=2)
        return data
    except Exception as e:
        print(f"  ⚠️  Congressional fetch error ({cache_key}): {type(e).__name__}: {e}")
        return entry.get("data", [])  # return stale cache if available


def _normalise_senate(raw):
    """Flatten Senate JSON into standard transaction list."""
    transactions = []
    if not isinstance(raw, list):
        return transactions
    for entry in raw:
        senator = f"{entry.get('first_name', '')} {entry.get('last_name', '')}".strip()
        for tx in entry.get("transactions", []):
            ticker = tx.get("ticker", "--").upper().strip()
            if ticker == "--" or not ticker:
                continue
            transactions.append({
                "member":   senator,
                "chamber":  "senate",
                "ticker":   ticker,
                "type":     tx.get("type", ""),
                "amount":   _parse_amount(tx.get("amount", "")),
                "amount_str": tx.get("amount", ""),
                "date":     tx.get("transaction_date", ""),
                "filed":    entry.get("date_recieved", ""),
                "asset":    tx.get("asset_description", ""),
            })
    return transactions


def _normalise_house(raw):
    """Flatten House JSON into standard transaction list."""
    transactions = []
    if not isinstance(raw, list):
        return transactions
    for tx in raw:
        ticker = tx.get("ticker", "--").upper().strip()
        if ticker == "--" or not ticker or len(ticker) > 8:
            continue
        member = tx.get("representative", tx.get("member", "")).strip()
        transactions.append({
            "member":   member,
            "chamber":  "house",
            "ticker":   ticker,
            "type":     tx.get("type", tx.get("transaction_type", "")),
            "amount":   _parse_amount(tx.get("amount", "")),
            "amount_str": tx.get("amount", ""),
            "date":     tx.get("transaction_date", ""),
            "filed":    tx.get("disclosure_date", ""),
            "asset":    tx.get("asset_description", ""),
        })
    return transactions


def _is_buy(tx_type):
    t = (tx_type or "").lower()
    return "purchase" in t or "buy" in t


def _is_sell(tx_type):
    t = (tx_type or "").lower()
    return "sale" in t or "sell" in t


def _days_ago(date_str, n=30):
    """Return True if date_str is within last n days."""
    if not date_str:
        return False
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m/%d/%y"):
        try:
            dt = datetime.strptime(date_str.strip(), fmt)
            return (datetime.now() - dt).days <= n
        except ValueError:
            continue
    return False


def run_congressional_engine(screener_tickers=None, verbose=True):
    """
    Main entry point. Returns dict of signals.

    screener_tickers: set of tickers currently in screener — focus analysis here
    verbose: print report to log
    """
    if verbose:
        print("\n" + "=" * 55)
        print("  CONGRESSIONAL TRADING ENGINE")
        print("  Tracking 535 Congress members (Senate + House)")
        print("=" * 55)

    # ── Fetch both chambers ───────────────────────────────────────────────────
    senate_raw  = _fetch_with_cache(SENATE_URL, "senate")
    house_raw   = _fetch_with_cache(HOUSE_URL,  "house")

    senate_txns = _normalise_senate(senate_raw)
    house_txns  = _normalise_house(house_raw)
    all_txns    = senate_txns + house_txns

    if verbose:
        print(f"\n  📊 Data: {len(senate_txns):,} Senate + {len(house_txns):,} House transactions")

    # ── Filter: last 30 days ─────────────────────────────────────────────────
    recent = [t for t in all_txns if _days_ago(t["date"], 30) or _days_ago(t["filed"], 30)]

    if verbose:
        print(f"  📅 Last 30 days: {len(recent)} transactions")

    # ── Group by ticker ───────────────────────────────────────────────────────
    buys_by_ticker  = defaultdict(list)
    sells_by_ticker = defaultdict(list)

    for tx in recent:
        if _is_buy(tx["type"]):
            buys_by_ticker[tx["ticker"]].append(tx)
        elif _is_sell(tx["type"]):
            sells_by_ticker[tx["ticker"]].append(tx)

    # ── Score ticker signals ──────────────────────────────────────────────────
    ticker_signals   = {}   # ticker → signal dict
    sector_buys      = defaultdict(list)  # sector → [tickers]

    for ticker, txns in buys_by_ticker.items():
        n_members = len(set(t["member"] for t in txns))
        total_amt = sum(t["amount"] for t in txns)
        high_value = any(t["member"] in HIGH_VALUE_SENATORS for t in txns)
        chamber_mix = len(set(t["chamber"] for t in txns)) > 1  # both chambers

        # Signal tier
        if n_members >= 5 or (n_members >= 3 and total_amt >= 150_000):
            tier = "STRONG_CLUSTER"
            score_boost = 12
            label = "🏛 STRONG CLUSTER BUY"
        elif n_members >= 3 or (n_members >= 2 and total_amt >= 75_000):
            tier = "CLUSTER"
            score_boost = 8
            label = "🏛 CLUSTER BUY"
        elif n_members >= 2 or (n_members == 1 and total_amt >= 50_000 and high_value):
            tier = "MODERATE"
            score_boost = 4
            label = "🏛 CONGRESSIONAL BUY"
        else:
            tier = "WEAK"
            score_boost = 2
            label = "🏛 WATCH"

        if high_value:
            score_boost = min(score_boost + 3, 12)
            label += " (committee)"
        if chamber_mix:
            score_boost = min(score_boost + 2, 12)
            label += " (bipartisan)"

        ticker_signals[ticker] = {
            "ticker":      ticker,
            "direction":   "BUY",
            "tier":        tier,
            "n_members":   n_members,
            "total_amt":   total_amt,
            "score_boost": score_boost,
            "label":       label,
            "members":     list(set(t["member"] for t in txns))[:5],
            "high_value":  high_value,
        }

        # ETF implication via sector
        sector = STOCK_SECTOR.get(ticker)
        if sector and n_members >= 2:
            sector_buys[sector].append(ticker)

    # Add sell signals
    for ticker, txns in sells_by_ticker.items():
        n_members = len(set(t["member"] for t in txns))
        if n_members >= 3:
            ticker_signals[ticker] = ticker_signals.get(ticker, {})
            ticker_signals[ticker].update({
                "sell_members": n_members,
                "sell_label":   "🏛 CLUSTER SELL — caution",
                "score_penalty": -6,
            })

    # ── ETF sector signals ────────────────────────────────────────────────────
    etf_signals = {}
    for sector, tickers in sector_buys.items():
        if len(tickers) >= 2:   # 2+ different stocks in same sector
            etf_targets = SECTOR_ETF_MAP.get(sector, [])
            conviction  = min(len(tickers) * 20, 90)  # 2 stocks = 40%, 4 = 80%
            etf_signals[sector] = {
                "sector":      sector,
                "stocks":      tickers,
                "etf_targets": etf_targets,
                "conviction":  conviction,
                "label":       f"🏛 Congressional sector signal: {sector}",
            }

    # ── Output ────────────────────────────────────────────────────────────────
    output = {
        "generated_at":  datetime.now().isoformat(),
        "window_days":   30,
        "total_recent":  len(recent),
        "ticker_signals": ticker_signals,
        "etf_signals":   etf_signals,
        "screener_hits": {},
    }

    # Filter to screener universe for relevance
    if screener_tickers:
        output["screener_hits"] = {
            t: s for t, s in ticker_signals.items()
            if t in screener_tickers
        }

    # ── Print report ──────────────────────────────────────────────────────────
    if verbose:
        print()
        # Strong signals
        strong = [(t, s) for t, s in ticker_signals.items()
                  if s.get("tier") in ("STRONG_CLUSTER", "CLUSTER")]
        strong.sort(key=lambda x: x[1]["n_members"], reverse=True)

        if strong:
            print(f"  🏛 CONGRESSIONAL BUY CLUSTERS ({len(strong)} tickers):")
            for ticker, sig in strong[:8]:
                members_str = ", ".join(sig["members"][:2])
                if len(sig["members"]) > 2:
                    members_str += f" +{len(sig['members'])-2} more"
                print(f"     {ticker:<8} {sig['label']:<30} "
                      f"n={sig['n_members']} "
                      f"~${sig['total_amt']:,.0f} | {members_str}")
        else:
            print("  🏛 No strong congressional clusters in last 30 days")

        if etf_signals:
            print()
            print(f"  📊 ETF SECTOR SIGNALS ({len(etf_signals)} sectors):")
            for sector, sig in etf_signals.items():
                etf_str = ", ".join(sig["etf_targets"][:3])
                print(f"     {sector:<15} {sig['conviction']}% conviction "
                      f"→ {etf_str}")
                print(f"                 Stocks: {', '.join(sig['stocks'])}")

        if screener_tickers and output["screener_hits"]:
            print()
            print(f"  🎯 SCREENER MATCHES ({len(output['screener_hits'])}):")
            for ticker, sig in output["screener_hits"].items():
                print(f"     {ticker:<8} +{sig['score_boost']}pts — {sig['label']}")

        print()

    # Save signals file
    with open(SIGNAL_FILE, "w") as _sf:
        json.dump(output, _sf, indent=2)
    return output


if __name__ == "__main__":
    run_congressional_engine(verbose=True)
