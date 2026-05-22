"""
etf_engine.py — InvestOS ETF Signal Engine
==========================================
Scores 30 ETFs across: Core, Sector, Thematic, Defensive, International
Two scoring tracks:
  - CORE/DEFENSIVE: regime + momentum + fundamentals
  - THEMATIC: regime + news signal + momentum only

Account type routing:
  - RRSP → prefer US-listed ETFs (IRS withholding treaty exemption)
  - TFSA/FHSA → prefer .TO ETFs (no foreign withholding)

Outputs top picks per account type with signal reasoning.
"""

import yfinance as yf
from datetime import datetime, date
import traceback

# ── ETF UNIVERSE ─────────────────────────────────────────────────────────────
# Format: ticker, name, category, track, account_preference, sector_signal
ETF_UNIVERSE = [
    # ── CORE EQUITY ──────────────────────────────────────────────────────────
    ("VFV.TO",  "S&P 500 (CAD)",          "CORE",      "core",     "RRSP",  None),
    ("XIC.TO",  "TSX Composite",          "CORE",      "core",     "TFSA",  "TSX_BROAD"),
    ("XEQT.TO", "All-Equity Global",      "CORE",      "core",     "TFSA",  "TSX_BROAD"),
    ("QQQ",     "NASDAQ 100",             "CORE",      "core",     "RRSP",  "TECH"),
    ("XEF.TO",  "Intl Developed",         "CORE",      "core",     "TFSA",  None),
    ("VWO",     "Emerging Markets",       "CORE",      "core",     "RRSP",  None),
    ("VOO",     "S&P 500 (USD)",          "CORE",      "core",     "RRSP",  None),

    # ── SECTOR ───────────────────────────────────────────────────────────────
    ("XEG.TO",  "Canadian Energy",        "SECTOR",    "core",     "TFSA",  "CANADIAN_ENERGY"),
    ("ZEB.TO",  "Canadian Banks",         "SECTOR",    "core",     "TFSA",  "CANADIAN_BANKS"),
    ("XLE",     "US Energy",              "SECTOR",    "core",     "RRSP",  "OIL_PRODUCERS"),
    ("ZGD.TO",  "Canadian Gold Miners",   "SECTOR",    "core",     "TFSA",  "GOLD"),
    ("GLD",     "Gold",                   "SECTOR",    "core",     "RRSP",  "GOLD"),
    ("XRE.TO",  "Canadian REITs",         "SECTOR",    "core",     "TFSA",  "CANADIAN_REITS"),
    ("ZLB.TO",  "Canadian Low-Vol",       "SECTOR",    "core",     "TFSA",  None),

    # ── THEMATIC — AI / TECH ─────────────────────────────────────────────────
    ("BOTZ",    "Robotics & AI",          "THEMATIC",  "thematic", "RRSP",  "TECH"),
    ("SMH",     "Semiconductors",         "THEMATIC",  "thematic", "RRSP",  "TECH"),
    ("SKYY",    "Cloud Computing",        "THEMATIC",  "thematic", "RRSP",  "TECH"),
    ("CIBR",    "Cybersecurity",          "THEMATIC",  "thematic", "RRSP",  "CYBERSECURITY"),

    # ── THEMATIC — DEFENSE / WAR ─────────────────────────────────────────────
    ("ITA",     "Aerospace & Defense",    "THEMATIC",  "thematic", "RRSP",  "DEFENSE"),
    ("SHLD",    "Defense Tech",           "THEMATIC",  "thematic", "RRSP",  "DEFENSE"),
    ("PPA",     "Aerospace & Defense",    "THEMATIC",  "thematic", "RRSP",  "DEFENSE"),

    # ── THEMATIC — EMERGING TECH ─────────────────────────────────────────────
    ("QTUM",    "Quantum Computing",      "THEMATIC",  "thematic", "RRSP",  "TECH"),
    ("ARKG",    "Genomics",               "THEMATIC",  "thematic", "RRSP",  "BIOTECH"),
    ("BLOK",    "Blockchain",             "THEMATIC",  "thematic", "RRSP",  "TECH"),

    # ── DEFENSIVE / BOND ─────────────────────────────────────────────────────
    ("ZAG.TO",  "Canadian Agg Bonds",     "DEFENSIVE", "core",     "TFSA",  None),
    ("XBB.TO",  "Canadian Bond Universe", "DEFENSIVE", "core",     "TFSA",  None),
    ("TLT",     "US Long Bonds",          "DEFENSIVE", "core",     "RRSP",  None),
    ("ZLB.TO",  "Canadian Low-Vol Eq",    "DEFENSIVE", "core",     "TFSA",  None),

    # ── INTERNATIONAL ────────────────────────────────────────────────────────
    ("EFA",     "Intl Developed",         "INTL",      "core",     "RRSP",  None),
    ("EEM",     "Emerging Markets",       "INTL",      "core",     "RRSP",  None),
]

# ── SECTOR SIGNAL → ETF BOOST MAP ────────────────────────────────────────────
# Maps sector sentiment keys to ETF tickers and boost amounts
SECTOR_ETF_BOOST = {
    "DEFENSE":          [("ITA", +25), ("SHLD", +25), ("PPA", +20)],
    "GOLD":             [("GLD", +25), ("ZGD.TO", +25)],
    "OIL":              [("XEG.TO", +20), ("XLE", +20)],
    "CANADIAN_ENERGY":  [("XEG.TO", +20), ("XLE", +15)],
    "OIL_PRODUCERS":    [("XEG.TO", +15), ("XLE", +20)],
    "TECH":             [("SMH", +20), ("BOTZ", +15), ("QQQ", +15), ("SKYY", +10)],
    "CYBERSECURITY":    [("CIBR", +25)],
    "BIOTECH":          [("ARKG", +20)],
    "CANADIAN_BANKS":   [("ZEB.TO", +20)],
    "AIRLINES":         [("ITA", -10)],   # airlines bearish → general defense still ok
    "CONSUMER_DISCRETIONARY": [("QQQ", -10)],
    "AUTOS":            [("QQQ", -5)],
}

# ── REGIME → CATEGORY WEIGHTS ─────────────────────────────────────────────────
REGIME_WEIGHTS = {
    "RISK_ON":  {"CORE": 1.0, "SECTOR": 1.0, "THEMATIC": 1.0, "DEFENSIVE": 0.3, "INTL": 1.0},
    "NEUTRAL":  {"CORE": 0.8, "SECTOR": 0.7, "THEMATIC": 0.6, "DEFENSIVE": 0.8, "INTL": 0.7},
    "RISK_OFF": {"CORE": 0.5, "SECTOR": 0.4, "THEMATIC": 0.3, "DEFENSIVE": 1.2, "INTL": 0.4},
}

# ── ACCOUNT TYPE ALLOCATION ──────────────────────────────────────────────────
ACCOUNT_ALLOCATION = {
    "RISK_ON":  {"equity_pct": 85, "bond_pct": 5,  "gold_pct": 5,  "cash_pct": 5},
    "NEUTRAL":  {"equity_pct": 60, "bond_pct": 25, "gold_pct": 10, "cash_pct": 5},
    "RISK_OFF": {"equity_pct": 35, "bond_pct": 40, "gold_pct": 15, "cash_pct": 10},
}


def _fetch_etf_data(ticker):
    """Fetch price, momentum, MA data for one ETF via yfinance."""
    try:
        t = yf.Ticker(ticker)
        hist = t.history(period="12mo", interval="1d", auto_adjust=True)
        if hist.empty or len(hist) < 20:
            return None
        closes = hist["Close"].dropna().tolist()
        if len(closes) < 20:
            return None

        price   = closes[-1]
        ret_30  = (closes[-1] / closes[-30] - 1) * 100 if len(closes) >= 30 else 0
        ret_90  = (closes[-1] / closes[-90] - 1) * 100 if len(closes) >= 90 else 0
        ma50    = sum(closes[-50:])  / 50   if len(closes) >= 50  else None
        ma200   = sum(closes[-200:]) / 200  if len(closes) >= 200 else None

        return {
            "price":        round(price, 2),
            "ret_30":       round(ret_30, 1),
            "ret_90":       round(ret_90, 1),
            "above_ma50":   price > ma50   if ma50  else False,
            "above_ma200":  price > ma200  if ma200 else False,
            "ma50":         round(ma50,  2) if ma50  else None,
            "ma200":        round(ma200, 2) if ma200 else None,
        }
    except Exception:
        return None


def _score_etf(ticker, name, category, track, sector_signal,
               price_data, sector_sentiment, unified_regime, breadth):
    """Score one ETF. Returns 0-100 score with reasoning."""
    if not price_data:
        return None

    score    = 50.0  # base
    reasons  = []

    # ── MOMENTUM (both tracks) ───────────────────────────────────────────────
    r30 = price_data["ret_30"]
    r90 = price_data["ret_90"]

    if r90 > 15:   score += 15; reasons.append(f"Strong 90d: +{r90:.1f}%")
    elif r90 > 5:  score += 8;  reasons.append(f"Positive 90d: +{r90:.1f}%")
    elif r90 < -10: score -= 12; reasons.append(f"Weak 90d: {r90:.1f}%")
    elif r90 < 0:  score -= 5;  reasons.append(f"Negative 90d: {r90:.1f}%")

    if r30 > 8:    score += 8;  reasons.append(f"Strong 30d: +{r30:.1f}%")
    elif r30 > 2:  score += 4
    elif r30 < -5: score -= 8

    # ── MA POSITION (both tracks) ────────────────────────────────────────────
    if price_data["above_ma200"]:  score += 10; reasons.append("Above 200MA ✅")
    else:                          score -= 10; reasons.append("Below 200MA ⚠️")
    if price_data["above_ma50"]:   score += 5
    else:                          score -= 5

    # ── SECTOR SIGNAL BOOST (both tracks) ───────────────────────────────────
    if sector_signal and sector_sentiment:
        ss = sector_sentiment.get(sector_signal, {})
        net = ss.get("net_score", 0)
        if net > 200:   score += 20; reasons.append(f"{sector_signal} strongly bullish")
        elif net > 100: score += 12; reasons.append(f"{sector_signal} bullish")
        elif net > 50:  score += 6
        elif net < -100: score -= 15; reasons.append(f"{sector_signal} bearish")
        elif net < -50:  score -= 8

    # ── BREADTH OVERLAY ──────────────────────────────────────────────────────
    if breadth:
        sig = breadth.get("signal", "MODERATE")
        if category == "THEMATIC":
            # Thematic ETFs benefit from broad participation
            if sig == "BROAD_BULL":   score += 8;  reasons.append("Broad bull supports themes")
            elif sig == "BEAR_BREADTH": score -= 12; reasons.append("Narrow market — avoid themes")
            elif sig == "NARROW":     score -= 5
        elif category in ("CORE", "INTL"):
            if sig == "BROAD_BULL":   score += 5
            elif sig == "BEAR_BREADTH": score -= 5
        elif category == "DEFENSIVE":
            if sig == "BEAR_BREADTH": score += 8;  reasons.append("Breadth weak — defensive preferred")

    # ── REGIME CATEGORY WEIGHT ───────────────────────────────────────────────
    regime_key = unified_regime if unified_regime in REGIME_WEIGHTS else "NEUTRAL"
    cat_weight = REGIME_WEIGHTS[regime_key].get(category, 1.0)
    score = score * cat_weight

    # ── DEFENSIVE gets bonus in RISK_OFF ────────────────────────────────────
    if category == "DEFENSIVE" and unified_regime == "RISK_OFF":
        reasons.append("RISK_OFF — defensive preferred")

    return {
        "score":    min(100, max(0, round(score, 1))),
        "reasons":  reasons[:3],  # top 3 reasons
        "ret_30":   r30,
        "ret_90":   r90,
        "price":    price_data["price"],
        "above_ma200": price_data["above_ma200"],
    }


def run_etf_engine(sector_sentiment, unified_regime, breadth, verbose=True):
    """
    Score all ETFs, return ranked picks per account type.
    """
    if verbose:
        print("\n=======================================================")
        print("  ETF SIGNAL ENGINE")
        print("=======================================================")
        print(f"  Regime: {unified_regime} | Breadth: {breadth.get('signal','?') if breadth else '?'}")
        print(f"  Scoring {len(ETF_UNIVERSE)} ETFs...\n")

    regime_key  = unified_regime if unified_regime in REGIME_WEIGHTS else "NEUTRAL"
    allocation  = ACCOUNT_ALLOCATION.get(regime_key, ACCOUNT_ALLOCATION["NEUTRAL"])
    scored      = []

    for ticker, name, category, track, acct_pref, sector_signal in ETF_UNIVERSE:
        price_data = _fetch_etf_data(ticker)
        result     = _score_etf(ticker, name, category, track, sector_signal,
                                price_data, sector_sentiment, unified_regime, breadth)
        if not result:
            continue

        # Apply sector-specific boost from SECTOR_ETF_BOOST
        boost = 0
        for sector, boosts in SECTOR_ETF_BOOST.items():
            ss = sector_sentiment.get(sector, {}) if sector_sentiment else {}
            net = ss.get("net_score", 0)
            if net > 50:   # bullish signal
                for t, b in boosts:
                    if t == ticker and b > 0:
                        boost += b
            elif net < -50:  # bearish signal
                for t, b in boosts:
                    if t == ticker and b < 0:
                        boost += b

        final_score = min(100, max(0, round(result["score"] + boost * 0.5, 1)))

        scored.append({
            "ticker":       ticker,
            "name":         name,
            "category":     category,
            "track":        track,
            "acct_pref":    acct_pref,
            "sector_signal":sector_signal,
            "score":        final_score,
            "reasons":      result["reasons"],
            "ret_30":       result["ret_30"],
            "ret_90":       result["ret_90"],
            "price":        result["price"],
            "above_ma200":  result["above_ma200"],
            "signal_boost": boost,
        })

    scored.sort(key=lambda x: x["score"], reverse=True)

    # ── Build picks per account type ─────────────────────────────────────────
    def top_for_account(acct, n=5):
        """Top N ETFs for a given account type."""
        # RRSP: prefer US-listed (no .TO) OR explicitly tagged RRSP
        # TFSA: prefer .TO OR explicitly tagged TFSA
        # Show top by score but flag preferred ones
        picks = []
        for etf in scored:
            preferred = (
                (acct == "RRSP" and (etf["acct_pref"] == "RRSP" or not etf["ticker"].endswith(".TO")))
                or
                (acct in ("TFSA", "FHSA") and (etf["acct_pref"] == "TFSA" or etf["ticker"].endswith(".TO")))
            )
            picks.append({**etf, "preferred": preferred})

        # Sort: preferred first, then by score
        picks.sort(key=lambda x: (not x["preferred"], -x["score"]))
        return picks[:n]

    rrsp_picks  = top_for_account("RRSP",  5)
    tfsa_picks  = top_for_account("TFSA",  5)
    fhsa_picks  = top_for_account("FHSA",  4)

    if verbose:
        for acct, picks in [("RRSP", rrsp_picks), ("TFSA", tfsa_picks)]:
            print(f"  📊 {acct} TOP PICKS:")
            for p in picks:
                star = "⭐" if p["preferred"] else "  "
                print(f"  {star} {p['ticker']:10s} Score:{p['score']:5.1f}  "
                      f"90d:{p['ret_90']:+.1f}%  {p['name']}")
                if p["reasons"]:
                    print(f"       → {' | '.join(p['reasons'][:2])}")
            print()

    return {
        "scored":       scored,
        "rrsp_picks":   rrsp_picks,
        "tfsa_picks":   tfsa_picks,
        "fhsa_picks":   fhsa_picks,
        "allocation":   allocation,
        "regime":       unified_regime,
        "etf_count":    len(scored),
    }
