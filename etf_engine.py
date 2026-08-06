"""
etf_engine.py — InvestOS ETF Signal Engine v2.1
==============================================
Scores 32 ETFs (+ XOM/CVX as energy confirmation signals).

v2.1: Score cap by category — prevents RISK_ON multiplier from inflating
thematic ETFs to 100 regardless of actual quality difference.
  THEMATIC (ARKG, BOTZ, CIBR etc.): capped at 85
  SECTOR commodity (XLE, XEG, GLD): capped at 88
  CORE, DEFENSIVE: uncapped
"""

import yfinance as yf
from datetime import datetime, date
import traceback

ETF_UNIVERSE = [
    ("VFV.TO",  "S&P 500 (CAD)",         "CORE",      "core",     "ALL",  None,              0.09, "index",     "sp500"),
    ("VOO",     "S&P 500 (USD)",          "CORE",      "core",     "RRSP", None,              0.03, "index",     "sp500"),
    ("XIC.TO",  "TSX Composite",          "CORE",      "core",     "ALL",  "TSX_BROAD",       0.06, "index",     "tsx_broad"),
    ("XEQT.TO", "All-Equity Global",      "CORE",      "core",     "FHSA", "TSX_BROAD",       0.17, "index",     "tsx_broad"),
    ("QQQ",     "NASDAQ 100",             "CORE",      "core",     "RRSP", "TECH",            0.20, "index",     "nasdaq"),
    ("XEF.TO",  "Intl Developed",         "CORE",      "core",     "TFSA", None,              0.22, "index",     "intl_dev"),
    ("VWO",     "Emerging Markets",       "CORE",      "core",     "RRSP", None,              0.08, "index",     "em"),
    ("XEG.TO",  "Canadian Energy",        "SECTOR",    "core",     "TFSA", "CANADIAN_ENERGY", 0.61, "commodity", "cdn_energy"),
    ("XLE",     "US Energy",              "SECTOR",    "core",     "RRSP", "OIL_PRODUCERS",   0.09, "commodity", "us_energy"),
    ("ZEB.TO",  "Canadian Banks",         "SECTOR",    "core",     "FHSA", "CANADIAN_BANKS",  0.28, "earnings",  "cdn_banks"),
    ("ZGD.TO",  "Canadian Gold Miners",   "SECTOR",    "core",     "TFSA", "GOLD",            0.61, "commodity", "gold"),
    ("GLD",     "Gold",                   "SECTOR",    "core",     "RRSP", "GOLD",            0.40, "commodity", "gold"),
    ("XRE.TO",  "Canadian REITs",         "SECTOR",    "core",     "TFSA", "CANADIAN_REITS",  0.61, "earnings",  "cdn_reit"),
    ("BOTZ",    "Robotics & AI",          "THEMATIC",  "thematic", "RRSP", "TECH",            0.69, "theme",     "ai_tech"),
    ("SMH",     "Semiconductors",         "THEMATIC",  "thematic", "RRSP", "TECH",            0.35, "earnings",  "semis"),
    ("SKYY",    "Cloud Computing",        "THEMATIC",  "thematic", "RRSP", "TECH",            0.68, "theme",     "cloud"),
    ("CIBR",    "Cybersecurity",          "THEMATIC",  "thematic", "RRSP", "CYBERSECURITY",   0.60, "theme",     "cyber"),
    ("ITA",     "Aerospace & Defense",    "THEMATIC",  "thematic", "RRSP", "DEFENSE",         0.40, "earnings",  "defense"),
    ("PPA",     "Aerospace & Defense",    "THEMATIC",  "thematic", "RRSP", "DEFENSE",         0.20, "earnings",  "defense"),
    ("SHLD",    "Defense Tech",           "THEMATIC",  "thematic", "RRSP", "DEFENSE",         0.50, "theme",     "defense"),
    ("QTUM",    "Quantum Computing",      "THEMATIC",  "thematic", "RRSP", "TECH",            0.76, "theme",     "quantum"),
    ("ARKG",    "Genomics",               "THEMATIC",  "thematic", "RRSP", "BIOTECH",         0.75, "theme",     "genomics"),
    ("BLOK",    "Blockchain",             "THEMATIC",  "thematic", "RRSP", "TECH",            0.76, "theme",     "blockchain"),
    ("ZAG.TO",  "Canadian Agg Bonds",     "DEFENSIVE", "core",     "FHSA", None,              0.14, "bond",      "cdn_bond"),
    ("XBB.TO",  "Canadian Bond Universe", "DEFENSIVE", "core",     "FHSA", None,              0.10, "bond",      "cdn_bond"),
    ("TLT",     "US Long Bonds",          "DEFENSIVE", "core",     "RRSP", None,              0.15, "bond",      "us_ltbond"),
    ("ZLB.TO",  "Canadian Low-Vol Eq",    "DEFENSIVE", "core",     "FHSA", None,              0.39, "index",     "cdn_lowvol"),
    ("EFA",     "Intl Developed",         "INTL",      "core",     "RRSP", None,              0.32, "index",     "intl_dev"),
    ("EEM",     "Emerging Markets",       "INTL",      "core",     "RRSP", None,              0.68, "index",     "em"),
    ("XOM",     "ExxonMobil",             "SIGNAL",    "core",     "RRSP", "OIL_PRODUCERS",   0.00, "earnings",  "us_energy"),
    ("CVX",     "Chevron",                "SIGNAL",    "core",     "RRSP", "OIL_PRODUCERS",   0.00, "earnings",  "us_energy"),
]

FHSA_PREFERRED = {"XEQT.TO","VFV.TO","XIC.TO","ZEB.TO","ZAG.TO","XBB.TO","ZLB.TO","XEF.TO"}
FHSA_AVOID     = {"BOTZ","SMH","SKYY","CIBR","ITA","SHLD","PPA","QTUM","ARKG","BLOK",
                   "GLD","TLT","VWO","EEM","XLE","QQQ","VOO","XOM","CVX"}

OVERLAP_GROUPS = {
    "sp500": "S&P 500 (pick one)", "tsx_broad": "TSX broad", "cdn_energy": "Canadian energy",
    "us_energy": "US energy", "gold": "Gold", "cdn_bond": "Canadian bonds",
    "defense": "Defense", "intl_dev": "International developed", "em": "Emerging markets",
}

SECTOR_ETF_BOOST = {
    "DEFENSE":         [("ITA",+25),("SHLD",+20),("PPA",+20)],
    "GOLD":            [("GLD",+25),("ZGD.TO",+25)],
    "OIL":             [("XEG.TO",+20),("XLE",+20)],
    "CANADIAN_ENERGY": [("XEG.TO",+20),("XLE",+10)],
    "OIL_PRODUCERS":   [("XEG.TO",+15),("XLE",+20)],
    "TECH":            [("SMH",+20),("BOTZ",+15),("QQQ",+15),("SKYY",+10)],
    "CYBERSECURITY":   [("CIBR",+25)],
    "BIOTECH":         [("ARKG",+20)],
    "CANADIAN_BANKS":  [("ZEB.TO",+20)],
    "AIRLINES":        [("ITA",-10)],
}

REGIME_WEIGHTS = {
    "RISK_ON":  {"CORE":1.0,"SECTOR":1.0,"THEMATIC":1.0,"DEFENSIVE":0.3,"INTL":1.0,"SIGNAL":1.0},
    "NEUTRAL":  {"CORE":0.8,"SECTOR":0.7,"THEMATIC":0.6,"DEFENSIVE":0.8,"INTL":0.7,"SIGNAL":0.8},
    "RISK_OFF": {"CORE":0.5,"SECTOR":0.4,"THEMATIC":0.3,"DEFENSIVE":1.2,"INTL":0.4,"SIGNAL":0.4},
}

ACCOUNT_ALLOCATION = {
    "RISK_ON":  {"equity_pct":85,"bond_pct":5, "gold_pct":5, "cash_pct":5},
    "NEUTRAL":  {"equity_pct":60,"bond_pct":25,"gold_pct":10,"cash_pct":5},
    "RISK_OFF": {"equity_pct":35,"bond_pct":40,"gold_pct":15,"cash_pct":10},
}


def _fetch_etf_data(ticker):
    try:
        t    = yf.Ticker(ticker)
        hist = t.history(period="12mo", interval="1d", auto_adjust=True)
        if hist.empty or len(hist) < 20:
            return None
        closes = hist["Close"].dropna().tolist()
        if len(closes) < 20:
            return None

        price  = closes[-1]
        ret_30 = (closes[-1]/closes[-30]-1)*100 if len(closes)>=30 else 0
        ret_90 = (closes[-1]/closes[-90]-1)*100 if len(closes)>=90 else 0
        ma50   = sum(closes[-50:])/50   if len(closes)>=50  else None
        ma200  = sum(closes[-200:])/200 if len(closes)>=200 else None

        hi52  = max(closes); lo52 = min(closes); rng = hi52 - lo52
        range_pct    = round((price - lo52) / rng * 100) if rng > 0 else 50
        overextended = range_pct >= 80
        dca_recommended = overextended or (price > (ma200 or 0) and range_pct >= 70)

        div_yield = div_5yr_avg = div_flag = None
        try:
            info = t.info
            div_yield   = info.get("dividendYield") or info.get("trailingAnnualDividendYield")
            div_5yr_avg = info.get("fiveYearAvgDividendYield")
            if div_yield:   div_yield   = round(div_yield * 100, 2)
            if div_5yr_avg: div_5yr_avg = round(div_5yr_avg, 2)
            if div_yield and div_5yr_avg and div_5yr_avg > 0:
                ratio = div_yield / div_5yr_avg
                div_flag = "HIGH" if ratio > 1.5 else "NORMAL" if ratio > 0.8 else "LOW"
            elif div_yield and div_yield > 0: div_flag = "NORMAL"
            else: div_flag = "NONE"
        except: pass

        return {
            "price": round(price, 2), "ret_30": round(ret_30, 1), "ret_90": round(ret_90, 1),
            "above_ma50": price > ma50 if ma50 else False,
            "above_ma200": price > ma200 if ma200 else False,
            "ma50": round(ma50, 2) if ma50 else None,
            "ma200": round(ma200, 2) if ma200 else None,
            "hi52": round(hi52, 2), "lo52": round(lo52, 2),
            "range_pct": range_pct, "overextended": overextended,
            "dca_recommended": dca_recommended,
            "div_yield": div_yield, "div_5yr_avg": div_5yr_avg, "div_flag": div_flag,
        }
    except: return None


def _score_etf(ticker, name, category, track, sector_signal,
               price_data, sector_sentiment, unified_regime, breadth):
    if not price_data:
        return None

    score = 50.0
    reasons = []
    r90 = price_data["ret_90"]
    r30 = price_data["ret_30"]

    # Momentum
    if r90 > 15:    score += 15; reasons.append(f"Strong 90d: +{r90:.1f}%")
    elif r90 > 5:   score += 8;  reasons.append(f"Positive 90d: +{r90:.1f}%")
    elif r90 < -10: score -= 12; reasons.append(f"Weak 90d: {r90:.1f}%")
    elif r90 < 0:   score -= 5;  reasons.append(f"Negative 90d: {r90:.1f}%")
    if r30 > 8:     score += 8;  reasons.append(f"Strong 30d: +{r30:.1f}%")
    elif r30 > 2:   score += 4
    elif r30 < -5:  score -= 8

    # Overextended
    if price_data.get("overextended"):
        score -= 8
        reasons.append(f"Overextended: top {100-price_data['range_pct']}% of 52wk range")

    # MA position
    if price_data["above_ma200"]:  score += 10; reasons.append("Above 200MA ✅")
    else:                          score -= 10; reasons.append("Below 200MA ⚠️")
    if price_data["above_ma50"]:   score += 5
    else:                          score -= 5

    # Sector signal
    if sector_signal and sector_sentiment:
        net = sector_sentiment.get(sector_signal, {}).get("net_score", 0)
        if net > 200:    score += 20; reasons.append(f"{sector_signal} strongly bullish")
        elif net > 100:  score += 12; reasons.append(f"{sector_signal} bullish")
        elif net > 50:   score += 6
        elif net < -100: score -= 15; reasons.append(f"{sector_signal} bearish")
        elif net < -50:  score -= 8

    # Breadth overlay
    if breadth:
        sig = breadth.get("signal","MODERATE")
        if category == "THEMATIC":
            if sig == "BROAD_BULL":    score += 8;  reasons.append("Broad market supports themes")
            elif sig == "BEAR_BREADTH":score -= 12; reasons.append("Narrow market — avoid themes")
            elif sig == "NARROW":      score -= 5
        elif category == "DEFENSIVE":
            if sig == "BEAR_BREADTH":  score += 8;  reasons.append("Weak breadth — defensive preferred")

    # Regime weight
    rk    = unified_regime if unified_regime in REGIME_WEIGHTS else "NEUTRAL"
    score = score * REGIME_WEIGHTS[rk].get(category, 1.0)

    # ── SCORE CAP v2.1 — prevent RISK_ON multiplier from inflating to 100 ────
    # In RISK_ON, regime weight=1.0 + strong momentum + sector boost can push
    # thematic ETFs to 100, making them indistinguishable from each other.
    # Caps force meaningful ranking within each category.
    #   THEMATIC: max 85 — ARKG, ITA, CIBR, SKYY, BOTZ, SHLD, QTUM, BLOK
    #   SECTOR commodity: max 88 — XLE, XEG.TO, GLD, ZGD.TO
    #   CORE, DEFENSIVE, INTL: uncapped — anchor positions, let them score freely
    if category == "THEMATIC":
        score = min(score, 85)
    elif category == "SECTOR" and track == "core":
        # Only cap commodity sector ETFs, not earnings-driven sector ETFs
        thesis_map = {t[0]: t[7] for t in ETF_UNIVERSE}
        if thesis_map.get(ticker) == "commodity":
            score = min(score, 88)

    return {
        "score":      min(100, max(0, round(score, 1))),
        "reasons":    reasons[:3],
        "ret_30":     r30,
        "ret_90":     r90,
        "price":      price_data["price"],
        "above_ma200":price_data["above_ma200"],
    }


def run_etf_engine(sector_sentiment, unified_regime, breadth, verbose=True):
    if verbose:
        print("\n=======================================================")
        print("  ETF SIGNAL ENGINE")
        print("=======================================================")
        print(f"  Regime: {unified_regime} | Breadth: {breadth.get('signal','?') if breadth else '?'}")
        n_etfs = len([e for e in ETF_UNIVERSE if e[2] != 'SIGNAL'])
        print(f"  Scoring {n_etfs} ETFs + XOM/CVX confirmation...\n")

    rk         = unified_regime if unified_regime in REGIME_WEIGHTS else "NEUTRAL"
    allocation = ACCOUNT_ALLOCATION.get(rk, ACCOUNT_ALLOCATION["NEUTRAL"])

    # Energy confirmation
    energy_confirmed = False
    xom_data = _fetch_etf_data("XOM")
    cvx_data = _fetch_etf_data("CVX")
    if xom_data and cvx_data:
        energy_confirmed = (
            xom_data["above_ma200"] and cvx_data["above_ma200"] and
            xom_data["ret_90"] > 0   and cvx_data["ret_90"] > 0
        )
        if verbose:
            xom_ok = "✅" if xom_data["above_ma200"] else "❌"
            cvx_ok = "✅" if cvx_data["above_ma200"] else "❌"
            print(f"  Energy confirmation: XOM {xom_ok} {xom_data['ret_90']:+.1f}% | "
                  f"CVX {cvx_ok} {cvx_data['ret_90']:+.1f}% | "
                  f"{'CONFIRMED ✅' if energy_confirmed else 'NOT confirmed ⚠️'}")

    # Score all ETFs
    scored = []
    for row in ETF_UNIVERSE:
        ticker, name, category, track, acct_pref, sector_signal, mer, thesis, overlap_group = row
        if category == "SIGNAL":
            continue
        price_data = _fetch_etf_data(ticker)
        result     = _score_etf(ticker, name, category, track, sector_signal,
                                price_data, sector_sentiment, unified_regime, breadth)
        if not result:
            continue

        boost = 0
        for sector, boosts in SECTOR_ETF_BOOST.items():
            net = (sector_sentiment or {}).get(sector, {}).get("net_score", 0)
            for t, b in boosts:
                if t == ticker:
                    if net > 50  and b > 0: boost += b
                    if net < -50 and b < 0: boost += b

        if overlap_group in ("cdn_energy","us_energy") and energy_confirmed:
            boost += 10

        final_score = min(100, max(0, round(result["score"] + boost * 0.5, 1)))
        # Re-apply cap after boost (boost can push capped scores over limit)
        if category == "THEMATIC":
            final_score = min(final_score, 85)
        elif category == "SECTOR" and thesis == "commodity":
            final_score = min(final_score, 88)

        pd_data = price_data or {}
        scored.append({
            "ticker": ticker, "name": name, "category": category,
            "track": track, "acct_pref": acct_pref, "sector_signal": sector_signal,
            "overlap_group": overlap_group, "mer": mer, "thesis": thesis,
            "score": final_score, "reasons": result["reasons"],
            "ret_30": result["ret_30"], "ret_90": result["ret_90"],
            "price": result["price"], "above_ma200": result["above_ma200"],
            "signal_boost": boost,
            "range_pct": pd_data.get("range_pct", 50),
            "overextended": pd_data.get("overextended", False),
            "dca_recommended": pd_data.get("dca_recommended", False),
            "div_yield": pd_data.get("div_yield"),
            "div_flag": pd_data.get("div_flag"),
            "energy_confirmed": energy_confirmed if overlap_group in ("cdn_energy","us_energy") else None,
        })

    scored.sort(key=lambda x: x["score"], reverse=True)

    def dedupe(picks):
        seen_groups = set(); out = []
        for p in picks:
            grp = p.get("overlap_group")
            if grp and grp in seen_groups: continue
            if grp: seen_groups.add(grp)
            out.append(p)
        return out

    def top_for_account(acct, n=5):
        picks = []
        for e in scored:
            ticker = e["ticker"]
            if acct == "RRSP":
                preferred = not ticker.endswith(".TO") or e["acct_pref"] == "RRSP"
            elif acct == "TFSA":
                preferred = ticker.endswith(".TO") or e["acct_pref"] == "TFSA"
            else:
                if ticker in FHSA_AVOID: continue
                preferred = ticker in FHSA_PREFERRED
            picks.append({**e, "preferred": preferred})
        picks.sort(key=lambda x: (not x["preferred"], -x["score"]))
        return dedupe(picks)[:n]

    rrsp_picks = top_for_account("RRSP", 5)
    tfsa_picks = top_for_account("TFSA", 5)
    fhsa_picks = top_for_account("FHSA", 5)

    if verbose:
        for acct, picks in [("RRSP",rrsp_picks),("TFSA",tfsa_picks),("FHSA",fhsa_picks)]:
            print(f"  📊 {acct} TOP PICKS:")
            for p in picks:
                star = "⭐" if p["preferred"] else "  "
                dca  = " [DCA]" if p.get("dca_recommended") else ""
                ext  = " ⚠️OVEREXTENDED" if p.get("overextended") else ""
                cap  = " [CAPPED]" if p["category"] in ("THEMATIC",) and p["score"] == 85 else ""
                print(f"  {star} {p['ticker']:10s} Score:{p['score']:5.1f}  "
                      f"90d:{p['ret_90']:+.1f}%  MER:{p['mer']:.2f}%{dca}{ext}{cap}")
            print()

    return {
        "scored": scored, "rrsp_picks": rrsp_picks,
        "tfsa_picks": tfsa_picks, "fhsa_picks": fhsa_picks,
        "allocation": allocation, "regime": unified_regime,
        "etf_count": len(scored), "energy_confirmed": energy_confirmed,
    }
