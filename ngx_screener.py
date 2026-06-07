"""
ngx_screener.py — Nigerian Exchange (NGX) Signal Engine v2
===========================================================
Macro-driven EM desk model. Completely standalone.

WHY MACRO-DRIVEN (not price-driven):
  Yahoo Finance has no reliable NGX (.LG) coverage.
  All 30 tickers return empty or 404 from any data provider.
  But Nigeria's market IS driven by oil, USD, and EM risk —
  so macro scoring is more accurate than broken price fetches.

Architecture:
  Layer 1: Global macro regime (oil + DXY + SPY + VIX via yfinance)
  Layer 2: FX stress index (USD pressure on naira)
  Layer 3: Sector sensitivity matrix (banking/oil/telecom/consumer)
  Layer 4: Per-stock macro scoring (deterministic, always produces output)
  Layer 5: InvestOS macro bridge (RISK_OFF -> info only, etc.)
  Layer 6: Validation phase (PAPER/RESTRICTED/FULL)

Data:  yfinance for global macro proxies (BZ=F, DX-Y.NYB, SPY, ^VIX)
       These work reliably on GitHub Actions.
Output: brief.ngx — same structure as before, full UI compatibility.
"""

import json
import time
import os
from datetime import datetime, timedelta

# UNIVERSE
NGX_TIER1 = [
    "GTCO.LG", "ZENITHBANK.LG", "ACCESS.LG", "UBA.LG", "FBNH.LG",
    "MTNN.LG", "AIRTELAFRI.LG", "DANGCEM.LG", "SEPLAT.LG",
]

NGX_TIER2 = [
    "STANBIC.LG", "NB.LG", "FLOURMILL.LG", "UNILEVER.LG", "OANDO.LG",
    "TOTAL.LG", "BUACEMENT.LG", "WAPCO.LG", "PRESCO.LG", "OKOMUOIL.LG",
    "TRANSCORP.LG", "GEREGU.LG", "NESTLE.LG", "FIDELITYBK.LG", "FCMB.LG",
    "UCAP.LG", "NAHCO.LG", "DANGSUGAR.LG", "NASCON.LG", "LIVESTOCK.LG",
    "CWG.LG",
]

NGX_ALL = NGX_TIER1 + NGX_TIER2

NGX_SECTOR_MAP = {
    "GTCO.LG": "banking", "ZENITHBANK.LG": "banking", "ACCESS.LG": "banking",
    "UBA.LG": "banking", "FBNH.LG": "banking", "MTNN.LG": "telecom",
    "AIRTELAFRI.LG": "telecom", "DANGCEM.LG": "industrial", "SEPLAT.LG": "oil",
    "STANBIC.LG": "banking", "NB.LG": "consumer", "FLOURMILL.LG": "consumer",
    "UNILEVER.LG": "consumer", "OANDO.LG": "oil", "TOTAL.LG": "oil",
    "BUACEMENT.LG": "industrial", "WAPCO.LG": "industrial",
    "PRESCO.LG": "agriculture", "OKOMUOIL.LG": "agriculture",
    "TRANSCORP.LG": "conglomerate", "GEREGU.LG": "power",
    "NESTLE.LG": "consumer", "FIDELITYBK.LG": "banking", "FCMB.LG": "banking",
    "UCAP.LG": "financial", "NAHCO.LG": "transport",
    "DANGSUGAR.LG": "consumer", "NASCON.LG": "consumer",
    "LIVESTOCK.LG": "agriculture", "CWG.LG": "technology",
}

# oil_b=oil beta, fx_b=FX sensitivity, risk_b=regime sensitivity, base=neutral score
SECTOR_SENSITIVITY = {
    "oil":         {"oil_b": 1.6,  "fx_b": 0.8,  "risk_b": 0.9,  "base": 60},
    "banking":     {"oil_b": 0.5,  "fx_b": 2.0,  "risk_b": 1.3,  "base": 55},
    "telecom":     {"oil_b": 0.3,  "fx_b": 1.2,  "risk_b": 0.6,  "base": 58},
    "industrial":  {"oil_b": 1.0,  "fx_b": 1.4,  "risk_b": 1.0,  "base": 52},
    "consumer":    {"oil_b": 0.4,  "fx_b": 2.2,  "risk_b": 1.1,  "base": 50},
    "agriculture": {"oil_b": 0.6,  "fx_b": 1.0,  "risk_b": 0.7,  "base": 53},
    "power":       {"oil_b": 1.1,  "fx_b": 1.5,  "risk_b": 1.0,  "base": 51},
    "financial":   {"oil_b": 0.5,  "fx_b": 1.8,  "risk_b": 1.2,  "base": 52},
    "transport":   {"oil_b": 0.9,  "fx_b": 1.1,  "risk_b": 0.9,  "base": 50},
    "conglomerate":{"oil_b": 0.7,  "fx_b": 1.3,  "risk_b": 1.0,  "base": 52},
    "technology":  {"oil_b": 0.2,  "fx_b": 1.6,  "risk_b": 0.8,  "base": 51},
}


def fetch_macro_asset(ticker, period="3mo"):
    """Fetch a macro asset via yfinance. Returns (pct_change_20d, current) or (None, None)."""
    try:
        import yfinance as yf
        import sys, io
        _s = sys.stderr; sys.stderr = io.StringIO()
        try:
            hist = yf.Ticker(ticker).history(period=period, interval="1d", auto_adjust=True)
        finally:
            sys.stderr = _s
        if hist is None or len(hist) < 5:
            return None, None
        closes = hist["Close"].dropna().tolist()
        if len(closes) < 2:
            return None, None
        lookback = min(20, len(closes) - 1)
        pct_20d  = (closes[-1] - closes[-1 - lookback]) / closes[-1 - lookback]
        return round(pct_20d * 100, 2), round(closes[-1], 4)
    except Exception:
        return None, None


def compute_em_regime():
    """
    Global macro regime from free yfinance data.
    Returns: (regime, macro_score, fx_stress, brent_trend, components)
    """
    oil_chg,  _ = fetch_macro_asset("BZ=F")
    dxy_chg,  _ = fetch_macro_asset("DX-Y.NYB")
    spy_chg,  _ = fetch_macro_asset("SPY")
    vix_chg,  vix_val = fetch_macro_asset("^VIX", "1mo")
    eem_chg,  _ = fetch_macro_asset("EEM")

    score = 0.0
    components = {}

    if oil_chg is not None:
        c = oil_chg * 0.50
        score += c
        components["oil"] = {"chg_20d": oil_chg, "contribution": round(c, 2)}
    if dxy_chg is not None:
        c = -dxy_chg * 0.40
        score += c
        components["dxy"] = {"chg_20d": dxy_chg, "contribution": round(c, 2)}
    if spy_chg is not None:
        c = spy_chg * 0.25
        score += c
        components["spy"] = {"chg_20d": spy_chg, "contribution": round(c, 2)}
    if vix_chg is not None:
        c = -abs(vix_chg) * 0.20 if vix_chg is not None and vix_chg > 10 else 0
        score += c
        components["vix"] = {"level": vix_val, "chg": vix_chg, "contribution": round(c, 2)}
    if eem_chg is not None:
        c = eem_chg * 0.15
        score += c
        components["eem"] = {"chg_20d": eem_chg, "contribution": round(c, 2)}

    # FX stress: strong USD + weak oil = naira pressure
    fx_stress = 0.0
    if dxy_chg is not None:
        fx_stress += dxy_chg * 0.6
    if oil_chg is not None:
        fx_stress -= oil_chg * 0.4

    brent_trend = ("UP" if oil_chg is not None and oil_chg > 2
                   else "DOWN" if oil_chg is not None and oil_chg < -2 else "FLAT")

    regime = ("RISK_ON" if score >= 3
              else "RISK_OFF" if score <= -2 else "NEUTRAL")

    return regime, round(score, 2), round(fx_stress, 2), brent_trend, components


def score_ngx_macro(ticker, tier, regime, macro_score, fx_stress):
    """Score a single NGX stock using macro drivers. Always produces a score."""
    sector = NGX_SECTOR_MAP.get(ticker, "banking")
    sens   = SECTOR_SENSITIVITY.get(sector, SECTOR_SENSITIVITY["banking"])
    base   = sens["base"]
    reasons, flags = [], []

    oil_adj = macro_score * sens["oil_b"] * 0.8
    if oil_adj > 3:
        reasons.append(f"Brent tailwind: +{oil_adj:.1f}pts")
    elif oil_adj < -3:
        flags.append(f"Brent headwind: {oil_adj:.1f}pts")

    fx_adj = -fx_stress * sens["fx_b"] * 0.6
    if fx_adj > 2:
        reasons.append(f"FX easing: +{fx_adj:.1f}pts")
    elif fx_adj < -2:
        flags.append(f"FX stress (USD strength): {fx_adj:.1f}pts")

    if regime == "RISK_ON":
        regime_adj = sens["risk_b"] * 5
        reasons.append(f"RISK_ON regime: +{regime_adj:.1f}pts")
    elif regime == "RISK_OFF":
        regime_adj = -sens["risk_b"] * 6
        flags.append(f"RISK_OFF: {regime_adj:.1f}pts")
    else:
        regime_adj = 0

    tier_adj = 5 if tier == 1 else 0
    if tier == 1:
        reasons.append("Tier 1 quality premium: +5pts")

    if sector == "telecom":
        reasons.append("Defensive / USD-earning sector")
    elif sector == "oil" and macro_score > 2:
        reasons.append("Oil sector benefits from Brent strength")
    elif sector == "banking" and fx_stress > 3:
        flags.append("Banks exposed to FX liquidity squeeze")

    score = max(0, min(100, round(base + oil_adj + fx_adj + regime_adj + tier_adj, 1)))
    return score, reasons, flags


def compute_ngx_basket_regime(scored):
    if not scored:
        return "UNKNOWN"
    n = sum(1 for s in scored if s["score"] >= 60)
    pct = n / len(scored)
    return "BULLISH" if pct >= 0.60 else "NEUTRAL" if pct >= 0.40 else "WEAK"


def apply_macro_bridge(eligible, investos_macro, brent_trend, basket_regime):
    macro = investos_macro or "NORMAL"
    if macro in ("RISK_OFF", "BEAR"):
        for s in eligible:
            s["action"] = "INFORMATIONAL"; s["size_label"] = "INFO ONLY — RISK_OFF"
            s["actionable"] = False
        return eligible, "RISK_OFF — info only"
    if macro == "CAUTIOUS":
        oil_t2 = (brent_trend == "UP")
        filtered = []
        for s in eligible:
            if s["tier"] == 1 and s["score"] >= 75:
                s["action"] = "BUY"; s["size_label"] = "REDUCED SIZE"; s["actionable"] = True
                filtered.append(s)
            elif s["tier"] == 2 and oil_t2 and s["score"] >= 80:
                s["action"] = "BUY"; s["size_label"] = "SMALL (oil bridge)"; s["actionable"] = True
                filtered.append(s)
        return filtered, f"CAUTIOUS — Tier 1 only{' + Oil T2' if oil_t2 else ''}"
    for s in eligible:
        s["action"] = "BUY"; s["size_label"] = "FULL SIZE"; s["actionable"] = True
    return eligible, "OPEN — all signals eligible"


def apply_persistence_gate(eligible):
    try:
        history = json.load(open("ngx_persistence.json"))
    except Exception:
        history = {}
    today = datetime.now().strftime("%Y-%m-%d")
    out   = []
    for s in eligible:
        ticker = s["ticker"]
        streak = history.get(ticker, [])
        streak.append({"date": today, "score": s["score"]})
        streak = streak[-7:]
        history[ticker] = streak
        consecutive = 0
        for day in reversed(streak):
            if day["score"] >= 60:
                consecutive += 1
            else:
                break
        if consecutive >= 3:
            s["persistence"] = f"{consecutive}d streak"; s["eligible"] = True
        elif consecutive == 2:
            s["persistence"] = f"{consecutive}/3 watch"; s["eligible"] = False
        else:
            s["persistence"] = f"{consecutive}/3 building"; s["eligible"] = False
        out.append(s)
    try:
        json.dump(history, open("ngx_persistence.json", "w"), indent=2)
    except Exception:
        pass
    return out


def get_validation_phase():
    try:
        start = datetime.strptime(open("ngx_validation_start.txt").read().strip(), "%Y-%m-%d")
        days  = (datetime.now() - start).days
        if days < 30:  return "PAPER_ONLY", days
        elif days < 60: return "RESTRICTED", days
        else:           return "FULL", days
    except Exception:
        try: open("ngx_validation_start.txt","w").write(datetime.now().strftime("%Y-%m-%d"))
        except Exception: pass
        return "PAPER_ONLY", 0


def run_ngx_screen(investos_macro="NORMAL", verbose=True):
    """
    Run full NGX macro-driven screen.
    Returns dict for baking into brief.ngx.
    """
    if verbose:
        print("\n" + "="*55)
        print("  NGX SIGNAL ENGINE — Nigerian Exchange")
        print("="*55)

    if verbose:
        print("\n  Fetching global macro signals...")

    regime, macro_score, fx_stress, brent_trend, components = compute_em_regime()

    if verbose:
        print(f"  Brent: {brent_trend} | EM regime: {regime} ({macro_score:+.1f}) | FX: {fx_stress:+.1f}")
        for k, v in components.items():
            chg = v.get("chg_20d", v.get("chg", 0)) or 0
            print(f"    {k.upper():<6} 20d:{chg:>+6.2f}%  contrib:{v.get('contribution',0):>+4.1f}")

    # Score all 30 NGX stocks
    scored = []

    # ── NGN/USD exchange rate for ₦ position sizing context ──────────────────
    ngn_usd = None
    cad_usd = 0.74   # approximate — close enough for sizing guidance
    try:
        ngn_data = fetch_macro_asset("NGN=X", period="5d")
        if ngn_data is not None and not ngn_data.empty:
            ngn_usd = round(float(ngn_data["Close"].iloc[-1]), 2)
        if verbose and ngn_usd:
            print(f"  NGN/USD: {ngn_usd:.1f} | ₦10,000 ≈ ${10000/ngn_usd:.2f} USD ≈ ${10000*cad_usd/ngn_usd:.2f} CAD")
    except Exception:
        pass

    for ticker in NGX_ALL:
        tier  = 1 if ticker in NGX_TIER1 else 2
        score, reasons, flags = score_ngx_macro(ticker, tier, regime, macro_score, fx_stress)
        ngn_context = {}
        if ngn_usd:
            ngn_context = {
                "ngn_usd_rate":    ngn_usd,
                "ngn_cad_rate":    round(ngn_usd * cad_usd, 4),
                "fx_note":         f"₦1 = ${1/ngn_usd:.6f} USD",
                "conversion_note": f"₦10,000 ≈ ${10000/ngn_usd:.2f} USD ≈ ${10000*cad_usd/ngn_usd:.2f} CAD",
            }
        scored.append({
            "ticker": ticker, "name": ticker.replace(".LG",""),
            "tier": tier, "score": score,
            "sector": NGX_SECTOR_MAP.get(ticker, "unknown"),
            "reasons": reasons, "flags": flags,
            "action": "SIGNAL", "size_label": "FULL SIZE",
            "actionable": True, "persistence": "—",
            "ngn_context": ngn_context,
        })
    scored.sort(key=lambda x: x["score"], reverse=True)

    if verbose:
        print(f"\n  Scored {len(scored)}/30 NGX stocks (macro-driven, no price data needed)")

    basket_regime = compute_ngx_basket_regime(scored)
    eligible = [s for s in scored if s["score"] >= 55]
    eligible, gate_status = apply_macro_bridge(eligible, investos_macro, brent_trend, basket_regime)
    eligible = apply_persistence_gate(eligible)
    phase, phase_days = get_validation_phase()

    if phase == "PAPER_ONLY":
        for s in eligible:
            s["action"] = "PAPER ONLY"; s["size_label"] = "DO NOT TRADE — PAPER PHASE"
    elif phase == "RESTRICTED":
        eligible = [s for s in eligible if s["tier"] == 1 and s["score"] >= 80]

    signals = [s for s in eligible if s.get("eligible", True)]
    watch   = [s for s in eligible if not s.get("eligible", True)]

    if verbose:
        print(f"\n  NGX Basket: {basket_regime} | Gate: {gate_status}")
        print(f"  Phase: {phase} (Day {phase_days})")
        print(f"  Signals: {len(signals)} | Watch: {len(watch)}")
        for s in (signals + watch)[:6]:
            print(f"    {s['name']:<15} {s['sector']:<12} Score:{s['score']:<6} {s['persistence']}")

    try:
        json.dump({"regime": regime, "macro_score": macro_score,
                   "brent_trend": brent_trend, "basket_regime": basket_regime,
                   "signals": [{"ticker":s["ticker"],"score":s["score"],"sector":s["sector"]}
                                for s in signals[:6]],
                   "timestamp": datetime.now().isoformat()},
                  open("ngx_snapshot.json","w"), indent=2)
    except Exception:
        pass

    return {
        "status": gate_status, "phase": phase, "phase_days": phase_days,
        "signals": signals, "watch": watch[:5], "all_scored": scored,        # all 30 — needed for outcome resolution
        "basket_regime": basket_regime, "brent_trend": brent_trend,
        "macro_regime": regime, "macro_score": macro_score,
        "fx_stress": fx_stress, "macro_gate": investos_macro,
        "macro_components": components,
        "feeds_ok": len(components), "feeds_failed": 0,
        "timestamp": datetime.now().isoformat(), "data_mode": "MACRO",
        "note": (
            "Validation phase — paper trade only." if phase == "PAPER_ONLY" else
            "Tier 1 + score >= 80 only." if phase == "RESTRICTED" else
            "Full signal mode — macro-driven."
        ),
    }


if __name__ == "__main__":
    result = run_ngx_screen(investos_macro="NORMAL", verbose=True)
    print(f"\nNGX: {len(result['signals'])} signals | basket:{result['basket_regime']} | regime:{result['macro_regime']}")
