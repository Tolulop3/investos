"""
ngx_screener.py — Nigerian Exchange (NGX) Signal Engine v2.1
===========================================================
Macro-driven EM desk model. Completely standalone.

v2.1 changes (guardrail fix):
  - eligible gate: 55 → 40 (stocks always appear in dashboard strip)
  - WATCH floor: 45 (shows in NGX strip even in RISK_OFF — informational)
  - ENTER threshold: 65 (was 75) + RISK_ON + Tier 1 + 3d persistence
  - RESTRICTED phase threshold: 80 → 65 (achievable in NEUTRAL/RISK_ON)
  - RISK_OFF no longer silences WATCH signals — they appear as tracking

WHY THIS MATTERS:
  With macro_score=-9.2 (current), old thresholds produced 0 signals and 0 watch.
  Nothing appeared in the dashboard NGX strip.
  New thresholds: telecom (~52), some banking (~40-45) show as WATCH/WAIT,
  building persistence history so signals fire immediately when macro turns.

WHY MACRO-DRIVEN:
  Yahoo Finance has no reliable NGX (.LG) coverage.
  All 30 tickers return empty or 404 from any data provider.
  Nigeria's market IS driven by oil, USD, and EM risk —
  macro scoring is more accurate than broken price fetches.
"""

import json
import time
import os
from datetime import datetime, timedelta
try:
    from ngx_price_engine import (gate_level, fetch_all_ngx_prices,
                                   blend_price_with_macro, get_gate_status_label,
                                   fetch_ngx_market_snapshot)
    _NGX_PRICE_ENGINE = True
except ImportError:
    _NGX_PRICE_ENGINE = False
    def gate_level(): return 0

# ── UNIVERSE ──────────────────────────────────────────────────────────────────
NGX_TIER1 = [
    "GTCO.LG", "ZENITHBANK.LG", "ACCESSCORP.LG", "UBA.LG", "FIRSTHOLDCO.LG",
    "MTNN.LG", "AIRTELAFRI.LG", "DANGCEM.LG", "SEPLAT.LG",
]

NGX_TIER2 = [
    "STANBIC.LG", "NB.LG", "UNILEVER.LG", "OANDO.LG",
    "TOTAL.LG", "BUACEMENT.LG", "HBMNG.LG", "PRESCO.LG", "OKOMUOIL.LG",
    "TRANSCORP.LG", "GEREGU.LG", "NESTLE.LG", "FIDELITYBK.LG", "FCMB.LG",
    "UCAP.LG", "NAHCO.LG", "DANGSUGAR.LG", "NASCON.LG", "LIVESTOCK.LG",
    "CWG.LG",
]

NGX_ALL = NGX_TIER1 + NGX_TIER2

# NGX_SECTOR_MAP feeds SECTOR_SENSITIVITY (below) — the live macro-scoring
# engine for the current 29 tickers (score_ngx_macro()). Left UNCHANGED,
# deliberately, when NGX_API_SECTOR_MAP was introduced: swapping these
# values to the /v1/companies taxonomy would silently break scoring for
# every current ticker (SECTOR_SENSITIVITY has no matching keys for the
# new category strings, so every ticker would fall back to the "banking"
# profile). Do not touch without a deliberate SECTOR_SENSITIVITY remap.
NGX_SECTOR_MAP = {
    "GTCO.LG": "banking",      "ZENITHBANK.LG": "banking", "ACCESSCORP.LG": "banking",
    "UBA.LG": "banking",       "FIRSTHOLDCO.LG": "banking","MTNN.LG": "telecom",
    "AIRTELAFRI.LG": "telecom","DANGCEM.LG": "industrial",  "SEPLAT.LG": "oil",
    "STANBIC.LG": "banking",   "NB.LG": "consumer",
    "UNILEVER.LG": "consumer", "OANDO.LG": "oil",          "TOTAL.LG": "oil",
    "BUACEMENT.LG": "industrial","HBMNG.LG": "industrial",
    "PRESCO.LG": "agriculture","OKOMUOIL.LG": "agriculture",
    "TRANSCORP.LG": "conglomerate","GEREGU.LG": "power",
    "NESTLE.LG": "consumer",   "FIDELITYBK.LG": "banking", "FCMB.LG": "banking",
    "UCAP.LG": "financial",    "NAHCO.LG": "transport",
    "DANGSUGAR.LG": "consumer","NASCON.LG": "consumer",
    "LIVESTOCK.LG": "agriculture","CWG.LG": "technology",

    # ── NGX Session D: ticker-to-sector bridge for the 56 PAPER_ONLY
    # candidates (NGX_TIER3_CANDIDATES), added 2026-07-23. These tickers are
    # NOT in NGX_ALL / main scoring rotation -- adding them here only means
    # score_ngx_macro() resolves a real sector if ever called for one of
    # them; it does not put them into live scoring (that's gated separately
    # by NGX_ALL membership, untouched). The 29 entries above are unchanged.
    #
    # Mechanical (36): NGX_API_SECTOR_MAP category maps unambiguously to one
    # existing legacy key -- no judgment call.
    "ARADEL.LG": "oil", "CONOIL.LG": "oil", "ETERNA.LG": "oil", "JAPAULGOLD.LG": "oil",
    "BERGER.LG": "industrial", "BETAGLAS.LG": "industrial", "CAP.LG": "industrial",
    "BUAFOODS.LG": "consumer", "CADBURY.LG": "consumer", "CHAMPION.LG": "consumer",
    "GUINNESS.LG": "consumer", "HONYFLOUR.LG": "consumer", "INTBREW.LG": "consumer",
    "PZ.LG": "consumer", "VITAFOAM.LG": "consumer",
    "CUSTODIAN.LG": "conglomerate", "UACN.LG": "conglomerate",
    "ELLAHLAKES.LG": "agriculture", "FTNCOCOA.LG": "agriculture", "ZICHIS.LG": "agriculture",
    "FIDSON.LG": "HEALTHCARE", "MAYBAKER.LG": "HEALTHCARE",
    "MECURE.LG": "HEALTHCARE", "NEIMETH.LG": "HEALTHCARE",
    "JBERGER.LG": "CONSTRUCTION/REAL ESTATE", "NIDF.LG": "CONSTRUCTION/REAL ESTATE",
    "NREIT.LG": "CONSTRUCTION/REAL ESTATE", "UPDC.LG": "CONSTRUCTION/REAL ESTATE",
    "UPDCREIT.LG": "CONSTRUCTION/REAL ESTATE",
    "VFDGROUP.LG": "INVESTMENT",
    "EUNISELL.LG": "SERVICES", "IKEJAHOTEL.LG": "SERVICES", "SKYAVN.LG": "SERVICES",
    "TIP.LG": "SERVICES", "TRANSCOHOT.LG": "SERVICES",
    "TRANSPOWER.LG": "power",

    # Judgment (14): FINANCIAL SERVICES / ICT tickers assigned to the more
    # granular legacy key by per-ticker evidence (see Session D proposal for
    # full reasoning). Tier A = confirmed via ngx_api_debug.json's real
    # sub_sector field. Tier B = ticker-symbol substring or brand evidence.
    "ETI.LG": "banking", "WEMABANK.LG": "banking",                 # Tier A: sub_sector="Banking"
    "STERLINGNG.LG": "banking", "JAIZBANK.LG": "banking",          # Tier A: sub_sector="Banking"
    "NGXGROUP.LG": "financial",     # Tier A: sub_sector="Other Financial Institutions"
    "AIICO.LG": "financial",        # Tier A: sub_sector="Insurance Carriers..."
    "LINKASSURE.LG": "financial",   # Tier B: "ASSURE" in ticker
    "SOVRENINS.LG": "financial",    # Tier B: "INS" suffix in ticker
    "NEM.LG": "financial",          # Tier B: ticker is the insurer's own brand initials
    "NPFMCRFBK.LG": "banking",      # Tier B: ticker spells out "microfinance bank"
    "MANSARD.LG": "financial",      # Tier B: brand recognition (AXA Mansard Insurance)
    "CORNERST.LG": "financial",     # Tier B: brand recognition (Cornerstone Insurance)
    "WAPIC.LG": "financial",        # Tier B: brand recognition (Wapic Insurance)
    "ETRANZACT.LG": "technology",   # Tier B: payments-switching fintech, not a telecom carrier

    # Tier C (6): no local corroboration for any of these -- muted default
    # (SERVICES) rather than a forced banking/financial or telecom/technology
    # guess. CHAMS flagged as the one worth a real lookup in a future
    # session if it's worth resolving properly; not done here.
    "ABBEYBDS.LG": "SERVICES", "INFINITY.LG": "SERVICES", "AFRIPRUD.LG": "SERVICES",
    "CONHALLPLC.LG": "SERVICES", "MBENEFIT.LG": "SERVICES", "CHAMS.LG": "SERVICES",
}

# ── NGX universe expansion (Session B, Part 2) ───────────────────────────────
# 56 candidate tickers, filtered from the live /v1/companies universe (150
# symbols) by market_cap >= NGN 25B and volume not-null & >= 1,000 (see
# session analysis — thresholds chosen so 0 of the current 29 are excluded).
# NOT yet part of NGX_ALL / main scoring rotation — see NGX_TIER3_CANDIDATES.
#
# NGX_API_SECTOR_MAP is the /v1/companies 13-category sector taxonomy,
# covering all 85 tickers (current 29 + these 56). Reporting / future
# sector-diversity purposes ONLY — NOT wired into SECTOR_SENSITIVITY or
# score_ngx_macro(), unlike NGX_SECTOR_MAP above. This is the canonical
# taxonomy going forward for any NEW sector-diversity logic once the 56
# actually enter scoring.
NGX_TIER3_CANDIDATES = [
    "ABBEYBDS.LG", "AFRIPRUD.LG", "AIICO.LG", "ARADEL.LG", "BERGER.LG",
    "BETAGLAS.LG", "BUAFOODS.LG", "CADBURY.LG", "CAP.LG", "CHAMPION.LG",
    "CHAMS.LG", "CONHALLPLC.LG", "CONOIL.LG", "CORNERST.LG", "CUSTODIAN.LG",
    "ELLAHLAKES.LG", "ETERNA.LG", "ETI.LG", "ETRANZACT.LG", "EUNISELL.LG",
    "FIDSON.LG", "FTNCOCOA.LG", "GUINNESS.LG", "HONYFLOUR.LG", "IKEJAHOTEL.LG",
    "INFINITY.LG", "INTBREW.LG", "JAIZBANK.LG", "JAPAULGOLD.LG", "JBERGER.LG",
    "LINKASSURE.LG", "MANSARD.LG", "MAYBAKER.LG", "MBENEFIT.LG", "MECURE.LG",
    "NEIMETH.LG", "NEM.LG", "NGXGROUP.LG", "NIDF.LG", "NPFMCRFBK.LG",
    "NREIT.LG", "PZ.LG", "SKYAVN.LG", "SOVRENINS.LG", "STERLINGNG.LG",
    "TIP.LG", "TRANSCOHOT.LG", "TRANSPOWER.LG", "UACN.LG", "UPDC.LG",
    "UPDCREIT.LG", "VFDGROUP.LG", "VITAFOAM.LG", "WAPIC.LG", "WEMABANK.LG",
    "ZICHIS.LG",
]

NGX_API_SECTOR_MAP = {
    # Current 29 (unchanged tickers, new taxonomy for reporting only)
    "GTCO.LG": "FINANCIAL SERVICES", "ZENITHBANK.LG": "FINANCIAL SERVICES",
    "ACCESSCORP.LG": "FINANCIAL SERVICES", "UBA.LG": "FINANCIAL SERVICES",
    "FIRSTHOLDCO.LG": "FINANCIAL SERVICES", "MTNN.LG": "ICT",
    "AIRTELAFRI.LG": "ICT", "DANGCEM.LG": "INDUSTRIAL GOODS",
    "SEPLAT.LG": "OIL AND GAS", "STANBIC.LG": "FINANCIAL SERVICES",
    "NB.LG": "CONSUMER GOODS", "UNILEVER.LG": "CONSUMER GOODS",
    "OANDO.LG": "OIL AND GAS", "TOTAL.LG": "OIL AND GAS",
    "BUACEMENT.LG": "INDUSTRIAL GOODS", "HBMNG.LG": "INDUSTRIAL GOODS",
    "PRESCO.LG": "AGRICULTURE", "OKOMUOIL.LG": "AGRICULTURE",
    "TRANSCORP.LG": "CONGLOMERATES", "GEREGU.LG": "UTILITIES",
    "NESTLE.LG": "CONSUMER GOODS", "FIDELITYBK.LG": "FINANCIAL SERVICES",
    "FCMB.LG": "FINANCIAL SERVICES", "UCAP.LG": "FINANCIAL SERVICES",
    "NAHCO.LG": "SERVICES", "DANGSUGAR.LG": "CONSUMER GOODS",
    "NASCON.LG": "CONSUMER GOODS", "LIVESTOCK.LG": "AGRICULTURE",
    "CWG.LG": "ICT",
    # 56 new candidates (NGX_TIER3_CANDIDATES)
    "ABBEYBDS.LG": "FINANCIAL SERVICES", "AFRIPRUD.LG": "FINANCIAL SERVICES",
    "AIICO.LG": "FINANCIAL SERVICES", "ARADEL.LG": "OIL AND GAS",
    "BERGER.LG": "INDUSTRIAL GOODS", "BETAGLAS.LG": "INDUSTRIAL GOODS",
    "BUAFOODS.LG": "CONSUMER GOODS", "CADBURY.LG": "CONSUMER GOODS",
    "CAP.LG": "INDUSTRIAL GOODS", "CHAMPION.LG": "CONSUMER GOODS",
    "CHAMS.LG": "ICT", "CONHALLPLC.LG": "FINANCIAL SERVICES",
    "CONOIL.LG": "OIL AND GAS", "CORNERST.LG": "FINANCIAL SERVICES",
    "CUSTODIAN.LG": "CONGLOMERATES", "ELLAHLAKES.LG": "AGRICULTURE",
    "ETERNA.LG": "OIL AND GAS", "ETI.LG": "FINANCIAL SERVICES",
    "ETRANZACT.LG": "ICT", "EUNISELL.LG": "SERVICES",
    "FIDSON.LG": "HEALTHCARE", "FTNCOCOA.LG": "AGRICULTURE",
    "GUINNESS.LG": "CONSUMER GOODS", "HONYFLOUR.LG": "CONSUMER GOODS",
    "IKEJAHOTEL.LG": "SERVICES", "INFINITY.LG": "FINANCIAL SERVICES",
    "INTBREW.LG": "CONSUMER GOODS", "JAIZBANK.LG": "FINANCIAL SERVICES",
    "JAPAULGOLD.LG": "OIL AND GAS", "JBERGER.LG": "CONSTRUCTION/REAL ESTATE",
    "LINKASSURE.LG": "FINANCIAL SERVICES", "MANSARD.LG": "FINANCIAL SERVICES",
    "MAYBAKER.LG": "HEALTHCARE", "MBENEFIT.LG": "FINANCIAL SERVICES",
    "MECURE.LG": "HEALTHCARE", "NEIMETH.LG": "HEALTHCARE",
    "NEM.LG": "FINANCIAL SERVICES", "NGXGROUP.LG": "FINANCIAL SERVICES",
    "NIDF.LG": "CONSTRUCTION/REAL ESTATE", "NPFMCRFBK.LG": "FINANCIAL SERVICES",
    "NREIT.LG": "CONSTRUCTION/REAL ESTATE", "PZ.LG": "CONSUMER GOODS",
    "SKYAVN.LG": "SERVICES", "SOVRENINS.LG": "FINANCIAL SERVICES",
    "STERLINGNG.LG": "FINANCIAL SERVICES", "TIP.LG": "SERVICES",
    "TRANSCOHOT.LG": "SERVICES", "TRANSPOWER.LG": "UTILITIES",
    "UACN.LG": "CONGLOMERATES", "UPDC.LG": "CONSTRUCTION/REAL ESTATE",
    "UPDCREIT.LG": "CONSTRUCTION/REAL ESTATE", "VFDGROUP.LG": "INVESTMENT",
    "VITAFOAM.LG": "CONSUMER GOODS", "WAPIC.LG": "FINANCIAL SERVICES",
    "WEMABANK.LG": "FINANCIAL SERVICES", "ZICHIS.LG": "AGRICULTURE",
}

# oil_b=oil beta, fx_b=FX sensitivity, risk_b=regime sensitivity, base=neutral score
#
# The 11 entries below are the ORIGINAL live-scoring set for the current 29 tickers
# (keyed by NGX_SECTOR_MAP's lowercase labels) — untouched by the NGX Session C
# sector expansion below.
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

    # ── NGX Session C: coefficients for API-taxonomy sectors with no legacy
    # equivalent, added for the 56-ticker universe expansion. NOT wired into
    # score_ngx_macro() yet — that still reads NGX_SECTOR_MAP exclusively, so
    # this addition changes zero live scoring for the current 29 tickers.
    # These candidates are PAPER_ONLY under the per-ticker validation clock for
    # 30-60+ days; no urgency to wire them in before they accumulate real
    # resolved-outcome history of their own.
    #
    # HEALTHCARE: directionally-informed, not backtested. Nigerian pharma
    # manufacturers (FIDSON, MAYBAKER, MECURE, NEIMETH) import most active
    # pharmaceutical ingredients in USD, so fx_b sits above the muted floor —
    # but demand is inelastic/defensive (people need medicine regardless of
    # the macro cycle), so risk_b stays at the table's defensive floor, same
    # as telecom. No direct oil-price channel, so oil_b matches technology's
    # floor.
    "HEALTHCARE": {"oil_b": 0.2, "fx_b": 1.3, "risk_b": 0.6, "base": 51},

    # Shared muted default: CONSTRUCTION/REAL ESTATE, INVESTMENT, SERVICES.
    # No sector-specific conviction for any of these three — each is either
    # internally heterogeneous (CONSTRUCTION/REAL ESTATE mixes construction
    # contractors like JBERGER with income-focused REITs like NIDF/NREIT/
    # UPDCREIT; SERVICES mixes aviation handling, hotels, and logistics) or
    # too thin to generalize from (INVESTMENT has exactly one ticker,
    # VFDGROUP). Sits at or below the table's floor on every coefficient —
    # damped macro-adjustment until each sector earns real coefficients from
    # its own resolved-outcome history, same posture as PAPER_ONLY gating for
    # new tickers.
    "CONSTRUCTION/REAL ESTATE": {"oil_b": 0.3, "fx_b": 1.0, "risk_b": 0.6, "base": 50},
    "INVESTMENT":               {"oil_b": 0.3, "fx_b": 1.0, "risk_b": 0.6, "base": 50},
    "SERVICES":                 {"oil_b": 0.3, "fx_b": 1.0, "risk_b": 0.6, "base": 50},

    # FINANCIAL SERVICES and ICT are DELIBERATELY absent. Each collapses two
    # legacy sectors with genuinely different coefficients (banking vs.
    # financial; telecom vs. technology) — inventing a single blended number
    # would wash out a real distinction. When the ticker-level bridge is
    # built (see NGX_API_SECTOR_TO_SENSITIVITY note below), each ticker in
    # these two API categories should route to the more granular legacy key
    # by the same kind of per-ticker judgment NGX_SECTOR_MAP already uses.
    #
    # NATURAL RESOURCES is DELIBERATELY absent. Zero of the 85 tracked
    # tickers are tagged this way today (JAPAULGOLD, the one plausible
    # candidate, is currently tagged OIL AND GAS in NGX_API_SECTOR_MAP —
    # separate pre-existing data point, not fixed here, out of scope for
    # this session). No basis for a coefficient with zero member tickers.
    # If a ticker is ever tagged NATURAL RESOURCES before real coefficients
    # exist, use the shared muted default above explicitly — do not let it
    # fall through to score_ngx_macro()'s "banking" fallback, which was
    # calibrated for a sector this has nothing to do with.
}

# Canonical API-sector (NGX_API_SECTOR_MAP value) → SECTOR_SENSITIVITY key.
# Only categories that resolve to exactly ONE coefficient set are listed.
# FINANCIAL SERVICES and ICT are intentionally excluded — they need
# per-ticker resolution to the granular legacy key, not a single lookup here.
# NATURAL RESOURCES is intentionally excluded — no member tickers, no
# coefficients; see comment above.
#
# NOT wired into score_ngx_macro() or NGX_SECTOR_MAP. This is prep only —
# the actual next NGX task is building a per-ticker map from each of the 56
# NGX_TIER3_CANDIDATES to one of the existing granular SECTOR_SENSITIVITY
# keys (an NGX_SECTOR_MAP-equivalent for the expansion tickers), the same
# way NGX_SECTOR_MAP does today for the current 29. That bridge is NOT built
# in this session — this mapping only proves coefficients exist to route to
# once it is.
NGX_API_SECTOR_TO_SENSITIVITY = {
    "AGRICULTURE":              "agriculture",
    "CONGLOMERATES":            "conglomerate",
    "CONSUMER GOODS":           "consumer",
    "INDUSTRIAL GOODS":         "industrial",
    "OIL AND GAS":              "oil",
    "UTILITIES":                "power",
    "HEALTHCARE":               "HEALTHCARE",
    "CONSTRUCTION/REAL ESTATE": "CONSTRUCTION/REAL ESTATE",
    "INVESTMENT":               "INVESTMENT",
    "SERVICES":                 "SERVICES",
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
    oil_chg, _        = fetch_macro_asset("BZ=F")
    dxy_chg, _        = fetch_macro_asset("DX-Y.NYB")
    spy_chg, _        = fetch_macro_asset("SPY")
    vix_chg, vix_val  = fetch_macro_asset("^VIX", "1mo")
    eem_chg, _        = fetch_macro_asset("EEM")

    score = 0.0
    components = {}

    if oil_chg is not None:
        c = oil_chg * 0.50; score += c
        components["oil"] = {"chg_20d": oil_chg, "contribution": round(c, 2)}
    if dxy_chg is not None:
        c = -dxy_chg * 0.40; score += c
        components["dxy"] = {"chg_20d": dxy_chg, "contribution": round(c, 2)}
    if spy_chg is not None:
        c = spy_chg * 0.25; score += c
        components["spy"] = {"chg_20d": spy_chg, "contribution": round(c, 2)}
    if vix_chg is not None:
        c = -abs(vix_chg) * 0.20 if vix_chg > 10 else 0; score += c
        components["vix"] = {"level": vix_val, "chg": vix_chg, "contribution": round(c, 2)}
    if eem_chg is not None:
        c = eem_chg * 0.15; score += c
        components["eem"] = {"chg_20d": eem_chg, "contribution": round(c, 2)}

    fx_stress = 0.0
    if dxy_chg is not None: fx_stress += dxy_chg * 0.6
    if oil_chg is not None:  fx_stress -= oil_chg * 0.4

    brent_trend = ("UP"   if oil_chg is not None and oil_chg >  2 else
                   "DOWN" if oil_chg is not None and oil_chg < -2 else "FLAT")

    regime = ("RISK_ON"  if score >= 3 else
              "RISK_OFF" if score <= -2 else "NEUTRAL")

    return regime, round(score, 2), round(fx_stress, 2), brent_trend, components


def score_ngx_macro(ticker, tier, regime, macro_score, fx_stress):
    """Score a single NGX stock using macro drivers."""
    sector = NGX_SECTOR_MAP.get(ticker, "banking")
    sens   = SECTOR_SENSITIVITY.get(sector, SECTOR_SENSITIVITY["banking"])
    base   = sens["base"]
    reasons, flags = [], []

    oil_adj = macro_score * sens["oil_b"] * 0.8
    if oil_adj > 3:    reasons.append(f"Brent tailwind: +{oil_adj:.1f}pts")
    elif oil_adj < -3: flags.append(f"Brent headwind: {oil_adj:.1f}pts")

    fx_adj = -fx_stress * sens["fx_b"] * 0.6
    if fx_adj > 2:     reasons.append(f"FX easing: +{fx_adj:.1f}pts")
    elif fx_adj < -2:  flags.append(f"FX stress (USD strength): {fx_adj:.1f}pts")

    if regime == "RISK_ON":
        regime_adj = sens["risk_b"] * 5
        reasons.append(f"RISK_ON regime: +{regime_adj:.1f}pts")
    elif regime == "RISK_OFF":
        regime_adj = -sens["risk_b"] * 6
        flags.append(f"RISK_OFF: {regime_adj:.1f}pts")
    else:
        regime_adj = 0

    tier_adj = 5 if tier == 1 else 0
    if tier == 1: reasons.append("Tier 1 quality premium: +5pts")

    if sector == "telecom":
        reasons.append("Defensive / USD-earning sector")
    elif sector == "oil" and macro_score > 2:
        reasons.append("Oil sector benefits from Brent strength")
    elif sector == "banking" and fx_stress > 3:
        flags.append("Banks exposed to FX liquidity squeeze")

    score = max(0, min(100, round(base + oil_adj + fx_adj + regime_adj + tier_adj, 1)))
    return score, reasons, flags


def compute_ngx_basket_regime(scored):
    if not scored: return "UNKNOWN"
    n   = sum(1 for s in scored if s["score"] >= 55)
    pct = n / len(scored)
    return "BULLISH" if pct >= 0.60 else "NEUTRAL" if pct >= 0.40 else "WEAK"


def apply_macro_bridge(all_scored, investos_macro, brent_trend, basket_regime):
    """
    v2.1: RISK_OFF no longer silences WATCH signals.
    WATCH = informational tracking (score 45-64), always shown.
    ENTER = requires RISK_ON + score >= 65 + Tier 1.
    Returns (signals_list, watch_list, gate_status).
    """
    macro = investos_macro or "NORMAL"

    signals = []
    watch   = []

    for s in all_scored:
        score  = s["score"]
        tier   = s["tier"]

        # ── WAIT: score below watch floor ────────────────────────────────────
        if score < 45:
            s["action"] = "WAIT"
            s["size_label"] = "WAIT — score below 45"
            s["actionable"] = False
            continue

        # ── WATCH: score 45-64 OR RISK_OFF (always informational) ────────────
        if score < 65 or macro in ("RISK_OFF", "BEAR"):
            s["action"]     = "WATCH"
            s["size_label"] = f"WATCH — tracking ({'RISK_OFF' if macro in ('RISK_OFF','BEAR') else 'score <65'})"
            s["actionable"] = False
            watch.append(s)
            continue

        # ── CAUTIOUS: Tier 1 only, smaller size ──────────────────────────────
        if macro == "CAUTIOUS":
            if tier == 1 and score >= 65:
                s["action"]     = "WATCH"
                s["size_label"] = "CAUTIOUS — Tier 1 watch"
                s["actionable"] = False
                watch.append(s)
            elif tier == 1 and score >= 75:
                s["action"]     = "BUY"
                s["size_label"] = "REDUCED SIZE (cautious)"
                s["actionable"] = True
                signals.append(s)
            else:
                s["action"] = "WAIT"; s["actionable"] = False
            continue

        # ── ENTER: RISK_ON + score >= 65 + Tier 1 ────────────────────────────
        if macro not in ("RISK_OFF","BEAR","CAUTIOUS"):
            if tier == 1 and score >= 65:
                s["action"]     = "BUY"
                s["size_label"] = "FULL SIZE" if score >= 75 else "SMALL (watch threshold)"
                s["actionable"] = True
                signals.append(s)
            elif tier == 2 and score >= 75:
                s["action"]     = "BUY"
                s["size_label"] = "SMALL (Tier 2)"
                s["actionable"] = True
                signals.append(s)
            else:
                s["action"]     = "WATCH"
                s["size_label"] = "WATCH — Tier 2 or score marginal"
                s["actionable"] = False
                watch.append(s)

    if macro in ("RISK_OFF", "BEAR"):
        gate_status = "RISK_OFF — WATCH only, no entries"
    elif macro == "CAUTIOUS":
        gate_status = f"CAUTIOUS — Tier 1 ≥65 watch, ≥75 enter"
    else:
        gate_status = "OPEN — Tier 1 ≥65, Tier 2 ≥75 eligible"

    return signals, watch, gate_status


def apply_persistence_gate(signals):
    """Require 3 consecutive scoring days at threshold before ENTER fires."""
    try:
        history = json.load(open("ngx_persistence.json"))
    except Exception:
        history = {}
    today = datetime.now().strftime("%Y-%m-%d")
    out   = []
    for s in signals:
        ticker = s["ticker"]
        streak = history.get(ticker, [])
        streak.append({"date": today, "score": s["score"]})
        streak = streak[-7:]
        history[ticker] = streak
        consecutive = 0
        for day in reversed(streak):
            if day["score"] >= 65:
                consecutive += 1
            else:
                break
        if consecutive >= 3:
            s["persistence"] = f"{consecutive}d streak ✅"; s["eligible"] = True
        elif consecutive == 2:
            s["persistence"] = f"{consecutive}/3 days"; s["eligible"] = False
        else:
            s["persistence"] = f"{consecutive}/3 building"; s["eligible"] = False
        out.append(s)
    try:
        json.dump(history, open("ngx_persistence.json","w"), indent=2)
    except Exception:
        pass
    return out


NGX_TICKER_START_FILE = "ngx_ticker_start_dates.json"


def _load_ticker_start_dates():
    try:
        return json.load(open(NGX_TICKER_START_FILE))
    except Exception:
        return {}


def _save_ticker_start_dates(dates):
    try:
        json.dump(dates, open(NGX_TICKER_START_FILE, "w"), indent=2, sort_keys=True)
    except Exception:
        pass


def get_validation_phase(ticker):
    """
    Per-ticker validation clock — replaces the single global clock that
    used to read one date from ngx_validation_start.txt (that file is no
    longer read; left in place, unused, as a historical artifact).

    Each ticker gets its own first-seen date in ngx_ticker_start_dates.json.
    A ticker with no entry yet is brand new — it gets today's date written
    and starts at PAPER_ONLY, Day 0. This is what makes new universe
    additions always start fresh regardless of how long the existing
    universe has been running (see ngx_persistence.json for the same
    per-ticker-JSON-state precedent already used in this codebase).
    """
    dates = _load_ticker_start_dates()
    if ticker not in dates:
        dates[ticker] = datetime.now().strftime("%Y-%m-%d")
        _save_ticker_start_dates(dates)
        return "PAPER_ONLY", 0

    start = datetime.strptime(dates[ticker], "%Y-%m-%d")
    days  = (datetime.now() - start).days
    if days < 30:   return "PAPER_ONLY", days
    elif days < 60: return "RESTRICTED", days
    else:           return "FULL", days


def run_ngx_screen(investos_macro="NORMAL", verbose=True):
    """Run full NGX macro-driven screen. Returns dict for brief.ngx."""
    if verbose:
        print("\n" + "="*55)
        print("  NGX SIGNAL ENGINE — Nigerian Exchange")
        print("="*55)
        print("\n  Fetching global macro signals...")

    regime, macro_score, fx_stress, brent_trend, components = compute_em_regime()

    if verbose:
        print(f"  Brent: {brent_trend} | EM regime: {regime} ({macro_score:+.1f}) | FX: {fx_stress:+.1f}")
        for k, v in components.items():
            chg = v.get("chg_20d", v.get("chg", 0)) or 0
            print(f"    {k.upper():<6} 20d:{chg:>+6.2f}%  contrib:{v.get('contribution',0):>+4.1f}")

    # ── NGN/USD rate for ₦ position sizing context ────────────────────────────
    ngn_usd = None; cad_usd = 0.74
    try:
        ngn_data = fetch_macro_asset("NGN=X", period="5d")
        if ngn_data and ngn_data[1]:
            ngn_usd = round(float(ngn_data[1]), 2)
        if verbose and ngn_usd:
            usd_per_10k = 10000 / ngn_usd
            cad_per_10k = usd_per_10k / cad_usd
            print(f"  ₦/USD: {ngn_usd:.1f} | $1 USD = ₦{ngn_usd:.0f} | "
                  f"₦10,000 ≈ ${usd_per_10k:.2f} USD (~${cad_per_10k:.2f} CAD)")
    except Exception:
        pass

    # ── Score all 30 NGX stocks ───────────────────────────────────────────────
    scored = []
    for ticker in NGX_ALL:
        tier  = 1 if ticker in NGX_TIER1 else 2
        score, reasons, flags = score_ngx_macro(ticker, tier, regime, macro_score, fx_stress)
        ngn_context = {}
        if ngn_usd:
            ngn_context = {
                "ngn_usd_rate":    ngn_usd,
                "fx_note":         f"$1 USD = ₦{ngn_usd:.0f}",
                "conversion_note": f"₦10,000 ≈ ${10000/ngn_usd:.2f} USD (~${10000/ngn_usd/cad_usd:.2f} CAD)",
            }
        scored.append({
            "ticker": ticker, "name": ticker.replace(".LG",""),
            "tier": tier, "score": score,
            "sector": NGX_SECTOR_MAP.get(ticker,"unknown"),
            "reasons": reasons, "flags": flags,
            "action": "WAIT", "size_label": "—",
            "actionable": False, "persistence": "—",
            "ngn_context": ngn_context,
        })
    # ── Gate 1: Blend price data if NGN_MARKETS_KEY is set ─────────────────
    _gate = gate_level() if _NGX_PRICE_ENGINE else 0
    _prices = {}
    _ngx_market = {}
    if _gate >= 1 and _NGX_PRICE_ENGINE:
        if verbose:
            print(f"  🔑 Gate {_gate} active: {get_gate_status_label(_gate)}")
            print(f"  📡 Fetching NGX price data...")
        _prices = fetch_all_ngx_prices(NGX_ALL, verbose=verbose)
        # Load market snapshot separately (already fetched inside fetch_all_ngx_prices)
        try:
            import json as _j
            _cache = _j.load(open("ngx_price_cache.json"))
            _ngx_market = _cache.get("prices", {}).get("__market__", {}).get("data", {})
        except Exception:
            pass
        # Apply NGX breadth adjustment to base macro score
        # adv_dec_ratio < 0.4 = bearish NGX market
        if _ngx_market:
            _adr = _ngx_market.get("adv_dec_ratio", 0.5)
            _asi_chg = _ngx_market.get("asi_change_pct", 0) or 0
            if verbose:
                _adv = _ngx_market.get("advancers", 0)
                _dec = _ngx_market.get("decliners", 0)
                print(f"  📊 NGX market: ASI {_asi_chg:+.2f}% | "
                      f"{_adv} adv / {_dec} dec (ratio: {_adr:.2f})")
            # Penalise all scores when NGX breadth is very bearish
            if _adr < 0.30:
                for s in scored:
                    s["score"] = max(0, round(s["score"] * 0.90, 1))
                if verbose:
                    print(f"  ⚠️  NGX breadth very bearish (ratio {_adr:.2f}) — scores dampened 10%")
        for s in scored:
            pd = _prices.get(s["ticker"])
            if pd:
                blended, price_reasons = blend_price_with_macro(
                    s["score"], pd, s["ticker"])
                s["score"]         = blended
                s["price_data"]    = pd
                s["price_reasons"] = price_reasons
                s["reasons"]       = s["reasons"] + price_reasons
    else:
        if verbose and _NGX_PRICE_ENGINE:
            print(f"  ℹ️  Gate 0: macro-only scoring (set NGN_MARKETS_KEY for price data)")
        _gate = 0

    scored.sort(key=lambda x: x["score"], reverse=True)

    if verbose:
        print(f"\n  Scored {len(scored)}/30 NGX stocks (macro-driven, no price data needed)")

    basket_regime = compute_ngx_basket_regime(scored)

    # ── v2.1 gate: WATCH shows even in RISK_OFF ───────────────────────────────
    signals, watch, gate_status = apply_macro_bridge(scored, investos_macro, brent_trend, basket_regime)

    # Apply persistence gate only to ENTER-eligible signals
    signals = apply_persistence_gate(signals)

    # Per-ticker validation phase — replaces the single global clock.
    # Computed once for the whole universe (also ensures any brand-new
    # ticker gets its Day-0 start date written immediately). Different
    # tickers can be in different phases simultaneously now.
    ticker_phases = {t: get_validation_phase(t) for t in NGX_ALL}
    phase_counts = {"PAPER_ONLY": 0, "RESTRICTED": 0, "FULL": 0}
    for _ph, _d in ticker_phases.values():
        phase_counts[_ph] += 1

    filtered_signals = []
    for s in signals:
        sig_phase, sig_phase_days = ticker_phases.get(s["ticker"]) or get_validation_phase(s["ticker"])
        s["phase"], s["phase_days"] = sig_phase, sig_phase_days
        if sig_phase == "PAPER_ONLY":
            s["action"] = "PAPER ONLY"
            s["size_label"] = "DO NOT TRADE — PAPER PHASE"
            filtered_signals.append(s)
        elif sig_phase == "RESTRICTED":
            # v2.1: threshold lowered from 80 → 65 (now achievable in NEUTRAL/RISK_ON)
            if s["tier"] == 1 and s["score"] >= 65:
                filtered_signals.append(s)
        else:  # FULL
            filtered_signals.append(s)
    signals = filtered_signals

    for s in watch:
        w_phase, w_phase_days = ticker_phases.get(s["ticker"]) or get_validation_phase(s["ticker"])
        s["phase"], s["phase_days"] = w_phase, w_phase_days
        if w_phase == "PAPER_ONLY":
            s["action"] = "PAPER ONLY"
            s["size_label"] = "DO NOT TRADE — PAPER PHASE"

    # Dominant phase/day — a single summary value for display/backward-compat
    # (e.g. the brief's "note" field). The authoritative per-ticker values
    # live on each signal/watch dict and in ngx_ticker_start_dates.json.
    phase = max(phase_counts, key=phase_counts.get)
    phase_days = next((d for t, (p, d) in ticker_phases.items() if p == phase), 0)

    eligible_signals = [s for s in signals if s.get("eligible", True)]

    if verbose:
        gate_label = f"Gate {_gate}: {get_gate_status_label(_gate)}" if _NGX_PRICE_ENGINE else "Gate 0: macro-only"
    print(f"\n  NGX Basket: {basket_regime} | {gate_label}")
    if _prices:
        ok = sum(1 for t in NGX_ALL if t in _prices)
        print(f"  💰 Price data: {ok}/{len(NGX_ALL)} tickers fetched from NGN Markets")
        print(f"  Phases (per-ticker): {phase_counts['FULL']} FULL | "
              f"{phase_counts['RESTRICTED']} RESTRICTED | {phase_counts['PAPER_ONLY']} PAPER_ONLY")
        print(f"  Signals: {len(eligible_signals)} | Watch: {len(watch)}")
        if watch:
            print(f"  WATCH picks (tracking, not actionable):")
            for s in watch[:5]:
                print(f"    {s['name']:<15} {s['sector']:<12} Score:{s['score']:<6} {s['action']}")
        for s in eligible_signals[:4]:
            print(f"    {s['name']:<15} {s['sector']:<12} Score:{s['score']:<6} {s['persistence']}")

    try:
        json.dump({
            "regime": regime, "macro_score": macro_score,
            "brent_trend": brent_trend, "basket_regime": basket_regime,
            "signals": [{"ticker":s["ticker"],"score":s["score"],"sector":s["sector"]}
                        for s in eligible_signals[:6]],
            "watch":   [{"ticker":s["ticker"],"score":s["score"],"sector":s["sector"]}
                        for s in watch[:6]],
            "timestamp": datetime.now().isoformat(),
        }, open("ngx_snapshot.json","w"), indent=2)
    except Exception:
        pass

    return {
        "status":          gate_status,
        "phase":           phase,
        "phase_days":      phase_days,
        "signals":         eligible_signals,
        "watch":           watch[:8],
        "all_scored":      scored,
        "basket_regime":   basket_regime,
        "brent_trend":     brent_trend,
        "macro_regime":    regime,
        "macro_score":     macro_score,
        "fx_stress":       fx_stress,
        "macro_gate":      investos_macro,
        "macro_components":components,
        "feeds_ok":        len(components),
        "feeds_failed":    0,
        "timestamp":       datetime.now().isoformat(),
        "data_mode":       "MACRO",
        "note": (
            "Validation phase — paper trade only." if phase == "PAPER_ONLY" else
            "Tier 1 + score ≥65 only (RESTRICTED)." if phase == "RESTRICTED" else
            "Full signal mode — macro-driven."
        ),
    }


def test_lg_suffix():
    """
    Test whether yfinance can fetch NGX tickers with the .LG suffix.
    Does NOT modify the live ticker list — diagnostic only.
    If 4+/5 work, flag for full NGX yfinance rewrite.
    """
    TEST_TICKERS = [
        "DANGCEM.LG", "MTNN.LG", "GTCO.LG", "ZENITHBANK.LG", "ACCESSCORP.LG"
    ]
    print("\n" + "="*55)
    print("  .LG SUFFIX TEST — yfinance NGX ticker probe")
    print("="*55)

    try:
        import yfinance as yf
        import sys, io
    except ImportError:
        print("  ❌ yfinance not installed — cannot run test")
        return

    results = []
    for ticker in TEST_TICKERS:
        try:
            _s = sys.stderr; sys.stderr = io.StringIO()
            try:
                hist = yf.Ticker(ticker).history(period="1mo", interval="1d", auto_adjust=True)
            finally:
                sys.stderr = _s
            rows = len(hist) if hist is not None else 0
            if rows > 0:
                last_close = round(hist["Close"].dropna().iloc[-1], 4)
                status = "✅ WORKING"
                results.append(True)
            else:
                last_close = None
                status = "❌ FAILED (empty)"
                results.append(False)
        except Exception as e:
            last_close = None
            status = f"❌ FAILED ({type(e).__name__})"
            results.append(False)
        rows_str = str(rows) if rows else "—"
        close_str = str(last_close) if last_close else "—"
        print(f"  {ticker:<18} {status:<25} rows={rows_str:<4} last_close={close_str}")

    working = sum(results)
    print(f"\n  .LG suffix test: {working}/5 tickers working")
    if working >= 4:
        print("  🚩 FLAG: 4+/5 working — eligible for full NGX yfinance rewrite next session")
    elif working > 0:
        print(f"  ⚠️  Partial coverage — {working}/5 working, not enough for reliable price data")
    else:
        print("  ❌ No tickers working — .LG suffix not supported by yfinance")
    return working


if __name__ == "__main__":
    result = run_ngx_screen(investos_macro="NORMAL", verbose=True)
    print(f"\nNGX: {len(result['signals'])} signals | "
          f"watch:{len(result['watch'])} | basket:{result['basket_regime']} | "
          f"regime:{result['macro_regime']}")
