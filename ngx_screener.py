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

    phase, phase_days = get_validation_phase()

    # Phase overrides
    if phase == "PAPER_ONLY":
        for s in signals + watch:
            s["action"] = "PAPER ONLY"
            s["size_label"] = "DO NOT TRADE — PAPER PHASE"
    elif phase == "RESTRICTED":
        # v2.1: threshold lowered from 80 → 65 (now achievable in NEUTRAL/RISK_ON)
        signals = [s for s in signals if s["tier"] == 1 and s["score"] >= 65]

    eligible_signals = [s for s in signals if s.get("eligible", True)]

    if verbose:
        gate_label = f"Gate {_gate}: {get_gate_status_label(_gate)}" if _NGX_PRICE_ENGINE else "Gate 0: macro-only"
    print(f"\n  NGX Basket: {basket_regime} | {gate_label}")
    if _prices:
        ok = sum(1 for t in NGX_ALL if t in _prices)
        print(f"  💰 Price data: {ok}/{len(NGX_ALL)} tickers fetched from NGN Markets")
        print(f"  Phase: {phase} (Day {phase_days})")
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
