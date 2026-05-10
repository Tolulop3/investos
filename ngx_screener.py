"""
ngx_screener.py — Nigerian Exchange (NGX) Signal Engine
=======================================================
Screens 30 NGX stocks using 5-pillar scoring system.
Completely standalone — zero impact on existing InvestOS pipeline.

Pillars:
  Trend       35% — MA structure (price vs MA20 vs MA50)
  Momentum    25% — 5D, 10D, 20D price change
  Tradability 15% — volume consistency + price continuity
  Stability   15% — drawdown + daily range volatility
  RS vs ASI   10% — relative strength vs NGX All-Share Index

Hard gates:
  tradability < 2 → no signal (illiquid)
  volume < 40% of 20D avg → suppress (volume kill-switch)

Regime layers:
  NGX basket regime: % of stocks scoring ≥60
  InvestOS macro bridge: RISK_OFF/BEAR → info only
  Oil modifier: Brent 20D vs 60D trend affects thresholds

Validation phases:
  Day 1-30: PAPER ONLY
  Day 31-60: Tier 1 + score ≥ 80
  Day 60+: full signals

Data source: Yahoo Finance (NGX tickers use .LG suffix)
"""

import json
import time
import urllib.request
import urllib.error
from datetime import datetime, timedelta
import os

# ── UNIVERSE ───────────────────────────────────────────────────────────────────
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

# ── DATA FETCH ─────────────────────────────────────────────────────────────────

def _fetch_ohlcv(ticker, days=60):
    """
    Fetch daily OHLCV for an NGX ticker.
    Uses yfinance (curl_cffi transport) — same as main screener.
    Raw urllib.request gets 403'd by Yahoo from GitHub Actions;
    yfinance bypasses this with its own session management.
    """
    try:
        import yfinance as yf
        import sys, io

        # Suppress yfinance stderr (404 errors for illiquid tickers)
        _stderr = sys.stderr
        sys.stderr = io.StringIO()
        try:
            t = yf.Ticker(ticker)
            hist = t.history(period="3mo", interval="1d", auto_adjust=True)
        finally:
            sys.stderr = _stderr

        if hist is None or len(hist) < 10:
            return None

        closes  = [float(v) for v in hist["Close"].dropna().tolist()]
        volumes = [float(v) for v in hist["Volume"].dropna().tolist()]
        highs   = [float(v) for v in hist["High"].dropna().tolist()]
        lows    = [float(v) for v in hist["Low"].dropna().tolist()]

        if len(closes) < 10:
            return None

        return {
            "ticker":  ticker,
            "closes":  closes,
            "volumes": volumes,
            "highs":   highs,
            "lows":    lows,
            "current": closes[-1],
        }
    except Exception:
        return None


def _fetch_brent_trend():
    """
    Fetch Brent crude 20D vs 60D trend using yfinance.
    Returns: "UP", "DOWN", or "FLAT"
    """
    try:
        import yfinance as yf
        import sys, io
        _stderr = sys.stderr
        sys.stderr = io.StringIO()
        try:
            hist = yf.Ticker("BZ=F").history(period="3mo", interval="1d")
        finally:
            sys.stderr = _stderr
        if hist is None or len(hist) < 20:
            return "FLAT"
        closes = hist["Close"].dropna().tolist()
        ma20 = sum(closes[-20:]) / 20
        ma60 = sum(closes[-60:]) / 60 if len(closes) >= 60 else ma20
        if ma20 > ma60 * 1.02:   return "UP"
        elif ma20 < ma60 * 0.98: return "DOWN"
        return "FLAT"
    except Exception:
        return "FLAT"


# ── SCORING ────────────────────────────────────────────────────────────────────

def score_ngx_stock(ticker, data, tier, all_closes_map):
    """
    Score a single NGX stock on 5 pillars.
    Returns (score, pillars, signals, flags, tradability_score)
    """
    closes  = data["closes"]
    volumes = data["volumes"]
    highs   = data.get("highs", closes)
    lows    = data.get("lows",  closes)
    price   = closes[-1]

    signals = []
    flags   = []
    pillars = {}

    # ── 1. TRADABILITY (15pts) — gate pillar ────────────────────────────
    # Volume consistency + price continuity + tier base + persistence
    trad = 0

    # Volume consistency: how often does daily volume > 50% of 20D avg?
    avg_vol_20d = sum(volumes[-20:]) / max(len(volumes[-20:]), 1)
    if avg_vol_20d > 0:
        consistent_days = sum(1 for v in volumes[-10:] if v >= avg_vol_20d * 0.5)
        if consistent_days >= 8:   trad += 5; signals.append("📊 Volume consistent")
        elif consistent_days >= 5: trad += 3
        else:                      trad += 0; flags.append("⚠️ Inconsistent volume")

    # Price continuity: few gaps > 5% in last 10 days
    gaps = sum(1 for i in range(max(0, len(closes)-10), len(closes)-1)
               if closes[i] > 0 and abs(closes[i+1] - closes[i]) / closes[i] > 0.05)
    if gaps == 0:   trad += 4
    elif gaps <= 1: trad += 2
    else:           trad += 0; flags.append(f"⚠️ {gaps} price gaps >5%")

    # Tier base
    trad += 4 if tier == 1 else 2

    # Volume kill-switch: current vol < 40% of 20D avg → suppress
    current_vol = volumes[-1] if volumes else 0
    if avg_vol_20d > 0 and current_vol < avg_vol_20d * 0.40:
        flags.append("🔴 Volume kill-switch: today's vol < 40% avg")
        return 0, {}, [], flags, trad  # suppress signal

    pillars["tradability"] = min(15, trad)

    # Hard gate
    if trad < 2:
        flags.append("❌ Tradability gate failed — illiquid")
        return 0, pillars, [], flags, trad

    # ── 2. TREND (35pts) ─────────────────────────────────────────────────
    trend = 0

    # MA20 and MA50
    ma20 = sum(closes[-20:]) / min(20, len(closes))
    ma50 = sum(closes[-50:]) / min(50, len(closes)) if len(closes) >= 50 else ma20

    if price > ma20 > ma50:
        trend = 35; signals.append("🔼 Price > MA20 > MA50 — strong uptrend")
    elif price > ma20:
        trend = 22; signals.append("📈 Price above MA20")
    elif price > ma50:
        trend = 12; signals.append("📊 Price above MA50 only")
    elif price < ma20 < ma50:
        trend = 0;  flags.append("🔻 Price < MA20 < MA50 — downtrend")
    else:
        trend = 6

    pillars["trend"] = trend

    # ── 3. MOMENTUM (25pts) ──────────────────────────────────────────────
    mom = 0

    perf_5d  = ((closes[-1] - closes[-6])  / closes[-6]  * 100) if len(closes) >= 6  else 0
    perf_10d = ((closes[-1] - closes[-11]) / closes[-11] * 100) if len(closes) >= 11 else 0
    perf_20d = ((closes[-1] - closes[-21]) / closes[-21] * 100) if len(closes) >= 21 else 0

    # 20D momentum (primary — 15pts)
    if perf_20d > 10:   mom += 15; signals.append(f"🚀 20D momentum: +{perf_20d:.1f}%")
    elif perf_20d > 5:  mom += 10; signals.append(f"📈 20D: +{perf_20d:.1f}%")
    elif perf_20d > 0:  mom += 5
    elif perf_20d < -10: mom += 0; flags.append(f"⚠️ 20D pullback: {perf_20d:.1f}%")
    else: mom += 2

    # 10D confirmation (6pts)
    if perf_10d > 5:   mom = min(25, mom + 6)
    elif perf_10d > 0: mom = min(25, mom + 3)
    elif perf_10d < -5: mom = max(0, mom - 3)

    # 5D (4pts)
    if perf_5d > 3:    mom = min(25, mom + 4)
    elif perf_5d > 0:  mom = min(25, mom + 2)
    elif perf_5d < -3: mom = max(0, mom - 2)

    pillars["momentum"] = mom

    # ── 4. STABILITY (15pts) ─────────────────────────────────────────────
    stab = 15

    # Drawdown from 20D high
    high_20d = max(closes[-20:]) if len(closes) >= 20 else price
    drawdown  = (price - high_20d) / high_20d * 100 if high_20d > 0 else 0

    if drawdown < -15:  stab -= 8; flags.append(f"⚠️ Deep drawdown: {drawdown:.1f}%")
    elif drawdown < -8: stab -= 4

    # Daily range volatility (intraday noise)
    if len(highs) >= 10 and len(lows) >= 10:
        ranges = [(highs[i] - lows[i]) / closes[i] * 100
                  for i in range(-10, 0) if closes[i] > 0]
        avg_range = sum(ranges) / len(ranges) if ranges else 0
        if avg_range > 8:   stab -= 5; flags.append(f"⚠️ High intraday volatility: {avg_range:.1f}%")
        elif avg_range > 5: stab -= 2

    pillars["stability"] = max(0, stab)

    # ── 5. RELATIVE STRENGTH vs ASI (10pts) ─────────────────────────────
    rs = 0
    # Compare stock's 20D perf vs median of all stocks in universe
    all_perfs = []
    for t, d in all_closes_map.items():
        c = d.get("closes", [])
        if len(c) >= 21:
            all_perfs.append((c[-1] - c[-21]) / c[-21] * 100)
    if all_perfs:
        median_perf = sorted(all_perfs)[len(all_perfs)//2]
        if perf_20d > median_perf + 5:   rs = 10; signals.append("💪 Top RS vs NGX universe")
        elif perf_20d > median_perf:      rs = 6
        elif perf_20d > median_perf - 5:  rs = 3
        else:                              rs = 0; flags.append("⚠️ Below median RS")

    pillars["rs_vs_asi"] = rs

    # ── TOTAL ─────────────────────────────────────────────────────────────
    total = sum(pillars.values())
    total = max(0, min(100, total))

    return total, pillars, signals, flags, trad


# ── REGIME + MACRO BRIDGE ──────────────────────────────────────────────────────

def compute_ngx_basket_regime(scored_stocks):
    """
    NGX basket regime from % of stocks scoring ≥60.
    BULLISH: ≥60% | NEUTRAL: ≥40% | WEAK: <40%
    """
    if not scored_stocks:
        return "NEUTRAL"
    above60 = sum(1 for s in scored_stocks if s["score"] >= 60)
    pct = above60 / len(scored_stocks)
    if pct >= 0.60:   return "BULLISH"
    elif pct >= 0.40: return "NEUTRAL"
    return "WEAK"


def apply_macro_bridge(signals, investos_macro, brent_trend, ngx_basket_regime):
    """
    Gate NGX signals through InvestOS macro regime + oil modifier.

    Returns filtered signals with action labels.
    """
    result  = []
    action_label = "SIGNAL"
    size_label   = "FULL SIZE"

    # Gate 1: RISK_OFF or BEAR → info only
    if investos_macro in ("RISK_OFF", "BEAR", "CAPITAL_PRESERVATION"):
        for s in signals:
            s["action"]     = "INFO ONLY"
            s["size_label"] = "NO TRADE — MACRO RISK_OFF"
            s["reason"]     = f"InvestOS macro: {investos_macro}"
        return signals, "INFO_ONLY"

    # Gate 2: CAUTIOUS → Tier 1 only, score ≥75, max 3
    if investos_macro in ("CAUTIOUS", "DEFENSIVE", "NEUTRAL"):
        signals = [s for s in signals if s["tier"] == 1 and s["score"] >= 75]
        signals = signals[:3]
        action_label = "CAUTIOUS SIGNAL"
        size_label   = "REDUCED SIZE"

    # Oil modifier
    if brent_trend == "DOWN":
        # Oil falling → raise threshold +5
        signals = [s for s in signals if s["score"] >= (80 if investos_macro in ("CAUTIOUS","DEFENSIVE","NEUTRAL") else 65)]
    elif brent_trend == "UP" and investos_macro in ("CAUTIOUS", "DEFENSIVE", "NEUTRAL"):
        # Oil rising → allow Tier 2 in cautious
        pass  # keep existing filtered list

    # Breadth cap based on basket regime
    breadth_cap = {"BULLISH": 6, "NEUTRAL": 4, "WEAK": 3}.get(ngx_basket_regime, 4)
    signals = signals[:breadth_cap]

    for s in signals:
        s["action"]     = action_label
        s["size_label"] = size_label

    return signals, "ACTIVE"


# ── 3-DAY PERSISTENCE GATE ────────────────────────────────────────────────────

def apply_persistence_gate(signals, score_history_path="ngx_score_history.json"):
    """
    Day 1-2 = WATCH. Day 3+ = eligible signal.
    Tracks consecutive days a stock has been in signal pool.
    """
    # Load history
    history = {}
    if os.path.exists(score_history_path):
        try:
            history = json.load(open(score_history_path))
        except Exception:
            history = {}

    today = datetime.now().strftime("%Y-%m-%d")
    updated_history = {}
    result = []

    for s in signals:
        ticker = s["ticker"]
        entry  = history.get(ticker, {"first_seen": today, "streak": 0})

        # Update streak
        last_date = entry.get("last_date", "")
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        if last_date == yesterday:
            streak = entry.get("streak", 0) + 1
        elif last_date == today:
            streak = entry.get("streak", 0)
        else:
            streak = 1  # reset

        updated_history[ticker] = {
            "first_seen": entry.get("first_seen", today),
            "last_date":  today,
            "streak":     streak,
            "last_score": s["score"],
        }

        s["streak_days"] = streak
        if streak >= 3:
            s["persistence"] = "ELIGIBLE"
        elif streak == 2:
            s["persistence"] = "WATCH (Day 2)"
        else:
            s["persistence"] = "WATCH (Day 1)"

        result.append(s)

    # Save updated history
    try:
        json.dump(updated_history, open(score_history_path, "w"), indent=2)
    except Exception:
        pass

    return result


# ── VALIDATION PHASE ──────────────────────────────────────────────────────────

def get_validation_phase(launch_date_str="2026-05-09"):
    """
    Returns validation phase based on days since launch.
    Day 1-30: PAPER ONLY | Day 31-60: Tier1 ≥80 | Day 60+: FULL
    """
    try:
        launch = datetime.strptime(launch_date_str, "%Y-%m-%d")
        days   = (datetime.now() - launch).days
        if days < 0:   return "PAPER_ONLY", 0
        if days < 30:  return "PAPER_ONLY", days
        if days < 60:  return "RESTRICTED", days
        return "FULL", days
    except Exception:
        return "PAPER_ONLY", 0


# ── MAIN ENTRY POINT ──────────────────────────────────────────────────────────

def run_ngx_screen(investos_macro="NORMAL", verbose=True):
    """
    Run full NGX screen. Returns dict ready for baking into brief.ngx.

    Args:
        investos_macro: regime string from InvestOS macro engine
        verbose: print progress
    """
    if verbose:
        print("\n" + "="*55)
        print("  NGX SIGNAL ENGINE — Nigerian Exchange")
        print("="*55)

    # 1. Fetch Brent trend
    brent_trend = _fetch_brent_trend()
    if verbose:
        print(f"\n  🛢  Brent oil trend: {brent_trend}")

    # 2. Fetch all ticker data
    all_data = {}
    failed   = []
    if verbose:
        print(f"\n  Fetching {len(NGX_ALL)} NGX tickers...")

    for ticker in NGX_ALL:
        data = _fetch_ohlcv(ticker)
        if data:
            all_data[ticker] = data
        else:
            failed.append(ticker)
        time.sleep(0.15)  # be polite to Yahoo

    if verbose:
        print(f"  ✅ {len(all_data)} fetched | ❌ {len(failed)} failed")
        if failed:
            print(f"     Failed: {failed}")

    if not all_data:
        # Try loading cached snapshot from last successful run
        try:
            cached = json.load(open("ngx_snapshot.json"))
            cached["status"]  = "CACHED"
            cached["note"]    = "Live fetch failed — showing last known NGX data"
            print("  📁 Using cached NGX snapshot (live fetch failed)")
            return cached
        except Exception:
            pass
        return {
            "status":        "NO_DATA",
            "signals":       [],
            "note":          "NGX data unavailable — yfinance fetch failed",
            "basket_regime": "UNKNOWN",
            "brent_trend":   brent_trend,
            "macro_gate":    investos_macro,
            "phase":         "PAPER_ONLY",
            "timestamp":     datetime.now().isoformat(),
        }

    # 3. Score all stocks
    scored = []
    for ticker in NGX_ALL:
        if ticker not in all_data:
            continue
        tier  = 1 if ticker in NGX_TIER1 else 2
        score, pillars, signals, flags, trad = score_ngx_stock(
            ticker, all_data[ticker], tier, all_data
        )
        if score > 0:
            scored.append({
                "ticker":      ticker,
                "tier":        tier,
                "score":       score,
                "pillars":     pillars,
                "signals":     signals,
                "flags":       flags,
                "tradability": trad,
                "price":       all_data[ticker]["current"],
                "perf_20d":    round(
                    (all_data[ticker]["closes"][-1] - all_data[ticker]["closes"][-21])
                    / all_data[ticker]["closes"][-21] * 100
                    if len(all_data[ticker]["closes"]) >= 21 else 0, 2
                ),
                "action":      "SIGNAL",
                "size_label":  "FULL SIZE",
            })

    # Sort by score descending
    scored.sort(key=lambda x: x["score"], reverse=True)

    # 4. NGX basket regime
    ngx_basket_regime = compute_ngx_basket_regime(scored)

    # 5. Macro bridge + oil modifier
    # Only pass stocks scoring ≥55 to the bridge
    eligible = [s for s in scored if s["score"] >= 55]
    eligible, gate_status = apply_macro_bridge(
        eligible, investos_macro, brent_trend, ngx_basket_regime
    )

    # 6. Persistence gate
    eligible = apply_persistence_gate(eligible)

    # 7. Validation phase
    phase, phase_days = get_validation_phase()
    if phase == "PAPER_ONLY":
        for s in eligible:
            s["action"]     = "PAPER ONLY"
            s["size_label"] = "DO NOT TRADE — PAPER PHASE"

    if verbose:
        print(f"\n  📊 NGX Basket Regime: {ngx_basket_regime}")
        print(f"  🌍 Macro gate: {investos_macro} → {gate_status}")
        print(f"  🧪 Phase: {phase} (Day {phase_days})")
        print(f"\n  🏆 TOP NGX SIGNALS ({len(eligible)} eligible):")
        for s in eligible[:6]:
            pers = s.get("persistence", "")
            print(f"     {s['ticker']:<15} Score:{s['score']:>3}  "
                  f"Tier:{s['tier']}  {s['action']}  {pers}")
        if not eligible:
            print("     No signals meet criteria today")

    # Save successful snapshot for fallback
    try:
        _snapshot = {
            "status":        gate_status,
            "phase":         phase,
        }
        json.dump({
            "status": gate_status, "phase": phase,
            "signals": [{"ticker": s["ticker"], "score": s["score"],
                         "tier": s["tier"], "reasons": s.get("reasons",[])[:3]}
                        for s in signals[:6]],
        }, open("ngx_snapshot.json", "w"))
    except Exception:
        pass

    return {
        "status":        gate_status,
        "phase":         phase,
        "phase_days":    phase_days,
        "signals":       eligible,
        "all_scored":    scored[:15],  # top 15 for display
        "basket_regime": ngx_basket_regime,
        "brent_trend":   brent_trend,
        "macro_gate":    investos_macro,
        "feeds_ok":      len(all_data),
        "feeds_failed":  len(failed),
        "timestamp":     datetime.now().isoformat(),
        "note":          (
            "Validation phase — paper trade only. Real signals from Day 31."
            if phase == "PAPER_ONLY" else
            "Tier 1 + score ≥80 only. Full signals from Day 61."
            if phase == "RESTRICTED" else
            "Full signal mode."
        ),
    }


if __name__ == "__main__":
    result = run_ngx_screen(investos_macro="NORMAL", verbose=True)
    print(f"\nNGX complete: {len(result['signals'])} signals")
