"""
InvestOS — FX & Gold Signal Engine
====================================
Daily directional calls for:
  EUR/USD | USD/JPY | GBP/USD | USD/CAD | XAU/USD (Gold)

Signal sources:
  - Price action (momentum, breakouts, support/resistance)
  - Macro drivers (BoC/Fed rates, inflation, oil, risk sentiment)
  - News sentiment from news_analyzer.py
  - Multi-timeframe confirmation (daily + weekly trend)

Output per pair:
  - Direction (LONG / SHORT / NEUTRAL)
  - Entry zone
  - Target
  - Stop loss
  - Conviction score (0-100)
  - Hold period
  - Key driver

Broker API placeholder: ready to wire to OANDA, Interactive Brokers,
or any REST API when you're ready to automate.

PURELY MANUAL execution for now.
"""

import json
import time
import urllib.request
import urllib.parse
from datetime import datetime, timedelta
from collections import defaultdict

# ============================================================
# FX PAIR CONFIG
# ============================================================

FX_PAIRS = {
    "EURUSD=X": {
        "name":        "EUR/USD",
        "base":        "EUR",
        "quote":       "USD",
        "pip_value":   0.0001,
        "typical_spread_pips": 0.8,
        "daily_range_pips":    80,
        "drivers":     ["ECB policy", "Fed policy", "EU economic data", "risk sentiment"],
        "notes":       "Most liquid pair. Very signal-friendly. Tight spreads.",
    },
    "JPY=X": {
        "name":        "USD/JPY",
        "base":        "USD",
        "quote":       "JPY",
        "pip_value":   0.01,
        "typical_spread_pips": 1.0,
        "daily_range_pips":    100,
        "drivers":     ["BoJ policy", "Fed policy", "risk-off flows", "carry trade"],
        "notes":       "Strong macro correlation. Risk-off = JPY strengthens.",
    },
    "GBPUSD=X": {
        "name":        "GBP/USD",
        "base":        "GBP",
        "quote":       "USD",
        "pip_value":   0.0001,
        "typical_spread_pips": 1.2,
        "daily_range_pips":    120,
        "drivers":     ["BoE policy", "UK economic data", "Brexit effects", "USD strength"],
        "notes":       "High volatility. Strong news-driven moves.",
    },
    "CAD=X": {
        "name":        "USD/CAD",
        "base":        "USD",
        "quote":       "CAD",
        "pip_value":   0.0001,
        "typical_spread_pips": 1.5,
        "daily_range_pips":    90,
        "drivers":     ["Oil price", "BoC policy", "Fed policy", "Canadian economic data", "tariffs"],
        "notes":       "Oil-correlated. Most relevant to Canadian investor.",
    },
    "GC=F": {
        "name":        "XAU/USD (Gold)",
        "base":        "XAU",
        "quote":       "USD",
        "pip_value":   0.10,
        "typical_spread_pips": 3.0,
        "daily_range_pips":    200,
        "drivers":     ["Risk sentiment", "real yields", "USD strength", "geopolitics", "inflation"],
        "notes":       "Safe haven. Strong in risk-off, tariff, and war scenarios.",
    },
}

# ============================================================
# MACRO SIGNAL WEIGHTS FOR FX
# Maps news signals to pair direction
# ============================================================

FX_SIGNAL_MAP = {
    # Trump/tariffs
    "trump_tariff_canada_specific": {
        "EURUSD=X": ("SHORT", 0.4),   # USD strengthens on US assertiveness
        "JPY=X":    ("SHORT", 0.5),   # Risk-off = JPY strengthens = USD/JPY falls
        "GBPUSD=X": ("SHORT", 0.3),
        "CAD=X":    ("LONG",  0.8),   # USD/CAD rises = CAD weakens under tariffs
        "GC=F":     ("LONG",  0.7),   # Gold = safe haven
    },
    "trump_tariff_negative": {
        "CAD=X":    ("LONG",  0.6),
        "GC=F":     ("LONG",  0.5),
        "JPY=X":    ("SHORT", 0.4),
    },
    "trump_deregulation_positive": {
        "EURUSD=X": ("SHORT", 0.3),   # USD strengthens
        "CAD=X":    ("SHORT", 0.2),
    },

    # War/geopolitical
    "war_escalation": {
        "GC=F":     ("LONG",  0.9),   # Gold surges on war
        "JPY=X":    ("SHORT", 0.7),   # JPY safe haven
        "EURUSD=X": ("SHORT", 0.5),
        "GBPUSD=X": ("SHORT", 0.4),
        "CAD=X":    ("LONG",  0.3),   # Mixed — USD up but oil also up
    },
    "middle_east_tension": {
        "GC=F":     ("LONG",  0.8),
        "CAD=X":    ("SHORT", 0.4),   # Oil up = CAD up = USD/CAD falls
        "JPY=X":    ("SHORT", 0.5),
    },
    "peace_deal": {
        "GC=F":     ("SHORT", 0.6),   # Gold falls on risk-on
        "JPY=X":    ("LONG",  0.4),   # JPY weakens
        "EURUSD=X": ("LONG",  0.3),
    },

    # Central banks
    "boc_rate_cut": {
        "CAD=X":    ("LONG",  0.8),   # CAD weakens = USD/CAD rises
    },
    "boc_rate_hike": {
        "CAD=X":    ("SHORT", 0.8),   # CAD strengthens = USD/CAD falls
    },
    "fed_rate_cut": {
        "EURUSD=X": ("LONG",  0.8),   # EUR/USD rises as USD weakens
        "GBPUSD=X": ("LONG",  0.7),
        "GC=F":     ("LONG",  0.7),   # Gold rises on weaker USD
        "JPY=X":    ("SHORT", 0.5),   # USD/JPY falls
        "CAD=X":    ("SHORT", 0.4),
    },
    "fed_rate_hike": {
        "EURUSD=X": ("SHORT", 0.8),
        "GBPUSD=X": ("SHORT", 0.7),
        "GC=F":     ("SHORT", 0.6),
        "JPY=X":    ("LONG",  0.5),
        "CAD=X":    ("LONG",  0.3),
    },

    # Inflation
    "inflation_hot": {
        "GC=F":     ("LONG",  0.7),   # Gold = inflation hedge
        "EURUSD=X": ("SHORT", 0.3),   # If US inflation = Fed hike expectations
    },
    "inflation_cooling": {
        "GC=F":     ("SHORT", 0.4),
        "EURUSD=X": ("LONG",  0.4),
    },

    # Oil
    "oil_price_spike": {
        "CAD=X":    ("SHORT", 0.7),   # Oil up = CAD up = USD/CAD falls
        "JPY=X":    ("LONG",  0.3),   # Japan imports oil = JPY weakens
    },
    "oil_price_drop": {
        "CAD=X":    ("LONG",  0.6),
        "JPY=X":    ("SHORT", 0.2),
    },

    # Risk sentiment
    "earnings_beats": {
        "JPY=X":    ("LONG",  0.3),   # Risk-on = JPY weakens = USD/JPY rises
        "GC=F":     ("SHORT", 0.3),
    },
    "earnings_misses": {
        "JPY=X":    ("SHORT", 0.3),
        "GC=F":     ("LONG",  0.4),
    },
}

# ============================================================
# PRICE DATA FETCHER
# ============================================================

def fetch_fx_data(symbol, verbose=False):
    """Fetch FX/Gold price data from Yahoo Finance"""
    try:
        url = (f"https://query1.finance.yahoo.com/v8/finance/chart/"
               f"{urllib.parse.quote(symbol)}?interval=1d&range=6mo")
        req = urllib.request.Request(
            url, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        )
        with urllib.request.urlopen(req, timeout=12) as r:
            data = json.loads(r.read().decode())

        result = data['chart']['result'][0]
        meta   = result['meta']
        quotes = result['indicators']['quote'][0]

        closes = [c for c in quotes.get('close', [])  if c]
        highs  = [h for h in quotes.get('high', [])   if h]
        lows   = [l for l in quotes.get('low', [])    if l]
        vols   = [v for v in quotes.get('volume', []) if v]

        if len(closes) < 20:
            return None

        price      = meta.get('regularMarketPrice', closes[-1])
        prev_close = closes[-2] if len(closes) >= 2 else closes[-1]
        day_chg    = (price - prev_close) / prev_close * 100

        # Moving averages
        ma20  = sum(closes[-20:]) / 20
        ma50  = sum(closes[-50:]) / 50  if len(closes) >= 50 else sum(closes) / len(closes)
        ma200 = sum(closes[-200:]) / 200 if len(closes) >= 200 else sum(closes) / len(closes)

        # Momentum
        perf_5d  = (closes[-1] - closes[-6])  / closes[-6]  * 100 if len(closes) >= 6  else 0
        perf_20d = (closes[-1] - closes[-21]) / closes[-21] * 100 if len(closes) >= 21 else 0
        perf_60d = (closes[-1] - closes[-61]) / closes[-61] * 100 if len(closes) >= 61 else 0

        # Support and resistance (recent highs/lows)
        recent_high = max(highs[-20:]) if highs else price * 1.02
        recent_low  = min(lows[-20:])  if lows  else price * 0.98
        w52_high    = max(highs) if highs else price
        w52_low     = min(lows)  if lows  else price

        # Volatility (ATR approximation)
        if len(highs) >= 14 and len(lows) >= 14:
            ranges = [highs[i] - lows[i] for i in range(-14, 0)]
            atr    = sum(ranges) / 14
        else:
            atr = price * 0.008

        # RSI (14-period)
        if len(closes) >= 15:
            gains  = [max(0, closes[i] - closes[i-1]) for i in range(-14, 0)]
            losses = [max(0, closes[i-1] - closes[i]) for i in range(-14, 0)]
            avg_g  = sum(gains) / 14
            avg_l  = sum(losses) / 14
            rsi    = 100 - (100 / (1 + avg_g / avg_l)) if avg_l > 0 else 50
        else:
            rsi = 50

        # Trend determination
        if price > ma50 > ma200:
            trend = "UPTREND"
        elif price < ma50 < ma200:
            trend = "DOWNTREND"
        elif price > ma50:
            trend = "ABOVE_MA50"
        else:
            trend = "BELOW_MA50"

        time.sleep(0.15)

        return {
            "symbol":       symbol,
            "price":        round(price, 5 if 'USD' in symbol and 'GC' not in symbol else 2),
            "day_chg_pct":  round(day_chg, 3),
            "ma20":         round(ma20, 5),
            "ma50":         round(ma50, 5),
            "ma200":        round(ma200, 5),
            "perf_5d":      round(perf_5d, 3),
            "perf_20d":     round(perf_20d, 3),
            "perf_60d":     round(perf_60d, 3),
            "recent_high":  round(recent_high, 5),
            "recent_low":   round(recent_low, 5),
            "w52_high":     round(w52_high, 5),
            "w52_low":      round(w52_low, 5),
            "atr":          round(atr, 5),
            "rsi":          round(rsi, 1),
            "trend":        trend,
            "status":       "ok"
        }

    except Exception as e:
        return {"symbol": symbol, "status": f"error: {str(e)[:40]}"}


# ============================================================
# TECHNICAL SIGNAL GENERATOR
# ============================================================

def generate_technical_signal(price_data, pair_config):
    """
    Generate directional signal from price action alone.
    Uses: trend, momentum, RSI, breakout detection.
    """
    if not price_data or price_data.get("status") != "ok":
        return {"direction": "NEUTRAL", "conviction": 0, "reason": "No data"}

    p     = price_data
    price = p["price"]
    rsi   = p["rsi"]
    atr   = p["atr"]

    score     = 0
    reasons   = []
    direction = "NEUTRAL"

    # Trend alignment
    if p["trend"] == "UPTREND":
        score += 30; reasons.append("Price above MA50 > MA200 (uptrend)")
    elif p["trend"] == "DOWNTREND":
        score -= 30; reasons.append("Price below MA50 < MA200 (downtrend)")
    elif p["trend"] == "ABOVE_MA50":
        score += 15
    else:
        score -= 15

    # Momentum
    if p["perf_20d"] > 1.5:
        score += 20; reasons.append(f"Strong 20d momentum: +{p['perf_20d']}%")
    elif p["perf_20d"] > 0.5:
        score += 10
    elif p["perf_20d"] < -1.5:
        score -= 20; reasons.append(f"Weak 20d: {p['perf_20d']}%")
    elif p["perf_20d"] < -0.5:
        score -= 10

    # RSI
    if 40 <= rsi <= 65:
        score += 10; reasons.append(f"RSI healthy: {rsi}")
    elif rsi < 35:
        score += 5; reasons.append(f"RSI oversold: {rsi} — potential bounce")
    elif rsi > 70:
        score -= 10; reasons.append(f"RSI overbought: {rsi} — caution")

    # Near 52w high (breakout potential)
    dist_from_high = (price - p["w52_high"]) / p["w52_high"] * 100
    if dist_from_high > -2:
        score += 15; reasons.append("Near 52w high — breakout zone")
    elif dist_from_high < -15:
        score -= 5

    # Recent range position
    range_pos = (price - p["recent_low"]) / (p["recent_high"] - p["recent_low"]) if p["recent_high"] != p["recent_low"] else 0.5
    if range_pos > 0.7:
        score += 8
    elif range_pos < 0.3:
        score -= 8

    # Determine direction
    if score >= 25:
        direction = "LONG"
    elif score <= -25:
        direction = "SHORT"
    else:
        direction = "NEUTRAL"

    # Entry / target / stop
    if direction == "LONG":
        entry  = round(price, 4)
        target = round(price + atr * 2.5, 4)
        stop   = round(price - atr * 1.5, 4)
    elif direction == "SHORT":
        entry  = round(price, 4)
        target = round(price - atr * 2.5, 4)
        stop   = round(price + atr * 1.5, 4)
    else:
        entry  = round(price, 4)
        target = round(price, 4)
        stop   = round(price, 4)

    # Risk/reward
    if direction != "NEUTRAL" and entry != stop:
        rr = abs(target - entry) / abs(stop - entry)
    else:
        rr = 0

    conviction = min(85, max(0, abs(score)))

    return {
        "direction":  direction,
        "conviction": conviction,
        "tech_score": score,
        "entry":      entry,
        "target":     target,
        "stop":       stop,
        "rr_ratio":   round(rr, 2),
        "reasons":    reasons[:3],
        "hold_period": "3-7 days" if conviction >= 50 else "1-3 days",
    }


# ============================================================
# MACRO SIGNAL AGGREGATOR
# ============================================================

def aggregate_macro_signals_for_fx(news_analysis):
    """
    Convert news_analysis active_signals into FX directional scores.
    Each signal votes for/against each pair.
    """
    if not news_analysis:
        return {}

    active_signals = news_analysis.get("active_signals", {})
    pair_scores    = defaultdict(lambda: {"long": 0, "short": 0, "signals": []})

    for signal_name, signal_data in active_signals.items():
        if signal_name not in FX_SIGNAL_MAP:
            continue

        conf = signal_data.get("confidence", 0)
        if conf < 25:
            continue

        for symbol, (direction, weight) in FX_SIGNAL_MAP[signal_name].items():
            adjusted = conf * weight
            if direction == "LONG":
                pair_scores[symbol]["long"] += adjusted
            else:
                pair_scores[symbol]["short"] += adjusted

            pair_scores[symbol]["signals"].append({
                "name":      signal_name.replace("_", " ").title(),
                "direction": direction,
                "confidence": conf,
                "weight":    weight
            })

    # Convert to net direction per pair
    macro_signals = {}
    for symbol, scores in pair_scores.items():
        net   = scores["long"] - scores["short"]
        total = scores["long"] + scores["short"]

        macro_signals[symbol] = {
            "net_score":  round(net, 1),
            "long_score": round(scores["long"], 1),
            "short_score":round(scores["short"], 1),
            "direction":  "LONG" if net > 20 else "SHORT" if net < -20 else "NEUTRAL",
            "conviction": min(90, round(abs(net) / max(1, total) * 100, 0)),
            "signals":    scores["signals"][:4]
        }

    return macro_signals


# ============================================================
# SIGNAL COMBINER — TECHNICAL + MACRO
# ============================================================

def combine_signals(tech_signal, macro_signal, pair_name):
    """
    Combine technical and macro signals.
    Both must agree for high-conviction call.
    If they disagree → NEUTRAL.
    """
    tech_dir  = tech_signal.get("direction", "NEUTRAL")
    macro_dir = macro_signal.get("direction", "NEUTRAL") if macro_signal else "NEUTRAL"
    tech_conv = tech_signal.get("conviction", 0)
    macro_conv= macro_signal.get("conviction", 0) if macro_signal else 0

    # Both agree = high conviction
    if tech_dir == macro_dir and tech_dir != "NEUTRAL":
        combined_conviction = min(95, round((tech_conv * 0.45) + (macro_conv * 0.55)))
        final_direction     = tech_dir
        alignment           = "ALIGNED ✅"

    # Only technical
    elif tech_dir != "NEUTRAL" and macro_dir == "NEUTRAL":
        combined_conviction = round(tech_conv * 0.7)
        final_direction     = tech_dir
        alignment           = "TECH ONLY"

    # Only macro
    elif macro_dir != "NEUTRAL" and tech_dir == "NEUTRAL":
        combined_conviction = round(macro_conv * 0.7)
        final_direction     = macro_dir
        alignment           = "MACRO ONLY"

    # Conflicting
    elif tech_dir != "NEUTRAL" and macro_dir != "NEUTRAL" and tech_dir != macro_dir:
        combined_conviction = 15
        final_direction     = "NEUTRAL"
        alignment           = "CONFLICTED ⚠️"

    else:
        combined_conviction = 0
        final_direction     = "NEUTRAL"
        alignment           = "NO SIGNAL"

    # Signal quality label
    if combined_conviction >= 70:
        quality = "🔥 STRONG"
    elif combined_conviction >= 50:
        quality = "✅ MODERATE"
    elif combined_conviction >= 30:
        quality = "📊 WEAK"
    else:
        quality = "😐 NEUTRAL"

    return {
        "direction":   final_direction,
        "conviction":  combined_conviction,
        "quality":     quality,
        "alignment":   alignment,
        "tech_dir":    tech_dir,
        "macro_dir":   macro_dir,
        "tech_conv":   tech_conv,
        "macro_conv":  macro_conv,
    }


# ============================================================
# MAIN FX ENGINE
# ============================================================

def run_fx_engine(news_analysis=None, verbose=True):
    """
    Full FX/Gold signal generation:
    1. Fetch price data for all pairs
    2. Generate technical signals
    3. Aggregate macro signals from news
    4. Combine into final calls
    5. Return structured signal output
    """
    now = datetime.now()
    if verbose:
        print(f"\n{'='*55}")
        print(f"  FX & GOLD SIGNAL ENGINE")
        print(f"  {now.strftime('%B %d, %Y %I:%M %p')}")
        print(f"{'='*55}")

    # Macro signals from news
    macro_signals = aggregate_macro_signals_for_fx(news_analysis)
    if verbose and macro_signals:
        print(f"\n📰 Macro signals affecting FX: {len(macro_signals)} pairs")

    results = {}

    for symbol, config in FX_PAIRS.items():
        if verbose:
            print(f"\n  → {config['name']}...", end=" ", flush=True)

        # 1. Price data
        price_data = fetch_fx_data(symbol)
        if not price_data or price_data.get("status") != "ok":
            if verbose: print("❌ data unavailable")
            results[symbol] = {
                "pair": config["name"], "direction": "NEUTRAL",
                "conviction": 0, "quality": "😐 NEUTRAL",
                "status": "unavailable"
            }
            continue

        # 2. Technical signal
        tech = generate_technical_signal(price_data, config)

        # 3. Macro signal
        macro = macro_signals.get(symbol, {})

        # 4. Combine
        combined = combine_signals(tech, macro, config["name"])

        # 5. Build final call
        direction  = combined["direction"]
        conviction = combined["conviction"]

        # Risk parameters (1.5R:1 minimum, ideally 2.5R:1)
        entry  = tech["entry"]
        target = tech["target"]
        stop   = tech["stop"]
        atr    = price_data.get("atr", entry * 0.008)

        # Recalculate with macro conviction weighting
        if direction == "LONG":
            stop   = round(entry - atr * 1.5, 5)
            target = round(entry + atr * 2.5, 5)
        elif direction == "SHORT":
            stop   = round(entry + atr * 1.5, 5)
            target = round(entry - atr * 2.5, 5)

        rr = round(abs(target - entry) / abs(stop - entry), 2) if stop != entry else 0

        # Key driver (most confident signal)
        all_reasons = tech.get("reasons", []) + [s["name"] for s in macro.get("signals", [])]
        key_driver  = all_reasons[0] if all_reasons else config["drivers"][0]

        result = {
            "pair":        config["name"],
            "symbol":      symbol,
            "price":       price_data["price"],
            "day_chg_pct": price_data["day_chg_pct"],
            "direction":   direction,
            "conviction":  conviction,
            "quality":     combined["quality"],
            "alignment":   combined["alignment"],
            "entry":       entry,
            "target":      target,
            "stop":        stop,
            "rr_ratio":    rr,
            "key_driver":  key_driver,
            "hold_period": tech["hold_period"],
            "tech": {
                "direction":   tech["direction"],
                "conviction":  tech["conviction"],
                "rsi":         price_data["rsi"],
                "trend":       price_data["trend"],
                "perf_20d":    price_data["perf_20d"],
                "reasons":     tech["reasons"],
            },
            "macro": {
                "direction":   macro.get("direction", "NEUTRAL"),
                "conviction":  macro.get("conviction", 0),
                "signals":     macro.get("signals", []),
            },
            "price_context": {
                "ma20":        price_data["ma20"],
                "ma50":        price_data["ma50"],
                "ma200":       price_data["ma200"],
                "w52_high":    price_data["w52_high"],
                "w52_low":     price_data["w52_low"],
                "atr":         atr,
            },
            "notes":       config["notes"],
            "status":      "ok",

            # Broker API placeholder
            # Uncomment and configure when ready to automate:
            # "broker_order": {
            #     "action":     direction,
            #     "symbol":     symbol,
            #     "units":      1000,          # Position size
            #     "entry":      entry,
            #     "take_profit":target,
            #     "stop_loss":  stop,
            #     "type":       "MARKET",      # or "LIMIT"
            #     "broker":     "OANDA",       # or "IB", "SAXO"
            #     "api_key":    "YOUR_KEY_HERE"
            # }
        }

        results[symbol] = result

        if verbose:
            icon = "🟢" if direction == "LONG" else "🔴" if direction == "SHORT" else "⚪"
            print(f"{icon} {direction} ({conviction}%) | {combined['quality']}")

    # Summary
    active_calls = [r for r in results.values() if r.get("direction") != "NEUTRAL" and r.get("conviction", 0) >= 40]

    if verbose:
        print(f"\n{'='*55}")
        print(f"  FX SUMMARY — {len(active_calls)} active calls")
        print(f"{'='*55}")
        for r in sorted(active_calls, key=lambda x: x["conviction"], reverse=True):
            icon = "🟢" if r["direction"] == "LONG" else "🔴"
            print(f"\n  {icon} {r['pair']:<15} {r['direction']:<7} Conv: {r['conviction']}%")
            print(f"     Entry: {r['entry']} | Target: {r['target']} | Stop: {r['stop']}")
            print(f"     R/R: {r['rr_ratio']} | Hold: {r['hold_period']}")
            print(f"     Driver: {r['key_driver']}")

    return {
        "generated_at":  now.isoformat(),
        "date":          now.strftime("%B %d, %Y"),
        "pairs":         results,
        "active_calls":  active_calls,
        "total_signals": len(active_calls),
        "top_call":      active_calls[0] if active_calls else None,
    }


if __name__ == "__main__":
    print("FX Engine standalone test...")
    result = run_fx_engine(news_analysis=None, verbose=True)
    with open("fx_signals.json", "w") as f:
        json.dump(result, f, indent=2, default=str)
    print(f"\n💾 Saved to fx_signals.json")
