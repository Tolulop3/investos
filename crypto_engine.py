"""
InvestOS — Crypto Signal Engine
=================================
Daily directional analysis for BTC and SOL.

Signal sources:
  - Price action (momentum, 200d MA, RSI, breakout zones)
  - Macro drivers from news_analyzer (risk sentiment, Fed, inflation)
  - BTC dominance proxy (SOL-specific)
  - Volume confirmation

Output per coin:
  - Direction: LONG / SHORT / NEUTRAL
  - Verdict:   BULL BUY | BEAR SELL | HOLD | WAIT
  - Entry, target, stop, R/R
  - Conviction 0-100%
  - Hold period: 7-30 days (not intraday)

RISK WARNING:
  BTC moves 3-5x more than stocks. SOL moves 5-10x more.
  Max position: 2-3% of portfolio per coin.
  Always use the stop loss provided.
"""

import json
import time
import urllib.request
import urllib.parse
from datetime import datetime

CRYPTO_ASSETS = {
    "BTC-USD": {
        "name":           "Bitcoin",
        "display":        "BTC/USD",
        "stop_atr_mult":  2.0,
        "target_atr_mult":3.5,
        "notes":          "Digital gold. Leads entire crypto market. Watch Fed + risk sentiment.",
    },
    "SOL-USD": {
        "name":           "Solana",
        "display":        "SOL/USD",
        "stop_atr_mult":  2.5,
        "target_atr_mult":4.0,
        "notes":          "High beta to BTC. Underperforms when BTC dominance rises.",
    },
}

CRYPTO_MACRO_MAP = {
    "fed_rate_cut":                ("LONG",  0.75),
    "trump_deregulation_positive": ("LONG",  0.60),
    "earnings_beats":              ("LONG",  0.45),
    "inflation_hot":               ("LONG",  0.40),
    "peace_deal":                  ("LONG",  0.35),
    "boc_rate_cut":                ("LONG",  0.25),
    "fed_rate_hike":               ("SHORT", 0.80),
    "war_escalation":              ("SHORT", 0.65),
    "trump_tariff_negative":       ("SHORT", 0.55),
    "trump_tariff_canada_specific":("SHORT", 0.35),
    "middle_east_tension":         ("SHORT", 0.45),
    "earnings_misses":             ("SHORT", 0.40),
    "boc_rate_hike":               ("SHORT", 0.25),
}


def fetch_crypto_data(symbol):
    try:
        url = (f"https://query1.finance.yahoo.com/v8/finance/chart/"
               f"{urllib.parse.quote(symbol)}?interval=1d&range=1y")
        req = urllib.request.Request(
            url, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        )
        with urllib.request.urlopen(req, timeout=14) as r:
            data = json.loads(r.read().decode())

        result = data['chart']['result'][0]
        meta   = result['meta']
        quotes = result['indicators']['quote'][0]
        closes  = [c for c in quotes.get('close',  []) if c]
        highs   = [h for h in quotes.get('high',   []) if h]
        lows    = [l for l in quotes.get('low',    []) if l]
        volumes = [v for v in quotes.get('volume', []) if v]

        if len(closes) < 30:
            return None

        price   = meta.get('regularMarketPrice', closes[-1])
        prev    = closes[-2] if len(closes) >= 2 else closes[-1]
        day_chg = (price - prev) / prev * 100

        ma20  = sum(closes[-20:]) / 20
        ma50  = sum(closes[-50:]) / 50   if len(closes) >= 50  else sum(closes)/len(closes)
        ma200 = sum(closes[-200:]) / 200 if len(closes) >= 200 else sum(closes)/len(closes)

        perf_7d  = (closes[-1]-closes[-8])  /closes[-8]  *100 if len(closes)>=8  else 0
        perf_30d = (closes[-1]-closes[-31]) /closes[-31] *100 if len(closes)>=31 else 0
        perf_90d = (closes[-1]-closes[-91]) /closes[-91] *100 if len(closes)>=91 else 0

        atr = sum(highs[i]-lows[i] for i in range(-14,0))/14 if len(highs)>=14 else price*0.04

        if len(closes) >= 15:
            gains  = [max(0, closes[i]-closes[i-1]) for i in range(-14,0)]
            losses = [max(0, closes[i-1]-closes[i]) for i in range(-14,0)]
            ag, al = sum(gains)/14, sum(losses)/14
            rsi = 100-100/(1+ag/al) if al > 0 else 50
        else:
            rsi = 50

        vol_ratio = 1.0
        if len(volumes) >= 20:
            avg_v = sum(volumes[-20:])/20
            vol_ratio = sum(volumes[-5:])/5/avg_v if avg_v > 0 else 1.0

        recent_high = max(highs[-30:]) if len(highs)>=30 else price*1.1
        recent_low  = min(lows[-30:])  if len(lows) >=30 else price*0.9
        w52_high    = max(highs) if highs else price
        w52_low     = min(lows)  if lows  else price

        if   price>ma50>ma200: trend = "UPTREND"
        elif price<ma50<ma200: trend = "DOWNTREND"
        elif price>ma200:      trend = "ABOVE_200D"
        else:                  trend = "BELOW_200D"

        time.sleep(0.2)
        return {"symbol":symbol,"price":round(price,2),"day_chg_pct":round(day_chg,2),
                "ma20":round(ma20,2),"ma50":round(ma50,2),"ma200":round(ma200,2),
                "perf_7d":round(perf_7d,2),"perf_30d":round(perf_30d,2),"perf_90d":round(perf_90d,2),
                "atr":round(atr,2),"rsi":round(rsi,1),"vol_ratio":round(vol_ratio,2),
                "recent_high":round(recent_high,2),"recent_low":round(recent_low,2),
                "w52_high":round(w52_high,2),"w52_low":round(w52_low,2),
                "trend":trend,"status":"ok"}
    except Exception as e:
        return {"symbol":symbol,"status":f"error: {str(e)[:50]}"}


def generate_crypto_technical(price_data, config):
    p = price_data
    price = p["price"]
    atr   = p["atr"]
    score = 0
    reasons = []

    # 200d MA — primary regime
    if price > p["ma200"]:
        score += 30; reasons.append(f"Above 200d MA (${p['ma200']:,.0f})")
    else:
        score -= 30; reasons.append(f"Below 200d MA — bearish structure")

    if p["ma50"] > p["ma200"]: score += 15
    elif p["ma50"] < p["ma200"]: score -= 15

    # Momentum
    p30 = p["perf_30d"]
    if   p30 > 15:  score += 20; reasons.append(f"Strong 30d: +{p30}%")
    elif p30 > 5:   score += 10
    elif p30 < -15: score -= 20; reasons.append(f"Weak 30d: {p30}%")
    elif p30 < -5:  score -= 10

    # RSI
    rsi = p["rsi"]
    if   45 <= rsi <= 70: score += 10; reasons.append(f"RSI healthy: {rsi:.0f}")
    elif rsi < 30:        score += 8;  reasons.append(f"RSI oversold: {rsi:.0f}")
    elif rsi > 75:        score -= 12; reasons.append(f"RSI overbought: {rsi:.0f}")

    # Volume
    vr = p["vol_ratio"]
    if   vr > 1.3: score += 8;  reasons.append(f"Volume: {vr:.1f}x avg")
    elif vr < 0.7: score -= 5

    # Near 52w high
    dist = (price - p["w52_high"]) / p["w52_high"] * 100
    if dist > -10: score += 12; reasons.append("Near 52w high")

    direction = "LONG" if score >= 30 else "SHORT" if score <= -30 else "NEUTRAL"
    sm, tm = config["stop_atr_mult"], config["target_atr_mult"]

    if direction == "LONG":
        entry = price; stop = round(price - atr*sm, 2); target = round(price + atr*tm, 2)
    elif direction == "SHORT":
        entry = price; stop = round(price + atr*sm, 2); target = round(price - atr*tm, 2)
    else:
        entry = stop = target = price

    rr = round(abs(target-entry)/abs(stop-entry), 2) if stop != entry else 0

    return {"direction":direction,"conviction":min(85,max(0,abs(score))),
            "tech_score":score,"entry":round(entry,2),"target":target,"stop":stop,
            "rr_ratio":rr,"reasons":reasons[:3],
            "hold_days":"14-30 days" if abs(score)>=40 else "7-14 days"}


def aggregate_macro_for_crypto(news_analysis):
    if not news_analysis:
        return {"BTC-USD":{"direction":"NEUTRAL","conviction":0,"signals":[]},
                "SOL-USD":{"direction":"NEUTRAL","conviction":0,"signals":[]}}

    active = news_analysis.get("active_signals", {})
    scores = {"BTC-USD":{"long":0,"short":0,"signals":[]},
              "SOL-USD":{"long":0,"short":0,"signals":[]}}

    for sig, data in active.items():
        if sig not in CRYPTO_MACRO_MAP: continue
        conf = data.get("confidence", 0)
        if conf < 25: continue
        direction, weight = CRYPTO_MACRO_MAP[sig]
        for symbol in ["BTC-USD","SOL-USD"]:
            mult = 1.2 if symbol == "SOL-USD" else 1.0
            adj  = conf * weight * mult
            scores[symbol]["long" if direction=="LONG" else "short"] += adj
            scores[symbol]["signals"].append({
                "name": sig.replace("_"," ").title()[:30],
                "direction": direction, "confidence": conf
            })

    result = {}
    for symbol, s in scores.items():
        net   = s["long"] - s["short"]
        total = s["long"] + s["short"]
        result[symbol] = {
            "direction":  "LONG" if net>25 else "SHORT" if net<-25 else "NEUTRAL",
            "conviction": min(85, round(abs(net)/max(1,total)*100)),
            "net_score":  round(net,1),
            "signals":    s["signals"][:4],
        }
    return result


def get_btc_dominance_signal(btc_data, sol_data):
    if not btc_data or not sol_data:
        return {"signal":"NEUTRAL","note":"","sol_adj":0}
    diff = btc_data.get("perf_30d",0) - sol_data.get("perf_30d",0)
    if diff > 15:
        return {"signal":"BTC_DOMINANT","sol_adj":-15,
                "note":f"BTC outperforming SOL by {diff:.1f}% — dominance rising, SOL lags"}
    elif diff < -15:
        return {"signal":"ALTS_SEASON","sol_adj":+10,
                "note":f"SOL outperforming BTC by {abs(diff):.1f}% — altcoin strength"}
    return {"signal":"NEUTRAL","note":f"BTC/SOL in sync ({diff:+.1f}% spread)","sol_adj":0}


def combine_crypto_signals(tech, macro):
    td = tech.get("direction","NEUTRAL")
    md = macro.get("direction","NEUTRAL") if macro else "NEUTRAL"
    tc = tech.get("conviction",0)
    mc = macro.get("conviction",0) if macro else 0

    if td == md and td != "NEUTRAL":
        conv = min(92, round(tc*0.45 + mc*0.55)); direction = td; alignment = "ALIGNED ✅"
    elif td != "NEUTRAL" and md == "NEUTRAL":
        conv = round(tc*0.65); direction = td; alignment = "TECH ONLY"
    elif md != "NEUTRAL" and td == "NEUTRAL":
        conv = round(mc*0.65); direction = md; alignment = "MACRO ONLY"
    elif td != "NEUTRAL" and md != "NEUTRAL" and td != md:
        conv = 15; direction = "NEUTRAL"; alignment = "CONFLICTED ⚠️"
    else:
        conv = 0; direction = "NEUTRAL"; alignment = "NO SIGNAL"

    quality = "🔥 STRONG" if conv>=70 else "✅ MODERATE" if conv>=50 else "📊 WEAK" if conv>=30 else "😐 NEUTRAL"

    if   direction=="LONG"  and conv>=55: verdict = "🟢 BULL — BUY/HOLD"
    elif direction=="SHORT" and conv>=55: verdict = "🔴 BEAR — SELL/SHORT"
    elif direction=="LONG":               verdict = "🟡 MILD BULL — HOLD"
    elif direction=="SHORT":              verdict = "🟡 MILD BEAR — REDUCE"
    else:                                 verdict = "⚪ NEUTRAL — WAIT"

    return {"direction":direction,"conviction":conv,"quality":quality,
            "alignment":alignment,"verdict":verdict,"tech_dir":td,"macro_dir":md}


def run_crypto_engine(news_analysis=None, portfolio_value=10000, verbose=True):
    now = datetime.now()
    if verbose:
        print(f"\n{'='*55}")
        print(f"  CRYPTO SIGNAL ENGINE")
        print(f"{'='*55}")
        print(f"  ⚠️  HIGH RISK — max 2-3% of portfolio per coin\n")

    if verbose: print("  → BTC...", end=" ", flush=True)
    btc_data = fetch_crypto_data("BTC-USD")
    if verbose: print("✅" if btc_data and btc_data.get("status")=="ok" else "❌")

    if verbose: print("  → SOL...", end=" ", flush=True)
    sol_data = fetch_crypto_data("SOL-USD")
    if verbose: print("✅" if sol_data and sol_data.get("status")=="ok" else "❌")

    macro_signals = aggregate_macro_for_crypto(news_analysis)
    dom_signal    = get_btc_dominance_signal(btc_data, sol_data)

    if verbose and dom_signal["signal"] != "NEUTRAL":
        print(f"\n  📊 Dominance: {dom_signal['note']}")

    results = {}
    price_map = {"BTC-USD": btc_data, "SOL-USD": sol_data}

    for symbol, config in CRYPTO_ASSETS.items():
        pd = price_map[symbol]
        if not pd or pd.get("status") != "ok":
            results[symbol] = {"name":config["display"],"direction":"NEUTRAL",
                               "conviction":0,"verdict":"⚪ DATA UNAVAILABLE","status":"unavailable"}
            continue

        tech  = generate_crypto_technical(pd, config)
        macro = dict(macro_signals.get(symbol, {}))

        # SOL dominance adjustment
        if symbol == "SOL-USD" and dom_signal["sol_adj"] != 0:
            adj = dom_signal["sol_adj"]
            macro["conviction"] = max(0, min(90, macro.get("conviction",0)+adj))
            if adj < 0:
                macro.setdefault("signals",[]).append(
                    {"name":"BTC Dominance Rising","direction":"SHORT","confidence":60})

        combined  = combine_crypto_signals(tech, macro)
        direction = combined["direction"]
        conviction= combined["conviction"]
        atr       = pd["atr"]

        # Final levels
        if direction == "LONG":
            entry  = pd["price"]
            stop   = round(entry - atr * config["stop_atr_mult"], 2)
            target = round(entry + atr * config["target_atr_mult"], 2)
        elif direction == "SHORT":
            entry  = pd["price"]
            stop   = round(entry + atr * config["stop_atr_mult"], 2)
            target = round(entry - atr * config["target_atr_mult"], 2)
        else:
            entry = stop = target = pd["price"]

        rr = round(abs(target-entry)/abs(stop-entry), 2) if stop != entry else 0

        # Position size
        if direction != "NEUTRAL" and conviction >= 40:
            size_pct  = 0.03 if conviction>=70 else 0.025 if conviction>=55 else 0.015
            size_usd  = round(portfolio_value * size_pct, 2)
            size_note = f"Max {round(size_pct*100,1)}% — HIGH RISK"
        else:
            size_pct = 0; size_usd = 0; size_note = "No position — wait for clearer signal"

        if   direction=="LONG"  and conviction>=55: action = "BUY / ADD"
        elif direction=="LONG":                      action = "HOLD"
        elif direction=="SHORT" and conviction>=55:  action = "SELL / REDUCE"
        elif direction=="SHORT":                     action = "REDUCE EXPOSURE"
        else:                                        action = "WAIT FOR SETUP"

        all_reasons = tech.get("reasons",[]) + [s["name"] for s in macro.get("signals",[])]
        key_driver  = all_reasons[0] if all_reasons else config["notes"]

        results[symbol] = {
            "name":       config["display"],
            "full_name":  config["name"],
            "symbol":     symbol,
            "price":      pd["price"],
            "day_chg_pct":pd["day_chg_pct"],
            "direction":  direction,
            "verdict":    combined["verdict"],
            "action":     action,
            "conviction": conviction,
            "quality":    combined["quality"],
            "alignment":  combined["alignment"],
            "entry":      entry,
            "target":     target,
            "stop":       stop,
            "rr_ratio":   rr,
            "hold_period":tech["hold_days"],
            "key_driver": key_driver,
            "sizing":     {"dollars":size_usd,"pct":round(size_pct*100,1),"note":size_note},
            "risk_warning":"⚠️ HIGH RISK — use hard stops, max 3% position size",
            "tech":       {"direction":tech["direction"],"conviction":tech["conviction"],
                          "rsi":pd["rsi"],"trend":pd["trend"],"perf_7d":pd["perf_7d"],
                          "perf_30d":pd["perf_30d"],"perf_90d":pd["perf_90d"],
                          "ma200":pd["ma200"],"vol_ratio":pd["vol_ratio"],
                          "reasons":tech["reasons"]},
            "macro":      {"direction":macro.get("direction","NEUTRAL"),
                          "conviction":macro.get("conviction",0),
                          "signals":macro.get("signals",[])},
            "dominance":  dom_signal if symbol=="SOL-USD" else {},
            "notes":      config["notes"],
            "status":     "ok",
        }

        if verbose:
            icon = "🟢" if direction=="LONG" else "🔴" if direction=="SHORT" else "⚪"
            print(f"\n  {icon} {config['display']}: ${pd['price']:,.2f} ({pd['day_chg_pct']:+.1f}%)")
            print(f"     {combined['verdict']} | {conviction}% conviction | {combined['alignment']}")
            if direction != "NEUTRAL":
                print(f"     Entry:${entry:,.2f}  Target:${target:,.2f}  Stop:${stop:,.2f}  R/R:{rr}")
                print(f"     Size: ${size_usd:,} ({round(size_pct*100,1)}% of portfolio)")

    if verbose: print(f"\n{'='*55}")

    return {
        "generated_at":     now.isoformat(),
        "date":             now.strftime("%B %d, %Y"),
        "assets":           results,
        "dominance_signal": dom_signal,
        "risk_note":        "HIGH RISK — Never size crypto like a stock position.",
    }


if __name__ == "__main__":
    result = run_crypto_engine(verbose=True)
    with open("crypto_signals.json","w") as f:
        json.dump(result, f, indent=2, default=str)
    print(f"\n💾 Saved to crypto_signals.json")
