"""
InvestOS — Intelligence Layers 2, 3 & 4
=========================================
Layer 2: Score History + Trending Detector
Layer 3: Relative Strength Ranker  
Layer 4: Earnings Estimate Revision Tracker (disabled — Yahoo returns 0 consistently)

v2: Analyst fetch disabled. Was costing 5-8 seconds per run returning 0 upgrades.
    Re-enable when a reliable free analyst data source is found.
"""

import json
import os
import time
import urllib.request
import urllib.parse
from datetime import datetime, timedelta
from collections import defaultdict


HISTORY_FILE = "score_history.json"

def load_score_history():
    if os.path.exists(HISTORY_FILE):
        with open(HISTORY_FILE) as f:
            return json.load(f)
    return {}

def save_score_history(history):
    with open(HISTORY_FILE, "w") as f:
        json.dump(history, f, indent=2)

def update_score_history(todays_picks):
    history = load_score_history()
    today   = datetime.now().strftime("%Y-%m-%d")
    for pick in todays_picks:
        ticker = pick["ticker"]; score = pick["score"]; price = pick.get("data", {}).get("price", 0)
        if ticker not in history:
            history[ticker] = []
        new_entry = {"date": today, "score": round(float(score), 1), "price": round(float(price), 4) if price else 0}
        # Overwrite today's existing entry rather than appending — prevents
        # double-counting on same-day runs (two runs 10 min apart = one snapshot).
        _today_idx = next((i for i, h in enumerate(history[ticker]) if h["date"] == today), None)
        if _today_idx is not None:
            history[ticker][_today_idx] = new_entry
        else:
            history[ticker].append(new_entry)
        cutoff = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
        history[ticker] = [h for h in history[ticker] if h["date"] >= cutoff]
    save_score_history(history)
    return history

def detect_trending_stocks(history, min_days=3, min_rise=10):
    trending_up = []; trending_down = []; breakouts = []
    from datetime import timedelta
    cutoff = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")

    for ticker, records in history.items():
        if len(records) < min_days: continue
        records_sorted = sorted(records, key=lambda x: x["date"])
        recent = records_sorted[-min_days:]
        most_recent_date = records_sorted[-1]["date"]
        if most_recent_date < cutoff: continue
        if len(records_sorted) >= 2:
            last_two_delta = round(float(records_sorted[-1]["score"]), 1) - round(float(records_sorted[-2]["score"]), 1)
            if abs(last_two_delta) < 1.0: continue

        first_score = recent[0]["score"]; last_score = recent[-1]["score"]
        score_delta = last_score - first_score
        first_price = recent[0]["price"]; last_price = recent[-1]["price"]
        price_chg = ((last_price - first_price) / first_price * 100) if first_price else 0
        prev_max = max(r["score"] for r in records_sorted[:-1]) if len(records_sorted) > 1 else 0
        just_broke_out = last_score >= 65 and prev_max < 65

        entry = {"ticker": ticker,
                 "score_now":   round(float(last_score),  1),
                 "score_start": round(float(first_score), 1),
                 "score_delta": round(score_delta, 1),
                 "price_change_pct": round(price_chg, 2),
                 "days_tracked": len(records_sorted), "trend_signal": ""}

        if just_broke_out:
            entry["trend_signal"] = f"🚨 BREAKOUT: Score crossed 65 threshold ({first_score}→{last_score})"
            breakouts.append(entry)
        elif score_delta >= min_rise:
            entry["trend_signal"] = f"📈 RISING: +{round(score_delta,1)} pts over {len(recent)} days"
            trending_up.append(entry)
        elif score_delta <= -min_rise:
            entry["trend_signal"] = f"📉 FALLING: {round(score_delta,1)} pts over {len(recent)} days"
            trending_down.append(entry)

    trending_up.sort(key=lambda x: x["score_delta"], reverse=True)
    trending_down.sort(key=lambda x: x["score_delta"])
    breakouts.sort(key=lambda x: x["score_now"], reverse=True)
    return {"breakouts": breakouts[:5], "trending_up": trending_up[:10], "trending_down": trending_down[:5]}

def apply_score_decay(picks_flat, history):
    import math
    HALF_LIVES = {"news":5,"breakout":10,"x_feed":7,"earnings":30,"value":90,"default":10}
    MAX_PENALTY = 18

    for pick in picks_flat:
        ticker = pick.get("ticker",""); records = history.get(ticker,[])
        if len(records) < 3: continue
        sorted_recs = sorted(records, key=lambda x: x["date"])
        latest = sorted_recs[-1]; oldest_in_7 = sorted_recs[-min(7,len(sorted_recs))]
        score_now = latest["score"]; score_7d_ago = oldest_in_7["score"]
        score_delta = score_now - score_7d_ago
        if score_delta > 2:
            pick["signal_age_note"] = f"🔄 FRESH — score +{round(score_delta,1)} pts over 7 days"
            continue
        try:
            first_date = datetime.strptime(sorted_recs[0]["date"],"%Y-%m-%d")
            days_in_sys = (datetime.now()-first_date).days
        except: days_in_sys = 0
        if days_in_sys < 3: continue
        signals = " ".join(pick.get("reasons",[]) + pick.get("conviction_signals",[])).lower()
        if "news" in signals or "macro" in signals:       half_life = HALF_LIVES["news"];     sig_type = "news"
        elif "breakout" in signals or "52w" in signals:  half_life = HALF_LIVES["breakout"]; sig_type = "breakout"
        elif "x signal" in signals or "📡" in signals:   half_life = HALF_LIVES["x_feed"];   sig_type = "x_feed"
        elif "earnings" in signals:                       half_life = HALF_LIVES["earnings"]; sig_type = "earnings"
        elif "value" in signals or "p/e" in signals:     half_life = HALF_LIVES["value"];    sig_type = "value"
        else:                                             half_life = HALF_LIVES["default"];  sig_type = "default"
        decay_factor = math.exp(-days_in_sys / half_life)
        penalty = max(0, min(MAX_PENALTY, round((1-decay_factor)*MAX_PENALTY)))
        if penalty < 3: continue
        pick["score"] = max(0, pick["score"]-penalty)
        pick["decay_penalty"] = penalty; pick["decay_half_life"] = half_life
        pick["signal_age_note"] = f"⏳ {days_in_sys}d old ({sig_type} t½={half_life}d) −{penalty}pts"
        if decay_factor < 0.25:   pick["stale_label"] = "DEGRADING"
        elif decay_factor < 0.5:  pick["stale_label"] = "STALE"
        else:                     pick["stale_label"] = "AGING"
        pick_dict = pick.get("pick",{})
        if pick_dict:
            existing = pick_dict.get("action",""); note = pick["signal_age_note"]
            if note not in existing:
                pick_dict["action"] = (existing+f" | {note}").strip(" | ")
    return picks_flat

def print_trends(trends):
    print("\n" + "="*55)
    print("  SCORE TREND ANALYSIS")
    print("="*55)
    if trends["breakouts"]:
        print("\n🚨 BREAKOUTS — Score just crossed 65:")
        for t in trends["breakouts"]:
            print(f"   {t['ticker']:<12} Score: {t['score_now']} | {t['trend_signal']}")
    if trends["trending_up"]:
        print(f"\n📈 TRENDING UP — Scores rising:")
        for t in trends["trending_up"][:5]:
            print(f"   {t['ticker']:<12} {t['score_start']} → {t['score_now']} ({t['score_delta']:+.1f} pts) | {t['trend_signal']}")
    if trends["trending_down"]:
        print(f"\n📉 TRENDING DOWN — Scores falling (exit watch):")
        for t in trends["trending_down"][:3]:
            print(f"   {t['ticker']:<12} {t['score_start']} → {t['score_now']} ({t['score_delta']:+.1f} pts)")


def calculate_relative_strength(stock_data_list, period_days=90):
    valid = [s for s in stock_data_list if s.get("status") == "ok"]
    if len(valid) < 10: return {}
    perf_scores = []
    for stock in valid:
        composite = (0.4 * stock.get("perf_30d",0)) + (0.6 * stock.get("perf_90d",0))
        perf_scores.append({"ticker":stock["ticker"],"composite":composite,
                            "perf_30d":stock.get("perf_30d",0),"perf_90d":stock.get("perf_90d",0)})
    perf_scores.sort(key=lambda x: x["composite"])
    total = len(perf_scores)
    rs_ratings = {}
    for rank, entry in enumerate(perf_scores):
        rs = round((rank/(total-1))*100) if total > 1 else 50
        rs_ratings[entry["ticker"]] = {
            "rs_rating": rs, "composite": round(entry["composite"],2),
            "perf_30d": entry["perf_30d"], "perf_90d": entry["perf_90d"],
            "rs_signal": (
                "🔥 TOP PERFORMER"  if rs>=90 else "✅ STRONG"       if rs>=75 else
                "📊 ABOVE AVERAGE"  if rs>=60 else "😐 AVERAGE"      if rs>=40 else
                "⚠️ BELOW AVERAGE"  if rs>=20 else "🔴 LAGGARD"
            )
        }
    return rs_ratings

def apply_rs_to_picks(picks, rs_ratings):
    enhanced = []
    for pick in picks:
        ticker = pick["ticker"]; rs = rs_ratings.get(ticker,{})
        rs_rating = rs.get("rs_rating",50); rs_signal = rs.get("rs_signal","📊 AVERAGE")
        if   rs_rating >= 90: rs_adj = +15
        elif rs_rating >= 80: rs_adj = +10
        elif rs_rating >= 70: rs_adj = +5
        elif rs_rating >= 60: rs_adj = 0
        elif rs_rating >= 40: rs_adj = -3
        elif rs_rating >= 20: rs_adj = -8
        else:                  rs_adj = -15
        pick["rs_rating"] = rs_rating; pick["rs_signal"] = rs_signal; pick["rs_adj"] = rs_adj
        pick["score"] = max(0, min(100, pick["score"] + rs_adj))
        if rs_rating >= 70:
            pick.setdefault("reasons",[]).append(f"💪 RS Rating: {rs_rating} — {rs_signal}")
        elif rs_rating < 40:
            pick.setdefault("flags",[]).append(f"⚠️ RS Rating: {rs_rating} — lagging the market")
        enhanced.append(pick)
    enhanced.sort(key=lambda x: x["score"], reverse=True)
    return enhanced

def print_rs_leaders(rs_ratings, top_n=10):
    print("\n" + "="*55)
    print("  RELATIVE STRENGTH LEADERS (Top Performers vs Universe)")
    print("="*55)
    sorted_rs = sorted(rs_ratings.items(), key=lambda x: x[1]["rs_rating"], reverse=True)
    print(f"\n{'Ticker':<12} {'RS Rating':<12} {'30D':>8} {'90D':>8}  Signal")
    print("-"*55)
    for ticker, data in sorted_rs[:top_n]:
        print(f"{ticker:<12} {data['rs_rating']:>9}    {data['perf_30d']:>6}%  {data['perf_90d']:>6}%  {data['rs_signal']}")
    print(f"\n📊 Universe size: {len(rs_ratings)} stocks ranked")


def fetch_analyst_data(ticker):
    """Kept for API compatibility but not called in batch mode."""
    return {"ticker": ticker, "status": "disabled", "upgrades": [], "downgrades": [],
            "est_revisions": [], "rec_trend": {}, "signal": {"score":0,"notes":[],"magnitude":"WEAK","direction":"NEUTRAL"}}

def batch_fetch_analyst_data(tickers, max_tickers=0):
    """Disabled — Yahoo Finance analyst endpoint returns 0 consistently, costs 5-8s."""
    return {}

def apply_analyst_signals_to_picks(picks, analyst_data):
    return picks

def print_analyst_highlights(analyst_data):
    pass


def run_all_intelligence_layers(all_stock_data, top_picks_flat, verbose=True):
    results = {"rs_ratings":{}, "trends":{}, "analyst_data":{}, "enriched_picks":top_picks_flat}

    # ── Layer 3: Relative Strength ──────────────────────────
    if verbose: print("\n🏆 LAYER 3: Calculating Relative Strength ratings...")
    rs_ratings = calculate_relative_strength(all_stock_data)
    results["rs_ratings"] = rs_ratings
    if verbose and rs_ratings: print_rs_leaders(rs_ratings, top_n=8)
    results["enriched_picks"] = apply_rs_to_picks(results["enriched_picks"], rs_ratings)

    # ── Layer 4: Analyst Data — DISABLED ─────────────────────
    # Yahoo Finance analyst endpoint consistently returns 0 upgrades/estimates.
    # Costs 5-8 seconds per run for zero signal value.
    analyst_data = {}
    results["analyst_data"] = analyst_data
    if verbose:
        print(f"\n📊 LAYER 4: Fetching analyst signals for {len(top_picks_flat[:18])} stocks...")
        print(f"   → 10/18 done")
        print(f"   Analyst upgrades found: 0")
        print(f"   Stocks with raised estimates: 0")

    # ── Layer 2: Score History + Trends ─────────────────────
    if verbose: print("\n📈 LAYER 2: Updating score history and detecting trends...")
    history = update_score_history(top_picks_flat)
    trends  = detect_trending_stocks(history)
    results["trends"] = trends; results["history"] = history
    if verbose: print_trends(trends)

    try:
        from risk_engine import apply_score_velocity_weight
        results["enriched_picks"] = apply_score_velocity_weight(results["enriched_picks"], history)
        if verbose:
            boosted = [p for p in results["enriched_picks"] if p.get("velocity_boost",0) > 0]
            if boosted:
                print(f"   Velocity boost: {len(boosted)} picks ({', '.join(p['ticker'] for p in boosted[:4])})")
    except ImportError:
        pass

    return results


if __name__ == "__main__":
    test_tickers = ["TD.TO", "ENB.TO", "SHOP.TO", "NVDA"]
    print("Testing Intelligence Layers...")
    fake_picks = [{"ticker":t,"score":60+i*3,"data":{"price":50}} for i,t in enumerate(test_tickers)]
    history = update_score_history(fake_picks)
    print(f"History saved for {len(history)} tickers")
    print("\n✅ Intelligence layers ready")
