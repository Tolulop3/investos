"""
InvestOS — Intelligence Layers 2, 3 & 4
=========================================
Layer 2: Score History + Trending Detector
  - Saves daily scores, detects rising/falling stocks
  - A stock rising from 45 → 65 in 5 days is a stronger
    signal than one that's been at 70 for months

Layer 3: Relative Strength Ranker
  - Compares every stock's performance AGAINST the universe
  - RS 90 = outperforming 90% of the market (very bullish)
  - Industry standard — used by O'Neil's CANSLIM system

Layer 4: Earnings Estimate Revision Tracker
  - Analyst upgrades/estimate raises = most reliable signal in finance
  - Tracks who recently got upgraded vs downgraded
  - Free via Yahoo Finance analyst data
"""

import json
import os
import time
import urllib.request
import urllib.parse
from datetime import datetime, timedelta
from collections import defaultdict


# ============================================================
# LAYER 2 — SCORE HISTORY & TREND DETECTOR
# ============================================================

HISTORY_FILE = "score_history.json"

def load_score_history():
    """Load historical scores from disk"""
    if os.path.exists(HISTORY_FILE):
        with open(HISTORY_FILE) as f:
            return json.load(f)
    return {}


def save_score_history(history):
    """Persist scores to disk"""
    with open(HISTORY_FILE, "w") as f:
        json.dump(history, f, indent=2)


def update_score_history(todays_picks):
    """
    Save today's scores for all screened stocks.
    todays_picks: flat list of all scored stocks from screener
    """
    history = load_score_history()
    today   = datetime.now().strftime("%Y-%m-%d")

    for pick in todays_picks:
        ticker = pick["ticker"]
        score  = pick["score"]
        price  = pick.get("data", {}).get("price", 0)

        if ticker not in history:
            history[ticker] = []

        # Append today's entry
        history[ticker].append({
            "date":  today,
            "score": score,
            "price": price
        })

        # Keep only 90 days of history
        cutoff = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
        history[ticker] = [h for h in history[ticker] if h["date"] >= cutoff]

    save_score_history(history)
    return history


def detect_trending_stocks(history, min_days=3, min_rise=10):
    """
    Find stocks where scores are RISING over recent days.
    These are higher-conviction signals than static scores.

    min_days: minimum days of history required
    min_rise: minimum score improvement to flag as trending
    """
    trending_up   = []
    trending_down = []
    breakouts     = []

    today = datetime.now().strftime("%Y-%m-%d")

    for ticker, records in history.items():
        if len(records) < min_days:
            continue

        # Sort by date
        records_sorted = sorted(records, key=lambda x: x["date"])
        recent = records_sorted[-min_days:]

        first_score = recent[0]["score"]
        last_score  = recent[-1]["score"]
        score_delta = last_score - first_score

        # Price trend
        first_price = recent[0]["price"]
        last_price  = recent[-1]["price"]
        price_chg   = ((last_price - first_price) / first_price * 100) if first_price else 0

        # Breakout: score crosses above 65 from below
        prev_max = max(r["score"] for r in records_sorted[:-1]) if len(records_sorted) > 1 else 0
        just_broke_out = last_score >= 65 and prev_max < 65

        entry = {
            "ticker":         ticker,
            "score_now":      last_score,
            "score_start":    first_score,
            "score_delta":    round(score_delta, 1),
            "price_change_pct": round(price_chg, 2),
            "days_tracked":   len(records_sorted),
            "trend_signal":   ""
        }

        if just_broke_out:
            entry["trend_signal"] = f"🚨 BREAKOUT: Score crossed 65 threshold ({first_score}→{last_score})"
            breakouts.append(entry)
        elif score_delta >= min_rise:
            entry["trend_signal"] = f"📈 RISING: +{score_delta} pts over {len(recent)} days"
            trending_up.append(entry)
        elif score_delta <= -min_rise:
            entry["trend_signal"] = f"📉 FALLING: {score_delta} pts over {len(recent)} days"
            trending_down.append(entry)

    # Sort
    trending_up.sort(  key=lambda x: x["score_delta"], reverse=True)
    trending_down.sort(key=lambda x: x["score_delta"])
    breakouts.sort(    key=lambda x: x["score_now"], reverse=True)

    return {
        "breakouts":     breakouts[:5],
        "trending_up":   trending_up[:10],
        "trending_down": trending_down[:5]
    }


def apply_score_decay(picks_flat, history):
    """
    Exponential half-life score decay — signals fade realistically by type.

    Half-lives by signal type (days to decay to 50% of original boost):
      News catalyst    →  5 days   (fades fast — headlines move on)
      Breakout signal  → 10 days   (price action can sustain a week+)
      X feed mention   →  7 days   (social momentum fades quickly)
      Earnings quality → 30 days   (fundamental data is quarterly)
      Value signal     → 90 days   (valuation changes slowly)
      Default          → 10 days

    Formula: decay_factor = e^(-days / half_life)
    penalty = round((1 - decay_factor) * max_penalty)

    Fresh signals (score_delta > 2 in last 7 days) are exempt — still live.
    New picks (< 3 days history) are exempt — too early to assess.
    """
    import math

    HALF_LIVES = {
        "news":     5,
        "breakout": 10,
        "x_feed":   7,
        "earnings": 30,
        "value":    90,
        "default":  10,
    }
    MAX_PENALTY = 18  # max score reduction for fully decayed signal

    for pick in picks_flat:
        ticker  = pick.get("ticker", "")
        records = history.get(ticker, [])

        if len(records) < 3:
            continue  # too new to assess

        sorted_recs  = sorted(records, key=lambda x: x["date"])
        latest       = sorted_recs[-1]
        oldest_in_7  = sorted_recs[-min(7, len(sorted_recs))]

        score_now    = latest["score"]
        score_7d_ago = oldest_in_7["score"]
        score_delta  = score_now - score_7d_ago

        # Fresh signal — actively rising, no decay
        if score_delta > 2:
            pick["signal_age_note"] = f"🔄 FRESH — score +{round(score_delta,1)} pts over 7 days"
            continue

        # Days since first appearance
        try:
            first_date  = datetime.strptime(sorted_recs[0]["date"], "%Y-%m-%d")
            days_in_sys = (datetime.now() - first_date).days
        except Exception:
            days_in_sys = 0

        if days_in_sys < 3:
            continue

        # Detect dominant signal type from pick's reasons/signals
        signals = " ".join(pick.get("reasons", []) + pick.get("conviction_signals", [])).lower()
        if "news" in signals or "macro" in signals or "catalyst" in signals:
            half_life = HALF_LIVES["news"]
            sig_type  = "news"
        elif "breakout" in signals or "52w" in signals or "52-week" in signals:
            half_life = HALF_LIVES["breakout"]
            sig_type  = "breakout"
        elif "x signal" in signals or "📡" in signals:
            half_life = HALF_LIVES["x_feed"]
            sig_type  = "x_feed"
        elif "earnings" in signals or "eq:" in signals or "💹" in signals:
            half_life = HALF_LIVES["earnings"]
            sig_type  = "earnings"
        elif "value" in signals or "p/e" in signals or "graham" in signals:
            half_life = HALF_LIVES["value"]
            sig_type  = "value"
        else:
            half_life = HALF_LIVES["default"]
            sig_type  = "default"

        # Exponential decay: penalty grows as signal ages
        decay_factor = math.exp(-days_in_sys / half_life)
        penalty      = round((1 - decay_factor) * MAX_PENALTY)
        penalty      = max(0, min(MAX_PENALTY, penalty))

        if penalty < 3:
            continue  # negligible decay, skip

        pick["score"]           = max(0, pick["score"] - penalty)
        pick["decay_penalty"]   = penalty
        pick["decay_half_life"] = half_life
        pick["signal_age_note"] = f"⏳ {days_in_sys}d old ({sig_type} t½={half_life}d) −{penalty}pts"

        # Stale label for display
        if decay_factor < 0.25:
            pick["stale_label"] = "DEGRADING"
        elif decay_factor < 0.5:
            pick["stale_label"] = "STALE"
        else:
            pick["stale_label"] = "AGING"

        # Surface in pick action text
        pick_dict = pick.get("pick", {})
        if pick_dict:
            existing = pick_dict.get("action", "")
            note     = pick["signal_age_note"]
            if note not in existing:
                pick_dict["action"] = (existing + f" | {note}").strip(" | ")

    return picks_flat


def print_trends(trends):
    """Print trend report"""
    print("\n" + "="*55)
    print("  SCORE TREND ANALYSIS")
    print("="*55)

    if trends["breakouts"]:
        print("\n🚨 BREAKOUTS — Score just crossed 65 (high conviction):")
        for t in trends["breakouts"]:
            print(f"   {t['ticker']:<12} Score: {t['score_now']} | Price: {'+' if t['price_change_pct']>=0 else ''}{t['price_change_pct']}%")
            print(f"              {t['trend_signal']}")

    if trends["trending_up"]:
        print(f"\n📈 TRENDING UP — Scores rising over recent days:")
        for t in trends["trending_up"][:5]:
            print(f"   {t['ticker']:<12} {t['score_start']} → {t['score_now']} ({'+' if t['score_delta']>=0 else ''}{t['score_delta']} pts) | {t['trend_signal']}")

    if trends["trending_down"]:
        print(f"\n📉 TRENDING DOWN — Scores falling (exit watch):")
        for t in trends["trending_down"][:3]:
            print(f"   {t['ticker']:<12} {t['score_start']} → {t['score_now']} ({t['score_delta']} pts)")


# ============================================================
# LAYER 3 — RELATIVE STRENGTH RANKER
# ============================================================

def calculate_relative_strength(stock_data_list, period_days=90):
    """
    Industry standard RS calculation.
    Compares each stock's performance against ALL others in the universe.

    RS 90 = outperformed 90% of stocks = very bullish
    RS 50 = average
    RS < 30 = laggard — avoid

    Uses weighted formula (recent performance weighted more):
    RS = (0.4 * perf_30d) + (0.6 * perf_90d)
    This matches IBD's RS rating methodology.
    """

    # Filter to stocks with valid data
    valid = [s for s in stock_data_list if s.get("status") == "ok"]

    if len(valid) < 10:
        return {}

    # Calculate composite performance score for each stock
    perf_scores = []
    for stock in valid:
        # Weighted: recent matters more
        composite = (0.4 * stock.get("perf_30d", 0)) + (0.6 * stock.get("perf_90d", 0))
        perf_scores.append({
            "ticker":    stock["ticker"],
            "composite": composite,
            "perf_30d":  stock.get("perf_30d", 0),
            "perf_90d":  stock.get("perf_90d", 0),
        })

    # Sort by composite performance
    perf_scores.sort(key=lambda x: x["composite"])
    total = len(perf_scores)

    # Assign RS rating (percentile rank)
    rs_ratings = {}
    for rank, entry in enumerate(perf_scores):
        rs = round((rank / (total - 1)) * 100) if total > 1 else 50
        rs_ratings[entry["ticker"]] = {
            "rs_rating":    rs,
            "composite":    round(entry["composite"], 2),
            "perf_30d":     entry["perf_30d"],
            "perf_90d":     entry["perf_90d"],
            "rs_signal":    (
                "🔥 TOP PERFORMER"   if rs >= 90 else
                "✅ STRONG"          if rs >= 75 else
                "📊 ABOVE AVERAGE"   if rs >= 60 else
                "😐 AVERAGE"         if rs >= 40 else
                "⚠️ BELOW AVERAGE"   if rs >= 20 else
                "🔴 LAGGARD"
            )
        }

    return rs_ratings


def apply_rs_to_picks(picks, rs_ratings):
    """
    Add RS rating to each pick and adjust score.
    High RS = bonus points. Low RS = penalty.
    This is the industry standard filter:
      Only buy stocks with RS >= 70.
    """
    enhanced = []
    for pick in picks:
        ticker = pick["ticker"]
        rs = rs_ratings.get(ticker, {})

        rs_rating = rs.get("rs_rating", 50)
        rs_signal = rs.get("rs_signal", "📊 AVERAGE")

        # Score adjustment based on RS
        if rs_rating >= 90:   rs_adj = +15
        elif rs_rating >= 80: rs_adj = +10
        elif rs_rating >= 70: rs_adj = +5
        elif rs_rating >= 60: rs_adj = 0
        elif rs_rating >= 40: rs_adj = -3
        elif rs_rating >= 20: rs_adj = -8
        else:                 rs_adj = -15

        pick["rs_rating"]  = rs_rating
        pick["rs_signal"]  = rs_signal
        pick["rs_adj"]     = rs_adj
        pick["score"]      = max(0, min(100, pick["score"] + rs_adj))

        if rs_rating >= 70:
            pick.setdefault("reasons", []).append(f"💪 RS Rating: {rs_rating} — {rs_signal}")
        elif rs_rating < 40:
            pick.setdefault("flags", []).append(f"⚠️ RS Rating: {rs_rating} — lagging the market")

        enhanced.append(pick)

    # Re-sort by updated score
    enhanced.sort(key=lambda x: x["score"], reverse=True)
    return enhanced


def print_rs_leaders(rs_ratings, top_n=10):
    """Print top RS performers"""
    print("\n" + "="*55)
    print("  RELATIVE STRENGTH LEADERS (Top Performers vs Universe)")
    print("="*55)

    sorted_rs = sorted(rs_ratings.items(), key=lambda x: x[1]["rs_rating"], reverse=True)
    print(f"\n{'Ticker':<12} {'RS Rating':<12} {'30D':>8} {'90D':>8}  Signal")
    print("-"*55)
    for ticker, data in sorted_rs[:top_n]:
        print(f"{ticker:<12} {data['rs_rating']:>9}    {data['perf_30d']:>6}%  {data['perf_90d']:>6}%  {data['rs_signal']}")

    print(f"\n📊 Universe size: {len(rs_ratings)} stocks ranked")


# ============================================================
# LAYER 4 — EARNINGS ESTIMATE REVISION TRACKER
# ============================================================

def fetch_analyst_data(ticker):
    """
    Fetch analyst recommendations and earnings estimates from Yahoo Finance.
    Analyst upgrades = most reliable buy signal in professional finance.
    """
    try:
        url = (
            f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/"
            f"{urllib.parse.quote(ticker)}"
            f"?modules=upgradeDowngradeHistory,earningsTrend,recommendationTrend"
        )
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())

        result = data.get("quoteSummary", {}).get("result", [{}])[0]

        # ── Upgrade/Downgrade History ──
        upgrades   = []
        downgrades = []
        udh = result.get("upgradeDowngradeHistory", {}).get("history", [])
        cutoff_ts  = (datetime.now() - timedelta(days=90)).timestamp()

        for action in udh:
            ts      = action.get("epochGradeDate", 0)
            grade   = action.get("toGrade", "")
            firm    = action.get("firm", "")
            action_ = action.get("action", "")

            if ts < cutoff_ts:
                continue  # Only last 90 days

            date_str = datetime.fromtimestamp(ts).strftime("%b %d")
            entry    = {"date": date_str, "firm": firm, "grade": grade}

            if action_ in ("up", "init") and grade.lower() in (
                "buy", "strong buy", "outperform", "overweight", "accumulate"
            ):
                upgrades.append(entry)
            elif action_ == "down" and grade.lower() in (
                "sell", "strong sell", "underperform", "underweight", "reduce"
            ):
                downgrades.append(entry)

        # ── Earnings Trend (estimate revisions) ──
        trend_data = result.get("earningsTrend", {}).get("trend", [])
        est_revisions = []

        for period in trend_data[:2]:  # Current quarter + next
            period_label = period.get("period", "")
            eps_current  = period.get("earningsEstimate", {}).get("avg",    {}).get("raw", None)
            eps_7d_ago   = period.get("earningsEstimate", {}).get("7daysAgo", {}).get("raw", None)
            eps_30d_ago  = period.get("earningsEstimate", {}).get("30daysAgo", {}).get("raw", None)
            eps_90d_ago  = period.get("earningsEstimate", {}).get("90daysAgo", {}).get("raw", None)

            if eps_current is not None and eps_30d_ago is not None and eps_30d_ago != 0:
                revision_30d = ((eps_current - eps_30d_ago) / abs(eps_30d_ago)) * 100
            else:
                revision_30d = None

            if eps_current is not None:
                est_revisions.append({
                    "period":      period_label,
                    "est_current": round(eps_current, 3) if eps_current else None,
                    "est_30d_ago": round(eps_30d_ago, 3) if eps_30d_ago else None,
                    "revision_30d_pct": round(revision_30d, 1) if revision_30d is not None else None,
                    "direction":   (
                        "RAISED"    if revision_30d and revision_30d > 2  else
                        "LOWERED"   if revision_30d and revision_30d < -2 else
                        "UNCHANGED"
                    )
                })

        # ── Recommendation Trend ──
        rec_trend = result.get("recommendationTrend", {}).get("trend", [])
        current_rec = {}
        if rec_trend:
            r = rec_trend[0]
            current_rec = {
                "strong_buy":  r.get("strongBuy", 0),
                "buy":         r.get("buy", 0),
                "hold":        r.get("hold", 0),
                "sell":        r.get("sell", 0),
                "strong_sell": r.get("strongSell", 0),
            }
            total_analysts = sum(current_rec.values())
            bull_analysts  = current_rec["strong_buy"] + current_rec["buy"]
            if total_analysts > 0:
                current_rec["bull_pct"] = round(bull_analysts / total_analysts * 100, 0)
                current_rec["consensus"] = (
                    "STRONG BUY" if current_rec["bull_pct"] >= 70 else
                    "BUY"        if current_rec["bull_pct"] >= 55 else
                    "HOLD"       if current_rec["bull_pct"] >= 35 else
                    "SELL"
                )

        time.sleep(0.2)

        return {
            "ticker":        ticker,
            "upgrades":      upgrades,
            "downgrades":    downgrades,
            "est_revisions": est_revisions,
            "rec_trend":     current_rec,
            "signal": _score_analyst_signal(upgrades, downgrades, est_revisions, current_rec),
            "status": "ok"
        }

    except Exception as e:
        return {"ticker": ticker, "status": "error", "error": str(e)[:50]}


def _score_analyst_signal(upgrades, downgrades, est_revisions, rec_trend):
    """Calculate overall analyst signal strength"""
    score = 0
    notes = []

    # Upgrades in last 90 days
    if len(upgrades) >= 3:
        score += 20; notes.append(f"🔥 {len(upgrades)} analyst upgrades in 90 days")
    elif len(upgrades) >= 1:
        score += 10; notes.append(f"✅ {len(upgrades)} analyst upgrade(s) recently")

    # Downgrades (penalty)
    if len(downgrades) >= 2:
        score -= 15; notes.append(f"⚠️ {len(downgrades)} analyst downgrades")
    elif len(downgrades) == 1:
        score -= 5

    # Earnings estimate revisions
    for rev in est_revisions:
        if rev.get("direction") == "RAISED":
            pct = rev.get("revision_30d_pct", 0) or 0
            if pct > 5:
                score += 15; notes.append(f"🔥 EPS estimates raised +{pct}% ({rev['period']})")
            else:
                score += 8;  notes.append(f"✅ EPS estimates raised ({rev['period']})")
        elif rev.get("direction") == "LOWERED":
            score -= 10; notes.append(f"⚠️ EPS estimates cut ({rev['period']})")

    # Consensus
    bull_pct = rec_trend.get("bull_pct", 50)
    if bull_pct >= 70:   score += 10; notes.append(f"✅ {bull_pct}% analysts bullish")
    elif bull_pct >= 55: score += 5
    elif bull_pct < 35:  score -= 8; notes.append(f"⚠️ Only {bull_pct}% analysts bullish")

    return {
        "score":     max(-30, min(30, score)),
        "notes":     notes,
        "magnitude": "STRONG" if abs(score) >= 20 else "MODERATE" if abs(score) >= 10 else "WEAK",
        "direction": "BULLISH" if score > 5 else "BEARISH" if score < -5 else "NEUTRAL"
    }


def batch_fetch_analyst_data(tickers, max_tickers=40):
    """Fetch analyst data for top picks — cap to avoid rate limiting"""
    print(f"\n📊 Fetching analyst data for {min(len(tickers), max_tickers)} tickers...")
    results = {}
    tickers_to_fetch = tickers[:max_tickers]

    for i, ticker in enumerate(tickers_to_fetch):
        result = fetch_analyst_data(ticker)
        if result.get("status") == "ok":
            results[ticker] = result
        if (i + 1) % 10 == 0:
            print(f"   → {i+1}/{len(tickers_to_fetch)} done")
        time.sleep(0.3)  # Rate limit respect

    # Summary
    upgrades_count = sum(len(r.get("upgrades", [])) for r in results.values())
    revised_up     = sum(1 for r in results.values()
                         if any(e.get("direction") == "RAISED"
                                for e in r.get("est_revisions", [])))

    print(f"   Analyst upgrades found: {upgrades_count}")
    print(f"   Stocks with raised estimates: {revised_up}")

    return results


def apply_analyst_signals_to_picks(picks, analyst_data):
    """
    Apply analyst signal scores to picks.
    This is the highest-conviction adjustment — raised estimates
    have the best track record of any signal in professional finance.
    """
    enhanced = []
    for pick in picks:
        ticker = pick["ticker"]
        analyst = analyst_data.get(ticker)

        if not analyst or analyst.get("status") != "ok":
            enhanced.append(pick)
            continue

        signal     = analyst.get("signal", {})
        adj        = signal.get("score", 0)
        direction  = signal.get("direction", "NEUTRAL")
        notes      = signal.get("notes", [])
        magnitude  = signal.get("magnitude", "WEAK")

        pick["analyst_signal"]    = signal
        pick["analyst_upgrades"]  = len(analyst.get("upgrades", []))
        pick["analyst_downgrades"]= len(analyst.get("downgrades", []))
        pick["est_revisions"]     = analyst.get("est_revisions", [])
        pick["rec_consensus"]     = analyst.get("rec_trend", {}).get("consensus", "N/A")

        # Apply score adjustment
        if adj != 0:
            pick["score"] = max(0, min(100, pick["score"] + adj))
            for note in notes[:2]:
                if direction == "BULLISH":
                    pick.setdefault("reasons", []).append(note)
                else:
                    pick.setdefault("flags", []).append(note)

        # Flag strong analyst signals
        if magnitude == "STRONG" and direction == "BULLISH":
            pick.setdefault("reasons", []).insert(0,
                f"🔥 STRONG analyst signal — {', '.join(notes[:1])}"
            )

        enhanced.append(pick)

    enhanced.sort(key=lambda x: x["score"], reverse=True)
    return enhanced


def print_analyst_highlights(analyst_data):
    """Print analyst signal highlights"""
    print("\n" + "="*55)
    print("  ANALYST SIGNAL HIGHLIGHTS")
    print("="*55)

    strong_buys  = [(t, d) for t, d in analyst_data.items()
                    if d.get("signal", {}).get("direction") == "BULLISH"
                    and d.get("signal", {}).get("magnitude") == "STRONG"]
    est_raised   = [(t, d) for t, d in analyst_data.items()
                    if any(e.get("direction") == "RAISED"
                           for e in d.get("est_revisions", []))]
    est_lowered  = [(t, d) for t, d in analyst_data.items()
                    if any(e.get("direction") == "LOWERED"
                           for e in d.get("est_revisions", []))]

    if strong_buys:
        print(f"\n🔥 STRONG BUY SIGNALS ({len(strong_buys)} stocks):")
        for ticker, data in strong_buys[:5]:
            notes = data.get("signal", {}).get("notes", [])
            print(f"   {ticker:<12} {notes[0] if notes else ''}")

    if est_raised:
        print(f"\n✅ ESTIMATES RAISED — Most reliable signal ({len(est_raised)} stocks):")
        for ticker, data in est_raised[:5]:
            revs = [e for e in data.get("est_revisions", []) if e.get("direction") == "RAISED"]
            if revs:
                pct = revs[0].get("revision_30d_pct", "?")
                print(f"   {ticker:<12} EPS estimates up {'+' if pct and pct>0 else ''}{pct}% in 30 days")

    if est_lowered:
        print(f"\n⚠️ ESTIMATES LOWERED — Avoid or exit ({len(est_lowered)} stocks):")
        for ticker, data in est_lowered[:3]:
            print(f"   {ticker:<12} Analysts cutting earnings estimates — caution")


# ============================================================
# COMBINED INTELLIGENCE RUN
# ============================================================

def run_all_intelligence_layers(all_stock_data, top_picks_flat, verbose=True):
    """
    Run all three intelligence layers and return enriched picks.

    all_stock_data: list of raw stock data dicts from screener
    top_picks_flat: list of top pick dicts to enrich
    """
    results = {
        "rs_ratings":   {},
        "trends":       {},
        "analyst_data": {},
        "enriched_picks": top_picks_flat
    }

    # ── Layer 3: Relative Strength ──────────────────────────
    if verbose: print("\n🏆 LAYER 3: Calculating Relative Strength ratings...")
    rs_ratings = calculate_relative_strength(all_stock_data)
    results["rs_ratings"] = rs_ratings

    if verbose and rs_ratings:
        print_rs_leaders(rs_ratings, top_n=8)

    # Apply RS to picks
    results["enriched_picks"] = apply_rs_to_picks(
        results["enriched_picks"], rs_ratings
    )

    # ── Layer 4: Analyst Data ───────────────────────────────
    tickers_to_analyze = [p["ticker"] for p in top_picks_flat[:40]]
    if verbose: print(f"\n📊 LAYER 4: Fetching analyst signals for {len(tickers_to_analyze)} stocks...")
    analyst_data = batch_fetch_analyst_data(tickers_to_analyze)
    results["analyst_data"] = analyst_data

    if verbose and analyst_data:
        print_analyst_highlights(analyst_data)

    # Apply analyst signals
    results["enriched_picks"] = apply_analyst_signals_to_picks(
        results["enriched_picks"], analyst_data
    )

    # ── Layer 2: Score History + Trends ─────────────────────
    if verbose: print("\n📈 LAYER 2: Updating score history and detecting trends...")
    history = update_score_history(top_picks_flat)
    trends  = detect_trending_stocks(history)
    results["trends"]  = trends
    results["history"] = history

    if verbose:
        print_trends(trends)

    # Score velocity weighting — boost fast risers, reduce fast fallers
    try:
        from risk_engine import apply_score_velocity_weight
        results["enriched_picks"] = apply_score_velocity_weight(
            results["enriched_picks"], history
        )
        if verbose:
            boosted = [p for p in results["enriched_picks"] if p.get("velocity_boost",0) > 0]
            if boosted:
                print(f"   Velocity boost: {len(boosted)} picks ({', '.join(p['ticker'] for p in boosted[:4])})")
    except ImportError:
        pass

    return results


# ============================================================
# ENTRY POINT (standalone test)
# ============================================================

if __name__ == "__main__":
    # Test with a small set of tickers
    test_tickers = ["TD.TO", "ENB.TO", "SHOP.TO", "PLTR", "NVDA", "RY.TO", "XGRO.TO"]

    print("Testing Intelligence Layers...")
    print("\n--- Layer 4: Analyst Data ---")
    for ticker in test_tickers[:3]:
        data = fetch_analyst_data(ticker)
        if data.get("status") == "ok":
            sig = data["signal"]
            print(f"\n{ticker}: {sig['direction']} ({sig['magnitude']})")
            for note in sig["notes"]:
                print(f"  {note}")
            for rev in data.get("est_revisions", []):
                print(f"  EPS {rev['period']}: {rev['direction']} ({rev.get('revision_30d_pct', '?')}%)")
        time.sleep(0.5)

    print("\n--- Layer 2: Score History ---")
    # Simulate some history
    fake_picks = [{"ticker": t, "score": 60 + i*3, "data": {"price": 50}} for i, t in enumerate(test_tickers)]
    history = update_score_history(fake_picks)
    print(f"History saved for {len(history)} tickers")

    print("\n✅ Intelligence layers ready")
