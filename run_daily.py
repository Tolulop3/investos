"""
InvestOS — Master Daily Runner v4
===================================
Runs automatically via GitHub Actions every morning (Toronto time).

LAYERS:
  1. News & Macro Analysis      (news_analyzer.py)
  2. Market Regime Filter       (ml_engine.py)
  3. Stock Screen 500+          (stock_screener.py)
  4. News Score Adjustment      (bridge layer)
  5. ML Scoring + Sizing        (ml_engine.py)
  6. RS + History + Analyst     (intelligence_layers.py)
  7. RSS Signal Cross-Reference (portfolio_engine.py)
  8. Conviction Engine          (bridge layer)
  9. FX & Gold Signals          (fx_engine.py)
 10. Crypto Signals             (crypto_engine.py)
 11. Bake dashboard HTML        (run_daily.py)
 12. Send morning brief email   (run_daily.py)

Run locally:   python run_daily.py
Test mode:     python run_daily.py --test
GitHub mode:   python run_daily.py --github
"""

import json
import os
import sys
import time
import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text      import MIMEText
from datetime             import datetime

# ── Core imports ──────────────────────────────────────────
from stock_screener      import run_full_screen
from portfolio_engine    import (fetch_rss_signals, aggregate_x_signals,
                                  project_portfolio_growth, compute_bucket_allocation,
                                  compute_deployment_plan, CONFIG)
from risk_engine         import (run_stress_simulation, run_decay_monitor, run_risk_audit,
                                  check_fx_staleness, get_template_rotation, check_drawdown_lock,
                                  get_current_drawdown, track_signal_accuracy,
                                  compute_position_size_guardrail,
                                  SURVIVORSHIP_NOTE, WHEN_THIS_FAILS)
from news_analyzer       import run_news_analysis
from intelligence_layers import run_all_intelligence_layers, detect_trending_stocks, update_score_history, load_score_history
from ml_engine           import run_ml_engine, get_market_regime
from fx_engine           import run_fx_engine
from content_engine      import run_content_engine
from crypto_engine       import run_crypto_engine


# ============================================================
# BRIDGE FUNCTIONS
# ============================================================

def apply_news_to_screener(screener_results, news_analysis):
    """Apply news ticker adjustments to screener scores"""
    ticker_adj = news_analysis.get("ticker_adjustments", {})
    count = 0
    for bucket in ["FHSA_top5","TFSA_growth_top5","TFSA_income_top5","TFSA_swing_top3",
                   "FHSA_all","TFSA_core_all","TFSA_income_all","TFSA_swing_all"]:
        for pick in screener_results.get(bucket, []):
            adj = ticker_adj.get(pick["ticker"], {})
            n   = adj.get("adjustment", 0)
            if n != 0:
                pick["score"]          = max(0, min(100, pick["score"] + n))
                pick["news_adjustment"]= n
                pick["news_sentiment"] = adj.get("news_sentiment", "NEUTRAL")
                pick["news_reasons"]   = adj.get("reasons", [])
                if n > 0:
                    pick.setdefault("reasons", []).append(f"📰 News +{n}pts: {', '.join(adj.get('reasons',[])[:1])}")
                else:
                    pick.setdefault("flags", []).append(f"📰 News {n}pts: {', '.join(adj.get('reasons',[])[:1])}")
                count += 1

    for bucket in ["FHSA_top5","TFSA_growth_top5","TFSA_income_top5","TFSA_swing_top3"]:
        screener_results[bucket] = sorted(
            screener_results.get(bucket, []), key=lambda x: x["score"], reverse=True
        )
    print(f"   Applied news adjustments: {count} picks | Regime: {news_analysis.get('macro_regime','NORMAL')}")
    return screener_results


def build_conviction_picks(screener_results, x_signals, trends, news_analysis, ml_results):
    """Multi-signal conviction — 2+ signals = high confidence pick"""
    x_tickers          = set()
    trending_tickers   = {t["ticker"] for t in trends.get("trending_up", [])}
    breakout_tickers   = {t["ticker"] for t in trends.get("breakouts", [])}
    regime             = ml_results.get("regime", {}).get("regime", "NORMAL")

    for item in x_signals.get("top_tickers", []):
        x_tickers.add(item["ticker"].upper())
    for s in x_signals.get("account_summaries", []):
        for t in s.get("tickers", []):
            x_tickers.add(t.upper())

    all_picks = (
        screener_results.get("FHSA_top5", []) +
        screener_results.get("TFSA_growth_top5", []) +
        screener_results.get("TFSA_income_top5", []) +
        screener_results.get("TFSA_swing_top3", [])
    )

    conviction = []
    seen       = set()

    for pick in all_picks:
        ticker = pick["ticker"]
        if ticker in seen:
            continue
        seen.add(ticker)

        clean  = ticker.replace(".TO","").replace("-UN","").upper()
        sigs   = []
        boost  = 0

        if clean in x_tickers or ticker in x_tickers:
            sigs.append("📡 X Signal Source"); boost += 10
        if ticker in trending_tickers:
            sigs.append("📈 Score Trending Up"); boost += 8
        if ticker in breakout_tickers:
            sigs.append("🚨 Score Breakout"); boost += 12
        if pick.get("news_sentiment") == "BULLISH":
            sigs.append("📰 Positive News Macro"); boost += 6
        rs = pick.get("rs_rating", 50)
        if rs >= 80:
            sigs.append(f"💪 RS {rs} — Top Performer"); boost += 8
        elif rs >= 70:
            sigs.append(f"💪 RS {rs}"); boost += 4
        ml = pick.get("ml_prob", 0.5)
        if ml >= 0.68:
            sigs.append(f"🤖 ML Score {ml:.0%}"); boost += 8
        elif ml >= 0.58:
            sigs.append(f"🤖 ML Score {ml:.0%}"); boost += 4
        analyst = pick.get("analyst_signal", {})
        if analyst.get("direction") == "BULLISH" and analyst.get("magnitude") in ("STRONG","MODERATE"):
            sigs.append("📊 Analyst Estimates Raised"); boost += 8

        if len(sigs) >= 2:
            pick["conviction_signals"] = sigs
            pick["conviction_boost"]   = boost
            pick["conviction_count"]   = len(sigs)
            pick["score"]              = min(100, pick["score"] + boost)
            conviction.append(pick)

    conviction.sort(key=lambda x: (x["conviction_count"], x["score"]), reverse=True)
    return conviction


def build_calendar(screener_results, news_analysis):
    """Build action calendar from picks + macro events"""
    calendar   = []
    seen       = set()
    active_sig = news_analysis.get("active_signals", {})

    # Macro alerts first
    if "trump_tariff_canada_specific" in active_sig:
        calendar.append({"date":"Today","title":"🚨 Canada Tariff Alert",
                         "desc":"Trump tariff news — FHSA defensive positioning recommended",
                         "urgency":"urgent","ticker":"MACRO","action":"REVIEW"})
    if "boc_rate_cut" in active_sig or "boc_rate_hike" in active_sig:
        calendar.append({"date":"Today","title":"🏦 Bank of Canada Rate Decision",
                         "desc":"Rate decision in news — REITs, utilities, banks all affected",
                         "urgency":"urgent","ticker":"BoC","action":"REVIEW"})
    if "fed_rate_cut" in active_sig or "fed_rate_hike" in active_sig:
        calendar.append({"date":"Today","title":"🏛️ Fed Rate Signal",
                         "desc":"US rate signal — affects growth stocks and CAD/USD",
                         "urgency":"urgent","ticker":"Fed","action":"REVIEW"})

    all_picks = (
        screener_results.get("FHSA_top5",[]) + screener_results.get("TFSA_growth_top5",[]) +
        screener_results.get("TFSA_income_top5",[]) + screener_results.get("TFSA_swing_top3",[])
    )

    for pick in all_picks:
        d = pick.get("data", {}); ticker = pick["ticker"]; p = pick.get("pick", {})
        days_ex = d.get("days_to_ex_div", 999)

        if 0 < days_ex <= 45 and f"ex_{ticker}" not in seen:
            calendar.append({"date": d.get("ex_div_date","TBD"),
                              "title":f"💰 {ticker} Ex-Dividend",
                              "desc": f"Buy {max(1,days_ex-3)} days before to capture ${d.get('div_rate',0):.2f}/share",
                              "urgency":"urgent" if days_ex<=7 else "soon",
                              "ticker":ticker,"action":"BUY"})
            seen.add(f"ex_{ticker}")

        earn = d.get("next_earnings","N/A")
        if earn != "N/A" and p.get("category") == "SWING" and f"earn_{ticker}" not in seen:
            calendar.append({"date":earn,"title":f"⚠️ EXIT {ticker} before earnings",
                              "desc":"Swing position — exit 1-2 days before to avoid volatility",
                              "urgency":"urgent","ticker":ticker,"action":"EXIT"})
            seen.add(f"earn_{ticker}")

    order = {"urgent":0,"soon":1,"info":2}
    calendar.sort(key=lambda x: order.get(x.get("urgency","info"),2))
    return calendar[:12]


def load_signal_accuracy_summary():
    """
    Load auto-tracked signal accuracy — no manual input needed.
    Replaces scorecard. Tracks model directional accuracy automatically.
    """
    from risk_engine import load_signal_accuracy
    data = load_signal_accuracy()
    return data.get("summary", {
        "total_signals":  0,
        "resolved":       0,
        "pending":        0,
        "accuracy_7d":    None,
        "model_verdict":  "⏳ Building — runs automatically each day",
        "note":           "Fully automatic — no manual trade logging needed",
    })


def load_brief_history(n=5):
    """Load last N daily briefs for weekly recap"""
    history = []
    for i in range(1, n+1):
        fname = f"brief_history_{i}.json"
        if os.path.exists(fname):
            try:
                with open(fname) as f:
                    history.append(json.load(f))
            except:
                pass
    return history


def rotate_brief_history(brief):
    """Keep rolling window of last 5 briefs"""
    for i in range(4, 0, -1):
        src = f"brief_history_{i}.json"
        dst = f"brief_history_{i+1}.json"
        if os.path.exists(src):
            os.rename(src, dst)
    with open("brief_history_1.json", "w") as f:
        json.dump(brief, f, default=str)


# ============================================================
# MAIN RUN
# ============================================================

def run_daily(test_mode=False):
    start = datetime.now()
    sep   = "="*60

    print(f"\n{sep}")
    print(f"  INVESTOS — DAILY RUN v4.0")
    print(f"  {start.strftime('%B %d, %Y at %I:%M %p')}")
    print(f"  Layers: News · Regime · Screen · ML · RS · X · Conviction · FX · Crypto · Risk · Content")
    print(f"{sep}")

    # ── Drawdown lock check ──────────────────────────────────
    from risk_engine import get_current_drawdown
    current_dd = get_current_drawdown()
    lock_check = check_drawdown_lock(current_dd)
    if lock_check["locked"]:
        print(f"\n  ⚠️  DRAWDOWN LOCK ACTIVE — {lock_check['message']}")
        print(f"  System will still run analysis but parameter changes are blocked.\n")

    # ── 1. News & Macro ──────────────────────────────────────
    print("\n[1/10] 📰 NEWS & MACRO ANALYSIS")
    news = run_news_analysis(verbose=True)
    macro_regime = news.get("macro_regime","NORMAL")
    print(f"\n  🌍 Regime: {macro_regime} — {news.get('regime_note','')}")

    # ── 2. Market Regime (200-day MA) ────────────────────────
    print(f"\n[2/10] 📊 MARKET REGIME FILTER")
    regime = get_market_regime(verbose=True)

    # ── 3. Stock Screen ──────────────────────────────────────
    print(f"\n[3/10] 🔍 STOCK SCREEN (500+ universe)")
    screener = run_full_screen(max_tickers=30 if test_mode else None, verbose=True)

    # ── 4. News Adjustment ───────────────────────────────────
    print(f"\n[4/10] 🔗 APPLYING NEWS TO SCORES")
    screener = apply_news_to_screener(screener, news)

    # ── 5. ML Engine ─────────────────────────────────────────
    print(f"\n[5/10] 🤖 ML ENGINE (XGBoost + Position Sizing)")
    rs_for_ml = {}  # Will be populated after intelligence layer
    ml_results = run_ml_engine(screener, rs_for_ml, verbose=True)

    # ── 6. Intelligence Layers ───────────────────────────────
    print(f"\n[6/10] 🧠 INTELLIGENCE LAYERS (RS + History + Analyst)")
    all_raw = [p["data"] for bucket in
               ["FHSA_all","TFSA_core_all","TFSA_income_all","TFSA_swing_all"]
               for p in screener.get(bucket,[]) if p.get("data")]
    top_flat = (screener.get("FHSA_top5",[]) + screener.get("TFSA_growth_top5",[]) +
                screener.get("TFSA_income_top5",[]) + screener.get("TFSA_swing_top3",[]))
    intel = run_all_intelligence_layers(all_raw, top_flat, verbose=True)

    # ── 7. X Signal Feeds ────────────────────────────────────
    print(f"\n[7/10] 📡 X SIGNAL FEEDS")
    x_feeds = []
    for account in CONFIG["x_accounts"]:
        print(f"  → @{account['handle']}...", end=" ", flush=True)
        feed = fetch_rss_signals(account["handle"])
        x_feeds.append(feed)
        print("✅" if feed["status"]=="ok" else "❌")
    x_signals    = aggregate_x_signals(x_feeds)
    online_feeds = sum(1 for f in x_feeds if f["status"]=="ok")

    # ── 8. Conviction Engine ─────────────────────────────────
    print(f"\n[8/10] 🎯 CONVICTION ENGINE")
    trends   = intel.get("trends", {})
    conviction = build_conviction_picks(screener, x_signals, trends, news, ml_results)
    print(f"  High-conviction picks (2+ signals): {len(conviction)}")
    for p in conviction[:3]:
        print(f"  {p['ticker']:<12} Score:{p['score']}  Signals:{p['conviction_count']}  ML:{p.get('ml_prob',0.5):.0%}")

    # ── 9. FX & Gold ─────────────────────────────────────────
    print(f"\n[9/12] 💱 FX & GOLD SIGNALS")
    fx_signals = run_fx_engine(news_analysis=news, verbose=True)

    # FX staleness indicator
    fx_signals, stale_pairs = check_fx_staleness(fx_signals)
    if stale_pairs:
        print(f"  ⚠️  Stale FX pairs: {', '.join(stale_pairs)}")

    with open("fx_signals.json","w") as f:
        json.dump(fx_signals, f, indent=2, default=str)

    # ── 10. Crypto Signals ───────────────────────────────────
    print(f"\n[10/12] 🪙 CRYPTO SIGNALS (BTC + SOL)")
    tfsa_bal = CONFIG["accounts"]["TFSA"]["balance"]
    crypto_signals = run_crypto_engine(
        news_analysis=news,
        portfolio_value=tfsa_bal,
        verbose=True
    )
    with open("crypto_signals.json","w") as f:
        json.dump(crypto_signals, f, indent=2, default=str)

    # ── 11. Risk Audit (Stress + Decay) ──────────────────────
    print(f"\n[11/12] 🛡  RISK AUDIT (Stress Test + Decay Monitor)")
    score_history = intel.get("history", {})
    risk_report   = run_risk_audit(
        screener_results = screener,
        score_history    = score_history,
        fx_signals       = fx_signals,
        verbose          = True
    )
    with open("risk_report.json","w") as f:
        json.dump(risk_report, f, indent=2, default=str)

    # ── 12. Content Engine ───────────────────────────────────
    print(f"\n[12/12] ✍️  SOCIAL CONTENT ENGINE")

    # Build brief first (content needs it)
    tfsa_bal  = CONFIG["accounts"]["TFSA"]["balance"]
    fhsa_bal  = CONFIG["accounts"]["FHSA"]["balance"]
    tfsa_proj = project_portfolio_growth(tfsa_bal, 300, 12, 20)
    fhsa_proj = project_portfolio_growth(fhsa_bal, 667, 8, 5)
    calendar  = build_calendar(screener, news)

    # ── Deployment plan (default: full balance as deploy amount) ─
    # Dashboard's calculator overrides this with user-entered amount
    tfsa_bal   = CONFIG["accounts"]["TFSA"]["balance"]
    deploy_amt = tfsa_bal if tfsa_bal > 0 else 100   # fallback for empty account
    deployment_plan = compute_deployment_plan(
        deploy_amount    = deploy_amt,
        account_balance  = tfsa_bal,
        top_picks        = (screener.get("TFSA_growth_top5",[]) +
                            screener.get("TFSA_income_top5",[]) +
                            screener.get("TFSA_swing_top3",[])),
        fx_signals       = fx_signals,
        crypto_signals   = crypto_signals,
        regime           = regime.get("regime","NORMAL"),
    )

    # ── Signal accuracy tracking (automatic — no manual input) ─
    all_picks_flat = (screener.get("FHSA_top5",[]) + screener.get("TFSA_growth_top5",[]) +
                      screener.get("TFSA_income_top5",[]) + screener.get("TFSA_swing_top3",[]))
    score_hist_for_acc = intel.get("history", {})
    signal_accuracy = track_signal_accuracy(all_picks_flat, score_hist_for_acc)

    # ── Position size guardrails ───────────────────────────────
    # Compute max sizes for top conviction picks
    for pick in conviction[:5]:
        acc_sum  = signal_accuracy if signal_accuracy.get("resolved",0) > 0 else None
        guardrail= compute_position_size_guardrail(
            pick["ticker"],
            tfsa_bal,
            pick.get("pick",{}).get("category","GROWTH CORE"),
            acc_sum,
            regime.get("regime","NORMAL")
        )
        pick["size_guardrail"] = guardrail
        # Override pick amount with guardrail recommended size
        if pick.get("pick") and guardrail["recommended"] > 0:
            pick["pick"]["amount"] = guardrail["recommended"]

    brief = {
        "date":             start.strftime("%B %d, %Y"),
        "generated_at":     start.isoformat(),
        "run_duration_sec": round((datetime.now()-start).total_seconds(), 1),

        "macro": {
            "regime":           macro_regime,
            "regime_note":      news.get("regime_note",""),
            "signals_detected": news.get("signals_detected",0),
            "articles_read":    news.get("articles_fetched",0),
            "active_signals":   news.get("active_signals",{}),
            "sector_sentiment": news.get("sector_sentiment",{}),
            "headline_summary": news.get("headline_summary",{}),
        },

        "market_regime": regime,

        "accounts": {
            "FHSA": {
                "balance":         CONFIG["accounts"]["FHSA"]["balance"],
                "max_loss_buffer": round(CONFIG["accounts"]["FHSA"]["balance"]*0.16,2),
                "top_picks":       screener.get("FHSA_top5",[]),
                "projection":      fhsa_proj,
            },
            "TFSA": {
                "balance":      CONFIG["accounts"]["TFSA"]["balance"],
                "buckets":      compute_bucket_allocation(CONFIG["accounts"]["TFSA"]["balance"]),
                "growth_picks": screener.get("TFSA_growth_top5",[]),
                "income_picks": screener.get("TFSA_income_top5",[]),
                "swing_picks":  screener.get("TFSA_swing_top3",[]),
                "projection":   tfsa_proj,
            }
        },

        "conviction_picks":  conviction[:5],
        "fx_signals":        fx_signals,

        "ml": {
            "regime":           ml_results.get("regime",{}),
            "position_sizing":  ml_results.get("position_sizing",[]),
            "backtest_summary": ml_results.get("backtest_summary",{}),
            "feature_importance": ml_results.get("feature_importance",{}),
            "regime_signal":    ml_results.get("regime_signal",""),
        },

        "intelligence": {
            "trends":  trends,
            "rs_top10":sorted(intel.get("rs_ratings",{}).items(),
                              key=lambda x: x[1]["rs_rating"], reverse=True)[:10],
        },

        "signals":        x_signals,
        "x_feeds_status": [{"handle":f["handle"],"status":f["status"],
                            "posts":len(f.get("posts",[]))} for f in x_feeds],
        "calendar":       calendar,
        "signal_accuracy": signal_accuracy,
        "screen_stats":   screener["stats"] | {
            "universe": screener["universe_size"],
            "screened": screener["screened"]
        },
        "crypto":          crypto_signals,
        "deployment_plan": deployment_plan,
        "risk_report":    {
            "stress_test":    risk_report.get("stress_test",{}),
            "decay_monitor":  risk_report.get("decay_monitor",{}),
            "drawdown_lock":  risk_report.get("drawdown_lock",{}),
            "stale_fx_pairs": stale_pairs,
            "robustness_score": risk_report.get("decay_monitor",{}).get("robustness_score", 60),
        },
    }

    # ── OUTCOME TRACKING ────────────────────────────────────────
    # Log today's picks, resolve yesterday's, compute win rate
    try:
        from outcome_tracker import log_picks, resolve_outcomes, compute_win_rate, print_win_rate_report

        # Gather all picks for logging
        all_picks_to_log = (
            screener.get("FHSA_top5", []) +
            screener.get("TFSA_growth_top5", []) +
            screener.get("TFSA_swing_top3", [])
        )

        # Current prices for resolving yesterday's picks
        current_prices = {p["ticker"]: p.get("data", {}).get("price", 0)
                         for p in all_picks_to_log if p.get("data", {}).get("price")}

        resolve_outcomes(current_prices)
        log_picks(all_picks_to_log)
        win_rate = compute_win_rate()
        print_win_rate_report(win_rate)
        brief["win_rate"] = win_rate
    except Exception as e:
        print(f"   ⚠️  Outcome tracker error: {e}")
        brief["win_rate"] = None

    # ── DAILY SHORTLIST ──────────────────────────────────────────
    # Build the 3-pick morning brief
    try:
        from portfolio_engine import build_daily_shortlist
        shortlist = build_daily_shortlist(brief, fx_signals, brief.get("win_rate"))
        brief["shortlist"] = shortlist
        if shortlist.get("primary"):
            p = shortlist["primary"]
            print(f"\n  🎯 TODAY'S PRIMARY PICK: {p['ticker']} "
                  f"| Score {p['score']} | ML {p['ml_prob']}% | {p['signals']} signals")
        if shortlist.get("fx_play"):
            fx = shortlist["fx_play"]
            print(f"  💱 FX PLAY: {fx['pair']} {fx['direction']} ({fx['conviction']}% conviction)")
    except Exception as e:
        print(f"   ⚠️  Shortlist error: {e}")
        brief["shortlist"] = None

    # Content generation
    brief_history = load_brief_history()
    content = run_content_engine(brief, brief_history=brief_history, verbose=True)
    brief["content"] = content

    # ── SAVE ALL FILES ───────────────────────────────────────
    with open("latest_brief.json","w") as f:
        json.dump(brief, f, indent=2, default=str)
    with open("news_analysis.json","w") as f:
        json.dump(news, f, indent=2, default=str)
    with open("content_output.json","w") as f:
        json.dump(content, f, indent=2, default=str)

    rotate_brief_history(brief)

    # ── FINAL SUMMARY ────────────────────────────────────────
    elapsed = round((datetime.now()-start).total_seconds(), 1)
    fx_calls = fx_signals.get("total_signals", 0)
    regime_spx = regime.get("regime","?")

    print(f"\n{sep}")
    print(f"  ✅ COMPLETE in {elapsed}s")
    print(f"{sep}")
    print(f"  📰 News:       {news.get('articles_fetched',0)} articles | {news.get('signals_detected',0)} signals")
    print(f"  📊 SPX Regime: {regime_spx} ({regime.get('pct_above_ma',0):+.1f}% vs 200d MA)")
    print(f"  🔍 Screened:   {brief['screen_stats']['screened']}/{brief['screen_stats']['universe']}")
    print(f"  🤖 ML:         {ml_results.get('picks_scored',0)} picks scored")
    print(f"  🎯 Conviction: {len(conviction)} picks (2+ signals aligned)")
    print(f"  💱 FX calls:   {fx_calls} active signals")
    print(f"  🪙 Crypto:     BTC {crypto_signals.get('assets',{}).get('BTC-USD',{}).get('verdict','—')} | SOL {crypto_signals.get('assets',{}).get('SOL-USD',{}).get('verdict','—')}")
    print(f"  🛡  Risk:       Robustness {risk_report.get('decay_monitor',{}).get('robustness_score',60)}/100 | Stress {risk_report.get('stress_test',{}).get('verdict','—')[:20] if risk_report.get('stress_test') else 'N/A'}")
    print(f"  📡 X feeds:    {online_feeds}/7 online")
    print(f"  📅 Calendar:   {len(calendar)} action items")

    if conviction:
        top = conviction[0]
        print(f"\n  🏆 #1 CONVICTION: {top['ticker']} "
              f"— Score {top['score']}/100 | "
              f"{top['conviction_count']} signals | "
              f"ML {top.get('ml_prob',0.5):.0%}")

    fx_top = fx_signals.get("top_call")
    if fx_top:
        icon = "🟢" if fx_top["direction"]=="LONG" else "🔴"
        print(f"  {icon} TOP FX:       {fx_top['pair']} {fx_top['direction']} "
              f"({fx_top['conviction']}% conviction)")

    print(f"\n  📝 TWEET READY:")
    print(f"  {content['tweet'][:120]}...")
    print(f"\n  💾 Files saved: latest_brief.json | fx_signals.json | content_output.json")
    print(f"  🖥  Open: index.html\n")

    return brief


# ============================================================
# DASHBOARD BAKING
# ============================================================

MARKER_START = "// INVESTOS_DATA_START"
MARKER_END   = "// INVESTOS_DATA_END"

def bake_dashboard(brief, fx_signals, crypto_signals):
    """
    Inject today's data directly into index.html.
    """
    import os
    dashboard_file = "index.html"

    # --- Debug: show working directory and files present ---
    cwd = os.getcwd()
    files = os.listdir(cwd)
    print(f"  📁 Working dir: {cwd}")
    html_files = [f for f in files if f.endswith('.html')]
    print(f"  📄 HTML files found: {html_files}")

    if not os.path.exists(dashboard_file):
        print(f"  ❌ {dashboard_file} not found in {cwd}")
        print(f"  All files: {sorted(files)}")
        return False

    print(f"  ✅ Found {dashboard_file} ({os.path.getsize(dashboard_file)//1024}KB)")

    try:
        # Build a SLIM brief — strip heavy fields that break JS or bloat HTML
        slim_brief = {}
        keep_keys = [
            "date", "macro", "accounts", "conviction_picks", "fx_signals",
            "ml", "intelligence", "signals", "x_feeds_status", "calendar",
            "signal_accuracy", "screen_stats", "crypto", "deployment_plan",
            "risk_report", "content", "win_rate", "shortlist",
            "FHSA_top5", "TFSA_growth_top5", "TFSA_income_top5", "TFSA_swing_top3",
            "screen_results",
        ]
        for k in keep_keys:
            if k in brief:
                slim_brief[k] = brief[k]
        # Also pull picks from screen_results if nested there
        if "screen_results" not in slim_brief and "FHSA_top5" not in slim_brief:
            for k in ["FHSA_top5","TFSA_growth_top5","TFSA_income_top5","TFSA_swing_top3"]:
                if k in brief:
                    slim_brief[k] = brief[k]

        baked = json.dumps({
            "brief":    slim_brief,
            "fx":       fx_signals  or {},
            "crypto":   crypto_signals or {},
            "baked_at": datetime.now().isoformat(),
        }, default=str, ensure_ascii=True)  # ensure_ascii=True prevents JS-breaking chars
        # Escape </script> sequences that would break inline JS
        baked = baked.replace("</script>", r"<\/script>").replace("</", r"<\/")
        print(f"  📦 Baked JSON size: {len(baked)//1024}KB")
    except Exception as e:
        print(f"  ❌ JSON serialization failed: {e}")
        return False

    try:
        with open(dashboard_file, "r", encoding="utf-8") as f:
            html = f.read()
        print(f"  ✅ Read {dashboard_file}: {len(html)//1024}KB, {html.count(chr(10))} lines")
    except Exception as e:
        print(f"  ❌ Failed to read {dashboard_file}: {e}")
        return False

    has_start = MARKER_START in html
    has_end   = MARKER_END   in html
    print(f"  Marker START found: {has_start}")
    print(f"  Marker END found:   {has_end}")

    import re

    if has_start and has_end:
        s    = html.index(MARKER_START) + len(MARKER_START)
        e    = html.index(MARKER_END)
        html = html[:s] + "\nconst BAKED = " + baked + ";\n" + html[e:]
        print(f"  ✅ Injected via markers")
    else:
        # Fallback 1: replace any const BAKED = ...; line
        pattern = r'const BAKED\s*=\s*[^;]+;'
        if re.search(pattern, html):
            html = re.sub(pattern, f"const BAKED = {baked};", html, count=1)
            print(f"  ✅ Injected via regex BAKED replace")
        elif "document.addEventListener('DOMContentLoaded'" in html:
            # Fallback 2: inject before DOMContentLoaded
            html = html.replace(
                "document.addEventListener('DOMContentLoaded'",
                f"const BAKED = {baked};\ndocument.addEventListener('DOMContentLoaded'",
                1
            )
            print(f"  ✅ Injected before DOMContentLoaded")
        else:
            # Fallback 3: inject before </script>
            html = html.replace("</script>", f"const BAKED = {baked};\n</script>", 1)
            print(f"  ✅ Injected before </script>")

    try:
        with open(dashboard_file, "w", encoding="utf-8") as f:
            f.write(html)
        # Verify the write worked
        with open(dashboard_file, "r", encoding="utf-8") as f:
            verify = f.read()
        if "const BAKED = null" in verify:
            print(f"  ❌ VERIFICATION FAILED — BAKED is still null after write!")
            return False
        if "const BAKED = {" in verify or 'const BAKED = {"' in verify:
            print(f"  ✅ Dashboard baked and VERIFIED ({len(html)//1024}KB)")
            return True
        print(f"  ⚠️  Written but BAKED content unclear — check manually")
        return True
    except Exception as e:
        print(f"  ❌ Failed to write {dashboard_file}: {e}")
        return False


# ============================================================
# MORNING BRIEF EMAIL
# ============================================================

def send_morning_brief(brief, fx_signals, crypto_signals):
    """
    Send the morning brief email.
    Only called on successful run (--github flag).
    Credentials come from GitHub Secrets via environment variables.

    Setup (one time):
      GitHub repo → Settings → Secrets → Actions → New secret
        GMAIL_USER     = your.investos.email@gmail.com
        GMAIL_PASSWORD = your-gmail-app-password  (NOT your login password)
        NOTIFY_EMAIL   = where you want to receive the brief
    """
    gmail_user  = os.environ.get("GMAIL_USER")
    gmail_pass  = os.environ.get("GMAIL_PASSWORD")
    notify_to   = os.environ.get("NOTIFY_EMAIL")

    if not all([gmail_user, gmail_pass, notify_to]):
        print("  ⚠️  Email credentials not set — skipping brief email")
        print("      Set GMAIL_USER, GMAIL_PASSWORD, NOTIFY_EMAIL in GitHub Secrets")
        return False

    # ── Pull the key data ─────────────────────────────────
    today       = datetime.now().strftime("%B %d, %Y")
    regime_data = brief.get("market_regime", {})
    regime      = regime_data.get("regime", "NORMAL")
    regime_icons= {"BULL":"✅","NORMAL":"📊","CAUTION":"⚠️","BEAR":"🔴","RISK_OFF":"🚨"}
    regime_icon = regime_icons.get(regime, "📊")

    conviction  = brief.get("conviction_picks", [])[:5]
    accounts    = brief.get("accounts", {})
    fhsa_picks  = accounts.get("FHSA", {}).get("top_picks", [])[:2]
    tfsa_income = accounts.get("TFSA", {}).get("income_picks", [])[:2]
    tfsa_growth = accounts.get("TFSA", {}).get("growth_picks", [])[:2]
    tfsa_swing  = accounts.get("TFSA", {}).get("swing_picks", [])[:1]
    deployment  = brief.get("deployment_plan", {})
    signal_acc  = brief.get("signal_accuracy", {})
    fx_top      = fx_signals.get("top_call") if fx_signals else None
    btc         = crypto_signals.get("assets", {}).get("BTC-USD", {}) if crypto_signals else {}
    sol         = crypto_signals.get("assets", {}).get("SOL-USD", {}) if crypto_signals else {}
    news        = brief.get("market_regime", {})
    screen      = brief.get("screen_stats", {})
    acc_7d      = signal_acc.get("accuracy_7d")

    # ── Regime line ───────────────────────────────────────
    regime_scale = {"BULL":"100%","NORMAL":"85%","CAUTION":"65%","BEAR":"40%"}.get(regime,"85%")
    regime_note  = regime_data.get("note", "")

    # ── Build pick lines ──────────────────────────────────
    def pick_line(p, icon="📈"):
        t   = p.get("ticker","?")
        sc  = p.get("score",0)
        pk  = p.get("pick",{})
        act = (pk.get("action","") or "")[:55]
        cat = pk.get("category","")
        tag = "[FHSA]" if "FHSA" in cat else "[TFSA]"
        return f"  {icon} {tag} {t:<10} Score {sc:>3}/100 — {act}"

    all_picks = []
    for p in fhsa_picks:
        all_picks.append(pick_line(p, "🏠"))
    for p in tfsa_income:
        all_picks.append(pick_line(p, "💰"))
    for p in tfsa_growth:
        all_picks.append(pick_line(p, "📈"))
    for p in tfsa_swing:
        all_picks.append(pick_line(p, "⚡"))

    picks_text = "\n".join(all_picks) if all_picks else "  No picks generated today"

    # ── Deployment plan lines ─────────────────────────────
    deploy_lines = []
    splits = deployment.get("splits", {})
    deploy_amt = deployment.get("deploy_amount", 0)
    if splits and deploy_amt:
        deploy_lines.append(f"  Based on ${deploy_amt:,.0f} deployment ({regime} — {regime_scale} deployed):")
        bucket_icons = {"floor":"🏛 ","model_picks":"🤖","swing":"⚡","crypto":"🪙"}
        for name, s in splits.items():
            icon  = bucket_icons.get(name,"  ")
            venue = s.get("venue","").replace("_ACCOUNT","").replace("_"," ")
            deploy_lines.append(
                f"  {icon} {s.get('pick','?'):<12} ${s.get('dollars',0):>7.2f}"
                f"  stop {s.get('stop','?')}  [{venue}]"
            )
        skipped = deployment.get("skip_buckets",[])
        if skipped:
            deploy_lines.append(f"  ⏸  No signal for: {', '.join(skipped)} — hold as cash")
        uninvested = deployment.get("uninvested",0)
        if uninvested > 0.5:
            deploy_lines.append(f"  💵 Uninvested: ${uninvested:.2f} — regime reduction, keep as cash")
    else:
        deploy_lines.append("  Update balance in CONFIG to see deployment plan")

    deploy_text = "\n".join(deploy_lines)

    # ── FX call ───────────────────────────────────────────
    if fx_top:
        fx_dir  = fx_top.get("direction","?")
        fx_pair = fx_top.get("pair","?")
        fx_conv = fx_top.get("conviction",0)
        fx_stop = fx_top.get("stop","?")
        fx_tgt  = fx_top.get("target","?")
        fx_why  = (fx_top.get("key_driver","") or "")[:60]
        fx_icon = "🟢" if fx_dir=="LONG" else "🔴"
        fx_text = (f"  {fx_icon} {fx_pair} {fx_dir} — {fx_conv}% conviction\n"
                   f"  Entry: {fx_top.get('entry','?')} | Stop: {fx_stop} | Target: {fx_tgt}\n"
                   f"  Why: {fx_why}")
    else:
        fx_text = "  No high-conviction FX call today"

    # ── Crypto ───────────────────────────────────────────
    def crypto_line(a, name):
        if not a: return f"  {name}: No data"
        d   = a.get("direction","NEUTRAL")
        c   = a.get("conviction",0)
        p   = a.get("price",0)
        v   = a.get("verdict","")
        icon= "🟢" if d=="LONG" else "🔴" if d=="SHORT" else "⚪"
        return f"  {icon} {name}: {d} — {c}% conviction — ${p:,.0f}  {v}"

    crypto_text = crypto_line(btc,"BTC") + "\n" + crypto_line(sol,"SOL")


    # ── Signal accuracy ───────────────────────────────────
    if acc_7d is not None:
        acc_icon  = "✅" if acc_7d >= 65 else "⚠️" if acc_7d >= 50 else "🔴"
        acc_text  = f"  {acc_icon} 7-day accuracy: {acc_7d}%  |  Signals tracked: {signal_acc.get('total_signals',0)}"
    else:
        acc_text = "  Accuracy tracking building — check back in 7 days"

    # ── Stats line ───────────────────────────────────────
    stats_text = (f"  Screened {screen.get('screened',0)}/{screen.get('universe',0)} stocks  |"
                  f"  {screen.get('tfsa_growth',0)} growth  |"
                  f"  {screen.get('tfsa_income',0)} income  |"
                  f"  {screen.get('tfsa_swing',0)} swing candidates")

    # ── GitHub Pages URL ─────────────────────────────────
    repo = os.environ.get("GITHUB_REPOSITORY","your-username/investos")
    username = repo.split("/")[0] if "/" in repo else repo
    dashboard_url = f"https://{username}.github.io/investos"

    # ══════════════════════════════════════════════════════
    # BUILD THE EMAIL
    # ══════════════════════════════════════════════════════
    subject = f"📊 InvestOS Brief — {today} — {regime_icon} {regime}"

    # Plain text version
    plain = f"""
InvestOS Daily Brief — {today}
{'='*50}

MARKET REGIME: {regime_icon} {regime}
{regime_note[:100] if regime_note else ''}
Deploy scale: {regime_scale}

{'─'*50}
TODAY'S PICKS
{'─'*50}
{picks_text}

{'─'*50}
DEPLOYMENT PLAN
{'─'*50}
{deploy_text}

{'─'*50}
FX TOP CALL
{'─'*50}
{fx_text}

{'─'*50}
CRYPTO
{'─'*50}
{crypto_text}

{'─'*50}
SIGNAL ACCURACY
{'─'*50}
{acc_text}

{'─'*50}
SCREEN STATS
{'─'*50}
{stats_text}

{'─'*50}
Full dashboard: {dashboard_url}
{'─'*50}

⚠️  Model suggestions only. Always verify before executing. Use stop losses.
"""

    # HTML version — clean and readable on mobile
    def regime_color(r):
        return {"BULL":"#00f5a0","NORMAL":"#ffc947","CAUTION":"#ffc947","BEAR":"#ff4d4d","RISK_OFF":"#ff4d4d"}.get(r,"#ffc947")

    rc = regime_color(regime)

    html_picks = ""
    for p in (fhsa_picks + tfsa_income + tfsa_growth + tfsa_swing):
        t   = p.get("ticker","?")
        sc  = p.get("score",0)
        pk  = p.get("pick",{})
        cat = pk.get("category","")
        act = (pk.get("action","") or "")[:70]
        stp = pk.get("exit_note","")[:60]
        tag = "FHSA" if "FHSA" in cat else "TFSA"
        sc_col = "#00f5a0" if sc>=75 else "#ffc947" if sc>=55 else "#ff4d4d"
        html_picks += f"""
        <tr>
          <td style="padding:10px 8px;border-bottom:1px solid #2a2a3e">
            <span style="font-size:15px;font-weight:700;color:#fff;font-family:monospace">{t}</span>
            <span style="font-size:10px;padding:2px 6px;background:#1a1a2e;border-radius:3px;color:#3d9bff;margin-left:6px">{tag}</span>
          </td>
          <td style="padding:10px 8px;border-bottom:1px solid #2a2a3e;text-align:center">
            <span style="font-size:15px;font-weight:800;color:{sc_col};font-family:monospace">{sc}</span>
          </td>
          <td style="padding:10px 8px;border-bottom:1px solid #2a2a3e;font-size:11px;color:#aaa">{act}</td>
          <td style="padding:10px 8px;border-bottom:1px solid #2a2a3e;font-size:10px;color:#888">{stp}</td>
        </tr>"""

    html_deploy = ""
    bucket_bg = {"floor":"rgba(0,245,160,.08)","model_picks":"rgba(61,155,255,.08)",
                 "swing":"rgba(255,201,71,.08)","crypto":"rgba(255,153,0,.08)"}
    bucket_col = {"floor":"#00f5a0","model_picks":"#3d9bff","swing":"#ffc947","crypto":"rgba(255,153,0,.9)"}
    bucket_label = {"floor":"FLOOR 50%","model_picks":"MODEL 30%","swing":"SWING 15%","crypto":"CRYPTO 5%"}
    for name, s in splits.items():
        bg  = bucket_bg.get(name,"transparent")
        col = bucket_col.get(name,"#fff")
        lbl = bucket_label.get(name,name.upper())
        venue = s.get("venue","").replace("_ACCOUNT","").replace("_"," ")
        html_deploy += f"""
        <tr style="background:{bg}">
          <td style="padding:10px 8px;border-bottom:1px solid #2a2a3e;font-size:10px;color:{col};font-weight:700">{lbl}</td>
          <td style="padding:10px 8px;border-bottom:1px solid #2a2a3e;font-size:14px;font-weight:700;color:#fff;font-family:monospace">{s.get('pick','?')}</td>
          <td style="padding:10px 8px;border-bottom:1px solid #2a2a3e;font-size:14px;font-weight:700;color:{col}">${s.get('dollars',0):.2f}</td>
          <td style="padding:10px 8px;border-bottom:1px solid #2a2a3e;font-size:11px;color:#ff4d4d">Stop {s.get('stop','?')}</td>
          <td style="padding:10px 8px;border-bottom:1px solid #2a2a3e;font-size:10px;color:#888">{venue}</td>
        </tr>"""

    if not html_deploy:
        html_deploy = '<tr><td colspan="5" style="padding:10px;color:#888;font-size:11px">Update balance in CONFIG to see deployment plan</td></tr>'

    fx_color = "#00f5a0" if fx_top and fx_top.get("direction")=="LONG" else "#ff4d4d"
    fx_html  = f"""
      <p style="margin:0;font-size:13px;color:{fx_color};font-weight:700">
        {'🟢' if fx_top and fx_top.get('direction')=='LONG' else '🔴'}
        {fx_top.get('pair','?') if fx_top else '—'}
        {fx_top.get('direction','?') if fx_top else ''}
        — {fx_top.get('conviction',0) if fx_top else 0}% conviction
      </p>
      <p style="margin:6px 0 0;font-size:11px;color:#888">
        Entry: {fx_top.get('entry','?') if fx_top else '—'} &nbsp;|&nbsp;
        Stop: {fx_top.get('stop','?') if fx_top else '—'} &nbsp;|&nbsp;
        Target: {fx_top.get('target','?') if fx_top else '—'}
      </p>
      <p style="margin:4px 0 0;font-size:11px;color:#aaa">{(fx_top.get('key_driver','') or '')[:80] if fx_top else 'No high-conviction FX call today'}</p>
    """ if fx_top else "<p style='color:#888;font-size:12px'>No high-conviction FX call today</p>"

    def crypto_html_row(a, name):
        if not a: return f"<tr><td colspan='3' style='padding:8px;color:#888;font-size:11px'>{name}: No data</td></tr>"
        d   = a.get("direction","NEUTRAL")
        c   = a.get("conviction",0)
        p   = a.get("price",0)
        v   = a.get("verdict","")
        col = "#00f5a0" if d=="LONG" else "#ff4d4d" if d=="SHORT" else "#888"
        icon= "🟢" if d=="LONG" else "🔴" if d=="SHORT" else "⚪"
        return f"""
        <tr>
          <td style="padding:8px;font-size:13px;font-weight:700;color:#fff">{icon} {name}</td>
          <td style="padding:8px;font-size:13px;font-weight:700;color:{col}">{d}</td>
          <td style="padding:8px;font-size:12px;color:#aaa">{c}% — ${p:,.0f} — {v}</td>
        </tr>"""

    acc_col = "#00f5a0" if acc_7d and acc_7d >= 65 else "#ffc947" if acc_7d and acc_7d >= 50 else "#ff4d4d"

    html_body = f"""
<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#0d0d1a;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif">
<div style="max-width:620px;margin:0 auto;background:#0d0d1a">

  <!-- Header -->
  <div style="background:linear-gradient(135deg,#1a1a2e,#12122a);padding:24px 28px;border-bottom:2px solid {rc}">
    <div style="font-size:11px;color:#888;letter-spacing:2px;margin-bottom:6px">INVESTOS DAILY BRIEF</div>
    <div style="font-size:22px;font-weight:800;color:#fff;margin-bottom:4px">{today}</div>
    <div style="display:inline-block;padding:4px 14px;background:{rc}22;border:1px solid {rc};border-radius:3px;
                font-size:12px;font-weight:700;color:{rc};letter-spacing:1px">{regime_icon} {regime} — {regime_scale} DEPLOYED</div>
  </div>

  <!-- Picks -->
  <div style="padding:20px 28px 0">
    <div style="font-size:9px;color:#888;letter-spacing:2px;margin-bottom:12px">TODAY'S PICKS</div>
    <table style="width:100%;border-collapse:collapse">
      <thead>
        <tr style="background:#1a1a2e">
          <th style="padding:8px;text-align:left;font-size:9px;color:#888;letter-spacing:1px">TICKER</th>
          <th style="padding:8px;text-align:center;font-size:9px;color:#888;letter-spacing:1px">SCORE</th>
          <th style="padding:8px;text-align:left;font-size:9px;color:#888;letter-spacing:1px">ACTION</th>
          <th style="padding:8px;text-align:left;font-size:9px;color:#888;letter-spacing:1px">EXIT</th>
        </tr>
      </thead>
      <tbody>{html_picks}</tbody>
    </table>
  </div>

  <!-- Deployment -->
  <div style="padding:20px 28px 0">
    <div style="font-size:9px;color:#888;letter-spacing:2px;margin-bottom:8px">DEPLOYMENT PLAN</div>
    {'<div style="font-size:11px;color:#888;margin-bottom:8px">'+deploy_lines[0]+'</div>' if deploy_lines else ''}
    <table style="width:100%;border-collapse:collapse">
      <thead>
        <tr style="background:#1a1a2e">
          <th style="padding:8px;text-align:left;font-size:9px;color:#888;letter-spacing:1px">BUCKET</th>
          <th style="padding:8px;text-align:left;font-size:9px;color:#888;letter-spacing:1px">PICK</th>
          <th style="padding:8px;text-align:left;font-size:9px;color:#888;letter-spacing:1px">AMOUNT</th>
          <th style="padding:8px;text-align:left;font-size:9px;color:#888;letter-spacing:1px">STOP</th>
          <th style="padding:8px;text-align:left;font-size:9px;color:#888;letter-spacing:1px">VENUE</th>
        </tr>
      </thead>
      <tbody>{html_deploy}</tbody>
    </table>
  </div>

  <!-- FX -->
  <div style="padding:20px 28px 0">
    <div style="font-size:9px;color:#888;letter-spacing:2px;margin-bottom:10px">FX TOP CALL</div>
    <div style="background:#1a1a2e;border-radius:4px;padding:14px 16px">
      {fx_html}
    </div>
  </div>

  <!-- Crypto -->
  <div style="padding:20px 28px 0">
    <div style="font-size:9px;color:#888;letter-spacing:2px;margin-bottom:10px">CRYPTO</div>
    <table style="width:100%;border-collapse:collapse;background:#1a1a2e;border-radius:4px">
      <tbody>
        {crypto_html_row(btc,'BTC')}
        {crypto_html_row(sol,'SOL')}
      </tbody>
    </table>
  </div>

  <!-- Accuracy + Stats -->
  <div style="padding:20px 28px">
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px">
      <div style="background:#1a1a2e;border-radius:4px;padding:14px 16px">
        <div style="font-size:9px;color:#888;letter-spacing:1px;margin-bottom:6px">SIGNAL ACCURACY</div>
        <div style="font-size:20px;font-weight:800;color:{acc_col};font-family:monospace">
          {f'{acc_7d}%' if acc_7d is not None else '—'}
        </div>
        <div style="font-size:10px;color:#888;margin-top:4px">7-day · {signal_acc.get('total_signals',0)} signals tracked</div>
      </div>
      <div style="background:#1a1a2e;border-radius:4px;padding:14px 16px">
        <div style="font-size:9px;color:#888;letter-spacing:1px;margin-bottom:6px">TODAY'S SCREEN</div>
        <div style="font-size:20px;font-weight:800;color:#3d9bff;font-family:monospace">
          {screen.get('screened',0)}/{screen.get('universe',0)}
        </div>
        <div style="font-size:10px;color:#888;margin-top:4px">stocks analyzed</div>
      </div>
    </div>
  </div>

  <!-- Footer -->
  <div style="padding:16px 28px 24px;border-top:1px solid #1a1a2e;text-align:center">
    <a href="{dashboard_url}"
       style="display:inline-block;padding:12px 28px;background:{rc};color:#000;
              font-weight:800;font-size:13px;letter-spacing:1px;border-radius:3px;
              text-decoration:none">OPEN FULL DASHBOARD →</a>
    <p style="margin:16px 0 0;font-size:10px;color:#555;line-height:1.6">
      Model suggestions only — not financial advice.<br>
      Always verify prices before executing. Use stop losses.
    </p>
  </div>

</div>
</body>
</html>"""

    # ── Send ──────────────────────────────────────────────
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = f"InvestOS <{gmail_user}>"
        msg["To"]      = notify_to

        msg.attach(MIMEText(plain,     "plain"))
        msg.attach(MIMEText(html_body, "html"))

        ctx = ssl.create_default_context()
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=ctx) as server:
            server.login(gmail_user, gmail_pass)
            server.sendmail(gmail_user, notify_to, msg.as_string())

        print(f"  ✅ Morning brief sent → {notify_to}")
        return True

    except Exception as e:
        print(f"  ⚠️  Email failed: {e}")
        print("      Check GMAIL_USER / GMAIL_PASSWORD secrets in GitHub")
        return False


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    github_mode = "--github" in sys.argv
    test_mode   = "--test"   in sys.argv

    brief = run_daily(test_mode=test_mode)

    if brief:
        # Always bake the dashboard (local or GitHub)
        print("\n  📊 Baking dashboard...")

        fx  = {}
        cry = {}
        try:
            with open("fx_signals.json")     as f: fx  = json.load(f)
        except: pass
        try:
            with open("crypto_signals.json") as f: cry = json.load(f)
        except: pass

        bake_dashboard(brief, fx, cry)

        # Send morning brief email only in GitHub Actions mode
        if github_mode:
            print("  📧 Sending morning brief...")
            send_morning_brief(brief, fx, cry)

        print("  ✅ InvestOS complete")


