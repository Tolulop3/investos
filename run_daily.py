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
try:
    from ngx_screener import run_ngx_screen as run_ngx_engine
    HAS_NGX = True
    try:
        from ngx_outcome_tracker import (log_ngx_signals, resolve_ngx_outcomes,
                                          print_ngx_outcome_report)
        HAS_NGX_TRACKER = True
    except ImportError:
        HAS_NGX_TRACKER = False
except ImportError:
    HAS_NGX = False
    HAS_NGX_TRACKER = False
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
                                  compute_deployment_plan, CONFIG,
                                  get_scorecard, load_trades)
from risk_engine         import (run_stress_simulation, run_decay_monitor, run_risk_audit,
                                  check_fx_staleness, get_template_rotation, check_drawdown_lock,
                                  get_current_drawdown, track_signal_accuracy,
                                  compute_position_size_guardrail,
                                  SURVIVORSHIP_NOTE, WHEN_THIS_FAILS)
from news_analyzer       import run_news_analysis
from insider_engine      import run_insider_engine
from etf_engine          import run_etf_engine
from intelligence_layers import run_all_intelligence_layers, detect_trending_stocks, update_score_history, load_score_history, apply_score_decay
from ml_engine           import run_ml_engine, get_market_regime, get_cooldown_set
from fx_engine           import run_fx_engine
from content_engine      import run_content_engine
from crypto_engine       import run_crypto_engine
from options_engine      import run_options_engine
from regime_predictor    import predict_regime_shift
from ml_retrainer        import retrain_if_due


# ============================================================
# BRIDGE FUNCTIONS
# ============================================================

def apply_news_to_screener(screener_results, news_analysis):
    ticker_adj     = news_analysis.get("ticker_adjustments", {})
    sector_sent    = news_analysis.get("sector_sentiment",   {})
    count          = 0
    sector_penalised = 0
    hard_excluded  = set()   # tickers with news_penalty>=15 + HIGH/CRITICAL signal

    SECTOR_MAP = {
        "Communication Services": "TELECOM",
        "Industrials":            "AIRLINES",
        "Consumer Discretionary": "CONSUMER_DISCRETIONARY",
        "Consumer Cyclical":      "CONSUMER_DISCRETIONARY",
        "Materials":              "CANADIAN_MATERIALS",
        "Energy":                 "OIL",
        "Utilities":              "CANADIAN_UTILITIES",
        "Financials":             "CANADIAN_BANKS",
        "Real Estate":            "CANADIAN_REITS",
    }

    buckets = ["FHSA_top5","TFSA_growth_top5","TFSA_income_top5","TFSA_swing_top3",
               "FHSA_all","TFSA_core_all","TFSA_income_all","TFSA_swing_all"]

    for bucket in buckets:
        for pick in screener_results.get(bucket, []):
            adj = ticker_adj.get(pick["ticker"], {})
            n   = adj.get("adjustment", 0)
            if n != 0:
                n_capped = max(-15, min(15, n))
                pick["score"]           = max(0, min(100, pick["score"] + n_capped))
                pick["news_adjustment"] = n_capped
                pick["news_original"]   = n
                pick["news_sentiment"]  = adj.get("news_sentiment", "NEUTRAL")
                pick["news_reasons"]    = adj.get("reasons", [])
                mag = adj.get("causing_magnitude", "LOW")
                if n_capped <= -15 and mag in ("HIGH", "CRITICAL"):
                    pick["news_hard_exclude"] = True
                    hard_excluded.add(pick["ticker"])
                if n_capped > 0:
                    pick.setdefault("reasons", []).append(
                        f"📰 News +{n_capped}pts: {', '.join(adj.get('reasons',[])[:1])}")
                else:
                    pick.setdefault("flags", []).append(
                        f"📰 News {n_capped}pts: {', '.join(adj.get('reasons',[])[:1])}")
                count += 1

            yf_sector  = pick.get("sector", "")
            news_sector = SECTOR_MAP.get(yf_sector)
            if news_sector and sector_sent:
                net = sector_sent.get(news_sector, {}).get("net_score", 0)
                if net <= -300:   penalty = -12
                elif net <= -200: penalty = -8
                elif net <= -100: penalty = -5
                else:             penalty = 0
                if penalty < 0:
                    pick["score"] = max(0, pick["score"] + penalty)
                    pick.setdefault("flags", []).append(
                        f"⚠️ Sector headwind ({news_sector} net:{net}): {penalty}pts")
                    sector_penalised += 1

    # Hard-exclude picks from top-5 buckets so they don't enter sizing or logging
    if hard_excluded:
        print(f"   🚫 News hard-exclude (penalty≥15 + HIGH/CRITICAL): {sorted(hard_excluded)}")
        for bucket in ["FHSA_top5","TFSA_growth_top5","TFSA_income_top5","TFSA_swing_top3"]:
            screener_results[bucket] = [
                p for p in screener_results.get(bucket, [])
                if not p.get("news_hard_exclude")
            ]

    for bucket in ["FHSA_top5","TFSA_growth_top5","TFSA_income_top5","TFSA_swing_top3"]:
        screener_results[bucket] = sorted(
            screener_results.get(bucket, []), key=lambda x: x["score"], reverse=True
        )
    screener_results["_news_hard_excluded"] = hard_excluded
    print(f"   Applied news adjustments: {count} picks | Regime: {news_analysis.get('macro_regime','NORMAL')}")
    if sector_penalised:
        print(f"   ⚠️ Sector headwind penalty: {sector_penalised} picks docked")
    return screener_results


def compute_early_regime(macro_regime_str, market_regime_dict):
    market_reg = market_regime_dict.get("regime", "UNKNOWN")
    macro_reg  = macro_regime_str or "NORMAL"

    if market_reg == "BULL":       m = 1.0
    elif market_reg == "RECOVERY": m = 0.3
    elif market_reg == "CAUTION":  m = -0.3
    elif market_reg == "BEAR":     m = -1.0
    else:                          m = 0.0

    if macro_reg in ("BULL","RISK_ON","NORMAL"): n = 0.5
    elif macro_reg == "CAUTIOUS":                n = -0.3
    elif macro_reg in ("RISK_OFF","BEAR"):       n = -1.0
    else:                                        n = 0.0

    # 70% market structure (SPX vs 200MA), 30% news macro
    # Market regime is the primary signal — SPX +8% above 200MA in BULL
    # should NOT be overridden by news sentiment alone.
    # News macro affects individual pick adjustments (±8pts) separately.
    score = 0.70 * m + 0.30 * n

    if score >= 0.4:    regime = "RISK_ON";              blocks = []
    elif score >= 0.1:  regime = "NEUTRAL";              blocks = []
    elif score >= -0.3: regime = "DEFENSIVE";            blocks = ["SWING"]
    else:               regime = "CAPITAL_PRESERVATION"; blocks = ["SWING", "GROWTH CORE"]

    return regime, blocks


def apply_regime_filter(screener_results, early_regime, category_blocks):
    if not category_blocks:
        return screener_results

    removed_count = 0
    filtered = {}

    for bucket, picks in screener_results.items():
        if not isinstance(picks, list):
            filtered[bucket] = picks
            continue
        kept = []
        for pick in picks:
            cat = pick.get("pick", {}).get("category", "") or ""
            if any(blk in cat for blk in category_blocks):
                removed_count += 1
                pick["regime_blocked"] = True
                pick["regime_block_reason"] = f"{early_regime} blocks {cat} picks"
                continue
            if early_regime == "DEFENSIVE" and "GROWTH CORE" in cat:
                if pick.get("score", 0) > 75:
                    pick["score"] = 75
                    pick.setdefault("flags", []).append("⚠️ DEFENSIVE regime: GROWTH CORE capped at 75")
            kept.append(pick)
        filtered[bucket] = kept

    if removed_count:
        print(f"  🛡  Regime filter [{early_regime}]: {removed_count} picks removed (blocked: {category_blocks})")

    return filtered


def build_conviction_picks(screener_results, x_signals, trends, news_analysis, ml_results, early_regime="RISK_ON", options_signals=None, cooldown_set=None):
    x_tickers          = set()
    trending_tickers   = {t["ticker"] for t in trends.get("trending_up", [])}
    breakout_tickers   = {t["ticker"] for t in trends.get("breakouts", [])}

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
    # Also include ML sizing picks — these may have been sector-capped from screener
    # but still scored by ML. Avoids AFRM-type gaps where ML sees the pick but conviction doesn't.
    ml_sized = ml_results.get("position_sizing", []) if ml_results else []
    for _mp in ml_sized:
        _mt = _mp.get("ticker","")
        if _mt and _mp.get("weight_pct",0) > 0:
            # Build a minimal pick dict from ML output to feed conviction
            _base = screener_results.get("TFSA_core_all", [])
            _found = next((p for p in _base if p.get("ticker") == _mt), None)
            if _found and _found.get("ticker") not in {p.get("ticker") for p in all_picks}:
                all_picks = list(all_picks) + [_found]

    conviction = []
    seen       = set()

    # Conviction is signal-count only — 2+ independent signals aligned.
    # ML prob is one of those signals (≥0.68 adds a signal), but it is NOT a gate.
    # The ML gate (sector-first + compression-aware ML) was already applied in the
    # sizing path. Conviction does not re-gate; it only counts evidence signals.

    for pick in all_picks:
        ticker = pick["ticker"]
        if ticker in seen:
            continue
        seen.add(ticker)
        # Skip tickers on cooldown — invisible to conviction AND content engine
        if cooldown_set and (ticker in cooldown_set or ticker.replace(".TO","").replace("-UN","").upper() in cooldown_set):
            continue

        clean = ticker.replace(".TO","").replace("-UN","").upper()
        sigs  = []
        boost = 0

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
        # Options flow signal — only bullish signals count (HIGH_IV is a warning, not bullish)
        if options_signals:
            opt = options_signals.get(ticker) or options_signals.get(clean)
            if opt:
                opt_sig = opt.get("signal","")
                opt_pcr = opt.get("pcr_stock", 1.0)
                if opt_sig == "UNUSUAL_CALL_SWEEP":
                    # Genuine unusual sweep — high quality signal
                    sigs.append(f"🎯 Options: Unusual Call Sweep"); boost += 10
                elif opt_sig == "BULLISH_PCR" and opt_pcr <= 0.40:
                    # Strongly bullish PCR (not just barely <0.5)
                    sigs.append(f"📊 Options: Bullish PCR {opt_pcr:.2f}"); boost += 6
                elif opt_sig == "HIGH_CALL_VOLUME":
                    # Elevated call volume without PCR context — moderate signal
                    sigs.append(f"📊 Options: High Call Volume"); boost += 4

        if len(sigs) >= 2:
            rs_check = pick.get("rs_rating", 0)
            if rs_check < 70:
                pick["rs_blocked"] = True
                continue

            cat = pick.get("pick", {}).get("category", "") or ""
            STYLE_MAP = {
                "SWING": "breakout", "GROWTH CORE": "momentum",
                "FHSA Conservative": "defensive", "INCOME": "dividend",
                "DIVIDEND": "dividend", "WATCH": "defensive",
            }
            pick_style = next((v for k, v in STYLE_MAP.items() if k in cat), "momentum")
            REGIME_BLOCKS = {
                "RISK_ON": [], "NEUTRAL": [],
                "DEFENSIVE": ["breakout"],
                "CAPITAL_PRESERVATION": ["breakout", "momentum"],
            }
            if pick_style in REGIME_BLOCKS.get(early_regime, []):
                pick["regime_blocked_conviction"] = True
                continue

            boost_capped = min(20, boost)
            pick["conviction_signals"] = sigs
            pick["conviction_boost"]   = boost_capped
            pick["conviction_count"]   = len(sigs)
            pick["score"]              = min(100, pick["score"] + boost_capped)
            conviction.append(pick)

    MOMENTUM_SIGS = {"📡 X Signal Source", "📈 Score Trending Up", "🚨 Score Breakout", "💪"}
    for pick in conviction:
        sigs = pick.get("conviction_signals", [])
        mom_count = sum(1 for s in sigs if any(s.startswith(ms) for ms in MOMENTUM_SIGS))
        if mom_count >= 3:
            penalty = 8 * (mom_count - 2)
            pick["score"]        = max(0, pick["score"] - penalty)
            pick["corr_penalty"] = penalty
            pick.setdefault("flags", []).append(f"⚠️ Momentum echo -{penalty}pts ({mom_count} correlated signals)")

    EV_BINS = {
        (90, 100): (0.392, 0.80, 0.85),
        (75,  89): (0.441, 0.90, 0.80),
        (60,  74): (0.706, 2.50, 0.90),
        ( 0,  59): (0.500, 1.10, 1.00),
    }
    def score_to_ev(score):
        for (lo, hi), (prob, aw, al) in EV_BINS.items():
            if lo <= score <= hi:
                return prob * aw - (1 - prob) * al
        return 0.0

    for pick in conviction:
        pick["expected_value"] = round(score_to_ev(pick.get("score", 0)), 3)

    conviction.sort(key=lambda x: (x.get("expected_value", 0), x.get("conviction_count", 0)), reverse=True)
    return conviction


def build_calendar(screener_results, news_analysis):
    calendar   = []
    seen       = set()
    active_sig = news_analysis.get("active_signals", {})

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


def load_brief_history(n=5):
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
    for i in range(4, 0, -1):
        src = f"brief_history_{i}.json"
        dst = f"brief_history_{i+1}.json"
        if os.path.exists(src):
            os.rename(src, dst)
    with open("brief_history_1.json", "w") as f:
        json.dump(brief, f, default=str)



# ============================================================
# OBSIDIAN DAILY BRIDGE
# ============================================================

def write_obsidian_daily(brief, unified_regime, macro_regime,
                          rolling_sharpe, risk_multiplier,
                          neg_alpha_days, start):
    """
    Writes a daily markdown note to the investos-brain Obsidian vault.

    On Mac (local run):
      → ~/Documents/investos-brain/ exists → writes daily/YYYY-MM-DD.md
      → Obsidian Git pulls it within 10 min

    In GitHub Actions (cloud run):
      → vault doesn't exist → writes history/obsidian/YYYY-MM-DD.md
      → committed alongside normal data files
      → never breaks the pipeline (silent fail)
    """
    import pathlib

    try:
        today       = start.strftime("%Y-%m-%d")
        vault_root  = pathlib.Path.home() / "Documents" / "investos-brain"
        vault_daily = vault_root / "daily"

        if vault_root.exists():
            vault_daily.mkdir(parents=True, exist_ok=True)
            note_path = vault_daily / f"{today}.md"
        else:
            fallback = pathlib.Path("history") / "obsidian"
            fallback.mkdir(parents=True, exist_ok=True)
            note_path = fallback / f"{today}.md"

        # ── Pull metrics from brief ───────────────────────────
        wr         = brief.get("win_rate", {}) or {}
        wr30       = (wr.get("windows", {}).get("30d", {}) or {}).get("win_rate", 0) or 0
        wr_all     = wr.get("win_rate", 0) or 0
        streak     = wr.get("streak", 0)
        stype      = wr.get("streak_type", "WIN")

        br         = brief.get("breadth", {}) or {}
        p50        = br.get("pct_above_50", 0) or 0
        p200       = br.get("pct_above_200", 0) or 0
        b_signal   = br.get("signal", "UNKNOWN")

        risk_rep   = brief.get("risk_report", {}) or {}
        robustness = risk_rep.get("robustness_score", "N/A")

        pcr_data   = brief.get("market_pcr", {}) or {}
        pcr_val    = pcr_data.get("pcr", "N/A")
        pcr_signal = pcr_data.get("signal", "NEUTRAL")

        picks      = brief.get("conviction_picks", []) or []
        elapsed    = round((datetime.now() - start).total_seconds(), 1)

        # ── Status icons ─────────────────────────────────────
        regime_icon = {"RISK_ON": "🟢", "NEUTRAL": "🟡",
                       "DEFENSIVE": "🟠", "CAPITAL_PRESERVATION": "🔴"}.get(unified_regime, "⚪")
        macro_icon  = {"RISK_ON": "🟢", "CAUTIOUS": "🟡",
                       "RISK_OFF": "🔴", "NORMAL": "⚪"}.get(macro_regime, "⚪")
        sharpe_icon = "✅" if rolling_sharpe >= 0.5 else "⚠️" if rolling_sharpe >= 0 else "🔴"
        wr30_icon   = "✅" if wr30 >= 60 else "⚠️" if wr30 >= 50 else "🔴"
        b_icon      = "✅" if p200 >= 70 else "⚠️" if p200 >= 55 else "🔴"
        streak_icon = "✅" if stype == "WIN" else "⚠️"

        # ── Build picks section ───────────────────────────────
        picks_lines = []
        for i, p in enumerate(picks[:5], 1):
            ticker  = p.get("ticker", "?")
            score   = p.get("score", 0)
            signals = p.get("signals", [])
            sig_str = " · ".join(signals[:3]) if signals else "—"
            conv    = " 🎯 CONVICTION" if p.get("conviction") else ""
            picks_lines.append(f"{i}. **{ticker}** — score {score}{conv}  \n   {sig_str}")
        picks_md = "\n".join(picks_lines) if picks_lines else "_No picks today_"

        # ── Build flags section ───────────────────────────────
        flags = []
        if rolling_sharpe < 0:
            flags.append(f"🔴 Sharpe negative ({rolling_sharpe:.3f}) — guard engaged")
        if neg_alpha_days > 20:
            flags.append(f"⚠️ Neg alpha streak: {neg_alpha_days} days")
        if risk_multiplier < 1.0:
            flags.append(f"⚠️ Risk multiplier: {risk_multiplier:.2f}× (reduced exposure)")
        if p200 < 55:
            flags.append(f"⚠️ Breadth weak: {p200}% above 200MA")
        if not flags:
            flags.append("✅ No flags — system healthy")
        flags_md = "\n".join(f"- {f}" for f in flags)

        # ── Write note ────────────────────────────────────────
        note = f"""# {today}

## Regime
| Signal | Value | Status |
|--------|-------|--------|
| Unified | {unified_regime} @ {risk_multiplier:.2f}× | {regime_icon} |
| Macro | {macro_regime} | {macro_icon} |
| PCR | {pcr_val} | {pcr_signal} |
| Breadth 50MA | {p50}% | — |
| Breadth 200MA | {p200}% | {b_icon} {b_signal} |

## System Health
| Metric | Value | Status |
|--------|-------|--------|
| Sharpe (90d) | {rolling_sharpe:.3f} | {sharpe_icon} |
| WR 30d | {wr30}% | {wr30_icon} |
| WR overall | {wr_all}% | — |
| Streak | {streak} {stype} | {streak_icon} |
| Neg alpha | {neg_alpha_days} days | — |
| Robustness | {robustness}/100 | — |
| Runtime | {elapsed}s | — |

## Top Picks
{picks_md}

## Flags
{flags_md}

## Notes
_Add notes here_

---
_Generated by InvestOS at {datetime.now().strftime("%H:%M ET")} · NFA · Educational only_
"""
        note_path.write_text(note, encoding="utf-8")
        print(f"  📓 Obsidian note → {note_path}")

    except Exception as _oe:
        print(f"  ⚠️  Obsidian bridge skipped: {_oe}")


# ============================================================
# MAIN RUN
# ============================================================

def run_daily(test_mode=False, dry_run=False):
    start = datetime.now()
    sep   = "="*60

    print(f"\n{sep}")
    print(f"  INVESTOS — DAILY RUN v4.0")
    print(f"  {start.strftime('%B %d, %Y at %I:%M %p')}")
    print(f"  Layers: News · Regime · Screen · ML · RS · X · Conviction · FX · Crypto · Risk · Content")
    print(f"{sep}")

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

    # ── 2. Market Regime ─────────────────────────────────────
    print(f"\n[2/10] 📊 MARKET REGIME FILTER")
    regime = get_market_regime(verbose=True)
    # Sentinel: overwritten by 3-layer engine at step [11/12].
    # Ensures log_picks() always receives a string even if the risk block fails early.
    unified_regime = "NEUTRAL"

    # ── 2b. Strategy Engine ─────────────────────────────────
    # early_regime isn't computed until step 4.5 (after news + market combine)
    # Use market regime + macro regime directly here — same inputs, no dependency
    try:
        from strategy_engine import get_strategy as _gs, log_strategy as _ls
        _macro_r  = news.get("macro_regime") if news else None
        # Derive preliminary unified regime from market signal + macro
        _mkt_sig  = (regime or {}).get("signal", "NEUTRAL")
        if _mkt_sig == "FULL_EXPOSURE" and _macro_r not in ("RISK_OFF", "BEAR"):
            _unified = "RISK_ON"
        elif _macro_r in ("RISK_OFF", "BEAR"):
            _unified = "DEFENSIVE"
        elif _macro_r in ("CAUTIOUS",):
            _unified = "CAUTIOUS"
        else:
            _unified = "CAUTIOUS"
        strategy_name, strategy_profile = _gs(unified_regime=_unified, macro_regime=_macro_r)
        _ls(strategy_name, strategy_profile, verbose=True)
    except Exception as _se:
        print(f"  ⚠️  Strategy engine error: {_se} — using default weights")
        strategy_name, strategy_profile = "RISK_ON", None

    # ── 3. Stock Screen ──────────────────────────────────────────
    print(f"\n[3/10] 🔍 STOCK SCREEN (500+ universe)")
    try:
        screener = run_full_screen(max_tickers=30 if test_mode else None, verbose=True, strategy_profile=strategy_profile)
    except Exception as _scr_err:
        import traceback as _tb
        print(f"\n⚠️  SCREENER ERROR: {_scr_err}")
        _tb.print_exc()
        screener = {"FHSA_top5":[], "TFSA_growth_top5":[], "TFSA_income_top5":[],
                    "TFSA_swing_top3":[], "FHSA_all":[], "TFSA_core_all":[],
                    "TFSA_income_all":[], "TFSA_swing_all":[], "breadth":None, "stats":{}}

    # ── Fetch-failure tracking — swap out after 3 consecutive failures ─────────
    try:
        from scout_agent import update_failure_log as _ufl, get_inactive_tickers as _git
        _all_screened = set()
        for _bkt in ["FHSA_all", "TFSA_core_all", "TFSA_income_all", "TFSA_swing_all"]:
            for _p in screener.get(_bkt, []):
                if isinstance(_p, dict) and _p.get("ticker"):
                    _all_screened.add(_p["ticker"])
        _failed_tickers = [t for t in screener.get("failed_tickers", []) if isinstance(t, str)]
        _inactive = _ufl(list(_all_screened), _failed_tickers)
        if _inactive:
            print(f"  🚫 Fetch-failure watch: {_inactive} → marked inactive (3 consecutive failures)")
    except Exception as _ffe:
        print(f"  ⚠️  Fetch-failure log update failed: {_ffe}")

    # ── all_scores.json — full scored universe snapshot ─────────────────────
    # Union of all ranked bucket lists before news/insider/regime filters.
    # Deduplicated by ticker; highest score wins when a ticker appears in
    # multiple buckets. Consumed by VETT for full-universe coverage.
    try:
        import json as _asj
        _all_scored = {}
        for _bkt in ["FHSA_all", "TFSA_core_all", "TFSA_income_all", "TFSA_swing_all"]:
            for _p in screener.get(_bkt, []):
                _t  = _p.get("ticker")
                _sc = float(_p.get("score", 0) or 0)
                _sec = (_p.get("sector") or
                        (_p.get("data") or {}).get("sector", "") or "Unknown")
                if _t and (_t not in _all_scored or _sc > _all_scored[_t]["score"]):
                    if   _sc >= 90: _tier = "90-100"
                    elif _sc >= 75: _tier = "75-89"
                    elif _sc >= 60: _tier = "60-74"
                    else:           _tier = "below-60"
                    _all_scored[_t] = {"score": round(_sc, 1), "tier": _tier, "sector": _sec}
        _as_out = {"generated_at": datetime.now().isoformat(),
                   "universe_size": len(_all_scored)}
        _as_out.update(_all_scored)
        with open("all_scores.json", "w") as _asf:
            _asj.dump(_as_out, _asf, indent=2)
        print(f"  📋 all_scores.json: {len(_all_scored)} tickers saved")
    except Exception as _ase:
        print(f"  ⚠️  all_scores.json write failed: {_ase}")

    # ── 4. News Adjustment ───────────────────────────────────
    print(f"\n[4/10] 🔗 APPLYING NEWS TO SCORES")
    screener = apply_news_to_screener(screener, news)

    # ── 4b. Insider Engine ────────────────────────────────────
    _insider_ok = True
    try:
        all_picks = []
        for acct_picks in screener.values():
            if isinstance(acct_picks, list):
                all_picks.extend(acct_picks)
        updated_picks, insider_scores = run_insider_engine(all_picks, verbose=True)
        screener["insider_scores"] = insider_scores
    except Exception as _ie:
        _insider_ok = False
        print(f"  ⚠️  Insider engine error: {_ie} — continuing without")

    # ── 4c. Options Flow Engine ──────────────────────────────
    _options_ok = True
    options_signals = {}
    market_pcr_data = {"pcr": None, "signal": "NEUTRAL", "macro_adj": 0.0}
    try:
        us_picks_for_opts = [p for p in all_picks
                             if isinstance(p, dict) and not p.get("ticker", "").endswith(".TO")]
        _, options_signals, market_pcr_data = run_options_engine(
            us_picks_for_opts, verbose=True
        )
    except Exception as _oe:
        _options_ok = False
        print(f"  ⚠️  Options engine error: {_oe} — continuing without")

    # ── 4.5 Regime Authority Filter ──────────────────────────
    early_regime, category_blocks = compute_early_regime(macro_regime, regime)
    screener = apply_regime_filter(screener, early_regime, category_blocks)
    print(f"  🎯 Early regime: {early_regime} | Blocks: {category_blocks or 'none'}")

    # ── 5. ML Engine ─────────────────────────────────────────
    print(f"\n[5/10] 🤖 ML ENGINE (XGBoost + Position Sizing)")
    rs_for_ml = {}
    REGIME_MAX_EQUITY = {
        "RISK_ON": 1.0, "NEUTRAL": 0.75,
        "DEFENSIVE": 0.50, "CAPITAL_PRESERVATION": 0.25,
    }
    ml_max_equity = REGIME_MAX_EQUITY.get(early_regime, 1.0)
    # Weekly ML retrain on real outcomes (skips if <7 days since last)
    try:
        retrain_if_due()
    except Exception as _rte:
        print(f"  ⚠️  ML retrain skipped: {_rte}")

    # Load previous day's win_rate for Kelly position sizing calibration
    _wr_for_kelly = {}
    try:
        with open("latest_brief.json") as _wrf:
            _wr_for_kelly = json.load(_wrf).get("win_rate", {})
    except Exception:
        pass  # First run or missing — Kelly uses embedded defaults
    try:
        ml_results = run_ml_engine(screener, rs_for_ml, verbose=True, max_equity=ml_max_equity,
                       sector_sentiment=news.get("sector_sentiment", {}),
                       win_rate_data=_wr_for_kelly)
    except Exception as _ml_err:
        import traceback as _tb
        print(f"\n⚠️  ML ENGINE CRASHED: {_ml_err}")
        _tb.print_exc()
        _all = (screener.get("FHSA_top5",[]) + screener.get("TFSA_growth_top5",[]) +
                screener.get("TFSA_income_top5",[]) + screener.get("TFSA_swing_top3",[]))
        for _p in _all:
            _p.setdefault("ml_prob",   0.5)
            _p.setdefault("ml_signal", "📊 NEUTRAL (ML error)")
        ml_results = {
            "picks": _all, "ml_trained": False, "feature_importance": {},
            "position_sizing": [], "backtest_summary": {}, "picks_scored": 0,
            "regime_signal": "NEUTRAL",
            "regime": {"regime":"BULL","signal":"FULL_EXPOSURE","cash_pct":0.0,
                       "spx_price":0,"ma200":0,"pct_diff":0,"full_exposure_pct":100},
        }

    # ── 6. Intelligence Layers ───────────────────────────────
    # ── EVIDENCE ENGINE — enrich picks with historical backing ──────────────
    try:
        from evidence_engine import enrich_picks_with_evidence, get_tier_evidence_summary
        _spx_ret = regime.get("pct_above_ma", 0) if regime else 0
        # Enrich ALL ml shortlist picks (not just conviction — those are usually empty)
        # sized_positions is the 5-pick basket that actually shows on dashboard
        _all_for_evidence = ml_results.get("sized_positions", [])
        if not _all_for_evidence:
            # Fallback: try all screener candidates that made it to ML step
            _all_for_evidence = (
                screener.get("TFSA_growth_top5", []) +
                screener.get("TFSA_income_top5", [])
            )[:10]
        if _all_for_evidence:
            enrich_picks_with_evidence(_all_for_evidence, unified_regime,
                                       spx_90d_return=_spx_ret, verbose=True)
        # Add tier evidence summary to brief for dashboard
        brief["evidence_summary"] = get_tier_evidence_summary()
    except Exception as _ee:
        pass  # non-fatal — evidence is additive, never blocking

    print(f"\n[6/10] 🧠 INTELLIGENCE LAYERS (RS + History + Analyst)")
    all_raw = [p["data"] for bucket in
               ["FHSA_all","TFSA_core_all","TFSA_income_all","TFSA_swing_all"]
               for p in screener.get(bucket,[]) if p.get("data")]
    top_flat = (screener.get("FHSA_top5",[]) + screener.get("TFSA_growth_top5",[]) +
                screener.get("TFSA_income_top5",[]) + screener.get("TFSA_swing_top3",[]))
    intel = run_all_intelligence_layers(all_raw, top_flat, verbose=True)

    # ── 6b. Post-intelligence sizing cap for exit watch tickers ─
    exit_watch_tickers = set(t.get("ticker","") for t in intel.get("trends",{}).get("trending_down",[]))
    if exit_watch_tickers and ml_results.get("position_sizing"):
        capped = []
        for sz in ml_results["position_sizing"]:
            if sz.get("ticker","") in exit_watch_tickers:
                orig_pct = sz.get("weight_pct", 0)
                if orig_pct > 10.0:
                    print(f"   ⚠️  Exit watch cap: {sz['ticker']} {orig_pct:.1f}% → 10.0%")
                    sz = dict(sz)
                    sz["weight_pct"]  = 10.0
                    sz["dollar_amt"]  = round(sz.get("dollar_amt",0) * (10.0/orig_pct), 2)
                    sz["exit_capped"] = True
            capped.append(sz)
        ml_results["position_sizing"] = capped

    # ── 6c. HIGH_IV options warning → cap at 10% max allocation ─
    # TGT flagged "size conservatively" but still got 20% — connect the warning to sizing
    high_iv_tickers = {t for t, sig in (options_signals or {}).items()
                       if isinstance(sig, dict) and sig.get("signal") == "HIGH_IV"}
    if high_iv_tickers and ml_results.get("position_sizing"):
        capped_iv = []
        for sz in ml_results["position_sizing"]:
            if sz.get("ticker","") in high_iv_tickers:
                orig_pct = sz.get("weight_pct", 0)
                if orig_pct > 10.0:
                    print(f"   ⚠️  HIGH_IV cap: {sz['ticker']} {orig_pct:.1f}% → 10.0% (elevated IV)")
                    sz = dict(sz)
                    sz["weight_pct"]  = 10.0
                    sz["dollar_amt"]  = round(sz.get("dollar_amt",0) * (10.0/orig_pct), 2)
                    sz["iv_capped"]   = True
            capped_iv.append(sz)
        ml_results["position_sizing"] = capped_iv

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
    trends = intel.get("trends", {})
    # Compute cooldown set here — tickers on cooldown are invisible to conviction AND sizing
    try:
        cooldown_set, cooldown_tiers = get_cooldown_set(verbose=True)
    except Exception as _e:
        print(f"  ⚠️  Cooldown computation error: {_e}")
        cooldown_set, cooldown_tiers = set(), {}

    # ── Loss-streak flags from outcome_tracker ────────────────────────────
    # outcome_tracker.resolve_outcomes() writes cooldown_flags.json when
    # it detects 2+ losses ≥1.5% in last 10 resolved picks for a ticker.
    # Merged here so loss-streak tickers are invisible to ML + conviction.
    try:
        import json as _jflags, datetime as _dttf
        _flags = _jflags.load(open("cooldown_flags.json"))
        _today_str = _dttf.date.today().isoformat()
        _loss_flagged = [t for t, d in _flags.items() if d.get("expires","") >= _today_str]
        for _ftk in _loss_flagged:
            cooldown_set.add(_ftk)
        if _loss_flagged:
            print(f"  🛑 Loss-streak cooldowns: {', '.join(_loss_flagged)}")
    except FileNotFoundError:
        pass
    except Exception:
        pass

    # ── Long-cooldown tickers from long_cooldowns.json ───────────────────
    # 90-day rolling block with auto-renew if rolling WR < 35% at expiry.
    # Replaces permanent_exclusions.json — no more indefinite blocks.
    # Auto-renew and write-back happen in get_cooldown_set() (ml_engine.py).
    try:
        import json as _jlc2, os as _olc2, datetime as _dtlc2
        if _olc2.path.exists("long_cooldowns.json"):
            _lc2_data  = _jlc2.load(open("long_cooldowns.json"))
            _today_lc2 = _dtlc2.date.today().isoformat()
            _lc2_active = {t: v for t, v in _lc2_data.items()
                           if _today_lc2 <= v.get("blocked_until", "")}
            for _ltk in _lc2_active:
                cooldown_set.add(_ltk)
            if _lc2_active:
                _lc2_parts = [f"{t} (until {v['blocked_until']})"
                              for t, v in _lc2_active.items()]
                print(f"  🔄 Long cooldowns ({len(_lc2_active)}): {', '.join(_lc2_parts)}")
                # ── Cooldown watch: momentum on blocked names for Oct 3 evaluation ──
                _rs_map = intel.get("rs_ratings", {})
                for _lc_t, _lc_v in sorted(_lc2_active.items()):
                    _rs_entry = _rs_map.get(_lc_t, {})
                    _rs_val   = int(_rs_entry.get("rs_rating", 0))
                    if _rs_val >= 90:
                        print(f"  👁 Cooldown watch: {_lc_t} RS={_rs_val} "
                              f"(blocked until {_lc_v['blocked_until']})")
    except Exception as _lc2_e:
        print(f"  ⚠️  long_cooldowns.json error: {_lc2_e}")

    # ── News hard-exclusion (penalty≥15 + HIGH/CRITICAL signal) ─────────────
    _news_hard_excl = screener.get("_news_hard_excluded", set())
    if isinstance(_news_hard_excl, set) and _news_hard_excl:
        cooldown_set.update(_news_hard_excl)
        print(f"  🚫 News hard-exclusions added to cooldown: {sorted(_news_hard_excl)}")

    print(f"  📋 Consolidated exclusion set: {len(cooldown_set)} tickers total")

    score_history_for_decay = intel.get("history", {})
    all_picks_for_decay = (
        screener.get("FHSA_top5", []) + screener.get("TFSA_growth_top5", []) +
        screener.get("TFSA_income_top5", []) + screener.get("TFSA_swing_top3", [])
    )
    apply_score_decay(all_picks_for_decay, score_history_for_decay)

    try:
        conviction = build_conviction_picks(screener, x_signals, trends, news, ml_results, early_regime=early_regime, options_signals=options_signals, cooldown_set=cooldown_set)
    except Exception as _e:
        print(f"  ⚠️  Conviction engine error: {_e}")
        conviction = []

    def _pearson(a, b):
        n = min(len(a), len(b))
        if n < 5: return 0.0
        a, b = a[-n:], b[-n:]
        ma = sum(a)/n; mb = sum(b)/n
        num = sum((a[i]-ma)*(b[i]-mb) for i in range(n))
        da  = (sum((a[i]-ma)**2 for i in range(n)))**0.5
        db  = (sum((b[i]-mb)**2 for i in range(n)))**0.5
        return num/(da*db) if da*db > 0 else 0.0

    # ── Correlation filter — sector + return proximity ───────────────────────
    # closes_30d is rarely in pick data so Pearson on price series never fires.
    # Use sector + 30d return proximity instead:
    #   Same sector AND 30d returns within 3% = momentum echo, remove lower-scored.
    #   Also cap at 2 picks per sector to prevent concentration.
    SECTOR_CAP    = 2      # max conviction picks per sector
    RETURN_TOL    = 3.0    # % — within this 30d band in same sector = correlated
    sector_counts = {}
    kept = []; removed = []
    for pick in conviction:
        sector = (pick.get("sector","") or
                  pick.get("data",{}).get("sector","") or "Unknown")
        r30    = pick.get("data",{}).get("perf_30d", None)
        correlated = False
        # Sector cap — hard limit 2 per sector
        if sector_counts.get(sector, 0) >= SECTOR_CAP and sector not in ("","Unknown"):
            pick["corr_flag"] = "sector_cap"
            pick["corr_with"] = f"{sector} (>{SECTOR_CAP} picks)"
            removed.append(pick); correlated = True
        else:
            # Return proximity within same sector
            for k in kept:
                k_sector = (k.get("sector","") or
                            k.get("data",{}).get("sector","") or "Unknown")
                k_r30    = k.get("data",{}).get("perf_30d", None)
                if (sector == k_sector and sector not in ("","Unknown") and
                        r30 is not None and k_r30 is not None and
                        abs(r30 - k_r30) < RETURN_TOL):
                    correlated = True
                    pick["corr_flag"] = round(abs(r30 - k_r30), 1)
                    pick["corr_with"] = k["ticker"]
                    removed.append(pick); break
        if not correlated:
            kept.append(pick)
            sector_counts[sector] = sector_counts.get(sector, 0) + 1
    conviction = kept
    if removed:
        print(f"  🔗 Correlation filter: {len(removed)} redundant picks removed "
              f"{[p['ticker']+'≈'+p.get('corr_with','?') for p in removed]}")

    all_recent_scores = []
    for ticker_hist, records in score_history_for_decay.items():
        sorted_recs = sorted(records, key=lambda x: x["date"], reverse=True)
        for r in sorted_recs[:30]:
            all_recent_scores.append(r.get("score", 0))

    if len(all_recent_scores) >= 20:
        all_recent_scores.sort()
        p70_idx           = int(len(all_recent_scores) * 0.70)
        p70_score         = all_recent_scores[p70_idx]
        density_threshold = min(p70_score, 80)
        pre_density       = len(conviction)
        conviction        = [p for p in conviction if p.get("score", 0) >= density_threshold]
        if len(conviction) < pre_density:
            print(f"  📊 Quality filter: {pre_density} → {len(conviction)} picks "
                  f"(threshold: {density_threshold:.0f} = 70th percentile)")

    print(f"  High-conviction picks (2+ signals): {len(conviction)}")
    for p in conviction[:3]:
        print(f"  {p['ticker']:<12} Score:{p['score']}  Signals:{p['conviction_count']}  ML:{p.get('ml_prob',0.5):.0%}")

    # ── 9. FX & Gold ─────────────────────────────────────────
    print(f"\n[9/12] 💱 FX & GOLD SIGNALS")
    try:
        fx_signals = run_fx_engine(news_analysis=news, verbose=True)
    except Exception as _e:
        print(f"  ⚠️  FX engine error: {_e}")
        fx_signals = {}
    fx_signals, stale_pairs = check_fx_staleness(fx_signals)
    if stale_pairs:
        print(f"  ⚠️  Stale FX pairs: {', '.join(stale_pairs)}")
    with open("fx_signals.json","w") as f:
        json.dump(fx_signals, f, indent=2, default=str)

    # ── 10. Crypto Signals ───────────────────────────────────
    print(f"\n[10/12] 🪙 CRYPTO SIGNALS (BTC + SOL)")
    tfsa_bal = CONFIG["accounts"]["TFSA"]["balance"]
    crypto_signals = run_crypto_engine(news_analysis=news, portfolio_value=10000, verbose=True)  # hardcoded same as ML engine
    with open("crypto_signals.json","w") as f:
        json.dump(crypto_signals, f, indent=2, default=str)

    # ── 11. Risk Audit ───────────────────────────────────────
    print(f"\n[11/12] 🛡  RISK AUDIT (Stress Test + Decay Monitor)")
    score_history = intel.get("history", {})
    breadth       = screener.get("breadth")

    if breadth:
        sig_icon = {"BROAD_BULL":"🟢","MODERATE":"🟡","NARROW":"🟠","BEAR_BREADTH":"🔴"}.get(breadth["signal"],"📊")
        print(f"  {sig_icon} Breadth: {breadth['pct_above_50']}% above 50MA | "
              f"{breadth['pct_above_200']}% above 200MA | {breadth['signal']}")

    # Pre-read win rate from existing outcomes (before new picks are logged)
    _wr_pre = {}
    try:
        from outcome_tracker import compute_win_rate
        _wr_pre = compute_win_rate() or {}
    except Exception:
        pass
    risk_report = run_risk_audit(
        screener_results=screener, score_history=score_history,
        fx_signals=fx_signals, verbose=True, win_rate_data=_wr_pre
    )
    with open("risk_report.json","w") as f:
        json.dump(risk_report, f, indent=2, default=str)

    # ── Unified 3-Layer Regime Engine ────────────────────────
    market_reg = regime.get("regime", "UNKNOWN")
    if market_reg == "BULL":       market_score = 1.0
    elif market_reg == "RECOVERY": market_score = 0.3
    elif market_reg == "CAUTION":  market_score = -0.3
    elif market_reg == "BEAR":     market_score = -1.0
    else:                          market_score = 0.0

    macro_reg       = news.get("macro_regime", "NORMAL")
    news_signals    = news.get("active_signals", {})
    high_risk_count = 0
    if isinstance(news_signals, dict):
        high_risk_count = sum(1 for s in news_signals.values()
                              if isinstance(s, dict) and s.get("level","").upper() == "HIGH")
    elif isinstance(news_signals, list):
        high_risk_count = sum(1 for s in news_signals
                              if isinstance(s, dict) and s.get("level","").upper() == "HIGH")
    sigs_detected = news.get("signals_detected", 0) or 0
    if sigs_detected >= 5: high_risk_count = max(high_risk_count, 3)

    if macro_reg in ("BULL","RISK_ON","NORMAL"):
        macro_score = 0.5 if high_risk_count == 0 else 0.0
    elif macro_reg == "CAUTIOUS":
        macro_score = -0.3
    elif macro_reg in ("RISK_OFF","BEAR"):
        macro_score = -1.0
    else:
        macro_score = 0.0

    if macro_reg in ("RISK_OFF","BEAR") and market_reg == "BULL":
        macro_score = max(macro_score, -0.3)

    rolling_sharpe = risk_report.get("decay_monitor", {}).get("rolling_sharpe", {}).get("sharpe", 0) or 0
    neg_alpha_days = risk_report.get("decay_monitor", {}).get("neg_alpha_streak", 0) or 0
    robustness     = risk_report.get("robustness_score", 50) or 50

    if rolling_sharpe >= 0.5 and neg_alpha_days < 30:    health_score = 1.0
    elif rolling_sharpe >= 0.0 and neg_alpha_days < 60:  health_score = 0.0
    elif rolling_sharpe >= -1.0:                          health_score = -0.5
    else:                                                 health_score = -1.0

    unified_score = (0.40 * market_score + 0.30 * macro_score + 0.30 * health_score)

    scores_list   = [market_score, macro_score, health_score]
    same_sign     = all(s >= 0 for s in scores_list) or all(s <= 0 for s in scores_list)
    regime_confidence = min(1.0, round(abs(unified_score) * (1.5 if same_sign else 0.7), 2))

    if unified_score >= 0.5:
        unified_regime  = "RISK_ON";             system_exposure = 1.0
        allowed_styles  = ["breakout","momentum","growth","swing"]; blocked_styles = []
    elif unified_score >= 0.1:
        unified_regime  = "NEUTRAL";             system_exposure = 0.75
        allowed_styles  = ["momentum","value","dividend","defensive"]
        blocked_styles  = ["high_beta","speculative"]
    elif unified_score >= -0.2:
        unified_regime  = "DEFENSIVE";           system_exposure = 0.50
        allowed_styles  = ["defensive","dividend","mean_reversion"]
        blocked_styles  = ["breakout","high_beta","swing"]
    else:
        unified_regime  = "CAPITAL_PRESERVATION"; system_exposure = 0.25
        allowed_styles  = ["dividend","floor"]
        blocked_styles  = ["breakout","momentum","high_beta","swing","speculative"]

    if rolling_sharpe < -1.0 or neg_alpha_days > 60:
        system_exposure = min(system_exposure, 0.30)
        if unified_regime not in ("CAPITAL_PRESERVATION",):
            unified_regime = "DEFENSIVE"
    if high_risk_count >= 3:
        system_exposure = min(system_exposure, 0.50)

    # NOTE: `system_exposure` computed below is informational only (stored in the
    # brief for display and does not gate `allowed_styles`/`blocked_styles`, which
    # were already fixed above). It is NOT passed into ml_engine.py and never
    # scales any dollar or percentage sizing figure — position sizing already
    # finished in Step 5, several steps before this block runs. See the "SIZING
    # STACK" log printed during Step 5 (ML ENGINE) for what was actually applied;
    # see Phase 2 analysis for a proposal to wire this into real sizing.
    sharpe_guard_active = False
    if rolling_sharpe < 0.3 and rolling_sharpe >= 0.0:
        system_exposure = min(system_exposure, system_exposure * 0.6)
        sharpe_guard_active = True
        print(f"  📊 SHARPE ADVISORY (informational only — does not affect sizing):")
        print(f"     Rolling Sharpe {rolling_sharpe:.2f} is below the 0.3 caution threshold.")
        print(f"     This advisory is not currently wired into position sizing — actual sizing "
              f"was already finalized in Step 5 (ML Engine). See the SIZING STACK log there for "
              f"the % of capital actually deployed (regime_equity_pct × max_equity_cap × "
              f"drawdown_multiplier × effective_weight_pct). See Phase 2 analysis for a proposal "
              f"to wire this advisory into real sizing.")
    elif rolling_sharpe < 0.0:
        system_exposure = min(system_exposure, system_exposure * 0.4)
        sharpe_guard_active = True
        print(f"  📊 SHARPE ADVISORY (informational only — does not affect sizing):")
        print(f"     Rolling Sharpe {rolling_sharpe:.2f} is negative.")
        print(f"     This advisory is not currently wired into position sizing — actual sizing "
              f"was already finalized in Step 5 (ML Engine). See the SIZING STACK log there for "
              f"the % of capital actually deployed (regime_equity_pct × max_equity_cap × "
              f"drawdown_multiplier × effective_weight_pct). See Phase 2 analysis for a proposal "
              f"to wire this advisory into real sizing.")

    breadth_pct = screener.get("breadth", {}).get("pct_above_200ma", 60)
    if rolling_sharpe < 0.0 and neg_alpha_days >= 30 and breadth_pct < 50:
        if "mean_reversion" not in allowed_styles: allowed_styles.append("mean_reversion")
        if "value" not in allowed_styles:          allowed_styles.append("value")
        print(f"  🔄 MEAN REVERSION TRIGGER: Sharpe {rolling_sharpe:.2f} "
              f"< 0 for {neg_alpha_days}d + breadth {breadth_pct:.0f}% < 50%")

    exposure_reason = (f"M:{market_score:+.1f} N:{macro_score:+.1f} H:{health_score:+.1f} "
                       f"→ {unified_score:+.2f} Sharpe:{rolling_sharpe:.2f}")

    print(f"  🎯 Unified regime: {unified_regime} ({system_exposure*100:.0f}%) | {exposure_reason}")
    print(f"     Allowed: {allowed_styles}")
    if blocked_styles:
        print(f"     Blocked: {blocked_styles}")

    # ── PCR vs Price Regime conflict detection ────────────────────────────────
    # When options market positioning (PCR) disagrees with price-based regime,
    # surface explicitly — both inputs matter, system shouldn't silently pick one.
    _pcr_signal = market_pcr_data.get("signal", "NEUTRAL") if market_pcr_data else "NEUTRAL"
    _pcr_val    = market_pcr_data.get("pcr") if market_pcr_data else None
    _price_bull = unified_regime in ("RISK_ON",) and regime.get("regime") == "BULL"
    _pcr_bearish = _pcr_signal in ("BEARISH", "EXTREME_FEAR") if _pcr_signal else False
    _pcr_bullish = _pcr_signal in ("BULLISH", "GREED") if _pcr_signal else False

    if _price_bull and _pcr_bearish and _pcr_val:
        print(f"  ⚡ SIGNAL CONFLICT: Price regime BULL vs Options PCR {_pcr_val:.3f} {_pcr_signal}")
        print(f"     → Price structure says buy. Options market is hedging heavily.")
        print(f"     → Hold positions but reduce new entries by 25% until conflict resolves.")
    elif not _price_bull and _pcr_bullish and _pcr_val:
        print(f"  ⚡ SIGNAL CONFLICT: Regime cautious vs Options PCR {_pcr_val:.3f} {_pcr_signal}")
        print(f"     → Price structure weak. But options showing bullish positioning.")

    # ── Regime Convergence Detector ──────────────────────────────────────────
    # Fires on TREND DETERIORATION, not just point-in-time threshold breach.
    # Uses a 3-run rolling comparison via regime_state.json persistence.
    import json as _rj
    _STATE_FILE = "regime_state.json"
    _breadth_50 = screener.get("breadth", {}).get("pct_above_50", 100) if screener else 100
    _breadth_200 = screener.get("breadth", {}).get("pct_above_200", 100) if screener else 100
    _spx_pct    = regime.get("pct_above_ma", 100) if regime else 100
    _macro_caut = macro_regime in ("CAUTIOUS", "DEFENSIVE", "RISK_OFF") if macro_regime else False

    # Load rolling state (last 3 runs)
    _rstate = {}
    try:
        _rstate = _rj.load(open(_STATE_FILE))
    except Exception:
        pass
    _b50_hist   = _rstate.get("breadth_50_history", [])
    _b200_hist  = _rstate.get("breadth_200_history", [])
    _spx_hist   = _rstate.get("spx_cushion_history", [])
    _b50_hist.append(round(_breadth_50, 1))
    _b200_hist.append(round(_breadth_200, 1))
    _spx_hist.append(round(_spx_pct, 1))
    _b50_hist  = _b50_hist[-4:]   # keep last 4 runs
    _b200_hist = _b200_hist[-4:]
    _spx_hist  = _spx_hist[-4:]
    try:
        _rj.dump({"breadth_50_history": _b50_hist, "breadth_200_history": _b200_hist,
                  "spx_cushion_history": _spx_hist}, open(_STATE_FILE, "w"), indent=2)
    except Exception:
        pass

    # Detect deterioration: compare current to 3-run avg
    _caution_signals = []
    if len(_b50_hist) >= 3:
        _b50_avg = sum(_b50_hist[:-1]) / len(_b50_hist[:-1])
        _b50_drop = _b50_avg - _breadth_50
        if _b50_drop >= 5:
            _caution_signals.append(f"Breadth 50MA dropping: {_b50_avg:.1f}% → {_breadth_50:.1f}% ({-_b50_drop:.1f}pp trend)")
        elif _breadth_50 < 62:
            _caution_signals.append(f"Breadth {_breadth_50:.1f}% above 50MA (compressed)")
    else:
        if _breadth_50 < 65:
            _caution_signals.append(f"Breadth {_breadth_50:.1f}% above 50MA (compressed)")

    if len(_b200_hist) >= 3:
        _b200_avg = sum(_b200_hist[:-1]) / len(_b200_hist[:-1])
        _b200_drop = _b200_avg - _breadth_200
        if _b200_drop >= 3:
            _caution_signals.append(f"200MA breadth falling: {_b200_avg:.1f}% → {_breadth_200:.1f}% — regime reclassification risk")

    if len(_spx_hist) >= 3:
        _spx_avg = sum(_spx_hist[:-1]) / len(_spx_hist[:-1])
        _spx_drop = _spx_avg - _spx_pct
        if _spx_drop >= 1.0:
            _caution_signals.append(f"SPX cushion shrinking: {_spx_avg:.1f}% → {_spx_pct:.1f}% above 200MA")
        elif _spx_pct < 8.0:
            _caution_signals.append(f"SPX only +{_spx_pct:.1f}% above 200MA (thin cushion)")

    if _pcr_bearish:
        _caution_signals.append(f"Options PCR {_pcr_val:.3f} BEARISH")
    if _macro_caut:
        _caution_signals.append(f"Macro regime {macro_regime}")
    if unified_regime == "NEUTRAL":
        _caution_signals.append("Unified regime at NEUTRAL")

    # ── RISK MULTIPLIER — translates text advice into actual size adjustments ──
    # This is the sizing trust gate: convergence flags now produce a real number.
    # The multiplier is applied to all new position sizes in the brief.
    _convergence_fired = len(_caution_signals) >= 3
    _conflict_fired    = _price_bull and _pcr_bearish

    if unified_regime in ("CAPITAL_PRESERVATION", "DEFENSIVE"):
        _risk_multiplier = 0.25
    elif unified_regime == "NEUTRAL":
        _risk_multiplier = 0.50
    elif _convergence_fired and _conflict_fired:
        _risk_multiplier = 0.50   # both firing: more conservative
    elif _convergence_fired or _conflict_fired:
        _risk_multiplier = 0.75   # one firing: standard caution
    else:
        _risk_multiplier = 1.00   # clean: full deployment

    _cash_reserve = round((1 - _risk_multiplier) * 100, 0)

    if len(_caution_signals) >= 3:
        print(f"  🔶 REGIME CONVERGENCE: {len(_caution_signals)} independent layers compressing toward caution:")
        for _cs in _caution_signals:
            print(f"     • {_cs}")
        print(f"     → Reduce new position sizing. Watch 200MA breadth for regime reclassification.")

    if _risk_multiplier < 1.0:
        if unified_regime in ("CAPITAL_PRESERVATION", "DEFENSIVE"):
            _rm_reason = f"{unified_regime} regime"
        elif unified_regime == "NEUTRAL":
            _rm_reason = "NEUTRAL regime"
        elif _convergence_fired and _conflict_fired:
            _rm_reason = "convergence+PCR conflict"
        elif _convergence_fired:
            _rm_reason = "convergence"
        else:
            _rm_reason = "PCR conflict"
        print(f"  📐 RISK MULTIPLIER: {_risk_multiplier:.2f}× ({_rm_reason})")
        print(f"     → Positions sized at {_risk_multiplier*100:.0f}% of full allocation | {_cash_reserve:.0f}% held as cash")
    else:
        print(f"  ✅ RISK MULTIPLIER: 1.00× — full deployment, no caution flags")

    # ── 11c. Regime Shift Predictor ───────────────────────────────────────────
    regime_momentum_data = {}
    try:
        regime_momentum_data = predict_regime_shift(
            current_unified_regime=unified_regime, verbose=True
        )
        # Advisory only — small nudge to unified_score, never overrides regime directly
        momentum = regime_momentum_data.get("momentum","STABLE")
        confidence = regime_momentum_data.get("confidence", 0.0)
        if momentum == "ACCELERATING" and confidence > 0.6:
            unified_score = min(1.0, unified_score + 0.05)
        elif momentum == "DECELERATING" and confidence > 0.6:
            unified_score = max(-1.0, unified_score - 0.05)
    except Exception as _rpe:
        print(f"  ⚠️  Regime predictor error: {_rpe}")

    # ── 11b. ETF Signal Engine ────────────────────────────────
    print(f"\n[ETF] 📊 ETF SIGNAL ENGINE")
    etf_signals = {}
    try:
        etf_signals = run_etf_engine(
            sector_sentiment=news.get("sector_sentiment", {}),
            unified_regime=unified_regime,
            breadth=screener.get("breadth"),
            verbose=True,
        )
        with open("etf_signals.json", "w") as f:
            json.dump(etf_signals, f, indent=2, default=str)
    except Exception as _etfe:
        import traceback as _etb
        print(f"   ⚠️ ETF engine error: {_etfe}")
        _etb.print_exc()

    # ── 12. Content Engine ───────────────────────────────────
    print(f"\n[12/12] ✍️  SOCIAL CONTENT ENGINE")

    tfsa_bal  = CONFIG["accounts"]["TFSA"]["balance"]
    fhsa_bal  = CONFIG["accounts"]["FHSA"]["balance"]
    tfsa_proj = project_portfolio_growth(tfsa_bal, 300, 12, 20)
    fhsa_proj = project_portfolio_growth(fhsa_bal, 667, 8, 5)
    calendar  = build_calendar(screener, news)

    deploy_amt = tfsa_bal if tfsa_bal > 0 else 100
    deployment_plan = compute_deployment_plan(
        deploy_amount=deploy_amt, account_balance=tfsa_bal,
        top_picks=(screener.get("TFSA_growth_top5",[]) +
                   screener.get("TFSA_income_top5",[]) +
                   screener.get("TFSA_swing_top3",[])),
        fx_signals=fx_signals, crypto_signals=crypto_signals,
        regime=regime.get("regime","NORMAL"),
    )

    all_picks_flat = (screener.get("FHSA_top5",[]) + screener.get("TFSA_growth_top5",[]) +
                      screener.get("TFSA_income_top5",[]) + screener.get("TFSA_swing_top3",[]))
    score_hist_for_acc = intel.get("history", {})
    signal_accuracy = track_signal_accuracy(all_picks_flat, score_hist_for_acc)

    for pick in conviction[:5]:
        acc_sum  = signal_accuracy if signal_accuracy.get("resolved",0) > 0 else None
        guardrail = compute_position_size_guardrail(
            pick["ticker"], tfsa_bal,
            pick.get("pick",{}).get("category","GROWTH CORE"),
            acc_sum, regime.get("regime","NORMAL")
        )
        pick["size_guardrail"] = guardrail
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
        "system_exposure": {
            "pct":            system_exposure,
            "label":          unified_regime,
            "reason":         exposure_reason,
            "sharpe":         round(rolling_sharpe, 2),
            "unified_score":  round(unified_score, 3),
            "unified_regime": unified_regime,
            "confidence":     regime_confidence,
            "allowed_styles": allowed_styles,
            "blocked_styles": blocked_styles,
            "market_score":   round(market_score, 1),
            "macro_score":    round(macro_score, 1),
            "health_score":   round(health_score, 1),
            "high_risk_count": high_risk_count,
        },

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
            "regime":              ml_results.get("regime",{}),
            "position_sizing":     ml_results.get("target_weights",[]),
            "account_allocations": ml_results.get("account_allocations",{}),
            "backtest_summary":    ml_results.get("backtest_summary",{}),
            "feature_importance":  ml_results.get("feature_importance",{}),
            "regime_signal":       ml_results.get("regime_signal",""),
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
        "breadth":         screener.get("breadth"),
        "unified_regime":  unified_regime,    # logged to outcomes + shown in dashboard
        "macro_regime":    macro_reg,          # RISK_OFF/CAUTIOUS/NORMAL/RISK_ON
        "crypto":          crypto_signals,
        "deployment_plan": deployment_plan,
        "portfolio_scorecard": get_scorecard(),
        "open_trades":    [{"ticker":t["ticker"],"account":t["account"],
                            "action":t["action"],"price":t["price"],
                            "shares":t["shares"],"total_value":t["total_value"],
                            "stop_price":t["stop_price"],"target_price":t["target_price"],
                            "category":t["category"],"date":t["date"],"notes":t["notes"]}
                           for t in load_trades() if t.get("status") == "OPEN"],
        "etf_signals":        etf_signals,
        "options_signals":    options_signals,
        "market_pcr":         market_pcr_data,
        "regime_momentum":    regime_momentum_data,
        "risk_report":    {
            "stress_test":    risk_report.get("stress_test",{}),
            "decay_monitor":  risk_report.get("decay_monitor",{}),
            "drawdown_lock":  risk_report.get("drawdown_lock",{}),
            "stale_fx_pairs": stale_pairs,
            "robustness_score": risk_report.get("decay_monitor",{}).get("robustness_score", 60),
            "risk_multiplier":   _risk_multiplier if "_risk_multiplier" in dir() else 1.0,
            "cash_reserve_pct":  _cash_reserve if "_cash_reserve" in dir() else 0,
            "convergence_fired": _convergence_fired if "_convergence_fired" in dir() else False,
            "conflict_fired":    _conflict_fired if "_conflict_fired" in dir() else False,
        },
        "sized_positions": [
            {**p,
             "weight_pct_adj":  round(p.get("weight_pct", 0) * (_risk_multiplier if "_risk_multiplier" in dir() else 1.0), 1),
             "dollar_amt_adj":  round(p.get("dollar_amt", 0) * (_risk_multiplier if "_risk_multiplier" in dir() else 1.0), 0)}
            for p in (ml_results.get("sized_positions", []) if ml_results else [])
        ],
    }

    # ── GATE STATUS (baked so dashboard can display active filters) ──────
    try:
        import json as _jgs, os as _osg, datetime as _dtgs
        _lc_gs_data = {}
        if _osg.path.exists("long_cooldowns.json"):
            _lc_raw = _jgs.load(open("long_cooldowns.json"))
            _today_gs = _dtgs.date.today().isoformat()
            _lc_gs_data = {t: v.get("blocked_until","")
                           for t, v in _lc_raw.items()
                           if _today_gs <= v.get("blocked_until", "")}
        _gate_obj = ml_results.get("gate") if ml_results else None
        _gate_summary = _gate_obj.summary() if _gate_obj else {}
        brief["gate_status"] = {
            "sector_first_gate":       True,
            "sector_allow":            ["ENERGY", "BANKS", "FINANCIALS"],
            "sector_block":            ["MATERIALS", "TELECOM", "HEALTHCARE", "REIT", "CONSUMER"],
            "materials_75_block":      True,
            "ml_gate_threshold_pct":   round((_gate_summary.get("ml_gate_threshold") or 0) * 100, 1),
            **_gate_summary,
            "long_cooldowns":          _lc_gs_data,
            "long_cooldowns_count":    len(_lc_gs_data),
        }
    except Exception:
        brief["gate_status"] = None

    # ── OUTCOME TRACKING ─────────────────────────────────────
    try:
        from outcome_tracker import log_picks, resolve_outcomes, compute_win_rate, print_win_rate_report
        all_picks_to_log = (
            screener.get("FHSA_top5", []) +
            screener.get("TFSA_growth_top5", []) +
            screener.get("TFSA_swing_top3", [])
        )
        # Strip cooldown tickers and hard-excluded picks — they must not appear
        # in the outcome log (would skew WR and inflate logged-pick count).
        _log_cd = cooldown_set if isinstance(cooldown_set, set) else set()
        all_picks_to_log = [p for p in all_picks_to_log
                            if p.get("ticker") not in _log_cd
                            and not p.get("news_hard_exclude")]
        current_prices = {p["ticker"]: p.get("data",{}).get("price",0)
                         for p in all_picks_to_log if p.get("data",{}).get("price")}
        resolve_outcomes(current_prices)
        import hashlib as _hl, subprocess as _sp, time as _t
        _is_gh  = "--github" in sys.argv
        _ts_b   = str(int(_t.time())).encode()
        try:    _git_h = _sp.check_output(["git","rev-parse","--short","HEAD"],
                                          stderr=_sp.DEVNULL).decode().strip().encode()
        except: _git_h = b"unknown"
        _run_id = _hl.sha256(_ts_b + _git_h).hexdigest()[:8]
        # unified_regime:      set at lines 1017-1035 (3-layer regime engine output)
        # macro_regime:        news.get("macro_regime") — RISK_OFF/CAUTIOUS/NORMAL/RISK_ON
        # market_breadth_50ma: screener breadth dict, pct of universe above 50MA
        log_picks(all_picks_to_log, regime=regime, unified_regime=unified_regime,
                  macro_regime=macro_reg,
                  market_breadth_50ma=screener.get("breadth", {}).get("pct_above_50"),
                  run_type="scheduled" if _is_gh else "manual",
                  run_id=_run_id)

        # ── SIGNAL LEDGER (tamper-evident audit trail) ────────────
        try:
            from signal_ledger import append_signals, resolve_ledger
            news_sigs = news.get("signals", []) if news else []
            append_signals(all_picks_to_log, regime=regime, news_signals=news_sigs)
            resolve_ledger(current_prices)
        except Exception as _sle:
            print(f"   ⚠️  Signal ledger: {_sle}")
        win_rate = compute_win_rate()
        print_win_rate_report(win_rate)
        brief["win_rate"] = win_rate
    except Exception as e:
        print(f"   ⚠️  Outcome tracker error: {e}")
        brief["win_rate"] = None

    # ── DAILY SHORTLIST ───────────────────────────────────────
    try:
        all_conv = brief.get("conviction_picks", [])
        primary = None
        for p in all_conv[:5]:
            if p.get("score", 0) >= 70:
                d = p.get("data", {}); pk = p.get("pick", {})
                primary = {
                    "ticker": p["ticker"], "score": p.get("score",0),
                    "ml_prob": round(p.get("ml_prob",0.5)*100),
                    "signals": p.get("conviction_count",0),
                    "category": pk.get("category",""),
                    "exp_low": pk.get("exp_low",0), "exp_high": pk.get("exp_high",0),
                    "amount": pk.get("amount",0), "action": pk.get("action",""),
                    "reasons": p.get("reasons",[])[:3],
                }
                break

        backup = None
        for p in all_conv[1:6]:
            if primary and p["ticker"] == primary["ticker"]: continue
            if p.get("score",0) >= 60:
                pk = p.get("pick",{})
                backup = {
                    "ticker": p["ticker"], "score": p.get("score",0),
                    "ml_prob": round(p.get("ml_prob",0.5)*100),
                    "signals": p.get("conviction_count",0),
                    "category": pk.get("category",""),
                    "exp_low": pk.get("exp_low",0), "exp_high": pk.get("exp_high",0),
                    "amount": pk.get("amount",0),
                }
                break

        fx_play = None
        active_fx = [v for v in (fx_signals.get("pairs") or {}).values()
                     if v.get("conviction",0) >= 50 and v.get("direction") != "NEUTRAL"]
        active_fx.sort(key=lambda x: x.get("conviction",0), reverse=True)
        if active_fx:
            best = active_fx[0]
            fx_play = {"pair": best.get("pair",""), "direction": best.get("direction",""),
                       "conviction": best.get("conviction",0), "entry": best.get("entry",0),
                       "target": best.get("target",0), "stop": best.get("stop",0)}

        brief["shortlist"] = {"primary": primary, "backup": backup, "fx_play": fx_play}

        if primary:
            print(f"\n  🎯 TODAY'S PRIMARY PICK: {primary['ticker']} "
                  f"| Score {primary['score']} | ML {primary['ml_prob']}% | {primary['signals']} signals")
        if fx_play:
            print(f"  💱 FX PLAY: {fx_play['pair']} {fx_play['direction']} ({fx_play['conviction']}% conviction)")

    except Exception as e:
        print(f"   ⚠️  Shortlist error: {e}")
        brief["shortlist"] = None

    # Content generation
    brief_history = load_brief_history()
    content = run_content_engine(brief, brief_history=brief_history, verbose=True)
    brief["content"] = content

    # ── SAVE ALL FILES ────────────────────────────────────────
    with open("latest_brief.json","w") as f:
        json.dump(brief, f, indent=2, default=str)

    # ── DAILY HISTORY ARCHIVE ─────────────────────────────────
    try:
        os.makedirs("history", exist_ok=True)
        _today = datetime.now().strftime("%Y-%m-%d")
        _snap  = {
            "date":             _today,
            "regime":           brief.get("market_regime",{}),
            "sharpe":           brief.get("risk_report",{}).get("decay_monitor",{}).get("rolling_sharpe",None),
            "win_rate_30d":     (brief.get("win_rate",{}) or {}).get("windows",{}).get("30d",{}).get("win_rate",None),
            "win_rate_overall": brief.get("win_rate",{}).get("overall",None) if brief.get("win_rate") else None,
            "breadth":          brief.get("breadth",{}),
            "sector_sentiment": news.get("sector_sentiment",{}),
            "system_exposure":  brief.get("system_exposure",None),
            "conviction_picks": brief.get("conviction_picks",[]),
            "top_picks":        [{"ticker":p.get("ticker"),"score":p.get("score"),
                                  "category":p.get("category")}
                                 for p in (brief.get("TFSA_growth_top5",[]) +
                                           brief.get("FHSA_top5",[]))[:10]],
            "etf_top":          [{"ticker":e.get("ticker"),"score":e.get("score"),
                                  "ret_90":e.get("ret_90")}
                                 for e in brief.get("etf_signals",{}).get("scored",[])[:5]],
            "macro_signals":    [s.get("signal") for s in
                                 brief.get("macro",{}).get("signals",[])],
        }
        if not dry_run:
            with open(f"history/{_today}.json","w") as _f:
                json.dump(_snap, _f, indent=2, default=str)
            print(f"  📅 History snapshot saved: history/{_today}.json")
        else:
            print(f"  📅 [DRY-RUN] History snapshot skipped: history/{_today}.json")
    except Exception as _he:
        print(f"  ⚠️  History archive failed: {_he}")

    with open("news_analysis.json","w") as f:
        json.dump(news, f, indent=2, default=str)
    with open("content_output.json","w") as f:
        json.dump(content, f, indent=2, default=str)

    rotate_brief_history(brief)

    # ── FINAL SUMMARY ─────────────────────────────────────────
    elapsed    = round((datetime.now()-start).total_seconds(), 1)
    fx_calls   = fx_signals.get("total_signals", 0)
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
    print(f"  📡 X feeds:    {online_feeds}/{len(CONFIG['x_accounts'])} online")
    print(f"  📅 Calendar:   {len(calendar)} action items")

    if conviction:
        top = conviction[0]
        print(f"\n  🏆 #1 CONVICTION: {top['ticker']} — Score {top['score']}/100 | "
              f"{top['conviction_count']} signals | ML {top.get('ml_prob',0.5):.0%}")

    fx_top = fx_signals.get("top_call")
    if fx_top:
        icon = "🟢" if fx_top["direction"]=="LONG" else "🔴"
        print(f"  {icon} TOP FX:       {fx_top['pair']} {fx_top['direction']} ({fx_top['conviction']}% conviction)")

    print(f"\n  📝 TWEET READY:")
    print(f"  {content['tweet'][:120]}...")
    print(f"\n  💾 Files saved: latest_brief.json | fx_signals.json | content_output.json")
    print(f"  🖥  Open: index.html\n")

    # ── Run manifest — consumed by scripts/verify_run.py ─────────────────────
    try:
        _gate_obj_m  = ml_results.get("gate") if ml_results else None
        _gate_sum_m  = _gate_obj_m.summary() if _gate_obj_m else {}
        _manifest = {
            "run_date":             datetime.now().strftime("%Y-%m-%d"),
            "insider_ok":           _insider_ok,
            "options_ok":           _options_ok,
            "gate_status":          _gate_sum_m.get("ml_gate_status", "UNKNOWN"),
            "gate_decisions_count": len(_gate_sum_m),
            "unified_regime":       unified_regime,
            "substitution_tickers": ml_results.get("substitution_tickers", []) if ml_results else [],
            "pre_gate_excluded":    ml_results.get("pre_gate_excluded", []) if ml_results else [],
            "all_scores_count":     brief.get("screen_stats", {}).get("screened", 0),
        }
        with open("run_manifest.json", "w") as _mf:
            json.dump(_manifest, _mf, indent=2)
    except Exception as _me:
        print(f"  ⚠️  run_manifest.json write failed: {_me}")

    return brief


# ============================================================
# DASHBOARD BAKING
# ============================================================

MARKER_START = "// INVESTOS_DATA_START"
MARKER_END   = "// INVESTOS_DATA_END"

def bake_dashboard(brief, fx_signals, crypto_signals):
    import os, re
    dashboard_file = "index.html"

    cwd = os.getcwd()
    print(f"  📁 Working dir: {cwd}")
    html_files = [f for f in os.listdir(cwd) if f.endswith('.html')]
    print(f"  📄 HTML files found: {html_files}")

    if not os.path.exists(dashboard_file):
        print(f"  ❌ {dashboard_file} not found in {cwd}")
        return False

    print(f"  ✅ Found {dashboard_file} ({os.path.getsize(dashboard_file)//1024}KB)")

    try:
        slim_brief = {}
        keep_keys = [
            "date", "generated_at", "run_duration_sec",
            "macro", "market_regime", "system_exposure",
            "accounts", "conviction_picks", "fx_signals",
            "ml", "intelligence", "signals", "x_feeds_status", "calendar",
            "signal_accuracy", "screen_stats", "breadth", "crypto",
            "deployment_plan", "risk_report", "content", "win_rate", "shortlist",
            "etf_signals", "ngx",
            "gate_status",
            "FHSA_top5", "TFSA_growth_top5", "TFSA_income_top5", "TFSA_swing_top3",
            "screen_results",
            # NOTE: open_trades and portfolio_scorecard intentionally excluded
        ]
        for k in keep_keys:
            if k in brief:
                slim_brief[k] = brief[k]

        def _sanitize_floats(obj, _depth=0):
            """Recursively round all floats to 4dp to kill IEEE 754 artifacts.
            Also catches string-encoded floats (e.g. '57.599999999999994')
            that survive json.dumps(default=str) and bypass float isinstance check."""
            if _depth > 20: return obj
            if isinstance(obj, float):
                return round(obj, 4)
            if isinstance(obj, str):
                # Catch string-encoded floats with long decimal tails
                if len(obj) > 6 and obj.replace('.','',1).replace('-','',1).isdigit():
                    try:
                        f = float(obj)
                        rounded = round(f, 4)
                        # Only reformat if it was an artifact (too many decimal places)
                        if len(obj.split('.')[-1]) > 6:
                            return str(rounded) if '.' in obj else obj
                    except ValueError:
                        pass
                return obj
            if isinstance(obj, dict):
                return {k: _sanitize_floats(v, _depth+1) for k, v in obj.items()}
            if isinstance(obj, list):
                return [_sanitize_floats(i, _depth+1) for i in obj]
            return obj

        baked_payload = _sanitize_floats({
            "brief":    slim_brief,
            "fx":       fx_signals  or {},
            "crypto":   crypto_signals or {},
            "baked_at": datetime.now().isoformat(),
        })
        baked = json.dumps(baked_payload, default=str, ensure_ascii=True)
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

    if has_start and has_end:
        s    = html.index(MARKER_START) + len(MARKER_START)
        e    = html.index(MARKER_END)
        html = html[:s] + f"\nconst BAKED = {baked};\n" + html[e:]
        print(f"  ✅ Injected via direct BAKED assignment")
    else:
        pattern = r'const BAKED\s*=\s*[^;]+;'
        if re.search(pattern, html):
            html = re.sub(pattern, f"const BAKED = {baked};", html, count=1)
            print(f"  ✅ Injected via regex BAKED replace")
        elif "document.addEventListener('DOMContentLoaded'" in html:
            html = html.replace(
                "document.addEventListener('DOMContentLoaded'",
                f"const BAKED = {baked};\ndocument.addEventListener('DOMContentLoaded'", 1)
            print(f"  ✅ Injected before DOMContentLoaded")
        else:
            html = html.replace("</script>", f"const BAKED = {baked};\n</script>", 1)
            print(f"  ✅ Injected before </script>")

    try:
        with open(dashboard_file, "w", encoding="utf-8") as f:
            f.write(html)
        with open(dashboard_file, "r", encoding="utf-8") as f:
            verify = f.read()
        has_ngx = '"ngx"' in verify
        if 'const BAKED = {' in verify and '"brief"' in verify:
            ngx_note = ' ✅ NGX included' if has_ngx else ' ⚠️ NGX MISSING from bake'
            print(f"  ✅ Dashboard baked and VERIFIED ({len(html)//1024}KB){ngx_note}")
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
    gmail_user = os.environ.get("GMAIL_USER")
    gmail_pass = os.environ.get("GMAIL_PASSWORD")
    notify_to  = os.environ.get("NOTIFY_EMAIL")

    if not all([gmail_user, gmail_pass, notify_to]):
        print("  ⚠️  Email credentials not set — skipping brief email")
        print("      Set GMAIL_USER, GMAIL_PASSWORD, NOTIFY_EMAIL in GitHub Secrets")
        return False

    today       = datetime.now().strftime("%B %d, %Y")
    regime_data = brief.get("market_regime", {})
    regime      = regime_data.get("regime", "NORMAL")
    regime_icons= {"BULL":"✅","NORMAL":"📊","CAUTION":"⚠️","BEAR":"🔴","RISK_OFF":"🚨"}
    regime_icon = regime_icons.get(regime, "📊")
    rc          = {"BULL":"#00f5a0","NORMAL":"#ffc947","CAUTION":"#ffc947",
                   "BEAR":"#ff4d4d","RISK_OFF":"#ff4d4d"}.get(regime,"#ffc947")

    accounts    = brief.get("accounts", {})
    fhsa_picks  = accounts.get("FHSA",{}).get("top_picks",[])[:2]
    tfsa_income = accounts.get("TFSA",{}).get("income_picks",[])[:2]
    tfsa_growth = accounts.get("TFSA",{}).get("growth_picks",[])[:2]
    tfsa_swing  = accounts.get("TFSA",{}).get("swing_picks",[])[:1]
    fx_top      = fx_signals.get("top_call") if fx_signals else None
    btc         = crypto_signals.get("assets",{}).get("BTC-USD",{}) if crypto_signals else {}
    sol         = crypto_signals.get("assets",{}).get("SOL-USD",{}) if crypto_signals else {}
    screen      = brief.get("screen_stats",{})
    signal_acc  = brief.get("signal_accuracy",{})
    acc_7d      = signal_acc.get("accuracy_7d")
    regime_scale= {"BULL":"100%","NORMAL":"85%","CAUTION":"65%","BEAR":"40%"}.get(regime,"85%")

    repo = os.environ.get("GITHUB_REPOSITORY","your-username/investos")
    username = repo.split("/")[0] if "/" in repo else repo
    dashboard_url = f"https://{username}.github.io/investos"

    subject  = f"📊 InvestOS Brief — {today} — {regime_icon} {regime}"
    plain    = f"InvestOS Daily Brief — {today}\nRegime: {regime} | Dashboard: {dashboard_url}\nNFA · Educational only"

    # Build pick rows
    all_email_picks = fhsa_picks + tfsa_income + tfsa_growth + tfsa_swing
    html_picks_rows = ""
    for p in all_email_picks:
        t   = p.get("ticker","?"); sc = p.get("score",0)
        pk  = p.get("pick",{}); cat = pk.get("category","")
        act = (pk.get("action","") or "")[:65]
        tag = "FHSA" if "FHSA" in cat else "TFSA"
        sc_col = "#00f5a0" if sc>=75 else "#ffc947" if sc>=55 else "#ff4d4d"
        html_picks_rows += (f'<tr><td style="padding:9px 8px;border-bottom:1px solid #2a2a3e">'
            f'<span style="font-size:14px;font-weight:700;color:#fff;font-family:monospace">{t}</span>'
            f' <span style="font-size:9px;padding:1px 5px;background:#1a1a2e;border-radius:3px;color:#3d9bff">{tag}</span></td>'
            f'<td style="padding:9px 8px;border-bottom:1px solid #2a2a3e;text-align:center">'
            f'<span style="font-size:14px;font-weight:800;color:{sc_col};font-family:monospace">{sc}</span></td>'
            f'<td style="padding:9px 8px;border-bottom:1px solid #2a2a3e;font-size:11px;color:#aaa">{act}</td></tr>')

    # FX row
    fx_html = ("<p style='margin:0;font-size:13px;font-weight:700;color:"
               + ("#00f5a0" if fx_top and fx_top.get("direction")=="LONG" else "#ff4d4d") + "'>"
               + (f"{'🟢' if fx_top.get('direction')=='LONG' else '🔴'} {fx_top.get('pair','')} {fx_top.get('direction','')} — {fx_top.get('conviction',0)}% conviction</p>"
                  f"<p style='margin:5px 0 0;font-size:11px;color:#888'>Entry {fx_top.get('entry','')} · Stop {fx_top.get('stop','')} · Target {fx_top.get('target','')}</p>"
                  if fx_top else "<p style='font-size:12px;color:#888'>No FX call today</p>"))

    def cr(a, nm):
        if not a: return f"<tr><td colspan='3' style='padding:8px;color:#888;font-size:11px'>{nm}: No data</td></tr>"
        d = a.get("direction","NEUTRAL"); c = a.get("conviction",0); pr = a.get("price",0)
        col = "#00f5a0" if d=="LONG" else "#ff4d4d" if d=="SHORT" else "#888"
        ic  = "🟢" if d=="LONG" else "🔴" if d=="SHORT" else "⚪"
        return (f"<tr><td style='padding:8px;font-size:13px;font-weight:700;color:#fff'>{ic} {nm}</td>"
                f"<td style='padding:8px;font-size:13px;font-weight:700;color:{col}'>{d}</td>"
                f"<td style='padding:8px;font-size:12px;color:#aaa'>{c}% · ${pr:,.0f}</td></tr>")

    wr      = brief.get("win_rate",{}) or {}
    wr30    = (wr.get("windows",{}).get("30d",{}) or {}).get("win_rate", wr.get("win_rate_30d",0)) or 0
    wr_col  = "#00f5a0" if wr30 >= 60 else "#ffc947" if wr30 >= 50 else "#ff4d4d"
    streak  = wr.get("streak",0); stype = wr.get("streak_type","WIN")
    s_col   = "#00f5a0" if stype=="WIN" else "#ff4d4d"
    br      = brief.get("breadth",{}) or {}
    p200    = br.get("pct_above_200",0) or 0
    b_col   = "#00f5a0" if p200>=70 else "#ffc947" if p200>=55 else "#ff4d4d"
    acc_col = "#00f5a0" if acc_7d and acc_7d>=65 else "#ffc947" if acc_7d and acc_7d>=50 else "#ff4d4d"

    html_body = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#0d0d1a;font-family:-apple-system,BlinkMacSystemFont,sans-serif">
<div style="max-width:620px;margin:0 auto;background:#0d0d1a">

  <div style="background:linear-gradient(135deg,#1a1a2e,#12122a);padding:22px 26px;border-bottom:2px solid {rc}">
    <div style="font-size:10px;color:#888;letter-spacing:2px;margin-bottom:4px">INVESTOS DAILY BRIEF</div>
    <div style="font-size:20px;font-weight:800;color:#fff;margin-bottom:6px">{today}</div>
    <span style="display:inline-block;padding:3px 12px;background:{rc}22;border:1px solid {rc};
          border-radius:3px;font-size:11px;font-weight:700;color:{rc}">{regime_icon} {regime} · {regime_scale} DEPLOYED</span>
  </div>

  <div style="display:grid;grid-template-columns:1fr 1fr 1fr;border-bottom:1px solid #1a1a2e">
    <div style="padding:14px 16px;border-right:1px solid #1a1a2e">
      <div style="font-size:8px;color:#888;letter-spacing:1px;margin-bottom:4px">WIN RATE 30D</div>
      <div style="font-size:22px;font-weight:800;color:{wr_col};font-family:monospace">{wr30:.1f}%</div>
      <div style="font-size:9px;color:{s_col};margin-top:2px">{streak} {stype} streak</div>
    </div>
    <div style="padding:14px 16px;border-right:1px solid #1a1a2e">
      <div style="font-size:8px;color:#888;letter-spacing:1px;margin-bottom:4px">BREADTH 200MA</div>
      <div style="font-size:22px;font-weight:800;color:{b_col};font-family:monospace">{p200:.1f}%</div>
      <div style="font-size:9px;color:#888;margin-top:2px">{br.get("signal","").replace("_"," ")}</div>
    </div>
    <div style="padding:14px 16px">
      <div style="font-size:8px;color:#888;letter-spacing:1px;margin-bottom:4px">SCREENED</div>
      <div style="font-size:22px;font-weight:800;color:#3d9bff;font-family:monospace">{screen.get("screened",0)}/{screen.get("universe",0)}</div>
      <div style="font-size:9px;color:#888;margin-top:2px">{screen.get("signals_detected",0) if hasattr(screen,"get") else 0} signals active</div>
    </div>
  </div>

  <div style="padding:18px 26px 0">
    <div style="font-size:8px;color:#888;letter-spacing:2px;margin-bottom:10px">TODAY'S PICKS</div>
    <table style="width:100%;border-collapse:collapse">
      <thead><tr style="background:#1a1a2e">
        <th style="padding:7px 8px;text-align:left;font-size:8px;color:#888">TICKER</th>
        <th style="padding:7px 8px;text-align:center;font-size:8px;color:#888">SCORE</th>
        <th style="padding:7px 8px;text-align:left;font-size:8px;color:#888">ACTION</th>
      </tr></thead>
      <tbody>{html_picks_rows if html_picks_rows else "<tr><td colspan='3' style='padding:10px;color:#888;font-size:11px'>No picks today — staying patient</td></tr>"}</tbody>
    </table>
  </div>

  <div style="display:grid;grid-template-columns:1fr 1fr;padding:18px 26px 0;gap:12px">
    <div>
      <div style="font-size:8px;color:#888;letter-spacing:2px;margin-bottom:8px">FX TOP CALL</div>
      <div style="background:#1a1a2e;border-radius:4px;padding:12px 14px">{fx_html}</div>
    </div>
    <div>
      <div style="font-size:8px;color:#888;letter-spacing:2px;margin-bottom:8px">CRYPTO</div>
      <table style="width:100%;border-collapse:collapse;background:#1a1a2e;border-radius:4px">
        <tbody>{cr(btc,"BTC")}{cr(sol,"SOL")}</tbody>
      </table>
    </div>
  </div>

  <div style="padding:18px 26px 22px;margin-top:4px;text-align:center;border-top:1px solid #1a1a2e">
    <a href="{dashboard_url}" style="display:inline-block;padding:11px 26px;background:{rc};color:#000;
       font-weight:800;font-size:13px;letter-spacing:1px;border-radius:3px;text-decoration:none">
       OPEN FULL DASHBOARD →</a>
    <p style="margin:12px 0 0;font-size:9px;color:#444">Model suggestions only · Not financial advice · NFA</p>
  </div>

</div></body></html>"""

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
    github_mode  = "--github"       in sys.argv
    test_mode    = "--test"         in sys.argv
    dry_run      = "--dry-run"      in sys.argv   # skip git, email, history write
    json_metrics = "--json-metrics" in sys.argv   # print machine-readable metrics line

    if dry_run:
        print("  🔬 DRY-RUN MODE: git/email/history writes suppressed")

    brief = run_daily(test_mode=test_mode, dry_run=dry_run)

    if brief:
        print("\n  📊 Baking dashboard...")
        fx  = {}
        cry = {}
        try:
            with open("fx_signals.json")     as f: fx  = json.load(f)
        except: pass
        try:
            with open("crypto_signals.json") as f: cry = json.load(f)
        except: pass


        # ── NGX Engine ────────────────────────────────────────
        if HAS_NGX:
            try:
                print(f"\n[NGX] 🇳🇬 NIGERIAN EXCHANGE SIGNALS")
                _ngx_news = brief.get("regime_note", "NORMAL")
                _ngx_macro_str = (
                    "RISK_OFF" if _ngx_news in ("RISK_OFF","BEAR") else
                    "CAUTIOUS" if _ngx_news == "CAUTIOUS" else
                    "NORMAL"
                )
                ngx_result = run_ngx_engine(investos_macro=_ngx_macro_str, verbose=True)
                brief["ngx"] = ngx_result
                if HAS_NGX_TRACKER:
                    resolve_ngx_outcomes(ngx_result)
                    log_ngx_signals(ngx_result)
                    print_ngx_outcome_report()
                try:
                    with open("latest_brief.json","w") as _f:
                        json.dump(brief, _f, indent=2, default=str)
                except Exception: pass
            except Exception as _ngx_e:
                import traceback as _tb
                print(f"  ⚠️  NGX engine error: {_ngx_e}")
                _tb.print_exc()
                brief["ngx"] = {"error": str(_ngx_e), "picks": []}

        # ── Round score_history.json floats before baking ───────────────
        try:
            import json as _cj
            _sh = _cj.load(open("score_history.json"))
            for _recs in _sh.values():
                for _r in _recs:
                    if "score" in _r:
                        _r["score"] = round(float(_r["score"]), 1)
            _cj.dump(_sh, open("score_history.json","w"), indent=2)
        except Exception:
            pass
        bake_dashboard(brief, fx, cry)
        try:
            from signal_ledger import bake_audit_page
            bake_audit_page()
        except Exception as _sle2:
            pass   # non-critical

        if github_mode and not dry_run:
            print("  📧 Sending morning brief...")
            send_morning_brief(brief, fx, cry)
        elif dry_run:
            print("  📧 [DRY-RUN] Email suppressed")

        # ── Factor Attribution Report ──────────────────────────────
        try:
            import factor_investigation as _fi
            print()
            print("=" * 55)
            print("  FACTOR ATTRIBUTION REPORT")
            print("=" * 55)
            _fi.main()
        except Exception as _fe:
            print(f"  ⚠️  Factor report skipped: {_fe}")

        # ── PF Drift Monitor ───────────────────────────────────
        # Compares current tier PF against clean post-backlog baseline
        # (Jun 25 2026). Jun 22 baseline deprecated — was computed on
        # incomplete data with 339 stale unresolved picks hiding bad outcomes.
        # Alert threshold: >0.20 PF drop = investigate.
        try:
            import json as _pjson, os as _pos

            # Optionally load from pf_baseline.json if present (overrides hardcoded)
            PF_BASELINE = {
                "90-100":   0.91,   # post-backlog clean baseline Jun 25 2026
                "75-89":    1.07,
                "60-74":    1.92,
                "below-60": 1.09,
            }
            _baseline_file = "pf_baseline.json"
            if _pos.path.exists(_baseline_file):
                try:
                    with open(_baseline_file) as _bf:
                        _bdata = _pjson.load(_bf)
                    if "tiers" in _bdata:
                        PF_BASELINE = {t: v["pf"] for t, v in _bdata["tiers"].items()}
                except Exception:
                    pass  # fall back to hardcoded
            PF_ALERT_THRESHOLD = 0.20

            _log_path = "outcomes_log.json"
            if _pos.path.exists(_log_path):
                with open(_log_path) as _pf:
                    _picks = _pjson.load(_pf)

                _resolved = [e for e in _picks
                             if e.get("resolved") and e.get("outcome") in ("WIN","LOSS")]

                def _tier_pf(picks):
                    wins   = [e for e in picks if e["outcome"] == "WIN"]
                    losses = [e for e in picks if e["outcome"] == "LOSS"]
                    gw = sum(e.get("actual_return", 0) for e in wins)
                    gl = abs(sum(e.get("actual_return", 0) for e in losses))
                    return round(gw / gl, 2) if gl > 0 else 0.0

                _tier_map = {
                    "90-100":   [e for e in _resolved if e.get("score", 0) >= 90],
                    "75-89":    [e for e in _resolved if 75 <= e.get("score", 0) < 90],
                    "60-74":    [e for e in _resolved if 60 <= e.get("score", 0) < 75],
                    "below-60": [e for e in _resolved if e.get("score", 0) < 60],
                }
                _current_pf = {t: _tier_pf(p) for t, p in _tier_map.items() if p}

                print()
                print("─" * 55)
                print("  PF DRIFT vs BASELINE (Jun 25 2026)")
                print("─" * 55)
                for _tier, _base in PF_BASELINE.items():
                    _cur = _current_pf.get(_tier)
                    if _cur is None:
                        continue
                    _drift = _cur - _base
                    _adrift = abs(_drift)
                    if _adrift >= 0.20:
                        _icon = "🔴" if _drift < 0 else "✅"
                        _note = "— ALERT" if _drift < 0 else "— ALERT ↑"
                    elif _adrift >= 0.10:
                        _icon = "🟠" if _drift < 0 else "🟡"
                        _note = "— significant"
                    elif _adrift >= 0.05:
                        _icon = "🟡"
                        _note = "— drifting"
                    else:
                        _icon = "↔ "
                        _note = "— stable"
                    print(f"  {_icon} [{_tier:>8}]  cur={_cur:.2f}  base={_base:.2f}  "
                          f"drift={_drift:+.2f}  {_note}")
                print("─" * 55)
        except Exception as _dfe:
            print(f"  ⚠️  PF drift monitor skipped: {_dfe}")

        # ── Strategy version snapshot (OOS anchor) ──────────────────────────
        try:
            import strategy_version as _sv
            _sv.log_strategy_version(outcomes_path="outcomes_log.json")
        except Exception as _sve:
            print(f"  ⚠️ strategy_version log failed: {_sve}")

        # ── GLOBAL_WATCH scoring (foreign-listed quarantine) ──────────────────
        # Foreign exchange tickers scored for reference only — NEVER traded.
        # Strict containment: MUST NOT appear in picks, sizing, ledger, or any
        # main output (all_scores.json, latest_brief.json, outcomes_log.json).
        try:
            import json as _gwj
            from stock_screener import fetch_ticker_full as _gw_fetch, score_stock as _gw_score
            _gw_path = "global_watch.json"
            _gs_path = "global_scores.json"
            if os.path.exists(_gw_path):
                _gw_data = _gwj.load(open(_gw_path))
                _gw_scores = {}
                _gw_pass   = []
                _gw_fail   = []
                for _gw_ticker, _gw_meta in list(_gw_data.items()):
                    if _gw_meta.get("status") != "active":
                        continue
                    try:
                        _gw_tdata = _gw_fetch(_gw_ticker)
                        if _gw_tdata and _gw_tdata.get("status") == "ok":
                            _gw_s, _, _, _ = _gw_score(_gw_tdata)
                            _gw_scores[_gw_ticker] = {
                                "score":  round(float(_gw_s), 1),
                                "source": _gw_meta.get("source", "unknown"),
                            }
                            _gw_pass.append(_gw_ticker)
                        else:
                            _gw_fail.append(_gw_ticker)
                    except Exception:
                        _gw_fail.append(_gw_ticker)

                # Reset failures on success; increment on failure; prune at 3
                for _gw_t in _gw_pass:
                    if _gw_t in _gw_data:
                        _gw_data[_gw_t]["consecutive_failures"] = 0
                for _gw_t in _gw_fail:
                    if _gw_t in _gw_data:
                        _gw_data[_gw_t]["consecutive_failures"] = (
                            _gw_data[_gw_t].get("consecutive_failures", 0) + 1
                        )
                        if _gw_data[_gw_t]["consecutive_failures"] >= 3:
                            print(f"  🌏 Global watch: removed {_gw_t} (3 consecutive fetch failures)")
                            del _gw_data[_gw_t]

                # Persist updated failure counts
                with open(_gw_path, "w") as _gwf:
                    _gwj.dump(_gw_data, _gwf, indent=2)

                # Write scores to global_scores.json ONLY — never to main outputs
                with open(_gs_path, "w") as _gsf:
                    _gwj.dump(_gw_scores, _gsf, indent=2)

                # Display line
                _gw_active = [t for t, m in _gw_data.items() if m.get("status") == "active"]
                if _gw_scores:
                    _gw_top = max(_gw_scores.items(), key=lambda x: x[1]["score"])
                    print(f"\n  🌏 Global watch: {len(_gw_active)} tickers | "
                          f"top: {_gw_top[0]} {_gw_top[1]['score']}")
                else:
                    print(f"\n  🌏 Global watch: {len(_gw_active)} tickers | no scores available")
        except Exception as _gwe:
            print(f"  ⚠️  Global watch scoring failed: {_gwe}")

        # ── History Analyzer ─────────────────────────────────────────────
        try:
            from history_analyzer import run_history_analysis as _ha
            _ha(verbose=False)
        except Exception:
            pass

        # ── Strategist Agent ──────────────────────────────────────────────
        # Calls Claude API to write daily research note — silent fail
        try:
            if os.environ.get("ANTHROPIC_API_KEY"):
                from strategist_agent import run_strategist as _sa
                _sa(verbose=False)
                print("  🧠 Strategist note → history/obsidian/research/")
        except Exception:
            pass

        # ── Obsidian Daily Bridge ─────────────────────────────────────────
        # All values pulled from brief — no bare local variable deps
        try:
            _rr         = (brief or {}).get("risk_report", {}) or {}
            _dm         = _rr.get("decay_monitor", {}) or {}
            _rs_raw     = _dm.get("rolling_sharpe", 0)
            _obs_sharpe = float(_rs_raw.get("sharpe", 0) if isinstance(_rs_raw, dict) else _rs_raw or 0)
            _obs_rm     = float(_rr.get("risk_multiplier", 1.0) or 1.0)
            _obs_na     = int(_dm.get("neg_alpha_days", 0) or 0)
            _obs_ur     = (brief or {}).get("system_exposure", {}).get("unified_regime") or "DEFENSIVE"
            _obs_mr     = (brief or {}).get("macro", {}).get("regime", "NORMAL")
            # start is a local inside run_daily() — recover from brief["generated_at"]
            _gen_at     = (brief or {}).get("generated_at", "")
            _obs_start  = (datetime.fromisoformat(_gen_at) if _gen_at
                           else datetime.now())
            write_obsidian_daily(
                brief           = brief,
                unified_regime  = _obs_ur,
                macro_regime    = _obs_mr,
                rolling_sharpe  = _obs_sharpe,
                risk_multiplier = _obs_rm,
                neg_alpha_days  = _obs_na,
                start           = _obs_start,
            )
        except Exception as _obe:
            print(f"  ⚠️  Obsidian bridge failed: {_obe}")

        print("  ✅ InvestOS complete")

        if json_metrics and brief:
            import json as _jm
            _wr_data   = brief.get("win_rate", {}) or {}
            _win_30d   = (_wr_data.get("windows") or {}).get("30d", {}).get("win_rate", 0)
            _stress    = brief.get("risk_report", {}).get("stress_test", {})
            _top5_drop = _stress.get("remove_top5", {}).get("avg_score_drop", 0) if _stress else 0
            _decay     = brief.get("risk_report", {}).get("decay_monitor", {}) or {}
            _metrics   = {
                "win_rate_30d":      round(float(_win_30d or 0), 1),
                "rolling_sharpe":    round(float(_decay.get("rolling_sharpe", 0) or 0), 3),
                "stress_top5_drop":  round(float(_top5_drop or 0), 1),
                "conviction_picks":  len(brief.get("conviction_picks", []) or []),
                "error_count":       0,
                "ngx_win_rate":      0.0,
                "dry_run":           dry_run,
            }
            print(f"AGENT_METRICS_JSON={_jm.dumps(_metrics)}")
