"""
InvestOS — Outcome Tracker
==========================
Logs every pick with entry price AND feature snapshot at signal time.
Next run checks what happened — win/loss/magnitude.
Feeds back into ML model over time.

v2 fix: log_picks() now saves all ML features at signal time.
  Previously: only ticker, price, score, ml_prob were saved.
  Result: 1,483 resolved picks with AUC=0.500 (all features were 0.0).

  Now saves: perf_90d, volatility, roe, profit_margin, pe_ratio,
             rev_growth, earn_growth, div_yield, debt_equity, rs_rating,
             regime, spx_vs_ma200, news_boost.

  These are passed from pick['data'] + regime context at call time.
  Once ~300 picks accumulate with real features, XGBoost AUC > 0.50.
"""

import json
import os
from datetime import datetime, timedelta

OUTCOMES_FILE = "outcomes_log.json"
WIN_RATE_FILE = "win_rate.json"


def load_outcomes():
    if os.path.exists(OUTCOMES_FILE):
        try:
            with open(OUTCOMES_FILE) as f:
                data = json.load(f)
            # One-time cleanup: round float artifact scores (e.g. 60.400000000000006)
            _dirty = False
            for entry in data:
                raw = entry.get("score")
                if raw is not None:
                    clean = round(float(raw), 1)
                    if str(raw) != str(clean):
                        entry["score"] = clean
                        _dirty = True
            if _dirty:
                with open(OUTCOMES_FILE, "w") as f:
                    import json as _j
                    _j.dump(data, f, indent=2, default=str)
            return data
        except Exception:
            pass
    return []


def save_outcomes(outcomes):
    with open(OUTCOMES_FILE, "w") as f:
        json.dump(outcomes, f, indent=2, default=str)


def log_picks(picks, run_time=None, regime=None):
    """
    Log today's picks with entry price and full ML feature snapshot.

    v2: regime parameter added to capture market context at signal time.
    Pass regime=brief['market_regime'] from run_daily.py.

    Feature snapshot saved per pick:
      - All fundamentals from pick['data'] (perf_90d, roe, volatility etc.)
      - rs_rating from intelligence_layers
      - news_adjustment from news_analyzer
      - regime, spx_vs_ma200 from market regime at signal time

    These unlock ML training on real data instead of zeros.
    """
    if not picks:
        return

    outcomes = load_outcomes()
    now      = run_time or datetime.now().isoformat()
    date_str = datetime.now().strftime("%Y-%m-%d")

    # Don't double-log same picks on same date
    logged_today = {o["ticker"] for o in outcomes
                    if o.get("signal_date") == date_str and o.get("resolved") is False}

    # Extract regime context once (same for all picks this run)
    regime_str   = "BULL"
    spx_vs_ma200 = 0.0
    if regime:
        regime_str   = regime.get("regime", "BULL") or "BULL"
        spx_vs_ma200 = float(regime.get("pct_above_ma", 0) or 0)

    new_logged = 0
    for pick in picks:
        ticker = pick.get("ticker")
        if not ticker or ticker in logged_today:
            continue

        d = pick.get("data", {})

        # ── ML feature snapshot — captured at signal time ─────────────────
        # These are the exact fields ml_retrainer.py needs.
        # Previously all missing → all features were 0.0 → AUC=0.500.
        entry = {
            # ── Identity + outcome fields (unchanged) ─────────────────────
            "ticker":        ticker,
            "signal_date":   date_str,
            "signal_time":   now,
            "entry_price":   d.get("price", 0),
            "score":         round(float(pick.get("score", 0) or 0), 1),
            "ml_prob":       pick.get("ml_prob", 0.5),
            "category":      pick.get("pick", {}).get("category", ""),
            "exp_low":       pick.get("pick", {}).get("exp_low", 0),
            "exp_high":      pick.get("pick", {}).get("exp_high", 0),
            "resolved":      False,
            "exit_price":    None,
            "actual_return": None,
            "outcome":       None,
            "resolved_date": None,
            "outcome_90d":    None,
            "return_90d":     None,

            # ── ML features: momentum ──────────────────────────────────────
            "perf_90d":      d.get("perf_90d", 0) or 0,      # → momentum_6m
            "perf_30d":      d.get("perf_30d", 0) or 0,      # → skip-period check
            "volatility":    d.get("volatility", 2.0) or 2.0,# → vol_adj_momentum

            # ── ML features: quality / value ───────────────────────────────
            "roe":           d.get("roe", 0) or 0,
            "profit_margin": d.get("profit_margin", 0) or 0,
            "pe_ratio":      d.get("pe_ratio", 20) or 20,
            "rev_growth":    d.get("rev_growth", 0) or 0,
            "earn_growth":   d.get("earn_growth", 0) or 0,
            "div_yield":     d.get("div_yield", 0) or 0,
            "debt_equity":   d.get("debt_equity", 1) or 1,

            # ── ML features: relative strength ─────────────────────────────
            "rs_rating":     pick.get("rs_rating", 50) or 50,

            # ── ML features: news signal applied ───────────────────────────
            # news_adjustment = raw points added/removed (capped at ±8)
            "news_boost":    float(pick.get("news_adjustment", 0) or
                                   pick.get("news_original", 0) or 0),

            # ── ML features: market context at signal time ─────────────────
            "regime":        regime_str,
            "spx_vs_ma200":  spx_vs_ma200,

            # ── Extra context (not ML features but useful for analysis) ─────
            "rsi":           d.get("rsi_approx", 50) or 50,
            "above_ma200":   bool(d.get("above_ma200", True)),
            "above_ma50":    bool(d.get("above_ma50", True)),
            "sector":        d.get("sector", ""),

            # ── Factor attribution fields (for leaderboard analysis) ──────
            # These enable the hedge fund critique's factor isolation question:
            # "Which factor actually predicts returns?"
            "score_rank":    pick.get("score_rank", 0),      # rank in universe today
            "score_pct":     pick.get("score_pct", 50.0),    # top X% of universe
            "options_signal": pick.get("options_signal", ""), # HIGH_IV / BULLISH_PCR / etc
            "conviction":    pick.get("conviction", False),   # was this a 2+ signal pick
            "kelly_wt":      float(pick.get("kelly_wt", 0) or 0),  # kelly fraction at signal
        }
        outcomes.append(entry)
        new_logged += 1

    save_outcomes(outcomes)
    print(f"   📝 Outcome tracker: logged {new_logged} new picks ({len(outcomes)} total)")
    return new_logged


def resolve_outcomes(current_prices):
    """
    Check unresolved picks. Resolve WIN/LOSS after 7 calendar days (~5 trading days).
    """
    if not current_prices:
        return

    outcomes = load_outcomes()
    today    = datetime.now().date()
    resolved = 0

    for o in outcomes:
        if o.get("resolved"):
            continue

        signal_date = datetime.strptime(o["signal_date"], "%Y-%m-%d").date()
        days_passed = (today - signal_date).days

        if days_passed >= 7:
            ticker      = o["ticker"]
            entry_price = o.get("entry_price", 0)
            exit_price  = current_prices.get(ticker)

            if exit_price and entry_price and entry_price > 0:
                ret = (exit_price - entry_price) / entry_price * 100
                o["exit_price"]    = round(exit_price, 2)
                o["actual_return"] = round(ret, 2)
                o["resolved"]      = True
                o["resolved_date"] = today.isoformat()

                if ret > 0.3:
                    o["outcome"] = "WIN"
                elif ret < -0.3:
                    o["outcome"] = "LOSS"
                else:
                    o["outcome"] = "FLAT"

                resolved += 1

    save_outcomes(outcomes)
    if resolved:
        print(f"   ✅ Resolved {resolved} outcomes")

    # ── Loss-triggered cooldown ──────────────────────────────────────────
    # If a ticker has 2+ losses ≥1.5% within the last 5 resolved picks,
    # flag it for cooldown. run_daily.py reads cooldown_flags.json and
    # adds these to the cooldown set before ML engine runs.
    # This catches the GRT-UN.TO pattern: repeated losses, no circuit breaker.
    _flagged = _detect_loss_streak(outcomes)
    if _flagged:
        import json as _jcf
        _existing = {}
        try:
            _existing = _jcf.load(open("cooldown_flags.json"))
        except Exception:
            pass
        import datetime as _dtt
        _today = _dtt.date.today().isoformat()
        for _tk in _flagged:
            if _tk not in _existing or _existing[_tk].get("expires", "") < _today:
                _existing[_tk] = {
                    "reason":  "loss_streak",
                    "flagged": _today,
                    "expires": (
                        _dtt.date.today() + _dtt.timedelta(days=7)
                    ).isoformat(),
                }
                print(f"   🛑 Loss-streak cooldown: {_tk} (7 days)")
        _jcf.dump(_existing, open("cooldown_flags.json", "w"), indent=2)

    return resolved


def _detect_loss_streak(outcomes, window=10, min_losses=2, min_loss_pct=1.5):
    """
    Scan recent outcomes for tickers with loss streaks.
    Returns set of tickers to flag for cooldown.

    Rule: if a ticker has min_losses or more losses ≥min_loss_pct%
    within the last `window` resolved picks, flag it.
    This is structural — no cooldown currently fires on consecutive losses.
    """
    resolved = [o for o in outcomes if o.get("outcome") in ("WIN","LOSS","FLAT")]
    if not resolved:
        return set()

    # Sort by resolved_date descending, take last window picks
    resolved_sorted = sorted(
        resolved,
        key=lambda x: x.get("resolved_date", x.get("signal_date", "")),
        reverse=True
    )[:window * 4]  # look at 4x window to catch tickers that appear multiple times

    # Group by ticker, count recent losses
    from collections import defaultdict
    ticker_losses = defaultdict(list)
    ticker_all    = defaultdict(int)

    for o in resolved_sorted:
        tk  = o.get("ticker", "")
        ret = o.get("actual_return", 0) or 0
        if not tk:
            continue
        ticker_all[tk] += 1
        if o.get("outcome") == "LOSS" and abs(ret) >= min_loss_pct:
            ticker_losses[tk].append(ret)

    flagged = set()
    for tk, losses in ticker_losses.items():
        if len(losses) >= min_losses:
            flagged.add(tk)

    return flagged


def compute_time_weighted_win_rate(resolved):
    """Time-weighted win rate — recent picks count more."""
    import math

    if len(resolved) < 3:
        return None

    today  = datetime.now().date()
    LAMBDA = 0.02  # half-life ~35 days

    total_weight    = 0.0
    weighted_wins   = 0.0
    weighted_return = 0.0
    last_30 = []
    last_90 = []

    for o in resolved:
        try:
            resolved_date = datetime.strptime(o["resolved_date"], "%Y-%m-%d").date()
        except Exception:
            resolved_date = today

        days_ago = max(0, (today - resolved_date).days)
        weight   = math.exp(-LAMBDA * days_ago)
        is_win   = 1.0 if o["outcome"] == "WIN" else 0.0
        ret      = o.get("actual_return", 0) or 0

        total_weight    += weight
        weighted_wins   += weight * is_win
        weighted_return += weight * ret

        if days_ago <= 30: last_30.append(o)
        if days_ago <= 90: last_90.append(o)

    tw_win_rate   = round(weighted_wins / total_weight * 100, 1) if total_weight > 0 else None
    tw_avg_return = round(weighted_return / total_weight, 2) if total_weight > 0 else None

    def flat_wr(picks):
        if not picks: return None
        return round(len([p for p in picks if p["outcome"]=="WIN"]) / len(picks) * 100, 1)

    tw_30d = flat_wr(last_30)
    tw_90d = flat_wr(last_90)

    if tw_30d is not None and tw_90d is not None and len(last_30)>=3 and len(last_90)>=5:
        diff  = tw_30d - tw_90d
        trend = "IMPROVING" if diff >= 5 else "DECLINING" if diff <= -5 else "STABLE"
    else:
        trend = "BUILDING"

    return {
        "tw_win_rate":     tw_win_rate,
        "tw_avg_return":   tw_avg_return,
        "tw_trend":        trend,
        "tw_30d_win_rate": tw_30d,
        "tw_90d_win_rate": tw_90d,
        "tw_30d_count":    len(last_30),
        "tw_90d_count":    len(last_90),
        "tw_lambda":       LAMBDA,
        "tw_halflife_days":35,
    }


def compute_win_rate():
    """
    Compute win rate + expectancy + calibration curve + multi-window stats.
    Expectancy = (WR × avg_win) - (loss_rate × avg_loss)
    Positive expectancy = real edge.
    """
    outcomes = load_outcomes()
    resolved = [o for o in outcomes if o.get("resolved") and o.get("outcome")]

    if len(resolved) < 3:
        return {
            "total_resolved": len(resolved), "wins": 0, "win_rate": None,
            "avg_return": None, "best_return": None, "worst_return": None,
            "message": f"Building... ({len(resolved)} outcomes tracked so far)",
            "by_score_tier": {}, "by_category": {}, "recent_10": [],
            "streak": 0, "streak_type": None, "time_weighted": None,
        }

    wins   = [o for o in resolved if o["outcome"] == "WIN"]
    losses = [o for o in resolved if o["outcome"] == "LOSS"]
    flats  = [o for o in resolved if o["outcome"] == "FLAT"]

    win_rate   = len(wins) / len(resolved) * 100
    avg_return = sum(o["actual_return"] for o in resolved) / len(resolved)
    avg_win    = sum(o["actual_return"] for o in wins)   / len(wins)   if wins   else 0.0
    avg_loss   = sum(o["actual_return"] for o in losses) / len(losses) if losses else 0.0
    loss_rate  = len(losses) / len(resolved)
    expectancy = round((win_rate/100 * avg_win) - (loss_rate * abs(avg_loss)), 3)

    # Calibration curve
    calibration = {}
    for lo in range(50, 100, 10):
        hi = lo + 9
        b  = [o for o in resolved if lo <= o.get("score", 0) <= hi]
        if len(b) >= 5:
            bw = len([o for o in b if o["outcome"] == "WIN"])
            calibration[f"{lo}-{hi}"] = {
                "win_rate": round(bw/len(b)*100, 1),
                "count":    len(b),
                "avg_ret":  round(sum(o["actual_return"] for o in b)/len(b), 2),
            }

    # Multi-window
    windows = {}
    for days, label in [(14,"14d"),(30,"30d"),(60,"60d")]:
        cutoff = (datetime.now()-timedelta(days=days)).strftime("%Y-%m-%d")
        wp = [o for o in resolved if o.get("signal_date","")>=cutoff]
        if wp:
            ww  = len([o for o in wp if o["outcome"]=="WIN"])
            wa  = sum(o["actual_return"] for o in wp if o["outcome"]=="WIN")  / max(len([o for o in wp if o["outcome"]=="WIN"]),1)
            la  = sum(o["actual_return"] for o in wp if o["outcome"]=="LOSS") / max(len([o for o in wp if o["outcome"]=="LOSS"]),1)
            wlr = len([o for o in wp if o["outcome"]=="LOSS"]) / len(wp)
            windows[label] = {
                "win_rate":   round(ww/len(wp)*100, 1),
                "count":      len(wp),
                "avg_ret":    round(sum(o["actual_return"] for o in wp)/len(wp), 2),
                "expectancy": round((ww/len(wp)*wa) - (wlr*abs(la)), 3),
            }

    # By score tier
    by_score = {}
    for tier, min_s, max_s in [("90-100",90,100),("75-89",75,89),("60-74",60,74),("below-60",0,59)]:
        tp = [o for o in resolved if min_s <= o.get("score",0) <= max_s]
        if tp:
            tw       = len([o for o in tp if o["outcome"]=="WIN"])
            rets     = [o["actual_return"] for o in tp]
            wins_abs = [r for r in rets if r > 0]
            loss_abs = [abs(r) for r in rets if r < 0]
            pf       = round(sum(wins_abs)/sum(loss_abs), 2) if loss_abs else 0
            avg_win  = round(sum(wins_abs)/len(wins_abs), 2) if wins_abs else 0
            avg_loss = round(sum(loss_abs)/len(loss_abs), 2) if loss_abs else 0
            by_score[tier] = {
                "win_rate":      round(tw/len(tp)*100, 1),
                "count":         len(tp),
                "avg_ret":       round(sum(rets)/len(rets), 2),
                "avg_return":    round(sum(rets)/len(rets), 2),
                "profit_factor": pf,
                "avg_win":       avg_win,
                "avg_loss":      avg_loss,
            }

    # By category
    by_cat = {}
    for cat in set(o.get("category","OTHER") for o in resolved):
        cp = [o for o in resolved if o.get("category")==cat]
        if cp:
            cw = len([o for o in cp if o["outcome"]=="WIN"])
            by_cat[cat] = {"win_rate": round(cw/len(cp)*100,1), "count": len(cp)}

    # Recent 10
    recent = sorted(resolved, key=lambda x: x.get("resolved_date",""), reverse=True)[:10]
    recent_10 = [{"ticker":o["ticker"],"date":o["signal_date"],"ret":o["actual_return"],
                  "outcome":o["outcome"],"score":o.get("score",0)} for o in recent]

    # Streak
    streak = 0; streak_type = None
    for o in sorted(resolved, key=lambda x: x.get("resolved_date",""), reverse=True):
        if streak == 0:
            streak_type = o["outcome"]; streak = 1
        elif o["outcome"] == streak_type:
            streak += 1
        else:
            break

    tw = compute_time_weighted_win_rate(resolved)

    # Feature coverage report — shows ML data quality
    feature_fields = ["perf_90d","volatility","roe","profit_margin","pe_ratio",
                      "rev_growth","earn_growth","div_yield","debt_equity","rs_rating",
                      "regime","spx_vs_ma200","news_boost"]
    feature_coverage = {}
    for field in feature_fields:
        filled = sum(1 for o in resolved
                     if o.get(field) is not None and o.get(field) != 0
                     and o.get(field) != "" and o.get(field) != 20)  # 20 = pe default
        feature_coverage[field] = {
            "pct": round(filled/len(resolved)*100, 1),
            "filled": filled,
            "total": len(resolved),
        }

    result = {
        "total_resolved": len(resolved),
        "wins":           len(wins),
        "losses":         len(losses),
        "flats":          len(flats),
        "win_rate":       round(win_rate, 1),
        "avg_return":     round(avg_return, 2),
        "avg_win":        round(avg_win, 2),
        "avg_loss":       round(avg_loss, 2),
        "expectancy":     expectancy,
        "best_return":    round(max(o["actual_return"] for o in resolved), 2),
        "worst_return":   round(min(o["actual_return"] for o in resolved), 2),
        "by_score_tier":  by_score,
        "by_category":    by_cat,
        "recent_10":      recent_10,
        "streak":         streak,
        "streak_type":    streak_type,
        "time_weighted":  tw,
        "calibration":    calibration,
        "windows":        windows,
        "feature_coverage": feature_coverage,
        "message":        f"{win_rate:.0f}% win rate | Expectancy: {expectancy:+.3f}% per pick",
    }

    with open(WIN_RATE_FILE, "w") as f:
        json.dump(result, f, indent=2)

    return result


def print_win_rate_report(wr):
    print("\n" + "="*55)
    print("  OUTCOME TRACKER — WIN RATE REPORT")
    print("="*55)
    if wr.get("win_rate") is None:
        print(f"  {wr['message']}"); return

    print(f"  Total resolved:  {wr['total_resolved']} picks")
    print(f"  Win rate:        {wr['win_rate']}%  (flat average)")
    print(f"  Avg return/pick: {wr['avg_return']:+.2f}%")
    print(f"  Best:  {wr['best_return']:+.2f}%   Worst: {wr['worst_return']:+.2f}%")

    tw = wr.get("time_weighted")
    if tw and tw.get("tw_win_rate") is not None:
        trend_icon = {"IMPROVING":"📈","DECLINING":"📉","STABLE":"➡️","BUILDING":"🔨"}.get(tw["tw_trend"],"")
        print(f"\n  TIME-WEIGHTED WIN RATE (recent picks count more):")
        print(f"  {trend_icon} TW Win rate:  {tw['tw_win_rate']}%  (35-day half-life decay)")
        if tw.get("tw_avg_return") is not None:
            print(f"  TW Avg return: {tw['tw_avg_return']:+.2f}%")
        if tw.get("tw_30d_win_rate") is not None:
            print(f"  Last 30 days:  {tw['tw_30d_win_rate']}%  ({tw['tw_30d_count']} picks)")
        if tw.get("tw_90d_win_rate") is not None:
            print(f"  Last 90 days:  {tw['tw_90d_win_rate']}%  ({tw['tw_90d_count']} picks)")
        print(f"  Trend:         {tw['tw_trend']}")

    if wr.get("streak"):
        icon = "🔥" if wr["streak_type"]=="WIN" else "❄️"
        print(f"\n  {icon} Current streak: {wr['streak']} {'WIN' if wr['streak_type']=='WIN' else 'LOSS'}")

    if wr.get("by_score_tier"):
        print(f"\n  WIN RATE BY SCORE TIER:")
        for tier, data in sorted(wr["by_score_tier"].items(), reverse=True):
            bar  = "█" * int(data["win_rate"] / 5)
            pf   = data.get("profit_factor", 0)
            pflag = "✅" if pf >= 1.5 else "⚠️ " if pf >= 1.0 else "🔴"
            print(f"  Score {tier:<10} {bar:<15} {data['win_rate']}%  "
                  f"({data['count']} picks, avg {data['avg_ret']:+.1f}%, PF={pf:.2f} {pflag})")

    # Feature coverage — shows ML data quality
    fc = wr.get("feature_coverage", {})
    if fc:
        new_features  = {k:v for k,v in fc.items() if v["pct"] > 1}
        zero_features = {k:v for k,v in fc.items() if v["pct"] <= 1}
        if new_features:
            print(f"\n  ML FEATURE COVERAGE (new picks only):")
            for fname, fdata in list(new_features.items())[:4]:
                print(f"    {fname:<20} {fdata['pct']:.0f}% ({fdata['filled']}/{fdata['total']} picks)")
            best_filled = max(v["filled"] for v in new_features.values())
            needed = max(0, 300 - best_filled)
            if needed > 0:
                print(f"    Building... ~{needed:,} more featured picks until ML retrains with signal")
        if zero_features and not new_features:
            print(f"\n  ⚠️  ML features: 0% coverage ({len(zero_features)} fields)")
            print(f"     All historical picks lack feature data — new picks being captured from today")

    if wr.get("recent_10"):
        print(f"\n  LAST {len(wr['recent_10'])} PICKS:")
        for r in wr["recent_10"]:
            icon = "✅" if r["outcome"]=="WIN" else ("❌" if r["outcome"]=="LOSS" else "➖")
            print(f"  {icon} {r['ticker']:<10} {r['ret']:+.1f}%  (score {r['score']})  {r['date']}")
