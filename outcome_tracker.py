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


def log_picks(picks, run_time=None, regime=None, unified_regime=None,
              macro_regime=None, market_breadth_50ma=None,
              run_type=None, run_id=None):
    """
    Log today's picks with entry price and full ML feature snapshot.

    v2: regime parameter added to capture market context at signal time.
    Pass regime=brief['market_regime'] from run_daily.py.

    Feature snapshot saved per pick:
      - All fundamentals from pick['data'] (perf_90d, roe, volatility etc.)
      - rs_rating from intelligence_layers
      - news_adjustment from news_analyzer
      - regime, spx_vs_ma200 from market regime at signal time
      - unified_regime, macro_regime, market_breadth_50ma (added 2026-07-04)

    These unlock ML training on real data instead of zeros.

    COVERAGE TIMELINE (audited 2026-07-05):
      Call site: run_daily.py:1413 — ONE call site, all three fields confirmed wired.
      unified_regime:      live since 2026-07-01. Resolved coverage = 0% (timing:
                           picks resolve after 90d hold; first real values resolve ~Sep 29).
      macro_regime:        live since 2026-07-05. Resolved coverage = 0% (same timing).
      market_breadth_50ma: live since 2026-06-28. Resolved coverage = 12.8% (backfilled
                           from history snapshots; strategy_health.json has no regime data).
      Action: no code fix needed. Coverage self-corrects as new picks resolve Oct 2026+.
    """
    if not picks:
        return

    outcomes = load_outcomes()
    now      = run_time or datetime.now().isoformat()
    date_str = datetime.now().strftime("%Y-%m-%d")

    # Don't double-log same (ticker, signal_date) — check all entries, not just unresolved.
    # The old guard had `and not o.get("outcome")` which allowed re-logging a same-day
    # pick that had already been resolved (e.g. on a second pipeline run).
    logged_today = {o["ticker"] for o in outcomes
                    if o.get("signal_date") == date_str}

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
            # Use None when unavailable — never default to 50 which masks missing data
            # (ml_retrainer.py treats None as 0 sentinel, not inflated 50)
            "rs_rating":     pick.get("rs_rating") if pick.get("rs_rating") not in (None, 0) else None,

            # ── ML features: news signal applied ───────────────────────────
            # news_adjustment = raw points added/removed (capped at ±8)
            "news_boost":    float(pick.get("news_adjustment", 0) or
                                   pick.get("news_original", 0) or 0),

            # ── ML features: market context at signal time ─────────────────
            "regime":              regime_str,
            "unified_regime":      unified_regime or "UNKNOWN",
            "macro_regime":        macro_regime,          # RISK_OFF/CAUTIOUS/NORMAL/RISK_ON
            "market_breadth_50ma": market_breadth_50ma,  # % of universe above 50MA
            "spx_vs_ma200":        spx_vs_ma200,

            # ── Extra context (not ML features but useful for analysis) ─────
            "rsi":           d.get("rsi_approx", 50) or 50,
            "above_ma200":   bool(d.get("above_ma200", True)),
            "above_ma50":    bool(d.get("above_ma50", True)),
            # Prefer the curated SECTOR_MAP label (set by apply_sector_cap on the
            # pick dict), then fall back to the raw yfinance sector string.
            "sector":        (pick.get("sector") or
                              d.get("sector", "") or d.get("industry", "") or "").strip(),

            # ── Factor attribution fields (for leaderboard analysis) ──────
            # These enable the hedge fund critique's factor isolation question:
            # "Which factor actually predicts returns?"
            "score_rank":    pick.get("score_rank", 0),      # rank in universe today
            "score_pct":     pick.get("score_pct", 50.0),    # top X% of universe
            "options_signal": pick.get("options_signal", ""), # HIGH_IV / BULLISH_PCR / etc
            "conviction":    pick.get("conviction", False),   # was this a 2+ signal pick
            "kelly_wt":      float(pick.get("kelly_wt", 0) or 0),  # kelly fraction at signal

            # ── Run provenance ─────────────────────────────────────────────
            "run_type":      run_type or "manual",   # "scheduled" | "manual"
            "run_id":        run_id or "",
        }
        outcomes.append(entry)
        new_logged += 1

    save_outcomes(outcomes)
    print(f"   📝 Outcome tracker: logged {new_logged} new picks ({len(outcomes)} total)")
    return new_logged


def _fetch_stale_prices(tickers):
    """
    Fetch current prices for tickers not in today's screener picks.
    Used to resolve overdue entries that dropped out of the active universe.
    Batches via yfinance to minimise API calls.
    """
    prices = {}
    if not tickers:
        return prices
    try:
        import yfinance as yf
        # yfinance batch download — single call for all tickers
        batch = list(tickers)
        data  = yf.download(batch, period="2d", auto_adjust=True,
                            progress=False, threads=True)
        if data.empty:
            return prices
        closes = data["Close"] if "Close" in data else data.get("close", None)
        if closes is None:
            return prices
        # Single ticker returns a Series, multiple returns a DataFrame
        if hasattr(closes, "columns"):
            for t in batch:
                col = t.replace(".TO","") if t not in closes.columns else t
                if col in closes.columns:
                    val = closes[col].dropna()
                    if not val.empty:
                        prices[t] = float(val.iloc[-1])
        else:
            # Single ticker case
            if len(batch) == 1 and not closes.empty:
                prices[batch[0]] = float(closes.dropna().iloc[-1])
    except Exception as _e:
        pass  # graceful fallback — unresolved entries stay pending
    return prices


def resolve_outcomes(current_prices):
    """
    Check unresolved picks. Resolve WIN/LOSS after 7 calendar days (~5 trading days).
    Includes a stale-resolution pass for overdue entries not in today's screener picks.
    """
    if current_prices is None:
        current_prices = {}

    outcomes = load_outcomes()
    today    = datetime.now().date()
    resolved = 0

    # ── Identify overdue tickers not covered by today's screener prices ──
    overdue_missing = set()
    for o in outcomes:
        if o.get("resolved") or o.get("outcome"):
            continue
        signal_date = datetime.strptime(o["signal_date"], "%Y-%m-%d").date()
        if (today - signal_date).days >= 7:
            ticker = o["ticker"]
            if ticker not in current_prices:
                overdue_missing.add(ticker)

    # Fetch prices for stale tickers in one batch
    if overdue_missing:
        stale_prices = _fetch_stale_prices(overdue_missing)
        print(f"   🔄 Stale resolution: {len(overdue_missing)} overdue tickers, "
              f"{len(stale_prices)} prices fetched")
        current_prices = {**current_prices, **stale_prices}

    for o in outcomes:
        if o.get("resolved") or o.get("outcome"):
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
    _win_sum  = sum(o["actual_return"] for o in wins)
    _loss_sum = sum(abs(o["actual_return"]) for o in losses)
    profit_factor = round(_win_sum / _loss_sum, 2) if _loss_sum else 0.0

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
                      "regime","spx_vs_ma200","news_boost","sector",
                      "unified_regime","macro_regime","market_breadth_50ma"]
    feature_coverage = {}
    for field in feature_fields:
        filled = sum(1 for o in resolved
                     if o.get(field) is not None and o.get(field) != 0
                     and o.get(field) != "" and o.get(field) != 20   # 20 = pe default
                     and o.get(field) != "UNKNOWN")                  # UNKNOWN = missing regime
        feature_coverage[field] = {
            "pct": round(filled/len(resolved)*100, 1),
            "filled": filled,
            "total": len(resolved),
        }

    # Regime field coverage across ALL logged picks (not just resolved)
    # Shows current-run capture rate; resolved coverage lags by 90-day hold period.
    try:
        all_outcomes = json.load(open(OUTCOMES_FILE)) if os.path.exists(OUTCOMES_FILE) else []
        _n_all = len(all_outcomes)
        if _n_all > 0:
            for _rf in ("unified_regime", "macro_regime", "market_breadth_50ma", "sector"):
                _filled = sum(1 for o in all_outcomes
                              if o.get(_rf) is not None
                              and o.get(_rf) not in ("", 0, "UNKNOWN"))
                feature_coverage[f"ALL_{_rf}"] = {
                    "pct": round(_filled / _n_all * 100, 1),
                    "filled": _filled,
                    "total": _n_all,
                }
    except Exception:
        pass

    # ── OOS performance block (signal_date >= OOS start) ────────────────
    OOS_START = "2026-06-26"
    oos_all      = [o for o in outcomes if (o.get("signal_date") or "") >= OOS_START]
    oos_resolved = [o for o in oos_all if o.get("resolved") and o.get("outcome") in ("WIN","LOSS","FLAT")]
    oos_wins     = [o for o in oos_resolved if o["outcome"] == "WIN"]
    oos_losses   = [o for o in oos_resolved if o["outcome"] == "LOSS"]
    oos_wr       = round(100 * len(oos_wins) / len(oos_resolved), 1) if oos_resolved else None
    oos_avg_ret  = round(sum(o["actual_return"] for o in oos_resolved) / len(oos_resolved), 2) if oos_resolved else None

    # SPY return over OOS period
    oos_spx_return = None
    try:
        import urllib.request as _ur, json as _jspx
        _spy_url = "https://query1.finance.yahoo.com/v8/finance/chart/SPY?interval=1d&range=30d"
        _req = _ur.Request(_spy_url, headers={"User-Agent": "Mozilla/5.0"})
        with _ur.urlopen(_req, timeout=8) as _r:
            _d = _jspx.loads(_r.read().decode())
        _ts   = _d["chart"]["result"][0]["timestamp"]
        _cls  = _d["chart"]["result"][0]["indicators"]["quote"][0]["close"]
        _dates = [__import__("datetime").date.fromtimestamp(t).isoformat() for t in _ts]
        # find closest close on or after OOS_START
        _pairs = [(d, c) for d, c in zip(_dates, _cls) if d >= OOS_START and c]
        if _pairs:
            _start_px = _pairs[0][1]
            _end_px   = next((c for d, c in reversed(_pairs) if c), _pairs[-1][1])
            oos_spx_return = round((_end_px - _start_px) / _start_px * 100, 2)
    except Exception:
        pass

    # OOS tier breakdown
    oos_tiers = {}
    for lo, hi in [(90, 100), (75, 89), (60, 74), (0, 59)]:
        bucket = [o for o in oos_resolved if lo <= (o.get("score") or 0) <= hi]
        label  = f"{lo}-{hi}"
        oos_tiers[label] = {
            "n":       len(bucket),
            "wr":      round(100 * sum(1 for o in bucket if o["outcome"] == "WIN") / len(bucket), 1) if bucket else None,
        }

    oos_block = {
        "start_date":   OOS_START,
        "logged":       len(oos_all),
        "resolved":     len(oos_resolved),
        "wins":         len(oos_wins),
        "losses":       len(oos_losses),
        "win_rate":     oos_wr,
        "avg_return":   oos_avg_ret,
        "spx_return":   oos_spx_return,
        "active_return": round(oos_avg_ret - oos_spx_return, 2) if (oos_avg_ret is not None and oos_spx_return is not None) else None,
        "tiers":        oos_tiers,
    }

    result = {
        "total_resolved": len(resolved),
        "wins":           len(wins),
        "losses":         len(losses),
        "flats":          len(flats),
        "win_rate":       round(win_rate, 1),
        "profit_factor":  profit_factor,
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
        "oos":            oos_block,
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

    print(f"  Total resolved:  {wr['total_resolved']} picks (ex-NGX, ex-dupes)")
    print(f"  ✅ CORRECTED WR: {wr['win_rate']}%  | PF: {wr.get('profit_factor', 0):.2f}")
    print(f"  NGX:             tracked separately — UNRESOLVED (paid API needed)")
    print(f"  Avg return/pick: {wr['avg_return']:+.2f}%")
    print(f"  Best:  {wr['best_return']:+.2f}%   Worst: {wr['worst_return']:+.2f}%")

    oos = wr.get("oos")
    if oos:
        print(f"\n  OOS PERFORMANCE (since {oos['start_date']}):")
        print(f"    Picks logged:    {oos['logged']}")
        print(f"    Resolved:        {oos['resolved']}")
        if oos["resolved"] > 0:
            wr_str  = f"{oos['win_rate']}%" if oos["win_rate"] is not None else "—"
            ret_str = f"{oos['avg_return']:+.2f}%" if oos["avg_return"] is not None else "—"
            spx_str = f"{oos['spx_return']:+.2f}%" if oos["spx_return"] is not None else "—"
            act_str = f"{oos['active_return']:+.2f}%" if oos.get("active_return") else "—"
            print(f"    Win rate:        {wr_str}")
            print(f"    Avg return:      {ret_str}")
            print(f"    SPX return (same period): {spx_str}")
            print(f"    Active return vs SPX:     {act_str}")
            print(f"    Score tier breakdown:")
            for tier_label in ["90-100", "75-89", "60-74", "0-59"]:
                td = oos["tiers"].get(tier_label, {})
                n  = td.get("n", 0)
                if n > 0:
                    wr_t = f"{td['wr']}%" if td["wr"] is not None else "—"
                    print(f"      {tier_label}: {n} picks | WR {wr_t}")
        else:
            print(f"    No resolved OOS picks yet — check back in a few days")

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
