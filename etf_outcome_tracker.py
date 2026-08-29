"""
etf_outcome_tracker.py — InvestOS ETF Signal Outcome Tracker
==============================================================
etf_engine.py has scored ETFs daily since inception with no way to know
whether any of it was ever right. This closes that gap, following the
same log-at-signal-time -> resolve-after-N-days -> pure classify
function -> win-rate report shape as outcome_tracker.py (stocks) and
ngx_outcome_tracker.py (NGX) -- but as its own file, not a reuse of
either, because neither the schema nor the threshold transfers:

  - outcome_tracker.py's log_picks() entries carry a stock-ML feature
    snapshot (perf_90d, roe, rs_rating, ...) that has no ETF analogue --
    etf_engine.py's scoring is rules-only, no ML.
  - A single WIN/LOSS/FLAT threshold does not work across the ETF
    universe: 2 years of real price history showed DEFENSIVE bond ETFs
    (ZAG.TO 30d p10=0.14%) and THEMATIC ETFs (ARKG 30d p10=1.64%) differ
    by 10x+ in typical 30-day move size. See CATEGORY_THRESHOLDS below.

Files:
  etf_outcomes.json — all logged/resolved ETF signal entries
"""

import json
import os
from datetime import datetime, timedelta

ETF_OUTCOMES_FILE = "etf_outcomes.json"

# Resolution window: 30 calendar days, uniform across the universe (the
# complexity lives in the threshold, not the window -- same design choice
# NGX made with its single 14-day window). Matches what etf_engine.py's
# own scoring already emphasizes (ret_90 as the primary signal, not a
# 7-day catalyst) better than the stock engine's 7-day or NGX's 14-day
# windows would.
#
# NOTE: this tracker started logging 2026-08-07, so with a 30-day window the
# first signal only becomes eligible to resolve on 2026-09-06. An all-pending
# etf_outcomes.json before that date is expected, not a resolution bug --
# see test_etf_outcome_tracker.py.
RESOLUTION_DAYS = 30

# Category-tiered WIN/LOSS/FLAT thresholds, calibrated near each
# category's own p10 of |30-day return| (same principle the stock ±0.5%
# threshold was calibrated by) from 2 years of real price history on a
# SMALL sample per category -- named explicitly so the sample size is
# never hidden behind a number that looks more authoritative than it is.
# THEMATIC, INTL, and SECTOR are PROVISIONAL -- revisit once enough real
# resolved ETF outcomes accumulate to check these against real data
# instead of a handful of representative tickers.
#
#   CORE             (n=2/7):  VOO, XIC.TO                     -- solid
#   SECTOR_COMMODITY:          XEG.TO, XLE, GLD, ZGD.TO        -- PROVISIONAL.
#                          Held at the old lumped SECTOR 1.00 for now. A
#                          2026-08-29 dry run on 70 days of history put
#                          |fwd-30d return| p10 at ~1.7 and trailing
#                          |ret_30| p10 at ~2.7 -- but that window was a
#                          commodity trend (XLE mean|r| ~15%), too regime-
#                          specific to recalibrate from. Revisit with real
#                          resolved outcomes or a proper 2yr recalibration.
#   SECTOR_EARNINGS:          ZEB.TO, XRE.TO                   -- bank/REIT
#                          ETFs move like CORE (dry run: fwd-30d mean|r|
#                          ~2.7% vs commodity ~15%, ~identical to CORE's
#                          ~2.2%). Threshold set to CORE's 0.75, carved
#                          down from the lumped 1.00 that was hiding real
#                          directional moves as FLAT.
#   THEMATIC   (n=2/10): ARKG, BOTZ                            -- PROVISIONAL,
#                          most heterogeneous category (genomics vs
#                          quantum vs defense), least representative pair
#   DEFENSIVE  (n=2/4):  TLT, ZAG.TO                           -- solid,
#                          both bond ETFs, consistent low-vol signature.
#                          ZLB.TO also sits here (low-vol equity); the dry
#                          run found its data too thin (1 ticker) to give
#                          it its own bucket -- left for a separate look.
#   INTL       (n=1/2):  EEM only (missed EFA, the calmer developed-
#                          markets half)                        -- PROVISIONAL,
#                          not even a real distribution, one ticker's own
#                          time series
CATEGORY_THRESHOLDS = {
    "DEFENSIVE":        0.25,
    "CORE":             0.75,
    "SECTOR_COMMODITY": 1.00,
    "SECTOR_EARNINGS":  0.75,
    "SECTOR":           1.00,   # legacy alias -- pre-2026-08-29 logged entries
    "INTL":             1.00,
    "THEMATIC":         1.50,
}
DEFAULT_THRESHOLD_PCT = 0.75  # fallback for any category not in the table above


def _threshold_for(category):
    """Single source of truth for a category's WIN/LOSS/FLAT band, so the
    resolver can record the exact threshold it applied (threshold_used)
    without re-deriving it."""
    return CATEGORY_THRESHOLDS.get(category, DEFAULT_THRESHOLD_PCT)


def _classify_etf_outcome(actual_return_pct, category):
    """The one place ETF WIN/LOSS/FLAT classification happens -- never
    duplicate this. Threshold is category-keyed, unlike outcome_tracker.py's
    single OUTCOME_THRESHOLD_PCT, because a flat threshold provably doesn't
    work across this universe (see CATEGORY_THRESHOLDS above)."""
    threshold = _threshold_for(category)
    if actual_return_pct > threshold:
        return "WIN"
    elif actual_return_pct < -threshold:
        return "LOSS"
    else:
        return "FLAT"


def load_etf_outcomes():
    if os.path.exists(ETF_OUTCOMES_FILE):
        try:
            return json.load(open(ETF_OUTCOMES_FILE))
        except Exception:
            pass
    return []


def save_etf_outcomes(outcomes):
    try:
        json.dump(outcomes, open(ETF_OUTCOMES_FILE, "w"), indent=2, default=str)
    except Exception as e:
        print(f"   ⚠️  ETF outcome save failed: {e}")


def log_etf_signals(etf_result, run_time=None):
    """
    Log today's full scored ETF universe for future resolution.
    Called after run_etf_engine() returns.

    Logs the FULL scored list (minus SIGNAL confirmation-only entries,
    which aren't real recommendations), not just each account's top-N --
    the point is validating the scoring itself, and only ever logging
    cherry-picked top picks risks survivorship bias (never learning
    whether a low-scored ETF's low score was justified). acct_flags
    records which accounts' top-N a ticker was in that day, so reporting
    can still slice down to "top-5 FHSA picks only" without having
    thrown that information away at logging time.

    entry_price is the price etf_engine.py already fetched this run
    (_fetch_etf_data() inside run_etf_engine()) -- no new fetch needed,
    same principle as ngx_outcome_tracker.py's log_ngx_signals(): a price
    can only ever be captured live, right now, so a signal logged
    without one can never be resolved later.
    """
    if not etf_result:
        return 0

    scored = etf_result.get("scored", [])
    if not scored:
        return 0

    outcomes  = load_etf_outcomes()
    today_str = datetime.now().strftime("%Y-%m-%d")
    now       = run_time or datetime.now().isoformat()

    logged_today = {o["ticker"] for o in outcomes if o.get("signal_date") == today_str}

    rrsp_tickers = {p["ticker"] for p in etf_result.get("rrsp_picks", [])}
    tfsa_tickers = {p["ticker"] for p in etf_result.get("tfsa_picks", [])}
    fhsa_tickers = {p["ticker"] for p in etf_result.get("fhsa_picks", [])}

    regime = etf_result.get("regime", "NEUTRAL")

    new_logged = 0
    for e in scored:
        ticker = e.get("ticker")
        if not ticker or ticker in logged_today:
            continue

        entry = {
            "ticker":           ticker,
            "name":             e.get("name", ticker),
            "category":         e.get("category"),   # drives threshold at resolution
            "signal_date":      today_str,
            "signal_time":      now,
            "score_at_signal":  e.get("score"),
            "entry_price":      e.get("price"),        # already fetched this run
            "regime_at_signal": regime,
            "acct_flags": {
                "rrsp": ticker in rrsp_tickers,
                "tfsa": ticker in tfsa_tickers,
                "fhsa": ticker in fhsa_tickers,
            },

            # Resolution fields (filled in after RESOLUTION_DAYS)
            "resolved":          False,
            "resolved_date":     None,
            "exit_price":        None,
            "actual_return_pct": None,
            "outcome":           None,   # WIN | LOSS | FLAT
            "threshold_used":    None,   # the +/- band applied at classification
        }
        outcomes.append(entry)
        new_logged += 1

    save_etf_outcomes(outcomes)
    if new_logged:
        print(f"   📝 ETF outcomes: logged {new_logged} signals ({len(outcomes)} total)")
    return new_logged


def resolve_etf_outcomes(current_prices=None):
    """
    Check unresolved ETF signals. Resolve after RESOLUTION_DAYS (30
    calendar days). current_prices is an optional {ticker: price} dict
    (e.g. from a fresh etf_engine scoring pass this run) -- for any
    ticker not in it, falls back to a live per-ticker fetch via
    etf_engine._fetch_etf_data(), the same fetch etf_engine.py already
    uses for scoring, so no new fetch mechanism is introduced here.

    Never silently defaults a missing price to a fake outcome -- leaves
    unresolved and retries next run, same principle as outcome_tracker.py
    and ngx_outcome_tracker.py's resolution logic.
    """
    outcomes = load_etf_outcomes()
    if not outcomes:
        return 0

    today = datetime.now().date()
    resolved = 0
    current_prices = current_prices or {}

    for o in outcomes:
        if o.get("resolved"):
            continue

        try:
            signal_date = datetime.strptime(o["signal_date"], "%Y-%m-%d").date()
        except (KeyError, ValueError):
            continue

        days_passed = (today - signal_date).days
        if days_passed < RESOLUTION_DAYS:
            continue

        ticker      = o["ticker"]
        entry_price = o.get("entry_price")
        if entry_price is None or entry_price <= 0:
            o["outcome"] = None
            continue  # never captured a valid entry price -- can't resolve, leave unresolved

        exit_price = current_prices.get(ticker)
        if exit_price is None:
            try:
                import etf_engine as _ee
                price_data = _ee._fetch_etf_data(ticker)
                exit_price = price_data["price"] if price_data else None
            except Exception:
                exit_price = None

        if exit_price is None:
            continue  # fetch failed -- leave unresolved, retry next run

        actual_return_pct = (exit_price - entry_price) / entry_price * 100

        o["resolved"]          = True
        o["resolved_date"]     = today.isoformat()
        o["exit_price"]        = round(exit_price, 4)
        o["actual_return_pct"] = round(actual_return_pct, 2)
        o["outcome"]           = _classify_etf_outcome(actual_return_pct, o.get("category"))
        o["threshold_used"]    = _threshold_for(o.get("category"))

        resolved += 1

    save_etf_outcomes(outcomes)
    if resolved:
        print(f"   ✅ ETF outcomes: resolved {resolved} signals")
    return resolved


def etf_outcome_summary():
    """
    Win-rate report. Carries full_universe and top_n_by_account as
    SEPARATE top-level sections, never blended -- same multi-cut pattern
    win_rate.json already uses (by_score_tier, by_category, windows),
    and the specific design decision made when this was scoped: a
    top-N-per-account win rate is a meaningfully different population
    (FHSA's thematic/GLD/TLT/etc exclusions alone make it so) from the
    full scored universe, and blending them into one number would hide
    that difference rather than surface it.
    """
    outcomes = load_etf_outcomes()
    if not outcomes:
        return {}

    resolved = [o for o in outcomes if o.get("resolved")]
    pending  = [o for o in outcomes if not o.get("resolved")]

    def _stats(rows):
        if not rows:
            return {"total_resolved": 0, "wins": 0, "losses": 0, "flats": 0, "win_rate": 0}
        wins   = sum(1 for o in rows if o.get("outcome") == "WIN")
        losses = sum(1 for o in rows if o.get("outcome") == "LOSS")
        flats  = sum(1 for o in rows if o.get("outcome") == "FLAT")
        return {
            "total_resolved": len(rows),
            "wins":     wins,
            "losses":   losses,
            "flats":    flats,
            "win_rate": round(wins / len(rows) * 100, 1) if rows else 0,
        }

    full_universe = _stats(resolved)

    top_n_by_account = {}
    for acct in ("rrsp", "tfsa", "fhsa"):
        acct_rows = [o for o in resolved if o.get("acct_flags", {}).get(acct)]
        top_n_by_account[acct] = _stats(acct_rows)

    # By category, full universe only (same population footnote applies)
    by_category = {}
    for o in resolved:
        cat = o.get("category", "unknown")
        by_category.setdefault(cat, []).append(o)
    by_category = {cat: _stats(rows) for cat, rows in by_category.items()}

    return {
        "total_logged":     len(outcomes),
        "pending":          len(pending),
        "full_universe":    full_universe,
        "top_n_by_account": top_n_by_account,
        "by_category":      by_category,
    }


def print_etf_outcome_report():
    """Print formatted ETF outcome report."""
    s = etf_outcome_summary()
    if not s:
        print("   No ETF outcomes logged yet")
        return

    print(f"\n{'='*50}")
    print(f"  ETF SIGNAL OUTCOMES")
    print(f"{'='*50}")
    print(f"  Total logged: {s['total_logged']}  |  Pending: {s['pending']}")

    fu = s["full_universe"]
    if fu["total_resolved"] > 0:
        print(f"\n  FULL UNIVERSE WIN RATE: {fu['win_rate']}% "
              f"({fu['wins']}W / {fu['losses']}L / {fu['flats']}F, n={fu['total_resolved']})")
    else:
        print(f"\n  FULL UNIVERSE: no resolved signals yet "
              f"(signals resolve after {RESOLUTION_DAYS} days)")

    for acct, stats in s["top_n_by_account"].items():
        if stats["total_resolved"] > 0:
            print(f"  {acct.upper()} top-N WIN RATE: {stats['win_rate']}% "
                  f"({stats['wins']}W / {stats['losses']}L / {stats['flats']}F, n={stats['total_resolved']})")

    if s["by_category"]:
        print(f"\n  BY CATEGORY:")
        for cat, stats in sorted(s["by_category"].items(), key=lambda x: -x[1]["total_resolved"]):
            if stats["total_resolved"] > 0:
                print(f"    {cat:<17} {stats['win_rate']:>5.1f}%  (n={stats['total_resolved']})")
