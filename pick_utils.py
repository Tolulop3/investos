"""
InvestOS — shared pick-list utilities
======================================
Canonical fix for a recurring bug class (2026-08-08): stock_screener.py's
FHSA pass and TFSA pass each call classify_pick() independently for a
ticker that qualifies for both, producing two separate dict objects for
the same ticker -- not shared references. Any code that flattens multiple
screener buckets together (for score history, signal accuracy, stress-test
baselines, sector-cap reserve pools, etc.) risks silently processing both
copies as if they were different tickers.

Confirmed live in production 2026-08-08: TOST/CSCO/RTX/VLO collapsed to
score=50.0 in score_history.json when an un-scored duplicate silently
overwrote the real ML-scored entry. A deeper audit found the same root
cause recurring independently in 6+ call sites across run_daily.py,
ml_engine.py, and risk_engine.py -- including one severe case
(ml_engine.py's _apply_sector_cap) where a duplicate could be added to
the FINAL POSITION-SIZING BASKET twice, meaning real double capital
allocation to the same stock.

Rather than patch each call site with its own ad-hoc concatenation +
dedup logic (exactly how this bug proliferated in the first place), this
module provides ONE canonical, tested utility every such call site should
use.
"""


def dedupe_picks_by_ticker(picks, verbose=False, label="picks"):
    """
    Collapse a list of pick dicts -- possibly containing multiple
    independent duplicate objects per ticker -- down to one canonical
    entry per ticker.

    Priority: prefer the copy that has an 'ml_prob' field (the
    definitive signal it went through ML scoring) over raw score -- an
    un-scored duplicate's pre-ML score can coincidentally exceed the
    ML-scored copy's post-adjustment score, so score alone isn't a
    reliable tiebreaker. Degrades gracefully to score-only comparison
    when neither copy has ml_prob (e.g. summary dicts built without that
    field, as in risk_engine.py's stress-test baseline).

    `label` is only used in the (optional) diagnostic print -- pass a
    short name for the call site (e.g. "top_flat", "signal_accuracy") so
    a real mismatch is traceable to where it was caught.
    """
    def _priority(p):
        return (1 if "ml_prob" in p else 0, p.get("score", 0) or 0)

    seen: dict = {}
    for p in picks:
        ticker = p.get("ticker")
        if not ticker:
            continue
        existing = seen.get(ticker)
        if existing is not None:
            score_gap   = abs((existing.get("score", 0) or 0) - (p.get("score", 0) or 0))
            ml_mismatch = ("ml_prob" in existing) != ("ml_prob" in p)
            if verbose and (score_gap > 0.5 or ml_mismatch):
                print(f"   ⚠️  {label} duplicate: {ticker} appeared multiple times "
                      f"(scores {existing.get('score')} vs {p.get('score')}, "
                      f"ml_prob present: {'ml_prob' in existing} vs {'ml_prob' in p}) "
                      f"-- keeping the higher-priority copy")
        if existing is None or _priority(p) > _priority(existing):
            seen[ticker] = p

    deduped = list(seen.values())
    if verbose and len(deduped) < len(picks):
        print(f"   ℹ️  {label} dedup: {len(picks)} → {len(deduped)} picks "
              f"(removed {len(picks)-len(deduped)} duplicates)")
    return deduped


def get_pick_data(p):
    """
    Canonical accessor for a pick's classify_pick() sub-dict -- every
    screener bucket builds picks as {"ticker":..., "score":..., "data":
    {...raw market data, including "sector"...}, "pick": {...category,
    hold_days, exp_low, exp_high, action, etc...}, ...}. classify_pick()
    output lives ONLY under p["pick"], never at the top level.

    FIX (2026-08-09): this is the structural fix for a recurring bug class
    -- ac4973cd (category routing read pick["category"], always None, so
    routing silently fell through) and run_daily.py's sector-headwind
    penalty (same shape, sector instead of category) both happened because
    there was no single place enforcing this shape; every call site
    hand-rolled its own `.get("pick", {})` chain. Use this (and
    get_pick_category/get_pick_field/get_pick_sector below) instead of
    reaching into p["pick"] or p["data"] directly -- see
    tests/test_invariants.py's test_no_raw_pick_dict_access_outside_pick_utils
    for the enforcement mechanism that keeps this the ONLY place that does.
    """
    return p.get("pick") or {}


def get_pick_category(p):
    """category lives under p["pick"]["category"], never p["category"]."""
    return get_pick_data(p).get("category", "")


def get_pick_field(p, field, default=None):
    """Any other classify_pick() output field (hold_days, exp_low,
    exp_high, action, exit_note, risk_label, etc) -- same shape as
    get_pick_category, generalized."""
    return get_pick_data(p).get(field, default)


def get_pick_sector(p):
    """
    sector lives under p["data"]["sector"] (raw yfinance sector string,
    e.g. "Financial Services"), never at the top level -- p["sector"] is
    written back later in ml_engine.py's scoring loop for a DIFFERENT,
    normalized purpose (sector_canonical / gate checks), so it can be
    present-but-stale or absent depending on pipeline stage. Checking it
    first (if present) then falling back to the raw data dict matches the
    one call site that already had this right (run_daily.py's
    correlation filter) before this became a shared utility.
    """
    return p.get("sector", "") or p.get("data", {}).get("sector", "")


def dedupe_raw_data_by_ticker(data_list):
    """
    Same root cause as dedupe_picks_by_ticker(), but for raw stock "data"
    dicts (perf_90d/volatility/sector/etc, keyed by "ticker") rather than
    pick dicts -- e.g. run_daily.py's `all_raw` fed into
    calculate_relative_strength(). Unlike pick dicts, a straddling ticker's
    "data" entries across FHSA/TFSA buckets are literally the SAME object
    reference (see stock_screener.py's `"data": d` in both the FHSA and
    TFSA pass), so there is no "which copy is better" question -- plain
    first-seen-wins is correct, no score/ml_prob priority logic needed.

    Un-deduped, a straddling ticker is inserted twice into
    calculate_relative_strength()'s population, inflating `total` and
    shifting every stock's percentile-ranked rs_rating, not just the
    duplicate's.
    """
    seen = set()
    out = []
    for d in data_list:
        t = d.get("ticker")
        if not t or t in seen:
            continue
        seen.add(t)
        out.append(d)
    return out
