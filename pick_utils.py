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
