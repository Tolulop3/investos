"""
InvestOS — Signal Quality Module
==================================
EARNINGS DATE FILTER
   Removes picks within 7 days of earnings — the biggest single source of
   false signals. Earnings create binary gap risk that invalidates any
   technical or fundamental analysis.

── 2026-08-29 audit, item 4.1 ────────────────────────────────────────────────
This file used to also contain a Graham intrinsic-value filter, a 52-week-high
breakout signal, an earnings-quality scorer, an `apply_all_signal_quality`
orchestrator, and a full second SEC EDGAR Form-4 insider stack
(`_edgar_get` / `_resolve_cik` / `fetch_insider_transactions` /
`compute_insider_signal` / `apply_insider_signals`). ~940 lines. NONE of it
was reachable — the only production import is `apply_earnings_filter` in
run_daily.py, and the only test touches `is_near_earnings`. The insider stack
duplicated the live `insider_engine.py`. All removed. Recover from git history
(pre-2026-08-29) if any of it is ever wanted.
"""

from datetime import datetime, timedelta  # noqa: F401  (timedelta kept for callers/tests)


# ─────────────────────────────────────────────
# FIELD ACCESS HELPER
# ─────────────────────────────────────────────
def _get(pick, field, default=None):
    """
    Safely reads a field from a pick dict.
    Picks have structure: {"ticker": x, "score": y, "data": {...all stock data...}}
    This helper checks top-level first, then pick["data"] as fallback.
    """
    val = pick.get(field)
    if val is not None:
        return val
    return pick.get("data", {}).get(field, default)


# ══════════════════════════════════════════════════════════════════
# EARNINGS DATE FILTER
# ══════════════════════════════════════════════════════════════════

def is_near_earnings(pick, days_before=7, days_after=2):
    """
    Returns True if the pick has earnings within [days_before] days ahead
    or [days_after] days behind (to catch post-earnings gap risk).

    Logic:
    - 7 days before: price action becomes unpredictable, options IV spikes,
      any buy signal is overwhelmed by binary earnings outcome
    - 2 days after: post-earnings drift can be violent in either direction

    Data source: next_earnings field from stock_screener (Yahoo Finance)
    """
    next_earnings = _get(pick, "next_earnings")
    if not next_earnings:
        return False  # No earnings date = don't filter

    today = datetime.now().date()

    try:
        # FIX (2026-08-09): this always parsed as "YYYY-MM-DD" and silently
        # failed (caught by the bare except below, returning False) on every
        # real call -- stock_screener.py actually formats this field as
        # "%b %d, %Y" (e.g. "Aug 12, 2026", see its next_earnings assignment),
        # never ISO. Confirmed live: a simulated 3-days-out earnings date in
        # the real format returned False (should be True) before this fix.
        # Try the real format first, ISO/timestamp as fallbacks in case the
        # upstream field shape ever changes.
        if isinstance(next_earnings, (int, float)):
            earnings_date = datetime.fromtimestamp(next_earnings).date()
        else:
            ne_str = str(next_earnings).strip()
            earnings_date = None
            for fmt in ("%b %d, %Y", "%Y-%m-%d"):
                try:
                    earnings_date = datetime.strptime(ne_str, fmt).date()
                    break
                except ValueError:
                    continue
            if earnings_date is None:
                # Last resort: ISO-prefix parse (original behavior), in case
                # a caller ever passes a raw ISO timestamp string.
                earnings_date = datetime.strptime(ne_str[:10], "%Y-%m-%d").date()

        days_to_earnings = (earnings_date - today).days

        # Block window: 7 days before through 2 days after
        if -days_after <= days_to_earnings <= days_before:
            return True

    except Exception:
        pass  # Can't parse date = don't filter

    return False


def apply_earnings_filter(picks, verbose=False):
    """
    Filter out picks within the earnings danger zone.
    Returns (clean_picks, filtered_out) tuple.
    """
    clean = []
    filtered = []

    for pick in picks:
        if is_near_earnings(pick):
            ticker = pick.get("ticker", "?")
            earnings = _get(pick, "next_earnings", "?")
            if verbose:
                print(f"   ⚠️  {ticker} filtered — earnings within 7 days ({earnings})")
            filtered.append(pick)
        else:
            clean.append(pick)

    if verbose and filtered:
        print(f"   🚫 Earnings filter removed {len(filtered)} picks: "
              f"{[p.get('ticker') for p in filtered]}")

    return clean, filtered
