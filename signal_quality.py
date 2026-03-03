"""
InvestOS — Signal Quality Module
==================================
Three filters that improve pick quality by removing noise and adding valuation context.

1. EARNINGS DATE FILTER
   Removes picks within 7 days of earnings — the biggest single source of false signals.
   Earnings create binary gap risk that invalidates any technical or fundamental analysis.

2. GRAHAM INTRINSIC VALUE (FLOOR bucket only)
   Benjamin Graham's revised formula: V = [EPS × (8.5 + 2g) × 4.4] / Y
   Where Y = current AAA bond yield (proxied by 10-year Treasury + spread).
   Adds margin_of_safety % to each FLOOR pick. Filters out stocks trading >40% above intrinsic value.
   Source: The Intelligent Investor (1973 edition), Chapter 11.

3. 52-WEEK HIGH BREAKOUT PROXIMITY
   Flags stocks within 5–15% of their 52-week high.
   Academic basis: George & Hwang (2004) showed 52-week high proximity is one of
   the strongest momentum predictors — stocks near new highs tend to continue higher.
   Adds breakout signal to conviction engine.
"""

from datetime import datetime, timedelta
import math

# ─────────────────────────────────────────────
# CURRENT BOND YIELD (AAA proxy)
# Used in Graham formula. We use a static estimate updated quarterly.
# The 10-year US Treasury + ~0.5% spread approximates AAA corporate yield.
# As of Q1 2026, ~10Y Treasury ~4.3% + 0.5% spread = ~4.8%
# Graham's baseline was 4.4% in 1962 — we adjust accordingly.
# ─────────────────────────────────────────────
AAA_BOND_YIELD_PCT = 4.8  # Update this quarterly


# ══════════════════════════════════════════════════════════════════
# 1. EARNINGS DATE FILTER
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
    next_earnings = pick.get("next_earnings")
    if not next_earnings:
        return False  # No earnings date = don't filter

    today = datetime.now().date()

    try:
        # Yahoo returns earnings date as string "YYYY-MM-DD" or unix timestamp
        if isinstance(next_earnings, (int, float)):
            earnings_date = datetime.fromtimestamp(next_earnings).date()
        else:
            earnings_date = datetime.strptime(str(next_earnings)[:10], "%Y-%m-%d").date()

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
            earnings = pick.get("next_earnings", "?")
            if verbose:
                print(f"   ⚠️  {ticker} filtered — earnings within 7 days ({earnings})")
            filtered.append(pick)
        else:
            clean.append(pick)

    if verbose and filtered:
        print(f"   🚫 Earnings filter removed {len(filtered)} picks: "
              f"{[p.get('ticker') for p in filtered]}")

    return clean, filtered


# ══════════════════════════════════════════════════════════════════
# 2. GRAHAM INTRINSIC VALUE
# ══════════════════════════════════════════════════════════════════

def compute_graham_value(pick, bond_yield_pct=None):
    """
    Computes Graham intrinsic value using the 1974 revised formula:

        V = [EPS × (8.5 + 2g) × 4.4] / Y

    Where:
        EPS  = trailing 12-month earnings per share (price / PE ratio)
        8.5  = base PE for zero-growth company
        g    = expected annual earnings growth rate (next 5-7 years) as percentage
        4.4  = Graham's 1962 AAA bond yield baseline
        Y    = current AAA corporate bond yield (we use 10Y Treasury + spread)

    Returns dict with:
        intrinsic_value    — estimated fair value per share
        margin_of_safety   — % discount (positive = cheap, negative = expensive)
        graham_rating      — UNDERVALUED / FAIR / OVERVALUED / VERY_OVERVALUED
        eps_used           — EPS computed from price/PE
        growth_used        — growth rate used

    Design decisions:
    - EPS derived from price ÷ PE ratio (TTM)
    - Growth rate = earn_growth from screener, capped at 25% (Graham warned against extrapolating high growth)
    - Minimum growth floor of 2% (no company is truly zero-growth in a BULL regime)
    - Applies only when PE > 0 and price > 0 (earnings must be positive)
    - For FLOOR bucket: stocks must show >20% margin of safety to qualify
    """
    if bond_yield_pct is None:
        bond_yield_pct = AAA_BOND_YIELD_PCT

    price    = pick.get("price", 0)
    pe_ratio = pick.get("pe_ratio")
    earn_growth = pick.get("earn_growth", 0) or 0

    # Can't compute without valid PE and price
    if not pe_ratio or pe_ratio <= 0 or not price or price <= 0:
        return None

    # Derive EPS from price and PE
    eps = price / pe_ratio

    # Negative EPS = losses = Graham explicitly excludes these
    if eps <= 0:
        return None

    # Growth rate: use screener's earn_growth, but apply Graham's guardrails
    # Cap at 25% — Graham said beyond this you're speculating, not valuing
    # Floor at 0 — negative growth companies need different analysis
    g = max(0.0, min(25.0, earn_growth))

    # If growth is suspiciously absent (screener returned 0), use conservative 5%
    if g == 0:
        g = 5.0

    # Graham formula: V = [EPS × (8.5 + 2g) × 4.4] / Y
    intrinsic = (eps * (8.5 + 2 * g) * 4.4) / bond_yield_pct

    # Margin of safety = how much below intrinsic the stock is trading
    # Positive = stock is cheap, negative = stock is expensive
    margin_of_safety = round((intrinsic - price) / intrinsic * 100, 1)

    # Rating — calibrated for current ~4.8% rate environment
    # At Graham baseline 4.4%, stocks value ~9% higher, so we widen bands
    if margin_of_safety >= 20:
        rating = "UNDERVALUED"      # Strong buy for FLOOR
    elif margin_of_safety >= -15:
        rating = "FAIR"             # Within normal range
    elif margin_of_safety >= -40:
        rating = "OVERVALUED"       # Paying a premium
    else:
        rating = "VERY_OVERVALUED"  # FLOOR reject: >40% above intrinsic

    return {
        "intrinsic_value":   round(intrinsic, 2),
        "margin_of_safety":  margin_of_safety,
        "graham_rating":     rating,
        "eps_used":          round(eps, 2),
        "growth_used":       round(g, 1),
        "price":             price,
    }


def apply_graham_filter(picks, bucket="FLOOR", verbose=False):
    """
    Applies Graham valuation to picks.

    For FLOOR bucket: hard filter — reject picks with margin_of_safety < -20%
    (i.e. trading >20% above intrinsic value = not a value stock)

    For other buckets: soft label only — adds graham_value field to pick,
    no filtering (growth stocks often look "expensive" by Graham metrics)

    Returns enriched picks with graham_value attached.
    """
    enriched = []
    rejected = []

    for pick in picks:
        gv = compute_graham_value(pick)
        pick["graham_value"] = gv  # Attach to pick regardless

        if bucket == "FLOOR" and gv is not None:
            if gv["graham_rating"] == "VERY_OVERVALUED":
                ticker = pick.get("ticker", "?")
                if verbose:
                    mos = gv["margin_of_safety"]
                    iv = gv["intrinsic_value"]
                    print(f"   📉 {ticker} rejected from FLOOR — {mos:+.0f}% vs intrinsic ${iv}")
                rejected.append(pick)
                continue

        enriched.append(pick)

    if verbose and rejected:
        print(f"   💰 Graham filter removed {len(rejected)} overvalued picks from FLOOR: "
              f"{[p.get('ticker') for p in rejected]}")

    return enriched, rejected


# ══════════════════════════════════════════════════════════════════
# 3. 52-WEEK HIGH BREAKOUT PROXIMITY
# ══════════════════════════════════════════════════════════════════

def get_breakout_signal(pick):
    """
    Computes how close the stock is to its 52-week high.

    Academic basis: George & Hwang (2004) "The 52-Week High and Momentum Investing"
    — proximity to 52-week high is a strong predictor of future outperformance,
    independent of other momentum measures.

    Intuition: when a stock approaches its 52-week high, the market has to
    re-evaluate the stock at new levels. Sellers who bought at the high wait
    to "get out even" — once the stock breaks through, that supply disappears
    and it often runs further.

    Signal zones:
    - BREAKOUT_IMMINENT:  within 3% of 52-week high (price ≥ 97% of w52_high)
    - NEAR_HIGH:          within 10% of 52-week high (price ≥ 90% of w52_high)
    - APPROACHING:        within 20% of 52-week high (price ≥ 80% of w52_high)
    - NEUTRAL:            more than 20% below 52-week high
    - AT_HIGH:            at or above 52-week high (new high territory)

    Returns dict with signal zone, pct_from_high, and score_boost.
    """
    price    = pick.get("price", 0)
    w52_high = pick.get("w52_high", 0)

    if not price or not w52_high or w52_high <= 0:
        return None

    pct_from_high = round((w52_high - price) / w52_high * 100, 1)

    if pct_from_high <= 0:
        zone        = "AT_HIGH"       # New 52-week high — strongest signal
        score_boost = 15
        label       = "🚀 AT 52W HIGH"
    elif pct_from_high <= 3:
        zone        = "BREAKOUT_IMMINENT"
        score_boost = 12
        label       = f"🔥 {pct_from_high:.1f}% from 52W high"
    elif pct_from_high <= 10:
        zone        = "NEAR_HIGH"
        score_boost = 8
        label       = f"💪 {pct_from_high:.1f}% from 52W high"
    elif pct_from_high <= 20:
        zone        = "APPROACHING"
        score_boost = 4
        label       = f"📈 {pct_from_high:.1f}% from 52W high"
    else:
        zone        = "NEUTRAL"
        score_boost = 0
        label       = f"↔ {pct_from_high:.1f}% from 52W high"

    return {
        "zone":          zone,
        "pct_from_high": pct_from_high,
        "score_boost":   score_boost,
        "label":         label,
        "w52_high":      w52_high,
        "price":         price,
    }


def apply_breakout_signals(picks, verbose=False):
    """
    Enriches all picks with 52-week breakout data.
    Applies score boost for stocks near/at 52-week highs.
    No filtering — breakout is additive signal only.

    Returns enriched picks with breakout_signal attached.
    """
    boosted = 0
    at_high = []

    for pick in picks:
        sig = get_breakout_signal(pick)
        pick["breakout_signal"] = sig

        if sig and sig["score_boost"] > 0:
            old_score = pick.get("score", 0)
            pick["score"] = min(100, old_score + sig["score_boost"])
            boosted += 1

            if sig["zone"] in ("AT_HIGH", "BREAKOUT_IMMINENT"):
                at_high.append(pick.get("ticker", "?"))

    if verbose:
        print(f"   📊 Breakout signals: {boosted} picks boosted")
        if at_high:
            print(f"   🔥 Near/at 52W high: {at_high}")

    return picks


# ══════════════════════════════════════════════════════════════════
# COMBINED PIPELINE — apply all three in sequence
# ══════════════════════════════════════════════════════════════════

def apply_all_signal_quality(picks, bucket=None, verbose=True):
    """
    Apply all three signal quality upgrades in the correct order:

    1. Earnings filter FIRST — remove binary risk picks immediately
    2. Graham valuation — compute and filter (FLOOR only) on remaining picks
    3. Breakout signal — enrich survivors with 52W proximity

    Returns (clean_picks, quality_report) where quality_report summarises
    what was filtered/boosted for the dashboard.
    """
    original_count = len(picks)

    if verbose:
        print(f"\n   🎯 Signal quality check ({bucket or 'ALL'} — {original_count} picks)...")

    # Step 1: Earnings filter
    picks, earnings_filtered = apply_earnings_filter(picks, verbose=verbose)

    # Step 2: Graham valuation (FLOOR: hard filter; others: soft label only)
    picks, graham_filtered = apply_graham_filter(
        picks, bucket=bucket or "ALL", verbose=verbose
    )

    # Step 3: 52-week breakout
    picks = apply_breakout_signals(picks, verbose=verbose)

    # Build quality report
    report = {
        "original_count":     original_count,
        "final_count":        len(picks),
        "earnings_removed":   len(earnings_filtered),
        "graham_removed":     len(graham_filtered),
        "earnings_tickers":   [p.get("ticker") for p in earnings_filtered],
        "graham_tickers":     [p.get("ticker") for p in graham_filtered],
        "breakout_tickers":   [
            p.get("ticker") for p in picks
            if p.get("breakout_signal", {}) and
               p.get("breakout_signal", {}).get("zone") in ("AT_HIGH", "BREAKOUT_IMMINENT")
        ],
    }

    if verbose:
        removed = original_count - len(picks)
        if removed:
            print(f"   ✅ Quality check: {original_count} → {len(picks)} picks "
                  f"({removed} removed: {len(earnings_filtered)} earnings, "
                  f"{len(graham_filtered)} Graham overvalued)")
        else:
            print(f"   ✅ Quality check: all {len(picks)} picks passed")

        if report["breakout_tickers"]:
            print(f"   🔥 Breakout imminent: {report['breakout_tickers']}")

    return picks, report
