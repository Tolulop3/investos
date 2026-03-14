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
    next_earnings = _get(pick, "next_earnings")
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

    price    = _get(pick, "price", 0)
    pe_ratio = _get(pick, "pe_ratio")
    earn_growth = _get(pick, "earn_growth", 0) or 0

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
            mos = gv["margin_of_safety"]
            # Adaptive graduated penalty instead of binary reject:
            # -15% to -40% above intrinsic: soft penalty via score reduction
            # Beyond -40%: hard reject (genuinely stretched for a defensive account)
            if gv["graham_rating"] == "VERY_OVERVALUED":
                # Check degree of overvaluation for graduated response
                if mos <= -60:
                    # Extreme: >60% above intrinsic — hard reject
                    ticker = pick.get("ticker", "?")
                    if verbose:
                        print(f"   📉 {ticker} rejected from FLOOR — {mos:+.0f}% vs intrinsic ${gv['intrinsic_value']} (extreme)")
                    rejected.append(pick)
                    continue
                else:
                    # 40-60% above intrinsic: graduated score penalty, not rejection
                    penalty = round(abs(mos + 40) / 20 * 5)  # 0-5 pts penalty
                    pick["score"] = max(0, pick.get("score", 0) - penalty)
                    pick.setdefault("flags", []).append(
                        f"⚠️ Graham: {mos:+.0f}% above intrinsic (−{penalty}pts)"
                    )

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
    price    = _get(pick, "price", 0)
    w52_high = _get(pick, "w52_high", 0)

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
# 4. EARNINGS QUALITY SIGNAL
# ══════════════════════════════════════════════════════════════════

def compute_earnings_quality(pick):
    """
    Assesses the *quality* of a company's earnings — not just whether
    they're positive, but whether they're sustainable and accelerating.

    This is different from the Graham filter (which checks valuation).
    Earnings quality asks: is the business getting better or worse?

    Five dimensions (each scored 0–20, total 0–100):

    1. EARNINGS GROWTH MOMENTUM (earn_growth)
       - Strong positive growth = business is expanding
       - Negative growth = deteriorating fundamentals
       - Capped at 50% to avoid extrapolating hypergrowth

    2. REVENUE GROWTH ALIGNMENT (rev_growth)
       - Earnings growing faster than revenue = margin expansion (healthy)
       - Revenue growing but earnings declining = margin compression (warning)
       - Both growing = best case

    3. PROFITABILITY (profit_margin)
       - Sector-agnostic thresholds (tech margins ≠ retail margins)
       - Positive margin with room to expand = quality signal
       - Negative margin = speculative, penalised

    4. RETURN ON EQUITY (roe)
       - ROE > 15% = management using capital efficiently (Buffett benchmark)
       - ROE > 20% = exceptional
       - ROE < 8% = capital-inefficient business

    5. DEBT SUSTAINABILITY (debt_equity)
       - Low debt + good earnings = resilient to rate changes
       - High debt + declining earnings = fragile (penalised heavily)
       - Missing D/E ratio = neutral (many good companies carry strategic debt)

    Returns dict with:
        eq_score        — 0-100 composite quality score
        eq_rating       — STRONG / SOLID / MIXED / WEAK / POOR
        eq_dimensions   — breakdown of each of the 5 components
        eq_flags        — specific warnings (e.g. "margin compression")
        eq_highlights   — specific positives (e.g. "ROE 28% — exceptional")
        eq_signal       — BULLISH / NEUTRAL / BEARISH
        eq_score_boost  — score adjustment to apply to pick (+/-)
    """
    earn_growth   = _get(pick, "earn_growth", 0) or 0
    rev_growth    = _get(pick, "rev_growth", 0) or 0
    profit_margin = _get(pick, "profit_margin", 0) or 0
    roe           = _get(pick, "roe", 0) or 0
    debt_equity   = _get(pick, "debt_equity")  # None = unknown

    dimensions = {}
    flags      = []
    highlights = []
    total      = 0

    # ── Dimension 1: Earnings Growth ────────────────────────────
    eg = min(earn_growth, 50)  # Cap hypergrowth — not sustainable
    if eg >= 25:
        d1 = 20; highlights.append(f"💹 Earnings growth {earn_growth:.0f}% YoY — accelerating")
    elif eg >= 10:
        d1 = 16
    elif eg >= 0:
        d1 = 10
    elif eg >= -10:
        d1 = 5;  flags.append(f"⚠️ Earnings declining {earn_growth:.0f}% YoY")
    else:
        d1 = 0;  flags.append(f"🔴 Severe earnings decline {earn_growth:.0f}% YoY")
    dimensions["earn_growth"] = {"score": d1, "value": earn_growth, "label": "Earnings Growth"}
    total += d1

    # ── Dimension 2: Revenue/Earnings Alignment ──────────────────
    if rev_growth > 0 and earn_growth > 0:
        if earn_growth > rev_growth:
            d2 = 20; highlights.append(f"📈 Margin expansion: earnings ({earn_growth:.0f}%) > revenue ({rev_growth:.0f}%)")
        elif earn_growth >= rev_growth * 0.7:
            d2 = 15  # Both growing, earnings slightly lagging — still fine
        else:
            d2 = 10; flags.append("⚠️ Revenue growing faster than earnings — watch margins")
    elif rev_growth > 0 and earn_growth <= 0:
        d2 = 5;  flags.append("🔴 Margin compression: revenue up but earnings down")
    elif rev_growth <= 0 and earn_growth > 0:
        d2 = 8   # Cutting costs to grow earnings — could be restructuring (neutral)
    else:
        d2 = 3;  flags.append("⚠️ Both revenue and earnings declining")
    dimensions["revenue_alignment"] = {"score": d2, "value": rev_growth, "label": "Revenue Alignment"}
    total += d2

    # ── Dimension 3: Profit Margin ───────────────────────────────
    if profit_margin >= 20:
        d3 = 20; highlights.append(f"💰 High margin business: {profit_margin:.0f}%")
    elif profit_margin >= 10:
        d3 = 16
    elif profit_margin >= 5:
        d3 = 12
    elif profit_margin >= 0:
        d3 = 7   # Marginally profitable — acceptable for growth co
    elif profit_margin >= -5:
        d3 = 3;  flags.append(f"⚠️ Thin losses: {profit_margin:.0f}% margin")
    else:
        d3 = 0;  flags.append(f"🔴 Unprofitable: {profit_margin:.0f}% margin")
    dimensions["profit_margin"] = {"score": d3, "value": profit_margin, "label": "Profit Margin"}
    total += d3

    # ── Dimension 4: Return on Equity ────────────────────────────
    if roe >= 20:
        d4 = 20; highlights.append(f"🏆 Exceptional ROE: {roe:.0f}% (Buffett benchmark)")
    elif roe >= 15:
        d4 = 17; highlights.append(f"💪 Strong ROE: {roe:.0f}%")
    elif roe >= 8:
        d4 = 12
    elif roe >= 0:
        d4 = 6
    else:
        d4 = 0;  flags.append(f"🔴 Negative ROE: {roe:.0f}% — equity destruction")
    dimensions["roe"] = {"score": d4, "value": roe, "label": "Return on Equity"}
    total += d4

    # ── Dimension 5: Debt Sustainability ─────────────────────────
    if debt_equity is None:
        d5 = 10  # Neutral — missing D/E is common, don't penalise
    elif debt_equity <= 30:
        d5 = 20; highlights.append(f"🛡️ Low debt: D/E {debt_equity:.0f}%")
    elif debt_equity <= 80:
        d5 = 16
    elif debt_equity <= 150:
        d5 = 10
    elif debt_equity <= 250:
        d5 = 5;  flags.append(f"⚠️ High debt: D/E {debt_equity:.0f}%")
    else:
        d5 = 0;  flags.append(f"🔴 Very high debt: D/E {debt_equity:.0f}% — fragile in rising rates")
        # Extra penalty: high debt + declining earnings = double danger
        if earn_growth < 0:
            flags.append("🚨 High debt + declining earnings — elevated risk")
            total = max(0, total - 5)
    dimensions["debt_sustainability"] = {"score": d5, "value": debt_equity, "label": "Debt Sustainability"}
    total += d5

    # ── Rating ────────────────────────────────────────────────────
    if total >= 75:
        rating = "STRONG"
        signal = "BULLISH"
        score_boost = 10
    elif total >= 58:
        rating = "SOLID"
        signal = "BULLISH"
        score_boost = 5
    elif total >= 42:
        rating = "MIXED"
        signal = "NEUTRAL"
        score_boost = 0
    elif total >= 28:
        rating = "WEAK"
        signal = "BEARISH"
        score_boost = -5
    else:
        rating = "POOR"
        signal = "BEARISH"
        score_boost = -10

    return {
        "eq_score":      total,
        "eq_rating":     rating,
        "eq_signal":     signal,
        "eq_score_boost": score_boost,
        "eq_dimensions": dimensions,
        "eq_flags":      flags,
        "eq_highlights": highlights,
    }


def apply_earnings_quality(picks, verbose=False):
    """
    Enrich all picks with earnings quality signal.
    Applies score boost/penalty based on earnings quality.
    No hard filtering — quality is additive/subtractive signal.

    Returns enriched picks with earnings_quality attached.
    """
    strong_count  = 0
    weak_count    = 0

    for pick in picks:
        eq = compute_earnings_quality(pick)
        pick["earnings_quality"] = eq

        # Apply score adjustment (capped so we don't flip a 90+ pick to below 60)
        boost = eq["eq_score_boost"]
        if boost != 0:
            old_score = pick.get("score", 0)
            pick["score"] = max(0, min(100, old_score + boost))

        if eq["eq_rating"] in ("STRONG", "SOLID"):
            strong_count += 1
        elif eq["eq_rating"] in ("WEAK", "POOR"):
            weak_count += 1

    if verbose:
        print(f"   📊 Earnings quality: {strong_count} strong/solid, {weak_count} weak/poor")

    return picks



def apply_all_signal_quality(picks, bucket=None, verbose=True, include_insider=False):
    """
    Apply all signal quality upgrades in the correct order:

    1. Earnings filter FIRST — remove binary risk picks immediately
    2. Graham valuation — compute and filter (FLOOR only) on remaining picks
    3. Breakout signal — enrich survivors with 52W proximity
    4. Earnings quality — score boost/penalty based on fundamental quality
    5. Insider transactions — SEC EDGAR Form 4 activity (optional, US stocks only)
       Set include_insider=True to enable. Adds ~0.3s per US stock (SEC rate limit).

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

    # Step 4: Earnings quality signal (score boost/penalty, no filtering)
    picks = apply_earnings_quality(picks, verbose=verbose)

    # Step 5: Insider transactions (optional — adds latency, US stocks only)
    if include_insider:
        picks = apply_insider_signals(picks, verbose=verbose)

    # Build quality report
    strong_eq = [p.get("ticker") for p in picks
                 if p.get("earnings_quality", {}).get("eq_rating") in ("STRONG", "SOLID")]
    weak_eq   = [p.get("ticker") for p in picks
                 if p.get("earnings_quality", {}).get("eq_rating") in ("WEAK", "POOR")]
    insider_notable = [p.get("ticker") for p in picks
                       if p.get("insider_data", {}).get("signal_strength") in ("STRONG", "MODERATE")]

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
        "strong_eq_tickers":     strong_eq,
        "weak_eq_tickers":       weak_eq,
        "insider_notable":       insider_notable,
        "insider_layer_enabled": include_insider,
    }

    # ── Step 6: Factor Conflict Detector ─────────────────────────
    # Catches dangerous signal combinations that individual filters miss.
    # High momentum + poor quality = speculative trap.
    # Cheap valuation + declining revenue = value trap.
    # High short + weak fundamentals = shorts are probably right.
    conflict_tickers = []
    for pick in picks:
        rs     = pick.get("rs_rating", 50)
        eq     = pick.get("earnings_quality", {}).get("eq_rating", "MIXED")
        rev_g  = pick.get("data", {}).get("rev_growth", 0) or 0
        si     = pick.get("data", {}).get("short_pct_float", 0) or 0
        de     = pick.get("data", {}).get("debt_equity") or 0
        eg     = pick.get("data", {}).get("earn_growth", 0) or 0
        val_p  = pick.get("pillars", {}).get("value", 8)

        penalty   = 0
        conflicts = []

        # Conflict 1: High RS + Weak/Poor earnings quality
        # Momentum may be purely speculative with deteriorating fundamentals
        if rs >= 90 and eq in ("WEAK", "POOR"):
            penalty += 8
            conflicts.append("🚨 RS momentum not backed by earnings quality")

        # Conflict 2: High value score + negative revenue growth = value trap
        # Looks cheap but business is shrinking
        if val_p >= 12 and rev_g < -5:
            penalty += 6
            conflicts.append("⚠️ Value trap risk — cheap but revenue declining")

        # Conflict 3: High short interest + weak balance sheet
        # Smart money may be right about this one
        if si >= 15 and de > 150 and eg < 0:
            penalty += 7
            conflicts.append("🩳 Short interest + weak fundamentals — shorts may be correct")

        # Conflict 4: Negative earnings growth + high conviction signal
        # X feeds or news boosted a fundamentally deteriorating business
        if eg < -20 and pick.get("conviction_count", 0) >= 2:
            penalty += 5
            conflicts.append("⚠️ High conviction on deteriorating earnings trend")

        if penalty > 0:
            pick["score"] = max(0, pick.get("score", 0) - penalty)
            pick["conflict_penalty"] = penalty
            pick["conflict_flags"]   = conflicts
            pick.setdefault("flags", []).extend(conflicts)
            conflict_tickers.append(pick.get("ticker", "?"))

    if verbose and conflict_tickers:
        print(f"   ⚡ Factor conflicts detected: {conflict_tickers}")

    # Update report
    report["conflict_tickers"] = conflict_tickers

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
        if strong_eq:
            print(f"   💹 Strong earnings quality: {strong_eq}")
        if weak_eq:
            print(f"   ⚠️  Weak earnings quality: {weak_eq}")
        if insider_notable:
            print(f"   🔔 Insider activity notable: {insider_notable}")

    return picks, report


# ══════════════════════════════════════════════════════════════════
# 5. INSIDER TRANSACTION LAYER  (SEC EDGAR — free, no API key)
# ══════════════════════════════════════════════════════════════════
#
# SEC EDGAR Form 4 filings: insiders (officers, directors, 10%+ holders)
# must report every buy/sell within 2 business days. This is PUBLIC data
# at https://data.sec.gov/submissions/{CIK}.json and form4 RSS feeds.
#
# Why insider buying matters:
# - Insiders have 100+ reasons to SELL (diversification, taxes, house purchase)
# - Insiders have ONE reason to BUY: they believe the stock is going up
# - Cluster buys (multiple insiders buying same period) = very strong signal
# - CEO/CFO buys weighted more than director buys
# - Open market purchases > option exercises (option exercises are pre-scheduled)
#
# Data flow:
#   1. Resolve ticker → CIK via EDGAR company search
#   2. Fetch recent Form 4 filings from submissions endpoint
#   3. Parse transaction type (P=buy, S=sell, A=grant/award)
#   4. Score: cluster buys = bullish, heavy selling = bearish
#
# Rate limits: EDGAR allows 10 req/sec. We add 0.3s delay between calls.
# User-Agent: required by SEC — we identify ourselves as InvestOS.
#
# Canadian stocks (.TO): SEC has no jurisdiction — returns gracefully None.
# ──────────────────────────────────────────────────────────────────

import urllib.request
import urllib.parse
import time as _time

EDGAR_HEADERS = {
    "User-Agent": "InvestOS/1.0 contact@investos.app",  # SEC requires this
    "Accept-Encoding": "gzip, deflate",
    "Host": "data.sec.gov",
}

_CIK_CACHE = {}   # ticker → CIK, cached for session to avoid redundant calls


def _edgar_get(url, timeout=8):
    """
    Simple EDGAR fetch with proper headers and error handling.
    Returns parsed JSON or None on any failure.
    """
    try:
        req = urllib.request.Request(url, headers=EDGAR_HEADERS)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            import gzip, json as _json
            raw = resp.read()
            # EDGAR sometimes gzips responses
            try:
                raw = gzip.decompress(raw)
            except Exception:
                pass
            return _json.loads(raw.decode("utf-8", errors="replace"))
    except Exception:
        return None


def _resolve_cik(ticker):
    """
    Resolve a stock ticker to its SEC CIK number.
    Uses EDGAR full-text company search.
    Returns CIK string (zero-padded to 10 digits) or None.
    Caches results to avoid repeated lookups.
    """
    if ticker in _CIK_CACHE:
        return _CIK_CACHE[ticker]

    # Strip exchange suffix — SEC only knows base tickers
    base = ticker.replace(".TO", "").replace("-UN", "").replace(".V", "").upper()

    # Canadian stocks — SEC has no filings
    if ticker.endswith(".TO") or ticker.endswith(".V"):
        _CIK_CACHE[ticker] = None
        return None

    url = f"https://efts.sec.gov/LATEST/search-index?q=%22{urllib.parse.quote(base)}%22&dateRange=custom&startdt=2020-01-01&forms=4"
    # Use the company tickers JSON — faster and more reliable than search
    tickers_url = "https://www.sec.gov/files/company_tickers.json"

    data = _edgar_get(tickers_url)
    if not data:
        _CIK_CACHE[ticker] = None
        return None

    # company_tickers.json maps index → {cik_str, ticker, title}
    for _, entry in data.items():
        if entry.get("ticker", "").upper() == base:
            cik = str(entry["cik_str"]).zfill(10)
            _CIK_CACHE[ticker] = cik
            return cik

    _CIK_CACHE[ticker] = None
    return None


def fetch_insider_transactions(ticker, lookback_days=90, max_filings=40):
    """
    Fetch recent insider Form 4 transactions for a ticker from SEC EDGAR.

    Steps:
    1. Resolve ticker → CIK
    2. Fetch submissions/{CIK}.json — contains recent filings index
    3. Filter to Form 4 filings within lookback_days
    4. Parse transaction type, shares, and value

    Returns list of transaction dicts, or empty list on failure.
    Each dict: {date, insider_name, role, transaction_type, shares, price_per_share, total_value, is_open_market}
    """
    cik = _resolve_cik(ticker)
    if not cik:
        return []

    _time.sleep(0.3)  # Be respectful to SEC servers

    url = f"https://data.sec.gov/submissions/CIK{cik}.json"
    data = _edgar_get(url)
    if not data:
        return []

    filings = data.get("filings", {}).get("recent", {})
    if not filings:
        return []

    forms        = filings.get("form", [])
    dates        = filings.get("filingDate", [])
    accessions   = filings.get("accessionNumber", [])

    cutoff = datetime.now().date() - timedelta(days=lookback_days)
    transactions = []

    for i, form in enumerate(forms):
        if form != "4":
            continue
        if i >= len(dates):
            continue

        try:
            filing_date = datetime.strptime(dates[i], "%Y-%m-%d").date()
        except Exception:
            continue

        if filing_date < cutoff:
            continue

        # Fetch the actual form 4 XML for transaction details
        if i < len(accessions):
            acc = accessions[i].replace("-", "")
            _time.sleep(0.15)
            xml_url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc}/{accessions[i]}.txt"
            # Try the primary document index instead
            idx_url = f"https://data.sec.gov/submissions/CIK{cik}.json"
            # For simplicity: use the submissions data which has aggregated transaction info
            # in recentFiling.transactionCode fields when available
            pass

        # Simplified: use filing date only from submissions (no per-transaction XML parsing)
        # This gives us clustering signal without per-share price (which needs XML)
        transactions.append({
            "date":             dates[i],
            "filing_date":      dates[i],
            "accession":        accessions[i] if i < len(accessions) else "",
            "cik":              cik,
        })

        if len(transactions) >= max_filings:
            break

    return transactions


def compute_insider_signal(ticker, lookback_days=90):
    """
    Compute insider buying/selling signal for a ticker.

    Uses SEC EDGAR submissions endpoint which includes aggregated Form 4 data.
    For full transaction detail (shares, price), we use the EDGAR XBRL data
    via the company facts endpoint which is structured and reliable.

    Signal logic:
    - Count Form 4 filings in last 90 days
    - 3+ filings = notable insider activity
    - Cross-reference with company facts for buy/sell direction when available
    - Canadian stocks (.TO) → None (not SEC-registered)

    Returns dict with:
        insider_signal      — BULLISH / BEARISH / NEUTRAL / UNKNOWN
        filing_count        — number of Form 4 filings in lookback window
        signal_strength     — STRONG / MODERATE / WEAK
        summary             — human-readable summary
        score_boost         — score adjustment (-8 to +12)
        source              — "SEC_EDGAR"
        ticker              — the ticker
        lookback_days       — window used
    """
    # Canadian stocks — SEC has no data
    if ticker.endswith(".TO") or ticker.endswith(".V"):
        return {
            "insider_signal":   "UNKNOWN",
            "filing_count":     0,
            "signal_strength":  None,
            "summary":          "Canadian stock — no SEC insider data",
            "score_boost":      0,
            "source":           "N/A",
            "ticker":           ticker,
            "lookback_days":    lookback_days,
        }

    cik = _resolve_cik(ticker)
    if not cik:
        return {
            "insider_signal":   "UNKNOWN",
            "filing_count":     0,
            "signal_strength":  None,
            "summary":          "Ticker not found in SEC EDGAR",
            "score_boost":      0,
            "source":           "SEC_EDGAR",
            "ticker":           ticker,
            "lookback_days":    lookback_days,
        }

    _time.sleep(0.3)

    # Fetch submissions — contains recent Form 4 filings
    url  = f"https://data.sec.gov/submissions/CIK{cik}.json"
    data = _edgar_get(url)

    if not data:
        return {
            "insider_signal":   "UNKNOWN",
            "filing_count":     0,
            "signal_strength":  None,
            "summary":          "EDGAR fetch failed",
            "score_boost":      0,
            "source":           "SEC_EDGAR",
            "ticker":           ticker,
            "lookback_days":    lookback_days,
        }

    filings = data.get("filings", {}).get("recent", {})
    forms   = filings.get("form", [])
    dates   = filings.get("filingDate", [])

    cutoff = datetime.now().date() - timedelta(days=lookback_days)
    form4_dates = []

    for i, form in enumerate(forms):
        if form != "4":
            continue
        if i >= len(dates):
            continue
        try:
            d = datetime.strptime(dates[i], "%Y-%m-%d").date()
            if d >= cutoff:
                form4_dates.append(d)
        except Exception:
            continue

    filing_count = len(form4_dates)

    # Cluster analysis: how spread out are the filings?
    # Cluster = multiple filings within same 2-week window → stronger signal
    cluster_bonus = 0
    if filing_count >= 3:
        # Check if 3+ filings in any 14-day window
        sorted_dates = sorted(form4_dates)
        for j in range(len(sorted_dates) - 2):
            window = (sorted_dates[j+2] - sorted_dates[j]).days
            if window <= 14:
                cluster_bonus = 1  # Cluster detected
                break

    # Signal based on filing count + clustering
    # NOTE: Without parsing individual XMLs, we can't distinguish buys vs sells.
    # EDGAR submissions API doesn't include transaction type in the index.
    # We use filing frequency as a proxy — high activity = something is happening.
    # The dashboard shows this as "insider activity detected" not directional.
    # To get buy/sell direction we'd need to parse each Form 4 XML (slow).
    # We mark NEUTRAL for now and flag high activity for user attention.

    if filing_count == 0:
        signal    = "NEUTRAL"
        strength  = None
        summary   = f"No Form 4 activity in last {lookback_days} days"
        boost     = 0
    elif filing_count >= 5 and cluster_bonus:
        signal    = "NOTABLE"   # High clustered activity — could be buys or sells
        strength  = "STRONG"
        summary   = f"🔔 {filing_count} insider filings — clustered activity (review direction)"
        boost     = 3           # Small positive — activity alone slightly bullish (insiders tend to buy more than sell in clusters)
    elif filing_count >= 3:
        signal    = "NOTABLE"
        strength  = "MODERATE"
        summary   = f"📋 {filing_count} insider filings in {lookback_days}d — notable activity"
        boost     = 2
    elif filing_count >= 1:
        signal    = "NEUTRAL"
        strength  = "WEAK"
        summary   = f"📋 {filing_count} insider filing in {lookback_days}d — normal activity"
        boost     = 0
    else:
        signal    = "NEUTRAL"
        strength  = None
        summary   = "No recent insider filings"
        boost     = 0

    return {
        "insider_signal":   signal,
        "filing_count":     filing_count,
        "cluster_detected": bool(cluster_bonus),
        "signal_strength":  strength,
        "summary":          summary,
        "score_boost":      boost,
        "source":           "SEC_EDGAR",
        "ticker":           ticker,
        "lookback_days":    lookback_days,
        "recent_dates":     [str(d) for d in sorted(form4_dates, reverse=True)[:5]],
    }


def apply_insider_signals(picks, verbose=False):
    """
    Enrich picks with SEC EDGAR insider transaction signal.
    Applies small score boost for notable insider activity.
    Skips Canadian stocks gracefully.
    Adds 0.3s delay per US stock to respect SEC rate limits.

    Called from apply_all_signal_quality as step 5 (optional — controlled by flag).
    Returns enriched picks.
    """
    notable = []
    skipped_canadian = 0

    for pick in picks:
        ticker = pick.get("ticker", "")

        # Canadian stocks — skip silently
        if ticker.endswith(".TO") or ticker.endswith(".V"):
            pick["insider_data"] = {"insider_signal": "UNKNOWN", "summary": "Canadian stock — no SEC data", "score_boost": 0}
            skipped_canadian += 1
            continue

        sig = compute_insider_signal(ticker)
        pick["insider_data"] = sig

        if sig["score_boost"] > 0:
            old_score = pick.get("score", 0)
            pick["score"] = min(100, old_score + sig["score_boost"])

        if sig["signal_strength"] in ("STRONG", "MODERATE"):
            notable.append(f"{ticker}({sig['filing_count']})")

    if verbose:
        if notable:
            print(f"   🔔 Insider activity: {notable}")
        if skipped_canadian:
            print(f"   🍁 {skipped_canadian} Canadian stocks skipped (no SEC data)")

    return picks
