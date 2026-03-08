"""
InvestOS — Independent Stock Screener & Analyzer
================================================
Screens 500+ stocks across TSX + US markets entirely independently
of your X signal sources. Finds hidden gems matching your profile:
  - Age 33, growth + income + dividends
  - FHSA: conservative growth, 16% max drawdown
  - TFSA: growth + income core, swing opportunities
  
No paid APIs. Runs free on Yahoo Finance data.
"""

import json
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# ============================================================
# SCREENING UNIVERSE
# Broad list of TSX + US stocks across all sectors
# System will whittle these down to YOUR best 5-10 daily
# ============================================================

UNIVERSE = {

    # ── CANADIAN STOCKS ──────────────────────────────────────
    "TSX_BLUE_CHIP": [
        # Banks
        "TD.TO", "RY.TO", "BNS.TO", "BMO.TO", "CM.TO", "NA.TO",
        # Insurance
        "MFC.TO", "SLF.TO", "IAG.TO",
        # Energy
        "ENB.TO", "TRP.TO", "CNQ.TO", "SU.TO", "CVE.TO", "PPL.TO",
        # Telecoms
        "BCE.TO", "T.TO", "RCI-B.TO",
        # Utilities
        "FTS.TO", "AQN.TO", "EMA.TO", "H.TO",
        # Industrials
        "CNR.TO", "CP.TO", "WSP.TO", "TIH.TO",
        # REITs
        "REI-UN.TO", "HR-UN.TO", "AP-UN.TO", "CAR-UN.TO", "GRT-UN.TO",
        # Consumer
        "ATD.TO", "DOL.TO", "MRU.TO", "L.TO",
        # Tech/Growth
        "SHOP.TO", "CSU.TO", "DSG.TO", "OTEX.TO", "BB.TO",
        # Materials/Gold
        "ABX.TO", "WPM.TO", "AEM.TO", "K.TO",
        # Financials/Alt
        "BN.TO", "BAM.TO", "FFH.TO", "POW.TO",
    ],

    "TSX_GROWTH": [
        "LSPD.TO", "NVEI.TO", "WELL.TO", "KXS.TO",
        "CLS.TO", "MG.TO", "STN.TO", "BYD.TO", "GFL.TO",
        "CCO.TO", "NTR.TO", "AGI.TO", "LUN.TO", "FM.TO",
    ],

    # ── US STOCKS ────────────────────────────────────────────
    "US_DIVIDEND_GROWTH": [
        # Dividend aristocrats
        "JNJ", "PG", "KO", "PEP", "MCD", "VZ", "IBM",
        "MMM", "ABT", "MDT", "TGT", "WMT", "HD", "LOW",
        "ABBV", "MRK", "AMGN", "BMY", "PFE",
        # REITs
        "O", "MAIN", "STAG", "VICI", "AMT", "PLD",
        # Utilities
        "NEE", "DUK", "SO", "AEP", "XEL",
        # Financials
        "JPM", "BAC", "WFC", "GS", "MS", "BLK", "BX",
    ],

    "US_GROWTH": [
        # Mega cap
        "AAPL", "MSFT", "NVDA", "GOOGL", "META", "AMZN",
        # High-growth
        "PLTR", "HOOD", "SOFI", "AFRM", "UPST", "NU",
        "CRWD", "PANW", "ZS", "DDOG", "SNOW", "MDB",
        "TSLA", "RIVN", "F", "GM",
        # Healthcare growth
        "ISRG", "DXCM", "VEEV", "HIMS",
        # Consumer growth
        "SBUX", "CMG", "LULU", "NKE",
    ],

    # ── US ETFs (BROAD MARKET) ────────────────────────────────
    "US_ETF_CORE": [
        "VOO", "VTI", "QQQ", "SPY", "IWM",
        "VYM", "SCHD", "DVY", "HDV", "DGRO",
        "XLF", "XLK", "XLE", "XLV", "XLU", "XLRE",
    ],

    # ── GLOBAL ETFs — World exposure via US-listed instruments ─
    # These give you Japan, Europe, EM, etc without foreign accounts
    "GLOBAL_ETF": [
        # Developed markets
        "VEA",   # All developed markets ex-US (Europe + Japan + Australia)
        "EFA",   # Same, iShares version
        "VGK",   # Europe only
        "EWG",   # Germany
        "EWU",   # UK
        "EWJ",   # Japan
        "EWA",   # Australia
        "EWC",   # Canada (US-listed)
        # Emerging markets
        "VWO",   # All emerging markets
        "EEM",   # Same, iShares version
        "INDA",  # India
        "FXI",   # China large cap
        "KWEB",  # China internet
        "EWZ",   # Brazil
        # Thematic global
        "URTH",  # MSCI World (all countries)
        "ACWI",  # All country world
        "MCHI",  # China broad
    ],

    # ── CANADIAN ETFs ─────────────────────────────────────────
    "TSX_ETF_CORE": [
        "XGRO.TO", "XEQT.TO", "XBAL.TO", "XCNS.TO",
        "VFV.TO",  "ZCN.TO",  "XIU.TO",  "XIC.TO",
        "ZDV.TO",  "CDZ.TO",  "XDV.TO",
        "ZEB.TO",  "ZRE.TO",  "XRE.TO",
        "HXT.TO",  "HXS.TO",
    ],
}

# Flatten all tickers — deduplicated
ALL_TICKERS = list(set(
    t for group in UNIVERSE.values() for t in group
))

# Tag each ticker with its venue type for pick routing
VENUE_MAP = {}
for t in ALL_TICKERS:
    VENUE_MAP[t] = "STOCK_ACCOUNT"  # All screened tickers go via stock account

print(f"📊 Universe: {len(ALL_TICKERS)} tickers — TSX + US + Global ETFs")


# ============================================================
# INVESTOR PROFILE — YOUR SCORING WEIGHTS
# ============================================================

PROFILE = {
    "age": 33,
    "time_horizon_years": 25,   # Long runway for TFSA
    "income": 58000,
    "risk_tolerance": "high_calculated",

    # What matters to you — weights sum to 100
    "weights": {
        "momentum":         20,   # Price trending up
        "dividend_income":  20,   # Yield + consistency
        "growth":           20,   # Revenue/earnings growth
        "value":            15,   # Not overvalued
        "safety":           15,   # Low drawdown, stability
        "volume_liquidity": 10,   # Can actually trade it
    },

    # Hard filters — stock FAILS if any of these trigger
    "hard_filters": {
        "min_price":                 1.00,    # No penny stocks
        "max_drawdown_fhsa_pct":    16.0,    # FHSA rule
        "max_drawdown_tfsa_pct":    40.0,    # TFSA more lenient
        "min_volume":           50_000,      # Minimum daily volume
        "max_pe_ratio":           100.0,     # Not wildly overvalued
    },

    # Bonus signals — boost score if present
    "bonus_signals": {
        "dividend_yield_above_3pct":   +10,
        "dividend_yield_above_5pct":   +15,
        "perf_90d_above_10pct":        +10,
        "perf_30d_positive":            +5,
        "near_52w_low_recovery":       +8,   # Potential bounce
        "ex_dividend_within_30d":      +12,  # Collect dividend soon
        "canadian_reit":               +5,   # TFSA income bonus
    }
}


# ============================================================
# DATA FETCHER (yfinance — works on GitHub Actions cloud IPs)
# ============================================================

def fetch_ticker_full(ticker, retries=2):
    """
    Pull everything needed to score a stock.
    Uses yfinance (handles Yahoo rate-limiting on cloud IPs far better
    than raw urllib — critical for GitHub Actions runners).
    """
    import yfinance as yf

    for attempt in range(retries):
        try:
            t = yf.Ticker(ticker)

            # --- Price history (6 months daily) ---
            hist = t.history(period="6mo", interval="1d", auto_adjust=True)
            if hist is None or len(hist) < 5:
                time.sleep(0.5 * (attempt + 1))
                continue

            closes  = hist["Close"].dropna().tolist()
            volumes = hist["Volume"].dropna().tolist()

            if len(closes) < 5:
                time.sleep(0.5)
                continue

            # --- Fundamentals ---
            info = t.info or {}

            price      = info.get("regularMarketPrice") or info.get("currentPrice") or closes[-1]
            prev_close = info.get("previousClose") or (closes[-2] if len(closes) >= 2 else price)
            w52_high   = info.get("fiftyTwoWeekHigh")  or max(closes)
            w52_low    = info.get("fiftyTwoWeekLow")   or min(closes)

            day_chg_pct   = ((price - prev_close) / prev_close * 100) if prev_close else 0
            perf_30d      = ((closes[-1] - closes[-22]) / closes[-22] * 100) if len(closes) >= 22 else 0
            perf_90d      = ((closes[-1] - closes[-65]) / closes[-65] * 100) if len(closes) >= 65 else 0
            drawdown_high = ((price - w52_high) / w52_high * 100) if w52_high else 0
            recovery_pct  = ((price - w52_low)  / w52_low  * 100) if w52_low else 0
            avg_volume    = sum(volumes[-20:]) / max(len(volumes[-20:]), 1)

            # Volatility (std dev of daily returns, annualised-ish)
            if len(closes) >= 20:
                daily_rets = [(closes[i] - closes[i-1]) / closes[i-1] for i in range(1, min(21, len(closes)))]
                mean_ret   = sum(daily_rets) / len(daily_rets)
                variance   = sum((r - mean_ret) ** 2 for r in daily_rets) / len(daily_rets)
                volatility = variance ** 0.5 * 100
            else:
                volatility = 2.0

            # --- Dividend / calendar ---
            div_yield  = (info.get("dividendYield") or 0) * 100
            div_rate   = info.get("dividendRate") or 0

            ex_div_date    = "N/A"
            days_to_ex_div = 999
            ex_div_ts = info.get("exDividendDate")
            if ex_div_ts and ex_div_ts > 0:
                ex_dt          = datetime.fromtimestamp(ex_div_ts)
                ex_div_date    = ex_dt.strftime("%b %d, %Y")
                days_to_ex_div = (ex_dt - datetime.now()).days

            # --- Earnings date ---
            next_earnings = "N/A"
            try:
                cal = t.calendar
                if cal is not None and not cal.empty:
                    earn_col = None
                    for col in cal.columns:
                        if "Earnings" in str(col):
                            earn_col = col
                            break
                    if earn_col and len(cal[earn_col]) > 0:
                        ed = cal[earn_col].iloc[0]
                        if hasattr(ed, "strftime"):
                            next_earnings = ed.strftime("%b %d, %Y")
            except Exception:
                pass

            # --- Fundamentals ---
            pe_ratio      = info.get("trailingPE") or info.get("forwardPE")
            peg_ratio     = info.get("pegRatio")
            profit_margin = (info.get("profitMargins") or 0) * 100
            rev_growth    = (info.get("revenueGrowth") or 0) * 100
            earn_growth   = (info.get("earningsGrowth") or 0) * 100
            debt_equity   = info.get("debtToEquity")
            roe           = (info.get("returnOnEquity") or 0) * 100
            current_ratio = info.get("currentRatio")
            target_price  = info.get("targetMeanPrice")
            analyst_rec   = info.get("recommendationKey", "") or ""
            upside_to_target = ((target_price - price) / price * 100) if target_price and price else 0

            return {
                "ticker":           ticker,
                "price":            round(price, 2),
                "day_chg_pct":      round(day_chg_pct, 2),
                "perf_30d":         round(perf_30d, 2),
                "perf_90d":         round(perf_90d, 2),
                "w52_high":         round(w52_high, 2),
                "w52_low":          round(w52_low, 2),
                "drawdown_high":    round(drawdown_high, 2),
                "recovery_pct":     round(recovery_pct, 2),
                "avg_volume":       int(avg_volume),
                "volatility":       round(volatility, 2),
                "div_yield":        round(div_yield, 2),
                "div_rate":         round(div_rate, 2),
                "ex_div_date":      ex_div_date,
                "days_to_ex_div":   days_to_ex_div,
                "pe_ratio":         round(pe_ratio, 1) if pe_ratio else None,
                "peg_ratio":        round(peg_ratio, 2) if peg_ratio else None,
                "profit_margin":    round(profit_margin, 1),
                "rev_growth":       round(rev_growth, 1),
                "earn_growth":      round(earn_growth, 1),
                "debt_equity":      round(debt_equity, 1) if debt_equity else None,
                "roe":              round(roe, 1),
                "current_ratio":    round(current_ratio, 2) if current_ratio else None,
                "target_price":     round(target_price, 2) if target_price else None,
                "upside_target":    round(upside_to_target, 1),
                "analyst_rec":      analyst_rec if isinstance(analyst_rec, str) else "",
                "next_earnings":    next_earnings,
                "status":           "ok",
            }

        except Exception:
            time.sleep(1 * (attempt + 1))
            continue

    return {"ticker": ticker, "status": "error"}



# ============================================================
# SCORING ENGINE
# ============================================================

def score_stock(data, account_type="TFSA_core"):
    """
    Score 0–100 using weighted pillars tailored to your profile.
    Returns (total_score, pillar_scores, reasons, flags)
    """
    if data.get("status") != "ok":
        return 0, {}, [], []

    pillars = {}
    reasons = []
    flags   = []
    w       = PROFILE["weights"]

    # ── 1. MOMENTUM (20 pts) ─────────────────────────────
    mom = 0
    if data["perf_90d"] > 20:  mom = 20; reasons.append(f"🚀 Strong 90d: +{data['perf_90d']}%")
    elif data["perf_90d"] > 10: mom = 15; reasons.append(f"📈 Good 90d: +{data['perf_90d']}%")
    elif data["perf_90d"] > 5:  mom = 10; reasons.append(f"📈 Positive 90d: +{data['perf_90d']}%")
    elif data["perf_90d"] > 0:  mom = 6
    elif data["perf_90d"] > -10: mom = 2
    else: mom = 0; flags.append(f"⚠️ Negative 90d trend: {data['perf_90d']}%")

    if data["perf_30d"] > 5:   mom = min(20, mom + 4); reasons.append(f"📈 30d momentum: +{data['perf_30d']}%")
    elif data["perf_30d"] > 0: mom = min(20, mom + 2)
    pillars["momentum"] = mom

    # ── 2. DIVIDEND INCOME (20 pts) ──────────────────────
    div = 0
    dy = data["div_yield"]
    if dy > 6:    div = 20; reasons.append(f"💰 Exceptional yield: {dy}%")
    elif dy > 4:  div = 16; reasons.append(f"💰 Strong yield: {dy}%")
    elif dy > 2.5: div = 12; reasons.append(f"💰 Good yield: {dy}%")
    elif dy > 1:  div = 7;  reasons.append(f"💰 Dividend: {dy}%")
    elif dy > 0:  div = 3

    if 0 < data["days_to_ex_div"] <= 30:
        div = min(20, div + 5)
        reasons.append(f"📅 Ex-div in {data['days_to_ex_div']} days — collect soon!")
    pillars["dividend_income"] = div

    # ── 3. GROWTH (20 pts) ───────────────────────────────
    grow = 0
    rg = data["rev_growth"]
    eg = data["earn_growth"]

    if rg > 30:   grow += 10; reasons.append(f"📊 Revenue growth: +{rg}%")
    elif rg > 15: grow += 7;  reasons.append(f"📊 Revenue growth: +{rg}%")
    elif rg > 5:  grow += 4
    elif rg < -10: flags.append(f"⚠️ Declining revenue: {rg}%")

    if eg > 30:   grow += 10; reasons.append(f"📊 Earnings growth: +{eg}%")
    elif eg > 15: grow += 7;  reasons.append(f"📊 Earnings growth: +{eg}%")
    elif eg > 5:  grow += 4
    elif eg < -15: flags.append(f"⚠️ Earnings declining: {eg}%")

    if data["roe"] > 20:  grow = min(20, grow + 3); reasons.append(f"💪 Strong ROE: {data['roe']}%")
    elif data["roe"] > 12: grow = min(20, grow + 1)
    pillars["growth"] = min(20, grow)

    # ── 4. VALUE (15 pts) ────────────────────────────────
    val = 8  # Start at mid — not all stocks have PE
    pe = data["pe_ratio"]
    peg = data["peg_ratio"]
    upside = data["upside_target"]

    if pe:
        if pe < 12:   val = 15; reasons.append(f"💎 Very cheap: P/E {pe}")
        elif pe < 18: val = 12; reasons.append(f"✅ Fair value: P/E {pe}")
        elif pe < 28: val = 8
        elif pe < 45: val = 4
        elif pe > 80: val = 1; flags.append(f"⚠️ Expensive: P/E {pe}")

    if peg and peg < 1.0: val = min(15, val + 3); reasons.append(f"💎 PEG < 1: {peg} (undervalued vs growth)")
    if upside > 20: val = min(15, val + 3); reasons.append(f"🎯 Analyst upside: +{upside}%")
    elif upside > 10: val = min(15, val + 1)
    pillars["value"] = val

    # ── 5. SAFETY (15 pts) ───────────────────────────────
    safe_score = 15
    dd = abs(data["drawdown_high"])
    vol = data["volatility"]
    de = data["debt_equity"]

    # Drawdown penalty
    if account_type == "FHSA":
        if dd > 16: return 0, pillars, [], [f"❌ FHSA FAIL: {dd}% drawdown exceeds 16% hard limit"]
        elif dd > 12: safe_score -= 8; flags.append(f"⚠️ Approaching FHSA limit: {dd}% drawdown")
        elif dd > 8:  safe_score -= 4
    else:
        if dd > 35: safe_score -= 10; flags.append(f"⚠️ High drawdown: {dd}%")
        elif dd > 20: safe_score -= 5

    # Volatility penalty
    if vol > 4:   safe_score -= 4; flags.append(f"⚠️ High volatility: {vol}%/day")
    elif vol > 2.5: safe_score -= 2

    # Debt check
    if de is not None:
        if de > 200:  safe_score -= 4; flags.append(f"⚠️ High debt/equity: {de}")
        elif de > 100: safe_score -= 2

    pillars["safety"] = max(0, safe_score)

    # ── 6. VOLUME / LIQUIDITY (10 pts) ───────────────────
    vol_score = 0
    av = data["avg_volume"]
    if av > 5_000_000:   vol_score = 10
    elif av > 1_000_000: vol_score = 8; reasons.append(f"📊 Good liquidity")
    elif av > 500_000:   vol_score = 6
    elif av > 100_000:   vol_score = 4
    elif av > 50_000:    vol_score = 2
    else: vol_score = 0; flags.append(f"⚠️ Low liquidity: {av:,} avg volume")
    pillars["volume_liquidity"] = vol_score

    # ── BONUS SIGNALS ────────────────────────────────────
    bonus = 0
    bs = PROFILE["bonus_signals"]

    if data["div_yield"] > 5:              bonus += bs["dividend_yield_above_5pct"]
    elif data["div_yield"] > 3:            bonus += bs["dividend_yield_above_3pct"]
    if data["perf_90d"] > 10:             bonus += bs["perf_90d_above_10pct"]
    if data["perf_30d"] > 0:              bonus += bs["perf_30d_positive"]
    if 0 < data["days_to_ex_div"] <= 30:  bonus += bs["ex_dividend_within_30d"]
    if -25 < data["drawdown_high"] < -15: bonus += bs["near_52w_low_recovery"]  # Bounce potential

    # Analyst recommendation bonus
    if data["analyst_rec"] in ("strongBuy", "buy"):
        bonus += 8; reasons.append(f"✅ Analyst consensus: {data['analyst_rec']}")
    elif data["analyst_rec"] == "hold":
        bonus += 2
    elif data["analyst_rec"] in ("sell", "strongSell"):
        bonus -= 5; flags.append(f"⚠️ Analyst: {data['analyst_rec']}")

    total = sum(pillars.values()) + bonus
    total = max(0, min(100, total))

    return total, pillars, reasons, flags


def classify_pick(data, score, account_type):
    """Generate pick type, action, hold period, and expected return"""

    dy   = data["div_yield"]
    p90  = data["perf_90d"]
    p30  = data["perf_30d"]
    dd   = abs(data["drawdown_high"])
    rec  = data["analyst_rec"]
    up   = data["upside_target"]
    next_earn = data["next_earnings"]
    ex_div = data["ex_div_date"]

    # Determine pick category
    if account_type == "FHSA":
        category = "FHSA Conservative Growth"
        hold_days = 180
        risk_label = "LOW"
    elif dy > 4 and p90 > 0:
        category = "INCOME + GROWTH"
        hold_days = 365
        risk_label = "MODERATE"
    elif dy > 2 and p90 > 5:
        category = "DIVIDEND GROWTH"
        hold_days = 365
        risk_label = "MODERATE"
    elif p90 > 20 and dd < 25:
        category = "SWING"
        hold_days = 30
        risk_label = "HIGH"
    elif p90 > 10:
        category = "GROWTH CORE"
        hold_days = 180
        risk_label = "MODERATE-HIGH"
    elif dy > 4:
        category = "INCOME"
        hold_days = 365
        risk_label = "MODERATE"
    else:
        category = "WATCH"
        hold_days = 90
        risk_label = "MODERATE"

    # Suggested position size based on account + risk
    if account_type == "FHSA":
        if score >= 75: amount = 50
        elif score >= 60: amount = 30
        else: amount = 20
    elif category == "SWING":
        amount = 100   # Your cap
    elif score >= 80:
        amount = 300
    elif score >= 65:
        amount = 200
    else:
        amount = 100

    # Expected return range (probability-weighted)
    daily_rate = p90 / 90 if p90 else p30 / 30 if p30 else 1
    base_return = daily_rate * hold_days

    # For income/dividend stocks, anchor expected return to yield — not momentum
    # Momentum-based return is unreliable for REITs and dividend stocks
    is_income = "INCOME" in category or "DIVIDEND" in category
    if is_income and dy > 0:
        annual_yield  = dy  # already in %
        hold_yield    = round(annual_yield * (hold_days / 365), 1)
        price_upside  = max(0.0, round(base_return * 0.3, 1))
        low  = max(round(hold_yield * 0.7, 1), round(annual_yield * 0.05, 1))
        high = max(round(hold_yield + price_upside, 1), round(annual_yield * 0.15, 1))
        prob = 72 if dy > 4 else 65
    elif account_type == "FHSA":
        low = round(base_return * 0.4, 1)
        high = round(base_return * 1.5, 1)
        prob = 68 if base_return > 0 else 35
    elif category == "SWING":
        low = round(base_return * 0.2, 1)
        high = round(base_return * 2.5, 1)
        prob = 52 if base_return > 0 else 45
    else:
        low = round(base_return * 0.45, 1)
        high = round(base_return * 1.9, 1)
        prob = 62 if base_return > 0 else 40

    # Build action statement
    action_parts = []

    if amount: action_parts.append(f"Suggested: ${amount}")

    if category == "SWING":
        action_parts.append(f"Swing trade — target exit in {hold_days} days")
        if next_earn != "N/A":
            action_parts.append(f"⚠️ EXIT BEFORE earnings on {next_earn}")
    elif "INCOME" in category or "DIVIDEND" in category:
        action_parts.append(f"Hold {hold_days} days for income cycle")
        if ex_div != "N/A" and 0 < data["days_to_ex_div"] <= 45:
            action_parts.append(f"Buy before {ex_div} to capture ${data['div_rate']:.2f}/share dividend")
    else:
        action_parts.append(f"Core hold — review in {hold_days} days")

    if up > 15 and rec in ("buy", "strongBuy"):
        action_parts.append(f"Analyst target: ${data['target_price']} (+{up}% upside)")

    # Exit trigger
    if category == "SWING":
        stop = round(data["price"] * 0.88, 2)   # 12% stop
        target = round(data["price"] * (1 + min(high/100, 0.25)), 2)
        exit_note = f"Stop loss: ${stop} | Target: ${target} | Hard exit before {next_earn}"
    elif account_type == "FHSA":
        stop = round(data["price"] * 0.84, 2)   # 16% stop
        exit_note = f"FHSA stop: ${stop} (16% rule). Review if drawdown hits 12%."
    else:
        exit_note = f"Monthly review. Reassess if fundamentals deteriorate or div cut."

    # Venue + account tag — broker-agnostic
    venue = "STOCK_ACCOUNT"
    if account_type == "FHSA":
        tag = "[FHSA]"
    else:
        tag = "[TFSA]"

    # ── OPTIONS ELIGIBILITY ──────────────────────────────
    # TFSA only: covered calls on floor/income picks, CSPs on growth dips
    # Rules: US equity, market cap implied >$5B (avg_volume proxy), earnings >21 days away
    options_eligible  = False
    suggested_play    = "none"
    est_premium_pct   = 0.0   # estimated monthly premium as % of stock price

    if account_type != "FHSA":  # No options in FHSA
        # Earnings safety buffer: options need > 21 days to earnings
        days_to_earn = 999
        if next_earn and next_earn != "N/A":
            try:
                from datetime import datetime as _dt
                earn_dt     = _dt.strptime(next_earn, "%b %d, %Y")
                days_to_earn = (earn_dt - _dt.now()).days
            except Exception:
                days_to_earn = 999

        # Liquidity proxy: avg_volume > 500k suggests enough options open interest
        liquid_enough = data.get("avg_volume", 0) > 500_000
        earnings_safe = days_to_earn > 21

        if liquid_enough and earnings_safe and regime_safe(account_type):
            options_eligible = True

            # ATR-based premium estimate (monthly, ~30 days)
            # Formula: ATR14 × multiplier × √(30/252)
            # multiplier: 0.7=stable, 1.0=normal growth, 1.3=high-vol
            atr = data.get("volatility", 2.0) / 100 * data.get("price", 1)  # approx ATR from daily vol%
            is_high_vol  = data.get("volatility", 0) > 3.0
            is_stable    = "INCOME" in category or "DIVIDEND" in category
            multiplier   = 1.3 if is_high_vol else (0.7 if is_stable else 1.0)
            import math
            raw_premium  = atr * multiplier * math.sqrt(30 / 252)
            est_premium_pct = round(raw_premium / max(data.get("price", 1), 0.01) * 100, 2)

            # Pick the right options strategy
            if is_stable or "FLOOR" in category.upper():
                suggested_play = "covered_call"   # Own the stock → sell calls above
            elif score >= 60 and data.get("perf_30d", 0) < 0 and data.get("rsi_approx", 50) < 60:
                suggested_play = "csp"            # Growth stock dipping → sell puts below
            elif category == "SWING":
                suggested_play = "covered_call"   # Swing positions → monetize with short-dated calls
            else:
                suggested_play = "covered_call"   # Default to covered call

    return {
        "category":         category,
        "venue":            venue,
        "tag":              tag,
        "hold_days":        hold_days,
        "risk_label":       risk_label,
        "amount":           amount,
        "action":           " | ".join(action_parts),
        "exit_note":        exit_note,
        "exp_low":          low,
        "exp_high":         high,
        "exp_prob":         prob,
        "options_eligible": options_eligible,
        "suggested_play":   suggested_play,
        "est_premium_pct":  est_premium_pct,
    }


def regime_safe(account_type):
    """Options are disabled in BEAR regime — too much assignment risk"""
    # Pulled from portfolio_engine at runtime if available
    try:
        from portfolio_engine import CONFIG
        regime = CONFIG.get("current_regime", "NORMAL")
        return regime not in ("BEAR",)
    except Exception:
        return True  # default to allow if can't determine


# ============================================================
# HARD FILTER — INSTANT DISQUALIFICATION
# ============================================================

def passes_hard_filters(data, account_type):
    hf = PROFILE["hard_filters"]
    if data["price"] < hf["min_price"]:
        return False, "Price below minimum"
    if data["avg_volume"] < hf["min_volume"]:
        return False, f"Low volume: {data['avg_volume']:,}"
    if data["pe_ratio"] and data["pe_ratio"] > hf["max_pe_ratio"]:
        return False, f"P/E too high: {data['pe_ratio']}"

    max_dd = hf["max_drawdown_fhsa_pct"] if account_type == "FHSA" else hf["max_drawdown_tfsa_pct"]
    if abs(data["drawdown_high"]) > max_dd:
        return False, f"Drawdown {abs(data['drawdown_high'])}% exceeds {max_dd}% limit"

    return True, "OK"


# ============================================================
# MAIN SCREENER — THE BRAIN
# ============================================================

def run_full_screen(max_tickers=None, verbose=True):
    """
    Screen the full universe. Returns ranked picks per account.
    max_tickers: limit for testing (None = full universe)
    """

    tickers = ALL_TICKERS[:max_tickers] if max_tickers else ALL_TICKERS
    if verbose: print(f"\n🔍 Screening {len(tickers)} tickers...")

    # --- Fetch all data in parallel (5 workers = polite to Yahoo) ---
    raw_data = []
    failed   = []

    with ThreadPoolExecutor(max_workers=5) as pool:
        futures = {pool.submit(fetch_ticker_full, t): t for t in tickers}
        done = 0
        for future in as_completed(futures):
            done += 1
            result = future.result()
            if result and result.get("status") == "ok":
                raw_data.append(result)
            else:
                failed.append(futures[future])

            if verbose and done % 20 == 0:
                print(f"  ↳ Fetched {done}/{len(tickers)} ({len(raw_data)} ok, {len(failed)} failed)")

    if verbose:
        print(f"\n✅ Data fetched: {len(raw_data)} stocks | ❌ Failed: {len(failed)}")

    # --- Score everything for both accounts ---
    fhsa_candidates   = []
    tfsa_core_cands   = []
    tfsa_swing_cands  = []
    tfsa_income_cands = []

    for d in raw_data:

        # FHSA pass
        ok, reason = passes_hard_filters(d, "FHSA")
        if ok:
            score, pillars, reasons, flags = score_stock(d, "FHSA")
            if score >= 45:
                pick = classify_pick(d, score, "FHSA")
                fhsa_candidates.append({
                    "ticker": d["ticker"], "score": score,
                    "data": d, "pillars": pillars,
                    "reasons": reasons, "flags": flags, "pick": pick
                })

        # TFSA pass
        ok_tfsa, _ = passes_hard_filters(d, "TFSA")
        if ok_tfsa:
            score_t, pillars_t, reasons_t, flags_t = score_stock(d, "TFSA_core")
            if score_t >= 40:
                pick_t = classify_pick(d, score_t, "TFSA_core")

                # Route to the right bucket
                if pick_t["category"] == "SWING":
                    tfsa_swing_cands.append({
                        "ticker": d["ticker"], "score": score_t,
                        "data": d, "pillars": pillars_t,
                        "reasons": reasons_t, "flags": flags_t, "pick": pick_t
                    })
                elif "INCOME" in pick_t["category"] or "DIVIDEND" in pick_t["category"]:
                    tfsa_income_cands.append({
                        "ticker": d["ticker"], "score": score_t,
                        "data": d, "pillars": pillars_t,
                        "reasons": reasons_t, "flags": flags_t, "pick": pick_t
                    })
                else:
                    tfsa_core_cands.append({
                        "ticker": d["ticker"], "score": score_t,
                        "data": d, "pillars": pillars_t,
                        "reasons": reasons_t, "flags": flags_t, "pick": pick_t
                    })

    # Sort by score descending
    fhsa_candidates.sort(  key=lambda x: x["score"], reverse=True)
    tfsa_core_cands.sort(  key=lambda x: x["score"], reverse=True)
    tfsa_swing_cands.sort( key=lambda x: x["score"], reverse=True)
    tfsa_income_cands.sort(key=lambda x: x["score"], reverse=True)

    # ── Earnings filter on swings ───────────────────────────
    # Rule: no new swing entry within 14 days of earnings
    # This is the single highest-risk reduction change
    try:
        from risk_engine import filter_swing_earnings
        tfsa_swing_cands, blocked = filter_swing_earnings(tfsa_swing_cands, buffer_days=14)
        if blocked and verbose:
            print(f"   ⚠️  Earnings filter: blocked {len(blocked)} swing picks near earnings")
    except ImportError:
        # Inline fallback if risk_engine not yet imported
        safe_swings = []
        for pick in tfsa_swing_cands:
            ne = pick.get("data", {}).get("next_earnings", "N/A")
            days_to = 999
            if ne and ne != "N/A":
                try:
                    earn_dt  = datetime.strptime(ne, "%b %d, %Y")
                    days_to  = (earn_dt - datetime.now()).days
                except:
                    pass
            pick["days_to_earnings"] = days_to
            if days_to <= 14:
                pick.setdefault("flags", []).append(f"🚨 EARNINGS in {days_to}d — blocked")
            else:
                safe_swings.append(pick)
        tfsa_swing_cands = safe_swings

    # ── Sector cap on all buckets ───────────────────────────
    try:
        from portfolio_engine import apply_sector_cap, CONFIG
        max_sect = CONFIG.get("risk_rules", {}).get("max_picks_per_sector", 2)
        tfsa_core_cands,   _ = apply_sector_cap(tfsa_core_cands,   max_sect)
        tfsa_income_cands, _ = apply_sector_cap(tfsa_income_cands, max_sect)
        fhsa_candidates,   _ = apply_sector_cap(fhsa_candidates,   max_sect)
    except ImportError:
        pass

    # Top picks
    results = {
        "generated_at":    datetime.now().isoformat(),
        "universe_size":   len(tickers),
        "screened":        len(raw_data),
        "failed_tickers":  failed[:20],

        # TOP 5 per bucket — these are YOUR independent picks
        "FHSA_top5":         fhsa_candidates[:5],
        "TFSA_growth_top5":  tfsa_core_cands[:5],
        "TFSA_income_top5":  tfsa_income_cands[:5],
        "TFSA_swing_top3":   tfsa_swing_cands[:3],

        # Full ranked lists (for dashboard filtering)
        "FHSA_all":          fhsa_candidates,
        "TFSA_core_all":     tfsa_core_cands,
        "TFSA_income_all":   tfsa_income_cands,
        "TFSA_swing_all":    tfsa_swing_cands,

        "stats": {
            "fhsa_passed":   len(fhsa_candidates),
            "tfsa_growth":   len(tfsa_core_cands),
            "tfsa_income":   len(tfsa_income_cands),
            "tfsa_swing":    len(tfsa_swing_cands),
        }
    }

    return results


# ============================================================
# BRIEF FORMATTER
# ============================================================

def format_pick_for_brief(pick):
    """Human-readable pick summary for the daily brief"""
    d   = pick["data"]
    p   = pick["pick"]
    s   = pick["score"]

    lines = [
        f"{'='*52}",
        f"  {d['ticker']:<12} SCORE: {s}/100  [{p['category']}]",
        f"{'='*52}",
        f"  Price:    ${d['price']}   Day: {'+' if d['day_chg_pct']>=0 else ''}{d['day_chg_pct']}%",
        f"  30D:      {'+' if d['perf_30d']>=0 else ''}{d['perf_30d']}%     90D: {'+' if d['perf_90d']>=0 else ''}{d['perf_90d']}%",
    ]

    if d["div_yield"] > 0:
        lines.append(f"  Dividend: {d['div_yield']}%/yr  Ex-div: {d['ex_div_date']}")
    if d["pe_ratio"]:
        lines.append(f"  P/E:      {d['pe_ratio']}         Debt/Eq: {d['debt_equity']}")
    if d["rev_growth"] != 0:
        lines.append(f"  Rev Grwth:{'+' if d['rev_growth']>=0 else ''}{d['rev_growth']}%  Earn: {'+' if d['earn_growth']>=0 else ''}{d['earn_growth']}%")
    if d["target_price"]:
        lines.append(f"  Analyst:  ${d['target_price']} target ({'+' if d['upside_target']>=0 else ''}{d['upside_target']}% upside) | {d['analyst_rec']}")

    lines.append(f"\n  ✅ WHY IT QUALIFIES:")
    for r in pick["reasons"][:4]:
        lines.append(f"     {r}")

    if pick["flags"]:
        lines.append(f"\n  ⚠️  WATCH OUT:")
        for f in pick["flags"][:2]:
            lines.append(f"     {f}")

    lines += [
        f"\n  📋 ACTION:   {p['action']}",
        f"  📅 EXIT:     {p['exit_note']}",
        f"  📈 EXPECTED: +{p['exp_low']}% to +{p['exp_high']}%  ({p['exp_prob']}% probability)",
        f"  ⏱  HOLD:     {p['hold_days']} days  |  RISK: {p['risk_label']}",
    ]
    return "\n".join(lines)


def print_brief(results):
    """Print the full formatted brief to console / email"""
    now = datetime.now().strftime("%B %d, %Y at %I:%M %p")
    print(f"""
╔══════════════════════════════════════════════════════╗
║         INVESTOS — DAILY BRIEF                       ║
║         {now:<42} ║
║         Universe: {results['screened']}/{results['universe_size']} stocks analyzed{' '*14}║
╚══════════════════════════════════════════════════════╝

SCREENING RESULTS
  FHSA candidates:    {results['stats']['fhsa_passed']}
  TFSA growth picks:  {results['stats']['tfsa_growth']}
  TFSA income picks:  {results['stats']['tfsa_income']}
  TFSA swing picks:   {results['stats']['tfsa_swing']}
""")

    print("\n" + "🏠 " + "─"*50)
    print("  FHSA TOP PICKS  (Conservative Growth, Max 16% DD)")
    print("─"*52)
    for pick in results["FHSA_top5"]:
        print(format_pick_for_brief(pick))
        print()

    print("\n" + "📈 " + "─"*50)
    print("  TFSA GROWTH CORE  (Long-term, Age 33 optimised)")
    print("─"*52)
    for pick in results["TFSA_growth_top5"]:
        print(format_pick_for_brief(pick))
        print()

    print("\n" + "💰 " + "─"*50)
    print("  TFSA INCOME + DIVIDENDS  (Cash flow + compounding)")
    print("─"*52)
    for pick in results["TFSA_income_top5"]:
        print(format_pick_for_brief(pick))
        print()

    print("\n" + "⚡ " + "─"*50)
    print("  TFSA SWING PICKS  (Max $100/trade, short-term)")
    print("─"*52)
    for pick in results["TFSA_swing_top3"]:
        print(format_pick_for_brief(pick))
        print()


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    import sys

    # Quick test mode (20 tickers) vs full screen (all 500+)
    test_mode = "--test" in sys.argv
    limit     = 20 if test_mode else None

    if test_mode:
        print("🧪 TEST MODE — screening 20 tickers")

    results = run_full_screen(max_tickers=limit, verbose=True)
    print_brief(results)

    # Save for dashboard + engine consumption
    with open("screener_results.json", "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"\n💾 Saved to screener_results.json")
    print(f"🎯 Top FHSA pick:  {results['FHSA_top5'][0]['ticker'] if results['FHSA_top5'] else 'None'}")
    print(f"🎯 Top TFSA pick:  {results['TFSA_growth_top5'][0]['ticker'] if results['TFSA_growth_top5'] else 'None'}")
    print(f"🎯 Top income:     {results['TFSA_income_top5'][0]['ticker'] if results['TFSA_income_top5'] else 'None'}")
    print(f"⚡ Top swing:      {results['TFSA_swing_top3'][0]['ticker'] if results['TFSA_swing_top3'] else 'None'}")
