"""
InvestOS — Portfolio Engine
============================
Broker-agnostic. Scale-agnostic. Deploy $30 or $1,000 — same rules.

Execution venues (you choose the platform, model doesn't care):
  STOCK_ACCOUNT  → Any broker with TSX + US access
  FX_ACCOUNT     → Any FX margin platform
  CRYPTO_ACCOUNT → Any spot crypto or crypto ETF platform

To update your balances: edit CONFIG["accounts"] below.
Everything else is automatic.
"""

import json
import urllib.request
import urllib.parse
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET

# ============================================================
# CONFIGURATION — YOUR PERSONAL PARAMETERS
# ============================================================
# ============================================================
# YOUR ONLY JOB: update balances when you deposit money.
# Everything else — sizing, routing, risk rules — is automatic.
# ============================================================
CONFIG = {

    # ── WHO YOU ARE ────────────────────────────────────────────
    "investor": {
        "age":          33,
        "risk_profile": "high_calculated",   # Growth-focused, risk-aware
        "strategy":     ["growth", "income", "dividends", "fx", "crypto"],
        "currency":     "CAD",               # Base currency for sizing
    },

    # ── YOUR ACCOUNTS ──────────────────────────────────────────
    # Update balances here when you deposit. Nothing else to change.
    "accounts": {

        "FHSA": {
            # Canada First Home Savings Account
            # Conservative by rule — max 16% drawdown, ETFs preferred
            # Tax deductible contributions + tax-free withdrawals for first home
            "balance":          0,        # ← UPDATE when you deposit
            "max_drawdown_pct": 16,       # Hard rule — never override
            "style":            "conservative_growth",
            "venue":            "STOCK_ACCOUNT",
            "tag":              "[FHSA]",
        },

        "TFSA": {
            # Tax-Free Savings Account — all gains tax-free forever
            # Full range: stocks + ETFs. Route FX/crypto to separate venues.
            "balance":          0,        # ← UPDATE when you deposit
            "style":            "growth_income",
            "venue":            "STOCK_ACCOUNT",
            "tag":              "[TFSA]",

            # ── Capital Buckets ─────────────────────────────────
            # Structured so model failure ≠ financial failure.
            # If model fails 100%: Floor still pays dividends.
            # Each bucket has a different failure mode — they don't all fail together.
            "buckets": {
                "floor": {
                    "pct":  50,
                    "desc": "Dividend core. Pays you 5-7%/yr regardless of model quality.",
                    "examples": ["ENB.TO", "RY.TO", "T.TO", "CNR.TO", "VFV.TO"],
                },
                "model_picks": {
                    "pct":  30,
                    "desc": "ML-assisted growth picks. Max loss if model fails = 30% of TFSA.",
                    "examples": ["SHOP.TO", "NVDA", "MSFT", "CSU.TO", "CRWD"],
                },
                "swing": {
                    "pct":  15,
                    "desc": "Short-term trades. $100 hard cap — losses are small and defined.",
                    "examples": ["PLTR", "HOOD", "SOFI", "AMD", "TSLA"],
                },
                "crypto": {
                    "pct":  5,
                    "desc": "BTC + SOL. Mentally written off. Never more than 5% total.",
                    "examples": ["BTC-USD", "SOL-USD"],
                },
            },

            # Hard limits — never overridden by model
            "max_single_position_pct": 10,   # No single stock > 10% of TFSA
            "max_swing_per_trade_pct":  2,   # Swing trades capped at 2% of balance (min $30, max $200)
            "max_crypto_total_pct":     5,   # BTC + SOL combined never > 5%
        },
    },

    # ── EXECUTION VENUES ───────────────────────────────────────
    # Categories only — no broker names.
    # You choose the platform. Swap brokers anytime without touching code.
    "venues": {
        "STOCK_ACCOUNT":  "Any broker with TSX + US access (e.g. Questrade, IBKR, Wealthsimple)",
        "FX_ACCOUNT":     "Any FX margin platform (e.g. OANDA, IBKR, IG Markets)",
        "CRYPTO_ACCOUNT": "Any spot crypto or crypto ETF (e.g. Coinbase, Kraken, BTCC.B in TFSA)",
    },

    # ── MARKETS YOU TRADE ──────────────────────────────────────
    "markets": {
        "canadian_stocks": True,   # TSX blue chips, ETFs, REITs
        "us_stocks":       True,   # NYSE, NASDAQ — growth + dividends
        "global_etfs":     True,   # VEA, VWO, EWJ, VGK — world exposure via US-listed ETFs
        "fx_pairs":        True,   # EUR/USD, USD/CAD, GBP/USD, USD/JPY, XAU/USD
        "crypto":          True,   # BTC + SOL directional signals
    },

    # ── SIGNAL SOURCES (X/Twitter) ─────────────────────────────
    "x_accounts": [
        {"handle": "nolimitgains",    "focus": "macro_market_analysis", "weight": "macro_context"},
        {"handle": "juliuselum",      "focus": "wealth_building",       "weight": "mindset_strategy"},
        {"handle": "aleabitoreddit",  "focus": "retail_sentiment",      "weight": "momentum_signal"},
        {"handle": "amitisinvesting", "focus": "stock_picks",           "weight": "high_signal_picks"},
        {"handle": "olumidecapital",  "focus": "capital_investing",     "weight": "position_ideas"},
        {"handle": "easyeatsbodega",  "focus": "consumer_trends",       "weight": "sector_signals"},
        {"handle": "optionsbuffett",  "focus": "options_flow",          "weight": "options_signal"},
    ],

    # ── RISK RULES ─────────────────────────────────────────────
    # These apply regardless of account, balance, or broker.
    "risk_rules": {
        "max_picks_per_sector":       2,    # No sector concentration
        "swing_earnings_buffer_days": 14,   # Never hold swing through earnings
        "score_velocity_weight":      0.25, # Rising stocks boosted in conviction ranking
        "min_conviction_score":       60,   # Only act on picks scoring 60+
        "stop_loss_pct": {
            "FHSA":         16,   # FHSA hard stop — house money
            "floor":         8,   # Dividend core — tight stop, high quality names
            "model_picks":  12,   # Standard stop
            "swing":        10,   # Swing — defined risk, exit fast
            "crypto":       20,   # Crypto — wide stop, high volatility
        },
    },

    # ── WATCHLIST ──────────────────────────────────────────────
    # Screener covers 500+ stocks. These are starting seeds.
    # Model surfaces the best ones daily — you don't pick from this list.
    "watchlist": {
        "conservative": ["XGRO.TO", "XEQT.TO", "VFV.TO", "ZCN.TO", "XIU.TO",
                         "VEA", "VWO", "VGK", "EWJ", "EWC"],    # Global ETFs added
        "floor":        ["TD.TO", "RY.TO", "ENB.TO", "T.TO", "BCE.TO",
                         "CNR.TO", "FTS.TO", "MFC.TO", "SLF.TO", "BNS.TO",
                         "JNJ", "PG", "KO", "VZ", "O", "MAIN"],
        "growth":       ["SHOP.TO", "CSU.TO", "BN.TO", "ATD.TO", "WSP.TO",
                         "NVDA", "MSFT", "AAPL", "GOOGL", "META",
                         "CRWD", "PLTR", "DDOG", "SNOW", "PANW"],
        "swing":        ["PLTR", "HOOD", "SOFI", "AMD", "TSLA",
                         "NVDA", "AFRM", "NU", "HIMS", "RIVN"],
        "global_etfs":  ["VEA", "VWO", "VGK", "EWJ", "EWC", "EEM",
                         "EWG", "EWU", "EWA", "INDA", "FXI", "KWEB"],
    },
}

# ============================================================
# DATA FETCHERS
# ============================================================

def fetch_stock_data(ticker):
    """Fetch stock data from Yahoo Finance (free, no API key)"""
    try:
        clean_ticker = ticker.replace(".TO", "") if ticker.endswith(".TO") else ticker
        yahoo_ticker = ticker
        
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{urllib.parse.quote(yahoo_ticker)}?interval=1d&range=3mo"
        req = urllib.request.Request(url, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        with urllib.request.urlopen(req, timeout=10) as response:
            data = json.loads(response.read().decode())
        
        result = data['chart']['result'][0]
        meta = result['meta']
        
        current_price = meta.get('regularMarketPrice', 0)
        prev_close = meta.get('previousClose', current_price)
        day_change_pct = ((current_price - prev_close) / prev_close * 100) if prev_close else 0
        
        # Get 52 week range
        week52_high = meta.get('fiftyTwoWeekHigh', 0)
        week52_low = meta.get('fiftyTwoWeekLow', 0)
        
        # Calculate from 52w high for drawdown
        drawdown_from_high = ((current_price - week52_high) / week52_high * 100) if week52_high else 0
        
        # Get historical closes for trend
        closes = result['indicators']['quote'][0].get('close', [])
        closes = [c for c in closes if c is not None]
        
        # 30 day performance
        perf_30d = ((closes[-1] - closes[-22]) / closes[-22] * 100) if len(closes) >= 22 else 0
        # 90 day performance  
        perf_90d = ((closes[-1] - closes[0]) / closes[0] * 100) if len(closes) > 1 else 0
        
        return {
            "ticker": ticker,
            "price": round(current_price, 2),
            "day_change_pct": round(day_change_pct, 2),
            "week52_high": round(week52_high, 2),
            "week52_low": round(week52_low, 2),
            "drawdown_from_high_pct": round(drawdown_from_high, 2),
            "perf_30d": round(perf_30d, 2),
            "perf_90d": round(perf_90d, 2),
            "volume": meta.get('regularMarketVolume', 0),
            "status": "ok"
        }
    except Exception as e:
        return {"ticker": ticker, "status": "error", "error": str(e), "price": 0}


def fetch_dividend_info(ticker):
    """Fetch dividend info from Yahoo Finance"""
    try:
        url = f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{urllib.parse.quote(ticker)}?modules=summaryDetail,defaultKeyStatistics"
        req = urllib.request.Request(url, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        with urllib.request.urlopen(req, timeout=10) as response:
            data = json.loads(response.read().decode())
        
        summary = data['quoteSummary']['result'][0]['summaryDetail']
        
        div_yield = summary.get('dividendYield', {}).get('raw', 0) or 0
        div_rate = summary.get('dividendRate', {}).get('raw', 0) or 0
        ex_div_date = summary.get('exDividendDate', {}).get('fmt', 'N/A') or 'N/A'
        payout_ratio = summary.get('payoutRatio', {}).get('raw', 0) or 0
        
        return {
            "dividend_yield_pct": round(div_yield * 100, 2),
            "annual_dividend": round(div_rate, 2),
            "ex_dividend_date": ex_div_date,
            "payout_ratio_pct": round(payout_ratio * 100, 1)
        }
    except:
        return {"dividend_yield_pct": 0, "annual_dividend": 0, "ex_dividend_date": "N/A", "payout_ratio_pct": 0}


def fetch_earnings_calendar(ticker):
    """Get next earnings date"""
    try:
        url = f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{urllib.parse.quote(ticker)}?modules=calendarEvents"
        req = urllib.request.Request(url, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        with urllib.request.urlopen(req, timeout=10) as response:
            data = json.loads(response.read().decode())
        
        result = data['quoteSummary']['result'][0]
        earnings = result.get('calendarEvents', {}).get('earnings', {})
        dates = earnings.get('earningsDate', [])
        
        if dates:
            next_earnings = dates[0].get('fmt', 'N/A')
        else:
            next_earnings = 'N/A'
            
        return {"next_earnings": next_earnings}
    except:
        return {"next_earnings": "N/A"}


def fetch_rss_signals(handle):
    """Fetch X account posts via RSS bridge (nitter)"""
    # Multiple nitter instances for redundancy
    nitter_instances = [
        f"https://nitter.privacydev.net/{handle}/rss",
        f"https://nitter.poast.org/{handle}/rss",
        f"https://nitter.net/{handle}/rss"
    ]
    
    for rss_url in nitter_instances:
        try:
            req = urllib.request.Request(rss_url, headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            })
            with urllib.request.urlopen(req, timeout=8) as response:
                content = response.read().decode('utf-8', errors='ignore')
            
            root = ET.fromstring(content)
            channel = root.find('channel')
            items = channel.findall('item') if channel else []
            
            posts = []
            for item in items[:5]:  # Last 5 posts
                title = item.findtext('title', '') or ''
                description = item.findtext('description', '') or ''
                pub_date = item.findtext('pubDate', '') or ''
                
                # Clean HTML tags
                import re
                clean_desc = re.sub('<[^<]+?>', '', description)
                clean_desc = clean_desc.replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>').replace('&quot;', '"')
                
                posts.append({
                    "text": f"{title} {clean_desc}".strip()[:500],
                    "date": pub_date[:16] if pub_date else ""
                })
            
            return {"handle": handle, "posts": posts, "status": "ok"}
        except:
            continue
    
    return {"handle": handle, "posts": [], "status": "unavailable"}


def extract_tickers_from_text(text):
    """Extract stock tickers mentioned in text"""
    import re
    # Match $TICKER patterns and common mentions
    tickers = re.findall(r'\$([A-Z]{1,5}(?:\.[A-Z]{1,2})?)', text.upper())
    # Also match common patterns like "PLTR" or "NVDA" standalone
    standalone = re.findall(r'\b(PLTR|NVDA|AMD|HOOD|SHOP|TD|RY|ENB|TSLA|AAPL|MSFT|AMZN|META|SOFI|MSTR|BTC|ETH)\b', text.upper())
    return list(set(tickers + standalone))


# ============================================================
# ANALYSIS ENGINE
# ============================================================

def score_stock_for_age33(stock_data, div_data, account_type):
    """
    Score a stock 0-100 based on 33yo growth+income strategy
    Age 33 = can take more risk, time horizon 20-30 years
    """
    score = 0
    reasons = []
    
    if stock_data.get('status') != 'ok':
        return 0, ["Data unavailable"]
    
    price = stock_data['price']
    day_chg = stock_data['day_change_pct']
    perf_30d = stock_data['perf_30d']
    perf_90d = stock_data['perf_90d']
    drawdown = abs(stock_data['drawdown_from_high_pct'])
    div_yield = div_data['dividend_yield_pct']
    
    if account_type == "FHSA":
        # Conservative growth — reward stability, penalize drawdown
        if drawdown < 10: score += 25; reasons.append("Low drawdown risk ✓")
        elif drawdown < 16: score += 10; reasons.append("Approaching drawdown limit ⚠️")
        else: score -= 20; reasons.append("Exceeds 16% drawdown limit ✗")
        
        if perf_30d > 0: score += 15; reasons.append(f"+{perf_30d}% 30d trend ✓")
        if perf_90d > 5: score += 20; reasons.append(f"+{perf_90d}% 90d strong ✓")
        if div_yield > 2: score += 15; reasons.append(f"{div_yield}% dividend yield ✓")
        if day_chg > 0: score += 10; reasons.append("Positive today ✓")
        
    elif account_type == "TFSA_core":
        # Growth + income — balanced long term
        if perf_90d > 10: score += 25; reasons.append(f"Strong 90d: +{perf_90d}% ✓")
        elif perf_90d > 5: score += 15; reasons.append(f"Good 90d: +{perf_90d}% ✓")
        
        if div_yield > 3: score += 20; reasons.append(f"Strong dividend: {div_yield}% ✓")
        elif div_yield > 1.5: score += 10; reasons.append(f"Dividend: {div_yield}% ✓")
        
        if perf_30d > 2: score += 15; reasons.append(f"30d momentum: +{perf_30d}% ✓")
        if drawdown < 20: score += 10; reasons.append("Acceptable risk range ✓")
        if day_chg > 0.5: score += 10; reasons.append("Positive momentum today ✓")
        
    elif account_type == "TFSA_swing":
        # High momentum, short term
        if perf_30d > 5: score += 30; reasons.append(f"Strong momentum: +{perf_30d}% ✓")
        elif perf_30d > 2: score += 15; reasons.append(f"Momentum building: +{perf_30d}% ✓")
        
        if day_chg > 1: score += 20; reasons.append(f"Strong day: +{day_chg}% ✓")
        elif day_chg > 0: score += 10; reasons.append("Positive day ✓")
        
        volume = stock_data.get('volume', 0)
        if volume > 5000000: score += 20; reasons.append("High volume ✓")
        elif volume > 1000000: score += 10; reasons.append("Good volume ✓")
        
        # Distance from 52w high = potential
        if 10 < drawdown < 25: score += 15; reasons.append("Recovery potential ✓")
    
    return max(0, min(100, score)), reasons


def calculate_expected_return(stock_data, account_type, hold_days=90):
    """Estimate expected return range based on historical performance"""
    if stock_data.get('status') != 'ok':
        return None
    
    perf_30d = stock_data['perf_30d']
    perf_90d = stock_data['perf_90d']
    
    # Simple projection based on recent trend
    daily_rate = perf_90d / 90 if perf_90d else perf_30d / 30
    expected = daily_rate * hold_days
    
    # Confidence band (wider for swings, tighter for conservative)
    if account_type == "FHSA":
        low = expected * 0.4
        high = expected * 1.6
        probability = 65 if expected > 0 else 35
    elif account_type == "TFSA_core":
        low = expected * 0.5
        high = expected * 1.8
        probability = 60 if expected > 0 else 40
    else:  # swing
        low = expected * 0.3
        high = expected * 2.2
        probability = 55 if expected > 0 else 45
    
    return {
        "expected_pct": round(expected, 1),
        "low_pct": round(low, 1),
        "high_pct": round(high, 1),
        "probability_pct": probability,
        "hold_days": hold_days
    }


def check_fhsa_drawdown_alert(positions, fhsa_balance):
    """Check if any FHSA position is approaching 16% drawdown"""
    alerts = []
    max_loss = fhsa_balance * 0.16
    
    for pos in positions:
        stock = pos.get('stock_data', {})
        if stock.get('status') == 'ok':
            drawdown = abs(stock['drawdown_from_high_pct'])
            if drawdown >= 14:
                alerts.append({
                    "ticker": pos['ticker'],
                    "drawdown": drawdown,
                    "urgency": "CRITICAL" if drawdown >= 15 else "WARNING",
                    "message": f"⚠️ {pos['ticker']} at {drawdown}% drawdown — approaching 16% limit!"
                })
    return alerts


# ============================================================
# SIGNAL AGGREGATOR
# ============================================================

def aggregate_x_signals(x_feeds):
    """Aggregate signals from all X accounts"""
    all_tickers = {}
    sentiment_summary = []
    
    for feed in x_feeds:
        handle = feed['handle']
        posts = feed.get('posts', [])
        
        if not posts:
            continue
            
        # Get account config
        account_config = next((a for a in CONFIG['x_accounts'] if a['handle'] == handle), {})
        weight = account_config.get('weight', 'general')
        focus = account_config.get('focus', 'general')
        
        account_tickers = []
        account_text = ""
        
        for post in posts[:3]:
            text = post.get('text', '')
            tickers = extract_tickers_from_text(text)
            account_tickers.extend(tickers)
            account_text += f" {text}"
        
        # Count ticker mentions with source weight
        for ticker in account_tickers:
            if ticker not in all_tickers:
                all_tickers[ticker] = {"count": 0, "sources": [], "weight_score": 0}
            all_tickers[ticker]["count"] += 1
            all_tickers[ticker]["sources"].append(f"@{handle}")
            
            # High signal sources get more weight
            if weight == "high_signal_picks":
                all_tickers[ticker]["weight_score"] += 3
            elif weight in ["macro_context", "position_ideas"]:
                all_tickers[ticker]["weight_score"] += 2
            else:
                all_tickers[ticker]["weight_score"] += 1
        
        # Generate sentiment summary for this account
        if account_text.strip():
            # Simple bullish/bearish detection
            bullish_words = ['bull', 'buy', 'long', 'breakout', 'upside', 'growth', 'target', 'calls', 'moon', '🚀', '📈', 'accumulate']
            bearish_words = ['bear', 'sell', 'short', 'breakdown', 'avoid', 'risk', 'puts', '📉', 'dump', 'overvalued']
            
            text_lower = account_text.lower()
            bull_score = sum(1 for w in bullish_words if w in text_lower)
            bear_score = sum(1 for w in bearish_words if w in text_lower)
            
            if bull_score > bear_score:
                sentiment = "🟢 Bullish"
            elif bear_score > bull_score:
                sentiment = "🔴 Bearish"
            else:
                sentiment = "🟡 Neutral"
            
            if account_tickers:
                sentiment_summary.append({
                    "handle": handle,
                    "sentiment": sentiment,
                    "tickers": list(set(account_tickers))[:5],
                    "focus": focus,
                    "latest_post": posts[0].get('text', '')[:200] if posts else ""
                })
    
    # Sort tickers by weight score
    top_signals = sorted(all_tickers.items(), key=lambda x: x[1]['weight_score'], reverse=True)[:10]
    
    return {
        "top_tickers": [{"ticker": t, "data": d} for t, d in top_signals],
        "account_summaries": sentiment_summary
    }


# ============================================================
# PORTFOLIO PROJECTOR
# ============================================================

def project_portfolio_growth(balance, monthly_contribution, annual_return_pct, years):
    """Project portfolio growth with contributions — no goal, just honest compounding math"""
    projections = {}
    monthly_rate = annual_return_pct / 100 / 12
    current = balance

    for month in range(1, years * 12 + 1):
        current = current * (1 + monthly_rate) + monthly_contribution
        yr = month // 12
        if month % 12 == 0 and yr <= years:
            projections[f"{yr}yr"] = round(current, 2)

    return {
        "projections":       projections,
        "balance":           balance,
        "monthly_contrib":   monthly_contribution,
        "annual_return_pct": annual_return_pct,
        "note":              f"Projection assumes {annual_return_pct}% annual return + ${monthly_contribution}/mo contributions. Not guaranteed.",
    }


def compute_bucket_allocation(account_balance):
    """
    Convert bucket percentages to hard dollar amounts.
    Works at any balance — $30 or $30,000.
    """
    buckets = CONFIG["accounts"]["TFSA"]["buckets"]
    result  = {}

    for name, b in buckets.items():
        pct     = b["pct"] / 100
        dollars = round(account_balance * pct, 2)

        # Max single position per bucket
        if name == "floor":
            max_pos = round(min(dollars * 0.25, account_balance * 0.10), 2)
        elif name == "model_picks":
            max_pos = round(min(dollars * 0.20, account_balance * 0.10), 2)
        elif name == "swing":
            # Scale with balance: min $10, max $200, always ≤ 2% of balance
            raw     = round(account_balance * CONFIG["accounts"]["TFSA"]["max_swing_per_trade_pct"] / 100, 2)
            max_pos = max(10, min(200, raw))
        elif name == "crypto":
            max_pos = round(min(dollars * 0.60, account_balance * 0.03), 2)
        else:
            max_pos = round(dollars * 0.20, 2)

        result[name] = {
            "pct":          b["pct"],
            "dollars":      dollars,
            "max_position": max_pos,
            "desc":         b["desc"],
            "examples":     b.get("examples", []),
        }

    # Survival analysis
    floor_d = result["floor"]["dollars"]
    result["_survival_check"] = {
        "if_model_fails_completely": f"${floor_d:,.0f} dividend floor still intact — pays ~${round(floor_d*0.06):,.0f}/yr",
        "worst_case_tfsa_loss":      f"${round(account_balance*0.50):,.0f} (model+swing+crypto all zero)",
        "floor_annual_income":       f"~${round(floor_d*0.06):,.0f}/yr at 6% avg dividend yield",
    }
    return result


def compute_deployment_plan(deploy_amount, account_balance=None, top_picks=None,
                             fx_signals=None, crypto_signals=None, regime="NORMAL"):
    """
    THE CORE USER-FACING FUNCTION.

    You say: "I have $X to deploy today."
    Model says: exactly where each dollar goes, which specific pick to buy,
                and what your stop loss is.

    Works at any amount — $30 or $1,000. Same rules, same logic.

    Returns a ready-to-execute deployment plan:
      {
        "deploy_amount": 100,
        "splits": {
            "floor":       {"dollars": 50, "pick": "ENB.TO", "stop": ...},
            "model_picks": {"dollars": 30, "pick": "NVDA",   "stop": ...},
            "swing":       {"dollars": 15, "pick": "PLTR",   "stop": ...},
            "crypto":      {"dollars": 5,  "pick": "BTC",    "stop": ...},
        },
        "skip_buckets": ["swing"],   # If no strong signal today
        "uninvested":   15,          # Cash to keep if no signal
        "venue_map": {
            "ENB.TO": "STOCK_ACCOUNT",
            "PLTR":   "STOCK_ACCOUNT",
            "EUR/USD":"FX_ACCOUNT",
            "BTC":    "CRYPTO_ACCOUNT",
        }
      }
    """
    balance  = account_balance or CONFIG["accounts"]["TFSA"]["balance"] or deploy_amount
    buckets  = compute_bucket_allocation(balance)
    stops    = CONFIG["risk_rules"]["stop_loss_pct"]
    regime_scale = {"BULL":1.0,"NORMAL":0.85,"CAUTION":0.65,"BEAR":0.40}.get(regime,0.85)

    plan = {
        "deploy_amount": deploy_amount,
        "regime":        regime,
        "regime_scale":  regime_scale,
        "splits":        {},
        "skip_buckets":  [],
        "uninvested":    0,
        "venue_map":     {},
        "generated_at":  __import__('datetime').datetime.now().isoformat(),
    }

    remaining = deploy_amount

    # ── Floor bucket ──────────────────────────────────────────
    floor_dollars  = round(deploy_amount * buckets["floor"]["pct"] / 100 * regime_scale, 2)
    floor_pick     = None
    if top_picks:
        for p in top_picks:
            cat = p.get("pick",{}).get("category","")
            if "INCOME" in cat or "DIVIDEND" in cat or "FLOOR" in cat or "FHSA" in cat:
                floor_pick = p
                break
        if not floor_pick:  # Fall back to highest-scored non-swing pick
            non_swing = [p for p in top_picks if "SWING" not in p.get("pick",{}).get("category","")]
            floor_pick = non_swing[0] if non_swing else None

    if floor_pick and floor_dollars >= 5:
        price     = floor_pick.get("data",{}).get("price",0)
        stop_pct  = stops["floor"] / 100
        plan["splits"]["floor"] = {
            "dollars":    floor_dollars,
            "pick":       floor_pick["ticker"],
            "category":   floor_pick.get("pick",{}).get("category",""),
            "price":      price,
            "stop":       round(price*(1-stop_pct),2) if price else None,
            "stop_pct":   stops["floor"],
            "score":      floor_pick.get("score",0),
            "action":     floor_pick.get("pick",{}).get("action",""),
            "venue":      "STOCK_ACCOUNT",
            "tag":        "[TFSA]" if "FHSA" not in floor_pick.get("pick",{}).get("category","") else "[FHSA]",
        }
        plan["venue_map"][floor_pick["ticker"]] = "STOCK_ACCOUNT"
        remaining -= floor_dollars
    else:
        plan["skip_buckets"].append("floor")
        plan["uninvested"] += floor_dollars

    # ── Model picks bucket ────────────────────────────────────
    model_dollars = round(deploy_amount * buckets["model_picks"]["pct"] / 100 * regime_scale, 2)
    model_pick    = None
    if top_picks:
        for p in top_picks:
            cat = p.get("pick",{}).get("category","")
            if "GROWTH" in cat or "CORE" in cat:
                model_pick = p
                break

    if model_pick and model_dollars >= 5:
        price    = model_pick.get("data",{}).get("price",0)
        stop_pct = stops["model_picks"] / 100
        plan["splits"]["model_picks"] = {
            "dollars":    model_dollars,
            "pick":       model_pick["ticker"],
            "category":   model_pick.get("pick",{}).get("category",""),
            "price":      price,
            "stop":       round(price*(1-stop_pct),2) if price else None,
            "stop_pct":   stops["model_picks"],
            "score":      model_pick.get("score",0),
            "action":     model_pick.get("pick",{}).get("action",""),
            "venue":      "STOCK_ACCOUNT",
            "tag":        "[TFSA]",
        }
        plan["venue_map"][model_pick["ticker"]] = "STOCK_ACCOUNT"
        remaining -= model_dollars
    else:
        plan["skip_buckets"].append("model_picks")
        plan["uninvested"] += model_dollars

    # ── Swing bucket ──────────────────────────────────────────
    swing_dollars = round(deploy_amount * buckets["swing"]["pct"] / 100 * regime_scale, 2)
    swing_pick    = None
    # Check FX first (if FX signal is strong, use that for swing)
    fx_swing      = None
    if fx_signals and fx_signals.get("top_call"):
        top_fx = fx_signals["top_call"]
        if top_fx.get("conviction",0) >= 70:
            fx_swing = top_fx

    if top_picks and not fx_swing:
        for p in top_picks:
            if "SWING" in p.get("pick",{}).get("category",""):
                swing_pick = p
                break

    if fx_swing and swing_dollars >= 5:
        plan["splits"]["swing"] = {
            "dollars":    swing_dollars,
            "pick":       fx_swing.get("pair","FX"),
            "category":   "FX SWING",
            "price":      fx_swing.get("entry",0),
            "stop":       fx_swing.get("stop",0),
            "stop_pct":   stops["swing"],
            "conviction": fx_swing.get("conviction",0),
            "action":     f"{fx_swing.get('direction','?')} {fx_swing.get('pair','')} — {fx_swing.get('key_driver','')}",
            "venue":      "FX_ACCOUNT",
            "tag":        "[FX]",
        }
        plan["venue_map"][fx_swing.get("pair","FX")] = "FX_ACCOUNT"
        remaining -= swing_dollars
    elif swing_pick and swing_dollars >= 5:
        price    = swing_pick.get("data",{}).get("price",0)
        stop_pct = stops["swing"] / 100
        plan["splits"]["swing"] = {
            "dollars":    swing_dollars,
            "pick":       swing_pick["ticker"],
            "category":   "SWING",
            "price":      price,
            "stop":       round(price*(1-stop_pct),2) if price else None,
            "stop_pct":   stops["swing"],
            "score":      swing_pick.get("score",0),
            "action":     swing_pick.get("pick",{}).get("action",""),
            "venue":      "STOCK_ACCOUNT",
            "tag":        "[TFSA]",
        }
        plan["venue_map"][swing_pick["ticker"]] = "STOCK_ACCOUNT"
        remaining -= swing_dollars
    else:
        plan["skip_buckets"].append("swing")
        plan["uninvested"] += swing_dollars

    # ── Crypto bucket ─────────────────────────────────────────
    crypto_dollars = round(deploy_amount * buckets["crypto"]["pct"] / 100, 2)
    crypto_pick    = None
    if crypto_signals and crypto_signals.get("assets"):
        assets = crypto_signals["assets"]
        # BTC first, then SOL
        for symbol in ["BTC-USD","SOL-USD"]:
            a = assets.get(symbol,{})
            if a.get("direction") == "LONG" and a.get("conviction",0) >= 55:
                crypto_pick = a
                break

    if crypto_pick and crypto_dollars >= 1:
        plan["splits"]["crypto"] = {
            "dollars":    crypto_dollars,
            "pick":       crypto_pick.get("name","BTC"),
            "category":   "CRYPTO",
            "price":      crypto_pick.get("price",0),
            "stop":       crypto_pick.get("stop",0),
            "stop_pct":   stops["crypto"],
            "conviction": crypto_pick.get("conviction",0),
            "action":     crypto_pick.get("action",""),
            "venue":      "CRYPTO_ACCOUNT",
            "tag":        "[CRYPTO]",
        }
        plan["venue_map"][crypto_pick.get("name","BTC")] = "CRYPTO_ACCOUNT"
        remaining -= crypto_dollars
    else:
        plan["skip_buckets"].append("crypto")
        plan["uninvested"] += crypto_dollars

    # ── Uninvested cash ───────────────────────────────────────
    plan["uninvested"]    = round(max(0, remaining), 2)
    plan["total_deployed"] = round(deploy_amount - plan["uninvested"], 2)

    # ── Human-readable summary ────────────────────────────────
    lines = [f"DEPLOY ${deploy_amount:.2f} — {regime} REGIME"]
    for name, s in plan["splits"].items():
        lines.append(f"  {s['tag']} {s['pick']:<12} ${s['dollars']:.2f} → stop ${s['stop'] or '?'} | {s['action'][:50]}")
    if plan["skip_buckets"]:
        lines.append(f"  ⏸  Skipped: {', '.join(plan['skip_buckets'])} (no signal / below min)")
    if plan["uninvested"] > 0:
        lines.append(f"  💵 Uninvested: ${plan['uninvested']:.2f} — hold as cash")
    plan["summary"] = "\n".join(lines)

    return plan


def generate_full_brief():
    """Generate the complete daily investment brief"""
    today = datetime.now().strftime("%B %d, %Y")
    
    print(f"🔄 Generating brief for {today}...")
    
    # 1. Fetch all stock data
    print("📊 Fetching market data...")
    all_tickers = (
        CONFIG['watchlist']['FHSA_candidates'] +
        CONFIG['watchlist']['TFSA_growth'] +
        CONFIG['watchlist']['TFSA_income'] +
        CONFIG['watchlist']['TFSA_swing']
    )
    
    stock_data = {}
    dividend_data = {}
    earnings_data = {}
    
    for ticker in all_tickers[:20]:  # Cap at 20 to avoid rate limiting
        stock_data[ticker] = fetch_stock_data(ticker)
        dividend_data[ticker] = fetch_dividend_info(ticker)
        earnings_data[ticker] = fetch_earnings_calendar(ticker)
    
    # 2. Fetch X signals
    print("📡 Fetching signal feeds...")
    x_feeds = []
    for account in CONFIG['x_accounts']:
        feed = fetch_rss_signals(account['handle'])
        x_feeds.append(feed)
    
    signals = aggregate_x_signals(x_feeds)
    
    # 3. Score stocks
    print("🧮 Scoring stocks...")
    fhsa_scored = []
    for ticker in CONFIG['watchlist']['FHSA_candidates']:
        if ticker in stock_data:
            score, reasons = score_stock_for_age33(stock_data[ticker], dividend_data.get(ticker, {}), "FHSA")
            expected = calculate_expected_return(stock_data[ticker], "FHSA", hold_days=180)
            fhsa_scored.append({
                "ticker": ticker,
                "score": score,
                "reasons": reasons,
                "stock": stock_data[ticker],
                "dividend": dividend_data.get(ticker, {}),
                "earnings": earnings_data.get(ticker, {}),
                "expected_return": expected
            })
    
    tfsa_core_scored = []
    for ticker in CONFIG['watchlist']['TFSA_growth'] + CONFIG['watchlist']['TFSA_income']:
        if ticker in stock_data:
            score, reasons = score_stock_for_age33(stock_data[ticker], dividend_data.get(ticker, {}), "TFSA_core")
            expected = calculate_expected_return(stock_data[ticker], "TFSA_core", hold_days=365)
            tfsa_core_scored.append({
                "ticker": ticker,
                "score": score,
                "reasons": reasons,
                "stock": stock_data[ticker],
                "dividend": dividend_data.get(ticker, {}),
                "earnings": earnings_data.get(ticker, {}),
                "expected_return": expected
            })
    
    tfsa_swing_scored = []
    for ticker in CONFIG['watchlist']['TFSA_swing']:
        if ticker in stock_data:
            score, reasons = score_stock_for_age33(stock_data[ticker], dividend_data.get(ticker, {}), "TFSA_swing")
            expected = calculate_expected_return(stock_data[ticker], "TFSA_swing", hold_days=30)
            tfsa_swing_scored.append({
                "ticker": ticker,
                "score": score,
                "reasons": reasons,
                "stock": stock_data[ticker],
                "dividend": dividend_data.get(ticker, {}),
                "earnings": earnings_data.get(ticker, {}),
                "expected_return": expected
            })
    
    # Sort by score
    fhsa_scored.sort(key=lambda x: x['score'], reverse=True)
    tfsa_core_scored.sort(key=lambda x: x['score'], reverse=True)
    tfsa_swing_scored.sort(key=lambda x: x['score'], reverse=True)
    
    # 4. Portfolio projections
    tfsa_projection = project_portfolio_growth(
        balance=10000,
        monthly_contribution=300,
        annual_return_pct=12,
        years=20
    )
    
    fhsa_projection = project_portfolio_growth(
        balance=200,
        monthly_contribution=667,  # ~$8000/year
        annual_return_pct=8,
        years=5
    )
    
    # 5. FHSA drawdown check (empty positions for now, but framework is ready)
    fhsa_alerts = []
    for item in fhsa_scored:
        drawdown = abs(item['stock'].get('drawdown_from_high_pct', 0))
        if drawdown >= 14 and item['stock'].get('status') == 'ok':
            fhsa_alerts.append({
                "ticker": item['ticker'],
                "drawdown": drawdown,
                "urgency": "CRITICAL" if drawdown >= 15 else "WARNING"
            })
    
    # 6. Assemble brief
    brief = {
        "date": today,
        "generated_at": datetime.now().isoformat(),
        "accounts": {
            "FHSA": {
                "balance": CONFIG['accounts']['FHSA']['balance'],
                "max_loss_buffer": round(CONFIG['accounts']['FHSA']['balance'] * 0.16, 2),
                "alerts": fhsa_alerts,
                "top_picks": fhsa_scored[:3],
                "projection": fhsa_projection
            },
            "TFSA": {
                "balance": CONFIG['accounts']['TFSA']['balance'],
                "buckets": compute_bucket_allocation(CONFIG['accounts']['TFSA']['balance']),
                "top_core_picks": tfsa_core_scored[:3],
                "top_swing_picks": tfsa_swing_scored[:2],
                "projection": tfsa_projection
            }
        },
        "signals": signals,
        "x_feeds_status": [{"handle": f['handle'], "status": f['status'], "post_count": len(f.get('posts', []))} for f in x_feeds],
        "market_summary": {
            "total_tickers_analyzed": len([t for t in stock_data.values() if t.get('status') == 'ok']),
            "avg_day_change": round(sum(s.get('day_change_pct', 0) for s in stock_data.values() if s.get('status') == 'ok') / max(1, len([s for s in stock_data.values() if s.get('status') == 'ok'])), 2)
        }
    }
    
    return brief


if __name__ == "__main__":
    brief = generate_full_brief()
    
    # Save to JSON for dashboard consumption
    with open('/home/claude/latest_brief.json', 'w') as f:
        json.dump(brief, f, indent=2, default=str)
    
    print("\n✅ Brief generated successfully!")
    print(f"📊 Tickers analyzed: {brief['market_summary']['total_tickers_analyzed']}")
    print(f"📡 X feeds: {sum(1 for f in brief['x_feeds_status'] if f['status'] == 'ok')}/{len(brief['x_feeds_status'])} online")
    print(f"🏠 FHSA top pick: {brief['accounts']['FHSA']['top_picks'][0]['ticker'] if brief['accounts']['FHSA']['top_picks'] else 'N/A'}")
    print(f"📈 TFSA buckets: floor ${brief['accounts']['TFSA']['buckets']['floor']['dollars']:,.0f} | model ${brief['accounts']['TFSA']['buckets']['model_picks']['dollars']:,.0f} | swing ${brief['accounts']['TFSA']['buckets']['swing']['dollars']:,.0f}")
    print(f"⚠️ FHSA alerts: {len(brief['accounts']['FHSA']['alerts'])}")


# ============================================================
# TRADE LOGGING — real drawdown tracking
# ============================================================

import csv
import os

TRADES_FILE = "trades.csv"
TRADES_HEADERS = ["date","ticker","account","action","price","shares","total_value",
                  "stop_price","target_price","category","status","exit_date",
                  "exit_price","pnl_pct","notes"]

def load_trades():
    if not os.path.exists(TRADES_FILE):
        return []
    with open(TRADES_FILE, newline="") as f:
        return list(csv.DictReader(f))

def log_trade(ticker, account, action, price, total_value,
              stop_price=0, target_price=0, category="", notes=""):
    """
    Log a trade entry. Call this manually when you make a trade.
    Usage: python -c "from portfolio_engine import log_trade; log_trade('ENB.TO','TFSA','BUY',56.88,200,53.10,67.50,'INCOME')"
    """
    exists = os.path.exists(TRADES_FILE)
    with open(TRADES_FILE, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=TRADES_HEADERS)
        if not exists:
            w.writeheader()
        w.writerow({
            "date":         datetime.now().strftime("%Y-%m-%d"),
            "ticker":       ticker,
            "account":      account,
            "action":       action,
            "price":        price,
            "shares":       round(total_value / price, 4) if price else 0,
            "total_value":  total_value,
            "stop_price":   stop_price,
            "target_price": target_price,
            "category":     category,
            "status":       "OPEN",
            "exit_date":    "",
            "exit_price":   "",
            "pnl_pct":      "",
            "notes":        notes,
        })
    print(f"✅ Logged: {action} {ticker} @ ${price} (${total_value})")


def close_trade(ticker, exit_price, notes=""):
    """Mark an open trade as closed and calculate P&L"""
    trades = load_trades()
    updated = False
    for t in trades:
        if t["ticker"] == ticker and t["status"] == "OPEN":
            entry = float(t["price"]) if t["price"] else 0
            pnl   = round((exit_price - entry) / entry * 100, 2) if entry else 0
            t["status"]     = "CLOSED"
            t["exit_date"]  = datetime.now().strftime("%Y-%m-%d")
            t["exit_price"] = exit_price
            t["pnl_pct"]    = pnl
            t["notes"]      = notes or t["notes"]
            updated = True
            print(f"✅ Closed {ticker} @ ${exit_price} | P&L: {pnl:+.1f}%")
            break
    if updated:
        with open(TRADES_FILE, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=TRADES_HEADERS)
            w.writeheader()
            w.writerows(trades)
    else:
        print(f"⚠️  No open trade found for {ticker}")


def get_scorecard():
    """Calculate real win rate and P&L from trades.csv"""
    trades  = load_trades()
    closed  = [t for t in trades if t["status"] == "CLOSED"]
    open_t  = [t for t in trades if t["status"] == "OPEN"]

    if not closed:
        return {
            "recs_made":    len(trades),
            "open":         len(open_t),
            "closed":       0,
            "wins":         0,
            "losses":       0,
            "win_rate_pct": 0,
            "avg_win_pct":  0,
            "avg_loss_pct": 0,
            "total_pnl_pct":0,
            "note":         "No closed trades yet — scorecard builds as you close positions",
        }

    wins   = [t for t in closed if float(t.get("pnl_pct",0) or 0) > 0]
    losses = [t for t in closed if float(t.get("pnl_pct",0) or 0) <= 0]

    avg_win  = round(sum(float(t["pnl_pct"]) for t in wins)  / len(wins),  1) if wins   else 0
    avg_loss = round(sum(float(t["pnl_pct"]) for t in losses)/ len(losses),1) if losses else 0
    all_pnl  = [float(t.get("pnl_pct",0) or 0) for t in closed]

    return {
        "recs_made":    len(trades),
        "open":         len(open_t),
        "closed":       len(closed),
        "wins":         len(wins),
        "losses":       len(losses),
        "win_rate_pct": round(len(wins) / len(closed) * 100, 1) if closed else 0,
        "avg_win_pct":  avg_win,
        "avg_loss_pct": abs(avg_loss),
        "total_pnl_pct":round(sum(all_pnl) / len(all_pnl), 1) if all_pnl else 0,
        "best_trade":   max(all_pnl) if all_pnl else 0,
        "worst_trade":  min(all_pnl) if all_pnl else 0,
        "note":         f"{len(open_t)} open positions · {len(closed)} closed",
    }


def apply_sector_cap(picks, max_per_sector=2):
    """
    Enforce sector diversification: max N picks per sector.
    Keeps the highest-scored pick in each sector when over limit.
    Returns filtered list with sector counts noted.
    """
    SECTOR_MAP = {
        # Energy
        "ENB.TO":"ENERGY","CNQ.TO":"ENERGY","SU.TO":"ENERGY","CVE.TO":"ENERGY",
        "TRP.TO":"ENERGY","PPL.TO":"ENERGY","WCP.TO":"ENERGY","ARX.TO":"ENERGY",
        # Banks
        "TD.TO":"BANKS","RY.TO":"BANKS","BNS.TO":"BANKS","BMO.TO":"BANKS",
        "CM.TO":"BANKS","NA.TO":"BANKS",
        # Tech
        "SHOP.TO":"TECH","CSU.TO":"TECH","NVDA":"TECH","AMD":"TECH",
        "PLTR":"TECH","HOOD":"TECH","SOFI":"TECH","RBLX":"TECH",
        # Telecom
        "T.TO":"TELECOM","BCE.TO":"TELECOM","RCI-B.TO":"TELECOM",
        # ETFs
        "XGRO.TO":"ETF","XEQT.TO":"ETF","VFV.TO":"ETF","ZCN.TO":"ETF","XIU.TO":"ETF",
        # Consumer
        "ATD.TO":"CONSUMER","MRU.TO":"CONSUMER","L.TO":"CONSUMER",
        # Infrastructure
        "BN.TO":"INFRA","BAM.TO":"INFRA","CNR.TO":"INFRA","CP.TO":"INFRA",
    }

    sector_counts = {}
    filtered      = []
    removed       = []

    for pick in picks:
        ticker = pick["ticker"]
        sector = SECTOR_MAP.get(ticker, "OTHER")
        count  = sector_counts.get(sector, 0)

        if count < max_per_sector:
            sector_counts[sector] = count + 1
            pick["sector"]        = sector
            filtered.append(pick)
        else:
            removed.append({"ticker": ticker, "sector": sector,
                            "reason": f"Sector cap: already {max_per_sector} {sector} picks"})

    if removed:
        print(f"   Sector cap removed {len(removed)} picks: "
              f"{', '.join(r['ticker'] for r in removed)}")

    return filtered, removed


def normalize_x_signals_by_frequency(x_feeds, lookback_days=7):
    """
    Normalize X signal weight by posting frequency.
    Account posting 10x/day gets same weight as one posting 2x/week —
    by normalizing to 'mentions per day' rather than raw mention count.
    High-conviction tickers = mentioned across multiple accounts, not just repeated by one.
    """
    ticker_mentions = {}  # ticker -> {handle: count_normalized}

    for feed in x_feeds:
        handle    = feed.get("handle", "")
        posts     = feed.get("posts", [])
        post_count = max(1, len(posts))

        # Normalize: 1 mention from low-volume account counts more
        # than 5 mentions from high-volume account
        # Scale: up to 10 posts/day = normal, >10 = diminishing weight
        freq_weight = min(1.0, 10 / post_count)

        for post in posts:
            for ticker in post.get("tickers", []):
                t = ticker.upper()
                if t not in ticker_mentions:
                    ticker_mentions[t] = {}
                ticker_mentions[t][handle] = ticker_mentions[t].get(handle, 0) + freq_weight

    # Score: unique accounts mentioning + frequency weight
    scored = {}
    for ticker, handles in ticker_mentions.items():
        unique_accounts = len(handles)
        total_weight    = sum(handles.values())
        # Cross-account signal is strongest signal
        scored[ticker] = {
            "unique_accounts": unique_accounts,
            "total_weight":    round(total_weight, 2),
            "conviction_mult": min(2.0, 1.0 + (unique_accounts - 1) * 0.4),
            "handles":         list(handles.keys()),
        }

    return scored
