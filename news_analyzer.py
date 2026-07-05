"""
InvestOS — News & Macro Sentiment Analyzer
===========================================
Runs BEFORE the screener every morning.
Reads real news from free RSS feeds, extracts market signals,
and produces a sector sentiment map that adjusts stock scoring.

v2: Fixed double https:// typo in 5 feed URLs (was silently failing since Day 1).
    MarketWatch, CNBC, Investopedia, Guardian, Yahoo Finance all had "https://"
    Barrons (403) replaced with AP Business.
    Yahoo Finance removed (unreliable endpoint).
    Reuters Business added.
    Expected: 9/15 → 13-14/15 feeds online, 111 → 140-160+ articles.
"""

import json
import random
import time
import urllib.request
import urllib.parse
import urllib.error
import xml.etree.ElementTree as ET
import re
from datetime import datetime, timedelta
from collections import defaultdict

# Rotating user-agents for feeds that block the default Python UA (Reuters, AP, Investopedia)
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) "
    "Version/17.4.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:126.0) "
    "Gecko/20100101 Firefox/126.0",
]

# Apply rotating UA + rate-limit delay to feeds known to 403 on the default UA
ROTATING_UA_SOURCES = {"Reuters Business", "AP Business", "Investopedia"}

NEWS_SOURCES = [
    # Global macro — v2: fixed double https:// typo, Barrons→AP, Yahoo→Reuters
    {"name": "MarketWatch",          "url": "https://feeds.content.dowjones.io/public/rss/mw_realtimeheadlines", "weight": 3},
    {"name": "CNBC Markets",         "url": "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=100003114", "weight": 3},
    {"name": "Reuters Business",     "url": "https://feeds.reuters.com/reuters/businessNews",           "weight": 3},
    {"name": "Financial Times",      "url": "https://www.ft.com/rss/home",                             "weight": 3},
    {"name": "WSJ Markets",          "url": "https://feeds.a.dj.com/rss/RSSMarketsMain.xml",           "weight": 3},

    # Canadian specific
    {"name": "Globe & Mail Markets", "url": "https://www.theglobeandmail.com/arc/outboundfeeds/rss/category/investing/", "weight": 3},
    {"name": "Financial Post",       "url": "https://financialpost.com/feed",                          "weight": 3},
    {"name": "CBC Business",         "url": "https://www.cbc.ca/cmlink/rss-business",                  "weight": 2},

    # Policy & political
    {"name": "AP Business",          "url": "https://feeds.apnews.com/apf-business",                   "weight": 2},
    {"name": "Guardian Business",    "url": "https://www.theguardian.com/business/rss",                "weight": 2},

    # Health
    {"name": "WHO News",             "url": "https://www.who.int/rss-feeds/news-english.xml",          "weight": 2},
    {"name": "Investopedia",         "url": "https://www.investopedia.com/feedbuilder/feed/getfeed/?feedName=rss_headline", "weight": 2},

    # Energy & commodities
    {"name": "OilPrice.com",         "url": "https://oilprice.com/rss/main",                           "weight": 2},

    # Central banks
    {"name": "Bank of Canada",       "url": "https://www.bankofcanada.ca/feed/",                       "weight": 3},
    {"name": "Fed Reserve",          "url": "https://www.federalreserve.gov/feeds/press_all.xml",      "weight": 3},
]

SIGNAL_MAP = {

    "trump_tariff_negative": {
        "keywords": ["tariff", "trade war", "import duty", "trade barrier", "protectionist",
                     "sanctions", "trade dispute", "tariff increase", "levy on"],
        "sectors_bullish":  [],
        "sectors_bearish":  ["TSX_EXPORTERS", "TRADE_SENSITIVE_US", "CANADIAN_ENERGY", "CANADIAN_MATERIALS", "AUTOS"],
        "macro_effects":    {"CAD_USD": "negative", "TSX_BROAD": "negative"},
        "score_adjust":     {"safety_weight": +5, "momentum_weight": -3},
        "magnitude":        "HIGH",
        "note":             "Tariffs hurt Canadian exporters and US cross-border freight carriers — reduce exposure"
    },
    "trump_tariff_canada_specific": {
        "keywords": ["canada tariff", "canadian goods", "usmca", "nafta", "25% tariff canada",
                     "steel tariff", "aluminum tariff", "lumber tariff"],
        "sectors_bullish":  ["US_DEFENSE", "US_DOMESTIC"],
        "sectors_bearish":  ["TSX_EXPORTERS", "TRADE_SENSITIVE_US", "TSX_MATERIALS", "TSX_ENERGY", "TSX_AUTOS"],
        "macro_effects":    {"CAD_USD": "very_negative", "TSX_BROAD": "very_negative"},
        "score_adjust":     {"safety_weight": +8, "fhsa_conservative_boost": +5},
        "magnitude":        "CRITICAL",
        "note":             "Direct Canada tariff — defensive FHSA positioning, reduce TSX exporters and US cross-border freight"
    },
    "trump_deregulation_positive": {
        "keywords": ["deregulation", "cut regulations", "executive order business",
                     "reduce corporate tax", "tax cuts", "pro-business"],
        "sectors_bullish":  ["US_FINANCIALS", "US_ENERGY", "US_INDUSTRIALS"],
        "sectors_bearish":  [],
        "macro_effects":    {"USD": "positive", "US_MARKETS": "positive"},
        "score_adjust":     {"momentum_weight": +3, "growth_weight": +2},
        "magnitude":        "MODERATE",
        "note":             "US deregulation positive for US growth stocks in TFSA"
    },
    "war_escalation": {
        "keywords": ["military strike", "invasion", "war escalation", "bombing campaign",
                     "missile attack", "troops deployed", "nato response", "armed conflict",
                     "military offensive", "war breaks out", "conflict escalates"],
        "sectors_bullish":  ["DEFENSE", "GOLD", "OIL", "CYBERSECURITY"],
        "sectors_bearish":  ["TRAVEL", "CONSUMER_DISCRETIONARY", "EMERGING_MARKETS"],
        "macro_effects":    {"GOLD": "very_positive", "OIL": "positive", "RISK_OFF": "true"},
        "score_adjust":     {"safety_weight": +8, "dividend_weight": +3, "growth_weight": -3},
        "magnitude":        "HIGH",
        "note":             "War = risk-off. Boost gold, defense, energy. Reduce growth exposure."
    },
    "middle_east_tension": {
        "keywords": ["israel", "iran", "gaza", "strait of hormuz", "houthi", "red sea",
                     "oil supply disruption", "opec cut", "middle east conflict"],
        "sectors_bullish":  ["OIL", "CANADIAN_ENERGY", "DEFENSE", "GOLD"],
        "sectors_bearish":  ["AIRLINES", "SHIPPING"],
        "macro_effects":    {"OIL_PRICE": "positive", "CANADIAN_ENERGY": "positive"},
        "score_adjust":     {"canadian_energy_boost": +8},
        "magnitude":        "HIGH",
        "note":             "Middle East tension = Canadian energy stocks benefit (ENB, CNQ, SU)"
    },
    "russia_ukraine": {
        "keywords": ["ukraine", "russia sanctions", "russia invasion", "nato ukraine",
                     "ukrainian conflict", "zelensky", "putin", "russian energy"],
        "sectors_bullish":  ["DEFENSE", "EUROPEAN_ENERGY", "GOLD", "AGRICULTURE"],
        "sectors_bearish":  ["EUROPEAN_STOCKS", "NATURAL_GAS"],
        "macro_effects":    {"GOLD": "positive", "WHEAT": "positive"},
        "score_adjust":     {"defense_boost": +6, "gold_boost": +6},
        "magnitude":        "MODERATE",
        "note":             "Russia/Ukraine ongoing — defense and gold remain supported"
    },
    "peace_deal": {
        "keywords": ["ceasefire", "peace deal", "peace agreement", "negotiations succeed",
                     "treaty signed", "conflict ends", "diplomatic solution"],
        "sectors_bullish":  ["TRAVEL", "CONSUMER", "EMERGING_MARKETS", "INDUSTRIALS"],
        "sectors_bearish":  ["DEFENSE", "GOLD"],
        "macro_effects":    {"RISK_ON": "true", "OIL": "slightly_negative"},
        "score_adjust":     {"momentum_weight": +4, "safety_weight": -3},
        "magnitude":        "MODERATE",
        "note":             "Peace = risk-on. Growth and consumer stocks benefit."
    },
    "health_outbreak": {
        "keywords": ["outbreak", "pandemic", "epidemic", "virus spreading", "health emergency",
                     "who alert", "new variant", "lockdown", "quarantine", "contagion",
                     "public health emergency", "mpox", "novel virus"],
        "sectors_bullish":  ["PHARMA", "BIOTECH", "HEALTHCARE", "CONSUMER_STAPLES", "ECOMMERCE"],
        "sectors_bearish":  ["TRAVEL", "HOSPITALITY", "RESTAURANTS", "REITs_RETAIL", "AIRLINES"],
        "macro_effects":    {"RISK_OFF": "true", "HEALTHCARE_DEMAND": "spike"},
        "score_adjust":     {"safety_weight": +6, "healthcare_boost": +8},
        "magnitude":        "HIGH",
        "note":             "Health outbreak = defensive positioning. Pharma, staples, ecommerce."
    },
    "pharma_breakthrough": {
        "keywords": ["fda approved", "drug approved", "clinical trial success", "vaccine approved",
                     "breakthrough therapy", "phase 3 success", "hc canada approved"],
        "sectors_bullish":  ["PHARMA", "BIOTECH"],
        "sectors_bearish":  [],
        "macro_effects":    {},
        "score_adjust":     {"pharma_boost": +10},
        "magnitude":        "MODERATE",
        "note":             "Pharma approvals = sector-specific boost"
    },
    "boc_rate_cut": {
        "keywords": ["bank of canada rate cut", "boc cuts", "tiff macklem cut",
                     "canadian interest rate lower", "boc dovish", "rate reduction canada"],
        "sectors_bullish":  ["CANADIAN_REITS", "CANADIAN_UTILITIES", "CANADIAN_BANKS",
                             "CANADIAN_HOUSING", "TSX_BROAD"],
        "sectors_bearish":  ["CAD_CURRENCY"],
        "macro_effects":    {"CAD_USD": "slightly_negative", "TSX_BROAD": "positive",
                             "CANADIAN_BONDS": "positive"},
        "score_adjust":     {"canadian_reit_boost": +10, "dividend_weight": +4},
        "magnitude":        "HIGH",
        "note":             "BoC cut = REITs, utilities, and dividend stocks rally hard"
    },
    "boc_rate_hold": {
        "keywords": ["bank of canada holds", "boc holds", "rate unchanged canada",
                     "tiff macklem hold", "boc pause"],
        "sectors_bullish":  ["TSX_BROAD"],
        "sectors_bearish":  [],
        "macro_effects":    {"TSX_BROAD": "slightly_positive"},
        "score_adjust":     {},
        "magnitude":        "LOW",
        "note":             "BoC hold — markets typically neutral to slightly positive"
    },
    "boc_rate_hike": {
        "keywords": ["bank of canada hike", "boc raises rate", "rate increase canada",
                     "tiff macklem hike", "boc hawkish"],
        "sectors_bullish":  ["CANADIAN_BANKS", "CAD_CURRENCY"],
        "sectors_bearish":  ["CANADIAN_REITS", "CANADIAN_UTILITIES", "CANADIAN_HOUSING",
                             "GROWTH_STOCKS", "TELECOM"],
        "macro_effects":    {"CAD_USD": "positive", "TSX_REITS": "negative"},
        "score_adjust":     {"safety_weight": +5, "reit_penalty": +8, "bank_boost": +5},
        "magnitude":        "HIGH",
        "note":             "BoC hike = banks up, REITs and utilities down"
    },
    "fed_rate_cut": {
        "keywords": ["federal reserve cut", "fed cuts", "jerome powell cut", "fomc cut",
                     "fed dovish", "rate cut fed", "pivot fed", "fed easing"],
        "sectors_bullish":  ["REITS", "UTILITIES", "GROWTH_STOCKS", "TECH", "SMALL_CAP"],
        "sectors_bearish":  ["USD"],
        "macro_effects":    {"USD": "negative", "US_STOCKS": "positive", "GOLD": "positive"},
        "score_adjust":     {"growth_weight": +5, "momentum_weight": +4},
        "magnitude":        "HIGH",
        "note":             "Fed cut = growth stocks rally. TFSA growth picks get a boost."
    },
    "fed_rate_hike": {
        "keywords": ["federal reserve hike", "fed raises", "fomc hike", "powell hike",
                     "fed hawkish", "rate increase fed", "fed tightening"],
        "sectors_bullish":  ["FINANCIALS", "USD"],
        "sectors_bearish":  ["REITS", "UTILITIES", "GROWTH", "TECH", "BONDS", "TELECOM"],
        "macro_effects":    {"USD": "positive", "GROWTH_STOCKS": "negative"},
        "score_adjust":     {"safety_weight": +5, "value_weight": +3, "growth_weight": -4},
        "magnitude":        "HIGH",
        "note":             "Fed hike = defensive tilt. Value over growth. FHSA more conservative."
    },
    "inflation_hot": {
        "keywords": ["inflation higher", "cpi above", "inflation surges", "prices rising faster",
                     "hot inflation", "core cpi", "inflation surprise"],
        "sectors_bullish":  ["COMMODITIES", "ENERGY", "GOLD", "REAL_ASSETS", "CANADIAN_ENERGY"],
        "sectors_bearish":  ["BONDS", "REITS", "LONG_DURATION", "CONSUMER_DISCRETIONARY"],
        "macro_effects":    {"GOLD": "positive", "ENERGY": "positive"},
        "score_adjust":     {"safety_weight": +3, "canadian_energy_boost": +5},
        "magnitude":        "MODERATE",
        "note":             "Hot inflation = commodities and real assets outperform"
    },
    "inflation_cooling": {
        "keywords": ["inflation cooling", "cpi lower", "disinflation", "inflation falls",
                     "price pressures ease", "inflation below target"],
        "sectors_bullish":  ["GROWTH", "TECH", "REITS", "CONSUMER"],
        "sectors_bearish":  ["COMMODITIES", "GOLD"],
        "macro_effects":    {"GROWTH_STOCKS": "positive", "BONDS": "positive"},
        "score_adjust":     {"growth_weight": +4, "momentum_weight": +3},
        "magnitude":        "MODERATE",
        "note":             "Cooling inflation = growth stocks and tech rally"
    },
    "cad_weakness": {
        "keywords": ["canadian dollar falls", "cad weakens", "loonie drops", "cad/usd lower",
                     "canadian dollar pressure"],
        "sectors_bullish":  ["TSX_EXPORTERS", "VFV.TO", "US_LISTED_STOCKS"],
        "sectors_bearish":  ["CANADIAN_IMPORTERS", "CANADIAN_CONSUMER"],
        "macro_effects":    {"US_EXPOSURE": "more_valuable"},
        "score_adjust":     {"us_etf_boost": +5},
        "magnitude":        "MODERATE",
        "note":             "Weak CAD = your US-listed holdings worth more in CAD"
    },
    "canadian_housing": {
        "keywords": ["canadian housing", "home prices canada", "real estate canada",
                     "housing market canada", "mortgage rate canada"],
        "sectors_bullish":  ["CANADIAN_REITS", "HOME_BUILDERS", "FINANCIALS"],
        "sectors_bearish":  [],
        "macro_effects":    {"FHSA_URGENCY": "note"},
        "score_adjust":     {"reit_boost": +4},
        "magnitude":        "LOW",
        "note":             "Housing news relevant to FHSA timeline"
    },
    "earnings_beats": {
        "keywords": ["beats earnings", "earnings beat", "revenue beat", "above expectations",
                     "strong quarter", "record revenue", "raises guidance", "eps beat"],
        "sectors_bullish":  ["REPORTING_SECTOR"],
        "sectors_bearish":  [],
        "macro_effects":    {"BROAD_SENTIMENT": "positive"},
        "score_adjust":     {"momentum_weight": +3, "growth_weight": +2},
        "magnitude":        "MODERATE",
        "note":             "Strong earnings season = risk-on, growth picks benefit"
    },
    "earnings_misses": {
        "keywords": ["misses earnings", "earnings miss", "below expectations", "disappoints",
                     "cuts guidance", "lowers outlook", "weak quarter", "eps miss"],
        "sectors_bullish":  [],
        "sectors_bearish":  ["REPORTING_SECTOR"],
        "macro_effects":    {"BROAD_SENTIMENT": "negative"},
        "score_adjust":     {"safety_weight": +3, "momentum_weight": -2},
        "magnitude":        "MODERATE",
        "note":             "Earnings misses = defensive tilt, watch FHSA positions"
    },
    "oil_price_spike": {
        "keywords": ["oil price spike", "crude surges", "brent rises", "wti higher",
                     "oil supply cut", "opec cuts", "energy crisis", "oil shortage"],
        "sectors_bullish":  ["CANADIAN_ENERGY", "OIL_PRODUCERS", "PIPELINES"],
        "sectors_bearish":  ["AIRLINES", "CONSUMER", "TRANSPORT"],
        "macro_effects":    {"CAD_USD": "positive", "TSX_ENERGY": "very_positive"},
        "score_adjust":     {"canadian_energy_boost": +8},
        "magnitude":        "HIGH",
        "note":             "Oil spike = TSX energy stocks surge (ENB, CNQ, SU, CVE)"
    },
    "oil_price_drop": {
        "keywords": ["oil price falls", "crude drops", "brent lower", "oil sell-off",
                     "opec increases output", "oil glut", "energy weakness"],
        "sectors_bullish":  ["AIRLINES", "CONSUMER", "TRANSPORT"],
        "sectors_bearish":  ["CANADIAN_ENERGY", "TSX_ENERGY", "PIPELINES"],
        "macro_effects":    {"CAD_USD": "slightly_negative", "TSX_ENERGY": "negative"},
        "score_adjust":     {"canadian_energy_boost": -5},
        "magnitude":        "MODERATE",
        "note":             "Oil drop = Canadian energy stocks under pressure"
    },
}

SECTOR_TICKERS = {
    "CANADIAN_ENERGY":    ["ENB.TO", "TRP.TO", "CNQ.TO", "SU.TO", "CVE.TO", "PPL.TO"],
    "CANADIAN_REITS":     ["REI-UN.TO", "HR-UN.TO", "AP-UN.TO", "CAR-UN.TO", "GRT-UN.TO"],
    "CANADIAN_BANKS":     ["TD.TO", "RY.TO", "BNS.TO", "BMO.TO", "CM.TO", "NA.TO"],
    "CANADIAN_UTILITIES": ["FTS.TO", "AQN.TO", "EMA.TO", "H.TO"],
    "CANADIAN_MATERIALS": ["ABX.TO", "WPM.TO", "AEM.TO", "K.TO", "NTR.TO"],
    "TSX_EXPORTERS":      ["CNR.TO", "CP.TO", "MG.TO", "ATD.TO"],
    "TRADE_SENSITIVE_US": ["UNP", "CSX", "NSC", "FDX", "UPS"],
    "TSX_BROAD":          ["XGRO.TO", "XEQT.TO", "XIU.TO", "XIC.TO", "ZCN.TO"],
    "DEFENSE":            ["LMT", "RTX", "NOC", "GD", "CAE.TO"],
    "GOLD":               ["ABX.TO", "WPM.TO", "AEM.TO", "GLD"],
    "PHARMA":             ["JNJ", "PFE", "ABBV", "MRK", "BMY"],
    "BIOTECH":            ["AMGN", "BIIB", "REGN", "MRNA"],
    "HEALTHCARE":         ["UNH", "CVS", "ISRG", "DXCM"],
    "CONSUMER_STAPLES":   ["WMT", "PG", "KO", "PEP", "L.TO", "MRU.TO", "DOL.TO"],
    "REITS":              ["O", "VICI", "AMT", "REI-UN.TO", "CAR-UN.TO"],
    "UTILITIES":          ["NEE", "DUK", "FTS.TO", "AQN.TO"],
    "GROWTH_STOCKS":      ["SHOP.TO", "NVDA", "MSFT", "GOOGL", "META", "AMZN"],
    "TECH":               ["AAPL", "MSFT", "NVDA", "AMD", "GOOGL"],
    "FINANCIALS":         ["JPM", "BAC", "GS", "MS", "TD.TO", "RY.TO"],
    "US_DOMESTIC":        ["SPY", "VOO", "VTI"],
    "OIL_PRODUCERS":      ["CNQ.TO", "SU.TO", "CVE.TO"],
    "PIPELINES":          ["ENB.TO", "TRP.TO", "PPL.TO"],
    "SMALL_CAP":          ["IWM"],
    "ECOMMERCE":          ["AMZN", "SHOP.TO"],
    "AIRLINES":           ["AC.TO"],
    "TELECOM":            ["BCE.TO", "RCI-B.TO", "T.TO"],
    "AUTOS":              ["GM", "F", "TM"],
    "SHIPPING":           ["AC.TO"],
}


def fetch_news_feed(source):
    try:
        use_rotating = source["name"] in ROTATING_UA_SOURCES
        ua = random.choice(USER_AGENTS) if use_rotating else (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        )
        if use_rotating:
            time.sleep(random.uniform(1, 2))

        req = urllib.request.Request(
            source["url"],
            headers={
                "User-Agent": ua,
                "Accept": "application/rss+xml, application/xml, text/xml, */*",
                "Accept-Language": "en-US,en;q=0.9",
                "Cache-Control": "no-cache",
            }
        )
        # Single retry for transient DNS/network errors (not 403/auth failures)
        _last_exc = None
        for _attempt in range(2):
            try:
                with urllib.request.urlopen(req, timeout=10) as resp:
                    content = resp.read().decode("utf-8", errors="ignore")
                break
            except urllib.error.HTTPError as _he:
                raise  # Don't retry HTTP errors (403, 404, etc.) — they won't change
            except Exception as _ne:
                _last_exc = _ne
                if _attempt == 0:
                    import time as _t; _t.sleep(2)
        else:
            raise _last_exc

        root = ET.fromstring(content)
        channel = root.find("channel") or root

        articles = []
        for item in channel.findall(".//item")[:15]:
            title = item.findtext("title", "") or ""
            desc  = item.findtext("description", "") or ""
            link  = item.findtext("link", "") or ""
            date  = item.findtext("pubDate", "") or ""
            clean = re.sub("<[^<]+?>", " ", desc)
            clean = re.sub(r"\s+", " ", clean).strip()
            articles.append({
                "title":  title.strip(),
                "desc":   clean[:300],
                "link":   link,
                "date":   date[:25],
                "source": source["name"],
                "weight": source["weight"]
            })

        return {"source": source["name"], "articles": articles, "status": "ok"}

    except Exception as e:
        return {"source": source["name"], "articles": [], "status": f"error: {type(e).__name__}: {str(e)}"}


def extract_signals_from_articles(articles):
    detected = defaultdict(lambda: {
        "count": 0, "confidence": 0, "articles": [], "signal_data": None
    })

    for article in articles:
        text = f"{article['title']} {article['desc']}".lower()
        src_weight = article.get("weight", 1)

        for signal_name, signal_data in SIGNAL_MAP.items():
            hits = 0
            matched_keywords = []
            for kw in signal_data["keywords"]:
                if kw.lower() in text:
                    hits += 1
                    matched_keywords.append(kw)
            if hits > 0:
                confidence = min(100, hits * 25 * src_weight)
                detected[signal_name]["count"]      += hits
                detected[signal_name]["confidence"] = min(100, detected[signal_name]["confidence"] + confidence)
                detected[signal_name]["signal_data"] = signal_data
                detected[signal_name]["articles"].append({
                    "title": article["title"], "source": article["source"],
                    "keywords": matched_keywords
                })

    return detected


def build_sector_sentiment(detected_signals):
    sector_scores = defaultdict(lambda: {"bullish": 0, "bearish": 0, "signals": []})

    for signal_name, detection in detected_signals.items():
        if detection["confidence"] < 20:
            continue
        sig = detection["signal_data"]
        conf = detection["confidence"]
        mag_mult = {"CRITICAL": 3, "HIGH": 2, "MODERATE": 1.5, "LOW": 1}.get(sig.get("magnitude", "LOW"), 1)

        for sector in sig.get("sectors_bullish", []):
            sector_scores[sector]["bullish"] += conf * mag_mult
            sector_scores[sector]["signals"].append({"name": signal_name, "direction": "bullish", "confidence": conf})
        for sector in sig.get("sectors_bearish", []):
            sector_scores[sector]["bearish"] += conf * mag_mult
            sector_scores[sector]["signals"].append({"name": signal_name, "direction": "bearish", "confidence": conf})

    result = {}
    for sector, data in sector_scores.items():
        net = data["bullish"] - data["bearish"]
        result[sector] = {
            "bullish_score": round(data["bullish"], 0),
            "bearish_score": round(data["bearish"], 0),
            "net_score":     round(net, 0),
            "sentiment":     "BULLISH" if net > 50 else "BEARISH" if net < -50 else "NEUTRAL",
            "signals":       data["signals"][:5]
        }
    return result


def build_ticker_adjustments(sector_sentiment, detected_signals):
    ticker_adj = defaultdict(lambda: {"adjustment": 0, "reasons": [], "news_sentiment": "NEUTRAL"})

    for sector, sentiment in sector_sentiment.items():
        if sector not in SECTOR_TICKERS:
            continue
        net = sentiment["net_score"]
        adj = max(-20, min(20, net / 15))
        for ticker in SECTOR_TICKERS[sector]:
            ticker_adj[ticker]["adjustment"] += adj
            if abs(adj) > 2:
                direction = "📈" if adj > 0 else "📉"
                ticker_adj[ticker]["reasons"].append(f"{direction} {sector} ({sentiment['sentiment']})")

    for ticker in ticker_adj:
        ticker_adj[ticker]["adjustment"] = round(max(-25, min(25, ticker_adj[ticker]["adjustment"])), 1)
        adj = ticker_adj[ticker]["adjustment"]
        ticker_adj[ticker]["news_sentiment"] = "BULLISH" if adj > 5 else "BEARISH" if adj < -5 else "NEUTRAL"

    return dict(ticker_adj)


def build_score_weight_adjustments(detected_signals):
    adjustments = {
        "safety_weight": 0, "momentum_weight": 0, "growth_weight": 0,
        "dividend_weight": 0, "value_weight": 0,
        "regime": "NORMAL", "regime_note": "No major macro signals detected"
    }

    for signal_name, detection in detected_signals.items():
        if detection["confidence"] < 30:
            continue
        sig = detection["signal_data"]
        for key, val in sig.get("score_adjust", {}).items():
            if key in adjustments:
                adjustments[key] = max(-10, min(10, adjustments[key] + val))

    safety = adjustments["safety_weight"]
    growth = adjustments["growth_weight"]

    if safety > 8:
        adjustments["regime"] = "RISK_OFF"
        adjustments["regime_note"] = "Multiple risk signals — defensive positioning recommended"
    elif safety > 4:
        adjustments["regime"] = "CAUTIOUS"
        adjustments["regime_note"] = "Elevated uncertainty — slight defensive tilt"
    elif growth > 6:
        adjustments["regime"] = "RISK_ON"
        adjustments["regime_note"] = "Positive macro signals — growth stocks favoured"
    elif growth > 3:
        adjustments["regime"] = "CONSTRUCTIVE"
        adjustments["regime_note"] = "Moderately positive environment"

    return adjustments


def generate_headline_summary(detected_signals, articles_all):
    active_signals = [(name, data) for name, data in detected_signals.items()
                      if data["confidence"] >= 30]
    active_signals.sort(key=lambda x: x[1]["confidence"], reverse=True)

    summary_lines = []
    for name, data in active_signals[:6]:
        sig  = data["signal_data"]
        conf = data["confidence"]
        mag  = sig.get("magnitude", "LOW")
        note = sig.get("note", "")
        summary_lines.append({
            "signal": name.replace("_", " ").title(),
            "magnitude": mag, "confidence": conf,
            "bars": "█" * min(5, int(conf / 20)),
            "note": note,
            "articles": [a["title"] for a in data["articles"][:2]]
        })

    top_headlines = []
    seen = set()
    for art in articles_all:
        if art["title"] not in seen and len(art["title"]) > 20:
            top_headlines.append({"title": art["title"], "source": art["source"]})
            seen.add(art["title"])
        if len(top_headlines) >= 10:
            break

    return {"active_signals": summary_lines, "top_headlines": top_headlines}


def run_news_analysis(verbose=True):
    now = datetime.now()
    if verbose:
        print(f"\n📰 NEWS ANALYSIS — {now.strftime('%B %d, %Y %I:%M %p')}")
        print(f"   Fetching {len(NEWS_SOURCES)} news sources...")

    all_articles = []
    feed_results = []

    for source in NEWS_SOURCES:
        result = fetch_news_feed(source)
        feed_results.append(result)
        all_articles.extend(result["articles"])
        if verbose:
            if result["status"] == "ok" and len(result["articles"]) == 0:
                status = f"⚠️  0 articles (endpoint OK — BoC feed may have no recent items)"
            elif result["status"] == "ok":
                status = f"✅ {len(result['articles'])} articles"
            else:
                status = f"❌ error: {result['status'][:30]}"
            print(f"   {source['name']:<25} {status}")

    if verbose:
        online = sum(1 for f in feed_results if f["status"] == "ok")
        print(f"\n   Feeds online: {online}/{len(NEWS_SOURCES)} | Articles: {len(all_articles)}")

    if verbose: print("\n🧠 Extracting market signals...")
    detected = extract_signals_from_articles(all_articles)

    active = [(n, d) for n, d in detected.items() if d["confidence"] >= 30]
    if verbose:
        print(f"   Active signals detected: {len(active)}")
        for name, data in sorted(active, key=lambda x: x[1]["confidence"], reverse=True)[:8]:
            mag = data["signal_data"].get("magnitude", "?")
            print(f"   [{mag:8}] {name:<35} confidence: {data['confidence']}")

    # ── Signal EMA smoothing ─────────────────────────────────────────────────
    # Prevents confidence spikes from single-run noise (e.g. oil_price_spike 50%→100%)
    # Loads prior smoothed confidence from signal_state.json, applies EMA alpha=0.5
    _sig_state_path = "signal_state.json"
    try:
        with open(_sig_state_path) as _sf:
            _sig_state = json.load(_sf)
    except (FileNotFoundError, json.JSONDecodeError):
        _sig_state = {}

    for _sig_name, _sig_data in detected.items():
        _raw_conf  = _sig_data["confidence"]
        _prior_ema = _sig_state.get(_sig_name, {}).get("ema_confidence", _raw_conf)
        _smoothed  = round(0.50 * _raw_conf + 0.50 * _prior_ema)
        _sig_data["confidence"]     = _smoothed
        _sig_data["raw_confidence"] = _raw_conf
        _sig_state[_sig_name]       = {"ema_confidence": _smoothed}

    try:
        with open(_sig_state_path, "w") as _sf:
            json.dump(_sig_state, _sf)
    except Exception:
        pass

    sector_sentiment   = build_sector_sentiment(detected)
    ticker_adjustments = build_ticker_adjustments(sector_sentiment, detected)
    weight_adjustments = build_score_weight_adjustments(detected)
    headline_summary   = generate_headline_summary(detected, all_articles)

    result = {
        "timestamp":          now.isoformat(),
        "date":               now.strftime("%B %d, %Y"),
        "articles_fetched":   len(all_articles),
        "feeds_online":       sum(1 for f in feed_results if f["status"] == "ok"),
        "feeds_total":        len(NEWS_SOURCES),
        "signals_detected":   len(active),
        "active_signals":     {n: {"confidence": d["confidence"], "note": d["signal_data"].get("note", "")}
                               for n, d in active},
        "sector_sentiment":   sector_sentiment,
        "ticker_adjustments": ticker_adjustments,
        "weight_adjustments": weight_adjustments,
        "headline_summary":   headline_summary,
        "macro_regime":       weight_adjustments["regime"],
        "regime_note":        weight_adjustments["regime_note"],
    }

    if verbose:
        print(f"\n🌍 MACRO REGIME: {result['macro_regime']}")
        print(f"   {result['regime_note']}")

        if sector_sentiment:
            print(f"\n📊 SECTOR SENTIMENT:")
            for sector, data in sorted(sector_sentiment.items(),
                                       key=lambda x: abs(x[1]["net_score"]), reverse=True)[:8]:
                icon = "🟢" if data["sentiment"] == "BULLISH" else "🔴" if data["sentiment"] == "BEARISH" else "🟡"
                print(f"   {icon} {sector:<30} net: {data['net_score']:+.0f}")

        boosted   = [(t, d) for t, d in ticker_adjustments.items() if d["adjustment"] > 3]
        penalized = [(t, d) for t, d in ticker_adjustments.items() if d["adjustment"] < -3]

        if boosted:
            print(f"\n📈 NEWS-BOOSTED TICKERS:")
            for ticker, data in sorted(boosted, key=lambda x: x[1]["adjustment"], reverse=True)[:8]:
                print(f"   +{data['adjustment']:4.1f}  {ticker:<12}  {', '.join(data['reasons'][:2])}")

        if penalized:
            print(f"\n📉 NEWS-PENALIZED TICKERS:")
            for ticker, data in sorted(penalized, key=lambda x: x[1]["adjustment"])[:8]:
                print(f"   {data['adjustment']:5.1f}  {ticker:<12}  {', '.join(data['reasons'][:2])}")

    return result


if __name__ == "__main__":
    result = run_news_analysis(verbose=True)
    with open("news_analysis.json", "w") as f:
        json.dump(result, f, indent=2, default=str)
    print(f"\n💾 Saved to news_analysis.json")
    print(f"🎯 {result['signals_detected']} active signals across {result['articles_fetched']} articles")
