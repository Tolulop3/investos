"""
InvestOS — Portfolio Engine
============================
Broker-agnostic. Scale-agnostic. Deploy $30 or $1,000 — same rules.

v2 changes:
  - SECTOR_MAP in apply_sector_cap() redesigned:
    TECH split into TECH_SEMIS / TECH_SOFTWARE / TECH_INTERNET / TECH_CYBER / FINTECH / TECH_CA
    ETF split into ETF_US / ETF_CA / ETF_INTL / ETF_SECTOR
    Result: NVDA + MSFT + GOOGL + META can all appear (genuinely different subsectors)
  - @easyeatsbodega removed from x_accounts (food account, not finance)
"""

import json
import urllib.request
import urllib.parse
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET

CONFIG = {
    "investor": {
        "age":          33,
        "risk_profile": "high_calculated",
        "strategy":     ["growth", "income", "dividends", "fx", "crypto"],
        "currency":     "CAD",
    },
    "accounts": {
        "FHSA": {
            "balance":          10,
            "max_drawdown_pct": 16,
            "style":            "conservative_growth",
            "venue":            "STOCK_ACCOUNT",
            "tag":              "[FHSA]",
        },
        "TFSA": {
            "balance":          10,
            "style":            "growth_income",
            "venue":            "STOCK_ACCOUNT",
            "tag":              "[TFSA]",
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
            "max_single_position_pct": 10,
            "max_swing_per_trade_pct":  2,
            "max_crypto_total_pct":     5,
        },
    },
    "venues": {
        "STOCK_ACCOUNT":  "Any broker with TSX + US access",
        "FX_ACCOUNT":     "Any FX margin platform",
        "CRYPTO_ACCOUNT": "Any spot crypto or crypto ETF",
    },
    "markets": {
        "canadian_stocks": True,
        "us_stocks":       True,
        "global_etfs":     True,
        "fx_pairs":        True,
        "crypto":          True,
    },
    # v2: removed @easyeatsbodega (food/restaurant account, not finance)
    # @ssaasquatch kept (saas_tech_picks — relevant, intermittent failures acceptable)
    "x_accounts": [
        {"handle": "nolimitgains",     "focus": "macro_market_analysis",  "weight": "macro_context"},
        {"handle": "juliuselum",       "focus": "wealth_building",        "weight": "mindset_strategy"},
        {"handle": "aleabitoreddit",   "focus": "retail_sentiment",       "weight": "momentum_signal"},
        {"handle": "amitisinvesting",  "focus": "stock_picks",            "weight": "high_signal_picks"},
        {"handle": "olumidecapital",   "focus": "capital_investing",      "weight": "position_ideas"},
        {"handle": "optionsbuffett",   "focus": "options_flow",           "weight": "options_signal"},
        {"handle": "sjcapitalinvest",  "focus": "capital_markets_picks",  "weight": "high_signal_picks"},
        {"handle": "clintoptions",     "focus": "options_flow_trades",    "weight": "options_signal"},
        {"handle": "ssaasquatch",      "focus": "saas_tech_picks",        "weight": "growth_signal"},
    ],
    "risk_rules": {
        "max_picks_per_sector":       2,
        "swing_earnings_buffer_days": 14,
        "score_velocity_weight":      0.25,
        "min_conviction_score":       60,
        "stop_loss_pct": {
            "FHSA":        16,
            "floor":        8,
            "model_picks": 12,
            "swing":       10,
            "crypto":      20,
        },
    },
    "watchlist": {
        "conservative": ["XGRO.TO", "XEQT.TO", "VFV.TO", "ZCN.TO", "XIU.TO",
                         "VEA", "VWO", "VGK", "EWJ", "EWC"],
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


def fetch_stock_data(ticker):
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{urllib.parse.quote(ticker)}?interval=1d&range=3mo"
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
        week52_high = meta.get('fiftyTwoWeekHigh', 0)
        week52_low  = meta.get('fiftyTwoWeekLow', 0)
        drawdown_from_high = ((current_price - week52_high) / week52_high * 100) if week52_high else 0
        closes = result['indicators']['quote'][0].get('close', [])
        closes = [c for c in closes if c is not None]
        perf_30d = ((closes[-1]-closes[-22])/closes[-22]*100) if len(closes)>=22 else 0
        perf_90d = ((closes[-1]-closes[0])/closes[0]*100) if len(closes)>1 else 0
        return {
            "ticker": ticker, "price": round(current_price,2),
            "day_change_pct": round(day_change_pct,2),
            "week52_high": round(week52_high,2), "week52_low": round(week52_low,2),
            "drawdown_from_high_pct": round(drawdown_from_high,2),
            "perf_30d": round(perf_30d,2), "perf_90d": round(perf_90d,2),
            "volume": meta.get('regularMarketVolume',0), "status": "ok"
        }
    except Exception as e:
        return {"ticker": ticker, "status": "error", "error": str(e), "price": 0}


def fetch_dividend_info(ticker):
    try:
        url = f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{urllib.parse.quote(ticker)}?modules=summaryDetail,defaultKeyStatistics"
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=10) as response:
            data = json.loads(response.read().decode())
        summary = data['quoteSummary']['result'][0]['summaryDetail']
        div_yield    = summary.get('dividendYield', {}).get('raw', 0) or 0
        div_rate     = summary.get('dividendRate', {}).get('raw', 0) or 0
        ex_div_date  = summary.get('exDividendDate', {}).get('fmt', 'N/A') or 'N/A'
        payout_ratio = summary.get('payoutRatio', {}).get('raw', 0) or 0
        return {"dividend_yield_pct": round(div_yield*100,2), "annual_dividend": round(div_rate,2),
                "ex_dividend_date": ex_div_date, "payout_ratio_pct": round(payout_ratio*100,1)}
    except:
        return {"dividend_yield_pct":0,"annual_dividend":0,"ex_dividend_date":"N/A","payout_ratio_pct":0}


def fetch_rss_signals(handle):
    nitter_instances = [
        f"https://nitter.privacydev.net/{handle}/rss",
        f"https://nitter.poast.org/{handle}/rss",
        f"https://nitter.net/{handle}/rss"
    ]
    for rss_url in nitter_instances:
        try:
            req = urllib.request.Request(rss_url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=8) as response:
                content = response.read().decode('utf-8', errors='ignore')
            root = ET.fromstring(content)
            channel = root.find('channel')
            items = channel.findall('item') if channel else []
            posts = []
            for item in items[:5]:
                title = item.findtext('title', '') or ''
                description = item.findtext('description', '') or ''
                pub_date = item.findtext('pubDate', '') or ''
                import re
                clean_desc = re.sub('<[^<]+?>', '', description)
                clean_desc = clean_desc.replace('&amp;','&').replace('&lt;','<').replace('&gt;','>').replace('&quot;','"')
                posts.append({"text": f"{title} {clean_desc}".strip()[:500], "date": pub_date[:16]})
            return {"handle": handle, "posts": posts, "status": "ok"}
        except:
            continue
    return {"handle": handle, "posts": [], "status": "unavailable"}


def extract_tickers_from_text(text):
    import re
    tickers = re.findall(r'\$([A-Z]{1,5}(?:\.[A-Z]{1,2})?)', text.upper())
    standalone = re.findall(r'\b(PLTR|NVDA|AMD|HOOD|SHOP|TD|RY|ENB|TSLA|AAPL|MSFT|AMZN|META|SOFI|MSTR|BTC|ETH)\b', text.upper())
    return list(set(tickers + standalone))


def aggregate_x_signals(x_feeds):
    all_tickers = {}
    sentiment_summary = []
    for feed in x_feeds:
        handle = feed['handle']
        posts  = feed.get('posts', [])
        if not posts: continue
        account_config = next((a for a in CONFIG['x_accounts'] if a['handle']==handle), {})
        weight = account_config.get('weight','general')
        focus  = account_config.get('focus','general')
        account_tickers = []; account_text = ""
        for post in posts[:3]:
            text = post.get('text','')
            tickers = extract_tickers_from_text(text)
            account_tickers.extend(tickers)
            account_text += f" {text}"
        for ticker in account_tickers:
            if ticker not in all_tickers:
                all_tickers[ticker] = {"count":0,"sources":[],"weight_score":0}
            all_tickers[ticker]["count"] += 1
            all_tickers[ticker]["sources"].append(f"@{handle}")
            if weight == "high_signal_picks": all_tickers[ticker]["weight_score"] += 3
            elif weight in ["macro_context","position_ideas"]: all_tickers[ticker]["weight_score"] += 2
            else: all_tickers[ticker]["weight_score"] += 1
        if account_text.strip():
            bullish_words = ['bull','buy','long','breakout','upside','growth','target','calls','moon','🚀','📈','accumulate']
            bearish_words = ['bear','sell','short','breakdown','avoid','risk','puts','📉','dump','overvalued']
            text_lower = account_text.lower()
            bull_score = sum(1 for w in bullish_words if w in text_lower)
            bear_score = sum(1 for w in bearish_words if w in text_lower)
            sentiment = "🟢 Bullish" if bull_score>bear_score else "🔴 Bearish" if bear_score>bull_score else "🟡 Neutral"
            if account_tickers:
                sentiment_summary.append({"handle":handle,"sentiment":sentiment,
                                          "tickers":list(set(account_tickers))[:5],
                                          "focus":focus,"latest_post":posts[0].get('text','')[:200] if posts else ""})
    top_signals = sorted(all_tickers.items(), key=lambda x: x[1]['weight_score'], reverse=True)[:10]
    return {"top_tickers":[{"ticker":t,"data":d} for t,d in top_signals],
            "account_summaries":sentiment_summary}


def project_portfolio_growth(balance, monthly_contribution, annual_return_pct, years):
    projections = {}
    monthly_rate = annual_return_pct / 100 / 12
    current = balance
    for month in range(1, years*12+1):
        current = current*(1+monthly_rate)+monthly_contribution
        yr = month//12
        if month%12==0 and yr<=years:
            projections[f"{yr}yr"] = round(current,2)
    return {"projections":projections,"balance":balance,"monthly_contrib":monthly_contribution,
            "annual_return_pct":annual_return_pct,
            "note":f"Projection assumes {annual_return_pct}% annual return + ${monthly_contribution}/mo contributions. Not guaranteed."}


def compute_bucket_allocation(account_balance):
    buckets = CONFIG["accounts"]["TFSA"]["buckets"]
    result  = {}
    for name, b in buckets.items():
        pct     = b["pct"]/100
        dollars = round(account_balance*pct,2)
        if name=="floor":         max_pos = round(min(dollars*0.25,account_balance*0.10),2)
        elif name=="model_picks": max_pos = round(min(dollars*0.20,account_balance*0.10),2)
        elif name=="swing":
            raw = round(account_balance*CONFIG["accounts"]["TFSA"]["max_swing_per_trade_pct"]/100,2)
            max_pos = max(10,min(200,raw))
        elif name=="crypto":      max_pos = round(min(dollars*0.60,account_balance*0.03),2)
        else:                     max_pos = round(dollars*0.20,2)
        result[name] = {"pct":b["pct"],"dollars":dollars,"max_position":max_pos,
                        "desc":b["desc"],"examples":b.get("examples",[])}
    floor_d = result["floor"]["dollars"]
    result["_survival_check"] = {
        "if_model_fails_completely": f"${floor_d:,.0f} dividend floor still intact",
        "worst_case_tfsa_loss":      f"${round(account_balance*0.50):,.0f}",
        "floor_annual_income":       f"~${round(floor_d*0.06):,.0f}/yr at 6% avg yield",
    }
    return result


def compute_deployment_plan(deploy_amount, account_balance=None, top_picks=None,
                             fx_signals=None, crypto_signals=None, regime="NORMAL"):
    balance  = account_balance or CONFIG["accounts"]["TFSA"]["balance"] or deploy_amount
    buckets  = compute_bucket_allocation(balance)
    stops    = CONFIG["risk_rules"]["stop_loss_pct"]
    regime_scale = {"BULL":1.0,"NORMAL":0.85,"CAUTION":0.65,"BEAR":0.40}.get(regime,0.85)
    plan = {"deploy_amount":deploy_amount,"regime":regime,"regime_scale":regime_scale,
            "splits":{},"skip_buckets":[],"uninvested":0,"venue_map":{},
            "generated_at":datetime.now().isoformat()}
    remaining = deploy_amount

    floor_dollars = round(deploy_amount*buckets["floor"]["pct"]/100*regime_scale,2)
    floor_pick = None
    if top_picks:
        for p in top_picks:
            cat = p.get("pick",{}).get("category","")
            if "INCOME" in cat or "DIVIDEND" in cat or "FLOOR" in cat or "FHSA" in cat:
                floor_pick = p; break
        if not floor_pick:
            non_swing = [p for p in top_picks if "SWING" not in p.get("pick",{}).get("category","")]
            floor_pick = non_swing[0] if non_swing else None
    if floor_pick and floor_dollars>=5:
        price = floor_pick.get("data",{}).get("price",0)
        stop_pct = stops["floor"]/100
        plan["splits"]["floor"] = {"dollars":floor_dollars,"pick":floor_pick["ticker"],
                                    "category":floor_pick.get("pick",{}).get("category",""),
                                    "price":price,"stop":round(price*(1-stop_pct),2) if price else None,
                                    "stop_pct":stops["floor"],"score":floor_pick.get("score",0),
                                    "action":floor_pick.get("pick",{}).get("action",""),
                                    "venue":"STOCK_ACCOUNT","tag":"[TFSA]"}
        plan["venue_map"][floor_pick["ticker"]] = "STOCK_ACCOUNT"
        remaining -= floor_dollars
    else:
        plan["skip_buckets"].append("floor"); plan["uninvested"] += floor_dollars

    model_dollars = round(deploy_amount*buckets["model_picks"]["pct"]/100*regime_scale,2)
    model_pick = None
    if top_picks:
        for p in top_picks:
            cat = p.get("pick",{}).get("category","")
            if "GROWTH" in cat or "CORE" in cat:
                model_pick = p; break
    if model_pick and model_dollars>=5:
        price = model_pick.get("data",{}).get("price",0)
        stop_pct = stops["model_picks"]/100
        plan["splits"]["model_picks"] = {"dollars":model_dollars,"pick":model_pick["ticker"],
                                          "category":model_pick.get("pick",{}).get("category",""),
                                          "price":price,"stop":round(price*(1-stop_pct),2) if price else None,
                                          "stop_pct":stops["model_picks"],"score":model_pick.get("score",0),
                                          "action":model_pick.get("pick",{}).get("action",""),
                                          "venue":"STOCK_ACCOUNT","tag":"[TFSA]"}
        plan["venue_map"][model_pick["ticker"]] = "STOCK_ACCOUNT"
        remaining -= model_dollars
    else:
        plan["skip_buckets"].append("model_picks"); plan["uninvested"] += model_dollars

    swing_dollars = round(deploy_amount*buckets["swing"]["pct"]/100*regime_scale,2)
    swing_pick = None; fx_swing = None
    if fx_signals and fx_signals.get("top_call"):
        top_fx = fx_signals["top_call"]
        if top_fx.get("conviction",0)>=70: fx_swing = top_fx
    if top_picks and not fx_swing:
        for p in top_picks:
            if "SWING" in p.get("pick",{}).get("category",""): swing_pick = p; break
    if fx_swing and swing_dollars>=5:
        plan["splits"]["swing"] = {"dollars":swing_dollars,"pick":fx_swing.get("pair","FX"),
                                    "category":"FX SWING","price":fx_swing.get("entry",0),
                                    "stop":fx_swing.get("stop",0),"stop_pct":stops["swing"],
                                    "conviction":fx_swing.get("conviction",0),
                                    "action":f"{fx_swing.get('direction','?')} {fx_swing.get('pair','')}",
                                    "venue":"FX_ACCOUNT","tag":"[FX]"}
        plan["venue_map"][fx_swing.get("pair","FX")] = "FX_ACCOUNT"
        remaining -= swing_dollars
    elif swing_pick and swing_dollars>=5:
        price = swing_pick.get("data",{}).get("price",0)
        stop_pct = stops["swing"]/100
        plan["splits"]["swing"] = {"dollars":swing_dollars,"pick":swing_pick["ticker"],
                                    "category":"SWING","price":price,
                                    "stop":round(price*(1-stop_pct),2) if price else None,
                                    "stop_pct":stops["swing"],"score":swing_pick.get("score",0),
                                    "action":swing_pick.get("pick",{}).get("action",""),
                                    "venue":"STOCK_ACCOUNT","tag":"[TFSA]"}
        plan["venue_map"][swing_pick["ticker"]] = "STOCK_ACCOUNT"
        remaining -= swing_dollars
    else:
        plan["skip_buckets"].append("swing"); plan["uninvested"] += swing_dollars

    crypto_dollars = round(deploy_amount*buckets["crypto"]["pct"]/100,2)
    crypto_pick = None
    if crypto_signals and crypto_signals.get("assets"):
        assets = crypto_signals["assets"]
        for symbol in ["BTC-USD","SOL-USD"]:
            a = assets.get(symbol,{})
            if a.get("direction")=="LONG" and a.get("conviction",0)>=55:
                crypto_pick = a; break
    if crypto_pick and crypto_dollars>=1:
        plan["splits"]["crypto"] = {"dollars":crypto_dollars,"pick":crypto_pick.get("name","BTC"),
                                     "category":"CRYPTO","price":crypto_pick.get("price",0),
                                     "stop":crypto_pick.get("stop",0),"stop_pct":stops["crypto"],
                                     "conviction":crypto_pick.get("conviction",0),
                                     "action":crypto_pick.get("action",""),
                                     "venue":"CRYPTO_ACCOUNT","tag":"[CRYPTO]"}
        plan["venue_map"][crypto_pick.get("name","BTC")] = "CRYPTO_ACCOUNT"
        remaining -= crypto_dollars
    else:
        plan["skip_buckets"].append("crypto"); plan["uninvested"] += crypto_dollars

    plan["uninvested"]    = round(max(0,remaining),2)
    plan["total_deployed"] = round(deploy_amount-plan["uninvested"],2)
    lines = [f"DEPLOY ${deploy_amount:.2f} — {regime} REGIME"]
    for name, s in plan["splits"].items():
        lines.append(f"  {s['tag']} {s['pick']:<12} ${s['dollars']:.2f} → stop ${s['stop'] or '?'} | {s['action'][:50]}")
    if plan["skip_buckets"]:
        lines.append(f"  ⏸  Skipped: {', '.join(plan['skip_buckets'])}")
    if plan["uninvested"]>0:
        lines.append(f"  💵 Uninvested: ${plan['uninvested']:.2f}")
    plan["summary"] = "\n".join(lines)
    return plan


import csv
import os
TRADES_FILE = "trades.csv"
TRADES_HEADERS = ["date","ticker","account","action","price","shares","total_value",
                  "stop_price","target_price","category","status","exit_date",
                  "exit_price","pnl_pct","notes"]

def load_trades():
    if not os.path.exists(TRADES_FILE): return []
    with open(TRADES_FILE, newline="") as f:
        return list(csv.DictReader(f))

def log_trade(ticker, account, action, price, total_value,
              stop_price=0, target_price=0, category="", notes=""):
    exists = os.path.exists(TRADES_FILE)
    with open(TRADES_FILE, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=TRADES_HEADERS)
        if not exists: w.writeheader()
        w.writerow({"date":datetime.now().strftime("%Y-%m-%d"),"ticker":ticker,"account":account,
                    "action":action,"price":price,"shares":round(total_value/price,4) if price else 0,
                    "total_value":total_value,"stop_price":stop_price,"target_price":target_price,
                    "category":category,"status":"OPEN","exit_date":"","exit_price":"","pnl_pct":"","notes":notes})
    print(f"✅ Logged: {action} {ticker} @ ${price} (${total_value})")

def close_trade(ticker, exit_price, notes=""):
    trades = load_trades(); updated = False
    for t in trades:
        if t["ticker"]==ticker and t["status"]=="OPEN":
            entry = float(t["price"]) if t["price"] else 0
            pnl   = round((exit_price-entry)/entry*100,2) if entry else 0
            t["status"]="CLOSED"; t["exit_date"]=datetime.now().strftime("%Y-%m-%d")
            t["exit_price"]=exit_price; t["pnl_pct"]=pnl; t["notes"]=notes or t["notes"]
            updated=True; print(f"✅ Closed {ticker} @ ${exit_price} | P&L: {pnl:+.1f}%"); break
    if updated:
        with open(TRADES_FILE,"w",newline="") as f:
            w=csv.DictWriter(f,fieldnames=TRADES_HEADERS); w.writeheader(); w.writerows(trades)
    else:
        print(f"⚠️  No open trade found for {ticker}")

def get_scorecard():
    trades=load_trades(); closed=[t for t in trades if t["status"]=="CLOSED"]
    open_t=[t for t in trades if t["status"]=="OPEN"]
    if not closed:
        return {"recs_made":len(trades),"open":len(open_t),"closed":0,"wins":0,"losses":0,
                "win_rate_pct":0,"avg_win_pct":0,"avg_loss_pct":0,"total_pnl_pct":0,
                "note":"No closed trades yet"}
    wins=[t for t in closed if float(t.get("pnl_pct",0) or 0)>0]
    losses=[t for t in closed if float(t.get("pnl_pct",0) or 0)<=0]
    avg_win =round(sum(float(t["pnl_pct"]) for t in wins)/len(wins),1)   if wins   else 0
    avg_loss=round(sum(float(t["pnl_pct"]) for t in losses)/len(losses),1) if losses else 0
    all_pnl=[float(t.get("pnl_pct",0) or 0) for t in closed]
    return {"recs_made":len(trades),"open":len(open_t),"closed":len(closed),
            "wins":len(wins),"losses":len(losses),
            "win_rate_pct":round(len(wins)/len(closed)*100,1) if closed else 0,
            "avg_win_pct":avg_win,"avg_loss_pct":abs(avg_loss),
            "total_pnl_pct":round(sum(all_pnl)/len(all_pnl),1) if all_pnl else 0,
            "best_trade":max(all_pnl) if all_pnl else 0,"worst_trade":min(all_pnl) if all_pnl else 0,
            "note":f"{len(open_t)} open positions · {len(closed)} closed"}


def apply_sector_cap(picks, max_per_sector=2, regime=None):
    """
    Enforce sector diversification: max N picks per sector.

    v2 SECTOR_MAP redesign:
      TECH split into genuine subsectors — NVDA, MSFT, GOOGL, META can ALL appear:
        TECH_SEMIS:    NVDA, AMD
        TECH_SOFTWARE: MSFT, AAPL, VEEV
        TECH_INTERNET: GOOGL, META, AMZN
        TECH_CYBER:    CRWD, PANW, ZS, DDOG, SNOW, MDB
        FINTECH:       PLTR, HOOD, SOFI, AFRM, NU
        TECH_CA:       SHOP.TO, CSU.TO, OTEX.TO, BB.TO, DSG.TO

      ETF split into venue-based subsectors:
        ETF_US:     VOO, VTI, QQQ, SPY, IWM, SCHD, VYM etc
        ETF_CA:     XGRO.TO, XEQT.TO, VFV.TO, ZCN.TO, XIU.TO etc
        ETF_INTL:   VEA, VWO, EEM, EFA, VGK, EWJ etc
        ETF_SECTOR: XLF, XLK, XLE, XLV, XLU, XLRE
    """
    if regime is None:
        try:    regime = CONFIG.get("current_regime","NORMAL")
        except: regime = "NORMAL"

    regime_caps = {"BULL":3,"NORMAL":2,"CAUTION":2,"BEAR":1}
    effective_max = regime_caps.get(regime, max_per_sector)

    SECTOR_MAP = {
        # ── Energy ─────────────────────────────────────────────────────
        "ENB.TO":"ENERGY","CNQ.TO":"ENERGY","SU.TO":"ENERGY","CVE.TO":"ENERGY",
        "TRP.TO":"ENERGY","PPL.TO":"ENERGY","WCP.TO":"ENERGY","ARX.TO":"ENERGY",
        # ── Banks ──────────────────────────────────────────────────────
        "TD.TO":"BANKS","RY.TO":"BANKS","BNS.TO":"BANKS","BMO.TO":"BANKS",
        "CM.TO":"BANKS","NA.TO":"BANKS",
        # ── Tech — split into genuine subsectors ───────────────────────
        # Semiconductors
        "NVDA":"TECH_SEMIS","AMD":"TECH_SEMIS",
        # Software platforms
        "MSFT":"TECH_SOFTWARE","AAPL":"TECH_SOFTWARE","VEEV":"TECH_SOFTWARE",
        # Internet/ad platforms
        "GOOGL":"TECH_INTERNET","META":"TECH_INTERNET","AMZN":"TECH_INTERNET",
        # Cybersecurity / cloud data
        "CRWD":"TECH_CYBER","PANW":"TECH_CYBER","ZS":"TECH_CYBER",
        "DDOG":"TECH_CYBER","SNOW":"TECH_CYBER","MDB":"TECH_CYBER",
        # Fintech / high-beta growth
        "PLTR":"FINTECH","HOOD":"FINTECH","SOFI":"FINTECH",
        "AFRM":"FINTECH","NU":"FINTECH","RBLX":"FINTECH","HIMS":"FINTECH",
        # Canadian tech
        "SHOP.TO":"TECH_CA","CSU.TO":"TECH_CA","OTEX.TO":"TECH_CA",
        "BB.TO":"TECH_CA","DSG.TO":"TECH_CA","LSPD.TO":"TECH_CA",
        # ── Telecom ────────────────────────────────────────────────────
        "T.TO":"TELECOM","BCE.TO":"TELECOM","RCI-B.TO":"TELECOM","VZ":"TELECOM",
        # ── ETFs — split by region/type ────────────────────────────────
        "VOO":"ETF_US","VTI":"ETF_US","QQQ":"ETF_US","SPY":"ETF_US","IWM":"ETF_US",
        "SCHD":"ETF_US","VYM":"ETF_US","DVY":"ETF_US","HDV":"ETF_US","DGRO":"ETF_US",
        "XGRO.TO":"ETF_CA","XEQT.TO":"ETF_CA","VFV.TO":"ETF_CA","ZCN.TO":"ETF_CA",
        "XIU.TO":"ETF_CA","XIC.TO":"ETF_CA","XBAL.TO":"ETF_CA","HXT.TO":"ETF_CA",
        "HXS.TO":"ETF_CA","ZEB.TO":"ETF_CA","ZRE.TO":"ETF_CA","ZDV.TO":"ETF_CA",
        "VEA":"ETF_INTL","VWO":"ETF_INTL","EEM":"ETF_INTL","EFA":"ETF_INTL",
        "VGK":"ETF_INTL","EWJ":"ETF_INTL","EWG":"ETF_INTL","EWU":"ETF_INTL",
        "EWA":"ETF_INTL","EWC":"ETF_INTL","INDA":"ETF_INTL","FXI":"ETF_INTL",
        "KWEB":"ETF_INTL","EWZ":"ETF_INTL","URTH":"ETF_INTL","ACWI":"ETF_INTL","MCHI":"ETF_INTL",
        "XLF":"ETF_SECTOR","XLK":"ETF_SECTOR","XLE":"ETF_SECTOR",
        "XLV":"ETF_SECTOR","XLU":"ETF_SECTOR","XLRE":"ETF_SECTOR",
        # ── Consumer ───────────────────────────────────────────────────
        "ATD.TO":"CONSUMER","MRU.TO":"CONSUMER","L.TO":"CONSUMER","DOL.TO":"CONSUMER",
        "WMT":"CONSUMER","TGT":"CONSUMER","SBUX":"CONSUMER","CMG":"CONSUMER",
        "NKE":"CONSUMER","LULU":"CONSUMER","MCD":"CONSUMER","KO":"CONSUMER","PEP":"CONSUMER",
        # ── Infrastructure ─────────────────────────────────────────────
        "BN.TO":"INFRA","BAM.TO":"INFRA","CNR.TO":"INFRA","CP.TO":"INFRA","WSP.TO":"INFRA",
        # ── Utilities ──────────────────────────────────────────────────
        "FTS.TO":"UTILITIES","AQN.TO":"UTILITIES","EMA.TO":"UTILITIES","H.TO":"UTILITIES",
        "NEE":"UTILITIES","DUK":"UTILITIES","SO":"UTILITIES","AEP":"UTILITIES","XEL":"UTILITIES",
        # ── REITs ──────────────────────────────────────────────────────
        "REI-UN.TO":"REIT","HR-UN.TO":"REIT","AP-UN.TO":"REIT","CAR-UN.TO":"REIT",
        "GRT-UN.TO":"REIT","O":"REIT","MAIN":"REIT","STAG":"REIT","VICI":"REIT",
        "AMT":"REIT","PLD":"REIT",
        # ── Healthcare ─────────────────────────────────────────────────
        "JNJ":"HEALTHCARE","MRK":"HEALTHCARE","ABBV":"HEALTHCARE","ABT":"HEALTHCARE",
        "PFE":"HEALTHCARE","BMY":"HEALTHCARE","AMGN":"HEALTHCARE","MDT":"HEALTHCARE",
        "ISRG":"HEALTHCARE","DXCM":"HEALTHCARE","VEEV":"HEALTHCARE",
        # ── Financials ─────────────────────────────────────────────────
        "JPM":"FINANCIALS","BAC":"FINANCIALS","WFC":"FINANCIALS","GS":"FINANCIALS",
        "MS":"FINANCIALS","BLK":"FINANCIALS","BX":"FINANCIALS",
        "MFC.TO":"FINANCIALS","SLF.TO":"FINANCIALS","IAG.TO":"FINANCIALS",
        "FFH.TO":"FINANCIALS","POW.TO":"FINANCIALS",
        # ── Materials / Gold ───────────────────────────────────────────
        "ABX.TO":"MATERIALS","WPM.TO":"MATERIALS","AEM.TO":"MATERIALS","K.TO":"MATERIALS",
        "NTR.TO":"MATERIALS","AGI.TO":"MATERIALS","LUN.TO":"MATERIALS","FM.TO":"MATERIALS",
        # ── EV / Auto ──────────────────────────────────────────────────
        "TSLA":"AUTO","RIVN":"AUTO","F":"AUTO","GM":"AUTO","MG.TO":"AUTO",
    }

    sector_counts = {}
    filtered      = []
    removed       = []

    for pick in picks:
        ticker = pick["ticker"]
        sector = SECTOR_MAP.get(ticker, "OTHER")
        count  = sector_counts.get(sector, 0)
        if count < effective_max:
            sector_counts[sector] = count + 1
            pick["sector"] = sector
            filtered.append(pick)
        else:
            removed.append({"ticker":ticker,"sector":sector,
                            "reason":f"Sector cap ({regime}): already {effective_max} {sector} picks"})

    if removed:
        print(f"   Sector cap [{regime}] removed {len(removed)} picks: "
              f"{', '.join(r['ticker'] for r in removed)}")

    return filtered, removed


def normalize_x_signals_by_frequency(x_feeds, lookback_days=7):
    ticker_mentions = {}
    for feed in x_feeds:
        handle    = feed.get("handle","")
        posts     = feed.get("posts",[])
        post_count = max(1,len(posts))
        freq_weight = min(1.0, 10/post_count)
        for post in posts:
            for ticker in post.get("tickers",[]):
                t = ticker.upper()
                if t not in ticker_mentions: ticker_mentions[t] = {}
                ticker_mentions[t][handle] = ticker_mentions[t].get(handle,0)+freq_weight
    scored = {}
    for ticker, handles in ticker_mentions.items():
        unique_accounts = len(handles)
        total_weight    = sum(handles.values())
        scored[ticker] = {"unique_accounts":unique_accounts,"total_weight":round(total_weight,2),
                          "conviction_mult":min(2.0,1.0+(unique_accounts-1)*0.4),
                          "handles":list(handles.keys())}
    return scored
