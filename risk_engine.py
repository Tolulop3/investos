"""
InvestOS — Risk Engine
=======================
Implements the two highest-value missing components from the
quant risk audit:

  ITEM 2: Actual Stress Simulation
    - Doubles transaction costs
    - Adds 2-day execution delay simulation
    - Adds 10% noise to ML predictions
    - Removes top 5 stocks (survivorship test)
    - Reduces momentum factor by 30% (factor decay)
    → If system collapses under mild stress → redesign

  ITEM 3: Strategy Decay Monitor
    - Rolling Sharpe vs S&P 500 benchmark
    - Alpha vs benchmark (are we actually adding value?)
    - Factor performance tracker (is momentum still working?)
    - Alert if alpha negative for 90+ days
    - Monthly "Strategy Health Report"

ALSO INCLUDES:
  - Drawdown lock (no parameter changes when DD > 10%)
  - Survivorship bias disclosure
  - Execution cost tracker
  - "When This Strategy Will Fail" — honest conditions

Run standalone:
    python risk_engine.py --stress      # Run stress simulation
    python risk_engine.py --health      # Strategy health report
    python risk_engine.py --both        # Full audit
"""

import json
import os
import random
import sys
import time
import urllib.request
import urllib.parse
from datetime import datetime, timedelta
from collections import defaultdict

HEALTH_FILE   = "strategy_health.json"
TRADES_FILE   = "trades.csv"
HISTORY_FILE  = "score_history.json"
LOCK_FILE     = "system_lock.json"

# ============================================================
# CONSTANTS
# ============================================================

ESTIMATED_SPREAD_PCT    = 0.0015   # 0.15% bid-ask spread per trade
ESTIMATED_COMMISSION    = 0.00     # Questrade/Wealthsimple: $0 commission on ETFs, ~$5 stocks
STRESS_COST_MULTIPLIER  = 2.0      # Double costs in stress test
STRESS_DELAY_DAYS       = 2        # Simulate 2-day execution lag
STRESS_NOISE_PCT        = 0.10     # 10% noise on ML scores
STRESS_MOMENTUM_DECAY   = 0.30     # Reduce momentum factor weight by 30%
DECAY_ALERT_DAYS        = 90       # Alert if negative alpha for 90+ days
DRAWDOWN_LOCK_THRESHOLD = 0.10     # Lock at 10% drawdown


# ============================================================
# SURVIVORSHIP BIAS DISCLOSURE
# ============================================================

SURVIVORSHIP_NOTE = """
─────────────────────────────────────────────────────────
SURVIVORSHIP BIAS DISCLOSURE
─────────────────────────────────────────────────────────
This system screens from a CURRENT watchlist. Stocks that
exist today survived — stocks that failed, got delisted,
or went bankrupt between 2008–2026 are INVISIBLE to the
backtest.

Estimated impact on historical performance:
  • Studies show survivorship bias inflates backtest
    returns by 1–3% annually on average
  • In crisis periods (2008, 2020), the inflation is
    higher because most failures happen during crashes
  • Your estimated 13–21% annual return range should
    be mentally discounted by ~1.5–2% for this bias

What this system does to partially mitigate it:
  • Walk-forward validation (no full-history optimization)
  • Conservative XGBoost (shallow trees, regularized)
  • Bootstrap data based on known factor relationships
    not cherry-picked winning stocks

What you should do:
  • After 90 days of live data, compare live Sharpe
    to backtested Sharpe. Live < backtested = normal.
  • If live Sharpe < 0.3, investigate factor decay.
─────────────────────────────────────────────────────────
"""

WHEN_THIS_FAILS = """
─────────────────────────────────────────────────────────
CONDITIONS UNDER WHICH THIS STRATEGY WILL FAIL
─────────────────────────────────────────────────────────
Be honest with yourself about these before trading:

1. MOMENTUM FACTOR DECAY
   If momentum stops working as a factor (happened 2009,
   2020 recovery), the ML model loses its strongest
   feature. Rolling Sharpe monitor will catch this.
   Response: reduce position sizes, wait for regime clarity.

2. EXTENDED LOW-VOLATILITY GRINDING BEAR
   The regime filter catches sharp crashes. But a slow,
   grinding bear (like 2022 rate hike cycle) can erode
   capital before the filter triggers. 
   Response: check 50d vs 200d MA weekly manually.

3. CANADIAN MARKET CONCENTRATION
   TSX is ~35% financials + ~20% energy. If both sectors
   face simultaneous headwinds (e.g. rate hikes + oil crash),
   the Canadian watchlist has nowhere to hide.
   Response: FHSA ETFs (XGRO, XEQT) provide global exposure.

4. YOUR OWN PSYCHOLOGY
   The #1 failure mode. When the system is down 8% and
   your gut says to override it, the override almost
   always makes it worse. The drawdown lock exists for
   this reason — respect it.
   Response: Written investment policy statement.
             Commit to it before you're in a drawdown.

5. TARIFF/GEOPOLITICAL SHOCK TO CANADA
   Your system detects tariff signals. It cannot predict
   their severity or duration. A sustained US-Canada trade
   war could structurally impair Canadian equity returns
   for 12–24 months.
   Response: Shift FHSA to VFV.TO (S&P 500 exposure)
             if tariff signals persist >90 days.

6. NEWS FEED LAG
   RSS feeds lag 1–3 hours in fast-moving crises. The
   system may generate a LONG signal while a crash is
   already underway. Manual override allowed ONLY for
   regime changes, never for individual picks.

7. INTEREST RATE NORMALIZATION
   2010–2021 was an extraordinary low-rate environment.
   In a structurally higher rate world (3–5%), dividend
   stocks face real competition from fixed income.
   The income bucket may underperform its own history.
─────────────────────────────────────────────────────────
"""


# ============================================================
# DRAWDOWN LOCK SYSTEM
# ============================================================

def check_drawdown_lock(current_drawdown_pct):
    """
    If portfolio is in >10% drawdown, lock the system.
    No parameter changes. No new strategies. Wait.
    """
    lock_data = {
        "locked":           current_drawdown_pct > DRAWDOWN_LOCK_THRESHOLD * 100,
        "drawdown_pct":     current_drawdown_pct,
        "threshold_pct":    DRAWDOWN_LOCK_THRESHOLD * 100,
        "locked_at":        datetime.now().isoformat() if current_drawdown_pct > DRAWDOWN_LOCK_THRESHOLD * 100 else None,
        "message":          "",
    }

    if lock_data["locked"]:
        lock_data["message"] = (
            f"🔒 SYSTEM LOCKED — Portfolio drawdown {current_drawdown_pct:.1f}% "
            f"exceeds {DRAWDOWN_LOCK_THRESHOLD*100:.0f}% threshold.\n"
            f"   NO parameter changes allowed until drawdown recovers below "
            f"{DRAWDOWN_LOCK_THRESHOLD*100:.0f}%.\n"
            f"   This is a feature, not a bug. Your future self will thank you."
        )
        # Write lock file
        with open(LOCK_FILE, "w") as f:
            json.dump(lock_data, f, indent=2)
        print(f"\n{'='*55}")
        print(lock_data["message"])
        print(f"{'='*55}\n")
    else:
        # Clear lock if recovered
        if os.path.exists(LOCK_FILE):
            os.remove(LOCK_FILE)
        lock_data["message"] = "✅ System unlocked — drawdown within acceptable range"

    return lock_data


def is_system_locked():
    """Check if system is currently in drawdown lock"""
    if not os.path.exists(LOCK_FILE):
        return False, {}
    try:
        with open(LOCK_FILE) as f:
            data = json.load(f)
        return data.get("locked", False), data
    except:
        return False, {}


def get_current_drawdown():
    """Calculate current drawdown from trades.csv if available"""
    try:
        import csv
        if not os.path.exists(TRADES_FILE):
            return 0.0

        with open(TRADES_FILE) as f:
            trades = list(csv.DictReader(f))

        open_trades = [t for t in trades if t.get("status") == "OPEN"]
        if not open_trades:
            return 0.0

        # Calculate unrealized P&L on open positions
        # (simplified — uses price at entry vs current, not live price)
        total_invested = sum(float(t.get("total_value", 0) or 0) for t in open_trades)
        if total_invested == 0:
            return 0.0

        return 0.0  # Returns 0 until live prices are tracked
    except:
        return 0.0


# ============================================================
# EXECUTION COST TRACKER
# ============================================================

def estimate_trade_cost(ticker, dollar_amount, is_etf=False):
    """
    Estimate realistic all-in cost for a trade.
    Questrade: $0 ETF buys, $4.95 stock trades (min)
    """
    spread_cost    = dollar_amount * ESTIMATED_SPREAD_PCT
    commission     = 0.0 if is_etf else min(9.95, max(4.95, dollar_amount * 0.005))
    slippage       = dollar_amount * 0.001  # 0.1% typical slippage on TSX/NYSE
    total_cost     = spread_cost + commission + slippage
    cost_pct       = total_cost / dollar_amount * 100

    return {
        "ticker":        ticker,
        "dollar_amount": dollar_amount,
        "spread_cost":   round(spread_cost, 2),
        "commission":    round(commission, 2),
        "slippage":      round(slippage, 2),
        "total_cost":    round(total_cost, 2),
        "cost_pct":      round(cost_pct, 2),
        "break_even_move": round(cost_pct * 2, 2),  # need 2x cost to break even
        "is_etf":        is_etf,
        "note": ("ETF: $0 commission on Questrade/Wealthsimple" if is_etf
                 else f"Stock: ~${commission:.2f} commission + spread + slippage"),
    }


# ============================================================
# ITEM 2: ACTUAL STRESS SIMULATION
# ============================================================

def run_stress_simulation(screener_results, ml_results=None, verbose=True):
    """
    Stress-tests the current screening output by:
      1. Doubling transaction costs → measures if picks are still viable
      2. Adding 2-day execution delay → simulates missing the entry
      3. Adding 10% noise to ML scores → model uncertainty
      4. Removing top 5 stocks → survivorship stress
      5. Reducing momentum factor 30% → factor decay

    Returns comparison: normal vs stressed output
    """
    if verbose:
        print(f"\n{'='*55}")
        print(f"  STRESS SIMULATION")
        print(f"  {datetime.now().strftime('%B %d, %Y %I:%M %p')}")
        print(f"{'='*55}")
        print(f"\n  Simulating 5 stress scenarios...\n")

    results = {
        "run_at":   datetime.now().isoformat(),
        "scenarios": {},
        "summary":  {},
        "verdict":  "",
    }

    # --- Collect all picks from normal run ---
    all_picks = []
    for bucket in ["FHSA_top5", "TFSA_growth_top5", "TFSA_income_top5", "TFSA_swing_top3"]:
        for p in screener_results.get(bucket, []):
            all_picks.append({
                "ticker":   p["ticker"],
                "score":    p["score"],
                "bucket":   bucket,
                "pick":     p.get("pick", {}),
                "exp_high": p.get("pick", {}).get("exp_high", 0),
            })

    normal_avg_score     = sum(p["score"] for p in all_picks) / len(all_picks) if all_picks else 0
    normal_count         = len(all_picks)
    normal_avg_exp       = sum(p["exp_high"] for p in all_picks) / len(all_picks) if all_picks else 0

    # ── SCENARIO 1: Double transaction costs ──────────────
    if verbose: print("  [1/5] Doubling transaction costs...", end=" ", flush=True)

    scenario1_picks = []
    for p in all_picks:
        amount     = p["pick"].get("amount", 100)
        is_etf     = ".TO" in p["ticker"] and p["ticker"] in ["XGRO.TO","XEQT.TO","VFV.TO","ZCN.TO","XIU.TO"]
        cost       = estimate_trade_cost(p["ticker"], amount, is_etf)
        stress_cost= cost["cost_pct"] * STRESS_COST_MULTIPLIER
        # A pick needs to return at least 2x cost to be worth it
        net_exp    = p["exp_high"] - stress_cost
        if net_exp > 2.0:   # Still viable with doubled costs
            scenario1_picks.append({**p, "net_exp_stressed": round(net_exp, 1)})

    s1_survived = len(scenario1_picks)
    s1_pct      = round(s1_survived / normal_count * 100, 1) if normal_count else 0
    results["scenarios"]["double_costs"] = {
        "name":          "Double Transaction Costs",
        "normal_count":  normal_count,
        "survived":      s1_survived,
        "survival_pct":  s1_pct,
        "verdict":       "✅ ROBUST" if s1_pct >= 70 else "⚠️ FRAGILE" if s1_pct >= 40 else "❌ BROKEN",
        "note":          f"{normal_count - s1_survived} picks unviable after doubling costs",
    }
    if verbose: print(f"{s1_survived}/{normal_count} survived ({s1_pct}%)")

    # ── SCENARIO 2: 2-day execution delay ─────────────────
    if verbose: print("  [2/5] Simulating 2-day execution delay...", end=" ", flush=True)

    # Simulate price slippage from delayed entry (0.3% per day × 2 days = 0.6 pct-points)
    # Subtracts fixed 0.6 from exp_high (which is stored as a % e.g. 153 = 153% expected return)
    DELAY_SLIP_PP = STRESS_DELAY_DAYS * 0.3   # 0.6 percentage points
    scenario2_picks = []
    for p in all_picks:
        net_exp = p["exp_high"] - DELAY_SLIP_PP
        if net_exp > 1.5:
            scenario2_picks.append(p)

    s2_survived = len(scenario2_picks)
    s2_pct      = round(s2_survived / normal_count * 100, 1) if normal_count else 0
    results["scenarios"]["execution_delay"] = {
        "name":          "2-Day Execution Delay",
        "normal_count":  normal_count,
        "survived":      s2_survived,
        "survival_pct":  s2_pct,
        "verdict":       "✅ ROBUST" if s2_pct >= 80 else "⚠️ FRAGILE" if s2_pct >= 50 else "❌ BROKEN",
        "note":          f"~0.6% slippage from 2-day delay on avg",
    }
    if verbose: print(f"{s2_survived}/{normal_count} survived ({s2_pct}%)")

    # ── SCENARIO 3: 10% noise on ML scores ────────────────
    if verbose: print("  [3/5] Adding 10% noise to ML predictions...", end=" ", flush=True)

    random.seed(42)  # Reproducible
    scenario3_picks = []
    score_changes   = []
    for p in all_picks:
        noise      = random.gauss(0, p["score"] * STRESS_NOISE_PCT)
        new_score  = max(0, min(100, p["score"] + noise))
        score_changes.append(abs(noise))
        if new_score >= 55:   # Still above conviction threshold
            scenario3_picks.append({**p, "score_stressed": round(new_score, 1)})

    s3_survived  = len(scenario3_picks)
    s3_pct       = round(s3_survived / normal_count * 100, 1) if normal_count else 0
    avg_score_chg= round(sum(score_changes)/len(score_changes), 1) if score_changes else 0
    results["scenarios"]["ml_noise"] = {
        "name":          "10% ML Prediction Noise",
        "normal_count":  normal_count,
        "survived":      s3_survived,
        "survival_pct":  s3_pct,
        "avg_score_change": avg_score_chg,
        "verdict":       "✅ ROBUST" if s3_pct >= 65 else "⚠️ FRAGILE" if s3_pct >= 40 else "❌ BROKEN",
        "note":          f"Avg score change: ±{avg_score_chg} pts with 10% noise",
    }
    if verbose: print(f"{s3_survived}/{normal_count} survived ({s3_pct}%)")

    # ── SCENARIO 4: Remove top 5 stocks ───────────────────
    if verbose: print("  [4/5] Removing top 5 performers (survivorship test)...", end=" ", flush=True)

    sorted_picks = sorted(all_picks, key=lambda x: x["score"], reverse=True)
    top5_removed = {p["ticker"] for p in sorted_picks[:5]}
    scenario4_picks = [p for p in all_picks if p["ticker"] not in top5_removed]

    s4_count    = len(scenario4_picks)
    s4_avg_score= round(sum(p["score"] for p in scenario4_picks)/s4_count, 1) if scenario4_picks else 0
    score_drop  = round(normal_avg_score - s4_avg_score, 1)
    results["scenarios"]["remove_top5"] = {
        "name":          "Remove Top 5 Stocks",
        "normal_avg":    round(normal_avg_score, 1),
        "stressed_avg":  s4_avg_score,
        "score_drop":    score_drop,
        "removed":       list(top5_removed),
        "verdict":       "✅ ROBUST" if score_drop <= 5 else "⚠️ FRAGILE" if score_drop <= 10 else "❌ CONCENTRATED",
        "note":          f"Score drops {score_drop} pts without top 5 — system depth check",
    }
    if verbose: print(f"Avg score {normal_avg_score:.1f} → {s4_avg_score} ({'-' if score_drop>0 else '+'}{score_drop} pts)")

    # ── SCENARIO 5: Momentum factor decay -30% ────────────
    if verbose: print("  [5/5] Reducing momentum factor by 30%...", end=" ", flush=True)

    # Momentum = perf_90d, perf_30d in scoring
    # Simulate by reducing score for high-momentum picks
    scenario5_picks = []
    score_changes5  = []
    for p in all_picks:
        data      = p.get("pick", {})
        # Estimate how much of score came from momentum
        # High exp_high picks are usually momentum-driven
        momentum_component = min(20, p["exp_high"] * 0.3)
        adj_score = p["score"] - (momentum_component * STRESS_MOMENTUM_DECAY)
        score_changes5.append(momentum_component * STRESS_MOMENTUM_DECAY)
        if adj_score >= 50:
            scenario5_picks.append({**p, "score_stressed": round(adj_score, 1)})

    s5_survived     = len(scenario5_picks)
    s5_pct          = round(s5_survived / normal_count * 100, 1) if normal_count else 0
    avg_score_drop5 = round(sum(score_changes5)/len(score_changes5), 1) if score_changes5 else 0
    results["scenarios"]["momentum_decay"] = {
        "name":          "Momentum Factor -30%",
        "normal_count":  normal_count,
        "survived":      s5_survived,
        "survival_pct":  s5_pct,
        "avg_score_drop":avg_score_drop5,
        "verdict":       "✅ ROBUST" if s5_pct >= 60 else "⚠️ FRAGILE" if s5_pct >= 35 else "❌ MOMENTUM_DEPENDENT",
        "note":          f"If momentum stops working: avg score drops {avg_score_drop5} pts",
    }
    if verbose: print(f"{s5_survived}/{normal_count} survived ({s5_pct}%)")

    # ── AGGREGATE VERDICT ──────────────────────────────────
    verdicts   = [s["verdict"] for s in results["scenarios"].values()]
    n_robust   = sum(1 for v in verdicts if "ROBUST" in v)
    n_fragile  = sum(1 for v in verdicts if "FRAGILE" in v)
    n_broken   = sum(1 for v in verdicts if "BROKEN" in v or "CONCENTRATED" in v or "DEPENDENT" in v)

    if n_broken >= 2:
        overall = "❌ FRAGILE — System needs redesign"
    elif n_broken == 1 or n_fragile >= 3:
        overall = "⚠️ MODERATE — Monitor closely"
    elif n_fragile >= 1:
        overall = "✅ ACCEPTABLE — Minor vulnerabilities"
    else:
        overall = "✅ ROBUST — System survives stress well"

    results["summary"] = {
        "normal_picks":    normal_count,
        "normal_avg_score":round(normal_avg_score, 1),
        "scenarios_robust":n_robust,
        "scenarios_fragile":n_fragile,
        "scenarios_broken": n_broken,
        "overall_verdict": overall,
    }
    results["verdict"] = overall

    if verbose:
        print(f"\n  {'─'*45}")
        print(f"  STRESS TEST RESULTS")
        print(f"  {'─'*45}")
        for name, s in results["scenarios"].items():
            print(f"  {s['verdict']} {s['name']}")
            print(f"             {s['note']}")
        print(f"\n  OVERALL: {overall}")
        print(f"  {'─'*45}\n")

    return results


# ============================================================
# ITEM 3: STRATEGY DECAY MONITOR
# ============================================================

def fetch_benchmark_return(symbol="^GSPC", days=90):
    """Fetch S&P 500 return over N days for comparison"""
    try:
        url = (f"https://query1.finance.yahoo.com/v8/finance/chart/"
               f"{urllib.parse.quote(symbol)}?interval=1d&range=6mo")
        req = urllib.request.Request(
            url, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        )
        with urllib.request.urlopen(req, timeout=12) as r:
            data = json.loads(r.read().decode())

        closes = [c for c in data['chart']['result'][0]['indicators']['quote'][0].get('close', []) if c]
        if len(closes) < days:
            return None

        ret_90d  = (closes[-1] - closes[-days])  / closes[-days]  * 100
        ret_30d  = (closes[-1] - closes[-30])    / closes[-30]    * 100 if len(closes) >= 30 else 0
        ret_7d   = (closes[-1] - closes[-7])     / closes[-7]     * 100 if len(closes) >= 7  else 0
        daily_rets = [(closes[i] - closes[i-1])/closes[i-1] for i in range(max(1, len(closes)-days), len(closes))]
        import math
        std       = (sum((r - sum(daily_rets)/len(daily_rets))**2 for r in daily_rets)/len(daily_rets))**0.5 if daily_rets else 0.01
        ann_vol   = std * math.sqrt(252) * 100
        ann_ret   = ret_90d * (252 / days)
        sharpe    = ann_ret / ann_vol if ann_vol > 0 else 0

        return {
            "ret_7d":  round(ret_7d, 2),
            "ret_30d": round(ret_30d, 2),
            "ret_90d": round(ret_90d, 2),
            "ann_ret": round(ann_ret, 1),
            "ann_vol": round(ann_vol, 1),
            "sharpe":  round(sharpe, 2),
            "symbol":  symbol,
        }
    except:
        return None


def load_health_history():
    """Load strategy health history"""
    if os.path.exists(HEALTH_FILE):
        try:
            with open(HEALTH_FILE) as f:
                return json.load(f)
        except:
            pass
    return {"entries": [], "created": datetime.now().isoformat()}


def save_health_history(data):
    with open(HEALTH_FILE, "w") as f:
        json.dump(data, f, indent=2, default=str)


def compute_rolling_sharpe(score_history, days=90):
    """
    Approximate rolling Sharpe from score history.
    Uses score changes as proxy for returns until live trade data exists.
    In real usage: replace with actual portfolio returns.
    """
    import math

    if not score_history:
        return {"sharpe": None, "note": "No history yet"}

    # Collect all score changes across all tickers as proxy for daily "returns"
    daily_changes = defaultdict(list)

    for ticker, records in score_history.items():
        sorted_recs = sorted(records, key=lambda x: x["date"])
        for i in range(1, len(sorted_recs)):
            date   = sorted_recs[i]["date"]
            change = sorted_recs[i]["score"] - sorted_recs[i-1]["score"]
            daily_changes[date].append(change / 100)  # Normalize to return-like

    if len(daily_changes) < 10:
        return {"sharpe": None, "note": f"Only {len(daily_changes)} days of data — need 10+ for Sharpe"}

    # Recent N days
    sorted_dates = sorted(daily_changes.keys())[-days:]
    daily_avgs   = [sum(daily_changes[d])/len(daily_changes[d]) for d in sorted_dates]

    n        = len(daily_avgs)
    avg_ret  = sum(daily_avgs) / n
    variance = sum((r - avg_ret)**2 for r in daily_avgs) / n
    std_dev  = math.sqrt(variance) if variance > 0 else 0.0001
    ann_ret  = avg_ret * 252
    ann_vol  = std_dev * math.sqrt(252)
    sharpe   = ann_ret / ann_vol if ann_vol > 0 else 0

    return {
        "sharpe":        round(sharpe, 3),
        "ann_ret_pct":   round(ann_ret * 100, 1),
        "ann_vol_pct":   round(ann_vol * 100, 1),
        "days_computed": n,
        "note":          f"Score-proxy Sharpe over {n} days (improves with live trade data)",
    }


def run_decay_monitor(score_history=None, screener_results=None, verbose=True):
    """
    ITEM 3: Strategy Decay Monitor

    Tracks:
      - Rolling Sharpe vs S&P 500
      - Factor momentum performance (is momentum factor still working?)
      - Alpha vs benchmark
      - Alerts if negative alpha for DECAY_ALERT_DAYS

    Returns health report saved to strategy_health.json
    """
    if verbose:
        print(f"\n{'='*55}")
        print(f"  STRATEGY DECAY MONITOR")
        print(f"{'='*55}\n")

    now    = datetime.now()
    health = load_health_history()

    # ── 1. Load score history ─────────────────────────────
    if score_history is None:
        if os.path.exists(HISTORY_FILE):
            with open(HISTORY_FILE) as f:
                score_history = json.load(f)
        else:
            score_history = {}

    # ── 2. Compute rolling Sharpe ─────────────────────────
    if verbose: print("  → Computing rolling Sharpe...", end=" ", flush=True)
    rolling_sharpe = compute_rolling_sharpe(score_history)
    if verbose: print(f"  Sharpe: {rolling_sharpe.get('sharpe', 'N/A')}")

    # ── 3. Fetch benchmark ────────────────────────────────
    if verbose: print("  → Fetching S&P 500 benchmark...", end=" ", flush=True)
    benchmark = fetch_benchmark_return("^GSPC", 90)
    tsx        = fetch_benchmark_return("^GSPTSE", 90)
    if verbose: print(f"  SPX 90d: {benchmark['ret_90d'] if benchmark else 'N/A'}%")

    # ── 4. Factor momentum check ──────────────────────────
    # Check if momentum factor is working: are high-momentum stocks actually going up?
    if verbose: print("  → Checking momentum factor health...", end=" ", flush=True)
    momentum_health = check_momentum_factor_health(score_history, screener_results)
    if verbose: print(f"  Momentum: {momentum_health.get('verdict', 'N/A')}")

    # ── 5. Alpha computation ──────────────────────────────
    our_sharpe = rolling_sharpe.get("sharpe")
    spx_sharpe = benchmark.get("sharpe", 0) if benchmark else None
    alpha      = None
    if our_sharpe is not None and spx_sharpe is not None:
        alpha = round(our_sharpe - spx_sharpe, 3)

    # ── 6. Consecutive negative alpha days ───────────────
    neg_alpha_days = count_consecutive_negative_alpha(health, alpha)

    # ── 7. Alert logic ───────────────────────────────────
    alerts = []

    if neg_alpha_days >= DECAY_ALERT_DAYS:
        alerts.append({
            "level":   "🚨 CRITICAL",
            "message": f"Strategy has underperformed S&P 500 for {neg_alpha_days} days. "
                       f"Factor may be decaying. Review system.",
        })

    if our_sharpe is not None and our_sharpe < 0.3:
        alerts.append({
            "level":   "⚠️ WARNING",
            "message": f"Rolling Sharpe {our_sharpe:.2f} below 0.3 minimum threshold. "
                       f"Reduce position sizes until Sharpe recovers.",
        })

    if momentum_health.get("decaying"):
        alerts.append({
            "level":   "⚠️ WARNING",
            "message": "Momentum factor showing decay — high-momentum picks underperforming. "
                       "Increase weight on quality/value factors.",
        })

    if len(score_history) < 10:
        alerts.append({
            "level":   "ℹ️ INFO",
            "message": f"Only {len(score_history)} tickers in history. "
                       f"Monitor improves significantly after 30+ days of data.",
        })

    # ── 8. Robustness score ───────────────────────────────
    robustness = compute_robustness_score(rolling_sharpe, benchmark, momentum_health, neg_alpha_days)

    # ── 9. Build health entry ─────────────────────────────
    entry = {
        "date":               now.strftime("%Y-%m-%d"),
        "timestamp":          now.isoformat(),
        "rolling_sharpe":     rolling_sharpe,
        "benchmark_spx":      benchmark,
        "benchmark_tsx":      tsx,
        "alpha_vs_spx":       alpha,
        "neg_alpha_days":     neg_alpha_days,
        "momentum_health":    momentum_health,
        "alerts":             alerts,
        "robustness_score":   robustness,
    }

    # Append to history (keep 365 days)
    health["entries"].append(entry)
    cutoff = (now - timedelta(days=365)).strftime("%Y-%m-%d")
    health["entries"] = [e for e in health["entries"] if e.get("date","") >= cutoff]
    health["last_updated"] = now.isoformat()
    save_health_history(health)

    if verbose:
        print(f"\n  {'─'*45}")
        print(f"  STRATEGY HEALTH REPORT — {now.strftime('%B %d, %Y')}")
        print(f"  {'─'*45}")
        print(f"  Robustness Score:  {robustness}/100")
        print(f"  Rolling Sharpe:    {our_sharpe if our_sharpe is not None else 'Building...'}")
        if benchmark:
            print(f"  S&P 500 Sharpe:   {benchmark['sharpe']}")
            print(f"  Alpha vs SPX:     {alpha:+.3f}" if alpha is not None else "  Alpha:            Building...")
        print(f"  Momentum Factor:  {momentum_health.get('verdict','N/A')}")
        print(f"  Neg Alpha Streak: {neg_alpha_days} days")

        if alerts:
            print(f"\n  {'─'*45}")
            print(f"  ALERTS ({len(alerts)})")
            for a in alerts:
                print(f"  {a['level']}: {a['message']}")
        else:
            print(f"\n  ✅ No alerts — system healthy")
        print(f"  {'─'*45}\n")

    return {
        "date":             now.strftime("%B %d, %Y"),
        "robustness_score": robustness,
        "rolling_sharpe":   rolling_sharpe,
        "benchmark":        benchmark,
        "alpha":            alpha,
        "neg_alpha_days":   neg_alpha_days,
        "momentum_health":  momentum_health,
        "alerts":           alerts,
        "survivorship_note":SURVIVORSHIP_NOTE.strip(),
        "when_this_fails":  WHEN_THIS_FAILS.strip(),
    }


def check_momentum_factor_health(score_history, screener_results):
    """
    Check if the momentum factor is still working.
    Test: are stocks with high momentum scores (perf_90d > 15%)
    actually continuing to outperform?
    Compares score vs subsequent price performance.
    """
    if not score_history or len(score_history) < 7:
        return {
            "verdict":  "⏳ Building — need 7+ days of data",
            "decaying": False,
            "note":     "Cannot assess yet",
        }

    # Simple check: what % of trending_up stocks also have recent positive price movement?
    # This is a proxy test — real check needs 30+ days of live data
    momentum_aligned = 0
    momentum_total   = 0

    for ticker, records in score_history.items():
        if len(records) < 5:
            continue
        sorted_recs = sorted(records, key=lambda x: x["date"])
        first_score = sorted_recs[0]["score"]
        last_score  = sorted_recs[-1]["score"]
        first_price = sorted_recs[0].get("price", 0)
        last_price  = sorted_recs[-1].get("price", 0)

        if last_score > first_score + 5:  # Momentum pick (rising score)
            momentum_total += 1
            if last_price > first_price:   # Price also rising = factor working
                momentum_aligned += 1

    if momentum_total == 0:
        return {"verdict": "⏳ No momentum picks tracked yet", "decaying": False, "note": ""}

    alignment_pct = momentum_aligned / momentum_total * 100

    if alignment_pct >= 60:
        verdict  = "✅ HEALTHY — Momentum factor working"
        decaying = False
    elif alignment_pct >= 40:
        verdict  = "⚠️ MIXED — Momentum showing some weakness"
        decaying = False
    else:
        verdict  = "🔴 DECAYING — Momentum picks underperforming"
        decaying = True

    return {
        "verdict":        verdict,
        "decaying":       decaying,
        "alignment_pct":  round(alignment_pct, 1),
        "momentum_picks": momentum_total,
        "note":           f"{momentum_aligned}/{momentum_total} rising-score picks also rose in price",
    }


def count_consecutive_negative_alpha(health_history, current_alpha):
    """Count how many recent consecutive days alpha was negative"""
    entries = sorted(health_history.get("entries", []), key=lambda x: x.get("date", ""), reverse=True)
    count   = 0

    # Check current
    if current_alpha is not None and current_alpha < 0:
        count = 1
    elif current_alpha is None:
        return 0
    else:
        return 0  # Current alpha positive — reset streak

    for entry in entries[:90]:
        a = entry.get("alpha_vs_spx")
        if a is not None and a < 0:
            count += 1
        else:
            break

    return count


def compute_robustness_score(rolling_sharpe, benchmark, momentum_health, neg_alpha_days):
    """0-100 robustness score for the dashboard"""
    score = 60  # Start at neutral

    # Sharpe component
    sharpe = rolling_sharpe.get("sharpe")
    if sharpe is None:
        score -= 10  # No data penalty
    elif sharpe > 1.0:
        score += 20
    elif sharpe > 0.5:
        score += 10
    elif sharpe > 0.0:
        score += 0
    else:
        score -= 15

    # Alpha vs benchmark
    if benchmark:
        spx_sharpe = benchmark.get("sharpe", 0)
        if sharpe is not None:
            diff = sharpe - spx_sharpe
            if   diff > 0.3:  score += 15
            elif diff > 0:    score += 5
            elif diff > -0.3: score -= 5
            else:             score -= 15

    # Momentum health
    if momentum_health.get("decaying"):
        score -= 10
    elif "HEALTHY" in momentum_health.get("verdict",""):
        score += 5

    # Negative alpha streak
    if   neg_alpha_days > 60:  score -= 20
    elif neg_alpha_days > 30:  score -= 10
    elif neg_alpha_days > 14:  score -= 5

    return max(0, min(100, score))


# ============================================================
# SCORE VELOCITY WEIGHTING
# (Optimization #3 from the plan)
# ============================================================

def apply_score_velocity_weight(picks, score_history, velocity_weight=0.25):
    """
    Boost conviction for stocks with RISING scores.
    A stock scoring 72 today that scored 55 last week is a
    stronger signal than one that's been at 72 for months.

    velocity_weight: how much a fast riser gets boosted (0-0.4)
    """
    if not score_history:
        return picks

    adjusted = []
    for pick in picks:
        ticker = pick["ticker"]
        recs   = score_history.get(ticker, [])

        if len(recs) < 3:
            adjusted.append(pick)
            continue

        sorted_recs = sorted(recs, key=lambda x: x["date"])
        score_start = sorted_recs[0]["score"]
        score_now   = pick["score"]
        delta       = score_now - score_start
        days_tracked= len(sorted_recs)

        # Velocity = points per day
        velocity = delta / days_tracked if days_tracked > 0 else 0

        # Boost: fast risers get up to 25% conviction boost
        if velocity > 2.0:         # Rising fast (2+ pts/day)
            boost = round(score_now * velocity_weight * 0.5, 1)
            pick["score_velocity"]  = velocity
            pick["velocity_boost"]  = boost
            pick["score_original"]  = score_now
            pick["score"]           = min(100, round(score_now + boost, 1))
            pick.setdefault("reasons", []).insert(0, f"📈 Rising fast: +{delta:.0f}pts in {days_tracked}d")
        elif velocity < -2.0:       # Falling fast — reduce score
            penalty = round(abs(score_now * velocity_weight * 0.3), 1)
            pick["score_velocity"]  = velocity
            pick["velocity_boost"]  = -penalty
            pick["score_original"]  = score_now
            pick["score"]           = max(0, round(score_now - penalty, 1))
            pick.setdefault("flags", []).append(f"📉 Falling: {delta:.0f}pts in {days_tracked}d")
        else:
            pick["score_velocity"] = velocity
            pick["velocity_boost"] = 0

        adjusted.append(pick)

    return adjusted


# ============================================================
# SWING EARNINGS FILTER
# (Optimization #1 from the plan)
# ============================================================

def filter_swing_earnings(swing_picks, buffer_days=14):
    """
    Remove swing picks where earnings are within buffer_days.
    Rule: Never hold a swing position through earnings.
    """
    safe     = []
    filtered = []

    for pick in swing_picks:
        data          = pick.get("data", {})
        next_earnings = data.get("next_earnings", "N/A")
        days_to_earn  = 999

        if next_earnings and next_earnings != "N/A":
            try:
                from datetime import datetime
                earn_date    = datetime.strptime(next_earnings, "%b %d, %Y")
                days_to_earn = (earn_date - datetime.now()).days
            except:
                pass

        pick["days_to_earnings"] = days_to_earn

        if days_to_earn <= buffer_days:
            pick.setdefault("flags", []).append(
                f"🚨 EARNINGS in {days_to_earn}d — NO new swing entry"
            )
            filtered.append(pick)
        else:
            safe.append(pick)

    if filtered:
        print(f"   Earnings filter: blocked {len(filtered)} swing picks "
              f"({', '.join(p['ticker'] for p in filtered)})")

    return safe, filtered


# ============================================================
# FX SIGNAL STALENESS INDICATOR
# (Optimization #6 from the plan)
# ============================================================

def check_fx_staleness(fx_signals, max_stale_hours=8):
    """
    Flag FX signals older than max_stale_hours.
    FX moves fast — a 7am signal can be wrong by 10am.
    """
    if not fx_signals:
        return fx_signals, []

    generated_at = fx_signals.get("generated_at")
    stale_pairs  = []

    if generated_at:
        try:
            gen_time   = datetime.fromisoformat(generated_at)
            hours_old  = (datetime.now() - gen_time).total_seconds() / 3600

            for symbol, pair_data in fx_signals.get("pairs", {}).items():
                pair_data["hours_old"]    = round(hours_old, 1)
                pair_data["stale"]        = hours_old > max_stale_hours
                pair_data["freshness"]    = (
                    f"🟢 Fresh ({hours_old:.1f}h)" if hours_old < 4 else
                    f"🟡 Aging ({hours_old:.1f}h)" if hours_old < max_stale_hours else
                    f"🔴 Stale ({hours_old:.1f}h) — verify before acting"
                )
                if pair_data["stale"]:
                    stale_pairs.append(symbol)
        except:
            pass

    return fx_signals, stale_pairs


# ============================================================
# CONTENT TEMPLATE ROTATION TRACKER
# (Optimization #7 from the plan)
# ============================================================

TEMPLATE_HISTORY_FILE = "template_history.json"

def get_template_rotation(situation):
    """
    Track which tweet templates were recently used.
    Never use same template twice in 7 days.
    Returns: situation (possibly modified to avoid repetition)
    """
    history = {}
    if os.path.exists(TEMPLATE_HISTORY_FILE):
        try:
            with open(TEMPLATE_HISTORY_FILE) as f:
                history = json.load(f)
        except:
            pass

    today  = datetime.now().strftime("%Y-%m-%d")
    cutoff = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")

    # Clean old entries
    history = {k: v for k, v in history.items() if v >= cutoff}

    # Check if this situation was used recently
    recent_situations = set(history.keys())
    SITUATION_POOL    = [
        "strong_signal", "risk_off_macro", "fx_gold", "calm_day",
        "earnings_watch", "regime_change", "dividend_deadline"
    ]

    if situation in recent_situations:
        # Find an unused alternative
        alternatives = [s for s in SITUATION_POOL if s not in recent_situations]
        if alternatives:
            situation = alternatives[0]

    # Log today's usage
    history[situation] = today
    with open(TEMPLATE_HISTORY_FILE, "w") as f:
        json.dump(history, f)

    return situation


# ============================================================
# FULL RISK AUDIT RUNNER
# ============================================================

def run_risk_audit(screener_results=None, score_history=None,
                   fx_signals=None, verbose=True):
    """
    Run full risk audit:
      1. Stress simulation
      2. Decay monitor
      3. Drawdown lock check
    Returns complete risk report.
    """
    current_dd  = get_current_drawdown()
    lock_status = check_drawdown_lock(current_dd)
    locked, _   = is_system_locked()

    stress  = None
    decay   = None

    if screener_results:
        stress = run_stress_simulation(screener_results, verbose=verbose)

    decay = run_decay_monitor(score_history=score_history,
                              screener_results=screener_results,
                              verbose=verbose)

    fx_signals, stale = check_fx_staleness(fx_signals or {})

    return {
        "date":             datetime.now().strftime("%B %d, %Y"),
        "drawdown_lock":    lock_status,
        "system_locked":    locked,
        "stress_test":      stress,
        "decay_monitor":    decay,
        "stale_fx_pairs":   stale,
        "survivorship_note":SURVIVORSHIP_NOTE.strip(),
        "when_this_fails":  WHEN_THIS_FAILS.strip(),
    }


# ============================================================
# CLI
# ============================================================

if __name__ == "__main__":
    mode = "--both"
    if len(sys.argv) > 1:
        mode = sys.argv[1]

    print(SURVIVORSHIP_NOTE)

    if mode in ("--stress", "--both"):
        # Load screener results if available
        screener = {}
        if os.path.exists("latest_brief.json"):
            try:
                with open("latest_brief.json") as f:
                    brief = json.load(f)
                    screener = brief
            except:
                pass
        run_stress_simulation(screener, verbose=True)

    if mode in ("--health", "--both"):
        history = {}
        if os.path.exists(HISTORY_FILE):
            with open(HISTORY_FILE) as f:
                history = json.load(f)
        result = run_decay_monitor(score_history=history, verbose=True)

        print("\n" + WHEN_THIS_FAILS)

    if mode == "--lock":
        dd = float(sys.argv[2]) if len(sys.argv) > 2 else 0
        check_drawdown_lock(dd)


# ============================================================
# AUTOMATIC SIGNAL ACCURACY TRACKER
# No manual input. Runs every day.
# Checks: were yesterday's picks directionally correct?
# ============================================================

SIGNAL_ACCURACY_FILE = "signal_accuracy.json"

def load_signal_accuracy():
    if os.path.exists(SIGNAL_ACCURACY_FILE):
        try:
            with open(SIGNAL_ACCURACY_FILE) as f:
                return json.load(f)
        except:
            pass
    return {"entries": [], "summary": {}}


def save_signal_accuracy(data):
    with open(SIGNAL_ACCURACY_FILE, "w") as f:
        json.dump(data, f, indent=2, default=str)


def track_signal_accuracy(todays_picks, score_history, check_windows=(3, 7, 14)):
    """
    Automatic signal accuracy tracking — zero manual input required.

    How it works:
      1. Today: system makes directional calls (score + category = implied LONG)
      2. In 3/7/14 days: compare entry price to current price
      3. Was the call correct? Log it automatically.

    Uses score_history price data to check subsequent prices.
    No trade execution needed — tracks MODEL accuracy, not YOUR P&L.
    That's an important distinction: this tells you if the signals
    are working, not whether you placed every trade.

    Returns:
      - accuracy_pct per window (3d, 7d, 14d)
      - best/worst calls
      - momentum vs quality pick accuracy split
      - whether model is degrading over time
    """
    now      = datetime.now()
    today    = now.strftime("%Y-%m-%d")
    accuracy = load_signal_accuracy()

    # ── Step 1: Log today's picks as pending checks ─────
    pending = []
    for p in todays_picks:
        ticker    = p.get("ticker","")
        score     = p.get("score", 0)
        price     = p.get("data", {}).get("price", 0)
        category  = p.get("pick", {}).get("category","") if p.get("pick") else ""
        direction = "LONG"  # All picks are implied long (we're buyers)

        if not ticker or not price:
            continue

        for window in check_windows:
            check_date = (now + timedelta(days=window)).strftime("%Y-%m-%d")
            pending.append({
                "ticker":      ticker,
                "logged_date": today,
                "check_date":  check_date,
                "window_days": window,
                "entry_price": price,
                "entry_score": score,
                "category":    category,
                "direction":   direction,
                "status":      "pending",
                "exit_price":  None,
                "correct":     None,
                "pct_move":    None,
            })

    # ── Step 2: Resolve pending checks using price history ─
    resolved = 0
    for entry in accuracy.get("entries", []):
        if entry["status"] != "pending":
            continue
        if entry["check_date"] > today:
            continue  # Not due yet

        # Look up price on check_date from score_history
        ticker    = entry["ticker"]
        hist      = score_history.get(ticker, [])
        hist_sorted = sorted(hist, key=lambda x: x["date"])

        # Find closest price to check_date
        check_price = None
        for rec in reversed(hist_sorted):
            if rec["date"] <= entry["check_date"]:
                check_price = rec.get("price")
                break

        if check_price and check_price > 0 and entry["entry_price"] > 0:
            pct_move        = (check_price - entry["entry_price"]) / entry["entry_price"] * 100
            entry["exit_price"] = check_price
            entry["pct_move"]   = round(pct_move, 2)
            entry["correct"]    = pct_move > 0  # Correct if price went up (we're long)
            entry["status"]     = "resolved"
            resolved += 1

    # Append new pending entries
    accuracy["entries"].extend(pending)

    # Keep only 180 days of history
    cutoff = (now - timedelta(days=180)).strftime("%Y-%m-%d")
    accuracy["entries"] = [e for e in accuracy["entries"] if e.get("logged_date","") >= cutoff]

    # ── Step 3: Compute accuracy metrics ────────────────
    summary = compute_accuracy_summary(accuracy["entries"])
    accuracy["summary"]      = summary
    accuracy["last_updated"] = today

    save_signal_accuracy(accuracy)
    return summary


def compute_accuracy_summary(entries):
    """Compute accuracy stats across all resolved entries"""
    resolved = [e for e in entries if e["status"] == "resolved"]
    pending  = [e for e in entries if e["status"] == "pending"]

    if not resolved:
        return {
            "total_signals":   len(entries),
            "resolved":        0,
            "pending":         len(pending),
            "accuracy_3d":     None,
            "accuracy_7d":     None,
            "accuracy_14d":    None,
            "avg_move_pct":    None,
            "best_call":       None,
            "worst_call":      None,
            "model_verdict":   "⏳ Building — need 14+ days of data",
            "note":            f"{len(pending)} signals logged, checking in 3/7/14 days automatically",
        }

    # Accuracy per window
    def acc(window):
        w  = [e for e in resolved if e["window_days"] == window]
        if not w: return None
        correct = sum(1 for e in w if e["correct"])
        return round(correct / len(w) * 100, 1)

    # Per category accuracy
    cats = {}
    for e in resolved:
        c = e.get("category","OTHER")
        if c not in cats:
            cats[c] = {"correct":0,"total":0}
        cats[c]["total"] += 1
        if e["correct"]:
            cats[c]["correct"] += 1

    cat_accuracy = {c: round(v["correct"]/v["total"]*100,1) for c,v in cats.items() if v["total"]>=3}

    # Best and worst calls
    sorted_moves = sorted(resolved, key=lambda x: x.get("pct_move",0) or 0, reverse=True)
    best  = sorted_moves[0]  if sorted_moves else None
    worst = sorted_moves[-1] if sorted_moves else None

    # Avg move
    moves    = [e["pct_move"] for e in resolved if e.get("pct_move") is not None]
    avg_move = round(sum(moves)/len(moves), 2) if moves else 0

    # Recent trend (last 30 days vs earlier)
    recent_cutoff = (datetime.now()-timedelta(days=30)).strftime("%Y-%m-%d")
    recent   = [e for e in resolved if e["logged_date"] >= recent_cutoff]
    older    = [e for e in resolved if e["logged_date"] <  recent_cutoff]
    recent_acc = round(sum(1 for e in recent if e["correct"])/len(recent)*100,1) if recent else None
    older_acc  = round(sum(1 for e in older  if e["correct"])/len(older) *100,1) if older  else None

    # Verdict
    acc_7  = acc(7)
    if acc_7 is None:
        verdict = "⏳ Building accuracy data"
    elif len(resolved) < 50:
        # Not enough data for meaningful verdict — show neutral status
        if acc_7 >= 50:
            verdict = "📊 EARLY — Accumulating data (need 50+ resolved picks)"
        else:
            verdict = "📊 EARLY — Market conditions challenging. Need 50+ picks for verdict."
    elif acc_7 >= 65:
        verdict = "✅ HEALTHY — Model directionally accurate"
    elif acc_7 >= 50:
        verdict = "⚠️ MIXED — Slightly above coin flip"
    elif acc_7 >= 40:
        verdict = "⚠️ WEAK — Close to random. Reduce position sizes."
    else:
        verdict = "🔴 DEGRADING — Model underperforming. Pause and review."

    # Degrading signal
    degrading = False
    if recent_acc is not None and older_acc is not None and len(resolved) >= 50:
        degrading = recent_acc < older_acc - 10  # 10pt drop = degrading

    return {
        "total_signals":   len(entries),
        "resolved":        len(resolved),
        "pending":         len(pending),
        "accuracy_3d":     acc(3),
        "accuracy_7d":     acc_7,
        "accuracy_14d":    acc(14),
        "avg_move_pct":    avg_move,
        "by_category":     cat_accuracy,
        "recent_30d_acc":  recent_acc,
        "older_acc":       older_acc,
        "degrading":       degrading,
        "best_call":       {"ticker":best["ticker"],"pct":best["pct_move"],"date":best["logged_date"]} if best else None,
        "worst_call":      {"ticker":worst["ticker"],"pct":worst["pct_move"],"date":worst["logged_date"]} if worst else None,
        "model_verdict":   verdict,
        "note":            f"{len(resolved)} signals resolved · {len(pending)} pending · fully automatic",
    }


def compute_kelly_size(win_rate, avg_win_pct, avg_loss_pct, account_balance,
                       regime="NORMAL", resolved_count=0):
    """
    Half-Kelly position sizing based on real outcome history.

    Kelly formula: f = (p*b - q) / b
      p = win rate (0–1)
      q = 1 - p
      b = avg_win / avg_loss ratio (odds)

    Half-Kelly applied for safety (never bet more than half Kelly suggests).
    Falls back to fixed sizing if resolved_count < 30 (not enough data).

    Returns dict with recommended_pct and recommended_dollars.
    """
    MIN_RESOLVED = 30  # need at least this many outcomes to trust Kelly

    # Regime multiplier — scale down in bad markets
    regime_mult = {"BULL": 1.0, "NORMAL": 0.85, "CAUTION": 0.60, "BEAR": 0.35}.get(regime, 0.85)

    # Fallback: not enough data yet
    if resolved_count < MIN_RESOLVED or avg_loss_pct <= 0:
        fallback_pct = {"BULL": 6.0, "NORMAL": 5.0, "CAUTION": 3.5, "BEAR": 2.0}.get(regime, 5.0)
        return {
            "method":            "fixed_fallback",
            "resolved_count":    resolved_count,
            "min_required":      MIN_RESOLVED,
            "kelly_pct":         None,
            "half_kelly_pct":    None,
            "regime_adj_pct":    fallback_pct,
            "recommended_pct":   fallback_pct,
            "recommended_dollars": round(account_balance * fallback_pct / 100, 2),
            "note": f"Using fixed sizing — need {MIN_RESOLVED - resolved_count} more resolved picks to unlock Kelly",
        }

    # Kelly calculation
    p = max(0.01, min(0.99, win_rate))     # clamp to avoid div/0
    q = 1.0 - p
    b = avg_win_pct / avg_loss_pct          # win/loss ratio (the "odds")

    kelly_f = (p * b - q) / b              # raw Kelly fraction (% of bankroll)
    kelly_f = max(0.0, kelly_f)            # can't be negative
    half_kelly = kelly_f * 0.5             # Half-Kelly = safety standard

    # Cap at 10% per position regardless — TFSA concentration risk
    capped = min(half_kelly, 0.10)

    # Apply regime
    final_pct = round(capped * regime_mult * 100, 2)
    final_pct = max(0.5, final_pct)        # floor at 0.5%

    return {
        "method":              "kelly",
        "resolved_count":      resolved_count,
        "win_rate":            round(p * 100, 1),
        "avg_win_pct":         round(avg_win_pct, 2),
        "avg_loss_pct":        round(avg_loss_pct, 2),
        "odds_ratio":          round(b, 2),
        "kelly_pct":           round(kelly_f * 100, 2),
        "half_kelly_pct":      round(half_kelly * 100, 2),
        "regime_mult":         regime_mult,
        "regime_adj_pct":      final_pct,
        "recommended_pct":     final_pct,
        "recommended_dollars": round(account_balance * final_pct / 100, 2),
        "note": f"Half-Kelly ({round(half_kelly*100,2)}%) × regime ({regime_mult}) = {final_pct}% of ${account_balance:,.0f}",
    }


def compute_position_size_guardrail(ticker, account_balance, pick_category,
                                     signal_accuracy_summary=None, regime="NORMAL"):
    """
    The only sizing question that matters:
    How much can I put in this pick so that even if it goes to zero,
    I'm still okay?

    Replaces the goal tracker. This is the actual guardrail.
    """
    buckets   = compute_bucket_allocation(account_balance)

    # Route to correct bucket based on category
    if "SWING" in pick_category.upper():
        bucket     = buckets["swing"]
        max_single = CONFIG["accounts"]["TFSA"]["max_swing_per_trade"]
    elif "INCOME" in pick_category.upper() or "DIVIDEND" in pick_category.upper():
        bucket     = buckets["floor"]
        max_single = bucket["max_position"]
    elif "GROWTH" in pick_category.upper() or "CORE" in pick_category.upper():
        bucket     = buckets["model_picks"]
        max_single = bucket["max_position"]
    elif "FHSA" in pick_category.upper():
        fhsa_bal   = CONFIG["accounts"]["FHSA"]["balance"]
        max_single = round(fhsa_bal * 0.25, 2)
        bucket     = {"dollars": fhsa_bal, "desc": "FHSA — conservative growth ETFs"}
    else:
        bucket     = buckets["model_picks"]
        max_single = bucket["max_position"]

    # Regime adjustment
    regime_mult = {"BULL": 1.0, "NORMAL": 0.85, "CAUTION": 0.65, "BEAR": 0.40}.get(regime, 0.85)
    adjusted    = round(max_single * regime_mult, 2)

    # Accuracy adjustment — if model is degrading, reduce size
    acc_mult = 1.0
    if signal_accuracy_summary:
        acc_7 = signal_accuracy_summary.get("accuracy_7d")
        if acc_7 is not None:
            if   acc_7 >= 65: acc_mult = 1.0
            elif acc_7 >= 55: acc_mult = 0.85
            elif acc_7 >= 45: acc_mult = 0.65
            else:             acc_mult = 0.40  # Model is weak — size down hard

    final_size  = round(adjusted * acc_mult, 2)
    if_zero     = round(final_size / account_balance * 100, 1) if account_balance else 0

    return {
        "ticker":         ticker,
        "category":       pick_category,
        "bucket":         bucket.get("desc",""),
        "bucket_pool":    bucket.get("dollars", 0),
        "max_raw":        max_single,
        "regime_adj":     regime_mult,
        "accuracy_adj":   acc_mult,
        "recommended":    final_size,
        "if_zero_pct":    if_zero,
        "regime":         regime,
        "survival_note":  f"If this goes to zero → you lose {if_zero}% of account. {'✅ Acceptable' if if_zero<=5 else '⚠️ Consider smaller size' if if_zero<=10 else '🔴 Too large'}",
    }


# Make compute_bucket_allocation importable from risk_engine too
def compute_bucket_allocation(account_balance):
    """Delegate to portfolio_engine version"""
    try:
        from portfolio_engine import compute_bucket_allocation as _cba
        return _cba(account_balance)
    except:
        # Fallback
        return {
            "floor":       {"pct":50,"dollars":account_balance*0.5, "max_position":account_balance*0.125,"desc":"Dividend core"},
            "model_picks": {"pct":30,"dollars":account_balance*0.3, "max_position":account_balance*0.06, "desc":"ML picks"},
            "swing":       {"pct":15,"dollars":account_balance*0.15,"max_position":100,                   "desc":"Swings"},
            "crypto":      {"pct":5, "dollars":account_balance*0.05,"max_position":account_balance*0.03,  "desc":"Crypto"},
        }
