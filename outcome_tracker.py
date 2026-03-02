"""
InvestOS — Outcome Tracker
==========================
Logs every pick with entry price at signal time.
Next run checks what happened — win/loss/magnitude.
Feeds back into ML model over time.
This is what gets you to 89%.
"""

import json
import os
from datetime import datetime, timedelta

OUTCOMES_FILE = "outcomes_log.json"
WIN_RATE_FILE = "win_rate.json"


def load_outcomes():
    if os.path.exists(OUTCOMES_FILE):
        try:
            with open(OUTCOMES_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return []


def save_outcomes(outcomes):
    with open(OUTCOMES_FILE, "w") as f:
        json.dump(outcomes, f, indent=2, default=str)


def log_picks(picks, run_time=None):
    """
    Log today's picks with entry price.
    Called at end of each run — before market opens next day.
    """
    if not picks:
        return

    outcomes = load_outcomes()
    now = run_time or datetime.now().isoformat()
    date_str = datetime.now().strftime("%Y-%m-%d")

    # Don't double-log same picks on same date
    logged_today = {o["ticker"] for o in outcomes
                    if o.get("signal_date") == date_str and o.get("resolved") is False}

    new_logged = 0
    for pick in picks:
        ticker = pick.get("ticker")
        if not ticker or ticker in logged_today:
            continue

        entry = {
            "ticker":        ticker,
            "signal_date":   date_str,
            "signal_time":   now,
            "entry_price":   pick.get("data", {}).get("price", 0),
            "score":         pick.get("score", 0),
            "ml_prob":       pick.get("ml_prob", 0.5),
            "category":      pick.get("pick", {}).get("category", ""),
            "exp_low":       pick.get("pick", {}).get("exp_low", 0),
            "exp_high":      pick.get("pick", {}).get("exp_high", 0),
            "resolved":      False,
            "exit_price":    None,
            "actual_return": None,
            "outcome":       None,   # "WIN" | "LOSS" | "FLAT"
            "resolved_date": None,
        }
        outcomes.append(entry)
        new_logged += 1

    save_outcomes(outcomes)
    print(f"   📝 Outcome tracker: logged {new_logged} new picks ({len(outcomes)} total)")
    return new_logged


def resolve_outcomes(current_prices):
    """
    Check unresolved picks. If 1+ trading days have passed,
    mark as WIN/LOSS based on next-day close.
    current_prices: dict of {ticker: price}
    """
    if not current_prices:
        return

    outcomes  = load_outcomes()
    today     = datetime.now().date()
    resolved  = 0

    for o in outcomes:
        if o.get("resolved"):
            continue

        signal_date = datetime.strptime(o["signal_date"], "%Y-%m-%d").date()
        days_passed = (today - signal_date).days

        # Resolve after 1 trading day (next morning run = check yesterday's picks)
        if days_passed >= 1:
            ticker      = o["ticker"]
            entry_price = o.get("entry_price", 0)
            exit_price  = current_prices.get(ticker)

            if exit_price and entry_price and entry_price > 0:
                ret = (exit_price - entry_price) / entry_price * 100
                o["exit_price"]    = round(exit_price, 2)
                o["actual_return"] = round(ret, 2)
                o["resolved"]      = True
                o["resolved_date"] = today.isoformat()

                # WIN = any positive return, LOSS = negative, FLAT = within 0.3%
                if ret > 0.3:
                    o["outcome"] = "WIN"
                elif ret < -0.3:
                    o["outcome"] = "LOSS"
                else:
                    o["outcome"] = "FLAT"

                resolved += 1

    save_outcomes(outcomes)
    if resolved:
        print(f"   ✅ Resolved {resolved} outcomes")
    return resolved


def compute_win_rate():
    """
    Compute overall win rate + by category/score tier.
    Returns dict that gets baked into dashboard.
    """
    outcomes = load_outcomes()
    resolved = [o for o in outcomes if o.get("resolved") and o.get("outcome")]

    if len(resolved) < 3:
        return {
            "total_resolved": len(resolved),
            "win_rate": None,
            "avg_return": None,
            "message": f"Building... ({len(resolved)} outcomes tracked so far)",
            "by_score_tier": {},
            "by_category": {},
            "recent_10": [],
            "streak": 0,
            "streak_type": None,
        }

    wins  = [o for o in resolved if o["outcome"] == "WIN"]
    losses = [o for o in resolved if o["outcome"] == "LOSS"]
    flats  = [o for o in resolved if o["outcome"] == "FLAT"]

    win_rate   = len(wins) / len(resolved) * 100
    avg_return = sum(o["actual_return"] for o in resolved) / len(resolved)

    # By score tier
    by_score = {}
    for tier, min_s, max_s in [("90-100", 90, 100), ("75-89", 75, 89),
                                ("60-74", 60, 74), ("below-60", 0, 59)]:
        tier_picks = [o for o in resolved if min_s <= o.get("score", 0) <= max_s]
        if tier_picks:
            tier_wins = len([o for o in tier_picks if o["outcome"] == "WIN"])
            by_score[tier] = {
                "win_rate": round(tier_wins / len(tier_picks) * 100, 1),
                "count":    len(tier_picks),
                "avg_ret":  round(sum(o["actual_return"] for o in tier_picks) / len(tier_picks), 2)
            }

    # By category
    by_cat = {}
    cats = set(o.get("category", "OTHER") for o in resolved)
    for cat in cats:
        cat_picks = [o for o in resolved if o.get("category") == cat]
        if cat_picks:
            cat_wins = len([o for o in cat_picks if o["outcome"] == "WIN"])
            by_cat[cat] = {
                "win_rate": round(cat_wins / len(cat_picks) * 100, 1),
                "count":    len(cat_picks),
            }

    # Recent 10
    recent = sorted(resolved, key=lambda x: x.get("resolved_date", ""), reverse=True)[:10]
    recent_10 = [{
        "ticker":  o["ticker"],
        "date":    o["signal_date"],
        "ret":     o["actual_return"],
        "outcome": o["outcome"],
        "score":   o.get("score", 0),
    } for o in recent]

    # Current streak
    streak = 0
    streak_type = None
    for o in sorted(resolved, key=lambda x: x.get("resolved_date", ""), reverse=True):
        if streak == 0:
            streak_type = o["outcome"]
            streak = 1
        elif o["outcome"] == streak_type:
            streak += 1
        else:
            break

    result = {
        "total_resolved":  len(resolved),
        "wins":            len(wins),
        "losses":          len(losses),
        "flats":           len(flats),
        "win_rate":        round(win_rate, 1),
        "avg_return":      round(avg_return, 2),
        "best_return":     round(max(o["actual_return"] for o in resolved), 2),
        "worst_return":    round(min(o["actual_return"] for o in resolved), 2),
        "by_score_tier":   by_score,
        "by_category":     by_cat,
        "recent_10":       recent_10,
        "streak":          streak,
        "streak_type":     streak_type,
        "message":         f"{win_rate:.0f}% win rate on {len(resolved)} picks",
    }

    # Save for dashboard
    with open(WIN_RATE_FILE, "w") as f:
        json.dump(result, f, indent=2)

    return result


def print_win_rate_report(wr):
    print("\n" + "="*55)
    print("  OUTCOME TRACKER — WIN RATE REPORT")
    print("="*55)
    if wr.get("win_rate") is None:
        print(f"  {wr['message']}")
        return

    print(f"  Total resolved:  {wr['total_resolved']} picks")
    print(f"  Win rate:        {wr['win_rate']}%  (target: 89%)")
    print(f"  Avg return/pick: {wr['avg_return']:+.2f}%")
    print(f"  Best:  {wr['best_return']:+.2f}%   Worst: {wr['worst_return']:+.2f}%")

    if wr.get("streak"):
        icon = "🔥" if wr["streak_type"] == "WIN" else "❄️"
        print(f"  {icon} Current streak: {wr['streak']} {wr['streak_type']}S")

    if wr.get("by_score_tier"):
        print(f"\n  WIN RATE BY SCORE TIER:")
        for tier, data in sorted(wr["by_score_tier"].items(), reverse=True):
            bar = "█" * int(data["win_rate"] / 5)
            print(f"  Score {tier:<10} {bar} {data['win_rate']}%  ({data['count']} picks, avg {data['avg_ret']:+.1f}%)")

    if wr.get("recent_10"):
        print(f"\n  LAST {len(wr['recent_10'])} PICKS:")
        for r in wr["recent_10"]:
            icon = "✅" if r["outcome"] == "WIN" else ("❌" if r["outcome"] == "LOSS" else "➖")
            print(f"  {icon} {r['ticker']:<10} {r['ret']:+.1f}%  (score {r['score']})  {r['date']}")
