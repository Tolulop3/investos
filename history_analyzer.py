"""
history_analyzer.py
-------------------
Weekly analysis of InvestOS history/ snapshots.
Produces history_analysis.json — structured, repurposable output.

Run automatically via GitHub Actions (Sunday, after scout_agent).
Run manually: python history_analyzer.py

Outputs:
  history_analysis.json   — machine-readable summary (product-ready)
  history/obsidian/weekly_YYYY-WNN.md  — Obsidian weekly note

What it answers:
  1. Regime timeline       — when did we switch, how long did each last
  2. Pick quality by macro — does RISK_OFF actually hurt our picks?
  3. Sector rotation       — which sectors dominate each regime
  4. Sharpe trajectory     — are we recovering or deteriorating?
  5. Signal hit rate       — which macro signals correlate with good picks
  6. OOS performance       — picks since Jun 26 2026 (v4.1 freeze date)
"""

import json
import os
import pathlib
from datetime import datetime, timezone
from collections import defaultdict


OOS_START = "2026-06-26"   # v4.1 freeze date — out-of-sample Day 0


# ─────────────────────────────────────────────────────────────────────────────
# LOAD
# ─────────────────────────────────────────────────────────────────────────────

def load_history(history_dir="history"):
    """Load all YYYY-MM-DD.json snapshots, sorted by date."""
    snaps = []
    p = pathlib.Path(history_dir)
    if not p.exists():
        return snaps
    for f in sorted(p.glob("????-??-??.json")):
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
            if data.get("date"):
                snaps.append(data)
        except Exception:
            pass
    return snaps


def load_outcomes(path="outcomes_log.json"):
    """Load resolved picks from outcomes_log.json."""
    try:
        with open(path, encoding="utf-8") as f:
            picks = json.load(f)
        return [p for p in picks if p.get("outcome") is not None]
    except Exception:
        return []


# ─────────────────────────────────────────────────────────────────────────────
# ANALYSIS MODULES
# ─────────────────────────────────────────────────────────────────────────────

def regime_timeline(snaps):
    """
    Build a day-by-day regime timeline.
    Returns list of regime periods with start, end, duration, avg_sharpe.
    """
    if not snaps:
        return []

    periods = []
    current = None

    for s in snaps:
        date = s.get("date")
        regime_data = s.get("regime", {}) or {}
        # regime can be dict with "signal" key or string
        if isinstance(regime_data, dict):
            regime = regime_data.get("signal", "UNKNOWN")
        else:
            regime = str(regime_data)

        sharpe = s.get("sharpe") or 0

        if current is None or current["regime"] != regime:
            if current:
                periods.append(current)
            current = {
                "regime":      regime,
                "start":       date,
                "end":         date,
                "days":        1,
                "sharpes":     [sharpe],
            }
        else:
            current["end"]    = date
            current["days"]  += 1
            current["sharpes"].append(sharpe)

    if current:
        periods.append(current)

    # Compute avg sharpe per period
    for p in periods:
        s_vals = [x for x in p["sharpes"] if x is not None]
        p["avg_sharpe"] = round(sum(s_vals) / len(s_vals), 3) if s_vals else None
        del p["sharpes"]

    return periods


def sharpe_trajectory(snaps, window=7):
    """
    Rolling window of Sharpe — are we recovering or deteriorating?
    Returns list of {date, sharpe, rolling_avg, direction}.
    """
    trajectory = []
    buffer = []

    for s in snaps:
        sharpe = s.get("sharpe")
        if sharpe is None:
            continue
        buffer.append(sharpe)
        if len(buffer) > window:
            buffer.pop(0)

        roll_avg = round(sum(buffer) / len(buffer), 3)
        direction = "improving" if len(buffer) >= 3 and buffer[-1] > buffer[-3] else \
                    "declining"  if len(buffer) >= 3 and buffer[-1] < buffer[-3] else \
                    "stable"

        trajectory.append({
            "date":        s["date"],
            "sharpe":      round(sharpe, 3),
            "rolling_avg": roll_avg,
            "direction":   direction,
        })

    return trajectory


def pick_quality_by_macro(resolved_picks, snaps):
    """
    Join resolved picks to the daily snapshot on signal_date.
    Group by macro_signals active that day.
    Returns PF and WR per macro condition.
    """
    # Build date → macro_signals lookup from history
    date_to_signals = {}
    for s in snaps:
        date_to_signals[s["date"]] = s.get("macro_signals", []) or []

    # Group picks by macro signal presence
    signal_groups = defaultdict(lambda: {"wins": 0, "losses": 0,
                                          "gross_win": 0.0, "gross_loss": 0.0})

    for pick in resolved_picks:
        date = (pick.get("signal_date") or pick.get("date") or "")[:10]
        ret  = pick.get("return_pct") or 0
        win  = pick.get("outcome") == "WIN"
        loss = pick.get("outcome") == "LOSS"

        signals = date_to_signals.get(date, ["NO_DATA"])
        if not signals:
            signals = ["NO_SIGNALS"]

        for sig in signals:
            grp = signal_groups[sig]
            if win:
                grp["wins"]      += 1
                grp["gross_win"] += abs(ret)
            elif loss:
                grp["losses"]     += 1
                grp["gross_loss"] += abs(ret)

    results = {}
    for sig, grp in signal_groups.items():
        total = grp["wins"] + grp["losses"]
        if total < 5:    # skip low-sample signals
            continue
        pf = round(grp["gross_win"] / grp["gross_loss"], 2) \
             if grp["gross_loss"] > 0 else None
        wr = round(grp["wins"] / total * 100, 1)
        results[sig] = {
            "n":      total,
            "wr":     wr,
            "pf":     pf,
            "status": "✅" if (pf or 0) >= 1.5 else
                      "⚠️" if (pf or 0) >= 1.0 else "🔴",
        }

    # Sort by PF descending
    return dict(sorted(results.items(),
                        key=lambda x: x[1]["pf"] or 0, reverse=True))


def sector_rotation(snaps):
    """
    Track which sectors are bullish/bearish over time.
    Returns weekly aggregated sentiment per sector.
    """
    weekly = defaultdict(lambda: defaultdict(list))

    for s in snaps:
        date = s.get("date", "")
        if not date:
            continue
        # ISO week
        try:
            dt   = datetime.fromisoformat(date)
            week = f"{dt.year}-W{dt.isocalendar()[1]:02d}"
        except Exception:
            continue

        sentiment = s.get("sector_sentiment", {}) or {}
        for sector, data in sentiment.items():
            if isinstance(data, dict):
                net = data.get("net_score", 0) or 0
            else:
                net = 0
            weekly[week][sector].append(net)

    # Aggregate to weekly averages
    result = {}
    for week, sectors in sorted(weekly.items()):
        result[week] = {}
        for sector, nets in sectors.items():
            avg = round(sum(nets) / len(nets), 0)
            result[week][sector] = {
                "avg_net": avg,
                "signal":  "BULL" if avg > 0 else "BEAR" if avg < 0 else "NEUTRAL",
            }

    return result


def oos_performance(resolved_picks):
    """
    Picks resolved since OOS_START (v4.1 freeze date).
    Compares against pre-OOS baseline.
    """
    pre_oos  = [p for p in resolved_picks
                if (p.get("signal_date") or p.get("date") or "") < OOS_START]
    post_oos = [p for p in resolved_picks
                if (p.get("signal_date") or p.get("date") or "") >= OOS_START]

    def metrics(picks):
        if not picks:
            return {"n": 0, "wr": None, "avg_return": None, "pf": None}
        wins   = [p for p in picks if p.get("outcome") == "WIN"]
        losses = [p for p in picks if p.get("outcome") == "LOSS"]
        rets   = [p.get("return_pct") or 0 for p in picks]
        gw = sum(abs(p.get("return_pct") or 0) for p in wins)
        gl = sum(abs(p.get("return_pct") or 0) for p in losses)
        return {
            "n":          len(picks),
            "wr":         round(len(wins) / len(picks) * 100, 1),
            "avg_return": round(sum(rets) / len(rets), 2),
            "pf":         round(gw / gl, 2) if gl > 0 else None,
        }

    return {
        "oos_start":   OOS_START,
        "pre_oos":     metrics(pre_oos),
        "post_oos":    metrics(post_oos),
        "note":        "post_oos picks are the true out-of-sample test of v4.1 rules",
    }


def breadth_trend(snaps, window=5):
    """Track market breadth (% above 200MA) trend."""
    readings = []
    for s in snaps:
        b = s.get("breadth", {}) or {}
        p200 = b.get("pct_above_200") if isinstance(b, dict) else None
        if p200 is not None:
            readings.append({"date": s["date"], "pct_above_200": p200})

    # Add rolling direction
    for i, r in enumerate(readings):
        if i >= window:
            delta = r["pct_above_200"] - readings[i - window]["pct_above_200"]
            r["trend_5d"] = round(delta, 1)
            r["direction"] = "expanding" if delta > 1 else \
                             "contracting" if delta < -1 else "stable"
        else:
            r["trend_5d"]  = None
            r["direction"] = "insufficient_data"

    return readings


# ─────────────────────────────────────────────────────────────────────────────
# OBSIDIAN WEEKLY NOTE
# ─────────────────────────────────────────────────────────────────────────────

def write_obsidian_weekly(analysis, week_label):
    """Write a weekly summary note to history/obsidian/weekly_YYYY-WNN.md"""
    try:
        out_dir = pathlib.Path("history") / "obsidian"
        out_dir.mkdir(parents=True, exist_ok=True)
        note_path = out_dir / f"weekly_{week_label}.md"

        oos  = analysis.get("oos_performance", {})
        post = oos.get("post_oos", {})
        pre  = oos.get("pre_oos", {})

        # Regime periods summary
        periods   = analysis.get("regime_timeline", [])
        last_3    = periods[-3:] if len(periods) >= 3 else periods
        regime_md = "\n".join(
            f"| {p['regime']} | {p['start']} → {p['end']} "
            f"| {p['days']}d | {p.get('avg_sharpe', 'N/A')} |"
            for p in last_3
        )

        # Top macro signals by PF
        sig_quality = analysis.get("pick_quality_by_macro", {})
        top_signals = list(sig_quality.items())[:5]
        sig_md = "\n".join(
            f"| {sig} | {d['n']} | {d['wr']}% | {d['pf']} | {d['status']} |"
            for sig, d in top_signals
        ) or "_Insufficient data_"

        # Sharpe trajectory — last 7 days
        traj    = analysis.get("sharpe_trajectory", [])
        last_7  = traj[-7:] if len(traj) >= 7 else traj
        traj_md = "\n".join(
            f"| {t['date']} | {t['sharpe']} | {t['rolling_avg']} | {t['direction']} |"
            for t in last_7
        ) or "_No data_"

        note = f"""---
week: {week_label}
generated: {datetime.now(timezone.utc).strftime("%Y-%m-%d")}
oos_n: {post.get('n', 0)}
oos_wr: {post.get('wr')}
oos_pf: {post.get('pf')}
pre_oos_pf: {pre.get('pf')}
tags: [investos, weekly, performance]
---

# InvestOS Weekly — {week_label}

## OOS Performance (v4.1, since {OOS_START})
| Period | n | WR | Avg Return | PF |
|--------|---|----|------------|-----|
| Pre-OOS (in-sample) | {pre.get('n', 0)} | {pre.get('wr')}% | {pre.get('avg_return')}% | {pre.get('pf')} |
| **Post-OOS (live test)** | **{post.get('n', 0)}** | **{post.get('wr')}%** | **{post.get('avg_return')}%** | **{post.get('pf')}** |

> OOS picks are made under frozen v4.1 rules — no tuning since {OOS_START}.

## Regime Timeline (recent)
| Regime | Period | Duration | Avg Sharpe |
|--------|--------|----------|------------|
{regime_md}

## Sharpe Trajectory (last 7 days)
| Date | Sharpe | 7d Avg | Direction |
|------|--------|--------|-----------|
{traj_md}

## Pick Quality by Macro Signal (min 5 picks)
| Signal | n | WR | PF | Status |
|--------|---|----|----|--------|
{sig_md}

## Notes
_Add weekly observations here_

---
_Generated by InvestOS history_analyzer.py · NFA · Educational only_
"""
        note_path.write_text(note, encoding="utf-8")
        print(f"  📓 Weekly Obsidian note → {note_path}")

    except Exception as e:
        print(f"  ⚠️  Weekly Obsidian note failed: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def run_history_analysis(
    history_dir   = "history",
    outcomes_path = "outcomes_log.json",
    output_path   = "history_analysis.json",
    verbose       = True,
):
    if verbose:
        print("\n" + "=" * 60)
        print("  HISTORY ANALYZER")
        print("=" * 60)

    snaps    = load_history(history_dir)
    resolved = load_outcomes(outcomes_path)

    if verbose:
        print(f"  Loaded {len(snaps)} history snapshots")
        print(f"  Loaded {len(resolved)} resolved picks")

    if not snaps:
        print("  ⚠️  No history snapshots found — run daily first")
        return {}

    analysis = {
        "generated":           datetime.now(timezone.utc).isoformat(),
        "history_days":        len(snaps),
        "date_range":          {
            "first": snaps[0]["date"],
            "last":  snaps[-1]["date"],
        },
        "oos_start":           OOS_START,

        # Module outputs
        "regime_timeline":     regime_timeline(snaps),
        "sharpe_trajectory":   sharpe_trajectory(snaps),
        "breadth_trend":       breadth_trend(snaps),
        "sector_rotation":     sector_rotation(snaps),
        "pick_quality_by_macro": pick_quality_by_macro(resolved, snaps),
        "oos_performance":     oos_performance(resolved),
    }

    # Quick summary stats
    traj     = analysis["sharpe_trajectory"]
    oos_post = analysis["oos_performance"]["post_oos"]
    analysis["summary"] = {
        "current_sharpe":    traj[-1]["sharpe"]      if traj else None,
        "sharpe_direction":  traj[-1]["direction"]   if traj else None,
        "sharpe_7d_avg":     traj[-1]["rolling_avg"] if traj else None,
        "regime_count":      len(analysis["regime_timeline"]),
        "current_regime":    analysis["regime_timeline"][-1]["regime"]
                             if analysis["regime_timeline"] else None,
        "oos_picks":         oos_post["n"],
        "oos_wr":            oos_post["wr"],
        "oos_pf":            oos_post["pf"],
        "oos_status":        "✅" if (oos_post["pf"] or 0) >= 1.5 else
                             "⚠️" if (oos_post["pf"] or 0) >= 1.0 else
                             "🔴" if oos_post["n"] >= 10 else
                             "⏳ insufficient data",
    }

    # Save JSON
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(analysis, f, indent=2, default=str)

    if verbose:
        print(f"\n  ✅ Analysis saved → {output_path}")
        s = analysis["summary"]
        print(f"\n  SUMMARY:")
        print(f"    Date range:       {analysis['date_range']['first']} → {analysis['date_range']['last']}")
        print(f"    History days:     {analysis['history_days']}")
        print(f"    Current Sharpe:   {s['current_sharpe']} ({s['sharpe_direction']})")
        print(f"    Current regime:   {s['current_regime']}")
        print(f"    OOS picks:        {s['oos_picks']} (target: 60 by Sep 26)")
        print(f"    OOS WR:           {s['oos_wr']}%")
        print(f"    OOS PF:           {s['oos_pf']} {s['oos_status']}")

        # Regime timeline
        print(f"\n  REGIME HISTORY ({len(analysis['regime_timeline'])} periods):")
        for p in analysis["regime_timeline"][-5:]:
            print(f"    {p['regime']:25s} {p['start']} → {p['end']} "
                  f"({p['days']}d, Sharpe {p.get('avg_sharpe', 'N/A')})")

        # Pick quality by macro
        pqm = analysis["pick_quality_by_macro"]
        if pqm:
            print(f"\n  PICK QUALITY BY MACRO SIGNAL (top 5):")
            print(f"    {'Signal':30s} {'n':>5} {'WR':>7} {'PF':>6} Status")
            print(f"    {'-'*60}")
            for sig, d in list(pqm.items())[:5]:
                print(f"    {sig:30s} {d['n']:>5} {d['wr']:>6}% {str(d['pf']):>6} {d['status']}")

    # Weekly Obsidian note
    week_label = datetime.now(timezone.utc).strftime("%Y-W%W")
    write_obsidian_weekly(analysis, week_label)

    return analysis


if __name__ == "__main__":
    run_history_analysis()
