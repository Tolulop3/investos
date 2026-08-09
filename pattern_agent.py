"""
pattern_agent.py — InvestOS v4.2
─────────────────────────────────
Runs 15 minutes after the daily run (9:45am ET via GitHub Actions).
Reads system outputs → detects patterns → writes back to Obsidian vault.

THIS IS THE AUTONOMY LOOP CLOSER:
  Engine produces → Pattern agent reads → Patterns written back → 
  Score boosts applied → Better picks → Repeat

WHAT IT DOES:
  1. Reads last 7 daily history snapshots
  2. Reads last 30 days of outcomes_log.json
  3. Reads score_history.json
  4. Detects: ticker streaks, score velocity, regime correlations,
     sector concentration, regime drift signals
  5. Writes: history/obsidian/patterns.md (weekly summary)
  6. Writes: history/obsidian/tickers/TICKER.md (per recurring ticker)
  7. Writes: history/obsidian/watchlist.md (signal-weighted watchlist)
  8. Writes: pattern_signals.json (machine-readable, for score boosts)

CLOSES THE LOOP:
  pattern_signals.json is read by stock_screener.py
  Tickers with 3+ consecutive days in picks → +3 score boost
  Tickers flagged as AVOID → score penalty
"""

import json
import os
import pathlib
from datetime import datetime, timezone, timedelta
from collections import defaultdict


# ── Config ────────────────────────────────────────────────────────────────
HISTORY_DIR       = pathlib.Path("history")
OBSIDIAN_DIR      = HISTORY_DIR / "obsidian"
TICKER_DIR        = OBSIDIAN_DIR / "tickers"
OUTCOMES_PATH     = pathlib.Path("outcomes_log.json")
SCORE_HISTORY_PATH= pathlib.Path("score_history.json")
PATTERN_OUT       = pathlib.Path("pattern_signals.json")

STREAK_THRESHOLD  = 3    # days in picks before flagging
VELOCITY_THRESHOLD= 5.0  # score points/day = notable trend
LOOKBACK_DAYS     = 30   # outcomes window
HISTORY_WINDOW    = 7    # daily snapshots to read
MIN_PICKS_FOR_PF  = 5    # minimum picks to compute profit factor


# ── Loaders ───────────────────────────────────────────────────────────────

def load_history(n=HISTORY_WINDOW):
    """Load last n daily history snapshots, sorted oldest→newest."""
    snaps = []
    if not HISTORY_DIR.exists():
        return snaps
    files = sorted(HISTORY_DIR.glob("????-??-??.json"))[-n:]
    for f in files:
        try:
            snaps.append(json.loads(f.read_text()))
        except Exception:
            pass
    return snaps


def load_outcomes(days=LOOKBACK_DAYS):
    """Load resolved outcomes from last N days."""
    try:
        all_picks = json.loads(OUTCOMES_PATH.read_text())
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).date().isoformat()
        return [p for p in all_picks
                if p.get("outcome") is not None
                and (p.get("signal_date") or p.get("date") or "") >= cutoff]
    except Exception:
        return []


def load_score_history():
    """Load per-ticker score history."""
    try:
        return json.loads(SCORE_HISTORY_PATH.read_text())
    except Exception:
        return {}


# ── Pattern Detection ──────────────────────────────────────────────────────

def detect_ticker_streaks(snaps):
    """
    Find tickers appearing in picks on consecutive days.
    Returns dict: ticker → {days, scores, streak_signal}
    """
    ticker_days = defaultdict(list)

    for snap in snaps:
        date = snap.get("date", "")
        picks = snap.get("conviction_picks", []) or []
        top   = snap.get("top_picks", []) or []
        all_picks = picks + top

        for p in all_picks:
            t = p.get("ticker") if isinstance(p, dict) else None
            s = p.get("score", 0) if isinstance(p, dict) else 0
            if t:
                ticker_days[t].append({"date": date, "score": s})

    streaks = {}
    for ticker, appearances in ticker_days.items():
        if len(appearances) >= STREAK_THRESHOLD:
            scores = [a["score"] for a in appearances]
            streaks[ticker] = {
                "days":          len(appearances),
                "avg_score":     round(sum(scores) / len(scores), 1),
                "scores":        scores,
                "dates":         [a["date"] for a in appearances],
                "streak_signal": "WATCH" if len(appearances) >= STREAK_THRESHOLD else None,
            }

    return dict(sorted(streaks.items(), key=lambda x: x[1]["days"], reverse=True))


def detect_score_velocity(score_history, snaps):
    """
    Find tickers with rapidly rising or falling scores.
    Uses score_history for full trend, snaps for recent confirmation.
    """
    velocity = {}

    # Get most recent date from snaps
    recent_dates = [s.get("date","") for s in snaps if s.get("date")]
    if not recent_dates:
        return velocity

    for ticker, history in score_history.items():
        if not isinstance(history, list) or len(history) < 3:
            continue

        # history is list of {"date": ..., "score": ...}
        try:
            sorted_h = sorted(history, key=lambda x: x.get("date", ""))
            recent   = sorted_h[-3:]
            scores   = [h.get("score", 0) for h in recent]
            dates    = [h.get("date", "") for h in recent]

            if len(scores) < 2:
                continue

            delta    = scores[-1] - scores[0]
            days     = max(1, (len(recent) - 1))
            vel      = delta / days

            if abs(vel) >= VELOCITY_THRESHOLD:
                velocity[ticker] = {
                    "velocity":   round(vel, 2),
                    "direction":  "RISING" if vel > 0 else "FALLING",
                    "score_now":  scores[-1],
                    "score_3d_ago": scores[0],
                    "delta":      round(delta, 1),
                    "dates":      dates,
                }
        except Exception:
            continue

    return dict(sorted(velocity.items(),
                        key=lambda x: abs(x[1]["velocity"]), reverse=True))


def detect_regime_drift(snaps):
    """
    Detect if regime is trending toward a transition.
    Looks at breadth slope, Sharpe slope, PCR trend.
    """
    if len(snaps) < 3:
        return {"status": "insufficient_data"}

    breadths = []
    sharpes  = []

    for s in snaps:
        b = s.get("breadth", {})
        p200 = b.get("pct_above_200") if isinstance(b, dict) else None
        if p200 is not None:
            try:
                breadths.append(float(p200))
            except (TypeError, ValueError):
                pass

        sh = s.get("sharpe")
        if sh is not None:
            # history snapshot stores sharpe as dict {"sharpe": -3.058, ...}
            # or sometimes as a raw float — handle both
            if isinstance(sh, dict):
                sh = sh.get("sharpe")
            if sh is not None:
                try:
                    sharpes.append(float(sh))
                except (TypeError, ValueError):
                    pass

    result = {"status": "stable", "flags": []}

    if len(breadths) >= 3:
        breadth_slope = (breadths[-1] - breadths[0]) / len(breadths)
        result["breadth_slope_per_day"] = round(breadth_slope, 2)
        result["breadth_now"] = breadths[-1]
        if breadth_slope < -0.5:
            result["flags"].append(
                f"Breadth declining {breadth_slope:.1f}%/day — "
                f"watch for regime shift if sustained"
            )

    if len(sharpes) >= 3:
        sharpe_slope = (sharpes[-1] - sharpes[0]) / len(sharpes)
        result["sharpe_slope_per_day"] = round(sharpe_slope, 3)
        result["sharpe_now"] = sharpes[-1]
        if sharpe_slope > 0.05:
            result["flags"].append(
                f"Sharpe improving {sharpe_slope:.3f}/day — "
                f"guard disengagement approaching"
            )
        elif sharpe_slope < -0.05:
            result["flags"].append(
                f"Sharpe deteriorating {sharpe_slope:.3f}/day — "
                f"monitor position sizing"
            )

    if result["flags"]:
        result["status"] = "watch"

    return result


def detect_sector_concentration(snaps):
    """
    Flag if recent picks are concentrated in one sector.
    Risk: correlated losses if sector turns.
    """
    sector_counts = defaultdict(int)
    total_picks   = 0

    for snap in snaps[-3:]:  # last 3 days
        picks = (snap.get("conviction_picks") or []) + (snap.get("top_picks") or [])
        for p in picks:
            if isinstance(p, dict):
                sector = p.get("sector", "UNKNOWN")
                sector_counts[sector] += 1
                total_picks += 1

    if total_picks == 0:
        return {}

    concentrations = {}
    for sector, count in sector_counts.items():
        pct = count / total_picks * 100
        if pct >= 40:  # flag if sector > 40% of picks
            concentrations[sector] = {
                "count": count,
                "pct":   round(pct, 1),
                "flag":  "HIGH_CONCENTRATION",
            }

    return concentrations


def ticker_pf(ticker, outcomes):
    """Compute profit factor for a ticker from recent outcomes."""
    picks = [p for p in outcomes if p.get("ticker") == ticker]
    if len(picks) < MIN_PICKS_FOR_PF:
        return None, len(picks)

    # FIX (2026-08-09): "return_pct" was never a real outcomes_log.json field
    # (real field: actual_return) -- pf was always None, and write_watchlist()'s
    # "(pf or 0) < 1.0" AVOID condition below was unconditionally true for any
    # FALLING-velocity ticker regardless of its real profit factor.
    wins   = [abs(p.get("actual_return", 0)) for p in picks if p.get("outcome") == "WIN"]
    losses = [abs(p.get("actual_return", 0)) for p in picks if p.get("outcome") == "LOSS"]

    gw = sum(wins)
    gl = sum(losses)

    pf = round(gw / gl, 2) if gl > 0 else None
    return pf, len(picks)


# ── Writers ───────────────────────────────────────────────────────────────

def write_ticker_note(ticker, streak_data, velocity_data, outcomes):
    """Write per-ticker Obsidian note."""
    TICKER_DIR.mkdir(parents=True, exist_ok=True)
    path = TICKER_DIR / f"{ticker}.md"

    pf, n = ticker_pf(ticker, outcomes)
    streak = streak_data.get(ticker, {})
    vel    = velocity_data.get(ticker, {})

    lines = [
        f"---",
        f"ticker: {ticker}",
        f"updated: {datetime.now(timezone.utc).date().isoformat()}",
        f"streak_days: {streak.get('days', 0)}",
        f"score_now: {streak.get('avg_score', 'N/A')}",
        f"velocity: {vel.get('velocity', 0)}",
        f"pf_30d: {pf}",
        f"picks_30d: {n}",
        f"tags: [investos, ticker, pattern]",
        f"---",
        f"",
        f"# {ticker} — Pattern Log",
        f"",
    ]

    if streak:
        lines += [
            f"## Streak",
            f"Appeared in picks **{streak['days']} of last {HISTORY_WINDOW} days**.",
            f"Avg score: {streak['avg_score']}",
            f"Dates: {', '.join(streak.get('dates', []))}",
            f"",
        ]

    if vel:
        direction = vel.get("direction", "")
        icon = "📈" if direction == "RISING" else "📉"
        lines += [
            f"## Score Velocity",
            f"{icon} **{direction}** at {vel['velocity']:+.1f} pts/day",
            f"Score: {vel.get('score_3d_ago')} → {vel.get('score_now')} "
            f"(Δ {vel.get('delta'):+.1f} over 3 days)",
            f"",
        ]

    if pf is not None:
        pf_icon = "✅" if pf >= 1.5 else "⚠️" if pf >= 1.0 else "🔴"
        lines += [
            f"## Recent Performance (last {LOOKBACK_DAYS}d)",
            f"Profit Factor: **{pf}** {pf_icon}  |  n={n} picks",
            f"",
        ]

    lines += [
        f"---",
        f"_Auto-generated by pattern_agent.py · InvestOS v4.2_",
    ]

    path.write_text("\n".join(lines), encoding="utf-8")


def write_watchlist(streaks, velocity, outcomes):
    """
    Write machine-readable + human-readable watchlist.
    Ordered by signal strength.
    """
    OBSIDIAN_DIR.mkdir(parents=True, exist_ok=True)

    # Build combined signal list
    all_tickers = set(streaks.keys()) | set(velocity.keys())

    scored = []
    for ticker in all_tickers:
        s = streaks.get(ticker, {})
        v = velocity.get(ticker, {})
        pf, n = ticker_pf(ticker, outcomes)

        signal_score = (
            s.get("days", 0) * 10 +
            abs(v.get("velocity", 0)) * 5 +
            (10 if pf and pf >= 1.5 else 0)
        )

        direction = v.get("direction", "")
        action = "WATCH"
        if s.get("days", 0) >= STREAK_THRESHOLD and direction == "RISING":
            action = "CONSIDER"
        elif direction == "FALLING" and (pf or 0) < 1.0:
            action = "AVOID"

        scored.append({
            "ticker":       ticker,
            "signal_score": round(signal_score, 1),
            "streak_days":  s.get("days", 0),
            "velocity":     v.get("velocity", 0),
            "pf_30d":       pf,
            "n_30d":        n,
            "action":       action,
        })

    scored.sort(key=lambda x: x["signal_score"], reverse=True)

    # Markdown watchlist
    today = datetime.now(timezone.utc).date().isoformat()
    lines = [
        f"---",
        f"updated: {today}",
        f"tags: [investos, watchlist, pattern]",
        f"---",
        f"",
        f"# InvestOS Pattern Watchlist — {today}",
        f"",
        f"Auto-generated by pattern_agent.py. Do not edit manually.",
        f"",
        f"| Ticker | Action | Streak | Velocity | PF (30d) | n |",
        f"|--------|--------|--------|----------|----------|---|",
    ]

    for item in scored[:30]:
        action_icon = {"CONSIDER": "🟢", "WATCH": "🟡", "AVOID": "🔴"}.get(item["action"], "⚪")
        vel_str = f"{item['velocity']:+.1f}" if item["velocity"] else "—"
        pf_str  = str(item["pf_30d"]) if item["pf_30d"] else "—"
        lines.append(
            f"| {item['ticker']} | {action_icon} {item['action']} | "
            f"{item['streak_days']}d | {vel_str} | {pf_str} | {item['n_30d']} |"
        )

    lines += ["", "---", "_InvestOS v4.2 · NFA · Educational only_"]
    (OBSIDIAN_DIR / "watchlist.md").write_text("\n".join(lines), encoding="utf-8")

    return scored


def check_alpha_half_life(verbose=True):
    """
    Monthly WR for 90-100 tier picks over the last 6 months.
    Flags ALERT if 2 consecutive months fall below 45%.
    """
    try:
        all_outcomes = json.loads(OUTCOMES_PATH.read_text())
    except Exception:
        return {"status": "error", "months": [], "flag": False}

    cutoff = (datetime.now(timezone.utc) - timedelta(days=200)).date().isoformat()
    top_tier = [
        o for o in all_outcomes
        if o.get("resolved") is True
        and o.get("outcome") in ("WIN", "LOSS", "FLAT")
        and (o.get("score", 0) or 0) >= 90
        and (o.get("signal_date") or o.get("date") or "") >= cutoff
    ]

    if not top_tier:
        return {"status": "insufficient_data", "months": [], "flag": False}

    by_month = defaultdict(list)
    for o in top_tier:
        sig_date = o.get("signal_date") or o.get("date") or ""
        if sig_date:
            by_month[sig_date[:7]].append(o)

    today = datetime.now(timezone.utc).date()
    months = []
    for i in range(5, -1, -1):
        m = today.month - i
        y = today.year
        while m <= 0:
            m += 12
            y -= 1
        key  = f"{y:04d}-{m:02d}"
        picks = by_month.get(key, [])
        wr = (sum(1 for p in picks if p["outcome"] == "WIN") / len(picks) * 100
              if picks else None)
        months.append({"month": key, "n": len(picks),
                       "wr": round(wr, 1) if wr is not None else None})

    THRESHOLD = 45.0
    flag, consecutive = False, 0
    for m in months:
        if m["wr"] is not None and m["wr"] < THRESHOLD:
            consecutive += 1
            if consecutive >= 2:
                flag = True
                break
        else:
            consecutive = 0

    status = "ALERT" if flag else "OK"
    if verbose:
        icon = "🚨" if flag else "✅"
        print(f"  {icon} Alpha half-life ({status}): 90-100 tier monthly WR (last 6 months)")
        for m in months:
            wr_str = f"{m['wr']:.0f}%" if m["wr"] is not None else "—"
            warn   = " ⚠️" if m["wr"] is not None and m["wr"] < THRESHOLD else ""
            print(f"     {m['month']}: n={m['n']}  WR={wr_str}{warn}")

    return {"status": status, "months": months, "threshold": THRESHOLD, "flag": flag}


def write_patterns_summary(snaps, streaks, velocity,
                           regime_drift, sector_conc, scored_watchlist,
                           alpha_half_life=None):
    """Write weekly patterns summary note."""
    OBSIDIAN_DIR.mkdir(parents=True, exist_ok=True)
    today = datetime.now(timezone.utc).date().isoformat()

    lines = [
        f"---",
        f"date: {today}",
        f"tags: [investos, patterns, weekly]",
        f"---",
        f"",
        f"# InvestOS Pattern Summary — {today}",
        f"",
        f"## Regime Drift",
    ]

    drift = regime_drift
    if drift.get("status") == "watch":
        for flag in drift.get("flags", []):
            lines.append(f"- ⚠️ {flag}")
    else:
        lines.append(f"- ✅ Regime stable — no drift signals")

    if drift.get("breadth_now"):
        lines.append(f"- Breadth (200MA): {drift['breadth_now']}%  "
                     f"({drift.get('breadth_slope_per_day', 0):+.2f}%/day)")
    if drift.get("sharpe_now"):
        lines.append(f"- Sharpe: {drift['sharpe_now']:.3f}  "
                     f"({drift.get('sharpe_slope_per_day', 0):+.3f}/day)")

    lines += ["", "## Ticker Streaks (appeared 3+ days in picks)"]
    if streaks:
        for ticker, data in list(streaks.items())[:10]:
            lines.append(f"- **{ticker}** — {data['days']}d streak, "
                         f"avg score {data['avg_score']}")
    else:
        lines.append("- No tickers with 3+ day streaks this week")

    lines += ["", "## Score Velocity (rising/falling fast)"]
    if velocity:
        for ticker, data in list(velocity.items())[:8]:
            icon = "📈" if data["direction"] == "RISING" else "📉"
            lines.append(f"- {icon} **{ticker}** {data['velocity']:+.1f} pts/day  "
                         f"({data['score_3d_ago']} → {data['score_now']})")
    else:
        lines.append("- No notable score velocity this week")

    if sector_conc:
        lines += ["", "## ⚠️ Sector Concentration Risk"]
        for sector, data in sector_conc.items():
            lines.append(f"- **{sector}**: {data['pct']}% of recent picks "
                         f"({data['count']} picks) — {data['flag']}")

    lines += ["", "## Signal Watchlist (top 10)"]
    lines.append("| Ticker | Action | Streak | Velocity | PF |")
    lines.append("|--------|--------|--------|----------|----|")
    for item in scored_watchlist[:10]:
        action_icon = {"CONSIDER": "🟢", "WATCH": "🟡", "AVOID": "🔴"}.get(item["action"], "⚪")
        vel_str = f"{item['velocity']:+.1f}" if item["velocity"] else "—"
        pf_str  = str(item["pf_30d"]) if item["pf_30d"] else "—"
        lines.append(f"| {item['ticker']} | {action_icon} {item['action']} | "
                     f"{item['streak_days']}d | {vel_str} | {pf_str} |")

    if alpha_half_life and alpha_half_life.get("months"):
        thr  = alpha_half_life.get("threshold", 45)
        icon = "🚨 ALERT" if alpha_half_life.get("flag") else "✅ OK"
        lines += ["", f"## 📉 Alpha Half-Life Monitor (90–100 tier, monthly WR)"]
        lines.append(f"- Status: **{icon}** (flag if 2 consecutive months < {thr}%)")
        for m in alpha_half_life["months"]:
            wr_str = f"{m['wr']:.0f}%" if m["wr"] is not None else "—"
            warn   = " ⚠️" if m["wr"] is not None and m["wr"] < thr else ""
            lines.append(f"- {m['month']}: n={m['n']}  WR={wr_str}{warn}")

    lines += [
        "", "---",
        f"_Generated by pattern_agent.py · InvestOS v4.2 · {today}_",
        "_NFA · Educational only_"
    ]

    (OBSIDIAN_DIR / "patterns.md").write_text("\n".join(lines), encoding="utf-8")
    print(f"  📊 Patterns summary → {OBSIDIAN_DIR}/patterns.md")


def write_pattern_signals(scored_watchlist):
    """
    Write machine-readable pattern_signals.json.
    This is what stock_screener.py reads for score boosts.
    Format: {ticker: {action, boost, reason}}
    """
    signals = {}
    for item in scored_watchlist:
        ticker = item["ticker"]
        action = item["action"]

        if action == "CONSIDER":
            boost = 3.0
            reason = f"Pattern: {item['streak_days']}d streak + rising velocity"
        elif action == "AVOID":
            boost = -5.0
            reason = f"Pattern: falling score + PF {item['pf_30d'] or 'n/a'}"
        else:
            boost = 0.0
            reason = "Pattern: watch — no strong signal"

        if boost != 0:
            signals[ticker] = {
                "action":  action,
                "boost":   boost,
                "reason":  reason,
                "updated": datetime.now(timezone.utc).date().isoformat(),
            }

    PATTERN_OUT.write_text(json.dumps(signals, indent=2), encoding="utf-8")
    print(f"  🔁 Pattern signals → {PATTERN_OUT} ({len(signals)} tickers)")
    return signals


# ── Main ──────────────────────────────────────────────────────────────────

def run_pattern_agent(verbose=True):
    today = datetime.now(timezone.utc).date().isoformat()

    if verbose:
        print(f"\n{'='*55}")
        print(f"  PATTERN AGENT — {today}")
        print(f"{'='*55}\n")

    # Load
    snaps    = load_history(HISTORY_WINDOW)
    outcomes = load_outcomes(LOOKBACK_DAYS)
    score_h  = load_score_history()

    if verbose:
        print(f"  Loaded: {len(snaps)} history snapshots, "
              f"{len(outcomes)} recent outcomes, "
              f"{len(score_h)} scored tickers")

    # Detect
    streaks     = detect_ticker_streaks(snaps)
    velocity    = detect_score_velocity(score_h, snaps)
    regime_drift= detect_regime_drift(snaps)
    sector_conc = detect_sector_concentration(snaps)

    if verbose:
        print(f"  Streaks detected:   {len(streaks)} tickers (3+ days)")
        print(f"  Velocity signals:   {len(velocity)} tickers")
        print(f"  Regime drift:       {regime_drift.get('status', 'unknown')}")
        if regime_drift.get("flags"):
            for f in regime_drift["flags"]:
                print(f"    ⚠️  {f}")
        if sector_conc:
            print(f"  Sector concentration: {list(sector_conc.keys())}")

    # Write ticker notes for streaking tickers
    TICKER_DIR.mkdir(parents=True, exist_ok=True)
    for ticker in list(streaks.keys())[:20]:
        write_ticker_note(ticker, streaks, velocity, outcomes)
    if verbose:
        print(f"  📝 Ticker notes written: {min(len(streaks), 20)}")

    # Write watchlist
    scored = write_watchlist(streaks, velocity, outcomes)
    if verbose:
        print(f"  📋 Watchlist updated: {len(scored)} tickers")
        consider = [s for s in scored if s["action"] == "CONSIDER"]
        avoid    = [s for s in scored if s["action"] == "AVOID"]
        if consider:
            print(f"  🟢 CONSIDER: {[s['ticker'] for s in consider[:5]]}")
        if avoid:
            print(f"  🔴 AVOID:    {[s['ticker'] for s in avoid[:5]]}")

    # Alpha half-life monitor
    alpha_hl = check_alpha_half_life(verbose=verbose)
    if verbose and alpha_hl.get("flag"):
        print(f"  🚨 ALPHA ALERT: 90-100 tier WR below 45% for 2+ consecutive months")

    # Write patterns summary
    write_patterns_summary(snaps, streaks, velocity,
                           regime_drift, sector_conc, scored, alpha_hl)

    # Write machine-readable signals for score_screener
    signals = write_pattern_signals(scored)

    if verbose:
        print(f"\n{'='*55}")
        print(f"  PATTERN AGENT COMPLETE")
        print(f"  Outputs:")
        print(f"    history/obsidian/patterns.md      ← weekly summary")
        print(f"    history/obsidian/watchlist.md     ← signal watchlist")
        print(f"    history/obsidian/tickers/*.md     ← per-ticker notes")
        print(f"    pattern_signals.json              ← score boost feed")
        print(f"  Boosting {len([s for s in signals.values() if s['boost']>0])} tickers")
        print(f"  Penalising {len([s for s in signals.values() if s['boost']<0])} tickers")
        print(f"{'='*55}\n")

    return {
        "streaks":      len(streaks),
        "velocity":     len(velocity),
        "regime_drift": regime_drift.get("status"),
        "signals":      len(signals),
    }


if __name__ == "__main__":
    run_pattern_agent(verbose=True)
