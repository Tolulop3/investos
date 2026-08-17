"""
scripts/backfill_walkforward_oos.py — One-time walk-forward OOS backfill
==========================================================================
strategy_version.json already accumulates one dated, version-tagged entry
per day (see strategy_version.py::log_strategy_version) but until the
2026-08-17 fix, entries carried a resolved-pick COUNT with no performance
numbers (win rate / avg return / alpha vs SPX). This script backfills the
missing "oos_performance" field on every EXISTING entry that lacks it,
replaying outcome_tracker.py's own OOS win-rate math for that entry's
(oos_start, date) window, so the dashboard has real multi-week history on
day one instead of starting from zero.

CAVEAT (documented, not hidden): this uses TODAY's final "resolved" status
for every pick, not what was actually known as of each historical date. A
pick signaled near a given entry's date may not have been resolved for
weeks after — so early/recent windows in the backfilled series reflect
final outcomes, not real-time knowledge at the time. This does NOT affect
which picks are included (signal_date is still bounded to
oos_start <= signal_date <= entry_date, matching what oos_resolved_picks
already measured live), only how complete their resolution looks in
hindsight. Older windows are least affected (most picks are long since
resolved by now); the most recent 1-2 weeks of any backfilled window are
the part to read with that caveat in mind.

Idempotent: only fills entries where "oos_performance" is missing/None —
safe to re-run, and never touches entries the live pipeline already wrote
(going forward, run_daily.py passes fresh win_rate data directly).

Usage:
    python scripts/backfill_walkforward_oos.py            # writes strategy_version.json
    python scripts/backfill_walkforward_oos.py --dry-run   # prints the reconstructed series only
"""

import json
import sys
import urllib.request

SV_PATH = "strategy_version.json"
OUTCOMES_PATH = "outcomes_log.json"


def _load(path):
    with open(path) as f:
        return json.load(f)


def _fetch_spx_series():
    """One fetch covering the whole backfill range (not per-entry — avoids
    hammering the endpoint 50 times). Best-effort: returns {} on any failure,
    matching outcome_tracker.py's own try/except-wrapped SPX fetch."""
    try:
        url = "https://query1.finance.yahoo.com/v8/finance/chart/SPY?interval=1d&range=1y"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=10) as r:
            d = json.loads(r.read().decode())
        ts = d["chart"]["result"][0]["timestamp"]
        cls = d["chart"]["result"][0]["indicators"]["quote"][0]["close"]
        import datetime as _dt
        return {
            _dt.date.fromtimestamp(t).isoformat(): c
            for t, c in zip(ts, cls) if c
        }
    except Exception as e:
        print(f"  ⚠️  SPX series fetch failed ({e}) — active_return will be null for all entries")
        return {}


def _spx_return(spx_series, start_date, end_date):
    if not spx_series:
        return None
    dates = sorted(d for d in spx_series if start_date <= d <= end_date)
    if not dates:
        return None
    start_px, end_px = spx_series[dates[0]], spx_series[dates[-1]]
    if not start_px:
        return None
    return round((end_px - start_px) / start_px * 100, 2)


def _reconstruct_oos_performance(outcomes, oos_start, as_of_date, spx_series):
    window = [
        o for o in outcomes
        if o.get("resolved") and o.get("outcome") in ("WIN", "LOSS", "FLAT")
        and oos_start <= (o.get("signal_date") or "") <= as_of_date
    ]
    if not window:
        return {"win_rate": None, "avg_return": None, "spx_return": None,
                "active_return": None, "resolved": 0, "tiers": {}}

    wins = [o for o in window if o["outcome"] == "WIN"]
    wr = round(100 * len(wins) / len(window), 1)
    avg_ret = round(sum(o["actual_return"] for o in window) / len(window), 2)
    spx_ret = _spx_return(spx_series, oos_start, as_of_date)

    # Gapless, half-open bounds (matches the 2026-08-17 fix in
    # outcome_tracker.py) -- integer bounds silently dropped float scores
    # landing in (59,60)/(74,75)/(89,90), e.g. 74.3, 89.7.
    tiers = {}
    for lo, hi, in_tier in [
        (90, 100, lambda s: s >= 90),
        (75, 89,  lambda s: 75 <= s < 90),
        (60, 74,  lambda s: 60 <= s < 75),
        (0, 59,   lambda s: s < 60),
    ]:
        bucket = [o for o in window if in_tier(o.get("score") or 0)]
        tiers[f"{lo}-{hi}"] = {
            "n": len(bucket),
            "wr": round(100 * sum(1 for o in bucket if o["outcome"] == "WIN") / len(bucket), 1) if bucket else None,
        }

    return {
        "win_rate":      wr,
        "avg_return":    avg_ret,
        "spx_return":    spx_ret,
        "active_return": round(avg_ret - spx_ret, 2) if spx_ret is not None else None,
        "resolved":      len(window),
        "tiers":         tiers,
    }


def main():
    dry_run = "--dry-run" in sys.argv

    history = _load(SV_PATH)
    outcomes = _load(OUTCOMES_PATH)
    spx_series = _fetch_spx_series()

    n_filled = 0
    for entry in history:
        if entry.get("oos_performance"):
            continue
        perf = _reconstruct_oos_performance(
            outcomes, entry["oos_start"], entry["date"], spx_series,
        )
        # Distinguishes hindsight-reconstructed entries (this script, using
        # TODAY's final resolved status for picks signaled by that date) from
        # entries log_strategy_version() computes live going forward (real
        # point-in-time knowledge) -- see module docstring's CAVEAT. Phase 2's
        # dashboard should read this before treating a window as "as it
        # looked live" vs "as it looks in hindsight".
        perf["backfilled"] = True
        entry["oos_performance"] = perf
        n_filled += 1
        print(f"  {entry['date']}  v{entry['version']:<5} "
              f"n={perf['resolved']:<4} "
              f"WR={perf['win_rate'] if perf['win_rate'] is not None else '--':>5} "
              f"avg={perf['avg_return'] if perf['avg_return'] is not None else '--':>6} "
              f"alpha={perf['active_return'] if perf['active_return'] is not None else '--'}")

    print(f"\n{'[DRY RUN] would fill' if dry_run else 'Filled'} {n_filled}/{len(history)} entries")

    if not dry_run and n_filled:
        with open(SV_PATH, "w") as f:
            json.dump(history, f, indent=2)
        print(f"✅ Wrote {SV_PATH}")


if __name__ == "__main__":
    main()
