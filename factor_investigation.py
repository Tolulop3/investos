#!/usr/bin/env python3
"""
Factor Attribution Investigation — InvestOS
Run: python factor_investigation.py
Answers the hedge fund question: What is actually making money?

Uses whatever is captured in outcomes.json.
Shows coverage for each factor so you know what's real vs estimated.
"""

import json
from collections import defaultdict
from datetime import datetime

def load():
    # Try both filenames — outcome_tracker uses "outcomes_log.json"
    for fname in ["outcomes_log.json", "outcomes.json"]:
        try:
            data = json.load(open(fname))
            print(f"  Loaded {len(data)} total picks from {fname}")
            return data
        except FileNotFoundError:
            continue
        except Exception as e:
            print(f"  ERROR loading {fname}: {e}")
    print("  ERROR: No outcomes file found (tried outcomes_log.json, outcomes.json)")
    return []

def resolved_only(outcomes):
    return [o for o in outcomes
            if o.get("outcome") in ("WIN","LOSS","FLAT")
            and o.get("actual_return") is not None]

def tier(score):
    if score >= 90: return "90-100"
    if score >= 75: return "75-89"
    if score >= 60: return "60-74"
    return "below-60"

def stats(picks):
    if not picks:
        return None
    wins     = [p for p in picks if p.get("outcome") == "WIN"]
    rets     = [p.get("actual_return", 0) or 0 for p in picks]
    wins_r   = [r for r in rets if r > 0]
    loss_r   = [abs(r) for r in rets if r < 0]
    wr       = len(wins) / len(picks) * 100
    avg      = sum(rets) / len(rets)
    pf       = round(sum(wins_r)/sum(loss_r), 2) if loss_r else 0
    return {
        "n": len(picks), "wr": round(wr,1), "avg": round(avg,2),
        "pf": pf,
        "avg_win":  round(sum(wins_r)/len(wins_r), 2) if wins_r else 0,
        "avg_loss": round(sum(loss_r)/len(loss_r), 2) if loss_r else 0,
    }

def pf_flag(pf):
    return "✅" if pf >= 1.5 else "⚠️ " if pf >= 1.0 else "🔴"

def print_stats(label, s, width=28):
    if not s: return
    print(f"  {label:<{width}}  n={s['n']:>4}  WR={s['wr']:>5.1f}%  "
          f"avg={s['avg']:>+5.2f}%  PF={s['pf']:.2f} {pf_flag(s['pf'])}")

def main():
    print("\nFACTOR ATTRIBUTION INVESTIGATION")
    print("="*60)

    outcomes = load()
    resolved = resolved_only(outcomes)
    print(f"  Resolved picks available: {len(resolved)}")

    if not resolved:
        print("  No resolved picks found — check outcomes_log.json exists in repo")
        return
    print()

    # ── 1. TIER BASELINE ──────────────────────────────────────
    print("1. TIER BASELINE (profit factor view)")
    print("─"*60)
    by_tier = defaultdict(list)
    for o in resolved:
        by_tier[tier(o.get("score", 0))].append(o)

    for t in ["90-100","75-89","60-74","below-60"]:
        print_stats(t, stats(by_tier[t]))
    print()

    # ── 2. SCORE INVERSION — REGIME BREAKDOWN ────────────────
    print("2. DO HIGH SCORES FAIL IN A SPECIFIC REGIME?")
    print("─"*60)
    high = by_tier["90-100"]
    by_regime = defaultdict(list)
    for o in high:
        by_regime[o.get("regime", "Unknown")].append(o)

    covered = sum(1 for o in high if o.get("regime") and o["regime"] != "Unknown")
    print(f"  Coverage: {covered}/{len(high)} picks have regime data")
    for regime in sorted(by_regime, key=lambda r: -len(by_regime[r])):
        print_stats(f"  {regime}", stats(by_regime[regime]), width=22)
    print()

    # ── 3. SECTOR CONCENTRATION ───────────────────────────────
    print("3. IS THE 90-100 TIER CONCENTRATED IN ONE SECTOR?")
    print("─"*60)
    sector_counts = defaultdict(list)
    for o in high:
        s = o.get("sector", "") or "Unknown"
        sector_counts[s].append(o)

    covered_s = sum(1 for o in high if o.get("sector"))
    print(f"  Coverage: {covered_s}/{len(high)} picks have sector data")
    for sector in sorted(sector_counts, key=lambda s: -len(sector_counts[s]))[:8]:
        print_stats(f"  {sector[:22]}", stats(sector_counts[sector]), width=24)
    print()

    # Compare: same sectors in 60-74 tier
    print("  Same sectors in 60-74 tier (for comparison):")
    low = by_tier["60-74"]
    low_sectors = defaultdict(list)
    for o in low:
        s = o.get("sector", "") or "Unknown"
        low_sectors[s].append(o)
    top_sectors = sorted(sector_counts, key=lambda s: -len(sector_counts[s]))[:4]
    for sector in top_sectors:
        print_stats(f"  {sector[:22]}", stats(low_sectors.get(sector, [])), width=24)
    print()

    # ── 4. DATE TREND — are high scores getting worse over time? ──
    print("4. DATE TREND — Is the inversion getting better or worse?")
    print("─"*60)
    dated = [o for o in high if o.get("signal_date")]
    dated.sort(key=lambda x: x["signal_date"])
    n = len(dated)
    if n >= 60:
        thirds = [dated[:n//3], dated[n//3:2*n//3], dated[2*n//3:]]
        for i, group in enumerate(thirds, 1):
            d_range = f"{group[0]['signal_date']} → {group[-1]['signal_date']}"
            s = stats(group)
            if s:
                print(f"  Period {i} ({d_range[:22]})")
                print(f"    {'':28}  n={s['n']:>4}  WR={s['wr']:>5.1f}%  "
                      f"avg={s['avg']:>+5.2f}%  PF={s['pf']:.2f} {pf_flag(s['pf'])}")
    else:
        print(f"  Only {n} dated picks — need more for trend")
    print()

    # ── 5. TICKER CONCENTRATION ───────────────────────────────
    print("5. IS THE 90-100 TIER DOMINATED BY REPEAT LOSERS?")
    print("─"*60)
    ticker_stats = defaultdict(list)
    for o in high:
        ticker_stats[o.get("ticker","?")].append(o)

    worst = sorted(ticker_stats.items(),
                   key=lambda x: stats(x[1])["avg"] if stats(x[1]) else 0)[:10]
    print("  Worst avg-return tickers in 90-100 tier:")
    for ticker, picks in worst:
        s = stats(picks)
        if s and s["n"] >= 3:
            print_stats(f"  {ticker}", s, width=16)
    print()

    # ── 6. ML COVERAGE ────────────────────────────────────────
    print("6. ML FACTOR COVERAGE (shows what requires more time)")
    print("─"*60)
    has_ml    = [o for o in resolved if o.get("ml_prob") not in (None, 0.5, 0)]
    has_rs    = [o for o in resolved if o.get("rs_rating") not in (None, 0, 50)]
    has_news  = [o for o in resolved if o.get("news_boost") not in (None, 0)]
    has_sect  = [o for o in resolved if o.get("sector")]
    has_reg   = [o for o in resolved if o.get("regime") and o["regime"] != "Unknown"]
    has_ureg  = [o for o in resolved if o.get("unified_regime") and o["unified_regime"] != "UNKNOWN"]

    n_res = len(resolved) or 1  # guard against div/zero
    print(f"  ml_prob (real, not 0.5 default): {len(has_ml):>4} picks  "
          f"({len(has_ml)/n_res*100:.0f}% coverage)")
    print(f"  rs_rating (real, not 50 default): {len(has_rs):>4} picks  "
          f"({len(has_rs)/n_res*100:.0f}% coverage)")
    print(f"  news_boost:     {len(has_news):>4} picks  ({len(has_news)/n_res*100:.0f}% coverage)")
    print(f"  sector:         {len(has_sect):>4} picks  ({len(has_sect)/n_res*100:.0f}% coverage)")
    print(f"  regime:         {len(has_reg):>4} picks  ({len(has_reg)/n_res*100:.0f}% coverage)")
    print(f"  unified_regime: {len(has_ureg):>4} picks  ({len(has_ureg)/n_res*100:.0f}% coverage)")
    print()

    if len(has_ml) >= 50:
        print("  ML ATTRIBUTION (available picks):")
        high_ml = [o for o in has_ml if tier(o.get("score",0)) == "90-100"]
        if high_ml:
            ml_strong = [o for o in high_ml if o.get("ml_prob",0) >= 0.20]
            ml_weak   = [o for o in high_ml if o.get("ml_prob",0) <  0.20]
            print_stats("  90-100 + ML≥20%", stats(ml_strong), width=20)
            print_stats("  90-100 + ML<20%", stats(ml_weak), width=20)
        print()

    # ── 7. THE ANSWER ─────────────────────────────────────────
    print("7. WHAT THE DATA SAYS (available now)")
    print("─"*60)
    t90 = stats(by_tier["90-100"])
    t60 = stats(by_tier["60-74"])
    if t90 and t60:
        print(f"  90-100 tier PF = {t90['pf']:.2f} → "
              + ("LOSES MONEY in aggregate" if t90['pf'] < 1.0 else "marginal edge"))
        print(f"  60-74 tier PF  = {t60['pf']:.2f} → "
              + ("STRONG EDGE" if t60['pf'] >= 1.5 else "positive edge"))
        print()
        if t90['pf'] < 1.0:
            print("  IMPLICATION: The 90-100 scoring tier should NOT be traded")
            print("  without additional ML confirmation (≥20% threshold).")
            print("  Evidence-backed rule: WATCH, not BUY, for this tier.")
            print()
            print("  The system already does this. Now you can prove why.")

if __name__ == "__main__":
    main()
