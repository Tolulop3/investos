# INVESTOS — Personal Intelligence Platform
**@adejuwon_t · Toronto, Canada · Live at [tolulop3.github.io/investos](https://tolulop3.github.io/investos)**

> **FOR AI ASSISTANTS:** This README is the authoritative context document for every session.
> Read this fully before touching any file. The system is live, running daily.
> Do not break what works. Optimise only.
>
> Start every session by stating current system state before suggesting anything.
> Do not re-explain decisions listed in "Architecture Decisions — Do Not Revisit".
> Do not flag intentional design choices as bugs.

---

## WHAT THIS IS

A fully automated daily investment intelligence platform. Every weekday at 9:30am ET,
GitHub Actions runs `run_daily.py`, which screens 189 stocks, scores them with ML,
detects macro signals from 15 news feeds, sizes positions by volatility, generates ETF
picks, tracks NGX signals, and bakes everything into a live dashboard.

Personal tool for one user (Toronto). Not yet a product. Not public-facing.
Goal: open the dashboard, know what the market is doing, know what to do, trust it.

**Bigger goal: InvestOS is the IP engine. Every product below reads from it.**

**Project started: March 1, 2026.** 1,500+ commits since. Full history is in git log —
this doc tracks state and major milestones, not every change. See SESSION LOG below for
the milestone-level arc from day 1, and CURRENT SYSTEM STATE above for where things
stand right now.

---

## CURRENT SYSTEM STATE (as of August 15, 2026)

> Rule version: **v4.1 ran continuously from Jun 26 → Aug 15, 2026.** v4.2 (pillar
> rebalance) shipped Aug 15 — this is Day 0. Numbers below marked "OOS (v4.1)" are the
> closed v4.1 window; v4.2 has no resolved picks yet. See `strategy_version.py` for the
> versioning history — a July 4 sector-gate change was informally called "v4.2" in an
> earlier draft of this doc before that label was ever actually logged; that was wrong
> and has been corrected everywhere in this file. Don't reuse "v4.2" for anything else.

| Metric | Value | Status |
|--------|-------|--------|
| Rolling Sharpe (90d) | **-0.32** (1,611 picks, May 17 → Aug 15) | 🔴 WARNING, guard firing — but see note below, this is a rolling-window artifact of a known event, not new degradation |
| Alpha vs SPX | **-4.18** | 🔴 same caveat |
| Win rate overall | **47.2%** (N=2,546 resolved) | ⚠️ |
| PF overall | **1.05** | ⚠️ Near break-even |
| Avg return/pick | +0.09% | ⚠️ |
| Unified regime | NEUTRAL (+0.16) | ⚠️ market +1.0 / macro -0.3 / health -0.5 |
| Macro regime | RISK_OFF | 🔴 |
| Robustness | 25/100 | 🔴 (was 50 on Jul 4 — mechanically tracks Sharpe, see note below) |
| Risk multiplier / exposure | 0.5× / 75% | ⚠️ NEUTRAL band |
| Neg alpha streak | **50 calendar days** | 🔴 |
| Universe | 262 tickers (static + dynamic scout) | ✅ |
| OOS (v4.1, closed Aug 15) | 689 logged / 579 resolved / WR 47.2% | Active return -3.25% vs SPX +3.34% |
| OOS (v4.2, started Aug 15) | 0 resolved yet | ⏳ Real read: ~Nov 15, 2026. Informal interim look ~Oct 15 — NOT a decision point |
| NGX | tracked separately in `ngx_outcomes.json` | excluded from main metrics |
| Runtime | ~90s | ✅ |
| Last audit | Aug 15, 2026 (this refresh) | |

> **Why Sharpe looks like it's declining (checked 2026-08-15, see
> `strategy_version.py`'s SHARPE SLIDE EXPLAINED note):** it's a rolling-90-day-window
> artifact, not ongoing degradation. One outsized bad week (Jun 22-28, n=341 resolved,
> avg return -3.72%) — which lines up exactly with a real, already-documented SPX
> drawdown, not a stock-picking failure — stays anchored in the window while the
> strong early-May weeks roll off the back. Last 14 days alone (clean of that event):
> +0.86% avg return, 48.1% WR — actually fine. This reading should mechanically
> self-correct around **Sep 20-26** as that cohort rolls out, independent of v4.2. If
> Sharpe does NOT recover by then, that's a genuine new signal worth investigating —
> not yet done, this only explains the past 23 days.

**Target goal (unchanged, not yet met — Architecture Decision #5):** Sharpe > 1.0
sustained, before any product launch. Current: -0.32, but see the rolling-window note
above before reading that as things getting worse — it isn't, yet. Still a real
1.32-point gap regardless of the mechanical explanation. Robustness (`risk_engine.py::
compute_robustness_score()`) dropping 50→25 on Aug 3 is not a separate signal — it's a
deterministic formula built directly from rolling Sharpe, alpha-vs-SPX, and
neg-alpha-days, so it mechanically tracks the same explained rolling-window artifact
above, not an independent problem. v4.2's real payoff won't be measurable until the
Nov 15 read either way.

**Score tier performance (N=2,546 resolved picks, Aug 15, 2026 — this is the exact
pattern v4.2 was built to fix; see `strategy_version.py`'s v4.2 note for the full
root-cause and the honest limits of what was and wasn't confirmed):**
| Tier | n | WR | Avg Return | PF | Notes |
|------|---|----|------------|-----|-------|
| 90-100 | 1,103 | 43.6% | -0.29% | 0.85 🔴 | Worst tier — the inversion v4.2 targets |
| 75-89 | 693 | 50.8% | +0.11% | 1.07 ⚠️ | |
| 60-74 | 509 | 52.3% | +0.87% | 1.72 ✅ | Sweet spot — strong edge |
| below-60 | 216 | 42.1% | -0.04% | 0.98 ⚠️ | |

**ML calibration (by ml_prob bucket, Aug 15, 2026):**
| ml_prob | N | WR | PF | Signal |
|---------|---|----|----|--------|
| 0.0-0.2 | 908 | 45.8% | 1.00 | ⚠️ Neutral — this is the ML-gate-excluded band |
| 0.2-0.4 | 423 | 46.6% | 0.94 | ⚠️ |
| 0.4-0.5 | 246 | 48.0% | 1.15 | ⚠️ |
| 0.5-0.6 | 163 | 50.9% | 1.24 | ⚠️ |
| 0.6-0.8 | 322 | 54.7% | 2.96 | ✅ Sweet spot |
| 0.8-1.0 | 484 | 43.8% | 0.64 | 🔴 Overconfidence band |

> NGX excluded from all main metrics. Chronic losers (10 tickers) permanently excluded
> via `long_cooldowns.json` (90-day rolling block, auto-renew if WR < 35% on expiry).

---

## THE PLATFORM VISION

InvestOS is the oil well. The products below are the refineries.

```
INVESTOS ENGINE (private IP — never expose)
              ↓
         BAKED_DATA
    (single source of truth)
              ↓
  ┌───────────┼───────────┬──────────────┐
  ↓           ↓           ↓              ↓
AllocOS    InvestOS    Regime Feed    Data API
(personal   Lens        (macro        (B2B
→ product)  (Chrome     signals)      signal
            extension)               feed)
```

---

## PRODUCT ROADMAP

### PRODUCT 1 — AllocOS
Portfolio allocation intelligence. Reads InvestOS signals + account balances →
outputs exact deployment plan per account (TFSA/FHSA/RRSP/NGX).
Telegram morning brief + web dashboard.
**Tiers:** Free (regime only) · Pro $15 CAD/mo · Elite $35 CAD/mo
**Status:** Design complete. Needs Telegram bot setup first.

### PRODUCT 2 — InvestOS Lens
Chrome extension. Hover over any ticker → InvestOS score ring popup.
**Status:** Design complete. Standalone build — any session.

### PRODUCT 3 — NGX Intelligence
Institutional macro signals for Nigerian Exchange.
**FTSE frontier reclassification September 2026 → $840M-$1B+ inflows.**
**Status:** Engine running. Waitlist page = next marketing step.

### PRODUCT 4 — Data API
Daily JSON signal feed for advisors, boutique PMs, fintech builders.
$200-500 USD/month. No extra build — API wrapper on BAKED_DATA.

### PRODUCT 5 — Newsletter
NFA educational brief via Telegram. Content engine already generates daily.

### PRODUCT 6 — Fund (Year 3+)
Requires 3yr verified track record + OSC exempt structure.

---

## AUTONOMY STACK (current state)

```
LAYER 1 — DATA PIPELINE ✅ fully autonomous
──────────────────────────────────────────────
GitHub Actions 9:30am ET weekdays
→ run_daily.py (12 steps, 189 tickers, 65s)
→ scout_agent.py (Sunday 6am — universe expansion)
→ ml_retrainer.py (auto-fires when coverage gate hits ~143 picks)
→ Commits outcomes, history, scores to repo

LAYER 2 — KNOWLEDGE LAYER ✅ live June 27
──────────────────────────────────────────────
history/obsidian/YYYY-MM-DD.md — daily notes (GitHub Actions writes)
history/obsidian/patterns.md   — weekly pattern summary
history/obsidian/watchlist.md  — signal watchlist
history/obsidian/tickers/*.md  — per-ticker notes (auto-created)
history/obsidian/research/*.md — daily strategist research notes
pattern_signals.json           — machine-readable score boosts

LAYER 3 — PATTERN FEEDBACK LOOP ✅ live June 27
──────────────────────────────────────────────
pattern_agent.py (9:45am ET):
→ reads last 7 daily snapshots + outcomes
→ detects streaks, velocity, regime drift, sector concentration
→ writes pattern_signals.json (score boosts/penalties)
→ stock_screener.py reads this → CONSIDER tickers get +3pts
→ AVOID tickers get -5pts
LOOP CLOSED: observation → pattern → score → picks → outcomes → repeat

LAYER 4 — STRATEGIST ✅ live June 27
──────────────────────────────────────────────
strategist_agent.py (10:00am ET):
→ reads brief + patterns + history + Obsidian notes
→ calls Claude API to reason about the data
→ writes research note to history/obsidian/research/YYYY-MM-DD.md
→ flags contradictions, regime drift, opportunities, watch items

LAYER 5 — ML IMPROVEMENT ⏳ on timers
──────────────────────────────────────────────
ML retrain: ~143 more featured picks needed (~3 weeks from Jun 26)
Walk-forward validation: August 2026
Portfolio optimizer: September 2026 (90d outcomes resolve)
```

---

## OBSIDIAN VAULT

**On Mac:** `~/Documents/investos-brain` (synced via Obsidian Git plugin)
**In repo:** `history/obsidian/` (written by GitHub Actions)

Both locations contain the same files. The Git plugin syncs within 10 minutes.

**Why notes may not appear on Mac yet:**
The Obsidian Git plugin needs to pull from the repo after GitHub Actions commits.
Check: Obsidian → Command palette → "Obsidian Git: Pull" to force sync.
Or wait for the auto-pull interval (set in plugin settings, default 5-10 min).

**Vault structure:**
```
history/obsidian/
├── YYYY-MM-DD.md      ← daily system notes (auto-written)
├── patterns.md        ← weekly pattern summary (pattern_agent)
├── watchlist.md       ← signal watchlist (pattern_agent)
├── tickers/           ← per-ticker notes (pattern_agent)
│   └── TICKER.md
└── research/          ← daily research notes (strategist_agent)
    └── YYYY-MM-DD.md
```

---

## FILE REGISTRY

| File | Lines | Purpose |
|------|-------|---------|
| `run_daily.py` | ~1982 | Master orchestrator — 12 steps + all agent calls |
| `stock_screener.py` | ~1210 | Screens 189 tickers, scores 0-100, reads pattern_signals |
| `ml_engine.py` | 1210 | XGBoost + vol-targeted sizing + Kelly + sector cap + ML gate |
| `ml_retrainer.py` | 443 | XGBoost retraining with coverage gate |
| `risk_engine.py` | ~1476 | Stress test, decay monitor, unified regime engine |
| `news_analyzer.py` | 625 | 15 RSS feeds, sector sentiment, macro regime |
| `intelligence_layers.py` | ~792 | RS ratings, score history, trend detection |
| `etf_engine.py` | ~408 | 30 ETFs, RRSP/TFSA/FHSA routing |
| `content_engine.py` | ~641 | Tweet + thread generation |
| `portfolio_engine.py` | ~1213 | Trades tracker, scorecard, open positions |
| `outcome_tracker.py` | 559 | Win rate tracking, score tier analysis |
| `factor_investigation.py` | 212 | Factor attribution — runs every daily run |
| `scout_agent.py` | ~410 | Sunday 6AM: universe expansion (30 ETF sources) |
| `pattern_agent.py` | 595 | 9:45am: detects patterns, writes signals, closes loop |
| `strategist_agent.py` | 356 | 10:00am: Claude reasons about data, writes research note |
| `history_analyzer.py` | 488 | Silent daily: generates history_analysis.json |
| `strategy_version.py` | — | OOS anchor — logs rule params per run, now on v4.2 |
| `strategy_engine.py` | 247 | Regime-aware dynamic factor weights (RISK_ON/CAUTIOUS/DEFENSIVE/CAPITAL_PRESERVATION) |
| `gate_engine.py` | 187 | Hysteresis-aware ML gate (score≥90, ml_prob<20% → excluded), built Jul 9 |
| `pick_utils.py` | 145 | Shared pick-dict accessors — sector/category/hold_days live nested, not top-level; this is the fix for that class of bug |
| `evidence_engine.py` | 266 | Historical backing enrichment for picks — revived Aug 9 after being silently broken since inception |
| `insider_engine.py` | 745 | SEC EDGAR Form 4 insider trading signals — logged as an ML feature |
| `ngx_screener.py` | ~360 | Nigerian Exchange macro-driven signals |
| `crypto_engine.py` | ~357 | BTC + SOL signals |
| `index.html` | ~7148 | Full dashboard — baked daily |
| `.github/workflows/daily_run.yml` | — | 9:30am ET weekdays — main pipeline |
| `.github/workflows/pattern_agent.yml` | — | 9:45am ET weekdays |
| `.github/workflows/strategist_agent.yml` | — | 10:00am ET weekdays |
| `.github/workflows/weekly_scout.yml` | — | Sunday 6AM ET |
| `.github/workflows/tests.yml` | — | CI test suite |

> `congressional_engine.py` was removed Aug 9, 2026 (dead code — SEC EDGAR Form 4 now
> handled by `insider_engine.py`).

**Persistent data files (never delete):**
| File | Contents |
|------|----------|
| `outcomes_log.json` | Every pick — 2,656 total (2,546 resolved) |
| `cooldown_flags.json` / `long_cooldowns.json` | Permanent exclusions + 90-day rolling cooldowns |
| `pf_baseline.json` | PF baseline, locked June 25 |
| `pattern_signals.json` | Pattern agent score boosts — updated 9:45am |
| `history_analysis.json` | Weekly structured analysis |
| `strategy_version.json` | Rule-state audit trail — v4.1 Jun 26 → Aug 15, v4.2 from Aug 15 |
| `universe_current.json` | Current universe (static + dynamic scout, 262 tickers) |
| `history/YYYY-MM-DD.json` | Daily snapshots — started June 6, 2026 |
| `history/obsidian/` | All Obsidian notes — synced to Mac vault |

---

## ARCHITECTURE

### The Daily Pipeline (12 steps + agents)

```
9:30am  [1]  NEWS & MACRO    → news_analyzer.py
        [2]  MARKET REGIME   → risk_engine.py
        [3]  STOCK SCREEN    → stock_screener.py (reads pattern_signals.json)
        [4]  APPLY NEWS      → run_daily.py
        [4c] INSIDER SIGNALS → insider_engine.py (SEC EDGAR Form 4 — replaces the
                                removed congressional_engine.py)
        [5]  ML ENGINE       → ml_engine.py (sector-first gate + ML gate — gate_engine.py)
        [6]  INTELLIGENCE    → intelligence_layers.py
        [7]  X FEEDS         → run_daily.py
        [8]  CONVICTION      → run_daily.py
        [9]  FX & GOLD       → risk_engine.py
        [10] CRYPTO          → crypto_engine.py
        [11] RISK AUDIT      → risk_engine.py
        [ETF] ETF ENGINE     → etf_engine.py
        [12] CONTENT         → content_engine.py
        [NGX] NGX ENGINE     → ngx_screener.py
        [END] FACTOR REPORT  → factor_investigation.py
        [END] HISTORY ANALYZER → history_analyzer.py (silent)
        [END] STRATEGIST     → strategist_agent.py (if ANTHROPIC_API_KEY set)
        [END] OBSIDIAN BRIDGE → writes daily note

9:45am  pattern_agent.py (separate workflow)
        → reads snapshots + outcomes
        → writes pattern_signals.json + Obsidian notes

10:00am strategist_agent.py (separate workflow)
        → reads everything → calls Claude API
        → writes research note
```

### Score Compression (v4.1 — June 26 2026, unchanged by v4.2)
```python
# Above 85: each raw point = 0.4 calibrated points (was 0.6)
# Raw 100 → 91. Raw 94 → 88.6 (shifts to 75-89 tier)
adjusted = 85 + (excess * 0.4)
```
This compresses the *final* 0-100 total and is independent of how pillars are weighted.
v4.2 (below) changes the pillar weighting that feeds into this — the two are complementary,
not overlapping.

### Pillar Rebalance (v4.2 — August 15 2026)
```python
# Momentum cap 35 -> 22, growth 15 -> 20, value 12 -> 16, safety 13 -> 15
# (dividend_income, volume_liquidity unchanged). Bonus layer capped at +/-15 (was
# unbounded). Root cause: momentum was overweighted since 2026-05-03 based on a
# self-referential health check that never validated momentum against future win/loss.
# Full root-cause + validation methodology + the honest limits of what got confirmed:
# strategy_version.py's v4.2 OVERRIDE NOTE and DOWNSTREAM GAP INVESTIGATION note.
```

### Three-Layer Unified Regime Engine
```
market_score  = +1.0   (SPX vs 200MA)
macro_score   = -0.3   (news signals)
health_score  = -0.5   (Sharpe -0.32, currently negative — see CURRENT SYSTEM STATE)
unified_score = (0.40 × market) + (0.30 × macro) + (0.30 × health)
→ current: NEUTRAL (75% exposure, 0.5× risk multiplier) — re-derive from live brief,
  this example is illustrative, not a fixed constant
```

### Safety Gates (in order)
1. Long cooldowns — `long_cooldowns.json` — 90-day rolling block, 10 chronic losers, auto-renew WR<35%
2. Loss-streak cooldown — 2+ losses in 7d → 3d block; 3+ in 14d → 7d block
3. Materials≥75 block (Jul 4, expanded to MATERIALS/HEALTHCARE/REIT/TELECOM Jul 8) — pre-emptive exclusion before the sector-first gate below
4. Sector diversity cap — max 2 picks per normalized sector (TFSA basket only)
5. Sector-first gate (`ml_engine.py`, added Jul 4, hysteresis via `gate_engine.py` since Jul 9) — score≥90:
   ENERGY/FINANCIALS pass without ML gate (BANKS is a dead alias — normalizes to FINANCIALS).
   MATERIALS/TELECOM/HEALTHCARE/REIT/CONSUMER blocked regardless of ML.
   All other sectors → ML gate (ml_prob ≥ 20% required).
6. Sharpe guard — Sharpe negative → sizing cut (currently 0.5×, was 12% under DEFENSIVE — scales with regime)

### Pattern Signal Boost (new — June 27)
```python
# Applied last in score_stock() — after all caps
# CONSIDER (+3pts): 3+ day streak + rising velocity
# AVOID (-5pts): falling score + poor PF
# Capped at ±5 — nudges, never overrides fundamentals
```

---

## LONG COOLDOWNS (long_cooldowns.json)

90-day rolling block with auto-renew. Replaces permanent_exclusions.json (no indefinite blocks).
On expiry: rolling WR from last 20 resolved picks. WR < 35% → renew 90 days. WR ≥ 35% → clear.

Current 10 tickers (blocked until 2026-10-02):
F, DXCM, WPM.TO, FM.TO, ABX.TO, AEM.TO, AGI.TO, MDB, MSFT, HOOD

K.TO and NTR.TO removed from block (July 4, 2026 — dropped from July 2 review list).

**SCORE_CAP_74** (capped at 74, not excluded):
F, DXCM, WPM.TO, FM.TO, ABX.TO, AEM.TO, AGI.TO, MDB, MSFT

---

## KNOWN ISSUES (open)

**Fixed since July 4 audit (75 substantive commits — categorized, not exhaustive):**
- ✅ v4.2 pillar rebalance (Aug 15) — momentum cap 35→22, growth/value/safety raised back
  toward pre-V2 levels, bonus capped ±15 — root cause of the 90-100 tier inversion, see
  `strategy_version.py`
- ✅ FHSA/TFSA duplicate-pick dedup unified into shared `pick_utils.py` — several picks
  were silently double-counted or mis-routed across accounts
- ✅ Category routing bug — read the wrong dict key, was always None
- ✅ ML train/holdout split — was leaky (positional), now date-based
- ✅ Evidence Engine — was silently broken since inception (3 compounding bugs), now revived
- ✅ calibration_check qcut crash + ETF ZLB.TO duplicate category bug
- ✅ Insider engine (SEC EDGAR) — 403 fixed (User-Agent), Form4 direction parsing completed,
   now logged as an ML feature
- ✅ Sharpe advisory wired into real position sizing (Phase 2)
- ✅ ticker.js gated at the edge — was allowing unauthorized billing
- ✅ Dashboard: gated-out picks no longer surface as "#1 CONVICTION" or the day's tweet;
   cooldown-flagged tickers filtered from the accounts dashboard
- ✅ ANTHROPIC_API_KEY added — strategist agent live, producing daily research notes
- ✅ NGX expanded to 56 candidate tickers with per-ticker validation clock (was single
   global phase gate)

**Open issues:**
| Priority | Issue | Detail | Fix |
|----------|-------|--------|-----|
| 🔴 HIGH | Rolling Sharpe negative | -0.32, below the 0.3 guard threshold — sizing cut to 0.5× | Guard is firing; needs sustained recovery, not a code fix |
| 🔴 HIGH | 90-100 tier still worst-performing | 43.6% WR / PF 0.85, the exact pattern v4.2 targets | v4.2 shipped Aug 15 — no resolved picks yet, watch OOS |
| ⚠️ MEDIUM | Residual score-tier gap, cause unidentified | Even after modeling every known downstream filter (ML gate, sector gate, materials≥75 block) correctly scoped to when each was actually live, ~12-15pt of the 90-100 tier's WR gap has no confirmed cause. Full explanation logged in `strategy_version.py`'s "DOWNSTREAM GAP INVESTIGATION" note (2026-08-15) — includes a negative result (an earlier gate-based explanation didn't hold up under era-correct re-analysis) so it isn't re-investigated the same way. | See "Per-date code reconstruction — scoped" below |
| ⚠️ MEDIUM | ML retrain coverage gate | Watch for auto-fire | Auto-fires |
| ⚠️ MEDIUM | 60-74 tier PF drifting | Was 1.71-1.92 in July, now 1.72 — stable, keep monitoring | Monitor |
| ℹ️ LOW | BoC feed 0 articles | No recent items in RSS | Not a code issue |
| ℹ️ LOW | Reuters/AP/Investopedia ❌ | GitHub Actions DNS — 12/15 stable ceiling | Architecture decision |
| ℹ️ INFO | Congressional 403 | S3 endpoints gone. api.congress.gov is clean path | Add CONGRESS_API_KEY |

**Per-date code reconstruction — scoped (not started):**
The residual score-tier gap above can't be fully explained from current data. Closing it
would need per-historical-date reconstruction of exactly what the pipeline did, not just
today's code applied retroactively. Scoped into two halves:
- **Reconstructable (tedious, not blocked):** `stock_screener.py` / `ml_engine.py` changed
  22 and 56 times respectively between 2026-06-14 and 2026-08-08 — pinning the exact
  commit active at each historical `signal_date` and replaying `score_stock()` against
  it is mechanical git-log work, just a lot of it.
- **Not reconstructable from git history at all:** the reserve/substitution pool (which
  candidate got swapped in when another was gated out) was never persisted —
  `screener_results.json` is an ephemeral runtime file, never committed. `gate_engine.py`
  hysteresis (day-over-day memory in `gate.decide()`) also isn't captured by point-in-time
  snapshots — it needs sequential replay of state that was never logged either.
- **Recommended path, if this gets picked up:** instrument the pipeline to persist gate
  decisions and reserve-pool composition going forward (small, real code change), then
  revisit with real forward data instead of trying to reconstruct history that was never
  captured. Reconstructing the past is the expensive, possibly-incomplete option; logging
  the future properly is cheap and conclusive.

---

## OPEN WATCH ITEMS

| Item | Status | Notes |
|------|--------|-------|
| Sharpe recovery | 🔴 Not recovering | -0.32 and declining since Jul 4 (was 0.42) — needs real attention, not just time |
| v4.2 OOS read (real) | ⏳ ~Nov 15, 2026 | Day 0 = Aug 15, 2026. #4 (RISK_ON momentum multiplier) deferred until this read |
| v4.2 interim checkpoint | ⏳ ~Oct 15, 2026 | Informal, directional only — NOT a decision point, don't tune anything off it |
| Sharpe rolling-window recovery | ⏳ ~Sep 20-26, 2026 | Mechanical — Jun 22-28 cohort rolls out of the 90d window. If Sharpe doesn't recover by then, that's a genuine new signal, not yet investigated |
| ML retrain | ⏳ Auto-fires on coverage gate | |
| Walk-forward validation | ⏳ Aug 2026 target (README says "60+ days history required" — check if this has quietly become feasible) | |
| NGX FTSE catalyst | ⏳ Sept 2026 | Institutional reclassification window — universe already expanded ahead of it |
| AGI.TO | Check current status | Was "18 picks, 33% WR, approaching exclusion" as of Jul 4 — verify against current `long_cooldowns.json` before assuming still true |

---

## ARCHITECTURE DECISIONS — DO NOT REVISIT

1. **Regime engine: 3 inputs → 1 output.** market + macro + health. Correct as-is.
2. **9/15 news feeds ceiling.** GitHub Actions DNS. Not fixable. 12/15 stable.
3. **NGX 0% WR** — macro reversed during resolution. Thesis failed correctly. Not a bug.
4. **No DCA for NGX** — FX conversion cost. WAIT/WATCH/ENTER framework instead.
5. **No product launch yet** — engine must recover. Target: Sharpe > 1.0 sustained.
6. **Double bake** — intentional. First bake fast (pre-NGX), second includes NGX.
7. **Score ceiling → 0.4 curve** — NOT a hard ceiling at 89. Curve steepening ships.
   Raw 100 → 91. Raw 94 → 88.6. Ordinal ordering preserved. ML gate diagnostic intact.
8. **Pattern boost capped at ±5** — nudges, never overrides fundamentals.
9. **Strategist agent is read-only** — it observes and writes notes. Never touches scores.
10. **Obsidian notes live in repo** — `history/obsidian/`. Mac vault = same files via Git.
11. **No auto-execution** — system generates signals and research. Human decides.
12. **InvestOS never exposes raw engine logic** — BAKED_DATA is the interface layer.

---

## SECURITY

| Item | Status |
|------|--------|
| Hardcoded secrets | ✅ None — all via os.environ / GitHub Secrets |
| trades.csv | ✅ Gitignored |
| ANTHROPIC_API_KEY | ✅ Set — strategist agent producing daily research notes |
| Netlify function auth | ✅ x-investos-key header required |
| ML model cache (.pkl) | ✅ Gitignored |
| __pycache__ | ✅ Gitignored (fixed June 27) |

---

## ETF ENGINE

30 ETFs across 5 categories. Account routing:
- **RRSP** → US-listed ETFs (IRS treaty exemption on dividends)
- **TFSA** → .TO ETFs, sector/thematic OK
- **FHSA** → Conservative Canadian only. Excludes thematic + GLD, TLT, VWO, EEM, XLE, QQQ, VOO

---

## NGX ENGINE

Macro-driven scoring for 30 Nigerian Exchange stocks.
Inputs: Brent (primary), DXY, SPY, VIX, EEM. Resolution: 14 days.
Phase: RESTRICTED (Day 48) — Tier 1 stocks, score ≥80 only.
Entry: WAIT / WATCH / ENTER (not DCA — FX conversion cost).

**FTSE frontier reclassification September 2026 → institutional window opens now.**
GTCO(~₦135), MTNN(~₦820), SEPLAT(~₦11,363) — primary targets for passive inflows.

---

## ACCOUNT CONTEXT

| Account | Purpose | Strategy |
|---------|---------|----------|
| TFSA | Growth + income | Sector ETFs, dividend stocks, selective thematic |
| FHSA | Home purchase savings | Conservative Canadian — XEQT.TO, ZEB.TO, ZAG.TO |
| RRSP | Retirement | US-listed — XLE, VOO, QQQ, GLD, CIBR |
| NGX | Nigerian equity | Via Bamboo Invest — SEPLAT, MTNN, GTCO |

---

## PERFORMANCE HISTORY

| Date | Sharpe | WR 30d | Event |
|------|--------|--------|-------|
| Early 2026 | 1.07 | ~60% | BCE.TO losses begin |
| May 22 | 0.47 | 62.4% | ETF engine v1 launched |
| May 27 | -0.661 | 63.2% | Sharpe guard fires |
| Jun 6 | +0.715 | 63.6% | Guard OFF — all-time high WR |
| Jun 21 | 1.964 | 62.1% | Regime RISK_OFF (war escalation) |
| Jun 22 | 2.034 | 61.6% | Macro CAUTIOUS, recovering |
| Jun 26 | -3.058 | 47.6% | Guard re-engaged — DEFENSIVE 0.25× |
| Jun 27 | -3.056 | 46.8% | First Sharpe micro-recovery |

---

## IMMEDIATE NEXT BUILDS (in order)

```
0. [DONE] ANTHROPIC_API_KEY — strategist agent live since ~July 2026

1. Sharpe recovery — the actual blocker before anything else on this list matters.
   -0.32 and declining since Jul 4 (was 0.42). v4.2 (Aug 15) targets one real
   contributor (90-100 tier PF 0.85) but has zero resolved picks yet — watch,
   don't assume fixed.

2. UI — search overlay (⌘K)
   → ticker search → full research card
   → replaces Lookup tab
   → highest UI value remaining

3. Telegram bot setup (5 min via @BotFather)
   → needed for AllocOS Phase 1

4. AllocOS Phase 1 (personal build)
   → you are user zero
   → Telegram morning brief from BAKED_DATA

5. InvestOS Lens (Chrome extension)
   → standalone, any session

6. NGX waitlist page (Carrd.co)
   → email capture before FTSE catalyst
```

Per Architecture Decision #5, none of 2-6 should ship as a public product before Sharpe
recovers — they can still be *built* (personal use), just not launched.

---

## HOW TO CONTINUE A SESSION

Start with:
> "I'm continuing InvestOS development. Read the README.md and the latest log,
> then tell me system state before we build anything."

The AI should:
1. Read this README fully
2. Read the latest GitHub Actions log if provided
3. State current system state (metrics table above)
4. Confirm what was last built
5. Not re-explain Architecture Decisions
6. Pick up from Immediate Next Builds — in order

---

## SESSION LOG

**Early history (Mar 1 – Jun 21, 2026) — condensed.** ~1,100 commits in this window,
mostly manual "Add files via upload" cycles (bulk re-uploads, not incremental diffs) and
bot-generated daily briefs, before commit-message discipline (`fix:`/`feat:` prefixes)
started Jul 3. Not itemized here — full detail is in `git log`. Milestones worth knowing:

| Date | Change | Notes |
|------|--------|-------|
| Mar 1, 2026 | **Project started.** Core engine bootstrapped essentially whole | `stock_screener.py`, `ml_engine.py`, `risk_engine.py`, `news_analyzer.py`, `run_daily.py`, `daily_run.yml` all first appear same day |
| Mar 1-2 | Rapid upload/delete/re-upload churn | Early bootstrapping via GitHub web UI, not local dev — 382 commits in March alone |
| Mar 2 | `outcome_tracker.py` added | Win rate / outcome tracking begins |
| May 3, 2026 | **Momentum pillar cap raised 20→35 ("V2")** | Based on a flawed self-referential health check (score-vs-price correlation, not future win/loss) — this is the root cause the Aug 15 v4.2 fix reverses. See `strategy_version.py`'s v4.2 note |
| Jun 25-26 | v4.1 rule freeze declared, OOS tracking begins | Day 0 = Jun 26. First disciplined "frozen rule set" commitment in the project |

Detailed log resumes below from Jun 22 onward (dedup/baseline work that preceded the v4.1 freeze).

| Date | Change | File(s) | Notes |
|------|--------|---------|-------|
| Jun 22 | Dedup pass | outcomes_log.json | 2005→1881 entries |
| Jun 22 | SCORE_CAP_74: F, DXCM, WPM.TO | stock_screener.py | |
| Jun 22 | PF baseline locked | pf_baseline.json | |
| Jun 22 | Congressional engine | congressional_engine.py | HTTP 403 = DATA UNAVAILABLE |
| Jun 23 | SCORE_CAP_74: FM.TO added | stock_screener.py | -14.3% worst single loss |
| Jun 25 | PF baseline reset (post-backlog) | pf_baseline.json | Clean baseline |
| Jun 26 | Curve steepening 0.6→0.4 | stock_screener.py | v4.1 OOS Day 0 |
| Jun 26 | ML gate fix in conviction engine | run_daily.py | QCOM false positive fixed |
| Jun 26 | TIH.TO airline fix | ml_engine.py | Industrials→AIRLINES removed |
| Jun 26 | Obsidian vault created | investos-brain repo | ~/Documents/investos-brain |
| Jun 26 | Obsidian daily bridge fixed | run_daily.py | Variable scope: all from brief |
| Jun 26 | history_analyzer.py built | history_analyzer.py | 6 modules, silent daily |
| Jun 26 | strategy_version.py built | strategy_version.py | OOS anchor, 41 params |
| Jun 26 | __pycache__ gitignored | .gitignore | Stops binary conflicts |
| Jun 27 | pattern_agent.py built | pattern_agent.py | 595 lines, closes autonomy loop |
| Jun 27 | pattern_agent.yml workflow | .github/workflows/ | 9:45am ET weekdays |
| Jun 27 | scout_agent upgraded | scout_agent.py | 17→30 ETF sources, RSI 80→75 |
| Jun 27 | pattern_signals wired to screener | stock_screener.py | ±3-5pt boost/penalty |
| Jun 27 | strategist_agent.py built | strategist_agent.py | Claude API, daily research note |
| Jun 27 | strategist_agent.yml workflow | .github/workflows/ | 10:00am ET weekdays |
| Jun 27 | README updated | README.md | Full platform state captured |
| Jul 4 | Sector-first gate + Materials≥75 block | ml_engine.py | ENERGY/FINANCIALS bypass ML gate; MATERIALS/TELECOM/HEALTHCARE/REIT/CONSUMER blocked at score≥90 |
| Jul 8 | Materials≥75 block expanded | ml_engine.py | Now covers MATERIALS/HEALTHCARE/REIT/TELECOM, not materials-only |
| Jul 9 | gate_engine.py built | gate_engine.py | Hysteresis-aware ML gate, replaces flat threshold |
| Jul 23 | ML train/holdout split fixed | ml_engine.py | Was leaky (positional), now date-based by signal_date |
| Aug 6 | score-tier inversion — first pass | stock_screener.py | Flattened rs_adj curve, split SWING category cap |
| Aug 8-9 | FHSA/TFSA dedup unified | pick_utils.py (new) | Several picks were silently double-counted/mis-routed; congressional_engine.py removed (dead), Evidence Engine revived |
| Aug 9 | Insider engine completed | insider_engine.py | SEC EDGAR Form4, 403 fixed, logged as ML feature |
| Aug 10 | Sharpe advisory wired into sizing | ml_engine.py | Phase 2 |
| Aug 15 | v4.2 — pillar rebalance (root cause) | stock_screener.py, strategy_version.py | Momentum cap 35→22, bonus capped ±15 — see strategy_version.py for full root-cause + validation + the downstream-gate investigation's negative result |
| Aug 15 | README refreshed | README.md | Was stale since Jul 4 (6 weeks, 75+ unlogged commits) — this refresh |

---

## CONVERSATION CONTEXT

- "Uncle" = Claude
- Rule versioning: v4.1 ran Jun 26 → Aug 15, 2026. v4.2 (pillar rebalance) started
  Aug 15 — see strategy_version.py, not this doc, for the authoritative version history
- Deployable capital scales with the current risk multiplier (0.5× as of Aug 15) — see
  the live dashboard for actual account figures, not this doc
- Score ceiling decision: curve 0.4 (NOT hard cap at 89) — ordinal ordering preserved.
  Independent of and complementary to v4.2's pillar rebalance — don't conflate the two
- Strategist agent: ANTHROPIC_API_KEY is set, live since ~July 2026
- Obsidian sync: check Git plugin pull interval if notes not appearing on Mac
- AllocOS: next product build after Telegram bot setup — still not started as of Aug 15
- NGX FTSE: September 2026 catalyst — universe already expanded to 56 candidates ahead of it
- Downstream-gate investigation (Aug 15): tested whether ML/sector gates explain the
  score-tier inversion gap. Came back negative under correct era-scoping. Don't
  re-run the same naive "apply today's rules to all history" test — see
  strategy_version.py's DOWNSTREAM GAP INVESTIGATION note for why it's wrong

---

*InvestOS v4.2 — Built session by session. Every line has a reason.*
*NFA · Educational only · Personal use only*
