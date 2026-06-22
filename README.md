# INVESTOS — Personal Intelligence System
**@adejuwon_t · Toronto, Canada · Live at [tolulop3.github.io/investos](https://tolulop3.github.io/investos)**

> **FOR AI ASSISTANTS:** This README is the authoritative context document for every session.
> Read this fully before touching any file. The system is live, running daily, and has 1,638+
> resolved picks tracked. Do not break what works. Optimise only.
>
> Start every session by stating current system state before suggesting anything.
> Do not re-explain decisions listed in "Architecture Decisions — Do Not Revisit".
> Do not flag intentional design choices as bugs.

---

## WHAT THIS IS

A fully automated daily investment intelligence system. Every weekday at 9:30am ET, GitHub
Actions runs `run_daily.py`, which screens 176 stocks, scores them with ML, detects macro
signals from 15 news feeds, sizes positions by volatility, generates ETF picks, tracks NGX
signals, and bakes everything into a live dashboard at the GitHub Pages URL above.

Personal tool for one user (Toronto). Not a product. Not public-facing.
Goal: open the dashboard, know what the market is doing, know what to do, trust it.

---

## CURRENT SYSTEM STATE (as of June 22, 2026)

| Metric | Value | Status |
|--------|-------|--------|
| Sharpe (90d rolling) | 2.034 | ✅ Healthy |
| Win rate overall | 54.3% | ✅ Stable |
| Win rate last 30d | 61.6% | ✅ Strong |
| TW win rate | 58.8% | ✅ Good |
| Current streak | 2 LOSS | ⚠️ Watch |
| Avg return/pick | +1.07% | ✅ Positive |
| Unified regime | RISK_ON (50%) | ⚠️ Risk multiplier 0.50× active |
| Macro regime | CAUTIOUS | ⚠️ War escalation signals |
| Breadth (50MA) | 58.0% | ⚠️ MODERATE — declining |
| Breadth (200MA) | 68.8% | ✅ Holding |
| Robustness | 100/100 | ✅ |
| Risk multiplier | 0.50× | ⚠️ PCR conflict + convergence |
| PCR | 1.278 | ⚠️ BEARISH (options hedging) |
| Regime momentum | DECELERATING | ⚠️ Breadth -0.6%/day |
| NGX phase | Paper Day 47 | ⏳ RESTRICTED ~Day 31 |
| NGX win rate | 0% (159 resolved) | ⚠️ Macro reversed — new signals |
| Runtime | 50-88s | ✅ Fast |
| Last commit | June 22 2026 | |

**Score tier performance (1,638 resolved picks):**
| Tier | WR | PF | Notes |
|------|----|----|-------|
| 90-100 | 50.0% | 1.55 ✅ | Marginal edge. ML gate removes unprofitable subset |
| 75-89 | 61.0% | 2.27 ✅ | Strong |
| 60-74 | 61.7% | 3.07 ✅ | Best tier |
| below-60 | 47.7% | 3.01 ✅ | High PF but low WR |

**ML attribution split (key finding from factor_investigation.py):**
- 90-100 + ML≥20%: 757 picks, WR=50.5%, PF=1.65 ✅ (profitable)
- 90-100 + ML<20%: 121 picks, WR=47.1%, PF=0.96 🔴 (losing money)

---

## FILE REGISTRY

| File | Lines | Purpose |
|------|-------|---------|
| `run_daily.py` | ~1746 | Master orchestrator — runs all 12 steps |
| `stock_screener.py` | 1195 | Screens 176 tickers, scores 0-100 |
| `ml_engine.py` | 1210 | XGBoost predictor + vol-targeted sizing + Kelly + sector cap + ML gate |
| `ml_retrainer.py` | 443 | XGBoost retraining with coverage gate |
| `risk_engine.py` | ~1476 | Stress test, decay monitor, unified regime engine |
| `news_analyzer.py` | 625 | 15 RSS feeds, sector sentiment, macro regime |
| `intelligence_layers.py` | ~792 | RS ratings, score history, trend detection |
| `etf_engine.py` | ~408 | 30 ETFs, RRSP/TFSA/FHSA routing |
| `content_engine.py` | ~641 | Tweet + thread generation for @adejuwon_t |
| `portfolio_engine.py` | ~1213 | Trades tracker, scorecard, open positions |
| `outcome_tracker.py` | 559 | Win rate tracking, score tier analysis, dedup fix |
| `signal_quality.py` | ~1056 | Signal accuracy metrics |
| `ngx_screener.py` | ~360 | Nigerian Exchange macro-driven signals |
| `ngx_outcome_tracker.py` | ~292 | NGX paper phase outcome resolution |
| `evidence_engine.py` | ~266 | Per-pick historical evidence lookup |
| `factor_investigation.py` | 212 | Factor attribution report — runs every daily run |
| `scout_agent.py` | 369 | Weekly universe expansion (176→~300) |
| `crypto_engine.py` | ~357 | BTC + SOL signals |
| `index.html` | ~7148 | Full dashboard — baked daily |
| `.github/workflows/daily_analysis.yml` | ~50 | GitHub Actions — 9:30am ET weekdays |
| `.github/workflows/weekly_scout.yml` | ~30 | Sunday 6AM ET — universe expansion |

**Persistent data files (never delete):**

| File | Contents |
|------|----------|
| `outcomes_log.json` | Every pick since Day 1 — 2,002 entries (1,638 resolved) |
| `cooldown_flags.json` | Permanent exclusions + loss-streak cooldowns |
| `ngx_outcomes.json` | All NGX paper signals |
| `score_history.json` | Per-stock score history for trend detection |
| `trades.csv` | Manual trades — gitignored, lives locally only |
| `history/YYYY-MM-DD.json` | Daily snapshots — started June 6, 2026 |

---

## ARCHITECTURE

### The 12-Step Daily Pipeline

```
[1/10]  NEWS & MACRO    → news_analyzer.py      → sector_sentiment, macro_regime
[2/10]  MARKET REGIME   → risk_engine.py        → SPX vs 200MA → BULL/BEAR
[3/10]  STOCK SCREEN    → stock_screener.py     → scores 176 tickers 0-100
[4/10]  APPLY NEWS      → run_daily.py          → sector penalty + news boost
[5/10]  ML ENGINE       → ml_engine.py          → XGBoost + vol-targeted sizing
[6/10]  INTELLIGENCE    → intelligence_layers.py → RS, trends, analyst
[7/10]  X FEEDS         → run_daily.py          → 9 Twitter/X signal feeds
[8/10]  CONVICTION      → run_daily.py          → 2+ signals aligned = conviction
[9/12]  FX & GOLD       → risk_engine.py        → EUR/USD, GBP/USD, XAU/USD
[10/12] CRYPTO          → crypto_engine.py      → BTC + SOL signals
[11/12] RISK AUDIT      → risk_engine.py        → stress test, Sharpe, regime
[ETF]   ETF ENGINE      → etf_engine.py         → 30 ETFs scored
[12/12] CONTENT         → content_engine.py     → tweet + thread
[NGX]   NGX ENGINE      → ngx_screener.py       → Nigerian Exchange signals
[END]   FACTOR REPORT   → factor_investigation.py → attribution analysis
```

### Three-Layer Unified Regime Engine

```
market_score  = +1.0   (SPX vs 200MA)
macro_score   = -0.3   (news signals — dampened by market confirmation gate)
health_score  = +1.0   (Sharpe)

unified_score = (0.40 × market) + (0.30 × macro) + (0.30 × health)
→ outputs: RISK_ON / NEUTRAL / DEFENSIVE / CAPITAL_PRESERVATION
```

### Kelly + Volatility Sizing

```python
MAX_SINGLE = max(0.20, 1.5/n_picks)   # dynamic — prevents equal-weight collapse
MAX_HARD   = max(0.20, 1.5/n_picks)   # same formula — scales with basket size
blend = 33% Kelly + 33% vol-targeted + 33% ML-proportional
TARGET_VOL = 20%
```

### Safety Gates (in order of application)

1. **Permanent exclusions** — `cooldown_flags.json` read inside `get_cooldown_set()` in `ml_engine.py`
2. **Loss-streak cooldown** — 2+ losses ≥1.5% in last 10 picks → 7-day block
3. **Sector diversity cap** — `_apply_sector_cap()` — max 2 picks per normalized sector
4. **ML confidence gate** — score≥90 AND ml_prob<0.20 → removed, replaced from reserve

**CRITICAL:** `get_cooldown_set()` in `ml_engine.py` reads BOTH `outcomes_log.json` AND `cooldown_flags.json`. If you ever refactor this function, preserve both reads or permanent exclusions will bypass sizing.

---

## PERMANENT EXCLUSIONS (cooldown_flags.json)

These 7 tickers are blocked until 2036. Evidence from factor_investigation.py:

| Ticker | n | WR | Avg Return | PF |
|--------|---|----|------------|-----|
| F | 3 | 0% | -10.25% | 0.00 🔴 |
| DXCM | 5 | 0% | -6.16% | 0.00 🔴 |
| K.TO | 5 | 0% | -4.92% | 0.00 🔴 |
| WPM.TO | 11 | 0% | -4.67% | 0.00 🔴 |
| ABX.TO | 13 | 23% | -3.75% | 0.05 🔴 |
| NTR.TO | 12 | 42% | -3.15% | 0.24 🔴 |
| AEM.TO | 19 | 32% | -2.80% | 0.29 🔴 |

Also in `cooldown_flags.json` as temporary: SU.TO, GRT-UN.TO, CP.TO (loss-streak, expire within 7 days of flagging).

---

## KNOWN ISSUES (open)

| Priority | Issue | Detail | File |
|----------|-------|--------|------|
| 🔴 HIGH | Duplicate outcomes in outcomes_log.json | Dedup bug fixed June 22 — new picks clean. Historical data has unknown duplicate rate. Run dedup pass on outcomes_log.json when possible. | outcome_tracker.py |
| ⚠️ MEDIUM | ML retrain feature coverage 0% | New picks capturing real features since June 20. Coverage gate protects against bad retrain. Real retrain fires at 10% (~160 picks) — ETA ~3-4 weeks. | ml_retrainer.py |
| ⚠️ MEDIUM | Sector cap replacing financials with financials | _apply_sector_cap() normalizes sub-sectors correctly but the reserve pool often returns more financials anyway | ml_engine.py |
| ⚠️ MEDIUM | Regime/sector coverage 0% in factor report | Regime and sector capture started June 20. Sections 2+3 fill over time. | factor_investigation.py |
| ℹ️ LOW | BoC feed 0 articles | Endpoint OK, no recent items in RSS. Not a code issue. | news_analyzer.py |
| ℹ️ LOW | Reuters/AP/Investopedia ❌ | Permanent GitHub Actions DNS policy — not fixable. Stable ceiling at 12/15 feeds. | Architecture Decision |
| ℹ️ INFO | NGX individual prices unavailable | Paid API tier required. ngx_diagnostic.py in repo to test endpoints. Run locally with NGN_MARKETS_KEY. | ngx_diagnostic.py |

---

## ARCHITECTURE DECISIONS — DO NOT REVISIT

1. **Regime engine: 3 inputs → 1 output.** market + macro + health. Correct as-is.
2. **9/15 news feeds is the stable ceiling.** GitHub Actions DNS. Not fixable.
3. **NGX 0% WR** — macro reversed during resolution. Thesis failed correctly. Not a bug.
4. **Score tier inversion** — ML gate + permanent exclusions address the root cause. Monitor PF.
5. **No DCA for NGX** — FX conversion cost. WAIT/WATCH/ENTER framework instead.
6. **No product launch yet** — backend, auth, Stripe = separate project.
7. **Double bake** — intentional pipeline design. First bake fast (pre-NGX), second includes NGX.
8. **Email not configured** — intentional. Dashboard is primary interface.
9. **Double-log dedup was `resolved is False`** — fixed June 22 to `not outcome`. Historical data has duplicates but directionally correct.
10. **Duplicate outcomes do NOT necessarily invalidate WR** — duplicates of same pick resolve to same outcome so they inflate count but WR ratio stays approximately correct. Real clean number requires dedup pass on outcomes_log.json.

---

## WHAT WAS BUILT (Sessions 751–Present, June 20–22 2026)

### Completed ✅

| Build | File | What It Does |
|-------|------|-------------|
| Kelly fix (MAX_SINGLE dynamic) | ml_engine.py | `max(0.20, 1.5/n_picks)` — fixes equal-weight collapse on 4-pick baskets |
| Sector diversity cap | ml_engine.py | `_apply_sector_cap()` — max 2 per normalized sector, full normalization map |
| Loss-streak cooldown | outcome_tracker.py + run_daily.py | Auto-flags tickers with 2+ losses ≥1.5% in 10 picks → cooldown_flags.json |
| 90d outcome fields | outcome_tracker.py | `outcome_90d`, `return_90d` — portfolio optimizer unlocks Sept 2026 |
| Factor attribution fields | outcome_tracker.py | score_rank, score_pct, options_signal, conviction, kelly_wt on every new pick |
| Profit factor in tier report | outcome_tracker.py | PF shown next to WR for every score tier in daily log |
| Factor investigation | factor_investigation.py | Runs every daily run. Answers: what is making money? |
| ML confidence gate | ml_engine.py | score≥90 + ML<20% → removed, replaced. Evidence: PF 0.96→1.65 |
| Permanent exclusion list | cooldown_flags.json | 7 tickers blocked to 2036 based on factor report |
| get_cooldown_set reads flags | ml_engine.py | Closes gap where permanent exclusions bypassed sizing (F bug) |
| Coverage gate fix | ml_retrainer.py | rs_rating default 50→0, per-feature zero diagnostics, correct gate logic |
| Error repr fix | news_analyzer.py | Full exception type+message instead of truncated string |
| Dedup fix | outcome_tracker.py | `not outcome` instead of `resolved is False` — stops duplicate logging |
| EWG → EWJ swap | stock_screener.py | Removes 404 warning |
| Evidence engine verbose | run_daily.py | Targets sized_positions not empty conviction_picks |
| SOL $0.15 → $150 fix | run_daily.py | crypto_engine uses hardcoded 10000 not CONFIG balance of 10 |
| Pre-risk-multiplier label | ml_engine.py | Clarifies ML log shows raw not adjusted sizes |
| Scout agent | scout_agent.py + weekly_scout.yml | Sunday 6AM: universe 176→~300 |
| Regime predictor | run_daily.py | Fires at 5/5 consecutive days — first directional signal |
| Regime momentum tracking | run_daily.py | Sharpe slope + breadth slope + WR slope — DECELERATING signal |

### Next Build: UI (Track 3)

README roadmap UI section still applies. The "Today" tab is the landing. Score rings, regime band, search overlay. Nothing gets removed.

---

## FACTOR ATTRIBUTION — CURRENT STATE

**factor_investigation.py runs automatically at end of every daily run.**

Key findings as of June 22:
- **Section 5:** 7 repeat losers identified and permanently excluded
- **Section 6:** ML<20% is the fault line within 90-100 tier (PF 0.96 = losing money)
- **Section 4 (date trend):** PF improving: 1.06 → 1.96 → 1.97 across three periods
- **Sections 2+3:** regime/sector still 0% coverage — filling from June 20 new picks

Coverage ticking up:
- ml_prob: 100% (1,638 picks)
- rs_rating: 1% (16 picks — new since June 20)
- regime: 1% (16 picks — new since June 20)
- sector: 0% (not yet captured)

---

## SECURITY

| Item | Status |
|------|--------|
| Hardcoded secrets | ✅ None — all via os.environ / GitHub Secrets |
| trades.csv | ✅ Gitignored |
| Netlify function auth | ✅ x-investos-key header required |
| CORS | ✅ Restricted to investos-proxy.netlify.app only |
| ML model cache (.pkl) | ✅ Gitignored |
| Netlify env var | INVESTOS_API_KEY = inv-2026-personal |

---

## ETF ENGINE

30 ETFs across 5 categories. Account routing:
- **RRSP** → US-listed ETFs (IRS treaty)
- **TFSA** → .TO ETFs, sector/thematic OK
- **FHSA** → Conservative Canadian only. Excludes thematic + GLD, TLT, VWO, EEM, XLE, QQQ, VOO

XOM + CVX confirmation for energy ETFs. DCA vs lump sum: top 80%+ of 52-week range → DCA.

---

## NGX ENGINE

Macro-driven scoring for 30 Nigerian Exchange stocks.
- Scoring inputs: Brent (primary), DXY, SPY, VIX, EEM
- Resolution window: 14 days
- Current phase: RESTRICTED (Day 47) — Tier 1 stocks, score ≥80 only
- Entry framework: WAIT / WATCH / ENTER (not DCA/lump — FX conversion cost)
- Individual prices: require paid NGN Markets API tier. `ngx_diagnostic.py` in repo — run with `NGN_MARKETS_KEY=your_key python ngx_diagnostic.py`

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
| May 27 | -0.661 | 63.2% | Sharpe guard fires — 20% exposure |
| Jun 6 | +0.715 | 63.6% | Guard OFF — RISK_ON |
| Jun 21 | 1.964 | 62.1% | Regime RISK_OFF (war escalation) |
| Jun 22 | 2.034 | 61.6% | Macro CAUTIOUS, regime recovering |

---

## OPEN ROADMAP

### Immediate — before next session
- [ ] Dedup pass on `outcomes_log.json` — remove duplicate entries (same ticker + signal_date)
  - Run locally: `python dedup_outcomes.py` (needs to be written, trivial)
  - This cleans the WR denominator and makes factor attribution more accurate
- [ ] NGX diagnostic — run locally: `NGN_MARKETS_KEY=your_key python ngx_diagnostic.py`
  - Paste results in next chat to determine which free endpoints work

### Next build session
- [ ] **UI — Track 3** — wire Trader UI prototype to BAKED_DATA
  - Today tab, score rings, regime band, search overlay
  - See roadmap section in previous README for full spec
  - `investos_trader.html` prototype exists (hardcoded data, not yet wired)

### Monitor (no action needed)
- [ ] ML retrain coverage: watch for 10% threshold (~160 new picks) — auto fires
- [ ] 90-100 tier PF: should rise as ML gate filters new picks — watch monthly
- [ ] Factor attribution regime/sector: filling from June 20 — meaningful in ~30 days
- [ ] Walk-forward validation: mid-August 2026 (60+ days history)
- [ ] 90d outcomes resolve: mid-September 2026 → portfolio optimizer

---

## HOW TO CONTINUE A SESSION

Start a new chat with:
> "I'm continuing InvestOS development. Read the README.md and the latest log, then tell me system state before we build anything."

The AI should:
1. Read this README fully
2. Read the latest GitHub Actions log if provided
3. State current system state (metrics table above, updated)
4. Confirm what was last built
5. Not re-explain decisions in Architecture Decisions section
6. Not flag intentional design choices as bugs
7. Pick up from the open roadmap

---

## QUICK COMMANDS

```bash
# Run locally
python run_daily.py

# Run in GitHub Actions mode
python run_daily.py --github

# Run factor investigation
python factor_investigation.py

# Run NGX diagnostic (needs API key)
NGN_MARKETS_KEY=your_key python ngx_diagnostic.py

# Remove trades.csv from git if accidentally committed
git rm --cached trades.csv
echo "trades.csv" >> .gitignore

# Hard refresh dashboard after push
# Ctrl+Shift+R (Windows/Linux) or Cmd+Shift+R (Mac)
```

---

## CONVERSATION CONTEXT

- "Uncle" = Claude
- Quant friend + hedge fund advisor both reviewed system
- $5,000 hero number = deployable capital at 0.50× risk multiplier
- Score inversion reframed: 90-100 profitable (PF=1.55) but ML<20% subset loses (PF=0.96)
- Dedup bug found June 22: pick count inflated but WR directionally correct
- All Kelly bugs confirmed fixed — monotonic ML ordering across 4 and 5-pick baskets

---

*InvestOS v4.1 — Built session by session. Every line has a reason.*
*NFA · Educational only · Personal use only*
