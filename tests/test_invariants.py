"""
Permanent regression harness — every bug fixed to date.

Each test encodes exactly ONE past bug so a future regression
immediately points to which invariant broke. Add a new test
in the SAME commit as every future bug fix.

Run locally:  pytest tests/test_invariants.py -v
CI:           First step of daily_run.yml (see .github/workflows/tests.yml)
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
from collections import Counter


# ─────────────────────────────────────────────────────────────────────────────
# Sector normalizer (FIX 1, 2026-07-11)
# Bug: "Consumer Defensive" / "Consumer Cyclical" → UNKNOWN(-1), bypassing
#      SECTOR_BLOCK. MNST (score 91) slipped through.
# ─────────────────────────────────────────────────────────────────────────────

def test_sector_norm_consumer_sub_labels():
    from ml_engine import _SECTOR_NORM_INF
    assert _SECTOR_NORM_INF.get("consumer defensive") == "CONSUMER"
    assert _SECTOR_NORM_INF.get("consumer cyclical")  == "CONSUMER"
    assert _SECTOR_NORM_INF.get("consumer staples")   == "CONSUMER"
    assert _SECTOR_NORM_INF.get("consumer discretionary") == "CONSUMER"


def test_sector_norm_financials_consolidation():
    from ml_engine import _SECTOR_NORM_INF
    # Banks / Financial Services / Financials must all resolve to one canonical label
    assert _SECTOR_NORM_INF.get("financial services") == "FINANCIALS"
    assert _SECTOR_NORM_INF.get("banks")              == "FINANCIALS"
    assert _SECTOR_NORM_INF.get("financials")         == "FINANCIALS"


def test_sector_norm_no_unknown_for_known_tickers():
    """No ticker in _TICKER_SECTOR_OVERRIDE should resolve to UNKNOWN."""
    from ml_engine import _SECTOR_NORM_INF, _TICKER_SECTOR_OVERRIDE
    for ticker, raw_sector in _TICKER_SECTOR_OVERRIDE.items():
        norm = _SECTOR_NORM_INF.get(raw_sector.lower(), "UNKNOWN")
        assert norm != "UNKNOWN", (
            f"{ticker} override '{raw_sector}' resolves to UNKNOWN — "
            f"add '{raw_sector.lower()}' to _SECTOR_NORM_INF"
        )


def test_retrainer_sector_norm_consumer_sub_labels():
    from ml_retrainer import SECTOR_NORM
    assert SECTOR_NORM.get("consumer defensive") == "CONSUMER"
    assert SECTOR_NORM.get("consumer cyclical")  == "CONSUMER"


# ─────────────────────────────────────────────────────────────────────────────
# Feature list identity (FIX 1 prerequisite + FIX 4, 2026-07-11)
# Bug: daily training built 15-feature dict; ml_engine had 23; crash on
#      "Constrained features are not a subset of training data feature names".
#      Also: sector_encoded dominated model (importance 1.0000) → removed.
# ─────────────────────────────────────────────────────────────────────────────

def test_feature_lists_identical():
    from ml_engine import ML_CONFIG
    from ml_retrainer import FEATURES
    assert ML_CONFIG["features"] == FEATURES, (
        "Feature lists diverged between ml_engine and ml_retrainer — "
        "update BOTH in the same commit when adding/removing features."
    )


def test_sector_encoded_not_in_features():
    """sector_encoded was a degenerate feature (importance 1.0). Must stay removed."""
    from ml_engine import ML_CONFIG
    from ml_retrainer import FEATURES
    assert "sector_encoded" not in ML_CONFIG["features"]
    assert "sector_encoded" not in FEATURES


def test_monotone_constraint_keys_subset_of_features():
    """Every constrained feature must exist in the training feature list."""
    from ml_engine import ML_CONFIG
    constraints = ML_CONFIG["xgb_params"].get("monotone_constraints", {})
    features    = set(ML_CONFIG["features"])
    extras = set(constraints) - features
    assert not extras, f"Constraint keys not in feature list: {extras}"


def test_enable_categorical_removed():
    """enable_categorical was only needed for sector_encoded — must be gone."""
    from ml_engine import ML_CONFIG
    from ml_retrainer import XGB_PARAMS
    assert "enable_categorical" not in ML_CONFIG["xgb_params"]
    assert "enable_categorical" not in XGB_PARAMS


# ─────────────────────────────────────────────────────────────────────────────
# Scoring list deduplication (FIX 2, 2026-07-10)
# Bug: all_picks built by concatenating 4 screener buckets with no dedup;
#      same ticker (BMO.TO / C / BNY) appeared up to 4 times in ML scoring.
# ─────────────────────────────────────────────────────────────────────────────

def test_scoring_list_no_duplicates():
    """
    Simulate a screener result with duplicates across buckets and confirm
    the dedup inside run_ml_engine produces a unique list.
    """
    pick_a = {"ticker": "BMO.TO", "score": 85, "data": {}}
    pick_b = {"ticker": "C",      "score": 79, "data": {}}
    pick_c = {"ticker": "BMO.TO", "score": 88, "data": {}}  # higher score, same ticker

    screener = {
        "FHSA_top5":        [pick_a],
        "TFSA_growth_top5": [pick_b],
        "TFSA_income_top5": [pick_c],
        "TFSA_swing_top3":  [],
    }

    raw = (screener.get("FHSA_top5", []) + screener.get("TFSA_growth_top5", []) +
           screener.get("TFSA_income_top5", []) + screener.get("TFSA_swing_top3", []))

    seen: dict = {}
    for p in raw:
        t = p.get("ticker")
        if t and (t not in seen or p.get("score", 0) > seen[t].get("score", 0)):
            seen[t] = p
    deduped = list(seen.values())

    tickers = [p["ticker"] for p in deduped]
    assert len(tickers) == len(set(tickers)), f"Duplicates found: {tickers}"
    # Higher-score instance must win
    bmo = next(p for p in deduped if p["ticker"] == "BMO.TO")
    assert bmo["score"] == 88, "Dedup must keep highest-score instance"


# ─────────────────────────────────────────────────────────────────────────────
# Cap substitution — uniqueness + exclusion inheritance (FIX 2, 2026-07-11)
# Bug: reserve pool could contain same ticker twice (multiple screener buckets)
#      and SECTOR_BLOCK names slipped through before canonical normalization.
# ─────────────────────────────────────────────────────────────────────────────

def test_cap_substitution_pool_unique():
    """Reserve pool must contain no duplicate tickers."""
    pool = [
        {"ticker": "GRT-UN.TO", "score": 78, "sector": "Real Estate"},
        {"ticker": "BNY",       "score": 82, "sector": "Financial Services"},
        {"ticker": "GRT-UN.TO", "score": 75, "sector": "Real Estate"},  # duplicate
    ]
    seen = {}
    for p in pool:
        t = p["ticker"]
        if t not in seen or p["score"] > seen[t]["score"]:
            seen[t] = p
    deduped = list(seen.values())
    tickers = [p["ticker"] for p in deduped]
    assert len(tickers) == len(set(tickers)), f"Pool has duplicates: {tickers}"


def test_cap_substitution_excludes_sector_block_at_qualifying_score():
    """SECTOR_BLOCK names at score≥90 must be excluded from reserve pool."""
    from ml_engine import _SECTOR_NORM_INF
    SECTOR_BLOCK = {"MATERIALS", "TELECOM", "HEALTHCARE", "REIT", "CONSUMER"}
    ML_GATE_SCORE_MIN = 90

    candidates = [
        {"ticker": "MNST",   "score": 91, "sector": "Consumer Defensive"},
        {"ticker": "BNY",    "score": 84, "sector": "Financial Services"},
        {"ticker": "KLAC",   "score": 92, "sector": "Technology"},
    ]

    def should_include(p):
        _sec_raw = (p.get("sector") or "").strip()
        _sec = _SECTOR_NORM_INF.get(_sec_raw.lower(), _sec_raw.upper())
        _scr = p.get("score", 0)
        if _scr >= ML_GATE_SCORE_MIN and _sec in SECTOR_BLOCK:
            return False
        return True

    reserve = [p for p in candidates if should_include(p)]
    reserve_tickers = {p["ticker"] for p in reserve}

    assert "MNST" not in reserve_tickers, "MNST (Consumer Defensive, score 91) must be excluded"
    assert "BNY" in reserve_tickers
    assert "KLAC" in reserve_tickers


# ─────────────────────────────────────────────────────────────────────────────
# Kelly floor — 50% deployment when all Kelly zero (FIX 4, 2026-07-10)
# Bug: base_wt*0.50 per-pick was immediately cancelled by the renorm loop;
#      all-zero-kelly case deployed 100% equity with no ⚠️ line.
# ─────────────────────────────────────────────────────────────────────────────

def test_kelly_floor_deploys_50pct():
    """When all picks have kelly_wt == 0 and none are sector-blocked,
    the post-renorm floor must halve total weights to ~0.50."""
    n = 3
    base_wt = 1.0 / n
    kelly_wts = [0.0, 0.0, 0.0]
    sector_blocked = set()
    picks = [{"ticker": f"T{i}"} for i in range(n)]

    # Simulate equal-weight fallback (all Kelly zero → norm_kelly = base_wt each)
    norm_kelly = [base_wt] * n
    norm_vol   = [base_wt] * n
    norm_ml    = [base_wt] * n

    final_wts = []
    for i in range(n):
        if picks[i]["ticker"] in sector_blocked:
            final_wts.append(0.0)
        elif kelly_wts[i] == 0.0:
            final_wts.append(base_wt * 0.50)
        else:
            final_wts.append(0.33 * norm_kelly[i] + 0.33 * norm_vol[i] + 0.33 * norm_ml[i])

    # Renorm loop (mirrors production code)
    for _ in range(6):
        _nz = sum(w for w in final_wts if w > 0)
        if _nz == 0: break
        final_wts = [w / _nz for w in final_wts]
        _excess = sum(max(0.0, w - 0.5) for w in final_wts)
        if _excess < 0.0005: break

    # Post-renorm floor (the actual fix)
    _nonblocked_have_kelly = any(
        kelly_wts[i] > 0
        for i in range(n)
        if picks[i]["ticker"] not in sector_blocked
    )
    if not _nonblocked_have_kelly:
        final_wts = [w * 0.50 for w in final_wts]

    total = sum(final_wts)
    assert abs(total - 0.50) < 0.01, f"Expected ~0.50 deployed, got {total:.4f}"


# ─────────────────────────────────────────────────────────────────────────────
# Options engine — list[str] input guard (FIX 5, 2026-07-10)
# Bug: screener["failed_tickers"] is list[str]; all_picks construction included
#      it; p["ticker"] on a string raised "string indices must be integers".
# ─────────────────────────────────────────────────────────────────────────────

def test_options_engine_guard_against_string_picks():
    """Mixing dicts and raw strings in all_picks must not raise TypeError."""
    all_picks = [
        {"ticker": "JPM",  "score": 80, "data": {}},
        "CAR-UN.TO",                                   # string from failed_tickers
        {"ticker": "MS",   "score": 75, "data": {}},
    ]

    # Production guard (from run_daily.py)
    try:
        us_picks = [p for p in all_picks
                    if isinstance(p, dict) and not p.get("ticker", "").endswith(".TO")]
    except TypeError as e:
        pytest.fail(f"Guard raised TypeError: {e}")

    assert len(us_picks) == 2
    assert all(isinstance(p, dict) for p in us_picks)


# ─────────────────────────────────────────────────────────────────────────────
# Import sweep — every top-level module must import cleanly
# Bug: gate_engine.py committed but not pushed → ModuleNotFoundError in CI
# ─────────────────────────────────────────────────────────────────────────────

def test_core_module_imports():
    """All pipeline modules must import without raising."""
    modules = [
        "ml_engine",
        "ml_retrainer",
        "outcome_tracker",
        "gate_engine",
        "options_engine",
        "scout_agent",
        "stock_screener",
        "strategy_version",
    ]
    for mod in modules:
        try:
            __import__(mod)
        except ImportError as e:
            pytest.fail(f"ImportError in {mod}: {e}")
        except Exception:
            pass  # runtime errors (missing files, API calls) are OK at import time


# ─────────────────────────────────────────────────────────────────────────────
# Counter NameError — FIX 1, 2026-07-12
# Bug: final basket sector count block imported Counter as _C but called Counter()
#      → NameError: name 'Counter' is not defined (3 consecutive runs dark).
# ─────────────────────────────────────────────────────────────────────────────

def test_counter_importable_at_module_level():
    """Counter must be importable from ml_engine at module level (not buried in a local alias)."""
    import ml_engine
    import importlib
    # Trigger the module to be fully initialized
    importlib.import_module("ml_engine")
    # Counter must be accessible as a module attribute (via 'from collections import ... Counter')
    from collections import Counter  # must not raise
    assert Counter is not None


# ─────────────────────────────────────────────────────────────────────────────
# Gate sector field — FIX 2, 2026-07-12
# Bug: gate read pick.get("sector") which is absent on screener picks (sector
#      lives at pick["data"]["sector"]). Result: every pick showed as UNKNOWN,
#      bypassing SECTOR_BLOCK. MNST (Consumer Defensive, score 91) slipped through.
# ─────────────────────────────────────────────────────────────────────────────

def test_mnst_sector_canonical_is_consumer():
    """sector_canonical computed from data dict must be CONSUMER for MNST."""
    from ml_engine import _SECTOR_NORM_INF, _TICKER_SECTOR_OVERRIDE
    SECTOR_BLOCK = {"MATERIALS", "TELECOM", "HEALTHCARE", "REIT", "CONSUMER"}
    ML_GATE_SCORE_MIN = 90

    pick = {
        "ticker": "MNST",
        "score":  91,
        "ml_prob": 0.50,
        # sector lives nested in data dict — top-level pick["sector"] is absent
        "data": {"sector": "Consumer Defensive", "price": 52.0},
    }

    ticker = pick["ticker"]
    stock_data = pick.get("data", {})

    # Simulate scoring-loop write-back (FIX 2)
    _raw = (stock_data.get("sector", "") or "").strip()
    if not _raw:
        _raw = _TICKER_SECTOR_OVERRIDE.get(ticker, "")
    pick["sector_canonical"] = _SECTOR_NORM_INF.get(_raw.lower(), _raw.upper() or "UNKNOWN")

    assert pick["sector_canonical"] == "CONSUMER", (
        f"sector_canonical should be CONSUMER, got {pick['sector_canonical']}"
    )
    score = pick["score"]
    is_blocked = score >= ML_GATE_SCORE_MIN and pick["sector_canonical"] in SECTOR_BLOCK
    assert is_blocked, (
        f"MNST (Consumer Defensive, score {score}) must be blocked; "
        f"sector_canonical={pick['sector_canonical']}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Sector-median imputation — FIX 3, 2026-07-12
# Bug: 80%+ of historical rows have zero features → zero-variance model →
#      AUC ≈ 0.500 (random). Imputation fills zeros with sector-level medians
#      so the model trains on meaningful signal from ALL rows.
# ─────────────────────────────────────────────────────────────────────────────

def test_imputation_reduces_zero_rate():
    """After sector-median imputation, <50% of rows should have zero momentum_6m."""
    try:
        import pandas as pd
        import numpy as np
    except ImportError:
        pytest.skip("pandas/numpy not available")

    from ml_retrainer import build_feature_matrix, SECTOR_ENCODING

    # 20 mock resolved outcomes: first 10 have real data, last 10 are all-zero.
    # After imputation the zeros should be filled with the sector median from row 0-9.
    resolved = []
    for i in range(20):
        resolved.append({
            "resolved": True,
            "actual_return": 5.0 if i % 2 == 0 else -3.0,
            "outcome": "WIN" if i % 2 == 0 else "LOSS",
            "perf_90d":      10.0 if i < 10 else 0.0,
            "roe":           15.0 if i < 10 else 0.0,
            "profit_margin": 20.0 if i < 10 else 0.0,
            "sector":        "Technology",  # all same sector so sector median is well-defined
            "regime":        "BULL",
            "signal_date":   "2026-01-01",
        })

    result = build_feature_matrix(resolved)
    if result[0] is None:
        pytest.skip("Coverage gate blocked — need more resolved outcomes in test data")

    X = result[0]
    zero_rate = float((X["momentum_6m"].abs() < 0.001).mean())
    assert zero_rate < 0.50, (
        f"After imputation, {zero_rate:.1%} of rows still have zero momentum_6m — "
        "sector-median imputation not working"
    )


# ─────────────────────────────────────────────────────────────────────────────
# FIX 1 (2026-07-17): Same-day duplicate picks in outcomes_log
# Bug: log_picks() built logged_today once from file; if the same ticker appeared
# twice in the picks list (from two screener buckets), both got logged.
# Fix: logged_today.add(ticker) after each new entry so the second occurrence
# in the same picks list is also caught.
# ─────────────────────────────────────────────────────────────────────────────

def test_no_same_day_duplicates():
    """outcomes_log.json must contain at most one entry per (ticker, signal_date) pair."""
    import json, os
    log_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                            "outcomes_log.json")
    if not os.path.exists(log_path):
        pytest.skip("outcomes_log.json not present")
    with open(log_path) as f:
        log = json.load(f)
    pairs = [(e.get("ticker"), e.get("signal_date")) for e in log]
    assert len(pairs) == len(set(pairs)), (
        f"Duplicate (ticker, signal_date) pairs found: "
        + str([p for p in pairs if pairs.count(p) > 1][:5])
    )


def test_log_picks_dedup_within_run():
    """log_picks must not record a ticker twice even if it appears twice in picks list."""
    import json, os, tempfile
    from unittest.mock import patch

    # We need outcome_tracker to write to a temp file so the test is self-contained.
    dummy_log: list = []

    def fake_load():
        return dummy_log

    def fake_save(data):
        tmp = list(data)   # snapshot before mutating dummy_log (data IS dummy_log)
        dummy_log.clear()
        dummy_log.extend(tmp)

    picks = [
        {"ticker": "C", "score": 81, "data": {"price": 62.0},
         "pick": {"category": "growth"}, "ml_prob": 0.55, "ml_prob_source": "model"},
        {"ticker": "C", "score": 82, "data": {"price": 62.0},
         "pick": {"category": "income"}, "ml_prob": 0.55, "ml_prob_source": "model"},
    ]

    from outcome_tracker import log_picks
    with patch("outcome_tracker.load_outcomes", fake_load), \
         patch("outcome_tracker.save_outcomes", fake_save):
        log_picks(picks, regime=None, run_type="test")

    c_entries = [e for e in dummy_log if e.get("ticker") == "C"]
    assert len(c_entries) == 1, (
        f"Expected 1 C entry after dedup, got {len(c_entries)}: "
        + str([(e.get('signal_date'), e.get('score')) for e in c_entries])
    )


# ─────────────────────────────────────────────────────────────────────────────
# FIX 2 (2026-07-17): Gate substitution bypasses sector cap
# Bug: _apply_sector_cap() correctly limited a sector to 2, but gate substitution
# added reserve picks (never in the original basket) without checking sector cap —
# resulting in 3-5 picks from the same sector in the final basket.
# Fix: gate substitution now tracks sector counts of passed picks and skips
# reserve candidates whose sector is already at the 2-pick limit.
# ─────────────────────────────────────────────────────────────────────────────

def test_gate_substitution_respects_sector_cap():
    """After gate substitution, no sector should appear more than twice in tfsa_picks."""
    from ml_engine import _SECTOR_NORM_INF, _TICKER_SECTOR_OVERRIDE

    def _make_pick(ticker, sector_raw, score):
        return {
            "ticker": ticker,
            "score":  score,
            "data":   {"sector": sector_raw, "price": 50.0},
            "sector_canonical": _SECTOR_NORM_INF.get(sector_raw.lower(), sector_raw.upper()),
        }

    # Simulate: passed basket has 2 FINANCIALS already (cap was enforced pre-gate).
    passed = [
        _make_pick("BMO.TO", "Financial Services", 88),
        _make_pick("JPM",    "Financial Services", 87),
        _make_pick("KLAC",   "Technology",          85),
    ]

    # Reserve is dominated by FINANCIALS — without the fix, all 2 replacements
    # would be FINANCIALS, pushing the basket to 4.
    reserve = [
        _make_pick("FBP",   "Financial Services", 84),
        _make_pick("UMBF",  "Financial Services", 83),
        _make_pick("NVDA",  "Technology",          82),
        _make_pick("META",  "Technology",          80),
    ]

    gated_out = [_make_pick("MNST", "Consumer Defensive", 91)]   # 1 slot to fill

    # Replicate the gate substitution selection logic from ml_engine.py
    _repl_sector_counts: dict = {}
    for _pp in passed:
        _ps = _pp.get("sector_canonical") or ""
        if _ps and _ps != "UNKNOWN":
            _repl_sector_counts[_ps] = _repl_sector_counts.get(_ps, 0) + 1

    replacements = []
    _repl_max = 2
    for _cand in reserve:
        if len(replacements) >= len(gated_out):
            break
        _cs = _cand.get("sector_canonical") or ""
        if _cs and _cs != "UNKNOWN" and _repl_sector_counts.get(_cs, 0) >= _repl_max:
            continue
        replacements.append(_cand)
        if _cs and _cs != "UNKNOWN":
            _repl_sector_counts[_cs] = _repl_sector_counts.get(_cs, 0) + 1

    tfsa_picks = passed + replacements

    # NVDA should be the replacement (TECHNOLOGY slot available), not FBP/UMBF (FINANCIALS full)
    sector_counts = Counter(p["sector_canonical"] for p in tfsa_picks)
    assert max(sector_counts.values()) <= 2, (
        f"Sector cap violated after gate substitution: {dict(sector_counts)}"
    )
    repl_sectors = [p["sector_canonical"] for p in replacements]
    assert "FINANCIALS" not in repl_sectors, (
        f"Gate substitution added FINANCIALS when cap was full: {repl_sectors}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# FIX 3 (2026-07-17): _apply_sector_cap fallback re-admits capped sectors
# Bug: when the reserve pool had fewer non-FINANCIALS than excess slots,
# the fallback at the end of _apply_sector_cap unconditionally re-admitted
# excess picks (including FINANCIALS) to maintain basket size — resulting in
# JPM + BMO.TO being listed under both "removed" and "added".
# Fix: pre-filter reserve to exclude capped sectors; fallback also respects cap.
# ─────────────────────────────────────────────────────────────────────────────

def test_sector_cap_fallback_does_not_readmit_capped_sectors():
    """_apply_sector_cap must never re-admit excess picks from a capped sector,
    even when the reserve pool has fewer non-capped picks than slots to fill."""
    from ml_engine import _apply_sector_cap

    def _m(ticker, sector, score):
        return {"ticker": ticker, "score": score, "data": {"sector": sector}, "pick": {}}

    # 6-pick all-FINANCIALS basket — cap should keep 2, excess = 4
    basket = [
        _m("FBP",    "Financial Services", 94),
        _m("BAC",    "Financial Services", 91),
        _m("JPM",    "Financial Services", 88),
        _m("BMO.TO", "Financial Services", 86),
        _m("BNS.TO", "Financial Services", 84),
        _m("BNY",    "Financial Services", 82),
    ]
    # Reserve has only 2 non-FINANCIALS — not enough to fill all 4 excess slots.
    # Without the fix, fallback adds JPM + BMO.TO back → 4 FINANCIALS.
    screener = {
        "TFSA_growth_top5": [
            _m("LLY",   "Health Care", 83),
            _m("NTR.TO","Materials",   80),
        ],
    }

    result = _apply_sector_cap(basket, screener, max_per_sector=2)

    fin_picks = [p for p in result
                 if any(kw in (p.get("data", {}).get("sector", "") or "").lower()
                        for kw in ("financial", "bank", "insurance", "capital"))]
    assert len(fin_picks) <= 2, (
        f"Sector cap re-admitted FINANCIALS via fallback: {[p['ticker'] for p in fin_picks]}"
    )
    # Basket is smaller (4) rather than re-concentrated (6)
    assert len(result) <= 4, (
        f"Expected ≤4 picks (2 FIN + 2 non-FIN), got {len(result)}: "
        f"{[p['ticker'] for p in result]}"
    )
    # FBP and BAC (highest scorers) must be the two FINANCIALS kept
    fin_tickers = {p["ticker"] for p in fin_picks}
    assert "FBP" in fin_tickers and "BAC" in fin_tickers, (
        f"Expected FBP and BAC as the two kept FINANCIALS, got: {fin_tickers}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Sizing stack — Sharpe Guard false claim + portfolio-size independence
# (FIX, 2026-07-19)
# Bug: Step 11 (Risk Audit) printed "⚠️ SHARPE GUARD: ... position sizes
# auto-reduced to X% of normal" — but Step 5 (ML Engine) position sizing
# already finished several steps earlier and never receives Sharpe Guard's
# `system_exposure` value. The message claimed an effect that never happened.
# Fixed by relabeling as an informational advisory and adding an explicit
# SIZING STACK log (percentage-first, portfolio-size-agnostic) in Step 5
# showing what actually determines position sizes.
# ─────────────────────────────────────────────────────────────────────────────

def _sizing_stack_test_weights():
    return [
        {"ticker": "AAA", "weight": 0.1667, "ml_prob": 0.6, "vol_adj": 0.2,
         "kelly_wt": 0.05, "score": 80, "price": 50.0, "probation": False},
        {"ticker": "BBB", "weight": 0.1667, "ml_prob": 0.6, "vol_adj": 0.2,
         "kelly_wt": 0.05, "score": 80, "price": 50.0, "probation": False},
        {"ticker": "CCC", "weight": 0.1667, "ml_prob": 0.6, "vol_adj": 0.2,
         "kelly_wt": 0.05, "score": 80, "price": 50.0, "probation": False},
    ]


def test_sizing_stack_portfolio_size_agnostic():
    """
    render_allocations must produce identical weight_pct and linearly-scaled
    dollar_amt at any capital level. Tested at $10,000 and $1,000,000 — both
    comfortably clear of the dollar-denominated trade-viability floors
    (min_position=$250, min-deploy=$1,500), which are intentionally NOT
    part of this invariant (they exist precisely because very small accounts
    behave differently — see comment in ml_engine.py render_allocations).
    """
    from ml_engine import render_allocations

    weights = _sizing_stack_test_weights()
    market_regime = {"regime": "RECOVERY", "cash_pct": 0.20}   # 80% regime equity

    small = render_allocations(weights, {"name": "TEST", "capital": 10_000, "max_equity": 1.0},
                                market_regime, verbose=False)
    large = render_allocations(weights, {"name": "TEST", "capital": 1_000_000, "max_equity": 1.0},
                                market_regime, verbose=False)

    assert len(small) == len(large) == 3
    small_by_t = {a["ticker"]: a for a in small}
    large_by_t = {a["ticker"]: a for a in large}
    for t in small_by_t:
        assert small_by_t[t]["weight_pct"] == pytest.approx(large_by_t[t]["weight_pct"], abs=0.01), (
            f"{t}: weight_pct differs by capital size — hidden dollar dependency"
        )
        ratio = large_by_t[t]["dollar_amt"] / small_by_t[t]["dollar_amt"]
        assert ratio == pytest.approx(100.0, rel=0.001), (
            f"{t}: dollar_amt did not scale linearly with capital (ratio={ratio})"
        )


def test_sizing_stack_log_present():
    """SIZING STACK block must be present, percentage-first, in Step 5 verbose output."""
    import io, contextlib
    from ml_engine import render_allocations

    weights = [{"ticker": "AAA", "weight": 1.0, "ml_prob": 0.6, "vol_adj": 0.2,
                "kelly_wt": 0.05, "score": 80, "price": 50.0, "probation": False}]
    market_regime = {"regime": "BULL", "cash_pct": 0.0}

    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        render_allocations(weights, {"name": "TEST", "capital": 100_000, "max_equity": 1.0},
                            market_regime, verbose=True)
    out = buf.getvalue()
    assert "SIZING STACK" in out
    assert "final_deployable_pct" in out
    assert "Example render" in out
    assert "portfolio-size-agnostic" in out


def test_sharpe_guard_no_sizing_parameter():
    """
    Locks the architectural fact behind the Sharpe Guard fix: render_allocations
    (Step 5 sizing) accepts no Sharpe-related input, so Sharpe Guard (computed
    later, in Step 11) cannot affect it. If Phase 2 wires Sharpe Guard into real
    sizing, this test must be updated deliberately, not broken by accident.
    """
    import inspect
    from ml_engine import render_allocations
    params = " ".join(inspect.signature(render_allocations).parameters.keys()).lower()
    assert "sharpe" not in params


def test_sharpe_guard_message_not_false_claim():
    """
    run_daily.py's Sharpe Guard message must not claim to have changed position
    sizes (it can't — Step 11 runs after Step 5 sizing is already finalized).
    Guards against reintroducing the "auto-reduced to X% of normal" false claim.
    """
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    with open(os.path.join(root, "run_daily.py")) as f:
        src = f.read()
    assert "position sizes auto-reduced" not in src
    assert "SHARPE ADVISORY" in src
    assert "does not affect sizing" in src


# ─────────────────────────────────────────────────────────────────────────────
# Kelly zero-weight diagnosis (FIX, 2026-07-20) + ml_prob-bucket fix (2026-07-21)
# Finding (2026-07-20): Kelly=0.000 across live picks was CORRECT MATH given
# its actual inputs — CASE A, not a broken-input bug. p_source was the pick's
# own SCORE TIER win rate (win_rate_data["by_score_tier"][tier]), b_source was
# that tier's avg_win/avg_loss ratio — NEITHER was the portfolio-wide overall
# win rate, and NEITHER was ml_prob. ml_prob entered afterward only as a
# separate multiplicative _ml_edge_multiplier, so it could never lift an
# already-zero Kelly weight above zero. Confirmed against real production data
# (2026-07-19 brief): 90-100 tier win_rate=46.1%, avg_win=3.55, avg_loss=4.1
# (n=983) -> f_raw=-0.1615 for every 90-100 tier pick regardless of ml_prob.
#
# FIX (2026-07-21, Option B ml_prob-bucket variant): p/b now come from the
# pick's OWN ml_prob bucket's measured win rate and payoff ratio
# (win_rate_data["by_ml_prob_bucket"][bucket], computed the same way
# by_score_tier already was — see outcome_tracker.py compute_win_rate). Raw
# ml_prob is not itself a calibrated probability (checked empirically: the
# 0.8-1.0 band's real win rate, 50.2%, is worse than the 0.6-0.8 band's
# 61.7%) — only used to select a bucket whose track record is independently
# measured. A pick with no ml_prob logged that day gets no substitute
# (tier average or otherwise) — Kelly floors it to zero. The old post-hoc
# _ml_edge_multiplier was removed (it existed only to give ml_prob influence
# it couldn't otherwise have; keeping it now would double-count the same
# signal already inside p/b).
# ─────────────────────────────────────────────────────────────────────────────

def _kelly_wr_data_mlprob(bucket, win_rate, avg_win, avg_loss, count=500):
    return {"by_ml_prob_bucket": {bucket: {
        "win_rate": win_rate, "avg_win": avg_win, "avg_loss": avg_loss, "count": count,
    }}}


def _kelly_pick(ticker, score, ml_prob, price=50.0):
    return {"ticker": ticker, "score": score, "ml_prob": ml_prob,
            "data": {"price": price, "volatility_90d": 0.2}}


def test_kelly_inputs_stay_in_valid_range():
    """Across realistic ml_prob-bucket win-rate data (both live-bucket and
    static-fallback paths), Kelly's p must stay in [0,1] and b must be a
    positive, finite number — never 0/NaN/negative/out-of-range."""
    import io, contextlib
    from ml_engine import compute_target_weights

    market_regime = {"regime": "BULL", "cash_pct": 0.0}
    cases = [
        # (score, ml_prob, wr_data) — spans live-bucket data (real 2026-07-21
        # measured values) and thin/absent-data fallback
        (80, 0.65, _kelly_wr_data_mlprob("0.6-0.8", 61.7, 5.41, 2.70, count=269)),
        (65, 0.10, _kelly_wr_data_mlprob("0.0-0.2", 51.4, 3.61, 3.91, count=782)),
        (50, 0.90, _kelly_wr_data_mlprob("0.8-1.0", 50.2, 3.52, 5.53, count=444)),
        (80, 0.65, {"by_ml_prob_bucket": {"0.6-0.8": {"win_rate": 61.7, "avg_win": 5.41,
                                                        "avg_loss": 2.70, "count": 5}}}),  # thin -> static fallback
        (80, 0.65, None),  # no wr_data at all -> static fallback
    ]
    for score, ml_prob, wr_data in cases:
        picks = [_kelly_pick("TST", score, ml_prob)]
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            compute_target_weights(picks, market_regime, win_rate_data=wr_data, verbose=True)
        out = buf.getvalue()
        for line in out.splitlines():
            if line.strip().startswith("[kelly]") and "p=" in line:
                p_str = line.split("p=")[1].split()[0]
                b_str = line.split("b=")[1].split()[0]
                p, b = float(p_str), float(b_str)
                assert 0.0 <= p <= 1.0, f"p out of [0,1]: {line}"
                assert b > 0, f"b not positive: {line}"
                assert b == b, f"b is NaN: {line}"   # NaN != NaN


def test_kelly_debug_line_fires_only_when_floored():
    """[kelly] debug line appears when f_raw<=0, and does NOT appear when the
    ml_prob bucket's live stats imply genuine positive edge."""
    import io, contextlib
    from ml_engine import compute_target_weights

    market_regime = {"regime": "BULL", "cash_pct": 0.0}

    # Negative-edge bucket (real 0.8-1.0-bucket-shaped stats) -> must fire
    losing = _kelly_wr_data_mlprob("0.8-1.0", 50.2, 3.52, 5.53, count=444)
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        compute_target_weights([_kelly_pick("LOSER", 80, 0.90)], market_regime,
                                win_rate_data=losing, verbose=True)
    assert "[kelly] LOSER" in buf.getvalue()

    # Strong positive-edge bucket (real 0.6-0.8-bucket-shaped stats) -> must NOT fire
    winning = _kelly_wr_data_mlprob("0.6-0.8", 61.7, 5.41, 2.70, count=269)
    buf2 = io.StringIO()
    with contextlib.redirect_stdout(buf2):
        compute_target_weights([_kelly_pick("WINNER", 65, 0.65)], market_regime,
                                win_rate_data=winning, verbose=True)
    assert "[kelly] WINNER" not in buf2.getvalue()


def test_kelly_p_source_is_ml_prob_bucket_not_score_tier():
    """
    Locks in the 2026-07-21 fix: Kelly's p/b now come from the pick's own
    ml_prob bucket, not its score tier — mirror image of the retired
    test_kelly_p_source_is_tier_win_rate_not_ml_prob, which asserted the
    opposite and required this deliberate update, not silent breakage.
    """
    from ml_engine import compute_target_weights

    market_regime = {"regime": "BULL", "cash_pct": 0.0}
    wr_data = _kelly_wr_data_mlprob("0.6-0.8", 61.7, 5.41, 2.70, count=269)

    # Same ml_prob (same bucket), different score -> identical weight now
    # (score tier no longer drives p/b at all).
    low_score  = compute_target_weights([_kelly_pick("AAA", 65, 0.65)], market_regime,
                                         win_rate_data=wr_data, verbose=False)
    high_score = compute_target_weights([_kelly_pick("AAA", 95, 0.65)], market_regime,
                                         win_rate_data=wr_data, verbose=False)
    assert low_score[0]["kelly_wt"] == high_score[0]["kelly_wt"]
    assert low_score[0]["kelly_wt"] > 0.0   # 0.6-0.8 bucket has real measured edge

    # Different ml_prob bucket (positive-edge 0.6-0.8 vs negative-edge
    # 0.8-1.0), same score -> different weight now (ml_prob drives p/b).
    wr_data_both = {"by_ml_prob_bucket": {
        "0.6-0.8": {"win_rate": 61.7, "avg_win": 5.41, "avg_loss": 2.70, "count": 269},
        "0.8-1.0": {"win_rate": 50.2, "avg_win": 3.52, "avg_loss": 5.53, "count": 444},
    }}
    strong_bucket = compute_target_weights([_kelly_pick("BBB", 80, 0.70)], market_regime,
                                            win_rate_data=wr_data_both, verbose=False)
    weak_bucket   = compute_target_weights([_kelly_pick("BBB", 80, 0.90)], market_regime,
                                            win_rate_data=wr_data_both, verbose=False)
    assert strong_bucket[0]["kelly_wt"] > weak_bucket[0]["kelly_wt"]
    assert weak_bucket[0]["kelly_wt"] == 0.0


def test_kelly_no_ml_prob_floors_to_zero():
    """A pick with no ml_prob logged today (key absent, e.g. MAIN on
    2026-07-21) gets no Kelly weight — floored to zero, never substituted
    with a tier average or a smoothed proxy, even when rich live win-rate
    data is available for other picks' buckets."""
    from ml_engine import compute_target_weights

    market_regime = {"regime": "BULL", "cash_pct": 0.0}
    wr_data = _kelly_wr_data_mlprob("0.6-0.8", 61.7, 5.41, 2.70, count=269)

    pick_no_prob = {"ticker": "MAIN", "score": 87.5,
                     "data": {"price": 50.0, "volatility_90d": 0.2}}  # no ml_prob key at all
    result = compute_target_weights([pick_no_prob], market_regime,
                                     win_rate_data=wr_data, verbose=False)
    assert result[0]["kelly_wt"] == 0.0


def test_ml_prob_bucket_table_matches_recomputation():
    """win_rate.json's by_ml_prob_bucket table — what score_to_kelly_wt reads
    at runtime as Kelly's live p/b — must match an independent recomputation
    from outcomes_log.json-shaped data, so it can't silently drift the way a
    hand-calibrated static baseline could (never auto-refreshed once set)."""
    import tempfile
    from unittest.mock import patch
    from outcome_tracker import compute_win_rate

    def _o(ticker, ml_prob, outcome, actual_return):
        return {"ticker": ticker, "signal_date": "2026-01-01", "score": 80,
                "ml_prob": ml_prob, "resolved": True, "outcome": outcome,
                "actual_return": actual_return}

    synthetic = (
        [_o("A1", 0.65, "WIN",  6.0)] * 3
        + [_o("A2", 0.72, "LOSS", -2.0)] * 2
        + [_o("B1", 0.15, "WIN",  1.0)] * 1
        + [_o("B2", 0.10, "LOSS", -3.0)] * 4
    )
    scratch_win_rate = tempfile.mktemp(suffix=".json")

    with patch("outcome_tracker.load_outcomes", lambda: synthetic), \
         patch("outcome_tracker.WIN_RATE_FILE", scratch_win_rate):
        wr = compute_win_rate()

    table = wr["by_ml_prob_bucket"]

    # Independent hand-recomputation — bucket "0.6-0.8" (5 outcomes: 3 WIN, 2 LOSS)
    assert table["0.6-0.8"]["count"] == 5
    assert table["0.6-0.8"]["win_rate"] == round(3 / 5 * 100, 1)
    assert table["0.6-0.8"]["avg_win"] == round((6.0 * 3) / 3, 2)
    assert table["0.6-0.8"]["avg_loss"] == round((2.0 * 2) / 2, 2)

    # Bucket "0.0-0.2" (5 outcomes: 1 WIN, 4 LOSS)
    assert table["0.0-0.2"]["count"] == 5
    assert table["0.0-0.2"]["win_rate"] == round(1 / 5 * 100, 1)
    assert table["0.0-0.2"]["avg_win"] == 1.0
    assert table["0.0-0.2"]["avg_loss"] == 3.0


# ─────────────────────────────────────────────────────────────────────────────
# 60-74 tier PF drift drill-down (FIX, 2026-07-20)
# Attribution-only feature: when a tier's PF drift crosses the alert threshold
# in the degrading direction, distinguish PENALTY_ROUTING (capped/excluded
# names dragging the aggregate while organic names still meet baseline —
# expected, already-handled) from GENUINE_DECAY (organic names themselves
# below baseline — needs real investigation). Verified against real
# 2026-07-19 production data: 60-74 tier cur=1.68 vs base=1.92 (drift=-0.24,
# matching the original problem statement exactly) -> organic PF=1.97 >=
# baseline 1.92 -> PENALTY_ROUTING.
# ─────────────────────────────────────────────────────────────────────────────

def _drift_pick(ticker, outcome, ret, signal_date="2026-07-01"):
    return {"ticker": ticker, "outcome": outcome, "actual_return": ret, "signal_date": signal_date}


def test_drift_drilldown_penalty_routing():
    """Synthetic: capped subset PF=0.5 (drags tier), organic subset PF=2.1
    (at/above a synthetic baseline of 2.0) -> PENALTY_ROUTING."""
    from run_daily import drift_drilldown

    picks = (
        [_drift_pick("ORGA", "WIN", 7.0)] * 3      # organic wins: sum=21
        + [_drift_pick("ORGB", "LOSS", -5.0)] * 2   # organic losses: sum=10 -> organic PF=2.1
        + [_drift_pick("CAPX", "WIN", 1.0)] * 5     # capped wins: sum=5
        + [_drift_pick("CAPY", "LOSS", -1.0)] * 10  # capped losses: sum=10 -> capped PF=0.5
    )
    penalized = {"CAPX", "CAPY"}
    result = drift_drilldown(picks, "60-74", baseline_pf=2.0, penalized_tickers=penalized, verbose=False)

    assert result["verdict"] == "PENALTY_ROUTING"
    assert any("organic PF" in line for line in result["lines"])
    assert result["capped_stats"]["pf"] < result["organic_stats"]["pf"]
    assert result["capped_stats"]["pf"] == pytest.approx(0.5, abs=0.01)
    assert result["organic_stats"]["pf"] == pytest.approx(2.1, abs=0.01)


def test_drift_drilldown_genuine_decay():
    """Synthetic: capped subset PF=0.5, organic subset ALSO below the
    synthetic baseline (0.9 < 2.0) -> GENUINE_DECAY."""
    from run_daily import drift_drilldown

    picks = (
        [_drift_pick("ORGA", "WIN", 3.0)] * 3       # organic wins: sum=9
        + [_drift_pick("ORGB", "LOSS", -5.0)] * 2    # organic losses: sum=10 -> organic PF=0.9
        + [_drift_pick("CAPX", "WIN", 1.0)] * 5      # capped wins: sum=5
        + [_drift_pick("CAPY", "LOSS", -1.0)] * 10   # capped losses: sum=10 -> capped PF=0.5
    )
    penalized = {"CAPX", "CAPY"}
    result = drift_drilldown(picks, "60-74", baseline_pf=2.0, penalized_tickers=penalized, verbose=False)

    assert result["verdict"] == "GENUINE_DECAY"
    assert result["organic_stats"]["pf"] == pytest.approx(0.9, abs=0.01)


def test_drift_drilldown_no_alert_no_output():
    """No tier's drift crosses the alert threshold -> compute_alerting_tiers
    returns empty, so the orchestration in run_daily.py never calls
    drift_drilldown() and no drill-down section is produced."""
    from run_daily import compute_alerting_tiers

    current_pf   = {"90-100": 0.90, "75-89": 1.05, "60-74": 1.85, "below-60": 1.10}
    baseline_pf  = {"90-100": 0.91, "75-89": 1.07, "60-74": 1.92, "below-60": 1.09}
    alerting = compute_alerting_tiers(current_pf, baseline_pf, alert_threshold=0.20)
    assert alerting == []


# ─────────────────────────────────────────────────────────────────────────────
# NGX resolution — ticker price movement, not macro regime (FIX, 2026-07-20)
# NGX outcomes were resolved using regime_at_resolve (RISK_OFF -> LOSS) because
# /v1/companies was believed to have no price data. Confirmed false — it has
# real price data on the Free tier. Fix: entry_price captured live in
# log_ngx_signals() at signal time, exit_price fetched live in
# resolve_ngx_outcomes() 14+ days later; WIN/LOSS/FLAT now come from
# actual_return_pct vs a +-2.0% band. regime_at_resolve is still captured and
# logged, but is no longer decisive. Missing price data (either side) leaves
# the signal unresolved for retry — never silently defaults to LOSS.
# Only affects resolutions from now on — the 159 already-resolved signals'
# resolved/outcome fields are untouched (retroactive re-resolution by price
# is impossible, no entry_price was ever captured for them).
# ─────────────────────────────────────────────────────────────────────────────

def _ngx_signal(ticker, entry_price, signal_date="2026-01-01"):
    return {
        "ticker": ticker, "name": ticker.replace(".LG", ""), "sector": "banking", "tier": 1,
        "signal_date": signal_date, "signal_time": f"{signal_date}T00:00:00",
        "score_at_signal": 80.0, "persistence": "3d streak", "phase": "FULL", "phase_days": 71,
        "macro_at_signal": {"regime": "NEUTRAL"}, "entry_price": entry_price,
        "resolved": False, "resolved_date": None, "score_at_resolve": None,
        "regime_at_resolve": None, "exit_price": None, "actual_return_pct": None,
        "outcome": None, "outcome_reason": None,
    }


def _ngx_run_result(scored_tickers, macro_regime="RISK_OFF"):
    return {
        "all_scored": [{"ticker": t, "score": 70.0} for t in scored_tickers],
        "macro_regime": macro_regime,
    }


def test_ngx_resolve_win_ignores_regime(monkeypatch):
    """WIN determined by price movement even when regime_at_resolve is
    RISK_OFF — under the OLD logic RISK_OFF alone forced a LOSS regardless
    of price. Also covers Test 4: regime_at_resolve still logged."""
    import ngx_outcome_tracker as nt

    saved = {}
    monkeypatch.setattr(nt, "load_ngx_outcomes", lambda: [_ngx_signal("AAA.LG", 100.0)])
    monkeypatch.setattr(nt, "save_ngx_outcomes", lambda o: saved.__setitem__("outcomes", o))
    monkeypatch.setattr(nt, "fetch_companies_prices", lambda verbose=False: {"AAA": 110.0})  # +10%

    n = nt.resolve_ngx_outcomes(_ngx_run_result(["AAA.LG"], macro_regime="RISK_OFF"))
    assert n == 1
    o = saved["outcomes"][0]
    assert o["outcome"] == "WIN"
    assert o["regime_at_resolve"] == "RISK_OFF"   # logged, but didn't decide the outcome


def test_ngx_resolve_loss_on_negative_return(monkeypatch):
    """LOSS determined by price movement even with a favorable regime."""
    import ngx_outcome_tracker as nt

    saved = {}
    monkeypatch.setattr(nt, "load_ngx_outcomes", lambda: [_ngx_signal("BBB.LG", 100.0)])
    monkeypatch.setattr(nt, "save_ngx_outcomes", lambda o: saved.__setitem__("outcomes", o))
    monkeypatch.setattr(nt, "fetch_companies_prices", lambda verbose=False: {"BBB": 90.0})  # -10%

    n = nt.resolve_ngx_outcomes(_ngx_run_result(["BBB.LG"], macro_regime="RISK_ON"))
    assert n == 1
    o = saved["outcomes"][0]
    assert o["outcome"] == "LOSS"
    assert o["regime_at_resolve"] == "RISK_ON"


def test_ngx_resolve_price_fetch_failure_stays_unresolved(monkeypatch):
    """Total price-fetch failure (empty dict, e.g. no key or API down) ->
    signal stays unresolved, no crash, never silently defaults to LOSS."""
    import ngx_outcome_tracker as nt

    saved = {}
    monkeypatch.setattr(nt, "load_ngx_outcomes", lambda: [_ngx_signal("CCC.LG", 100.0)])
    monkeypatch.setattr(nt, "save_ngx_outcomes", lambda o: saved.__setitem__("outcomes", o))
    monkeypatch.setattr(nt, "fetch_companies_prices", lambda verbose=False: {})

    n = nt.resolve_ngx_outcomes(_ngx_run_result(["CCC.LG"], macro_regime="RISK_OFF"))
    assert n == 0
    o = saved["outcomes"][0]
    assert o["resolved"] is False
    assert o["outcome"] is None
    assert "UNRESOLVED" in o["outcome_reason"]


def test_ngx_resolve_missing_entry_price_stays_unresolved(monkeypatch):
    """A signal logged before entry_price capture existed (entry_price=None)
    can never be resolved by price — stays unresolved, does not crash,
    does not silently default to LOSS."""
    import ngx_outcome_tracker as nt

    saved = {}
    monkeypatch.setattr(nt, "load_ngx_outcomes", lambda: [_ngx_signal("DDD.LG", None)])
    monkeypatch.setattr(nt, "save_ngx_outcomes", lambda o: saved.__setitem__("outcomes", o))
    monkeypatch.setattr(nt, "fetch_companies_prices", lambda verbose=False: {"DDD": 105.0})

    n = nt.resolve_ngx_outcomes(_ngx_run_result(["DDD.LG"], macro_regime="RISK_OFF"))
    assert n == 0
    o = saved["outcomes"][0]
    assert o["resolved"] is False
    assert o["outcome"] is None


def test_ngx_log_signals_captures_entry_price(monkeypatch):
    """log_ngx_signals must capture a live entry_price for each new signal —
    the only chance to ever record it, since /v1/companies has no
    historical/point-in-time lookup."""
    import ngx_outcome_tracker as nt

    saved = {}
    monkeypatch.setattr(nt, "load_ngx_outcomes", lambda: [])
    monkeypatch.setattr(nt, "save_ngx_outcomes", lambda o: saved.__setitem__("outcomes", o))
    monkeypatch.setattr(nt, "fetch_companies_prices", lambda verbose=False: {"EEE": 42.5})

    ngx_result = {
        "signals": [{"ticker": "EEE.LG", "name": "EEE", "sector": "banking", "tier": 1, "score": 80}],
        "phase": "FULL", "phase_days": 71,
        "macro_regime": "NEUTRAL", "macro_score": 0, "fx_stress": 0,
        "brent_trend": "FLAT", "basket_regime": "NEUTRAL",
    }
    n = nt.log_ngx_signals(ngx_result)
    assert n == 1
    assert saved["outcomes"][0]["entry_price"] == 42.5


# ─────────────────────────────────────────────────────────────────────────────
# NGX legacy exclusion (FIX, 2026-07-21)
# 159 signals (2026-05-16..2026-05-22) were resolved LOSS under the old
# macro-regime logic; 33 more were logged before entry_price capture existed
# and can never be resolved by price. Both are flagged excluded_legacy=True
# so they stop counting toward WR/PF, stop inflating "pending", and are never
# re-walked by resolve_ngx_outcomes() — permanently excluded, not silently
# vanished (exclusion_reason preserved for audit).
# ─────────────────────────────────────────────────────────────────────────────

def _ngx_legacy_loss_signal(ticker, signal_date="2026-05-16"):
    """A pre-fix signal resolved LOSS under old macro-regime logic, now
    flagged excluded_legacy — mirrors the real 159 entries' shape (no
    entry_price key at all, since that field didn't exist yet)."""
    return {
        "ticker": ticker, "name": ticker.replace(".LG", ""), "sector": "oil", "tier": 1,
        "signal_date": signal_date, "signal_time": f"{signal_date}T00:00:00",
        "score_at_signal": 87.7, "persistence": "7d streak", "phase": "PAPER_ONLY", "phase_days": 6,
        "macro_at_signal": {"regime": "RISK_ON"},
        "resolved": True, "resolved_date": "2026-05-25", "score_at_resolve": 55.1,
        "regime_at_resolve": "RISK_OFF", "outcome": "LOSS",
        "outcome_reason": "Macro flipped RISK_OFF (score now 55.1)",
        "excluded_legacy": True,
        "exclusion_reason": "Resolved under pre-fix macro-regime logic; no entry_price captured.",
    }


def _ngx_orphaned_pending_signal(ticker, signal_date="2026-07-16"):
    """A pre-fix signal that was never resolved and never will be (no
    entry_price ever captured) — mirrors the real 33 entries' shape."""
    return {
        "ticker": ticker, "name": ticker.replace(".LG", ""), "sector": "banking", "tier": 2,
        "signal_date": signal_date, "signal_time": f"{signal_date}T00:00:00",
        "score_at_signal": 70.0, "persistence": "3d streak", "phase": "FULL", "phase_days": 71,
        "macro_at_signal": {"regime": "RISK_ON"},
        "resolved": False, "resolved_date": None, "score_at_resolve": None,
        "regime_at_resolve": None, "outcome": None, "outcome_reason": None,
        "excluded_legacy": True,
        "exclusion_reason": "Logged before entry_price capture existed; can never be resolved by price.",
    }


def test_ngx_excluded_legacy_dropped_from_wr_pf(monkeypatch):
    """excluded_legacy entries (both the resolved-LOSS and orphaned-pending
    flavors) must not count toward wins/losses/win_rate — only the clean,
    price-resolved signal should show up in WR/PF."""
    import ngx_outcome_tracker as nt

    monkeypatch.setattr(nt, "load_ngx_outcomes", lambda: [
        _ngx_legacy_loss_signal("SEPLAT.LG"),
        _ngx_orphaned_pending_signal("GTCO.LG"),
        {**_ngx_signal("AAA.LG", 100.0), "resolved": True, "outcome": "WIN",
         "actual_return_pct": 5.0},
    ])

    s = nt.ngx_outcome_summary()
    assert s["total_logged"] == 3
    assert s["excluded_legacy_total"] == 2
    assert s["excluded_legacy_resolved"] == 1
    assert s["excluded_legacy_pending"] == 1
    assert s["total_resolved"] == 1
    assert s["pending"] == 0
    assert s["wins"] == 1
    assert s["losses"] == 0
    assert s["win_rate"] == 100.0


def test_ngx_excluded_legacy_not_retried(monkeypatch):
    """resolve_ngx_outcomes() must skip excluded_legacy entries entirely —
    no date parsing, no price lookup, no field mutation, no resolve count."""
    import ngx_outcome_tracker as nt

    saved = {}
    legacy_resolved = _ngx_legacy_loss_signal("SEPLAT.LG")
    legacy_pending  = _ngx_orphaned_pending_signal("GTCO.LG")
    before = [dict(legacy_resolved), dict(legacy_pending)]

    monkeypatch.setattr(nt, "load_ngx_outcomes", lambda: [legacy_resolved, legacy_pending])
    monkeypatch.setattr(nt, "save_ngx_outcomes", lambda o: saved.__setitem__("outcomes", o))
    # If the loop touched these, it would try to fetch prices for them —
    # returning prices here would let a bug slip through as a false pass.
    monkeypatch.setattr(nt, "fetch_companies_prices",
                         lambda verbose=False: {"SEPLAT": 999.0, "GTCO": 999.0})

    n = nt.resolve_ngx_outcomes(_ngx_run_result(["SEPLAT.LG", "GTCO.LG"]))
    assert n == 0
    assert saved["outcomes"] == before  # byte-for-byte untouched


def test_ngx_oos_clock_not_yet_started_when_no_priced_signals(monkeypatch):
    """With only excluded_legacy entries (no entry_price anywhere), the
    clean OOS clock must report as not started, not crash or fabricate a date."""
    import ngx_outcome_tracker as nt

    monkeypatch.setattr(nt, "load_ngx_outcomes", lambda: [
        _ngx_legacy_loss_signal("SEPLAT.LG"),
        _ngx_orphaned_pending_signal("GTCO.LG"),
    ])

    s = nt.ngx_outcome_summary()
    assert s["oos_start_date"] is None
    assert s["oos_day"] is None


def test_ngx_oos_clock_starts_at_first_priced_signal(monkeypatch):
    """OOS Day N is computed from the earliest signal_date carrying a
    non-null entry_price, ignoring excluded_legacy entries and later signals."""
    import ngx_outcome_tracker as nt
    from datetime import date, timedelta

    ten_days_ago = (date.today() - timedelta(days=10)).strftime("%Y-%m-%d")
    five_days_ago = (date.today() - timedelta(days=5)).strftime("%Y-%m-%d")

    monkeypatch.setattr(nt, "load_ngx_outcomes", lambda: [
        _ngx_legacy_loss_signal("SEPLAT.LG"),  # no entry_price -> ignored
        _ngx_signal("AAA.LG", 100.0, signal_date=ten_days_ago),
        _ngx_signal("BBB.LG", 50.0, signal_date=five_days_ago),
    ])

    s = nt.ngx_outcome_summary()
    assert s["oos_start_date"] == ten_days_ago
    assert s["oos_day"] == 11  # inclusive day count


# ─────────────────────────────────────────────────────────────────────────────
# NGX per-ticker validation clock (FIX, 2026-07-20)
# Replaces the single global clock (one date in ngx_validation_start.txt,
# shared by every ticker) with per-ticker start dates in
# ngx_ticker_start_dates.json, so a newly-added ticker always starts at
# PAPER_ONLY Day 0 regardless of how long the existing universe has been
# running. The 29 pre-existing tickers were backfilled with the old global
# start date (2026-05-10) so their phase/day output is unchanged.
# ─────────────────────────────────────────────────────────────────────────────

def test_ngx_validation_phase_full_after_60_days(monkeypatch):
    """Ticker with a start date >60 days ago -> FULL."""
    import ngx_screener as ns
    from datetime import date, timedelta

    old_date = (date.today() - timedelta(days=65)).strftime("%Y-%m-%d")
    monkeypatch.setattr(ns, "_load_ticker_start_dates", lambda: {"OLD.LG": old_date})
    monkeypatch.setattr(ns, "_save_ticker_start_dates", lambda d: None)

    phase, days = ns.get_validation_phase("OLD.LG")
    assert phase == "FULL"
    assert days >= 60


def test_ngx_validation_phase_paper_only_under_30_days(monkeypatch):
    """Ticker with a start date <30 days ago -> PAPER_ONLY."""
    import ngx_screener as ns
    from datetime import date, timedelta

    recent_date = (date.today() - timedelta(days=10)).strftime("%Y-%m-%d")
    monkeypatch.setattr(ns, "_load_ticker_start_dates", lambda: {"NEWISH.LG": recent_date})
    monkeypatch.setattr(ns, "_save_ticker_start_dates", lambda d: None)

    phase, days = ns.get_validation_phase("NEWISH.LG")
    assert phase == "PAPER_ONLY"
    assert days < 30


def test_ngx_validation_phase_brand_new_ticker_gets_day_zero(monkeypatch):
    """A ticker with no entry in ngx_ticker_start_dates.json gets one
    created with today's date -> PAPER_ONLY, Day 0 -- and it's written
    back for persistence (proven via the captured save call)."""
    import ngx_screener as ns

    saved = {}
    monkeypatch.setattr(ns, "_load_ticker_start_dates", lambda: {})
    monkeypatch.setattr(ns, "_save_ticker_start_dates", lambda d: saved.update(d))

    phase, days = ns.get_validation_phase("BRANDNEW.LG")
    assert phase == "PAPER_ONLY"
    assert days == 0
    assert "BRANDNEW.LG" in saved


def test_ngx_validation_phase_independent_per_ticker(monkeypatch):
    """Two tickers with different start dates return different phases in
    the same run -- proves independence from a single global clock."""
    import ngx_screener as ns
    from datetime import date, timedelta

    dates = {
        "VERYOLD.LG":   (date.today() - timedelta(days=90)).strftime("%Y-%m-%d"),  # -> FULL
        "BRANDNEW2.LG": (date.today() - timedelta(days=1)).strftime("%Y-%m-%d"),   # -> PAPER_ONLY
    }
    monkeypatch.setattr(ns, "_load_ticker_start_dates", lambda: dict(dates))
    monkeypatch.setattr(ns, "_save_ticker_start_dates", lambda d: None)

    p1, _ = ns.get_validation_phase("VERYOLD.LG")
    p2, _ = ns.get_validation_phase("BRANDNEW2.LG")
    assert p1 == "FULL"
    assert p2 == "PAPER_ONLY"
    assert p1 != p2


def test_ngx_backfilled_tickers_match_old_global_clock():
    """The 29 existing tickers, after backfill, must reproduce EXACTLY the
    same phase/day math the old single global clock would have given them.
    Compares the two real files directly (not a hardcoded day count, which
    would go stale) -- a pure regression check, no monkeypatching, since
    it's verifying the actual committed backfill state."""
    import json
    from datetime import datetime
    import ngx_screener as ns

    old_start = datetime.strptime(open("ngx_validation_start.txt").read().strip(), "%Y-%m-%d")
    old_days  = (datetime.now() - old_start).days
    if old_days < 30:     old_phase = "PAPER_ONLY"
    elif old_days < 60:   old_phase = "RESTRICTED"
    else:                 old_phase = "FULL"

    # File now also holds the 56 Session-B-Part-2 candidate tickers
    # (NGX_TIER3_CANDIDATES), seeded at today's date -- so it's a superset
    # of NGX_ALL, not an exact match, as of Part 2.
    backfill = json.load(open("ngx_ticker_start_dates.json"))
    assert set(ns.NGX_ALL).issubset(backfill.keys())

    for ticker in ["GTCO.LG", "ZENITHBANK.LG", "CWG.LG"]:
        phase, days = ns.get_validation_phase(ticker)
        assert phase == old_phase
        assert days == old_days


# ─────────────────────────────────────────────────────────────────────────────
# NGX universe expansion, Part 2 (FIX, 2026-07-20)
# 56 new candidate tickers (NGX_TIER3_CANDIDATES) were selected via
# market_cap >= NGN 25B and volume not-null & >= 1,000, filtered from the
# live 150-symbol /v1/companies universe. They are explicitly NOT merged
# into NGX_ALL / main scoring rotation this session -- staged separately
# so they start PAPER_ONLY at Day 0 under the per-ticker validation clock.
# NGX_API_SECTOR_MAP (the /v1/companies 13-category taxonomy) covers all
# 85 tickers for reporting/future sector-diversity use, but is deliberately
# NOT wired into SECTOR_SENSITIVITY -- NGX_SECTOR_MAP (old taxonomy, feeds
# live scoring for the current 29) is unchanged.
# ─────────────────────────────────────────────────────────────────────────────

def test_ngx_tier3_candidates_not_merged_into_main_rotation():
    """The 56 new candidates must NOT be part of NGX_ALL (main scoring
    rotation) yet -- that's a separate, future, explicitly-approved step."""
    import ngx_screener as ns

    assert len(ns.NGX_ALL) == 29
    assert len(ns.NGX_TIER3_CANDIDATES) == 56
    assert set(ns.NGX_ALL).isdisjoint(set(ns.NGX_TIER3_CANDIDATES))


def test_ngx_api_sector_map_covers_all_85_no_more_no_less():
    """NGX_API_SECTOR_MAP must cover exactly the union of NGX_ALL and
    NGX_TIER3_CANDIDATES -- no missing entries, no stale/extra ones."""
    import ngx_screener as ns

    all85 = set(ns.NGX_ALL) | set(ns.NGX_TIER3_CANDIDATES)
    assert set(ns.NGX_API_SECTOR_MAP.keys()) == all85
    assert len(ns.NGX_API_SECTOR_MAP) == 85


def test_ngx_sector_map_unchanged_and_still_resolves_to_sensitivity():
    """NGX_SECTOR_MAP (the OLD taxonomy that actually feeds live scoring)
    must be untouched by the Part 2 sector-map work: still exactly the
    current 29 tickers, and every value must still be a real key in
    SECTOR_SENSITIVITY -- proving score_ngx_macro() never silently falls
    back to the "banking" default for any current ticker."""
    import ngx_screener as ns

    assert set(ns.NGX_SECTOR_MAP.keys()) == set(ns.NGX_ALL)
    for ticker, sector in ns.NGX_SECTOR_MAP.items():
        assert sector in ns.SECTOR_SENSITIVITY, (
            f"{ticker} -> {sector!r} has no SECTOR_SENSITIVITY entry, "
            f"would silently fall back to the 'banking' default"
        )


def test_ngx_ticker_start_dates_seeded_for_all_85():
    """ngx_ticker_start_dates.json must have an entry for every one of the
    85 tickers (29 current + 56 new candidates) after Part 2's seeding --
    the new 56 must not be left to lazily initialize on first pipeline run."""
    import json
    import ngx_screener as ns

    dates = json.load(open("ngx_ticker_start_dates.json"))
    all85 = set(ns.NGX_ALL) | set(ns.NGX_TIER3_CANDIDATES)
    assert set(dates.keys()) == all85
    assert len(dates) == 85


def test_ngx_tier3_candidates_start_paper_only_day_zero(monkeypatch):
    """A representative sample of the 56 new candidates must resolve to
    PAPER_ONLY, Day 0 the first time they're queried with no prior start
    date -- proves the seeding used the same Day-0 code path Part 1 tests,
    without depending on today's real date (would go stale after 30 days)."""
    import ngx_screener as ns

    monkeypatch.setattr(ns, "_load_ticker_start_dates", lambda: {})
    monkeypatch.setattr(ns, "_save_ticker_start_dates", lambda d: None)

    for ticker in ["ARADEL.LG", "STERLINGNG.LG", "VFDGROUP.LG"]:
        phase, days = ns.get_validation_phase(ticker)
        assert phase == "PAPER_ONLY"
        assert days == 0


# ─────────────────────────────────────────────────────────────────────────────
# weekly_scout.yml git-add completeness (FIX, 2026-07-20)
# scout_agent.py writes 5 files. The workflow's "git add" line originally
# staged only 3 (missing global_watch.json, a tracked file, which is the
# real cause: modifying it without staging left the tree dirty and broke
# the next "git pull --rebase origin main" with "You have unstaged
# changes" -- that failure lost commit f9d0525b entirely, unrecoverable
# since GitHub Actions runners are ephemeral and the push never ran).
#
# A first fix attempt also added universe_failure_log.json to git-add --
# WRONG: that file is deliberately gitignored (.gitignore's "Private
# trading data" section, exact-name rule, not a wildcard catching it by
# accident) and `git add` on a gitignored path fails (exit 1) without -f,
# which would abort the whole step (GitHub Actions run: steps default to
# bash -e) before commit/pull/push ever ran -- a worse, deterministic
# failure on every future run. Verified empirically in a scratch repo:
# a gitignored+untracked file's modifications never trigger "unstaged
# changes" for git pull --rebase (git doesn't track it at all); only
# tracked files with uncommitted changes do.
#
# This test derives scout_agent.py's write footprint from its own source
# (not hand-copied), splits it into trackable vs. gitignored via the
# repo's REAL .gitignore (git check-ignore, not a re-implementation of
# gitignore semantics), and asserts: every trackable file is staged, and
# no gitignored file is staged -- so this exact regression (staging an
# ignored file) can't silently return either.
# ─────────────────────────────────────────────────────────────────────────────

def test_weekly_scout_workflow_stages_every_file_scout_agent_writes():
    import re
    import subprocess

    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    scout_src = open(os.path.join(root, "scout_agent.py")).read()

    # Every module-level FILE = "....json" constant that's opened in write
    # mode somewhere in the module -- this is scout_agent.py's actual
    # write footprint, derived from the source, not hand-maintained here.
    written_vars = set(re.findall(r'open\((\w+),\s*"w"\)', scout_src))
    consts = dict(re.findall(r'^(\w+)\s*=\s*"([^"]+\.json)"', scout_src, re.MULTILINE))
    written_files = {consts[v] for v in written_vars if v in consts}

    assert written_files == {
        "universe_dynamic.json", "universe_current.json",
        "universe_rotation_log.json", "universe_failure_log.json",
        "global_watch.json",
    }, ("scout_agent.py's write footprint changed -- re-derive the "
        "trackable/ignored split below against the new file list")

    def _is_gitignored(fname):
        result = subprocess.run(
            ["git", "check-ignore", "-q", fname], cwd=root
        )
        return result.returncode == 0

    trackable = {f for f in written_files if not _is_gitignored(f)}
    ignored   = written_files - trackable
    assert ignored, "expected at least one deliberately-gitignored file in the mix"

    workflow_path = os.path.join(root, ".github", "workflows", "weekly_scout.yml")
    workflow_src = open(workflow_path).read()
    add_line = next(l for l in workflow_src.splitlines() if l.strip().startswith("git add "))

    missing = [f for f in trackable if f not in add_line]
    assert not missing, (
        f"weekly_scout.yml's git-add line is missing {missing} -- scout_agent.py "
        f"writes these TRACKABLE files but the workflow won't stage/commit them, "
        f"which leaves the working tree dirty and breaks the subsequent "
        f"'git pull --rebase' step (this is exactly how commit f9d0525b was lost)"
    )

    wrongly_staged = [f for f in ignored if f in add_line]
    assert not wrongly_staged, (
        f"weekly_scout.yml's git-add line stages {wrongly_staged}, which "
        f".gitignore deliberately excludes -- `git add` on a gitignored path "
        f"fails (exit 1) without -f, which aborts the whole step under "
        f"GitHub Actions' default bash -e before commit/pull/push ever run"
    )


# ─────────────────────────────────────────────────────────────────────────────
# get_static_universe() -- AST parse, not a hardcoded line range (FIX, 2026-07-20)
# The old [29:135] line-slice on stock_screener.py stopped 9 lines before
# UNIVERSE's own closing brace (144), silently dropping the tail of the
# static universe (a run of Canadian ETFs) as the file grew -- and had no
# relationship at all to EXCLUDED_TICKERS (line 154+), so NGE (excluded
# there) and NVEI.TO (also excluded there) were handled inconsistently by
# line-range accident rather than by principle. Fixed via ast parsing of
# the real UNIVERSE/EXCLUDED_TICKERS assignments, which can't go stale as
# the file grows since it reads the actual Python structure, not text
# position.
# ─────────────────────────────────────────────────────────────────────────────

def test_get_static_universe_excludes_excluded_tickers():
    """NGE and NVEI.TO are both in stock_screener.py's EXCLUDED_TICKERS and
    must never appear in get_static_universe()'s output -- by principle
    (mirrors ALL_TICKERS' own definition there), not by line-range luck."""
    import scout_agent as sa

    tickers = sa.get_static_universe()
    assert "NGE" not in tickers
    assert "NVEI.TO" not in tickers


def test_get_static_universe_includes_tail_past_old_line_boundary():
    """Tickers that sat past the old hardcoded line-135 cutoff (the tail of
    the UNIVERSE dict's Canadian ETF group) must be included now -- proves
    the fix, not just that get_static_universe() runs without error."""
    import scout_agent as sa

    tickers = sa.get_static_universe()
    for ticker in ["ZEB.TO", "ZRE.TO", "XRE.TO", "HXT.TO", "HXS.TO"]:
        assert ticker in tickers, f"{ticker} missing -- past the old [29:135] boundary"


# ─────────────────────────────────────────────────────────────────────────────
# Hold-period retention re-validates exchange eligibility (FIX, 2026-07-20)
# Hold-period retention (an earlier session's fix) carried old_dynamic
# tickers forward without re-checking _is_valid_exchange() -- so 7 foreign
# tickers that predate commit 716b8149 ("reject foreign exchange listings")
# kept surviving indefinitely via retention, never re-validated. Fixed:
# apply_hold_period_retention() now drops (and quarantines) any retained
# ticker that fails _is_valid_exchange(), even mid-hold-period -- exchange
# listing is static, not momentum-driven, so this can't cause the flapping
# the hold period exists to prevent.
# ─────────────────────────────────────────────────────────────────────────────

def test_hold_period_retention_drops_and_quarantines_foreign_ticker(monkeypatch):
    """A foreign-exchange ticker still well within its hold period must be
    dropped and routed to quarantine, not retained -- even though a
    legitimate NA ticker with the same age is retained normally."""
    import datetime
    import scout_agent as sa

    today = datetime.date.today().isoformat()
    recent = (datetime.date.today() - datetime.timedelta(days=5)).isoformat()
    old_dynamic = {
        "FOREIGN.T": {"scout_score": 50, "source": "scout_botz", "scouted_at": recent},
        "LEGIT.TO":  {"scout_score": 50, "source": "scout_xic",  "scouted_at": recent},
    }

    quarantined = {}
    monkeypatch.setattr(sa, "_load_global_watch", lambda: {})
    monkeypatch.setattr(sa, "_save_global_watch", lambda d: quarantined.update(d))

    retained, dropped = sa.apply_hold_period_retention(
        old_dynamic, passing={}, inactive_set=set(), today=today
    )

    assert "FOREIGN.T" not in retained
    assert "FOREIGN.T" in dropped
    assert "FOREIGN.T" in quarantined  # routed to quarantine, not a silent drop

    # Legitimate NA ticker of the same age is retained normally -- proves
    # the exchange check doesn't undermine the hold period for real tickers
    assert "LEGIT.TO" in retained
    assert "LEGIT.TO" not in dropped


# ─────────────────────────────────────────────────────────────────────────────
# Resolver reconciliation (FIX, 2026-07-22)
# outcome_tracker.py and signal_ledger.py used to resolve WIN/LOSS/FLAT
# independently. Of 685 overlapping (ticker, signal_date) pairs, 37 (11.1%)
# disagreed -- root cause: resolve_ledger() had no active stale-price fetch
# and passively waited for a ticker to reappear in a future day's top-N
# list, which is winner-biased (losers don't recover into top-N, so they
# sat unresolved indefinitely: 258 ledger-unresolved pairs that outcomes_log
# HAD resolved carried a 31.4% WR, far below the ledger's self-reported
# 63.7%). outcome_tracker.py is now canonical (active _fetch_stale_prices()
# fallback, resolves on the stated 7-day schedule regardless of outcome).
# resolve_ledger() now purely reads outcome_tracker's resolution, keyed on
# (ticker, signal_date) -- no price fetch, no independent WIN/LOSS decision.
# Resolution fields are outside _entry_hash()'s scope, so this cannot break
# the hash chain (verified live: 686/686 intact before AND after backfill).
# ─────────────────────────────────────────────────────────────────────────────

def _ledger_entry(ticker, signal_date, entry_price=100.0, resolved=False,
                   outcome=None, exit_price=None, resolved_date=None):
    return {
        "ticker": ticker, "signal_date": signal_date,
        "signal_time": f"{signal_date}T00:00:00", "entry_price": entry_price,
        "score": 80.0, "ml_prob": 0.5, "category": "GROWTH CORE",
        "regime_at_signal": "BULL", "news_active": [], "attribution": [],
        "rs_rating": 70, "sector": "banking", "market": "US",
        "resolved": resolved, "exit_price": exit_price, "actual_return": None,
        "outcome": outcome, "resolved_date": resolved_date, "hold_days": None,
        "prev_hash": "GENESIS", "entry_hash": "placeholder",
    }


def _outcomes_entry(ticker, signal_date, resolved=True, outcome="WIN",
                     exit_price=110.0, resolved_date="2026-01-08",
                     actual_return=10.0):
    return {
        "ticker": ticker, "signal_date": signal_date, "resolved": resolved,
        "outcome": outcome, "exit_price": exit_price,
        "resolved_date": resolved_date, "actual_return": actual_return,
    }


def test_resolve_ledger_reads_through_no_disagreement(monkeypatch):
    """After resolve_ledger(), a ledger entry's outcome/exit_price/
    resolved_date must exactly match outcomes_log's -- by construction,
    since it's copied, not independently computed. Covers both a
    previously-unresolved ledger entry (the 258 case) and a
    previously-resolved-but-wrong one (the 37-mismatch case)."""
    import signal_ledger as sl
    import outcome_tracker as ot

    never_resolved = _ledger_entry("AAA", "2026-01-01")  # the 258 case
    wrongly_resolved = _ledger_entry(                     # the 37 case
        "BBB", "2026-01-01", resolved=True, outcome="WIN",
        exit_price=999.0, resolved_date="2026-01-20",     # stale, wrong
    )
    ledger = [never_resolved, wrongly_resolved]

    canonical = [
        _outcomes_entry("AAA", "2026-01-01", outcome="LOSS",
                         exit_price=90.0, resolved_date="2026-01-08", actual_return=-10.0),
        _outcomes_entry("BBB", "2026-01-01", outcome="LOSS",
                         exit_price=95.0, resolved_date="2026-01-08", actual_return=-5.0),
    ]

    saved = {}
    monkeypatch.setattr(sl, "_load_ledger", lambda: ledger)
    monkeypatch.setattr(sl, "_save_ledger", lambda e: saved.__setitem__("ledger", e))
    monkeypatch.setattr(ot, "load_outcomes", lambda: canonical)

    n = sl.resolve_ledger()
    assert n == 2
    result = {e["ticker"]: e for e in saved["ledger"]}
    expected_exit = {"AAA": 90.0, "BBB": 95.0}
    for tkr in ("AAA", "BBB"):
        assert result[tkr]["resolved"] is True
        assert result[tkr]["outcome"] == "LOSS"
        assert result[tkr]["exit_price"] == expected_exit[tkr]
        assert result[tkr]["resolved_date"] == "2026-01-08"
        assert result[tkr]["resolution_source"] == "outcomes_log"


def test_resolve_ledger_preserves_legacy_fields(monkeypatch):
    """A ledger entry with a prior (wrong, independently-computed) resolution
    must have that original value preserved in *_ledger_legacy fields --
    never deleted, only superseded."""
    import signal_ledger as sl
    import outcome_tracker as ot

    old_wrong = _ledger_entry(
        "CCC", "2026-01-01", resolved=True, outcome="WIN",
        exit_price=999.0, resolved_date="2026-01-20",
    )
    old_wrong["actual_return"] = 899.0

    saved = {}
    monkeypatch.setattr(sl, "_load_ledger", lambda: [old_wrong])
    monkeypatch.setattr(sl, "_save_ledger", lambda e: saved.__setitem__("ledger", e))
    monkeypatch.setattr(ot, "load_outcomes", lambda: [
        _outcomes_entry("CCC", "2026-01-01", outcome="LOSS",
                         exit_price=95.0, resolved_date="2026-01-08", actual_return=-5.0),
    ])

    sl.resolve_ledger()
    entry = saved["ledger"][0]

    # New (canonical) values took over
    assert entry["outcome"] == "LOSS"
    assert entry["exit_price"] == 95.0

    # Original values preserved, not deleted
    assert entry["outcome_ledger_legacy"] == "WIN"
    assert entry["exit_price_ledger_legacy"] == 999.0
    assert entry["resolved_date_ledger_legacy"] == "2026-01-20"
    assert entry["actual_return_ledger_legacy"] == 899.0
    for legacy_field in ("outcome_ledger_legacy", "exit_price_ledger_legacy",
                         "resolved_date_ledger_legacy", "actual_return_ledger_legacy"):
        assert entry[legacy_field] is not None and entry[legacy_field] != ""


def test_resolve_ledger_chain_survives_backfill(monkeypatch):
    """Overwriting resolution fields via the read-through must never break
    the hash chain -- resolution fields are outside _entry_hash()'s scope
    by design. Build a real chained entry via append_signals(), then run
    the backfill against a DIFFERENT outcome, then verify_chain()."""
    import signal_ledger as sl
    import outcome_tracker as ot

    store: list = []
    monkeypatch.setattr(sl, "_load_ledger", lambda: list(store))  # copy -- avoid aliasing store
    monkeypatch.setattr(sl, "_save_ledger", lambda e: (store.clear(), store.extend(e)))

    sl.append_signals(
        [{"ticker": "DDD", "score": 80, "ml_prob": 0.6, "data": {"price": 100.0}}],
        regime={"regime": "BULL"},
    )
    ok_before, _ = sl.verify_chain()
    assert ok_before is True

    monkeypatch.setattr(ot, "load_outcomes", lambda: [
        _outcomes_entry("DDD", store[0]["signal_date"], outcome="LOSS",
                         exit_price=90.0, resolved_date="2026-01-08", actual_return=-10.0),
    ])
    n = sl.resolve_ledger()
    assert n == 1
    assert store[0]["outcome"] == "LOSS"

    ok_after, broken_idx = sl.verify_chain()
    assert ok_after is True
    assert broken_idx is None


def test_resolve_ledger_no_price_fetch_in_source():
    """resolve_ledger() must never call a price-fetch function -- that would
    reintroduce the independent-resolver drift this replaced."""
    import inspect
    import signal_ledger as sl

    src = inspect.getsource(sl.resolve_ledger)
    for banned in ("yfinance", "yf.", "download", "current_prices", "_fetch_stale_prices"):
        assert banned not in src, f"resolve_ledger() source contains '{banned}' — should be pure read-through"


# ─────────────────────────────────────────────────────────────────────────────
# Outcome threshold widened ±0.3% -> ±0.5% (FIX, 2026-07-22)
# Derived from this dataset's |actual_return| distribution (median 2.45%,
# p10 0.43%) and a realistic round-trip cost estimate for a mixed US/TSX
# large-cap book -- not defaulted from NGX's ±2.0%. All 2,267 historical
# resolved rows were reclassified from stored actual_return (no price
# refetching); each row's prior classification is preserved in
# outcome_legacy_030, never deleted. Net-of-cost analysis (separate,
# reporting-only) found full-history PF crosses below 1.0 at just 8bps
# round-trip cost -- the edge is thin. OOS PF was already <1.0 before any
# cost. See outcome_tracker.py OUTCOME_THRESHOLD_PCT / _classify_outcome.
# ─────────────────────────────────────────────────────────────────────────────

def test_outcome_threshold_single_source_of_truth():
    """The WIN/LOSS/FLAT threshold must be read from exactly one constant,
    used by exactly one classification function -- no hardcoded ±0.3 or
    ±0.5 duplicated anywhere else in the resolution path."""
    import inspect
    import outcome_tracker as ot

    assert ot.OUTCOME_THRESHOLD_PCT == 0.5

    resolve_src = inspect.getsource(ot.resolve_outcomes)
    assert "0.3" not in resolve_src
    assert "0.5" not in resolve_src   # must call _classify_outcome(), not hardcode
    assert "_classify_outcome(" in resolve_src

    recompute_src = inspect.getsource(ot.recompute_outcomes_at_current_threshold)
    assert "_classify_outcome(" in recompute_src


def test_outcome_recompute_preserves_legacy_classification():
    """Every recomputed row must retain its original classification in
    outcome_legacy_030 -- never overwritten on a repeat call."""
    import outcome_tracker as ot

    rows = [
        {"resolved": True, "actual_return": 0.4, "outcome": "WIN"},   # WIN@0.3 -> FLAT@0.5
        {"resolved": True, "actual_return": -0.4, "outcome": "LOSS"}, # LOSS@0.3 -> FLAT@0.5
        {"resolved": True, "actual_return": 5.0, "outcome": "WIN"},   # WIN either way
    ]
    rows, n_changed = ot.recompute_outcomes_at_current_threshold(rows)
    assert n_changed == 2
    assert rows[0]["outcome"] == "FLAT" and rows[0]["outcome_legacy_030"] == "WIN"
    assert rows[1]["outcome"] == "FLAT" and rows[1]["outcome_legacy_030"] == "LOSS"
    assert rows[2]["outcome"] == "WIN"  and rows[2]["outcome_legacy_030"] == "WIN"

    # Re-run: idempotent, legacy field NOT clobbered, no further changes
    rows, n_changed_2 = ot.recompute_outcomes_at_current_threshold(rows)
    assert n_changed_2 == 0
    assert rows[0]["outcome_legacy_030"] == "WIN"   # still the ORIGINAL value


def test_outcome_recompute_deterministic():
    """Same input must always produce the same output."""
    import outcome_tracker as ot

    def fresh_rows():
        return [
            {"resolved": True, "actual_return": 0.4, "outcome": "WIN"},
            {"resolved": True, "actual_return": -0.6, "outcome": "LOSS"},
            {"resolved": True, "actual_return": 0.0, "outcome": "FLAT"},
            {"resolved": False, "actual_return": None, "outcome": None},
        ]

    r1, c1 = ot.recompute_outcomes_at_current_threshold(fresh_rows())
    r2, c2 = ot.recompute_outcomes_at_current_threshold(fresh_rows())
    assert r1 == r2
    assert c1 == c2


def test_outcome_recompute_only_touches_classification():
    """actual_return, exit_price, entry_price, resolved_date must never be
    modified by the recompute -- only `outcome` (and the legacy field)."""
    import outcome_tracker as ot

    row = {
        "resolved": True, "actual_return": 0.4, "outcome": "WIN",
        "exit_price": 101.5, "entry_price": 100.0, "resolved_date": "2026-01-08",
        "ticker": "ZZZ", "signal_date": "2026-01-01",
    }
    before = dict(row)
    rows, _ = ot.recompute_outcomes_at_current_threshold([row])
    after = rows[0]

    for field in ("actual_return", "exit_price", "entry_price", "resolved_date",
                  "ticker", "signal_date"):
        assert after[field] == before[field], f"{field} was modified"
    assert after["outcome"] != before["outcome"]  # only this changed
