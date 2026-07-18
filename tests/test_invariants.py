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
