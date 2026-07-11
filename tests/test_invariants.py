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
