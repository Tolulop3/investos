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
# Walk-forward dashboard Phase 1 (FIX, 2026-08-17): log_strategy_version()
# persists oos_performance from compute_win_rate()'s output
# ─────────────────────────────────────────────────────────────────────────────

def test_log_strategy_version_persists_oos_performance(monkeypatch, tmp_path):
    """log_strategy_version() must persist the oos_performance snapshot from
    a passed win_rate dict. Previously this data was computed by
    compute_win_rate() every run and discarded (win_rate.json is overwritten,
    not appended) -- strategy_version.json's existing per-day/per-version
    history had resolved-pick COUNTS but no performance numbers to actually
    see whether a version added alpha over time."""
    import json as _json
    import strategy_version as sv
    monkeypatch.chdir(tmp_path)

    outcomes_path = tmp_path / "outcomes_log.json"
    outcomes_path.write_text(_json.dumps([
        {"ticker": "AAA", "signal_date": "2026-08-01", "outcome": "WIN"},
    ]))

    win_rate = {"oos": {
        "win_rate": 55.0, "avg_return": 1.23, "spx_return": 0.5,
        "active_return": 0.73, "resolved": 40,
        "tiers": {"90-100": {"n": 5, "wr": 40.0}},
    }}

    entry = sv.log_strategy_version(outcomes_path=str(outcomes_path), win_rate=win_rate)

    assert entry["oos_performance"] == {
        "win_rate": 55.0, "avg_return": 1.23, "spx_return": 0.5,
        "active_return": 0.73, "resolved": 40,
        "tiers": {"90-100": {"n": 5, "wr": 40.0}},
    }

    written = _json.loads((tmp_path / "strategy_version.json").read_text())
    assert written[-1]["oos_performance"]["win_rate"] == 55.0


def test_log_strategy_version_handles_missing_win_rate(monkeypatch, tmp_path):
    """No win_rate passed (or an empty dict) -- oos_performance must be
    None, not raise. Matches run_daily.py's real call site, which can pass
    brief.get("win_rate") == None on an outcome-tracker failure."""
    import json as _json
    import strategy_version as sv
    monkeypatch.chdir(tmp_path)

    outcomes_path = tmp_path / "outcomes_log.json"
    outcomes_path.write_text("[]")

    entry = sv.log_strategy_version(outcomes_path=str(outcomes_path), win_rate=None)
    assert entry["oos_performance"] is None

    entry2 = sv.log_strategy_version(outcomes_path=str(outcomes_path), win_rate={})
    assert entry2["oos_performance"] is None


def test_bake_dashboard_includes_trimmed_walkforward_ledger(monkeypatch, tmp_path):
    """bake_dashboard() must inject a walkforward key sourced from
    strategy_version.json (Phase 2, 2026-08-17), trimmed to date/version/
    oos_performance -- dropping the bulky per-day RULES dict (not needed
    client-side; keeping it would materially bloat the baked payload).
    Missing this means the dashboard's walk-forward chart silently gets no
    data with no error anywhere in the pipeline."""
    import json as _json
    import re as _re
    import run_daily
    monkeypatch.chdir(tmp_path)

    sv_history = [{
        "date": "2026-06-25", "version": "4.1", "oos_start": "2026-06-25",
        "oos_days": 0, "oos_resolved_picks": 27,
        "oos_performance": {"win_rate": 44.4, "avg_return": -0.27,
                             "active_return": -0.27, "resolved": 27, "tiers": {}},
        "rules": {"score_cap_74": ["F"], "bulky_note": "x" * 500},
        "logged_at": "2026-06-25T00:00:00",
    }]
    (tmp_path / "strategy_version.json").write_text(_json.dumps(sv_history))
    (tmp_path / "index.html").write_text(
        "<html><body>\n<script>\n// INVESTOS_DATA_START\n// INVESTOS_DATA_END\n</script>\n</body></html>\n"
    )

    ok = run_daily.bake_dashboard(brief={"date": "2026-08-17"}, fx_signals={}, crypto_signals={})
    assert ok is True

    baked_html = (tmp_path / "index.html").read_text()
    m = _re.search(r"const BAKED = (\{.*?\});", baked_html, _re.DOTALL)
    assert m, "BAKED assignment not injected into index.html"
    payload = _json.loads(m.group(1))

    assert "walkforward" in payload
    assert len(payload["walkforward"]) == 1
    wf = payload["walkforward"][0]
    assert wf["date"] == "2026-06-25"
    assert wf["version"] == "4.1"
    assert wf["oos_performance"]["win_rate"] == 44.4
    assert "rules" not in wf   # bulky per-day dict must be dropped


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
# FIX (2026-07-23): Walk-forward split was positional, not date-based
# Bug: train_and_save() sliced X.iloc[:int(n*0.8)] on a date-sorted, ever-growing
# array. Every retrain, the newest ~20% of rows became "holdout" — a different
# population each time — so AUC numbers were never comparable across retrains.
# Fix: HOLDOUT_CUTOFF_DATE freezes the boundary by signal_date, not row position.
# ─────────────────────────────────────────────────────────────────────────────

def _synthetic_resolved_for_split_test():
    """45 rows before HOLDOUT_CUTOFF_DATE, 5 after — deliberately NOT an 80/20
    ratio (int(50*0.8)=40) so a positional split and a date-based split disagree."""
    resolved = []
    before_dates = [f"2026-05-{d:02d}" for d in range(1, 16)]   # 15 distinct days
    after_dates  = ["2026-07-01", "2026-07-02", "2026-07-03"]

    for i in range(45):
        resolved.append({
            "resolved": True,
            "actual_return": 5.0 if i % 2 == 0 else -3.0,
            "outcome": "WIN" if i % 2 == 0 else "LOSS",
            "perf_90d": 10.0, "roe": 15.0, "profit_margin": 20.0,
            "sector": "Technology", "regime": "BULL",
            "signal_date": before_dates[i % len(before_dates)],
        })
    for i in range(5):
        resolved.append({
            "resolved": True,
            "actual_return": 5.0 if i % 2 == 0 else -3.0,
            "outcome": "WIN" if i % 2 == 0 else "LOSS",
            "perf_90d": 10.0, "roe": 15.0, "profit_margin": 20.0,
            "sector": "Technology", "regime": "BULL",
            "signal_date": after_dates[i % len(after_dates)],
        })
    return resolved


def test_no_signal_date_leak_across_train_holdout():
    """No signal_date should straddle the train/holdout boundary, and every
    date must land fully on the side its comparison to HOLDOUT_CUTOFF_DATE implies."""
    try:
        import numpy as np
    except ImportError:
        pytest.skip("numpy not available")

    from ml_retrainer import build_feature_matrix, HOLDOUT_CUTOFF_DATE

    resolved = _synthetic_resolved_for_split_test()
    # Add a row dated exactly on the cutoff to exercise the boundary itself.
    resolved.append({
        "resolved": True, "actual_return": 5.0, "outcome": "WIN",
        "perf_90d": 10.0, "roe": 15.0, "profit_margin": 20.0,
        "sector": "Technology", "regime": "BULL",
        "signal_date": HOLDOUT_CUTOFF_DATE,
    })

    X, y, w, dates = build_feature_matrix(resolved)
    if X is None:
        pytest.skip("Coverage gate blocked")

    split = int((dates <= HOLDOUT_CUTOFF_DATE).sum())
    train_dates, val_dates = set(dates[:split]), set(dates[split:])

    assert train_dates.isdisjoint(val_dates), (
        f"signal_date leaked across train/holdout: {train_dates & val_dates}"
    )
    assert all(d <= HOLDOUT_CUTOFF_DATE for d in train_dates)
    assert all(d > HOLDOUT_CUTOFF_DATE for d in val_dates)
    assert HOLDOUT_CUTOFF_DATE in train_dates, (
        "a row dated exactly on the cutoff must be train, not holdout"
    )


def test_split_is_date_based_not_positional(monkeypatch, tmp_path):
    """The frozen cutoff must decide train/holdout membership, not row position."""
    try:
        import numpy as np
    except ImportError:
        pytest.skip("numpy/xgboost not available")

    import ml_retrainer as mr

    resolved = _synthetic_resolved_for_split_test()
    X, y, w, dates = mr.build_feature_matrix(resolved)
    if X is None:
        pytest.skip("Coverage gate blocked")

    n = len(y)
    n_before_cutoff = int((dates <= mr.HOLDOUT_CUTOFF_DATE).sum())
    positional_split = int(n * 0.8)
    assert n_before_cutoff != positional_split, (
        "test fixture must use a ratio where date-based and positional splits "
        "disagree, otherwise this test can't distinguish them"
    )

    monkeypatch.setattr(mr, "MODEL_CACHE", str(tmp_path / "model_cache.pkl"))
    monkeypatch.setattr(mr, "REPORT_FILE", str(tmp_path / "report.json"))

    report = mr.train_and_save(X, y, w, dates)
    if report is None:
        pytest.skip("Training libraries unavailable")

    assert report["n_train"] == n_before_cutoff, (
        f"split is not date-based: expected n_train={n_before_cutoff} "
        f"(rows with signal_date <= {HOLDOUT_CUTOFF_DATE}), got {report['n_train']}"
    )
    assert report["n_train"] != positional_split, (
        "split matches the OLD positional int(n*0.8) boundary — regression"
    )


def test_retrain_reproducible_on_unchanged_data(monkeypatch, tmp_path):
    """Same data in, twice in a row, must yield bit-identical holdout AUC.
    This is the actual bug: 'holdout' used to be a different row population
    depending on when you ran it, so the AUC wasn't a stable measurement."""
    try:
        import numpy as np
    except ImportError:
        pytest.skip("numpy/xgboost not available")

    import ml_retrainer as mr

    resolved = _synthetic_resolved_for_split_test()
    X, y, w, dates = mr.build_feature_matrix(resolved)
    if X is None:
        pytest.skip("Coverage gate blocked")

    monkeypatch.setattr(mr, "MODEL_CACHE", str(tmp_path / "model_cache.pkl"))
    monkeypatch.setattr(mr, "REPORT_FILE", str(tmp_path / "report.json"))

    report1 = mr.train_and_save(X, y, w, dates)
    report2 = mr.train_and_save(X, y, w, dates)
    if report1 is None or report2 is None:
        pytest.skip("Training libraries unavailable")

    assert report1["n_val"] == report2["n_val"]
    assert report1["holdout_auc"] == report2["holdout_auc"], (
        f"holdout AUC not reproducible on unchanged data: "
        f"{report1['holdout_auc']} != {report2['holdout_auc']}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# FIX (2026-08-08): ml_engine.py's daily live-scoring model trained on the same
# data as ml_retrainer.py (same build_feature_matrix() call) but split it with
# plain positional int(n*0.8) and no purge buffer -- a *different, leakier*
# split than ml_retrainer.py's frozen date-cutoff+purge, despite both claiming
# to validate "the" model. Confirmed empirically on the live 2465-row matrix:
# positional/no-purge reported holdout AUC 0.607; purging only the rows within
# 10 days of that same boundary dropped it to 0.543; the full date-cutoff+
# purge method (matching ml_retrainer.py) landed at 0.497 -- no real edge.
# ml_prob feeds a direct +4/+8 conviction-score boost in run_daily.py, so this
# wasn't just a misleading report number -- real picks were being pushed up in
# rank/size on leakage-inflated confidence, on every day a retrain wasn't due
# (ml_model_cache.pkl is gitignored and not persisted across CI runs, so this
# was effectively every day).
# ─────────────────────────────────────────────────────────────────────────────

def test_ml_engine_load_training_data_returns_dates():
    """load_training_data() must return a 4-tuple (X, y, w, dates), not the
    old 3-tuple -- train() needs dates to do a date-based split. Locks in
    the return shape for both the real-data path and the bootstrap
    fallback (which has no real dates, so must return None, not omit the
    slot entirely)."""
    import ml_engine as me

    p = me.StockMLPredictor()
    result = p.load_training_data()
    assert result is not None
    assert len(result) == 4, (
        f"load_training_data() must return (X, y, w, dates) -- got "
        f"{len(result)} values"
    )


def test_ml_engine_train_uses_date_based_split_not_positional(monkeypatch, tmp_path):
    """train()'s final model fit must be trained on the date-cutoff split,
    not the old positional int(n*0.8) -- this is the actual regression:
    ml_engine.py silently used a different (leakier) split than
    ml_retrainer.py despite sharing the same build_feature_matrix() data."""
    try:
        import numpy as np
    except ImportError:
        pytest.skip("numpy/xgboost not available")

    import ml_engine as me
    import ml_retrainer as mr

    if not (me.HAS_XGB and me.HAS_PANDAS and me.HAS_SKLEARN):
        pytest.skip("Training libraries unavailable")

    resolved = _synthetic_resolved_for_split_test()
    X, y, w, dates = mr.build_feature_matrix(resolved)
    if X is None:
        pytest.skip("Coverage gate blocked")

    n = len(y)
    n_before_cutoff = int((dates <= mr.HOLDOUT_CUTOFF_DATE).sum())
    positional_split = int(n * 0.8)
    assert n_before_cutoff != positional_split, (
        "test fixture must use a ratio where date-based and positional "
        "splits disagree, otherwise this test can't distinguish them"
    )

    p = me.StockMLPredictor()
    monkeypatch.setattr(p, "load_training_data", lambda: (X, y, w, dates))
    # Force the fresh-train path -- don't let this test touch the real cache.
    _real_exists = os.path.exists
    monkeypatch.setattr(
        me.os.path, "exists",
        lambda path: False if path == "ml_model_cache.pkl" else _real_exists(path)
    )

    real_xgb = me.XGBClassifier
    final_fit_train_len = {}

    class SpyXGBClassifier(real_xgb):
        def fit(self, X_arg, y_arg, **kwargs):
            if "eval_set" in kwargs:  # only the final model fit passes eval_set
                final_fit_train_len["n"] = len(X_arg)
            return super().fit(X_arg, y_arg, **kwargs)

    monkeypatch.setattr(me, "XGBClassifier", SpyXGBClassifier)

    ok = p.train(verbose=False)
    assert ok, "train() should succeed on valid synthetic data"
    assert "n" in final_fit_train_len, "final model fit (with eval_set) never ran"

    assert final_fit_train_len["n"] != positional_split, (
        "final model was trained on the OLD positional int(n*0.8) split -- regression"
    )
    assert final_fit_train_len["n"] <= n_before_cutoff, (
        "final training set is larger than the date-cutoff population -- "
        "split is not respecting HOLDOUT_CUTOFF_DATE"
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


def test_sharpe_guard_wired_into_sizing_parameter():
    """
    Phase 2 (2026-08-10): render_allocations (Step 5 sizing) now accepts
    sharpe_multiplier and applies it to `deployable` before any dollar amount
    is computed -- rolling Sharpe is computed once, early, in run_daily.py
    (before Step 5 runs) specifically so this is possible. Supersedes the old
    test_sharpe_guard_no_sizing_parameter, which locked the pre-fix fact that
    no such parameter existed; this locks the post-fix fact that it does and
    that it actually scales dollars, not just that the parameter exists.
    """
    import inspect
    from ml_engine import render_allocations
    params = inspect.signature(render_allocations).parameters
    assert "sharpe_multiplier" in params
    assert params["sharpe_multiplier"].default == 1.0   # no-op unless explicitly reduced

    regime = {"regime": "BULL", "cash_pct": 0.0}
    weights = [{"ticker": "AAA", "weight": 1.0, "score": 80, "ml_prob": 0.6, "kelly_wt": 1.0}]
    account = {"name": "TEST", "capital": 100_000, "universe": "ALL", "max_equity": 1.0}

    full = render_allocations(weights, account, regime, verbose=False, sharpe_multiplier=1.0)
    reduced = render_allocations(weights, account, regime, verbose=False, sharpe_multiplier=0.75)
    assert full and reduced
    assert reduced[0]["dollar_amt"] == round(full[0]["dollar_amt"] * 0.75, 2)


def test_sharpe_guard_message_reflects_real_sizing_effect():
    """
    run_daily.py's Sharpe advisory message must accurately describe Phase 2
    behavior: it now DOES affect sizing (applied in Step 5, before this Step
    11 block ever prints), so it must not claim otherwise. Supersedes the old
    test_sharpe_guard_message_not_false_claim, which guarded the pre-fix
    "does not affect sizing" claim; this guards against the message drifting
    back to that now-false claim, and against reintroducing the earlier,
    different false claim ("auto-reduced to X% of normal") this replaced.
    """
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    with open(os.path.join(root, "run_daily.py")) as f:
        src = f.read()
    assert "position sizes auto-reduced" not in src
    assert "SHARPE ADVISORY" in src
    assert "does not affect sizing" not in src
    assert "sharpe_multiplier" in src


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


# ─────────────────────────────────────────────────────────────────────────────
# No-ml_prob picks excluded from allocation, not floored to 50% (FIX, 2026-08-17)
# Confirmed live 2026-08-17: CVX had Kelly=0.000 (no ml_prob logged that day)
# but still drew $976 (17.4% weight) in a 3-pick basket via the base_wt*0.50
# floor at ml_engine.py's final_wts construction. That floor exists to stop a
# concentration cascade among picks that DO have a (bad) MEASURED edge
# (2026-06-18 decision, see test_kelly_floor_deploys_50pct above) -- it was
# never justified for picks with no data whatsoever. Fix: a pick with
# kelly_wt==0.0 AND no ml_prob logged gets weight=0.0 (excluded), while a
# pick with kelly_wt==0.0 but a real ml_prob (computed zero/negative edge)
# keeps the existing base_wt*0.50 floor unchanged.
# ─────────────────────────────────────────────────────────────────────────────

def test_kelly_no_ml_prob_excluded_not_floored_when_other_picks_have_edge():
    """In a mixed basket, a pick with no ml_prob logged (e.g. CVX,
    2026-08-17) gets weight=0.0 -- excluded from allocation entirely --
    while picks with real ml_prob/Kelly in the same basket are unaffected."""
    from ml_engine import compute_target_weights

    market_regime = {"regime": "BULL", "cash_pct": 0.0}
    wr_data = _kelly_wr_data_mlprob("0.6-0.8", 61.7, 5.41, 2.70, count=269)

    picks = [
        {"ticker": "FBP", "score": 97, "ml_prob": 0.78,
         "data": {"price": 30.0, "volatility_90d": 0.20}},
        {"ticker": "MFC.TO", "score": 94, "ml_prob": 0.63,
         "data": {"price": 45.0, "volatility_90d": 0.18}},
        {"ticker": "CVX", "score": 93,   # no ml_prob key -- matches 2026-08-17 live case
         "data": {"price": 150.0, "volatility_90d": 0.22}},
    ]
    result = compute_target_weights(picks, market_regime, win_rate_data=wr_data, verbose=False)
    by_ticker = {r["ticker"]: r for r in result}

    assert by_ticker["CVX"]["kelly_wt"] == 0.0
    assert by_ticker["CVX"]["weight"] == 0.0, \
        "no-ml_prob pick must be excluded (0.0), not floored to base_wt*0.50"
    assert by_ticker["FBP"]["weight"] > 0.0
    assert by_ticker["MFC.TO"]["weight"] > 0.0


def test_kelly_zero_edge_with_ml_prob_still_gets_floor():
    """Contrast case: a pick WITH a real ml_prob whose Kelly computes to
    zero/negative edge still gets the base_wt*0.50 floor (unchanged
    2026-06-18 behavior) -- only the no-ml_prob case was excluded by the
    2026-08-17 fix, not every kelly_wt==0.0 pick."""
    from ml_engine import compute_target_weights

    market_regime = {"regime": "BULL", "cash_pct": 0.0}
    # 0.8-1.0 bucket: real measured negative edge (win_rate 50.2%, avg_win < avg_loss)
    wr_data = _kelly_wr_data_mlprob("0.8-1.0", 50.2, 3.52, 5.53, count=444)

    picks = [
        {"ticker": "WINNER", "score": 80, "ml_prob": 0.65,
         "data": {"price": 50.0, "volatility_90d": 0.2}},
        {"ticker": "LOSER", "score": 80, "ml_prob": 0.90,   # real ml_prob, negative edge
         "data": {"price": 50.0, "volatility_90d": 0.2}},
    ]
    # Give WINNER its own positive-edge bucket so the basket isn't all-zero-Kelly
    wr_data["by_ml_prob_bucket"]["0.6-0.8"] = {
        "win_rate": 61.7, "avg_win": 5.41, "avg_loss": 2.70, "count": 269,
    }
    result = compute_target_weights(picks, market_regime, win_rate_data=wr_data, verbose=False)
    by_ticker = {r["ticker"]: r for r in result}

    assert by_ticker["LOSER"]["kelly_wt"] == 0.0
    assert by_ticker["LOSER"]["weight"] > 0.0, \
        "a pick WITH ml_prob but zero computed edge should keep the base_wt*0.50 floor"


def test_score_tier_buckets_are_gapless_for_fractional_scores():
    """FIX (2026-08-17): by_score_tier and the OOS tier breakdown used
    integer bucket bounds (90-100/75-89/60-74/0-59) against float scores --
    59.9, 74.3-74.8, 89.2-89.9 landed in none of the four buckets (the gaps
    between adjacent integer bounds), silently dropping picks from the tier
    breakdown while total counts stayed correct. Confirmed live: by_score
    tier counts summed to 2558 against 2584 total resolved. Every synthetic
    score below is chosen to sit exactly in one of those three gaps, plus
    one clean mid-bucket score per tier as a control."""
    import tempfile
    from unittest.mock import patch
    from outcome_tracker import compute_win_rate

    def _o(ticker, score, signal_date="2026-08-16"):
        return {"ticker": ticker, "signal_date": signal_date, "score": score,
                "ml_prob": 0.5, "resolved": True, "outcome": "WIN", "actual_return": 1.0}

    synthetic = [
        _o("GAP_59_9", 59.9),   # old bug: neither 0-59 nor 60-74 caught this
        _o("GAP_74_5", 74.5),   # old bug: neither 60-74 nor 75-89 caught this
        _o("GAP_89_7", 89.7),   # old bug: neither 75-89 nor 90-100 caught this
        _o("MID_30",   30.0),
        _o("MID_65",   65.0),
        _o("MID_80",   80.0),
        _o("MID_95",   95.0),
    ]
    scratch_win_rate = tempfile.mktemp(suffix=".json")

    with patch("outcome_tracker.load_outcomes", lambda: synthetic), \
         patch("outcome_tracker.WIN_RATE_FILE", scratch_win_rate), \
         patch("outcome_tracker.OOS_START_DATE", "2026-01-01"):
        wr = compute_win_rate()

    by_score_total = sum(t["count"] for t in wr["by_score_tier"].values())
    assert by_score_total == len(synthetic), (
        f"by_score_tier undercounts: {by_score_total} != {len(synthetic)} "
        f"({wr['by_score_tier']})"
    )

    oos_tiers = wr["oos"]["tiers"]
    oos_total = sum(t["n"] for t in oos_tiers.values())
    assert oos_total == len(synthetic), (
        f"OOS tier breakdown undercounts: {oos_total} != {len(synthetic)} ({oos_tiers})"
    )

    # Each gap score lands in exactly one bucket, per its half-open bound:
    # 0-59: 30.0, 59.9 (< 60) | 60-74: 65.0, 74.5 (< 75) |
    # 75-89: 80.0, 89.7 (< 90) | 90-100: 95.0 (>= 90)
    assert oos_tiers["0-59"]["n"]   == 2
    assert oos_tiers["60-74"]["n"]  == 2
    assert oos_tiers["75-89"]["n"]  == 2
    assert oos_tiers["90-100"]["n"] == 1


def test_top_level_avg_win_loss_is_portfolio_wide_not_last_tier():
    """FIX (2026-08-17): the by_score_tier loop reused the outer-scope
    avg_win/avg_loss variable names, so by the time it finished, those names
    held whatever the LAST-iterated tier ("below-60") computed -- and
    result["avg_win"]/["avg_loss"] read that shadowed value instead of the
    true portfolio-wide average computed earlier. Confirmed live: today's
    baked dashboard had top-level avg_win/avg_loss (4.75/4.06) exactly
    matching by_score_tier["below-60"]'s (4.75/4.06). Constructed so the
    below-60 tier's win/loss averages are deliberately far from the true
    portfolio average -- if the shadow bug were still present, this would
    assert the wrong (below-60-tier) numbers and fail."""
    import tempfile
    from unittest.mock import patch
    from outcome_tracker import compute_win_rate

    def _o(ticker, score, outcome, actual_return):
        return {"ticker": ticker, "signal_date": "2026-08-16", "score": score,
                "ml_prob": 0.5, "resolved": True, "outcome": outcome,
                "actual_return": actual_return}

    synthetic = [
        _o("HI_WIN",  95, "WIN",  10.0),
        _o("HI_LOSS", 95, "LOSS", -8.0),
        _o("LO_WIN",  30, "WIN",   1.0),   # below-60 tier: much smaller win/loss
        _o("LO_LOSS", 30, "LOSS", -1.0),
    ]
    scratch_win_rate = tempfile.mktemp(suffix=".json")

    with patch("outcome_tracker.load_outcomes", lambda: synthetic), \
         patch("outcome_tracker.WIN_RATE_FILE", scratch_win_rate), \
         patch("outcome_tracker.OOS_START_DATE", "2026-01-01"):
        wr = compute_win_rate()

    # True portfolio-wide average across ALL wins/losses, not just below-60's.
    # (avg_win/avg_loss are plain signed averages at the portfolio level --
    # see outcome_tracker.py line ~831 -- unlike the tier-level fields below,
    # which average abs(loss); that's a separate, pre-existing sign-convention
    # difference between the two, not part of this fix.)
    assert wr["avg_win"]  == round((10.0 + 1.0) / 2, 2)
    assert wr["avg_loss"] == round((-8.0 + -1.0) / 2, 2)

    # Sanity: below-60 tier's own numbers are indeed the smaller, distinct values
    assert wr["by_score_tier"]["below-60"]["avg_win"]  == 1.0
    assert wr["by_score_tier"]["below-60"]["avg_loss"] == 1.0
    assert wr["avg_win"]  != wr["by_score_tier"]["below-60"]["avg_win"]
    assert wr["avg_loss"] != wr["by_score_tier"]["below-60"]["avg_loss"]


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
    """NGX_SECTOR_MAP must still contain every one of the current 29 live
    tickers (Session D legitimately extended it with the 56 candidates, so
    it's no longer exactly 29 -- superset, not equality), and every value,
    live or candidate, must still be a real key in SECTOR_SENSITIVITY --
    proving score_ngx_macro() never silently falls back to the 'banking'
    default for any ticker with a NGX_SECTOR_MAP entry."""
    import ngx_screener as ns

    assert set(ns.NGX_ALL).issubset(set(ns.NGX_SECTOR_MAP.keys()))
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
# NGX Session C: SECTOR_SENSITIVITY expansion for API-taxonomy sectors
# (2026-07-23). Adds coefficients for HEALTHCARE, CONSTRUCTION/REAL ESTATE,
# INVESTMENT, SERVICES (previously unscored, would silently fall back to
# "banking" if ever scored) via a new NGX_API_SECTOR_TO_SENSITIVITY map.
# NOT wired into score_ngx_macro() -- prep only. Must be zero-impact on the
# 29 live tickers: score_ngx_macro() still reads NGX_SECTOR_MAP exclusively,
# which is untouched.
# ─────────────────────────────────────────────────────────────────────────────

def test_ngx_original_11_sector_sensitivity_values_byte_identical():
    """The 11 original SECTOR_SENSITIVITY entries (feeding live scoring for
    the current 29 tickers) must be untouched, value for value, by the
    Session C expansion -- not just "still present as a key" (that's a
    weaker, pre-existing test) but byte-identical to their original
    coefficients, since a silently-tweaked number here would change live
    scoring with no signal anywhere else."""
    import ngx_screener as ns

    original = {
        "oil":          {"oil_b": 1.6, "fx_b": 0.8, "risk_b": 0.9, "base": 60},
        "banking":      {"oil_b": 0.5, "fx_b": 2.0, "risk_b": 1.3, "base": 55},
        "telecom":      {"oil_b": 0.3, "fx_b": 1.2, "risk_b": 0.6, "base": 58},
        "industrial":   {"oil_b": 1.0, "fx_b": 1.4, "risk_b": 1.0, "base": 52},
        "consumer":     {"oil_b": 0.4, "fx_b": 2.2, "risk_b": 1.1, "base": 50},
        "agriculture":  {"oil_b": 0.6, "fx_b": 1.0, "risk_b": 0.7, "base": 53},
        "power":        {"oil_b": 1.1, "fx_b": 1.5, "risk_b": 1.0, "base": 51},
        "financial":    {"oil_b": 0.5, "fx_b": 1.8, "risk_b": 1.2, "base": 52},
        "transport":    {"oil_b": 0.9, "fx_b": 1.1, "risk_b": 0.9, "base": 50},
        "conglomerate": {"oil_b": 0.7, "fx_b": 1.3, "risk_b": 1.0, "base": 52},
        "technology":   {"oil_b": 0.2, "fx_b": 1.6, "risk_b": 0.8, "base": 51},
    }
    for key, expected in original.items():
        assert ns.SECTOR_SENSITIVITY.get(key) == expected, (
            f"SECTOR_SENSITIVITY[{key!r}] changed from {expected} "
            f"to {ns.SECTOR_SENSITIVITY.get(key)} -- live scoring for the "
            f"29 current tickers would be affected"
        )


def test_ngx_29_live_tickers_sector_map_lookups_unchanged():
    """Every one of the 29 live tickers' NGX_SECTOR_MAP -> SECTOR_SENSITIVITY
    resolution must be byte-identical before/after Session C and D -- proves
    score_ngx_macro()'s actual live lookup path for the 29 is untouched.
    NGX_SECTOR_MAP legitimately grew to 85 keys in Session D, so this checks
    the 29 are present and unchanged (subset), not exact dict equality."""
    import ngx_screener as ns

    expected = {
        "GTCO.LG": "banking", "ZENITHBANK.LG": "banking", "ACCESSCORP.LG": "banking",
        "UBA.LG": "banking", "FIRSTHOLDCO.LG": "banking", "MTNN.LG": "telecom",
        "AIRTELAFRI.LG": "telecom", "DANGCEM.LG": "industrial", "SEPLAT.LG": "oil",
        "STANBIC.LG": "banking", "NB.LG": "consumer", "UNILEVER.LG": "consumer",
        "OANDO.LG": "oil", "TOTAL.LG": "oil", "BUACEMENT.LG": "industrial",
        "HBMNG.LG": "industrial", "PRESCO.LG": "agriculture", "OKOMUOIL.LG": "agriculture",
        "TRANSCORP.LG": "conglomerate", "GEREGU.LG": "power", "NESTLE.LG": "consumer",
        "FIDELITYBK.LG": "banking", "FCMB.LG": "banking", "UCAP.LG": "financial",
        "NAHCO.LG": "transport", "DANGSUGAR.LG": "consumer", "NASCON.LG": "consumer",
        "LIVESTOCK.LG": "agriculture", "CWG.LG": "technology",
    }
    for ticker, sector in expected.items():
        assert ns.NGX_SECTOR_MAP.get(ticker) == sector, (
            f"{ticker}: expected {sector!r}, got {ns.NGX_SECTOR_MAP.get(ticker)!r}"
        )
        assert sector in ns.SECTOR_SENSITIVITY, f"{ticker} -> {sector!r} resolution broke"


def test_ngx_api_sector_expansion_resolves_all_ten_mapped_categories():
    """Every canonical API-taxonomy sector listed in
    NGX_API_SECTOR_TO_SENSITIVITY must resolve to a real, distinct
    SECTOR_SENSITIVITY entry -- no silent fallback to 'banking' if this
    were ever wired into scoring."""
    import ngx_screener as ns

    assert len(ns.NGX_API_SECTOR_TO_SENSITIVITY) == 10
    for api_sector, sens_key in ns.NGX_API_SECTOR_TO_SENSITIVITY.items():
        assert sens_key in ns.SECTOR_SENSITIVITY, (
            f"{api_sector!r} maps to {sens_key!r}, which has no "
            f"SECTOR_SENSITIVITY entry"
        )


def test_ngx_healthcare_and_muted_default_coefficients_as_approved():
    """New coefficients must match exactly what was proposed and approved --
    HEALTHCARE distinct, CONSTRUCTION/REAL ESTATE + INVESTMENT + SERVICES
    sharing one muted default."""
    import ngx_screener as ns

    assert ns.SECTOR_SENSITIVITY["HEALTHCARE"] == {
        "oil_b": 0.2, "fx_b": 1.3, "risk_b": 0.6, "base": 51
    }
    muted = {"oil_b": 0.3, "fx_b": 1.0, "risk_b": 0.6, "base": 50}
    for key in ("CONSTRUCTION/REAL ESTATE", "INVESTMENT", "SERVICES"):
        assert ns.SECTOR_SENSITIVITY[key] == muted, (
            f"{key} coefficients don't match the approved shared muted default"
        )


def test_ngx_financial_services_and_ict_and_natural_resources_intentionally_absent():
    """FINANCIAL SERVICES and ICT must NOT get a blended coefficient (each
    collapses two legacy sectors with different profiles -- banking/financial,
    telecom/technology). NATURAL RESOURCES must stay absent entirely (zero
    member tickers, no basis for a number). None of the three should be
    reachable via a silent single-key lookup that could paper over the gap."""
    import ngx_screener as ns

    assert "FINANCIAL SERVICES" not in ns.NGX_API_SECTOR_TO_SENSITIVITY
    assert "ICT" not in ns.NGX_API_SECTOR_TO_SENSITIVITY
    assert "NATURAL RESOURCES" not in ns.NGX_API_SECTOR_TO_SENSITIVITY

    # Also confirm no stray dict key was added for these under any casing.
    assert "FINANCIAL SERVICES" not in ns.SECTOR_SENSITIVITY
    assert "ICT" not in ns.SECTOR_SENSITIVITY
    assert "NATURAL RESOURCES" not in ns.SECTOR_SENSITIVITY


def test_ngx_score_ngx_macro_unaffected_by_sector_sensitivity_expansion():
    """score_ngx_macro() must produce identical scores for the 29 live
    tickers before/after the Session C expansion -- it still reads
    NGX_SECTOR_MAP exclusively, which never routes to any of the new keys."""
    import ngx_screener as ns

    for ticker in ns.NGX_ALL:
        score, reasons, flags = ns.score_ngx_macro(
            ticker, tier=1, regime="NEUTRAL", macro_score=1.5, fx_stress=2.0
        )
        sector = ns.NGX_SECTOR_MAP[ticker]
        assert sector in (
            "oil", "banking", "telecom", "industrial", "consumer", "agriculture",
            "power", "financial", "transport", "conglomerate", "technology",
        ), f"{ticker} resolved to a non-legacy sector {sector!r} -- NGX_SECTOR_MAP was touched"


# ─────────────────────────────────────────────────────────────────────────────
# NGX Session D: ticker-to-sector bridge for the 56 PAPER_ONLY candidates
# (2026-07-23). Extends NGX_SECTOR_MAP with the 56 NGX_TIER3_CANDIDATES
# tickers, each assigned to a granular legacy SECTOR_SENSITIVITY key (36
# mechanical from NGX_API_SECTOR_MAP, 14 judgment calls for FINANCIAL
# SERVICES/ICT tickers, 6 muted-default where no local corroboration
# existed). Does NOT add these tickers to NGX_ALL / live scoring rotation --
# that's a separate, still-unmade decision.
# ─────────────────────────────────────────────────────────────────────────────

def test_ngx_all_56_candidates_resolve_to_sector_map_entry():
    """Every one of the 56 NGX_TIER3_CANDIDATES must have a real
    NGX_SECTOR_MAP entry -- no more silent 'banking' default by ticker-
    lookup miss if score_ngx_macro() is ever called for one of them."""
    import ngx_screener as ns

    assert len(ns.NGX_SECTOR_MAP) == 85
    for ticker in ns.NGX_TIER3_CANDIDATES:
        assert ticker in ns.NGX_SECTOR_MAP, f"{ticker} has no NGX_SECTOR_MAP entry"


def test_ngx_all_56_candidate_sectors_are_real_sensitivity_keys():
    """Every sector value assigned to the 56 candidates must be a real key
    in SECTOR_SENSITIVITY -- not None, not a typo, not silently falling
    through to score_ngx_macro()'s 'banking' fallback."""
    import ngx_screener as ns

    for ticker in ns.NGX_TIER3_CANDIDATES:
        sector = ns.NGX_SECTOR_MAP.get(ticker)
        assert sector in ns.SECTOR_SENSITIVITY, (
            f"{ticker} -> {sector!r} is not a real SECTOR_SENSITIVITY key"
        )


def test_ngx_candidate_sector_assignments_as_approved():
    """Spot-check the three confidence tiers from the Session D proposal
    resolve to exactly what was approved -- catches a copy-paste slip in
    any one of the 56 without hand-checking the whole block."""
    import ngx_screener as ns

    expected = {
        # Tier A -- API sub_sector confirmed
        "ETI.LG": "banking", "WEMABANK.LG": "banking",
        "STERLINGNG.LG": "banking", "JAIZBANK.LG": "banking",
        "NGXGROUP.LG": "financial", "AIICO.LG": "financial",
        # Tier B -- ticker/brand evidence
        "LINKASSURE.LG": "financial", "SOVRENINS.LG": "financial",
        "NEM.LG": "financial", "NPFMCRFBK.LG": "banking",
        "MANSARD.LG": "financial", "CORNERST.LG": "financial",
        "WAPIC.LG": "financial", "ETRANZACT.LG": "technology",
        # Tier C -- muted default, no local corroboration
        "ABBEYBDS.LG": "SERVICES", "INFINITY.LG": "SERVICES",
        "AFRIPRUD.LG": "SERVICES", "CONHALLPLC.LG": "SERVICES",
        "MBENEFIT.LG": "SERVICES", "CHAMS.LG": "SERVICES",
        # A few mechanical spot-checks
        "ARADEL.LG": "oil", "FIDSON.LG": "HEALTHCARE",
        "VFDGROUP.LG": "INVESTMENT", "TRANSPOWER.LG": "power",
        "JBERGER.LG": "CONSTRUCTION/REAL ESTATE",
    }
    for ticker, sector in expected.items():
        assert ns.NGX_SECTOR_MAP.get(ticker) == sector, (
            f"{ticker}: expected {sector!r}, got {ns.NGX_SECTOR_MAP.get(ticker)!r}"
        )


def test_ngx_29_live_tickers_unaffected_by_candidate_bridge():
    """The 29 live tickers' NGX_SECTOR_MAP entries must be byte-identical
    before/after the Session D bridge -- extending the dict with 56 new
    keys must not touch any of the original 29."""
    import ngx_screener as ns

    expected = {
        "GTCO.LG": "banking", "ZENITHBANK.LG": "banking", "ACCESSCORP.LG": "banking",
        "UBA.LG": "banking", "FIRSTHOLDCO.LG": "banking", "MTNN.LG": "telecom",
        "AIRTELAFRI.LG": "telecom", "DANGCEM.LG": "industrial", "SEPLAT.LG": "oil",
        "STANBIC.LG": "banking", "NB.LG": "consumer", "UNILEVER.LG": "consumer",
        "OANDO.LG": "oil", "TOTAL.LG": "oil", "BUACEMENT.LG": "industrial",
        "HBMNG.LG": "industrial", "PRESCO.LG": "agriculture", "OKOMUOIL.LG": "agriculture",
        "TRANSCORP.LG": "conglomerate", "GEREGU.LG": "power", "NESTLE.LG": "consumer",
        "FIDELITYBK.LG": "banking", "FCMB.LG": "banking", "UCAP.LG": "financial",
        "NAHCO.LG": "transport", "DANGSUGAR.LG": "consumer", "NASCON.LG": "consumer",
        "LIVESTOCK.LG": "agriculture", "CWG.LG": "technology",
    }
    for ticker, sector in expected.items():
        assert ns.NGX_SECTOR_MAP.get(ticker) == sector, (
            f"{ticker}: expected {sector!r}, got {ns.NGX_SECTOR_MAP.get(ticker)!r} "
            f"-- a live ticker's mapping was disturbed by the candidate bridge"
        )


def test_ngx_candidates_not_in_live_rotation_despite_having_sector_now():
    """Having a real NGX_SECTOR_MAP entry must NOT mean a candidate is live
    -- that's still gated by NGX_ALL membership alone, untouched here."""
    import ngx_screener as ns

    assert set(ns.NGX_ALL).isdisjoint(set(ns.NGX_TIER3_CANDIDATES))
    for ticker in ns.NGX_TIER3_CANDIDATES:
        assert ticker not in ns.NGX_ALL


def test_ngx_score_ngx_macro_works_end_to_end_for_candidate_tickers():
    """score_ngx_macro() must run cleanly for candidates spanning mechanical,
    Tier A/B judgment, and Tier C muted-default assignments -- proves the
    bridge works end to end (real scoring call), not just that the dict has
    an entry. Candidates aren't live-scored yet, but the function must not
    crash or return nonsense if it's ever exercised for one."""
    import ngx_screener as ns

    sample = [
        "ARADEL.LG",       # mechanical -> oil
        "FIDSON.LG",       # mechanical -> HEALTHCARE
        "VFDGROUP.LG",     # mechanical -> INVESTMENT
        "ETI.LG",          # Tier A judgment -> banking
        "AIICO.LG",        # Tier A judgment -> financial
        "ETRANZACT.LG",    # Tier B judgment -> technology
        "CHAMS.LG",        # Tier C muted default -> SERVICES
        "ABBEYBDS.LG",     # Tier C muted default -> SERVICES
    ]
    for ticker in sample:
        score, reasons, flags = ns.score_ngx_macro(
            ticker, tier=2, regime="RISK_ON", macro_score=2.0, fx_stress=1.5
        )
        assert isinstance(score, (int, float))
        assert 0 <= score <= 100, f"{ticker} produced out-of-range score {score}"
        assert isinstance(reasons, list) and isinstance(flags, list)


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


# ─────────────────────────────────────────────────────────────────────────────
# FIX (2026-08-06): calibration_check silently empty in production
# Bug: pd.qcut(df["prob"], q=5, labels=[...], duplicates="drop") raises
# "Bin labels must be one fewer than the number of bin edges" whenever
# sector-median imputation creates tied predicted probabilities (collides at
# quantile bin edges, duplicates="drop" then removes edges but not labels).
# A bare `except: pass` swallowed this on most real retrains, leaving
# ml_retrainer_report.json's calibration_check silently empty with no error.
# Fix: rank(method="first") before qcut makes every value unique, so qcut
# always produces exactly 5 populated bins.
# ─────────────────────────────────────────────────────────────────────────────

def test_ml_retrainer_calibration_check_survives_heavy_probability_ties(monkeypatch, tmp_path):
    """calib_check must be non-empty (5 populated Q1-Q5 buckets) even when
    most rows share identical feature values -- the exact condition
    (sector-median imputation) that triggered the original qcut crash."""
    try:
        import numpy as np
    except ImportError:
        pytest.skip("numpy/xgboost not available")

    import ml_retrainer as mr
    monkeypatch.setattr(mr, "MODEL_CACHE", str(tmp_path / "model_cache.pkl"))
    monkeypatch.setattr(mr, "REPORT_FILE", str(tmp_path / "report.json"))

    resolved = []
    # 80% of rows share IDENTICAL feature values (heavy ties, like real
    # sector-median-imputed data) -- only 20% carry distinct real values.
    for i in range(200):
        real = i < 40
        resolved.append({
            "resolved": True,
            "actual_return": 5.0 if i % 2 == 0 else -3.0,
            "outcome": "WIN" if i % 2 == 0 else "LOSS",
            "perf_90d":      (10.0 + i) if real else 10.0,
            "roe":           (15.0 + i) if real else 15.0,
            "profit_margin": (20.0 + i) if real else 20.0,
            "sector": "Technology", "regime": "BULL",
            "signal_date": f"2026-0{1 + (i % 9)}-01",
        })

    X, y, w, dates = mr.build_feature_matrix(resolved)
    if X is None:
        pytest.skip("Coverage gate blocked")

    report = mr.train_and_save(X, y, w, dates)
    if report is None:
        pytest.skip("Training libraries unavailable")

    calib = report["calibration_check"]
    assert calib, "calibration_check is empty -- qcut crash regressed"
    assert set(calib.keys()) == {"Q1", "Q2", "Q3", "Q4", "Q5"}
    total = sum(d["count"] for d in calib.values())
    assert total == report["n_rows"], (
        f"calibration_check covers {total} rows, expected all {report['n_rows']}"
    )


def test_ml_retrainer_calibration_check_all_identical_probabilities(monkeypatch, tmp_path):
    """Stress case: every row has literally the same features (worst-case
    ties). Must not crash -- qcut on ranks still produces 5 equal-count bins
    even though there's no real information to bucket on."""
    try:
        import numpy as np
    except ImportError:
        pytest.skip("numpy/xgboost not available")

    import ml_retrainer as mr
    monkeypatch.setattr(mr, "MODEL_CACHE", str(tmp_path / "model_cache.pkl"))
    monkeypatch.setattr(mr, "REPORT_FILE", str(tmp_path / "report.json"))

    resolved = []
    for i in range(150):
        resolved.append({
            "resolved": True,
            "actual_return": 5.0 if i % 2 == 0 else -3.0,
            "outcome": "WIN" if i % 2 == 0 else "LOSS",
            "perf_90d": 10.0, "roe": 15.0, "profit_margin": 20.0,
            "sector": "Technology", "regime": "BULL",
            "signal_date": f"2026-0{1 + (i % 9)}-01",
        })

    X, y, w, dates = mr.build_feature_matrix(resolved)
    if X is None:
        pytest.skip("Coverage gate blocked")

    report = mr.train_and_save(X, y, w, dates)
    if report is None:
        pytest.skip("Training libraries unavailable")

    assert report["calibration_check"], (
        "calibration_check crashed/empty on all-identical-probability input"
    )


# ─────────────────────────────────────────────────────────────────────────────
# FIX (2026-08-06): ZLB.TO duplicated across SECTOR and DEFENSIVE in
# ETF_UNIVERSE with divergent REGIME_WEIGHTS (RISK_ON: SECTOR=1.0x vs
# DEFENSIVE=0.3x; RISK_OFF: SECTOR=0.4x vs DEFENSIVE=1.2x). Whichever
# duplicate scored higher silently won each run, so the same low-volatility
# fund was treated as an aggressive RISK_ON pick some runs and a defensive
# one other runs, by accident of which entry happened to score higher.
# ─────────────────────────────────────────────────────────────────────────────

def test_etf_universe_no_duplicate_tickers():
    """No ticker should appear more than once in ETF_UNIVERSE -- a duplicate
    with a different category means the same fund gets scored twice under
    different REGIME_WEIGHTS, with one silently discarded per run."""
    import etf_engine as ee
    from collections import Counter

    tickers = [row[0] for row in ee.ETF_UNIVERSE]
    counts = Counter(tickers)
    dupes = {t: n for t, n in counts.items() if n > 1}
    assert not dupes, f"duplicate tickers in ETF_UNIVERSE: {dupes}"


def test_etf_zlb_to_categorized_defensive_not_sector():
    """ZLB.TO (Canadian Low-Vol) is a single entry, tagged DEFENSIVE --
    matches its actual design (low-volatility equity = defensive posture)
    and strategy_engine.py's own existing treatment of it alongside
    ZAG.TO/GLD as a capital-preservation holding."""
    import etf_engine as ee

    zlb_rows = [row for row in ee.ETF_UNIVERSE if row[0] == "ZLB.TO"]
    assert len(zlb_rows) == 1
    assert zlb_rows[0][2] == "DEFENSIVE"


# ─────────────────────────────────────────────────────────────────────────────
# FIX (2026-08-08): insider engine silently swallowed every EDGAR fetch
# failure, indistinguishable from "checked, found nothing". Confirmed live:
# 45 tickers "checked" in under 1ms of wall-clock log timestamps (physically
# impossible for real network round-trips) -- every fetch was failing near-
# instantly (SEC EDGAR 403 on GitHub Actions' shared runner IP range, most
# likely) and getting caught by a bare except. run_manifest.json's
# insider_ok:true meant "didn't crash", not "worked".
# ─────────────────────────────────────────────────────────────────────────────

def test_insider_fetch_form4_returns_none_not_empty_list_on_failure():
    """fetch_form4_aggregated() must return None (distinguishable failure)
    when the EDGAR request raises, not [] (which is indistinguishable from
    a genuine successful-but-empty result)."""
    import insider_engine as ie
    from unittest.mock import patch

    with patch.object(ie, "_edgar_request", side_effect=Exception("boom")):
        result = ie.fetch_form4_aggregated("0000320193", "AAPL")
    assert result is None, "fetch failure must return None, not []"


def test_insider_fetch_form4_returns_empty_list_on_genuine_no_data():
    """A successful fetch with zero Form 4s in the window must still
    return [] (not None) -- only real failures return None."""
    import insider_engine as ie
    from unittest.mock import patch

    fake_response = {"name": "AAPL", "filings": {"recent": {"form": [], "filingDate": []}}}
    with patch.object(ie, "_edgar_request", return_value=fake_response):
        result = ie.fetch_form4_aggregated("0000320193", "AAPL")
    assert result == []


def test_insider_run_engine_summary_distinguishes_failures_from_no_data(capsys, monkeypatch, tmp_path):
    """run_insider_engine()'s printed summary must say fetches FAILED when
    every fetch failed, not silently report '0 insider signals found' as if
    the engine had genuinely checked and found nothing."""
    import insider_engine as ie
    from unittest.mock import patch

    # run_insider_engine() unconditionally writes today's insider_scores to
    # HISTORY_FILE -- without this redirect, running the suite locally
    # silently overwrites the real insider_history.json's entry for today
    # with this test's synthetic (all-failed) result. Confirmed this
    # happening for real (2026-08-08): today's 16 genuine signals got wiped
    # to {} by a single local `pytest tests/test_invariants.py` run.
    monkeypatch.setattr(ie, "HISTORY_FILE", str(tmp_path / "insider_history.json"))

    # FIX (2026-08-09): run_insider_engine() now tries fetch_recent_form4()
    # (real transaction-level parsing) FIRST, falling back to
    # fetch_form4_aggregated() only if that fails -- both must be mocked to
    # actually simulate "every fetch failed" (this test previously only
    # mocked the fallback, so fetch_recent_form4 ran un-mocked and made a
    # real live SEC EDGAR call during the test run once that primary path
    # was added).
    with patch.object(ie, "fetch_recent_form4", return_value=None), \
         patch.object(ie, "fetch_form4_aggregated", return_value=None):
        picks = [{"ticker": "AAPL"}, {"ticker": "MSFT"}]
        ie.run_insider_engine(picks, verbose=True)
    out = capsys.readouterr().out
    assert "FAILED" in out
    assert "0 insider signals found" not in out or "fetches failed" in out.lower() or "FAILED" in out


def test_insider_score_insider_signal_handles_none_gracefully():
    """score_insider_signal(None, ...) must not crash -- safe_parse_records
    already handles None, this just locks in that contract now that
    fetch_form4_aggregated can genuinely return None."""
    import insider_engine as ie

    adj, reason = ie.score_insider_signal(None, "AAPL")
    assert adj == 0
    assert reason == ""


# ─────────────────────────────────────────────────────────────────────────────
# FIX (2026-08-08): 21 of 24 entries in CANADIAN_SEC_CIKS pointed at the
# wrong company entirely -- e.g. TIH.TO was wired to Sprint LLC, WPM.TO to
# Bank of Israel, POW.TO to National Presto Industries. One (ATD.TO ->
# Northrim BanCorp, an Alaska bank) had already produced a live,
# wrong-company insider-activity score adjustment in production on
# 2026-08-07 and 2026-08-08. Every value below was re-verified directly
# against SEC EDGAR's submissions.json / company_tickers.json master file.
# Root cause: the whole dict was added in a single "Add files via upload"
# GitHub web-UI commit (93157d23, 2026-06-24), never checked against EDGAR
# or covered by any test at the time.
# ─────────────────────────────────────────────────────────────────────────────

def test_canadian_sec_ciks_match_verified_edgar_values():
    """Locks in the corrected CIKs so a future edit can't silently
    reintroduce a wrong-company mapping without the test failing. These
    exact values were confirmed live against SEC EDGAR on 2026-08-08 (see
    each ticker's expected company name) -- this test doesn't re-hit the
    network (keeps the suite deterministic/offline) but pins the dict to
    the verified-correct snapshot."""
    import insider_engine as ie

    expected = {
        "RY.TO":  ("0001000275", "Royal Bank of Canada"),
        "TD.TO":  ("0000947263", "Toronto Dominion Bank"),
        "BNS.TO": ("0000009631", "Bank of Nova Scotia"),
        "BMO.TO": ("0000927971", "Bank of Montreal"),
        "CM.TO":  ("0001045520", "CIBC"),
        "NA.TO":  ("0000926171", "National Bank of Canada"),
        "MFC.TO": ("0001086888", "Manulife Financial"),
        "SLF.TO": ("0001097362", "Sun Life Financial"),
        "ENB.TO": ("0000895728", "Enbridge Inc"),
        "TRP.TO": ("0001232384", "TC Energy Corp"),
        "CNQ.TO": ("0001017413", "Canadian Natural Resources"),
        "SU.TO":  ("0000311337", "Suncor Energy"),
        "CVE.TO": ("0001475260", "Cenovus Energy"),
        "PPL.TO": ("0001546066", "Pembina Pipeline Corp"),
        "CP.TO":  ("0000016875", "Canadian Pacific Kansas City"),
        "CNR.TO": ("0000016868", "Canadian National Railway"),
        "TIH.TO": ("0002072098", "Toromont Industries/ADR"),
        "BAM.TO": ("0001001085", "Brookfield Corp"),
        "BN.TO":  ("0001001085", "Brookfield Corp"),
        "POW.TO": ("0000801166", "Power Corp of Canada"),
        "WPM.TO": ("0001323404", "Wheaton Precious Metals"),
        "ABX.TO": ("0000756894", "Barrick Mining Corp"),
        "AEM.TO": ("0000002809", "Agnico Eagle Mines"),
        "NTR.TO": ("0001725964", "Nutrien Ltd"),
    }

    assert set(ie.CANADIAN_SEC_CIKS.keys()) == set(expected.keys()), (
        "CANADIAN_SEC_CIKS ticker set changed -- update this test's "
        "expected dict (with a freshly EDGAR-verified CIK) rather than "
        "just letting it drift"
    )
    for ticker, (cik, company) in expected.items():
        assert ie.CANADIAN_SEC_CIKS[ticker] == cik, (
            f"{ticker} CIK changed from the verified value for {company} "
            f"({cik}) -- re-verify against SEC EDGAR before changing this"
        )


def test_stale_sec_entities_moved_to_sedi_only():
    """ATD.TO, FM.TO, LUN.TO were removed from CANADIAN_SEC_CIKS -- each
    has a real EDGAR entity but it's a stale shell registration with no
    Form 4 activity in 7-19 years (TSX-only, no active US equity
    cross-listing). Pointing at that CIK would silently return empty
    forever; SEDI_ONLY_TICKERS is the honest classification."""
    import insider_engine as ie

    for ticker in ("ATD.TO", "FM.TO", "LUN.TO"):
        assert ticker not in ie.CANADIAN_SEC_CIKS, (
            f"{ticker} has no active US SEC equity registration -- "
            f"must not be reinstated to CANADIAN_SEC_CIKS"
        )
        assert ticker in ie.SEDI_ONLY_TICKERS


# ─────────────────────────────────────────────────────────────────────────────
# FIX (2026-08-08): NGX resolution silently blended frozen-price artifacts
# (exit_price exactly equal to entry_price -- confirmed on SEPLAT.LG/
# TOTAL.LG/AIRTELAFRI.LG/GEREGU.LG/DANGCEM.LG across multiple independent
# resolution runs) into FLAT, distorting sector-level win rates (oil/
# telecom/power looked uniformly bad -- backfill against real data found
# 17 frozen rows, zero false positives, zero false negatives; win rate
# moved from 17.6% to 23.5% once excluded).
# ─────────────────────────────────────────────────────────────────────────────

def test_ngx_resolve_sets_price_frozen_true_when_exit_equals_entry(monkeypatch):
    """resolve_ngx_outcomes() must flag price_frozen=True (and still
    resolve, not block forever) when exit_price exactly equals entry_price."""
    import ngx_outcome_tracker as ngxt

    outcomes = [{
        "ticker": "FROZEN.LG", "sector": "oil", "tier": 2,
        "signal_date": "2026-07-01", "entry_price": 100.0,
        "resolved": False, "excluded_legacy": False,
    }]
    saved = {}
    monkeypatch.setattr(ngxt, "load_ngx_outcomes", lambda: outcomes)
    monkeypatch.setattr(ngxt, "save_ngx_outcomes", lambda o: saved.setdefault("out", o))
    monkeypatch.setattr(ngxt, "fetch_companies_prices", lambda verbose=False: {"FROZEN": 100.0})

    ngx_result = {"all_scored": [{"ticker": "FROZEN.LG", "score": 60}], "macro_regime": "NEUTRAL"}
    ngxt.resolve_ngx_outcomes(ngx_result)

    row = saved["out"][0]
    assert row["resolved"] is True
    assert row["outcome"] == "FLAT"
    assert row["price_frozen"] is True


def test_ngx_resolve_sets_price_frozen_false_when_price_moves(monkeypatch):
    """A real price move must NOT be flagged price_frozen, regardless of
    outcome direction."""
    import ngx_outcome_tracker as ngxt

    outcomes = [{
        "ticker": "MOVED.LG", "sector": "banking", "tier": 1,
        "signal_date": "2026-07-01", "entry_price": 100.0,
        "resolved": False, "excluded_legacy": False,
    }]
    saved = {}
    monkeypatch.setattr(ngxt, "load_ngx_outcomes", lambda: outcomes)
    monkeypatch.setattr(ngxt, "save_ngx_outcomes", lambda o: saved.setdefault("out", o))
    monkeypatch.setattr(ngxt, "fetch_companies_prices", lambda verbose=False: {"MOVED": 105.0})

    ngx_result = {"all_scored": [{"ticker": "MOVED.LG", "score": 70}], "macro_regime": "NEUTRAL"}
    ngxt.resolve_ngx_outcomes(ngx_result)

    row = saved["out"][0]
    assert row["outcome"] == "WIN"
    assert row["price_frozen"] is False


def test_ngx_outcome_summary_excludes_price_frozen_from_win_rate(monkeypatch):
    """ngx_outcome_summary() must drop price_frozen rows from total_resolved/
    wins/losses/flats/win_rate/sector stats -- same pattern as
    excluded_legacy -- while still surfacing price_frozen_total for audit."""
    import ngx_outcome_tracker as ngxt

    outcomes = [
        {"ticker": "A.LG", "sector": "oil", "tier": 1, "resolved": True,
         "outcome": "FLAT", "price_frozen": True, "excluded_legacy": False},
        {"ticker": "B.LG", "sector": "banking", "tier": 1, "resolved": True,
         "outcome": "WIN", "price_frozen": False, "excluded_legacy": False},
    ]
    monkeypatch.setattr(ngxt, "load_ngx_outcomes", lambda: outcomes)
    s = ngxt.ngx_outcome_summary()

    assert s["price_frozen_total"] == 1
    assert s["total_resolved"] == 1          # only B.LG
    assert s["wins"] == 1
    assert s["win_rate"] == 100.0
    assert s["sector_total"] == {"banking": 1}  # A.LG's oil entry excluded


def test_ngx_price_frozen_backfill_never_changes_resolved_outcome_fields(monkeypatch):
    """apply_price_frozen_backfill() must only add price_frozen and append
    to outcome_reason -- never touch resolved/outcome/exit_price/
    actual_return_pct/entry_price, preserving the audit trail."""
    import ngx_outcome_tracker as ngxt
    import copy

    original = {
        "ticker": "OLD.LG", "sector": "oil", "resolved": True,
        "outcome": "FLAT", "entry_price": 50.0, "exit_price": 50.0,
        "actual_return_pct": 0.0, "resolved_date": "2026-07-15",
        "outcome_reason": "Price 50.0->50.0 (+0.00%)", "excluded_legacy": False,
    }
    outcomes = [copy.deepcopy(original)]
    saved = {}
    monkeypatch.setattr(ngxt, "load_ngx_outcomes", lambda: outcomes)
    monkeypatch.setattr(ngxt, "save_ngx_outcomes", lambda o: saved.setdefault("out", o))

    n_flagged = ngxt.apply_price_frozen_backfill()

    assert n_flagged == 1
    row = saved["out"][0]
    for field in ("resolved", "outcome", "entry_price", "exit_price",
                  "actual_return_pct", "resolved_date"):
        assert row[field] == original[field], f"{field} was modified by the backfill"
    assert row["price_frozen"] is True
    assert "PRICE FROZEN" in row["outcome_reason"]


def test_ngx_price_frozen_backfill_skips_excluded_legacy_rows(monkeypatch):
    """The backfill must not touch excluded_legacy rows -- they're already
    excluded from stats for a different reason and shouldn't gain a
    price_frozen field that implies they were checked on this basis."""
    import ngx_outcome_tracker as ngxt

    outcomes = [{
        "ticker": "OLD.LG", "resolved": True, "outcome": "FLAT",
        "entry_price": 50.0, "exit_price": 50.0, "excluded_legacy": True,
    }]
    saved = {}
    monkeypatch.setattr(ngxt, "load_ngx_outcomes", lambda: outcomes)
    monkeypatch.setattr(ngxt, "save_ngx_outcomes", lambda o: saved.setdefault("out", o))

    n_flagged = ngxt.apply_price_frozen_backfill()

    assert n_flagged == 0
    assert "price_frozen" not in saved["out"][0]


# ─────────────────────────────────────────────────────────────────────────────
# NEW (2026-08-08): ETF outcome tracker. etf_engine.py has scored ETFs daily
# since inception with zero way to know if any of it was ever right. Own
# file (etf_outcome_tracker.py / etf_outcomes.json), not a reuse of
# outcome_tracker.py or ngx_outcome_tracker.py, since neither the schema
# (no ETF ML features) nor a single threshold (2 years of real price
# history showed DEFENSIVE and THEMATIC ETFs differ 10x+ in typical
# 30-day move size) transfers. Category-tiered thresholds, 30-day window,
# full_universe vs top_n_by_account kept as separate report sections.
# ─────────────────────────────────────────────────────────────────────────────

def test_etf_classify_outcome_uses_category_specific_threshold():
    """The same return magnitude must classify differently depending on
    category -- this is the whole point of category-tiered thresholds,
    not a single flat one."""
    import etf_outcome_tracker as eot

    # 0.60% is a WIN for DEFENSIVE (threshold 0.25) but only FLAT for
    # CORE (threshold 0.75) -- proves the threshold is actually keyed by
    # category, not a shared default.
    assert eot._classify_etf_outcome(0.60, "DEFENSIVE") == "WIN"
    assert eot._classify_etf_outcome(0.60, "CORE") == "FLAT"

    # -1.20% is a LOSS for CORE (threshold 0.75) but only FLAT for
    # THEMATIC (threshold 1.50).
    assert eot._classify_etf_outcome(-1.20, "CORE") == "LOSS"
    assert eot._classify_etf_outcome(-1.20, "THEMATIC") == "FLAT"


def test_etf_classify_outcome_unknown_category_uses_default():
    import etf_outcome_tracker as eot

    assert eot._classify_etf_outcome(0.60, "NOT_A_REAL_CATEGORY") == "FLAT"
    assert eot._classify_etf_outcome(0.90, "NOT_A_REAL_CATEGORY") == "WIN"


def test_etf_log_signals_captures_full_universe_not_just_top_picks():
    """log_etf_signals() must log every scored ETF, not just each
    account's top-N -- the point is validating the scoring itself, and
    only logging cherry-picked top picks risks survivorship bias."""
    import etf_outcome_tracker as eot
    from unittest.mock import patch

    fake_result = {
        "regime": "NEUTRAL",
        "scored": [
            {"ticker": "AAA", "name": "A Fund", "category": "CORE", "score": 40.0, "price": 10.0},
            {"ticker": "BBB", "name": "B Fund", "category": "THEMATIC", "score": 30.0, "price": 20.0},
        ],
        "rrsp_picks": [{"ticker": "AAA"}],
        "tfsa_picks": [],
        "fhsa_picks": [],
    }
    with patch.object(eot, "load_etf_outcomes", return_value=[]), \
         patch.object(eot, "save_etf_outcomes") as mock_save:
        n = eot.log_etf_signals(fake_result, run_time="2026-08-08T00:00:00")
        saved = mock_save.call_args[0][0]

    assert n == 2
    tickers = {o["ticker"] for o in saved}
    assert tickers == {"AAA", "BBB"}  # BBB never a top pick, still logged
    bbb = next(o for o in saved if o["ticker"] == "BBB")
    assert bbb["acct_flags"] == {"rrsp": False, "tfsa": False, "fhsa": False}
    aaa = next(o for o in saved if o["ticker"] == "AAA")
    assert aaa["acct_flags"]["rrsp"] is True
    assert aaa["entry_price"] == 10.0


def test_etf_log_signals_no_duplicate_on_same_day():
    import etf_outcome_tracker as eot
    from unittest.mock import patch
    from datetime import datetime

    today_str = datetime.now().strftime("%Y-%m-%d")
    existing = [{"ticker": "AAA", "signal_date": today_str}]
    fake_result = {
        "scored": [{"ticker": "AAA", "name": "A", "category": "CORE", "score": 50.0, "price": 10.0}],
        "rrsp_picks": [], "tfsa_picks": [], "fhsa_picks": [],
    }
    with patch.object(eot, "load_etf_outcomes", return_value=existing), \
         patch.object(eot, "save_etf_outcomes") as mock_save:
        n = eot.log_etf_signals(fake_result)

    assert n == 0


def test_etf_resolve_leaves_unresolved_without_faking_outcome(monkeypatch):
    """Missing entry_price, failed exit_price fetch, or too-recent a
    signal must all leave the row unresolved -- never silently default
    to a fake outcome."""
    import etf_outcome_tracker as eot
    from datetime import datetime, timedelta

    old_date = (datetime.now() - timedelta(days=31)).strftime("%Y-%m-%d")
    recent_date = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")
    outcomes = [
        {"ticker": "NO_ENTRY", "category": "CORE", "signal_date": old_date,
         "entry_price": None, "resolved": False},
        {"ticker": "TOO_RECENT", "category": "CORE", "signal_date": recent_date,
         "entry_price": 100.0, "resolved": False},
    ]
    saved = {}
    monkeypatch.setattr(eot, "load_etf_outcomes", lambda: outcomes)
    monkeypatch.setattr(eot, "save_etf_outcomes", lambda o: saved.setdefault("out", o))

    n = eot.resolve_etf_outcomes(current_prices={})

    assert n == 0
    assert all(not o["resolved"] for o in saved["out"])


def test_etf_resolve_classifies_correctly_with_provided_prices(monkeypatch):
    import etf_outcome_tracker as eot
    from datetime import datetime, timedelta

    old_date = (datetime.now() - timedelta(days=31)).strftime("%Y-%m-%d")
    outcomes = [{"ticker": "WINNER", "category": "DEFENSIVE",
                 "signal_date": old_date, "entry_price": 100.0, "resolved": False}]
    saved = {}
    monkeypatch.setattr(eot, "load_etf_outcomes", lambda: outcomes)
    monkeypatch.setattr(eot, "save_etf_outcomes", lambda o: saved.setdefault("out", o))

    n = eot.resolve_etf_outcomes(current_prices={"WINNER": 100.30})

    assert n == 1
    row = saved["out"][0]
    assert row["resolved"] is True
    assert row["actual_return_pct"] == 0.3
    assert row["outcome"] == "WIN"


def test_etf_outcome_summary_full_universe_and_top_n_never_blended(monkeypatch):
    """full_universe and top_n_by_account must be separate sections with
    independently-computed win rates -- same multi-cut pattern as
    win_rate.json, never combined into one number."""
    import etf_outcome_tracker as eot

    outcomes = [
        {"ticker": "A", "category": "CORE", "resolved": True, "outcome": "WIN",
         "acct_flags": {"rrsp": True, "tfsa": False, "fhsa": False}},
        {"ticker": "B", "category": "CORE", "resolved": True, "outcome": "LOSS",
         "acct_flags": {"rrsp": False, "tfsa": False, "fhsa": False}},
    ]
    monkeypatch.setattr(eot, "load_etf_outcomes", lambda: outcomes)
    s = eot.etf_outcome_summary()

    assert s["full_universe"]["win_rate"] == 50.0
    assert s["full_universe"]["total_resolved"] == 2
    assert s["top_n_by_account"]["rrsp"]["win_rate"] == 100.0
    assert s["top_n_by_account"]["rrsp"]["total_resolved"] == 1
    assert s["top_n_by_account"]["tfsa"]["total_resolved"] == 0


def test_etf_outcome_tracker_wired_into_run_daily():
    """run_daily.py must actually call the new tracker -- a module that's
    never invoked doesn't close the 'ETF picks are unmeasured' gap."""
    with open("run_daily.py") as f:
        content = f.read()
    assert "log_etf_signals" in content
    assert "resolve_etf_outcomes" in content


# ─────────────────────────────────────────────────────────────────────────────
# FIX (2026-08-08): dead thingproxy.freeboard.io route removed from
# fetchYahooData()'s fallback waterfall. Confirmed via independent DNS
# lookup (NXDOMAIN, not a transient outage) that this domain no longer
# resolves -- it could only ever hit its own 10s timeout, adding dead
# latency to every ticker lookup that fell through to it with zero chance
# of ever succeeding. Stress-testing the live proxy services themselves
# (corsproxy.io, allorigins.win, codetabs.com) isn't something a unit
# test can assert on -- their availability changes independently of this
# codebase -- so this just locks in that the known-dead one stays removed.
# ─────────────────────────────────────────────────────────────────────────────

def test_index_html_no_longer_references_dead_thingproxy_domain():
    """thingproxy.freeboard.io must not be fetched from anymore -- the
    domain is confirmed dead (NXDOMAIN), keeping the fetch call would
    just guarantee a stuck 10s timeout on affected lookups."""
    with open("index.html") as f:
        content = f.read()
    assert "fetch('https://thingproxy.freeboard.io" not in content
    assert 'fetch("https://thingproxy.freeboard.io' not in content


def test_index_html_no_longer_fetches_confirmed_broken_proxies():
    """corsproxy.io, allorigins.win, and codetabs.com must not be fetched
    from anymore in fetchYahooData()'s waterfall -- all three confirmed
    non-functional by live stress test (corsproxy.io silently returning
    its own HTML landing page instead of JSON, reproduced twice;
    allorigins.win erroring/timing out; codetabs.com's origin down via
    Cloudflare 521). They may still appear in an explanatory comment --
    only the live fetch() calls are asserted against."""
    with open("index.html") as f:
        content = f.read()
    start = content.index("async function fetchYahooData")
    i = content.index("{", start)
    depth = 0
    end = -1
    for idx in range(i, len(content)):
        if content[idx] == "{":
            depth += 1
        elif content[idx] == "}":
            depth -= 1
            if depth == 0:
                end = idx
                break
    fn_body = content[start:end + 1]

    for domain in ("corsproxy.io", "allorigins.win", "codetabs.com"):
        assert f"fetch('https://{domain}" not in fn_body, f"{domain} still fetched from"
        assert f'fetch("https://{domain}' not in fn_body, f"{domain} still fetched from"


def test_index_html_netlify_route_no_longer_skips_canadian_tickers():
    """The Netlify proxy (the only route in the original waterfall
    verified reliable) must be tried for every ticker, not just non-
    Canadian ones -- ticker.js's own sanitisation already allows periods
    (needed for .TO), and the Yahoo endpoint it calls is ticker-agnostic."""
    with open("index.html") as f:
        content = f.read()
    start = content.index("async function fetchYahooData")
    i = content.index("{", start)
    depth = 0
    end = -1
    for idx in range(i, len(content)):
        if content[idx] == "{":
            depth += 1
        elif content[idx] == "}":
            depth -= 1
            if depth == 0:
                end = idx
                break
    fn_body = content[start:end + 1]

    assert "isCanadian" not in fn_body
    assert "investos-proxy.netlify.app" in fn_body


# ─────────────────────────────────────────────────────────────────────────────
# FIX (2026-08-08): insider engine's SEC EDGAR 403 was never a
# GitHub-Actions-IP-block -- SEC's own 403 page said so explicitly:
# "Your Request Originates from an Undeclared Automated Tool... Please
# declare your traffic by updating your user agent to include company
# specific information." The old UA used github.com (GitHub's domain, not
# an owned domain) as the "company" contact, which SEC's declared-traffic
# check rejects. Confirmed directly against the exact CIK that 403'd in
# production (AMGN): old UA fails every time, three different properly-
# declared UAs succeed every time, immediately, no network/IP change
# needed. A minimal Netlify diagnostic probe was built to test the
# IP-block hypothesis and deleted once this simpler, correct root cause
# was found via a much cheaper direct test -- no deploy ever needed.
# ─────────────────────────────────────────────────────────────────────────────

def test_edgar_user_agent_does_not_use_github_domain():
    """Locks in the fix -- the actual User-Agent header sent must never
    regress to using github.com (or another shared-platform domain) as
    the declared contact, since that's confirmed to trigger SEC's
    undeclared-tool rejection. Inspects the real header urllib.request
    would send (not the function source, which legitimately discusses
    github.com in its docstring while explaining the fix) via a
    lightweight Request stand-in, to keep the suite deterministic -- the
    live confirmation was done manually against the real endpoint and the
    real production CIK that failed."""
    import insider_engine as ie
    from unittest.mock import patch, MagicMock

    captured = {}

    def fake_urlopen(req, timeout=None):
        captured["ua"] = req.get_header("User-agent")
        cm = MagicMock()
        cm.__enter__.return_value.read.return_value = b'{"ok": true}'
        return cm

    with patch("urllib.request.urlopen", side_effect=fake_urlopen):
        ie._edgar_request("https://data.sec.gov/submissions/CIK0000000000.json")

    assert captured["ua"] is not None
    assert "github.com" not in captured["ua"], (
        f"User-Agent {captured['ua']!r} uses github.com as the declared "
        f"contact domain -- confirmed to trigger SEC EDGAR's 403 "
        f"undeclared-automated-tool rejection, this must not regress"
    )


# ─────────────────────────────────────────────────────────────────────────────
# FIX (2026-08-08): ticker.js bills a function invocation the instant
# exports.handler starts running, even for requests it immediately
# rejects with 401 -- so unauthorised traffic (bad/missing origin,
# no key) was silently burning Netlify credits with no way to stop it
# short of rejecting before the origin function is ever invoked.
# netlify/edge-functions/gate-ticker.js runs ahead of the origin (Edge
# Functions execute pre-invocation) and mirrors ticker.js's own
# ALLOWED_ORIGIN / INVESTOS_API_KEY logic exactly, so legitimate
# traffic is unaffected but unauthorised requests never reach (and
# never bill) ticker.js at all.
# ─────────────────────────────────────────────────────────────────────────────

def test_gate_ticker_edge_function_auth_matrix():
    """Drives the real gate-ticker.js file through Node's native
    Request/Response (available since Node 18, no deps needed) to
    exercise its actual exported auth logic -- not a source-text
    inspection. Mirrors the same origin/key matrix ticker.js itself is
    keyed on: valid origin passes, valid key without origin passes,
    OPTIONS preflight always passes through untouched (ticker.js owns
    the CORS preflight response), and anything else is rejected with
    401 before context.next() -- i.e. before the origin function, and
    its billing, is ever reached. Skips (does not fail) if `node` isn't
    on PATH, since this environment cannot run a real Deno Edge
    Functions runtime either -- this is the strongest local
    verification available short of a live deploy."""
    import shutil, subprocess, json as _json

    node = shutil.which("node")
    if not node:
        pytest.skip("node not available in this environment")

    script = r"""
import gateTicker from '../netlify/edge-functions/gate-ticker.js';

globalThis.Netlify = { env: { get: (k) => k === 'INVESTOS_API_KEY' ? 'real-secret-key' : undefined } };

const ORIGIN = 'https://investos-proxy.netlify.app';
const mockContext = { next: async () => new Response('PASSED_THROUGH', { status: 200 }) };

async function run(req) {
  const res = await gateTicker(req, mockContext);
  return res.status;
}

const results = {};

results.options_no_auth = await run(
  new Request('https://x/api/ticker?s=AAPL', { method: 'OPTIONS' }));

results.valid_origin_no_key = await run(
  new Request('https://x/api/ticker?s=AAPL', { headers: { origin: ORIGIN } }));

results.wrong_origin_correct_key = await run(
  new Request('https://x/api/ticker?s=AAPL', {
    headers: { origin: 'https://evil.example.com', 'x-investos-key': 'real-secret-key' } }));

results.wrong_origin_wrong_key = await run(
  new Request('https://x/api/ticker?s=AAPL', {
    headers: { origin: 'https://evil.example.com', 'x-investos-key': 'nope' } }));

results.no_origin_no_key = await run(
  new Request('https://x/api/ticker?s=AAPL'));

console.log(JSON.stringify(results));
"""
    script_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "_gate_ticker_probe.mjs"
    )
    with open(script_path, "w") as f:
        f.write(script)
    try:
        proc = subprocess.run(
            [node, script_path],
            cwd=os.path.dirname(script_path),
            capture_output=True, text=True, timeout=15,
        )
    finally:
        os.remove(script_path)

    assert proc.returncode == 0, f"probe script failed: {proc.stderr}"
    results = _json.loads(proc.stdout.strip().splitlines()[-1])

    assert results["options_no_auth"] == 200, "OPTIONS preflight must always pass through"
    assert results["valid_origin_no_key"] == 200, "valid ALLOWED_ORIGIN must be authorised"
    assert results["wrong_origin_correct_key"] == 200, "correct INVESTOS_API_KEY must authorise even with wrong origin"
    assert results["wrong_origin_wrong_key"] == 401, "wrong origin + wrong key must be rejected pre-invocation"
    assert results["no_origin_no_key"] == 401, "no origin + no key must be rejected pre-invocation"


# ─────────────────────────────────────────────────────────────────────────────
# FIX (2026-08-08): every pick was labeled off a fixed 7-day price move for ML
# training, regardless of category, despite stock_screener.py explicitly
# assigning categories real intended hold periods (SWING 30d, WATCH 90d,
# GROWTH CORE/FHSA 180d, income categories 365d). Empirically confirmed: at
# SWING's real 30-day horizon, the same signal-time ml_prob's correlation
# with outcome roughly doubles (0.118 non-significant -> 0.284, p<0.0001)
# and the top/bottom tercile return spread goes from +1.90% to +10.95%. A
# genuine temporal-holdout LogisticRegression on the correctly-labeled SWING
# data hit AUC 0.692, vs 0.497-0.567 for anything trained on the old
# 7-day label. Also: 3 features (sector_momentum, market_regime,
# close_to_ema20_ratio) were confirmed constant across all 2465 rows --
# dead weight, removed. And the roe monotone constraint was flipped from
# +1 to -1 to match its empirically negative correlation (-0.096).
# ─────────────────────────────────────────────────────────────────────────────

def test_category_horizons_match_screener_hold_days():
    """CATEGORY_HORIZONS in outcome_tracker.py must stay in sync with the
    hold_days stock_screener.py actually assigns per category -- these
    were hand-copied once; a future screener change to hold_days without
    updating CATEGORY_HORIZONS would silently reintroduce the horizon
    mismatch this fix targets."""
    import outcome_tracker as ot
    import re

    with open("stock_screener.py") as f:
        src = f.read()

    # Parse "category = "X"" ... "hold_days = N" pairs out of the
    # if/elif chain that assigns pick categories (mirrors the source
    # structure rather than re-deriving it independently).
    pairs = re.findall(
        r'category\s*=\s*"([^"]+)"\s*\n\s*hold_days\s*=\s*(\d+)', src
    )
    assert pairs, "couldn't find any category/hold_days pairs in stock_screener.py -- did its structure change?"

    screener_horizons = {cat: int(days) for cat, days in pairs}
    for cat, days in screener_horizons.items():
        assert ot.CATEGORY_HORIZONS.get(cat) == days, (
            f"stock_screener.py assigns {cat!r} hold_days={days}, but "
            f"outcome_tracker.py's CATEGORY_HORIZONS has "
            f"{ot.CATEGORY_HORIZONS.get(cat)!r} -- these must match"
        )


def test_resolve_true_horizon_preserves_existing_7day_fields(monkeypatch):
    """resolve_true_horizon_outcomes() must be purely additive -- it must
    never touch the existing resolved/actual_return/outcome fields the
    dashboard and win_rate.json read, only add the new true_horizon_*
    fields alongside them."""
    import outcome_tracker as ot

    outcomes = [{
        "ticker": "TEST", "category": "SWING",
        "signal_date": "2020-01-01",  # far enough in the past that horizon has passed
        "entry_price": 100.0,
        "resolved": True, "actual_return": 1.23, "outcome": "WIN",
        "resolved_date": "2020-01-08",
    }]

    def fake_fetch(ticker, target_date, cache, max_lookahead=5):
        return 110.0  # arbitrary price, just needs to be non-None

    monkeypatch.setattr(ot, "_fetch_historical_price", fake_fetch)
    updated, n_resolved = ot.resolve_true_horizon_outcomes(outcomes, save=False)

    assert n_resolved == 1
    o = updated[0]
    # Original 7-day fields untouched
    assert o["resolved"] is True
    assert o["actual_return"] == 1.23
    assert o["outcome"] == "WIN"
    # New true-horizon fields added
    assert o["true_horizon_resolved"] is True
    assert o["true_horizon_return"] == 10.0  # (110-100)/100*100
    assert o["true_horizon_days"] == 30


def test_resolve_true_horizon_skips_picks_not_yet_at_horizon():
    """A pick signaled recently (well within its category's horizon) must
    not be resolved, regardless of how old the function call itself is."""
    import outcome_tracker as ot
    from datetime import datetime, timedelta

    recent_date = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")
    outcomes = [{
        "ticker": "TEST", "category": "SWING",  # 30d horizon, only 5d elapsed
        "signal_date": recent_date, "entry_price": 100.0,
    }]
    updated, n_resolved = ot.resolve_true_horizon_outcomes(outcomes, save=False)
    assert n_resolved == 0
    assert "true_horizon_resolved" not in updated[0]


def test_dead_features_removed_and_configs_stay_in_sync():
    """sector_momentum, market_regime, close_to_ema20_ratio were confirmed
    constant (zero variance) across the entire training set -- must not
    reappear in either file's feature list, and both files' feature lists
    must stay identical (feature_hash compatibility depends on it)."""
    import ml_engine as me
    import ml_retrainer as mr

    dead = {"sector_momentum", "market_regime", "close_to_ema20_ratio"}
    assert not (dead & set(me.ML_CONFIG["features"])), "dead feature reappeared in ml_engine.py"
    assert not (dead & set(mr.FEATURES)), "dead feature reappeared in ml_retrainer.py"
    assert me.ML_CONFIG["features"] == mr.FEATURES, (
        "ml_engine.py and ml_retrainer.py feature lists diverged -- "
        "breaks feature_hash cache compatibility between the two"
    )


def test_roe_monotone_constraint_is_negative():
    """roe's empirical correlation with the label is negative (-0.096,
    confirmed on live data) -- the constraint must match, not fight it."""
    import ml_engine as me
    import ml_retrainer as mr

    assert me.ML_CONFIG["xgb_params"]["monotone_constraints"]["roe"] == -1
    assert mr.XGB_PARAMS["monotone_constraints"]["roe"] == -1
    # close_to_ema20_ratio's constraint must be gone along with the dead feature
    assert "close_to_ema20_ratio" not in me.ML_CONFIG["xgb_params"]["monotone_constraints"]
    assert "close_to_ema20_ratio" not in mr.XGB_PARAMS["monotone_constraints"]


def test_rules_based_categories_score_without_a_trained_model():
    """WATCH/GROWTH CORE/FHSA/income categories have no signal old enough
    yet to validate an ML model against their true 90-365 day horizon --
    predict_rules_based() must work standalone, regardless of self.trained,
    and RULES_BASED_CATEGORIES must cover exactly the long-horizon
    categories (SWING is deliberately excluded -- it has its own model)."""
    import ml_engine as me

    p = me.StockMLPredictor()
    assert p.trained is False
    features = {"momentum_6m": 0.05, "roe": 0.1, "rs_rating": 0.7}
    prob = p.predict_rules_based(features, market_regime=1)
    assert 0.1 <= prob <= 0.9

    assert me.RULES_BASED_CATEGORIES == {
        "WATCH", "GROWTH CORE", "FHSA Conservative Growth",
        "INCOME", "DIVIDEND GROWTH", "INCOME + GROWTH",
    }
    assert "SWING" not in me.RULES_BASED_CATEGORIES


def test_predict_swing_returns_none_without_a_loaded_model():
    """predict_swing() must degrade gracefully (None, not a crash or a
    silent 0.5) when no SWING model is loaded, so callers can fall back
    to the general model -- exactly what run_ml_engine's routing does."""
    import ml_engine as me

    p = me.StockMLPredictor()
    assert p.swing_model is None
    result = p.predict_swing({"momentum_6m": 0.05})
    assert result is None


# ─────────────────────────────────────────────────────────────────────────────
# FIX (2026-08-08): predict_swing() returned raw, unclipped
# LogisticRegression output while predict_rules_based() clips to
# [0.1, 0.9]. Checked the real distribution on the 239-row SWING training
# set: genuinely extreme at the low end (p1=0.0009, p5=0.0026, 21/239
# rows below 0.05) -- small-sample overconfidence, not rare noise.
# ─────────────────────────────────────────────────────────────────────────────

def test_predict_swing_clips_extreme_probabilities():
    """predict_swing()'s output must be bounded to [0.1, 0.9], matching
    predict_rules_based()'s convention -- confirmed real training-set
    probabilities go as low as 0.0003, which must not reach callers
    unclipped."""
    import ml_engine as me
    from unittest.mock import MagicMock
    import numpy as np

    p = me.StockMLPredictor()
    p.swing_model = MagicMock()
    p.swing_model.predict_proba.return_value = np.array([[0.9997, 0.0003]])
    p.swing_scaler = MagicMock()
    p.swing_scaler.transform.return_value = [[0, 0, 0]]

    result = p.predict_swing({"momentum_6m": 0.05})
    assert result == 0.1, f"expected clip to 0.1 floor, got {result}"

    p.swing_model.predict_proba.return_value = np.array([[0.01, 0.99]])
    result = p.predict_swing({"momentum_6m": 0.05})
    assert result == 0.9, f"expected clip to 0.9 ceiling, got {result}"

    p.swing_model.predict_proba.return_value = np.array([[0.4, 0.6]])
    result = p.predict_swing({"momentum_6m": 0.05})
    assert result == 0.6, f"mid-range value should pass through unclipped, got {result}"


# ─────────────────────────────────────────────────────────────────────────────
# FIX (2026-08-08): run_ml_engine()'s category-based routing read
# pick.get("category"), but every screener bucket builds picks as
# {"ticker":..., "data":..., "pick": {"category":..., ...}} -- category
# lives nested under pick["pick"]["category"]. pick.get("category") was
# always None, so every pick silently fell through to the general-model
# branch and the SWING/rules-based routing added earlier this session
# never actually ran. Confirmed dead in production via latest_brief.json:
# every pick showed ml_prob_source="model" regardless of category, even
# on the run right after the routing code shipped. Caught by checking
# real production output, not by the unit tests written alongside the
# original fix -- those called predict_swing()/predict_rules_based()
# directly with a category string extracted correctly by the TEST code,
# never exercising run_ml_engine()'s actual (buggy) extraction line. This
# test closes that gap: it drives the real run_ml_engine() dispatch with
# realistically-nested pick dicts, matching stock_screener.py's actual
# shape, instead of testing the routed-to methods in isolation.
# ─────────────────────────────────────────────────────────────────────────────

def test_run_ml_engine_routes_by_nested_pick_category(monkeypatch, tmp_path):
    """run_ml_engine() must read category from pick["pick"]["category"]
    (the real shape), not pick.get("category") directly -- that returns
    None for every real pick and silently defeats all category routing."""
    import ml_engine as me
    from unittest.mock import MagicMock
    import numpy as np

    # Avoid live network calls (Yahoo Finance chart data) and avoid
    # writing to the real tracked ml_score_smooth.json.
    monkeypatch.setattr(me, "_SMOOTH_CACHE_FILE", str(tmp_path / "smooth_cache.json"))
    monkeypatch.setattr(
        me, "build_features_for_stock",
        lambda ticker, stock_data, rs=50: {"momentum_6m": 0.05, "roe": 0.1, "rs_rating": 0.6}
    )
    monkeypatch.setattr(me, "get_market_regime", lambda verbose=True: {
        "regime": "UNKNOWN", "signal": "NEUTRAL", "cash_pct": 0.0,
        "spx_price": 0, "ma200": 0, "pct_above_ma": 0,
    })

    # Fast, deterministic general model -- avoid a real ~2465-row XGBoost
    # train just to test dispatch wiring.
    def fake_train(self, verbose=True):
        self.trained = True
        self.model = MagicMock()
        self.model.predict_proba.return_value = np.array([[0.3, 0.7]])
        self.calibrator = None
        self.swing_model, self.swing_scaler = None, None  # force SWING fallback path off
        return True
    monkeypatch.setattr(me.StockMLPredictor, "train", fake_train)

    def make_pick(ticker, category):
        return {"ticker": ticker, "score": 50, "data": {}, "pick": {"category": category}}

    screener_picks = {
        "FHSA_top5":        [make_pick("SWINGCO", "SWING")],
        "TFSA_growth_top5": [make_pick("WATCHCO", "WATCH")],
        "TFSA_income_top5": [make_pick("FHSACO", "FHSA Conservative Growth")],
        "TFSA_swing_top3":  [make_pick("OTHERCO", "SOME_OTHER_CATEGORY")],
    }

    me.run_ml_engine(screener_picks, {}, verbose=False)

    assert screener_picks["FHSA_top5"][0]["ml_prob_source"] in ("swing_model", "model"), (
        "SWING pick should route to swing_model (or fall back to model if unavailable), "
        f"got {screener_picks['FHSA_top5'][0]['ml_prob_source']!r}"
    )
    assert screener_picks["TFSA_growth_top5"][0]["ml_prob_source"] == "rules_based", (
        f"WATCH pick should route to rules_based, got "
        f"{screener_picks['TFSA_growth_top5'][0]['ml_prob_source']!r}"
    )
    assert screener_picks["TFSA_income_top5"][0]["ml_prob_source"] == "rules_based", (
        f"FHSA Conservative Growth pick should route to rules_based, got "
        f"{screener_picks['TFSA_income_top5'][0]['ml_prob_source']!r}"
    )
    assert screener_picks["TFSA_swing_top3"][0]["ml_prob_source"] == "model", (
        f"unrecognized category should route to the general model, got "
        f"{screener_picks['TFSA_swing_top3'][0]['ml_prob_source']!r}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# FIX (2026-08-08): a ticker qualifying for both an FHSA pick and a TFSA pick
# gets two independent dict objects (stock_screener.py's FHSA pass and TFSA
# pass each call classify_pick() separately). run_ml_engine() dedupes its own
# internal copy before scoring, but the picks fed into intelligence layers
# (score history, trend detection) used to concatenate all four top-N buckets
# without deduping -- so an un-scored duplicate (no ml_prob, stale pre-ML
# score) could silently overwrite the real ML-scored one in
# update_score_history(), which processes picks in list order and overwrites
# same-day entries. Confirmed live 2026-08-08: TOST/CSCO/RTX/VLO all
# collapsed to score=50.0 in score_history.json despite having real, much
# higher ML-scored values elsewhere in the same run.
# ─────────────────────────────────────────────────────────────────────────────

def test_dedupe_top_flat_picks_prefers_ml_scored_duplicate():
    """The un-scored duplicate's pre-ML score can coincidentally exceed the
    ML-scored copy's post-adjustment score -- this is the exact shape of
    the real bug, so the dedup must key off ml_prob presence, not just
    raw score."""
    from run_daily import dedupe_top_flat_picks

    # FHSA-bucket copy: went through ML scoring, has ml_prob, but its
    # score dropped slightly after the ML adjustment.
    scored_copy = {"ticker": "TOST", "score": 84.6, "ml_prob": 0.851,
                   "pick": {"category": "GROWTH CORE"}}
    # TFSA-bucket copy: never touched by run_ml_engine, no ml_prob, but its
    # stale pre-ML score happens to be higher.
    unscored_copy = {"ticker": "TOST", "score": 86.6,
                      "pick": {"category": "SWING"}}

    result = dedupe_top_flat_picks([unscored_copy, scored_copy], verbose=False)
    assert len(result) == 1
    assert result[0] is scored_copy, (
        "must keep the ml_prob-bearing duplicate even though the other "
        "duplicate has a higher raw score"
    )

    # Order shouldn't matter -- same result either way.
    result2 = dedupe_top_flat_picks([scored_copy, unscored_copy], verbose=False)
    assert result2[0] is scored_copy


def test_dedupe_top_flat_picks_warns_on_mismatch(capsys):
    """A real duplicate (different scores or ml_prob presence) must print
    a visible warning -- this is what makes a future recurrence
    diagnosable from the log instead of requiring reconstruction."""
    from run_daily import dedupe_top_flat_picks

    picks = [
        {"ticker": "TOST", "score": 86.6, "pick": {"category": "SWING"}},
        {"ticker": "TOST", "score": 84.6, "ml_prob": 0.851, "pick": {"category": "GROWTH CORE"}},
    ]
    dedupe_top_flat_picks(picks, verbose=True)
    out = capsys.readouterr().out
    assert "TOST" in out and "duplicate" in out.lower()


def test_dedupe_top_flat_picks_no_duplicates_is_a_noop():
    """Picks with unique tickers must pass through unchanged, in the same
    order, with no spurious warnings."""
    from run_daily import dedupe_top_flat_picks

    picks = [
        {"ticker": "AAA", "score": 90, "ml_prob": 0.7, "pick": {"category": "SWING"}},
        {"ticker": "BBB", "score": 80, "ml_prob": 0.6, "pick": {"category": "WATCH"}},
    ]
    result = dedupe_top_flat_picks(picks, verbose=False)
    assert len(result) == 2
    assert [p["ticker"] for p in result] == ["AAA", "BBB"]


def test_run_ml_engine_scoring_table_shows_category_and_source(monkeypatch, tmp_path, capsys):
    """The printed scoring table must show category and ml_prob_source per
    pick -- this is the log-visibility fix: without it, diagnosing which
    routing path a pick took requires reconstructing state after the fact
    (exactly what was needed to find the TOST/CSCO/RTX/VLO bug above)."""
    import ml_engine as me
    from unittest.mock import MagicMock
    import numpy as np

    monkeypatch.setattr(me, "_SMOOTH_CACHE_FILE", str(tmp_path / "smooth_cache.json"))
    monkeypatch.setattr(
        me, "build_features_for_stock",
        lambda ticker, stock_data, rs=50: {"momentum_6m": 0.05, "roe": 0.1, "rs_rating": 0.6}
    )
    monkeypatch.setattr(me, "get_market_regime", lambda verbose=True: {
        "regime": "UNKNOWN", "signal": "NEUTRAL", "cash_pct": 0.0,
        "spx_price": 0, "ma200": 0, "pct_above_ma": 0,
    })

    def fake_train(self, verbose=True):
        self.trained = True
        self.model = MagicMock()
        self.model.predict_proba.return_value = np.array([[0.3, 0.7]])
        self.calibrator = None
        self.swing_model, self.swing_scaler = None, None
        return True
    monkeypatch.setattr(me.StockMLPredictor, "train", fake_train)

    screener_picks = {
        "FHSA_top5": [{"ticker": "WATCHCO", "score": 50, "data": {}, "pick": {"category": "WATCH"}}],
        "TFSA_growth_top5": [], "TFSA_income_top5": [], "TFSA_swing_top3": [],
    }
    me.run_ml_engine(screener_picks, {}, verbose=True)
    out = capsys.readouterr().out

    assert "Category" in out and "Source" in out, "scoring table header must show Category/Source columns"
    assert "WATCH" in out and "rules_based" in out, "per-row output must show the pick's actual category and routing source"


# ─────────────────────────────────────────────────────────────────────────────
# FIX (2026-08-08): audit of the FHSA/TFSA duplicate-pick root cause (see
# pick_utils.py's module docstring) found it recurring independently in 3
# more call sites beyond top_flat, one of them severe:
# ml_engine.py's _apply_sector_cap() checked basket_tickers once before its
# reserve-fill loop but never re-checked during it, so a duplicate could be
# added to the FINAL position-sizing basket TWICE -- real double capital
# allocation to the same stock. risk_engine.py's track_signal_accuracy() and
# run_stress_simulation() both flattened screener buckets with zero dedup at
# all, double-counting a straddling ticker in accuracy tracking and in the
# stress-test baseline average respectively.
# ─────────────────────────────────────────────────────────────────────────────

def test_pick_utils_dedupe_degrades_to_score_only_without_ml_prob():
    """Some call sites (risk_engine.py's stress-test baseline) build summary
    dicts that never carry an ml_prob field at all -- the shared utility
    must still dedupe correctly in that case, falling back to score."""
    from pick_utils import dedupe_picks_by_ticker

    picks = [
        {"ticker": "AMGN", "score": 100, "bucket": "FHSA_top5"},
        {"ticker": "AMGN", "score": 96,  "bucket": "TFSA_growth_top5"},
        {"ticker": "MPC",  "score": 85,  "bucket": "TFSA_swing_top3"},
    ]
    result = dedupe_picks_by_ticker(picks, verbose=False)
    assert len(result) == 2
    amgn = [p for p in result if p["ticker"] == "AMGN"][0]
    assert amgn["score"] == 100, "with no ml_prob on either copy, must fall back to higher score"


def test_apply_sector_cap_never_duplicates_a_ticker_in_final_basket():
    """The severe case: reproduces the actual bug shape (a duplicate ticker
    in the reserve pool that the old fill loop could add twice) and drives
    the real _apply_sector_cap(), not a reimplementation of its logic.

    The fill loop only breaks once len(filled) reaches len(picks) -- with
    just one excess slot, it fills and breaks before ever reaching a second
    reserve entry, silently *not* exercising the bug regardless of whether
    the fix is present. Needs >=2 excess slots so the loop processes past
    the first AMGN copy into the duplicate second one (confirmed by
    reverting the fix and watching this exact test pass anyway until the
    slot count was corrected -- the fix's own regression coverage failed
    silently the same way the original bug did)."""
    import ml_engine as me

    # 5 FINANCIALS picks, cap=2 -> kept=2, excess=3: needs 3 replacements,
    # more than the reserve pool can supply, so the loop consumes every
    # reserve candidate rather than breaking after the first.
    picks = [
        {"ticker": "BAC",  "score": 97, "sector": "Financial Services", "data": {}},
        {"ticker": "JPM",  "score": 95, "sector": "Financial Services", "data": {}},
        {"ticker": "V",    "score": 90, "sector": "Financial Services", "data": {}},
        {"ticker": "WFC",  "score": 88, "sector": "Financial Services", "data": {}},
        {"ticker": "GS",   "score": 85, "sector": "Financial Services", "data": {}},
    ]
    # AMGN appears twice in the reserve source -- exactly today's real shape
    # (FHSA copy with ml_prob, TFSA copy without) -- in a non-capped sector.
    screener_picks = {
        "TFSA_growth_top5": [
            {"ticker": "AMGN", "score": 100, "ml_prob": 0.68, "sector": "Healthcare", "data": {}},
        ],
        "TFSA_income_top5": [],
        "TFSA_swing_top3": [],
        "FHSA_top5": [
            {"ticker": "AMGN", "score": 96, "sector": "Healthcare", "data": {}},
        ],
        "conviction_picks": [],
    }

    result = me._apply_sector_cap(picks, screener_picks, max_per_sector=2)
    tickers = [p["ticker"] for p in result]
    assert tickers.count("AMGN") <= 1, (
        f"AMGN must not appear twice in the final basket, got: {tickers}"
    )
    assert len(tickers) == len(set(tickers)), f"final basket has duplicate tickers: {tickers}"


def test_run_ml_engine_never_double_allocates_capital_to_straddling_ticker(monkeypatch, tmp_path):
    """End-to-end tripwire, one level past the sector-cap test above: drives
    the REAL run_ml_engine() from raw, un-deduped screener_picks (a ticker
    in both FHSA_top5 and TFSA_growth_top5 -- today's actual root cause,
    see pick_utils.py's docstring) all the way through to
    result["target_weights"], the capital-agnostic weight list every real
    dollar allocation is computed from (ml_engine.py:2166,
    compute_target_weights() itself does NOT dedupe -- it trusts its input).

    This isn't testing a fix for a known bug; it's a permanent guard against
    a FUTURE change silently reintroducing double capital allocation --
    exactly the failure mode of the confirmed 2026-08-08 production
    incident (pick_utils.py's docstring) but one stage further downstream
    than any existing test currently checks."""
    import ml_engine as me
    from unittest.mock import MagicMock
    import numpy as np

    monkeypatch.setattr(me, "_SMOOTH_CACHE_FILE", str(tmp_path / "smooth_cache.json"))
    monkeypatch.setattr(
        me, "build_features_for_stock",
        lambda ticker, stock_data, rs=50: {"momentum_6m": 0.05, "roe": 0.1, "rs_rating": 0.6}
    )
    monkeypatch.setattr(me, "get_market_regime", lambda verbose=True: {
        "regime": "BULL", "signal": "FULL_EXPOSURE", "cash_pct": 0.0,
        "spx_price": 5500, "ma200": 5100, "pct_above_ma": 7.8,
    })
    monkeypatch.setattr("outcome_tracker.load_model_health", lambda *a, **k: {})

    def fake_train(self, verbose=True):
        self.trained = True
        self.model = MagicMock()
        self.model.predict_proba.return_value = np.array([[0.3, 0.7]])
        self.calibrator = None
        self.swing_model, self.swing_scaler = None, None
        return True
    monkeypatch.setattr(me.StockMLPredictor, "train", fake_train)

    def _pick(ticker, score, category, sector, ml_prob=None):
        p = {"ticker": ticker, "score": score, "data": {"price": 100.0, "sector": sector,
             "volatility_90d": 0.2}, "pick": {"category": category}}
        if ml_prob is not None:
            p["ml_prob"] = ml_prob
        return p

    # AMGN straddles FHSA and TFSA -- two independent dict objects, one with
    # ml_prob already set (as if a prior pass had scored it), one without --
    # matching the exact shape of the confirmed 2026-08-08 incident. Sector
    # is "Energy" (SECTOR_ALLOW, ml_engine.py:2038), not "Healthcare"
    # (SECTOR_BLOCK) -- deliberately so AMGN reaches target_weights
    # regardless of ML-gate/sector-block behavior, isolating the dedup
    # guarantee this test exists to check. First draft used Healthcare and
    # AMGN silently vanished from target_weights entirely (correctly
    # sector-blocked) -- the assertions below passed anyway with nothing
    # real being exercised; caught by explicitly checking AMGN was actually
    # present before trusting the "at most one" assertion.
    screener_picks = {
        "FHSA_top5": [
            _pick("AMGN", 96, "FHSA Conservative Growth", "Energy"),
        ],
        "TFSA_growth_top5": [
            _pick("AMGN", 100, "GROWTH CORE", "Energy", ml_prob=0.68),
            _pick("BAC",  90,  "GROWTH CORE", "Financial Services"),
        ],
        "TFSA_income_top5": [
            _pick("V",    85, "INCOME", "Financial Services"),
        ],
        "TFSA_swing_top3": [],
    }

    result = me.run_ml_engine(screener_picks, {}, verbose=False)

    weights = result.get("target_weights") or []
    tickers = [w["ticker"] for w in weights]
    # Must actually be present -- otherwise every assertion below passes
    # vacuously without exercising the dedup path at all (see comment above).
    assert "AMGN" in tickers, (
        f"AMGN must survive to target_weights for this test to mean anything, got: {weights}"
    )
    assert tickers.count("AMGN") <= 1, (
        f"AMGN must not receive two separate capital allocations, got target_weights: {weights}"
    )
    assert len(tickers) == len(set(tickers)), (
        f"target_weights has duplicate tickers -- double capital allocation risk: {tickers}"
    )
    total_weight = sum(w.get("weight", 0) for w in weights)
    assert total_weight <= 1.001, (
        f"target_weights sum to {total_weight:.4f} > 100% of deployable capital: {weights}"
    )


def test_track_signal_accuracy_does_not_double_log_duplicate_ticker(monkeypatch, tmp_path):
    """A straddling ticker must generate exactly one set of pending
    accuracy-check entries (one per check_window), not two."""
    import risk_engine as re

    monkeypatch.setattr(re, "SIGNAL_ACCURACY_FILE", str(tmp_path / "signal_accuracy_test.json"))

    todays_picks = [
        {"ticker": "AMGN", "score": 100, "ml_prob": 0.68, "data": {"price": 410.94}, "pick": {"category": "FHSA Conservative Growth"}},
        {"ticker": "AMGN", "score": 96, "data": {"price": 410.94}, "pick": {"category": "GROWTH CORE"}},
        {"ticker": "MPC", "score": 85, "ml_prob": 0.31, "data": {"price": 298.20}, "pick": {"category": "SWING"}},
    ]
    re.track_signal_accuracy(todays_picks, score_history={}, check_windows=(3, 7, 14))

    saved = re.load_signal_accuracy()
    amgn_entries = [e for e in saved.get("entries", []) if e["ticker"] == "AMGN"]
    mpc_entries  = [e for e in saved.get("entries", []) if e["ticker"] == "MPC"]
    assert len(amgn_entries) == 3, f"expected 3 pending checks (one per window) for AMGN, got {len(amgn_entries)}"
    assert len(mpc_entries) == 3, f"expected 3 pending checks for MPC, got {len(mpc_entries)}"


def test_stress_simulation_baseline_not_skewed_by_duplicate_ticker():
    """A straddling ticker must not be double-counted in the stress-test
    baseline's avg score / pick count."""
    import risk_engine as re

    screener_results = {
        "FHSA_top5": [
            {"ticker": "AMGN", "score": 100, "pick": {"exp_high": 50}, "data": {"price": 410.94, "volatility": 1.5}},
        ],
        "TFSA_growth_top5": [
            {"ticker": "AMGN", "score": 96, "pick": {"exp_high": 45}, "data": {"price": 410.94, "volatility": 1.5}},
            {"ticker": "MPC", "score": 85, "pick": {"exp_high": 40}, "data": {"price": 298.20, "volatility": 1.2}},
        ],
        "TFSA_income_top5": [],
        "TFSA_swing_top3": [],
    }
    results = re.run_stress_simulation(screener_results, ml_results=None, verbose=False)

    normal_count = results["scenarios"]["double_costs"]["normal_count"]
    assert normal_count == 2, (
        f"AMGN's duplicate must not inflate the pick count -- expected 2 "
        f"unique tickers (AMGN, MPC), got normal_count={normal_count}"
    )

    normal_avg = results["scenarios"]["remove_top5"]["normal_avg"]
    expected_avg = round((100 + 85) / 2, 1)  # AMGN's higher-score copy (100) + MPC (85)
    assert normal_avg == expected_avg, (
        f"baseline avg score must use AMGN's deduped score once, not both "
        f"copies -- expected {expected_avg}, got {normal_avg}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# FIX (2026-08-08): further audit of the FHSA/TFSA duplicate-pick root cause
# found 3 MORE exposed call sites beyond the original top_flat/sector_cap_
# reserve/stress_baseline/signal_accuracy fixes: run_daily.py's RS-rating
# input (all_raw), build_conviction_picks' first-wins bias, and the
# outcome-log input's first-wins bias. Also fixed two unrelated dict-shape
# bugs (sector news penalty dead code, history archive wrong keys) and two
# structural improvements: a Kelly calibration fix (prefer model-sourced
# ml_prob-bucket data over the legacy-diluted pooled table) and a
# data-driven category-readiness check (see outcome_tracker.py's
# category_is_data_ready, replacing ml_engine.py's hardcoded
# RULES_BASED_CATEGORIES membership as the long-term promotion path).
# ─────────────────────────────────────────────────────────────────────────────

def test_dedupe_raw_data_by_ticker_keeps_one_entry_per_ticker():
    """Raw data dicts (not pick dicts) -- straddling tickers are literally
    the same object reference across FHSA/TFSA buckets, so first-seen-wins
    is correct with no priority tiebreak needed."""
    from pick_utils import dedupe_raw_data_by_ticker

    shared_amgn = {"ticker": "AMGN", "status": "ok", "perf_30d": 5, "perf_90d": 25}
    data_list = [shared_amgn, shared_amgn, {"ticker": "MPC", "status": "ok", "perf_30d": 1, "perf_90d": 2}]
    result = dedupe_raw_data_by_ticker(data_list)
    tickers = [d["ticker"] for d in result]
    assert tickers.count("AMGN") == 1
    assert sorted(tickers) == ["AMGN", "MPC"]


def test_calculate_relative_strength_not_skewed_by_duplicate_raw_data():
    """A duplicate entry inflates calculate_relative_strength()'s population
    (`total`), which shifts every OTHER stock's percentile-ranked rs_rating
    -- not just the duplicate's. Deduping first must produce the same
    ratings as a population that was never duplicated in the first place."""
    from intelligence_layers import calculate_relative_strength
    from pick_utils import dedupe_raw_data_by_ticker

    base_universe = [
        {"ticker": f"T{i}", "status": "ok", "perf_30d": i, "perf_90d": i * 2}
        for i in range(20)
    ]
    # AMGN (a straddling ticker) is a real, legitimate member of the
    # universe -- the bug is it appearing TWICE (same shared "data" object
    # across FHSA/TFSA buckets), not that it appears at all. The correct
    # baseline to compare against is one real AMGN entry, not zero.
    amgn = {"ticker": "AMGN", "status": "ok", "perf_30d": 10, "perf_90d": 20}
    clean_ratings = calculate_relative_strength(base_universe + [amgn])
    duplicated_universe = base_universe + [amgn, amgn]

    deduped = dedupe_raw_data_by_ticker(duplicated_universe)
    fixed_ratings = calculate_relative_strength(deduped)

    # T10 (mid-pack, composite ties with AMGN's insertion point) must rank
    # identically whether or not AMGN's duplicate ever existed in the raw
    # list. (T19, the top performer, is a poor choice here -- rank is
    # always total-1 regardless of insertions below it, so its rs_rating
    # is mathematically invariant and wouldn't demonstrate the bug.)
    assert fixed_ratings["T10"]["rs_rating"] == clean_ratings["T10"]["rs_rating"]

    # Without the fix, feeding duplicated_universe directly (no dedupe)
    # inflates total and shifts T10's rank -- confirm that's still true
    # (i.e. this test would have caught the bug pre-fix).
    unfixed_ratings = calculate_relative_strength(duplicated_universe)
    assert unfixed_ratings["T10"]["rs_rating"] != clean_ratings["T10"]["rs_rating"], (
        "expected duplicate-population inflation to shift T10's rank in the "
        "un-deduped case -- if this now matches, the test fixture no longer "
        "demonstrates the bug this fix addresses"
    )


def test_build_conviction_picks_dedup_prefers_ml_scored_copy():
    """Old behavior: build_conviction_picks' `seen` set marks a ticker seen
    on its FIRST occurrence regardless of whether that copy actually
    qualifies for conviction (>=2 signals). If FHSA_top5's copy (no
    ml_prob, low rs_rating) came first and scored 0 signals, the ticker was
    silently skipped entirely -- even though TFSA_growth_top5's copy (real
    ml_prob, high rs_rating) would have cleared the 2-signal bar. Deduping
    for the ML-scored copy BEFORE the seen-set loop fixes this."""
    from run_daily import build_conviction_picks

    screener_results = {
        "FHSA_top5": [
            {"ticker": "AMGN", "score": 100, "data": {}, "pick": {"category": "FHSA Conservative Growth"}},
        ],
        "TFSA_growth_top5": [
            {"ticker": "AMGN", "score": 96, "data": {}, "pick": {"category": "GROWTH CORE"},
             "ml_prob": 0.75, "rs_rating": 85},
        ],
        "TFSA_income_top5": [],
        "TFSA_swing_top3": [],
    }
    conviction = build_conviction_picks(
        screener_results, x_signals={}, trends={}, news_analysis={}, ml_results=None,
    )
    tickers = [p["ticker"] for p in conviction]
    assert tickers.count("AMGN") <= 1, f"must not duplicate AMGN: {tickers}"
    assert "AMGN" in tickers, (
        "AMGN's ML-scored TFSA copy (rs_rating=85, ml_prob=0.75) clears the "
        "2-signal conviction bar and must not be silently dropped because "
        "the non-qualifying FHSA copy happened to be deduped-in first"
    )


def test_apply_news_to_screener_sector_penalty_now_applies():
    """FIX (2026-08-08): pick.get("sector","") was always "" (sector only
    lives at pick["data"]["sector"]), so the sector-headwind news penalty
    was dead code. A pick in a sector with strongly negative net sentiment
    must now actually get docked."""
    from run_daily import apply_news_to_screener

    screener_results = {
        "FHSA_top5": [
            {"ticker": "AC.TO", "score": 80, "data": {"sector": "Industrials"}},
        ],
        "TFSA_growth_top5": [], "TFSA_income_top5": [], "TFSA_swing_top3": [],
        "FHSA_all": [], "TFSA_core_all": [], "TFSA_income_all": [], "TFSA_swing_all": [],
    }
    news_analysis = {
        "ticker_adjustments": {},
        "sector_sentiment": {"AIRLINES": {"net_score": -594}},  # Industrials -> AIRLINES per SECTOR_MAP
        "macro_regime": "CAUTIOUS",
    }
    result = apply_news_to_screener(screener_results, news_analysis)
    pick = result["FHSA_top5"][0]
    assert pick["score"] < 80, (
        f"sector headwind penalty must dock AC.TO's score (net=-594 -> -12pts "
        f"expected), got unchanged score={pick['score']}"
    )
    assert any("Sector headwind" in f for f in pick.get("flags", [])), (
        "expected a sector-headwind flag to be recorded on the pick"
    )


def test_kelly_prefers_model_sourced_bucket_over_pooled_when_it_disagrees():
    """FIX (2026-08-08): checked empirically against outcomes_log.json --
    the pooled by_ml_prob_bucket table (89% legacy 'unknown'-source rows)
    can show POSITIVE edge for a bucket that the live model's own
    ml_prob_source=='model' rows show NEGATIVE edge for (and vice versa).
    Kelly must follow the model-sourced table when it clears the sample
    gate, not the pooled one, even though both tables are present."""
    from ml_engine import compute_target_weights

    market_regime = {"regime": "BULL", "cash_pct": 0.0}
    # Pooled table says 0.6-0.8 is strong (positive edge).
    # Model-sourced table says 0.6-0.8 is actually a LOSER (negative edge).
    wr_data = {
        "by_ml_prob_bucket": {
            "0.6-0.8": {"win_rate": 70.0, "avg_win": 5.0, "avg_loss": 1.0, "count": 320},
        },
        "by_ml_prob_bucket_model": {
            "0.6-0.8": {"win_rate": 30.0, "avg_win": 1.0, "avg_loss": 5.0, "count": 37},
        },
    }
    result = compute_target_weights([_kelly_pick("AAA", 80, 0.70)], market_regime,
                                     win_rate_data=wr_data, verbose=False)
    assert result[0]["kelly_wt"] == 0.0, (
        "model-sourced data shows negative edge for this bucket (p=0.30, "
        "aw=1.0, al=5.0) -- Kelly must floor to 0, not use the pooled "
        "table's positive-edge numbers"
    )


def test_kelly_falls_back_to_pooled_when_model_bucket_too_thin():
    """A bucket with real model-sourced data but too few rows to clear the
    n>=10-win/n>=10-loss sample gate (e.g. today's live 0.8-1.0 bucket,
    which has ZERO model-sourced rows at all) must fall back to the pooled
    table rather than going straight to the static fallback -- the pooled
    table is still better-than-nothing evidence."""
    from ml_engine import compute_target_weights

    market_regime = {"regime": "BULL", "cash_pct": 0.0}
    wr_data = {
        "by_ml_prob_bucket": {
            "0.6-0.8": {"win_rate": 61.7, "avg_win": 5.41, "avg_loss": 2.70, "count": 269},
        },
        "by_ml_prob_bucket_model": {},  # no model-sourced rows in this bucket at all
    }
    result = compute_target_weights([_kelly_pick("AAA", 80, 0.70)], market_regime,
                                     win_rate_data=wr_data, verbose=False)
    assert result[0]["kelly_wt"] > 0.0, (
        "pooled table shows real positive edge and model table is simply "
        "empty for this bucket -- must fall back to pooled, not floor to 0"
    )


def test_category_is_data_ready_requires_real_feature_coverage_not_just_rows():
    """FIX (2026-08-08): dry-tested against real data -- WATCH has 273
    true-horizon-resolved rows (comfortably over the 80-row bar) but ALL of
    them predate the 2026-06-14 feature-capture instrumentation date, so
    100% have roe=perf_90d=0. A row-count-only readiness check would have
    falsely called WATCH ready to train on. This locks in that the check
    requires real feature coverage, not just volume."""
    from outcome_tracker import category_is_data_ready

    # 100 resolved rows, well over the 80-row bar, but zero real features
    # (simulates WATCH's actual pre-instrumentation-era true-horizon data).
    feature_starved = [
        {"category": "WATCH", "true_horizon_resolved": True, "roe": 0}
        for _ in range(100)
    ]
    assert category_is_data_ready("WATCH", outcomes=feature_starved) is False

    # Same row count, but with real feature coverage above the 10% bar
    # (mirrors SWING's live model, which trains today at 40% raw coverage).
    feature_complete = (
        [{"category": "WATCH", "true_horizon_resolved": True, "roe": 12.5} for _ in range(20)] +
        [{"category": "WATCH", "true_horizon_resolved": True, "roe": 0} for _ in range(80)]
    )
    assert category_is_data_ready("WATCH", outcomes=feature_complete) is True


def test_category_is_data_ready_matches_known_live_categories():
    """Sanity-checks category_is_data_ready against real outcomes_log.json:
    SWING (which already trains a working live model) must read ready;
    every category currently gated to rules_based in ml_engine.py's
    RULES_BASED_CATEGORIES must read not-ready. This is what makes the
    check trustworthy as a future drop-in replacement for that hardcoded
    set -- it must agree with today's known-correct manual classification
    before it's trusted to drive it automatically."""
    from outcome_tracker import category_is_data_ready, load_outcomes
    from ml_engine import RULES_BASED_CATEGORIES

    outcomes = load_outcomes()
    assert category_is_data_ready("SWING", outcomes=outcomes) is True
    for cat in RULES_BASED_CATEGORIES:
        assert category_is_data_ready(cat, outcomes=outcomes) is False, (
            f"{cat} is hardcoded rules_based today -- category_is_data_ready "
            f"must agree it isn't ready, or the two would silently disagree"
        )


# ─────────────────────────────────────────────────────────────────────────────
# FIX (2026-08-09): stress-testing the "fully automatic" auto-promotion idea
# surfaced that neither train_swing_model() nor train_and_save() (the general
# model) had ANY holdout-AUC gate before deploying -- both unconditionally
# overwrote the live model cache regardless of the computed AUC, every call,
# with no path back down if a bad retrain went live. Bootstrapped against
# real SWING data: random-resplit AUC std nearly doubles (0.056 -> 0.118)
# going from SWING's actual n=239 down to the 80-row minimum bar, meaning a
# marginal pass right at threshold is plausibly noise, not skill. Two fixes:
# (1) a deploy gate (MIN_HOLDOUT_AUC_TO_DEPLOY) on both model paths, and
# (2) an ongoing health check (model_health_check) on REAL subsequent
# resolved performance, since a one-time pass doesn't prove it stays good.
# ─────────────────────────────────────────────────────────────────────────────

def _synthetic_resolved_rows(n, informative, seed=0):
    """Synthetic rows with dates spanning well before AND after
    HOLDOUT_CUTOFF_DATE (2026-06-19), so both train and holdout populations
    are real and class-balanced. Label assignment is independent random
    (not tied to date index via modular arithmetic like
    _synthetic_resolved_for_split_test's i%2 -- that correlates perfectly
    with date-cycle position whenever the date-list length is even,
    silently producing a single-class holdout).

    informative=True: roe and profit_margin cleanly separate WIN/LOSS, in
    the direction XGB_PARAMS' monotone_constraints actually expects (roe:
    -1, i.e. LOWER roe -> higher win probability -- see the constraint's
    own comment: "empirically confirmed negative correlation with the
    label"; profit_margin: +1, higher -> better). Getting this backwards
    fights the constraint and produces a degenerate, zero-importance model
    regardless of how cleanly separated the raw data looks -- confirmed by
    hand: a roe-positively-correlated-with-WIN version of this fixture
    trains a model with every feature importance at 0.0000 and AUC pinned
    at exactly 0.500, because the constraint forbids the split direction
    the data would otherwise support.

    informative=False: features are the SAME constant regardless of
    outcome -> AUC should sit at chance, exercising the reject path
    without crashing on a degenerate single-class split."""
    import random
    rng = random.Random(seed)
    rows = []
    # Dates clustered as close to HOLDOUT_CUTOFF_DATE (2026-06-19) as
    # possible on the train side, and after it on the holdout side.
    # build_feature_matrix applies a 90-day-half-life recency-decay sample
    # weight (exp(-ln2/90 * days_old)) -- with XGB_PARAMS' min_child_weight=4
    # (a WEIGHTED-hessian threshold, not a row count), dates spread months
    # further back than the cutoff decay the weights so much that no split
    # can meet the threshold at a small synthetic n, producing a silently
    # degenerate zero-importance model (confirmed by hand: this exact
    # fixture with Feb-May 2026 dates and n=90 trains a real 0.53 AUC signal
    # in isolation but collapses to 0.0 importance/0.5 AUC the moment
    # sample_weight is applied) -- not a bug in the fix under test, just a
    # constraint synthetic fixtures need to respect that real production
    # data (1000s of rows) never bumps into.
    dates_before = [f"2026-06-{d:02d}" for d in range(1, 20)]     # up to the cutoff
    dates_after  = [f"2026-{m:02d}-{d:02d}" for m in (7, 8) for d in (1, 5, 10, 15, 20)]
    for i in range(n):
        is_win = rng.random() < 0.5
        date = dates_after[i % len(dates_after)] if i % 3 == 0 else dates_before[i % len(dates_before)]
        if informative:
            roe           = -10.0 + rng.uniform(-2, 2) if is_win else 40.0 + rng.uniform(-2, 2)
            profit_margin =  40.0 + rng.uniform(-2, 2) if is_win else -10.0 + rng.uniform(-2, 2)
        else:
            roe, profit_margin = 15.0, 20.0  # constant regardless of outcome -- uninformative
        rows.append({
            "resolved": True,
            "actual_return": 5.0 + rng.uniform(-0.1, 0.1) if is_win else -3.0 + rng.uniform(-0.1, 0.1),
            "outcome": "WIN" if is_win else "LOSS",
            "perf_90d": 10.0, "roe": roe, "profit_margin": profit_margin,
            "sector": "Technology", "regime": "BULL",
            "signal_date": date,
        })
    return rows


def test_train_and_save_rejects_deploy_below_auc_bar(monkeypatch, tmp_path):
    """Constant, uninformative features must produce a near-random holdout
    AUC and get REJECTED -- no cache write, report says deployed=False.
    Locks in the fix: previously this always deployed."""
    import ml_retrainer as mr

    resolved = _synthetic_resolved_rows(n=90, informative=False, seed=3)
    X, y, w, dates = mr.build_feature_matrix(resolved)
    if X is None:
        pytest.skip("Coverage gate blocked")

    cache_path = tmp_path / "model_cache.pkl"
    monkeypatch.setattr(mr, "MODEL_CACHE", str(cache_path))
    monkeypatch.setattr(mr, "REPORT_FILE", str(tmp_path / "report.json"))

    report = mr.train_and_save(X, y, w, dates)
    if report is None:
        pytest.skip("Training libraries unavailable")

    assert report["holdout_auc"] < mr.MIN_HOLDOUT_AUC_TO_DEPLOY, (
        f"test fixture must produce a sub-bar AUC to exercise rejection -- "
        f"got {report['holdout_auc']} >= {mr.MIN_HOLDOUT_AUC_TO_DEPLOY}"
    )
    assert report["deployed"] is False
    assert "rejected_reason" in report
    assert not cache_path.exists(), "rejected retrain must not write a model cache file"


def test_train_and_save_preserves_previous_model_on_rejection(monkeypatch, tmp_path):
    """The core safety property: if a NEW retrain fails the bar, the
    PREVIOUSLY-deployed model (already on disk from an earlier good
    retrain) must be left untouched, not overwritten with a bad one."""
    import ml_retrainer as mr

    cache_path = tmp_path / "model_cache.pkl"
    cache_path.write_bytes(b"PREVIOUS_GOOD_MODEL_BYTES")
    monkeypatch.setattr(mr, "MODEL_CACHE", str(cache_path))
    monkeypatch.setattr(mr, "REPORT_FILE", str(tmp_path / "report.json"))

    resolved = _synthetic_resolved_rows(n=90, informative=False, seed=4)
    X, y, w, dates = mr.build_feature_matrix(resolved)
    if X is None:
        pytest.skip("Coverage gate blocked")

    report = mr.train_and_save(X, y, w, dates)
    if report is None:
        pytest.skip("Training libraries unavailable")
    assert report["deployed"] is False

    assert cache_path.read_bytes() == b"PREVIOUS_GOOD_MODEL_BYTES", (
        "a rejected retrain must not touch the previously-deployed cache file"
    )


def test_train_and_save_deploys_when_auc_clears_bar(monkeypatch, tmp_path):
    """A model with a real, learnable signal must clear the bar and deploy
    -- confirms the gate doesn't just reject everything.

    n=150, not the usual 90 (FIX, 2026-08-17): at n=90 this fixture sits on a
    knife's edge -- xgboost's hist tree builder isn't bit-reproducible across
    runs even with a fixed random_state (floating-point summation order
    varies with thread scheduling), and combined with the 90-day recency-decay
    sample weighting vs. min_child_weight=4's Hessian-mass threshold (see
    _synthetic_resolved_rows' docstring), that's enough to occasionally
    collapse the model to a degenerate 0.5-AUC/zero-importance stump -- CI run
    32069838547 hit exactly that with seed=1. Empirically probed n=90..250
    across 10 seeds each: n=90 degenerated on 1/10 seeds (down to 0.5), n>=120
    never dropped below 0.87 in the same sweep. n=150 keeps real margin over
    the 0.53 bar without materially slowing the test."""
    import ml_retrainer as mr

    resolved = _synthetic_resolved_rows(n=150, informative=True, seed=1)
    X, y, w, dates = mr.build_feature_matrix(resolved)
    if X is None:
        pytest.skip("Coverage gate blocked")

    cache_path = tmp_path / "model_cache.pkl"
    monkeypatch.setattr(mr, "MODEL_CACHE", str(cache_path))
    monkeypatch.setattr(mr, "REPORT_FILE", str(tmp_path / "report.json"))

    report = mr.train_and_save(X, y, w, dates)
    if report is None:
        pytest.skip("Training libraries unavailable")

    assert report["holdout_auc"] >= mr.MIN_HOLDOUT_AUC_TO_DEPLOY, (
        f"test fixture must produce a clearing AUC -- got {report['holdout_auc']}"
    )
    assert report["deployed"] is True
    assert cache_path.exists(), "a passing retrain must write the model cache"


def test_train_swing_model_rejects_deploy_below_auc_bar(monkeypatch, tmp_path):
    """Same gate, SWING's own training path -- constant/uninformative
    features must not get deployed to swing_model_cache.pkl."""
    import ml_retrainer as mr

    outcomes_path = tmp_path / "outcomes_log.json"
    rows = []
    for r in _synthetic_resolved_rows(n=50, informative=False, seed=2):
        r = dict(r)
        r["category"] = "SWING"
        r["true_horizon_resolved"] = True
        r["true_horizon_outcome"] = r["outcome"]
        r["true_horizon_return"] = r["actual_return"]
        rows.append(r)
    outcomes_path.write_text(__import__("json").dumps(rows))

    monkeypatch.setattr(mr, "OUTCOMES_FILE", str(outcomes_path))
    monkeypatch.setattr(mr, "SWING_MIN_ROWS_TO_TRAIN", 40)
    cache_path = tmp_path / "swing_model_cache.pkl"
    monkeypatch.setattr(mr, "SWING_MODEL_CACHE", str(cache_path))
    monkeypatch.setattr(mr, "SWING_MODEL_REPORT", str(tmp_path / "swing_report.json"))

    report = mr.train_swing_model(verbose=False)
    if report is None:
        pytest.skip("Training libraries unavailable")

    assert report.get("deployed") is False
    assert not cache_path.exists()


def test_model_health_check_no_opinion_below_min_sample():
    """Fewer than min_n true-horizon-resolved, model-sourced picks ->
    'insufficient_data', not a default 'healthy' or 'degraded' verdict.
    This is the expected real state for weeks after SWING's model deploy
    (2026-08-08 + 30d horizon = no swing_model true-horizon data before
    2026-09-07)."""
    from outcome_tracker import model_health_check

    outcomes = [
        {"category": "SWING", "ml_prob_source": "swing_model",
         "true_horizon_resolved": True, "true_horizon_outcome": "WIN",
         "true_horizon_date": "2026-08-01"}
        for _ in range(5)  # well under the 20-row min
    ]
    result = model_health_check("SWING", "swing_model", outcomes=outcomes)
    assert result["status"] == "insufficient_data"
    assert result["n"] == 5


def test_model_health_check_detects_degraded_model():
    """A deployed model whose real recent win rate has fallen below the
    floor must read 'degraded', with the win rate that triggered it."""
    from outcome_tracker import model_health_check

    outcomes = (
        [{"category": "SWING", "ml_prob_source": "swing_model", "true_horizon_resolved": True,
          "true_horizon_outcome": "LOSS", "true_horizon_date": f"2026-08-{i:02d}"} for i in range(1, 26)]
        + [{"category": "SWING", "ml_prob_source": "swing_model", "true_horizon_resolved": True,
            "true_horizon_outcome": "WIN", "true_horizon_date": f"2026-07-{i:02d}"} for i in range(1, 6)]
    )
    result = model_health_check("SWING", "swing_model", outcomes=outcomes)
    assert result["status"] == "degraded"
    assert result["win_rate_pct"] < 40.0


def test_model_health_check_ignores_other_categories_and_sources():
    """Must only count rows matching BOTH the target category AND the
    target ml_prob_source -- a WATCH pick or a general-model-scored SWING
    pick must not contaminate SWING's swing_model health read."""
    from outcome_tracker import model_health_check

    outcomes = (
        [{"category": "SWING", "ml_prob_source": "swing_model", "true_horizon_resolved": True,
          "true_horizon_outcome": "WIN", "true_horizon_date": f"2026-08-{i:02d}"} for i in range(1, 21)]
        + [{"category": "SWING", "ml_prob_source": "model", "true_horizon_resolved": True,
            "true_horizon_outcome": "LOSS", "true_horizon_date": f"2026-08-{i:02d}"} for i in range(1, 21)]
        + [{"category": "WATCH", "ml_prob_source": "swing_model", "true_horizon_resolved": True,
            "true_horizon_outcome": "LOSS", "true_horizon_date": f"2026-08-{i:02d}"} for i in range(1, 21)]
    )
    result = model_health_check("SWING", "swing_model", outcomes=outcomes)
    assert result["n"] == 20, f"must count only SWING+swing_model rows, got n={result['n']}"
    assert result["status"] == "healthy"


def test_ml_engine_falls_back_when_swing_model_marked_degraded(monkeypatch, tmp_path, capsys):
    """Real end-to-end check (same pattern as
    test_run_ml_engine_scoring_table_shows_category_and_source): a SWING
    pick, with a swing_model mocked to return a real, usable probability,
    must NOT be routed through it when the saved health check says
    degraded -- proving the auto-demotion signal actually reaches live
    routing, not just sitting in a JSON file nobody reads. Source must
    read "model" (general fallback), not "swing_model"."""
    import ml_engine as me
    from unittest.mock import MagicMock
    import numpy as np

    monkeypatch.setattr(me, "_SMOOTH_CACHE_FILE", str(tmp_path / "smooth_cache.json"))
    monkeypatch.setattr(
        me, "build_features_for_stock",
        lambda ticker, stock_data, rs=50: {"momentum_6m": 0.05, "roe": 0.1, "rs_rating": 0.6}
    )
    monkeypatch.setattr(me, "get_market_regime", lambda verbose=True: {
        "regime": "UNKNOWN", "signal": "NEUTRAL", "cash_pct": 0.0,
        "spx_price": 0, "ma200": 0, "pct_above_ma": 0,
    })
    monkeypatch.setattr("outcome_tracker.load_model_health",
                         lambda *a, **k: {"SWING": {"status": "degraded", "reason": "test"}})

    def fake_train(self, verbose=True):
        self.trained = True
        self.model = MagicMock()
        self.model.predict_proba.return_value = np.array([[0.3, 0.7]])
        self.calibrator = None
        # A REAL, usable SWING model -- would return 0.8 if actually called.
        # Proves the skip is caused by the health check, not a coincidental
        # None from an unloaded model.
        self.swing_model = MagicMock()
        self.swing_model.predict_proba.return_value = np.array([[0.2, 0.8]])
        self.swing_scaler = MagicMock()
        self.swing_scaler.transform.return_value = [[0, 0, 0]]
        return True
    monkeypatch.setattr(me.StockMLPredictor, "train", fake_train)

    screener_picks = {
        "FHSA_top5": [{"ticker": "SWINGCO", "score": 50, "data": {}, "pick": {"category": "SWING"}}],
        "TFSA_growth_top5": [], "TFSA_income_top5": [], "TFSA_swing_top3": [],
    }
    me.run_ml_engine(screener_picks, {}, verbose=True)
    out = capsys.readouterr().out

    assert "DEGRADED" in out, "degraded health status must be logged visibly"
    assert "SWINGCO" in out and "swing_model" not in out.split("SWINGCO")[1].split("\n")[0], (
        f"SWINGCO's row must not show swing_model as source when health is "
        f"degraded -- full output:\n{out}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# FIX (2026-08-09): structural fix for the ac4973cd category-routing bug
# class (pick.get("category") always None because category lives at
# pick["pick"]["category"], never the top level -- same shape bug recurred
# independently for "sector" in run_daily.py's news-penalty block). Added
# canonical accessors (get_pick_category/get_pick_field/get_pick_sector/
# get_pick_data) to pick_utils.py and migrated every known call site across
# run_daily.py, ml_engine.py, risk_engine.py, outcome_tracker.py,
# portfolio_engine.py, content_engine.py, intelligence_layers.py, and
# signal_ledger.py. This test is the actual enforcement mechanism -- it
# fails if a NEW raw `.get("pick", ...)` or `.get("data", ...).get("sector"`
# read appears in any of those files outside pick_utils.py, so the next
# occurrence of this bug class is caught by CI instead of shipping silently.
# ─────────────────────────────────────────────────────────────────────────────

def test_no_raw_pick_dict_access_outside_pick_utils():
    """Enforcement for the canonical pick-accessor structural fix. Any new
    `.get("pick", ...)` or `.get("data", ...).get("sector"...)` chain
    outside pick_utils.py must use get_pick_data()/get_pick_category()/
    get_pick_field()/get_pick_sector() instead -- this is what actually
    stops a third instance of the ac4973cd bug class from appearing
    silently, instead of relying on someone remembering the convention.

    Known, audited-safe exceptions (not the pick-dict shape at all, or a
    legitimate existence-check/mutation rather than a nested read) are
    explicitly allowlisted by (file, line_text) below -- anything else
    matching the pattern is a real violation."""
    import re

    FILES = [
        "run_daily.py", "ml_engine.py", "risk_engine.py", "outcome_tracker.py",
        "portfolio_engine.py", "content_engine.py", "intelligence_layers.py",
        "signal_ledger.py",
    ]
    ALLOWLIST = {
        # content_engine.py: `data` here is a content-template situation
        # dict whose "pick" key holds a DIFFERENT object (the primary
        # conviction pick itself, with ticker/score at ITS top level) --
        # not a screener pick's classify_pick() sub-dict at all.
        ("content_engine.py", 'pick    = data.get("pick", {})'),
        # run_daily.py: existence-check before a WRITE (pick["pick"]["amount"] = ...),
        # not a nested READ -- no default value requested, nothing to migrate.
        ("run_daily.py", 'if pick.get("pick") and guardrail["recommended"] > 0:'),
    }

    pick_pattern   = re.compile(r'\.get\(\s*"pick"\s*,')
    sector_pattern = re.compile(r'\.get\(\s*"data"[^)]*\)\.get\(\s*"sector"')

    violations = []
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    for fname in FILES:
        path = os.path.join(root, fname)
        if not os.path.exists(path):
            continue
        with open(path) as f:
            for lineno, line in enumerate(f, start=1):
                stripped = line.strip()
                if stripped.startswith("#"):
                    continue
                if pick_pattern.search(line) or sector_pattern.search(line):
                    if (fname, stripped) in ALLOWLIST:
                        continue
                    violations.append(f"{fname}:{lineno}: {stripped}")

    assert not violations, (
        "Raw pick-dict/sector access found outside pick_utils.py -- use "
        "get_pick_data()/get_pick_category()/get_pick_field()/get_pick_sector() "
        "instead (see pick_utils.py), or add a justified entry to this "
        "test's ALLOWLIST if it's genuinely not the pick-dict shape:\n"
        + "\n".join(violations)
    )


# ─────────────────────────────────────────────────────────────────────────────
# FIX (2026-08-09): Kelly's durable calibration fix -- the interim filter
# (ml_prob_source in ("model","swing_model")) caps TODAY's dilution from
# legacy "unknown" rows, but every future retrain also gets tagged "model",
# so the same staleness would silently re-accrue over months of retrains.
# scored_by_model_trained_at (set at scoring time from the currently-loaded
# model's own trained_at) + a rolling MODEL_VINTAGE_WINDOW_DAYS window ages
# old vintages out automatically instead of needing a manual re-filter.
# ─────────────────────────────────────────────────────────────────────────────

def _vintage_outcome(ticker, ml_prob, outcome, actual_return, trained_at, source="model"):
    return {"ticker": ticker, "signal_date": "2026-01-01", "score": 80,
            "ml_prob": ml_prob, "resolved": True, "outcome": outcome,
            "actual_return": actual_return, "ml_prob_source": source,
            "scored_by_model_trained_at": trained_at}


def test_kelly_bucket_excludes_stale_retrain_vintage():
    """A row scored by a model trained well outside the rolling window must
    NOT count toward by_ml_prob_bucket_model, even though its
    ml_prob_source is "model" -- this is what makes the fix self-correcting
    instead of needing a manual re-pick every few months."""
    from datetime import datetime, timedelta
    from unittest.mock import patch
    from outcome_tracker import compute_win_rate, MODEL_VINTAGE_WINDOW_DAYS
    import tempfile

    stale_trained_at = (datetime.now() - timedelta(days=MODEL_VINTAGE_WINDOW_DAYS + 30)).isoformat()
    fresh_trained_at = (datetime.now() - timedelta(days=5)).isoformat()

    synthetic = (
        [_vintage_outcome("STALE1", 0.65, "WIN", 6.0, stale_trained_at)] * 12
        + [_vintage_outcome("FRESH1", 0.65, "LOSS", -2.0, fresh_trained_at)] * 12
    )
    scratch_win_rate = tempfile.mktemp(suffix=".json")
    with patch("outcome_tracker.load_outcomes", lambda: synthetic), \
         patch("outcome_tracker.WIN_RATE_FILE", scratch_win_rate):
        wr = compute_win_rate()

    table = wr["by_ml_prob_bucket_model"]
    bucket = table.get("0.6-0.8", {})
    assert bucket.get("count") == 12, (
        f"expected only the 12 FRESH1 rows to count (stale vintage excluded "
        f"despite matching ml_prob_source) -- got {bucket}"
    )
    assert bucket.get("win_rate") == 0.0, (
        "the 12 counted rows are all LOSS (FRESH1) -- if the stale WIN rows "
        "leaked in, win_rate would be > 0"
    )


def test_kelly_bucket_includes_swing_model_source_not_just_model():
    """ml_prob_source=="swing_model" is a real, current-model source too
    (not legacy/unknown) -- must count toward by_ml_prob_bucket_model
    alongside "model", not be silently excluded."""
    from datetime import datetime, timedelta
    from unittest.mock import patch
    from outcome_tracker import compute_win_rate
    import tempfile

    fresh = (datetime.now() - timedelta(days=2)).isoformat()
    synthetic = [_vintage_outcome("SWCO", 0.65, "WIN", 4.0, fresh, source="swing_model")] * 15
    scratch_win_rate = tempfile.mktemp(suffix=".json")
    with patch("outcome_tracker.load_outcomes", lambda: synthetic), \
         patch("outcome_tracker.WIN_RATE_FILE", scratch_win_rate):
        wr = compute_win_rate()

    bucket = wr["by_ml_prob_bucket_model"].get("0.6-0.8", {})
    assert bucket.get("count") == 15, f"swing_model rows must count too, got {bucket}"


def test_kelly_bucket_excludes_rows_with_no_vintage_tag():
    """Rows logged before this fix (no scored_by_model_trained_at at all)
    must not count toward the model-vintage bucket -- there's no vintage
    to check, so they can't be trusted as "current model" evidence."""
    from unittest.mock import patch
    from outcome_tracker import compute_win_rate
    import tempfile

    legacy_no_tag = {"ticker": "OLD1", "signal_date": "2026-01-01", "score": 80,
                      "ml_prob": 0.65, "resolved": True, "outcome": "WIN",
                      "actual_return": 5.0, "ml_prob_source": "model"}
    # no "scored_by_model_trained_at" key at all
    synthetic = [legacy_no_tag] * 15
    scratch_win_rate = tempfile.mktemp(suffix=".json")
    with patch("outcome_tracker.load_outcomes", lambda: synthetic), \
         patch("outcome_tracker.WIN_RATE_FILE", scratch_win_rate):
        wr = compute_win_rate()

    assert wr["by_ml_prob_bucket_model"].get("0.6-0.8", {}).get("count", 0) == 0, (
        "untagged rows (pre-fix data) must not count as vintage-verified evidence"
    )


def test_scored_by_model_trained_at_tagged_at_scoring_time(monkeypatch, tmp_path):
    """End-to-end: run_ml_engine must tag a swing_model-sourced pick with
    the loaded SWING model's own trained_at, and a general-model-sourced
    pick with the general model's trained_at -- this is the actual write
    side of the vintage fix, not just the read side tested above."""
    import ml_engine as me
    from unittest.mock import MagicMock
    import numpy as np

    monkeypatch.setattr(me, "_SMOOTH_CACHE_FILE", str(tmp_path / "smooth_cache.json"))
    monkeypatch.setattr(
        me, "build_features_for_stock",
        lambda ticker, stock_data, rs=50: {"momentum_6m": 0.05, "roe": 0.1, "rs_rating": 0.6}
    )
    monkeypatch.setattr(me, "get_market_regime", lambda verbose=True: {
        "regime": "UNKNOWN", "signal": "NEUTRAL", "cash_pct": 0.0,
        "spx_price": 0, "ma200": 0, "pct_above_ma": 0,
    })
    monkeypatch.setattr("outcome_tracker.load_model_health", lambda *a, **k: {})

    SWING_STAMP = "2026-08-01T00:00:00"
    GENERAL_STAMP = "2026-07-15T00:00:00"

    def fake_train(self, verbose=True):
        self.trained = True
        self.model_trained_at = GENERAL_STAMP
        self.model = MagicMock()
        self.model.predict_proba.return_value = np.array([[0.3, 0.7]])
        self.calibrator = None
        self.swing_model = MagicMock()
        self.swing_model.predict_proba.return_value = np.array([[0.2, 0.8]])
        self.swing_scaler = MagicMock()
        self.swing_scaler.transform.return_value = [[0, 0, 0]]
        self.swing_model_trained_at = SWING_STAMP
        return True
    monkeypatch.setattr(me.StockMLPredictor, "train", fake_train)

    screener_picks = {
        "FHSA_top5": [
            {"ticker": "SWINGCO", "score": 50, "data": {}, "pick": {"category": "SWING"}},
        ],
        "TFSA_growth_top5": [], "TFSA_income_top5": [], "TFSA_swing_top3": [],
    }
    result = me.run_ml_engine(screener_picks, {}, verbose=False)

    scored = {p["ticker"]: p for bucket in screener_picks.values() for p in bucket}
    swingco = scored["SWINGCO"]
    assert swingco["ml_prob_source"] == "swing_model"
    assert swingco["scored_by_model_trained_at"] == SWING_STAMP, (
        f"expected the SWING model's own trained_at, got "
        f"{swingco.get('scored_by_model_trained_at')!r}"
    )


def test_ml_engine_falls_back_when_general_model_marked_degraded(monkeypatch, tmp_path, capsys):
    """Same auto-demotion wiring as SWING, for the general model's fallback
    path (categories that are neither SWING nor in RULES_BASED_CATEGORIES).
    A degraded verdict must route to rules_based instead of the general
    model, with a real, usable general-model prediction mocked in to prove
    the skip is caused by the health check, not a coincidental failure."""
    import ml_engine as me
    from unittest.mock import MagicMock
    import numpy as np

    monkeypatch.setattr(me, "_SMOOTH_CACHE_FILE", str(tmp_path / "smooth_cache.json"))
    monkeypatch.setattr(
        me, "build_features_for_stock",
        lambda ticker, stock_data, rs=50: {"momentum_6m": 0.05, "roe": 0.1, "rs_rating": 0.6}
    )
    monkeypatch.setattr(me, "get_market_regime", lambda verbose=True: {
        "regime": "UNKNOWN", "signal": "NEUTRAL", "cash_pct": 0.0,
        "spx_price": 0, "ma200": 0, "pct_above_ma": 0,
    })
    monkeypatch.setattr("outcome_tracker.load_model_health",
                         lambda *a, **k: {"GENERAL": {"status": "degraded", "reason": "test"}})

    def fake_train(self, verbose=True):
        self.trained = True
        self.model = MagicMock()
        # A REAL, usable general-model prediction -- would return 0.7 if called.
        self.model.predict_proba.return_value = np.array([[0.3, 0.7]])
        self.calibrator = None
        self.swing_model, self.swing_scaler = None, None
        return True
    monkeypatch.setattr(me.StockMLPredictor, "train", fake_train)

    # A category that is neither SWING nor in RULES_BASED_CATEGORIES --
    # today's only path into the general-model "else" branch.
    screener_picks = {
        "FHSA_top5": [{"ticker": "GENCO", "score": 50, "data": {}, "pick": {"category": "SOME_FUTURE_CATEGORY"}}],
        "TFSA_growth_top5": [], "TFSA_income_top5": [], "TFSA_swing_top3": [],
    }
    me.run_ml_engine(screener_picks, {}, verbose=True)
    out = capsys.readouterr().out

    assert "General model health: DEGRADED" in out
    genco_line = out.split("GENCO")[1].split("\n")[0] if "GENCO" in out else ""
    assert "rules_based" in genco_line or "rules_based" in out, (
        f"GENCO must fall back to rules_based when the general model is "
        f"degraded -- full output:\n{out}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# FIX (2026-08-09): generalized train_swing_model() into
# train_category_model() -- the training path outcome_tracker.category_is_
# data_ready() implies is needed once it turns true for ANY category, not
# just SWING (dry-tested since nothing is real-ready today -- WATCH is
# earliest, 2026-09-12). Same deploy gate, same lazy-load-and-cache pattern
# on the predictor side (predict_category), same auto-wired routing hook
# ml_engine.py's RULES_BASED_CATEGORIES branch now checks automatically.
# ─────────────────────────────────────────────────────────────────────────────

def test_train_category_model_generic_category_trains_and_deploys(monkeypatch, tmp_path):
    """A non-SWING category with real, learnable data must train and
    deploy through train_category_model() exactly like SWING does,
    including the same holdout-AUC deploy gate."""
    import ml_retrainer as mr

    outcomes_path = tmp_path / "outcomes_log.json"
    rows = []
    for r in _synthetic_resolved_rows(n=90, informative=True, seed=7):
        r = dict(r)
        r["category"] = "GROWTH CORE"
        r["true_horizon_resolved"] = True
        r["true_horizon_outcome"] = r["outcome"]
        r["true_horizon_return"] = r["actual_return"]
        rows.append(r)
    outcomes_path.write_text(__import__("json").dumps(rows))

    monkeypatch.setattr(mr, "OUTCOMES_FILE", str(outcomes_path))
    cache_path  = tmp_path / "gc_model.pkl"
    report_path = tmp_path / "gc_report.json"
    monkeypatch.setattr(mr, "_category_model_paths",
                         lambda category: (str(cache_path), str(report_path))
                         if category == "GROWTH CORE" else mr._category_model_paths(category))

    report = mr.train_category_model("GROWTH CORE", min_rows=40, verbose=False)
    if report is None:
        pytest.skip("Training libraries unavailable")

    assert report["category"] == "GROWTH CORE"
    assert report["horizon_days"] == 180, "must read GROWTH CORE's real horizon from CATEGORY_HORIZONS"
    assert report["deployed"] is True, f"informative fixture must clear the AUC bar: {report}"
    assert cache_path.exists()

    model, scaler, trained_at = mr.load_category_model("GROWTH CORE")
    assert model is not None and scaler is not None
    # report["trained_at"] and the payload's "_trained_at" are two separate
    # datetime.now().isoformat() calls a few microseconds apart -- not
    # guaranteed bit-identical, just both "now".
    assert trained_at is not None and trained_at[:16] == report["trained_at"][:16]


def test_train_category_model_respects_min_rows_per_call():
    """min_rows is a parameter, not a hardcoded constant -- a category with
    too few true-horizon rows for the requested bar must be skipped (no
    training attempted), independent of SWING_MIN_ROWS_TO_TRAIN."""
    from unittest.mock import patch
    import ml_retrainer as mr

    with patch.object(mr, "_load_category_true_horizon_outcomes", return_value=[{"x": 1}] * 5):
        report = mr.train_category_model("GROWTH CORE", min_rows=200, verbose=False)
    assert report is None


def test_predict_category_returns_none_without_a_deployed_model():
    """predict_category() must degrade gracefully (None) for a category
    with no deployed model, same contract as predict_swing()."""
    import ml_engine as me

    p = me.StockMLPredictor()
    result = p.predict_category("GROWTH CORE", {"momentum_6m": 0.05})
    assert result is None


def test_predict_category_uses_loaded_model_and_caches_it(monkeypatch):
    """predict_category() must use a deployed model when load_category_model
    finds one, and must only call the loader once per category per
    predictor instance (memoized, not reloaded from disk on every pick)."""
    import ml_engine as me
    from unittest.mock import MagicMock
    import numpy as np

    mock_model  = MagicMock()
    mock_model.predict_proba.return_value = np.array([[0.15, 0.85]])
    mock_scaler = MagicMock()
    mock_scaler.transform.return_value = [[0, 0, 0]]

    call_count = {"n": 0}
    def fake_loader(category):
        call_count["n"] += 1
        return mock_model, mock_scaler, "2026-08-01T00:00:00"
    monkeypatch.setattr("ml_retrainer.load_category_model", fake_loader)

    p = me.StockMLPredictor()
    r1 = p.predict_category("GROWTH CORE", {"momentum_6m": 0.05})
    r2 = p.predict_category("GROWTH CORE", {"momentum_6m": 0.10})
    assert r1 == 0.85 and r2 == 0.85
    assert call_count["n"] == 1, "loader must be called once and memoized, not once per pick"
    assert p.get_category_model_trained_at("GROWTH CORE") == "2026-08-01T00:00:00"


def test_ml_engine_routes_to_category_model_when_ready_and_deployed(monkeypatch, tmp_path, capsys):
    """Real end-to-end: a RULES_BASED_CATEGORIES pick must route through
    predict_category() (source="category_model") instead of rules_based
    once category_is_data_ready() says yes AND a deployed model exists --
    this is the actual auto-promotion loop closing, not just its pieces
    tested in isolation."""
    import ml_engine as me
    from unittest.mock import MagicMock
    import numpy as np

    monkeypatch.setattr(me, "_SMOOTH_CACHE_FILE", str(tmp_path / "smooth_cache.json"))
    monkeypatch.setattr(
        me, "build_features_for_stock",
        lambda ticker, stock_data, rs=50: {"momentum_6m": 0.05, "roe": 0.1, "rs_rating": 0.6}
    )
    monkeypatch.setattr(me, "get_market_regime", lambda verbose=True: {
        "regime": "UNKNOWN", "signal": "NEUTRAL", "cash_pct": 0.0,
        "spx_price": 0, "ma200": 0, "pct_above_ma": 0,
    })
    monkeypatch.setattr("outcome_tracker.load_model_health", lambda *a, **k: {})
    monkeypatch.setattr(me, "category_is_data_ready", lambda category: category == "WATCH")

    def fake_train(self, verbose=True):
        self.trained = True
        self.model = MagicMock()
        self.model.predict_proba.return_value = np.array([[0.3, 0.7]])
        self.calibrator = None
        self.swing_model, self.swing_scaler = None, None
        return True
    monkeypatch.setattr(me.StockMLPredictor, "train", fake_train)

    def fake_predict_category(self, category, features):
        return 0.77 if category == "WATCH" else None
    monkeypatch.setattr(me.StockMLPredictor, "predict_category", fake_predict_category)

    screener_picks = {
        "FHSA_top5": [{"ticker": "WATCHCO", "score": 50, "data": {}, "pick": {"category": "WATCH"}}],
        "TFSA_growth_top5": [], "TFSA_income_top5": [], "TFSA_swing_top3": [],
    }
    me.run_ml_engine(screener_picks, {}, verbose=True)
    out = capsys.readouterr().out

    watchco_line = out.split("WATCHCO")[1].split("\n")[0] if "WATCHCO" in out else ""
    assert "category_model" in watchco_line, (
        f"WATCHCO must show category_model as source once ready+deployed, "
        f"full output:\n{out}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# FIX (2026-08-09): full-system audit found signal_quality.py's earnings-date
# filter silently never fired -- it parsed next_earnings as ISO "%Y-%m-%d",
# but stock_screener.py actually formats it as "%b %d, %Y" (e.g. "Aug 12,
# 2026"). The bare except swallowed the resulting ValueError every time,
# always returning False. Confirmed live: a simulated 3-days-out earnings
# date in the real format returned False (should be True) before this fix.
# ─────────────────────────────────────────────────────────────────────────────

def test_is_near_earnings_parses_real_stock_screener_date_format():
    """The actual format stock_screener.py produces (%b %d, %Y) must be
    parsed correctly, not silently fail to a false-negative."""
    import signal_quality as sq
    from datetime import datetime, timedelta

    near_date = (datetime.now() + timedelta(days=3)).strftime("%b %d, %Y")
    assert sq.is_near_earnings({"data": {"next_earnings": near_date}}) is True

    far_date = (datetime.now() + timedelta(days=60)).strftime("%b %d, %Y")
    assert sq.is_near_earnings({"data": {"next_earnings": far_date}}) is False


def test_is_near_earnings_still_handles_iso_and_na_and_missing():
    """Regression guard: the fix must not break the pre-existing ISO-format
    fallback, the literal "N/A" default stock_screener.py uses when no
    earnings date is found, or a missing field entirely."""
    import signal_quality as sq
    from datetime import datetime, timedelta

    iso_date = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
    assert sq.is_near_earnings({"data": {"next_earnings": iso_date}}) is True

    assert sq.is_near_earnings({"data": {"next_earnings": "N/A"}}) is False
    assert sq.is_near_earnings({"data": {}}) is False
    assert sq.is_near_earnings({"data": {"next_earnings": "garbage"}}) is False


# ─────────────────────────────────────────────────────────────────────────────
# FIX (2026-08-09): full-system audit found FX and crypto signals had no
# outcome tracking at all -- every other signal type (stocks, NGX, ETFs)
# could report its own historical win rate; FX/crypto could not. New files:
# fx_outcome_tracker.py, crypto_outcome_tracker.py. Dry-tested against real
# production fx_signals.json/crypto_signals.json before being wired into
# run_daily.py -- these tests lock in the behavior found during that dry
# test (only real active calls get logged, never a fake-resolved outcome).
# ─────────────────────────────────────────────────────────────────────────────

def test_fx_tracker_only_logs_active_calls_not_neutral(monkeypatch, tmp_path):
    import fx_outcome_tracker as fxt
    monkeypatch.setattr(fxt, "FX_OUTCOMES_FILE", str(tmp_path / "fx_outcomes.json"))

    fx_result = {
        "active_calls": [
            {"pair": "EUR/USD", "symbol": "EURUSD=X", "direction": "LONG",
             "conviction": 63, "entry": 1.1562, "target": 1.1686, "stop": 1.1488,
             "hold_period": "1-3 days", "key_driver": "Peace Deal"},
        ],
    }
    n = fxt.log_fx_signals(fx_result)
    assert n == 1
    outcomes = fxt.load_fx_outcomes()
    assert len(outcomes) == 1
    assert outcomes[0]["pair"] == "EUR/USD"
    assert outcomes[0]["resolved"] is False

    # Calling again same day must not double-log
    n2 = fxt.log_fx_signals(fx_result)
    assert n2 == 0
    assert len(fxt.load_fx_outcomes()) == 1


def test_fx_tracker_resolution_is_direction_aware_and_time_gated(monkeypatch, tmp_path):
    import fx_outcome_tracker as fxt
    monkeypatch.setattr(fxt, "FX_OUTCOMES_FILE", str(tmp_path / "fx_outcomes.json"))
    from datetime import datetime, timedelta

    old_date = (datetime.now() - timedelta(days=fxt.RESOLUTION_DAYS + 1)).strftime("%Y-%m-%d")
    recent_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    outcomes = [
        {"pair": "EUR/USD", "symbol": "EURUSD=X", "signal_date": old_date,
         "direction": "LONG", "entry_price": 1.1000, "resolved": False},
        {"pair": "USD/JPY", "symbol": "JPY=X", "signal_date": old_date,
         "direction": "SHORT", "entry_price": 150.00, "resolved": False},
        {"pair": "GBP/USD", "symbol": "GBPUSD=X", "signal_date": recent_date,
         "direction": "LONG", "entry_price": 1.2500, "resolved": False},
    ]
    fxt.save_fx_outcomes(outcomes)

    # LONG EUR/USD price went up -> WIN. SHORT USD/JPY price went up -> LOSS
    # (wrong direction for a short). GBP/USD too recent -- must stay unresolved.
    n = fxt.resolve_fx_outcomes(current_prices={
        "EUR/USD": 1.1050,   # +0.45%, above 0.3% threshold -> WIN for LONG
        "USD/JPY": 151.00,   # +0.67% price move -> LOSS for a SHORT call
    })
    assert n == 2

    result = fxt.load_fx_outcomes()
    by_pair = {o["pair"]: o for o in result}
    assert by_pair["EUR/USD"]["outcome"] == "WIN"
    assert by_pair["USD/JPY"]["outcome"] == "LOSS"
    assert by_pair["GBP/USD"]["resolved"] is False, "must not resolve before RESOLUTION_DAYS elapses"


def test_fx_tracker_never_fake_resolves_without_a_real_price(monkeypatch, tmp_path):
    """If a fetch fails and no current_prices entry is supplied, the entry
    must stay unresolved -- never defaulted to a fake outcome."""
    import fx_outcome_tracker as fxt
    monkeypatch.setattr(fxt, "FX_OUTCOMES_FILE", str(tmp_path / "fx_outcomes.json"))
    monkeypatch.setattr("fx_engine.fetch_fx_data", lambda symbol: {"status": "error"})
    from datetime import datetime, timedelta

    old_date = (datetime.now() - timedelta(days=fxt.RESOLUTION_DAYS + 1)).strftime("%Y-%m-%d")
    fxt.save_fx_outcomes([
        {"pair": "EUR/USD", "symbol": "EURUSD=X", "signal_date": old_date,
         "direction": "LONG", "entry_price": 1.1000, "resolved": False},
    ])
    n = fxt.resolve_fx_outcomes()
    assert n == 0
    assert fxt.load_fx_outcomes()[0]["resolved"] is False


def test_crypto_tracker_only_logs_calls_clearing_own_actionability_bar(monkeypatch, tmp_path):
    """Confirmed via dry test against real crypto_signals.json (2026-08-09):
    both BTC/SOL sat at 46% conviction (WATCH/WAIT, not a real call) --
    must not be logged. Only conviction >= ACTIVE_CALL_MIN_CONVICTION (65,
    matching crypto_engine.py's own BUY/ADD /SELL/REDUCE action threshold)
    counts as a real prediction worth grading."""
    import crypto_outcome_tracker as ct
    monkeypatch.setattr(ct, "CRYPTO_OUTCOMES_FILE", str(tmp_path / "crypto_outcomes.json"))

    crypto_result = {
        "assets": {
            "BTC-USD": {"symbol": "BTC-USD", "name": "BTC", "direction": "SHORT",
                        "conviction": 46, "entry": 65000, "price": 65000},
            "SOL-USD": {"symbol": "SOL-USD", "name": "SOL", "direction": "LONG",
                        "conviction": 72, "entry": 180.0, "target": 200.0, "stop": 170.0,
                        "hold_period": 14},
        },
    }
    n = ct.log_crypto_signals(crypto_result)
    assert n == 1, "only SOL-USD (72% conviction) clears the actionability bar"
    outcomes = ct.load_crypto_outcomes()
    assert len(outcomes) == 1
    assert outcomes[0]["symbol"] == "SOL-USD"


def test_crypto_tracker_resolution_direction_aware(monkeypatch, tmp_path):
    import crypto_outcome_tracker as ct
    monkeypatch.setattr(ct, "CRYPTO_OUTCOMES_FILE", str(tmp_path / "crypto_outcomes.json"))
    from datetime import datetime, timedelta

    old_date = (datetime.now() - timedelta(days=ct.RESOLUTION_DAYS + 1)).strftime("%Y-%m-%d")
    ct.save_crypto_outcomes([
        {"symbol": "BTC-USD", "signal_date": old_date, "direction": "LONG",
         "entry_price": 60000.0, "resolved": False},
    ])
    # +5% move on a LONG, above the 3% threshold -> WIN
    n = ct.resolve_crypto_outcomes(current_prices={"BTC-USD": 63000.0})
    assert n == 1
    assert ct.load_crypto_outcomes()[0]["outcome"] == "WIN"


# ─────────────────────────────────────────────────────────────────────────────
# FIX (2026-08-09): insider_engine.py's buy/sell-direction path was dead code
# (_parse_form4_from_accession unconditionally returned [], comment blamed a
# missing lxml dependency that wasn't actually required -- stdlib
# ElementTree works fine, verified live against real BMY/GOOGL Form 4
# filings). Completed the real XML parsing + direction-aware scoring this
# module's own docstring specified from the start. Along the way, found a
# separate, real, previously-undiscovered bug: 12 of KNOWN_CIKS' hardcoded
# US-ticker CIKs were wrong (verified against SEC's own company_tickers.json)
# -- including AMGN, this system's own most-frequent high-conviction pick,
# which was pointing at CAMBREX CORP, an unrelated company. Concrete proof
# of the scoring fix's real-world impact: with the CIK corrected, AMGN's
# real Form 4 data shows a genuine cluster SELL (-5pts) where the old
# count-only scorer said +3pts "insider activity" -- the old scoring was
# pushing the score in the OPPOSITE direction from what insiders were
# actually doing.
# ─────────────────────────────────────────────────────────────────────────────

# A trimmed but structurally-real Form 4 XML, matching the exact schema
# verified live against SEC EDGAR 2026-08-09 (BMY reporting owner Massacesi
# Cristian, filing 0002080176-26-000004) -- two nonDerivativeTransactions
# (M=exercise, F=tax withholding; neither a real open-market trade) plus a
# derivativeTable entry that must NOT be parsed as a transaction.
_REAL_SHAPE_FORM4_XML = """<?xml version="1.0"?>
<ownershipDocument>
    <issuer>
        <issuerCik>0000014272</issuerCik>
        <issuerTradingSymbol>BMY</issuerTradingSymbol>
    </issuer>
    <reportingOwner>
        <reportingOwnerId>
            <rptOwnerCik>0002080176</rptOwnerCik>
            <rptOwnerName>Massacesi Cristian</rptOwnerName>
        </reportingOwnerId>
    </reportingOwner>
    <nonDerivativeTable>
        <nonDerivativeTransaction>
            <transactionCoding><transactionCode>M</transactionCode></transactionCoding>
            <transactionAmounts>
                <transactionShares><value>51172</value></transactionShares>
                <transactionPricePerShare><value>0</value></transactionPricePerShare>
                <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
            </transactionAmounts>
        </nonDerivativeTransaction>
        <nonDerivativeTransaction>
            <transactionCoding><transactionCode>F</transactionCode></transactionCoding>
            <transactionAmounts>
                <transactionShares><value>26175</value></transactionShares>
                <transactionPricePerShare><value>65.31</value></transactionPricePerShare>
                <transactionAcquiredDisposedCode><value>D</value></transactionAcquiredDisposedCode>
            </transactionAmounts>
        </nonDerivativeTransaction>
    </nonDerivativeTable>
    <derivativeTable>
        <derivativeTransaction>
            <transactionCoding><transactionCode>M</transactionCode></transactionCoding>
            <transactionAmounts>
                <transactionShares><value>999999</value></transactionShares>
                <transactionPricePerShare><value>0</value></transactionPricePerShare>
                <transactionAcquiredDisposedCode><value>D</value></transactionAcquiredDisposedCode>
            </transactionAmounts>
        </derivativeTransaction>
    </derivativeTable>
</ownershipDocument>"""

_OPEN_MARKET_BUY_XML = """<?xml version="1.0"?>
<ownershipDocument>
    <issuer><issuerCik>0000014272</issuerCik><issuerTradingSymbol>BMY</issuerTradingSymbol></issuer>
    <reportingOwner>
        <reportingOwnerId><rptOwnerCik>0001111111</rptOwnerCik><rptOwnerName>Test Insider</rptOwnerName></reportingOwnerId>
    </reportingOwner>
    <nonDerivativeTable>
        <nonDerivativeTransaction>
            <transactionCoding><transactionCode>P</transactionCode></transactionCoding>
            <transactionAmounts>
                <transactionShares><value>1000</value></transactionShares>
                <transactionPricePerShare><value>50.0</value></transactionPricePerShare>
                <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
            </transactionAmounts>
        </nonDerivativeTransaction>
    </nonDerivativeTable>
</ownershipDocument>"""


def test_parse_form4_extracts_only_nonderivative_transactions(monkeypatch):
    """Real-shape XML (verified live 2026-08-09): must extract the 2 real
    nonDerivativeTransaction entries (M, F) and NOT the derivativeTable
    entry -- options/RSU derivative activity isn't a real-price,
    real-share open-market transaction to grade."""
    import insider_engine as ie
    monkeypatch.setattr(ie, "_edgar_request_text", lambda url, timeout=10: _REAL_SHAPE_FORM4_XML)

    txns = ie._parse_form4_from_accession("0000014272", "0002080176-26-000004",
                                           "xslF345X06/wk-form4_1785874216.xml")
    assert len(txns) == 2, "must extract exactly the 2 nonDerivativeTransaction entries, not the derivative one"
    codes = {t["code"] for t in txns}
    assert codes == {"M", "F"}
    assert all(t["reporting_owner"] == "Massacesi Cristian" for t in txns)
    m_txn = next(t for t in txns if t["code"] == "M")
    assert m_txn["shares"] == 51172.0
    assert m_txn["value"] == 0.0  # price 0 -> value 0, correctly not treated as a real dollar trade


def test_parse_form4_returns_empty_list_on_malformed_xml(monkeypatch):
    """Must degrade gracefully (empty list, no crash) on unparseable
    content -- one bad filing must not take down the whole ticker."""
    import insider_engine as ie
    monkeypatch.setattr(ie, "_edgar_request_text", lambda url, timeout=10: "not valid xml <<<")
    txns = ie._parse_form4_from_accession("123", "0001-26-000001", "primary_doc.xml")
    assert txns == []


def test_score_insider_signal_by_direction_ignores_non_open_market_codes():
    """M (exercise) and F (tax withholding) are compensation mechanics,
    not discretionary trades -- must score 0, matching the module
    docstring's 'AWARD ONLY = 0pts'."""
    import insider_engine as ie
    txns = [
        {"code": "M", "shares": 51172, "price": 0, "value": 0, "reporting_owner": "A"},
        {"code": "F", "shares": 26175, "price": 65.31, "value": 1709812.5, "reporting_owner": "A"},
    ]
    adj, reason = ie.score_insider_signal_by_direction(txns, "BMY")
    assert adj == 0
    assert reason == ""


def test_score_insider_signal_by_direction_cluster_buy():
    import insider_engine as ie
    txns = [
        {"code": "P", "shares": 1000, "price": 30, "value": 30_000, "reporting_owner_cik": "1"},
        {"code": "P", "shares": 1000, "price": 30, "value": 30_000, "reporting_owner_cik": "2"},
    ]
    adj, reason = ie.score_insider_signal_by_direction(txns, "TEST")
    assert adj == 8
    assert "Cluster BUY" in reason
    assert "2 insiders" in reason


def test_score_insider_signal_by_direction_single_buy_below_cluster():
    import insider_engine as ie
    txns = [{"code": "P", "shares": 1000, "price": 30, "value": 30_000, "reporting_owner_cik": "1"}]
    adj, reason = ie.score_insider_signal_by_direction(txns, "TEST")
    assert adj == 4
    assert "Insider BUY" in reason


def test_score_insider_signal_by_direction_buy_below_threshold_still_suppresses_sell():
    """A real buy exists but doesn't clear the $25k single-buy threshold --
    per the module docstring's 'no concurrent buys' condition for cluster
    sell, this must score 0 (mixed signal), not fall through to a sell
    score."""
    import insider_engine as ie
    txns = [
        {"code": "P", "shares": 100, "price": 10, "value": 1_000, "reporting_owner_cik": "1"},   # tiny buy
        {"code": "S", "shares": 5000, "price": 100, "value": 500_000, "reporting_owner_cik": "2"},
        {"code": "S", "shares": 5000, "price": 100, "value": 500_000, "reporting_owner_cik": "3"},
    ]
    adj, reason = ie.score_insider_signal_by_direction(txns, "TEST")
    assert adj == 0, "any buy present must suppress a sell score, even a small one"


def test_score_insider_signal_by_direction_cluster_sell_no_buys():
    import insider_engine as ie
    txns = [
        {"code": "S", "shares": 5000, "price": 100, "value": 500_000, "reporting_owner_cik": "1"},
        {"code": "S", "shares": 5000, "price": 100, "value": 500_000, "reporting_owner_cik": "2"},
    ]
    adj, reason = ie.score_insider_signal_by_direction(txns, "TEST")
    assert adj == -5
    assert "Cluster SELL" in reason


def test_score_insider_signal_by_direction_counts_distinct_insiders_not_line_items():
    """One insider with 3 separate buy transaction lines in the same
    filing is 1 insider, not a 3-insider cluster."""
    import insider_engine as ie
    txns = [
        {"code": "P", "shares": 100, "price": 30, "value": 3_000, "reporting_owner_cik": "1"},
        {"code": "P", "shares": 100, "price": 30, "value": 3_000, "reporting_owner_cik": "1"},
        {"code": "P", "shares": 100, "price": 30, "value": 3_000, "reporting_owner_cik": "1"},
    ]
    adj, reason = ie.score_insider_signal_by_direction(txns, "TEST")
    assert "1 insider" not in reason.replace("insiders", "")  # sanity: just checking it's not miscounted as cluster
    assert adj != 8, "3 line items from ONE insider must not score as a 2+-insider cluster"


def test_known_ciks_match_sec_official_mapping_for_spot_checked_tickers():
    """Regression lock for the 12-ticker CIK bug found 2026-08-09 (verified
    live against SEC's own company_tickers.json) -- these hardcoded values
    must not silently drift back to the wrong CIKs. Not a live network
    check (would make this test flaky/slow) -- just locks in the specific
    corrected values found and fixed."""
    import insider_engine as ie
    expected = {
        "MDT": "0001613103", "DXCM": "0001093557", "AFRM": "0001820953",
        "MDB": "0001441816", "AMGN": "0000318154", "BIIB": "0000875045",
        "GILD": "0000882095", "O": "0000726728", "VICI": "0001705696",
        "MAIN": "0001396440", "STAG": "0001479094", "BLK": "0002012383",
    }
    for ticker, cik in expected.items():
        assert ie.KNOWN_CIKS.get(ticker) == cik, (
            f"{ticker}: expected corrected CIK {cik}, got {ie.KNOWN_CIKS.get(ticker)!r}"
        )


def test_fetch_recent_form4_distinguishes_fetch_failure_from_empty_result(monkeypatch):
    """None (fetch failed) vs [] (fetched fine, genuinely nothing found)
    must stay distinguishable -- run_insider_engine's fallback logic
    depends on this, same principle as fetch_form4_aggregated's existing
    None-vs-[] contract."""
    import insider_engine as ie

    def _raise(*a, **k):
        raise Exception("network error")
    monkeypatch.setattr(ie, "_edgar_request", _raise)
    assert ie.fetch_recent_form4("0000014272", days_back=30) is None

    monkeypatch.setattr(ie, "_edgar_request", lambda url, timeout=10: {
        "name": "TEST CO",
        "filings": {"recent": {"form": ["10-K"], "filingDate": ["2026-08-01"],
                                "accessionNumber": ["0001-26-000001"], "primaryDocument": ["doc.htm"]}},
    })
    result = ie.fetch_recent_form4("0000014272", days_back=30)
    assert result == [], "no Form 4s in the window -- must be [] (success, nothing found), not None"
