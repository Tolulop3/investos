"""
Tests for the multi-account sizing refactor.

Covers:
  1. load_accounts() — present and absent cases
  2. compute_target_weights() — returns correct fields, no dollar_amt
  3. render_allocations() — dollar_amt == deployable * weight (to the cent)
  4. VERIFY: weights * 10,000 == dollar_amt from calculate_position_sizes($10k)
  5. Universe filter: US_ONLY strips .TO tickers; CA_CONSERVATIVE strips US picks
  6. Min-position floor: small accounts drop sub-floor positions and re-normalize
  7. accounts.json absent: render_allocations with legacy account matches to the cent

Run: python3 test_account_sizing.py
"""

import json
import os
import sys

# ─── Minimal stubs so ml_engine loads without optional deps ───────────────────
import importlib, types

def _stub(name):
    m = types.ModuleType(name)
    sys.modules[name] = m
    return m

for _dep in ["joblib", "xgboost", "sklearn", "sklearn.preprocessing",
             "sklearn.metrics", "numpy", "pandas"]:
    if _dep not in sys.modules:
        _stub(_dep)

# Make numpy and pandas just barely functional for the module-level guards
sys.modules["numpy"].ndarray = object
sys.modules["pandas"].DataFrame = object
sys.modules["pandas"].Series    = object
sys.modules["xgboost"].XGBClassifier = object
sys.modules["sklearn.preprocessing"].StandardScaler = object
sys.modules["sklearn.metrics"].roc_auc_score = lambda *a, **kw: 0.5

# Now import
sys.path.insert(0, os.path.dirname(__file__))
from ml_engine import (
    compute_target_weights, render_allocations,
    calculate_position_sizes, load_accounts,
    _UNIVERSE_FILTERS, _FHSA_AVOID,
)

# ─── Fixtures ─────────────────────────────────────────────────────────────────

def _pick(ticker, score=70, ml_prob=0.55, vol=0.20, price=50.0):
    return {
        "ticker": ticker,
        "score":  score,
        "ml_prob": ml_prob,
        "data":   {"volatility_90d": vol, "price": price, "sector": "Financials"},
        "news_adjustment": 0,
    }

PICKS = [
    _pick("AAPL",  score=72, ml_prob=0.61, vol=0.18, price=185.0),
    _pick("RY.TO", score=68, ml_prob=0.55, vol=0.20, price=130.0),
    _pick("TD.TO", score=65, ml_prob=0.52, vol=0.22, price=75.0),
    _pick("MSFT",  score=75, ml_prob=0.63, vol=0.16, price=410.0),
    _pick("ENB.TO",score=62, ml_prob=0.50, vol=0.25, price=55.0),
]

REGIME_BULL = {"regime": "BULL", "cash_pct": 0.0,
               "spx_price": 5500, "ma200": 5100, "pct_above_ma": 7.8,
               "full_exposure_pct": 100, "signal": "FULL_EXPOSURE"}

REGIME_CAUTION = {"regime": "CAUTION", "cash_pct": 0.30,
                  "spx_price": 5000, "ma200": 5100, "pct_above_ma": -2.0,
                  "full_exposure_pct": 70, "signal": "REDUCE_EXPOSURE"}

ML_CONFIG_STUB = {
    "drawdown_reduction_trigger": 0.10,
    "drawdown_reduction_amount":  0.30,
    "max_positions": 8,
}

# Patch ML_CONFIG so get_cooldown_set and compute_target_weights work without files
import ml_engine as _mle
_orig_cfg = _mle.ML_CONFIG
_mle.ML_CONFIG = ML_CONFIG_STUB

_orig_cooldown = _mle.get_cooldown_set
_mle.get_cooldown_set = lambda **kw: (set(), {})


# ─── Tests ────────────────────────────────────────────────────────────────────

def test_load_accounts_present():
    """load_accounts() reads accounts.json when present."""
    test_path = "/tmp/_test_accounts.json"
    payload = [{"name": "TFSA", "capital": 5000, "universe": "ALL", "max_equity": 1.0}]
    with open(test_path, "w") as f:
        json.dump(payload, f)
    orig_open = open
    import builtins
    _real_open = builtins.open
    def _mock_open(path, *a, **kw):
        if path == "accounts.json":
            return _real_open(test_path, *a, **kw)
        return _real_open(path, *a, **kw)
    builtins.open = _mock_open
    try:
        result = load_accounts()
        assert result == payload, f"expected {payload}, got {result}"
    finally:
        builtins.open = _real_open
        os.unlink(test_path)


def test_load_accounts_absent():
    """load_accounts() returns [] when accounts.json is absent."""
    import builtins
    _real_open = builtins.open
    def _mock_open(path, *a, **kw):
        if path == "accounts.json":
            raise FileNotFoundError
        return _real_open(path, *a, **kw)
    builtins.open = _mock_open
    try:
        result = load_accounts()
        assert result == [], f"expected [], got {result}"
    finally:
        builtins.open = _real_open


def test_compute_target_weights_fields():
    """compute_target_weights returns expected fields with no dollar_amt."""
    weights = compute_target_weights(PICKS, REGIME_BULL, verbose=False)
    assert weights, "expected non-empty weights"
    for w in weights:
        assert "ticker"     in w, "missing ticker"
        assert "weight"     in w, "missing raw weight fraction"
        assert "weight_pct" in w, "missing weight_pct"
        assert "ml_prob"    in w, "missing ml_prob"
        assert "kelly_wt"   in w, "missing kelly_wt"
        assert "vol_adj"    in w, "missing vol_adj"
        assert "score"      in w, "missing score"
        assert "dollar_amt" not in w, "dollar_amt must NOT be in target weights"
    # weight field should be consistent with weight_pct
    for w in weights:
        assert abs(w["weight"] * 100 - w["weight_pct"]) < 0.01, \
            f"weight/weight_pct mismatch for {w['ticker']}"


def test_render_all_universe_matches_legacy():
    """
    VERIFY: render_allocations(universe=ALL, capital=10000) == calculate_position_sizes($10k)
    Dollar amounts must match to the cent.
    """
    weights = compute_target_weights(PICKS, REGIME_BULL, verbose=False)
    if not weights:
        print("  ⚠️  No weights produced — skipping VERIFY check")
        return

    account = {"name": "TFSA", "capital": 10000, "universe": "ALL", "max_equity": 1.0}
    new_sized = render_allocations(weights, account, REGIME_BULL, verbose=False)

    legacy_sized = calculate_position_sizes(
        PICKS, portfolio_value=10000, market_regime=REGIME_BULL, verbose=False
    )

    assert len(new_sized) == len(legacy_sized), \
        f"length mismatch: new={len(new_sized)} legacy={len(legacy_sized)}"

    for n, l in zip(new_sized, legacy_sized):
        assert n["ticker"] == l["ticker"], f"ticker order mismatch: {n['ticker']} vs {l['ticker']}"
        assert n["dollar_amt"] == l["dollar_amt"], \
            (f"{n['ticker']}: dollar_amt mismatch — "
             f"new={n['dollar_amt']:.2f} legacy={l['dollar_amt']:.2f} "
             f"(diff={abs(n['dollar_amt']-l['dollar_amt']):.4f})")


def test_universe_filter_us_only():
    """US_ONLY filter keeps only non-.TO tickers."""
    weights = compute_target_weights(PICKS, REGIME_BULL, verbose=False)
    if not weights:
        return
    account = {"name": "RRSP", "capital": 20000, "universe": "US_ONLY", "max_equity": 0.9}
    allocs  = render_allocations(weights, account, REGIME_BULL, verbose=False)
    us_tickers   = {w["ticker"] for w in weights if not w["ticker"].endswith(".TO")}
    alloc_tickers = {a["ticker"] for a in allocs}
    assert alloc_tickers.issubset(us_tickers), \
        f"CA tickers leaked into RRSP: {alloc_tickers - us_tickers}"


def test_universe_filter_ca_conservative():
    """CA_CONSERVATIVE keeps only .TO tickers (and not FHSA_AVOID)."""
    weights = compute_target_weights(PICKS, REGIME_BULL, verbose=False)
    if not weights:
        return
    account = {"name": "FHSA", "capital": 5000, "universe": "CA_CONSERVATIVE", "max_equity": 0.85}
    allocs  = render_allocations(weights, account, REGIME_BULL, verbose=False)
    for a in allocs:
        assert a["ticker"].endswith(".TO"),   f"non-.TO ticker in FHSA: {a['ticker']}"
        assert a["ticker"] not in _FHSA_AVOID, f"FHSA_AVOID ticker in FHSA: {a['ticker']}"


def test_min_position_floor():
    """Positions below min_floor are dropped and remaining weights re-normalize."""
    weights = compute_target_weights(PICKS, REGIME_BULL, verbose=False)
    if not weights:
        return
    # Use very small capital so most positions fall below $250
    account = {"name": "SMALL", "capital": 500, "universe": "ALL", "max_equity": 1.0}
    allocs  = render_allocations(weights, account, REGIME_BULL,
                                  min_position=250.0, verbose=False)
    for a in allocs:
        assert a["dollar_amt"] >= 250.0 or a["dollar_amt"] == 0.0, \
            f"{a['ticker']}: dollar_amt {a['dollar_amt']:.2f} below floor"
    if allocs:
        total_pct = sum(a["weight_pct"] for a in allocs)
        assert abs(total_pct - 100.0) < 0.1, \
            f"weights don't re-normalize to 100% after floor drop: {total_pct:.2f}%"


def test_zero_capital_returns_empty():
    """render_allocations with capital=0 returns []."""
    weights = compute_target_weights(PICKS, REGIME_BULL, verbose=False)
    account = {"name": "EMPTY", "capital": 0, "universe": "ALL", "max_equity": 1.0}
    allocs  = render_allocations(weights, account, REGIME_BULL, verbose=False)
    assert allocs == [], f"expected [] for zero capital, got {allocs}"


def test_caution_regime_reduces_deployable():
    """CAUTION (30% cash) deploys only 70% of capital."""
    weights = compute_target_weights(PICKS, REGIME_CAUTION, verbose=False)
    if not weights:
        return
    account  = {"name": "TFSA", "capital": 10000, "universe": "ALL", "max_equity": 1.0}
    allocs   = render_allocations(weights, account, REGIME_CAUTION, verbose=False)
    deployed = sum(a["dollar_amt"] for a in allocs)
    # CAUTION: regime_equity_pct = 0.70, so max deployable = 7000
    assert deployed <= 7000 * 1.01, \
        f"deployed ${deployed:.2f} exceeds caution cap of $7,000"


# ─── Runner ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    tests = [
        test_load_accounts_absent,
        test_load_accounts_present,
        test_compute_target_weights_fields,
        test_render_all_universe_matches_legacy,
        test_universe_filter_us_only,
        test_universe_filter_ca_conservative,
        test_min_position_floor,
        test_zero_capital_returns_empty,
        test_caution_regime_reduces_deployable,
    ]
    passed = failed = 0
    for t in tests:
        try:
            t()
            print(f"  PASS  {t.__name__}")
            passed += 1
        except AssertionError as e:
            print(f"  FAIL  {t.__name__}: {e}")
            failed += 1
        except Exception as e:
            import traceback
            print(f"  ERROR {t.__name__}: {e}")
            traceback.print_exc()
            failed += 1
    print(f"\n{passed}/{passed+failed} passed")
    if failed:
        raise SystemExit(1)
    # Restore
    _mle.ML_CONFIG         = _orig_cfg
    _mle.get_cooldown_set  = _orig_cooldown
