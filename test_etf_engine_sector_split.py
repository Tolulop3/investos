"""Guards for the 2026-08-29 SECTOR -> SECTOR_COMMODITY / SECTOR_EARNINGS split
in etf_engine.py. The split must stay score- and pick-neutral: it only exists
to give the two halves separate resolution thresholds (etf_outcome_tracker.py).

Run: python3 test_etf_engine_sector_split.py
"""

import sys
import types
import random


def _stub_yfinance():
    """etf_engine imports yfinance at module load; stub it with deterministic
    synthetic price history so run_etf_engine() can execute offline."""
    yf = types.ModuleType("yfinance")

    class _Hist:
        def __init__(self, closes): self._c = closes
        @property
        def empty(self): return False
        def __len__(self): return len(self._c)
        def __getitem__(self, key):
            c = self._c
            class _S:
                def dropna(s): return s
                def tolist(s): return c
            return _S()

    class _Ticker:
        def __init__(self, tk): self.tk = tk
        def history(self, **kw):
            random.seed(abs(hash(self.tk)) % 9999)
            base = 50 + random.random() * 50
            return _Hist([base * (1 + 0.0007 * i + random.uniform(-.015, .015))
                          for i in range(260)])
        @property
        def info(self): return {}

    yf.Ticker = _Ticker
    sys.modules["yfinance"] = yf


_stub_yfinance()
import etf_engine as ee


COMMODITY = {"XEG.TO", "XLE", "GLD", "ZGD.TO"}
EARNINGS  = {"ZEB.TO", "XRE.TO"}


def test_universe_has_split_categories_and_no_bare_sector():
    cats = {row[2] for row in ee.ETF_UNIVERSE}
    assert "SECTOR" not in cats, "stale bare SECTOR category still in ETF_UNIVERSE"
    assert "SECTOR_COMMODITY" in cats and "SECTOR_EARNINGS" in cats
    by_ticker = {row[0]: row[2] for row in ee.ETF_UNIVERSE}
    for tk in COMMODITY:
        assert by_ticker[tk] == "SECTOR_COMMODITY", tk
    for tk in EARNINGS:
        assert by_ticker[tk] == "SECTOR_EARNINGS", tk


def test_regime_weights_cover_both_and_match_old_sector():
    # Old single SECTOR row weights: RISK_ON 1.0 / NEUTRAL 0.7 / RISK_OFF 0.4.
    expected = {"RISK_ON": 1.0, "NEUTRAL": 0.7, "RISK_OFF": 0.4}
    for regime, w in expected.items():
        rw = ee.REGIME_WEIGHTS[regime]
        assert rw["SECTOR_COMMODITY"] == w, (regime, rw.get("SECTOR_COMMODITY"))
        assert rw["SECTOR_EARNINGS"] == w, (regime, rw.get("SECTOR_EARNINGS"))
        # neutrality depends on the two halves carrying the SAME weight
        assert rw["SECTOR_COMMODITY"] == rw["SECTOR_EARNINGS"]


def test_zlb_still_defensive():
    rows = [r for r in ee.ETF_UNIVERSE if r[0] == "ZLB.TO"]
    assert len(rows) == 1 and rows[0][2] == "DEFENSIVE"


def test_run_engine_emits_split_categories_and_keeps_commodity_cap():
    res = ee.run_etf_engine(sector_sentiment={}, unified_regime="RISK_ON",
                            breadth={"signal": "BROAD_BULL"}, verbose=False)
    by_ticker = {s["ticker"]: s for s in res["scored"]}
    present = {s["category"] for s in res["scored"]}
    assert "SECTOR" not in present
    assert {"SECTOR_COMMODITY", "SECTOR_EARNINGS"} <= present
    for tk in COMMODITY:
        assert by_ticker[tk]["score"] <= 88, f"{tk} broke the 88 commodity cap"


if __name__ == "__main__":
    tests = [
        test_universe_has_split_categories_and_no_bare_sector,
        test_regime_weights_cover_both_and_match_old_sector,
        test_zlb_still_defensive,
        test_run_engine_emits_split_categories_and_keeps_commodity_cap,
    ]
    passed = failed = 0
    for tc in tests:
        try:
            tc()
            print(f"  PASS  {tc.__name__}")
            passed += 1
        except AssertionError as e:
            print(f"  FAIL  {tc.__name__}: {e}")
            failed += 1
        except Exception as e:
            import traceback
            print(f"  ERROR {tc.__name__}: {e}")
            traceback.print_exc()
            failed += 1
    print(f"\n{passed}/{passed+failed} passed")
    if failed:
        raise SystemExit(1)
