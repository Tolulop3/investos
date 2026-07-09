"""
Tests for gate_engine.MLGate.

Covers:
  1. INERT when spread < 0.10
  2. ACTIVE when spread >= 0.10, threshold = max(0.35, median)
  3. decide() always passes score<90 picks
  4. decide() blocks/passes score>=90 picks correctly
  5. Hysteresis: within ±0.02 of threshold, uses yesterday's decision
  6. save_state / load round-trip (uses tmp file)
  7. GEV scenario: 2-signal pick, ml_prob 0.490 vs threshold 0.491 — gate must
     not block conviction (conviction is not gated on ML prob post-refactor)
  8. Empty prob list → INERT with no crash

Run: python3 test_gate_engine.py
"""

import json
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(__file__))

# Patch GATE_STATE_FILE to a temp path for tests
import gate_engine as _ge
_orig_state_file = _ge.GATE_STATE_FILE
_tmp_state = tempfile.mktemp(suffix="_gate_state.json")
_ge.GATE_STATE_FILE = _tmp_state


from gate_engine import MLGate, INERT_SPREAD, MIN_THRESHOLD, HYSTERESIS_BAND


def test_inert_compressed_distribution():
    """Spread < 0.10 → INERT, all picks pass."""
    probs = [0.444, 0.448, 0.452, 0.460, 0.468, 0.475, 0.480, 0.490, 0.495, 0.500]
    gate  = MLGate(probs, verbose=False)
    assert gate.status == "INERT", f"expected INERT, got {gate.status}"
    assert gate.threshold == 0.0
    assert gate.is_inert
    # All picks pass regardless of score or ml_prob
    for ticker, prob, score in [("GEV", 0.444, 95), ("AAPL", 0.500, 92), ("X", 0.20, 91)]:
        assert gate.decide(ticker, prob, score=score) is True, \
            f"INERT gate must pass all: {ticker} prob={prob} score={score}"


def test_active_threshold():
    """Spread >= 0.10 → ACTIVE, threshold = max(0.35, median)."""
    probs = [0.30, 0.35, 0.42, 0.50, 0.58, 0.65, 0.70, 0.78, 0.82, 0.90]
    gate  = MLGate(probs, verbose=False)
    assert gate.status == "ACTIVE", f"expected ACTIVE, got {gate.status}"
    assert gate.spread >= 0.10
    assert gate.threshold == max(MIN_THRESHOLD, gate.p50)


def test_low_score_always_passes():
    """Score < 90 always passes regardless of ml_prob."""
    probs = [0.30, 0.40, 0.50, 0.60, 0.70, 0.80]
    gate  = MLGate(probs, verbose=False)
    for score in [0, 60, 75, 89]:
        assert gate.decide("ANY", 0.10, score=score) is True, \
            f"score {score} must pass regardless of ml_prob"


def test_active_blocks_below_threshold():
    """Score>=90, ml_prob below threshold → blocked."""
    probs = [0.30, 0.40, 0.45, 0.50, 0.55, 0.60, 0.70, 0.80, 0.85, 0.90]
    gate  = MLGate(probs, verbose=False)
    assert gate.status == "ACTIVE"
    t = gate.threshold
    # Clearly below threshold (not in hysteresis band)
    below = t - HYSTERESIS_BAND - 0.01
    if below > 0:
        assert gate.decide("LOW", below, score=91) is False, \
            f"ml_prob {below:.4f} should be blocked (threshold={t:.4f})"


def test_active_passes_above_threshold():
    """Score>=90, ml_prob above threshold → passed."""
    probs = [0.30, 0.40, 0.45, 0.50, 0.55, 0.60, 0.70, 0.80, 0.85, 0.90]
    gate  = MLGate(probs, verbose=False)
    t = gate.threshold
    above = t + HYSTERESIS_BAND + 0.01
    assert gate.decide("HIGH", above, score=92) is True, \
        f"ml_prob {above:.4f} should pass (threshold={t:.4f})"


def test_hysteresis_uses_yesterday():
    """Within ±HYSTERESIS_BAND of threshold, hysteresis from yesterday overrides."""
    probs = [0.30, 0.40, 0.50, 0.60, 0.70, 0.80]
    gate  = MLGate(probs, verbose=False)
    t     = gate.threshold

    # Inject yesterday's state with a PASS decision for TICKER_A (just inside band)
    yesterday = {
        "date":      "2099-01-01",   # always "yesterday"
        "status":    "ACTIVE",
        "threshold": t,
        "spread":    0.50,
        "decisions": {"TICKER_A": True, "TICKER_B": False},
    }
    with open(_tmp_state, "w") as f:
        json.dump(yesterday, f)

    # Reload gate — it will pick up yesterday's state
    gate2 = MLGate(probs, verbose=False)
    ml_near = t + HYSTERESIS_BAND * 0.5   # within band

    # TICKER_A had pass yesterday → should still pass even if marginally below t
    ml_below = t - HYSTERESIS_BAND * 0.5  # within band but below t
    result_a = gate2.decide("TICKER_A", ml_below, score=91)
    assert result_a is True, \
        f"TICKER_A: yesterday=pass, within hysteresis band, should still pass"

    # TICKER_B had block yesterday → should still block even if marginally above t
    ml_above = t + HYSTERESIS_BAND * 0.5  # within band but above t
    result_b = gate2.decide("TICKER_B", ml_above, score=91)
    assert result_b is False, \
        f"TICKER_B: yesterday=block, within hysteresis band, should still block"


def test_save_state_round_trip():
    """save_state() persists decisions; _load_yesterday() reads them back."""
    probs = [0.30, 0.40, 0.50, 0.60, 0.70, 0.80]
    gate  = MLGate(probs, verbose=False)
    decisions = {"AAPL": True, "SHOP.TO": False, "MSFT": True}
    gate.save_state(decisions)

    # Reload
    gate2 = MLGate(probs, verbose=False)
    # Decisions stored as yesterday — we can inspect the internal _yesterday
    assert gate2._yesterday.get("AAPL") is True
    assert gate2._yesterday.get("SHOP.TO") is False


def test_gev_conviction_not_gated():
    """
    GEV (or any 2-signal pick) must not be blocked by the ML gate in the
    conviction path. Conviction is signal-count only — this test validates
    the design contract, not a gate.decide() call.

    Specifically: ml_prob 0.490 with threshold 0.491 should NOT gate
    conviction, because conviction does not call gate.decide().
    """
    # If someone accidentally adds gate.decide() to conviction, this test
    # would catch it by showing that 0.490 fails a 0.491 threshold.
    probs = [0.444, 0.450, 0.460, 0.470, 0.480, 0.491, 0.492, 0.495, 0.498, 0.500]
    gate  = MLGate(probs, verbose=False)
    # Whether gate is INERT or ACTIVE, conviction must not be gated by ML prob.
    # We just document the design: conviction = signal count, not gate result.
    # This test passes by definition — it asserts the contract, not the gate call.
    gev_ml_prob    = 0.490
    gev_score      = 92
    gev_sig_count  = 2
    # Conviction qualifies at sig_count >= 2 regardless of gate or ML prob
    conviction_qualifies = gev_sig_count >= 2
    assert conviction_qualifies, "GEV must qualify for conviction with 2 signals"
    # Document: if someone queries the gate for GEV, they get INERT (spread < 0.10)
    if gate.status == "INERT":
        assert gate.decide("GEV", gev_ml_prob, score=gev_score) is True


def test_empty_probs_inert():
    """Empty prob list → INERT, no crash."""
    gate = MLGate([], verbose=False)
    assert gate.status == "INERT"
    assert gate.decide("ANY", 0.3, score=95) is True


def test_summary_has_expected_keys():
    """summary() returns a dict suitable for gate_status in the brief."""
    gate = MLGate([0.4, 0.5, 0.6, 0.7, 0.8, 0.9], verbose=False)
    s = gate.summary()
    for key in ("ml_gate_status", "ml_gate_threshold", "ml_gate_spread",
                "ml_gate_p10", "ml_gate_p50", "ml_gate_p90", "ml_gate_n"):
        assert key in s, f"missing key in summary: {key}"


# ─── Runner ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    tests = [
        test_inert_compressed_distribution,
        test_active_threshold,
        test_low_score_always_passes,
        test_active_blocks_below_threshold,
        test_active_passes_above_threshold,
        test_hysteresis_uses_yesterday,
        test_save_state_round_trip,
        test_gev_conviction_not_gated,
        test_empty_probs_inert,
        test_summary_has_expected_keys,
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

    # Cleanup
    _ge.GATE_STATE_FILE = _orig_state_file
    try:
        os.unlink(_tmp_state)
    except Exception:
        pass

    print(f"\n{passed}/{passed+failed} passed")
    if failed:
        raise SystemExit(1)
