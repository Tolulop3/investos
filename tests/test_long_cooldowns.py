"""
Long-cooldown chronic-loser judging (ml_engine.get_cooldown_set / _lc_judge).

Follow-ups from the first production week of the auto-nomination fix (cc2f00ab):
  * AEM.TO cleared at exactly 35% WR while averaging -10.2%/pick (PF 0.12) -- a
    win-rate-only rule can't see "wins often enough, loses far bigger".
  * BKNG's entry was written with avg_return NaN (33 outcome rows have a NaN
    true_horizon_return); NaN is invalid JSON for strict parsers.
  * Entries kept stats frozen at whatever a human typed months ago.

Every fixture pick has proxy fields hard-wired to resolved/WIN, so a regression
back to the 7-day-proxy fields would fail all of these.

Run: pytest tests/test_long_cooldowns.py -v
"""

import json
import math
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest


def _pick(ticker, outcome, ret, i):
    return {
        "ticker": ticker, "signal_date": f"2026-06-{1 + i:02d}",
        "resolved": True, "outcome": "WIN",   # proxy deliberately WRONG
        "true_horizon_resolved": True, "true_horizon_outcome": outcome,
        "true_horizon_return": ret, "true_horizon_date": f"2026-06-{1 + i:02d}",
    }


def _picks(ticker, spec):
    """spec: list of (outcome, return) in date order."""
    return [_pick(ticker, o, r, i) for i, (o, r) in enumerate(spec)]


def _strict_loads(text):
    def _boom(c):
        raise ValueError(f"non-JSON constant {c!r}")
    return json.loads(text, parse_constant=_boom)


def _run(monkeypatch, tmp_path, picks, cooldowns):
    import ml_engine as mle
    monkeypatch.chdir(tmp_path)
    (tmp_path / "outcomes_log.json").write_text(json.dumps(picks))
    (tmp_path / "long_cooldowns.json").write_text(json.dumps(cooldowns))
    blocked, tiers = mle.get_cooldown_set(verbose=False)
    saved = _strict_loads((tmp_path / "long_cooldowns.json").read_text())
    return blocked, tiers, saved


EXPIRED = "2020-01-01"
ACTIVE = "2099-01-01"


def _entry(blocked_until, **kw):
    e = {"blocked_until": blocked_until, "reason": "chronic_loser", "n": 3,
         "WR": 0.0, "avg_return": -10.0, "auto_renew": True, "renew_threshold_WR": 0.35}
    e.update(kw)
    return e


AEM_STYLE = [("WIN", 5.0)] * 7 + [("LOSS", -15.0)] * 13   # WR exactly 35%, PF 0.18, avg -8%


def test_judge_flags_low_pf_negative_avg_even_at_threshold_wr():
    import ml_engine as mle
    j = mle._lc_judge(_picks("AEM", AEM_STYLE), 0.35)
    assert j["wr"] == pytest.approx(0.35)
    assert j["is_chronic"] is True and j["why"] == "pf"
    assert j["pf"] < 0.5 and j["avg_return"] < -3.0


def test_nominates_win_rate_borderline_but_large_loss_ticker(monkeypatch, tmp_path):
    blocked, tiers, saved = _run(monkeypatch, tmp_path, _picks("AEMX", AEM_STYLE), {})
    assert "AEMX" in blocked and tiers["AEMX"] == "long_cd"
    assert saved["AEMX"]["reason"] == "chronic_loser_auto"
    assert saved["AEMX"]["PF"] == pytest.approx(0.18, abs=0.01)
    assert saved["AEMX"]["avg_return"] == pytest.approx(-8.0)


@pytest.mark.parametrize("spec", [
    # healthy: 40% WR but winners outweigh losers (PF 1.67)
    [("WIN", 5.0)] * 8 + [("LOSS", -2.0)] * 12,
    # low PF but average loss is mild -- avg floor must keep this from firing
    [("WIN", 0.5)] * 10 + [("LOSS", -2.0)] * 10,
    # WR 45%, PF 0.55, avg -0.4%
    [("WIN", 1.0)] * 9 + [("LOSS", -1.5)] * 11,
])
def test_does_not_nominate_when_pf_leg_would_overfire(monkeypatch, tmp_path, spec):
    blocked, _tiers, saved = _run(monkeypatch, tmp_path, _picks("OKCO", spec), {})
    assert "OKCO" not in blocked and "OKCO" not in saved


def test_nan_return_rows_are_excluded_and_file_stays_strict_json(monkeypatch, tmp_path):
    spec = [("LOSS", -10.0)] * 8 + [("FLAT", float("nan"))] * 2 + [("LOSS", -12.0)] * 2
    blocked, _t, saved = _run(monkeypatch, tmp_path, _picks("NANCO", spec), {})
    assert "NANCO" in blocked
    assert saved["NANCO"]["n"] == 10                       # 12 rows, 2 NaN dropped
    assert math.isfinite(saved["NANCO"]["avg_return"])
    assert "NaN" not in (tmp_path / "long_cooldowns.json").read_text()


def test_nan_rows_can_drop_a_ticker_below_the_minimum_sample(monkeypatch, tmp_path):
    spec = [("LOSS", -10.0)] * 9 + [("FLAT", float("nan"))] * 2    # 11 rows, 9 usable
    blocked, _t, saved = _run(monkeypatch, tmp_path, _picks("THINNAN", spec), {})
    assert "THINNAN" not in blocked and "THINNAN" not in saved


def test_existing_nan_metadata_is_healed(monkeypatch, tmp_path):
    entry = _entry(ACTIVE, avg_return=float("nan"))
    (tmp_path / "long_cooldowns.json").write_text(json.dumps({"BKNGX": entry}))  # writes NaN
    blocked, _t, saved = _run_existing(monkeypatch, tmp_path, [])
    assert "BKNGX" in blocked
    assert saved["BKNGX"]["avg_return"] is None


def _run_existing(monkeypatch, tmp_path, picks):
    """Like _run, but keeps the long_cooldowns.json the test already wrote."""
    import ml_engine as mle
    monkeypatch.chdir(tmp_path)
    (tmp_path / "outcomes_log.json").write_text(json.dumps(picks))
    blocked, tiers = mle.get_cooldown_set(verbose=False)
    saved = _strict_loads((tmp_path / "long_cooldowns.json").read_text())
    return blocked, tiers, saved


def test_renewal_refreshes_stale_stats(monkeypatch, tmp_path):
    picks = _picks("STALE", [("LOSS", -5.0)] * 20)
    blocked, _t, saved = _run(monkeypatch, tmp_path, picks, {"STALE": _entry(EXPIRED)})
    assert "STALE" in blocked
    e = saved["STALE"]
    assert e["blocked_until"] > "2026-01-01"
    assert (e["n"], e["WR"], e["avg_return"], e["PF"]) == (20, 0.0, -5.0, 0.0)   # all losers: PF 0, not None
    assert e["reason"] == "chronic_loser"                  # provenance untouched


def test_expired_entry_renews_on_pf_leg_not_just_win_rate(monkeypatch, tmp_path):
    blocked, _t, saved = _run(monkeypatch, tmp_path, _picks("AEMR", AEM_STYLE),
                              {"AEMR": _entry(EXPIRED)})
    assert "AEMR" in blocked
    assert saved["AEMR"]["blocked_until"] > "2026-01-01"


def test_expired_entry_with_healthy_record_is_still_cleared(monkeypatch, tmp_path):
    spec = [("WIN", 6.0)] * 12 + [("LOSS", -2.0)] * 8
    blocked, _t, saved = _run(monkeypatch, tmp_path, _picks("RECOVERED", spec),
                              {"RECOVERED": _entry(EXPIRED)})
    assert "RECOVERED" not in blocked and "RECOVERED" not in saved


def test_expired_entry_with_no_data_still_renews(monkeypatch, tmp_path):
    blocked, _t, saved = _run(monkeypatch, tmp_path, [], {"NODATA": _entry(EXPIRED, n=5)})
    assert "NODATA" in blocked
    assert saved["NODATA"]["n"] == 5                       # no data -> stats not overwritten
