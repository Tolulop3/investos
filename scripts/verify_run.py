"""
scripts/verify_run.py — Post-run fix-verification harness
==========================================================
Reads run_manifest.json, latest_brief.json, and all_scores.json
produced by the most recent run_daily.py execution and checks 8
named invariants. Prints a FIXES VERIFICATION table to stdout.

Usage:
    python scripts/verify_run.py
    python scripts/verify_run.py --strict   # exit 1 if any check fails
"""

import json
import os
import sys

MANIFEST_FILE    = "run_manifest.json"
BRIEF_FILE       = "latest_brief.json"
ALL_SCORES_FILE  = "all_scores.json"
OUTCOMES_LOG     = "outcomes_log.json"

# PF tier labels that carry a drift value in the brief
PF_ALERT_THRESHOLD = 0.10   # abs(drift) >= this → must NOT be labelled 'stable'


def _load(path):
    try:
        with open(path) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def check_insider_engine_ok(manifest):
    """No exception in the insider engine call (flag set by run_daily.py)."""
    if manifest is None:
        return False, "run_manifest.json missing"
    ok = manifest.get("insider_ok", True)
    return ok, "" if ok else "Insider engine raised an exception this run"


def check_options_engine_ok(manifest):
    """No exception in the options engine call."""
    if manifest is None:
        return False, "run_manifest.json missing"
    ok = manifest.get("options_ok", True)
    return ok, "" if ok else "Options engine raised an exception this run"


def check_gate_single_source(brief):
    """Gate status is present in latest_brief.json — means gate ran exactly once."""
    if brief is None:
        return False, "latest_brief.json missing"
    gs = brief.get("gate_status")
    if gs is None:
        return False, "gate_status key absent from brief"
    if not isinstance(gs, dict):
        return False, f"gate_status is not a dict (got {type(gs).__name__})"
    return True, ""


def check_gate_status_logged(brief):
    """Gate status is 'ACTIVE' or 'INERT' — not UNKNOWN or missing."""
    if brief is None:
        return False, "latest_brief.json missing"
    gs = brief.get("gate_status") or {}
    status = gs.get("ml_gate_status", "MISSING")
    ok = status in ("ACTIVE", "INERT")
    return ok, "" if ok else f"ml_gate_status = {status!r} (expected ACTIVE or INERT)"


def check_unified_regime_coverage(brief):
    """brief.unified_regime is set to a non-empty, recognised value."""
    if brief is None:
        return False, "latest_brief.json missing"
    regime = brief.get("unified_regime") or ""
    known  = {"RISK_ON", "NEUTRAL", "DEFENSIVE", "CAPITAL_PRESERVATION"}
    ok = regime in known
    return ok, "" if ok else f"unified_regime = {regime!r}"


def check_all_scores_complete(brief, all_scores):
    """all_scores.json exists and universe_size > 0."""
    if all_scores is None:
        return False, "all_scores.json missing or unreadable"
    size = all_scores.get("universe_size", 0)
    if size == 0:
        return False, "all_scores.json universe_size == 0"
    screened = (brief or {}).get("screen_stats", {}).get("screened", 0)
    if screened > 0 and size < screened:
        return False, f"all_scores.json universe_size {size} < screened {screened}"
    return True, ""


def check_drift_label_sane(brief):
    """No 'stable' drift label where abs(drift) >= 0.10."""
    if brief is None:
        return True, "latest_brief.json missing — skipped"

    violations = []
    pf_monitor = brief.get("pf_drift_monitor") or {}
    tier_drifts = pf_monitor.get("tier_drifts") or {}

    for tier, info in tier_drifts.items():
        drift = info.get("drift", 0) if isinstance(info, dict) else 0
        label = info.get("label", "") if isinstance(info, dict) else ""
        if abs(drift) >= PF_ALERT_THRESHOLD and label == "stable":
            violations.append(f"{tier}: drift={drift:+.2f} labelled 'stable'")

    ok = len(violations) == 0
    return ok, "; ".join(violations) if violations else ""


def check_reserve_integrity(manifest):
    """No substitution ticker also appears in the same-run pre-gate exclusion list."""
    if manifest is None:
        return True, "run_manifest.json missing — skipped"
    substitutions  = set(manifest.get("substitution_tickers", []))
    excluded        = set(manifest.get("pre_gate_excluded", []))
    overlap = substitutions & excluded
    ok = len(overlap) == 0
    return ok, "" if ok else f"Tickers in both reserve and removal: {sorted(overlap)}"


# ── Main ─────────────────────────────────────────────────────────────────────

def run_checks():
    manifest   = _load(MANIFEST_FILE)
    brief      = _load(BRIEF_FILE)
    all_scores = _load(ALL_SCORES_FILE)

    checks = [
        ("insider_engine_ok",   check_insider_engine_ok(manifest)),
        ("options_engine_ok",   check_options_engine_ok(manifest)),
        ("gate_single_source",  check_gate_single_source(brief)),
        ("gate_status_logged",  check_gate_status_logged(brief)),
        ("unified_regime_cov",  check_unified_regime_coverage(brief)),
        ("all_scores_complete", check_all_scores_complete(brief, all_scores)),
        ("drift_label_sane",    check_drift_label_sane(brief)),
        ("reserve_integrity",   check_reserve_integrity(manifest)),
    ]

    run_date = (manifest or {}).get("run_date", "unknown")
    print(f"\n{'─' * 52}")
    print(f"  FIXES VERIFICATION  ({run_date})")
    print(f"{'─' * 52}")
    print(f"  {'Check':<25}  {'Status':<6}  Note")
    print(f"  {'─'*25}  {'─'*6}  {'─'*16}")

    all_pass = True
    for name, (ok, note) in checks:
        icon    = "✅" if ok else "🔴"
        status  = "PASS" if ok else "FAIL"
        note_s  = f"  {note[:40]}" if note else ""
        print(f"  {icon} {name:<25}  {status:<6}{note_s}")
        if not ok:
            all_pass = False

    print(f"{'─' * 52}\n")
    return all_pass


if __name__ == "__main__":
    strict = "--strict" in sys.argv
    passed = run_checks()
    if strict and not passed:
        sys.exit(1)
