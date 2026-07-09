"""
gate_engine.py — Single ML Gate for InvestOS
=============================================
One MLGate instance per run. Both the sizing path (ml_engine.py) and the
conviction path (run_daily.py) use the same instance via ml_results["gate"].

Gate hierarchy (order matters):
  1. Sector-first gate — primary, evidence-backed (evaluated in ml_engine.py).
  2. ML gate (this module) — secondary, compression-aware:
       spread = p90 − p10 of today's ML probs
       if spread < INERT_SPREAD (0.10):
           status = INERT — distribution too compressed to discriminate
           → all sector-approved picks pass; log the spread.
       else:
           threshold = max(MIN_THRESHOLD=0.35, median)
           Hysteresis ±HYSTERESIS_BAND=0.02: a pick within that band of the
           threshold keeps yesterday's gate decision (prevents margin flip-flops).

Conviction path: NOT gated on ML prob.
Conviction = 2+ independent signals aligned — that's a model-independent claim.
"""

import json
import os
from datetime import datetime

GATE_STATE_FILE  = "gate_state.json"
INERT_SPREAD     = 0.10   # spread below this → gate disabled (can't discriminate)
MIN_THRESHOLD    = 0.35   # floor when gate is active (never gate below 35%)
HYSTERESIS_BAND  = 0.02   # ±band around threshold reuses yesterday's decision

OUTCOMES_LOG_FILE  = "outcomes_log.json"
PROBATION_CAP      = 0.50  # new tickers: weight capped at 50% of computed allocation


def load_outcomes_ticker_counts():
    """
    Return {ticker: count} from outcomes_log.json.
    Count = number of logged (resolved or pending) entries per ticker.
    Returns {} if file absent — all tickers treated as new.
    Called once per run; result passed to compute_target_weights.
    """
    try:
        with open(OUTCOMES_LOG_FILE) as f:
            entries = json.load(f)
        if not isinstance(entries, list):
            return {}
        counts = {}
        for e in entries:
            t = e.get("ticker")
            if t:
                counts[t] = counts.get(t, 0) + 1
        return counts
    except (FileNotFoundError, json.JSONDecodeError):
        return {}
    except Exception:
        return {}


class MLGate:
    """
    Compute ML gate decisions for today's pick set.

    Usage (in ml_engine.py):
        gate = MLGate(all_ml_probs, verbose=True)
        for pick in candidates:
            passed = gate.decide(pick["ticker"], pick["ml_prob"], score=pick["score"])
        gate.save_state(decisions_dict)   # {ticker: bool}

    Conviction path:
        Do NOT call gate.decide() in the conviction path. Conviction is
        signal-count only — ML prob is one signal among many, not a gate.
    """

    def __init__(self, all_ml_probs, verbose=True):
        probs = sorted(p for p in all_ml_probs if p is not None)
        n = len(probs)

        if n == 0:
            self.status    = "INERT"
            self.threshold = 0.0
            self.spread    = 0.0
            self.p10 = self.p50 = self.p90 = 0.5
            self.n         = 0
            self._yesterday = {}
            if verbose:
                self._log()
            return

        self.n   = n
        self.p10 = probs[max(0, n // 10)]
        self.p90 = probs[min(n - 1, 9 * n // 10)]
        self.p50 = (probs[n // 2] if n % 2 else
                    (probs[n // 2 - 1] + probs[n // 2]) / 2)
        self.spread = self.p90 - self.p10

        if self.spread < INERT_SPREAD:
            self.status    = "INERT"
            self.threshold = 0.0
        else:
            self.status    = "ACTIVE"
            self.threshold = max(MIN_THRESHOLD, self.p50)

        self._yesterday = self._load_yesterday()

        if verbose:
            self._log()

    # ── Public API ────────────────────────────────────────────────────────────

    def decide(self, ticker, ml_prob, score=None):
        """
        Return True (pass gate) or False (block).

        Rules:
        - INERT gate → always True.
        - score < 90 → always True (only 90-100 tier gets ML-gated).
        - Within ±HYSTERESIS_BAND of threshold → use yesterday's decision.
        - Otherwise → ml_prob >= threshold.
        """
        if self.status == "INERT":
            return True
        if score is not None and score < 90:
            return True
        if ticker in self._yesterday and abs(ml_prob - self.threshold) <= HYSTERESIS_BAND:
            return self._yesterday[ticker]
        return ml_prob >= self.threshold

    def save_state(self, decisions):
        """Persist per-ticker decisions for tomorrow's hysteresis baseline."""
        try:
            payload = {
                "date":      datetime.today().strftime("%Y-%m-%d"),
                "status":    self.status,
                "threshold": self.threshold,
                "spread":    round(self.spread, 4),
                "decisions": {t: bool(v) for t, v in decisions.items()},
            }
            with open(GATE_STATE_FILE, "w") as f:
                json.dump(payload, f, indent=2)
        except Exception:
            pass

    @property
    def is_inert(self):
        return self.status == "INERT"

    def summary(self):
        """Dict suitable for gate_status in the brief and dashboard."""
        return {
            "ml_gate_status":    self.status,
            "ml_gate_threshold": round(self.threshold, 4) if self.threshold else None,
            "ml_gate_spread":    round(self.spread, 4),
            "ml_gate_p10":       round(self.p10, 4),
            "ml_gate_p50":       round(self.p50, 4),
            "ml_gate_p90":       round(self.p90, 4),
            "ml_gate_n":         self.n,
        }

    # ── Internal ──────────────────────────────────────────────────────────────

    def _log(self):
        if self.status == "INERT":
            print(f"  🔕 ML gate: INERT — spread={self.spread:.3f} < {INERT_SPREAD} "
                  f"(p10={self.p10:.3f} p90={self.p90:.3f}, n={self.n}) "
                  "— all sector-approved picks pass")
        else:
            hyst_note = ""
            if self._yesterday:
                n_kept = sum(1 for v in self._yesterday.values() if v)
                hyst_note = (f" | hysteresis: {len(self._yesterday)} yesterday decisions "
                             f"({n_kept} pass)")
            print(f"  🚦 ML gate: ACTIVE — threshold={self.threshold:.3f} "
                  f"(spread={self.spread:.3f}, p10={self.p10:.3f} "
                  f"median={self.p50:.3f} p90={self.p90:.3f}, n={self.n}){hyst_note}")

    def _load_yesterday(self):
        try:
            with open(GATE_STATE_FILE) as f:
                data = json.load(f)
            file_date = datetime.strptime(data["date"], "%Y-%m-%d").date()
            today     = datetime.today().date()
            if (today - file_date).days <= 1:
                return {t: bool(v) for t, v in data.get("decisions", {}).items()}
        except Exception:
            pass
        return {}
