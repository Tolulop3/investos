"""
strategist_agent.py — InvestOS v4.2
─────────────────────────────────────
Daily investment strategist. Runs after pattern_agent (10:00am ET).
Reads all system outputs → reasons about them → writes a research note.

NOT a trading bot. NOT auto-execution.
A thinking layer that:
  1. Checks if regime is drifting from stated intent
  2. Reviews pick quality against historical evidence
  3. Spots opportunities the daily screen may have missed
  4. Flags contradictions between what the engine says and what data shows
  5. Writes a daily research note you can trust

Uses the Anthropic API — calls Claude to do the actual reasoning.
Commits research note to history/obsidian/research/YYYY-MM-DD.md

AUTONOMY STACK POSITION:
  Layer 1: Engine runs (9:30am)
  Layer 2: Pattern agent closes loop (9:45am)
  Layer 3: Strategist thinks and writes (10:00am)  ← THIS FILE
  Layer 4: You read the note and decide
"""

import json
import os
import pathlib
from datetime import datetime, timezone, timedelta

# ── Config ────────────────────────────────────────────────────────────────
RESEARCH_DIR     = pathlib.Path("history") / "obsidian" / "research"
BRIEF_PATH       = pathlib.Path("latest_brief.json")
HISTORY_ANALYSIS = pathlib.Path("history_analysis.json")
PATTERN_SIGNALS  = pathlib.Path("pattern_signals.json")
OUTCOMES_PATH    = pathlib.Path("outcomes_log.json")
HISTORY_DIR      = pathlib.Path("history")

MODEL            = "claude-sonnet-4-6"
MAX_TOKENS       = 1500


# ── Loaders ───────────────────────────────────────────────────────────────

def load_brief():
    try:
        return json.loads(BRIEF_PATH.read_text())
    except Exception:
        return {}


def load_history_analysis():
    try:
        return json.loads(HISTORY_ANALYSIS.read_text())
    except Exception:
        return {}


def load_pattern_signals():
    try:
        if PATTERN_SIGNALS.exists():
            return json.loads(PATTERN_SIGNALS.read_text())
    except Exception:
        pass
    return {}


def load_recent_obsidian_notes(n=5):
    """Load last n Obsidian daily notes as text."""
    notes = []
    obsidian_dir = pathlib.Path("history") / "obsidian"
    if not obsidian_dir.exists():
        return notes
    files = sorted(obsidian_dir.glob("????-??-??.md"))[-n:]
    for f in files:
        try:
            notes.append({"date": f.stem, "content": f.read_text(encoding="utf-8")[:800]})
        except Exception:
            pass
    return notes


def load_recent_outcomes(days=14):
    """Load resolved picks from last N days."""
    try:
        picks = json.loads(OUTCOMES_PATH.read_text())
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).date().isoformat()
        resolved = [p for p in picks
                    if p.get("outcome") is not None
                    and (p.get("signal_date") or p.get("date") or "") >= cutoff]
        wins   = [p for p in resolved if p.get("outcome") == "WIN"]
        losses = [p for p in resolved if p.get("outcome") == "LOSS"]
        return {
            "n":          len(resolved),
            "wins":       len(wins),
            "losses":     len(losses),
            "wr":         round(len(wins)/len(resolved)*100, 1) if resolved else 0,
            "recent":     resolved[-5:],
        }
    except Exception:
        return {}


# ── Context Builder ────────────────────────────────────────────────────────

def build_context(brief, history_analysis, pattern_signals,
                  obsidian_notes, recent_outcomes):
    """
    Assemble a tight, structured context for the strategist prompt.
    Keeps token count reasonable — strategist needs facts, not raw JSON.
    """
    date = brief.get("date", datetime.now().strftime("%B %d, %Y"))

    # Regime
    sys_exp  = brief.get("system_exposure", {}) or {}
    mkt_reg  = brief.get("market_regime",   {}) or {}
    macro    = brief.get("macro",           {}) or {}
    rm       = brief.get("regime_momentum", {}) or {}
    risk_rep = brief.get("risk_report",     {}) or {}
    decay    = risk_rep.get("decay_monitor",{}) or {}
    breadth  = brief.get("breadth",         {}) or {}
    pcr_data = brief.get("market_pcr",      {}) or {}

    sharpe_raw = decay.get("rolling_sharpe", 0)
    sharpe = sharpe_raw.get("sharpe", 0) if isinstance(sharpe_raw, dict) else sharpe_raw

    # Top picks from ML sizing
    ml_picks = brief.get("ml", {}).get("position_sizing", [])[:5]
    picks_summary = [
        f"{p['ticker']} (score={p['score']}, ML={p['ml_prob']:.0%}, {p['weight_pct']:.0f}%)"
        for p in ml_picks
    ]

    # Conviction picks
    conviction = brief.get("conviction_picks", [])
    conviction_summary = [
        f"{p.get('ticker')} score={p.get('score')} signals={p.get('conviction_count',0)}"
        for p in conviction
    ] if conviction else ["None today"]

    # Active macro signals
    active_signals = list((macro.get("active_signals") or {}).keys())

    # Pattern signals active
    boosted = [f"{t}(+{d['boost']:.0f})" for t, d in pattern_signals.items()
               if d.get("boost", 0) > 0]
    penalised = [f"{t}({d['boost']:.0f})" for t, d in pattern_signals.items()
                 if d.get("boost", 0) < 0]

    # Regime drift from history analyzer
    ha_summary = ""
    if history_analysis:
        traj = history_analysis.get("sharpe_trajectory", [])
        if traj:
            last = traj[-1]
            ha_summary = (f"Sharpe 7d avg={last.get('rolling_avg')} "
                         f"direction={last.get('direction')}")

    # Recent outcome performance
    oc = recent_outcomes
    oc_summary = (f"{oc.get('n',0)} picks in 14d: {oc.get('wr',0)}% WR "
                 f"({oc.get('wins',0)}W/{oc.get('losses',0)}L)")

    # Recent Obsidian notes summary
    notes_text = ""
    for note in obsidian_notes[-3:]:
        notes_text += f"\n--- {note['date']} ---\n{note['content'][:400]}\n"

    context = f"""DATE: {date}

REGIME SNAPSHOT:
  Market regime: {mkt_reg.get('regime')} (SPX ${mkt_reg.get('spx_price',0):,.0f}, +{mkt_reg.get('pct_above_ma',0):.1f}% above 200MA)
  Unified regime: {sys_exp.get('label')} at {sys_exp.get('pct',0)*100:.0f}% exposure
  Risk multiplier: {risk_rep.get('risk_multiplier',1.0):.2f}×
  Macro regime: {macro.get('regime')} — {macro.get('regime_note','')}
  Active signals: {', '.join(active_signals)}
  PCR: {pcr_data.get('pcr',0)} ({pcr_data.get('signal','')})
  Breadth (200MA): {breadth.get('pct_above_200',0)}%
  Sharpe (90d): {sharpe:.3f}
  Neg alpha streak: {decay.get('neg_alpha_days',0)} days
  Regime momentum: {rm.get('momentum','STABLE')} (confidence={rm.get('confidence',0):.0%})
  Breadth slope: {rm.get('breadth_slope',0):+.2f}%/day
  Days in current regime: {rm.get('days_in_regime',0)}

TODAY'S PICKS (ML-sized basket):
  {chr(10).join(picks_summary) if picks_summary else 'None'}

CONVICTION PICKS (2+ signals aligned):
  {chr(10).join(conviction_summary)}

PATTERN SIGNALS (from pattern_agent):
  Score-boosted tickers: {', '.join(boosted) if boosted else 'None yet'}
  Score-penalised tickers: {', '.join(penalised) if penalised else 'None yet'}

RECENT PERFORMANCE (14 days):
  {oc_summary}
  {ha_summary}

RECENT OBSIDIAN NOTES (last 3 days):
{notes_text if notes_text else '  No notes yet'}
"""
    return context


# ── Strategist Prompt ─────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are the InvestOS daily investment strategist. 
You are NOT a trading bot. You think, observe, and advise.

Your role:
1. Read the system state and spot what needs attention
2. Flag contradictions between what the engine says and what data shows
3. Identify regime drift before it becomes a problem
4. Spot opportunities the daily screen may have missed
5. Give honest, direct commentary — no fluff

Your output is a research note for the system owner (Toronto, Canada).
They run a personal investment system across TFSA/FHSA/RRSP/NGX accounts.
NFA context — educational framing only.

Be direct. Be specific. Use the data. Max 400 words.
Structure: Regime Check → Pick Quality → Opportunities → Watch → One Action."""

def call_strategist_api(context):
    """Call Anthropic API with the strategist prompt."""
    try:
        import urllib.request

        user_prompt = f"""Here is today's InvestOS system state. 
Write a daily research note as the investment strategist.

{context}

Write the research note now. Be specific, direct, and useful.
Reference actual tickers and numbers from the data above."""

        payload = json.dumps({
            "model":      MODEL,
            "max_tokens": MAX_TOKENS,
            "system":     SYSTEM_PROMPT,
            "messages":   [{"role": "user", "content": user_prompt}],
        }).encode()

        req = urllib.request.Request(
            "https://api.anthropic.com/v1/messages",
            data=payload,
            headers={
                "Content-Type":      "application/json",
                "anthropic-version": "2023-06-01",
            },
            method="POST",
        )

        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read())

        for block in data.get("content", []):
            if block.get("type") == "text":
                return block["text"]

    except Exception as e:
        return f"[Strategist API call failed: {e}]"

    return "[No content returned]"


# ── Writer ────────────────────────────────────────────────────────────────

def write_research_note(date_str, note_text, context_summary):
    """Write research note to Obsidian research folder."""
    RESEARCH_DIR.mkdir(parents=True, exist_ok=True)
    path = RESEARCH_DIR / f"{date_str}.md"

    content = f"""---
date: {date_str}
type: research
generated: {datetime.now(timezone.utc).isoformat()}
tags: [investos, research, daily, strategist]
---

# InvestOS Daily Research — {date_str}

{note_text}

---

## System Context Used

```
{context_summary[:800]}
```

---
_Generated by strategist_agent.py · InvestOS v4.2 · NFA · Educational only_
"""
    path.write_text(content, encoding="utf-8")
    return path


# ── Main ──────────────────────────────────────────────────────────────────

def run_strategist(verbose=True):
    today = datetime.now(timezone.utc).date().isoformat()

    if verbose:
        print(f"\n{'='*55}")
        print(f"  STRATEGIST AGENT — {today}")
        print(f"{'='*55}\n")

    # Load all inputs
    brief           = load_brief()
    history_analysis= load_history_analysis()
    pattern_signals = load_pattern_signals()
    obsidian_notes  = load_recent_obsidian_notes(5)
    recent_outcomes = load_recent_outcomes(14)

    if not brief:
        print("  ⚠️  No latest_brief.json found — skipping")
        return

    if verbose:
        print(f"  Loaded: brief={bool(brief)}, history_analysis={bool(history_analysis)}")
        print(f"          pattern_signals={len(pattern_signals)} tickers")
        print(f"          obsidian_notes={len(obsidian_notes)} notes")
        print(f"          recent_outcomes={recent_outcomes.get('n',0)} picks (14d)")

    # Build context
    context = build_context(brief, history_analysis, pattern_signals,
                            obsidian_notes, recent_outcomes)

    if verbose:
        print(f"  Context built: {len(context)} chars")
        print(f"  Calling strategist API...")

    # Call API
    note_text = call_strategist_api(context)

    if verbose:
        print(f"  Response: {len(note_text)} chars")
        print(f"\n{'─'*55}")
        print(note_text[:600])
        print(f"{'─'*55}\n")

    # Write note
    path = write_research_note(today, note_text, context)

    if verbose:
        print(f"  📝 Research note → {path}")
        print(f"\n{'='*55}")
        print(f"  STRATEGIST COMPLETE")
        print(f"{'='*55}\n")

    return note_text


if __name__ == "__main__":
    run_strategist(verbose=True)
