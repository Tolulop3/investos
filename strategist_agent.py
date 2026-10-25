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

HYBRID DESIGN (2026-10-25): a deterministic core computes the checks
below from system data and ALWAYS produces the research note — regime
state, contradiction flags, pick quality, opportunities, watch items,
and one prioritised action. The Anthropic API layer (Claude) adds a
commentary section on top when the call succeeds; if it fails, the note
still ships and the failure is recorded in the note itself. The AI is an
enhancement, never a single point of failure. (Before this change the
note WAS the API call: when it began failing on 2026-07-23, 86 of the
first 90 notes were failure banners and nothing else was produced.)
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


# ── Deterministic Core ──────────────────────────────────────────────────

def build_deterministic_note(brief, history_analysis, pattern_signals,
                             recent_outcomes):
    """
    Compute the strategist's checks from system data and render the note.
    Pure function of its inputs — no API, no I/O — so it cannot fail the
    way the API layer did. Returns (note_markdown, flags_list).
    """
    sys_exp  = brief.get("system_exposure", {}) or {}
    mkt_reg  = brief.get("market_regime",   {}) or {}
    macro    = brief.get("macro",           {}) or {}
    rm       = brief.get("regime_momentum", {}) or {}
    risk_rep = brief.get("risk_report",     {}) or {}
    decay    = risk_rep.get("decay_monitor", {}) or {}
    breadth  = brief.get("breadth",         {}) or {}
    pcr_data = brief.get("market_pcr",      {}) or {}

    mkt   = str(mkt_reg.get("regime") or "UNKNOWN")
    mac   = str(macro.get("regime") or "UNKNOWN")
    expo  = float(sys_exp.get("pct") or 0)
    label = sys_exp.get("label") or "?"
    neg_alpha = int(decay.get("neg_alpha_days") or 0)
    sharpe_raw = decay.get("rolling_sharpe", 0)
    sharpe = sharpe_raw.get("sharpe", 0) if isinstance(sharpe_raw, dict) else (sharpe_raw or 0)
    slope = float(rm.get("breadth_slope") or 0)
    pcr_signal = str(pcr_data.get("signal") or "")

    ml_picks = (brief.get("ml", {}) or {}).get("position_sizing", [])[:5]
    basket = [str(p.get("ticker")) for p in ml_picks if p.get("ticker")]
    conviction = brief.get("conviction_picks", []) or []

    boosted   = {t: d for t, d in pattern_signals.items() if (d.get("boost") or 0) > 0}
    penalised = {t: d for t, d in pattern_signals.items() if (d.get("boost") or 0) < 0}
    penalised_in_basket = [t for t in basket if t in penalised]
    boosted_not_in_basket = [t for t in boosted if t not in basket]

    # ── Contradiction flags ──
    flags = []
    if "BULL" in mkt and "RISK_OFF" in mac:
        flags.append(f"Market regime is {mkt} but the macro overlay is {mac} (defensive).")
    if "BEAR" in mkt and "RISK_ON" in mac:
        flags.append(f"Market regime is {mkt} but the macro overlay is {mac} (risk-seeking).")
    if expo >= 0.70 and "RISK_OFF" in mac:
        flags.append(f"Exposure is {expo*100:.0f}% while the macro overlay recommends defensive positioning.")
    if expo <= 0.30 and "RISK_ON" in mac:
        flags.append(f"Exposure is only {expo*100:.0f}% while the macro overlay is {mac}.")
    if neg_alpha >= 30 and expo >= 0.60:
        flags.append(f"Negative-alpha streak is {neg_alpha} days while exposure sits at {expo*100:.0f}%.")
    if "BEARISH" in pcr_signal and "BULL" in mkt:
        flags.append(f"Options PCR reads {pcr_signal} against a {mkt} market regime — hedging the engine isn't pricing.")
    if "BULLISH" in pcr_signal and "BEAR" in mkt:
        flags.append(f"Options PCR reads {pcr_signal} against a {mkt} market regime.")
    if slope < 0 and "BULL" in mkt:
        flags.append(f"Breadth slope is {slope:+.2f}%/day under a {mkt} regime — participation is narrowing.")
    for t in penalised_in_basket:
        flags.append(f"{t} is in today's ML basket but the pattern agent flags it "
                     f"{penalised[t].get('action', 'AVOID')} ({penalised[t].get('reason', '')}).")

    # ── Sections ──
    active_signals = list((macro.get("active_signals") or {}).keys())
    regime_md = f"""## Regime Check
- Market: **{mkt}** — SPX ${float(mkt_reg.get('spx_price') or 0):,.0f}, {float(mkt_reg.get('pct_above_ma') or 0):+.1f}% vs its 200-day average.
- System stance: **{label}** at {expo*100:.0f}% exposure · risk multiplier {float(risk_rep.get('risk_multiplier') or 1.0):.2f}×.
- Macro overlay: **{mac}** — {macro.get('regime_note', '')}
- Active macro signals: {', '.join(active_signals) if active_signals else 'none'}.
- Breadth: {breadth.get('pct_above_200', 0)}% above 200MA, slope {slope:+.2f}%/day · PCR {pcr_data.get('pcr', 0)} ({pcr_signal or 'n/a'}).
- Regime age: {rm.get('days_in_regime', 0)} days · momentum {rm.get('momentum', 'STABLE')} (confidence {float(rm.get('confidence') or 0):.0%})."""

    flags_md = "## Contradictions & Flags\n" + (
        "\n".join(f"- ⚠️ {f}" for f in flags) if flags
        else "- None today — the system's signals are internally consistent.")

    if ml_picks:
        scores = [float(p.get("score") or 0) for p in ml_picks]
        probs  = [float(p.get("ml_prob") or 0) for p in ml_picks]
        basket_txt = ", ".join(
            f"{p.get('ticker')} (score {float(p.get('score') or 0):.0f}, ML {float(p.get('ml_prob') or 0):.0%}, {float(p.get('weight_pct') or 0):.0f}%)"
            for p in ml_picks)
        quality_lines = [
            f"- Today's ML basket: {basket_txt}.",
            f"- Basket means: score {sum(scores)/len(scores):.0f}, ML probability {sum(probs)/len(probs):.0%}.",
        ]
        low = [p.get("ticker") for p in ml_picks if float(p.get("score") or 0) < 60]
        if low:
            quality_lines.append(f"- ⚠️ Below the usual quality line (score < 60): {', '.join(low)}.")
    else:
        quality_lines = ["- No ML-sized basket today."]

    oc = recent_outcomes or {}
    if oc.get("n"):
        quality_lines.append(
            f"- Last 14 days: {oc['n']} resolved picks at {oc.get('wr', 0)}% win rate "
            f"({oc.get('wins', 0)}W / {oc.get('losses', 0)}L).")
    aligned = [t for t in basket if t in boosted]
    if aligned:
        quality_lines.append(f"- Pattern-aligned basket names (boosted): {', '.join(aligned)}.")
    if penalised_in_basket:
        quality_lines.append(
            f"- ⚠️ Pattern-penalised names sized anyway: {', '.join(penalised_in_basket)} "
            f"(see Contradictions).")
    quality_md = "## Pick Quality\n" + "\n".join(quality_lines)

    opp_lines = []
    conv_out = [p for p in conviction if p.get("ticker") not in basket]
    if conv_out:
        opp_lines.append("- Conviction picks outside the basket: " + ", ".join(
            f"{p.get('ticker')} (score {p.get('score')}, {p.get('conviction_count', 0)} signals)"
            for p in conv_out))
    if boosted_not_in_basket:
        opp_lines.append("- Pattern-boosted but not picked: " + ", ".join(
            f"{t} (+{boosted[t].get('boost', 0):.0f}: {boosted[t].get('reason', '')})"
            for t in boosted_not_in_basket))
    opp_md = "## Opportunities\n" + (
        "\n".join(opp_lines) if opp_lines
        else "- Nothing new outside today's basket — the screen and the pattern layer agree on where the action is.")

    watch_lines = [f"- Negative-alpha streak: {neg_alpha} days."]
    traj = (history_analysis or {}).get("sharpe_trajectory", [])
    if traj:
        last = traj[-1]
        watch_lines.append(
            f"- Rolling Sharpe (90d): {float(sharpe or 0):.3f} · 7d trajectory avg "
            f"{last.get('rolling_avg')}, direction {last.get('direction')}.")
    else:
        watch_lines.append(f"- Rolling Sharpe (90d): {float(sharpe or 0):.3f}.")
    if penalised:
        watch_lines.append("- Pattern-penalised: " + ", ".join(
            f"{t} ({d.get('boost', 0):+.0f})" for t, d in penalised.items()))
    watch_lines.append(f"- Regime age: {rm.get('days_in_regime', 0)} days in {mkt}.")
    watch_md = "## Watch\n" + "\n".join(watch_lines)

    # ── One Action (deterministic priority ladder) ──
    if penalised_in_basket:
        t = penalised_in_basket[0]
        action = (f"Review {t} before acting on today's basket — the pattern agent flags it "
                  f"{penalised[t].get('action', 'AVOID')} ({penalised[t].get('reason', '')}), "
                  f"yet the engine sized it today.")
    elif expo >= 0.70 and "RISK_OFF" in mac:
        action = (f"Decide the stance conflict explicitly: {expo*100:.0f}% exposure into a "
                  f"{mac} macro read is today's biggest open contradiction.")
    elif neg_alpha >= 30:
        action = (f"Audit the recent pick stream: a {neg_alpha}-day negative-alpha streak "
                  f"with {oc.get('n', 0)} resolved picks in 14 days at {oc.get('wr', 0)}% win rate.")
    elif oc.get("n", 0) >= 20 and oc.get("wr", 0) < 40:
        action = (f"Pick quality is below the line — {oc.get('wr', 0)}% win rate over "
                  f"{oc.get('n', 0)} resolved picks (14d). Review category mix before adding new picks.")
    elif flags:
        action = f"Resolve today's top flag: {flags[0]}"
    else:
        action = "No action required — no material contradictions today; the system is internally consistent."
    action_md = f"## One Action\n{action}"

    note = "\n\n".join([regime_md, flags_md, quality_md, opp_md, watch_md, action_md])
    return note, flags


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
        import urllib.error

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

        api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        req = urllib.request.Request(
            "https://api.anthropic.com/v1/messages",
            data=payload,
            headers={
                "Content-Type":      "application/json",
                "anthropic-version": "2023-06-01",
                "x-api-key":         api_key,
            },
            method="POST",
        )

        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read())

        for block in data.get("content", []):
            if block.get("type") == "text":
                return block["text"]

    except urllib.error.HTTPError as e:
        # Surface the API's own error message (2026-10-25). Until now the
        # body was discarded and only "HTTP Error 400" reached the note —
        # which is why the failure that started 2026-07-23 (after 4 good
        # notes) stayed undiagnosed for ~3 months: the status line alone
        # can't distinguish billing, model, and request-shape causes.
        body = ""
        try:
            body = e.read().decode("utf-8", "replace")
        except Exception:
            pass
        detail = body.strip()
        try:
            detail = json.loads(body).get("error", {}).get("message", detail)
        except Exception:
            pass
        print(f"  ❌ Anthropic API error {e.code}: {detail[:500]}")
        return (f"[Strategist API call failed: HTTP {e.code} — {detail[:300]}]")

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
        # Loud, not silent: producing no note at all is a real failure.
        print("  ❌ No latest_brief.json found — cannot build a note")
        raise SystemExit(1)

    if verbose:
        print(f"  Loaded: brief={bool(brief)}, history_analysis={bool(history_analysis)}")
        print(f"          pattern_signals={len(pattern_signals)} tickers")
        print(f"          obsidian_notes={len(obsidian_notes)} notes")
        print(f"          recent_outcomes={recent_outcomes.get('n',0)} picks (14d)")

    # Build context
    context = build_context(brief, history_analysis, pattern_signals,
                            obsidian_notes, recent_outcomes)

    # Hard cap (2026-10-25): the builder is deliberately tight (~a few K
    # chars), but the state it summarises keeps growing — cap it so the
    # prompt can never balloon into a size-based API rejection.
    if len(context) > 12000:
        context = context[:12000] + "\n  [context truncated at 12,000 chars]"

    if verbose:
        print(f"  Context built: {len(context)} chars")
        print(f"  Calling strategist API...")

    # Deterministic core — always runs; the note exists even if the
    # API layer is down (hybrid design, 2026-10-25).
    try:
        core_note, flags = build_deterministic_note(
            brief, history_analysis, pattern_signals, recent_outcomes)
    except Exception as e:
        # The core failing is a genuine bug — loud, never silent.
        print(f"  ❌ Deterministic core failed: {e}")
        raise SystemExit(1)

    # AI commentary layer — an enhancement, not a dependency. On failure
    # the note still ships, with the API's own error recorded in place of
    # the commentary (the error-body surfacing from the 2026-10-25 fix).
    ai_text = call_strategist_api(context)
    if ai_text.startswith("[Strategist API call failed") or ai_text == "[No content returned]":
        print("  ⚠️  AI commentary unavailable this run — core note still written")
        ai_section = ("## Strategist Commentary (AI)\n\n"
                      f"_Unavailable this run. {ai_text}_")
    else:
        ai_section = f"## Strategist Commentary (AI)\n\n{ai_text}"

    note_text = core_note + "\n\n---\n\n" + ai_section

    if verbose:
        print(f"  Core note: {len(core_note)} chars · flags: {len(flags)}")
        print(f"  AI commentary: {len(ai_text)} chars")
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
