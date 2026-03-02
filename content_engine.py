"""
InvestOS — Social Content Engine
==================================
Generates daily X content for @adejuwon_t

Philosophy:
  - Short. Observational. One thing worth knowing.
  - Makes followers feel like insiders.
  - Never a data dump. Never hype.
  - Always: NFA | Educational content only.

Output:
  - TWEET: single post, punchy, under 240 chars
  - THREAD: 5-6 tweet thread expanding the brief
  - WEEKLY: recap post for Sunday

Voice: Direct. Calm. Confident. Data-backed. Not a guru.
       Someone who actually watches the market daily
       and shares what they notice.
"""

import json
import random
from datetime import datetime, timedelta


# ============================================================
# VOICE CONFIG
# ============================================================

VOICE = {
    "handle":     "@adejuwon_t",
    "brand":      "",           # Fill when ready
    "disclaimer": "NFA | Educational only 🇨🇦",
    "hashtags":   {
        "base":   ["#investing", "#TSX"],
        "fx":     ["#forex", "#gold", "#XAUUSD"],
        "canada": ["#TFSA", "#Canada", "#CanadianInvestor"],
        "macro":  ["#macro", "#markets"],
    },
    "style": {
        "max_tweet_chars": 260,
        "thread_tweets":   6,
        "tone":            "calm_observational",
    }
}


# ============================================================
# TWEET TEMPLATES
# Each template fits a different market situation
# ============================================================

TWEET_TEMPLATES = {

    "risk_off_macro": [
        "{macro_icon} {macro_signal} in the air today.\n\nWatching {ticker} — {one_line_reason}.\n\n{disclaimer}",
        "Market's pricing in {macro_signal}.\n\n{ticker} worth keeping an eye on this week.\n\n{one_line_reason}.\n\n{disclaimer}",
        "Something shifting in the macro today.\n\n{macro_signal} — affects {affected_sector}.\n\nKnow what you own.\n\n{disclaimer}",
    ],

    "strong_signal": [
        "{ticker} showing up in the data today.\n\n{one_line_reason}.\n\nWorth a look.\n\n{disclaimer}",
        "One thing catching my eye: {ticker}\n\n{one_line_reason}.\n\nNo rush. Just watching.\n\n{disclaimer}",
        "Signal count on {ticker}: {signal_count}/6\n\nWhen multiple things agree, I pay attention.\n\n{disclaimer}",
    ],

    "fx_gold": [
        "Gold moving today.\n\n{gold_reason}.\n\nSafe haven flows are real right now.\n\n{disclaimer}",
        "{fx_pair} — {fx_direction} bias based on today's data.\n\n{fx_reason}.\n\nWatch closely.\n\n{disclaimer}",
        "Macro shifting. FX is pricing it in.\n\n{fx_pair}: {fx_direction}\n\nKey driver: {fx_reason}\n\n{disclaimer}",
    ],

    "dividend_deadline": [
        "Dividend reminder 📅\n\n{ticker} goes ex-div {ex_div_date}.\n\nDeadline to buy: {buy_deadline}.\n\n${div_amount}/share.\n\n{disclaimer}",
        "Clock ticking on {ticker}.\n\nEx-div {ex_div_date} — buy by {buy_deadline} to collect ${div_amount}/share.\n\n{disclaimer}",
    ],

    "regime_change": [
        "Market regime shifted to {regime} today.\n\nSystem auto-adjusting. Watching how {affected} holds up.\n\n{disclaimer}",
        "{regime_icon} Regime: {regime}\n\nWhat this means: {regime_note}\n\n{disclaimer}",
    ],

    "calm_day": [
        "Quiet day in the data today.\n\nNo urgent signals. Sometimes that's the signal.\n\nStay patient.\n\n{disclaimer}",
        "Market doing its thing.\n\nNothing screaming buy or sell right now.\n\nBest days to do nothing are underrated.\n\n{disclaimer}",
        "Low signal day.\n\nWatching {ticker} for a setup that isn't quite there yet.\n\nPatience is a position.\n\n{disclaimer}",
    ],

    "earnings_watch": [
        "Heads up — {ticker} reports earnings {earnings_date}.\n\nIf you're holding, know your exit plan.\n\nEarnings = volatility.\n\n{disclaimer}",
    ],
}


# ============================================================
# CONTENT BUILDER
# ============================================================

def pick_best_tweet_situation(brief):
    """
    Decide what today's tweet is about based on the brief data.
    Priority: urgent calendar > strong conviction pick > FX signal > macro > calm
    """
    macro    = brief.get("macro", {})
    regime   = macro.get("regime", "NORMAL")
    sigs     = macro.get("signals_detected", 0)
    cal      = brief.get("calendar", [])
    conv     = brief.get("conviction_picks", [])
    fx       = brief.get("fx_signals", {})

    # Priority 1: Urgent calendar item today
    urgent = [c for c in cal if c.get("urgency") == "urgent"]
    if urgent:
        item = urgent[0]
        if "Ex-Dividend" in item.get("title", "") or "dividend" in item.get("title", "").lower():
            return "dividend_deadline", item
        if "EXIT" in item.get("action", ""):
            return "earnings_watch", item

    # Priority 2: High conviction pick (3+ signals)
    strong_picks = [p for p in conv if p.get("conviction_count", 0) >= 3]
    if strong_picks:
        return "strong_signal", strong_picks[0]

    # Priority 3: Strong FX/Gold signal
    fx_pairs = fx.get("pairs", {}) if fx else {}
    strong_fx = [r for r in fx_pairs.values()
                 if r.get("conviction", 0) >= 60 and r.get("direction") != "NEUTRAL"]
    if strong_fx:
        top_fx = sorted(strong_fx, key=lambda x: x["conviction"], reverse=True)[0]
        if "Gold" in top_fx.get("pair", ""):
            return "fx_gold", top_fx
        return "fx_gold", top_fx

    # Priority 4: Risk-off macro
    if regime in ("RISK_OFF", "CAUTIOUS") or sigs >= 3:
        return "risk_off_macro", macro

    # Priority 5: Regime note
    if regime in ("BEAR", "RECOVERY"):
        return "regime_change", {"regime": regime, "regime_note": macro.get("regime_note", "")}

    # Default: calm observational
    top_pick = conv[0] if conv else None
    return "calm_day", top_pick


def build_tweet(brief):
    """Build the daily tweet — short, punchy, one thing worth knowing"""

    situation, data = pick_best_tweet_situation(brief)
    macro    = brief.get("macro", {})
    regime   = macro.get("regime", "NORMAL")
    fx       = brief.get("fx_signals", {})
    conv     = brief.get("conviction_picks", [])
    cal      = brief.get("calendar", [])

    regime_icons = {
        "BULL": "🟢", "RECOVERY": "🟡", "CAUTION": "🟡",
        "CAUTIOUS": "⚠️", "RISK_OFF": "🚨", "BEAR": "🔴", "NORMAL": "📊"
    }
    regime_icon = regime_icons.get(regime, "📊")

    tweet = ""

    if situation == "dividend_deadline":
        ticker   = data.get("ticker", "")
        title    = data.get("title", "")
        desc     = data.get("desc", "")
        # Extract from description
        tweet = f"📅 Dividend deadline coming up.\n\n{title.replace('💰 ', '').replace('📅 ', '')}\n\n{desc[:100]}\n\n{VOICE['disclaimer']}"

    elif situation == "strong_signal":
        ticker  = data.get("ticker", "")
        sigs    = data.get("conviction_count", 0)
        reasons = data.get("reasons", [])
        reason  = reasons[0].replace("📰 ","").replace("💰 ","").replace("🚀 ","").replace("✅ ","") if reasons else "multiple signals aligned"
        tweet   = f"{ticker} showing up in the data today.\n\n{reason[:80]}.\n\n{sigs} signals aligned.\n\n{VOICE['disclaimer']}"

    elif situation == "fx_gold":
        pair      = data.get("pair", "Gold")
        direction = data.get("direction", "")
        driver    = data.get("key_driver", "")[:60]
        icon      = "🟢" if direction == "LONG" else "🔴"
        if "Gold" in pair:
            tweet = f"Gold {icon} {direction} bias today.\n\n{driver}.\n\nSafe haven flows worth watching.\n\n{VOICE['disclaimer']}"
        else:
            tweet = f"{pair} — {icon} {direction} bias.\n\n{driver[:60]}.\n\nWatch closely.\n\n{VOICE['disclaimer']}"

    elif situation == "risk_off_macro":
        sigs = macro.get("signals_detected", 0)
        active = list(macro.get("active_signals", {}).keys())
        top_sig = active[0].replace("_", " ").title() if active else "Macro uncertainty"
        affected = "the market"
        # Find what sector is most affected
        sector_sent = macro.get("sector_sentiment", {})
        bearish = [s for s, d in sector_sent.items() if d.get("sentiment") == "BEARISH"]
        if bearish:
            affected = bearish[0].replace("_", " ").lower()
        tweet = f"{regime_icon} {top_sig[:40]} in focus today.\n\nWatching how {affected} holds up.\n\nKnow what you own.\n\n{VOICE['disclaimer']}"

    elif situation == "regime_change":
        regime_name = data.get("regime", regime)
        note        = data.get("regime_note", "")[:80]
        tweet       = f"{regime_icon} Regime: {regime_name}\n\n{note}\n\nAdjusting accordingly.\n\n{VOICE['disclaimer']}"

    elif situation == "earnings_watch":
        ticker = data.get("ticker", "")
        date   = data.get("date", "soon")
        tweet  = f"⚠️ {ticker} earnings {date}.\n\nIf you're in a swing position, have your exit ready.\n\nEarnings = vol spike.\n\n{VOICE['disclaimer']}"

    else:  # calm_day
        if data:
            ticker  = data.get("ticker", "")
            reasons = data.get("reasons", [])
            note    = reasons[0][:60] if reasons else "not quite there yet"
            tweet   = f"Quiet signal day.\n\nWatching {ticker} — {note}.\n\nNo rush.\n\n{VOICE['disclaimer']}"
        else:
            tweet   = f"Low signal day today.\n\nSometimes the best trade is no trade.\n\nPatience is a position.\n\n{VOICE['disclaimer']}"

    # Clean up and trim
    tweet = tweet.strip()
    if len(tweet) > 270:
        tweet = tweet[:267] + "..."

    return {
        "tweet":     tweet,
        "situation": situation,
        "chars":     len(tweet),
        "hashtags":  _pick_hashtags(situation, brief),
    }


def _pick_hashtags(situation, brief):
    """Pick 2-3 relevant hashtags based on situation"""
    tags = list(VOICE["hashtags"]["base"])  # Always include base

    if situation in ("fx_gold",):
        tags += random.sample(VOICE["hashtags"]["fx"], 1)
    elif situation in ("dividend_deadline", "strong_signal"):
        tags += random.sample(VOICE["hashtags"]["canada"], 1)
    elif situation in ("risk_off_macro", "regime_change"):
        tags += random.sample(VOICE["hashtags"]["macro"], 1)

    return " ".join(tags[:3])


# ============================================================
# THREAD BUILDER
# ============================================================

def build_thread(brief):
    """
    Build a 5-6 tweet thread expanding the daily brief.
    Format: Hook → Macro → Top Pick → FX/Gold → Watch List → Close
    """
    macro    = brief.get("macro", {})
    regime   = macro.get("regime", "NORMAL")
    conv     = brief.get("conviction_picks", [])
    fx       = brief.get("fx_signals", {})
    cal      = brief.get("calendar", [])
    screen   = brief.get("screen_stats", {})
    date     = brief.get("date", datetime.now().strftime("%B %d, %Y"))

    regime_icons = {
        "BULL": "🟢", "RECOVERY": "🟡", "CAUTIOUS": "⚠️",
        "RISK_OFF": "🚨", "BEAR": "🔴", "NORMAL": "📡", "CAUTION": "🟡"
    }
    icon = regime_icons.get(regime, "📡")

    threads = []

    # ── Tweet 1: Hook ──────────────────────────────────────
    sigs   = macro.get("signals_detected", 0)
    arts   = macro.get("articles_read", 0)
    screened = screen.get("screened", 0)
    hook_lines = [
        f"Morning brief — {date} 🧵\n\n{arts} news articles read.\n{screened}+ stocks screened.\nHere's what matters today.",
        f"Daily analysis — {date} 🧵\n\n{sigs} macro signals detected.\n{screened} stocks screened.\nLet's get into it.",
        f"What the data says today — {date} 🧵\n\n{arts} articles read.\n{sigs} signals flagged.\nThread 👇",
    ]
    threads.append(f"1/ {random.choice(hook_lines)}")

    # ── Tweet 2: Macro regime ──────────────────────────────
    regime_note   = macro.get("regime_note", "")
    active_sigs   = list(macro.get("active_signals", {}).keys())[:2]
    sig_names     = " + ".join([s.replace("_", " ").title()[:25] for s in active_sigs]) if active_sigs else "No major signals"

    threads.append(
        f"2/ {icon} MACRO REGIME: {regime}\n\n"
        f"{regime_note[:100]}\n\n"
        f"Key signals: {sig_names}"
    )

    # ── Tweet 3: Top conviction pick ───────────────────────
    if conv:
        top = conv[0]
        ticker   = top.get("ticker", "")
        score    = top.get("score", 0)
        sigs_hit = top.get("conviction_signals", [])
        reasons  = top.get("reasons", [])
        pick_d   = top.get("data", {})
        pick_p   = top.get("pick", {})

        reason_line = reasons[0][:60] if reasons else "multiple factors aligned"
        sig_count   = len(sigs_hit)
        exp_low     = pick_p.get("exp_low", 0)
        exp_high    = pick_p.get("exp_high", 0)
        amount      = pick_p.get("amount", 0)
        category    = pick_p.get("category", "")

        threads.append(
            f"3/ TOP SIGNAL: ${ticker}\n\n"
            f"Score: {score}/100 | {sig_count} signals aligned\n"
            f"{reason_line}\n\n"
            f"Category: {category}\n"
            f"Expected: +{exp_low}% → +{exp_high}%\n\n"
            f"NFA — do your own research."
        )
    else:
        threads.append(
            f"3/ No high-conviction picks today.\n\n"
            f"Screened {screen.get('screened',0)}+ stocks.\n"
            f"Nothing clearing all filters.\n\n"
            f"Staying patient. That's a valid position."
        )

    # ── Tweet 4: FX / Gold ─────────────────────────────────
    fx_pairs = fx.get("pairs", {}) if fx else {}
    active_fx = sorted(
        [r for r in fx_pairs.values() if r.get("conviction", 0) >= 50 and r.get("direction") != "NEUTRAL"],
        key=lambda x: x["conviction"], reverse=True
    )

    if active_fx:
        top_fx   = active_fx[0]
        fx_icon  = "🟢" if top_fx["direction"] == "LONG" else "🔴"
        pair_name = top_fx.get("pair", "")
        direction = top_fx.get("direction", "")
        conviction= top_fx.get("conviction", 0)
        driver    = top_fx.get("key_driver", "")[:50]
        alignment = top_fx.get("alignment", "")

        second_line = ""
        if len(active_fx) >= 2:
            fx2    = active_fx[1]
            icon2  = "🟢" if fx2["direction"] == "LONG" else "🔴"
            second_line = f"\n{icon2} {fx2['pair']}: {fx2['direction']} ({fx2['conviction']}%)"

        threads.append(
            f"4/ FX & GOLD\n\n"
            f"{fx_icon} {pair_name}: {direction} ({conviction}% conviction)\n"
            f"Driver: {driver}"
            f"{second_line}\n\n"
            f"Signals {alignment}\n"
            f"NFA — manual execution only."
        )
    else:
        threads.append(
            f"4/ FX & GOLD\n\n"
            f"No strong directional signals today.\n\n"
            f"Watching price action but nothing with conviction.\n"
            f"Sometimes the right call is no call."
        )

    # ── Tweet 5: Watch list / Calendar ─────────────────────
    urgent_items = [c for c in cal if c.get("urgency") == "urgent"][:2]
    soon_items   = [c for c in cal if c.get("urgency") == "soon"][:2]
    watch_items  = urgent_items + soon_items

    if watch_items:
        watch_lines = "\n".join([f"• {item['title'][:55]}" for item in watch_items[:3]])
        threads.append(
            f"5/ WATCH THIS WEEK\n\n"
            f"{watch_lines}\n\n"
            f"Calendar drives prices.\n"
            f"Know the dates before they happen."
        )
    else:
        threads.append(
            f"5/ WATCH THIS WEEK\n\n"
            f"No major deadlines or earnings this week.\n\n"
            f"Good time to review positions and make sure\n"
            f"you're comfortable with what you hold."
        )

    # ── Tweet 6: Close ─────────────────────────────────────
    close_lines = [
        f"6/ That's the brief.\n\nBuilt on real data. 500+ stocks screened daily.\nNot predictions — signal analysis.\n\n{VOICE['disclaimer']}\n\nFollow for daily analysis 👊",
        f"6/ System runs daily.\n\nNews feeds, price data, macro signals — all automated.\nI share what's worth knowing.\n\n{VOICE['disclaimer']}\n\nFollow for more 📊",
        f"6/ Daily analysis done.\n\nNot financial advice — sharing the process.\nBuild your own view from the data.\n\n{VOICE['disclaimer']}",
    ]
    threads.append(random.choice(close_lines))

    return {
        "tweets":  threads,
        "count":   len(threads),
        "date":    date,
    }


# ============================================================
# WEEKLY RECAP BUILDER
# ============================================================

def build_weekly_recap(brief_history):
    """
    Build Sunday weekly recap post.
    brief_history: list of last 5 daily briefs
    """
    if not brief_history:
        return {"post": "Weekly recap — no history yet. System just launched.", "type": "weekly"}

    # Summarize the week
    regimes   = [b.get("macro", {}).get("regime", "NORMAL") for b in brief_history]
    top_picks = []
    for b in brief_history:
        for p in b.get("conviction_picks", [])[:1]:
            top_picks.append(p.get("ticker", ""))

    dominant_regime = max(set(regimes), key=regimes.count)
    unique_picks    = list(dict.fromkeys(top_picks))[:3]  # deduplicated

    post = (
        f"Week in review 📊\n\n"
        f"Dominant regime: {dominant_regime}\n"
        f"Top signals this week: {', '.join(['$'+t for t in unique_picks]) if unique_picks else 'None flagged'}\n\n"
        f"What worked: Staying in line with the regime.\n"
        f"What to watch next week: Follow the macro.\n\n"
        f"Full analysis runs every morning.\n"
        f"{VOICE['disclaimer']}"
    )

    return {"post": post, "type": "weekly", "date": datetime.now().strftime("%B %d, %Y")}


# ============================================================
# MAIN CONTENT ENGINE
# ============================================================

def run_content_engine(brief, brief_history=None, verbose=True):
    """
    Generate all social content for the day.
    Returns tweet, thread, and optional weekly recap.
    """
    now = datetime.now()

    if verbose:
        print(f"\n{'='*55}")
        print(f"  CONTENT ENGINE — @adejuwon_t")
        print(f"{'='*55}")

    # Inject FX signals into brief if not there
    fx_file = "fx_signals.json"
    if "fx_signals" not in brief:
        try:
            import os
            if os.path.exists(fx_file):
                with open(fx_file) as f:
                    brief["fx_signals"] = json.load(f)
        except:
            pass

    # Build tweet
    tweet_data = build_tweet(brief)
    if verbose:
        print(f"\n📝 TWEET ({tweet_data['chars']} chars):")
        print(f"{'─'*50}")
        print(tweet_data["tweet"])
        if tweet_data["hashtags"]:
            print(f"\n{tweet_data['hashtags']}")
        print(f"{'─'*50}")

    # Build thread
    thread_data = build_thread(brief)
    if verbose:
        print(f"\n🧵 THREAD ({thread_data['count']} tweets):")
        print(f"{'─'*50}")
        for i, t in enumerate(thread_data["tweets"]):
            print(f"\n{t}")
        print(f"{'─'*50}")

    # Weekly recap (Sundays only)
    weekly = None
    if now.weekday() == 6 and brief_history:
        weekly = build_weekly_recap(brief_history)
        if verbose:
            print(f"\n📅 WEEKLY RECAP:")
            print(weekly["post"])

    result = {
        "date":         now.strftime("%B %d, %Y"),
        "generated_at": now.isoformat(),
        "handle":       VOICE["handle"],
        "tweet":        tweet_data["tweet"],
        "tweet_hashtags": tweet_data["hashtags"],
        "tweet_chars":  tweet_data["chars"],
        "tweet_situation": tweet_data["situation"],
        "thread":       thread_data["tweets"],
        "thread_count": thread_data["count"],
        "weekly_recap": weekly,
        "copy_paste": {
            "tweet_full": tweet_data["tweet"] + "\n\n" + tweet_data["hashtags"],
            "thread_1":   thread_data["tweets"][0] if thread_data["tweets"] else "",
        }
    }

    return result


if __name__ == "__main__":
    # Test with sample brief
    sample = {
        "date": datetime.now().strftime("%B %d, %Y"),
        "macro": {
            "regime": "CAUTIOUS",
            "regime_note": "Tariff signals detected — slight defensive tilt",
            "signals_detected": 4,
            "articles_read": 187,
            "active_signals": {
                "trump_tariff_canada_specific": {"confidence": 85, "note": "Canada tariff risk"},
                "oil_price_spike": {"confidence": 62, "note": "Oil up — energy stocks benefit"}
            },
            "sector_sentiment": {
                "CANADIAN_ENERGY": {"sentiment": "BULLISH", "net_score": 120},
                "TSX_EXPORTERS":   {"sentiment": "BEARISH", "net_score": -90}
            }
        },
        "conviction_picks": [{
            "ticker": "ENB.TO", "score": 91, "conviction_count": 5,
            "conviction_signals": ["📡 X Signal","💰 7.2% yield","📈 Trending up"],
            "reasons": ["Exceptional yield: 7.2%", "Oil price tailwind", "Ex-div in 14 days"],
            "data": {"price": 56.88, "day_chg_pct": 1.42, "div_yield": 7.2},
            "pick": {"category": "INCOME+GROWTH", "amount": 200, "exp_low": 9.2,
                     "exp_high": 18.4, "hold_days": 365, "exit_note": "Buy before Mar 10"}
        }],
        "calendar": [
            {"title": "💰 ENB.TO Ex-Dividend", "date": "Mar 14", "urgency": "urgent",
             "ticker": "ENB.TO", "desc": "Buy before Mar 10 for $4.10/share"},
        ],
        "screen_stats": {"screened": 412, "universe": 518}
    }

    result = run_content_engine(sample, verbose=True)
    with open("content_output.json", "w") as f:
        json.dump(result, f, indent=2)
    print(f"\n💾 Saved to content_output.json")
