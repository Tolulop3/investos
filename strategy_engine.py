"""
InvestOS Strategy Engine
Regime-aware dynamic factor weights for stock_screener.py

Architecture:
  Regime Detection → Strategy Engine → Dynamic Weights → Score Pillars → Final Score

The score tier inversion (90-100 = 49.5% WR vs 60-74 = 64.7% WR) is caused
by momentum being rewarded linearly regardless of regime. This engine fixes that
by adjusting pillar weights AND applying a momentum percentile curve that penalises
blow-off moves (top 5% momentum = crowded, high mean-reversion risk).
"""

# ── STRATEGY PROFILES ─────────────────────────────────────────────────────────
# Each profile defines pillar weights (must sum to 1.0 across scored pillars)
# and behavioural rules for the screener.
#
# Pillars in stock_screener.py:
#   momentum (35pts max), dividend_income (15pts), growth (15pts),
#   value (12pts), safety (13pts), volume_liquidity (10pts)
#
# Weight keys here map to screener pillar names.
# A weight of 0.0 means that pillar is effectively zeroed (multiplied by 0).

STRATEGY_PROFILES = {

    "RISK_ON": {
        "description": "Momentum + growth. Strong breadth, macro supportive.",
        "target":      "High RS, strong momentum, earnings growth, small/mid outperformance",
        "weights": {
            "momentum":         1.00,   # full momentum
            "growth":           1.00,   # full earnings growth
            "dividend_income":  0.60,   # dividend matters less when chasing growth
            "value":            0.80,   # value still relevant but not primary
            "safety":           0.70,   # accept more volatility
            "volume_liquidity": 1.00,
        },
        "momentum_curve": "bell_soft",  # soft bell: rewards momentum, discounts extreme percentile
        "extension_penalty": True,      # still penalise top-5% blow-off
        "pullback_bonus":   True,       # reward pullback-from-high setups
        "min_score_threshold": 60,
        "stock_screening_enabled": True,
    },

    "CAUTIOUS": {
        "description": "Income + quality. Trend intact, breadth weakening.",
        "target":      "Dividend growers, lower beta, free cash flow, pricing power",
        "weights": {
            "momentum":         0.50,   # half momentum weight — don't chase strength
            "growth":           0.70,   # earnings still matter
            "dividend_income":  1.50,   # overweight dividend — income is a floor
            "value":            1.20,   # value matters more when growth slows
            "safety":           1.30,   # quality/safety premium
            "volume_liquidity": 1.00,
        },
        "momentum_curve": "bell",       # penalise extreme momentum (crowded)
        "extension_penalty": True,
        "pullback_bonus":   True,
        "min_score_threshold": 60,
        "stock_screening_enabled": True,
    },

    "DEFENSIVE": {
        "description": "Quality compounders. Economic uncertainty, trend deteriorating.",
        "target":      "High ROE, low debt, consistent earnings, balance sheet strength",
        "weights": {
            "momentum":         0.20,   # momentum almost irrelevant — survive first
            "growth":           0.80,   # steady earnings > explosive growth
            "dividend_income":  1.30,   # income with capital preservation
            "value":            1.40,   # value/quality premium at its highest
            "safety":           1.80,   # safety is PRIMARY pillar
            "volume_liquidity": 1.00,
        },
        "momentum_curve": "inverse",    # penalise high momentum (mean reversion risk)
        "extension_penalty": True,
        "pullback_bonus":   False,      # don't catch falling knives
        "min_score_threshold": 65,      # higher bar — fewer, better picks
        "stock_screening_enabled": True,
    },

    "CAPITAL_PRESERVATION": {
        "description": "Cash + bonds + gold. Highest risk regime.",
        "target":      "ZAG.TO, ZLB.TO, GLD. Do not own stocks.",
        "weights": {
            "momentum":         0.0,
            "growth":           0.0,
            "dividend_income":  0.0,
            "value":            0.0,
            "safety":           0.0,
            "volume_liquidity": 0.0,
        },
        "momentum_curve": "none",
        "extension_penalty": False,
        "pullback_bonus":   False,
        "min_score_threshold": 999,     # effectively blocks all stock picks
        "stock_screening_enabled": False,
        "recommended_etfs": ["ZAG.TO", "ZLB.TO", "GLD"],
    },
}

# ── REGIME → STRATEGY MAPPING ─────────────────────────────────────────────────
# Maps the unified regime output to strategy profiles.
# The regime engine already outputs these values.

REGIME_TO_STRATEGY = {
    "RISK_ON":               "RISK_ON",
    "NEUTRAL":               "CAUTIOUS",
    "CAUTIOUS":              "CAUTIOUS",
    "DEFENSIVE":             "DEFENSIVE",
    "CAPITAL_PRESERVATION":  "CAPITAL_PRESERVATION",
    # breadth-adjusted overrides
    "BROAD_BULL":            "RISK_ON",
    "BROAD_BEAR":            "DEFENSIVE",
}

# ── MOMENTUM CURVE FUNCTIONS ──────────────────────────────────────────────────

def apply_momentum_curve(momentum_raw, momentum_percentile, curve_type):
    """
    Adjust momentum score based on its position in the universe distribution.

    linear     (legacy): no adjustment — reward momentum as-is
    bell_soft  (RISK_ON): light bell — rewards 70-90th pct, discounts 90th+ (blow-off)
    bell       (CAUTIOUS): stronger bell — taper above 85th percentile
    inverse    (DEFENSIVE): penalise any high-momentum reading
    """
    if curve_type == "linear" or momentum_percentile is None:
        return momentum_raw

    if curve_type == "bell":
        # Peak at 70-85th pct. Above 90th = crowded, penalise.
        if momentum_percentile > 95:
            return round(momentum_raw * 0.70, 1)
        elif momentum_percentile > 90:
            return round(momentum_raw * 0.85, 1)
        elif momentum_percentile > 85:
            return round(momentum_raw * 0.95, 1)
        elif momentum_percentile >= 70:
            return round(momentum_raw * 1.05, 1)
        else:
            return momentum_raw

    if curve_type == "bell_soft":
        # Lighter version for RISK_ON — still rewards genuine momentum,
        # but discounts extreme-percentile names (BB.TO RS-100, BX velocity spikes)
        # where score inflation concentrates without ML confirmation.
        # Validated: ~5pt discount at 35pt max pillar — real but not overcorrecting.
        if momentum_percentile > 95:
            # Top 5%: blow-off territory in a bull market. Discount without blocking.
            return round(momentum_raw * 0.85, 1)
        elif momentum_percentile > 90:
            # 90-95th pct: light penalty for crowding
            return round(momentum_raw * 0.92, 1)
        elif momentum_percentile >= 70:
            # Sweet spot: small reward for genuine momentum leadership
            return round(momentum_raw * 1.03, 1)
        else:
            return momentum_raw

    if curve_type == "inverse":
        # Defensive: high momentum = likely extended = avoid
        if momentum_percentile > 80:
            return round(momentum_raw * 0.60, 1)
        elif momentum_percentile > 60:
            return round(momentum_raw * 0.80, 1)
        else:
            return momentum_raw

    return momentum_raw


# ── MAIN INTERFACE ─────────────────────────────────────────────────────────────

def get_strategy(unified_regime, breadth_signal=None, macro_regime=None):
    """
    Returns the active strategy profile dict for this regime combination.

    Args:
        unified_regime:  string from risk_engine (RISK_ON/NEUTRAL/DEFENSIVE/etc.)
        breadth_signal:  optional string (BROAD_BULL/BROAD_BEAR/etc.)
        macro_regime:    optional string from news_analyzer (RISK_OFF/CAUTIOUS/etc.)

    Returns:
        (strategy_name, profile_dict)
    """
    # Breadth overrides take priority for stock selection
    if breadth_signal in REGIME_TO_STRATEGY:
        strategy_name = REGIME_TO_STRATEGY[breadth_signal]
    else:
        strategy_name = REGIME_TO_STRATEGY.get(unified_regime, "CAUTIOUS")

    # Macro regime can push toward more defensive
    if macro_regime in ("RISK_OFF", "BEAR") and strategy_name == "RISK_ON":
        strategy_name = "CAUTIOUS"

    return strategy_name, STRATEGY_PROFILES[strategy_name]


def apply_strategy_weights(pillars, strategy_profile, momentum_percentile=None):
    """
    Applies dynamic factor weights to raw pillar scores.

    Args:
        pillars:              dict of {pillar_name: raw_score}
        strategy_profile:     profile dict from STRATEGY_PROFILES
        momentum_percentile:  0-100 position of this stock's momentum in universe

    Returns:
        dict of {pillar_name: weighted_score}
    """
    weights   = strategy_profile["weights"]
    curve     = strategy_profile.get("momentum_curve", "linear")
    weighted  = {}

    for pillar, raw in pillars.items():
        w = weights.get(pillar, 1.0)
        if pillar == "momentum":
            # Apply momentum curve before weight
            adjusted = apply_momentum_curve(raw, momentum_percentile, curve)
            weighted[pillar] = round(adjusted * w, 1)
        else:
            weighted[pillar] = round(raw * w, 1)

    return weighted


def get_strategy_label(strategy_name, regime):
    """Short label for dashboard display."""
    labels = {
        "RISK_ON":              "🚀 GROWTH MODE",
        "CAUTIOUS":             "⚖️  QUALITY TILT",
        "DEFENSIVE":            "🛡️  DEFENSIVE",
        "CAPITAL_PRESERVATION": "💵 CAPITAL PRESERVATION",
    }
    return labels.get(strategy_name, strategy_name)


def log_strategy(strategy_name, profile, verbose=True):
    if not verbose:
        return
    curve = profile.get("momentum_curve", "linear")
    print(f"   📊 Strategy: {strategy_name} — {profile['description']}")
    if curve != "linear":
        print(f"   📐 Momentum curve: {curve} — top-percentile names discounted")
    if not profile.get("stock_screening_enabled", True):
        etfs = profile.get("recommended_etfs", [])
        print(f"   💵 Stock screening DISABLED — route to: {', '.join(etfs)}")
