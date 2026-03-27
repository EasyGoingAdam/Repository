"""
First Strike Command — Central Intelligence Aggregation Engine
Ingests signals from all apps, detects convergence, maintains state,
generates composite odds and recommendations.
"""
from __future__ import annotations
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from datetime import datetime, timezone, timedelta
from typing import Optional, List, Tuple
from .signals import (
    get_recent_signals, get_command_state, update_command_state,
    apply_confidence_decay, Signal, store_signal, MARKET_SLUG
)
import api as polyapi
import analyzer as analyzermod
import tracker


CONVERGENCE_THRESHOLD = 3    # signals from N different apps = convergence
HIGH_IMPORTANCE_THRESHOLD = 75
URGENT_IMPORTANCE_THRESHOLD = 85

# ── Desk credibility weights ───────────────────────────────────────────────────
# Pulse (X/Twitter) is the fastest signal — officials + analysts break news in
# real time. Beacon captures structured journalism. Historian context (encoded
# as older signals still in the window) adds narrative weight. Volatility
# trend provides a market-physics signal. Market and OrderFlow lag real events.
DESK_WEIGHTS = {
    "pulse":     0.40,   # X/Twitter: highest — real-time official + analyst signal
    "beacon":    0.35,   # News/GDELT: second — credibility-scored journalism
    "market":    0.10,   # Polymarket price/momentum: crowd-sourced, lags events
    "orderflow": 0.15,   # Order book depth: elevated — strong imbalances signal big moves
}

# How much volatility (price momentum) can shift the estimate.
# Positive z-score (above mean) = market already pricing in escalation.
# Negative z-score (below mean) = market underpricing risk → push estimate up.
VOLATILITY_WEIGHT = 0.04   # max ±4pp adjustment from volatility z-score


def run_command_cycle() -> dict:
    """
    Full command cycle:
    1. Pull recent signals from all apps
    2. Apply confidence decay
    3. Detect convergence
    4. Compute house odds
    5. Determine alert level + recommendation
    6. Update command state
    7. Return full state payload
    """
    # 1. Get market data
    market = polyapi.find_iran_boots_market()
    market_odds = market.yes_price if market else None

    # 2. Get signals — 48h window (historian depth) with 12h half-life decay
    # Using 48h lets older high-importance signals (historian context) still
    # influence the estimate, while recent signals dominate via decay.
    raw_signals = get_recent_signals(hours=48, limit=200)
    signals = apply_confidence_decay(raw_signals, half_life_hours=12)

    # 3. Separate by source app
    by_app = {"pulse": [], "beacon": [], "market": [], "orderflow": []}
    for s in signals:
        app = s.get("source_app", "unknown")
        if app in by_app:
            by_app[app].append(s)

    # 4. Convergence detection
    apps_with_signals = [app for app, sigs in by_app.items() if sigs]
    converging = len(apps_with_signals) >= CONVERGENCE_THRESHOLD

    # 5. Directional consensus
    bullish = [s for s in signals if s["signal_direction"] == "bullish"]
    bearish = [s for s in signals if s["signal_direction"] == "bearish"]
    neutral = [s for s in signals if s["signal_direction"] == "neutral"]

    total = len(signals) or 1
    bull_pct = len(bullish) / total
    bear_pct = len(bearish) / total

    # Weighted directional score: decayed_confidence × importance × desk_weight
    # Pulse (40%) and Beacon (35%) outweigh Market (15%) and OrderFlow (10%)
    def _signal_weight(s: dict) -> float:
        desk_mult = DESK_WEIGHTS.get(s.get("source_app", ""), 0.10)
        return s.get("decayed_confidence", 0) * s.get("importance_score", 0) * desk_mult

    bull_weight = sum(_signal_weight(s) for s in bullish)
    bear_weight = sum(_signal_weight(s) for s in bearish)
    total_weight = bull_weight + bear_weight + 1

    # 6. Compute house odds (START from market price, adjust based on signals)
    # Adjustment scales with signal quality: high-importance signals from
    # Pulse/Beacon move it more than market/orderflow noise.
    # Max ±20pp from strong convergent intelligence.
    if market_odds is not None and signals:
        raw_ratio = (bull_weight - bear_weight) / total_weight

        # Quality factor: how much of the DIRECTIONAL signal weight comes from
        # high-value desks (pulse + beacon) vs noise (market + orderflow)?
        directional = bullish + bearish
        pb_directional = [s for s in directional if s.get("source_app") in ("pulse", "beacon")]
        pb_weight = sum(_signal_weight(s) for s in pb_directional)
        total_dir_weight = bull_weight + bear_weight
        quality_factor = (pb_weight / total_dir_weight) if total_dir_weight > 0 else 0.3
        quality_factor = max(0.3, min(1.0, quality_factor))  # floor at 30%

        # Scale: ±20pp max, dampened by quality factor
        signal_adjustment = raw_ratio * 0.20 * quality_factor

        # Volatility component: incorporate price momentum (z-score from
        # stored volatility metrics or live computation).
        vol_adjustment = 0.0
        try:
            from intelligence.signals import get_all_latest_volatility
            all_vol = get_all_latest_volatility()
            # Find the Iran boots market volatility record
            iran_vol = next(
                (v for v in all_vol
                 if "iran" in str(v.get("market_id", "")).lower()
                 or v.get("window_minutes") == 1440),   # 24h window
                None
            )
            if iran_vol:
                z = iran_vol.get("z_score", 0)
                # Negative z = price below mean = market underpricing → push up
                # Positive z = price above mean = market overpricing → push down
                vol_adjustment = -z * VOLATILITY_WEIGHT / 3.0
                vol_adjustment = max(-VOLATILITY_WEIGHT, min(VOLATILITY_WEIGHT, vol_adjustment))
        except Exception:
            pass

        house_odds = max(0.02, min(0.97,
            market_odds + signal_adjustment + vol_adjustment))
    else:
        house_odds = market_odds

    odds_delta = round((house_odds - market_odds) * 100, 2) if (house_odds and market_odds) else 0

    # 7. Order book bias (from most recent orderflow signal or live fetch)
    orderbook_bias = _get_orderbook_bias(market, by_app["orderflow"])

    # 8. Alert level
    max_importance = max((s.get("importance_score", 0) for s in signals), default=0)
    alert_level, recommended_action = _determine_alert(
        signals, converging, max_importance, odds_delta, orderbook_bias
    )

    # 9. Top signals (highest importance * decayed_confidence)
    ranked = sorted(
        signals,
        key=lambda s: s.get("importance_score", 0) * s.get("decayed_confidence", 0),
        reverse=True
    )
    top_signals = ranked[:5]

    # 10. Reason summary
    reason_parts = []
    if converging:
        reason_parts.append(f"Signal convergence across {len(apps_with_signals)} apps")
    if bullish:
        reason_parts.append(f"{len(bullish)} bullish signals")
    if bearish:
        reason_parts.append(f"{len(bearish)} bearish signals")
    if abs(odds_delta) > 1:
        direction_word = "above" if odds_delta > 0 else "below"
        reason_parts.append(f"House odds {abs(odds_delta):.1f}pp {direction_word} market")
    reason_summary = ". ".join(reason_parts) if reason_parts else "No significant signals in past 24h"

    state = {
        "market_odds": round(market_odds * 100, 1) if market_odds else None,
        "house_odds": round(house_odds * 100, 1) if house_odds else None,
        "odds_delta": odds_delta,
        "alert_level": alert_level,
        "reason_summary": reason_summary,
        "orderbook_bias": orderbook_bias,
        "recommended_action": recommended_action,
        "top_signals": [_trim_signal(s) for s in top_signals],
        "signal_counts": {
            "bullish": len(bullish),
            "bearish": len(bearish),
            "neutral": len(neutral),
            "total": len(signals),
        },
        "apps_active": apps_with_signals,
        "convergence": converging,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }

    update_command_state(state)
    return state


def _get_orderbook_bias(market, orderflow_signals: list) -> str:
    """Get order book directional bias."""
    if market and market.clob_token_ids:
        try:
            depth = polyapi.get_orderbook_depth(market.clob_token_ids[0])
            imbalance = depth.get("imbalance", 0)
            if imbalance > 0.15:
                return "bullish"
            elif imbalance < -0.15:
                return "bearish"
        except Exception:
            pass

    # Fall back to orderflow signals
    if orderflow_signals:
        last = orderflow_signals[0]
        return last.get("signal_direction", "neutral")

    return "neutral"


def _determine_alert(signals: list, converging: bool, max_importance: int,
                     odds_delta: float, ob_bias: str) -> Tuple[str, str]:
    """Determine alert level and recommended action."""

    # Check for official confirmation
    has_official = any(
        s.get("confidence_score", 0) >= 85 and s.get("importance_score", 0) >= 80
        for s in signals
    )

    # Check for multi-app agreement
    multi_app = converging and len([s for s in signals if s.get("importance_score", 0) >= 60]) >= 3

    # Check for major price movement signal
    major_price_move = abs(odds_delta) >= 5

    # Check for strong order book shift — now a first-class alert trigger
    strong_ob = ob_bias in ("bullish", "bearish") and max_importance >= 70
    extreme_ob = any(
        s.get("source_app") == "orderflow" and s.get("importance_score", 0) >= 80
        for s in signals
    )

    if has_official or (multi_app and major_price_move):
        return "URGENT", "urgent"
    elif extreme_ob and (multi_app or major_price_move):
        return "URGENT", "urgent"  # extreme orderflow + other confirmation = urgent
    elif multi_app or (max_importance >= HIGH_IMPORTANCE_THRESHOLD and abs(odds_delta) >= 2):
        return "IMPORTANT", "edge"
    elif extreme_ob or (strong_ob and abs(odds_delta) >= 1):
        return "IMPORTANT", "edge"  # extreme orderflow alone = important
    elif len(signals) >= 3 or max_importance >= 60:
        return "DIGEST", "monitor"
    else:
        return "NO_ACTION", "no_action"


def _trim_signal(s: dict) -> dict:
    return {
        "id": s.get("id"),
        "source_app": s.get("source_app"),
        "headline": s.get("headline", "")[:100],
        "signal_direction": s.get("signal_direction"),
        "confidence_score": s.get("confidence_score"),
        "importance_score": s.get("importance_score"),
        "decayed_confidence": s.get("decayed_confidence"),
        "probability_impact_estimate": s.get("probability_impact_estimate"),
        "timestamp": s.get("timestamp"),
        "link": s.get("link", ""),
    }


_last_market_signal_price = None

def generate_market_signal(market) -> Optional[Signal]:
    """
    Generate a signal from the current market state.
    Only fires on actual price CHANGES (≥2pp from last signal), not static mismatches.
    """
    global _last_market_signal_price
    if not market:
        return None

    yes_price = market.yes_price
    prev_state = get_command_state()
    prev_odds = prev_state.get("market_odds")

    if prev_odds:
        price_change = yes_price * 100 - prev_odds
    else:
        price_change = 0

    # Only generate if price actually moved ≥2pp from the LAST SIGNAL we generated
    if _last_market_signal_price is not None:
        delta_from_last = abs(yes_price - _last_market_signal_price)
        if delta_from_last < 0.02:
            return None  # price hasn't moved enough since last signal

    if abs(price_change) >= 2:
        direction = "bullish" if price_change > 0 else "bearish"
        importance = min(95, int(abs(price_change) * 12))
        confidence = 85
        headline = f"Price moved {price_change:+.1f}pp to {yes_price:.1%} YES"
    elif abs(price_change) >= 1:
        # Smaller moves: lower importance
        direction = "bullish" if price_change > 0 else "bearish"
        importance = min(65, int(abs(price_change) * 20 + 30))
        confidence = 70
        headline = f"Price shift {price_change:+.1f}pp to {yes_price:.1%} YES"
    else:
        return None  # No meaningful price change

    _last_market_signal_price = yes_price

    return Signal(
        source_app="market",
        timestamp=datetime.now(timezone.utc).isoformat(),
        headline=headline,
        raw_text=pred.reasoning[:300],
        signal_direction=direction,
        confidence_score=confidence,
        importance_score=importance,
        probability_impact_estimate=round(price_change, 2),
        reasoning=f"Polymarket price signal. Prediction delta: {(pred.predicted_yes_prob - yes_price)*100:+.1f}pp",
        link=f"https://polymarket.com/event/us-forces-enter-iran-by",
        market_slug=MARKET_SLUG,
    )


def generate_orderflow_signal(market) -> Optional[Signal]:
    """
    Generate a signal from current order book state.
    Lower threshold (0.05) catches early shifts.
    Extreme imbalance (>0.25) generates high-importance alerts.
    """
    if not market or not market.clob_token_ids:
        return None
    try:
        depth = polyapi.get_orderbook_depth(market.clob_token_ids[0])
        if not depth:
            return None
        imbalance = depth.get("imbalance", 0)
        bid_notional = depth.get("total_bid_notional", 0)
        ask_notional = depth.get("total_ask_notional", 0)
        spread = depth.get("spread", 0.02)

        if abs(imbalance) < 0.05:
            return None  # too balanced to be interesting

        direction = "bullish" if imbalance > 0 else "bearish"
        strength = abs(imbalance)

        # Tiered importance: extreme imbalance gets high-priority alert
        if strength >= 0.40:
            importance = 92
            confidence = 88
        elif strength >= 0.25:
            importance = 80
            confidence = 82
        elif strength >= 0.15:
            importance = 65
            confidence = 70
        else:
            importance = 45
            confidence = 55

        # Descriptive headlines by tier
        if imbalance > 0.40:
            headline = f"MASSIVE buy pressure: ${bid_notional:,.0f} bids vs ${ask_notional:,.0f} asks (imb {imbalance:+.2f})"
        elif imbalance > 0.25:
            headline = f"Strong buy pressure: ${bid_notional:,.0f} bids vs ${ask_notional:,.0f} asks"
        elif imbalance < -0.40:
            headline = f"MASSIVE sell wall: ${ask_notional:,.0f} asks vs ${bid_notional:,.0f} bids (imb {imbalance:+.2f})"
        elif imbalance < -0.25:
            headline = f"Heavy sell pressure: ${ask_notional:,.0f} asks vs ${bid_notional:,.0f} bids"
        elif imbalance > 0:
            headline = f"Buy-side leaning: imbalance {imbalance:+.3f} (${bid_notional:,.0f} bids)"
        else:
            headline = f"Sell-side leaning: imbalance {imbalance:+.3f} (${ask_notional:,.0f} asks)"

        return Signal(
            source_app="orderflow",
            timestamp=datetime.now(timezone.utc).isoformat(),
            headline=headline,
            raw_text=f"Bid: ${bid_notional:,.0f} | Ask: ${ask_notional:,.0f} | Spread: {spread*100:.2f}¢ | Imbalance: {imbalance:+.3f}",
            signal_direction=direction,
            confidence_score=confidence,
            importance_score=importance,
            probability_impact_estimate=round(imbalance * 3, 2),
            reasoning=f"CLOB order book. Spread: {spread*100:.2f}¢. Imbalance: {imbalance:+.3f}. Strength tier: {'EXTREME' if strength >= 0.40 else 'STRONG' if strength >= 0.25 else 'MODERATE' if strength >= 0.15 else 'MILD'}",
            link="https://polymarket.com/event/us-forces-enter-iran-by",
            market_slug=MARKET_SLUG,
        )
    except Exception:
        return None
