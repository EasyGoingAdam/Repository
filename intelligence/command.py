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

    # 2. Get recent signals (24h) with decay
    raw_signals = get_recent_signals(hours=24, limit=100)
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

    # Weighted directional score: sum of (decayed_confidence * importance)
    bull_weight = sum(s.get("decayed_confidence", 0) * s.get("importance_score", 0) for s in bullish)
    bear_weight = sum(s.get("decayed_confidence", 0) * s.get("importance_score", 0) for s in bearish)
    total_weight = bull_weight + bear_weight + 1

    # 6. Compute house odds (blend market price with signal direction)
    if market_odds is not None and signals:
        signal_adjustment = (bull_weight - bear_weight) / total_weight * 0.08
        house_odds = max(0.02, min(0.97, market_odds + signal_adjustment))
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

    # Check for strong order book shift
    strong_ob = ob_bias in ("bullish", "bearish") and max_importance >= 70

    if has_official or (multi_app and major_price_move):
        return "URGENT", "urgent"
    elif multi_app or (max_importance >= HIGH_IMPORTANCE_THRESHOLD and abs(odds_delta) >= 2):
        return "IMPORTANT", "edge"
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


def generate_market_signal(market) -> Optional[Signal]:
    """Generate a structured signal from the current market state."""
    if not market:
        return None

    weights = tracker.load_weights()
    pred = analyzermod.analyze_market(market, weights)

    yes_price = market.yes_price
    prev_state = get_command_state()
    prev_odds = prev_state.get("market_odds")

    if prev_odds:
        price_change = yes_price * 100 - prev_odds
    else:
        price_change = 0

    if abs(price_change) >= 2:
        direction = "bullish" if price_change > 0 else "bearish"
        importance = min(95, int(abs(price_change) * 15))
        confidence = 85
        headline = f"Price moved {price_change:+.1f}pp to {yes_price:.1%} YES"
    elif pred.predicted_yes_prob > yes_price + 0.03:
        direction = "bullish"
        importance = 55
        confidence = int(pred.confidence * 100)
        headline = f"Model above market: {pred.predicted_yes_prob:.1%} vs {yes_price:.1%}"
    elif pred.predicted_yes_prob < yes_price - 0.03:
        direction = "bearish"
        importance = 55
        confidence = int(pred.confidence * 100)
        headline = f"Model below market: {pred.predicted_yes_prob:.1%} vs {yes_price:.1%}"
    else:
        return None  # No interesting signal

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
    """Generate a signal from current order book state."""
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

        if abs(imbalance) < 0.10:
            return None

        direction = "bullish" if imbalance > 0 else "bearish"
        strength = abs(imbalance)
        importance = min(90, int(strength * 100))
        confidence = min(88, int(50 + strength * 60))

        if imbalance > 0.3:
            headline = f"Strong buy pressure: ${bid_notional:,.0f} bids vs ${ask_notional:,.0f} asks"
        elif imbalance < -0.3:
            headline = f"Sell wall forming: ${ask_notional:,.0f} asks vs ${bid_notional:,.0f} bids"
        elif imbalance > 0:
            headline = f"Mild buy pressure: imbalance {imbalance:+.3f}"
        else:
            headline = f"Mild sell pressure: imbalance {imbalance:+.3f}"

        return Signal(
            source_app="orderflow",
            timestamp=datetime.now(timezone.utc).isoformat(),
            headline=headline,
            raw_text=f"Bid: ${bid_notional:,.0f} | Ask: ${ask_notional:,.0f} | Spread: {spread*100:.2f}¢ | Imbalance: {imbalance:+.3f}",
            signal_direction=direction,
            confidence_score=confidence,
            importance_score=importance,
            probability_impact_estimate=round(imbalance * 3, 2),
            reasoning=f"CLOB order book analysis. Spread: {spread*100:.2f}¢. Net imbalance: {imbalance:+.3f}",
            link="https://polymarket.com/event/us-forces-enter-iran-by",
            market_slug=MARKET_SLUG,
        )
    except Exception:
        return None
