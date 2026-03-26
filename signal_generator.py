"""
Trading Signal Generator — combine volatility, momentum, and prediction signals
to generate buy/sell recommendations. Track signal performance over time.
"""
import hashlib
from datetime import datetime, timezone, timedelta
import api as polyapi
import analyzer as analyzermod
import tracker
from volatility import compute_volatility
from intelligence.signals import (
    get_managed_watchlist,
    store_trading_signal,
    get_unevaluated_signals,
    update_trading_signal_evaluation,
    get_evaluated_trading_signals,
    get_price_snapshots,
    get_latest_price_snapshot,
)


# Component weights for composite signal
SIGNAL_WEIGHTS = {
    "volatility": 0.35,
    "momentum": 0.25,
    "volume": 0.15,
    "prediction": 0.25,
}

# Minimum composite strength to generate a signal
SIGNAL_THRESHOLD = 0.3


def generate_trading_signals() -> list[dict]:
    """
    Generate trading signals for all watched markets.
    Combines volatility, momentum, volume, and prediction components.
    Returns list of generated signals.
    """
    watchlist = get_managed_watchlist()
    weights = tracker.load_weights()
    generated = []

    for entry in watchlist:
        try:
            market = polyapi.get_market_by_id(entry["market_id"])
            if not market or not market.active or market.closed:
                continue

            signal = _generate_signal_for_market(market, weights)
            if signal:
                store_trading_signal(signal)
                generated.append(signal)
        except Exception:
            continue

    return generated


def _generate_signal_for_market(market, weights: dict) -> dict:
    """Generate a trading signal for a single market."""

    # 1. Volatility component (mean reversion)
    vol_metric = compute_volatility(market.id, window_minutes=360)
    vol_component = 0.0
    vol_z = 0.0
    if vol_metric:
        vol_z = vol_metric.get("z_score", 0)
        # Negative z = underpriced = buy signal (positive component)
        # Positive z = overpriced = sell signal (negative component)
        vol_component = vol_metric.get("mean_reversion_signal", 0)

    # 2. Momentum component
    momentum_component = 0.0
    if market.clob_token_ids:
        history = polyapi.get_price_history(market.clob_token_ids[0], interval="1w", fidelity=60)
        if len(history) >= 2:
            prices = [float(p.get("p", 0)) for p in history if "p" in p]
            if len(prices) >= 2:
                mid = len(prices) // 2
                recent_avg = sum(prices[mid:]) / len(prices[mid:])
                earlier_avg = sum(prices[:mid]) / len(prices[:mid])
                if earlier_avg > 0:
                    change = (recent_avg - earlier_avg) / earlier_avg
                    momentum_component = max(-1.0, min(1.0, change * 10))

    # 3. Volume component (volume spike detection)
    volume_component = 0.0
    snapshots = get_price_snapshots(market.id, hours=48)
    if len(snapshots) >= 10:
        volumes = [s.get("volume_24hr", 0) or 0 for s in snapshots if s.get("volume_24hr")]
        if volumes:
            avg_vol = sum(volumes) / len(volumes)
            current_vol = market.volume_24hr
            if avg_vol > 0:
                vol_ratio = current_vol / avg_vol
                # High volume confirms the signal direction
                volume_component = max(-1.0, min(1.0, (vol_ratio - 1.0)))

    # 4. Prediction component (model vs market)
    prediction_component = 0.0
    try:
        pred = analyzermod.analyze_market(market, weights)
        edge = pred.predicted_yes_prob - market.yes_price
        prediction_component = max(-1.0, min(1.0, edge * 10))
    except Exception:
        pass

    # Composite signal
    composite = (
        SIGNAL_WEIGHTS["volatility"] * vol_component +
        SIGNAL_WEIGHTS["momentum"] * momentum_component +
        SIGNAL_WEIGHTS["volume"] * volume_component +
        SIGNAL_WEIGHTS["prediction"] * prediction_component
    )

    if abs(composite) < SIGNAL_THRESHOLD:
        return None

    signal_type = "buy" if composite > 0 else "sell"
    strength = min(abs(composite), 1.0)

    now = datetime.now(timezone.utc)
    sig_id = hashlib.sha256(
        f"{market.id}:{now.isoformat()[:16]}".encode()
    ).hexdigest()[:16]

    reasoning_parts = []
    if abs(vol_component) > 0.1:
        reasoning_parts.append(f"volatility z={vol_z:+.2f} ({vol_component:+.2f})")
    if abs(momentum_component) > 0.1:
        reasoning_parts.append(f"momentum={momentum_component:+.2f}")
    if abs(volume_component) > 0.1:
        reasoning_parts.append(f"volume_spike={volume_component:+.2f}")
    if abs(prediction_component) > 0.1:
        reasoning_parts.append(f"prediction_edge={prediction_component:+.2f}")

    return {
        "id": sig_id,
        "market_id": market.id,
        "market_question": market.question,
        "timestamp": now.isoformat(),
        "unix_ts": now.timestamp(),
        "signal_type": signal_type,
        "strength": round(strength, 4),
        "price_at_signal": market.yes_price,
        "reasoning": f"{signal_type.upper()} (strength={strength:.2f}): {'; '.join(reasoning_parts)}",
        "volatility_z_score": vol_z,
        "mean_reversion_component": round(vol_component, 4),
        "momentum_component": round(momentum_component, 4),
        "volume_component": round(volume_component, 4),
        "prediction_component": round(prediction_component, 4),
    }


def evaluate_past_signals() -> dict:
    """
    Evaluate past trading signals by checking actual price movements.
    Computes profit/loss for each signal.
    """
    unevaluated = get_unevaluated_signals(min_age_hours=1.0)
    evaluated_count = 0

    for sig in unevaluated:
        market_id = sig["market_id"]
        signal_ts = sig["unix_ts"]
        price_at_signal = sig["price_at_signal"]

        # Look up prices at various intervals after the signal
        snapshots = get_price_snapshots(market_id, hours=48)
        if not snapshots:
            continue

        price_1h = _find_price_at_offset(snapshots, signal_ts, 3600)
        price_6h = _find_price_at_offset(snapshots, signal_ts, 21600)
        price_24h = _find_price_at_offset(snapshots, signal_ts, 86400)

        # Use the latest available price for P&L
        eval_price = price_24h or price_6h or price_1h
        if eval_price is None:
            # Use latest snapshot
            latest = get_latest_price_snapshot(market_id)
            if latest:
                eval_price = latest["yes_price"]

        if eval_price is None:
            continue

        # Calculate profit/loss
        if sig["signal_type"] == "buy":
            pl = eval_price - price_at_signal
        else:  # sell
            pl = price_at_signal - eval_price

        update_trading_signal_evaluation(sig["id"], {
            "price_after_1h": price_1h,
            "price_after_6h": price_6h,
            "price_after_24h": price_24h,
            "profit_loss": round(pl, 6),
        })
        evaluated_count += 1

    return {"evaluated": evaluated_count, "pending": len(unevaluated) - evaluated_count}


def _find_price_at_offset(snapshots: list, signal_ts: float, offset_seconds: int):
    """Find the closest price snapshot to signal_ts + offset_seconds."""
    target_ts = signal_ts + offset_seconds
    best = None
    best_diff = float("inf")

    for s in snapshots:
        diff = abs(s["unix_ts"] - target_ts)
        # Must be after the target time and within 30 minutes tolerance
        if s["unix_ts"] >= target_ts - 60 and diff < best_diff and diff < 1800:
            best = s["yes_price"]
            best_diff = diff

    return best


def get_signal_performance_report() -> dict:
    """
    Compute overall signal performance statistics.
    """
    evaluated = get_evaluated_trading_signals()

    if not evaluated:
        return {
            "total_signals": 0,
            "evaluated": 0,
            "win_rate": 0,
            "avg_profit": 0,
            "total_profit": 0,
            "by_type": {},
            "by_market": {},
        }

    wins = 0
    profits = []
    by_type = {"buy": {"count": 0, "wins": 0, "total_pl": 0},
               "sell": {"count": 0, "wins": 0, "total_pl": 0}}
    by_market = {}

    for sig in evaluated:
        pl = sig.get("profit_loss", 0) or 0
        profits.append(pl)
        is_win = pl > 0

        if is_win:
            wins += 1

        stype = sig["signal_type"]
        if stype in by_type:
            by_type[stype]["count"] += 1
            by_type[stype]["total_pl"] += pl
            if is_win:
                by_type[stype]["wins"] += 1

        mid = sig["market_id"]
        if mid not in by_market:
            by_market[mid] = {
                "question": sig.get("market_question", ""),
                "count": 0, "wins": 0, "total_pl": 0,
            }
        by_market[mid]["count"] += 1
        by_market[mid]["total_pl"] += pl
        if is_win:
            by_market[mid]["wins"] += 1

    total = len(evaluated)
    avg_profit = sum(profits) / total if total else 0

    # Compute win rates
    for t in by_type.values():
        t["win_rate"] = round(t["wins"] / t["count"], 4) if t["count"] else 0
        t["avg_pl"] = round(t["total_pl"] / t["count"], 6) if t["count"] else 0
    for m in by_market.values():
        m["win_rate"] = round(m["wins"] / m["count"], 4) if m["count"] else 0
        m["avg_pl"] = round(m["total_pl"] / m["count"], 6) if m["count"] else 0

    return {
        "total_signals": total,
        "evaluated": total,
        "win_rate": round(wins / total, 4) if total else 0,
        "avg_profit": round(avg_profit, 6),
        "total_profit": round(sum(profits), 6),
        "by_type": by_type,
        "by_market": by_market,
    }
