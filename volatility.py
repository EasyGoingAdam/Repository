"""
Volatility Analyzer — compute volatility metrics and detect trading opportunities.

Uses price_snapshots data to compute standard deviation, Bollinger bands,
Z-scores, and mean reversion signals for each watched market.
"""
import statistics
from datetime import datetime, timezone
from intelligence.signals import (
    get_price_snapshots,
    get_managed_watchlist,
    store_volatility_metric,
    get_latest_volatility,
    get_all_latest_volatility,
    get_latest_price_snapshot,
)


def compute_volatility(market_id: str, window_minutes: int = 360) -> dict:
    """
    Compute volatility metrics for a market over the given time window.
    Requires at least 10 data points. Returns metric dict or None.
    """
    hours = window_minutes / 60
    snapshots = get_price_snapshots(market_id, hours=max(int(hours) + 1, 24))

    prices = [s["yes_price"] for s in snapshots if s.get("yes_price") is not None]

    if len(prices) < 5:
        return None

    current_price = prices[-1]
    mean_price = statistics.mean(prices)
    std_dev = statistics.stdev(prices) if len(prices) > 1 else 0.0

    bollinger_upper = mean_price + 2 * std_dev
    bollinger_lower = mean_price - 2 * std_dev

    z_score = (current_price - mean_price) / std_dev if std_dev > 0.001 else 0.0

    # Mean reversion signal: negative z = buy opportunity, positive z = sell opportunity
    mean_reversion_signal = max(-1.0, min(1.0, -z_score))

    metric = {
        "market_id": market_id,
        "window_minutes": window_minutes,
        "mean_price": round(mean_price, 6),
        "std_dev": round(std_dev, 6),
        "bollinger_upper": round(bollinger_upper, 6),
        "bollinger_lower": round(bollinger_lower, 6),
        "z_score": round(z_score, 4),
        "current_price": round(current_price, 6),
        "price_range_high": round(max(prices), 6),
        "price_range_low": round(min(prices), 6),
        "mean_reversion_signal": round(mean_reversion_signal, 4),
    }

    store_volatility_metric(metric)
    return metric


def compute_all_volatility() -> dict:
    """
    Compute volatility for all active markets at three time windows.
    Returns summary.
    """
    watchlist = get_managed_watchlist()
    computed = 0
    skipped = 0

    windows = [60, 480, 1440, 10080]  # 1h, 8h, 24h, 7d

    for entry in watchlist:
        for window in windows:
            result = compute_volatility(entry["market_id"], window)
            if result:
                computed += 1
            else:
                skipped += 1

    return {"computed": computed, "skipped": skipped, "markets": len(watchlist)}


def detect_opportunities(threshold_z: float = 1.5) -> list[dict]:
    """
    Find markets where price has deviated significantly from moving average.
    Z-score > threshold = sell opportunity (overpriced)
    Z-score < -threshold = buy opportunity (underpriced)
    """
    all_vol = get_all_latest_volatility()
    watchlist = {w["market_id"]: w for w in get_managed_watchlist()}

    opportunities = []
    for vol in all_vol:
        z = vol.get("z_score", 0)
        if abs(z) < threshold_z:
            continue

        market_info = watchlist.get(vol["market_id"], {})
        direction = "BUY" if z < 0 else "SELL"
        strength = min(abs(z) / 3.0, 1.0)  # normalize to 0-1

        opportunities.append({
            "market_id": vol["market_id"],
            "question": market_info.get("question", "Unknown"),
            "direction": direction,
            "z_score": z,
            "strength": round(strength, 3),
            "current_price": vol.get("current_price"),
            "mean_price": vol.get("mean_price"),
            "bollinger_upper": vol.get("bollinger_upper"),
            "bollinger_lower": vol.get("bollinger_lower"),
            "std_dev": vol.get("std_dev"),
            "window_minutes": vol.get("window_minutes"),
        })

    opportunities.sort(key=lambda x: abs(x["z_score"]), reverse=True)
    return opportunities


def get_market_volatility_summary(market_id: str) -> dict:
    """
    Get a complete volatility summary for a market across all time windows.
    """
    metrics = get_latest_volatility(market_id)
    if not metrics:
        return None

    latest_snap = get_latest_price_snapshot(market_id)
    current_price = latest_snap["yes_price"] if latest_snap else None

    summary = {
        "market_id": market_id,
        "current_price": current_price,
        "windows": {},
    }

    for m in metrics:
        window = m["window_minutes"]
        z = m.get("z_score", 0)

        # Determine Bollinger position
        if current_price is not None and m.get("bollinger_upper") and m.get("bollinger_lower"):
            band_width = m["bollinger_upper"] - m["bollinger_lower"]
            if band_width > 0:
                position = (current_price - m["bollinger_lower"]) / band_width
            else:
                position = 0.5
        else:
            position = 0.5

        summary["windows"][window] = {
            "mean_price": m.get("mean_price"),
            "std_dev": m.get("std_dev"),
            "bollinger_upper": m.get("bollinger_upper"),
            "bollinger_lower": m.get("bollinger_lower"),
            "z_score": z,
            "bollinger_position": round(position, 3),
            "mean_reversion_signal": m.get("mean_reversion_signal"),
            "signal_label": _z_score_label(z),
        }

    return summary


def _z_score_label(z: float) -> str:
    if z > 2.0:
        return "STRONG SELL (very overpriced)"
    elif z > 1.5:
        return "SELL (overpriced)"
    elif z > 0.5:
        return "slight sell pressure"
    elif z < -2.0:
        return "STRONG BUY (very underpriced)"
    elif z < -1.5:
        return "BUY (underpriced)"
    elif z < -0.5:
        return "slight buy pressure"
    else:
        return "neutral (fair value)"
