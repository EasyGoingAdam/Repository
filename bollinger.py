"""Bollinger band computation and mean-reversion signal detection."""
from __future__ import annotations

import statistics
from typing import Optional, List, Dict
from config import BOLLINGER_PERIOD, BOLLINGER_STD_MULTIPLIER


def compute_bollinger(snapshots: List[Dict]) -> Optional[Dict]:
    """Compute Bollinger bands from the most recent BOLLINGER_PERIOD snapshots.
    Returns dict with sma, upper, lower, std, z_score, or None if insufficient data."""
    if len(snapshots) < BOLLINGER_PERIOD:
        return None

    prices = [s["yes_price"] for s in snapshots[-BOLLINGER_PERIOD:]]
    current = prices[-1]
    sma = statistics.mean(prices)
    std = statistics.pstdev(prices)

    upper = sma + BOLLINGER_STD_MULTIPLIER * std
    lower = max(0.0, sma - BOLLINGER_STD_MULTIPLIER * std)
    z_score = (current - sma) / std if std > 0.001 else 0.0

    return {
        "sma": round(sma, 6),
        "upper": round(upper, 6),
        "lower": round(lower, 6),
        "std": round(std, 6),
        "z_score": round(z_score, 4),
        "current_price": round(current, 6),
    }


def detect_signal(current_price: float, bands: Dict, imbalance: float = 0.0) -> Optional[Dict]:
    """Detect buy/sell signal based on mean reversion.
    BUY when price < lower band, SELL when price > upper band."""
    from datetime import datetime, timezone

    z = bands["z_score"]
    strength = min(1.0, abs(z) / 3.0)

    now = datetime.now(timezone.utc)
    base = {
        "timestamp": now.isoformat(),
        "unix_ts": now.timestamp(),
        "price_at_signal": current_price,
        "bollinger_upper": bands["upper"],
        "bollinger_lower": bands["lower"],
        "bollinger_mean": bands["sma"],
        "z_score": z,
        "strength": round(strength, 4),
        "imbalance": imbalance,
    }

    if current_price < bands["lower"]:
        return {**base, "signal_type": "BUY"}
    elif current_price > bands["upper"]:
        return {**base, "signal_type": "SELL"}
    return None


def compute_band_series(snapshots: List[Dict]) -> List[Dict]:
    """Compute rolling Bollinger bands across all snapshots for chart overlay."""
    if len(snapshots) < BOLLINGER_PERIOD:
        return []

    series = []
    for i in range(BOLLINGER_PERIOD - 1, len(snapshots)):
        try:
            window = [s["yes_price"] for s in snapshots[i - BOLLINGER_PERIOD + 1: i + 1]]
            if not window:
                continue
            sma = statistics.mean(window)
            std = statistics.pstdev(window) if len(window) > 1 else 0.0
            series.append({
                "t": snapshots[i]["unix_ts"],
                "p": snapshots[i]["yes_price"],
                "sma": round(sma, 6),
                "upper": round(sma + BOLLINGER_STD_MULTIPLIER * std, 6),
                "lower": round(max(0.0, sma - BOLLINGER_STD_MULTIPLIER * std), 6),
            })
        except (KeyError, TypeError, ZeroDivisionError):
            continue
    return series
