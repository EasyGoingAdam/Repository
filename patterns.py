"""Pattern recognition — support/resistance, regime detection, ADX, Hurst."""
from __future__ import annotations

import math
import statistics
from typing import Optional, List, Dict

from config import (
    ADX_PERIOD, HURST_WINDOW, REGIME_TRENDING_ADX,
    SUPPORT_RESISTANCE_TOLERANCE, SUPPORT_RESISTANCE_MIN_TOUCHES,
)


def find_support_resistance(prices: List[float],
                            tolerance: float = SUPPORT_RESISTANCE_TOLERANCE,
                            min_touches: int = SUPPORT_RESISTANCE_MIN_TOUCHES) -> Dict:
    """Identify support and resistance levels from reversal points."""
    if len(prices) < 5:
        return {"support": [], "resistance": []}

    # Find local minima and maxima
    mins = []
    maxs = []
    for i in range(2, len(prices) - 2):
        if prices[i] <= prices[i - 1] and prices[i] <= prices[i - 2] and \
           prices[i] <= prices[i + 1] and prices[i] <= prices[i + 2]:
            mins.append(prices[i])
        if prices[i] >= prices[i - 1] and prices[i] >= prices[i - 2] and \
           prices[i] >= prices[i + 1] and prices[i] >= prices[i + 2]:
            maxs.append(prices[i])

    def cluster(levels: List[float]) -> List[float]:
        if not levels:
            return []
        levels.sort()
        clusters = []
        current = [levels[0]]
        for l in levels[1:]:
            if abs(l - current[0]) <= tolerance:
                current.append(l)
            else:
                if len(current) >= min_touches:
                    clusters.append(round(statistics.mean(current), 6))
                current = [l]
        if len(current) >= min_touches:
            clusters.append(round(statistics.mean(current), 6))
        return clusters

    return {
        "support": cluster(mins),
        "resistance": cluster(maxs),
    }


def compute_adx(prices: List[float], period: int = ADX_PERIOD) -> Optional[float]:
    """Approximate ADX from single-price series. Fully guarded against div-by-zero."""
    if not prices or period < 1 or len(prices) < period * 2 + 1:
        return None

    # Approximate +DM/-DM from consecutive changes
    plus_dm = []
    minus_dm = []
    tr_list = []

    for i in range(1, len(prices)):
        diff = prices[i] - prices[i - 1]
        tr_list.append(abs(diff))
        if diff > 0:
            plus_dm.append(diff)
            minus_dm.append(0.0)
        else:
            plus_dm.append(0.0)
            minus_dm.append(abs(diff))

    if len(tr_list) < period:
        return None

    # Wilder's smoothing
    def wilder_smooth(vals: List[float], p: int) -> List[float]:
        result = [sum(vals[:p])]
        for v in vals[p:]:
            result.append(result[-1] - result[-1] / p + v)
        return result

    sm_tr = wilder_smooth(tr_list, period)
    sm_plus = wilder_smooth(plus_dm, period)
    sm_minus = wilder_smooth(minus_dm, period)

    # DI+ and DI-
    dx_list = []
    for i in range(len(sm_tr)):
        if sm_tr[i] < 1e-10:
            dx_list.append(0.0)
            continue
        di_plus = sm_plus[i] / sm_tr[i] * 100
        di_minus = sm_minus[i] / sm_tr[i] * 100
        di_sum = di_plus + di_minus
        if di_sum < 1e-10:
            dx_list.append(0.0)
        else:
            dx_list.append(abs(di_plus - di_minus) / di_sum * 100)

    if len(dx_list) < period:
        return None

    # ADX = smoothed DX
    adx = sum(dx_list[:period]) / period
    for d in dx_list[period:]:
        adx = (adx * (period - 1) + d) / period

    return round(adx, 4)


def compute_hurst(prices: List[float], window: int = HURST_WINDOW) -> Optional[float]:
    """Hurst exponent via rescaled range (R/S) analysis.
    H > 0.5 = trending, H < 0.5 = mean-reverting, H = 0.5 = random walk."""
    if len(prices) < window:
        return None

    series = prices[-window:]
    returns = [series[i] - series[i - 1] for i in range(1, len(series))]

    if len(returns) < 20:
        return None

    # R/S for different sub-window sizes
    sizes = []
    rs_values = []
    n = len(returns)
    for size in [int(n / 8), int(n / 4), int(n / 2), n]:
        if size < 8:
            continue
        # Split into non-overlapping windows
        rs_for_size = []
        for start in range(0, n - size + 1, size):
            chunk = returns[start:start + size]
            if len(chunk) < 2:
                continue
            mean_c = statistics.mean(chunk)
            deviations = [sum(chunk[:i + 1]) - (i + 1) * mean_c for i in range(len(chunk))]
            if not deviations:
                continue
            R = max(deviations) - min(deviations)
            S = statistics.stdev(chunk)
            if S > 1e-10:
                rs_for_size.append(R / S)
        if rs_for_size:
            sizes.append(math.log(size))
            rs_values.append(math.log(statistics.mean(rs_for_size)))

    if len(sizes) < 2:
        return None

    # Linear regression: log(R/S) = H * log(n) + c
    n_pts = len(sizes)
    sum_x = sum(sizes)
    sum_y = sum(rs_values)
    sum_xy = sum(x * y for x, y in zip(sizes, rs_values))
    sum_xx = sum(x * x for x in sizes)
    denom = n_pts * sum_xx - sum_x * sum_x
    if abs(denom) < 1e-10:
        return 0.5
    H = (n_pts * sum_xy - sum_x * sum_y) / denom
    return round(max(0.0, min(1.0, H)), 4)


def detect_regime(adx: Optional[float], hurst: Optional[float]) -> str:
    """ADX-dominant regime detection.
    ADX is more reliable than Hurst for noisy 30s prediction market data."""
    # ADX is primary — it was averaging 65 in live data (clearly trending)
    if adx is not None and adx > REGIME_TRENDING_ADX:
        return "trending"
    if adx is not None and adx < 20:
        return "mean_reverting"
    # Only use Hurst as tiebreaker in ambiguous ADX range (20-25)
    if hurst is not None:
        return "trending" if hurst > 0.55 else "mean_reverting"
    return "unknown"


def detect_breakout(current_price: float,
                    support_levels: List[float],
                    resistance_levels: List[float]) -> Optional[Dict]:
    """Check if price just broke through a support/resistance level."""
    for level in resistance_levels:
        if current_price > level and (current_price - level) / level < 0.01:
            return {"type": "breakout_up", "level": level, "price": current_price}
    for level in support_levels:
        if current_price < level and (level - current_price) / level < 0.01:
            return {"type": "breakout_down", "level": level, "price": current_price}
    return None
