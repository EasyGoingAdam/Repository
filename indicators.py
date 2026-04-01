"""Technical indicators engine — pure Python, no external deps."""
from __future__ import annotations

import math
import statistics
from typing import Optional, List, Dict
from datetime import datetime, timezone

from config import (
    RSI_PERIOD, MACD_FAST, MACD_SLOW, MACD_SIGNAL,
    STOCH_K_PERIOD, STOCH_D_PERIOD, ROC_PERIOD, WILLIAMS_R_PERIOD,
    ATR_PERIOD, KELTNER_EMA_PERIOD, KELTNER_ATR_PERIOD, KELTNER_MULTIPLIER,
    HIST_VOL_WINDOW, VOL_OF_VOL_WINDOW, CCI_PERIOD,
    BOLLINGER_PERIOD, BOLLINGER_STD_MULTIPLIER,
)
import bollinger


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ema(values: List[float], period: int) -> List[Optional[float]]:
    """Exponential moving average. Returns list same length as input."""
    if len(values) < period:
        return [None] * len(values)
    result = [None] * (period - 1)
    sma = sum(values[:period]) / period
    result.append(sma)
    mult = 2.0 / (period + 1)
    prev = sma
    for v in values[period:]:
        prev = (v - prev) * mult + prev
        result.append(prev)
    return result


def _sma(values: List[float], period: int) -> List[Optional[float]]:
    """Simple moving average."""
    result = []
    for i in range(len(values)):
        if i < period - 1:
            result.append(None)
        else:
            result.append(sum(values[i - period + 1:i + 1]) / period)
    return result


def _true_range(prices: List[float]) -> List[float]:
    """Pseudo true range from single-price series (no OHLC)."""
    tr = []
    for i in range(1, len(prices)):
        tr.append(abs(prices[i] - prices[i - 1]))
    return tr


# ---------------------------------------------------------------------------
# Momentum Indicators
# ---------------------------------------------------------------------------

def compute_rsi(prices: List[float], period: int = RSI_PERIOD) -> Optional[float]:
    """Wilder's RSI. Returns 0-100."""
    if len(prices) < period + 1:
        return None
    changes = [prices[i] - prices[i - 1] for i in range(1, len(prices))]
    gains = [max(0, c) for c in changes]
    losses = [max(0, -c) for c in changes]

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period

    if avg_loss < 1e-10:
        return 100.0
    rs = avg_gain / avg_loss
    return round(100.0 - 100.0 / (1.0 + rs), 4)


def compute_macd(prices: List[float]) -> Optional[Dict]:
    """MACD line, signal line, histogram."""
    if len(prices) < MACD_SLOW + MACD_SIGNAL:
        return None
    fast_ema = _ema(prices, MACD_FAST)
    slow_ema = _ema(prices, MACD_SLOW)

    macd_line = []
    for f, s in zip(fast_ema, slow_ema):
        if f is not None and s is not None:
            macd_line.append(f - s)

    if len(macd_line) < MACD_SIGNAL:
        return None

    signal_line = _ema(macd_line, MACD_SIGNAL)
    if signal_line[-1] is None:
        return None

    histogram = macd_line[-1] - signal_line[-1]
    return {
        "macd_line": round(macd_line[-1], 6),
        "macd_signal": round(signal_line[-1], 6),
        "macd_histogram": round(histogram, 6),
    }


def compute_stochastic(prices: List[float]) -> Optional[Dict]:
    """%K and %D of stochastic oscillator."""
    if len(prices) < STOCH_K_PERIOD + STOCH_D_PERIOD:
        return None

    k_values = []
    for i in range(STOCH_K_PERIOD - 1, len(prices)):
        window = prices[i - STOCH_K_PERIOD + 1:i + 1]
        hh = max(window)
        ll = min(window)
        rng = hh - ll
        if rng < 1e-10:
            k_values.append(50.0)
        else:
            k_values.append((prices[i] - ll) / rng * 100.0)

    if len(k_values) < STOCH_D_PERIOD:
        return None

    d_value = sum(k_values[-STOCH_D_PERIOD:]) / STOCH_D_PERIOD
    return {
        "stoch_k": round(k_values[-1], 4),
        "stoch_d": round(d_value, 4),
    }


def compute_roc(prices: List[float], period: int = ROC_PERIOD) -> Optional[float]:
    """Rate of change (percent)."""
    if len(prices) < period + 1:
        return None
    old = prices[-period - 1]
    if abs(old) < 1e-10:
        return None
    return round((prices[-1] - old) / old * 100.0, 4)


def compute_williams_r(prices: List[float], period: int = WILLIAMS_R_PERIOD) -> Optional[float]:
    """Williams %R. Returns -100 to 0."""
    if len(prices) < period:
        return None
    window = prices[-period:]
    hh = max(window)
    ll = min(window)
    rng = hh - ll
    if rng < 1e-10:
        return -50.0
    return round((hh - prices[-1]) / rng * -100.0, 4)


# ---------------------------------------------------------------------------
# Volatility Indicators
# ---------------------------------------------------------------------------

def compute_atr(prices: List[float], period: int = ATR_PERIOD) -> Optional[float]:
    """Average True Range using pseudo TR."""
    tr = _true_range(prices)
    if len(tr) < period:
        return None
    atr = sum(tr[:period]) / period
    for i in range(period, len(tr)):
        atr = (atr * (period - 1) + tr[i]) / period
    return round(atr, 6)


def compute_keltner(prices: List[float]) -> Optional[Dict]:
    """Keltner Channels: EMA ± multiplier * ATR."""
    ema_vals = _ema(prices, KELTNER_EMA_PERIOD)
    if ema_vals[-1] is None:
        return None
    atr = compute_atr(prices, KELTNER_ATR_PERIOD)
    if atr is None:
        return None
    mid = ema_vals[-1]
    return {
        "keltner_upper": round(mid + KELTNER_MULTIPLIER * atr, 6),
        "keltner_lower": round(max(0, mid - KELTNER_MULTIPLIER * atr), 6),
        "keltner_mid": round(mid, 6),
    }


def compute_historical_volatility(prices: List[float], window: int = HIST_VOL_WINDOW) -> Optional[float]:
    """Annualized std dev of log returns."""
    if len(prices) < window + 1:
        return None
    log_returns = []
    for i in range(-window, 0):
        if prices[i - 1] > 0 and prices[i] > 0:
            log_returns.append(math.log(prices[i] / prices[i - 1]))
    if len(log_returns) < 2:
        return None
    std = statistics.stdev(log_returns)
    # Annualize: ~2880 30-second periods per day, 365 days
    annualized = std * math.sqrt(2880 * 365)
    return round(annualized, 6)


def compute_vol_of_vol(prices: List[float]) -> Optional[float]:
    """Volatility of volatility — std dev of rolling historical vol."""
    min_needed = HIST_VOL_WINDOW + VOL_OF_VOL_WINDOW + 1
    if len(prices) < min_needed:
        return None
    vols = []
    for end in range(HIST_VOL_WINDOW + 1, len(prices) + 1):
        subset = prices[end - HIST_VOL_WINDOW - 1:end]
        v = compute_historical_volatility(subset, HIST_VOL_WINDOW)
        if v is not None:
            vols.append(v)
    if len(vols) < VOL_OF_VOL_WINDOW:
        return None
    return round(statistics.stdev(vols[-VOL_OF_VOL_WINDOW:]), 6)


# ---------------------------------------------------------------------------
# Volume / Liquidity Indicators
# ---------------------------------------------------------------------------

def compute_obv(snapshots: List[Dict]) -> Optional[float]:
    """On-Balance Volume using volume_24hr deltas."""
    if len(snapshots) < 3:
        return None
    obv = 0.0
    for i in range(1, len(snapshots)):
        v_curr = snapshots[i].get("volume_24hr") or 0
        v_prev = snapshots[i - 1].get("volume_24hr") or 0
        delta_vol = max(0, v_curr - v_prev)  # ignore resets
        if snapshots[i]["yes_price"] > snapshots[i - 1]["yes_price"]:
            obv += delta_vol
        elif snapshots[i]["yes_price"] < snapshots[i - 1]["yes_price"]:
            obv -= delta_vol
    return round(obv, 2)


def compute_volume_roc(snapshots: List[Dict], period: int = 12) -> Optional[float]:
    """Volume rate of change."""
    if len(snapshots) < period + 1:
        return None
    curr = snapshots[-1].get("volume_24hr") or 0
    prev = snapshots[-period - 1].get("volume_24hr") or 0
    if prev < 1:
        return None
    return round((curr - prev) / prev * 100.0, 4)


def compute_liquidity_score(snapshot: Dict) -> Optional[float]:
    """Normalized liquidity score (sigmoid to 0-1)."""
    liq = snapshot.get("liquidity") or 0
    if liq <= 0:
        return None
    # Sigmoid centered at $50k
    return round(1.0 / (1.0 + math.exp(-0.00004 * (liq - 50000))), 4)


# ---------------------------------------------------------------------------
# Mean Reversion
# ---------------------------------------------------------------------------

def compute_cci(prices: List[float], period: int = CCI_PERIOD) -> Optional[float]:
    """Commodity Channel Index."""
    if len(prices) < period:
        return None
    window = prices[-period:]
    sma = sum(window) / period
    mean_dev = sum(abs(p - sma) for p in window) / period
    if mean_dev < 1e-10:
        return 0.0
    return round((prices[-1] - sma) / (0.015 * mean_dev), 4)


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def _smooth_prices(prices: List[float], period: int = 5) -> List[float]:
    """EMA-smooth raw prices to reduce 30s polling noise.
    Uses a short EMA(5) which preserves trends but dampens tick noise."""
    if len(prices) < period:
        return prices
    smoothed = _ema(prices, period)
    # Replace None values with originals
    return [s if s is not None else p for s, p in zip(smoothed, prices)]


def compute_all(snapshots: List[Dict]) -> Dict:
    """Compute all indicators from snapshot list. Returns flat dict for DB storage."""
    now = datetime.now(timezone.utc)
    raw_prices = [s["yes_price"] for s in snapshots if s.get("yes_price") is not None]

    result = {
        "timestamp": now.isoformat(),
        "unix_ts": now.timestamp(),
    }

    if len(raw_prices) < 5:
        return result

    # Smooth prices with EMA(5) to reduce 30s polling noise
    prices = _smooth_prices(raw_prices)

    # Each indicator is wrapped in try/except so one failure doesn't crash all
    def _safe(fn, *args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            print(f"[Indicators] {fn.__name__} error: {e}")
            return None

    # Momentum
    result["rsi_14"] = _safe(compute_rsi, prices)
    macd = _safe(compute_macd, prices)
    if macd:
        result["macd_line"] = macd.get("macd_line")
        result["macd_signal"] = macd.get("macd_signal")
        result["macd_histogram"] = macd.get("macd_histogram")
    stoch = _safe(compute_stochastic, prices)
    if stoch:
        result["stoch_k"] = stoch.get("stoch_k")
        result["stoch_d"] = stoch.get("stoch_d")
    result["roc_12"] = _safe(compute_roc, prices)
    result["williams_r"] = _safe(compute_williams_r, prices)

    # Volatility
    result["atr_14"] = _safe(compute_atr, prices)
    keltner = _safe(compute_keltner, prices)
    if keltner:
        result["keltner_upper"] = keltner.get("keltner_upper")
        result["keltner_lower"] = keltner.get("keltner_lower")
        result["keltner_mid"] = keltner.get("keltner_mid")
    result["historical_vol"] = _safe(compute_historical_volatility, prices)
    result["vol_of_vol"] = _safe(compute_vol_of_vol, prices)

    # Volume / Liquidity
    result["obv"] = _safe(compute_obv, snapshots)
    result["volume_roc"] = _safe(compute_volume_roc, snapshots)
    if snapshots:
        result["liquidity_score"] = _safe(compute_liquidity_score, snapshots[-1])

    # Mean Reversion
    result["cci_20"] = _safe(compute_cci, prices)
    bands = _safe(bollinger.compute_bollinger, snapshots)
    if bands:
        result["bollinger_upper"] = bands.get("upper")
        result["bollinger_lower"] = bands.get("lower")
        result["bollinger_sma"] = bands.get("sma")
        result["bollinger_z"] = bands.get("z_score")

    return result
