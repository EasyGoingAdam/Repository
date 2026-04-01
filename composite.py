"""Composite signal engine — multi-indicator weighted scoring with regime adaptation.

v2 improvements based on 12h live data analysis:
- Signal cooldown to prevent spam (min 5 min between same-direction signals)
- ADX-dominant regime detection (was being overridden by Hurst)
- Trend-aware mean reversion (don't fight strong trends)
- Confidence floor (min 0.5 to fire)
- Higher composite threshold (0.4 vs 0.3)
- Profit target and stop-loss levels on each signal
- Dampened indicator scaling to handle prediction market price granularity
"""
from __future__ import annotations

import statistics
from typing import Optional, List, Dict
from datetime import datetime, timezone

from config import (
    REGIME_TRENDING_ADX,
    WEIGHT_MEAN_REVERSION, WEIGHT_MOMENTUM, WEIGHT_VOLATILITY,
    WEIGHT_ORDER_FLOW, WEIGHT_VOLUME, KELLY_FRACTION,
)

# Tuned thresholds based on 12h live data
COMPOSITE_THRESHOLD = 0.4  # raised from 0.3
CONFIDENCE_FLOOR = 0.45    # don't fire below this
COOLDOWN_SECONDS = 300     # 5 min between same-direction signals
TREND_SUPPRESSION = 0.6    # scale down mean-reversion when trending strongly

# Track last signal for cooldown (thread-safe)
import threading
_cooldown_lock = threading.Lock()
_last_signal_type = None
_last_signal_ts = 0.0


def _clamp(v: float, lo: float = -1.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, v))


# ---------------------------------------------------------------------------
# Component scorers (each returns -1 to +1)
# ---------------------------------------------------------------------------

def _mean_reversion_score(ind: Dict, regime: str, adx: float) -> float:
    """Bollinger z-score + RSI + CCI + Williams %R → consensus.
    Dampened when market is strongly trending (ADX > 40)."""
    scores = []

    z = ind.get("bollinger_z")
    if z is not None:
        # Inverted and dampened: prediction market z-scores are often large due to
        # price granularity (0.5c ticks), so use gentler scaling
        scores.append(_clamp(-z / 3.0))

    rsi = ind.get("rsi_14")
    if rsi is not None:
        # Widen the neutral band: RSI 25-75 is normal for prediction markets
        # Only score strongly at extremes
        if rsi < 25:
            scores.append(_clamp((25 - rsi) / 15.0))
        elif rsi > 75:
            scores.append(_clamp((75 - rsi) / 15.0))
        else:
            scores.append(0.0)

    cci = ind.get("cci_20")
    if cci is not None:
        # CCI swings wildly on prediction markets, use 200 as threshold not 100
        scores.append(_clamp(-cci / 200.0))

    wr = ind.get("williams_r")
    if wr is not None:
        scores.append(_clamp((-50 - wr) / 40.0))

    if not scores:
        return 0.0

    raw = statistics.mean(scores)

    # CRITICAL FIX: suppress mean-reversion signals when trend is strong
    # Fighting a strong trend was the #1 cause of losses
    if regime == "trending" and adx is not None and adx > 40:
        raw *= TREND_SUPPRESSION * (40.0 / max(adx, 40))

    return round(raw, 4)


def _momentum_score(ind: Dict) -> float:
    """MACD histogram + ROC + Stochastic crossover → consensus."""
    scores = []

    hist = ind.get("macd_histogram")
    if hist is not None:
        # Prediction market MACD histograms are tiny (0.001 range)
        # Scale to match but don't let it dominate
        scores.append(_clamp(hist / 0.005))

    roc = ind.get("roc_12")
    if roc is not None:
        # ROC of 1% on a prediction market is a big move
        scores.append(_clamp(roc / 2.0))

    k = ind.get("stoch_k")
    d = ind.get("stoch_d")
    if k is not None and d is not None:
        # Stochastic crossover direction
        diff = (k - d) / 30.0
        scores.append(_clamp(diff))

    return round(statistics.mean(scores), 4) if scores else 0.0


def _volatility_score(ind: Dict) -> float:
    """Volatility regime assessment.
    Positive = low vol (good for mean-rev entries), negative = high vol (risky)."""
    scores = []

    vov = ind.get("vol_of_vol")
    if vov is not None:
        # Normalize: avg vol_of_vol was 0.35, current spikes to 1.0+
        scores.append(_clamp(-(vov - 0.35) / 0.5))

    bz = ind.get("bollinger_z")
    if bz is not None:
        # Price within 1 std = calm, beyond 2 std = volatile
        scores.append(_clamp(1.0 - abs(bz) / 2.0))

    hv = ind.get("historical_vol")
    if hv is not None:
        # Avg was 1.01; higher = more volatile = negative
        scores.append(_clamp(-(hv - 1.0) / 1.5))

    return round(statistics.mean(scores), 4) if scores else 0.0


def _orderflow_score(snapshot: Dict) -> float:
    """Order book imbalance → directional signal.
    Dampened: imbalance flips rapidly on thin prediction markets."""
    imb = snapshot.get("imbalance")
    if imb is None:
        return 0.0
    # Less aggressive: the imbalance was swinging -0.62 to +0.61
    # Only treat > 0.2 as meaningful
    if abs(imb) < 0.15:
        return 0.0
    return round(_clamp(imb * 1.2), 4)


def _volume_score(ind: Dict) -> float:
    """OBV direction + volume ROC → conviction."""
    scores = []

    obv = ind.get("obv")
    if obv is not None:
        # OBV reached 3000+ in our data; normalize gently
        scores.append(_clamp(obv / 100000.0))

    vroc = ind.get("volume_roc")
    if vroc is not None:
        scores.append(_clamp(vroc / 30.0))

    return round(statistics.mean(scores), 4) if scores else 0.0


# ---------------------------------------------------------------------------
# Regime Detection Override
# ---------------------------------------------------------------------------

def _determine_regime(ind: Dict) -> str:
    """ADX-dominant regime detection. ADX is more reliable than Hurst
    for the noisy 30s data we're working with."""
    adx = ind.get("adx")
    hurst = ind.get("hurst_exponent")

    # ADX is the primary signal (it was avg 65 in our data — clearly trending)
    if adx is not None and adx > REGIME_TRENDING_ADX:
        return "trending"
    if adx is not None and adx < 20:
        return "mean_reverting"

    # Only use Hurst as tiebreaker in ambiguous ADX range
    if hurst is not None:
        return "trending" if hurst > 0.55 else "mean_reverting"

    return "unknown"


# ---------------------------------------------------------------------------
# Composite Signal Generator
# ---------------------------------------------------------------------------

def generate_signal(indicators: Dict, snapshot: Dict,
                    risk_metrics: Optional[Dict] = None) -> Optional[Dict]:
    """Generate composite buy/sell signal from all indicators."""
    global _last_signal_type, _last_signal_ts
    now = datetime.now(timezone.utc)
    now_ts = now.timestamp()

    # Override regime with our improved detection
    adx = indicators.get("adx")
    regime = _determine_regime(indicators)

    # Compute component scores (mean reversion is now trend-aware)
    mr_score = _mean_reversion_score(indicators, regime, adx or 0)
    mom_score = _momentum_score(indicators)
    vol_score = _volatility_score(indicators)
    of_score = _orderflow_score(snapshot)
    v_score = _volume_score(indicators)

    # Regime-adaptive weights
    w_mr = WEIGHT_MEAN_REVERSION
    w_mom = WEIGHT_MOMENTUM
    w_vol = WEIGHT_VOLATILITY
    w_of = WEIGHT_ORDER_FLOW
    w_v = WEIGHT_VOLUME

    if regime == "trending":
        # Strong shift: momentum-driven, suppress mean reversion
        w_mom += 0.15
        w_mr -= 0.15
    elif regime == "mean_reverting":
        w_mr += 0.10
        w_mom -= 0.05
        w_of -= 0.05  # OB imbalance is noisy in ranging markets

    # Weighted composite score
    composite = (
        mr_score * w_mr +
        mom_score * w_mom +
        vol_score * w_vol +
        of_score * w_of +
        v_score * w_v
    )
    composite = round(_clamp(composite), 4)

    # Confidence: requires agreement among components
    component_scores = [mr_score, mom_score, vol_score, of_score, v_score]
    non_zero = [s for s in component_scores if abs(s) > 0.05]

    if len(non_zero) >= 3:
        same_direction = sum(1 for s in non_zero if (s > 0) == (composite > 0))
        agreement = same_direction / len(non_zero)
        std = statistics.stdev(non_zero) if len(non_zero) > 1 else 0.5
        confidence = round(agreement * max(0.2, 1.0 - std), 4)
    elif len(non_zero) >= 2:
        confidence = 0.3
    else:
        confidence = 0.15

    # --- GATE CHECKS ---
    # 1. Composite threshold (raised)
    if abs(composite) < COMPOSITE_THRESHOLD:
        return None

    # 2. Confidence floor
    if confidence < CONFIDENCE_FLOOR:
        return None

    signal_type = "BUY" if composite > 0 else "SELL"

    # 3. Cooldown: no same-direction signal within 5 minutes (thread-safe)
    with _cooldown_lock:
        if signal_type == _last_signal_type and (now_ts - _last_signal_ts) < COOLDOWN_SECONDS:
            return None
        # Passed all gates — this is a real signal
        _last_signal_type = signal_type
        _last_signal_ts = now_ts

    # Kelly / risk info
    kelly = 0.0
    sharpe = sortino = max_dd = var_95 = None
    if risk_metrics:
        kelly = risk_metrics.get("kelly_fraction", 0.0) or 0.0
        sharpe = risk_metrics.get("sharpe_ratio")
        sortino = risk_metrics.get("sortino_ratio")
        max_dd = risk_metrics.get("max_drawdown")
        var_95 = risk_metrics.get("var_95")

    suggested_pct = round(kelly * confidence * 100, 2)

    # Profit target & stop-loss based on ATR and Bollinger bandwidth
    current_price = snapshot.get("yes_price", 0)
    atr = indicators.get("atr_14") or 0.005
    bb_upper = indicators.get("bollinger_upper") or current_price + 0.01
    bb_lower = indicators.get("bollinger_lower") or current_price - 0.01
    bb_sma = indicators.get("bollinger_sma") or current_price

    if signal_type == "BUY":
        target = round(min(bb_sma, current_price + atr * 3), 4)
        stop = round(max(0.01, current_price - atr * 2), 4)
    else:
        target = round(max(bb_sma, current_price - atr * 3), 4)
        stop = round(min(0.99, current_price + atr * 2), 4)

    # Build reasoning
    parts = []
    if abs(mr_score) > 0.15:
        parts.append(f"MeanRev {'buy' if mr_score > 0 else 'sell'}({mr_score:+.2f})")
    if abs(mom_score) > 0.15:
        parts.append(f"Mom {'up' if mom_score > 0 else 'down'}({mom_score:+.2f})")
    if abs(of_score) > 0.15:
        parts.append(f"OB {'bid' if of_score > 0 else 'ask'}({of_score:+.2f})")
    if abs(vol_score) > 0.15:
        parts.append(f"Vol {'calm' if vol_score > 0 else 'hot'}({vol_score:+.2f})")
    parts.append(f"{regime.upper()} ADX={adx:.0f}" if adx else regime.upper())
    parts.append(f"Target={target*100:.1f}c Stop={stop*100:.1f}c")
    reasoning = " | ".join(parts)

    return {
        "timestamp": now.isoformat(),
        "unix_ts": now_ts,
        "signal_type": signal_type,
        "composite_score": composite,
        "confidence": confidence,
        "mean_reversion_score": mr_score,
        "momentum_score": mom_score,
        "volatility_score": vol_score,
        "orderflow_score": of_score,
        "volume_score": v_score,
        "kelly_fraction": kelly,
        "suggested_position_pct": suggested_pct,
        "current_sharpe": sharpe,
        "current_sortino": sortino,
        "max_drawdown": max_dd,
        "var_95": var_95,
        "regime": regime,
        "adx": adx,
        "reasoning": reasoning,
    }
