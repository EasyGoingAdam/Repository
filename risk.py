"""Risk metrics — Sharpe, Sortino, VaR, Kelly criterion, drawdown."""
from __future__ import annotations

import math
import statistics
from typing import Optional, List, Dict
from datetime import datetime, timezone

from config import RISK_FREE_RATE, VAR_CONFIDENCE, KELLY_FRACTION


def compute_sharpe(returns: List[float], risk_free: float = RISK_FREE_RATE) -> Optional[float]:
    """Annualized Sharpe ratio."""
    if len(returns) < 2:
        return None
    mean_r = statistics.mean(returns)
    std_r = statistics.stdev(returns)
    if std_r < 1e-10:
        return None
    # Per-period excess, annualize assuming ~2880 periods/day * 365
    periods_per_year = 2880 * 365
    rf_per_period = risk_free / periods_per_year
    return round((mean_r - rf_per_period) / std_r * math.sqrt(periods_per_year), 4)


def compute_sortino(returns: List[float], risk_free: float = RISK_FREE_RATE) -> Optional[float]:
    """Annualized Sortino ratio (downside deviation only)."""
    if len(returns) < 2:
        return None
    periods_per_year = 2880 * 365
    rf_per_period = risk_free / periods_per_year
    mean_r = statistics.mean(returns)
    downside = [r for r in returns if r < 0]
    if len(downside) < 2:
        return None
    down_std = math.sqrt(sum(d * d for d in downside) / len(downside))
    if down_std < 1e-10:
        return None
    return round((mean_r - rf_per_period) / down_std * math.sqrt(periods_per_year), 4)


def compute_max_drawdown(equity_curve: List[float]) -> float:
    """Maximum peak-to-trough decline as a fraction."""
    if len(equity_curve) < 2:
        return 0.0
    peak = equity_curve[0]
    max_dd = 0.0
    for v in equity_curve:
        if v > peak:
            peak = v
        dd = (peak - v) / peak if peak > 0 else 0
        if dd > max_dd:
            max_dd = dd
    return round(max_dd, 6)


def compute_var(returns: List[float], confidence: float = VAR_CONFIDENCE) -> Optional[float]:
    """Historical Value at Risk at given confidence level."""
    if len(returns) < 10:
        return None
    sorted_r = sorted(returns)
    idx = int((1 - confidence) * len(sorted_r))
    idx = max(0, min(idx, len(sorted_r) - 1))
    return round(sorted_r[idx], 6)


def compute_kelly(win_rate: float, avg_win: float, avg_loss: float) -> float:
    """Kelly criterion position fraction (half-Kelly applied)."""
    if avg_loss < 1e-10 or win_rate <= 0:
        return 0.0
    b = avg_win / avg_loss
    p = win_rate
    q = 1 - p
    f = (b * p - q) / b
    f = max(0, min(1, f))
    return round(f * KELLY_FRACTION, 4)


def compute_metrics(signals: List[Dict], snapshots: List[Dict]) -> Dict:
    """Evaluate signal performance and compute all risk metrics."""
    now = datetime.now(timezone.utc)
    result = {
        "timestamp": now.isoformat(),
        "unix_ts": now.timestamp(),
        "total_signals": len(signals),
        "profitable_signals": 0,
    }

    if len(signals) < 2 or len(snapshots) < 20:
        return result

    # Build price lookup by unix_ts
    snap_prices = [(s["unix_ts"], s["yes_price"]) for s in snapshots if s.get("yes_price")]
    if not snap_prices:
        return result

    # Evaluate each signal: look 10 snapshots ahead (~5 min)
    returns = []
    wins = 0
    win_amounts = []
    loss_amounts = []

    for sig in signals:
        try:
            sig_ts = sig.get("unix_ts", 0)
            if not sig_ts:
                continue
            price_at = None
            price_after = None

            for ts, p in snap_prices:
                if price_at is None and ts >= sig_ts:
                    price_at = p
                if price_at is not None and ts >= sig_ts + 300:  # 5 min later
                    price_after = p
                    break

            if price_at is None or price_after is None:
                continue

            direction = 1.0 if sig.get("signal_type") == "BUY" else -1.0
            pnl = (price_after - price_at) * direction
            returns.append(pnl)

            if pnl > 0:
                wins += 1
                win_amounts.append(pnl)
            elif pnl < 0:
                loss_amounts.append(abs(pnl))
        except (KeyError, TypeError, ValueError):
            continue

    result["profitable_signals"] = wins

    if len(returns) > 0:
        result["win_rate"] = round(wins / len(returns), 4)
        result["avg_profit"] = round(statistics.mean(returns), 6)
        result["sharpe_ratio"] = compute_sharpe(returns)
        result["sortino_ratio"] = compute_sortino(returns)
        result["var_95"] = compute_var(returns)

        # Equity curve for drawdown
        equity = [0.0]
        for r in returns:
            equity.append(equity[-1] + r)
        # Shift to positive for drawdown calc
        base = abs(min(equity)) + 1.0
        equity_pos = [e + base for e in equity]
        result["max_drawdown"] = compute_max_drawdown(equity_pos)

        # Kelly
        avg_w = statistics.mean(win_amounts) if win_amounts else 0
        avg_l = statistics.mean(loss_amounts) if loss_amounts else 0
        wr = wins / len(returns) if returns else 0
        result["kelly_fraction"] = compute_kelly(wr, avg_w, avg_l)

    return result
