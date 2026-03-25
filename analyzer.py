"""
Prediction engine for Polymarket markets.

The analyzer uses multiple signals to generate a probability estimate:
  1. Market price (efficient market hypothesis baseline)
  2. Price momentum (recent trend direction)
  3. Liquidity & volume (market quality / conviction)
  4. Time decay (how far from resolution)
  5. Order book imbalance (buy vs sell pressure)
  6. Volatility (uncertainty signal)
  7. Recency-weighted trade flow

The algorithm is parameterized by weights that can be refined based on
historical prediction accuracy (see refine_weights()).
"""
import math
from datetime import datetime, timezone
from typing import Optional
from models import Market, Prediction
import api as polyapi

# Default signal weights — updated by refine_weights()
DEFAULT_WEIGHTS = {
    "market_price": 0.55,       # baseline: trust the market
    "momentum": 0.10,           # recent price direction
    "volume_conviction": 0.08,  # high volume → more trust in price
    "liquidity_quality": 0.07,  # deep book → less manipulation risk
    "time_decay": 0.08,         # near expiry → price more reliable
    "orderbook_imbalance": 0.07,# bid/ask skew
    "trade_flow": 0.05,         # net recent trade direction
}


def analyze_market(market: Market, weights: dict = None) -> Prediction:
    """
    Run full analysis on a market and return a Prediction.
    """
    if weights is None:
        weights = DEFAULT_WEIGHTS

    signals = {}

    # 1. Market price baseline
    market_price = market.yes_price
    signals["market_price"] = market_price
    signals["market_price_weight"] = weights["market_price"]

    # 2. Price history / momentum
    momentum_score = 0.0
    price_history = []
    if market.clob_token_ids:
        yes_token = market.clob_token_ids[0]
        price_history = polyapi.get_price_history(yes_token, interval="1w", fidelity=60)
        momentum_score = _compute_momentum(price_history, market_price)
    signals["momentum"] = momentum_score
    signals["price_history_points"] = len(price_history)

    # 3. Volume conviction — normalize against $1M reference
    volume_score = min(market.volume / 1_000_000, 1.0)
    signals["volume_score"] = volume_score
    signals["volume_usd"] = market.volume

    # 4. Liquidity quality — normalize against $100k
    liquidity_score = min(market.liquidity / 100_000, 1.0)
    signals["liquidity_score"] = liquidity_score
    signals["liquidity_usd"] = market.liquidity

    # 5. Time decay — how far until resolution?
    time_signal = _time_signal(market)
    signals["time_signal"] = time_signal
    signals["end_date"] = market.end_date

    # 6. Order book imbalance — use full depth data
    ob_signal = 0.0
    ob_spread = None
    if market.clob_token_ids:
        yes_token = market.clob_token_ids[0]
        depth = polyapi.get_orderbook_depth(yes_token)
        ob_signal = depth.get("imbalance", 0.0)
        ob_spread = depth.get("spread")
        signals["best_bid"] = depth.get("best_bid")
        signals["best_ask"] = depth.get("best_ask")
        signals["ob_spread_cents"] = round(ob_spread * 100, 2) if ob_spread else None
        signals["total_bid_notional"] = depth.get("total_bid_notional", 0)
        signals["total_ask_notional"] = depth.get("total_ask_notional", 0)
    signals["orderbook_imbalance"] = ob_signal

    # 7. Trade flow — net direction of recent trades
    trade_flow = 0.0
    if market.clob_token_ids:
        yes_token = market.clob_token_ids[0]
        trades = polyapi.get_trades(yes_token, limit=50)
        trade_flow = _trade_flow_signal(trades)
    signals["trade_flow"] = trade_flow

    # Composite score: weighted combination of signals
    # Signals are mapped to probability-space adjustments around market_price
    adjustment = (
        weights["momentum"] * momentum_score * 0.1 +
        weights["volume_conviction"] * (volume_score - 0.5) * 0.05 +
        weights["liquidity_quality"] * (liquidity_score - 0.5) * 0.05 +
        weights["time_decay"] * time_signal * 0.05 +
        weights["orderbook_imbalance"] * ob_signal * 0.05 +
        weights["trade_flow"] * trade_flow * 0.05
    )

    raw_prob = market_price * weights["market_price"] + adjustment
    # Clamp to [0.02, 0.98]
    predicted_prob = max(0.02, min(0.98, raw_prob))

    # Confidence: higher volume + liquidity + more history = more confident
    confidence = _compute_confidence(market, len(price_history), weights)

    reasoning = _build_reasoning(market, signals, predicted_prob, confidence, adjustment)

    return Prediction(
        market_id=market.id,
        market_question=market.question,
        timestamp=datetime.now(timezone.utc).isoformat(),
        yes_price_at_prediction=market_price,
        predicted_yes_prob=round(predicted_prob, 4),
        confidence=round(confidence, 3),
        reasoning=reasoning,
        signals=signals,
    )


def _compute_momentum(history: list[dict], current_price: float) -> float:
    """Returns -1 to +1. Positive = price rising."""
    if len(history) < 2:
        return 0.0
    prices = [float(p.get("p", 0)) for p in history if "p" in p]
    if len(prices) < 2:
        return 0.0
    # Compare recent half vs earlier half
    mid = len(prices) // 2
    recent_avg = sum(prices[mid:]) / len(prices[mid:])
    earlier_avg = sum(prices[:mid]) / len(prices[:mid])
    if earlier_avg == 0:
        return 0.0
    change = (recent_avg - earlier_avg) / earlier_avg
    # Normalize to [-1, 1]
    return max(-1.0, min(1.0, change * 10))


def _time_signal(market: Market) -> float:
    """
    Returns 0 to 1. Near expiry = high signal (price should be reliable).
    Far from expiry = low signal (more uncertainty).
    """
    if not market.end_date:
        return 0.5
    try:
        end = datetime.fromisoformat(market.end_date.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        days_remaining = (end - now).total_seconds() / 86400
        if days_remaining <= 0:
            return 1.0
        # Logistic: within 7 days = high, 365 days = low
        return 1.0 / (1.0 + math.exp((days_remaining - 30) / 20))
    except Exception:
        return 0.5


def _orderbook_imbalance(ob: dict) -> float:
    """
    Returns -1 to +1. Positive = more bids (buy pressure → bullish YES).
    """
    try:
        bids = ob.get("bids", [])
        asks = ob.get("asks", [])
        bid_vol = sum(float(b.get("size", 0)) for b in bids)
        ask_vol = sum(float(a.get("size", 0)) for a in asks)
        total = bid_vol + ask_vol
        if total == 0:
            return 0.0
        return (bid_vol - ask_vol) / total
    except Exception:
        return 0.0


def _trade_flow_signal(trades: list[dict]) -> float:
    """
    Returns -1 to +1. Recent net buying of YES tokens = positive.
    """
    if not trades:
        return 0.0
    buy_vol = 0.0
    sell_vol = 0.0
    for t in trades[:20]:  # last 20 trades
        size = float(t.get("size", 0))
        side = t.get("side", "").upper()
        if side == "BUY":
            buy_vol += size
        elif side == "SELL":
            sell_vol += size
    total = buy_vol + sell_vol
    if total == 0:
        return 0.0
    return (buy_vol - sell_vol) / total


def _compute_confidence(market: Market, history_points: int, weights: dict) -> float:
    """Confidence based on data quality and market depth."""
    vol_factor = min(market.volume / 500_000, 1.0)
    liq_factor = min(market.liquidity / 50_000, 1.0)
    history_factor = min(history_points / 100, 1.0)
    base = 0.4 + 0.3 * vol_factor + 0.2 * liq_factor + 0.1 * history_factor
    return min(0.95, base)


def _build_reasoning(market: Market, signals: dict, pred_prob: float, confidence: float, adjustment: float) -> str:
    bid_str = f"{signals.get('best_bid', 0)*100:.1f}¢" if signals.get("best_bid") else "N/A"
    ask_str = f"{signals.get('best_ask', 0)*100:.1f}¢" if signals.get("best_ask") else "N/A"
    spread_str = f"{signals.get('ob_spread_cents', 'N/A')}¢" if signals.get("ob_spread_cents") else "N/A"
    bid_notional = signals.get("total_bid_notional", 0)
    ask_notional = signals.get("total_ask_notional", 0)

    lines = [
        f"Market: {market.question}",
        f"Current YES price: {market.yes_price:.1%}",
        f"Predicted YES probability: {pred_prob:.1%} (confidence: {confidence:.0%})",
        f"",
        f"Key signals:",
        f"  • Market price (baseline): {signals['market_price']:.1%}",
        f"  • Best Bid / Ask: {bid_str} / {ask_str}  (spread: {spread_str})",
        f"  • Bid-side depth: ${bid_notional:,.0f}  |  Ask-side depth: ${ask_notional:,.0f}",
        f"  • Order book imbalance: {signals['orderbook_imbalance']:+.3f}  ({'buy pressure' if signals['orderbook_imbalance'] > 0.05 else 'sell pressure' if signals['orderbook_imbalance'] < -0.05 else 'balanced'})",
        f"  • Momentum: {signals['momentum']:+.2f} ({'rising' if signals['momentum'] > 0.05 else 'falling' if signals['momentum'] < -0.05 else 'flat'})",
        f"  • Volume: ${signals['volume_usd']:,.0f} (conviction: {signals['volume_score']:.1%})",
        f"  • Liquidity: ${signals['liquidity_usd']:,.0f}",
        f"  • Time signal: {signals['time_signal']:.2f} (higher near expiry)",
        f"  • Trade flow: {signals['trade_flow']:+.2f}",
        f"  • Price history data points: {signals['price_history_points']}",
        f"",
        f"Net adjustment from signals: {adjustment:+.4f}",
    ]

    # Add plain-English interpretation
    if pred_prob > 0.7:
        lines.append(f"\nAssessment: HIGH probability event. Market strongly expects YES.")
    elif pred_prob > 0.5:
        lines.append(f"\nAssessment: Moderate probability. Slight lean toward YES.")
    elif pred_prob > 0.3:
        lines.append(f"\nAssessment: LOW probability. Market leans toward NO.")
    else:
        lines.append(f"\nAssessment: VERY LOW probability. Market strongly expects NO.")

    if market.end_date:
        lines.append(f"Resolves: {market.end_date[:10]}")

    return "\n".join(lines)


def analyze_orderbook_depth(market: Market, cents_range: float = 0.10) -> str:
    """
    Detailed order book depth analysis showing bid/ask strength at each
    price level within ± cents_range of the mid-price.

    Returns a formatted string report.
    """
    if not market.clob_token_ids:
        return "No CLOB token IDs available for this market."

    yes_token = market.clob_token_ids[0]
    depth = polyapi.get_orderbook_depth(yes_token)

    if not depth:
        return "Order book data unavailable."

    mid = (depth.get("best_bid", 0) + depth.get("best_ask", 1)) / 2
    best_bid = depth.get("best_bid")
    best_ask = depth.get("best_ask")
    spread = depth.get("spread")
    imbalance = depth.get("imbalance", 0)
    total_bid = depth.get("total_bid_notional", 0)
    total_ask = depth.get("total_ask_notional", 0)

    lines = [
        f"Order Book Depth — {market.question[:55]}",
        f"{'─'*65}",
        f"  Best Bid:  {best_bid:.3f} ({best_bid*100:.1f}¢)" if best_bid else "  Best Bid:  N/A",
        f"  Best Ask:  {best_ask:.3f} ({best_ask*100:.1f}¢)" if best_ask else "  Best Ask:  N/A",
        f"  Spread:    {spread*100:.2f}¢  |  Mid: {mid*100:.2f}¢" if spread else "  Spread:    N/A",
        f"  Imbalance: {imbalance:+.3f}  ({'BUY pressure' if imbalance > 0.05 else 'SELL pressure' if imbalance < -0.05 else 'balanced'})",
        f"  Total Bids: ${total_bid:,.0f}   Total Asks: ${total_ask:,.0f}",
        "",
    ]

    # Build price-level table within range
    low = max(0.01, mid - cents_range)
    high = min(0.99, mid + cents_range)

    # Collect all levels in range
    bid_map = {round(b["price"], 3): b for b in depth.get("bids", [])
               if low <= b["price"] <= high}
    ask_map = {round(a["price"], 3): a for a in depth.get("asks", [])
               if low <= a["price"] <= high}

    all_prices = sorted(
        set(list(bid_map.keys()) + list(ask_map.keys())),
        reverse=True
    )

    if not all_prices:
        # Fall back to showing best 5 bids and asks
        lines.append("  Top 5 Bids:")
        for b in depth.get("bids", [])[:5]:
            bar = "█" * min(int(b["size"] / 50), 20)
            lines.append(f"    {b['price']*100:5.1f}¢  ${b['size']:>8,.0f}  {bar}")
        lines.append("")
        lines.append("  Top 5 Asks:")
        for a in depth.get("asks", [])[:5]:
            bar = "█" * min(int(a["size"] / 50), 20)
            lines.append(f"    {a['price']*100:5.1f}¢  ${a['size']:>8,.0f}  {bar}")
    else:
        max_size = max(
            [b["size"] for b in bid_map.values()] +
            [a["size"] for a in ask_map.values()] + [1]
        )
        header = f"  {'Price':>6}  {'Bid $':>9}  {'Ask $':>9}  {'Side':>5}  Visual"
        lines.append(header)
        lines.append("  " + "─" * 58)

        for price in all_prices:
            bid_lvl = bid_map.get(price)
            ask_lvl = ask_map.get(price)
            is_mid = abs(price - mid) < 0.005

            if bid_lvl:
                bar_len = int((bid_lvl["size"] / max_size) * 25)
                bar = "▓" * bar_len
                mid_marker = " ← mid" if is_mid else ""
                lines.append(
                    f"  {price*100:6.2f}¢  ${bid_lvl['size']:>8,.0f}  {'':>9}   BID  {bar}{mid_marker}"
                )
            if ask_lvl:
                bar_len = int((ask_lvl["size"] / max_size) * 25)
                bar = "░" * bar_len
                mid_marker = " ← mid" if is_mid else ""
                lines.append(
                    f"  {price*100:6.2f}¢  {'':>9}  ${ask_lvl['size']:>8,.0f}   ASK  {bar}{mid_marker}"
                )

    # Interpretation
    lines.append("")
    lines.append("  Interpretation:")
    if imbalance > 0.15:
        lines.append("  Strong BUY pressure — more $ sitting on bid side.")
        lines.append("  Price likely to be supported / drift upward.")
    elif imbalance < -0.15:
        lines.append("  Strong SELL pressure — more $ sitting on ask side.")
        lines.append("  Price may face resistance / drift downward.")
    else:
        lines.append("  Book is roughly balanced. Price likely to consolidate near mid.")

    if spread and spread < 0.02:
        lines.append(f"  Tight spread ({spread*100:.2f}¢) → high liquidity, efficient pricing.")
    elif spread and spread > 0.05:
        lines.append(f"  Wide spread ({spread*100:.2f}¢) → thin market, larger edge for market makers.")

    return "\n".join(lines)


def refine_weights(predictions: list, current_weights: dict) -> dict:
    """
    Simple gradient-based weight refinement using resolved predictions.
    Minimizes Brier score by adjusting signal weights.

    Returns updated weights dict.
    """
    resolved = [p for p in predictions if p.outcome is not None and p.score is not None]
    if len(resolved) < 3:
        print(f"  Need at least 3 resolved predictions to refine (have {len(resolved)}).")
        return current_weights

    new_weights = dict(current_weights)
    learning_rate = 0.05

    for pred in resolved:
        actual = 1.0 if pred.outcome == "YES" else 0.0
        error = pred.predicted_yes_prob - actual  # positive = over-estimated YES

        signals = pred.signals
        market_price = signals.get("market_price", 0.5)
        baseline_error = market_price - actual

        # If our prediction was worse than market price, reduce non-market weights
        if abs(error) > abs(baseline_error):
            # Our signals hurt — shrink non-market weights
            for key in new_weights:
                if key != "market_price":
                    new_weights[key] *= (1 - learning_rate * 0.5)
            new_weights["market_price"] = min(0.85, new_weights["market_price"] * (1 + learning_rate * 0.3))
        else:
            # Our signals helped — slightly boost signal confidence
            momentum_direction = signals.get("momentum", 0)
            if (momentum_direction > 0 and error < 0) or (momentum_direction < 0 and error > 0):
                new_weights["momentum"] = min(0.3, new_weights["momentum"] * (1 + learning_rate))
            trade_flow = signals.get("trade_flow", 0)
            if (trade_flow > 0 and error < 0) or (trade_flow < 0 and error > 0):
                new_weights["trade_flow"] = min(0.2, new_weights["trade_flow"] * (1 + learning_rate))

    # Re-normalize so weights still conceptually sum to ~1
    total = sum(new_weights.values())
    new_weights = {k: v / total for k, v in new_weights.items()}

    return new_weights
