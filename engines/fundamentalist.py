"""
Engine 4: The Fundamentalist
==============================
Philosophy: Trust the full analysis model. Bet where the model disagrees with the market.
Uses the multi-signal prediction engine (momentum + volume + liquidity + time + orderbook + trade flow).
Theory: The composite model sees what individual signals miss.
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import analyzer as analyzermod
import tracker

ENGINE_NAME = "fundamentalist"
STRATEGY = "Trust the multi-signal model. Bet where model disagrees with market price by >3%."
BUDGET = 1000.0
MIN_EDGE = 0.03  # minimum 3% edge to bet


def place_bets(markets):
    """Returns list of bet dicts."""
    bets = []
    raw_weights = []
    weights = tracker.load_weights()

    for m in markets:
        try:
            pred = analyzermod.analyze_market(m, weights)
        except Exception:
            continue

        predicted = pred.predicted_yes_prob
        market_price = m.yes_price
        edge = predicted - market_price

        if abs(edge) < MIN_EDGE:
            continue

        if edge > 0:
            side = "YES"  # model thinks YES is underpriced
            bet_edge = edge
        else:
            side = "NO"  # model thinks NO is underpriced
            bet_edge = abs(edge)

        # Weight by edge * confidence
        weight = bet_edge * pred.confidence
        raw_weights.append((m, side, weight))

    if not raw_weights:
        # If no edges found, spread evenly
        for m in markets:
            raw_weights.append((m, "YES" if m.yes_price < 0.50 else "NO", 0.05))

    # Normalize to budget
    total_weight = sum(w for _, _, w in raw_weights) or 1
    for m, side, weight in raw_weights:
        amount = round((weight / total_weight) * BUDGET, 2)
        if amount < 1:
            continue
        entry_price = m.yes_price if side == "YES" else m.no_price
        if entry_price <= 0.01:
            continue
        shares = amount / entry_price

        bets.append({
            "engine_name": ENGINE_NAME,
            "market_id": m.id,
            "market_question": m.question,
            "side": side,
            "amount": amount,
            "entry_price": round(entry_price, 4),
            "shares": round(shares, 4),
        })

    return bets
