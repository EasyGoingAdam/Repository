"""
Engine 2: The Momentum Rider
=============================
Philosophy: Trends continue. Bet WITH the recent price direction.
Uses price history to detect momentum. Strong trend + high volume = bigger bet.
Theory: Smart money moves first, price hasn't fully adjusted yet.
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import api as polyapi
from analyzer import _compute_momentum

ENGINE_NAME = "momentum"
STRATEGY = "Bet WITH the trend. Rising prices = YES, falling = NO. Size by momentum strength."
BUDGET = 1000.0
MIN_MOMENTUM = 0.05


def place_bets(markets):
    """Returns list of bet dicts."""
    bets = []
    raw_weights = []

    for m in markets:
        momentum = 0.0
        if m.clob_token_ids:
            try:
                history = polyapi.get_price_history(m.clob_token_ids[0], interval="1w", fidelity=60)
                momentum = _compute_momentum(history, m.yes_price)
            except Exception:
                pass

        if abs(momentum) < MIN_MOMENTUM:
            # Flat market — small bet on YES (default)
            raw_weights.append((m, "YES", 0.02))
            continue

        if momentum > 0:
            side = "YES"
        else:
            side = "NO"

        # Weight by momentum strength
        weight = abs(momentum)
        raw_weights.append((m, side, weight))

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
