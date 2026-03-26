"""
Engine 1: The Contrarian
========================
Philosophy: The crowd is wrong at the extremes. Bet AGAINST the market direction.
If YES > 0.55, bet NO. If YES < 0.45, bet YES.
Bet size proportional to distance from 0.50.
Theory: Markets overreact, prices revert to mean.
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import api as polyapi

ENGINE_NAME = "contrarian"
STRATEGY = "Bet AGAINST the crowd. Bigger bets on markets furthest from 50/50. Theory: mean reversion."
BUDGET = 1000.0


def place_bets(markets):
    """Returns list of bet dicts."""
    bets = []
    raw_weights = []

    for m in markets:
        price = m.yes_price
        distance = abs(price - 0.50)

        if price > 0.55:
            side = "NO"
            weight = distance
        elif price < 0.45:
            side = "YES"
            weight = distance
        else:
            # Near 50/50: use orderbook imbalance to pick side
            side = "YES"
            if m.clob_token_ids:
                try:
                    depth = polyapi.get_orderbook_depth(m.clob_token_ids[0])
                    if depth.get("imbalance", 0) < 0:
                        side = "NO"  # more sell pressure = contrarian buys NO
                except Exception:
                    pass
            weight = 0.05  # small bet on neutral markets

        raw_weights.append((m, side, weight))

    # Normalize weights to budget
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
