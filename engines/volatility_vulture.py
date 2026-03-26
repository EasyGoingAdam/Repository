"""
Engine 3: The Volatility Vulture
=================================
Philosophy: Volatile markets have the most mispricing opportunity.
Big bets on high-volatility markets (using orderbook imbalance for direction).
Skip stable markets.
Theory: Where there's chaos, there's profit.
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import api as polyapi
from volatility import compute_volatility

ENGINE_NAME = "volatility_vulture"
STRATEGY = "Big bets on volatile markets, use orderbook imbalance for direction. Skip stable markets."
BUDGET = 1000.0


def place_bets(markets):
    """Returns list of bet dicts."""
    bets = []
    raw_weights = []

    for m in markets:
        # Compute volatility
        vol_metric = compute_volatility(m.id, window_minutes=1440)
        std_dev = vol_metric.get("std_dev", 0) if vol_metric else 0

        if std_dev < 0.005:
            # Too stable — skip
            continue

        # Direction: use orderbook imbalance
        side = "YES"
        if m.clob_token_ids:
            try:
                depth = polyapi.get_orderbook_depth(m.clob_token_ids[0])
                imbalance = depth.get("imbalance", 0)
                if imbalance < -0.05:
                    side = "NO"
                elif imbalance > 0.05:
                    side = "YES"
                else:
                    # Neutral imbalance — bet on the cheaper side
                    side = "YES" if m.yes_price < 0.50 else "NO"
            except Exception:
                side = "YES" if m.yes_price < 0.50 else "NO"

        # Weight by volatility (more volatile = bigger bet)
        weight = std_dev * 10  # scale up
        raw_weights.append((m, side, weight))

    if not raw_weights:
        return bets

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
