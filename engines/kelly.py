"""
Engine 5: The Kelly Criterion
==============================
Philosophy: Mathematically optimal bet sizing. Aggressive where edge is large.
Uses half-Kelly to reduce variance. Concentrates capital on highest-edge bets.
Theory: Maximize long-term growth rate via information-theoretic optimal sizing.
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import analyzer as analyzermod
import tracker

ENGINE_NAME = "kelly"
STRATEGY = "Half-Kelly optimal sizing. Concentrates bets where model edge is largest."
BUDGET = 1000.0
MIN_KELLY = 0.02  # minimum Kelly fraction to bet


def place_bets(markets):
    """Returns list of bet dicts."""
    bets = []
    kelly_bets = []
    weights = tracker.load_weights()

    for m in markets:
        try:
            pred = analyzermod.analyze_market(m, weights)
        except Exception:
            continue

        predicted = pred.predicted_yes_prob

        # Calculate Kelly for YES side
        yes_price = m.yes_price
        no_price = m.no_price

        # YES bet: p=predicted_yes_prob, odds = 1/yes_price - 1
        if yes_price > 0.01 and yes_price < 0.99:
            b_yes = (1.0 / yes_price) - 1.0  # decimal odds for YES
            p_yes = predicted
            q_yes = 1.0 - p_yes
            kelly_yes = (p_yes * b_yes - q_yes) / b_yes if b_yes > 0 else 0

            # NO bet: p=1-predicted_yes_prob, odds = 1/no_price - 1
            b_no = (1.0 / no_price) - 1.0 if no_price > 0.01 else 0
            p_no = 1.0 - predicted
            q_no = predicted
            kelly_no = (p_no * b_no - q_no) / b_no if b_no > 0 else 0

            # Pick the side with higher Kelly fraction
            if kelly_yes > kelly_no and kelly_yes > MIN_KELLY:
                half_kelly = kelly_yes / 2.0
                kelly_bets.append((m, "YES", half_kelly, yes_price))
            elif kelly_no > MIN_KELLY:
                half_kelly = kelly_no / 2.0
                kelly_bets.append((m, "NO", half_kelly, no_price))

    if not kelly_bets:
        # Fallback: equal spread
        per_market = BUDGET / max(len(markets), 1)
        for m in markets:
            side = "YES" if m.yes_price < 0.50 else "NO"
            entry_price = m.yes_price if side == "YES" else m.no_price
            if entry_price <= 0.01:
                continue
            bets.append({
                "engine_name": ENGINE_NAME,
                "market_id": m.id,
                "market_question": m.question,
                "side": side,
                "amount": round(per_market, 2),
                "entry_price": round(entry_price, 4),
                "shares": round(per_market / entry_price, 4),
            })
        return bets

    # Normalize Kelly fractions to budget
    total_kelly = sum(k for _, _, k, _ in kelly_bets) or 1
    for m, side, kelly_frac, entry_price in kelly_bets:
        amount = round((kelly_frac / total_kelly) * BUDGET, 2)
        if amount < 1:
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
