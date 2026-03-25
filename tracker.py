"""
Prediction tracker — stores predictions, resolves them, scores performance.

Storage: JSON files in ./data/
  - predictions.json   : all predictions
  - weights.json       : current algorithm weights
  - watchlist.json     : markets being monitored
"""
import json
import os
from datetime import datetime, timezone
from typing import Optional
from models import Market, Prediction, PerformanceReport
import api as polyapi

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
PREDICTIONS_FILE = os.path.join(DATA_DIR, "predictions.json")
WEIGHTS_FILE = os.path.join(DATA_DIR, "weights.json")
WATCHLIST_FILE = os.path.join(DATA_DIR, "watchlist.json")

os.makedirs(DATA_DIR, exist_ok=True)


# ── Persistence ─────────────────────────────────────────────────────────────

def load_predictions() -> list[Prediction]:
    if not os.path.exists(PREDICTIONS_FILE):
        return []
    with open(PREDICTIONS_FILE) as f:
        data = json.load(f)
    return [Prediction.from_dict(d) for d in data]


def save_predictions(predictions: list[Prediction]):
    with open(PREDICTIONS_FILE, "w") as f:
        json.dump([p.to_dict() for p in predictions], f, indent=2)


def load_weights() -> dict:
    from analyzer import DEFAULT_WEIGHTS
    if not os.path.exists(WEIGHTS_FILE):
        return dict(DEFAULT_WEIGHTS)
    with open(WEIGHTS_FILE) as f:
        return json.load(f)


def save_weights(weights: dict):
    with open(WEIGHTS_FILE, "w") as f:
        json.dump(weights, f, indent=2)


def load_watchlist() -> list[dict]:
    if not os.path.exists(WATCHLIST_FILE):
        return []
    with open(WATCHLIST_FILE) as f:
        return json.load(f)


def save_watchlist(watchlist: list[dict]):
    with open(WATCHLIST_FILE, "w") as f:
        json.dump(watchlist, f, indent=2)


# ── Watchlist ────────────────────────────────────────────────────────────────

def add_to_watchlist(market: Market):
    watchlist = load_watchlist()
    ids = {w["id"] for w in watchlist}
    if market.id not in ids:
        watchlist.append({"id": market.id, "question": market.question,
                          "slug": market.slug, "added": datetime.now(timezone.utc).isoformat()})
        save_watchlist(watchlist)
        print(f"  Added to watchlist: {market.question}")
    else:
        print(f"  Already on watchlist: {market.question}")


def remove_from_watchlist(market_id: str):
    watchlist = load_watchlist()
    watchlist = [w for w in watchlist if w["id"] != market_id]
    save_watchlist(watchlist)


# ── Prediction Storage ────────────────────────────────────────────────────────

def store_prediction(pred: Prediction):
    predictions = load_predictions()
    predictions.append(pred)
    save_predictions(predictions)
    print(f"  Stored prediction #{len(predictions)} for: {pred.market_question[:60]}...")


def get_pending_predictions() -> list[Prediction]:
    return [p for p in load_predictions() if p.outcome is None]


def get_resolved_predictions() -> list[Prediction]:
    return [p for p in load_predictions() if p.outcome is not None]


# ── Resolution & Scoring ──────────────────────────────────────────────────────

def resolve_prediction(pred: Prediction, market: Market) -> Optional[Prediction]:
    """
    Check if a market has resolved and update the prediction outcome.
    Uses current market price as proxy when not formally closed:
      - price > 0.95 → YES resolved
      - price < 0.05 → NO resolved
    """
    current_price = market.yes_price

    if market.closed:
        outcome = "YES" if current_price > 0.5 else "NO"
    elif current_price > 0.95:
        outcome = "YES"
    elif current_price < 0.05:
        outcome = "NO"
    else:
        return None  # still pending

    pred.outcome = outcome
    pred.resolved_price = current_price
    pred.score = _brier_score(pred.predicted_yes_prob, outcome)
    return pred


def _brier_score(predicted_prob: float, outcome: str) -> float:
    """Brier score: (p - actual)^2. Lower = better. 0=perfect, 1=worst."""
    actual = 1.0 if outcome == "YES" else 0.0
    return round((predicted_prob - actual) ** 2, 4)


def check_and_resolve_pending():
    """Refresh all pending predictions against live market data."""
    predictions = load_predictions()
    pending = [p for p in predictions if p.outcome is None]
    resolved_count = 0

    for pred in pending:
        market = polyapi.get_market_by_id(pred.market_id)
        if not market:
            continue
        updated = resolve_prediction(pred, market)
        if updated:
            # Update in main list
            for i, p in enumerate(predictions):
                if p.market_id == pred.market_id and p.timestamp == pred.timestamp:
                    predictions[i] = updated
                    resolved_count += 1
                    break

    if resolved_count:
        save_predictions(predictions)
        print(f"  Resolved {resolved_count} prediction(s).")
    else:
        print("  No new resolutions.")
    return resolved_count


# ── Performance Report ────────────────────────────────────────────────────────

def generate_performance_report() -> PerformanceReport:
    predictions = load_predictions()
    resolved = [p for p in predictions if p.outcome is not None and p.score is not None]
    pending = [p for p in predictions if p.outcome is None]

    if not resolved:
        return PerformanceReport(
            total_predictions=len(predictions),
            resolved=0,
            pending=len(pending),
            avg_brier_score=0.0,
            calibration_error=0.0,
            accuracy=0.0,
            profit_loss_units=0.0,
            best_predictions=[],
            worst_predictions=[],
        )

    scores = [p.score for p in resolved]
    avg_brier = sum(scores) / len(scores)

    # Calibration: mean |predicted_prob - actual|
    cal_errors = []
    correct = 0
    pl = 0.0
    for p in resolved:
        actual = 1.0 if p.outcome == "YES" else 0.0
        cal_errors.append(abs(p.predicted_yes_prob - actual))
        if (p.predicted_yes_prob > 0.5) == (actual > 0.5):
            correct += 1
        # Hypothetical P&L: bet $1 on our predicted side at market price
        if p.predicted_yes_prob > 0.5:
            # Bet YES at market price
            odds = (1.0 / p.yes_price_at_prediction) - 1
            pl += odds if p.outcome == "YES" else -1.0
        else:
            # Bet NO at (1 - market_price)
            no_price = 1.0 - p.yes_price_at_prediction
            if no_price > 0:
                odds = (1.0 / no_price) - 1
                pl += odds if p.outcome == "NO" else -1.0

    calibration_error = sum(cal_errors) / len(cal_errors)
    accuracy = correct / len(resolved)

    sorted_by_score = sorted(resolved, key=lambda p: p.score)
    best = [p.to_dict() for p in sorted_by_score[:3]]
    worst = [p.to_dict() for p in sorted_by_score[-3:]]

    return PerformanceReport(
        total_predictions=len(predictions),
        resolved=len(resolved),
        pending=len(pending),
        avg_brier_score=round(avg_brier, 4),
        calibration_error=round(calibration_error, 4),
        accuracy=round(accuracy, 4),
        profit_loss_units=round(pl, 4),
        best_predictions=best,
        worst_predictions=worst,
    )
