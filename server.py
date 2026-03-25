"""
FastAPI server for Polymarket Analyzer.
Serves the web dashboard and exposes API endpoints.
"""
import os
import sys
from pathlib import Path
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

# Ensure our modules are importable
sys.path.insert(0, str(Path(__file__).parent))

import api as polyapi
import analyzer as analyzermod
import tracker

app = FastAPI(title="Polymarket Analyzer", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── API Routes ────────────────────────────────────────────────────────────────

@app.get("/api/market/iran")
def get_iran_market():
    """Fetch the Iran Dec 31 market data."""
    market = polyapi.find_iran_boots_market()
    if not market:
        raise HTTPException(status_code=404, detail="Market not found")
    return {
        "id": market.id,
        "question": market.question,
        "slug": market.slug,
        "yes_price": market.yes_price,
        "no_price": market.no_price,
        "volume": market.volume,
        "liquidity": market.liquidity,
        "end_date": market.end_date,
        "active": market.active,
        "closed": market.closed,
        "outcomes": market.outcomes,
        "outcome_prices": market.outcome_prices,
    }


@app.get("/api/market/{market_id}")
def get_market(market_id: str):
    """Fetch any market by ID."""
    market = polyapi.get_market_by_id(market_id)
    if not market:
        raise HTTPException(status_code=404, detail="Market not found")
    return market.__dict__


@app.get("/api/analysis/iran")
def analyze_iran():
    """Run full analysis on the Iran market."""
    market = polyapi.find_iran_boots_market()
    if not market:
        raise HTTPException(status_code=404, detail="Market not found")
    weights = tracker.load_weights()
    pred = analyzermod.analyze_market(market, weights)
    return {
        "market": {
            "question": market.question,
            "yes_price": market.yes_price,
            "volume": market.volume,
            "liquidity": market.liquidity,
            "end_date": market.end_date,
        },
        "prediction": {
            "predicted_yes_prob": pred.predicted_yes_prob,
            "confidence": pred.confidence,
            "reasoning": pred.reasoning,
        },
        "signals": pred.signals,
    }


@app.get("/api/orderbook/iran")
def get_iran_orderbook(range: float = 0.12):
    """Get order book depth for the Iran market."""
    market = polyapi.find_iran_boots_market()
    if not market or not market.clob_token_ids:
        raise HTTPException(status_code=404, detail="Market or order book not found")

    yes_token = market.clob_token_ids[0]
    depth = polyapi.get_orderbook_depth(yes_token)

    if not depth:
        raise HTTPException(status_code=503, detail="Order book unavailable")

    mid = ((depth.get("best_bid") or 0.5) + (depth.get("best_ask") or 0.5)) / 2
    low = max(0.01, mid - range)
    high = min(0.99, mid + range)

    filtered_bids = [b for b in depth.get("bids", []) if low <= b["price"] <= high]
    filtered_asks = [a for a in depth.get("asks", []) if low <= a["price"] <= high]

    return {
        "best_bid": depth.get("best_bid"),
        "best_ask": depth.get("best_ask"),
        "spread": depth.get("spread"),
        "imbalance": depth.get("imbalance"),
        "total_bid_notional": depth.get("total_bid_notional"),
        "total_ask_notional": depth.get("total_ask_notional"),
        "bids": filtered_bids,
        "asks": filtered_asks,
    }


@app.get("/api/price-history/iran")
def get_iran_price_history():
    """Get price history for the Iran market."""
    market = polyapi.find_iran_boots_market()
    if not market or not market.clob_token_ids:
        raise HTTPException(status_code=404, detail="No token IDs")
    yes_token = market.clob_token_ids[0]
    history = polyapi.get_price_history(yes_token, interval="max", fidelity=60)
    return {"history": history}


@app.post("/api/predict/iran")
def predict_iran(background_tasks: BackgroundTasks):
    """Run analysis and store a prediction for the Iran market."""
    market = polyapi.find_iran_boots_market()
    if not market:
        raise HTTPException(status_code=404, detail="Market not found")
    weights = tracker.load_weights()
    pred = analyzermod.analyze_market(market, weights)
    background_tasks.add_task(tracker.store_prediction, pred)
    return {
        "stored": True,
        "predicted_yes_prob": pred.predicted_yes_prob,
        "confidence": pred.confidence,
        "timestamp": pred.timestamp,
    }


@app.get("/api/predictions")
def get_predictions():
    """Get all stored predictions."""
    preds = tracker.load_predictions()
    return {"predictions": [p.to_dict() for p in preds]}


@app.post("/api/resolve")
def resolve_predictions():
    """Check and resolve pending predictions."""
    count = tracker.check_and_resolve_pending()
    return {"resolved": count}


@app.get("/api/performance")
def get_performance():
    """Get prediction performance report."""
    report = tracker.generate_performance_report()
    return report.to_dict()


@app.post("/api/refine")
def refine_weights():
    """Refine algorithm weights from resolved predictions."""
    predictions = tracker.load_predictions()
    current = tracker.load_weights()
    new_weights = analyzermod.refine_weights(predictions, current)
    tracker.save_weights(new_weights)
    return {
        "previous": current,
        "updated": new_weights,
        "delta": {k: round(new_weights[k] - current[k], 5) for k in current},
    }


@app.get("/api/search")
def search_markets(q: str, limit: int = 10):
    """Search for markets by keyword."""
    markets = polyapi.search_markets(q, limit=limit)
    return {"markets": [m.__dict__ for m in markets]}


@app.get("/api/watchlist")
def get_watchlist():
    return {"watchlist": tracker.load_watchlist()}


# ── Static files + Dashboard ──────────────────────────────────────────────────

app.mount("/static", StaticFiles(directory=Path(__file__).parent / "static"), name="static")

@app.get("/", response_class=HTMLResponse)
def dashboard():
    with open(Path(__file__).parent / "static" / "index.html") as f:
        return f.read()


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("server:app", host="0.0.0.0", port=port, reload=False)
