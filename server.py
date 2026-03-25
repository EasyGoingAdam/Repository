"""
FastAPI server — Polymarket Analyzer + First Strike Intelligence System
"""
import os
import sys
import asyncio
import threading
import time
from pathlib import Path
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

sys.path.insert(0, str(Path(__file__).parent))

import api as polyapi
import analyzer as analyzermod
import tracker
from intelligence.signals import init_db, get_recent_signals, get_all_signals, get_command_state
from intelligence.pulse import run_pulse_cycle
from intelligence.beacon import run_beacon_cycle
from intelligence.command import run_command_cycle, generate_market_signal, generate_orderflow_signal
from intelligence.signals import store_signal

app = FastAPI(title="First Strike Intelligence", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Startup ───────────────────────────────────────────────────────────────────

@app.on_event("startup")
async def startup():
    init_db()
    # Kick off background intelligence loop
    threading.Thread(target=_intel_loop, daemon=True).start()


def _intel_loop():
    """Background thread: runs intelligence cycles on a schedule."""
    time.sleep(5)  # let server fully start first
    while True:
        try:
            # Market + orderflow signals every 2 minutes
            market = polyapi.find_iran_boots_market()
            if market:
                ms = generate_market_signal(market)
                if ms:
                    store_signal(ms)
                ofs = generate_orderflow_signal(market)
                if ofs:
                    store_signal(ofs)

            # News (Beacon) every 10 minutes
            if int(time.time()) % 600 < 120:
                run_beacon_cycle()

            # Twitter (Pulse) every 5 minutes if key present
            if int(time.time()) % 300 < 120:
                run_pulse_cycle()

            # Command aggregation every 2 minutes
            run_command_cycle()

        except Exception as e:
            print(f"Intel loop error: {e}")

        time.sleep(120)  # 2 minute base interval


# ── Existing Market API ───────────────────────────────────────────────────────

@app.get("/api/market/iran")
def get_iran_market():
    market = polyapi.find_iran_boots_market()
    if not market:
        raise HTTPException(status_code=404, detail="Market not found")
    return {
        "id": market.id, "question": market.question, "slug": market.slug,
        "yes_price": market.yes_price, "no_price": market.no_price,
        "volume": market.volume, "liquidity": market.liquidity,
        "end_date": market.end_date, "active": market.active, "closed": market.closed,
        "outcomes": market.outcomes, "outcome_prices": market.outcome_prices,
    }

@app.get("/api/market/{market_id}")
def get_market(market_id: str):
    market = polyapi.get_market_by_id(market_id)
    if not market:
        raise HTTPException(status_code=404, detail="Market not found")
    return market.__dict__

@app.get("/api/analysis/iran")
def analyze_iran():
    market = polyapi.find_iran_boots_market()
    if not market:
        raise HTTPException(status_code=404, detail="Market not found")
    weights = tracker.load_weights()
    pred = analyzermod.analyze_market(market, weights)
    return {
        "market": {
            "question": market.question, "yes_price": market.yes_price,
            "volume": market.volume, "liquidity": market.liquidity, "end_date": market.end_date,
        },
        "prediction": {
            "predicted_yes_prob": pred.predicted_yes_prob,
            "confidence": pred.confidence, "reasoning": pred.reasoning,
        },
        "signals": pred.signals,
    }

@app.get("/api/orderbook/iran")
def get_iran_orderbook(range: float = 0.12):
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
    return {
        "best_bid": depth.get("best_bid"), "best_ask": depth.get("best_ask"),
        "spread": depth.get("spread"), "imbalance": depth.get("imbalance"),
        "total_bid_notional": depth.get("total_bid_notional"),
        "total_ask_notional": depth.get("total_ask_notional"),
        "bids": [b for b in depth.get("bids", []) if low <= b["price"] <= high],
        "asks": [a for a in depth.get("asks", []) if low <= a["price"] <= high],
    }

@app.get("/api/price-history/iran")
def get_iran_price_history():
    market = polyapi.find_iran_boots_market()
    if not market or not market.clob_token_ids:
        raise HTTPException(status_code=404, detail="No token IDs")
    history = polyapi.get_price_history(market.clob_token_ids[0], interval="max", fidelity=60)
    return {"history": history}

@app.post("/api/predict/iran")
def predict_iran(background_tasks: BackgroundTasks):
    market = polyapi.find_iran_boots_market()
    if not market:
        raise HTTPException(status_code=404, detail="Market not found")
    weights = tracker.load_weights()
    pred = analyzermod.analyze_market(market, weights)
    background_tasks.add_task(tracker.store_prediction, pred)
    return {"stored": True, "predicted_yes_prob": pred.predicted_yes_prob,
            "confidence": pred.confidence, "timestamp": pred.timestamp}

@app.get("/api/predictions")
def get_predictions():
    preds = tracker.load_predictions()
    return {"predictions": [p.to_dict() for p in preds]}

@app.post("/api/resolve")
def resolve_predictions():
    count = tracker.check_and_resolve_pending()
    return {"resolved": count}

@app.get("/api/performance")
def get_performance():
    return tracker.generate_performance_report().to_dict()

@app.post("/api/refine")
def refine_weights():
    predictions = tracker.load_predictions()
    current = tracker.load_weights()
    new_weights = analyzermod.refine_weights(predictions, current)
    tracker.save_weights(new_weights)
    return {"previous": current, "updated": new_weights,
            "delta": {k: round(new_weights[k] - current[k], 5) for k in current}}

@app.get("/api/search")
def search_markets(q: str, limit: int = 10):
    markets = polyapi.search_markets(q, limit=limit)
    return {"markets": [m.__dict__ for m in markets]}

@app.get("/api/watchlist")
def get_watchlist():
    return {"watchlist": tracker.load_watchlist()}


# ── First Strike Command API ──────────────────────────────────────────────────

@app.get("/api/command/state")
def get_command_state_api():
    """Get current aggregated command state."""
    return get_command_state()


@app.get("/api/command/signals")
def get_signals(hours: int = 24, source: str = None, limit: int = 150):
    """Get recent intelligence signals."""
    signals = get_recent_signals(hours=hours, source=source, limit=limit)
    return {"signals": signals, "count": len(signals)}


@app.get("/api/command/signals/all")
def get_all_signals_api(limit: int = 200):
    """Get all stored signals."""
    signals = get_all_signals(limit=limit)
    return {"signals": signals, "count": len(signals)}


@app.post("/api/command/run")
def run_command(background_tasks: BackgroundTasks):
    """Manually trigger a full intelligence cycle."""
    background_tasks.add_task(_full_cycle)
    return {"triggered": True}


def _full_cycle():
    try:
        market = polyapi.find_iran_boots_market()
        if market:
            ms = generate_market_signal(market)
            if ms:
                store_signal(ms)
            ofs = generate_orderflow_signal(market)
            if ofs:
                store_signal(ofs)
        run_beacon_cycle()
        run_pulse_cycle()
        run_command_cycle()
    except Exception as e:
        print(f"Full cycle error: {e}")


@app.post("/api/command/pulse")
def run_pulse(background_tasks: BackgroundTasks):
    """Trigger Twitter/Pulse scan."""
    background_tasks.add_task(run_pulse_cycle)
    return {"triggered": True}


@app.post("/api/command/beacon")
def run_beacon(background_tasks: BackgroundTasks):
    """Trigger News/Beacon scan."""
    background_tasks.add_task(run_beacon_cycle)
    return {"triggered": True}


@app.get("/api/command/status")
def get_system_status():
    """Health check + system status for all 5 apps."""
    from intelligence.signals import get_recent_signals as grs
    import os
    counts = {}
    for app in ["pulse", "beacon", "market", "orderflow"]:
        counts[app] = len(grs(hours=1, source=app))
    return {
        "status": "online",
        "twitter_configured": bool(os.environ.get("TWITTER_API_KEY")),
        "signal_counts_1h": counts,
        "apps": {
            "pulse": "active" if counts["pulse"] > 0 else "standby",
            "beacon": "active" if counts["beacon"] > 0 else "standby",
            "market": "active" if counts["market"] > 0 else "standby",
            "orderflow": "active" if counts["orderflow"] > 0 else "standby",
            "command": "online",
        }
    }


# ── Pages ─────────────────────────────────────────────────────────────────────

app.mount("/static", StaticFiles(directory=Path(__file__).parent / "static"), name="static")

@app.get("/", response_class=HTMLResponse)
def dashboard():
    with open(Path(__file__).parent / "static" / "index.html") as f:
        return f.read()

@app.get("/command", response_class=HTMLResponse)
def command_center():
    with open(Path(__file__).parent / "static" / "command.html") as f:
        return f.read()


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("server:app", host="0.0.0.0", port=port, reload=False)
