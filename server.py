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
from pydantic import BaseModel
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

sys.path.insert(0, str(Path(__file__).parent))

import api as polyapi
import analyzer as analyzermod
import tracker
from intelligence.signals import (
    init_db, get_recent_signals, get_all_signals, get_command_state,
    store_orderbook_snapshot, get_orderbook_snapshots
)
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
            # Market + orderflow signals + OB snapshot every 2 minutes
            market = polyapi.find_iran_boots_market()
            if market:
                ms = generate_market_signal(market)
                if ms:
                    store_signal(ms)
                if market.clob_token_ids:
                    depth = polyapi.get_orderbook_depth(market.clob_token_ids[0])
                    if depth:
                        store_orderbook_snapshot(depth, yes_price=market.yes_price)
                        ofs = generate_orderflow_signal(market)
                        if ofs:
                            store_signal(ofs)

            # News (Beacon) every 10 minutes
            if int(time.time()) % 600 < 120:
                run_beacon_cycle()

            # Twitter (Pulse) every cycle (~2 min) if key present
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


@app.get("/api/orderbook/history")
def get_orderbook_history(hours: int = 24, range: float = 0.10):
    """
    Return OB snapshots with bid/ask depth computed at the given ±range.
    Each snapshot includes: timestamp, yes_price, bid_depth, ask_depth,
    imbalance, spread — filtered to within ±range of the mid at that snapshot.
    Also returns current live depth for the range selector.
    """
    snapshots = get_orderbook_snapshots(hours=hours, limit=500)
    result = []
    for snap in snapshots:
        mid = ((snap.get("best_bid") or 0.5) + (snap.get("best_ask") or 0.5)) / 2
        low = max(0.01, mid - range)
        high = min(0.99, mid + range)

        bid_depth = sum(
            b["size"] * b["price"]
            for b in snap.get("bids", [])
            if low <= b.get("price", 0) <= high
        )
        ask_depth = sum(
            a["size"] * a["price"]
            for a in snap.get("asks", [])
            if low <= a.get("price", 0) <= high
        )
        total = bid_depth + ask_depth

        result.append({
            "timestamp": snap["timestamp"],
            "unix_ts": snap["unix_ts"],
            "yes_price": snap.get("yes_price"),
            "best_bid": snap.get("best_bid"),
            "best_ask": snap.get("best_ask"),
            "spread": snap.get("spread"),
            "bid_depth": round(bid_depth, 2),
            "ask_depth": round(ask_depth, 2),
            "total_depth": round(total, 2),
            "imbalance": round((bid_depth - ask_depth) / total, 4) if total else 0,
        })

    return {"snapshots": result, "count": len(result), "range": range}


class ChatRequest(BaseModel):
    question: str


@app.post("/api/beacon/chat")
def beacon_chat(req: ChatRequest):
    """
    Search all collected beacon (news) signals for articles relevant to the question.
    Keyword-matches against headline + raw_text, returns top matches + a synthesized answer.
    """
    import re as _re
    question = req.question.lower().strip()
    if not question:
        return {"answer": "Please enter a question.", "articles": [], "matches": 0}

    signals = get_recent_signals(hours=72, source="beacon", limit=500)

    stop = {"the","a","an","is","are","was","were","what","how","when","where","why",
            "do","does","did","and","or","but","in","on","at","to","for","of","with",
            "any","have","has","been","that","this","it","its","there","their","they"}
    words = set(_re.findall(r"[a-z]+", question)) - stop

    scored = []
    for sig in signals:
        text = ((sig.get("headline") or "") + " " + (sig.get("raw_text") or "")).lower()
        hits = sum(1 for w in words if w in text)
        if hits > 0:
            scored.append((hits, sig))
    scored.sort(key=lambda x: (-x[0], -(x[1].get("importance_score") or 0)))
    top = [s for _, s in scored[:6]]

    if not top:
        answer = (
            f"No recent news articles matched your query. "
            f"Beacon currently has {len(signals)} articles in the 72-hour window covering Iran/military topics. "
            f"Try broader keywords like 'troops', 'nuclear', 'sanctions', or 'diplomatic'."
        )
    else:
        bull  = sum(1 for s in top if s.get("signal_direction") == "bullish")
        bear  = sum(1 for s in top if s.get("signal_direction") == "bearish")
        neut  = len(top) - bull - bear
        sentiment = "escalatory" if bull > bear else "de-escalatory" if bear > bull else "mixed"

        # Extract unique sources
        def _src(r): return r.split("]")[0].lstrip("[") if r and r.startswith("[") else "GDELT"
        srcs = list(dict.fromkeys(_src(s.get("reasoning","")) for s in top))

        avg_imp = round(sum(s.get("importance_score",0) for s in top) / len(top))
        top_headline = top[0].get("headline","")

        answer = (
            f"Found <strong>{len(scored)} relevant article(s)</strong> matching your query "
            f"(showing top {len(top)}). "
            f"Coverage is predominantly <strong>{sentiment}</strong> "
            f"({bull} escalatory, {bear} de-escalatory, {neut} neutral). "
            f"Sources: <strong>{', '.join(srcs[:4])}</strong>. "
            f"Average importance score: <strong>{avg_imp}/100</strong>.<br><br>"
            f"📌 Most relevant: \"{top_headline}\""
        )

    return {"answer": answer, "articles": top, "matches": len(scored)}


@app.get("/api/orderbook/weighted")
def get_weighted_orderbook():
    """
    Returns OB with proximity-weighted depth.
    Weight formula: w = max(0, 1 - |price - mid| * 2)
    Orders far from mid get low weight; orders close to mid get high weight.
    """
    market = polyapi.find_iran_boots_market()
    if not market or not market.clob_token_ids:
        raise HTTPException(status_code=404, detail="Market not found")
    depth = polyapi.get_orderbook_depth(market.clob_token_ids[0])
    if not depth:
        raise HTTPException(status_code=503, detail="OB unavailable")

    best_bid = depth.get("best_bid") or 0.5
    best_ask = depth.get("best_ask") or 0.5
    mid = (best_bid + best_ask) / 2

    def weight(price: float) -> float:
        return max(0.0, 1.0 - abs(price - mid) * 2.0)

    w_bids, w_asks = [], []
    for b in depth.get("bids", []):
        w = weight(b["price"])
        raw_not = b.get("notional", b["size"] * b["price"])
        w_bids.append({**b, "weight": round(w, 4), "weighted_notional": round(raw_not * w, 2)})
    for a in depth.get("asks", []):
        w = weight(a["price"])
        raw_not = a.get("notional", a["size"] * a["price"])
        w_asks.append({**a, "weight": round(w, 4), "weighted_notional": round(raw_not * w, 2)})

    w_bid_total = sum(b["weighted_notional"] for b in w_bids)
    w_ask_total = sum(a["weighted_notional"] for a in w_asks)
    w_total = w_bid_total + w_ask_total or 1

    return {
        "mid": round(mid, 4),
        "weighted_bid_total": round(w_bid_total, 2),
        "weighted_ask_total": round(w_ask_total, 2),
        "weighted_imbalance": round((w_bid_total - w_ask_total) / w_total, 4),
        "raw_bid_total": depth.get("total_bid_notional", 0),
        "raw_ask_total": depth.get("total_ask_notional", 0),
        "bids": w_bids, "asks": w_asks,
    }


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

@app.get("/pulse", response_class=HTMLResponse)
def pulse_desk():
    with open(Path(__file__).parent / "static" / "desk-pulse.html") as f:
        return f.read()

@app.get("/beacon", response_class=HTMLResponse)
def beacon_desk():
    with open(Path(__file__).parent / "static" / "desk-beacon.html") as f:
        return f.read()

@app.get("/market-desk", response_class=HTMLResponse)
def market_desk():
    with open(Path(__file__).parent / "static" / "desk-market.html") as f:
        return f.read()

@app.get("/orderflow", response_class=HTMLResponse)
def orderflow_desk():
    with open(Path(__file__).parent / "static" / "desk-orderflow.html") as f:
        return f.read()


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("server:app", host="0.0.0.0", port=port, reload=False)
