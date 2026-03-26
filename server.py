"""
FastAPI server — Polymarket Analyzer + First Strike Intelligence System
"""
import os
import sys
import asyncio
import threading
import time
import uuid
from pathlib import Path
from fastapi import FastAPI, HTTPException, BackgroundTasks, Request
from pydantic import BaseModel
from fastapi.responses import HTMLResponse, JSONResponse

# ── Live viewer tracking ───────────────────────────────────────────────────────
_viewer_sessions: dict = {}   # session_id -> last_seen (epoch float)
_VIEWER_TIMEOUT = 90          # seconds of silence before a session expires
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

sys.path.insert(0, str(Path(__file__).parent))

import api as polyapi
import analyzer as analyzermod
import tracker
from intelligence.signals import (
    init_db, get_recent_signals, get_all_signals, get_command_state,
    store_orderbook_snapshot, get_orderbook_snapshots,
    get_managed_watchlist, get_all_latest_prices, get_price_snapshots,
    get_all_latest_volatility, get_active_trading_signals as db_get_active_signals,
    add_subscriber, get_active_subscribers, update_last_sms,
    add_email_subscriber, get_active_email_subscribers, update_last_email, unsubscribe_email,
)
from intelligence.pulse import run_pulse_cycle
from intelligence.beacon import run_beacon_cycle
from intelligence.command import run_command_cycle, generate_market_signal, generate_orderflow_signal
from intelligence.signals import store_signal
import discovery
import collector
import volatility as volmod
import signal_generator as siggen
import video_monitor as vidmon

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
    """Background thread: multi-market intelligence + data collection."""
    time.sleep(5)  # let server fully start first

    # Initial discovery on startup
    try:
        print("[Intel] Running initial market discovery...")
        discovery.refresh_watchlist()
        print("[Intel] Backfilling historical data...")
        result = collector.backfill_all_markets()
        print(f"[Intel] Backfill: {result['markets_filled']} markets, {result['total_points']} data points")
    except Exception as e:
        print(f"[Intel] Startup error: {e}")

    # Rebuild signal history from APIs after any cold start
    try:
        print("[Intel] Rebuilding signal history from Beacon (GDELT) + Pulse (Twitter)...")
        for _ in range(3):          # Run 3 beacon cycles immediately to grab recent news
            run_beacon_cycle()
        run_pulse_cycle()           # Run pulse once to seed Twitter signals
        run_command_cycle()
        print("[Intel] Signal rebuild complete.")
    except Exception as e:
        print(f"[Intel] Signal rebuild error: {e}")

    cycle = 0
    while True:
        try:
            # ── Every 2 minutes: collect prices + compute volatility + signals ──
            print(f"[Intel] Cycle {cycle}: collecting snapshots...")
            snap_result = collector.collect_all_snapshots()
            print(f"[Intel] Collected {snap_result['collected']} snapshots")

            volmod.compute_all_volatility()
            signals = siggen.generate_trading_signals()
            if signals:
                print(f"[Intel] Generated {len(signals)} trading signal(s)")

            # ── Legacy Iran market intelligence (keep existing) ──
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

            # ── Every 10 minutes (cycle % 5 == 0): evaluate signals + beacon ──
            if cycle % 5 == 0:
                eval_result = siggen.evaluate_past_signals()
                if eval_result["evaluated"]:
                    print(f"[Intel] Evaluated {eval_result['evaluated']} signals")
                run_beacon_cycle()

            # ── Every 60 minutes (cycle % 30 == 0): refresh watchlist ──
            if cycle % 30 == 0 and cycle > 0:
                discovery.refresh_watchlist()

            # Twitter (Pulse) + Command every cycle
            run_pulse_cycle()
            cmd_state = run_command_cycle()

            # ── SMS alert if URGENT or IMPORTANT ──────────────────────────────
            alert_lvl = cmd_state.get("alert_level", "")
            if alert_lvl in ("URGENT", "IMPORTANT"):
                top_sigs = cmd_state.get("top_signals", [])
                headline = top_sigs[0].get("headline", "") if top_sigs else cmd_state.get("reason_summary", "")
                send_alert_email(alert_lvl, headline, cmd_state.get("market_odds"))

            # ── Every 5 minutes (cycle % 2 == 1): video channel AI scan ──────
            if cycle % 2 == 1:
                try:
                    vid_result = vidmon.run_video_monitor_cycle(
                        send_sms_fn=send_alert_email,
                        store_signal_fn=store_signal,
                    )
                    if vid_result["alerts"]:
                        print(f"[Video] {vid_result['alerts']} channel alert(s), {vid_result['sms_sent']} SMS sent")
                except Exception as ve:
                    print(f"[Video] Monitor error: {ve}")

        except Exception as e:
            print(f"[Intel] Loop error: {e}")

        cycle += 1
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
    resolved = [p for p in predictions if p.outcome is not None]
    new_weights = analyzermod.refine_weights(predictions, current)
    tracker.save_weights(new_weights)
    return {
        "previous": current,
        "updated": new_weights,
        "delta": {k: round(new_weights[k] - current[k], 5) for k in current},
        "predictions_used": len(resolved),
    }

@app.get("/api/weights")
def get_weights():
    """Return current algorithm weights with descriptions."""
    weights = tracker.load_weights()
    DESCRIPTIONS = {
        "market_price":        "Baseline trust in the efficient market price",
        "momentum":            "1-week price trend direction",
        "volume_conviction":   "High volume = market consensus is stronger",
        "liquidity_quality":   "Deep order book = less manipulation risk",
        "time_decay":          "Proximity to resolution date",
        "orderbook_imbalance": "Proximity-weighted bid vs ask pressure",
        "trade_flow":          "Net direction of last 50 executed trades",
    }
    return {
        "weights": weights,
        "descriptions": DESCRIPTIONS,
        "total": round(sum(weights.values()), 4),
    }

@app.get("/api/desk-weights")
def get_desk_weights():
    """Return the per-desk credibility weights used by Command aggregation."""
    from intelligence.command import DESK_WEIGHTS
    DESK_DESCRIPTIONS = {
        "pulse":     "X/Twitter: highest — real-time official + analyst signal",
        "beacon":    "News/GDELT: second — credibility-scored journalism",
        "market":    "Polymarket price/momentum: crowd-sourced, lags events",
        "orderflow": "Order book depth: positioning intent, slowest to update",
    }
    return {
        "desk_weights": DESK_WEIGHTS,
        "descriptions": DESK_DESCRIPTIONS,
        "total": round(sum(DESK_WEIGHTS.values()), 4),
    }

@app.get("/api/user/position")
def get_user_position():
    """
    Fetch @easygoinga's Polymarket portfolio — all Iran-related positions.
    Wallet is hardcoded; override with POLYMARKET_WALLET env var if needed.
    """
    import requests as req
    from datetime import datetime, timezone as _tz

    WALLET = os.environ.get("POLYMARKET_WALLET", "0xC3907f00Ba3fD35ef96c3b9AFa1483B644af2a1F").strip()
    IRAN_KEYWORDS = ["iran", "irgc", "tehran", "persian", "hormuz", "fordow", "khamenei"]

    try:
        r = req.get(
            "https://data-api.polymarket.com/positions",
            params={"user": WALLET, "sizeThreshold": "0"},
            timeout=15,
        )
        if r.status_code != 200:
            return {"found": False, "error": f"Polymarket API {r.status_code}"}
        all_pos = r.json()
        if not isinstance(all_pos, list):
            return {"found": False, "error": "Unexpected response"}

        # Filter to Iran-related positions with meaningful size
        iran_pos = []
        for p in all_pos:
            title = (p.get("title") or "").lower()
            if any(kw in title for kw in IRAN_KEYWORDS) and float(p.get("size", 0)) >= 1.0:
                iran_pos.append(p)

        # Sort by |cashPnl| descending — most impactful first
        iran_pos.sort(key=lambda p: abs(float(p.get("cashPnl", 0))), reverse=True)

        # Build portfolio summary
        total_invested  = sum(float(p.get("initialValue", 0) or 0) for p in iran_pos)
        total_cur_val   = sum(float(p.get("currentValue", 0) or 0) for p in iran_pos)
        total_cash_pnl  = sum(float(p.get("cashPnl", 0) or 0) for p in iran_pos)
        total_realized  = sum(float(p.get("realizedPnl", 0) or 0) for p in iran_pos)
        total_positions = len(iran_pos)
        winning = sum(1 for p in iran_pos if float(p.get("cashPnl", 0)) > 0)
        losing  = sum(1 for p in iran_pos if float(p.get("cashPnl", 0)) < 0)

        # Top positions (up to 8)
        top = []
        for p in iran_pos[:8]:
            size    = float(p.get("size", 0))
            avg     = float(p.get("avgPrice", 0) or 0)
            cur     = float(p.get("curPrice", 0) or 0)
            pnl     = float(p.get("cashPnl", 0) or 0)
            outcome = (p.get("outcome") or "YES").upper()
            top.append({
                "title":     p.get("title", ""),
                "outcome":   outcome,
                "shares":    round(size, 2),
                "avg_price": round(avg, 4),
                "cur_price": round(cur, 4),
                "cash_pnl":  round(pnl, 2),
                "redeemable": bool(p.get("redeemable")),
                "end_date":  p.get("endDate", ""),
            })

        return {
            "found":           True,
            "wallet":          WALLET[:6] + "..." + WALLET[-4:],
            "summary": {
                "total_positions":  total_positions,
                "total_invested":   round(total_invested, 2),
                "current_value":    round(total_cur_val, 2),
                "unrealized_pnl":   round(total_cash_pnl, 2),
                "realized_pnl":     round(total_realized, 2),
                "total_pnl":        round(total_cash_pnl + total_realized, 2),
                "winning":          winning,
                "losing":           losing,
            },
            "top_positions":   top,
            "fetched_at":      datetime.now(_tz.utc).isoformat(),
        }
    except Exception as e:
        return {"found": False, "error": str(e)}

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


@app.get("/api/export/signals")
def export_all_signals():
    """Export every stored signal as JSON — use to back up before a redeploy."""
    signals = get_all_signals(limit=10000)
    return JSONResponse(
        content={"signals": signals, "count": len(signals),
                 "exported_at": datetime.now(timezone.utc).isoformat()},
        headers={"Content-Disposition": "attachment; filename=signals_export.json"},
    )


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


@app.get("/api/pulse/debug")
def pulse_debug():
    """
    Live diagnostic: calls twitterapi.io directly and returns raw response.
    Use this to verify API key + endpoint are working.
    """
    import os
    import requests as req
    key = os.environ.get("TWITTER_API_KEY", "")
    if not key:
        return {"error": "TWITTER_API_KEY env var not set", "configured": False}

    results = {}
    # Test search endpoint
    try:
        r = req.get(
            "https://api.twitterapi.io/twitter/tweet/advanced_search",
            headers={"X-API-Key": key},
            params={"query": "iran military", "queryType": "Latest"},
            timeout=15,
        )
        results["search"] = {
            "status_code": r.status_code,
            "tweet_count": len(r.json().get("tweets", [])) if r.status_code == 200 else 0,
            "raw_preview": r.text[:400],
        }
    except Exception as e:
        results["search"] = {"error": str(e)}

    # Test user info endpoint
    try:
        r2 = req.get(
            "https://api.twitterapi.io/twitter/user/info",
            headers={"X-API-Key": key},
            params={"userName": "CENTCOM"},
            timeout=10,
        )
        results["user_info"] = {
            "status_code": r2.status_code,
            "raw_preview": r2.text[:400],
        }
    except Exception as e:
        results["user_info"] = {"error": str(e)}

    return {"configured": True, "key_preview": key[:8] + "...", "results": results}


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


# ── Multi-Market API ──────────────────────────────────────────────────────────

@app.get("/api/discovery/refresh")
def api_discovery_refresh():
    result = discovery.refresh_watchlist()
    return result


@app.get("/api/watchlist/managed")
def api_managed_watchlist():
    watchlist = get_managed_watchlist()
    latest_prices = {p["market_id"]: p for p in get_all_latest_prices()}
    for w in watchlist:
        price_info = latest_prices.get(w["market_id"], {})
        w["latest_yes_price"] = price_info.get("yes_price")
        w["latest_snapshot_at"] = price_info.get("timestamp")
    return {"watchlist": watchlist, "count": len(watchlist)}


@app.post("/api/watchlist/add/{market_id}")
def api_add_to_watchlist(market_id: str):
    ok = discovery.add_market_manually(market_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Market not found")
    return {"added": True, "market_id": market_id}


@app.delete("/api/watchlist/remove/{market_id}")
def api_remove_from_watchlist(market_id: str):
    from intelligence.signals import deactivate_watchlist_entry
    deactivate_watchlist_entry(market_id)
    return {"removed": True, "market_id": market_id}


@app.get("/api/prices/{market_id}")
def api_price_history(market_id: str, hours: int = 24):
    snapshots = get_price_snapshots(market_id, hours=hours)
    return {"market_id": market_id, "snapshots": snapshots, "count": len(snapshots)}


@app.get("/api/prices/all/latest")
def api_all_latest_prices():
    prices = get_all_latest_prices()
    return {"prices": prices, "count": len(prices)}


@app.get("/api/volatility/iran/summary")
def api_iran_volatility_summary():
    """
    Comprehensive volatility summary for the Iran YES (boots on ground) market.
    Returns high/low/range/change across 1h, 8h, 24h, 7d windows
    plus Bollinger bands, z-score, and a limit-price recommendation.
    """
    import statistics as _stats
    from datetime import datetime as _dt, timezone as _tz, timedelta as _td

    market = polyapi.find_iran_boots_market()
    if not market:
        raise HTTPException(status_code=404, detail="Market not found")

    # Resolve market_id: look for the boots/forces-enter market in the watchlist.
    # Try multiple keyword matches ranked by specificity.
    watchlist = get_managed_watchlist()
    BOOTS_KEYWORDS = ["forces enter", "boots", "ground troops", "military enter"]
    IRAN_KEYWORDS  = ["iran"]
    iran_entry = None
    for kw in BOOTS_KEYWORDS:
        iran_entry = next((w for w in watchlist
                           if kw in (w.get("question","") + w.get("slug","")).lower()), None)
        if iran_entry:
            break
    if not iran_entry:
        iran_entry = next((w for w in watchlist
                           if any(k in (w.get("question","") + w.get("slug","")).lower()
                                  for k in IRAN_KEYWORDS)), None)
    market_id = iran_entry["market_id"] if iran_entry else str(market.id)

    # Fetch 7 days of price snapshots; also try the API market.id as fallback
    all_snaps = get_price_snapshots(market_id, hours=168)
    if not all_snaps and str(market.id) != market_id:
        all_snaps = get_price_snapshots(str(market.id), hours=168)

    now = _dt.now(_tz.utc)

    # Always inject the current live price as the most recent point so windows
    # reflect real-time reality even if the collector hasn't snapped yet.
    live_snap = {
        "timestamp": now.isoformat(),
        "yes_price": market.yes_price,
    }
    all_snaps = list(all_snaps) + [live_snap]

    def _window(hours: int):
        cutoff = now - _td(hours=hours)
        pts = []
        for s in all_snaps:
            ts_raw = s.get("timestamp", "")
            try:
                ts = _dt.fromisoformat(ts_raw.replace("Z", "+00:00"))
                if ts >= cutoff and s.get("yes_price") is not None:
                    pts.append({"ts": ts, "p": float(s["yes_price"])})
            except Exception:
                pass
        if not pts:
            return None
        prices = [x["p"] for x in pts]
        mean = sum(prices) / len(prices)
        std  = _stats.stdev(prices) if len(prices) > 1 else 0.0
        first, last = prices[0], prices[-1]
        chg = last - first
        return {
            "high":             round(max(prices) * 100, 2),
            "low":              round(min(prices) * 100, 2),
            "mean":             round(mean * 100, 2),
            "std_dev_pct":      round(std * 100, 2),
            "range_pct":        round((max(prices) - min(prices)) * 100, 2),
            "change_pct":       round(chg * 100, 2),
            "bollinger_upper":  round((mean + 2 * std) * 100, 2),
            "bollinger_lower":  round(max(0, mean - 2 * std) * 100, 2),
            "z_score":          round((last - mean) / std, 3) if std > 0.001 else 0.0,
            "points":           len(prices),
        }

    windows = {
        "1h":  _window(1),
        "8h":  _window(8),
        "24h": _window(24),
        "7d":  _window(168),
    }

    cur = market.yes_price
    cur_pct = round(cur * 100, 2)

    # ── Limit-price recommendation ────────────────────────────────────────────
    # Use 24h window as primary signal; fall back to 8h or 1h
    ref = windows.get("24h") or windows.get("8h") or windows.get("1h")
    rec = {"timing": "insufficient_data", "suggested_limit": None, "reasoning": "Not enough price history yet."}
    if ref:
        z = ref["z_score"]
        std = ref["std_dev_pct"]
        mean = ref["mean"]
        boll_lo = ref["bollinger_lower"]
        boll_hi = ref["bollinger_upper"]

        if z < -2.0:
            timing = "strong_buy"
            limit  = round(cur_pct + 0.3, 2)      # near market — will fill fast
            reason = f"Price is {abs(z):.1f}σ below 24h mean ({mean:.1f}¢). Very underpriced vs historical range. Buy at or near market."
        elif z < -1.0:
            timing = "buy"
            limit  = round(max(boll_lo, cur_pct - std * 0.5), 2)
            reason = f"Price is {abs(z):.1f}σ below mean. Pull limit slightly below market to capture intra-day dip."
        elif z < 0:
            timing = "mild_buy"
            limit  = round(cur_pct - std * 0.8, 2)
            reason = f"Price slightly below 24h mean. Set limit ~{std*0.8:.1f}¢ below current to catch a dip."
        elif z < 1.0:
            timing = "neutral_wait"
            limit  = round(mean - std * 0.5, 2)
            reason = f"Price near fair value. Be patient — set limit near 24h mean ({mean:.1f}¢) to save vs current."
        else:
            timing = "overpriced_wait"
            limit  = round(mean, 2)
            reason = f"Price {z:.1f}σ ABOVE 24h mean. Likely to revert. Target limit near mean ({mean:.1f}¢)."

        rec = {
            "timing": timing,
            "suggested_limit": limit,
            "reasoning": reason,
            "bollinger_lower": boll_lo,
            "bollinger_upper": boll_hi,
            "mean_24h": mean,
            "z_score_24h": z,
        }

    # Recent price series for chart (last 24h, max 200 points)
    # all_snaps already includes the live_snap appended above, so chart is current
    chart_snaps = []
    cutoff_24h = now - _td(hours=24)
    for s in all_snaps:
        ts_raw = s.get("timestamp", "")
        try:
            ts = _dt.fromisoformat(ts_raw.replace("Z", "+00:00"))
            if ts >= cutoff_24h and s.get("yes_price") is not None:
                chart_snaps.append({"t": ts_raw, "p": round(float(s["yes_price"]) * 100, 2)})
        except Exception:
            pass
    # Deduplicate by timestamp, keep last occurrence (live snap wins)
    seen = {}
    for pt in chart_snaps:
        seen[pt["t"][:16]] = pt   # minute-level dedup key
    chart_snaps = sorted(seen.values(), key=lambda x: x["t"])[-200:]

    return {
        "market_id": market_id,
        "question":  market.question,
        "current_price_pct": cur_pct,
        "windows": windows,
        "recommendation": rec,
        "chart": chart_snaps,
        "snapshot_count": len(all_snaps),
    }


@app.get("/api/volatility/{market_id}")
def api_volatility(market_id: str):
    summary = volmod.get_market_volatility_summary(market_id)
    if not summary:
        raise HTTPException(status_code=404, detail="No volatility data yet")
    return summary


@app.get("/api/volatility/opportunities")
def api_volatility_opportunities(threshold: float = 1.5):
    opps = volmod.detect_opportunities(threshold_z=threshold)
    return {"opportunities": opps, "count": len(opps)}


@app.get("/api/volatility/all")
def api_all_volatility():
    all_vol = get_all_latest_volatility()
    return {"metrics": all_vol, "count": len(all_vol)}


@app.get("/api/signals/trading")
def api_trading_signals(hours: int = 24):
    signals = db_get_active_signals(hours=hours)
    return {"signals": signals, "count": len(signals)}


@app.get("/api/signals/performance")
def api_signal_performance():
    return siggen.get_signal_performance_report()


@app.post("/api/signals/evaluate")
def api_evaluate_signals(background_tasks: BackgroundTasks):
    background_tasks.add_task(siggen.evaluate_past_signals)
    return {"triggered": True}


@app.post("/api/collect")
def api_collect_snapshots(background_tasks: BackgroundTasks):
    background_tasks.add_task(collector.collect_all_snapshots)
    return {"triggered": True}


@app.get("/api/analysis/{market_id}")
def api_analyze_market(market_id: str):
    market = polyapi.get_market_by_id(market_id)
    if not market:
        raise HTTPException(status_code=404, detail="Market not found")
    weights = tracker.load_weights()
    pred = analyzermod.analyze_market(market, weights)
    return {
        "market": {
            "id": market.id, "question": market.question,
            "yes_price": market.yes_price, "volume": market.volume,
            "liquidity": market.liquidity, "end_date": market.end_date,
        },
        "prediction": {
            "predicted_yes_prob": pred.predicted_yes_prob,
            "confidence": pred.confidence, "reasoning": pred.reasoning,
        },
        "signals": pred.signals,
    }


@app.get("/api/dashboard")
def api_dashboard():
    """Composite endpoint for the multi-market dashboard."""
    watchlist = get_managed_watchlist()
    latest_prices = {p["market_id"]: p for p in get_all_latest_prices()}
    latest_vol = {v["market_id"]: v for v in get_all_latest_volatility()}
    active_signals = db_get_active_signals(hours=24)
    signal_map = {}
    for s in active_signals:
        mid = s["market_id"]
        if mid not in signal_map or s["unix_ts"] > signal_map[mid]["unix_ts"]:
            signal_map[mid] = s

    markets = []
    for w in watchlist:
        mid = w["market_id"]
        price_info = latest_prices.get(mid, {})
        vol_info = latest_vol.get(mid, {})
        sig_info = signal_map.get(mid)

        markets.append({
            "market_id": mid,
            "question": w.get("question", ""),
            "slug": w.get("slug", ""),
            "yes_price": price_info.get("yes_price"),
            "volume": w.get("volume", 0),
            "liquidity": w.get("liquidity", 0),
            "end_date": w.get("end_date"),
            "z_score": vol_info.get("z_score"),
            "std_dev": vol_info.get("std_dev"),
            "bollinger_upper": vol_info.get("bollinger_upper"),
            "bollinger_lower": vol_info.get("bollinger_lower"),
            "mean_price": vol_info.get("mean_price"),
            "active_signal": sig_info.get("signal_type") if sig_info else None,
            "signal_strength": sig_info.get("strength") if sig_info else None,
            "last_snapshot": price_info.get("timestamp"),
        })

    perf = siggen.get_signal_performance_report()
    opps = volmod.detect_opportunities(threshold_z=1.5)

    return {
        "markets": markets,
        "total_markets": len(markets),
        "active_signals_count": len(active_signals),
        "opportunities": opps[:5],
        "signal_performance": {
            "win_rate": perf.get("win_rate", 0),
            "total_signals": perf.get("total_signals", 0),
            "avg_profit": perf.get("avg_profit", 0),
        },
    }


# ── SMS / Twilio ──────────────────────────────────────────────────────────────

class SubscribeRequest(BaseModel):
    phone: str = ""      # kept for backward compat
    email: str = ""


def _send_email(to_addr: str, subject: str, body_html: str) -> dict:
    """
    Send an HTML email via SMTP (default: Gmail).
    Required env vars:
      EMAIL_FROM      — sender address (e.g. alerts@gmail.com)
      EMAIL_PASSWORD  — SMTP password / Gmail App Password
    Optional:
      SMTP_HOST       — default smtp.gmail.com
      SMTP_PORT       — default 587
    """
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

    from_addr = os.environ.get("EMAIL_FROM", "")
    password  = os.environ.get("EMAIL_PASSWORD", "")
    smtp_host = os.environ.get("SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(os.environ.get("SMTP_PORT", "587"))

    if not from_addr or not password:
        msg = f"Email not configured — EMAIL_FROM={bool(from_addr)} EMAIL_PASSWORD={bool(password)}"
        print(f"[Email] {msg}")
        return {"ok": False, "error": msg}
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = f"First Strike Intelligence <{from_addr}>"
        msg["To"]      = to_addr
        msg.attach(MIMEText(body_html, "html"))

        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.ehlo()
            server.starttls()
            server.login(from_addr, password)
            server.sendmail(from_addr, to_addr, msg.as_string())

        print(f"[Email] Sent to {to_addr[:4]}***")
        return {"ok": True}
    except Exception as e:
        print(f"[Email] Exception: {e}")
        return {"ok": False, "error": str(e)}


def _email_html(urgency_label: str, urgency_color: str, headline: str, price_str: str) -> str:
    """Build a clean HTML email body for alerts."""
    return f"""<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"/></head>
<body style="margin:0;padding:0;background:#060a0f;font-family:'Courier New',monospace">
  <div style="max-width:560px;margin:0 auto;padding:24px">
    <div style="background:#0d1520;border:1px solid #1a2535;border-radius:10px;overflow:hidden">
      <!-- Header -->
      <div style="background:linear-gradient(90deg,#0a0f1a,#0d1520);padding:16px 24px;border-bottom:1px solid #1a2535">
        <span style="font-size:18px;font-weight:900;letter-spacing:.12em;color:#fff">
          <span style="color:#ef4444">FIRST</span> STRIKE
        </span>
        <span style="margin-left:12px;font-size:11px;background:rgba(239,68,68,.15);color:#ef4444;border:1px solid rgba(239,68,68,.3);padding:2px 10px;border-radius:4px;font-weight:700;letter-spacing:.08em">
          INTELLIGENCE
        </span>
      </div>
      <!-- Alert level banner -->
      <div style="background:{urgency_color};padding:12px 24px">
        <span style="font-size:14px;font-weight:700;color:#fff;letter-spacing:.06em">{urgency_label}</span>
      </div>
      <!-- Body -->
      <div style="padding:20px 24px">
        <div style="font-size:15px;font-weight:700;color:#e2e8f0;line-height:1.5;margin-bottom:16px">
          {headline}
        </div>
        <div style="display:flex;gap:16px;flex-wrap:wrap;margin-bottom:20px">
          <div style="background:#060a0f;border:1px solid #1a2535;border-radius:6px;padding:10px 16px">
            <div style="font-size:9px;color:#64748b;text-transform:uppercase;letter-spacing:.06em;margin-bottom:2px">Iran Boots Market</div>
            <div style="font-size:20px;font-weight:700;color:#eab308">{price_str}</div>
          </div>
        </div>
        <a href="https://USvsIran.Trade" style="display:inline-block;background:rgba(59,130,246,.15);border:1px solid rgba(59,130,246,.4);color:#3b82f6;padding:10px 20px;border-radius:6px;font-size:12px;font-weight:700;text-decoration:none;letter-spacing:.04em">
          → View Full Dashboard
        </a>
      </div>
      <!-- Footer -->
      <div style="padding:12px 24px;border-top:1px solid #1a2535;font-size:10px;color:#64748b">
        EasyGoingA's First Strike Intelligence · <a href="https://USvsIran.Trade" style="color:#64748b">USvsIran.Trade</a>
        · <a href="https://USvsIran.Trade/api/unsubscribe?email=__EMAIL__" style="color:#64748b">Unsubscribe</a>
      </div>
    </div>
  </div>
</body>
</html>"""


def send_alert_email(alert_level: str, headline: str, odds=None):
    """
    Email all active subscribers when URGENT or IMPORTANT signal fires.
    Respects a 30-minute cooldown per subscriber to avoid spam.
    """
    from datetime import datetime as _dt2, timezone as _tz2, timedelta as _td2
    subscribers = get_active_email_subscribers()
    if not subscribers:
        return

    price_str = f"{odds:.1f}¢ YES" if odds else "—"
    if alert_level == "URGENT":
        urgency_label = "🚨 URGENT ALERT"
        urgency_color = "rgba(239,68,68,.85)"
        subject = f"🚨 URGENT — {headline[:60]}"
    else:
        urgency_label = "⚠️ IMPORTANT ALERT"
        urgency_color = "rgba(234,179,8,.75)"
        subject = f"⚠️ IMPORTANT — {headline[:60]}"

    now      = _dt2.now(_tz2.utc)
    cooldown = _td2(minutes=30)

    for sub in subscribers:
        last = sub.get("last_email_at")
        if last:
            try:
                last_dt = _dt2.fromisoformat(last.replace("Z", "+00:00"))
                if now - last_dt < cooldown:
                    continue
            except Exception:
                pass
        html = _email_html(urgency_label, urgency_color, headline[:200], price_str).replace(
            "__EMAIL__", sub["email"]
        )
        result = _send_email(sub["email"], subject, html)
        if result.get("ok"):
            update_last_email(sub["email"])


# keep old name as alias so video_monitor.py still works
send_alert_sms = send_alert_email


@app.post("/api/subscribe")
def api_subscribe(req: SubscribeRequest):
    """Subscribe an email address to breaking-news alerts."""
    email = (req.email or req.phone or "").strip().lower()
    if not email or "@" not in email:
        return {"subscribed": False, "error": "Valid email address required"}
    result = add_email_subscriber(email)
    return result


@app.get("/api/unsubscribe")
def api_unsubscribe(email: str = ""):
    """One-click unsubscribe link (used in email footer)."""
    if email:
        unsubscribe_email(email)
        return HTMLResponse("<html><body style='font-family:sans-serif;text-align:center;padding:60px;background:#060a0f;color:#e2e8f0'>"
                            "<h2>✓ Unsubscribed</h2><p>You've been removed from First Strike alerts.</p>"
                            "<a href='/' style='color:#3b82f6'>← Back to Dashboard</a></body></html>")
    return {"error": "No email provided"}


@app.get("/api/subscribers")
def api_subscribers():
    """Admin: list all email subscribers."""
    subs = get_active_email_subscribers()
    masked = [
        {**s, "email": s["email"][:3] + "****" + s["email"][s["email"].find("@"):]}
        for s in subs
    ]
    return {"subscribers": masked, "count": len(subs)}


@app.post("/api/email/test")
def api_email_test():
    """Admin: fire a test email to all subscribers."""
    subs = get_active_email_subscribers()
    sent = 0
    errors = []
    email_debug = {
        "from_set":     bool(os.environ.get("EMAIL_FROM")),
        "password_set": bool(os.environ.get("EMAIL_PASSWORD")),
        "smtp_host":    os.environ.get("SMTP_HOST", "smtp.gmail.com"),
        "smtp_port":    os.environ.get("SMTP_PORT", "587"),
        "from_addr":    os.environ.get("EMAIL_FROM", "NOT SET"),
    }
    subject = "🔔 First Strike — Email alerts are active"
    html = _email_html(
        "🔔 TEST ALERT",
        "rgba(59,130,246,.85)",
        "First Strike email alerts are now active. You'll receive URGENT and IMPORTANT signals as they happen.",
        "—"
    )
    for sub in subs:
        body = html.replace("__EMAIL__", sub["email"])
        result = _send_email(sub["email"], subject, body)
        if result.get("ok"):
            update_last_email(sub["email"])
            sent += 1
        else:
            errors.append({"email": sub["email"][:3] + "***", "error": result.get("error", "unknown")})
    return {
        "triggered": True, "sent": sent, "recipient_count": len(subs),
        "email_config": email_debug, "errors": errors
    }


# Legacy SMS endpoint aliases (keep URLs working)
@app.post("/api/sms/test")
def api_sms_test():
    return api_email_test()


@app.post("/api/email/send-news")
@app.post("/api/sms/send-news")
def api_email_send_news():
    """Admin: immediately email the latest top signal to all subscribers."""
    state = get_command_state()
    top = state.get("top_signals", [])
    headline = top[0].get("headline", "No headline available") if top else state.get("reason_summary", "No signals")
    odds = state.get("market_odds")
    alert_level = state.get("alert_level", "IMPORTANT")
    send_alert_email(alert_level, headline, odds)
    return {
        "triggered": True,
        "headline": headline,
        "alert_level": alert_level,
        "recipients": len(get_active_email_subscribers()),
    }


# ── Live Viewer Counter ───────────────────────────────────────────────────────

@app.post("/api/heartbeat")
def api_heartbeat(request: Request):
    """Called every 30s by each open browser tab to register presence."""
    sid = request.headers.get("X-Session-Id", "")
    if not sid:
        sid = str(uuid.uuid4())
    now = time.time()
    _viewer_sessions[sid] = now
    # Prune expired sessions
    expired = [k for k, v in _viewer_sessions.items() if now - v > _VIEWER_TIMEOUT]
    for k in expired:
        _viewer_sessions.pop(k, None)
    return {"session_id": sid, "viewers": len(_viewer_sessions)}


@app.get("/api/viewers")
def api_viewers():
    """Return current live viewer count."""
    now = time.time()
    active = sum(1 for v in _viewer_sessions.values() if now - v <= _VIEWER_TIMEOUT)
    return {"viewers": active}


# ── Tournament API ────────────────────────────────────────────────────────────

@app.get("/api/tournament/run")
def api_tournament_run():
    from engines.tournament import run_tournament
    result = run_tournament()
    return result


@app.get("/api/tournament/leaderboard")
def api_tournament_leaderboard():
    from engines.tournament import get_leaderboard
    return {"leaderboard": get_leaderboard()}


@app.get("/api/tournament/resolve")
def api_tournament_resolve():
    from engines.tournament import resolve_tournament
    return resolve_tournament()


@app.get("/api/tournament/engine/{name}")
def api_tournament_engine(name: str):
    from engines.tournament import get_engine_detail
    return get_engine_detail(name)


@app.get("/api/tournament/markets")
def api_tournament_markets():
    from engines.markets import get_tournament_markets
    return {"markets": get_tournament_markets()}


@app.get("/api/tournament/summary")
def api_tournament_summary():
    from engines.tournament import get_tournament_summary
    return get_tournament_summary()


# ── Pages ─────────────────────────────────────────────────────────────────────

app.mount("/static", StaticFiles(directory=Path(__file__).parent / "static"), name="static")

@app.get("/", response_class=HTMLResponse)
def dashboard():
    with open(Path(__file__).parent / "static" / "index.html") as f:
        return f.read()

@app.get("/command")
def command_center():
    """Command is now merged into the main dashboard — redirect there."""
    from fastapi.responses import RedirectResponse
    return RedirectResponse(url="/", status_code=301)

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

@app.get("/volatility", response_class=HTMLResponse)
def volatility_desk():
    with open(Path(__file__).parent / "static" / "desk-volatility.html") as f:
        return f.read()

@app.get("/historian", response_class=HTMLResponse)
def historian_desk():
    with open(Path(__file__).parent / "static" / "desk-historian.html") as f:
        return f.read()

@app.get("/video", response_class=HTMLResponse)
def video_desk():
    with open(Path(__file__).parent / "static" / "desk-video.html") as f:
        return f.read()


@app.get("/api/video/streams")
def api_video_streams():
    """
    Search YouTube Data API v3 for live streams related to Iran/military news.
    Falls back to empty list if YOUTUBE_API_KEY is not set.
    """
    import requests as _req
    key = os.environ.get("YOUTUBE_API_KEY", "")
    if not key:
        return {"live_count": 0, "streams": [], "no_api_key": True,
                "message": "Set YOUTUBE_API_KEY in Railway env vars to enable live stream discovery"}

    queries = [
        "Iran military news live",
        "Iran US war live stream",
        "Middle East breaking news live",
        "Iran boots on ground live",
    ]
    seen_ids = set()
    streams = []

    for q in queries:
        try:
            r = _req.get(
                "https://www.googleapis.com/youtube/v3/search",
                params={
                    "part": "snippet",
                    "q": q,
                    "type": "video",
                    "eventType": "live",
                    "maxResults": 5,
                    "order": "viewCount",
                    "relevanceLanguage": "en",
                    "key": key,
                },
                timeout=8,
            )
            if r.status_code != 200:
                continue
            items = r.json().get("items", [])
            for item in items:
                vid_id = item.get("id", {}).get("videoId", "")
                if not vid_id or vid_id in seen_ids:
                    continue
                seen_ids.add(vid_id)
                snip = item.get("snippet", {})
                thumb = (snip.get("thumbnails", {}).get("medium", {}).get("url", "")
                         or snip.get("thumbnails", {}).get("default", {}).get("url", ""))
                streams.append({
                    "video_id": vid_id,
                    "title": snip.get("title", ""),
                    "channel": snip.get("channelTitle", ""),
                    "thumbnail": thumb,
                    "published": snip.get("publishedAt", ""),
                    "viewers": None,
                })
        except Exception:
            continue

    return {"live_count": len(streams), "streams": streams[:12], "no_api_key": False}


@app.get("/api/video/monitor")
def api_video_monitor():
    """Return AI channel monitor state — which channels are live, what keywords hit."""
    state = vidmon.get_monitor_state()
    return {
        "last_scan": state.get("last_scan"),
        "channels_live": state.get("channels_live", 0),
        "alerts_fired": state.get("alerts_fired", 0),
        "youtube_key_set": bool(os.environ.get("YOUTUBE_API_KEY")),
        "channel_status": list(state.get("channel_status", {}).values()),
    }


@app.post("/api/video/scan")
def api_video_scan():
    """Manually trigger an immediate AI video channel scan."""
    alerts = vidmon.scan_all_channels()
    for alert in alerts:
        from intelligence.signals import Signal, MARKET_SLUG
        try:
            kws = ", ".join(alert.get("keywords", [])[:3])
            sig = Signal(
                source_app="video",
                timestamp=alert["timestamp"],
                headline=f"[{alert['channel']}] {alert['title'][:120]}",
                raw_text=f"Keywords: {kws}",
                signal_direction="bullish",
                confidence_score=90 if alert["level"] == "URGENT" else 70,
                importance_score=90 if alert["level"] == "URGENT" else 65,
                probability_impact_estimate=5.0 if alert["level"] == "URGENT" else 2.0,
                reasoning=f"Video monitor: {kws}",
                link=f"https://www.youtube.com/watch?v={alert.get('video_id','')}",
                market_slug=MARKET_SLUG,
            )
            store_signal(sig)
            send_alert_email(alert["level"], sig.headline, None)
        except Exception:
            pass
    return {"scanned": len(vidmon.CHANNELS), "alerts": len(alerts),
            "youtube_key_set": bool(os.environ.get("YOUTUBE_API_KEY"))}


@app.get("/markets", response_class=HTMLResponse)
def multi_market_dashboard():
    with open(Path(__file__).parent / "static" / "multi-market.html") as f:
        return f.read()


@app.get("/tournament", response_class=HTMLResponse)
def tournament_dashboard():
    with open(Path(__file__).parent / "static" / "tournament.html") as f:
        return f.read()


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("server:app", host="0.0.0.0", port=port, reload=False)
