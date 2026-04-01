"""FastAPI server with background price poller and SSE streaming.

Safety: resilient poll loop, thread-safe SSE broadcast, health endpoint,
graceful error recovery on all API/DB failures.
"""
from __future__ import annotations

import asyncio
import json
import os
import traceback
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional, Dict, List

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

# Use absolute paths relative to this file so Railway deploys work
_HERE = os.path.dirname(os.path.abspath(__file__))
_STATIC_DIR = os.path.join(_HERE, "static")

import api_client
import bollinger
import indicators
import patterns
import risk
import composite
import db
from config import MARKET_ID, POLL_INTERVAL_SECONDS, RISK_COMPUTE_INTERVAL, PORT

# Global state
yes_token_id: Optional[str] = None
market_info: Dict = {}
latest_state: Dict = {}
latest_risk: Dict = {}
poll_count: int = 0
poll_errors: int = 0
poll_successes: int = 0
last_poll_error: Optional[str] = None
app_start_time: float = 0.0

# Thread-safe SSE client list
_sse_lock = asyncio.Lock()
sse_clients: List[asyncio.Queue] = []


async def broadcast(payload: dict):
    """Thread-safe broadcast to all SSE clients."""
    try:
        msg = json.dumps(payload, default=str)
    except (TypeError, ValueError) as e:
        print(f"[SSE] JSON serialize error: {e}")
        return

    async with _sse_lock:
        dead = []
        for q in sse_clients:
            try:
                q.put_nowait(msg)
            except asyncio.QueueFull:
                dead.append(q)
        for q in dead:
            try:
                sse_clients.remove(q)
            except ValueError:
                pass


async def backfill_history():
    """Load historical price data into the DB."""
    if not yes_token_id:
        return
    try:
        count = await asyncio.to_thread(db.get_snapshot_count)
        if count > 50:
            print(f"[Backfill] Already have {count} snapshots, skipping")
            return

        print("[Backfill] Fetching historical prices...")
        history = await api_client.fetch_price_history(yes_token_id, interval="max", fidelity=60)
        if not history:
            print("[Backfill] No history returned")
            return

        for point in history:
            ts = point["t"]
            dt = datetime.fromtimestamp(ts, tz=timezone.utc)
            await asyncio.to_thread(db.store_snapshot, {
                "timestamp": dt.isoformat(),
                "unix_ts": ts,
                "yes_price": point["p"],
                "is_backfill": 1,
            })
        print(f"[Backfill] Stored {len(history)} historical snapshots")
    except Exception as e:
        print(f"[Backfill] Error: {e}")


async def poll_loop():
    """Main polling loop. Never crashes — catches all exceptions and continues."""
    global latest_state, latest_risk, poll_count, poll_errors, poll_successes, last_poll_error

    while True:
        try:
            # Fetch current data (both can return {} on failure)
            ob = await api_client.fetch_orderbook(yes_token_id) or {}
            mkt = await api_client.fetch_market(MARKET_ID) or {}

            # Determine current price — need at least one source
            current_price = None
            if ob.get("best_bid") is not None and ob.get("best_ask") is not None:
                current_price = (ob["best_bid"] + ob["best_ask"]) / 2
            elif mkt.get("yes_price") is not None:
                current_price = mkt["yes_price"]

            if current_price is None:
                print("[Poll] Could not determine price, skipping")
                poll_errors += 1
                last_poll_error = "No price data from API"
                await asyncio.sleep(POLL_INTERVAL_SECONDS)
                continue

            now = datetime.now(timezone.utc)
            snapshot = {
                "timestamp": now.isoformat(),
                "unix_ts": now.timestamp(),
                "yes_price": round(current_price, 6),
                "best_bid": ob.get("best_bid"),
                "best_ask": ob.get("best_ask"),
                "spread": ob.get("spread"),
                "imbalance": ob.get("imbalance"),
                "volume_24hr": mkt.get("volume_24hr"),
                "liquidity": mkt.get("liquidity"),
            }
            await asyncio.to_thread(db.store_snapshot, snapshot)

            # Get recent snapshots for all computations
            recent = await asyncio.to_thread(db.get_recent_snapshots, 500)
            if not recent:
                poll_errors += 1
                last_poll_error = "No snapshots in DB"
                await asyncio.sleep(POLL_INTERVAL_SECONDS)
                continue

            # Compute all technical indicators (safe — each indicator wrapped in try/except)
            indicator_values = indicators.compute_all(recent)

            # Add pattern/regime detection (each wrapped individually)
            prices = [s["yes_price"] for s in recent if s.get("yes_price") is not None]
            try:
                adx_val = patterns.compute_adx(prices)
            except Exception:
                adx_val = None
            try:
                hurst_val = patterns.compute_hurst(prices)
            except Exception:
                hurst_val = None
            regime = patterns.detect_regime(adx_val, hurst_val)
            indicator_values["adx"] = adx_val
            indicator_values["hurst_exponent"] = hurst_val
            indicator_values["regime"] = regime

            await asyncio.to_thread(db.store_indicator_snapshot, indicator_values)

            # Compute Bollinger bands (for chart overlay)
            try:
                bands = bollinger.compute_bollinger(recent)
                band_series = bollinger.compute_band_series(recent)
            except Exception:
                bands = None
                band_series = []

            # Generate composite signal
            comp_signal = None
            try:
                comp_signal = composite.generate_signal(indicator_values, snapshot, latest_risk)
                if comp_signal:
                    await asyncio.to_thread(db.store_composite_signal, comp_signal)
                    print(f"[Signal] {comp_signal['signal_type']} score={comp_signal['composite_score']:.3f} "
                          f"conf={comp_signal['confidence']:.2f} regime={regime} | {comp_signal['reasoning']}")
            except Exception as e:
                print(f"[Poll] Composite signal error: {e}")

            # Keep old Bollinger signal for backward compat
            old_signal = None
            try:
                if bands:
                    old_signal = bollinger.detect_signal(current_price, bands, ob.get("imbalance", 0))
                    if old_signal:
                        await asyncio.to_thread(db.store_signal, old_signal)
            except Exception:
                pass

            # Risk metrics every N polls
            poll_count += 1
            if poll_count % RISK_COMPUTE_INTERVAL == 0:
                try:
                    comp_signals = await asyncio.to_thread(db.get_recent_composite_signals, 200)
                    latest_risk = risk.compute_metrics(comp_signals, recent)
                    if latest_risk.get("total_signals", 0) > 0:
                        await asyncio.to_thread(db.store_risk_metrics, latest_risk)
                except Exception as e:
                    print(f"[Poll] Risk metrics error: {e}")

            # Support/resistance
            try:
                sr = patterns.find_support_resistance(prices)
            except Exception:
                sr = {"support": [], "resistance": []}

            latest_state = {
                "snapshot": snapshot,
                "bollinger": bands,
                "signal": comp_signal,
                "old_signal": old_signal,
                "indicators": indicator_values,
                "risk": latest_risk,
                "support_resistance": sr,
                "band_series": band_series[-100:] if band_series else [],
                "market": {
                    "question": market_info.get("question", ""),
                    "volume": mkt.get("volume"),
                    "volume_24hr": mkt.get("volume_24hr"),
                    "liquidity": mkt.get("liquidity"),
                },
                "connected_clients": len(sse_clients),
            }

            await broadcast(latest_state)
            poll_successes += 1
            last_poll_error = None

        except asyncio.CancelledError:
            print("[Poll] Cancelled, shutting down")
            return
        except Exception as e:
            poll_errors += 1
            last_poll_error = str(e)
            print(f"[Poll] Error: {e}")
            traceback.print_exc()

        await asyncio.sleep(POLL_INTERVAL_SECONDS)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global yes_token_id, market_info, app_start_time
    app_start_time = datetime.now(timezone.utc).timestamp()

    # Safety: verify all critical paths exist at startup
    print(f"[Startup] App directory: {_HERE}")
    print(f"[Startup] Static directory: {_STATIC_DIR} (exists={os.path.isdir(_STATIC_DIR)})")
    print(f"[Startup] index.html exists: {os.path.isfile(os.path.join(_STATIC_DIR, 'index.html'))}")
    print(f"[Startup] PORT={PORT}")

    db.init_db()
    print("[Startup] Database initialized OK")
    print("[Startup] Fetching market metadata...")
    try:
        market_info = await api_client.fetch_market(MARKET_ID) or {}
    except Exception as e:
        print(f"[Startup] Failed to fetch market: {e}")
        market_info = {}

    if market_info.get("clob_token_ids"):
        yes_token_id = market_info["clob_token_ids"][0]
        print(f"[Startup] YES token: {yes_token_id}")
        print(f"[Startup] Market: {market_info.get('question')}")
        print(f"[Startup] Current YES price: {market_info.get('yes_price')}")
    else:
        print("[Startup] WARNING: Could not resolve token IDs — will retry in poll loop")

    await backfill_history()
    task = asyncio.create_task(poll_loop())
    print(f"[Startup] Polling every {POLL_INTERVAL_SECONDS}s. Dashboard at http://localhost:8888")

    yield

    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    await api_client.close_session()


app = FastAPI(title="Polymarket Volatility Monitor", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
app.mount("/static", StaticFiles(directory=_STATIC_DIR), name="static")


@app.get("/", response_class=HTMLResponse)
async def index():
    index_path = os.path.join(_STATIC_DIR, "index.html")
    try:
        with open(index_path) as f:
            return f.read()
    except FileNotFoundError:
        return HTMLResponse(f"<h1>Dashboard not found</h1><p>{index_path} is missing</p>", 500)


# ==================== HEALTH CHECK ====================

@app.get("/api/health")
async def api_health():
    """Comprehensive health check — use this to verify app is functioning."""
    now = datetime.now(timezone.utc).timestamp()
    uptime = now - app_start_time if app_start_time else 0

    # Check DB
    db_ok = False
    snap_count = 0
    try:
        snap_count = await asyncio.to_thread(db.get_snapshot_count)
        db_ok = True
    except Exception:
        pass

    # Check API connectivity
    api_ok = bool(yes_token_id)

    # Check poll loop
    poll_ok = poll_successes > 0 and (last_poll_error is None or poll_errors < poll_successes)

    # Check last data freshness
    last_snap = latest_state.get("snapshot", {})
    last_ts = last_snap.get("unix_ts", 0)
    data_age = now - last_ts if last_ts else None
    data_fresh = data_age is not None and data_age < 120  # within 2 minutes

    healthy = db_ok and api_ok and poll_ok and data_fresh

    return {
        "status": "healthy" if healthy else "degraded",
        "uptime_seconds": round(uptime),
        "checks": {
            "database": {"ok": db_ok, "snapshot_count": snap_count},
            "api": {"ok": api_ok, "token_resolved": bool(yes_token_id)},
            "poll_loop": {
                "ok": poll_ok,
                "successes": poll_successes,
                "errors": poll_errors,
                "last_error": last_poll_error,
            },
            "data_freshness": {
                "ok": data_fresh,
                "age_seconds": round(data_age) if data_age else None,
                "last_price": last_snap.get("yes_price"),
            },
        },
        "sse_clients": len(sse_clients),
    }


# ==================== DATA ENDPOINTS ====================

@app.get("/api/history")
async def api_history(limit: int = 500):
    snapshots = await asyncio.to_thread(db.get_recent_snapshots, limit)
    try:
        band_series = bollinger.compute_band_series(snapshots)
    except Exception:
        band_series = []
    return {"snapshots": snapshots, "band_series": band_series}


@app.get("/api/signals")
async def api_signals(limit: int = 50):
    signals = await asyncio.to_thread(db.get_recent_signals, limit)
    return {"signals": signals}


@app.get("/api/status")
async def api_status():
    count = await asyncio.to_thread(db.get_snapshot_count)
    return {
        "market_id": MARKET_ID,
        "question": market_info.get("question", ""),
        "yes_token_id": yes_token_id,
        "snapshot_count": count,
        "latest": latest_state.get("snapshot"),
        "bollinger": latest_state.get("bollinger"),
        "indicators": latest_state.get("indicators"),
        "active_signal": latest_state.get("signal"),
        "risk": latest_state.get("risk"),
        "connected_clients": len(sse_clients),
        "poll_successes": poll_successes,
        "poll_errors": poll_errors,
    }


@app.get("/api/indicators")
async def api_indicators():
    return {"indicators": latest_state.get("indicators", {})}


@app.get("/api/indicators/history")
async def api_indicators_history(limit: int = 200):
    data = await asyncio.to_thread(db.get_recent_indicator_snapshots, limit)
    return {"indicators": data}


@app.get("/api/risk")
async def api_risk_endpoint():
    return {"risk": latest_risk}


@app.get("/api/composite")
async def api_composite(limit: int = 50):
    signals = await asyncio.to_thread(db.get_recent_composite_signals, limit)
    return {"signals": signals}


# ==================== SSE ====================

@app.get("/events")
async def sse(request: Request):
    q: asyncio.Queue = asyncio.Queue(maxsize=50)
    async with _sse_lock:
        sse_clients.append(q)

    async def stream():
        try:
            if latest_state:
                try:
                    init = {**latest_state, "connected_clients": len(sse_clients)}
                    yield f"data: {json.dumps(init, default=str)}\n\n"
                except (TypeError, ValueError):
                    pass

            while True:
                try:
                    msg = await asyncio.wait_for(q.get(), timeout=15.0)
                    yield f"data: {msg}\n\n"
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
                except asyncio.CancelledError:
                    break

                if await request.is_disconnected():
                    break
        finally:
            async with _sse_lock:
                try:
                    sse_clients.remove(q)
                except ValueError:
                    pass

    return StreamingResponse(stream(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=PORT, reload=True)
