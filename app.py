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

from fastapi import FastAPI, Request, Query
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
latest_orderbook_raw: Dict = {}
poll_count: int = 0
poll_errors: int = 0
poll_successes: int = 0
last_poll_error: Optional[str] = None
app_start_time: float = 0.0

# Alert history (in-memory ring buffer)
alert_history: List[Dict] = []
MAX_ALERTS = 200

# Breakout tracking
last_breakout_ts: float = 0.0
BREAKOUT_COOLDOWN = 600  # 10 min between breakout alerts

# Thread-safe SSE client list
_sse_lock = asyncio.Lock()
sse_clients: List[asyncio.Queue] = []

# Runtime settings (mutable via API)
runtime_settings: Dict = {
    "poll_interval": POLL_INTERVAL_SECONDS,
    "composite_threshold": 0.4,
    "confidence_floor": 0.45,
    "cooldown_seconds": 300,
    "bollinger_std": 1.0,
    "rsi_period": 14,
    "macd_fast": 12,
    "macd_slow": 26,
    "macd_signal": 9,
}


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


def add_alert(alert_type: str, message: str, data: dict = None):
    """Add an alert to the in-memory history."""
    global alert_history
    alert = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "unix_ts": datetime.now(timezone.utc).timestamp(),
        "type": alert_type,
        "message": message,
        "data": data or {},
    }
    alert_history.append(alert)
    if len(alert_history) > MAX_ALERTS:
        alert_history = alert_history[-MAX_ALERTS:]
    return alert


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


def check_breakout(prices: list, sr: dict, current_price: float) -> Optional[Dict]:
    """Check if price broke through support/resistance levels."""
    global last_breakout_ts
    now_ts = datetime.now(timezone.utc).timestamp()
    if now_ts - last_breakout_ts < BREAKOUT_COOLDOWN:
        return None

    for level in sr.get("resistance", []):
        if current_price > level and len(prices) >= 2 and prices[-2] <= level:
            last_breakout_ts = now_ts
            return {"type": "resistance_break", "level": level, "price": current_price,
                    "direction": "up", "message": f"Price broke above resistance at {level*100:.1f}c"}

    for level in sr.get("support", []):
        if current_price < level and len(prices) >= 2 and prices[-2] >= level:
            last_breakout_ts = now_ts
            return {"type": "support_break", "level": level, "price": current_price,
                    "direction": "down", "message": f"Price broke below support at {level*100:.1f}c"}

    return None


async def poll_loop():
    """Main polling loop. Never crashes — catches all exceptions and continues."""
    global latest_state, latest_risk, latest_orderbook_raw
    global poll_count, poll_errors, poll_successes, last_poll_error

    while True:
        try:
            # Fetch current data (both can return {} on failure)
            ob = await api_client.fetch_orderbook(yes_token_id) or {}
            mkt = await api_client.fetch_market(MARKET_ID) or {}

            # Store raw orderbook for depth chart
            try:
                raw_book = await api_client.fetch_orderbook_raw(yes_token_id)
                if raw_book:
                    latest_orderbook_raw = raw_book
            except Exception:
                pass

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
                    # Add alert for signal
                    add_alert("signal", f"{comp_signal['signal_type']} signal fired (score={comp_signal['composite_score']:.3f})", comp_signal)
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

            # Check for breakouts
            breakout = None
            try:
                breakout = check_breakout(prices, sr, current_price)
                if breakout:
                    add_alert("breakout", breakout["message"], breakout)
                    print(f"[Breakout] {breakout['message']}")
            except Exception:
                pass

            # Check for regime change
            prev_regime = latest_state.get("indicators", {}).get("regime")
            if prev_regime and regime != prev_regime and prev_regime != "unknown":
                add_alert("regime_change", f"Regime changed from {prev_regime.upper()} to {regime.upper()}", {"from": prev_regime, "to": regime})

            # Price change calculations
            price_changes = {}
            if len(recent) >= 2:
                price_changes["prev"] = recent[-2].get("yes_price")
            # 1h ago (~120 snapshots at 30s interval)
            if len(recent) >= 120:
                price_1h = recent[-120].get("yes_price")
                if price_1h:
                    price_changes["1h"] = round((current_price - price_1h) / price_1h * 100, 2)
                    price_changes["1h_price"] = price_1h
            # 6h ago (~720 snapshots)
            if len(recent) >= 500:
                price_6h = recent[0].get("yes_price")
                if price_6h:
                    price_changes["max"] = round((current_price - price_6h) / price_6h * 100, 2)
                    price_changes["max_price"] = price_6h

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
                "price_changes": price_changes,
                "breakout": breakout,
                "alerts": alert_history[-5:],
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


# ==================== NEW ENDPOINTS ====================

@app.get("/api/orderbook/depth")
async def api_orderbook_depth():
    """Return raw orderbook levels for depth chart."""
    return latest_orderbook_raw or {"bids": [], "asks": []}


@app.get("/api/alerts")
async def api_alerts(limit: int = 50):
    """Return recent alerts."""
    return {"alerts": alert_history[-limit:]}


@app.get("/api/settings")
async def api_get_settings():
    """Return current runtime settings."""
    return runtime_settings


@app.post("/api/settings")
async def api_update_settings(request: Request):
    """Update runtime settings."""
    body = await request.json()
    for key in body:
        if key in runtime_settings:
            runtime_settings[key] = body[key]
    return {"ok": True, "settings": runtime_settings}


@app.get("/api/export/snapshots")
async def api_export_snapshots(limit: int = 500, format: str = "json"):
    """Export price snapshots as JSON or CSV."""
    data = await asyncio.to_thread(db.get_recent_snapshots, limit)
    if format == "csv":
        if not data:
            return StreamingResponse(iter(["no data"]), media_type="text/csv")
        headers = list(data[0].keys())
        lines = [",".join(headers)]
        for row in data:
            lines.append(",".join(str(row.get(h, "")) for h in headers))
        csv_text = "\n".join(lines)
        return StreamingResponse(
            iter([csv_text]),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=snapshots.csv"},
        )
    return {"snapshots": data}


@app.get("/api/export/signals")
async def api_export_signals(limit: int = 200, format: str = "json"):
    """Export composite signals as JSON or CSV."""
    data = await asyncio.to_thread(db.get_recent_composite_signals, limit)
    if format == "csv":
        if not data:
            return StreamingResponse(iter(["no data"]), media_type="text/csv")
        headers = list(data[0].keys())
        lines = [",".join(headers)]
        for row in data:
            lines.append(",".join(str(row.get(h, "")) for h in headers))
        csv_text = "\n".join(lines)
        return StreamingResponse(
            iter([csv_text]),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=signals.csv"},
        )
    return {"signals": data}


@app.get("/api/performance")
async def api_performance():
    """Cumulative P&L from composite signals."""
    signals = await asyncio.to_thread(db.get_recent_composite_signals, 200)
    snapshots = await asyncio.to_thread(db.get_recent_snapshots, 500)
    if not signals or not snapshots:
        return {"performance": [], "summary": {}}

    price_map = {}
    for s in snapshots:
        price_map[int(s["unix_ts"])] = s["yes_price"]

    pnl_series = []
    cumulative = 0.0
    wins = 0
    losses = 0
    for sig in signals:
        sig_ts = int(sig["unix_ts"])
        entry_price = None
        for s in snapshots:
            if int(s["unix_ts"]) >= sig_ts:
                entry_price = s["yes_price"]
                break
        if entry_price is None:
            continue
        # Check price 5 min later
        exit_price = None
        for s in snapshots:
            if int(s["unix_ts"]) >= sig_ts + 300:
                exit_price = s["yes_price"]
                break
        if exit_price is None:
            continue
        pnl = (exit_price - entry_price) if sig["signal_type"] == "BUY" else (entry_price - exit_price)
        cumulative += pnl
        if pnl > 0:
            wins += 1
        else:
            losses += 1
        pnl_series.append({
            "unix_ts": sig_ts,
            "signal_type": sig["signal_type"],
            "entry": entry_price,
            "exit": exit_price,
            "pnl": round(pnl, 6),
            "cumulative": round(cumulative, 6),
        })

    return {
        "performance": pnl_series,
        "summary": {
            "total_trades": wins + losses,
            "wins": wins,
            "losses": losses,
            "win_rate": round(wins / (wins + losses), 4) if (wins + losses) > 0 else 0,
            "cumulative_pnl": round(cumulative, 6),
        },
    }


@app.get("/api/related-markets")
async def api_related_markets():
    """Fetch related Polymarket markets."""
    try:
        session = await api_client.get_session()
        async with session.get(
            f"{api_client.GAMMA_BASE}/markets",
            params={"limit": 10, "active": True, "order": "volume24hr", "ascending": False,
                    "tag": "politics"},
        ) as resp:
            if resp.status >= 400:
                return {"markets": []}
            data = await resp.json()
            markets = []
            for m in (data if isinstance(data, list) else []):
                outcome_prices = []
                raw_prices = m.get("outcomePrices")
                if raw_prices:
                    if isinstance(raw_prices, str):
                        try:
                            outcome_prices = [float(p) for p in json.loads(raw_prices)]
                        except Exception:
                            pass
                    elif isinstance(raw_prices, list):
                        outcome_prices = [float(p) for p in raw_prices]
                markets.append({
                    "id": str(m.get("id", "")),
                    "question": m.get("question", ""),
                    "yes_price": outcome_prices[0] if outcome_prices else None,
                    "volume_24hr": float(m.get("volume24hr", 0) or 0),
                    "liquidity": float(m.get("liquidityNum", 0) or 0),
                })
            return {"markets": markets[:10]}
    except Exception as e:
        print(f"[API] Related markets error: {e}")
        return {"markets": []}


@app.get("/api/signal-replay/{signal_id}")
async def api_signal_replay(signal_id: int):
    """Get market context around a specific signal for replay."""
    signals = await asyncio.to_thread(db.get_recent_composite_signals, 500)
    target = None
    for s in signals:
        if s.get("id") == signal_id:
            target = s
            break
    if not target:
        return {"error": "Signal not found"}

    sig_ts = target["unix_ts"]
    snapshots = await asyncio.to_thread(db.get_recent_snapshots, 500)
    # Get snapshots in a window around the signal (5 min before, 10 min after)
    context_snaps = [s for s in snapshots if sig_ts - 300 <= s["unix_ts"] <= sig_ts + 600]

    indicators_hist = await asyncio.to_thread(db.get_recent_indicator_snapshots, 500)
    context_indicators = [i for i in indicators_hist if sig_ts - 300 <= i["unix_ts"] <= sig_ts + 600]

    return {
        "signal": target,
        "snapshots": context_snaps,
        "indicators": context_indicators,
    }


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
