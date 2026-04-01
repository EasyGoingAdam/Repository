"""Async Polymarket API client (Gamma + CLOB)."""
from __future__ import annotations

import aiohttp
import asyncio
from typing import Optional, List, Dict
from config import GAMMA_BASE, CLOB_BASE

HEADERS = {
    "User-Agent": "PolymarketVolatilityMonitor/1.0",
    "Accept": "application/json",
}

_session: Optional[aiohttp.ClientSession] = None


async def get_session() -> aiohttp.ClientSession:
    global _session
    if _session is None or _session.closed:
        _session = aiohttp.ClientSession(headers=HEADERS, timeout=aiohttp.ClientTimeout(total=15))
    return _session


async def close_session():
    global _session
    if _session and not _session.closed:
        await _session.close()
        _session = None


async def _get(url: str, params: dict = None, retries: int = 3):
    session = await get_session()
    for attempt in range(retries):
        try:
            async with session.get(url, params=params) as resp:
                if resp.status >= 400:
                    print(f"[API] {url} returned {resp.status}")
                    if attempt == retries - 1:
                        return None
                    await asyncio.sleep(2 ** attempt)
                    continue
                return await resp.json()
        except Exception as e:
            print(f"[API] {url} attempt {attempt+1}/{retries}: {e}")
            if attempt == retries - 1:
                return None
            await asyncio.sleep(min(2 ** attempt, 5))


async def fetch_market(market_id: str) -> Dict:
    """Fetch market metadata from Gamma API."""
    try:
        data = await _get(f"{GAMMA_BASE}/markets/{market_id}")
    except Exception:
        data = None
    if not data:
        return {}

    clob_ids = []
    raw_ids = data.get("clobTokenIds")
    if raw_ids:
        if isinstance(raw_ids, str):
            import json
            try:
                clob_ids = json.loads(raw_ids)
            except (json.JSONDecodeError, TypeError):
                clob_ids = [raw_ids]
        elif isinstance(raw_ids, list):
            clob_ids = raw_ids

    outcome_prices = []
    raw_prices = data.get("outcomePrices")
    if raw_prices:
        if isinstance(raw_prices, str):
            import json
            try:
                outcome_prices = [float(p) for p in json.loads(raw_prices)]
            except (json.JSONDecodeError, TypeError, ValueError):
                pass
        elif isinstance(raw_prices, list):
            outcome_prices = [float(p) for p in raw_prices]

    return {
        "id": str(data.get("id", market_id)),
        "question": data.get("question", ""),
        "slug": data.get("slug", ""),
        "clob_token_ids": clob_ids,
        "outcome_prices": outcome_prices,
        "yes_price": outcome_prices[0] if outcome_prices else None,
        "volume": float(data.get("volumeNum", 0) or 0),
        "volume_24hr": float(data.get("volume24hr", 0) or 0),
        "liquidity": float(data.get("liquidityNum", 0) or 0),
        "end_date": data.get("endDate"),
        "active": data.get("active", False),
    }


async def fetch_price_history(token_id: str, interval: str = "max", fidelity: int = 60) -> List[Dict]:
    """Get historical price data from CLOB."""
    try:
        data = await _get(
            f"{CLOB_BASE}/prices-history",
            params={"market": token_id, "interval": interval, "fidelity": fidelity},
        )
        history = data.get("history", []) if isinstance(data, dict) else []
        result = []
        for point in history:
            try:
                t = float(point["t"])
                # Handle millisecond timestamps
                if t > 1e12:
                    t = t / 1000.0
                result.append({"p": float(point["p"]), "t": t})
            except (KeyError, ValueError, TypeError):
                continue
        return result
    except Exception:
        return []


async def fetch_orderbook_raw(token_id: str) -> Dict:
    """Fetch raw orderbook levels for depth chart visualization."""
    try:
        raw = await _get(f"{CLOB_BASE}/book", params={"token_id": token_id})
        if not raw:
            return {"bids": [], "asks": []}
        bids = []
        for lvl in raw.get("bids", []):
            try:
                bids.append({"price": float(lvl.get("price", 0)), "size": float(lvl.get("size", 0))})
            except (ValueError, TypeError):
                continue
        asks = []
        for lvl in raw.get("asks", []):
            try:
                asks.append({"price": float(lvl.get("price", 0)), "size": float(lvl.get("size", 0))})
            except (ValueError, TypeError):
                continue
        bids.sort(key=lambda x: x["price"], reverse=True)
        asks.sort(key=lambda x: x["price"])
        # Add cumulative sizes
        cum = 0
        for b in bids:
            cum += b["size"]
            b["cumulative"] = round(cum, 2)
        cum = 0
        for a in asks:
            cum += a["size"]
            a["cumulative"] = round(cum, 2)
        return {"bids": bids, "asks": asks}
    except Exception:
        return {"bids": [], "asks": []}


async def fetch_orderbook(token_id: str) -> Dict:
    """Fetch order book and compute depth metrics."""
    try:
        raw = await _get(f"{CLOB_BASE}/book", params={"token_id": token_id})
        if not raw:
            return {}

        def parse_levels(levels: list, sort_desc: bool) -> list:
            parsed = []
            for lvl in levels:
                try:
                    p = float(lvl.get("price", lvl.get("p", 0)))
                    s = float(lvl.get("size", lvl.get("s", 0)))
                    parsed.append({"price": p, "size": s})
                except (ValueError, TypeError):
                    continue
            parsed.sort(key=lambda x: x["price"], reverse=sort_desc)
            cumulative = 0.0
            for lvl in parsed:
                cumulative += lvl["size"]
                lvl["cumulative_size"] = round(cumulative, 4)
                lvl["notional"] = round(lvl["price"] * lvl["size"], 2)
            return parsed

        bids = parse_levels(raw.get("bids", []), sort_desc=True)
        asks = parse_levels(raw.get("asks", []), sort_desc=False)

        best_bid = bids[0]["price"] if bids else None
        best_ask = asks[0]["price"] if asks else None
        spread = round(best_ask - best_bid, 4) if (best_bid is not None and best_ask is not None) else None

        total_bid = sum(b["notional"] for b in bids)
        total_ask = sum(a["notional"] for a in asks)
        total = total_bid + total_ask
        imbalance = round((total_bid - total_ask) / total, 4) if total else 0

        return {
            "best_bid": best_bid,
            "best_ask": best_ask,
            "spread": spread,
            "imbalance": imbalance,
            "total_bid_notional": round(total_bid, 2),
            "total_ask_notional": round(total_ask, 2),
        }
    except Exception:
        return {}
