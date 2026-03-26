"""Polymarket API client (Gamma + CLOB)."""
import requests
import time
from typing import Optional, Union
from models import Market

GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE = "https://clob.polymarket.com"

HEADERS = {
    "User-Agent": "PolymarketAnalyzer/1.0",
    "Accept": "application/json",
}


def _get(url: str, params: dict = None, retries: int = 3) -> Union[dict, list]:
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=15)
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            if attempt == retries - 1:
                raise
            time.sleep(2 ** attempt)


def search_markets(query: str, limit: int = 20, open_only: bool = False) -> list[Market]:
    """Search markets by keyword."""
    params = {"q": query, "limit": limit, "active": "true"}
    if open_only:
        params["closed"] = "false"
    data = _get(f"{GAMMA_BASE}/markets", params=params)
    markets = []
    for m in (data if isinstance(data, list) else data.get("markets", [])):
        markets.append(_parse_market(m))
    return markets


def get_events(limit: int = 20, active: bool = True, closed: bool = False,
               order: str = "volume24hr", ascending: bool = False) -> list[dict]:
    """Fetch events with their sub-markets."""
    params = {
        "limit": limit,
        "active": str(active).lower(),
        "closed": str(closed).lower(),
        "order": order,
        "ascending": str(ascending).lower(),
    }
    data = _get(f"{GAMMA_BASE}/events", params=params)
    return data if isinstance(data, list) else []


def get_event_markets(event: dict) -> list[Market]:
    """Parse markets from an event response."""
    markets = []
    for m in event.get("markets", []):
        markets.append(_parse_market(m))
    return markets


def get_market_by_slug(slug: str) -> Optional[Market]:
    """Fetch a specific market by slug."""
    data = _get(f"{GAMMA_BASE}/markets", params={"slug": slug})
    items = data if isinstance(data, list) else data.get("markets", [])
    if items:
        return _parse_market(items[0])
    return None


def get_market_by_id(market_id: str) -> Optional[Market]:
    """Fetch a specific market by ID."""
    data = _get(f"{GAMMA_BASE}/markets/{market_id}")
    if data:
        return _parse_market(data)
    return None


def get_price_history(token_id: str, interval: str = "1d", fidelity: int = 60) -> list[dict]:
    """Get price history from CLOB timeseries."""
    try:
        data = _get(
            f"{CLOB_BASE}/prices-history",
            params={"market": token_id, "interval": interval, "fidelity": fidelity}
        )
        return data.get("history", []) if isinstance(data, dict) else []
    except Exception:
        return []


def get_orderbook(token_id: str) -> dict:
    """Get current order book for a token."""
    try:
        return _get(f"{CLOB_BASE}/book", params={"token_id": token_id})
    except Exception:
        return {}


def get_orderbook_depth(token_id: str) -> dict:
    """
    Fetch full order book and return structured depth data.
    Returns bids and asks as lists of {price, size, cumulative_size}.
    Bids sorted descending (best bid first).
    Asks sorted ascending (best ask first).
    """
    try:
        raw = _get(f"{CLOB_BASE}/book", params={"token_id": token_id})
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
        spread = round(best_ask - best_bid, 4) if (best_bid and best_ask) else None

        total_bid_notional = sum(b["notional"] for b in bids)
        total_ask_notional = sum(a["notional"] for a in asks)
        total_notional = total_bid_notional + total_ask_notional
        imbalance = (total_bid_notional - total_ask_notional) / total_notional if total_notional else 0

        return {
            "bids": bids,
            "asks": asks,
            "best_bid": best_bid,
            "best_ask": best_ask,
            "spread": spread,
            "total_bid_notional": round(total_bid_notional, 2),
            "total_ask_notional": round(total_ask_notional, 2),
            "imbalance": round(imbalance, 4),   # >0 means more buy pressure
        }
    except Exception:
        return {}


def get_trades(token_id: str, limit: int = 100) -> list[dict]:
    """Get recent trades for a market."""
    try:
        data = _get(f"{CLOB_BASE}/trades", params={"market": token_id, "limit": limit})
        return data if isinstance(data, list) else []
    except Exception:
        return []


def find_iran_boots_market() -> Optional[Market]:
    """
    Find the 'US forces enter Iran by December 31' market.
    Direct fetch by known ID (1394299) — the Dec 31 2026 resolution market.
    Also tries nearby slugs as fallback.
    """
    # Direct fetch by known ID
    m = get_market_by_id("1394299")
    if m:
        return m

    # Fallback: try slugs
    slugs = [
        "us-forces-enter-iran-by-december-31-573-642-385-371-179-425-262",
        "us-forces-enter-iran-by-december-31",
        "us-boots-on-the-ground-in-iran",
    ]
    for slug in slugs:
        m = get_market_by_slug(slug)
        if m:
            return m

    # Last resort: search for Iran event sub-markets
    try:
        event_data = _get(f"{GAMMA_BASE}/events/158299")
        markets_raw = event_data.get("markets", []) if isinstance(event_data, dict) else []
        for raw in markets_raw:
            m = _parse_market(raw)
            if "december" in m.question.lower() and "iran" in m.question.lower():
                return m
    except Exception:
        pass

    return None


def _parse_market(m: dict) -> Market:
    outcomes = []
    raw_outcomes = m.get("outcomes", "[]")
    if isinstance(raw_outcomes, str):
        import json
        try:
            outcomes = json.loads(raw_outcomes)
        except Exception:
            outcomes = []
    elif isinstance(raw_outcomes, list):
        outcomes = raw_outcomes

    prices = []
    raw_prices = m.get("outcomePrices", "[]")
    if isinstance(raw_prices, str):
        import json
        try:
            prices = [float(x) for x in json.loads(raw_prices)]
        except Exception:
            prices = []
    elif isinstance(raw_prices, list):
        prices = [float(x) for x in raw_prices]

    clob_ids = []
    raw_clob = m.get("clobTokenIds", "[]")
    if isinstance(raw_clob, str):
        import json
        try:
            clob_ids = json.loads(raw_clob)
        except Exception:
            clob_ids = []
    elif isinstance(raw_clob, list):
        clob_ids = raw_clob

    return Market(
        id=str(m.get("id", "")),
        question=m.get("question", ""),
        slug=m.get("slug", ""),
        outcomes=outcomes,
        outcome_prices=prices,
        volume=float(m.get("volumeNum", m.get("volume", 0)) or 0),
        liquidity=float(m.get("liquidityNum", m.get("liquidity", 0)) or 0),
        end_date=m.get("endDate"),
        active=bool(m.get("active", False)),
        closed=bool(m.get("closed", False)),
        description=m.get("description", ""),
        category=m.get("category", ""),
        volume_24hr=float(m.get("volume24hr", 0) or 0),
        clob_token_ids=clob_ids,
    )
