"""
Price Collector — snapshot prices for all watched markets at regular intervals.

Stores data in SQLite price_snapshots table for volatility analysis
and algorithm training.
"""
import time
from datetime import datetime, timezone
import api as polyapi
from intelligence.signals import (
    store_price_snapshot,
    store_price_snapshot_with_ts,
    get_managed_watchlist,
    get_snapshot_count,
)


def collect_price_snapshot(market) -> dict:
    """
    Collect a price snapshot for a single market.
    Fetches current price + orderbook depth, stores in SQLite.
    Returns the snapshot data dict.
    """
    data = {
        "yes_price": market.yes_price,
        "no_price": market.no_price,
        "volume": market.volume,
        "volume_24hr": market.volume_24hr,
        "liquidity": market.liquidity,
    }

    # Fetch orderbook depth if available
    if market.clob_token_ids:
        try:
            depth = polyapi.get_orderbook_depth(market.clob_token_ids[0])
            if depth:
                data["best_bid"] = depth.get("best_bid")
                data["best_ask"] = depth.get("best_ask")
                data["spread"] = depth.get("spread")
                data["imbalance"] = depth.get("imbalance")
        except Exception:
            pass

    store_price_snapshot(market.id, data)
    return data


def collect_all_snapshots() -> dict:
    """
    Collect price snapshots for all active watched markets.
    Returns summary with counts.
    """
    watchlist = get_managed_watchlist()
    if not watchlist:
        return {"collected": 0, "failed": 0, "total": 0}

    collected = 0
    failed = 0

    for entry in watchlist:
        try:
            market = polyapi.get_market_by_id(entry["market_id"])
            if market and market.active and not market.closed:
                collect_price_snapshot(market)
                collected += 1
            time.sleep(0.5)  # rate limit courtesy
        except Exception as e:
            failed += 1

    return {"collected": collected, "failed": failed, "total": len(watchlist)}


def backfill_history(market) -> int:
    """
    Pull historical price data from CLOB API and store as backfill snapshots.
    Only runs if we have fewer than 50 existing snapshots for this market.
    Returns number of data points stored.
    """
    if not market.clob_token_ids:
        return 0

    existing = get_snapshot_count(market.id)
    if existing >= 50:
        return 0  # already have enough data

    yes_token = market.clob_token_ids[0]
    history = polyapi.get_price_history(yes_token, interval="max", fidelity=60)

    if not history:
        return 0

    stored = 0
    for point in history:
        try:
            price = float(point.get("p", 0))
            timestamp = point.get("t", 0)
            if price <= 0 or not timestamp:
                continue

            # Convert timestamp (could be seconds or milliseconds)
            if timestamp > 1e12:
                timestamp = timestamp / 1000.0

            ts_dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)

            data = {
                "yes_price": price,
                "no_price": 1.0 - price,
            }
            store_price_snapshot_with_ts(
                market.id, data, ts_dt.isoformat(), timestamp
            )
            stored += 1
        except Exception:
            continue

    return stored


def backfill_all_markets() -> dict:
    """Backfill history for all watched markets that need it."""
    watchlist = get_managed_watchlist()
    total_stored = 0
    markets_filled = 0

    for entry in watchlist:
        try:
            market = polyapi.get_market_by_id(entry["market_id"])
            if market and market.clob_token_ids:
                count = backfill_history(market)
                if count > 0:
                    total_stored += count
                    markets_filled += 1
                time.sleep(0.5)
        except Exception:
            continue

    return {"markets_filled": markets_filled, "total_points": total_stored}
