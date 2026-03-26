"""
Market Discovery — find and manage a watchlist of US politics/Israel markets.

Searches Polymarket for relevant markets, ranks by volume/liquidity,
and maintains a managed watchlist in SQLite.
"""
import math
from datetime import datetime, timezone
import api as polyapi
from intelligence.signals import (
    upsert_managed_watchlist,
    get_managed_watchlist,
    deactivate_watchlist_entry,
)

# Search queries targeting US politics and Israel
DISCOVERY_QUERIES = [
    "US politics",
    "president",
    "congress",
    "election 2026",
    "senate",
    "Trump",
    "Biden",
    "Republican",
    "Democrat",
    "Israel",
    "Israel Iran",
    "Netanyahu",
    "Gaza",
    "Middle East",
    "US military",
    "sanctions",
    "Supreme Court",
    "House speaker",
]


def discover_markets(max_results: int = 20) -> list:
    """
    Search Polymarket for US politics and Israel markets.
    Uses both the events endpoint (high-volume active events) and
    keyword searches. Returns top `max_results` ranked by composite score.
    """
    seen_ids = set()
    all_markets = []

    # Strategy 1: Browse top active events and filter for relevant ones
    try:
        events = polyapi.get_events(limit=50, active=True, closed=False, order="volume24hr")
        for event in events:
            event_title = (event.get("title", "") + " " + event.get("description", "")).lower()
            event_markets = polyapi.get_event_markets(event)
            for m in event_markets:
                if m.id not in seen_ids and _is_relevant_combined(m, event_title):
                    seen_ids.add(m.id)
                    all_markets.append(m)
    except Exception as e:
        print(f"  Event browse failed: {e}")

    # Strategy 2: Keyword searches (catches markets not in top events)
    for query in DISCOVERY_QUERIES:
        try:
            results = polyapi.search_markets(query, limit=30, open_only=True)
            for m in results:
                if m.id not in seen_ids and _is_relevant(m):
                    seen_ids.add(m.id)
                    all_markets.append(m)
        except Exception as e:
            print(f"  Search '{query}' failed: {e}")
            continue

    print(f"  Found {len(all_markets)} candidate markets before filtering")

    # Filter: active, not closed, decent volume, has CLOB tokens
    filtered = [
        m for m in all_markets
        if m.active
        and not m.closed
        and m.volume >= 10_000
        and m.clob_token_ids
    ]
    print(f"  After filtering: {len(filtered)} markets")

    # Rank by composite score with diversity
    if not filtered:
        return []

    max_24hr = max((m.volume_24hr for m in filtered), default=1) or 1

    # Bonus for high-priority topics
    priority_keywords = [
        "israel", "iran", "gaza", "ceasefire", "military",
        "tariff", "immigration", "border", "executive order",
        "supreme court", "impeach", "sanction",
    ]

    scored = []
    for m in filtered:
        vol_score = math.log10(max(m.volume, 1)) / 7.0
        liq_score = math.log10(max(m.liquidity + 1, 1)) / 6.0
        activity_score = m.volume_24hr / max_24hr
        # Priority bonus for key geopolitical/policy topics
        text_lower = m.question.lower()
        priority_bonus = 0.2 if any(kw in text_lower for kw in priority_keywords) else 0.0
        composite = 0.4 * vol_score + 0.25 * liq_score + 0.15 * activity_score + priority_bonus
        scored.append((composite, m))

    scored.sort(key=lambda x: x[0], reverse=True)

    # Diversity: limit to max 3 markets per "event cluster" (similar question stems)
    selected = []
    cluster_counts = {}
    for _, m in scored:
        # Cluster by first 4 words of question to group similar markets
        cluster_key = " ".join(m.question.lower().split()[:4])
        count = cluster_counts.get(cluster_key, 0)
        if count >= 3:
            continue
        cluster_counts[cluster_key] = count + 1
        selected.append(m)
        if len(selected) >= max_results:
            break

    return selected


def _is_relevant_combined(market, event_text: str = "") -> bool:
    """Check relevance using both market and event-level text."""
    text = (market.question + " " + market.description + " " + market.category + " " + event_text).lower()
    keywords = [
        "politic", "president", "congress", "senate", "house", "election",
        "republican", "democrat", "trump", "biden", "gop",
        "israel", "iran", "gaza", "netanyahu", "hamas", "hezbollah",
        "middle east", "military", "sanction", "supreme court",
        "governor", "legislation", "bill sign", "executive order",
        "cabinet", "secretary", "attorney general", "doj", "fbi",
        "immigration", "border", "tariff", "impeach",
        "us forces", "boots on the ground", "pentagon",
        "ceasefire", "2028", "nominee", "nomination",
        "white house", "oval office", "veto", "pardon",
    ]
    return any(kw in text for kw in keywords)


def _is_relevant(market) -> bool:
    """Check if a market is related to US politics or Israel."""
    text = (market.question + " " + market.description + " " + market.category).lower()
    keywords = [
        "politic", "president", "congress", "senate", "house", "election",
        "republican", "democrat", "trump", "biden", "gop",
        "israel", "iran", "gaza", "netanyahu", "hamas", "hezbollah",
        "middle east", "military", "sanction", "supreme court",
        "governor", "legislation", "bill sign", "executive order",
        "cabinet", "secretary", "attorney general", "doj", "fbi",
        "immigration", "border", "tariff", "impeach",
        "us forces", "boots on the ground", "pentagon",
    ]
    return any(kw in text for kw in keywords)


def refresh_watchlist(max_markets: int = 20) -> dict:
    """
    Run discovery and update the managed watchlist in SQLite.
    Returns summary of changes.
    """
    print("  Running market discovery...")
    markets = discover_markets(max_results=max_markets)

    if not markets:
        print("  No markets discovered.")
        return {"added": 0, "total": 0}

    existing = {w["market_id"] for w in get_managed_watchlist()}
    new_ids = set()
    added = 0

    for m in markets:
        new_ids.add(m.id)
        entry = {
            "market_id": m.id,
            "question": m.question,
            "slug": m.slug,
            "category": m.category,
            "tags": [],
            "volume": m.volume,
            "liquidity": m.liquidity,
            "end_date": m.end_date,
            "yes_token_id": m.clob_token_ids[0] if m.clob_token_ids else None,
            "no_token_id": m.clob_token_ids[1] if len(m.clob_token_ids) > 1 else None,
            "added_at": datetime.now(timezone.utc).isoformat(),
            "discovery_source": "auto",
        }
        upsert_managed_watchlist(entry)
        if m.id not in existing:
            added += 1

    # Deactivate markets that dropped off (only auto-discovered ones)
    removed = 0
    for mid in existing - new_ids:
        deactivate_watchlist_entry(mid)
        removed += 1

    total = len(get_managed_watchlist())
    print(f"  Discovery complete: {added} added, {removed} removed, {total} active")
    return {"added": added, "removed": removed, "total": total}


def get_watched_markets() -> list:
    """Fetch fresh Market objects for all active watchlist entries."""
    watchlist = get_managed_watchlist()
    markets = []
    for entry in watchlist:
        try:
            m = polyapi.get_market_by_id(entry["market_id"])
            if m and m.active and not m.closed:
                markets.append(m)
        except Exception:
            continue
    return markets


def add_market_manually(market_id: str) -> bool:
    """Add a specific market to the watchlist by ID."""
    m = polyapi.get_market_by_id(market_id)
    if not m:
        return False
    entry = {
        "market_id": m.id,
        "question": m.question,
        "slug": m.slug,
        "category": m.category,
        "tags": [],
        "volume": m.volume,
        "liquidity": m.liquidity,
        "end_date": m.end_date,
        "yes_token_id": m.clob_token_ids[0] if m.clob_token_ids else None,
        "no_token_id": m.clob_token_ids[1] if len(m.clob_token_ids) > 1 else None,
        "added_at": datetime.now(timezone.utc).isoformat(),
        "discovery_source": "manual",
    }
    upsert_managed_watchlist(entry)
    return True
