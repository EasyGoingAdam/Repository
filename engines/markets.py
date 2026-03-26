"""
Tournament Market Selection — find ~20 markets near 50/50 ending by April 30.
"""
import sys
import os
import math
import json
import sqlite3
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import api as polyapi
from intelligence.signals import DB_PATH, init_db

# Political/geopolitical keywords for priority ranking
POLITICAL_KEYWORDS = [
    "trump", "biden", "president", "congress", "senate", "election",
    "republican", "democrat", "israel", "iran", "gaza", "netanyahu",
    "ceasefire", "military", "sanction", "tariff", "border",
    "supreme court", "pentagon", "nato", "ukraine", "russia", "china",
    "nuclear", "missile", "war", "invasion", "oil", "crude",
    "fed", "recession", "inflation", "approval", "indictment",
    "executive order", "legislation", "governor", "cabinet",
    "negotiation", "deal", "treaty", "diplomatic",
]


def select_tournament_markets(max_markets=20):
    """
    Find markets near 50/50 odds ending by April 30, 2026.
    Returns list of Market objects.
    """
    init_db()
    all_raw = []

    # Browse top active events
    for offset in range(0, 300, 50):
        try:
            events = polyapi.get_events(limit=50, active=True, closed=False,
                                         order="volume24hr", ascending=False)
            if not events:
                break
            for event in events:
                for m in event.get("markets", []):
                    all_raw.append(m)
        except Exception:
            break
        # Only need one batch if we get enough events
        if len(all_raw) > 2000:
            break

    print(f"  Scanned {len(all_raw)} raw markets")

    candidates = []
    seen_ids = set()

    for m in all_raw:
        mid = str(m.get("id", ""))
        if mid in seen_ids:
            continue
        seen_ids.add(mid)

        # Parse prices
        raw_prices = m.get("outcomePrices", "[]")
        if isinstance(raw_prices, str):
            try:
                prices = [float(x) for x in json.loads(raw_prices)]
            except Exception:
                continue
        elif isinstance(raw_prices, list):
            prices = [float(x) for x in raw_prices]
        else:
            continue

        if not prices:
            continue
        yes_price = prices[0]

        # Must be near 50/50
        if yes_price < 0.25 or yes_price > 0.75:
            continue

        # End date: must end by April 30 and not already expired
        end_date = m.get("endDate", "")
        if not end_date or end_date > "2026-05-01" or end_date < "2026-03-26":
            continue

        if m.get("closed"):
            continue

        # Must have CLOB tokens
        clob_raw = m.get("clobTokenIds", "[]")
        if isinstance(clob_raw, str):
            try:
                clob = json.loads(clob_raw)
            except Exception:
                clob = []
        else:
            clob = clob_raw or []
        if not clob:
            continue

        vol = float(m.get("volumeNum", 0) or 0)
        liq = float(m.get("liquidityNum", m.get("liquidity", 0)) or 0)

        # Minimum volume filter
        if vol < 500:
            continue

        question = m.get("question", "")
        text = (question + " " + m.get("description", "")).lower()
        is_political = any(kw in text for kw in POLITICAL_KEYWORDS)

        candidates.append({
            "id": mid,
            "question": question,
            "slug": m.get("slug", ""),
            "yes_price": yes_price,
            "volume": vol,
            "liquidity": liq,
            "end_date": end_date,
            "clob_token_id": clob[0] if clob else None,
            "is_political": is_political,
        })

    print(f"  {len(candidates)} candidates after filtering")

    if not candidates:
        return []

    # Rank: closeness to 0.50 (40%), volume (40%), political bonus (20%)
    max_vol = max(c["volume"] for c in candidates) or 1

    scored = []
    for c in candidates:
        closeness = 1.0 - abs(c["yes_price"] - 0.50) * 4  # 0.50 = 1.0, 0.25/0.75 = 0.0
        vol_score = math.log10(max(c["volume"], 1)) / math.log10(max(max_vol, 10))
        pol_bonus = 0.2 if c["is_political"] else 0.0
        composite = 0.4 * closeness + 0.4 * vol_score + pol_bonus
        scored.append((composite, c))

    scored.sort(key=lambda x: x[0], reverse=True)

    # Diversity: limit 2 per question stem
    selected = []
    stems = {}
    for _, c in scored:
        stem = " ".join(c["question"].lower().split()[:5])
        count = stems.get(stem, 0)
        if count >= 2:
            continue
        stems[stem] = count + 1
        selected.append(c)
        if len(selected) >= max_markets:
            break

    # Store in SQLite
    _store_tournament_markets(selected)

    # Convert to Market objects
    markets = []
    for c in selected:
        m = polyapi.get_market_by_id(c["id"])
        if m:
            markets.append(m)

    return markets


def _store_tournament_markets(candidates):
    """Store selected markets in tournament_markets table."""
    conn = sqlite3.connect(DB_PATH)
    now = datetime.now(timezone.utc).isoformat()
    for c in candidates:
        conn.execute("""
            INSERT OR REPLACE INTO tournament_markets
            (market_id, question, slug, yes_price_at_selection, volume, liquidity,
             end_date, yes_token_id, selected_at)
            VALUES (?,?,?,?,?,?,?,?,?)
        """, (
            c["id"], c["question"], c["slug"], c["yes_price"],
            c["volume"], c["liquidity"], c["end_date"],
            c.get("clob_token_id"), now,
        ))
    conn.commit()
    conn.close()


def get_tournament_markets():
    """Load tournament markets from SQLite."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM tournament_markets ORDER BY yes_price_at_selection").fetchall()
    conn.close()
    return [dict(r) for r in rows]
