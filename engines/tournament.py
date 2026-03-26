"""
Tournament Runner — orchestrates the 5-engine betting competition.
Selects markets, runs all engines, stores bets, resolves outcomes, ranks engines.
"""
import sys
import os
import sqlite3
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import api as polyapi
from intelligence.signals import DB_PATH, init_db
from engines.markets import select_tournament_markets, get_tournament_markets
from engines import contrarian, momentum, volatility_vulture, fundamentalist, kelly

ENGINES = [
    (contrarian, "Bet AGAINST the crowd. Mean reversion: bigger bets on markets furthest from 50/50."),
    (momentum, "Bet WITH the trend. Rising prices = YES, falling = NO. Size by momentum strength."),
    (volatility_vulture, "Big bets on volatile markets, orderbook imbalance for direction. Skip stable markets."),
    (fundamentalist, "Trust the multi-signal model. Bet where model disagrees with market price by >3%."),
    (kelly, "Half-Kelly optimal sizing. Concentrates bets where model edge is largest."),
]

STARTING_BALANCE = 1000.0


def run_tournament():
    """
    Full tournament: select markets, run all 5 engines, store everything.
    Returns summary dict.
    """
    init_db()

    # Clear previous tournament data
    conn = sqlite3.connect(DB_PATH)
    conn.execute("DELETE FROM tournament_bets")
    conn.execute("DELETE FROM tournament_engines")
    conn.execute("DELETE FROM tournament_markets")
    conn.commit()
    conn.close()

    # Step 1: Select markets
    print("\n  Selecting tournament markets (near 50/50, ending by April 30)...")
    markets = select_tournament_markets(max_markets=20)
    print(f"  Selected {len(markets)} markets")

    if not markets:
        print("  No suitable markets found!")
        return {"markets": 0, "engines": 0, "total_bets": 0}

    # Step 2: Run each engine
    all_bets = {}
    for engine_module, strategy_desc in ENGINES:
        name = engine_module.ENGINE_NAME
        print(f"\n  Running engine: {name}...")

        try:
            bets = engine_module.place_bets(markets)
        except Exception as e:
            print(f"    Error: {e}")
            bets = []

        all_bets[name] = bets
        print(f"    Placed {len(bets)} bets, total wagered: ${sum(b['amount'] for b in bets):,.2f}")

        # Store engine
        _store_engine(name, strategy_desc, bets)

        # Store bets
        _store_bets(bets)

        # Rate limit between engines (API calls)
        time.sleep(1)

    total_bets = sum(len(b) for b in all_bets.values())
    return {
        "markets": len(markets),
        "engines": len(ENGINES),
        "total_bets": total_bets,
        "bets_by_engine": {name: len(bets) for name, bets in all_bets.items()},
    }


def _store_engine(name, strategy, bets):
    conn = sqlite3.connect(DB_PATH)
    total_wagered = sum(b["amount"] for b in bets)
    conn.execute("""
        INSERT OR REPLACE INTO tournament_engines
        (engine_name, strategy_summary, starting_balance, current_balance, total_bets, wins, losses, roi)
        VALUES (?,?,?,?,?,0,0,0)
    """, (name, strategy, STARTING_BALANCE, STARTING_BALANCE - total_wagered + total_wagered, len(bets)))
    conn.commit()
    conn.close()


def _store_bets(bets):
    now = datetime.now(timezone.utc).isoformat()
    conn = sqlite3.connect(DB_PATH)
    for b in bets:
        conn.execute("""
            INSERT INTO tournament_bets
            (engine_name, market_id, market_question, side, amount, entry_price, shares, timestamp)
            VALUES (?,?,?,?,?,?,?,?)
        """, (
            b["engine_name"], b["market_id"], b["market_question"],
            b["side"], b["amount"], b["entry_price"], b["shares"], now,
        ))
    conn.commit()
    conn.close()


def resolve_tournament():
    """
    Check which tournament markets have resolved and update bet P&L.
    Returns summary.
    """
    init_db()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # Get unresolved bets (id is the primary key)
    bets = conn.execute("SELECT * FROM tournament_bets WHERE resolved = 0").fetchall()
    bets = [dict(b) for b in bets]

    if not bets:
        conn.close()
        return {"resolved": 0, "pending": 0}

    resolved_count = 0
    market_cache = {}

    for bet in bets:
        mid = bet["market_id"]

        # Cache market lookups
        if mid not in market_cache:
            m = polyapi.get_market_by_id(mid)
            market_cache[mid] = m

        market = market_cache[mid]
        if not market:
            continue

        # Check if resolved
        outcome = None
        if market.closed:
            outcome = "YES" if market.yes_price > 0.50 else "NO"
        elif market.yes_price > 0.95:
            outcome = "YES"
        elif market.yes_price < 0.05:
            outcome = "NO"

        if outcome is None:
            continue

        # Calculate P&L
        if bet["side"] == outcome:
            # Won: get $1 per share minus entry cost
            profit = bet["shares"] * 1.0 - bet["amount"]
        else:
            # Lost: lose entire amount
            profit = -bet["amount"]

        # Update bet
        conn.execute("""
            UPDATE tournament_bets SET
                outcome = ?, exit_price = ?, profit_loss = ?, resolved = 1
            WHERE id = ?
        """, (outcome, market.yes_price, round(profit, 4), bet["id"]))

        # Update tournament market
        conn.execute("""
            UPDATE tournament_markets SET outcome = ?, final_price = ?, resolved = 1
            WHERE market_id = ?
        """, (outcome, market.yes_price, mid))

        resolved_count += 1

    conn.commit()

    # Update engine stats
    _update_engine_stats(conn)

    conn.close()

    pending = len(bets) - resolved_count
    return {"resolved": resolved_count, "pending": pending}


def _update_engine_stats(conn):
    """Recalculate engine stats from resolved bets."""
    engines = conn.execute("SELECT engine_name FROM tournament_engines").fetchall()
    for eng in engines:
        name = eng[0]
        bets = conn.execute(
            "SELECT * FROM tournament_bets WHERE engine_name = ?", (name,)
        ).fetchall()

        total = len(bets)
        total_wagered = sum(b["amount"] for b in bets)
        resolved = [b for b in bets if b["resolved"]]
        wins = sum(1 for b in resolved if (b["profit_loss"] or 0) > 0)
        losses = sum(1 for b in resolved if (b["profit_loss"] or 0) <= 0)
        total_pl = sum(b["profit_loss"] or 0 for b in resolved)
        current_balance = STARTING_BALANCE + total_pl
        roi = (total_pl / STARTING_BALANCE) * 100 if STARTING_BALANCE else 0

        conn.execute("""
            UPDATE tournament_engines SET
                current_balance = ?, total_bets = ?, wins = ?, losses = ?, roi = ?
            WHERE engine_name = ?
        """, (round(current_balance, 2), total, wins, losses, round(roi, 2), name))

    conn.commit()


def get_leaderboard():
    """Return engines ranked by current balance."""
    init_db()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT * FROM tournament_engines ORDER BY current_balance DESC
    """).fetchall()
    conn.close()

    leaderboard = []
    for i, r in enumerate(rows):
        d = dict(r)
        d["rank"] = i + 1
        # Get bet summary
        leaderboard.append(d)

    return leaderboard


def get_engine_detail(engine_name):
    """Return all bets for an engine."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    engine = conn.execute(
        "SELECT * FROM tournament_engines WHERE engine_name = ?", (engine_name,)
    ).fetchone()
    bets = conn.execute(
        "SELECT * FROM tournament_bets WHERE engine_name = ? ORDER BY amount DESC",
        (engine_name,)
    ).fetchall()
    conn.close()

    return {
        "engine": dict(engine) if engine else None,
        "bets": [dict(b) for b in bets],
    }


def get_tournament_summary():
    """Get full tournament state for the dashboard."""
    init_db()
    leaderboard = get_leaderboard()
    markets = get_tournament_markets()

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    all_bets = conn.execute("SELECT * FROM tournament_bets ORDER BY engine_name, amount DESC").fetchall()
    conn.close()

    resolved_markets = sum(1 for m in markets if m.get("resolved"))
    total_markets = len(markets)

    return {
        "leaderboard": leaderboard,
        "markets": markets,
        "total_markets": total_markets,
        "resolved_markets": resolved_markets,
        "total_bets": len(all_bets),
        "bets": [dict(b) for b in all_bets],
    }
