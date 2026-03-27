"""
Signal model, SQLite storage, and deduplication for First Strike Intelligence System.
"""
from __future__ import annotations
import sqlite3
import hashlib
import json
import os
import time
from typing import Optional, List
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, asdict, field

# Support a persistent data directory via env var (e.g. Railway Volume mounted at /data)
_data_dir = os.environ.get(
    "PERSISTENT_DATA_DIR",
    os.path.join(os.path.dirname(__file__), "..", "data")
)
DB_PATH = os.path.join(_data_dir, "signals.db")

MARKET_SLUG = "us-forces-enter-iran-by-december-31"


@dataclass
class Signal:
    source_app: str           # pulse / beacon / market / orderflow
    timestamp: str
    headline: str
    raw_text: str
    signal_direction: str     # bullish / bearish / neutral
    confidence_score: int     # 0-100
    importance_score: int     # 0-100
    probability_impact_estimate: float  # +/- percentage points
    reasoning: str
    link: str
    market_slug: str = MARKET_SLUG
    id: Optional[str] = None  # SHA256 dedup hash

    def __post_init__(self):
        if not self.id:
            raw = f"{self.source_app}:{self.headline}:{self.timestamp[:16]}"
            self.id = hashlib.sha256(raw.encode()).hexdigest()[:16]

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "Signal":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id TEXT PRIMARY KEY,
            source_app TEXT,
            timestamp TEXT,
            headline TEXT,
            raw_text TEXT,
            signal_direction TEXT,
            confidence_score INTEGER,
            importance_score INTEGER,
            probability_impact_estimate REAL,
            reasoning TEXT,
            link TEXT,
            market_slug TEXT,
            created_at REAL DEFAULT (unixepoch())
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS command_state (
            id INTEGER PRIMARY KEY DEFAULT 1,
            market_odds REAL,
            house_odds REAL,
            odds_delta REAL,
            alert_level TEXT,
            reason_summary TEXT,
            orderbook_bias TEXT,
            recommended_action TEXT,
            updated_at TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS signal_scores (
            signal_id TEXT PRIMARY KEY,
            predicted_impact REAL,
            actual_price_change REAL,
            score REAL,
            evaluated_at TEXT
        )
    """)
    # Order book snapshots — one row per snapshot, stores full depth as JSON
    conn.execute("""
        CREATE TABLE IF NOT EXISTS orderbook_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            unix_ts REAL NOT NULL,
            yes_price REAL,
            best_bid REAL,
            best_ask REAL,
            spread REAL,
            imbalance REAL,
            total_bid_notional REAL,
            total_ask_notional REAL,
            bids_json TEXT,
            asks_json TEXT
        )
    """)
    # ── Multi-market tables ────────────────────────────────────────────────
    conn.execute("""
        CREATE TABLE IF NOT EXISTS managed_watchlist (
            market_id TEXT PRIMARY KEY,
            question TEXT NOT NULL,
            slug TEXT,
            category TEXT,
            tags TEXT,
            volume REAL,
            liquidity REAL,
            end_date TEXT,
            yes_token_id TEXT,
            no_token_id TEXT,
            added_at TEXT NOT NULL,
            last_snapshot_at TEXT,
            active INTEGER DEFAULT 1,
            discovery_source TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS price_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            market_id TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            unix_ts REAL NOT NULL,
            yes_price REAL NOT NULL,
            no_price REAL,
            volume REAL,
            volume_24hr REAL,
            liquidity REAL,
            best_bid REAL,
            best_ask REAL,
            spread REAL,
            imbalance REAL,
            is_backfill INTEGER DEFAULT 0
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_price_snap_market_ts ON price_snapshots(market_id, unix_ts)")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS volatility_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            market_id TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            unix_ts REAL NOT NULL,
            window_minutes INTEGER NOT NULL,
            mean_price REAL,
            std_dev REAL,
            bollinger_upper REAL,
            bollinger_lower REAL,
            z_score REAL,
            current_price REAL,
            price_range_high REAL,
            price_range_low REAL,
            mean_reversion_signal REAL
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_vol_market_ts ON volatility_metrics(market_id, unix_ts)")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS trading_signals (
            id TEXT PRIMARY KEY,
            market_id TEXT NOT NULL,
            market_question TEXT,
            timestamp TEXT NOT NULL,
            unix_ts REAL NOT NULL,
            signal_type TEXT NOT NULL,
            strength REAL NOT NULL,
            price_at_signal REAL NOT NULL,
            reasoning TEXT,
            volatility_z_score REAL,
            mean_reversion_component REAL,
            momentum_component REAL,
            volume_component REAL,
            prediction_component REAL,
            price_after_1h REAL,
            price_after_6h REAL,
            price_after_24h REAL,
            profit_loss REAL,
            evaluated INTEGER DEFAULT 0
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tsig_market ON trading_signals(market_id, unix_ts)")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sms_subscribers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            phone TEXT NOT NULL UNIQUE,
            subscribed_at TEXT NOT NULL,
            active INTEGER DEFAULT 1,
            last_sms_at TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS email_subscribers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT NOT NULL UNIQUE,
            subscribed_at TEXT NOT NULL,
            active INTEGER DEFAULT 1,
            last_email_at TEXT
        )
    """)
    # ── Tournament tables ──────────────────────────────────────────────────
    conn.execute("""
        CREATE TABLE IF NOT EXISTS tournament_markets (
            market_id TEXT PRIMARY KEY,
            question TEXT NOT NULL,
            slug TEXT,
            yes_price_at_selection REAL,
            volume REAL,
            liquidity REAL,
            end_date TEXT,
            yes_token_id TEXT,
            selected_at TEXT,
            outcome TEXT,
            final_price REAL,
            resolved INTEGER DEFAULT 0
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS tournament_bets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            engine_name TEXT NOT NULL,
            market_id TEXT NOT NULL,
            market_question TEXT,
            side TEXT NOT NULL,
            amount REAL NOT NULL,
            entry_price REAL NOT NULL,
            shares REAL NOT NULL,
            timestamp TEXT NOT NULL,
            outcome TEXT,
            exit_price REAL,
            profit_loss REAL,
            resolved INTEGER DEFAULT 0
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tb_engine ON tournament_bets(engine_name)")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS tournament_engines (
            engine_name TEXT PRIMARY KEY,
            strategy_summary TEXT,
            starting_balance REAL DEFAULT 1000,
            current_balance REAL DEFAULT 1000,
            total_bets INTEGER DEFAULT 0,
            wins INTEGER DEFAULT 0,
            losses INTEGER DEFAULT 0,
            roi REAL DEFAULT 0
        )
    """)
    conn.commit()
    # Migration: add alert_sent column if it doesn't exist yet
    try:
        conn.execute("ALTER TABLE signals ADD COLUMN alert_sent INTEGER DEFAULT 0")
        conn.commit()
    except Exception:
        pass
    conn.close()


# ── SMS Subscriber CRUD ────────────────────────────────────────────────────────

def add_subscriber(phone: str) -> dict:
    """Add a phone number to the SMS subscriber list. Returns {subscribed, already_subscribed}."""
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute(
            "INSERT INTO sms_subscribers (phone, subscribed_at, active) VALUES (?, ?, 1)",
            (phone, datetime.now(timezone.utc).isoformat()),
        )
        conn.commit()
        return {"subscribed": True, "already_subscribed": False}
    except sqlite3.IntegrityError:
        # Already exists — reactivate if previously unsubscribed
        conn.execute("UPDATE sms_subscribers SET active=1 WHERE phone=?", (phone,))
        conn.commit()
        return {"subscribed": False, "already_subscribed": True}
    finally:
        conn.close()


def get_active_subscribers() -> list:
    """Return all active SMS subscribers."""
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute(
        "SELECT id, phone, subscribed_at, last_sms_at FROM sms_subscribers WHERE active=1 ORDER BY subscribed_at DESC"
    ).fetchall()
    conn.close()
    return [{"id": r[0], "phone": r[1], "subscribed_at": r[2], "last_sms_at": r[3]} for r in rows]


def update_last_sms(phone: str):
    """Record when we last sent an SMS to this subscriber."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "UPDATE sms_subscribers SET last_sms_at=? WHERE phone=?",
        (datetime.now(timezone.utc).isoformat(), phone),
    )
    conn.commit()
    conn.close()


# ── Email Subscriber CRUD ──────────────────────────────────────────────────────

def add_email_subscriber(email: str) -> dict:
    """Add an email address to the subscriber list."""
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute(
            "INSERT INTO email_subscribers (email, subscribed_at, active) VALUES (?, ?, 1)",
            (email.lower().strip(), datetime.now(timezone.utc).isoformat()),
        )
        conn.commit()
        return {"subscribed": True, "already_subscribed": False}
    except sqlite3.IntegrityError:
        conn.execute("UPDATE email_subscribers SET active=1 WHERE email=?", (email.lower().strip(),))
        conn.commit()
        return {"subscribed": False, "already_subscribed": True}
    finally:
        conn.close()


def get_active_email_subscribers() -> list:
    """Return all active email subscribers."""
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute(
        "SELECT id, email, subscribed_at, last_email_at FROM email_subscribers WHERE active=1 ORDER BY subscribed_at DESC"
    ).fetchall()
    conn.close()
    return [{"id": r[0], "email": r[1], "subscribed_at": r[2], "last_email_at": r[3]} for r in rows]


def update_last_email(email: str):
    """Record when we last emailed this subscriber."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "UPDATE email_subscribers SET last_email_at=? WHERE email=?",
        (datetime.now(timezone.utc).isoformat(), email.lower().strip()),
    )
    conn.commit()
    conn.close()


def unsubscribe_email(email: str):
    """Mark an email subscriber as inactive (unsubscribe)."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("UPDATE email_subscribers SET active=0 WHERE email=?", (email.lower().strip(),))
    conn.commit()
    conn.close()


# ── Managed Watchlist CRUD ─────────────────────────────────────────────────

def upsert_managed_watchlist(market_dict: dict):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        INSERT OR REPLACE INTO managed_watchlist
        (market_id, question, slug, category, tags, volume, liquidity, end_date,
         yes_token_id, no_token_id, added_at, active, discovery_source)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,1,?)
    """, (
        market_dict["market_id"], market_dict["question"], market_dict.get("slug"),
        market_dict.get("category"), json.dumps(market_dict.get("tags", [])),
        market_dict.get("volume"), market_dict.get("liquidity"), market_dict.get("end_date"),
        market_dict.get("yes_token_id"), market_dict.get("no_token_id"),
        market_dict.get("added_at", datetime.now(timezone.utc).isoformat()),
        market_dict.get("discovery_source", "auto"),
    ))
    conn.commit()
    conn.close()


def get_managed_watchlist() -> list[dict]:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM managed_watchlist WHERE active = 1").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def deactivate_watchlist_entry(market_id: str):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("UPDATE managed_watchlist SET active = 0 WHERE market_id = ?", (market_id,))
    conn.commit()
    conn.close()


# ── Price Snapshots CRUD ───────────────────────────────────────────────────

def store_price_snapshot(market_id: str, data: dict):
    now = datetime.now(timezone.utc)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        INSERT INTO price_snapshots
        (market_id, timestamp, unix_ts, yes_price, no_price, volume, volume_24hr,
         liquidity, best_bid, best_ask, spread, imbalance, is_backfill)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        market_id, now.isoformat(), now.timestamp(),
        data.get("yes_price"), data.get("no_price"),
        data.get("volume"), data.get("volume_24hr"),
        data.get("liquidity"), data.get("best_bid"), data.get("best_ask"),
        data.get("spread"), data.get("imbalance"),
        data.get("is_backfill", 0),
    ))
    conn.commit()
    conn.close()


def store_price_snapshot_with_ts(market_id: str, data: dict, ts_iso: str, unix_ts: float):
    """Store a price snapshot with explicit timestamp (for backfill)."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        INSERT INTO price_snapshots
        (market_id, timestamp, unix_ts, yes_price, no_price, volume, volume_24hr,
         liquidity, best_bid, best_ask, spread, imbalance, is_backfill)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,1)
    """, (
        market_id, ts_iso, unix_ts,
        data.get("yes_price"), data.get("no_price"),
        data.get("volume"), data.get("volume_24hr"),
        data.get("liquidity"), data.get("best_bid"), data.get("best_ask"),
        data.get("spread"), data.get("imbalance"),
    ))
    conn.commit()
    conn.close()


def get_price_snapshots(market_id: str, hours: int = 24, limit: int = 2000) -> list[dict]:
    since = (datetime.now(timezone.utc) - timedelta(hours=hours)).timestamp()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT * FROM price_snapshots
        WHERE market_id = ? AND unix_ts > ?
        ORDER BY unix_ts ASC LIMIT ?
    """, (market_id, since, limit)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_latest_price_snapshot(market_id: str) -> Optional[dict]:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    row = conn.execute("""
        SELECT * FROM price_snapshots
        WHERE market_id = ? ORDER BY unix_ts DESC LIMIT 1
    """, (market_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def get_all_latest_prices() -> list[dict]:
    """Get the most recent price snapshot for each market."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT p.* FROM price_snapshots p
        INNER JOIN (
            SELECT market_id, MAX(unix_ts) as max_ts
            FROM price_snapshots GROUP BY market_id
        ) latest ON p.market_id = latest.market_id AND p.unix_ts = latest.max_ts
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_snapshot_count(market_id: str) -> int:
    conn = sqlite3.connect(DB_PATH)
    row = conn.execute(
        "SELECT COUNT(*) FROM price_snapshots WHERE market_id = ?", (market_id,)
    ).fetchone()
    conn.close()
    return row[0] if row else 0


# ── Volatility Metrics CRUD ────────────────────────────────────────────────

def store_volatility_metric(metric: dict):
    now = datetime.now(timezone.utc)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        INSERT INTO volatility_metrics
        (market_id, timestamp, unix_ts, window_minutes, mean_price, std_dev,
         bollinger_upper, bollinger_lower, z_score, current_price,
         price_range_high, price_range_low, mean_reversion_signal)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        metric["market_id"], now.isoformat(), now.timestamp(),
        metric["window_minutes"], metric.get("mean_price"), metric.get("std_dev"),
        metric.get("bollinger_upper"), metric.get("bollinger_lower"),
        metric.get("z_score"), metric.get("current_price"),
        metric.get("price_range_high"), metric.get("price_range_low"),
        metric.get("mean_reversion_signal"),
    ))
    conn.commit()
    conn.close()


def get_latest_volatility(market_id: str, window_minutes: int = None) -> list[dict]:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    if window_minutes:
        rows = conn.execute("""
            SELECT * FROM volatility_metrics
            WHERE market_id = ? AND window_minutes = ?
            ORDER BY unix_ts DESC LIMIT 1
        """, (market_id, window_minutes)).fetchall()
    else:
        # Get latest for each window size
        rows = conn.execute("""
            SELECT v.* FROM volatility_metrics v
            INNER JOIN (
                SELECT market_id, window_minutes, MAX(unix_ts) as max_ts
                FROM volatility_metrics WHERE market_id = ?
                GROUP BY market_id, window_minutes
            ) latest ON v.market_id = latest.market_id
                AND v.window_minutes = latest.window_minutes
                AND v.unix_ts = latest.max_ts
        """, (market_id,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_all_latest_volatility() -> list[dict]:
    """Get latest volatility metric (6h window) for all markets."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT v.* FROM volatility_metrics v
        INNER JOIN (
            SELECT market_id, MAX(unix_ts) as max_ts
            FROM volatility_metrics WHERE window_minutes = 360
            GROUP BY market_id
        ) latest ON v.market_id = latest.market_id AND v.unix_ts = latest.max_ts
        WHERE v.window_minutes = 360
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ── Trading Signals CRUD ───────────────────────────────────────────────────

def store_trading_signal(sig: dict):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        INSERT OR IGNORE INTO trading_signals
        (id, market_id, market_question, timestamp, unix_ts, signal_type, strength,
         price_at_signal, reasoning, volatility_z_score, mean_reversion_component,
         momentum_component, volume_component, prediction_component)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        sig["id"], sig["market_id"], sig.get("market_question"),
        sig["timestamp"], sig["unix_ts"],
        sig["signal_type"], sig["strength"], sig["price_at_signal"],
        sig.get("reasoning"), sig.get("volatility_z_score"),
        sig.get("mean_reversion_component"), sig.get("momentum_component"),
        sig.get("volume_component"), sig.get("prediction_component"),
    ))
    conn.commit()
    conn.close()


def get_active_trading_signals(hours: int = 24) -> list[dict]:
    since = (datetime.now(timezone.utc) - timedelta(hours=hours)).timestamp()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT * FROM trading_signals
        WHERE unix_ts > ? ORDER BY unix_ts DESC
    """, (since,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_unevaluated_signals(min_age_hours: float = 1.0) -> list[dict]:
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=min_age_hours)).timestamp()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT * FROM trading_signals
        WHERE evaluated = 0 AND unix_ts < ?
        ORDER BY unix_ts ASC
    """, (cutoff,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def update_trading_signal_evaluation(signal_id: str, updates: dict):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        UPDATE trading_signals SET
            price_after_1h = ?, price_after_6h = ?, price_after_24h = ?,
            profit_loss = ?, evaluated = 1
        WHERE id = ?
    """, (
        updates.get("price_after_1h"), updates.get("price_after_6h"),
        updates.get("price_after_24h"), updates.get("profit_loss"), signal_id,
    ))
    conn.commit()
    conn.close()


def get_evaluated_trading_signals() -> list[dict]:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT * FROM trading_signals WHERE evaluated = 1 ORDER BY unix_ts DESC
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def store_orderbook_snapshot(depth: dict, yes_price: float = None):
    """Store a full order book snapshot."""
    import json as _json
    conn = sqlite3.connect(DB_PATH)
    now = datetime.now(timezone.utc)
    conn.execute("""
        INSERT INTO orderbook_snapshots
        (timestamp, unix_ts, yes_price, best_bid, best_ask, spread, imbalance,
         total_bid_notional, total_ask_notional, bids_json, asks_json)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)
    """, (
        now.isoformat(),
        now.timestamp(),
        yes_price,
        depth.get("best_bid"),
        depth.get("best_ask"),
        depth.get("spread"),
        depth.get("imbalance"),
        depth.get("total_bid_notional"),
        depth.get("total_ask_notional"),
        _json.dumps(depth.get("bids", [])),
        _json.dumps(depth.get("asks", [])),
    ))
    conn.commit()
    conn.close()


def get_orderbook_snapshots(hours: int = 24, limit: int = 288) -> list:
    """Return recent OB snapshots, newest first."""
    import json as _json
    since = (datetime.now(timezone.utc) - timedelta(hours=hours)).timestamp()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT * FROM orderbook_snapshots
        WHERE unix_ts > ?
        ORDER BY unix_ts ASC
        LIMIT ?
    """, (since, limit)).fetchall()
    conn.close()
    result = []
    for r in rows:
        d = dict(r)
        try:
            d["bids"] = _json.loads(d.pop("bids_json", "[]"))
            d["asks"] = _json.loads(d.pop("asks_json", "[]"))
        except Exception:
            d["bids"] = []
            d["asks"] = []
        result.append(d)
    return result


def store_signal(sig: Signal) -> bool:
    """Store signal, return False if duplicate."""
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute("""
            INSERT OR IGNORE INTO signals
            (id, source_app, timestamp, headline, raw_text, signal_direction,
             confidence_score, importance_score, probability_impact_estimate,
             reasoning, link, market_slug)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            sig.id, sig.source_app, sig.timestamp, sig.headline, sig.raw_text,
            sig.signal_direction, sig.confidence_score, sig.importance_score,
            sig.probability_impact_estimate, sig.reasoning, sig.link, sig.market_slug
        ))
        stored = conn.execute("SELECT changes()").fetchone()[0] > 0
        conn.commit()
        return stored
    finally:
        conn.close()


def mark_signal_alerted(signal_id: str):
    """Mark a signal as having triggered an email alert (prevents duplicate alerts)."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("UPDATE signals SET alert_sent=1 WHERE id=?", (signal_id,))
    conn.commit()
    conn.close()


def get_recent_signals(hours: int = 24, source: str = None, limit: int = 100, unalerted_only: bool = False) -> list[dict]:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    since = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    unalerted_clause = " AND (alert_sent IS NULL OR alert_sent = 0)" if unalerted_only else ""
    if source:
        rows = conn.execute(
            f"SELECT * FROM signals WHERE timestamp > ? AND source_app = ?{unalerted_clause} ORDER BY created_at DESC LIMIT ?",
            (since, source, limit)
        ).fetchall()
    else:
        rows = conn.execute(
            f"SELECT * FROM signals WHERE timestamp > ?{unalerted_clause} ORDER BY created_at DESC LIMIT ?",
            (since, limit)
        ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_all_signals(limit: int = 200) -> list[dict]:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT * FROM signals ORDER BY created_at DESC LIMIT ?", (limit,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_command_state() -> dict:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM command_state WHERE id = 1").fetchone()
    conn.close()
    if row:
        return dict(row)
    return {
        "market_odds": None,
        "house_odds": None,
        "odds_delta": None,
        "alert_level": "no_action",
        "reason_summary": "Initializing...",
        "orderbook_bias": "neutral",
        "recommended_action": "monitor",
        "updated_at": None,
    }


def update_command_state(state: dict):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        INSERT OR REPLACE INTO command_state
        (id, market_odds, house_odds, odds_delta, alert_level, reason_summary,
         orderbook_bias, recommended_action, updated_at)
        VALUES (1,?,?,?,?,?,?,?,?)
    """, (
        state.get("market_odds"), state.get("house_odds"), state.get("odds_delta"),
        state.get("alert_level", "no_action"), state.get("reason_summary", ""),
        state.get("orderbook_bias", "neutral"), state.get("recommended_action", "monitor"),
        datetime.now(timezone.utc).isoformat()
    ))
    conn.commit()
    conn.close()


def apply_confidence_decay(signals: list[dict], half_life_hours: float = 12.0) -> list[dict]:
    """Decay confidence scores based on signal age."""
    now = time.time()
    for s in signals:
        try:
            ts = datetime.fromisoformat(s["timestamp"].replace("Z", "+00:00"))
            age_hours = (datetime.now(timezone.utc) - ts).total_seconds() / 3600
            decay = 0.5 ** (age_hours / half_life_hours)
            s["decayed_confidence"] = round(s["confidence_score"] * decay)
        except Exception:
            s["decayed_confidence"] = s.get("confidence_score", 0)
    return signals
