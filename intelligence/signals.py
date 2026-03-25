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

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "signals.db")

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
    conn.commit()
    conn.close()


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


def get_recent_signals(hours: int = 24, source: str = None, limit: int = 100) -> list[dict]:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    since = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    if source:
        rows = conn.execute(
            "SELECT * FROM signals WHERE timestamp > ? AND source_app = ? ORDER BY created_at DESC LIMIT ?",
            (since, source, limit)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM signals WHERE timestamp > ? ORDER BY created_at DESC LIMIT ?",
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
