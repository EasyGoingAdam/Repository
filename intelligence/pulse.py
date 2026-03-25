"""
First Strike Pulse — X/Twitter Intelligence Module
Monitors curated accounts and keywords for early Iran/military signals.
Requires env var: TWITTER_API_KEY (twitterapi.io)
"""
import os
import re
import requests
import time
from datetime import datetime, timezone
from typing import Optional
from .signals import Signal, store_signal, MARKET_SLUG

TWITTER_API_KEY = os.environ.get("TWITTER_API_KEY", "")
TWITTERAPI_BASE = "https://api.twitterapi.io/twitter"

# Curated high-value accounts for Iran/military intelligence
MONITORED_ACCOUNTS = [
    "DeptofDefense", "CENTCOM", "SecDef", "StateDept", "POTUS",
    "Reuters", "AP", "BBCWorld", "AlMonitor", "RALee85",
    "Natsecjeff", "AmbJohnBolton", "KenRoth", "borzou",
    "iranintl", "IrnaEnglish", "PressTV",
    "OSINTdefender", "IntelCrab", "WarMonitor_",
]

KEYWORDS = [
    "iran troops", "iran invasion", "US iran military", "boots iran",
    "iran deployment", "pentagon iran", "US forces iran", "iran strike",
    "iran ground", "iran war", "iran nuclear deal", "iran sanctions",
    "strait hormuz troops", "iran escalation", "CENTCOM iran",
    "iran attack", "iranian military", "US military iran",
    "iran nuclear", "iran missiles", "tehran", "irgc",
]

# Credibility weights per account type
CREDIBILITY_MAP = {
    "DeptofDefense": 95, "CENTCOM": 95, "SecDef": 95, "StateDept": 90,
    "POTUS": 95, "Reuters": 88, "AP": 88, "BBCWorld": 85,
    "AlMonitor": 75, "RALee85": 72, "Natsecjeff": 70,
    "iranintl": 72, "IrnaEnglish": 60, "PressTV": 40,
    "OSINTdefender": 68, "IntelCrab": 65, "WarMonitor_": 62,
}

# Escalation language patterns
ESCALATION_TERMS = [
    "imminent", "immediate", "deployed", "invade", "invasion",
    "war", "strike", "attack", "crossed", "entered", "boots",
    "ground forces", "special operations", "soldiers", "troops",
    "military action", "armed forces",
]

DENIAL_TERMS = [
    "no plans", "not planning", "deny", "denied", "false", "misleading",
    "no troops", "not sending", "peace", "diplomatic",
]


def _headers() -> dict:
    return {"X-API-Key": TWITTER_API_KEY, "Content-Type": "application/json"}


def search_tweets(query: str, max_results: int = 20) -> list[dict]:
    if not TWITTER_API_KEY:
        return []
    try:
        r = requests.get(
            f"{TWITTERAPI_BASE}/tweet/search/recent",
            headers=_headers(),
            params={"query": query, "max_results": max_results, "tweet.fields": "created_at,author_id,public_metrics"},
            timeout=10
        )
        if r.status_code == 200:
            data = r.json()
            return data.get("data", [])
    except Exception:
        pass
    return []


def get_user_timeline(username: str, max_results: int = 10) -> list[dict]:
    if not TWITTER_API_KEY:
        return []
    try:
        r = requests.get(
            f"{TWITTERAPI_BASE}/user/last_tweets",
            headers=_headers(),
            params={"userName": username, "maxResults": max_results},
            timeout=10
        )
        if r.status_code == 200:
            data = r.json()
            return data.get("tweets", [])
    except Exception:
        pass
    return []


def _score_tweet(text: str, author: str) -> dict:
    """Score a tweet for signal direction, confidence, and importance."""
    text_lower = text.lower()

    escalation_hits = sum(1 for t in ESCALATION_TERMS if t in text_lower)
    denial_hits = sum(1 for t in DENIAL_TERMS if t in text_lower)

    base_credibility = CREDIBILITY_MAP.get(author, 50)

    if escalation_hits > denial_hits:
        direction = "bullish"
        confidence = min(95, base_credibility + escalation_hits * 5)
        impact = round(escalation_hits * 1.5, 1)
    elif denial_hits > escalation_hits:
        direction = "bearish"
        confidence = min(95, base_credibility + denial_hits * 5)
        impact = round(-denial_hits * 1.5, 1)
    else:
        direction = "neutral"
        confidence = max(20, base_credibility - 20)
        impact = 0.0

    # Importance: credibility + escalation urgency
    importance = min(100, base_credibility + escalation_hits * 8)

    # Classify reliability
    if base_credibility >= 88:
        classification = "official"
    elif base_credibility >= 70:
        classification = "semi-confirmed"
    elif escalation_hits >= 3:
        classification = "rumor (high-urgency)"
    else:
        classification = "rumor"

    return {
        "direction": direction,
        "confidence": confidence,
        "importance": importance,
        "impact": impact,
        "classification": classification,
        "escalation_hits": escalation_hits,
    }


def run_pulse_cycle() -> list[Signal]:
    """Run one full pulse cycle — returns new signals stored."""
    if not TWITTER_API_KEY:
        return []

    new_signals = []

    # 1. Search key queries
    queries = [
        "iran troops lang:en -is:retweet",
        "US military iran lang:en -is:retweet",
        "iran war 2025 lang:en -is:retweet",
        "pentagon iran lang:en -is:retweet",
        "CENTCOM iran lang:en -is:retweet",
    ]

    seen_texts = set()

    for query in queries:
        tweets = search_tweets(query, max_results=10)
        for tweet in tweets:
            text = tweet.get("text", "")
            if not text or text[:50] in seen_texts:
                continue
            seen_texts.add(text[:50])

            author = tweet.get("author_id", "unknown")
            score = _score_tweet(text, author)

            # Filter noise
            if score["importance"] < 30 and score["escalation_hits"] == 0:
                continue

            sig = Signal(
                source_app="pulse",
                timestamp=tweet.get("created_at", datetime.now(timezone.utc).isoformat()),
                headline=text[:120],
                raw_text=text,
                signal_direction=score["direction"],
                confidence_score=score["confidence"],
                importance_score=score["importance"],
                probability_impact_estimate=score["impact"],
                reasoning=f"[{score['classification']}] @{author} — {score['escalation_hits']} escalation term(s) detected. Query: '{query}'",
                link=f"https://twitter.com/i/web/status/{tweet.get('id', '')}",
                market_slug=MARKET_SLUG,
            )
            if store_signal(sig):
                new_signals.append(sig)

    # 2. Monitor high-priority accounts
    priority_accounts = ["DeptofDefense", "CENTCOM", "StateDept", "iranintl"]
    for account in priority_accounts:
        tweets = get_user_timeline(account, max_results=5)
        for tweet in tweets:
            text = tweet.get("text", tweet.get("full_text", ""))
            if not text or text[:50] in seen_texts:
                continue
            iran_relevant = any(kw in text.lower() for kw in ["iran", "tehran", "irgc", "persian"])
            if not iran_relevant:
                continue
            seen_texts.add(text[:50])
            score = _score_tweet(text, account)

            sig = Signal(
                source_app="pulse",
                timestamp=tweet.get("created_at", datetime.now(timezone.utc).isoformat()),
                headline=text[:120],
                raw_text=text,
                signal_direction=score["direction"],
                confidence_score=score["confidence"],
                importance_score=score["importance"],
                probability_impact_estimate=score["impact"],
                reasoning=f"[{score['classification']}] Official account @{account}",
                link=f"https://twitter.com/{account}",
                market_slug=MARKET_SLUG,
            )
            if store_signal(sig):
                new_signals.append(sig)

    return new_signals
