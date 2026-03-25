"""
First Strike Pulse — X/Twitter Intelligence Module
Monitors curated accounts and keywords for early Iran/military signals.
Fetches ~10-20 tweets per minute. Authors are AI-weighted via profile lookup.
Requires env var: TWITTER_API_KEY (twitterapi.io)
"""
from __future__ import annotations
import os
import re
import requests
import time
from datetime import datetime, timezone
from typing import Optional
from .signals import Signal, store_signal, MARKET_SLUG

TWITTER_API_KEY = os.environ.get("TWITTER_API_KEY", "")
TWITTERAPI_BASE = "https://api.twitterapi.io/twitter"

# ── Monitored accounts ────────────────────────────────────────────────────────
MONITORED_ACCOUNTS = [
    "DeptofDefense", "CENTCOM", "SecDef", "StateDept", "POTUS",
    "Reuters", "AP", "BBCWorld", "AlMonitor", "RALee85",
    "Natsecjeff", "AmbJohnBolton", "KenRoth", "borzou",
    "iranintl", "IrnaEnglish", "PressTV",
    "OSINTdefender", "IntelCrab", "WarMonitor_",
    "LaurenEtta", "IranWire", "RadioFarda_", "VOAIran",
    "HamedFard", "MaziarBahari",
]

# ── Search queries (rotated each cycle for variety) ──────────────────────────
SEARCH_QUERIES = [
    "iran troops lang:en -is:retweet",
    "US military iran lang:en -is:retweet",
    "iran war 2025 lang:en -is:retweet",
    "pentagon iran lang:en -is:retweet",
    "CENTCOM iran lang:en -is:retweet",
    "iran invasion ground forces lang:en -is:retweet",
    "boots on ground iran lang:en -is:retweet",
    "iran nuclear strike lang:en -is:retweet",
    "IRGC deploy lang:en -is:retweet",
    "iran military operation lang:en -is:retweet",
    "iran escalation lang:en -is:retweet",
    "iran attack response lang:en -is:retweet",
]

# ── Pre-known credibility scores ──────────────────────────────────────────────
CREDIBILITY_MAP = {
    "DeptofDefense": 95, "CENTCOM": 95, "SecDef": 95, "StateDept": 90,
    "POTUS": 95, "Reuters": 88, "AP": 88, "BBCWorld": 85,
    "AlMonitor": 78, "RALee85": 74, "Natsecjeff": 72,
    "iranintl": 75, "IrnaEnglish": 60, "PressTV": 38,
    "OSINTdefender": 68, "IntelCrab": 66, "WarMonitor_": 63,
    "IranWire": 72, "RadioFarda_": 72, "VOAIran": 75,
    "LaurenEtta": 70, "MaziarBahari": 68, "HamedFard": 65,
    "borzou": 70, "AmbJohnBolton": 72, "KenRoth": 68,
}

# ── Escalation / denial language ─────────────────────────────────────────────
ESCALATION_TERMS = [
    "imminent", "immediate", "deployed", "invade", "invasion",
    "war", "strike", "attack", "crossed", "entered", "boots",
    "ground forces", "special operations", "soldiers", "troops",
    "military action", "armed forces", "incursion", "offensive",
    "airstrikes", "bombardment", "special forces", "marines",
    "combat mission", "boots on the ground", "regime change",
    "ground invasion", "military buildup", "forward deployed",
]

DENIAL_TERMS = [
    "no plans", "not planning", "deny", "denied", "false", "misleading",
    "no troops", "not sending", "peace", "diplomatic", "de-escalate",
    "ceasefire", "negotiations", "sanctions relief", "withdraw", "retreat",
    "no military action", "diplomatic solution",
]

# ── Official-source keywords in bio ──────────────────────────────────────────
OFFICIAL_BIO_TERMS = [
    "official", "government", "minister", "secretary", "pentagon",
    "ambassador", "department", "ministry", "general", "admiral",
    "colonel", "senator", "congressman", "white house", "state dept",
    "department of defense", "department of state", "nato",
]

# ── In-memory profile cache (persists for server lifetime) ───────────────────
_PROFILE_CACHE: dict = {}


def _headers() -> dict:
    return {"X-API-Key": TWITTER_API_KEY, "Content-Type": "application/json"}


def get_user_profile(username: str) -> dict:
    """
    Fetch Twitter user profile via twitterapi.io.
    Returns followers_count, verified, description, name.
    Results cached in _PROFILE_CACHE.
    """
    if username in _PROFILE_CACHE:
        return _PROFILE_CACHE[username]
    if not TWITTER_API_KEY:
        return {}
    try:
        r = requests.get(
            f"{TWITTERAPI_BASE}/user/info",
            headers=_headers(),
            params={"userName": username},
            timeout=8,
        )
        if r.status_code == 200:
            d = r.json()
            # twitterapi.io may nest under "data" key
            data = d.get("data", d)
            profile = {
                "followers": data.get("followers_count", data.get("followersCount", 0)),
                "verified": bool(
                    data.get("verified") or
                    data.get("is_blue_verified") or
                    data.get("isBlueVerified")
                ),
                "description": (data.get("description") or data.get("bio") or "").lower(),
                "name": data.get("name", username),
                "tweet_count": data.get("statuses_count", data.get("statusesCount", 0)),
            }
            _PROFILE_CACHE[username] = profile
            return profile
    except Exception:
        pass
    _PROFILE_CACHE[username] = {}
    return {}


def _author_credibility(username: str) -> int:
    """
    Compute author credibility 0-100 by combining:
    - Pre-known score from CREDIBILITY_MAP
    - Live Twitter profile data (followers, verified, bio keywords)
    """
    base = CREDIBILITY_MAP.get(username, 45)

    profile = get_user_profile(username)
    if not profile:
        return base

    followers = profile.get("followers", 0)
    verified  = profile.get("verified", False)
    bio       = profile.get("description", "")

    # Follower count boost (log scale)
    if followers >= 5_000_000:
        base = max(base, 82)
    elif followers >= 1_000_000:
        base = max(base, 76)
    elif followers >= 500_000:
        base = max(base, 70)
    elif followers >= 100_000:
        base = max(base, 63)
    elif followers >= 10_000:
        base = max(base, 55)

    # Verified account boost
    if verified:
        base = min(95, base + 8)

    # Official bio language boost
    official_bio_hits = sum(1 for t in OFFICIAL_BIO_TERMS if t in bio)
    if official_bio_hits >= 2:
        base = min(95, base + 10)
    elif official_bio_hits == 1:
        base = min(95, base + 5)

    return min(95, base)


def _classify_author(credibility: int, username: str) -> str:
    """Classify author tier for display."""
    if username in ("DeptofDefense", "CENTCOM", "SecDef", "StateDept", "POTUS"):
        return "official"
    if credibility >= 85:
        return "official"
    if credibility >= 68:
        return "semi-confirmed"
    if credibility >= 50:
        return "analyst"
    return "unverified"


def _score_tweet(text: str, username: str) -> dict:
    """Score tweet for direction, confidence, importance, and impact."""
    text_lower = text.lower()

    escalation_hits = sum(1 for t in ESCALATION_TERMS if t in text_lower)
    denial_hits     = sum(1 for t in DENIAL_TERMS     if t in text_lower)

    credibility = _author_credibility(username)
    profile     = _PROFILE_CACHE.get(username, {})
    cls         = _classify_author(credibility, username)

    if escalation_hits > denial_hits:
        direction  = "bullish"
        confidence = min(95, credibility + escalation_hits * 4)
        impact     = round(escalation_hits * 1.5, 1)
    elif denial_hits > escalation_hits:
        direction  = "bearish"
        confidence = min(95, credibility + denial_hits * 4)
        impact     = round(-denial_hits * 1.5, 1)
    else:
        direction  = "neutral"
        confidence = max(20, credibility - 20)
        impact     = 0.0

    importance = min(100, credibility + escalation_hits * 8 + denial_hits * 5)

    return {
        "direction":      direction,
        "confidence":     confidence,
        "importance":     importance,
        "impact":         impact,
        "classification": cls,
        "credibility":    credibility,
        "escalation_hits": escalation_hits,
        "followers":      profile.get("followers", 0),
        "verified":       profile.get("verified", False),
    }


def search_tweets(query: str, max_results: int = 20) -> list:
    if not TWITTER_API_KEY:
        return []
    try:
        r = requests.get(
            f"{TWITTERAPI_BASE}/tweet/search/recent",
            headers=_headers(),
            params={
                "query": query,
                "max_results": max_results,
                "tweet.fields": "created_at,author_id,public_metrics,author",
            },
            timeout=12,
        )
        if r.status_code == 200:
            data = r.json()
            return data.get("data", data.get("tweets", []))
    except Exception:
        pass
    return []


def get_user_timeline(username: str, max_results: int = 10) -> list:
    if not TWITTER_API_KEY:
        return []
    try:
        r = requests.get(
            f"{TWITTERAPI_BASE}/user/last_tweets",
            headers=_headers(),
            params={"userName": username, "maxResults": max_results},
            timeout=10,
        )
        if r.status_code == 200:
            return r.json().get("tweets", [])
    except Exception:
        pass
    return []


# ── Rotating query index ──────────────────────────────────────────────────────
_query_idx = 0


def run_pulse_cycle() -> list:
    """
    Run one full pulse cycle.
    - Searches 3 rotating queries (20 tweets each = ~60 tweets/cycle)
    - Monitors 4 high-priority accounts (5 tweets each)
    - Scores each tweet with AI-derived author credibility
    - Returns newly stored Signal objects
    """
    global _query_idx
    if not TWITTER_API_KEY:
        return []

    new_signals = []
    seen_texts: set = set()

    # 1. Rotating search queries — pick 3 from the list each cycle
    cycle_queries = []
    for i in range(3):
        cycle_queries.append(SEARCH_QUERIES[(_query_idx + i) % len(SEARCH_QUERIES)])
    _query_idx = (_query_idx + 3) % len(SEARCH_QUERIES)

    for query in cycle_queries:
        tweets = search_tweets(query, max_results=20)
        for tweet in tweets:
            text = tweet.get("text", tweet.get("full_text", ""))
            if not text or text[:60] in seen_texts:
                continue
            seen_texts.add(text[:60])

            # Try to get username from nested author field
            author_obj = tweet.get("author", tweet.get("user", {}))
            username = (
                author_obj.get("userName") or
                author_obj.get("username") or
                author_obj.get("screen_name") or
                tweet.get("author_username") or
                tweet.get("author_id", "unknown")
            )

            score = _score_tweet(text, username)

            # Filter out noise — require some escalation or decent credibility
            if score["importance"] < 28 and score["escalation_hits"] == 0:
                continue

            followers = score["followers"]
            verified  = score["verified"]
            credibility = score["credibility"]
            cls = score["classification"]

            reasoning = (
                f"[{cls}] @{username} "
                f"(cred:{credibility} flw:{_fmt_followers(followers)}"
                f"{' ✓verified' if verified else ''}) — "
                f"{score['escalation_hits']} escalation term(s). "
                f"Query: '{query}'"
            )

            sig = Signal(
                source_app="pulse",
                timestamp=tweet.get("created_at", datetime.now(timezone.utc).isoformat()),
                headline=text[:120],
                raw_text=text,
                signal_direction=score["direction"],
                confidence_score=score["confidence"],
                importance_score=score["importance"],
                probability_impact_estimate=score["impact"],
                reasoning=reasoning,
                link=f"https://twitter.com/i/web/status/{tweet.get('id', tweet.get('tweet_id', ''))}",
                market_slug=MARKET_SLUG,
            )
            if store_signal(sig):
                new_signals.append(sig)

    # 2. High-priority account monitoring
    priority_accounts = ["DeptofDefense", "CENTCOM", "StateDept", "iranintl", "VOAIran"]
    for account in priority_accounts:
        tweets = get_user_timeline(account, max_results=5)
        for tweet in tweets:
            text = tweet.get("text", tweet.get("full_text", ""))
            if not text or text[:60] in seen_texts:
                continue
            iran_relevant = any(
                kw in text.lower()
                for kw in ["iran", "tehran", "irgc", "persian", "hormuz", "nuclear"]
            )
            if not iran_relevant:
                continue
            seen_texts.add(text[:60])
            score = _score_tweet(text, account)

            credibility = score["credibility"]
            cls = score["classification"]
            profile = _PROFILE_CACHE.get(account, {})
            followers = profile.get("followers", CREDIBILITY_MAP.get(account, 50) * 10000)
            verified  = profile.get("verified", credibility >= 85)

            reasoning = (
                f"[{cls}] Official account @{account} "
                f"(cred:{credibility} flw:{_fmt_followers(followers)}"
                f"{' ✓verified' if verified else ''})"
            )

            sig = Signal(
                source_app="pulse",
                timestamp=tweet.get("created_at", datetime.now(timezone.utc).isoformat()),
                headline=text[:120],
                raw_text=text,
                signal_direction=score["direction"],
                confidence_score=score["confidence"],
                importance_score=score["importance"],
                probability_impact_estimate=score["impact"],
                reasoning=reasoning,
                link=f"https://twitter.com/{account}",
                market_slug=MARKET_SLUG,
            )
            if store_signal(sig):
                new_signals.append(sig)

    return new_signals


def _fmt_followers(n: int) -> str:
    """Human-readable follower count."""
    if n >= 1_000_000:
        return f"{n/1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n/1_000:.0f}K"
    return str(n)
