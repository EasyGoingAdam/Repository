"""
First Strike Pulse — X/Twitter Intelligence Module
Uses twitterapi.io (KaitoTwitterAPI) correct endpoints:
  - Search: GET /twitter/tweet/advanced_search  (queryType="Latest")
  - Timeline: GET /twitter/user/last_tweets     (userName=X)
  - Profile:  GET /twitter/user/info            (userName=X)
Response shape: { "tweets": [...], "has_next_page": bool }
Tweet fields: id, text, createdAt, author.userName, author.followers, author.isBlueVerified
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
TWITTERAPI_BASE = "https://api.twitterapi.io"   # NOTE: no /twitter suffix on base

# ── Monitored accounts ────────────────────────────────────────────────────────
PRIORITY_ACCOUNTS = [
    "DeptofDefense", "CENTCOM", "StateDept", "POTUS",
    "iranintl", "VOAIran", "RadioFarda_",
    "Reuters", "AP", "BBCWorld",
]

# ── Search queries (rotated each cycle) ──────────────────────────────────────
SEARCH_QUERIES = [
    "iran troops military",
    "US iran invasion",
    "CENTCOM iran",
    "pentagon iran",
    "iran war 2025",
    "iran ground forces",
    "boots iran ground",
    "iran nuclear strike",
    "IRGC deployment",
    "iran military operation",
    "iran escalation",
    "iran attack US forces",
]

# ── Pre-known credibility scores ──────────────────────────────────────────────
CREDIBILITY_MAP = {
    "DeptofDefense": 95, "CENTCOM": 95, "SecDef": 95, "StateDept": 90,
    "POTUS": 95, "Reuters": 88, "AP": 88, "BBCWorld": 85,
    "AlMonitor": 78, "RALee85": 74, "Natsecjeff": 72,
    "iranintl": 75, "IrnaEnglish": 60, "PressTV": 38,
    "OSINTdefender": 68, "IntelCrab": 66, "WarMonitor_": 63,
    "IranWire": 72, "RadioFarda_": 72, "VOAIran": 75,
    "borzou": 70, "AmbJohnBolton": 72, "KenRoth": 68,
}

ESCALATION_TERMS = [
    "imminent", "immediate", "deployed", "invade", "invasion",
    "war", "strike", "attack", "crossed", "entered", "boots",
    "ground forces", "special operations", "soldiers", "troops",
    "military action", "armed forces", "incursion", "offensive",
    "airstrikes", "bombardment", "special forces", "marines",
    "combat mission", "boots on the ground", "ground invasion",
    "military buildup", "forward deployed",
]

DENIAL_TERMS = [
    "no plans", "not planning", "deny", "denied", "false", "misleading",
    "no troops", "not sending", "peace", "diplomatic", "de-escalate",
    "ceasefire", "negotiations", "sanctions relief", "withdraw",
    "no military action", "diplomatic solution",
]

OFFICIAL_BIO_TERMS = [
    "official", "government", "minister", "secretary", "pentagon",
    "ambassador", "department", "ministry", "general", "admiral",
    "colonel", "senator", "congressman", "white house", "nato",
]

_PROFILE_CACHE: dict = {}
_query_idx: int = 0


def _headers() -> dict:
    return {"X-API-Key": TWITTER_API_KEY}


# ── API calls ─────────────────────────────────────────────────────────────────

def search_tweets(query: str, count: int = 20) -> list:
    """
    Search recent tweets via twitterapi.io advanced_search.
    Correct endpoint: GET /twitter/tweet/advanced_search
    Required params: query (str), queryType ("Latest"|"Top")
    """
    if not TWITTER_API_KEY:
        return []
    try:
        r = requests.get(
            f"{TWITTERAPI_BASE}/twitter/tweet/advanced_search",
            headers=_headers(),
            params={"query": query, "queryType": "Latest"},
            timeout=15,
        )
        if r.status_code == 200:
            return r.json().get("tweets", [])
        else:
            print(f"[Pulse] search error {r.status_code}: {r.text[:200]}")
    except Exception as e:
        print(f"[Pulse] search exception: {e}")
    return []


def get_user_timeline(username: str, count: int = 10) -> list:
    """
    Fetch a user's recent tweets.
    Correct endpoint: GET /twitter/user/last_tweets
    Required params: userName (str)
    """
    if not TWITTER_API_KEY:
        return []
    try:
        r = requests.get(
            f"{TWITTERAPI_BASE}/twitter/user/last_tweets",
            headers=_headers(),
            params={"userName": username},
            timeout=12,
        )
        if r.status_code == 200:
            return r.json().get("tweets", [])
        else:
            print(f"[Pulse] timeline error {r.status_code} for @{username}: {r.text[:200]}")
    except Exception as e:
        print(f"[Pulse] timeline exception for @{username}: {e}")
    return []


def get_user_profile(username: str) -> dict:
    """
    Fetch Twitter user profile for credibility scoring.
    Correct endpoint: GET /twitter/user/info
    Response: { "data": { "userName", "followers", "isBlueVerified", "description", ... } }
    """
    if username in _PROFILE_CACHE:
        return _PROFILE_CACHE[username]
    if not TWITTER_API_KEY:
        return {}
    try:
        r = requests.get(
            f"{TWITTERAPI_BASE}/twitter/user/info",
            headers=_headers(),
            params={"userName": username},
            timeout=10,
        )
        if r.status_code == 200:
            raw = r.json()
            data = raw.get("data", raw)
            profile = {
                "followers":    data.get("followers", 0),
                "verified":     bool(data.get("isBlueVerified", False)),
                "description":  (data.get("description", "") or "").lower(),
                "name":         data.get("name", username),
            }
            _PROFILE_CACHE[username] = profile
            return profile
        else:
            print(f"[Pulse] profile error {r.status_code} for @{username}")
    except Exception as e:
        print(f"[Pulse] profile exception for @{username}: {e}")
    _PROFILE_CACHE[username] = {}
    return {}


def _extract_author(tweet: dict) -> str:
    """Extract username from twitterapi.io tweet object."""
    # twitterapi.io nests author as tweet.author.userName
    author = tweet.get("author", {})
    return (
        author.get("userName") or
        author.get("username") or
        author.get("screen_name") or
        tweet.get("author_username") or
        "unknown"
    )


def _extract_timestamp(tweet: dict) -> str:
    """Extract ISO timestamp — twitterapi.io uses 'createdAt' not 'created_at'."""
    return (
        tweet.get("createdAt") or
        tweet.get("created_at") or
        datetime.now(timezone.utc).isoformat()
    )


def _extract_tweet_id(tweet: dict) -> str:
    return str(tweet.get("id") or tweet.get("tweet_id") or "")


def _author_credibility(username: str) -> int:
    """Compute author credibility 0-100 from pre-known map + live profile data."""
    base = CREDIBILITY_MAP.get(username, 45)
    profile = get_user_profile(username)
    if not profile:
        return base

    followers = profile.get("followers", 0)
    verified  = profile.get("verified", False)
    bio       = profile.get("description", "")

    # Follower count boost
    if followers >= 5_000_000:  base = max(base, 82)
    elif followers >= 1_000_000: base = max(base, 76)
    elif followers >= 500_000:  base = max(base, 70)
    elif followers >= 100_000:  base = max(base, 63)
    elif followers >= 10_000:   base = max(base, 55)

    if verified:
        base = min(95, base + 8)

    official_hits = sum(1 for t in OFFICIAL_BIO_TERMS if t in bio)
    if official_hits >= 2: base = min(95, base + 10)
    elif official_hits == 1: base = min(95, base + 5)

    return min(95, base)


def _classify(credibility: int, username: str) -> str:
    if username in ("DeptofDefense", "CENTCOM", "SecDef", "StateDept", "POTUS"):
        return "official"
    if credibility >= 85: return "official"
    if credibility >= 68: return "semi-confirmed"
    if credibility >= 50: return "analyst"
    return "unverified"


def _score_tweet(text: str, username: str) -> dict:
    text_lower = text.lower()
    esc_hits  = sum(1 for t in ESCALATION_TERMS if t in text_lower)
    deny_hits = sum(1 for t in DENIAL_TERMS if t in text_lower)
    cred      = _author_credibility(username)
    cls       = _classify(cred, username)

    if esc_hits > deny_hits:
        direction  = "bullish"
        confidence = min(95, cred + esc_hits * 4)
        impact     = round(esc_hits * 1.5, 1)
    elif deny_hits > esc_hits:
        direction  = "bearish"
        confidence = min(95, cred + deny_hits * 4)
        impact     = round(-deny_hits * 1.5, 1)
    else:
        direction  = "neutral"
        confidence = max(20, cred - 20)
        impact     = 0.0

    importance = min(100, cred + esc_hits * 8 + deny_hits * 5)
    profile    = _PROFILE_CACHE.get(username, {})

    return {
        "direction":      direction,
        "confidence":     confidence,
        "importance":     importance,
        "impact":         impact,
        "classification": cls,
        "credibility":    cred,
        "escalation_hits": esc_hits,
        "followers":      profile.get("followers", 0),
        "verified":       profile.get("verified", False),
    }


def _fmt_followers(n: int) -> str:
    if n >= 1_000_000: return f"{n/1_000_000:.1f}M"
    if n >= 1_000:     return f"{n/1_000:.0f}K"
    return str(n)


def run_pulse_cycle() -> list:
    """
    Run one full pulse cycle.
    Rotates through 3 search queries (20 results each) + monitors priority accounts.
    Returns list of newly stored Signal objects.
    """
    global _query_idx
    if not TWITTER_API_KEY:
        print("[Pulse] No TWITTER_API_KEY set — skipping cycle")
        return []

    new_signals = []
    seen: set = set()

    # 1. Rotating search queries — 3 per cycle
    queries = [SEARCH_QUERIES[(_query_idx + i) % len(SEARCH_QUERIES)] for i in range(3)]
    _query_idx = (_query_idx + 3) % len(SEARCH_QUERIES)

    for query in queries:
        tweets = search_tweets(query, count=20)
        print(f"[Pulse] query='{query}' → {len(tweets)} tweets")
        for tw in tweets:
            text = tw.get("text", "")
            if not text or text[:60] in seen:
                continue
            seen.add(text[:60])

            username = _extract_author(tw)
            score    = _score_tweet(text, username)

            if score["importance"] < 28 and score["escalation_hits"] == 0:
                continue

            tw_id = _extract_tweet_id(tw)
            reasoning = (
                f"[{score['classification']}] @{username} "
                f"(cred:{score['credibility']} "
                f"flw:{_fmt_followers(score['followers'])}"
                f"{' ✓' if score['verified'] else ''}) — "
                f"{score['escalation_hits']} escalation term(s). "
                f"Query: '{query}'"
            )

            sig = Signal(
                source_app="pulse",
                timestamp=_extract_timestamp(tw),
                headline=text[:120],
                raw_text=text,
                signal_direction=score["direction"],
                confidence_score=score["confidence"],
                importance_score=score["importance"],
                probability_impact_estimate=score["impact"],
                reasoning=reasoning,
                link=f"https://twitter.com/i/web/status/{tw_id}" if tw_id else "",
                market_slug=MARKET_SLUG,
            )
            if store_signal(sig):
                new_signals.append(sig)

    # 2. Priority account monitoring
    for account in PRIORITY_ACCOUNTS:
        tweets = get_user_timeline(account, count=5)
        for tw in tweets:
            text = tw.get("text", "")
            if not text or text[:60] in seen:
                continue
            iran_rel = any(
                kw in text.lower()
                for kw in ["iran", "tehran", "irgc", "persian", "hormuz", "nuclear", "centcom"]
            )
            if not iran_rel:
                continue
            seen.add(text[:60])
            score = _score_tweet(text, account)

            reasoning = (
                f"[{score['classification']}] Official account @{account} "
                f"(cred:{score['credibility']}"
                f"{' ✓' if score['verified'] else ''})"
            )

            sig = Signal(
                source_app="pulse",
                timestamp=_extract_timestamp(tw),
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

    print(f"[Pulse] cycle complete — {len(new_signals)} new signals stored")
    return new_signals
