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

# Stores the last cycle's breaking/alert signals for the intel loop to consume
_last_cycle_alerts: list = []

# ── Monitored accounts ────────────────────────────────────────────────────────
PRIORITY_ACCOUNTS = [
    # US Government / Military
    "DeptofDefense", "CENTCOM", "StateDept", "POTUS", "SecDef",
    "USNavy", "USMC", "ArmyChiefStaff",
    # Iran / Middle East news
    "iranintl", "VOAIran", "RadioFarda_", "IranWire", "IrnaEnglish",
    "AJEnglish", "AJArabic",
    # Wire services
    "Reuters", "AP", "BBCWorld", "AFP",
    # Defense / OSINT analysts
    "OSINTdefender", "IntelCrab", "WarMonitor_", "Natsecjeff", "RALee85",
    # Think tanks / policy
    "AmbJohnBolton", "CSIS", "BrookingsInst",
]

# ── Search queries (rotated each cycle) ──────────────────────────────────────
SEARCH_QUERIES = [
    # Boots on the ground
    "boots on the ground iran",
    "US troops iran ground forces",
    "american soldiers iran",
    "special forces iran deployment",
    "marines iran",
    "infantry iran military",
    "US ground invasion iran",
    "troops enter iran",
    "military incursion iran",
    "ground operation iran 2025",
    # Ceasefire / diplomacy
    "iran ceasefire",
    "iran peace deal 2025",
    "iran nuclear agreement",
    "iran diplomacy negotiations",
    "iran sanctions deal",
    "iran JCPOA 2025",
    "iran US diplomacy Oman",
    "iran de-escalation",
    "iran talks Trump",
    # Escalation / strikes
    "US iran invasion 2025",
    "CENTCOM iran",
    "pentagon iran military",
    "iran war 2025",
    "iran nuclear strike",
    "IRGC deployment",
    "iran military operation",
    "iran escalation",
    "US attack iran",
    "airstrikes iran",
    "iran missile attack",
    # Nuclear
    "iran nuclear bomb",
    "iran enrichment weapons",
    "fordow iran",
    "iran nuclear breakout",
    "IAEA iran",
    "iran uranium enrichment",
    # Proxy / regional
    "hezbollah iran attack",
    "houthi iran 2025",
    "iran proxy war",
    "IRGC operation",
    "iran strait of hormuz",
    "hormuz blockade",
    # Key figures
    "khamenei military",
    "trump iran ultimatum",
    "hegseth iran",
    "netanyahu iran attack",
]

# ── Pre-known credibility scores ──────────────────────────────────────────────
CREDIBILITY_MAP = {
    # US official
    "DeptofDefense": 95, "CENTCOM": 95, "SecDef": 95, "StateDept": 90,
    "POTUS": 95, "USNavy": 88, "USMC": 88, "ArmyChiefStaff": 88,
    # Wire services
    "Reuters": 88, "AP": 88, "AFP": 86, "BBCWorld": 85,
    # Iran / regional news
    "iranintl": 76, "VOAIran": 75, "RadioFarda_": 72, "IranWire": 72,
    "IrnaEnglish": 58, "PressTV": 35, "AJEnglish": 74, "AJArabic": 70,
    # OSINT / analysts
    "OSINTdefender": 68, "IntelCrab": 66, "WarMonitor_": 63,
    "RALee85": 74, "Natsecjeff": 72, "AlMonitor": 78,
    # Policy / think tanks
    "AmbJohnBolton": 72, "KenRoth": 68, "CSIS": 76, "BrookingsInst": 76,
    "borzou": 70,
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

# ── Breaking news patterns — phrases that indicate CONFIRMED events ───────────
BREAKING_PHRASES = [
    # Confirmed military action
    "troops entered iran", "soldiers in iran", "boots on the ground in iran",
    "forces crossed into iran", "ground invasion of iran", "us forces in iran",
    "american troops in iran", "military entered iran", "deployed to iran",
    "entered iranian territory", "crossed the border into iran",
    # Ceasefire/peace
    "ceasefire signed", "ceasefire agreed", "ceasefire reached", "ceasefire announced",
    "peace deal signed", "peace agreement reached", "iran ceasefire",
    "iran agreed to ceasefire", "iran accepts ceasefire",
    # War / escalation confirmed
    "war declared", "declaration of war", "iran at war", "state of war",
    "iran confirms attack", "us declares war", "war on iran",
    # Nuclear milestone
    "iran has nuclear weapon", "iran detonated", "nuclear test iran",
    "iran achieved nuclear", "iran bomb confirmed",
    # Official confirmation language
    "#breaking", "breaking:", "just in:", "alert:", "confirmed:",
    "we confirm", "we can confirm", "officially confirmed",
]

# High-credibility accounts whose tweets ALWAYS get reply scanning
TIER1_ACCOUNTS = {
    "DeptofDefense", "CENTCOM", "SecDef", "StateDept", "POTUS",
    "Reuters", "AP", "BBCWorld", "AFP", "iranintl",
}

# Confirmation words found in replies that boost the alert tier
REPLY_CONFIRMATION_TERMS = [
    "confirmed", "breaking", "verified", "multiple sources", "official",
    "developing", "just confirmed", "sources confirm", "now confirmed",
    "several outlets", "can confirm", "#confirmed", "#breaking",
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


def get_tweet_replies(tweet_id: str, count: int = 10) -> list:
    """
    Fetch replies to a tweet using conversation_id search.
    Uses twitterapi.io advanced_search with conversation_id filter.
    """
    if not TWITTER_API_KEY or not tweet_id:
        return []
    try:
        r = requests.get(
            f"{TWITTERAPI_BASE}/twitter/tweet/advanced_search",
            headers=_headers(),
            params={"query": f"conversation_id:{tweet_id}", "queryType": "Latest"},
            timeout=15,
        )
        if r.status_code == 200:
            tweets = r.json().get("tweets", [])
            # Filter out the original tweet — only replies
            return [t for t in tweets if str(t.get("id", "")) != str(tweet_id)][:count]
        else:
            print(f"[Pulse] replies error {r.status_code} for tweet {tweet_id}: {r.text[:120]}")
    except Exception as e:
        print(f"[Pulse] replies exception: {e}")
    return []


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


def _breaking_tier(text: str, username: str, score: dict, replies: list = None) -> str:
    """
    Classify a tweet as 'BREAKING', 'ALERT', or '' (routine).

    BREAKING — immediate email, very high confidence of major event
    ALERT    — important signal, fires with normal cooldown
    ''       — routine, stored but no special alert
    """
    text_lower = text.lower()
    is_tier1 = username in TIER1_ACCOUNTS
    cred = score.get("credibility", 0)
    importance = score.get("importance", 0)

    # Check for breaking phrases
    phrase_hits = sum(1 for p in BREAKING_PHRASES if p in text_lower)

    # Check replies for confirmation
    reply_confirmations = 0
    reply_breaking_phrases = 0
    if replies:
        for reply in replies:
            reply_text = reply.get("text", "").lower()
            reply_confirmations += sum(1 for t in REPLY_CONFIRMATION_TERMS if t in reply_text)
            reply_breaking_phrases += sum(1 for p in BREAKING_PHRASES if p in reply_text)

    # BREAKING tier conditions:
    # 1. Tier 1 account + breaking phrase
    # 2. Very high importance (90+) from high-credibility source (80+) + breaking phrase
    # 3. 2+ breaking phrases regardless of source
    # 4. 3+ reply confirmations from various accounts
    if (is_tier1 and phrase_hits >= 1):
        return "BREAKING"
    if (cred >= 80 and importance >= 88 and phrase_hits >= 1):
        return "BREAKING"
    if phrase_hits >= 2:
        return "BREAKING"
    if reply_breaking_phrases >= 2 or reply_confirmations >= 4:
        return "BREAKING"

    # ALERT tier conditions:
    # High-importance signal from credible source
    if importance >= 75 and cred >= 65:
        return "ALERT"
    if is_tier1 and importance >= 60:
        return "ALERT"
    if phrase_hits >= 1 and cred >= 55:
        return "ALERT"

    return ""


def _scan_replies(tweet_id: str, username: str, importance: int) -> list:
    """
    Scan replies for a tweet if it's high-importance or from a Tier 1 account.
    Returns list of reply tweets (may be empty).
    """
    # Only scan if worth the API call
    if not tweet_id:
        return []
    if importance >= 75 or username in TIER1_ACCOUNTS:
        return get_tweet_replies(tweet_id, count=10)
    return []


def _fmt_followers(n: int) -> str:
    if n >= 1_000_000: return f"{n/1_000_000:.1f}M"
    if n >= 1_000:     return f"{n/1_000:.0f}K"
    return str(n)


def run_pulse_cycle() -> list:
    """
    Run one full pulse cycle.
    Rotates through 3 search queries (20 results each) + monitors priority accounts.
    Returns list of newly stored Signal objects.
    Populates _last_cycle_alerts with any BREAKING/ALERT tier signals detected.
    """
    global _query_idx, _last_cycle_alerts
    _last_cycle_alerts = []

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

            # Check breaking tier (with reply scanning for high-importance)
            replies = _scan_replies(tw_id, username, score["importance"])
            tier = _breaking_tier(text, username, score, replies)
            if tier:
                _last_cycle_alerts.append({
                    "signal_id": sig.id,
                    "tier": tier,
                    "tweet_text": text,
                    "username": username,
                    "link": sig.link,
                    "importance": score["importance"],
                    "credibility": score["credibility"],
                    "direction": score["direction"],
                    "verified": score.get("verified", False),
                    "reply_count": len(replies),
                })

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

            tw_id = _extract_tweet_id(tw)
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

            # Check breaking tier (with reply scanning for high-importance)
            replies = _scan_replies(tw_id, account, score["importance"])
            tier = _breaking_tier(text, account, score, replies)
            if tier:
                _last_cycle_alerts.append({
                    "signal_id": sig.id,
                    "tier": tier,
                    "tweet_text": text,
                    "username": account,
                    "link": sig.link,
                    "importance": score["importance"],
                    "credibility": score["credibility"],
                    "direction": score["direction"],
                    "verified": score.get("verified", False),
                    "reply_count": len(replies),
                })

    print(f"[Pulse] cycle complete — {len(new_signals)} new signals stored, {len(_last_cycle_alerts)} alert(s)")
    return new_signals


def get_last_cycle_alerts() -> list:
    """Return breaking/alert signals detected in the last pulse cycle."""
    return list(_last_cycle_alerts)
