"""
First Strike Beacon — News + Official Signals Module
Sources: GDELT, RSS feeds (Reuters, AP, BBC, Al Monitor), free news APIs
No paid API key required — uses GDELT and public RSS.
"""
import os
import re
import time
import hashlib
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from urllib.parse import quote
from .signals import Signal, store_signal, MARKET_SLUG

# RSS feeds — all free, no auth needed
RSS_FEEDS = {
    "Reuters World": "https://feeds.reuters.com/reuters/worldNews",
    "AP Top News": "https://feeds.apnews.com/rss/apf-topnews",
    "BBC World": "https://feeds.bbci.co.uk/news/world/rss.xml",
    "Al Monitor": "https://www.al-monitor.com/rss",
    "Defense One": "https://www.defenseone.com/rss/all/",
    "Breaking Defense": "https://breakingdefense.com/feed/",
}

# GDELT free API endpoint
GDELT_BASE = "https://api.gdeltproject.org/api/v2/doc/doc"

IRAN_KEYWORDS = [
    "iran", "tehran", "irgc", "iranian", "persian gulf",
    "strait of hormuz", "nuclear deal", "jcpoa",
]

MILITARY_KEYWORDS = [
    "troops", "military", "soldiers", "deploy", "invasion", "strike",
    "ground forces", "special forces", "war", "combat", "boots",
    "pentagon", "centcom", "armed forces", "airstrikes",
]

OFFICIAL_SOURCES = {
    "Defense One": 85, "Breaking Defense": 82, "Reuters World": 90,
    "AP Top News": 90, "BBC World": 85, "Al Monitor": 78,
    "White House": 95, "DoD": 95, "State Department": 92,
}

ESCALATION_PHRASES = [
    "sends troops", "deploys forces", "military action", "ground invasion",
    "boots on the ground", "special operations", "armed conflict",
    "declares war", "attacks iran", "enters iran", "crosses border",
    "imminent attack", "strike planned",
]

DEESCALATION_PHRASES = [
    "diplomatic solution", "sanctions lifted", "peace talks", "ceasefire",
    "nuclear agreement", "no military", "withdraw", "de-escalate",
]


def _score_article(title: str, body: str, source: str) -> dict:
    text = (title + " " + body).lower()
    esc = sum(1 for p in ESCALATION_PHRASES if p in text)
    deesc = sum(1 for p in DEESCALATION_PHRASES if p in text)
    iran_hits = sum(1 for k in IRAN_KEYWORDS if k in text)
    mil_hits = sum(1 for k in MILITARY_KEYWORDS if k in text)

    credibility = OFFICIAL_SOURCES.get(source, 60)

    if esc > deesc:
        direction = "bullish"
        confidence = min(95, credibility + esc * 6)
        impact = round(esc * 2.0, 1)
    elif deesc > esc:
        direction = "bearish"
        confidence = min(95, credibility + deesc * 5)
        impact = round(-deesc * 1.5, 1)
    else:
        direction = "neutral"
        confidence = max(30, credibility - 15)
        impact = 0.0

    importance = min(100, credibility + (esc + deesc) * 7 + iran_hits * 3 + mil_hits * 3)

    return {
        "direction": direction,
        "confidence": int(confidence),
        "importance": int(importance),
        "impact": impact,
        "esc": esc,
        "deesc": deesc,
        "iran_hits": iran_hits,
        "mil_hits": mil_hits,
    }


def _is_iran_relevant(title: str, body: str) -> bool:
    text = (title + " " + body).lower()
    has_iran = any(k in text for k in IRAN_KEYWORDS)
    has_military = any(k in text for k in MILITARY_KEYWORDS)
    return has_iran and has_military


def fetch_rss(feed_name: str, url: str) -> list[dict]:
    try:
        r = requests.get(url, timeout=10, headers={"User-Agent": "FirstStrike/1.0"})
        root = ET.fromstring(r.content)
        items = []
        for item in root.iter("item"):
            title = item.findtext("title", "")
            desc = item.findtext("description", "")
            link = item.findtext("link", "")
            pub_date = item.findtext("pubDate", "")
            items.append({"title": title, "description": desc, "link": link, "pub_date": pub_date})
        return items[:20]
    except Exception:
        return []


def fetch_gdelt_iran() -> list[dict]:
    """Query GDELT for recent Iran military events."""
    try:
        params = {
            "query": "iran military troops deployment",
            "mode": "artlist",
            "maxrecords": 20,
            "format": "json",
            "timespan": "24h",
            "sort": "ToneDesc",
        }
        r = requests.get(GDELT_BASE, params=params, timeout=15)
        if r.status_code == 200:
            data = r.json()
            return data.get("articles", [])
    except Exception:
        pass
    return []


def run_beacon_cycle() -> list[Signal]:
    """Run one full beacon cycle — returns new signals stored."""
    new_signals = []

    # 1. RSS feeds
    for feed_name, url in RSS_FEEDS.items():
        articles = fetch_rss(feed_name, url)
        for art in articles:
            title = art.get("title", "")
            body = art.get("description", "")
            link = art.get("link", "")

            if not _is_iran_relevant(title, body):
                continue

            score = _score_article(title, body, feed_name)
            if score["importance"] < 35:
                continue

            sig = Signal(
                source_app="beacon",
                timestamp=datetime.now(timezone.utc).isoformat(),
                headline=title[:150],
                raw_text=body[:500],
                signal_direction=score["direction"],
                confidence_score=score["confidence"],
                importance_score=score["importance"],
                probability_impact_estimate=score["impact"],
                reasoning=(
                    f"[{feed_name}] Escalation terms: {score['esc']}, "
                    f"De-escalation: {score['deesc']}, Iran refs: {score['iran_hits']}, "
                    f"Military refs: {score['mil_hits']}"
                ),
                link=link,
                market_slug=MARKET_SLUG,
            )
            if store_signal(sig):
                new_signals.append(sig)

    # 2. GDELT structured events
    gdelt_articles = fetch_gdelt_iran()
    for art in gdelt_articles:
        title = art.get("title", "")
        url = art.get("url", "")
        source = art.get("domain", "GDELT")
        tone = float(art.get("tone", 0))

        if not any(k in title.lower() for k in IRAN_KEYWORDS):
            continue

        direction = "bullish" if tone < -2 else "bearish" if tone > 2 else "neutral"
        confidence = min(85, 55 + abs(int(tone)) * 3)
        impact = round(tone * -0.5, 1)  # negative tone = more conflict = bullish YES

        sig = Signal(
            source_app="beacon",
            timestamp=datetime.now(timezone.utc).isoformat(),
            headline=title[:150],
            raw_text=f"GDELT event. Tone score: {tone:.1f}. Source: {source}",
            signal_direction=direction,
            confidence_score=confidence,
            importance_score=min(80, 50 + abs(int(tone)) * 4),
            probability_impact_estimate=impact,
            reasoning=f"GDELT structured event. Tone: {tone:.1f} (negative=conflict). Domain: {source}",
            link=url,
            market_slug=MARKET_SLUG,
        )
        if store_signal(sig):
            new_signals.append(sig)

    return new_signals
