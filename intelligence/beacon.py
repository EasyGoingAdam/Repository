"""
First Strike Beacon — Global News Intelligence Module
Sources: GDELT, RSS feeds (Reuters, AP, BBC, Al Monitor, Al Jazeera, Haaretz,
Times of Israel, IRNA, FARS, Yedioth, NHK, France24, DW, TASS, Xinhua, etc.)
No paid API key required — uses GDELT and public RSS.
Supports multi-language news with automatic translation via free APIs.
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

# ── RSS Feeds — English ──────────────────────────────────────────────────────

RSS_FEEDS_EN = {
    # Wire services
    "Reuters World": "https://feeds.reuters.com/reuters/worldNews",
    "AP Top News": "https://feeds.apnews.com/rss/apf-topnews",
    "BBC World": "https://feeds.bbci.co.uk/news/world/rss.xml",
    # Middle East / defense
    "Al Monitor": "https://www.al-monitor.com/rss",
    "Defense One": "https://www.defenseone.com/rss/all/",
    "Breaking Defense": "https://breakingdefense.com/feed/",
    "Times of Israel": "https://www.timesofisrael.com/feed/",
    "Middle East Eye": "https://www.middleeasteye.net/rss",
    "The National (UAE)": "https://www.thenationalnews.com/rss",
    "Jerusalem Post": "https://www.jpost.com/rss/rssfeedsheadlines.aspx",
    # US politics / policy
    "Politico": "https://rss.politico.com/politics-news.xml",
    "The Hill": "https://thehill.com/feed/",
    "NPR Politics": "https://feeds.npr.org/1014/rss.xml",
    # OSINT / military
    "War on the Rocks": "https://warontherocks.com/feed/",
    "The War Zone": "https://www.thedrive.com/the-war-zone/feed",
}

# ── RSS Feeds — Non-English (require translation) ────────────────────────────

RSS_FEEDS_INTL = {
    # Arabic
    "Al Jazeera Arabic": {
        "url": "https://www.aljazeera.net/aljazeerarss/a7c186be-1baa-4bd4-9d80-a84db769f779/73d0e1b4-532f-45ef-b135-bfdff8b8cab9",
        "lang": "ar",
    },
    # Hebrew
    "Ynet (Hebrew)": {
        "url": "https://www.ynet.co.il/Integration/StoryRss2.xml",
        "lang": "he",
    },
    "Haaretz (Hebrew)": {
        "url": "https://www.haaretz.co.il/cmlink/1.1617539",
        "lang": "he",
    },
    # Persian / Farsi
    "IRNA (Farsi)": {
        "url": "https://www.irna.ir/rss",
        "lang": "fa",
    },
    # French
    "France24 Moyen-Orient": {
        "url": "https://www.france24.com/fr/moyen-orient/rss",
        "lang": "fr",
    },
    # German
    "DW Nahost": {
        "url": "https://rss.dw.com/xml/rss-de-nahost",
        "lang": "de",
    },
    # Russian
    "RT Russian": {
        "url": "https://russian.rt.com/rss",
        "lang": "ru",
    },
    # Turkish
    "TRT Haber": {
        "url": "https://www.trthaber.com/xml_mobile.php?ession=18",
        "lang": "tr",
    },
    # Chinese
    "Xinhua Int'l": {
        "url": "http://www.news.cn/world/rss.xml",
        "lang": "zh",
    },
    # Japanese
    "NHK World": {
        "url": "https://www3.nhk.or.jp/rss/news/cat6.xml",
        "lang": "ja",
    },
}

# ── GDELT queries (multiple keyword sets for broader coverage) ────────────────

GDELT_BASE = "https://api.gdeltproject.org/api/v2/doc/doc"

GDELT_QUERIES = [
    "iran military troops deployment",
    "israel gaza ceasefire hamas",
    "iran nuclear enrichment iaea",
    "us iran sanctions pentagon",
    "hezbollah israel lebanon",
    "iran strait hormuz navy",
    "netanyahu iran strike",
]

# ── Keywords ──────────────────────────────────────────────────────────────────

IRAN_KEYWORDS = [
    "iran", "tehran", "irgc", "iranian", "persian gulf",
    "strait of hormuz", "nuclear deal", "jcpoa", "khamenei",
    "raisi", "pezeshkian", "quds force", "natanz", "fordow",
    "bushehr", "arak", "parchin",
]

ISRAEL_KEYWORDS = [
    "israel", "israeli", "idf", "netanyahu", "tel aviv",
    "jerusalem", "gaza", "hamas", "hezbollah", "west bank",
    "knesset", "mossad", "shin bet", "iron dome", "kibbutz",
    "golan", "rafah", "jenin", "nablus",
]

MILITARY_KEYWORDS = [
    "troops", "military", "soldiers", "deploy", "invasion", "strike",
    "ground forces", "special forces", "war", "combat", "boots",
    "pentagon", "centcom", "armed forces", "airstrikes", "missile",
    "drone", "carrier", "naval", "submarine", "f-35", "b-52",
    "bunker buster", "sortie", "battalion", "brigade", "division",
]

US_POLICY_KEYWORDS = [
    "white house", "state department", "congress", "senate",
    "sanctions", "executive order", "tariff", "diplomatic",
    "ambassador", "envoy", "ceasefire", "peace deal",
    "negotiation", "summit", "bilateral",
]

ALL_RELEVANT_KEYWORDS = IRAN_KEYWORDS + ISRAEL_KEYWORDS + MILITARY_KEYWORDS + US_POLICY_KEYWORDS

OFFICIAL_SOURCES = {
    "Defense One": 85, "Breaking Defense": 82, "Reuters World": 90,
    "AP Top News": 90, "BBC World": 85, "Al Monitor": 78,
    "White House": 95, "DoD": 95, "State Department": 92,
    "Times of Israel": 80, "Jerusalem Post": 78,
    "Middle East Eye": 75, "The National (UAE)": 72,
    "Haaretz (Hebrew)": 82, "Ynet (Hebrew)": 78,
    "IRNA (Farsi)": 65, "Al Jazeera Arabic": 75,
    "France24 Moyen-Orient": 80, "DW Nahost": 78,
    "Politico": 82, "The Hill": 78, "NPR Politics": 85,
    "War on the Rocks": 80, "The War Zone": 78,
    "RT Russian": 55, "Xinhua Int'l": 55,
    "NHK World": 78, "TRT Haber": 65,
}

# Escalation: use short stems/words so "deployed", "deploying", "deploys" all match
ESCALATION_WORDS = [
    "deploy", "invasion", "invade", "invaded", "offensive", "mobiliz",
    "airstrike", "airstrikes", "bombing", "bombed", "missile", "missiles",
    "strike", "strikes", "attack", "attacked", "attacking",
    "troops", "soldiers", "infantry", "marines", "battalion",
    "war ", " war", "wartime", "warfare", "combat",
    "boots on the ground", "ground forces", "ground offensive",
    "special forces", "special ops", "navy seal",
    "carrier group", "battle group", "naval fleet",
    "mobilization", "military buildup", "war footing",
    "declares war", "declaration of war",
    "escalat", "retaliat", "preemptive",
    "nuclear threat", "red line", "ultimatum",
    "crosses border", "enters iran", "invaded iran",
    "fires at", "fires on", "fired at", "fired on",
    "shoots down", "shot down", "intercept",
    "blockade", "siege", "embargo",
    "casualties", "killed", "wounded",
    "explosion", "detonation", "blast",
]

DEESCALATION_WORDS = [
    "ceasefire", "cease-fire", "cease fire",
    "peace talk", "peace deal", "peace agreement", "peace plan",
    "diplomatic", "diplomacy", "negotiat", "mediat",
    "truce", "armistice", "stand down", "de-escalat",
    "withdraw", "pullback", "pull back", "pullout", "pull out",
    "sanctions lifted", "sanctions relief", "sanctions eased",
    "nuclear agreement", "nuclear deal",
    "humanitarian", "aid corridor",
    "back-channel", "backchannel",
    "cooling tension", "tensions eas", "tensions cool",
    "no military", "rules out military",
    "restraint", "calm", "deescalat",
]


# ── Translation ──────────────────────────────────────────────────────────────

def translate_text(text, source_lang, target_lang="en"):
    """
    Translate text using free translation APIs.
    Tries MyMemory API (free, no key needed, 5000 chars/day).
    Falls back to returning original text with lang tag if translation fails.
    """
    if not text or source_lang == "en":
        return text

    text_trimmed = text[:500]  # limit to avoid API issues

    # MyMemory free translation API
    try:
        langpair = f"{source_lang}|{target_lang}"
        r = requests.get(
            "https://api.mymemory.translated.net/get",
            params={"q": text_trimmed, "langpair": langpair},
            timeout=8,
        )
        if r.status_code == 200:
            data = r.json()
            translated = data.get("responseData", {}).get("translatedText", "")
            if translated and translated != text_trimmed:
                return translated
    except Exception:
        pass

    # Fallback: return original with language tag
    return f"[{source_lang}] {text_trimmed}"


# ── Scoring ──────────────────────────────────────────────────────────────────

def _score_article(title, body, source):
    text = (title + " " + body).lower()
    esc = sum(1 for w in ESCALATION_WORDS if w in text)
    deesc = sum(1 for w in DEESCALATION_WORDS if w in text)
    iran_hits = sum(1 for k in IRAN_KEYWORDS if k in text)
    israel_hits = sum(1 for k in ISRAEL_KEYWORDS if k in text)
    mil_hits = sum(1 for k in MILITARY_KEYWORDS if k in text)
    policy_hits = sum(1 for k in US_POLICY_KEYWORDS if k in text)

    credibility = OFFICIAL_SOURCES.get(source, 60)

    # Keyword-based fallback: if an article mentions Iran/Israel + military
    # keywords but no exact escalation phrases, infer direction from context
    if esc == 0 and deesc == 0 and (iran_hits + israel_hits) >= 1:
        if mil_hits >= 3:
            esc = 1  # military-heavy Iran/Israel article = mild escalation
        elif policy_hits >= 2 and mil_hits == 0:
            deesc = 1  # policy-heavy, no military = mild de-escalation

    if esc > deesc:
        direction = "bullish"
        confidence = min(95, credibility + esc * 5)
        impact = round(min(esc * 1.5, 8.0), 1)
    elif deesc > esc:
        direction = "bearish"
        confidence = min(95, credibility + deesc * 4)
        impact = round(max(-deesc * 1.2, -6.0), 1)
    else:
        direction = "neutral"
        confidence = max(30, credibility - 20)
        impact = 0.0

    # Importance: blend credibility with directional signal strength
    # Cap keyword contributions so importance spreads across 40-100 range
    dir_strength = min(max(esc, deesc), 5)  # cap at 5 to avoid all-100
    importance = min(100, int(
        credibility * 0.6
        + dir_strength * 6
        + min(iran_hits, 2) * 2
        + min(israel_hits, 2) * 2
        + min(mil_hits, 3) * 1.5
        + min(policy_hits, 2) * 1
    ))

    return {
        "direction": direction,
        "confidence": int(confidence),
        "importance": int(importance),
        "impact": impact,
        "esc": esc, "deesc": deesc,
        "iran_hits": iran_hits, "israel_hits": israel_hits,
        "mil_hits": mil_hits, "policy_hits": policy_hits,
    }


def _is_relevant(title, body):
    """Check if article is relevant to Iran/Israel/US military/policy."""
    text = (title + " " + body).lower()
    has_region = any(k in text for k in IRAN_KEYWORDS + ISRAEL_KEYWORDS)
    has_topic = any(k in text for k in MILITARY_KEYWORDS + US_POLICY_KEYWORDS)
    # Accept if it mentions region + topic, or has 3+ keyword hits total
    if has_region and has_topic:
        return True
    total_hits = sum(1 for k in ALL_RELEVANT_KEYWORDS if k in text)
    return total_hits >= 3


# ── Deduplication ────────────────────────────────────────────────────────────

_seen_headlines = set()  # in-memory dedup cache (cleared on restart)

def _is_duplicate(title):
    """Check if we've already processed this headline this session."""
    key = hashlib.md5(title.strip().lower().encode()).hexdigest()
    if key in _seen_headlines:
        return True
    _seen_headlines.add(key)
    # Keep cache from growing unbounded
    if len(_seen_headlines) > 5000:
        _seen_headlines.clear()
    return False


# ── RSS Fetching ─────────────────────────────────────────────────────────────

def fetch_rss(feed_name, url):
    try:
        r = requests.get(url, timeout=10, headers={"User-Agent": "FirstStrike/2.0"})
        root = ET.fromstring(r.content)
        items = []
        # Handle both RSS and Atom feeds
        for item in root.iter("item"):
            title = item.findtext("title", "")
            desc = item.findtext("description", "")
            link = item.findtext("link", "")
            pub_date = item.findtext("pubDate", "")
            items.append({"title": title, "description": desc, "link": link, "pub_date": pub_date})
        if not items:
            # Try Atom format
            ns = {"atom": "http://www.w3.org/2005/Atom"}
            for entry in root.iter("{http://www.w3.org/2005/Atom}entry"):
                title = entry.findtext("{http://www.w3.org/2005/Atom}title", "")
                desc = entry.findtext("{http://www.w3.org/2005/Atom}summary", "")
                link_el = entry.find("{http://www.w3.org/2005/Atom}link")
                link = link_el.get("href", "") if link_el is not None else ""
                items.append({"title": title, "description": desc, "link": link})
        return items[:25]
    except Exception:
        return []


def fetch_gdelt(query, timespan="24h", max_records=25):
    """Query GDELT for articles matching a keyword query."""
    try:
        params = {
            "query": query,
            "mode": "artlist",
            "maxrecords": max_records,
            "format": "json",
            "timespan": timespan,
            "sort": "DateDesc",
        }
        r = requests.get(GDELT_BASE, params=params, timeout=15)
        if r.status_code == 200:
            data = r.json()
            return data.get("articles", [])
    except Exception:
        pass
    return []


# ── Main Beacon Cycle ────────────────────────────────────────────────────────

def run_beacon_cycle():
    """
    Run one full beacon cycle:
    1. English RSS feeds
    2. International RSS feeds (with translation)
    3. Multiple GDELT queries
    Returns list of new Signal objects stored.
    """
    new_signals = []

    # 1. English RSS feeds
    for feed_name, url in RSS_FEEDS_EN.items():
        articles = fetch_rss(feed_name, url)
        for art in articles:
            sig = _process_article(art, feed_name, lang="en")
            if sig:
                new_signals.append(sig)

    # 2. International RSS feeds (translate title + description)
    for feed_name, feed_info in RSS_FEEDS_INTL.items():
        articles = fetch_rss(feed_name, feed_info["url"])
        lang = feed_info["lang"]
        for art in articles:
            # Translate title and description
            orig_title = art.get("title", "")
            orig_desc = art.get("description", "")

            translated_title = translate_text(orig_title, lang)
            translated_desc = translate_text(orig_desc, lang)

            # Check relevance on translated text
            art_translated = {
                "title": translated_title,
                "description": translated_desc,
                "link": art.get("link", ""),
            }
            sig = _process_article(art_translated, feed_name, lang=lang,
                                   original_title=orig_title)
            if sig:
                new_signals.append(sig)

        time.sleep(0.5)  # rate limit between international feeds

    # 3. GDELT — multiple query sets for broader coverage
    for query in GDELT_QUERIES:
        gdelt_articles = fetch_gdelt(query)
        for art in gdelt_articles:
            title = art.get("title", "")
            url = art.get("url", "")
            source = art.get("domain", "GDELT")
            tone = float(art.get("tone", 0))
            lang = art.get("language", "English")

            # Translate non-English GDELT articles
            if lang and lang.lower() not in ("english", "en", ""):
                title = translate_text(title, _lang_code(lang))

            if not any(k in title.lower() for k in ALL_RELEVANT_KEYWORDS[:20]):
                continue

            direction = "bullish" if tone < -2 else "bearish" if tone > 2 else "neutral"
            confidence = min(85, 55 + abs(int(tone)) * 3)
            impact = round(tone * -0.5, 1)

            sig = Signal(
                source_app="beacon",
                timestamp=datetime.now(timezone.utc).isoformat(),
                headline=title[:150],
                raw_text=f"GDELT [{query}]. Tone: {tone:.1f}. Source: {source}. Lang: {lang}",
                signal_direction=direction,
                confidence_score=confidence,
                importance_score=min(80, 50 + abs(int(tone)) * 4),
                probability_impact_estimate=impact,
                reasoning=f"GDELT query '{query}'. Tone: {tone:.1f}. Domain: {source}",
                link=url,
                market_slug=MARKET_SLUG,
            )
            if store_signal(sig):
                new_signals.append(sig)

        time.sleep(0.3)  # rate limit between GDELT queries

    return new_signals


def _process_article(art, feed_name, lang="en", original_title=None):
    """Process a single article and return a Signal if relevant, else None."""
    title = art.get("title", "")
    body = art.get("description", "")
    link = art.get("link", "")

    # Deduplicate: skip if we've already seen this headline
    if _is_duplicate(title):
        return None

    if not _is_relevant(title, body):
        return None

    score = _score_article(title, body, feed_name)
    if score["importance"] < 35:
        return None

    # Build headline with language info
    headline = title[:150]
    if lang != "en" and original_title:
        # Include original language title in raw_text for reference
        raw_text = f"[Translated from {lang}] {body[:400]}\n\n[Original] {original_title[:150]}"
    else:
        raw_text = body[:500]

    sig = Signal(
        source_app="beacon",
        timestamp=datetime.now(timezone.utc).isoformat(),
        headline=headline,
        raw_text=raw_text,
        signal_direction=score["direction"],
        confidence_score=score["confidence"],
        importance_score=score["importance"],
        probability_impact_estimate=score["impact"],
        reasoning=(
            f"[{feed_name}] lang={lang} Esc:{score['esc']} De-esc:{score['deesc']} "
            f"Iran:{score['iran_hits']} Israel:{score['israel_hits']} "
            f"Mil:{score['mil_hits']} Policy:{score['policy_hits']}"
        ),
        link=link,
        market_slug=MARKET_SLUG,
    )
    if store_signal(sig):
        return sig
    return None


def _lang_code(gdelt_lang):
    """Map GDELT language names to ISO codes for translation."""
    mapping = {
        "arabic": "ar", "hebrew": "he", "persian": "fa", "farsi": "fa",
        "french": "fr", "german": "de", "russian": "ru", "chinese": "zh",
        "japanese": "ja", "turkish": "tr", "spanish": "es", "portuguese": "pt",
        "korean": "ko", "hindi": "hi", "urdu": "ur", "italian": "it",
    }
    return mapping.get(gdelt_lang.lower(), "en")
