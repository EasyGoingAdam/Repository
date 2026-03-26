"""
Video Feed Monitor — AI channel watcher
Polls YouTube channels for live streams, scans titles + chat for escalation keywords,
stores signals and fires SMS when threshold breached.
"""
from __future__ import annotations
import os
import time
import threading
from datetime import datetime, timezone
from typing import Optional

# ── Monitored channel roster ──────────────────────────────────────────────────
CHANNELS = [
    {"id": "UCshCsg1YVKli8yBai-wa78w", "name": "Agenda Free TV",    "priority": 1},
    {"id": "UCNye-wNBqNL5ZzHSJj3l8Bg", "name": "Al Jazeera English","priority": 2},
    {"id": "UC16niRr50-MSBwiO3YDb3RA", "name": "BBC News",           "priority": 3},
    {"id": "UCupvZG-5ko_eiXAupbDfxWw", "name": "CNN",                "priority": 4},
    {"id": "UCknLrEdhRCp1aegoMqRaCZg", "name": "Iran International", "priority": 5},
    {"id": "UCt-ATdQvbGnGXFSUEJoGlTA", "name": "WION",               "priority": 6},
    {"id": "UCzWQYUVCpZqtN93H8RR44Qw", "name": "DW News",            "priority": 7},
    {"id": "UCIALMKvObZNtJ6AmdCLP_Lg", "name": "Bloomberg TV",       "priority": 8},
]

# ── Escalation keyword tiers ──────────────────────────────────────────────────
TIER1_KEYWORDS = [  # Highest urgency — immediate SMS
    "boots on ground", "us forces enter iran", "iran invaded",
    "war declared", "nuclear strike", "missiles launched at",
    "us troops in iran", "iran attack on us", "war with iran",
    "iran bombs", "iran strikes", "military invasion",
    "centcom confirms", "pentagon confirms",
]
TIER2_KEYWORDS = [  # Important — IMPORTANT level alert
    "iran military", "us iran", "irgc", "tehran strike",
    "ceasefire broken", "nuclear site", "hormuz blocked",
    "drone strike iran", "hegseth iran", "trump iran",
    "khamenei", "persian gulf", "carrier strike group",
    "evacuation order", "emergency landing",
]
TIER3_KEYWORDS = [  # Digest level — monitor
    "iran", "tehran", "middle east crisis", "oil tanker",
    "sanctions", "proxy war", "houthi", "hezbollah",
    "israel iran", "nuclear deal", "fordow", "natanz",
]

# ── State tracking ────────────────────────────────────────────────────────────
_monitor_state: dict = {
    "last_scan": None,
    "channels_live": 0,
    "alerts_fired": 0,
    "channel_status": {},   # channel_id -> {name, live, video_id, title, last_check, keywords_found}
}
_state_lock = threading.Lock()


def get_monitor_state() -> dict:
    with _state_lock:
        return dict(_monitor_state)


def _update_channel_state(ch_id: str, **kwargs):
    with _state_lock:
        if ch_id not in _monitor_state["channel_status"]:
            _monitor_state["channel_status"][ch_id] = {}
        _monitor_state["channel_status"][ch_id].update(kwargs)
        _monitor_state["channel_status"][ch_id]["last_check"] = datetime.now(timezone.utc).isoformat()


def scan_all_channels() -> list:
    """
    Scan all monitored channels for live streams and keyword escalation.
    Returns list of alert dicts for any hits found.
    """
    key = os.environ.get("YOUTUBE_API_KEY", "")
    alerts = []

    if not key:
        # Without API key: mark channels as unknown, return no alerts
        with _state_lock:
            _monitor_state["last_scan"] = datetime.now(timezone.utc).isoformat()
        return []

    try:
        import requests
    except ImportError:
        return []

    live_count = 0

    for ch in CHANNELS:
        ch_id   = ch["id"]
        ch_name = ch["name"]
        try:
            # Step 1: Find current live stream for this channel
            r = requests.get(
                "https://www.googleapis.com/youtube/v3/search",
                params={
                    "part": "snippet",
                    "channelId": ch_id,
                    "type": "video",
                    "eventType": "live",
                    "maxResults": 1,
                    "key": key,
                },
                timeout=8,
            )
            if r.status_code != 200:
                _update_channel_state(ch_id, name=ch_name, live=False, error=f"HTTP {r.status_code}")
                continue

            items = r.json().get("items", [])
            if not items:
                _update_channel_state(ch_id, name=ch_name, live=False, video_id=None, title=None, keywords_found=[])
                continue

            live_count += 1
            item     = items[0]
            video_id = item["id"]["videoId"]
            title    = item["snippet"].get("title", "")
            desc     = item["snippet"].get("description", "")
            combined = (title + " " + desc).lower()

            # Step 2: Keyword scan on title+description
            t1_hits = [k for k in TIER1_KEYWORDS if k in combined]
            t2_hits = [k for k in TIER2_KEYWORDS if k in combined]
            t3_hits = [k for k in TIER3_KEYWORDS if k in combined]
            all_hits = t1_hits + t2_hits + t3_hits

            _update_channel_state(ch_id, name=ch_name, live=True, video_id=video_id,
                                  title=title, keywords_found=all_hits)

            # Step 3: Sample live chat (up to 20 messages)
            chat_hits = []
            try:
                vr = requests.get(
                    "https://www.googleapis.com/youtube/v3/videos",
                    params={"part": "liveStreamingDetails", "id": video_id, "key": key},
                    timeout=6,
                )
                if vr.status_code == 200:
                    vitems = vr.json().get("items", [])
                    if vitems:
                        chat_id = vitems[0].get("liveStreamingDetails", {}).get("activeLiveChatId")
                        if chat_id:
                            cr = requests.get(
                                "https://www.googleapis.com/youtube/v3/liveChat/messages",
                                params={"part": "snippet", "liveChatId": chat_id,
                                        "maxResults": 20, "key": key},
                                timeout=6,
                            )
                            if cr.status_code == 200:
                                msgs = cr.json().get("items", [])
                                for msg in msgs:
                                    txt = msg.get("snippet", {}).get("displayMessage", "").lower()
                                    for k in TIER1_KEYWORDS + TIER2_KEYWORDS:
                                        if k in txt and k not in chat_hits:
                                            chat_hits.append(k)
            except Exception:
                pass

            # Step 4: Build alerts
            if t1_hits or (chat_hits and any(k in TIER1_KEYWORDS for k in chat_hits)):
                alerts.append({
                    "level": "URGENT",
                    "channel": ch_name,
                    "video_id": video_id,
                    "title": title,
                    "keywords": t1_hits + [k for k in chat_hits if k in TIER1_KEYWORDS],
                    "source": "video_monitor",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                })
            elif t2_hits or chat_hits:
                alerts.append({
                    "level": "IMPORTANT",
                    "channel": ch_name,
                    "video_id": video_id,
                    "title": title,
                    "keywords": t2_hits + chat_hits,
                    "source": "video_monitor",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                })

        except Exception as e:
            _update_channel_state(ch_id, name=ch_name, live=False, error=str(e)[:80])
            continue

        time.sleep(0.3)  # be polite to the API

    with _state_lock:
        _monitor_state["last_scan"] = datetime.now(timezone.utc).isoformat()
        _monitor_state["channels_live"] = live_count
        if alerts:
            _monitor_state["alerts_fired"] += len(alerts)

    return alerts


def run_video_monitor_cycle(send_sms_fn=None, store_signal_fn=None) -> dict:
    """
    Full monitoring cycle: scan → store signals → fire SMS.
    Called from the main intel loop every N minutes.
    """
    alerts = scan_all_channels()
    results = {"alerts": len(alerts), "sms_sent": 0}

    for alert in alerts:
        # Store as a signal in the DB
        if store_signal_fn:
            try:
                from intelligence.signals import Signal, MARKET_SLUG
                kws = ", ".join(alert.get("keywords", [])[:3])
                sig = Signal(
                    source_app="video",
                    timestamp=alert["timestamp"],
                    headline=f"[{alert['channel']}] {alert['title'][:120]}",
                    raw_text=f"Keywords detected: {kws}",
                    signal_direction="bullish" if alert["level"] in ("URGENT", "IMPORTANT") else "neutral",
                    confidence_score=90 if alert["level"] == "URGENT" else 70,
                    importance_score=90 if alert["level"] == "URGENT" else 65,
                    probability_impact_estimate=5.0 if alert["level"] == "URGENT" else 2.0,
                    reasoning=f"Video monitor detected escalation keywords: {kws}",
                    link=f"https://www.youtube.com/watch?v={alert.get('video_id','')}",
                    market_slug=MARKET_SLUG,
                )
                store_signal_fn(sig)
            except Exception:
                pass

        # Fire SMS for URGENT / IMPORTANT
        if send_sms_fn and alert["level"] in ("URGENT", "IMPORTANT"):
            try:
                kws = ", ".join(alert.get("keywords", [])[:2])
                headline = f"[VIDEO] {alert['channel']}: {alert['title'][:100]}\nKeywords: {kws}"
                send_sms_fn(alert["level"], headline, odds=None)
                results["sms_sent"] += 1
            except Exception:
                pass

    return results
