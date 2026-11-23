"""DaddyLive (DLHD) schedule scraper with DoH bypass and 18 canonical Italian sports channels."""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Optional, Sequence
from urllib.parse import quote, urljoin, urlsplit

import httpx
from bs4 import BeautifulSoup

from app.config import Settings
from app.core.network import create_doh_client, create_standard_client
from app.graphics._utils import get_timezone
from app.models.domain import ProviderEvent, StreamSource
from app.providers.base import BaseStreamProvider, ProviderHarvestResult
from app.taxonomy import detect_demographics_from_stream_title

logger = logging.getLogger(__name__)

MAX_CHANNELS_PER_EVENT = 30
MIN_SCHEDULE_HTML_BYTES = 5000
EXCLUDED_CATEGORIES: tuple[str, ...] = (
    "tv show",
    "tv shows",
    "upcoming event",
    "upcoming events",
)
DEFAULT_MIRRORS: tuple[str, ...] = (
    "https://thedaddy.li",
    "https://dlhd.pk",
    "https://dlhd.st",
    "https://dlstreams.st",
    "https://thedaddy.pk",
    "https://daddylive.pk",
)
STATUS_CHECKER_URL = "https://daddylive.pk/check_status.php?url=https://dlhd.pk"
CHROME_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
)
PLAYER_TEMPLATES: tuple[str, ...] = (
    "watch.php?id={id}",
    "stream/stream-{id}.php",
    "cast/stream-{id}.php",
    "watch/stream-{id}.php",
    "plus/stream-{id}.php",
    "casting/stream-{id}.php",
    "player/stream-{id}.php",
)
PLAYER_CACHE_TTL_SECONDS = 60.0
BLOCKED_DOMAINS: frozenset[str] = frozenset({"dlive.sx", "dlhd.sx"})

# Canonical 18 Italian sports channels exposed natively on DaddyLive 24/7
DADDYLIVE_CANONICAL_CHANNELS: dict[str, dict[str, str]] = {
    # Linear Sky Sport IT
    "sky sport uno": {"id": "461", "name": "Sky Sport UNO Italy"},
    "sky sport calcio": {"id": "870", "name": "Sky Sport Calcio Italy"},
    "sky sport tennis": {"id": "576", "name": "Sky Sport Tennis Italy"},
    "sky sport arena": {"id": "462", "name": "Sky Sport Arena Italy"},
    "sky sport basket": {"id": "875", "name": "Sky Sport Basket Italy"},
    "sky sport max": {"id": "460", "name": "Sky Sport MAX Italy"},
    "sky sport f1": {"id": "577", "name": "Sky Sport F1 Italy"},
    "sky sport motogp": {"id": "575", "name": "Sky Sport MotoGP Italy"},
    "sky sport golf": {"id": "574", "name": "Sky Sports Golf Italy"},
    "sky sport 24": {"id": "869", "name": "Sky Sport 24 Italy"},
    # Sky Calcio Contemporanea (LCN 251-254)
    "sky sport 251": {"id": "871", "name": "Sky Calcio 1 (251) Italy"},
    "sky sport 252": {"id": "872", "name": "Sky Calcio 2 (252) Italy"},
    "sky sport 253": {"id": "873", "name": "Sky Calcio 3 (253) Italy"},
    "sky sport 254": {"id": "874", "name": "Sky Calcio 4 (254) Italy"},
    # DAZN, Eurosport & Rai Sport
    "dazn 1": {"id": "877", "name": "DAZN ZONA Italy"},
    "eurosport 1": {"id": "878", "name": "EuroSport 1 Italy"},
    "eurosport 2": {"id": "879", "name": "EuroSport 2 Italy"},
    "rai sport": {"id": "882", "name": "Rai Sport Italy"},
}

_SCHEDULE_PATHS: tuple[str, ...] = (
    "/index.php",
    "/schedule/schedule-generated.php",
    "/",
)

_EXTRA_ID_RE = re.compile(r"(?:watchextra\.php\?id=|^extra_)(\d+)", re.IGNORECASE)
_STREAMED_ID_RE = re.compile(r"(?:watchstreamed\.php\?id=|^streamed_)(\d+)", re.IGNORECASE)
_CHANNEL_ID_RE = re.compile(
    r"(?:watch\.php\?id=|[?&]id=|stream-|/watch/|channel_id=)(\d+)(?:\.php)?",
    re.IGNORECASE,
)
_JUNK_CHANNEL_RE = re.compile(
    r"\b(?:channel\s+not\s+listed|just\s+ask\s+in\s+chat|no\s+stream(?:s)?|no\s+channel)\b",
    re.IGNORECASE,
)
_EMOJI_RE = re.compile(r"[\U00010000-\U0010ffff\u2600-\u27bf\ufe0f]+")
_ORDINAL_RE = re.compile(r"\b(\d{1,2})(?:st|nd|rd|th)\b", re.IGNORECASE)
_TIME_HHMM_RE = re.compile(r"\b(\d{1,2}):(\d{2})\b")
_EVENT_SPLIT_RE = re.compile(r"\s+(?:vs\.?|v\.?|@|–|—|-)\s+", re.IGNORECASE)
_ISO_DATE_RE = re.compile(r"\b(\d{4}-\d{2}-\d{2})\b")
_TEXT_DATE_RE = re.compile(
    r"\b(\d{1,2})\s+([A-Za-z]{3,9})\s+(\d{4})\b|\b([A-Za-z]{3,9})\s+(\d{1,2}),?\s+(\d{4})\b"
)

_IT_CHANNEL_RE = re.compile(
    r"(?:\b(?:it|ita|italia|italian|sky\s*sport|dazn|rai\s*(?:1|2|3|sport)?|"
    r"mediaset|canale\s*5|italia\s*1|tv8|nove|cielo|supertennis|solocalcio|"
    r"equtv|eurosport\s*\d*\s*(?:it|ita))\b)",
    re.IGNORECASE,
)
_EN_CHANNEL_RE = re.compile(
    r"(?:\b(?:en|eng|uk|usa|us|english|sky\s*sports|tnt\s*sports|bt\s*sport|"
    r"espn|fox\s*sports?|cbs|nbc|peacock|paramount|tsn|sportsnet|"
    r"premier\s*sports|supersport|optus|setanta|bein\s*sports?\s*(?:en|english)|"
    r"abc|nfl\s*network|nba\s*tv)\b)",
    re.IGNORECASE,
)

_SPORT_HINT_MAP: dict[str, str] = {
    "soccer": "football",
    "football": "football",
    "calcio": "football",
    "tennis": "tennis",
    "basketball": "basketball",
    "nba": "basketball",
    "motorsport": "motorsport",
    "motorsports": "motorsport",
    "f1": "motorsport",
    "formula 1": "motorsport",
    "motogp": "motorsport",
    "boxing": "fighting",
    "mma": "fighting",
    "ufc": "fighting",
    "wwe": "fighting",
    "volleyball": "volleyball",
    "nfl": "american-football",
    "american football": "american-football",
    "ice hockey": "hockey",
    "hockey": "hockey",
    "baseball": "baseball",
    "rugby": "rugby",
    "handball": "handball",
}


def _is_excluded_category(cat_name: str) -> bool:
    lowered = (cat_name or "").strip().lower()
    return any(token in lowered for token in EXCLUDED_CATEGORIES)


def _is_valid_schedule_payload(payload: str) -> bool:
    """Verify payload is non-empty valid schedule JSON or real DLHD schedule HTML."""
    text = (payload or "").strip()
    if not text:
        return False
    if text[0] in ("{", "["):
        try:
            data = json.loads(text)
            return isinstance(data, (dict, list)) and bool(data)
        except json.JSONDecodeError:
            return False
    if len(text.encode("utf-8", errors="ignore")) < MIN_SCHEDULE_HTML_BYTES:
        return False
    return (
        "schedule__category" in text
        or "schedule__dayTitle" in text
        or "schedule__event" in text
    )


def filter_daddylive_events_by_window(
    events: Sequence[ProviderEvent],
    *,
    now_utc: Optional[datetime] = None,
    retention_hours: int = 6,
    timezone_name: str = "Europe/Rome",
) -> list[ProviderEvent]:
    """Keep events from `now - retention_hours` through end of current broadcast day (05:59 next morning)."""
    utc_now = now_utc or datetime.now(timezone.utc)
    if utc_now.tzinfo is None:
        utc_now = utc_now.replace(tzinfo=timezone.utc)
    now_ms = int(utc_now.timestamp() * 1000)
    min_start_ms = now_ms - max(1, int(retention_hours)) * 3600 * 1000

    tz = get_timezone(timezone_name)
    local_now = utc_now.astimezone(tz)
    broadcast_end_local = (
        datetime(local_now.year, local_now.month, local_now.day, 5, 59, 59, tzinfo=tz)
        + timedelta(days=1)
    )
    max_start_ms = int(broadcast_end_local.timestamp() * 1000)

    return [
        ev
        for ev in events
        if ev.start_ms <= 0 or min_start_ms <= ev.start_ms <= max_start_ms
    ]


def detect_channel_language(channel_name: str) -> str:
    """Infer ISO-like language tag (`it`, `en`, or ``) from a DaddyLive channel label."""
    text = (channel_name or "").strip()
    if not text:
        return ""
    if _IT_CHANNEL_RE.search(text):
        return "it"
    if _EN_CHANNEL_RE.search(text):
        return "en"
    return ""


def _language_rank(lang: str) -> int:
    if lang == "it":
        return 0
    if lang == "en":
        return 1
    return 2


def _normalize_sport_hint(raw_cat: str) -> str:
    key = re.sub(r"\s+", " ", (raw_cat or "").strip().lower())
    if key in _SPORT_HINT_MAP:
        return _SPORT_HINT_MAP[key]
    for token, hint in _SPORT_HINT_MAP.items():
        if token in key:
            return hint
    return key


def _extract_channel_id(raw_value: Any) -> str:
    text = str(raw_value or "").strip()
    if not text:
        return ""
    if text.isdigit():
        return text
    extra_m = _EXTRA_ID_RE.search(text)
    if extra_m:
        return f"extra_{extra_m.group(1)}"
    streamed_m = _STREAMED_ID_RE.search(text)
    if streamed_m:
        return f"streamed_{streamed_m.group(1)}"
    match = _CHANNEL_ID_RE.search(text)
    return match.group(1) if match else ""


def _build_watch_url(base_url: str, channel_id: str) -> str:
    base = base_url.rstrip("/")
    if channel_id.startswith("extra_"):
        return f"{base}/watchextra.php?id={channel_id[6:]}"
    if channel_id.startswith("streamed_"):
        return f"{base}/watchstreamed.php?id={channel_id[9:]}"
    return f"{base}/watch.php?id={channel_id}"


def _parse_date_header(date_text: str, reference_dt: datetime) -> datetime:
    cleaned = _ORDINAL_RE.sub(r"\1", (date_text or "").strip())
    iso_m = _ISO_DATE_RE.search(cleaned)
    if iso_m:
        try:
            return datetime.strptime(iso_m.group(1), "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except ValueError:
            pass

    text_m = _TEXT_DATE_RE.search(cleaned)
    if text_m:
        if text_m.group(1):
            candidate = f"{text_m.group(1)} {text_m.group(2)} {text_m.group(3)}"
            formats = ("%d %b %Y", "%d %B %Y")
        else:
            candidate = f"{text_m.group(5)} {text_m.group(4)} {text_m.group(6)}"
            formats = ("%d %b %Y", "%d %B %Y")
        for fmt in formats:
            try:
                return datetime.strptime(candidate, fmt).replace(tzinfo=timezone.utc)
            except ValueError:
                continue

    return reference_dt.astimezone(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    )


def _parse_start_ms(
    time_value: Any,
    date_base: datetime,
    *,
    explicit_ts: Any = None,
) -> int:
    if explicit_ts is not None and str(explicit_ts).strip():
        try:
            val = int(float(str(explicit_ts).strip()))
            if val > 10_000_000_000:
                return val
            if val > 1_000_000_000:
                return val * 1000
        except (ValueError, TypeError):
            pass

    raw = str(time_value or "").strip()
    if raw:
        try:
            iso = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            if iso.tzinfo is None:
                iso = iso.replace(tzinfo=timezone.utc)
            return int(iso.timestamp() * 1000)
        except ValueError:
            pass

        tm = _TIME_HHMM_RE.search(raw)
        if tm:
            hour = int(tm.group(1))
            minute = int(tm.group(2))
            if 0 <= hour <= 23 and 0 <= minute <= 59:
                dt = date_base.replace(hour=hour, minute=minute, second=0, microsecond=0)
                if hour < 6:
                    dt += timedelta(days=1)
                return int(dt.timestamp() * 1000)
    return 0


def _split_event_title(raw_title: str) -> tuple[str, str, str]:
    cleaned = re.sub(r"\s+", " ", (raw_title or "").strip())
    if not cleaned:
        return "", "", ""

    competition = ""
    if ":" in cleaned:
        prefix, rest = cleaned.split(":", 1)
        if rest.strip() and len(prefix.strip()) <= 60:
            competition = prefix.strip()
            cleaned = rest.strip()

    parts = _EVENT_SPLIT_RE.split(cleaned, maxsplit=1)
    if len(parts) == 2 and parts[0].strip() and parts[1].strip():
        return competition, parts[0].strip(), parts[1].strip()
    return competition, cleaned, ""


def _build_channel_sources(
    raw_channels: list[tuple[str, str]],
    base_url: str,
) -> list[StreamSource]:
    base = base_url.rstrip("/")
    deduped: list[tuple[int, int, str, str, str]] = []
    seen_ids: set[str] = set()

    for idx, (ch_id, ch_name) in enumerate(raw_channels):
        if _JUNK_CHANNEL_RE.search(ch_name or ""):
            continue
        cid = _extract_channel_id(ch_id)
        if not cid or cid in ("0", "00") or cid in seen_ids:
            continue
        seen_ids.add(cid)
        label = re.sub(r"\s+", " ", (ch_name or f"Channel {cid}").strip())
        lang = detect_channel_language(label)
        deduped.append((_language_rank(lang), idx, cid, label, lang))

    deduped.sort(key=lambda item: (item[0], item[1]))
    sources: list[StreamSource] = []
    for _, _, cid, label, lang in deduped[:MAX_CHANNELS_PER_EVENT]:
        watch_url = _build_watch_url(base, cid)
        display_title = f"{label} ({lang.upper()})" if lang else label
        sources.append(
            StreamSource(
                provider="daddylive",
                title=display_title,
                url=watch_url,
                language=lang,
                priority=4,
                requires_proxy=True,
                proxy_host="dlstreams",
                channel_name=label,
                raw_url=watch_url,
            )
        )
    return sources


def _collect_json_channels(ev: dict[str, Any]) -> list[tuple[str, str]]:
    collected: list[tuple[str, str]] = []
    for key in ("channels", "channels2", "streams", "sources"):
        block = ev.get(key)
        if not block:
            continue
        items = list(block.values()) if isinstance(block, dict) else list(block) if isinstance(block, list) else []
        for item in items:
            if not isinstance(item, dict):
                continue
            cid = _extract_channel_id(
                item.get("channel_id")
                or item.get("id")
                or item.get("url")
                or item.get("href")
            )
            cname = str(
                item.get("channel_name")
                or item.get("name")
                or item.get("title")
                or (f"Channel {cid}" if cid else "")
            ).strip()
            if cid:
                collected.append((cid, cname))
    return collected


def _build_provider_event(
    *,
    raw_title: str,
    category: str,
    start_ms: int,
    raw_channels: list[tuple[str, str]],
    base_url: str,
) -> Optional[ProviderEvent]:
    sources = _build_channel_sources(raw_channels, base_url)
    if not sources or not raw_title.strip():
        return None

    competition, home, away = _split_event_title(raw_title)
    is_women, is_youth = detect_demographics_from_stream_title(raw_title)
    if competition and (not is_women or not is_youth):
        comp_w, comp_y = detect_demographics_from_stream_title(competition)
        is_women = is_women or comp_w
        is_youth = is_youth or comp_y

    return ProviderEvent(
        provider="daddylive",
        title=raw_title.strip(),
        home=home,
        away=away,
        competition=competition or category.strip(),
        sport_hint=_normalize_sport_hint(category),
        start_ms=start_ms,
        is_women=is_women,
        is_youth=is_youth,
        streams=sources,
    )


def _parse_json_schedule(
    data: Any,
    base_url: str,
    reference_dt: datetime,
) -> list[ProviderEvent]:
    events: list[ProviderEvent] = []

    if isinstance(data, dict):
        # Unwrap {"events": [...]} / {"data": [...]} / {"schedule": {...}}
        for envelope_key in ("events", "data", "matches"):
            if isinstance(data.get(envelope_key), list):
                return _parse_json_event_list(
                    data[envelope_key], base_url, reference_dt, category=""
                )
        if isinstance(data.get("schedule"), dict):
            data = data["schedule"]

        for date_key, categories in data.items():
            date_base = _parse_date_header(str(date_key), reference_dt)
            if isinstance(categories, list):
                events.extend(
                    _parse_json_event_list(categories, base_url, date_base, category="")
                )
                continue
            if not isinstance(categories, dict):
                continue
            for category_name, raw_items in categories.items():
                if not isinstance(raw_items, list) or _is_excluded_category(str(category_name)):
                    continue
                events.extend(
                    _parse_json_event_list(
                        raw_items, base_url, date_base, category=str(category_name)
                    )
                )
        return events

    if isinstance(data, list):
        return _parse_json_event_list(data, base_url, reference_dt, category="")

    return events


def _parse_json_event_list(
    raw_items: list[Any],
    base_url: str,
    date_base: datetime,
    *,
    category: str,
) -> list[ProviderEvent]:
    parsed: list[ProviderEvent] = []
    if _is_excluded_category(category):
        return parsed
    for item in raw_items:
        if not isinstance(item, dict):
            continue
        title = str(item.get("event") or item.get("title") or item.get("name") or "").strip()
        if not title:
            home = str(item.get("home") or "").strip()
            away = str(item.get("away") or "").strip()
            if home and away:
                title = f"{home} vs {away}"
        if not title:
            continue

        item_cat = str(item.get("category") or item.get("sport") or category or "").strip()
        if _is_excluded_category(item_cat):
            continue
        start_ms = _parse_start_ms(
            item.get("time") or item.get("start_time"),
            date_base,
            explicit_ts=item.get("start_ms") or item.get("timestamp") or item.get("unix"),
        )
        raw_channels = _collect_json_channels(item)
        ev = _build_provider_event(
            raw_title=title,
            category=item_cat,
            start_ms=start_ms,
            raw_channels=raw_channels,
            base_url=base_url,
        )
        if ev is not None:
            parsed.append(ev)
    return parsed


def _strip_emojis(text: str) -> str:
    cleaned = _EMOJI_RE.sub(" ", text or "")
    return re.sub(r"\s+", " ", cleaned).strip()


def _extract_header_date_time_title(
    header_text: str,
    data_title: str,
    default_date: datetime,
    reference_dt: datetime,
) -> tuple[datetime, str, str]:
    """Extract event date, HH:MM time, and cleaned competition/match title from eventHeader."""
    raw_visible = _strip_emojis(header_text)
    raw_attr = _strip_emojis(data_title)
    combined = raw_attr or raw_visible

    event_date = default_date
    for candidate_src in (raw_visible, raw_attr):
        if not candidate_src:
            continue
        cleaned_ord = _ORDINAL_RE.sub(r"\1", candidate_src)
        if _TEXT_DATE_RE.search(cleaned_ord) or _ISO_DATE_RE.search(cleaned_ord):
            event_date = _parse_date_header(candidate_src, reference_dt)
            combined = _TEXT_DATE_RE.sub(" ", _ISO_DATE_RE.sub(" ", cleaned_ord))
            combined = re.sub(r"\s+", " ", combined).strip(" -:|,")
            break

    time_text = ""
    for candidate_src in (raw_visible, raw_attr):
        if not candidate_src:
            continue
        tm = _TIME_HHMM_RE.search(candidate_src)
        if tm:
            time_text = tm.group(0)
            break

    if time_text:
        combined = _TIME_HHMM_RE.sub(" ", combined, count=1)
        combined = re.sub(r"\b(?:UK|GMT|UTC)\b", " ", combined, flags=re.IGNORECASE)
        combined = re.sub(r"\s+", " ", combined).strip(" -:|")

    return event_date, time_text, combined


def _parse_html_schedule(
    html: str,
    base_url: str,
    reference_dt: datetime,
) -> list[ProviderEvent]:
    soup = BeautifulSoup(html, "html.parser")
    events: list[ProviderEvent] = []
    current_date = reference_dt.astimezone(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    current_cat = ""

    # Real live thedaddy.li/index.php DOM: .schedule__category -> .schedule__event
    category_blocks = soup.select(".schedule__category")
    if category_blocks:
        for cat_block in category_blocks:
            cat_header = cat_block.select_one(
                ".schedule__catHeader, .schedule__categoryHeader, .schedule__meta, h2, h3, summary"
            )
            cat_name = _strip_emojis(
                cat_header.get_text(" ", strip=True)
                if cat_header
                else str(cat_block.get("data-category") or "")
            )
            if _is_excluded_category(cat_name):
                continue
            if _TEXT_DATE_RE.search(_ORDINAL_RE.sub(r"\1", cat_name)) or _ISO_DATE_RE.search(cat_name):
                current_date = _parse_date_header(cat_name, reference_dt)
            elif cat_name:
                current_cat = cat_name

            for ev_node in cat_block.select(".schedule__event"):
                day_parent = ev_node.find_parent(class_="schedule__day")
                ev_date = current_date
                if day_parent:
                    day_title = day_parent.select_one(".schedule__dayTitle")
                    if day_title:
                        ev_date = _parse_date_header(
                            _strip_emojis(day_title.get_text(" ", strip=True)),
                            reference_dt,
                        )

                ch_container = ev_node.select_one(".schedule__channels") or ev_node
                anchors = [
                    a for a in ch_container.find_all("a", href=True)
                    if _extract_channel_id(a["href"])
                ]
                if not anchors:
                    continue

                header_el = ev_node.select_one(".schedule__eventHeader")
                time_el = ev_node.select_one(".schedule__time, .time, td.time")
                raw_data_time = str(time_el.get("data-time") or "").strip() if time_el else ""
                if header_el is not None:
                    data_title = str(header_el.get("data-title") or "")
                    header_text = header_el.get_text(" ", strip=True)
                    raw_time = raw_data_time or (time_el.get_text(" ", strip=True) if time_el else "")
                    if raw_time and raw_time not in header_text:
                        header_text = f"{raw_time} {header_text}"
                    ev_date, time_text, title_text = _extract_header_date_time_title(
                        header_text, data_title, ev_date, reference_dt
                    )
                    if raw_data_time and not time_text:
                        time_text = raw_data_time
                else:
                    title_el = ev_node.select_one(
                        ".schedule__eventTitle, .event-title, .title, summary, strong"
                    )
                    time_text = raw_data_time or (
                        time_el.get_text(" ", strip=True) if time_el else ""
                    )
                    title_text = _strip_emojis(title_el.get_text(" ", strip=True) if title_el else "")

                if not title_text:
                    continue

                raw_channels = [
                    (
                        _extract_channel_id(a["href"]),
                        _strip_emojis(a.get_text(" ", strip=True) or str(a.get("title") or "")),
                    )
                    for a in anchors
                ]
                ev = _build_provider_event(
                    raw_title=title_text,
                    category=current_cat,
                    start_ms=_parse_start_ms(time_text, ev_date),
                    raw_channels=raw_channels,
                    base_url=base_url,
                )
                if ev is not None:
                    events.append(ev)
        if events:
            return events

    # Legacy structured DLHD DOM: .schedule__dayTitle, .schedule__catHeader, .schedule__event
    structured_nodes = soup.select(
        ".schedule__dayTitle, .schedule__catHeader, .schedule__event, "
        ".card-header, .event, tr[data-event], details"
    )
    if structured_nodes:
        for node in structured_nodes:
            classes = " ".join(node.get("class") or [])
            if "dayTitle" in classes or "schedule__dayTitle" in classes:
                current_date = _parse_date_header(node.get_text(" ", strip=True), reference_dt)
                continue
            if "catHeader" in classes or "card-header" in classes:
                text = _strip_emojis(node.get_text(" ", strip=True))
                if _TEXT_DATE_RE.search(_ORDINAL_RE.sub(r"\1", text)) or _ISO_DATE_RE.search(text):
                    current_date = _parse_date_header(text, reference_dt)
                else:
                    current_cat = text
                continue
            if _is_excluded_category(current_cat):
                continue

            ch_container = node.select_one(".schedule__channels") or node
            anchors = [
                a for a in ch_container.find_all("a", href=True)
                if _extract_channel_id(a["href"])
            ]
            if not anchors:
                continue

            header_el = node.select_one(".schedule__eventHeader")
            time_el = node.select_one(".schedule__time, .time, td.time")
            raw_data_time = str(time_el.get("data-time") or "").strip() if time_el else ""
            ev_date = current_date
            if header_el is not None:
                header_text = header_el.get_text(" ", strip=True)
                raw_time = raw_data_time or (time_el.get_text(" ", strip=True) if time_el else "")
                if raw_time and raw_time not in header_text:
                    header_text = f"{raw_time} {header_text}"
                ev_date, time_text, title_text = _extract_header_date_time_title(
                    header_text,
                    str(header_el.get("data-title") or ""),
                    current_date,
                    reference_dt,
                )
                if raw_data_time and not time_text:
                    time_text = raw_data_time
            else:
                title_el = node.select_one(
                    ".schedule__eventTitle, .event-title, .title, summary, strong"
                )
                time_text = raw_data_time or (
                    time_el.get_text(" ", strip=True) if time_el else ""
                )
                title_text = _strip_emojis(title_el.get_text(" ", strip=True) if title_el else "")
                if not title_text:
                    # Strip anchor text to reconstruct event title from remaining node text
                    clone_texts = [
                        t.strip()
                        for t in node.stripped_strings
                        if t.strip() and not any(t.strip() == a.get_text(" ", strip=True) for a in anchors)
                    ]
                    joined = _strip_emojis(" ".join(clone_texts))
                    if not time_text:
                        tm = _TIME_HHMM_RE.search(joined)
                        if tm:
                            time_text = tm.group(0)
                            joined = joined.replace(time_text, "", 1).strip(" -:|")
                    title_text = joined

            raw_channels = [
                (_extract_channel_id(a["href"]), _strip_emojis(a.get_text(" ", strip=True)))
                for a in anchors
            ]
            ev = _build_provider_event(
                raw_title=title_text,
                category=current_cat,
                start_ms=_parse_start_ms(time_text, ev_date),
                raw_channels=raw_channels,
                base_url=base_url,
            )
            if ev is not None:
                events.append(ev)
        if events:
            return events

    return events


def parse_daddylive_schedule(
    payload: str,
    base_url: str,
    *,
    reference_dt: Optional[datetime] = None,
) -> list[ProviderEvent]:
    """Parse DaddyLive schedule (JSON or HTML) into `ProviderEvent` items.

    Caps channels per event at `MAX_CHANNELS_PER_EVENT` (30), prioritizing
    Italian channels first, then English channels, then other feeds.
    """
    text = (payload or "").strip()
    if not text:
        return []
    ref = reference_dt or datetime.now(timezone.utc)
    if text[0] in ("{", "["):
        try:
            data = json.loads(text)
            return _parse_json_schedule(data, base_url, ref)
        except json.JSONDecodeError:
            pass
    return _parse_html_schedule(text, base_url, ref)


class DaddyLiveService(BaseStreamProvider):
    """Async DaddyLive schedule harvester (DoH) + 18 canonical Italian sports channels."""

    name: str = "daddylive"
    priority: int = 4

    def __init__(
        self,
        settings: Optional[Settings] = None,
        *,
        client: Optional[httpx.AsyncClient] = None,
        mirrors: Optional[Sequence[str]] = None,
    ) -> None:
        super().__init__(settings)
        self._external_client = client
        self._owned_client: Optional[httpx.AsyncClient] = None
        self._probe_client: Optional[httpx.AsyncClient] = None
        self._custom_mirrors: bool = mirrors is not None
        self._mirrors: tuple[str, ...] = tuple(
            m.rstrip("/") for m in (mirrors or DEFAULT_MIRRORS) if m.strip()
        )
        self._active_mirror: str = self._mirrors[0] if self._mirrors else DEFAULT_MIRRORS[0]
        self._player_cache: dict[tuple[str, str], tuple[float, str]] = {}
        self._player_locks: dict[str, asyncio.Lock] = {}
        self._last_good_events: list[ProviderEvent] = []

    def _get_channel_lock(self, cid: str) -> asyncio.Lock:
        lock = self._player_locks.get(cid)
        if lock is None:
            lock = asyncio.Lock()
            self._player_locks[cid] = lock
        return lock

    @staticmethod
    def _is_blocked_mirror(origin: str) -> bool:
        netloc = urlsplit(origin).netloc.lower() or origin.lower()
        return any(blocked in netloc for blocked in BLOCKED_DOMAINS)

    @property
    def enabled(self) -> bool:
        """Return True when DaddyLive harvesting is enabled in Settings."""
        return bool(self.settings.ENABLE_DADDYLIVE)

    def _get_client(self) -> httpx.AsyncClient:
        if self._external_client is not None:
            return self._external_client
        if self._owned_client is None or self._owned_client.is_closed:
            self._owned_client = create_doh_client(timeout=15.0)
        return self._owned_client

    def _get_easyproxy_client(self) -> httpx.AsyncClient:
        if self._external_client is not None:
            return self._external_client
        if self._probe_client is None or self._probe_client.is_closed:
            self._probe_client = create_standard_client(
                timeout=8.0,
                verify=True,
                follow_redirects=False,
            )
        return self._probe_client

    async def aclose(self) -> None:
        """Close owned DoH and EasyProxy HTTP clients if created internally."""
        if self._owned_client is not None and not self._owned_client.is_closed:
            await self._owned_client.aclose()
            self._owned_client = None
        if self._probe_client is not None and not self._probe_client.is_closed:
            await self._probe_client.aclose()
            self._probe_client = None

    @staticmethod
    def _normalize_origin(candidate: str) -> str:
        raw = (candidate or "").strip().rstrip("/")
        if not raw:
            return ""
        if not raw.startswith(("http://", "https://")):
            raw = f"https://{raw.lstrip('/')}"
        parsed = urlsplit(raw)
        if not parsed.netloc:
            return ""
        return f"{parsed.scheme}://{parsed.netloc}"

    async def _discover_mirrors(self, client: httpx.AsyncClient) -> tuple[str, ...]:
        """Query STATUS_CHECKER_URL for canonical mirror with fast fallback on DEFAULT_MIRRORS."""
        if self._custom_mirrors:
            return self._mirrors
        discovered = ""
        try:
            resp = await client.get(
                STATUS_CHECKER_URL,
                headers={"User-Agent": CHROME_USER_AGENT, "Accept": "application/json, text/plain, */*"},
                timeout=5.0,
            )
            if resp.status_code == 200 and resp.text.strip():
                text = resp.text.strip()
                status_offline = False
                if text.startswith("{"):
                    try:
                        data = resp.json()
                        if isinstance(data, dict):
                            raw_status = str(data.get("status") or "").strip().lower()
                            if raw_status and raw_status not in ("online", "ok", "up", "active"):
                                status_offline = True
                            else:
                                for key in ("final_url", "canonical", "active", "redirect", "domain", "url"):
                                    val = data.get(key)
                                    if isinstance(val, str) and val.strip():
                                        discovered = self._normalize_origin(val)
                                        if discovered:
                                            break
                    except ValueError:
                        pass
                if not discovered and not status_offline:
                    m = re.search(r"https?://[^\s\"'<>]+", text)
                    if m:
                        discovered = self._normalize_origin(m.group(0))
        except Exception as exc:
            logger.debug("DaddyLive status checker failed, falling back to DEFAULT_MIRRORS: %s", exc)

        if discovered and self._is_blocked_mirror(discovered):
            logger.debug("Ignoring blocked DaddyLive mirror: %s", discovered)
            discovered = ""

        safe_mirrors = [m for m in self._mirrors if not self._is_blocked_mirror(m)]
        if discovered:
            ordered = [discovered] + [m for m in safe_mirrors if m != discovered]
            return tuple(ordered)
        return tuple(safe_mirrors) or self._mirrors

    async def _fetch_first_valid(
        self,
        client: httpx.AsyncClient,
        mirror: str,
        paths: tuple[str, ...],
    ) -> str:
        headers = {
            "User-Agent": CHROME_USER_AGENT,
            "Referer": f"{mirror}/",
            "Origin": mirror,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
        for path in paths:
            url = urljoin(f"{mirror}/", path.lstrip("/"))
            try:
                resp = await client.get(url, headers=headers)
                if resp.status_code == 200 and _is_valid_schedule_payload(resp.text):
                    return resp.text
            except Exception as exc:
                logger.debug("DaddyLive fetch failed for %s: %s", url, exc)
        return ""

    async def resolve_working_player_url(
        self,
        channel_id: str,
        ep_url: str = "",
        ep_pass: str = "",
    ) -> str:
        """Resolve a working player/EasyProxy URL via 7-player failover cascade (60s TTL cache)."""
        cid = _extract_channel_id(channel_id) or str(channel_id).strip()
        if not cid:
            return ""

        ep_base = (ep_url or "").strip().rstrip("/")
        cache_key = (cid, ep_base)
        now = time.monotonic()
        cached = self._player_cache.get(cache_key)
        if cached and now - cached[0] < PLAYER_CACHE_TTL_SECONDS:
            return cached[1]

        async with self._get_channel_lock(cid):
            now = time.monotonic()
            cached = self._player_cache.get(cache_key)
            if cached and now - cached[0] < PLAYER_CACHE_TTL_SECONDS:
                return cached[1]

            client = self._get_easyproxy_client() if ep_base else self._get_client()
            domain = (self._active_mirror or DEFAULT_MIRRORS[0]).rstrip("/")
            headers = {"User-Agent": CHROME_USER_AGENT, "Referer": f"{domain}/"}

            def _format_target(player_url: str) -> str:
                if not ep_base:
                    return player_url
                return (
                    f"{ep_base}/extractor/video.m3u8?host=dlstreams"
                    f"&d={quote(player_url, safe='')}&redirect_stream=true"
                    f"&api_password={quote(ep_pass or '', safe='')}"
                )

            if cid.startswith("extra_"):
                candidate_paths: tuple[str, ...] = (f"watchextra.php?id={cid[6:]}",)
            elif cid.startswith("streamed_"):
                candidate_paths = (f"watchstreamed.php?id={cid[9:]}",)
            else:
                candidate_paths = tuple(template.format(id=cid) for template in PLAYER_TEMPLATES)

            first_fallback = _format_target(f"{domain}/{candidate_paths[0]}")
            for rel_path in candidate_paths:
                player_url = f"{domain}/{rel_path}"
                target_url = _format_target(player_url)
                try:
                    resp = await client.get(
                        target_url,
                        headers=headers,
                        follow_redirects=False,
                        timeout=6.0,
                    )
                    if resp.status_code in (200, 302, 307):
                        self._player_cache[cache_key] = (time.monotonic(), target_url)
                        return target_url
                except Exception as exc:
                    logger.debug("DaddyLive player probe failed for %s: %s", target_url, exc)

            self._player_cache[cache_key] = (time.monotonic(), first_fallback)
            return first_fallback

    @staticmethod
    def _build_canonical_channels(base_url: str) -> dict[str, list[StreamSource]]:
        base = base_url.rstrip("/")
        channels: dict[str, list[StreamSource]] = {}
        for canon_key, info in DADDYLIVE_CANONICAL_CHANNELS.items():
            cid = info["id"]
            label = info["name"]
            watch_url = _build_watch_url(base, cid)
            channels[canon_key] = [
                StreamSource(
                    provider="daddylive",
                    title=f"{label} (IT)",
                    url=watch_url,
                    language="it",
                    priority=4,
                    requires_proxy=True,
                    proxy_host="dlstreams",
                    channel_name=label,
                    raw_url=watch_url,
                )
            ]
        return channels

    async def harvest(
        self,
        *,
        now_utc: Optional[datetime] = None,
    ) -> ProviderHarvestResult:
        """Scrape DaddyLive active mirror schedule and attach canonical Italian sports channels."""
        if not self.enabled:
            return ProviderHarvestResult(provider=self.name)

        client = self._get_client()
        utc_now = now_utc or datetime.now(timezone.utc)
        if utc_now.tzinfo is None:
            utc_now = utc_now.replace(tzinfo=timezone.utc)

        events: list[ProviderEvent] = []
        errors: list[str] = []
        scraped_any = False

        mirrors = await self._discover_mirrors(client)
        for mirror in mirrors:
            try:
                sched_payload = await self._fetch_first_valid(client, mirror, _SCHEDULE_PATHS)
                if not sched_payload:
                    continue
                parsed_events = parse_daddylive_schedule(
                    sched_payload, mirror, reference_dt=utc_now
                )
                if parsed_events:
                    events = filter_daddylive_events_by_window(
                        parsed_events,
                        now_utc=utc_now,
                        retention_hours=self.settings.MATCH_RETENTION_HOURS,
                        timezone_name=self.settings.TIMEZONE,
                    )
                    self._active_mirror = mirror
                    self._last_good_events = list(events)
                    scraped_any = True
                    break
            except Exception as exc:
                errors.append(f"{mirror}: {exc}")
                logger.warning("DaddyLive mirror %s failed: %s", mirror, exc)

        if not scraped_any and self._last_good_events:
            events = filter_daddylive_events_by_window(
                self._last_good_events,
                now_utc=utc_now,
                retention_hours=self.settings.MATCH_RETENTION_HOURS,
                timezone_name=self.settings.TIMEZONE,
            )
            self._last_good_events = list(events)
            logger.warning(
                "DaddyLive harvest failed across all mirrors; retaining %d cached events",
                len(events),
            )

        active_mirror = self._active_mirror or (mirrors[0] if mirrors else DEFAULT_MIRRORS[0])
        linear_channels = self._build_canonical_channels(active_mirror)

        raw_count = len(events) + sum(len(v) for v in linear_channels.values())
        error_msg = "; ".join(errors) if (not scraped_any and not events and errors) else ""
        return ProviderHarvestResult(
            provider=self.name,
            events=events,
            linear_channels=linear_channels,
            raw_count=raw_count,
            error=error_msg,
        )
