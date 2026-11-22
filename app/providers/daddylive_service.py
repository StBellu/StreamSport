"""DaddyLive (DLHD) schedule & 24/7 channel scraper with DoH bypass."""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone
from typing import Any, Optional, Sequence
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

from app.config import Settings
from app.core.network import create_doh_client
from app.models.domain import ProviderEvent, StreamSource
from app.providers.base import BaseStreamProvider, ProviderHarvestResult
from app.taxonomy import detect_demographics_from_stream_title

logger = logging.getLogger(__name__)

MAX_CHANNELS_PER_EVENT = 30
DEFAULT_MIRRORS: tuple[str, ...] = (
    "https://dlhd.dad",
    "https://daddylive.mp",
    "https://dlive.sx",
)
_SCHEDULE_PATHS: tuple[str, ...] = (
    "/schedule/schedule-generated.php",
    "/schedule/schedule-generated.json",
    "/api/schedule",
    "/",
)
_CHANNELS_PATHS: tuple[str, ...] = (
    "/24-7-channels.php",
    "/24-7-channels.json",
    "/api/channels",
)

_CHANNEL_ID_RE = re.compile(
    r"(?:watch\.php\?id=|stream-|/watch/|channel_id=)(\d+)",
    re.IGNORECASE,
)
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
_CHANNEL_SUFFIX_NOISE_RE = re.compile(
    r"(?:\b(?:hd|fhd|uhd|sd|4k|it|ita|uk|en|eng|usa|us|es|fr|de|pt|br|vip|backup|server\s*\d*)\b|"
    r"\(\s*(?:it|ita|uk|en|eng|usa|us|hd|fhd)\s*\)|"
    r"\[\s*(?:it|ita|uk|en|eng|usa|us|hd|fhd)\s*\])",
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


def normalize_daddylive_channel_key(name: str) -> str:
    """Normalize a 24/7 channel name for cross-matching with Diretta/EPG broadcasters."""
    cleaned = _CHANNEL_SUFFIX_NOISE_RE.sub(" ", name or "")
    cleaned = re.sub(r"[^\w\s]", " ", cleaned.lower())
    return re.sub(r"\s+", " ", cleaned).strip()


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
    match = _CHANNEL_ID_RE.search(text)
    return match.group(1) if match else ""


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
        cid = _extract_channel_id(ch_id)
        if not cid or cid in seen_ids:
            continue
        seen_ids.add(cid)
        label = re.sub(r"\s+", " ", (ch_name or f"Channel {cid}").strip())
        lang = detect_channel_language(label)
        deduped.append((_language_rank(lang), idx, cid, label, lang))

    deduped.sort(key=lambda item: (item[0], item[1]))
    sources: list[StreamSource] = []
    for _, _, cid, label, lang in deduped[:MAX_CHANNELS_PER_EVENT]:
        watch_url = f"{base}/watch.php?id={cid}"
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
                if not isinstance(raw_items, list):
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

    # Primary structured DLHD DOM: .schedule__day, .schedule__category, .schedule__event
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
                text = node.get_text(" ", strip=True)
                if _TEXT_DATE_RE.search(_ORDINAL_RE.sub(r"\1", text)) or _ISO_DATE_RE.search(text):
                    current_date = _parse_date_header(text, reference_dt)
                else:
                    current_cat = text
                continue

            anchors = [
                a for a in node.find_all("a", href=True)
                if _extract_channel_id(a["href"])
            ]
            if not anchors:
                continue

            time_el = node.select_one(".schedule__time, .time, td.time")
            title_el = node.select_one(
                ".schedule__eventTitle, .event-title, .title, summary, strong"
            )
            time_text = time_el.get_text(" ", strip=True) if time_el else ""
            title_text = title_el.get_text(" ", strip=True) if title_el else ""
            if not title_text:
                # Strip anchor text to reconstruct event title from remaining node text
                clone_texts = [
                    t.strip()
                    for t in node.stripped_strings
                    if t.strip() and not any(t.strip() == a.get_text(" ", strip=True) for a in anchors)
                ]
                joined = " ".join(clone_texts)
                if not time_text:
                    tm = _TIME_HHMM_RE.search(joined)
                    if tm:
                        time_text = tm.group(0)
                        joined = joined.replace(time_text, "", 1).strip(" -:|")
                title_text = joined

            raw_channels = [
                (_extract_channel_id(a["href"]), a.get_text(" ", strip=True))
                for a in anchors
            ]
            ev = _build_provider_event(
                raw_title=title_text,
                category=current_cat,
                start_ms=_parse_start_ms(time_text, current_date),
                raw_channels=raw_channels,
                base_url=base_url,
            )
            if ev is not None:
                events.append(ev)
        if events:
            return events

    # Generic HTML fallback: group channel anchors by immediate parent block
    grouped: dict[int, tuple[Any, list[tuple[str, str]]]] = {}
    for a in soup.find_all("a", href=True):
        cid = _extract_channel_id(a["href"])
        if not cid:
            continue
        parent = a.find_parent(["div", "li", "tr", "p"]) or soup
        pid = id(parent)
        if pid not in grouped:
            grouped[pid] = (parent, [])
        grouped[pid][1].append((cid, a.get_text(" ", strip=True)))

    for parent, raw_channels in grouped.values():
        anchor_labels = {label for _, label in raw_channels if label}
        fragments = [
            s.strip()
            for s in parent.stripped_strings
            if s.strip() and s.strip() not in anchor_labels
        ]
        if not fragments:
            continue
        raw_line = " ".join(fragments)
        tm = _TIME_HHMM_RE.search(raw_line)
        time_text = tm.group(0) if tm else ""
        title_text = raw_line.replace(time_text, "", 1).strip(" -:|") if time_text else raw_line
        ev = _build_provider_event(
            raw_title=title_text,
            category=current_cat,
            start_ms=_parse_start_ms(time_text, current_date),
            raw_channels=raw_channels,
            base_url=base_url,
        )
        if ev is not None:
            events.append(ev)

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


def parse_daddylive_channels(
    payload: str,
    base_url: str,
) -> dict[str, list[StreamSource]]:
    """Parse DaddyLive 24/7 TV channel list (JSON or HTML) into normalized channel map."""
    text = (payload or "").strip()
    if not text:
        return {}
    base = base_url.rstrip("/")
    channels: dict[str, list[StreamSource]] = {}
    seen_per_key: dict[str, set[str]] = {}

    def _add_entry(cid: str, label: str) -> None:
        clean_id = _extract_channel_id(cid)
        clean_label = re.sub(r"\s+", " ", (label or "").strip())
        if not clean_id or not clean_label:
            return
        norm_key = normalize_daddylive_channel_key(clean_label)
        if not norm_key:
            return
        seen = seen_per_key.setdefault(norm_key, set())
        if clean_id in seen:
            return
        seen.add(clean_id)
        lang = detect_channel_language(clean_label)
        watch_url = f"{base}/watch.php?id={clean_id}"
        channels.setdefault(norm_key, []).append(
            StreamSource(
                provider="daddylive",
                title=f"{clean_label} ({lang.upper()})" if lang else clean_label,
                url=watch_url,
                language=lang,
                priority=4,
                requires_proxy=True,
                proxy_host="dlstreams",
                channel_name=clean_label,
                raw_url=watch_url,
            )
        )

    if text[0] in ("{", "["):
        try:
            data = json.loads(text)
            items = (
                data
                if isinstance(data, list)
                else data.get("channels") or data.get("data") or []
                if isinstance(data, dict)
                else []
            )
            for item in items:
                if not isinstance(item, dict):
                    continue
                _add_entry(
                    str(item.get("channel_id") or item.get("id") or item.get("url") or ""),
                    str(item.get("channel_name") or item.get("name") or item.get("title") or ""),
                )
            return channels
        except json.JSONDecodeError:
            pass

    soup = BeautifulSoup(text, "html.parser")
    for a in soup.find_all("a", href=True):
        href = str(a["href"])
        cid = _extract_channel_id(href)
        if not cid:
            continue
        label = a.get_text(" ", strip=True) or str(a.get("title") or "")
        _add_entry(cid, label)

    return channels


class DaddyLiveService(BaseStreamProvider):
    """Async DaddyLive schedule and 24/7 TV channel harvester via DoH."""

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
        self._mirrors: tuple[str, ...] = tuple(
            m.rstrip("/") for m in (mirrors or DEFAULT_MIRRORS) if m.strip()
        )

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

    async def aclose(self) -> None:
        """Close owned DoH client if created internally."""
        if self._owned_client is not None and not self._owned_client.is_closed:
            await self._owned_client.aclose()
            self._owned_client = None

    async def _fetch_first_valid(
        self,
        client: httpx.AsyncClient,
        mirror: str,
        paths: tuple[str, ...],
    ) -> str:
        headers = {"Referer": f"{mirror}/", "Origin": mirror}
        for path in paths:
            url = urljoin(f"{mirror}/", path.lstrip("/"))
            try:
                resp = await client.get(url, headers=headers)
                if resp.status_code == 200 and resp.text.strip():
                    return resp.text
            except Exception as exc:
                logger.debug("DaddyLive fetch failed for %s: %s", url, exc)
        return ""

    async def harvest(self) -> ProviderHarvestResult:
        """Scrape DaddyLive active mirror schedule and 24/7 TV channels."""
        if not self.enabled:
            return ProviderHarvestResult(provider=self.name)

        client = self._get_client()
        events: list[ProviderEvent] = []
        linear_channels: dict[str, list[StreamSource]] = {}
        errors: list[str] = []

        for mirror in self._mirrors:
            try:
                sched_payload = await self._fetch_first_valid(client, mirror, _SCHEDULE_PATHS)
                if sched_payload:
                    events = parse_daddylive_schedule(sched_payload, mirror)

                ch_payload = await self._fetch_first_valid(client, mirror, _CHANNELS_PATHS)
                if ch_payload:
                    linear_channels = parse_daddylive_channels(ch_payload, mirror)

                if events or linear_channels:
                    break
            except Exception as exc:
                errors.append(f"{mirror}: {exc}")
                logger.warning("DaddyLive mirror %s failed: %s", mirror, exc)

        raw_count = len(events) + sum(len(v) for v in linear_channels.values())
        error_msg = "; ".join(errors) if (not events and not linear_channels and errors) else ""
        return ProviderHarvestResult(
            provider=self.name,
            events=events,
            linear_channels=linear_channels,
            raw_count=raw_count,
            error=error_msg,
        )
