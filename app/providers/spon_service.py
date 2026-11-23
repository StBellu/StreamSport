"""Sportsonline / Sportzonline (SPON) live schedule provider with DoH bypass."""

from __future__ import annotations

import logging
import re
import time
from datetime import date, datetime, timedelta, timezone, tzinfo
from typing import Optional
from urllib.parse import urlparse
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import httpx

from app.config import Settings
from app.core.network import create_doh_client
from app.models.domain import ProviderEvent, StreamSource
from app.providers.base import BaseStreamProvider, ProviderHarvestResult
from app.taxonomy.demographics import (
    detect_demographics,
    detect_demographics_from_stream_title,
)

logger = logging.getLogger(__name__)

SPON_PROG_URLS: tuple[str, ...] = (
    "https://sportsonline.st/prog.txt",
    "https://sportzonline.st/prog.txt",
    "https://sportsonline.gl/prog.txt",
    "https://sportsonline.ci/prog.txt",
)

_WEEKDAY_MAP: dict[str, int] = {
    "MONDAY": 0,
    "TUESDAY": 1,
    "WEDNESDAY": 2,
    "THURSDAY": 3,
    "FRIDAY": 4,
    "SATURDAY": 5,
    "SUNDAY": 6,
}

_LANGUAGE_MAP: dict[str, str] = {
    "ITALIAN": "ITA",
    "ITALIANO": "ITA",
    "ITA": "ITA",
    "ENGLISH": "ENG",
    "ENG": "ENG",
    "PORTUGUESE": "POR",
    "BRAZILIAN": "POR",
    "PORTUGUES": "POR",
    "SPANISH": "SPA",
    "ESPANOL": "SPA",
    "FRENCH": "FRA",
    "FRANCAIS": "FRA",
    "GERMAN": "DEU",
    "DEUTSCH": "DEU",
    "DUTCH": "NLD",
    "TURKISH": "TUR",
    "ARABIC": "ARA",
    "POLISH": "POL",
    "ROMANIAN": "RON",
    "GREEK": "ELL",
    "RUSSIAN": "RUS",
}

_ITALIAN_CHANNEL_CODES: frozenset[str] = frozenset({"HD7", "HD8"})

_CHANNEL_LANG_LINE_RE = re.compile(
    r"^\s*([A-Z]{1,4}\d{1,3})\s*[:\-]?\s+([A-Za-z][A-Za-z\s/()\-]{1,32})\s*$",
    re.IGNORECASE,
)
_DAY_HEADER_RE = re.compile(
    r"^\s*(MONDAY|TUESDAY|WEDNESDAY|THURSDAY|FRIDAY|SATURDAY|SUNDAY)\b(.*)$",
    re.IGNORECASE,
)
_LAST_UPDATE_RE = re.compile(
    r"LAST\s+UPDATE\s*:\s*(\d{1,2})-(\d{1,2})-(\d{2,4})",
    re.IGNORECASE,
)
_EXPLICIT_DMY_RE = re.compile(r"\b(\d{1,2})[/.\-](\d{1,2})(?:[/.\-](\d{2,4}))?\b")
_EXPLICIT_YMD_RE = re.compile(r"\b(\d{4})-(\d{1,2})-(\d{1,2})\b")
_SCHEDULE_LINE_RE = re.compile(
    r"^\s*(\d{1,2})[:.](\d{2})\s+(.+?)\s*\|\s*(https?://\S+.*)$",
    re.IGNORECASE,
)
_TEAM_SPLIT_RE = re.compile(r"\s+(?:x|vs\.?|v\.?|–|—)\s+", re.IGNORECASE)
_URL_RE = re.compile(r"https?://[^\s|]+", re.IGNORECASE)
_CHANNEL_CODE_FROM_URL_RE = re.compile(
    r"/([a-z]{1,4}\d{1,3})(?:\.(?:php|html?))?(?:[?#].*)?$",
    re.IGNORECASE,
)


def _last_sunday(year: int, month: int) -> datetime:
    nxt = datetime(year + 1, 1, 1, tzinfo=timezone.utc) if month == 12 else datetime(year, month + 1, 1, tzinfo=timezone.utc)
    last_day = nxt - timedelta(days=1)
    offset = (last_day.weekday() + 1) % 7
    return last_day - timedelta(days=offset)


class _EuropeLondonFallback(tzinfo):
    """Exact GMT/BST fallback when system tzdata is unavailable."""

    def fromutc(self, dt: datetime) -> datetime:
        utc_naive = dt.replace(tzinfo=None)
        start_utc = _last_sunday(utc_naive.year, 3).replace(tzinfo=None, hour=1)
        end_utc = _last_sunday(utc_naive.year, 10).replace(tzinfo=None, hour=1)
        offset = timedelta(hours=1) if start_utc <= utc_naive < end_utc else timedelta(0)
        return (utc_naive + offset).replace(tzinfo=self)

    def utcoffset(self, dt: Optional[datetime]) -> timedelta:
        if dt is None:
            return timedelta(0)
        naive = dt.replace(tzinfo=None)
        start_local = _last_sunday(naive.year, 3).replace(tzinfo=None, hour=1)
        end_local = _last_sunday(naive.year, 10).replace(tzinfo=None, hour=2)
        return timedelta(hours=1) if start_local <= naive < end_local else timedelta(0)

    def dst(self, dt: Optional[datetime]) -> timedelta:
        return timedelta(hours=1) if self.utcoffset(dt) == timedelta(hours=1) else timedelta(0)

    def tzname(self, dt: Optional[datetime]) -> str:
        return "BST" if self.utcoffset(dt) == timedelta(hours=1) else "GMT"


def _load_london_tz() -> tzinfo:
    try:
        return ZoneInfo("Europe/London")
    except ZoneInfoNotFoundError:
        return _EuropeLondonFallback()


_LONDON_TZ: tzinfo = _load_london_tz()


def _normalize_lang_code(raw_lang: str) -> str:
    upper = raw_lang.strip().upper()
    for token, code in _LANGUAGE_MAP.items():
        if token in upper:
            return code
    return upper[:3] if upper else ""


def _extract_channel_code(url: str) -> str:
    parsed = urlparse(url.strip())
    match = _CHANNEL_CODE_FROM_URL_RE.search(parsed.path)
    if match:
        return match.group(1).upper()
    return ""


def _resolve_day_header_date(day_name: str, tail: str, ref_date: date) -> date:
    ymd = _EXPLICIT_YMD_RE.search(tail)
    if ymd:
        try:
            return date(int(ymd.group(1)), int(ymd.group(2)), int(ymd.group(3)))
        except ValueError:
            pass

    dmy = _EXPLICIT_DMY_RE.search(tail)
    if dmy:
        day_num = int(dmy.group(1))
        month_num = int(dmy.group(2))
        year_raw = dmy.group(3)
        year_num = int(year_raw) if year_raw else ref_date.year
        if year_num < 100:
            year_num += 2000
        try:
            return date(year_num, month_num, day_num)
        except ValueError:
            pass

    target_wd = _WEEKDAY_MAP[day_name.upper()]
    delta = (target_wd - ref_date.weekday() + 1) % 7 - 1
    return ref_date + timedelta(days=delta)


def _split_competition_and_teams(raw_title: str) -> tuple[str, str, str]:
    title = re.sub(r"\s+", " ", raw_title).strip()
    competition = ""
    match_part = title

    if ":" in title:
        prefix, rest = title.split(":", 1)
        if not _TEAM_SPLIT_RE.search(prefix) and rest.strip():
            competition = prefix.strip()
            match_part = rest.strip()

    parts = _TEAM_SPLIT_RE.split(match_part, maxsplit=1)
    if len(parts) == 2:
        return competition, parts[0].strip(), parts[1].strip()
    return competition, "", ""


def parse_prog_txt(text: str, now_utc: Optional[datetime] = None) -> list[ProviderEvent]:
    """Parse Sportsonline `prog.txt` text into normalized `ProviderEvent` records."""
    if not text or not text.strip():
        return []

    ref_utc = now_utc or datetime.now(timezone.utc)
    if ref_utc.tzinfo is None:
        ref_utc = ref_utc.replace(tzinfo=timezone.utc)
    ref_london = ref_utc.astimezone(_LONDON_TZ)
    base_date = ref_london.date()
    last_update_match = _LAST_UPDATE_RE.search(text)
    if last_update_match:
        d_num = int(last_update_match.group(1))
        m_num = int(last_update_match.group(2))
        y_num = int(last_update_match.group(3))
        if y_num < 100:
            y_num += 2000
        try:
            base_date = date(y_num, m_num, d_num)
        except ValueError:
            pass
    current_date = base_date

    channel_lang_map: dict[str, tuple[str, str]] = {}
    event_buckets: dict[tuple[int, str], ProviderEvent] = {}
    seen_streams: dict[tuple[int, str], set[str]] = {}

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        day_match = _DAY_HEADER_RE.match(line)
        if day_match and "|" not in line:
            current_date = _resolve_day_header_date(
                day_match.group(1), day_match.group(2), base_date
            )
            continue

        sched_match = _SCHEDULE_LINE_RE.match(line)
        if not sched_match:
            lang_match = _CHANNEL_LANG_LINE_RE.match(line)
            if lang_match:
                ch_code = lang_match.group(1).upper()
                raw_lang = lang_match.group(2).strip()
                norm_lang = _normalize_lang_code(raw_lang)
                channel_lang_map[ch_code] = (norm_lang, raw_lang.upper())
            continue

        hour = int(sched_match.group(1))
        minute = int(sched_match.group(2))
        if hour > 23 or minute > 59:
            continue

        raw_title = sched_match.group(3).strip()
        urls_part = sched_match.group(4).strip()
        urls = _URL_RE.findall(urls_part)
        if not urls:
            continue

        dt_local = datetime(
            current_date.year,
            current_date.month,
            current_date.day,
            hour,
            minute,
            tzinfo=_LONDON_TZ,
        )
        start_ms = int(dt_local.astimezone(timezone.utc).timestamp() * 1000)

        competition, home, away = _split_competition_and_teams(raw_title)
        is_women_t, is_youth_t = detect_demographics_from_stream_title(raw_title)
        is_women_c, is_youth_c = detect_demographics(competition)

        bucket_key = (start_ms, raw_title.lower())
        if bucket_key not in event_buckets:
            event_buckets[bucket_key] = ProviderEvent(
                provider="spon",
                title=raw_title,
                home=home,
                away=away,
                competition=competition,
                start_ms=start_ms,
                is_women=is_women_t or is_women_c,
                is_youth=is_youth_t or is_youth_c,
                streams=[],
            )
            seen_streams[bucket_key] = set()

        event = event_buckets[bucket_key]
        for url in urls:
            if url in seen_streams[bucket_key]:
                continue
            seen_streams[bucket_key].add(url)

            ch_code = _extract_channel_code(url)
            lang_code, raw_lang_label = channel_lang_map.get(ch_code, ("", ""))

            is_italian = (
                ch_code in _ITALIAN_CHANNEL_CODES
                or lang_code == "ITA"
                or "ITALIAN" in raw_lang_label
                or "ITALIAN" in raw_title.upper()
                or "(ITA)" in raw_title.upper()
                or "[ITA]" in raw_title.upper()
            )
            if is_italian:
                final_lang = "it"
                stream_priority = 3
            else:
                final_lang = lang_code or "ENG"
                stream_priority = 31

            quality = "SD" if ch_code.startswith("SD") else "HD"
            if is_italian:
                stream_title = f"[SPON] 🇮🇹 [720p] {ch_code or 'SPON'}"
            else:
                label_parts = [p for p in (ch_code or "SPON", final_lang, quality) if p]
                stream_title = f"[SPON] {' '.join(label_parts)}"

            event.streams.append(
                StreamSource(
                    provider="spon",
                    title=stream_title,
                    url=url,
                    quality=quality,
                    language=final_lang,
                    priority=stream_priority,
                    requires_proxy=True,
                    proxy_host="",
                    channel_name=ch_code,
                    raw_url=url,
                )
            )

    events = list(event_buckets.values())
    for event in events:
        event.streams.sort(key=lambda s: (s.priority, s.channel_name))
    events.sort(key=lambda e: (e.start_ms, e.title))
    return events


class SponService(BaseStreamProvider):
    """Sportsonline / Sportzonline live schedule scraper using DoH DNS bypass."""

    name: str = "spon"
    priority: int = 3

    def __init__(
        self,
        settings: Optional[Settings] = None,
        *,
        client: Optional[httpx.AsyncClient] = None,
        prog_urls: Optional[tuple[str, ...]] = None,
    ) -> None:
        super().__init__(settings)
        self._external_client = client
        self._owned_client: Optional[httpx.AsyncClient] = None
        if prog_urls is not None:
            self._prog_urls = prog_urls
        else:
            candidates = [
                self.settings.SPON_PRIMARY_URL,
                self.settings.SPON_FALLBACK_URL,
                *SPON_PROG_URLS,
            ]
            self._prog_urls = tuple(dict.fromkeys(u for u in candidates if u))
        self._cache_result: Optional[ProviderHarvestResult] = None
        self._cache_expires_at: float = 0.0

    @property
    def enabled(self) -> bool:
        return bool(self.settings.ENABLE_SPON)

    def _get_client(self) -> httpx.AsyncClient:
        if self._external_client is not None:
            return self._external_client
        if self._owned_client is None or self._owned_client.is_closed:
            self._owned_client = create_doh_client(timeout=15.0)
        return self._owned_client

    async def aclose(self) -> None:
        if self._owned_client is not None and not self._owned_client.is_closed:
            await self._owned_client.aclose()
            self._owned_client = None

    async def harvest(self) -> ProviderHarvestResult:
        """Fetch `prog.txt` via DoH across Sportsonline mirrors with in-memory TTL cache."""
        ttl = int(getattr(self.settings, "SPON_CACHE_TTL", 7200) or 0)
        now_mono = time.monotonic()
        if ttl > 0 and self._cache_result is not None and now_mono < self._cache_expires_at:
            return self._cache_result

        client = self._get_client()
        last_error = ""
        prog_text = ""

        for url in self._prog_urls:
            try:
                resp = await client.get(
                    url,
                    headers={"Accept": "text/plain, */*"},
                    timeout=15.0,
                )
                if resp.status_code != 200:
                    last_error = f"HTTP {resp.status_code} from {url}"
                    continue
                if resp.text.strip():
                    prog_text = resp.text
                    break
                last_error = f"Empty response from {url}"
            except Exception as exc:
                last_error = f"{url}: {exc}"
                logger.warning("SponService fetch failed for %s: %s", url, exc)

        if not prog_text:
            return ProviderHarvestResult(provider=self.name, error=last_error or "All SPON mirrors failed")

        events = parse_prog_txt(prog_text)
        result = ProviderHarvestResult(
            provider=self.name,
            events=events,
            raw_count=len(events),
        )
        if ttl > 0:
            self._cache_result = result
            self._cache_expires_at = time.monotonic() + ttl
        return result
