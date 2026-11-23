"""StreamFree mirror discovery and player-link harvester (delegates _0x tokens to EasyProxy)."""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from datetime import datetime, timezone
from typing import Any, Optional
from urllib.parse import urlencode, urljoin, urlsplit

import httpx
from bs4 import BeautifulSoup

from app.config import Settings
from app.core.network import create_doh_client
from app.models.domain import ProviderEvent, StreamSource
from app.providers.base import BaseStreamProvider, ProviderHarvestResult
from app.taxonomy import detect_demographics_from_stream_title

logger = logging.getLogger(__name__)

DOMAINS_API_URL = "https://strmfree.link/api/domains"
FALLBACK_MIRROR = "https://streamfree.top"
CHROME_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)
RESOLVE_CACHE_TTL_SECONDS = 300.0
_MATCH_ENDPOINTS: tuple[str, ...] = (
    "/streams",
    "/api/v1/matches",
    "/api/matches",
    "/api/v1/events",
    "/",
)

_PLAYER_PATH_RE = re.compile(r"/player/([^/]+)/([^/?#]+)", re.IGNORECASE)
_EVENT_SPLIT_RE = re.compile(r"\s+(?:vs\.?|v\.?|@|–|—|-)\s+", re.IGNORECASE)
_QUALITY_RE = re.compile(r"\b(4k|2160p|1080p|fhd|720p|hd|540p|sd)\b", re.IGNORECASE)
_OX_RE = re.compile(r"const\s+_0x\s*=\s*(\{.*?\})\s*;", re.DOTALL)


def _normalize_mirror_url(candidate: str) -> str:
    raw = (candidate or "").strip().rstrip("/")
    if not raw:
        return ""
    if not raw.startswith(("http://", "https://")):
        raw = f"https://{raw.lstrip('/')}"
    parsed = urlsplit(raw)
    if not parsed.netloc:
        return ""
    return f"{parsed.scheme}://{parsed.netloc}"


def extract_mirror_url(payload: Any, fallback: str = FALLBACK_MIRROR) -> str:
    """Extract active StreamFree mirror origin from `/api/domains` JSON payload."""
    candidates: list[str] = []
    if isinstance(payload, dict):
        for key in ("active", "domain", "url", "primary"):
            val = payload.get(key)
            if isinstance(val, str) and val.strip():
                candidates.append(val)
        for key in ("domains", "mirrors", "data"):
            val = payload.get(key)
            if isinstance(val, list):
                for entry in val:
                    if isinstance(entry, str):
                        candidates.append(entry)
                    elif isinstance(entry, dict):
                        for subkey in ("domain", "url", "host"):
                            if isinstance(entry.get(subkey), str):
                                candidates.append(entry[subkey])
    elif isinstance(payload, list):
        for entry in payload:
            if isinstance(entry, str):
                candidates.append(entry)
            elif isinstance(entry, dict):
                for subkey in ("domain", "url", "host"):
                    if isinstance(entry.get(subkey), str):
                        candidates.append(entry[subkey])
    elif isinstance(payload, str):
        candidates.append(payload)

    for cand in candidates:
        normalized = _normalize_mirror_url(cand)
        if normalized:
            return normalized
    return _normalize_mirror_url(fallback) or FALLBACK_MIRROR


def _parse_start_ms(item: dict[str, Any]) -> int:
    for key in ("start_ms", "startTimeMs", "timestamp_ms"):
        val = item.get(key)
        if val is not None and str(val).strip():
            try:
                num = int(float(val))
                return num if num > 10_000_000_000 else num * 1000
            except (ValueError, TypeError):
                pass
    for key in (
        "match_timestamp",
        "timestamp",
        "start_timestamp",
        "unix",
        "date",
        "start_time",
        "startTime",
        "kickoff",
    ):
        val = item.get(key)
        if val is None or not str(val).strip():
            continue
        raw = str(val).strip()
        try:
            num = int(float(raw))
            return num if num > 10_000_000_000 else num * 1000
        except (ValueError, TypeError):
            pass
        try:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1000)
        except ValueError:
            continue
    return 0


def _split_title(title: str) -> tuple[str, str]:
    cleaned = re.sub(r"\s+", " ", (title or "").strip())
    if not cleaned:
        return "", ""
    parts = _EVENT_SPLIT_RE.split(cleaned, maxsplit=1)
    if len(parts) == 2 and parts[0].strip() and parts[1].strip():
        return parts[0].strip(), parts[1].strip()
    return cleaned, ""


def _build_player_url(mirror: str, category: str, slug: str, raw_url: str = "") -> str:
    base = mirror.rstrip("/")
    if raw_url:
        m = _PLAYER_PATH_RE.search(urlsplit(raw_url).path or "")
        if m:
            return f"{base}/player/{m.group(1)}/{m.group(2)}"
    cat = (category or "sports").strip().strip("/").lower()
    clean_slug = (slug or "").strip().strip("/")
    if not clean_slug:
        return ""
    return f"{base}/player/{cat}/{clean_slug}"


def _extract_quality(text: str) -> str:
    m = _QUALITY_RE.search(text or "")
    return m.group(1).upper() if m else ""


def _extract_team_field(item: dict[str, Any], *keys: str) -> str:
    for key in keys:
        val = item.get(key)
        if isinstance(val, dict):
            name = str(val.get("name") or val.get("title") or "").strip()
            if name:
                return name
        elif isinstance(val, str) and val.strip():
            return val.strip()
    return ""


def _parse_json_matches(data: Any, mirror: str) -> list[ProviderEvent]:
    items: list[tuple[str, dict[str, Any]]] = []

    if isinstance(data, list):
        for entry in data:
            if isinstance(entry, dict):
                items.append(("", entry))
    elif isinstance(data, dict):
        # Live `/streams` payload: {"streams": {"soccer": [...], "basketball": [...], ...}}
        streams_block = data.get("streams")
        if isinstance(streams_block, dict):
            for cat_key, val in streams_block.items():
                if isinstance(val, list):
                    for entry in val:
                        if isinstance(entry, dict):
                            items.append((str(cat_key), entry))
        elif isinstance(streams_block, list):
            for entry in streams_block:
                if isinstance(entry, dict):
                    items.append(("", entry))
        else:
            unwrapped = None
            for key in ("matches", "events", "data", "results"):
                if isinstance(data.get(key), list):
                    unwrapped = data[key]
                    break
            if unwrapped is not None:
                for entry in unwrapped:
                    if isinstance(entry, dict):
                        items.append(("", entry))
            else:
                for cat_key, val in data.items():
                    if isinstance(val, list):
                        for entry in val:
                            if isinstance(entry, dict):
                                items.append((str(cat_key), entry))

    events: list[ProviderEvent] = []
    for default_cat, item in items:
        cat = str(
            item.get("category")
            or item.get("sport")
            or item.get("sport_slug")
            or item.get("cat")
            or default_cat
            or "sports"
        ).strip()
        home = _extract_team_field(item, "home", "home_team", "team1")
        away = _extract_team_field(item, "away", "away_team", "team2")
        title = str(item.get("title") or item.get("name") or item.get("event") or "").strip()
        if not title and home and away:
            title = f"{home} vs {away}"
        if not title:
            continue
        if not home:
            home, away = _split_title(title)

        competition = str(
            item.get("competition") or item.get("league") or item.get("tournament") or ""
        ).strip()
        start_ms = _parse_start_ms(item)
        stream_key = str(
            item.get("stream_key") or item.get("slug") or item.get("key") or item.get("id") or ""
        ).strip()
        item_url = str(item.get("player_url") or item.get("url") or "").strip()

        sources: list[StreamSource] = []
        raw_streams = item.get("streams") or item.get("sources") or item.get("channels")
        if isinstance(raw_streams, list) and raw_streams:
            for idx, st in enumerate(raw_streams):
                if isinstance(st, dict):
                    st_cat = str(st.get("category") or st.get("cat") or cat).strip()
                    st_slug = str(
                        st.get("stream_key") or st.get("slug") or st.get("key") or stream_key
                    ).strip()
                    st_url = str(st.get("player_url") or st.get("url") or item_url).strip()
                    player_url = _build_player_url(mirror, st_cat, st_slug, st_url)
                    if not player_url:
                        continue
                    st_title = str(
                        st.get("title") or st.get("name") or f"{title} #{idx + 1}"
                    ).strip()
                    quality = str(st.get("quality") or _extract_quality(st_title)).strip()
                    lang = str(st.get("language") or st.get("lang") or "en").strip().lower()
                    sources.append(
                        StreamSource(
                            provider="streamfree",
                            title=st_title,
                            url=player_url,
                            quality=quality,
                            language=lang,
                            priority=5,
                            requires_proxy=True,
                            proxy_host="streamfree",
                            channel_name=st_cat.lower(),
                            raw_url=st_slug or player_url,
                        )
                    )
        if not sources:
            player_url = _build_player_url(mirror, cat, stream_key, item_url)
            if not player_url:
                continue
            quality = str(item.get("quality") or _extract_quality(title)).strip()
            lang = str(item.get("language") or item.get("lang") or "en").strip().lower()
            sources.append(
                StreamSource(
                    provider="streamfree",
                    title=f"{title} ({quality})" if quality else title,
                    url=player_url,
                    quality=quality,
                    language=lang,
                    priority=5,
                    requires_proxy=True,
                    proxy_host="streamfree",
                    channel_name=cat.lower(),
                    raw_url=stream_key or player_url,
                )
            )

        is_women, is_youth = detect_demographics_from_stream_title(title)
        if competition and (not is_women or not is_youth):
            comp_w, comp_y = detect_demographics_from_stream_title(competition)
            is_women = is_women or comp_w
            is_youth = is_youth or comp_y

        events.append(
            ProviderEvent(
                provider="streamfree",
                title=title,
                home=home,
                away=away,
                competition=competition,
                sport_hint=cat.lower(),
                start_ms=start_ms,
                is_women=is_women,
                is_youth=is_youth,
                streams=sources,
            )
        )
    return events


def _parse_html_matches(html: str, mirror: str) -> list[ProviderEvent]:
    soup = BeautifulSoup(html, "html.parser")
    events: list[ProviderEvent] = []
    seen_urls: set[str] = set()

    for a in soup.find_all("a", href=True):
        m = _PLAYER_PATH_RE.search(str(a["href"]))
        if not m:
            continue
        cat, slug = m.group(1), m.group(2)
        player_url = _build_player_url(mirror, cat, slug)
        if not player_url or player_url in seen_urls:
            continue
        seen_urls.add(player_url)

        title = a.get_text(" ", strip=True) or slug.replace("-", " ").title()
        home, away = _split_title(title)
        quality = _extract_quality(title)
        is_women, is_youth = detect_demographics_from_stream_title(title)
        events.append(
            ProviderEvent(
                provider="streamfree",
                title=title,
                home=home,
                away=away,
                sport_hint=cat.lower(),
                is_women=is_women,
                is_youth=is_youth,
                streams=[
                    StreamSource(
                        provider="streamfree",
                        title=title,
                        url=player_url,
                        quality=quality,
                        language="en",
                        priority=5,
                        requires_proxy=True,
                        proxy_host="streamfree",
                        raw_url=player_url,
                    )
                ],
            )
        )
    return events


def parse_streamfree_matches(payload: Any, mirror_url: str) -> list[ProviderEvent]:
    """Parse StreamFree schedule (JSON API or HTML) into `ProviderEvent` records.

    Always emits `/player/{cat}/{slug}` URLs without resolving ephemeral `_0x`
    tokens, delegating just-in-time extraction to EasyProxy (`host=streamfree`).
    """
    mirror = _normalize_mirror_url(mirror_url) or FALLBACK_MIRROR
    if isinstance(payload, (dict, list)):
        return _parse_json_matches(payload, mirror)
    text = str(payload or "").strip()
    if not text:
        return []
    if text[0] in ("{", "["):
        try:
            return _parse_json_matches(json.loads(text), mirror)
        except json.JSONDecodeError:
            pass
    return _parse_html_matches(text, mirror)


class StreamFreeService(BaseStreamProvider):
    """Async StreamFree mirror discovery and live match harvester."""

    name: str = "streamfree"
    priority: int = 5

    def __init__(
        self,
        settings: Optional[Settings] = None,
        *,
        client: Optional[httpx.AsyncClient] = None,
    ) -> None:
        super().__init__(settings)
        self._external_client = client
        self._owned_client: Optional[httpx.AsyncClient] = None
        self._active_mirror: str = (
            _normalize_mirror_url(self.settings.STREAMFREE_PRIMARY_MIRROR)
            or FALLBACK_MIRROR
        )
        self._resolve_cache: dict[tuple[str, str], tuple[float, list[dict[str, str]]]] = {}

    @property
    def enabled(self) -> bool:
        """Return True when StreamFree harvesting is enabled in Settings."""
        return bool(self.settings.ENABLE_STREAMFREE)

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

    def _fallback_mirrors(self) -> list[str]:
        candidates: list[str] = []
        primary = _normalize_mirror_url(self.settings.STREAMFREE_PRIMARY_MIRROR)
        if primary:
            candidates.append(primary)
        for raw in (self.settings.STREAMFREE_FALLBACK_MIRRORS or "").split(","):
            norm = _normalize_mirror_url(raw)
            if norm and norm not in candidates:
                candidates.append(norm)
        if FALLBACK_MIRROR not in candidates:
            candidates.append(FALLBACK_MIRROR)
        return candidates

    async def discover_mirror(self, client: httpx.AsyncClient) -> str:
        """Query `STREAMFREE_DIRECTORY_URL` to find the active StreamFree mirror."""
        directory_url = (self.settings.STREAMFREE_DIRECTORY_URL or DOMAINS_API_URL).strip()
        primary_fallback = (
            _normalize_mirror_url(self.settings.STREAMFREE_PRIMARY_MIRROR)
            or FALLBACK_MIRROR
        )
        try:
            resp = await client.get(
                directory_url,
                headers={"User-Agent": CHROME_USER_AGENT, "Accept": "application/json"},
            )
            if resp.status_code == 200 and resp.text.strip():
                try:
                    return extract_mirror_url(resp.json(), primary_fallback)
                except ValueError:
                    return extract_mirror_url(resp.text, primary_fallback)
        except Exception as exc:
            logger.debug("StreamFree mirror discovery failed, using fallback: %s", exc)
        return primary_fallback

    async def resolve_stream_url(
        self,
        stream_key: str,
        category: str = "soccer",
        domain: Optional[str] = None,
    ) -> list[dict[str, str]]:
        """Resolve signed `.m3u8` URLs for `stream_key` via sources + routing + `_0x` (300s cache)."""
        key = (stream_key or "").strip()
        if not key:
            return []

        cat = (category or "soccer").strip().lower()
        cache_key = (key, cat)
        now = time.monotonic()
        cached = self._resolve_cache.get(cache_key)
        if cached and now - cached[0] < RESOLVE_CACHE_TTL_SECONDS:
            return cached[1]

        client = self._get_client()
        mirror = _normalize_mirror_url(domain or "") or self._active_mirror
        if not mirror:
            mirror = await self.discover_mirror(client)

        api_headers = {
            "User-Agent": CHROME_USER_AGENT,
            "Accept": "application/json, text/html, */*",
            "Referer": f"{mirror}/player/{cat}/{key}",
            "Origin": mirror,
        }

        sources_req = client.get(f"{mirror}/api/v1/sources/{key}", headers=api_headers)
        route_req = client.get(f"{mirror}/get-stream-key/{key}", headers=api_headers)
        sources_resp, route_resp = await asyncio.gather(
            sources_req, route_req, return_exceptions=True
        )

        sources_list: list[dict[str, Any]] = []
        if isinstance(sources_resp, httpx.Response) and sources_resp.status_code == 200:
            try:
                payload = sources_resp.json()
                raw_src = payload.get("sources") if isinstance(payload, dict) else payload
                if isinstance(raw_src, list):
                    sources_list = [s for s in raw_src if isinstance(s, dict)]
            except Exception:
                pass

        server_name = ""
        if isinstance(route_resp, httpx.Response) and route_resp.status_code == 200:
            try:
                route_data = route_resp.json()
                if isinstance(route_data, dict):
                    server_name = str(route_data.get("server_name") or "").strip().lower()
            except Exception:
                pass

        if server_name == "externalcdn":
            route_prefix = "/live-externalcdn"
        elif server_name == "origin":
            route_prefix = "/live"
        else:
            route_prefix = "/live-cdn"

        resolved: list[dict[str, str]] = []
        player_referer = f"{mirror}/player/{cat}/{key}"

        for src in sources_list:
            embed_url = str(src.get("url") or src.get("embed_url") or "").strip()
            if not embed_url:
                continue
            src_quality = str(src.get("quality") or "").strip()

            try:
                embed_resp = await client.get(
                    embed_url,
                    headers={
                        "User-Agent": CHROME_USER_AGENT,
                        "Referer": player_referer,
                        "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
                    },
                )
                if embed_resp.status_code != 200 or not embed_resp.text:
                    continue
                ox_match = _OX_RE.search(embed_resp.text)
                if not ox_match:
                    continue
                ox_data = json.loads(ox_match.group(1))
            except Exception as exc:
                logger.debug("StreamFree embed _0x extraction failed for %s: %s", embed_url, exc)
                continue

            if not isinstance(ox_data, dict):
                continue

            parsed_embed = urlsplit(embed_url)
            embed_host = parsed_embed.netloc
            embed_scheme = parsed_embed.scheme or "https"
            embed_origin = f"{embed_scheme}://{embed_host}" if embed_host else mirror

            variants: dict[str, dict[str, Any]] = {}
            if "_t" in ox_data or "_e" in ox_data or "_s" in ox_data:
                variants[src_quality or "1080p"] = ox_data
            else:
                for var_name, var_tokens in ox_data.items():
                    if isinstance(var_tokens, dict):
                        variants[str(var_name)] = var_tokens

            for variant, tokens in variants.items():
                params: list[tuple[str, str]] = []
                for tk in ("_t", "_ta", "_e", "_s", "_sa"):
                    if tokens.get(tk) is not None and str(tokens.get(tk)) != "":
                        params.append((tk, str(tokens[tk])))
                if tokens.get("_n") is not None and str(tokens.get("_n")) != "":
                    params.append(("_n", str(tokens["_n"])))
                if not params:
                    continue

                m3u8_url = (
                    f"{embed_origin}{route_prefix}/{variant}/index.m3u8?"
                    f"{urlencode(params)}"
                )
                resolved.append(
                    {
                        "m3u8_url": m3u8_url,
                        "embed_url": embed_url,
                        "origin": f"https://{embed_host}" if embed_host else embed_origin,
                        "quality": src_quality or variant,
                    }
                )

        if resolved:
            self._resolve_cache[cache_key] = (now, resolved)
        return resolved

    async def harvest(self) -> ProviderHarvestResult:
        """Discover active mirror and fetch StreamFree live matches."""
        if not self.enabled:
            return ProviderHarvestResult(provider=self.name)

        client = self._get_client()
        discovered = await self.discover_mirror(client)
        candidate_mirrors = [discovered] + [
            m for m in self._fallback_mirrors() if m != discovered
        ]

        events: list[ProviderEvent] = []
        last_error = ""

        for mirror in candidate_mirrors:
            headers = {
                "User-Agent": CHROME_USER_AGENT,
                "Accept": "application/json, text/html, */*",
                "Referer": f"{mirror}/",
                "Origin": mirror,
            }
            for endpoint in _MATCH_ENDPOINTS:
                url = urljoin(f"{mirror}/", endpoint.lstrip("/"))
                try:
                    resp = await client.get(url, headers=headers)
                    if resp.status_code != 200 or not resp.text.strip():
                        continue
                    parsed = parse_streamfree_matches(resp.text, mirror)
                    if parsed:
                        self._active_mirror = mirror
                        events = parsed
                        break
                except Exception as exc:
                    last_error = str(exc)
                    logger.debug("StreamFree fetch failed for %s: %s", url, exc)
            if events:
                break

        return ProviderHarvestResult(
            provider=self.name,
            events=events,
            raw_count=len(events),
            error=last_error if not events else "",
        )
