"""MandraKodi stream provider supporting XOR payloads, MPD ClearKey, DTT HLS, Socceron, and AceStream."""

from __future__ import annotations

import base64
import binascii
import json
import logging
import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Any, Optional
from urllib.parse import parse_qs, unquote

import httpx

from app.config import Settings
from app.core.network import create_standard_client
from app.models.domain import ProviderEvent, StreamSource
from app.providers.base import BaseStreamProvider, ProviderHarvestResult
from app.providers.tvvoo_service import normalize_channel_name
from app.taxonomy.demographics import (
    detect_demographics,
    detect_demographics_from_stream_title,
)

logger = logging.getLogger(__name__)

_STRICT_B64_RE = re.compile(rb"^[A-Za-z0-9+/=\s]+$")
_ACESTREAM_RE = re.compile(r"acestream://([a-fA-F0-9]{40})", re.IGNORECASE)
_KODIPROP_KEY_RE = re.compile(
    r"(?:inputstream\.adaptive\.license_key|clearkey)\s*[=:]\s*([^\s|;]+)",
    re.IGNORECASE,
)
_TEAM_SPLIT_RE = re.compile(r"\s+(?:vs\.?|v\.?|x|–|—|-)\s+", re.IGNORECASE)
_TIME_PREFIX_RE = re.compile(r"^(?:(\d{1,2})[:.](\d{2})\s*[-|:]?\s*)")
_SPORT_HINTS: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"\b(?:tennis|atp|wta|slam|davis)\b", re.IGNORECASE), "tennis"),
    (re.compile(r"\b(?:nba|basket|euroleague|eurolega|lba)\b", re.IGNORECASE), "basket"),
    (re.compile(r"\b(?:f1|formula\s*1|motogp|moto\s*[23]|superbike)\b", re.IGNORECASE), "motori"),
    (re.compile(r"\b(?:ufc|mma|boxe|boxing|wwe)\b", re.IGNORECASE), "combat"),
    (re.compile(r"\b(?:volley|superlega|pallavolo)\b", re.IGNORECASE), "volley"),
    (re.compile(r"\b(?:nfl|american\s+football)\b", re.IGNORECASE), "nfl"),
    (re.compile(r"\b(?:nhl|hockey)\b", re.IGNORECASE), "hockey"),
    (re.compile(r"\b(?:mlb|baseball)\b", re.IGNORECASE), "baseball"),
    (re.compile(r"\b(?:rugby|six\s+nations)\b", re.IGNORECASE), "rugby"),
    (re.compile(r"\b(?:serie\s*[ab]|champions|europa|conference|premier|liga|calcio|soccer)\b", re.IGNORECASE), "football"),
)


def _xor_bytes(raw: bytes, key: str) -> bytes:
    if not key:
        return raw
    key_bytes = key.encode("utf-8")
    klen = len(key_bytes)
    return bytes(b ^ key_bytes[idx % klen] for idx, b in enumerate(raw))


def xor_encrypt(data: bytes | str, key: str) -> str:
    """XOR-encrypt `data` with repeating `key` and return a base64 ASCII string."""
    raw = data.encode("utf-8") if isinstance(data, str) else data
    xored = _xor_bytes(raw, key)
    return base64.b64encode(xored).decode("ascii")


def xor_decrypt(data: bytes | str, key: str) -> str:
    """XOR-decrypt base64-encoded string or raw XOR bytes using repeating `key`."""
    if not data:
        return ""

    if isinstance(data, str):
        cleaned = data.strip()
        try:
            padded = cleaned + "=" * (-len(cleaned) % 4)
            decoded = base64.b64decode(padded, validate=True)
            return _xor_bytes(decoded, key).decode("utf-8")
        except (binascii.Error, ValueError, UnicodeDecodeError):
            return _xor_bytes(data.encode("latin1"), key).decode("utf-8", errors="replace")

    stripped = data.strip()
    if stripped and len(stripped) % 4 == 0 and _STRICT_B64_RE.match(stripped):
        try:
            decoded = base64.b64decode(stripped, validate=True)
            return _xor_bytes(decoded, key).decode("utf-8")
        except (binascii.Error, ValueError, UnicodeDecodeError):
            pass

    return _xor_bytes(data, key).decode("utf-8", errors="replace")


def _normalize_clearkey(raw_ck: Any, kid: str = "", key: str = "") -> str:
    """Normalize ClearKey representations (`kid:key`, JSON dict, or separate fields) to `kid:key`."""
    if kid and key:
        return f"{kid.strip()}:{key.strip()}"
    if isinstance(raw_ck, dict):
        if "kid" in raw_ck and "key" in raw_ck:
            return f"{str(raw_ck['kid']).strip()}:{str(raw_ck['key']).strip()}"
        if "keys" in raw_ck and isinstance(raw_ck["keys"], list) and raw_ck["keys"]:
            first = raw_ck["keys"][0]
            if isinstance(first, dict) and first.get("kid") and first.get("k"):
                return f"{first['kid']}:{first['k']}"
        pairs = [f"{k}:{v}" for k, v in raw_ck.items() if k and v]
        return ",".join(pairs)

    text = str(raw_ck or "").strip()
    if not text:
        return ""
    if text.startswith("{"):
        try:
            parsed = json.loads(text)
            return _normalize_clearkey(parsed)
        except json.JSONDecodeError:
            pass
    return text


def _split_kodi_pipe_url(raw_url: str) -> tuple[str, dict[str, str], str]:
    """Extract clean URL, HTTP headers, and embedded ClearKey from Kodi `url|Header=Val` syntax."""
    url_part = raw_url.strip()
    headers: dict[str, str] = {}
    embedded_ck = ""

    if "|" not in url_part:
        return url_part, headers, embedded_ck

    base_url, query_part = url_part.split("|", 1)
    for key, vals in parse_qs(query_part, keep_blank_values=False).items():
        if not vals:
            continue
        val = unquote(vals[0]).strip()
        low = key.lower()
        if low in ("ck", "clearkey", "license_key", "key"):
            embedded_ck = val
        elif low == "user-agent":
            headers["User-Agent"] = val
        elif low == "referer":
            headers["Referer"] = val
        elif low == "origin":
            headers["Origin"] = val
        else:
            headers[key] = val

    return base_url.strip(), headers, embedded_ck


def _infer_sport_hint(text: str, explicit_sport: str = "") -> str:
    if explicit_sport:
        return explicit_sport.strip().lower()
    for pattern, sport in _SPORT_HINTS:
        if pattern.search(text):
            return sport
    return ""


def _parse_start_ms(raw_val: Any) -> int:
    if raw_val is None or raw_val == "":
        return 0
    if isinstance(raw_val, (int, float)):
        val = int(raw_val)
        return val * 1000 if val < 10_000_000_000 else val
    text = str(raw_val).strip()
    if text.isdigit():
        val = int(text)
        return val * 1000 if val < 10_000_000_000 else val
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except ValueError:
        pass
    match = re.match(r"^(\d{1,2}):(\d{2})$", text)
    if match:
        now = datetime.now(timezone.utc)
        dt = now.replace(
            hour=int(match.group(1)),
            minute=int(match.group(2)),
            second=0,
            microsecond=0,
        )
        return int(dt.timestamp() * 1000)
    return 0


def _parse_teams_from_title(title: str) -> tuple[str, str]:
    clean = _TIME_PREFIX_RE.sub("", title).strip()
    # Strip trailing channel/language tags in brackets if present
    clean = re.sub(r"\s*\[[^\]]+\]\s*$", "", clean).strip()
    if ":" in clean and not _TEAM_SPLIT_RE.search(clean.split(":", 1)[0]):
        clean = clean.split(":", 1)[1].strip()
    parts = _TEAM_SPLIT_RE.split(clean, maxsplit=1)
    if len(parts) == 2:
        return parts[0].strip(), parts[1].strip()
    return "", ""


def _detect_language(item: dict[str, Any], title: str) -> str:
    explicit = str(item.get("language") or item.get("lang") or "").strip().upper()
    if explicit:
        if explicit in ("IT", "ITA", "ITALIAN", "ITALIANO"):
            return "ITA"
        if explicit in ("EN", "ENG", "ENGLISH", "UK"):
            return "ENG"
        return explicit[:3]
    upper = title.upper()
    if any(tok in upper for tok in ("[ITA]", "(ITA)", "ITALIA", "SKY SPORT", "DAZN", "RAI ", "MEDIASET")):
        return "ITA"
    return "ITA"


class MandraKodiService(BaseStreamProvider):
    """MandraKodi provider for MPD ClearKey, DTT HLS, Socceron events, and AceStream feeds."""

    name: str = "mandrakodi"
    priority: int = 2

    def __init__(
        self,
        settings: Optional[Settings] = None,
        *,
        client: Optional[httpx.AsyncClient] = None,
    ) -> None:
        super().__init__(settings)
        self._external_client = client
        self._owned_client: Optional[httpx.AsyncClient] = None

    @property
    def enabled(self) -> bool:
        return bool(self.settings.ENABLE_MANDRAKODI)

    def _get_client(self) -> httpx.AsyncClient:
        if self._external_client is not None:
            return self._external_client
        if self._owned_client is None or self._owned_client.is_closed:
            self._owned_client = create_standard_client(timeout=15.0)
        return self._owned_client

    async def aclose(self) -> None:
        if self._owned_client is not None and not self._owned_client.is_closed:
            await self._owned_client.aclose()
            self._owned_client = None

    def _build_request_headers(self) -> dict[str, str]:
        headers: dict[str, str] = {"Accept": "application/json, text/xml, */*"}
        if self.settings.MANDRAKODI_DEVICE_ID:
            headers["X-Device-Id"] = self.settings.MANDRAKODI_DEVICE_ID
        if self.settings.MANDRAKODI_PASSWORD:
            headers["X-Auth-Token"] = xor_encrypt(
                self.settings.MANDRAKODI_DEVICE_ID or "streamsport",
                self.settings.MANDRAKODI_PASSWORD,
            )
        return headers

    def _decode_raw_payload(self, raw: str | bytes | dict[str, Any] | list[Any]) -> Any:
        if isinstance(raw, (dict, list)):
            return raw

        text = raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else raw
        stripped = text.strip()
        if not stripped:
            return []

        if stripped.startswith(("{", "[")):
            try:
                parsed = json.loads(stripped)
                if isinstance(parsed, dict) and isinstance(parsed.get("payload"), str):
                    key = self.settings.MANDRAKODI_PASSWORD
                    if key:
                        decrypted = xor_decrypt(parsed["payload"], key)
                        return self._decode_raw_payload(decrypted)
                return parsed
            except json.JSONDecodeError:
                pass

        if stripped.startswith("<"):
            return stripped

        key = self.settings.MANDRAKODI_PASSWORD
        if key:
            decrypted = xor_decrypt(raw, key)
            dec_stripped = decrypted.strip()
            if dec_stripped.startswith(("{", "[")):
                try:
                    return json.loads(dec_stripped)
                except json.JSONDecodeError:
                    pass
            if dec_stripped.startswith("<"):
                return dec_stripped
        return stripped

    def _xml_to_items(self, xml_text: str) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError as exc:
            logger.warning("MandraKodi XML parse failed: %s", exc)
            return items

        for node in root.iter():
            if node.tag.lower() not in ("item", "channel", "entry", "stream"):
                continue
            item: dict[str, Any] = dict(node.attrib)
            for child in node:
                tag = child.tag.lower()
                text = (child.text or "").strip()
                if not text:
                    continue
                if tag in ("title", "name", "label"):
                    item.setdefault("title", text)
                elif tag in ("link", "url", "mpd", "hls", "acestream", "externallink"):
                    item.setdefault("url", text)
                elif tag in ("clearkey", "key", "drm_key", "license_key"):
                    item["clearkey"] = text
                elif tag in ("kid",):
                    item["kid"] = text
                elif tag in ("type", "category", "group", "kind"):
                    item.setdefault("category", text)
                elif tag in ("sport",):
                    item["sport"] = text
                elif tag in ("competition", "league"):
                    item["competition"] = text
                elif tag in ("start", "date", "time", "start_ms"):
                    item.setdefault("start_time", text)
                elif tag in ("lang", "language"):
                    item["language"] = text
            if item.get("title") or item.get("url"):
                items.append(item)
        return items

    def _flatten_json_items(self, data: Any, inherited_category: str = "") -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        if isinstance(data, list):
            for entry in data:
                items.extend(self._flatten_json_items(entry, inherited_category))
            return items

        if not isinstance(data, dict):
            return items

        cat = str(data.get("category") or data.get("group") or data.get("type") or inherited_category)
        has_nested = False
        for key in ("items", "channels", "events", "streams", "categories", "list"):
            nested = data.get(key)
            if isinstance(nested, list):
                has_nested = True
                items.extend(self._flatten_json_items(nested, cat))

        if not has_nested or any(
            k in data for k in ("url", "link", "stream_url", "mpd", "mpd_url", "hls", "acestream")
        ):
            merged = dict(data)
            if cat and not merged.get("category"):
                merged["category"] = cat
            items.append(merged)
        return items

    def _build_stream_source(self, item: dict[str, Any], title: str) -> Optional[StreamSource]:
        raw_url = str(
            item.get("mpd_url")
            or item.get("mpd")
            or item.get("url")
            or item.get("link")
            or item.get("stream_url")
            or item.get("hls")
            or item.get("acestream")
            or ""
        ).strip()
        if not raw_url:
            return None

        clean_url, parsed_headers, embedded_ck = _split_kodi_pipe_url(raw_url)
        kodiprop_match = _KODIPROP_KEY_RE.search(str(item.get("kodiprop") or item.get("info") or ""))
        if not embedded_ck and kodiprop_match:
            embedded_ck = kodiprop_match.group(1).strip()

        item_headers: dict[str, str] = {}
        if isinstance(item.get("headers"), dict):
            item_headers.update({str(k): str(v) for k, v in item["headers"].items()})
        if item.get("user_agent"):
            item_headers["User-Agent"] = str(item["user_agent"])
        if item.get("referer"):
            item_headers["Referer"] = str(item["referer"])
        if item.get("origin"):
            item_headers["Origin"] = str(item["origin"])
        item_headers.update(parsed_headers)

        clearkey = _normalize_clearkey(
            item.get("clearkey") or item.get("drm_key") or item.get("license_key") or item.get("key") or embedded_ck,
            kid=str(item.get("kid") or ""),
            key=str(item.get("key_value") or item.get("k") or ""),
        )

        # Detect AceStream
        ace_match = _ACESTREAM_RE.search(clean_url)
        if ace_match or str(item.get("type") or "").lower() == "acestream":
            ace_url = f"acestream://{ace_match.group(1).lower()}" if ace_match else clean_url
            return StreamSource(
                provider=self.name,
                title=f"[AceStream] {title}",
                url=ace_url,
                quality=str(item.get("quality") or "HD"),
                language=_detect_language(item, title),
                priority=7,
                requires_proxy=False,
                proxy_host="",
                is_acestream=True,
                channel_name=normalize_channel_name(title),
                raw_url=ace_url,
            )

        # Detect MPEG-DASH ClearKey
        is_mpd = bool(
            clearkey
            or ".mpd" in clean_url.lower()
            or item.get("mpd")
            or item.get("mpd_url")
            or str(item.get("format") or "").lower() == "dash"
        )
        lang = _detect_language(item, title)
        quality = str(item.get("quality") or ("1080p50" if is_mpd else "FHD"))

        if is_mpd:
            return StreamSource(
                provider=self.name,
                title=f"[MPD] {title}",
                url=clean_url,
                quality=quality,
                language=lang,
                priority=2,
                requires_proxy=True,
                proxy_host="mpd",
                clearkey=clearkey,
                headers=item_headers,
                channel_name=normalize_channel_name(title),
                raw_url=clean_url,
            )

        # DTT free-to-air / direct HLS / Socceron stream
        needs_proxy = bool(item.get("requires_proxy")) or bool(item_headers)
        return StreamSource(
            provider=self.name,
            title=title,
            url=clean_url,
            quality=quality,
            language=lang,
            priority=2,
            requires_proxy=needs_proxy,
            proxy_host="hls" if needs_proxy else "",
            headers=item_headers,
            channel_name=normalize_channel_name(title),
            raw_url=clean_url,
        )

    def _is_event_item(self, item: dict[str, Any], title: str) -> bool:
        cat = str(item.get("category") or item.get("type") or item.get("kind") or "").lower()
        if any(tok in cat for tok in ("socceron", "event", "match", "live_event")):
            return True
        if item.get("home") or item.get("away") or item.get("start_ms") or item.get("start_time"):
            return True
        home, away = _parse_teams_from_title(title)
        return bool(home and away)

    def parse_payload(self, raw_payload: str | bytes | dict[str, Any] | list[Any]) -> ProviderHarvestResult:
        """Parse a MandraKodi JSON, XML, or XOR-encrypted payload into channels & events."""
        decoded = self._decode_raw_payload(raw_payload)
        if isinstance(decoded, str):
            raw_items = self._xml_to_items(decoded) if "<" in decoded else []
        else:
            raw_items = self._flatten_json_items(decoded)

        linear_channels: dict[str, list[StreamSource]] = {}
        event_buckets: dict[tuple[str, int], ProviderEvent] = {}

        for item in raw_items:
            title = str(item.get("title") or item.get("name") or item.get("channel") or "").strip()
            if not title:
                continue
            source = self._build_stream_source(item, title)
            if source is None:
                continue

            if self._is_event_item(item, title):
                home = str(item.get("home") or "").strip()
                away = str(item.get("away") or "").strip()
                if not (home and away):
                    parsed_home, parsed_away = _parse_teams_from_title(title)
                    home = home or parsed_home
                    away = away or parsed_away

                competition = str(item.get("competition") or item.get("league") or "").strip()
                start_ms = _parse_start_ms(
                    item.get("start_ms") or item.get("start_time") or item.get("date") or item.get("time")
                )
                is_women_t, is_youth_t = detect_demographics_from_stream_title(title)
                is_women_c, is_youth_c = detect_demographics(competition)
                key = (title.lower(), start_ms)

                if key not in event_buckets:
                    event_buckets[key] = ProviderEvent(
                        provider=self.name,
                        title=title,
                        home=home,
                        away=away,
                        competition=competition,
                        sport_hint=_infer_sport_hint(
                            f"{competition} {title}", str(item.get("sport") or "")
                        ),
                        start_ms=start_ms,
                        is_women=is_women_t or is_women_c,
                        is_youth=is_youth_t or is_youth_c,
                        streams=[],
                    )
                event_buckets[key].streams.append(source)
            else:
                ch_key = normalize_channel_name(str(item.get("channel") or title))
                if not ch_key:
                    continue
                source.channel_name = ch_key
                linear_channels.setdefault(ch_key, []).append(source)

        for sources in linear_channels.values():
            sources.sort(key=lambda s: s.priority)
        events = list(event_buckets.values())
        for ev in events:
            ev.streams.sort(key=lambda s: s.priority)

        return ProviderHarvestResult(
            provider=self.name,
            events=events,
            linear_channels=linear_channels,
            raw_count=len(raw_items),
        )

    async def harvest(self) -> ProviderHarvestResult:
        """Fetch and parse MandraKodi streams, skipping gracefully when `MANDRAKODI_API_URL` is unset."""
        api_url = (self.settings.MANDRAKODI_API_URL or "").strip()
        if not api_url:
            logger.warning(
                "MandraKodiService: MANDRAKODI_API_URL is empty; skipping remote fetch."
            )
            return ProviderHarvestResult(provider=self.name)

        client = self._get_client()
        resp = await client.get(api_url, headers=self._build_request_headers(), timeout=15.0)
        resp.raise_for_status()
        return self.parse_payload(resp.content)
