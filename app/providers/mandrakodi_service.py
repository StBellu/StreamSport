"""MandraKodi stream provider supporting XOR payloads, MPD ClearKey, DTT HLS, Socceron, and AceStream."""

from __future__ import annotations

import asyncio
import base64
import binascii
import html
import json
import logging
import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Any, Optional
from urllib.parse import parse_qs, unquote
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import httpx

from app.config import Settings
from app.core.network import create_standard_client, resolve_proxy_url
from app.models.domain import ProviderEvent, StreamSource
from app.providers.base import BaseStreamProvider, ProviderHarvestResult, compact_preview
from app.providers.tvvoo_service import normalize_channel_name
from app.taxonomy.channels import SKY_CHID_TO_CANONICAL
from app.taxonomy.demographics import (
    detect_demographics,
    detect_demographics_from_stream_title,
)

logger = logging.getLogger(__name__)

MANDRAKODI_NUM_TESTS: tuple[str, ...] = (
    "A1A165",
    "A1A165A",
    "A1A103",
    "A1A105",
    "A1A156",
    "A1A134C",
    "A1A134F",
    "A1A215",
)

_SKY_NEGATIVE_TTL_SECONDS = 300.0
_SKY_DEFAULT_TTL_SECONDS = 6 * 3600.0
_JWT_MAX_AGE_SECONDS = 6 * 3600.0
_SUPERTENNIS_CACHE_TTL_SECONDS = 60.0
_SUPERTENNIS_MIN_SEGMENT_BYTES = 250_000

_SKY_FREESHOT_MAP: dict[str, str] = {
    "skysport24": "SkySport24IT",
    "skysportuno": "SkySportUnoIT",
    "skysportarena": "SkySportArenaIT",
    "skysportmax": "SkySportMaxIT",
    "skysporttennis": "SkySportTennisIT",
    "skysportf1": "SkySportF1IT",
    "skysportmotogp": "SkySportMotoGPIT",
    "skysportcalcio": "SkySportCalcioIT",
    "skysportgolf": "SkySportGolfIT",
    "skysportnba": "SkySportNBAIT",
}

_SKY_CHANNEL_NAMES: dict[str, str] = {
    "skysport24": "sky sport 24",
    "skysportuno": "sky sport uno",
    "skysportarena": "sky sport arena",
    "skysportmax": "sky sport max",
    "skysporttennis": "sky sport tennis",
    "skysportf1": "sky sport f1",
    "skysportmotogp": "sky sport motogp",
    "skysportcalcio": "sky sport calcio",
    "skysportgolf": "sky sport golf",
    "skysportnba": "sky sport nba",
}

_KODI_TAG_RE = re.compile(r"\[/?(?:COLOR|B|I|UPPERCASE|LOWERCASE)[^\]]*\]", re.IGNORECASE)
_KODI_CR_RE = re.compile(r"\[CR\]", re.IGNORECASE)
_SECTION_BANNER_RE = re.compile(r"^=+\s*(.*?)\s*=+$")
_DATE_PREFIX_RE = re.compile(
    r"^\d{1,2}(?:st|nd|rd|th)?\s+[A-Za-z]{3,9}\s+\d{4}\s*",
    re.IGNORECASE,
)
_TECH_SUFFIX_RE = re.compile(
    r"\s*(?:\([^)]*(?:MPD|M3U8|ACE|WEB|HD|FHD|4K|1080P|720P|ITA|ENG|POL|FR|ES|DE|UK|MULTI)[^)]*\)|"
    r"\[[^\]]*(?:MPD|M3U8|ACE|WEB|HD|FHD|4K|1080P|720P|FS|MH)[^\]]*\])\s*$",
    re.IGNORECASE,
)


def _strip_kodi_tags(raw: str) -> str:
    """Strip Kodi BBCode markup (`[COLOR ...]`, `[CR]`, etc.) and unescape HTML."""
    text = html.unescape(raw or "")
    text = _KODI_CR_RE.sub(" ", text)
    text = _KODI_TAG_RE.sub("", text)
    return re.sub(r"\s+", " ", text).strip()


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


def extract_jwt_iat(url: str) -> Optional[int]:
    """Extract the `iat` (issued-at) epoch claim from a JWT embedded in a DAZN MPD URL."""
    if not url:
        return None
    jwt_match = re.search(r"eyJ[a-zA-Z0-9_-]+\.eyJ[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+", url)
    if not jwt_match:
        return None
    try:
        payload_b64 = jwt_match.group(0).split(".")[1]
        payload_b64 += "=" * (-len(payload_b64) % 4)
        payload = json.loads(base64.b64decode(payload_b64).decode("utf-8"))
        raw_iat = payload.get("iat")
        return int(raw_iat) if raw_iat is not None else None
    except Exception:
        return None


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
    clean = _strip_kodi_tags(title)
    clean = _DATE_PREFIX_RE.sub("", clean).strip()
    clean = _TIME_PREFIX_RE.sub("", clean).strip()
    # Strip trailing channel/format tags like `(MPD)`, `(ITA - M3U8)`, `[1080]`
    for _ in range(3):
        stripped = _TECH_SUFFIX_RE.sub("", clean).strip()
        if stripped == clean:
            break
        clean = stripped
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
        self._sky_cache: dict[str, tuple[Optional[StreamSource], float]] = {}
        self._sky_locks: dict[str, asyncio.Lock] = {}
        self._st_ott_cache: list[StreamSource] = []
        self._st_ott_cache_time: float = 0.0
        self._st_ott_lock = asyncio.Lock()

    @property
    def enabled(self) -> bool:
        return bool(self.settings.ENABLE_MANDRAKODI)

    def _get_client(self) -> httpx.AsyncClient:
        if self._external_client is not None:
            return self._external_client
        if self._owned_client is None or self._owned_client.is_closed:
            self._owned_client = create_standard_client(
                timeout=15.0,
                proxy=resolve_proxy_url(self.settings, "mandrakodi"),
            )
        return self._owned_client

    async def aclose(self) -> None:
        if self._owned_client is not None and not self._owned_client.is_closed:
            await self._owned_client.aclose()
            self._owned_client = None

    def _build_request_headers(self) -> dict[str, str]:
        password = self.settings.MANDRAKODI_PASSWORD or ""
        device_id = self.settings.MANDRAKODI_DEVICE_ID or ""
        return {
            "User-Agent": f"MandraKodi2@@1.2.84@@{password}@@{device_id}",
            "Accept": "application/json, text/plain, */*",
        }

    def _decode_raw_payload(self, raw: str | bytes | dict[str, Any] | list[Any]) -> Any:
        if isinstance(raw, (dict, list)):
            return raw

        text = raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else raw
        stripped = text.strip()
        if not stripped:
            return []

        if stripped.startswith(("{", "[")):
            try:
                parsed = json.loads(stripped, strict=False)
                if isinstance(parsed, dict) and isinstance(parsed.get("payload"), str):
                    for key in (self.settings.MANDRAKODI_XOR_KEY, self.settings.MANDRAKODI_PASSWORD):
                        if key:
                            decrypted = xor_decrypt(parsed["payload"], key)
                            dec_s = decrypted.strip()
                            if dec_s.startswith(("{", "[", "<")):
                                return self._decode_raw_payload(decrypted)
                return parsed
            except json.JSONDecodeError:
                pass

        if stripped.startswith("<"):
            return stripped

        tried: set[str] = set()
        for key in (self.settings.MANDRAKODI_XOR_KEY, self.settings.MANDRAKODI_PASSWORD):
            if not key or key in tried:
                continue
            tried.add(key)
            decrypted = xor_decrypt(raw, key)
            dec_stripped = decrypted.strip()
            if dec_stripped.startswith(("{", "[")):
                try:
                    return json.loads(dec_stripped, strict=False)
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

    def _flatten_json_items(
        self,
        data: Any,
        inherited_category: str = "",
        inherited_competition: str = "",
    ) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        if isinstance(data, list):
            current_comp = inherited_competition
            for entry in data:
                if isinstance(entry, dict):
                    raw_t = _strip_kodi_tags(str(entry.get("title") or entry.get("name") or ""))
                    sec_m = _SECTION_BANNER_RE.match(raw_t)
                    if sec_m and (
                        str(entry.get("link") or "").strip().lower() == "ignore"
                        or str(entry.get("myresolve") or "").strip().lower() == "ignore"
                        or not any(k in entry for k in ("url", "myresolve", "mpd_url", "hls"))
                    ):
                        sec_name = sec_m.group(1).strip()
                        if sec_name and sec_name.upper() not in ("LAST MINUTE", "LIVE TV"):
                            current_comp = sec_name
                        continue
                items.extend(self._flatten_json_items(entry, inherited_category, current_comp))
            return items

        if not isinstance(data, dict):
            return items

        cat = str(data.get("category") or data.get("group") or data.get("type") or inherited_category)
        comp = str(data.get("competition") or data.get("league") or inherited_competition)
        has_nested = False
        for key in ("items", "channels", "events", "streams", "categories", "list"):
            nested = data.get(key)
            if isinstance(nested, list):
                has_nested = True
                items.extend(self._flatten_json_items(nested, cat, comp))

        if not has_nested or any(
            k in data for k in ("url", "link", "stream_url", "mpd", "mpd_url", "hls", "acestream", "myresolve")
        ):
            merged = dict(data)
            if cat and not merged.get("category"):
                merged["category"] = cat
            if comp and not merged.get("competition"):
                merged["competition"] = comp
            items.append(merged)
        return items

    @staticmethod
    def _is_skipped_item(item: dict[str, Any], title: str) -> bool:
        if item.get("enabled") in (0, "0", False):
            return True
        if str(item.get("link") or "").strip().lower() == "ignore":
            return True
        raw_mr = str(item.get("myresolve") or "").strip().lower()
        if raw_mr == "ignore" or raw_mr in ("daddycode@@0", "daddycode@@00"):
            return True
        upper_title = title.upper()
        if (
            "HAVE PROBLEM" in upper_title
            or "CHANNEL NOT LISTED" in upper_title
            or "=== " in title
            or title.strip().startswith("===")
        ):
            return True
        return False

    def _build_myresolve_source(
        self,
        myresolve_val: str,
        item: dict[str, Any],
        title: str,
        item_headers: dict[str, str],
        lang: str,
    ) -> Optional[StreamSource]:
        prefix, _, payload_val = myresolve_val.partition("@@")
        prefix_low = prefix.strip().lower()
        val = payload_val.strip()
        ch_key = normalize_channel_name(title)

        if prefix_low == "mediahosting":
            if not val.lower().startswith("http"):
                return None
            return StreamSource(
                provider=self.name,
                title=title,
                url=val,
                quality=str(item.get("quality") or "FHD"),
                language=lang,
                priority=2,
                requires_proxy=True,
                proxy_host="",
                headers=item_headers,
                channel_name=ch_key,
                raw_url=val,
            )

        if prefix_low == "amstaff":
            decoded_val = val
            if not decoded_val.lower().startswith("http"):
                try:
                    padded = decoded_val + "=" * (-len(decoded_val) % 4)
                    decoded_val = base64.b64decode(padded).decode("utf-8", errors="replace").strip()
                except Exception:
                    return None
            parts = decoded_val.split("|")
            target_url = parts[0].strip().rstrip("?")
            if not target_url:
                return None
            clearkey = ""
            ck_part = ""
            headers = dict(item_headers)
            if len(parts) >= 2:
                ck_part = parts[1].strip().strip("'\"")
                if ck_part and ":" not in ck_part and len(ck_part) > 20:
                    try:
                        padded_ck = ck_part + "=" * (-len(ck_part) % 4)
                        dec_ck = base64.b64decode(padded_ck).decode("utf-8", errors="ignore").strip()
                        if dec_ck.startswith("{") or ":" in dec_ck:
                            ck_part = dec_ck
                    except Exception:
                        pass
                if ck_part.lower() not in ("0000", "0", "none", "clear", ""):
                    clearkey = _normalize_clearkey(ck_part)
                    if ":" not in clearkey:
                        clearkey = ""
            if len(parts) >= 3 and parts[2].strip():
                headers["User-Agent"] = parts[2].strip()

            is_clear_stream = (
                ck_part.lower() in ("0000", "0", "none", "clear", "")
                and not clearkey
            ) or "-clr.isml" in target_url.lower()
            if is_clear_stream and (
                "-clr.isml" in target_url.lower() or "mediaset.net" in target_url.lower()
            ):
                target_url = target_url.replace("/manifest.mpd", "/manifest.m3u8").replace(".mpd", ".m3u8")
                clearkey = ""

            is_mpd = bool(clearkey or ".mpd" in target_url.lower())
            return StreamSource(
                provider=self.name,
                title=f"[MPD] {title}" if is_mpd else title,
                url=target_url,
                quality=str(item.get("quality") or ("1080p50" if is_mpd else "FHD")),
                language=lang,
                priority=1 if (is_mpd or lang == "ITA") else 2,
                requires_proxy=True if (is_mpd or headers) else False,
                proxy_host="mpd" if is_mpd else ("hls" if headers else ""),
                clearkey=clearkey,
                headers=headers,
                channel_name=ch_key,
                raw_url=target_url,
            )

        if prefix_low == "freeshot":
            if not val:
                return None
            return StreamSource(
                provider=self.name,
                title=title,
                url=val,
                quality=str(item.get("quality") or "FHD"),
                language=lang,
                priority=2,
                requires_proxy=True,
                proxy_host="freeshot",
                headers=item_headers,
                channel_name=ch_key,
                raw_url=val,
            )

        if prefix_low == "sky":
            if not val:
                return None
            mapped = _SKY_FREESHOT_MAP.get(val.lower(), val)
            sky_ch = SKY_CHID_TO_CANONICAL.get(val.lower()) or _SKY_CHANNEL_NAMES.get(val.lower(), ch_key)
            return StreamSource(
                provider=self.name,
                title=title,
                url=mapped,
                quality=str(item.get("quality") or "FHD"),
                language="ITA",
                priority=1,
                requires_proxy=True,
                proxy_host="freeshot",
                headers=item_headers,
                channel_name=sky_ch,
                raw_url=mapped,
            )

        if prefix_low in ("daddy", "daddycode"):
            if not val or (prefix_low == "daddycode" and val in ("0", "00")):
                return None
            target_url = f"https://thedaddy.li/watch.php?id={val}" if prefix_low == "daddycode" else val
            return StreamSource(
                provider=self.name,
                title=title,
                url=target_url,
                quality=str(item.get("quality") or "HD"),
                language=lang,
                priority=3,
                requires_proxy=True,
                proxy_host="dlstreams",
                headers=item_headers,
                channel_name=ch_key,
                raw_url=target_url,
            )

        if prefix_low == "vavooplay":
            if not val:
                return None
            return StreamSource(
                provider=self.name,
                title=title,
                url=val,
                quality=str(item.get("quality") or "FHD"),
                language=lang,
                priority=2,
                requires_proxy=True,
                proxy_host="vavoo",
                headers=item_headers,
                channel_name=ch_key,
                raw_url=val,
            )

        if prefix_low == "spon":
            if not val:
                return None
            return StreamSource(
                provider=self.name,
                title=title,
                url=val,
                quality=str(item.get("quality") or "HD"),
                language=lang,
                priority=3,
                requires_proxy=True,
                proxy_host="",
                headers=item_headers,
                channel_name=ch_key,
                raw_url=val,
            )

        if prefix_low == "ffmpeg":
            clean_url, parsed_headers, embedded_ck = _split_kodi_pipe_url(val)
            if not clean_url:
                return None
            headers = dict(item_headers)
            headers.update(parsed_headers)
            is_mpd = bool(embedded_ck or ".mpd" in clean_url.lower())
            needs_proxy = is_mpd or bool(headers) or bool(item.get("requires_proxy"))
            return StreamSource(
                provider=self.name,
                title=f"[MPD] {title}" if is_mpd else title,
                url=clean_url,
                quality=str(item.get("quality") or "FHD"),
                language=lang,
                priority=1 if (is_mpd or lang == "ITA") else 2,
                requires_proxy=needs_proxy,
                proxy_host="mpd" if is_mpd else "",
                clearkey=embedded_ck,
                headers=headers,
                channel_name=ch_key,
                raw_url=clean_url,
            )

        return None

    def _build_stream_source(self, item: dict[str, Any], title: str) -> Optional[StreamSource]:
        item_headers: dict[str, str] = {}
        if isinstance(item.get("headers"), dict):
            item_headers.update({str(k): str(v) for k, v in item["headers"].items()})
        if item.get("user_agent"):
            item_headers["User-Agent"] = str(item["user_agent"])
        if item.get("referer"):
            item_headers["Referer"] = str(item["referer"])
        if item.get("origin"):
            item_headers["Origin"] = str(item["origin"])

        lang = _detect_language(item, title)
        myresolve_raw = str(item.get("myresolve") or "").strip()
        if myresolve_raw and "@@" in myresolve_raw:
            resolved_src = self._build_myresolve_source(myresolve_raw, item, title, item_headers, lang)
            if resolved_src is not None:
                return resolved_src
            if myresolve_raw.lower().startswith("mediahosting@@"):
                return None

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
            proxy_host="",
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
            raw_title = str(item.get("title") or item.get("name") or item.get("channel") or "").strip()
            title = _strip_kodi_tags(raw_title)
            if not title or self._is_skipped_item(item, title):
                continue
            source = self._build_stream_source(item, title)
            if source is None:
                continue

            if self._is_event_item(item, title):
                home = _strip_kodi_tags(str(item.get("home") or ""))
                away = _strip_kodi_tags(str(item.get("away") or ""))
                if not (home and away):
                    parsed_home, parsed_away = _parse_teams_from_title(title)
                    home = home or parsed_home
                    away = away or parsed_away

                competition = _strip_kodi_tags(str(item.get("competition") or item.get("league") or ""))
                start_ms = _parse_start_ms(
                    item.get("start_ms") or item.get("start_time") or item.get("date") or item.get("time")
                )
                if start_ms == 0:
                    tm_m = _TIME_PREFIX_RE.match(_DATE_PREFIX_RE.sub("", title).strip())
                    if tm_m:
                        try:
                            tz_rome = ZoneInfo("Europe/Rome")
                        except ZoneInfoNotFoundError:
                            tz_rome = timezone.utc
                        now_rome = datetime.now(tz_rome)
                        hh, mm = int(tm_m.group(1)), int(tm_m.group(2))
                        if 0 <= hh <= 23 and 0 <= mm <= 59:
                            dt_ev = now_rome.replace(hour=hh, minute=mm, second=0, microsecond=0)
                            start_ms = int(dt_ev.astimezone(timezone.utc).timestamp() * 1000)

                now_ts = time.time()
                jwt_iat = extract_jwt_iat(source.url)
                if jwt_iat is not None:
                    if (now_ts - jwt_iat) > _JWT_MAX_AGE_SECONDS:
                        continue
                    if start_ms > 0 and abs((start_ms / 1000.0) - now_ts) > _JWT_MAX_AGE_SECONDS:
                        continue

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
                raw_ch = _strip_kodi_tags(str(item.get("channel") or ""))
                ch_key = source.channel_name or normalize_channel_name(raw_ch or title)
                if not ch_key:
                    continue
                source.channel_name = ch_key
                linear_channels.setdefault(ch_key, []).append(source)

        for sources in linear_channels.values():
            sources.sort(key=lambda s: (0 if s.language == "ITA" else 1, 0 if s.clearkey else 1, s.priority))
        events = list(event_buckets.values())
        for ev in events:
            ev.streams.sort(key=lambda s: (0 if s.language == "ITA" else 1, 0 if s.clearkey else 1, s.priority))

        return ProviderHarvestResult(
            provider=self.name,
            events=events,
            linear_channels=linear_channels,
            raw_count=len(raw_items),
        )

    async def resolve_sky_channel(self, channel_id: str) -> Optional[StreamSource]:
        """Resolve a Sky Italia channel JIT via `numTest=A1A159&id=<chid>` with negative caching."""
        sky_chid = channel_id.strip()
        if sky_chid.lower().startswith("sky@@"):
            sky_chid = sky_chid.split("@@", 1)[1].strip()
        if not sky_chid:
            return None

        now = time.time()
        cached = self._sky_cache.get(sky_chid)
        if cached is not None and now < cached[1]:
            return cached[0]

        lock = self._sky_locks.setdefault(sky_chid, asyncio.Lock())
        async with lock:
            now = time.time()
            cached = self._sky_cache.get(sky_chid)
            if cached is not None and now < cached[1]:
                return cached[0]

            api_url = (self.settings.MANDRAKODI_API_URL or "").strip()
            if not api_url:
                self._sky_cache[sky_chid] = (None, now + _SKY_NEGATIVE_TTL_SECONDS)
                return None

            client = self._get_client()
            try:
                resp = await client.get(
                    api_url,
                    params={"numTest": "A1A159", "id": sky_chid},
                    headers=self._build_request_headers(),
                    timeout=12.0,
                )
                resp.raise_for_status()
                raw_text = resp.text
            except Exception as exc:
                logger.warning("MandraKodi A1A159 resolve failed for %s: %s", sky_chid, exc)
                self._sky_cache[sky_chid] = (None, now + _SKY_NEGATIVE_TTL_SECONDS)
                return None

            upper_raw = raw_text.upper()
            if "HAVE PROBLEM" in upper_raw or raw_text.strip().lower() == "ignore":
                self._sky_cache[sky_chid] = (None, now + _SKY_NEGATIVE_TTL_SECONDS)
                return None

            parsed: Any = None
            try:
                outer = json.loads(raw_text, strict=False)
                if isinstance(outer, dict) and outer.get("data"):
                    decrypted = self._decode_raw_payload(outer["data"])
                    parsed = decrypted
                else:
                    parsed = self._decode_raw_payload(raw_text)
            except json.JSONDecodeError:
                parsed = self._decode_raw_payload(raw_text)

            if isinstance(parsed, str):
                upper_dec = parsed.upper()
                if "HAVE PROBLEM" in upper_dec or parsed.strip().lower() == "ignore":
                    self._sky_cache[sky_chid] = (None, now + _SKY_NEGATIVE_TTL_SECONDS)
                    return None
                try:
                    parsed = json.loads(parsed, strict=False)
                except json.JSONDecodeError:
                    parsed = None

            if not isinstance(parsed, dict):
                self._sky_cache[sky_chid] = (None, now + _SKY_NEGATIVE_TTL_SECONDS)
                return None

            manifest = str(parsed.get("manifest") or parsed.get("mpd_url") or parsed.get("url") or "").strip()
            if not manifest or manifest.lower() == "ignore" or "HAVE PROBLEM" in manifest.upper():
                self._sky_cache[sky_chid] = (None, now + _SKY_NEGATIVE_TTL_SECONDS)
                return None

            clearkey = _normalize_clearkey(
                parsed.get("clearkey"),
                kid=str(parsed.get("kid") or ""),
                key=str(parsed.get("key") or ""),
            )
            exp_ts = now + _SKY_DEFAULT_TTL_SECONDS
            fine_str = str(parsed.get("fine") or "").strip()
            if fine_str:
                try:
                    try:
                        tz_rome: Any = ZoneInfo("Europe/Rome")
                    except ZoneInfoNotFoundError:
                        tz_rome = timezone.utc
                    dt = datetime.strptime(fine_str, "%d/%m/%Y %H:%M:%S").replace(tzinfo=tz_rome)
                    exp_ts = max(now + 60.0, dt.timestamp())
                except Exception:
                    pass

            canonical = SKY_CHID_TO_CANONICAL.get(sky_chid.lower()) or normalize_channel_name(sky_chid)
            src = StreamSource(
                provider=self.name,
                title=f"[MPD] {canonical.upper()}",
                url=manifest,
                quality="1080p50",
                language="ITA",
                priority=1,
                requires_proxy=True,
                proxy_host="mpd",
                clearkey=clearkey,
                channel_name=canonical,
                raw_url=manifest,
            )
            self._sky_cache[sky_chid] = (src, exp_ts)
            return src

    async def get_active_supertennis_ott_streams(self) -> list[StreamSource]:
        """Probe SuperTennis interactive OTT feeds (`supertennis 1..4`) using lightweight Range requests."""
        now = time.time()
        if self._st_ott_cache and (now - self._st_ott_cache_time) < _SUPERTENNIS_CACHE_TTL_SECONDS:
            return list(self._st_ott_cache)

        async with self._st_ott_lock:
            now = time.time()
            if self._st_ott_cache and (now - self._st_ott_cache_time) < _SUPERTENNIS_CACHE_TTL_SECONDS:
                return list(self._st_ott_cache)

            client = self._get_client()
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

            async def _probe_channel(ch_num: int) -> Optional[StreamSource]:
                manifest_url = (
                    f"https://421b2081187a40368f24e4fb496bfc26.msvdn.net/channel{ch_num}/manifest.mpd"
                )
                try:
                    resp = await client.get(
                        manifest_url,
                        headers=headers,
                        follow_redirects=True,
                        timeout=2.5,
                    )
                    if resp.status_code != 200:
                        return None
                    text = resp.text
                    rep_match = re.search(
                        r'<Representation width="[0-9]+" height="[0-9]+"[^>]*id="([^"]+)"', text
                    )
                    time_match = re.search(r'<S t="([0-9]+)"', text)
                    media_match = re.search(r'media="([^"]+)"', text)
                    if not (rep_match and time_match and media_match):
                        return None

                    seg_rel = (
                        media_match.group(1)
                        .replace("$RepresentationID$", rep_match.group(1))
                        .replace("$Time$", time_match.group(1))
                    )
                    seg_url = str(resp.url).rsplit("/", 1)[0] + "/" + seg_rel
                    seg_resp = await client.get(
                        seg_url,
                        headers={**headers, "Range": "bytes=0-250000"},
                        timeout=2.5,
                    )
                    if seg_resp.status_code not in (200, 206):
                        return None

                    content_range = seg_resp.headers.get("content-range", "")
                    total_size = (
                        int(content_range.split("/")[-1])
                        if "/" in content_range
                        else len(seg_resp.content)
                    )
                    if total_size <= _SUPERTENNIS_MIN_SEGMENT_BYTES:
                        return None

                    ch_key = f"supertennis {ch_num}"
                    return StreamSource(
                        provider=self.name,
                        title=f"[MPD] SuperTennis OTT {ch_num}",
                        url=manifest_url,
                        quality="720p50",
                        language="ITA",
                        priority=1,
                        requires_proxy=True,
                        proxy_host="mpd",
                        channel_name=ch_key,
                        raw_url=manifest_url,
                    )
                except Exception as exc:
                    logger.debug("SuperTennis OTT %d probe failed: %s", ch_num, exc)
                    return None

            probed = await asyncio.gather(*(_probe_channel(i) for i in range(1, 5)))
            active = [src for src in probed if src is not None]
            self._st_ott_cache = active
            self._st_ott_cache_time = time.time()
            return list(active)

    async def _fetch_num_test(
        self, client: httpx.AsyncClient, api_url: str, num_test: str
    ) -> tuple[ProviderHarvestResult, int, int, str]:
        resp = await client.get(
            api_url,
            params={"numTest": num_test},
            headers=self._build_request_headers(),
            timeout=15.0,
        )
        resp.raise_for_status()
        parsed = self.parse_payload(resp.content)
        return parsed, resp.status_code, len(resp.content), compact_preview(resp.content, max_len=90)

    async def harvest(self) -> ProviderHarvestResult:
        """Fetch and parse MandraKodi streams across all numTest packs."""
        api_url = (self.settings.MANDRAKODI_API_URL or "").strip()
        if not api_url:
            logger.warning(
                "MandraKodiService: MANDRAKODI_API_URL is empty; skipping remote fetch."
            )
            return ProviderHarvestResult(
                provider=self.name,
                diagnostic="MANDRAKODI_API_URL empty",
            )

        client = self._get_client()
        results = await asyncio.gather(
            *(self._fetch_num_test(client, api_url, nt) for nt in MANDRAKODI_NUM_TESTS),
            return_exceptions=True,
        )

        merged_linear: dict[str, list[StreamSource]] = {}
        seen_linear: set[tuple[str, str, str]] = set()
        merged_events: dict[tuple[str, int], ProviderEvent] = {}
        seen_ev_streams: dict[tuple[str, int], set[tuple[str, str]]] = {}
        total_raw = 0
        errors: list[str] = []
        ok_packs = 0
        empty_samples: list[str] = []

        for nt, outcome in zip(MANDRAKODI_NUM_TESTS, results):
            if isinstance(outcome, Exception):
                logger.warning("MandraKodiService numTest=%s failed: %s", nt, outcome)
                errors.append(f"{nt}: {outcome}")
                continue
            res, status_code, nbytes, preview = outcome
            total_raw += res.raw_count
            if res.linear_channels or res.events:
                ok_packs += 1
            else:
                empty_samples.append(f"{nt}=HTTP {status_code} {nbytes}B '{preview}'")

            for ch_key, sources in res.linear_channels.items():
                bucket = merged_linear.setdefault(ch_key, [])
                for src in sources:
                    dedup = (ch_key, src.url, src.clearkey)
                    if dedup not in seen_linear:
                        seen_linear.add(dedup)
                        bucket.append(src)

            for ev in res.events:
                ev_key = (ev.title.lower(), ev.start_ms)
                if ev_key not in merged_events:
                    merged_events[ev_key] = ProviderEvent(
                        provider=ev.provider,
                        title=ev.title,
                        home=ev.home,
                        away=ev.away,
                        competition=ev.competition,
                        sport_hint=ev.sport_hint,
                        start_ms=ev.start_ms,
                        is_women=ev.is_women,
                        is_youth=ev.is_youth,
                        streams=[],
                    )
                    seen_ev_streams[ev_key] = set()
                for src in ev.streams:
                    s_dedup = (src.url, src.clearkey)
                    if s_dedup not in seen_ev_streams[ev_key]:
                        seen_ev_streams[ev_key].add(s_dedup)
                        merged_events[ev_key].streams.append(src)

        if not merged_linear and not merged_events and len(errors) == len(MANDRAKODI_NUM_TESTS):
            return ProviderHarvestResult(
                provider=self.name,
                error="; ".join(errors[:3]),
                diagnostic=f"all {len(MANDRAKODI_NUM_TESTS)} packs failed",
            )

        st_ott_streams = await self.get_active_supertennis_ott_streams()
        for src in st_ott_streams:
            ch_key = src.channel_name
            bucket = merged_linear.setdefault(ch_key, [])
            dedup = (ch_key, src.url, src.clearkey)
            if dedup not in seen_linear:
                seen_linear.add(dedup)
                bucket.append(src)

        for sources in merged_linear.values():
            sources.sort(key=lambda s: (0 if s.language == "ITA" else 1, 0 if s.clearkey else 1, s.priority))
        events_list = list(merged_events.values())
        for ev in events_list:
            ev.streams.sort(key=lambda s: (0 if s.language == "ITA" else 1, 0 if s.clearkey else 1, s.priority))

        if merged_linear or events_list:
            diagnostic = f"packs={ok_packs}/{len(MANDRAKODI_NUM_TESTS)} ok, st_ott={len(st_ott_streams)}"
            if errors:
                diagnostic += f", failed={len(errors)}"
        else:
            sample = "; ".join(empty_samples[:2])
            diagnostic = f"packs=0/{len(MANDRAKODI_NUM_TESTS)}, failed={len(errors)}"
            if sample:
                diagnostic += f", sample={sample}"
            logger.warning("MandraKodi harvest returned 0 channels/events (%s)", diagnostic)

        return ProviderHarvestResult(
            provider=self.name,
            events=events_list,
            linear_channels=merged_linear,
            raw_count=total_raw,
            diagnostic=diagnostic,
        )
