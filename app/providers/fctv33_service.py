"""FCTV33 Protobuf catalog harvester & dynamic domain/sfver handshake service."""

from __future__ import annotations

import asyncio
import json
import logging
import re
from typing import Any, Optional, Sequence

import httpx

from app.config import Settings
from app.core.network import create_doh_client
from app.models.domain import ProviderEvent, StreamSource
from app.providers.base import BaseStreamProvider, ProviderHarvestResult
from app.taxonomy import detect_demographics_from_stream_title

logger = logging.getLogger(__name__)

DOMAINS_CONFIG_URL = (
    "https://raw.githubusercontent.com/realbestia1/domains/refs/heads/main/domains.json"
)
DEFAULT_DATA_API_BASE = "https://apis-data10.tcpwe138stya.ru"
DEFAULT_PLAYER_REFERER = "https://nadia01eo.tn76degree12ec3out.cfd/"
DEFAULT_STREAM_DIGIT = "seth"
DEFAULT_SITE_TYPE = 2001

SPORT_TYPE_TO_HINT: dict[int, str] = {
    1: "football",
    2: "basketball",
    3: "tennis",
    4: "baseball",
    6: "cricket",
    7: "motorsport",
    8: "rugby",
    9: "american-football",
    11: "hockey",
    13: "volleyball",
    14: "fighting",
    16: "handball",
}
DEFAULT_SPORT_TYPES: tuple[int, ...] = (1, 2, 3, 7, 8, 9, 11, 13, 14, 16)
_CATALOG_ENDPOINTS: tuple[str, ...] = (
    "/api/match/list",
    "/api/match/live",
    "/api/home/matches",
)
_DIGIT_CANDIDATES: tuple[str, ...] = ("seth", "trd", "sith", "foth", "fith")
_EVENT_SPLIT_RE = re.compile(r"\s+(?:vs\.?|v\.?|@|–|—|-)\s+", re.IGNORECASE)


def rot47(text: str) -> str:
    """Decode/encode FCTV33 `/api/common/params` ROT47 payload."""
    res: list[str] = []
    for ch in text:
        code = ord(ch)
        if 0x21 <= code <= 0x4F:
            res.append(chr(code + 0x2F))
        elif 0x50 <= code <= 0x7E:
            res.append(chr(code - 0x2F))
        else:
            res.append(ch)
    return "".join(res)


def read_varint(buffer: bytes, offset: int = 0) -> tuple[int, int]:
    """Read an unsigned base-128 Protobuf varint starting at `offset`."""
    value = 0
    shift = 0
    index = offset
    length = len(buffer)
    while index < length:
        byte = buffer[index]
        index += 1
        value |= (byte & 0x7F) << shift
        if (byte & 0x80) == 0:
            break
        shift += 7
    return value, index


def read_length_delimited(buffer: bytes, offset: int = 0) -> tuple[bytes, int]:
    """Read a length-delimited Protobuf bytes chunk starting at `offset`."""
    length, start = read_varint(buffer, offset)
    end = min(len(buffer), start + max(0, length))
    return buffer[start:end], end


def read_fields(buffer: bytes) -> dict[int, list[Any]]:
    """Decode top-level Protobuf wire fields (varint, fixed32/64, length-delimited)."""
    fields: dict[int, list[Any]] = {}
    offset = 0
    length = len(buffer)
    while offset < length:
        tag, next_offset = read_varint(buffer, offset)
        if next_offset == offset or tag <= 0:
            break
        offset = next_offset
        field = tag >> 3
        wire = tag & 0x7
        if wire == 0:
            val, after = read_varint(buffer, offset)
            offset = after
            fields.setdefault(field, []).append(val)
        elif wire == 1:
            if offset + 8 > length:
                break
            fields.setdefault(field, []).append(
                int.from_bytes(buffer[offset : offset + 8], "little")
            )
            offset += 8
        elif wire == 2:
            chunk, after = read_length_delimited(buffer, offset)
            offset = after
            fields.setdefault(field, []).append(chunk)
        elif wire == 5:
            if offset + 4 > length:
                break
            fields.setdefault(field, []).append(
                int.from_bytes(buffer[offset : offset + 4], "little")
            )
            offset += 4
        else:
            break
    return fields


def parse_api_envelope(buffer: bytes) -> dict[str, Any]:
    """Parse FCTV33 outer Protobuf envelope (`message` in field 3, `payload` in field 10)."""
    fields = read_fields(buffer)
    msg_chunks = fields.get(3, [])
    msg = ""
    if msg_chunks:
        first = msg_chunks[0]
        if isinstance(first, bytes):
            msg = first.decode("utf-8", errors="replace")
        elif isinstance(first, str):
            msg = first
    return {
        "message": msg,
        "payload": [c for c in fields.get(10, []) if isinstance(c, bytes)],
    }


def _decode_int_or_varint_bytes(raw: Any) -> Optional[int]:
    if isinstance(raw, int):
        return raw
    if isinstance(raw, bytes) and raw:
        if raw.isdigit():
            try:
                return int(raw.decode("ascii"))
            except ValueError:
                pass
        if len(raw) <= 8:
            val, _ = read_varint(raw, 0)
            return val
    return None


def _decode_id_str(raw: Any) -> str:
    if isinstance(raw, int):
        return str(raw)
    if isinstance(raw, bytes) and raw:
        if raw.isdigit():
            return raw.decode("ascii", errors="ignore")
        if len(raw) <= 8:
            return str(read_varint(raw, 0)[0])
        return raw.decode("utf-8", errors="replace").strip()
    return ""


def _decode_text(raw: Any) -> str:
    if isinstance(raw, bytes):
        # Skip raw binary submessages that contain non-printable control bytes
        text = raw.decode("utf-8", errors="replace").strip()
        if not text:
            return ""
        if any(ord(ch) < 32 and ch not in ("\t", "\n", "\r") for ch in text):
            return ""
        return text
    if isinstance(raw, str):
        return raw.strip()
    return ""


def _extract_team_name(raw: Any) -> str:
    if isinstance(raw, str):
        return raw.strip()
    if not isinstance(raw, bytes) or not raw:
        return ""
    direct = _decode_text(raw)
    if direct:
        return direct
    sub = read_fields(raw)
    for key in (2, 3, 1, 4):
        for cand in sub.get(key, []):
            txt = _decode_text(cand)
            if txt:
                return txt
    return ""


def parse_stream_item(buffer: bytes) -> dict[str, Any]:
    """Parse an FCTV33 stream item submessage matching `EasyProxy` contract."""
    fields = read_fields(buffer)
    stream_id = _decode_id_str(fields.get(1, [None])[0]) or None

    name = ""
    name_chunk = fields.get(3, [None])[0]
    if isinstance(name_chunk, bytes):
        name = name_chunk.decode("utf-8", errors="replace").strip()

    url = ""
    url_chunk = fields.get(4, [None])[0]
    if isinstance(url_chunk, bytes):
        url = url_chunk.decode("utf-8", errors="replace").strip()

    site_type = _decode_int_or_varint_bytes(fields.get(9, [None])[0])
    return {
        "streamId": stream_id,
        "name": name,
        "url": url,
        "siteType": site_type,
    }


def _normalize_timestamp_ms(val: Optional[int]) -> int:
    if val is None or val <= 0:
        return 0
    if 1_000_000_000 <= val <= 9_999_999_999:
        return val * 1000
    if 1_000_000_000_000 <= val <= 9_999_999_999_999:
        return val
    return 0


def build_fctv33_stream_url(
    match_id: str,
    stream_id: str,
    sport_type: int,
    site_type: int = DEFAULT_SITE_TYPE,
) -> str:
    """Format canonical `fctv33://` URL recognized by EasyProxy's `Fctv33Extractor`."""
    return (
        f"fctv33://{match_id}/{stream_id}"
        f"?sportType={int(sport_type)}&siteType={int(site_type or DEFAULT_SITE_TYPE)}"
    )


def parse_fctv33_match_message(
    buffer: bytes,
    *,
    default_sport_type: int = 1,
) -> Optional[ProviderEvent]:
    """Parse a single FCTV33 Protobuf match payload into a `ProviderEvent`."""
    if not buffer:
        return None
    fields = read_fields(buffer)
    if not fields:
        return None

    # Support wrapper layout `{1: match_info_bytes, 2: [stream_item_bytes, ...]}`
    match_fields = fields
    stream_chunks: list[bytes] = []
    f1 = fields.get(1, [None])[0]
    if isinstance(f1, bytes):
        nested = read_fields(f1)
        if nested.get(1) and isinstance(nested[1][0], (int, bytes)):
            match_fields = nested
            stream_chunks = [c for c in fields.get(2, []) if isinstance(c, bytes)]

    match_id = _decode_id_str(match_fields.get(1, [None])[0])
    if not match_id or not match_id.isdigit():
        return None

    sport_type = _decode_int_or_varint_bytes(match_fields.get(2, [None])[0])
    if sport_type is None or sport_type <= 0 or sport_type > 200:
        sport_type = default_sport_type

    competition = _decode_text(match_fields.get(3, [None])[0])
    home = _extract_team_name(match_fields.get(4, [None])[0])
    away = _extract_team_name(match_fields.get(5, [None])[0])

    # Timestamps commonly live in field 6 (or another varint field in epoch range)
    start_ms = _normalize_timestamp_ms(
        _decode_int_or_varint_bytes(match_fields.get(6, [None])[0])
    )
    if start_ms == 0:
        for fnum in (7, 8, 9, 10):
            for val in match_fields.get(fnum, []):
                if isinstance(val, int):
                    cand_ms = _normalize_timestamp_ms(val)
                    if cand_ms > 0:
                        start_ms = cand_ms
                        break
            if start_ms > 0:
                break

    if not home and not away:
        raw_title = _decode_text(match_fields.get(7, [None])[0])
        if raw_title:
            parts = _EVENT_SPLIT_RE.split(raw_title, maxsplit=1)
            if len(parts) == 2:
                home, away = parts[0].strip(), parts[1].strip()
            else:
                home = raw_title

    if not home:
        return None

    # Gather stream_item submessages from wrapper field 2 or match fields 7/8/11/12
    if not stream_chunks:
        for fnum in (8, 11, 12, 7):
            for cand in match_fields.get(fnum, []):
                if isinstance(cand, bytes):
                    item_info = parse_stream_item(cand)
                    if item_info.get("streamId"):
                        stream_chunks.append(cand)

    streams: list[StreamSource] = []
    seen_stream_ids: set[str] = set()
    for chunk in stream_chunks:
        item = parse_stream_item(chunk)
        sid = str(item.get("streamId") or "").strip()
        if not sid or not sid.isdigit() or sid in seen_stream_ids:
            continue
        seen_stream_ids.add(sid)
        site_type = int(item.get("siteType") or DEFAULT_SITE_TYPE)
        st_url = build_fctv33_stream_url(match_id, sid, sport_type, site_type)
        st_name = item.get("name") or f"FCTV33 #{len(streams) + 1}"
        streams.append(
            StreamSource(
                provider="fctv33",
                title=st_name,
                url=st_url,
                language="en",
                priority=6,
                requires_proxy=True,
                proxy_host="fctv33",
                raw_url=st_url,
            )
        )

    # Fallback: single scalar streamId on field 8/9 if no stream_item submessage
    if not streams:
        direct_sid = _decode_id_str(match_fields.get(8, [None])[0])
        if direct_sid and direct_sid.isdigit():
            site_type = (
                _decode_int_or_varint_bytes(match_fields.get(9, [None])[0])
                or DEFAULT_SITE_TYPE
            )
            st_url = build_fctv33_stream_url(match_id, direct_sid, sport_type, site_type)
            streams.append(
                StreamSource(
                    provider="fctv33",
                    title="FCTV33 Stream",
                    url=st_url,
                    language="en",
                    priority=6,
                    requires_proxy=True,
                    proxy_host="fctv33",
                    raw_url=st_url,
                )
            )

    if not streams:
        return None

    title = f"{home} vs {away}" if away else home
    is_women, is_youth = detect_demographics_from_stream_title(title)
    if competition and (not is_women or not is_youth):
        comp_w, comp_y = detect_demographics_from_stream_title(competition)
        is_women = is_women or comp_w
        is_youth = is_youth or comp_y

    return ProviderEvent(
        provider="fctv33",
        title=title,
        home=home,
        away=away,
        competition=competition,
        sport_hint=SPORT_TYPE_TO_HINT.get(sport_type, "others"),
        start_ms=start_ms,
        is_women=is_women,
        is_youth=is_youth,
        streams=streams,
    )


def parse_fctv33_catalog(
    buffer: bytes,
    *,
    default_sport_type: int = 1,
) -> list[ProviderEvent]:
    """Parse FCTV33 binary Protobuf envelope into `ProviderEvent` matches."""
    if not buffer:
        return []
    env = parse_api_envelope(buffer)
    payload_chunks = env.get("payload") or []
    if not payload_chunks:
        return []

    events: list[ProviderEvent] = []
    for chunk in payload_chunks:
        ev = parse_fctv33_match_message(chunk, default_sport_type=default_sport_type)
        if ev is not None:
            events.append(ev)
            continue
        # Container message holding repeated match submessages in fields 1/2/3/10
        inner_fields = read_fields(chunk)
        for fnum in (1, 2, 3, 10):
            for sub_chunk in inner_fields.get(fnum, []):
                if isinstance(sub_chunk, bytes):
                    sub_ev = parse_fctv33_match_message(
                        sub_chunk, default_sport_type=default_sport_type
                    )
                    if sub_ev is not None:
                        events.append(sub_ev)
    return events


def parse_common_params(rot47_text: str) -> dict[str, str]:
    """Decode FCTV33 `/api/common/params` ROT47 payload into handshake settings."""
    out = {
        "stream_digit": DEFAULT_STREAM_DIGIT,
        "player_referer": DEFAULT_PLAYER_REFERER,
        "sfver": "",
    }
    if not (rot47_text or "").strip():
        return out
    try:
        cfg = json.loads(rot47(rot47_text))
    except Exception:
        return out
    if not isinstance(cfg, dict):
        return out

    sfver = str(cfg.get("sfver") or cfg.get("common:sfver") or "").strip()
    if sfver:
        out["sfver"] = sfver

    raw_clients = cfg.get("common:web:client")
    if isinstance(raw_clients, str):
        try:
            web_clients = json.loads(raw_clients)
        except Exception:
            web_clients = {}
    elif isinstance(raw_clients, dict):
        web_clients = raw_clients
    else:
        web_clients = {}

    if isinstance(web_clients, dict):
        for digit in _DIGIT_CANDIDATES:
            entry = web_clients.get(digit) or {}
            if not isinstance(entry, dict):
                continue
            domain_list = entry.get("iframePlayerDomains") or []
            if domain_list:
                out["stream_digit"] = digit
                out["player_referer"] = f"https://{str(domain_list[0]).rstrip('/')}/"
                if not out["sfver"] and entry.get("sfver"):
                    out["sfver"] = str(entry["sfver"]).strip()
                break
    return out


class Fctv33Service(BaseStreamProvider):
    """Async FCTV33 Protobuf catalog harvester with dynamic domain & sfver handshake."""

    name: str = "fctv33"
    priority: int = 6

    def __init__(
        self,
        settings: Optional[Settings] = None,
        *,
        client: Optional[httpx.AsyncClient] = None,
        sport_types: Optional[Sequence[int]] = None,
    ) -> None:
        super().__init__(settings)
        self._external_client = client
        self._owned_client: Optional[httpx.AsyncClient] = None
        self._sport_types: tuple[int, ...] = tuple(sport_types or DEFAULT_SPORT_TYPES)

    @property
    def enabled(self) -> bool:
        """Return True when FCTV33 harvesting is enabled in Settings."""
        return bool(self.settings.ENABLE_FCTV33)

    def _get_client(self) -> httpx.AsyncClient:
        if self._external_client is not None:
            return self._external_client
        if self._owned_client is None or self._owned_client.is_closed:
            self._owned_client = create_doh_client(timeout=15.0)
        return self._owned_client

    async def aclose(self) -> None:
        """Close owned HTTP client if created internally."""
        if self._owned_client is not None and not self._owned_client.is_closed:
            await self._owned_client.aclose()
            self._owned_client = None

    async def _fetch_domains_and_handshake(
        self,
        client: httpx.AsyncClient,
    ) -> tuple[str, str, dict[str, str]]:
        site_url = ""
        data_api_base = DEFAULT_DATA_API_BASE

        try:
            dom_resp = await client.get(
                DOMAINS_CONFIG_URL, headers={"Accept": "application/json"}
            )
            if dom_resp.status_code == 200:
                cfg = dom_resp.json()
                if isinstance(cfg, dict):
                    raw_site = str(cfg.get("fctv33") or "").strip().rstrip("/")
                    if raw_site:
                        site_url = raw_site if raw_site.startswith("http") else f"https://{raw_site}"
                    raw_api = str(cfg.get("fctv33_api") or cfg.get("data_api") or "").strip().rstrip("/")
                    if raw_api:
                        data_api_base = raw_api if raw_api.startswith("http") else f"https://{raw_api}"
        except Exception as exc:
            logger.debug("FCTV33 domains config fetch failed, using fallback: %s", exc)

        params_headers: dict[str, str] = {}
        if site_url:
            params_headers["Referer"] = f"{site_url}/"
            params_headers["Origin"] = site_url

        handshake = {
            "stream_digit": DEFAULT_STREAM_DIGIT,
            "player_referer": DEFAULT_PLAYER_REFERER,
            "sfver": "",
        }
        try:
            resp = await client.get(
                f"{data_api_base}/api/common/params", headers=params_headers
            )
            if resp.status_code == 200 and resp.text.strip():
                handshake = parse_common_params(resp.text)
        except Exception as exc:
            logger.debug("FCTV33 common params handshake failed: %s", exc)

        return data_api_base, site_url, handshake

    async def _fetch_sport_catalog(
        self,
        client: httpx.AsyncClient,
        data_api_base: str,
        sport_type: int,
        handshake: dict[str, str],
    ) -> list[ProviderEvent]:
        referer = handshake.get("player_referer") or DEFAULT_PLAYER_REFERER
        headers = {
            "Referer": referer,
            "Origin": referer.rstrip("/"),
            "Accept": "*/*",
        }
        params: dict[str, str] = {
            "sportType": str(sport_type),
            "digit": handshake.get("stream_digit") or DEFAULT_STREAM_DIGIT,
            "country": "IT",
            "continent": "EU",
        }
        if handshake.get("sfver"):
            params["sfver"] = handshake["sfver"]

        for endpoint in _CATALOG_ENDPOINTS:
            try:
                resp = await client.get(
                    f"{data_api_base}{endpoint}", params=params, headers=headers
                )
                if resp.status_code != 200 or not resp.content:
                    continue
                parsed = parse_fctv33_catalog(
                    resp.content, default_sport_type=sport_type
                )
                if parsed:
                    return parsed
            except Exception as exc:
                logger.debug(
                    "FCTV33 catalog fetch failed (%s sportType=%s): %s",
                    endpoint,
                    sport_type,
                    exc,
                )
        return []

    async def harvest(self) -> ProviderHarvestResult:
        """Perform FCTV33 domain/params handshake and harvest Protobuf match catalog."""
        if not self.enabled:
            return ProviderHarvestResult(provider=self.name)

        client = self._get_client()
        data_api_base, _, handshake = await self._fetch_domains_and_handshake(client)

        tasks = [
            self._fetch_sport_catalog(client, data_api_base, st, handshake)
            for st in self._sport_types
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        events: list[ProviderEvent] = []
        errors: list[str] = []
        for res in results:
            if isinstance(res, Exception):
                errors.append(str(res))
            else:
                events.extend(res)

        return ProviderHarvestResult(
            provider=self.name,
            events=events,
            raw_count=len(events),
            error="; ".join(errors) if (not events and errors) else "",
        )
