"""FCTV33 Protobuf catalog harvester & dynamic domain/sfver handshake service."""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
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
DEFAULT_PLAYER_REFERER = "https://www.fctv33hd.host/"
DEFAULT_PLAYER_ORIGIN = "https://www.fctv33hd.host"
CHROME_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)
SFVER_CACHE_TTL_SECONDS = 600.0
DETAIL_CACHE_TTL_SECONDS = 90.0
LIVE_WINDOW_MS = 90 * 60 * 1000
DEFAULT_STREAM_DIGIT = "foth"
DEFAULT_SITE_TYPE = 2001

_SFVER_HEX_RE = re.compile(rb"[0-9a-f]{32}")
_IT_BROADCASTER_RE = re.compile(
    r"\b(?:it|ita|italia|italian|sky\s*sport|dazn|rai|mediaset|eurosport\s*it|supertennis)\b",
    re.IGNORECASE,
)

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


_ASIAN_EXOTIC_VOLLEY_COUNTRIES: frozenset[str] = frozenset({
    "south korea", "korea", "philippines", "thailand",
    "vietnam", "indonesia", "malaysia", "taiwan",
})
_ASIAN_EXOTIC_BASKET_COUNTRIES: frozenset[str] = frozenset({
    "china", "south korea", "korea", "philippines", "taiwan", "japan",
    "vietnam", "thailand", "indonesia", "malaysia", "singapore", "india",
    "mongolia", "asian", "africa", "rwanda", "uganda", "kenya", "senegal", "angola",
})
_EXOTIC_CLUB_COUNTRIES: frozenset[str] = frozenset({
    "myanmar", "vietnam", "cambodia", "laos", "indonesia", "thailand",
    "mauritania", "antigua", "antigua and barbuda", "suriname", "st. kitts",
    "st. kitts and nevis", "saint kitts", "maldives", "nepal", "bangladesh",
    "bhutan", "mongolia", "fiji", "tahiti", "uganda", "rwanda", "burundi",
    "somalia", "djibouti", "eritrea", "sudan", "south sudan", "gambia",
    "liberia", "sierra leone", "guinea-bissau", "tanzania", "kenya",
    "zambia", "zimbabwe",
})


def is_junk_event(
    sport_hint: str,
    competition: str,
    title: str,
    country: str = "",
) -> bool:
    """Filter out youth, reserve, amateur, and minor exotic FCTV33 events."""
    silo = (sport_hint or "").strip().lower()
    comp = (competition or "").strip().lower()
    cntry = (country or "").strip().lower()
    ttl = (title or "").strip().lower()
    comp_or_title = comp or ttl

    if silo == "tennis":
        if re.search(
            r"(risultati\s*in\s*tempo\s*reale|tennis:\s*atp|atp\s*&\s*wta|schedule\s*only|placeholder)",
            comp_or_title,
        ):
            return True
        if re.search(r"\b(table\s*tennis|ping\s*pong|tt\s*cup|setka\s*cup)\b", comp_or_title):
            return True
        return False

    if silo in ("motorsport", "motor-sports"):
        return bool(
            re.search(r"\b(tractor\s*pull|lawn\s*mower|demolition\s*derby|figure\s*8)\b", comp_or_title)
        )

    if silo in ("fighting", "fight"):
        if re.search(
            r"\b(muay\s*mun\s*sanan\s*mueang|the\s*global\s*fight|new\s*power\s*muay\s*thai|rajadamnern\s*knockout)\b",
            comp_or_title,
        ):
            return True
        if re.search(r"\bep\.\s*\d+\b", comp_or_title) and not re.search(
            r"\b(ufc|wwe|aew|one\s*championship)\b", comp_or_title
        ):
            return True
        return False

    if silo == "american-football":
        return bool(
            re.search(
                r"\b(fcs|ncaa\s*fcs|division\s*(?:ii|iii|1-aa|i-aa)|d-?[23]|naia|juco|high\s*school)\b",
                comp_or_title,
            )
        )

    if silo == "baseball":
        return bool(
            re.search(
                r"\b(milb|triple-a|double-a|single-a|class\s*a|rookie|minor\s*league|phoenix\s*league|college\s*baseball|ncaa)\b",
                comp_or_title,
            )
        )

    if silo == "hockey":
        return bool(
            re.search(r"\b(ahl|echl|ohl|whl|qmjhl|ushl|junior|college\s*hockey|ncaa)\b", comp_or_title)
        )

    if silo in ("volleyball", "volley"):
        full_text = f"{comp} {ttl} {cntry}"
        if re.search(r"\b(italia|italy|superlega|serie\s*a[123]?|coppa\s*italia)\b", full_text):
            return False
        if re.search(r"\b(college\s*volleyball|ncaa|district|universiade|amateur)\b", comp_or_title):
            return True
        is_asian_comp = bool(
            re.search(
                r"\b(kovo\s*cup|kovo|premier\s*volleyball|pvl|pro\s*challenge|v-league\s*korea|thailand\s*league|spikers\s*turf)\b",
                comp_or_title,
            )
        )
        if cntry in _ASIAN_EXOTIC_VOLLEY_COUNTRIES or is_asian_comp:
            if not re.search(r"\b(vnl|nations\s*league|world\s*championship|olympic|cev|champions\s*league)\b", comp_or_title):
                return True
        return False

    if silo == "basketball":
        full_text = f"{comp} {ttl} {cntry}"
        if re.search(
            r"\b(u-?1[0-9]|u-?2[01]|under\s*1[0-9]|under\s*2[01]|sub-?1[0-9]|sub-?20|junior|high\s*school)\b",
            comp_or_title,
        ):
            if not re.search(r"\b(italia|italy|lba|serie\s*a)\b", full_text):
                return True
        if re.search(
            r"\b(division\s*(?:ii|iii|1-aa|i-aa)|d-?[23]|naia|juco|mpbl|zone\s*5|amateur|development\s*league)\b",
            comp_or_title,
        ):
            return True
        is_women = bool(re.search(r"\b(women(?:['’]s)?|femminile|femenil|damen|ladies|wnbl|wbla)\b", comp_or_title)) or "women" in cntry
        if is_women:
            is_wnba = bool(re.search(r"\bwnba\b", comp_or_title)) or "women's national basketball association" in comp
            is_it_women = bool(re.search(r"\b(serie\s*a[12]?|coppa\s*italia|lba|italia|italy)\b", full_text))
            is_top_fiba = bool(re.search(r"\b(fiba\s*world\s*cup|eurobasket|olympic)\b", comp_or_title)) and "asia" not in comp_or_title
            if not (is_wnba or is_it_women or is_top_fiba):
                return True
        is_asian_comp = bool(
            re.search(
                r"\b(cba|chinese\s*basketball|kbl|korean\s*basketball|pba|philippine\s*basketball|governors\s*cup|commissioner\s*s\s*cup|t1\s*league|tpbl|plg|b\.league|road\s*to\s*b\.?a\.?l\.?|champions\s*clubs|basketball\s*africa\s*league|bal)\b",
                comp_or_title,
            )
        )
        if cntry in _ASIAN_EXOTIC_BASKET_COUNTRIES or is_asian_comp:
            is_top_fiba = bool(re.search(r"\b(fiba\s*(?:world\s*cup|qualifier)|olympic)\b", comp_or_title)) and not bool(
                re.search(r"\b(club|asia|bal|league)\b", comp_or_title)
            )
            if not is_top_fiba:
                return True
        if re.search(r"\b(aba\s*league\s*2|israeli\s*national\s*league)\b", comp_or_title):
            return True
        return False

    if silo == "football":
        if re.search(r"\b(college\s*soccer|ncaa)\b", comp_or_title):
            return True
        if re.search(
            r"\b(j[23]\s*league|k\s*league\s*[234]|torneo\s*betplay|colombia(?:n)?\s*torneo\s*betplay|liga\s*de\s*expansi[oó]n|usl\s*league\s*(?:one|two)|mls\s*next\s*pro|s[eé]rie\s*[cd]\s*brazil|primera\s*b\s*(?:nacional|chile|colombia|metropolitana)?)\b",
            comp_or_title,
        ):
            return True
        if re.search(r"\b(u-?\d{1,2}|under\s*\d{1,2}|sub-?\d{1,2}|soeratin|juvenil|cadete|allievi|giovanissimi)\b", comp_or_title):
            is_it_youth = bool(
                re.search(
                    r"\b(italia|italy|primavera|juventus|milan|inter|roma|atalanta|fiorentina|lazio|napoli|torino)\b",
                    f"{comp} {ttl}",
                )
            )
            is_u21_major = (
                bool(re.search(r"\b(u-?21|under\s*21)\b", comp_or_title))
                and bool(re.search(r"\b(euro|uefa|qualif)\b", comp_or_title))
                and "women" not in comp_or_title
            )
            if not is_it_youth and not is_u21_major:
                return True
        if re.search(r"\b(reserve|reserves|amateur|development\s*league)\b", comp_or_title):
            return True
        if re.search(r"\b(afcon|world cup|qualifier|nations league|copa america|asian cup|gold cup|futsal afcon)\b", comp_or_title):
            return False
        is_women = bool(re.search(r"\b(women(?:['’]s)?|femminile|femenil|dames|frauen|ladies)\b", comp_or_title))
        if is_women:
            is_top_women = bool(re.search(r"\b(serie a|coppa italia|italia|italy|champions|uwcl|world cup|nations league|euro)\b", comp_or_title))
            is_it_team = bool(re.search(r"\b(juventus|roma|inter|milan|fiorentina|sassuolo|como|lazio|sampdoria)\b", ttl))
            if not is_top_women and not is_it_team:
                return True
        if cntry in _EXOTIC_CLUB_COUNTRIES:
            return True
        for ex in _EXOTIC_CLUB_COUNTRIES:
            if re.search(rf"\b{re.escape(ex)}\b", comp_or_title):
                return True

    return False


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
                raw_url=f"{match_id}:{sid}",
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
                    raw_url=f"{match_id}:{direct_sid}",
                )
            )

    if not streams:
        return None

    title = f"{home} vs {away}" if away else home
    sport_hint = SPORT_TYPE_TO_HINT.get(sport_type, "others")
    if is_junk_event(sport_hint, competition, title):
        return None
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


def extract_sfver_from_bs(buffer: bytes) -> str:
    """Extract 32-char hex `sfver` signature from `/api/common/bs` Protobuf response."""
    if not buffer:
        return ""
    top = read_fields(buffer)
    for chunk in top.get(10, []):
        if isinstance(chunk, bytes):
            sub = read_fields(chunk)
            for cand in sub.get(2, []):
                if isinstance(cand, bytes):
                    txt = _decode_text(cand)
                    if re.fullmatch(r"[0-9a-f]{32}", txt):
                        return txt
                    m = _SFVER_HEX_RE.search(cand)
                    if m:
                        return m.group(0).decode("ascii")
    m = _SFVER_HEX_RE.search(buffer)
    return m.group(0).decode("ascii") if m else ""


def _read_nested_sub2(raw: Any) -> str:
    if not isinstance(raw, bytes) or not raw:
        return ""
    sub = read_fields(raw)
    for cand in sub.get(2, []):
        txt = _decode_text(cand)
        if txt:
            return txt
    return _decode_text(raw)


def parse_fctv33_live_catalog(
    buffer: bytes,
    *,
    sport_type: int = 1,
) -> tuple[list[dict[str, Any]], set[int]]:
    """Parse real live `/sfver{sig}/api/match/live` Protobuf catalog.

    Returns `(matches, active_match_ids)` where `matches` is a list of metadata dicts
    and `active_match_ids` comes from `wrapper[2]` (`field 50`).
    """
    if not buffer:
        return [], set()
    top = read_fields(buffer)
    payload_chunks = [c for c in top.get(10, []) if isinstance(c, bytes)]
    if not payload_chunks:
        return [], set()

    wrapper = read_fields(payload_chunks[0])
    active_ids: set[int] = set()
    for active_chunk in wrapper.get(2, []):
        if not isinstance(active_chunk, bytes):
            continue
        af = read_fields(active_chunk)
        mid = _decode_int_or_varint_bytes(af.get(50, [None])[0])
        if mid and mid > 0:
            active_ids.add(mid)

    matches: list[dict[str, Any]] = []
    for match_chunk in wrapper.get(1, []):
        if not isinstance(match_chunk, bytes):
            continue
        mf = read_fields(match_chunk)
        match_id = _decode_int_or_varint_bytes(mf.get(1, [None])[0])
        if not match_id or match_id <= 0:
            continue

        start_ms = _normalize_timestamp_ms(
            _decode_int_or_varint_bytes(mf.get(3, [None])[0])
        )

        league_name = ""
        country_name = ""
        comp_chunk = mf.get(10, [None])[0]
        if isinstance(comp_chunk, bytes):
            comp_f = read_fields(comp_chunk)
            if comp_f.get(3):
                league_name = _read_nested_sub2(comp_f[3][0])
            if comp_f.get(80) and isinstance(comp_f[80][0], bytes):
                c80 = read_fields(comp_f[80][0])
                if c80.get(3):
                    country_name = _read_nested_sub2(c80[3][0])

        competition = league_name
        if country_name and league_name and country_name.lower() not in league_name.lower():
            competition = f"{country_name} - {league_name}"

        event_title = ""
        teams: list[str] = []
        for c30 in mf.get(30, []):
            if not isinstance(c30, bytes):
                continue
            f30 = read_fields(c30)
            if not event_title and f30.get(2):
                event_title = _decode_text(f30[2][0])
            for t_chunk in f30.get(10, []):
                if not isinstance(t_chunk, bytes):
                    continue
                tf = read_fields(t_chunk)
                if tf.get(3):
                    t_name = _read_nested_sub2(tf[3][0])
                    if t_name:
                        teams.append(t_name)

        home = teams[0] if len(teams) >= 1 else ""
        away = teams[1] if len(teams) >= 2 else ""
        if (not home or not away) and event_title:
            parts = _EVENT_SPLIT_RE.split(event_title, maxsplit=1)
            if len(parts) == 2:
                home = home or parts[0].strip()
                away = away or parts[1].strip()
            elif not home:
                home = event_title

        if not home and not event_title:
            continue

        slug = _decode_text(mf.get(150, [None])[0])
        matches.append(
            {
                "match_id": match_id,
                "sport_type": sport_type,
                "start_ms": start_ms,
                "competition": competition,
                "home": home,
                "away": away,
                "title": event_title or (f"{home} vs {away}" if away else home),
                "slug": slug,
            }
        )
    return matches, active_ids


def parse_fctv33_match_detail(buffer: bytes, match_id: int) -> list[StreamSource]:
    """Parse `/api/match/detail` Protobuf response (`top[10][0]` -> `f10[2]`) into `StreamSource`s."""
    if not buffer:
        return []
    top = read_fields(buffer)
    payload_chunks = [c for c in top.get(10, []) if isinstance(c, bytes)]
    if not payload_chunks:
        return []

    f10 = read_fields(payload_chunks[0])
    raw_streams: list[tuple[int, int, str, str]] = []
    seen_sids: set[str] = set()

    for idx, st_chunk in enumerate(f10.get(2, [])):
        if not isinstance(st_chunk, bytes):
            continue
        sf = read_fields(st_chunk)
        sid = _decode_id_str(sf.get(1, [None])[0])
        if not sid or not sid.isdigit() or sid in seen_sids:
            continue
        seen_sids.add(sid)
        channel_name = _decode_text(sf.get(3, [None])[0])
        is_it = bool(channel_name and _IT_BROADCASTER_RE.search(channel_name))
        raw_streams.append((0 if is_it else 1, idx, sid, channel_name))

    raw_streams.sort(key=lambda item: (item[0], item[1]))
    sources: list[StreamSource] = []
    for lang_rank, _, sid, channel_name in raw_streams:
        target = f"{match_id}:{sid}"
        display_title = (
            f"FCTV33 Live • {channel_name} • 1080p 50fps"
            if channel_name
            else "FCTV33 Live • 1080p 50fps"
        )
        sources.append(
            StreamSource(
                provider="fctv33",
                title=display_title,
                url=target,
                quality="1080p",
                language="it" if lang_rank == 0 else "en",
                priority=6,
                requires_proxy=True,
                proxy_host="fctv33",
                channel_name=channel_name,
                raw_url=target,
            )
        )
    return sources


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
        self._sfver_cache: Optional[tuple[float, str]] = None
        self._detail_cache: dict[tuple[int, int], tuple[float, list[StreamSource]]] = {}
        self._detail_sem: Optional[asyncio.Semaphore] = None
        self._known_matches_by_sport: dict[int, list[dict[str, Any]]] = {}

    @property
    def known_matches(self) -> list[dict[str, Any]]:
        """Non-junk catalog metadata from recent harvests for JIT detail lookup."""
        return [m for items in self._known_matches_by_sport.values() for m in items]

    def _get_detail_sem(self) -> asyncio.Semaphore:
        if self._detail_sem is None:
            self._detail_sem = asyncio.Semaphore(8)
        return self._detail_sem

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

    @staticmethod
    def _live_headers() -> dict[str, str]:
        return {
            "User-Agent": CHROME_USER_AGENT,
            "Referer": DEFAULT_PLAYER_REFERER,
            "Origin": DEFAULT_PLAYER_ORIGIN,
            "Accept": "*/*",
        }

    async def _fetch_sfver_bs(
        self,
        client: httpx.AsyncClient,
        api_base: str,
    ) -> str:
        """Fetch 32-char `sfver` signature via `/api/common/bs` with 600s TTL cache."""
        now = time.monotonic()
        if self._sfver_cache and now - self._sfver_cache[0] < SFVER_CACHE_TTL_SECONDS:
            return self._sfver_cache[1]

        try:
            resp = await client.get(
                f"{api_base}/api/common/bs",
                params=[
                    ("code", "100"),
                    ("code", "101"),
                    ("sportType", "1"),
                    ("stream", "true"),
                ],
                headers=self._live_headers(),
            )
            if resp.status_code == 200 and resp.content:
                sig = extract_sfver_from_bs(resp.content)
                if sig:
                    self._sfver_cache = (now, sig)
                    return sig
        except Exception as exc:
            logger.debug("FCTV33 /api/common/bs handshake failed: %s", exc)
        return ""

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

    async def _fetch_match_detail_streams(
        self,
        client: httpx.AsyncClient,
        api_base: str,
        match_id: int,
        sport_type: int,
        sem: asyncio.Semaphore,
    ) -> list[StreamSource]:
        async with sem:
            try:
                resp = await client.get(
                    f"{api_base}/api/match/detail",
                    params={
                        "matchId": str(match_id),
                        "sportType": str(sport_type),
                        "digit": "foth",
                        "country": "IT",
                        "continent": "EU",
                    },
                    headers=self._live_headers(),
                )
                if resp.status_code == 200 and resp.content:
                    return parse_fctv33_match_detail(resp.content, match_id)
            except Exception as exc:
                logger.debug(
                    "FCTV33 match detail failed (matchId=%s sportType=%s): %s",
                    match_id,
                    sport_type,
                    exc,
                )
        return []

    async def resolve_match_streams(
        self,
        match_id: str,
        sport_type: int = 1,
    ) -> list[StreamSource]:
        """On-demand JIT `/api/match/detail` resolution with 90s TTL cache and shared DoH client."""
        raw_id = str(match_id or "").strip()
        if raw_id.startswith("fctv33-"):
            raw_id = raw_id[7:]
        if not raw_id.isdigit():
            return []

        mid = int(raw_id)
        stype = int(sport_type or 1)
        cache_key = (mid, stype)
        now = time.monotonic()
        cached = self._detail_cache.get(cache_key)
        if cached and now - cached[0] < DETAIL_CACHE_TTL_SECONDS:
            return cached[1]

        client = self._get_client()
        api_base = (self.settings.FCTV33_API_BASE or DEFAULT_DATA_API_BASE).rstrip("/")
        streams = await self._fetch_match_detail_streams(
            client, api_base, mid, stype, self._get_detail_sem()
        )
        if streams:
            self._detail_cache[cache_key] = (time.monotonic(), streams)
        return streams

    async def _fetch_live_sport_events(
        self,
        client: httpx.AsyncClient,
        api_base: str,
        sfver: str,
        sport_type: int,
        sem: asyncio.Semaphore,
    ) -> list[ProviderEvent]:
        resp = await client.get(
            f"{api_base}/sfver{sfver}/api/match/live",
            params={"sportType": str(sport_type), "stream": "true"},
            headers=self._live_headers(),
        )
        if resp.status_code != 200 or not resp.content:
            return []

        matches, active_ids = parse_fctv33_live_catalog(
            resp.content, sport_type=sport_type
        )
        if not matches:
            return []

        sport_hint = SPORT_TYPE_TO_HINT.get(sport_type, "others")
        matches = [
            m
            for m in matches
            if not is_junk_event(sport_hint, m["competition"], m["title"])
        ]
        self._known_matches_by_sport[sport_type] = [
            {
                "match_id": m["match_id"],
                "sport_type": sport_type,
                "home": m["home"],
                "away": m["away"],
                "title": m["title"],
                "competition": m["competition"],
                "start_ms": m["start_ms"],
            }
            for m in matches
        ]
        if not matches:
            return []

        now_ms = int(time.time() * 1000)
        if active_ids:
            target_matches = [m for m in matches if m["match_id"] in active_ids]
        else:
            target_matches = [
                m
                for m in matches
                if m["start_ms"] > 0 and abs(m["start_ms"] - now_ms) <= LIVE_WINDOW_MS
            ]

        if not target_matches:
            return []

        detail_tasks = [
            self._fetch_match_detail_streams(
                client, api_base, m["match_id"], sport_type, sem
            )
            for m in target_matches
        ]
        detail_results = await asyncio.gather(*detail_tasks, return_exceptions=True)

        events: list[ProviderEvent] = []
        for meta, streams_res in zip(target_matches, detail_results):
            if isinstance(streams_res, Exception) or not streams_res:
                continue
            title = meta["title"]
            competition = meta["competition"]
            is_women, is_youth = detect_demographics_from_stream_title(title)
            if competition and (not is_women or not is_youth):
                comp_w, comp_y = detect_demographics_from_stream_title(competition)
                is_women = is_women or comp_w
                is_youth = is_youth or comp_y

            events.append(
                ProviderEvent(
                    provider="fctv33",
                    title=title,
                    home=meta["home"],
                    away=meta["away"],
                    competition=competition,
                    sport_hint=SPORT_TYPE_TO_HINT.get(sport_type, "others"),
                    start_ms=meta["start_ms"],
                    is_women=is_women,
                    is_youth=is_youth,
                    streams=streams_res,
                )
            )
        return events

    async def harvest(self) -> ProviderHarvestResult:
        """Perform FCTV33 sfver handshake and harvest Protobuf live match catalog."""
        if not self.enabled:
            return ProviderHarvestResult(provider=self.name)

        client = self._get_client()
        api_base = (self.settings.FCTV33_API_BASE or DEFAULT_DATA_API_BASE).rstrip("/")

        events: list[ProviderEvent] = []
        errors: list[str] = []

        sfver = await self._fetch_sfver_bs(client, api_base)
        if sfver:
            sem = asyncio.Semaphore(8)
            live_tasks = [
                self._fetch_live_sport_events(client, api_base, sfver, st, sem)
                for st in self._sport_types
            ]
            live_results = await asyncio.gather(*live_tasks, return_exceptions=True)
            for res in live_results:
                if isinstance(res, Exception):
                    errors.append(str(res))
                else:
                    events.extend(res)

            if events or not errors:
                return ProviderHarvestResult(
                    provider=self.name,
                    events=events,
                    raw_count=len(events),
                    error="",
                )

        # Fallback to legacy domains.json + /api/common/params handshake
        data_api_base, _, handshake = await self._fetch_domains_and_handshake(client)
        tasks = [
            self._fetch_sport_catalog(client, data_api_base, st, handshake)
            for st in self._sport_types
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

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
