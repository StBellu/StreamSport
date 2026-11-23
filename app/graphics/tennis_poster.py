"""Broadcast-grade 1225x700 Tennis Poster generator with 4 layouts and athlete cutout waterfall."""

from __future__ import annotations

import asyncio
import io
import logging
import os
import re
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Optional
from urllib.parse import quote, quote_plus

import httpx
from curl_cffi.requests import AsyncSession as CurlAsyncSession
from PIL import Image, ImageDraw, ImageFilter, ImageOps
from rapidfuzz import fuzz

from app.config import Settings, get_settings
from app.core.database import DatabaseManager
from app.core.network import DEFAULT_USER_AGENT, create_standard_client
from app.graphics._utils import (
    image_to_jpeg_bytes,
    load_font,
    measure_text,
    safe_match_filename,
    strip_non_bmp,
)
from app.graphics.thesportsdb_service import render_dark_canvas
from app.models.domain import Match

logger = logging.getLogger(__name__)

CANVAS_WIDTH = 1225
CANVAS_HEIGHT = 700
ATP_SILHOUETTE_BYTES = 4729

_PIL_EXECUTOR = ThreadPoolExecutor(max_workers=4, thread_name_prefix="tennis-pil")

_DOUBLES_SPLIT_RE = re.compile(r"\s*[/&]\s*")
_WTA_TORSO_RE = re.compile(
    r"https?://[^\"'\s<>]*cloudfront\.net[^\"'\s<>]*-Torso_[^\"'\s<>]+\.(?:png|webp|jpg|jpeg)(?:\?[^\"'\s<>]*)?",
    re.IGNORECASE,
)
_WTA_TORSO_ANY_RE = re.compile(
    r"https?://[^\"'\s<>]+-Torso_[^\"'\s<>]+\.(?:png|webp|jpg|jpeg)(?:\?[^\"'\s<>]*)?",
    re.IGNORECASE,
)

_GENERIC_STAGE_MARKERS = (
    "tba",
    "tbd",
    "winner",
    "qualifier",
    "vincitore",
    "qualificato",
    "session",
    "sessione",
    "day ",
    "giorno ",
    "round ",
    "turno ",
    "ceremony",
    "multiplex",
    "diretta",
)

_MISSING_ATHLETE_TTL_SECONDS = 6 * 3600.0
_MISSING_ATHLETES: dict[str, float] = {}

_SURNAME_PARTICLES: frozenset[str] = frozenset(
    {"de", "la", "del", "da", "di", "van", "von", "der", "den", "ten", "ter", "el", "al"}
)

_SPECIAL_TEAM_ENTITIES: dict[str, str] = {
    "pacific oceania": "PAC",
    "team europe": "EUR",
    "europa": "EUR",
    "europe": "EUR",
    "team world": "WLD",
    "resto del mondo": "WLD",
    "world": "WLD",
}

# Country name (EN & IT) + ISO3 -> ISO2 lowercase for ESPN 500px flag CDN (130+ entries).
_COUNTRY_TO_ISO2: dict[str, str] = {
    "italy": "it", "italia": "it", "ita": "it", "it": "it",
    "spain": "es", "spagna": "es", "esp": "es", "es": "es",
    "usa": "us", "united states": "us", "stati uniti": "us", "us": "us",
    "france": "fr", "francia": "fr", "fra": "fr", "fr": "fr",
    "germany": "de", "germania": "de", "ger": "de", "deu": "de", "de": "de",
    "australia": "au", "aus": "au", "au": "au",
    "great britain": "gb", "united kingdom": "gb", "gran bretagna": "gb", "regno unito": "gb", "gbr": "gb", "uk": "gb", "gb": "gb",
    "england": "gb-eng", "inghilterra": "gb-eng", "eng": "gb-eng",
    "serbia": "rs", "srb": "rs", "rs": "rs",
    "canada": "ca", "can": "ca", "ca": "ca",
    "argentina": "ar", "arg": "ar", "ar": "ar",
    "netherlands": "nl", "holland": "nl", "olanda": "nl", "paesi bassi": "nl", "ned": "nl", "nl": "nl",
    "czechia": "cz", "czech republic": "cz", "repubblica ceca": "cz", "cechia": "cz", "cze": "cz", "cz": "cz",
    "poland": "pl", "polonia": "pl", "pol": "pl", "pl": "pl",
    "switzerland": "ch", "svizzera": "ch", "sui": "ch", "ch": "ch",
    "japan": "jp", "giappone": "jp", "jpn": "jp", "jp": "jp",
    "china": "cn", "cina": "cn", "chn": "cn", "cn": "cn",
    "brazil": "br", "brasile": "br", "bra": "br", "br": "br",
    "belgium": "be", "belgio": "be", "bel": "be", "be": "be",
    "austria": "at", "aut": "at", "at": "at",
    "croatia": "hr", "croazia": "hr", "cro": "hr", "hr": "hr",
    "ukraine": "ua", "ucraina": "ua", "ukr": "ua", "ua": "ua",
    "kazakhstan": "kz", "kazakistan": "kz", "kaz": "kz", "kz": "kz",
    "greece": "gr", "grecia": "gr", "gre": "gr", "gr": "gr",
    "denmark": "dk", "danimarca": "dk", "den": "dk", "dk": "dk",
    "norway": "no", "norvegia": "no", "nor": "no", "no": "no",
    "sweden": "se", "svezia": "se", "swe": "se", "se": "se",
    "romania": "ro", "rou": "ro", "ro": "ro",
    "bulgaria": "bg", "bul": "bg", "bg": "bg",
    "chile": "cl", "cile": "cl", "chi": "cl", "cl": "cl",
    "colombia": "co", "col": "co", "co": "co",
    "south korea": "kr", "corea del sud": "kr", "korea": "kr", "korea republic": "kr", "corea": "kr", "kor": "kr", "kr": "kr",
    "north korea": "kp", "corea del nord": "kp", "prk": "kp", "kp": "kp",
    "india": "in", "ind": "in", "in": "in",
    "slovakia": "sk", "slovacchia": "sk", "svk": "sk", "sk": "sk",
    "slovenia": "si", "slo": "si", "si": "si",
    "hungary": "hu", "ungheria": "hu", "hun": "hu", "hu": "hu",
    "portugal": "pt", "portogallo": "pt", "por": "pt", "pt": "pt",
    "finland": "fi", "finlandia": "fi", "fin": "fi", "fi": "fi",
    "latvia": "lv", "lettonia": "lv", "lat": "lv", "lv": "lv",
    "lithuania": "lt", "lituania": "lt", "ltu": "lt", "lt": "lt",
    "estonia": "ee", "est": "ee", "ee": "ee",
    "russia": "ru", "rus": "ru", "ru": "ru",
    "belarus": "by", "bielorussia": "by", "blr": "by", "by": "by",
    "taiwan": "tw", "chinese taipei": "tw", "taipei cinese": "tw", "tpe": "tw", "tpo": "tw", "tw": "tw",
    "hong kong": "hk", "hkg": "hk", "hk": "hk",
    "thailand": "th", "thailandia": "th", "tha": "th", "th": "th",
    "indonesia": "id", "ina": "id", "id": "id",
    "philippines": "ph", "filippine": "ph", "phi": "ph", "ph": "ph",
    "vietnam": "vn", "vie": "vn", "vn": "vn",
    "malaysia": "my", "malesia": "my", "mas": "my", "my": "my",
    "singapore": "sg", "sin": "sg", "sg": "sg",
    "pakistan": "pk", "pak": "pk", "pk": "pk",
    "uzbekistan": "uz", "uzb": "uz", "uz": "uz",
    "georgia": "ge", "geo": "ge", "ge": "ge",
    "armenia": "am", "arm": "am", "am": "am",
    "azerbaijan": "az", "azerbaigian": "az", "aze": "az", "az": "az",
    "mexico": "mx", "messico": "mx", "mex": "mx", "mx": "mx",
    "peru": "pe", "perù": "pe", "per": "pe", "pe": "pe",
    "uruguay": "uy", "uru": "uy", "uy": "uy",
    "paraguay": "py", "par": "py", "py": "py",
    "ecuador": "ec", "ecu": "ec", "ec": "ec",
    "bolivia": "bo", "bol": "bo", "bo": "bo",
    "venezuela": "ve", "ven": "ve", "ve": "ve",
    "puerto rico": "pr", "porto rico": "pr", "pur": "pr", "pr": "pr",
    "dominican republic": "do", "repubblica dominicana": "do", "dom": "do", "do": "do",
    "costa rica": "cr", "crc": "cr", "cr": "cr",
    "el salvador": "sv", "esa": "sv", "sv": "sv",
    "barbados": "bb", "brb": "bb", "bb": "bb",
    "new zealand": "nz", "nuova zelanda": "nz", "nzl": "nz", "nz": "nz",
    "fiji": "fj", "figi": "fj", "fij": "fj", "fj": "fj",
    "samoa": "ws", "wsm": "ws", "ws": "ws",
    "papua new guinea": "pg", "papua nuova guinea": "pg", "png": "pg", "pg": "pg",
    "south africa": "za", "sudafrica": "za", "rsa": "za", "za": "za",
    "tunisia": "tn", "tun": "tn", "tn": "tn",
    "morocco": "ma", "marocco": "ma", "mar": "ma", "ma": "ma",
    "algeria": "dz", "alg": "dz", "dz": "dz",
    "egypt": "eg", "egitto": "eg", "egy": "eg", "eg": "eg",
    "ivory coast": "ci", "cote d ivoire": "ci", "costa d avorio": "ci", "civ": "ci", "ci": "ci",
    "israel": "il", "israele": "il", "isr": "il", "il": "il",
    "turkey": "tr", "turchia": "tr", "turkiye": "tr", "tur": "tr", "tr": "tr",
    "saudi arabia": "sa", "arabia saudita": "sa", "ksa": "sa", "sa": "sa",
    "united arab emirates": "ae", "emirati arabi uniti": "ae", "uae": "ae", "ae": "ae",
    "qatar": "qa", "qat": "qa", "qa": "qa",
    "jordan": "jo", "giordania": "jo", "jor": "jo", "jo": "jo",
    "lebanon": "lb", "libano": "lb", "lib": "lb", "lb": "lb",
    "syria": "sy", "siria": "sy", "syr": "sy", "sy": "sy",
    "iran": "ir", "irn": "ir", "ir": "ir",
    "iraq": "iq", "irq": "iq", "iq": "iq",
    "kuwait": "kw", "kuw": "kw", "kw": "kw",
    "oman": "om", "oma": "om", "om": "om",
    "bahrain": "bh", "bahrein": "bh", "bhr": "bh", "bh": "bh",
    "palestine": "ps", "palestina": "ps", "ple": "ps", "ps": "ps",
    "ireland": "ie", "irlanda": "ie", "irl": "ie", "ie": "ie",
    "luxembourg": "lu", "lussemburgo": "lu", "lux": "lu", "lu": "lu",
    "bosnia and herzegovina": "ba", "bosnia herzegovina": "ba", "bosnia ed erzegovina": "ba", "bih": "ba", "ba": "ba",
    "north macedonia": "mk", "macedonia del nord": "mk", "macedonia": "mk", "mkd": "mk", "mk": "mk",
    "moldova": "md", "moldavia": "md", "mda": "md", "md": "md",
    "cyprus": "cy", "cipro": "cy", "cyp": "cy", "cy": "cy",
    "malta": "mt", "mlt": "mt", "mt": "mt",
    "monaco": "mc", "mon": "mc", "mc": "mc",
    "tajikistan": "tj", "tagikistan": "tj", "tjk": "tj", "tj": "tj",
    "turkmenistan": "tm", "tkm": "tm", "tm": "tm",
    "kyrgyzstan": "kg", "kirghizistan": "kg", "kgz": "kg", "kg": "kg",
    "bangladesh": "bd", "ban": "bd", "bd": "bd",
    "nepal": "np", "nep": "np", "np": "np",
    "sri lanka": "lk", "sri": "lk", "lk": "lk",
    "myanmar": "mm", "mya": "mm", "mm": "mm",
    "cambodia": "kh", "cambogia": "kh", "cam": "kh", "kh": "kh",
    "laos": "la", "lao": "la", "la": "la",
    "maldives": "mv", "maldive": "mv", "mdv": "mv", "mv": "mv",
    "mongolia": "mn", "mgl": "mn", "mn": "mn",
    "bhutan": "bt", "bhu": "bt", "bt": "bt",
    "brunei": "bn", "brunei darussalam": "bn", "bru": "bn", "bn": "bn",
    "yemen": "ye", "yem": "ye", "ye": "ye",
    "macau": "mo", "macao": "mo", "mac": "mo", "mo": "mo",
    "guam": "gu", "gum": "gu", "gu": "gu",
}


def select_tournament_background(
    competition: str,
    is_women: bool = False,
    bg_dir: Optional[Path] = None,
) -> Path:
    """Select the official 1225x700 tournament background file from `TENNIS_BG_DIR`."""
    directory = bg_dir or get_settings().TENNIS_BG_DIR
    comp = (competition or "").lower()

    if "australian open" in comp:
        filename = "bg_australian_open.jpg"
    elif "roland garros" in comp or "french open" in comp:
        filename = "bg_roland_garros.jpg"
    elif "wimbledon" in comp:
        filename = "bg_wimbledon.jpg"
    elif "us open" in comp:
        filename = "bg_us_open.jpg"
    elif "davis cup" in comp or "coppa davis" in comp:
        filename = "bg_davis_cup.jpg"
    elif "bjk" in comp or "billie jean king" in comp:
        filename = "bg_bjk_cup.jpg"
    elif "united cup" in comp:
        filename = "bg_united_cup.jpg"
    elif "laver cup" in comp:
        filename = "bg_laver_cup.jpg"
    elif "challenger" in comp:
        filename = "bg_challenger.jpg"
    else:
        filename = "bg_wta.jpg" if is_women else "bg_atp.jpg"

    candidate = directory / filename
    if candidate.is_file():
        return candidate
    fallback = directory / ("bg_wta.jpg" if is_women else "bg_atp.jpg")
    if fallback.is_file():
        return fallback
    return directory / "bg_atp.jpg"


def _slugify_player(name: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "-", (name or "").lower()).strip("-")
    return cleaned


def _deabbreviate_player_name(name: str, slug: str) -> str:
    """Expand abbreviated names like `J. Sinner` or `Sinner J.` using Flashscore `slug`."""
    clean = strip_non_bmp(name).strip()
    if slug:
        slug_parts = [p for p in slug.strip("/").split("-") if p and not p.isdigit()]
        # Diretta slugs follow `lastname-firstname` (e.g. `paolini-jasmine`).
        if len(slug_parts) >= 2:
            if "." in clean or len(clean.split()) <= 2:
                return " ".join(p.capitalize() for p in reversed(slug_parts))
    return clean


def _unaccent(text: str) -> str:
    nfkd = unicodedata.normalize("NFKD", text or "")
    return "".join(ch for ch in nfkd if not unicodedata.combining(ch))


def _is_player_match(query: str, candidate_name: str) -> bool:
    """Strict athlete identity verification (surname + initial + particle support)."""
    q = _unaccent(re.sub(r"[\(\)\[\]]", "", query or "")).strip().lower()
    c = _unaccent(candidate_name or "").strip().lower()
    if not q or not c:
        return False
    if q == c:
        return True

    q_tokens = [t for t in re.split(r"[\s.\-]+", q) if t]
    c_tokens = [t for t in re.split(r"[\s.\-]+", c) if t]
    if not q_tokens or not c_tokens:
        return False

    q_initials = [t[0] for t in q_tokens if len(t) == 1]
    q_words = [t for t in q_tokens if len(t) > 1]
    c_words = [t for t in c_tokens if len(t) > 1]
    if not q_words or not c_words:
        return False

    q_sig = [w for w in q_words if w not in _SURNAME_PARTICLES]
    c_sig = [w for w in c_words if w not in _SURNAME_PARTICLES]
    if not q_sig or not c_sig:
        return False

    if set(q_sig) == set(c_sig):
        return True
    if len(c_sig) >= 2 and set(c_sig).issubset(set(q_sig)):
        return True
    if len(q_sig) >= 2 and set(q_sig).issubset(set(c_sig)):
        return True

    # Trailing initials ("Etcheverry T. M." / "Herbert P.H.") -> surname is first word.
    q_sur = q_words[0] if len(q_tokens[-1]) == 1 else q_words[-1]
    c_sur = c_words[0] if len(c_tokens[-1]) == 1 else c_words[-1]

    if q_sur not in c_words and c_sur not in q_words:
        return False

    if q_initials:
        c_given_inits = [t[0] for t in c_tokens if t != c_sur and t not in _SURNAME_PARTICLES]
        if q_initials[0] not in c_given_inits:
            return False
    elif len(q_words) > 1:
        q_given = [t for t in q_words if t != q_sur and t not in _SURNAME_PARTICLES]
        c_given = [t for t in c_words if t != c_sur and t not in _SURNAME_PARTICLES]
        if q_given and c_given:
            if not ({t[0] for t in q_given} & {t[0] for t in c_given}):
                return False

    return True


def _extract_surname(player_name: str) -> str:
    """Extract clean uppercase surname (handles particles and trailing double initials)."""
    clean = strip_non_bmp(player_name).strip()
    if not clean:
        return ""
    tokens = clean.split()
    if len(tokens) == 1:
        return tokens[0].upper()

    # Strip trailing initials such as "Etcheverry T. M." or "Sinner J."
    while len(tokens) > 1 and (
        (len(tokens[-1]) <= 3 and tokens[-1].endswith("."))
        or (len(tokens[-1]) == 1 and tokens[-1].isalpha())
    ):
        tokens.pop()
    if len(tokens) == 1:
        return tokens[0].upper()

    # Strip leading initials such as "J. Sinner" or "T. M. Etcheverry"
    while len(tokens) > 1 and (
        (len(tokens[0]) <= 3 and tokens[0].endswith("."))
        or (len(tokens[0]) == 1 and tokens[0].isalpha())
    ):
        tokens.pop(0)
    if len(tokens) == 1:
        return tokens[0].upper()

    # Preserve compound surname particles ("Alex De Minaur", "Botic Van De Zandschulp")
    surname_parts = [tokens[-1]]
    for tok in reversed(tokens[:-1]):
        if tok.lower() in _SURNAME_PARTICLES:
            surname_parts.insert(0, tok)
        else:
            break
    return " ".join(surname_parts).upper()


def _resolve_country_iso2(team_name: str, country_hint: str = "") -> Optional[str]:
    for raw in (team_name, country_hint):
        cleaned = re.sub(r"[^a-z\s]", " ", _unaccent(raw).lower()).strip()
        cleaned = " ".join(cleaned.split())
        if not cleaned:
            continue
        if cleaned in _SPECIAL_TEAM_ENTITIES:
            return _SPECIAL_TEAM_ENTITIES[cleaned]
        if cleaned in _COUNTRY_TO_ISO2:
            return _COUNTRY_TO_ISO2[cleaned]
        stripped = re.sub(r"\b(?:w|women|men|team)\b", "", cleaned).strip()
        if stripped in _SPECIAL_TEAM_ENTITIES:
            return _SPECIAL_TEAM_ENTITIES[stripped]
        if stripped in _COUNTRY_TO_ISO2:
            return _COUNTRY_TO_ISO2[stripped]
    return None


def _is_country_tie(match: Match) -> Optional[tuple[str, str]]:
    comp = f"{match.competition} {match.canonical_competition} {match.alt_league}".lower()
    if not any(kw in comp for kw in ("davis cup", "coppa davis", "bjk", "billie jean king", "united cup", "laver cup")):
        return None
    if not match.home_team or not match.away_team:
        return None
    iso_h = _resolve_country_iso2(match.home_team.name, match.home_team.country)
    iso_a = _resolve_country_iso2(match.away_team.name, match.away_team.country)
    if iso_h and iso_a:
        return iso_h, iso_a
    return None


def _is_stage_card(match: Match) -> bool:
    if not match.home_team or not match.away_team:
        return True
    h = match.home_team.name.strip().lower()
    a = match.away_team.name.strip().lower()
    if not h or not a:
        return True
    for marker in _GENERIC_STAGE_MARKERS:
        if h.startswith(marker) or a.startswith(marker) or marker in h or marker in a:
            return True
    return False


def _split_doubles(name: str) -> list[str]:
    parts = [p.strip() for p in _DOUBLES_SPLIT_RE.split(name or "") if p.strip()]
    return parts[:2]


def _validate_and_prepare_cutout(
    img_bytes: bytes,
    *,
    require_strict_vertical: bool = True,
    provider: str = "",
) -> Optional[bytes]:
    """Verify valid image bytes, reject 4729B ATP silhouettes & horizontal banners."""
    if not img_bytes:
        return None
    if provider == "atp" and len(img_bytes) == ATP_SILHOUETTE_BYTES:
        return None
    if len(img_bytes) == ATP_SILHOUETTE_BYTES:
        # Strict ATP default grey silhouette rejection regardless of source path.
        return None

    try:
        with Image.open(io.BytesIO(img_bytes)) as img:
            img.load()
            w, h = img.size
            if w <= 0 or h <= 0:
                return None

            if img.mode == "RGBA":
                alpha_bbox = img.getchannel("A").getbbox()
                if alpha_bbox is None:
                    return None
                cropped = img.crop(alpha_bbox)
                cw, ch = cropped.size
                if require_strict_vertical and ch <= cw:
                    return None
                if not require_strict_vertical and cw > ch * 1.45:
                    return None
                out = io.BytesIO()
                cropped.save(out, format="PNG")
                return out.getvalue()

            if require_strict_vertical and h <= w:
                return None
            if not require_strict_vertical and w > h * 1.45:
                return None
            return img_bytes
    except Exception:
        return None


def load_fallback_silhouette_bytes(is_women: bool, assets_dir: Optional[Path] = None) -> bytes:
    """Read local PNG fallback silhouette from `media/Tennis/assets/` (read-only)."""
    directory = assets_dir or get_settings().TENNIS_ASSETS_DIR
    filename = "Unknow_female.png" if is_women else "Unknow_male.png"
    path = directory / filename
    return path.read_bytes()


def _build_player_queries(query_name: str) -> list[str]:
    clean = strip_non_bmp(query_name).strip()
    if not clean:
        return []
    queries = [clean]
    surname = _extract_surname(clean).title()
    if surname and surname.lower() != clean.lower():
        queries.append(surname)
    return queries


async def _fetch_espn_cutout(
    query_name: str,
    client: httpx.AsyncClient,
) -> Optional[tuple[str, bytes]]:
    best_url = ""
    best_score = 0.0
    for q in _build_player_queries(query_name):
        search_url = f"https://site.web.api.espn.com/apis/search/v2?query={quote_plus(q)}&type=player"
        try:
            resp = await client.get(search_url)
            if resp.status_code != 200:
                continue
            payload = resp.json()
        except Exception:
            continue

        for block in payload.get("results", []) or []:
            if block.get("type") not in (None, "player"):
                continue
            for item in block.get("contents", []) or []:
                sport_hint = f"{item.get('subtitle', '')} {item.get('sport', '')} {item.get('description', '')}".lower()
                if sport_hint and not any(k in sport_hint for k in ("tennis", "atp", "wta")):
                    continue
                cand_name = str(item.get("displayName") or item.get("name") or "").strip()
                if not _is_player_match(query_name, cand_name):
                    continue
                score = fuzz.token_set_ratio(query_name.lower(), cand_name.lower())
                img_obj = item.get("image") or {}
                img_url = img_obj.get("default") if isinstance(img_obj, dict) else str(img_obj)
                if not img_url or "a.espncdn.com" not in img_url:
                    continue
                if score > best_score:
                    best_score = score
                    best_url = img_url
        if best_url:
            break

    if not best_url:
        return None

    try:
        img_resp = await client.get(best_url)
        if img_resp.status_code != 200:
            return None
        validated = _validate_and_prepare_cutout(
            img_resp.content,
            require_strict_vertical=False,
            provider="espn",
        )
        if validated is not None:
            return best_url, validated
    except Exception:
        return None
    return None


async def _fetch_atp_cutout(query_name: str) -> Optional[tuple[str, bytes]]:
    """Query ATP Tour player finder via Safari 15.5 impersonation and reject 4729B silhouettes."""
    try:
        async with CurlAsyncSession(impersonate="safari15_5", timeout=15) as session:
            best_pid = ""
            best_score = -1.0
            for q in _build_player_queries(query_name):
                find_url = f"https://www.atptour.com/-/www/players/find/byname/{quote(q)}/en"
                resp = await session.get(find_url)
                if resp.status_code != 200:
                    continue
                data = resp.json()
                if not isinstance(data, list) or not data:
                    continue

                for entry in data:
                    if not isinstance(entry, dict):
                        continue
                    pid = str(entry.get("PlayerId") or entry.get("playerId") or "").strip()
                    full = str(
                        entry.get("PlayerName")
                        or f"{entry.get('FirstName', '')} {entry.get('LastName', '')}"
                    ).strip()
                    if not pid or not _is_player_match(query_name, full):
                        continue
                    score = float(fuzz.token_set_ratio(query_name.lower(), full.lower()))
                    status = str(entry.get("Active") or entry.get("strStatus") or "").strip().lower()
                    if status in ("a", "active"):
                        score += 50.0
                    if score > best_score:
                        best_score = score
                        best_pid = pid
                if best_pid:
                    break

            if not best_pid:
                return None

            candidate_urls = (
                f"https://www.atptour.com/-/media/alias/player-gladiator-headshot/{best_pid}",
                f"https://www.atptour.com/-/media/alias/player-headshot/{best_pid}",
            )
            for img_url in candidate_urls:
                img_resp = await session.get(img_url)
                if img_resp.status_code != 200:
                    continue
                raw = img_resp.content
                if len(raw) == ATP_SILHOUETTE_BYTES:
                    logger.debug("Discarded 4729-byte ATP grey silhouette for %s", query_name)
                    continue
                validated = _validate_and_prepare_cutout(
                    raw,
                    require_strict_vertical=True,
                    provider="atp",
                )
                if validated is not None:
                    return img_url, validated
    except Exception as exc:
        logger.debug("ATP cutout lookup failed for '%s': %s", query_name, exc)
    return None


async def _fetch_wta_cutout(
    query_name: str,
    client: httpx.AsyncClient,
) -> Optional[tuple[str, bytes]]:
    """Query WTA API & profile HTML for CloudFront `-Torso_` cutouts with `?height=600`."""
    best_player: Optional[dict[str, Any]] = None
    best_score = -1.0

    for q in _build_player_queries(query_name):
        api_url = f"https://api.wtatennis.com/tennis/players?name={quote(q)}"
        try:
            resp = await client.get(api_url)
            if resp.status_code != 200:
                continue
            payload = resp.json()
        except Exception:
            continue

        players = payload if isinstance(payload, list) else payload.get("content", []) or []
        for item in players:
            if not isinstance(item, dict):
                continue
            full = str(
                item.get("fullName")
                or f"{item.get('firstName', '')} {item.get('lastName', '')}"
            ).strip()
            if not _is_player_match(query_name, full):
                continue
            score = float(fuzz.token_set_ratio(query_name.lower(), full.lower()))
            status = str(item.get("strStatus") or item.get("status") or "").strip().lower()
            if status in ("a", "active"):
                score += 50.0
            if score > best_score:
                best_score = score
                best_player = item
        if best_player:
            break

    if not best_player:
        return None

    pid = str(best_player.get("id") or best_player.get("playerId") or "").strip()
    if not pid:
        return None
    full_name = str(
        best_player.get("fullName")
        or f"{best_player.get('firstName', '')}-{best_player.get('lastName', '')}"
    )
    name_slug = _slugify_player(full_name)
    profile_url = (
        f"https://www.wtatennis.com/players/{pid}/{name_slug}"
        if name_slug
        else f"https://www.wtatennis.com/players/{pid}"
    )

    try:
        prof_resp = await client.get(profile_url)
        if prof_resp.status_code != 200:
            return None
        html = prof_resp.text
    except Exception:
        return None

    match_url = _WTA_TORSO_RE.search(html) or _WTA_TORSO_ANY_RE.search(html)
    if not match_url:
        return None
    torso_url = match_url.group(0)
    low_url = torso_url.lower()
    if any(bad in low_url for bad in ("home-share", "banner", "tile", "landscape")):
        return None
    if "?" not in torso_url:
        torso_url = f"{torso_url}?height=600"

    try:
        img_resp = await client.get(torso_url)
        if img_resp.status_code != 200:
            return None
        validated = _validate_and_prepare_cutout(
            img_resp.content,
            require_strict_vertical=True,
            provider="wta",
        )
        if validated is not None:
            return torso_url, validated
    except Exception:
        return None
    return None


async def fetch_athlete_cutout(
    name: str,
    slug: str,
    is_women: bool,
    db: Optional[DatabaseManager],
    client: httpx.AsyncClient,
    *,
    assets_dir: Optional[Path] = None,
) -> bytes:
    """Resolve an athlete cutout PNG/image bytes via 4-step waterfall with fallback silhouette."""
    query_name = _deabbreviate_player_name(name, slug)
    cache_slug = (slug or _slugify_player(query_name)).strip().lower()
    gender_label = "women" if is_women else "men"

    # Step 1: SQLite cache + live URL HTTP 200 & vertical aspect ratio check.
    if db is not None and cache_slug:
        cached = await db.get_tennis_athlete(cache_slug)
        if cached and cached.get("image_url") and cached.get("gender") == gender_label:
            cached_url = str(cached["image_url"])
            cached_provider = str(cached.get("provider") or "")
            try:
                resp = await client.get(cached_url)
                if resp.status_code == 200:
                    strict_vert = cached_provider != "espn"
                    validated = _validate_and_prepare_cutout(
                        resp.content,
                        require_strict_vertical=strict_vert,
                        provider=cached_provider,
                    )
                    if validated is not None:
                        return validated
            except Exception:
                pass
            await db.delete_tennis_athlete(cache_slug)

    if query_name:
        neg_key = f"{gender_label}:{cache_slug or query_name.lower()}"
        neg_exp = _MISSING_ATHLETES.get(neg_key)
        if neg_exp is not None and time.monotonic() < neg_exp:
            return load_fallback_silhouette_bytes(is_women, assets_dir=assets_dir)

        # Step 2: ESPN Search API.
        espn_res = await _fetch_espn_cutout(query_name, client)
        if espn_res is not None:
            img_url, img_bytes = espn_res
            if db is not None and cache_slug:
                await db.upsert_tennis_athlete(cache_slug, "espn", img_url, gender_label)
            return img_bytes

        # Step 3: Strict gender-partitioned official tours (ATP vs WTA).
        if not is_women:
            atp_res = await _fetch_atp_cutout(query_name)
            if atp_res is not None:
                img_url, img_bytes = atp_res
                if db is not None and cache_slug:
                    await db.upsert_tennis_athlete(cache_slug, "atp", img_url, gender_label)
                return img_bytes
        else:
            wta_res = await _fetch_wta_cutout(query_name, client)
            if wta_res is not None:
                img_url, img_bytes = wta_res
                if db is not None and cache_slug:
                    await db.upsert_tennis_athlete(cache_slug, "wta", img_url, gender_label)
                return img_bytes

        _MISSING_ATHLETES[neg_key] = time.monotonic() + _MISSING_ATHLETE_TTL_SECONDS

    # Step 4: Local fallback silhouettes (never persisted to DB cache).
    return load_fallback_silhouette_bytes(is_women, assets_dir=assets_dir)


def _rounded_mask(size: tuple[int, int], radius: int) -> Image.Image:
    mask = Image.new("L", size, 0)
    draw = ImageDraw.Draw(mask)
    draw.rounded_rectangle((0, 0, size[0], size[1]), radius=radius, fill=255)
    return mask


def _scale_cutout_to_height(
    cutout_bytes: bytes,
    target_height: int,
    *,
    mirror: bool = False,
) -> Image.Image:
    with Image.open(io.BytesIO(cutout_bytes)) as src:
        rgba = src.convert("RGBA")
        if mirror:
            rgba = ImageOps.mirror(rgba)
        w, h = rgba.size
        if h <= 0:
            return rgba
        ratio = target_height / float(h)
        new_w = max(1, int(round(w * ratio)))
        return rgba.resize((new_w, target_height), Image.Resampling.LANCZOS)


def _paste_rgba_centered(
    canvas: Image.Image,
    sprite: Image.Image,
    center_x: int,
    bottom_y: int,
) -> None:
    x = center_x - (sprite.width // 2)
    y = bottom_y - sprite.height
    canvas.alpha_composite(sprite, dest=(x, y))


def _draw_lower_third_banner(
    canvas: Image.Image,
    headline: str,
    subtitle: str,
) -> None:
    """Composite semi-transparent obsidian glass lower-third banner."""
    box_x0, box_y0 = 48, 556
    box_x1, box_y1 = CANVAS_WIDTH - 48, 674
    box_w = box_x1 - box_x0
    box_h = box_y1 - box_y0

    overlay = Image.new("RGBA", canvas.size, (0, 0, 0, 0))
    odraw = ImageDraw.Draw(overlay)
    odraw.rounded_rectangle(
        (box_x0, box_y0, box_x1, box_y1),
        radius=18,
        fill=(10, 14, 22, 212),
        outline=(255, 255, 255, 48),
        width=2,
    )
    # Subtle top accent bar inside the obsidian box.
    odraw.rounded_rectangle(
        (box_x0 + 28, box_y0 + 1, box_x1 - 28, box_y0 + 4),
        radius=2,
        fill=(245, 197, 24, 180),
    )
    canvas.alpha_composite(overlay)

    draw = ImageDraw.Draw(canvas)
    clean_head = strip_non_bmp(headline).upper()
    clean_sub = strip_non_bmp(subtitle).upper()

    max_w = box_w - 64
    head_size = 42
    head_font = load_font(head_size, bold=True)
    while head_size > 22 and measure_text(draw, clean_head, head_font)[0] > max_w:
        head_size -= 2
        head_font = load_font(head_size, bold=True)

    hw, hh = measure_text(draw, clean_head, head_font)
    sub_font = load_font(20, bold=False)
    sw, sh = measure_text(draw, clean_sub, sub_font) if clean_sub else (0, 0)

    if clean_sub:
        total_h = hh + 10 + sh
        head_y = box_y0 + (box_h - total_h) // 2 - 2
        sub_y = head_y + hh + 10
        draw.text(((CANVAS_WIDTH - hw) // 2, head_y), clean_head, font=head_font, fill=(250, 252, 255, 255))
        draw.text(((CANVAS_WIDTH - sw) // 2, sub_y), clean_sub, font=sub_font, fill=(180, 194, 214, 240))
    else:
        head_y = box_y0 + (box_h - hh) // 2 - 2
        draw.text(((CANVAS_WIDTH - hw) // 2, head_y), clean_head, font=head_font, fill=(250, 252, 255, 255))


def _load_background_rgba(bg_path: Path) -> Image.Image:
    if bg_path.is_file():
        with Image.open(bg_path) as bg:
            fitted = ImageOps.fit(bg.convert("RGBA"), (CANVAS_WIDTH, CANVAS_HEIGHT), method=Image.Resampling.LANCZOS)
            return fitted
    return Image.new("RGBA", (CANVAS_WIDTH, CANVAS_HEIGHT), (12, 15, 20, 255))


def _render_stage_card_sync(bg_path: Path, title: str, competition: str, stage: str) -> bytes:
    canvas = _load_background_rgba(bg_path)
    # Slight dark vignette for glass card contrast.
    dim = Image.new("RGBA", canvas.size, (6, 10, 16, 115))
    canvas.alpha_composite(dim)

    card_w, card_h = 840, 320
    x0 = (CANVAS_WIDTH - card_w) // 2
    y0 = (CANVAS_HEIGHT - card_h) // 2
    x1, y1 = x0 + card_w, y0 + card_h

    glass = Image.new("RGBA", canvas.size, (0, 0, 0, 0))
    gdraw = ImageDraw.Draw(glass)
    gdraw.rounded_rectangle(
        (x0, y0, x1, y1),
        radius=26,
        fill=(10, 14, 22, 218),
        outline=(255, 255, 255, 55),
        width=2,
    )
    canvas.alpha_composite(glass)

    draw = ImageDraw.Draw(canvas)
    comp_text = strip_non_bmp(competition or title or "TENNIS").upper()
    sub_text = strip_non_bmp(stage or title).upper()

    comp_font = load_font(44, bold=True)
    while comp_font.size > 24 and measure_text(draw, comp_text, comp_font)[0] > card_w - 80:
        comp_font = load_font(comp_font.size - 2, bold=True)

    cw, ch = measure_text(draw, comp_text, comp_font)
    sub_font = load_font(26, bold=False)
    sw, sh = measure_text(draw, sub_text, sub_font) if sub_text and sub_text != comp_text else (0, 0)

    total_h = ch + (18 + sh if sw else 0)
    cy = y0 + (card_h - total_h) // 2
    draw.text(((CANVAS_WIDTH - cw) // 2, cy), comp_text, font=comp_font, fill=(248, 250, 252, 255))
    if sw:
        draw.text(
            ((CANVAS_WIDTH - sw) // 2, cy + ch + 18),
            sub_text,
            font=sub_font,
            fill=(245, 197, 24, 245),
        )
    return image_to_jpeg_bytes(canvas, quality=90)


def _crop_flag_image(img: Image.Image) -> Image.Image:
    """Trim transparent padding around ESPN flag PNGs and 3px outer anti-aliased edges."""
    rgba = img.convert("RGBA")
    bbox = rgba.getbbox()
    if bbox:
        left, top, right, bottom = bbox
        if (right - left) > 24 and (bottom - top) > 24:
            rgba = rgba.crop((left + 3, top + 3, right - 3, bottom - 3))
        else:
            rgba = rgba.crop(bbox)
    return rgba


def _render_country_tie_sync(
    bg_path: Path,
    left_flag_bytes: Optional[bytes],
    right_flag_bytes: Optional[bytes],
    left_label: str,
    right_label: str,
    headline: str,
    subtitle: str,
) -> bytes:
    canvas = _load_background_rgba(bg_path)
    dim = Image.new("RGBA", canvas.size, (6, 10, 16, 70))
    canvas.alpha_composite(dim)

    flag_w, flag_h = 364, 236
    radius = 24
    y_flag = 165
    left_x = 128
    right_x = CANVAS_WIDTH - 128 - flag_w
    mask = _rounded_mask((flag_w, flag_h), radius)

    def _place_flag(flag_bytes: Optional[bytes], label: str, x: int) -> None:
        # Drop shadow
        shadow = Image.new("RGBA", canvas.size, (0, 0, 0, 0))
        sdraw = ImageDraw.Draw(shadow)
        sdraw.rounded_rectangle(
            (x + 4, y_flag + 10, x + flag_w + 4, y_flag + flag_h + 10),
            radius=radius,
            fill=(0, 0, 0, 150),
        )
        shadow = shadow.filter(ImageFilter.GaussianBlur(radius=12))
        canvas.alpha_composite(shadow)

        if flag_bytes:
            try:
                with Image.open(io.BytesIO(flag_bytes)) as src:
                    trimmed = _crop_flag_image(src)
                    card = ImageOps.fit(
                        trimmed,
                        (flag_w, flag_h),
                        method=Image.Resampling.LANCZOS,
                    )
                    card.putalpha(mask)
                    canvas.alpha_composite(card, dest=(x, y_flag))
                    return
            except Exception:
                pass

        # Fallback country badge if ESPN flag unavailable.
        fallback_card = Image.new("RGBA", (flag_w, flag_h), (18, 24, 36, 235))
        fdraw = ImageDraw.Draw(fallback_card)
        f_font = load_font(44, bold=True)
        txt = strip_non_bmp(label).upper()
        tw, th = measure_text(fdraw, txt, f_font)
        fdraw.text(((flag_w - tw) // 2, (flag_h - th) // 2), txt, font=f_font, fill=(245, 247, 252, 255))
        fallback_card.putalpha(mask)
        canvas.alpha_composite(fallback_card, dest=(x, y_flag))

    _place_flag(left_flag_bytes, left_label, left_x)
    _place_flag(right_flag_bytes, right_label, right_x)

    # Center VS badge
    vs_r = 40
    cx = CANVAS_WIDTH // 2
    cy = y_flag + flag_h // 2
    vs_layer = Image.new("RGBA", canvas.size, (0, 0, 0, 0))
    vdraw = ImageDraw.Draw(vs_layer)
    vdraw.ellipse(
        (cx - vs_r, cy - vs_r, cx + vs_r, cy + vs_r),
        fill=(12, 16, 24, 235),
        outline=(245, 197, 24, 240),
        width=3,
    )
    vs_font = load_font(28, bold=True)
    vw, vh = measure_text(vdraw, "VS", vs_font)
    vdraw.text((cx - vw // 2, cy - vh // 2 - 2), "VS", font=vs_font, fill=(255, 255, 255, 255))
    canvas.alpha_composite(vs_layer)

    _draw_lower_third_banner(canvas, headline, subtitle)
    return image_to_jpeg_bytes(canvas, quality=90)


def _render_singles_sync(
    bg_path: Path,
    left_bytes: bytes,
    right_bytes: bytes,
    left_is_fallback: bool,
    right_is_fallback: bool,
    headline: str,
    subtitle: str,
) -> bytes:
    del left_is_fallback  # Left fallback faces right/inward naturally.
    canvas = _load_background_rgba(bg_path)
    dim = Image.new("RGBA", canvas.size, (6, 10, 16, 55))
    canvas.alpha_composite(dim)

    left_sprite = _scale_cutout_to_height(left_bytes, 510, mirror=False)
    right_sprite = _scale_cutout_to_height(right_bytes, 510, mirror=right_is_fallback)

    bottom_y = 585
    _paste_rgba_centered(canvas, left_sprite, int(CANVAS_WIDTH * 0.27), bottom_y)
    _paste_rgba_centered(canvas, right_sprite, int(CANVAS_WIDTH * 0.73), bottom_y)

    _draw_lower_third_banner(canvas, headline, subtitle)
    return image_to_jpeg_bytes(canvas, quality=90)


def _render_doubles_sync(
    bg_path: Path,
    left_a: bytes,
    left_b: bytes,
    right_a: bytes,
    right_b: bytes,
    left_a_fallback: bool,
    left_b_fallback: bool,
    right_a_fallback: bool,
    right_b_fallback: bool,
    headline: str,
    subtitle: str,
) -> bytes:
    del left_a_fallback, left_b_fallback
    canvas = _load_background_rgba(bg_path)
    dim = Image.new("RGBA", canvas.size, (6, 10, 16, 65))
    canvas.alpha_composite(dim)

    # Depth plane 1 (behind & inward): Partners B at 350px height.
    sprite_lb = _scale_cutout_to_height(left_b, 350, mirror=False)
    sprite_rb = _scale_cutout_to_height(right_b, 350, mirror=right_b_fallback)
    _paste_rgba_centered(canvas, sprite_lb, int(CANVAS_WIDTH * 0.35), 570)
    _paste_rgba_centered(canvas, sprite_rb, int(CANVAS_WIDTH * 0.65), 570)

    # Depth plane 2 (in front & outward): Partners A at 400px height.
    sprite_la = _scale_cutout_to_height(left_a, 400, mirror=False)
    sprite_ra = _scale_cutout_to_height(right_a, 400, mirror=right_a_fallback)
    _paste_rgba_centered(canvas, sprite_la, int(CANVAS_WIDTH * 0.19), 588)
    _paste_rgba_centered(canvas, sprite_ra, int(CANVAS_WIDTH * 0.81), 588)

    _draw_lower_third_banner(canvas, headline, subtitle)
    return image_to_jpeg_bytes(canvas, quality=90)


class TennisPosterService:
    """Generates and disk-caches 1225x700 16:9 Tennis posters."""

    def __init__(
        self,
        settings: Optional[Settings] = None,
        client: Optional[httpx.AsyncClient] = None,
    ) -> None:
        self._settings = settings or get_settings()
        self._client = client
        self._owns_client = client is None

    def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = create_standard_client(
                timeout=15.0,
                headers={"User-Agent": DEFAULT_USER_AGENT},
                follow_redirects=True,
            )
            self._owns_client = True
        return self._client

    async def aclose(self) -> None:
        """Close the owned HTTP client if created internally."""
        if self._owns_client and self._client is not None and not self._client.is_closed:
            await self._client.aclose()
            self._client = None

    async def _download_flag(self, iso2: str) -> Optional[bytes]:
        url = f"https://a.espncdn.com/i/teamlogos/countries/500/{iso2.lower()}.png"
        client = self._get_client()
        try:
            resp = await client.get(url)
            if resp.status_code == 200 and resp.content:
                return resp.content
        except Exception:
            pass
        return None

    async def _run_in_pool(self, fn: Any, *args: Any) -> bytes:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(_PIL_EXECUTOR, fn, *args)

    async def render_match_poster(
        self,
        match: Match,
        db: Optional[DatabaseManager] = None,
    ) -> bytes:
        """Return cached or freshly rendered 1225x700 JPEG bytes for a Tennis match."""
        if not self._settings.ENABLE_TENNIS_POSTERS:
            return await self._run_in_pool(
                render_dark_canvas,
                match.title,
                match.canonical_competition or match.competition,
                CANVAS_WIDTH,
                CANVAS_HEIGHT,
            )

        cache_dir = self._settings.BASE_POSTER_CACHE_DIR
        cache_dir.mkdir(parents=True, exist_ok=True)
        cache_path = cache_dir / f"{safe_match_filename(match.id)}.jpg"
        if cache_path.is_file() and cache_path.stat().st_size > 0:
            return await asyncio.to_thread(cache_path.read_bytes)

        comp_label = match.canonical_competition or match.competition or match.alt_league or "Tennis"
        subtitle = f"{comp_label} • {match.stage}" if match.stage else comp_label
        bg_path = select_tournament_background(
            f"{match.competition} {match.canonical_competition} {match.alt_league}",
            is_women=match.is_women,
            bg_dir=self._settings.TENNIS_BG_DIR,
        )

        # Layout 1: Stage Card (generic / missing players).
        if _is_stage_card(match):
            jpeg_bytes = await self._run_in_pool(
                _render_stage_card_sync,
                bg_path,
                match.title,
                comp_label,
                match.stage,
            )
            await self._atomic_write(cache_path, jpeg_bytes)
            return jpeg_bytes

        # Layout 2: Country Tie (Davis Cup / BJK Cup / United Cup).
        tie_isos = _is_country_tie(match)
        if tie_isos is not None:
            iso_h, iso_a = tie_isos
            left_flag, right_flag = await asyncio.gather(
                self._download_flag(iso_h),
                self._download_flag(iso_a),
            )
            assert match.home_team is not None and match.away_team is not None
            headline = f"{match.home_team.name}  vs  {match.away_team.name}"
            jpeg_bytes = await self._run_in_pool(
                _render_country_tie_sync,
                bg_path,
                left_flag,
                right_flag,
                match.home_team.name,
                match.away_team.name,
                headline,
                subtitle,
            )
            await self._atomic_write(cache_path, jpeg_bytes)
            return jpeg_bytes

        assert match.home_team is not None and match.away_team is not None
        home_name = match.home_team.name
        away_name = match.away_team.name
        client = self._get_client()
        fallback_bytes = load_fallback_silhouette_bytes(
            match.is_women,
            assets_dir=self._settings.TENNIS_ASSETS_DIR,
        )

        # Layout 4: Doubles (detected via `/` or `&` in player names).
        if _DOUBLES_SPLIT_RE.search(home_name) or _DOUBLES_SPLIT_RE.search(away_name):
            home_pair = _split_doubles(home_name)
            away_pair = _split_doubles(away_name)
            while len(home_pair) < 2:
                home_pair.append(home_pair[0] if home_pair else "Player")
            while len(away_pair) < 2:
                away_pair.append(away_pair[0] if away_pair else "Player")

            la, lb, ra, rb = await asyncio.gather(
                fetch_athlete_cutout(home_pair[0], "", match.is_women, db, client, assets_dir=self._settings.TENNIS_ASSETS_DIR),
                fetch_athlete_cutout(home_pair[1], "", match.is_women, db, client, assets_dir=self._settings.TENNIS_ASSETS_DIR),
                fetch_athlete_cutout(away_pair[0], "", match.is_women, db, client, assets_dir=self._settings.TENNIS_ASSETS_DIR),
                fetch_athlete_cutout(away_pair[1], "", match.is_women, db, client, assets_dir=self._settings.TENNIS_ASSETS_DIR),
            )
            left_lbl = f"{_extract_surname(home_pair[0])} / {_extract_surname(home_pair[1])}"
            right_lbl = f"{_extract_surname(away_pair[0])} / {_extract_surname(away_pair[1])}"
            headline = f"{left_lbl}  vs  {right_lbl}"
            jpeg_bytes = await self._run_in_pool(
                _render_doubles_sync,
                bg_path,
                la,
                lb,
                ra,
                rb,
                la == fallback_bytes,
                lb == fallback_bytes,
                ra == fallback_bytes,
                rb == fallback_bytes,
                headline,
                subtitle,
            )
            await self._atomic_write(cache_path, jpeg_bytes)
            return jpeg_bytes

        # Layout 3: Singles.
        left_bytes, right_bytes = await asyncio.gather(
            fetch_athlete_cutout(home_name, match.home_team.slug, match.is_women, db, client, assets_dir=self._settings.TENNIS_ASSETS_DIR),
            fetch_athlete_cutout(away_name, match.away_team.slug, match.is_women, db, client, assets_dir=self._settings.TENNIS_ASSETS_DIR),
        )
        left_sur = _extract_surname(_deabbreviate_player_name(home_name, match.home_team.slug))
        right_sur = _extract_surname(_deabbreviate_player_name(away_name, match.away_team.slug))
        headline = f"{left_sur}  vs  {right_sur}"

        jpeg_bytes = await self._run_in_pool(
            _render_singles_sync,
            bg_path,
            left_bytes,
            right_bytes,
            left_bytes == fallback_bytes,
            right_bytes == fallback_bytes,
            headline,
            subtitle,
        )
        await self._atomic_write(cache_path, jpeg_bytes)
        return jpeg_bytes

    @staticmethod
    async def _atomic_write(path: Path, data: bytes) -> None:
        def _write() -> None:
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_suffix(f".tmp.{os.getpid()}")
            tmp.write_bytes(data)
            os.replace(tmp, path)

        await asyncio.to_thread(_write)
