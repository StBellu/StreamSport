"""Italian linear TV EPG ingestion, filtering, simulcast deduplication, and reconciliation."""

from __future__ import annotations

import asyncio
import logging
import re
import time
import unicodedata
from datetime import datetime, timedelta, timezone, tzinfo
from typing import Any, Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import httpx
from rapidfuzz import fuzz

from app.config import Settings, get_settings
from app.core.network import create_standard_client
from app.models.domain import EPGProgram, Match, SportSilo, TeamInfo
from app.taxonomy import (
    DEFAULT_GENRE_CLASSIFIER,
    DEFAULT_TEAM_MATCHER,
    detect_demographics,
    detect_demographics_from_stream_title,
)
from app.taxonomy.channels import (
    SKY_CHANNEL_IDS,
    TIVUSAT_CHANNEL_IDS,
    canonical_from_sky_id,
    canonical_from_tivusat_id,
    canonicalize_channel_name,
)

logger = logging.getLogger(__name__)

def _last_sunday(year: int, month: int) -> datetime:
    if month == 12:
        nxt = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
    else:
        nxt = datetime(year, month + 1, 1, tzinfo=timezone.utc)
    last_day = nxt - timedelta(days=1)
    offset = (last_day.weekday() + 1) % 7
    return last_day - timedelta(days=offset)


class _EuropeRomeFallback(tzinfo):
    """Exact EU CET/CEST fallback when system tzdata is unavailable (e.g. bare Windows)."""

    def fromutc(self, dt: datetime) -> datetime:
        if dt.tzinfo is not self:
            raise ValueError("dt.tzinfo is not self")
        utc_naive = dt.replace(tzinfo=None)
        start_utc = _last_sunday(utc_naive.year, 3).replace(tzinfo=None, hour=1)
        end_utc = _last_sunday(utc_naive.year, 10).replace(tzinfo=None, hour=1)
        offset = timedelta(hours=2) if start_utc <= utc_naive < end_utc else timedelta(hours=1)
        return (utc_naive + offset).replace(tzinfo=self)

    def utcoffset(self, dt: Optional[datetime]) -> timedelta:
        if dt is None:
            return timedelta(hours=1)
        naive = dt.replace(tzinfo=None)
        start_local = _last_sunday(naive.year, 3).replace(tzinfo=None, hour=2)
        end_local = _last_sunday(naive.year, 10).replace(tzinfo=None, hour=3)
        return timedelta(hours=2) if start_local <= naive < end_local else timedelta(hours=1)

    def dst(self, dt: Optional[datetime]) -> timedelta:
        return timedelta(hours=1) if self.utcoffset(dt) == timedelta(hours=2) else timedelta(0)

    def tzname(self, dt: Optional[datetime]) -> str:
        return "CEST" if self.utcoffset(dt) == timedelta(hours=2) else "CET"


def _load_rome_tz() -> tzinfo:
    try:
        return ZoneInfo("Europe/Rome")
    except ZoneInfoNotFoundError:
        return _EuropeRomeFallback()


_ROME_TZ: tzinfo = _load_rome_tz()
_SKY_MAX_LOOKBACK = timedelta(minutes=165)
_SIMULCAST_WINDOW_MS = 30 * 60 * 1000
_RECONCILE_WINDOW_MS = 60 * 60 * 1000
_LINEAR_PRE_MATCH_MS = 35 * 60 * 1000
_LINEAR_POST_MATCH_MS = 30 * 60 * 1000
_MOTORI_PRE_MATCH_MS = 120 * 60 * 1000
_MOTORI_POST_MATCH_MS = 20 * 60 * 1000
_TENNIS_1V1_PRE_MATCH_MS = 360 * 60 * 1000
_TENNIS_1V1_POST_MATCH_MS = 60 * 60 * 1000
_SKY_DAY_CACHE_TTL_SECONDS = 3 * 3600
_ROLLING_CACHE_WINDOW_MS = int(3.5 * 3600 * 1000)

_GENERALIST_CANONICAL_CHANNELS: frozenset[str] = frozenset(
    {
        "rai 1",
        "rai 2",
        "rai 3",
        "rai 4k",
        "canale 5",
        "italia 1",
        "rete 4",
        "tv8",
        "nove",
        "cielo",
        "20 mediaset",
        "dmax",
    }
)

SKY_EVENTS_URL = "https://apid.sky.it/gtv/v1/events"
TIVUSAT_EPG_URL = "https://services.tivulaguida.it/api/epg/channels/{ch_id}/date/{date_str}"

# Verified against https://apid.sky.it/gtv/v1/channels?env=DTH
# Note: Sky Italia replaced Eurosport 1 & 2 (LCN 210/211) with Sky Sport Legend & Sky Sport Mix.
SKY_SPORTS_CHANNELS: dict[str, str] = {
    "9097": "Sky Sport Uno",
    "9113": "Sky Sport Calcio",
    "11237": "Sky Sport Tennis",
    "9093": "Sky Sport Arena",
    "9116": "Sky Sport Basket",
    "9103": "Sky Sport Max",
    "9096": "Sky Sport F1",
    "9102": "Sky Sport MotoGP",
    "10254": "Sky Sport Golf",
    "11910": "Sky Sport Legend",
    "11909": "Sky Sport Mix",
    "6000": "SuperTennis",
    "10013": "Sky Sport 4K",
    "11402": "DAZN 1",
    "11401": "DAZN 2",
    "11849": "DAZN 3",
    "11850": "DAZN 4",
    "11497": "Sky Sport 251",
    "11496": "Sky Sport 252",
    "11494": "Sky Sport 253",
    "11490": "Sky Sport 254",
    "11491": "Sky Sport 255",
    "11492": "Sky Sport 256",
    "11386": "Sky Sport 257",
    "9046": "Sky Sport 258",
    "8613": "Sky Sport 259",
}

SKY_GENERALIST_CHANNELS: dict[str, str] = {
    "899": "Rai 1",
    "898": "Rai 2",
    "897": "Rai 3",
    "10354": "Canale 5",
    "10454": "Italia 1",
    "8195": "TV8",
    "12116": "NOVE",
    "10458": "20 Mediaset",
    "8133": "Cielo",
    "10464": "Rete 4",
}

SKY_CHANNELS: dict[str, str] = {**SKY_SPORTS_CHANNELS, **SKY_GENERALIST_CHANNELS}
SKY_SPORTS_CHANNEL_IDS: frozenset[str] = frozenset(SKY_SPORTS_CHANNELS.keys())
SKY_GENERALIST_CHANNEL_IDS: frozenset[str] = frozenset(SKY_GENERALIST_CHANNELS.keys())

# Verified against https://services.tivulaguida.it/api/epg/channels
TIVUSAT_SPORTS_CHANNELS: dict[str, str] = {
    "88": "Rai Sport HD",
    "295": "SOLOCALCIO",
    "277": "EQUtv",
}

TIVUSAT_GENERALIST_CHANNELS: dict[str, str] = {
    "42": "Rai 1",
    "43": "Rai 2",
    "44": "Rai 3",
    "279": "TV8",
    "140": "NOVE",
    "280": "Cielo",
    "249": "20 Mediaset",
}

TIVUSAT_CHANNELS: dict[str, str] = {**TIVUSAT_SPORTS_CHANNELS, **TIVUSAT_GENERALIST_CHANNELS}
TIVUSAT_SPORTS_CHANNEL_IDS: frozenset[str] = frozenset(TIVUSAT_SPORTS_CHANNELS.keys())

_NON_LIVE_TITLE_RE = re.compile(
    r"(?:"
    r"\btg\s*(?:sport|24|1|2|3|4|5|com|la7)?\b"
    r"|\bsport\s*24\b"
    r"|\bpaddock\s+live\b"
    r"|\bcalciomercato\b"
    r"|\brassegna\s+stampa\b"
    r"|\bstudio\b"
    r"|\bspeciale\b"
    r"|\bhighlights\b"
    r"|\bsintesi\b"
    r"|\breplica\b"
    r"|\bon\s*board\b"
    r"|\brubrica\b"
    r"|\bpre\s*-?\s*partita\b"
    r"|\bpost\s*-?\s*partita\b"
    r"|\bsky\s+calcio\s+club\b"
    r"|\b(?:la\s+)?domenica\s+sportiva\b"
    r"|\b90[°o]\s*minuto\b"
    r"|\bsabato\s+al\s+90[°o]\b"
    r"|\bpressing\b"
    r"|\btiki\s+taka\b"
    r"|\brace\s+anatomy\b"
    r"|\bafter\s+race\b"
    r"|\bl['’]originale\b"
    r"|\bsupertele\b"
    r"|\banteprima\b"
    r"|\bmagazine\b"
    r"|\bdocumentario\b"
    r"|\brewind\b"
    r"|\brivedi\b"
    r"|\bbest\s+of\b"
    r"|\bfederico\s+buffa\b"
    r"|\bicarus\b"
    r"|\bbasket\s+room\b"
    r"|\bsky\s+sport\s+tech\b"
    r"|\bnessun\s+evento\b"
    r"|^\s*ep\.?\s*\d+\b"
    r"|\bdifferita\b"
    r"|\barchivio\b"
    r"|\bstory\b"
    r"|\blegends\b"
    r"|\bdoc\b"
    r"|\bintervista\b"
    r"|\bpodcast\b"
    r"|[\(\[]\s*r\s*[\)\]]"
    r")",
    re.IGNORECASE,
)

_NON_LIVE_DESC_RE = re.compile(
    r"(?:"
    r"\bin\s+replica\b"
    r"|\brubrica\b"
    r"|\bnotiziario\b"
    r"|\brassegna\s+stampa\b"
    r"|\bapprofondimento\b"
    r"|\btalk\s+show\b"
    r"|\bazioni\s+salienti\b"
    r"|\bhighlights\b"
    r"|\bsintesi\b"
    r"|\bcalciomercato\b"
    r"|\bpre\s*-?\s*partita\b"
    r"|\bpost\s*-?\s*partita\b"
    r"|\btrasmissione\s+dedicata\b"
    r"|\bdifferita\b"
    r"|\barchivio\b"
    r"|\bdocumentario\b"
    r"|\bintervista\b"
    r"|\bpodcast\b"
    r"|\bspeciale\b"
    r"|\bbest\s+of\b"
    r")",
    re.IGNORECASE,
)

_TENNIS_IDENT_RE = re.compile(
    r"\b(?:tennis|atp|wta|challenger|wimbledon|roland\s+garros|us\s+open|"
    r"australian\s+open|grande\s+slam|grand\s+slam|davis\s+cup|coppa\s+davis|"
    r"bjk\s*cup|billie\s+jean\s+king|united\s+cup|laver\s+cup|supertennis)\b",
    re.IGNORECASE,
)

_GENERIC_LIVE_TENNIS_RE = re.compile(
    r"(?:"
    r"\b(?:diretta|live|in\s+diretta|multicampo)\b.*\b(?:tennis|atp|wta|slam|grande\s+slam|supertennis)\b"
    r"|\b(?:tennis|atp\s+tour|wta\s+tour|supertennis|grande\s+slam)\b.*\b(?:diretta|live|in\s+diretta|multicampo)\b"
    r"|^\s*(?:atp\s+tour|wta\s+tour|grande\s+slam|tennis\s+live|supertennis\s+live)\s*$"
    r")",
    re.IGNORECASE,
)

_SPORTS_EVENT_HINT_RE = re.compile(
    r"(?:"
    r"\b(?:serie\s*[abc]|coppa\s+italia|supercoppa|champions\s+league|europa\s+league|"
    r"conference\s+league|nations\s+league|world\s+cup|mondiali|euro\s*\d{4}|"
    r"premier\s+league|la\s*liga|bundesliga|ligue\s*1|calcio)\b"
    r"|\b(?:formula\s*1|\bf1\b|motogp|moto\s*2|moto\s*3|superbike|wsbk|gran\s+premio|\bgp\b)\b"
    r"|\b(?:tennis|atp|wta|wimbledon|roland\s+garros|australian\s+open|us\s+open|coppa\s+davis|bjk\s*cup)\b"
    r"|\b(?:basket|nba|eurolega|euroleague|lba|pallacanestro)\b"
    r"|\b(?:volley|pallavolo|superlega|rugby|sei\s+nazioni|six\s+nations|ciclismo|"
    r"giro\s+d['’]italia|tour\s+de\s+france|olimpiadi|atletica|boxe|ufc|mma|nfl|nhl|mlb|pallamano)\b"
    r")",
    re.IGNORECASE,
)

_SIMULCAST_NOISE_RE = re.compile(
    r"\b(?:diretta|live|in\s+diretta|hd|4k|uhd|replica|sky\s+sport|tv8|cielo|rai\s*[123]|"
    r"canale\s*5|italia\s*1|nove|20\s*mediaset)\b",
    re.IGNORECASE,
)

_YEAR_RE = re.compile(r"\b(19\d{2}|20\d{2})\b")
_CLUB_YEAR_PREFIX_RE = re.compile(
    r"\b(?:fc|tsg|sv|tsv|vfb|vfl|sc|ac|as|us|ss|ssv|sg|fk|bk|if|nk|gnk|cd|ud|sd|cf|asd|usd|ssd|real|club)\s*$",
    re.IGNORECASE,
)
_TEAM_SPLIT_RE = re.compile(r"\s+(?:vs\.?|v\.?|-|–|—)\s+", re.IGNORECASE)
_ROUND_PHASE_SUFFIX_RE = re.compile(
    r"(?:\s*[-–—,|]\s*|\s*[\(\[]\s*)"
    r"(?:"
    r"\d+[aª°\^]?\s*giornata"
    r"|giornata\s*\d+"
    r"|quarti\s+di\s+finale|quarto\s+di\s+finale"
    r"|ottavi\s+di\s+finale|sedicesimi\s+di\s+finale"
    r"|semifinal[ei]|final[ei]?"
    r"|play\s*-?\s*(?:off|out|in)s?"
    r"|gara\s*\d+|game\s*\d+|match\s*\d+"
    r"|andata|ritorno"
    r"|turno\s+preliminare|fase\s+a\s+gironi"
    r"|round\s+of\s+\d+|quarter\s*-?\s*finals?|semi\s*-?\s*finals?"
    r")"
    r"(?:\s*[\)\]])?\s*$",
    re.IGNORECASE,
)
_MOTORI_PREFIX_RE = re.compile(
    r"^(formula\s*1|f1|motogp|moto\s*2|moto\s*3|wrc|wsbk|sbk|superbike|indycar|nascar)\b\s*[:\-–—]?\s*(.*)$",
    re.IGNORECASE,
)
_COMPOUND_HYPHEN_PREFIXES: frozenset[str] = frozenset(
    {
        "saint",
        "st",
        "al",
        "el",
        "ben",
        "abu",
        "ibn",
        "san",
        "santa",
        "sao",
        "under",
        "u",
        "sub",
        "play",
        "all",
        "pre",
        "post",
        "ex",
        "de",
        "del",
        "la",
        "le",
        "mac",
        "mc",
    }
)
_KNOWN_COMPOUND_HYPHEN_WORDS: frozenset[str] = frozenset(
    {
        "auger-aliassime",
        "bautista-agut",
        "carreno-busta",
        "davidovich-fokina",
        "mpetshi-perricard",
        "clermont-ferrand",
        "emilia-romagna",
        "friuli-venezia",
        "trentino-alto",
        "saint-etienne",
        "saint-germain",
        "west-ham",
    }
)

_SIMULCAST_TITLE_RE = re.compile(
    r"\b(?:diretta\s+gol|diretta\s+goal|zona\s+gol|zona\s+serie\s*[ab]|zona\s+dazn|mosaico|red\s*zone)\b",
    re.IGNORECASE,
)
_EPISODIC_SERIES_RE = re.compile(r"\bS\d+\s*Ep\d+\b", re.IGNORECASE)
_GENERALIST_STUDIO_DESC_RE = re.compile(
    r"\b(?:conduce|notiziario|interviste|anticipazioni|rassegna\s+stampa)\b",
    re.IGNORECASE,
)
_UUID_RE = re.compile(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}")
_TENNIS_EXCLUDED_TOKENS: frozenset[str] = frozenset(
    {
        "men", "women", "singles", "doubles", "singolare", "doppio", "live", "tour",
        "2025", "2026", "2027", "masters", "finals", "open", "quarti", "semifinali",
        "semifinal", "finale", "giornata", "sessione", "day", "night", "court",
        "campo", "round", "turno", "match", "diretta", "tennis", "atp", "wta",
    }
)
_KNOWN_TENNIS_CITIES: tuple[str, ...] = (
    "roma", "rome", "parigi", "paris", "londra", "london", "wimbledon", "madrid",
    "barcellona", "barcelona", "montecarlo", "monte carlo", "torino", "turin",
    "milano", "milan", "miami", "indian wells", "cincinnati", "toronto", "montreal",
    "new york", "melbourne", "sydney", "tokyo", "pechino", "beijing", "shanghai",
    "wuhan", "doha", "dubai", "basilea", "basel", "vienna", "halle", "queens",
    "eastbourne", "berlino", "berlin", "stuttgart", "rotterdam", "marsiglia",
    "marseille", "malaga", "bologna", "riyadh",
)

_COUNTRY_TEAM_MAP: dict[str, str] = {
    "norvegia": "Norway",
    "portogallo": "Portugal",
    "lituania": "Lithuania",
    "azerbaigian": "Azerbaijan",
    "spagna": "Spain",
    "francia": "France",
    "germania": "Germany",
    "inghilterra": "England",
    "italia": "Italy",
    "olanda": "Netherlands",
    "paesi bassi": "Netherlands",
    "croazia": "Croatia",
    "serbia": "Serbia",
    "belgio": "Belgium",
    "svizzera": "Switzerland",
    "austria": "Austria",
    "svezia": "Sweden",
    "danimarca": "Denmark",
    "polonia": "Poland",
    "repubblica ceca": "Czechia",
    "cechia": "Czechia",
    "turchia": "Turkey",
    "grecia": "Greece",
    "brasile": "Brazil",
    "argentina": "Argentina",
    "stati uniti": "USA",
    "ucraina": "Ukraine",
    "irlanda": "Ireland",
    "scozia": "Scotland",
    "galles": "Wales",
    "romania": "Romania",
    "bulgaria": "Bulgaria",
    "ungheria": "Hungary",
    "slovacchia": "Slovakia",
    "slovenia": "Slovenia",
    "finlandia": "Finland",
    "islanda": "Iceland",
    "israele": "Israel",
    "albania": "Albania",
    "macedonia del nord": "North Macedonia",
    "macedonia": "North Macedonia",
    "bosnia": "Bosnia and Herzegovina",
    "bosnia ed erzegovina": "Bosnia and Herzegovina",
    "montenegro": "Montenegro",
    "georgia": "Georgia",
    "cipro": "Cyprus",
    "malta": "Malta",
    "armenia": "Armenia",
    "moldavia": "Moldova",
    "kazakistan": "Kazakhstan",
    "lussemburgo": "Luxembourg",
    "bielorussia": "Belarus",
    "kosovo": "Kosovo",
    "estonia": "Estonia",
    "lettonia": "Latvia",
    "far oer": "Faroe Islands",
    "isole far oer": "Faroe Islands",
    "gibilterra": "Gibraltar",
    "liechtenstein": "Liechtenstein",
    "san marino": "San Marino",
    "andorra": "Andorra",
}

_SKY_SLUG_COMP_MAP: dict[str, str] = {
    "serie-a": "Serie A",
    "serie-b": "Serie B",
    "serie-c": "Serie C",
    "premier-league": "Premier League",
    "bundesliga": "Bundesliga",
    "bundesliga-2": "2. Bundesliga",
    "2-bundesliga": "2. Bundesliga",
    "uefa-champions-league": "UEFA Champions League",
    "champions-league": "UEFA Champions League",
    "uefa-europa-league": "UEFA Europa League",
    "europa-league": "UEFA Europa League",
    "uefa-conference-league": "UEFA Conference League",
    "conference-league": "UEFA Conference League",
    "uefa-nations-league": "UEFA Nations League",
    "nations-league": "UEFA Nations League",
    "coppa-italia": "Coppa Italia",
    "supercoppa-italiana": "Supercoppa Italiana",
    "fa-cup": "FA Cup",
    "carabao-cup": "Carabao Cup",
    "efl-cup": "Carabao Cup",
    "la-liga": "LaLiga",
    "laliga": "LaLiga",
    "ligue-1": "Ligue 1",
    "copa-libertadores": "Copa Libertadores",
    "copa-sudamericana": "Copa Sudamericana",
    "fifa-world-cup": "FIFA World Cup",
    "tennis-wimbledon": "Wimbledon",
    "tennis-roland-garros": "Roland Garros",
    "tennis-us-open": "US Open",
    "tennis-australian-open": "Australian Open",
    "tennis-atp-finals": "ATP Finals",
    "tennis-wta-finals": "WTA Finals",
    "tennis-coppa-davis": "Coppa Davis",
    "tennis-davis-cup": "Coppa Davis",
    "tennis-billie-jean-king-cup": "BJK Cup",
    "tennis-bjk-cup": "BJK Cup",
    "tennis-laver-cup": "Laver Cup",
    "tennis-united-cup": "United Cup",
    "f1": "Formula 1",
    "formula-1": "Formula 1",
    "formula1": "Formula 1",
    "f2": "Formula 2",
    "formula-2": "Formula 2",
    "f3": "Formula 3",
    "formula-3": "Formula 3",
    "motogp": "MotoGP",
    "moto-gp": "MotoGP",
    "moto2": "Moto2",
    "moto3": "Moto3",
    "superbike": "Superbike",
    "worldsbk": "Superbike",
    "sbk": "Superbike",
    "indycar": "IndyCar",
    "nascar": "NASCAR",
    "wec": "WEC",
    "wrc": "WRC",
    "eurolega": "Eurolega",
    "euroleague": "Eurolega",
    "eurocup": "Eurocup",
    "nba": "NBA",
    "wnba": "WNBA",
    "serie-a-basket": "LBA Serie A",
    "basket-serie-a": "LBA Serie A",
    "lba": "LBA Serie A",
    "serie-a2-basket": "Serie A2 Basket",
    "bcl": "BCL",
    "fiba": "FIBA",
    "superlega": "Superlega",
    "superlega-credembanca": "Superlega",
    "volley-superlega": "Superlega",
    "serie-a1-femminile": "Serie A1 Femminile",
    "volley-femminile": "Serie A1 Femminile",
    "cev-champions-league": "CEV Champions League",
    "six-nations": "Sei Nazioni",
    "sei-nazioni": "Sei Nazioni",
    "rugby-serie-a-elite": "Serie A Elite Rugby",
    "rugby-championship": "Rugby Championship",
    "pga-tour": "PGA Tour",
    "dp-world-tour": "DP World Tour",
    "ryder-cup": "Ryder Cup",
}

_MAGAZINE_SLUG_RE = re.compile(
    r"(?:\b1-vs-1\b|-remix\b|\bremix\b|-magazine\b|\bmagazine\b|-speciale?\b|\bspeciale\b|"
    r"-preview\b|\bpreview\b|\banteprima\b|-talk\b|\btalk\b|-stor(?:y|ies)\b|boat-show|"
    r"this-is-padel|atp-tour-this-week|paddock-live|conf(?:erenza)?-stampa|talent-time|"
    r"\b90-10\b|la-giovane-italia|\brubrica\b|matchplan|mondo-gol|calcio-show|calciomercato|"
    r"/replica\b|/differita\b)",
    re.IGNORECASE,
)


def _strip_accents(text: str) -> str:
    norm = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in norm if not unicodedata.combining(ch))


def _ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _is_generalist_channel(channel_name: str, channel_id: str = "") -> bool:
    if channel_id and (channel_id in SKY_GENERALIST_CHANNEL_IDS or channel_id in TIVUSAT_GENERALIST_CHANNELS):
        return True
    canon = canonicalize_channel_name(channel_name, country="Italy")
    return canon in _GENERALIST_CANONICAL_CHANNELS


def _is_simulcast_title(title: str) -> bool:
    return bool(_SIMULCAST_TITLE_RE.search(_strip_accents(title or "")))


def _parse_sky_semantic_url(content_url: str) -> tuple[Optional[str], bool]:
    """Parse Sky Italia semantic URL `/(sport|programmi-tv)/{comp_slug}/{event_slug}`."""
    if not content_url or not isinstance(content_url, str):
        return None, False

    m = re.search(r"/(sport|programmi-tv)/([^/]+)/([^/]+)", content_url)
    if not m:
        return None, False

    section = m.group(1).lower()
    comp_slug = m.group(2).lower()
    event_slug = m.group(3).lower()
    slug_combined = f"{comp_slug} {event_slug}"
    is_magazine = bool(_MAGAZINE_SLUG_RE.search(slug_combined))

    if section == "programmi-tv" and not any(
        k in comp_slug for k in ("sport", "calcio", "tennis", "f1", "motogp", "basket", "volley")
    ):
        is_magazine = True

    if comp_slug in _SKY_SLUG_COMP_MAP:
        return _SKY_SLUG_COMP_MAP[comp_slug], is_magazine
    if comp_slug == "tennis-wta":
        return "WTA", is_magazine
    if comp_slug == "tennis-atp":
        return "ATP", is_magazine
    if comp_slug == "tennis-challenger":
        return "Challenger", is_magazine

    m_tennis = re.match(
        r"^tennis-(wta|atp|challenger)(?:-masters)?(?:-(\d{3,4}))?-([a-z0-9-]+)$",
        comp_slug,
    )
    if m_tennis:
        circuit = m_tennis.group(1).upper()
        tier = m_tennis.group(2)
        raw_city = m_tennis.group(3)
        has_masters = "masters" in comp_slug
        city = " ".join(
            w.capitalize() for w in raw_city.split("-") if w not in ("atp", "wta", "men", "women")
        )
        if circuit == "ATP" and (has_masters or tier == "1000"):
            return f"ATP Masters 1000 {city}".strip(), is_magazine
        if tier:
            return f"{circuit} {tier} {city}".strip(), is_magazine
        return f"{circuit} {city}".strip(), is_magazine

    if "padel" in comp_slug:
        clean_p = " ".join(w.capitalize() for w in comp_slug.replace("padel-", "").split("-"))
        return f"Padel {clean_p}".strip(), is_magazine

    clean_name = " ".join(
        w.capitalize() for w in comp_slug.split("-") if w not in ("live", "diretta", "sport")
    )
    if clean_name and len(clean_name) >= 3:
        return clean_name, is_magazine

    return None, is_magazine


def _extract_competition_from_synopsis(synopsis: str) -> Optional[str]:
    """Extract competition hint from EPG synopsis when title/URL lacks it."""
    if not synopsis or not isinstance(synopsis, str):
        return None
    s = synopsis.strip()

    m_ten = re.search(r"\b(WTA|ATP)\s*(\d{3,4})?\b", s, re.I)
    if m_ten:
        circ = m_ten.group(1).upper()
        tier = m_ten.group(2)
        all_cities = re.findall(r"\b(?:di|da)\s+([A-Z][a-z]+)", s)
        city = ""
        for c in all_cities:
            if c not in (
                "Tennis", "Diretta", "Giappone", "Austria", "Francia",
                "Spagna", "Italia", "Cina", "Germania",
            ):
                city = c
                break
        if not city:
            s_low = s.lower()
            for kc in _KNOWN_TENNIS_CITIES:
                if re.search(rf"\b{re.escape(kc)}\b", s_low):
                    city = kc.title()
                    break
        if tier and city:
            return f"{circ} {tier} {city}"
        if tier:
            return f"{circ} {tier}"
        if city:
            return f"{circ} {city}"
        return circ

    m_slam = re.search(
        r"\b(Wimbledon|Roland Garros|US Open|Australian Open|Coppa Davis|BJK Cup)\b",
        s,
        re.I,
    )
    if m_slam:
        return m_slam.group(1)

    m_gp = re.search(
        r"\b(?:Gran Premio|GP)\s+(?:del\s+|della\s+|di\s+)?([A-Za-z\s]+?)"
        r"(?:\s+di\s+(?:F1|Formula\s*1|MotoGP)|\s*[,.]|$)",
        s,
        re.I,
    )
    if m_gp and any(k in s.lower() for k in ("f1", "formula 1", "motogp")):
        sport = "Formula 1" if ("f1" in s.lower() or "formula" in s.lower()) else "MotoGP"
        return f"{sport}: GP {m_gp.group(1).strip()}"

    m_league = re.search(
        r"\b(Serie A|Serie B|Serie C|Premier League|Bundesliga|UEFA Champions League|"
        r"Champions League|UEFA Europa League|Europa League|Conference League|Coppa Italia|"
        r"Eurolega|Eurocup|Superlega)\b",
        s,
        re.I,
    )
    if m_league:
        l_name = m_league.group(1).title()
        if "Champions League" in l_name:
            return "UEFA Champions League"
        if "Europa League" in l_name:
            return "UEFA Europa League"
        return l_name

    return None


def _extract_sky_poster_url(content: dict[str, Any]) -> str:
    """Extract valid Sky 16:9 poster URL from `content.imagesMap`, skipping generic placeholders."""
    images_map = content.get("imagesMap")
    if not isinstance(images_map, list):
        return ""
    img_dict: dict[str, str] = {}
    for item in images_map:
        if not isinstance(item, dict):
            continue
        key = item.get("key")
        url = (item.get("img") or {}).get("url")
        if isinstance(key, str) and isinstance(url, str) and _UUID_RE.search(url):
            img_dict[key] = url
    chosen = (
        img_dict.get("scene_key_art")
        or img_dict.get("scene")
        or img_dict.get("cover_clean")
        or img_dict.get("background")
        or img_dict.get("cover")
        or ""
    )
    if not chosen:
        return ""
    return f"https://guidatv.sky.it{chosen}" if chosen.startswith("/") else chosen


def _passes_epg_program_guards(
    *,
    is_generalist: bool,
    title: str,
    description: str,
    start_ms: int,
    end_ms: int,
    competition_hint: str = "",
) -> bool:
    """Apply shared generalist duration/studio and episodic documentary guards."""
    if _EPISODIC_SERIES_RE.search(f"{title} {description}"):
        return False
    if is_generalist:
        duration_min = (end_ms - start_ms) / 60000.0
        if 0 < duration_min < 25:
            return False
        if description and _GENERALIST_STUDIO_DESC_RE.search(_strip_accents(description)):
            return False
        combined_hint = _strip_accents(f"{title} {competition_hint}")
        if not _SPORTS_EVENT_HINT_RE.search(combined_hint) and not _SIMULCAST_TITLE_RE.search(combined_hint):
            return False
    return True


def _merge_and_handover_channel_blocks(programs: list[EPGProgram]) -> list[EPGProgram]:
    """Merge contiguous non-1v1 blocks on the same channel and clamp overlapping end_ms."""
    if not programs:
        return []

    by_channel: dict[tuple[str, str], list[EPGProgram]] = {}
    for prog in programs:
        by_channel.setdefault((prog.source, prog.channel_id), []).append(prog)

    result: list[EPGProgram] = []
    for ch_progs in by_channel.values():
        ordered = sorted(ch_progs, key=lambda p: p.start_ms)
        merged: list[EPGProgram] = []
        for curr in ordered:
            if merged:
                prev = merged[-1]
                _, prev_home, prev_away = _extract_epg_teams(prev.title)
                _, curr_home, curr_away = _extract_epg_teams(curr.title)
                is_1v1_pair = bool(prev_home and prev_away) or bool(curr_home and curr_away)
                if (
                    not is_1v1_pair
                    and prev.title.strip().casefold() == curr.title.strip().casefold()
                    and 0 <= (curr.start_ms - prev.end_ms) <= 25 * 60_000
                ):
                    merged[-1] = prev.model_copy(
                        update={
                            "end_ms": max(prev.end_ms, curr.end_ms),
                            "description": prev.description or curr.description,
                            "competition_hint": prev.competition_hint or curr.competition_hint,
                            "poster_url": prev.poster_url or curr.poster_url,
                            "is_live": prev.is_live or curr.is_live,
                            "is_simulcast": prev.is_simulcast or curr.is_simulcast,
                        }
                    )
                    continue
            merged.append(curr)

        # Sequential Channel Handover: clamp overlapping end_ms to next program's start_ms
        for i in range(len(merged) - 1):
            curr = merged[i]
            nxt = merged[i + 1]
            if curr.end_ms > nxt.start_ms > curr.start_ms:
                merged[i] = curr.model_copy(update={"end_ms": nxt.start_ms})

        result.extend(merged)

    result.sort(key=lambda p: (p.start_ms, p.channel_name, p.id))
    return result


def _translate_country_team(name: str) -> str:
    clean = _strip_accents(name or "").strip().lower()
    return _COUNTRY_TEAM_MAP.get(clean, "")


def _is_tennis_doubles(text: str) -> bool:
    return bool(re.search(r"\b(?:doubles?|doppi[oa]?)\b", text or "", re.IGNORECASE))


def _extract_tennis_circuit_and_city(text: str) -> tuple[Optional[str], Optional[str]]:
    """Extract tennis circuit (`atp`, `wta`, `challenger`, `slam`) and city from text."""
    t = _strip_accents((text or "").lower())
    circuit: Optional[str] = None
    if re.search(r"\b(?:wta|wta\s*1000|wta\s*500|wta\s*250|wta\s*125)\b", t):
        circuit = "wta"
    elif re.search(r"\bchallenger\b", t):
        circuit = "challenger"
    elif re.search(r"\b(?:wimbledon|roland\s*garros|us\s*open|australian\s*open)\b", t):
        circuit = "slam"
    elif re.search(r"\b(?:atp|masters\s*1000|atp\s*500|atp\s*250|atp\s*finals|masters)\b", t):
        circuit = "atp"

    city: Optional[str] = None
    m_city = re.search(r"(?:atp|wta|challenger)[^:]*?:\s*([a-z]+)", t)
    if m_city and m_city.group(1).strip() not in _TENNIS_EXCLUDED_TOKENS:
        city = m_city.group(1).strip()

    if not city:
        m_city2 = re.search(
            r"\b(?:atp|wta|challenger)(?:\s+(?:masters|finals|\d{3,4}|series|tour|singles|doubles|doppio|singolare|men|women|uomini|donne))*\s+([a-z]+)",
            t,
        )
        if m_city2 and m_city2.group(1).strip() not in _TENNIS_EXCLUDED_TOKENS:
            city = m_city2.group(1).strip()

    if not city:
        m_city3 = re.search(r"\b(?:di|da)\s+([a-z]+),\s*[a-z]+", t)
        if m_city3 and m_city3.group(1).strip() not in _TENNIS_EXCLUDED_TOKENS:
            city = m_city3.group(1).strip()

    if not city:
        for kc in _KNOWN_TENNIS_CITIES:
            if re.search(rf"\b{re.escape(kc)}\b", t):
                city = kc
                break

    if not city and "internazionali bnl" in t:
        city = "roma"

    return circuit, city


def _is_supertennis_program(epg: EPGProgram) -> bool:
    if epg.channel_id == "6000":
        return True
    return "supertennis" in (epg.channel_name or "").lower()


def clamp_sky_from(requested_from: datetime, now_utc: datetime) -> datetime:
    """Clamp Sky API `from` parameter to at most 165 minutes in the past (prevents HTTP 422)."""
    req = _ensure_utc(requested_from)
    now = _ensure_utc(now_utc)
    min_allowed = now - _SKY_MAX_LOOKBACK
    return req if req >= min_allowed else min_allowed


def _has_historical_year(text: str, ref_year: int) -> bool:
    for match in _YEAR_RE.finditer(text):
        year_val = int(match.group(1))
        if 1960 <= year_val < ref_year:
            prefix = text[: match.start()].rstrip()
            if _CLUB_YEAR_PREFIX_RE.search(prefix):
                continue
            return True
    return False


def is_non_live_or_studio(
    title: str,
    description: str = "",
    *,
    current_year: Optional[int] = None,
) -> bool:
    """Return True if the EPG entry is a studio show, news, magazine, highlights, or replay."""
    clean_title = _strip_accents((title or "").strip())
    if not clean_title:
        return True
    if _NON_LIVE_TITLE_RE.search(clean_title):
        return True
    ref_year = current_year if current_year is not None else datetime.now(timezone.utc).year
    if _has_historical_year(clean_title, ref_year):
        return True
    clean_desc = _strip_accents((description or "").strip())
    if clean_desc and _NON_LIVE_DESC_RE.search(clean_desc):
        return True
    return False


def is_out_of_calendar_tennis(
    title: str,
    sport_hint: str,
    active_tennis_cities: frozenset[str],
    *,
    current_year: Optional[int] = None,
) -> bool:
    """Return True if a tennis EPG entry references a non-active tournament (archive replay)."""
    if not active_tennis_cities:
        return False

    hint_norm = _strip_accents((sport_hint or "").lower())
    title_norm = _strip_accents((title or "").strip())
    if "tennis" not in hint_norm and not _TENNIS_IDENT_RE.search(title_norm):
        return False

    ref_year = current_year if current_year is not None else datetime.now(timezone.utc).year
    for year_str in _YEAR_RE.findall(title_norm):
        year_val = int(year_str)
        if 1970 <= year_val < ref_year:
            return True

    title_low = re.sub(r"\s+", " ", title_norm.lower())
    for city in active_tennis_cities:
        city_clean = _strip_accents(city).lower().strip()
        if not city_clean:
            continue
        if re.search(rf"(?<!\w){re.escape(city_clean)}(?!\w)", title_low):
            return False

    if _GENERIC_LIVE_TENNIS_RE.search(title_norm):
        return False

    return True


def _normalize_simulcast_title(title: str) -> str:
    clean = _strip_accents((title or "").lower())
    clean = _SIMULCAST_NOISE_RE.sub(" ", clean)
    clean = re.sub(r"[^\w\s]", " ", clean)
    return re.sub(r"\s+", " ", clean).strip()


def _split_channel_names(channel_name: str) -> list[str]:
    parts: list[str] = []
    seen: set[str] = set()
    for raw in (channel_name or "").split(","):
        cleaned = raw.strip()
        if cleaned:
            key = cleaned.casefold()
            if key not in seen:
                seen.add(key)
                parts.append(cleaned)
    return parts


def _are_simulcasts(a: EPGProgram, b: EPGProgram) -> bool:
    if abs(a.start_ms - b.start_ms) > _SIMULCAST_WINDOW_MS:
        return False

    hint_a = _strip_accents(a.sport_hint).lower().strip()
    hint_b = _strip_accents(b.sport_hint).lower().strip()
    if hint_a and hint_b and hint_a != hint_b:
        if hint_a not in hint_b and hint_b not in hint_a:
            return False

    norm_a = _normalize_simulcast_title(a.title)
    norm_b = _normalize_simulcast_title(b.title)
    if not norm_a or not norm_b:
        return False
    if norm_a == norm_b:
        return True

    min_len = min(len(norm_a), len(norm_b))
    if min_len < 6:
        return False

    sort_score = fuzz.token_sort_ratio(norm_a, norm_b)
    if sort_score >= 86:
        return True
    if min_len >= 10 and fuzz.token_set_ratio(norm_a, norm_b) >= 92:
        return True
    return False


def deduplicate_simulcasts(programs: list[EPGProgram]) -> list[EPGProgram]:
    """Merge identical broadcasts across channels within 30m, expanding window and channels."""
    if not programs:
        return []

    ordered = sorted(programs, key=lambda p: (p.start_ms, p.source != "sky", p.channel_name))
    groups: list[list[EPGProgram]] = []

    for prog in ordered:
        matched_group: Optional[list[EPGProgram]] = None
        for group in groups:
            if _are_simulcasts(group[0], prog):
                matched_group = group
                break
        if matched_group is not None:
            matched_group.append(prog)
        else:
            groups.append([prog])

    merged: list[EPGProgram] = []
    for group in groups:
        primary = group[0]
        channels: list[str] = []
        seen_channels: set[str] = set()
        for item in group:
            for ch in _split_channel_names(item.channel_name):
                key = ch.casefold()
                if key not in seen_channels:
                    seen_channels.add(key)
                    channels.append(ch)

        best_desc = max((g.description for g in group if g.description), key=len, default="")
        best_cat = next((g.category for g in group if g.category), "")
        best_hint = next((g.sport_hint for g in group if g.sport_hint), "")
        best_comp = next((g.competition_hint for g in group if g.competition_hint), "")
        best_poster = next((g.poster_url for g in group if g.poster_url), "")
        merged.append(
            primary.model_copy(
                update={
                    "channel_name": ", ".join(channels),
                    "start_ms": min(g.start_ms for g in group),
                    "end_ms": max(g.end_ms for g in group),
                    "description": best_desc,
                    "category": best_cat,
                    "sport_hint": best_hint,
                    "competition_hint": best_comp,
                    "poster_url": best_poster,
                    "is_live": any(g.is_live for g in group),
                    "is_simulcast": any(g.is_simulcast for g in group),
                }
            )
        )
    return merged


def _slugify(text: str, max_len: int = 48) -> str:
    clean = _strip_accents((text or "").lower())
    slug = re.sub(r"[^a-z0-9]+", "-", clean).strip("-")
    if len(slug) > max_len:
        slug = slug[:max_len].rstrip("-")
    return slug or "event"


def _infer_silo_from_epg(epg: EPGProgram) -> SportSilo:
    combined = _strip_accents(
        f"{epg.competition_hint} {epg.sport_hint} {epg.category} {epg.channel_name} {epg.title}"
    ).lower()

    if any(k in combined for k in ("formula 1", "f1", "motogp", "moto 2", "moto 3", "superbike", "motori", "automobilismo", "motociclismo")):
        return SportSilo.MOTORI
    if any(k in combined for k in ("tennis", "supertennis", "atp", "wta", "wimbledon", "roland garros")):
        return SportSilo.TENNIS
    if any(k in combined for k in ("basket", "nba", "wnba", "eurolega", "euroleague", "pallacanestro")):
        return SportSilo.BASKET
    if any(k in combined for k in ("ufc", "mma", "boxe", "boxing", "pugilato", "wrestling", "wwe", "aew")):
        return SportSilo.COMBAT
    if any(k in combined for k in ("volley", "pallavolo", "superlega")):
        return SportSilo.VOLLEY
    if any(k in combined for k in ("nfl", "redzone", "red zone", "football americano")):
        return SportSilo.NFL
    if any(k in combined for k in ("hockey", "nhl")):
        return SportSilo.HOCKEY
    if any(k in combined for k in ("baseball", "mlb")):
        return SportSilo.BASEBALL
    if any(k in combined for k in ("rugby", "sei nazioni", "six nations")):
        return SportSilo.RUGBY
    if any(k in combined for k in ("pallamano", "handball")):
        return SportSilo.HANDBALL
    if any(k in combined for k in ("golf", "ciclismo", "cycling", "sci", "biathlon", "atletica", "equtv", "ippica", "padel", "snooker", "olimpiadi")):
        return SportSilo.ALTRI_SPORT
    if any(k in combined for k in ("serie a", "serie b", "serie c", "coppa italia", "zona dazn", "solocalcio")):
        return SportSilo.CALCIO_ITA
    if any(k in combined for k in ("calcio", "football", "soccer", "champions", "europa league", "conference league", "premier league", "bundesliga", "liga", "dazn")):
        return SportSilo.FOOTBALL
    return SportSilo.ALTRI_SPORT


def _split_unspaced_hyphen(text: str) -> Optional[tuple[str, str]]:
    """Split `TeamA-TeamB` on an unspaced hyphen unless it belongs to a known compound name."""
    for idx, ch in enumerate(text):
        if ch != "-" or idx == 0 or idx == len(text) - 1:
            continue
        if text[idx - 1].isspace() or text[idx + 1].isspace():
            continue
        left = text[:idx].strip()
        right = text[idx + 1 :].strip()
        m_left = re.search(r"([A-Za-zÀ-ÖØ-öø-ÿ0-9]+)$", left)
        m_right = re.match(r"^([A-Za-zÀ-ÖØ-öø-ÿ0-9]+)", right)
        if not m_left or not m_right:
            continue
        lw = m_left.group(1)
        rw = m_right.group(1)
        if len(lw) < 2 or len(rw) < 2:
            continue
        lw_low = _strip_accents(lw).lower()
        rw_low = _strip_accents(rw).lower()
        if rw.isdigit() or rw_low in {"off", "offs", "in", "out", "star", "stars"}:
            continue
        if lw_low in _COMPOUND_HYPHEN_PREFIXES:
            continue
        if f"{lw_low}-{rw_low}" in _KNOWN_COMPOUND_HYPHEN_WORDS:
            continue
        return left, right
    return None


def _strip_round_phase_suffixes(text: str) -> str:
    cleaned = text.strip()
    while True:
        updated = _ROUND_PHASE_SUFFIX_RE.sub("", cleaned).strip()
        if updated == cleaned:
            break
        cleaned = updated
    return cleaned


def _normalize_motorsports_session(text: str) -> str:
    """Map Italian/English motorsports session labels to their counterpart for matching."""
    out = text
    replacements = (
        (r"\bprove\s+libere\s*([123])\b", r"FP\1"),
        (r"\blibere\s*([123])\b", r"FP\1"),
        (r"\bfree\s+practice\s*([123])\b", r"FP\1"),
        (r"\bfp\s*([123])\b", r"Prove Libere \1"),
        (r"\bprove\s+libere\b", "FP"),
        (r"\bqualifiche\b", "Qualifying"),
        (r"\bqualifying\b", "Qualifiche"),
        (r"\bgara\b", "Race"),
        (r"\brace\b", "Gara"),
    )
    for pattern, repl in replacements:
        new_out = re.sub(pattern, repl, out, flags=re.IGNORECASE)
        if new_out != out:
            out = new_out
            break
    return out


def _extract_epg_teams(title: str) -> tuple[str, str, str]:
    """Return `(league_hint, home, away)` parsed from an EPG program title."""
    cleaned = re.sub(r"\s*[\(\[]\s*(?:diretta|live)\s*[\)\]]\s*", " ", title or "", flags=re.IGNORECASE)
    cleaned = re.sub(r"^(?:diretta|live)\s*[:\-–]\s*", "", cleaned.strip(), flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()

    league_hint = ""
    if ":" in cleaned:
        prefix, rest = cleaned.split(":", 1)
        if rest.strip():
            league_hint = prefix.strip()
            cleaned = rest.strip()

    # Preserve motorsports prefix (F1, MotoGP, WRC, SBK, IndyCar) and treat as single-entity.
    m_hint = _MOTORI_PREFIX_RE.match(league_hint) if league_hint else None
    m_body = _MOTORI_PREFIX_RE.match(cleaned)
    if m_hint or m_body:
        if m_body:
            sport_prefix = m_body.group(1).strip()
            rest_body = m_body.group(2).strip()
            if not league_hint or _strip_accents(league_hint).lower() in {"motori", "automobilismo", "motociclismo"}:
                league_hint = sport_prefix
            elif sport_prefix.lower() not in league_hint.lower():
                league_hint = f"{league_hint} - {sport_prefix}"
            cleaned = rest_body or cleaned
        return league_hint, cleaned.strip(), ""

    cleaned = _strip_round_phase_suffixes(cleaned)

    # Handle multiple `-` with a competition prefix: e.g. "Serie A - Inter - Juventus"
    dash_parts = [p.strip() for p in re.split(r"\s+[-–—]\s+", cleaned) if p.strip()]
    if len(dash_parts) >= 3:
        comp_part = " - ".join(dash_parts[:-2])
        league_hint = f"{league_hint} - {comp_part}" if league_hint else comp_part
        return league_hint, dash_parts[-2], dash_parts[-1]

    parts = _TEAM_SPLIT_RE.split(cleaned, maxsplit=1)
    if len(parts) == 2 and parts[0].strip() and parts[1].strip():
        return league_hint, _strip_round_phase_suffixes(parts[0]), _strip_round_phase_suffixes(parts[1])

    unspaced = _split_unspaced_hyphen(cleaned)
    if unspaced is not None:
        return league_hint, _strip_round_phase_suffixes(unspaced[0]), _strip_round_phase_suffixes(unspaced[1])

    return league_hint, cleaned.strip(), ""


_ALTRI_SPORT_EXPLICIT_RE = re.compile(
    r"\b(?:golf|ciclismo|cycling|sci|biathlon|atletica|equtv|ippica|padel|snooker)\b",
    re.IGNORECASE,
)


def is_epg_match_compatible(epg: EPGProgram, match: Match) -> bool:
    """Enforce strict sport silo compatibility between an EPG program and a Match."""
    epg_silo = _infer_silo_from_epg(epg)
    football_family = {SportSilo.FOOTBALL, SportSilo.CALCIO_ITA}
    if epg_silo in football_family:
        return match.silo in football_family
    if epg_silo == SportSilo.ALTRI_SPORT:
        if match.silo == SportSilo.ALTRI_SPORT:
            return True
        combined = _strip_accents(f"{epg.sport_hint} {epg.category} {epg.channel_name} {epg.title}")
        return not bool(_ALTRI_SPORT_EXPLICIT_RE.search(combined))
    return epg_silo == match.silo


def _competition_tiebreak_bonus(epg: EPGProgram, match: Match) -> float:
    hint = _strip_accents(f"{epg.competition_hint} {epg.sport_hint}").lower().strip()
    if not hint:
        return 0.0
    m_comp = _strip_accents(f"{match.competition} {match.canonical_competition} {match.alt_league}").lower()
    if not m_comp:
        return 0.0
    hint_tokens = {t for t in re.findall(r"[a-z0-9]+", hint) if len(t) >= 3}
    comp_tokens = {t for t in re.findall(r"[a-z0-9]+", m_comp) if len(t) >= 3}
    if hint_tokens & comp_tokens:
        return 0.05
    return 0.0


def _epg_matches_diretta(epg: EPGProgram, match: Match) -> float:
    """Return match confidence score > 0 if `epg` represents `match`, else 0.0."""
    if not epg.is_live:
        return 0.0
    if epg.is_simulcast or _is_simulcast_title(epg.title):
        return 0.0
    if not is_epg_match_compatible(epg, match):
        return 0.0

    # Asymmetric linear TV window:
    # - Standard: [match.date - 35m, match.date + 30m] (or active overlap up to 90m pre-match)
    # - Motori:   [match.date - 120m, match.date + 20m]
    # - Tennis 1v1: [match.date - 360m, match.date + 60m]
    if match.silo == SportSilo.MOTORI:
        pre_ms, post_ms = _MOTORI_PRE_MATCH_MS, _MOTORI_POST_MATCH_MS
    elif match.silo == SportSilo.TENNIS:
        if match.away_team is not None:
            pre_ms, post_ms = _TENNIS_1V1_PRE_MATCH_MS, _TENNIS_1V1_POST_MATCH_MS
        else:
            pre_ms, post_ms = 60 * 60 * 1000, 30 * 60 * 1000
    else:
        pre_ms, post_ms = _LINEAR_PRE_MATCH_MS, _LINEAR_POST_MATCH_MS

    time_diff = match.date - epg.start_ms  # positive when EPG starts before match kickoff
    in_asym_window = -post_ms <= time_diff <= pre_ms
    overlaps = (
        epg.start_ms <= match.date <= (epg.end_ms + _LINEAR_POST_MATCH_MS)
        and time_diff <= max(pre_ms, 90 * 60 * 1000)
    )
    if not in_asym_window and not overlaps:
        return 0.0

    _, epg_home, epg_away = _extract_epg_teams(epg.title)
    epg_women, epg_youth = detect_demographics_from_stream_title(epg.title)

    # Tennis Singles vs Doubles guard + Tournament Session Bridge
    if match.silo == SportSilo.TENNIS:
        m_eval = f"{match.competition} {match.alt_league} {match.title}"
        ep_eval = f"{epg.competition_hint} {epg.title} {epg.description}"
        if _is_tennis_doubles(m_eval) != _is_tennis_doubles(ep_eval):
            return 0.0
        if not epg_away:
            # SuperTennis Guard: never bridge generic SuperTennis sessions to 1v1 matches
            if _is_supertennis_program(epg):
                return 0.0
            if is_non_live_or_studio(epg.title, epg.description):
                return 0.0
            m_circ, m_city = _extract_tennis_circuit_and_city(m_eval)
            ep_circ, ep_city = _extract_tennis_circuit_and_city(ep_eval)
            circ_ok = (m_circ == ep_circ) if (m_circ and ep_circ) else True
            city_ok = bool(
                m_city
                and ep_city
                and (
                    m_city == ep_city
                    or m_city in ep_eval.lower()
                    or ep_city in m_eval.lower()
                )
            )
            if circ_ok and city_ok:
                return 0.85 + _competition_tiebreak_bonus(epg, match)
            return 0.0

    # Pairwise team/athlete match
    if match.home_team and match.away_team and epg_home and epg_away:
        alt_h2 = _translate_country_team(epg_home)
        alt_a2 = _translate_country_team(epg_away)
        res = DEFAULT_TEAM_MATCHER.match_score(
            match.home_team.name,
            match.away_team.name,
            epg_home,
            epg_away,
            alt_home_1=match.alt_home,
            alt_away_1=match.alt_away,
            alt_home_2=alt_h2,
            alt_away_2=alt_a2,
            date_1_ms=match.date,
            date_2_ms=epg.start_ms if in_asym_window else 0,
            is_women_1=match.is_women,
            is_youth_1=match.is_youth,
            is_women_2=epg_women,
            is_youth_2=epg_youth,
        )
        return (res.score + _competition_tiebreak_bonus(epg, match)) if res.matched else 0.0

    # Single-entity events (e.g. F1 / MotoGP / Boxing cards)
    if match.away_team is None and match.home_team is not None:
        epg_single = epg.title if not epg_away else f"{epg_home} {epg_away}"
        alt_epg = (
            _normalize_motorsports_session(epg_single)
            if match.silo == SportSilo.MOTORI
            else ""
        )
        res = DEFAULT_TEAM_MATCHER.match_score(
            match.home_team.name,
            "",
            epg_single,
            "",
            alt_home_1=match.alt_home or match.competition,
            alt_home_2=alt_epg if alt_epg != epg_single else "",
            date_1_ms=match.date,
            date_2_ms=epg.start_ms if in_asym_window else 0,
            is_women_1=match.is_women,
            is_youth_1=match.is_youth,
            is_women_2=epg_women,
            is_youth_2=epg_youth,
        )
        return (res.score + _competition_tiebreak_bonus(epg, match)) if res.matched else 0.0

    return 0.0


def _merge_broadcasters(existing: list[str], additions: list[str]) -> list[str]:
    merged = list(existing)
    seen = {b.casefold() for b in merged}
    for ch in additions:
        key = ch.casefold()
        if key not in seen:
            seen.add(key)
            merged.append(ch)
    return merged


_REPLAY_OR_STUDIO_HARD_RE = re.compile(
    r"(?:"
    r"\btg\s*(?:sport|24|1|2|3|4|5|com|la7)?\b"
    r"|\bsport\s*24\b"
    r"|\bpaddock\s+live\b"
    r"|\bcalciomercato\b"
    r"|\brassegna\s+stampa\b"
    r"|\bstudio\b"
    r"|\bhighlights\b"
    r"|\bsintesi\b"
    r"|\breplica\b"
    r"|\bon\s*board\b"
    r"|\brubrica\b"
    r"|\bpre\s*-?\s*partita\b"
    r"|\bpost\s*-?\s*partita\b"
    r"|\bsky\s+calcio\s+club\b"
    r"|\b(?:la\s+)?domenica\s+sportiva\b"
    r"|\b90[°o]\s*minuto\b"
    r"|\bsabato\s+al\s+90[°o]\b"
    r"|\bpressing\b"
    r"|\btiki\s+taka\b"
    r"|\brace\s+anatomy\b"
    r"|\bafter\s+race\b"
    r"|\bl['’]originale\b"
    r"|\bsupertele\b"
    r"|\banteprima\b"
    r"|\bmagazine\b"
    r"|\bdocumentario\b"
    r"|\brewind\b"
    r"|\brivedi\b"
    r"|\bbest\s+of\b"
    r"|\bnessun\s+evento\b"
    r"|\bdifferita\b"
    r"|\barchivio\b"
    r"|\bintervista\b"
    r"|\bpodcast\b"
    r"|[\(\[]\s*r\s*[\)\]]"
    r")",
    re.IGNORECASE,
)


def _is_allowlisted_orphan(epg: EPGProgram) -> bool:
    """Strict Orphan Allowlist: admit only certified live simulcasts or legitimate non-1v1 live events."""
    if not epg.is_live:
        return False
    if _EPISODIC_SERIES_RE.search(f"{epg.title} {epg.description}"):
        return False

    is_sim = epg.is_simulcast or _is_simulcast_title(epg.title)
    ch_names = _split_channel_names(epg.channel_name)
    all_generalist = bool(ch_names) and all(
        _is_generalist_channel(ch, epg.channel_id) for ch in ch_names
    )

    if all_generalist:
        if is_non_live_or_studio(epg.title, epg.description):
            return False
        if not _passes_epg_program_guards(
            is_generalist=True,
            title=epg.title,
            description=epg.description,
            start_ms=epg.start_ms,
            end_ms=epg.end_ms,
            competition_hint=epg.competition_hint,
        ):
            return False

    if is_sim:
        return True

    title_norm = _strip_accents(epg.title or "")
    if _REPLAY_OR_STUDIO_HARD_RE.search(title_norm):
        return False
    if is_non_live_or_studio(epg.title, epg.description):
        has_explicit_live = bool(re.search(r"\b(?:live|diretta)\b", title_norm, re.I))
        if all_generalist or not has_explicit_live:
            return False

    silo = _infer_silo_from_epg(epg)
    if silo in (
        SportSilo.MOTORI,
        SportSilo.TENNIS,
        SportSilo.BASKET,
        SportSilo.VOLLEY,
        SportSilo.RUGBY,
        SportSilo.COMBAT,
        SportSilo.NFL,
        SportSilo.HOCKEY,
        SportSilo.BASEBALL,
        SportSilo.HANDBALL,
        SportSilo.ALTRI_SPORT,
    ):
        return True

    # Football / Calcio ITA non-simulcast: require dedicated sports channel and live/competition/1v1 signal
    if all_generalist:
        return False
    _, _, epg_away = _extract_epg_teams(epg.title)
    return bool(
        epg_away
        or epg.competition_hint
        or re.search(r"\b(?:live|diretta)\b", title_norm, re.I)
    )


def reconcile_with_schedule(
    diretta_matches: list[Match],
    epg_programs: list[EPGProgram],
) -> tuple[list[Match], list[Match]]:
    """Enrich `diretta_matches` with EPG broadcasters and build allowlisted orphan EPG `Match` objects."""
    broadcaster_updates: dict[int, list[str]] = {
        idx: list(m.broadcasters) for idx, m in enumerate(diretta_matches)
    }
    orphan_matches: list[Match] = []
    seen_orphan_ids: set[str] = set()
    discarded_orphans = 0

    for epg in epg_programs:
        epg_channels = _split_channel_names(epg.channel_name)
        best_idx: Optional[int] = None
        best_score = 0.0

        for idx, match in enumerate(diretta_matches):
            score = _epg_matches_diretta(epg, match)
            if score > best_score:
                best_score = score
                best_idx = idx

        if best_idx is not None:
            broadcaster_updates[best_idx] = _merge_broadcasters(
                broadcaster_updates[best_idx], epg_channels
            )
            continue

        if not _is_allowlisted_orphan(epg):
            discarded_orphans += 1
            continue

        # Build candidate orphan EPG Match for allowlisted live sports programs.
        silo = _infer_silo_from_epg(epg)
        league_hint, home_name, away_name = _extract_epg_teams(epg.title)
        comp_text = epg.competition_hint or league_hint or epg.sport_hint or epg.category
        is_women, is_youth = detect_demographics(comp_text)
        if not is_women or not is_youth:
            title_w, title_y = detect_demographics_from_stream_title(epg.title)
            is_women = is_women or title_w
            is_youth = is_youth or title_y

        category, genres = DEFAULT_GENRE_CLASSIFIER.classify_event(
            silo=silo.value,
            competition=comp_text,
            title=epg.title,
            is_women=is_women,
            is_youth=is_youth,
        )

        prefix = "tivusat" if epg.source == "tivusat" else "sky"
        slug = _slugify(epg.title)
        orphan_id = f"{prefix}_{epg.channel_id}_{slug}_{epg.start_ms}"
        if orphan_id in seen_orphan_ids:
            continue
        seen_orphan_ids.add(orphan_id)

        home_team = TeamInfo(name=home_name) if home_name else TeamInfo(name=epg.title)
        away_team = TeamInfo(name=away_name) if away_name else None

        orphan_matches.append(
            Match(
                id=orphan_id,
                title=epg.title,
                category=category,
                silo=silo,
                genres=genres,
                date=epg.start_ms,
                end_date=epg.end_ms,
                poster=epg.poster_url,
                competition=comp_text,
                home_team=home_team,
                away_team=away_team,
                is_women=is_women,
                is_youth=is_youth,
                broadcasters=epg_channels,
                source_origin="epg",
            )
        )

    if discarded_orphans:
        logger.debug(
            "EPG orphan allowlist: created %d orphans, discarded %d non-allowlisted programs",
            len(orphan_matches),
            discarded_orphans,
        )

    enriched = [
        m.model_copy(update={"broadcasters": broadcaster_updates[idx]})
        if broadcaster_updates[idx] != m.broadcasters
        else m
        for idx, m in enumerate(diretta_matches)
    ]
    return enriched, orphan_matches


def filter_ghost_epg_matches(matches: list[Match]) -> list[Match]:
    """Discard EPG-derived orphan matches that have zero attached playable streams."""
    return [m for m in matches if m.source_origin != "epg" or bool(m.sources)]


_GENERIC_CONTAINER_KEYWORDS: tuple[str, ...] = (
    "diretta gol", "zona gol", "zona dazn", "zona serie a", "zona serie b",
    "studio", "speciale", "mondo gol", "magazine", "highlights", "tg24",
    "sport24", "rubrica", "calcio show", "sky calcio club", "tutto il calcio",
    "linea notte", "anteprima", "l'originale",
)


def filter_conflicting_italian_channels(
    matches: list[Match],
    epg_programs: list[EPGProgram],
) -> list[Match]:
    """Purge palpably incorrect Italian TV sources when EPG confirms a different event on that channel."""
    if not matches or not epg_programs:
        return matches

    epg_by_channel: dict[str, list[EPGProgram]] = {}
    for ep in epg_programs:
        for ch_name in _split_channel_names(ep.channel_name):
            canon = canonicalize_channel_name(ch_name, country="Italy")
            if canon:
                epg_by_channel.setdefault(canon, []).append(ep)

    if not epg_by_channel:
        return matches

    total_purged = 0
    result: list[Match] = []
    for m in matches:
        if not m.sources or m.date <= 0:
            result.append(m)
            continue

        diretta_canons = {
            canonicalize_channel_name(bc, country="Italy")
            for bc in m.broadcasters
            if bc
        }
        m_title_clean = _normalize_simulcast_title(m.title)
        m_tokens = {t for t in m_title_clean.split() if len(t) >= 3}

        kept_sources = []
        changed = False
        for src in m.sources:
            src_label = src.channel_name or src.title
            canon = canonicalize_channel_name(src_label, country="Italy")
            if not canon or canon not in epg_by_channel:
                kept_sources.append(src)
                continue
            if canon in diretta_canons:
                kept_sources.append(src)
                continue

            pre_ms = _MOTORI_PRE_MATCH_MS if m.silo == SportSilo.MOTORI else 90 * 60_000
            post_ms = 20 * 60_000

            has_any_epg = False
            has_match_or_ambiguous = False
            conflicting_ep: Optional[EPGProgram] = None

            for ep in epg_by_channel[canon]:
                time_diff = m.date - ep.start_ms
                if time_diff > pre_ms or time_diff < -post_ms:
                    continue

                has_any_epg = True
                ep_low = (ep.title or "").lower()
                if ep.is_simulcast or any(k in ep_low for k in _GENERIC_CONTAINER_KEYWORDS):
                    if canon in ("dazn 1", "dazn 2", "dazn 3", "dazn 4") and not _is_simulcast_title(m.title):
                        conflicting_ep = conflicting_ep or ep
                        continue
                    has_match_or_ambiguous = True
                    break

                if _epg_matches_diretta(ep, m) > 0:
                    has_match_or_ambiguous = True
                    break

                ep_clean = _normalize_simulcast_title(ep.title)
                ep_tokens = {t for t in ep_clean.split() if len(t) >= 3}
                if m_tokens & ep_tokens:
                    has_match_or_ambiguous = True
                    break
                if m_title_clean and ep_clean and fuzz.token_set_ratio(m_title_clean, ep_clean) >= 55:
                    has_match_or_ambiguous = True
                    break

                if m.silo == SportSilo.TENNIS and _infer_silo_from_epg(ep) == SportSilo.TENNIS:
                    has_match_or_ambiguous = True
                    break

                conflicting_ep = conflicting_ep or ep

            if not has_any_epg or has_match_or_ambiguous or conflicting_ep is None:
                kept_sources.append(src)
            else:
                changed = True
                total_purged += 1

        if changed:
            result.append(m.model_copy(update={"sources": kept_sources}))
        else:
            result.append(m)

    if total_purged > 0:
        logger.info("EPG conflict filter: purged %d conflicting Italian channel sources", total_purged)
    return result


def _parse_iso_to_ms(value: str) -> int:
    if not value:
        return 0
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except ValueError:
        return 0


def _parse_tivusat_dt_to_ms(value: str) -> int:
    if not value:
        return 0
    raw = value.strip()
    for fmt in ("%d-%m-%Y %H:%M", "%d-%m-%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            naive = datetime.strptime(raw, fmt)
            rome_dt = naive.replace(tzinfo=_ROME_TZ)
            return int(rome_dt.astimezone(timezone.utc).timestamp() * 1000)
        except ValueError:
            continue
    return _parse_iso_to_ms(raw)


def _is_sports_program(
    *,
    is_dedicated_sports_channel: bool,
    title: str,
    description: str,
    category: str,
    sport_hint: str,
) -> bool:
    if is_dedicated_sports_channel:
        return True
    cat_low = _strip_accents(f"{category} {sport_hint}").lower()
    if any(k in cat_low for k in ("sport", "calcio", "motori", "tennis", "basket", "ciclismo", "rugby", "volley")):
        return True
    combined = _strip_accents(f"{title} {description}")
    return bool(_SPORTS_EVENT_HINT_RE.search(combined))


class ItalianEPGService:
    """Async EPG ingestion service for Sky Italia and Tivùsat Guida TV."""

    def __init__(
        self,
        settings: Optional[Settings] = None,
        client: Optional[httpx.AsyncClient] = None,
    ) -> None:
        self._settings = settings or get_settings()
        self._client = client
        self._owned_client: Optional[httpx.AsyncClient] = None
        self._sky_day_cache: dict[str, tuple[float, list[EPGProgram]]] = {}
        self._rolling_cache: list[EPGProgram] = []

    def _get_client(self) -> httpx.AsyncClient:
        """Return the injected or lazily created shared HTTP client."""
        if self._client is not None:
            return self._client
        if self._owned_client is None or self._owned_client.is_closed:
            self._owned_client = create_standard_client(timeout=20.0)
        return self._owned_client

    async def aclose(self) -> None:
        """Close the owned HTTP client if one was created."""
        if self._owned_client is not None and not self._owned_client.is_closed:
            await self._owned_client.aclose()
            self._owned_client = None

    @staticmethod
    def _compute_sky_is_live(
        ev: dict[str, Any],
        *,
        title: str,
        description: str,
        content_url: str,
    ) -> bool:
        raw_hl = ev.get("programHighlights")
        hl = str(raw_hl).strip().upper() if raw_hl is not None else ""
        url_low = content_url.lower()
        if "/replica" in url_low or "/differita" in url_low:
            return False
        if is_non_live_or_studio(title, description):
            return False
        if hl == "L":
            return True
        if raw_hl is not None and hl != "":
            return False
        if "/live-" in url_low:
            return True
        combined_low = f"{title} {description}".lower()
        if "diretta" in combined_low or re.search(r"\blive\b", title, re.I):
            return True
        return False

    async def _fetch_sky_day(
        self,
        client: httpx.AsyncClient,
        *,
        from_dt: datetime,
        to_dt: datetime,
        now_utc: datetime,
    ) -> list[EPGProgram]:
        """Fetch Sky Guida TV events for the `[from_dt, to_dt]` window with 3h day-snapshot cache."""
        from_iso = from_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        to_iso = to_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        day_key = now_utc.astimezone(_ROME_TZ).strftime("%Y-%m-%d")
        ch_ids = ",".join(dict.fromkeys([*SKY_CHANNELS.keys(), *SKY_CHANNEL_IDS.keys()]))

        programs: list[EPGProgram] = []
        page_num = 0
        page_size = 999
        fetch_failed = False

        while True:
            url = (
                f"{SKY_EVENTS_URL}?from={from_iso}&to={to_iso}"
                f"&pageSize={page_size}&pageNum={page_num}&env=DTH&channels={ch_ids}"
            )
            try:
                resp = await client.get(url)
                resp.raise_for_status()
                payload = resp.json()
            except Exception as exc:
                logger.warning("Sky EPG fetch failed (page=%s): %s", page_num, exc)
                fetch_failed = page_num == 0
                break

            events = payload.get("events") or []
            if not events:
                break

            for ev in events:
                ch_info = ev.get("channel") or {}
                ch_id = str(ch_info.get("id", "")).strip()
                ch_name = SKY_CHANNELS.get(ch_id) or canonical_from_sky_id(ch_id)
                if not ch_id or not ch_name:
                    continue

                content = ev.get("content") or {}
                title = (
                    ev.get("eventTitle")
                    or ev.get("epgEventTitle")
                    or content.get("contentTitle")
                    or ""
                ).strip()
                if not title:
                    continue

                content_url = str(content.get("url") or ev.get("url") or "").strip()
                url_comp, is_magazine_slug = _parse_sky_semantic_url(content_url)
                if is_magazine_slug:
                    continue

                description = (ev.get("eventSynopsis") or "").strip()
                genre_name = ((content.get("genre") or {}).get("name") or "").strip()
                subgenre_name = ((content.get("subgenre") or {}).get("name") or "").strip()

                is_dedicated_sports = (
                    ch_id in SKY_SPORTS_CHANNEL_IDS
                    or not _is_generalist_channel(ch_name, ch_id)
                )
                if not _is_sports_program(
                    is_dedicated_sports_channel=is_dedicated_sports,
                    title=title,
                    description=description,
                    category=genre_name,
                    sport_hint=subgenre_name,
                ):
                    continue

                start_ms = _parse_iso_to_ms(ev.get("starttime", ""))
                end_ms = _parse_iso_to_ms(ev.get("endtime", ""))
                if start_ms <= 0 or end_ms <= start_ms:
                    continue

                competition_hint = url_comp or _extract_competition_from_synopsis(description) or ""
                if not _passes_epg_program_guards(
                    is_generalist=not is_dedicated_sports,
                    title=title,
                    description=description,
                    start_ms=start_ms,
                    end_ms=end_ms,
                    competition_hint=competition_hint,
                ):
                    continue

                is_live = self._compute_sky_is_live(
                    ev,
                    title=title,
                    description=description,
                    content_url=content_url,
                )
                poster_url = _extract_sky_poster_url(content)
                ev_id = str(ev.get("eventId") or f"{ch_id}_{start_ms}")

                programs.append(
                    EPGProgram(
                        id=f"sky_{ev_id}",
                        channel_id=ch_id,
                        channel_name=ch_name,
                        title=title,
                        description=description,
                        category=genre_name,
                        start_ms=start_ms,
                        end_ms=end_ms,
                        sport_hint=subgenre_name,
                        competition_hint=competition_hint,
                        poster_url=poster_url,
                        is_live=is_live,
                        is_simulcast=_is_simulcast_title(title),
                        source="sky",
                    )
                )

            total = int(payload.get("total") or 0)
            if len(events) < page_size or (total and (page_num + 1) * page_size >= total):
                break
            page_num += 1

        now_mono = time.monotonic()
        now_ms = int(now_utc.timestamp() * 1000)

        if programs:
            merged = _merge_and_handover_channel_blocks(programs)
            self._sky_day_cache[day_key] = (now_mono, merged)
            # Prune expired day snapshots (> 3h TTL)
            expired_keys = [
                k for k, (ts, _) in self._sky_day_cache.items()
                if (now_mono - ts) > _SKY_DAY_CACHE_TTL_SECONDS
            ]
            for k in expired_keys:
                self._sky_day_cache.pop(k, None)
            return merged

        if fetch_failed:
            # Anti-midnight / outage fallback: check today or yesterday snapshot within 3h TTL
            prev_day_key = (now_utc.astimezone(_ROME_TZ) - timedelta(days=1)).strftime("%Y-%m-%d")
            for candidate_key in (day_key, prev_day_key):
                cached = self._sky_day_cache.get(candidate_key)
                if cached and (now_mono - cached[0]) <= _SKY_DAY_CACHE_TTL_SECONDS:
                    still_valid = [p for p in cached[1] if p.end_ms > now_ms]
                    if still_valid:
                        logger.warning(
                            "Sky EPG fetch failed; serving %d still-valid programs from %s snapshot",
                            len(still_valid),
                            candidate_key,
                        )
                        return still_valid

        return []

    async def _fetch_sky_with_client(
        self,
        client: httpx.AsyncClient,
        *,
        requested_from: Optional[datetime] = None,
        hours_ahead: int = 36,
        now_utc: Optional[datetime] = None,
    ) -> list[EPGProgram]:
        if not self._settings.ENABLE_SKY_EPG:
            return []

        now = _ensure_utc(now_utc or datetime.now(timezone.utc))
        raw_from = requested_from if requested_from is not None else now - _SKY_MAX_LOOKBACK
        from_dt = clamp_sky_from(raw_from, now)
        to_dt = now + timedelta(hours=max(1, hours_ahead))
        return await self._fetch_sky_day(
            client,
            from_dt=from_dt,
            to_dt=to_dt,
            now_utc=now,
        )

    async def fetch_sky_epg(
        self,
        *,
        requested_from: Optional[datetime] = None,
        hours_ahead: int = 36,
        now_utc: Optional[datetime] = None,
    ) -> list[EPGProgram]:
        """Fetch raw sports EPG items from Sky Italia REST API with 165m lookback clamp."""
        if not self._settings.ENABLE_SKY_EPG:
            return []
        return await self._fetch_sky_with_client(
            self._get_client(),
            requested_from=requested_from,
            hours_ahead=hours_ahead,
            now_utc=now_utc,
        )

    async def _fetch_tivusat_channel_day(
        self,
        client: httpx.AsyncClient,
        ch_id: str,
        date_str: str,
    ) -> list[EPGProgram]:
        url = TIVUSAT_EPG_URL.format(ch_id=ch_id, date_str=date_str)
        try:
            resp = await client.get(url)
            resp.raise_for_status()
            payload = resp.json()
        except Exception as exc:
            logger.warning("Tivùsat EPG fetch failed (ch=%s date=%s): %s", ch_id, date_str, exc)
            return []

        events = payload.get("events") or []
        canonical_channel = TIVUSAT_CHANNELS.get(ch_id) or canonical_from_tivusat_id(ch_id)
        if not canonical_channel:
            return []
        is_sports_ch = ch_id in TIVUSAT_SPORTS_CHANNEL_IDS or not _is_generalist_channel(
            canonical_channel, ch_id
        )
        items: list[EPGProgram] = []

        for ev in events:
            prog = ev.get("program") or {}
            title = (prog.get("title") or ev.get("title") or "").strip()
            if not title:
                continue
            description = (prog.get("description") or ev.get("description") or "").strip()

            genres = prog.get("genres") or []
            cats = prog.get("categories") or []
            genre_names = [str(g.get("name", "")).strip() for g in genres if isinstance(g, dict) and g.get("name")]
            cat_names = [str(c.get("name", "")).strip() for c in cats if isinstance(c, dict) and c.get("name")]
            category = ", ".join(genre_names or cat_names)

            if not _is_sports_program(
                is_dedicated_sports_channel=is_sports_ch,
                title=title,
                description=description,
                category=category,
                sport_hint="",
            ):
                continue

            start_ms = _parse_tivusat_dt_to_ms(ev.get("date_start", ""))
            end_ms = _parse_tivusat_dt_to_ms(ev.get("date_end", ""))
            if start_ms <= 0 or end_ms <= start_ms:
                continue

            comp_hint = _extract_competition_from_synopsis(description) or ""
            if not _passes_epg_program_guards(
                is_generalist=not is_sports_ch,
                title=title,
                description=description,
                start_ms=start_ms,
                end_ms=end_ms,
                competition_hint=comp_hint,
            ):
                continue

            ev_id = str(ev.get("id") or f"{ch_id}_{start_ms}")
            items.append(
                EPGProgram(
                    id=f"tivusat_{ev_id}",
                    channel_id=ch_id,
                    channel_name=canonical_channel,
                    title=title,
                    description=description,
                    category=category,
                    start_ms=start_ms,
                    end_ms=end_ms,
                    sport_hint=genre_names[0] if genre_names else "",
                    competition_hint=comp_hint,
                    is_live=not is_non_live_or_studio(title, description),
                    is_simulcast=_is_simulcast_title(title),
                    source="tivusat",
                )
            )
        return items

    async def _fetch_tivusat_with_client(
        self,
        client: httpx.AsyncClient,
        *,
        days_ahead: int = 1,
        now_utc: Optional[datetime] = None,
    ) -> list[EPGProgram]:
        if not self._settings.ENABLE_TIVUSAT_EPG:
            return []

        now_rome = _ensure_utc(now_utc or datetime.now(timezone.utc)).astimezone(_ROME_TZ)
        dates = [
            (now_rome + timedelta(days=offset)).strftime("%Y-%m-%d")
            for offset in range(0, max(0, days_ahead) + 1)
        ]

        channel_ids = list(dict.fromkeys([*TIVUSAT_CHANNELS.keys(), *TIVUSAT_CHANNEL_IDS.keys()]))
        coros = [
            self._fetch_tivusat_channel_day(client, ch_id, date_str)
            for ch_id in channel_ids
            for date_str in dates
        ]
        results = await asyncio.gather(*coros)
        items: list[EPGProgram] = []
        for batch in results:
            items.extend(batch)
        return _merge_and_handover_channel_blocks(items)

    async def fetch_tivusat_epg(
        self,
        *,
        days_ahead: int = 1,
        now_utc: Optional[datetime] = None,
    ) -> list[EPGProgram]:
        """Fetch raw sports EPG items from Tivùsat Guida TV REST API."""
        if not self._settings.ENABLE_TIVUSAT_EPG:
            return []
        return await self._fetch_tivusat_with_client(
            self._get_client(),
            days_ahead=days_ahead,
            now_utc=now_utc,
        )

    def _apply_rolling_cache(
        self,
        fresh_programs: list[EPGProgram],
        *,
        now_ms: int,
    ) -> list[EPGProgram]:
        """Retain still-active/recent programs across midnight transitions without time-shift duplicates."""
        if not self._rolling_cache:
            self._rolling_cache = list(fresh_programs)
            return fresh_programs

        fresh_ids = {p.id for p in fresh_programs}
        retained: list[EPGProgram] = []
        for prev in self._rolling_cache:
            if prev.id in fresh_ids:
                continue
            if prev.start_ms <= 0 or (now_ms - prev.start_ms) >= _ROLLING_CACHE_WINDOW_MS:
                continue
            if prev.end_ms <= now_ms - 30 * 60_000:
                continue

            prev_canon = canonicalize_channel_name(prev.channel_name, country="Italy")
            prev_norm = _normalize_simulcast_title(prev.title)
            is_time_shifted = False
            for curr in fresh_programs:
                curr_canon = canonicalize_channel_name(curr.channel_name, country="Italy")
                if prev_canon and curr_canon and prev_canon == curr_canon:
                    if abs(curr.start_ms - prev.start_ms) <= 90 * 60_000:
                        curr_norm = _normalize_simulcast_title(curr.title)
                        if prev_norm and curr_norm and (
                            prev_norm == curr_norm
                            or (len(prev_norm) >= 5 and prev_norm in curr_norm)
                            or (len(curr_norm) >= 5 and curr_norm in prev_norm)
                        ):
                            is_time_shifted = True
                            break
            if not is_time_shifted:
                retained.append(prev)

        combined = [*retained, *fresh_programs]
        self._rolling_cache = combined
        return combined

    async def fetch_epg(
        self,
        active_tennis_cities: frozenset[str] = frozenset(),
        *,
        requested_from: Optional[datetime] = None,
        hours_ahead: int = 36,
        days_ahead: int = 1,
        now_utc: Optional[datetime] = None,
    ) -> list[EPGProgram]:
        """Fetch Sky + Tivùsat EPGs, filter studio/replays/out-of-calendar tennis, and merge simulcasts."""
        ref_now = _ensure_utc(now_utc or datetime.now(timezone.utc))
        now_ms = int(ref_now.timestamp() * 1000)
        client = self._get_client()

        sky_items, tivusat_items = await asyncio.gather(
            self._fetch_sky_with_client(
                client,
                requested_from=requested_from,
                hours_ahead=hours_ahead,
                now_utc=ref_now,
            ),
            self._fetch_tivusat_with_client(
                client,
                days_ahead=days_ahead,
                now_utc=ref_now,
            ),
        )
        combined = [*sky_items, *tivusat_items]
        filtered: list[EPGProgram] = []
        for prog in combined:
            if is_non_live_or_studio(prog.title, prog.description, current_year=ref_now.year):
                continue
            if is_out_of_calendar_tennis(
                prog.title,
                prog.sport_hint or prog.channel_name,
                active_tennis_cities,
                current_year=ref_now.year,
            ):
                continue
            filtered.append(prog)

        deduped = deduplicate_simulcasts(filtered)
        return self._apply_rolling_cache(deduped, now_ms=now_ms)
