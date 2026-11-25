"""Italian linear TV EPG ingestion, filtering, simulcast deduplication, and reconciliation."""

from __future__ import annotations

import asyncio
import logging
import re
import time
import unicodedata
from datetime import date, datetime, timedelta, timezone, tzinfo
from typing import Any, Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import httpx
from rapidfuzz import fuzz

from app.config import Settings, get_settings
from app.core.network import create_standard_client
from app.models.domain import (
    DIRETTA_POST_MATCH_MS,
    SKY_EPG_POST_END_MS,
    EPGProgram,
    Match,
    SportSilo,
    TeamInfo,
)
from app.ingestion.diretta_service import extract_tennis_cities
from app.taxonomy import (
    DEFAULT_GENRE_CLASSIFIER,
    DEFAULT_TEAM_MATCHER,
    detect_demographics,
    detect_demographics_from_stream_title,
)
from app.taxonomy.channels import (
    canonical_from_sky_id,
    canonicalize_channel_name,
)
from app.taxonomy.leagues import DEFAULT_LEAGUE_TAXONOMY
from app.taxonomy.matcher import (
    _extract_motorsport_series,
    _extract_motorsport_session,
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
_SKY_MAX_LOOKBACK = timedelta(hours=3)
_SIMULCAST_WINDOW_MS = 45 * 60 * 1000
_RECONCILE_WINDOW_MS = 60 * 60 * 1000
_LINEAR_PRE_MATCH_MS = 60 * 60 * 1000
_LINEAR_POST_MATCH_MS = 30 * 60 * 1000
_MOTORI_PRE_MATCH_MS = 120 * 60 * 1000
_MOTORI_POST_MATCH_MS = 20 * 60 * 1000
_TENNIS_1V1_PRE_MATCH_MS = 360 * 60 * 1000
_TENNIS_1V1_POST_MATCH_MS = 60 * 60 * 1000
_SKY_OUTAGE_CACHE_TTL_SECONDS = 3 * 3600
_SKY_FUTURE_BUCKET_TTL_SECONDS = 12 * 3600
_SKY_POST_MIDNIGHT_ROTATION_MINUTE = 20
SKY_FEED_DAYS: tuple[int, ...] = (0, 1, 2)
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
    "807": "Rai Sport",
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
# Sky API silently drops all FTA channels (`isAvailableOnSkygo=False`) when mixed with Pay-TV IDs.
SKY_FTA_SPORTS_CHANNEL_IDS: frozenset[str] = frozenset({"6000", "807"})
SKY_FTA_CHANNEL_IDS: tuple[str, ...] = tuple(
    cid
    for cid in SKY_CHANNELS
    if cid in SKY_FTA_SPORTS_CHANNEL_IDS or cid in SKY_GENERALIST_CHANNEL_IDS
)
SKY_PAY_CHANNEL_IDS: tuple[str, ...] = tuple(
    cid
    for cid in SKY_CHANNELS
    if cid not in SKY_FTA_SPORTS_CHANNEL_IDS and cid not in SKY_GENERALIST_CHANNEL_IDS
)
SKY_FTA_CHANNEL_ID_SET: frozenset[str] = frozenset(SKY_FTA_CHANNEL_IDS)
SKY_PAY_CHANNEL_ID_SET: frozenset[str] = frozenset(SKY_PAY_CHANNEL_IDS)

_HARD_NON_LIVE_PATTERN = (
    r"\btg\b"
    r"|\bnotiziario\b"
    r"|\bcalciomercato\b"
    r"|\brassegna\s+stampa\b"
    r"|\bconf(?:erenza)?\.?\s+stampa\b"
    r"|\bpit\s+walk\b"
    r"|\bafter\s+party\b"
    r"|\bstudio\b"
    r"|\bshow\b"
    r"|\bhighlights\b"
    r"|\bsintesi\b"
    r"|\breplica\b"
    r"|\bon\s*board\b"
    r"|\brubrica\b"
    r"|\bpre\s*-?\s*(?:partita|gara|match)\b"
    r"|\bpost\s*-?\s*(?:partita|gara|match)\b"
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
)
_HARD_NON_LIVE_TITLE_RE = re.compile(rf"(?:{_HARD_NON_LIVE_PATTERN})", re.IGNORECASE)
_NON_LIVE_TITLE_RE = re.compile(
    rf"(?:{_HARD_NON_LIVE_PATTERN}|\bspeciale\b|^\s*ep\.?\s*\d+\b|\bstory\b|\blegends\b|\bdoc\b)",
    re.IGNORECASE,
)

_NON_LIVE_DESC_RE = re.compile(
    r"(?:"
    r"\bin\s+replica\b"
    r"|\brubrica\b"
    r"|\bnotiziario\b"
    r"|\brassegna\s+stampa\b"
    r"|\bconf(?:erenza)?\.?\s+stampa\b"
    r"|\bapprofondimento\b"
    r"|\btalk\s+show\b"
    r"|\bazioni\s+salienti\b"
    r"|\bhighlights\b"
    r"|\bsintesi\b"
    r"|\bcalciomercato\b"
    r"|\bpre\s*-?\s*(?:partita|gara|match)\b"
    r"|\bpost\s*-?\s*(?:partita|gara|match)\b"
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
_BARE_DATE_TITLE_RE = re.compile(r"^\d{1,2}[/\-.]\d{1,2}(?:[/\-.]\d{2,4})?$")
_NON_LIVE_URL_RE = re.compile(
    r"/(?:pre-?partita|post-?partita|pre-?gara|post-?gara|replica|differita|highlights|sintesi|speciale|magazine|rubrica|documentario|studio)\b",
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
_DATE_TOKEN_RE = re.compile(r"\b\d{1,2}[/\-.]\d{1,2}(?:[/\-.]\d{2,4})?\b")
_STAG_EP_TOKEN_RE = re.compile(
    r"(?:\bs\d+\s*ep\d+\b|\bstag(?:ione)?\.?\s*\d+\b|\bep\.?\s*\d+\b)",
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
_COMP_YEAR_PREFIX_RE = re.compile(
    r"^(?:(?:uefa|cev|fiba|atp|wta)\s+)?"
    r"(?:champions\s+league|europa\s+league|conference\s+league|serie\s+[abc12]|"
    r"premier\s+league|bundesliga|eurolega|superlega)"
    r"(?:\s+(?:f|m|femminile|maschile|donne|uomini))?\s+(?:19|20)\d{2}\s+",
    re.IGNORECASE,
)
_COMP_WOMEN_F_RE = re.compile(
    r"\b(?:champions\s+league|europa\s+league|serie\s+a1?|coppa\s+italia|supercoppa|"
    r"volley|pallavolo|basket|calcio|cev)\s+f\b",
    re.IGNORECASE,
)
_U23_BTEAM_RE = re.compile(r"\b(?:u[- ]?23|under[- ]?23|next\s*gen)\b", re.IGNORECASE)
_TRAILING_WOMEN_CODE_RE = re.compile(r"\s+[DW]\s*$")

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
_NON_TEAM_KEYWORDS: frozenset[str] = frozenset(
    {
        "gara", "gp", "gran premio", "finale", "semifinale", "quarti", "ottavi",
        "giornata", "highlights", "sintesi", "diretta", "live", "speciale",
        "studio", "prove libere", "qualifiche", "sprint", "warm up", "giorno",
        "day", "round", "turno", "episodio", "stagione", "after party", "paddock",
        "rubrica", "notiziario", "show", "review", "preview", "prepartita",
        "postpartita", "conf stampa", "conferenza stampa",
    }
)
_SEQUENTIAL_SLOT_SIDE_RE = re.compile(
    r"^(?:giorno|day|gara|game|match|round|turno|ep|episodio|stagione)\s*\d+$",
    re.IGNORECASE,
)

# Closed 6-category orphan allowlist & fallback silo matchers
_CALCIO_ITA_HINT_RE = re.compile(
    r"\b(?:serie\s*[abc]|coppa\s+italia|supercoppa\s+italiana|zona\s+dazn|zona\s+serie\s*[ab])\b",
    re.IGNORECASE,
)
_MOTORI_FALLBACK_RE = re.compile(
    r"\b(?:formula\s*1|formula\s*2|formula\s*3|formula\s*e|f1|f2|f3|motogp|moto\s*2|moto\s*3|"
    r"superbike|wsbk|sbk|wrc|indycar|nascar|wec|motori|automobilismo|motociclismo)\b",
    re.IGNORECASE,
)
_TENNIS_FALLBACK_RE = re.compile(
    r"\b(?:tennis|supertennis|atp|wta|challenger|wimbledon|roland\s+garros|us\s+open|"
    r"australian\s+open|coppa\s+davis|davis\s+cup|bjk\s*cup|billie\s+jean\s+king|laver\s+cup|united\s+cup)\b",
    re.IGNORECASE,
)
_BASKET_FALLBACK_RE = re.compile(
    r"\b(?:basket|basketball|nba|wnba|eurolega|euroleague|eurocup|pallacanestro|fiba|lba)\b",
    re.IGNORECASE,
)
_COMBAT_FALLBACK_RE = re.compile(
    r"\b(?:ufc|mma|boxe|boxing|pugilato|wrestling|wwe|aew)\b",
    re.IGNORECASE,
)
_VOLLEY_FALLBACK_RE = re.compile(
    r"\b(?:volley|pallavolo|superlega|cev\s+champions\s+league)\b",
    re.IGNORECASE,
)
_NFL_FALLBACK_RE = re.compile(
    r"\b(?:nfl|red\s*zone|football\s+americano)\b",
    re.IGNORECASE,
)
_HOCKEY_FALLBACK_RE = re.compile(r"\b(?:hockey|nhl)\b", re.IGNORECASE)
_BASEBALL_FALLBACK_RE = re.compile(r"\b(?:baseball|mlb)\b", re.IGNORECASE)
_RUGBY_FALLBACK_RE = re.compile(
    r"\b(?:rugby|sei\s+nazioni|six\s+nations|nations\s+championship|urc|top\s*14)\b",
    re.IGNORECASE,
)
_HANDBALL_FALLBACK_RE = re.compile(r"\b(?:pallamano|handball)\b", re.IGNORECASE)
_ALTRI_SPORT_EXPLICIT_RE = re.compile(
    r"\b(?:golf|ciclismo|cycling|sci|sport\s+invernali|biathlon|atletica|nuoto|tuffi|"
    r"pattinaggio|equtv|ippica|padel|snooker|olimpiadi|vela)\b",
    re.IGNORECASE,
)
_FOOTBALL_FALLBACK_RE = re.compile(
    r"\b(?:calcio|football|soccer|champions\s+league|europa\s+league|conference\s+league|"
    r"nations\s+league|premier\s+league|bundesliga|la\s*liga|ligue\s*1|diretta\s+gol)\b",
    re.IGNORECASE,
)
_MOTORI_TRACK_SESSION_RE = re.compile(
    r"\b(?:prove\s+libere(?:\s*[123])?|libere\s*[123]|free\s+practice(?:\s*[123])?|"
    r"fp\s*[123]|qualifich[ei]|qualifying|quali|shootout|"
    r"sprint(?:\s*(?:race|gara|qualifich[ei]|qualifying))?|gara|race|warm\s*up)\b",
    re.IGNORECASE,
)
_GOLF_ORPHAN_RE = re.compile(
    r"\b(?:golf|pga\s+tour|dp\s+world\s+tour|ryder\s+cup|liv\s+golf|lpga)\b",
    re.IGNORECASE,
)
_PADEL_ORPHAN_RE = re.compile(r"\bpadel\b", re.IGNORECASE)
_RECOGNIZED_NON_1V1_SPORT_RE = re.compile(
    r"\b(?:ciclismo|cycling|giro\s+d['’]italia|tour\s+de\s+france|vuelta|"
    r"sci|sport\s+invernali|biathlon|slalom|discesa\s+libera|super-?g|"
    r"salto\s+con\s+gli\s+sci|snowboard|atletica|diamond\s+league|maratona|"
    r"nuoto|tuffi|swimming|pattinaggio|skating|olimpiadi|olympics|giochi\s+olimpici)\b",
    re.IGNORECASE,
)

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
    "golf-dp-world-tour": "DP World Tour",
    "ryder-cup": "Ryder Cup",
    "cev-champions-league-femminile": "CEV Champions League Femminile",
    "cev-champions-league-f": "CEV Champions League Femminile",
    "fiba-qualificazioni-world-cup": "FIBA World Cup Qualifiers",
    "diretta-gol-champions-league": "UEFA Champions League",
    "diretta-gol-europa-and-conference-league": "UEFA Europa & Conference League",
    "nations-championship": "Nations Championship",
}

_NON_SPORTS_GENRES: frozenset[str] = frozenset(
    {"film", "cinema", "informazione", "ragazzi e musica", "mondo e tendenze"}
)
_NON_SPORTS_SUBGENRES: frozenset[str] = frozenset(
    {
        "fiction",
        "soap opera",
        "reality show",
        "gioco",
        "quiz",
        "sit com",
        "telefilm",
        "talk show",
        "show",
        "documentario",
        "cucina",
        "meteo",
        "cartoni animati",
        "attualita",
        "lifestyle",
        "scienza",
        "arte e cultura",
        "avventura",
        "religione",
        "musica",
        "societa",
        "biografico",
        "romantico",
        "drammatico",
        "azione",
        "commedia",
    }
)


def _strip_accents(text: str) -> str:
    norm = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in norm if not unicodedata.combining(ch))


def _ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _is_generalist_channel(channel_name: str, channel_id: str = "") -> bool:
    if channel_id and channel_id in SKY_GENERALIST_CHANNEL_IDS:
        return True
    canon = canonicalize_channel_name(channel_name, country="Italy")
    return canon in _GENERALIST_CANONICAL_CHANNELS


def _is_simulcast_title(title: str) -> bool:
    return bool(_SIMULCAST_TITLE_RE.search(_strip_accents(title or "")))


def extract_competition_from_sky_url(content_url: str) -> Optional[str]:
    """Extract a known competition name from a Sky `content.url` slug; never invent from unknown slugs."""
    if not content_url or not isinstance(content_url, str):
        return None

    m = re.search(r"/(?:sport|programmi-tv)/([^/]+)/([^/]+)", content_url)
    if not m:
        return None

    comp_slug = m.group(1).lower()
    if comp_slug in _SKY_SLUG_COMP_MAP:
        return _SKY_SLUG_COMP_MAP[comp_slug]
    if comp_slug == "tennis-wta":
        return "WTA"
    if comp_slug == "tennis-atp":
        return "ATP"
    if comp_slug == "tennis-challenger":
        return "Challenger"

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
            return f"ATP Masters 1000 {city}".strip()
        if tier:
            return f"{circuit} {tier} {city}".strip()
        return f"{circuit} {city}".strip()

    if comp_slug == "padel":
        return "Padel"
    if comp_slug.startswith("padel-"):
        clean_p = " ".join(w.capitalize() for w in comp_slug.removeprefix("padel-").split("-") if w)
        return f"Padel {clean_p}".strip() if clean_p else "Padel"
    if comp_slug.startswith("premier-padel"):
        clean_p = " ".join(w.capitalize() for w in comp_slug.removeprefix("premier-padel").strip("-").split("-") if w)
        return f"Premier Padel {clean_p}".strip() if clean_p else "Premier Padel"

    return None


def _extract_sky_poster_url(content: dict[str, Any]) -> str:
    """Extract valid 16:9 Sky poster URL (`scene_key_art` -> `scene`), dropping 403/400 placeholders."""
    images_map = content.get("imagesMap")
    if not isinstance(images_map, list):
        return ""
    img_dict: dict[str, str] = {}
    for item in images_map:
        if not isinstance(item, dict):
            continue
        key = item.get("key")
        url = (item.get("img") or {}).get("url")
        if (
            key in ("scene_key_art", "scene")
            and isinstance(url, str)
            and _UUID_RE.search(url)
            and "cgid=90" not in url.lower()
        ):
            img_dict[key] = url
    chosen = img_dict.get("scene_key_art") or img_dict.get("scene") or ""
    if not chosen:
        return ""
    return f"https://guidatv.sky.it{chosen}" if chosen.startswith("/") else chosen


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
                _, prev_home, prev_away = _extract_epg_teams(prev.title, prev.content_title)
                _, curr_home, curr_away = _extract_epg_teams(curr.title, curr.content_title)
                is_1v1_pair = bool(prev_home and prev_away) or bool(curr_home and curr_away)
                if (
                    not is_1v1_pair
                    and prev.title.strip().casefold() == curr.title.strip().casefold()
                    and 0 <= (curr.start_ms - prev.end_ms) <= 25 * 60_000
                ):
                    merged[-1] = prev.model_copy(
                        update={
                            "end_ms": max(prev.end_ms, curr.end_ms),
                            "epg_title": prev.epg_title or curr.epg_title,
                            "content_title": prev.content_title or curr.content_title,
                            "description": prev.description or curr.description,
                            "content_url": prev.content_url or curr.content_url,
                            "program_highlights": (
                                "L"
                                if (prev.program_highlights == "L" or curr.program_highlights == "L")
                                else (prev.program_highlights or curr.program_highlights)
                            ),
                            "season_number": (
                                prev.season_number
                                if prev.season_number is not None
                                else curr.season_number
                            ),
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


def _is_tennis_doubles(text: str) -> bool:
    return bool(re.search(r"\b(?:doubles?|doppi[oa]?)\b", text or "", re.IGNORECASE))


def _extract_tennis_circuit(text: str) -> Optional[str]:
    """Extract normalized tennis circuit identifier (`atp`, `wta`, `challenger`, `slam`, `davis`, `bjk`)."""
    t = _strip_accents((text or "").lower())
    if re.search(r"\b(?:wta|wta\s*1000|wta\s*500|wta\s*250|wta\s*125)\b", t):
        return "wta"
    if re.search(r"\bchallenger\b", t):
        return "challenger"
    if re.search(r"\b(?:wimbledon|roland\s*garros|us\s*open|australian\s*open)\b", t):
        return "slam"
    if re.search(r"\b(?:coppa\s+davis|davis\s+cup)\b", t):
        return "davis"
    if re.search(r"\b(?:bjk\s*cup|billie\s+jean\s+king)\b", t):
        return "bjk"
    if re.search(r"\b(?:atp|masters\s*1000|atp\s*500|atp\s*250|atp\s*finals|masters)\b", t):
        return "atp"
    return None


def _tennis_match_tokens(match: Match) -> set[str]:
    """Extract dynamic active tournament/city tokens directly from a Diretta tennis `Match`."""
    tokens = extract_tennis_cities(match.competition, match.alt_league)
    header = _strip_accents(f"{match.competition} {match.alt_league}").lower()
    if "internazionali bnl" in header:
        tokens.add("roma")
    cleaned = {t for t in (_strip_accents(tok).lower().strip() for tok in tokens) if len(t) >= 3}
    if "coppa davis" in cleaned:
        cleaned.add("davis cup")
    elif "davis cup" in cleaned:
        cleaned.add("coppa davis")
    return cleaned


def _tennis_tournament_matches_text(match: Match, normalized_eval_low: str) -> bool:
    """Return True if any active tournament/city token of `match` appears in `normalized_eval_low`."""
    for tok in _tennis_match_tokens(match):
        if re.search(rf"(?<!\w){re.escape(tok)}(?!\w)", normalized_eval_low):
            return True
    return False


def _is_supertennis_program(epg: EPGProgram) -> bool:
    if epg.channel_id == "6000":
        return True
    return "supertennis" in (epg.channel_name or "").lower()


def clamp_sky_from(requested_from: datetime, now_utc: datetime) -> datetime:
    """Clamp Sky API `from` parameter to at most 3h lookback and >= `22:05 UTC` previous day."""
    req = _ensure_utc(requested_from)
    now = _ensure_utc(now_utc)
    lookback_floor = now - _SKY_MAX_LOOKBACK
    prev_utc_day = now.date() - timedelta(days=1)
    midnight_safe_floor = datetime(
        prev_utc_day.year,
        prev_utc_day.month,
        prev_utc_day.day,
        22,
        5,
        0,
        tzinfo=timezone.utc,
    )
    min_allowed = max(lookback_floor, midnight_safe_floor)
    return req if req >= min_allowed else min_allowed


def _sky_logical_rome_day(local_now: datetime) -> str:
    """Return logical Rome YYYY-MM-DD anchored to post-midnight `00:20` rotation."""
    if local_now.hour == 0 and local_now.minute < _SKY_POST_MIDNIGHT_ROTATION_MINUTE:
        return (local_now - timedelta(days=1)).date().isoformat()
    return local_now.date().isoformat()


def _rome_day_bounds_utc(local_day: date) -> tuple[datetime, datetime]:
    """Return UTC `(start, end)` bounds covering `[00:00:00, 23:59:59]` Rome time."""
    next_day = local_day + timedelta(days=1)
    start_utc = datetime(
        local_day.year, local_day.month, local_day.day, 0, 0, 0, tzinfo=_ROME_TZ
    ).astimezone(timezone.utc)
    end_utc = (
        datetime(
            next_day.year, next_day.month, next_day.day, 0, 0, 0, tzinfo=_ROME_TZ
        )
        - timedelta(seconds=1)
    ).astimezone(timezone.utc)
    return start_utc, end_utc


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
    epg_title: str = "",
    content_title: str = "",
    content_url: str = "",
    season_number: Optional[int] = None,
    current_year: Optional[int] = None,
) -> bool:
    """Return True if the EPG entry is a studio show, news, magazine, series, highlights, or replay."""
    ref_year = current_year if current_year is not None else datetime.now(timezone.utc).year
    if season_number is not None and season_number < ref_year:
        return True
    if content_url and _NON_LIVE_URL_RE.search(content_url):
        return True
    clean_title = _strip_accents((title or "").strip())
    if not clean_title:
        return True
    clean_epg = _strip_accents((epg_title or "").strip())
    clean_ct = _strip_accents((content_title or "").strip())
    clean_desc = _strip_accents((description or "").strip())
    if _BARE_DATE_TITLE_RE.match(clean_title) and (
        not clean_ct or _BARE_DATE_TITLE_RE.match(clean_ct)
    ):
        return True
    if _EPISODIC_SERIES_RE.search(f"{clean_title} {clean_epg} {clean_ct} {clean_desc}"):
        return True
    for candidate in (clean_title, clean_epg, clean_ct):
        if candidate and _NON_LIVE_TITLE_RE.search(candidate):
            return True
    for candidate in (clean_title, clean_ct, clean_desc):
        if candidate and _has_historical_year(candidate, ref_year):
            return True
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
    clean = _DATE_TOKEN_RE.sub(" ", clean)
    clean = _STAG_EP_TOKEN_RE.sub(" ", clean)
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
    if a.channel_id and b.channel_id and a.channel_id == b.channel_id:
        return False

    silo_a = _infer_silo_from_epg(a)
    silo_b = _infer_silo_from_epg(b)
    if silo_a != silo_b:
        football_family = {SportSilo.FOOTBALL, SportSilo.CALCIO_ITA}
        if not ({silo_a, silo_b} <= football_family):
            a_generic = silo_a == SportSilo.ALTRI_SPORT and _strip_accents(a.sport_hint or "").strip().lower() in {"", "altri", "intrattenimento"}
            b_generic = silo_b == SportSilo.ALTRI_SPORT and _strip_accents(b.sport_hint or "").strip().lower() in {"", "altri", "intrattenimento"}
            if not (a_generic or b_generic):
                return False

    if silo_a == SportSilo.MOTORI or silo_b == SportSilo.MOTORI:
        a_txt = f"{a.competition_hint} {a.title} {a.content_title}"
        b_txt = f"{b.competition_hint} {b.title} {b.content_title}"
        ser_a, ser_b = _extract_motorsport_series(a_txt), _extract_motorsport_series(b_txt)
        if ser_a and ser_b and ser_a != ser_b:
            return False
        sess_a, sess_b = _extract_motorsport_session(a_txt), _extract_motorsport_session(b_txt)
        if sess_a and sess_b and sess_a != sess_b:
            return False

    if silo_a == SportSilo.TENNIS or silo_b == SportSilo.TENNIS:
        a_ten = f"{a.title} {a.content_title} {a.description}"
        b_ten = f"{b.title} {b.content_title} {b.description}"
        if _is_tennis_doubles(a_ten) != _is_tennis_doubles(b_ten):
            return False

    sim_a = a.is_simulcast or _is_simulcast_title(a.title) or _is_simulcast_title(a.content_title)
    sim_b = b.is_simulcast or _is_simulcast_title(b.title) or _is_simulcast_title(b.content_title)
    if sim_a != sim_b:
        return False

    norms_a = {n for n in (_normalize_simulcast_title(a.title), _normalize_simulcast_title(a.content_title)) if n}
    norms_b = {n for n in (_normalize_simulcast_title(b.title), _normalize_simulcast_title(b.content_title)) if n}
    if not norms_a or not norms_b:
        return False

    if sim_a and sim_b:
        comp_a = _strip_accents(a.competition_hint or "").strip().lower()
        comp_b = _strip_accents(b.competition_hint or "").strip().lower()
        if comp_a and comp_b and comp_a == comp_b:
            return True
        for na in norms_a:
            for nb in norms_b:
                if na == nb:
                    return True
                if min(len(na), len(nb)) >= 6 and fuzz.token_sort_ratio(na, nb) >= 80:
                    return True
        return False

    for na in norms_a:
        for nb in norms_b:
            if na == nb:
                return True
            if min(len(na), len(nb)) >= 6 and fuzz.token_sort_ratio(na, nb) >= 86:
                return True
    return False


def deduplicate_simulcasts(programs: list[EPGProgram]) -> list[EPGProgram]:
    """Merge identical broadcasts across channels within 45m, expanding window and channels."""
    if not programs:
        return []

    ordered = sorted(
        programs,
        key=lambda p: (
            p.start_ms,
            not p.is_live,
            not p.is_dedicated_sports,
            p.source != "sky",
            p.channel_name,
        ),
    )
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

        best_epg_title = next((g.epg_title for g in group if g.epg_title), "")
        best_content_title = next((g.content_title for g in group if g.content_title), "")
        best_desc = max((g.description for g in group if g.description), key=len, default="")
        best_cat = next((g.category for g in group if g.category), "")
        best_hint = next((g.sport_hint for g in group if g.sport_hint), "")
        best_url = next((g.content_url for g in group if g.content_url), "")
        best_hl = (
            "L"
            if any(g.program_highlights == "L" for g in group)
            else next((g.program_highlights for g in group if g.program_highlights), "")
        )
        best_season = next((g.season_number for g in group if g.season_number is not None), None)
        best_comp = next((g.competition_hint for g in group if g.competition_hint), "")
        best_poster = next((g.poster_url for g in group if g.poster_url), "")
        merged.append(
            primary.model_copy(
                update={
                    "is_dedicated_sports": any(g.is_dedicated_sports for g in group),
                    "channel_name": ", ".join(channels),
                    "start_ms": min(g.start_ms for g in group),
                    "end_ms": max(g.end_ms for g in group),
                    "epg_title": best_epg_title,
                    "content_title": best_content_title,
                    "description": best_desc,
                    "category": best_cat,
                    "sport_hint": best_hint,
                    "content_url": best_url,
                    "program_highlights": best_hl,
                    "season_number": best_season,
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


_SUBGENRE_SILO_MAP: dict[str, SportSilo] = {
    "tennis": SportSilo.TENNIS,
    "basket": SportSilo.BASKET,
    "pallacanestro": SportSilo.BASKET,
    "volley": SportSilo.VOLLEY,
    "pallavolo": SportSilo.VOLLEY,
    "motori": SportSilo.MOTORI,
    "automobilismo": SportSilo.MOTORI,
    "motociclismo": SportSilo.MOTORI,
    "rugby": SportSilo.RUGBY,
    "pugilato": SportSilo.COMBAT,
    "boxe": SportSilo.COMBAT,
    "mma": SportSilo.COMBAT,
    "arti marziali": SportSilo.COMBAT,
    "wrestling": SportSilo.COMBAT,
    "combattimento": SportSilo.COMBAT,
    "football americano": SportSilo.NFL,
    "hockey": SportSilo.HOCKEY,
    "hockey su ghiaccio": SportSilo.HOCKEY,
    "baseball": SportSilo.BASEBALL,
    "pallamano": SportSilo.HANDBALL,
    "handball": SportSilo.HANDBALL,
    "golf": SportSilo.ALTRI_SPORT,
    "ciclismo": SportSilo.ALTRI_SPORT,
    "sport invernali": SportSilo.ALTRI_SPORT,
    "sci": SportSilo.ALTRI_SPORT,
    "atletica": SportSilo.ALTRI_SPORT,
    "nuoto": SportSilo.ALTRI_SPORT,
    "vela": SportSilo.ALTRI_SPORT,
    "ippica": SportSilo.ALTRI_SPORT,
    "padel": SportSilo.ALTRI_SPORT,
    "snooker": SportSilo.ALTRI_SPORT,
    "olimpiadi": SportSilo.ALTRI_SPORT,
    "pattinaggio": SportSilo.ALTRI_SPORT,
}


def _infer_silo_from_epg(epg: EPGProgram) -> SportSilo:
    sh = _strip_accents(epg.sport_hint or "").strip().lower()
    if sh == "calcio":
        ita_text = _strip_accents(f"{epg.competition_hint} {epg.title} {epg.content_title}")
        return SportSilo.CALCIO_ITA if _CALCIO_ITA_HINT_RE.search(ita_text) else SportSilo.FOOTBALL
    if sh in _SUBGENRE_SILO_MAP:
        return _SUBGENRE_SILO_MAP[sh]

    combined = _strip_accents(
        f"{epg.competition_hint} {epg.title} {epg.content_title} {epg.channel_name}"
    )
    if _MOTORI_FALLBACK_RE.search(combined):
        return SportSilo.MOTORI
    if _TENNIS_FALLBACK_RE.search(combined):
        return SportSilo.TENNIS
    if _BASKET_FALLBACK_RE.search(combined):
        return SportSilo.BASKET
    if _COMBAT_FALLBACK_RE.search(combined):
        return SportSilo.COMBAT
    if _VOLLEY_FALLBACK_RE.search(combined):
        return SportSilo.VOLLEY
    if _NFL_FALLBACK_RE.search(combined):
        return SportSilo.NFL
    if _HOCKEY_FALLBACK_RE.search(combined):
        return SportSilo.HOCKEY
    if _BASEBALL_FALLBACK_RE.search(combined):
        return SportSilo.BASEBALL
    if _RUGBY_FALLBACK_RE.search(combined):
        return SportSilo.RUGBY
    if _HANDBALL_FALLBACK_RE.search(combined):
        return SportSilo.HANDBALL
    if _ALTRI_SPORT_EXPLICIT_RE.search(combined):
        return SportSilo.ALTRI_SPORT
    if _CALCIO_ITA_HINT_RE.search(combined):
        return SportSilo.CALCIO_ITA
    if _FOOTBALL_FALLBACK_RE.search(combined):
        return SportSilo.FOOTBALL
    return SportSilo.ALTRI_SPORT


def _strip_round_phase_suffixes(text: str) -> str:
    cleaned = text.strip()
    while True:
        updated = _ROUND_PHASE_SUFFIX_RE.sub("", cleaned).strip()
        if updated == cleaned:
            break
        cleaned = updated
    return cleaned


def _is_valid_team_side(side: str) -> bool:
    norm = _strip_accents((side or "").strip())
    if not norm or _BARE_DATE_TITLE_RE.match(norm):
        return False
    low = re.sub(r"[^\w\s]", " ", norm.lower())
    low = re.sub(r"\s+", " ", low).strip()
    if len(low) < 2 or low.isdigit():
        return False
    if low in _NON_TEAM_KEYWORDS or _SEQUENTIAL_SLOT_SIDE_RE.match(low):
        return False
    return True


def _parse_teams_from_text(raw: str) -> tuple[str, str, str]:
    """Parse `(league_hint, home, away)` from a single title string without inventing 1v1 pairs."""
    cleaned = re.sub(r"\s*[\(\[]\s*(?:diretta|live)\s*[\)\]]\s*", " ", raw or "", flags=re.IGNORECASE)
    cleaned = re.sub(r"^(?:diretta|live)\s*[:\-–]\s*", "", cleaned.strip(), flags=re.IGNORECASE)
    cleaned = _STAG_EP_TOKEN_RE.sub(" ", cleaned)
    cleaned = re.sub(r"\s+[-–—](?:\s*[-–—])+\s+", " - ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" -–—:")

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

    # Strip leading tournament + year prefix without colon (e.g. "CEV Champions League F 2026 Conegliano - Novara")
    m_comp_pref = _COMP_YEAR_PREFIX_RE.match(cleaned)
    if m_comp_pref and cleaned[m_comp_pref.end() :].strip():
        comp_pref = m_comp_pref.group(0).strip()
        league_hint = f"{league_hint} - {comp_pref}" if league_hint else comp_pref
        cleaned = cleaned[m_comp_pref.end() :].strip()

    # Handle multiple `-` with a competition prefix: e.g. "Serie A - Inter - Juventus"
    dash_parts = [p.strip() for p in re.split(r"\s+[-–—]\s+", cleaned) if p.strip()]
    if len(dash_parts) >= 3:
        cand_h = _strip_round_phase_suffixes(dash_parts[-2])
        cand_a = _strip_round_phase_suffixes(dash_parts[-1])
        if _is_valid_team_side(cand_h) and _is_valid_team_side(cand_a):
            comp_part = " - ".join(dash_parts[:-2])
            league_hint = f"{league_hint} - {comp_part}" if league_hint else comp_part
            return league_hint, cand_h, cand_a
        return league_hint, cleaned.strip(), ""

    parts = _TEAM_SPLIT_RE.split(cleaned, maxsplit=1)
    if len(parts) == 2 and parts[0].strip() and parts[1].strip():
        left = _strip_round_phase_suffixes(parts[0])
        right = _strip_round_phase_suffixes(parts[1])
        if _is_valid_team_side(left) and _is_valid_team_side(right):
            return league_hint, left, right

    return league_hint, cleaned.strip(), ""


def _extract_epg_teams(title: str, content_title: str = "") -> tuple[str, str, str]:
    """Return `(league_hint, home, away)`, preferring structured `content_title` when 1v1."""
    t_hint, t_h, t_a = _parse_teams_from_text(title)
    if content_title and content_title.strip() != (title or "").strip():
        ct_hint, ct_h, ct_a = _parse_teams_from_text(content_title)
        if ct_h and ct_a:
            return (ct_hint or t_hint), ct_h, ct_a
    return t_hint, t_h, t_a


def _extract_alt_epg_teams(epg: EPGProgram, primary_home: str, primary_away: str) -> tuple[str, str]:
    """Return alternate 1v1 pair from `epg.title` when `content_title` supplied a distinct pair."""
    if not epg.content_title or epg.content_title.strip() == (epg.title or "").strip():
        return "", ""
    _, t_h, t_a = _parse_teams_from_text(epg.title)
    if t_h and t_a and (t_h != primary_home or t_a != primary_away):
        return t_h, t_a
    return "", ""


def is_epg_match_compatible(epg: EPGProgram, match: Match) -> bool:
    """Enforce strict sport silo compatibility between an EPG program and a Match."""
    epg_silo = _infer_silo_from_epg(epg)
    football_family = {SportSilo.FOOTBALL, SportSilo.CALCIO_ITA}
    if epg_silo in football_family:
        return match.silo in football_family
    if epg_silo == SportSilo.ALTRI_SPORT:
        if match.silo == SportSilo.ALTRI_SPORT:
            return True
        combined = _strip_accents(f"{epg.sport_hint} {epg.category} {epg.channel_name} {epg.title} {epg.content_title}")
        return not bool(_ALTRI_SPORT_EXPLICIT_RE.search(combined))
    return epg_silo == match.silo


def _detect_epg_demographics(epg: EPGProgram, match: Optional[Match] = None) -> tuple[bool, bool]:
    """Detect `(is_women, is_youth)` across Phase A fields with U23 senior-league exception."""
    m_url_comp = re.search(r"/(?:sport|programmi-tv)/([^/]+)/", epg.content_url or "")
    url_comp_text = m_url_comp.group(1).replace("-", " ") if m_url_comp else ""
    comp_w, comp_y = detect_demographics(f"{epg.competition_hint} {url_comp_text}")
    title_w, title_y = detect_demographics_from_stream_title(
        f"{epg.title} {epg.content_title} {epg.description}"
    )
    explicit_f_w = bool(
        _COMP_WOMEN_F_RE.search(
            _strip_accents(f"{epg.title} {epg.content_title} {epg.competition_hint}")
        )
    )
    is_women = comp_w or title_w or explicit_f_w
    is_youth = comp_y or title_y

    if is_youth and match is not None and not match.is_youth and not comp_y:
        m_teams = f"{match.home_team.name if match.home_team else ''} {match.away_team.name if match.away_team else ''}"
        if _U23_BTEAM_RE.search(m_teams):
            is_youth = False

    return is_women, is_youth


def _is_fta_topdown_candidate(epg: EPGProgram) -> bool:
    """Return True if an FTA item with blank `programHighlights` may attach Top-Down onto Diretta."""
    if epg.program_highlights != "":
        return False
    is_fta = epg.channel_id in SKY_FTA_CHANNEL_ID_SET or _is_generalist_channel(
        epg.channel_name, epg.channel_id
    )
    if not is_fta:
        return False
    return not is_non_live_or_studio(
        epg.title,
        epg.description,
        epg_title=epg.epg_title,
        content_title=epg.content_title,
        content_url=epg.content_url,
        season_number=epg.season_number,
    )


def _base_competition_slug(slug: str) -> str:
    clean = (slug or "").strip().lower()
    for suffix in ("-women", "-femminile", "-youth"):
        if clean.endswith(suffix):
            clean = clean[: -len(suffix)]
            break
    if clean == "uefa-womens-champions-league":
        return "champions-league"
    return clean


def _competition_score_modifier(epg: EPGProgram, match: Match) -> float:
    """Return +0.05 on matching competition, -0.30 on certified slug conflict, or 0.0."""
    if epg.competition_hint and match.canonical_competition:
        lookup_silo = "football" if match.silo == SportSilo.CALCIO_ITA else match.silo.value
        lm = DEFAULT_LEAGUE_TAXONOMY.match_league(lookup_silo, epg.competition_hint, "")
        if lm is None:
            lm = DEFAULT_LEAGUE_TAXONOMY.match_league(
                lookup_silo, epg.competition_hint, "Italia"
            )
        if lm is not None:
            if _base_competition_slug(lm.canonical_slug) == _base_competition_slug(
                match.canonical_competition
            ):
                return 0.05
            return -0.30

    hint = _strip_accents(f"{epg.competition_hint} {epg.sport_hint}").lower().strip()
    if not hint:
        return 0.0
    m_comp = _strip_accents(
        f"{match.competition} {match.canonical_competition} {match.alt_league}"
    ).lower()
    if not m_comp:
        return 0.0
    hint_tokens = {t for t in re.findall(r"[a-z0-9]+", hint) if len(t) >= 3}
    comp_tokens = {t for t in re.findall(r"[a-z0-9]+", m_comp) if len(t) >= 3}
    if hint_tokens & comp_tokens:
        return 0.05
    return 0.0


def _time_proximity_bonus(match_ms: int, epg_ms: int, window_ms: int) -> float:
    """Return continuous [0.0, +0.03] tie-break favoring closer kickoff alignment."""
    if match_ms <= 0 or epg_ms <= 0 or window_ms <= 0:
        return 0.0
    ratio = max(0.0, 1.0 - abs(match_ms - epg_ms) / float(window_ms))
    return round(0.03 * ratio, 4)


def _epg_matches_diretta(epg: EPGProgram, match: Match) -> float:
    """Return match confidence score > 0 if `epg` represents `match`, else 0.0."""
    fta_fallback = False
    if not epg.is_live:
        if (
            match.home_team is not None
            and match.away_team is not None
            and _is_fta_topdown_candidate(epg)
        ):
            fta_fallback = True
        else:
            return 0.0

    if epg.is_simulcast or _is_simulcast_title(epg.title) or _is_simulcast_title(epg.content_title):
        return 0.0
    # Fast O(1) integer time-bounds guard before regex-based silo compatibility checks
    raw_diff = match.date - epg.start_ms
    if raw_diff < -_TENNIS_1V1_POST_MATCH_MS or (
        raw_diff > _TENNIS_1V1_PRE_MATCH_MS and match.date > epg.end_ms + _LINEAR_POST_MATCH_MS
    ):
        return 0.0
    if not is_epg_match_compatible(epg, match):
        return 0.0

    # Asymmetric linear TV window:
    # - Standard: [match.date - 60m, match.date + 30m] (or active overlap up to 90m pre-match)
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
        not fta_fallback
        and epg.start_ms <= match.date <= (epg.end_ms + _LINEAR_POST_MATCH_MS)
        and time_diff <= max(pre_ms, 90 * 60 * 1000)
    )
    if not in_asym_window and not overlaps:
        return 0.0

    comp_mod = _competition_score_modifier(epg, match)
    time_bonus = _time_proximity_bonus(match.date, epg.start_ms, max(pre_ms, 90 * 60 * 1000))

    _, epg_home, epg_away = _extract_epg_teams(epg.title, epg.content_title)
    epg_women, epg_youth = _detect_epg_demographics(epg, match=match)

    # Tennis Singles vs Doubles guard + Dynamic Tournament Session Bridge
    if match.silo == SportSilo.TENNIS:
        m_eval = f"{match.competition} {match.alt_league} {match.title}"
        ep_eval = f"{epg.competition_hint} {epg.title} {epg.content_title} {epg.description}"
        if _is_tennis_doubles(m_eval) != _is_tennis_doubles(ep_eval):
            return 0.0
        if not epg_away:
            # SuperTennis Guard: never bridge generic SuperTennis sessions to 1v1 matches
            if _is_supertennis_program(epg):
                return 0.0
            if is_non_live_or_studio(
                epg.title,
                epg.description,
                epg_title=epg.epg_title,
                content_title=epg.content_title,
                content_url=epg.content_url,
                season_number=epg.season_number,
            ):
                return 0.0
            if comp_mod < 0:
                return 0.0
            m_circ = _extract_tennis_circuit(m_eval)
            ep_circ = _extract_tennis_circuit(ep_eval)
            circ_ok = (m_circ == ep_circ) if (m_circ and ep_circ) else True
            ep_eval_low = re.sub(r"\s+", " ", _strip_accents(ep_eval).lower()).strip()
            if circ_ok and _tennis_tournament_matches_text(match, ep_eval_low):
                return 0.85 + comp_mod + time_bonus
            return 0.0

    # Pairwise team/athlete match
    if match.home_team and match.away_team and epg_home and epg_away:
        m_home = match.home_team.name
        m_away = match.away_team.name
        m_alt_home = match.alt_home
        m_alt_away = match.alt_away
        if match.is_women:
            m_home = _TRAILING_WOMEN_CODE_RE.sub("", m_home).strip()
            m_away = _TRAILING_WOMEN_CODE_RE.sub("", m_away).strip()
            m_alt_home = _TRAILING_WOMEN_CODE_RE.sub("", m_alt_home).strip()
            m_alt_away = _TRAILING_WOMEN_CODE_RE.sub("", m_alt_away).strip()
        alt_h2, alt_a2 = _extract_alt_epg_teams(epg, epg_home, epg_away)
        res = DEFAULT_TEAM_MATCHER.match_score(
            m_home,
            m_away,
            epg_home,
            epg_away,
            alt_home_1=m_alt_home,
            alt_away_1=m_alt_away,
            alt_home_2=alt_h2,
            alt_away_2=alt_a2,
            date_1_ms=match.date,
            date_2_ms=0,  # Sport-specific time windows (60m/120m/360m) already enforced above.
            is_women_1=match.is_women,
            is_youth_1=match.is_youth,
            is_women_2=epg_women,
            is_youth_2=epg_youth,
            allow_neutral_women_2=True,
        )
        if not res.matched:
            return 0.0
        adjusted = res.score + comp_mod
        if comp_mod < 0 and adjusted < DEFAULT_TEAM_MATCHER.avg_threshold:
            return 0.0
        return adjusted + time_bonus

    # Single-entity events (e.g. F1 / MotoGP / Boxing cards)
    if match.away_team is None and match.home_team is not None:
        epg_single = (epg.content_title or epg.title) if not epg_away else f"{epg_home} {epg_away}"
        alt_epg = (
            epg.title
            if (not epg_away and epg.content_title and epg.content_title != epg.title)
            else ""
        )
        res = DEFAULT_TEAM_MATCHER.match_score(
            match.home_team.name,
            "",
            epg_single,
            "",
            alt_home_1=match.alt_home or match.competition,
            alt_home_2=alt_epg,
            date_1_ms=match.date,
            date_2_ms=0,  # Sport-specific time windows (60m/120m/360m) already enforced above.
            is_women_1=match.is_women,
            is_youth_1=match.is_youth,
            is_women_2=epg_women,
            is_youth_2=epg_youth,
            silo=match.silo.value,
            competition_1=match.competition,
            competition_2=epg.competition_hint,
            allow_neutral_women_2=True,
        )
        if not res.matched:
            return 0.0
        adjusted = res.score + comp_mod
        if comp_mod < 0 and adjusted < DEFAULT_TEAM_MATCHER.single_entity_threshold:
            return 0.0
        return adjusted + time_bonus

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


def _is_allowlisted_orphan(
    epg: EPGProgram,
    diretta_tennis_matches: Optional[list[Match]] = None,
) -> bool:
    """Closed 6-category orphan allowlist: multicast, motori track, tennis multicourt, golf, padel, non-1v1 sports."""
    if not epg.is_live:
        return False
    if is_non_live_or_studio(
        epg.title,
        epg.description,
        epg_title=epg.epg_title,
        content_title=epg.content_title,
        content_url=epg.content_url,
        season_number=epg.season_number,
    ):
        return False

    ch_names = _split_channel_names(epg.channel_name)
    all_generalist = bool(ch_names) and all(
        _is_generalist_channel(ch, epg.channel_id) for ch in ch_names
    )
    if all_generalist:
        duration_min = (epg.end_ms - epg.start_ms) / 60000.0 if epg.end_ms > epg.start_ms else 0.0
        if 0 < duration_min < 25:
            return False
        if epg.description and _GENERALIST_STUDIO_DESC_RE.search(_strip_accents(epg.description)):
            return False

    # Category 1: Multicast / Simulcast containers (Diretta Gol, Zona DAZN, RedZone, Mosaico)
    if epg.is_simulcast or _is_simulcast_title(epg.title) or _is_simulcast_title(epg.content_title):
        return True

    # Strictly zero 1v1 orphan cards across Categories 2-6
    _, _, epg_away = _extract_epg_teams(epg.title, epg.content_title)
    if epg_away:
        return False

    silo = _infer_silo_from_epg(epg)
    eval_text = _strip_accents(
        f"{epg.sport_hint} {epg.competition_hint} {epg.title} {epg.content_title} {epg.content_url}"
    )

    # Category 2: Motori live track sessions (prove libere / FP1-3, qualifiche, sprint, gara, warm up)
    if silo == SportSilo.MOTORI:
        track_text = _strip_accents(f"{epg.title} {epg.content_title}")
        return bool(_MOTORI_TRACK_SESSION_RE.search(track_text))

    # Category 3: Tennis multicourt without 1v1 teams (active Diretta tournament + anti-replay >3.5h)
    if silo == SportSilo.TENNIS:
        if not diretta_tennis_matches:
            return False
        eval_low = re.sub(r"\s+", " ", eval_text.lower()).strip()
        matching_tournaments = [
            m for m in diretta_tennis_matches if _tennis_tournament_matches_text(m, eval_low)
        ]
        if not matching_tournaments:
            return False
        anti_replay_floor = epg.start_ms - _ROLLING_CACHE_WINDOW_MS
        return any(
            m.date >= anti_replay_floor
            or (m.end_date > 0 and m.end_date >= anti_replay_floor)
            for m in matching_tournaments
        )

    # Category 4: Golf live
    if _GOLF_ORPHAN_RE.search(f"{eval_text} {epg.channel_name}"):
        return True

    # Category 5: Padel live
    if _PADEL_ORPHAN_RE.search(eval_text):
        return True

    # Category 6: Recognized non-1v1 sports (Cycling, Skiing/Winter, Athletics, Swimming, Skating, Olympics)
    if _RECOGNIZED_NON_1V1_SPORT_RE.search(eval_text):
        return True

    return False


def reconcile_with_schedule(
    diretta_matches: list[Match],
    epg_programs: list[EPGProgram],
) -> tuple[list[Match], list[Match]]:
    """Enrich `diretta_matches` with EPG broadcasters and build closed-allowlist orphan `Match` objects."""
    broadcaster_updates: dict[int, list[str]] = {
        idx: list(m.broadcasters) for idx, m in enumerate(diretta_matches)
    }
    end_date_updates: dict[int, int] = {
        idx: m.end_date for idx, m in enumerate(diretta_matches)
    }
    fallback_poster_updates: dict[int, str] = {
        idx: m.fallback_poster for idx, m in enumerate(diretta_matches)
    }
    football_family = (SportSilo.FOOTBALL, SportSilo.CALCIO_ITA)
    diretta_by_silo: dict[SportSilo, list[tuple[int, Match]]] = {}
    for idx, m in enumerate(diretta_matches):
        diretta_by_silo.setdefault(m.silo, []).append((idx, m))
    all_indexed: list[tuple[int, Match]] = list(enumerate(diretta_matches))
    football_indexed = [
        *diretta_by_silo.get(SportSilo.FOOTBALL, []),
        *diretta_by_silo.get(SportSilo.CALCIO_ITA, []),
    ]
    diretta_tennis_matches = [m for _, m in diretta_by_silo.get(SportSilo.TENNIS, [])]

    orphan_matches: list[Match] = []
    seen_orphan_ids: set[str] = set()
    discarded_orphans = 0

    for epg in epg_programs:
        epg_channels = _split_channel_names(epg.channel_name)
        is_sim = epg.is_simulcast or _is_simulcast_title(epg.title) or _is_simulcast_title(epg.content_title)
        best_idx: Optional[int] = None
        best_score = 0.0

        if not is_sim and (epg.is_live or _is_fta_topdown_candidate(epg)):
            epg_silo = _infer_silo_from_epg(epg)
            if epg_silo in football_family:
                candidates = football_indexed
            elif epg_silo == SportSilo.ALTRI_SPORT:
                combined_altri = _strip_accents(
                    f"{epg.sport_hint} {epg.category} {epg.channel_name} {epg.title} {epg.content_title}"
                )
                candidates = (
                    diretta_by_silo.get(SportSilo.ALTRI_SPORT, [])
                    if _ALTRI_SPORT_EXPLICIT_RE.search(combined_altri)
                    else all_indexed
                )
            else:
                candidates = diretta_by_silo.get(epg_silo, [])

            for idx, match in candidates:
                score = _epg_matches_diretta(epg, match)
                if score > best_score:
                    best_score = score
                    best_idx = idx

        if best_idx is not None:
            broadcaster_updates[best_idx] = _merge_broadcasters(
                broadcaster_updates[best_idx], epg_channels
            )
            if epg.poster_url and not fallback_poster_updates[best_idx]:
                fallback_poster_updates[best_idx] = epg.poster_url
            if epg.end_ms > 0:
                target_end = epg.end_ms - (DIRETTA_POST_MATCH_MS - SKY_EPG_POST_END_MS)
                if target_end > end_date_updates[best_idx]:
                    end_date_updates[best_idx] = target_end
            continue

        if not _is_allowlisted_orphan(epg, diretta_tennis_matches=diretta_tennis_matches):
            discarded_orphans += 1
            continue

        # Build candidate orphan EPG Match for closed-allowlist live non-1v1 programs.
        silo = _infer_silo_from_epg(epg)
        league_hint, home_name, away_name = _extract_epg_teams(epg.title, epg.content_title)
        comp_text = epg.competition_hint or league_hint or epg.sport_hint or epg.category
        is_women, is_youth = _detect_epg_demographics(epg)

        category, genres = DEFAULT_GENRE_CLASSIFIER.classify_event(
            silo=silo.value,
            competition=comp_text,
            title=epg.title,
            is_women=is_women,
            is_youth=is_youth,
        )

        slug = _slugify(epg.title)
        orphan_id = f"sky_{epg.channel_id}_{slug}_{epg.start_ms}"
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

    enriched: list[Match] = []
    for idx, m in enumerate(diretta_matches):
        updates: dict[str, Any] = {}
        if broadcaster_updates[idx] != m.broadcasters:
            updates["broadcasters"] = broadcaster_updates[idx]
        if end_date_updates[idx] != m.end_date:
            updates["end_date"] = end_date_updates[idx]
        if fallback_poster_updates[idx] != m.fallback_poster:
            updates["fallback_poster"] = fallback_poster_updates[idx]
        enriched.append(m.model_copy(update=updates) if updates else m)
    return enriched, orphan_matches


def filter_ghost_epg_matches(matches: list[Match]) -> list[Match]:
    """Discard EPG-derived orphan matches that have zero attached playable streams."""
    return [m for m in matches if m.source_origin != "epg" or bool(m.sources)]


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
                is_container_or_studio = (
                    ep.is_simulcast
                    or _is_simulcast_title(ep.title)
                    or _is_simulcast_title(ep.content_title)
                    or is_non_live_or_studio(
                        ep.title,
                        ep.description,
                        epg_title=ep.epg_title,
                        content_title=ep.content_title,
                        content_url=ep.content_url,
                        season_number=ep.season_number,
                    )
                )
                if is_container_or_studio:
                    if canon in ("dazn 1", "dazn 2", "dazn 3", "dazn 4") and not _is_simulcast_title(m.title):
                        conflicting_ep = conflicting_ep or ep
                        continue
                    has_match_or_ambiguous = True
                    break

                if _epg_matches_diretta(ep, m) > 0:
                    has_match_or_ambiguous = True
                    break

                ep_clean = _normalize_simulcast_title(ep.content_title or ep.title)
                ep_clean_alt = _normalize_simulcast_title(ep.title) if ep.content_title else ""
                ep_tokens = {t for t in f"{ep_clean} {ep_clean_alt}".split() if len(t) >= 3}
                if m_tokens & ep_tokens:
                    has_match_or_ambiguous = True
                    break
                if m_title_clean and (
                    (ep_clean and fuzz.token_set_ratio(m_title_clean, ep_clean) >= 55)
                    or (ep_clean_alt and fuzz.token_set_ratio(m_title_clean, ep_clean_alt) >= 55)
                ):
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


def _should_ingest_sky_event(
    *,
    channel_id: str,
    genre_name: str,
    subgenre_name: str,
    title: str,
    content_title: str,
    description: str,
) -> bool:
    """Upstream non-destructive gate filtering non-sports filler while preserving Rai Sport / SuperTennis."""
    if channel_id in SKY_FTA_SPORTS_CHANNEL_IDS:
        return True
    g_low = _strip_accents(genre_name or "").strip().lower()
    sg_low = _strip_accents(subgenre_name or "").strip().lower()
    if channel_id in SKY_PAY_CHANNEL_ID_SET:
        if g_low and g_low != "sport":
            return False
        return True
    if g_low == "sport":
        return True
    if g_low in _NON_SPORTS_GENRES or sg_low in _NON_SPORTS_SUBGENRES:
        return False
    combined = _strip_accents(f"{title} {content_title} {description}")
    return bool(_SPORTS_EVENT_HINT_RE.search(combined))


class ItalianEPGService:
    """Async EPG ingestion service for Sky Italia Guida TV."""

    def __init__(
        self,
        settings: Optional[Settings] = None,
        client: Optional[httpx.AsyncClient] = None,
    ) -> None:
        self._settings = settings or get_settings()
        self._client = client
        self._owned_client: Optional[httpx.AsyncClient] = None
        self._sky_day_cache: dict[str, tuple[float, list[EPGProgram]]] = {}
        self._sky_future_cache: dict[int, tuple[str, float, list[EPGProgram]]] = {}
        self._rolling_cache: list[EPGProgram] = []
        self.last_fetch_failed: bool = False

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
        *,
        channel_id: str,
        title: str,
        epg_title: str,
        content_title: str,
        description: str,
        content_url: str,
        program_highlights: str,
        season_number: Optional[int],
        current_year: int,
    ) -> bool:
        url_low = content_url.lower()
        if "/replica" in url_low or "/differita" in url_low:
            return False
        if season_number is not None and season_number < current_year:
            return False
        if is_non_live_or_studio(
            title,
            description,
            epg_title=epg_title,
            content_title=content_title,
            content_url=content_url,
            season_number=season_number,
            current_year=current_year,
        ):
            return False
        if program_highlights == "L":
            return True
        if program_highlights != "":
            return False
        if "/live-" in url_low or "/diretta-" in url_low:
            return True
        if channel_id in SKY_FTA_CHANNEL_ID_SET:
            if re.search(r"\b(?:live|diretta)\b", f"{title} {content_title}", re.I):
                return True
        return False

    async def _fetch_sky_channel_group(
        self,
        client: httpx.AsyncClient,
        *,
        channel_ids: tuple[str, ...],
        from_dt: datetime,
        to_dt: datetime,
        now_utc: datetime,
        allow_422_retry: bool = True,
    ) -> tuple[list[EPGProgram], bool]:
        """Fetch paginated Sky Guida TV events for one channel group (Pay-TV or FTA)."""
        if not channel_ids:
            return [], False
        from_iso = from_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        to_iso = to_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        ch_ids = ",".join(channel_ids)
        now_ms = int(now_utc.timestamp() * 1000)
        ref_year = now_utc.year

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
            except httpx.HTTPStatusError as exc:
                if (
                    exc.response is not None
                    and exc.response.status_code == 422
                    and page_num == 0
                    and allow_422_retry
                    and from_dt < now_utc
                ):
                    logger.warning(
                        "Sky EPG returned HTTP 422 for from=%s; retrying once from now_utc=%s",
                        from_iso,
                        now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    )
                    fallback_to = (
                        to_dt if to_dt > now_utc else (now_utc + timedelta(hours=1))
                    )
                    return await self._fetch_sky_channel_group(
                        client,
                        channel_ids=channel_ids,
                        from_dt=now_utc,
                        to_dt=fallback_to,
                        now_utc=now_utc,
                        allow_422_retry=False,
                    )
                logger.warning("Sky EPG fetch failed (page=%s): %s", page_num, exc)
                fetch_failed = page_num == 0
                break
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
                raw_event_title = (ev.get("eventTitle") or "").strip()
                raw_epg_title = (ev.get("epgEventTitle") or "").strip()
                raw_content_title = (content.get("contentTitle") or "").strip()
                title = raw_event_title or raw_epg_title or raw_content_title
                if not title:
                    continue
                epg_title = (
                    raw_epg_title if raw_epg_title and raw_epg_title != title else ""
                )
                content_title = (
                    raw_content_title
                    if raw_content_title and raw_content_title != title
                    else ""
                )

                description = (ev.get("eventSynopsis") or "").strip()
                genre_name = ((content.get("genre") or {}).get("name") or "").strip()
                subgenre_name = (
                    (content.get("subgenre") or {}).get("name") or ""
                ).strip()

                if not _should_ingest_sky_event(
                    channel_id=ch_id,
                    genre_name=genre_name,
                    subgenre_name=subgenre_name,
                    title=title,
                    content_title=content_title,
                    description=description,
                ):
                    continue

                start_ms = _parse_iso_to_ms(ev.get("starttime", ""))
                end_ms = _parse_iso_to_ms(ev.get("endtime", ""))
                if start_ms <= 0 or end_ms <= start_ms:
                    continue
                if now_ms > end_ms + SKY_EPG_POST_END_MS:
                    continue

                content_url = str(content.get("url") or ev.get("url") or "").strip()
                raw_hl = ev.get("programHighlights")
                program_highlights = (
                    str(raw_hl).strip().upper() if raw_hl is not None else ""
                )
                raw_season = content.get("seasonNumber")
                season_number: Optional[int] = None
                if isinstance(raw_season, int) and not isinstance(raw_season, bool):
                    season_number = raw_season
                elif isinstance(raw_season, str) and raw_season.strip().isdigit():
                    season_number = int(raw_season.strip())

                is_dedicated_sports = ch_id in SKY_SPORTS_CHANNEL_IDS
                competition_hint = extract_competition_from_sky_url(content_url) or ""
                is_live = self._compute_sky_is_live(
                    channel_id=ch_id,
                    title=title,
                    epg_title=epg_title,
                    content_title=content_title,
                    description=description,
                    content_url=content_url,
                    program_highlights=program_highlights,
                    season_number=season_number,
                    current_year=ref_year,
                )
                poster_url = _extract_sky_poster_url(content)
                ev_id = str(ev.get("eventId") or f"{ch_id}_{start_ms}")

                programs.append(
                    EPGProgram(
                        id=f"sky_{ev_id}",
                        channel_id=ch_id,
                        channel_name=ch_name,
                        is_dedicated_sports=is_dedicated_sports,
                        title=title,
                        epg_title=epg_title,
                        content_title=content_title,
                        description=description,
                        category=genre_name,
                        start_ms=start_ms,
                        end_ms=end_ms,
                        sport_hint=subgenre_name,
                        content_url=content_url,
                        program_highlights=program_highlights,
                        season_number=season_number,
                        competition_hint=competition_hint,
                        poster_url=poster_url,
                        is_live=is_live,
                        is_simulcast=_is_simulcast_title(title) or _is_simulcast_title(content_title),
                        source="sky",
                    )
                )

            total = int(payload.get("total") or 0)
            if len(events) < page_size or (total and (page_num + 1) * page_size >= total):
                break
            page_num += 1

        return programs, fetch_failed

    async def _fetch_sky_window(
        self,
        client: httpx.AsyncClient,
        *,
        from_dt: datetime,
        to_dt: datetime,
        now_utc: datetime,
        allow_422_retry: bool = True,
    ) -> tuple[list[EPGProgram], bool]:
        """Fetch Pay-TV and FTA Sky Guida TV groups in parallel for `[from_dt, to_dt]`."""
        (pay_progs, pay_failed), (fta_progs, fta_failed) = await asyncio.gather(
            self._fetch_sky_channel_group(
                client,
                channel_ids=SKY_PAY_CHANNEL_IDS,
                from_dt=from_dt,
                to_dt=to_dt,
                now_utc=now_utc,
                allow_422_retry=allow_422_retry,
            ),
            self._fetch_sky_channel_group(
                client,
                channel_ids=SKY_FTA_CHANNEL_IDS,
                from_dt=from_dt,
                to_dt=to_dt,
                now_utc=now_utc,
                allow_422_retry=allow_422_retry,
            ),
        )
        combined: list[EPGProgram] = []
        seen_ids: set[str] = set()
        for prog in (*pay_progs, *fta_progs):
            if prog.id not in seen_ids:
                seen_ids.add(prog.id)
                combined.append(prog)
        fetch_failed = pay_failed and fta_failed
        if fetch_failed:
            return [], True
        return (_merge_and_handover_channel_blocks(combined) if combined else []), False

    async def _fetch_sky_with_client(
        self,
        client: httpx.AsyncClient,
        *,
        requested_from: Optional[datetime] = None,
        days: tuple[int, ...] = SKY_FEED_DAYS,
        now_utc: Optional[datetime] = None,
    ) -> list[EPGProgram]:
        if not self._settings.ENABLE_SKY_EPG:
            self.last_fetch_failed = False
            return []

        now = _ensure_utc(now_utc or datetime.now(timezone.utc))
        ref_ts = now.timestamp()
        now_ms = int(ref_ts * 1000)
        local_now = now.astimezone(_ROME_TZ)
        today_local = local_now.date()
        today_key = today_local.isoformat()
        prev_day_key = (today_local - timedelta(days=1)).isoformat()
        logical_day = _sky_logical_rome_day(local_now)

        ordered_days = sorted(dict.fromkeys(days))
        merged_programs: list[EPGProgram] = []
        seen_ids: set[str] = set()
        attempted_buckets = 0
        failed_buckets = 0

        for day_offset in ordered_days:
            attempted_buckets += 1
            bucket_progs: list[EPGProgram] = []
            if day_offset == 0:
                raw_from = (
                    requested_from
                    if requested_from is not None
                    else now - _SKY_MAX_LOOKBACK
                )
                from_dt = clamp_sky_from(raw_from, now)
                _, day0_end_utc = _rome_day_bounds_utc(today_local)
                to_dt = (
                    day0_end_utc
                    if day0_end_utc > from_dt
                    else (from_dt + timedelta(hours=1))
                )
                day0_progs, failed = await self._fetch_sky_window(
                    client,
                    from_dt=from_dt,
                    to_dt=to_dt,
                    now_utc=now,
                    allow_422_retry=True,
                )
                if day0_progs:
                    self._sky_day_cache[today_key] = (ref_ts, day0_progs)
                    expired_keys = [
                        k
                        for k, (ts, _) in self._sky_day_cache.items()
                        if (ref_ts - ts) > _SKY_OUTAGE_CACHE_TTL_SECONDS
                    ]
                    for k in expired_keys:
                        self._sky_day_cache.pop(k, None)
                    bucket_progs = day0_progs
                elif failed:
                    for candidate_key in (today_key, prev_day_key):
                        cached = self._sky_day_cache.get(candidate_key)
                        if (
                            cached
                            and (ref_ts - cached[0]) <= _SKY_OUTAGE_CACHE_TTL_SECONDS
                        ):
                            still_valid = [
                                p
                                for p in cached[1]
                                if now_ms <= p.end_ms + SKY_EPG_POST_END_MS
                            ]
                            if still_valid:
                                logger.warning(
                                    "Sky EPG fetch failed; serving %d still-valid programs from %s snapshot",
                                    len(still_valid),
                                    candidate_key,
                                )
                                bucket_progs = still_valid
                                break
                    if not bucket_progs:
                        failed_buckets += 1
            elif day_offset > 0:
                cached_fut = self._sky_future_cache.get(day_offset)
                if (
                    cached_fut is not None
                    and cached_fut[0] == logical_day
                    and (ref_ts - cached_fut[1]) < _SKY_FUTURE_BUCKET_TTL_SECONDS
                ):
                    bucket_progs = [
                        p
                        for p in cached_fut[2]
                        if now_ms <= p.end_ms + SKY_EPG_POST_END_MS
                    ]
                else:
                    target_date = today_local + timedelta(days=day_offset)
                    start_utc, end_utc = _rome_day_bounds_utc(target_date)
                    fut_progs, failed = await self._fetch_sky_window(
                        client,
                        from_dt=start_utc,
                        to_dt=end_utc,
                        now_utc=now,
                        allow_422_retry=False,
                    )
                    if not failed:
                        self._sky_future_cache[day_offset] = (
                            logical_day,
                            ref_ts,
                            fut_progs,
                        )
                        bucket_progs = fut_progs
                    elif cached_fut is not None:
                        bucket_progs = [
                            p
                            for p in cached_fut[2]
                            if now_ms <= p.end_ms + SKY_EPG_POST_END_MS
                        ]
                    if failed and not bucket_progs:
                        failed_buckets += 1

            for prog in bucket_progs:
                if prog.id not in seen_ids and now_ms <= prog.end_ms + SKY_EPG_POST_END_MS:
                    seen_ids.add(prog.id)
                    merged_programs.append(prog)

        self.last_fetch_failed = (
            attempted_buckets > 0
            and failed_buckets == attempted_buckets
            and not merged_programs
        )
        return merged_programs

    async def fetch_sky_epg(
        self,
        *,
        requested_from: Optional[datetime] = None,
        days: tuple[int, ...] = SKY_FEED_DAYS,
        now_utc: Optional[datetime] = None,
    ) -> list[EPGProgram]:
        """Fetch raw sports EPG items from Sky Italia REST API by per-day buckets (3h lookback clamp)."""
        if not self._settings.ENABLE_SKY_EPG:
            return []
        return await self._fetch_sky_with_client(
            self._get_client(),
            requested_from=requested_from,
            days=days,
            now_utc=now_utc,
        )

    def _apply_rolling_cache(
        self,
        fresh_programs: list[EPGProgram],
        *,
        now_ms: int,
    ) -> list[EPGProgram]:
        """Retain still-active/recent programs across midnight transitions without time-shift duplicates."""
        active_fresh = [
            p
            for p in fresh_programs
            if p.end_ms <= 0 or now_ms <= p.end_ms + SKY_EPG_POST_END_MS
        ]
        if not self._rolling_cache:
            self._rolling_cache = list(active_fresh)
            return active_fresh

        fresh_ids = {p.id for p in active_fresh}
        retained: list[EPGProgram] = []
        for prev in self._rolling_cache:
            if prev.id in fresh_ids:
                continue
            if prev.start_ms <= 0 or (now_ms - prev.start_ms) >= _ROLLING_CACHE_WINDOW_MS:
                continue
            if prev.end_ms > 0 and now_ms > prev.end_ms + SKY_EPG_POST_END_MS:
                continue

            prev_canon = canonicalize_channel_name(prev.channel_name, country="Italy")
            prev_norm = _normalize_simulcast_title(prev.title)
            is_time_shifted = False
            for curr in active_fresh:
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

        combined = [*retained, *active_fresh]
        self._rolling_cache = combined
        return combined

    async def fetch_epg(
        self,
        active_tennis_cities: frozenset[str] = frozenset(),
        *,
        requested_from: Optional[datetime] = None,
        days: tuple[int, ...] = SKY_FEED_DAYS,
        now_utc: Optional[datetime] = None,
    ) -> list[EPGProgram]:
        """Fetch Sky Italia EPG, filter studio/replays/out-of-calendar tennis, and merge simulcasts."""
        ref_now = _ensure_utc(now_utc or datetime.now(timezone.utc))
        now_ms = int(ref_now.timestamp() * 1000)
        client = self._get_client()

        sky_items = await self._fetch_sky_with_client(
            client,
            requested_from=requested_from,
            days=days,
            now_utc=ref_now,
        )
        filtered: list[EPGProgram] = []
        for prog in sky_items:
            if is_non_live_or_studio(
                prog.title,
                prog.description,
                epg_title=prog.epg_title,
                content_title=prog.content_title,
                content_url=prog.content_url,
                season_number=prog.season_number,
                current_year=ref_now.year,
            ):
                continue
            tennis_eval_title = (
                f"{prog.title} {prog.content_title}".strip()
                if prog.content_title
                else prog.title
            )
            if is_out_of_calendar_tennis(
                tennis_eval_title,
                prog.sport_hint or prog.channel_name,
                active_tennis_cities,
                current_year=ref_now.year,
            ):
                continue
            filtered.append(prog)

        deduped = deduplicate_simulcasts(filtered)
        result = self._apply_rolling_cache(deduped, now_ms=now_ms)
        if result:
            self.last_fetch_failed = False
        return result
