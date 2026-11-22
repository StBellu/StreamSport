"""Italian linear TV EPG ingestion, filtering, simulcast deduplication, and reconciliation."""

from __future__ import annotations

import asyncio
import logging
import re
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
_TEAM_SPLIT_RE = re.compile(r"\s+(?:vs\.?|v\.?|-|–|—)\s+", re.IGNORECASE)


def _strip_accents(text: str) -> str:
    norm = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in norm if not unicodedata.combining(ch))


def _ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def clamp_sky_from(requested_from: datetime, now_utc: datetime) -> datetime:
    """Clamp Sky API `from` parameter to at most 165 minutes in the past (prevents HTTP 422)."""
    req = _ensure_utc(requested_from)
    now = _ensure_utc(now_utc)
    min_allowed = now - _SKY_MAX_LOOKBACK
    return req if req >= min_allowed else min_allowed


def is_non_live_or_studio(title: str, description: str = "") -> bool:
    """Return True if the EPG entry is a studio show, news, magazine, highlights, or replay."""
    clean_title = _strip_accents((title or "").strip())
    if not clean_title:
        return True
    if _NON_LIVE_TITLE_RE.search(clean_title):
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
        merged.append(
            primary.model_copy(
                update={
                    "channel_name": ", ".join(channels),
                    "start_ms": min(g.start_ms for g in group),
                    "end_ms": max(g.end_ms for g in group),
                    "description": best_desc,
                    "category": best_cat,
                    "sport_hint": best_hint,
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
        f"{epg.sport_hint} {epg.category} {epg.channel_name} {epg.title}"
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


def _extract_epg_teams(title: str) -> tuple[str, str, str]:
    """Return `(league_hint, home, away)` parsed from an EPG program title."""
    cleaned = re.sub(r"\s*[\(\[]\s*(?:diretta|live)\s*[\)\]]\s*", " ", title or "", flags=re.IGNORECASE)
    cleaned = re.sub(r"^(?:diretta|live)\s*[:\-–]\s*", "", cleaned.strip(), flags=re.IGNORECASE)

    league_hint = ""
    if ":" in cleaned:
        prefix, rest = cleaned.split(":", 1)
        if rest.strip():
            league_hint = prefix.strip()
            cleaned = rest.strip()

    parts = _TEAM_SPLIT_RE.split(cleaned, maxsplit=1)
    if len(parts) == 2 and parts[0].strip() and parts[1].strip():
        return league_hint, parts[0].strip(), parts[1].strip()
    return league_hint, cleaned.strip(), ""


def _epg_matches_diretta(epg: EPGProgram, match: Match) -> float:
    """Return match confidence score > 0 if `epg` represents `match`, else 0.0."""
    # Time guard: check kickoff proximity (60m) or active broadcast overlap.
    start_diff = abs(match.date - epg.start_ms)
    overlaps = epg.start_ms <= match.date <= epg.end_ms
    if start_diff > _RECONCILE_WINDOW_MS and not overlaps:
        return 0.0

    _, epg_home, epg_away = _extract_epg_teams(epg.title)
    epg_women, epg_youth = detect_demographics_from_stream_title(epg.title)

    # Pairwise team/athlete match
    if match.home_team and match.away_team and epg_home and epg_away:
        res = DEFAULT_TEAM_MATCHER.match_score(
            match.home_team.name,
            match.away_team.name,
            epg_home,
            epg_away,
            alt_home_1=match.alt_home,
            alt_away_1=match.alt_away,
            date_1_ms=match.date,
            date_2_ms=epg.start_ms if start_diff <= _RECONCILE_WINDOW_MS else 0,
            is_women_1=match.is_women,
            is_youth_1=match.is_youth,
            is_women_2=epg_women,
            is_youth_2=epg_youth,
        )
        return res.score if res.matched else 0.0

    # Single-entity events (e.g. F1 / MotoGP / Boxing cards)
    if match.away_team is None and match.home_team is not None:
        epg_single = epg.title if not epg_away else f"{epg_home} {epg_away}"
        res = DEFAULT_TEAM_MATCHER.match_score(
            match.home_team.name,
            "",
            epg_single,
            "",
            alt_home_1=match.alt_home or match.competition,
            date_1_ms=match.date,
            date_2_ms=epg.start_ms if start_diff <= _RECONCILE_WINDOW_MS else 0,
            is_women_1=match.is_women,
            is_youth_1=match.is_youth,
            is_women_2=epg_women,
            is_youth_2=epg_youth,
        )
        return res.score if res.matched else 0.0

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


def reconcile_with_schedule(
    diretta_matches: list[Match],
    epg_programs: list[EPGProgram],
) -> tuple[list[Match], list[Match]]:
    """Enrich `diretta_matches` with EPG broadcasters and build orphan EPG `Match` objects."""
    broadcaster_updates: dict[int, list[str]] = {
        idx: list(m.broadcasters) for idx, m in enumerate(diretta_matches)
    }
    orphan_matches: list[Match] = []
    seen_orphan_ids: set[str] = set()

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

        # Build candidate orphan EPG Match for unmatched live sports programs.
        silo = _infer_silo_from_epg(epg)
        league_hint, home_name, away_name = _extract_epg_teams(epg.title)
        comp_text = league_hint or epg.sport_hint or epg.category
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
                competition=comp_text,
                home_team=home_team,
                away_team=away_team,
                is_women=is_women,
                is_youth=is_youth,
                broadcasters=epg_channels,
                source_origin="epg",
            )
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

        from_iso = from_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        to_iso = to_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        ch_ids = ",".join(SKY_CHANNELS.keys())

        programs: list[EPGProgram] = []
        page_num = 0
        page_size = 999

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
                break

            events = payload.get("events") or []
            if not events:
                break

            for ev in events:
                ch_info = ev.get("channel") or {}
                ch_id = str(ch_info.get("id", "")).strip()
                if not ch_id or ch_id not in SKY_CHANNELS:
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

                description = (ev.get("eventSynopsis") or "").strip()
                genre_name = ((content.get("genre") or {}).get("name") or "").strip()
                subgenre_name = ((content.get("subgenre") or {}).get("name") or "").strip()

                if not _is_sports_program(
                    is_dedicated_sports_channel=ch_id in SKY_SPORTS_CHANNEL_IDS,
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

                ev_id = str(ev.get("eventId") or f"{ch_id}_{start_ms}")
                programs.append(
                    EPGProgram(
                        id=f"sky_{ev_id}",
                        channel_id=ch_id,
                        channel_name=SKY_CHANNELS[ch_id],
                        title=title,
                        description=description,
                        category=genre_name,
                        start_ms=start_ms,
                        end_ms=end_ms,
                        sport_hint=subgenre_name,
                        is_live=True,
                        source="sky",
                    )
                )

            total = int(payload.get("total") or 0)
            if len(events) < page_size or (total and (page_num + 1) * page_size >= total):
                break
            page_num += 1

        return programs

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
        if self._client is not None:
            return await self._fetch_sky_with_client(
                self._client,
                requested_from=requested_from,
                hours_ahead=hours_ahead,
                now_utc=now_utc,
            )
        async with create_standard_client(timeout=20.0) as client:
            return await self._fetch_sky_with_client(
                client,
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
        canonical_channel = TIVUSAT_CHANNELS[ch_id]
        is_sports_ch = ch_id in TIVUSAT_SPORTS_CHANNEL_IDS
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
                    is_live=True,
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

        coros = [
            self._fetch_tivusat_channel_day(client, ch_id, date_str)
            for ch_id in TIVUSAT_CHANNELS
            for date_str in dates
        ]
        results = await asyncio.gather(*coros)
        items: list[EPGProgram] = []
        for batch in results:
            items.extend(batch)
        return items

    async def fetch_tivusat_epg(
        self,
        *,
        days_ahead: int = 1,
        now_utc: Optional[datetime] = None,
    ) -> list[EPGProgram]:
        """Fetch raw sports EPG items from Tivùsat Guida TV REST API."""
        if not self._settings.ENABLE_TIVUSAT_EPG:
            return []
        if self._client is not None:
            return await self._fetch_tivusat_with_client(
                self._client,
                days_ahead=days_ahead,
                now_utc=now_utc,
            )
        async with create_standard_client(timeout=20.0) as client:
            return await self._fetch_tivusat_with_client(
                client,
                days_ahead=days_ahead,
                now_utc=now_utc,
            )

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

        async def _gather(client: httpx.AsyncClient) -> list[EPGProgram]:
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
                if is_non_live_or_studio(prog.title, prog.description):
                    continue
                if is_out_of_calendar_tennis(
                    prog.title,
                    prog.sport_hint or prog.channel_name,
                    active_tennis_cities,
                    current_year=ref_now.year,
                ):
                    continue
                filtered.append(prog)
            return deduplicate_simulcasts(filtered)

        if self._client is not None:
            return await _gather(self._client)

        async with create_standard_client(timeout=20.0) as client:
            return await _gather(client)
