"""Official sports schedule ingestion from Flashscore / Diretta.it feeds."""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
import unicodedata
from datetime import datetime, timezone
from typing import Any, Iterable, NamedTuple, Optional

import httpx

from app.config import Settings, get_settings
from app.core.network import DEFAULT_USER_AGENT, create_standard_client
from app.models.domain import Match, SportSilo, TeamInfo
from app.taxonomy import (
    DEFAULT_GENRE_CLASSIFIER,
    DEFAULT_LEAGUE_TAXONOMY,
    detect_demographics,
    normalize_country,
)

logger = logging.getLogger(__name__)


class SportFeedSpec(NamedTuple):
    """Mapping between Flashscore sport ID, canonical silo, and default duration."""

    silo: SportSilo
    duration_minutes: int


SPORT_IDS: dict[int, SportFeedSpec] = {
    1: SportFeedSpec(SportSilo.FOOTBALL, 135),
    2: SportFeedSpec(SportSilo.TENNIS, 240),
    3: SportFeedSpec(SportSilo.BASKET, 150),
    4: SportFeedSpec(SportSilo.HOCKEY, 160),
    5: SportFeedSpec(SportSilo.NFL, 195),
    6: SportFeedSpec(SportSilo.BASEBALL, 195),
    7: SportFeedSpec(SportSilo.HANDBALL, 120),
    8: SportFeedSpec(SportSilo.RUGBY, 130),
    12: SportFeedSpec(SportSilo.VOLLEY, 135),
    16: SportFeedSpec(SportSilo.COMBAT, 180),  # Boxe
    28: SportFeedSpec(SportSilo.COMBAT, 180),  # MMA
    32: SportFeedSpec(SportSilo.MOTORI, 180),  # Formula 1
    33: SportFeedSpec(SportSilo.MOTORI, 180),  # MotoGP
}

FEED_DAYS: tuple[int, ...] = (-1, 0, 1, 2)
IT_FEED_URL = "https://400.flashscore.ninja/400/x/feed/f_{sport_id}_{day}_3_it_1"
EN_FEED_URL = "https://2.flashscore.ninja/2/x/feed/f_{sport_id}_{day}_3_en-uk_1"
OTT_EXCLUDE: tuple[str, ...] = (
    "now tv",
    "sky go",
    "amazon prime",
    "raiplay",
    "mediaset infinity",
    "timvision",
    "discovery+",
    "onefootball",
    "uefa.tv",
    "youtube",
    "twitch",
    "apple tv",
    "dazn app",
)

_BROADCASTER_REGION_SUFFIX_RE = re.compile(
    r"\s*\((?:ita|it|eng|uk|hd)\)\s*$",
    re.IGNORECASE,
)
_NUMERIC_TRAILING_ID_RE = re.compile(r"(?:-\d+)+$")
_INITIAL_TOKEN_RE = re.compile(r"^[A-Za-zÀ-ÖØ-öø-ÿ]\.?$")
_TENNIS_SURFACE_RE = re.compile(r",\s*[^,]+$")
_TENNIS_PARENS_RE = re.compile(r"\([^)]*\)")
_TENNIS_CIRCUIT_PREFIX_RE = re.compile(
    r"^(?:atp|wta|challenger|itf|davis\s+cup|coppa\s+davis|bjk\s+cup|billie\s+jean\s+king\s+cup|"
    r"united\s+cup|laver\s+cup|esibizione|exhibition)"
    r"(?:\s+(?:uomini|donne|men|women))?"
    r"(?:\s*-\s*(?:singolare|doppio|singles|doubles|gruppo\s+mondiale|world\s+group))?\s*:\s*",
    re.IGNORECASE,
)
_TENNIS_CITY_STOPWORDS: frozenset[str] = frozenset(
    {
        "atp",
        "wta",
        "challenger",
        "finals",
        "open",
        "cup",
        "tour",
        "singolare",
        "doppio",
        "singles",
        "doubles",
        "uomini",
        "donne",
        "men",
        "women",
        "play offs",
        "playoffs",
        "qualificazioni",
        "qualification",
        "gruppo mondiale",
        "world group",
        "relegation",
        "mondo",
        "world",
        "masters",
        "250",
        "500",
        "1000",
        "125",
    }
)


def _strip_accents(text: str) -> str:
    norm = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in norm if not unicodedata.combining(ch))


def _clean_broadcaster_name(raw_name: str) -> str:
    cleaned = _BROADCASTER_REGION_SUFFIX_RE.sub("", raw_name.strip())
    return re.sub(r"\s+", " ", cleaned).strip()


def extract_tv_broadcasters(al_value: Any) -> list[str]:
    """Extract linear TV channel names from Flashscore `AL` field (key `"1"` only).

    Key `"2"` (bookmaker/betting streams) is strictly ignored.
    """
    if not al_value:
        return []

    payload: Any = al_value
    if isinstance(al_value, str):
        raw = al_value.strip()
        if not raw:
            return []
        try:
            payload = json.loads(raw)
        except (ValueError, TypeError):
            return []

    if not isinstance(payload, dict):
        return []

    tv_entries = payload.get("1")
    if not tv_entries:
        return []

    if isinstance(tv_entries, dict):
        tv_items: list[Any] = [tv_entries]
    elif isinstance(tv_entries, list):
        tv_items = tv_entries
    else:
        return []

    channels: list[str] = []
    seen: set[str] = set()
    for item in tv_items:
        raw_name = ""
        if isinstance(item, str):
            raw_name = item
        elif isinstance(item, dict):
            for key in ("BN", "bn", "name", "title", "channel"):
                val = item.get(key)
                if isinstance(val, str) and val.strip():
                    raw_name = val
                    break
        cleaned = _clean_broadcaster_name(raw_name)
        if cleaned:
            norm_key = cleaned.casefold()
            if any(ott in norm_key for ott in OTT_EXCLUDE):
                continue
            if norm_key not in seen:
                seen.add(norm_key)
                channels.append(cleaned)
    return channels


def _parse_block_kv(block: str) -> dict[str, str]:
    kv: dict[str, str] = {}
    for pair in block.split("¬"):
        if not pair or "÷" not in pair:
            continue
        key, value = pair.split("÷", 1)
        kv[key] = value
    return kv


def parse_flashscore_feed(raw_text: str) -> list[dict[str, Any]]:
    """Parse Flashscore's `~`/`¬`/`÷` delimited feed into normalized event dicts."""
    if not raw_text:
        return []

    events: list[dict[str, Any]] = []
    current_league: dict[str, str] = {
        "ZA": "",
        "ZY": "",
        "ZB": "",
        "ZAE": "",
        "ZL": "",
    }

    for block in raw_text.split("~"):
        if not block:
            continue
        kv = _parse_block_kv(block)
        if not kv:
            continue

        if "ZA" in kv and "AA" not in kv:
            zy_val = kv.get("ZY", "").strip()
            zb_val = kv.get("ZB", "").strip()
            current_league = {
                "ZA": kv.get("ZA", "").strip(),
                "ZY": zy_val,
                "ZB": zb_val,
                "ZAE": kv.get("ZAE", "").strip(),
                "ZL": kv.get("ZL", "").strip(),
            }
            continue

        aa = kv.get("AA", "").strip()
        if not aa:
            continue

        ad_raw = kv.get("AD", "").strip()
        try:
            start_ms = int(float(ad_raw)) * 1000 if ad_raw else 0
        except (TypeError, ValueError):
            start_ms = 0

        za = kv.get("ZA", "").strip() or current_league["ZA"]
        zy = kv.get("ZY", "").strip() or current_league["ZY"]
        zb = kv.get("ZB", "").strip() or current_league["ZB"]
        zae = kv.get("ZAE", "").strip() or current_league["ZAE"]
        zl = kv.get("ZL", "").strip() or current_league["ZL"]

        # Prefer ZY (country name) over ZB when ZB is purely numeric country ID.
        country = zy if zy else ("" if zb.isdigit() else zb)

        home = kv.get("AE", "").strip() or kv.get("CX", "").strip()
        away = kv.get("AF", "").strip()
        wu = kv.get("WU", "").strip()
        wv = kv.get("WV", "").strip()
        broadcasters = extract_tv_broadcasters(kv.get("AL"))

        events.append(
            {
                "aa": aa,
                "za": za,
                "country": country,
                "zae": zae,
                "zl": zl,
                "start_ms": start_ms,
                "home": home,
                "away": away,
                "wu": wu,
                "wv": wv,
                "broadcasters": broadcasters,
                "raw": kv,
            }
        )

    return events


def _slug_to_tokens(slug: str) -> list[str]:
    clean = slug.strip().strip("/")
    if not clean:
        return []
    if "/" in clean:
        segments = [seg for seg in clean.split("/") if seg]
        # Handle path shapes like /player/paolini-jasmine/12345 or /player/paolini-jasmine
        clean = segments[-2] if len(segments) >= 2 and len(segments[-1]) <= 10 and any(c.isdigit() for c in segments[-1]) else segments[-1]
    clean = _NUMERIC_TRAILING_ID_RE.sub("", clean)
    return [tok for tok in clean.split("-") if tok and not tok.isdigit()]


def _format_name_token(token: str) -> str:
    if not token:
        return ""
    if "'" in token:
        return "'".join(_format_name_token(part) for part in token.split("'"))
    return token[:1].upper() + token[1:].lower()


def _deabbreviate_single_player(short_name: str, slug: str) -> str:
    clean_short = re.sub(r"\s+", " ", short_name).strip()
    slug_tokens = _slug_to_tokens(slug)
    if not slug_tokens:
        return clean_short
    if not clean_short:
        return " ".join(_format_name_token(t) for t in slug_tokens)

    raw_words = clean_short.split()
    trailing_initials: list[str] = []
    surname_words = list(raw_words)
    while surname_words and _INITIAL_TOKEN_RE.match(surname_words[-1]):
        trailing_initials.insert(0, _strip_accents(surname_words.pop()).rstrip(".").lower())

    leading_initials: list[str] = []
    if not trailing_initials:
        while surname_words and _INITIAL_TOKEN_RE.match(surname_words[0]):
            leading_initials.append(_strip_accents(surname_words.pop(0)).rstrip(".").lower())

    initials = trailing_initials or leading_initials
    if not surname_words:
        return " ".join(_format_name_token(t) for t in slug_tokens)

    # Expand hyphenated/apostrophe surname words into flat ASCII slug tokens for matching.
    surname_flat: list[str] = []
    for word in surname_words:
        parts = re.split(r"[-']", _strip_accents(word).lower())
        surname_flat.extend(p for p in parts if p)

    surname_display = " ".join(
        "-".join(_format_name_token(p) for p in w.split("-")) if "-" in w else _format_name_token(w)
        for w in surname_words
    )

    k = len(surname_flat)
    slug_low = [t.lower() for t in slug_tokens]
    if k > 0 and len(slug_low) > k:
        # Standard Flashscore slug order: <surname-tokens>-<firstname-tokens>
        if slug_low[:k] == surname_flat:
            first_tokens = slug_tokens[k:]
            if not initials or _strip_accents(first_tokens[0]).lower().startswith(initials[0]):
                first_display = " ".join(_format_name_token(t) for t in first_tokens)
                return f"{first_display} {surname_display}".strip()
        # Alternate slug order: <firstname-tokens>-<surname-tokens>
        if slug_low[-k:] == surname_flat:
            first_tokens = slug_tokens[:-k]
            if not initials or _strip_accents(first_tokens[0]).lower().startswith(initials[0]):
                first_display = " ".join(_format_name_token(t) for t in first_tokens)
                return f"{first_display} {surname_display}".strip()

    # Fallback when short_name is already full (no initials) or transliterated differently.
    if not initials:
        return clean_short

    # If initials are present and slug starts with first surname token, reconstruct cleanly.
    if surname_flat and len(slug_low) >= 2 and slug_low[0] == surname_flat[0]:
        first_tokens = slug_tokens[min(k, len(slug_tokens) - 1) :]
        first_display = " ".join(_format_name_token(t) for t in first_tokens)
        return f"{first_display} {surname_display}".strip()

    return clean_short


def deabbreviate_tennis_name(short_name: str, slug: str) -> str:
    """Convert abbreviated Flashscore tennis names + slug into full names.

    Handles single players (e.g. ``Paolini J.`` + ``paolini-jasmine-12345`` ->
    ``Jasmine Paolini``), multi-word surnames/firstnames, and doubles pairs
    (``Player A / Player B``).
    """
    short_clean = (short_name or "").strip()
    slug_clean = (slug or "").strip()
    if not short_clean and not slug_clean:
        return ""

    if "/" in short_clean:
        short_parts = [p.strip() for p in short_clean.split("/") if p.strip()]
        if "/" in slug_clean:
            slug_parts = [p.strip() for p in slug_clean.split("/")]
        elif "|" in slug_clean:
            slug_parts = [p.strip() for p in slug_clean.split("|")]
        else:
            slug_parts = []
        resolved = [
            _deabbreviate_single_player(
                part,
                slug_parts[idx] if idx < len(slug_parts) else "",
            )
            for idx, part in enumerate(short_parts)
        ]
        return " / ".join(resolved)

    return _deabbreviate_single_player(short_clean, slug_clean)


def _split_competition_and_country(za: str, country: str, silo: SportSilo) -> tuple[str, str]:
    """Normalize Flashscore `ZA` headers like `ITALIA: Serie A` for taxonomy rules."""
    comp = (za or "").strip()
    ctry = (country or "").strip()
    if ":" not in comp:
        return comp, ctry

    prefix, rest = comp.split(":", 1)
    prefix_clean = prefix.strip()
    rest_clean = rest.strip()
    if not rest_clean:
        return comp, ctry

    # Keep circuit prefixes intact for tennis and motorsports.
    if silo in (SportSilo.TENNIS, SportSilo.MOTORI):
        return comp, ctry

    prefix_norm = normalize_country(prefix_clean)
    ctry_norm = normalize_country(ctry) if ctry else ""
    if ctry_norm and prefix_norm == ctry_norm:
        return rest_clean, ctry
    if not ctry and prefix_norm:
        return rest_clean, prefix_clean
    if prefix_clean.isupper() and len(prefix_clean) >= 3:
        return rest_clean, ctry or prefix_clean
    return comp, ctry


def _clean_motorsports_title(competition: str, stage: str) -> tuple[str, str]:
    """Return `(clean_competition, display_title)` avoiding duplicated stage suffixes."""
    comp = (competition or "").strip()
    stg = (stage or "").strip()
    if not stg:
        return comp, comp
    suffix = f" - {stg}"
    if comp.lower().endswith(suffix.lower()):
        base = comp[: -len(suffix)].strip()
        return base, f"{base} - {stg}"
    if stg.lower() in comp.lower():
        return comp, comp
    return comp, f"{comp} - {stg}"


def extract_tennis_cities(competition: str, alt_league: str = "") -> set[str]:
    """Extract normalized tournament/city names from a tennis competition header."""
    cities: set[str] = set()
    for raw in (competition, alt_league):
        text = (raw or "").strip()
        if not text:
            continue
        cleaned = _TENNIS_CIRCUIT_PREFIX_RE.sub("", text)
        if ":" in cleaned:
            cleaned = cleaned.split(":", 1)[1]
        cleaned = _TENNIS_SURFACE_RE.sub("", cleaned)
        cleaned = _TENNIS_PARENS_RE.sub(" ", cleaned)
        cleaned = _strip_accents(cleaned).lower()
        cleaned = re.sub(r"\s+", " ", cleaned).strip(" -–—")
        if not cleaned:
            continue
        for segment in re.split(r"\s+[-–—]\s+", cleaned):
            seg = segment.strip()
            if len(seg) >= 3 and seg not in _TENNIS_CITY_STOPWORDS:
                cities.add(seg)
    return cities


class DirettaService:
    """Async ingestion service for Flashscore / Diretta.it official schedules."""

    def __init__(
        self,
        settings: Optional[Settings] = None,
        client: Optional[httpx.AsyncClient] = None,
    ) -> None:
        self._settings = settings or get_settings()
        self._client = client

    def build_headers(self, lang: str = "it") -> dict[str, str]:
        """Build mandatory Flashscore/Diretta HTTP request headers."""
        base_url = "https://www.flashscore.com" if lang == "en" else "https://www.diretta.it"
        fsign = (self._settings.FLASHSCORE_FSIGN or "").strip() or "SW9D1eZo"
        return {
            "User-Agent": DEFAULT_USER_AGENT,
            "Origin": base_url,
            "Referer": f"{base_url}/",
            "x-fsign": fsign,
        }

    @staticmethod
    def build_feed_url(sport_id: int, day: int, *, lang: str = "it") -> str:
        """Build the primary IT or secondary EN Flashscore feed URL."""
        template = IT_FEED_URL if lang == "it" else EN_FEED_URL
        return template.format(sport_id=sport_id, day=day)

    async def _fetch_feed_events(
        self,
        client: httpx.AsyncClient,
        sport_id: int,
        day: int,
        *,
        lang: str,
    ) -> list[dict[str, Any]]:
        url = self.build_feed_url(sport_id, day, lang=lang)
        try:
            resp = await client.get(url, headers=self.build_headers(lang=lang))
            resp.raise_for_status()
            return parse_flashscore_feed(resp.text)
        except Exception as exc:
            logger.warning(
                "Flashscore feed fetch failed (sport=%s day=%s lang=%s): %s",
                sport_id,
                day,
                lang,
                exc,
            )
            return []

    def _build_match(
        self,
        *,
        match_id: str,
        title: str,
        base_silo: SportSilo,
        duration_minutes: int,
        start_ms: int,
        competition: str,
        country: str,
        stage: str,
        home_team: Optional[TeamInfo],
        away_team: Optional[TeamInfo],
        alt_home: str,
        alt_away: str,
        alt_league: str,
        alt_title: str = "",
        alt_competition: str = "",
        alt_country: str = "",
        broadcasters: list[str],
        now_ms: int,
    ) -> Optional[Match]:
        is_women, is_youth = detect_demographics(competition, country)

        league_match = DEFAULT_LEAGUE_TAXONOMY.match_league(
            base_silo.value, competition, country
        )
        if league_match is None and alt_league and alt_league != competition:
            league_match = DEFAULT_LEAGUE_TAXONOMY.match_league(
                base_silo.value, alt_league, country
            )

        eff_silo = base_silo
        if league_match is not None and league_match.catalog_target == SportSilo.CALCIO_ITA.value:
            eff_silo = SportSilo.CALCIO_ITA
        elif base_silo == SportSilo.FOOTBALL and normalize_country(country) == "italy":
            eff_silo = SportSilo.CALCIO_ITA

        if league_match is not None:
            is_women = is_women or league_match.is_women
            is_youth = is_youth or league_match.is_youth

        admitted = DEFAULT_LEAGUE_TAXONOMY.is_admitted_event(
            eff_silo.value,
            competition,
            country,
            has_broadcasters=bool(broadcasters),
            has_streams=False,
        )
        if not admitted and alt_league and alt_league != competition:
            admitted = DEFAULT_LEAGUE_TAXONOMY.is_admitted_event(
                eff_silo.value,
                alt_league,
                country,
                has_broadcasters=bool(broadcasters),
                has_streams=False,
            )
        if not admitted:
            return None

        canonical_comp = league_match.canonical_slug if league_match else ""
        category, genres = DEFAULT_GENRE_CLASSIFIER.classify_event(
            silo=eff_silo.value,
            competition=competition,
            title=title,
            country=country,
            canonical_competition=canonical_comp,
            is_women=is_women,
            is_youth=is_youth,
        )

        return Match(
            id=match_id,
            title=title,
            category=category,
            silo=eff_silo,
            genres=genres,
            date=start_ms,
            end_date=start_ms + duration_minutes * 60 * 1000,
            popular=league_match.is_popular if league_match else False,
            competition=competition,
            canonical_competition=canonical_comp,
            country=country,
            stage=stage,
            home_team=home_team,
            away_team=away_team,
            alt_home=alt_home,
            alt_away=alt_away,
            alt_league=alt_league,
            alt_title=alt_title,
            alt_competition=alt_competition,
            alt_country=alt_country,
            is_women=is_women,
            is_youth=is_youth,
            broadcasters=broadcasters,
            source_origin="diretta",
            updated_at=now_ms,
        )

    def _fuse_sport_day(
        self,
        sport_id: int,
        day: int,
        it_events: list[dict[str, Any]],
        en_events: list[dict[str, Any]],
        *,
        now_ms: int,
        tennis_cities: set[str],
    ) -> list[Match]:
        spec = SPORT_IDS[sport_id]
        base_silo = spec.silo
        duration_minutes = spec.duration_minutes

        en_by_aa: dict[str, dict[str, Any]] = {ev["aa"]: ev for ev in en_events}
        matches: list[Match] = []

        # Motorsports (F1 ID 32 & MotoGP ID 33): deduplicate driver rows into one session card.
        if sport_id in (32, 33):
            primary_events = it_events if it_events else en_events
            grouped: dict[str, list[dict[str, Any]]] = {}
            for ev in primary_events:
                comp, country = _split_competition_and_country(
                    ev["za"], ev["country"], base_silo
                )
                raw_za = ev["za"]
                sess_name = ev["zae"]
                time_epoch = ev["start_ms"] // 1000
                key = f"{raw_za}_{sess_name}_{time_epoch}"
                grouped.setdefault(key, []).append({**ev, "_comp": comp, "_country": country})

            for group in grouped.values():
                first = group[0]
                first_aa = first["aa"]
                comp = first["_comp"]
                country = first["_country"]
                stage = first["zae"]
                clean_comp, title = _clean_motorsports_title(comp, stage)

                merged_broadcasters: list[str] = []
                seen_bc: set[str] = set()
                en_match: Optional[dict[str, Any]] = None
                for row in group:
                    if en_match is None and row["aa"] in en_by_aa:
                        en_match = en_by_aa[row["aa"]]
                    for bc in row["broadcasters"]:
                        bc_key = bc.casefold()
                        if bc_key not in seen_bc:
                            seen_bc.add(bc_key)
                            merged_broadcasters.append(bc)

                alt_league = ""
                alt_title = ""
                alt_competition = ""
                alt_country = ""
                if en_match is not None:
                    en_comp, en_country = _split_competition_and_country(
                        en_match["za"], en_match["country"], base_silo
                    )
                    en_clean_comp, en_title = _clean_motorsports_title(en_comp, en_match["zae"])
                    alt_league = en_clean_comp
                    alt_competition = en_clean_comp
                    alt_country = en_country
                    alt_title = en_title
                    if not merged_broadcasters and en_match["broadcasters"]:
                        merged_broadcasters = list(en_match["broadcasters"])

                match = self._build_match(
                    match_id=f"diretta-{first_aa}",
                    title=title,
                    base_silo=base_silo,
                    duration_minutes=duration_minutes,
                    start_ms=first["start_ms"],
                    competition=clean_comp,
                    country=country,
                    stage=stage,
                    home_team=TeamInfo(name=title, alt_name=alt_title),
                    away_team=None,
                    alt_home=alt_title,
                    alt_away="",
                    alt_league=alt_league,
                    alt_title=alt_title,
                    alt_competition=alt_competition,
                    alt_country=alt_country,
                    broadcasters=merged_broadcasters,
                    now_ms=now_ms,
                )
                if match is not None:
                    matches.append(match)
            return matches

        # Standard team / individual sports: merge IT primary and EN secondary by AA ID.
        seen_aa: set[str] = set()
        ordered_pairs: list[tuple[dict[str, Any], Optional[dict[str, Any]]]] = []
        for it_ev in it_events:
            aa = it_ev["aa"]
            if aa in seen_aa:
                continue
            seen_aa.add(aa)
            ordered_pairs.append((it_ev, en_by_aa.get(aa)))

        for en_ev in en_events:
            aa = en_ev["aa"]
            if aa not in seen_aa:
                seen_aa.add(aa)
                ordered_pairs.append((en_ev, en_ev))

        for primary_ev, en_ev in ordered_pairs:
            aa = primary_ev["aa"]
            comp, country = _split_competition_and_country(
                primary_ev["za"], primary_ev["country"], base_silo
            )
            stage = primary_ev["zae"]
            home_raw = primary_ev["home"]
            away_raw = primary_ev["away"]

            if base_silo == SportSilo.TENNIS:
                home_name = deabbreviate_tennis_name(home_raw, primary_ev["wu"])
                away_name = deabbreviate_tennis_name(away_raw, primary_ev["wv"])
            else:
                home_name = home_raw
                away_name = away_raw

            alt_league = ""
            alt_home = ""
            alt_away = ""
            alt_title = ""
            alt_competition = ""
            alt_country = ""
            if en_ev is not None:
                en_comp, en_country = _split_competition_and_country(
                    en_ev["za"], en_ev["country"], base_silo
                )
                alt_league = en_comp
                alt_competition = en_comp
                alt_country = en_country
                if base_silo == SportSilo.TENNIS:
                    alt_home = deabbreviate_tennis_name(en_ev["home"], en_ev["wu"])
                    alt_away = deabbreviate_tennis_name(en_ev["away"], en_ev["wv"])
                else:
                    alt_home = en_ev["home"]
                    alt_away = en_ev["away"]
                alt_title = f"{alt_home} vs {alt_away}" if alt_away else alt_home

            broadcasters = list(primary_ev["broadcasters"])
            if not broadcasters and en_ev is not None:
                broadcasters = list(en_ev["broadcasters"])

            title = f"{home_name} vs {away_name}" if away_name else home_name
            home_team = (
                TeamInfo(name=home_name, alt_name=alt_home, slug=primary_ev["wu"])
                if home_name
                else None
            )
            away_team = (
                TeamInfo(name=away_name, alt_name=alt_away, slug=primary_ev["wv"])
                if away_name
                else None
            )

            match = self._build_match(
                match_id=f"diretta-{aa}",
                title=title,
                base_silo=base_silo,
                duration_minutes=duration_minutes,
                start_ms=primary_ev["start_ms"],
                competition=comp,
                country=country,
                stage=stage,
                home_team=home_team,
                away_team=away_team,
                alt_home=alt_home,
                alt_away=alt_away,
                alt_league=alt_league,
                alt_title=alt_title,
                alt_competition=alt_competition,
                alt_country=alt_country,
                broadcasters=broadcasters,
                now_ms=now_ms,
            )
            if match is not None:
                matches.append(match)
                if base_silo == SportSilo.TENNIS and day in (0, 1):
                    tennis_cities.update(extract_tennis_cities(comp, alt_league))

        return matches

    async def fetch_schedule(
        self,
        *,
        days: tuple[int, ...] = FEED_DAYS,
        sport_ids: Optional[Iterable[int]] = None,
        now_utc: Optional[datetime] = None,
    ) -> tuple[list[Match], frozenset[str]]:
        """Fetch and fuse IT+EN Flashscore feeds into admitted `Match` objects."""
        if not self._settings.ENABLE_DIRETTA:
            return [], frozenset()

        target_sports = [sid for sid in (sport_ids or SPORT_IDS.keys()) if sid in SPORT_IDS]
        if not target_sports:
            return [], frozenset()

        now_ms = (
            int(now_utc.timestamp() * 1000)
            if now_utc is not None
            else int(time.time() * 1000)
        )

        async def _run_with_client(client: httpx.AsyncClient) -> tuple[list[Match], frozenset[str]]:
            tasks: list[tuple[int, int, str, Any]] = []
            coros = []
            for sid in target_sports:
                for day in days:
                    tasks.append((sid, day, "it", None))
                    coros.append(self._fetch_feed_events(client, sid, day, lang="it"))
                    tasks.append((sid, day, "en", None))
                    coros.append(self._fetch_feed_events(client, sid, day, lang="en"))

            results = await asyncio.gather(*coros)
            feeds_by_key: dict[tuple[int, int, str], list[dict[str, Any]]] = {}
            for (sid, day, lang, _), parsed in zip(tasks, results):
                feeds_by_key[(sid, day, lang)] = parsed

            all_matches: list[Match] = []
            seen_match_ids: set[str] = set()
            tennis_cities: set[str] = set()

            for sid in target_sports:
                for day in days:
                    it_evs = feeds_by_key.get((sid, day, "it"), [])
                    en_evs = feeds_by_key.get((sid, day, "en"), [])
                    day_matches = self._fuse_sport_day(
                        sid,
                        day,
                        it_evs,
                        en_evs,
                        now_ms=now_ms,
                        tennis_cities=tennis_cities,
                    )
                    for m in day_matches:
                        if m.id not in seen_match_ids:
                            seen_match_ids.add(m.id)
                            all_matches.append(m)

            all_matches.sort(key=lambda m: (m.date, m.title))
            return all_matches, frozenset(tennis_cities)

        if self._client is not None:
            return await _run_with_client(self._client)

        async with create_standard_client(timeout=20.0) as client:
            return await _run_with_client(client)
