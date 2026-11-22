"""5-level cascading GenreClassifier and Stremio/Nuvio catalog builders."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Sequence

from app.taxonomy.demographics import detect_demographics
from app.taxonomy.leagues import (
    DEFAULT_LEAGUE_TAXONOMY,
    SPORT_SILOS,
    LeagueTaxonomy,
    normalize_country,
)

CATALOG_PREFIX = "ss_"
LIVE_CATALOG_ID = "ss_live"


@dataclass(frozen=True, slots=True)
class CatalogDefinition:
    """Internal definition shared by Stremio (nested) and Nuvio (flat) builders."""

    id: str
    silo: str
    name: str
    genres: tuple[str, ...] = ()


CATALOG_DEFINITIONS: tuple[CatalogDefinition, ...] = (
    CatalogDefinition(
        id="ss_live",
        silo="live",
        name="🔴 Live Ora",
        genres=(
            "Calcio Italiano",
            "Calcio Internazionale",
            "Tennis",
            "Basket",
            "Motori",
            "Combat",
            "Volley",
            "NFL",
            "Hockey",
            "Baseball",
            "Rugby",
            "Pallamano",
            "Altri Sport",
        ),
    ),
    CatalogDefinition(
        id="ss_calcio_ita",
        silo="calcio_ita",
        name="🇮🇹 Calcio Italiano",
        genres=(
            "Serie A",
            "Serie B",
            "Serie C",
            "Coppa Italia",
            "Calcio Femminile",
            "Primavera / Giovanili",
            "Diretta Gol",
        ),
    ),
    CatalogDefinition(
        id="ss_football",
        silo="football",
        name="⚽ Calcio Internazionale",
        genres=(
            "Champions League",
            "Europa & Conference",
            "Premier League",
            "La Liga",
            "Bundesliga",
            "Ligue 1",
            "Sudamerica",
            "Nazionali",
            "Calcio Femminile",
            "Altri Campionati",
        ),
    ),
    CatalogDefinition(
        id="ss_tennis",
        silo="tennis",
        name="🎾 Tennis",
        genres=(
            "Grande Slam",
            "ATP Tour",
            "WTA Tour",
            "Challenger",
            "Coppe Nazionali",
            "Altri Tornei",
        ),
    ),
    CatalogDefinition(
        id="ss_basket",
        silo="basket",
        name="🏀 Basket",
        genres=(
            "NBA",
            "WNBA",
            "Eurolega & Coppe",
            "LBA Serie A",
            "Campionati Esteri",
        ),
    ),
    CatalogDefinition(
        id="ss_motori",
        silo="motori",
        name="🏎️ Motori",
        genres=(
            "Formula 1",
            "MotoGP",
            "Superbike",
            "Altre Categorie",
        ),
    ),
    CatalogDefinition(
        id="ss_combat",
        silo="combat",
        name="🥊 Combat & Wrestling",
        genres=(
            "UFC",
            "MMA",
            "Boxe",
            "Wrestling",
        ),
    ),
    CatalogDefinition(
        id="ss_volley",
        silo="volley",
        name="🏐 Volley",
        genres=(
            "SuperLega",
            "Serie A1 Femminile",
            "CEV Champions League",
            "Nazionali",
            "Campionati Esteri",
        ),
    ),
    CatalogDefinition(
        id="ss_nfl",
        silo="nfl",
        name="🏈 NFL & Football US",
        genres=(
            "NFL",
            "NCAA",
            "RedZone",
        ),
    ),
    CatalogDefinition(
        id="ss_hockey",
        silo="hockey",
        name="🏒 Hockey",
        genres=(
            "NHL",
            "Campionati Europei",
            "Nazionali",
        ),
    ),
    CatalogDefinition(
        id="ss_baseball",
        silo="baseball",
        name="⚾ Baseball",
        genres=(
            "MLB",
            "Campionati Esteri",
        ),
    ),
    CatalogDefinition(
        id="ss_rugby",
        silo="rugby",
        name="🏉 Rugby",
        genres=(
            "Sei Nazioni",
            "Coppe Europee",
            "URC & Club",
            "Nazionali",
        ),
    ),
    CatalogDefinition(
        id="ss_handball",
        silo="handball",
        name="🤾 Pallamano",
        genres=(
            "EHF Champions",
            "Campionati Nazionali",
            "Nazionali",
        ),
    ),
    CatalogDefinition(
        id="ss_altri_sport",
        silo="altri_sport",
        name="🏅 Altri Sport",
        genres=(
            "Ciclismo",
            "Golf",
            "Sport Invernali",
            "Atletica",
            "Snooker & Freccette",
            "Padel",
            "Olimpiadi",
            "Altri Sport",
        ),
    ),
)

_SILO_TO_CATALOG_ID: dict[str, str] = {
    c.silo: c.id for c in CATALOG_DEFINITIONS if c.silo in SPORT_SILOS
}

_SILO_LIVE_GENRE: dict[str, str] = {
    "calcio_ita": "Calcio Italiano",
    "football": "Calcio Internazionale",
    "tennis": "Tennis",
    "basket": "Basket",
    "motori": "Motori",
    "combat": "Combat",
    "volley": "Volley",
    "nfl": "NFL",
    "hockey": "Hockey",
    "baseball": "Baseball",
    "rugby": "Rugby",
    "handball": "Pallamano",
    "altri_sport": "Altri Sport",
}

# Level 0 Simulcast patterns
_REDZONE_RE = re.compile(r"\b(?:nfl\s+)?red\s*zone\b", re.IGNORECASE)
_DIRETTA_GOL_RE = re.compile(r"\b(?:diretta\s+gol|zona\s+dazn|zona\s+gol)\b", re.IGNORECASE)
_EURO_COMP_HINT_RE = re.compile(
    r"\b(?:champions\s+league|europa\s+league|conference\s+league|premier\s+league|la\s*liga|bundesliga|ligue\s*1)\b",
    re.IGNORECASE,
)
_MULTICAMPO_RE = re.compile(r"\b(?:multicampo|diretta\s+campo|simulcast)\b", re.IGNORECASE)
_TENNIS_HINT_RE = re.compile(r"\b(?:tennis|atp|wta|slam|wimbledon|roland\s+garros|us\s+open|australian\s+open)\b", re.IGNORECASE)

# Canonical slug -> (catalog_id, genres) quick map for Level 1 when pre-validated
_CANONICAL_SLUG_ROUTING: dict[str, tuple[str, list[str]]] = {
    "serie-a": ("ss_calcio_ita", ["Serie A"]),
    "serie-b": ("ss_calcio_ita", ["Serie B"]),
    "serie-c": ("ss_calcio_ita", ["Serie C"]),
    "coppa-italia": ("ss_calcio_ita", ["Coppa Italia"]),
    "supercoppa-italiana": ("ss_calcio_ita", ["Coppa Italia"]),
    "serie-a-femminile": ("ss_calcio_ita", ["Calcio Femminile"]),
    "coppa-italia-femminile": ("ss_calcio_ita", ["Calcio Femminile"]),
    "primavera-1": ("ss_calcio_ita", ["Primavera / Giovanili"]),
    "primavera-2": ("ss_calcio_ita", ["Primavera / Giovanili"]),
    "coppa-italia-primavera": ("ss_calcio_ita", ["Primavera / Giovanili"]),
    "champions-league": ("ss_football", ["Champions League"]),
    "uefa-super-cup": ("ss_football", ["Champions League"]),
    "club-world-cup": ("ss_football", ["Champions League"]),
    "europa-league": ("ss_football", ["Europa & Conference"]),
    "conference-league": ("ss_football", ["Europa & Conference"]),
    "premier-league": ("ss_football", ["Premier League"]),
    "championship": ("ss_football", ["Premier League"]),
    "fa-cup": ("ss_football", ["Premier League"]),
    "efl-cup": ("ss_football", ["Premier League"]),
    "la-liga": ("ss_football", ["La Liga"]),
    "la-liga-2": ("ss_football", ["La Liga"]),
    "copa-del-rey": ("ss_football", ["La Liga"]),
    "bundesliga": ("ss_football", ["Bundesliga"]),
    "2-bundesliga": ("ss_football", ["Bundesliga"]),
    "dfb-pokal": ("ss_football", ["Bundesliga"]),
    "ligue-1": ("ss_football", ["Ligue 1"]),
    "ligue-2": ("ss_football", ["Ligue 1"]),
    "coupe-de-france": ("ss_football", ["Ligue 1"]),
    "brasileirao-serie-a": ("ss_football", ["Sudamerica"]),
    "brasileirao-serie-b": ("ss_football", ["Sudamerica"]),
    "ecuador-ligapro-serie-a": ("ss_football", ["Sudamerica"]),
    "colombia-primera-a": ("ss_football", ["Sudamerica"]),
    "copa-libertadores": ("ss_football", ["Sudamerica"]),
    "copa-sudamericana": ("ss_football", ["Sudamerica"]),
    "liga-profesional-argentina": ("ss_football", ["Sudamerica"]),
    "uefa-womens-champions-league": ("ss_football", ["Calcio Femminile"]),
    "wsl-women": ("ss_football", ["Calcio Femminile"]),
    "liga-f-women": ("ss_football", ["Calcio Femminile"]),
    "frauen-bundesliga": ("ss_football", ["Calcio Femminile"]),
    "d1-arkema": ("ss_football", ["Calcio Femminile"]),
    "nwsl": ("ss_football", ["Calcio Femminile"]),
    "australian-open": ("ss_tennis", ["Grande Slam"]),
    "roland-garros": ("ss_tennis", ["Grande Slam"]),
    "wimbledon": ("ss_tennis", ["Grande Slam"]),
    "us-open": ("ss_tennis", ["Grande Slam"]),
    "atp-finals": ("ss_tennis", ["ATP Tour"]),
    "atp-tour": ("ss_tennis", ["ATP Tour"]),
    "wta-finals": ("ss_tennis", ["WTA Tour"]),
    "wta-tour": ("ss_tennis", ["WTA Tour"]),
    "atp-challenger": ("ss_tennis", ["Challenger"]),
    "davis-cup": ("ss_tennis", ["Coppe Nazionali"]),
    "bjk-cup": ("ss_tennis", ["Coppe Nazionali"]),
    "united-cup": ("ss_tennis", ["Coppe Nazionali"]),
    "laver-cup": ("ss_tennis", ["Coppe Nazionali"]),
    "nba": ("ss_basket", ["NBA"]),
    "wnba": ("ss_basket", ["WNBA"]),
    "euroleague": ("ss_basket", ["Eurolega & Coppe"]),
    "euroleague-women": ("ss_basket", ["Eurolega & Coppe"]),
    "eurocup-basket": ("ss_basket", ["Eurolega & Coppe"]),
    "basketball-champions-league": ("ss_basket", ["Eurolega & Coppe"]),
    "fiba-international": ("ss_basket", ["Eurolega & Coppe"]),
    "lba-serie-a": ("ss_basket", ["LBA Serie A"]),
    "serie-a2-basket": ("ss_basket", ["LBA Serie A"]),
    "formula-1": ("ss_motori", ["Formula 1"]),
    "formula-2": ("ss_motori", ["Formula 1"]),
    "formula-3": ("ss_motori", ["Formula 1"]),
    "motogp": ("ss_motori", ["MotoGP"]),
    "moto2": ("ss_motori", ["MotoGP"]),
    "moto3": ("ss_motori", ["MotoGP"]),
    "wsbk": ("ss_motori", ["Superbike"]),
    "ufc": ("ss_combat", ["UFC"]),
    "boxing": ("ss_combat", ["Boxe"]),
    "wwe": ("ss_combat", ["Wrestling"]),
    "aew": ("ss_combat", ["Wrestling"]),
    "superlega-volley": ("ss_volley", ["SuperLega"]),
    "serie-a1-femminile-volley": ("ss_volley", ["Serie A1 Femminile"]),
    "cev-champions-league": ("ss_volley", ["CEV Champions League"]),
    "nfl": ("ss_nfl", ["NFL"]),
    "ncaaf": ("ss_nfl", ["NCAA"]),
    "nhl": ("ss_hockey", ["NHL"]),
    "mlb": ("ss_baseball", ["MLB"]),
    "six-nations": ("ss_rugby", ["Sei Nazioni"]),
    "ehf-champions-league": ("ss_handball", ["EHF Champions"]),
}


def build_stremio_catalogs(catalog_type: str = "tv") -> list[dict[str, Any]]:
    """Build Stremio `ManifestCatalog` entries with nested `genre` dropdown filters."""
    catalogs: list[dict[str, Any]] = []
    for cat in CATALOG_DEFINITIONS:
        extra: list[dict[str, Any]] = []
        if cat.genres:
            extra.append(
                {
                    "name": "genre",
                    "options": list(cat.genres),
                    "isRequired": False,
                }
            )
        extra.extend(
            [
                {"name": "search", "isRequired": False},
                {"name": "skip", "isRequired": False},
            ]
        )
        catalogs.append(
            {
                "type": catalog_type,
                "id": cat.id,
                "name": cat.name,
                "extra": extra,
            }
        )
    return catalogs


def build_nuvio_catalogs(catalog_type: str = "tv") -> list[dict[str, Any]]:
    """Build flattened independent Nuvio catalog definitions (no nested `genre` dropdown)."""
    return [
        {
            "type": catalog_type,
            "id": cat.id,
            "name": cat.name,
            "extra": [
                {"name": "search", "isRequired": False},
                {"name": "skip", "isRequired": False},
            ],
        }
        for cat in CATALOG_DEFINITIONS
    ]


def _normalize_catalog_id(value: str) -> str:
    clean = (value or "").strip().lower()
    if not clean:
        return ""
    if clean.startswith(CATALOG_PREFIX):
        return clean
    if clean == "live":
        return LIVE_CATALOG_ID
    return _SILO_TO_CATALOG_ID.get(clean, f"{CATALOG_PREFIX}{clean}")


class GenreClassifier:
    """5-level cascading classifier for StreamSport events and catalogs."""

    def __init__(self, taxonomy: LeagueTaxonomy | None = None) -> None:
        self.taxonomy = taxonomy or DEFAULT_LEAGUE_TAXONOMY

    def classify_event(
        self,
        *,
        silo: str = "",
        competition: str = "",
        title: str = "",
        country: str = "",
        canonical_competition: str = "",
        is_women: bool | None = None,
        is_youth: bool | None = None,
    ) -> tuple[str, list[str]]:
        """Return `(primary_catalog_id, genres)` across the 5-level cascade."""
        combined_text = f"{competition} {title}".strip()
        silo_norm = (silo or "").strip().lower()
        country_norm = normalize_country(country)

        # Level 0: Early Simulcast routing
        if _REDZONE_RE.search(combined_text):
            return "ss_nfl", ["NFL", "RedZone"]

        if _DIRETTA_GOL_RE.search(combined_text):
            if _EURO_COMP_HINT_RE.search(combined_text) or silo_norm == "football":
                return "ss_football", ["Diretta Gol", "Champions League"]
            return "ss_calcio_ita", ["Diretta Gol", "Serie A"]

        if _MULTICAMPO_RE.search(combined_text):
            if silo_norm == "tennis" or _TENNIS_HINT_RE.search(combined_text):
                return "ss_tennis", ["Multicampo", "ATP Tour"]
            if silo_norm in ("calcio_ita", "football"):
                cat = "ss_calcio_ita" if silo_norm == "calcio_ita" else "ss_football"
                return cat, ["Diretta Gol" if cat == "ss_calcio_ita" else "Altri Campionati"]

        # Level 1: Validated Canonical Competition routing
        if canonical_competition and canonical_competition in _CANONICAL_SLUG_ROUTING:
            cat_id, genres = _CANONICAL_SLUG_ROUTING[canonical_competition]
            return cat_id, list(genres)

        if competition:
            lookup_silo = silo_norm if silo_norm in SPORT_SILOS else "football"
            league_match = self.taxonomy.match_league(lookup_silo, competition, country)
            if league_match is not None:
                cat_id = _SILO_TO_CATALOG_ID.get(
                    league_match.catalog_target,
                    _SILO_TO_CATALOG_ID.get(league_match.silo, "ss_altri_sport"),
                )
                if league_match.genre:
                    return cat_id, [league_match.genre]
                if league_match.canonical_slug in _CANONICAL_SLUG_ROUTING:
                    mapped_cat, mapped_genres = _CANONICAL_SLUG_ROUTING[league_match.canonical_slug]
                    return mapped_cat, list(mapped_genres)

        # Level 2 & Level 3: Sport Silo resolution + Sport Sub-classifiers
        comp_women, comp_youth = detect_demographics(competition, country)
        eff_women = comp_women if is_women is None else is_women
        eff_youth = comp_youth if is_youth is None else is_youth

        if silo_norm in ("calcio_ita", "football"):
            is_italian = silo_norm == "calcio_ita" or country_norm == "italy"
            cat_id = "ss_calcio_ita" if is_italian else "ss_football"
            if eff_women:
                return cat_id, ["Calcio Femminile"]
            if eff_youth:
                return cat_id, ["Primavera / Giovanili"] if is_italian else ["Altri Campionati"]
            if is_italian:
                return "ss_calcio_ita", ["Serie A" if "serie a" in competition.lower() else "Coppa Italia" if "coppa" in competition.lower() else "Serie C"]
            return "ss_football", ["Altri Campionati"]

        if silo_norm == "tennis":
            comp_low = competition.lower()
            if any(k in comp_low for k in ("wimbledon", "roland garros", "australian open", "us open")):
                return "ss_tennis", ["Grande Slam"]
            if "challenger" in comp_low:
                return "ss_tennis", ["Challenger"]
            if eff_women or "wta" in comp_low:
                return "ss_tennis", ["WTA Tour"]
            if "atp" in comp_low:
                return "ss_tennis", ["ATP Tour"]
            return "ss_tennis", ["Altri Tornei"]

        if silo_norm == "basket":
            comp_low = competition.lower()
            if eff_women or "wnba" in comp_low:
                return "ss_basket", ["WNBA"]
            if "nba" in comp_low:
                return "ss_basket", ["NBA"]
            if country_norm == "italy":
                return "ss_basket", ["LBA Serie A"]
            if any(k in comp_low for k in ("euroleague", "eurolega", "eurocup", "champions")):
                return "ss_basket", ["Eurolega & Coppe"]
            return "ss_basket", ["Campionati Esteri"]

        if silo_norm == "motori":
            comp_low = competition.lower()
            if "formula 1" in comp_low or re.search(r"\bf1\b", comp_low):
                return "ss_motori", ["Formula 1"]
            if "motogp" in comp_low or "moto gp" in comp_low or "moto2" in comp_low or "moto3" in comp_low:
                return "ss_motori", ["MotoGP"]
            if "superbike" in comp_low or "wsbk" in comp_low:
                return "ss_motori", ["Superbike"]
            return "ss_motori", ["Altre Categorie"]

        if silo_norm == "combat":
            comp_low = competition.lower()
            if "ufc" in comp_low:
                return "ss_combat", ["UFC"]
            if any(k in comp_low for k in ("boxe", "boxing", "pugilato")):
                return "ss_combat", ["Boxe"]
            if any(k in comp_low for k in ("wwe", "aew", "wrestling")):
                return "ss_combat", ["Wrestling"]
            return "ss_combat", ["MMA"]

        if silo_norm in _SILO_TO_CATALOG_ID:
            cat_id = _SILO_TO_CATALOG_ID[silo_norm]
            cat_def = next((c for c in CATALOG_DEFINITIONS if c.id == cat_id), None)
            default_genre = cat_def.genres[-1] if cat_def and cat_def.genres else "Altri Sport"
            return cat_id, [default_genre]

        # Level 4: Fallback
        return "ss_altri_sport", ["Altri Sport"]


DEFAULT_GENRE_CLASSIFIER = GenreClassifier()


def classify_event(
    *,
    silo: str = "",
    competition: str = "",
    title: str = "",
    country: str = "",
    canonical_competition: str = "",
    is_women: bool | None = None,
    is_youth: bool | None = None,
) -> tuple[str, list[str]]:
    """Module-level convenience wrapper around `DEFAULT_GENRE_CLASSIFIER`."""
    return DEFAULT_GENRE_CLASSIFIER.classify_event(
        silo=silo,
        competition=competition,
        title=title,
        country=country,
        canonical_competition=canonical_competition,
        is_women=is_women,
        is_youth=is_youth,
    )


def matches_catalog_filter(
    match_silo: str,
    match_category: str,
    match_genres: Sequence[str],
    catalog_id: str,
    selected_genre: str | None = None,
) -> bool:
    """Check whether an event belongs to `(catalog_id, selected_genre)`."""
    req_cat = _normalize_catalog_id(catalog_id)
    if not req_cat:
        return False

    genre_norm = (selected_genre or "").strip().casefold()
    genres_lower = {g.strip().casefold() for g in match_genres if g}

    if req_cat == LIVE_CATALOG_ID:
        if not genre_norm or genre_norm in ("all", "tutti"):
            return True
        silo_label = _SILO_LIVE_GENRE.get((match_silo or "").strip().lower(), "").casefold()
        return genre_norm == silo_label or genre_norm in genres_lower

    event_cat = _normalize_catalog_id(match_category or match_silo)
    if event_cat != req_cat:
        return False

    if not genre_norm or genre_norm in ("all", "tutti"):
        return True

    return genre_norm in genres_lower
