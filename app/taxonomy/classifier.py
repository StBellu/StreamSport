"""5-level cascading GenreClassifier and Stremio/Nuvio catalog builders."""

from __future__ import annotations

import re
import unicodedata
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
        id="calcio_italiano",
        silo="calcio_ita",
        name="🇮🇹 CALCIO ITALIANO",
        genres=(
            "Serie A",
            "Serie B",
            "Serie C",
            "Coppa Italia e Supercoppa",
            "Calcio Femminile",
            "Primavera e Giovanili",
        ),
    ),
    CatalogDefinition(
        id="calcio_estero",
        silo="football",
        name="🌍 CALCIO INTERNAZIONALE E COPPE",
        genres=(
            "Champions League",
            "Champions League Femminile",
            "Europa e Conference League",
            "Premier League",
            "La Liga",
            "Bundesliga e Ligue 1",
            "Altri Campionati Europei",
            "Americhe e Leghe Extra-UE",
            "Europei Under 21 e Nazionali Giovanili",
            "Nazionali e Amichevoli",
        ),
    ),
    CatalogDefinition(
        id="tennis",
        silo="tennis",
        name="🎾 TENNIS",
        genres=(
            "ATP",
            "WTA",
            "Coppa Davis e BJK Cup",
            "Grandi Slam",
            "Challenger e Altri",
        ),
    ),
    CatalogDefinition(
        id="motori",
        silo="motori",
        name="🏎️ MOTORI",
        genres=(
            "Formula 1",
            "MotoGP e Superbike",
            "Rally e WRC",
            "NASCAR e IndyCar",
        ),
    ),
    CatalogDefinition(
        id="basket",
        silo="basket",
        name="🏀 BASKET",
        genres=(
            "NBA",
            "LBA Serie A",
            "Eurolega ed Eurocup",
            "WNBA e Femminile",
            "Campionati Esteri ed NBL",
            "NCAA e College Basket",
            "FIBA e Tornei Nazionali",
        ),
    ),
    CatalogDefinition(
        id="volley",
        silo="volley",
        name="🏐 VOLLEY",
        genres=(
            "Superlega e Serie A1",
            "Champions League Volley",
            "Volley Femminile",
            "Nazionali e Internazionale",
        ),
    ),
    CatalogDefinition(
        id="football_americano",
        silo="nfl",
        name="🏈 FOOTBALL AMERICANO",
        genres=(
            "NFL",
            "NCAA College Football",
            "CFL e Altre Leghe",
        ),
    ),
    CatalogDefinition(
        id="baseball",
        silo="baseball",
        name="⚾ BASEBALL",
        genres=(
            "MLB",
            "NCAA College Baseball",
            "Campionati Internazionali",
        ),
    ),
    CatalogDefinition(
        id="hockey",
        silo="hockey",
        name="🏒 HOCKEY",
        genres=(
            "NHL",
            "KHL e Leghe Europee",
            "Mondiali e Nazionali",
        ),
    ),
    CatalogDefinition(
        id="combattimento",
        silo="combat",
        name="🥊 SPORT DA COMBATTIMENTO",
        genres=(
            "UFC",
            "MMA",
            "Boxe",
            "Wrestling e WWE",
        ),
    ),
    CatalogDefinition(
        id="altri_sport",
        silo="altri_sport",
        name="🎯 ALTRI SPORT",
        genres=(
            "Golf",
            "Freccette / Darts",
            "Rugby e AFL",
            "Ciclismo",
            "Cricket",
            "Pallamano",
            "Sport Invernali",
            "Sport Olimpici e Atletica",
            "Biliardo, Padel e Altri",
        ),
    ),
)

_CATALOG_BY_ID: dict[str, CatalogDefinition] = {c.id: c for c in CATALOG_DEFINITIONS}

_SILO_TO_CATALOG_ID: dict[str, str] = {
    "calcio_ita": "calcio_italiano",
    "football": "calcio_estero",
    "tennis": "tennis",
    "motori": "motori",
    "basket": "basket",
    "volley": "volley",
    "nfl": "football_americano",
    "baseball": "baseball",
    "hockey": "hockey",
    "combat": "combattimento",
    "rugby": "altri_sport",
    "handball": "altri_sport",
    "altri_sport": "altri_sport",
}

_LEGACY_CATALOG_ID_MAP: dict[str, str] = {
    "ss_calcio_ita": "calcio_italiano",
    "ss_football": "calcio_estero",
    "ss_tennis": "tennis",
    "ss_motori": "motori",
    "ss_basket": "basket",
    "ss_volley": "volley",
    "ss_nfl": "football_americano",
    "ss_baseball": "baseball",
    "ss_hockey": "hockey",
    "ss_combat": "combattimento",
    "ss_rugby": "altri_sport",
    "ss_handball": "altri_sport",
    "ss_altri_sport": "altri_sport",
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
    "rugby": "Rugby e AFL",
    "handball": "Pallamano",
    "altri_sport": "Altri Sport",
}


def _slugify_genre(genre: str) -> str:
    """Convert a display genre name into a stable lowercase underscore slug."""
    norm = unicodedata.normalize("NFKD", genre)
    ascii_text = "".join(ch for ch in norm if not unicodedata.combining(ch))
    return re.sub(r"[^a-z0-9]+", "_", ascii_text.lower()).strip("_")


def _catalog_emoji(name: str) -> str:
    """Extract the leading emoji token from a catalog display name."""
    parts = name.split(" ", 1)
    return parts[0] if parts else ""


NUVIO_CATALOG_LOOKUP: dict[str, tuple[str, str]] = {
    f"nuvio_{cat.id}_{_slugify_genre(genre)}": (cat.id, genre)
    for cat in CATALOG_DEFINITIONS
    for genre in cat.genres
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
    "serie-a": ("calcio_italiano", ["Serie A"]),
    "serie-b": ("calcio_italiano", ["Serie B"]),
    "serie-c": ("calcio_italiano", ["Serie C"]),
    "coppa-italia": ("calcio_italiano", ["Coppa Italia e Supercoppa"]),
    "supercoppa-italiana": ("calcio_italiano", ["Coppa Italia e Supercoppa"]),
    "serie-a-femminile": ("calcio_italiano", ["Calcio Femminile"]),
    "coppa-italia-femminile": ("calcio_italiano", ["Calcio Femminile"]),
    "primavera-1": ("calcio_italiano", ["Primavera e Giovanili"]),
    "primavera-2": ("calcio_italiano", ["Primavera e Giovanili"]),
    "coppa-italia-primavera": ("calcio_italiano", ["Primavera e Giovanili"]),
    "champions-league": ("calcio_estero", ["Champions League"]),
    "uefa-super-cup": ("calcio_estero", ["Champions League"]),
    "club-world-cup": ("calcio_estero", ["Champions League"]),
    "uefa-womens-champions-league": ("calcio_estero", ["Champions League Femminile"]),
    "europa-league": ("calcio_estero", ["Europa e Conference League"]),
    "conference-league": ("calcio_estero", ["Europa e Conference League"]),
    "premier-league": ("calcio_estero", ["Premier League"]),
    "championship": ("calcio_estero", ["Premier League"]),
    "fa-cup": ("calcio_estero", ["Premier League"]),
    "efl-cup": ("calcio_estero", ["Premier League"]),
    "wsl-women": ("calcio_estero", ["Premier League"]),
    "la-liga": ("calcio_estero", ["La Liga"]),
    "la-liga-2": ("calcio_estero", ["La Liga"]),
    "copa-del-rey": ("calcio_estero", ["La Liga"]),
    "liga-f-women": ("calcio_estero", ["La Liga"]),
    "bundesliga": ("calcio_estero", ["Bundesliga e Ligue 1"]),
    "2-bundesliga": ("calcio_estero", ["Bundesliga e Ligue 1"]),
    "dfb-pokal": ("calcio_estero", ["Bundesliga e Ligue 1"]),
    "frauen-bundesliga": ("calcio_estero", ["Bundesliga e Ligue 1"]),
    "ligue-1": ("calcio_estero", ["Bundesliga e Ligue 1"]),
    "ligue-2": ("calcio_estero", ["Bundesliga e Ligue 1"]),
    "coupe-de-france": ("calcio_estero", ["Bundesliga e Ligue 1"]),
    "d1-arkema": ("calcio_estero", ["Bundesliga e Ligue 1"]),
    "primeira-liga": ("calcio_estero", ["Altri Campionati Europei"]),
    "eredivisie": ("calcio_estero", ["Altri Campionati Europei"]),
    "jupiler-pro-league": ("calcio_estero", ["Altri Campionati Europei"]),
    "super-lig": ("calcio_estero", ["Altri Campionati Europei"]),
    "scottish-premiership": ("calcio_estero", ["Altri Campionati Europei"]),
    "super-league-greece": ("calcio_estero", ["Altri Campionati Europei"]),
    "brasileirao-serie-a": ("calcio_estero", ["Americhe e Leghe Extra-UE"]),
    "brasileirao-serie-b": ("calcio_estero", ["Americhe e Leghe Extra-UE"]),
    "ecuador-ligapro-serie-a": ("calcio_estero", ["Americhe e Leghe Extra-UE"]),
    "colombia-primera-a": ("calcio_estero", ["Americhe e Leghe Extra-UE"]),
    "copa-libertadores": ("calcio_estero", ["Americhe e Leghe Extra-UE"]),
    "copa-sudamericana": ("calcio_estero", ["Americhe e Leghe Extra-UE"]),
    "liga-profesional-argentina": ("calcio_estero", ["Americhe e Leghe Extra-UE"]),
    "mls": ("calcio_estero", ["Americhe e Leghe Extra-UE"]),
    "liga-mx": ("calcio_estero", ["Americhe e Leghe Extra-UE"]),
    "saudi-pro-league": ("calcio_estero", ["Americhe e Leghe Extra-UE"]),
    "nwsl": ("calcio_estero", ["Americhe e Leghe Extra-UE"]),
    "uefa-youth-league": ("calcio_estero", ["Europei Under 21 e Nazionali Giovanili"]),
    "euro-u21": ("calcio_estero", ["Europei Under 21 e Nazionali Giovanili"]),
    "u21-international": ("calcio_estero", ["Europei Under 21 e Nazionali Giovanili"]),
    "nations-league": ("calcio_estero", ["Nazionali e Amichevoli"]),
    "world-cup": ("calcio_estero", ["Nazionali e Amichevoli"]),
    "euro-championship": ("calcio_estero", ["Nazionali e Amichevoli"]),
    "copa-america": ("calcio_estero", ["Nazionali e Amichevoli"]),
    "afcon": ("calcio_estero", ["Nazionali e Amichevoli"]),
    "international-friendlies": ("calcio_estero", ["Nazionali e Amichevoli"]),
    "australian-open": ("tennis", ["Grandi Slam"]),
    "roland-garros": ("tennis", ["Grandi Slam"]),
    "wimbledon": ("tennis", ["Grandi Slam"]),
    "us-open": ("tennis", ["Grandi Slam"]),
    "atp-finals": ("tennis", ["ATP"]),
    "atp-tour": ("tennis", ["ATP"]),
    "wta-finals": ("tennis", ["WTA"]),
    "wta-tour": ("tennis", ["WTA"]),
    "atp-challenger": ("tennis", ["Challenger e Altri"]),
    "itf-tennis": ("tennis", ["Challenger e Altri"]),
    "davis-cup": ("tennis", ["Coppa Davis e BJK Cup"]),
    "bjk-cup": ("tennis", ["Coppa Davis e BJK Cup"]),
    "united-cup": ("tennis", ["Coppa Davis e BJK Cup"]),
    "laver-cup": ("tennis", ["Coppa Davis e BJK Cup"]),
    "nba": ("basket", ["NBA"]),
    "wnba": ("basket", ["WNBA e Femminile"]),
    "euroleague-women": ("basket", ["WNBA e Femminile"]),
    "serie-a1-femminile-basket": ("basket", ["WNBA e Femminile"]),
    "euroleague": ("basket", ["Eurolega ed Eurocup"]),
    "eurocup-basket": ("basket", ["Eurolega ed Eurocup"]),
    "basketball-champions-league": ("basket", ["Eurolega ed Eurocup"]),
    "fiba-international": ("basket", ["FIBA e Tornei Nazionali"]),
    "lba-serie-a": ("basket", ["LBA Serie A"]),
    "serie-a2-basket": ("basket", ["LBA Serie A"]),
    "liga-acb": ("basket", ["Campionati Esteri ed NBL"]),
    "bbl-germany": ("basket", ["Campionati Esteri ed NBL"]),
    "betclic-elite": ("basket", ["Campionati Esteri ed NBL"]),
    "nbl-australia": ("basket", ["Campionati Esteri ed NBL"]),
    "ncaa-basketball": ("basket", ["NCAA e College Basket"]),
    "formula-1": ("motori", ["Formula 1"]),
    "formula-2": ("motori", ["Formula 1"]),
    "formula-3": ("motori", ["Formula 1"]),
    "formula-e": ("motori", ["Formula 1"]),
    "motogp": ("motori", ["MotoGP e Superbike"]),
    "moto2": ("motori", ["MotoGP e Superbike"]),
    "moto3": ("motori", ["MotoGP e Superbike"]),
    "wsbk": ("motori", ["MotoGP e Superbike"]),
    "wrc": ("motori", ["Rally e WRC"]),
    "indycar": ("motori", ["NASCAR e IndyCar"]),
    "nascar": ("motori", ["NASCAR e IndyCar"]),
    "wec": ("motori", ["NASCAR e IndyCar"]),
    "ufc": ("combattimento", ["UFC"]),
    "bellator": ("combattimento", ["MMA"]),
    "pfl": ("combattimento", ["MMA"]),
    "one-championship": ("combattimento", ["MMA"]),
    "ksw": ("combattimento", ["MMA"]),
    "oktagon": ("combattimento", ["MMA"]),
    "boxing": ("combattimento", ["Boxe"]),
    "wwe": ("combattimento", ["Wrestling e WWE"]),
    "aew": ("combattimento", ["Wrestling e WWE"]),
    "superlega-volley": ("volley", ["Superlega e Serie A1"]),
    "serie-a2-volley": ("volley", ["Superlega e Serie A1"]),
    "serie-a1-femminile-volley": ("volley", ["Volley Femminile"]),
    "sultanlar-ligi": ("volley", ["Volley Femminile"]),
    "cev-champions-league": ("volley", ["Champions League Volley"]),
    "vnl-volley": ("volley", ["Nazionali e Internazionale"]),
    "plusliga": ("volley", ["Nazionali e Internazionale"]),
    "nfl": ("football_americano", ["NFL"]),
    "ncaaf": ("football_americano", ["NCAA College Football"]),
    "cfl": ("football_americano", ["CFL e Altre Leghe"]),
    "ufl": ("football_americano", ["CFL e Altre Leghe"]),
    "nhl": ("hockey", ["NHL"]),
    "khl": ("hockey", ["KHL e Leghe Europee"]),
    "shl": ("hockey", ["KHL e Leghe Europee"]),
    "liiga": ("hockey", ["KHL e Leghe Europee"]),
    "del-hockey": ("hockey", ["KHL e Leghe Europee"]),
    "nl-switzerland": ("hockey", ["KHL e Leghe Europee"]),
    "iihf-world-championship": ("hockey", ["Mondiali e Nazionali"]),
    "mlb": ("baseball", ["MLB"]),
    "ncaa-baseball": ("baseball", ["NCAA College Baseball"]),
    "npb": ("baseball", ["Campionati Internazionali"]),
    "kbo": ("baseball", ["Campionati Internazionali"]),
    "wbc": ("baseball", ["Campionati Internazionali"]),
    "six-nations": ("altri_sport", ["Rugby e AFL"]),
    "rugby-championship": ("altri_sport", ["Rugby e AFL"]),
    "rugby-world-cup": ("altri_sport", ["Rugby e AFL"]),
    "rugby-champions-cup": ("altri_sport", ["Rugby e AFL"]),
    "rugby-challenge-cup": ("altri_sport", ["Rugby e AFL"]),
    "urc-rugby": ("altri_sport", ["Rugby e AFL"]),
    "top-14": ("altri_sport", ["Rugby e AFL"]),
    "premiership-rugby": ("altri_sport", ["Rugby e AFL"]),
    "super-rugby": ("altri_sport", ["Rugby e AFL"]),
    "serie-a-elite-rugby": ("altri_sport", ["Rugby e AFL"]),
    "afl": ("altri_sport", ["Rugby e AFL"]),
    "ehf-champions-league": ("altri_sport", ["Pallamano"]),
    "ehf-european-league": ("altri_sport", ["Pallamano"]),
    "handball-bundesliga": ("altri_sport", ["Pallamano"]),
    "liga-asobal": ("altri_sport", ["Pallamano"]),
    "starligue": ("altri_sport", ["Pallamano"]),
    "serie-a-gold-handball": ("altri_sport", ["Pallamano"]),
    "ihf-handball": ("altri_sport", ["Pallamano"]),
    "golf-major": ("altri_sport", ["Golf"]),
    "darts": ("altri_sport", ["Freccette / Darts"]),
    "cycling-major": ("altri_sport", ["Ciclismo"]),
    "cricket-major": ("altri_sport", ["Cricket"]),
    "winter-sports": ("altri_sport", ["Sport Invernali"]),
    "athletics": ("altri_sport", ["Sport Olimpici e Atletica"]),
    "olympics": ("altri_sport", ["Sport Olimpici e Atletica"]),
    "snooker": ("altri_sport", ["Biliardo, Padel e Altri"]),
    "padel": ("altri_sport", ["Biliardo, Padel e Altri"]),
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


def build_nuvio_catalogs(media_type: str = "tv") -> list[dict[str, Any]]:
    """Build flattened independent Nuvio catalog definitions (one per genre, extra=[])."""
    catalogs: list[dict[str, Any]] = []
    for cat in CATALOG_DEFINITIONS:
        emoji = _catalog_emoji(cat.name)
        for genre in cat.genres:
            catalogs.append(
                {
                    "type": media_type,
                    "id": f"nuvio_{cat.id}_{_slugify_genre(genre)}",
                    "name": f"{emoji} {genre}" if emoji else genre,
                    "extra": [],
                }
            )
    return catalogs


def _normalize_catalog_id(value: str) -> str:
    clean = (value or "").strip().lower()
    if not clean:
        return ""
    if clean in _CATALOG_BY_ID:
        return clean
    if clean in _LEGACY_CATALOG_ID_MAP:
        return _LEGACY_CATALOG_ID_MAP[clean]
    if clean in ("live", LIVE_CATALOG_ID):
        return LIVE_CATALOG_ID
    if clean in _SILO_TO_CATALOG_ID:
        return _SILO_TO_CATALOG_ID[clean]
    return clean


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

        # Level 0: Early Simulcast routing (must emit only valid CATALOG_DEFINITIONS genres)
        if _REDZONE_RE.search(combined_text):
            return "football_americano", ["NFL"]

        if _DIRETTA_GOL_RE.search(combined_text):
            if _EURO_COMP_HINT_RE.search(combined_text) or silo_norm == "football":
                return "calcio_estero", ["Champions League"]
            return "calcio_italiano", ["Serie A"]

        if _MULTICAMPO_RE.search(combined_text):
            if silo_norm == "tennis" or _TENNIS_HINT_RE.search(combined_text):
                low = combined_text.lower()
                if any(k in low for k in ("wimbledon", "roland garros", "australian open", "us open")):
                    return "tennis", ["Grandi Slam"]
                if "davis" in low or "bjk" in low or "billie jean" in low:
                    return "tennis", ["Coppa Davis e BJK Cup"]
                if "wta" in low or is_women is True:
                    return "tennis", ["WTA"]
                return "tennis", ["ATP"]
            if silo_norm in ("calcio_ita", "football"):
                if silo_norm == "calcio_ita":
                    return "calcio_italiano", ["Serie A"]
                return "calcio_estero", ["Altri Campionati Europei"]

        # Level 1: Validated Canonical Competition routing
        if canonical_competition and canonical_competition in _CANONICAL_SLUG_ROUTING:
            cat_id, genres = _CANONICAL_SLUG_ROUTING[canonical_competition]
            return cat_id, list(genres)

        if competition:
            lookup_silo = silo_norm if silo_norm in SPORT_SILOS else "football"
            league_match = self.taxonomy.match_league(lookup_silo, competition, country)
            if league_match is not None:
                cat_id = _normalize_catalog_id(league_match.catalog_target or league_match.silo)
                if cat_id not in _CATALOG_BY_ID:
                    cat_id = _SILO_TO_CATALOG_ID.get(league_match.silo, "altri_sport")
                if league_match.genre and league_match.genre in _CATALOG_BY_ID[cat_id].genres:
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
            if is_italian:
                if eff_women:
                    return "calcio_italiano", ["Calcio Femminile"]
                if eff_youth:
                    return "calcio_italiano", ["Primavera e Giovanili"]
                comp_low = competition.lower()
                if "serie a" in comp_low:
                    return "calcio_italiano", ["Serie A"]
                if "serie b" in comp_low:
                    return "calcio_italiano", ["Serie B"]
                if "coppa" in comp_low or "supercoppa" in comp_low:
                    return "calcio_italiano", ["Coppa Italia e Supercoppa"]
                return "calcio_italiano", ["Serie C"]

            if eff_women:
                if "champions" in competition.lower() or "uwcl" in competition.lower():
                    return "calcio_estero", ["Champions League Femminile"]
                return "calcio_estero", ["Altri Campionati Europei"]
            if eff_youth:
                return "calcio_estero", ["Europei Under 21 e Nazionali Giovanili"]
            return "calcio_estero", ["Altri Campionati Europei"]

        if silo_norm == "tennis":
            comp_low = competition.lower()
            if any(k in comp_low for k in ("wimbledon", "roland garros", "french open", "australian open", "us open")):
                return "tennis", ["Grandi Slam"]
            if any(k in comp_low for k in ("davis", "bjk", "billie jean", "united cup", "laver cup")):
                return "tennis", ["Coppa Davis e BJK Cup"]
            if "challenger" in comp_low or "itf" in comp_low:
                return "tennis", ["Challenger e Altri"]
            if eff_women or "wta" in comp_low:
                return "tennis", ["WTA"]
            if "atp" in comp_low:
                return "tennis", ["ATP"]
            return "tennis", ["Challenger e Altri"]

        if silo_norm == "basket":
            comp_low = competition.lower()
            if eff_women or "wnba" in comp_low:
                return "basket", ["WNBA e Femminile"]
            if "nba" in comp_low:
                return "basket", ["NBA"]
            if country_norm == "italy":
                return "basket", ["LBA Serie A"]
            if any(k in comp_low for k in ("euroleague", "eurolega", "eurocup", "champions")):
                return "basket", ["Eurolega ed Eurocup"]
            if "ncaa" in comp_low or "college" in comp_low:
                return "basket", ["NCAA e College Basket"]
            if "fiba" in comp_low or "eurobasket" in comp_low or "world cup" in comp_low:
                return "basket", ["FIBA e Tornei Nazionali"]
            return "basket", ["Campionati Esteri ed NBL"]

        if silo_norm == "motori":
            comp_low = competition.lower()
            if "formula 1" in comp_low or re.search(r"\bf1\b", comp_low) or "formula e" in comp_low:
                return "motori", ["Formula 1"]
            if any(k in comp_low for k in ("motogp", "moto gp", "moto2", "moto3", "superbike", "wsbk")):
                return "motori", ["MotoGP e Superbike"]
            if any(k in comp_low for k in ("wrc", "rally", "dakar", "erc")):
                return "motori", ["Rally e WRC"]
            return "motori", ["NASCAR e IndyCar"]

        if silo_norm == "combat":
            comp_low = competition.lower()
            if "ufc" in comp_low:
                return "combattimento", ["UFC"]
            if any(k in comp_low for k in ("boxe", "boxing", "pugilato")):
                return "combattimento", ["Boxe"]
            if any(k in comp_low for k in ("wwe", "aew", "wrestling")):
                return "combattimento", ["Wrestling e WWE"]
            return "combattimento", ["MMA"]

        if silo_norm == "volley":
            comp_low = competition.lower()
            if eff_women or "femminil" in comp_low or "women" in comp_low:
                return "volley", ["Volley Femminile"]
            if "champions" in comp_low or "cev" in comp_low:
                return "volley", ["Champions League Volley"]
            if country_norm == "italy" or "superlega" in comp_low or "serie a1" in comp_low:
                return "volley", ["Superlega e Serie A1"]
            return "volley", ["Nazionali e Internazionale"]

        if silo_norm == "nfl":
            comp_low = competition.lower()
            if "ncaa" in comp_low or "college" in comp_low:
                return "football_americano", ["NCAA College Football"]
            if "cfl" in comp_low or "ufl" in comp_low:
                return "football_americano", ["CFL e Altre Leghe"]
            return "football_americano", ["NFL"]

        if silo_norm == "baseball":
            comp_low = competition.lower()
            if "ncaa" in comp_low or "college" in comp_low:
                return "baseball", ["NCAA College Baseball"]
            if "mlb" in comp_low:
                return "baseball", ["MLB"]
            return "baseball", ["Campionati Internazionali"]

        if silo_norm == "hockey":
            comp_low = competition.lower()
            if "nhl" in comp_low:
                return "hockey", ["NHL"]
            if any(k in comp_low for k in ("iihf", "world", "mondial", "olympic", "nazional")):
                return "hockey", ["Mondiali e Nazionali"]
            return "hockey", ["KHL e Leghe Europee"]

        if silo_norm == "rugby":
            return "altri_sport", ["Rugby e AFL"]

        if silo_norm == "handball":
            return "altri_sport", ["Pallamano"]

        if silo_norm == "altri_sport":
            comp_low = competition.lower()
            if "golf" in comp_low or "pga" in comp_low or "ryder" in comp_low:
                return "altri_sport", ["Golf"]
            if "dart" in comp_low or "freccette" in comp_low or "pdc" in comp_low:
                return "altri_sport", ["Freccette / Darts"]
            if "rugby" in comp_low or "afl" in comp_low or "six nations" in comp_low:
                return "altri_sport", ["Rugby e AFL"]
            if any(k in comp_low for k in ("ciclismo", "cycling", "giro d'italia", "tour de france", "vuelta", "uci")):
                return "altri_sport", ["Ciclismo"]
            if "cricket" in comp_low or "ipl" in comp_low or "ashes" in comp_low:
                return "altri_sport", ["Cricket"]
            if "pallamano" in comp_low or "handball" in comp_low or "ehf" in comp_low:
                return "altri_sport", ["Pallamano"]
            if any(k in comp_low for k in ("sci", "ski", "biathlon", "fis", "invernali", "winter")):
                return "altri_sport", ["Sport Invernali"]
            if any(k in comp_low for k in ("olimp", "olympic", "atletica", "athletics", "diamond league")):
                return "altri_sport", ["Sport Olimpici e Atletica"]
            return "altri_sport", ["Biliardo, Padel e Altri"]

        # Level 4: Fallback
        return "altri_sport", ["Biliardo, Padel e Altri"]


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
    if catalog_id in NUVIO_CATALOG_LOOKUP:
        base_cat_id, mapped_genre = NUVIO_CATALOG_LOOKUP[catalog_id]
        req_cat = base_cat_id
        effective_genre = selected_genre or mapped_genre
    else:
        req_cat = _normalize_catalog_id(catalog_id)
        effective_genre = selected_genre

    if not req_cat:
        return False

    genre_norm = (effective_genre or "").strip().casefold()
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
