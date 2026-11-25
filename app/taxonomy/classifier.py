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

# Level 0 Simulcast patterns (13 families)
_SIMULCAST_RE = re.compile(
    r"\b(?:red\s*zone|redzone|golazo(?:\s*show)?|simulcast|diretta\s*gol|direttagol|"
    r"zona\s*gol|zona\s*serie\s*[ab]|zona\s*dazn|zona\s*europa|konferenz|"
    r"multifoot|multiplex|multiliga|multichampions|multipremier|multigoal|"
    r"multicampo|diretta\s*campo|goal\s*rush|diretta\s*basket|diretta\s*eurolega|"
    r"diretta\s*tennis|early\s*rounds|tennis\s*channel|whip\s*around)\b",
    re.IGNORECASE,
)
_VS_SEP_RE = re.compile(r"\s+(?:vs\.?|v\.?)\s+", re.IGNORECASE)
_TENNIS_MULTI_RE = re.compile(r"\b(?:atp|wta)\b", re.IGNORECASE)
_EURO_COMP_HINT_RE = re.compile(
    r"\b(?:champions\s+league|europa\s+league|conference\s+league|premier\s+league|la\s*liga|bundesliga|ligue\s*1)\b",
    re.IGNORECASE,
)
_TENNIS_HINT_RE = re.compile(r"\b(?:tennis|atp|wta|slam|wimbledon|roland\s+garros|us\s+open|australian\s+open)\b", re.IGNORECASE)

# Silo resolution regexes (used by resolve_sport_silo fallback)
_WINTER_ATHLETICS_GUARD_RE = re.compile(
    r"\b(?:pattinaggio|skating|sci|ski|curling|biathlon|atletica|athletics|nuoto|swimming)\b",
    re.IGNORECASE,
)
_EXPLICIT_MOTOR_SERIES_RE = re.compile(
    r"\b(?:f[123e]|formula\s*(?:1|2|3|e|one)|motogp|moto\s*gp|moto[23e]|superbike|wsbk|worldsbk|rally|wrc|nascar|indycar|wec|dtm|imsa)\b",
    re.IGNORECASE,
)
_SILO_MOTORSPORT_RE = re.compile(
    r"\b(?:formula\s*[123e]|f[123e]|formula\s*one|motogp|moto\s*gp|moto\s*[23e]|"
    r"motomondiale|motociclismo|automobilismo|superbike|wsbk|worldsbk|wec|"
    r"world\s*endurance|le\s*mans|imsa|dtm|supercars|nascar|indycar|indy\s*500|"
    r"rally|wrc|dakar|erc|mxgp|motocross|supercross|gp|gran\s*premio)\b",
    re.IGNORECASE,
)
_SILO_TENNIS_RE = re.compile(
    r"\b(?:atp|wta|bjk|davis\s*cup|(?<!afc\s)wimbledon|us\s*open|roland\s*garros|australian\s*open|french\s*open)\b",
    re.IGNORECASE,
)
_SILO_BASKET_RE = re.compile(r"\b(?:nba|wnba|euroleague|eurolega|eurocup|lba|nbl|fiba|cba|acb|bbl)\b", re.IGNORECASE)
_SILO_COMBAT_RE = re.compile(r"\b(?:ufc|wwe|aew|bellator|boxing|boxe|mma|pfl|one\s*championship)\b", re.IGNORECASE)
_SILO_HOCKEY_RE = re.compile(r"\b(?:nhl|khl|shl|liiga|extraliga|ice\s*hockey)\b", re.IGNORECASE)
_SILO_NFL_RE = re.compile(r"\b(?:nfl|cfl|ufl|cfb|college\s*football|american\s*football|super\s*bowl)\b", re.IGNORECASE)
_SILO_BASEBALL_RE = re.compile(r"\b(?:baseball|mlb|npb|kbo|lmb)\b", re.IGNORECASE)
_SILO_VOLLEY_RE = re.compile(r"\b(?:volleyball|volley|pallavolo|superlega|plusliga)\b", re.IGNORECASE)
_SILO_RUGBY_RE = re.compile(r"\b(?:rugby|six\s*nations|sei\s*nazioni|urc|top\s*14|super\s*rugby|afl|aflw)\b", re.IGNORECASE)
_SILO_HANDBALL_RE = re.compile(r"\b(?:handball|pallamano|ehf|ihf)\b", re.IGNORECASE)
_SILO_FOOTBALL_CUE_RE = re.compile(
    r"\b(?:fc|cf|calcio|football|soccer|serie\s*[abc]|premier\s*league|la\s*liga|bundesliga|ligue\s*[12]|"
    r"champions\s*league|europa\s*league|conference\s*league|copa|coppa|primavera|u23|next\s*gen|futuro)\b",
    re.IGNORECASE,
)
_OTHER_SPORTS_EXCLUSION_RE = re.compile(
    r"\b(?:snooker|padel|golf|pga|lpga|darts|freccette|pdc|rugby|afl|cricket|ciclismo|cycling|handball|pallamano)\b",
    re.IGNORECASE,
)


def resolve_sport_silo(sport_hint: str = "", league: str = "", title: str = "") -> str:
    """Infer canonical internal sport silo from an explicit hint, league, or event title."""
    hint = (sport_hint or "").strip().lower()
    hint_map = {
        "calcio_ita": "calcio_ita",
        "calcio_italiano": "calcio_ita",
        "football": "football",
        "soccer": "football",
        "calcio": "football",
        "tennis": "tennis",
        "motori": "motori",
        "motor-sports": "motori",
        "motorsport": "motori",
        "motorsports": "motori",
        "basket": "basket",
        "basketball": "basket",
        "volley": "volley",
        "volleyball": "volley",
        "nfl": "nfl",
        "american-football": "nfl",
        "american_football": "nfl",
        "baseball": "baseball",
        "hockey": "hockey",
        "ice-hockey": "hockey",
        "combat": "combat",
        "fight": "combat",
        "combattimento": "combat",
        "rugby": "rugby",
        "handball": "handball",
        "pallamano": "handball",
    }
    if hint in hint_map:
        return hint_map[hint]

    eval_text = f"{league} {title}".strip().lower()
    if not eval_text:
        return "altri_sport"

    if _WINTER_ATHLETICS_GUARD_RE.search(eval_text) and not _EXPLICIT_MOTOR_SERIES_RE.search(eval_text):
        return "altri_sport"

    if _SILO_MOTORSPORT_RE.search(eval_text):
        return "motori"
    if _SILO_TENNIS_RE.search(eval_text):
        return "tennis"
    if _SILO_BASKET_RE.search(eval_text):
        return "basket"
    if _SILO_COMBAT_RE.search(eval_text):
        return "combat"
    if _SILO_HOCKEY_RE.search(eval_text) or (
        re.search(r"\bdel\b", eval_text) and any(k in eval_text for k in ("hockey", "eishockey"))
    ):
        return "hockey"
    if _SILO_NFL_RE.search(eval_text):
        return "nfl"
    if _SILO_BASEBALL_RE.search(eval_text):
        return "baseball"
    if _SILO_VOLLEY_RE.search(eval_text):
        return "volley"
    if _SILO_RUGBY_RE.search(eval_text):
        return "rugby"
    if _SILO_HANDBALL_RE.search(eval_text):
        return "handball"

    if _SILO_FOOTBALL_CUE_RE.search(eval_text) and not _OTHER_SPORTS_EXCLUSION_RE.search(eval_text):
        return "football"

    return "altri_sport"


def _resolve_simulcast(
    combined_text: str,
    silo_norm: str,
    is_women: bool | None,
) -> tuple[str, list[str]] | None:
    """Resolve all 13 simulcast / multi-court broadcast families."""
    is_tennis_multi = (
        bool(_TENNIS_MULTI_RE.search(combined_text))
        and bool(re.search(r"[&+,]", combined_text))
        and not bool(_VS_SEP_RE.search(combined_text))
    )
    if not _SIMULCAST_RE.search(combined_text) and not is_tennis_multi:
        return None

    t = combined_text.lower().replace("-", " ").replace("_", " ")

    # 1. NFL RedZone
    if re.search(r"\b(?:nfl\s+)?red\s*zone\b|\bredzone\b", t):
        return "football_americano", ["NFL"]

    # 2. Golazo Show
    if "golazo" in t:
        if any(k in t for k in ("europa", "conference", "uel", "uecl")):
            return "calcio_estero", ["Europa e Conference League"]
        return "calcio_estero", ["Champions League"]

    # 3. Serie A / Zona Serie A
    if re.search(r"\b(?:zona\s*)?serie\s*a\b", t):
        return "calcio_italiano", ["Serie A"]

    # 4. Serie B / Zona Serie B
    if re.search(r"\b(?:zona\s*)?serie\s*b\b", t):
        return "calcio_italiano", ["Serie B"]

    # 5. Champions League / MultiChampions
    if re.search(r"\b(?:champions|ucl|multichampions)\b", t):
        return "calcio_estero", ["Champions League"]

    # 6. Europa / Conference League
    if re.search(r"\b(?:europa|conference|uel|uecl)\b", t):
        return "calcio_estero", ["Europa e Conference League"]

    # 7. Premier League / Goal Rush / MultiPremier
    if re.search(r"\b(?:premier|multipremier|goal\s*rush)\b", t):
        return "calcio_estero", ["Premier League"]

    # 8. Bundesliga Konferenz
    if re.search(r"\b(?:bundesliga|konferenz)\b", t):
        return "calcio_estero", ["Bundesliga e Ligue 1"]

    # 9. LaLiga / MultiLiga
    if re.search(r"\b(?:laliga|la\s*liga|multiliga)\b", t):
        return "calcio_estero", ["La Liga"]

    # 10. Ligue 1 / Multifoot / Multiplex
    if re.search(r"\b(?:ligue\s*1|multifoot|multiplex)\b", t):
        return "calcio_estero", ["Bundesliga e Ligue 1"]

    # 11. Basket simulcast (NBA / NCAA / Eurolega / LBA)
    if re.search(r"\b(?:basket|eurolega|euroleague|nba|ncaa)\b", t):
        if "nba" in t:
            return "basket", ["NBA"]
        if "ncaa" in t or "college" in t:
            return "basket", ["NCAA e College Basket"]
        if "euro" in t:
            return "basket", ["Eurolega ed Eurocup"]
        return "basket", ["LBA Serie A"]

    # 12. Tennis multi-court / early rounds / tennis channel
    if (
        silo_norm == "tennis"
        or _TENNIS_HINT_RE.search(combined_text)
        or re.search(r"\b(?:diretta\s*tennis|early\s*rounds|tennis\s*channel)\b", t)
        or is_tennis_multi
    ):
        if any(k in t for k in ("wimbledon", "roland garros", "australian open", "us open")):
            return "tennis", ["Grandi Slam"]
        if "davis" in t or "bjk" in t or "billie jean" in t:
            return "tennis", ["Coppa Davis e BJK Cup"]
        has_atp = "atp" in t
        has_wta = "wta" in t
        if has_atp and has_wta:
            return "tennis", ["Challenger e Altri"]
        if has_wta or is_women is True:
            return "tennis", ["WTA"]
        return "tennis", ["ATP"]

    # 13. Generic Diretta Gol / Zona Gol / Zona DAZN / Multicampo / Simulcast
    if silo_norm == "calcio_ita" or any(k in t for k in ("italia", "serie", "dazn", "sky")):
        return "calcio_italiano", ["Serie A"]
    if _EURO_COMP_HINT_RE.search(combined_text):
        return "calcio_estero", ["Champions League"]
    if silo_norm == "football":
        return "calcio_estero", ["Altri Campionati Europei"]
    return "calcio_italiano", ["Serie A"]


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
        simulcast_res = _resolve_simulcast(combined_text, silo_norm, is_women)
        if simulcast_res is not None:
            return simulcast_res

        # Level 1: Validated Canonical Competition routing (Single Source of Truth in LeagueTaxonomy)
        if canonical_competition:
            resolved = self.taxonomy.resolve_catalog_and_genre(canonical_competition)
            if resolved is not None:
                cat_id = _normalize_catalog_id(resolved[0])
                if cat_id in _CATALOG_BY_ID and resolved[1] and resolved[1][0] in _CATALOG_BY_ID[cat_id].genres:
                    return cat_id, list(resolved[1])

        if competition:
            lookup_silo = silo_norm if silo_norm in SPORT_SILOS else "football"
            league_match = self.taxonomy.match_league(lookup_silo, competition, country)
            if league_match is not None:
                cat_id = _normalize_catalog_id(league_match.catalog_target or league_match.silo)
                if cat_id not in _CATALOG_BY_ID:
                    cat_id = _SILO_TO_CATALOG_ID.get(league_match.silo, "altri_sport")
                if league_match.genre and league_match.genre in _CATALOG_BY_ID[cat_id].genres:
                    return cat_id, [league_match.genre]

        # Level 2 & Level 3: Sport Silo resolution + Sport Sub-classifiers
        if not silo_norm or silo_norm in ("other", "altri_sport", "altri-sport"):
            silo_norm = resolve_sport_silo(silo_norm, competition, title)

        comp_women, comp_youth = detect_demographics(competition, country)
        eff_women = comp_women if is_women is None else is_women
        eff_youth = comp_youth if is_youth is None else is_youth

        if silo_norm in ("calcio_ita", "football"):
            is_italian = silo_norm == "calcio_ita" or country_norm == "italy"
            if is_italian:
                if eff_women:
                    return "calcio_italiano", ["Calcio Femminile"]
                comb_low = combined_text.lower()
                if any(k in comb_low for k in ("next gen", "milan futuro", "atalanta u23")):
                    return "calcio_italiano", ["Serie C"]
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
            if any(k in comp_low for k in ("fiba", "eurobasket", "world cup", "mondiali", "europei")):
                return "basket", ["FIBA e Tornei Nazionali"]
            return "basket", ["Campionati Esteri ed NBL"]

        if silo_norm == "motori":
            eval_low = combined_text.lower()
            if (
                "formula 1" in eval_low
                or "formula 2" in eval_low
                or "formula 3" in eval_low
                or "formula e" in eval_low
                or re.search(r"\bf[123e]\b", eval_low)
            ):
                return "motori", ["Formula 1"]
            if any(k in eval_low for k in ("motogp", "moto gp", "moto2", "moto3", "superbike", "wsbk")):
                return "motori", ["MotoGP e Superbike"]
            if any(k in eval_low for k in ("wrc", "rally", "dakar", "erc")):
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
