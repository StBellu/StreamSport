"""Canonical league taxonomy, strict silo boundaries, homonym disambiguation, and tier admission."""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass

from app.taxonomy.demographics import detect_demographics

SPORT_SILOS: tuple[str, ...] = (
    "calcio_ita",
    "football",
    "tennis",
    "basket",
    "motori",
    "combat",
    "volley",
    "nfl",
    "hockey",
    "baseball",
    "rugby",
    "handball",
    "altri_sport",
)

_COUNTRY_ALIASES: dict[str, str] = {
    "it": "italy",
    "ita": "italy",
    "italia": "italy",
    "italy": "italy",
    "br": "brazil",
    "bra": "brazil",
    "brasile": "brazil",
    "brazil": "brazil",
    "ec": "ecuador",
    "ecu": "ecuador",
    "ecuador": "ecuador",
    "co": "colombia",
    "col": "colombia",
    "colombia": "colombia",
    "ar": "argentina",
    "arg": "argentina",
    "argentina": "argentina",
    "en": "england",
    "eng": "england",
    "uk": "england",
    "gb": "england",
    "inghilterra": "england",
    "england": "england",
    "regno unito": "england",
    "united kingdom": "england",
    "great britain": "england",
    "es": "spain",
    "esp": "spain",
    "spagna": "spain",
    "spain": "spain",
    "de": "germany",
    "ger": "germany",
    "germania": "germany",
    "germany": "germany",
    "fr": "france",
    "fra": "france",
    "francia": "france",
    "france": "france",
    "pt": "portugal",
    "por": "portugal",
    "portogallo": "portugal",
    "portugal": "portugal",
    "nl": "netherlands",
    "ned": "netherlands",
    "olanda": "netherlands",
    "paesi bassi": "netherlands",
    "netherlands": "netherlands",
    "holland": "netherlands",
    "be": "belgium",
    "bel": "belgium",
    "belgio": "belgium",
    "belgium": "belgium",
    "tr": "turkey",
    "tur": "turkey",
    "turchia": "turkey",
    "turkey": "turkey",
    "gr": "greece",
    "gre": "greece",
    "grecia": "greece",
    "greece": "greece",
    "sco": "scotland",
    "scozia": "scotland",
    "scotland": "scotland",
    "sa": "saudi_arabia",
    "ksa": "saudi_arabia",
    "arabia saudita": "saudi_arabia",
    "saudi arabia": "saudi_arabia",
    "us": "usa",
    "usa": "usa",
    "stati uniti": "usa",
    "united states": "usa",
    "mx": "mexico",
    "mex": "mexico",
    "messico": "mexico",
    "mexico": "mexico",
    "eu": "europe",
    "europa": "europe",
    "europe": "europe",
    "world": "world",
    "mondo": "world",
    "international": "international",
    "internazionale": "international",
}


def normalize_country(country: str) -> str:
    if not country:
        return ""
    norm = unicodedata.normalize("NFKD", country)
    ascii_text = "".join(ch for ch in norm if not unicodedata.combining(ch))
    key = re.sub(r"\s+", " ", ascii_text.lower().strip())
    return _COUNTRY_ALIASES.get(key, key)


def _normalize_text(text: str) -> str:
    if not text:
        return ""
    norm = unicodedata.normalize("NFKD", text)
    ascii_text = "".join(ch for ch in norm if not unicodedata.combining(ch))
    return re.sub(r"\s+", " ", ascii_text.strip())


def _silo_rule_bucket(silo: str) -> str:
    s = (silo or "").strip().lower()
    if s in ("calcio_ita", "football"):
        return "football"
    if s in SPORT_SILOS:
        return s
    return "altri_sport"


@dataclass(frozen=True, slots=True)
class LeagueRule:
    """Canonical competition matching rule confined to a single sport silo."""

    canonical_slug: str
    canonical_name: str
    silo: str
    pattern: re.Pattern[str]
    tier: int = 1
    priority: int = 100
    country_include: tuple[str, ...] = ()
    country_exclude: tuple[str, ...] = ()
    requires_country_match: bool = False
    is_women: bool | None = None
    is_youth: bool | None = None
    catalog_target: str | None = None
    genre: str | None = None
    is_popular: bool = False


@dataclass(frozen=True, slots=True)
class LeagueMatch:
    """Result of matching a competition against the canonical league taxonomy."""

    canonical_slug: str
    canonical_name: str
    silo: str
    tier: int
    catalog_target: str
    genre: str | None
    is_women: bool
    is_youth: bool
    is_popular: bool
    rule: LeagueRule | None = None


def _rule(
    slug: str,
    name: str,
    silo: str,
    regex: str,
    *,
    tier: int = 1,
    priority: int = 100,
    country_include: tuple[str, ...] = (),
    country_exclude: tuple[str, ...] = (),
    requires_country_match: bool = False,
    is_women: bool | None = None,
    is_youth: bool | None = None,
    catalog_target: str | None = None,
    genre: str | None = None,
    is_popular: bool = False,
) -> LeagueRule:
    return LeagueRule(
        canonical_slug=slug,
        canonical_name=name,
        silo=silo,
        pattern=re.compile(regex, re.IGNORECASE),
        tier=tier,
        priority=priority,
        country_include=country_include,
        country_exclude=country_exclude,
        requires_country_match=requires_country_match,
        is_women=is_women,
        is_youth=is_youth,
        catalog_target=catalog_target or silo,
        genre=genre,
        is_popular=is_popular,
    )


# Explicit minor/rejected competition patterns per silo (used when not already Tier 1/2).
_MINOR_COMPETITION_PATTERNS: dict[str, re.Pattern[str]] = {
    "football": re.compile(
        r"(?:"
        r"\bserie\s*d\b"
        r"|\beccellenza\b"
        r"|\bpromozione\b"
        r"|\bprima\s+categoria\b"
        r"|\bseconda\s+categoria\b"
        r"|\bterza\s+categoria\b"
        r"|\bregionalliga\b"
        r"|\boberliga\b"
        r"|\bnational\s+league\s+(?:north|south)\b"
        r"|\bisthmian\b"
        r"|\bnorthern\s+premier\b"
        r"|\bsouthern\s+league\b"
        r"|\btercera\b"
        r"|\bcampeonato\s+de\s+portugal\b"
        r"|\bamateur\b"
        r"|\breserve\s+league\b"
        r"|\bregional\b"
        r")",
        re.IGNORECASE,
    ),
    "tennis": re.compile(
        r"(?:"
        r"\bitf\b"
        r"|\b(?:m|w)(?:15|25|35|40)\b"
        r"|\bfutures\b"
        r"|\butr\s*pro\b"
        r"|\bexhibition\b"
        r")",
        re.IGNORECASE,
    ),
}


def _build_default_rules() -> list[LeagueRule]:
    rules: list[LeagueRule] = [
        # =====================================================================
        # FOOTBALL — PRIORITY 10: Homonym Disambiguation (South America FIRST)
        # =====================================================================
        _rule(
            "brasileirao-serie-a",
            "Brasileirão Série A",
            "football",
            r"(?:brasileir[aã]o(?:\s+s[eé]rie\s*a)?|s[eé]rie\s*a\s+betano|s[eé]rie\s*a.*brazil|brazil.*s[eé]rie\s*a)",
            tier=1,
            priority=10,
            country_exclude=("italy", "ecuador"),
            is_women=False,
            is_youth=False,
            catalog_target="football",
            genre="Sudamerica",
        ),
        _rule(
            "brasileirao-serie-a",
            "Brasileirão Série A",
            "football",
            r"^s[eé]rie\s*a(?:\s*-\s*.*)?$",
            tier=1,
            priority=11,
            country_include=("brazil",),
            requires_country_match=True,
            is_women=False,
            is_youth=False,
            catalog_target="football",
            genre="Sudamerica",
        ),
        _rule(
            "brasileirao-serie-b",
            "Brasileirão Série B",
            "football",
            r"(?:brasileir[aã]o\s+s[eé]rie\s*b|s[eé]rie\s*b.*brazil|brazil.*s[eé]rie\s*b)",
            tier=2,
            priority=12,
            country_exclude=("italy", "ecuador"),
            is_women=False,
            is_youth=False,
            catalog_target="football",
            genre="Sudamerica",
        ),
        _rule(
            "brasileirao-serie-b",
            "Brasileirão Série B",
            "football",
            r"^s[eé]rie\s*b(?:\s*-\s*.*)?$",
            tier=2,
            priority=13,
            country_include=("brazil",),
            requires_country_match=True,
            is_women=False,
            is_youth=False,
            catalog_target="football",
            genre="Sudamerica",
        ),
        _rule(
            "ecuador-ligapro-serie-a",
            "LigaPro Serie A Ecuador",
            "football",
            r"(?:ligapro(?:\s+serie\s*a)?|serie\s*a.*ecuador|ecuador.*serie\s*a)",
            tier=2,
            priority=14,
            country_exclude=("italy", "brazil"),
            is_women=False,
            is_youth=False,
            catalog_target="football",
            genre="Sudamerica",
        ),
        _rule(
            "ecuador-ligapro-serie-a",
            "LigaPro Serie A Ecuador",
            "football",
            r"^serie\s*a(?:\s*-\s*.*)?$",
            tier=2,
            priority=15,
            country_include=("ecuador",),
            requires_country_match=True,
            is_women=False,
            is_youth=False,
            catalog_target="football",
            genre="Sudamerica",
        ),
        _rule(
            "colombia-primera-a",
            "Categoría Primera A",
            "football",
            r"(?:liga\s+betplay|categor[ií]a\s+primera\s*a|primera\s*a.*colombia|colombia.*primera\s*a)",
            tier=2,
            priority=16,
            is_women=False,
            is_youth=False,
            catalog_target="football",
            genre="Sudamerica",
        ),
        # =====================================================================
        # FOOTBALL — PRIORITY 20: Women's & Youth Leagues (never senior men)
        # =====================================================================
        _rule(
            "serie-a-femminile",
            "Serie A Femminile",
            "football",
            r"(?:serie\s*a.*(?:femminil|women|donne|ebay)|ebay\s+serie\s*a)",
            tier=1,
            priority=20,
            country_include=("italy",),
            is_women=True,
            is_youth=False,
            catalog_target="calcio_ita",
            genre="Calcio Femminile",
        ),
        _rule(
            "coppa-italia-femminile",
            "Coppa Italia Femminile",
            "football",
            r"coppa\s+italia.*(?:femminil|women|donne)",
            tier=2,
            priority=21,
            country_include=("italy",),
            is_women=True,
            is_youth=False,
            catalog_target="calcio_ita",
            genre="Calcio Femminile",
        ),
        _rule(
            "primavera-1",
            "Primavera 1",
            "football",
            r"(?:primavera\s*1|campionato\s+primavera\s*1?)",
            tier=2,
            priority=22,
            country_include=("italy",),
            is_women=False,
            is_youth=True,
            catalog_target="calcio_ita",
            genre="Primavera / Giovanili",
        ),
        _rule(
            "primavera-2",
            "Primavera 2",
            "football",
            r"primavera\s*2",
            tier=2,
            priority=23,
            country_include=("italy",),
            is_women=False,
            is_youth=True,
            catalog_target="calcio_ita",
            genre="Primavera / Giovanili",
        ),
        _rule(
            "coppa-italia-primavera",
            "Coppa Italia Primavera",
            "football",
            r"coppa\s+italia\s+primavera",
            tier=2,
            priority=24,
            country_include=("italy",),
            is_women=False,
            is_youth=True,
            catalog_target="calcio_ita",
            genre="Primavera / Giovanili",
        ),
        _rule(
            "uefa-womens-champions-league",
            "UEFA Women's Champions League",
            "football",
            r"(?:uwcl|women'?s?\s+champions\s+league|champions\s+league\s+(?:women|femminile|donne))",
            tier=1,
            priority=25,
            is_women=True,
            is_youth=False,
            catalog_target="football",
            genre="Calcio Femminile",
        ),
        _rule(
            "uefa-youth-league",
            "UEFA Youth League",
            "football",
            r"(?:uefa\s+)?youth\s+league",
            tier=2,
            priority=26,
            is_women=False,
            is_youth=True,
            catalog_target="football",
            genre="Altri Campionati",
        ),
        _rule(
            "wsl-women",
            "Women's Super League",
            "football",
            r"(?:\bwsl\b|women'?s?\s+super\s+league)",
            tier=1,
            priority=27,
            is_women=True,
            is_youth=False,
            catalog_target="football",
            genre="Calcio Femminile",
        ),
        _rule(
            "liga-f-women",
            "Liga F",
            "football",
            r"(?:\bliga\s*f\b|primera\s+divisi[oó]n\s+femenina)",
            tier=1,
            priority=28,
            is_women=True,
            is_youth=False,
            catalog_target="football",
            genre="Calcio Femminile",
        ),
        _rule(
            "frauen-bundesliga",
            "Frauen-Bundesliga",
            "football",
            r"frauen[- ]?bundesliga",
            tier=1,
            priority=28,
            is_women=True,
            is_youth=False,
            catalog_target="football",
            genre="Calcio Femminile",
        ),
        _rule(
            "d1-arkema",
            "Première Ligue Féminine",
            "football",
            r"(?:d1\s+arkema|premi[eè]re\s+ligue\s+(?:f[eé]minine|women))",
            tier=1,
            priority=28,
            is_women=True,
            is_youth=False,
            catalog_target="football",
            genre="Calcio Femminile",
        ),
        _rule(
            "nwsl",
            "NWSL",
            "football",
            r"\bnwsl\b",
            tier=1,
            priority=28,
            is_women=True,
            is_youth=False,
            catalog_target="football",
            genre="Calcio Femminile",
        ),
        # =====================================================================
        # FOOTBALL — PRIORITY 30: Italian Senior Football (calcio_ita)
        # =====================================================================
        _rule(
            "serie-a",
            "Serie A",
            "football",
            (
                r"^(?:italy\s*[:-]?\s*)?serie\s*a(?:\s*(?:enilive|tim))?"
                r"(?!.*\b(?:femminil|women|donne|ebay|primavera|brasileir|betano|ecuador|ligapro|argentin|elite|a1|a2)\b)"
                r"(?:\s*[-:]\s*.*)?$"
            ),
            tier=1,
            priority=30,
            country_include=("italy",),
            country_exclude=("brazil", "ecuador", "colombia", "argentina"),
            is_women=False,
            is_youth=False,
            catalog_target="calcio_ita",
            genre="Serie A",
            is_popular=True,
        ),
        _rule(
            "serie-b",
            "Serie B",
            "football",
            (
                r"^(?:italy\s*[:-]?\s*)?serie\s*b(?:\s*kt)?"
                r"(?!.*\b(?:femminil|women|donne|primavera|brasileir|ecuador)\b)"
                r"(?:\s*[-:]\s*.*)?$"
            ),
            tier=1,
            priority=31,
            country_include=("italy",),
            country_exclude=("brazil", "ecuador", "colombia", "argentina"),
            is_women=False,
            is_youth=False,
            catalog_target="calcio_ita",
            genre="Serie B",
            is_popular=True,
        ),
        _rule(
            "serie-c",
            "Serie C",
            "football",
            r"^(?:italy\s*[:-]?\s*)?(?:serie\s*c|lega\s*pro)(?:\s+(?:now|sky|girone\s+[abc]))?(?:\s*[-:]\s*.*)?$",
            tier=2,
            priority=32,
            country_include=("italy",),
            country_exclude=("brazil", "ecuador"),
            is_women=False,
            is_youth=False,
            catalog_target="calcio_ita",
            genre="Serie C",
        ),
        _rule(
            "coppa-italia",
            "Coppa Italia",
            "football",
            r"\bcoppa\s+italia\b(?!.*\b(?:femminil|women|donne|primavera|serie\s*[cd]|dilettanti|eccellenza)\b)",
            tier=1,
            priority=33,
            country_include=("italy",),
            is_women=False,
            is_youth=False,
            catalog_target="calcio_ita",
            genre="Coppa Italia",
            is_popular=True,
        ),
        _rule(
            "supercoppa-italiana",
            "Supercoppa Italiana",
            "football",
            r"\bsupercoppa\s+italiana\b(?!.*\b(?:femminil|women|donne|primavera)\b)",
            tier=1,
            priority=34,
            country_include=("italy",),
            is_women=False,
            is_youth=False,
            catalog_target="calcio_ita",
            genre="Coppa Italia",
            is_popular=True,
        ),
        # Rejected Italian Tier 3 (explicit so they match Tier 3 and get rejected without streams)
        _rule(
            "serie-d",
            "Serie D",
            "football",
            r"\bserie\s*d\b",
            tier=3,
            priority=38,
            country_include=("italy",),
            catalog_target="calcio_ita",
        ),
        _rule(
            "eccellenza",
            "Eccellenza",
            "football",
            r"\beccellenza\b",
            tier=3,
            priority=39,
            country_include=("italy",),
            catalog_target="calcio_ita",
        ),
        # =====================================================================
        # FOOTBALL — PRIORITY 40: European & Global Tier 1/2
        # =====================================================================
        _rule(
            "champions-league",
            "UEFA Champions League",
            "football",
            r"(?:uefa\s+)?champions\s+league(?!.*\b(?:women|femminile|donne|youth|asia|afc|caf|concacaf)\b)",
            tier=1,
            priority=40,
            is_women=False,
            is_youth=False,
            catalog_target="football",
            genre="Champions League",
            is_popular=True,
        ),
        _rule(
            "europa-league",
            "UEFA Europa League",
            "football",
            r"(?:uefa\s+)?europa\s+league",
            tier=1,
            priority=41,
            is_women=False,
            is_youth=False,
            catalog_target="football",
            genre="Europa & Conference",
            is_popular=True,
        ),
        _rule(
            "conference-league",
            "UEFA Conference League",
            "football",
            r"(?:uefa\s+)?(?:europa\s+)?conference\s+league",
            tier=1,
            priority=42,
            is_women=False,
            is_youth=False,
            catalog_target="football",
            genre="Europa & Conference",
            is_popular=True,
        ),
        _rule(
            "uefa-super-cup",
            "UEFA Super Cup",
            "football",
            r"(?:uefa\s+super\s+cup|supercoppa\s+(?:uefa|europea))",
            tier=1,
            priority=43,
            catalog_target="football",
            genre="Champions League",
            is_popular=True,
        ),
        _rule(
            "nations-league",
            "UEFA Nations League",
            "football",
            r"(?:uefa\s+)?nations\s+league",
            tier=1,
            priority=44,
            catalog_target="football",
            genre="Nazionali",
            is_popular=True,
        ),
        _rule(
            "world-cup",
            "FIFA World Cup",
            "football",
            r"(?:fifa\s+world\s+cup|world\s+cup(?:\s+-\s+qualification)?|mondiali|coppa\s+del\s+mondo)",
            tier=1,
            priority=45,
            catalog_target="football",
            genre="Nazionali",
            is_popular=True,
        ),
        _rule(
            "euro-championship",
            "UEFA Euro",
            "football",
            r"(?:uefa\s+euro(?:\s+\d{4})?|euro\s+\d{4}(?:\s+qualifiers?)?|campionati\s+europei)",
            tier=1,
            priority=46,
            catalog_target="football",
            genre="Nazionali",
            is_popular=True,
        ),
        _rule(
            "copa-america",
            "Copa América",
            "football",
            r"copa\s+am[eé]rica",
            tier=1,
            priority=47,
            catalog_target="football",
            genre="Nazionali",
            is_popular=True,
        ),
        _rule(
            "afcon",
            "Africa Cup of Nations",
            "football",
            r"(?:africa\s+cup\s+of\s+nations|coppa\s+d'?africa|\bafcon\b)",
            tier=1,
            priority=48,
            catalog_target="football",
            genre="Nazionali",
        ),
        _rule(
            "copa-libertadores",
            "Copa Libertadores",
            "football",
            r"copa\s+libertadores",
            tier=1,
            priority=49,
            catalog_target="football",
            genre="Sudamerica",
            is_popular=True,
        ),
        _rule(
            "copa-sudamericana",
            "Copa Sudamericana",
            "football",
            r"copa\s+sudamericana",
            tier=2,
            priority=50,
            catalog_target="football",
            genre="Sudamerica",
        ),
        _rule(
            "club-world-cup",
            "FIFA Club World Cup",
            "football",
            r"(?:club\s+world\s+cup|mondiale\s+per\s+club)",
            tier=1,
            priority=51,
            catalog_target="football",
            genre="Champions League",
            is_popular=True,
        ),
        # Top European & International Domestic Leagues
        _rule(
            "premier-league",
            "Premier League",
            "football",
            r"^(?:england\s*[:-]?\s*)?premier\s+league(?!\s*2\b)(?:\s*[-:]\s*.*)?$",
            tier=1,
            priority=52,
            country_include=("england",),
            is_women=False,
            is_youth=False,
            catalog_target="football",
            genre="Premier League",
            is_popular=True,
        ),
        _rule(
            "championship",
            "EFL Championship",
            "football",
            r"(?:efl\s+)?championship",
            tier=2,
            priority=53,
            country_include=("england",),
            requires_country_match=True,
            catalog_target="football",
            genre="Premier League",
        ),
        _rule(
            "fa-cup",
            "FA Cup",
            "football",
            r"\bfa\s+cup\b",
            tier=1,
            priority=54,
            country_include=("england",),
            catalog_target="football",
            genre="Premier League",
        ),
        _rule(
            "efl-cup",
            "Carabao Cup / EFL Cup",
            "football",
            r"(?:efl\s+cup|carabao\s+cup|league\s+cup)",
            tier=2,
            priority=55,
            country_include=("england",),
            catalog_target="football",
            genre="Premier League",
        ),
        _rule(
            "la-liga",
            "La Liga",
            "football",
            r"(?:la\s*liga(?:\s+ea\s*sports)?|primera\s+divisi[oó]n)(?!.*(?:femenin|2\b|hypermotion))",
            tier=1,
            priority=56,
            country_include=("spain",),
            is_women=False,
            is_youth=False,
            catalog_target="football",
            genre="La Liga",
            is_popular=True,
        ),
        _rule(
            "la-liga-2",
            "La Liga 2",
            "football",
            r"(?:la\s*liga\s*2|la\s*liga\s+hypermotion|segunda\s+divisi[oó]n)",
            tier=2,
            priority=57,
            country_include=("spain",),
            catalog_target="football",
            genre="La Liga",
        ),
        _rule(
            "copa-del-rey",
            "Copa del Rey",
            "football",
            r"copa\s+del\s+rey",
            tier=1,
            priority=58,
            country_include=("spain",),
            catalog_target="football",
            genre="La Liga",
        ),
        _rule(
            "2-bundesliga",
            "2. Bundesliga",
            "football",
            r"2\.?\s*bundesliga",
            tier=2,
            priority=59,
            country_include=("germany",),
            catalog_target="football",
            genre="Bundesliga",
        ),
        _rule(
            "bundesliga",
            "Bundesliga",
            "football",
            r"^(?:germany\s*[:-]?\s*)?bundesliga(?:\s*[-:]\s*.*)?$",
            tier=1,
            priority=60,
            country_include=("germany",),
            is_women=False,
            is_youth=False,
            catalog_target="football",
            genre="Bundesliga",
            is_popular=True,
        ),
        _rule(
            "dfb-pokal",
            "DFB-Pokal",
            "football",
            r"dfb[- ]?pokal",
            tier=1,
            priority=61,
            country_include=("germany",),
            catalog_target="football",
            genre="Bundesliga",
        ),
        _rule(
            "ligue-1",
            "Ligue 1",
            "football",
            r"ligue\s*1(?:\s+mcdonald'?s|\s+uber\s+eats)?",
            tier=1,
            priority=62,
            country_include=("france",),
            is_women=False,
            is_youth=False,
            catalog_target="football",
            genre="Ligue 1",
            is_popular=True,
        ),
        _rule(
            "ligue-2",
            "Ligue 2",
            "football",
            r"ligue\s*2",
            tier=2,
            priority=63,
            country_include=("france",),
            catalog_target="football",
            genre="Ligue 1",
        ),
        _rule(
            "coupe-de-france",
            "Coupe de France",
            "football",
            r"coupe\s+de\s+france",
            tier=1,
            priority=64,
            country_include=("france",),
            catalog_target="football",
            genre="Ligue 1",
        ),
        _rule(
            "primeira-liga",
            "Liga Portugal",
            "football",
            r"(?:liga\s+portugal(?:\s+betclic)?|primeira\s+liga)",
            tier=1,
            priority=65,
            country_include=("portugal",),
            catalog_target="football",
            genre="Altri Campionati",
        ),
        _rule(
            "eredivisie",
            "Eredivisie",
            "football",
            r"\beredivisie\b",
            tier=1,
            priority=66,
            country_include=("netherlands",),
            catalog_target="football",
            genre="Altri Campionati",
        ),
        _rule(
            "jupiler-pro-league",
            "Jupiler Pro League",
            "football",
            r"jupiler\s+(?:pro\s+)?league",
            tier=2,
            priority=67,
            country_include=("belgium",),
            catalog_target="football",
            genre="Altri Campionati",
        ),
        _rule(
            "super-lig",
            "Süper Lig",
            "football",
            r"s[uü]per\s+lig",
            tier=2,
            priority=68,
            country_include=("turkey",),
            catalog_target="football",
            genre="Altri Campionati",
        ),
        _rule(
            "scottish-premiership",
            "Scottish Premiership",
            "football",
            r"(?:scottish\s+)?premiership",
            tier=2,
            priority=69,
            country_include=("scotland",),
            requires_country_match=True,
            catalog_target="football",
            genre="Altri Campionati",
        ),
        _rule(
            "super-league-greece",
            "Super League Greece",
            "football",
            r"super\s+league",
            tier=2,
            priority=70,
            country_include=("greece",),
            requires_country_match=True,
            catalog_target="football",
            genre="Altri Campionati",
        ),
        _rule(
            "saudi-pro-league",
            "Saudi Pro League",
            "football",
            r"(?:saudi\s+(?:pro|professional)\s+league|roshn\s+saudi\s+league)",
            tier=2,
            priority=71,
            catalog_target="football",
            genre="Altri Campionati",
        ),
        _rule(
            "mls",
            "MLS",
            "football",
            r"(?:\bmls\b|major\s+league\s+soccer)",
            tier=2,
            priority=72,
            catalog_target="football",
            genre="Altri Campionati",
        ),
        _rule(
            "liga-profesional-argentina",
            "Liga Profesional Argentina",
            "football",
            r"(?:liga\s+profesional|torneo\s+betano|primera\s+divisi[oó]n.*argentina)",
            tier=2,
            priority=73,
            catalog_target="football",
            genre="Sudamerica",
        ),
        _rule(
            "liga-mx",
            "Liga MX",
            "football",
            r"\bliga\s+mx\b",
            tier=2,
            priority=74,
            catalog_target="football",
            genre="Altri Campionati",
        ),
        # =====================================================================
        # TENNIS — Strict Silo Boundary
        # =====================================================================
        _rule(
            "australian-open",
            "Australian Open",
            "tennis",
            r"australian\s+open",
            tier=1,
            priority=10,
            genre="Grande Slam",
            is_popular=True,
        ),
        _rule(
            "roland-garros",
            "Roland Garros",
            "tennis",
            r"(?:roland\s+garros|french\s+open)",
            tier=1,
            priority=11,
            genre="Grande Slam",
            is_popular=True,
        ),
        _rule(
            "wimbledon",
            "Wimbledon",
            "tennis",
            r"\bwimbledon\b",
            tier=1,
            priority=12,
            genre="Grande Slam",
            is_popular=True,
        ),
        _rule(
            "us-open",
            "US Open",
            "tennis",
            r"\bus\s+open\b",
            tier=1,
            priority=13,
            genre="Grande Slam",
            is_popular=True,
        ),
        _rule(
            "davis-cup",
            "Davis Cup",
            "tennis",
            r"(?:davis\s+cup|coppa\s+davis)",
            tier=1,
            priority=15,
            is_women=False,
            genre="Coppe Nazionali",
            is_popular=True,
        ),
        _rule(
            "bjk-cup",
            "Billie Jean King Cup",
            "tennis",
            r"(?:bjk\s*cup|billie\s+jean\s+king)",
            tier=1,
            priority=16,
            is_women=True,
            genre="Coppe Nazionali",
            is_popular=True,
        ),
        _rule(
            "united-cup",
            "United Cup",
            "tennis",
            r"\bunited\s+cup\b",
            tier=1,
            priority=17,
            genre="Coppe Nazionali",
        ),
        _rule(
            "laver-cup",
            "Laver Cup",
            "tennis",
            r"\blaver\s+cup\b",
            tier=1,
            priority=18,
            genre="Coppe Nazionali",
        ),
        _rule(
            "atp-finals",
            "ATP Finals",
            "tennis",
            r"atp\s+(?:world\s+tour\s+)?finals|nitto\s+atp\s+finals",
            tier=1,
            priority=20,
            is_women=False,
            genre="ATP Tour",
            is_popular=True,
        ),
        _rule(
            "wta-finals",
            "WTA Finals",
            "tennis",
            r"wta\s+finals",
            tier=1,
            priority=21,
            is_women=True,
            genre="WTA Tour",
            is_popular=True,
        ),
        _rule(
            "atp-challenger",
            "ATP Challenger",
            "tennis",
            r"(?:atp\s+)?challenger",
            tier=2,
            priority=25,
            is_women=False,
            genre="Challenger",
        ),
        _rule(
            "atp-tour",
            "ATP Tour",
            "tennis",
            r"\batp\b",
            tier=1,
            priority=30,
            is_women=False,
            genre="ATP Tour",
            is_popular=True,
        ),
        _rule(
            "wta-tour",
            "WTA Tour",
            "tennis",
            r"\bwta\b",
            tier=1,
            priority=31,
            is_women=True,
            genre="WTA Tour",
            is_popular=True,
        ),
        _rule(
            "itf-tennis",
            "ITF Futures",
            "tennis",
            r"(?:\bitf\b|\b(?:m|w)(?:15|25|35|40|50|60|75|80|100)\b|\bfutures\b)",
            tier=3,
            priority=90,
            genre="Challenger",
        ),
        # =====================================================================
        # BASKET — Strict Silo Boundary
        # =====================================================================
        _rule(
            "wnba",
            "WNBA",
            "basket",
            r"\bwnba\b",
            tier=1,
            priority=10,
            is_women=True,
            genre="WNBA",
            is_popular=True,
        ),
        _rule(
            "nba",
            "NBA",
            "basket",
            r"\bnba\b(?!.*(?:g\s*league|summer))",
            tier=1,
            priority=11,
            is_women=False,
            genre="NBA",
            is_popular=True,
        ),
        _rule(
            "euroleague-women",
            "EuroLeague Women",
            "basket",
            r"euroleague\s+women",
            tier=1,
            priority=15,
            is_women=True,
            genre="Eurolega & Coppe",
        ),
        _rule(
            "euroleague",
            "EuroLeague",
            "basket",
            r"(?:euroleague|eurolega)",
            tier=1,
            priority=16,
            is_women=False,
            genre="Eurolega & Coppe",
            is_popular=True,
        ),
        _rule(
            "eurocup-basket",
            "EuroCup",
            "basket",
            r"\beurocup\b",
            tier=2,
            priority=17,
            genre="Eurolega & Coppe",
        ),
        _rule(
            "basketball-champions-league",
            "Basketball Champions League",
            "basket",
            r"(?:basketball\s+champions\s+league|\bbcl\b)",
            tier=2,
            priority=18,
            genre="Eurolega & Coppe",
        ),
        _rule(
            "lba-serie-a",
            "LBA Serie A",
            "basket",
            r"(?:lba|lega\s*a\b|serie\s*a(?:\s+unipolsai)?)",
            tier=1,
            priority=20,
            country_include=("italy",),
            is_women=False,
            genre="LBA Serie A",
            is_popular=True,
        ),
        _rule(
            "serie-a2-basket",
            "Serie A2 Basket",
            "basket",
            r"serie\s*a2",
            tier=2,
            priority=21,
            country_include=("italy",),
            genre="LBA Serie A",
        ),
        _rule(
            "liga-acb",
            "Liga ACB",
            "basket",
            r"(?:liga\s+)?acb|liga\s+endesa",
            tier=1,
            priority=22,
            genre="Campionati Esteri",
        ),
        _rule(
            "bbl-germany",
            "Basketball Bundesliga",
            "basket",
            r"\bbbl\b|easycredit\s+bbl",
            tier=2,
            priority=23,
            genre="Campionati Esteri",
        ),
        _rule(
            "betclic-elite",
            "Betclic Élite",
            "basket",
            r"(?:betclic\s+[eé]lite|lnb\s+pro\s*a)",
            tier=2,
            priority=24,
            genre="Campionati Esteri",
        ),
        _rule(
            "ncaa-basketball",
            "NCAA Basketball",
            "basket",
            r"\bncaa\b|march\s+madness",
            tier=2,
            priority=25,
            genre="Campionati Esteri",
        ),
        _rule(
            "fiba-international",
            "FIBA International",
            "basket",
            r"(?:fiba\s+(?:world\s+cup|eurobasket|americup|olympic)|eurobasket)",
            tier=1,
            priority=26,
            genre="Eurolega & Coppe",
        ),
        # =====================================================================
        # MOTORI — Strict Silo Boundary
        # =====================================================================
        _rule(
            "formula-1",
            "Formula 1",
            "motori",
            r"(?:formula\s*1|\bf1\b)",
            tier=1,
            priority=10,
            genre="Formula 1",
            is_popular=True,
        ),
        _rule(
            "formula-2",
            "Formula 2",
            "motori",
            r"(?:formula\s*2|\bf2\b)",
            tier=2,
            priority=11,
            genre="Formula 1",
        ),
        _rule(
            "formula-3",
            "Formula 3",
            "motori",
            r"(?:formula\s*3|\bf3\b)",
            tier=2,
            priority=12,
            genre="Formula 1",
        ),
        _rule(
            "motogp",
            "MotoGP",
            "motori",
            r"\bmoto\s*gp\b",
            tier=1,
            priority=15,
            genre="MotoGP",
            is_popular=True,
        ),
        _rule(
            "moto2",
            "Moto2",
            "motori",
            r"\bmoto\s*2\b",
            tier=2,
            priority=16,
            genre="MotoGP",
        ),
        _rule(
            "moto3",
            "Moto3",
            "motori",
            r"\bmoto\s*3\b",
            tier=2,
            priority=17,
            genre="MotoGP",
        ),
        _rule(
            "wsbk",
            "WorldSBK",
            "motori",
            r"(?:world\s*sbk|wsbk|superbike)",
            tier=1,
            priority=20,
            genre="Superbike",
        ),
        _rule(
            "indycar",
            "IndyCar",
            "motori",
            r"\bindy\s*car\b",
            tier=2,
            priority=25,
            genre="Altre Categorie",
        ),
        _rule(
            "nascar",
            "NASCAR",
            "motori",
            r"\bnascar\b",
            tier=2,
            priority=26,
            genre="Altre Categorie",
        ),
        _rule(
            "wrc",
            "WRC",
            "motori",
            r"(?:\bwrc\b|world\s+rally\s+championship)",
            tier=2,
            priority=27,
            genre="Altre Categorie",
        ),
        _rule(
            "formula-e",
            "Formula E",
            "motori",
            r"formula\s*e",
            tier=2,
            priority=28,
            genre="Altre Categorie",
        ),
        _rule(
            "wec",
            "FIA WEC",
            "motori",
            r"(?:\bwec\b|world\s+endurance|24\s+hours\s+of\s+le\s+mans|le\s+mans)",
            tier=2,
            priority=29,
            genre="Altre Categorie",
        ),
        # =====================================================================
        # COMBAT — Strict Silo Boundary
        # =====================================================================
        _rule("ufc", "UFC", "combat", r"\bufc\b", tier=1, priority=10, genre="UFC", is_popular=True),
        _rule("bellator", "Bellator MMA", "combat", r"\bbellator\b", tier=2, priority=15, genre="MMA"),
        _rule("pfl", "PFL", "combat", r"\bpfl\b", tier=2, priority=16, genre="MMA"),
        _rule("one-championship", "ONE Championship", "combat", r"\bone\s+championship\b|\bone\s+friday\b", tier=2, priority=17, genre="MMA"),
        _rule("ksw", "KSW", "combat", r"\bksw\b", tier=2, priority=18, genre="MMA"),
        _rule("oktagon", "Oktagon MMA", "combat", r"\boktagon\b", tier=2, priority=19, genre="MMA"),
        _rule(
            "boxing",
            "Boxing",
            "combat",
            r"(?:\bboxing\b|\bboxe\b|pugilato|matchroom|top\s+rank|queensberry|\bwbc\b|\bwba\b|\bibf\b|\bwbo\b|world\s+title)",
            tier=1,
            priority=20,
            genre="Boxe",
            is_popular=True,
        ),
        _rule("wwe", "WWE", "combat", r"\bwwe\b", tier=1, priority=25, genre="Wrestling"),
        _rule("aew", "AEW", "combat", r"\baew\b", tier=2, priority=26, genre="Wrestling"),
        # =====================================================================
        # VOLLEY — Strict Silo Boundary
        # =====================================================================
        _rule(
            "superlega-volley",
            "SuperLega Credem Banca",
            "volley",
            r"(?:superlega|serie\s*a1\s+(?:maschile|men)|credem\s+banca)",
            tier=1,
            priority=10,
            country_include=("italy",),
            is_women=False,
            genre="SuperLega",
            is_popular=True,
        ),
        _rule(
            "serie-a1-femminile-volley",
            "Serie A1 Femminile",
            "volley",
            r"serie\s*a1.*(?:femminil|women|donne|tigot[aà])",
            tier=1,
            priority=11,
            country_include=("italy",),
            is_women=True,
            genre="Serie A1 Femminile",
            is_popular=True,
        ),
        _rule(
            "serie-a2-volley",
            "Serie A2 Volley",
            "volley",
            r"serie\s*a2",
            tier=2,
            priority=15,
            country_include=("italy",),
            genre="SuperLega",
        ),
        _rule(
            "cev-champions-league",
            "CEV Champions League",
            "volley",
            r"(?:cev\s+)?champions\s+league",
            tier=1,
            priority=20,
            genre="CEV Champions League",
        ),
        _rule(
            "vnl-volley",
            "Volleyball Nations League",
            "volley",
            r"(?:nations\s+league|\bvnl\b|world\s+championship)",
            tier=1,
            priority=25,
            genre="Nazionali",
        ),
        _rule("plusliga", "PlusLiga", "volley", r"\bplusliga\b", tier=2, priority=30, genre="Campionati Esteri"),
        _rule("sultanlar-ligi", "Sultanlar Ligi", "volley", r"sultanlar\s+ligi", tier=2, priority=31, is_women=True, genre="Campionati Esteri"),
        # =====================================================================
        # NFL / HOCKEY / BASEBALL / RUGBY / HANDBALL
        # =====================================================================
        _rule("nfl", "NFL", "nfl", r"\bnfl\b", tier=1, priority=10, genre="NFL", is_popular=True),
        _rule("ncaaf", "NCAA Football", "nfl", r"\bncaa\b|college\s+football", tier=2, priority=15, genre="NCAA"),
        _rule("cfl", "CFL", "nfl", r"\bcfl\b", tier=2, priority=20, genre="NFL"),
        _rule("ufl", "UFL", "nfl", r"\bufl\b", tier=2, priority=21, genre="NFL"),
        _rule("nhl", "NHL", "hockey", r"\bnhl\b", tier=1, priority=10, genre="NHL", is_popular=True),
        _rule("khl", "KHL", "hockey", r"\bkhl\b", tier=2, priority=15, genre="Campionati Europei"),
        _rule("shl", "SHL", "hockey", r"\bshl\b", tier=2, priority=16, genre="Campionati Europei"),
        _rule("liiga", "Liiga", "hockey", r"\bliiga\b", tier=2, priority=17, genre="Campionati Europei"),
        _rule("del-hockey", "DEL", "hockey", r"\bdel\b", tier=2, priority=18, country_include=("germany",), genre="Campionati Europei"),
        _rule("nl-switzerland", "National League", "hockey", r"\bnational\s+league\b", tier=2, priority=19, genre="Campionati Europei"),
        _rule("iihf-world-championship", "IIHF World Championship", "hockey", r"\biihf\b|world\s+championship", tier=1, priority=20, genre="Nazionali"),
        _rule("mlb", "MLB", "baseball", r"\bmlb\b", tier=1, priority=10, genre="MLB", is_popular=True),
        _rule("npb", "NPB", "baseball", r"\bnpb\b", tier=2, priority=15, genre="Campionati Esteri"),
        _rule("kbo", "KBO", "baseball", r"\bkbo\b", tier=2, priority=16, genre="Campionati Esteri"),
        _rule("wbc", "World Baseball Classic", "baseball", r"world\s+baseball\s+classic|\bwbc\b", tier=1, priority=17, genre="MLB"),
        _rule("six-nations", "Six Nations", "rugby", r"(?:six\s+nations|sei\s+nazioni)", tier=1, priority=10, genre="Sei Nazioni", is_popular=True),
        _rule("rugby-championship", "Rugby Championship", "rugby", r"rugby\s+championship", tier=1, priority=11, genre="Nazionali"),
        _rule("rugby-world-cup", "Rugby World Cup", "rugby", r"(?:rugby\s+)?world\s+cup", tier=1, priority=12, genre="Nazionali"),
        _rule("rugby-champions-cup", "Investec Champions Cup", "rugby", r"champions\s+cup", tier=1, priority=15, genre="Coppe Europee"),
        _rule("rugby-challenge-cup", "EPCR Challenge Cup", "rugby", r"challenge\s+cup", tier=2, priority=16, genre="Coppe Europee"),
        _rule("urc-rugby", "United Rugby Championship", "rugby", r"(?:united\s+rugby\s+championship|\burc\b)", tier=1, priority=17, genre="URC & Club"),
        _rule("top-14", "Top 14", "rugby", r"\btop\s*14\b", tier=1, priority=18, genre="URC & Club"),
        _rule("premiership-rugby", "Premiership Rugby", "rugby", r"premiership", tier=1, priority=19, genre="URC & Club"),
        _rule("super-rugby", "Super Rugby", "rugby", r"super\s+rugby", tier=2, priority=20, genre="URC & Club"),
        _rule("serie-a-elite-rugby", "Serie A Elite Rugby", "rugby", r"serie\s*a\s+elite", tier=2, priority=21, country_include=("italy",), genre="URC & Club"),
        _rule("ehf-champions-league", "EHF Champions League", "handball", r"(?:ehf\s+)?champions\s+league", tier=1, priority=10, genre="EHF Champions"),
        _rule("ehf-european-league", "EHF European League", "handball", r"(?:ehf\s+)?european\s+league", tier=2, priority=15, genre="EHF Champions"),
        _rule("handball-bundesliga", "Handball-Bundesliga", "handball", r"bundesliga", tier=1, priority=16, genre="Campionati Nazionali"),
        _rule("liga-asobal", "Liga ASOBAL", "handball", r"asobal", tier=2, priority=17, genre="Campionati Nazionali"),
        _rule("starligue", "Liqui Moly StarLigue", "handball", r"starligue", tier=2, priority=18, genre="Campionati Nazionali"),
        _rule("serie-a-gold-handball", "Serie A Gold Pallamano", "handball", r"serie\s*a\s+gold", tier=2, priority=19, country_include=("italy",), genre="Campionati Nazionali"),
        _rule("ihf-handball", "IHF / EHF Championship", "handball", r"(?:world\s+championship|european\s+championship|\bihf\b|\behf\s+euro\b)", tier=1, priority=20, genre="Nazionali"),
        # =====================================================================
        # ALTRI SPORT — Strict Silo Boundary
        # =====================================================================
        _rule("cycling-major", "Ciclismo", "altri_sport", r"(?:giro\s+d'?italia|tour\s+de\s+france|vuelta|uci|milano[- ]sanremo|parigi[- ]roubaix|ronde\s+van\s+vlaanderen|strade\s+bianche)", tier=1, priority=10, genre="Ciclismo"),
        _rule("golf-major", "Golf", "altri_sport", r"(?:\bpga\b|dp\s+world\s+tour|liv\s+golf|ryder\s+cup|masters\s+tournament|the\s+open)", tier=1, priority=15, genre="Golf"),
        _rule("winter-sports", "Sport Invernali", "altri_sport", r"(?:\bfis\b|alpine\s+skiing|sci\s+alpino|biathlon|cross[- ]country|ski\s+jumping)", tier=1, priority=20, genre="Sport Invernali"),
        _rule("athletics", "Atletica", "altri_sport", r"(?:diamond\s+league|world\s+athletics|atletica)", tier=1, priority=25, genre="Atletica"),
        _rule("snooker", "Snooker", "altri_sport", r"(?:snooker|crucible|world\s+snooker)", tier=2, priority=30, genre="Snooker & Freccette"),
        _rule("darts", "Darts", "altri_sport", r"(?:\bpdc\b|premier\s+league\s+darts|world\s+darts)", tier=2, priority=31, genre="Snooker & Freccette"),
        _rule("padel", "Premier Padel", "altri_sport", r"premier\s+padel", tier=2, priority=35, genre="Padel"),
        _rule("olympics", "Olimpiadi", "altri_sport", r"(?:olympic|olimpiad)", tier=1, priority=40, genre="Olimpiadi"),
    ]
    return rules


def _demographics_slug(base_slug: str, is_women: bool, is_youth: bool, country_norm: str) -> str:
    slug = base_slug
    if is_women and not any(tok in slug for tok in ("women", "femminile", "wnba", "wta", "wsl", "liga-f", "nwsl", "frauen", "arkema", "bjk", "sultanlar")):
        suffix = "-femminile" if country_norm == "italy" else "-women"
        slug = f"{slug}{suffix}"
    if is_youth and not any(tok in slug for tok in ("primavera", "youth", "u19", "u20", "u21", "u23")):
        slug = f"{slug}-youth"
    return slug


def _demographics_name(base_name: str, is_women: bool, is_youth: bool, country_norm: str) -> str:
    name = base_name
    if is_women and not any(tok in name.lower() for tok in ("women", "femminile", "femenina", "frauen", "wnba", "wta", "wsl", "liga f", "nwsl", "bjk", "sultanlar")):
        suffix = " Femminile" if country_norm == "italy" else " Women"
        name = f"{name}{suffix}"
    if is_youth and not any(tok in name.lower() for tok in ("primavera", "youth", "u19", "u20", "u21", "u23")):
        name = f"{name} Youth"
    return name


class LeagueTaxonomy:
    """Hierarchical league resolver enforcing strict silo boundaries & demographic isolation."""

    def __init__(self, rules: list[LeagueRule] | None = None) -> None:
        raw_rules = rules if rules is not None else _build_default_rules()
        self._rules_by_bucket: dict[str, list[LeagueRule]] = {}
        for r in raw_rules:
            bucket = _silo_rule_bucket(r.silo)
            self._rules_by_bucket.setdefault(bucket, []).append(r)
        for bucket in self._rules_by_bucket:
            self._rules_by_bucket[bucket].sort(key=lambda r: r.priority)

    def match_league(
        self,
        silo: str,
        competition: str,
        country: str = "",
    ) -> LeagueMatch | None:
        """Match `competition` strictly within `silo`'s rule bucket."""
        clean_comp = _normalize_text(competition)
        if not clean_comp:
            return None

        bucket = _silo_rule_bucket(silo)
        bucket_rules = self._rules_by_bucket.get(bucket, ())
        country_norm = normalize_country(country)
        is_women, is_youth = detect_demographics(competition, country)

        for rule in bucket_rules:
            # Strict demographic compatibility: senior men rules never match women/youth.
            if rule.is_women is not None and rule.is_women != is_women:
                continue
            if rule.is_youth is not None and rule.is_youth != is_youth:
                continue

            if rule.country_exclude and country_norm in rule.country_exclude:
                continue

            if rule.country_include:
                if rule.requires_country_match:
                    if country_norm not in rule.country_include:
                        continue
                elif country_norm and country_norm not in rule.country_include:
                    continue

            if not rule.pattern.search(clean_comp):
                continue

            # Guarantee women/youth events never inherit senior men's canonical slug.
            slug = _demographics_slug(rule.canonical_slug, is_women, is_youth, country_norm)
            name = _demographics_name(rule.canonical_name, is_women, is_youth, country_norm)
            target = rule.catalog_target or silo
            genre = rule.genre
            if is_women and bucket == "football" and genre not in ("Calcio Femminile",):
                genre = "Calcio Femminile"
            elif is_youth and target == "calcio_ita":
                genre = "Primavera / Giovanili"

            return LeagueMatch(
                canonical_slug=slug,
                canonical_name=name,
                silo=silo if silo in SPORT_SILOS else bucket,
                tier=rule.tier,
                catalog_target=target,
                genre=genre,
                is_women=is_women,
                is_youth=is_youth,
                is_popular=rule.is_popular and not is_women and not is_youth,
                rule=rule,
            )

        return None

    def is_admitted_event(
        self,
        silo: str,
        competition: str,
        country: str = "",
        has_broadcasters: bool = False,
        has_streams: bool = False,
    ) -> bool:
        """Tier 1/2 admission filter.

        Admits recognized Tier 1/2 competitions unconditionally. Rejects minor
        leagues (Tier 3 or unrecognized low-tier regional/ITF competitions)
        when neither official broadcasters nor live streams are present.
        """
        if has_streams:
            return True

        match = self.match_league(silo, competition, country)
        if match is not None:
            if match.tier in (1, 2):
                return True
            return bool(has_broadcasters or has_streams)

        bucket = _silo_rule_bucket(silo)
        minor_re = _MINOR_COMPETITION_PATTERNS.get(bucket)
        clean_comp = _normalize_text(competition)
        if minor_re and clean_comp and minor_re.search(clean_comp):
            return bool(has_broadcasters or has_streams)

        # Unrecognized football/tennis/minor competitions without coverage are dropped.
        return bool(has_broadcasters)


DEFAULT_LEAGUE_TAXONOMY = LeagueTaxonomy()
