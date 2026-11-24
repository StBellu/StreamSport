"""Team/entity alias normalization, protected acronyms, and variant expansion.

Design Note on Stopwords vs. Distinguishing Club Words:
-------------------------------------------------------
`STOPWORDS` includes the full historical set of sports prefixes/suffixes for
reference, while `normalize_team_name` deliberately separates:
1. `CORPORATE_STOPWORDS`: generic legal/sport prefixes and suffixes (`fc`, `ac`,
   `as`, `ss`, `ssc`, `us`, `ud`, `cd`, `cf`, `sc`, `fk`, `sk`, `bk`, `ik`,
   `calcio`, `football`, `club`, `bc`, `basket`, `pallacanestro`, `volley`,
   `ssd`, etc.) that are safe to strip unless protected.
2. `DISTINGUISHING_TOKENS`: words such as `united`, `city`, `town`, `rovers`,
   `wanderers`, `albion`, `real`, `atletico`, `sporting`, `deportivo` that are
   NEVER stripped because doing so would collapse distinct rival clubs
   (e.g., `Manchester United` vs. `Manchester City`, `Real Madrid` vs.
   `Atletico Madrid`, `Bristol City` vs. `Bristol Rovers`).
3. `PROTECTED_ACRONYMS`: short abbreviations (`psg`, `bvb`, `lafc`, `nycfc`,
   `f1`, `ufc`, `nba`, `nfl`, `nhl`, `mlb`, `aek`, `cska`, `psv`, `az`, `qpr`,
   `wba`, `mls`, `atp`, `wta`) that are always preserved before alias expansion.
"""

from __future__ import annotations

import re
import unicodedata

PROTECTED_ACRONYMS: frozenset[str] = frozenset(
    {
        "psg",
        "bvb",
        "lafc",
        "nycfc",
        "f1",
        "ufc",
        "nba",
        "nfl",
        "nhl",
        "mlb",
        "aek",
        "cska",
        "psv",
        "az",
        "qpr",
        "wba",
        "mls",
        "atp",
        "wta",
    }
)

# Full stopword catalogue (kept for callers inspecting taxonomy configuration).
STOPWORDS: frozenset[str] = frozenset(
    {
        "fc",
        "ac",
        "as",
        "ss",
        "ssc",
        "us",
        "ud",
        "cd",
        "cf",
        "sc",
        "fk",
        "sk",
        "bk",
        "ik",
        "calcio",
        "football",
        "club",
        "sporting",
        "deportivo",
        "real",
        "atletico",
        "united",
        "city",
        "town",
        "rovers",
        "wanderers",
        "albion",
        "bc",
        "basket",
        "basketball",
        "pallacanestro",
        "volley",
        "ssd",
    }
)

# Words that carry club identity and must NOT be stripped during normalization.
DISTINGUISHING_TOKENS: frozenset[str] = frozenset(
    {
        "united",
        "city",
        "town",
        "rovers",
        "wanderers",
        "albion",
        "real",
        "atletico",
        "sporting",
        "deportivo",
    }
)

# Generic corporate/federation tokens safe to strip when not protected.
CORPORATE_STOPWORDS: frozenset[str] = (STOPWORDS - DISTINGUISHING_TOKENS) | frozenset(
    {
        "afc",
        "cfc",
        "rsc",
        "ogc",
        "vfb",
        "vfl",
        "tsv",
        "fsv",
        "sv",
        "spvgg",
        "kv",
        "nk",
        "gnk",
        "hjk",
        "krc",
        "rc",
        "rcd",
        "ca",
        "sd",
        "ad",
        "se",
        "ec",
        "cr",
        "sl",
        "uc",
        "usl",
        "srl",
        "spa",
        "acf",
    }
)

# Canonical club & entity synonym groups.
# The first entry of each tuple is the canonical normalized form; remaining
# entries are recognized aliases / surface forms.
_ALIAS_GROUP_DEFINITIONS: tuple[tuple[str, ...], ...] = (
    # --- Italy (Serie A / B) ---
    ("internazionale", "inter", "inter milan", "internazionale milano", "fc internazionale", "fc internazionale milano"),
    ("juventus", "juve", "juventus turin", "juventus torino", "juventus fc"),
    ("milan", "ac milan", "milan ac"),
    ("roma", "as roma"),
    ("lazio", "ss lazio"),
    ("napoli", "ssc napoli"),
    ("atalanta", "atalanta bergamo", "atalanta bc"),
    ("fiorentina", "acf fiorentina"),
    ("torino", "torino fc"),
    ("bologna", "bologna fc", "bologna 1909"),
    ("hellas verona", "verona", "hellas"),
    ("udinese", "udinese calcio"),
    ("genoa", "genoa cfc"),
    ("cagliari", "cagliari calcio"),
    ("lecce", "us lecce"),
    ("parma", "parma calcio", "parma 1913"),
    ("como", "como 1907"),
    ("venezia", "venezia fc"),
    ("empoli", "empoli fc"),
    ("monza", "ac monza"),
    ("sassuolo", "us sassuolo"),
    ("cremonese", "us cremonese"),
    ("pisa", "pisa sporting club", "pisa sc"),
    ("spezia", "spezia calcio"),
    ("sampdoria", "uc sampdoria"),
    ("palermo", "palermo fc"),
    ("bari", "ssc bari"),
    ("brescia", "brescia calcio"),
    # --- Italy (Serie C B-teams, kept distinct from senior clubs) ---
    ("juventus next gen", "juve next gen", "juventus u23"),
    ("milan futuro", "ac milan futuro", "milan u23"),
    ("atalanta u23", "atalanta under 23"),
    # --- England ---
    ("manchester city", "man city", "mancity"),
    ("manchester united", "man utd", "man united", "manutd"),
    ("tottenham hotspur", "tottenham", "spurs"),
    ("wolverhampton wanderers", "wolverhampton", "wolves"),
    ("newcastle united", "newcastle", "newcastle utd"),
    ("west ham united", "west ham", "west ham utd"),
    ("brighton and hove albion", "brighton", "brighton hove albion"),
    ("nottingham forest", "nottm forest", "nottingham"),
    ("aston villa", "villa"),
    ("leicester city", "leicester"),
    ("leeds united", "leeds", "leeds utd"),
    ("sheffield united", "sheffield utd"),
    ("west bromwich albion", "west brom", "wba"),
    ("queens park rangers", "qpr"),
    ("arsenal", "arsenal fc"),
    ("chelsea", "chelsea fc"),
    ("liverpool", "liverpool fc"),
    ("everton", "everton fc"),
    # --- Spain ---
    ("barcelona", "barca", "fc barcelona"),
    ("atletico madrid", "atletico", "atleti", "atl madrid", "atletico de madrid"),
    ("athletic bilbao", "athletic", "athletic club", "athletic club bilbao"),
    ("real madrid", "real"),
    ("real sociedad", "sociedad", "la real"),
    ("real betis", "betis", "real betis balompie"),
    ("sevilla", "sevilla fc"),
    ("valencia", "valencia cf"),
    ("villarreal", "villarreal cf"),
    # --- Germany ---
    ("bayern munich", "bayern", "bayern munchen", "fc bayern", "fc bayern munchen", "fc bayern munich"),
    ("borussia dortmund", "bvb", "dortmund", "bvb dortmund"),
    ("borussia monchengladbach", "monchengladbach", "gladbach", "b mg", "bmg"),
    ("bayer leverkusen", "leverkusen", "bayer 04 leverkusen"),
    ("rb leipzig", "leipzig", "rasenballsport leipzig"),
    ("eintracht frankfurt", "frankfurt", "sge"),
    ("vfb stuttgart", "stuttgart"),
    # --- France ---
    ("paris saint germain", "psg", "paris sg", "paris saint-germain"),
    ("olympique marseille", "om", "marseille", "olympique de marseille"),
    ("olympique lyon", "ol", "lyon", "olympique lyonnais"),
    ("as monaco", "monaco"),
    ("lille", "losc", "losc lille"),
    # --- Portugal / Netherlands / Europe ---
    ("sporting lisbon", "sporting cp", "sporting", "sporting clube de portugal"),
    ("porto", "fc porto"),
    ("benfica", "sl benfica"),
    ("feyenoord", "feyenoord rotterdam"),
    ("psv eindhoven", "psv"),
    ("ajax", "afc ajax", "ajax amsterdam"),
    ("az alkmaar", "az"),
    ("galatasaray", "galatasaray sk"),
    ("fenerbahce", "fenerbahce sk"),
    ("besiktas", "besiktas jk"),
    ("olympiacos", "olympiakos", "olympiacos piraeus"),
    ("crvena zvezda", "red star", "red star belgrade"),
    ("stade rennais", "rennes"),
    ("ofi crete", "ofi"),
    ("nec nijmegen", "nec", "nijmegen"),
    ("bodo glimt", "bodø/glimt", "fk bodo glimt"),
    ("fc kobenhavn", "kobenhavn", "copenhagen", "fc copenhagen"),
    ("malmo ff", "malmo", "malmö"),
    ("lks lodz", "łks łódź"),
    # --- Basket cross-city aliases ---
    ("cb canarias", "canarias", "tenerife", "la laguna tenerife"),
    ("derthona basket", "derthona", "tortona"),
    ("joventut badalona", "joventut", "badalona"),
    ("unicaja malaga", "unicaja", "malaga", "baloncesto malaga"),
    # --- MLS / US / NBA / Motorsports ---
    ("inter miami", "inter miami cf"),
    ("los angeles fc", "lafc"),
    ("new york city", "nycfc", "new york city fc"),
    ("los angeles galaxy", "la galaxy"),
    ("los angeles lakers", "lakers", "la lakers"),
    ("los angeles clippers", "clippers", "la clippers"),
    ("boston celtics", "celtics"),
    ("new york knicks", "knicks"),
    ("brooklyn nets", "nets"),
    ("golden state warriors", "warriors", "gsw"),
    ("philadelphia 76ers", "76ers", "sixers", "philadelphia sixers"),
    ("miami heat", "heat"),
    ("chicago bulls", "bulls"),
    ("milwaukee bucks", "bucks"),
    ("denver nuggets", "nuggets"),
    ("phoenix suns", "suns"),
    ("dallas mavericks", "mavs", "mavericks"),
    ("oklahoma city thunder", "okc", "okc thunder", "thunder"),
    ("cleveland cavaliers", "cavs", "cavaliers"),
    ("san antonio spurs", "sa spurs"),
    ("formula 1", "f1", "formula one"),
    ("motogp", "moto gp"),
    ("worldsbk", "wsbk", "superbike"),
)

# Public alias lookup: normalized alias -> canonical normalized name.
TEAM_ALIASES: dict[str, str] = {}
_CANONICAL_TO_VARIANTS: dict[str, tuple[str, ...]] = {}
CANONICAL_ENTITIES: frozenset[str] = frozenset()

_PUNCT_RE = re.compile(r"[^\w\s]", re.UNICODE)
_WHITESPACE_RE = re.compile(r"\s+")
# Strip trailing demographic/bracket annotations such as "(W)" or "[U21]" on team names.
_TRAILING_TAG_RE = re.compile(r"\s*[\(\[]\s*(?:w|women|donne|f|u\d{2})\s*[\)\]]\s*$", re.IGNORECASE)

# Non-NFKD European characters that unicodedata.normalize("NFKD", ...) does not decompose.
_CHAR_TRANSLITERATIONS: dict[str, str] = {
    "ø": "o",
    "Ø": "O",
    "æ": "ae",
    "Æ": "AE",
    "œ": "oe",
    "Œ": "OE",
    "ß": "ss",
    "ð": "d",
    "Ð": "D",
    "þ": "th",
    "Þ": "TH",
    "ł": "l",
    "Ł": "L",
    "đ": "d",
    "Đ": "D",
    "ı": "i",
    "İ": "I",
}
_CHAR_TRANS_TABLE = str.maketrans(_CHAR_TRANSLITERATIONS)


def _basic_clean(name: str) -> str:
    if not name:
        return ""
    cleaned = _TRAILING_TAG_RE.sub("", name)
    cleaned = cleaned.translate(_CHAR_TRANS_TABLE)
    normalized = unicodedata.normalize("NFKD", cleaned)
    ascii_text = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    ascii_text = ascii_text.lower().replace("&", " and ")
    ascii_text = _PUNCT_RE.sub(" ", ascii_text)
    return _WHITESPACE_RE.sub(" ", ascii_text).strip()


def _strip_corporate_tokens(cleaned: str) -> str:
    if not cleaned:
        return ""
    if cleaned in PROTECTED_ACRONYMS:
        return cleaned
    tokens = cleaned.split()
    kept: list[str] = []
    for tok in tokens:
        if tok in PROTECTED_ACRONYMS:
            kept.append(tok)
        elif tok in CORPORATE_STOPWORDS:
            continue
        else:
            kept.append(tok)
    return " ".join(kept) if kept else cleaned


def _build_alias_tables() -> None:
    global CANONICAL_ENTITIES
    canonicals: set[str] = set()
    # Short/squashed surface forms accepted on input but omitted from outward
    # variant expansion to avoid noisy fuzzy-matching against rival clubs.
    _INPUT_ONLY_FORMS = {"inter", "real", "atl madrid", "manutd", "mancity", "man utd", "man city", "man u"}

    for group in _ALIAS_GROUP_DEFINITIONS:
        canonical = _basic_clean(group[0])
        canonicals.add(canonical)
        variants_ordered: list[str] = [canonical]
        for raw in group:
            basic = _basic_clean(raw)
            stripped = _strip_corporate_tokens(basic)
            for form in (basic, stripped):
                if form and form not in TEAM_ALIASES:
                    TEAM_ALIASES[form] = canonical
                if (
                    form
                    and form not in variants_ordered
                    and form not in _INPUT_ONLY_FORMS
                ):
                    variants_ordered.append(form)
        _CANONICAL_TO_VARIANTS[canonical] = tuple(variants_ordered)
    # Also accept "man u" on input -> manchester united.
    TEAM_ALIASES["man u"] = "manchester united"
    CANONICAL_ENTITIES = frozenset(canonicals)


_build_alias_tables()


def normalize_team_name(name: str) -> str:
    """Normalize a team/athlete/event entity name while protecting acronyms & key qualifiers."""
    basic = _basic_clean(name)
    if not basic:
        return ""

    if basic in TEAM_ALIASES:
        return TEAM_ALIASES[basic]

    stripped = _strip_corporate_tokens(basic)
    if stripped in TEAM_ALIASES:
        return TEAM_ALIASES[stripped]

    return stripped


def expand_team_variants(name: str, alt_name: str = "") -> list[str]:
    """Return deduplicated normalized variants for `name` (and optional `alt_name`)."""
    variants: list[str] = []
    seen: set[str] = set()

    def _add(val: str) -> None:
        if val and val not in seen:
            seen.add(val)
            variants.append(val)

    for raw in (name, alt_name):
        if not raw:
            continue
        basic = _basic_clean(raw)
        stripped = _strip_corporate_tokens(basic)
        canonical = normalize_team_name(raw)

        _add(canonical)
        if canonical in _CANONICAL_TO_VARIANTS:
            for alias_variant in _CANONICAL_TO_VARIANTS[canonical]:
                _add(alias_variant)
        _add(stripped)
        if basic and basic != stripped and len(basic.split()) <= 4:
            _add(basic)

    return variants
