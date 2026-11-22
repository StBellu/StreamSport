"""Strict demographic detection (women / youth) for StreamSport.

CRITICAL RULE:
`detect_demographics` MUST ONLY analyze competition/league/stage names — never
athlete or team names — so male initials such as "D. Medvedev" or "F. Cobolli"
can never false-trigger "Donne" or "Femminile".
"""

from __future__ import annotations

import re
import unicodedata

# Explicit women's competition tokens and circuit identifiers.
# Deliberately excludes bare single-letter patterns like "D." or "F." or "W."
# so player/team names can never trigger false positives.
_WOMEN_COMPETITION_RE = re.compile(
    r"(?:"
    r"\bwomen(?:'?s)?\b"
    r"|\bwomans?\b"
    r"|\bfemminil[ei]\b"
    r"|\bdonne\b"
    r"|\bladies\b"
    r"|\bfemenin[ao]\b"
    r"|\bfemenil\b"
    r"|\bfeminin[ae]?\b"
    r"|\bfem\.?\b"
    r"|\bfemm\.?\b"
    r"|\bfrauen\b"
    r"|\bdames\b"
    r"|\bvrouwen\b"
    r"|\bkvinder\b"
    r"|\bdamallsvenskan\b"
    r"|\bgirls\b"
    r"|\bragazze\b"
    r"|\bwta\b"
    r"|\bwnba\b"
    r"|\buwcl\b"
    r"|\bnwsl\b"
    r"|\bwsl\b"
    r"|\bbjk\s*cup\b"
    r"|\bbillie\s+jean\s+king\b"
    r"|\bliga\s+f\b"
    r"|\bserie\s+a\s+ebay\b"
    r"|\bebay\s+serie\s+a\b"
    r"|\bd1\s+arkema\b"
    r"|\bsultanlar\s+ligi\b"
    r"|\bwe\s+league\b"
    r"|\bnadeshiko\b"
    r"|\bshebelieves\b"
    r"|\barnold\s+clark\b"
    r"|\bpinatar\s+cup\b"
    r"|\bw(?:15|25|35|40|50|60|75|80|100)\b"
    r"|[\(\[]\s*w\s*[\)\]]"
    r")",
    re.IGNORECASE,
)

# Explicit youth / age-group competition markers.
# Deliberately avoids bare "young" (to avoid colliding with club names like Young Boys
# if a stream title ever leaks into competition text).
_YOUTH_COMPETITION_RE = re.compile(
    r"(?:"
    r"\bu[- ]?(?:1[3-9]|2[0-3])s?\b"
    r"|\bunder[- ]?(?:1[3-9]|2[0-3])\b"
    r"|\bprimavera(?:\s*[123])?\b"
    r"|\byouth\b"
    r"|\bjuniors?\b"
    r"|\bjuniores\b"
    r"|\bnext\s*gen\b"
    r"|\bavenir\b"
    r"|\bespoirs?\b"
    r"|\bberretti\b"
    r"|\ballievi\b"
    r"|\bgiovanissimi\b"
    r"|\bgiovanili\b"
    r"|\bacademy\b"
    r"|\bpremier\s+league\s+2\b"
    r"|\bpl2\b"
    r"|\bviareggio\s+cup\b"
    r"|\btorneo\s+di\s+viareggio\b"
    r"|\bmaurice\s+revello\b"
    r"|\btoulon\s+tournament\b"
    r"|\bboys\b"
    r")",
    re.IGNORECASE,
)

# Stream-title pattern: only explicit brackets/circuit/division markers are
# accepted so player initials like "D. Medvedev vs F. Cobolli" stay unaffected.
_WOMEN_STREAM_TITLE_RE = re.compile(
    r"(?:"
    r"[\(\[]\s*(?:w|women|woman|donne|femminile|ladies|f)\s*[\)\]]"
    r"|\bwomen(?:'?s)?\b"
    r"|\bdonne\b"
    r"|\bfemminil[ei]\b"
    r"|\bladies\b"
    r"|\bfrauen\b"
    r"|\bfemenin[ao]\b"
    r"|\bfemenil\b"
    r"|\bwta\b"
    r"|\bwnba\b"
    r"|\buwcl\b"
    r"|\bnwsl\b"
    r"|\bwsl\b"
    r"|\bbjk\s*cup\b"
    r"|\bbillie\s+jean\s+king\b"
    r"|\bliga\s+f\b"
    r"|\bserie\s+a\s+ebay\b"
    r"|\bw(?:15|25|35|40|50|60|75|80|100)\b"
    r")",
    re.IGNORECASE,
)


def _strip_accents_lower(text: str) -> str:
    if not text:
        return ""
    normalized = unicodedata.normalize("NFKD", text)
    ascii_text = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    return ascii_text.lower().strip()


def detect_demographics(competition_name: str, country: str = "") -> tuple[bool, bool]:
    """Detect `(is_women, is_youth)` strictly from the competition/league name.

    Never pass athlete or team names into `competition_name`.
    `country` is accepted for call-site symmetry and ignored for demographic flags
    to prevent country strings from introducing false matches.
    """
    del country  # Intentionally unused: demographics depend solely on competition text.
    clean = _strip_accents_lower(competition_name)
    if not clean:
        return False, False

    is_women = bool(_WOMEN_COMPETITION_RE.search(clean))
    is_youth = bool(_YOUTH_COMPETITION_RE.search(clean))
    return is_women, is_youth


def detect_demographics_from_stream_title(title: str) -> tuple[bool, bool]:
    """Detect `(is_women, is_youth)` from a provider stream title.

    Only explicit demographic tags like `(W)`, `(Women)`, `(Donne)`, `WTA`,
    `WNBA`, `U21`, `Primavera` match, keeping player names such as
    `"D. Medvedev vs F. Cobolli"` safe from false positives.
    """
    clean = _strip_accents_lower(title)
    if not clean:
        return False, False

    is_women = bool(_WOMEN_STREAM_TITLE_RE.search(clean))
    is_youth = bool(_YOUTH_COMPETITION_RE.search(clean))
    return is_women, is_youth
