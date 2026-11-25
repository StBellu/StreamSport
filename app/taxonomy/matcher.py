"""Bilateral invertible RapidFuzz team/event matcher for StreamSport."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from rapidfuzz import fuzz

from app.taxonomy.aliases import (
    CANONICAL_ENTITIES,
    _basic_clean,
    expand_team_variants,
)

MAX_TIME_DIFF_MS = 60 * 60 * 1000  # 60 minutes
DEMOGRAPHIC_PENALTY = 0.30
NEUTRAL_WOMEN_PENALTY = 0.08
AVG_THRESHOLD = 0.75
MIN_SIDE_THRESHOLD = 0.65
SINGLE_ENTITY_THRESHOLD = 0.80

# Mutually exclusive club identity qualifiers (prevents derby collisions such as
# "Manchester United" vs. "Manchester City" or "Real Madrid" vs. "Atletico Madrid").
_CONFLICTING_QUALIFIERS: tuple[tuple[str, str], ...] = (
    ("united", "city"),
    ("utd", "city"),
    ("real", "atletico"),
    ("real", "atl"),
    ("wednesday", "united"),
    ("rovers", "wanderers"),
)


@dataclass(frozen=True, slots=True)
class MatchResult:
    """Outcome of bilateral team/entity matching."""

    matched: bool
    score: float
    inverted: bool = False
    direct_score: float = 0.0
    inverted_score: float = 0.0
    min_side_score: float = 0.0
    demographic_penalty: float = 0.0
    reason: str = ""


def _has_conflicting_identity(a: str, b: str) -> bool:
    tokens_a = set(a.split())
    tokens_b = set(b.split())
    for q1, q2 in _CONFLICTING_QUALIFIERS:
        if (q1 in tokens_a and q2 in tokens_b) or (q2 in tokens_a and q1 in tokens_b):
            return True
    return False


def _has_initial_surname_conflict(a: str, b: str) -> bool:
    """Prevent collisions like 'A. Muller' vs 'G. Muller' or 'Jannik Sinner' vs 'M. Sinner'."""
    ta = a.split()
    tb = b.split()
    if len(ta) < 2 or len(tb) < 2:
        return False
    has_single_a = any(len(t) == 1 and t.isalpha() for t in ta)
    has_single_b = any(len(t) == 1 and t.isalpha() for t in tb)
    if not (has_single_a or has_single_b):
        return False

    # Case 1: "<initial/first> ... <surname>" (e.g. "j sinner" vs "m sinner", "jannik sinner" vs "m sinner")
    if ta[-1] == tb[-1] and len(ta[-1]) >= 3:
        if ta[0][0] != tb[0][0]:
            return True

    # Case 2: "<surname> <initial...>" (e.g. "muller a" vs "muller g")
    if ta[0] == tb[0] and len(ta[0]) >= 3:
        if ta[-1][0] != tb[-1][0] and (len(ta[-1]) == 1 or len(tb[-1]) == 1):
            return True

    # Case 3: cross-order "<surname> <initial>" vs "<initial/first> ... <surname>"
    # (e.g. Flashscore "muller a" vs Sky/provider "g muller" or "gilles muller")
    if len(ta[-1]) == 1 and len(ta[0]) >= 3 and ta[0] == tb[-1]:
        if ta[-1][0] != tb[0][0]:
            return True

    # Case 4: reverse cross-order "<initial/first> ... <surname>" vs "<surname> <initial>"
    if len(tb[-1]) == 1 and len(tb[0]) >= 3 and tb[0] == ta[-1]:
        if tb[-1][0] != ta[0][0]:
            return True

    return False


def _string_similarity(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0
    if a.replace(" ", "") == b.replace(" ", ""):
        return 0.98
    if _has_conflicting_identity(a, b) or _has_initial_surname_conflict(a, b):
        return min(fuzz.token_sort_ratio(a, b) / 100.0, 0.45)

    tokens_a = a.split()
    tokens_b = b.split()
    min_tokens = min(len(tokens_a), len(tokens_b))
    max_tokens = max(len(tokens_a), len(tokens_b))
    shorter_len = min(len(a), len(b))

    # Guard against short single-token subset inflation in token_set_ratio
    # (e.g., "real" vs. "real betis", "city" vs. "bristol city", or "monaco" vs. "bayern monaco").
    if min_tokens == 1 and max_tokens >= 2 and shorter_len <= 7:
        ts_or_sort = fuzz.token_sort_ratio(a, b) / 100.0
        wr = fuzz.WRatio(a, b) / 100.0
        return 0.65 * ts_or_sort + 0.35 * wr

    token_set = fuzz.token_set_ratio(a, b) / 100.0
    w_ratio = fuzz.WRatio(a, b) / 100.0
    return 0.5 * token_set + 0.5 * w_ratio


def _variants_similarity(variants_a: list[str], variants_b: list[str]) -> float:
    if not variants_a or not variants_b:
        return 0.0

    # If both sides map cleanly to distinct known canonical entities (e.g.
    # "internazionale" vs. "inter miami", or "manchester united" vs. "manchester city"),
    # never allow fuzzy substring/edit overlap to cross-match them.
    canons_a = {v for v in variants_a if v in CANONICAL_ENTITIES}
    canons_b = {v for v in variants_b if v in CANONICAL_ENTITIES}
    if canons_a and canons_b and canons_a.isdisjoint(canons_b):
        return 0.0

    best = 0.0
    for va in variants_a:
        for vb in variants_b:
            sim = _string_similarity(va, vb)
            if sim > best:
                best = sim
                if best >= 1.0:
                    return 1.0
    return best


def _extract_attr(obj: Any, *names: str, default: Any = None) -> Any:
    for name in names:
        if isinstance(obj, dict) and name in obj:
            val = obj[name]
            if val is not None:
                return val
        elif hasattr(obj, name):
            val = getattr(obj, name)
            if val is not None:
                return val
    return default


def _extract_teams(obj: Any) -> tuple[str, str]:
    teams = _extract_attr(obj, "teams")
    if teams is not None:
        home = _extract_attr(teams, "home", default="") or ""
        away = _extract_attr(teams, "away", default="") or ""
        return str(home), str(away)
    home = _extract_attr(obj, "home", "home_team", "title", default="") or ""
    away = _extract_attr(obj, "away", "away_team", default="") or ""
    return str(home), str(away)


def _extract_timestamp_ms(obj: Any) -> int:
    raw = _extract_attr(obj, "date", "start_time_ms", "timestamp_ms", "start_ms", default=0)
    try:
        return int(raw or 0)
    except (TypeError, ValueError):
        return 0


_MOTORSPORT_SERIES_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("f1", re.compile(r"\b(?:formula\s*(?:1|one)|f1)\b", re.IGNORECASE)),
    ("f2", re.compile(r"\b(?:formula\s*2|f2)\b", re.IGNORECASE)),
    ("f3", re.compile(r"\b(?:formula\s*3|f3)\b", re.IGNORECASE)),
    ("fe", re.compile(r"\b(?:formula\s*e|f\.?e\.?|abb\s+fia\s+formula\s*e)\b", re.IGNORECASE)),
    ("motogp", re.compile(r"\b(?:moto\s*gp)\b", re.IGNORECASE)),
    ("moto2", re.compile(r"\b(?:moto\s*2)\b", re.IGNORECASE)),
    ("moto3", re.compile(r"\b(?:moto\s*3)\b", re.IGNORECASE)),
    ("wsbk", re.compile(r"\b(?:world\s*sbk|wsbk|superbike)\b", re.IGNORECASE)),
    ("wrc", re.compile(r"\b(?:wrc|world\s*rally)\b", re.IGNORECASE)),
    ("nascar", re.compile(r"\bnascar\b", re.IGNORECASE)),
    ("indycar", re.compile(r"\bindy\s*car\b", re.IGNORECASE)),
    ("wec", re.compile(r"\b(?:wec|world\s*endurance)\b", re.IGNORECASE)),
)

_MOTORSPORT_SESSION_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("fp1", re.compile(r"\b(?:fp\s*1|free\s*practice\s*1|practice\s*1|prove\s*libere\s*1|libere\s*1)\b", re.IGNORECASE)),
    ("fp2", re.compile(r"\b(?:fp\s*2|free\s*practice\s*2|practice\s*2|prove\s*libere\s*2|libere\s*2)\b", re.IGNORECASE)),
    ("fp3", re.compile(r"\b(?:fp\s*3|free\s*practice\s*3|practice\s*3|prove\s*libere\s*3|libere\s*3)\b", re.IGNORECASE)),
    ("fp", re.compile(r"\b(?:free\s*practice|practice|prove\s*libere|warm\s*up)\b", re.IGNORECASE)),
    ("sprint_qualifying", re.compile(r"\b(?:sprint\s*(?:qualifying|qualifiche|shootout))\b", re.IGNORECASE)),
    ("sprint", re.compile(r"\bsprint(?:\s*race|\s*gara)?\b", re.IGNORECASE)),
    ("qualifying", re.compile(r"\b(?:qualifying|qualifiche|quali|q[123]|pole\s*position)\b", re.IGNORECASE)),
    ("race", re.compile(r"\b(?:feature\s*race|main\s*race|race|gara|grand\s*prix|gran\s*premio|\bgp\b)\b", re.IGNORECASE)),
)

_MOTORSPORT_STOPWORDS: frozenset[str] = frozenset(
    {
        "formula", "one", "f1", "f2", "f3", "fe", "motogp", "moto2", "moto3", "moto",
        "superbike", "wsbk", "worldsbk", "nascar", "indycar", "wec", "wrc",
        "prove", "libere", "free", "practice", "fp1", "fp2", "fp3",
        "qualifiche", "qualifying", "quali", "gara", "race", "sprint", "shootout",
        "feature", "main", "warm", "world", "championship", "motorsport", "motori",
        "grand", "prix", "gran", "premio", "gp", "live", "diretta", "sky", "sport",
        "sports", "channel", "canal", "stream", "italia", "the", "of", "and",
    }
)


def _extract_motorsport_series(text: str) -> str:
    for series_key, pattern in _MOTORSPORT_SERIES_PATTERNS:
        if pattern.search(text):
            return series_key
    return ""


def _extract_motorsport_session(text: str) -> str:
    for sess_key, pattern in _MOTORSPORT_SESSION_PATTERNS:
        if pattern.search(text):
            return sess_key
    return ""


def _motorsport_track_tokens(text: str) -> set[str]:
    cleaned = _basic_clean(text)
    return {
        tok
        for tok in cleaned.split()
        if len(tok) >= 3 and tok not in _MOTORSPORT_STOPWORDS and not tok.isdigit()
    }


def _match_motorsport_session(text_1: str, text_2: str, *, force_motorsport: bool = False) -> MatchResult | None:
    ser_1 = _extract_motorsport_series(text_1)
    ser_2 = _extract_motorsport_series(text_2)
    if not force_motorsport and not (ser_1 and ser_2):
        return None
    if not ser_1 and not ser_2:
        return None
    if ser_1 != ser_2:
        return MatchResult(matched=False, score=0.0, reason="motorsport_series_mismatch")

    sess_1 = _extract_motorsport_session(text_1)
    sess_2 = _extract_motorsport_session(text_2)
    if sess_1 and sess_2 and sess_1 != sess_2:
        return MatchResult(matched=False, score=0.0, reason="motorsport_session_mismatch")

    tracks_1 = _motorsport_track_tokens(text_1)
    tracks_2 = _motorsport_track_tokens(text_2)
    if tracks_1 and tracks_2:
        track_ok = bool(tracks_1 & tracks_2) or any(
            fuzz.ratio(w1, w2) >= 70.0 for w1 in tracks_1 for w2 in tracks_2
        )
        if not track_ok:
            return MatchResult(matched=False, score=0.0, reason="motorsport_track_mismatch")

    return MatchResult(
        matched=True,
        score=0.90,
        inverted=False,
        direct_score=0.90,
        inverted_score=0.0,
        min_side_score=0.90,
        demographic_penalty=0.0,
        reason="motorsport_session_matched",
    )


class TeamMatcher:
    """Bilateral RapidFuzz matcher with rigid time window & demographic penalty."""

    def __init__(
        self,
        *,
        max_time_diff_ms: int = MAX_TIME_DIFF_MS,
        demographic_penalty: float = DEMOGRAPHIC_PENALTY,
        avg_threshold: float = AVG_THRESHOLD,
        min_side_threshold: float = MIN_SIDE_THRESHOLD,
        single_entity_threshold: float = SINGLE_ENTITY_THRESHOLD,
    ) -> None:
        self.max_time_diff_ms = max_time_diff_ms
        self.demographic_penalty = demographic_penalty
        self.avg_threshold = avg_threshold
        self.min_side_threshold = min_side_threshold
        self.single_entity_threshold = single_entity_threshold

    def match_score(
        self,
        home_1: str,
        away_1: str,
        home_2: str,
        away_2: str,
        *,
        alt_home_1: str = "",
        alt_away_1: str = "",
        alt_home_2: str = "",
        alt_away_2: str = "",
        date_1_ms: int = 0,
        date_2_ms: int = 0,
        is_women_1: bool = False,
        is_youth_1: bool = False,
        is_women_2: bool = False,
        is_youth_2: bool = False,
        silo: str = "",
        competition_1: str = "",
        competition_2: str = "",
        allow_neutral_women_2: bool = False,
    ) -> MatchResult:
        """Compute bilateral (direct & inverted) similarity score with guards."""
        if date_1_ms > 0 and date_2_ms > 0:
            if abs(date_1_ms - date_2_ms) > self.max_time_diff_ms:
                return MatchResult(
                    matched=False,
                    score=0.0,
                    inverted=False,
                    reason="time_window_exceeded",
                )

        if is_youth_1 != is_youth_2:
            penalty = self.demographic_penalty
        elif is_women_1 != is_women_2:
            if allow_neutral_women_2 and is_women_1 and not is_women_2:
                penalty = NEUTRAL_WOMEN_PENALTY
            else:
                penalty = self.demographic_penalty
        else:
            penalty = 0.0

        h1_vars = expand_team_variants(home_1, alt_home_1)
        h2_vars = expand_team_variants(home_2, alt_home_2)
        a1_vars = expand_team_variants(away_1, alt_away_1)
        a2_vars = expand_team_variants(away_2, alt_away_2)

        # Single-entity fallback (e.g., F1 / MotoGP / Simulcast / Boxing cards).
        if not a1_vars and not a2_vars:
            silo_low = (silo or "").strip().lower()
            is_moto_silo = silo_low in ("motori", "motor-sports", "motorsport", "motorsports")
            moto_text_1 = f"{competition_1} {home_1} {alt_home_1}".strip()
            moto_text_2 = f"{competition_2} {home_2} {alt_home_2}".strip()
            moto_res = _match_motorsport_session(
                moto_text_1,
                moto_text_2,
                force_motorsport=is_moto_silo,
            )
            if moto_res is not None:
                return moto_res

            raw_score = _variants_similarity(h1_vars, h2_vars)
            final_score = max(0.0, raw_score - penalty)
            matched = final_score >= self.single_entity_threshold
            return MatchResult(
                matched=matched,
                score=round(final_score, 4),
                inverted=False,
                direct_score=round(final_score, 4),
                inverted_score=0.0,
                min_side_score=round(final_score, 4),
                demographic_penalty=penalty,
                reason="single_entity" if matched else "below_single_entity_threshold",
            )

        # Mismatched arity (one side has away team, the other is single-title).
        if not a1_vars or not a2_vars:
            return MatchResult(
                matched=False,
                score=0.0,
                inverted=False,
                demographic_penalty=penalty,
                reason="arity_mismatch",
            )

        s_hh = _variants_similarity(h1_vars, h2_vars)
        s_aa = _variants_similarity(a1_vars, a2_vars)
        direct_avg = max(0.0, ((s_hh + s_aa) / 2.0) - penalty)
        direct_min = max(0.0, min(s_hh, s_aa) - penalty)

        s_ha = _variants_similarity(h1_vars, a2_vars)
        s_ah = _variants_similarity(a1_vars, h2_vars)
        inv_avg = max(0.0, ((s_ha + s_ah) / 2.0) - penalty)
        inv_min = max(0.0, min(s_ha, s_ah) - penalty)

        if (inv_avg, inv_min) > (direct_avg, direct_min):
            inverted = True
            best_avg = inv_avg
            best_min = inv_min
        else:
            inverted = False
            best_avg = direct_avg
            best_min = direct_min

        matched = best_avg >= self.avg_threshold and best_min >= self.min_side_threshold
        if matched:
            reason = "matched_inverted" if inverted else "matched_direct"
        elif penalty > 0:
            reason = "demographic_mismatch"
        else:
            reason = "below_threshold"

        return MatchResult(
            matched=matched,
            score=round(best_avg, 4),
            inverted=inverted,
            direct_score=round(direct_avg, 4),
            inverted_score=round(inv_avg, 4),
            min_side_score=round(best_min, 4),
            demographic_penalty=penalty,
            reason=reason,
        )

    def match_events(self, event: Any, candidate: Any) -> MatchResult:
        """Compare two `Match` or `ProviderEvent`-like objects."""
        h1, a1 = _extract_teams(event)
        h2, a2 = _extract_teams(candidate)

        return self.match_score(
            h1,
            a1,
            h2,
            a2,
            alt_home_1=str(_extract_attr(event, "alt_home", default="") or ""),
            alt_away_1=str(_extract_attr(event, "alt_away", default="") or ""),
            alt_home_2=str(_extract_attr(candidate, "alt_home", default="") or ""),
            alt_away_2=str(_extract_attr(candidate, "alt_away", default="") or ""),
            date_1_ms=_extract_timestamp_ms(event),
            date_2_ms=_extract_timestamp_ms(candidate),
            is_women_1=bool(_extract_attr(event, "is_women", default=False)),
            is_youth_1=bool(_extract_attr(event, "is_youth", default=False)),
            is_women_2=bool(_extract_attr(candidate, "is_women", default=False)),
            is_youth_2=bool(_extract_attr(candidate, "is_youth", default=False)),
            silo=str(_extract_attr(event, "silo", "category", default="") or ""),
            competition_1=str(_extract_attr(event, "competition", default="") or ""),
            competition_2=str(_extract_attr(candidate, "competition", default="") or ""),
        )


DEFAULT_TEAM_MATCHER = TeamMatcher()
