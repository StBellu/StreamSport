"""Bilateral invertible RapidFuzz team/event matcher for StreamSport."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from rapidfuzz import fuzz

from app.taxonomy.aliases import (
    CANONICAL_ENTITIES,
    expand_team_variants,
)

MAX_TIME_DIFF_MS = 60 * 60 * 1000  # 60 minutes
DEMOGRAPHIC_PENALTY = 0.30
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


def _string_similarity(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0
    if _has_conflicting_identity(a, b):
        return min(fuzz.token_sort_ratio(a, b) / 100.0, 0.50)

    tokens_a = a.split()
    tokens_b = b.split()
    min_tokens = min(len(tokens_a), len(tokens_b))
    max_tokens = max(len(tokens_a), len(tokens_b))
    shorter_len = min(len(a), len(b))

    # Guard against short single-token subset inflation in token_set_ratio
    # (e.g., "real" vs. "real betis" or "city" vs. "bristol city").
    if min_tokens == 1 and max_tokens >= 2 and shorter_len <= 5:
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

        penalty = (
            self.demographic_penalty
            if (is_women_1 != is_women_2 or is_youth_1 != is_youth_2)
            else 0.0
        )

        h1_vars = expand_team_variants(home_1, alt_home_1)
        h2_vars = expand_team_variants(home_2, alt_home_2)
        a1_vars = expand_team_variants(away_1, alt_away_1)
        a2_vars = expand_team_variants(away_2, alt_away_2)

        # Single-entity fallback (e.g., F1 / MotoGP / Simulcast / Boxing cards).
        if not a1_vars and not a2_vars:
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
        )


DEFAULT_TEAM_MATCHER = TeamMatcher()
