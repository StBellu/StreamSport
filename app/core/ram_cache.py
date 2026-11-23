"""Zero-latency in-memory RAM cache with atomic snapshot replacement."""

import time
from dataclasses import dataclass, field
from typing import Any, Optional

from app.models.domain import Match
from app.taxonomy.classifier import NUVIO_CATALOG_LOOKUP, _normalize_catalog_id


@dataclass(frozen=True, slots=True)
class CatalogSnapshot:
    """Immutable read-only snapshot served by Stremio/Nuvio HTTP routes."""

    matches_by_id: dict[str, Match] = field(default_factory=dict)
    active_matches: tuple[Match, ...] = ()
    matches_by_catalog: dict[str, tuple[Match, ...]] = field(default_factory=dict)
    genres_by_catalog: dict[str, tuple[str, ...]] = field(default_factory=dict)
    calendar_by_silo: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    active_tennis_cities: frozenset[str] = field(default_factory=frozenset)
    updated_at: int = 0


def _matches_search(match: Match, needle: str) -> bool:
    """Case-insensitive substring check across key match text fields."""
    fields = [
        match.title,
        match.competition,
        match.canonical_competition,
        match.alt_league,
        match.alt_home,
        match.alt_away,
        match.alt_title,
        match.alt_competition,
        match.alt_country,
        match.home_team.name if match.home_team else "",
        match.away_team.name if match.away_team else "",
    ]
    return any(needle in value.lower() for value in fields if value)


class RAMCacheManager:
    """Holds the active CatalogSnapshot and swaps it atomically on updates."""

    def __init__(self) -> None:
        self._snapshot = CatalogSnapshot()

    @property
    def snapshot(self) -> CatalogSnapshot:
        """Return the current immutable catalog snapshot."""
        return self._snapshot

    def get_match(self, match_id: str) -> Optional[Match]:
        """O(1) lookup by raw match ID or `streamsport:`-prefixed Stremio ID."""
        return self._snapshot.matches_by_id.get(match_id)

    def get_catalog(
        self,
        catalog_id: str,
        genre: Optional[str] = None,
        search: Optional[str] = None,
        skip: int = 0,
        limit: int = 100,
    ) -> list[Match]:
        """Filter and paginate matches for a catalog purely in RAM."""
        if limit <= 0:
            return []
        start = max(skip, 0)

        effective_genre = genre
        if catalog_id in NUVIO_CATALOG_LOOKUP:
            base_cat_id, mapped_genre = NUVIO_CATALOG_LOOKUP[catalog_id]
            items = self._snapshot.matches_by_catalog.get(base_cat_id, ())
            effective_genre = genre or mapped_genre
        else:
            items = self._snapshot.matches_by_catalog.get(catalog_id)
            if items is None:
                norm_id = _normalize_catalog_id(catalog_id)
                items = self._snapshot.matches_by_catalog.get(norm_id, ())

        if effective_genre:
            genre_norm = effective_genre.strip().lower()
            items = tuple(
                m for m in items if any(g.lower() == genre_norm for g in m.genres)
            )

        if search:
            needle = search.strip().lower()
            if needle:
                items = tuple(m for m in items if _matches_search(m, needle))

        return list(items[start : start + limit])

    def update_snapshot(
        self,
        active_matches: list[Match] | tuple[Match, ...],
        matches_by_catalog: dict[str, tuple[Match, ...]],
        genres_by_catalog: dict[str, tuple[str, ...]],
        active_tennis_cities: frozenset[str] | set[str] = frozenset(),
        calendar_by_silo: Optional[dict[str, list[dict[str, Any]]]] = None,
        updated_at: Optional[int] = None,
    ) -> None:
        """Atomically build and swap in a fresh CatalogSnapshot."""
        matches_tuple = tuple(active_matches)
        by_id: dict[str, Match] = {}
        for match in matches_tuple:
            by_id[match.id] = match
            by_id[match.stremio_id] = match

        prev = self._snapshot
        ts = updated_at if updated_at is not None else int(time.time() * 1000)
        self._snapshot = CatalogSnapshot(
            matches_by_id=by_id,
            active_matches=matches_tuple,
            matches_by_catalog=dict(matches_by_catalog),
            genres_by_catalog=dict(genres_by_catalog),
            calendar_by_silo=(
                dict(calendar_by_silo)
                if calendar_by_silo is not None
                else prev.calendar_by_silo
            ),
            active_tennis_cities=frozenset(active_tennis_cities),
            updated_at=ts,
        )

    def update_calendar(self, calendar_by_silo: dict[str, list[dict[str, Any]]]) -> None:
        """Atomically update the cached TheSportsDB calendar index in the snapshot."""
        prev = self._snapshot
        self._snapshot = CatalogSnapshot(
            matches_by_id=prev.matches_by_id,
            active_matches=prev.active_matches,
            matches_by_catalog=prev.matches_by_catalog,
            genres_by_catalog=prev.genres_by_catalog,
            calendar_by_silo=dict(calendar_by_silo),
            active_tennis_cities=prev.active_tennis_cities,
            updated_at=prev.updated_at,
        )
