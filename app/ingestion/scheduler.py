"""Background PipelineScheduler and O(1) catalog bucket builder."""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Optional

from app.config import Settings, get_settings
from app.core.database import DatabaseManager
from app.core.ram_cache import RAMCacheManager
from app.graphics.banner_service import (
    PRE_LIVE_WINDOW_MS,
    BannerService,
    get_live_window_minutes,
)
from app.graphics.thesportsdb_service import TheSportsDBService
from app.ingestion.diretta_service import DirettaService, extract_tennis_cities
from app.ingestion.epg_service import (
    ItalianEPGService,
    filter_ghost_epg_matches,
    reconcile_with_schedule,
)
from app.models.domain import EPGProgram, Match, SportSilo
from app.providers.stream_orchestrator import StreamOrchestrator
from app.providers.tvvoo_service import TvVooService
from app.taxonomy.classifier import (
    CATALOG_DEFINITIONS,
    LIVE_CATALOG_ID,
    matches_catalog_filter,
)

logger = logging.getLogger(__name__)


def _live_bounds_ms(match: Match) -> tuple[int, int]:
    """Return `(live_start_ms, live_end_ms)` for a match."""
    live_win_ms = get_live_window_minutes(match.silo) * 60 * 1000
    live_start = match.date - PRE_LIVE_WINDOW_MS
    live_end = max(match.end_date, match.date + live_win_ms)
    return live_start, live_end


def is_match_live_window(match: Match, now_ms: int) -> bool:
    """Return True if `now_ms` falls within `[date - 20m, max(end_date, date + live_window)]`."""
    if match.date <= 0:
        return False
    live_start, live_end = _live_bounds_ms(match)
    return live_start <= now_ms <= live_end


def _smart_sort_key(match: Match, now_ms: int) -> tuple[int, int, int, str]:
    """Sort live matches first (popular boosted, start asc), then upcoming, then finished."""
    live_start, live_end = _live_bounds_ms(match)
    if match.date > 0 and live_start <= now_ms <= live_end:
        return (0, 0 if match.popular else 1, match.date, match.title.casefold())
    if match.date <= 0 or now_ms < live_start:
        return (1, 0 if match.popular else 1, match.date, match.title.casefold())
    return (2, 0 if match.popular else 1, -match.date, match.title.casefold())


def build_catalog_buckets(
    matches: list[Match],
    *,
    now_ms: Optional[int] = None,
) -> tuple[dict[str, tuple[Match, ...]], dict[str, tuple[str, ...]]]:
    """Partition and smart-sort matches into catalog buckets and active genre lists."""
    current_ms = int(now_ms if now_ms is not None else time.time() * 1000)
    ordered = sorted(matches, key=lambda m: _smart_sort_key(m, current_ms))

    matches_by_catalog: dict[str, tuple[Match, ...]] = {}
    genres_by_catalog: dict[str, tuple[str, ...]] = {}

    for cat_def in CATALOG_DEFINITIONS:
        if cat_def.id == LIVE_CATALOG_ID:
            bucket = [
                m
                for m in ordered
                if is_match_live_window(m, current_ms)
                and matches_catalog_filter(
                    m.silo.value, m.category, m.genres, cat_def.id, None
                )
            ]
        else:
            bucket = [
                m
                for m in ordered
                if matches_catalog_filter(
                    m.silo.value, m.category, m.genres, cat_def.id, None
                )
            ]

        matches_by_catalog[cat_def.id] = tuple(bucket)

        active_genres: list[str] = []
        seen_genres: set[str] = set()
        for declared_genre in cat_def.genres:
            if any(
                matches_catalog_filter(
                    m.silo.value, m.category, m.genres, cat_def.id, declared_genre
                )
                for m in bucket
            ):
                key = declared_genre.casefold()
                if key not in seen_genres:
                    seen_genres.add(key)
                    active_genres.append(declared_genre)

        if cat_def.id != LIVE_CATALOG_ID:
            for m in bucket:
                for g in m.genres:
                    clean_g = g.strip()
                    if clean_g and clean_g.casefold() not in seen_genres:
                        seen_genres.add(clean_g.casefold())
                        active_genres.append(clean_g)

        genres_by_catalog[cat_def.id] = tuple(active_genres)

    return matches_by_catalog, genres_by_catalog


class PipelineScheduler:
    """Orchestrates startup DB hydration and periodic ingestion/enrichment/cache cycles."""

    def __init__(
        self,
        settings: Optional[Settings] = None,
        *,
        db: DatabaseManager,
        ram_cache: RAMCacheManager,
        diretta: DirettaService,
        epg: ItalianEPGService,
        orchestrator: StreamOrchestrator,
        tsdb: TheSportsDBService,
        banner: BannerService,
    ) -> None:
        self._settings = settings or get_settings()
        self._db = db
        self._ram_cache = ram_cache
        self._diretta = diretta
        self._epg = epg
        self._orchestrator = orchestrator
        self._tsdb = tsdb
        self._banner = banner

        self._epg_cache: list[EPGProgram] = []
        self._last_epg_sync_monotonic: float = 0.0
        self._task: Optional[asyncio.Task[None]] = None
        self._stop_event = asyncio.Event()
        self._run_lock = asyncio.Lock()

    async def hydrate_from_db(self, *, now_ms: Optional[int] = None) -> int:
        """Load persisted active matches into RAMCacheManager at startup for instant O(1) serving."""
        current_ms = int(now_ms if now_ms is not None else time.time() * 1000)
        retention_ms = self._settings.MATCH_RETENTION_HOURS * 3600 * 1000
        min_end_ms = current_ms - retention_ms

        active_matches = await self._db.load_active_matches(min_end_ms)
        matches_by_catalog, genres_by_catalog = build_catalog_buckets(
            active_matches, now_ms=current_ms
        )
        tennis_cities: set[str] = set()
        for m in active_matches:
            if m.silo == SportSilo.TENNIS:
                tennis_cities.update(extract_tennis_cities(m.competition, m.alt_league))

        self._ram_cache.update_snapshot(
            active_matches=active_matches,
            matches_by_catalog=matches_by_catalog,
            genres_by_catalog=genres_by_catalog,
            active_tennis_cities=frozenset(tennis_cities),
            updated_at=current_ms,
        )
        logger.info("Hydrated %d active matches from SQLite into RAM cache", len(active_matches))
        return len(active_matches)

    def _cleanup_tvvoo_sessions(self) -> int:
        removed = 0
        for provider in self._orchestrator.providers:
            if isinstance(provider, TvVooService):
                removed += provider.live_manifest_manager.cleanup_idle_sessions()
        return removed

    async def run_once(self, *, now_ms: Optional[int] = None) -> int:
        """Execute one full ingestion -> EPG -> streams -> TSDB -> DB -> RAM snapshot cycle."""
        async with self._run_lock:
            current_ms = int(now_ms if now_ms is not None else time.time() * 1000)
            logger.info("Pipeline cycle started")

            # 1. Official schedule ingestion
            diretta_matches, active_tennis_cities = await self._diretta.fetch_schedule()
            logger.info(
                "Diretta ingestion completed: %d matches, %d active tennis cities",
                len(diretta_matches),
                len(active_tennis_cities),
            )

            # 2. Italian EPG ingestion (throttled by EPG_SYNC_INTERVAL_SECONDS)
            if self._settings.ENABLE_SKY_EPG or self._settings.ENABLE_TIVUSAT_EPG:
                mono_now = time.monotonic()
                if (
                    not self._epg_cache
                    or (mono_now - self._last_epg_sync_monotonic)
                    >= self._settings.EPG_SYNC_INTERVAL_SECONDS
                ):
                    self._epg_cache = await self._epg.fetch_epg(active_tennis_cities)
                    self._last_epg_sync_monotonic = mono_now
                epg_programs = list(self._epg_cache)
            else:
                self._epg_cache = []
                epg_programs = []

            enriched, orphan_matches = reconcile_with_schedule(diretta_matches, epg_programs)
            combined_matches = [*enriched, *orphan_matches]
            logger.info(
                "EPG reconciliation completed: %d EPG programs, %d orphan EPG candidates",
                len(epg_programs),
                len(orphan_matches),
            )

            # 3. Concurrent stream harvesting & top-down attachment (+ ghost EPG filter)
            harvest_results = await self._orchestrator.harvest_all()
            for provider_name, res in harvest_results.items():
                if res.error:
                    logger.warning(
                        "Provider '%s' harvest error: %s",
                        provider_name,
                        res.error,
                    )
                else:
                    logger.info(
                        "Provider '%s' harvested: %d events, %d linear channels",
                        provider_name,
                        len(res.events),
                        len(res.linear_channels),
                    )
            matches = self._orchestrator.attach_streams(combined_matches, harvest_results)
            matches = filter_ghost_epg_matches(matches)
            with_streams = sum(1 for m in matches if m.sources)
            logger.info(
                "Stream attachment completed: %d/%d matches have streams",
                with_streams,
                len(matches),
            )

            # 4. TheSportsDB calendar sync & in-RAM poster reconciliation
            calendar_by_silo = None
            if self._settings.ENABLE_THESPORTSDB:
                calendar_by_silo = await self._tsdb.sync_calendar()
                reconciled: list[Match] = []
                for m in matches:
                    if m.silo != SportSilo.TENNIS and not m.poster:
                        poster_url = self._tsdb.reconcile_match(m)
                        if poster_url:
                            m = m.model_copy(update={"poster": poster_url})
                    reconciled.append(m)
                matches = reconciled

            # Ensure updated_at is populated for all matches before persistence
            matches = [
                m if m.updated_at > 0 else m.model_copy(update={"updated_at": current_ms})
                for m in matches
            ]

            # 5. Persist to SQLite, purge expired (> MATCH_RETENTION_HOURS), and clean disk posters
            await self._db.upsert_matches(matches)
            purged_ids = await self._db.purge_expired(self._settings.MATCH_RETENTION_HOURS)
            if purged_ids:
                self._banner.cleanup_poster_files(purged_ids)

            # 6. Reload active matches and swap RAM snapshot atomically
            retention_ms = self._settings.MATCH_RETENTION_HOURS * 3600 * 1000
            min_end_ms = current_ms - retention_ms
            active_matches = await self._db.load_active_matches(min_end_ms)

            matches_by_catalog, genres_by_catalog = build_catalog_buckets(
                active_matches, now_ms=current_ms
            )
            self._ram_cache.update_snapshot(
                active_matches=active_matches,
                matches_by_catalog=matches_by_catalog,
                genres_by_catalog=genres_by_catalog,
                active_tennis_cities=active_tennis_cities,
                calendar_by_silo=calendar_by_silo,
                updated_at=current_ms,
            )

            self._cleanup_tvvoo_sessions()
            logger.info(
                "Pipeline cycle completed: %d ingested, %d active in RAM, %d purged",
                len(matches),
                len(active_matches),
                len(purged_ids),
            )
            return len(active_matches)

    async def _loop(self) -> None:
        interval = max(10, int(self._settings.SYNC_INTERVAL_SECONDS))
        while not self._stop_event.is_set():
            try:
                await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.exception("PipelineScheduler cycle failed: %s", exc)
                self._cleanup_tvvoo_sessions()

            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=interval)
            except asyncio.TimeoutError:
                continue

    def start(self) -> None:
        """Start the background periodic sync task if not already running."""
        if self._task is not None and not self._task.done():
            return
        self._stop_event.clear()
        self._task = asyncio.create_task(self._loop(), name="streamsport-pipeline-scheduler")

    async def stop(self) -> None:
        """Signal the background loop to stop and await task completion."""
        self._stop_event.set()
        if self._task is not None:
            if not self._task.done():
                self._task.cancel()
                try:
                    await self._task
                except asyncio.CancelledError:
                    pass
            self._task = None
