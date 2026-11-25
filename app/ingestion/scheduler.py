"""Background PipelineScheduler and O(1) catalog bucket builder."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import logging
import time
from typing import Optional

from app.config import Settings, get_settings
from app.core.database import DatabaseManager
from app.core.ram_cache import RAMCacheManager
from app.graphics._utils import get_timezone
from app.graphics.banner_service import (
    PRE_LIVE_WINDOW_MS,
    BannerService,
    get_live_window_minutes,
)
from app.graphics.thesportsdb_service import TheSportsDBService
from app.ingestion.diretta_service import DirettaService, extract_tennis_cities
from app.ingestion.epg_service import (
    ItalianEPGService,
    filter_conflicting_italian_channels,
    filter_ghost_epg_matches,
    reconcile_with_schedule,
)
from app.models.domain import (
    SKY_EPG_POST_END_MS,
    EPGProgram,
    Match,
    SportSilo,
    get_match_expiry_ms,
)
from app.providers.stream_orchestrator import StreamOrchestrator
from app.providers.tvvoo_service import TvVooService
from app.taxonomy.classifier import (
    CATALOG_DEFINITIONS,
    matches_catalog_filter,
)

logger = logging.getLogger(__name__)


def _live_bounds_ms(match: Match) -> tuple[int, int]:
    """Return `(live_start_ms, live_end_ms)` for a match."""
    live_start = match.date - PRE_LIVE_WINDOW_MS
    if match.end_date > 0:
        live_end = get_match_expiry_ms(match)
    else:
        live_win_ms = get_live_window_minutes(match.silo) * 60 * 1000
        live_end = match.date + live_win_ms
    return live_start, live_end


def is_match_live_window(match: Match, now_ms: int) -> bool:
    """Return True if `now_ms` falls within `[date - 20m, live_end]`."""
    if match.date <= 0:
        return False
    live_start, live_end = _live_bounds_ms(match)
    return live_start <= now_ms <= live_end


def _smart_sort_key(match: Match, now_ms: int) -> tuple[int, int, int, str]:
    """Sort live/post-match active events first (popular boosted, start asc), then upcoming."""
    live_start, live_end = _live_bounds_ms(match)
    if match.date > 0 and live_start <= now_ms <= live_end:
        return (0, 0 if match.popular else 1, match.date, match.title.casefold())
    return (1, 0 if match.popular else 1, match.date, match.title.casefold())


def build_catalog_buckets(
    matches: list[Match],
    *,
    now_ms: Optional[int] = None,
) -> tuple[dict[str, tuple[Match, ...]], dict[str, tuple[str, ...]]]:
    """Partition and smart-sort non-expired matches into catalog buckets and active genre lists."""
    current_ms = int(now_ms if now_ms is not None else time.time() * 1000)
    active = [
        m
        for m in matches
        if (exp := get_match_expiry_ms(m)) <= 0 or current_ms <= exp
    ]
    ordered = sorted(active, key=lambda m: _smart_sort_key(m, current_ms))

    matches_by_catalog: dict[str, tuple[Match, ...]] = {}
    genres_by_catalog: dict[str, tuple[str, ...]] = {}

    for cat_def in CATALOG_DEFINITIONS:
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
        loaded = await self._db.load_active_matches(now_ms=current_ms)
        active_matches = [
            m
            for m in loaded
            if (exp := get_match_expiry_ms(m)) <= 0 or current_ms <= exp
        ]
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
            ref_utc = datetime.fromtimestamp(current_ms / 1000.0, tz=timezone.utc)
            logger.info("Pipeline cycle started")

            # 1. Official schedule ingestion (preserve tennis cities on outage/0-match)
            prev_snapshot = self._ram_cache.snapshot
            diretta_matches, fetched_tennis_cities = await self._diretta.fetch_schedule(
                now_utc=ref_utc
            )
            discarded_ids = sorted(
                getattr(self._diretta, "last_discarded_match_ids", frozenset())
            )
            diretta_failed = bool(getattr(self._diretta, "last_fetch_failed", False))
            diretta_failed_feeds = int(getattr(self._diretta, "last_failed_feeds", 0))
            if (diretta_failed or (self._settings.ENABLE_DIRETTA and not diretta_matches)) and prev_snapshot.active_tennis_cities:
                active_tennis_cities = fetched_tennis_cities or prev_snapshot.active_tennis_cities
                logger.warning(
                    "Diretta ingestion returned %d matches (failed=%s); preserving %d active tennis cities",
                    len(diretta_matches),
                    diretta_failed,
                    len(active_tennis_cities),
                )
            else:
                active_tennis_cities = fetched_tennis_cities
                logger.info(
                    "Diretta ingestion completed: %d matches, %d active tennis cities",
                    len(diretta_matches),
                    len(active_tennis_cities),
                )

            # 2. Italian EPG ingestion (throttled by EPG_SYNC_INTERVAL_SECONDS, with stale fallback)
            epg_failed = False
            if self._settings.ENABLE_SKY_EPG:
                mono_now = time.monotonic()
                if (
                    not self._epg_cache
                    or (mono_now - self._last_epg_sync_monotonic)
                    >= self._settings.EPG_SYNC_INTERVAL_SECONDS
                ):
                    fresh_epg = await self._epg.fetch_epg(
                        active_tennis_cities, now_utc=ref_utc
                    )
                    if fresh_epg or not self._epg_cache:
                        self._epg_cache = fresh_epg
                        self._last_epg_sync_monotonic = mono_now
                    else:
                        logger.warning(
                            "EPG fetch returned 0 programs; retaining %d stale EPG programs",
                            len(self._epg_cache),
                        )
                epg_programs = [
                    p
                    for p in self._epg_cache
                    if p.end_ms <= 0 or current_ms <= p.end_ms + SKY_EPG_POST_END_MS
                ]
                epg_failed = bool(getattr(self._epg, "last_fetch_failed", False)) and not bool(
                    epg_programs
                )
            else:
                self._epg_cache = []
                epg_programs = []

            enriched, orphan_matches = reconcile_with_schedule(diretta_matches, epg_programs)
            combined_matches = [*enriched, *orphan_matches]
            epg_discarded = max(0, len(epg_programs) - len(orphan_matches))
            logger.info(
                "EPG reconciliation completed: %d Diretta matches, %d EPG programs, %d orphan candidates created (%d non-orphan/matched/discarded)",
                len(diretta_matches),
                len(epg_programs),
                len(orphan_matches),
                epg_discarded,
            )

            # Stale fallback on total ingestion outage to avoid wiping active RAM/DB catalog
            if (
                self._settings.ENABLE_DIRETTA
                and diretta_failed
                and not combined_matches
                and prev_snapshot.active_matches
            ):
                logger.warning(
                    "Total ingestion outage detected (0 Diretta + 0 EPG); preserving %d stale RAM matches",
                    len(prev_snapshot.active_matches),
                )
                return len(prev_snapshot.active_matches)

            # 3. Concurrent stream harvesting & top-down attachment (+ conflict & ghost EPG filters)
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
            matches = filter_conflicting_italian_channels(matches, epg_programs)
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

            # Filter out expired items before persisting and purge cancelled/expired rows
            matches = [
                m
                for m in matches
                if (exp := get_match_expiry_ms(m)) <= 0 or current_ms <= exp
            ]

            # 5. Persist to SQLite, prune stale/discarded/expired rows, and clean disk posters
            diretta_prune_ok = (not self._settings.ENABLE_DIRETTA) or (
                not diretta_failed
                and diretta_failed_feeds == 0
                and (bool(diretta_matches) or not prev_snapshot.active_matches)
            )
            epg_prune_ok = (not self._settings.ENABLE_SKY_EPG) or (not epg_failed)
            active_ids = {m.id for m in matches}

            await self._db.upsert_matches(matches)
            deleted_ids = (
                await self._db.delete_matches(discarded_ids)
                if discarded_ids
                else []
            )
            stale_ids: list[str] = []
            if diretta_prune_ok and epg_prune_ok:
                stale_ids = await self._db.prune_stale_matches(active_ids)
            elif epg_prune_ok:
                stale_ids = await self._db.prune_stale_matches(
                    active_ids, origins=("epg", "sky_epg")
                )
            elif diretta_prune_ok:
                stale_ids = await self._db.prune_stale_matches(
                    active_ids, origins=("diretta",)
                )
            purged_ids = await self._db.purge_expired(now_ms=current_ms)
            removed_ids = list(dict.fromkeys([*deleted_ids, *stale_ids, *purged_ids]))
            if removed_ids:
                self._banner.cleanup_poster_files(removed_ids)

            # 6. Reload active matches and swap RAM snapshot atomically
            loaded_active = await self._db.load_active_matches(now_ms=current_ms)
            active_matches = [
                m
                for m in loaded_active
                if (exp := get_match_expiry_ms(m)) <= 0 or current_ms <= exp
            ]

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
                "Pipeline cycle completed: %d ingested, %d active in RAM, %d removed (%d stale pruned)",
                len(matches),
                len(active_matches),
                len(removed_ids),
                len(stale_ids),
            )
            return len(active_matches)

    def get_current_sync_interval(self, now_utc: Optional[datetime] = None) -> int:
        """Return night interval (00:00–11:59) or day interval (12:00–23:59) in local timezone."""
        tz = get_timezone(self._settings.TIMEZONE)
        utc_dt = now_utc if now_utc is not None else datetime.now(timezone.utc)
        if utc_dt.tzinfo is None:
            utc_dt = utc_dt.replace(tzinfo=timezone.utc)
        local_dt = utc_dt.astimezone(tz)
        if 0 <= local_dt.hour < 12:
            return max(10, int(self._settings.SYNC_INTERVAL_SECONDS_NIGHT))
        return max(10, int(self._settings.SYNC_INTERVAL_SECONDS))

    async def _loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.exception("PipelineScheduler cycle failed: %s", exc)
                self._cleanup_tvvoo_sessions()

            interval = self.get_current_sync_interval()
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
        for svc in (self._diretta, self._epg):
            aclose = getattr(svc, "aclose", None)
            if callable(aclose):
                await aclose()
