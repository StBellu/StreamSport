"""StreamSport FastAPI application entry point and lifespan manager."""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from typing import AsyncIterator, Optional

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from app.config import Settings, get_settings
from app.core.database import DatabaseManager
from app.core.ram_cache import RAMCacheManager
from app.graphics import BannerService, TennisPosterService, TheSportsDBService
from app.ingestion import DirettaService, ItalianEPGService, PipelineScheduler
from app.providers import (
    DaddyLiveService,
    Fctv33Service,
    MandraKodiService,
    SponService,
    StreamFreeService,
    StreamOrchestrator,
    TvVooService,
)
from app.routers import configure_router, media_router, stremio_router

logger = logging.getLogger(__name__)


class HealthResponse(BaseModel):
    """Health check status payload serialized directly by pydantic-core."""

    status: str
    matches: int
    updated_at: int


_NOISY_THIRD_PARTY_LOGGERS: tuple[str, ...] = (
    "aiosqlite",
    "httpx",
    "httpcore",
    "hpack",
    "PIL",
    "asyncio",
    "urllib3",
    "charset_normalizer",
    "multipart",
    "watchfiles",
)


def _configure_logging(level_name: str) -> None:
    """Configure application-wide logging level without letting 3rd-party libs spam DEBUG."""
    level = getattr(logging, (level_name or "INFO").strip().upper(), logging.INFO)
    root_level = max(level, logging.INFO)
    logging.basicConfig(
        level=root_level,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    logging.getLogger().setLevel(root_level)
    logging.getLogger("app").setLevel(level)
    for lib_name in _NOISY_THIRD_PARTY_LOGGERS:
        logging.getLogger(lib_name).setLevel(logging.WARNING)


def create_app(settings_override: Optional[Settings] = None) -> FastAPI:
    """Build and configure the StreamSport FastAPI application."""
    resolved_settings = settings_override or get_settings()
    _configure_logging(resolved_settings.LOG_LEVEL)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        settings = settings_override or get_settings()
        _configure_logging(settings.LOG_LEVEL)
        settings.DATA_DIR.mkdir(parents=True, exist_ok=True)
        settings.POSTER_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        settings.BASE_POSTER_CACHE_DIR.mkdir(parents=True, exist_ok=True)

        db = DatabaseManager(db_path=settings.DB_PATH)
        await db.connect()

        ram_cache = RAMCacheManager()
        diretta = DirettaService(settings=settings)
        epg = ItalianEPGService(settings=settings)
        tsdb = TheSportsDBService(settings=settings)
        tennis_poster = TennisPosterService(settings=settings)
        banner = BannerService(
            settings=settings,
            tsdb_service=tsdb,
            tennis_service=tennis_poster,
        )

        # Share the exact same TvVooService (and LiveManifestManager) between orchestrator and /live/manifest.m3u8
        tvvoo = TvVooService(settings=settings)
        providers = [
            tvvoo,
            MandraKodiService(settings=settings),
            SponService(settings=settings),
            DaddyLiveService(settings=settings),
            StreamFreeService(settings=settings),
            Fctv33Service(settings=settings),
        ]
        orchestrator = StreamOrchestrator(providers=providers, settings=settings)

        scheduler = PipelineScheduler(
            settings=settings,
            db=db,
            ram_cache=ram_cache,
            diretta=diretta,
            epg=epg,
            orchestrator=orchestrator,
            tsdb=tsdb,
            banner=banner,
        )

        app.state.settings = settings
        app.state.db = db
        app.state.ram_cache = ram_cache
        app.state.diretta = diretta
        app.state.epg = epg
        app.state.tsdb = tsdb
        app.state.tennis_poster = tennis_poster
        app.state.banner = banner
        app.state.tvvoo = tvvoo
        app.state.orchestrator = orchestrator
        app.state.scheduler = scheduler

        await scheduler.hydrate_from_db()

        disable_bg = os.environ.get("DISABLE_BACKGROUND_SCHEDULER", "").strip().lower() in (
            "1",
            "true",
            "yes",
        )
        if settings.APP_ENV.strip().lower() != "test" and not disable_bg:
            scheduler.start()

        try:
            yield
        finally:
            await scheduler.stop()
            await orchestrator.aclose()
            await tsdb.aclose()
            await tennis_poster.aclose()
            await db.close()

    application = FastAPI(
        title="StreamSport",
        version="1.0.0",
        lifespan=lifespan,
    )
    application.state.settings = resolved_settings

    application.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @application.get("/health")
    async def health_check(request: Request) -> HealthResponse:
        ram_cache: RAMCacheManager = request.app.state.ram_cache
        snap = ram_cache.snapshot
        return HealthResponse(
            status="ok",
            matches=len(snap.active_matches),
            updated_at=snap.updated_at,
        )

    # Register configure and media routes before parameterized /{config}/... routes
    application.include_router(configure_router)
    application.include_router(media_router)
    application.include_router(stremio_router)

    return application


app = create_app()
