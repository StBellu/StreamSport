"""Phase 2 Ingestion services: Diretta.it/Flashscore schedule & Italian EPG."""

from app.ingestion.diretta_service import (
    FEED_DAYS,
    SPORT_IDS,
    DirettaService,
    SportFeedSpec,
    deabbreviate_tennis_name,
    extract_tennis_cities,
    extract_tv_broadcasters,
    parse_flashscore_feed,
)
from app.ingestion.epg_service import (
    SKY_CHANNELS,
    SKY_FEED_DAYS,
    SKY_GENERALIST_CHANNELS,
    SKY_SPORTS_CHANNELS,
    ItalianEPGService,
    clamp_sky_from,
    deduplicate_simulcasts,
    filter_ghost_epg_matches,
    is_non_live_or_studio,
    is_out_of_calendar_tennis,
    reconcile_with_schedule,
)
from app.ingestion.scheduler import (
    PipelineScheduler,
    build_catalog_buckets,
    is_match_live_window,
)

__all__ = [
    "DirettaService",
    "FEED_DAYS",
    "ItalianEPGService",
    "PipelineScheduler",
    "SKY_CHANNELS",
    "SKY_FEED_DAYS",
    "SKY_GENERALIST_CHANNELS",
    "SKY_SPORTS_CHANNELS",
    "SPORT_IDS",
    "SportFeedSpec",
    "build_catalog_buckets",
    "clamp_sky_from",
    "deabbreviate_tennis_name",
    "deduplicate_simulcasts",
    "extract_tennis_cities",
    "extract_tv_broadcasters",
    "filter_ghost_epg_matches",
    "is_match_live_window",
    "is_non_live_or_studio",
    "is_out_of_calendar_tennis",
    "parse_flashscore_feed",
    "reconcile_with_schedule",
]
