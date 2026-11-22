"""StreamSport Phase 2 Visual Assets, Posters, and Banner rendering package."""

from app.graphics.banner_service import (
    BANNER_HEIGHT,
    BANNER_WIDTH,
    BannerService,
    cleanup_poster_files,
    compute_badge_state,
    get_live_window_minutes,
)
from app.graphics.tennis_poster import (
    ATP_SILHOUETTE_BYTES,
    CANVAS_HEIGHT,
    CANVAS_WIDTH,
    TennisPosterService,
    fetch_athlete_cutout,
    load_fallback_silhouette_bytes,
    select_tournament_background,
)
from app.graphics.thesportsdb_service import (
    TheSportsDBService,
    render_dark_canvas,
)

__all__ = [
    "ATP_SILHOUETTE_BYTES",
    "BANNER_HEIGHT",
    "BANNER_WIDTH",
    "BannerService",
    "CANVAS_HEIGHT",
    "CANVAS_WIDTH",
    "TennisPosterService",
    "TheSportsDBService",
    "cleanup_poster_files",
    "compute_badge_state",
    "fetch_athlete_cutout",
    "get_live_window_minutes",
    "load_fallback_silhouette_bytes",
    "render_dark_canvas",
    "select_tournament_background",
]
