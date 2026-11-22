"""FastAPI HTTP router modules for StreamSport."""

from app.routers.configure import router as configure_router
from app.routers.media import extract_client_ip, router as media_router
from app.routers.stremio import router as stremio_router

__all__ = [
    "configure_router",
    "extract_client_ip",
    "media_router",
    "stremio_router",
]
