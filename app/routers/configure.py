"""Configuration UI and Base64URL encoding helper routes."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, ORJSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app.config import Settings
from app.core.ram_cache import RAMCacheManager
from app.core.user_config import UserConfig, decode_config, encode_config
from app.routers._utils import compute_base_url

router = APIRouter()

_TEMPLATES_DIR = Path(__file__).resolve().parent.parent / "templates"
_TEMPLATES = Jinja2Templates(directory=str(_TEMPLATES_DIR))


def _build_source_statuses(settings: Settings) -> list[dict[str, object]]:
    return [
        {"name": "Diretta", "enabled": settings.ENABLE_DIRETTA, "kind": "Schedule"},
        {"name": "Sky EPG", "enabled": settings.ENABLE_SKY_EPG, "kind": "EPG"},
        {"name": "Tivùsat", "enabled": settings.ENABLE_TIVUSAT_EPG, "kind": "EPG"},
        {"name": "TheSportsDB", "enabled": settings.ENABLE_THESPORTSDB, "kind": "Posters"},
        {"name": "Tennis Posters", "enabled": settings.ENABLE_TENNIS_POSTERS, "kind": "Posters"},
        {"name": "TvVoo", "enabled": settings.ENABLE_TVVOO, "kind": "Provider"},
        {"name": "MandraKodi", "enabled": settings.ENABLE_MANDRAKODI, "kind": "Provider"},
        {"name": "SPON", "enabled": settings.ENABLE_SPON, "kind": "Provider"},
        {"name": "DaddyLive", "enabled": settings.ENABLE_DADDYLIVE, "kind": "Provider"},
        {"name": "StreamFree", "enabled": settings.ENABLE_STREAMFREE, "kind": "Provider"},
        {"name": "FCTV33", "enabled": settings.ENABLE_FCTV33, "kind": "Provider"},
    ]


def _render_configure(request: Request, config: Optional[str] = None) -> HTMLResponse:
    settings: Settings = request.app.state.settings
    ram_cache: RAMCacheManager = request.app.state.ram_cache
    user_cfg = decode_config(config, settings)
    base_url = compute_base_url(request)

    return _TEMPLATES.TemplateResponse(
        request=request,
        name="configure.html",
        context={
            "base_url": base_url,
            "user_cfg": user_cfg,
            "initial_token": encode_config(user_cfg),
            "source_statuses": _build_source_statuses(settings),
            "active_matches_count": len(ram_cache.snapshot.active_matches),
            "updated_at": ram_cache.snapshot.updated_at,
        },
    )


@router.get("/", include_in_schema=False)
async def root_redirect() -> RedirectResponse:
    return RedirectResponse(url="/configure", status_code=307)


@router.get("/configure", response_class=HTMLResponse)
async def configure_page(request: Request) -> HTMLResponse:
    return _render_configure(request)


@router.get("/{config}/configure", response_class=HTMLResponse)
async def configured_page(request: Request, config: str) -> HTMLResponse:
    return _render_configure(request, config=config)


@router.post("/api/encode-config", response_class=ORJSONResponse)
async def api_encode_config(request: Request, payload: UserConfig) -> ORJSONResponse:
    cfg_token = encode_config(payload)
    base_url = compute_base_url(request)
    manifest_url = f"{base_url}/{cfg_token}/manifest.json"
    stremio_url = (
        manifest_url.replace("https://", "stremio://", 1).replace("http://", "stremio://", 1)
    )
    nuvio_url = f"{base_url}/{cfg_token}/nuvio/manifest.json"
    return ORJSONResponse(
        content={
            "config": cfg_token,
            "manifest_url": manifest_url,
            "stremio_url": stremio_url,
            "nuvio_url": nuvio_url,
        }
    )
