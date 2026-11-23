"""Media serving routes: TvVoo Zero-Bandwidth HLS Manifest & Dynamic 889x500 Posters."""

from __future__ import annotations

import asyncio
import logging
import re
from typing import Optional
from urllib.parse import quote, unquote

from fastapi import APIRouter, Query, Request
from fastapi.responses import RedirectResponse, Response

from app.config import Settings
from app.core.database import DatabaseManager
from app.core.network import is_safe_public_ip
from app.core.ram_cache import RAMCacheManager
from app.core.user_config import decode_config
from app.graphics import BANNER_HEIGHT, BANNER_WIDTH, BannerService, render_dark_canvas
from app.providers.daddylive_service import DaddyLiveService
from app.providers.tvvoo_service import TvVooService, is_safe_vavoo_play_url

logger = logging.getLogger(__name__)

router = APIRouter()
_DADDYLIVE_CID_RE = re.compile(r"^[0-9a-zA-Z_-]{1,32}$")


def extract_client_ip(request: Request) -> Optional[str]:
    """Extract the first valid public client IP from proxy headers or socket peer."""
    for header in ("cf-connecting-ip", "true-client-ip", "x-real-ip"):
        raw = (request.headers.get(header) or "").strip()
        if raw and is_safe_public_ip(raw):
            return raw

    xff = request.headers.get("x-forwarded-for") or ""
    if xff:
        for part in xff.split(","):
            candidate = part.strip()
            if candidate and is_safe_public_ip(candidate):
                return candidate

    if request.client and request.client.host:
        host = request.client.host.strip()
        if is_safe_public_ip(host):
            return host

    return None


async def _handle_live_manifest(
    request: Request,
    url: str,
    *,
    config: Optional[str] = None,
) -> Response:
    if not is_safe_vavoo_play_url(url):
        return Response(
            content="Forbidden stream target",
            status_code=403,
            media_type="text/plain",
            headers={"Cache-Control": "no-store"},
        )

    tvvoo: TvVooService = request.app.state.tvvoo
    settings: Settings = request.app.state.settings
    client_ip = extract_client_ip(request)

    try:
        m3u8 = await tvvoo.live_manifest_manager.get_live_manifest(url, client_ip)
        return Response(
            content=m3u8,
            media_type="application/vnd.apple.mpegurl",
            headers={
                "Cache-Control": "no-store, no-cache, must-revalidate",
                "Access-Control-Allow-Origin": "*",
            },
        )
    except Exception as exc:
        logger.warning("TvVoo direct manifest failed for %s (%s), checking EP fallback", url, exc)
        user_cfg = decode_config(config, settings)
        ep_url = (user_cfg.ep_url or "").strip().rstrip("/")
        if ep_url:
            pass_qs = (
                f"&api_password={quote(user_cfg.ep_pass, safe='')}"
                if user_cfg.ep_pass
                else ""
            )
            fallback_url = (
                f"{ep_url}/extractor/video.m3u8"
                f"?host=vavoo&d={quote(url, safe='')}"
                f"&redirect_stream=true{pass_qs}"
            )
            return RedirectResponse(
                url=fallback_url,
                status_code=307,
                headers={
                    "Cache-Control": "no-store",
                    "Access-Control-Allow-Origin": "*",
                },
            )
        return Response(
            content="Live manifest resolution failed",
            status_code=502,
            media_type="text/plain",
            headers={"Cache-Control": "no-store"},
        )


async def _handle_poster(request: Request, match_id: str) -> Response:
    ram_cache: RAMCacheManager = request.app.state.ram_cache
    banner_service: BannerService = request.app.state.banner
    db: DatabaseManager = request.app.state.db

    clean_id = unquote(match_id)
    if clean_id.lower().endswith(".jpg"):
        clean_id = clean_id[:-4]

    match = ram_cache.get_match(clean_id)
    if match is not None:
        jpeg_bytes = await banner_service.get_banner(match, db=db)
    else:
        jpeg_bytes = await asyncio.to_thread(
            render_dark_canvas,
            "StreamSport",
            "Evento Live",
            width=BANNER_WIDTH,
            height=BANNER_HEIGHT,
        )

    return Response(
        content=jpeg_bytes,
        media_type="image/jpeg",
        headers={"Cache-Control": "public, max-age=60"},
    )


def _resolve_daddylive_service(request: Request) -> Optional[DaddyLiveService]:
    direct = getattr(request.app.state, "daddylive", None)
    if isinstance(direct, DaddyLiveService):
        return direct
    orchestrator = getattr(request.app.state, "orchestrator", None)
    providers = getattr(orchestrator, "providers", ()) if orchestrator else ()
    for provider in providers:
        if isinstance(provider, DaddyLiveService):
            return provider
    return None


async def _handle_daddylive_stream(
    request: Request,
    channel_id: str,
    *,
    config: Optional[str] = None,
) -> Response:
    clean_cid = unquote(channel_id).strip()
    if not _DADDYLIVE_CID_RE.match(clean_cid):
        return Response(
            content="ID canale DaddyLive non valido",
            status_code=400,
            media_type="text/plain",
            headers={"Cache-Control": "no-store"},
        )

    settings: Settings = request.app.state.settings
    user_cfg = decode_config(config, settings)
    ep_url = (user_cfg.ep_url or "").strip().rstrip("/")
    if not ep_url:
        return Response(
            content="EasyProxy non configurato per DaddyLive",
            status_code=400,
            media_type="text/plain",
            headers={"Cache-Control": "no-store"},
        )

    daddylive = _resolve_daddylive_service(request)
    if daddylive is None:
        return Response(
            content="Servizio DaddyLive non disponibile",
            status_code=503,
            media_type="text/plain",
            headers={"Cache-Control": "no-store"},
        )

    resolved_url = await daddylive.resolve_working_player_url(
        clean_cid,
        ep_url=ep_url,
        ep_pass=user_cfg.ep_pass,
    )
    if resolved_url:
        return RedirectResponse(
            url=resolved_url,
            status_code=307,
            headers={
                "Cache-Control": "no-store",
                "Access-Control-Allow-Origin": "*",
            },
        )
    return Response(
        content="Nessun player DaddyLive funzionante al momento per questo canale",
        status_code=502,
        media_type="text/plain",
        headers={"Cache-Control": "no-store"},
    )


@router.get("/daddylive/stream/{channel_id}.m3u8")
async def daddylive_stream(request: Request, channel_id: str) -> Response:
    return await _handle_daddylive_stream(request, channel_id)


@router.get("/{config}/daddylive/stream/{channel_id}.m3u8")
async def configured_daddylive_stream(
    request: Request,
    config: str,
    channel_id: str,
) -> Response:
    return await _handle_daddylive_stream(request, channel_id, config=config)


@router.get("/live/manifest.m3u8")
async def live_manifest(
    request: Request,
    url: str = Query(..., min_length=1),
) -> Response:
    return await _handle_live_manifest(request, url)


@router.get("/{config}/live/manifest.m3u8")
async def configured_live_manifest(
    request: Request,
    config: str,
    url: str = Query(..., min_length=1),
) -> Response:
    return await _handle_live_manifest(request, url, config=config)


@router.get("/poster/{match_id}.jpg")
async def poster_jpg(request: Request, match_id: str) -> Response:
    return await _handle_poster(request, match_id)


@router.get("/poster/{match_id}")
async def poster_raw(request: Request, match_id: str) -> Response:
    return await _handle_poster(request, match_id)


@router.get("/{config}/poster/{match_id}.jpg")
async def configured_poster_jpg(request: Request, config: str, match_id: str) -> Response:
    _ = config
    return await _handle_poster(request, match_id)


@router.get("/{config}/poster/{match_id}")
async def configured_poster_raw(request: Request, config: str, match_id: str) -> Response:
    _ = config
    return await _handle_poster(request, match_id)
