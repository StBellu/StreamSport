"""Unified Stremio & Nuvio Catalog/Meta/Stream FastAPI router (100% RAM-served)."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional
from urllib.parse import parse_qs, quote, unquote

from fastapi import APIRouter, Request
from fastapi.responses import ORJSONResponse

from app.config import Settings
from app.core.ram_cache import RAMCacheManager
from app.core.user_config import decode_config
from app.graphics._utils import get_timezone
from app.models.domain import Match
from app.models.stremio import (
    CatalogResponse,
    Manifest,
    ManifestBehaviorHints,
    ManifestCatalog,
    MetaDetail,
    MetaPreview,
    MetaResponse,
    StreamResponse,
)
from app.providers.stream_orchestrator import StreamOrchestrator
from app.routers._utils import compute_base_url
from app.taxonomy import build_nuvio_catalogs, build_stremio_catalogs

router = APIRouter()

_NO_STORE_HEADERS = {"Cache-Control": "no-store"}
_CATALOG_PAGE_LIMIT = 100


def _poster_url(base_url: str, match: Match, config: Optional[str]) -> str:
    prefix = f"/{quote(config.strip('/'), safe='')}" if config and config.strip("/") else ""
    return f"{base_url}{prefix}/poster/{quote(match.id, safe='')}.jpg"


def _format_event_time(match: Match, tz_name: str) -> tuple[str, str]:
    """Return `(hh_mm, full_label)` in configured local timezone."""
    if match.date <= 0:
        return "", ""
    tz = get_timezone(tz_name)
    dt = datetime.fromtimestamp(match.date / 1000.0, tz=timezone.utc).astimezone(tz)
    return dt.strftime("%H:%M"), dt.strftime("%d/%m/%Y %H:%M")


def _build_description(match: Match, tz_name: str) -> str:
    lines: list[str] = []
    comp = match.canonical_competition or match.competition
    if comp:
        lines.append(f"🏆 {comp}")
    if match.stage:
        lines.append(f"📍 {match.stage}")
    _, full_dt = _format_event_time(match, tz_name)
    if full_dt:
        lines.append(f"🕒 {full_dt} ({tz_name})")
    if match.broadcasters:
        lines.append(f"📺 TV: {', '.join(match.broadcasters)}")
    if match.sources:
        lines.append(f"⚡ {len(match.sources)} flussi disponibili")
    return "\n".join(lines) if lines else match.title


def _build_manifest(*, nuvio: bool) -> Manifest:
    if nuvio:
        catalogs = [ManifestCatalog.model_validate(c) for c in build_nuvio_catalogs("tv")]
        return Manifest(
            id="org.streamsport.nuvio",
            version="1.0.0",
            name="StreamSport (Nuvio)",
            description="Aggregatore sportivo live per Nuvio con cataloghi appiattiti.",
            resources=["catalog", "meta", "stream"],
            types=["tv"],
            idPrefixes=["streamsport:"],
            catalogs=catalogs,
            behaviorHints=ManifestBehaviorHints(
                configurable=True,
                configurationRequired=False,
            ),
        )

    catalogs = [ManifestCatalog.model_validate(c) for c in build_stremio_catalogs("tv")]
    return Manifest(
        id="org.streamsport.addon",
        version="1.0.0",
        name="StreamSport",
        description="Aggregatore sportivo live per Stremio (Serie A, Champions, Tennis, Motori, Basket e TV Lineare).",
        resources=["catalog", "meta", "stream"],
        types=["tv"],
        idPrefixes=["streamsport:"],
        catalogs=catalogs,
        behaviorHints=ManifestBehaviorHints(
            configurable=True,
            configurationRequired=False,
        ),
    )


def _parse_catalog_extras(
    request: Request,
    extra: Optional[str],
) -> tuple[Optional[str], Optional[str], int]:
    params: dict[str, str] = {}
    if extra:
        parsed = parse_qs(unquote(extra), keep_blank_values=False)
        for key, vals in parsed.items():
            if vals:
                params[key] = vals[0]

    for key in ("genre", "search", "skip"):
        qp_val = request.query_params.get(key)
        if qp_val is not None and qp_val != "":
            params[key] = qp_val

    genre = params.get("genre")
    search = params.get("search")
    try:
        skip = max(0, int(params.get("skip", "0")))
    except ValueError:
        skip = 0
    return genre, search, skip


def _handle_catalog(
    request: Request,
    media_type: str,
    catalog_id: str,
    *,
    extra: Optional[str] = None,
    config: Optional[str] = None,
) -> ORJSONResponse:
    ram_cache: RAMCacheManager = request.app.state.ram_cache
    settings: Settings = request.app.state.settings
    base_url = compute_base_url(request)

    genre, search, skip = _parse_catalog_extras(request, extra)
    matches = ram_cache.get_catalog(
        catalog_id,
        genre=genre,
        search=search,
        skip=skip,
        limit=_CATALOG_PAGE_LIMIT,
    )

    metas: list[MetaPreview] = []
    for match in matches:
        hhmm, _ = _format_event_time(match, settings.TIMEZONE)
        poster = _poster_url(base_url, match, config)
        metas.append(
            MetaPreview(
                id=match.stremio_id,
                type=media_type,
                name=match.title,
                poster=poster,
                posterShape="landscape",
                description=_build_description(match, settings.TIMEZONE),
                genres=list(match.genres),
                releaseInfo=hhmm or None,
            )
        )

    payload = CatalogResponse(metas=metas).model_dump(exclude_none=True)
    return ORJSONResponse(content=payload, headers=_NO_STORE_HEADERS)


def _handle_meta(
    request: Request,
    media_type: str,
    item_id: str,
    *,
    config: Optional[str] = None,
) -> ORJSONResponse:
    ram_cache: RAMCacheManager = request.app.state.ram_cache
    settings: Settings = request.app.state.settings
    base_url = compute_base_url(request)

    clean_id = unquote(item_id)
    match = ram_cache.get_match(clean_id)
    if match is None:
        return ORJSONResponse(
            content=MetaResponse(meta=None).model_dump(),
            status_code=404,
            headers=_NO_STORE_HEADERS,
        )

    hhmm, _ = _format_event_time(match, settings.TIMEZONE)
    poster = _poster_url(base_url, match, config)
    meta = MetaDetail(
        id=match.stremio_id,
        type=media_type,
        name=match.title,
        poster=poster,
        posterShape="landscape",
        background=poster,
        description=_build_description(match, settings.TIMEZONE),
        genres=list(match.genres),
        releaseInfo=hhmm or None,
        country=match.country or None,
        behaviorHints={"defaultVideoId": match.stremio_id},
    )
    payload = MetaResponse(meta=meta).model_dump(exclude_none=True)
    return ORJSONResponse(content=payload, headers=_NO_STORE_HEADERS)


def _handle_stream(
    request: Request,
    item_id: str,
    *,
    config: Optional[str] = None,
) -> ORJSONResponse:
    ram_cache: RAMCacheManager = request.app.state.ram_cache
    orchestrator: StreamOrchestrator = request.app.state.orchestrator
    settings: Settings = request.app.state.settings
    base_url = compute_base_url(request)

    clean_id = unquote(item_id)
    match = ram_cache.get_match(clean_id)
    if match is None:
        return ORJSONResponse(
            content=StreamResponse(streams=[]).model_dump(exclude_none=True),
            headers=_NO_STORE_HEADERS,
        )

    user_cfg = decode_config(config, settings)
    streams = orchestrator.build_stremio_streams(
        match,
        user_cfg,
        base_url,
        config_segment=config or "",
    )
    payload = StreamResponse(streams=streams).model_dump(exclude_none=True)
    return ORJSONResponse(content=payload, headers=_NO_STORE_HEADERS)


# ------------------------------------------------------------------
# Fixed Stremio & Nuvio routes (registered BEFORE /{config}/...!)
# ------------------------------------------------------------------


@router.get("/manifest.json", response_class=ORJSONResponse)
async def stremio_manifest() -> ORJSONResponse:
    return ORJSONResponse(
        content=_build_manifest(nuvio=False).model_dump(exclude_none=True),
        headers=_NO_STORE_HEADERS,
    )


@router.get("/nuvio/manifest.json", response_class=ORJSONResponse)
async def nuvio_manifest() -> ORJSONResponse:
    return ORJSONResponse(
        content=_build_manifest(nuvio=True).model_dump(exclude_none=True),
        headers=_NO_STORE_HEADERS,
    )


@router.get("/catalog/{media_type}/{catalog_id}/{extra}.json", response_class=ORJSONResponse)
async def stremio_catalog_extra(
    request: Request,
    media_type: str,
    catalog_id: str,
    extra: str,
) -> ORJSONResponse:
    return _handle_catalog(request, media_type, catalog_id, extra=extra)


@router.get("/catalog/{media_type}/{catalog_id}.json", response_class=ORJSONResponse)
async def stremio_catalog(
    request: Request,
    media_type: str,
    catalog_id: str,
) -> ORJSONResponse:
    return _handle_catalog(request, media_type, catalog_id)


@router.get("/nuvio/catalog/{media_type}/{catalog_id}/{extra}.json", response_class=ORJSONResponse)
async def nuvio_catalog_extra(
    request: Request,
    media_type: str,
    catalog_id: str,
    extra: str,
) -> ORJSONResponse:
    return _handle_catalog(request, media_type, catalog_id, extra=extra)


@router.get("/nuvio/catalog/{media_type}/{catalog_id}.json", response_class=ORJSONResponse)
async def nuvio_catalog(
    request: Request,
    media_type: str,
    catalog_id: str,
) -> ORJSONResponse:
    return _handle_catalog(request, media_type, catalog_id)


@router.get("/meta/{media_type}/{item_id}.json", response_class=ORJSONResponse)
async def stremio_meta(
    request: Request,
    media_type: str,
    item_id: str,
) -> ORJSONResponse:
    return _handle_meta(request, media_type, item_id)


@router.get("/nuvio/meta/{media_type}/{item_id}.json", response_class=ORJSONResponse)
async def nuvio_meta(
    request: Request,
    media_type: str,
    item_id: str,
) -> ORJSONResponse:
    return _handle_meta(request, media_type, item_id)


@router.get("/stream/{media_type}/{item_id}.json", response_class=ORJSONResponse)
async def stremio_stream(
    request: Request,
    media_type: str,
    item_id: str,
) -> ORJSONResponse:
    return _handle_stream(request, item_id)


@router.get("/nuvio/stream/{media_type}/{item_id}.json", response_class=ORJSONResponse)
async def nuvio_stream(
    request: Request,
    media_type: str,
    item_id: str,
) -> ORJSONResponse:
    return _handle_stream(request, item_id)


# ------------------------------------------------------------------
# Configured /{config}/... routes
# ------------------------------------------------------------------


@router.get("/{config}/nuvio/manifest.json", response_class=ORJSONResponse)
async def configured_nuvio_manifest(config: str) -> ORJSONResponse:
    _ = config
    return ORJSONResponse(
        content=_build_manifest(nuvio=True).model_dump(exclude_none=True),
        headers=_NO_STORE_HEADERS,
    )


@router.get("/{config}/manifest.json", response_class=ORJSONResponse)
async def configured_stremio_manifest(config: str) -> ORJSONResponse:
    _ = config
    return ORJSONResponse(
        content=_build_manifest(nuvio=False).model_dump(exclude_none=True),
        headers=_NO_STORE_HEADERS,
    )


@router.get("/{config}/nuvio/catalog/{media_type}/{catalog_id}/{extra}.json", response_class=ORJSONResponse)
async def configured_nuvio_catalog_extra(
    request: Request,
    config: str,
    media_type: str,
    catalog_id: str,
    extra: str,
) -> ORJSONResponse:
    return _handle_catalog(request, media_type, catalog_id, extra=extra, config=config)


@router.get("/{config}/nuvio/catalog/{media_type}/{catalog_id}.json", response_class=ORJSONResponse)
async def configured_nuvio_catalog(
    request: Request,
    config: str,
    media_type: str,
    catalog_id: str,
) -> ORJSONResponse:
    return _handle_catalog(request, media_type, catalog_id, config=config)


@router.get("/{config}/catalog/{media_type}/{catalog_id}/{extra}.json", response_class=ORJSONResponse)
async def configured_stremio_catalog_extra(
    request: Request,
    config: str,
    media_type: str,
    catalog_id: str,
    extra: str,
) -> ORJSONResponse:
    return _handle_catalog(request, media_type, catalog_id, extra=extra, config=config)


@router.get("/{config}/catalog/{media_type}/{catalog_id}.json", response_class=ORJSONResponse)
async def configured_stremio_catalog(
    request: Request,
    config: str,
    media_type: str,
    catalog_id: str,
) -> ORJSONResponse:
    return _handle_catalog(request, media_type, catalog_id, config=config)


@router.get("/{config}/nuvio/meta/{media_type}/{item_id}.json", response_class=ORJSONResponse)
async def configured_nuvio_meta(
    request: Request,
    config: str,
    media_type: str,
    item_id: str,
) -> ORJSONResponse:
    return _handle_meta(request, media_type, item_id, config=config)


@router.get("/{config}/meta/{media_type}/{item_id}.json", response_class=ORJSONResponse)
async def configured_stremio_meta(
    request: Request,
    config: str,
    media_type: str,
    item_id: str,
) -> ORJSONResponse:
    return _handle_meta(request, media_type, item_id, config=config)


@router.get("/{config}/nuvio/stream/{media_type}/{item_id}.json", response_class=ORJSONResponse)
async def configured_nuvio_stream(
    request: Request,
    config: str,
    media_type: str,
    item_id: str,
) -> ORJSONResponse:
    return _handle_stream(request, item_id, config=config)


@router.get("/{config}/stream/{media_type}/{item_id}.json", response_class=ORJSONResponse)
async def configured_stremio_stream(
    request: Request,
    config: str,
    media_type: str,
    item_id: str,
) -> ORJSONResponse:
    return _handle_stream(request, item_id, config=config)
