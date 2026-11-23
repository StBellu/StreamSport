"""Shared HTTP request helpers for StreamSport FastAPI routers."""

from __future__ import annotations

import re

from fastapi import Request

from app.config import get_settings

_HOST_REGEX = re.compile(r"^[a-zA-Z0-9.\-_]+(?::\d{1,5})?$")


def compute_base_url(request: Request) -> str:
    """Compute public base URL honoring `PUBLIC_URL` and validated proxy headers."""
    settings = getattr(request.app.state, "settings", None) or get_settings()
    public_url = (getattr(settings, "PUBLIC_URL", "") or "").strip().rstrip("/")
    if public_url:
        return public_url

    raw_proto = (
        (request.headers.get("x-forwarded-proto") or request.url.scheme or "http")
        .split(",")[0]
        .strip()
        .lower()
    )
    proto = "https" if raw_proto == "https" else "http"

    raw_host = (
        request.headers.get("x-forwarded-host")
        or request.headers.get("host")
        or request.url.netloc
        or ""
    )
    candidate_host = raw_host.split(",")[0].strip()
    if candidate_host and _HOST_REGEX.match(candidate_host):
        host = candidate_host
    else:
        host = request.url.netloc

    if host:
        return f"{proto}://{host}".rstrip("/")
    return str(request.base_url).rstrip("/")
