"""Shared HTTP request helpers for StreamSport FastAPI routers."""

from __future__ import annotations

from fastapi import Request


def compute_base_url(request: Request) -> str:
    """Compute public base URL honoring `X-Forwarded-Proto` and `X-Forwarded-Host`."""
    fwd_proto = (request.headers.get("x-forwarded-proto") or "").split(",")[0].strip()
    fwd_host = (request.headers.get("x-forwarded-host") or "").split(",")[0].strip()
    if fwd_host:
        proto = fwd_proto or request.url.scheme or "http"
        return f"{proto}://{fwd_host}".rstrip("/")
    if fwd_proto:
        host = request.headers.get("host") or request.url.netloc
        if host:
            return f"{fwd_proto}://{host}".rstrip("/")
    return str(request.base_url).rstrip("/")
