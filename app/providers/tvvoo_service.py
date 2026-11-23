"""TvVoo / Vavoo primary linear stream provider and zero-bandwidth LiveManifestManager."""

from __future__ import annotations

import asyncio
import base64
from collections import OrderedDict
import ipaddress
import json
import logging
import re
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Optional
from urllib.parse import parse_qs, quote, urljoin, urlparse, urlsplit

import httpx

from app.config import Settings
from app.core.network import create_standard_client, is_safe_public_ip
from app.models.domain import StreamSource
from app.providers.base import BaseStreamProvider, ProviderHarvestResult
from app.taxonomy.channels import (
    TVVOO_COUNTRY_GROUPS,
    canonicalize_channel_name,
    server_tag_rank,
)

logger = logging.getLogger(__name__)

PING_URLS: tuple[str, ...] = (
    "https://www.vypn.net/api/app/ping",
    "https://www.vavoo.tv/api/app/ping",
)

GEIL_MIRROR_BASES: tuple[str, ...] = (
    "https://geil.app",
    "https://geil.win",
    "https://geil.to",
)

# vavoo.to rejects direct connections; use active MediaHubMX mirror bases.
VAVOO_MIRROR_BASES: tuple[str, ...] = (
    "https://voltop.net",
    "https://vavoo.net",
    "https://huhu.to",
    "https://oha.to",
    "https://kool.to",
    "https://kool.ws",
)

# geil.to exposes only the v2 /live/resolve endpoint.
VAVOO_V2_BASES: tuple[str, ...] = (
    "https://voltop.net",
    "https://vavoo.net",
    "https://geil.to",
    "https://kool.to",
    "https://kool.ws",
)

VAVOO_GROUPS: tuple[str, ...] = (
    "Italy",
    "United Kingdom",
    "Germany",
    "France",
    "Spain",
    "Portugal",
    "Netherlands",
    "Albania",
    "Balkans",
    "Turkey",
)

_GROUP_ALIASES: dict[str, tuple[str, ...]] = {
    "Netherlands": ("Netherlands", "Nederland", "Holland"),
}

_GROUP_LANGUAGE_MAP: dict[str, str] = {
    "Italy": "ITA",
    "United Kingdom": "ENG",
    "Germany": "DEU",
    "France": "FRA",
    "Spain": "SPA",
    "Portugal": "POR",
    "Netherlands": "NLD",
    "Albania": "ALB",
    "Balkans": "BLK",
    "Turkey": "TUR",
}

VAVOO_PING_UA = "okhttp/4.11.0"
VAVOO_RESOLVE_UA = "MediaHubMX/2"
VAVOO_API_UA = "okhttp/4.11.0"
VAVOO_PLAYLIST_UA = "VAVOO/2.6"

_VYPN_PACKAGE = "net.vypn.app"
_VYPN_VERSION = "1.4.1"

_V2_LINK_RE = re.compile(r"/(?:live/play|live2/play3)/([^/?#]+)", re.IGNORECASE)
_PLAY_ID_RE = re.compile(r"/play/([^/?#]+)", re.IGNORECASE)
_M3U8_ATTR_URI_RE = re.compile(r'URI=(["\'])([^"\']+)\1', re.IGNORECASE)

_COUNTRY_PREFIX_RE = re.compile(r"^(?:it|ita|uk|de|fr|es|pt|nl|al|tr|bk)\s*[:|\-]\s*", re.IGNORECASE)
_TRAILING_DOT_TAG_RE = re.compile(r"(?:\s*\.[a-z0-9]{1,3})+$", re.IGNORECASE)
_TRAILING_PAREN_TAG_RE = re.compile(r"\s*[\(\[]\s*(?:\d+|[a-z]{1,4})\s*[\)\]]\s*$", re.IGNORECASE)
_QUALITY_TOKEN_RE = re.compile(r"\b(?:fhd|uhd|hd|sd|1080p?|720p?|hevc|h\.?265)\b", re.IGNORECASE)
_MULTI_SPACE_RE = re.compile(r"\s+")

_DEFAULT_VALID_SECONDS = 20 * 60.0
_PROACTIVE_REFRESH_SECONDS = 5 * 60.0
_THROTTLE_SECONDS = 1.5
_STALE_FALLBACK_SECONDS = 10.0
_IDLE_TTL_SECONDS = 45 * 60.0
_MAX_MANIFEST_BYTES = 2 * 1024 * 1024  # 2 MiB guard: text playlists only

_ALLOWED_PLAY_HOST_PREFIXES: tuple[str, ...] = (
    "geil.",
    "vavoo.",
    "vypn.",
    "voltop.",
    "huhu.",
    "oha.",
    "kool.",
)
_ALLOWED_PLAY_PATH_PREFIXES: tuple[str, ...] = (
    "/play",
    "/live/play",
    "/live2/play3",
    "/vavoo-iptv/play",
)
_BLOCKED_HOSTNAMES: frozenset[str] = frozenset({
    "localhost",
    "0.0.0.0",
    "metadata.google.internal",
})
_BLOCKED_HOST_SUFFIXES: tuple[str, ...] = (
    ".localhost",
    ".local",
    ".internal",
    ".lan",
)


def is_safe_vavoo_play_url(url: str) -> bool:
    """Validate that a play URL is an authorized Vavoo/Geil stream endpoint (anti-SSRF)."""
    if not url or not isinstance(url, str):
        return False
    try:
        parsed = urlsplit(url.strip())
    except Exception:
        return False

    if (parsed.scheme or "").lower() not in ("http", "https"):
        return False

    hostname = (parsed.hostname or "").lower().strip().strip("[]")
    if not hostname:
        return False
    if hostname in _BLOCKED_HOSTNAMES or hostname.endswith(_BLOCKED_HOST_SUFFIXES):
        return False
    try:
        ipaddress.ip_address(hostname)
        if not is_safe_public_ip(hostname):
            return False
        # IP literals are not in the authorized Vavoo/Geil host allowlist
        return False
    except ValueError:
        pass

    is_allowed_host = any(
        hostname.startswith(prefix) or f".{prefix}" in hostname
        for prefix in _ALLOWED_PLAY_HOST_PREFIXES
    )
    if not is_allowed_host:
        return False

    path = parsed.path or ""
    return any(path.startswith(prefix) for prefix in _ALLOWED_PLAY_PATH_PREFIXES)


def is_safe_stream_target(url: str) -> bool:
    """Validate that an upstream CDN stream URL does not target private/internal networks."""
    if not url or not isinstance(url, str):
        return False
    try:
        parsed = urlsplit(url.strip())
    except Exception:
        return False

    if (parsed.scheme or "").lower() not in ("http", "https"):
        return False

    hostname = (parsed.hostname or "").lower().strip().strip("[]")
    if not hostname:
        return False
    if hostname in _BLOCKED_HOSTNAMES or hostname.endswith(_BLOCKED_HOST_SUFFIXES):
        return False

    try:
        ipaddress.ip_address(hostname)
        return is_safe_public_ip(hostname)
    except ValueError:
        return True


def normalize_channel_name(name: str, country: Optional[str] = None) -> str:
    """Normalize a Vavoo/TV channel title via shared canonical taxonomy."""
    return canonicalize_channel_name(name, country=country)


def _detect_quality(raw_name: str, is_italian: bool) -> str:
    upper = raw_name.upper()
    if "4K" in upper or "UHD" in upper:
        return "4K"
    if "FHD" in upper or "1080" in upper:
        return "FHD"
    if "SD" in upper:
        return "SD"
    if "HD" in upper:
        return "FHD" if is_italian else "HD"
    return "FHD" if is_italian else "HD"


def _decode_sig_payload(sig: str) -> tuple[Optional[dict[str, Any]], Optional[dict[str, Any]], bool]:
    """Decode base64 `addonSig` JSON and its inner `data` object."""
    padded = sig.strip() + "=" * (-len(sig.strip()) % 4)
    decoded = base64.b64decode(padded).decode("utf-8")
    sig_obj = json.loads(decoded)
    if not isinstance(sig_obj, dict) or "data" not in sig_obj:
        return None, None, False

    raw_data = sig_obj["data"]
    data_was_str = isinstance(raw_data, str)
    if data_was_str:
        data_obj = json.loads(raw_data)
    elif isinstance(raw_data, dict):
        data_obj = dict(raw_data)
    else:
        return None, None, False

    if not isinstance(data_obj, dict):
        return None, None, False
    return sig_obj, data_obj, data_was_str


def rewrite_addon_sig_ip(sig: str, client_ip: Optional[str]) -> str:
    """Decode base64 `addonSig` and rewrite `data.ips` / `data.ip` to prioritize `client_ip`."""
    if not sig or not client_ip:
        return sig
    try:
        sig_obj, data_obj, data_was_str = _decode_sig_payload(sig)
        if sig_obj is None or data_obj is None:
            return sig

        existing_ips = data_obj.get("ips")
        if not isinstance(existing_ips, list):
            existing_ips = []
        data_obj["ips"] = [client_ip] + [ip for ip in existing_ips if ip and ip != client_ip]
        if "ip" in data_obj and isinstance(data_obj["ip"], str):
            data_obj["ip"] = client_ip

        sig_obj["data"] = json.dumps(data_obj) if data_was_str else data_obj
        encoded = json.dumps(sig_obj).encode("utf-8")
        return base64.b64encode(encoded).decode("ascii")
    except Exception as exc:
        logger.warning("TvVoo addonSig IP rewrite failed, keeping original sig: %s", exc)
        return sig


def extract_addon_sig_valid_until(sig: str) -> Optional[float]:
    """Extract `validUntil` from a base64 `addonSig` as UTC epoch seconds."""
    if not sig:
        return None
    try:
        _, data_obj, _ = _decode_sig_payload(sig)
        if not data_obj:
            return None
        raw_val = data_obj.get("validUntil")
        if raw_val is None:
            return None
        val = float(raw_val)
        # Vavoo timestamps may be epoch milliseconds
        if val > 1e11:
            val /= 1000.0
        return val
    except Exception:
        return None


def rewrite_hls_manifest(raw_m3u8: str, base_url: str) -> str:
    """Rewrite all relative segment/key/map URIs in an HLS playlist to absolute CDN URLs."""
    if not raw_m3u8:
        return raw_m3u8

    def _replace_attr_uri(match: re.Match[str]) -> str:
        quote_char = match.group(1)
        uri = match.group(2).strip()
        if not uri or uri.startswith(("http://", "https://", "data:")):
            return match.group(0)
        abs_uri = urljoin(base_url, uri)
        return f"URI={quote_char}{abs_uri}{quote_char}"

    out_lines: list[str] = []
    for line in raw_m3u8.splitlines():
        trimmed = line.strip()
        if not trimmed:
            out_lines.append(line)
            continue
        if trimmed.startswith("#"):
            if "URI=" in line or "uri=" in line:
                out_lines.append(_M3U8_ATTR_URI_RE.sub(_replace_attr_uri, line))
            else:
                out_lines.append(line)
            continue
        if trimmed.startswith(("http://", "https://")):
            out_lines.append(line)
        else:
            out_lines.append(urljoin(base_url, trimmed))
    return "\n".join(out_lines)


def _normalize_mediahubmx_play_url(play_url: str) -> str:
    if "/watch" in play_url:
        params = parse_qs(urlparse(play_url).query)
        live_id = (params.get("live") or [None])[0]
        if live_id:
            return f"https://vavoo.to/vavoo-iptv/play/{live_id}"

    match = _PLAY_ID_RE.search(play_url)
    if match and "/vavoo-iptv/play/" not in play_url:
        return f"https://vavoo.to/vavoo-iptv/play/{match.group(1)}"
    return play_url


def _extract_resolved_url(payload: Any) -> Optional[str]:
    if isinstance(payload, list) and payload:
        first = payload[0]
        if isinstance(first, dict) and first.get("url"):
            return str(first["url"])
    elif isinstance(payload, dict):
        if payload.get("url"):
            return str(payload["url"])
        nested = payload.get("data")
        if isinstance(nested, dict) and nested.get("url"):
            return str(nested["url"])
    return None


@dataclass(slots=True)
class _LiveStreamSession:
    play_url: str
    client_ip: Optional[str]
    stream_url: str
    base_url: str
    valid_until: float
    last_fetched_at: float
    last_manifest: Optional[str] = None
    last_manifest_at: float = 0.0


class LiveManifestManager:
    """Thread-safe / asyncio single-flight HLS manifest manager with zero video bandwidth."""

    MAX_SESSIONS: int = 1000

    def __init__(
        self,
        *,
        client: Optional[httpx.AsyncClient] = None,
        ping_urls: tuple[str, ...] = PING_URLS,
        geil_bases: tuple[str, ...] = GEIL_MIRROR_BASES,
        mirror_bases: tuple[str, ...] = VAVOO_MIRROR_BASES,
        v2_bases: tuple[str, ...] = VAVOO_V2_BASES,
        playlist_timeout: float = 4.0,
        resolve_timeout: float = 10.0,
        idle_ttl_seconds: float = _IDLE_TTL_SECONDS,
        proactive_refresh_seconds: float = _PROACTIVE_REFRESH_SECONDS,
        throttle_seconds: float = _THROTTLE_SECONDS,
        max_sessions: int = MAX_SESSIONS,
        sibling_lookup: Optional[Callable[[str], list[str]]] = None,
    ) -> None:
        self._external_client = client
        self._owned_client: Optional[httpx.AsyncClient] = None
        self._ping_urls = ping_urls
        self._geil_bases = geil_bases
        self._mirror_bases = mirror_bases
        self._v2_bases = v2_bases
        self._playlist_timeout = playlist_timeout
        self._resolve_timeout = resolve_timeout
        self._idle_ttl_seconds = idle_ttl_seconds
        self._proactive_refresh_seconds = proactive_refresh_seconds
        self._throttle_seconds = throttle_seconds
        self._max_sessions = max(1, max_sessions)
        self._sibling_lookup = sibling_lookup

        self._thread_lock = threading.RLock()
        self._sessions: OrderedDict[tuple[str, str], _LiveStreamSession] = OrderedDict()
        self._inflight: dict[tuple[str, str], asyncio.Task[_LiveStreamSession]] = {}
        self._background_tasks: set[asyncio.Task[Any]] = set()
        self._mirror_index = 0
        self._last_cleanup_at = time.time()

    def set_sibling_lookup(self, lookup: Optional[Callable[[str], list[str]]]) -> None:
        """Register a callback returning sibling play URLs for failover."""
        self._sibling_lookup = lookup

    def _get_client(self) -> httpx.AsyncClient:
        if self._external_client is not None:
            return self._external_client
        if self._owned_client is None or self._owned_client.is_closed:
            self._owned_client = create_standard_client(
                timeout=max(self._resolve_timeout, self._playlist_timeout),
                verify=False,
            )
        return self._owned_client

    async def aclose(self) -> None:
        """Close owned HTTP client and cancel pending background refresh tasks."""
        with self._thread_lock:
            tasks = list(self._background_tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        if self._owned_client is not None and not self._owned_client.is_closed:
            await self._owned_client.aclose()
            self._owned_client = None

    @staticmethod
    def _session_key(play_url: str, client_ip: Optional[str]) -> tuple[str, str]:
        return (play_url, client_ip or "default")

    def _next_mirror_order(self, bases: tuple[str, ...]) -> list[str]:
        if not bases:
            return []
        with self._thread_lock:
            start = self._mirror_index % len(bases)
            self._mirror_index = (self._mirror_index + 1) % len(bases)
        return list(bases[start:]) + list(bases[:start])

    def cleanup_idle_sessions(self, now: Optional[float] = None) -> int:
        """Evict sessions idle for longer than `idle_ttl_seconds` (default 45 min)."""
        current = time.time() if now is None else now
        removed = 0
        with self._thread_lock:
            stale_keys = [
                k
                for k, sess in self._sessions.items()
                if (current - sess.last_fetched_at) > self._idle_ttl_seconds
            ]
            for key in stale_keys:
                self._sessions.pop(key, None)
                removed += 1
            self._last_cleanup_at = current
        return removed

    async def fetch_addon_sig(self, client_ip: Optional[str] = None) -> Optional[str]:
        """Fetch and IP-rewrite an `addonSig` from vypn.net (fallback vavoo.tv)."""
        client = self._get_client()
        now_ms = int(time.time() * 1000)
        unique_id = base64.b16encode(f"{now_ms:016x}".encode("ascii")).decode("ascii")[:16].lower()

        base_payload: dict[str, Any] = {
            "token": "",
            "reason": "app-focus",
            "locale": "de",
            "theme": "dark",
            "metadata": {
                "device": {"type": "phone", "uniqueId": unique_id},
                "os": {"name": "android", "version": "14", "abis": ["arm64-v8a"], "host": "android"},
                "app": {"platform": "android"},
                "version": {"package": _VYPN_PACKAGE, "binary": _VYPN_VERSION, "js": _VYPN_VERSION},
            },
            "appFocusTime": 0,
            "playerActive": False,
            "playDuration": 0,
            "devMode": False,
            "hasAddon": True,
            "castConnected": False,
            "package": _VYPN_PACKAGE,
            "version": _VYPN_VERSION,
            "process": "app",
            "firstAppStart": now_ms - 86_400_000,
            "lastAppStart": now_ms,
            "ipLocation": client_ip or "",
            "adblockEnabled": True,
            "migrationApplied": False,
            "migrationTargetInstalled": False,
            "proxy": {
                "supported": ["ss"],
                "engine": "Mu",
                "ssVersion": "2022",
                "enabled": False,
                "autoServer": True,
                "id": "",
            },
            "iap": {"supported": False, "error": ""},
        }
        base_headers = {
            "User-Agent": VAVOO_PING_UA,
            "Accept": "application/json",
            "Content-Type": "application/json; charset=utf-8",
            "Accept-Language": "de",
        }

        for ping_url in self._ping_urls:
            attempts: list[tuple[dict[str, Any], dict[str, str]]] = []
            if client_ip:
                fwd_headers = {
                    **base_headers,
                    "X-Forwarded-For": client_ip,
                    "X-Real-IP": client_ip,
                }
                attempts.append((base_payload, fwd_headers))
                attempts.append(({**base_payload, "ipLocation": ""}, base_headers))
            else:
                attempts.append((base_payload, base_headers))

            for payload, headers in attempts:
                try:
                    resp = await client.post(
                        ping_url,
                        json=payload,
                        headers=headers,
                        timeout=self._resolve_timeout,
                    )
                    if resp.status_code != 200:
                        logger.warning("TvVoo ping %s returned HTTP %s", ping_url, resp.status_code)
                        continue
                    data = resp.json()
                    raw_sig = data.get("addonSig") or data.get("mhub")
                    if raw_sig:
                        return rewrite_addon_sig_ip(str(raw_sig), client_ip)
                except Exception as exc:
                    logger.warning("TvVoo ping %s failed: %s", ping_url, exc)
        return None

    async def _resolve_via_geil(self, play_url: str) -> Optional[str]:
        """Try fast tokenless Geil Resolver (`GET {geil_base}/live/resolve?url=...`)."""
        client = self._get_client()
        for base in self._geil_bases:
            try:
                resp = await client.get(
                    f"{base}/live/resolve",
                    params={"url": play_url},
                    headers={"User-Agent": VAVOO_RESOLVE_UA, "Accept": "application/json"},
                    timeout=self._resolve_timeout,
                )
                if resp.status_code != 200:
                    logger.debug("TvVoo Geil resolve %s returned HTTP %s", base, resp.status_code)
                    continue
                data = resp.json()
                if isinstance(data, dict):
                    resolved_url = str(data.get("url") or "").strip()
                    if resolved_url.startswith(("http://", "https://")):
                        return resolved_url
            except Exception as exc:
                logger.debug("TvVoo Geil resolve failed on %s: %s", base, exc)
        return None

    async def _resolve_via_v2(self, play_url: str) -> Optional[str]:
        match = _V2_LINK_RE.search(play_url)
        if not match:
            return None
        channel_id = match.group(1)
        client = self._get_client()

        for base in self._next_mirror_order(self._v2_bases):
            target_play = f"{base}/live/play/{channel_id}"
            try:
                resp = await client.get(
                    f"{base}/live/resolve",
                    params={"url": target_play},
                    headers={"User-Agent": VAVOO_RESOLVE_UA, "Accept": "application/json"},
                    timeout=self._resolve_timeout,
                )
                if resp.status_code != 200:
                    logger.debug("TvVoo v2 resolve %s returned HTTP %s", base, resp.status_code)
                    continue
                data = resp.json()
                if isinstance(data, dict) and data.get("url"):
                    return str(data["url"])
            except Exception as exc:
                logger.debug("TvVoo v2 resolve failed on %s: %s", base, exc)
        return None

    async def resolve_play_url(
        self,
        play_url: str,
        client_ip: Optional[str] = None,
    ) -> tuple[str, float]:
        """Resolve a Vavoo play URL via Geil first, then v2 / MediaHubMX fallback."""
        if not is_safe_vavoo_play_url(play_url):
            raise ValueError(f"Unsafe or unauthorized Vavoo play URL rejected: {play_url}")

        resolved_geil = await self._resolve_via_geil(play_url)
        if resolved_geil and is_safe_stream_target(resolved_geil):
            return resolved_geil, time.time() + _DEFAULT_VALID_SECONDS

        if _V2_LINK_RE.search(play_url):
            resolved_v2 = await self._resolve_via_v2(play_url)
            if resolved_v2 and is_safe_stream_target(resolved_v2):
                return resolved_v2, time.time() + _DEFAULT_VALID_SECONDS

        canonical_url = _normalize_mediahubmx_play_url(play_url)
        sig = await self.fetch_addon_sig(client_ip)
        valid_until = (
            extract_addon_sig_valid_until(sig) if sig else None
        ) or (time.time() + _DEFAULT_VALID_SECONDS)

        headers = {
            "User-Agent": VAVOO_RESOLVE_UA,
            "Accept": "application/json",
            "Content-Type": "application/json; charset=utf-8",
            "Origin": "https://vavoo.to",
            "Referer": "https://vavoo.to/",
        }
        if sig:
            headers["mediahubmx-signature"] = sig
        if client_ip:
            headers["X-Forwarded-For"] = client_ip
            headers["X-Real-IP"] = client_ip

        body = {
            "language": "de",
            "region": "AT",
            "url": canonical_url,
            "clientVersion": "3.1.0",
        }
        client = self._get_client()

        for base in self._next_mirror_order(self._mirror_bases):
            endpoint = f"{base}/mediahubmx-resolve.json"
            try:
                resp = await client.post(
                    endpoint,
                    json=body,
                    headers=headers,
                    timeout=self._resolve_timeout,
                )
                if resp.status_code in (451, 500, 502, 503, 504):
                    logger.warning("TvVoo resolve %s returned HTTP %s, rotating mirror", base, resp.status_code)
                    continue
                if resp.status_code != 200:
                    logger.warning("TvVoo resolve %s returned HTTP %s", base, resp.status_code)
                    continue
                resolved = _extract_resolved_url(resp.json())
                if resolved:
                    if not is_safe_stream_target(resolved):
                        raise ValueError(f"Unsafe resolved stream target rejected: {resolved}")
                    return resolved, valid_until
            except ValueError:
                raise
            except Exception as exc:
                logger.warning("TvVoo resolve failed on %s: %s", base, exc)

        raise RuntimeError(f"Failed to resolve Vavoo play URL across all mirrors: {play_url}")

    async def _refresh_session_impl(
        self,
        key: tuple[str, str],
        play_url: str,
        client_ip: Optional[str],
    ) -> _LiveStreamSession:
        current_task = asyncio.current_task()
        try:
            stream_url, valid_until = await self.resolve_play_url(play_url, client_ip)
            base_url = stream_url.rsplit("/", 1)[0] + "/" if "/" in stream_url else stream_url
            now = time.time()
            with self._thread_lock:
                prev = self._sessions.get(key)
                session = _LiveStreamSession(
                    play_url=play_url,
                    client_ip=client_ip,
                    stream_url=stream_url,
                    base_url=base_url,
                    valid_until=valid_until,
                    last_fetched_at=now,
                    last_manifest=prev.last_manifest if prev else None,
                    last_manifest_at=prev.last_manifest_at if prev else 0.0,
                )
                self._sessions[key] = session
                self._sessions.move_to_end(key)
                while len(self._sessions) > self._max_sessions:
                    self._sessions.popitem(last=False)
                return session
        finally:
            with self._thread_lock:
                if self._inflight.get(key) is current_task:
                    self._inflight.pop(key, None)

    def _get_or_start_refresh(
        self,
        key: tuple[str, str],
        play_url: str,
        client_ip: Optional[str],
    ) -> tuple[asyncio.Task[_LiveStreamSession], bool]:
        with self._thread_lock:
            existing = self._inflight.get(key)
            if existing is not None and not existing.done():
                return existing, False
            task = asyncio.create_task(self._refresh_session_impl(key, play_url, client_ip))
            self._inflight[key] = task
            self._background_tasks.add(task)

            def _on_done(done_task: asyncio.Task[_LiveStreamSession]) -> None:
                with self._thread_lock:
                    self._background_tasks.discard(done_task)
                    if self._inflight.get(key) is done_task:
                        self._inflight.pop(key, None)
                if not done_task.cancelled():
                    exc = done_task.exception()
                    if exc is not None:
                        logger.debug("LiveManifest background refresh error for %s: %s", play_url, exc)

            task.add_done_callback(_on_done)
            return task, True

    async def _fetch_playlist_text(self, stream_url: str) -> str:
        if not is_safe_stream_target(stream_url):
            raise ValueError(f"Unsafe upstream stream target rejected: {stream_url}")
        client = self._get_client()
        resp = await client.get(
            stream_url,
            headers={"User-Agent": VAVOO_PLAYLIST_UA, "Accept": "*/*"},
            timeout=self._playlist_timeout,
        )
        if resp.status_code in (401, 403):
            raise PermissionError(f"Upstream playlist auth error: HTTP {resp.status_code}")
        resp.raise_for_status()

        content_type = (resp.headers.get("content-type") or "").lower()
        if content_type.startswith(("video/", "audio/")) and "mpegurl" not in content_type:
            raise ValueError(f"Refusing to proxy binary media stream ({content_type})")
        if len(resp.content) > _MAX_MANIFEST_BYTES:
            raise ValueError(f"Manifest exceeds maximum size ({len(resp.content)} bytes)")
        return resp.text

    def get_session_snapshot(
        self, play_url: str, client_ip: Optional[str] = None
    ) -> Optional[_LiveStreamSession]:
        """Return the cached session object for inspection/testing."""
        with self._thread_lock:
            key = self._session_key(play_url, client_ip)
            session = self._sessions.get(key)
            if session is not None:
                self._sessions.move_to_end(key)
            return session

    async def _try_sibling_failover(
        self,
        play_url: str,
        client_ip: Optional[str],
        session: _LiveStreamSession,
    ) -> Optional[str]:
        if self._sibling_lookup is None:
            return None
        for sib_url in self._sibling_lookup(play_url):
            if not sib_url or sib_url == play_url or not is_safe_vavoo_play_url(sib_url):
                continue
            try:
                sib_stream_url, sib_valid_until = await self.resolve_play_url(sib_url, client_ip)
                raw_m3u8 = await self._fetch_playlist_text(sib_stream_url)
                sib_base = sib_stream_url.rsplit("/", 1)[0] + "/" if "/" in sib_stream_url else sib_stream_url
                with self._thread_lock:
                    session.stream_url = sib_stream_url
                    session.base_url = sib_base
                    session.valid_until = sib_valid_until
                return raw_m3u8
            except Exception as exc:
                logger.debug("LiveManifest sibling failover failed for %s: %s", sib_url, exc)
        return None

    async def get_live_manifest(
        self,
        play_url: str,
        client_ip: Optional[str] = None,
    ) -> str:
        """Fetch, rewrite relative URIs to CDN URLs, and cache an HLS playlist (0% video bandwidth)."""
        if not is_safe_vavoo_play_url(play_url):
            raise ValueError(f"Unsafe or unauthorized Vavoo play URL rejected: {play_url}")

        now = time.time()
        with self._thread_lock:
            if (now - self._last_cleanup_at) > 600.0:
                self.cleanup_idle_sessions(now)
            key = self._session_key(play_url, client_ip)
            session = self._sessions.get(key)
            if session is not None:
                self._sessions.move_to_end(key)

        if session is None or session.valid_until <= now:
            task, _ = self._get_or_start_refresh(key, play_url, client_ip)
            session = await task
        elif (session.valid_until - now) < self._proactive_refresh_seconds:
            # Non-blocking background token renewal when < 5 min remain
            self._get_or_start_refresh(key, play_url, client_ip)

        now = time.time()
        with self._thread_lock:
            session.last_fetched_at = now
            if (
                session.last_manifest is not None
                and (now - session.last_manifest_at) < self._throttle_seconds
            ):
                return session.last_manifest

        try:
            raw_m3u8 = await self._fetch_playlist_text(session.stream_url)
        except Exception as exc:
            logger.warning("LiveManifest upstream fetch failed for %s: %s", play_url, exc)
            if (
                session.last_manifest is not None
                and (now - session.last_manifest_at) < _STALE_FALLBACK_SECONDS
            ):
                if isinstance(exc, PermissionError):
                    self._get_or_start_refresh(key, play_url, client_ip)
                return session.last_manifest

            task, _ = self._get_or_start_refresh(key, play_url, client_ip)
            session = await task
            try:
                raw_m3u8 = await self._fetch_playlist_text(session.stream_url)
            except Exception:
                sib_m3u8 = await self._try_sibling_failover(play_url, client_ip, session)
                if sib_m3u8 is None:
                    raise
                raw_m3u8 = sib_m3u8

        rewritten = rewrite_hls_manifest(raw_m3u8, session.base_url)
        now_done = time.time()
        with self._thread_lock:
            session.last_manifest = rewritten
            session.last_manifest_at = now_done
            session.last_fetched_at = now_done
        return rewritten


def parse_m3u_vavoo_channels(m3u_text: str) -> list[tuple[str, str]]:
    """Parse Vavoo play entries from an M3U playlist text."""
    entries: list[tuple[str, str]] = []
    pending_name: Optional[str] = None
    for raw_line in m3u_text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("#EXTINF:"):
            pending_name = line.rsplit(",", 1)[-1].strip() if "," in line else ""
            continue
        if line.startswith("#"):
            continue
        if pending_name is not None:
            lower = line.lower()
            if (
                "vavoo" in lower
                or "/vavoo-iptv/play/" in lower
                or "/live2/play3/" in lower
                or "/live/play/" in lower
            ):
                entries.append((pending_name, line))
            pending_name = None
    return entries


class TvVooService(BaseStreamProvider):
    """Primary linear TV provider powered by MediaHubMX / Vavoo with No-Freeze manifest mode."""

    name: str = "tvvoo"
    priority: int = 1

    def __init__(
        self,
        settings: Optional[Settings] = None,
        *,
        client: Optional[httpx.AsyncClient] = None,
        live_manifest_manager: Optional[LiveManifestManager] = None,
        m3u_fallback_path: Optional[Path] = None,
    ) -> None:
        super().__init__(settings)
        self._external_client = client
        self._owned_client: Optional[httpx.AsyncClient] = None
        self.live_manifest_manager = live_manifest_manager or LiveManifestManager(client=client)
        self._m3u_fallback_path = m3u_fallback_path
        self._siblings_by_url: dict[str, list[str]] = {}
        self.live_manifest_manager.set_sibling_lookup(self._lookup_siblings)

    def _lookup_siblings(self, play_url: str) -> list[str]:
        return list(self._siblings_by_url.get(play_url, ()))

    @property
    def enabled(self) -> bool:
        return bool(self.settings.ENABLE_TVVOO)

    def _get_client(self) -> httpx.AsyncClient:
        if self._external_client is not None:
            return self._external_client
        if self._owned_client is None or self._owned_client.is_closed:
            self._owned_client = create_standard_client(timeout=15.0, verify=False)
        return self._owned_client

    async def aclose(self) -> None:
        await self.live_manifest_manager.aclose()
        if self._owned_client is not None and not self._owned_client.is_closed:
            await self._owned_client.aclose()
            self._owned_client = None

    async def _fetch_geil_catalog(self) -> dict[str, list[dict[str, Any]]]:
        """Try fast tokenless Geil catalog (`/live/catalog/live/channels.json`) paginated via `nextCursor`."""
        client = self._get_client()
        headers = {
            "User-Agent": VAVOO_RESOLVE_UA,
            "Accept": "application/json",
        }
        tag_to_group: dict[str, str] = {}
        for grp in VAVOO_GROUPS:
            for alias in _GROUP_ALIASES.get(grp, (grp,)):
                tag_to_group[alias.lower()] = grp
        for grp in TVVOO_COUNTRY_GROUPS:
            tag_to_group.setdefault(grp.lower(), grp)

        for base in GEIL_MIRROR_BASES:
            grouped: dict[str, list[dict[str, Any]]] = {}
            cursor: Any = None
            failed = False
            for _ in range(40):
                params = {"cursor": str(cursor)} if cursor else None
                try:
                    resp = await client.get(
                        f"{base}/live/catalog/live/channels.json",
                        params=params,
                        headers=headers,
                        timeout=12.0,
                    )
                    if resp.status_code != 200:
                        failed = True
                        break
                    payload = resp.json()
                    page_items = payload.get("items") or []
                    if not isinstance(page_items, list) or not page_items:
                        break
                    for item in page_items:
                        if not isinstance(item, dict):
                            continue
                        raw_tags = item.get("tags") or []
                        matched_group: Optional[str] = None
                        for tag in raw_tags:
                            grp = tag_to_group.get(str(tag).strip().lower())
                            if grp is not None:
                                matched_group = grp
                                break
                        if matched_group is not None:
                            grouped.setdefault(matched_group, []).append(item)
                    cursor = payload.get("nextCursor")
                    if not cursor:
                        break
                except Exception as exc:
                    logger.debug("TvVoo Geil catalog failed on %s: %s", base, exc)
                    failed = True
                    break
            if not failed and grouped:
                return grouped
        return {}

    async def _fetch_catalog_group(self, group: str, sig: str) -> list[dict[str, Any]]:
        client = self._get_client()
        headers = {
            "User-Agent": VAVOO_API_UA,
            "Accept": "application/json",
            "Content-Type": "application/json; charset=utf-8",
            "mediahubmx-signature": sig,
        }
        group_candidates = _GROUP_ALIASES.get(group, (group,))
        for group_name in group_candidates:
            for base in VAVOO_MIRROR_BASES:
                items: list[dict[str, Any]] = []
                cursor: Any = 0
                failed = False
                for _ in range(50):
                    body = {
                        "language": "de",
                        "region": "AT",
                        "catalogId": "iptv",
                        "id": "iptv",
                        "adult": False,
                        "search": "",
                        "sort": "name",
                        "filter": {"group": group_name},
                        "cursor": cursor,
                        "clientVersion": "3.1.0",
                    }
                    try:
                        resp = await client.post(
                            f"{base}/mediahubmx-catalog.json",
                            json=body,
                            headers=headers,
                            timeout=12.0,
                        )
                        if resp.status_code != 200:
                            failed = True
                            break
                        payload = resp.json()
                        page_items = payload.get("items") or []
                        if isinstance(page_items, list):
                            items.extend(it for it in page_items if isinstance(it, dict))
                        cursor = payload.get("nextCursor")
                        if not cursor:
                            break
                    except Exception:
                        failed = True
                        break
                if not failed and items:
                    return items
        return []

    async def _fetch_live2_index(self) -> list[dict[str, Any]]:
        client = self._get_client()
        for base in VAVOO_MIRROR_BASES:
            try:
                resp = await client.get(
                    f"{base}/live2/index",
                    params={"output": "json"},
                    headers={"User-Agent": VAVOO_PLAYLIST_UA, "Accept": "application/json"},
                    timeout=15.0,
                )
                if resp.status_code != 200:
                    continue
                data = resp.json()
                if isinstance(data, list) and data:
                    return [it for it in data if isinstance(it, dict)]
            except Exception as exc:
                logger.debug("TvVoo live2/index failed on %s: %s", base, exc)
        return []

    def _find_local_m3u_path(self) -> Optional[Path]:
        if self._m3u_fallback_path is not None:
            return self._m3u_fallback_path if self._m3u_fallback_path.is_file() else None
        candidates = (
            self.settings.BASE_DIR.parent / "tvvoo-main" / "src" / "channels" / "lista-it.m3u",
            self.settings.BASE_DIR / "tvvoo-main" / "src" / "channels" / "lista-it.m3u",
            Path("/app/tvvoo-main/src/channels/lista-it.m3u"),
        )
        for path in candidates:
            if path.is_file():
                return path
        return None

    def _build_stream_source(
        self,
        raw_name: str,
        play_url: str,
        group: str,
    ) -> Optional[tuple[str, StreamSource]]:
        country_arg = group if group in TVVOO_COUNTRY_GROUPS else None
        canonical = normalize_channel_name(raw_name, country=country_arg)
        if not canonical or not play_url:
            return None

        is_italian = group == "Italy"
        lang = _GROUP_LANGUAGE_MAP.get(group, "INT")
        quality = _detect_quality(raw_name, is_italian=is_italian)
        source_priority = 1 if is_italian else 10
        manifest_url = f"/live/manifest.m3u8?url={quote(play_url, safe='')}"

        source = StreamSource(
            provider=self.name,
            title=f"[⚡] {canonical.upper()} ({lang} {quality})",
            url=manifest_url,
            quality=quality,
            language=lang,
            priority=source_priority,
            requires_proxy=False,
            proxy_host="vavoo",
            channel_name=canonical,
            raw_url=play_url,
        )
        return canonical, source

    async def harvest(self) -> ProviderHarvestResult:
        """Harvest linear channels Geil-first, then ping+MediaHubMX, live2 & M3U fallbacks."""
        linear_channels: dict[str, list[StreamSource]] = {}
        ranked_entries: dict[str, list[tuple[int, StreamSource]]] = {}
        seen_raw_urls: set[tuple[str, str]] = set()
        raw_count = 0

        geil_grouped = await self._fetch_geil_catalog()
        sig: Optional[str] = None
        if not geil_grouped:
            sig = await self.live_manifest_manager.fetch_addon_sig()

        live2_cache: Optional[list[dict[str, Any]]] = None

        async def _get_live2() -> list[dict[str, Any]]:
            nonlocal live2_cache
            if live2_cache is None:
                live2_cache = await self._fetch_live2_index()
            return live2_cache

        for group in VAVOO_GROUPS:
            group_items: list[dict[str, Any]] = geil_grouped.get(group, []) if geil_grouped else []
            if not group_items and sig:
                group_items = await self._fetch_catalog_group(group, sig)

            if not group_items:
                accepted_groups = set(_GROUP_ALIASES.get(group, (group,)))
                all_live2 = await _get_live2()
                group_items = [
                    it for it in all_live2 if str(it.get("group") or "") in accepted_groups
                ]

            if not group_items and group == "Italy":
                m3u_path = self._find_local_m3u_path()
                if m3u_path is not None:
                    try:
                        m3u_text = m3u_path.read_text(encoding="utf-8", errors="ignore")
                        group_items = [
                            {"name": ch_name, "url": ch_url}
                            for ch_name, ch_url in parse_m3u_vavoo_channels(m3u_text)
                        ]
                    except Exception as exc:
                        logger.warning("TvVoo failed reading local M3U fallback %s: %s", m3u_path, exc)

            for item in group_items:
                raw_name = str(item.get("name") or "").strip()
                play_url = str(
                    item.get("url") or item.get("play") or item.get("href") or item.get("link") or ""
                ).strip()
                if not raw_name or not play_url:
                    continue
                raw_count += 1
                built = self._build_stream_source(raw_name, play_url, group)
                if built is None:
                    continue
                canonical, source = built
                dedup_key = (canonical, play_url)
                if dedup_key in seen_raw_urls:
                    continue
                seen_raw_urls.add(dedup_key)
                tag_rank = server_tag_rank(raw_name)
                ranked_entries.setdefault(canonical, []).append((tag_rank, source))

        new_siblings: dict[str, list[str]] = {}
        for canonical, entries in ranked_entries.items():
            entries.sort(key=lambda pair: (pair[1].priority, 0 if pair[1].language == "ITA" else 1, pair[0]))
            sorted_sources = [pair[1] for pair in entries]
            linear_channels[canonical] = sorted_sources
            sib_urls = [s.raw_url for s in sorted_sources if s.raw_url]
            if len(sib_urls) > 1:
                for u in sib_urls:
                    new_siblings[u] = [other for other in sib_urls if other != u]
        self._siblings_by_url = new_siblings

        return ProviderHarvestResult(
            provider=self.name,
            linear_channels=linear_channels,
            raw_count=raw_count,
        )
