"""DoH (DNS-over-HTTPS) resolver, anti-SSRF IP validation, and httpx factories."""

import asyncio
import ipaddress
import ssl
import time
import typing
from typing import Iterable, Optional
from urllib.parse import urlsplit

import httpcore
import httpx

_CGNAT_NETWORK = ipaddress.ip_network("100.64.0.0/10")
_BLOCKED_IPS = frozenset({"169.254.169.254", "172.16.0.2"})
_MIN_TTL_SECONDS = 60

_DIRECT_PROXY_TOKENS = frozenset({"direct", "none", "off", "false", "0"})
_SERVICE_PROXY_FIELDS: dict[str, str] = {
    "diretta": "DIRETTA_PROXY",
    "sky_epg": "SKY_EPG_PROXY",
    "thesportsdb": "THESPORTSDB_PROXY",
    "tennis_posters": "TENNIS_POSTERS_PROXY",
    "tvvoo": "TVVOO_PROXY",
    "mandrakodi": "MANDRAKODI_PROXY",
    "spon": "SPON_PROXY",
    "daddylive": "DADDYLIVE_PROXY",
    "streamfree": "STREAMFREE_PROXY",
    "fctv33": "FCTV33_PROXY",
}


def resolve_proxy_url(settings: typing.Any, service: str) -> Optional[str]:
    """Resolve effective outbound proxy URL for `service` (`None` means direct)."""
    if settings is None:
        return None
    field_name = _SERVICE_PROXY_FIELDS.get(service.strip().lower())
    if field_name:
        raw_override = str(getattr(settings, field_name, "") or "").strip()
        if raw_override:
            if raw_override.lower() in _DIRECT_PROXY_TOKENS:
                return None
            return raw_override
    raw_global = str(getattr(settings, "GLOBAL_PROXY", "") or "").strip()
    if raw_global and raw_global.lower() not in _DIRECT_PROXY_TOKENS:
        return raw_global
    return None


def format_proxy_label(proxy: Optional[str]) -> str:
    """Format a proxy URL without embedded credentials (`scheme://host[:port]` or `direct`)."""
    raw = (proxy or "").strip()
    if not raw:
        return "direct"
    try:
        parsed = urlsplit(raw)
        if parsed.scheme and parsed.hostname:
            port_part = f":{parsed.port}" if parsed.port else ""
            return f"{parsed.scheme}://{parsed.hostname}{port_part}"
    except Exception:
        pass
    return "<configured-proxy>"

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36"
)
DEFAULT_HEADERS: dict[str, str] = {
    "User-Agent": DEFAULT_USER_AGENT,
    "Accept-Language": "it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7",
}

_DOH_PROVIDERS: tuple[tuple[str, dict[str, str]], ...] = (
    ("https://cloudflare-dns.com/dns-query", {"Accept": "application/dns-json"}),
    ("https://dns.google/resolve", {"Accept": "application/json"}),
)


def is_safe_public_ip(ip_str: str) -> bool:
    """Return True only if `ip_str` is a routable public IP (anti-SSRF + sinkhole guard)."""
    cleaned = ip_str.strip()
    if cleaned in _BLOCKED_IPS:
        return False
    try:
        parsed = ipaddress.ip_address(cleaned)
    except ValueError:
        return False

    # Unwrap IPv4-mapped IPv6 addresses (e.g. ::ffff:127.0.0.1)
    if isinstance(parsed, ipaddress.IPv6Address) and parsed.ipv4_mapped is not None:
        parsed = parsed.ipv4_mapped

    if str(parsed) in _BLOCKED_IPS:
        return False
    if isinstance(parsed, ipaddress.IPv4Address) and parsed in _CGNAT_NETWORK:
        return False

    if (
        parsed.is_private
        or parsed.is_loopback
        or parsed.is_link_local
        or parsed.is_multicast
        or parsed.is_reserved
        or parsed.is_unspecified
        or not parsed.is_global
    ):
        return False
    return True


def _is_ip_literal(host: str) -> bool:
    try:
        ipaddress.ip_address(host.strip("[]"))
        return True
    except ValueError:
        return False


class DoHResolver:
    """In-memory cached DNS-over-HTTPS resolver with Cloudflare and Google endpoints."""

    def __init__(self, timeout: float = 5.0) -> None:
        self._timeout = timeout
        self._cache: dict[str, tuple[tuple[str, ...], float]] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._client: Optional[httpx.AsyncClient] = None

    def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=self._timeout,
                headers={"User-Agent": DEFAULT_USER_AGENT},
                follow_redirects=False,
            )
        return self._client

    async def aclose(self) -> None:
        """Close the underlying DoH HTTP client."""
        if self._client is not None and not self._client.is_closed:
            await self._client.aclose()
            self._client = None

    def invalidate(self, host: str) -> None:
        """Evict cached DNS answers for `host`."""
        self._cache.pop(host.lower(), None)

    async def _query_provider(
        self,
        endpoint: str,
        headers: dict[str, str],
        hostname: str,
        record_type: str,
    ) -> tuple[list[str], int]:
        client = self._get_client()
        resp = await client.get(
            endpoint,
            params={"name": hostname, "type": record_type},
            headers=headers,
        )
        resp.raise_for_status()
        payload = resp.json()
        if payload.get("Status", 0) != 0:
            return [], _MIN_TTL_SECONDS

        expected_type = 1 if record_type == "A" else 28
        ips: list[str] = []
        min_ttl = _MIN_TTL_SECONDS
        for answer in payload.get("Answer", []) or []:
            if answer.get("type") != expected_type:
                continue
            data = str(answer.get("data", "")).strip()
            if is_safe_public_ip(data):
                ips.append(data)
                ttl = int(answer.get("TTL", _MIN_TTL_SECONDS) or _MIN_TTL_SECONDS)
                min_ttl = max(min_ttl, ttl)
        return ips, min_ttl

    async def resolve_all(self, host: str) -> tuple[str, ...]:
        """Resolve `host` to one or more verified safe public IPs."""
        cleaned = host.strip().strip("[]").lower()
        if _is_ip_literal(cleaned):
            if not is_safe_public_ip(cleaned):
                raise httpcore.ConnectError(f"Blocked unsafe IP address: {cleaned}")
            return (cleaned,)

        now = time.monotonic()
        cached = self._cache.get(cleaned)
        if cached is not None and cached[1] > now:
            return cached[0]

        lock = self._locks.setdefault(cleaned, asyncio.Lock())
        async with lock:
            now = time.monotonic()
            cached = self._cache.get(cleaned)
            if cached is not None and cached[1] > now:
                return cached[0]

            for record_type in ("A", "AAAA"):
                for endpoint, headers in _DOH_PROVIDERS:
                    try:
                        ips, ttl = await self._query_provider(
                            endpoint, headers, cleaned, record_type
                        )
                    except Exception:
                        continue
                    if ips:
                        result = tuple(dict.fromkeys(ips))
                        self._cache[cleaned] = (result, time.monotonic() + ttl)
                        return result

        raise httpcore.ConnectError(f"DoH resolution failed or returned unsafe IP for {host}")

    async def resolve(self, host: str) -> str:
        """Resolve `host` to a single verified safe public IP address."""
        ips = await self.resolve_all(host)
        return ips[0]


_DEFAULT_RESOLVER = DoHResolver()


class DoHNetworkBackend(httpcore.AnyIOBackend):
    """httpcore network backend resolving hostnames via DoH while preserving TLS SNI."""

    def __init__(self, resolver: Optional[DoHResolver] = None) -> None:
        super().__init__()
        self._resolver = resolver or _DEFAULT_RESOLVER

    async def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: Optional[float] = None,
        local_address: Optional[str] = None,
        socket_options: Optional[Iterable[typing.Any]] = None,
    ) -> httpcore.AsyncNetworkStream:
        """Resolve hostname via DoH and connect directly to the safe public IP."""
        target_ips = await self._resolver.resolve_all(host)
        last_exc: Optional[Exception] = None
        for ip in target_ips:
            try:
                return await super().connect_tcp(
                    ip,
                    port,
                    timeout=timeout,
                    local_address=local_address,
                    socket_options=socket_options,
                )
            except httpcore.ConnectError as exc:
                last_exc = exc
        self._resolver.invalidate(host)
        assert last_exc is not None
        raise last_exc


class DoHAsyncHTTPTransport(httpx.AsyncHTTPTransport):
    """httpx AsyncHTTPTransport wired with `DoHNetworkBackend` on its connection pool."""

    def __init__(
        self,
        resolver: Optional[DoHResolver] = None,
        verify: ssl.SSLContext | str | bool = True,
        cert: typing.Any = None,
        trust_env: bool = True,
        http1: bool = True,
        http2: bool = False,
        limits: httpx.Limits = httpx.Limits(
            max_connections=100,
            max_keepalive_connections=20,
            keepalive_expiry=5.0,
        ),
        proxy: typing.Any = None,
        uds: Optional[str] = None,
        local_address: Optional[str] = None,
        retries: int = 0,
        socket_options: Optional[Iterable[typing.Any]] = None,
    ) -> None:
        super().__init__(
            verify=verify,
            cert=cert,
            trust_env=trust_env,
            http1=http1,
            http2=http2,
            limits=limits,
            proxy=proxy,
            uds=uds,
            local_address=local_address,
            retries=retries,
            socket_options=socket_options,
        )
        if proxy is None:
            ssl_context = getattr(self._pool, "_ssl_context", None)
            self._pool = httpcore.AsyncConnectionPool(
                ssl_context=ssl_context,
                max_connections=limits.max_connections,
                max_keepalive_connections=limits.max_keepalive_connections,
                keepalive_expiry=limits.keepalive_expiry,
                http1=http1,
                http2=http2,
                uds=uds,
                local_address=local_address,
                retries=retries,
                network_backend=DoHNetworkBackend(resolver),
                socket_options=socket_options,
            )


def create_doh_client(
    *,
    timeout: float = 15.0,
    headers: Optional[dict[str, str]] = None,
    follow_redirects: bool = True,
    verify: ssl.SSLContext | str | bool = True,
    http2: bool = False,
    resolver: Optional[DoHResolver] = None,
    limits: Optional[httpx.Limits] = None,
    proxy: Optional[str] = None,
) -> httpx.AsyncClient:
    """Create an `httpx.AsyncClient` using DoH DNS resolution and anti-SSRF checks."""
    merged_headers = {**DEFAULT_HEADERS, **(headers or {})}
    transport_limits = limits or httpx.Limits(
        max_connections=100,
        max_keepalive_connections=20,
        keepalive_expiry=5.0,
    )
    transport = DoHAsyncHTTPTransport(
        resolver=resolver,
        verify=verify,
        http2=http2,
        limits=transport_limits,
        proxy=proxy,
        trust_env=False,
    )
    return httpx.AsyncClient(
        transport=transport,
        timeout=timeout,
        headers=merged_headers,
        follow_redirects=follow_redirects,
    )


def create_standard_client(
    *,
    timeout: float = 15.0,
    headers: Optional[dict[str, str]] = None,
    follow_redirects: bool = True,
    verify: ssl.SSLContext | str | bool = True,
    http2: bool = False,
    limits: Optional[httpx.Limits] = None,
    proxy: Optional[str] = None,
) -> httpx.AsyncClient:
    """Create a standard `httpx.AsyncClient` with browser-like default headers."""
    merged_headers = {**DEFAULT_HEADERS, **(headers or {})}
    client_limits = limits or httpx.Limits(
        max_connections=100,
        max_keepalive_connections=20,
        keepalive_expiry=5.0,
    )
    return httpx.AsyncClient(
        timeout=timeout,
        headers=merged_headers,
        follow_redirects=follow_redirects,
        verify=verify,
        http2=http2,
        limits=client_limits,
        proxy=proxy,
        trust_env=False,
    )
