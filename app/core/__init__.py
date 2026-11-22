"""Core infrastructure: database, RAM cache, DoH networking, and user config."""

from app.core.database import DatabaseManager
from app.core.network import (
    DoHAsyncHTTPTransport,
    DoHNetworkBackend,
    DoHResolver,
    create_doh_client,
    create_standard_client,
    is_safe_public_ip,
)
from app.core.ram_cache import CatalogSnapshot, RAMCacheManager
from app.core.user_config import UserConfig, decode_config, encode_config

__all__ = [
    "CatalogSnapshot",
    "DatabaseManager",
    "DoHAsyncHTTPTransport",
    "DoHNetworkBackend",
    "DoHResolver",
    "RAMCacheManager",
    "UserConfig",
    "create_doh_client",
    "create_standard_client",
    "decode_config",
    "encode_config",
    "is_safe_public_ip",
]
