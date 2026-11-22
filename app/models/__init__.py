"""Domain and Stremio protocol models."""

from app.models.domain import (
    EPGProgram,
    Match,
    ProviderEvent,
    SportSilo,
    StreamSource,
    TeamInfo,
)
from app.models.stremio import (
    CatalogResponse,
    Manifest,
    ManifestBehaviorHints,
    ManifestCatalog,
    ManifestExtra,
    MetaDetail,
    MetaPreview,
    MetaResponse,
    StreamItem,
    StreamResponse,
)

__all__ = [
    "CatalogResponse",
    "EPGProgram",
    "Manifest",
    "ManifestBehaviorHints",
    "ManifestCatalog",
    "ManifestExtra",
    "Match",
    "MetaDetail",
    "MetaPreview",
    "MetaResponse",
    "ProviderEvent",
    "SportSilo",
    "StreamItem",
    "StreamResponse",
    "StreamSource",
    "TeamInfo",
]
