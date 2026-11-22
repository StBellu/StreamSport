"""Pydantic models for the Stremio and Nuvio Addon protocol."""

from typing import Any, Optional, Union

from pydantic import BaseModel, Field


class ManifestExtra(BaseModel):
    """Supported extra parameter for a Stremio catalog."""

    name: str
    isRequired: bool = False
    options: list[str] = Field(default_factory=list)
    optionsLimit: Optional[int] = None


class ManifestCatalog(BaseModel):
    """Catalog definition declared inside the addon manifest."""

    type: str
    id: str
    name: str
    extra: list[ManifestExtra] = Field(default_factory=list)


class ManifestBehaviorHints(BaseModel):
    """Manifest-level client capability and configuration hints."""

    configurable: bool = True
    configurationRequired: bool = False
    adult: bool = False
    p2p: bool = False


class Manifest(BaseModel):
    """Top-level Stremio/Nuvio addon manifest payload."""

    id: str
    version: str
    name: str
    description: str
    logo: Optional[str] = None
    background: Optional[str] = None
    resources: list[Union[str, dict[str, Any]]] = Field(
        default_factory=lambda: ["catalog", "meta", "stream"]
    )
    types: list[str] = Field(default_factory=lambda: ["tv"])
    idPrefixes: list[str] = Field(default_factory=lambda: ["streamsport:"])
    catalogs: list[ManifestCatalog] = Field(default_factory=list)
    behaviorHints: Optional[ManifestBehaviorHints] = None


class MetaPreview(BaseModel):
    """Compact catalog card item returned by `/catalog`."""

    id: str
    type: str = "tv"
    name: str
    poster: Optional[str] = None
    posterShape: str = "landscape"
    description: Optional[str] = None
    genres: list[str] = Field(default_factory=list)
    releaseInfo: Optional[str] = None


class MetaDetail(BaseModel):
    """Full event metadata returned by `/meta`."""

    id: str
    type: str = "tv"
    name: str
    poster: Optional[str] = None
    posterShape: str = "landscape"
    background: Optional[str] = None
    logo: Optional[str] = None
    description: Optional[str] = None
    genres: list[str] = Field(default_factory=list)
    releaseInfo: Optional[str] = None
    runtime: Optional[str] = None
    country: Optional[str] = None
    behaviorHints: dict[str, Any] = Field(default_factory=dict)


class StreamItem(BaseModel):
    """Single playable stream entry returned by `/stream`."""

    name: Optional[str] = None
    title: Optional[str] = None
    description: Optional[str] = None
    url: Optional[str] = None
    infoHash: Optional[str] = None
    externalUrl: Optional[str] = None
    behaviorHints: Optional[dict[str, Any]] = None


class CatalogResponse(BaseModel):
    """Stremio catalog endpoint response envelope."""

    metas: list[MetaPreview] = Field(default_factory=list)


class MetaResponse(BaseModel):
    """Stremio meta endpoint response envelope."""

    meta: Optional[MetaDetail] = None


class StreamResponse(BaseModel):
    """Stremio stream endpoint response envelope."""

    streams: list[StreamItem] = Field(default_factory=list)
