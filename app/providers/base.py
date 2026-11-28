"""Abstract base contract and shared helpers for StreamSport stream providers."""

from __future__ import annotations

import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional

from app.config import Settings, get_settings
from app.models.domain import ProviderEvent, StreamSource

_WHITESPACE_RE = re.compile(r"\s+")


def compact_preview(raw: str | bytes, max_len: int = 80) -> str:
    """Return a single-line, length-capped diagnostic preview of a raw response."""
    if isinstance(raw, bytes):
        text = raw.decode("utf-8", errors="replace")
    else:
        text = str(raw or "")
    collapsed = _WHITESPACE_RE.sub(" ", text).strip()
    if len(collapsed) <= max_len:
        return collapsed
    return collapsed[: max(0, max_len - 3)] + "..."


@dataclass(slots=True)
class ProviderHarvestResult:
    """Aggregated result returned by a single provider harvest cycle."""

    provider: str
    events: list[ProviderEvent] = field(default_factory=list)
    linear_channels: dict[str, list[StreamSource]] = field(default_factory=dict)
    raw_count: int = 0
    error: str = ""
    diagnostic: str = ""


class BaseStreamProvider(ABC):
    """Base interface implemented by every live stream provider."""

    name: str = "base"
    priority: int = 99

    def __init__(self, settings: Optional[Settings] = None) -> None:
        self._settings = settings or get_settings()

    @property
    def settings(self) -> Settings:
        """Return runtime application settings."""
        return self._settings

    @property
    @abstractmethod
    def enabled(self) -> bool:
        """Return True when this provider is enabled in configuration/env."""

    @abstractmethod
    async def harvest(self) -> ProviderHarvestResult:
        """Fetch and normalize live events and/or linear TV channels without proxying video."""

    async def safe_harvest(self) -> ProviderHarvestResult:
        """Execute `harvest()` honoring the `enabled` toggle and catching upstream errors."""
        if not self.enabled:
            return ProviderHarvestResult(provider=self.name, diagnostic="disabled")
        try:
            return await self.harvest()
        except Exception as exc:
            return ProviderHarvestResult(provider=self.name, error=str(exc))

    async def aclose(self) -> None:
        """Release any held HTTP clients or sessions."""
        return None
