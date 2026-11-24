"""Domain models for sports events, schedules, EPG, and stream sources."""

from enum import StrEnum
from typing import Optional

from pydantic import BaseModel, Field


class SportSilo(StrEnum):
    """Canonical sport categories used for isolation, classification, and catalogs."""

    CALCIO_ITA = "calcio_ita"
    FOOTBALL = "football"
    TENNIS = "tennis"
    BASKET = "basket"
    MOTORI = "motori"
    COMBAT = "combat"
    VOLLEY = "volley"
    NFL = "nfl"
    HOCKEY = "hockey"
    BASEBALL = "baseball"
    RUGBY = "rugby"
    HANDBALL = "handball"
    ALTRI_SPORT = "altri_sport"


class TeamInfo(BaseModel):
    """Normalized competitor or athlete metadata."""

    name: str
    alt_name: str = ""
    slug: str = ""
    country: str = ""


class StreamSource(BaseModel):
    """Single playable stream candidate attached to a sporting event."""

    provider: str
    title: str
    url: str
    quality: str = ""
    language: str = ""
    priority: int = 10
    requires_proxy: bool = True
    proxy_host: str = ""
    clearkey: str = ""
    headers: dict[str, str] = Field(default_factory=dict)
    is_acestream: bool = False
    channel_name: str = ""
    raw_url: str = ""


class Match(BaseModel):
    """Unified sports event record stored in SQLite and served from RAM cache."""

    id: str
    title: str
    category: str
    silo: SportSilo
    genres: list[str] = Field(default_factory=list)
    date: int
    end_date: int
    poster: str = ""
    popular: bool = False
    competition: str = ""
    canonical_competition: str = ""
    country: str = ""
    stage: str = ""
    sources: list[StreamSource] = Field(default_factory=list)
    home_team: Optional[TeamInfo] = None
    away_team: Optional[TeamInfo] = None
    alt_home: str = ""
    alt_away: str = ""
    alt_league: str = ""
    alt_title: str = ""
    alt_competition: str = ""
    alt_country: str = ""
    is_women: bool = False
    is_youth: bool = False
    has_tvvoo: bool = False
    broadcasters: list[str] = Field(default_factory=list)
    source_origin: str = "diretta"
    updated_at: int = 0

    @property
    def stremio_id(self) -> str:
        """Return the ID prefixed with `streamsport:` for Stremio routes."""
        return self.id if self.id.startswith("streamsport:") else f"streamsport:{self.id}"


class EPGProgram(BaseModel):
    """Linear TV broadcast schedule item from Sky Italia."""

    id: str
    channel_id: str
    channel_name: str
    is_dedicated_sports: bool = True
    title: str
    epg_title: str = ""
    content_title: str = ""
    description: str = ""
    category: str = ""
    start_ms: int
    end_ms: int
    sport_hint: str = ""
    content_url: str = ""
    program_highlights: str = ""
    season_number: Optional[int] = None
    competition_hint: str = ""
    poster_url: str = ""
    is_live: bool = True
    is_simulcast: bool = False
    source: str = "sky"


class ProviderEvent(BaseModel):
    """Intermediate event/stream bundle harvested from an external stream provider."""

    provider: str
    title: str
    home: str = ""
    away: str = ""
    competition: str = ""
    sport_hint: str = ""
    start_ms: int = 0
    is_women: bool = False
    is_youth: bool = False
    streams: list[StreamSource] = Field(default_factory=list)


DIRETTA_POST_MATCH_MS: int = 30 * 60 * 1000
SKY_EPG_POST_END_MS: int = 15 * 60 * 1000
DEFAULT_FALLBACK_DURATION_MS: int = 150 * 60 * 1000


def get_match_post_grace_ms(match: Match) -> int:
    """Return post-end visibility grace in ms (`15m` for Sky EPG, `30m` otherwise)."""
    return SKY_EPG_POST_END_MS if match.source_origin in ("epg", "sky_epg") else DIRETTA_POST_MATCH_MS


def get_match_expiry_ms(match: Match) -> int:
    """Return the exact expiration timestamp in ms after which `match` leaves catalogs and DB."""
    if match.end_date > 0:
        return match.end_date + get_match_post_grace_ms(match)
    if match.date > 0:
        return match.date + DEFAULT_FALLBACK_DURATION_MS
    return 0
