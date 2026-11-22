"""Application configuration loaded from environment variables and `.env`."""

from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

_BASE_DIR = Path(__file__).resolve().parent.parent


class Settings(BaseSettings):
    """Typed runtime settings for StreamSport."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # App settings
    APP_ENV: str = "production"
    HOST: str = "0.0.0.0"
    PORT: int = 7860
    LOG_LEVEL: str = "INFO"
    TIMEZONE: str = "Europe/Rome"

    # Intervals & retention
    SYNC_INTERVAL_SECONDS: int = 300
    EPG_SYNC_INTERVAL_SECONDS: int = 900
    THESPORTSDB_SYNC_INTERVAL_SECONDS: int = 43200
    MATCH_RETENTION_HOURS: int = 6

    # Feature & provider toggles
    ENABLE_DIRETTA: bool = True
    ENABLE_SKY_EPG: bool = True
    ENABLE_TIVUSAT_EPG: bool = True
    ENABLE_THESPORTSDB: bool = True
    ENABLE_TENNIS_POSTERS: bool = True
    ENABLE_TVVOO: bool = True
    ENABLE_MANDRAKODI: bool = True
    ENABLE_SPON: bool = True
    ENABLE_DADDYLIVE: bool = True
    ENABLE_STREAMFREE: bool = True
    ENABLE_FCTV33: bool = True

    # Secrets & external endpoints
    THESPORTSDB_USER: str = ""
    THESPORTSDB_PASS: str = ""
    MANDRAKODI_PASSWORD: str = ""
    MANDRAKODI_DEVICE_ID: str = ""
    MANDRAKODI_API_URL: str = ""
    FLASHSCORE_FSIGN: str = "SW9D1eZo"
    DEFAULT_EP_URL: str = ""
    DEFAULT_EP_PASS: str = ""

    # Filesystem paths
    BASE_DIR: Path = _BASE_DIR
    DATA_DIR: Path = _BASE_DIR / "data"
    DB_PATH: Path = _BASE_DIR / "data" / "streamsport.db"
    POSTER_CACHE_DIR: Path = _BASE_DIR / "data" / "posters"
    BASE_POSTER_CACHE_DIR: Path = _BASE_DIR / "data" / "posters_base"
    MEDIA_DIR: Path = _BASE_DIR / "media"
    TENNIS_ASSETS_DIR: Path = _BASE_DIR / "media" / "Tennis" / "assets"
    TENNIS_BG_DIR: Path = _BASE_DIR / "media" / "Tennis" / "backgrounds"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the cached singleton Settings instance."""
    return Settings()
