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
    SYNC_INTERVAL_SECONDS: int = 3600
    SYNC_INTERVAL_SECONDS_NIGHT: int = 14400
    EPG_SYNC_INTERVAL_SECONDS: int = 7200
    THESPORTSDB_SYNC_INTERVAL_SECONDS: int = 43200
    MATCH_RETENTION_HOURS: int = 6

    # Feature & provider toggles
    ENABLE_DIRETTA: bool = True
    ENABLE_SKY_EPG: bool = True
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
    MANDRAKODI_XOR_KEY: str = "my_secret_key"
    FLASHSCORE_FSIGN: str = "SW9D1eZo"
    DEFAULT_EP_URL: str = ""
    DEFAULT_EP_PASS: str = ""
    PUBLIC_URL: str = ""

    # Provider endpoints & cache TTLs
    STREAMFREE_DIRECTORY_URL: str = "https://strmfree.link/api/domains"
    STREAMFREE_PRIMARY_MIRROR: str = "strmfree.pk"
    STREAMFREE_FALLBACK_MIRRORS: str = "strmfree.st,streamfree.top"
    FCTV33_API_BASE: str = "https://apis-data10.tcpwe138stya.ru"
    SPON_PRIMARY_URL: str = "https://sportsonline.st/prog.txt"
    SPON_FALLBACK_URL: str = "https://sportsonline.cx/prog.txt"
    SPON_CACHE_TTL: int = 7200

    @property
    def DIRETTA_X_FSIGN(self) -> str:
        """Backward-compatible alias for `FLASHSCORE_FSIGN`."""
        return self.FLASHSCORE_FSIGN

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
