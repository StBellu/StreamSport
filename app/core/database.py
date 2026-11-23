"""Async SQLite persistence manager built on aiosqlite with WAL mode."""

import asyncio
import time
from pathlib import Path
from typing import Any, Optional

import aiosqlite
import orjson

from app.config import get_settings
from app.models.domain import Match, SportSilo, StreamSource, TeamInfo

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS matches (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    category TEXT NOT NULL,
    silo TEXT NOT NULL,
    genres TEXT NOT NULL,
    date INTEGER NOT NULL,
    end_date INTEGER NOT NULL,
    poster TEXT NOT NULL DEFAULT '',
    popular INTEGER NOT NULL DEFAULT 0,
    competition TEXT NOT NULL DEFAULT '',
    canonical_competition TEXT NOT NULL DEFAULT '',
    country TEXT NOT NULL DEFAULT '',
    stage TEXT NOT NULL DEFAULT '',
    sources TEXT NOT NULL DEFAULT '[]',
    teams TEXT NOT NULL DEFAULT '{}',
    alt_home TEXT NOT NULL DEFAULT '',
    alt_away TEXT NOT NULL DEFAULT '',
    alt_league TEXT NOT NULL DEFAULT '',
    alt_title TEXT NOT NULL DEFAULT '',
    alt_competition TEXT NOT NULL DEFAULT '',
    alt_country TEXT NOT NULL DEFAULT '',
    is_women INTEGER NOT NULL DEFAULT 0,
    is_youth INTEGER NOT NULL DEFAULT 0,
    has_tvvoo INTEGER NOT NULL DEFAULT 0,
    broadcasters TEXT NOT NULL DEFAULT '[]',
    source_origin TEXT NOT NULL DEFAULT 'diretta',
    updated_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_matches_date ON matches (date);
CREATE INDEX IF NOT EXISTS idx_matches_cat ON matches (category);

CREATE TABLE IF NOT EXISTS thesportsdb_cache (
    query_key TEXT PRIMARY KEY,
    thumb_url TEXT,
    status TEXT NOT NULL,
    checked_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_tsdb_status ON thesportsdb_cache (status);

CREATE TABLE IF NOT EXISTS tennis_athletes_cache (
    slug TEXT PRIMARY KEY,
    provider TEXT NOT NULL,
    image_url TEXT NOT NULL,
    gender TEXT NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_tennis_slug ON tennis_athletes_cache (slug);
"""

_UPSERT_MATCH_SQL = """
INSERT INTO matches (
    id, title, category, silo, genres, date, end_date, poster, popular,
    competition, canonical_competition, country, stage, sources, teams,
    alt_home, alt_away, alt_league, alt_title, alt_competition, alt_country,
    is_women, is_youth, has_tvvoo, broadcasters, source_origin, updated_at
) VALUES (
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
)
ON CONFLICT(id) DO UPDATE SET
    title = excluded.title,
    category = excluded.category,
    silo = excluded.silo,
    genres = excluded.genres,
    date = excluded.date,
    end_date = excluded.end_date,
    poster = excluded.poster,
    popular = excluded.popular,
    competition = excluded.competition,
    canonical_competition = excluded.canonical_competition,
    country = excluded.country,
    stage = excluded.stage,
    sources = excluded.sources,
    teams = excluded.teams,
    alt_home = excluded.alt_home,
    alt_away = excluded.alt_away,
    alt_league = excluded.alt_league,
    alt_title = excluded.alt_title,
    alt_competition = excluded.alt_competition,
    alt_country = excluded.alt_country,
    is_women = excluded.is_women,
    is_youth = excluded.is_youth,
    has_tvvoo = excluded.has_tvvoo,
    broadcasters = excluded.broadcasters,
    source_origin = excluded.source_origin,
    updated_at = excluded.updated_at;
"""


def _now_ms() -> int:
    return int(time.time() * 1000)


def _dumps_json(value: Any) -> str:
    return orjson.dumps(value).decode("utf-8")


def _match_to_row(match: Match) -> tuple[Any, ...]:
    teams_payload = {
        "home": match.home_team.model_dump() if match.home_team else None,
        "away": match.away_team.model_dump() if match.away_team else None,
    }
    sources_payload = [src.model_dump() for src in match.sources]
    return (
        match.id,
        match.title,
        match.category,
        match.silo.value,
        _dumps_json(match.genres),
        match.date,
        match.end_date,
        match.poster,
        int(match.popular),
        match.competition,
        match.canonical_competition,
        match.country,
        match.stage,
        _dumps_json(sources_payload),
        _dumps_json(teams_payload),
        match.alt_home,
        match.alt_away,
        match.alt_league,
        match.alt_title,
        match.alt_competition,
        match.alt_country,
        int(match.is_women),
        int(match.is_youth),
        int(match.has_tvvoo),
        _dumps_json(match.broadcasters),
        match.source_origin,
        match.updated_at,
    )


def _row_to_match(row: aiosqlite.Row) -> Match:
    teams_raw = orjson.loads(row["teams"] or "{}")
    home_raw = teams_raw.get("home") if isinstance(teams_raw, dict) else None
    away_raw = teams_raw.get("away") if isinstance(teams_raw, dict) else None

    sources_raw = orjson.loads(row["sources"] or "[]")
    genres_raw = orjson.loads(row["genres"] or "[]")
    broadcasters_raw = orjson.loads(row["broadcasters"] or "[]")

    return Match(
        id=row["id"],
        title=row["title"],
        category=row["category"],
        silo=SportSilo(row["silo"]),
        genres=genres_raw,
        date=row["date"],
        end_date=row["end_date"],
        poster=row["poster"],
        popular=bool(row["popular"]),
        competition=row["competition"],
        canonical_competition=row["canonical_competition"],
        country=row["country"],
        stage=row["stage"],
        sources=[StreamSource.model_validate(item) for item in sources_raw],
        home_team=TeamInfo.model_validate(home_raw) if home_raw else None,
        away_team=TeamInfo.model_validate(away_raw) if away_raw else None,
        alt_home=row["alt_home"],
        alt_away=row["alt_away"],
        alt_league=row["alt_league"],
        alt_title=row["alt_title"] if "alt_title" in row.keys() else "",
        alt_competition=row["alt_competition"] if "alt_competition" in row.keys() else "",
        alt_country=row["alt_country"] if "alt_country" in row.keys() else "",
        is_women=bool(row["is_women"]),
        is_youth=bool(row["is_youth"]),
        has_tvvoo=bool(row["has_tvvoo"]),
        broadcasters=broadcasters_raw,
        source_origin=row["source_origin"],
        updated_at=row["updated_at"],
    )


class DatabaseManager:
    """Async SQLite manager enforcing WAL mode, busy_timeout, and serialized writes."""

    def __init__(self, db_path: Optional[Path] = None) -> None:
        self._db_path = db_path or get_settings().DB_PATH
        self._db: Optional[aiosqlite.Connection] = None
        self._write_lock = asyncio.Lock()

    @property
    def db_path(self) -> Path:
        """Return the configured SQLite file path."""
        return self._db_path

    def _require_db(self) -> aiosqlite.Connection:
        if self._db is None:
            raise RuntimeError("DatabaseManager is not connected. Call connect() first.")
        return self._db

    async def connect(self) -> None:
        """Ensure data directory exists, open WAL connection, and create schema."""
        if self._db is not None:
            return
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = await aiosqlite.connect(self._db_path, isolation_level=None)
        conn.row_factory = aiosqlite.Row
        await conn.execute("PRAGMA journal_mode = WAL;")
        await conn.execute("PRAGMA synchronous = NORMAL;")
        await conn.execute("PRAGMA busy_timeout = 30000;")
        await conn.execute("PRAGMA foreign_keys = ON;")
        await conn.executescript(_SCHEMA_SQL)
        # Idempotent schema migration for pre-existing SQLite databases.
        async with conn.execute("PRAGMA table_info(matches);") as cur:
            existing_cols = {r[1] for r in await cur.fetchall()}
        for col in ("alt_title", "alt_competition", "alt_country"):
            if col not in existing_cols:
                await conn.execute(
                    f"ALTER TABLE matches ADD COLUMN {col} TEXT NOT NULL DEFAULT '';"
                )
        self._db = conn

    async def close(self) -> None:
        """Close the SQLite connection if currently open."""
        if self._db is not None:
            await self._db.close()
            self._db = None

    async def _run_write_tx(self, operation: Any) -> Any:
        """Serialize a write transaction under `self._write_lock` with BEGIN IMMEDIATE."""
        db = self._require_db()
        async with self._write_lock:
            await db.execute("BEGIN IMMEDIATE;")
            try:
                result = await operation(db)
                await db.execute("COMMIT;")
                return result
            except Exception:
                await db.execute("ROLLBACK;")
                raise

    async def upsert_matches(self, matches: list[Match]) -> None:
        """Insert or update a batch of matches in a single atomic transaction."""
        if not matches:
            return
        rows = [_match_to_row(match) for match in matches]

        async def _op(db: aiosqlite.Connection) -> None:
            await db.executemany(_UPSERT_MATCH_SQL, rows)

        await self._run_write_tx(_op)

    async def load_active_matches(self, min_end_ms: int) -> list[Match]:
        """Load all matches whose `end_date` is at or after `min_end_ms`."""
        db = self._require_db()
        async with db.execute(
            "SELECT * FROM matches WHERE end_date >= ? ORDER BY date ASC;",
            (min_end_ms,),
        ) as cursor:
            rows = await cursor.fetchall()
        return [_row_to_match(row) for row in rows]

    async def purge_expired(self, retention_hours: int) -> list[str]:
        """Delete expired matches and stale `not_found` TheSportsDB records."""
        now_ms = _now_ms()
        match_cutoff_ms = now_ms - (retention_hours * 3600 * 1000)
        tsdb_cutoff_ms = now_ms - (86400 * 1000)

        async def _op(db: aiosqlite.Connection) -> list[str]:
            async with db.execute(
                "SELECT id FROM matches WHERE end_date < ?;",
                (match_cutoff_ms,),
            ) as cursor:
                purged_ids = [row["id"] for row in await cursor.fetchall()]
            if purged_ids:
                await db.execute(
                    "DELETE FROM matches WHERE end_date < ?;",
                    (match_cutoff_ms,),
                )
            await db.execute(
                "DELETE FROM thesportsdb_cache WHERE status = 'not_found' AND checked_at < ?;",
                (tsdb_cutoff_ms,),
            )
            return purged_ids

        return await self._run_write_tx(_op)

    # ------------------------------------------------------------------
    # thesportsdb_cache CRUD
    # ------------------------------------------------------------------

    async def get_tsdb_cache(self, query_key: str) -> Optional[dict[str, Any]]:
        """Fetch a single cached TheSportsDB lookup entry by key."""
        db = self._require_db()
        async with db.execute(
            "SELECT query_key, thumb_url, status, checked_at FROM thesportsdb_cache WHERE query_key = ?;",
            (query_key,),
        ) as cursor:
            row = await cursor.fetchone()
        return dict(row) if row is not None else None

    async def get_tsdb_cache_bulk(self, query_keys: list[str]) -> dict[str, dict[str, Any]]:
        """Fetch multiple cached TheSportsDB entries in one query."""
        if not query_keys:
            return {}
        db = self._require_db()
        placeholders = ",".join("?" for _ in query_keys)
        sql = (
            "SELECT query_key, thumb_url, status, checked_at "
            f"FROM thesportsdb_cache WHERE query_key IN ({placeholders});"
        )
        async with db.execute(sql, query_keys) as cursor:
            rows = await cursor.fetchall()
        return {row["query_key"]: dict(row) for row in rows}

    async def upsert_tsdb_cache(
        self,
        query_key: str,
        thumb_url: Optional[str],
        status: str,
        checked_at: Optional[int] = None,
    ) -> None:
        """Insert or update a TheSportsDB cache record."""
        ts = checked_at if checked_at is not None else _now_ms()

        async def _op(db: aiosqlite.Connection) -> None:
            await db.execute(
                """
                INSERT INTO thesportsdb_cache (query_key, thumb_url, status, checked_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(query_key) DO UPDATE SET
                    thumb_url = excluded.thumb_url,
                    status = excluded.status,
                    checked_at = excluded.checked_at;
                """,
                (query_key, thumb_url, status, ts),
            )

        await self._run_write_tx(_op)

    async def delete_tsdb_cache(self, query_key: str) -> None:
        """Remove a single entry from `thesportsdb_cache`."""

        async def _op(db: aiosqlite.Connection) -> None:
            await db.execute(
                "DELETE FROM thesportsdb_cache WHERE query_key = ?;",
                (query_key,),
            )

        await self._run_write_tx(_op)

    # ------------------------------------------------------------------
    # tennis_athletes_cache CRUD
    # ------------------------------------------------------------------

    async def get_tennis_athlete(self, slug: str) -> Optional[dict[str, Any]]:
        """Fetch a cached tennis athlete cutout entry by slug."""
        db = self._require_db()
        async with db.execute(
            "SELECT slug, provider, image_url, gender, updated_at FROM tennis_athletes_cache WHERE slug = ?;",
            (slug,),
        ) as cursor:
            row = await cursor.fetchone()
        return dict(row) if row is not None else None

    async def upsert_tennis_athlete(
        self,
        slug: str,
        provider: str,
        image_url: str,
        gender: str,
        updated_at: Optional[int] = None,
    ) -> None:
        """Insert or update a tennis athlete cutout record."""
        ts = updated_at if updated_at is not None else _now_ms()

        async def _op(db: aiosqlite.Connection) -> None:
            await db.execute(
                """
                INSERT INTO tennis_athletes_cache (slug, provider, image_url, gender, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(slug) DO UPDATE SET
                    provider = excluded.provider,
                    image_url = excluded.image_url,
                    gender = excluded.gender,
                    updated_at = excluded.updated_at;
                """,
                (slug, provider, image_url, gender, ts),
            )

        await self._run_write_tx(_op)

    async def delete_tennis_athlete(self, slug: str) -> None:
        """Remove a tennis athlete entry by slug."""

        async def _op(db: aiosqlite.Connection) -> None:
            await db.execute(
                "DELETE FROM tennis_athletes_cache WHERE slug = ?;",
                (slug,),
            )

        await self._run_write_tx(_op)
