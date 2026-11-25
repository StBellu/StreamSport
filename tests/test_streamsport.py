"""End-to-end Phase 4 test suite for StreamSport."""

from __future__ import annotations

import io
import time
from pathlib import Path
from typing import Any

import httpcore
import httpx
import pytest
from PIL import Image

from app.config import Settings
from app.core.database import DatabaseManager
from app.core.network import DoHResolver, is_safe_public_ip
from app.core.ram_cache import RAMCacheManager
from app.core.user_config import UserConfig, decode_config, encode_config
from app.graphics import (
    BannerService,
    TennisPosterService,
    TheSportsDBService,
    compute_badge_state,
)
from app.ingestion import (
    DirettaService,
    ItalianEPGService,
    PipelineScheduler,
    build_catalog_buckets,
)
from app.main import create_app
from app.models.domain import (
    DIRETTA_POST_MATCH_MS,
    SKY_EPG_POST_END_MS,
    EPGProgram,
    Match,
    ProviderEvent,
    SportSilo,
    StreamSource,
    TeamInfo,
)
from app.providers import (
    DaddyLiveService,
    Fctv33Service,
    MandraKodiService,
    ProviderHarvestResult,
    SponService,
    StreamFreeService,
    StreamOrchestrator,
    TvVooService,
)


def _test_settings(tmp_path: Path, **overrides: Any) -> Settings:
    data_dir = tmp_path / "data"
    defaults: dict[str, Any] = {
        "APP_ENV": "test",
        "DATA_DIR": data_dir,
        "DB_PATH": data_dir / "streamsport.db",
        "POSTER_CACHE_DIR": data_dir / "posters",
        "BASE_POSTER_CACHE_DIR": data_dir / "posters_base",
        "MEDIA_DIR": Path(__file__).resolve().parent.parent / "media",
        "TENNIS_ASSETS_DIR": Path(__file__).resolve().parent.parent / "media" / "Tennis" / "assets",
        "TENNIS_BG_DIR": Path(__file__).resolve().parent.parent / "media" / "Tennis" / "backgrounds",
        "MATCH_RETENTION_HOURS": 6,
    }
    defaults.update(overrides)
    return Settings(**defaults)


def _make_match(
    match_id: str,
    title: str,
    *,
    silo: SportSilo = SportSilo.CALCIO_ITA,
    category: str = "calcio_ita",
    genres: list[str] | None = None,
    date_ms: int,
    end_ms: int,
    competition: str = "Serie A",
    canonical_competition: str = "serie-a",
    popular: bool = False,
    sources: list[StreamSource] | None = None,
    has_tvvoo: bool = False,
    home: str = "",
    away: str = "",
    slug_h: str = "",
    slug_a: str = "",
    source_origin: str = "diretta",
) -> Match:
    return Match(
        id=match_id,
        title=title,
        category=category,
        silo=silo,
        genres=genres or ["Serie A"],
        date=date_ms,
        end_date=end_ms,
        popular=popular,
        competition=competition,
        canonical_competition=canonical_competition,
        home_team=TeamInfo(name=home or title, slug=slug_h) if (home or title) else None,
        away_team=TeamInfo(name=away, slug=slug_a) if away else None,
        sources=sources or [],
        has_tvvoo=has_tvvoo,
        source_origin=source_origin,
        updated_at=date_ms,
    )


# ---------------------------------------------------------------------------
# 1. Database WAL mode, busy_timeout == 30000, upsert_matches, 6-hour purge
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_database_wal_busy_timeout_upsert_and_6h_purge(tmp_path: Path) -> None:
    db_path = tmp_path / "data" / "streamsport.db"
    db = DatabaseManager(db_path=db_path)
    await db.connect()
    try:
        conn = db._require_db()
        async with conn.execute("PRAGMA journal_mode;") as cur:
            row = await cur.fetchone()
            assert row is not None
            assert str(row[0]).lower() == "wal"

        async with conn.execute("PRAGMA busy_timeout;") as cur:
            row = await cur.fetchone()
            assert row is not None
            assert int(row[0]) == 30000

        now_ms = int(time.time() * 1000)

        m_dir_expired = _make_match(
            "diretta-old",
            "Old Match vs Expired",
            date_ms=now_ms - 180 * 60_000,
            end_ms=now_ms - 31 * 60_000,  # > 30m Diretta grace -> purged
            source_origin="diretta",
        )
        m_dir_grace = _make_match(
            "diretta-postmatch",
            "Inter vs Milan",
            date_ms=now_ms - 140 * 60_000,
            end_ms=now_ms - 25 * 60_000,  # <= 30m Diretta grace -> kept
            home="Inter",
            away="Milan",
            source_origin="diretta",
        )
        m_sky_expired = _make_match(
            "sky-old",
            "Old Sky Broadcast",
            date_ms=now_ms - 120 * 60_000,
            end_ms=now_ms - 16 * 60_000,  # > 15m Sky EPG grace -> purged
            source_origin="epg",
        )
        m_sky_grace = _make_match(
            "sky-postend",
            "Active Sky Post-End",
            date_ms=now_ms - 90 * 60_000,
            end_ms=now_ms - 10 * 60_000,  # <= 15m Sky EPG grace -> kept
            source_origin="epg",
        )
        m_cancel = _make_match(
            "diretta-cancel",
            "Cancelled Match",
            date_ms=now_ms + 3600_000,
            end_ms=now_ms + 7200_000,
            source_origin="diretta",
        )

        await db.upsert_matches([m_dir_expired, m_dir_grace, m_sky_expired, m_sky_grace, m_cancel])
        deleted_ids = await db.delete_matches(["diretta-cancel", "missing-id"])
        assert deleted_ids == ["diretta-cancel"]

        purged_ids = await db.purge_expired(now_ms=now_ms)
        assert set(purged_ids) == {"diretta-old", "sky-old"}

        remaining = await db.load_active_matches(now_ms=now_ms)
        assert {m.id for m in remaining} == {"diretta-postmatch", "sky-postend"}
    finally:
        await db.close()


# ---------------------------------------------------------------------------
# 2. Anti-SSRF IP check & DoHResolver mocked test
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_anti_ssrf_and_doh_resolver() -> None:
    assert is_safe_public_ip("8.8.8.8") is True
    assert is_safe_public_ip("1.1.1.1") is True
    for blocked in ("127.0.0.1", "192.168.1.1", "172.16.0.2", "169.254.169.254", "10.0.0.5"):
        assert is_safe_public_ip(blocked) is False

    # DoHResolver with mocked Cloudflare/Google JSON responses: rejects sinkhole, accepts public IP
    def handler(request: httpx.Request) -> httpx.Response:
        name = request.url.params.get("name", "")
        if name == "blocked.example":
            return httpx.Response(
                200,
                json={"Status": 0, "Answer": [{"type": 1, "TTL": 120, "data": "172.16.0.2"}]},
            )
        return httpx.Response(
            200,
            json={"Status": 0, "Answer": [{"type": 1, "TTL": 120, "data": "93.184.216.34"}]},
        )

    resolver = DoHResolver()
    resolver._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    try:
        resolved = await resolver.resolve("public.example")
        assert resolved == "93.184.216.34"

        with pytest.raises(httpcore.ConnectError):
            await resolver.resolve("blocked.example")

        with pytest.raises(httpcore.ConnectError):
            await resolver.resolve("127.0.0.1")
    finally:
        await resolver.aclose()


# ---------------------------------------------------------------------------
# 3. PipelineScheduler.run_once(): persistence, ghost EPG filter, RAM snapshot, cleanup
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pipeline_scheduler_run_once_and_poster_cleanup(tmp_path: Path) -> None:
    settings = _test_settings(tmp_path)
    settings.POSTER_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    settings.BASE_POSTER_CACHE_DIR.mkdir(parents=True, exist_ok=True)

    db = DatabaseManager(db_path=settings.DB_PATH)
    await db.connect()
    ram_cache = RAMCacheManager()

    now_ms = int(time.time() * 1000)

    # Pre-seed an expired match + poster files to verify 6h purge + cleanup_poster_files
    expired_match = _make_match(
        "diretta-expired",
        "Expired Event",
        date_ms=now_ms - 10 * 3600_000,
        end_ms=now_ms - 7 * 3600_000,
    )
    await db.upsert_matches([expired_match])
    stale_base = settings.BASE_POSTER_CACHE_DIR / "diretta-expired.jpg"
    stale_banner = settings.POSTER_CACHE_DIR / "diretta-expired__abc123.jpg"
    stale_base.write_bytes(b"stale-base")
    stale_banner.write_bytes(b"stale-banner")

    live_match = _make_match(
        "diretta-juve-milan",
        "Juventus - AC Milan",
        date_ms=now_ms - 10 * 60_000,
        end_ms=now_ms + 120 * 60_000,
        home="Juventus",
        away="AC Milan",
        popular=True,
    )

    epg_with_stream = EPGProgram(
        id="sky_1",
        channel_id="sky_sport_uno",
        channel_name="Sky Sport Uno",
        title="Diretta Gol Serie A",
        start_ms=now_ms - 5 * 60_000,
        end_ms=now_ms + 90 * 60_000,
        sport_hint="calcio",
        is_simulcast=True,
        source="sky",
    )
    epg_ghost = EPGProgram(
        id="sky_2",
        channel_id="canale-fantasma",
        channel_name="Canale Fantasma",
        title="Diretta Gol Serie B",
        start_ms=now_ms - 5 * 60_000,
        end_ms=now_ms + 90 * 60_000,
        sport_hint="calcio",
        is_simulcast=True,
        source="sky",
    )

    class MockDiretta(DirettaService):
        async def fetch_schedule(self, **kwargs: Any) -> tuple[list[Match], frozenset[str]]:
            return [live_match], frozenset({"roma"})

    class MockEPG(ItalianEPGService):
        async def fetch_epg(self, active_tennis_cities: frozenset[str] = frozenset(), **kwargs: Any) -> list[EPGProgram]:
            return [epg_with_stream, epg_ghost]

    class MockTSDB(TheSportsDBService):
        async def sync_calendar(self, force: bool = False) -> dict[str, list[dict[str, Any]]]:
            self.calendar_by_silo = {
                "football": [
                    {
                        "title": "Juventus vs AC Milan",
                        "home": "Juventus",
                        "away": "AC Milan",
                        "league": "Serie A",
                        "date_ms": live_match.date,
                        "poster_url": "https://www.thesportsdb.com/images/media/event/juv_mil.jpg",
                        "silo": "football",
                        "is_women": False,
                        "is_youth": False,
                    }
                ]
            }
            return self.calendar_by_silo

    class MockTvVooProvider(TvVooService):
        async def harvest(self) -> ProviderHarvestResult:
            return ProviderHarvestResult(
                provider="tvvoo",
                linear_channels={
                    "sky sport uno": [
                        StreamSource(
                            provider="tvvoo",
                            title="[⚡] SKY SPORT UNO (ITA FHD)",
                            url="/live/manifest.m3u8?url=https%3A%2F%2Fvavoo.to%2Fvavoo-iptv%2Fplay%2F111",
                            raw_url="https://vavoo.to/vavoo-iptv/play/111",
                            language="ITA",
                            quality="FHD",
                            priority=1,
                            requires_proxy=False,
                            channel_name="sky sport uno",
                        )
                    ]
                },
            )

    class MockSponProvider(SponService):
        async def harvest(self) -> ProviderHarvestResult:
            return ProviderHarvestResult(
                provider="spon",
                events=[
                    ProviderEvent(
                        provider="spon",
                        title="Juventus vs AC Milan",
                        home="Juventus",
                        away="AC Milan",
                        start_ms=live_match.date,
                        streams=[
                            StreamSource(
                                provider="spon",
                                title="HD7 ITALIAN",
                                url="https://spon.example/hd7",
                                language="it",
                                quality="HD",
                                requires_proxy=True,
                                proxy_host="sportsonline",
                            )
                        ],
                    )
                ],
            )

    tvvoo = MockTvVooProvider(settings=settings)
    spon = MockSponProvider(settings=settings)
    orchestrator = StreamOrchestrator(providers=[tvvoo, spon], settings=settings)
    tsdb = MockTSDB(settings=settings)
    banner = BannerService(settings=settings, tsdb_service=tsdb)

    scheduler = PipelineScheduler(
        settings=settings,
        db=db,
        ram_cache=ram_cache,
        diretta=MockDiretta(settings=settings),
        epg=MockEPG(settings=settings),
        orchestrator=orchestrator,
        tsdb=tsdb,
        banner=banner,
    )

    try:
        active_count = await scheduler.run_once(now_ms=now_ms)
        assert active_count == 2  # live_match + epg_with_stream (epg_ghost filtered out!)

        assert not stale_base.exists()
        assert not stale_banner.exists()

        # Verify RAM snapshot
        cached_juve = ram_cache.get_match("streamsport:diretta-juve-milan")
        assert cached_juve is not None
        assert cached_juve.poster == "https://www.thesportsdb.com/images/media/event/juv_mil.jpg"
        assert len(cached_juve.sources) == 1
        assert cached_juve.sources[0].provider == "spon"

        # Ghost EPG must NOT be present, valid EPG with TvVoo stream MUST be present
        all_ids = {m.id for m in ram_cache.snapshot.active_matches}
        assert all("canale-fantasma" not in mid and "evento-senza-flussi" not in mid for mid in all_ids)
        assert any(m.source_origin == "epg" and m.has_tvvoo for m in ram_cache.snapshot.active_matches)

        # Verify startup hydration from DB into a fresh RAMCacheManager
        fresh_cache = RAMCacheManager()
        scheduler_2 = PipelineScheduler(
            settings=settings,
            db=db,
            ram_cache=fresh_cache,
            diretta=MockDiretta(settings=settings),
            epg=MockEPG(settings=settings),
            orchestrator=orchestrator,
            tsdb=tsdb,
            banner=banner,
        )
        hydrated = await scheduler_2.hydrate_from_db(now_ms=now_ms)
        assert hydrated == 2
        assert fresh_cache.get_match("diretta-juve-milan") is not None
    finally:
        await orchestrator.aclose()
        await tsdb.aclose()
        await db.close()


# ---------------------------------------------------------------------------
# 4 & 5. Stremio vs Nuvio manifests, /catalog, /meta, /stream (0-disk RAM),
#        EasyProxy ClearKey/SPON/DLHD/StreamFree/FCTV33, TvVoo & countdown
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stremio_and_nuvio_routes_ram_only_and_stream_formatting(tmp_path: Path) -> None:
    settings = _test_settings(tmp_path)
    app = create_app(settings_override=settings)

    now_ms = int(time.time() * 1000)
    live_match = _make_match(
        "diretta-inter-napoli",
        "Inter vs Napoli",
        silo=SportSilo.CALCIO_ITA,
        category="calcio_italiano",
        genres=["Serie A"],
        date_ms=now_ms - 10 * 60_000,
        end_ms=now_ms + 125 * 60_000,
        popular=True,
        has_tvvoo=True,
        home="Inter",
        away="Napoli",
        sources=[
            StreamSource(
                provider="tvvoo",
                title="[⚡] SKY SPORT CALCIO (ITA FHD)",
                url="/live/manifest.m3u8?url=https%3A%2F%2Fvavoo.to%2Fvavoo-iptv%2Fplay%2F999",
                raw_url="https://vavoo.to/vavoo-iptv/play/999",
                language="ITA",
                quality="FHD",
                priority=1,
                requires_proxy=False,
                channel_name="sky sport calcio",
            ),
            StreamSource(
                provider="mandrakodi",
                title="Sky Sport Calcio ClearKey",
                url="https://cdn.example/stream.mpd",
                clearkey="abcd1234:ef567890",
                headers={"User-Agent": "CustomUA"},
                requires_proxy=True,
            ),
            StreamSource(
                provider="spon",
                title="[SPON] 🇮🇹 [720p] HD7",
                url="https://spon.example/hd7",
                language="it",
                requires_proxy=True,
                proxy_host="",
            ),
            StreamSource(
                provider="daddylive",
                title="DLHD Ch 10",
                url="https://dlhd.example/stream-10.php",
                requires_proxy=True,
                proxy_host="dlstreams",
            ),
            StreamSource(
                provider="streamfree",
                title="StreamFree FHD",
                url="https://streamfree.example/live.m3u8",
                headers={"Referer": "https://streamfree.example/", "Origin": "https://streamfree.example"},
                requires_proxy=True,
                proxy_host="",
            ),
            StreamSource(
                provider="fctv33",
                title="FCTV33 Edge",
                url="4424653:969541",
                requires_proxy=True,
                proxy_host="fctv33",
            ),
        ],
    )
    upcoming_match = _make_match(
        "diretta-roma-lazio",
        "Roma vs Lazio",
        silo=SportSilo.CALCIO_ITA,
        category="calcio_italiano",
        genres=["Serie A"],
        date_ms=now_ms + 90 * 60_000,
        end_ms=now_ms + 225 * 60_000,
        home="Roma",
        away="Lazio",
        sources=[
            StreamSource(
                provider="tvvoo",
                title="[⚡] DAZN 1 (ITA FHD)",
                url="/live/manifest.m3u8?url=https%3A%2F%2Fvavoo.to%2Fvavoo-iptv%2Fplay%2F777",
                raw_url="https://vavoo.to/vavoo-iptv/play/777",
                language="ITA",
                requires_proxy=False,
                channel_name="dazn 1",
            ),
            StreamSource(
                provider="spon",
                title="[SPON] 🇮🇹 [720p] HD7",
                url="https://spon.example/future",
                requires_proxy=True,
                proxy_host="",
            ),
        ],
    )
    coppa_match = _make_match(
        "diretta-coppa",
        "Atalanta vs Bologna",
        silo=SportSilo.CALCIO_ITA,
        category="calcio_italiano",
        genres=["Coppa Italia e Supercoppa"],
        date_ms=now_ms + 120 * 60_000,
        end_ms=now_ms + 255 * 60_000,
        competition="Coppa Italia",
        canonical_competition="coppa-italia",
        home="Atalanta",
        away="Bologna",
    )

    cfg_token = encode_config(
        UserConfig(
            ep_url="https://ep.example.com",
            ep_pass="secret123",
            prefer_direct_tvvoo=True,
            show_easyproxy_tvvoo=True,
        )
    )

    async with app.router.lifespan_context(app):
        all_matches = [live_match, upcoming_match, coppa_match]
        buckets, genres = build_catalog_buckets(all_matches, now_ms=now_ms)
        app.state.ram_cache.update_snapshot(
            active_matches=all_matches,
            matches_by_catalog=buckets,
            genres_by_catalog=genres,
            updated_at=now_ms,
        )

        # Prove zero-disk RAM serving: close SQLite connection completely before serving Stremio/Nuvio routes!
        await app.state.db.close()

        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            # Stremio manifest (11 nested genre catalogs)
            r_stremio = await client.get("/manifest.json")
            assert r_stremio.status_code == 200
            m_stremio = r_stremio.json()
            assert m_stremio["id"] == "org.streamsport.addon"
            assert [c["id"] for c in m_stremio["catalogs"]] == [
                "calcio_italiano",
                "calcio_estero",
                "tennis",
                "motori",
                "basket",
                "volley",
                "football_americano",
                "baseball",
                "hockey",
                "combattimento",
                "altri_sport",
            ]
            assert any(
                any(ex.get("name") == "genre" and ex.get("options") for ex in c.get("extra", []))
                for c in m_stremio["catalogs"]
            )

            # Nuvio manifest (flattened catalogs without nested genre dropdowns)
            r_nuvio = await client.get("/nuvio/manifest.json")
            assert r_nuvio.status_code == 200
            m_nuvio = r_nuvio.json()
            assert m_nuvio["id"] == "org.streamsport.nuvio"
            nuvio_ids = [c["id"] for c in m_nuvio["catalogs"]]
            assert "nuvio_calcio_italiano_serie_a" in nuvio_ids
            for c in m_nuvio["catalogs"]:
                assert all(ex.get("name") != "genre" for ex in c.get("extra", []))

            # Configured manifests
            assert (await client.get(f"/{cfg_token}/manifest.json")).json()["id"] == "org.streamsport.addon"
            assert (await client.get(f"/{cfg_token}/nuvio/manifest.json")).json()["id"] == "org.streamsport.nuvio"

            # Catalog + genre filter + Nuvio flattened catalog + X-Forwarded-* base_url honoring
            r_cat = await client.get(
                "/catalog/tv/calcio_italiano.json",
                headers={"X-Forwarded-Proto": "https", "X-Forwarded-Host": "addon.example.org"},
            )
            assert r_cat.status_code == 200
            assert r_cat.headers.get("cache-control") == "no-store"
            metas = r_cat.json()["metas"]
            assert len(metas) == 3
            assert metas[0]["poster"].startswith("https://addon.example.org/poster/")

            r_genre = await client.get(f"/{cfg_token}/catalog/tv/calcio_italiano/genre=Serie%20A.json")
            assert r_genre.status_code == 200
            genre_metas = r_genre.json()["metas"]
            assert len(genre_metas) == 2
            assert all(m["id"] in ("streamsport:diretta-inter-napoli", "streamsport:diretta-roma-lazio") for m in genre_metas)

            r_nuvio_cat = await client.get(f"/{cfg_token}/nuvio/catalog/tv/nuvio_calcio_italiano_serie_a.json")
            assert r_nuvio_cat.status_code == 200
            assert len(r_nuvio_cat.json()["metas"]) == 2

            # Meta route
            r_meta = await client.get(f"/{cfg_token}/meta/tv/streamsport:diretta-inter-napoli.json")
            assert r_meta.status_code == 200
            assert r_meta.json()["meta"]["name"] == "Inter vs Napoli"

            # Stream route (live match with all 6 providers)
            r_stream = await client.get(f"/{cfg_token}/stream/tv/streamsport:diretta-inter-napoli.json")
            assert r_stream.status_code == 200
            streams = r_stream.json()["streams"]
            urls = [s.get("url", "") for s in streams]
            assert any(f"/{cfg_token}/live/manifest.m3u8?url=" in u for u in urls)
            assert any("host=vavoo" in u and "api_password=secret123" in u for u in urls)
            assert any("/proxy/mpd/manifest.m3u8" in u and "clearkey=abcd1234%3Aef567890" in u for u in urls)
            assert any("/proxy/manifest.m3u8?url=https%3A%2F%2Fspon.example%2Fhd7" in u for u in urls)
            assert any(f"/{cfg_token}/daddylive/stream/10.m3u8" in u for u in urls)
            assert any("/proxy/manifest.m3u8?url=https%3A%2F%2Fstreamfree.example%2Flive.m3u8" in u and "h_Referer=" in u and "h_Origin=" in u for u in urls)
            assert any("host=fctv33" in u and "4424653%3A969541" in u for u in urls)

            # Pre-match countdown (> 20m before start)
            r_pre = await client.get(f"/{cfg_token}/stream/tv/streamsport:diretta-roma-lazio.json")
            assert r_pre.status_code == 200
            pre_streams = r_pre.json()["streams"]
            assert pre_streams[0]["name"].startswith("⏳ Tra ")
            assert "Europe/Rome" in pre_streams[0]["title"]


# ---------------------------------------------------------------------------
# 6. /live/manifest.m3u8 CDN rewriting (0% video bandwidth) & HTTP 307 fallback
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_live_manifest_rewriting_and_307_easyproxy_fallback(tmp_path: Path) -> None:
    settings = _test_settings(tmp_path)
    app = create_app(settings_override=settings)

    async with app.router.lifespan_context(app):
        mgr = app.state.tvvoo.live_manifest_manager
        seen_client_ips: list[str | None] = []

        async def mock_resolve(play_url: str, client_ip: str | None = None) -> tuple[str, float]:
            seen_client_ips.append(client_ip)
            if "fail" in play_url:
                raise RuntimeError("Upstream mirror down")
            return "https://cdn-edge.vavoo.example/hls/ch1/index.m3u8", time.time() + 1800

        async def mock_fetch_playlist(stream_url: str) -> str:
            return (
                "#EXTM3U\n"
                "#EXT-X-VERSION:3\n"
                '#EXT-X-KEY:METHOD=AES-128,URI="keys/enc.key"\n'
                "#EXTINF:4.0,\n"
                "seg001.ts\n"
            )

        mgr.resolve_play_url = mock_resolve  # type: ignore[method-assign]
        mgr._fetch_playlist_text = mock_fetch_playlist  # type: ignore[method-assign]

        cfg_token = encode_config(UserConfig(ep_url="https://ep.example.org", ep_pass="pw99"))

        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            r_ok = await client.get(
                "/live/manifest.m3u8",
                params={"url": "https://vavoo.to/vavoo-iptv/play/12345"},
                headers={"X-Forwarded-For": "10.0.0.2, 8.8.4.4"},
            )
            assert r_ok.status_code == 200
            assert "application/vnd.apple.mpegurl" in r_ok.headers["content-type"]
            assert seen_client_ips[-1] == "8.8.4.4"
            body = r_ok.text
            assert 'URI="https://cdn-edge.vavoo.example/hls/ch1/keys/enc.key"' in body
            assert "https://cdn-edge.vavoo.example/hls/ch1/seg001.ts" in body

            # Direct resolve failure -> 307 redirect to EasyProxy host=vavoo
            r_fail = await client.get(
                f"/{cfg_token}/live/manifest.m3u8",
                params={"url": "https://vavoo.to/vavoo-iptv/play/fail99"},
                follow_redirects=False,
            )
            assert r_fail.status_code == 307
            location = r_fail.headers["location"]
            assert location.startswith("https://ep.example.org/extractor/video.m3u8?host=vavoo")
            assert "api_password=pw99" in location


# ---------------------------------------------------------------------------
# 7. /poster/{match_id}.jpg rendering 889x500 JPEG with LIVE + TvVoo Ribbon
#    using real media/Tennis assets
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_tennis_poster_endpoint_with_real_media_assets_live_and_tvvoo_ribbon(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _test_settings(tmp_path)
    app = create_app(settings_override=settings)

    now_ms = int(time.time() * 1000)
    tennis_match = _make_match(
        "diretta-sinner-alcaraz",
        "Jannik Sinner - Carlos Alcaraz",
        silo=SportSilo.TENNIS,
        category="tennis",
        genres=["Grande Slam"],
        date_ms=now_ms - 5 * 60_000,
        end_ms=now_ms + 180 * 60_000,
        competition="ATP - SINGOLARE: Wimbledon (Regno Unito)",
        canonical_competition="wimbledon",
        home="Sinner J.",
        away="Alcaraz C.",
        slug_h="sinner-jannik",
        slug_a="alcaraz-carlos",
        has_tvvoo=True,
    )

    # Mock remote ESPN/ATP/WTA calls to return None so TennisPosterService uses real media/Tennis silhouettes + bg_wimbledon.jpg
    import app.graphics.tennis_poster as tp_mod

    async def mock_none(*args: Any, **kwargs: Any) -> Any:
        return None

    monkeypatch.setattr(tp_mod, "_fetch_espn_cutout", mock_none)
    monkeypatch.setattr(tp_mod, "_fetch_atp_cutout", mock_none)
    monkeypatch.setattr(tp_mod, "_fetch_wta_cutout", mock_none)

    async with app.router.lifespan_context(app):
        buckets, genres = build_catalog_buckets([tennis_match], now_ms=now_ms)
        app.state.ram_cache.update_snapshot(
            active_matches=[tennis_match],
            matches_by_catalog=buckets,
            genres_by_catalog=genres,
            updated_at=now_ms,
        )

        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            resp = await client.get("/poster/diretta-sinner-alcaraz.jpg")
            assert resp.status_code == 200
            assert resp.headers["content-type"] == "image/jpeg"

            with Image.open(io.BytesIO(resp.content)) as img:
                assert img.format == "JPEG"
                assert img.size == (889, 500)
                # Top-left LIVE pill area (#E5192D -> high R, low G/B)
                live_px = img.getpixel((70, 36))
                assert live_px[0] > 180 and live_px[1] < 80 and live_px[2] < 80
                # Top-right TvVoo golden ribbon area (#F5C518 -> high R & G, low B)
                gold_pixels = [
                    img.getpixel((x, y))
                    for y in range(0, 136)
                    for x in range(889 - 136, 889)
                ]
                gold_count = sum(
                    1 for r, g, b in gold_pixels if r > 180 and g > 150 and b < 80
                )
                assert gold_count > 500


# ---------------------------------------------------------------------------
# 8. /configure HTML UI, /api/encode-config, and /health JSON
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_configure_ui_and_health_endpoints(tmp_path: Path) -> None:
    settings = _test_settings(tmp_path)
    app = create_app(settings_override=settings)

    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            r_health = await client.get("/health")
            assert r_health.status_code == 200
            assert r_health.json()["status"] == "ok"
            assert r_health.json()["matches"] == 0

            r_root = await client.get("/", follow_redirects=False)
            assert r_root.status_code == 307
            assert r_root.headers["location"] == "/configure"

            r_cfg = await client.get("/configure")
            assert r_cfg.status_code == 200
            assert "text/html" in r_cfg.headers["content-type"]
            assert "StreamSport" in r_cfg.text
            assert "Installa su Stremio" in r_cfg.text
            assert "Installa su Nuvio" in r_cfg.text

            r_api = await client.post(
                "/api/encode-config",
                json={
                    "ep_url": "https://ep.test",
                    "ep_pass": "pw1",
                    "prefer_direct_tvvoo": True,
                    "show_easyproxy_tvvoo": False,
                },
            )
            assert r_api.status_code == 200
            data = r_api.json()
            decoded = decode_config(data["config"], settings)
            assert decoded.ep_url == "https://ep.test"
            assert decoded.ep_pass == "pw1"
            assert decoded.show_easyproxy_tvvoo is False
            assert data["stremio_url"].startswith("stremio://")
            assert data["nuvio_url"].endswith("/nuvio/manifest.json")


# ---------------------------------------------------------------------------
# 9. All ENABLE_*=false toggle checks
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_all_enable_toggles_false(tmp_path: Path) -> None:
    settings = _test_settings(
        tmp_path,
        ENABLE_DIRETTA=False,
        ENABLE_SKY_EPG=False,
        ENABLE_THESPORTSDB=False,
        ENABLE_TENNIS_POSTERS=False,
        ENABLE_TVVOO=False,
        ENABLE_MANDRAKODI=False,
        ENABLE_SPON=False,
        ENABLE_DADDYLIVE=False,
        ENABLE_STREAMFREE=False,
        ENABLE_FCTV33=False,
    )

    diretta = DirettaService(settings=settings)
    sched, cities = await diretta.fetch_schedule()
    assert sched == [] and cities == frozenset()

    epg = ItalianEPGService(settings=settings)
    assert await epg.fetch_epg() == []

    tsdb = TheSportsDBService(settings=settings)
    assert await tsdb.sync_calendar() == {}
    sample_match = _make_match(
        "diretta-t1",
        "Sample vs Match",
        silo=SportSilo.TENNIS,
        category="tennis",
        date_ms=int(time.time() * 1000),
        end_ms=int(time.time() * 1000) + 3600_000,
    )
    assert tsdb.reconcile_match(sample_match) is None

    # TennisPosterService disabled -> falls back to dark canvas (still valid 1225x700 JPEG)
    tennis_svc = TennisPosterService(settings=settings)
    tennis_bytes = await tennis_svc.render_match_poster(sample_match)
    with Image.open(io.BytesIO(tennis_bytes)) as img:
        assert img.size == (1225, 700)

    providers = [
        TvVooService(settings=settings),
        MandraKodiService(settings=settings),
        SponService(settings=settings),
        DaddyLiveService(settings=settings),
        StreamFreeService(settings=settings),
        Fctv33Service(settings=settings),
    ]
    assert all(not p.enabled for p in providers)

    orchestrator = StreamOrchestrator(providers=providers, settings=settings)
    assert await orchestrator.harvest_all() == {}
    await orchestrator.aclose()
    await tsdb.aclose()
    await tennis_svc.aclose()


@pytest.mark.asyncio
async def test_evolved_taxonomy_graphics_routes_and_legacy_config(tmp_path: Path) -> None:
    import base64
    from starlette.requests import Request
    from app.graphics.thesportsdb_service import _leagues_compatible, format_thumb_url
    from app.graphics.tennis_poster import _extract_surname, _is_player_match, _resolve_country_iso2
    from app.routers._utils import compute_base_url
    from app.taxonomy import (
        DEFAULT_TEAM_MATCHER,
        canonicalize_channel_name,
        classify_event,
        detect_channel_flag,
        normalize_team_name,
        resolve_canonical_channel,
        server_tag_rank,
    )
    from app.taxonomy.classifier import resolve_sport_silo
    from app.taxonomy.matcher import _string_similarity

    # 1. Channels taxonomy
    assert resolve_canonical_channel("Arena Sport 1 HD") is None
    assert resolve_canonical_channel("DAZN 1 CA") is None
    assert canonicalize_channel_name("Sky Sport Uno FHD .c") == "sky sport uno"
    assert server_tag_rank("SKY .c") < server_tag_rank("SKY .s") < server_tag_rank("SKY .b")
    assert detect_channel_flag("Sky Sports Premier League UK") == "🇬🇧"
    assert detect_channel_flag("Sky Sport Calcio IT") == "🇮🇹"

    # 2. Aliases, Matcher, Classifier
    assert normalize_team_name("København") == "fc kobenhavn"
    assert normalize_team_name("FC København") == "fc kobenhavn"
    assert normalize_team_name("Bodø/Glimt") == "bodo glimt"
    assert normalize_team_name("Juventus Next Gen") == "juventus next gen"
    assert _string_similarity("ostia mare", "ostiamare") == 0.98
    ms_res = DEFAULT_TEAM_MATCHER.match_score(
        "F1 GP Monaco - Qualifiche",
        "",
        "Formula 1 Monaco Qualifying",
        "",
    )
    assert ms_res.matched is True
    assert classify_event(title="Diretta Gol Serie A", silo=SportSilo.CALCIO_ITA) == ("calcio_italiano", ["Serie A"])
    assert classify_event(title="Konferenz Bundesliga", silo="") == ("calcio_estero", ["Bundesliga e Ligue 1"])
    assert resolve_sport_silo("", "", "F2 Feature Race Imola") == "motori"

    # 3. Graphics hardening
    serie_a_match = _make_match(
        "m-sa",
        "Inter vs Milan",
        date_ms=int(time.time() * 1000),
        end_ms=int(time.time() * 1000) + 7200_000,
        competition="Italian Serie A",
        canonical_competition="serie-a",
    )
    assert _leagues_compatible(serie_a_match, "Italian Serie B") is False
    assert _leagues_compatible(serie_a_match, "Italian Serie A") is True
    assert format_thumb_url("https://www.thesportsdb.com/images/media/event/thumb/abc.jpg/small") == (
        "https://www.thesportsdb.com/images/media/event/thumb/abc.jpg/medium"
    )
    assert _extract_surname("Etcheverry T. M.") == "ETCHEVERRY"
    assert _is_player_match("Jannik Sinner", "Jannik Sinner") is True
    assert _is_player_match("A. Muller", "Gilles Muller") is False
    assert _resolve_country_iso2("Team Europe") is not None

    banner = BannerService(settings=_test_settings(tmp_path, ENABLE_THESPORTSDB=False))
    nostream_match = serie_a_match.model_copy(update={"poster": "https://cdn.example/nostream_placeholder.jpg"})
    raw_poster = await banner._resolve_base_poster(nostream_match, db=None)
    assert len(raw_poster) > 100  # Rendered dark canvas instead of fetching nostream placeholder

    # 4. Legacy pipe-delimited UserConfig + PUBLIC_URL/_HOST_REGEX + SSRF 403 & DaddyLive 307
    pipe_tok = base64.urlsafe_b64encode(b"https://legacy-ep.test|legacy_pw|stremio").rstrip(b"=").decode("ascii")
    legacy_cfg = decode_config(pipe_tok, Settings())
    assert legacy_cfg.ep_url == "https://legacy-ep.test"
    assert legacy_cfg.ep_pass == "legacy_pw"

    scope = {
        "type": "http",
        "method": "GET",
        "path": "/manifest.json",
        "scheme": "http",
        "server": ("internal.local", 8000),
        "headers": [
            (b"x-forwarded-proto", b"https"),
            (b"x-forwarded-host", b"evil.host/steal@foo"),
            (b"host", b"safe.example.org"),
        ],
        "app": create_app(settings_override=_test_settings(tmp_path)),
    }
    assert compute_base_url(Request(scope)) == "https://safe.example.org"

    pub_app = create_app(settings_override=_test_settings(tmp_path, PUBLIC_URL="https://public.streamsport.test/"))
    scope["app"] = pub_app
    assert compute_base_url(Request(scope)) == "https://public.streamsport.test"

    app = create_app(settings_override=_test_settings(tmp_path))
    async with app.router.lifespan_context(app):
        dl = next(p for p in app.state.orchestrator.providers if isinstance(p, DaddyLiveService))
        dl._player_cache[("10", "https://ep.test")] = (
            time.monotonic(),
            "https://ep.test/extractor/video.m3u8?host=dlstreams&d=https%3A%2F%2Fdlhd.dad%2Fwatch.php%3Fid%3D10&redirect_stream=true",
        )
        cfg_tok = encode_config(UserConfig(ep_url="https://ep.test", ep_pass="pw1"))
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            r_ssrf = await client.get("/live/manifest.m3u8", params={"url": "http://127.0.0.1/internal"})
            assert r_ssrf.status_code == 403

            r_dl = await client.get(f"/{cfg_tok}/daddylive/stream/10.m3u8", follow_redirects=False)
            assert r_dl.status_code == 307
            assert "host=dlstreams" in r_dl.headers["location"]

    from datetime import datetime, timezone

    default_settings = Settings(_env_file=None, APP_ENV="test", DATA_DIR=tmp_path / "sync_cfg")
    assert default_settings.SYNC_INTERVAL_SECONDS == 3600
    assert default_settings.SYNC_INTERVAL_SECONDS_NIGHT == 14400
    assert default_settings.EPG_SYNC_INTERVAL_SECONDS == 7200

    sched_db = DatabaseManager(default_settings.DB_PATH)
    sched_ram = RAMCacheManager()
    sched = PipelineScheduler(
        settings=default_settings,
        db=sched_db,
        ram_cache=sched_ram,
        diretta=DirettaService(settings=default_settings),
        epg=ItalianEPGService(settings=default_settings),
        orchestrator=StreamOrchestrator([], settings=default_settings),
        tsdb=TheSportsDBService(settings=default_settings),
        banner=BannerService(settings=default_settings),
    )
    assert sched.get_current_sync_interval(datetime(2026, 11, 23, 2, 0, tzinfo=timezone.utc)) == 14400
    assert sched.get_current_sync_interval(datetime(2026, 11, 23, 10, 59, tzinfo=timezone.utc)) == 14400
    assert sched.get_current_sync_interval(datetime(2026, 11, 23, 11, 0, tzinfo=timezone.utc)) == 3600
    assert sched.get_current_sync_interval(datetime(2026, 11, 23, 17, 0, tzinfo=timezone.utc)) == 3600
    await sched.stop()
    await sched._orchestrator.aclose()
    await sched._tsdb.aclose()


@pytest.mark.asyncio
async def test_two_state_catalog_realtime_ram_expiry_badge_and_immediate_discard(tmp_path: Path) -> None:
    now_ms = int(time.time() * 1000)

    # 1. Two-state catalog (LIVE vs IN ARRIVO only; expired matches excluded immediately) + LIVE badge during grace
    m_dir_grace = _make_match(
        "diretta-grace",
        "Diretta Post-Match",
        silo=SportSilo.FOOTBALL,
        category="calcio_estero",
        genres=["Premier League"],
        date_ms=now_ms - 135 * 60_000,
        end_ms=now_ms - 25 * 60_000,  # +30m grace -> active & LIVE for 5m more
        source_origin="diretta",
    )
    m_sky_grace = _make_match(
        "sky-grace",
        "Sky Active Post-End",
        silo=SportSilo.FOOTBALL,
        category="calcio_estero",
        genres=["Premier League"],
        date_ms=now_ms - 120 * 60_000,
        end_ms=now_ms - 10 * 60_000,  # +15m grace -> active & LIVE for 5m more
        source_origin="epg",
    )
    m_expired_already = _make_match(
        "diretta-done",
        "Already Finished Match",
        silo=SportSilo.FOOTBALL,
        category="calcio_estero",
        genres=["Premier League"],
        date_ms=now_ms - 180 * 60_000,
        end_ms=now_ms - 35 * 60_000,  # > 30m -> must never sit at bottom of catalog
        source_origin="diretta",
    )
    m_upcoming = _make_match(
        "diretta-next",
        "Upcoming Match",
        silo=SportSilo.FOOTBALL,
        category="calcio_estero",
        genres=["Premier League"],
        date_ms=now_ms + 90 * 60_000,
        end_ms=now_ms + 225 * 60_000,
        source_origin="diretta",
    )

    assert compute_badge_state(m_dir_grace, now_ms=now_ms)[0] == "LIVE"
    assert compute_badge_state(m_sky_grace, now_ms=now_ms)[0] == "LIVE"

    buckets, _ = build_catalog_buckets(
        [m_upcoming, m_expired_already, m_sky_grace, m_dir_grace],
        now_ms=now_ms,
    )
    bucket_ids = [m.id for m in buckets["calcio_estero"]]
    assert bucket_ids == ["diretta-grace", "sky-grace", "diretta-next"]

    ram = RAMCacheManager()
    ram.update_snapshot(
        active_matches=[m_dir_grace, m_sky_grace, m_upcoming],
        matches_by_catalog=buckets,
        genres_by_catalog={},
        updated_at=now_ms,
    )
    # 6 minutes later: both grace windows expired (sky at +15m, diretta at +30m); only upcoming stays in RAM catalog
    later_ms = now_ms + 6 * 60_000
    assert SKY_EPG_POST_END_MS == 15 * 60_000 and DIRETTA_POST_MATCH_MS == 30 * 60_000
    live_cat = ram.get_catalog("calcio_estero", now_ms=later_ms)
    assert [m.id for m in live_cat] == ["diretta-next"]

    # 2. PipelineScheduler.run_once deletes last_discarded_match_ids from SQLite + cleans posters
    settings = _test_settings(tmp_path)
    settings.POSTER_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    settings.BASE_POSTER_CACHE_DIR.mkdir(parents=True, exist_ok=True)

    db = DatabaseManager(db_path=settings.DB_PATH)
    await db.connect()
    postponed = _make_match(
        "diretta-postponed",
        "Postponed Game",
        date_ms=now_ms + 3600_000,
        end_ms=now_ms + 7200_000,
    )
    await db.upsert_matches([postponed])
    p_base = settings.BASE_POSTER_CACHE_DIR / "diretta-postponed.jpg"
    p_base.write_bytes(b"img")

    class MockDiscardDiretta(DirettaService):
        async def fetch_schedule(self, **kwargs: Any) -> tuple[list[Match], frozenset[str]]:
            self.last_discarded_match_ids = frozenset({"diretta-postponed"})
            return [m_upcoming], frozenset()

    class EmptyEPG(ItalianEPGService):
        async def fetch_epg(self, active_tennis_cities: frozenset[str] = frozenset(), **kwargs: Any) -> list[EPGProgram]:
            return []

    tsdb = TheSportsDBService(settings=settings)
    banner = BannerService(settings=settings, tsdb_service=tsdb)
    orchestrator = StreamOrchestrator([], settings=settings)
    sched_ram = RAMCacheManager()
    scheduler = PipelineScheduler(
        settings=settings,
        db=db,
        ram_cache=sched_ram,
        diretta=MockDiscardDiretta(settings=settings),
        epg=EmptyEPG(settings=settings),
        orchestrator=orchestrator,
        tsdb=tsdb,
        banner=banner,
    )
    try:
        count = await scheduler.run_once(now_ms=now_ms)
        assert count == 1
        assert not p_base.exists()
        assert [m.id for m in await db.load_active_matches(now_ms=now_ms)] == ["diretta-next"]
    finally:
        await orchestrator.aclose()
        await tsdb.aclose()
        await db.close()


@pytest.mark.asyncio
async def test_pipeline_scheduler_prunes_stale_db_rows_with_outage_guards(
    tmp_path: Path,
) -> None:
    settings = _test_settings(tmp_path, ENABLE_THESPORTSDB=False)
    settings.POSTER_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    settings.BASE_POSTER_CACHE_DIR.mkdir(parents=True, exist_ok=True)

    db = DatabaseManager(db_path=settings.DB_PATH)
    await db.connect()
    ram_cache = RAMCacheManager()
    now_ms = int(time.time() * 1000)

    stale_tivusat = _make_match(
        "tivusat_295_rosario",
        "Campionato Argentino Rosario Central-Gimnasia Mdz",
        date_ms=now_ms + 3600_000,
        end_ms=now_ms + 7200_000,
        source_origin="epg",
    )
    stale_sky = _make_match(
        "sky_11497_cerignola",
        "Audace Cerignola - Inter U23",
        date_ms=now_ms + 3600_000,
        end_ms=now_ms + 7200_000,
        source_origin="epg",
    )
    stale_diretta = _make_match(
        "diretta-old-challenger",
        "Old Challenger Match",
        silo=SportSilo.TENNIS,
        category="tennis",
        genres=["Challenger e Altri"],
        date_ms=now_ms + 3600_000,
        end_ms=now_ms + 7200_000,
        source_origin="diretta",
    )
    await db.upsert_matches([stale_tivusat, stale_sky, stale_diretta])

    stale_posters = [
        settings.BASE_POSTER_CACHE_DIR / "tivusat_295_rosario.jpg",
        settings.POSTER_CACHE_DIR / "sky_11497_cerignola__v1.jpg",
        settings.BASE_POSTER_CACHE_DIR / "diretta-old-challenger.jpg",
    ]
    for p in stale_posters:
        p.write_bytes(b"stale")

    live_match = _make_match(
        "diretta-live",
        "Inter vs Milan",
        date_ms=now_ms + 1800_000,
        end_ms=now_ms + 9000_000,
        home="Inter",
        away="Milan",
        source_origin="diretta",
    )
    epg_simulcast = EPGProgram(
        id="sky_sim_1",
        channel_id="sky_sport_uno",
        channel_name="Sky Sport Uno",
        title="Diretta Gol Serie A",
        start_ms=now_ms - 5 * 60_000,
        end_ms=now_ms + 90 * 60_000,
        sport_hint="calcio",
        is_live=True,
        is_simulcast=True,
        source="sky",
    )

    class StatefulDiretta(DirettaService):
        def __init__(self, **kwargs: Any) -> None:
            super().__init__(**kwargs)
            self.fail_partial = False

        async def fetch_schedule(self, **kwargs: Any) -> tuple[list[Match], frozenset[str]]:
            self.last_discarded_match_ids = frozenset()
            self.last_fetch_failed = False
            self.last_failed_feeds = 1 if self.fail_partial else 0
            return [live_match], frozenset()

    class StatefulEPG(ItalianEPGService):
        def __init__(self, **kwargs: Any) -> None:
            super().__init__(**kwargs)
            self.fail_all = False

        async def fetch_epg(
            self, active_tennis_cities: frozenset[str] = frozenset(), **kwargs: Any
        ) -> list[EPGProgram]:
            if self.fail_all:
                self.last_fetch_failed = True
                return []
            self.last_fetch_failed = False
            return [epg_simulcast]

    class MockTvVooProvider(TvVooService):
        async def harvest(self) -> ProviderHarvestResult:
            return ProviderHarvestResult(
                provider="tvvoo",
                linear_channels={
                    "sky sport uno": [
                        StreamSource(
                            provider="tvvoo",
                            title="SKY SPORT UNO",
                            url="/live/manifest.m3u8?url=https%3A%2F%2Fvavoo.to%2Fvavoo-iptv%2Fplay%2F111",
                            raw_url="https://vavoo.to/vavoo-iptv/play/111",
                            language="ITA",
                            quality="FHD",
                            priority=1,
                            requires_proxy=False,
                            channel_name="sky sport uno",
                        )
                    ]
                },
            )

    diretta_svc = StatefulDiretta(settings=settings)
    epg_svc = StatefulEPG(settings=settings)
    tvvoo = MockTvVooProvider(settings=settings)
    orchestrator = StreamOrchestrator(providers=[tvvoo], settings=settings)
    tsdb = TheSportsDBService(settings=settings)
    banner = BannerService(settings=settings, tsdb_service=tsdb)
    scheduler = PipelineScheduler(
        settings=settings,
        db=db,
        ram_cache=ram_cache,
        diretta=diretta_svc,
        epg=epg_svc,
        orchestrator=orchestrator,
        tsdb=tsdb,
        banner=banner,
    )

    try:
        # 1. Healthy cycle prunes all 3 stale DB rows + deletes their poster files
        await scheduler.run_once(now_ms=now_ms)
        expected_sim_id = f"sky_sky_sport_uno_diretta-gol-serie-a_{epg_simulcast.start_ms}"
        db_ids = {m.id for m in await db.load_active_matches(now_ms=now_ms)}
        ram_ids = {m.id for m in ram_cache.snapshot.active_matches}
        assert db_ids == {"diretta-live", expected_sim_id}
        assert ram_ids == db_ids
        assert all(not p.exists() for p in stale_posters)

        # 2. Partial Diretta feed failure preserves persisted Diretta rows while still pruning stale EPG rows
        diretta_kept = _make_match(
            "diretta-kept",
            "Kept On Partial Failure",
            date_ms=now_ms + 3600_000,
            end_ms=now_ms + 7200_000,
            source_origin="diretta",
        )
        sky_stale_2 = _make_match(
            "sky-stale-2",
            "Stale Sky Row 2",
            date_ms=now_ms + 3600_000,
            end_ms=now_ms + 7200_000,
            source_origin="epg",
        )
        await db.upsert_matches([diretta_kept, sky_stale_2])
        diretta_svc.fail_partial = True
        await scheduler.run_once(now_ms=now_ms)
        db_ids_partial = {m.id for m in await db.load_active_matches(now_ms=now_ms)}
        assert "diretta-kept" in db_ids_partial
        assert "sky-stale-2" not in db_ids_partial

        # 3. Total Sky EPG outage preserves persisted EPG rows while still pruning stale Diretta rows
        sky_kept = _make_match(
            "sky-kept",
            "Kept On Sky Outage",
            date_ms=now_ms + 3600_000,
            end_ms=now_ms + 7200_000,
            source_origin="epg",
        )
        await db.upsert_matches([sky_kept])
        diretta_svc.fail_partial = False
        epg_svc.fail_all = True
        scheduler._epg_cache = []
        scheduler._last_epg_sync_monotonic = 0.0
        await scheduler.run_once(now_ms=now_ms)
        db_ids_epg_outage = {m.id for m in await db.load_active_matches(now_ms=now_ms)}
        assert "sky-kept" in db_ids_epg_outage
        assert "diretta-kept" not in db_ids_epg_outage
    finally:
        await orchestrator.aclose()
        await tsdb.aclose()
        await db.close()



