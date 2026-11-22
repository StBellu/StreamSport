"""Unit tests for Phase 3 International Providers and StreamOrchestrator."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from urllib.parse import parse_qs, urlsplit

import httpx
import pytest

from app.config import Settings
from app.core.user_config import UserConfig
from app.models.domain import Match, ProviderEvent, SportSilo, StreamSource, TeamInfo
from app.providers.base import BaseStreamProvider, ProviderHarvestResult
from app.providers.daddylive_service import (
    MAX_CHANNELS_PER_EVENT,
    DaddyLiveService,
    parse_daddylive_channels,
    parse_daddylive_schedule,
)
from app.providers.fctv33_service import (
    Fctv33Service,
    parse_api_envelope,
    parse_common_params,
    parse_fctv33_catalog,
    parse_stream_item,
    read_fields,
    read_varint,
    rot47,
)
from app.providers.stream_orchestrator import StreamOrchestrator
from app.providers.streamfree_service import (
    FALLBACK_MIRROR,
    StreamFreeService,
    extract_mirror_url,
    parse_streamfree_matches,
)


# ---------------------------------------------------------------------------
# Protobuf wire encoders for FCTV33 binary fixture construction
# ---------------------------------------------------------------------------

def _pb_varint(value: int) -> bytes:
    out = bytearray()
    v = value
    while True:
        byte = v & 0x7F
        v >>= 7
        if v:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            break
    return bytes(out)


def _pb_field_varint(field: int, value: int) -> bytes:
    return _pb_varint((field << 3) | 0) + _pb_varint(value)


def _pb_field_bytes(field: int, data: bytes) -> bytes:
    return _pb_varint((field << 3) | 2) + _pb_varint(len(data)) + data


def _pb_field_str(field: int, text: str) -> bytes:
    return _pb_field_bytes(field, text.encode("utf-8"))


# ---------------------------------------------------------------------------
# DaddyLiveService tests
# ---------------------------------------------------------------------------

def test_daddylive_json_parse_30_cap_and_language_priority() -> None:
    channels = [
        {"channel_name": f"World Feed {i}", "channel_id": str(100 + i)}
        for i in range(35)
    ]
    channels.append({"channel_name": "TNT Sports 1 UK", "channel_id": "31"})
    channels.append({"channel_name": "Sky Sport Calcio IT", "channel_id": "870"})
    channels.append({"channel_name": "DAZN 1 ITA", "channel_id": "871"})

    payload = json.dumps(
        {
            "Saturday 22nd Nov 2026 - Schedule Time UK GMT": {
                "Soccer": [
                    {
                        "time": "19:45",
                        "event": "Inter vs Juventus",
                        "channels": channels,
                    }
                ]
            }
        }
    )

    events = parse_daddylive_schedule(payload, "https://dlhd.dad")
    assert len(events) == 1
    ev = events[0]
    assert ev.provider == "daddylive"
    assert ev.home == "Inter"
    assert ev.away == "Juventus"
    assert ev.sport_hint == "football"
    expected_ms = int(datetime(2026, 11, 22, 19, 45, tzinfo=timezone.utc).timestamp() * 1000)
    assert ev.start_ms == expected_ms

    # Capped at 30, Italian channels first, then English, then others
    assert len(ev.streams) == MAX_CHANNELS_PER_EVENT == 30
    assert ev.streams[0].channel_name == "Sky Sport Calcio IT"
    assert ev.streams[1].channel_name == "DAZN 1 ITA"
    assert ev.streams[2].channel_name == "TNT Sports 1 UK"
    assert ev.streams[0].url == "https://dlhd.dad/watch.php?id=870"
    assert ev.streams[0].requires_proxy is True
    assert ev.streams[0].proxy_host == "dlstreams"


def test_daddylive_html_parse_and_24_7_channels() -> None:
    html = """
    <div class="schedule__dayTitle">2026-11-22</div>
    <div class="schedule__catHeader">Tennis</div>
    <div class="schedule__event">
      <span class="schedule__time">14:30</span>
      <span class="schedule__eventTitle">Jannik Sinner vs Carlos Alcaraz</span>
      <a href="/watch.php?id=55">ESPN UK</a>
      <a href="/watch.php?id=88">SuperTennis IT</a>
    </div>
    """
    events = parse_daddylive_schedule(html, "https://dlhd.dad/")
    assert len(events) == 1
    ev = events[0]
    assert ev.home == "Jannik Sinner"
    assert ev.away == "Carlos Alcaraz"
    assert ev.sport_hint == "tennis"
    assert ev.streams[0].url == "https://dlhd.dad/watch.php?id=88"
    assert ev.streams[1].url == "https://dlhd.dad/watch.php?id=55"

    ch_html = """
    <div class="grid">
      <a href="/watch.php?id=870">Sky Sport Calcio IT</a>
      <a href="/stream/stream-871.php">DAZN 1 HD</a>
    </div>
    """
    channels = parse_daddylive_channels(ch_html, "https://dlhd.dad")
    assert "sky sport calcio" in channels
    assert "dazn 1" in channels
    assert channels["sky sport calcio"][0].url == "https://dlhd.dad/watch.php?id=870"
    assert channels["dazn 1"][0].url == "https://dlhd.dad/watch.php?id=871"


@pytest.mark.asyncio
async def test_daddylive_service_harvest_and_toggle() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if "schedule-generated.php" in request.url.path:
            return httpx.Response(
                200,
                json={
                    "2026-11-22": {
                        "Soccer": [
                            {
                                "time": "20:00",
                                "event": "Milan vs Napoli",
                                "channels": [{"channel_name": "DAZN IT", "channel_id": "99"}],
                            }
                        ]
                    }
                },
            )
        if "24-7-channels.php" in request.url.path:
            return httpx.Response(200, text='<a href="/watch.php?id=99">DAZN 1</a>')
        return httpx.Response(404)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        svc = DaddyLiveService(
            Settings(ENABLE_DADDYLIVE=True),
            client=client,
            mirrors=("https://dlhd.dad",),
        )
        res = await svc.harvest()
        assert res.provider == "daddylive"
        assert len(res.events) == 1
        assert "dazn 1" in res.linear_channels

        disabled = DaddyLiveService(Settings(ENABLE_DADDYLIVE=False), client=client)
        assert disabled.enabled is False
        disabled_res = await disabled.safe_harvest()
        assert disabled_res.events == []


# ---------------------------------------------------------------------------
# StreamFreeService tests
# ---------------------------------------------------------------------------

def test_streamfree_mirror_and_parse_no_0x_extraction() -> None:
    assert extract_mirror_url({"domains": ["strmfree.example"]}) == "https://strmfree.example"
    assert extract_mirror_url({}, FALLBACK_MIRROR) == FALLBACK_MIRROR

    payload = {
        "matches": [
            {
                "category": "football",
                "slug": "inter-vs-juventus",
                "home": "Inter",
                "away": "Juventus",
                "competition": "Serie A",
                "timestamp": 1795377600,
                "quality": "1080p",
            }
        ]
    }
    events = parse_streamfree_matches(payload, "https://streamfree.top")
    assert len(events) == 1
    ev = events[0]
    assert ev.provider == "streamfree"
    assert ev.home == "Inter"
    assert ev.away == "Juventus"
    assert ev.streams[0].url == "https://streamfree.top/player/football/inter-vs-juventus"
    assert ev.streams[0].requires_proxy is True
    assert ev.streams[0].proxy_host == "streamfree"


@pytest.mark.asyncio
async def test_streamfree_service_harvest_discovers_mirror() -> None:
    seen_urls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen_urls.append(str(request.url))
        if request.url.host == "strmfree.link":
            return httpx.Response(200, json={"domains": ["sf-mirror.test"]})
        if request.url.host == "sf-mirror.test" and request.url.path == "/api/v1/matches":
            return httpx.Response(
                200,
                json={
                    "events": [
                        {
                            "category": "basketball",
                            "slug": "lakers-vs-celtics",
                            "title": "Los Angeles Lakers vs Boston Celtics",
                            "start_ms": 1795381200000,
                        }
                    ]
                },
            )
        return httpx.Response(404)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        svc = StreamFreeService(Settings(ENABLE_STREAMFREE=True), client=client)
        res = await svc.harvest()
        assert len(res.events) == 1
        assert res.events[0].streams[0].url == "https://sf-mirror.test/player/basketball/lakers-vs-celtics"
        assert all("/embed/" not in u and "get-stream-key" not in u for u in seen_urls)


# ---------------------------------------------------------------------------
# Fctv33Service tests
# ---------------------------------------------------------------------------

def test_fctv33_protobuf_readers_and_rot47_handshake() -> None:
    raw_cfg = json.dumps(
        {
            "sfver": "20261122",
            "common:web:client": json.dumps(
                {"trd": {"iframePlayerDomains": ["player.fctv.test"]}}
            ),
        }
    )
    encoded = rot47(raw_cfg)
    assert rot47(encoded) == raw_cfg
    parsed_cfg = parse_common_params(encoded)
    assert parsed_cfg["stream_digit"] == "trd"
    assert parsed_cfg["player_referer"] == "https://player.fctv.test/"
    assert parsed_cfg["sfver"] == "20261122"

    stream_msg = (
        _pb_field_varint(1, 98765)
        + _pb_field_str(3, "HD English")
        + _pb_field_varint(9, 2003)
    )
    stream_parsed = parse_stream_item(stream_msg)
    assert stream_parsed == {
        "streamId": "98765",
        "name": "HD English",
        "url": "",
        "siteType": 2003,
    }

    match_msg = (
        _pb_field_varint(1, 54321)
        + _pb_field_varint(2, 1)
        + _pb_field_str(3, "Serie A")
        + _pb_field_str(4, "Inter")
        + _pb_field_str(5, "Juventus")
        + _pb_field_varint(6, 1795377600)
        + _pb_field_bytes(8, stream_msg)
    )
    envelope = _pb_field_str(3, "Success") + _pb_field_bytes(10, match_msg)

    val, idx = read_varint(_pb_varint(300))
    assert val == 300 and idx == 2
    assert "3" not in read_fields(envelope) or isinstance(read_fields(envelope)[3][0], bytes)
    env = parse_api_envelope(envelope)
    assert env["message"] == "Success"

    events = parse_fctv33_catalog(envelope, default_sport_type=1)
    assert len(events) == 1
    ev = events[0]
    assert ev.provider == "fctv33"
    assert ev.home == "Inter"
    assert ev.away == "Juventus"
    assert ev.competition == "Serie A"
    assert ev.start_ms == 1795377600 * 1000
    assert ev.streams[0].url == "fctv33://54321/98765?sportType=1&siteType=2003"
    assert ev.streams[0].requires_proxy is True
    assert ev.streams[0].proxy_host == "fctv33"


@pytest.mark.asyncio
async def test_fctv33_service_harvest_handshake_and_catalog() -> None:
    stream_msg = _pb_field_varint(1, 777) + _pb_field_str(3, "Main Feed") + _pb_field_varint(9, 2001)
    match_msg = (
        _pb_field_varint(1, 112233)
        + _pb_field_varint(2, 3)
        + _pb_field_str(3, "ATP Finals")
        + _pb_field_str(4, "Jannik Sinner")
        + _pb_field_str(5, "Taylor Fritz")
        + _pb_field_varint(6, 1795380000)
        + _pb_field_bytes(8, stream_msg)
    )
    envelope = _pb_field_str(3, "Success") + _pb_field_bytes(10, match_msg)
    params_rot = rot47(
        json.dumps(
            {
                "sfver": "v99",
                "common:web:client": json.dumps({"seth": {"iframePlayerDomains": ["p.fctv.test"]}}),
            }
        )
    )

    def handler(request: httpx.Request) -> httpx.Response:
        if "domains.json" in str(request.url):
            return httpx.Response(
                200,
                json={"fctv33": "https://fctv33.site", "fctv33_api": "https://api.fctv33.test"},
            )
        if request.url.path == "/api/common/params":
            return httpx.Response(200, text=params_rot)
        if request.url.path == "/api/match/list":
            assert request.url.params.get("sfver") == "v99"
            if request.url.params.get("sportType") == "3":
                return httpx.Response(200, content=envelope)
            return httpx.Response(200, content=_pb_field_str(3, "Success"))
        return httpx.Response(404)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        svc = Fctv33Service(Settings(ENABLE_FCTV33=True), client=client, sport_types=(1, 3))
        res = await svc.harvest()
        assert len(res.events) == 1
        assert res.events[0].streams[0].url == "fctv33://112233/777?sportType=3&siteType=2001"


# ---------------------------------------------------------------------------
# StreamOrchestrator tests
# ---------------------------------------------------------------------------

class _StubProvider(BaseStreamProvider):
    def __init__(self, name: str, result: ProviderHarvestResult, *, enabled: bool = True, boom: bool = False):
        super().__init__(Settings())
        self.name = name
        self._result = result
        self._enabled = enabled
        self._boom = boom

    @property
    def enabled(self) -> bool:
        return self._enabled

    async def harvest(self) -> ProviderHarvestResult:
        if self._boom:
            raise RuntimeError("upstream boom")
        return self._result


@pytest.mark.asyncio
async def test_orchestrator_harvest_all_concurrent_and_resilient() -> None:
    p_ok = _StubProvider("tvvoo", ProviderHarvestResult(provider="tvvoo", raw_count=2))
    p_off = _StubProvider("spon", ProviderHarvestResult(provider="spon", raw_count=9), enabled=False)
    p_err = _StubProvider("fctv33", ProviderHarvestResult(provider="fctv33"), boom=True)

    orch = StreamOrchestrator([p_ok, p_off, p_err])
    res = await orch.harvest_all()
    assert set(res.keys()) == {"tvvoo", "fctv33"}
    assert res["tvvoo"].raw_count == 2
    assert "upstream boom" in res["fctv33"].error


def test_orchestrator_attach_streams_hierarchy_ghost_filter_and_has_tvvoo() -> None:
    kickoff_ms = 1795377600000
    diretta_match = Match(
        id="diretta-1",
        title="Inter - Juventus",
        category="Serie A",
        silo=SportSilo.CALCIO_ITA,
        date=kickoff_ms,
        end_date=kickoff_ms + 135 * 60_000,
        competition="Serie A",
        home_team=TeamInfo(name="Inter"),
        away_team=TeamInfo(name="Juventus"),
        alt_home="FC Internazionale",
        alt_away="Juventus FC",
        broadcasters=["DAZN 1", "Sky Sport Calcio"],
        source_origin="diretta",
    )
    epg_orphan_empty = Match(
        id="sky_9097_studio_1",
        title="Studio Live",
        category="Calcio",
        silo=SportSilo.CALCIO_ITA,
        date=kickoff_ms,
        end_date=kickoff_ms + 60 * 60_000,
        broadcasters=["Unknown Channel"],
        source_origin="epg",
    )
    epg_orphan_matched = Match(
        id="sky_9113_diretta_gol_1",
        title="Diretta Gol Serie A",
        category="Serie A",
        silo=SportSilo.CALCIO_ITA,
        date=kickoff_ms,
        end_date=kickoff_ms + 120 * 60_000,
        broadcasters=["Sky Sport Calcio"],
        source_origin="epg",
    )

    dlhd_extra = [
        StreamSource(
            provider="daddylive",
            title=f"World {i}",
            url=f"https://dlhd.dad/watch.php?id={200 + i}",
            channel_name=f"World {i}",
        )
        for i in range(35)
    ]

    harvest_results = {
        "tvvoo": ProviderHarvestResult(
            provider="tvvoo",
            linear_channels={
                "dazn 1": [
                    StreamSource(
                        provider="tvvoo",
                        title="DAZN 1 HD",
                        url="https://vavoo.to/play/dazn1",
                        raw_url="https://vavoo.to/play/dazn1",
                        language="it",
                        channel_name="DAZN 1",
                    )
                ],
                "sky sport calcio": [
                    StreamSource(
                        provider="tvvoo",
                        title="Sky Sport Calcio",
                        url="https://vavoo.to/play/skycalcio",
                        raw_url="https://vavoo.to/play/skycalcio",
                        language="it",
                        channel_name="Sky Sport Calcio",
                    )
                ],
            },
        ),
        "mandrakodi": ProviderHarvestResult(
            provider="mandrakodi",
            events=[
                ProviderEvent(
                    provider="mandrakodi",
                    title="Juventus vs Inter",
                    home="Juventus",
                    away="Inter",
                    start_ms=kickoff_ms + 10 * 60_000,
                    streams=[
                        StreamSource(
                            provider="mandrakodi",
                            title="MandraKodi MPD",
                            url="https://cdn.example/live.mpd",
                            clearkey="0011223344556677:8899aabbccddeeff",
                        ),
                        StreamSource(
                            provider="mandrakodi",
                            title="MandraKodi Ace",
                            url="acestream://abcdef123456",
                            is_acestream=True,
                        ),
                    ],
                ),
                # Women's match must NOT attach to senior men's Inter - Juventus
                ProviderEvent(
                    provider="mandrakodi",
                    title="Inter (W) vs Juventus (W)",
                    home="Inter",
                    away="Juventus",
                    start_ms=kickoff_ms,
                    is_women=True,
                    streams=[
                        StreamSource(
                            provider="mandrakodi",
                            title="Wrong Women Stream",
                            url="https://cdn.example/women.m3u8",
                        )
                    ],
                ),
            ],
        ),
        "spon": ProviderHarvestResult(
            provider="spon",
            events=[
                ProviderEvent(
                    provider="spon",
                    title="Inter vs Juventus",
                    home="Inter",
                    away="Juventus",
                    start_ms=kickoff_ms,
                    streams=[
                        StreamSource(
                            provider="spon",
                            title="HD2 ENGLISH",
                            url="https://sportsonline.st/hd2.php",
                            language="en",
                            proxy_host="sportsonline",
                        ),
                        StreamSource(
                            provider="spon",
                            title="HD7 ITALIAN",
                            url="https://sportsonline.st/hd7.php",
                            language="it",
                            proxy_host="sportsonline",
                        ),
                    ],
                )
            ],
        ),
        "daddylive": ProviderHarvestResult(
            provider="daddylive",
            events=[
                ProviderEvent(
                    provider="daddylive",
                    title="Inter vs Juventus",
                    home="Inter",
                    away="Juventus",
                    start_ms=kickoff_ms,
                    streams=dlhd_extra,
                )
            ],
        ),
        "streamfree": ProviderHarvestResult(
            provider="streamfree",
            events=[
                ProviderEvent(
                    provider="streamfree",
                    title="Inter vs Juventus",
                    home="Inter",
                    away="Juventus",
                    start_ms=kickoff_ms,
                    streams=[
                        StreamSource(
                            provider="streamfree",
                            title="StreamFree FHD",
                            url="https://streamfree.top/player/football/inter-juve",
                            proxy_host="streamfree",
                        )
                    ],
                )
            ],
        ),
        "fctv33": ProviderHarvestResult(
            provider="fctv33",
            events=[
                ProviderEvent(
                    provider="fctv33",
                    title="Inter vs Juventus",
                    home="Inter",
                    away="Juventus",
                    start_ms=kickoff_ms,
                    streams=[
                        StreamSource(
                            provider="fctv33",
                            title="FCTV33 Feed",
                            url="fctv33://54321/98765?sportType=1&siteType=2001",
                            proxy_host="fctv33",
                        ),
                        # Duplicate URL should be deduplicated
                        StreamSource(
                            provider="fctv33",
                            title="FCTV33 Feed Dup",
                            url="fctv33://54321/98765?sportType=1&siteType=2001",
                            proxy_host="fctv33",
                        ),
                    ],
                )
            ],
        ),
    }

    orch = StreamOrchestrator([])
    result = orch.attach_streams(
        [diretta_match, epg_orphan_empty, epg_orphan_matched],
        harvest_results,
    )

    ids = [m.id for m in result]
    assert "diretta-1" in ids
    assert "sky_9113_diretta_gol_1" in ids
    assert "sky_9097_studio_1" not in ids  # Orphan EPG ghost filtered out

    main = result[0]
    assert main.has_tvvoo is True
    urls = [s.url for s in main.sources]
    assert "https://cdn.example/women.m3u8" not in urls

    # Verify strict hierarchy: tvvoo -> mandrakodi -> spon (HD7 ITA first) -> daddylive (30) -> streamfree -> fctv33 -> acestream
    providers_seq = [
        ("acestream" if s.is_acestream else s.provider) for s in main.sources
    ]
    assert providers_seq[:2] == ["tvvoo", "tvvoo"]
    assert providers_seq[2] == "mandrakodi"
    assert providers_seq[3:5] == ["spon", "spon"]
    assert main.sources[3].title == "HD7 ITALIAN"
    dlhd_indices = [i for i, p in enumerate(providers_seq) if p == "daddylive"]
    assert len(dlhd_indices) == 30
    assert providers_seq[dlhd_indices[-1] + 1] == "streamfree"
    assert providers_seq[dlhd_indices[-1] + 2] == "fctv33"
    assert providers_seq[-1] == "acestream"
    assert len(urls) == len(set(urls))


def test_orchestrator_stremio_formatting_and_countdown() -> None:
    kickoff_ms = int(datetime(2026, 11, 22, 19, 45, tzinfo=timezone.utc).timestamp() * 1000)
    match = Match(
        id="diretta-inter",
        title="Inter - Juventus",
        category="Serie A",
        silo=SportSilo.CALCIO_ITA,
        date=kickoff_ms,
        end_date=kickoff_ms + 135 * 60_000,
        sources=[
            StreamSource(
                provider="tvvoo",
                title="DAZN 1",
                url="https://vavoo.to/play/1",
                raw_url="https://vavoo.to/play/1",
                language="it",
                channel_name="DAZN 1",
            ),
            StreamSource(
                provider="mandrakodi",
                title="MandraKodi ClearKey",
                url="https://cdn.example/stream.mpd",
                clearkey="aaa:bbb",
            ),
            StreamSource(
                provider="mandrakodi",
                title="MandraKodi HLS Proxy",
                url="https://cdn.example/stream.m3u8",
                proxy_host="hls",
                headers={"Referer": "https://cdn.example/"},
            ),
            StreamSource(
                provider="spon",
                title="HD7 ITALIAN",
                url="https://sportsonline.st/hd7.php",
                proxy_host="sportsonline",
                headers={"Referer": "https://sportsonline.st/"},
            ),
            StreamSource(
                provider="daddylive",
                title="Sky Sport Calcio",
                url="https://dlhd.dad/watch.php?id=870",
                proxy_host="dlstreams",
            ),
            StreamSource(
                provider="streamfree",
                title="StreamFree 1080p",
                url="https://streamfree.top/player/football/inter-juve",
                proxy_host="streamfree",
            ),
            StreamSource(
                provider="fctv33",
                title="FCTV33 HD",
                url="fctv33://54321/98765?sportType=1&siteType=2001",
                proxy_host="fctv33",
            ),
            StreamSource(
                provider="mandrakodi",
                title="AceStream P2P",
                url="acestream://deadbeef",
                is_acestream=True,
            ),
        ],
    )

    orch = StreamOrchestrator([])
    cfg = UserConfig(
        ep_url="https://ep.example.org",
        ep_pass="secret123",
        prefer_direct_tvvoo=True,
        show_easyproxy_tvvoo=True,
    )

    # 1. Pre-match (>20m before kickoff): countdown + linear TV channel only
    pre_items = orch.build_stremio_streams(
        match,
        cfg,
        "https://addon.example.com",
        config_segment="cfg123",
        now_ms=kickoff_ms - 90 * 60_000,
    )
    assert pre_items[0].name.startswith("⏳ Tra 1h 30m")
    assert "20:45 Europe/Rome" in (pre_items[0].title or "")
    # Pre-match includes linear TvVoo channels for pre-match studio
    assert any("/cfg123/live/manifest.m3u8?url=" in (it.url or "") for it in pre_items)
    assert not any("fctv33" in (it.url or "") for it in pre_items)

    # 2. Live window: all formatted streams
    live_items = orch.build_stremio_streams(
        match,
        cfg,
        "https://addon.example.com",
        now_ms=kickoff_ms + 30 * 60_000,
    )
    assert not any((it.name or "").startswith("⏳") for it in live_items)

    # TvVoo No-Freeze + EasyProxy
    assert live_items[0].url == "https://addon.example.com/live/manifest.m3u8?url=https%3A%2F%2Fvavoo.to%2Fplay%2F1"
    assert "[⚡]" in (live_items[0].title or "")
    ep_vavoo = parse_qs(urlsplit(live_items[1].url or "").query)
    assert ep_vavoo["host"] == ["vavoo"]
    assert ep_vavoo["redirect_stream"] == ["true"]
    assert ep_vavoo["api_password"] == ["secret123"]

    # MandraKodi ClearKey & MandraKodi HLS Proxy
    assert (live_items[2].url or "").startswith("https://ep.example.org/proxy/mpd/manifest.m3u8?")
    mk_qs = parse_qs(urlsplit(live_items[2].url or "").query)
    assert mk_qs["clearkey"] == ["aaa:bbb"]
    assert mk_qs["api_password"] == ["secret123"]

    assert (live_items[3].url or "").startswith("https://ep.example.org/proxy/hls/manifest.m3u8?")
    mk_hls_qs = parse_qs(urlsplit(live_items[3].url or "").query)
    assert mk_hls_qs["d"] == ["https://cdn.example/stream.m3u8"]
    assert mk_hls_qs["h_Referer"] == ["https://cdn.example/"]

    # SPON + headers
    spon_qs = parse_qs(urlsplit(live_items[4].url or "").query)
    assert spon_qs["host"] == ["sportsonline"]
    assert spon_qs["h_Referer"] == ["https://sportsonline.st/"]

    # DaddyLive, StreamFree, FCTV33
    assert parse_qs(urlsplit(live_items[5].url or "").query)["host"] == ["dlstreams"]
    assert parse_qs(urlsplit(live_items[6].url or "").query)["host"] == ["streamfree"]
    fctv_qs = parse_qs(urlsplit(live_items[7].url or "").query)
    assert fctv_qs["host"] == ["fctv33"]
    assert fctv_qs["d"] == ["fctv33://54321/98765?sportType=1&siteType=2001"]

    # AceStream at tail
    assert live_items[-1].externalUrl == "acestream://deadbeef"

    # 3. Missing ep_url yields single /configure prompt
    no_ep_items = orch.build_stremio_streams(
        match,
        UserConfig(ep_url="", prefer_direct_tvvoo=True, show_easyproxy_tvvoo=True),
        "https://addon.example.com",
        now_ms=kickoff_ms,
    )
    cfg_prompts = [it for it in no_ep_items if (it.externalUrl or "").endswith("/configure")]
    assert len(cfg_prompts) == 1

    # 4. Expired live window (>135m for football) returns []
    expired_items = orch.build_stremio_streams(
        match,
        cfg,
        "https://addon.example.com",
        now_ms=kickoff_ms + 136 * 60_000,
    )
    assert expired_items == []
