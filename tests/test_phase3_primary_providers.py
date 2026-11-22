"""Unit tests for Phase 3 Primary Stream Providers (TvVoo, MandraKodi, SPON)."""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import httpx
import pytest

from app.config import Settings
from app.providers.mandrakodi_service import (
    MandraKodiService,
    xor_decrypt,
    xor_encrypt,
)
from app.providers.spon_service import SponService, parse_prog_txt
from app.providers.tvvoo_service import (
    LiveManifestManager,
    TvVooService,
    normalize_channel_name,
    rewrite_addon_sig_ip,
    rewrite_hls_manifest,
)


def _make_addon_sig(ips: list[str], ip: str, valid_until: float) -> str:
    inner = json.dumps({"ips": ips, "ip": ip, "validUntil": valid_until})
    outer = json.dumps({"data": inner, "sig": "dummy"})
    return base64.b64encode(outer.encode("utf-8")).decode("ascii")


def _decode_addon_sig_data(sig: str) -> dict:
    padded = sig + "=" * (-len(sig) % 4)
    outer = json.loads(base64.b64decode(padded).decode("utf-8"))
    return json.loads(outer["data"])


# ---------------------------------------------------------------------------
# TvVoo / LiveManifestManager tests
# ---------------------------------------------------------------------------


def test_rewrite_hls_manifest_rewrites_all_relative_uris() -> None:
    raw = (
        "#EXTM3U\n"
        "#EXT-X-VERSION:3\n"
        '#EXT-X-KEY:METHOD=AES-128,URI="keys/enc.key"\n'
        '#EXT-X-MAP:URI="init.mp4"\n'
        '#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="aac",NAME="ITA",URI="audio/ita.m3u8"\n'
        "#EXTINF:4.0,\n"
        "seg_0001.ts?token=abc\n"
        "#EXTINF:4.0,\n"
        "/cdn/live/seg_0002.ts\n"
        "#EXTINF:4.0,\n"
        "https://other-cdn.example/seg_0003.ts\n"
    )
    base_url = "https://cdn1.vavoo.example/live/ch10/index.m3u8"
    out = rewrite_hls_manifest(raw, base_url)
    lines = out.splitlines()

    assert 'URI="https://cdn1.vavoo.example/live/ch10/keys/enc.key"' in lines[2]
    assert 'URI="https://cdn1.vavoo.example/live/ch10/init.mp4"' in lines[3]
    assert 'URI="https://cdn1.vavoo.example/live/ch10/audio/ita.m3u8"' in lines[4]
    assert lines[6] == "https://cdn1.vavoo.example/live/ch10/seg_0001.ts?token=abc"
    assert lines[8] == "https://cdn1.vavoo.example/cdn/live/seg_0002.ts"
    assert lines[10] == "https://other-cdn.example/seg_0003.ts"


def test_addon_sig_ip_rewrite_prioritizes_client_ip() -> None:
    sig = _make_addon_sig(["1.1.1.1", "2.2.2.2"], "1.1.1.1", time.time() + 1200)
    rewritten = rewrite_addon_sig_ip(sig, "93.44.10.20")
    data = _decode_addon_sig_data(rewritten)
    assert data["ips"] == ["93.44.10.20", "1.1.1.1", "2.2.2.2"]
    assert data["ip"] == "93.44.10.20"


@pytest.mark.asyncio
async def test_live_manifest_manager_proactive_refresh_throttle_and_single_flight() -> None:
    ping_calls = 0
    resolve_calls = 0
    manifest_calls = 0
    seen_ip_locations: list[str | None] = []
    seen_sig_ips: list[list[str]] = []

    now = time.time()
    # First token has only 120s left (< 5 min proactive threshold)
    expiring_sig = _make_addon_sig(["10.0.0.1"], "10.0.0.1", now + 120)
    fresh_sig = _make_addon_sig(["10.0.0.1"], "10.0.0.1", now + 1200)

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal ping_calls, resolve_calls, manifest_calls
        url = str(request.url)

        if "vypn.net/api/app/ping" in url:
            ping_calls += 1
            body = json.loads(request.content.decode("utf-8"))
            seen_ip_locations.append(body.get("ipLocation"))
            # Return expiring sig on first call, fresh sig on subsequent calls
            sig = expiring_sig if ping_calls == 1 else fresh_sig
            return httpx.Response(200, json={"addonSig": sig})

        if "/mediahubmx-resolve.json" in url:
            resolve_calls += 1
            sig_hdr = request.headers.get("mediahubmx-signature", "")
            seen_sig_ips.append(_decode_addon_sig_data(sig_hdr)["ips"])
            return httpx.Response(
                200,
                json=[{"url": f"https://cdn.vavoo.test/hls/stream_{resolve_calls}/index.m3u8"}],
            )

        if "cdn.vavoo.test/hls/" in url:
            manifest_calls += 1
            return httpx.Response(
                200,
                text=(
                    "#EXTM3U\n"
                    '#EXT-X-KEY:METHOD=AES-128,URI="key.bin"\n'
                    "#EXTINF:4.0,\n"
                    "chunk_1.ts\n"
                ),
                headers={"content-type": "application/vnd.apple.mpegurl"},
            )

        return httpx.Response(404)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        mgr = LiveManifestManager(client=client, throttle_seconds=1.5)
        play_url = "https://vavoo.to/vavoo-iptv/play/123456"

        # 5 concurrent callers -> single-flight should perform 1 ping + 1 resolve
        results = await asyncio.gather(
            *[mgr.get_live_manifest(play_url, "82.50.10.5") for _ in range(5)]
        )
        assert all(
            "https://cdn.vavoo.test/hls/stream_1/chunk_1.ts" in r for r in results
        )
        assert all(
            'URI="https://cdn.vavoo.test/hls/stream_1/key.bin"' in r for r in results
        )
        assert ping_calls == 1
        assert resolve_calls == 1
        assert seen_ip_locations[0] == "82.50.10.5"
        assert seen_sig_ips[0][0] == "82.50.10.5"

        # Fast throttle (< 1.5s): returns cached manifest AND triggers proactive background renewal (<5m left)
        throttled = await mgr.get_live_manifest(play_url, "82.50.10.5")
        assert throttled == results[0]
        assert manifest_calls == 1  # No extra CDN playlist fetch due to 1.5s throttle

        # Yield to event loop so non-blocking proactive background refresh completes
        await asyncio.sleep(0.05)
        assert ping_calls == 2
        assert resolve_calls == 2
        snap = mgr.get_session_snapshot(play_url, "82.50.10.5")
        assert snap is not None
        assert "stream_2" in snap.stream_url

        # Idle session cleanup (> 45 min)
        snap.last_fetched_at = time.time() - (46 * 60)
        removed = mgr.cleanup_idle_sessions()
        assert removed == 1
        assert mgr.get_session_snapshot(play_url, "82.50.10.5") is None

        await mgr.aclose()


@pytest.mark.asyncio
async def test_live_manifest_manager_v2_resolve_and_mirror_rotation() -> None:
    seen_hosts: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        host = urlparse(url).netloc
        if "/live/resolve" in url:
            seen_hosts.append(host)
            if host == "voltop.net":
                return httpx.Response(503)
            return httpx.Response(200, json={"url": "https://edge.vavoo.test/v2/live/playlist.m3u8"})
        if "edge.vavoo.test" in url:
            return httpx.Response(200, text="#EXTM3U\n#EXTINF:2.0,\nseg0.ts\n")
        return httpx.Response(404)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        mgr = LiveManifestManager(client=client)
        out = await mgr.get_live_manifest("https://vavoo.to/live2/play3/998877", None)
        assert "https://edge.vavoo.test/v2/live/seg0.ts" in out
        assert "voltop.net" in seen_hosts
        assert "vavoo.net" in seen_hosts
        await mgr.aclose()


@pytest.mark.asyncio
async def test_tvvoo_service_harvest_normalization_and_fallbacks(tmp_path: Path) -> None:
    assert normalize_channel_name("SKY SPORT UNO FHD .c") == "sky sport uno"
    assert normalize_channel_name("SKY SPORT CALCIO (1) .s .b") == "sky sport calcio"
    assert normalize_channel_name("RAI 1 HD") == "rai 1"
    assert normalize_channel_name("IT: DAZN 1 FHD") == "dazn 1"

    m3u_file = tmp_path / "lista-it.m3u"
    m3u_file.write_text(
        "#EXTM3U\n"
        '#EXTINF:-1 group-title="Sport",SKY SPORT F1 FHD\n'
        "https://vavoo.to/vavoo-iptv/play/f1play\n"
        '#EXTINF:-1 group-title="Sport",SKY SPORT UNO (D)\n'
        "https://dlhd.dad/watch.php?id=100\n",
        encoding="utf-8",
    )

    sig = _make_addon_sig(["1.1.1.1"], "1.1.1.1", time.time() + 1200)

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if "vypn.net/api/app/ping" in url:
            return httpx.Response(200, json={"addonSig": sig})
        if "/mediahubmx-catalog.json" in url:
            body = json.loads(request.content.decode("utf-8"))
            grp = body["filter"]["group"]
            if grp == "United Kingdom":
                return httpx.Response(
                    200,
                    json={
                        "items": [
                            {"name": "SKY SPORTS F1 HD .c", "url": "https://vavoo.to/vavoo-iptv/play/ukf1"}
                        ]
                    },
                )
            return httpx.Response(200, json={"items": []})
        if "/live2/index" in url:
            return httpx.Response(
                200,
                json=[
                    {"group": "Germany", "name": "SKY SPORT BUNDESLIGA 1 FHD", "url": "https://vavoo.to/live2/play3/555"}
                ],
            )
        return httpx.Response(404)

    settings = Settings(ENABLE_TVVOO=True)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        svc = TvVooService(settings=settings, client=client, m3u_fallback_path=m3u_file)
        res = await svc.safe_harvest()

    assert res.error == ""
    assert "sky sport f1" in res.linear_channels
    ita_src = res.linear_channels["sky sport f1"][0]
    assert ita_src.priority == 1
    assert ita_src.language == "ITA"
    assert ita_src.requires_proxy is False
    assert ita_src.raw_url == "https://vavoo.to/vavoo-iptv/play/f1play"
    assert parse_qs(urlparse(ita_src.url).query)["url"][0] == ita_src.raw_url

    assert "sky sports f1" in res.linear_channels
    assert res.linear_channels["sky sports f1"][0].priority == 10
    assert "sky sport bundesliga 1" in res.linear_channels
    assert res.linear_channels["sky sport bundesliga 1"][0].language == "DEU"

    # Disabled toggle test
    disabled_svc = TvVooService(settings=Settings(ENABLE_TVVOO=False))
    disabled_res = await disabled_svc.safe_harvest()
    assert disabled_res.linear_channels == {}
    assert disabled_res.events == []


# ---------------------------------------------------------------------------
# MandraKodiService tests
# ---------------------------------------------------------------------------


def test_xor_encrypt_decrypt_roundtrip() -> None:
    secret = "mandrakodi_secret_key"
    plaintext = '{"channels": [{"title": "Sky Sport Calcio", "accent": "Città"}]}'
    encrypted = xor_encrypt(plaintext, secret)
    assert encrypted != plaintext
    assert xor_decrypt(encrypted, secret) == plaintext
    assert xor_decrypt(encrypted.encode("ascii"), secret) == plaintext

    # Also support raw XOR bytes directly
    key_b = secret.encode("utf-8")
    raw_xor = bytes(b ^ key_b[i % len(key_b)] for i, b in enumerate(b"hello-mk"))
    assert xor_decrypt(raw_xor, secret) == "hello-mk"


@pytest.mark.asyncio
async def test_mandrakodi_graceful_skip_when_api_url_empty(caplog: pytest.LogCaptureFixture) -> None:
    settings = Settings(ENABLE_MANDRAKODI=True, MANDRAKODI_API_URL="")
    svc = MandraKodiService(settings=settings)
    with caplog.at_level(logging.WARNING):
        res = await svc.safe_harvest()
    assert res.error == ""
    assert res.events == []
    assert res.linear_channels == {}
    assert any("MANDRAKODI_API_URL is empty" in rec.message for rec in caplog.records)

    disabled = MandraKodiService(settings=Settings(ENABLE_MANDRAKODI=False, MANDRAKODI_API_URL="https://example.invalid"))
    res_disabled = await disabled.safe_harvest()
    assert res_disabled.events == []
    assert res_disabled.linear_channels == {}


@pytest.mark.asyncio
async def test_mandrakodi_mpd_clearkey_dtt_socceron_and_acestream() -> None:
    payload = {
        "items": [
            {
                "title": "Sky Sport Uno FHD",
                "mpd_url": "https://cdn.sky.test/uno/manifest.mpd",
                "clearkey": "00112233445566778899aabbccddeeff:ffeeddccbbaa99887766554433221100",
                "category": "sky",
            },
            {
                "title": "Rai 1 HD",
                "url": "https://mediapolis.rai.test/relinker/rai1.m3u8",
                "category": "dtt",
            },
            {
                "title": "D. Medvedev vs F. Cobolli",
                "url": "https://socceron.test/live/court1.m3u8",
                "category": "socceron",
                "competition": "ATP Masters 1000 Roma",
                "start_ms": 1763834400000,
            },
            {
                "title": "Inter (Donne) vs Juventus Women",
                "url": "acestream://1234567890abcdef1234567890abcdef12345678",
                "category": "event",
                "competition": "Serie A Femminile",
                "start_ms": 1763838000000,
            },
        ]
    }
    encrypted_body = xor_encrypt(json.dumps(payload), "test-mk-pass")

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers.get("X-Device-Id") == "dev-123"
        return httpx.Response(200, text=encrypted_body)

    settings = Settings(
        ENABLE_MANDRAKODI=True,
        MANDRAKODI_API_URL="https://mk.test/api/feed",
        MANDRAKODI_PASSWORD="test-mk-pass",
        MANDRAKODI_DEVICE_ID="dev-123",
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        svc = MandraKodiService(settings=settings, client=client)
        res = await svc.safe_harvest()

    assert res.error == ""
    assert "sky sport uno" in res.linear_channels
    mpd_src = res.linear_channels["sky sport uno"][0]
    assert mpd_src.proxy_host == "mpd"
    assert mpd_src.requires_proxy is True
    assert mpd_src.clearkey == "00112233445566778899aabbccddeeff:ffeeddccbbaa99887766554433221100"
    assert mpd_src.priority == 2

    assert "rai 1" in res.linear_channels
    dtt_src = res.linear_channels["rai 1"][0]
    assert dtt_src.requires_proxy is False
    assert dtt_src.priority == 2

    events_by_title = {e.title: e for e in res.events}
    tennis_ev = events_by_title["D. Medvedev vs F. Cobolli"]
    assert tennis_ev.home == "D. Medvedev"
    assert tennis_ev.away == "F. Cobolli"
    assert tennis_ev.is_women is False
    assert tennis_ev.sport_hint == "tennis"

    women_ev = events_by_title["Inter (Donne) vs Juventus Women"]
    assert women_ev.is_women is True
    assert women_ev.streams[0].is_acestream is True
    assert women_ev.streams[0].priority == 7
    assert women_ev.streams[0].url == "acestream://1234567890abcdef1234567890abcdef12345678"


# ---------------------------------------------------------------------------
# SponService tests
# ---------------------------------------------------------------------------


def test_parse_prog_txt_prioritizes_italian_channels() -> None:
    sample_prog = """
HD1 ENGLISH
HD2 ENGLISH
HD7 ITALIAN
HD8 ITALIAN
BR1 PORTUGUESE

SUNDAY
19:45   Serie A: Inter x Juventus | https://sportsonline.st/channels/hd/hd1.php
19:45   Serie A: Inter x Juventus | https://sportsonline.st/channels/hd/hd7.php
19:45   Serie A: Inter x Juventus | https://sportsonline.st/channels/pt/br1.php
21:00   NBA: Lakers vs Celtics | https://sportsonline.st/channels/hd/hd8.php
MONDAY
20:00   Premier League: Arsenal x Chelsea | https://sportsonline.st/channels/hd/hd2.php
"""
    now_utc = datetime(2026, 11, 22, 12, 0, tzinfo=timezone.utc)  # Sunday
    events = parse_prog_txt(sample_prog, now_utc=now_utc)
    assert len(events) == 3

    inter_ev = events[0]
    assert inter_ev.home == "Inter"
    assert inter_ev.away == "Juventus"
    assert inter_ev.competition == "Serie A"
    assert inter_ev.start_ms == int(datetime(2026, 11, 22, 19, 45, tzinfo=timezone.utc).timestamp() * 1000)

    # HD7 ITALIAN must come first (priority 3), followed by BR1 and HD1 (priority 31)
    assert len(inter_ev.streams) == 3
    first = inter_ev.streams[0]
    assert first.channel_name == "HD7"
    assert first.language == "ITA"
    assert first.priority == 3
    assert first.requires_proxy is True
    assert first.proxy_host == "sportsonline"
    assert all(s.priority == 31 for s in inter_ev.streams[1:])

    nba_ev = events[1]
    assert nba_ev.streams[0].channel_name == "HD8"
    assert nba_ev.streams[0].language == "ITA"
    assert nba_ev.streams[0].priority == 3

    mon_ev = events[2]
    assert mon_ev.start_ms == int(datetime(2026, 11, 23, 20, 0, tzinfo=timezone.utc).timestamp() * 1000)
    assert mon_ev.streams[0].language == "ENG"
    assert mon_ev.streams[0].priority == 31


@pytest.mark.asyncio
async def test_spon_service_mirror_fallback_and_enable_toggle() -> None:
    sample_prog = (
        "HD7 ITALIAN\n"
        "HD1 ENGLISH\n"
        "SUNDAY\n"
        "20:45 Napoli x Roma | https://sportsonline.st/channels/hd/hd7.php\n"
    )
    seen_urls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        seen_urls.append(url)
        if "sportsonline.st" in url:
            return httpx.Response(503)
        return httpx.Response(200, text=sample_prog)

    settings = Settings(ENABLE_SPON=True)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        svc = SponService(settings=settings, client=client)
        res = await svc.safe_harvest()

    assert res.error == ""
    assert len(seen_urls) == 2
    assert "sportsonline.st" in seen_urls[0]
    assert "sportzonline.st" in seen_urls[1]
    assert len(res.events) == 1
    assert res.events[0].streams[0].language == "ITA"
    assert res.events[0].streams[0].proxy_host == "sportsonline"

    disabled = SponService(settings=Settings(ENABLE_SPON=False))
    disabled_res = await disabled.safe_harvest()
    assert disabled_res.events == []
    assert disabled_res.error == ""
