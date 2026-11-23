"""Targeted unit tests for Phase 2 ingestion services (DirettaService & ItalianEPGService)."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from app.config import Settings
from app.ingestion import (
    DirettaService,
    ItalianEPGService,
    SKY_CHANNELS,
    SPORT_IDS,
    TIVUSAT_CHANNELS,
    clamp_sky_from,
    deabbreviate_tennis_name,
    deduplicate_simulcasts,
    extract_tv_broadcasters,
    filter_ghost_epg_matches,
    is_non_live_or_studio,
    is_out_of_calendar_tennis,
    parse_flashscore_feed,
    reconcile_with_schedule,
)
from app.models.domain import EPGProgram, Match, SportSilo, StreamSource, TeamInfo


def test_parse_flashscore_feed_and_al_filtering() -> None:
    al_json = json.dumps(
        {
            "1": [
                {"TVI": 1, "BN": "DAZN (Ita)"},
                {"TVI": 2, "BN": "Sky Sport Calcio"},
            ],
            "2": [
                {"BU": "/bookmaker/123", "BN": "bet365"},
                {"BU": "/bookmaker/456", "BN": "Betfair Live"},
            ],
        }
    )
    sample = (
        "SA÷1¬~"
        "ZA÷ITALIA: Serie A¬ZY÷Italia¬ZB÷98¬ZAE÷Giornata 12¬ZL÷/calcio/italia/serie-a/~"
        f"AA÷O0r8yHkP¬AD÷1732291200¬AE÷Inter¬CX÷Inter¬AF÷Juventus¬AL÷{al_json}~"
    )
    events = parse_flashscore_feed(sample)
    assert len(events) == 1
    ev = events[0]
    assert ev["aa"] == "O0r8yHkP"
    assert ev["za"] == "ITALIA: Serie A"
    assert ev["country"] == "Italia"
    assert ev["zae"] == "Giornata 12"
    assert ev["zl"] == "/calcio/italia/serie-a/"
    assert ev["start_ms"] == 1732291200 * 1000
    assert ev["home"] == "Inter"
    assert ev["away"] == "Juventus"
    # Key "1" TV channels kept; key "2" betting streams strictly excluded
    assert ev["broadcasters"] == ["DAZN", "Sky Sport Calcio"]
    assert extract_tv_broadcasters({"2": [{"BN": "bet365"}]}) == []


def test_tennis_deabbreviation_single_multiword_and_doubles() -> None:
    assert deabbreviate_tennis_name("Paolini J.", "paolini-jasmine") == "Jasmine Paolini"
    assert deabbreviate_tennis_name("Paolini J.", "paolini-jasmine-12345") == "Jasmine Paolini"
    assert deabbreviate_tennis_name("De Minaur A.", "de-minaur-alex-987") == "Alex De Minaur"
    assert (
        deabbreviate_tennis_name("Bautista Agut R.", "bautista-agut-roberto")
        == "Roberto Bautista Agut"
    )
    assert (
        deabbreviate_tennis_name("Del Potro J.", "del-potro-juan-martin")
        == "Juan Martin Del Potro"
    )
    assert (
        deabbreviate_tennis_name("Auger-Aliassime F.", "auger-aliassime-felix-555")
        == "Felix Auger-Aliassime"
    )
    assert (
        deabbreviate_tennis_name(
            "Errani S. / Paolini J.", "errani-sara-101 / paolini-jasmine-202"
        )
        == "Sara Errani / Jasmine Paolini"
    )


@pytest.mark.asyncio
async def test_diretta_f1_dedup_dual_feed_and_tennis_cities() -> None:
    requested_urls: list[str] = []
    seen_headers: list[dict[str, str]] = []

    it_football = (
        "SA÷1¬~"
        "ZA÷ITALIA: Serie A¬ZY÷Italia¬ZB÷98¬ZL÷/calcio/italia/serie-a/~"
        'AA÷fb001¬AD÷1732291200¬AE÷Inter¬AF÷Milan¬AL÷{"1":[{"BN":"DAZN (Ita)"}],"2":[{"BN":"bet365"}]}~'
        "ZA÷ITALIA: Serie D - Girone A¬ZY÷Italia¬ZB÷98¬ZL÷/calcio/italia/serie-d/~"
        "AA÷fb_minor¬AD÷1732291200¬AE÷Varese¬AF÷Asti~"
    )
    en_football = (
        "SA÷1¬~"
        "ZA÷ITALY: Serie A¬ZY÷Italy¬ZB÷98¬ZL÷/football/italy/serie-a/~"
        "AA÷fb001¬AD÷1732291200¬AE÷Inter Milan¬AF÷AC Milan~"
    )
    it_tennis = (
        "SA÷2¬~"
        "ZA÷ATP - SINGOLARE: Torino (Italia), cemento¬ZY÷Mondo¬ZAE÷Finale~AA÷tn001¬AD÷1732294800¬"
        "AE÷Sinner J.¬WU÷sinner-jannik-111¬AF÷Fritz T.¬WV÷fritz-taylor-222~"
    )
    en_tennis = (
        "SA÷2¬~"
        "ZA÷ATP - SINGLES: Turin (World), hard¬ZY÷World¬ZAE÷Final~AA÷tn001¬AD÷1732294800¬"
        "AE÷Sinner J.¬WU÷sinner-jannik-111¬AF÷Fritz T.¬WV÷fritz-taylor-222~"
    )
    it_f1 = (
        "SA÷32¬~"
        "ZA÷FORMULA 1: GP Las Vegas (USA) - Gara¬ZY÷USA¬ZAE÷Gara~AA÷drv1sess¬AD÷1732320000¬"
        'CX÷1¬AE÷Verstappen M.¬WU÷verstappen-max¬AL÷{"1":[{"BN":"Sky Sport F1"}],"2":[{"BN":"bet365"}]}~'
        "AA÷drv2sess¬AD÷1732320000¬CX÷2¬AE÷Leclerc C.¬WU÷leclerc-charles~"
    )
    en_f1 = (
        "SA÷32¬~"
        "ZA÷FORMULA 1: Las Vegas Grand Prix (USA) - Race¬ZY÷USA¬ZAE÷Race~AA÷drv1sess¬AD÷1732320000¬"
        "CX÷1¬AE÷Verstappen M.~"
    )

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        requested_urls.append(url)
        seen_headers.append((url, dict(request.headers)))
        if "f_1_0_3_it_1" in url:
            return httpx.Response(200, text=it_football)
        if "f_1_0_3_en-uk_1" in url:
            return httpx.Response(200, text=en_football)
        if "f_2_0_3_it_1" in url:
            return httpx.Response(200, text=it_tennis)
        if "f_2_0_3_en-uk_1" in url:
            return httpx.Response(200, text=en_tennis)
        if "f_32_0_3_it_1" in url:
            return httpx.Response(200, text=it_f1)
        if "f_32_0_3_en-uk_1" in url:
            return httpx.Response(200, text=en_f1)
        return httpx.Response(200, text="")

    settings = Settings(FLASHSCORE_FSIGN="test-fsign-val", ENABLE_DIRETTA=True)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        svc = DirettaService(settings=settings, client=client)
        matches, tennis_cities = await svc.fetch_schedule(days=(0,), sport_ids=(1, 2, 32))

    assert any("400.flashscore.ninja" in u for u in requested_urls)
    assert any("2.flashscore.ninja" in u for u in requested_urls)
    it_hdrs = [h for u, h in seen_headers if "400.flashscore.ninja" in u]
    en_hdrs = [h for u, h in seen_headers if "2.flashscore.ninja" in u]
    assert it_hdrs and all(h.get("origin") == "https://www.diretta.it" for h in it_hdrs)
    assert it_hdrs and all(h.get("referer") == "https://www.diretta.it/" for h in it_hdrs)
    assert en_hdrs and all(h.get("origin") == "https://www.flashscore.com" for h in en_hdrs)
    assert en_hdrs and all(h.get("referer") == "https://www.flashscore.com/" for h in en_hdrs)
    assert all(h.get("x-fsign") == "test-fsign-val" for _, h in seen_headers)

    by_id = {m.id: m for m in matches}
    # Serie D minor league without broadcasters rejected; Serie A, ATP, F1 admitted.
    assert "diretta-fb_minor" not in by_id
    assert set(by_id.keys()) == {"diretta-fb001", "diretta-tn001", "diretta-drv1sess"}

    fb = by_id["diretta-fb001"]
    assert fb.silo == SportSilo.CALCIO_ITA
    assert fb.category == "calcio_italiano"
    assert fb.alt_home == "Inter Milan"
    assert fb.alt_away == "AC Milan"
    assert fb.alt_league == "Serie A"
    assert fb.alt_title == "Inter Milan vs AC Milan"
    assert fb.broadcasters == ["DAZN"]
    assert fb.end_date - fb.date == SPORT_IDS[1].duration_minutes * 60 * 1000

    tn = by_id["diretta-tn001"]
    assert tn.home_team is not None and tn.home_team.name == "Jannik Sinner"
    assert tn.away_team is not None and tn.away_team.name == "Taylor Fritz"
    assert "torino" in tennis_cities
    assert "turin" in tennis_cities

    f1 = by_id["diretta-drv1sess"]
    assert f1.silo == SportSilo.MOTORI
    assert f1.away_team is None
    assert "Gara" in f1.title
    assert f1.broadcasters == ["Sky Sport F1"]
    assert "Las Vegas Grand Prix" in f1.alt_league


@pytest.mark.asyncio
async def test_diretta_disabled_returns_empty() -> None:
    called = False

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal called
        called = True
        return httpx.Response(200, text="")

    settings = Settings(ENABLE_DIRETTA=False)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        svc = DirettaService(settings=settings, client=client)
        matches, cities = await svc.fetch_schedule()
    assert matches == []
    assert cities == frozenset()
    assert not called


def test_clamp_sky_from_prevents_gt_3h_lookback() -> None:
    now = datetime(2026, 11, 22, 18, 0, 0, tzinfo=timezone.utc)
    too_old = now - timedelta(hours=5)
    clamped = clamp_sky_from(too_old, now)
    assert clamped == now - timedelta(minutes=165)
    assert (now - clamped) <= timedelta(minutes=165)

    recent = now - timedelta(minutes=45)
    assert clamp_sky_from(recent, now) == recent


def test_non_live_or_studio_and_out_of_calendar_tennis_filters() -> None:
    assert is_non_live_or_studio("Sky Sport 24 - Rassegna Stampa")
    assert is_non_live_or_studio("Paddock Live Show")
    assert is_non_live_or_studio("Calciomercato L'Originale")
    assert is_non_live_or_studio("Studio Tennis ATP Finals")
    assert is_non_live_or_studio("Sky Calcio Club")
    assert is_non_live_or_studio("F1 GP Italia - Highlights")
    assert is_non_live_or_studio("MotoGP - On Board")
    assert is_non_live_or_studio("Ep. 14", "La trasmissione dedicata ai gol della Serie A")
    assert is_non_live_or_studio("Nessun evento in onda")
    assert not is_non_live_or_studio("Diretta Gol Serie A")
    assert is_non_live_or_studio("Partita", "Sintesi e azioni salienti in replica")
    assert not is_non_live_or_studio("Serie A: Inter - Juventus", "Telecronaca diretta")

    active_cities = frozenset({"torino", "turin", "malaga"})
    assert not is_out_of_calendar_tennis("ATP Finals Torino: Sinner vs Fritz", "Tennis", active_cities)
    assert not is_out_of_calendar_tennis("ATP Tour Live - Diretta", "Tennis", active_cities)
    assert is_out_of_calendar_tennis("ATP 500 Halle: Sinner vs Hurkacz", "Tennis", active_cities)
    assert is_out_of_calendar_tennis(
        "Wimbledon 2021 - Finale Torino Replay", "Tennis", active_cities, current_year=2026
    )
    assert not is_out_of_calendar_tennis("ATP 500 Halle", "Tennis", frozenset())


def test_simulcast_deduplication_and_reconciliation() -> None:
    base_ms = 1_732_291_200_000
    epg_items = [
        EPGProgram(
            id="sky_1",
            channel_id="9097",
            channel_name="Sky Sport Uno",
            title="Champions League: Inter - Arsenal",
            start_ms=base_ms,
            end_ms=base_ms + 120 * 60_000,
            sport_hint="Calcio",
            source="sky",
        ),
        EPGProgram(
            id="sky_2",
            channel_id="8195",
            channel_name="TV8",
            title="Diretta Champions League: Inter - Arsenal HD",
            start_ms=base_ms + 10 * 60_000,
            end_ms=base_ms + 135 * 60_000,
            sport_hint="Calcio",
            source="sky",
        ),
        EPGProgram(
            id="sky_3",
            channel_id="9113",
            channel_name="Sky Sport Calcio",
            title="Diretta Gol Serie A",
            start_ms=base_ms,
            end_ms=base_ms + 120 * 60_000,
            sport_hint="Calcio",
            source="sky",
        ),
    ]
    deduped = deduplicate_simulcasts(epg_items)
    assert len(deduped) == 2
    ucl_epg = next(p for p in deduped if "Inter" in p.title)
    assert ucl_epg.start_ms == base_ms
    assert ucl_epg.end_ms == base_ms + 135 * 60_000
    assert "Sky Sport Uno" in ucl_epg.channel_name
    assert "TV8" in ucl_epg.channel_name

    diretta_match = Match(
        id="diretta-ucl1",
        title="Inter - Arsenal",
        category="ss_football",
        silo=SportSilo.FOOTBALL,
        genres=["Champions League"],
        date=base_ms + 15 * 60_000,
        end_date=base_ms + 150 * 60_000,
        competition="UEFA Champions League",
        home_team=TeamInfo(name="Inter"),
        away_team=TeamInfo(name="Arsenal"),
        broadcasters=[],
        source_origin="diretta",
    )

    enriched, orphans = reconcile_with_schedule([diretta_match], deduped)
    assert len(enriched) == 1
    assert enriched[0].broadcasters == ["Sky Sport Uno", "TV8"]
    assert len(orphans) == 1
    orphan = orphans[0]
    assert orphan.source_origin == "epg"
    assert orphan.id.startswith("sky_9113_diretta-gol-serie-a_")
    assert orphan.broadcasters == ["Sky Sport Calcio"]

    # Ghost filter: orphan without sources dropped; orphan with source kept; diretta kept.
    assert len(filter_ghost_epg_matches(enriched + orphans)) == 1
    orphan_with_stream = orphan.model_copy(
        update={
            "sources": [
                StreamSource(provider="tvvoo", title="Sky Sport Calcio FHD", url="http://x/1.m3u8")
            ]
        }
    )
    assert len(filter_ghost_epg_matches(enriched + [orphan_with_stream])) == 2


@pytest.mark.asyncio
async def test_italian_epg_service_toggles_and_sky_clamp_query() -> None:
    seen_urls: list[str] = []
    now = datetime(2026, 11, 22, 18, 0, 0, tzinfo=timezone.utc)

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        seen_urls.append(url)
        if "apid.sky.it" in url:
            return httpx.Response(
                200,
                json={
                    "events": [
                        {
                            "eventId": "101",
                            "starttime": "2026-11-22T19:00:00Z",
                            "endtime": "2026-11-22T21:00:00Z",
                            "eventTitle": "Serie A: Roma - Napoli",
                            "eventSynopsis": "Telecronaca diretta",
                            "channel": {"id": 9113, "name": "Sky Sport Calcio"},
                            "content": {
                                "genre": {"name": "Sport"},
                                "subgenre": {"name": "Calcio"},
                            },
                        },
                        {
                            "eventId": "102",
                            "starttime": "2026-11-22T18:00:00Z",
                            "endtime": "2026-11-22T18:30:00Z",
                            "eventTitle": "Sky Sport 24",
                            "eventSynopsis": "Notiziario",
                            "channel": {"id": 9097, "name": "Sky Sport Uno"},
                            "content": {
                                "genre": {"name": "Sport"},
                                "subgenre": {"name": "Calcio"},
                            },
                        },
                        {
                            "eventId": "103",
                            "starttime": "2026-11-22T20:00:00Z",
                            "endtime": "2026-11-22T22:00:00Z",
                            "eventTitle": "Film: Il Gladiatore",
                            "eventSynopsis": "Film drammatico",
                            "channel": {"id": 8195, "name": "TV8 HD"},
                            "content": {
                                "genre": {"name": "Cinema"},
                                "subgenre": {"name": "Azione"},
                            },
                        },
                    ],
                    "total": 3,
                },
            )
        return httpx.Response(200, json={"events": []})

    settings = Settings(ENABLE_SKY_EPG=True, ENABLE_TIVUSAT_EPG=False)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        svc = ItalianEPGService(settings=settings, client=client)
        progs = await svc.fetch_epg(
            requested_from=now - timedelta(hours=6),
            now_utc=now,
        )

    assert len(seen_urls) == 1
    assert "from=2026-11-22T15:15:00Z" in seen_urls[0]  # 18:00 - 165m = 15:15
    assert "tivulaguida" not in seen_urls[0]
    assert len(progs) == 1
    assert progs[0].title == "Serie A: Roma - Napoli"
    assert len(SKY_CHANNELS) >= 32
    assert "88" in TIVUSAT_CHANNELS and "295" in TIVUSAT_CHANNELS


@pytest.mark.asyncio
async def test_tivusat_only_epg_and_date_parsing() -> None:
    seen_urls: list[str] = []
    now = datetime(2026, 11, 22, 18, 0, 0, tzinfo=timezone.utc)

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        seen_urls.append(url)
        if "/channels/88/date/2026-11-22" in url:
            return httpx.Response(
                200,
                json={
                    "events": [
                        {
                            "id": 9001,
                            "date_start": "22-11-2026 20:30",
                            "date_end": "22-11-2026 22:30",
                            "program": {
                                "title": "Coppa Italia Volley: Perugia - Trentino",
                                "description": "Diretta semifinale",
                                "genres": [{"id": 36, "name": "Sport"}],
                            },
                        },
                        {
                            "id": 9002,
                            "date_start": "22-11-2026 06:00",
                            "date_end": "22-11-2026 07:00",
                            "program": {
                                "title": "Sabato al 90°",
                                "description": "La storica rubrica Rai",
                                "genres": [{"id": 36, "name": "Sport"}],
                            },
                        },
                    ]
                },
            )
        return httpx.Response(200, json={"events": []})

    settings = Settings(ENABLE_SKY_EPG=False, ENABLE_TIVUSAT_EPG=True)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        svc = ItalianEPGService(settings=settings, client=client)
        progs = await svc.fetch_epg(days_ahead=0, now_utc=now)

    assert all("apid.sky.it" not in u for u in seen_urls)
    assert any("services.tivulaguida.it" in u for u in seen_urls)
    assert len(progs) == 1
    assert progs[0].source == "tivusat"
    assert progs[0].channel_name == "Rai Sport HD"
    # 20:30 CET (UTC+1 on Nov 22) == 19:30 UTC
    expected_start_ms = int(datetime(2026, 11, 22, 19, 30, tzinfo=timezone.utc).timestamp() * 1000)
    assert progs[0].start_ms == expected_start_ms
