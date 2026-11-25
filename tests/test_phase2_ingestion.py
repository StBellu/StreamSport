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
    assert clamped == now - timedelta(hours=3)
    assert (now - clamped) <= timedelta(hours=3)

    recent = now - timedelta(minutes=45)
    assert clamp_sky_from(recent, now) == recent

    # Post-midnight HTTP 422 guard: never request earlier than 22:05 UTC of previous UTC day
    after_midnight = datetime(2026, 11, 24, 0, 30, 0, tzinfo=timezone.utc)
    clamped_midnight = clamp_sky_from(after_midnight - timedelta(hours=3), after_midnight)
    assert clamped_midnight == datetime(2026, 11, 23, 22, 5, 0, tzinfo=timezone.utc)


def test_non_live_or_studio_and_out_of_calendar_tennis_filters() -> None:
    assert is_non_live_or_studio("Notiziario Sportivo - Rassegna Stampa")
    assert is_non_live_or_studio("Pre-Partita F1")
    assert is_non_live_or_studio("Post-Partita Serie A")
    assert is_non_live_or_studio("Studio Tennis ATP Finals")
    assert is_non_live_or_studio("Rubrica Calcio Sera")
    assert is_non_live_or_studio("F1 GP Italia - Highlights")
    assert is_non_live_or_studio("MotoGP - On Board")
    assert is_non_live_or_studio("Ep. 14", "La trasmissione dedicata ai gol della Serie A")
    assert is_non_live_or_studio("Nessun evento in onda")
    assert is_non_live_or_studio("Serie A: Inter - Juventus (Differita)")
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
    from app.ingestion import SKY_FTA_CHANNEL_IDS, SKY_PAY_CHANNEL_IDS

    seen_urls: list[str] = []
    now = datetime(2026, 11, 22, 18, 0, 0, tzinfo=timezone.utc)

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        seen_urls.append(url)
        ch_list = request.url.params.get("channels", "").split(",")
        if "apid.sky.it" in url and "from=2026-11-22T15:00:00Z" in url:
            if "9113" in ch_list:
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
                                "programHighlights": "L",
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
                        ],
                        "total": 2,
                    },
                )
            if "8195" in ch_list:
                return httpx.Response(
                    200,
                    json={
                        "events": [
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
                            }
                        ],
                        "total": 1,
                    },
                )
        return httpx.Response(200, json={"events": [], "total": 0})

    settings = Settings(ENABLE_SKY_EPG=True)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        svc = ItalianEPGService(settings=settings, client=client)
        progs = await svc.fetch_epg(
            requested_from=now - timedelta(hours=6),
            now_utc=now,
        )
        # Second cycle 30m later refreshes only Day 0 (Pay + FTA); Days +1/+2 hit 12h cache
        await svc.fetch_epg(now_utc=now + timedelta(minutes=30))
        disabled = ItalianEPGService(settings=Settings(ENABLE_SKY_EPG=False), client=client)
        assert await disabled.fetch_epg(now_utc=now) == []

    assert len(seen_urls) == 8  # (3 days * 2 groups) + (Day 0 refresh * 2 groups)
    day0_urls = seen_urls[:2]
    assert all("from=2026-11-22T15:00:00Z" in u for u in day0_urls)  # 18:00 - 3h = 15:00

    pay_set = set(SKY_PAY_CHANNEL_IDS)
    fta_set = set(SKY_FTA_CHANNEL_IDS)
    assert pay_set.isdisjoint(fta_set)
    assert {"807", "6000"} <= fta_set

    day0_groups = [set(u.split("channels=")[1].split("&")[0].split(",")) for u in day0_urls]
    assert any(g == pay_set for g in day0_groups)
    assert any(g == fta_set for g in day0_groups)
    for g in day0_groups:
        assert not (g & pay_set and g & fta_set)  # Pay-TV and FTA NEVER mixed
        for excluded_sky in ("9094", "9513", "9893", "8293", "7587", "12120"):
            assert excluded_sky not in g

    assert len(progs) == 1
    assert progs[0].title == "Serie A: Roma - Napoli"
    assert progs[0].is_live is True
    assert len(SKY_CHANNELS) >= 32


@pytest.mark.asyncio
async def test_diretta_ott_exclude_and_outage_retains_tennis_cities() -> None:
    from app.ingestion.diretta_service import OTT_EXCLUDE

    for expected in (
        "sportitalia", "solocalcio", "milan tv", "inter tv",
        "juventus", "lazio", "roma", "torino", "napoli",
    ):
        assert expected in OTT_EXCLUDE

    phase = {"fail": False}
    now_utc = datetime(2026, 11, 22, 12, 0, tzinfo=timezone.utc)
    ts_sec = int(now_utc.timestamp()) + 3600

    def handler(request: httpx.Request) -> httpx.Response:
        if phase["fail"]:
            return httpx.Response(503)
        if "f_2_0_3_it_1" in str(request.url):
            return httpx.Response(
                200,
                text=(
                    "SA÷2¬~"
                    f"ZA÷ATP - SINGOLARE: Torino (Italia), cemento¬ZY÷Mondo¬ZAE÷Finale~"
                    f"AA÷t1¬AD÷{ts_sec}¬AE÷Sinner J.¬WU÷sinner-jannik-111¬"
                    "AF÷Alcaraz C.¬WV÷alcaraz-carlos-222~"
                ),
            )
        return httpx.Response(200, text="")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        svc = DirettaService(settings=Settings(ENABLE_DIRETTA=True), client=client)
        _, first_cities = await svc.fetch_schedule(days=(0,), sport_ids=(2,), now_utc=now_utc)
        assert "torino" in first_cities
        phase["fail"] = True
        _, second_cities = await svc.fetch_schedule(days=(0,), sport_ids=(2,), now_utc=now_utc)
        assert svc.last_fetch_failed is True
        assert "torino" in second_cities


def test_epg_semantic_slug_bridge_and_strict_orphan_allowlist() -> None:
    from app.ingestion.epg_service import (
        extract_competition_from_sky_url,
        filter_conflicting_italian_channels,
    )

    assert extract_competition_from_sky_url("https://guidatv.sky.it/sport/serie-a/inter-milan") == "Serie A"
    assert (
        extract_competition_from_sky_url(
            "/sport/cev-champions-league-femminile/conegliano-novara/e8c87a77-4e27-4097-aed7-b2ccb5e9e926"
        )
        == "CEV Champions League Femminile"
    )
    assert (
        extract_competition_from_sky_url(
            "/sport/sky-calcio-show/sky-calcio-show-ucl-24-11-2026/39044ac7-3310-4718-ad49-06599e84f3ea"
        )
        is None
    )
    assert (
        extract_competition_from_sky_url(
            "/programmi-tv/calcio/juventus-omonia-nicosia/c44a0ff7-a816-47df-88b6-9a95451aa9c3"
        )
        is None
    )

    base_ms = int(datetime(2026, 11, 22, 14, 0, tzinfo=timezone.utc).timestamp() * 1000)
    tennis_match = Match(
        id="diretta-tennis-roma",
        title="Jannik Sinner - Carlos Alcaraz",
        category="tennis",
        silo=SportSilo.TENNIS,
        genres=["ATP Masters"],
        date=base_ms + 30 * 60_000,
        end_date=base_ms + 150 * 60_000,
        competition="ATP Masters 1000 Roma",
        home_team=TeamInfo(name="Jannik Sinner"),
        away_team=TeamInfo(name="Carlos Alcaraz"),
        source_origin="diretta",
    )
    epg_list = [
        # Tournament Session Bridge match (ATP Roma -> Sky Sport Tennis)
        EPGProgram(
            id="sky_t1",
            channel_id="11237",
            channel_name="Sky Sport Tennis",
            title="ATP Masters 1000 Roma",
            competition_hint="ATP Masters 1000 Roma",
            start_ms=base_ms,
            end_ms=base_ms + 180 * 60_000,
            sport_hint="Tennis",
            is_live=True,
            source="sky",
        ),
        # Unmatched non-allowlisted generalist 1v1 football -> must NOT become orphan
        EPGProgram(
            id="sky_gen1",
            channel_id="899",
            channel_name="Rai 1",
            title="Film: Vita in diretta",
            start_ms=base_ms,
            end_ms=base_ms + 120 * 60_000,
            sport_hint="",
            is_live=True,
            source="sky",
        ),
        # Simulcast -> allowlisted orphan
        EPGProgram(
            id="sky_sim1",
            channel_id="9113",
            channel_name="Sky Sport Calcio",
            title="Diretta Gol Serie A",
            start_ms=base_ms,
            end_ms=base_ms + 120 * 60_000,
            sport_hint="Calcio",
            is_live=True,
            is_simulcast=True,
            source="sky",
        ),
    ]
    enriched, orphans = reconcile_with_schedule([tennis_match], epg_list)
    assert "Sky Sport Tennis" in enriched[0].broadcasters
    orphan_titles = [o.title for o in orphans]
    assert "Diretta Gol Serie A" in orphan_titles
    assert all("Vita in diretta" not in t for t in orphan_titles)

    # Channel conflict resolution: Sky Sport Uno purged when EPG confirms a different live match
    conflict_match = Match(
        id="m-conflict",
        title="Milan - Fiorentina",
        category="calcio_ita",
        silo=SportSilo.CALCIO_ITA,
        date=base_ms,
        end_date=base_ms + 100 * 60_000,
        broadcasters=["DAZN 1"],
        sources=[
            StreamSource(
                provider="tvvoo",
                title="Sky Sport Uno HD",
                url="http://x/uno",
                channel_name="sky sport uno",
            )
        ],
    )
    epg_uno = EPGProgram(
        id="ep-uno",
        channel_id="9097",
        channel_name="Sky Sport Uno",
        title="UEFA Champions League: Arsenal - Bayern Monaco",
        start_ms=base_ms,
        end_ms=base_ms + 120 * 60_000,
        sport_hint="Calcio",
        is_live=True,
        source="sky",
    )
    resolved = filter_conflicting_italian_channels([conflict_match], [epg_uno])
    assert resolved[0].sources == []


@pytest.mark.asyncio
async def test_diretta_ignores_foreign_en_broadcasters() -> None:
    now_utc = datetime(2026, 11, 22, 12, 0, tzinfo=timezone.utc)
    ts_sec = int(now_utc.timestamp()) + 3600
    en_al = json.dumps({"1": [{"BN": "Sky Sports Main Event"}, {"BN": "TNT Sports 1"}]})

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if "f_1_0_3_it_1" in url:
            return httpx.Response(
                200,
                text=(
                    "SA÷1¬~"
                    "ZA÷ITALIA: Serie A¬ZY÷Italia¬ZAE÷Giornata 13~"
                    f"AA÷it1¬AD÷{ts_sec}¬AE÷Internazionale¬WU÷inter¬AF÷Milan¬WV÷milan~"
                    "ZA÷ITALIA: Serie D - Girone A¬ZY÷Italia¬ZAE÷Giornata 13~"
                    f"AA÷minor1¬AD÷{ts_sec}¬AE÷Squadra Alfa¬WU÷alfa¬AF÷Squadra Beta¬WV÷beta~"
                ),
            )
        if "f_1_0_3_en-uk_1" in url:
            return httpx.Response(
                200,
                text=(
                    "SA÷1¬~"
                    "ZA÷ITALY: Serie A¬ZY÷Italy¬ZAE÷Round 13~"
                    f"AA÷it1¬AD÷{ts_sec}¬AE÷Inter Milan¬WU÷inter¬AF÷AC Milan¬WV÷milan¬AL÷{en_al}~"
                    "ZA÷ITALY: Serie D - Group A¬ZY÷Italy¬ZAE÷Round 13~"
                    f"AA÷minor1¬AD÷{ts_sec}¬AE÷Squadra Alfa¬WU÷alfa¬AF÷Squadra Beta¬WV÷beta¬AL÷{en_al}~"
                    "ZA÷ENGLAND: Premier League¬ZY÷England¬ZAE÷Round 12~"
                    f"AA÷enonly1¬AD÷{ts_sec}¬AE÷Liverpool¬WU÷liverpool¬AF÷Everton¬WV÷everton¬AL÷{en_al}~"
                ),
            )
        return httpx.Response(200, text="")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        svc = DirettaService(settings=Settings(ENABLE_DIRETTA=True), client=client)
        matches, _ = await svc.fetch_schedule(days=(0,), sport_ids=(1,), now_utc=now_utc)

    by_id = {m.id: m for m in matches}
    assert "diretta-it1" in by_id
    assert by_id["diretta-it1"].broadcasters == []
    assert by_id["diretta-it1"].alt_home == "Inter Milan"
    assert by_id["diretta-it1"].alt_away == "AC Milan"
    assert by_id["diretta-it1"].alt_league == "Serie A"

    # Serie D must NOT sneak in via foreign EN TV broadcasters
    assert "diretta-minor1" not in by_id

    # EN-only Tier 1 league is still admitted cleanly without foreign broadcasters
    assert "diretta-enonly1" in by_id
    assert by_id["diretta-enonly1"].broadcasters == []
    assert by_id["diretta-enonly1"].alt_home == "Liverpool"


@pytest.mark.asyncio
async def test_diretta_yesterday_window_only_00_to_04_59_live_only() -> None:
    requested_urls: list[str] = []
    # 02:30 CET (UTC+1) -> 01:30 UTC (inside 00:00-04:59 Rome window)
    night_utc = datetime(2026, 11, 24, 1, 30, tzinfo=timezone.utc)
    # 18:00 CET -> 17:00 UTC (outside 00:00-04:59 window)
    evening_utc = datetime(2026, 11, 24, 17, 0, tzinfo=timezone.utc)
    start_sec = int(night_utc.timestamp()) - 7200

    yest_it = (
        "SA÷1¬~"
        "ZA÷ITALIA: Serie A¬ZY÷Italia¬ZAE÷Giornata 14~"
        f"AA÷y_live¬AD÷{start_sec}¬AB÷2¬AC÷1¬AE÷Inter¬AF÷Napoli~"
        f"AA÷y_fin¬AD÷{start_sec}¬AB÷3¬AC÷3¬AO÷{start_sec + 6000}¬AE÷Milan¬AF÷Roma~"
    )

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        requested_urls.append(url)
        if "f_1_-1_3_it_1" in url:
            return httpx.Response(200, text=yest_it)
        return httpx.Response(200, text="")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        svc = DirettaService(settings=Settings(ENABLE_DIRETTA=True), client=client)
        night_matches, _ = await svc.fetch_schedule(sport_ids=(1,), now_utc=night_utc)
        night_ids = {m.id for m in night_matches}
        assert night_ids == {"diretta-y_live"}
        assert any("f_1_-1_3_it_1" in u for u in requested_urls)

        requested_urls.clear()
        await svc.fetch_schedule(sport_ids=(1,), now_utc=evening_utc)
        assert not any("_-1_" in u for u in requested_urls)


@pytest.mark.asyncio
async def test_diretta_future_12h_cache_and_00_20_rotation() -> None:
    requested_days: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        for day in (0, 1, 2):
            if f"f_1_{day}_3_it_1" in url or f"f_1_{day}_3_en-uk_1" in url:
                requested_days.append(day)
        return httpx.Response(200, text="")

    # Nov 24 14:00 CET = 13:00 UTC -> logical day 2026-11-24
    t0 = datetime(2026, 11, 24, 13, 0, tzinfo=timezone.utc)
    # Nov 24 19:00 CET (+5h, within 12h TTL) -> +1/+2 stay cached
    t1 = datetime(2026, 11, 24, 18, 0, tzinfo=timezone.utc)
    # Nov 25 00:05 CET = Nov 24 23:05 UTC (+10h05m < 12h TTL and < 00:20 Rome -> still cached)
    t_pre_rot = datetime(2026, 11, 24, 23, 5, tzinfo=timezone.utc)
    # Nov 25 00:21 CET = Nov 24 23:21 UTC (>= 00:20 Rome -> flips to 2026-11-25, refetches +1/+2)
    t_post_rot = datetime(2026, 11, 24, 23, 21, tzinfo=timezone.utc)
    # Nov 25 13:00 CET (+12h39m after t_post_rot) -> 12h TTL expired -> refetches +1/+2
    t_ttl_exp = datetime(2026, 11, 25, 12, 0, tzinfo=timezone.utc)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        svc = DirettaService(settings=Settings(ENABLE_DIRETTA=True), client=client)

        await svc.fetch_schedule(days=(0, 1, 2), sport_ids=(1,), now_utc=t0)
        assert requested_days.count(0) == 2 and requested_days.count(1) == 2 and requested_days.count(2) == 2

        requested_days.clear()
        await svc.fetch_schedule(days=(0, 1, 2), sport_ids=(1,), now_utc=t1)
        assert requested_days == [0, 0]

        requested_days.clear()
        await svc.fetch_schedule(days=(0, 1, 2), sport_ids=(1,), now_utc=t_pre_rot)
        assert requested_days == [0, 0]

        requested_days.clear()
        await svc.fetch_schedule(days=(0, 1, 2), sport_ids=(1,), now_utc=t_post_rot)
        assert requested_days.count(0) == 2 and requested_days.count(1) == 2 and requested_days.count(2) == 2

        requested_days.clear()
        await svc.fetch_schedule(days=(0, 1, 2), sport_ids=(1,), now_utc=t_ttl_exp)
        assert requested_days.count(0) == 2 and requested_days.count(1) == 2 and requested_days.count(2) == 2


@pytest.mark.asyncio
async def test_diretta_real_status_ab_ac_ao_and_sky_extension() -> None:
    now_utc = datetime(2026, 11, 24, 15, 0, tzinfo=timezone.utc)
    now_sec = int(now_utc.timestamp())
    now_ms = now_sec * 1000

    # live_long: started 3h ago (135m est already passed), still AB=2 -> extend end_date to now + 60m
    # fin_recent: AB=3, AC=3, finished 20m ago -> kept with end_date=AO, eligible for Sky extension
    # fin_old: AB=3, AC=3, finished 35m ago -> discarded & marked in last_discarded_match_ids
    # postponed: AB=3, AC=4 -> immediate discard & marked in last_discarded_match_ids
    # cancelled: AB=3, AC=5 -> immediate discard & marked in last_discarded_match_ids
    feed_it = (
        "SA÷1¬~"
        "ZA÷ITALIA: Serie A¬ZY÷Italia¬ZAE÷Giornata 14~"
        f"AA÷live_long¬AD÷{now_sec - 3 * 3600}¬AB÷2¬AC÷1¬AE÷Inter¬AF÷Milan~"
        f"AA÷fin_recent¬AD÷{now_sec - 7200}¬AB÷3¬AC÷3¬AO÷{now_sec - 20 * 60}¬AE÷Juventus¬AF÷Lazio~"
        f"AA÷fin_old¬AD÷{now_sec - 9000}¬AB÷3¬AC÷3¬AO÷{now_sec - 35 * 60}¬AE÷Napoli¬AF÷Torino~"
        f"AA÷postponed¬AD÷{now_sec + 3600}¬AB÷3¬AC÷4¬AE÷Roma¬AF÷Bologna~"
        f"AA÷cancelled¬AD÷{now_sec + 3600}¬AB÷3¬AC÷5¬AE÷Atalanta¬AF÷Como~"
    )

    def handler(request: httpx.Request) -> httpx.Response:
        if "f_1_0_3_it_1" in str(request.url):
            return httpx.Response(200, text=feed_it)
        return httpx.Response(200, text="")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        svc = DirettaService(settings=Settings(ENABLE_DIRETTA=True), client=client)
        matches, _ = await svc.fetch_schedule(days=(0,), sport_ids=(1,), now_utc=now_utc)

    by_id = {m.id: m for m in matches}
    assert set(by_id.keys()) == {"diretta-live_long", "diretta-fin_recent"}
    assert by_id["diretta-live_long"].end_date >= now_ms + 60 * 60_000
    assert by_id["diretta-fin_recent"].end_date == (now_sec - 20 * 60) * 1000
    assert {"diretta-fin_old", "diretta-postponed", "diretta-cancelled"} <= svc.last_discarded_match_ids

    # Pairing with Sky broadcast running until AO + 40m pushes expiry to sky_end_ms + 15m
    sky_end_ms = now_ms + 20 * 60_000
    sky_prog = EPGProgram(
        id="sky_juve",
        channel_id="9113",
        channel_name="Sky Sport Calcio",
        title="Serie A: Juventus - Lazio",
        start_ms=by_id["diretta-fin_recent"].date,
        end_ms=sky_end_ms,
        sport_hint="Calcio",
        is_live=True,
        source="sky",
    )
    enriched, _ = reconcile_with_schedule([by_id["diretta-fin_recent"]], [sky_prog])
    assert enriched[0].end_date == sky_end_ms - 15 * 60_000


@pytest.mark.asyncio
async def test_sky_epg_per_day_cache_rotation_422_retry_and_15m_expiry() -> None:
    calls: list[tuple[str, str, str]] = []
    state = {"raise_422_once": True}

    def handler(request: httpx.Request) -> httpx.Response:
        from_val = request.url.params.get("from", "")
        to_val = request.url.params.get("to", "")
        ch_param = request.url.params.get("channels", "")
        calls.append((from_val, to_val, ch_param))

        if state["raise_422_once"]:
            state["raise_422_once"] = False
            return httpx.Response(
                422,
                json={"description": "Dates validation has failed"},
                request=request,
            )

        if "9113" not in ch_param.split(","):
            return httpx.Response(200, json={"events": [], "total": 0})

        # Day 0: one program expired >15m ago (ignored), one program ending 10m ago (kept)
        if from_val.startswith("2026-11-24"):
            return httpx.Response(
                200,
                json={
                    "events": [
                        {
                            "eventId": "d0_exp",
                            "starttime": "2026-11-24T12:00:00Z",
                            "endtime": "2026-11-24T14:40:00Z",  # 20m before 15:00Z -> discarded
                            "eventTitle": "Serie A: Fiorentina - Genoa",
                            "programHighlights": "L",
                            "channel": {"id": 9113, "name": "Sky Sport Calcio"},
                            "content": {"genre": {"name": "Sport"}, "subgenre": {"name": "Calcio"}},
                        },
                        {
                            "eventId": "d0_keep",
                            "starttime": "2026-11-24T13:00:00Z",
                            "endtime": "2026-11-24T14:50:00Z",  # 10m before 15:00Z -> kept (+15m)
                            "eventTitle": "Serie A: Udinese - Parma",
                            "programHighlights": "L",
                            "channel": {"id": 9113, "name": "Sky Sport Calcio"},
                            "content": {"genre": {"name": "Sport"}, "subgenre": {"name": "Calcio"}},
                        },
                    ],
                    "total": 2,
                },
            )
        if from_val.startswith("2026-11-25"):
            return httpx.Response(
                200,
                json={
                    "events": [
                        {
                            "eventId": "d2_future",
                            "starttime": "2026-11-26T20:00:00Z",
                            "endtime": "2026-11-26T22:00:00Z",
                            "eventTitle": "Champions League: Milan - Liverpool",
                            "programHighlights": "L",
                            "channel": {"id": 9097, "name": "Sky Sport Uno"},
                            "content": {"genre": {"name": "Sport"}, "subgenre": {"name": "Calcio"}},
                        }
                    ],
                    "total": 1,
                },
            )
        return httpx.Response(200, json={"events": [], "total": 0})

    t0 = datetime(2026, 11, 24, 15, 0, tzinfo=timezone.utc)
    # 2026-11-25 00:05 CET = 2026-11-24 23:05 UTC (< 00:20 -> +1/+2 cached!)
    t_pre_rot = datetime(2026, 11, 24, 23, 5, tzinfo=timezone.utc)
    # 2026-11-25 00:25 CET = 2026-11-24 23:25 UTC (>= 00:20 -> +1/+2 rotated & refreshed)
    t_post_rot = datetime(2026, 11, 24, 23, 25, tzinfo=timezone.utc)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        svc = ItalianEPGService(settings=Settings(ENABLE_SKY_EPG=True), client=client)
        progs = await svc.fetch_epg(now_utc=t0)
        ids = {p.id for p in progs}
        assert "sky_d0_exp" not in ids
        assert "sky_d0_keep" in ids
        assert "sky_d2_future" in ids
        # 7 calls: initial Day 0 (Pay 422 + retry + FTA) + Day +1 (Pay + FTA) + Day +2 (Pay + FTA)
        assert len(calls) == 7
        assert any(c[0] == "2026-11-24T15:00:00Z" for c in calls[1:3])

        calls.clear()
        await svc.fetch_epg(now_utc=t_pre_rot)
        assert len(calls) == 2  # Only Day 0 (Pay + FTA) requested before 00:20 rotation

        calls.clear()
        await svc.fetch_epg(now_utc=t_post_rot)
        assert len(calls) == 6  # All 3 buckets (0, +1, +2) * 2 groups refreshed after 00:20 rotation


@pytest.mark.asyncio
async def test_sky_epg_phase_a_raw_extraction_pay_fta_posters_and_season_filter() -> None:
    from app.ingestion.epg_service import _extract_sky_poster_url

    # 1. Poster selection & validation unit checks: scene_key_art > scene, ignore background/cover_clean, reject 403/400
    assert _extract_sky_poster_url(
        {
            "imagesMap": [
                {"key": "background", "img": {"url": "/uuid/11111111-1111-1111-1111-111111111111/background"}},
                {"key": "cover_clean", "img": {"url": "/uuid/11111111-1111-1111-1111-111111111111/COVER_CLEAN_TALL"}},
                {"key": "scene", "img": {"url": "/uuid/11111111-1111-1111-1111-111111111111/16-9"}},
                {"key": "scene_key_art", "img": {"url": "/uuid/11111111-1111-1111-1111-111111111111/COVER_TITLE_WIDE"}},
            ]
        }
    ) == "https://guidatv.sky.it/uuid/11111111-1111-1111-1111-111111111111/COVER_TITLE_WIDE"
    assert _extract_sky_poster_url(
        {
            "imagesMap": [
                {"key": "background", "img": {"url": "/uuid/11111111-1111-1111-1111-111111111111/background"}},
                {"key": "scene", "img": {"url": "/uuid/11111111-1111-1111-1111-111111111111/16-9"}},
            ]
        }
    ) == "https://guidatv.sky.it/uuid/11111111-1111-1111-1111-111111111111/16-9"
    assert _extract_sky_poster_url(
        {
            "imagesMap": [
                {"key": "background", "img": {"url": "/uuid/11111111-1111-1111-1111-111111111111/background"}},
                {"key": "cover_clean", "img": {"url": "/uuid/11111111-1111-1111-1111-111111111111/COVER_CLEAN_TALL"}},
            ]
        }
    ) == ""
    assert _extract_sky_poster_url(
        {
            "imagesMap": [
                {"key": "scene_key_art", "img": {"url": "/uuid/SportCalcio_Scene_8PgF6U81O_.png"}},
                {"key": "scene", "img": {"url": "/uuid/22222222-2222-2222-2222-222222222222/16-9?cgid=90&sid=566"}},
            ]
        }
    ) == ""
    assert _extract_sky_poster_url(
        {
            "imagesMap": [
                {"key": "scene", "img": {"url": "/uuid/33333333-3333-3333-3333-333333333333/16-9?cgid=60"}},
            ]
        }
    ) == "https://guidatv.sky.it/uuid/33333333-3333-3333-3333-333333333333/16-9?cgid=60"

    # 2. End-to-end Phase A raw extraction (Pay-TV + FTA separated calls)
    now = datetime(2026, 11, 24, 17, 0, 0, tzinfo=timezone.utc)
    pay_events = [
        # Identical titles -> epg_title / content_title kept empty; /programmi-tv/calcio match with seasonNumber=None stays LIVE
        {
            "eventId": "p_atalanta",
            "starttime": "2026-11-24T17:30:00Z",
            "endtime": "2026-11-24T19:30:00Z",
            "eventTitle": "Atalanta U23 - Pineto",
            "epgEventTitle": "Atalanta U23 - Pineto",
            "eventSynopsis": "Serie C",
            "programHighlights": "L",
            "channel": {"id": 9116, "name": "Sky Sport"},
            "content": {
                "contentTitle": "Atalanta U23 - Pineto",
                "seasonNumber": None,
                "url": "/programmi-tv/calcio/atalanta-u23-pineto/44bce855-33f2-4fdd-99cd-264de7879dd7",
                "genre": {"name": "Sport"},
                "subgenre": {"name": "Calcio"},
                "imagesMap": [
                    {"key": "scene_key_art", "img": {"url": "/uuid/44bce855-33f2-4fdd-99cd-264de7879dd7/COVER_TITLE_WIDE"}}
                ],
            },
        },
        # Different contentTitle preserved cleanly
        {
            "eventId": "p_lipsia",
            "starttime": "2026-11-24T19:00:00Z",
            "endtime": "2026-11-24T21:00:00Z",
            "eventTitle": "Lipsia - LENS",
            "epgEventTitle": "Lipsia - LENS",
            "eventSynopsis": "UEFA Champions League",
            "programHighlights": "L",
            "channel": {"id": 11497, "name": "Sky Sport 251"},
            "content": {
                "contentTitle": "RB Lipsia - Lens",
                "seasonNumber": 2026,
                "url": "/sport/uefa-champions-league/rb-lipsia-lens/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                "genre": {"name": "Sport"},
                "subgenre": {"name": "Calcio"},
            },
        },
        # Docu-series exposed ONLY via epgEventTitle ("S1 Ep2 ...") -> filtered out as non-live/series
        {
            "eventId": "p_doc",
            "starttime": "2026-11-24T17:00:00Z",
            "endtime": "2026-11-24T18:00:00Z",
            "eventTitle": "La reggia del basket",
            "epgEventTitle": "S1 Ep2 - Scugnizzi per sempre",
            "eventSynopsis": "Racconto sul basket.",
            "programHighlights": None,
            "channel": {"id": 11910, "name": "Sky Sport Legend"},
            "content": {
                "contentTitle": "La reggia del basket",
                "seasonNumber": 2026,
                "url": "/programmi-tv/sport/scugnizzi-per-sempre/stagione-1/episodio-2/bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
                "genre": {"name": "Sport"},
                "subgenre": {"name": "Basket"},
            },
        },
        # Archival replay WITHOUT year in title, caught via seasonNumber=2021
        {
            "eventId": "p_archive",
            "starttime": "2026-11-24T17:00:00Z",
            "endtime": "2026-11-24T19:00:00Z",
            "eventTitle": "Roma - Feyenoord",
            "epgEventTitle": "Roma - Feyenoord",
            "eventSynopsis": "Grande sfida europea.",
            "programHighlights": None,
            "channel": {"id": 11910, "name": "Sky Sport Legend"},
            "content": {
                "contentTitle": "Roma - Feyenoord",
                "seasonNumber": 2021,
                "url": "/sport/calcio/roma-feyenoord/cccccccc-cccc-cccc-cccc-cccccccccccc",
                "genre": {"name": "Sport"},
                "subgenre": {"name": "Calcio"},
            },
        },
        # Synopsis containing "direttamente dalla stagione 2023" must NOT make F1 Notebook live!
        {
            "eventId": "p_f1_note",
            "starttime": "2026-11-24T18:00:00Z",
            "endtime": "2026-11-24T18:30:00Z",
            "eventTitle": "F1 Notebook",
            "epgEventTitle": "F1 Notebook",
            "eventSynopsis": "Le immagini direttamente dalla stagione 2023",
            "programHighlights": None,
            "channel": {"id": 9096, "name": "Sky Sport F1"},
            "content": {
                "contentTitle": "F1 Notebook",
                "seasonNumber": 2026,
                "url": "/sport/f1-notebook/f1-notebook/dddddddd-dddd-dddd-dddd-dddddddddddd",
                "genre": {"name": "Sport"},
                "subgenre": {"name": "Motori"},
            },
        },
        # Pay-TV placeholder genre != Sport -> dropped at upstream gate
        {
            "eventId": "p_empty",
            "starttime": "2026-11-24T18:00:00Z",
            "endtime": "2026-11-24T19:00:00Z",
            "eventTitle": "Programmazione placeholder",
            "eventSynopsis": "",
            "channel": {"id": 11401, "name": "DAZN 2"},
            "content": {"genre": {"name": "Altri Programmi"}, "subgenre": {"name": "Altri Canali"}},
        },
        # Pay-TV Sport/Altri with Padel live -> kept
        {
            "eventId": "p_padel",
            "starttime": "2026-11-24T18:00:00Z",
            "endtime": "2026-11-24T20:00:00Z",
            "eventTitle": "Premier Padel",
            "eventSynopsis": "Torneo di Padel",
            "programHighlights": "L",
            "channel": {"id": 9093, "name": "Sky Sport Arena"},
            "content": {
                "url": "/sport/premier-padel-roma/premier-padel/eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee",
                "genre": {"name": "Sport"},
                "subgenre": {"name": "Altri"},
            },
        },
    ]

    fta_events = [
        # SuperTennis live with hl=None + city only in contentTitle
        {
            "eventId": "f_davis",
            "starttime": "2026-11-24T17:30:00Z",
            "endtime": "2026-11-24T20:30:00Z",
            "eventTitle": "LIVE DAVIS CUP FINALS BY GENERALI",
            "epgEventTitle": "LIVE DAVIS CUP FINALS BY GENERALI",
            "eventSynopsis": "Tennis Coppa Davis",
            "programHighlights": None,
            "channel": {"id": 6000, "name": "SuperTennis HD"},
            "content": {
                "contentTitle": "LIVE DAVIS CUP FINALS 8 BOLOGNA",
                "seasonNumber": None,
                "url": "/sport/tennis-coppa-davis/live-davis-cup-finals-8-bologna/ffffffff-ffff-ffff-ffff-ffffffffffff",
                "genre": {"name": "Sport"},
                "subgenre": {"name": "Tennis"},
                "imagesMap": [{"key": "scene_key_art", "img": {"url": "/uuid/SportCalcio_Scene_8PgF6U81O_.png"}}],
            },
        },
        # RAI Sport event tagged Intrattenimento/Intrattenimento -> MUST NOT be dropped
        {
            "eventId": "f_raisport",
            "starttime": "2026-11-24T18:00:00Z",
            "endtime": "2026-11-24T19:00:00Z",
            "eventTitle": "Atletica",
            "eventSynopsis": "Meeting internazionale",
            "programHighlights": None,
            "channel": {"id": 807, "name": "RAI Sport "},
            "content": {
                "url": "/programmi-tv/intrattenimento/atletica/12121212-1212-1212-1212-121212121212",
                "genre": {"name": "Intrattenimento"},
                "subgenre": {"name": "Intrattenimento"},
            },
        },
        # Generalist Soap Opera with programHighlights="1" (Prima Visione) -> dropped at genre gate
        {
            "eventId": "f_soap",
            "starttime": "2026-11-24T18:00:00Z",
            "endtime": "2026-11-24T19:00:00Z",
            "eventTitle": "Ep. 16 - La promessa",
            "programHighlights": "1",
            "channel": {"id": 10464, "name": "Rete 4 HD"},
            "content": {"genre": {"name": "Intrattenimento"}, "subgenre": {"name": "Soap Opera"}},
        },
        # TV8 HD Sport/Calcio live -> kept
        {
            "eventId": "f_tv8",
            "starttime": "2026-11-24T20:00:00Z",
            "endtime": "2026-11-24T22:00:00Z",
            "eventTitle": "Lille OSC - FC Bayern Monaco",
            "eventSynopsis": "UEFA Champions League",
            "programHighlights": "L",
            "channel": {"id": 8195, "name": "TV8 HD"},
            "content": {
                "seasonNumber": 2026,
                "url": "/sport/uefa-champions-league/lille-osc-fc-bayern-monaco/34343434-3434-3434-3434-343434343434",
                "genre": {"name": "Sport"},
                "subgenre": {"name": "Calcio"},
                "imagesMap": [{"key": "scene", "img": {"url": "/uuid/34343434-3434-3434-3434-343434343434/16-9?cgid=60"}}],
            },
        },
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        from_val = request.url.params.get("from", "")
        ch_list = request.url.params.get("channels", "").split(",")
        if from_val.startswith("2026-11-24"):
            if "9113" in ch_list:
                return httpx.Response(200, json={"events": pay_events, "total": len(pay_events)})
            if "8195" in ch_list:
                return httpx.Response(200, json={"events": fta_events, "total": len(fta_events)})
        return httpx.Response(200, json={"events": [], "total": 0})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        svc = ItalianEPGService(settings=Settings(ENABLE_SKY_EPG=True), client=client)
        raw_items = await svc.fetch_sky_epg(days=(0,), now_utc=now)
        live_ready = await svc.fetch_epg(active_tennis_cities=frozenset({"bologna"}), days=(0,), now_utc=now)

    raw_by_id = {p.id: p for p in raw_items}
    assert "sky_p_empty" not in raw_by_id
    assert "sky_f_soap" not in raw_by_id
    assert {"sky_p_atalanta", "sky_p_lipsia", "sky_p_padel", "sky_f_davis", "sky_f_raisport", "sky_f_tv8"} <= set(raw_by_id.keys())

    # Raw field fidelity checks
    assert raw_by_id["sky_p_atalanta"].is_live is True
    assert raw_by_id["sky_p_atalanta"].epg_title == ""
    assert raw_by_id["sky_p_atalanta"].content_title == ""
    assert raw_by_id["sky_p_atalanta"].content_url.startswith("/programmi-tv/calcio/")
    assert raw_by_id["sky_p_lipsia"].content_title == "RB Lipsia - Lens"
    assert raw_by_id["sky_p_lipsia"].competition_hint == "UEFA Champions League"
    assert raw_by_id["sky_p_doc"].is_live is False
    assert raw_by_id["sky_p_archive"].is_live is False
    assert raw_by_id["sky_p_f1_note"].is_live is False
    assert raw_by_id["sky_p_f1_note"].competition_hint == ""  # No fake "F1 Notebook" competition!
    assert raw_by_id["sky_f_davis"].is_live is True
    assert raw_by_id["sky_f_davis"].content_title == "LIVE DAVIS CUP FINALS 8 BOLOGNA"
    assert raw_by_id["sky_f_davis"].poster_url == ""  # Non-UUID placeholder rejected
    assert raw_by_id["sky_f_tv8"].poster_url.endswith("/16-9?cgid=60")

    live_ids = {p.id for p in live_ready}
    assert {"sky_p_atalanta", "sky_p_lipsia", "sky_p_padel", "sky_f_davis", "sky_f_raisport", "sky_f_tv8"} == live_ids
    assert "sky_p_doc" not in live_ids
    assert "sky_p_archive" not in live_ids
    assert "sky_p_f1_note" not in live_ids


def test_phase_b_simulcast_topdown_matching_and_closed_orphan_allowlist() -> None:
    from app.ingestion.epg_service import filter_conflicting_italian_channels

    base_ms = int(datetime(2026, 11, 26, 17, 0, tzinfo=timezone.utc).timestamp() * 1000)

    # B.1: Simulcast deduplication (45m window, same-channel block, date isolation, Moto/Tennis guards)
    sim_inputs = [
        EPGProgram(
            id="sky_oly_pay",
            channel_id="11494",
            channel_name="Sky Sport 253",
            title="Olympiakos - Milan",
            start_ms=base_ms,
            end_ms=base_ms + 120 * 60_000,
            sport_hint="Calcio",
            program_highlights="L",
            is_live=True,
            source="sky",
        ),
        EPGProgram(
            id="sky_oly_uno_late",
            channel_id="9097",
            channel_name="Sky Sport Uno",
            title="Olympiakos - Milan",
            start_ms=base_ms + 45 * 60_000,
            end_ms=base_ms + 150 * 60_000,
            sport_hint="Calcio",
            program_highlights="",
            is_live=False,
            source="sky",
        ),
        # Same channel -> must NEVER merge
        EPGProgram(
            id="sky_date_a",
            channel_id="9116",
            channel_name="Sky Sport Basket",
            title="Prepartita Basket 26/11/2026",
            start_ms=base_ms,
            end_ms=base_ms + 30 * 60_000,
            sport_hint="Basket",
            is_live=True,
            source="sky",
        ),
        EPGProgram(
            id="sky_date_b",
            channel_id="9046",
            channel_name="Sky Sport 258",
            title="Postpartita Volley 26/11/2026",
            start_ms=base_ms + 10 * 60_000,
            end_ms=base_ms + 40 * 60_000,
            sport_hint="Volley",
            is_live=True,
            source="sky",
        ),
        # Motorsport sessions within 45m -> FP1 != FP2
        EPGProgram(
            id="sky_fp1",
            channel_id="9096",
            channel_name="Sky Sport F1",
            title="F1: GP Qatar - Prove Libere 1",
            start_ms=base_ms,
            end_ms=base_ms + 60 * 60_000,
            sport_hint="Motori",
            is_live=True,
            source="sky",
        ),
        EPGProgram(
            id="sky_fp2",
            channel_id="10013",
            channel_name="Sky Sport 4K",
            title="F1: GP Qatar - Prove Libere 2",
            start_ms=base_ms + 20 * 60_000,
            end_ms=base_ms + 80 * 60_000,
            sport_hint="Motori",
            is_live=True,
            source="sky",
        ),
        # Tennis Singles vs Doubles -> never merge
        EPGProgram(
            id="sky_ten_s",
            channel_id="11237",
            channel_name="Sky Sport Tennis",
            title="ATP Roma - Singolare",
            start_ms=base_ms,
            end_ms=base_ms + 90 * 60_000,
            sport_hint="Tennis",
            is_live=True,
            source="sky",
        ),
        EPGProgram(
            id="sky_ten_d",
            channel_id="9093",
            channel_name="Sky Sport Arena",
            title="ATP Roma - Doppio",
            start_ms=base_ms + 15 * 60_000,
            end_ms=base_ms + 105 * 60_000,
            sport_hint="Tennis",
            is_live=True,
            source="sky",
        ),
    ]
    deduped = deduplicate_simulcasts(sim_inputs)
    assert len(deduped) == 7
    oly = next(p for p in deduped if "Olympiakos" in p.title)
    assert oly.channel_name == "Sky Sport 253, Sky Sport Uno"
    assert oly.is_live is True

    # B.2 & B.3: Top-Down matching + FTA kicker + U23 + Women's Volley + Basketball + Motorsport + Closed 6-category allowlist
    diretta_fixtures = [
        Match(
            id="d_uel_oly",
            title="Olympiakos vs Milan",
            category="football",
            silo=SportSilo.FOOTBALL,
            genres=["UEFA Europa League"],
            date=base_ms + 45 * 60_000,  # 17:45
            end_date=base_ms + 180 * 60_000,
            competition="Europa League - Prima fase",
            home_team=TeamInfo(name="Olympiakos"),
            away_team=TeamInfo(name="Milan"),
            alt_home="Olympiacos Piraeus",
            alt_away="AC Milan",
            source_origin="diretta",
        ),
        Match(
            id="d_uel_tv8",
            title="Sturm Graz vs Milan",
            category="football",
            silo=SportSilo.FOOTBALL,
            genres=["UEFA Europa League"],
            date=base_ms + 180 * 60_000,  # 20:00
            end_date=base_ms + 315 * 60_000,
            competition="Europa League - Prima fase",
            home_team=TeamInfo(name="Sturm Graz"),
            away_team=TeamInfo(name="Milan"),
            source_origin="diretta",
        ),
        Match(
            id="d_volley_cone",
            title="Conegliano D vs Novara D",
            category="volley",
            silo=SportSilo.VOLLEY,
            genres=["Champions League Femminile"],
            date=base_ms + 150 * 60_000,  # 19:30
            end_date=base_ms + 285 * 60_000,
            competition="Champions League Femminile",
            home_team=TeamInfo(name="Conegliano D"),
            away_team=TeamInfo(name="Novara D"),
            alt_home="Conegliano W",
            alt_away="Novara W",
            is_women=True,
            source_origin="diretta",
        ),
        Match(
            id="d_volley_scandicci",
            title="Scandicci D vs Tovuz D",
            category="volley",
            silo=SportSilo.VOLLEY,
            genres=["Champions League Femminile"],
            date=base_ms + 30 * 60_000,  # 17:30
            end_date=base_ms + 165 * 60_000,
            competition="Champions League Femminile",
            home_team=TeamInfo(name="Scandicci D"),
            away_team=TeamInfo(name="Tovuz D"),
            alt_home="Scandicci W",
            alt_away="Tovuz W",
            is_women=True,
            source_origin="diretta",
        ),
        Match(
            id="d_seriec_u23",
            title="Audace Cerignola vs Inter U23",
            category="calcio_ita",
            silo=SportSilo.CALCIO_ITA,
            genres=["Serie C"],
            date=base_ms + 30 * 60_000,
            end_date=base_ms + 165 * 60_000,
            competition="Serie C - Girone C",
            home_team=TeamInfo(name="Audace Cerignola"),
            away_team=TeamInfo(name="Inter U23"),
            is_youth=False,
            source_origin="diretta",
        ),
        Match(
            id="d_basket_dubai",
            title="Dubai vs Partizan",
            category="basket",
            silo=SportSilo.BASKET,
            genres=["Eurolega"],
            date=base_ms,
            end_date=base_ms + 150 * 60_000,
            competition="Eurolega",
            home_team=TeamInfo(name="Dubai"),
            away_team=TeamInfo(name="Partizan"),
            alt_away="Partizan Mozzart Bet",
            source_origin="diretta",
        ),
        Match(
            id="d_f1_fp1",
            title="FORMULA 1: GP Qatar (Qatar) - Prove Libere 1",
            category="motori",
            silo=SportSilo.MOTORI,
            genres=["Formula 1"],
            date=base_ms + 30 * 60_000,
            end_date=base_ms + 150 * 60_000,
            competition="FORMULA 1: GP Qatar (Qatar)",
            home_team=TeamInfo(name="FORMULA 1: GP Qatar (Qatar) - Prove Libere 1"),
            alt_home="FORMULA 1: Qatar Grand Prix (Qatar) - Practice 1",
            source_origin="diretta",
        ),
        Match(
            id="d_tennis_playford",
            title="Player A vs Player B",
            category="tennis",
            silo=SportSilo.TENNIS,
            genres=["Challenger"],
            date=base_ms + 60 * 60_000,
            end_date=base_ms + 240 * 60_000,
            competition="CHALLENGER UOMINI - SINGOLARE: Playford (Australia), cemento",
            home_team=TeamInfo(name="Player A"),
            away_team=TeamInfo(name="Player B"),
            source_origin="diretta",
        ),
        Match(
            id="d_davis_ita_kor",
            title="Italia vs Corea del Sud",
            category="tennis",
            silo=SportSilo.TENNIS,
            genres=["Coppa Davis"],
            date=base_ms + 60 * 60_000,
            end_date=base_ms + 240 * 60_000,
            competition="SQUADRE - UOMINI: Coppa Davis - Gruppo Mondiale (Mondo)",
            home_team=TeamInfo(name="Italia"),
            away_team=TeamInfo(name="Corea del Sud"),
            source_origin="diretta",
        ),
        Match(
            id="d_tennis_ended_old",
            title="Old A vs Old B",
            category="tennis",
            silo=SportSilo.TENNIS,
            genres=["WTA"],
            date=base_ms - 400 * 60_000,
            end_date=base_ms - 300 * 60_000,  # >5h ago
            competition="WTA - SINGOLARE: Palermo (Italia), terra",
            home_team=TeamInfo(name="Old A"),
            away_team=TeamInfo(name="Old B"),
            source_origin="diretta",
        ),
    ]

    epg_probe = [
        # FTA at exact kickoff (hl="") -> MUST attach to d_uel_tv8, NOT create orphan
        EPGProgram(
            id="ep_tv8_live",
            channel_id="8195",
            channel_name="TV8",
            is_dedicated_sports=False,
            title="Sturm Graz - Milan",
            competition_hint="UEFA Europa League",
            start_ms=base_ms + 180 * 60_000,
            end_ms=base_ms + 300 * 60_000,
            sport_hint="Calcio",
            program_highlights="",
            is_live=False,
            source="sky",
        ),
        # FTA delayed differita (2h15m after kickoff) -> MUST NOT attach & NOT orphan
        EPGProgram(
            id="ep_tv8_delay",
            channel_id="8195",
            channel_name="TV8",
            is_dedicated_sports=False,
            title="Olympiakos - Milan",
            competition_hint="UEFA Europa League",
            start_ms=base_ms + 180 * 60_000,
            end_ms=base_ms + 300 * 60_000,
            sport_hint="Calcio",
            program_highlights="",
            is_live=False,
            source="sky",
        ),
        # DAZN 1 Volley Femminile using content_title + year prefix in title
        EPGProgram(
            id="ep_cone",
            channel_id="11402",
            channel_name="DAZN 1",
            title="CEV Champions League F 2026 Conegliano - Novara",
            content_title="Conegliano - Novara",
            competition_hint="CEV Champions League Femminile",
            start_ms=base_ms + 150 * 60_000,
            end_ms=base_ms + 270 * 60_000,
            sport_hint="Volley",
            program_highlights="L",
            is_live=True,
            source="sky",
        ),
        # Scandicci D vs Tovuz D <-> Savino Del Bene Scandicci - Turan Tovuz
        EPGProgram(
            id="ep_scandicci",
            channel_id="9093",
            channel_name="Sky Sport Arena",
            title="Savino Del Bene Scandicci - Turan Tovuz",
            competition_hint="CEV Champions League Femminile",
            start_ms=base_ms + 25 * 60_000,
            end_ms=base_ms + 145 * 60_000,
            sport_hint="Volley",
            program_highlights="L",
            is_live=True,
            source="sky",
        ),
        # U23 B-team in Serie C senior
        EPGProgram(
            id="ep_u23",
            channel_id="9097",
            channel_name="Sky Sport Uno",
            title="Audace Cerignola - Inter U23",
            competition_hint="Serie C",
            start_ms=base_ms + 25 * 60_000,
            end_ms=base_ms + 150 * 60_000,
            sport_hint="Calcio",
            program_highlights="L",
            is_live=True,
            source="sky",
        ),
        # Basket with "Basketball" stopword
        EPGProgram(
            id="ep_dubai",
            channel_id="9116",
            channel_name="Sky Sport Basket",
            title="Dubai Basketball - Partizan Belgrado",
            competition_hint="Eurolega",
            start_ms=base_ms,
            end_ms=base_ms + 120 * 60_000,
            sport_hint="Basket",
            program_highlights="L",
            is_live=True,
            source="sky",
        ),
        # Motorsport track match -> attaches to d_f1_fp1
        EPGProgram(
            id="ep_f1_fp1",
            channel_id="9096",
            channel_name="Sky Sport F1",
            title="GP Qatar: Prove Libere 1",
            competition_hint="Formula 1",
            start_ms=base_ms + 20 * 60_000,
            end_ms=base_ms + 90 * 60_000,
            sport_hint="Motori",
            program_highlights="L",
            is_live=True,
            source="sky",
        ),
        # Dynamic Tennis tournament bridge (Playford without static city list!)
        EPGProgram(
            id="ep_playford",
            channel_id="9103",
            channel_name="Sky Sport Max",
            title="Challenger: Playford",
            start_ms=base_ms + 50 * 60_000,
            end_ms=base_ms + 170 * 60_000,
            sport_hint="Tennis",
            program_highlights="L",
            is_live=True,
            source="sky",
        ),
        # === 6 ALLOWED NON-1v1 ORPHAN CATEGORIES ===
        EPGProgram(
            id="orp_1_multicast",
            channel_id="11497",
            channel_name="Sky Sport 251",
            title="Diretta Gol Uel/ Uecl 26/11/2026 (Diretta)",
            competition_hint="UEFA Europa & Conference League",
            start_ms=base_ms,
            end_ms=base_ms + 120 * 60_000,
            sport_hint="Calcio",
            is_live=True,
            is_simulcast=True,
            source="sky",
        ),
        EPGProgram(
            id="orp_2_motori_track",
            channel_id="9096",
            channel_name="Sky Sport F1",
            title="F1: GP Qatar - Qualifiche",
            competition_hint="Formula 1",
            start_ms=base_ms + 300 * 60_000,
            end_ms=base_ms + 360 * 60_000,
            sport_hint="Motori",
            is_live=True,
            source="sky",
        ),
        EPGProgram(
            id="orp_3_tennis_multicourt",
            channel_id="6000",
            channel_name="SuperTennis",
            title="LIVE DAVIS CUP FINALS BY GENERALI",
            content_title="LIVE DAVIS CUP FINALS 8 BOLOGNA",
            start_ms=base_ms + 30 * 60_000,
            end_ms=base_ms + 150 * 60_000,
            sport_hint="Tennis",
            is_live=True,
            source="sky",
        ),
        EPGProgram(
            id="orp_4_golf",
            channel_id="10254",
            channel_name="Sky Sport Golf",
            title="BMW Australian PGA Championship: Giorno 1",
            competition_hint="DP World Tour",
            start_ms=base_ms,
            end_ms=base_ms + 180 * 60_000,
            sport_hint="Golf",
            is_live=True,
            source="sky",
        ),
        EPGProgram(
            id="orp_5_padel",
            channel_id="9093",
            channel_name="Sky Sport Arena",
            title="Premier Padel Milano: Giorno 1",
            competition_hint="Premier Padel Milano",
            start_ms=base_ms,
            end_ms=base_ms + 120 * 60_000,
            sport_hint="Altri",
            is_live=True,
            source="sky",
        ),
        EPGProgram(
            id="orp_6_cycling",
            channel_id="807",
            channel_name="Rai Sport",
            title="Giro d'Italia: Tappa 5",
            start_ms=base_ms,
            end_ms=base_ms + 120 * 60_000,
            sport_hint="Intrattenimento",
            is_live=True,
            source="sky",
        ),
        # === REJECTED ORPHANS ===
        EPGProgram(
            id="rej_1v1_football",
            channel_id="11490",
            channel_name="Sky Sport 254",
            title="Arsenal - Borussia Dortmund",
            competition_hint="UEFA Champions League",
            start_ms=base_ms,
            end_ms=base_ms + 120 * 60_000,
            sport_hint="Calcio",
            is_live=True,
            source="sky",
        ),
        EPGProgram(
            id="rej_1v1_basket",
            channel_id="9116",
            channel_name="Sky Sport Basket",
            title="Lituania - Serbia",
            competition_hint="FIBA World Cup Qualifiers",
            start_ms=base_ms,
            end_ms=base_ms + 120 * 60_000,
            sport_hint="Basket",
            is_live=True,
            source="sky",
        ),
        EPGProgram(
            id="rej_1v1_tennis",
            channel_id="11237",
            channel_name="Sky Sport Tennis",
            title="Sinner - Alcaraz",
            start_ms=base_ms,
            end_ms=base_ms + 120 * 60_000,
            sport_hint="Tennis",
            is_live=True,
            source="sky",
        ),
        EPGProgram(
            id="rej_moto_conf",
            channel_id="9096",
            channel_name="Sky Sport F1",
            title="F1 Conf. Stampa Piloti",
            start_ms=base_ms,
            end_ms=base_ms + 60 * 60_000,
            sport_hint="Motori",
            is_live=True,
            source="sky",
        ),
        EPGProgram(
            id="rej_moto_pit",
            channel_id="9096",
            channel_name="Sky Sport F1",
            title="F1 Paddock Live Pit Walk",
            start_ms=base_ms,
            end_ms=base_ms + 60 * 60_000,
            sport_hint="Motori",
            is_live=True,
            source="sky",
        ),
        EPGProgram(
            id="rej_bare_date",
            channel_id="9116",
            channel_name="Sky Sport Basket",
            title="26/11/2026",
            content_url="/sport/prepartita-qualificazioni-mondiali-basket/26-11-2026/9441",
            start_ms=base_ms,
            end_ms=base_ms + 30 * 60_000,
            sport_hint="Altri",
            is_live=True,
            source="sky",
        ),
        EPGProgram(
            id="rej_after_party",
            channel_id="9097",
            channel_name="Sky Sport Uno",
            title="After Party - La Notte Dei Campioni 26/11/2026",
            start_ms=base_ms,
            end_ms=base_ms + 60 * 60_000,
            sport_hint="Calcio",
            is_live=True,
            source="sky",
        ),
        EPGProgram(
            id="rej_tennis_stale",
            channel_id="6000",
            channel_name="SuperTennis",
            title="WTA Palermo",
            start_ms=base_ms,
            end_ms=base_ms + 120 * 60_000,
            sport_hint="Tennis",
            is_live=True,
            source="sky",
        ),
        EPGProgram(
            id="rej_generic_altri",
            channel_id="11909",
            channel_name="Sky Sport Mix",
            title="Torneo Esibizione Generico",
            start_ms=base_ms,
            end_ms=base_ms + 90 * 60_000,
            sport_hint="Altri",
            is_live=True,
            source="sky",
        ),
    ]

    enriched, orphans = reconcile_with_schedule(diretta_fixtures, epg_probe)
    by_id = {m.id: m for m in enriched}
    assert by_id["d_uel_tv8"].broadcasters == ["TV8"]
    assert by_id["d_uel_oly"].broadcasters == []  # 2h15m TV8 differita rejected
    assert by_id["d_volley_cone"].broadcasters == ["DAZN 1"]
    assert by_id["d_volley_scandicci"].broadcasters == ["Sky Sport Arena"]
    assert by_id["d_seriec_u23"].broadcasters == ["Sky Sport Uno"]
    assert by_id["d_basket_dubai"].broadcasters == ["Sky Sport Basket"]
    assert by_id["d_f1_fp1"].broadcasters == ["Sky Sport F1"]
    assert by_id["d_tennis_playford"].broadcasters == ["Sky Sport Max"]

    orphan_titles = {o.title for o in orphans}
    assert orphan_titles == {
        "Diretta Gol Uel/ Uecl 26/11/2026 (Diretta)",
        "F1: GP Qatar - Qualifiche",
        "LIVE DAVIS CUP FINALS BY GENERALI",
        "BMW Australian PGA Championship: Giorno 1",
        "Premier Padel Milano: Giorno 1",
        "Giro d'Italia: Tappa 5",
    }

    # B.4: Structural filter_conflicting_italian_channels
    m_1v1 = Match(
        id="m-conf",
        title="Milan - Fiorentina",
        category="calcio_ita",
        silo=SportSilo.CALCIO_ITA,
        date=base_ms,
        end_date=base_ms + 100 * 60_000,
        sources=[
            StreamSource(provider="tvvoo", title="Sky Sport Uno", url="http://x/uno", channel_name="sky sport uno"),
            StreamSource(provider="tvvoo", title="DAZN 1", url="http://x/d1", channel_name="dazn 1"),
        ],
    )
    ep_uno_studio = EPGProgram(
        id="ep_studio",
        channel_id="9097",
        channel_name="Sky Sport Uno",
        title="Sky Calcio Show 26/11/2026",
        content_url="/sport/sky-calcio-show/sky-calcio-show/1111",
        start_ms=base_ms,
        end_ms=base_ms + 90 * 60_000,
        sport_hint="Calcio",
        is_live=True,
        source="sky",
    )
    ep_dazn_sim = EPGProgram(
        id="ep_zona",
        channel_id="11402",
        channel_name="DAZN 1",
        title="Zona DAZN",
        start_ms=base_ms,
        end_ms=base_ms + 90 * 60_000,
        sport_hint="Calcio",
        is_live=True,
        is_simulcast=True,
        source="sky",
    )
    cleaned_conf = filter_conflicting_italian_channels([m_1v1], [ep_uno_studio, ep_dazn_sim])
    assert [s.title for s in cleaned_conf[0].sources] == ["Sky Sport Uno"]


def test_leagues_phase_c_targeted_bugfixes() -> None:
    from app.taxonomy import DEFAULT_LEAGUE_TAXONOMY

    a2 = DEFAULT_LEAGUE_TAXONOMY.match_league("basket", "Serie A2", "Italia")
    assert a2 is not None
    assert a2.canonical_slug == "serie-a2-basket"
    assert a2.tier == 2

    a1 = DEFAULT_LEAGUE_TAXONOMY.match_league("basket", "Serie A", "Italia")
    assert a1 is not None
    assert a1.canonical_slug == "lba-serie-a"
    assert a1.tier == 1

    # Hockey keeps ONLY NHL; DEL, KHL, Coppa del Mondo are excluded.
    nhl = DEFAULT_LEAGUE_TAXONOMY.match_league("hockey", "NHL", "USA")
    assert nhl is not None
    assert nhl.canonical_slug == "nhl"
    assert nhl.tier == 1
    assert DEFAULT_LEAGUE_TAXONOMY.match_league("hockey", "Coppa del Mondo", "") is None
    assert DEFAULT_LEAGUE_TAXONOMY.match_league("hockey", "DEL", "Germania") is None
    assert DEFAULT_LEAGUE_TAXONOMY.match_league("hockey", "KHL", "Russia") is None

    # Women's youth Euro must stay in youth national genre and Tier 3 (broadcaster-gated).
    u19_w = DEFAULT_LEAGUE_TAXONOMY.match_league(
        "football", "Campionati Europei U19 - Femminile", "Europa"
    )
    assert u19_w is not None
    assert u19_w.tier == 3
    assert u19_w.genre == "Europei Under 21 e Nazionali Giovanili"
    assert not DEFAULT_LEAGUE_TAXONOMY.is_admitted_event(
        "football", "Campionati Europei U19 - Femminile", "Europa", has_broadcasters=False
    )
    assert DEFAULT_LEAGUE_TAXONOMY.is_admitted_event(
        "football", "Campionati Europei U19 - Femminile", "Europa", has_broadcasters=True
    )


def test_phase_c_taxonomy_tiers_and_single_source_routing() -> None:
    import app.taxonomy.classifier as classifier_mod
    from app.taxonomy import DEFAULT_LEAGUE_TAXONOMY, classify_event, normalize_team_name

    # 1. Unrecognized leagues NEVER enter even with Diretta broadcasters (OTT leak fix)
    for silo, comp, country in (
        ("football", "Ligue 1", "Congo"),
        ("football", "Cymru Premier", "Galles"),
        ("football", "National League", "Inghilterra"),
    ):
        assert DEFAULT_LEAGUE_TAXONOMY.match_league(silo, comp, country) is None
        assert not DEFAULT_LEAGUE_TAXONOMY.is_admitted_event(
            silo, comp, country, has_broadcasters=True, has_streams=True
        )

    # 2. Coppa Italia (T1) vs Coppa Italia Serie D (T3) vs Serie D (rejected)
    ci = DEFAULT_LEAGUE_TAXONOMY.match_league("football", "Coppa Italia", "Italia")
    assert ci is not None and ci.canonical_slug == "coppa-italia" and ci.tier == 1
    assert DEFAULT_LEAGUE_TAXONOMY.is_admitted_event("football", "Coppa Italia", "Italia")

    cid = DEFAULT_LEAGUE_TAXONOMY.match_league("football", "Coppa Italia Serie D", "Italia")
    assert cid is not None and cid.canonical_slug == "coppa-italia-serie-d" and cid.tier == 3
    assert cid.catalog_target == "calcio_italiano" and cid.genre == "Coppa Italia e Supercoppa"
    assert not DEFAULT_LEAGUE_TAXONOMY.is_admitted_event(
        "football", "Coppa Italia Serie D", "Italia", has_broadcasters=False
    )
    assert DEFAULT_LEAGUE_TAXONOMY.is_admitted_event(
        "football", "Coppa Italia Serie D", "Italia", has_broadcasters=True
    )

    assert DEFAULT_LEAGUE_TAXONOMY.match_league("football", "Serie D - Girone A", "Italia") is None
    assert not DEFAULT_LEAGUE_TAXONOMY.is_admitted_event(
        "football", "Serie D - Girone A", "Italia", has_broadcasters=True
    )

    # 3. Premier League Cup (rejected) vs EFL Cup (T2); Liga Portugal vs 2; Coupe de France
    assert DEFAULT_LEAGUE_TAXONOMY.match_league("football", "Premier League Cup", "Inghilterra") is None
    assert DEFAULT_LEAGUE_TAXONOMY.match_league("football", "Premier League", "Inghilterra") is not None
    efl = DEFAULT_LEAGUE_TAXONOMY.match_league("football", "EFL Cup", "Inghilterra")
    assert efl is not None and efl.canonical_slug == "efl-cup" and efl.tier == 2
    assert DEFAULT_LEAGUE_TAXONOMY.match_league("football", "Liga Portugal", "Portogallo") is not None
    assert DEFAULT_LEAGUE_TAXONOMY.match_league("football", "Liga Portugal 2", "Portogallo") is None
    cdf = DEFAULT_LEAGUE_TAXONOMY.match_league("football", "Coppa di Francia", "Francia")
    assert cdf is not None and cdf.canonical_slug == "coupe-de-france" and cdf.tier == 1

    # 4. UEFA Champions League (T1) vs AFC / Gulf Champions League (T3 Extra-EU, no women)
    ucl = DEFAULT_LEAGUE_TAXONOMY.match_league("football", "Champions League - Prima fase", "Europa")
    assert ucl is not None and ucl.canonical_slug == "champions-league" and ucl.tier == 1
    afc = DEFAULT_LEAGUE_TAXONOMY.match_league("football", "AFC Champions League 2", "Asia")
    assert afc is not None and afc.canonical_slug == "afc-champions-league" and afc.tier == 3
    assert afc.genre == "Americhe e Leghe Extra-UE"
    gulf = DEFAULT_LEAGUE_TAXONOMY.match_league("football", "Gulf Club Champions League", "Asia")
    assert gulf is not None and gulf.canonical_slug == "afc-champions-league" and gulf.tier == 3
    assert DEFAULT_LEAGUE_TAXONOMY.match_league("football", "AFC Women's Champions League", "Asia") is None

    # 5. Tennis: ZERO Challenger / ITF even with broadcasters
    assert DEFAULT_LEAGUE_TAXONOMY.is_admitted_event("tennis", "ATP - SINGOLARE: Torino (Italia)", "")
    assert DEFAULT_LEAGUE_TAXONOMY.is_admitted_event("tennis", "WTA - SINGOLARE: Roma (Italia)", "")
    assert DEFAULT_LEAGUE_TAXONOMY.is_admitted_event("tennis", "Wimbledon", "")
    assert DEFAULT_LEAGUE_TAXONOMY.is_admitted_event("tennis", "Coppa Davis", "")
    for minor_t in (
        "CHALLENGER UOMINI - SINGOLARE: Playford 2 (Australia)",
        "ITF UOMINI - SINGOLARE: M15 Antalya 9 (Turchia)",
        "ITF DONNE - SINGOLARE: W75 Florianopolis",
    ):
        assert DEFAULT_LEAGUE_TAXONOMY.match_league("tennis", minor_t, "") is None
        assert not DEFAULT_LEAGUE_TAXONOMY.is_admitted_event(
            "tennis", minor_t, "", has_broadcasters=True
        )

    # 6. Basket: NBA, FIBA IT/EN, Eurolega Femminile (T1), EuroCup Femminile (T3), zero NCAA / G League
    assert DEFAULT_LEAGUE_TAXONOMY.is_admitted_event("basket", "NBA", "USA")
    assert DEFAULT_LEAGUE_TAXONOMY.match_league("basket", "NBA G League - Preliminari", "USA") is None
    assert DEFAULT_LEAGUE_TAXONOMY.match_league("basket", "NCAA", "USA") is None
    fiba_it = DEFAULT_LEAGUE_TAXONOMY.match_league("basket", "Mondiali - Qualificazione - Secondo turno", "Europa")
    fiba_en = DEFAULT_LEAGUE_TAXONOMY.match_league("basket", "World Cup - Qualification - Second round", "Australasia")
    assert fiba_it is not None and fiba_it.canonical_slug == "fiba-international" and fiba_it.tier == 1
    assert fiba_en is not None and fiba_en.canonical_slug == "fiba-international" and fiba_en.tier == 1
    el_w = DEFAULT_LEAGUE_TAXONOMY.match_league("basket", "Eurolega Femminile - Prima fase", "Europa")
    assert el_w is not None and el_w.tier == 1 and el_w.genre == "WNBA e Femminile"
    ec_w = DEFAULT_LEAGUE_TAXONOMY.match_league("basket", "EuroCup Femminile", "Europa")
    assert ec_w is not None and ec_w.tier == 3 and ec_w.genre == "WNBA e Femminile"
    assert not DEFAULT_LEAGUE_TAXONOMY.is_admitted_event("basket", "EuroCup Femminile", "Europa", has_broadcasters=False)
    assert DEFAULT_LEAGUE_TAXONOMY.is_admitted_event("basket", "EuroCup Femminile", "Europa", has_broadcasters=True)

    # 7. Volley: SuperLega IT vs SuperLiga BR, CEV CL M/F (T1), CEV Cup & Challenge Cup (T3), zero PlusLiga
    assert DEFAULT_LEAGUE_TAXONOMY.match_league("volley", "SuperLega", "Italia") is not None
    assert DEFAULT_LEAGUE_TAXONOMY.match_league("volley", "SuperLiga", "Brasile") is None
    cev_w = DEFAULT_LEAGUE_TAXONOMY.match_league("volley", "Champions League Femminile", "Europa")
    assert cev_w is not None and cev_w.tier == 1 and cev_w.genre == "Volley Femminile"
    cev_cup_w = DEFAULT_LEAGUE_TAXONOMY.match_league("volley", "CEV Cup Femminile", "Europa")
    assert cev_cup_w is not None and cev_cup_w.tier == 3 and cev_cup_w.genre == "Volley Femminile"
    chal_w = DEFAULT_LEAGUE_TAXONOMY.match_league("volley", "Challenge Cup Femminile", "Europa")
    assert chal_w is not None and chal_w.tier == 3 and chal_w.genre == "Volley Femminile"
    assert DEFAULT_LEAGUE_TAXONOMY.match_league("volley", "PlusLiga", "Polonia") is None

    # 8. Rugby & Handball
    nat = DEFAULT_LEAGUE_TAXONOMY.match_league("rugby", "Nations Championship - Play Off", "Mondo")
    assert nat is not None and nat.canonical_slug == "nations-championship" and nat.tier == 1
    assert DEFAULT_LEAGUE_TAXONOMY.match_league("rugby", "Top 14", "Francia") is None
    assert DEFAULT_LEAGUE_TAXONOMY.match_league("rugby", "Six Nations Women", "Europa") is None
    assert DEFAULT_LEAGUE_TAXONOMY.match_league("rugby", "Test Match", "Mondo") is None
    ehf = DEFAULT_LEAGUE_TAXONOMY.match_league("handball", "Champions League Uomini", "Europa")
    assert ehf is not None and ehf.tier == 3 and ehf.genre == "Pallamano"
    assert DEFAULT_LEAGUE_TAXONOMY.match_league("handball", "2. Bundesliga", "Germania") is None

    # 9. Football Youth: Primavera 1, UEFA Youth League, Mondiali U17 T3 only; foreign club youth blocked
    for y_ok, y_country in (
        ("Primavera 1", "Italia"),
        ("UEFA Youth League - Prima fase", "Europa"),
        ("Mondiali U17", "Mondo"),
    ):
        ym = DEFAULT_LEAGUE_TAXONOMY.match_league("football", y_ok, y_country)
        assert ym is not None and ym.tier == 3
        assert not DEFAULT_LEAGUE_TAXONOMY.is_admitted_event("football", y_ok, y_country, has_broadcasters=False)
        assert DEFAULT_LEAGUE_TAXONOMY.is_admitted_event("football", y_ok, y_country, has_broadcasters=True)

    for y_bad, y_country in (
        ("DFB Youth League - Preliminari", "Germania"),
        ("Paraense U20 - Play Off", "Brasile"),
        ("U19 League", "Ucraina"),
        ("Liga MX U21", "Messico"),
        ("Premier League U18", "Inghilterra"),
    ):
        assert DEFAULT_LEAGUE_TAXONOMY.match_league("football", y_bad, y_country) is None
        assert not DEFAULT_LEAGUE_TAXONOMY.is_admitted_event("football", y_bad, y_country, has_broadcasters=True)

    # 10. Combat: UFC/Boxe/WWE T1, Bellator T3
    bellator = DEFAULT_LEAGUE_TAXONOMY.match_league("combat", "Bellator", "USA")
    assert bellator is not None and bellator.tier == 3
    assert DEFAULT_LEAGUE_TAXONOMY.match_league("combat", "Cage Warriors", "Europa") is None

    # 11. Single Source of Truth in classifier + safe Vero Volley alias
    assert not hasattr(classifier_mod, "_CANONICAL_SLUG_ROUTING")
    assert classify_event(canonical_competition="coppa-italia-serie-d") == (
        "calcio_italiano",
        ["Coppa Italia e Supercoppa"],
    )
    assert classify_event(canonical_competition="eurocup-basket-women") == (
        "basket",
        ["WNBA e Femminile"],
    )
    assert classify_event(canonical_competition="euro-championship-women-youth") == (
        "calcio_estero",
        ["Europei Under 21 e Nazionali Giovanili"],
    )
    assert normalize_team_name("Vero Volley") == "vero volley milano"
    assert normalize_team_name("Milano") == "milano"


def test_phase_c_post_deploy_epg_and_classifier_polishes() -> None:
    from app.ingestion.epg_service import _infer_silo_from_epg, _parse_teams_from_text
    from app.taxonomy.classifier import classify_event

    assert _parse_teams_from_text(
        "Benevento - Meta Catania - Stag. 2026 Ep. 10 - Benevento - Meta Catania (diretta)"
    ) == ("Benevento - Meta Catania", "Benevento", "Meta Catania")
    assert _parse_teams_from_text("Paris Saint-Germain - Al-Hilal") == (
        "",
        "Paris Saint-Germain",
        "Al-Hilal",
    )

    solocalcio_epg = EPGProgram(
        id="legacy_1",
        channel_id="295",
        channel_name="SOLOCALCIO",
        title="Campionato Argentino Rosario Central-Gimnasia Mdz",
        sport_hint="Calcio",
        start_ms=1_795_611_600_000,
        end_ms=1_795_618_800_000,
        source="sky",
    )
    assert _infer_silo_from_epg(solocalcio_epg) == SportSilo.FOOTBALL

    assert classify_event(
        silo="tennis",
        competition="Tennis",
        title="LIVE DAVIS CUP FINALS BY GENERALI",
    ) == ("tennis", ["Coppa Davis e BJK Cup"])
    assert classify_event(
        silo="tennis",
        competition="Tennis",
        title="Lauren Davis vs Coco Gauff",
        is_women=True,
    ) == ("tennis", ["WTA"])


def test_sky_diretta_matcher_window_aliases_and_initial_guard() -> None:
    from app.ingestion.epg_service import _epg_matches_diretta
    from app.taxonomy.matcher import DEFAULT_TEAM_MATCHER, _string_similarity

    # 1. Exonyms & canonical disjointness (AS Monaco != Bayern Monaco, Olanda == Paesi Bassi)
    monaco_vs_bayern = DEFAULT_TEAM_MATCHER.match_score(
        "AS Monaco", "PSG", "Bayern Monaco", "PSG"
    )
    assert monaco_vs_bayern.matched is False
    assert monaco_vs_bayern.min_side_score == 0.0

    bayern_exonym = DEFAULT_TEAM_MATCHER.match_score(
        "Bayern Munich", "Lilla", "FC Bayern Monaco", "Lille"
    )
    assert bayern_exonym.matched is True

    nl_exonym = DEFAULT_TEAM_MATCHER.match_score(
        "Olanda", "Israele", "Paesi Bassi", "Israele"
    )
    assert nl_exonym.matched is True

    # 2. Cross-order tennis initial+surname guard ("Muller A." vs "G. Muller")
    muller_conflict_1 = DEFAULT_TEAM_MATCHER.match_score(
        "Muller A.", "Sinner J.", "G. Muller", "J. Sinner"
    )
    assert muller_conflict_1.matched is False

    muller_conflict_2 = DEFAULT_TEAM_MATCHER.match_score(
        "Alexandre Muller", "Sinner J.", "Muller G.", "J. Sinner"
    )
    assert muller_conflict_2.matched is False

    muller_same = DEFAULT_TEAM_MATCHER.match_score(
        "Muller A.", "Sinner J.", "A. Muller", "J. Sinner"
    )
    assert muller_same.matched is True

    # 3. 7-char single-token subset damping ("monaco" vs "fake monaco")
    assert _string_similarity("monaco", "fake monaco") < 0.85

    # 4. Aligned 60m standard window (FTA without overlap) + >60m Motori (120m) & Tennis 1v1 (360m)
    base_ms = 1_795_611_600_000
    fta_match = Match(
        id="diretta-fta-50m",
        title="Olympiakos vs Milan",
        category="calcio_estero",
        silo=SportSilo.FOOTBALL,
        genres=["Europa e Conference League"],
        date=base_ms + 50 * 60_000,
        end_date=base_ms + 185 * 60_000,
        competition="Europa League - Prima fase",
        home_team=TeamInfo(name="Olympiakos"),
        away_team=TeamInfo(name="Milan"),
        source_origin="diretta",
    )
    fta_epg_50m = EPGProgram(
        id="sky_tv8_50m",
        channel_id="8195",
        channel_name="TV8",
        is_dedicated_sports=False,
        title="Olympiakos - Milan",
        start_ms=base_ms,
        end_ms=base_ms + 170 * 60_000,
        sport_hint="Calcio",
        program_highlights="",
        is_live=False,
        source="sky",
    )
    assert _epg_matches_diretta(fta_epg_50m, fta_match) > 0.0

    f1_match = Match(
        id="diretta-f1-90m",
        title="FORMULA 1: GP Qatar - Gara",
        category="motori",
        silo=SportSilo.MOTORI,
        genres=["Formula 1"],
        date=base_ms + 90 * 60_000,
        end_date=base_ms + 210 * 60_000,
        competition="FORMULA 1: GP Qatar (Qatar)",
        home_team=TeamInfo(name="FORMULA 1: GP Qatar (Qatar) - Gara"),
        source_origin="diretta",
    )
    f1_epg_90m = EPGProgram(
        id="sky_f1_90m",
        channel_id="9096",
        channel_name="Sky Sport F1",
        title="GP Qatar: Gara",
        start_ms=base_ms,
        end_ms=base_ms + 210 * 60_000,
        sport_hint="Motori",
        competition_hint="Formula 1",
        is_live=True,
        source="sky",
    )
    assert _epg_matches_diretta(f1_epg_90m, f1_match) > 0.0

    tennis_match = Match(
        id="diretta-ten-120m",
        title="Sinner J. vs Alcaraz C.",
        category="tennis",
        silo=SportSilo.TENNIS,
        genres=["ATP"],
        date=base_ms + 120 * 60_000,
        end_date=base_ms + 240 * 60_000,
        competition="ATP Masters 1000 Roma",
        home_team=TeamInfo(name="Sinner J."),
        away_team=TeamInfo(name="Alcaraz C."),
        source_origin="diretta",
    )
    tennis_epg_120m = EPGProgram(
        id="sky_ten_120m",
        channel_id="11237",
        channel_name="Sky Sport Tennis",
        title="Jannik Sinner - Carlos Alcaraz",
        start_ms=base_ms,
        end_ms=base_ms + 240 * 60_000,
        sport_hint="Tennis",
        is_live=True,
        source="sky",
    )
    assert _epg_matches_diretta(tennis_epg_120m, tennis_match) > 0.0


def test_sky_diretta_asymmetric_women_comp_conflict_and_time_tiebreak() -> None:
    from app.ingestion.epg_service import _epg_matches_diretta
    from app.taxonomy.matcher import DEFAULT_TEAM_MATCHER

    base_ms = 1_795_611_600_000

    # 1. Asymmetric women penalty: neutral Sky matches Women-only Diretta,
    #    prefers Men when both exist, and still blocks Women Sky -> Men Diretta.
    assert (
        DEFAULT_TEAM_MATCHER.match_score(
            "Vero Volley D", "Levallois Paris SC D",
            "Milano", "Levallois",
            is_women_1=True, is_women_2=False,
        ).matched
        is False
    )

    volley_w_dir = Match(
        id="dir-volley-w",
        title="Vero Volley D vs Levallois Paris SC D",
        category="volley",
        silo=SportSilo.VOLLEY,
        genres=["Champions League Volley"],
        date=base_ms,
        end_date=base_ms + 120 * 60_000,
        competition="Champions League Femminile",
        canonical_competition="cev-champions-league-women",
        country="Europa",
        home_team=TeamInfo(name="Vero Volley D"),
        away_team=TeamInfo(name="Levallois Paris SC D"),
        is_women=True,
        source_origin="diretta",
    )
    dazn_neutral = EPGProgram(
        id="sky-dazn-neutral",
        channel_id="11402",
        channel_name="DAZN 1",
        title="Milano - Levallois",
        start_ms=base_ms,
        end_ms=base_ms + 120 * 60_000,
        sport_hint="Volley",
        is_live=True,
        source="sky",
    )
    assert _epg_matches_diretta(dazn_neutral, volley_w_dir) > 0.0

    juve_roma_m = Match(
        id="dir-jr-m",
        title="Juventus vs Roma",
        category="calcio_italiano",
        silo=SportSilo.CALCIO_ITA,
        genres=["Serie A"],
        date=base_ms,
        end_date=base_ms + 135 * 60_000,
        competition="Serie A",
        canonical_competition="serie-a",
        country="Italia",
        home_team=TeamInfo(name="Juventus"),
        away_team=TeamInfo(name="Roma"),
        is_women=False,
        source_origin="diretta",
    )
    juve_roma_w = Match(
        id="dir-jr-w",
        title="Juventus D vs Roma D",
        category="calcio_italiano",
        silo=SportSilo.CALCIO_ITA,
        genres=["Calcio Femminile"],
        date=base_ms,
        end_date=base_ms + 135 * 60_000,
        competition="Serie A Femminile",
        canonical_competition="serie-a-femminile",
        country="Italia",
        home_team=TeamInfo(name="Juventus D"),
        away_team=TeamInfo(name="Roma D"),
        is_women=True,
        source_origin="diretta",
    )
    sky_jr_neutral = EPGProgram(
        id="sky-jr-neutral",
        channel_id="9097",
        channel_name="Sky Sport Uno",
        title="Juventus - Roma",
        start_ms=base_ms,
        end_ms=base_ms + 120 * 60_000,
        sport_hint="Calcio",
        is_live=True,
        source="sky",
    )
    sky_jr_explicit_w = sky_jr_neutral.model_copy(
        update={"id": "sky-jr-w", "title": "Juventus Femminile - Roma Femminile"}
    )
    assert _epg_matches_diretta(sky_jr_neutral, juve_roma_m) > _epg_matches_diretta(
        sky_jr_neutral, juve_roma_w
    )
    assert _epg_matches_diretta(sky_jr_explicit_w, juve_roma_m) == 0.0
    assert _epg_matches_diretta(sky_jr_explicit_w, juve_roma_w) > 0.0

    # 2. Certified competition conflict guard (-0.30) vs generic URL (0.0) & base-slug women tolerance
    sky_serie_a = sky_jr_neutral.model_copy(update={"competition_hint": "Serie A"})
    juve_roma_coppa = juve_roma_m.model_copy(
        update={"id": "dir-jr-coppa", "competition": "Coppa Italia", "canonical_competition": "coppa-italia"}
    )
    assert _epg_matches_diretta(sky_serie_a, juve_roma_coppa) == 0.0
    assert _epg_matches_diretta(sky_jr_neutral, juve_roma_coppa) > 0.0

    uwcl_dir = Match(
        id="dir-uwcl",
        title="Barcellona D vs Roma D",
        category="calcio_estero",
        silo=SportSilo.FOOTBALL,
        genres=["Champions League Femminile"],
        date=base_ms,
        end_date=base_ms + 135 * 60_000,
        competition="Champions League Femminile",
        canonical_competition="uefa-womens-champions-league",
        country="Europa",
        home_team=TeamInfo(name="Barcellona D"),
        away_team=TeamInfo(name="Roma D"),
        is_women=True,
        source_origin="diretta",
    )
    sky_ucl_w = EPGProgram(
        id="sky-uwcl",
        channel_id="9097",
        channel_name="Sky Sport Uno",
        title="Barcellona - Roma",
        competition_hint="UEFA Champions League",
        start_ms=base_ms,
        end_ms=base_ms + 120 * 60_000,
        sport_hint="Calcio",
        is_live=True,
        source="sky",
    )
    assert _epg_matches_diretta(sky_ucl_w, uwcl_dir) > 0.0

    # 3. Time proximity tie-break (+0.03 max): 5m beats 60m
    sky_5m = sky_serie_a.model_copy(update={"start_ms": base_ms - 5 * 60_000})
    sky_60m = sky_serie_a.model_copy(update={"start_ms": base_ms - 60 * 60_000})
    s5 = _epg_matches_diretta(sky_5m, juve_roma_m)
    s60 = _epg_matches_diretta(sky_60m, juve_roma_m)
    assert s5 > s60 > 0.0








