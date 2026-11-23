"""StreamOrchestrator: concurrent harvesting, top-down stream matching, sorting, and Stremio formatting."""

from __future__ import annotations

import asyncio
import importlib
import logging
import re
import time
from datetime import datetime, timedelta, timezone, tzinfo
from typing import Any, Optional, Sequence
from urllib.parse import quote, quote_plus, urlsplit
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from app.config import Settings, get_settings
from app.core.user_config import UserConfig
from app.models.domain import Match, ProviderEvent, SportSilo, StreamSource
from app.models.stremio import StreamItem
from app.providers.base import BaseStreamProvider, ProviderHarvestResult
from app.providers.daddylive_service import (
    MAX_CHANNELS_PER_EVENT,
    DaddyLiveService,
    _extract_channel_id,
)
from app.providers.fctv33_service import Fctv33Service
from app.providers.mandrakodi_service import MandraKodiService
from app.providers.streamfree_service import StreamFreeService
from app.taxonomy import (
    DEFAULT_TEAM_MATCHER,
    SKY_CHID_TO_CANONICAL,
    canonicalize_channel_name,
    detect_channel_flag,
    detect_demographics_from_stream_title,
)

logger = logging.getLogger(__name__)

PRE_MATCH_WINDOW_MS = 20 * 60 * 1000
MAX_TIME_DIFF_MS = DEFAULT_TEAM_MATCHER.max_time_diff_ms  # 60 minutes

_PROVIDER_RANK: dict[str, int] = {
    "tvvoo": 1,
    "mandrakodi": 2,
    "spon": 3,
    "daddylive": 4,
    "streamfree": 5,
    "fctv33": 6,
}

_LIVE_WINDOW_MINUTES: dict[SportSilo, int] = {
    SportSilo.CALCIO_ITA: 135,
    SportSilo.FOOTBALL: 135,
    SportSilo.TENNIS: 240,
    SportSilo.MOTORI: 180,
}
_DEFAULT_LIVE_WINDOW_MINUTES = 150

_EVENT_SPLIT_RE = re.compile(r"\s+(?:vs\.?|v\.?|@|–|—|-)\s+", re.IGNORECASE)
_VALID_DLHD_CID_RE = re.compile(r"^[0-9a-zA-Z_-]{1,32}$")
_FCTV33_URL_ID_RE = re.compile(r"^fctv33://(\d+)(?:/(\d+))?(?:.*[?&]sportType=(\d+))?", re.IGNORECASE)
_IT_BROADCASTER_RE = re.compile(
    r"\b(?:sky\s*sport|dazn|rai\s*(?:1|2|3|sport)?|canale\s*5|italia\s*1|rete\s*4|"
    r"tv8|nove|cielo|mediaset|supertennis|solocalcio|equtv|zona\s*dazn)\b",
    re.IGNORECASE,
)
_SPON_PRIORITY_RE = re.compile(r"\b(?:ita|italian|hd\s*7|hd\s*8|hd7|hd8)\b", re.IGNORECASE)
_DLHD_IT_RE = re.compile(
    r"\b(?:it|ita|italia|italian|sky\s*sport|dazn|rai|mediaset|supertennis|tv8|nove)\b",
    re.IGNORECASE,
)
_DLHD_EN_RE = re.compile(
    r"\b(?:en|eng|uk|usa|us|english|tnt|bt\s*sport|espn|fox|cbs|nbc|sky\s*sports)\b",
    re.IGNORECASE,
)
_CANONICAL_TO_SKY_CHID: dict[str, str] = {
    canonical: chid for chid, canonical in SKY_CHID_TO_CANONICAL.items()
}

_PROXY_HOST_DEFAULTS: dict[str, str] = {
    "daddylive": "dlstreams",
    "streamfree": "streamfree",
    "fctv33": "fctv33",
    "tvvoo": "vavoo",
}

_PROVIDER_SPECS: tuple[tuple[str, str], ...] = (
    ("app.providers.tvvoo_service", "TvVooService"),
    ("app.providers.mandrakodi_service", "MandraKodiService"),
    ("app.providers.spon_service", "SponService"),
    ("app.providers.daddylive_service", "DaddyLiveService"),
    ("app.providers.streamfree_service", "StreamFreeService"),
    ("app.providers.fctv33_service", "Fctv33Service"),
)


def _last_sunday(year: int, month: int) -> datetime:
    nxt = (
        datetime(year + 1, 1, 1, tzinfo=timezone.utc)
        if month == 12
        else datetime(year, month + 1, 1, tzinfo=timezone.utc)
    )
    last_day = nxt - timedelta(days=1)
    offset = (last_day.weekday() + 1) % 7
    return last_day - timedelta(days=offset)


class _EuropeRomeFallback(tzinfo):
    """Exact EU CET/CEST fallback when system tzdata is unavailable."""

    def fromutc(self, dt: datetime) -> datetime:
        utc_naive = dt.replace(tzinfo=None)
        start_utc = _last_sunday(utc_naive.year, 3).replace(tzinfo=None, hour=1)
        end_utc = _last_sunday(utc_naive.year, 10).replace(tzinfo=None, hour=1)
        offset = timedelta(hours=2) if start_utc <= utc_naive < end_utc else timedelta(hours=1)
        return (utc_naive + offset).replace(tzinfo=self)

    def utcoffset(self, dt: Optional[datetime]) -> timedelta:
        if dt is None:
            return timedelta(hours=1)
        naive = dt.replace(tzinfo=None)
        start_local = _last_sunday(naive.year, 3).replace(tzinfo=None, hour=2)
        end_local = _last_sunday(naive.year, 10).replace(tzinfo=None, hour=3)
        return timedelta(hours=2) if start_local <= naive < end_local else timedelta(hours=1)

    def dst(self, dt: Optional[datetime]) -> timedelta:
        return timedelta(hours=1) if self.utcoffset(dt) == timedelta(hours=2) else timedelta(0)

    def tzname(self, dt: Optional[datetime]) -> str:
        return "CEST" if self.utcoffset(dt) == timedelta(hours=2) else "CET"


def _load_rome_tz() -> tzinfo:
    try:
        return ZoneInfo("Europe/Rome")
    except ZoneInfoNotFoundError:
        return _EuropeRomeFallback()


_ROME_TZ: tzinfo = _load_rome_tz()


def normalize_channel_name(name: str) -> str:
    """Normalize linear TV broadcaster names via the shared canonical channel taxonomy."""
    return canonicalize_channel_name(name)


def _format_stream_name(base_label: str, src: StreamSource) -> str:
    """Prefix a country flag on `StreamItem.name` when detectable."""
    flag = detect_channel_flag(
        f"{src.channel_name or ''} {src.title or ''}".strip(),
        src.language or "",
    )
    if not flag or flag in base_label:
        return base_label
    parts = base_label.split(" ", 1)
    if len(parts) == 2 and parts[0] and not parts[0][0].isalnum():
        return f"{parts[0]} {flag} {parts[1]}"
    return f"{flag} {base_label}"


def _is_italian_channel(src: StreamSource) -> bool:
    if (src.language or "").strip().lower() in ("it", "ita"):
        return True
    combined = f"{src.channel_name} {src.title}"
    return bool(_IT_BROADCASTER_RE.search(combined))


def _source_sort_key(src: StreamSource) -> tuple[int, int, int]:
    """Return strict hierarchy sort key for `Match.sources`:
    1: tvvoo (IT first), 2: mandrakodi, 3: spon (ITA/HD7/HD8 first),
    4: daddylive (IT -> EN -> other), 5: streamfree, 6: fctv33 (IT first), 7: acestream.
    """
    if src.is_acestream:
        return (7, 0, src.priority)

    provider = (src.provider or "").strip().lower()
    tier = _PROVIDER_RANK.get(provider, 6)

    sub_rank = 1
    if provider == "tvvoo":
        sub_rank = 0 if _is_italian_channel(src) else 1
    elif provider == "mandrakodi":
        sub_rank = 0 if src.clearkey else 1
    elif provider == "spon":
        combined = f"{src.language} {src.title} {src.channel_name}"
        if (src.language or "").lower() == "it" or _SPON_PRIORITY_RE.search(combined):
            sub_rank = 0
    elif provider == "daddylive":
        lang = (src.language or "").lower()
        combined = f"{src.channel_name} {src.title}"
        if lang == "it" or _DLHD_IT_RE.search(combined):
            sub_rank = 0
        elif lang == "en" or _DLHD_EN_RE.search(combined):
            sub_rank = 1
        else:
            sub_rank = 2
    elif provider == "fctv33":
        lang = (src.language or "").lower()
        combined = f"{src.channel_name} {src.title}"
        sub_rank = 0 if (lang in ("it", "ita") or _DLHD_IT_RE.search(combined)) else 1

    return (tier, sub_rank, src.priority)


def _split_provider_teams(ev: ProviderEvent) -> tuple[str, str]:
    if ev.home.strip():
        return ev.home.strip(), ev.away.strip()
    cleaned = re.sub(r"\s+", " ", (ev.title or "").strip())
    if ":" in cleaned:
        _, rest = cleaned.split(":", 1)
        if rest.strip():
            cleaned = rest.strip()
    parts = _EVENT_SPLIT_RE.split(cleaned, maxsplit=1)
    if len(parts) == 2 and parts[0].strip() and parts[1].strip():
        return parts[0].strip(), parts[1].strip()
    return cleaned, ""


def _build_default_providers(settings: Settings) -> list[BaseStreamProvider]:
    providers: list[BaseStreamProvider] = []
    for module_path, class_name in _PROVIDER_SPECS:
        try:
            mod = importlib.import_module(module_path)
            cls = getattr(mod, class_name)
            providers.append(cls(settings=settings))
        except ImportError:
            logger.debug("Provider module %s not available yet, skipping", module_path)
    if not any(isinstance(p, DaddyLiveService) for p in providers):
        providers.append(DaddyLiveService(settings=settings))
    if not any(isinstance(p, StreamFreeService) for p in providers):
        providers.append(StreamFreeService(settings=settings))
    if not any(isinstance(p, Fctv33Service) for p in providers):
        providers.append(Fctv33Service(settings=settings))
    return providers


def _live_window_ms(silo: SportSilo) -> int:
    minutes = _LIVE_WINDOW_MINUTES.get(silo, _DEFAULT_LIVE_WINDOW_MINUTES)
    return minutes * 60 * 1000


def _build_easyproxy_headers_qs(headers: dict[str, str], *, quote_plus_vals: bool = False) -> str:
    parts: list[str] = []
    encoder = quote_plus if quote_plus_vals else (lambda v: quote(v, safe=""))
    for key, val in (headers or {}).items():
        if key and val:
            parts.append(f"&h_{quote(key, safe='')}={encoder(val)}")
    return "".join(parts)


class StreamOrchestrator:
    """Coordinates concurrent stream harvesting, matching, sorting, and Stremio formatting."""

    def __init__(
        self,
        providers: Optional[Sequence[BaseStreamProvider]] = None,
        *,
        settings: Optional[Settings] = None,
    ) -> None:
        self._settings = settings or get_settings()
        self._providers: list[BaseStreamProvider] = (
            list(providers)
            if providers is not None
            else _build_default_providers(self._settings)
        )

    @property
    def providers(self) -> list[BaseStreamProvider]:
        """Return configured stream provider instances."""
        return list(self._providers)

    async def aclose(self) -> None:
        """Close all provider clients cleanly."""
        await asyncio.gather(
            *(p.aclose() for p in self._providers),
            return_exceptions=True,
        )

    async def harvest_all(self) -> dict[str, ProviderHarvestResult]:
        """Run all enabled providers concurrently via `asyncio.gather`."""
        active = [p for p in self._providers if p.enabled]
        if not active:
            return {}

        raw_results = await asyncio.gather(
            *(p.safe_harvest() for p in active),
            return_exceptions=True,
        )
        results: dict[str, ProviderHarvestResult] = {}
        for provider, outcome in zip(active, raw_results):
            if isinstance(outcome, BaseException):
                results[provider.name] = ProviderHarvestResult(
                    provider=provider.name, error=str(outcome)
                )
            else:
                results[provider.name] = outcome
        return results

    def _build_linear_index(
        self,
        harvest_results: dict[str, ProviderHarvestResult],
    ) -> dict[str, list[StreamSource]]:
        """Merge `linear_channels` across TvVoo, MandraKodi, DaddyLive with Italian priority."""
        index: dict[str, list[StreamSource]] = {}
        for res in harvest_results.values():
            for raw_key, sources in (res.linear_channels or {}).items():
                norm_key = normalize_channel_name(raw_key)
                if norm_key:
                    index.setdefault(norm_key, []).extend(sources)
                for src in sources:
                    ch_name = src.channel_name
                    if not ch_name or ch_name == raw_key or ch_name == norm_key:
                        continue
                    alt_key = normalize_channel_name(ch_name)
                    if alt_key and alt_key != norm_key:
                        index.setdefault(alt_key, []).append(src)
        return index

    def _match_linear_channels(
        self,
        match: Match,
        linear_index: dict[str, list[StreamSource]],
        lookup_cache: Optional[dict[str, list[StreamSource]]] = None,
    ) -> list[StreamSource]:
        if not match.broadcasters or not linear_index:
            return []
        attached: list[StreamSource] = []
        for broadcaster in match.broadcasters:
            key = normalize_channel_name(broadcaster)
            if not key:
                continue
            if lookup_cache is not None and key in lookup_cache:
                candidates = lookup_cache[key]
            else:
                candidates = linear_index.get(key) or []
                if not candidates and len(key) >= 4:
                    # Word-boundary fallback for compound labels without matching e.g. "rai 1" -> "rai 10"
                    pat: Optional[re.Pattern[str]] = None
                    for idx_key, idx_sources in linear_index.items():
                        if key in idx_key:
                            if pat is None:
                                pat = re.compile(rf"(?<!\w){re.escape(key)}(?!\w)")
                            if pat.search(idx_key):
                                candidates = idx_sources
                                break
                        elif len(idx_key) >= 4 and idx_key in key:
                            if re.search(rf"(?<!\w){re.escape(idx_key)}(?!\w)", key):
                                candidates = idx_sources
                                break
                if lookup_cache is not None:
                    lookup_cache[key] = candidates
            if candidates:
                attached.extend(candidates)
        return attached

    def _match_provider_events(
        self,
        matches: list[Match],
        harvest_results: dict[str, ProviderHarvestResult],
    ) -> dict[int, list[StreamSource]]:
        """Time-bucketed bilateral RapidFuzz matching of `ProviderEvent`s onto `Match`es."""
        if not matches:
            return {}

        # Group match indices by 1-hour UTC bucket for fast lookup across +-60m window
        bucket_map: dict[int, list[int]] = {}
        untimed_indices: list[int] = []
        for idx, m in enumerate(matches):
            if m.date > 0:
                bucket = m.date // MAX_TIME_DIFF_MS
                bucket_map.setdefault(bucket, []).append(idx)
            else:
                untimed_indices.append(idx)

        all_indices = list(range(len(matches)))
        attached_by_match: dict[int, list[StreamSource]] = {}

        for res in harvest_results.values():
            for ev in res.events or []:
                if not ev.streams:
                    continue
                pe_home, pe_away = _split_provider_teams(ev)
                if not pe_home:
                    continue

                if ev.start_ms > 0:
                    ev_bucket = ev.start_ms // MAX_TIME_DIFF_MS
                    candidate_indices = list(untimed_indices)
                    for b in (ev_bucket - 1, ev_bucket, ev_bucket + 1):
                        candidate_indices.extend(bucket_map.get(b, ()))
                else:
                    candidate_indices = all_indices

                is_women_2 = ev.is_women
                is_youth_2 = ev.is_youth
                if not is_women_2 or not is_youth_2:
                    tw, ty = detect_demographics_from_stream_title(ev.title)
                    is_women_2 = is_women_2 or tw
                    is_youth_2 = is_youth_2 or ty

                best_idx: Optional[int] = None
                best_score = 0.0
                for idx in candidate_indices:
                    m = matches[idx]
                    h1 = m.home_team.name if m.home_team else m.title
                    a1 = m.away_team.name if m.away_team else ""
                    res_match = DEFAULT_TEAM_MATCHER.match_score(
                        h1,
                        a1,
                        pe_home,
                        pe_away,
                        alt_home_1=m.alt_home or (m.competition if not a1 else ""),
                        alt_away_1=m.alt_away,
                        date_1_ms=m.date,
                        date_2_ms=ev.start_ms,
                        is_women_1=m.is_women,
                        is_youth_1=m.is_youth,
                        is_women_2=is_women_2,
                        is_youth_2=is_youth_2,
                    )
                    if res_match.matched and res_match.score > best_score:
                        best_score = res_match.score
                        best_idx = idx

                if best_idx is not None:
                    attached_by_match.setdefault(best_idx, []).extend(ev.streams)

        return attached_by_match

    @staticmethod
    def _finalize_sources(raw_sources: list[StreamSource]) -> list[StreamSource]:
        """Sort by strict 7-tier hierarchy, cap DaddyLive at 30, and deduplicate URLs."""
        ordered = sorted(raw_sources, key=_source_sort_key)
        deduped: list[StreamSource] = []
        seen_urls: set[str] = set()
        dlhd_count = 0

        for src in ordered:
            dedup_key = (src.url or src.raw_url).strip()
            if not dedup_key or dedup_key in seen_urls:
                continue
            if (src.provider or "").lower() == "daddylive" and not src.is_acestream:
                if dlhd_count >= MAX_CHANNELS_PER_EVENT:
                    continue
                dlhd_count += 1
            seen_urls.add(dedup_key)
            deduped.append(src)
        return deduped

    def attach_streams(
        self,
        matches: list[Match],
        harvest_results: dict[str, ProviderHarvestResult],
    ) -> list[Match]:
        """Attach linear & event streams to `matches`, filter orphan EPG ghosts, and sort."""
        if not matches:
            return []

        linear_index = self._build_linear_index(harvest_results)
        broadcaster_cache: dict[str, list[StreamSource]] = {}
        event_matches = self._match_provider_events(matches, harvest_results)

        updated: list[Match] = []
        for idx, match in enumerate(matches):
            combined_sources: list[StreamSource] = list(match.sources)
            combined_sources.extend(
                self._match_linear_channels(match, linear_index, lookup_cache=broadcaster_cache)
            )
            combined_sources.extend(event_matches.get(idx, ()))

            final_sources = self._finalize_sources(combined_sources)

            # Orphan EPG Ghost Filter: drop EPG-only events with zero attached streams
            if match.source_origin == "epg" and not final_sources:
                continue

            has_tvvoo = any(
                (s.provider or "").lower() == "tvvoo" and not s.is_acestream
                for s in final_sources
            )
            updated.append(
                match.model_copy(
                    update={
                        "sources": final_sources,
                        "has_tvvoo": has_tvvoo,
                    }
                )
            )
        return updated

    def build_stremio_streams(
        self,
        match: Match,
        user_cfg: UserConfig,
        base_url: str,
        *,
        config_segment: str = "",
        now_ms: Optional[int] = None,
        use_daddylive_jit: bool = False,
    ) -> list[StreamItem]:
        """Format Stremio `StreamItem` list respecting live window, countdown, and EasyProxy."""
        current_ms = int(now_ms if now_ms is not None else time.time() * 1000)
        window_ms = _live_window_ms(match.silo)
        if match.date > 0 and current_ms > match.date + window_ms:
            return []

        base = base_url.rstrip("/")
        clean_seg = config_segment.strip("/")
        prefix = f"/{clean_seg}" if clean_seg else ""
        ep_url = (user_cfg.ep_url or "").strip().rstrip("/")
        ep_pass = (user_cfg.ep_pass or "").strip()
        pass_qs = f"&api_password={quote(ep_pass, safe='')}" if ep_pass else ""

        items: list[StreamItem] = []
        is_pre_match = match.date > 0 and current_ms < (match.date - PRE_MATCH_WINDOW_MS)

        if is_pre_match:
            diff_ms = max(0, match.date - current_ms)
            total_minutes = max(1, diff_ms // 60_000)
            hours, mins = divmod(total_minutes, 60)
            countdown = f"{hours}h {mins:02d}m" if hours > 0 else f"{mins}m"
            rome_dt = datetime.fromtimestamp(
                match.date / 1000.0, tz=timezone.utc
            ).astimezone(_ROME_TZ)
            rome_hhmm = rome_dt.strftime("%H:%M")
            items.append(
                StreamItem(
                    name=f"⏳ Tra {countdown}",
                    title=(
                        f"Inizio tra {countdown} (ore {rome_hhmm} Europe/Rome)\n"
                        f"{match.title}"
                    ),
                    behaviorHints={"notWebReady": True},
                )
            )

        candidate_sources = (
            [
                s
                for s in match.sources
                if (s.provider or "").lower() == "tvvoo" or bool(s.channel_name)
            ]
            if is_pre_match
            else list(match.sources)
        )

        acestream_items: list[StreamItem] = []
        missing_easyproxy = False

        for src in candidate_sources:
            if src.is_acestream:
                acestream_items.append(
                    StreamItem(
                        name=_format_stream_name("🔗 AceStream", src),
                        title=src.title,
                        externalUrl=src.url,
                    )
                )
                continue

            provider = (src.provider or "").strip().lower()
            raw_target = src.raw_url or src.url

            if provider == "tvvoo":
                if user_cfg.prefer_direct_tvvoo:
                    manifest_url = (
                        f"{base}{prefix}/live/manifest.m3u8"
                        f"?url={quote(raw_target, safe='')}"
                    )
                    items.append(
                        StreamItem(
                            name=_format_stream_name("⚡ TvVoo", src),
                            title=f"[⚡] {src.title} (No-Freeze)",
                            url=manifest_url,
                        )
                    )
                if user_cfg.show_easyproxy_tvvoo:
                    if ep_url:
                        ep_stream_url = (
                            f"{ep_url}/extractor/video.m3u8"
                            f"?host=vavoo&d={quote(raw_target, safe='')}"
                            f"&redirect_stream=true{pass_qs}"
                        )
                        items.append(
                            StreamItem(
                                name=_format_stream_name("🛰️ TvVoo EP", src),
                                title=f"[🛰️ EP] {src.title}",
                                url=ep_stream_url,
                            )
                        )
                    elif not user_cfg.prefer_direct_tvvoo:
                        missing_easyproxy = True
                continue

            if provider == "mandrakodi" and src.clearkey:
                if ep_url:
                    hdr_qs = _build_easyproxy_headers_qs(src.headers)
                    mpd_url = (
                        f"{ep_url}/proxy/mpd/manifest.m3u8"
                        f"?d={quote(src.url, safe='')}"
                        f"&clearkey={quote(src.clearkey, safe='')}"
                        f"{pass_qs}{hdr_qs}"
                    )
                    items.append(
                        StreamItem(
                            name=_format_stream_name("🔐 MandraKodi", src),
                            title=src.title,
                            url=mpd_url,
                        )
                    )
                else:
                    missing_easyproxy = True
                continue

            if not src.requires_proxy:
                items.append(
                    StreamItem(
                        name=_format_stream_name(f"📺 {provider.upper() or 'DIRECT'}", src),
                        title=src.title,
                        url=src.url,
                    )
                )
                continue

            if not ep_url:
                missing_easyproxy = True
                continue

            if provider == "daddylive" and use_daddylive_jit:
                cid = _extract_channel_id(src.raw_url or src.url)
                if cid and _VALID_DLHD_CID_RE.match(cid):
                    items.append(
                        StreamItem(
                            name=_format_stream_name("🛰️ DADDYLIVE", src),
                            title=src.title,
                            url=f"{base}{prefix}/daddylive/stream/{cid}.m3u8",
                        )
                    )
                    continue

            if provider == "spon":
                hdr_qs = _build_easyproxy_headers_qs(src.headers, quote_plus_vals=True)
                stream_url = (
                    f"{ep_url}/proxy/manifest.m3u8"
                    f"?url={quote_plus(src.url)}{pass_qs}{hdr_qs}"
                )
            elif provider == "streamfree" and not src.proxy_host:
                hdr_qs = _build_easyproxy_headers_qs(src.headers, quote_plus_vals=True)
                stream_url = (
                    f"{ep_url}/proxy/manifest.m3u8"
                    f"?url={quote_plus(src.url)}{hdr_qs}{pass_qs}"
                )
            else:
                proxy_host = src.proxy_host or _PROXY_HOST_DEFAULTS.get(provider, "")
                d_target = ((src.raw_url or "").strip() or src.url) if provider == "fctv33" else src.url
                if proxy_host in ("hls", "mpd"):
                    hdr_qs = _build_easyproxy_headers_qs(src.headers)
                    stream_url = (
                        f"{ep_url}/proxy/{proxy_host}/manifest.m3u8"
                        f"?d={quote(d_target, safe='')}{pass_qs}{hdr_qs}"
                    )
                elif proxy_host:
                    hdr_qs = _build_easyproxy_headers_qs(src.headers)
                    stream_url = (
                        f"{ep_url}/extractor/video.m3u8"
                        f"?host={quote(proxy_host, safe='')}"
                        f"&d={quote(d_target, safe='')}"
                        f"&redirect_stream=true{pass_qs}{hdr_qs}"
                    )
                else:
                    hdr_qs = _build_easyproxy_headers_qs(src.headers, quote_plus_vals=True)
                    stream_url = (
                        f"{ep_url}/proxy/manifest.m3u8"
                        f"?url={quote_plus(d_target)}{pass_qs}{hdr_qs}"
                    )

            items.append(
                StreamItem(
                    name=_format_stream_name(f"🛰️ {provider.upper() or 'PROXY'}", src),
                    title=src.title,
                    url=stream_url,
                )
            )

        if missing_easyproxy:
            items.append(
                StreamItem(
                    name="⚙️ EasyProxy Richiesto",
                    title="⚙️ Configura EasyProxy su /configure",
                    externalUrl=f"{base}/configure",
                )
            )

        items.extend(acestream_items)
        return items

    def _find_fctv33_jit_target(
        self,
        match: Match,
        fctv_service: Fctv33Service,
    ) -> Optional[tuple[str, int]]:
        for src in match.sources:
            if (src.provider or "").strip().lower() != "fctv33":
                continue
            m = _FCTV33_URL_ID_RE.match(src.url or "")
            if m:
                mid, sid, stype = m.group(1), m.group(2), int(m.group(3) or 1)
                if not sid or sid == "0":
                    return mid, stype

        if not fctv_service.known_matches:
            return None

        h1 = match.home_team.name if match.home_team else match.title
        a1 = match.away_team.name if match.away_team else ""
        best_target: Optional[tuple[str, int]] = None
        best_score = 0.0
        for cand in fctv_service.known_matches:
            c_title = cand.get("title") or ""
            cw, cy = detect_demographics_from_stream_title(c_title)
            if cand.get("competition"):
                comp_w, comp_y = detect_demographics_from_stream_title(cand["competition"])
                cw = cw or comp_w
                cy = cy or comp_y
            res = DEFAULT_TEAM_MATCHER.match_score(
                h1,
                a1,
                cand.get("home") or "",
                cand.get("away") or "",
                alt_home_1=match.alt_home or (match.competition if not a1 else ""),
                alt_away_1=match.alt_away,
                date_1_ms=match.date,
                date_2_ms=int(cand.get("start_ms") or 0),
                is_women_1=match.is_women,
                is_youth_1=match.is_youth,
                is_women_2=cw,
                is_youth_2=cy,
            )
            if res.matched and res.score > best_score:
                best_score = res.score
                best_target = (str(cand["match_id"]), int(cand.get("sport_type") or 1))
        return best_target

    async def build_stremio_streams_async(
        self,
        match: Match,
        user_cfg: UserConfig,
        base_url: str,
        *,
        config_segment: str = "",
        now_ms: Optional[int] = None,
    ) -> list[StreamItem]:
        """Resolve JIT StreamFree, MandraKodi Sky, FCTV33 detail, and DaddyLive routes."""
        current_ms = int(now_ms if now_ms is not None else time.time() * 1000)
        window_ms = _live_window_ms(match.silo)
        if match.date > 0 and current_ms > match.date + window_ms:
            return []

        ep_url = (user_cfg.ep_url or "").strip().rstrip("/")
        sf_service = next(
            (p for p in self._providers if isinstance(p, StreamFreeService) and p.enabled),
            None,
        )
        mk_service = next(
            (p for p in self._providers if isinstance(p, MandraKodiService) and p.enabled),
            None,
        )
        dl_service = next(
            (p for p in self._providers if isinstance(p, DaddyLiveService) and p.enabled),
            None,
        )
        fctv_service = next(
            (p for p in self._providers if isinstance(p, Fctv33Service) and p.enabled),
            None,
        )
        is_pre_match = match.date > 0 and current_ms < (match.date - PRE_MATCH_WINDOW_MS)

        if not ep_url or is_pre_match:
            return self.build_stremio_streams(
                match,
                user_cfg,
                base_url,
                config_segment=config_segment,
                now_ms=current_ms,
                use_daddylive_jit=False,
            )

        # 1. StreamFree dynamic _0x JIT tasks
        sf_indices = (
            [
                idx
                for idx, src in enumerate(match.sources)
                if (src.provider or "").strip().lower() == "streamfree"
                and ".m3u8" not in src.url.lower()
                and (src.raw_url or "/player/" in src.url)
            ]
            if sf_service is not None
            else []
        )

        async def _resolve_sf_one(src: StreamSource) -> list[dict[str, str]]:
            assert sf_service is not None
            parsed = urlsplit(src.url)
            path_cat = ""
            path_key = ""
            if "/player/" in (parsed.path or ""):
                parts = [p for p in parsed.path.split("/") if p]
                if len(parts) >= 3:
                    path_cat, path_key = parts[1], parts[2]
            category = (path_cat or src.channel_name or "soccer").strip().lower()
            raw_key = (path_key or src.raw_url or "").strip()
            if not raw_key:
                return []
            parsed_domain = f"{parsed.scheme}://{parsed.netloc}" if parsed.netloc else None
            domain = sf_service.active_mirror or parsed_domain
            return await sf_service.resolve_stream_url(raw_key, category, domain=domain)

        # 2. MandraKodi Sky A1A159 JIT + SuperTennis OTT tasks (with FreeShot already on match.sources)
        sky_chids: list[str] = []
        probe_supertennis = False
        if mk_service is not None:
            existing_mk_mpd = {
                normalize_channel_name(s.channel_name or s.title)
                for s in match.sources
                if (s.provider or "").lower() == "mandrakodi" and s.proxy_host == "mpd"
            }
            if (mk_service.settings.MANDRAKODI_API_URL or "").strip():
                for broadcaster in match.broadcasters:
                    canon = normalize_channel_name(broadcaster)
                    chid = _CANONICAL_TO_SKY_CHID.get(canon)
                    if chid and canon not in existing_mk_mpd and chid not in sky_chids:
                        sky_chids.append(chid)
            if match.silo == SportSilo.TENNIS and any(
                normalize_channel_name(b) == "supertennis" for b in match.broadcasters
            ):
                probe_supertennis = True

        # 3. FCTV33 JIT /api/match/detail if match lacks concrete FCTV33 streams
        has_concrete_fctv = any(
            (s.provider or "").strip().lower() == "fctv33"
            and (
                ":" in (s.raw_url or "")
                or (":" in (s.url or "") and not s.url.lower().startswith("fctv33://"))
                or (
                    (_m := _FCTV33_URL_ID_RE.match(s.url or "")) is not None
                    and bool(_m.group(2))
                    and _m.group(2) != "0"
                )
            )
            for s in match.sources
        )
        fctv_target = (
            self._find_fctv33_jit_target(match, fctv_service)
            if (fctv_service is not None and not has_concrete_fctv)
            else None
        )

        # Execute all independent JIT resolutions concurrently
        sf_task = asyncio.gather(
            *(_resolve_sf_one(match.sources[idx]) for idx in sf_indices),
            return_exceptions=True,
        )
        mk_sky_task = asyncio.gather(
            *(mk_service.resolve_sky_channel(chid) for chid in sky_chids),  # type: ignore[union-attr]
            return_exceptions=True,
        )
        st_ott_task = (
            mk_service.get_active_supertennis_ott_streams()  # type: ignore[union-attr]
            if probe_supertennis
            else asyncio.sleep(0, result=[])
        )
        fctv_task = (
            fctv_service.resolve_match_streams(fctv_target[0], sport_type=fctv_target[1])  # type: ignore[union-attr]
            if fctv_target is not None
            else asyncio.sleep(0, result=[])
        )

        resolved_batches, mk_sky_results, st_ott_results, fctv_results = await asyncio.gather(
            sf_task,
            mk_sky_task,
            st_ott_task,
            fctv_task,
            return_exceptions=True,
        )

        new_sources: list[StreamSource] = []
        idx_map = (
            dict(zip(sf_indices, resolved_batches))
            if isinstance(resolved_batches, list)
            else {}
        )
        for idx, src in enumerate(match.sources):
            outcome = idx_map.get(idx)
            if isinstance(outcome, list) and outcome:
                seen_urls: set[str] = set()
                added = 0
                ch_label = (src.channel_name or "").strip()
                flag = detect_channel_flag(ch_label, src.language or "") or "🇬🇧"
                for entry in outcome:
                    m3u8_url = (entry.get("m3u8_url") or "").strip()
                    if not m3u8_url or m3u8_url in seen_urls:
                        continue
                    seen_urls.add(m3u8_url)
                    qual = (entry.get("quality") or src.quality or "FHD").strip()
                    title = (
                        f"{flag} [StreamFree] {ch_label} [{qual}]"
                        if ch_label
                        else f"{flag} [StreamFree] {qual}"
                    )
                    new_sources.append(
                        StreamSource(
                            provider="streamfree",
                            title=title,
                            url=m3u8_url,
                            quality=qual,
                            language=src.language,
                            priority=src.priority,
                            requires_proxy=True,
                            proxy_host="",
                            headers={
                                "referer": entry.get("embed_url", src.url),
                                "origin": entry.get("origin", ""),
                            },
                            channel_name=src.channel_name,
                            raw_url=src.raw_url,
                        )
                    )
                    added += 1
                    if added >= 3:
                        break
            else:
                new_sources.append(src)

        extra_sources: list[StreamSource] = []
        if isinstance(mk_sky_results, list):
            for item in mk_sky_results:
                if isinstance(item, StreamSource):
                    extra_sources.append(item)
        if isinstance(st_ott_results, list):
            for item in st_ott_results:
                if isinstance(item, StreamSource):
                    extra_sources.append(item)
        if isinstance(fctv_results, list) and fctv_results:
            new_sources = [
                s
                for s in new_sources
                if not ((s.provider or "").strip().lower() == "fctv33")
            ]
            for item in fctv_results:
                if isinstance(item, StreamSource):
                    extra_sources.append(item)

        if extra_sources:
            new_sources = self._finalize_sources(new_sources + extra_sources)

        resolved_match = match.model_copy(update={"sources": new_sources})
        return self.build_stremio_streams(
            resolved_match,
            user_cfg,
            base_url,
            config_segment=config_segment,
            now_ms=current_ms,
            use_daddylive_jit=bool(dl_service is not None),
        )
