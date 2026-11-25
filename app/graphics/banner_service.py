"""Dynamic 889x500 Banner Service with LIVE/TIME badges, TvVoo Ribbon, and single-flight locks."""

from __future__ import annotations

import asyncio
import hashlib
import io
import logging
import os
import time
import weakref
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from PIL import Image, ImageDraw, ImageOps

from app.config import Settings, get_settings
from app.core.database import DatabaseManager
from app.graphics._utils import (
    get_timezone,
    image_to_jpeg_bytes,
    load_font,
    measure_text,
    safe_match_filename,
    strip_non_bmp,
)
from app.graphics.tennis_poster import TennisPosterService
from app.graphics.thesportsdb_service import TheSportsDBService, render_dark_canvas
from app.models.domain import Match, SportSilo, get_match_expiry_ms

logger = logging.getLogger(__name__)

BANNER_WIDTH = 889
BANNER_HEIGHT = 500
BANNER_CACHE_TTL_SECONDS = 60.0
PRE_LIVE_WINDOW_MS = 20 * 60 * 1000

_PIL_EXECUTOR = ThreadPoolExecutor(max_workers=4, thread_name_prefix="banner-pil")

_SPORT_LIVE_WINDOW_MINUTES: dict[SportSilo, int] = {
    SportSilo.CALCIO_ITA: 135,
    SportSilo.FOOTBALL: 135,
    SportSilo.TENNIS: 240,
    SportSilo.MOTORI: 180,
}
_DEFAULT_LIVE_WINDOW_MINUTES = 150


def get_live_window_minutes(silo: SportSilo) -> int:
    """Return sport-specific live duration window in minutes."""
    return _SPORT_LIVE_WINDOW_MINUTES.get(silo, _DEFAULT_LIVE_WINDOW_MINUTES)


def compute_badge_state(
    match: Match,
    *,
    now_ms: Optional[int] = None,
    timezone_name: Optional[str] = None,
) -> tuple[str, str]:
    """Compute `(badge_state, badge_label)` -> `('LIVE', 'LIVE')` or `('TIME', 'OGGI • HH:MM')`.

    Never produces a Replay badge.
    """
    current_ms = now_ms if now_ms is not None else int(time.time() * 1000)
    live_start = match.date - PRE_LIVE_WINDOW_MS
    if match.end_date > 0:
        live_end = get_match_expiry_ms(match)
    else:
        live_win_ms = get_live_window_minutes(match.silo) * 60 * 1000
        live_end = match.date + live_win_ms

    if live_start <= current_ms <= live_end:
        return "LIVE", "LIVE"

    tz = get_timezone(timezone_name or get_settings().TIMEZONE)
    now_local = datetime.fromtimestamp(current_ms / 1000.0, tz=timezone.utc).astimezone(tz)
    match_local = datetime.fromtimestamp(match.date / 1000.0, tz=timezone.utc).astimezone(tz)

    today = now_local.date()
    match_day = match_local.date()
    hm = match_local.strftime("%H:%M")

    if match_day == today:
        return "TIME", f"OGGI • {hm}"
    if match_day == today + timedelta(days=1):
        return "TIME", f"DOMANI • {hm}"
    return "TIME", f"{match_local.strftime('%d/%m')} • {hm}"


def _draw_live_pill(canvas: Image.Image) -> None:
    """Draw red `#E5192D` LIVE pill with glowing white dot in top-left corner."""
    font = load_font(20, bold=True)
    text = "LIVE"
    dummy_draw = ImageDraw.Draw(canvas)
    tw, th = measure_text(dummy_draw, text, font)

    pad_x = 16
    dot_area = 24
    pill_h = 38
    pill_w = pad_x + dot_area + tw + pad_x
    x0, y0 = 20, 18

    overlay = Image.new("RGBA", canvas.size, (0, 0, 0, 0))
    odraw = ImageDraw.Draw(overlay)

    # Subtle outer shadow
    odraw.rounded_rectangle(
        (x0 + 2, y0 + 3, x0 + pill_w + 2, y0 + pill_h + 3),
        radius=pill_h // 2,
        fill=(0, 0, 0, 120),
    )
    # Red pill #E5192D
    odraw.rounded_rectangle(
        (x0, y0, x0 + pill_w, y0 + pill_h),
        radius=pill_h // 2,
        fill=(229, 25, 45, 245),
        outline=(255, 255, 255, 70),
        width=1,
    )

    # Glowing / pulsing-style white dot
    dot_cx = x0 + pad_x + 7
    dot_cy = y0 + pill_h // 2
    odraw.ellipse((dot_cx - 10, dot_cy - 10, dot_cx + 10, dot_cy + 10), fill=(255, 255, 255, 65))
    odraw.ellipse((dot_cx - 7, dot_cy - 7, dot_cx + 7, dot_cy + 7), fill=(255, 255, 255, 130))
    odraw.ellipse((dot_cx - 4, dot_cy - 4, dot_cx + 4, dot_cy + 4), fill=(255, 255, 255, 255))

    text_x = x0 + pad_x + dot_area
    text_y = y0 + (pill_h - th) // 2 - 2
    odraw.text((text_x, text_y), text, font=font, fill=(255, 255, 255, 255))
    canvas.alpha_composite(overlay)


def _draw_time_pill(canvas: Image.Image, label: str) -> None:
    """Draw dark obsidian glass pill with vector clock icon + time label in top-left."""
    clean_label = strip_non_bmp(label)
    font = load_font(19, bold=True)
    dummy_draw = ImageDraw.Draw(canvas)
    tw, th = measure_text(dummy_draw, clean_label, font)

    pad_x = 15
    icon_area = 26
    pill_h = 38
    pill_w = pad_x + icon_area + tw + pad_x
    x0, y0 = 20, 18

    overlay = Image.new("RGBA", canvas.size, (0, 0, 0, 0))
    odraw = ImageDraw.Draw(overlay)

    odraw.rounded_rectangle(
        (x0 + 2, y0 + 3, x0 + pill_w + 2, y0 + pill_h + 3),
        radius=pill_h // 2,
        fill=(0, 0, 0, 115),
    )
    odraw.rounded_rectangle(
        (x0, y0, x0 + pill_w, y0 + pill_h),
        radius=pill_h // 2,
        fill=(12, 16, 24, 218),
        outline=(255, 255, 255, 65),
        width=1,
    )

    # Crisp vector clock icon
    cx = x0 + pad_x + 8
    cy = y0 + pill_h // 2
    r = 8
    odraw.ellipse(
        (cx - r, cy - r, cx + r, cy + r),
        outline=(226, 232, 240, 245),
        width=2,
    )
    odraw.line((cx, cy, cx, cy - 4), fill=(255, 255, 255, 255), width=2)
    odraw.line((cx, cy, cx + 4, cy + 1), fill=(255, 255, 255, 255), width=2)

    text_x = x0 + pad_x + icon_area
    text_y = y0 + (pill_h - th) // 2 - 2
    odraw.text((text_x, text_y), clean_label, font=font, fill=(248, 250, 252, 255))
    canvas.alpha_composite(overlay)


def _draw_tvvoo_ribbon(canvas: Image.Image) -> None:
    """Draw golden `#F5C518` corner ribbon with black lightning bolt in top-right corner."""
    patch_size = 136
    scale = 2
    hi = patch_size * scale

    ribbon_hi = Image.new("RGBA", (hi, hi), (0, 0, 0, 0))
    rdraw = ImageDraw.Draw(ribbon_hi)

    # Diagonal golden band across top-right corner (2x supersampled for anti-aliasing).
    outer_offset = 66 * scale
    inner_offset = 122 * scale
    shadow_shift = 4 * scale

    rdraw.polygon(
        [
            (hi - inner_offset - shadow_shift, 0),
            (hi - outer_offset + shadow_shift, 0),
            (hi, outer_offset - shadow_shift),
            (hi, inner_offset + shadow_shift),
        ],
        fill=(0, 0, 0, 95),
    )
    rdraw.polygon(
        [
            (hi - inner_offset, 0),
            (hi - outer_offset, 0),
            (hi, outer_offset),
            (hi, inner_offset),
        ],
        fill=(245, 197, 24, 252),
    )

    # Crisp black lightning bolt centered on the diagonal ribbon.
    bx = int(hi - (outer_offset + inner_offset) * 0.25)
    by = int((outer_offset + inner_offset) * 0.25)
    bolt = [
        (bx + 3 * scale, by - 14 * scale),
        (bx - 8 * scale, by + 2 * scale),
        (bx - 1 * scale, by + 2 * scale),
        (bx - 4 * scale, by + 15 * scale),
        (bx + 9 * scale, by - 2 * scale),
        (bx + 1 * scale, by - 2 * scale),
    ]
    rdraw.polygon(bolt, fill=(14, 16, 20, 255))

    ribbon = ribbon_hi.resize((patch_size, patch_size), Image.Resampling.LANCZOS)
    canvas.alpha_composite(ribbon, dest=(canvas.width - patch_size, 0))


def _compose_banner_sync(
    base_bytes: bytes,
    badge_state: str,
    badge_label: str,
    has_tvvoo: bool,
    fallback_title: str,
    fallback_competition: str,
) -> bytes:
    """Resize/crop base poster to 889x500 and composite badge + optional TvVoo ribbon."""
    try:
        with Image.open(io.BytesIO(base_bytes)) as src:
            canvas = ImageOps.fit(
                src.convert("RGBA"),
                (BANNER_WIDTH, BANNER_HEIGHT),
                method=Image.Resampling.LANCZOS,
            )
    except Exception:
        dark_bytes = render_dark_canvas(
            fallback_title,
            fallback_competition,
            width=BANNER_WIDTH,
            height=BANNER_HEIGHT,
        )
        with Image.open(io.BytesIO(dark_bytes)) as src:
            canvas = src.convert("RGBA")

    if badge_state == "LIVE":
        _draw_live_pill(canvas)
    else:
        _draw_time_pill(canvas, badge_label)

    if has_tvvoo:
        _draw_tvvoo_ribbon(canvas)

    return image_to_jpeg_bytes(canvas, quality=88)


def cleanup_poster_files(
    match_ids: list[str],
    *,
    base_dir: Optional[Path] = None,
    poster_dir: Optional[Path] = None,
) -> int:
    """Remove cached base and banner JPEGs for purged match IDs."""
    settings = get_settings()
    base_cache = base_dir or settings.BASE_POSTER_CACHE_DIR
    banner_cache = poster_dir or settings.POSTER_CACHE_DIR
    removed = 0

    for match_id in match_ids:
        safe_id = safe_match_filename(match_id)
        base_file = base_cache / f"{safe_id}.jpg"
        if base_file.is_file():
            try:
                base_file.unlink()
                removed += 1
            except OSError as exc:
                logger.debug("Failed to delete base poster %s: %s", base_file, exc)

        if banner_cache.is_dir():
            for candidate in banner_cache.glob(f"{safe_id}*.jpg"):
                if candidate.stem == safe_id or candidate.stem.startswith(f"{safe_id}__"):
                    try:
                        candidate.unlink()
                        removed += 1
                    except OSError as exc:
                        logger.debug("Failed to delete banner poster %s: %s", candidate, exc)

    return removed


class BannerService:
    """Serves 889x500 dynamic match banners with single-flight locking & 60s disk cache."""

    def __init__(
        self,
        settings: Optional[Settings] = None,
        tsdb_service: Optional[TheSportsDBService] = None,
        tennis_service: Optional[TennisPosterService] = None,
    ) -> None:
        self._settings = settings or get_settings()
        self._tsdb_service = tsdb_service
        self._tennis_service = tennis_service
        self._locks: weakref.WeakValueDictionary[tuple[str, str, bool], asyncio.Lock] = (
            weakref.WeakValueDictionary()
        )

    def _get_lock(self, key: tuple[str, str, bool]) -> asyncio.Lock:
        lock = self._locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[key] = lock
        return lock

    def _get_tsdb(self) -> TheSportsDBService:
        if self._tsdb_service is None:
            self._tsdb_service = TheSportsDBService(settings=self._settings)
        return self._tsdb_service

    def _get_tennis(self) -> TennisPosterService:
        if self._tennis_service is None:
            self._tennis_service = TennisPosterService(settings=self._settings)
        return self._tennis_service

    @staticmethod
    def _banner_cache_path(
        poster_dir: Path,
        safe_id: str,
        badge_state: str,
        badge_label: str,
        has_tvvoo: bool,
    ) -> Path:
        sig = hashlib.sha1(
            f"{badge_state}|{badge_label}|{int(has_tvvoo)}".encode("utf-8")
        ).hexdigest()[:10]
        return poster_dir / f"{safe_id}__{sig}.jpg"

    @staticmethod
    async def _atomic_write(path: Path, data: bytes) -> None:
        def _write() -> None:
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_suffix(f".tmp.{os.getpid()}")
            tmp.write_bytes(data)
            os.replace(tmp, path)

        await asyncio.to_thread(_write)

    @staticmethod
    def _is_fresh(path: Path, ttl_seconds: float = BANNER_CACHE_TTL_SECONDS) -> bool:
        try:
            st = path.stat()
        except OSError:
            return False
        if st.st_size <= 0:
            return False
        return (time.time() - st.st_mtime) < ttl_seconds

    async def _resolve_base_poster(
        self,
        match: Match,
        db: Optional[DatabaseManager],
    ) -> bytes:
        safe_id = safe_match_filename(match.id)
        base_path = self._settings.BASE_POSTER_CACHE_DIR / f"{safe_id}.jpg"
        if base_path.is_file() and base_path.stat().st_size > 0:
            return await asyncio.to_thread(base_path.read_bytes)

        if match.silo == SportSilo.TENNIS and self._settings.ENABLE_TENNIS_POSTERS:
            return await self._get_tennis().render_match_poster(match, db=db)

        tsdb = self._get_tsdb()
        poster_url = match.poster.strip()
        if "nostream" in poster_url.lower():
            poster_url = ""
        if not poster_url and self._settings.ENABLE_THESPORTSDB:
            poster_url = (await tsdb.resolve_poster_url(match, db=db)) or ""
        if "nostream" in poster_url.lower():
            poster_url = ""

        if poster_url:
            raw = await tsdb.fetch_poster_bytes(poster_url)
            if raw:
                await self._atomic_write(base_path, raw)
                return raw

        fallback_url = match.fallback_poster.strip()
        if fallback_url and "nostream" not in fallback_url.lower():
            raw_fallback = await tsdb.fetch_poster_bytes(fallback_url)
            if raw_fallback:
                await self._atomic_write(base_path, raw_fallback)
                return raw_fallback

        comp = match.canonical_competition or match.competition
        return await tsdb.render_dark_canvas_async(
            match.title,
            comp,
            width=BANNER_WIDTH,
            height=BANNER_HEIGHT,
        )

    async def get_banner(
        self,
        match: Match,
        db: Optional[DatabaseManager] = None,
        *,
        now_ms: Optional[int] = None,
    ) -> bytes:
        """Render or return cached 889x500 banner JPEG protected by single-flight lock."""
        badge_state, badge_label = compute_badge_state(
            match,
            now_ms=now_ms,
            timezone_name=self._settings.TIMEZONE,
        )
        safe_id = safe_match_filename(match.id)
        poster_dir = self._settings.POSTER_CACHE_DIR
        cache_path = self._banner_cache_path(
            poster_dir,
            safe_id,
            badge_state,
            badge_label,
            match.has_tvvoo,
        )

        if self._is_fresh(cache_path):
            return await asyncio.to_thread(cache_path.read_bytes)

        lock_key = (match.id, badge_state, match.has_tvvoo)
        lock = self._get_lock(lock_key)
        async with lock:
            if self._is_fresh(cache_path):
                return await asyncio.to_thread(cache_path.read_bytes)

            base_bytes = await self._resolve_base_poster(match, db=db)
            comp = match.canonical_competition or match.competition
            loop = asyncio.get_running_loop()
            banner_bytes = await loop.run_in_executor(
                _PIL_EXECUTOR,
                _compose_banner_sync,
                base_bytes,
                badge_state,
                badge_label,
                match.has_tvvoo,
                match.title,
                comp,
            )

            # Evict stale state variants for this match before writing the fresh banner.
            def _cleanup_old_variants() -> None:
                if poster_dir.is_dir():
                    for old in poster_dir.glob(f"{safe_id}__*.jpg"):
                        if old != cache_path:
                            try:
                                old.unlink()
                            except OSError:
                                pass

            await asyncio.to_thread(_cleanup_old_variants)
            await self._atomic_write(cache_path, banner_bytes)
            return banner_bytes

    def cleanup_poster_files(self, match_ids: list[str]) -> int:
        """Remove cached base and banner JPEGs for purged match IDs."""
        return cleanup_poster_files(
            match_ids,
            base_dir=self._settings.BASE_POSTER_CACHE_DIR,
            poster_dir=self._settings.POSTER_CACHE_DIR,
        )
