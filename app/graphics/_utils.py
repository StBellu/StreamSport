"""Shared Pillow font, text sanitization, and cache filename helpers."""

from __future__ import annotations

import hashlib
import io
import re
from datetime import datetime, timedelta, timezone, tzinfo
from functools import lru_cache
from typing import Optional, Union
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from PIL import Image, ImageDraw, ImageFont

FontType = Union[ImageFont.FreeTypeFont, ImageFont.ImageFont]

_BOLD_FONT_CANDIDATES: tuple[str, ...] = (
    "DejaVuSans-Bold.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
    "C:/Windows/Fonts/arialbd.ttf",
    "C:/Windows/Fonts/segoeuib.ttf",
    "Arial Bold.ttf",
)

_REGULAR_FONT_CANDIDATES: tuple[str, ...] = (
    "DejaVuSans.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    "C:/Windows/Fonts/arial.ttf",
    "C:/Windows/Fonts/segoeui.ttf",
    "Arial.ttf",
)

_SAFE_ID_RE = re.compile(r"[^A-Za-z0-9_.-]+")


def strip_non_bmp(text: str) -> str:
    """Strip 4-byte non-BMP characters (e.g. emojis `ord(ch) > 0xFFFF`) to avoid tofu glyphs."""
    if not text:
        return ""
    cleaned = "".join(ch for ch in text if ord(ch) <= 0xFFFF)
    return " ".join(cleaned.split())


@lru_cache(maxsize=64)
def load_font(size: int, bold: bool = True) -> FontType:
    """Load `DejaVuSans` (or system/Pillow fallback) at the requested pixel size."""
    candidates = _BOLD_FONT_CANDIDATES if bold else _REGULAR_FONT_CANDIDATES
    for candidate in candidates:
        try:
            return ImageFont.truetype(candidate, size=size)
        except OSError:
            continue
    try:
        return ImageFont.load_default(size=size)
    except TypeError:
        return ImageFont.load_default()


def measure_text(draw: ImageDraw.ImageDraw, text: str, font: FontType) -> tuple[int, int]:
    """Return `(width, height)` bounding dimensions for `text` rendered with `font`."""
    if not text:
        return 0, 0
    left, top, right, bottom = draw.textbbox((0, 0), text, font=font)
    return max(0, right - left), max(0, bottom - top)


def wrap_text_lines(
    draw: ImageDraw.ImageDraw,
    text: str,
    font: FontType,
    max_width: int,
    max_lines: int = 2,
) -> list[str]:
    """Word-wrap `text` into at most `max_lines` fitting within `max_width`."""
    cleaned = strip_non_bmp(text)
    if not cleaned:
        return []

    words = cleaned.split(" ")
    lines: list[str] = []
    current = ""

    for word in words:
        candidate = f"{current} {word}".strip() if current else word
        w, _ = measure_text(draw, candidate, font)
        if w <= max_width or not current:
            current = candidate
            continue
        lines.append(current)
        current = word
        if len(lines) >= max_lines:
            break

    if len(lines) < max_lines and current:
        lines.append(current)

    # Truncate final line with ellipsis if words remain or line still exceeds max_width.
    if lines:
        last = lines[-1]
        consumed_words = sum(len(line.split(" ")) for line in lines)
        if consumed_words < len(words) and not last.endswith("..."):
            last = f"{last}..."
        while last and measure_text(draw, last, font)[0] > max_width:
            if len(last) <= 4:
                break
            last = last[:-4].rstrip() + "..."
        lines[-1] = last

    return lines[:max_lines]


def safe_match_filename(match_id: str) -> str:
    """Convert an arbitrary `match.id` into a filesystem-safe stem."""
    sanitized = _SAFE_ID_RE.sub("_", match_id).strip("._")
    if not sanitized:
        return hashlib.sha1(match_id.encode("utf-8")).hexdigest()[:16]
    if len(sanitized) > 100:
        digest = hashlib.sha1(match_id.encode("utf-8")).hexdigest()[:10]
        return f"{sanitized[:88]}_{digest}"
    return sanitized


def image_to_jpeg_bytes(image: Image.Image, quality: int = 88) -> bytes:
    """Encode a Pillow `Image` as RGB JPEG bytes."""
    if image.mode != "RGB":
        rgb = Image.new("RGB", image.size, (12, 15, 20))
        if image.mode == "RGBA":
            rgb.paste(image, mask=image.split()[3])
        else:
            rgb.paste(image.convert("RGB"))
        image = rgb
    buf = io.BytesIO()
    image.save(buf, format="JPEG", quality=quality, optimize=True)
    return buf.getvalue()


class _EuropeRomeFallbackTz(tzinfo):
    """Exact EU CET/CEST fallback when `tzdata` is absent (e.g. Windows without tzdata)."""

    @staticmethod
    def _last_sunday(year: int, month: int) -> datetime:
        if month == 12:
            dt = datetime(year, 12, 31, 1, 0)
        else:
            dt = datetime(year, month + 1, 1, 1, 0) - timedelta(days=1)
        # weekday(): Monday=0 .. Sunday=6
        offset = (dt.weekday() + 1) % 7
        return dt - timedelta(days=offset)

    def _is_dst(self, dt: Optional[datetime]) -> bool:
        if dt is None:
            return False
        utc_dt = dt.replace(tzinfo=None)
        start = self._last_sunday(utc_dt.year, 3)
        end = self._last_sunday(utc_dt.year, 10)
        return start <= utc_dt < end

    def utcoffset(self, dt: Optional[datetime]) -> timedelta:
        if dt is not None and dt.tzinfo is self:
            # Convert wall-time approximation to UTC check for DST boundaries.
            approx_utc = dt.replace(tzinfo=None) - timedelta(hours=1)
            return timedelta(hours=2 if self._is_dst(approx_utc) else 1)
        return timedelta(hours=1)

    def dst(self, dt: Optional[datetime]) -> timedelta:
        if dt is not None and dt.tzinfo is self:
            approx_utc = dt.replace(tzinfo=None) - timedelta(hours=1)
            return timedelta(hours=1 if self._is_dst(approx_utc) else 0)
        return timedelta(0)

    def tzname(self, dt: Optional[datetime]) -> str:
        return "CEST" if self.dst(dt) else "CET"


def get_timezone(tz_name: str) -> tzinfo:
    """Resolve `ZoneInfo(tz_name)` with graceful CET/CEST or UTC fallback if tzdata is missing."""
    try:
        return ZoneInfo(tz_name)
    except ZoneInfoNotFoundError:
        if tz_name in ("Europe/Rome", "CET", "Europe/Berlin", "Europe/Paris", "Europe/Madrid"):
            return _EuropeRomeFallbackTz()
        return timezone.utc

