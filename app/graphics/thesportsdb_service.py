"""TheSportsDB authenticated calendar scraping, 4-gatekeeper matching, and dark canvas fallback."""

from __future__ import annotations

import asyncio
import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import Any, Optional
from urllib.parse import quote_plus, urljoin

import httpx
from bs4 import BeautifulSoup
from PIL import Image, ImageDraw
from rapidfuzz import fuzz

from app.config import Settings, get_settings
from app.core.database import DatabaseManager
from app.core.network import create_standard_client
from app.graphics._utils import (
    get_timezone,
    image_to_jpeg_bytes,
    load_font,
    measure_text,
    strip_non_bmp,
    wrap_text_lines,
)
from app.models.domain import Match, SportSilo
from app.taxonomy import DEFAULT_TEAM_MATCHER, detect_demographics

logger = logging.getLogger(__name__)

_TSDB_BASE_URL = "https://www.thesportsdb.com"
_TSDB_LOGIN_URL = f"{_TSDB_BASE_URL}/user_login.php"
_TSDB_CALENDAR_URL = f"{_TSDB_BASE_URL}/browse_calendar.php"
_TSDB_SEARCH_URL = f"{_TSDB_BASE_URL}/browse"

_RECONCILE_TIME_WINDOW_MS = 90 * 60 * 1000  # 90 minutes
_GP_CASCADE_WINDOW_MS = 72 * 3600 * 1000  # +-72 hours
_SEARCH_THROTTLE_SECONDS = 1.2
_NOT_FOUND_TTL_MS = 24 * 3600 * 1000

_PIL_EXECUTOR = ThreadPoolExecutor(max_workers=4, thread_name_prefix="tsdb-pil")

_LEAGUE_STOPWORDS = frozenset({
    "the", "of", "and", "de", "la", "le", "el", "il", "cup", "league", "liga",
    "serie", "division", "championship", "tournament", "pro", "men", "women",
})

_GP_SESSION_NOISE_RE = re.compile(
    r"\b(?:fp[123]|free\s*practice\s*[123]?|practice\s*[123]?|prove\s*libere\s*[123]?|"
    r"qualifying|qualifiche|quali|sprint(?:\s*(?:race|shootout|qualifying))?|"
    r"warm\s*up|warmup|race|gara|main\s*race|grand\s*prix|gp|formula\s*1|f1|motogp|moto2|moto3)\b",
    re.IGNORECASE,
)

_NON_RACE_SESSION_RE = re.compile(
    r"\b(?:fp[123]|free\s*practice|practice|prove\s*libere|qualifying|qualifiche|"
    r"quali|shootout|warm\s*up|warmup|sprint)\b",
    re.IGNORECASE,
)

_RACE_SESSION_RE = re.compile(
    r"\b(?:grand\s*prix|race|gara|feature\s*race)\b",
    re.IGNORECASE,
)

_SPLIT_TEAMS_RE = re.compile(r"\s+(?:vs\.?|v\.?|@)\s+", re.IGNORECASE)
_TIME_HM_RE = re.compile(r"\b(\d{1,2}):(\d{2})\b")

_SILO_SPORT_KEYWORDS: dict[SportSilo, tuple[str, ...]] = {
    SportSilo.CALCIO_ITA: ("soccer", "football", "calcio"),
    SportSilo.FOOTBALL: ("soccer", "football", "calcio"),
    SportSilo.TENNIS: ("tennis",),
    SportSilo.BASKET: ("basketball", "basket", "nba", "euroleague"),
    SportSilo.MOTORI: ("motorsport", "racing", "formula", "motogp", "f1", "indycar", "nascar"),
    SportSilo.COMBAT: ("fighting", "mma", "ufc", "boxing", "boxe", "wrestling"),
    SportSilo.VOLLEY: ("volleyball", "volley"),
    SportSilo.NFL: ("american football", "nfl", "ncaa football"),
    SportSilo.HOCKEY: ("ice hockey", "hockey", "nhl"),
    SportSilo.BASEBALL: ("baseball", "mlb"),
    SportSilo.RUGBY: ("rugby",),
    SportSilo.HANDBALL: ("handball",),
    SportSilo.ALTRI_SPORT: (),
}


def _map_sport_to_silo(sport_text: str) -> str:
    low = sport_text.strip().lower()
    if not low:
        return SportSilo.ALTRI_SPORT.value
    # Check specific disciplines before generic "football".
    ordered_silos = (
        SportSilo.NFL,
        SportSilo.TENNIS,
        SportSilo.BASKET,
        SportSilo.MOTORI,
        SportSilo.COMBAT,
        SportSilo.VOLLEY,
        SportSilo.HOCKEY,
        SportSilo.BASEBALL,
        SportSilo.RUGBY,
        SportSilo.HANDBALL,
        SportSilo.FOOTBALL,
    )
    for silo in ordered_silos:
        for kw in _SILO_SPORT_KEYWORDS[silo]:
            if kw in low:
                return silo.value
    return SportSilo.ALTRI_SPORT.value


def _looks_like_poster_url(url: str) -> bool:
    if not url:
        return False
    low = url.lower()
    if any(skip in low for skip in ("/icons/", "/flags/", "tiny", "spacer", "blank", ".svg")):
        return False
    return low.startswith("http://") or low.startswith("https://") or low.startswith("/")


def _normalize_tsdb_url(raw_url: str) -> str:
    if not raw_url:
        return ""
    if raw_url.startswith("//"):
        return f"https:{raw_url}"
    if raw_url.startswith("/"):
        return urljoin(_TSDB_BASE_URL, raw_url)
    return raw_url


def _split_event_teams(title: str) -> tuple[str, str]:
    cleaned = strip_non_bmp(title)
    parts = _SPLIT_TEAMS_RE.split(cleaned, maxsplit=1)
    if len(parts) == 2:
        return parts[0].strip(), parts[1].strip()
    return cleaned, ""


def _league_tokens(name: str) -> set[str]:
    cleaned = re.sub(r"[^a-z0-9\s]", " ", (name or "").lower())
    return {
        tok for tok in cleaned.split()
        if len(tok) >= 2 and tok not in _LEAGUE_STOPWORDS
    }


def _leagues_compatible(match: Match, entry_league: str) -> bool:
    """Gatekeeper 4: verify canonical/token compatibility between leagues."""
    if not entry_league:
        return True
    match_leagues = [
        x for x in (match.canonical_competition, match.competition, match.alt_league) if x
    ]
    if not match_leagues:
        return True

    entry_tokens = _league_tokens(entry_league)
    for ml in match_leagues:
        ml_tokens = _league_tokens(ml)
        if not entry_tokens or not ml_tokens:
            return True
        if entry_tokens & ml_tokens:
            return True
        if fuzz.token_set_ratio(ml.lower(), entry_league.lower()) >= 70:
            return True
    return False


def _gp_identity_tokens(text: str) -> set[str]:
    stripped = _GP_SESSION_NOISE_RE.sub(" ", (text or "").lower())
    cleaned = re.sub(r"[^a-z0-9\s]", " ", stripped)
    return {tok for tok in cleaned.split() if len(tok) >= 3}


def render_dark_canvas(
    title: str,
    competition: str,
    width: int = 889,
    height: int = 500,
) -> bytes:
    """Render a clean RGB (12, 15, 20) dark slate fallback poster as JPEG bytes."""
    clean_title = strip_non_bmp(title) or "Sport Event"
    clean_comp = strip_non_bmp(competition)

    img = Image.new("RGB", (width, height), (12, 15, 20))
    draw = ImageDraw.Draw(img)

    # Subtle outer frame and top/center accent lines.
    pad = max(12, width // 50)
    draw.rectangle(
        (pad, pad, width - pad - 1, height - pad - 1),
        outline=(30, 38, 52),
        width=2,
    )
    accent_w = min(140, width // 5)
    accent_x0 = (width - accent_w) // 2
    draw.rectangle(
        (accent_x0, pad + 26, accent_x0 + accent_w, pad + 30),
        fill=(56, 189, 248),
    )

    title_size = max(22, int(height * 0.088))
    title_font = load_font(title_size, bold=True)
    max_text_width = int(width * 0.82)

    lines = wrap_text_lines(draw, clean_title, title_font, max_text_width, max_lines=3)
    if len(lines) >= 3:
        title_size = max(20, int(height * 0.072))
        title_font = load_font(title_size, bold=True)
        lines = wrap_text_lines(draw, clean_title, title_font, max_text_width, max_lines=3)

    sub_size = max(16, int(height * 0.048))
    sub_font = load_font(sub_size, bold=False)
    sub_lines = (
        wrap_text_lines(draw, clean_comp.upper(), sub_font, max_text_width, max_lines=1)
        if clean_comp
        else []
    )

    line_gap = max(8, int(title_size * 0.28))
    title_heights = [measure_text(draw, line, title_font)[1] for line in lines]
    total_title_h = sum(title_heights) + max(0, len(lines) - 1) * line_gap
    sub_h = measure_text(draw, sub_lines[0], sub_font)[1] if sub_lines else 0
    sub_gap = 22 if sub_lines else 0

    total_block_h = total_title_h + sub_gap + sub_h
    y_cursor = (height - total_block_h) // 2

    for line, lh in zip(lines, title_heights):
        lw, _ = measure_text(draw, line, title_font)
        draw.text(((width - lw) // 2, y_cursor), line, font=title_font, fill=(244, 247, 252))
        y_cursor += lh + line_gap

    if sub_lines:
        y_cursor += sub_gap - line_gap
        sw, _ = measure_text(draw, sub_lines[0], sub_font)
        draw.text(
            ((width - sw) // 2, y_cursor),
            sub_lines[0],
            font=sub_font,
            fill=(148, 163, 184),
        )

    return image_to_jpeg_bytes(img, quality=88)


class TheSportsDBService:
    """Level 1 authenticated calendar scraper + 4-gatekeeper reconciliation + Level 2 search."""

    def __init__(
        self,
        settings: Optional[Settings] = None,
        client: Optional[httpx.AsyncClient] = None,
    ) -> None:
        self._settings = settings or get_settings()
        self._client = client
        self._owns_client = client is None
        self._login_lock = asyncio.Lock()
        self._search_lock = asyncio.Lock()
        self._last_search_monotonic = 0.0
        self._authenticated = False
        self._last_sync_monotonic = 0.0
        self.calendar_by_silo: dict[str, list[dict[str, Any]]] = {}

    def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = create_standard_client(timeout=15.0, follow_redirects=True)
            self._owns_client = True
        return self._client

    async def aclose(self) -> None:
        """Close the owned HTTP client if created internally."""
        if self._owns_client and self._client is not None and not self._client.is_closed:
            await self._client.aclose()
            self._client = None

    async def render_dark_canvas_async(
        self,
        title: str,
        competition: str,
        width: int = 889,
        height: int = 500,
    ) -> bytes:
        """Run `render_dark_canvas` inside the dedicated Pillow ThreadPoolExecutor."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            _PIL_EXECUTOR,
            render_dark_canvas,
            title,
            competition,
            width,
            height,
        )

    async def login(self, force: bool = False) -> bool:
        """Authenticate against `user_login.php` extracting CSRF/hidden form fields."""
        if not self._settings.ENABLE_THESPORTSDB:
            return False
        user = self._settings.THESPORTSDB_USER.strip()
        password = self._settings.THESPORTSDB_PASS
        if not user or not password:
            logger.debug("TheSportsDB credentials not configured; skipping Level 1 login.")
            return False

        async with self._login_lock:
            if self._authenticated and not force:
                return True

            client = self._get_client()
            try:
                login_page = await client.get(_TSDB_LOGIN_URL)
                login_page.raise_for_status()
            except Exception as exc:
                logger.warning("Failed to fetch TheSportsDB login form: %s", exc)
                self._authenticated = False
                return False

            soup = BeautifulSoup(login_page.text, "html.parser")
            form = None
            for candidate in soup.find_all("form"):
                if candidate.find("input", {"type": "password"}):
                    form = candidate
                    break
            if form is None:
                form = soup.find("form")

            payload: dict[str, str] = {}
            user_field = "username"
            pass_field = "password"
            action_url = _TSDB_LOGIN_URL

            if form is not None:
                raw_action = str(form.get("action") or "").strip()
                if raw_action:
                    action_url = urljoin(_TSDB_LOGIN_URL, raw_action)
                for inp in form.find_all("input"):
                    name = str(inp.get("name") or "").strip()
                    if not name:
                        continue
                    inp_type = str(inp.get("type") or "text").lower()
                    value = str(inp.get("value") or "")
                    if inp_type == "password":
                        pass_field = name
                    elif inp_type in ("text", "email") and name.lower() in (
                        "username", "user", "login", "email",
                    ):
                        user_field = name
                    else:
                        payload[name] = value

            payload[user_field] = user
            payload[pass_field] = password

            try:
                resp = await client.post(
                    action_url,
                    data=payload,
                    headers={"Referer": _TSDB_LOGIN_URL, "Origin": _TSDB_BASE_URL},
                )
            except Exception as exc:
                logger.warning("TheSportsDB login POST failed: %s", exc)
                self._authenticated = False
                return False

            final_url = str(resp.url).lower()
            still_login_form = (
                "user_login.php" in final_url
                or bool(BeautifulSoup(resp.text, "html.parser").find("input", {"type": "password"}))
            )
            self._authenticated = resp.status_code < 400 and not still_login_form
            if not self._authenticated:
                logger.warning("TheSportsDB authentication rejected or redirected to login.")
            return self._authenticated

    async def _get_authenticated(
        self,
        url: str,
        params: Optional[dict[str, str]] = None,
    ) -> Optional[httpx.Response]:
        """Perform an authenticated GET, automatically re-logging in on expired session."""
        if not await self.login():
            return None

        client = self._get_client()
        for attempt in range(2):
            try:
                resp = await client.get(url, params=params)
            except Exception as exc:
                logger.warning("TheSportsDB request error for %s: %s", url, exc)
                return None

            needs_relogin = resp.status_code in (401, 403) or "user_login.php" in str(resp.url).lower()
            if not needs_relogin and resp.status_code == 200 and "password" in resp.text[:4000].lower():
                if 'name="password"' in resp.text or "user_login.php" in resp.text:
                    needs_relogin = True

            if needs_relogin and attempt == 0:
                self._authenticated = False
                if not await self.login(force=True):
                    return None
                continue

            if resp.status_code >= 400:
                return None
            return resp
        return None

    def _parse_calendar_html(self, html: str, day_str: str) -> list[dict[str, Any]]:
        """Parse TheSportsDB `browse_calendar.php` rows into normalized index dicts."""
        soup = BeautifulSoup(html, "html.parser")
        entries: list[dict[str, Any]] = []
        base_date = datetime.strptime(day_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)

        for row in soup.find_all("tr"):
            links = row.find_all("a", href=True)
            event_link = None
            for link in links:
                href = str(link.get("href") or "")
                if "/event/" in href:
                    event_link = link
                    break
            if event_link is None:
                continue

            title = strip_non_bmp(event_link.get_text(" ", strip=True))
            if not title:
                continue

            row_text = row.get_text(" ", strip=True)
            time_match = _TIME_HM_RE.search(row_text)
            start_ms = 0
            if time_match:
                hh, mm = int(time_match.group(1)), int(time_match.group(2))
                if 0 <= hh <= 23 and 0 <= mm <= 59:
                    dt = base_date.replace(hour=hh, minute=mm, second=0, microsecond=0)
                    start_ms = int(dt.timestamp() * 1000)

            sport_hint_parts: list[str] = []
            for img in row.find_all("img"):
                alt = str(img.get("alt") or "")
                src = str(img.get("src") or "")
                if alt:
                    sport_hint_parts.append(alt)
                if "sport" in src.lower():
                    sport_hint_parts.append(src)
            for svg in row.find_all("svg"):
                for attr in ("aria-label", "data-icon", "title", "class"):
                    val = svg.get(attr)
                    if val:
                        sport_hint_parts.append(" ".join(val) if isinstance(val, list) else str(val))
            for link in links:
                href = str(link.get("href") or "")
                if "/sport/" in href or "/sports/" in href:
                    sport_hint_parts.append(href)
                    sport_hint_parts.append(link.get_text(" ", strip=True))

            league_name = ""
            for link in links:
                href = str(link.get("href") or "")
                if "/league/" in href:
                    league_name = strip_non_bmp(link.get_text(" ", strip=True))
                    if league_name:
                        break
            if not league_name:
                cells = row.find_all("td")
                if len(cells) >= 3:
                    league_name = strip_non_bmp(cells[2].get_text(" ", strip=True))

            poster_url = ""
            for img in row.find_all("img"):
                src = _normalize_tsdb_url(str(img.get("data-src") or img.get("src") or ""))
                if _looks_like_poster_url(src) and (
                    "thesportsdb.com/images/media" in src.lower()
                    or "r2.thesportsdb.com" in src.lower()
                    or "/event/" in src.lower()
                ):
                    poster_url = src
                    break

            silo = _map_sport_to_silo(" ".join(sport_hint_parts) + " " + league_name)
            home, away = _split_event_teams(title)
            is_women, is_youth = detect_demographics(league_name)
            event_url = _normalize_tsdb_url(str(event_link.get("href") or ""))

            entries.append({
                "title": title,
                "home": home,
                "away": away,
                "league": league_name,
                "silo": silo,
                "date_ms": start_ms,
                "day": day_str,
                "poster_url": poster_url,
                "event_url": event_url,
                "is_women": is_women,
                "is_youth": is_youth,
            })

        return entries

    async def sync_calendar(self, force: bool = False) -> dict[str, list[dict[str, Any]]]:
        """Scrape yesterday, today, and tomorrow calendars every 12h and index by silo."""
        if not self._settings.ENABLE_THESPORTSDB:
            return {}

        now_mono = time.monotonic()
        interval = self._settings.THESPORTSDB_SYNC_INTERVAL_SECONDS
        if not force and self.calendar_by_silo and (now_mono - self._last_sync_monotonic) < interval:
            return self.calendar_by_silo

        tz = get_timezone(self._settings.TIMEZONE)
        today_local = datetime.now(tz).date()
        today_utc = datetime.now(timezone.utc).date()
        day_candidates = sorted({
            (today_utc + timedelta(days=delta)).strftime("%Y-%m-%d")
            for delta in (-1, 0, 1)
        } | {
            (today_local + timedelta(days=delta)).strftime("%Y-%m-%d")
            for delta in (-1, 0, 1)
        })

        indexed: dict[str, list[dict[str, Any]]] = {}
        for day_str in day_candidates:
            resp = await self._get_authenticated(_TSDB_CALENDAR_URL, params={"d": day_str})
            if resp is None:
                continue
            for entry in self._parse_calendar_html(resp.text, day_str):
                silo_key = entry["silo"]
                indexed.setdefault(silo_key, []).append(entry)

        if indexed:
            self.calendar_by_silo = indexed
            self._last_sync_monotonic = now_mono
            logger.info(
                "TheSportsDB calendar synced: %d silos, %d entries",
                len(indexed),
                sum(len(v) for v in indexed.values()),
            )
        return self.calendar_by_silo

    def _silo_candidates(self, silo: SportSilo) -> list[dict[str, Any]]:
        if silo in (SportSilo.CALCIO_ITA, SportSilo.FOOTBALL):
            return (
                self.calendar_by_silo.get(SportSilo.FOOTBALL.value, [])
                + self.calendar_by_silo.get(SportSilo.CALCIO_ITA.value, [])
            )
        return self.calendar_by_silo.get(silo.value, [])

    def _passes_four_gatekeepers(self, match: Match, entry: dict[str, Any]) -> float:
        """Return match score >= 0.75 if all 4 gatekeepers pass, else 0.0."""
        # Gatekeeper 1: Time delta <= 90 minutes.
        entry_ms = int(entry.get("date_ms") or 0)
        if match.date > 0 and entry_ms > 0:
            if abs(match.date - entry_ms) > _RECONCILE_TIME_WINDOW_MS:
                return 0.0

        # Gatekeeper 3: Strict demographic congruence.
        if bool(entry.get("is_women")) != match.is_women or bool(entry.get("is_youth")) != match.is_youth:
            return 0.0

        # Gatekeeper 4: League compatibility.
        if not _leagues_compatible(match, str(entry.get("league") or "")):
            return 0.0

        # Gatekeeper 2: Bilateral fuzzy team match >= 0.75 via DEFAULT_TEAM_MATCHER.
        home_1 = match.home_team.name if match.home_team else match.title
        away_1 = match.away_team.name if match.away_team else ""
        res = DEFAULT_TEAM_MATCHER.match_score(
            home_1,
            away_1,
            str(entry.get("home") or entry.get("title") or ""),
            str(entry.get("away") or ""),
            alt_home_1=match.alt_home or (match.home_team.alt_name if match.home_team else ""),
            alt_away_1=match.alt_away or (match.away_team.alt_name if match.away_team else ""),
            date_1_ms=0,
            date_2_ms=0,
            is_women_1=match.is_women,
            is_youth_1=match.is_youth,
            is_women_2=bool(entry.get("is_women")),
            is_youth_2=bool(entry.get("is_youth")),
        )
        if res.matched and res.score >= 0.75:
            return res.score
        return 0.0

    def _motorsport_gp_cascade(self, match: Match) -> Optional[str]:
        """Weekend GP Cascade Fallback: inherit main GP Race 16:9 poster within +-72h."""
        if match.silo != SportSilo.MOTORI:
            return None

        match_tokens = _gp_identity_tokens(f"{match.title} {match.competition} {match.stage}")
        if not match_tokens:
            return None

        best_url: Optional[str] = None
        best_rank = (-1, 0)  # (is_explicit_race, shared_token_count)

        for entry in self._silo_candidates(SportSilo.MOTORI):
            poster_url = str(entry.get("poster_url") or "")
            if not poster_url:
                continue
            entry_ms = int(entry.get("date_ms") or 0)
            if match.date > 0 and entry_ms > 0:
                if abs(match.date - entry_ms) > _GP_CASCADE_WINDOW_MS:
                    continue

            entry_text = f"{entry.get('title', '')} {entry.get('league', '')}"
            entry_tokens = _gp_identity_tokens(entry_text)
            shared = len(match_tokens & entry_tokens)
            if shared == 0:
                continue

            is_non_race = bool(_NON_RACE_SESSION_RE.search(entry_text))
            is_race = bool(_RACE_SESSION_RE.search(entry_text)) and not is_non_race
            rank = (1 if is_race else 0, shared)
            if rank > best_rank:
                best_rank = rank
                best_url = poster_url

        return best_url

    def reconcile_match(self, match: Match) -> Optional[str]:
        """In-RAM reconciliation against `calendar_by_silo` using the 4 gatekeepers."""
        if not self._settings.ENABLE_THESPORTSDB:
            return None

        best_score = 0.0
        best_entry: Optional[dict[str, Any]] = None

        for entry in self._silo_candidates(match.silo):
            poster_url = str(entry.get("poster_url") or "")
            if not poster_url:
                continue
            score = self._passes_four_gatekeepers(match, entry)
            if score > best_score:
                best_score = score
                best_entry = entry

        if best_entry is not None:
            return str(best_entry["poster_url"])

        if match.silo == SportSilo.MOTORI:
            return self._motorsport_gp_cascade(match)
        return None

    async def _extract_poster_from_event_page(self, event_url: str) -> Optional[str]:
        """Fetch an event page to extract its 16:9 `og:image` or event thumb if missing in list."""
        client = self._get_client()
        try:
            resp = await client.get(event_url)
            if resp.status_code >= 400:
                return None
        except Exception:
            return None

        soup = BeautifulSoup(resp.text, "html.parser")
        og_img = soup.find("meta", attrs={"property": "og:image"})
        if og_img and og_img.get("content"):
            url = _normalize_tsdb_url(str(og_img["content"]))
            if _looks_like_poster_url(url):
                return url
        for img in soup.find_all("img"):
            src = _normalize_tsdb_url(str(img.get("src") or ""))
            if _looks_like_poster_url(src) and "/images/media/event/" in src.lower():
                return src
        return None

    def _validate_svg_sport_icon(self, container: Any, silo: SportSilo) -> bool:
        """Validate SVG/icon sport markers on search result cards against `match.silo`."""
        if silo == SportSilo.ALTRI_SPORT:
            return True
        keywords = _SILO_SPORT_KEYWORDS.get(silo, ())
        if not keywords:
            return True

        hints: list[str] = []
        for svg in container.find_all("svg"):
            for attr in ("aria-label", "data-icon", "title", "class"):
                val = svg.get(attr)
                if val:
                    hints.append(" ".join(val) if isinstance(val, list) else str(val))
            title_el = svg.find("title")
            if title_el:
                hints.append(title_el.get_text(" ", strip=True))
        for img in container.find_all("img"):
            alt = str(img.get("alt") or "")
            src = str(img.get("src") or "")
            if alt:
                hints.append(alt)
            if "/sport" in src.lower() or "icon" in src.lower():
                hints.append(src)

        if not hints:
            return False
        joined = " ".join(hints).lower()
        return any(kw in joined for kw in keywords)

    async def semantic_orphan_search(
        self,
        match: Match,
        db: Optional[DatabaseManager] = None,
    ) -> Optional[str]:
        """Level 2 semantic search on `/browse?s={home}+{away}` with 1.2s throttling & SQLite cache."""
        if not self._settings.ENABLE_THESPORTSDB:
            return None

        home = (match.home_team.name if match.home_team else "").strip()
        away = (match.away_team.name if match.away_team else "").strip()
        query_text = f"{home} {away}".strip() if (home or away) else match.title.strip()
        query_text = strip_non_bmp(query_text)
        if not query_text:
            return None

        cache_key = f"{match.silo.value}:{query_text.lower()}"
        if db is not None:
            cached = await db.get_tsdb_cache(cache_key)
            if cached is not None:
                if cached["status"] == "found" and cached.get("thumb_url"):
                    return str(cached["thumb_url"])
                if cached["status"] == "not_found":
                    age_ms = int(time.time() * 1000) - int(cached.get("checked_at") or 0)
                    if age_ms < _NOT_FOUND_TTL_MS:
                        return None

        async with self._search_lock:
            elapsed = time.monotonic() - self._last_search_monotonic
            if elapsed < _SEARCH_THROTTLE_SECONDS:
                await asyncio.sleep(_SEARCH_THROTTLE_SECONDS - elapsed)

            client = self._get_client()
            search_url = f"{_TSDB_SEARCH_URL}?s={quote_plus(query_text)}"
            try:
                resp = await client.get(search_url)
            except Exception as exc:
                logger.warning("TheSportsDB search error for '%s': %s", query_text, exc)
                return None
            finally:
                self._last_search_monotonic = time.monotonic()

        if resp.status_code >= 400:
            return None

        soup = BeautifulSoup(resp.text, "html.parser")
        found_url: Optional[str] = None

        candidates = soup.find_all(["tr", "div", "li"])
        for node in candidates:
            event_link = node.find("a", href=re.compile(r"/event/"))
            if event_link is None:
                continue
            if not self._validate_svg_sport_icon(node, match.silo):
                continue

            card_title = strip_non_bmp(event_link.get_text(" ", strip=True))
            cand_home, cand_away = _split_event_teams(card_title)
            res = DEFAULT_TEAM_MATCHER.match_score(
                home or match.title,
                away,
                cand_home,
                cand_away,
                alt_home_1=match.alt_home,
                alt_away_1=match.alt_away,
                date_1_ms=0,
                date_2_ms=0,
                is_women_1=match.is_women,
                is_youth_1=match.is_youth,
            )
            if not res.matched or res.score < 0.75:
                continue

            for img in node.find_all("img"):
                src = _normalize_tsdb_url(str(img.get("data-src") or img.get("src") or ""))
                if _looks_like_poster_url(src) and "/images/media/" in src.lower():
                    found_url = src
                    break

            if not found_url:
                event_href = _normalize_tsdb_url(str(event_link.get("href") or ""))
                if event_href:
                    found_url = await self._extract_poster_from_event_page(event_href)
            if found_url:
                break

        if db is not None:
            await db.upsert_tsdb_cache(
                cache_key,
                found_url,
                "found" if found_url else "not_found",
            )
        return found_url

    async def resolve_poster_url(
        self,
        match: Match,
        db: Optional[DatabaseManager] = None,
    ) -> Optional[str]:
        """Resolve a 16:9 poster URL via Level 1 reconciliation (or GP cascade) then Level 2 search."""
        if not self._settings.ENABLE_THESPORTSDB:
            return None
        url = self.reconcile_match(match)
        if url:
            return url
        return await self.semantic_orphan_search(match, db=db)

    async def fetch_poster_bytes(self, url: str) -> Optional[bytes]:
        """Download raw image bytes from a poster URL."""
        if not url:
            return None
        client = self._get_client()
        try:
            resp = await client.get(url)
            if resp.status_code == 200 and resp.content:
                return resp.content
        except Exception as exc:
            logger.debug("Failed to download poster %s: %s", url, exc)
        return None
