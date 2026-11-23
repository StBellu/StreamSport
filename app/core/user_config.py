"""Per-user EasyProxy and stream preference encoding/decoding."""

import base64
import json
from typing import Any, Optional
from urllib.parse import parse_qs

from pydantic import BaseModel

from app.config import Settings, get_settings


class UserConfig(BaseModel):
    """User-level configuration embedded in Stremio addon manifest URLs."""

    ep_url: str = ""
    ep_pass: str = ""
    prefer_direct_tvvoo: bool = True
    show_easyproxy_tvvoo: bool = True


def _parse_bool(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        norm = value.strip().lower()
        if norm in ("1", "true", "yes", "on"):
            return True
        if norm in ("0", "false", "no", "off"):
            return False
    return default


def encode_config(cfg: UserConfig) -> str:
    """Encode a `UserConfig` into unpadded URL-safe Base64 compact JSON."""
    payload = cfg.model_dump()
    raw_json = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return base64.urlsafe_b64encode(raw_json).rstrip(b"=").decode("ascii")


def _apply_defaults(cfg: UserConfig, settings: Settings) -> UserConfig:
    ep_url = (cfg.ep_url or settings.DEFAULT_EP_URL).strip().rstrip("/")
    ep_pass = (cfg.ep_pass or settings.DEFAULT_EP_PASS).strip()
    return cfg.model_copy(update={"ep_url": ep_url, "ep_pass": ep_pass})


def decode_config(raw: Optional[str], settings: Optional[Settings] = None) -> UserConfig:
    """Decode Base64URL JSON or query-string user config with `.env` fallbacks."""
    resolved_settings = settings or get_settings()
    if not raw or not raw.strip():
        return _apply_defaults(UserConfig(), resolved_settings)

    token = raw.strip()

    # Query-string format: epUrl=...&epPass=...
    is_query_style = (
        "=" in token
        and not token.startswith("{")
        and not token.endswith("=")
    ) or any(k in token for k in ("epUrl=", "epPass=", "ep_url=", "ep_pass="))
    if is_query_style:
        qs = parse_qs(token.lstrip("?"), keep_blank_values=True)
        ep_url = (qs.get("epUrl") or qs.get("ep_url") or [""])[0]
        ep_pass = (qs.get("epPass") or qs.get("ep_pass") or [""])[0]
        prefer_direct = _parse_bool(
            (qs.get("prefer_direct_tvvoo") or qs.get("preferDirectTvvoo") or [None])[0],
            True,
        )
        show_ep_tvvoo = _parse_bool(
            (qs.get("show_easyproxy_tvvoo") or qs.get("showEasyproxyTvvoo") or [None])[0],
            True,
        )
        return _apply_defaults(
            UserConfig(
                ep_url=ep_url,
                ep_pass=ep_pass,
                prefer_direct_tvvoo=prefer_direct,
                show_easyproxy_tvvoo=show_ep_tvvoo,
            ),
            resolved_settings,
        )

    # URL-safe Base64 JSON or legacy pipe-delimited `epUrl|epPass|client`
    try:
        padded = token + "=" * (-len(token) % 4)
        decoded_text = base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8")
        if "|" in decoded_text and not decoded_text.lstrip().startswith("{"):
            parts = [p.strip() for p in decoded_text.split("|")]
            return _apply_defaults(
                UserConfig(
                    ep_url=parts[0] if parts else "",
                    ep_pass=parts[1] if len(parts) > 1 else "",
                ),
                resolved_settings,
            )
        data = json.loads(decoded_text)
        if isinstance(data, dict):
            normalized = {
                "ep_url": str(data.get("ep_url") or data.get("epUrl") or ""),
                "ep_pass": str(data.get("ep_pass") or data.get("epPass") or ""),
                "prefer_direct_tvvoo": _parse_bool(
                    data.get("prefer_direct_tvvoo", data.get("preferDirectTvvoo")),
                    True,
                ),
                "show_easyproxy_tvvoo": _parse_bool(
                    data.get("show_easyproxy_tvvoo", data.get("showEasyproxyTvvoo")),
                    True,
                ),
            }
            return _apply_defaults(UserConfig.model_validate(normalized), resolved_settings)
    except Exception:
        pass

    return _apply_defaults(UserConfig(), resolved_settings)
