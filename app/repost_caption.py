"""Custom repost caption, separate from original-post transport policy.

When empty or disabled the original message is forwarded with its own caption.
Only the active override is passed into the media delivery pipeline.
"""
from __future__ import annotations

from typing import Any

from .delivery_content_helpers import normalize_caption_entities


def get_repost_caption_override(rule: Any) -> dict[str, Any] | None:
    if (getattr(rule, "mode", "repost") or "repost") != "repost":
        return None
    if not bool(getattr(rule, "repost_caption_enabled", False)):
        return None
    caption = str(getattr(rule, "repost_caption", None) or "")
    if not caption.strip():
        return None
    mode = str(getattr(rule, "repost_caption_delivery_mode", "copy_first") or "").strip().lower()
    if mode not in ("copy_first", "builder_first"):
        mode = "copy_first"
    entities = (
        normalize_caption_entities(
            getattr(rule, "repost_caption_entities_json", None)
        )
        if mode == "builder_first"
        else []
    )
    return {"text": caption, "entities": entities, "mode": mode}


def caption_override_content(override: dict[str, Any] | None) -> dict[str, Any] | None:
    """Produce the same entity payload used by existing Telethon builders."""
    if override is None:
        return None
    return {"text": override["text"], "entities": override["entities"]}
