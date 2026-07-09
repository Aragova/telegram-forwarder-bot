from __future__ import annotations

import ast
import json
from typing import Any


_VALID_CAPTION_DELIVERY_MODES = {"copy_first", "builder_first", "auto"}


def normalize_caption_entities(raw_entities: Any) -> list[dict[str, Any]]:
    if not raw_entities:
        return []

    parsed = raw_entities

    try:
        if isinstance(parsed, str):
            raw_text = parsed.strip()
            if not raw_text:
                return []

            try:
                parsed = json.loads(raw_text)
            except Exception:
                try:
                    parsed = ast.literal_eval(raw_text)
                except Exception:
                    return []

        if isinstance(parsed, str):
            parsed = parsed.strip()
            if not parsed:
                return []
            try:
                parsed = json.loads(parsed)
            except Exception:
                return []

        if isinstance(parsed, dict):
            parsed = [parsed]

        if not isinstance(parsed, list):
            return []

        normalized: list[dict[str, Any]] = []

        for item in parsed:
            if not isinstance(item, dict):
                continue

            entity_type = str(item.get("type") or "").strip().lower()
            offset = item.get("offset")
            length = item.get("length")

            try:
                offset = int(offset)
                length = int(length)
            except Exception:
                continue

            if not entity_type or offset < 0 or length <= 0:
                continue

            normalized_item: dict[str, Any] = {
                "type": entity_type,
                "offset": offset,
                "length": length,
            }

            if item.get("url"):
                normalized_item["url"] = str(item.get("url"))
            if item.get("language"):
                normalized_item["language"] = str(item.get("language"))
            if item.get("custom_emoji_id"):
                normalized_item["custom_emoji_id"] = str(item.get("custom_emoji_id"))

            normalized.append(normalized_item)

        return normalized
    except Exception:
        return []


def normalize_caption_delivery_mode(mode: Any) -> str:
    normalized_mode = str(mode or "auto").strip().lower()
    if normalized_mode not in _VALID_CAPTION_DELIVERY_MODES:
        return "auto"
    return normalized_mode


def is_custom_emoji_entity(entity: Any) -> bool:
    try:
        if isinstance(entity, dict):
            raw_type = entity.get("type") or entity.get("_") or ""
            custom_id = entity.get("custom_emoji_id") or entity.get("document_id")
        else:
            raw_type = getattr(entity, "type", "") or entity.__class__.__name__
            custom_id = (
                getattr(entity, "custom_emoji_id", None)
                or getattr(entity, "document_id", None)
            )

        normalized_type = str(raw_type or "").strip().lower().replace("_", "")
        if "customemoji" in normalized_type:
            return True

        if custom_id is not None:
            return True

    except Exception:
        return False

    return False


def _iter_content_entities(content: dict | None):
    content = content or {}

    for key in ("entities", "caption_entities", "text_entities", "raw_entities"):
        entities = content.get(key) or []

        if isinstance(entities, dict):
            entities = [entities]

        for entity in entities or []:
            yield entity


def content_requires_builder(content: dict | None) -> bool:
    for entity in _iter_content_entities(content):
        try:
            if is_custom_emoji_entity(entity):
                return True

            if isinstance(entity, dict):
                raw_type = entity.get("type") or entity.get("_") or ""
            elif isinstance(entity, (str, bytes, int, float, bool)):
                continue
            else:
                raw_type = getattr(entity, "type", "") or entity.__class__.__name__

            entity_type = str(raw_type or "").strip().lower()
            if entity_type:
                return True
        except Exception:
            continue

    return False


def extract_text_from_content(content: dict | None) -> str:
    content = content or {}
    return str(content.get("text") or "")


def video_caption_requires_premium(caption_entities: Any) -> bool:
    entities = normalize_caption_entities(caption_entities)

    for entity in entities:
        entity_type = str(entity.get("type") or "").strip().lower()
        if entity_type == "custom_emoji":
            return True

    return False


def build_video_caption_delivery_payload(
    *,
    caption: Any,
    raw_caption_entities: Any,
    caption_delivery_mode: Any,
) -> dict[str, Any]:
    caption_text = caption or ""
    caption_entities = normalize_caption_entities(raw_caption_entities)
    normalized_delivery_mode = normalize_caption_delivery_mode(caption_delivery_mode)
    requires_premium = video_caption_requires_premium(caption_entities)
    has_any_entities = bool(caption_entities)

    if normalized_delivery_mode == "builder_first":
        selected_mode = "premium"
    elif normalized_delivery_mode == "copy_first":
        selected_mode = "plain"
    else:
        selected_mode = "premium" if has_any_entities else "plain"

    caption_entities_json = None
    if caption_entities:
        try:
            caption_entities_json = json.dumps(caption_entities, ensure_ascii=False)
        except Exception:
            caption_entities_json = None

    if caption_entities_json and isinstance(caption_entities_json, str):
        try:
            json.loads(caption_entities_json)
        except Exception:
            caption_entities_json = None

    return {
        "caption": caption_text,
        "caption_entities": caption_entities,
        "caption_entities_json": caption_entities_json,
        "caption_delivery_mode": normalized_delivery_mode,
        "requires_premium": requires_premium,
        "has_any_entities": has_any_entities,
        "selected_mode": selected_mode,
    }
