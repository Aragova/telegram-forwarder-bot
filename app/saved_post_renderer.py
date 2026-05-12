from __future__ import annotations

import logging
import mimetypes
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from app.telethon_delivery_resolver import recover_album_ids_by_scan, verify_raw_album_ids
from pathlib import Path
from typing import Any

from aiogram.types import InputMediaDocument, InputMediaPhoto, InputMediaVideo

from app.saved_post_entities import deserialize_message_entities, saved_post_entities_to_telethon
from app.saved_post_entities import saved_post_requires_premium_send

logger = logging.getLogger("forwarder")


@dataclass(frozen=True)
class SavedPostRenderResult:
    ok: bool
    method: str
    kind: str
    chat_id: str | None = None
    message_id: int | None = None
    error_text: str | None = None
    premium_required: bool = False
    message_ids: list[int] | None = None
    raw_message_ids: list[int] | None = None
    recovery_status: str | None = None

    def to_dict(self) -> dict:
        return {
            "ok": self.ok,
            "method": self.method,
            "kind": self.kind,
            "chat_id": self.chat_id,
            "message_id": self.message_id,
            "error_text": self.error_text,
            "premium_required": self.premium_required,
            "message_ids": self.message_ids,
            "raw_message_ids": self.raw_message_ids,
            "recovery_status": self.recovery_status,
        }

class SavedPostSentUnverifiedError(Exception):
    def __init__(
        self,
        *,
        target_id: int | str,
        message_ids: list[int],
        verified_ids: list[int] | None = None,
        method: str,
        reason: str,
    ):
        super().__init__(reason)
        self.target_id = str(target_id)
        self.message_ids = [int(x) for x in message_ids if x]
        self.verified_ids = [int(x) for x in (verified_ids or []) if x]
        self.method = method
        self.reason = reason


class SavedPostRenderer:
    def __init__(
        self,
        *,
        bot,
        telethon_client=None,
        temp_dir: str | Path = "media/temp",
        logger_=None,
    ):
        self.bot = bot
        self.telethon_client = telethon_client
        self.temp_dir = Path(temp_dir)
        self.logger = logger_ or logger

    def detect_render_method(self, content: dict[str, Any]) -> str:
        if saved_post_requires_premium_send(content):
            return "telethon_builder"
        return "bot_api"

    async def send(
        self,
        *,
        chat_id: int | str,
        content: dict[str, Any],
        reply_markup=None,
        allow_premium: bool = True,
    ) -> SavedPostRenderResult:
        kind = str(content.get("kind") or "text")
        premium_required = saved_post_requires_premium_send(content)
        method = self.detect_render_method(content)
        self.logger.info(
            "SAVED_POST_RENDER_SEND_START | chat_id=%s | kind=%s | method=%s | premium_required=%s",
            chat_id,
            kind,
            method,
            premium_required,
        )
        raw: dict[str, Any] | None = None
        try:
            if method == "telethon_builder":
                if not allow_premium:
                    return SavedPostRenderResult(
                        ok=False,
                        method="telethon_builder",
                        kind=kind,
                        chat_id=str(chat_id),
                        error_text="Premium-отправка запрещена для этого сценария",
                        premium_required=True,
                    )
                if self.telethon_client is None:
                    return SavedPostRenderResult(
                        ok=False,
                        method="telethon_builder",
                        kind=kind,
                        chat_id=str(chat_id),
                        error_text="Telethon client недоступен для premium-отправки",
                        premium_required=True,
                    )
                raw = await send_saved_post_content_via_telethon(
                    bot=self.bot,
                    telethon_client=self.telethon_client,
                    chat_id=chat_id,
                    content=content,
                    temp_dir=self.temp_dir,
                )
            else:
                raw = await send_saved_post_content(
                    bot=self.bot,
                    chat_id=chat_id,
                    content=content,
                    reply_markup=reply_markup,
                )
            result = SavedPostRenderResult(
                ok=True,
                method=str(raw.get("method") or method),
                kind=str(raw.get("kind") or kind),
                chat_id=str(raw.get("chat_id") or chat_id),
                message_id=raw.get("message_id"),
                premium_required=premium_required,
                message_ids=raw.get("message_ids") or ([raw.get("message_id")] if raw.get("message_id") else None),
            )
            self.logger.info(
                "SAVED_POST_RENDER_SEND_DONE | chat_id=%s | kind=%s | method=%s | message_id=%s | premium_required=%s",
                chat_id,
                kind,
                result.method,
                result.message_id,
                premium_required,
            )
            return result
        except SavedPostSentUnverifiedError as exc:
            self.logger.warning(
                "SAVED_POST_RENDER_SEND_UNVERIFIED | chat_id=%s | kind=%s | method=%s | target_id=%s | raw_ids=%s | verified_ids=%s | reason=%s",
                chat_id,
                kind,
                exc.method,
                exc.target_id,
                exc.message_ids,
                exc.verified_ids,
                exc.reason,
            )
            return SavedPostRenderResult(
                ok=False,
                method=exc.method,
                kind=kind,
                chat_id=str(chat_id),
                error_text=normalize_saved_post_render_error(exc, kind=kind, premium_required=premium_required),
                premium_required=premium_required,
                message_ids=None,
                raw_message_ids=exc.message_ids,
                recovery_status="failed",
            )
        except Exception as exc:
            self.logger.warning(
                "SAVED_POST_RENDER_SEND_FAILED | chat_id=%s | kind=%s | method=%s | premium_required=%s | error=%s",
                chat_id,
                kind,
                method,
                premium_required,
                exc,
                exc_info=True,
            )
            message_ids = None
            if raw:
                message_ids = raw.get("message_ids") or (
                    [raw.get("message_id")] if raw.get("message_id") else None
                )
            return SavedPostRenderResult(
                ok=False,
                method=method,
                kind=kind,
                chat_id=str(chat_id),
                error_text=normalize_saved_post_render_error(exc, kind=kind, premium_required=premium_required),
                premium_required=premium_required,
                message_ids=message_ids,
            )


def normalize_telethon_target(chat_id: int | str) -> int | str:
    value = str(chat_id).strip()
    if value.lstrip("-").isdigit():
        return int(value)
    return chat_id


def _normalize_telegram_channel_id_for_compare(value: Any) -> str | None:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    if raw.startswith("-100"):
        normalized = raw[4:]
    elif raw.startswith("-"):
        normalized = raw[1:]
    else:
        normalized = raw
    return normalized if normalized.isdigit() else None


def _extract_telethon_message_peer_id(message: Any) -> str | None:
    if message is None:
        return None
    candidates = [
        getattr(message, "chat_id", None),
        getattr(getattr(message, "peer_id", None), "channel_id", None),
        getattr(getattr(message, "to_id", None), "channel_id", None),
        getattr(getattr(message, "peer_id", None), "chat_id", None),
        getattr(getattr(message, "to_id", None), "chat_id", None),
    ]
    for candidate in candidates:
        normalized = _normalize_telegram_channel_id_for_compare(candidate)
        if normalized is not None:
            return normalized
    return None


async def verify_telethon_sent_messages(
    *,
    telethon_client,
    target_id: int | str,
    message_ids: list[int],
    min_date=None,
) -> list[int]:
    if not message_ids:
        return []
    entity = normalize_telethon_target(target_id)
    messages = await telethon_client.get_messages(entity, ids=message_ids)
    if not isinstance(messages, list):
        messages = [messages]
    expected = [int(x) for x in message_ids if x]
    expected_set = set(expected)
    target_peer = _normalize_telegram_channel_id_for_compare(target_id)
    min_dt = None
    if min_date is not None:
        min_dt = min_date
        if getattr(min_dt, "tzinfo", None) is None:
            min_dt = min_dt.replace(tzinfo=timezone.utc)
        min_dt = min_dt - timedelta(seconds=60)
    valid: set[int] = set()
    for msg in messages:
        msg_id = int(getattr(msg, "id", 0) or 0)
        if msg_id <= 0 or msg_id not in expected_set:
            continue
        peer_id = _extract_telethon_message_peer_id(msg)
        if target_peer is not None and peer_id is not None and peer_id != target_peer:
            continue
        msg_date = getattr(msg, "date", None)
        if min_dt is not None and msg_date is not None:
            check_dt = msg_date
            if getattr(check_dt, "tzinfo", None) is None:
                check_dt = check_dt.replace(tzinfo=timezone.utc)
            if check_dt < min_dt:
                continue
        valid.add(msg_id)
    verified = [x for x in expected if x in valid]
    if len(verified) < len(expected):
        logger.warning(
            "SAVED_POST_TELETHON_SENT_IDS_VERIFY_FAILED | target_id=%s | expected_ids=%s | verified_ids=%s",
            target_id,
            expected,
            verified,
        )
    else:
        logger.info(
            "SAVED_POST_TELETHON_SENT_IDS_VERIFIED | target_id=%s | message_ids=%s",
            target_id,
            verified,
        )
    return verified


def get_album_source_message_ids(content: dict[str, Any]) -> list[int]:
    ids = content.get("source_message_ids")
    if not ids:
        ids = (content.get("forward_origin") or {}).get("message_ids")
    if not ids:
        first_id = (content.get("forward_origin") or {}).get("message_id")
        ids = [first_id] if first_id else []
    result: list[int] = []
    for item in ids or []:
        try:
            value = int(item)
            if value > 0:
                result.append(value)
        except Exception:
            continue
    return result


def get_album_source_chat_id(content: dict[str, Any]) -> int | str | None:
    value = (content.get("forward_origin") or {}).get("chat_id")
    if value is None:
        return None
    value_str = str(value).strip()
    if value_str.lstrip("-").isdigit():
        return int(value_str)
    return value_str


def normalize_saved_post_render_error(exc: Exception, *, kind: str, premium_required: bool) -> str:
    text = str(exc)
    if "Для premium-альбома нет source_message_ids" in text:
        return "Этот рекламный альбом сохранён без исходных ID сообщений. Замените рекламный пост альбомом заново."
    if "Для premium-альбома сохранены не все source_message_ids" in text:
        return "Этот рекламный альбом сохранён без исходных ID сообщений. Замените рекламный пост альбомом заново."
    if "Не удалось получить исходные сообщения альбома" in text:
        return "Не удалось отправить альбом через аккаунт. Проверьте права аккаунта в целевом канале/группе."
    if "file is too big" in text.lower() and kind == "album" and premium_required:
        return (
            "Не удалось отправить premium-альбом: один из файлов слишком большой для скачивания через Bot API. "
            "Замените рекламный пост альбомом заново, чтобы бот сохранил source_message_ids для отправки через аккаунт."
        )
    return text


def _guess_media_extension(kind: str, media: dict[str, Any]) -> str:
    if kind == "photo":
        return ".jpg"
    if kind == "video":
        return ".mp4"
    if kind == "animation":
        mime_type = str(media.get("mime_type") or "").lower()
        return ".gif" if "gif" in mime_type else ".mp4"
    if kind == "document":
        file_name = str(media.get("file_name") or "")
        suffix = Path(file_name).suffix
        if suffix:
            return suffix
        mime_type = str(media.get("mime_type") or "")
        guessed = mimetypes.guess_extension(mime_type)
        return guessed or ".bin"
    return ".bin"


async def download_saved_post_media_for_telethon(*, bot, content: dict[str, Any], temp_dir: str | Path) -> Path:
    media = content.get("media") or {}
    file_id = str(media.get("file_id") or "")
    if not file_id:
        raise ValueError("В saved post отсутствует media.file_id")
    kind = str(content.get("kind") or "document")
    ext = _guess_media_extension(kind, media)
    temp_dir_path = Path(temp_dir)
    temp_dir_path.mkdir(parents=True, exist_ok=True)
    file_info = await bot.get_file(file_id)
    local_path = temp_dir_path / f"saved_post_{file_id}{ext}"
    await bot.download_file(file_info.file_path, destination=local_path)
    return local_path




async def download_saved_post_media_item_for_telethon(*, bot, item: dict[str, Any], temp_dir: str | Path) -> Path:
    file_id = str(item.get("file_id") or "")
    if not file_id:
        raise ValueError("В элементе альбома отсутствует file_id")
    ext = _guess_media_extension(str(item.get("kind") or "document"), item)
    temp_dir_path = Path(temp_dir)
    temp_dir_path.mkdir(parents=True, exist_ok=True)
    file_info = await bot.get_file(file_id)
    local_path = temp_dir_path / f"saved_post_album_{file_id}{ext}"
    await bot.download_file(file_info.file_path, destination=local_path)
    return local_path


async def send_saved_post_album_via_telethon_source(
    *,
    telethon_client,
    chat_id: int | str,
    content: dict[str, Any],
) -> dict[str, Any]:
    source_chat_id = get_album_source_chat_id(content)
    source_message_ids = get_album_source_message_ids(content)
    media_items_count = len(content.get("media_items") or [])
    if media_items_count > 1 and len(source_message_ids) < media_items_count:
        raise ValueError("Для premium-альбома сохранены не все source_message_ids. Замените рекламный пост альбомом заново.")
    if source_chat_id is None or not source_message_ids:
        raise ValueError("Для premium-альбома нет source_message_ids")
    target_entity = normalize_telethon_target(chat_id)
    source_entity = normalize_telethon_target(source_chat_id)
    started_at = datetime.now(timezone.utc)
    messages = await telethon_client.get_messages(source_entity, ids=source_message_ids)
    if not isinstance(messages, list):
        messages = [messages]
    messages = [m for m in messages if m]
    if not messages:
        raise ValueError("Не удалось получить исходные сообщения альбома")
    medias = [m.media for m in messages if getattr(m, "media", None)]
    sent = await telethon_client.send_file(entity=target_entity, file=medias, caption=content.get("caption") or "", formatting_entities=saved_post_entities_to_telethon(content.get("caption_entities")))
    sent_messages = sent if isinstance(sent, list) else [sent]
    raw_ids = [int(m.id) for m in sent_messages if getattr(m, "id", None)]
    verified = await verify_raw_album_ids(telethon_client=telethon_client, target_id=chat_id, raw_message_ids=raw_ids, expected_count=media_items_count, expected_caption=content.get("caption"), started_at=started_at)
    if verified.ok:
        return {"ok": True, "message_id": verified.message_id, "message_ids": verified.message_ids, "chat_id": str(chat_id), "kind": "album", "method": verified.method}
    recovered = await recover_album_ids_by_scan(telethon_client=telethon_client, target_id=chat_id, expected_count=media_items_count, expected_caption=content.get("caption"), started_at=started_at)
    if recovered.ok:
        return {"ok": True, "message_id": recovered.message_id, "message_ids": recovered.message_ids, "chat_id": str(chat_id), "kind": "album", "method": recovered.method}
    raise SavedPostSentUnverifiedError(target_id=chat_id, message_ids=raw_ids, verified_ids=verified.message_ids, method="telethon_source_unverified", reason=recovered.error_text or verified.error_text or "Не удалось подтвердить ID отправленного альбома в целевом канале.")


async def send_saved_post_content_via_telethon(
    *,
    bot,
    telethon_client,
    chat_id: int | str,
    content: dict[str, Any],
    temp_dir: str | Path,
) -> dict[str, Any]:
    kind = str(content.get("kind") or "text")
    target_entity = normalize_telethon_target(chat_id)
    logger.info(
        "SAVED_POST_TELETHON_BUILDER_SEND_START | target_id=%s | target_entity=%s | kind=%s",
        chat_id,
        target_entity,
        kind,
    )
    local_path: Path | None = None
    local_paths: list[Path] = []
    try:
        if kind == "text":
            entities = saved_post_entities_to_telethon(content.get("entities"))
            sent = await telethon_client.send_message(entity=target_entity, message=content.get("text") or "", formatting_entities=entities)
        elif kind == "album":
            media_items = content.get("media_items") or []
            if not media_items:
                raise ValueError("В альбоме нет media_items")
            try:
                return await send_saved_post_album_via_telethon_source(
                    telethon_client=telethon_client,
                    chat_id=chat_id,
                    content=content,
                )
            except Exception as source_exc:
                source_message_ids = get_album_source_message_ids(content)
                if source_message_ids and len(source_message_ids) >= len(media_items):
                    raise source_exc
                logger.warning(
                    "SAVED_POST_TELETHON_SOURCE_ALBUM_UNAVAILABLE | target_id=%s | error=%s",
                    chat_id,
                    source_exc,
                )
            caption_entities = saved_post_entities_to_telethon(content.get("caption_entities"))
            started_at = datetime.now(timezone.utc)
            local_paths = [await download_saved_post_media_item_for_telethon(bot=bot, item=item, temp_dir=temp_dir) for item in media_items]
            sent = await telethon_client.send_file(entity=target_entity, file=[str(x) for x in local_paths], caption=content.get("caption") or "", formatting_entities=caption_entities)
            sent_messages = sent if isinstance(sent, list) else [sent]
            raw_ids = [int(m.id) for m in sent_messages if getattr(m, "id", None)]
            ids = await verify_telethon_sent_messages(
                telethon_client=telethon_client,
                target_id=chat_id,
                message_ids=raw_ids,
                min_date=started_at,
            )
            if len(ids) < len(media_items):
                raise ValueError("Не удалось подтвердить ID отправленного альбома в целевом канале.")
            logger.info("SAVED_POST_TELETHON_BUILDER_SEND_DONE | target_id=%s | message_id=%s | method=telethon_builder", chat_id, ids[0])
            return {"ok": True, "message_id": ids[0], "message_ids": ids, "chat_id": str(chat_id), "kind": kind, "method": "telethon_builder"}
        else:
            local_path = await download_saved_post_media_for_telethon(bot=bot, content=content, temp_dir=temp_dir)
            caption_entities = saved_post_entities_to_telethon(content.get("caption_entities"))
            sent = await telethon_client.send_file(
                entity=target_entity,
                file=str(local_path),
                caption=content.get("caption") or "",
                formatting_entities=caption_entities,
            )
        logger.info("SAVED_POST_TELETHON_BUILDER_SEND_DONE | target_id=%s | message_id=%s | method=telethon_builder", chat_id, sent.id)
        return {"ok": True, "message_id": sent.id, "message_ids": [sent.id], "chat_id": str(chat_id), "kind": kind, "method": "telethon_builder"}
    except Exception as exc:
        logger.warning("SAVED_POST_TELETHON_BUILDER_SEND_FAILED | target_id=%s | error=%s", chat_id, exc)
        raise
    finally:
        if local_path:
            try:
                local_path.unlink(missing_ok=True)
            except Exception:
                pass
        for path in local_paths:
            try:
                path.unlink(missing_ok=True)
            except Exception:
                pass


async def send_saved_post_content(
    *,
    bot,
    chat_id: int | str,
    content: dict[str, Any],
    reply_markup=None,
) -> dict[str, Any]:
    kind = str(content.get("kind") or "text")

    if kind == "text":
        msg = await bot.send_message(
            chat_id=chat_id,
            text=content.get("text") or "",
            entities=deserialize_message_entities(content.get("entities")),
            reply_markup=reply_markup,
        )
    elif kind == "photo":
        media = content.get("media") or {}
        msg = await bot.send_photo(
            chat_id=chat_id,
            photo=media["file_id"],
            caption=content.get("caption"),
            caption_entities=deserialize_message_entities(content.get("caption_entities")),
            reply_markup=reply_markup,
        )
    elif kind == "video":
        media = content.get("media") or {}
        msg = await bot.send_video(
            chat_id=chat_id,
            video=media["file_id"],
            caption=content.get("caption"),
            caption_entities=deserialize_message_entities(content.get("caption_entities")),
            reply_markup=reply_markup,
        )
    elif kind == "animation":
        media = content.get("media") or {}
        msg = await bot.send_animation(
            chat_id=chat_id,
            animation=media["file_id"],
            caption=content.get("caption"),
            caption_entities=deserialize_message_entities(content.get("caption_entities")),
            reply_markup=reply_markup,
        )
    elif kind == "album":
        media_items = content.get("media_items") or []
        if not media_items:
            raise ValueError("В альбоме нет media_items")
        caption_entities = deserialize_message_entities(content.get("caption_entities"))
        media_group = []
        for idx, item in enumerate(media_items):
            item_kind = str(item.get("kind") or "")
            kwargs = {"media": item["file_id"]}
            if idx == 0:
                kwargs["caption"] = content.get("caption")
                kwargs["caption_entities"] = caption_entities
            if item_kind == "photo":
                media_group.append(InputMediaPhoto(**kwargs))
            elif item_kind == "video":
                media_group.append(InputMediaVideo(**kwargs))
            elif item_kind == "document":
                media_group.append(InputMediaDocument(**kwargs))
            else:
                raise ValueError(f"Unsupported album item kind: {item_kind}")
        messages = await bot.send_media_group(chat_id=chat_id, media=media_group)
        if reply_markup is not None:
            await bot.send_message(chat_id=chat_id, text="⬆️ Предпросмотр рекламного альбома", reply_markup=reply_markup)
        ids = [m.message_id for m in messages]
        return {"ok": True, "message_id": ids[0], "message_ids": ids, "chat_id": str(chat_id), "kind": kind, "method": "bot_api"}
    elif kind == "document":
        media = content.get("media") or {}
        msg = await bot.send_document(
            chat_id=chat_id,
            document=media["file_id"],
            caption=content.get("caption"),
            caption_entities=deserialize_message_entities(content.get("caption_entities")),
            reply_markup=reply_markup,
        )
    else:
        raise ValueError(f"Unsupported saved post kind: {kind}")

    return {
        "ok": True,
        "message_id": msg.message_id,
        "message_ids": [msg.message_id],
        "chat_id": str(chat_id),
        "kind": kind,
        "method": "bot_api",
    }
