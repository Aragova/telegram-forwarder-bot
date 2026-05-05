from __future__ import annotations

import logging
import mimetypes
from dataclasses import dataclass
from pathlib import Path
from typing import Any

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

    def to_dict(self) -> dict:
        return {
            "ok": self.ok,
            "method": self.method,
            "kind": self.kind,
            "chat_id": self.chat_id,
            "message_id": self.message_id,
            "error_text": self.error_text,
            "premium_required": self.premium_required,
        }


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
            return SavedPostRenderResult(
                ok=False,
                method=method,
                kind=kind,
                chat_id=str(chat_id),
                error_text=str(exc),
                premium_required=premium_required,
            )


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


async def send_saved_post_content_via_telethon(
    *,
    bot,
    telethon_client,
    chat_id: int | str,
    content: dict[str, Any],
    temp_dir: str | Path,
) -> dict[str, Any]:
    kind = str(content.get("kind") or "text")
    logger.info("SAVED_POST_TELETHON_BUILDER_SEND_START | target_id=%s | kind=%s", chat_id, kind)
    local_path: Path | None = None
    try:
        if kind == "text":
            entities = saved_post_entities_to_telethon(content.get("entities"))
            sent = await telethon_client.send_message(entity=chat_id, message=content.get("text") or "", formatting_entities=entities)
        else:
            local_path = await download_saved_post_media_for_telethon(bot=bot, content=content, temp_dir=temp_dir)
            caption_entities = saved_post_entities_to_telethon(content.get("caption_entities"))
            sent = await telethon_client.send_file(
                entity=chat_id,
                file=str(local_path),
                caption=content.get("caption") or "",
                formatting_entities=caption_entities,
            )
        logger.info("SAVED_POST_TELETHON_BUILDER_SEND_DONE | target_id=%s | message_id=%s | method=telethon_builder", chat_id, sent.id)
        return {"ok": True, "message_id": sent.id, "chat_id": str(chat_id), "kind": kind, "method": "telethon_builder"}
    except Exception as exc:
        logger.warning("SAVED_POST_TELETHON_BUILDER_SEND_FAILED | target_id=%s | error=%s", chat_id, exc)
        raise
    finally:
        if local_path:
            try:
                local_path.unlink(missing_ok=True)
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
        "chat_id": str(chat_id),
        "kind": kind,
    }
