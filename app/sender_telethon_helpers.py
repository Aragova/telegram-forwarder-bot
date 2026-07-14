from __future__ import annotations

import logging
import asyncio
import json
import struct
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from telethon.tl import types as tl_types

from .config import settings
from .sender_primitives import _detect_message_media_kind
from .telethon_authoritative_resolver import TelethonAuthoritativeMessageResolver, TelethonSendOutcome, telethon_transport_failed

logger = logging.getLogger("forwarder")

MAX_TELEGRAM_THUMB_BYTES = 20 * 1024
MAX_TELEGRAM_THUMB_SIDE = 320


def _positive_int(value: Any) -> int:
    try:
        result = int(round(float(value)))
        return result if result > 0 else 0
    except Exception:
        return 0


class SenderTelethonHelpers:
    def __init__(self, owner: Any):
        self.owner = owner

    def _source_video_attribute(self, message) -> Any | None:
        document = getattr(getattr(message, "media", None), "document", None)
        for attr in getattr(document, "attributes", []) or []:
            if isinstance(attr, tl_types.DocumentAttributeVideo):
                return attr
        return None

    async def _probe_video_file(self, file_path: Path) -> dict[str, Any] | None:
        try:
            proc = await asyncio.create_subprocess_exec(
                "ffprobe", "-v", "error", "-print_format", "json",
                "-show_format", "-show_streams", str(file_path),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, _stderr = await proc.communicate()
            if proc.returncode != 0:
                return None
            data = json.loads(stdout.decode("utf-8", errors="ignore") or "{}")
            video_stream = next((s for s in data.get("streams", []) if s.get("codec_type") == "video"), None)
            if not video_stream:
                return None
            duration = _positive_int(video_stream.get("duration")) or _positive_int((data.get("format") or {}).get("duration"))
            width = _positive_int(video_stream.get("width"))
            height = _positive_int(video_stream.get("height"))
            if not duration or not width or not height:
                return None
            return {
                "duration": duration,
                "width": width,
                "height": height,
                "codec": video_stream.get("codec_name"),
                "has_video": True,
            }
        except Exception as exc:
            logger.warning("VIDEO_REUPLOAD_METADATA | source=ffprobe | path=%s | error=%s", file_path.name, exc)
            return None

    def _valid_thumb_file(self, path: Path | None) -> bool:
        try:
            if not path or not path.exists() or path.stat().st_size <= 0:
                return False
            if path.stat().st_size >= MAX_TELEGRAM_THUMB_BYTES:
                return False
            header = path.read_bytes()[:12]
            if not header.startswith(b"\xff\xd8\xff"):
                return False
            width, height = self._jpeg_dimensions(path)
            return bool(width and height and width <= MAX_TELEGRAM_THUMB_SIDE and height <= MAX_TELEGRAM_THUMB_SIDE)
        except Exception:
            return False

    def _jpeg_dimensions(self, path: Path) -> tuple[int, int]:
        try:
            data = path.read_bytes()
            pos = 2
            while pos + 9 < len(data):
                if data[pos] != 0xFF:
                    pos += 1
                    continue
                marker = data[pos + 1]
                pos += 2
                while marker == 0xFF and pos < len(data):
                    marker = data[pos]
                    pos += 1
                if marker in {0xD8, 0xD9, 0x01} or 0xD0 <= marker <= 0xD7:
                    continue
                if pos + 2 > len(data):
                    break
                segment_len = struct.unpack(">H", data[pos:pos + 2])[0]
                if segment_len < 2 or pos + segment_len > len(data):
                    break
                if marker in {0xC0, 0xC1, 0xC2, 0xC3, 0xC5, 0xC6, 0xC7, 0xC9, 0xCA, 0xCB, 0xCD, 0xCE, 0xCF}:
                    if segment_len >= 7:
                        height = struct.unpack(">H", data[pos + 3:pos + 5])[0]
                        width = struct.unpack(">H", data[pos + 5:pos + 7])[0]
                        return int(width), int(height)
                    break
                pos += segment_len
            return 0, 0
        except Exception:
            return 0, 0

    def _select_source_photo_thumb(self, thumbs: list[Any]) -> Any | None:
        photo_thumbs = [
            thumb for thumb in (thumbs or [])
            if isinstance(thumb, tl_types.PhotoSize)
            and not isinstance(thumb, (tl_types.VideoSize, tl_types.PhotoPathSize))
        ]
        if not photo_thumbs:
            return None
        return max(photo_thumbs, key=lambda t: (_positive_int(getattr(t, "w", 0)) * _positive_int(getattr(t, "h", 0)), _positive_int(getattr(t, "size", 0))))

    async def _download_source_video_thumb(self, message) -> tuple[Path | None, str]:
        document = getattr(getattr(message, "media", None), "document", None)
        thumbs = list(getattr(document, "thumbs", []) or [])
        selected_thumb = self._select_source_photo_thumb(thumbs)
        if not selected_thumb:
            return None, "none"
        tmp = Path(tempfile.NamedTemporaryFile(prefix="telegram_video_thumb_", suffix=".jpg", delete=False).name)
        try:
            downloaded = await self.owner.telethon.download_media(message, file=str(tmp), thumb=selected_thumb)
            path = Path(downloaded) if downloaded else tmp
            valid = self._valid_thumb_file(path)
            logger.info("VIDEO_REUPLOAD_THUMB | source=telegram | path=%s | valid=%s", path, valid)
            if valid:
                return path, "telegram"
            path.unlink(missing_ok=True)
            return None, "none"
        except Exception as exc:
            logger.warning("VIDEO_REUPLOAD_THUMB | source=telegram | path=%s | valid=False | error=%s", tmp, exc)
            tmp.unlink(missing_ok=True)
            return None, "none"

    async def _generate_video_thumb(self, file_path: Path, duration: int) -> tuple[Path | None, str, float | None]:
        seek_seconds = 1.0
        if duration > 10:
            seek_seconds = 2.0
        elif duration > 1:
            seek_seconds = max(0.5, min(float(duration) / 2.0, float(duration) - 0.1))
        tmp = Path(tempfile.NamedTemporaryFile(prefix="generated_video_thumb_", suffix=".jpg", delete=False).name)
        try:
            last_error = ""
            for quality in (5, 8, 12, 16, 20):
                tmp.unlink(missing_ok=True)
                proc = await asyncio.create_subprocess_exec(
                    "ffmpeg", "-y", "-ss", f"{seek_seconds:.2f}", "-i", str(file_path),
                    "-frames:v", "1", "-vf", "scale=320:320:force_original_aspect_ratio=decrease",
                    "-q:v", str(quality), str(tmp),
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                _stdout, stderr = await proc.communicate()
                last_error = stderr.decode("utf-8", errors="ignore")[-300:]
                valid = proc.returncode == 0 and self._valid_thumb_file(tmp)
                if valid:
                    logger.info("VIDEO_REUPLOAD_THUMB | source=generated_ffmpeg | seek_seconds=%s | path=%s | valid=True", seek_seconds, tmp)
                    return tmp, "generated_ffmpeg", seek_seconds
            tmp.unlink(missing_ok=True)
            logger.warning("VIDEO_REUPLOAD_THUMB | source=none | error=%s", last_error)
            return None, "none", seek_seconds
        except Exception as exc:
            tmp.unlink(missing_ok=True)
            logger.warning("VIDEO_REUPLOAD_THUMB | source=none | error=%s", exc)
            return None, "none", seek_seconds

    async def _build_video_reupload_kwargs(self, *, message, file_path: Path, force_document: bool) -> tuple[list[Any] | None, Path | None, str, dict[str, int]]:
        if force_document:
            return None, None, "none", {}
        source_attr = self._source_video_attribute(message)
        source_meta = {
            "duration": _positive_int(getattr(source_attr, "duration", 0)),
            "width": _positive_int(getattr(source_attr, "w", 0)),
            "height": _positive_int(getattr(source_attr, "h", 0)),
        } if source_attr else {}
        ffprobe_meta = await self._probe_video_file(file_path)
        meta = ffprobe_meta or source_meta
        meta_source = "ffprobe" if ffprobe_meta else "telegram_attribute"
        if not meta or not (meta.get("duration") and meta.get("width") and meta.get("height")):
            return None, None, "none", {}
        logger.info(
            "VIDEO_REUPLOAD_METADATA | source=%s | duration=%s | width=%s | height=%s | supports_streaming=%s",
            meta_source, meta["duration"], meta["width"], meta["height"], True,
        )
        attr_kwargs = {
            "duration": int(meta["duration"]),
            "w": int(meta["width"]),
            "h": int(meta["height"]),
            "supports_streaming": True,
        }
        if source_attr is not None:
            for name in ("round_message", "nosound"):
                if hasattr(source_attr, name):
                    attr_kwargs[name] = bool(getattr(source_attr, name))
        attributes = [tl_types.DocumentAttributeVideo(**attr_kwargs)]
        thumb_path, thumb_source = await self._download_source_video_thumb(message)
        if not thumb_path:
            thumb_path, thumb_source, _seek = await self._generate_video_thumb(file_path, int(meta["duration"]))
        return attributes, thumb_path, thumb_source, {"duration": int(meta["duration"]), "width": int(meta["width"]), "height": int(meta["height"])}

    async def send_text_via_telethon(
        self,
        *,
        target_id,
        target_thread_id,
        text: str,
        entities,
        source_message_ids: set[int] | None = None,
    ) -> TelethonSendOutcome:
        entity = int(target_id) if str(target_id).lstrip("-").isdigit() else target_id
        formatting_entities = self.owner._clone_telethon_entities(entities, text)

        logger.info(
            "TELETHON_TEXT_SEND | START | target=%s | thread=%s | text_len=%s | entities_in=%s | entities_out=%s",
            target_id,
            target_thread_id,
            len(text or ""),
            len(entities or []),
            len(formatting_entities or []),
        )

        send_kwargs = {
            "entity": entity,
            "message": text or "",
            "formatting_entities": formatting_entities or None,
            "link_preview": False,
        }

        if target_thread_id is not None:
            send_kwargs["comment_to"] = int(target_thread_id)

        resolver = TelethonAuthoritativeMessageResolver(self.owner.telethon)
        before_max_message_id = await resolver.get_before_max_message_id(entity, target_id)
        send_started_at = datetime.now(timezone.utc)
        try:
            sent = await self.owner.telethon.send_message(**send_kwargs)
        except Exception as exc:
            logger.warning("TELETHON_TEXT_SEND | FAILED | target=%s | thread=%s | error=%s", target_id, target_thread_id, exc)
            return telethon_transport_failed(str(exc))
        send_finished_at = datetime.now(timezone.utc)
        returned_candidate_id = int(sent.id) if sent else None
        if not sent:
            return telethon_transport_failed("telethon_send_message_returned_empty")
        try:
            resolved = await resolver.resolve_authoritative_single_message(
                target_entity=entity,
                target_id=target_id,
                sent=sent,
                expected_text=text or "",
                before_max_message_id=before_max_message_id,
                send_started_at=send_started_at,
                send_finished_at=send_finished_at,
                source_message_ids=source_message_ids,
                target_thread_id=target_thread_id,
            )
        except Exception as exc:
            logger.warning("TELETHON_TEXT_SEND | RESOLUTION_FAILED_AFTER_ACCEPT | target=%s | returned_candidate_id=%s | error=%s | action=no_second_send", target_id, returned_candidate_id, exc)
            return TelethonSendOutcome(True, True, False, None, [], returned_candidate_id=returned_candidate_id, returned_candidate_ids=[returned_candidate_id] if returned_candidate_id else [], resolution_method="resolver_exception", error_text=str(exc))
        sent_id = resolved.authoritative_message_id if resolved.ok else None
        logger.info(
            "TELETHON_TEXT_SEND | OK | target=%s | thread=%s | returned_candidate_id=%s | sent_message_id=%s | resolution_method=%s",
            target_id, target_thread_id, returned_candidate_id, sent_id, resolved.resolution_method,
        )
        return TelethonSendOutcome(True, True, bool(resolved.ok), sent_id, [sent_id] if sent_id else [], returned_candidate_id=returned_candidate_id, returned_candidate_ids=[returned_candidate_id] if returned_candidate_id else [], resolution_method=resolved.resolution_method, error_text=resolved.error_text)

    async def send_file_via_telethon(
        self,
        *,
        target_id,
        target_thread_id,
        message,
        file_path: Path | None = None,
        force_document: bool = False,
        post_row: dict | None = None,
        is_self_loop: bool = False,
    ) -> TelethonSendOutcome:
        content = self.owner._content_from_message_or_post(message=message, post_row=post_row)
        raw_text, raw_entities = self.owner._build_text_and_entities_from_content(content)
        formatting_entities = self.owner._clone_telethon_entities(raw_entities, raw_text)

        entity = int(target_id) if str(target_id).lstrip("-").isdigit() else target_id
        media_kind = _detect_message_media_kind(message)
        supports_streaming = media_kind == "video"

        try:
            logger.info(
                "TELETHON_FILE_SEND | START_ORIGINAL_MEDIA | target=%s | thread=%s | media_kind=%s | caption_len=%s | entities_in=%s | entities_out=%s | supports_streaming=%s",
                target_id,
                target_thread_id,
                media_kind,
                len(raw_text or ""),
                len(raw_entities or []),
                len(formatting_entities or []),
                supports_streaming,
            )

            send_kwargs = {
                "entity": entity,
                "file": getattr(message, "media", None),
                "caption": raw_text or "",
                "formatting_entities": formatting_entities or None,
                "force_document": force_document,
                "link_preview": False,
                "supports_streaming": supports_streaming,
            }

            if target_thread_id is not None:
                send_kwargs["comment_to"] = int(target_thread_id)

            resolver = TelethonAuthoritativeMessageResolver(self.owner.telethon)
            before_max_message_id = await resolver.get_before_max_message_id(entity, target_id)
            send_started_at = datetime.now(timezone.utc)
            try:
                sent = await self.owner.telethon.send_file(**send_kwargs)
            except Exception:
                raise
            send_finished_at = datetime.now(timezone.utc)
            returned_candidate_id = int(sent.id) if sent else None
            if not sent:
                return telethon_transport_failed("telethon_send_file_returned_empty")
            try:
                resolved = await resolver.resolve_authoritative_single_message(
                    target_entity=entity,
                    target_id=target_id,
                    sent=sent,
                    expected_message=message,
                    expected_text=raw_text or "",
                    before_max_message_id=before_max_message_id,
                    send_started_at=send_started_at,
                    send_finished_at=send_finished_at,
                    source_message_ids=({int(getattr(message, "id"))} if is_self_loop and getattr(message, "id", None) else None),
                    target_thread_id=target_thread_id,
                )
            except Exception as exc:
                logger.warning("TELETHON_FILE_SEND | RESOLUTION_FAILED_AFTER_ACCEPT | target=%s | returned_candidate_id=%s | error=%s | action=no_second_send", target_id, returned_candidate_id, exc)
                return TelethonSendOutcome(True, True, False, None, [], returned_candidate_id=returned_candidate_id, returned_candidate_ids=[returned_candidate_id] if returned_candidate_id else [], resolution_method="resolver_exception", error_text=str(exc))
            sent_id = resolved.authoritative_message_id if resolved.ok else None

            logger.info(
                "TELETHON_FILE_SEND | OK_ORIGINAL_MEDIA | target=%s | thread=%s | returned_candidate_id=%s | sent_message_id=%s | resolution_method=%s",
                target_id,
                target_thread_id,
                returned_candidate_id,
                sent_id,
                resolved.resolution_method,
            )
            return TelethonSendOutcome(True, True, bool(resolved.ok), sent_id, [sent_id] if sent_id else [], returned_candidate_id=returned_candidate_id, returned_candidate_ids=[returned_candidate_id] if returned_candidate_id else [], resolution_method=resolved.resolution_method, error_text=resolved.error_text)

        except Exception as exc:
            logger.warning(
                "TELETHON_FILE_SEND | FAILED_ORIGINAL_MEDIA | target=%s | thread=%s | error=%s",
                target_id,
                target_thread_id,
                exc,
            )

        if not file_path:
            logger.warning(
                "TELETHON_FILE_SEND | NO_FILE_PATH_FALLBACK | target=%s | thread=%s",
                target_id,
                target_thread_id,
            )
            return telethon_transport_failed("telethon_original_media_failed_no_file_path")

        temp_thumb_path: Path | None = None
        video_attributes = None
        thumb_source = "none"
        video_meta: dict[str, int] = {}
        if media_kind == "video" and file_path and Path(file_path).exists() and not force_document:
            video_attributes, temp_thumb_path, thumb_source, video_meta = await self._build_video_reupload_kwargs(
                message=message,
                file_path=Path(file_path),
                force_document=force_document,
            )

        try:
            logger.info(
                "TELETHON_FILE_SEND | START_FILE_PATH | target=%s | thread=%s | file=%s | media_kind=%s | caption_len=%s | entities_in=%s | entities_out=%s | duration=%s | width=%s | height=%s | attributes=%s | thumb_source=%s | thumb_path=%s | supports_streaming=%s",
                target_id,
                target_thread_id,
                file_path.name,
                media_kind,
                len(raw_text or ""),
                len(raw_entities or []),
                len(formatting_entities or []),
                video_meta.get("duration"),
                video_meta.get("width"),
                video_meta.get("height"),
                len(video_attributes or []),
                thumb_source,
                str(temp_thumb_path) if temp_thumb_path else None,
                supports_streaming,
            )

            send_kwargs = {
                "entity": entity,
                "file": str(file_path),
                "caption": raw_text or "",
                "formatting_entities": formatting_entities or None,
                "force_document": force_document,
                "link_preview": False,
                "supports_streaming": supports_streaming,
            }
            if video_attributes:
                send_kwargs["attributes"] = video_attributes
                send_kwargs["force_document"] = False
                send_kwargs["supports_streaming"] = True
            if temp_thumb_path:
                send_kwargs["thumb"] = str(temp_thumb_path)

            if target_thread_id is not None:
                send_kwargs["comment_to"] = int(target_thread_id)

            resolver = TelethonAuthoritativeMessageResolver(self.owner.telethon)
            before_max_message_id = await resolver.get_before_max_message_id(entity, target_id)
            send_started_at = datetime.now(timezone.utc)
            try:
                sent = await self.owner.telethon.send_file(**send_kwargs)
            except Exception:
                raise
            send_finished_at = datetime.now(timezone.utc)
            returned_candidate_id = int(sent.id) if sent else None
            if not sent:
                return telethon_transport_failed("telethon_send_file_returned_empty")
            try:
                resolved = await resolver.resolve_authoritative_single_message(
                    target_entity=entity,
                    target_id=target_id,
                    sent=sent,
                    expected_message=message,
                    expected_text=raw_text or "",
                    before_max_message_id=before_max_message_id,
                    send_started_at=send_started_at,
                    send_finished_at=send_finished_at,
                    source_message_ids=({int(getattr(message, "id"))} if is_self_loop and getattr(message, "id", None) else None),
                    target_thread_id=target_thread_id,
                )
            except Exception as exc:
                logger.warning("TELETHON_FILE_SEND | RESOLUTION_FAILED_AFTER_ACCEPT | target=%s | returned_candidate_id=%s | error=%s | action=no_second_send", target_id, returned_candidate_id, exc)
                return TelethonSendOutcome(True, True, False, None, [], returned_candidate_id=returned_candidate_id, returned_candidate_ids=[returned_candidate_id] if returned_candidate_id else [], resolution_method="resolver_exception", error_text=str(exc))
            sent_id = resolved.authoritative_message_id if resolved.ok else None

            logger.info(
                "TELETHON_FILE_SEND | OK_FILE_PATH | target=%s | thread=%s | file=%s | returned_candidate_id=%s | sent_message_id=%s | resolution_method=%s | duration=%s | thumb_used=%s",
                target_id,
                target_thread_id,
                file_path.name,
                returned_candidate_id,
                sent_id,
                resolved.resolution_method,
                video_meta.get("duration"),
                bool(temp_thumb_path),
            )
            return TelethonSendOutcome(True, True, bool(resolved.ok), sent_id, [sent_id] if sent_id else [], returned_candidate_id=returned_candidate_id, returned_candidate_ids=[returned_candidate_id] if returned_candidate_id else [], resolution_method=resolved.resolution_method, error_text=resolved.error_text)

        except Exception as exc:
            logger.warning(
                "TELETHON_FILE_SEND | FAILED_FILE_PATH | target=%s | thread=%s | file=%s | error=%s",
                target_id,
                target_thread_id,
                file_path.name if file_path else None,
                exc,
            )
            return telethon_transport_failed(str(exc))
        finally:
            if temp_thumb_path:
                try:
                    temp_thumb_path.unlink(missing_ok=True)
                except Exception:
                    pass

    async def send_album_via_telethon(
        self,
        *,
        messages,
        target_id,
        target_thread_id,
        post_rows: list[dict] | None = None,
        is_self_loop: bool = False,
    ) -> dict:
        downloaded_paths: list[Path] = []

        try:
            if not messages:
                return telethon_transport_failed("Пустой список сообщений для Telethon album send").to_reupload_album_result(sent_count=0)

            entity = int(target_id) if str(target_id).lstrip("-").isdigit() else target_id

            caption_text = ""
            caption_entities = None
            media_items = []

            for idx, message in enumerate(messages):
                media = getattr(message, "media", None)
                if not media:
                    return telethon_transport_failed("Один из элементов альбома не содержит media").to_reupload_album_result(sent_count=0)
                media_items.append(media)

                post_row = post_rows[idx] if post_rows and idx < len(post_rows) else None
                content = self.owner._content_from_message_or_post(message=message, post_row=post_row)
                raw_text, raw_entities = self.owner._build_text_and_entities_from_content(content)

                if raw_text and not caption_text:
                    caption_text = raw_text
                    caption_entities = raw_entities

            formatting_entities = self.owner._clone_telethon_entities(caption_entities, caption_text)

            logger.info(
                "TELETHON_ALBUM_SEND | START_ORIGINAL_MEDIA | target=%s | thread=%s | items=%s | caption_len=%s | entities_in=%s | entities_out=%s",
                target_id,
                target_thread_id,
                len(media_items),
                len(caption_text or ""),
                len(caption_entities or []),
                len(formatting_entities or []),
            )

            send_kwargs = {
                "entity": entity,
                "file": media_items,
                "caption": caption_text or "",
                "formatting_entities": formatting_entities or None,
                "link_preview": False,
            }

            if target_thread_id is not None:
                send_kwargs["comment_to"] = int(target_thread_id)

            resolver = TelethonAuthoritativeMessageResolver(self.owner.telethon)
            before_max_message_id = await resolver.get_before_max_message_id(entity, target_id)
            send_started_at = datetime.now(timezone.utc)
            sent = await self.owner.telethon.send_file(**send_kwargs)
            send_finished_at = datetime.now(timezone.utc)
            if not sent:
                return telethon_transport_failed("telethon_album_send_file_returned_empty").to_reupload_album_result(sent_count=0)
            sent_messages = sent if isinstance(sent, list) else [sent]

            if sent_messages:
                try:
                    resolved = await resolver.resolve_authoritative_album_messages(
                        target_entity=entity,
                        target_id=target_id,
                        sent_messages=sent_messages,
                        expected_messages=list(messages),
                        expected_text=caption_text or "",
                        before_max_message_id=before_max_message_id,
                        send_started_at=send_started_at,
                        send_finished_at=send_finished_at,
                        source_message_ids=({int(getattr(m, "id")) for m in messages if getattr(m, "id", None)} if is_self_loop else None),
                        target_thread_id=target_thread_id,
                    )
                except Exception as exc:
                    returned_ids = [int(m.id) for m in sent_messages if m and getattr(m, "id", None)]
                    logger.warning("TELETHON_ALBUM_SEND | RESOLUTION_FAILED_AFTER_ACCEPT | target=%s | returned_candidate_ids=%s | error=%s | action=no_second_send", target_id, returned_ids, exc)
                    return TelethonSendOutcome(True, True, False, None, [], returned_candidate_ids=returned_ids, resolution_method="resolver_exception", error_text=str(exc)).to_reupload_album_result(sent_count=len(sent_messages))
                if not resolved.ok:
                    logger.warning("TELETHON_ALBUM_SEND | ACCEPTED_UNRESOLVED | target=%s | thread=%s | returned_candidate_ids=%s", target_id, target_thread_id, resolved.returned_candidate_ids)
                    return TelethonSendOutcome(True, True, False, None, [], returned_candidate_ids=resolved.returned_candidate_ids or [], resolution_method=resolved.resolution_method, error_text=resolved.error_text or "telethon_album_target_id_unresolved").to_reupload_album_result(sent_count=len(sent_messages))
                first_id = int(resolved.authoritative_message_id)
                logger.info(
                    "TELETHON_ALBUM_SEND | OK_ORIGINAL_MEDIA | target=%s | thread=%s | sent_count=%s | returned_candidate_ids=%s | sent_message_ids=%s | first_message_id=%s | resolution_method=%s",
                    target_id,
                    target_thread_id,
                    len(sent_messages),
                    resolved.returned_candidate_ids,
                    resolved.authoritative_message_ids,
                    first_id,
                    resolved.resolution_method,
                )
                return {
                    "ok": True,
                    "sent_message_id": first_id,
                    "sent_message_ids": resolved.authoritative_message_ids or [],
                    "sent_count": len(sent_messages),
                    "error_text": None,
                    "returned_candidate_ids": resolved.returned_candidate_ids,
                    "resolution_method": resolved.resolution_method,
                }

            logger.warning(
                "TELETHON_ALBUM_SEND | EMPTY_ORIGINAL_MEDIA | target=%s | thread=%s",
                target_id,
                target_thread_id,
            )

        except Exception as exc:
            logger.warning(
                "TELETHON_ALBUM_SEND | FAILED_ORIGINAL_MEDIA | target=%s | thread=%s | error=%s",
                target_id,
                target_thread_id,
                exc,
            )

        try:
            files: list[str] = []
            caption_text = ""
            caption_entities = None

            for idx, message in enumerate(messages):
                file_path = await self.owner.telethon.download_media(
                    message,
                    file=str(settings.media_cache_path),
                )
                if not file_path:
                    return telethon_transport_failed(f"Не удалось скачать элемент альбома {idx + 1}/{len(messages)}").to_reupload_album_result(sent_count=len(files))

                path = Path(file_path)
                downloaded_paths.append(path)
                files.append(str(path))

                post_row = post_rows[idx] if post_rows and idx < len(post_rows) else None
                content = self.owner._content_from_message_or_post(message=message, post_row=post_row)
                raw_text, raw_entities = self.owner._build_text_and_entities_from_content(content)

                if raw_text and not caption_text:
                    caption_text = raw_text
                    caption_entities = raw_entities

            formatting_entities = self.owner._clone_telethon_entities(caption_entities, caption_text)

            logger.info(
                "TELETHON_ALBUM_SEND | START_FILE_PATH | target=%s | thread=%s | items=%s | caption_len=%s | entities_in=%s | entities_out=%s",
                target_id,
                target_thread_id,
                len(files),
                len(caption_text or ""),
                len(caption_entities or []),
                len(formatting_entities or []),
            )

            send_kwargs = {
                "entity": entity,
                "file": files,
                "caption": caption_text or "",
                "formatting_entities": formatting_entities or None,
                "link_preview": False,
            }

            if target_thread_id is not None:
                send_kwargs["comment_to"] = int(target_thread_id)

            resolver = TelethonAuthoritativeMessageResolver(self.owner.telethon)
            before_max_message_id = await resolver.get_before_max_message_id(entity, target_id)
            send_started_at = datetime.now(timezone.utc)
            sent = await self.owner.telethon.send_file(**send_kwargs)
            send_finished_at = datetime.now(timezone.utc)
            if not sent:
                return telethon_transport_failed("telethon_album_send_file_returned_empty").to_reupload_album_result(sent_count=0)
            sent_messages = sent if isinstance(sent, list) else [sent]

            if not sent_messages:
                return telethon_transport_failed("Telethon send_file(album) вернул пустой результат").to_reupload_album_result(sent_count=0)

            try:
                resolved = await resolver.resolve_authoritative_album_messages(
                    target_entity=entity,
                    target_id=target_id,
                    sent_messages=sent_messages,
                    expected_messages=list(messages),
                    expected_text=caption_text or "",
                    before_max_message_id=before_max_message_id,
                    send_started_at=send_started_at,
                    send_finished_at=send_finished_at,
                    source_message_ids=({int(getattr(m, "id")) for m in messages if getattr(m, "id", None)} if is_self_loop else None),
                    target_thread_id=target_thread_id,
                )
            except Exception as exc:
                returned_ids = [int(m.id) for m in sent_messages if m and getattr(m, "id", None)]
                logger.warning("TELETHON_ALBUM_SEND | RESOLUTION_FAILED_AFTER_ACCEPT | target=%s | returned_candidate_ids=%s | error=%s | action=no_second_send", target_id, returned_ids, exc)
                return TelethonSendOutcome(True, True, False, None, [], returned_candidate_ids=returned_ids, resolution_method="resolver_exception", error_text=str(exc)).to_reupload_album_result(sent_count=len(sent_messages))
            if not resolved.ok:
                logger.warning("TELETHON_ALBUM_SEND | ACCEPTED_UNRESOLVED_FILE_PATH | target=%s | thread=%s | returned_candidate_ids=%s", target_id, target_thread_id, resolved.returned_candidate_ids)
                return TelethonSendOutcome(True, True, False, None, [], returned_candidate_ids=resolved.returned_candidate_ids or [], resolution_method=resolved.resolution_method, error_text=resolved.error_text or "telethon_album_target_id_unresolved").to_reupload_album_result(sent_count=len(sent_messages))

            first_id = int(resolved.authoritative_message_id)
            logger.info(
                "TELETHON_ALBUM_SEND | OK_FILE_PATH | target=%s | thread=%s | sent_count=%s | returned_candidate_ids=%s | sent_message_ids=%s | first_message_id=%s | resolution_method=%s",
                target_id,
                target_thread_id,
                len(sent_messages),
                resolved.returned_candidate_ids,
                resolved.authoritative_message_ids,
                first_id,
                resolved.resolution_method,
            )
            return {
                "ok": True,
                "sent_message_id": first_id,
                "sent_message_ids": resolved.authoritative_message_ids or [],
                "sent_count": len(sent_messages),
                "error_text": None,
                "returned_candidate_ids": resolved.returned_candidate_ids,
                "resolution_method": resolved.resolution_method,
            }

        except Exception as exc:
            logger.exception(
                "TELETHON_ALBUM_SEND | FAILED_FILE_PATH | target=%s | thread=%s | error=%s",
                target_id,
                target_thread_id,
                exc,
            )
            return telethon_transport_failed(str(exc)).to_reupload_album_result(sent_count=0)

        finally:
            for path in downloaded_paths:
                try:
                    path.unlink(missing_ok=True)
                except Exception:
                    pass


    async def verify_album_delivery(
        self,
        *,
        target_id,
        expected_count: int,
        sent_message_ids: list[int] | None,
        target_thread_id: int | None = None,
        target_grouped_id: int | None = None,
    ):
        try:
            if not sent_message_ids:
                return {
                    "ok": False,
                    "error_text": "reupload_album_sent_ids_missing",
                    "grouped_id": None,
                    "count": 0,
                    "first_message_id": None,
                    "sent_message_ids": [],
                }

            entity = int(target_id) if str(target_id).lstrip("-").isdigit() else target_id
            fetched = await self.owner.telethon.get_messages(entity, ids=sent_message_ids)
            fetched_list = fetched if isinstance(fetched, list) else [fetched]
            fetched_list = [m for m in fetched_list if m]
            actual_ids = sorted(int(m.id) for m in fetched_list)

            if len(actual_ids) != len(sent_message_ids):
                return {
                    "ok": False,
                    "error_text": "verify_album_sent_ids_not_found",
                    "grouped_id": None,
                    "count": len(actual_ids),
                    "first_message_id": min(actual_ids) if actual_ids else None,
                    "sent_message_ids": actual_ids,
                }

            grouped_ids = {int(m.grouped_id) for m in fetched_list if getattr(m, "grouped_id", None)}
            if expected_count > 1 and len(grouped_ids) > 1:
                return {
                    "ok": False,
                    "error_text": "verify_album_grouped_id_mismatch",
                    "grouped_id": None,
                    "count": len(actual_ids),
                    "first_message_id": min(actual_ids),
                    "sent_message_ids": actual_ids,
                }

            grouped_id = next(iter(grouped_ids)) if grouped_ids else None
            if target_grouped_id and grouped_id and int(target_grouped_id) != int(grouped_id):
                return {
                    "ok": False,
                    "error_text": "verify_album_target_grouped_id_mismatch",
                    "grouped_id": grouped_id,
                    "count": len(actual_ids),
                    "first_message_id": min(actual_ids),
                    "sent_message_ids": actual_ids,
                }

            if len(actual_ids) != expected_count:
                return {
                    "ok": False,
                    "error_text": "verify_album_count_mismatch",
                    "grouped_id": grouped_id,
                    "count": len(actual_ids),
                    "first_message_id": min(actual_ids),
                    "sent_message_ids": actual_ids,
                }

            return {
                "ok": True,
                "error_text": None,
                "grouped_id": grouped_id,
                "count": len(actual_ids),
                "first_message_id": min(actual_ids),
                "sent_message_ids": actual_ids,
            }

        except Exception as exc:
            logger.exception("verify_album_delivery: ошибка verify: %s", exc)
            return {
                "ok": False,
                "error_text": f"Ошибка verify: {exc}",
                "grouped_id": None,
                "count": 0,
                "first_message_id": None,
                "sent_message_ids": [],
            }

    async def verify_self_loop_video_metadata(
        self,
        *,
        rule_id,
        source_message_id,
        target_id,
        sent_message_id,
    ) -> None:
        try:
            entity = int(target_id) if str(target_id).lstrip("-").isdigit() else target_id
            msg = await self.owner.telethon.get_messages(entity, ids=int(sent_message_id))
            if not msg or int(getattr(msg, "id", 0) or 0) != int(sent_message_id):
                logger.warning(
                    "SELF_LOOP_VIDEO_METADATA_VERIFY_WARNING | rule_id=%s | sent_message_id=%s | reason=message_missing",
                    rule_id,
                    sent_message_id,
                )
                return
            attr = self._source_video_attribute(msg)
            if not attr:
                logger.warning(
                    "SELF_LOOP_VIDEO_METADATA_VERIFY_WARNING | rule_id=%s | sent_message_id=%s | reason=attribute_missing",
                    rule_id,
                    sent_message_id,
                )
                return
            duration = _positive_int(getattr(attr, "duration", 0))
            width = _positive_int(getattr(attr, "w", 0))
            height = _positive_int(getattr(attr, "h", 0))
            thumbs = len(getattr(getattr(getattr(msg, "media", None), "document", None), "thumbs", []) or [])
            if not duration or not width or not height:
                reason = "duration_zero" if not duration else "size_zero"
                logger.warning(
                    "SELF_LOOP_VIDEO_METADATA_VERIFY_WARNING | rule_id=%s | sent_message_id=%s | reason=%s",
                    rule_id,
                    sent_message_id,
                    reason,
                )
                return
            logger.info(
                "SELF_LOOP_VIDEO_METADATA_VERIFY_OK | rule_id=%s | source_message_id=%s | sent_message_id=%s | duration=%s | width=%s | height=%s | thumbs=%s",
                rule_id,
                source_message_id,
                sent_message_id,
                duration,
                width,
                height,
                thumbs,
            )
        except Exception as exc:
            logger.warning(
                "SELF_LOOP_VIDEO_METADATA_VERIFY_WARNING | rule_id=%s | sent_message_id=%s | reason=verify_error | error=%s",
                rule_id,
                sent_message_id,
                exc,
            )
