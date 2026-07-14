from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

logger = logging.getLogger("forwarder")


@dataclass
class TelethonResolutionResult:
    ok: bool
    authoritative_message_id: int | None
    authoritative_message_ids: list[int] | None = None
    returned_candidate_id: int | None = None
    returned_candidate_ids: list[int] | None = None
    resolution_method: str | None = None
    error_text: str | None = None


def _message_id(message: Any) -> int | None:
    try:
        value = int(getattr(message, "id", 0) or 0)
        return value if value > 0 else None
    except Exception:
        return None


def _message_text(message: Any) -> str:
    return str(getattr(message, "message", None) or getattr(message, "text", None) or "")


def _document(message: Any) -> Any | None:
    return getattr(getattr(message, "media", None), "document", None)


def _photo(message: Any) -> Any | None:
    media = getattr(message, "media", None)
    return getattr(media, "photo", None) or (media if media.__class__.__name__.lower().endswith("photo") else None)


def _filename(document: Any) -> str | None:
    for attr in getattr(document, "attributes", []) or []:
        name = getattr(attr, "file_name", None)
        if name:
            return str(name)
    return None


def build_media_fingerprint(message: Any, *, text: str | None = None) -> dict[str, Any]:
    document = _document(message)
    photo = _photo(message)
    if document is not None:
        return {
            "kind": "document",
            "document_id": getattr(document, "id", None),
            "size": getattr(document, "size", None),
            "mime_type": getattr(document, "mime_type", None),
            "filename": _filename(document),
            "text": text if text is not None else _message_text(message),
        }
    if photo is not None:
        return {"kind": "photo", "photo_id": getattr(photo, "id", None), "text": text if text is not None else _message_text(message)}
    return {"kind": "text", "text": text if text is not None else _message_text(message)}


def _fingerprint_score(expected: dict[str, Any], actual: dict[str, Any]) -> int:
    if expected.get("kind") != actual.get("kind"):
        return -1
    if expected.get("document_id") and expected.get("document_id") == actual.get("document_id"):
        return 100
    if expected.get("photo_id") and expected.get("photo_id") == actual.get("photo_id"):
        return 90
    if expected.get("filename") and expected.get("filename") == actual.get("filename") and expected.get("size") == actual.get("size") and expected.get("mime_type") == actual.get("mime_type"):
        return 80
    if (expected.get("text") or "") == (actual.get("text") or ""):
        return 40
    return -1


@dataclass(slots=True)
class TelethonSendOutcome:
    transport_attempted: bool
    transport_accepted: bool
    authoritative_resolved: bool
    authoritative_message_id: int | None
    authoritative_message_ids: list[int]
    returned_candidate_id: int | None = None
    returned_candidate_ids: list[int] | None = None
    resolution_method: str | None = None
    error_text: str | None = None

    @property
    def ok(self) -> bool:
        return bool(self.transport_accepted and self.authoritative_resolved and self.authoritative_message_id)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, int):
            return self.authoritative_message_id == other
        return super().__eq__(other)

    def to_reupload_album_result(self, *, sent_count: int = 0) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "transport_attempted": self.transport_attempted,
            "transport_accepted": self.transport_accepted,
            "authoritative_resolved": self.authoritative_resolved,
            "sent_message_id": self.authoritative_message_id if self.authoritative_resolved else None,
            "sent_message_ids": list(self.authoritative_message_ids or []) if self.authoritative_resolved else [],
            "sent_count": sent_count,
            "error_text": self.error_text,
            "returned_candidate_id": self.returned_candidate_id,
            "returned_candidate_ids": list(self.returned_candidate_ids or []),
            "resolution_method": self.resolution_method,
        }


def telethon_transport_failed(error_text: str | None = None) -> TelethonSendOutcome:
    return TelethonSendOutcome(
        transport_attempted=True,
        transport_accepted=False,
        authoritative_resolved=False,
        authoritative_message_id=None,
        authoritative_message_ids=[],
        error_text=error_text,
    )


def telethon_not_attempted(error_text: str | None = None) -> TelethonSendOutcome:
    return TelethonSendOutcome(
        transport_attempted=False,
        transport_accepted=False,
        authoritative_resolved=False,
        authoritative_message_id=None,
        authoritative_message_ids=[],
        error_text=error_text,
    )


class TelethonAuthoritativeMessageResolver:
    def __init__(self, telethon: Any):
        self.telethon = telethon

    async def get_before_max_message_id(self, target_entity: Any, target_id: Any) -> int | None:
        try:
            messages = await self.telethon.get_messages(target_entity, limit=1)
            message = (messages[0] if isinstance(messages, list) and messages else messages)
            return _message_id(message)
        except Exception as exc:
            logger.warning("TELETHON_TARGET_WATERMARK_FAILED | target_id=%s | error=%s", target_id, exc)
            return None

    async def validate_message_in_target(self, target_entity: Any, target_id: Any, candidate_id: int | None, expected_fp: dict[str, Any], source_message_ids: set[int] | None = None) -> tuple[bool, Any | None, str]:
        if not candidate_id or candidate_id <= 0:
            return False, None, "invalid_id"
        if source_message_ids and int(candidate_id) in source_message_ids:
            return False, None, "source_id_collision"
        try:
            message = await self.telethon.get_messages(target_entity, ids=int(candidate_id))
        except Exception as exc:
            return False, None, f"get_failed:{exc}"
        if not message or not _message_id(message):
            logger.warning("TELETHON_RETURNED_ID_REJECTED | target_id=%s | returned_candidate_id=%s | reason=not_found_in_target", target_id, candidate_id)
            return False, None, "not_found_in_target"
        if hasattr(message, "out") and getattr(message, "out") is not True:
            return False, message, "not_outbound"
        if hasattr(message, "post") and getattr(message, "post") is not True:
            return False, message, "not_channel_post"
        score = _fingerprint_score(expected_fp, build_media_fingerprint(message))
        if score < 0:
            return False, message, "fingerprint_mismatch"
        return True, message, "candidate_verified"

    def _in_window(self, message: Any, started_at: datetime, finished_at: datetime) -> bool:
        date = getattr(message, "date", None)
        if not isinstance(date, datetime):
            return True
        if date.tzinfo is None:
            date = date.replace(tzinfo=timezone.utc)
        return (started_at - timedelta(seconds=30)) <= date <= (finished_at + timedelta(seconds=120))

    async def resolve_authoritative_single_message(self, *, target_entity: Any, target_id: Any, sent: Any, expected_message: Any = None, expected_text: str = "", before_max_message_id: int | None, send_started_at: datetime, send_finished_at: datetime, source_message_ids: set[int] | None = None) -> TelethonResolutionResult:
        returned_candidate_id = _message_id(sent)
        expected_fp = build_media_fingerprint(sent or expected_message, text=expected_text)
        ok, message, reason = await self.validate_message_in_target(target_entity, target_id, returned_candidate_id, expected_fp, source_message_ids)
        if ok:
            return TelethonResolutionResult(True, _message_id(message), returned_candidate_id=returned_candidate_id, resolution_method="returned_candidate_verified")
        best: list[tuple[int, Any]] = []
        for delay in (0, 0.7, 1.5):
            if delay:
                await asyncio.sleep(delay)
            try:
                history = await self.telethon.get_messages(target_entity, limit=50)
            except Exception as exc:
                logger.warning("TELETHON_HISTORY_RESOLUTION_FAILED | target_id=%s | error=%s", target_id, exc)
                continue
            for item in (history or []):
                mid = _message_id(item)
                if not mid or (before_max_message_id is not None and mid <= before_max_message_id):
                    continue
                if source_message_ids and mid in source_message_ids:
                    continue
                if not self._in_window(item, send_started_at, send_finished_at):
                    continue
                if hasattr(item, "out") and getattr(item, "out") is not True:
                    continue
                if hasattr(item, "post") and getattr(item, "post") is not True:
                    continue
                score = _fingerprint_score(expected_fp, build_media_fingerprint(item))
                if score >= 0:
                    best.append((score, item))
            if best:
                break
        if best:
            max_score = max(score for score, _ in best)
            winners = [m for score, m in best if score == max_score]
            if len(winners) == 1:
                authoritative = _message_id(winners[0])
                logger.info("TELETHON_AUTHORITATIVE_TARGET_RESOLVED | target_id=%s | returned_candidate_id=%s | authoritative_message_id=%s | resolution_method=target_history_media_fingerprint", target_id, returned_candidate_id, authoritative)
                return TelethonResolutionResult(True, authoritative, returned_candidate_id=returned_candidate_id, resolution_method="target_history_media_fingerprint")
        logger.warning("TELETHON_SEND_ACCEPTED_TARGET_ID_UNRESOLVED | target_id=%s | returned_candidate_id=%s | action=no_second_send", target_id, returned_candidate_id)
        return TelethonResolutionResult(False, None, returned_candidate_id=returned_candidate_id, resolution_method="unresolved", error_text=reason)

    async def resolve_authoritative_album_messages(self, *, target_entity: Any, target_id: Any, sent_messages: list[Any], expected_messages: list[Any], expected_text: str, before_max_message_id: int | None, send_started_at: datetime, send_finished_at: datetime, source_message_ids: set[int] | None = None) -> TelethonResolutionResult:
        if not sent_messages or any(m is None for m in sent_messages):
            return TelethonResolutionResult(False, None, returned_candidate_ids=[], resolution_method="album_unresolved", error_text="album_sent_messages_empty")
        if len(sent_messages) != len(expected_messages):
            return TelethonResolutionResult(False, None, returned_candidate_ids=[i for i in (_message_id(m) for m in sent_messages) if i], resolution_method="album_unresolved", error_text="album_sent_count_mismatch")

        grouped_ids = {getattr(m, "grouped_id", None) for m in sent_messages if getattr(m, "grouped_id", None) is not None}
        if len(grouped_ids) > 1:
            return TelethonResolutionResult(False, None, returned_candidate_ids=[i for i in (_message_id(m) for m in sent_messages) if i], resolution_method="album_unresolved", error_text="album_grouped_id_mismatch")

        ids = [_message_id(m) for m in sent_messages]
        resolved = []
        for sent, expected in zip(sent_messages, expected_messages):
            r = await self.resolve_authoritative_single_message(target_entity=target_entity, target_id=target_id, sent=sent, expected_message=expected, expected_text=expected_text, before_max_message_id=before_max_message_id, send_started_at=send_started_at, send_finished_at=send_finished_at, source_message_ids=source_message_ids)
            if not r.ok or not r.authoritative_message_id:
                return TelethonResolutionResult(False, None, returned_candidate_ids=[i for i in ids if i], resolution_method="album_unresolved", error_text=r.error_text)
            resolved.append(int(r.authoritative_message_id))
        if len(resolved) != len(expected_messages) or len(set(resolved)) != len(resolved):
            return TelethonResolutionResult(False, None, returned_candidate_ids=[i for i in ids if i], resolution_method="album_unresolved", error_text="album_authoritative_ids_not_unique")
        return TelethonResolutionResult(True, resolved[0] if resolved else None, authoritative_message_ids=resolved, returned_candidate_ids=[i for i in ids if i], resolution_method="album_authoritative_verified")
