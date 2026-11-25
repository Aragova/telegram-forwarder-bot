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
    pending_mask: list[bool] | None = None


def _message_id(message: Any) -> int | None:
    try:
        value = int(getattr(message, "id", 0) or 0)
        return value if value > 0 else None
    except Exception:
        return None


def _message_text(message: Any) -> str:
    return str(getattr(message, "message", None) or getattr(message, "text", None) or "")


def _message_thread_matches(message: Any, target_thread_id: int | None) -> bool:
    if target_thread_id is None:
        return True
    reply_to = getattr(message, "reply_to", None)
    if reply_to is None:
        return False
    expected = int(target_thread_id)
    for attr_name in ("reply_to_top_id", "top_msg_id", "reply_to_msg_id"):
        value = getattr(reply_to, attr_name, None)
        if value is not None and int(value) == expected:
            return True
    return False


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
        document_id = getattr(document, "id", None)
        size = getattr(document, "size", None)
        mime_type = getattr(document, "mime_type", None)
        filename = _filename(document)
        if document_id is None and size is None and mime_type is None and filename is None:
            return {"kind": "text", "text": text if text is not None else _message_text(message)}
        return {
            "kind": "document",
            "document_id": document_id,
            "size": size,
            "mime_type": mime_type,
            "filename": filename,
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
    pending_mask: list[bool] | None = None

    @property
    def ok(self) -> bool:
        return bool(self.transport_accepted and self.authoritative_resolved and self.authoritative_message_id)

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
            **({"pending_mask": self.pending_mask} if self.pending_mask is not None else {}),
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

    async def validate_message_in_target(self, target_entity: Any, target_id: Any, candidate_id: int | None, expected_fp: dict[str, Any], source_message_ids: set[int] | None = None, target_thread_id: int | None = None, target_is_broadcast_channel: bool = False) -> tuple[bool, Any | None, str]:
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
        if target_is_broadcast_channel:
            if getattr(message, "post", None) is not True:
                return False, message, "not_channel_post"
        elif hasattr(message, "out") and getattr(message, "out") is not True:
            return False, message, "not_outbound"
        if not _message_thread_matches(message, target_thread_id):
            return False, message, "thread_mismatch"
        score = _fingerprint_score(expected_fp, build_media_fingerprint(message))
        if score < 0:
            return False, message, "fingerprint_mismatch"
        return True, message, "candidate_verified"

    def _in_window(self, message: Any, started_at: datetime, finished_at: datetime, *, before_seconds: int = 30, after_seconds: int = 120) -> bool:
        date = getattr(message, "date", None)
        if not isinstance(date, datetime):
            return True
        if date.tzinfo is None:
            date = date.replace(tzinfo=timezone.utc)
        return (started_at - timedelta(seconds=before_seconds)) <= date <= (finished_at + timedelta(seconds=after_seconds))

    def _history_candidate_rejection_reason(self, message: Any, *, expected_fp: dict[str, Any], before_max_message_id: int | None, send_started_at: datetime, send_finished_at: datetime, source_message_ids: set[int] | None, target_thread_id: int | None, target_is_broadcast_channel: bool = False) -> str | None:
        mid = _message_id(message)
        if not mid or (before_max_message_id is not None and mid <= before_max_message_id):
            return "before_watermark"
        if source_message_ids and mid in source_message_ids:
            return "source_collision"
        if not self._in_window(message, send_started_at, send_finished_at, before_seconds=5, after_seconds=30):
            return "time_window"
        if target_is_broadcast_channel:
            if getattr(message, "post", None) is not True:
                return "not_channel_post"
        elif hasattr(message, "out") and getattr(message, "out") is not True:
            return "outbound"
        if not _message_thread_matches(message, target_thread_id):
            return "thread"
        actual_fp = build_media_fingerprint(message)
        if expected_fp.get("kind") != actual_fp.get("kind"):
            return "media_kind"
        return None

    def _is_safe_history_candidate(self, message: Any, *, expected_fp: dict[str, Any], before_max_message_id: int | None, send_started_at: datetime, send_finished_at: datetime, source_message_ids: set[int] | None, target_thread_id: int | None, target_is_broadcast_channel: bool = False) -> bool:
        return self._history_candidate_rejection_reason(message, expected_fp=expected_fp, before_max_message_id=before_max_message_id, send_started_at=send_started_at, send_finished_at=send_finished_at, source_message_ids=source_message_ids, target_thread_id=target_thread_id, target_is_broadcast_channel=target_is_broadcast_channel) is None

    async def resolve_scheduled_publication(
        self, *, target_entity: Any, target_id: Any, scheduled_id: int | None,
        source_message_ids: set[int] | None, target_thread_id: int | None,
        before_max_message_id: int | None, send_started_at: datetime,
        target_is_broadcast_channel: bool,
    ) -> int | None:
        """Use only explicit Telegram mapping; never scan by similar caption."""
        if not scheduled_id:
            return None
        ids = await self.resolve_scheduled_publications(
            target_entity=target_entity,target_id=target_id,scheduled_ids=[scheduled_id],
            source_message_ids=source_message_ids,target_thread_id=target_thread_id,
            before_max_message_id=before_max_message_id,send_started_at=send_started_at,
            target_is_broadcast_channel=target_is_broadcast_channel,
        )
        return ids[0] if ids else None

    async def resolve_scheduled_publications(
        self, *, target_entity: Any, target_id: Any, scheduled_ids: list[int],
        source_message_ids: set[int] | None, target_thread_id: int | None,
        before_max_message_id: int | None, send_started_at: datetime,
        target_is_broadcast_channel: bool, pending_mask: list[bool] | None = None,
        publication_inbox: Any = None,
    ) -> list[int]:
        """Resolve the complete vector in its original order, never partial groups."""
        from .telegram_publication import publication_candidate_mask, publication_pairs, verified_published_message

        inbox = publication_inbox if publication_inbox is not None else getattr(self.telethon,"publication_inbox",None)
        mask = publication_candidate_mask(scheduled_ids,pending_mask)
        if inbox is None or not mask:
            return []
        pending_ids = [mid for mid,flag in zip(scheduled_ids,mask) if flag]
        try:
            from telethon import utils

            peer_id = int(target_id) if str(target_id).lstrip("-").isdigit() else utils.get_peer_id(target_entity)
            mapped = await inbox.lookup_many(peer_id, pending_ids)
            if not publication_pairs(pending_ids,mapped):
                return []
            mapped_iter = iter(mapped)
            ids = [next(mapped_iter) if flag else mid for mid,flag in zip(scheduled_ids,mask)]
            if not publication_pairs(ids,ids):
                return []
            if len(ids)==1:
                messages = [await self.telethon.get_messages(target_entity,ids=ids[0])]
            else:
                messages = await self.telethon.get_messages(target_entity,ids=ids)
            if not isinstance(messages,(list,tuple)) or len(messages)!=len(ids):
                return []
            by_id = {getattr(m,"id",None):m for m in messages if m is not None}
            if set(by_id)!=set(ids):
                return []
            if len(ids)>1:
                groups = {getattr(m,"grouped_id",None) for m in messages}
                if len(groups)!=1 or not next(iter(groups)):
                    return []
            if not all(verified_published_message(
                by_id[mid],peer_id=peer_id,published_id=mid,thread_id=target_thread_id,
                source_ids=source_message_ids,before_id=before_max_message_id,
                started_at=send_started_at,broadcast=target_is_broadcast_channel,
                require_scheduled=flag,
            ) for mid,flag in zip(ids,mask)):
                return []
            if await inbox.lookup_many(peer_id, pending_ids) != mapped:
                return []
            return ids
        except Exception as exc:
            logger.warning("TELEGRAM_PUBLICATION_VECTOR_LOOKUP_FAILED | target_id=%s | error_type=%s",
                           target_id,type(exc).__name__)
            return []

    async def resolve_authoritative_single_message(self, *, target_entity: Any, target_id: Any, sent: Any, expected_message: Any = None, expected_text: str = "", before_max_message_id: int | None, send_started_at: datetime, send_finished_at: datetime, source_message_ids: set[int] | None = None, target_thread_id: int | None = None, target_is_broadcast_channel: bool = False, require_strong_media_match: bool = False, history_poll_delays: tuple[float, ...] | None = None) -> TelethonResolutionResult:
        returned_candidate_id = _message_id(sent)
        if getattr(sent, "video_processing_pending", None) is True:
            final_id = await self.resolve_scheduled_publication(
                target_entity=target_entity, target_id=target_id,
                scheduled_id=returned_candidate_id, source_message_ids=source_message_ids,
                target_thread_id=target_thread_id, before_max_message_id=before_max_message_id,
                send_started_at=send_started_at, target_is_broadcast_channel=target_is_broadcast_channel,
            )
            if final_id is not None:
                return TelethonResolutionResult(
                    True, final_id, authoritative_message_ids=[final_id],
                    returned_candidate_id=returned_candidate_id,
                    resolution_method="scheduled_publication_mapping",
                )
            # Telegram allocates scheduled and published IDs in separate namespaces.
            # Never look up a scheduled ID in the published history as a candidate.
            logger.info(
                "TELEGRAM_VIDEO_PROCESSING_PENDING | target_id=%s | scheduled_id=%s | estimated_publication_at=%s | action=await_publication_mapping",
                target_id, returned_candidate_id, getattr(sent, "date", None),
            )
            return TelethonResolutionResult(
                False, None, returned_candidate_id=returned_candidate_id,
                returned_candidate_ids=[returned_candidate_id] if returned_candidate_id else [],
                resolution_method="video_processing_pending",
                error_text="telegram_video_processing_pending",
            )
        expected_fp = build_media_fingerprint(expected_message or sent, text=expected_text)
        ok, message, reason = await self.validate_message_in_target(target_entity, target_id, returned_candidate_id, expected_fp, source_message_ids, target_thread_id, target_is_broadcast_channel)
        if ok and require_strong_media_match:
            # Never accept an old message from the same target, even if media
            # or a repetitive caption happens to match.
            candidate_fp = build_media_fingerprint(message)
            score = _fingerprint_score(expected_fp, candidate_fp)
            threshold = 80 if expected_fp.get("kind") == "document" else 90 if expected_fp.get("kind") == "photo" else 101
            if score < threshold:
                ok = False
                reason = "weak_media_fingerprint"
            elif (before_max_message_id is not None and returned_candidate_id <= before_max_message_id) or not self._in_window(message, send_started_at, send_finished_at, before_seconds=10, after_seconds=45):
                ok = False
                reason = "stale_or_pre_send_candidate"
            elif getattr(message, "chat_id", None) is not None and str(getattr(message, "chat_id")) != str(target_id):
                ok = False
                reason = "wrong_peer"
        if ok:
            return TelethonResolutionResult(True, _message_id(message), returned_candidate_id=returned_candidate_id, resolution_method="returned_candidate_verified")
        best: list[tuple[int, Any]] = []
        safe_history_candidates: list[Any] = []
        scanned_history: list[Any] = []
        rejection_counts = {
            "before_watermark": 0,
            "source_collision": 0,
            "time_window": 0,
            "outbound": 0,
            "not_channel_post": 0,
            "thread": 0,
            "media_kind": 0,
        }
        candidate_diagnostics: list[dict[str, Any]] = []
        for delay in (history_poll_delays if history_poll_delays is not None else (0, 0.7, 1.5)):
            if delay:
                await asyncio.sleep(delay)
            try:
                history = await self.telethon.get_messages(target_entity, limit=50)
            except Exception as exc:
                logger.warning("TELETHON_HISTORY_RESOLUTION_FAILED | target_id=%s | error=%s", target_id, exc)
                continue
            scanned_history = list(history or [])
            safe_history_candidates = []
            best = []
            rejection_counts = dict.fromkeys(rejection_counts, 0)
            candidate_diagnostics = []
            for item in scanned_history:
                rejection_reason = self._history_candidate_rejection_reason(item, expected_fp=expected_fp, before_max_message_id=before_max_message_id, send_started_at=send_started_at, send_finished_at=send_finished_at, source_message_ids=source_message_ids, target_thread_id=target_thread_id, target_is_broadcast_channel=target_is_broadcast_channel)
                if rejection_reason is not None:
                    rejection_counts[rejection_reason] = rejection_counts.get(rejection_reason, 0) + 1
                mid = _message_id(item)
                if mid and (before_max_message_id is None or mid > before_max_message_id):
                    candidate_diagnostics.append({
                        "candidate_id": mid,
                        "date": getattr(item, "date", None),
                        "out": getattr(item, "out", None),
                        "post": getattr(item, "post", None),
                        "media_kind": build_media_fingerprint(item).get("kind"),
                        "thread_id": getattr(getattr(item, "reply_to", None), "reply_to_top_id", None) or getattr(getattr(item, "reply_to", None), "top_msg_id", None) or getattr(getattr(item, "reply_to", None), "reply_to_msg_id", None),
                        "rejection_reason": rejection_reason,
                    })
                if rejection_reason is not None:
                    continue
                safe_history_candidates.append(item)
                score = _fingerprint_score(expected_fp, build_media_fingerprint(item))
                if require_strong_media_match:
                    required_score = 80 if expected_fp.get("kind") == "document" else 90 if expected_fp.get("kind") == "photo" else 101
                    matched = score >= required_score
                else:
                    matched = score >= 0
                if matched:
                    best.append((score, item))
            if best or (safe_history_candidates and not require_strong_media_match):
                break
        strong_match_ids: list[int] = []
        if best:
            max_score = max(score for score, _ in best)
            winners = [m for score, m in best if score == max_score]
            strong_match_ids = [mid for mid in (_message_id(m) for m in winners) if mid]
        safe_candidate_ids = [mid for mid in (_message_id(m) for m in safe_history_candidates) if mid]
        logger.info("TELETHON_HISTORY_RESOLUTION_SCAN | target_id=%s | before_max_message_id=%s | returned_candidate_id=%s | history_count=%s | safe_candidate_ids=%s | strong_match_ids=%s | rejection_counts=%s | candidate_diagnostics=%s", target_id, before_max_message_id, returned_candidate_id, len(scanned_history), safe_candidate_ids, strong_match_ids, rejection_counts, candidate_diagnostics[:10])
        if best:
            max_score = max(score for score, _ in best)
            winners = [m for score, m in best if score == max_score]
            if len(winners) == 1:
                authoritative = _message_id(winners[0])
                logger.info("TELETHON_AUTHORITATIVE_TARGET_RESOLVED | target_id=%s | returned_candidate_id=%s | authoritative_message_id=%s | resolution_method=target_history_media_fingerprint", target_id, returned_candidate_id, authoritative)
                return TelethonResolutionResult(True, authoritative, returned_candidate_id=returned_candidate_id, resolution_method="target_history_media_fingerprint")
        if len(safe_history_candidates) == 1 and not require_strong_media_match:
            authoritative = _message_id(safe_history_candidates[0])
            logger.info("TELETHON_AUTHORITATIVE_TARGET_RESOLVED | target_id=%s | returned_candidate_id=%s | authoritative_message_id=%s | resolution_method=target_history_unique_new_outbound", target_id, returned_candidate_id, authoritative)
            return TelethonResolutionResult(True, authoritative, returned_candidate_id=returned_candidate_id, resolution_method="target_history_unique_new_outbound")
        if len(safe_history_candidates) > 1:
            logger.warning("TELETHON_HISTORY_RESOLUTION_AMBIGUOUS | safe_candidate_ids=%s | action=no_guess_no_second_send", safe_candidate_ids)
        logger.warning("TELETHON_SEND_ACCEPTED_TARGET_ID_UNRESOLVED | target_id=%s | returned_candidate_id=%s | action=no_second_send", target_id, returned_candidate_id)
        return TelethonResolutionResult(False, None, returned_candidate_id=returned_candidate_id, resolution_method="unresolved", error_text=reason)

    async def resolve_authoritative_album_messages(self, *, target_entity: Any, target_id: Any, sent_messages: list[Any], expected_messages: list[Any], expected_text: str, before_max_message_id: int | None, send_started_at: datetime, send_finished_at: datetime, source_message_ids: set[int] | None = None, target_thread_id: int | None = None, target_is_broadcast_channel: bool = False) -> TelethonResolutionResult:
        if not sent_messages or any(m is None for m in sent_messages):
            return TelethonResolutionResult(False, None, returned_candidate_ids=[], resolution_method="album_unresolved", error_text="album_sent_messages_empty")
        if len(sent_messages) != len(expected_messages):
            return TelethonResolutionResult(False, None, returned_candidate_ids=[i for i in (_message_id(m) for m in sent_messages) if i], resolution_method="album_unresolved", error_text="album_sent_count_mismatch")

        pending_mask = [getattr(m,"video_processing_pending",None) is True for m in sent_messages]
        pending_ids = [_message_id(m) for m,flag in zip(sent_messages,pending_mask) if flag]
        if any(pending_mask):
            raw_ids = [_message_id(m) for m in sent_messages]
            final_ids = await self.resolve_scheduled_publications(
                target_entity=target_entity,target_id=target_id,scheduled_ids=raw_ids,
                source_message_ids=source_message_ids,target_thread_id=target_thread_id,
                before_max_message_id=before_max_message_id,send_started_at=send_started_at,
                target_is_broadcast_channel=target_is_broadcast_channel,pending_mask=pending_mask,
            ) if all(type(mid) is int and mid > 0 for mid in raw_ids) else []
            if final_ids:
                return TelethonResolutionResult(
                    True,final_ids[0],authoritative_message_ids=final_ids,
                    returned_candidate_ids=raw_ids,resolution_method="scheduled_publication_mapping",
                )
            logger.info(
                "TELEGRAM_ALBUM_PROCESSING_PENDING | target_id=%s | scheduled_ids=%s | returned_ids=%s | action=await_publication_mapping",
                target_id, pending_ids, raw_ids,
            )
            return TelethonResolutionResult(
                False, None, returned_candidate_ids=raw_ids,
                pending_mask=pending_mask,
                resolution_method="video_processing_pending",
                error_text="telegram_video_processing_pending",
            )

        grouped_ids = {getattr(m, "grouped_id", None) for m in sent_messages if getattr(m, "grouped_id", None) is not None}
        if len(grouped_ids) > 1:
            return TelethonResolutionResult(False, None, returned_candidate_ids=[i for i in (_message_id(m) for m in sent_messages) if i], resolution_method="album_unresolved", error_text="album_grouped_id_mismatch")

        ids = [_message_id(m) for m in sent_messages]
        resolved = []
        for sent, expected in zip(sent_messages, expected_messages):
            r = await self.resolve_authoritative_single_message(target_entity=target_entity, target_id=target_id, sent=sent, expected_message=expected, expected_text=expected_text, before_max_message_id=before_max_message_id, send_started_at=send_started_at, send_finished_at=send_finished_at, source_message_ids=source_message_ids, target_thread_id=target_thread_id, target_is_broadcast_channel=target_is_broadcast_channel)
            if not r.ok or not r.authoritative_message_id:
                return TelethonResolutionResult(False, None, returned_candidate_ids=[i for i in ids if i], resolution_method="album_unresolved", error_text=r.error_text)
            resolved.append(int(r.authoritative_message_id))
        if len(resolved) != len(expected_messages) or len(set(resolved)) != len(resolved):
            return TelethonResolutionResult(False, None, returned_candidate_ids=[i for i in ids if i], resolution_method="album_unresolved", error_text="album_authoritative_ids_not_unique")
        return TelethonResolutionResult(True, resolved[0] if resolved else None, authoritative_message_ids=resolved, returned_candidate_ids=[i for i in ids if i], resolution_method="album_authoritative_verified")
