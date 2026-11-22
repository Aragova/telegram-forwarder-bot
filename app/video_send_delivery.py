from __future__ import annotations

import logging
from pathlib import Path

from aiogram.exceptions import TelegramEntityTooLarge

from .delivery_idempotency import (
    build_delivery_idempotency_key,
    extract_sent_message_ids_from_attempt,
    normalize_valid_sent_message_ids,
)
from .runtime_utils import run_db
from .telethon_authoritative_resolver import TelethonSendOutcome

logger = logging.getLogger("forwarder")


class VideoSendDelivery:
    def __init__(self, owner):
        self.owner = owner

    async def execute_from_processed_job(
        self,
        *,
        processed_video_path: str | None = None,
        thumbnail_path: str | None = None,
        artifact_version: int | None = None,
        pipeline_version: int | None = None,
        **payload: object,
    ) -> dict:
        owner = self.owner
        delivery_id = int(payload.get("delivery_id") or 0)
        rule_id = int(payload.get("rule_id") or 0)
        target_id = str(payload.get("target_id") or "")
        tenant_id = int(payload.get("tenant_id") or 1)
        source_message_id = int(payload.get("message_id")) if payload.get("message_id") else None
        source_message_ids = [int(payload.get("message_id"))] if payload.get("message_id") else []
        idempotency_key = build_delivery_idempotency_key(
            operation_kind="video_send",
            delivery_id=int(delivery_id),
            target_id=target_id,
        )
        has_attempt_ledger = all(
            hasattr(owner.db, method_name)
            for method_name in (
                "get_delivery_attempt_by_idempotency_key",
                "create_delivery_attempt",
                "mark_delivery_attempt_sending",
                "mark_delivery_attempt_accepted",
                "mark_delivery_attempt_failed",
            )
        )
        logger.info("VIDEO SEND START | старт отправки для delivery_id=%s | rule_id=%s | stage=send", delivery_id, rule_id)
        if has_attempt_ledger:
            attempt = await run_db(owner.db.get_delivery_attempt_by_idempotency_key, idempotency_key)
            cached_sent_ids = extract_sent_message_ids_from_attempt(attempt)
            # Once a send may have reached Telegram, retries are read-only.
            # Even an 'accepted' cache entry is NOT proof that its id belongs
            # to this channel (e.g. rule 141 returned 370 instead of 2183).
            status = str((attempt or {}).get("status") or "").lower()
            if status in {"sending", "accepted", "verified", "failed_after_send"}:
                verified_ids = []
                if cached_sent_ids:
                    verified_ids = await owner._confirm_target_delivery_message_ids_with_retry(
                        rule_id=rule_id,
                        delivery_id=delivery_id,
                        source_channel=str(payload.get("source_channel") or ""),
                        target_id=target_id,
                        source_message_ids=source_message_ids,
                        candidate_sent_message_ids=cached_sent_ids,
                        method="video_send_cache_verify",
                        max_age_seconds=900 if status == "sending" else 86400 * 30,
                    )
                if verified_ids:
                    if hasattr(owner.db, "mark_delivery_attempt_verified"):
                        await run_db(owner.db.mark_delivery_attempt_verified, idempotency_key, sent_message_ids=verified_ids)
                    rule = await run_db(owner.db.get_rule, rule_id)
                    if rule:
                        await owner._run_post_send_step_safe(
                            step_name="reaction_after_video_cache_verify",
                            rule_id=rule_id,
                            delivery_id=delivery_id,
                            idempotency_key=idempotency_key,
                            accepted_sent_message_ids=verified_ids,
                            coro_factory=lambda: owner._add_reaction_for_rule_if_possible(
                                rule=rule, target_id=target_id, sent_message_id=int(verified_ids[0]),
                                source_channel=str(payload.get("source_channel") or ""),
                                source_message_ids=source_message_ids, delivery_id=delivery_id,
                            ),
                        )
                    await run_db(owner._mark_delivery_sent_sync, delivery_id, sent_message_id=int(verified_ids[0]), sent_message_ids=verified_ids, target_id=target_id, delivery_method="video_cache_verified")
                    return {"ok": True, "cache_hit": True, "sent_message_ids": verified_ids, "fallback_to_legacy": False}
                error = "video_send_accepted_target_id_unresolved_non_retryable"
                logger.warning("VIDEO_SEND_CACHE_UNRESOLVED | rule_id=%s | delivery_id=%s | status=%s | candidate_ids=%s | action=no_resend_manual_review", rule_id, delivery_id, status, cached_sent_ids)
                await run_db(owner._mark_delivery_faulty_sync, delivery_id, error)
                return {"ok": False, "retryable": False, "accepted": True, "error_text": error, "manual_review_required": True, "fallback_to_legacy": False}
            await run_db(
                owner.db.create_delivery_attempt,
                delivery_id=int(delivery_id),
                rule_id=int(rule_id),
                tenant_id=int(tenant_id),
                job_id=None,
                idempotency_key=idempotency_key,
                operation_kind="video_send",
                status="created",
                telegram_method=None,
                target_id=target_id,
                source_message_ids=[source_message_id] if source_message_id else None,
                sent_message_ids=None,
                error_text=None,
            )
            logger.info(
                "DELIVERY_ATTEMPT_CREATED | operation_kind=video_send | delivery_id=%s | rule_id=%s | target_id=%s | idempotency_key=%s",
                delivery_id,
                rule_id,
                target_id,
                idempotency_key,
            )
            acquired = await run_db(owner.db.mark_delivery_attempt_sending, idempotency_key, job_id=None, telegram_method="video_send")
            if not acquired:
                # A concurrent executor or a prior attempt owns this write.
                return {"ok": False, "retryable": False, "accepted": True, "error_text": "video_send_attempt_not_acquired", "fallback_to_legacy": False}
            logger.info(
                "DELIVERY_ATTEMPT_SENDING | operation_kind=video_send | delivery_id=%s | rule_id=%s | target_id=%s | idempotency_key=%s",
                delivery_id,
                rule_id,
                target_id,
                idempotency_key,
            )
        if int(artifact_version or 1) != 1 or int(pipeline_version or 1) != 1:
            logger.warning(
                "VIDEO FALLBACK TO LEGACY | неподдерживаемая версия контракта artifact=%s pipeline=%s | delivery_id=%s",
                artifact_version,
                pipeline_version,
                delivery_id,
            )
            return {"ok": False, "fallback_to_legacy": True, "retryable": False}
        if not processed_video_path:
            logger.warning("VIDEO STAGE FAILED | отсутствует processed_file_path для delivery_id=%s", delivery_id)
            return {"ok": False, "fallback_to_legacy": True, "retryable": False}
        if not Path(processed_video_path).is_file():
            logger.warning("VIDEO STAGE FAILED | обработанный файл не найден для delivery_id=%s", delivery_id)
            return {"ok": False, "fallback_to_legacy": True, "retryable": False}

        rule = await run_db(owner.db.get_rule, rule_id)
        if not rule:
            logger.warning("VIDEO FALLBACK TO LEGACY | правило не найдено для send | delivery_id=%s", delivery_id)
            return {"ok": False, "fallback_to_legacy": True, "retryable": False}

        caption_payload = owner._build_video_caption_delivery_payload(rule)
        video_info = await owner.video_processor.get_video_info(str(processed_video_path), use_cache=False)
        if not video_info:
            return {"ok": False, "fallback_to_legacy": True, "retryable": False}

        try:
            sent_msg = await owner.video_processor.send_with_retry(
                owner.bot,
                payload.get("target_id"),
                payload.get("target_thread_id"),
                str(processed_video_path),
                str(thumbnail_path) if thumbnail_path else None,
                caption_payload["caption"] or "",
                video_info["duration"],
                caption_entities_json=caption_payload["caption_entities_json"],
                caption_send_mode=caption_payload["selected_mode"],
            )
        except TelegramEntityTooLarge as exc:
            error_text = f"TelegramEntityTooLarge: {exc}"
            logger.error(
                "VIDEO_SEND_SIZE_REJECTED | delivery_id=%s | rule_id=%s | file_size_bytes=%s | retryable=False",
                delivery_id,
                rule_id,
                Path(processed_video_path).stat().st_size,
            )
            if has_attempt_ledger:
                await run_db(
                    owner.db.mark_delivery_attempt_failed,
                    idempotency_key,
                    status="failed_before_send",
                    error_text=error_text,
                )
            return {"ok": False, "fallback_to_legacy": False, "retryable": False, "error_text": error_text}
        if isinstance(sent_msg, TelethonSendOutcome):
            # Unknown result / wrong peer: do not guess the target message id,
            # call reaction handlers, or permit another Telegram send.
            if not sent_msg.authoritative_resolved:
                candidate_ids = sent_msg.returned_candidate_ids or []
                if has_attempt_ledger:
                    await run_db(owner.db.mark_delivery_attempt_accepted, idempotency_key, sent_message_ids=candidate_ids, telegram_method="video_send_unresolved")
                error = "video_send_accepted_target_id_unresolved_non_retryable"
                logger.warning("VIDEO_SEND_ACCEPTED_UNRESOLVED | rule_id=%s | delivery_id=%s | candidate_ids=%s | reason=%s | action=no_resend_manual_review", rule_id, delivery_id, candidate_ids, sent_msg.error_text)
                await run_db(owner._mark_delivery_faulty_sync, delivery_id, error)
                return {"ok": False, "retryable": False, "accepted": True, "transport_accepted": True, "authoritative_resolved": False, "error_text": error, "manual_review_required": True, "fallback_to_legacy": False}
            sent_message_ids = list(sent_msg.authoritative_message_ids)
        elif sent_msg:
            sent_message_ids = owner._extract_sent_message_ids(sent_msg)
        else:
            # An empty response is not proof of rejection.
            if has_attempt_ledger:
                await run_db(owner.db.mark_delivery_attempt_accepted, idempotency_key, sent_message_ids=[], telegram_method="video_send_unknown")
            error = "video_send_response_missing_non_retryable"
            await run_db(owner._mark_delivery_faulty_sync, delivery_id, error)
            return {"ok": False, "retryable": False, "accepted": True, "error_text": error, "manual_review_required": True, "fallback_to_legacy": False}

        valid_sent_ids = normalize_valid_sent_message_ids(sent_message_ids)
        if has_attempt_ledger:
            await run_db(owner.db.mark_delivery_attempt_accepted, idempotency_key, sent_message_ids=valid_sent_ids, telegram_method="video_send")
        if not valid_sent_ids:
            error = "video_send_accepted_without_ids_non_retryable"
            await run_db(owner._mark_delivery_faulty_sync, delivery_id, error)
            return {"ok": False, "retryable": False, "accepted": True, "error_text": error, "fallback_to_legacy": False}
        confirm_result = await owner._run_post_send_step_safe(
            step_name="verify_after_video_send",
            rule_id=rule_id,
            delivery_id=delivery_id,
            idempotency_key=idempotency_key,
            accepted_sent_message_ids=valid_sent_ids,
            coro_factory=lambda: owner._confirm_target_delivery_message_ids_with_retry(
                rule_id=rule_id,
                delivery_id=delivery_id,
                source_channel=str(payload.get("source_channel") or ""),
                target_id=str(payload.get("target_id") or ""),
                source_message_ids=source_message_ids,
                candidate_sent_message_ids=sent_message_ids,
                method="video_send",
                max_age_seconds=900,
            ),
        )
        valid_sent_message_ids = confirm_result.get("result") or []
        sent_message_id = valid_sent_message_ids[0] if valid_sent_message_ids else None
        logger.info("DELIVERY_SENT_MESSAGE_IDS_EXTRACTED | rule_id=%s | delivery_id=%s | method=%s | source_message_ids=%s | sent_message_ids=%s | result_type=%s", rule_id, delivery_id, "video_send", source_message_ids, sent_message_ids, type(sent_msg).__name__)
        if not sent_message_id:
            error = "video_send_accepted_target_id_unresolved_non_retryable"
            logger.warning("VIDEO_SEND_VERIFY_UNRESOLVED | rule_id=%s | delivery_id=%s | candidate_ids=%s | action=no_resend_manual_review", rule_id, delivery_id, valid_sent_ids)
            await run_db(owner._mark_delivery_faulty_sync, delivery_id, error)
            return {"ok": False, "retryable": False, "accepted": True, "error_text": error, "manual_review_required": True, "fallback_to_legacy": False}
        if has_attempt_ledger and hasattr(owner.db, "mark_delivery_attempt_verified"):
            await run_db(owner.db.mark_delivery_attempt_verified, idempotency_key, sent_message_ids=valid_sent_message_ids)
        await owner._run_post_send_step_safe(
            step_name="reaction_after_video_send",
            rule_id=rule_id,
            delivery_id=delivery_id,
            idempotency_key=idempotency_key,
            accepted_sent_message_ids=valid_sent_message_ids,
            coro_factory=lambda: owner._add_reaction_for_rule_if_possible(
                rule=rule, target_id=target_id, sent_message_id=int(sent_message_id),
                source_channel=str(payload.get("source_channel") or ""),
                source_message_ids=source_message_ids, delivery_id=delivery_id,
            ),
        )
        await run_db(owner._mark_delivery_sent_sync, delivery_id, sent_message_id=sent_message_id, sent_message_ids=(valid_sent_message_ids or valid_sent_ids), target_id=str(payload.get("target_id") or ""), delivery_method="video_send")
        await run_db(owner._touch_rule_after_send_sync, rule_id, int(payload.get("interval") or 0))
        logger.info("VIDEO SEND DONE | отправка завершена для delivery_id=%s", delivery_id)
        return {"ok": True, "fallback_to_legacy": False}

