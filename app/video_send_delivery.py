from __future__ import annotations

import logging
from pathlib import Path

from .delivery_idempotency import (
    build_delivery_idempotency_key,
    extract_sent_message_ids_from_attempt,
    normalize_valid_sent_message_ids,
)
from .runtime_utils import run_db

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
            if isinstance(attempt, dict) and str(attempt.get("status") or "") in {"accepted", "verified"} and cached_sent_ids:
                logger.info(
                    "DELIVERY_ATTEMPT_CACHE_HIT | operation_kind=video_send | delivery_id=%s | rule_id=%s | target_id=%s | idempotency_key=%s | sent_message_ids=%s",
                    delivery_id,
                    rule_id,
                    target_id,
                    idempotency_key,
                    cached_sent_ids,
                )
                await run_db(
                    owner._mark_delivery_sent_sync,
                    int(delivery_id),
                    sent_message_id=int(cached_sent_ids[0]),
                    sent_message_ids=cached_sent_ids,
                    target_id=str(target_id),
                    delivery_method="idempotency_cache",
                )
                logger.info(
                    "DELIVERY_MARKED_SENT_FROM_CACHE_HIT | delivery_id=%s | rule_id=%s | sent_message_ids=%s",
                    delivery_id,
                    rule_id,
                    cached_sent_ids,
                )
                return {
                    "ok": True,
                    "sent_message_ids": cached_sent_ids,
                    "sent_message_id": int(cached_sent_ids[0]),
                    "idempotency_key": idempotency_key,
                    "cache_hit": True,
                    "fallback_to_legacy": False,
                }
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
            await run_db(owner.db.mark_delivery_attempt_sending, idempotency_key, job_id=None, telegram_method="video_send")
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
        if not sent_msg:
            logger.warning("VIDEO STAGE RETRY | неуспешная отправка для delivery_id=%s", delivery_id)
            if has_attempt_ledger:
                await run_db(
                    owner.db.mark_delivery_attempt_failed,
                    idempotency_key,
                    status="failed_before_send",
                    error_text="send_with_retry returned empty result",
                )
                logger.info(
                    "DELIVERY_ATTEMPT_FAILED_BEFORE_SEND | operation_kind=video_send | delivery_id=%s | rule_id=%s | target_id=%s | idempotency_key=%s",
                    delivery_id,
                    rule_id,
                    target_id,
                    idempotency_key,
                )
            return {"ok": False, "fallback_to_legacy": False, "retryable": True}

        sent_message_ids = owner._extract_sent_message_ids(sent_msg)
        valid_sent_ids = normalize_valid_sent_message_ids(sent_message_ids)
        if has_attempt_ledger and valid_sent_ids:
            await run_db(owner.db.mark_delivery_attempt_accepted, idempotency_key, sent_message_ids=valid_sent_ids, telegram_method="video_send")
            logger.info(
                "DELIVERY_ATTEMPT_ACCEPTED | operation_kind=video_send | delivery_id=%s | rule_id=%s | target_id=%s | idempotency_key=%s | sent_message_ids=%s",
                delivery_id,
                rule_id,
                target_id,
                idempotency_key,
                valid_sent_ids,
            )
        elif has_attempt_ledger:
            logger.warning(
                "DELIVERY_ATTEMPT_ACCEPTED_SKIPPED_INVALID_IDS | operation_kind=%s | idempotency_key=%s | delivery_id=%s | raw_sent_message_ids=%s",
                "video_send",
                idempotency_key,
                delivery_id,
                sent_message_ids,
            )
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
        if sent_message_id:
            await owner._run_post_send_step_safe(
                step_name="reaction_after_video_send",
                rule_id=rule_id,
                delivery_id=delivery_id,
                idempotency_key=idempotency_key,
                accepted_sent_message_ids=valid_sent_ids,
                coro_factory=lambda: owner._add_reaction_if_possible(payload.get("target_id"), int(sent_message_id), rule_id=rule_id),
            )
        elif valid_sent_ids:
            sent_message_id = int(valid_sent_ids[0])
            logger.warning("DELIVERY_SENT_UNVERIFIED_AFTER_ACCEPTED | rule_id=%s | delivery_id=%s | method=%s | target_id=%s | sent_message_ids=%s", rule_id, delivery_id, "video_send", payload.get("target_id"), valid_sent_ids)
        else:
            logger.warning("DELIVERY_FALSE_SUCCESS_PREVENTED | rule_id=%s | delivery_id=%s | method=%s | target_id=%s | candidate_sent_message_ids=%s | action=retry_or_faulty", rule_id, delivery_id, "video_send", payload.get("target_id"), sent_message_ids)
            return {"ok": False, "fallback_to_legacy": False, "retryable": True}
        await run_db(owner._mark_delivery_sent_sync, delivery_id, sent_message_id=sent_message_id, sent_message_ids=(valid_sent_message_ids or valid_sent_ids), target_id=str(payload.get("target_id") or ""), delivery_method="video_send")
        await run_db(owner._touch_rule_after_send_sync, rule_id, int(payload.get("interval") or 0))
        logger.info("VIDEO SEND DONE | отправка завершена для delivery_id=%s", delivery_id)
        return {"ok": True, "fallback_to_legacy": False}

