from __future__ import annotations

import logging

from .runtime_utils import run_db
from .sender_primitives import DEBUG_FORCE_DISABLE_BOTAPI_FALLBACK, _prepare_html_text
from .telegram_send_result import telegram_send_result_from_raw

logger = logging.getLogger("forwarder")


def _reaction_applied_or_enqueued(result) -> bool:
    payload = result.get("result") if isinstance(result, dict) and isinstance(result.get("result"), dict) else result
    if isinstance(payload, dict):
        return bool(payload.get("applied") or payload.get("enqueued"))
    return bool(payload)


class RepostSingleDelivery:
    def __init__(self, owner):
        self.owner = owner

    async def deliver(self, rule, delivery_id, message_id, source_channel, target_id, target_thread_id, idempotency_key: str | None = None):
        owner = self.owner
        post_id = await run_db(owner._get_post_id_by_delivery_sync, delivery_id)
        delivery_ids = [int(delivery_id)]
        source_message_ids = [int(message_id)]

        strategy = await run_db(
            owner._resolve_repost_caption_delivery_strategy_sync,
            rule=rule,
            source_channel=source_channel,
            message_ids=source_message_ids,
            is_album=False,
        )

        caption_mode = strategy["configured_mode"]
        requires_builder = strategy["requires_builder"]
        use_copy_first = strategy["use_copy_first"]

        is_self_loop = owner._is_self_loop_rule(rule)
        if is_self_loop:
            logger.info(
                "SELF_LOOP_REPOST_MODE | single | rule_id=%s | delivery_id=%s | source_channel=%s | target_id=%s | source_message_id=%s | action=real_repost",
                rule.id,
                delivery_id,
                source_channel,
                target_id,
                message_id,
            )

        live_message_for_builder = None
        if use_copy_first and caption_mode == "auto":
            live_message = await owner._fetch_message(source_channel, message_id)
            if live_message:
                live_content = owner._content_from_message_or_post(
                    message=live_message,
                    post_row=None,
                )
                if owner._content_requires_builder(live_content):
                    requires_builder = True
                    use_copy_first = False
                    live_message_for_builder = live_message

                    logger.warning(
                        "SINGLE_LIVE_ENTITY_GUARD_BUILDER_REQUIRED | rule_id=%s | delivery_id=%s | message_id=%s | reason=live_entities_require_builder",
                        rule.id,
                        delivery_id,
                        message_id,
                    )

                    await run_db(
                        owner.db.log_delivery_event,
                        event_type="single_live_entity_guard_builder_required",
                        delivery_id=delivery_id,
                        rule_id=rule.id,
                        post_id=post_id,
                        status="processing",
                        extra={
                            "message_id": message_id,
                            "source_channel": source_channel,
                            "target_id": target_id,
                            "caption_delivery_mode": caption_mode,
                            "reason": "live_entities_require_builder",
                        },
                    )

        await run_db(
            owner.db.log_delivery_event,
            event_type="delivery_caption_mode_selected",
            delivery_id=delivery_id,
            rule_id=rule.id,
            post_id=post_id,
            status="processing",
            extra={
                "caption_delivery_mode": caption_mode,
                "requires_builder": requires_builder,
                "selected_path": "copy_first" if use_copy_first else "builder_first",
                "message_id": message_id,
                "source_channel": source_channel,
                "target_id": target_id,
            },
        )

        logger.info(
            "CAPTION_MODE | single | rule=%s | delivery=%s | mode=%s | requires_builder=%s | selected_path=%s",
            rule.id,
            delivery_id,
            caption_mode,
            requires_builder,
            "copy_first" if use_copy_first else "builder_first",
        )

        post_row = await run_db(
            owner._get_post_row_for_rule_message_sync,
            rule,
            source_channel,
            message_id,
        )

        # =========================================================
        # 1) COPY SINGLE
        # Выполняем только если текущий режим разрешает copy-first
        # =========================================================
        if use_copy_first:
            await owner._log_delivery_pipeline_step(
                rule_id=rule.id,
                delivery_ids=delivery_ids,
                event_type="delivery_pipeline_step",
                pipeline_stage="copy_single",
                pipeline_result="started",
                source_channel=source_channel,
                target_id=target_id,
                source_message_ids=source_message_ids,
                extra={
                    "attempt_no": 1,
                    "caption_delivery_mode": caption_mode,
                    "requires_builder": requires_builder,
                },
            )

            copy_result = await owner._copy_single_via_bot(
                source_channel,
                target_id,
                message_id,
                target_thread_id,
            )
            send_result = telegram_send_result_from_raw(
                copy_result.get("raw_result"),
                method="copy_single",
                fallback_sent_ids=copy_result.get("sent_ids") or [],
                error_text=copy_result.get("error_text"),
                attempted=bool(copy_result.get("attempted", True)),
            )
            copy_sent_ids = send_result.sent_message_ids
            valid_sent_ids = send_result.sent_message_ids
            log_fn = logger.info if send_result.ok else logger.warning
            log_fn(
                "TELEGRAM_SEND_RESULT | method=%s | ok=%s | sent_message_ids=%s | sent_message_id=%s | raw_result_type=%s | error_text=%s | retryable=%s",
                send_result.method,
                send_result.ok,
                send_result.sent_message_ids,
                send_result.sent_message_id,
                send_result.raw_result_type,
                send_result.error_text,
                send_result.retryable,
            )
            if idempotency_key and valid_sent_ids:
                await run_db(owner.db.mark_delivery_attempt_accepted, idempotency_key, sent_message_ids=valid_sent_ids, telegram_method="copy_single")
                logger.info("DELIVERY_ATTEMPT_ACCEPTED | operation=single | key=%s | delivery_id=%s | sent_message_ids=%s", idempotency_key, delivery_id, valid_sent_ids)
            elif idempotency_key:
                logger.warning("DELIVERY_ATTEMPT_ACCEPTED_SKIPPED_INVALID_IDS | operation=%s | key=%s | delivery_id=%s | raw_sent_message_ids=%s", "single", idempotency_key, delivery_id, copy_sent_ids)
            logger.info("COPY_SINGLE_RESULT_RAW_TYPE | rule_id=%s | delivery_id=%s | raw_type=%s", rule.id, delivery_id, copy_result.get("raw_result_type"))
            logger.info("COPY_SINGLE_EXTRACTED_SENT_IDS | rule_id=%s | delivery_id=%s | sent_ids=%s", rule.id, delivery_id, copy_sent_ids)
            valid_copy_sent_ids: list[int] = []
            post_send_warnings: list[str] = []
            if copy_sent_ids:
                logger.info("COPY_SINGLE_TARGET_CONFIRM_START | rule_id=%s | delivery_id=%s | target_id=%s | source_message_ids=%s | candidate_sent_message_ids=%s", rule.id, delivery_id, target_id, source_message_ids, copy_sent_ids)
                confirm_result = await owner._run_post_send_step_safe(
                    step_name="verify_after_copy_single",
                    rule_id=rule.id,
                    delivery_id=delivery_id,
                    idempotency_key=idempotency_key,
                    accepted_sent_message_ids=valid_sent_ids,
                    coro_factory=lambda: owner._confirm_target_delivery_message_ids_with_retry(
                        rule_id=rule.id,
                        delivery_id=delivery_id,
                        source_channel=str(source_channel or ""),
                        target_id=str(target_id),
                        source_message_ids=source_message_ids,
                        candidate_sent_message_ids=copy_sent_ids,
                        method="copy_single",
                    ),
                )
                valid_copy_sent_ids = confirm_result.get("result") or []
                if valid_copy_sent_ids:
                    logger.info("COPY_SINGLE_TARGET_CONFIRM_OK | rule_id=%s | delivery_id=%s | valid_sent_message_ids=%s", rule.id, delivery_id, valid_copy_sent_ids)
                else:
                    post_send_warnings.append("verify_failed_after_accepted")
                    logger.warning("COPY_SINGLE_TARGET_CONFIRM_FAILED | rule_id=%s | delivery_id=%s | candidate_sent_message_ids=%s", rule.id, delivery_id, copy_sent_ids)

            await owner._log_delivery_pipeline_step(
                rule_id=rule.id,
                delivery_ids=delivery_ids,
                event_type="delivery_pipeline_step",
                pipeline_stage="copy_single",
                pipeline_result="ok" if valid_copy_sent_ids else "failed",
                source_channel=source_channel,
                target_id=target_id,
                source_message_ids=source_message_ids,
                error_text=None if valid_copy_sent_ids else "copy_message не сработал",
                extra={
                    "attempt_no": 1,
                    "sent_message_id": valid_copy_sent_ids[0] if valid_copy_sent_ids else None,
                    "caption_delivery_mode": caption_mode,
                    "requires_builder": requires_builder,
                },
            )

            if valid_copy_sent_ids:
                accepted_target_message_ids = [int(x) for x in valid_copy_sent_ids]
                if is_self_loop and any(x in set(source_message_ids) for x in accepted_target_message_ids):
                    logger.warning(
                        "SELF_LOOP_SENT_ID_COLLISION | rule_id=%s | delivery_id=%s | source_message_ids=%s | candidate_sent_message_ids=%s | action=reject_as_new_target",
                        rule.id, delivery_id, source_message_ids, accepted_target_message_ids,
                    )
                    await run_db(owner._mark_delivery_faulty_sync, delivery_id, "self_loop_sent_id_collision")
                    return False
                candidate_sent_message_ids = accepted_target_message_ids
                authoritative_sent_message_id = int(accepted_target_message_ids[0])
                reaction_target_message_id = authoritative_sent_message_id
                if is_self_loop:
                    logger.info(
                        "SELF_LOOP_REACTION_TARGET_RESOLVED | rule_id=%s | delivery_id=%s | source_message_ids=%s | accepted_target_message_ids=%s | reaction_target_message_id=%s | method=copy_single",
                        rule.id, delivery_id, source_message_ids, accepted_target_message_ids, reaction_target_message_id,
                    )
                reaction_result = await owner._run_post_send_step_safe(
                    step_name="reaction_after_copy_single",
                    rule_id=rule.id,
                    delivery_id=delivery_id,
                    idempotency_key=idempotency_key,
                    accepted_sent_message_ids=valid_sent_ids,
                    coro_factory=lambda: owner._add_reaction_for_rule_if_possible(
                        rule=rule,
                        target_id=target_id,
                        sent_message_id=authoritative_sent_message_id,
                        source_channel=str(source_channel or ""),
                        source_message_ids=source_message_ids,
                        delivery_id=delivery_id,
                    ),
                )
                if not reaction_result.get("ok") or not _reaction_applied_or_enqueued(reaction_result):
                    post_send_warnings.append("reaction_failed_after_accepted")
                    if is_self_loop:
                        logger.warning("SELF_LOOP_REACTION_FAILED_NON_FATAL | rule_id=%s | reaction_target_message_id=%s | error=%s", rule.id, reaction_target_message_id, reaction_result.get("error"))
                elif is_self_loop:
                    logger.info("SELF_LOOP_REACTION_APPLIED | rule_id=%s | target_id=%s | reaction_target_message_id=%s", rule.id, target_id, reaction_target_message_id)

                await owner._log_delivery_final_success(
                    rule_id=rule.id,
                    delivery_ids=delivery_ids,
                    final_method="copy_single",
                    source_channel=source_channel,
                    target_id=target_id,
                    source_message_ids=source_message_ids,
                    sent_message_id=authoritative_sent_message_id,
                    verify_result=None,
                    extra={
                        "caption_delivery_mode": caption_mode,
                        "requires_builder": requires_builder,
                    },
                )

                await run_db(
                    owner._mark_delivery_sent_sync,
                    delivery_id,
                    sent_message_id=authoritative_sent_message_id,
                    sent_message_ids=candidate_sent_message_ids,
                    target_id=str(target_id),
                    delivery_method="copy_single",
                )
                return True
            if valid_sent_ids:
                logger.warning("DELIVERY_SENT_UNVERIFIED_AFTER_ACCEPTED | rule_id=%s | delivery_id=%s | target_id=%s | sent_message_ids=%s | warnings=%s", rule.id, delivery_id, target_id, valid_sent_ids, post_send_warnings)
                await run_db(
                    owner._mark_delivery_sent_sync,
                    delivery_id,
                    sent_message_id=int(valid_sent_ids[0]),
                    sent_message_ids=valid_sent_ids,
                    target_id=str(target_id),
                    delivery_method="copy_single_unverified",
                )
                return True
            if copy_result.get("attempted"):
                if is_self_loop and send_result.error_text:
                    logger.info("SELF_LOOP_COPY_TO_REUPLOAD_ALLOWED | rule_id=%s | delivery_id=%s | error=%s", rule.id, delivery_id, send_result.error_text)
                else:
                    error_text = "copy_single_uncertain_no_fallback: copy_message was attempted but target confirmation failed; manual review required"
                    logger.warning("COPY_SINGLE_UNCERTAIN_NO_FALLBACK | rule_id=%s | delivery_id=%s | reason=copy_attempted_without_verified_target_message", rule.id, delivery_id)
                    await run_db(owner._mark_delivery_faulty_sync, delivery_id, error_text)
                    await owner._log_delivery_final_failure(
                        rule_id=rule.id,
                        delivery_ids=delivery_ids,
                        final_method="copy_single_uncertain_no_fallback",
                        source_channel=source_channel,
                        target_id=target_id,
                        source_message_ids=source_message_ids,
                        error_text=error_text,
                        attempts_debug=[
                            {"stage": "copy_single", "ok": False, "attempted": True, "candidate_sent_message_ids": copy_sent_ids},
                        ],
                        extra={"non_retryable": True, "manual_review_required": True},
                    )
                    return False
            else:
                logger.info("COPY_TO_REUPLOAD_FALLBACK_ALLOWED | rule_id=%s | delivery_id=%s | reason=copy_not_attempted", rule.id, delivery_id)
        else:
            await owner._log_delivery_pipeline_step(
                rule_id=rule.id,
                delivery_ids=delivery_ids,
                event_type="delivery_pipeline_step",
                pipeline_stage="copy_single",
                pipeline_result="skipped",
                source_channel=source_channel,
                target_id=target_id,
                source_message_ids=source_message_ids,
                error_text="copy_single пропущен политикой caption mode",
                extra={
                    "caption_delivery_mode": caption_mode,
                    "requires_builder": requires_builder,
                    "skip_reason": "builder_required_or_builder_first",
                },
            )

        # =========================================================
        # 3) FETCH MESSAGE
        # =========================================================
        await owner._log_delivery_pipeline_step(
            rule_id=rule.id,
            delivery_ids=delivery_ids,
            event_type="delivery_pipeline_step",
            pipeline_stage="fetch_message",
            pipeline_result="started",
            source_channel=source_channel,
            target_id=target_id,
            source_message_ids=source_message_ids,
            extra={
                "caption_delivery_mode": caption_mode,
                "requires_builder": requires_builder,
            },
        )

        message = live_message_for_builder or await owner._fetch_message(source_channel, message_id)

        await owner._log_delivery_pipeline_step(
            rule_id=rule.id,
            delivery_ids=delivery_ids,
            event_type="delivery_pipeline_step",
            pipeline_stage="fetch_message",
            pipeline_result="ok" if message else "failed",
            source_channel=source_channel,
            target_id=target_id,
            source_message_ids=source_message_ids,
            error_text=None if message else "Сообщение не получено через MTProto",
            extra={
                "caption_delivery_mode": caption_mode,
                "requires_builder": requires_builder,
            },
        )

        if not message:
            await owner._log_delivery_final_failure(
                rule_id=rule.id,
                delivery_ids=delivery_ids,
                final_method="fetch_message_failed",
                source_channel=source_channel,
                target_id=target_id,
                source_message_ids=source_message_ids,
                error_text="Сообщение не получено через MTProto",
                attempts_debug=[
                    {
                        "stage": "copy_single",
                        "ok": False,
                        "skipped": not use_copy_first,
                    },
                    {
                        "stage": "fetch_message",
                        "ok": False,
                        "error_text": "Сообщение не получено через MTProto",
                    },
                ],
                extra={
                    "caption_delivery_mode": caption_mode,
                    "requires_builder": requires_builder,
                },
            )
            return False

        content = owner._content_from_message_or_post(message=message, post_row=post_row)
        built_text, _built_entities = owner._build_text_and_entities_from_content(content)

        # =========================================================
        # 4) REUPLOAD SINGLE
        # =========================================================
        await owner._log_delivery_pipeline_step(
            rule_id=rule.id,
            delivery_ids=delivery_ids,
            event_type="delivery_pipeline_step",
            pipeline_stage="reupload_single",
            pipeline_result="started",
            source_channel=source_channel,
            target_id=target_id,
            source_message_ids=source_message_ids,
            extra={
                "attempt_no": 1,
                "caption_delivery_mode": caption_mode,
                "requires_builder": requires_builder,
            },
        )

        reupload_outcome = await owner._reupload_message(
            message,
            target_id,
            target_thread_id,
            post_row=post_row,
            is_self_loop=is_self_loop,
        )
        if getattr(reupload_outcome, "transport_accepted", False) and not getattr(reupload_outcome, "authoritative_resolved", False):
            error_text = "telethon_send_accepted_target_id_unresolved_non_retryable"
            logger.warning(
                "REUPLOAD_SINGLE_ACCEPTED_TARGET_ID_UNRESOLVED | rule_id=%s | delivery_id=%s | target_id=%s | returned_candidate_id=%s | action=no_second_send_no_reaction_manual_review",
                rule.id, delivery_id, target_id, getattr(reupload_outcome, "returned_candidate_id", None),
            )
            await owner._log_delivery_pipeline_step(
                rule_id=rule.id,
                delivery_ids=delivery_ids,
                event_type="delivery_pipeline_step",
                pipeline_stage="reupload_single_accepted_target_id_unresolved",
                pipeline_result="terminal_manual_review",
                source_channel=source_channel,
                target_id=target_id,
                source_message_ids=source_message_ids,
                error_text=error_text,
                extra={
                    "transport_accepted": True,
                    "authoritative_resolved": False,
                    "returned_candidate_id": getattr(reupload_outcome, "returned_candidate_id", None),
                    "returned_candidate_ids": getattr(reupload_outcome, "returned_candidate_ids", None),
                    "resolution_method": getattr(reupload_outcome, "resolution_method", None),
                    "manual_review_required": True,
                    "non_retryable": True,
                    "action": "no_second_send",
                },
            )
            await run_db(owner._mark_delivery_faulty_sync, delivery_id, error_text)
            return False

        sent_message_id = getattr(reupload_outcome, "authoritative_message_id", None) if hasattr(reupload_outcome, "authoritative_message_id") else reupload_outcome
        send_result = telegram_send_result_from_raw(
            None,
            method="reupload_single",
            fallback_sent_ids=[sent_message_id] if sent_message_id else None,
        )
        log_fn = logger.info if send_result.ok else logger.warning
        log_fn(
            "TELEGRAM_SEND_RESULT | method=%s | ok=%s | sent_message_ids=%s | sent_message_id=%s | raw_result_type=%s | error_text=%s | retryable=%s",
            send_result.method, send_result.ok, send_result.sent_message_ids, send_result.sent_message_id, send_result.raw_result_type, send_result.error_text, send_result.retryable
        )
        candidate_sent_message_ids = send_result.sent_message_ids
        if idempotency_key and candidate_sent_message_ids and hasattr(owner.db, "mark_delivery_attempt_accepted"):
            await run_db(
                owner.db.mark_delivery_attempt_accepted,
                idempotency_key,
                sent_message_ids=candidate_sent_message_ids,
                telegram_method="reupload_single",
            )
            logger.info(
                "DELIVERY_ATTEMPT_ACCEPTED | operation=single | method=reupload_single | key=%s | delivery_id=%s | sent_message_ids=%s",
                idempotency_key,
                delivery_id,
                candidate_sent_message_ids,
            )
        valid_sent_message_ids = await owner._confirm_target_delivery_message_ids_with_retry(
            rule_id=rule.id,
            delivery_id=delivery_id,
            source_channel=str(source_channel or ""),
            target_id=str(target_id),
            source_message_ids=source_message_ids,
            candidate_sent_message_ids=candidate_sent_message_ids,
            method="reupload_single",
        )

        await owner._log_delivery_pipeline_step(
            rule_id=rule.id,
            delivery_ids=delivery_ids,
            event_type="delivery_pipeline_step",
            pipeline_stage="reupload_single",
            pipeline_result="ok" if valid_sent_message_ids else "failed",
            source_channel=source_channel,
            target_id=target_id,
            source_message_ids=source_message_ids,
            error_text=None if valid_sent_message_ids else "target_message_not_found_after_send",
            extra={
                "attempt_no": 1,
                "sent_message_id": sent_message_id,
                "caption_delivery_mode": caption_mode,
                "requires_builder": requires_builder,
            },
        )

        if valid_sent_message_ids:
            logger.info("REUPLOAD_SINGLE_TARGET_VERIFY_OK | rule_id=%s | delivery_id=%s | source_channel=%s | source_message_id=%s | target_id=%s | valid_sent_message_ids=%s", rule.id, delivery_id, source_channel, message_id, target_id, valid_sent_message_ids)
            accepted_target_message_ids = [int(x) for x in valid_sent_message_ids]
            if is_self_loop and any(x in set(source_message_ids) for x in accepted_target_message_ids):
                logger.warning(
                    "SELF_LOOP_SENT_ID_COLLISION | rule_id=%s | delivery_id=%s | source_message_ids=%s | candidate_sent_message_ids=%s | action=reject_as_new_target",
                    rule.id, delivery_id, source_message_ids, accepted_target_message_ids,
                )
                await run_db(owner._mark_delivery_faulty_sync, delivery_id, "self_loop_sent_id_collision")
                return False
            authoritative_sent_message_id = int(accepted_target_message_ids[0])
            if is_self_loop and hasattr(owner, "_verify_self_loop_video_metadata"):
                await owner._run_post_send_step_safe(
                    step_name="self_loop_video_metadata_verify",
                    rule_id=rule.id,
                    delivery_id=delivery_id,
                    idempotency_key=idempotency_key,
                    accepted_sent_message_ids=accepted_target_message_ids,
                    coro_factory=lambda: owner._verify_self_loop_video_metadata(
                        rule_id=rule.id, source_message_id=message_id, target_id=target_id, sent_message_id=authoritative_sent_message_id,
                    ),
                )
            reaction_target_message_id = authoritative_sent_message_id
            if is_self_loop:
                logger.info(
                    "SELF_LOOP_REACTION_TARGET_RESOLVED | rule_id=%s | delivery_id=%s | source_message_ids=%s | accepted_target_message_ids=%s | reaction_target_message_id=%s | method=reupload_single",
                    rule.id, delivery_id, source_message_ids, accepted_target_message_ids, reaction_target_message_id,
                )
            try:
                reaction_result = await owner._add_reaction_for_rule_if_possible(
                    rule=rule,
                    target_id=target_id,
                    sent_message_id=authoritative_sent_message_id,
                    source_channel=str(source_channel or ""),
                    source_message_ids=source_message_ids,
                    delivery_id=delivery_id,
                )
                if is_self_loop and _reaction_applied_or_enqueued(reaction_result):
                    logger.info("SELF_LOOP_REACTION_APPLIED | rule_id=%s | target_id=%s | reaction_target_message_id=%s", rule.id, target_id, reaction_target_message_id)
                elif is_self_loop:
                    logger.warning("SELF_LOOP_REACTION_FAILED_NON_FATAL | rule_id=%s | reaction_target_message_id=%s | error=%s", rule.id, reaction_target_message_id, reaction_result)
            except Exception as exc:
                if is_self_loop:
                    logger.warning("SELF_LOOP_REACTION_FAILED_NON_FATAL | rule_id=%s | reaction_target_message_id=%s | error=%s", rule.id, reaction_target_message_id, exc)

            await owner._log_delivery_final_success(
                rule_id=rule.id,
                delivery_ids=delivery_ids,
                final_method="reupload_single",
                source_channel=source_channel,
                target_id=target_id,
                source_message_ids=source_message_ids,
                sent_message_id=authoritative_sent_message_id,
                verify_result=None,
                extra={
                    "caption_delivery_mode": caption_mode,
                    "requires_builder": requires_builder,
                },
            )

            await run_db(
                owner._mark_delivery_sent_sync,
                delivery_id,
                sent_message_id=authoritative_sent_message_id,
                sent_message_ids=valid_sent_message_ids,
                target_id=str(target_id),
                delivery_method="reupload_single",
            )
            return True

        logger.warning("REUPLOAD_SINGLE_TARGET_VERIFY_FAILED | rule_id=%s | delivery_id=%s | source_channel=%s | source_message_id=%s | target_id=%s | candidate_sent_message_ids=%s | reason=target_message_not_found_after_send", rule.id, delivery_id, source_channel, message_id, target_id, candidate_sent_message_ids)
        logger.warning("DELIVERY_FALSE_SUCCESS_PREVENTED | rule_id=%s | delivery_id=%s | method=%s | target_id=%s | candidate_sent_message_ids=%s | action=retry_or_faulty", rule.id, delivery_id, "reupload_single", target_id, candidate_sent_message_ids)

        if is_self_loop and candidate_sent_message_ids:
            logger.warning(
                "SELF_LOOP_REUPLOAD_ACCEPTED_UNVERIFIED | rule_id=%s | delivery_id=%s | source_message_id=%s | candidate_sent_message_ids=%s | action=mark_sent_no_second_send",
                rule.id, delivery_id, message_id, candidate_sent_message_ids,
            )
            accepted_target_message_ids = [int(x) for x in candidate_sent_message_ids]
            if any(x in set(source_message_ids) for x in accepted_target_message_ids):
                logger.warning(
                    "SELF_LOOP_SENT_ID_COLLISION | rule_id=%s | delivery_id=%s | source_message_ids=%s | candidate_sent_message_ids=%s | action=reject_as_new_target",
                    rule.id, delivery_id, source_message_ids, accepted_target_message_ids,
                )
                await run_db(owner._mark_delivery_faulty_sync, delivery_id, "self_loop_sent_id_collision")
                return False
            authoritative_sent_message_id = int(accepted_target_message_ids[0])
            reaction_target_message_id = authoritative_sent_message_id
            logger.info(
                "SELF_LOOP_REACTION_TARGET_RESOLVED | rule_id=%s | delivery_id=%s | source_message_ids=%s | accepted_target_message_ids=%s | reaction_target_message_id=%s | method=reupload_single",
                rule.id, delivery_id, source_message_ids, accepted_target_message_ids, reaction_target_message_id,
            )
            reaction_step = await owner._run_post_send_step_safe(
                step_name="reaction_after_reupload_single_self_loop_unverified",
                rule_id=rule.id,
                delivery_id=delivery_id,
                idempotency_key=idempotency_key,
                accepted_sent_message_ids=candidate_sent_message_ids,
                coro_factory=lambda: owner._add_reaction_for_rule_if_possible(
                    rule=rule, target_id=target_id, sent_message_id=reaction_target_message_id,
                    source_channel=str(source_channel or ""), source_message_ids=source_message_ids, delivery_id=delivery_id,
                    allow_unverified_self_loop_target=True,
                ),
            )
            if _reaction_applied_or_enqueued(reaction_step):
                logger.info(
                    "SELF_LOOP_REACTION_APPLIED | rule_id=%s | delivery_id=%s | target_id=%s | reaction_target_message_id=%s",
                    rule.id, delivery_id, target_id, reaction_target_message_id,
                )
            else:
                reaction_payload = reaction_step.get("result") if isinstance(reaction_step, dict) and isinstance(reaction_step.get("result"), dict) else reaction_step
                reaction_reason = reaction_payload.get("reason") if isinstance(reaction_payload, dict) else None
                logger.warning(
                    "SELF_LOOP_REACTION_FAILED_NON_FATAL | rule_id=%s | delivery_id=%s | target_id=%s | reaction_target_message_id=%s | reason=%s",
                    rule.id, delivery_id, target_id, reaction_target_message_id, reaction_reason or "unknown",
                )
            await owner._log_delivery_final_success(
                rule_id=rule.id, delivery_ids=delivery_ids, final_method="reupload_single_self_loop_unverified",
                source_channel=source_channel, target_id=target_id, source_message_ids=source_message_ids,
                sent_message_id=authoritative_sent_message_id, sent_message_ids=candidate_sent_message_ids, verify_result=None,
                extra={
                    "caption_delivery_mode": caption_mode, "requires_builder": requires_builder,
                    "verification_ok": False, "post_send_warning": "target_message_not_found_after_send",
                    "candidate_sent_message_ids": candidate_sent_message_ids, "second_send_blocked": True,
                },
            )
            await run_db(
                owner._mark_delivery_sent_sync, delivery_id, sent_message_id=authoritative_sent_message_id,
                sent_message_ids=candidate_sent_message_ids, target_id=str(target_id),
                delivery_method="reupload_single_self_loop_unverified",
            )
            return True

        if is_self_loop:
            logger.warning(
                "SELF_LOOP_SEND_FAILED_NO_DEGRADED_FALLBACK | rule_id=%s | delivery_id=%s | source_message_id=%s | has_media=%s",
                rule.id, delivery_id, message_id, bool(getattr(message, "media", None)),
            )
            return False

        # =========================================================
        # 5) DEBUG: fallback disabled
        # =========================================================
        if DEBUG_FORCE_DISABLE_BOTAPI_FALLBACK:
            await owner._log_delivery_pipeline_step(
                rule_id=rule.id,
                delivery_ids=delivery_ids,
                event_type="delivery_pipeline_step",
                pipeline_stage="text_fallback",
                pipeline_result="failed",
                source_channel=source_channel,
                target_id=target_id,
                source_message_ids=source_message_ids,
                error_text="Bot API fallback принудительно отключён для диагностики",
                extra={
                    "caption_delivery_mode": caption_mode,
                    "requires_builder": requires_builder,
                },
            )

            await owner._log_delivery_final_failure(
                rule_id=rule.id,
                delivery_ids=delivery_ids,
                final_method="single_pipeline_final_failure",
                source_channel=source_channel,
                target_id=target_id,
                source_message_ids=source_message_ids,
                error_text="Не удалось доставить сообщение через Telethon, Bot API fallback отключён",
                attempts_debug=[
                    {
                        "stage": "copy_single",
                        "ok": False,
                        "skipped": not use_copy_first,
                    },
                    {
                        "stage": "fetch_message",
                        "ok": True,
                    },
                    {
                        "stage": "reupload_single",
                        "ok": False,
                        "sent_message_id": None,
                    },
                    {
                        "stage": "text_fallback",
                        "ok": False,
                        "disabled": True,
                    },
                ],
                extra={
                    "caption_delivery_mode": caption_mode,
                    "requires_builder": requires_builder,
                },
            )
            return False

        # =========================================================
        # 6) TEXT FALLBACK
        # =========================================================
        html_text = _prepare_html_text(built_text)

        await owner._log_delivery_pipeline_step(
            rule_id=rule.id,
            delivery_ids=delivery_ids,
            event_type="delivery_pipeline_step",
            pipeline_stage="text_fallback",
            pipeline_result="started",
            source_channel=source_channel,
            target_id=target_id,
            source_message_ids=source_message_ids,
            extra={
                "caption_delivery_mode": caption_mode,
                "requires_builder": requires_builder,
            },
        )

        text_fallback_ok = False
        text_fallback_sent_message_id = None

        if html_text:
            try:
                sent = await owner.bot.send_message(
                    chat_id=target_id,
                    message_thread_id=target_thread_id,
                    text=html_text,
                    parse_mode="HTML",
                    disable_web_page_preview=True,
                )

                await owner._add_reaction_for_rule_if_possible(
                    rule=rule,
                    target_id=target_id,
                    sent_message_id=sent.message_id,
                    source_channel=str(source_channel or ""),
                    source_message_ids=source_message_ids,
                    delivery_id=delivery_id,
                )

                text_fallback_ok = True
                text_fallback_sent_message_id = sent.message_id

                await owner._log_delivery_pipeline_step(
                    rule_id=rule.id,
                    delivery_ids=delivery_ids,
                    event_type="delivery_pipeline_step",
                    pipeline_stage="text_fallback",
                    pipeline_result="ok",
                    source_channel=source_channel,
                    target_id=target_id,
                    source_message_ids=source_message_ids,
                    extra={
                        "sent_message_id": sent.message_id,
                        "caption_delivery_mode": caption_mode,
                        "requires_builder": requires_builder,
                    },
                )

                await owner._log_delivery_final_success(
                    rule_id=rule.id,
                    delivery_ids=delivery_ids,
                    final_method="text_fallback",
                    source_channel=source_channel,
                    target_id=target_id,
                    source_message_ids=source_message_ids,
                    sent_message_id=sent.message_id,
                    verify_result=None,
                    extra={
                        "caption_delivery_mode": caption_mode,
                        "requires_builder": requires_builder,
                    },
                )

                await run_db(owner._mark_delivery_sent_sync, delivery_id)
                return {"ok": True, "sent_message_ids": []}

            except Exception as exc:
                await owner._log_delivery_pipeline_step(
                    rule_id=rule.id,
                    delivery_ids=delivery_ids,
                    event_type="delivery_pipeline_step",
                    pipeline_stage="text_fallback",
                    pipeline_result="failed",
                    source_channel=source_channel,
                    target_id=target_id,
                    source_message_ids=source_message_ids,
                    error_text=str(exc),
                    extra={
                        "caption_delivery_mode": caption_mode,
                        "requires_builder": requires_builder,
                    },
                )
        else:
            await owner._log_delivery_pipeline_step(
                rule_id=rule.id,
                delivery_ids=delivery_ids,
                event_type="delivery_pipeline_step",
                pipeline_stage="text_fallback",
                pipeline_result="failed",
                source_channel=source_channel,
                target_id=target_id,
                source_message_ids=source_message_ids,
                error_text="Текстовый fallback невозможен: текст пустой",
                extra={
                    "caption_delivery_mode": caption_mode,
                    "requires_builder": requires_builder,
                },
            )

        # =========================================================
        # 7) FINAL FAILURE
        # =========================================================
        await owner._log_delivery_final_failure(
            rule_id=rule.id,
            delivery_ids=delivery_ids,
            final_method="single_pipeline_final_failure",
            source_channel=source_channel,
            target_id=target_id,
            source_message_ids=source_message_ids,
            error_text="Не удалось доставить сообщение",
            attempts_debug=[
                {
                    "stage": "copy_single",
                    "ok": False,
                    "skipped": not use_copy_first,
                },
                {
                    "stage": "fetch_message",
                    "ok": True,
                },
                {
                    "stage": "reupload_single",
                    "ok": False,
                    "sent_message_id": None,
                },
                {
                    "stage": "text_fallback",
                    "ok": text_fallback_ok,
                    "sent_message_id": text_fallback_sent_message_id,
                },
            ],
            extra={
                "caption_delivery_mode": caption_mode,
                "requires_builder": requires_builder,
            },
        )
        return False
