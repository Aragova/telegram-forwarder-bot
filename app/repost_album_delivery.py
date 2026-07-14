from __future__ import annotations

import asyncio
import logging

from .delivery_idempotency import normalize_valid_sent_message_ids
from .runtime_utils import run_db


logger = logging.getLogger("forwarder")


def _reaction_applied_or_enqueued(result) -> bool:
    payload = result.get("result") if isinstance(result, dict) and isinstance(result.get("result"), dict) else result
    if isinstance(payload, dict):
        return bool(payload.get("applied") or payload.get("enqueued"))
    return bool(payload)


class RepostAlbumDelivery:
    def __init__(self, owner):
        self.owner = owner

    async def deliver(
        self,
        rule,
        album_rows,
        source_channel,
        target_id,
        target_thread_id,
        idempotency_key: str | None = None,
    ):
        owner = self.owner
        delivery_ids = [int(r["delivery_id"]) for r in album_rows]
        message_ids = [int(r["message_id"]) for r in album_rows]

        strategy = await run_db(
            owner._resolve_repost_caption_delivery_strategy_sync,
            rule=rule,
            source_channel=source_channel,
            message_ids=message_ids,
            is_album=True,
        )

        caption_mode = strategy["configured_mode"]
        requires_builder = strategy["requires_builder"]
        use_copy_first = strategy["use_copy_first"]

        post_rows_by_message_id = {
            int(r["message_id"]): r
            for r in album_rows
        }

        is_self_loop = owner._is_self_loop_rule(rule)
        if is_self_loop:
            logger.info(
                "SELF_LOOP_REPOST_MODE | album | rule_id=%s | delivery_ids=%s | source_channel=%s | target_id=%s | source_message_ids=%s | action=real_repost",
                rule.id,
                delivery_ids,
                source_channel,
                target_id,
                message_ids,
            )

        source_messages = None
        first_source_caption = None
        final_error_text = None
        attempts_debug: list[dict] = []

        logger.info(
            "CAPTION_MODE | album | rule=%s | mode=%s | requires_builder=%s | selected_path=%s | items=%s",
            rule.id,
            caption_mode,
            requires_builder,
            "copy_first" if use_copy_first else "builder_first",
            len(message_ids),
        )

        try:
            source_entities = []
            for row in album_rows:
                content = owner._content_from_message_or_post(message=None, post_row=row)
                source_entities.extend((content or {}).get("entities") or [])
            owner._log_caption_entity_inventory(
                source="album",
                rule_id=rule.id,
                message_ids=message_ids,
                entities=source_entities,
            )
        except Exception as exc:
            logger.warning("CAPTION_ENTITY_INVENTORY_FAILED | source=album | rule_id=%s | error=%s", rule.id, exc)

        # =========================================================
        # PREVIEW / caption text for verify
        # =========================================================
        try:
            fetched_preview = await owner._fetch_album_messages(source_channel, message_ids)
            if fetched_preview:
                source_messages = fetched_preview
                first_source_caption = owner._get_album_primary_text(
                    source_messages,
                    post_rows=[
                        post_rows_by_message_id.get(int(getattr(m, "id")))
                        for m in source_messages
                    ],
                )
        except Exception as exc:
            logger.warning(
                "Не удалось заранее получить preview альбома %s -> %s: %s",
                source_channel,
                target_id,
                exc,
            )
            source_messages = None
            first_source_caption = None

        try:
            album_caption_entities = []
            if source_messages:
                for message in source_messages:
                    message_id = int(getattr(message, "id"))
                    content = owner._content_from_message_or_post(message=message)
                    entities = (content or {}).get("entities") or []
                    if not entities:
                        row_content = owner._content_from_message_or_post(
                            message=None,
                            post_row=post_rows_by_message_id.get(message_id),
                        )
                        entities = (row_content or {}).get("entities") or []
                    album_caption_entities.extend(entities)
            else:
                for row in album_rows:
                    content = owner._content_from_message_or_post(message=None, post_row=row)
                    album_caption_entities.extend((content or {}).get("entities") or [])

            album_entity_counts = owner._caption_entity_counts(album_caption_entities)
            album_custom_emoji_count = int(album_entity_counts.get("custom_emoji") or 0)
        except Exception as exc:
            logger.warning(
                "ALBUM_CUSTOM_EMOJI_DETECT_FAILED | rule_id=%s | message_ids=%s | error=%s",
                rule.id,
                message_ids,
                exc,
            )
            album_custom_emoji_count = 0

        if album_custom_emoji_count > 0 and use_copy_first:
            logger.info(
                "ALBUM_CUSTOM_EMOJI_FORCE_TELETHON | rule_id=%s | message_ids=%s | custom_emoji=%s | previous_selected_path=%s | new_selected_path=%s",
                rule.id,
                message_ids,
                album_custom_emoji_count,
                "copy_first",
                "reupload_album",
            )
            use_copy_first = False
            requires_builder = True

        logger.info(
            "COPY_ALBUM_CAPTION_POLICY | rule_id=%s | requires_builder=%s | selected_path=%s | caption_override=%s | reason=%s",
            rule.id,
            requires_builder,
            "copy_first" if use_copy_first else "builder_first",
            False,
            "custom_emoji_requires_telethon" if album_custom_emoji_count > 0 else (
                "pure_copy_messages_no_caption_override" if use_copy_first and not requires_builder else "copy_messages_api_has_no_caption_override"
            ),
        )

        # =========================================================
        # 1) COPY VIA BOT API
        # Выполняем только если режим разрешает copy-first
        # =========================================================
        if use_copy_first:
            await owner._log_delivery_pipeline_step(
                rule_id=rule.id,
                delivery_ids=delivery_ids,
                event_type="delivery_pipeline_step",
                pipeline_stage="copy_album",
                pipeline_result="started",
                source_channel=source_channel,
                target_id=target_id,
                source_message_ids=message_ids,
                extra={
                    "attempt_no": 1,
                    "caption_delivery_mode": caption_mode,
                    "requires_builder": requires_builder,
                },
            )

            copy_result = await owner._copy_album_via_bot(
                source_channel=source_channel,
                target_id=target_id,
                message_ids=message_ids,
                target_thread_id=target_thread_id,
            )
            attempts_debug.append({"stage": "copy_album", **copy_result})
            if idempotency_key and copy_result.get("ok") and copy_result.get("sent_message_ids"):
                sent_ids = [int(x) for x in (copy_result.get("sent_message_ids") or []) if str(x).isdigit()]
                valid_sent_ids = normalize_valid_sent_message_ids(sent_ids)
                if valid_sent_ids:
                    await run_db(owner.db.mark_delivery_attempt_accepted, idempotency_key, sent_message_ids=valid_sent_ids, telegram_method="copy_album")
                    logger.info("DELIVERY_ATTEMPT_ACCEPTED | operation=album | key=%s | sent_message_ids=%s", idempotency_key, valid_sent_ids)
                else:
                    logger.warning("DELIVERY_ATTEMPT_ACCEPTED_SKIPPED_INVALID_IDS | operation=%s | key=%s | delivery_id=%s | raw_sent_message_ids=%s", "album", idempotency_key, delivery_id, sent_ids)

            await owner._log_delivery_pipeline_step(
                rule_id=rule.id,
                delivery_ids=delivery_ids,
                event_type="delivery_pipeline_step",
                pipeline_stage="copy_album",
                pipeline_result="ok" if copy_result["ok"] else "failed",
                source_channel=source_channel,
                target_id=target_id,
                source_message_ids=message_ids,
                error_text=copy_result.get("error_text"),
                extra={
                    "attempt_no": 1,
                    "sent_message_id": copy_result.get("sent_message_id"),
                    "sent_count": copy_result.get("sent_count"),
                    "caption_delivery_mode": caption_mode,
                    "requires_builder": requires_builder,
                },
            )

            if copy_result["ok"]:
                accepted_copy_sent_ids = normalize_valid_sent_message_ids(copy_result.get("sent_message_ids") or [])
                await owner._log_delivery_pipeline_step(
                    rule_id=rule.id,
                    delivery_ids=delivery_ids,
                    event_type="delivery_pipeline_step",
                    pipeline_stage="verify_after_copy",
                    pipeline_result="started",
                    source_channel=source_channel,
                    target_id=target_id,
                    source_message_ids=message_ids,
                    extra={"attempt_no": 1},
                )

                verify_step = await owner._run_post_send_step_safe(
                    step_name="verify_after_copy_album",
                    rule_id=rule.id,
                    delivery_id=delivery_ids[0] if delivery_ids else None,
                    idempotency_key=idempotency_key,
                    accepted_sent_message_ids=accepted_copy_sent_ids,
                    coro_factory=lambda: owner._verify_album_delivery(
                        target_id=target_id,
                        expected_count=len(message_ids),
                        sent_message_ids=copy_result.get("sent_message_ids"),
                        target_thread_id=target_thread_id,
                    ),
                )
                verified = verify_step.get("result") or {"ok": False, "error_text": "verify_after_copy_album_failed_non_fatal"}
                attempts_debug.append({"stage": "verify_after_copy", **verified})

                await owner._log_delivery_pipeline_step(
                    rule_id=rule.id,
                    delivery_ids=delivery_ids,
                    event_type="delivery_pipeline_step",
                    pipeline_stage="verify_after_copy",
                    pipeline_result="ok" if verified["ok"] else "failed",
                    source_channel=source_channel,
                    target_id=target_id,
                    source_message_ids=message_ids,
                    error_text=verified.get("error_text"),
                    extra={
                        "attempt_no": 1,
                        "verify_result": owner._serialize_pipeline_verify_result(verified),
                    },
                )

                if not verified["ok"]:
                    await asyncio.sleep(1.5)

                    await owner._log_delivery_pipeline_step(
                        rule_id=rule.id,
                        delivery_ids=delivery_ids,
                        event_type="delivery_pipeline_step",
                        pipeline_stage="verify_after_copy_retry_only",
                        pipeline_result="started",
                        source_channel=source_channel,
                        target_id=target_id,
                        source_message_ids=message_ids,
                        extra={"attempt_no": 2},
                    )

                    verified_retry = await owner._verify_album_delivery(
                        target_id=target_id,
                        expected_count=len(message_ids),
                        sent_message_ids=copy_result.get("sent_message_ids"),
                        target_thread_id=target_thread_id,
                    )
                    attempts_debug.append({"stage": "verify_after_copy_retry_only", **verified_retry})

                    await owner._log_delivery_pipeline_step(
                        rule_id=rule.id,
                        delivery_ids=delivery_ids,
                        event_type="delivery_pipeline_step",
                        pipeline_stage="verify_after_copy_retry_only",
                        pipeline_result="ok" if verified_retry["ok"] else "failed",
                        source_channel=source_channel,
                        target_id=target_id,
                        source_message_ids=message_ids,
                        error_text=verified_retry.get("error_text"),
                        extra={
                            "attempt_no": 2,
                            "verify_result": owner._serialize_pipeline_verify_result(verified_retry),
                        },
                    )

                    if verified_retry["ok"]:
                        verified = verified_retry

                if verified["ok"]:
                    accepted_target_message_ids = normalize_valid_sent_message_ids(verified.get("sent_message_ids") or copy_result.get("sent_message_ids") or [])
                    if is_self_loop and any(x in set(message_ids) for x in accepted_target_message_ids):
                        logger.warning(
                            "SELF_LOOP_SENT_ID_COLLISION | rule_id=%s | delivery_id=%s | source_message_ids=%s | candidate_sent_message_ids=%s | action=reject_as_new_target",
                            rule.id, (delivery_ids[0] if delivery_ids else None), message_ids, accepted_target_message_ids,
                        )
                        return False
                    sent_message_id = (accepted_target_message_ids[0] if accepted_target_message_ids else None) or verified.get("first_message_id") or copy_result.get("sent_message_id")
                    if is_self_loop:
                        logger.info(
                            "SELF_LOOP_REACTION_TARGET_RESOLVED | rule_id=%s | delivery_id=%s | source_message_ids=%s | accepted_target_message_ids=%s | reaction_target_message_id=%s | method=copy_album",
                            rule.id, (delivery_ids[0] if delivery_ids else None), message_ids, accepted_target_message_ids, sent_message_id,
                        )
                    reaction_result = await owner._run_post_send_step_safe(
                        step_name="reaction_after_copy_album",
                        rule_id=rule.id,
                        delivery_id=delivery_ids[0] if delivery_ids else None,
                        idempotency_key=idempotency_key,
                        accepted_sent_message_ids=accepted_copy_sent_ids,
                        coro_factory=lambda: owner._add_reaction_for_rule_if_possible(
                            rule=rule,
                            target_id=target_id,
                            sent_message_id=sent_message_id,
                            source_channel=str(source_channel or ""),
                            source_message_ids=message_ids,
                            delivery_id=(delivery_ids[0] if delivery_ids else None),
                        ),
                    )
                    if is_self_loop and _reaction_applied_or_enqueued(reaction_result):
                        logger.info("SELF_LOOP_REACTION_APPLIED | rule_id=%s | target_id=%s | reaction_target_message_id=%s", rule.id, target_id, sent_message_id)
                    elif is_self_loop:
                        logger.warning("SELF_LOOP_REACTION_FAILED_NON_FATAL | rule_id=%s | reaction_target_message_id=%s | error=%s", rule.id, sent_message_id, reaction_result.get("error"))

                    await owner._log_delivery_final_success(
                        rule_id=rule.id,
                        delivery_ids=delivery_ids,
                        final_method="copy_album_verified",
                        source_channel=source_channel,
                        target_id=target_id,
                        source_message_ids=message_ids,
                        sent_message_id=sent_message_id,
                        verify_result=verified,
                        extra={
                            "caption_delivery_mode": caption_mode,
                            "requires_builder": requires_builder,
                        },
                    )

                    await run_db(owner._mark_many_deliveries_sent_sync, delivery_ids)
                    return True
                if accepted_copy_sent_ids:
                    logger.warning("DELIVERY_SENT_UNVERIFIED_AFTER_ACCEPTED | rule_id=%s | delivery_ids=%s | target_id=%s | sent_message_ids=%s", rule.id, delivery_ids, target_id, accepted_copy_sent_ids)
                    await run_db(owner._mark_many_deliveries_sent_sync, delivery_ids)
                    return True
        else:
            copy_result = {
                "ok": False,
                "sent_message_id": None,
                "sent_count": 0,
                "error_text": "copy_album skipped because custom_emoji requires Telethon" if album_custom_emoji_count > 0 else "copy_album пропущен политикой caption mode",
            }

            await owner._log_delivery_pipeline_step(
                rule_id=rule.id,
                delivery_ids=delivery_ids,
                event_type="delivery_pipeline_step",
                pipeline_stage="copy_album",
                pipeline_result="skipped",
                source_channel=source_channel,
                target_id=target_id,
                source_message_ids=message_ids,
                error_text=copy_result["error_text"],
                extra={
                    "caption_delivery_mode": caption_mode,
                    "requires_builder": requires_builder,
                    "skip_reason": "custom_emoji_requires_telethon" if album_custom_emoji_count > 0 else "builder_required_or_builder_first",
                    "custom_emoji": album_custom_emoji_count,
                },
            )

        # =========================================================
        # 3) RETRY COPY ONLY IF COPY REALLY FAILED
        # =========================================================
        if use_copy_first and not copy_result["ok"] and not is_self_loop:
            await asyncio.sleep(1.2)

            await owner._log_delivery_pipeline_step(
                rule_id=rule.id,
                delivery_ids=delivery_ids,
                event_type="delivery_pipeline_step",
                pipeline_stage="copy_album_retry",
                pipeline_result="started",
                source_channel=source_channel,
                target_id=target_id,
                source_message_ids=message_ids,
                extra={"attempt_no": 2},
            )

            copy_retry_result = await owner._copy_album_via_bot(
                source_channel=source_channel,
                target_id=target_id,
                message_ids=message_ids,
                target_thread_id=target_thread_id,
            )
            attempts_debug.append({"stage": "copy_album_retry", **copy_retry_result})

            await owner._log_delivery_pipeline_step(
                rule_id=rule.id,
                delivery_ids=delivery_ids,
                event_type="delivery_pipeline_step",
                pipeline_stage="copy_album_retry",
                pipeline_result="ok" if copy_retry_result["ok"] else "failed",
                source_channel=source_channel,
                target_id=target_id,
                source_message_ids=message_ids,
                error_text=copy_retry_result.get("error_text"),
                extra={
                    "attempt_no": 2,
                    "sent_message_id": copy_retry_result.get("sent_message_id"),
                    "sent_count": copy_retry_result.get("sent_count"),
                },
            )
            if copy_retry_result["ok"]:
                await owner._log_delivery_pipeline_step(
                    rule_id=rule.id,
                    delivery_ids=delivery_ids,
                    event_type="delivery_pipeline_step",
                    pipeline_stage="verify_after_copy_retry",
                    pipeline_result="started",
                    source_channel=source_channel,
                    target_id=target_id,
                    source_message_ids=message_ids,
                    extra={"attempt_no": 1},
                )

                verified = await owner._verify_album_delivery(
                    target_id=target_id,
                    expected_count=len(message_ids),
                    sent_message_ids=copy_retry_result.get("sent_message_ids"),
                    target_thread_id=target_thread_id,
                )
                attempts_debug.append({"stage": "verify_after_copy_retry", **verified})

                await owner._log_delivery_pipeline_step(
                    rule_id=rule.id,
                    delivery_ids=delivery_ids,
                    event_type="delivery_pipeline_step",
                    pipeline_stage="verify_after_copy_retry",
                    pipeline_result="ok" if verified["ok"] else "failed",
                    source_channel=source_channel,
                    target_id=target_id,
                    source_message_ids=message_ids,
                    error_text=verified.get("error_text"),
                    extra={
                        "attempt_no": 1,
                        "verify_result": owner._serialize_pipeline_verify_result(verified),
                    },
                )

                if not verified["ok"]:
                    await asyncio.sleep(1.5)

                    await owner._log_delivery_pipeline_step(
                        rule_id=rule.id,
                        delivery_ids=delivery_ids,
                        event_type="delivery_pipeline_step",
                        pipeline_stage="verify_after_copy_retry_only_second",
                        pipeline_result="started",
                        source_channel=source_channel,
                        target_id=target_id,
                        source_message_ids=message_ids,
                        extra={"attempt_no": 2},
                    )

                    verified_retry = await owner._verify_album_delivery(
                        target_id=target_id,
                        expected_count=len(message_ids),
                        sent_message_ids=copy_retry_result.get("sent_message_ids"),
                        target_thread_id=target_thread_id,
                    )
                    attempts_debug.append({"stage": "verify_after_copy_retry_only_second", **verified_retry})

                    await owner._log_delivery_pipeline_step(
                        rule_id=rule.id,
                        delivery_ids=delivery_ids,
                        event_type="delivery_pipeline_step",
                        pipeline_stage="verify_after_copy_retry_only_second",
                        pipeline_result="ok" if verified_retry["ok"] else "failed",
                        source_channel=source_channel,
                        target_id=target_id,
                        source_message_ids=message_ids,
                        error_text=verified_retry.get("error_text"),
                        extra={
                            "attempt_no": 2,
                            "verify_result": owner._serialize_pipeline_verify_result(verified_retry),
                        },
                    )

                    if verified_retry["ok"]:
                        verified = verified_retry

                if verified["ok"]:
                    sent_message_id = verified.get("first_message_id") or copy_retry_result.get("sent_message_id")
                    await owner._add_reaction_for_rule_if_possible(
                            rule=rule,
                            target_id=target_id,
                            sent_message_id=sent_message_id,
                        )

                    await owner._log_delivery_final_success(
                        rule_id=rule.id,
                        delivery_ids=delivery_ids,
                        final_method="copy_album_retry_verified",
                        source_channel=source_channel,
                        target_id=target_id,
                        source_message_ids=message_ids,
                        sent_message_id=sent_message_id,
                        verify_result=verified,
                        extra={
                            "caption_delivery_mode": caption_mode,
                            "requires_builder": requires_builder,
                        },
                    )

                    await run_db(owner._mark_many_deliveries_sent_sync, delivery_ids)
                    return True
        else:
            copy_retry_result = {
                "ok": False,
                "sent_message_id": None,
                "sent_count": 0,
                "error_text": "Повторный copy не выполнялся",
            }

        # =========================================================
        # 4) FETCH SOURCE ALBUM
        # =========================================================
        if source_messages is None:
            source_messages = await owner._fetch_album_messages(source_channel, message_ids)

        if source_messages is not None and first_source_caption is None:
            first_source_caption = owner._get_album_primary_text(
                source_messages,
                post_rows=[
                    post_rows_by_message_id.get(int(getattr(m, "id")))
                    for m in source_messages
                ],
                is_self_loop=is_self_loop,
            )

        if len(source_messages) != len(message_ids):
            final_error_text = "Не удалось получить весь альбом через MTProto"

            await owner._log_delivery_final_failure(
                rule_id=rule.id,
                delivery_ids=delivery_ids,
                final_method="fetch_album_failed",
                source_channel=source_channel,
                target_id=target_id,
                source_message_ids=message_ids,
                error_text=final_error_text,
                attempts_debug=attempts_debug,
                extra={
                    "caption_delivery_mode": caption_mode,
                    "requires_builder": requires_builder,
                },
            )
            return False

        # =========================================================
        # 5) REUPLOAD AS ALBUM
        # =========================================================
        await owner._log_delivery_pipeline_step(
            rule_id=rule.id,
            delivery_ids=delivery_ids,
            event_type="delivery_pipeline_step",
            pipeline_stage="reupload_album",
            pipeline_result="started",
            source_channel=source_channel,
            target_id=target_id,
            source_message_ids=message_ids,
            extra={
                "attempt_no": 1,
                "caption_delivery_mode": caption_mode,
                "requires_builder": requires_builder,
            },
        )

        reupload_result = await owner._reupload_album(
            messages=source_messages,
            target_id=target_id,
            target_thread_id=target_thread_id,
            post_rows=[
                post_rows_by_message_id.get(int(getattr(m, "id")))
                for m in source_messages
            ],
            is_self_loop=is_self_loop,
        )
        attempts_debug.append({"stage": "reupload_album", **reupload_result})
        reupload_candidate_sent_ids = normalize_valid_sent_message_ids(reupload_result.get("sent_message_ids") or [])
        if idempotency_key and reupload_candidate_sent_ids:
            await run_db(
                owner.db.mark_delivery_attempt_accepted,
                idempotency_key,
                sent_message_ids=reupload_candidate_sent_ids,
                telegram_method="reupload_album",
            )
            logger.info(
                "DELIVERY_ATTEMPT_ACCEPTED | operation=album | method=reupload_album | key=%s | delivery_ids=%s | sent_message_ids=%s",
                idempotency_key, delivery_ids, reupload_candidate_sent_ids,
            )

        await owner._log_delivery_pipeline_step(
            rule_id=rule.id,
            delivery_ids=delivery_ids,
            event_type="delivery_pipeline_step",
            pipeline_stage="reupload_album",
            pipeline_result="ok" if reupload_result["ok"] else "failed",
            source_channel=source_channel,
            target_id=target_id,
            source_message_ids=message_ids,
            error_text=reupload_result.get("error_text"),
            extra={
                "attempt_no": 1,
                "sent_message_id": reupload_result.get("sent_message_id"),
                "sent_count": reupload_result.get("sent_count"),
                "caption_delivery_mode": caption_mode,
                "requires_builder": requires_builder,
            },
        )

        expected_count = len(message_ids)
        reupload_sent_count = int(reupload_result.get("sent_count") or 0)
        upload_confirmed_by_send_result = (
            reupload_result.get("ok") is True
            and reupload_sent_count >= expected_count
        )

        if reupload_result["ok"]:
            await owner._log_delivery_pipeline_step(
                rule_id=rule.id,
                delivery_ids=delivery_ids,
                event_type="delivery_pipeline_step",
                pipeline_stage="verify_after_reupload",
                pipeline_result="started",
                source_channel=source_channel,
                target_id=target_id,
                source_message_ids=message_ids,
                extra={"attempt_no": 1},
            )

            verified = await owner._verify_album_delivery(
                target_id=target_id,
                expected_count=len(message_ids),
                sent_message_ids=reupload_result.get("sent_message_ids"),
                target_thread_id=target_thread_id,
                target_grouped_id=reupload_result.get("target_grouped_id"),
            )
            attempts_debug.append({"stage": "verify_after_reupload", **verified})

            await owner._log_delivery_pipeline_step(
                rule_id=rule.id,
                delivery_ids=delivery_ids,
                event_type="delivery_pipeline_step",
                pipeline_stage="verify_after_reupload",
                pipeline_result="ok" if verified["ok"] else "failed",
                source_channel=source_channel,
                target_id=target_id,
                source_message_ids=message_ids,
                error_text=verified.get("error_text"),
                extra={
                    "attempt_no": 1,
                    "verify_result": owner._serialize_pipeline_verify_result(verified),
                },
            )

            if not verified["ok"]:
                await asyncio.sleep(1.5)

                await owner._log_delivery_pipeline_step(
                    rule_id=rule.id,
                    delivery_ids=delivery_ids,
                    event_type="delivery_pipeline_step",
                    pipeline_stage="verify_after_reupload_retry_only",
                    pipeline_result="started",
                    source_channel=source_channel,
                    target_id=target_id,
                    source_message_ids=message_ids,
                    extra={"attempt_no": 2},
                )

                verified_retry = await owner._verify_album_delivery(
                    target_id=target_id,
                    expected_count=len(message_ids),
                    sent_message_ids=reupload_result.get("sent_message_ids"),
                    target_thread_id=target_thread_id,
                    target_grouped_id=reupload_result.get("target_grouped_id"),
                )
                attempts_debug.append({"stage": "verify_after_reupload_retry_only", **verified_retry})

                await owner._log_delivery_pipeline_step(
                    rule_id=rule.id,
                    delivery_ids=delivery_ids,
                    event_type="delivery_pipeline_step",
                    pipeline_stage="verify_after_reupload_retry_only",
                    pipeline_result="ok" if verified_retry["ok"] else "failed",
                    source_channel=source_channel,
                    target_id=target_id,
                    source_message_ids=message_ids,
                    error_text=verified_retry.get("error_text"),
                    extra={
                        "attempt_no": 2,
                        "verify_result": owner._serialize_pipeline_verify_result(verified_retry),
                    },
                )
                if verified_retry["ok"]:
                    verified = verified_retry

            if verified["ok"]:
                sent_message_ids = [int(x) for x in (verified.get("sent_message_ids") or reupload_result.get("sent_message_ids") or [])]
                if is_self_loop and any(x in set(message_ids) for x in sent_message_ids):
                    logger.warning(
                        "SELF_LOOP_SENT_ID_COLLISION | rule_id=%s | delivery_id=%s | source_message_ids=%s | candidate_sent_message_ids=%s | action=reject_as_new_target",
                        rule.id, (delivery_ids[0] if delivery_ids else None), message_ids, sent_message_ids,
                    )
                    return False
                reaction_message_id, reaction_target_reason = await owner._select_reaction_message_id(
                    target_id=target_id,
                    sent_message_ids=sent_message_ids,
                )
                sent_message_id = (sent_message_ids[0] if sent_message_ids else None) or reupload_result.get("sent_message_id")

                if reaction_message_id:
                    if is_self_loop:
                        logger.info(
                            "SELF_LOOP_REACTION_TARGET_RESOLVED | rule_id=%s | delivery_id=%s | source_message_ids=%s | accepted_target_message_ids=%s | reaction_target_message_id=%s | method=reupload_album",
                            rule.id, (delivery_ids[0] if delivery_ids else None), message_ids, sent_message_ids, reaction_message_id,
                        )
                    try:
                        reaction_result = await owner._add_reaction_for_rule_if_possible(
                            rule=rule,
                            target_id=target_id,
                            sent_message_id=reaction_message_id,
                            source_channel=str(source_channel or ""),
                            source_message_ids=message_ids,
                            delivery_id=(delivery_ids[0] if delivery_ids else None),
                        )
                        if is_self_loop and _reaction_applied_or_enqueued(reaction_result):
                            logger.info("SELF_LOOP_REACTION_APPLIED | rule_id=%s | target_id=%s | reaction_target_message_id=%s", rule.id, target_id, reaction_message_id)
                        elif is_self_loop:
                            logger.warning("SELF_LOOP_REACTION_FAILED_NON_FATAL | rule_id=%s | reaction_target_message_id=%s | error=%s", rule.id, reaction_message_id, reaction_result)
                    except Exception as exc:
                        if is_self_loop:
                            logger.warning("SELF_LOOP_REACTION_FAILED_NON_FATAL | rule_id=%s | reaction_target_message_id=%s | error=%s", rule.id, reaction_message_id, exc)

                await owner._log_delivery_final_success(
                    rule_id=rule.id,
                    delivery_ids=delivery_ids,
                    final_method="reupload_album_verified",
                    source_channel=source_channel,
                    target_id=target_id,
                    source_message_ids=message_ids,
                    sent_message_id=sent_message_id,
                    sent_message_ids=sent_message_ids,
                    reaction_message_id=reaction_message_id,
                    verify_result=verified,
                    extra={
                        "caption_delivery_mode": caption_mode,
                        "requires_builder": requires_builder,
                        "reaction_target_reason": reaction_target_reason,
                    },
                )

                if is_self_loop:
                    await run_db(
                        owner._mark_album_deliveries_sent_sync,
                        delivery_ids=delivery_ids,
                        sent_message_ids=sent_message_ids,
                        target_id=str(target_id),
                        delivery_method="reupload_album_verified",
                    )
                else:
                    await run_db(owner._mark_many_deliveries_sent_sync, delivery_ids)
                return True

            if is_self_loop and reupload_candidate_sent_ids:
                if any(int(x) in set(message_ids) for x in reupload_candidate_sent_ids):
                    logger.warning(
                        "SELF_LOOP_SENT_ID_COLLISION | rule_id=%s | delivery_id=%s | source_message_ids=%s | candidate_sent_message_ids=%s | action=reject_as_new_target",
                        rule.id, (delivery_ids[0] if delivery_ids else None), message_ids, reupload_candidate_sent_ids,
                    )
                    return False
                logger.warning(
                    "SELF_LOOP_ALBUM_ACCEPTED_UNVERIFIED | rule_id=%s | delivery_ids=%s | candidate_sent_message_ids=%s | action=mark_sent_no_second_send",
                    rule.id, delivery_ids, reupload_candidate_sent_ids,
                )
                reaction_message_id = int(reupload_candidate_sent_ids[0])
                logger.info(
                    "SELF_LOOP_REACTION_TARGET_RESOLVED | rule_id=%s | delivery_id=%s | source_message_ids=%s | accepted_target_message_ids=%s | reaction_target_message_id=%s | method=reupload_album",
                    rule.id, (delivery_ids[0] if delivery_ids else None), message_ids, reupload_candidate_sent_ids, reaction_message_id,
                )
                async def _apply_unverified_album_reaction():
                    return await owner._add_reaction_for_rule_if_possible(
                        rule=rule,
                        target_id=target_id,
                        sent_message_id=reaction_message_id,
                        source_channel=str(source_channel or ""),
                        source_message_ids=message_ids,
                        delivery_id=(delivery_ids[0] if delivery_ids else None),
                        allow_unverified_self_loop_target=True,
                    )

                if hasattr(owner, "_run_post_send_step_safe"):
                    reaction_result = await owner._run_post_send_step_safe(
                        step_name="reaction_after_reupload_album_self_loop_unverified",
                        rule_id=rule.id,
                        delivery_id=delivery_ids[0] if delivery_ids else None,
                        idempotency_key=idempotency_key,
                        accepted_sent_message_ids=reupload_candidate_sent_ids,
                        coro_factory=_apply_unverified_album_reaction,
                    )
                else:
                    try:
                        reaction_result = {"ok": True, "result": await _apply_unverified_album_reaction()}
                    except Exception as exc:
                        reaction_result = {"ok": False, "error": str(exc)}
                if _reaction_applied_or_enqueued(reaction_result):
                    logger.info("SELF_LOOP_REACTION_APPLIED | rule_id=%s | target_id=%s | reaction_target_message_id=%s", rule.id, target_id, reaction_message_id)
                else:
                    reaction_payload = reaction_result.get("result") if isinstance(reaction_result, dict) and isinstance(reaction_result.get("result"), dict) else reaction_result
                    reaction_reason = reaction_payload.get("reason") if isinstance(reaction_payload, dict) else None
                    reaction_error = reaction_result.get("error") if isinstance(reaction_result, dict) else None
                    logger.warning(
                        "SELF_LOOP_REACTION_FAILED_NON_FATAL | rule_id=%s | delivery_id=%s | target_id=%s | reaction_target_message_id=%s | reason=%s",
                        rule.id, (delivery_ids[0] if delivery_ids else None), target_id, reaction_message_id, reaction_reason or reaction_error or "unknown",
                    )
                await owner._log_delivery_final_success(
                    rule_id=rule.id, delivery_ids=delivery_ids, final_method="reupload_album_self_loop_unverified",
                    source_channel=source_channel, target_id=target_id, source_message_ids=message_ids,
                    sent_message_id=int(reupload_candidate_sent_ids[0]), sent_message_ids=reupload_candidate_sent_ids,
                    verify_result=verified,
                    extra={
                        "caption_delivery_mode": caption_mode, "requires_builder": requires_builder,
                        "verification_ok": False, "post_send_warning": "target_message_not_found_after_send",
                        "candidate_sent_message_ids": reupload_candidate_sent_ids, "second_send_blocked": True,
                    },
                )
                await run_db(
                    owner._mark_album_deliveries_sent_sync,
                    delivery_ids=delivery_ids, sent_message_ids=reupload_candidate_sent_ids,
                    target_id=str(target_id), delivery_method="reupload_album_self_loop_unverified",
                )
                return True

            if upload_confirmed_by_send_result:
                logger.warning(
                    "REUPLOAD_ALBUM | verify не подтвердил альбом, но отправка уже подтверждена Telethon | "
                    "rule_id=%s | sent_count=%s | expected_count=%s | error=%s",
                    rule.id,
                    reupload_sent_count,
                    expected_count,
                    verified.get("error_text"),
                )

                await owner._log_delivery_pipeline_step(
                    rule_id=rule.id,
                    delivery_ids=delivery_ids,
                    event_type="delivery_pipeline_step",
                    pipeline_stage="verify_after_reupload",
                    pipeline_result="soft_failed_upload_confirmed",
                    source_channel=source_channel,
                    target_id=target_id,
                    source_message_ids=message_ids,
                    error_text=verified.get("error_text"),
                    extra={
                        "verify_result": owner._serialize_pipeline_verify_result(verified),
                        "sent_count": reupload_sent_count,
                        "expected_count": expected_count,
                        "sent_message_id": reupload_result.get("sent_message_id"),
                        "source_message_ids": message_ids,
                        "reason": "upload_confirmed_by_telethon_send_result",
                    },
                )

                sent_message_ids = reupload_result.get("sent_message_ids") or []
                logger.info(
                    "DELIVERY_SENT_MESSAGE_IDS_EXTRACTED | rule_id=%s | delivery_id=%s | method=%s | source_message_ids=%s | sent_message_ids=%s | result_type=%s",
                    rule.id,
                    (delivery_ids[0] if delivery_ids else None),
                    "reupload_album_unverified_success",
                    message_ids,
                    sent_message_ids,
                    type(reupload_result).__name__,
                )
                valid_sent_message_ids = await owner._validate_sent_message_ids_for_delivery(
                    rule_id=rule.id,
                    delivery_id=(delivery_ids[0] if delivery_ids else None),
                    source_channel=str(source_channel or ""),
                    target_id=str(target_id),
                    source_message_ids=message_ids,
                    candidate_sent_message_ids=sent_message_ids,
                    method="reupload_album_unverified_success",
                )
                sent_message_id = valid_sent_message_ids[0] if valid_sent_message_ids else None
                reaction_message_id, reaction_target_reason = await owner._select_reaction_message_id(
                    target_id=target_id,
                    sent_message_ids=valid_sent_message_ids,
                )

                await owner._log_delivery_final_success(
                    rule_id=rule.id,
                    delivery_ids=delivery_ids,
                    final_method="reupload_album_unverified_success",
                    source_channel=source_channel,
                    target_id=target_id,
                    source_message_ids=message_ids,
                    sent_message_id=sent_message_id,
                    sent_message_ids=valid_sent_message_ids,
                    reaction_message_id=reaction_message_id,
                    verify_result=verified,
                    extra={
                        "caption_delivery_mode": caption_mode,
                        "requires_builder": requires_builder,
                        "reaction_target_reason": reaction_target_reason,
                        "verify_ok": False,
                        "verify_count": verified.get("count"),
                        "verify_grouped_id": verified.get("grouped_id"),
                        "verify_first_message_id": verified.get("first_message_id"),
                        "first_sent_message_id": reupload_result.get("sent_message_id"),
                        "candidate_sent_message_ids": sent_message_ids,
                        "valid_sent_message_ids": valid_sent_message_ids,
                        "sent_count": reupload_sent_count,
                        "expected_count": expected_count,
                    },
                )

                if reaction_message_id:
                    try:
                        logger.info(
                            "REACTION_AFTER_REUPLOAD_UNVERIFIED_VALIDATED | start | rule_id=%s | delivery_id=%s | target_id=%s | message_id=%s | valid_sent_message_ids=%s",
                            rule.id,
                            (delivery_ids[0] if delivery_ids else None),
                            target_id,
                            reaction_message_id,
                            valid_sent_message_ids,
                        )
                        await owner._add_reaction_for_rule_if_possible(
                            rule=rule,
                            target_id=target_id,
                            sent_message_id=reaction_message_id,
                            source_channel=str(source_channel or ""),
                            source_message_ids=message_ids,
                            delivery_id=(delivery_ids[0] if delivery_ids else None),
                        )
                    except Exception as exc:
                        logger.warning(
                            "REACTION_AFTER_REUPLOAD_UNVERIFIED_VALIDATED | failed | rule_id=%s | delivery_id=%s | target_id=%s | message_id=%s | error=%s",
                            rule.id,
                            (delivery_ids[0] if delivery_ids else None),
                            target_id,
                            reaction_message_id,
                            exc,
                        )
                else:
                    logger.warning(
                        "REACTION_SKIPPED_UNVERIFIED_ALBUM_SENT_IDS | rule_id=%s | delivery_id=%s | source_channel=%s | target_id=%s | method=%s | candidate_sent_message_ids=%s | source_message_ids=%s | reason=%s",
                        rule.id,
                        (delivery_ids[0] if delivery_ids else None),
                        source_channel,
                        target_id,
                        "reupload_album_unverified_success",
                        sent_message_ids,
                        message_ids,
                        verified.get("error_text") or "verify_album_sent_ids_not_found",
                    )

                await run_db(owner._mark_many_deliveries_sent_sync, delivery_ids)
                return True

        if reupload_result.get("transport_accepted") and not reupload_result.get("authoritative_resolved"):
            error_text = "telethon_send_accepted_target_id_unresolved_non_retryable"
            logger.warning(
                "REUPLOAD_ALBUM_ACCEPTED_TARGET_ID_UNRESOLVED | rule_id=%s | delivery_ids=%s | target_id=%s | returned_candidate_ids=%s | action=no_retry_no_second_send",
                rule.id, delivery_ids, target_id, reupload_result.get("returned_candidate_ids"),
            )
            await owner._log_delivery_pipeline_step(
                rule_id=rule.id,
                delivery_ids=delivery_ids,
                event_type="delivery_pipeline_step",
                pipeline_stage="reupload_album_accepted_target_id_unresolved",
                pipeline_result="terminal_manual_review",
                source_channel=source_channel,
                target_id=target_id,
                source_message_ids=message_ids,
                error_text=error_text,
                extra={
                    "transport_accepted": True,
                    "authoritative_resolved": False,
                    "returned_candidate_ids": reupload_result.get("returned_candidate_ids"),
                    "resolution_method": reupload_result.get("resolution_method"),
                    "manual_review_required": True,
                    "non_retryable": True,
                    "action": "no_retry_no_second_send",
                },
            )
            for delivery_id in delivery_ids:
                await run_db(owner._mark_delivery_faulty_sync, delivery_id, error_text)
            return False

        if is_self_loop:
            logger.warning(
                "SELF_LOOP_ALBUM_SEND_FAILED_NO_FALLBACK | rule_id=%s | delivery_ids=%s | source_message_ids=%s",
                rule.id, delivery_ids, message_ids,
            )
            return False

        # =========================================================
        # 6) RETRY REUPLOAD ONLY IF REUPLOAD REALLY FAILED
        # =========================================================
        if not reupload_result["ok"]:
            await asyncio.sleep(1.2)

            await owner._log_delivery_pipeline_step(
                rule_id=rule.id,
                delivery_ids=delivery_ids,
                event_type="delivery_pipeline_step",
                pipeline_stage="reupload_album_retry",
                pipeline_result="started",
                source_channel=source_channel,
                target_id=target_id,
                source_message_ids=message_ids,
                extra={"attempt_no": 2},
            )

            reupload_retry_result = await owner._reupload_album(
                messages=source_messages,
                target_id=target_id,
                target_thread_id=target_thread_id,
                post_rows=[
                    post_rows_by_message_id.get(int(getattr(m, "id")))
                    for m in source_messages
                ],
                is_self_loop=is_self_loop,
            )
            attempts_debug.append({"stage": "reupload_album_retry", **reupload_retry_result})

            await owner._log_delivery_pipeline_step(
                rule_id=rule.id,
                delivery_ids=delivery_ids,
                event_type="delivery_pipeline_step",
                pipeline_stage="reupload_album_retry",
                pipeline_result="ok" if reupload_retry_result["ok"] else "failed",
                source_channel=source_channel,
                target_id=target_id,
                source_message_ids=message_ids,
                error_text=reupload_retry_result.get("error_text"),
                extra={
                    "attempt_no": 2,
                    "sent_message_id": reupload_retry_result.get("sent_message_id"),
                    "sent_count": reupload_retry_result.get("sent_count"),
                },
            )

            if reupload_retry_result["ok"]:
                await owner._log_delivery_pipeline_step(
                    rule_id=rule.id,
                    delivery_ids=delivery_ids,
                    event_type="delivery_pipeline_step",
                    pipeline_stage="verify_after_reupload_retry",
                    pipeline_result="started",
                    source_channel=source_channel,
                    target_id=target_id,
                    source_message_ids=message_ids,
                    extra={"attempt_no": 1},
                )

                verified = await owner._verify_album_delivery(
                    target_id=target_id,
                    expected_count=len(message_ids),
                    sent_message_ids=reupload_retry_result.get("sent_message_ids"),
                    target_thread_id=target_thread_id,
                    target_grouped_id=reupload_retry_result.get("target_grouped_id"),
                )
                attempts_debug.append({"stage": "verify_after_reupload_retry", **verified})

                await owner._log_delivery_pipeline_step(
                    rule_id=rule.id,
                    delivery_ids=delivery_ids,
                    event_type="delivery_pipeline_step",
                    pipeline_stage="verify_after_reupload_retry",
                    pipeline_result="ok" if verified["ok"] else "failed",
                    source_channel=source_channel,
                    target_id=target_id,
                    source_message_ids=message_ids,
                    error_text=verified.get("error_text"),
                    extra={
                        "attempt_no": 1,
                        "verify_result": owner._serialize_pipeline_verify_result(verified),
                    },
                )

                if not verified["ok"]:
                    await asyncio.sleep(1.5)

                    await owner._log_delivery_pipeline_step(
                        rule_id=rule.id,
                        delivery_ids=delivery_ids,
                        event_type="delivery_pipeline_step",
                        pipeline_stage="verify_after_reupload_retry_only_second",
                        pipeline_result="started",
                        source_channel=source_channel,
                        target_id=target_id,
                        source_message_ids=message_ids,
                        extra={"attempt_no": 2},
                    )

                    verified_retry = await owner._verify_album_delivery(
                        target_id=target_id,
                        expected_count=len(message_ids),
                        sent_message_ids=reupload_retry_result.get("sent_message_ids"),
                        target_thread_id=target_thread_id,
                        target_grouped_id=reupload_retry_result.get("target_grouped_id"),
                    )
                    attempts_debug.append({"stage": "verify_after_reupload_retry_only_second", **verified_retry})

                    await owner._log_delivery_pipeline_step(
                        rule_id=rule.id,
                        delivery_ids=delivery_ids,
                        event_type="delivery_pipeline_step",
                        pipeline_stage="verify_after_reupload_retry_only_second",
                        pipeline_result="ok" if verified_retry["ok"] else "failed",
                        source_channel=source_channel,
                        target_id=target_id,
                        source_message_ids=message_ids,
                        error_text=verified_retry.get("error_text"),
                        extra={
                            "attempt_no": 2,
                            "verify_result": owner._serialize_pipeline_verify_result(verified_retry),
                        },
                    )

                    if verified_retry["ok"]:
                        verified = verified_retry

                if verified["ok"]:
                    sent_message_ids = verified.get("sent_message_ids") or reupload_retry_result.get("sent_message_ids") or []
                    reaction_message_id, reaction_target_reason = await owner._select_reaction_message_id(
                        target_id=target_id,
                        sent_message_ids=sent_message_ids,
                    )
                    sent_message_id = (sent_message_ids[0] if sent_message_ids else None) or reupload_retry_result.get("sent_message_id")

                    if reaction_message_id:
                        await owner._add_reaction_for_rule_if_possible(
                        rule=rule,
                        target_id=target_id,
                        sent_message_id=reaction_message_id,
                    )

                    await owner._log_delivery_final_success(
                        rule_id=rule.id,
                        delivery_ids=delivery_ids,
                        final_method="reupload_album_retry_verified",
                        source_channel=source_channel,
                        target_id=target_id,
                        source_message_ids=message_ids,
                        sent_message_id=sent_message_id,
                        sent_message_ids=sent_message_ids,
                        reaction_message_id=reaction_message_id,
                        verify_result=verified,
                        extra={
                            "caption_delivery_mode": caption_mode,
                            "requires_builder": requires_builder,
                            "reaction_target_reason": reaction_target_reason,
                        },
                    )

                    await run_db(owner._mark_many_deliveries_sent_sync, delivery_ids)
                    return True
        else:
            reupload_retry_result = {
                "ok": False,
                "sent_message_id": None,
                "sent_count": 0,
                "error_text": "Повторный reupload не выполнялся, потому что первый reupload уже отработал",
            }

        # =========================================================
        # 7) EMERGENCY FALLBACK: ONE BY ONE
        # =========================================================
        await owner._log_delivery_pipeline_step(
            rule_id=rule.id,
            delivery_ids=delivery_ids,
            event_type="delivery_pipeline_step",
            pipeline_stage="send_album_one_by_one",
            pipeline_result="started",
            source_channel=source_channel,
            target_id=target_id,
            source_message_ids=message_ids,
            extra={"attempt_no": 1},
        )

        one_by_one_result = await owner._send_album_one_by_one(
            messages=source_messages,
            target_id=target_id,
            target_thread_id=target_thread_id,
            post_rows=[
                post_rows_by_message_id.get(int(getattr(m, "id")))
                for m in source_messages
            ],
        )
        attempts_debug.append({"stage": "send_album_one_by_one", **one_by_one_result})

        await owner._log_delivery_pipeline_step(
            rule_id=rule.id,
            delivery_ids=delivery_ids,
            event_type="delivery_pipeline_step",
            pipeline_stage="send_album_one_by_one",
            pipeline_result="ok" if one_by_one_result["ok"] else "failed",
            source_channel=source_channel,
            target_id=target_id,
            source_message_ids=message_ids,
            error_text=one_by_one_result.get("error_text"),
            extra={
                "attempt_no": 1,
                "sent_message_id": one_by_one_result.get("sent_message_id"),
                "sent_count": one_by_one_result.get("sent_count"),
            },
        )

        if one_by_one_result.get("transport_accepted") and not one_by_one_result.get("authoritative_resolved"):
            error_text = "telethon_one_by_one_send_accepted_target_id_unresolved_non_retryable"
            logger.warning(
                "ONE_BY_ONE_ACCEPTED_TARGET_ID_UNRESOLVED | rule_id=%s | delivery_ids=%s | target_id=%s | returned_candidate_ids=%s | action=no_retry_no_second_send | manual_review_required=True",
                rule.id, delivery_ids, target_id, one_by_one_result.get("returned_candidate_ids"),
            )
            await owner._log_delivery_pipeline_step(
                rule_id=rule.id,
                delivery_ids=delivery_ids,
                event_type="delivery_pipeline_step",
                pipeline_stage="one_by_one_accepted_target_id_unresolved",
                pipeline_result="terminal_manual_review",
                source_channel=source_channel,
                target_id=target_id,
                source_message_ids=message_ids,
                error_text=error_text,
                extra={
                    "transport_accepted": True,
                    "authoritative_resolved": False,
                    "returned_candidate_id": one_by_one_result.get("returned_candidate_id"),
                    "returned_candidate_ids": one_by_one_result.get("returned_candidate_ids"),
                    "resolution_method": one_by_one_result.get("resolution_method"),
                    "sent_count": one_by_one_result.get("sent_count"),
                    "resolved_authoritative_message_ids_before_unresolved": one_by_one_result.get("resolved_authoritative_message_ids_before_unresolved"),
                    "manual_review_required": True,
                    "non_retryable": True,
                    "action": "no_retry_no_second_send",
                },
            )
            for delivery_id in delivery_ids:
                await run_db(owner._mark_delivery_faulty_sync, delivery_id, error_text)
            return False

        if one_by_one_result["ok"]:
            sent_message_id = one_by_one_result.get("sent_message_id")
            if sent_message_id:
                await owner._add_reaction_for_rule_if_possible(
                            rule=rule,
                            target_id=target_id,
                            sent_message_id=sent_message_id,
                        )

            await owner._log_delivery_final_success(
                rule_id=rule.id,
                delivery_ids=delivery_ids,
                final_method="one_by_one_fallback",
                source_channel=source_channel,
                target_id=target_id,
                source_message_ids=message_ids,
                sent_message_id=sent_message_id,
                verify_result=None,
                extra={
                    "caption_delivery_mode": caption_mode,
                    "requires_builder": requires_builder,
                },
            )

            await run_db(owner._mark_many_deliveries_sent_sync, delivery_ids)
            return True

        # =========================================================
        # 8) FINAL FAILURE
        # =========================================================
        final_error_text = (
            one_by_one_result.get("error_text")
            or reupload_retry_result.get("error_text")
            or reupload_result.get("error_text")
            or copy_retry_result.get("error_text")
            or copy_result.get("error_text")
            or "Не удалось доставить альбом ни одним методом"
        )

        await owner._log_delivery_final_failure(
            rule_id=rule.id,
            delivery_ids=delivery_ids,
            final_method="album_pipeline_final_failure",
            source_channel=source_channel,
            target_id=target_id,
            source_message_ids=message_ids,
            error_text=final_error_text,
            attempts_debug=attempts_debug,
            extra={
                "caption_delivery_mode": caption_mode,
                "requires_builder": requires_builder,
            },
        )
        return False
