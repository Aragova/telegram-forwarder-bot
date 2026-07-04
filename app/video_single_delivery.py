from __future__ import annotations

import logging

from .runtime_utils import run_db
from .sender_primitives import _detect_message_media_kind

logger = logging.getLogger("forwarder")


class VideoSingleDelivery:
    def __init__(self, owner):
        self.owner = owner

    async def deliver(
        self,
        rule,
        delivery_id,
        message_id,
        source_channel,
        target_id,
        target_thread_id,
    ):
        owner = self.owner
        post_id = await run_db(owner._get_post_id_by_delivery_sync, delivery_id)

        await run_db(
            owner._log_video_event_sync,
            event_type="video_processing_started",
            delivery_id=delivery_id,
            rule_id=rule.id,
            post_id=post_id,
            status="processing",
            extra={
                "source_channel": source_channel,
                "target_id": target_id,
                "target_thread_id": target_thread_id,
                "source_message_id": message_id,
                "video_mode": True,
            },
        )

        try:
            message = await owner._fetch_message(source_channel, message_id)
            if not message:
                await run_db(
                    owner._finalize_video_failure_sync,
                    delivery_id=delivery_id,
                    rule_id=rule.id,
                    post_id=post_id,
                    source_channel=source_channel,
                    target_id=target_id,
                    target_thread_id=target_thread_id,
                    source_message_id=message_id,
                    error_text="Сообщение не получено через MTProto",
                )
                return False

            media_kind = _detect_message_media_kind(message)
            horizontal_intro, vertical_intro = await run_db(
                owner._get_rule_intro_items_sync,
                rule,
            )

            caption_payload = owner._build_video_caption_delivery_payload(rule)

            rule_caption = caption_payload["caption"]
            rule_caption_entities = caption_payload["caption_entities"]
            caption_entities_json = caption_payload["caption_entities_json"]
            caption_delivery_mode = caption_payload["caption_delivery_mode"]
            requires_premium = caption_payload["requires_premium"]
            selected_mode = caption_payload["selected_mode"]

            clip_duration_seconds = int(getattr(rule, "video_clip_duration_seconds", None) or 118)

            stage_logger = owner._build_video_stage_logger(
                rule=rule,
                delivery_id=delivery_id,
                post_id=post_id,
                source_channel=source_channel,
                target_id=target_id,
                source_message_id=message_id,
            )

            await run_db(
                owner._log_video_event_sync,
                event_type="video_download_started",
                delivery_id=delivery_id,
                rule_id=rule.id,
                post_id=post_id,
                status="processing",
                extra={
                    "source_channel": source_channel,
                    "target_id": target_id,
                    "target_thread_id": target_thread_id,
                    "source_message_id": message_id,
                    "media_kind": media_kind,
                    "trim_seconds": int(getattr(rule, "video_trim_seconds", 120) or 120),
                    "video_clip_duration_seconds": clip_duration_seconds,
                    "horizontal_intro_id": getattr(horizontal_intro, "id", None),
                    "vertical_intro_id": getattr(vertical_intro, "id", None),
                    "horizontal_intro_name": getattr(horizontal_intro, "display_name", None),
                    "vertical_intro_name": getattr(vertical_intro, "display_name", None),
                    "has_rule_caption": bool(rule_caption),
                    "has_rule_caption_entities": bool(rule_caption_entities),
                    "caption_delivery_mode": caption_delivery_mode,
                    "selected_mode": selected_mode,
                    "caption_requires_premium": requires_premium,
                },
            )

            if media_kind != "video":
                await run_db(
                    owner._log_video_event_sync,
                    event_type="video_processing_completed",
                    delivery_id=delivery_id,
                    rule_id=rule.id,
                    post_id=post_id,
                    status="sent",
                    extra={
                        "source_channel": source_channel,
                        "target_id": target_id,
                        "target_thread_id": target_thread_id,
                        "source_message_id": message_id,
                        "media_kind": media_kind,
                        "skipped": True,
                        "skip_reason": "not_video",
                    },
                )

                await run_db(owner._mark_delivery_sent_sync, delivery_id)
                return {"ok": True, "sent_message_ids": []}

            source_video_path = await owner._download_video_source(
                message,
                delivery_id=delivery_id,
                rule_id=rule.id,
                post_id=post_id,
                source_channel=source_channel,
                target_id=target_id,
                source_message_id=message_id,
            )

            if not source_video_path:
                await run_db(
                    owner._finalize_video_failure_sync,
                    delivery_id=delivery_id,
                    rule_id=rule.id,
                    post_id=post_id,
                    source_channel=source_channel,
                    target_id=target_id,
                    target_thread_id=target_thread_id,
                    source_message_id=message_id,
                    error_text="Не удалось скачать видео",
                )
                return False

            try:
                sent_msg = await owner.video_processor.process_video(
                    video_file_id=None,
                    context=None,
                    destination_channel=target_id,
                    target_thread_id=target_thread_id,
                    add_intro=bool(getattr(rule, "video_add_intro", False)),
                    intro_name_horizontal=getattr(horizontal_intro, "file_name", None) if horizontal_intro else None,
                    intro_name_vertical=getattr(vertical_intro, "file_name", None) if vertical_intro else None,
                    intro_item_horizontal=horizontal_intro,
                    intro_item_vertical=vertical_intro,
                    caption=rule_caption or "",
                    caption_entities_json=caption_entities_json,
                    caption_send_mode=selected_mode,
                    input_file_path=str(source_video_path),
                    clip_duration_seconds=clip_duration_seconds,
                    stage_logger=stage_logger,
                )
            finally:
                try:
                    source_video_path.unlink(missing_ok=True)
                except Exception:
                    pass

            if sent_msg:
                sent_message_ids = owner._extract_sent_message_ids(sent_msg)
                valid_sent_message_ids = await owner._confirm_target_delivery_message_ids_with_retry(
                    rule_id=rule.id,
                    delivery_id=delivery_id,
                    source_channel=str(source_channel or ""),
                    target_id=str(target_id),
                    source_message_ids=[int(message_id)],
                    candidate_sent_message_ids=sent_message_ids,
                    method="video_process",
                    max_age_seconds=900,
                )
                sent_message_id = valid_sent_message_ids[0] if valid_sent_message_ids else None
                logger.info("DELIVERY_SENT_MESSAGE_IDS_EXTRACTED | rule_id=%s | delivery_id=%s | method=%s | source_message_ids=%s | sent_message_ids=%s | result_type=%s", rule.id, delivery_id, "video_process", [message_id], sent_message_ids, type(sent_msg).__name__)

                try:
                    if sent_message_id:
                        await owner._add_reaction_for_rule_if_possible(
                            rule=rule,
                            target_id=target_id,
                            sent_message_id=sent_message_id,
                            source_channel=source_channel,
                            source_message_ids=[message_id],
                            delivery_id=delivery_id,
                            max_age_seconds=900,
                        )
                    else:
                        logger.warning(
                            "VIDEO_REACTION | не удалось подтвердить sent_message_id после process_video | rule=%s | delivery=%s | target=%s",
                            rule.id,
                            delivery_id,
                            target_id,
                        )
                except Exception as exc:
                    logger.warning(
                        "Не удалось поставить реакцию под видео-сообщение %s в %s: %s",
                        sent_message_id,
                        target_id,
                        exc,
                    )

                await run_db(
                    owner._finalize_video_success_sync,
                    delivery_id=delivery_id,
                    rule_id=rule.id,
                    post_id=post_id,
                    source_channel=source_channel,
                    target_id=target_id,
                    target_thread_id=target_thread_id,
                    source_message_id=message_id,
                    sent_message_id=sent_message_id,
                    fallback_mode="deliver_single",
                    caption_delivery_mode=caption_delivery_mode,
                    selected_mode=selected_mode,
                    caption_requires_premium=requires_premium,
                )
                return True

            await run_db(
                owner._finalize_video_failure_sync,
                delivery_id=delivery_id,
                rule_id=rule.id,
                post_id=post_id,
                source_channel=source_channel,
                target_id=target_id,
                target_thread_id=target_thread_id,
                source_message_id=message_id,
                error_text="Обычная доставка внутри video-ветки не сработала",
                fallback_mode="deliver_single",
                caption_delivery_mode=caption_delivery_mode,
                selected_mode=selected_mode,
                caption_requires_premium=requires_premium,
            )
            return False

        except Exception as exc:
            logger.exception("Ошибка video delivery rule=%s delivery=%s", rule.id, delivery_id)

            await run_db(
                owner._finalize_video_failure_sync,
                delivery_id=delivery_id,
                rule_id=rule.id,
                post_id=post_id,
                source_channel=source_channel,
                target_id=target_id,
                target_thread_id=target_thread_id,
                source_message_id=message_id,
                error_text=str(exc),
            )
            return False

