from __future__ import annotations
import logging, time
from datetime import datetime, timezone, timedelta
from typing import Any
from telethon.tl import types as tl_types
from pathlib import Path
from .repository_models import GLOBAL_INTERVAL_GAP_SECONDS, utc_now_iso
from .telegram_client import ReactionClientInfo
from .video_processor import VideoProcessor
from .scheduler_service import SchedulerService
from .top_time_guard_service import TopTimeGuardService
from .delivery_idempotency import build_delivery_idempotency_key, extract_sent_message_ids_from_attempt, normalize_valid_sent_message_ids
from .delivery_content_helpers import (
    build_video_caption_delivery_payload,
    normalize_caption_entities,
    video_caption_requires_premium,
)
from .runtime_utils import run_db
from .sender_primitives import (
    MAX_INVALID_MP4_RETRY,
    MAX_NORMAL_REACTION_ATTEMPTS,
    REACTION_POOL,
    NORMAL_REACTION_POOL,
    DEBUG_FORCE_DISABLE_BOTAPI_FALLBACK,
    _telethon_entities_to_bot,
    _build_text_with_entities,
    _utf16_text_length,
    _is_valid_entity_range_utf16,
    _markdownish_to_html,
    _prepare_html_text,
    _normalize_reaction_emoji,
    _detect_message_media_kind,
)

logger = logging.getLogger("forwarder")

class SenderService:
    def __init__(
        self, bot, telethon_client, reaction_clients: list[ReactionClientInfo], db
    ):
        self.bot = bot
        self.telethon = telethon_client
        self.reaction_clients = reaction_clients or []
        self.db = db
        self.scheduler_service = SchedulerService(self.db)

        self.video_processor = VideoProcessor(
            bot=self.bot,
            telethon_client=self.telethon,
        )

    def _extract_sent_message_id(self, sent_msg) -> int | None:
        from .sender_post_send_helpers import SenderPostSendHelpers

        return SenderPostSendHelpers(self).extract_sent_message_id(sent_msg)

    def _extract_sent_message_ids(self, sent_result) -> list[int]:
        from .sender_post_send_helpers import SenderPostSendHelpers

        return SenderPostSendHelpers(self).extract_sent_message_ids(sent_result)

    async def _validate_reaction_target_message(
        self,
        *,
        rule_id: int | None,
        source_channel: str,
        target_id: str,
        source_message_ids: list[int],
        sent_message_id: int | None,
        delivery_id: int | None = None,
        max_age_seconds: int = 300,
    ) -> int | None:
        from .sender_post_send_helpers import SenderPostSendHelpers

        return await SenderPostSendHelpers(self).validate_reaction_target_message(
            rule_id=rule_id,
            source_channel=source_channel,
            target_id=target_id,
            source_message_ids=source_message_ids,
            sent_message_id=sent_message_id,
            delivery_id=delivery_id,
            max_age_seconds=max_age_seconds,
        )

    async def _validate_sent_message_ids_for_delivery(
        self,
        *,
        rule_id: int | None,
        delivery_id: int | None,
        source_channel: str,
        target_id: str,
        source_message_ids: list[int],
        candidate_sent_message_ids: list[int],
        method: str,
        max_age_seconds: int = 300,
    ) -> list[int]:
        from .sender_post_send_helpers import SenderPostSendHelpers

        return await SenderPostSendHelpers(self).validate_sent_message_ids_for_delivery(
            rule_id=rule_id,
            delivery_id=delivery_id,
            source_channel=source_channel,
            target_id=target_id,
            source_message_ids=source_message_ids,
            candidate_sent_message_ids=candidate_sent_message_ids,
            method=method,
            max_age_seconds=max_age_seconds,
        )

    async def _confirm_target_delivery_message_ids(
        self,
        *,
        rule_id: int | None,
        delivery_id: int | None,
        source_channel: str,
        target_id: str,
        source_message_ids: list[int],
        candidate_sent_message_ids: list[int],
        method: str,
        max_age_seconds: int = 300,
    ) -> list[int]:
        from .sender_post_send_helpers import SenderPostSendHelpers

        return await SenderPostSendHelpers(self).confirm_target_delivery_message_ids(
            rule_id=rule_id,
            delivery_id=delivery_id,
            source_channel=source_channel,
            target_id=target_id,
            source_message_ids=source_message_ids,
            candidate_sent_message_ids=candidate_sent_message_ids,
            method=method,
            max_age_seconds=max_age_seconds,
        )

    async def _confirm_target_delivery_message_ids_with_retry(self, **kwargs) -> list[int]:
        from .sender_post_send_helpers import SenderPostSendHelpers

        return await SenderPostSendHelpers(self).confirm_target_delivery_message_ids_with_retry(
            **kwargs,
        )

    async def _run_post_send_step_safe(
        self,
        *,
        step_name: str,
        rule_id: int | None,
        delivery_id: int | None,
        idempotency_key: str | None = None,
        accepted_sent_message_ids: list[int] | None = None,
        coro_factory=None,
    ) -> dict:
        from .sender_post_send_helpers import SenderPostSendHelpers

        return await SenderPostSendHelpers(self).run_post_send_step_safe(
            step_name=step_name,
            rule_id=rule_id,
            delivery_id=delivery_id,
            idempotency_key=idempotency_key,
            accepted_sent_message_ids=accepted_sent_message_ids,
            coro_factory=coro_factory,
        )


    def _caption_entity_counts(self, entities) -> dict[str, int]:
        from .sender_content_helpers import SenderContentHelpers

        return SenderContentHelpers(self).caption_entity_counts(entities)
    def _log_caption_entity_inventory(self, *, source: str, rule_id=None, message_ids=None, entities=None) -> None:
        from .sender_content_helpers import SenderContentHelpers

        return SenderContentHelpers(self).log_caption_entity_inventory(source=source, rule_id=rule_id, message_ids=message_ids, entities=entities)
    def _normalize_video_caption_entities(self, raw_entities) -> list[dict]:
        return normalize_caption_entities(raw_entities)

    def _content_from_message_or_post(self, message=None, post_row=None) -> dict:
        from .sender_content_helpers import SenderContentHelpers

        return SenderContentHelpers(self).content_from_message_or_post(message=message, post_row=post_row)
    def _video_caption_requires_premium(self, caption: str | None, caption_entities) -> bool:
        return video_caption_requires_premium(caption_entities)

    def _build_video_caption_delivery_payload(self, rule) -> dict[str, Any]:
        payload = build_video_caption_delivery_payload(
            caption=getattr(rule, "video_caption", None),
            raw_caption_entities=getattr(rule, "video_caption_entities_json", None),
            caption_delivery_mode=self._get_rule_video_caption_delivery_mode(rule),
        )

        logger.info(
            "VIDEO_CAPTION_MODE | payload built | mode=%s | selected_mode=%s | has_caption=%s | entities=%s | requires_premium=%s",
            payload["caption_delivery_mode"],
            payload["selected_mode"],
            bool(payload["caption"]),
            len(payload["caption_entities"]),
            payload["requires_premium"],
        )

        return payload

    def _build_telethon_entities_from_content(self, content: dict | None, text: str) -> list:
        from .sender_content_helpers import SenderContentHelpers

        return SenderContentHelpers(self).build_telethon_entities_from_content(content, text)
    def _build_text_and_entities_from_content(self, content: dict | None) -> tuple[str, list]:
        from .sender_content_helpers import SenderContentHelpers

        return SenderContentHelpers(self).build_text_and_entities_from_content(content)
    def _serialize_pipeline_verify_result(self, verify_result: dict | None) -> dict:
        from .sender_delivery_logging_helpers import SenderDeliveryLoggingHelpers

        return SenderDeliveryLoggingHelpers(self).serialize_pipeline_verify_result(
            verify_result,
        )

    def _schedule_video_event_log(
        self,
        *,
        event_type: str,
        delivery_id: int,
        rule_id: int,
        post_id: int | None,
        status: str | None = None,
        error_text: str | None = None,
        extra: dict | None = None,
    ) -> dict:
        from .sender_video_logging_helpers import SenderVideoLoggingHelpers

        return SenderVideoLoggingHelpers(self).schedule_video_event_log(
            event_type=event_type,
            delivery_id=delivery_id,
            rule_id=rule_id,
            post_id=post_id,
            status=status,
            error_text=error_text,
            extra=extra,
        )

    def _get_post_row_for_rule_message_sync(
        self,
        rule,
        source_channel: str,
        message_id: int,
    ) -> dict | None:
        return self._get_post_row_for_rule_message(rule, source_channel, message_id)

    def _get_rule_intro_items_sync(self, rule):
        from .sender_state_sync_helpers import SenderStateSyncHelpers

        return SenderStateSyncHelpers(self).get_rule_intro_items_sync(rule)

    def _resolve_repost_caption_delivery_strategy_sync(
        self,
        *,
        rule,
        source_channel: str,
        message_ids: list[int],
        is_album: bool,
    ) -> dict[str, Any]:
        return self._resolve_repost_caption_delivery_strategy(
            rule=rule,
            source_channel=source_channel,
            message_ids=message_ids,
            is_album=is_album,
        )

    def _mark_delivery_sent_sync(
        self,
        delivery_id: int,
        *,
        sent_message_id: int | None = None,
        sent_message_ids: list[int] | None = None,
        target_id: str | None = None,
        delivery_method: str | None = None,
    ) -> dict:
        from .sender_state_sync_helpers import SenderStateSyncHelpers

        return SenderStateSyncHelpers(self).mark_delivery_sent_sync(
            delivery_id,
            sent_message_id=sent_message_id,
            sent_message_ids=sent_message_ids,
            target_id=target_id,
            delivery_method=delivery_method,
        )

    def _mark_many_deliveries_sent_sync(self, delivery_ids: list[int]) -> None:
        from .sender_state_sync_helpers import SenderStateSyncHelpers

        return SenderStateSyncHelpers(self).mark_many_deliveries_sent_sync(delivery_ids)

    def _mark_album_deliveries_sent_sync(
        self,
        delivery_ids: list[int],
        *,
        sent_message_ids: list[int] | None = None,
        target_id: str | None = None,
        delivery_method: str | None = None,
    ) -> None:
        from .sender_state_sync_helpers import SenderStateSyncHelpers

        return SenderStateSyncHelpers(self).mark_album_deliveries_sent_sync(
            delivery_ids,
            sent_message_ids=sent_message_ids,
            target_id=target_id,
            delivery_method=delivery_method,
        )

    def _mark_delivery_faulty_sync(self, delivery_id: int, error_text: str) -> None:
        from .sender_state_sync_helpers import SenderStateSyncHelpers

        return SenderStateSyncHelpers(self).mark_delivery_faulty_sync(
            delivery_id,
            error_text,
        )

    def _get_post_id_by_delivery_sync(self, delivery_id: int) -> int | None:
        from .sender_state_sync_helpers import SenderStateSyncHelpers

        return SenderStateSyncHelpers(self).get_post_id_by_delivery_sync(delivery_id)

    def _log_video_event_sync(
        self,
        *,
        event_type: str,
        delivery_id: int,
        rule_id: int,
        post_id: int | None,
        status: str | None = None,
        error_text: str | None = None,
        extra: dict | None = None,
    ) -> None:
        from .sender_video_logging_helpers import SenderVideoLoggingHelpers

        return SenderVideoLoggingHelpers(self).log_video_event_sync(
            event_type=event_type,
            delivery_id=delivery_id,
            rule_id=rule_id,
            post_id=post_id,
            status=status,
            error_text=error_text,
            extra=extra,
        )

    def _finalize_video_failure_sync(
        self,
        *,
        delivery_id: int,
        rule_id: int,
        post_id: int | None,
        source_channel: str,
        target_id: str,
        target_thread_id: int | None,
        source_message_id: int,
        error_text: str,
        fallback_mode: str | None = None,
        caption_delivery_mode: str | None = None,
        selected_mode: str | None = None,
        caption_requires_premium: bool | None = None,
    ) -> None:
        from .sender_video_logging_helpers import SenderVideoLoggingHelpers

        return SenderVideoLoggingHelpers(self).finalize_video_failure_sync(
            delivery_id=delivery_id,
            rule_id=rule_id,
            post_id=post_id,
            source_channel=source_channel,
            target_id=target_id,
            target_thread_id=target_thread_id,
            source_message_id=source_message_id,
            error_text=error_text,
            fallback_mode=fallback_mode,
            caption_delivery_mode=caption_delivery_mode,
            selected_mode=selected_mode,
            caption_requires_premium=caption_requires_premium,
        )

    def _finalize_video_success_sync(
        self,
        *,
        delivery_id: int,
        rule_id: int,
        post_id: int | None,
        source_channel: str,
        target_id: str,
        target_thread_id: int | None,
        source_message_id: int,
        sent_message_id: int | None,
        fallback_mode: str,
        caption_delivery_mode: str,
        selected_mode: str,
        caption_requires_premium: bool,
        candidate_sent_message_ids: list[int] | None = None,
        valid_sent_message_ids: list[int] | None = None,
    ) -> None:
        from .sender_video_logging_helpers import SenderVideoLoggingHelpers

        return SenderVideoLoggingHelpers(self).finalize_video_success_sync(
            delivery_id=delivery_id,
            rule_id=rule_id,
            post_id=post_id,
            source_channel=source_channel,
            target_id=target_id,
            target_thread_id=target_thread_id,
            source_message_id=source_message_id,
            sent_message_id=sent_message_id,
            fallback_mode=fallback_mode,
            caption_delivery_mode=caption_delivery_mode,
            selected_mode=selected_mode,
            caption_requires_premium=caption_requires_premium,
            candidate_sent_message_ids=candidate_sent_message_ids,
            valid_sent_message_ids=valid_sent_message_ids,
        )

    def _log_delivery_pipeline_step_sync(
        self,
        *,
        rule_id: int,
        delivery_ids: list[int],
        event_type: str,
        pipeline_stage: str,
        pipeline_result: str,
        source_channel: str,
        target_id: str,
        source_message_ids: list[int],
        error_text: str | None = None,
        extra: dict | None = None,
    ) -> None:
        from .sender_delivery_logging_helpers import SenderDeliveryLoggingHelpers

        return SenderDeliveryLoggingHelpers(self).log_delivery_pipeline_step_sync(
            rule_id=rule_id,
            delivery_ids=delivery_ids,
            event_type=event_type,
            pipeline_stage=pipeline_stage,
            pipeline_result=pipeline_result,
            source_channel=source_channel,
            target_id=target_id,
            source_message_ids=source_message_ids,
            error_text=error_text,
            extra=extra,
        )

    async def _log_delivery_pipeline_step(
        self,
        *,
        rule_id: int,
        delivery_ids: list[int],
        event_type: str,
        pipeline_stage: str,
        pipeline_result: str,
        source_channel: str,
        target_id: str,
        source_message_ids: list[int],
        error_text: str | None = None,
        extra: dict | None = None,
    ) -> None:
        from .sender_delivery_logging_helpers import SenderDeliveryLoggingHelpers

        return await SenderDeliveryLoggingHelpers(self).log_delivery_pipeline_step(
            rule_id=rule_id,
            delivery_ids=delivery_ids,
            event_type=event_type,
            pipeline_stage=pipeline_stage,
            pipeline_result=pipeline_result,
            source_channel=source_channel,
            target_id=target_id,
            source_message_ids=source_message_ids,
            error_text=error_text,
            extra=extra,
        )

    def _log_delivery_final_success_sync(
        self,
        *,
        rule_id: int,
        delivery_ids: list[int],
        final_method: str,
        source_channel: str,
        target_id: str,
        source_message_ids: list[int],
        sent_message_id: int | None = None,
        sent_message_ids: list[int] | None = None,
        reaction_message_id: int | None = None,
        verify_result: dict | None = None,
        extra: dict | None = None,
    ) -> None:
        from .sender_delivery_logging_helpers import SenderDeliveryLoggingHelpers

        return SenderDeliveryLoggingHelpers(self).log_delivery_final_success_sync(
            rule_id=rule_id,
            delivery_ids=delivery_ids,
            final_method=final_method,
            source_channel=source_channel,
            target_id=target_id,
            source_message_ids=source_message_ids,
            sent_message_id=sent_message_id,
            sent_message_ids=sent_message_ids,
            reaction_message_id=reaction_message_id,
            verify_result=verify_result,
            extra=extra,
        )

    async def _log_delivery_final_success(
        self,
        *,
        rule_id: int,
        delivery_ids: list[int],
        final_method: str,
        source_channel: str,
        target_id: str,
        source_message_ids: list[int],
        sent_message_id: int | None = None,
        sent_message_ids: list[int] | None = None,
        reaction_message_id: int | None = None,
        verify_result: dict | None = None,
        extra: dict | None = None,
    ) -> None:
        from .sender_delivery_logging_helpers import SenderDeliveryLoggingHelpers

        return await SenderDeliveryLoggingHelpers(self).log_delivery_final_success(
            rule_id=rule_id,
            delivery_ids=delivery_ids,
            final_method=final_method,
            source_channel=source_channel,
            target_id=target_id,
            source_message_ids=source_message_ids,
            sent_message_id=sent_message_id,
            sent_message_ids=sent_message_ids,
            reaction_message_id=reaction_message_id,
            verify_result=verify_result,
            extra=extra,
        )

    def _log_delivery_final_failure_sync(
        self,
        *,
        rule_id: int,
        delivery_ids: list[int],
        final_method: str,
        source_channel: str,
        target_id: str,
        source_message_ids: list[int],
        error_text: str,
        attempts_debug: list[dict] | None = None,
        extra: dict | None = None,
    ) -> None:
        from .sender_delivery_logging_helpers import SenderDeliveryLoggingHelpers

        return SenderDeliveryLoggingHelpers(self).log_delivery_final_failure_sync(
            rule_id=rule_id,
            delivery_ids=delivery_ids,
            final_method=final_method,
            source_channel=source_channel,
            target_id=target_id,
            source_message_ids=source_message_ids,
            error_text=error_text,
            attempts_debug=attempts_debug,
            extra=extra,
        )

    async def _log_delivery_final_failure(
        self,
        *,
        rule_id: int,
        delivery_ids: list[int],
        final_method: str,
        source_channel: str,
        target_id: str,
        source_message_ids: list[int],
        error_text: str,
        attempts_debug: list[dict] | None = None,
        extra: dict | None = None,
    ) -> None:
        from .sender_delivery_logging_helpers import SenderDeliveryLoggingHelpers

        return await SenderDeliveryLoggingHelpers(self).log_delivery_final_failure(
            rule_id=rule_id,
            delivery_ids=delivery_ids,
            final_method=final_method,
            source_channel=source_channel,
            target_id=target_id,
            source_message_ids=source_message_ids,
            error_text=error_text,
            attempts_debug=attempts_debug,
            extra=extra,
        )

    def _stage_name_ru(self, stage: str | None) -> str:
        from .sender_video_logging_helpers import SenderVideoLoggingHelpers

        return SenderVideoLoggingHelpers(self).stage_name_ru(stage)

    def _log_human_video_event(
        self,
        *,
        event_type: str,
        status: str | None = None,
        error_text: str | None = None,
        extra: dict | None = None,
    ) -> None:
        from .sender_video_logging_helpers import SenderVideoLoggingHelpers

        return SenderVideoLoggingHelpers(self).log_human_video_event(
            event_type=event_type,
            status=status,
            error_text=error_text,
            extra=extra,
        )

    def _get_rule_intro_items(self, rule):
        from .sender_state_sync_helpers import SenderStateSyncHelpers

        return SenderStateSyncHelpers(self).get_rule_intro_items(rule)

    def _is_self_loop_rule(self, rule) -> bool:
        return (
            str(rule.source_id) == str(rule.target_id)
            and rule.source_thread_id == rule.target_thread_id
        )

    def _get_post_id_by_delivery(self, delivery_id: int) -> int | None:
        from .sender_state_sync_helpers import SenderStateSyncHelpers

        return SenderStateSyncHelpers(self).get_post_id_by_delivery(delivery_id)

    def _handle_process_rule_exception_sync(
        self,
        *,
        rule_id: int,
        delivery_id: int,
        post_id: int | None,
        message_id: int,
        source_channel: str,
        target_id: str,
        target_thread_id: int | None,
        media_group_id: str | None,
        schedule_mode: str,
        interval: int,
        error_text: str,
    ) -> None:
        self.db.log_delivery_event(
            event_type="delivery_process_exception",
            delivery_id=delivery_id,
            rule_id=rule_id,
            post_id=post_id,
            status="faulty",
            error_text=error_text,
            extra={
                "message_id": message_id,
                "source_channel": source_channel,
                "target_id": target_id,
                "target_thread_id": target_thread_id,
                "media_group_id": media_group_id,
                "schedule_mode": schedule_mode,
            },
        )

        self.db.mark_delivery_faulty(delivery_id, error_text)

        if schedule_mode == "fixed":
            self.scheduler_service.touch_after_send(rule_id, interval)

    def _touch_rule_after_send_sync(self, rule_id: int, interval: int) -> None:
        self.scheduler_service.touch_after_send(rule_id, interval)

    def _prepare_album_delivery_sync(
        self,
        rule_id: int,
        source_channel: str,
        source_thread_id: int | None,
        media_group_id: str,
    ) -> dict[str, Any]:
        if self.db.is_album_already_sent(
            rule_id,
            source_channel,
            source_thread_id,
            media_group_id,
        ):
            album_rows = self.db.get_album_pending_for_rule(
                rule_id,
                source_channel,
                source_thread_id,
                media_group_id,
            )

            if album_rows:
                self.db.mark_many_deliveries_sent(
                    [int(r["delivery_id"]) for r in album_rows]
                )

            return {
                "already_sent": True,
                "album_rows": album_rows,
            }

        album_rows = self.db.get_album_pending_for_rule(
            rule_id,
            source_channel,
            source_thread_id,
            media_group_id,
        )

        if self.db.is_album_already_sent(
            rule_id,
            source_channel,
            source_thread_id,
            media_group_id,
        ):
            if album_rows:
                self.db.mark_many_deliveries_sent(
                    [int(r["delivery_id"]) for r in album_rows]
                )

            return {
                "already_sent": True,
                "album_rows": album_rows,
            }

        return {
            "already_sent": False,
            "album_rows": album_rows,
        }

    def _take_due_delivery_sync(self, rule_id: int, schedule_mode: str) -> dict[str, Any] | None:
        due = self.db.take_due_delivery(rule_id, utc_now_iso())
        if not due:
            return None

        delivery_id = int(due["delivery_id"])
        post_id = self.db.get_post_id_by_delivery(delivery_id)

        self.db.log_delivery_event(
            event_type="delivery_started",
            delivery_id=delivery_id,
            rule_id=rule_id,
            post_id=post_id,
            status="processing",
            extra={
                "message_id": int(due["message_id"]),
                "source_channel": str(due["source_channel"]),
                "target_id": str(due["target_id"]),
                "target_thread_id": due["target_thread_id"],
                "media_group_id": str(due["media_group_id"]) if due["media_group_id"] is not None else None,
                "schedule_mode": schedule_mode,
            },
        )

        return {
            "due": due,
            "post_id": post_id,
        }

    def _clone_telethon_entities(self, entities, text: str | None = None) -> list:
        from .sender_content_helpers import SenderContentHelpers

        return SenderContentHelpers(self).clone_telethon_entities(entities, text)
    async def _send_text_via_telethon(
        self,
        *,
        target_id,
        target_thread_id,
        text: str,
        entities,
    ) -> int | None:
        from .sender_telethon_helpers import SenderTelethonHelpers

        return await SenderTelethonHelpers(self).send_text_via_telethon(
            target_id=target_id,
            target_thread_id=target_thread_id,
            text=text,
            entities=entities,
        )

    async def _send_file_via_telethon(
        self,
        *,
        target_id,
        target_thread_id,
        message,
        file_path: Path | None = None,
        force_document: bool = False,
        post_row: dict | None = None,
    ) -> int | None:
        from .sender_telethon_helpers import SenderTelethonHelpers

        return await SenderTelethonHelpers(self).send_file_via_telethon(
            target_id=target_id,
            target_thread_id=target_thread_id,
            message=message,
            file_path=file_path,
            force_document=force_document,
            post_row=post_row,
        )

    async def _send_album_via_telethon(
        self,
        *,
        messages,
        target_id,
        target_thread_id,
        post_rows: list[dict] | None = None,
    ) -> dict:
        from .sender_telethon_helpers import SenderTelethonHelpers

        return await SenderTelethonHelpers(self).send_album_via_telethon(
            messages=messages,
            target_id=target_id,
            target_thread_id=target_thread_id,
            post_rows=post_rows,
        )

    def _build_video_stage_logger(
        self,
        *,
        rule,
        delivery_id: int,
        post_id: int | None,
        source_channel: str,
        target_id: str,
        source_message_id: int,
    ):
        progress_ui_last_emit_at: dict[str, float] = {}
        progress_ui_milestones: dict[str, set[int]] = {}
        progress_milestones = (0, 10, 25, 50, 75, 100)
        progress_ui_min_interval_sec = 20.0

        def should_emit_progress_ui(event_type: str, payload: dict) -> bool:
            if event_type not in {"video_download_progress", "video_ffmpeg_progress"}:
                return True

            now = time.monotonic()
            last_emit_at = progress_ui_last_emit_at.get(event_type, 0.0)
            percent_raw = payload.get("percent")
            try:
                percent = int(float(percent_raw))
            except (TypeError, ValueError):
                percent = None

            emitted = progress_ui_milestones.setdefault(event_type, set())
            if percent is not None:
                for milestone in progress_milestones:
                    if percent >= milestone and milestone not in emitted:
                        emitted.add(milestone)
                        progress_ui_last_emit_at[event_type] = now
                        return True

            if now - last_emit_at >= progress_ui_min_interval_sec:
                progress_ui_last_emit_at[event_type] = now
                return True

            return False

        def stage_logger(
            event_type: str,
            status: str | None = None,
            error_text: str | None = None,
            extra: dict | None = None,
        ):
            payload = dict(extra or {})
            payload.setdefault("source_channel", source_channel)
            payload.setdefault("target_id", target_id)
            payload.setdefault("source_message_id", source_message_id)

            if should_emit_progress_ui(event_type, payload):
                self._schedule_video_event_log(
                    event_type=event_type,
                    delivery_id=delivery_id,
                    rule_id=rule.id,
                    post_id=post_id,
                    status=status,
                    error_text=error_text,
                    extra=payload,
                )

            self._log_human_video_event(
                event_type=event_type,
                status=status,
                error_text=error_text,
                extra=payload,
            )

        return stage_logger

    def _get_album_primary_text(self, messages, post_rows: list[dict] | None = None) -> str | None:
        from .sender_content_helpers import SenderContentHelpers

        return SenderContentHelpers(self).get_album_primary_text(messages, post_rows)
    def _get_rule_video_caption_delivery_mode(self, rule) -> str:
        from .sender_content_helpers import SenderContentHelpers

        return SenderContentHelpers(self).get_rule_video_caption_delivery_mode(rule)
    def _resolve_repost_caption_delivery_strategy(
        self,
        *,
        rule,
        source_channel: str,
        message_ids: list[int],
        is_album: bool,
    ) -> dict[str, Any]:
        from .sender_content_helpers import SenderContentHelpers

        return SenderContentHelpers(self).resolve_repost_caption_delivery_strategy(rule=rule, source_channel=source_channel, message_ids=message_ids, is_album=is_album)
    def _get_rule_caption_delivery_mode(self, rule) -> str:
        from .sender_content_helpers import SenderContentHelpers

        return SenderContentHelpers(self).get_rule_caption_delivery_mode(rule)
    def _content_requires_builder(self, content: dict | None) -> bool:
        from .sender_content_helpers import SenderContentHelpers

        return SenderContentHelpers(self).content_requires_builder(content)
    def _get_post_row_for_rule_message(
        self,
        rule,
        source_channel: str,
        message_id: int,
    ) -> dict | None:
        from .sender_content_helpers import SenderContentHelpers

        return SenderContentHelpers(self).get_post_row_for_rule_message(rule, source_channel, message_id)
    def _single_requires_builder(
        self,
        rule,
        source_channel: str,
        message_id: int,
    ) -> bool | dict[str, object]:
        from .sender_content_helpers import SenderContentHelpers

        return SenderContentHelpers(self).single_requires_builder(rule, source_channel, message_id)
    def _album_requires_builder(
        self,
        rule,
        source_channel: str,
        message_ids: list[int],
    ) -> bool:
        from .sender_content_helpers import SenderContentHelpers

        return SenderContentHelpers(self).album_requires_builder(rule, source_channel, message_ids)
    async def _fetch_album_messages(self, source_channel, message_ids):
        messages = []

        for mid in message_ids:
            msg = await self._fetch_message(source_channel, mid)
            if not msg:
                break
            messages.append(msg)

        return messages

    async def _verify_album_delivery(
        self,
        *,
        target_id,
        expected_count: int,
        sent_message_ids: list[int] | None,
        target_thread_id: int | None = None,
        target_grouped_id: int | None = None,
    ):
        from .sender_telethon_helpers import SenderTelethonHelpers

        return await SenderTelethonHelpers(self).verify_album_delivery(
            target_id=target_id,
            expected_count=expected_count,
            sent_message_ids=sent_message_ids,
            target_thread_id=target_thread_id,
            target_grouped_id=target_grouped_id,
        )

    async def _verify_self_loop_video_metadata(
        self,
        *,
        rule_id,
        source_message_id,
        target_id,
        sent_message_id,
    ) -> None:
        from .sender_telethon_helpers import SenderTelethonHelpers

        return await SenderTelethonHelpers(self).verify_self_loop_video_metadata(
            rule_id=rule_id,
            source_message_id=source_message_id,
            target_id=target_id,
            sent_message_id=sent_message_id,
        )

    async def _try_add_normal_reaction(
        self,
        client,
        entity,
        sent_message_id,
        session_name: str,
        rule_id: int | None = None,
    ) -> bool:
        from .reaction_delivery import ReactionDelivery

        return await ReactionDelivery(self)._try_add_normal_reaction(
            client, entity, sent_message_id, session_name, rule_id=rule_id
        )

    async def _try_add_premium_reactions(
        self, client, entity, sent_message_id, session_name: str, fixed_reactions: list[str], rule_id: int | None = None
    ) -> bool:
        from .reaction_delivery import ReactionDelivery

        return await ReactionDelivery(self)._try_add_premium_reactions(
            client, entity, sent_message_id, session_name, fixed_reactions, rule_id=rule_id
        )

    async def _confirm_reaction(self, client, entity, message_id: int, emoji: str) -> bool:
        from .reaction_delivery import ReactionDelivery

        return await ReactionDelivery(self)._confirm_reaction(client, entity, message_id, emoji)

    async def _confirm_reaction_set(
        self, client, entity, message_id: int, emojis: list[str]
    ) -> tuple[bool, list[str]]:
        from .reaction_delivery import ReactionDelivery

        return await ReactionDelivery(self)._confirm_reaction_set(client, entity, message_id, emojis)

    async def _select_reaction_message_id(
        self, target_id, sent_message_ids: list[int] | None
    ) -> tuple[int | None, str]:
        from .reaction_delivery import ReactionDelivery

        return await ReactionDelivery(self)._select_reaction_message_id(target_id, sent_message_ids)

    async def _add_reaction_if_possible(self, target_id, sent_message_id, rule_id: int | None = None):
        from .reaction_delivery import ReactionDelivery

        return await ReactionDelivery(self)._add_reaction_if_possible(
            target_id, sent_message_id, rule_id=rule_id
        )

    async def _add_reaction_for_rule_if_possible(
        self,
        *,
        rule,
        target_id,
        sent_message_id,
        source_channel: str = "",
        source_message_ids: list[int] | None = None,
        delivery_id: int | None = None,
        max_age_seconds: int = 300,
        allow_unverified_self_loop_target: bool = False,
    ) -> dict:
        from .reaction_delivery import ReactionDelivery

        kwargs = {
            "rule": rule,
            "target_id": target_id,
            "sent_message_id": sent_message_id,
            "source_channel": source_channel,
            "source_message_ids": source_message_ids,
            "delivery_id": delivery_id,
            "max_age_seconds": max_age_seconds,
        }
        if allow_unverified_self_loop_target:
            kwargs["allow_unverified_self_loop_target"] = True
        return await ReactionDelivery(self)._add_reaction_for_rule_if_possible(**kwargs)

    async def process_rule_once(self, rule):
        schedule_mode = getattr(rule, "schedule_mode", "interval") or "interval"

        decision = TopTimeGuardService(self.db, logger=logger).build_guard_decision(rule, at_iso=utc_now_iso())
        if decision.get("blocked") is True and decision.get("resume_at"):
            resume_at = str(decision["resume_at"])
            resume_dt = datetime.fromisoformat(resume_at)
            if resume_dt.tzinfo is None:
                resume_dt = resume_dt.replace(tzinfo=timezone.utc)
            next_run_at = (resume_dt.astimezone(timezone.utc) + timedelta(seconds=GLOBAL_INTERVAL_GAP_SECONDS)).isoformat()
            await run_db(self.scheduler_service.set_next_run, int(rule.id), next_run_at)
            pause = decision.get("pause") or {}
            logger.info(
                "TOP_TIME_GUARD_BLOCKED_AUTO_POST | rule_id=%s | target_id=%s | target_thread_id=%s | pause_id=%s | resume_at=%s | next_run_at=%s",
                int(rule.id),
                str(getattr(rule, "target_id", "") or ""),
                getattr(rule, "target_thread_id", None),
                pause.get("id"),
                resume_at,
                next_run_at,
            )
            if hasattr(self.db, "log_event"):
                await run_db(
                    self.db.log_event,
                    event_type="top_time_auto_postponed",
                    rule_id=int(rule.id),
                    target_id=str(getattr(rule, "target_id", "") or ""),
                    target_thread_id=getattr(rule, "target_thread_id", None),
                    status="postponed",
                    extra={
                        "pause_id": pause.get("id"),
                        "target_id": str(getattr(rule, "target_id", "") or ""),
                        "target_thread_id": getattr(rule, "target_thread_id", None),
                        "resume_at": resume_at,
                        "next_run_at": next_run_at,
                        "reason": "top_time_pause",
                    },
                )
            return False

        taken = await run_db(self._take_due_delivery_sync, rule.id, schedule_mode)
        if not taken:
            return False

        due = taken["due"]
        post_id = taken["post_id"]

        delivery_id = int(due["delivery_id"])
        source_channel = str(due["source_channel"])
        message_id = int(due["message_id"])
        media_group_id = due["media_group_id"]
        target_id = str(due["target_id"])
        target_thread_id = due["target_thread_id"]
        interval = int(due["interval"])

        try:
            rule_mode = getattr(rule, "mode", "repost") or "repost"

            # VIDEO-РЕЖИМ:
            # Даже если сообщение пришло из альбома, обрабатываем его как отдельный пост.
            if rule_mode == "video":
                ok = await self._deliver_single_video(
                    rule,
                    delivery_id,
                    message_id,
                    source_channel,
                    target_id,
                    target_thread_id,
                )

                if ok or schedule_mode == "fixed":
                    await run_db(self._touch_rule_after_send_sync, rule.id, interval)

                return ok

            # REPOST-РЕЖИМ:
            # Старая стабильная логика альбомов сохраняется как есть.

            if media_group_id:
                prepared_album = await run_db(
                    self._prepare_album_delivery_sync,
                    rule.id,
                    source_channel,
                    due["source_thread_id"],
                    str(media_group_id),
                )

                album_rows = prepared_album["album_rows"]

                if prepared_album["already_sent"]:
                    logger.warning(
                        "⛔ Альбом media_group_id=%s уже был отправлен по правилу %s, пропускаю повторную доставку",
                        media_group_id,
                        rule.id,
                    )
                    return True

                ok = await self._deliver_album(
                    rule,
                    album_rows,
                    source_channel,
                    target_id,
                    target_thread_id,
                )

                if ok or schedule_mode == "fixed":
                    await run_db(self._touch_rule_after_send_sync, rule.id, interval)

                return ok

            # Обычный одиночный репост
            ok = await self._deliver_single(
                rule,
                delivery_id,
                message_id,
                source_channel,
                target_id,
                target_thread_id,
            )

            if ok or schedule_mode == "fixed":
                await run_db(self._touch_rule_after_send_sync, rule.id, interval)

            return ok

        except Exception as exc:
            logger.exception("Ошибка доставки rule=%s delivery=%s", rule.id, delivery_id)

            await run_db(
                self._handle_process_rule_exception_sync,
                rule_id=rule.id,
                delivery_id=delivery_id,
                post_id=post_id,
                message_id=message_id,
                source_channel=source_channel,
                target_id=target_id,
                target_thread_id=target_thread_id,
                media_group_id=str(media_group_id) if media_group_id is not None else None,
                schedule_mode=schedule_mode,
                interval=interval,
                error_text=str(exc),
            )
            return False

    async def execute_repost_single_from_job(
        self,
        *,
        rule_id: int,
        delivery_id: int,
        message_id: int,
        source_channel: str,
        source_thread_id: int | None = None,
        target_id: str,
        target_thread_id: int | None = None,
        mode: str = "repost",
        interval: int = 0,
        schedule_mode: str = "interval",
        media_group_id: str | None = None,
        job_type: str | None = None,
    ) -> bool:
        normalized_schedule_mode = str(schedule_mode or "interval")
        normalized_mode = str(mode or "repost")
        logger.info(
            "JOB EXECUTOR | repost_single | start | rule_id=%s | delivery_id=%s | message_id=%s | mode=%s | schedule_mode=%s",
            rule_id,
            delivery_id,
            message_id,
            normalized_mode,
            normalized_schedule_mode,
        )
        idempotency_key = build_delivery_idempotency_key(operation_kind="single", delivery_id=int(delivery_id), target_id=str(target_id))
        attempt = await run_db(self.db.get_delivery_attempt_by_idempotency_key, idempotency_key)
        cached_ids = extract_sent_message_ids_from_attempt(attempt)
        if isinstance(attempt, dict) and str(attempt.get("status") or "") in {"accepted", "verified"} and cached_ids:
            logger.info("DELIVERY_ATTEMPT_CACHE_HIT | operation=single | key=%s | delivery_id=%s | sent_message_ids=%s", idempotency_key, delivery_id, cached_ids)
            await run_db(self._mark_delivery_sent_sync, int(delivery_id), sent_message_id=int(cached_ids[0]), sent_message_ids=cached_ids, target_id=str(target_id), delivery_method="idempotency_cache")
            return True
        await run_db(self.db.create_delivery_attempt, delivery_id=int(delivery_id), rule_id=int(rule_id), tenant_id=1, job_id=None, idempotency_key=idempotency_key, operation_kind="single", status="created", target_id=str(target_id), source_message_ids=[int(message_id)])
        logger.info("DELIVERY_ATTEMPT_CREATED | operation=single | key=%s | delivery_id=%s", idempotency_key, delivery_id)
        await run_db(self.db.mark_delivery_attempt_sending, idempotency_key, job_id=None, telegram_method="copy_single")
        logger.info("DELIVERY_ATTEMPT_SENDING | operation=single | key=%s | delivery_id=%s", idempotency_key, delivery_id)
        try:
            rule = await run_db(self.db.get_rule, int(rule_id))
            if not rule:
                raise RuntimeError(f"Правило #{rule_id} не найдено для repost_single")
            ok = await self._deliver_single(
                rule,
                int(delivery_id),
                int(message_id),
                str(source_channel),
                str(target_id),
                target_thread_id,
                idempotency_key=idempotency_key,
            )
            if ok or normalized_schedule_mode == "fixed":
                await run_db(self._touch_rule_after_send_sync, int(rule_id), int(interval))
            if ok:
                logger.info(
                    "JOB EXECUTOR | repost_single | success | rule_id=%s | delivery_id=%s",
                    rule_id,
                    delivery_id,
                )
            else:
                await run_db(self.db.mark_delivery_attempt_failed, idempotency_key, status="failed_before_send", error_text="executor returned unsuccessful result")
                logger.info("DELIVERY_ATTEMPT_FAILED_BEFORE_SEND | operation=single | key=%s | delivery_id=%s", idempotency_key, delivery_id)
                delivery_row = await run_db(self.db.get_delivery, int(delivery_id))
                uncertain_error = "copy_single_uncertain_no_fallback"
                if (
                    isinstance(delivery_row, dict)
                    and str(delivery_row.get("status") or "") == "faulty"
                    and uncertain_error in str(delivery_row.get("error_text") or "")
                ):
                    logger.warning(
                        "JOB EXECUTOR | repost_single | non_retryable_uncertain | rule_id=%s | delivery_id=%s",
                        rule_id,
                        delivery_id,
                    )
                    return {"ok": False, "retryable": False, "error_text": str(delivery_row.get("error_text") or uncertain_error)}
                logger.warning(
                    "JOB EXECUTOR | repost_single | failed | rule_id=%s | delivery_id=%s | error=исполнитель вернул неуспешный результат",
                    rule_id,
                    delivery_id,
                )
            return bool(ok)
        except Exception as exc:
            await run_db(self.db.mark_delivery_attempt_failed, idempotency_key, status="failed_before_send", error_text=str(exc))
            logger.info("DELIVERY_ATTEMPT_FAILED_BEFORE_SEND | operation=single | key=%s | delivery_id=%s", idempotency_key, delivery_id)
            logger.warning(
                "JOB EXECUTOR | repost_single | failed | rule_id=%s | delivery_id=%s | error=%s",
                rule_id,
                delivery_id,
                exc,
            )
            logger.exception(
                "JOB EXECUTOR | repost_single | ошибка выполнения rule_id=%s delivery_id=%s",
                rule_id,
                delivery_id,
            )
            raise

    async def execute_repost_album_from_job(
        self,
        *,
        rule_id: int,
        delivery_id: int | None = None,
        message_id: int | None = None,
        source_channel: str,
        source_thread_id: int | None,
        media_group_id: str | None = None,
        target_id: str,
        target_thread_id: int | None,
        mode: str = "repost",
        interval: int = 0,
        schedule_mode: str = "interval",
        job_type: str | None = None,
        delivery_ids: list[int] | None = None,
    ) -> bool:
        normalized_schedule_mode = str(schedule_mode or "interval")
        normalized_mode = str(mode or "repost")
        logger.info(
            "JOB EXECUTOR | repost_album | start | rule_id=%s | delivery_id=%s | message_id=%s | mode=%s | schedule_mode=%s",
            rule_id,
            delivery_id,
            message_id,
            normalized_mode,
            normalized_schedule_mode,
        )
        album_delivery_ids = [int(x) for x in (delivery_ids or [])]
        if not album_delivery_ids and delivery_id is not None:
            album_delivery_ids = [int(delivery_id)]
        album_source_ids = list(album_delivery_ids)
        idempotency_key = build_delivery_idempotency_key(operation_kind="album", rule_id=int(rule_id), target_id=str(target_id), media_group_id=media_group_id, source_message_ids=album_source_ids)
        attempt = await run_db(self.db.get_delivery_attempt_by_idempotency_key, idempotency_key)
        cached_ids = extract_sent_message_ids_from_attempt(attempt)
        if isinstance(attempt, dict) and str(attempt.get("status") or "") in {"accepted", "verified"} and cached_ids:
            logger.info("DELIVERY_ATTEMPT_CACHE_HIT | operation=album | key=%s | sent_message_ids=%s", idempotency_key, cached_ids)
            await run_db(
                self._mark_album_deliveries_sent_sync,
                album_delivery_ids,
                sent_message_ids=cached_ids,
                target_id=str(target_id),
                delivery_method="idempotency_cache",
            )
            logger.info(
                "DELIVERY_ATTEMPT_CACHE_HIT | operation=album | marked deliveries sent | rule_id=%s | delivery_ids=%s",
                rule_id,
                album_delivery_ids,
            )
            return True
        await run_db(self.db.create_delivery_attempt, delivery_id=int(delivery_id or 0), rule_id=int(rule_id), tenant_id=1, job_id=None, idempotency_key=idempotency_key, operation_kind="album", status="created", target_id=str(target_id), source_message_ids=album_source_ids)
        logger.info("DELIVERY_ATTEMPT_CREATED | operation=album | key=%s | delivery_id=%s", idempotency_key, delivery_id)
        await run_db(self.db.mark_delivery_attempt_sending, idempotency_key, job_id=None, telegram_method="copy_album")
        logger.info("DELIVERY_ATTEMPT_SENDING | operation=album | key=%s | delivery_id=%s", idempotency_key, delivery_id)
        try:
            rule = await run_db(self.db.get_rule, int(rule_id))
            if not rule:
                raise RuntimeError(f"Правило #{rule_id} не найдено для repost_album")
            if not media_group_id:
                raise RuntimeError("Не указан media_group_id для repost_album")

            album_rows = await run_db(
                self.db.get_processing_album_for_rule,
                int(rule_id),
                str(source_channel),
                source_thread_id,
                str(media_group_id),
            )
            if not album_rows:
                raise RuntimeError(f"Не найден processing-альбом media_group_id={media_group_id}")
            album_delivery_ids = [int(r["delivery_id"]) for r in album_rows] or album_delivery_ids

            ok = await self._deliver_album(
                rule,
                album_rows,
                str(source_channel),
                str(target_id),
                target_thread_id,
                idempotency_key=idempotency_key,
            )
            if ok or normalized_schedule_mode == "fixed":
                await run_db(self._touch_rule_after_send_sync, int(rule_id), int(interval))
            if ok:
                latest_attempt = await run_db(self.db.get_delivery_attempt_by_idempotency_key, idempotency_key)
                sent_ids = extract_sent_message_ids_from_attempt(latest_attempt)
                try:
                    await run_db(
                        self._mark_album_deliveries_sent_sync,
                        album_delivery_ids,
                        sent_message_ids=sent_ids,
                        target_id=str(target_id),
                        delivery_method="repost_album",
                    )
                except Exception as mark_exc:
                    logger.warning(
                        "JOB EXECUTOR | repost_album | cannot mark deliveries sent | rule_id=%s | delivery_ids=%s | error=%s",
                        rule_id,
                        album_delivery_ids,
                        mark_exc,
                    )
                    raise
                logger.info(
                    "JOB EXECUTOR | repost_album | marked deliveries sent | rule_id=%s | delivery_ids=%s",
                    rule_id,
                    album_delivery_ids,
                )
                logger.info(
                    "JOB EXECUTOR | repost_album | success | rule_id=%s | delivery_id=%s | delivery_ids=%s",
                    rule_id,
                    delivery_id,
                    album_delivery_ids,
                )
            else:
                await run_db(self.db.mark_delivery_attempt_failed, idempotency_key, status="failed_before_send", error_text="executor returned unsuccessful result")
                logger.info("DELIVERY_ATTEMPT_FAILED_BEFORE_SEND | operation=album | key=%s | delivery_id=%s", idempotency_key, delivery_id)
                logger.warning(
                    "JOB EXECUTOR | repost_album | failed | rule_id=%s | delivery_id=%s | error=исполнитель вернул неуспешный результат",
                    rule_id,
                    delivery_id,
                )
            return bool(ok)
        except Exception as exc:
            await run_db(self.db.mark_delivery_attempt_failed, idempotency_key, status="failed_before_send", error_text=str(exc))
            logger.info("DELIVERY_ATTEMPT_FAILED_BEFORE_SEND | operation=album | key=%s | delivery_id=%s", idempotency_key, delivery_id)
            logger.warning(
                "JOB EXECUTOR | repost_album | failed | rule_id=%s | delivery_id=%s | error=%s",
                rule_id,
                delivery_id,
                exc,
            )
            logger.exception(
                "JOB EXECUTOR | repost_album | ошибка выполнения rule_id=%s delivery_id=%s",
                rule_id,
                delivery_id,
            )
            raise

    async def execute_video_delivery_from_job(
        self,
        *,
        rule_id: int,
        delivery_id: int,
        message_id: int,
        source_channel: str,
        source_thread_id: int | None = None,
        target_id: str,
        target_thread_id: int | None = None,
        mode: str = "video",
        interval: int = 0,
        schedule_mode: str = "interval",
        media_group_id: str | None = None,
        job_type: str | None = None,
    ) -> bool:
        normalized_schedule_mode = str(schedule_mode or "interval")
        normalized_mode = str(mode or "video")
        idempotency_key = build_delivery_idempotency_key(operation_kind="video_send", delivery_id=int(delivery_id), target_id=str(target_id))
        logger.info(
            "JOB EXECUTOR | video_delivery | start | rule_id=%s | delivery_id=%s | message_id=%s | mode=%s | schedule_mode=%s",
            rule_id,
            delivery_id,
            message_id,
            normalized_mode,
            normalized_schedule_mode,
        )
        has_attempt_ledger = all(
            hasattr(self.db, name)
            for name in (
                "get_delivery_attempt_by_idempotency_key",
                "create_delivery_attempt",
                "mark_delivery_attempt_sending",
                "mark_delivery_attempt_accepted",
                "mark_delivery_attempt_failed",
            )
        )
        try:
            attempt = await run_db(self.db.get_delivery_attempt_by_idempotency_key, idempotency_key) if has_attempt_ledger else None
            cached_sent_ids = extract_sent_message_ids_from_attempt(attempt)
            if attempt and str(attempt.get("status") or "").lower() in {"accepted", "verified"} and cached_sent_ids:
                logger.info(
                    "DELIVERY_ATTEMPT_CACHE_HIT | operation=video_send | job_type=video_delivery | delivery_id=%s | rule_id=%s | target_id=%s | idempotency_key=%s | sent_message_ids=%s",
                    delivery_id,
                    rule_id,
                    target_id,
                    idempotency_key,
                    cached_sent_ids,
                )
                await run_db(self._mark_delivery_sent_sync, int(delivery_id), sent_message_id=int(cached_sent_ids[0]), sent_message_ids=cached_sent_ids, target_id=str(target_id), delivery_method="idempotency_cache")
                return True

            if has_attempt_ledger:
                await run_db(self.db.create_delivery_attempt, delivery_id=int(delivery_id), rule_id=int(rule_id), tenant_id=1, job_id=None, idempotency_key=idempotency_key, operation_kind="video_send", status="created", telegram_method=None, target_id=str(target_id), source_message_ids=[int(message_id)] if message_id else None, sent_message_ids=None, error_text=None)
                logger.info("DELIVERY_ATTEMPT_CREATED | operation=video_send | job_type=video_delivery | delivery_id=%s | rule_id=%s | target_id=%s | idempotency_key=%s", delivery_id, rule_id, target_id, idempotency_key)
                await run_db(self.db.mark_delivery_attempt_sending, idempotency_key, job_id=None, telegram_method="video_delivery")
                logger.info("DELIVERY_ATTEMPT_SENDING | operation=video_send | job_type=video_delivery | delivery_id=%s | rule_id=%s | target_id=%s | idempotency_key=%s", delivery_id, rule_id, target_id, idempotency_key)

            rule = await run_db(self.db.get_rule, int(rule_id))
            if not rule:
                raise RuntimeError(f"Правило #{rule_id} не найдено для video_delivery")
            outcome = await self._deliver_single_video(rule, int(delivery_id), int(message_id), str(source_channel), str(target_id), target_thread_id)
            ok = bool(outcome.get("ok")) if isinstance(outcome, dict) else bool(outcome)
            raw_sent_ids = outcome.get("sent_message_ids") if isinstance(outcome, dict) else None
            valid_sent_ids = normalize_valid_sent_message_ids(raw_sent_ids)
            post_send_accepted = bool(valid_sent_ids)
            if valid_sent_ids and has_attempt_ledger:
                await run_db(self.db.mark_delivery_attempt_accepted, idempotency_key, sent_message_ids=valid_sent_ids, telegram_method="video_delivery")
                logger.info("DELIVERY_ATTEMPT_ACCEPTED | operation=video_send | job_type=video_delivery | delivery_id=%s | rule_id=%s | target_id=%s | idempotency_key=%s | sent_message_ids=%s", delivery_id, rule_id, target_id, idempotency_key, valid_sent_ids)
            elif raw_sent_ids is not None and has_attempt_ledger:
                logger.warning("DELIVERY_ATTEMPT_ACCEPTED_SKIPPED_INVALID_IDS | operation=video_send | job_type=video_delivery | delivery_id=%s | idempotency_key=%s | raw_sent_message_ids=%s", delivery_id, idempotency_key, raw_sent_ids)
            if post_send_accepted and not ok:
                logger.warning(
                    "DELIVERY_SENT_UNVERIFIED_AFTER_ACCEPTED | operation=video_send | job_type=video_delivery | delivery_id=%s | rule_id=%s | target_id=%s | sent_message_ids=%s",
                    delivery_id,
                    rule_id,
                    target_id,
                    valid_sent_ids,
                )
                ok = True
                await run_db(
                    self._mark_delivery_sent_sync,
                    int(delivery_id),
                    sent_message_id=int(valid_sent_ids[0]),
                    sent_message_ids=valid_sent_ids,
                    target_id=str(target_id),
                    delivery_method="video_delivery_unverified",
                )
            if ok or normalized_schedule_mode == "fixed":
                await run_db(self._touch_rule_after_send_sync, int(rule_id), int(interval))
            if not ok and has_attempt_ledger:
                await run_db(self.db.mark_delivery_attempt_failed, idempotency_key, status="failed_before_send", error_text="executor returned unsuccessful result")
                logger.info("DELIVERY_ATTEMPT_FAILED_BEFORE_SEND | operation=video_send | job_type=video_delivery | delivery_id=%s | rule_id=%s | target_id=%s | idempotency_key=%s", delivery_id, rule_id, target_id, idempotency_key)
            if ok:
                logger.info("JOB EXECUTOR | video_delivery | success | rule_id=%s | delivery_id=%s", rule_id, delivery_id)
            else:
                logger.warning("JOB EXECUTOR | video_delivery | failed | rule_id=%s | delivery_id=%s | error=исполнитель вернул неуспешный результат", rule_id, delivery_id)
            return bool(ok)
        except Exception as exc:
            if has_attempt_ledger:
                await run_db(self.db.mark_delivery_attempt_failed, idempotency_key, status="failed_before_send", error_text=str(exc))
                logger.info("DELIVERY_ATTEMPT_FAILED_BEFORE_SEND | operation=video_send | job_type=video_delivery | delivery_id=%s | rule_id=%s | target_id=%s | idempotency_key=%s", delivery_id, rule_id, target_id, idempotency_key)
            logger.warning(
                "JOB EXECUTOR | video_delivery | failed | rule_id=%s | delivery_id=%s | error=%s",
                rule_id,
                delivery_id,
                exc,
            )
            logger.exception(
                "JOB EXECUTOR | video_delivery | ошибка выполнения rule_id=%s delivery_id=%s",
                rule_id,
                delivery_id,
            )
            raise

    async def execute_video_download_from_job(
        self,
        *,
        job_id: int | None = None,
        job_attempt: int | None = None,
        rule_id: int,
        delivery_id: int,
        message_id: int,
        source_channel: str,
        target_id: str,
        invalid_file_attempts: int | None = None,
        **payload: object,
    ) -> dict:
        from .video_pipeline_stages import VideoPipelineStages

        return await VideoPipelineStages(self).execute_download_from_job(
            job_id=job_id, job_attempt=job_attempt, rule_id=rule_id, delivery_id=delivery_id,
            message_id=message_id, source_channel=source_channel, target_id=target_id,
            invalid_file_attempts=invalid_file_attempts, **payload,
        )

    async def execute_video_process_from_job(
        self,
        *,
        job_id: int | None = None,
        job_attempt: int | None = None,
        rule_id: int,
        delivery_id: int,
        source_video_path: str | None = None,
        artifact_version: int | None = None,
        invalid_file_attempts: int | None = None,
        **payload: object,
    ) -> dict:
        from .video_pipeline_stages import VideoPipelineStages

        return await VideoPipelineStages(self).execute_process_from_job(
            job_id=job_id, job_attempt=job_attempt, rule_id=rule_id, delivery_id=delivery_id,
            source_video_path=source_video_path, artifact_version=artifact_version,
            invalid_file_attempts=invalid_file_attempts, **payload,
        )

    async def _validate_mp4_file_for_pipeline(
        self,
        file_path: Path,
        *,
        delivery_id: int,
        job_id: int | None,
        stage: str,
    ) -> tuple[bool, str | None]:
        from .video_pipeline_stages import VideoPipelineStages

        return await VideoPipelineStages(self).validate_mp4_file_for_pipeline(
            file_path, delivery_id=delivery_id, job_id=job_id, stage=stage,
        )

    async def execute_video_send_from_job(
        self,
        *,
        processed_video_path: str | None = None,
        thumbnail_path: str | None = None,
        **payload: object,
    ) -> dict:
        return await self.execute_video_send_from_processed_job(
            processed_video_path=processed_video_path or payload.get("processed_file_path"),
            thumbnail_path=thumbnail_path,
            **payload,
        )

    async def execute_video_send_from_processed_job(
        self,
        *,
        processed_video_path: str | None = None,
        thumbnail_path: str | None = None,
        artifact_version: int | None = None,
        pipeline_version: int | None = None,
        **payload: object,
    ) -> dict:
        from .video_send_delivery import VideoSendDelivery

        return await VideoSendDelivery(self).execute_from_processed_job(
            processed_video_path=processed_video_path,
            thumbnail_path=thumbnail_path,
            artifact_version=artifact_version,
            pipeline_version=pipeline_version,
            **payload,
        )

    async def _deliver_single(self, rule, delivery_id, message_id, source_channel, target_id, target_thread_id, idempotency_key: str | None = None):
        from .repost_single_delivery import RepostSingleDelivery

        return await RepostSingleDelivery(self).deliver(
            rule,
            delivery_id,
            message_id,
            source_channel,
            target_id,
            target_thread_id,
            idempotency_key=idempotency_key,
        )

    async def _deliver_single_video(self, rule, delivery_id, message_id, source_channel, target_id, target_thread_id):
        from .video_single_delivery import VideoSingleDelivery

        return await VideoSingleDelivery(self).deliver(
            rule,
            delivery_id,
            message_id,
            source_channel,
            target_id,
            target_thread_id,
        )

    async def _deliver_album(self, rule, album_rows, source_channel, target_id, target_thread_id, idempotency_key: str | None = None):
        from .repost_album_delivery import RepostAlbumDelivery

        return await RepostAlbumDelivery(self).deliver(
            rule,
            album_rows,
            source_channel,
            target_id,
            target_thread_id,
            idempotency_key=idempotency_key,
        )

    async def _copy_single_via_bot(self, source_channel, target_id, message_id, target_thread_id):
        from .sender_botapi_copy_helpers import SenderBotApiCopyHelpers

        return await SenderBotApiCopyHelpers(self).copy_single_via_bot(
            source_channel,
            target_id,
            message_id,
            target_thread_id,
        )

    async def _copy_album_via_bot(self, source_channel, target_id, message_ids, target_thread_id):
        from .sender_botapi_copy_helpers import SenderBotApiCopyHelpers

        return await SenderBotApiCopyHelpers(self).copy_album_via_bot(
            source_channel,
            target_id,
            message_ids,
            target_thread_id,
        )

    async def _send_album_one_by_one(self, messages, target_id, target_thread_id, post_rows: list[dict] | None = None):
        from .sender_reupload_helpers import SenderReuploadHelpers

        return await SenderReuploadHelpers(self).send_album_one_by_one(
            messages,
            target_id,
            target_thread_id,
            post_rows=post_rows,
        )

    async def _reupload_album(self, messages, target_id, target_thread_id, post_rows: list[dict] | None = None):
        from .sender_reupload_helpers import SenderReuploadHelpers

        return await SenderReuploadHelpers(self).reupload_album(
            messages,
            target_id,
            target_thread_id,
            post_rows=post_rows,
        )

    async def _fetch_message(self, source_channel, message_id):
        from .sender_fetch_download_helpers import SenderFetchDownloadHelpers

        return await SenderFetchDownloadHelpers(self).fetch_message(
            source_channel,
            message_id,
        )

    async def _download_video_source(
        self,
        message,
        *,
        delivery_id: int | None = None,
        rule_id: int | None = None,
        post_id: int | None = None,
        source_channel: str | None = None,
        target_id: str | None = None,
        source_message_id: int | None = None,
    ):
        from .sender_fetch_download_helpers import SenderFetchDownloadHelpers

        return await SenderFetchDownloadHelpers(self).download_video_source(
            message,
            delivery_id=delivery_id,
            rule_id=rule_id,
            post_id=post_id,
            source_channel=source_channel,
            target_id=target_id,
            source_message_id=source_message_id,
        )

    async def _reupload_message(self, message, target_id, target_thread_id, post_row: dict | None = None):
        from .sender_reupload_helpers import SenderReuploadHelpers

        return await SenderReuploadHelpers(self).reupload_message(
            message,
            target_id,
            target_thread_id,
            post_row=post_row,
        )

    async def execute_repost_campaign_send_copy_from_job(self, *, copy_id: int, **kwargs) -> dict:
        from .sender_campaign_copy_helpers import SenderCampaignCopyHelpers

        return await SenderCampaignCopyHelpers(self).execute_send_copy_from_job(
            copy_id=copy_id,
            **kwargs,
        )

    async def execute_repost_campaign_delete_copy_from_job(self, *, copy_id: int, **kwargs) -> dict:
        from .sender_campaign_copy_helpers import SenderCampaignCopyHelpers

        return await SenderCampaignCopyHelpers(self).execute_delete_copy_from_job(
            copy_id=copy_id,
            **kwargs,
        )
