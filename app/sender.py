from __future__ import annotations
import asyncio
import logging, mimetypes, time
from datetime import datetime, timezone, timedelta
from typing import Any
from telethon.tl import types as tl_types
from pathlib import Path
from aiogram.methods import CopyMessages
from aiogram.types import FSInputFile, InputMediaDocument, InputMediaPhoto, InputMediaVideo
from .config import settings
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
from .telegram_send_result import telegram_send_result_from_raw
from .runtime_utils import run_db
from .sender_primitives import (
    MAX_INVALID_MP4_RETRY,
    MAX_NORMAL_REACTION_ATTEMPTS,
    REACTION_POOL,
    NORMAL_REACTION_POOL,
    DEBUG_FORCE_DISABLE_BOTAPI_FALLBACK,
    DEBUG_FORCE_SKIP_COPY_SINGLE,
    DEBUG_FORCE_SKIP_COPY_ALBUM,
    _telethon_entities_to_bot,
    _build_text_with_entities,
    _utf16_text_length,
    _is_valid_entity_range_utf16,
    _format_bytes_ru,
    _format_speed_ru,
    _format_eta_ru,
    _normalize_source_text,
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
        ids = self._extract_sent_message_ids(sent_msg)
        return ids[0] if ids else None

    def _extract_sent_message_ids(self, sent_result) -> list[int]:
        def _extract_one(item) -> list[int]:
            if item is None:
                return []
            if isinstance(item, (list, tuple)):
                nested: list[int] = []
                for nested_item in item:
                    nested.extend(_extract_one(nested_item))
                return nested
            if isinstance(item, dict):
                values = []
                for key in ("message_id", "id"):
                    val = item.get(key)
                    if val is not None:
                        try:
                            values.append(int(val))
                        except Exception:
                            pass
                if values:
                    return values
                for nested_key in ("message", "result", "data"):
                    if nested_key in item:
                        nested_values = _extract_one(item.get(nested_key))
                        if nested_values:
                            return nested_values
                return values
            for attr in ("message_id", "id"):
                try:
                    val = getattr(item, attr, None)
                    if val is not None:
                        return [int(val)]
                except Exception:
                    continue
            for key in ("message", "result", "data"):
                nested_obj = getattr(item, key, None)
                if nested_obj is not None:
                    nested_values = _extract_one(nested_obj)
                    if nested_values:
                        return nested_values
            return []

        if sent_result is None:
            return []
        raw_items = list(sent_result) if isinstance(sent_result, (list, tuple)) else [sent_result]
        ids: list[int] = []
        for item in raw_items:
            ids.extend(_extract_one(item))
        return [x for x in ids if isinstance(x, int)]

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
        from .reaction_delivery import ReactionDelivery

        return await ReactionDelivery(self)._validate_reaction_target_message(
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
        normalized_candidates: list[int] = []
        for value in candidate_sent_message_ids or []:
            try:
                normalized_candidates.append(int(value))
            except Exception:
                continue

        logger.info(
            "DELIVERY_SENT_MESSAGE_IDS_VALIDATE_START | rule_id=%s | delivery_id=%s | method=%s | target_id=%s | candidate_sent_message_ids=%s",
            rule_id,
            delivery_id,
            method,
            target_id,
            normalized_candidates,
        )

        valid_ids: list[int] = []
        for candidate_id in normalized_candidates:
            try:
                validated = await self._validate_reaction_target_message(
                    rule_id=rule_id,
                    source_channel=str(source_channel or ""),
                    target_id=str(target_id),
                    source_message_ids=source_message_ids or [],
                    sent_message_id=candidate_id,
                    delivery_id=delivery_id,
                    max_age_seconds=max_age_seconds,
                )
            except Exception as exc:
                logger.warning(
                    "DELIVERY_SENT_MESSAGE_IDS_VALIDATE_ITEM_FAILED | rule_id=%s | delivery_id=%s | method=%s | target_id=%s | sent_message_id=%s | error=%s",
                    rule_id,
                    delivery_id,
                    method,
                    target_id,
                    candidate_id,
                    exc,
                )
                continue
            if validated:
                valid_ids.append(int(validated))

        if valid_ids:
            logger.info(
                "DELIVERY_SENT_MESSAGE_IDS_VALIDATE_OK | rule_id=%s | delivery_id=%s | method=%s | target_id=%s | valid_sent_message_ids=%s",
                rule_id,
                delivery_id,
                method,
                target_id,
                valid_ids,
            )
            return valid_ids

        reason = "no_candidate_ids" if not normalized_candidates else "all_candidates_rejected"
        logger.warning(
            "DELIVERY_SENT_MESSAGE_IDS_VALIDATE_EMPTY | rule_id=%s | delivery_id=%s | method=%s | target_id=%s | candidate_sent_message_ids=%s | reason=%s",
            rule_id,
            delivery_id,
            method,
            target_id,
            normalized_candidates,
            reason,
        )
        return []

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
        normalized_candidates: list[int] = []
        for value in candidate_sent_message_ids or []:
            try:
                normalized_candidates.append(int(value))
            except Exception:
                continue

        logger.info(
            "DELIVERY_TARGET_CONFIRM_START | rule_id=%s | delivery_id=%s | method=%s | source_channel=%s | target_id=%s | source_message_ids=%s | candidate_sent_message_ids=%s",
            rule_id,
            delivery_id,
            method,
            source_channel,
            target_id,
            source_message_ids,
            normalized_candidates,
        )

        if not hasattr(self.telethon, "get_messages"):
            logger.warning(
                "DELIVERY_TARGET_CONFIRM_SKIPPED_NO_GET_MESSAGES | rule_id=%s | delivery_id=%s | method=%s | target_id=%s | candidate_sent_message_ids=%s",
                rule_id,
                delivery_id,
                method,
                target_id,
                normalized_candidates,
            )
            return normalized_candidates

        if not normalized_candidates:
            logger.warning(
                "DELIVERY_TARGET_CONFIRM_FAILED | rule_id=%s | delivery_id=%s | method=%s | source_channel=%s | target_id=%s | source_message_ids=%s | candidate_sent_message_ids=%s | reason=%s",
                rule_id, delivery_id, method, source_channel, target_id, source_message_ids, normalized_candidates, "no_candidate_ids"
            )
            return []

        valid_ids = await self._validate_sent_message_ids_for_delivery(
            rule_id=rule_id,
            delivery_id=delivery_id,
            source_channel=source_channel,
            target_id=target_id,
            source_message_ids=source_message_ids,
            candidate_sent_message_ids=normalized_candidates,
            method=method,
            max_age_seconds=max_age_seconds,
        )

        if not valid_ids:
            logger.warning(
                "DELIVERY_TARGET_CONFIRM_FAILED | rule_id=%s | delivery_id=%s | method=%s | source_channel=%s | target_id=%s | source_message_ids=%s | candidate_sent_message_ids=%s | reason=%s",
                rule_id, delivery_id, method, source_channel, target_id, source_message_ids, normalized_candidates, "all_candidates_rejected"
            )
            return []

        logger.info(
            "DELIVERY_TARGET_CONFIRM_OK | rule_id=%s | delivery_id=%s | method=%s | target_id=%s | valid_sent_message_ids=%s",
            rule_id,
            delivery_id,
            method,
            target_id,
            valid_ids,
        )
        return valid_ids

    async def _confirm_target_delivery_message_ids_with_retry(
        self,
        **kwargs,
    ) -> list[int]:
        attempts = (0.0, 0.7, 1.5)
        last_reason = "target_message_not_found_after_send"
        for attempt_no, delay_seconds in enumerate(attempts, start=1):
            if delay_seconds > 0:
                logger.info(
                    "DELIVERY_TARGET_CONFIRM_RETRY | rule_id=%s | delivery_id=%s | attempt=%s | delay=%s | candidate_sent_message_ids=%s | reason=%s",
                    kwargs.get("rule_id"), kwargs.get("delivery_id"), attempt_no, delay_seconds, kwargs.get("candidate_sent_message_ids"), last_reason
                )
                await asyncio.sleep(delay_seconds)
            valid_ids = await self._confirm_target_delivery_message_ids(**kwargs)
            if valid_ids:
                return valid_ids
        return []

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
        try:
            result = await coro_factory()
            return {"ok": True, "result": result}
        except Exception as exc:
            logger.warning(
                "POST_SEND_STEP_FAILED_NON_FATAL | step_name=%s | rule_id=%s | delivery_id=%s | idempotency_key=%s | accepted_sent_message_ids=%s | error=%s",
                step_name,
                rule_id,
                delivery_id,
                idempotency_key,
                accepted_sent_message_ids or [],
                exc,
            )
            return {"ok": False, "error": str(exc)}


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
        payload = dict(verify_result or {})
        return {
            "ok": bool(payload.get("ok")),
            "error_text": payload.get("error_text"),
            "grouped_id": payload.get("grouped_id"),
            "count": payload.get("count"),
            "first_message_id": payload.get("first_message_id"),
        }

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
    ) -> None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return

        loop.create_task(
            run_db(
                self._log_video_event_sync,
                event_type=event_type,
                delivery_id=delivery_id,
                rule_id=rule_id,
                post_id=post_id,
                status=status,
                error_text=error_text,
                extra=extra,
            )
        )

    def _get_post_row_for_rule_message_sync(
        self,
        rule,
        source_channel: str,
        message_id: int,
    ) -> dict | None:
        return self._get_post_row_for_rule_message(rule, source_channel, message_id)

    def _get_rule_intro_items_sync(self, rule):
        return self._get_rule_intro_items(rule)

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

    def _mark_delivery_sent_sync(self, delivery_id: int, *, sent_message_id: int | None = None, sent_message_ids: list[int] | None = None, target_id: str | None = None, delivery_method: str | None = None) -> None:
        if hasattr(self.db, "mark_delivery_sent_with_target_message"):
            self.db.mark_delivery_sent_with_target_message(delivery_id, sent_message_id=sent_message_id, sent_message_ids=sent_message_ids, target_id=target_id, delivery_method=delivery_method)
            return
        self.db.mark_delivery_sent(delivery_id)

    def _mark_many_deliveries_sent_sync(self, delivery_ids: list[int]) -> None:
        self.db.mark_many_deliveries_sent(delivery_ids)

    def _mark_album_deliveries_sent_sync(
        self,
        delivery_ids: list[int],
        *,
        sent_message_ids: list[int] | None = None,
        target_id: str | None = None,
        delivery_method: str | None = None,
    ) -> None:
        normalized_delivery_ids = [int(x) for x in (delivery_ids or [])]
        if not normalized_delivery_ids:
            raise RuntimeError("Не удалось определить deliveries альбома для перевода в sent")

        valid_sent_message_ids = normalize_valid_sent_message_ids(sent_message_ids)
        if valid_sent_message_ids and hasattr(self.db, "mark_delivery_sent_with_target_message"):
            for index, album_delivery_id in enumerate(normalized_delivery_ids):
                sent_message_id = valid_sent_message_ids[index] if index < len(valid_sent_message_ids) else valid_sent_message_ids[0]
                self.db.mark_delivery_sent_with_target_message(
                    album_delivery_id,
                    sent_message_id=int(sent_message_id),
                    sent_message_ids=valid_sent_message_ids,
                    target_id=target_id,
                    delivery_method=delivery_method,
                )
            return

        self.db.mark_many_deliveries_sent(normalized_delivery_ids)

    def _mark_delivery_faulty_sync(self, delivery_id: int, error_text: str) -> None:
        self.db.mark_delivery_faulty(delivery_id, error_text)

    def _get_post_id_by_delivery_sync(self, delivery_id: int) -> int | None:
        return self.db.get_post_id_by_delivery(delivery_id)

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
        self.db.log_video_event(
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
        extra = {
            "source_channel": source_channel,
            "target_id": target_id,
            "target_thread_id": target_thread_id,
            "source_message_id": source_message_id,
        }

        if fallback_mode is not None:
            extra["fallback_mode"] = fallback_mode
        if caption_delivery_mode is not None:
            extra["caption_delivery_mode"] = caption_delivery_mode
        if selected_mode is not None:
            extra["selected_mode"] = selected_mode
        if caption_requires_premium is not None:
            extra["caption_requires_premium"] = caption_requires_premium

        self.db.log_video_event(
            event_type="video_processing_failed",
            delivery_id=delivery_id,
            rule_id=rule_id,
            post_id=post_id,
            status="faulty",
            error_text=error_text,
            extra=extra,
        )
        self.db.mark_delivery_faulty(delivery_id, error_text)

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
    ) -> None:
        self.db.log_video_event(
            event_type="video_processing_completed",
            delivery_id=delivery_id,
            rule_id=rule_id,
            post_id=post_id,
            status="sent",
            extra={
                "source_channel": source_channel,
                "target_id": target_id,
                "target_thread_id": target_thread_id,
                "source_message_id": source_message_id,
                "sent_message_id": sent_message_id,
                "candidate_sent_message_ids": candidate_sent_message_ids,
                "valid_sent_message_ids": valid_sent_message_ids,
                "fallback_mode": fallback_mode,
                "caption_delivery_mode": caption_delivery_mode,
                "selected_mode": selected_mode,
                "caption_requires_premium": caption_requires_premium,
            },
        )
        self.db.mark_delivery_sent(delivery_id)

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
        """
        Единый лог промежуточного pipeline-шага.

        ВАЖНО:
        - не помечает доставку как faulty
        - не является финальной ошибкой
        - нужен для прозрачной диагностики
        """
        base_extra = {
            "pipeline_stage": pipeline_stage,
            "pipeline_result": pipeline_result,
            "source_channel": source_channel,
            "target_id": target_id,
            "source_message_ids": source_message_ids,
        }
        if extra:
            base_extra.update(extra)

        for delivery_id in delivery_ids:
            post_id = self._get_post_id_by_delivery(delivery_id)

            self.db.log_delivery_event(
                event_type=event_type,
                delivery_id=delivery_id,
                rule_id=rule_id,
                post_id=post_id,
                status="processing",
                error_text=error_text,
                extra=base_extra,
            )

        item_kind = "АЛЬБОМ" if len(source_message_ids) > 1 else "ОДИНОЧНЫЙ"
        log_line = (
            f"ПРАВИЛО {rule_id} | {item_kind} | ШАГ {pipeline_stage} → "
            f"{pipeline_result.upper()}"
        )
        if error_text:
            logger.warning("%s | %s", log_line, error_text)
        else:
            logger.info("%s", log_line)

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
        await run_db(
            self._log_delivery_pipeline_step_sync,
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
        """
        Единый финальный лог успешной доставки.
        """
        verify_payload = self._serialize_pipeline_verify_result(verify_result)

        base_extra = {
            "final_method": final_method,
            "source_channel": source_channel,
            "target_id": target_id,
            "source_message_ids": source_message_ids,
            "sent_message_id": sent_message_id,
            "sent_message_ids": sent_message_ids or ([sent_message_id] if sent_message_id is not None else []),
            "first_sent_message_id": sent_message_id,
            "reaction_message_id": reaction_message_id if reaction_message_id is not None else sent_message_id,
            "verify_ok": verify_payload.get("ok"),
            "verify_grouped_id": verify_payload.get("grouped_id"),
            "verify_count": verify_payload.get("count"),
            "verify_first_message_id": verify_payload.get("first_message_id"),
        }
        if extra:
            base_extra.update(extra)

        for delivery_id in delivery_ids:
            post_id = self._get_post_id_by_delivery(delivery_id)

            self.db.log_delivery_event(
                event_type="delivery_sent",
                delivery_id=delivery_id,
                rule_id=rule_id,
                post_id=post_id,
                status="sent",
                extra=base_extra,
            )

        logger.info(
            "ПРАВИЛО %s | ДОСТАВКА | ИТОГ → УСПЕХ (method=%s, source=%s, target=%s, count=%s)",
            rule_id,
            final_method,
            source_channel,
            target_id,
            len(source_message_ids),
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
        await run_db(
            self._log_delivery_final_success_sync,
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
        """
        Единый финальный лог неуспешной доставки.

        ВАЖНО:
        - только тут пишем delivery_failed / faulty
        - все промежуточные шаги не считаются финальными ошибками
        """
        base_extra = {
            "final_method": final_method,
            "source_channel": source_channel,
            "target_id": target_id,
            "source_message_ids": source_message_ids,
            "attempts": attempts_debug or [],
        }
        if extra:
            base_extra.update(extra)

        for delivery_id in delivery_ids:
            post_id = self._get_post_id_by_delivery(delivery_id)

            self.db.log_delivery_event(
                event_type="delivery_failed",
                delivery_id=delivery_id,
                rule_id=rule_id,
                post_id=post_id,
                status="faulty",
                error_text=error_text,
                extra=base_extra,
            )

            self.db.mark_delivery_faulty(delivery_id, error_text)

        logger.error(
            "ПРАВИЛО %s | ДОСТАВКА | ИТОГ → ОШИБКА (method=%s, source=%s, target=%s, count=%s) | %s",
            rule_id,
            final_method,
            source_channel,
            target_id,
            len(source_message_ids),
            error_text,
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
        await run_db(
            self._log_delivery_final_failure_sync,
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
        mapping = {
            "pipeline": "общий процесс",
            "download": "скачивание",
            "probe": "анализ видео",
            "trim": "обрезка",
            "normalize": "нормализация",
            "intro": "подготовка заставки",
            "concat": "склейка",
            "thumbnail": "создание превью",
            "send": "отправка",
        }
        return mapping.get(stage or "", stage or "неизвестный этап")

    def _log_human_video_event(
        self,
        *,
        event_type: str,
        status: str | None = None,
        error_text: str | None = None,
        extra: dict | None = None,
    ) -> None:
        payload = dict(extra or {})
        stage = payload.get("stage")
        stage_name = self._stage_name_ru(stage)

        if event_type == "video_stage_started":
            logger.info("▶️ Начат этап: %s", stage_name)
            return

        if event_type == "video_stage_completed":
            if stage == "download":
                file_size_mb = payload.get("file_size_mb")
                if file_size_mb is not None:
                    logger.info("✅ Скачивание завершено: %.1f МБ", float(file_size_mb))
                else:
                    logger.info("✅ Завершён этап: %s", stage_name)
            else:
                logger.info("✅ Завершён этап: %s", stage_name)
            return

        if event_type == "video_stage_failed":
            if error_text:
                logger.error("❌ Ошибка на этапе «%s»: %s", stage_name, error_text)
            else:
                logger.error("❌ Ошибка на этапе «%s»", stage_name)
            return

        if event_type == "video_ffmpeg_progress":
            operation = payload.get("operation")
            percent = payload.get("percent")
            processed_sec = payload.get("processed_sec")
            total_sec = payload.get("total_sec")
            speed = payload.get("speed")

            parts = []
            if operation:
                parts.append(str(operation))
            elif stage_name:
                parts.append(stage_name.capitalize())

            if percent is not None:
                parts.append(f"{float(percent):.1f}%")
            if processed_sec is not None and total_sec is not None:
                parts.append(f"{float(processed_sec):.1f} / {float(total_sec):.1f} сек")
            if speed:
                parts.append(f"скорость {speed}")

            logger.info("🎬 %s", " | ".join(parts))
            return

        if event_type == "video_send_retry":
            attempt = payload.get("attempt")
            max_retries = payload.get("max_retries")
            if attempt is not None and max_retries is not None:
                logger.warning("🔁 Повторная попытка отправки: %s из %s", attempt, max_retries)
            elif attempt is not None:
                logger.warning("🔁 Повторная попытка отправки: %s", attempt)
            else:
                logger.warning("🔁 Повторная попытка отправки")
            return

    def _get_rule_intro_items(self, rule):
        horizontal_intro = None
        vertical_intro = None

        horizontal_id = getattr(rule, "video_intro_horizontal_id", None)
        vertical_id = getattr(rule, "video_intro_vertical_id", None)

        try:
            if horizontal_id:
                horizontal_intro = self.db.get_intro_by_id(int(horizontal_id))
        except Exception:
            horizontal_intro = None

        try:
            if vertical_id:
                vertical_intro = self.db.get_intro_by_id(int(vertical_id))
        except Exception:
            vertical_intro = None

        return horizontal_intro, vertical_intro

    def _is_self_loop_rule(self, rule) -> bool:
        return (
            str(rule.source_id) == str(rule.target_id)
            and rule.source_thread_id == rule.target_thread_id
        )

    def _get_post_id_by_delivery(self, delivery_id: int) -> int | None:
        return self.db.get_post_id_by_delivery(delivery_id)

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
        try:
            entity = int(target_id) if str(target_id).lstrip("-").isdigit() else target_id
            formatting_entities = self._clone_telethon_entities(entities, text)

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

            sent = await self.telethon.send_message(**send_kwargs)
            sent_id = int(sent.id) if sent else None

            logger.info(
                "TELETHON_TEXT_SEND | OK | target=%s | thread=%s | sent_message_id=%s",
                target_id,
                target_thread_id,
                sent_id,
            )
            return sent_id

        except Exception as exc:
            logger.warning(
                "TELETHON_TEXT_SEND | FAILED | target=%s | thread=%s | error=%s",
                target_id,
                target_thread_id,
                exc,
            )
            return None

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
        content = self._content_from_message_or_post(message=message, post_row=post_row)
        raw_text, raw_entities = self._build_text_and_entities_from_content(content)
        formatting_entities = self._clone_telethon_entities(raw_entities, raw_text)

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

            sent = await self.telethon.send_file(**send_kwargs)
            sent_id = int(sent.id) if sent else None

            logger.info(
                "TELETHON_FILE_SEND | OK_ORIGINAL_MEDIA | target=%s | thread=%s | sent_message_id=%s",
                target_id,
                target_thread_id,
                sent_id,
            )
            return sent_id

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
            return None

        try:
            logger.info(
                "TELETHON_FILE_SEND | START_FILE_PATH | target=%s | thread=%s | file=%s | media_kind=%s | caption_len=%s | entities_in=%s | entities_out=%s | supports_streaming=%s",
                target_id,
                target_thread_id,
                file_path.name,
                media_kind,
                len(raw_text or ""),
                len(raw_entities or []),
                len(formatting_entities or []),
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

            if target_thread_id is not None:
                send_kwargs["comment_to"] = int(target_thread_id)

            sent = await self.telethon.send_file(**send_kwargs)
            sent_id = int(sent.id) if sent else None

            logger.info(
                "TELETHON_FILE_SEND | OK_FILE_PATH | target=%s | thread=%s | file=%s | sent_message_id=%s",
                target_id,
                target_thread_id,
                file_path.name,
                sent_id,
            )
            return sent_id

        except Exception as exc:
            logger.warning(
                "TELETHON_FILE_SEND | FAILED_FILE_PATH | target=%s | thread=%s | file=%s | error=%s",
                target_id,
                target_thread_id,
                file_path.name if file_path else None,
                exc,
            )
            return None

    async def _send_album_via_telethon(
        self,
        *,
        messages,
        target_id,
        target_thread_id,
        post_rows: list[dict] | None = None,
    ) -> dict:
        downloaded_paths: list[Path] = []

        try:
            if not messages:
                return {
                    "ok": False,
                    "sent_message_id": None,
                    "sent_count": 0,
                    "error_text": "Пустой список сообщений для Telethon album send",
                }

            entity = int(target_id) if str(target_id).lstrip("-").isdigit() else target_id

            caption_text = ""
            caption_entities = None
            media_items = []

            for idx, message in enumerate(messages):
                media = getattr(message, "media", None)
                if not media:
                    return {
                        "ok": False,
                        "sent_message_id": None,
                        "sent_count": 0,
                        "error_text": "Один из элементов альбома не содержит media",
                    }
                media_items.append(media)

                post_row = post_rows[idx] if post_rows and idx < len(post_rows) else None
                content = self._content_from_message_or_post(message=message, post_row=post_row)
                raw_text, raw_entities = self._build_text_and_entities_from_content(content)

                if raw_text and not caption_text:
                    caption_text = raw_text
                    caption_entities = raw_entities

            formatting_entities = self._clone_telethon_entities(caption_entities, caption_text)

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

            sent = await self.telethon.send_file(**send_kwargs)
            sent_messages = sent if isinstance(sent, list) else [sent]

            if sent_messages:
                first_id = int(sent_messages[0].id)
                logger.info(
                    "TELETHON_ALBUM_SEND | OK_ORIGINAL_MEDIA | target=%s | thread=%s | sent_count=%s | sent_message_ids=%s | first_message_id=%s",
                    target_id,
                    target_thread_id,
                    len(sent_messages),
                    [int(m.id) for m in sent_messages if m],
                    first_id,
                )
                return {
                    "ok": True,
                    "sent_message_id": first_id,
                    "sent_message_ids": [int(m.id) for m in sent_messages if m],
                    "sent_count": len(sent_messages),
                    "error_text": None,
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
                file_path = await self.telethon.download_media(
                    message,
                    file=str(settings.media_cache_path),
                )
                if not file_path:
                    return {
                        "ok": False,
                        "sent_message_id": None,
                        "sent_count": len(files),
                        "error_text": f"Не удалось скачать элемент альбома {idx + 1}/{len(messages)}",
                    }

                path = Path(file_path)
                downloaded_paths.append(path)
                files.append(str(path))

                post_row = post_rows[idx] if post_rows and idx < len(post_rows) else None
                content = self._content_from_message_or_post(message=message, post_row=post_row)
                raw_text, raw_entities = self._build_text_and_entities_from_content(content)

                if raw_text and not caption_text:
                    caption_text = raw_text
                    caption_entities = raw_entities

            formatting_entities = self._clone_telethon_entities(caption_entities, caption_text)

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

            sent = await self.telethon.send_file(**send_kwargs)
            sent_messages = sent if isinstance(sent, list) else [sent]

            if not sent_messages:
                return {
                    "ok": False,
                    "sent_message_id": None,
                    "sent_count": 0,
                    "error_text": "Telethon send_file(album) вернул пустой результат",
                }

            first_id = int(sent_messages[0].id)
            logger.info(
                "TELETHON_ALBUM_SEND | OK_FILE_PATH | target=%s | thread=%s | sent_count=%s | sent_message_ids=%s | first_message_id=%s",
                target_id,
                target_thread_id,
                len(sent_messages),
                [int(m.id) for m in sent_messages if m],
                first_id,
            )
            return {
                "ok": True,
                "sent_message_id": first_id,
                "sent_message_ids": [int(m.id) for m in sent_messages if m],
                "sent_count": len(sent_messages),
                "error_text": None,
            }

        except Exception as exc:
            logger.exception(
                "TELETHON_ALBUM_SEND | FAILED_FILE_PATH | target=%s | thread=%s | error=%s",
                target_id,
                target_thread_id,
                exc,
            )
            return {
                "ok": False,
                "sent_message_id": None,
                "sent_count": 0,
                "error_text": str(exc),
            }

        finally:
            for path in downloaded_paths:
                try:
                    path.unlink(missing_ok=True)
                except Exception:
                    pass

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
            fetched = await self.telethon.get_messages(entity, ids=sent_message_ids)
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
    ) -> None:
        from .reaction_delivery import ReactionDelivery

        return await ReactionDelivery(self)._add_reaction_for_rule_if_possible(
            rule=rule,
            target_id=target_id,
            sent_message_id=sent_message_id,
            source_channel=source_channel,
            source_message_ids=source_message_ids,
            delivery_id=delivery_id,
            max_age_seconds=max_age_seconds,
        )

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
        if DEBUG_FORCE_SKIP_COPY_SINGLE:
            logger.warning(
                "COPY_SINGLE | TEST MODE | принудительно пропускаю Bot API copy_message для проверки Telethon"
            )
            return {"attempted": False, "sent_ids": [], "fallback_allowed": True, "raw_result_type": "debug_skip"}

        try:
            sent = await self.bot.copy_message(
                chat_id=target_id,
                from_chat_id=source_channel,
                message_id=message_id,
                message_thread_id=target_thread_id,
            )
            sent_ids = self._extract_sent_message_ids(sent)
            return {"attempted": True, "sent_ids": sent_ids, "fallback_allowed": False, "raw_result_type": type(sent).__name__, "raw_result": sent}
        except Exception as exc:
            logger.warning(
                "Не удалось скопировать сообщение %s/%s в %s: %s",
                source_channel,
                message_id,
                target_id,
                exc,
            )
            return {"attempted": True, "sent_ids": [], "fallback_allowed": False, "raw_result_type": "exception", "error_text": str(exc)}

    async def _copy_album_via_bot(self, source_channel, target_id, message_ids, target_thread_id):
        if DEBUG_FORCE_SKIP_COPY_ALBUM:
            logger.warning(
                "COPY_ALBUM | TEST MODE | принудительно пропускаю Bot API CopyMessages для проверки Telethon album send"
            )
            return {
                "ok": False,
                "sent_message_id": None,
                "sent_count": 0,
                "error_text": "Bot API copy_album принудительно отключён",
            }

        try:
            sent_messages = await self.bot(
                CopyMessages(
                    chat_id=target_id,
                    from_chat_id=source_channel,
                    message_ids=message_ids,
                    message_thread_id=target_thread_id,
                )
            )

            if sent_messages and len(sent_messages) > 0:
                return {
                    "ok": True,
                    "sent_message_id": sent_messages[0].message_id,
                    "sent_message_ids": [int(m.message_id) for m in sent_messages],
                    "sent_count": len(sent_messages),
                    "error_text": None,
                }

            return {
                "ok": False,
                "sent_message_id": None,
                "sent_count": 0,
                "error_text": "CopyMessages вернул пустой результат",
            }

        except Exception as exc:
            return {
                "ok": False,
                "sent_message_id": None,
                "sent_count": 0,
                "error_text": str(exc),
            }

    async def _send_album_one_by_one(self, messages, target_id, target_thread_id, post_rows: list[dict] | None = None):
        sent_ids: list[int] = []

        try:
            if not messages:
                return {
                    "ok": False,
                    "sent_message_id": None,
                    "sent_count": 0,
                    "error_text": "Пустой список сообщений для one-by-one fallback",
                }

            for idx, message in enumerate(messages):
                post_row = post_rows[idx] if post_rows and idx < len(post_rows) else None
                sent_message_id = await self._reupload_message(
                    message=message,
                    target_id=target_id,
                    target_thread_id=target_thread_id,
                    post_row=post_row,
                )

                if not sent_message_id:
                    return {
                        "ok": False,
                        "sent_message_id": sent_ids[0] if sent_ids else None,
                        "sent_count": len(sent_ids),
                        "error_text": "Не удалось отправить один из элементов альбома в аварийном fallback",
                    }

                sent_ids.append(int(sent_message_id))

            return {
                "ok": True,
                "sent_message_id": sent_ids[0] if sent_ids else None,
                "sent_message_ids": sent_ids[:],
                "sent_count": len(sent_ids),
                "error_text": None,
            }

        except Exception as exc:
            return {
                "ok": False,
                "sent_message_id": sent_ids[0] if sent_ids else None,
                "sent_message_ids": sent_ids[:],
                "sent_count": len(sent_ids),
                "error_text": str(exc),
            }

    async def _reupload_album(self, messages, target_id, target_thread_id, post_rows: list[dict] | None = None):
        downloaded_paths: list[Path] = []

        try:
            if not messages:
                return {
                    "ok": False,
                    "sent_message_id": None,
                    "sent_count": 0,
                    "error_text": "Пустой список сообщений для reupload альбома",
                }

            logger.info(
                "REUPLOAD_ALBUM | START | target=%s | thread=%s | items=%s",
                target_id,
                target_thread_id,
                len(messages),
            )

            telethon_result = await self._send_album_via_telethon(
                messages=messages,
                target_id=target_id,
                target_thread_id=target_thread_id,
                post_rows=post_rows,
            )

            logger.info(
                "REUPLOAD_ALBUM | TELETHON_RESULT | ok=%s | sent_message_id=%s | sent_message_ids=%s | sent_count=%s | error=%s",
                telethon_result.get("ok"),
                telethon_result.get("sent_message_id"),
                telethon_result.get("sent_message_ids"),
                telethon_result.get("sent_count"),
                telethon_result.get("error_text"),
            )

            send_result = telegram_send_result_from_raw(
                telethon_result,
                method="reupload_album",
                fallback_sent_ids=telethon_result.get("sent_message_ids"),
                error_text=telethon_result.get("error_text"),
            )
            log_fn = logger.info if send_result.ok else logger.warning
            log_fn(
                "TELEGRAM_SEND_RESULT | method=%s | ok=%s | sent_message_ids=%s | sent_message_id=%s | raw_result_type=%s | error_text=%s | retryable=%s",
                send_result.method, send_result.ok, send_result.sent_message_ids, send_result.sent_message_id, send_result.raw_result_type, send_result.error_text, send_result.retryable
            )
            if send_result.ok:
                telethon_result["sent_message_ids"] = send_result.sent_message_ids
                telethon_result["sent_message_id"] = send_result.sent_message_id
                return telethon_result

            caption_index = None
            caption_text = None

            for idx, message in enumerate(messages):
                post_row = post_rows[idx] if post_rows and idx < len(post_rows) else None
                content = self._content_from_message_or_post(message=message, post_row=post_row)
                raw_text, _raw_entities = self._build_text_and_entities_from_content(content)

                text_value = (raw_text or "").strip()
                if text_value:
                    caption_index = idx
                    caption_text = text_value
                    break

            caption_html = None
            caption_plain = None

            if caption_text:
                normalized_caption = _normalize_source_text(caption_text)
                caption_plain = normalized_caption or caption_text

                try:
                    prepared_html = _prepare_html_text(caption_text)
                except Exception:
                    prepared_html = None

                suspicious = False
                prepared_check = prepared_html or ""
                suspicious_patterns = [
                    "*🔥",
                    "**FireFolder",
                    "__HTML_PLACEHOLDER_",
                    "***",
                    "[**",
                    "]**(",
                ]
                for pattern in suspicious_patterns:
                    if pattern in prepared_check:
                        suspicious = True
                        break

                if prepared_html and not suspicious:
                    caption_html = prepared_html
                else:
                    caption_html = None

            media_items = []

            for idx, message in enumerate(messages):
                file_path = await self.telethon.download_media(
                    message,
                    file=str(settings.media_cache_path),
                )
                if not file_path:
                    return {
                        "ok": False,
                        "sent_message_id": None,
                        "sent_count": 0,
                        "error_text": f"Не удалось скачать элемент альбома {idx + 1}/{len(messages)}",
                    }

                path = Path(file_path)
                downloaded_paths.append(path)

                input_file = FSInputFile(path)
                mime, _ = mimetypes.guess_type(path.name)
                mime = (mime or "").lower()

                item_caption = None
                item_parse_mode = None

                if caption_index == idx and caption_text:
                    if caption_html:
                        item_caption = caption_html
                        item_parse_mode = "HTML"
                    else:
                        item_caption = caption_plain
                        item_parse_mode = None

                if mime.startswith("image/"):
                    media_items.append(
                        InputMediaPhoto(
                            media=input_file,
                            caption=item_caption,
                            parse_mode=item_parse_mode,
                        )
                    )
                elif mime.startswith("video/"):
                    media_items.append(
                        InputMediaVideo(
                            media=input_file,
                            caption=item_caption,
                            parse_mode=item_parse_mode,
                        )
                    )
                else:
                    media_items.append(
                        InputMediaDocument(
                            media=input_file,
                            caption=item_caption,
                            parse_mode=item_parse_mode,
                        )
                    )

            sent_messages = await self.bot.send_media_group(
                chat_id=target_id,
                media=media_items,
                message_thread_id=target_thread_id,
            )

            if sent_messages and len(sent_messages) > 0:
                return {
                    "ok": True,
                    "sent_message_id": sent_messages[0].message_id,
                    "sent_message_ids": [int(m.message_id) for m in sent_messages],
                    "sent_count": len(sent_messages),
                    "error_text": None,
                }

            return {
                "ok": False,
                "sent_message_id": None,
                "sent_count": 0,
                "error_text": telethon_result.get("error_text") or "send_media_group вернул пустой результат",
            }

        except Exception as exc:
            logger.exception("reupload_album: ошибка reupload альбома: %s", exc)
            return {
                "ok": False,
                "sent_message_id": None,
                "sent_count": 0,
                "error_text": str(exc),
            }

        finally:
            for path in downloaded_paths:
                try:
                    path.unlink(missing_ok=True)
                except Exception:
                    pass

    async def _fetch_message(self, source_channel, message_id):
        try:
            entity = int(source_channel) if str(source_channel).lstrip("-").isdigit() else source_channel
            return await self.telethon.get_messages(entity, ids=message_id)
        except Exception as exc:
            logger.warning("Telethon не смог получить сообщение %s/%s: %s", source_channel, message_id, exc); return None

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
        started_at = time.monotonic()
        last_emit_at = 0.0
        last_emit_percent = -1
        last_ui_emit_at = 0.0
        emitted_ui_milestones: set[int] = set()
        ui_milestones = (0, 10, 25, 50, 75, 100)
        ui_min_interval_sec = 20.0

        cache_dir = settings.media_cache_path
        cache_dir.mkdir(parents=True, exist_ok=True)

        message_id = source_message_id or getattr(message, "id", None) or "unknown"
        delivery_part = delivery_id if delivery_id is not None else "manual"
        ext = getattr(getattr(message, "file", None), "ext", None) or ".mp4"
        if not str(ext).startswith("."):
            ext = f".{ext}"
        download_target_path = cache_dir / f"video_src_{delivery_part}_{message_id}_{int(time.time() * 1000)}{ext}"

        def progress_callback(current: int, total: int):
            nonlocal last_emit_at, last_emit_percent, last_ui_emit_at

            now = time.monotonic()
            elapsed = max(now - started_at, 0.001)
            speed = current / elapsed if elapsed > 0 else 0.0
            percent = int((current / total) * 100) if total else 0
            remaining_bytes = max(total - current, 0)
            eta_sec = (remaining_bytes / speed) if speed > 0 else 0.0

            should_emit = False
            if now - last_emit_at >= 1.0:
                should_emit = True
            if percent >= last_emit_percent + 5:
                should_emit = True
            if current == total and total > 0:
                should_emit = True

            if not should_emit:
                return

            last_emit_at = now
            last_emit_percent = percent

            logger.info(
                "📥 Скачивание видео: %s%% | %s из %s | скорость %s | осталось %s",
                percent,
                _format_bytes_ru(current),
                _format_bytes_ru(total),
                _format_speed_ru(speed),
                _format_eta_ru(eta_sec),
            )

            should_emit_ui = False
            for milestone in ui_milestones:
                if percent >= milestone and milestone not in emitted_ui_milestones:
                    emitted_ui_milestones.add(milestone)
                    should_emit_ui = True
                    break
            if now - last_ui_emit_at >= ui_min_interval_sec:
                should_emit_ui = True

            if should_emit_ui and delivery_id is not None and rule_id is not None:
                last_ui_emit_at = now
                try:
                    self._schedule_video_event_log(
                        event_type="video_download_progress",
                        delivery_id=delivery_id,
                        rule_id=rule_id,
                        post_id=post_id,
                        status="processing",
                        extra={
                            "source_channel": source_channel,
                            "target_id": target_id,
                            "source_message_id": source_message_id,
                            "stage": "download",
                            "percent": percent,
                            "downloaded_bytes": current,
                            "total_bytes": total,
                            "speed_bytes_per_sec": round(speed, 2),
                            "eta_sec": int(eta_sec),
                            "downloaded_human": _format_bytes_ru(current),
                            "total_human": _format_bytes_ru(total),
                            "speed_human": _format_speed_ru(speed),
                            "eta_human": _format_eta_ru(eta_sec),
                        },
                    )
                except Exception:
                    pass

        try:
            logger.info("📥 Начинаю скачивание исходного видео...")

            file_path = await self.telethon.download_media(
                message,
                file=str(download_target_path),
                progress_callback=progress_callback,
            )

            if not file_path:
                logger.warning("Не удалось скачать исходное видео: путь не получен")
                return None

            path = Path(file_path)
            if not path.exists() or not path.is_file():
                logger.warning("Не удалось скачать исходное видео: файл не найден после скачивания")
                return None

            file_size = path.stat().st_size
            if file_size <= 0:
                try:
                    path.unlink(missing_ok=True)
                except Exception:
                    pass
                logger.warning("Не удалось скачать исходное видео: файл пустой")
                return None

            elapsed_total = time.monotonic() - started_at
            avg_speed = file_size / elapsed_total if elapsed_total > 0 else 0.0

            logger.info(
                "✅ Скачивание завершено: %s за %.1f сек | средняя скорость %s",
                _format_bytes_ru(file_size),
                elapsed_total,
                _format_speed_ru(avg_speed),
            )

            if delivery_id is not None and rule_id is not None:
                try:
                    await run_db(
                        self._log_video_event_sync,
                        event_type="video_download_completed",
                        delivery_id=delivery_id,
                        rule_id=rule_id,
                        post_id=post_id,
                        status="completed",
                        extra={
                            "source_channel": source_channel,
                            "target_id": target_id,
                            "source_message_id": source_message_id,
                            "stage": "download",
                            "file_path": str(path),
                            "downloaded_bytes": file_size,
                            "total_bytes": file_size,
                            "downloaded_human": _format_bytes_ru(file_size),
                            "elapsed_sec": round(elapsed_total, 2),
                            "avg_speed_bytes_per_sec": round(avg_speed, 2),
                            "avg_speed_human": _format_speed_ru(avg_speed),
                        },
                    )
                except Exception:
                    pass

            return path

        except Exception as exc:
            logger.warning("Не удалось скачать исходное видео: %s", exc)

            if delivery_id is not None and rule_id is not None:
                try:
                    await run_db(
                        self._log_video_event_sync,
                        event_type="video_download_failed",
                        delivery_id=delivery_id,
                        rule_id=rule_id,
                        post_id=post_id,
                        status="failed",
                        error_text=str(exc),
                        extra={
                            "source_channel": source_channel,
                            "target_id": target_id,
                            "source_message_id": source_message_id,
                            "stage": "download",
                        },
                    )
                except Exception:
                    pass

            return None

    async def _reupload_message(self, message, target_id, target_thread_id, post_row: dict | None = None):
        content = self._content_from_message_or_post(message=message, post_row=post_row)
        raw_text, raw_entities = self._build_text_and_entities_from_content(content)

        if not getattr(message, "media", None):
            logger.info(
                "REUPLOAD_MESSAGE | TEXT_ONLY | target=%s | thread=%s | text_len=%s | entities=%s",
                target_id,
                target_thread_id,
                len(raw_text or ""),
                len(raw_entities or []),
            )

            sent_message_id = await self._send_text_via_telethon(
                target_id=target_id,
                target_thread_id=target_thread_id,
                text=raw_text,
                entities=raw_entities,
            )
            if sent_message_id:
                logger.info(
                    "REUPLOAD_MESSAGE | TELETHON_TEXT_USED | sent_message_id=%s",
                    sent_message_id,
                )
                return sent_message_id

            html_text = _prepare_html_text(raw_text)
            if html_text:
                logger.info("REUPLOAD_MESSAGE | BOTAPI_TEXT_FALLBACK | START")
                sent = await self.bot.send_message(
                    chat_id=target_id,
                    message_thread_id=target_thread_id,
                    text=html_text,
                    parse_mode="HTML",
                    disable_web_page_preview=True,
                )
                logger.info(
                    "REUPLOAD_MESSAGE | BOTAPI_TEXT_FALLBACK | OK | sent_message_id=%s",
                    sent.message_id,
                )
                return sent.message_id

            logger.warning("REUPLOAD_MESSAGE | TEXT_ONLY | ALL_METHODS_FAILED")
            return None

        file_path = await self.telethon.download_media(message, file=str(settings.media_cache_path))
        if not file_path:
            logger.warning("REUPLOAD_MESSAGE | DOWNLOAD_FAILED")
            return None

        try:
            path = Path(file_path)
            mime, _ = mimetypes.guess_type(path.name)
            mime = (mime or "").lower()

            logger.info(
                "REUPLOAD_MESSAGE | MEDIA | target=%s | thread=%s | file=%s | mime=%s | text_len=%s | entities=%s",
                target_id,
                target_thread_id,
                path.name,
                mime,
                len(raw_text or ""),
                len(raw_entities or []),
            )

            sent_message_id = await self._send_file_via_telethon(
                target_id=target_id,
                target_thread_id=target_thread_id,
                message=message,
                file_path=path,
                force_document=not (mime.startswith("image/") or mime.startswith("video/")),
                post_row=post_row,
            )
            if sent_message_id:
                logger.info(
                    "REUPLOAD_MESSAGE | TELETHON_FILE_USED | sent_message_id=%s",
                    sent_message_id,
                )
                return sent_message_id

            html_text = _prepare_html_text(raw_text)
            input_file = FSInputFile(path)

            logger.info("REUPLOAD_MESSAGE | BOTAPI_MEDIA_FALLBACK | START | mime=%s", mime)

            if mime.startswith("image/"):
                sent = await self.bot.send_photo(
                    chat_id=target_id,
                    message_thread_id=target_thread_id,
                    photo=input_file,
                    caption=html_text,
                    parse_mode="HTML" if html_text else None,
                )
            elif mime.startswith("video/"):
                sent = await self.bot.send_video(
                    chat_id=target_id,
                    message_thread_id=target_thread_id,
                    video=input_file,
                    caption=html_text,
                    parse_mode="HTML" if html_text else None,
                    supports_streaming=True,
                )
            else:
                sent = await self.bot.send_document(
                    chat_id=target_id,
                    message_thread_id=target_thread_id,
                    document=input_file,
                    caption=html_text,
                    parse_mode="HTML" if html_text else None,
                )

            logger.info(
                "REUPLOAD_MESSAGE | BOTAPI_MEDIA_FALLBACK | OK | sent_message_id=%s",
                sent.message_id,
            )
            return sent.message_id

        finally:
            try:
                Path(file_path).unlink(missing_ok=True)
            except Exception:
                pass


    async def execute_repost_campaign_send_copy_from_job(self, *, copy_id: int, **kwargs) -> dict:
        copy_row = await run_db(self.db.get_delivery_campaign_copy, int(copy_id))
        if not copy_row:
            return {"ok": False, "retryable": False, "error_text": "Копия кампании не найдена"}
        if str(copy_row.get("send_status") or "") == "sent":
            return {"ok": True, "already_sent": True}
        delivery = await run_db(self.db.get_delivery, int(copy_row.get("delivery_id") or 0))
        if not delivery:
            await run_db(self.db.mark_delivery_campaign_copy_send_failed, int(copy_id), "Delivery не найден")
            return {"ok": False, "retryable": False, "error_text": "Delivery не найден"}
        if str(delivery.get("delivery_method") or "") == "album":
            return {"ok": False, "retryable": False, "error_text": "Кампании для альбомов пока не поддерживаются в MVP"}
        await run_db(self.db.mark_delivery_campaign_copy_processing, int(copy_id))
        sent = await self.bot.copy_message(chat_id=str(copy_row.get("target_id")), from_chat_id=str(delivery.get("source_channel")), message_id=int(delivery.get("message_id")))
        sent_ids = [int(sent.message_id)] if getattr(sent, "message_id", None) else []
        rule = await run_db(self.db.get_rule, int(copy_row.get("rule_id") or 0))
        show_seconds = int(getattr(rule, "repost_campaign_show_seconds", 0) or 0)
        from app.repost_campaign_service import build_campaign_delete_after_iso
        from app.job_service import enqueue_repost_campaign_delete_copy
        delete_after_at = build_campaign_delete_after_iso(show_seconds)
        await run_db(self.db.mark_delivery_campaign_copy_sent, int(copy_id), sent_message_id=(sent_ids[0] if sent_ids else None), sent_message_ids=sent_ids, delivery_method="copy_single", delete_after_at=delete_after_at)
        await run_db(enqueue_repost_campaign_delete_copy, self.db, int(copy_id), run_at=delete_after_at)
        return {"ok": True, "copy_id": int(copy_id), "sent_message_ids": sent_ids}

    async def execute_repost_campaign_delete_copy_from_job(self, *, copy_id: int, **kwargs) -> dict:
        copy_row = await run_db(self.db.get_delivery_campaign_copy, int(copy_id))
        if not copy_row:
            return {"ok": False, "retryable": False, "error_text": "Копия кампании не найдена"}
        if str(copy_row.get("delete_status") or "") in {"deleted", "skipped"}:
            return {"ok": True, "already_done": True}
        msg_ids = copy_row.get("sent_message_ids") or []
        if not msg_ids and copy_row.get("sent_message_id"):
            msg_ids = [int(copy_row.get("sent_message_id"))]
        if not msg_ids:
            await run_db(self.db.mark_delivery_campaign_copy_delete_skipped, int(copy_id), "Нет message_id для удаления")
            return {"ok": True}
        await run_db(self.db.mark_delivery_campaign_copy_delete_processing, int(copy_id))
        for mid in msg_ids:
            try:
                await self.bot.delete_message(chat_id=str(copy_row.get("target_id")), message_id=int(mid))
            except Exception as exc:
                text = str(exc)
                if "not found" in text.lower() or "message to delete not found" in text.lower():
                    continue
                await run_db(self.db.mark_delivery_campaign_copy_delete_failed, int(copy_id), text)
                return {"ok": False, "retryable": ("retry after" in text.lower()), "error_text": text}
        await run_db(self.db.mark_delivery_campaign_copy_deleted, int(copy_id))
        return {"ok": True}
