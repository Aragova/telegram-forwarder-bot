from __future__ import annotations

import asyncio
import logging

from .runtime_utils import run_db

logger = logging.getLogger("forwarder")


class SenderVideoLoggingHelpers:
    def __init__(self, owner):
        self.owner = owner

    def schedule_video_event_log(
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
                self.log_video_event_sync,
                event_type=event_type,
                delivery_id=delivery_id,
                rule_id=rule_id,
                post_id=post_id,
                status=status,
                error_text=error_text,
                extra=extra,
            )
        )

    def log_video_event_sync(
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
        self.owner.db.log_video_event(
            event_type=event_type,
            delivery_id=delivery_id,
            rule_id=rule_id,
            post_id=post_id,
            status=status,
            error_text=error_text,
            extra=extra,
        )

    def finalize_video_failure_sync(
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

        self.owner.db.log_video_event(
            event_type="video_processing_failed",
            delivery_id=delivery_id,
            rule_id=rule_id,
            post_id=post_id,
            status="faulty",
            error_text=error_text,
            extra=extra,
        )
        self.owner.db.mark_delivery_faulty(delivery_id, error_text)

    def finalize_video_success_sync(
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
        normalized_candidate_ids: list[int] = []
        for value in candidate_sent_message_ids or []:
            try:
                normalized_candidate_ids.append(int(value))
            except Exception:
                continue

        normalized_valid_ids: list[int] = []
        for value in valid_sent_message_ids or []:
            try:
                normalized_valid_ids.append(int(value))
            except Exception:
                continue

        self.owner.db.log_video_event(
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
                "candidate_sent_message_ids": normalized_candidate_ids,
                "valid_sent_message_ids": normalized_valid_ids,
                "fallback_mode": fallback_mode,
                "caption_delivery_mode": caption_delivery_mode,
                "selected_mode": selected_mode,
                "caption_requires_premium": caption_requires_premium,
            },
        )
        self.owner.db.mark_delivery_sent(delivery_id)

    def stage_name_ru(self, stage: str | None) -> str:
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

    def log_human_video_event(
        self,
        *,
        event_type: str,
        status: str | None = None,
        error_text: str | None = None,
        extra: dict | None = None,
    ) -> None:
        payload = dict(extra or {})
        stage = payload.get("stage")
        stage_name = self.stage_name_ru(stage)

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
