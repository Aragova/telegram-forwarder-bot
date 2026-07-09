from __future__ import annotations

import logging

from .runtime_utils import run_db

logger = logging.getLogger("forwarder")


class SenderDeliveryLoggingHelpers:
    def __init__(self, owner):
        self.owner = owner

    def serialize_pipeline_verify_result(self, verify_result: dict | None) -> dict:
        payload = dict(verify_result or {})
        return {
            "ok": bool(payload.get("ok")),
            "error_text": payload.get("error_text"),
            "grouped_id": payload.get("grouped_id"),
            "count": payload.get("count"),
            "first_message_id": payload.get("first_message_id"),
        }

    def log_delivery_pipeline_step_sync(
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
            post_id = self.owner.db.get_post_id_by_delivery(delivery_id)

            self.owner.db.log_delivery_event(
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

    async def log_delivery_pipeline_step(
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
            self.log_delivery_pipeline_step_sync,
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

    def log_delivery_final_success_sync(
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
        verify_payload = self.serialize_pipeline_verify_result(verify_result)

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
            post_id = self.owner.db.get_post_id_by_delivery(delivery_id)

            self.owner.db.log_delivery_event(
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

    async def log_delivery_final_success(
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
            self.log_delivery_final_success_sync,
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

    def log_delivery_final_failure_sync(
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
            post_id = self.owner.db.get_post_id_by_delivery(delivery_id)

            self.owner.db.log_delivery_event(
                event_type="delivery_failed",
                delivery_id=delivery_id,
                rule_id=rule_id,
                post_id=post_id,
                status="faulty",
                error_text=error_text,
                extra=base_extra,
            )

            self.owner.db.mark_delivery_faulty(delivery_id, error_text)

        logger.error(
            "ПРАВИЛО %s | ДОСТАВКА | ИТОГ → ОШИБКА (method=%s, source=%s, target=%s, count=%s) | %s",
            rule_id,
            final_method,
            source_channel,
            target_id,
            len(source_message_ids),
            error_text,
        )

    async def log_delivery_final_failure(
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
            self.log_delivery_final_failure_sync,
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
