from __future__ import annotations

import inspect
from dataclasses import dataclass, replace
from typing import Any, Mapping

from app.delivery_context import DeliveryContext
from app.delivery_finalizer import DeliveryFinalizationResult, DeliveryFinalizer
from app.delivery_pipeline_result import DeliveryPipelineResult
from app.telegram_send_result import TelegramSendResult
from app.transport_policy import TransportRateLimited


@dataclass(frozen=True, slots=True)
class LegacyVideoDeliveryInput:
    """Safe compatibility input for future legacy video delivery adapters.

    The legacy payload is a transient pass-through only. Safe log helpers must
    not expose raw payloads, captions, Telegram objects, runtime services, DB
    objects, video paths, thumbnails, or processing state.
    """

    context: DeliveryContext | None
    source_chat_id: int | str | None
    source_message_id: int | None
    target_chat_id: int | str | None
    target_thread_id: int | None
    idempotency_key: str | None
    telegram_method: str | None
    legacy_payload: Mapping[str, Any] | None = None
    legacy_label: str | None = None

    def to_log_context(self) -> dict[str, object]:
        result: dict[str, object] = {
            "source_chat_id": self.source_chat_id,
            "source_message_id": self.source_message_id,
            "target_chat_id": self.target_chat_id,
            "target_thread_id": self.target_thread_id,
            "idempotency_key": self.idempotency_key,
            "telegram_method": self.telegram_method,
            "legacy_label": self.legacy_label,
            "has_legacy_payload": self.legacy_payload is not None,
        }
        if self.context is not None:
            result["context"] = self.context.to_log_context()
        return result


class LegacyVideoDeliveryPipeline:
    """Foundation bridge for the current legacy video delivery flow.

    The pipeline is intentionally dependency-injected and is not connected to
    runtime wiring. Its only external action is calling the injected legacy
    callable with LegacyVideoDeliveryInput; it does not create Telegram clients,
    repositories, transport policies, retries, DB/audit side effects, ffmpeg
    processes, or temp files.
    """

    def __init__(
        self,
        *,
        legacy_video_delivery: Any,
        finalizer: Any | None = None,
    ) -> None:
        self.legacy_video_delivery = legacy_video_delivery
        self.finalizer = finalizer or DeliveryFinalizer()

    async def run(self, input_data: LegacyVideoDeliveryInput) -> DeliveryFinalizationResult:
        validation_error = self._validate_input(input_data)
        context = input_data.context.with_operation("legacy_video_delivery") if input_data.context is not None else None
        if validation_error is not None:
            return self._finalize_pipeline_result(
                DeliveryPipelineResult.failed(context=context, reason=validation_error),
                context=context,
                input_data=input_data,
            )

        legacy_input = replace(input_data, context=context)
        try:
            legacy_result = self.legacy_video_delivery(legacy_input)
            if inspect.isawaitable(legacy_result):
                legacy_result = await legacy_result
        except TransportRateLimited as error:
            return self._finalize_pipeline_result(
                DeliveryPipelineResult.rate_limited(
                    context=context,
                    retry_after=error.retry_after_seconds,
                    reason="legacy_video_delivery_rate_limited",
                ),
                context=context,
                input_data=input_data,
            )
        except Exception as error:
            return self._finalize_pipeline_result(
                DeliveryPipelineResult.failed(context=context, error=error, reason="legacy_video_delivery_failed"),
                context=context,
                input_data=input_data,
            )

        return self._normalize_legacy_result(legacy_result, context=context, input_data=input_data)

    def _normalize_legacy_result(
        self,
        legacy_result: Any,
        *,
        context: DeliveryContext | None,
        input_data: LegacyVideoDeliveryInput,
    ) -> DeliveryFinalizationResult:
        if isinstance(legacy_result, DeliveryFinalizationResult):
            return legacy_result
        if isinstance(legacy_result, DeliveryPipelineResult):
            return self._finalize_pipeline_result(legacy_result, context=context, input_data=input_data)
        if isinstance(legacy_result, TelegramSendResult):
            return self._finalize_pipeline_result(
                DeliveryPipelineResult.sent(
                    context=context,
                    sent_message_ids=legacy_result.sent_message_ids,
                    reason=None,
                ),
                context=context,
                input_data=input_data,
            )
        if legacy_result is None:
            return self._finalize_pipeline_result(
                DeliveryPipelineResult.failed(context=context, reason="legacy_video_delivery_returned_none"),
                context=context,
                input_data=input_data,
            )
        return self._finalize_pipeline_result(
            DeliveryPipelineResult.failed(context=context, reason="unsupported_legacy_video_delivery_result"),
            context=context,
            input_data=input_data,
        )

    def _finalize_pipeline_result(
        self,
        pipeline_result: DeliveryPipelineResult,
        *,
        context: DeliveryContext | None,
        input_data: LegacyVideoDeliveryInput,
    ) -> DeliveryFinalizationResult:
        return self.finalizer.finalize(
            pipeline_result=pipeline_result,
            context=context,
            idempotency_key=input_data.idempotency_key,
            target_id=input_data.target_chat_id,
        )

    def _validate_input(self, input_data: LegacyVideoDeliveryInput) -> str | None:
        if self.legacy_video_delivery is None:
            return "legacy_video_delivery_not_configured"
        if input_data.context is None:
            return "missing_context"
        if input_data.target_chat_id is None:
            return "missing_target_chat_id"
        return None


__all__ = ["LegacyVideoDeliveryInput", "LegacyVideoDeliveryPipeline"]
