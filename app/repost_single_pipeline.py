from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from app.delivery_context import DeliveryContext
from app.delivery_finalizer import DeliveryFinalizationResult, DeliveryFinalizationSource, DeliveryFinalizer
from app.delivery_pipeline_result import DeliveryPipelineResult
from app.transport_policy import TransportRateLimited


@dataclass(frozen=True, slots=True)
class RepostSingleInput:
    """Safe input for one future repost/copy single-message pipeline run."""

    context: DeliveryContext | None
    source_chat_id: int | str | None
    source_message_id: int | None
    target_chat_id: int | str | None
    target_thread_id: int | None = None
    idempotency_key: str | None = None
    telegram_method: str | None = None
    disable_notification: bool | None = None
    protect_content: bool | None = None
    caption: str | None = None
    parse_mode: str | None = None
    caption_entities: object | None = None
    reply_markup: object | None = None
    allow_sending_without_reply: bool | None = None

    def to_log_context(self) -> dict[str, object]:
        result: dict[str, object] = {
            "source_chat_id": self.source_chat_id,
            "source_message_id": self.source_message_id,
            "target_chat_id": self.target_chat_id,
            "target_thread_id": self.target_thread_id,
            "idempotency_key": self.idempotency_key,
            "telegram_method": self.telegram_method,
        }
        if self.context is not None:
            result["context"] = self.context.to_log_context()
        return result


class RepostSinglePipeline:
    """Foundation pipeline for one repost/copy message.

    The pipeline is intentionally dependency-injected and not connected to
    runtime wiring yet. It does not create Telegram clients, repositories,
    transport policy, retries, or database/audit side effects.
    """

    def __init__(
        self,
        *,
        send_gateway: Any,
        post_send_steps: Any | None = None,
        finalizer: Any | None = None,
    ) -> None:
        self.send_gateway = send_gateway
        self.post_send_steps = post_send_steps
        self.finalizer = finalizer or DeliveryFinalizer()

    async def run(
        self,
        input_data: RepostSingleInput,
        *,
        mark_attempt_accepted: bool = True,
        verify_target: bool = True,
    ) -> DeliveryFinalizationResult:
        validation_error = self._validate_input(input_data)
        context = input_data.context.with_operation("copy_message") if input_data.context is not None else None
        if validation_error is not None:
            return self._finalize_pipeline_result(
                DeliveryPipelineResult.failed(context=context, reason=validation_error),
                context=context,
                input_data=input_data,
            )

        try:
            send_result = await self.send_gateway.copy_message(**self._copy_message_kwargs(input_data))
        except TransportRateLimited as error:
            return self._finalize_pipeline_result(
                DeliveryPipelineResult.rate_limited(
                    context=context,
                    retry_after=error.retry_after_seconds,
                    error=error,
                    reason="copy_message_rate_limited",
                ),
                context=context,
                input_data=input_data,
            )
        except Exception as error:
            return self._finalize_pipeline_result(
                DeliveryPipelineResult.failed(context=context, error=error, reason="copy_message_failed"),
                context=context,
                input_data=input_data,
            )

        pipeline_result = DeliveryPipelineResult.sent(
            context=context,
            sent_message_ids=send_result.sent_message_ids,
            reason=None,
        )
        if self.post_send_steps is None:
            return self._finalize_pipeline_result(pipeline_result, context=context, input_data=input_data)

        try:
            post_send_result = await self.post_send_steps.run_after_send(
                context=context,
                send_result=send_result,
                sent_message_ids=send_result.sent_message_ids,
                idempotency_key=input_data.idempotency_key,
                target_id=input_data.target_chat_id,
                telegram_method=input_data.telegram_method or "copy_message",
                mark_attempt_accepted=mark_attempt_accepted,
                verify_target=verify_target,
            )
        except Exception as error:
            return DeliveryFinalizationResult.failed(
                source=DeliveryFinalizationSource.POST_SEND,
                context=context,
                sent_message_ids=pipeline_result.sent_message_ids,
                target_id=input_data.target_chat_id,
                idempotency_key=input_data.idempotency_key,
                pipeline_status=pipeline_result.status.value,
                reason="post_send_steps_failed",
                error_type=error.__class__.__name__,
                error_text=str(error),
            )

        return self.finalizer.finalize(
            pipeline_result=pipeline_result,
            post_send_result=post_send_result,
            context=context,
            idempotency_key=input_data.idempotency_key,
            target_id=input_data.target_chat_id,
        )

    def _finalize_pipeline_result(
        self,
        pipeline_result: DeliveryPipelineResult,
        *,
        context: DeliveryContext | None,
        input_data: RepostSingleInput,
    ) -> DeliveryFinalizationResult:
        return self.finalizer.finalize(
            pipeline_result=pipeline_result,
            context=context,
            idempotency_key=input_data.idempotency_key,
            target_id=input_data.target_chat_id,
        )

    @staticmethod
    def _validate_input(input_data: RepostSingleInput) -> str | None:
        if input_data.context is None:
            return "missing_context"
        if input_data.source_chat_id is None:
            return "missing_source_chat_id"
        if input_data.source_message_id is None:
            return "missing_source_message_id"
        if input_data.target_chat_id is None:
            return "missing_target_chat_id"
        return None

    @staticmethod
    def _copy_message_kwargs(input_data: RepostSingleInput) -> dict[str, object]:
        kwargs: dict[str, object] = {
            "chat_id": input_data.target_chat_id,
            "from_chat_id": input_data.source_chat_id,
            "message_id": input_data.source_message_id,
        }
        optional_values = {
            "message_thread_id": input_data.target_thread_id,
            "disable_notification": input_data.disable_notification,
            "protect_content": input_data.protect_content,
            "caption": input_data.caption,
            "parse_mode": input_data.parse_mode,
            "caption_entities": input_data.caption_entities,
            "reply_markup": input_data.reply_markup,
            "allow_sending_without_reply": input_data.allow_sending_without_reply,
        }
        kwargs.update({key: value for key, value in optional_values.items() if value is not None})
        return kwargs


__all__ = ["RepostSingleInput", "RepostSinglePipeline"]
