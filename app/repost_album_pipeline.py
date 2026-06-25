from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from app.delivery_context import DeliveryContext
from app.delivery_finalizer import DeliveryFinalizationResult, DeliveryFinalizationSource, DeliveryFinalizer
from app.delivery_pipeline_result import DeliveryPipelineResult
from app.transport_policy import TransportRateLimited


@dataclass(frozen=True, slots=True)
class RepostAlbumInput:
    """Safe input for one future album repost/copy pipeline run."""

    context: DeliveryContext | None
    source_chat_id: int | str | None
    source_message_ids: tuple[int, ...] | list[int] | Iterable[int] | int | None
    target_chat_id: int | str | None
    target_thread_id: int | None = None
    idempotency_key: str | None = None
    telegram_method: str | None = None
    disable_notification: bool | None = None
    protect_content: bool | None = None
    allow_sending_without_reply: bool | None = None

    def to_log_context(self) -> dict[str, object]:
        source_message_ids = normalize_source_message_ids(self.source_message_ids)
        result: dict[str, object] = {
            "source_chat_id": self.source_chat_id,
            "source_message_ids": source_message_ids,
            "source_message_count": len(source_message_ids),
            "target_chat_id": self.target_chat_id,
            "target_thread_id": self.target_thread_id,
            "idempotency_key": self.idempotency_key,
            "telegram_method": self.telegram_method,
        }
        if self.context is not None:
            result["context"] = self.context.to_log_context()
        return result


def normalize_source_message_ids(value: Iterable[int] | int | None) -> tuple[int, ...]:
    if value is None:
        return ()
    if isinstance(value, int):
        return (int(value),)
    return tuple(int(message_id) for message_id in value)


class RepostAlbumPipeline:
    """Foundation pipeline for one album repost/copy logical delivery item.

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
        input_data: RepostAlbumInput,
        *,
        mark_attempt_accepted: bool = True,
        verify_target: bool = True,
    ) -> DeliveryFinalizationResult:
        source_message_ids = normalize_source_message_ids(input_data.source_message_ids)
        validation_error = self._validate_input(input_data, source_message_ids)
        context = input_data.context.with_operation("copy_album") if input_data.context is not None else None
        if validation_error is not None:
            return self._finalize_pipeline_result(
                DeliveryPipelineResult.failed(context=context, reason=validation_error),
                context=context,
                input_data=input_data,
            )

        sent_message_ids: list[int] = []
        for source_message_id in source_message_ids:
            try:
                send_result = await self.send_gateway.copy_message(
                    **self._copy_message_kwargs(input_data, source_message_id)
                )
            except TransportRateLimited as error:
                return DeliveryFinalizationResult.rate_limited(
                    source=DeliveryFinalizationSource.PIPELINE,
                    context=context,
                    sent_message_ids=tuple(sent_message_ids),
                    target_id=input_data.target_chat_id,
                    idempotency_key=input_data.idempotency_key,
                    pipeline_status="rate_limited",
                    retry_after=error.retry_after_seconds,
                    reason="copy_album_rate_limited",
                    error_type=error.__class__.__name__,
                    error_text=str(error),
                )
            except Exception as error:
                return DeliveryFinalizationResult.failed(
                    source=DeliveryFinalizationSource.PIPELINE,
                    context=context,
                    sent_message_ids=tuple(sent_message_ids),
                    target_id=input_data.target_chat_id,
                    idempotency_key=input_data.idempotency_key,
                    pipeline_status="failed",
                    reason="copy_album_failed",
                    error_type=error.__class__.__name__,
                    error_text=str(error),
                )
            sent_message_ids.extend(int(message_id) for message_id in getattr(send_result, "sent_message_ids", ()) or ())

        pipeline_result = DeliveryPipelineResult.sent(
            context=context,
            sent_message_ids=tuple(sent_message_ids),
            reason=None,
        )
        if self.post_send_steps is None:
            return self._finalize_pipeline_result(pipeline_result, context=context, input_data=input_data)

        try:
            post_send_result = await self.post_send_steps.run_after_send(
                context=context,
                send_result=None,
                sent_message_ids=tuple(sent_message_ids),
                idempotency_key=input_data.idempotency_key,
                target_id=input_data.target_chat_id,
                telegram_method=input_data.telegram_method or "copy_album",
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
        input_data: RepostAlbumInput,
    ) -> DeliveryFinalizationResult:
        return self.finalizer.finalize(
            pipeline_result=pipeline_result,
            context=context,
            idempotency_key=input_data.idempotency_key,
            target_id=input_data.target_chat_id,
        )

    @staticmethod
    def _validate_input(input_data: RepostAlbumInput, source_message_ids: tuple[int, ...]) -> str | None:
        if input_data.context is None:
            return "missing_context"
        if input_data.source_chat_id is None:
            return "missing_source_chat_id"
        if not source_message_ids:
            return "missing_source_message_ids"
        if input_data.target_chat_id is None:
            return "missing_target_chat_id"
        return None

    @staticmethod
    def _copy_message_kwargs(input_data: RepostAlbumInput, source_message_id: int) -> dict[str, object]:
        kwargs: dict[str, object] = {
            "chat_id": input_data.target_chat_id,
            "from_chat_id": input_data.source_chat_id,
            "message_id": source_message_id,
        }
        optional_values = {
            "message_thread_id": input_data.target_thread_id,
            "disable_notification": input_data.disable_notification,
            "protect_content": input_data.protect_content,
            "allow_sending_without_reply": input_data.allow_sending_without_reply,
        }
        kwargs.update({key: value for key, value in optional_values.items() if value is not None})
        return kwargs


__all__ = ["RepostAlbumInput", "RepostAlbumPipeline", "normalize_source_message_ids"]
