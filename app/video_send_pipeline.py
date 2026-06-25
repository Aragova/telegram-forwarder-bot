from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any

from app.delivery_context import DeliveryContext
from app.delivery_finalizer import DeliveryFinalizationResult, DeliveryFinalizationSource, DeliveryFinalizer
from app.delivery_pipeline_result import DeliveryPipelineResult
from app.transport_policy import TransportRateLimited


class VideoSendMethod(str, Enum):
    BOT_SEND_VIDEO = "bot_send_video"
    BOT_SEND_DOCUMENT = "bot_send_document"
    TELETHON_SEND_FILE = "telethon_send_file"


@dataclass(frozen=True, slots=True)
class VideoSendInput:
    """Safe input for sending an already prepared video object.

    The input is intentionally payload-free: it may keep functional send
    parameters, but safe log helpers must not expose raw video, thumbnail,
    caption, entities, reply markup, Telegram objects, or runtime services.
    """

    context: DeliveryContext | None
    target_chat_id: int | str | None
    target_thread_id: int | None
    video: Any | None
    method: VideoSendMethod | None
    idempotency_key: str | None = None
    telegram_method: str | None = None
    caption: str | None = None
    caption_entities: object | None = None
    parse_mode: str | None = None
    thumbnail: Any | None = None
    duration: int | None = None
    width: int | None = None
    height: int | None = None
    supports_streaming: bool | None = None
    disable_notification: bool | None = None
    protect_content: bool | None = None
    reply_markup: object | None = None
    allow_sending_without_reply: bool | None = None

    def to_log_context(self) -> dict[str, object]:
        result: dict[str, object] = {
            "target_chat_id": self.target_chat_id,
            "target_thread_id": self.target_thread_id,
            "method": self.method.value if isinstance(self.method, VideoSendMethod) else self.method,
            "idempotency_key": self.idempotency_key,
            "telegram_method": self.telegram_method,
            "has_video": self.video is not None,
            "has_caption": self.caption is not None,
            "has_caption_entities": self.caption_entities is not None,
            "has_thumbnail": self.thumbnail is not None,
            "duration": self.duration,
            "width": self.width,
            "height": self.height,
            "supports_streaming": self.supports_streaming,
            "disable_notification": self.disable_notification,
            "protect_content": self.protect_content,
        }
        if self.context is not None:
            result["context"] = self.context.to_log_context()
        return result


class VideoSendPipeline:
    """Foundation pipeline for sending an already prepared video.

    The pipeline is dependency-injected and is not connected to runtime wiring
    yet. It does not create Telegram clients, repositories, transport policy,
    retries, database/audit side effects, ffmpeg processes, or temp files.
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
        input_data: VideoSendInput,
        *,
        mark_attempt_accepted: bool = True,
        verify_target: bool = True,
    ) -> DeliveryFinalizationResult:
        validation_error = self._validate_input(input_data)
        operation_name = self._operation_name(input_data.method)
        context = input_data.context.with_operation(operation_name) if input_data.context is not None else None
        if validation_error is not None:
            return self._finalize_pipeline_result(
                DeliveryPipelineResult.failed(context=context, reason=validation_error),
                context=context,
                input_data=input_data,
            )

        try:
            send_result = await self._send(input_data)
        except TransportRateLimited as error:
            return self._finalize_pipeline_result(
                DeliveryPipelineResult.rate_limited(
                    context=context,
                    retry_after=error.retry_after_seconds,
                    error=error,
                    reason="video_send_rate_limited",
                ),
                context=context,
                input_data=input_data,
            )
        except Exception as error:
            return self._finalize_pipeline_result(
                DeliveryPipelineResult.failed(context=context, error=error, reason="video_send_failed"),
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
                telegram_method=input_data.telegram_method or operation_name,
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

    async def _send(self, input_data: VideoSendInput) -> Any:
        if input_data.method == VideoSendMethod.BOT_SEND_VIDEO:
            return await self.send_gateway.send_video(**self._send_video_kwargs(input_data))
        if input_data.method == VideoSendMethod.BOT_SEND_DOCUMENT:
            return await self.send_gateway.send_document(**self._send_document_kwargs(input_data))
        if input_data.method == VideoSendMethod.TELETHON_SEND_FILE:
            return await self.send_gateway.telethon_send_file(**self._telethon_send_file_kwargs(input_data))
        raise ValueError("missing_video_send_method")

    def _finalize_pipeline_result(
        self,
        pipeline_result: DeliveryPipelineResult,
        *,
        context: DeliveryContext | None,
        input_data: VideoSendInput,
    ) -> DeliveryFinalizationResult:
        return self.finalizer.finalize(
            pipeline_result=pipeline_result,
            context=context,
            idempotency_key=input_data.idempotency_key,
            target_id=input_data.target_chat_id,
        )

    @staticmethod
    def _validate_input(input_data: VideoSendInput) -> str | None:
        if input_data.context is None:
            return "missing_context"
        if input_data.target_chat_id is None:
            return "missing_target_chat_id"
        if input_data.video is None:
            return "missing_video"
        if input_data.method is None:
            return "missing_video_send_method"
        return None

    @staticmethod
    def _operation_name(method: VideoSendMethod | None) -> str:
        if method == VideoSendMethod.BOT_SEND_DOCUMENT:
            return "send_document"
        if method == VideoSendMethod.TELETHON_SEND_FILE:
            return "telethon_send_file"
        return "send_video"

    @staticmethod
    def _send_video_kwargs(input_data: VideoSendInput) -> dict[str, object]:
        return _clean_kwargs(
            chat_id=input_data.target_chat_id,
            video=input_data.video,
            message_thread_id=input_data.target_thread_id,
            caption=input_data.caption,
            caption_entities=input_data.caption_entities,
            parse_mode=input_data.parse_mode,
            thumbnail=input_data.thumbnail,
            duration=input_data.duration,
            width=input_data.width,
            height=input_data.height,
            supports_streaming=input_data.supports_streaming,
            disable_notification=input_data.disable_notification,
            protect_content=input_data.protect_content,
            reply_markup=input_data.reply_markup,
            allow_sending_without_reply=input_data.allow_sending_without_reply,
        )

    @staticmethod
    def _send_document_kwargs(input_data: VideoSendInput) -> dict[str, object]:
        return _clean_kwargs(
            chat_id=input_data.target_chat_id,
            document=input_data.video,
            message_thread_id=input_data.target_thread_id,
            caption=input_data.caption,
            caption_entities=input_data.caption_entities,
            parse_mode=input_data.parse_mode,
            disable_notification=input_data.disable_notification,
            protect_content=input_data.protect_content,
            reply_markup=input_data.reply_markup,
            allow_sending_without_reply=input_data.allow_sending_without_reply,
        )

    @staticmethod
    def _telethon_send_file_kwargs(input_data: VideoSendInput) -> dict[str, object]:
        return _clean_kwargs(
            entity=input_data.target_chat_id,
            file=input_data.video,
            caption=input_data.caption,
        )


def _clean_kwargs(**kwargs: object) -> dict[str, object]:
    return {key: value for key, value in kwargs.items() if value is not None}


__all__ = ["VideoSendInput", "VideoSendMethod", "VideoSendPipeline"]
