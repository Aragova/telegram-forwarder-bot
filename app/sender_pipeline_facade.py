from __future__ import annotations

from dataclasses import dataclass
from typing import Any

_SAFE_TEXT_LIMIT = 500
_REDACTED = "[redacted]"
_SECRET_MARKERS = ("SECRET_TOKEN", "TOKEN", "PRIVATE", "PASSWORD", "AUTH")


@dataclass(frozen=True, slots=True)
class SenderPipelineFeatureFlags:
    enable_repost_single_pipeline: bool = False
    enable_repost_album_pipeline: bool = False
    enable_video_send_pipeline: bool = False
    enable_legacy_video_delivery_pipeline: bool = False
    enable_repost_campaign_pipeline: bool = False
    enable_reaction_post_send_service: bool = False


@dataclass(frozen=True, slots=True)
class SenderPipelineFacadeResult:
    handled: bool
    pipeline_name: str | None = None
    result: object | None = None
    reason: str | None = None
    error_type: str | None = None
    error_text: str | None = None

    @classmethod
    def not_handled(
        cls,
        reason: str,
        *,
        pipeline_name: str | None = None,
        error_type: str | None = None,
        error_text: str | None = None,
    ) -> "SenderPipelineFacadeResult":
        return cls(
            handled=False,
            pipeline_name=pipeline_name,
            reason=reason,
            error_type=_safe_text(error_type),
            error_text=_safe_text(error_text),
        )

    @classmethod
    def handled(cls, *, pipeline_name: str, result: object | None = None) -> "SenderPipelineFacadeResult":
        return cls(handled=True, pipeline_name=pipeline_name, result=result)

    def to_log_context(self) -> dict[str, object]:
        return {
            "handled": self.handled,
            "pipeline_name": self.pipeline_name,
            "reason": self.reason,
            "error_type": self.error_type,
            "error_text": self.error_text,
        }

    def log_label(self) -> str:
        parts = [f"handled={self.handled}"]
        if self.pipeline_name:
            parts.append(f"pipeline={self.pipeline_name}")
        if self.reason:
            parts.append(f"reason={self.reason}")
        if self.error_type:
            parts.append(f"error_type={self.error_type}")
        return "sender_pipeline_facade(" + ", ".join(parts) + ")"


class SenderPipelineFacade:
    def __init__(
        self,
        *,
        flags: SenderPipelineFeatureFlags | None = None,
        repost_single_pipeline: Any | None = None,
        repost_album_pipeline: Any | None = None,
        video_send_pipeline: Any | None = None,
        legacy_video_delivery_pipeline: Any | None = None,
        repost_campaign_pipeline: Any | None = None,
        reaction_post_send_service: Any | None = None,
    ) -> None:
        self.flags = flags or SenderPipelineFeatureFlags()
        self.repost_single_pipeline = repost_single_pipeline
        self.repost_album_pipeline = repost_album_pipeline
        self.video_send_pipeline = video_send_pipeline
        self.legacy_video_delivery_pipeline = legacy_video_delivery_pipeline
        self.repost_campaign_pipeline = repost_campaign_pipeline
        self.reaction_post_send_service = reaction_post_send_service

    async def try_handle_repost_single(self, input_data: object | None = None) -> SenderPipelineFacadeResult:
        return await self._try_handle(
            enabled=self.flags.enable_repost_single_pipeline,
            dependency=self.repost_single_pipeline,
            pipeline_name="repost_single_pipeline",
            input_data=input_data,
        )

    async def try_handle_repost_album(self, input_data: object | None = None) -> SenderPipelineFacadeResult:
        return await self._try_handle(
            enabled=self.flags.enable_repost_album_pipeline,
            dependency=self.repost_album_pipeline,
            pipeline_name="repost_album_pipeline",
            input_data=input_data,
        )

    async def try_handle_video_send(self, input_data: object | None = None) -> SenderPipelineFacadeResult:
        return await self._try_handle(
            enabled=self.flags.enable_video_send_pipeline,
            dependency=self.video_send_pipeline,
            pipeline_name="video_send_pipeline",
            input_data=input_data,
        )

    async def try_handle_legacy_video_delivery(self, input_data: object | None = None) -> SenderPipelineFacadeResult:
        return await self._try_handle(
            enabled=self.flags.enable_legacy_video_delivery_pipeline,
            dependency=self.legacy_video_delivery_pipeline,
            pipeline_name="legacy_video_delivery_pipeline",
            input_data=input_data,
        )

    async def try_handle_repost_campaign(self, input_data: object | None = None) -> SenderPipelineFacadeResult:
        return await self._try_handle(
            enabled=self.flags.enable_repost_campaign_pipeline,
            dependency=self.repost_campaign_pipeline,
            pipeline_name="repost_campaign_pipeline",
            input_data=input_data,
        )

    async def try_handle_reactions(self, input_data: object | None = None) -> SenderPipelineFacadeResult:
        return await self._try_handle(
            enabled=self.flags.enable_reaction_post_send_service,
            dependency=self.reaction_post_send_service,
            pipeline_name="reaction_post_send_service",
            input_data=input_data,
        )

    async def _try_handle(
        self,
        *,
        enabled: bool,
        dependency: Any | None,
        pipeline_name: str,
        input_data: object | None,
    ) -> SenderPipelineFacadeResult:
        if not enabled:
            return SenderPipelineFacadeResult.not_handled("pipeline_disabled", pipeline_name=pipeline_name)
        run = getattr(dependency, "run", None)
        if dependency is None or not callable(run):
            return SenderPipelineFacadeResult.not_handled("pipeline_not_configured", pipeline_name=pipeline_name)
        try:
            result = await run(input_data)
        except Exception as error:
            return SenderPipelineFacadeResult.not_handled(
                "pipeline_failed",
                pipeline_name=pipeline_name,
                error_type=type(error).__name__,
                error_text=str(error),
            )
        return SenderPipelineFacadeResult(handled=True, pipeline_name=pipeline_name, result=result)


def _safe_text(value: object | None) -> str | None:
    if value is None:
        return None
    text = str(value)
    upper_text = text.upper()
    for marker in _SECRET_MARKERS:
        if marker in upper_text:
            return _REDACTED
    if len(text) > _SAFE_TEXT_LIMIT:
        return text[:_SAFE_TEXT_LIMIT]
    return text
