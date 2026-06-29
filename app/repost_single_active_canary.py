from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Any

from .delivery_context import DeliveryContext
from .repost_single_pipeline import RepostSingleInput, RepostSinglePipeline
from .repost_single_rollout_probe import RepostSingleProbeResult
from .sender_pipeline_rollout import SenderRolloutAction, SenderRolloutMode

logger = logging.getLogger("forwarder")


class RepostSingleActiveCanaryStatus(str, Enum):
    DISABLED = "disabled"
    NOT_SELECTED = "not_selected"
    NOT_READY = "not_ready"
    ACTIVE_GUARD_FAILED = "active_guard_failed"
    HANDLED = "handled"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class RepostSingleActiveCanaryResult:
    status: RepostSingleActiveCanaryStatus
    attempted_pipeline: bool = False
    should_continue_legacy: bool = True
    reason: str | None = None
    rule_id: int | str | None = None
    delivery_id: int | None = None
    pipeline_status: str | None = None

    def __post_init__(self) -> None:
        if self.attempted_pipeline and self.should_continue_legacy:
            raise ValueError("attempted_pipeline_requires_legacy_stop")


@dataclass(frozen=True, slots=True)
class RepostSingleActiveCanaryConfig:
    canary_enabled: bool = False
    enabled_rule_ids: tuple[int | str, ...] = ()

    @property
    def exactly_one_enabled_rule_id(self) -> bool:
        return len(self.enabled_rule_ids) == 1

    @property
    def single_rule_id(self) -> int | str | None:
        return self.enabled_rule_ids[0] if self.exactly_one_enabled_rule_id else None


class RepostSingleActiveCanaryRunner:
    def __init__(self, *, pipeline: RepostSinglePipeline, config: RepostSingleActiveCanaryConfig | None = None) -> None:
        self.pipeline = pipeline
        self.config = config or RepostSingleActiveCanaryConfig()

    async def try_run(
        self,
        *,
        probe_result: RepostSingleProbeResult | None,
        rule: Any,
        delivery_id: int | None,
        message_id: int | None,
        source_channel: int | str | None,
        target_id: int | str | None,
        target_thread_id: int | None = None,
        post_id: int | None = None,
        idempotency_key: str | None = None,
        caption_mode: str | None = None,
        requires_builder: bool = False,
        use_copy_first: bool = True,
        unsupported_features: tuple[str, ...] = (),
    ) -> RepostSingleActiveCanaryResult:
        rule_id = getattr(rule, "id", None)
        result = self._preflight(
            probe_result=probe_result,
            rule_id=rule_id,
            delivery_id=delivery_id,
            message_id=message_id,
            source_channel=source_channel,
            target_id=target_id,
            requires_builder=requires_builder,
            use_copy_first=use_copy_first,
            unsupported_features=unsupported_features,
        )
        if result is not None:
            self._log(result)
            return result

        input_data = RepostSingleInput(
            context=DeliveryContext(
                delivery_id=delivery_id,
                rule_id=_int_or_none(rule_id),
                post_id=post_id,
                source_id=source_channel,
                target_id=target_id,
                target_thread_id=target_thread_id,
                message_id=message_id,
                mode="repost",
                operation="active_canary_repost_single",
                is_album=False,
            ),
            source_chat_id=source_channel,
            source_message_id=message_id,
            target_chat_id=target_id,
            target_thread_id=target_thread_id,
            idempotency_key=idempotency_key,
            telegram_method="copy_message",
        )

        try:
            pipeline_result = await self.pipeline.run(input_data)
        except Exception as exc:
            result = RepostSingleActiveCanaryResult(
                status=RepostSingleActiveCanaryStatus.FAILED,
                attempted_pipeline=True,
                should_continue_legacy=False,
                reason=f"pipeline_exception:{type(exc).__name__}",
                rule_id=rule_id,
                delivery_id=delivery_id,
            )
            self._log(result)
            return result

        status = getattr(getattr(pipeline_result, "status", None), "value", None) or str(getattr(pipeline_result, "status", "unknown"))
        ok = bool(getattr(pipeline_result, "ok", False)) or status == "finalized"
        result = RepostSingleActiveCanaryResult(
            status=RepostSingleActiveCanaryStatus.HANDLED if ok else RepostSingleActiveCanaryStatus.FAILED,
            attempted_pipeline=True,
            should_continue_legacy=False,
            reason=None if ok else (getattr(pipeline_result, "reason", None) or "pipeline_failed"),
            rule_id=rule_id,
            delivery_id=delivery_id,
            pipeline_status=status,
        )
        self._log(result)
        return result

    def _preflight(self, **kwargs: Any) -> RepostSingleActiveCanaryResult | None:
        probe_result = kwargs["probe_result"]
        rule_id = kwargs["rule_id"]
        delivery_id = kwargs["delivery_id"]
        decision = getattr(probe_result, "decision", None)
        if decision is None or decision.action is not SenderRolloutAction.USE_PIPELINE or decision.mode is not SenderRolloutMode.ACTIVE:
            return RepostSingleActiveCanaryResult(RepostSingleActiveCanaryStatus.DISABLED, reason="not_active_use_pipeline", rule_id=rule_id, delivery_id=delivery_id)
        if not self.config.canary_enabled:
            return RepostSingleActiveCanaryResult(RepostSingleActiveCanaryStatus.ACTIVE_GUARD_FAILED, reason="active_canary_not_enabled", rule_id=rule_id, delivery_id=delivery_id)
        if not self.config.exactly_one_enabled_rule_id:
            return RepostSingleActiveCanaryResult(RepostSingleActiveCanaryStatus.ACTIVE_GUARD_FAILED, reason="active_canary_requires_exactly_one_rule_id", rule_id=rule_id, delivery_id=delivery_id)
        if rule_id != self.config.single_rule_id:
            return RepostSingleActiveCanaryResult(RepostSingleActiveCanaryStatus.NOT_SELECTED, reason="rule_not_active_canary", rule_id=rule_id, delivery_id=delivery_id)
        for name in kwargs["unsupported_features"]:
            return RepostSingleActiveCanaryResult(RepostSingleActiveCanaryStatus.NOT_READY, reason=f"unsupported_feature:{name}", rule_id=rule_id, delivery_id=delivery_id)
        if kwargs["requires_builder"] or not kwargs["use_copy_first"]:
            return RepostSingleActiveCanaryResult(RepostSingleActiveCanaryStatus.NOT_READY, reason="unsupported_feature:caption_builder", rule_id=rule_id, delivery_id=delivery_id)
        missing = _missing_required_reason(rule_id=rule_id, delivery_id=delivery_id, source_channel=kwargs["source_channel"], target_id=kwargs["target_id"], message_id=kwargs["message_id"])
        if missing:
            return RepostSingleActiveCanaryResult(RepostSingleActiveCanaryStatus.NOT_READY, reason=missing, rule_id=rule_id, delivery_id=delivery_id)
        return None

    @staticmethod
    def _log(result: RepostSingleActiveCanaryResult) -> None:
        logger.info(
            "SENDER_ROLLOUT | repost_single active_canary | status=%s | reason=%s | rule_id=%s | delivery_id=%s | pipeline_status=%s",
            result.status.value,
            result.reason,
            result.rule_id,
            result.delivery_id,
            result.pipeline_status,
        )


def _missing_required_reason(*, rule_id: int | str | None, delivery_id: int | None, source_channel: int | str | None, target_id: int | str | None, message_id: int | None) -> str | None:
    if rule_id is None or rule_id == "":
        return "missing_rule_id"
    if delivery_id is None:
        return "missing_delivery_id"
    if source_channel is None or source_channel == "":
        return "missing_source_id"
    if target_id is None or target_id == "":
        return "missing_target_id"
    if message_id is None:
        return "missing_source_message_id"
    return None


def _int_or_none(value: int | str | None) -> int | None:
    try:
        return int(value) if value is not None and value != "" else None
    except (TypeError, ValueError):
        return None


__all__ = [
    "RepostSingleActiveCanaryConfig",
    "RepostSingleActiveCanaryResult",
    "RepostSingleActiveCanaryRunner",
    "RepostSingleActiveCanaryStatus",
]
