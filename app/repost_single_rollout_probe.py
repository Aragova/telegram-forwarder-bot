from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from .sender_pipeline_rollout import (
    SenderPipelineName,
    SenderPipelineRolloutDecision,
    SenderPipelineRolloutStrategy,
    SenderRolloutAction,
)


class RepostSingleProbeStatus(str, Enum):
    DISABLED = "disabled"
    READY = "ready"
    NOT_READY = "not_ready"
    ACTIVE_NOT_ENABLED = "active_not_enabled"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class RepostSingleProbeResult:
    status: RepostSingleProbeStatus
    decision: SenderPipelineRolloutDecision | None = None
    reason: str | None = None
    rule_id: int | str | None = None
    source_id: int | str | None = None
    target_id: int | str | None = None
    source_message_id: int | None = None

    @property
    def ok(self) -> bool:
        return self.status is RepostSingleProbeStatus.READY

    def to_log_context(self) -> dict[str, object]:
        decision_context = self.decision.to_log_context() if self.decision is not None else {}
        return {
            "status": self.status.value,
            "reason": self.reason,
            "rule_id": self.rule_id,
            "source_id": self.source_id,
            "target_id": self.target_id,
            "source_message_id": self.source_message_id,
            "mode": decision_context.get("mode"),
            "action": decision_context.get("action"),
            "decision_reason": decision_context.get("reason"),
        }

    def log_label(self) -> str:
        action = self.decision.action.value if self.decision is not None else "none"
        mode = self.decision.mode.value if self.decision is not None else "none"
        parts = [
            f"status={self.status.value}",
            f"mode={mode}",
            f"action={action}",
        ]
        if self.rule_id is not None:
            parts.append(f"rule_id={self.rule_id}")
        if self.reason:
            parts.append(f"reason={self.reason}")
        return "repost_single_rollout_probe(" + ", ".join(parts) + ")"


class RepostSingleRolloutProbe:
    def __init__(self, *, rollout_strategy: SenderPipelineRolloutStrategy | None = None):
        self.rollout_strategy = rollout_strategy or SenderPipelineRolloutStrategy()

    def probe(
        self,
        *,
        rule_id: int | str | None,
        source_id: int | str | None,
        target_id: int | str | None,
        source_message_id: int | None,
        target_thread_id: int | None = None,
    ) -> RepostSingleProbeResult:
        try:
            decision = self.rollout_strategy.decide(
                pipeline_name=SenderPipelineName.REPOST_SINGLE,
                rule_id=rule_id,
                source_id=source_id,
                target_id=target_id,
            )
            base = {
                "decision": decision,
                "rule_id": rule_id,
                "source_id": source_id,
                "target_id": target_id,
                "source_message_id": source_message_id,
            }
            missing_reason = _missing_required_reason(
                rule_id=rule_id,
                source_id=source_id,
                target_id=target_id,
                source_message_id=source_message_id,
            )
            if decision.action is SenderRolloutAction.USE_LEGACY:
                if decision.reason == "rollout_disabled" or not missing_reason:
                    return RepostSingleProbeResult(status=RepostSingleProbeStatus.DISABLED, reason=decision.reason, **base)
                return RepostSingleProbeResult(status=RepostSingleProbeStatus.NOT_READY, reason=missing_reason, **base)
            if decision.action is SenderRolloutAction.USE_PIPELINE:
                return RepostSingleProbeResult(status=RepostSingleProbeStatus.ACTIVE_NOT_ENABLED, reason="active_not_enabled_in_stage_27", **base)
            if missing_reason:
                return RepostSingleProbeResult(status=RepostSingleProbeStatus.NOT_READY, reason=missing_reason, **base)
            return RepostSingleProbeResult(status=RepostSingleProbeStatus.READY, reason=decision.reason, **base)
        except Exception as exc:
            return RepostSingleProbeResult(
                status=RepostSingleProbeStatus.FAILED,
                reason=f"probe_failed:{type(exc).__name__}",
                rule_id=rule_id,
                source_id=source_id,
                target_id=target_id,
                source_message_id=source_message_id,
            )


def _missing_required_reason(*, rule_id, source_id, target_id, source_message_id) -> str | None:
    if rule_id is None or rule_id == "":
        return "missing_rule_id"
    if source_id is None or source_id == "":
        return "missing_source_id"
    if target_id is None or target_id == "":
        return "missing_target_id"
    if source_message_id is None:
        return "missing_source_message_id"
    return None
