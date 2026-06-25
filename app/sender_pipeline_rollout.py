from __future__ import annotations

import hashlib
from dataclasses import dataclass
from enum import Enum
from typing import Any

from .sender_pipeline_facade import SenderPipelineFeatureFlags


class SenderPipelineName(str, Enum):
    REPOST_SINGLE = "repost_single"
    REPOST_ALBUM = "repost_album"
    VIDEO_SEND = "video_send"
    LEGACY_VIDEO_DELIVERY = "legacy_video_delivery"
    REPOST_CAMPAIGN = "repost_campaign"
    REACTIONS = "reactions"


class SenderRolloutMode(str, Enum):
    DISABLED = "disabled"
    SHADOW = "shadow"
    DRY_RUN = "dry_run"
    ACTIVE = "active"


class SenderRolloutAction(str, Enum):
    USE_LEGACY = "use_legacy"
    SHADOW_PIPELINE = "shadow_pipeline"
    DRY_RUN_PIPELINE = "dry_run_pipeline"
    USE_PIPELINE = "use_pipeline"


@dataclass(frozen=True, slots=True)
class SenderPipelineRolloutConfig:
    mode: SenderRolloutMode = SenderRolloutMode.DISABLED

    enabled_pipelines: tuple[SenderPipelineName, ...] = ()
    shadow_pipelines: tuple[SenderPipelineName, ...] = ()
    dry_run_pipelines: tuple[SenderPipelineName, ...] = ()

    enabled_rule_ids: tuple[int | str, ...] = ()
    shadow_rule_ids: tuple[int | str, ...] = ()
    dry_run_rule_ids: tuple[int | str, ...] = ()
    blocked_rule_ids: tuple[int | str, ...] = ()

    rollout_percent: int = 0
    require_rule_allowlist: bool = True
    allow_active_without_rule_id: bool = False

    fail_closed: bool = True

    def to_admin_text(self) -> str:
        return _admin_text(self)


@dataclass(frozen=True, slots=True)
class SenderPipelineRolloutDecision:
    pipeline_name: SenderPipelineName
    mode: SenderRolloutMode
    action: SenderRolloutAction

    rule_id: int | str | None = None
    source_id: int | str | None = None
    target_id: int | str | None = None

    should_call_pipeline: bool = False
    should_use_pipeline_result: bool = False
    should_continue_legacy: bool = True
    should_record_shadow_result: bool = False

    reason: str | None = None

    def to_log_context(self) -> dict[str, object]:
        return {
            "pipeline_name": self.pipeline_name.value,
            "mode": self.mode.value,
            "action": self.action.value,
            "rule_id": self.rule_id,
            "source_id": self.source_id,
            "target_id": self.target_id,
            "should_call_pipeline": self.should_call_pipeline,
            "should_use_pipeline_result": self.should_use_pipeline_result,
            "should_continue_legacy": self.should_continue_legacy,
            "should_record_shadow_result": self.should_record_shadow_result,
            "reason": self.reason,
        }

    def log_label(self) -> str:
        parts = [
            f"pipeline={self.pipeline_name.value}",
            f"mode={self.mode.value}",
            f"action={self.action.value}",
        ]
        if self.rule_id is not None:
            parts.append(f"rule_id={self.rule_id}")
        if self.reason:
            parts.append(f"reason={self.reason}")
        return "sender_pipeline_rollout(" + ", ".join(parts) + ")"


class SenderPipelineRolloutStrategy:
    def __init__(self, *, config: SenderPipelineRolloutConfig | None = None) -> None:
        self.config = config or SenderPipelineRolloutConfig()

    def decide(
        self,
        *,
        pipeline_name: SenderPipelineName | str,
        rule_id: int | str | None = None,
        source_id: int | str | None = None,
        target_id: int | str | None = None,
    ) -> SenderPipelineRolloutDecision:
        config = self.config
        pipeline = _coerce_pipeline_name(pipeline_name)
        mode = _coerce_mode(config.mode)
        fallback_pipeline = pipeline or SenderPipelineName.REPOST_SINGLE
        fallback_mode = mode or SenderRolloutMode.DISABLED

        if config.fail_closed and (not _is_valid_percent(config.rollout_percent) or mode is None):
            return _legacy_decision(fallback_pipeline, fallback_mode, rule_id, source_id, target_id, "rollout_config_invalid")
        if pipeline is None:
            reason = "rollout_config_invalid" if config.fail_closed else "unknown_pipeline"
            return _legacy_decision(fallback_pipeline, fallback_mode, rule_id, source_id, target_id, reason)
        if mode is None:
            return _legacy_decision(pipeline, SenderRolloutMode.DISABLED, rule_id, source_id, target_id, "rollout_config_invalid")
        if mode is SenderRolloutMode.DISABLED:
            return _legacy_decision(pipeline, mode, rule_id, source_id, target_id, "rollout_disabled")
        if _contains_rule(config.blocked_rule_ids, rule_id):
            return _legacy_decision(pipeline, mode, rule_id, source_id, target_id, "rule_blocked")
        if not self._pipeline_allowed(mode, pipeline):
            return _legacy_decision(pipeline, mode, rule_id, source_id, target_id, "pipeline_not_allowed")
        if not self._rule_allowed(mode, pipeline, rule_id):
            return _legacy_decision(pipeline, mode, rule_id, source_id, target_id, "rule_not_allowed")

        if mode is SenderRolloutMode.SHADOW:
            return SenderPipelineRolloutDecision(
                pipeline_name=pipeline,
                mode=mode,
                action=SenderRolloutAction.SHADOW_PIPELINE,
                rule_id=rule_id,
                source_id=source_id,
                target_id=target_id,
                should_call_pipeline=True,
                should_continue_legacy=True,
                should_record_shadow_result=True,
                reason="shadow_enabled",
            )
        if mode is SenderRolloutMode.DRY_RUN:
            return SenderPipelineRolloutDecision(
                pipeline_name=pipeline,
                mode=mode,
                action=SenderRolloutAction.DRY_RUN_PIPELINE,
                rule_id=rule_id,
                source_id=source_id,
                target_id=target_id,
                should_continue_legacy=True,
                reason="dry_run_enabled",
            )
        return SenderPipelineRolloutDecision(
            pipeline_name=pipeline,
            mode=mode,
            action=SenderRolloutAction.USE_PIPELINE,
            rule_id=rule_id,
            source_id=source_id,
            target_id=target_id,
            should_call_pipeline=True,
            should_use_pipeline_result=True,
            should_continue_legacy=False,
            reason="active_enabled",
        )

    def to_feature_flags(self) -> SenderPipelineFeatureFlags:
        mode = _coerce_mode(self.config.mode)
        if mode is SenderRolloutMode.SHADOW:
            pipelines = _pipeline_set(self.config.shadow_pipelines) | _pipeline_set(self.config.enabled_pipelines)
        elif mode is SenderRolloutMode.ACTIVE:
            pipelines = _pipeline_set(self.config.enabled_pipelines)
        else:
            pipelines = set()
        return SenderPipelineFeatureFlags(
            enable_repost_single_pipeline=SenderPipelineName.REPOST_SINGLE in pipelines,
            enable_repost_album_pipeline=SenderPipelineName.REPOST_ALBUM in pipelines,
            enable_video_send_pipeline=SenderPipelineName.VIDEO_SEND in pipelines,
            enable_legacy_video_delivery_pipeline=SenderPipelineName.LEGACY_VIDEO_DELIVERY in pipelines,
            enable_repost_campaign_pipeline=SenderPipelineName.REPOST_CAMPAIGN in pipelines,
            enable_reaction_post_send_service=SenderPipelineName.REACTIONS in pipelines,
        )

    def to_admin_text(self) -> str:
        return _admin_text(self.config)

    def _pipeline_allowed(self, mode: SenderRolloutMode, pipeline: SenderPipelineName) -> bool:
        enabled = _pipeline_set(self.config.enabled_pipelines)
        if mode is SenderRolloutMode.ACTIVE:
            return pipeline in enabled
        if mode is SenderRolloutMode.SHADOW:
            return pipeline in enabled or pipeline in _pipeline_set(self.config.shadow_pipelines)
        if mode is SenderRolloutMode.DRY_RUN:
            return pipeline in enabled or pipeline in _pipeline_set(self.config.shadow_pipelines) or pipeline in _pipeline_set(self.config.dry_run_pipelines)
        return False

    def _rule_allowed(self, mode: SenderRolloutMode, pipeline: SenderPipelineName, rule_id: int | str | None) -> bool:
        if not self.config.require_rule_allowlist:
            if mode is SenderRolloutMode.ACTIVE and self.config.rollout_percent > 0:
                return is_rule_selected_by_percent(rule_id=rule_id, pipeline_name=pipeline, rollout_percent=self.config.rollout_percent)
            return mode is not SenderRolloutMode.ACTIVE or rule_id is not None or self.config.allow_active_without_rule_id
        if rule_id is None:
            return mode is not SenderRolloutMode.ACTIVE and self.config.allow_active_without_rule_id
        enabled = set(self.config.enabled_rule_ids)
        if mode is SenderRolloutMode.ACTIVE:
            return rule_id in enabled
        if mode is SenderRolloutMode.SHADOW:
            return rule_id in enabled or rule_id in set(self.config.shadow_rule_ids)
        if mode is SenderRolloutMode.DRY_RUN:
            return rule_id in enabled or rule_id in set(self.config.shadow_rule_ids) or rule_id in set(self.config.dry_run_rule_ids)
        return False


def is_rule_selected_by_percent(*, rule_id: int | str | None, pipeline_name: SenderPipelineName, rollout_percent: int) -> bool:
    if rule_id is None or rollout_percent <= 0:
        return False
    if rollout_percent >= 100:
        return True
    digest = hashlib.sha256(f"{pipeline_name.value}:{rule_id}".encode("utf-8")).hexdigest()
    bucket = int(digest[:8], 16) % 100
    return bucket < rollout_percent


def _legacy_decision(pipeline: SenderPipelineName, mode: SenderRolloutMode, rule_id: Any, source_id: Any, target_id: Any, reason: str) -> SenderPipelineRolloutDecision:
    return SenderPipelineRolloutDecision(
        pipeline_name=pipeline,
        mode=mode,
        action=SenderRolloutAction.USE_LEGACY,
        rule_id=rule_id,
        source_id=source_id,
        target_id=target_id,
        should_continue_legacy=True,
        reason=reason,
    )


def _coerce_pipeline_name(value: SenderPipelineName | str) -> SenderPipelineName | None:
    if isinstance(value, SenderPipelineName):
        return value
    try:
        return SenderPipelineName(str(value))
    except ValueError:
        return None


def _coerce_mode(value: SenderRolloutMode | str) -> SenderRolloutMode | None:
    if isinstance(value, SenderRolloutMode):
        return value
    try:
        return SenderRolloutMode(str(value))
    except ValueError:
        return None


def _pipeline_set(values: tuple[SenderPipelineName, ...]) -> set[SenderPipelineName]:
    return {pipeline for value in values if (pipeline := _coerce_pipeline_name(value)) is not None}


def _contains_rule(values: tuple[int | str, ...], rule_id: int | str | None) -> bool:
    return rule_id is not None and rule_id in set(values)


def _is_valid_percent(value: int) -> bool:
    return isinstance(value, int) and 0 <= value <= 100


def _admin_text(config: SenderPipelineRolloutConfig) -> str:
    def names(values: tuple[SenderPipelineName, ...]) -> str:
        pipelines = [pipeline.value for pipeline in _pipeline_set(values)]
        return ", ".join(sorted(pipelines)) if pipelines else "нет"

    mode = _coerce_mode(config.mode)
    mode_text = mode.name if mode else "INVALID"
    return "\n".join(
        (
            "🚦 Rollout новых pipeline",
            "",
            f"Режим: {mode_text}",
            f"Активные pipeline: {names(config.enabled_pipelines)}",
            f"Shadow pipeline: {names(config.shadow_pipelines)}",
            f"Dry-run pipeline: {names(config.dry_run_pipelines)}",
            f"Allowlist правил: {'включён' if config.require_rule_allowlist else 'выключен'}",
            f"Процент rollout: {config.rollout_percent}%",
            f"Fail-closed: {'да' if config.fail_closed else 'нет'}",
        )
    )
