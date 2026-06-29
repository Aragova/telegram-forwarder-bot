from __future__ import annotations

import inspect

from app.repost_single_rollout_probe import RepostSingleProbeStatus, RepostSingleRolloutProbe
from app.sender_pipeline_rollout import SenderPipelineName, SenderPipelineRolloutConfig, SenderPipelineRolloutStrategy, SenderRolloutMode


def _strategy(mode, **kwargs):
    return SenderPipelineRolloutStrategy(config=SenderPipelineRolloutConfig(mode=mode, **kwargs))


def test_disabled_decision_returns_disabled() -> None:
    result = RepostSingleRolloutProbe().probe(rule_id=1, source_id="s", target_id="t", source_message_id=10)
    assert result.status is RepostSingleProbeStatus.DISABLED
    assert not result.ok


def test_dry_run_allowed_ready() -> None:
    probe = RepostSingleRolloutProbe(rollout_strategy=_strategy(SenderRolloutMode.DRY_RUN, dry_run_pipelines=(SenderPipelineName.REPOST_SINGLE,), dry_run_rule_ids=(12,)))
    result = probe.probe(rule_id=12, source_id="s", target_id="t", source_message_id=10)
    assert result.status is RepostSingleProbeStatus.READY
    assert result.ok


def test_shadow_allowed_ready() -> None:
    probe = RepostSingleRolloutProbe(rollout_strategy=_strategy(SenderRolloutMode.SHADOW, shadow_pipelines=(SenderPipelineName.REPOST_SINGLE,), shadow_rule_ids=(12,)))
    result = probe.probe(rule_id=12, source_id="s", target_id="t", source_message_id=10)
    assert result.status is RepostSingleProbeStatus.READY


def test_active_allowed_is_not_enabled_in_stage_27() -> None:
    probe = RepostSingleRolloutProbe(rollout_strategy=_strategy(SenderRolloutMode.ACTIVE, enabled_pipelines=(SenderPipelineName.REPOST_SINGLE,), enabled_rule_ids=(12,)))
    result = probe.probe(rule_id=12, source_id="s", target_id="t", source_message_id=10)
    assert result.status is RepostSingleProbeStatus.ACTIVE_NOT_ENABLED
    assert result.reason == "active_not_enabled_in_stage_27"


def test_missing_required_fields_are_not_ready() -> None:
    probe = RepostSingleRolloutProbe(rollout_strategy=_strategy(SenderRolloutMode.DRY_RUN, dry_run_pipelines=(SenderPipelineName.REPOST_SINGLE,), dry_run_rule_ids=(12,)))
    assert probe.probe(rule_id=None, source_id="s", target_id="t", source_message_id=10).reason == "missing_rule_id"
    assert probe.probe(rule_id=12, source_id=None, target_id="t", source_message_id=10).reason == "missing_source_id"
    assert probe.probe(rule_id=12, source_id="s", target_id=None, source_message_id=10).reason == "missing_target_id"
    assert probe.probe(rule_id=12, source_id="s", target_id="t", source_message_id=None).reason == "missing_source_message_id"


def test_probe_has_no_pipeline_or_gateway_side_effect_imports() -> None:
    source = inspect.getsource(__import__("app.repost_single_rollout_probe", fromlist=["x"]))
    assert "RepostSinglePipeline" not in source
    assert "TelegramSendGateway" not in source
    assert ".run(" not in source
    assert "copy_message" not in source


def test_probe_log_context_is_safe() -> None:
    result = RepostSingleRolloutProbe().probe(rule_id=1, source_id="s", target_id="t", source_message_id=10)
    text = str(result.to_log_context()) + result.log_label()
    for forbidden in ("caption", "content_json", "SECRET_TOKEN", "session", "telegram_object"):
        assert forbidden not in text
