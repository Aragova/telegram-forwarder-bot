from __future__ import annotations

from app.sender_pipeline_rollout import SenderPipelineName, SenderRolloutAction, SenderRolloutMode
from app.sender_pipeline_rollout_runtime import (
    build_repost_single_active_canary_config_from_env,
    build_sender_pipeline_rollout_strategy_from_env,
    repost_single_active_canary_enabled_from_env,
)


def _decision(rule_id=12):
    return build_sender_pipeline_rollout_strategy_from_env().decide(
        pipeline_name=SenderPipelineName.REPOST_SINGLE,
        rule_id=rule_id,
        source_id=-100,
        target_id=-200,
    )


def test_default_env_active_canary_disabled(monkeypatch):
    monkeypatch.delenv("SENDER_PIPELINE_REPOST_SINGLE_ACTIVE_CANARY_ENABLED", raising=False)
    monkeypatch.delenv("SENDER_PIPELINE_ROLLOUT_MODE", raising=False)
    monkeypatch.delenv("SENDER_PIPELINE_ROLLOUT_REPOST_SINGLE_RULE_IDS", raising=False)

    config = build_repost_single_active_canary_config_from_env()

    assert repost_single_active_canary_enabled_from_env() is False
    assert config.canary_enabled is False
    assert config.enabled_rule_ids == ()


def test_active_mode_without_canary_env_does_not_arm_canary(monkeypatch):
    monkeypatch.setenv("SENDER_PIPELINE_ROLLOUT_MODE", "active")
    monkeypatch.setenv("SENDER_PIPELINE_ROLLOUT_REPOST_SINGLE_RULE_IDS", "12")
    monkeypatch.delenv("SENDER_PIPELINE_REPOST_SINGLE_ACTIVE_CANARY_ENABLED", raising=False)

    config = build_repost_single_active_canary_config_from_env()

    assert _decision().action is SenderRolloutAction.USE_PIPELINE
    assert config.canary_enabled is False
    assert config.exactly_one_enabled_rule_id is True


def test_active_mode_with_empty_rule_ids_is_not_exactly_one(monkeypatch):
    monkeypatch.setenv("SENDER_PIPELINE_ROLLOUT_MODE", "active")
    monkeypatch.setenv("SENDER_PIPELINE_ROLLOUT_REPOST_SINGLE_RULE_IDS", "")
    monkeypatch.setenv("SENDER_PIPELINE_REPOST_SINGLE_ACTIVE_CANARY_ENABLED", "1")

    config = build_repost_single_active_canary_config_from_env()

    assert _decision().action is SenderRolloutAction.USE_LEGACY
    assert config.canary_enabled is True
    assert config.exactly_one_enabled_rule_id is False


def test_active_mode_with_multiple_rule_ids_is_not_exactly_one(monkeypatch):
    monkeypatch.setenv("SENDER_PIPELINE_ROLLOUT_MODE", "active")
    monkeypatch.setenv("SENDER_PIPELINE_ROLLOUT_REPOST_SINGLE_RULE_IDS", "12,13")
    monkeypatch.setenv("SENDER_PIPELINE_REPOST_SINGLE_ACTIVE_CANARY_ENABLED", "yes")

    config = build_repost_single_active_canary_config_from_env()

    assert config.canary_enabled is True
    assert config.enabled_rule_ids == (12, 13)
    assert config.exactly_one_enabled_rule_id is False


def test_active_mode_with_exactly_one_rule_id_and_canary_enabled_is_eligible(monkeypatch):
    monkeypatch.setenv("SENDER_PIPELINE_ROLLOUT_MODE", "active")
    monkeypatch.setenv("SENDER_PIPELINE_ROLLOUT_REPOST_SINGLE_RULE_IDS", "12")
    monkeypatch.setenv("SENDER_PIPELINE_REPOST_SINGLE_ACTIVE_CANARY_ENABLED", "on")

    config = build_repost_single_active_canary_config_from_env()
    decision = _decision()

    assert config.canary_enabled is True
    assert config.enabled_rule_ids == (12,)
    assert config.exactly_one_enabled_rule_id is True
    assert decision.mode is SenderRolloutMode.ACTIVE
    assert decision.action is SenderRolloutAction.USE_PIPELINE


def test_dry_run_and_shadow_never_eligible_for_active_run(monkeypatch):
    monkeypatch.setenv("SENDER_PIPELINE_ROLLOUT_REPOST_SINGLE_RULE_IDS", "12")
    monkeypatch.setenv("SENDER_PIPELINE_REPOST_SINGLE_ACTIVE_CANARY_ENABLED", "1")

    for mode, expected_action in (("dry_run", SenderRolloutAction.DRY_RUN_PIPELINE), ("shadow", SenderRolloutAction.SHADOW_PIPELINE)):
        monkeypatch.setenv("SENDER_PIPELINE_ROLLOUT_MODE", mode)
        decision = _decision()
        assert decision.mode is not SenderRolloutMode.ACTIVE
        assert decision.action is expected_action
