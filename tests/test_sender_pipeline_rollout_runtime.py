from __future__ import annotations

from app.sender_pipeline_rollout import SenderPipelineName, SenderRolloutAction, SenderRolloutMode
from app.sender_pipeline_rollout_runtime import build_sender_pipeline_rollout_strategy_from_env


def _clear(monkeypatch):
    for key in (
        "SENDER_PIPELINE_ROLLOUT_MODE",
        "SENDER_PIPELINE_ROLLOUT_REPOST_SINGLE_RULE_IDS",
        "SENDER_PIPELINE_ROLLOUT_BLOCKED_RULE_IDS",
    ):
        monkeypatch.delenv(key, raising=False)


def test_default_env_builds_disabled_fail_closed_strategy(monkeypatch) -> None:
    _clear(monkeypatch)
    strategy = build_sender_pipeline_rollout_strategy_from_env()
    decision = strategy.decide(pipeline_name=SenderPipelineName.REPOST_SINGLE, rule_id=12)
    assert strategy.config.mode is SenderRolloutMode.DISABLED
    assert strategy.config.rollout_percent == 0
    assert strategy.config.require_rule_allowlist is True
    assert strategy.config.fail_closed is True
    assert decision.action is SenderRolloutAction.USE_LEGACY


def test_dry_run_env_enables_repost_single_allowlist(monkeypatch) -> None:
    _clear(monkeypatch)
    monkeypatch.setenv("SENDER_PIPELINE_ROLLOUT_MODE", "dry_run")
    monkeypatch.setenv("SENDER_PIPELINE_ROLLOUT_REPOST_SINGLE_RULE_IDS", "12,15")
    strategy = build_sender_pipeline_rollout_strategy_from_env()
    assert strategy.config.mode is SenderRolloutMode.DRY_RUN
    assert strategy.config.dry_run_pipelines == (SenderPipelineName.REPOST_SINGLE,)
    assert strategy.config.dry_run_rule_ids == (12, 15)
    assert strategy.decide(pipeline_name=SenderPipelineName.REPOST_SINGLE, rule_id=12).action is SenderRolloutAction.DRY_RUN_PIPELINE


def test_shadow_env_enables_repost_single_allowlist(monkeypatch) -> None:
    _clear(monkeypatch)
    monkeypatch.setenv("SENDER_PIPELINE_ROLLOUT_MODE", "shadow")
    monkeypatch.setenv("SENDER_PIPELINE_ROLLOUT_REPOST_SINGLE_RULE_IDS", "12")
    strategy = build_sender_pipeline_rollout_strategy_from_env()
    assert strategy.config.mode is SenderRolloutMode.SHADOW
    assert strategy.config.shadow_pipelines == (SenderPipelineName.REPOST_SINGLE,)
    assert strategy.config.shadow_rule_ids == (12,)
    assert strategy.decide(pipeline_name=SenderPipelineName.REPOST_SINGLE, rule_id=12).action is SenderRolloutAction.SHADOW_PIPELINE


def test_active_env_builds_active_config_without_runtime_send(monkeypatch) -> None:
    _clear(monkeypatch)
    monkeypatch.setenv("SENDER_PIPELINE_ROLLOUT_MODE", "active")
    monkeypatch.setenv("SENDER_PIPELINE_ROLLOUT_REPOST_SINGLE_RULE_IDS", "12")
    strategy = build_sender_pipeline_rollout_strategy_from_env()
    assert strategy.config.mode is SenderRolloutMode.ACTIVE
    assert strategy.config.enabled_pipelines == (SenderPipelineName.REPOST_SINGLE,)
    assert strategy.config.enabled_rule_ids == (12,)
    assert strategy.decide(pipeline_name=SenderPipelineName.REPOST_SINGLE, rule_id=12).action is SenderRolloutAction.USE_PIPELINE


def test_blocked_and_invalid_ids_are_safe(monkeypatch) -> None:
    _clear(monkeypatch)
    monkeypatch.setenv("SENDER_PIPELINE_ROLLOUT_MODE", "dry_run")
    monkeypatch.setenv("SENDER_PIPELINE_ROLLOUT_REPOST_SINGLE_RULE_IDS", "12,bad,15")
    monkeypatch.setenv("SENDER_PIPELINE_ROLLOUT_BLOCKED_RULE_IDS", "15,oops")
    strategy = build_sender_pipeline_rollout_strategy_from_env()
    assert strategy.config.dry_run_rule_ids == (12, 15)
    assert strategy.config.blocked_rule_ids == (15,)
    assert strategy.decide(pipeline_name=SenderPipelineName.REPOST_SINGLE, rule_id=15).action is SenderRolloutAction.USE_LEGACY


def test_unknown_mode_falls_back_disabled(monkeypatch) -> None:
    _clear(monkeypatch)
    monkeypatch.setenv("SENDER_PIPELINE_ROLLOUT_MODE", "surprise")
    strategy = build_sender_pipeline_rollout_strategy_from_env()
    assert strategy.config.mode is SenderRolloutMode.DISABLED
    assert strategy.config.fail_closed is True
