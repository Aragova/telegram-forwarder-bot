from pathlib import Path

from app.sender_pipeline_rollout import (
    SenderPipelineName,
    SenderPipelineRolloutConfig,
    SenderPipelineRolloutStrategy,
    SenderRolloutAction,
    SenderRolloutMode,
    is_rule_selected_by_percent,
)


def _flags_as_tuple(flags):
    return (
        flags.enable_repost_single_pipeline,
        flags.enable_repost_album_pipeline,
        flags.enable_video_send_pipeline,
        flags.enable_legacy_video_delivery_pipeline,
        flags.enable_repost_campaign_pipeline,
        flags.enable_reaction_post_send_service,
    )


def test_defaults_are_fail_closed():
    config = SenderPipelineRolloutConfig()
    strategy = SenderPipelineRolloutStrategy(config=config)

    assert config.mode is SenderRolloutMode.DISABLED
    assert config.rollout_percent == 0
    assert config.require_rule_allowlist is True
    assert config.allow_active_without_rule_id is False
    assert config.fail_closed is True
    assert _flags_as_tuple(strategy.to_feature_flags()) == (False, False, False, False, False, False)

    decision = strategy.decide(pipeline_name=SenderPipelineName.REPOST_SINGLE, rule_id=1)
    assert decision.action is SenderRolloutAction.USE_LEGACY
    assert decision.reason == "rollout_disabled"
    assert decision.should_call_pipeline is False
    assert decision.should_use_pipeline_result is False
    assert decision.should_continue_legacy is True


def test_unknown_pipeline_is_safe():
    decision = SenderPipelineRolloutStrategy().decide(pipeline_name="unknown")

    assert decision.action is SenderRolloutAction.USE_LEGACY
    assert decision.reason in {"unknown_pipeline", "rollout_config_invalid"}


def test_blocked_rule_wins():
    strategy = SenderPipelineRolloutStrategy(
        config=SenderPipelineRolloutConfig(
            mode=SenderRolloutMode.ACTIVE,
            enabled_pipelines=(SenderPipelineName.REPOST_SINGLE,),
            enabled_rule_ids=(123,),
            blocked_rule_ids=(123,),
        )
    )

    decision = strategy.decide(pipeline_name=SenderPipelineName.REPOST_SINGLE, rule_id=123)

    assert decision.action is SenderRolloutAction.USE_LEGACY
    assert decision.reason == "rule_blocked"


def test_active_requires_pipeline_allowlist():
    strategy = SenderPipelineRolloutStrategy(
        config=SenderPipelineRolloutConfig(mode=SenderRolloutMode.ACTIVE, enabled_rule_ids=(123,))
    )

    decision = strategy.decide(pipeline_name=SenderPipelineName.REPOST_SINGLE, rule_id=123)

    assert decision.action is SenderRolloutAction.USE_LEGACY
    assert decision.reason == "pipeline_not_allowed"


def test_active_requires_rule_allowlist_by_default():
    strategy = SenderPipelineRolloutStrategy(
        config=SenderPipelineRolloutConfig(
            mode=SenderRolloutMode.ACTIVE,
            enabled_pipelines=(SenderPipelineName.REPOST_SINGLE,),
            enabled_rule_ids=(456,),
        )
    )

    decision = strategy.decide(pipeline_name=SenderPipelineName.REPOST_SINGLE, rule_id=123)

    assert decision.action is SenderRolloutAction.USE_LEGACY
    assert decision.reason == "rule_not_allowed"


def test_active_allowed_rule_uses_pipeline():
    strategy = SenderPipelineRolloutStrategy(
        config=SenderPipelineRolloutConfig(
            mode=SenderRolloutMode.ACTIVE,
            enabled_pipelines=(SenderPipelineName.REPOST_SINGLE,),
            enabled_rule_ids=(123,),
        )
    )

    decision = strategy.decide(pipeline_name=SenderPipelineName.REPOST_SINGLE, rule_id=123)

    assert decision.action is SenderRolloutAction.USE_PIPELINE
    assert decision.should_call_pipeline is True
    assert decision.should_use_pipeline_result is True
    assert decision.should_continue_legacy is False


def test_shadow_mode_semantics():
    strategy = SenderPipelineRolloutStrategy(
        config=SenderPipelineRolloutConfig(
            mode=SenderRolloutMode.SHADOW,
            shadow_pipelines=(SenderPipelineName.REPOST_ALBUM,),
            shadow_rule_ids=(123,),
        )
    )

    decision = strategy.decide(pipeline_name=SenderPipelineName.REPOST_ALBUM, rule_id=123)

    assert decision.action is SenderRolloutAction.SHADOW_PIPELINE
    assert decision.should_call_pipeline is True
    assert decision.should_use_pipeline_result is False
    assert decision.should_continue_legacy is True
    assert decision.should_record_shadow_result is True


def test_dry_run_mode_semantics():
    strategy = SenderPipelineRolloutStrategy(
        config=SenderPipelineRolloutConfig(
            mode=SenderRolloutMode.DRY_RUN,
            dry_run_pipelines=(SenderPipelineName.VIDEO_SEND,),
            dry_run_rule_ids=(123,),
        )
    )

    decision = strategy.decide(pipeline_name=SenderPipelineName.VIDEO_SEND, rule_id=123)

    assert decision.action is SenderRolloutAction.DRY_RUN_PIPELINE
    assert decision.should_call_pipeline is False
    assert decision.should_use_pipeline_result is False
    assert decision.should_continue_legacy is True
    assert decision.should_record_shadow_result is False


def test_percentage_rollout_deterministic():
    assert is_rule_selected_by_percent(rule_id=123, pipeline_name=SenderPipelineName.REPOST_SINGLE, rollout_percent=0) is False
    assert is_rule_selected_by_percent(rule_id=123, pipeline_name=SenderPipelineName.REPOST_SINGLE, rollout_percent=100) is True
    first = is_rule_selected_by_percent(rule_id=123, pipeline_name=SenderPipelineName.REPOST_SINGLE, rollout_percent=50)
    second = is_rule_selected_by_percent(rule_id=123, pipeline_name=SenderPipelineName.REPOST_SINGLE, rollout_percent=50)
    assert first is second
    assert is_rule_selected_by_percent(rule_id=None, pipeline_name=SenderPipelineName.REPOST_SINGLE, rollout_percent=100) is False


def test_percentage_rollout_can_select_active_only_if_allowlist_disabled():
    strategy = SenderPipelineRolloutStrategy(
        config=SenderPipelineRolloutConfig(
            mode=SenderRolloutMode.ACTIVE,
            enabled_pipelines=(SenderPipelineName.REPOST_SINGLE,),
            require_rule_allowlist=False,
            rollout_percent=100,
        )
    )

    decision = strategy.decide(pipeline_name=SenderPipelineName.REPOST_SINGLE, rule_id=123)

    assert decision.action is SenderRolloutAction.USE_PIPELINE


def test_to_feature_flags_disabled_dry_run_active_and_shadow():
    disabled = SenderPipelineRolloutStrategy(config=SenderPipelineRolloutConfig()).to_feature_flags()
    dry_run = SenderPipelineRolloutStrategy(
        config=SenderPipelineRolloutConfig(
            mode=SenderRolloutMode.DRY_RUN,
            dry_run_pipelines=(SenderPipelineName.REPOST_SINGLE,),
        )
    ).to_feature_flags()
    active_strategy = SenderPipelineRolloutStrategy(
        config=SenderPipelineRolloutConfig(
            mode=SenderRolloutMode.ACTIVE,
            enabled_pipelines=(SenderPipelineName.REPOST_SINGLE, SenderPipelineName.REPOST_ALBUM),
            enabled_rule_ids=(123,),
        )
    )
    shadow_strategy = SenderPipelineRolloutStrategy(
        config=SenderPipelineRolloutConfig(
            mode=SenderRolloutMode.SHADOW,
            shadow_pipelines=(SenderPipelineName.REPOST_SINGLE,),
            shadow_rule_ids=(123,),
        )
    )

    assert _flags_as_tuple(disabled) == (False, False, False, False, False, False)
    assert _flags_as_tuple(dry_run) == (False, False, False, False, False, False)
    assert _flags_as_tuple(active_strategy.to_feature_flags()) == (True, True, False, False, False, False)
    shadow_flags = shadow_strategy.to_feature_flags()
    shadow_decision = shadow_strategy.decide(pipeline_name=SenderPipelineName.REPOST_SINGLE, rule_id=123)
    assert shadow_flags.enable_repost_single_pipeline is True
    assert shadow_decision.should_use_pipeline_result is False


def test_decision_safe_logs():
    decision = SenderPipelineRolloutStrategy().decide(
        pipeline_name=SenderPipelineName.REPOST_SINGLE,
        rule_id="123:SECRET_TOKEN",
        source_id="PRIVATE_SOURCE",
        target_id="PRIVATE_TARGET",
    )

    context = decision.to_log_context()
    label = decision.log_label()

    assert set(context) == {
        "pipeline_name",
        "mode",
        "action",
        "rule_id",
        "source_id",
        "target_id",
        "should_call_pipeline",
        "should_use_pipeline_result",
        "should_continue_legacy",
        "should_record_shadow_result",
        "reason",
    }
    assert "content_json" not in context
    assert "caption" not in context
    assert "payload" not in context
    assert "content_json" not in label
    assert "caption" not in label
    assert "payload" not in label


def test_admin_text():
    text = SenderPipelineRolloutStrategy().to_admin_text()

    assert "Rollout новых pipeline" in text
    assert "Режим: DISABLED" in text
    assert "Процент rollout: 0%" in text
    assert "payload" not in text.lower()
    assert "token" not in text.lower()


def test_module_import_boundaries():
    source = Path("app/sender_pipeline_rollout.py").read_text()

    for forbidden in (
        "aiogram",
        "telethon",
        "PostgresRepository",
        "app.sender",
        "worker_runtime",
        "video_processor",
        "TelegramSendGateway",
    ):
        assert forbidden not in source


def test_no_runtime_wiring():
    forbidden = ("SenderPipelineRolloutStrategy", "SenderPipelineRolloutConfig", "SenderRolloutMode")
    for filename in ("app/sender.py", "bot.py", "app/worker_runtime.py"):
        source = Path(filename).read_text()
        for marker in forbidden:
            assert marker not in source
