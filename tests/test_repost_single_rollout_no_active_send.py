from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def test_sender_does_not_probe_or_call_active_canary_runtime() -> None:
    source = _read("app/sender.py")
    assert "self.repost_single_rollout_probe.probe(" not in source
    assert "self.repost_single_active_canary_runner.try_run(" not in source
    assert "active_pipeline_failed_no_fallback" not in source
    assert "repost_single_active_pipeline_failed" not in source
    assert "repost_single_active_pipeline_uncertain" not in source
    assert "reaction_after_active_pipeline" not in source
    assert "RepostSinglePipeline" not in source
    assert "self.pipeline.run(" not in source
    assert ".try_handle_repost_single(" not in source


def test_bot_does_not_wire_active_repost_single_runtime() -> None:
    source = _read("bot.py")
    assert "RepostSingleActiveCanaryRunner" not in source
    assert "RepostSinglePipeline" not in source
    assert "TelegramSendGateway" not in source
    assert "TargetVerifier" not in source
    assert "PostSendSteps" not in source
    assert "DeliveryFinalizer" not in source
    assert "build_repost_single_active_canary_config_from_env" not in source
    assert "build_sender_pipeline_rollout_strategy_from_env" not in source
    assert "RepostSingleRolloutProbe" not in source
    assert "repost_single_active_canary_runner=" not in source


def test_legacy_copy_path_remains_the_single_runtime_path() -> None:
    source = _read("app/sender.py")
    copy_pos = source.index("copy_result = await self._copy_single_via_bot")
    verify_pos = source.index("verify_after_copy_single", copy_pos)
    reaction_pos = source.index("self._add_reaction_for_rule_if_possible", verify_pos)
    mark_sent_pos = source.index("self._mark_delivery_sent_sync", reaction_pos)
    touch_pos = source.index("self._touch_rule_after_send_sync", mark_sent_pos)
    assert copy_pos < verify_pos < reaction_pos < mark_sent_pos < touch_pos
