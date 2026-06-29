from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def test_sender_uses_probe_then_active_canary_but_not_pipeline_directly() -> None:
    source = _read("app/sender.py")
    assert "self.repost_single_rollout_probe.probe(" in source
    assert "self.repost_single_active_canary_runner.try_run(" in source
    assert "RepostSinglePipeline" not in source
    assert "self.pipeline.run(" not in source
    assert ".try_handle_repost_single(" not in source


def test_probe_does_not_import_sender_pipeline_or_gateway() -> None:
    source = _read("app/repost_single_rollout_probe.py")
    assert "TelegramSendGateway" not in source
    assert "RepostSinglePipeline" not in source
    assert "copy_message" not in source
    assert ".run(" not in source


def test_active_stage_27_fail_closed_reason_is_preserved_for_probe() -> None:
    source = _read("app/repost_single_rollout_probe.py")
    assert "active_not_enabled_in_stage_27" in source


def test_dry_run_shadow_canary_do_not_call_pipeline_run() -> None:
    source = _read("app/repost_single_active_canary.py")
    preflight_call_pos = source.index("result = self._preflight(")
    run_pos = source.index("self.pipeline.run(")
    assert preflight_call_pos < run_pos
    assert "decision.action is not SenderRolloutAction.USE_PIPELINE" in source
    assert "decision.mode is not SenderRolloutMode.ACTIVE" in source


def test_legacy_copy_remains_fallback_only_when_pipeline_not_attempted() -> None:
    source = _read("app/sender.py")
    probe_pos = source.index("self.repost_single_rollout_probe.probe(")
    canary_pos = source.index("self.repost_single_active_canary_runner.try_run(", probe_pos)
    stop_pos = source.index("if active_canary_result.attempted_pipeline:", canary_pos)
    copy_pos = source.index("copy_result = await self._copy_single_via_bot", stop_pos)
    assert probe_pos < canary_pos < stop_pos < copy_pos
