from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def test_sender_contains_probe_call_but_no_new_active_pipeline_send() -> None:
    source = _read("app/sender.py")
    assert "self.repost_single_rollout_probe.probe(" in source
    assert "RepostSinglePipeline" not in source
    assert ".try_handle_repost_single(" not in source
    assert "probe_result.ok" not in source
    assert "if probe_result" not in source


def test_probe_does_not_import_sender_pipeline_or_gateway() -> None:
    source = _read("app/repost_single_rollout_probe.py")
    assert "TelegramSendGateway" not in source
    assert "RepostSinglePipeline" not in source
    assert "copy_message" not in source
    assert ".run(" not in source


def test_active_stage_27_fail_closed_reason_is_present() -> None:
    source = _read("app/repost_single_rollout_probe.py")
    assert "active_not_enabled_in_stage_27" in source


def test_legacy_copy_remains_after_probe_logging() -> None:
    source = _read("app/sender.py")
    probe_pos = source.index("self.repost_single_rollout_probe.probe(")
    copy_pos = source.index("copy_result = await self._copy_single_via_bot", probe_pos)
    assert probe_pos < copy_pos
