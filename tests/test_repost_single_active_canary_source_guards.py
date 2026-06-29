from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def test_pipeline_run_called_only_from_experimental_active_canary_module():
    offenders = []
    for path in (ROOT / "app").glob("*.py"):
        source = path.read_text(encoding="utf-8")
        if "RepostSinglePipeline" in source and ".run(" in source and path.name not in {"repost_single_active_canary.py", "repost_single_pipeline.py"}:
            offenders.append(path.name)
    assert offenders == []
    assert "self.pipeline.run(" in read("app/repost_single_active_canary.py")


def test_sender_has_no_active_runtime_branch_and_keeps_legacy_copy_once():
    source = read("app/sender.py")
    forbidden = (
        "self.repost_single_active_canary_runner.try_run(",
        "active_pipeline_failed_no_fallback",
        "repost_single_active_pipeline_failed",
        "repost_single_active_pipeline_uncertain",
        "reaction_after_active_pipeline",
        "active_pipeline_uncertain_no_sent_message_ids",
        "repost_single_active_pipeline",
    )
    for token in forbidden:
        assert token not in source
    assert "RepostSinglePipeline" not in source
    assert "self.pipeline.run(" not in source
    assert source.count("copy_result = await self._copy_single_via_bot") == 1
    assert "verify_after_copy_single" in source
    assert "self._add_reaction_for_rule_if_possible" in source
    assert "self._mark_delivery_sent_sync" in source


def test_worker_and_video_do_not_import_repost_single_pipeline():
    assert "RepostSinglePipeline" not in read("app/worker_runtime.py")
    assert "RepostSinglePipeline" not in read("app/video_processor.py")


def test_album_video_campaign_reaction_modules_do_not_import_active_canary():
    for module in (
        "app/repost_album_pipeline.py",
        "app/video_send_pipeline.py",
        "app/legacy_video_delivery_pipeline.py",
        "app/repost_campaign_pipeline.py",
        "app/reaction_post_send_service.py",
    ):
        assert "repost_single_active_canary" not in read(module)
        assert "RepostSingleActiveCanary" not in read(module)


def test_bot_does_not_wire_active_canary_pipeline():
    source = read("bot.py")
    for token in (
        "RepostSingleActiveCanaryRunner",
        "RepostSinglePipeline(",
        "post_send_steps=PostSendSteps(",
        "attempt_ledger=AttemptLedgerService(repository=db)",
        "target_verifier=TargetVerifier(telethon_client=sender_telethon_client)",
        "finalizer=DeliveryFinalizer()",
        "repost_single_active_canary_runner=",
    ):
        assert token not in source


def test_active_canary_runner_remains_reaction_agnostic_and_test_only():
    source = read("app/repost_single_active_canary.py")
    forbidden = (
        "ReactionRuntimeResolver",
        "_add_reaction_for_rule_if_possible",
        "reaction_clients",
        "enqueue_reaction_job",
    )
    for token in forbidden:
        assert token not in source
