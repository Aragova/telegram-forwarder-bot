from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def test_pipeline_run_called_only_from_active_canary_module():
    offenders = []
    for path in (ROOT / "app").glob("*.py"):
        source = path.read_text(encoding="utf-8")
        if "RepostSinglePipeline" in source and ".run(" in source and path.name != "repost_single_active_canary.py" and path.name != "repost_single_pipeline.py":
            offenders.append(path.name)
    assert offenders == []
    assert "self.pipeline.run(" in read("app/repost_single_active_canary.py")


def test_sender_has_no_direct_repost_single_pipeline_run_or_extra_active_copy_message():
    source = read("app/sender.py")
    assert "RepostSinglePipeline" not in source
    assert "self.pipeline.run(" not in source
    assert "active_canary_result.attempted_pipeline" in source
    assert source.count("copy_result = await self._copy_single_via_bot") == 1


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


def test_bot_wires_post_send_steps_and_finalizer_for_active_canary_pipeline():
    source = read("bot.py")
    assert "RepostSinglePipeline(" in source
    assert "post_send_steps=PostSendSteps(" in source
    assert "attempt_ledger=AttemptLedgerService(repository=db)" in source
    assert "target_verifier=TargetVerifier(telethon_client=sender_telethon_client)" in source
    assert "finalizer=DeliveryFinalizer()" in source


def test_sender_active_canary_does_not_treat_reactions_as_unsupported():
    source = read("app/sender.py")
    assert 'unsupported_features=("reactions",)' not in source
    assert 'active_canary_unsupported_features = ("reactions",)' not in source
    assert 'reason="unsupported_feature:reactions"' not in source
    active_block = source[source.index("if self.repost_single_active_canary_runner is not None"):source.index("# =========================================================", source.index("if self.repost_single_active_canary_runner is not None"))]
    assert "unsupported_feature:reactions" not in active_block
    assert "reaction_after_active_pipeline" in active_block
    assert "self._add_reaction_for_rule_if_possible" in active_block
    assert "if active_canary_result.attempted_pipeline" in active_block


def test_active_canary_runner_remains_reaction_agnostic():
    source = read("app/repost_single_active_canary.py")
    forbidden = (
        "ReactionRuntimeResolver",
        "_add_reaction_for_rule_if_possible",
        "reaction_clients",
        "enqueue_reaction_job",
    )
    for token in forbidden:
        assert token not in source
