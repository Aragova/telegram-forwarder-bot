from __future__ import annotations

import asyncio
import inspect
from pathlib import Path

from app.delivery_finalizer import DeliveryFinalizer, DeliveryFinalizationStatus, DeliveryOutcome
from app.delivery_observability import (
    DeliveryDiagnosticSignal,
    DeliveryDiagnosticsSnapshot,
    DeliveryHealthStatus,
    DeliveryObservabilityService,
)
from app.delivery_pipeline_result import DeliveryPipelineResult
from app.reaction_post_send_service import ReactionPostSendResult
from app.repository_contracts import known_repository_responsibility_areas
from app.sender_legacy_inventory import build_legacy_cleanup_readiness
from app.sender_pipeline_facade import SenderPipelineFacade, SenderPipelineFacadeResult, SenderPipelineFeatureFlags
from app.telegram_send_gateway import TelegramSendGateway
from app.transport_operation import TransportOperationKind, classify_transport_operation
from app.transport_policy import build_sender_bot_policy, build_sender_telethon_policy

ROOT = Path(__file__).resolve().parents[1]


def _read_repo_text(relative_path: str) -> str:
    return (ROOT / relative_path).read_text(encoding="utf-8")


FEATURE_FLAG_NAMES = (
    "enable_repost_single_pipeline",
    "enable_repost_album_pipeline",
    "enable_video_send_pipeline",
    "enable_legacy_video_delivery_pipeline",
    "enable_repost_campaign_pipeline",
    "enable_reaction_post_send_service",
)


class ExplodingPipeline:
    def __init__(self) -> None:
        self.called = False

    async def run(self, input_data: object | None = None) -> object:
        self.called = True
        raise AssertionError("disabled pipeline must not be called")


def test_sender_pipeline_feature_flags_are_disabled_by_default() -> None:
    flags = SenderPipelineFeatureFlags()

    assert {name: getattr(flags, name) for name in FEATURE_FLAG_NAMES} == {name: False for name in FEATURE_FLAG_NAMES}


def test_sender_pipeline_facade_disabled_flags_do_not_call_dependencies() -> None:
    fake_dependencies = {name: ExplodingPipeline() for name in (
        "repost_single_pipeline",
        "repost_album_pipeline",
        "video_send_pipeline",
        "legacy_video_delivery_pipeline",
        "repost_campaign_pipeline",
        "reaction_post_send_service",
    )}
    facade = SenderPipelineFacade(flags=SenderPipelineFeatureFlags(), **fake_dependencies)

    async def run_disabled_matrix() -> list[SenderPipelineFacadeResult]:
        return [
            await facade.try_handle_repost_single({"case": "single"}),
            await facade.try_handle_repost_album({"case": "album"}),
            await facade.try_handle_video_send({"case": "video"}),
            await facade.try_handle_legacy_video_delivery({"case": "legacy"}),
            await facade.try_handle_repost_campaign({"case": "campaign"}),
            await facade.try_handle_reactions({"case": "reactions"}),
        ]

    results = asyncio.run(run_disabled_matrix())

    assert all(result.handled is False for result in results)
    assert all(result.reason == "pipeline_disabled" for result in results)
    assert all(dependency.called is False for dependency in fake_dependencies.values())


def test_sender_service_delivery_methods_do_not_call_pipeline_facade_yet() -> None:
    sender_source = _read_repo_text("app/sender.py")

    assert "sender_pipeline_facade" in sender_source
    assert "self.sender_pipeline_facade = sender_pipeline_facade" in sender_source
    for forbidden_token in (
        "self.sender_pipeline_facade.try_handle_",
        "sender_pipeline_facade.try_handle_",
        ".try_handle_repost_single(",
        ".try_handle_repost_album(",
        ".try_handle_video_send(",
        ".try_handle_legacy_video_delivery(",
        ".try_handle_repost_campaign(",
        ".try_handle_reactions(",
    ):
        assert forbidden_token not in sender_source


def test_pipeline_facade_not_wired_in_bot_or_worker_runtime() -> None:
    combined_runtime_source = "\n".join([_read_repo_text("bot.py"), _read_repo_text("app/worker_runtime.py")])

    for forbidden_token in ("SenderPipelineFacade", "SenderPipelineFeatureFlags", *FEATURE_FLAG_NAMES):
        assert forbidden_token not in combined_runtime_source


def test_foundation_modules_do_not_import_runtime_heavy_dependencies() -> None:
    foundation_modules = (
        "app/telegram_send_gateway.py",
        "app/target_verifier.py",
        "app/attempt_ledger_service.py",
        "app/post_send_steps.py",
        "app/delivery_finalizer.py",
        "app/repost_single_pipeline.py",
        "app/repost_album_pipeline.py",
        "app/video_send_pipeline.py",
        "app/legacy_video_delivery_pipeline.py",
        "app/repost_campaign_pipeline.py",
        "app/reaction_post_send_service.py",
        "app/sender_pipeline_facade.py",
        "app/repository_contracts.py",
        "app/delivery_observability.py",
        "app/delivery_observability_provider.py",
        "app/delivery_diagnostics_admin.py",
        "app/sender_legacy_inventory.py",
        "app/sender_pipeline_rollout_runtime.py",
        "app/repost_single_rollout_probe.py",
        "app/repost_single_active_canary.py",
    )
    forbidden_import_markers = (
        "from app.sender import",
        "import app.sender",
        "from app.worker_runtime import",
        "import app.worker_runtime",
        "from app.video_processor import",
        "import app.video_processor",
        "from aiogram",
        "import aiogram",
        "from telethon",
        "import telethon",
        "from app.postgres_repository import",
        "import app.postgres_repository",
        "PostgresRepository",
    )

    for module_path in foundation_modules:
        source = _read_repo_text(module_path)
        for marker in forbidden_import_markers:
            assert marker not in source, f"{module_path} must not contain {marker!r}"


def test_sender_legacy_inventory_remains_runtime_neutral() -> None:
    readiness = build_legacy_cleanup_readiness()

    assert readiness.total_entries > 0
    assert readiness.do_not_touch_count > 0


def test_telegram_send_gateway_public_methods_remain_narrow() -> None:
    public_methods = {name for name, member in inspect.getmembers(TelegramSendGateway, inspect.isfunction) if not name.startswith("_")}

    assert {
        "copy_message",
        "send_message",
        "send_video",
        "send_document",
        "send_media_group",
        "telethon_send_file",
    }.issubset(public_methods)
    assert public_methods.isdisjoint({
        "copy_messages",
        "delete_message",
        "delete_messages",
        "apply_reaction",
        "send_reaction",
        "cleanup_campaign_messages",
    })


def test_transport_operation_safety_matrix_remains_stable() -> None:
    assert classify_transport_operation("telethon", "get_messages") == TransportOperationKind.SAFE_READ
    assert classify_transport_operation("telethon", "download_media") == TransportOperationKind.DOWNLOAD
    assert classify_transport_operation("bot", "copy_message") == TransportOperationKind.NON_IDEMPOTENT_WRITE
    assert classify_transport_operation("bot", "send_message") == TransportOperationKind.NON_IDEMPOTENT_WRITE
    assert classify_transport_operation("bot", "unknown_sender_architecture_stage_23") == TransportOperationKind.UNKNOWN


def test_sender_transport_policies_do_not_retry_non_idempotent_writes() -> None:
    for policy in (build_sender_bot_policy(), build_sender_telethon_policy()):
        assert policy.retry_non_idempotent_writes is False
        assert policy.retry_unknown_operations is False


def test_sender_runtime_transport_wiring_remains_sender_only() -> None:
    bot_source = _read_repo_text("bot.py")

    assert "wrap_bot" in bot_source
    assert 'label="sender.bot"' in bot_source
    assert "wrap_telethon_client" in bot_source
    assert 'label="sender.telethon"' in bot_source
    for forbidden_token in ("reaction.bot", "reaction.telethon", "ReactionPostSendService"):
        assert forbidden_token not in bot_source


def test_delivery_finalizer_status_mapping_regression() -> None:
    finalizer = DeliveryFinalizer()

    matrix = (
        (DeliveryPipelineResult.sent(sent_message_ids=(1,)), DeliveryFinalizationStatus.FINALIZED, DeliveryOutcome.SENT),
        (DeliveryPipelineResult.failed(reason="boom"), DeliveryFinalizationStatus.FAILED, DeliveryOutcome.FAULTY),
        (DeliveryPipelineResult.rate_limited(retry_after=30), DeliveryFinalizationStatus.RATE_LIMITED, DeliveryOutcome.DEFERRED),
        (DeliveryPipelineResult.skipped(reason="skip"), DeliveryFinalizationStatus.SKIPPED, DeliveryOutcome.SKIPPED),
        (DeliveryPipelineResult.noop(reason="noop"), DeliveryFinalizationStatus.NOOP, DeliveryOutcome.NOOP),
    )

    for pipeline_result, expected_status, expected_outcome in matrix:
        result = finalizer.finalize_pipeline_result(pipeline_result=pipeline_result)
        assert result.status == expected_status
        assert result.outcome == expected_outcome


def test_pipelines_do_not_store_raw_objects_in_results_regression() -> None:
    sensitive_values = ("SECRET_TOKEN", "PRIVATE_CAPTION", "PRIVATE_VIDEO_PATH", "content_json")
    result_objects = (
        DeliveryPipelineResult.sent(reason="safe_reason", sent_message_ids=(10,)),
        DeliveryPipelineResult.failed(error_type="RuntimeError", error_text="sanitized failure", reason="safe_failure"),
        ReactionPostSendResult.failed(
            context=None,
            target_chat_id=100,
            target_message_ids=(1, 2),
            applied_message_ids=(),
            failed_message_ids=(2,),
            reaction_count=1,
            idempotency_key="reaction-key",
            error_type="RuntimeError",
            error_text="sanitized failure",
            reason="safe_reaction_failure",
        ),
        SenderPipelineFacadeResult.not_handled(
            "pipeline_failed",
            pipeline_name="repost_single_pipeline",
            error_type="RuntimeError",
            error_text="SECRET_TOKEN leaked",
        ),
        DeliveryDiagnosticsSnapshot(
            status=DeliveryHealthStatus.UNKNOWN,
            signals=(DeliveryDiagnosticSignal.NO_DATA,),
            total_rules=0,
            total_pending=0,
            total_processing=0,
            total_sent=0,
            total_faulty=0,
            total_deferred=0,
            total_rate_limited=0,
            stuck_processing_count=0,
            queue_lag_seconds=None,
            problem_rules=(),
            reason="SECRET_TOKEN unavailable",
        ),
    )

    for result in result_objects:
        safe_text = f"{result.to_log_context()} {result.log_label()}"
        for sensitive_value in sensitive_values:
            assert sensitive_value not in safe_text


def test_delivery_observability_is_runtime_neutral_and_safe() -> None:
    source = _read_repo_text("app/delivery_observability.py")
    for forbidden_token in (
        "PostgresRepository",
        "from aiogram",
        "import aiogram",
        "from telethon",
        "import telethon",
        "from app.sender import",
        "import app.sender",
        "from app.worker_runtime import",
        "import app.worker_runtime",
        "from app.video_processor import",
        "import app.video_processor",
    ):
        assert forbidden_token not in source

    snapshot = asyncio.run(DeliveryObservabilityService().collect_snapshot())

    assert snapshot.status == DeliveryHealthStatus.UNKNOWN
    assert snapshot.reason == "metrics_provider_not_configured"


def test_repository_contracts_are_runtime_neutral() -> None:
    source = _read_repo_text("app/repository_contracts.py")
    for forbidden_token in (
        "PostgresRepository",
        "AsyncSession",
        "SessionLocal",
        "from aiogram",
        "import aiogram",
        "from telethon",
        "import telethon",
        "from app.sender import",
        "import app.sender",
        "from app.worker_runtime import",
        "import app.worker_runtime",
        "from app.video_processor import",
        "import app.video_processor",
        "from app.postgres_repository import",
        "import app.postgres_repository",
    ):
        assert forbidden_token not in source

    assert known_repository_responsibility_areas() == (
        "delivery_attempt_ledger",
        "delivery_queue",
        "routing_rules",
        "post_storage",
        "audit_log",
        "problem_state",
        "intro_storage",
        "campaigns",
        "usage_limits",
        "payments",
    )


def test_stage_27_rollout_probe_modules_are_shadow_safe() -> None:
    for module_path in ("app/sender_pipeline_rollout_runtime.py", "app/repost_single_rollout_probe.py"):
        source = _read_repo_text(module_path)
        for forbidden in (
            "from aiogram",
            "import aiogram",
            "from telethon",
            "import telethon",
            "PostgresRepository",
            "worker_runtime",
            "video_processor",
            "TelegramSendGateway",
            "copy_message(",
            ".send_message(",
            ".send_video(",
            ".send_document(",
            ".send_media_group(",
        ):
            assert forbidden not in source, f"{module_path} must not contain {forbidden!r}"
