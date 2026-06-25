from __future__ import annotations

from pathlib import Path

from app.sender_legacy_inventory import (
    SENDER_LEGACY_INVENTORY,
    SenderLegacyArea,
    SenderLegacyCleanupStatus,
    SenderLegacyEntry,
    SenderLegacyRisk,
    build_legacy_cleanup_readiness,
    entries_by_area,
    entries_by_risk,
    entries_by_status,
    first_rollout_candidates,
    high_risk_entries,
)

ROOT = Path(__file__).resolve().parents[1]


def test_inventory_is_non_empty_and_immutable() -> None:
    assert isinstance(SENDER_LEGACY_INVENTORY, tuple)
    assert len(SENDER_LEGACY_INVENTORY) > 0
    assert all(isinstance(entry, SenderLegacyEntry) for entry in SENDER_LEGACY_INVENTORY)


def test_required_areas_are_covered() -> None:
    covered = {entry.area for entry in SENDER_LEGACY_INVENTORY}

    assert {
        SenderLegacyArea.REPOST_SINGLE,
        SenderLegacyArea.REPOST_ALBUM,
        SenderLegacyArea.VIDEO_SEND,
        SenderLegacyArea.LEGACY_VIDEO_DELIVERY,
        SenderLegacyArea.REPOST_CAMPAIGN,
        SenderLegacyArea.REACTIONS,
        SenderLegacyArea.ATTEMPT_LEDGER,
        SenderLegacyArea.TARGET_VERIFICATION,
        SenderLegacyArea.FINALIZATION,
        SenderLegacyArea.AUDIT_AND_SCHEDULER,
        SenderLegacyArea.QUEUE_AND_STATUS,
        SenderLegacyArea.CONTENT_AND_CAPTION,
        SenderLegacyArea.TRANSPORT_BOUNDARY,
    }.issubset(covered)


def test_entries_have_safe_fields() -> None:
    forbidden_context_tokens = ("def ", "class ", "content_json", "SECRET", "BEGIN")

    for entry in SENDER_LEGACY_INVENTORY:
        assert entry.name
        assert isinstance(entry.area, SenderLegacyArea)
        assert isinstance(entry.risk, SenderLegacyRisk)
        assert isinstance(entry.cleanup_status, SenderLegacyCleanupStatus)
        assert len(entry.log_label()) < 140
        context = entry.to_log_context()
        assert set(context) == {"name", "area", "risk", "cleanup_status", "replacement"}
        assert not any(token in str(context) for token in forbidden_context_tokens)


def test_first_rollout_candidates_are_safe() -> None:
    candidates = first_rollout_candidates()

    assert any(entry.area == SenderLegacyArea.REPOST_SINGLE for entry in candidates)
    assert all(entry.area != SenderLegacyArea.VIDEO_SEND for entry in candidates)
    assert all(entry.risk != SenderLegacyRisk.CRITICAL for entry in candidates)
    assert all(entry.cleanup_status != SenderLegacyCleanupStatus.DO_NOT_TOUCH_YET for entry in candidates)


def test_high_risk_helpers_and_unknown_filters() -> None:
    risks = {entry.risk for entry in high_risk_entries()}

    assert risks
    assert risks.issubset({SenderLegacyRisk.HIGH, SenderLegacyRisk.CRITICAL})
    assert SenderLegacyRisk.HIGH in risks
    assert SenderLegacyRisk.CRITICAL in risks
    assert entries_by_risk("unknown") == ()
    assert entries_by_area("unknown") == ()
    assert entries_by_status("unknown") == ()


def test_readiness_summary_matches_inventory_and_is_admin_safe() -> None:
    readiness = build_legacy_cleanup_readiness()

    assert readiness.total_entries == len(SENDER_LEGACY_INVENTORY)
    assert readiness.ready_for_shadow_count == len(entries_by_status(SenderLegacyCleanupStatus.READY_FOR_SHADOW))
    assert readiness.ready_for_active_rollout_count == len(entries_by_status(SenderLegacyCleanupStatus.READY_FOR_ACTIVE_ROLLOUT))
    assert readiness.do_not_touch_count == len(entries_by_status(SenderLegacyCleanupStatus.DO_NOT_TOUCH_YET))
    assert readiness.high_risk_count == len(entries_by_risk(SenderLegacyRisk.HIGH))
    assert readiness.critical_risk_count == len(entries_by_risk(SenderLegacyRisk.CRITICAL))
    assert set(readiness.to_log_context()) == {
        "total_entries",
        "ready_for_shadow_count",
        "ready_for_active_rollout_count",
        "do_not_touch_count",
        "high_risk_count",
        "critical_risk_count",
    }
    admin_text = readiness.to_admin_text()
    assert "Всего legacy-зон" in admin_text
    assert "Готово к shadow" in admin_text
    assert "Нельзя трогать пока" in admin_text


def test_inventory_references_real_sender_methods_where_possible() -> None:
    sender_source = (ROOT / "app/sender.py").read_text(encoding="utf-8")

    for entry in SENDER_LEGACY_INVENTORY:
        if entry.name.startswith("_") and not entry.name.endswith("_legacy_area"):
            assert f"def {entry.name}" in sender_source


def test_sender_legacy_inventory_has_no_runtime_imports() -> None:
    source = (ROOT / "app/sender_legacy_inventory.py").read_text(encoding="utf-8")

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


def test_docs_exist_and_mention_rollout_and_rollback() -> None:
    source = (ROOT / "docs/sender-legacy-cleanup.md").read_text(encoding="utf-8")

    assert "rollback" in source.lower() or "откат" in source.lower()
    assert "shadow" in source
    assert "single repost" in source
    assert "video" in source
    assert "delivery_attempts" in source
    assert "fail-closed" in source
