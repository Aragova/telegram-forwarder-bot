from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class SenderLegacyArea(str, Enum):
    REPOST_SINGLE = "repost_single"
    REPOST_ALBUM = "repost_album"
    VIDEO_SEND = "video_send"
    LEGACY_VIDEO_DELIVERY = "legacy_video_delivery"
    REPOST_CAMPAIGN = "repost_campaign"
    REACTIONS = "reactions"
    ATTEMPT_LEDGER = "attempt_ledger"
    TARGET_VERIFICATION = "target_verification"
    FINALIZATION = "finalization"
    AUDIT_AND_SCHEDULER = "audit_and_scheduler"
    QUEUE_AND_STATUS = "queue_and_status"
    CONTENT_AND_CAPTION = "content_and_caption"
    TRANSPORT_BOUNDARY = "transport_boundary"


class SenderLegacyRisk(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class SenderLegacyCleanupStatus(str, Enum):
    KEEP_AS_FALLBACK = "keep_as_fallback"
    READY_FOR_ADAPTER = "ready_for_adapter"
    READY_FOR_SHADOW = "ready_for_shadow"
    READY_FOR_ACTIVE_ROLLOUT = "ready_for_active_rollout"
    DO_NOT_TOUCH_YET = "do_not_touch_yet"


@dataclass(frozen=True, slots=True)
class SenderLegacyEntry:
    name: str
    area: SenderLegacyArea
    risk: SenderLegacyRisk
    cleanup_status: SenderLegacyCleanupStatus
    replacement: str | None = None
    notes: str | None = None

    def to_log_context(self) -> dict[str, str | None]:
        return {
            "name": self.name,
            "area": self.area.value,
            "risk": self.risk.value,
            "cleanup_status": self.cleanup_status.value,
            "replacement": self.replacement,
        }

    def log_label(self) -> str:
        return f"{self.area.value}:{self.name}:{self.cleanup_status.value}"


SENDER_LEGACY_INVENTORY: tuple[SenderLegacyEntry, ...] = (
    SenderLegacyEntry(
        name="_deliver_single",
        area=SenderLegacyArea.REPOST_SINGLE,
        risk=SenderLegacyRisk.HIGH,
        cleanup_status=SenderLegacyCleanupStatus.READY_FOR_SHADOW,
        replacement="legacy-preserving single repost extraction module",
        notes="Первый кандидат для extraction без изменения поведения; legacy copy-first flow должен остаться source of truth.",
    ),
    SenderLegacyEntry(
        name="_deliver_album",
        area=SenderLegacyArea.REPOST_ALBUM,
        risk=SenderLegacyRisk.HIGH,
        cleanup_status=SenderLegacyCleanupStatus.READY_FOR_ADAPTER,
        replacement="legacy-preserving album extraction module",
        notes="Нужно сохранить порядок альбома, logical item semantics и защиту от частичной доставки.",
    ),
    SenderLegacyEntry(
        name="_deliver_single_video",
        area=SenderLegacyArea.VIDEO_SEND,
        risk=SenderLegacyRisk.CRITICAL,
        cleanup_status=SenderLegacyCleanupStatus.DO_NOT_TOUCH_YET,
        replacement="legacy-preserving video extraction module",
        notes="Video path имеет download/process/send side effects; переносить только после single/album rollout.",
    ),
    SenderLegacyEntry(
        name="_download_video_source",
        area=SenderLegacyArea.LEGACY_VIDEO_DELIVERY,
        risk=SenderLegacyRisk.CRITICAL,
        cleanup_status=SenderLegacyCleanupStatus.DO_NOT_TOUCH_YET,
        replacement="legacy-preserving video delivery extraction module",
        notes="Мост к legacy video processing и файловым side effects; без отдельного этапа не менять.",
    ),
    SenderLegacyEntry(
        name="execute_repost_campaign_send_copy_from_job",
        area=SenderLegacyArea.REPOST_CAMPAIGN,
        risk=SenderLegacyRisk.HIGH,
        cleanup_status=SenderLegacyCleanupStatus.KEEP_AS_FALLBACK,
        replacement="legacy-preserving campaign extraction module",
        notes="Campaign copy/delete должен сохранять текущие Telegram side effects и cleanup guarantees.",
    ),
    SenderLegacyEntry(
        name="execute_repost_campaign_delete_copy_from_job",
        area=SenderLegacyArea.REPOST_CAMPAIGN,
        risk=SenderLegacyRisk.HIGH,
        cleanup_status=SenderLegacyCleanupStatus.KEEP_AS_FALLBACK,
        replacement="legacy-preserving campaign extraction module",
        notes="Удаление campaign копий переносить только вместе с проверенным rollback планом.",
    ),
    SenderLegacyEntry(
        name="_add_reaction_for_rule_if_possible",
        area=SenderLegacyArea.REACTIONS,
        risk=SenderLegacyRisk.HIGH,
        cleanup_status=SenderLegacyCleanupStatus.DO_NOT_TOUCH_YET,
        replacement="ReactionPostSendService",
        notes="Reaction logic запрещена к изменению без отдельной задачи; возможны Telegram side effects.",
    ),
    SenderLegacyEntry(
        name="_mark_delivery_sent_sync",
        area=SenderLegacyArea.ATTEMPT_LEDGER,
        risk=SenderLegacyRisk.HIGH,
        cleanup_status=SenderLegacyCleanupStatus.KEEP_AS_FALLBACK,
        replacement="existing sender.py delivery attempt helpers",
        notes="delivery_attempts и статусы должны оставаться консистентными для rollback и диагностики.",
    ),
    SenderLegacyEntry(
        name="_confirm_target_delivery_message_ids_with_retry",
        area=SenderLegacyArea.TARGET_VERIFICATION,
        risk=SenderLegacyRisk.HIGH,
        cleanup_status=SenderLegacyCleanupStatus.READY_FOR_ADAPTER,
        replacement="existing sender.py target confirmation helpers",
        notes="Защищает от false success после Telegram send/copy/reupload; удалять только после parity checks.",
    ),
    SenderLegacyEntry(
        name="_log_delivery_final_success_sync",
        area=SenderLegacyArea.FINALIZATION,
        risk=SenderLegacyRisk.MEDIUM,
        cleanup_status=SenderLegacyCleanupStatus.READY_FOR_ADAPTER,
        replacement="existing sender.py finalization helpers",
        notes="Финализация должна сохранить audit log, success/failure mapping и post-send steps.",
    ),
    SenderLegacyEntry(
        name="_touch_rule_after_send_sync",
        area=SenderLegacyArea.AUDIT_AND_SCHEDULER,
        risk=SenderLegacyRisk.HIGH,
        cleanup_status=SenderLegacyCleanupStatus.DO_NOT_TOUCH_YET,
        replacement="existing sender.py audit/scheduler helpers",
        notes="Scheduler/audit touch влияет на следующие доставки; менять только отдельным этапом.",
    ),
    SenderLegacyEntry(
        name="_take_due_delivery_sync",
        area=SenderLegacyArea.QUEUE_AND_STATUS,
        risk=SenderLegacyRisk.CRITICAL,
        cleanup_status=SenderLegacyCleanupStatus.DO_NOT_TOUCH_YET,
        replacement="repository logical queue helpers and diagnostics",
        notes="Очередь и статусы нельзя считать альтернативно; source of truth остаётся в repository queue helpers.",
    ),
    SenderLegacyEntry(
        name="_content_from_message_or_post",
        area=SenderLegacyArea.CONTENT_AND_CAPTION,
        risk=SenderLegacyRisk.MEDIUM,
        cleanup_status=SenderLegacyCleanupStatus.READY_FOR_SHADOW,
        replacement="DeliveryContentHelpers",
        notes="Caption/entities/content parity можно проверять в shadow без Telegram writes.",
    ),
    SenderLegacyEntry(
        name="_copy_single_via_bot",
        area=SenderLegacyArea.TRANSPORT_BOUNDARY,
        risk=SenderLegacyRisk.CRITICAL,
        cleanup_status=SenderLegacyCleanupStatus.KEEP_AS_FALLBACK,
        replacement="existing transport-wrapped Telegram writes",
        notes="Граница Telegram write side effects; fallback обязателен до завершения controlled rollout.",
    ),
)


@dataclass(frozen=True, slots=True)
class SenderLegacyCleanupReadiness:
    total_entries: int
    ready_for_shadow_count: int
    ready_for_active_rollout_count: int
    do_not_touch_count: int
    high_risk_count: int
    critical_risk_count: int

    def to_log_context(self) -> dict[str, int]:
        return {
            "total_entries": self.total_entries,
            "ready_for_shadow_count": self.ready_for_shadow_count,
            "ready_for_active_rollout_count": self.ready_for_active_rollout_count,
            "do_not_touch_count": self.do_not_touch_count,
            "high_risk_count": self.high_risk_count,
            "critical_risk_count": self.critical_risk_count,
        }

    def to_admin_text(self) -> str:
        return (
            "🧹 Legacy cleanup readiness\n\n"
            f"Всего legacy-зон: {self.total_entries}\n"
            f"Готово к shadow: {self.ready_for_shadow_count}\n"
            f"Готово к active rollout: {self.ready_for_active_rollout_count}\n"
            f"Нельзя трогать пока: {self.do_not_touch_count}\n"
            f"Высокий риск: {self.high_risk_count}\n"
            f"Критический риск: {self.critical_risk_count}"
        )


def _coerce_area(area: SenderLegacyArea | str) -> SenderLegacyArea | None:
    try:
        return area if isinstance(area, SenderLegacyArea) else SenderLegacyArea(area)
    except ValueError:
        return None


def _coerce_status(status: SenderLegacyCleanupStatus | str) -> SenderLegacyCleanupStatus | None:
    try:
        return status if isinstance(status, SenderLegacyCleanupStatus) else SenderLegacyCleanupStatus(status)
    except ValueError:
        return None


def _coerce_risk(risk: SenderLegacyRisk | str) -> SenderLegacyRisk | None:
    try:
        return risk if isinstance(risk, SenderLegacyRisk) else SenderLegacyRisk(risk)
    except ValueError:
        return None


def list_legacy_entries() -> tuple[SenderLegacyEntry, ...]:
    return SENDER_LEGACY_INVENTORY


def entries_by_area(area: SenderLegacyArea | str) -> tuple[SenderLegacyEntry, ...]:
    resolved = _coerce_area(area)
    if resolved is None:
        return ()
    return tuple(entry for entry in SENDER_LEGACY_INVENTORY if entry.area == resolved)


def entries_by_status(status: SenderLegacyCleanupStatus | str) -> tuple[SenderLegacyEntry, ...]:
    resolved = _coerce_status(status)
    if resolved is None:
        return ()
    return tuple(entry for entry in SENDER_LEGACY_INVENTORY if entry.cleanup_status == resolved)


def entries_by_risk(risk: SenderLegacyRisk | str) -> tuple[SenderLegacyEntry, ...]:
    resolved = _coerce_risk(risk)
    if resolved is None:
        return ()
    return tuple(entry for entry in SENDER_LEGACY_INVENTORY if entry.risk == resolved)


def first_rollout_candidates() -> tuple[SenderLegacyEntry, ...]:
    return tuple(
        entry
        for entry in SENDER_LEGACY_INVENTORY
        if entry.cleanup_status
        in {
            SenderLegacyCleanupStatus.READY_FOR_SHADOW,
            SenderLegacyCleanupStatus.READY_FOR_ACTIVE_ROLLOUT,
        }
        and entry.risk != SenderLegacyRisk.CRITICAL
    )


def high_risk_entries() -> tuple[SenderLegacyEntry, ...]:
    return tuple(
        entry
        for entry in SENDER_LEGACY_INVENTORY
        if entry.risk in {SenderLegacyRisk.HIGH, SenderLegacyRisk.CRITICAL}
    )


def build_legacy_cleanup_readiness() -> SenderLegacyCleanupReadiness:
    return SenderLegacyCleanupReadiness(
        total_entries=len(SENDER_LEGACY_INVENTORY),
        ready_for_shadow_count=len(entries_by_status(SenderLegacyCleanupStatus.READY_FOR_SHADOW)),
        ready_for_active_rollout_count=len(entries_by_status(SenderLegacyCleanupStatus.READY_FOR_ACTIVE_ROLLOUT)),
        do_not_touch_count=len(entries_by_status(SenderLegacyCleanupStatus.DO_NOT_TOUCH_YET)),
        high_risk_count=len(entries_by_risk(SenderLegacyRisk.HIGH)),
        critical_risk_count=len(entries_by_risk(SenderLegacyRisk.CRITICAL)),
    )
