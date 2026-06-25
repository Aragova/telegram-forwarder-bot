"""Narrow repository responsibility contracts for future repository cleanup.

This module is intentionally runtime-neutral: it must not import concrete
repository implementations, Telegram clients, DB sessions, sender runtime, or
transport/video processing code.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class DeliveryAttemptLedgerRepository(Protocol):
    """Repository responsibility required by delivery attempt ledger services."""

    def get_delivery_attempt_by_idempotency_key(self, *args: Any, **kwargs: Any) -> Any: ...
    def create_delivery_attempt(self, *args: Any, **kwargs: Any) -> Any: ...
    def mark_delivery_attempt_sending(self, *args: Any, **kwargs: Any) -> Any: ...
    def mark_delivery_attempt_accepted(self, *args: Any, **kwargs: Any) -> Any: ...
    def mark_delivery_attempt_failed(self, *args: Any, **kwargs: Any) -> Any: ...


@runtime_checkable
class DeliveryQueueRepository(Protocol):
    """Stable delivery queue/status methods exposed by the repository."""

    def get_due_delivery(self, *args: Any, **kwargs: Any) -> Any: ...
    def take_due_delivery(self, *args: Any, **kwargs: Any) -> Any: ...
    def take_due_delivery_and_create_job(self, *args: Any, **kwargs: Any) -> Any: ...
    def mark_delivery_sent(self, *args: Any, **kwargs: Any) -> Any: ...
    def mark_delivery_sent_with_target_message(self, *args: Any, **kwargs: Any) -> Any: ...
    def mark_delivery_faulty(self, *args: Any, **kwargs: Any) -> Any: ...
    def mark_delivery_pending(self, *args: Any, **kwargs: Any) -> Any: ...


@runtime_checkable
class AuditLogRepository(Protocol):
    """Repository responsibility for writing audit events."""

    def log_event(self, *args: Any, **kwargs: Any) -> Any: ...


@runtime_checkable
class RuleSnapshotRepository(Protocol):
    """Repository responsibility for stable rule/routing snapshots."""

    def get_rule(self, *args: Any, **kwargs: Any) -> Any: ...
    def get_rule_card_snapshot(self, *args: Any, **kwargs: Any) -> Any: ...


REPOSITORY_RESPONSIBILITY_AREAS: tuple[str, ...] = (
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


def known_repository_responsibility_areas() -> tuple[str, ...]:
    """Return the stable list of known repository responsibility areas."""

    return REPOSITORY_RESPONSIBILITY_AREAS


def is_known_repository_responsibility(area: str) -> bool:
    """Return whether *area* is a known repository responsibility area."""

    return area in REPOSITORY_RESPONSIBILITY_AREAS
