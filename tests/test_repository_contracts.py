import sys


def test_repository_contracts_module_imports_cleanly_without_runtime_modules():
    sys.modules.pop("app.repository_contracts", None)
    before = set(sys.modules)

    import app.repository_contracts as contracts

    imported_by_contracts = set(sys.modules) - before
    assert contracts.__name__ == "app.repository_contracts"
    assert "aiogram" not in imported_by_contracts
    assert "telethon" not in imported_by_contracts
    assert "app.postgres_repository" not in imported_by_contracts
    assert "app.sender" not in imported_by_contracts
    assert "app.worker_runtime" not in imported_by_contracts
    assert "app.video_processor" not in imported_by_contracts


def test_responsibility_areas_are_stable_unique_and_queryable():
    from app.repository_contracts import (
        is_known_repository_responsibility,
        known_repository_responsibility_areas,
    )

    areas = known_repository_responsibility_areas()

    assert isinstance(areas, tuple)
    assert areas
    assert len(areas) == len(set(areas))
    assert is_known_repository_responsibility("delivery_attempt_ledger") is True
    assert is_known_repository_responsibility("unknown") is False


def test_delivery_attempt_ledger_repository_runtime_protocol():
    from app.repository_contracts import DeliveryAttemptLedgerRepository

    class CompleteFake:
        def get_delivery_attempt_by_idempotency_key(self, *args, **kwargs): pass
        def create_delivery_attempt(self, *args, **kwargs): pass
        def mark_delivery_attempt_sending(self, *args, **kwargs): pass
        def mark_delivery_attempt_accepted(self, *args, **kwargs): pass
        def mark_delivery_attempt_failed(self, *args, **kwargs): pass

    class MissingFake:
        def get_delivery_attempt_by_idempotency_key(self, *args, **kwargs): pass
        def create_delivery_attempt(self, *args, **kwargs): pass
        def mark_delivery_attempt_sending(self, *args, **kwargs): pass
        def mark_delivery_attempt_accepted(self, *args, **kwargs): pass

    assert isinstance(CompleteFake(), DeliveryAttemptLedgerRepository) is True
    assert isinstance(MissingFake(), DeliveryAttemptLedgerRepository) is False


def test_delivery_queue_repository_runtime_protocol():
    from app.repository_contracts import DeliveryQueueRepository

    class CompleteFake:
        def get_due_delivery(self, *args, **kwargs): pass
        def take_due_delivery(self, *args, **kwargs): pass
        def take_due_delivery_and_create_job(self, *args, **kwargs): pass
        def mark_delivery_sent(self, *args, **kwargs): pass
        def mark_delivery_sent_with_target_message(self, *args, **kwargs): pass
        def mark_delivery_faulty(self, *args, **kwargs): pass
        def mark_delivery_pending(self, *args, **kwargs): pass

    class MissingFake:
        def get_due_delivery(self, *args, **kwargs): pass
        def take_due_delivery(self, *args, **kwargs): pass
        def take_due_delivery_and_create_job(self, *args, **kwargs): pass
        def mark_delivery_sent(self, *args, **kwargs): pass
        def mark_delivery_sent_with_target_message(self, *args, **kwargs): pass
        def mark_delivery_faulty(self, *args, **kwargs): pass

    assert isinstance(CompleteFake(), DeliveryQueueRepository) is True
    assert isinstance(MissingFake(), DeliveryQueueRepository) is False


def test_audit_log_repository_runtime_protocol():
    from app.repository_contracts import AuditLogRepository

    class CompleteFake:
        def log_event(self, *args, **kwargs): pass

    class MissingFake:
        pass

    assert isinstance(CompleteFake(), AuditLogRepository) is True
    assert isinstance(MissingFake(), AuditLogRepository) is False


def test_rule_snapshot_repository_runtime_protocol():
    from app.repository_contracts import RuleSnapshotRepository

    class CompleteFake:
        def get_rule(self, *args, **kwargs): pass
        def get_rule_card_snapshot(self, *args, **kwargs): pass

    class MissingFake:
        def get_rule(self, *args, **kwargs): pass

    assert isinstance(CompleteFake(), RuleSnapshotRepository) is True
    assert isinstance(MissingFake(), RuleSnapshotRepository) is False
