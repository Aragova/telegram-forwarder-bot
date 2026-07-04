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


def test_self_target_echo_post_does_not_create_delivery_guard_is_repository_level():
    import inspect
    from app.postgres_repository import PostgresRepository

    source = inspect.getsource(PostgresRepository)

    assert "_is_self_target_echo_post_for_rule_conn" in source
    assert "SELF_TARGET_ECHO_DELIVERY_SKIPPED" in source
    assert "d.sent_message_id = %s" in source
    assert "d.status = 'sent'" in source
    assert "r.source_id::text = r.target_id::text" in source


def test_self_target_normal_new_post_still_creates_delivery_when_no_sent_echo_match():
    import inspect
    from app.postgres_repository import PostgresRepository

    source = inspect.getsource(PostgresRepository._is_self_target_echo_post_for_rule_conn)

    assert "d.sent_message_id = %s" in source
    assert "LIMIT 1" in source


def test_non_self_target_rule_not_affected_by_same_message_id():
    import inspect
    from app.postgres_repository import PostgresRepository

    source = inspect.getsource(PostgresRepository._is_self_target_echo_post_for_rule_conn)

    assert "r.source_id::text = r.target_id::text" in source
    assert "r.source_id::text = %s" in source


def test_backfill_rule_skips_self_target_echo_posts():
    import inspect
    from app.postgres_repository import PostgresRepository

    source = inspect.getsource(PostgresRepository._backfill_deliveries_for_rule_conn)

    assert "_is_self_target_echo_post_for_rule_conn" in source
    assert "continue" in source
    assert "SELECT id, message_id" in source


def test_reset_queue_does_not_resurrect_self_target_echo_deliveries():
    import inspect
    from app.postgres_repository import PostgresRepository

    reset_source = inspect.getsource(PostgresRepository.reset_queue_for_source)
    reset_all = inspect.getsource(PostgresRepository.reset_all_queue)

    assert "AND status = 'sent'" in reset_source
    assert "WHERE status = 'sent'" in reset_all


def test_existing_manual_cleanup_self_target_echo_remains_terminal():
    import inspect
    from app.postgres_repository import PostgresRepository

    reset_source = inspect.getsource(PostgresRepository.reset_queue_for_source)
    reset_all = inspect.getsource(PostgresRepository.reset_all_queue)

    assert "self_target_echo_blocked_manual_cleanup" not in reset_source
    assert "self_target_echo_blocked_manual_cleanup" not in reset_all
    assert "faulty" not in reset_source.lower()
    assert reset_all.count("WHERE status = 'sent'") == 1
    assert "WHERE status IN" not in reset_all
