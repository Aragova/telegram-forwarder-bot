from __future__ import annotations

import inspect

from app.postgres_repository import PostgresRepository


def test_get_stuck_processing_deliveries_uses_select_only() -> None:
    source = inspect.getsource(PostgresRepository.get_stuck_processing_deliveries)
    upper = source.upper()
    assert "SELECT" in upper
    assert "STATUS = 'PROCESSING'" in upper
    assert "LIMIT" in upper
    assert "J.CREATED_AT" in upper
    assert "D.CREATED_AT" not in upper
    for forbidden in ("UPDATE", "INSERT", "DELETE", "DROP", "ALTER", "CREATE TABLE", "TRUNCATE"):
        assert forbidden not in upper


def test_reset_stuck_processing_deliveries_source_guards() -> None:
    source = inspect.getsource(PostgresRepository.reset_stuck_processing_deliveries_to_pending)
    upper = source.upper()
    assert "UPDATE DELIVERIES" in upper
    assert "STATUS = 'PROCESSING'" in upper
    assert "MAKE_INTERVAL(SECS => %S)" in upper or "MAKE_INTERVAL(SECS => %S)".replace("%S", "%S") in upper
    assert "LIMIT" in upper
    assert "J.CREATED_AT" in upper
    assert "D.CREATED_AT" not in upper
    assert "SENT_AT IS NULL" in upper
    assert "SENT_MESSAGE_ID IS NULL" in upper
    assert "SENT_MESSAGE_IDS_JSON IS NULL" in upper
    for forbidden in ("DROP", "ALTER", "CREATE TABLE", "TRUNCATE", "DELETE"):
        assert forbidden not in upper


def test_legacy_reset_stuck_processing_is_not_manual_recovery_method() -> None:
    source = inspect.getsource(PostgresRepository.reset_stuck_processing_deliveries_to_pending)
    assert "pending'" in source
    assert "faulty" not in source
    assert "sent'" not in source


def test_recovery_uses_job_timestamp_not_delivery_created_at() -> None:
    read_source = inspect.getsource(PostgresRepository.get_stuck_processing_deliveries).upper()
    reset_source = inspect.getsource(PostgresRepository.reset_stuck_processing_deliveries_to_pending).upper()
    for source in (read_source, reset_source):
        assert "J.CREATED_AT" in source
        assert "PROCESSING_CLOCK.PROCESSING_JOB_CREATED_AT" in source
        assert "D.CREATED_AT" not in source
        assert "CREATED_AT::TIMESTAMPTZ" not in source
