"""Real PostgreSQL contract; CI creates a dedicated disposable database."""
import os
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import pytest

from app.postgres_repository import PostgresRepository


@pytest.mark.skipif(os.getenv("VIMI_TEST_POSTGRES") != "1", reason="requires disposable PostgreSQL")
def test_durable_publication_contract():
    assert os.getenv("APP_PG_DB") == "vimi_publication_ci"
    repo = PostgresRepository()
    try:
        repo.init()
        key = str(uuid4())
        repo.create_delivery_attempt(delivery_id=1, rule_id=1, tenant_id=1,
                                     job_id=None, idempotency_key=key,
                                     operation_kind="album", status="created")
        assert repo.mark_delivery_attempt_sending(key)

        def reserve(_):
            try:
                return repo.get_or_create_mtproto_random_ids(key, 0, "fingerprint", 3)
            finally:
                repo.client.close_thread_connection()

        with ThreadPoolExecutor(max_workers=4) as pool:
            vectors = list(pool.map(reserve, range(8)))
        assert all(vector == vectors[0] for vector in vectors)
        assert len(set(vectors[0])) == 3
        assert all(0 < value < 2**63 for value in vectors[0])
        restarted = PostgresRepository()
        try:
            assert restarted.get_or_create_mtproto_random_ids(key, 0, "fingerprint", 3) == vectors[0]
            with pytest.raises(ValueError):
                restarted.get_or_create_mtproto_random_ids(key, 0, "different", 3)
        finally:
            restarted.client.close_all_connections()
        assert repo.mark_delivery_attempt_failed(key, status="failed_after_send", error_text="unknown")
        assert not repo.mark_delivery_attempt_failed(key, status="failed_before_send", error_text="late")
        assert not repo.mark_delivery_attempt_sending(key)
        assert repo.get_delivery_attempt_by_idempotency_key(key)["status"] == "failed_after_send"
        with pytest.raises(RuntimeError):
            repo.get_or_create_mtproto_random_ids(key, 0, "fingerprint", 3)
        scope = str(uuid4())
        repo.extend_transport_cooldown(scope, 60)
        repo.extend_transport_cooldown(scope, 1)
        assert repo.get_transport_cooldown(scope) > 30
        # Identity belongs to a ledger row, not a reusable delivery key.
        with repo.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM delivery_attempts WHERE idempotency_key=%s", (key,))
                cur.execute("SELECT COUNT(*) AS n FROM mtproto_send_intents")
                assert cur.fetchone()["n"] == 0
    finally:
        repo.client.close_all_connections()


@pytest.mark.skipif(os.getenv("VIMI_TEST_POSTGRES") != "1", reason="requires disposable PostgreSQL")
def test_reaction_lease_owner_and_retry_budget():
    assert os.getenv("APP_PG_DB") == "vimi_publication_ci"
    repo = PostgresRepository()
    try:
        repo.init()
        job_id = repo.enqueue_reaction_job(tenant_id=1, rule_id=1, target_id="-100123",
                                           message_id=101, max_attempts=2)
        assert repo.lease_due_reaction_job(worker_id="old", lock_timeout_seconds=1)["id"] == job_id
        with repo.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE reaction_jobs SET locked_at=NOW()-interval '10 seconds' WHERE id=%s", (job_id,))
        assert repo.lease_due_reaction_job(worker_id="new", lock_timeout_seconds=1)["id"] == job_id
        assert not repo.heartbeat_reaction_job(job_id=job_id, worker_id="old")
        assert not repo.mark_reaction_job_done(job_id=job_id, worker_id="old", result={})
        assert not repo.mark_reaction_job_failed(job_id=job_id, worker_id="old", error_text="stale")
        assert not repo.mark_reaction_job_skipped(job_id=job_id, worker_id="old", result={})
        assert repo.heartbeat_reaction_job(job_id=job_id, worker_id="new")
        # Verify heartbeat is committed to a distinct connection, not just visible locally.
        reader = PostgresRepository()
        try:
            with reader.connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT locked_at > NOW()-interval '5 seconds' AS fresh FROM reaction_jobs WHERE id=%s", (job_id,))
                    assert cur.fetchone()["fresh"]
        finally:
            reader.client.close_all_connections()
        assert repo.mark_reaction_job_failed(job_id=job_id, worker_id="new",
                                            error_text="exhausted", retry_after_seconds=1)
        reader = PostgresRepository()
        try:
            with reader.connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT status, locked_by FROM reaction_jobs WHERE id=%s", (job_id,))
                    assert cur.fetchone() == {"status": "failed", "locked_by": None}
        finally:
            reader.client.close_all_connections()
        assert repo.lease_due_reaction_job(worker_id="third", lock_timeout_seconds=1) is None
    finally:
        repo.client.close_all_connections()


@pytest.mark.skipif(os.getenv("VIMI_TEST_POSTGRES") != "1", reason="requires disposable PostgreSQL")
@pytest.mark.parametrize("status", ["sending", "accepted", "failed_after_send"])
def test_rescan_preserves_uncertain_attempt(status):
    assert os.getenv("APP_PG_DB") == "vimi_publication_ci"
    repo = PostgresRepository()
    try:
        repo.init()
        owner = str(uuid4())
        with repo.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO routing(source_id,target_id,created_date,is_active,rescan_owner_token) "
                    "VALUES(%s,%s,'2026-01-01',FALSE,%s) RETURNING id",
                    ("-100123", {"sending": "-100456", "accepted": "-100457",
                                 "failed_after_send": "-100458"}[status], owner),
                )
                rule_id = cur.fetchone()["id"]
        key = str(uuid4())
        repo.create_delivery_attempt(delivery_id=1, rule_id=rule_id, tenant_id=1,
                                     job_id=None, idempotency_key=key,
                                     operation_kind="single", status=status)
        with pytest.raises(RuntimeError, match="неизвестным результатом"):
            repo.reset_rule_history_for_replay(rule_id, owner_token=owner)
        assert repo.get_delivery_attempt_by_idempotency_key(key)["status"] == status
    finally:
        repo.client.close_all_connections()


@pytest.mark.skipif(os.getenv("VIMI_TEST_POSTGRES") != "1", reason="requires disposable PostgreSQL")
def test_publication_inbox_preserves_conflicts_and_cursor_cas():
    from datetime import datetime, timezone

    assert os.getenv("APP_PG_DB") == "vimi_publication_ci"
    repo = PostgresRepository()
    try:
        repo.init()
        repo.record_telegram_publication_mappings(123, -100456, [(9, 100), (10, 101)])
        repo.record_telegram_publication_mappings(123, -100456, [(9, 100)])
        assert repo.get_telegram_publication_mappings(123, -100456, [10, 9]) == [101, 100]
        assert repo.get_telegram_publication_mapping(124, -100456, 9) is None
        assert repo.get_telegram_publication_mapping(123, -100457, 9) is None
        repo.record_telegram_publication_mappings(123, -100456, [(9, 102)])
        assert repo.get_telegram_publication_mapping(123, -100456, 9) is None
        assert repo.get_telegram_publication_mappings(123, -100456, [9, 10]) == []
        repo.record_telegram_publication_mappings(123, -100456, [(11, 101)])
        assert repo.get_telegram_publication_mapping(123, -100456, 10) is None
        now = datetime.now(timezone.utc)
        repo.initialize_publication_recovery_cursor(123, 1, 0, now)
        assert repo.advance_publication_recovery_cursor(123, 0, 2, 0, now)
        assert not repo.advance_publication_recovery_cursor(123, 0, 3, 0, now)
        assert repo.get_publication_recovery_cursor(123)["pts"] == 2
    finally:
        repo.client.close_all_connections()
