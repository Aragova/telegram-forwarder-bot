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
        cursor = repo.get_publication_recovery_cursor(123)
        assert isinstance(cursor, dict)
        assert cursor["pts"] == 2
    finally:
        repo.client.close_all_connections()


def seed(repository, suffix, count=1):
    source = f"-100800{suffix}"
    target = f"-100900{suffix}"
    group = f"group-{suffix}" if count > 1 else None
    with repository.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO routing(source_id,target_id,created_date,tenant_id) VALUES (%s,%s,%s,1) RETURNING id",
                (source,target,"2026-01-01T00:00:00+00:00"),
            )
            rule_id=cur.fetchone()["id"]
            delivery_ids=[]
            source_ids=list(range(1001,1001+count))
            for source_id in source_ids:
                cur.execute(
                    """INSERT INTO posts(message_id,source_channel,content_json,media_group_id,created_at)
                       VALUES (%s,%s,%s,%s,%s) RETURNING id""",
                    (source_id,source,'{"media_kind":"video"}',group,"2026-01-01T00:00:00+00:00"),
                )
                post_id=cur.fetchone()["id"]
                cur.execute(
                    "INSERT INTO deliveries(rule_id,post_id,status,created_at,tenant_id) VALUES (%s,%s,'processing',%s,1) RETURNING id",
                    (rule_id,post_id,"2026-01-01T00:00:00+00:00"),
                )
                delivery_ids.append(cur.fetchone()["id"])
    key=f"test:{suffix}"
    start_attempt(repository,key,rule_id,delivery_ids,source_ids,target)
    return key,rule_id,delivery_ids,source_ids,source,target


def start_attempt(repository,key,rule_id,delivery_ids,source_ids,target):
    assert repository.create_delivery_attempt(
        delivery_id=delivery_ids[0],rule_id=rule_id,tenant_id=1,job_id=None,
        idempotency_key=key,operation_kind="album" if len(delivery_ids)>1 else "single",
        status="created",target_id=target,source_message_ids=source_ids,
    )
    assert repository.mark_delivery_attempt_sending(key)


def register(repository,seeded,raw_ids,mask=None):
    key,rule_id,delivery_ids,source_ids,source,target=seeded
    return repository.register_publication_receipt(
        key=key,delivery_ids=delivery_ids,rule_id=rule_id,account_id=123,
        peer_id=int(target),thread_id=None,source_channel=source,
        source_message_ids=source_ids,scheduled_ids=raw_ids,pending_mask=mask,
    )


def test_stage2_real_pg_mixed_album_is_atomic_and_rollback_preserves_evidence(repo):
    seeded=seed(repo,101,count=2)
    key,rule_id,deliveries,source_ids,source,target=seeded
    assert register(repo,seeded,[500,500],[False,True])
    assert isinstance(repo.get_publication_receipt(key), dict)
    assert register(repo,seeded,[500,500],[False,True])
    assert not register(repo,seeded,[500,502],[False,True])
    assert not repo.mark_delivery_attempt_accepted(key,sent_message_ids=[500,999],telegram_method="mtproto")
    assert not repo.mark_delivery_attempt_verified(key,sent_message_ids=[500,999])
    assert not repo.mark_delivery_faulty_if_attempt_unverified(deliveries[0],key,"stale worker")
    assert repo.get_delivery_attempt_by_idempotency_key(key)["status"]=="accepted"
    assert repo.get_delivery_attempt_by_idempotency_key(key)["sent_message_ids_json"]==[]
    assert not repo.finalize_publication_receipt(key,[500,501])
    assert [repo.get_delivery(d)["status"] for d in deliveries]==["processing","processing"]

    repo.record_telegram_publication_mappings(123,int(target),[(500,501)])
    repo.record_telegram_publication_mappings(123,int(target),[(500,501)])
    assert repo.get_telegram_publication_mappings(123,int(target),[500])==[501]
    assert repo.finalize_publication_receipt(key,[500,501])
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT sent_message_id FROM deliveries WHERE id=ANY(%s) ORDER BY id",(deliveries,))
            assert [row["sent_message_id"] for row in cur.fetchall()]==[500,501]
    assert repo.get_delivery_attempt_by_idempotency_key(key)["sent_message_ids_json"]==[500,501]
    started=repo.get_publication_receipt(key)["send_started_at"]
    assert not repo.claim_publication_reaction(key,[500,999],expected_started_at=started)
    assert repo.claim_publication_reaction(key,[500,501],expected_started_at=started)
    assert not repo.claim_publication_reaction(key,[500,501],expected_started_at=started)
    repo.finish_publication_reaction(key,"done",{"applied":True},expected_started_at=started)
    assert repo.rollback_last_delivery(rule_id)
    assert repo.get_publication_receipt(key) is None
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT published_ids FROM telegram_publication_receipts WHERE rule_id=%s", (rule_id,))
            assert cur.fetchone()["published_ids"] == [500,501]



def test_stage2_real_pg_conflicting_mapping_and_deadline_never_finalize(repo):
    seeded=seed(repo,102)
    key,rule_id,deliveries,source_ids,source,target=seeded
    assert register(repo,seeded,[77])
    repo.record_telegram_publication_mappings(123,int(target),[(77,900)])
    repo.record_telegram_publication_mappings(123,int(target),[(77,901)])
    assert repo.get_telegram_publication_mappings(123,int(target),[77])==[]
    assert not repo.finalize_publication_receipt(key,[900])
    assert repo.get_delivery(deliveries[0])["status"]=="processing"

    seeded2=seed(repo,103)
    key2,_,deliveries2,_,_,target2=seeded2
    assert register(repo,seeded2,[88])
    repo.record_telegram_publication_mappings(123,int(target2),[(88,902)])
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE telegram_publication_receipts SET deadline_at=NOW()-INTERVAL '1 second' WHERE idempotency_key=%s",(key2,))
    assert not repo.finalize_publication_receipt(key2,[902])
    assert repo.expire_publication_receipt(key2,"publication_receipt_expired_no_resend")
    assert repo.get_delivery(deliveries2[0])["status"]=="faulty"
    assert repo.get_delivery_attempt_by_idempotency_key(key2)["status"]=="accepted"



@pytest.fixture
def repo():
    if os.getenv("VIMI_TEST_POSTGRES") != "1":
        pytest.skip("requires disposable PostgreSQL")
    assert os.getenv("APP_PG_DB") == "vimi_publication_ci"
    repository = PostgresRepository()
    try:
        repository.init()
        yield repository
    finally:
        repository.client.close_all_connections()

def test_stage2_queued_echo_is_recognized_before_finalization(repo):
    key, rule_id, delivery_ids, source_ids, source, target = seed(repo, 109)
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE routing SET target_id=%s WHERE id=%s", (source, rule_id))
            cur.execute("UPDATE delivery_attempts SET target_id=%s WHERE idempotency_key=%s", (source, key))
    assert register(repo, (key, rule_id, delivery_ids, source_ids, source, source), [77])
    repo.record_telegram_publication_mappings(123, int(source), [(77, 900)])
    with repo.connect() as conn:
        assert repo._is_self_target_echo_post_for_rule_conn(conn, rule_id=rule_id,
            source_channel=source, source_thread_id=None, message_id=900)
        assert not repo._is_self_target_echo_post_for_rule_conn(conn, rule_id=rule_id,
            source_channel=source, source_thread_id=None, message_id=901)
    assert repo.finalize_publication_receipt(key, [900])
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE routing SET is_active=TRUE,next_run_at=NULL WHERE id=%s", (rule_id,))
            cur.execute("""INSERT INTO posts(message_id,source_channel,content_json,created_at)
                VALUES(900,%s,'{}','2026-01-01T00:00:00+00:00') RETURNING id""",(source,))
            post_id=cur.fetchone()["id"]
            cur.execute("""INSERT INTO deliveries(rule_id,post_id,status,created_at,tenant_id)
                VALUES(%s,%s,'pending','2026-01-01T00:00:00+00:00',1) RETURNING id""",(rule_id,post_id))
            echo_id=cur.fetchone()["id"]
    assert repo.take_due_delivery_and_create_job(rule_id,"2026-12-01T00:00:00+00:00") is None
    assert repo.get_delivery(echo_id)["status"] == "faulty"

def test_stage2_reaction_enqueue_is_atomic_and_one_shot(repo):
    seeded = seed(repo, 110)
    key, rule_id, delivery_ids, source_ids, source, target = seeded
    assert register(repo, seeded, [78])
    repo.record_telegram_publication_mappings(123, int(target), [(78, 901)])
    assert repo.finalize_publication_receipt(key, [901])
    started = repo.get_publication_receipt(key)["send_started_at"]
    assert repo.claim_publication_reaction(key, [901], expected_started_at=started)
    args = dict(tenant_id=1, rule_id=rule_id, target_id=target, message_id=901,
                publication_intent=(key, started))
    assert repo.enqueue_reaction_job(**(args | {"message_id": 78})) is None
    assert repo.enqueue_reaction_job(**(args | {"target_id": "not-a-peer"})) is None
    assert repo.enqueue_reaction_job(**args)
    assert repo.enqueue_reaction_job(**args) is None
    assert repo.get_publication_receipt(key)["reaction_state"] == "done"


def test_stage2_multiple_scheduled_claims_are_all_or_nothing(repo):
    seeded = seed(repo, 116, count=3)
    key, _, _, _, _, target = seeded
    assert register(repo, seeded, [80, 81, 82])
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT scheduled_id FROM telegram_publication_claims WHERE idempotency_key=%s ORDER BY scheduled_id",
                (key,),
            )
            assert [row["scheduled_id"] for row in cur.fetchall()] == [80, 81, 82]

    other_key, other_rule, deliveries, source_ids, source, _ = seed(repo, 117, count=2)
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE routing SET target_id=%s WHERE id=%s", (target, other_rule))
            cur.execute("UPDATE delivery_attempts SET target_id=%s WHERE idempotency_key=%s", (target, other_key))
    other = (other_key, other_rule, deliveries, source_ids, source, target)
    assert not register(repo, other, [79, 81])
    assert repo.get_publication_receipt(other_key) is None
    assert repo.get_delivery_attempt_by_idempotency_key(other_key)["status"] == "sending"
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM telegram_publication_claims WHERE idempotency_key=%s", (other_key,))
            assert cur.fetchone() is None

    repo.record_telegram_publication_mappings(123, int(target), [(80, 180), (81, 181), (82, 182)])
    assert repo.finalize_publication_receipt(key, [180, 181, 182])


def test_faulty_counter_and_clear_preserve_pending_publication_evidence(repo):
    baseline = repo.count_uncleared_faulty_deliveries()
    seeded = seed(repo, 124, count=3)
    key, rule_id, deliveries, _, source, target = seeded
    assert register(repo, seeded, [31, 32, 33])
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE telegram_publication_receipts SET deadline_at=NOW()-INTERVAL '1 second' WHERE idempotency_key=%s", (key,))
    assert repo.expire_publication_receipt(key, "publication_receipt_expired_no_resend")
    assert repo.count_uncleared_faulty_deliveries() == baseline + 3

    # Acknowledgement is an audit event only, not permission to replay.
    assert repo.clear_faulty_delivery_log(deliveries[0], admin_id=12345)
    assert repo.count_uncleared_faulty_deliveries() == baseline + 2
    assert repo.get_delivery(deliveries[0])["status"] == "faulty"
    assert repo.get_delivery_attempt_by_idempotency_key(key)["status"] == "accepted"
    receipt = repo.get_publication_receipt(key)
    assert receipt["published_ids"] is None and receipt["expired_at"] is not None
    with pytest.raises(RuntimeError, match="незавершённая публикация"):
        repo.clear_rule_deliveries(rule_id)

    # Neither the legacy nor the new self-target echo is a visible error.
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE deliveries SET error_text='Self-loop: skipped' WHERE id=%s", (deliveries[1],))
            cur.execute("UPDATE deliveries SET error_text='self_target_echo' WHERE id=%s", (deliveries[2],))
            cur.execute("UPDATE routing SET is_active=FALSE,rescan_owner_token='audit-124' WHERE id=%s", (rule_id,))
    assert repo.count_uncleared_faulty_deliveries() == baseline
    assert not set(deliveries).intersection(row["id"] for row in repo.get_faulty_deliveries(limit=10000))
    with pytest.raises(RuntimeError, match="неизвестным результатом"):
        repo.reset_rule_history_for_replay(rule_id, owner_token="audit-124")
