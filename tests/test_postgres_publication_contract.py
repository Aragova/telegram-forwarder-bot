"""Real PostgreSQL contract; CI creates a dedicated disposable database."""
import json
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
def test_restart_archives_historical_attempt_without_inventing_result(status):
    assert os.getenv("APP_PG_DB") == "vimi_publication_ci"
    repo = PostgresRepository()
    try:
        repo.init()
        with repo.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO routing(source_id,target_id,created_date,is_active) "
                    "VALUES(%s,%s,'2026-01-01',FALSE) RETURNING id",
                    ("-100123", {"sending": "-100456", "accepted": "-100457",
                                 "failed_after_send": "-100458"}[status]),
                )
                rule_id = cur.fetchone()["id"]
        key = str(uuid4())
        repo.create_delivery_attempt(delivery_id=1, rule_id=rule_id, tenant_id=1,
                                     job_id=None, idempotency_key=key,
                                     operation_kind="single", status=status)
        with repo.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """UPDATE delivery_attempts SET created_at=TIMESTAMPTZ '2026-11-21',
                       updated_at=TIMESTAMPTZ '2026-11-21' WHERE idempotency_key=%s""",
                    (key,),
                )
        attempt = repo.get_delivery_attempt_by_idempotency_key(key)
        assert _restart_from_snapshot(repo, rule_id) == (1, 0, 0)
        assert repo.get_delivery_attempt_by_idempotency_key(key) is None
        archived = repo.get_delivery_attempt_by_idempotency_key(f"{key}:rescan:{attempt['id']}")
        assert archived["status"] == status
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


def _restart_from_snapshot(repo, rule_id, *, observed=None, high_water=None):
    """Model a complete Telegram scan before committing the new queue."""
    was_active, owner = repo.begin_rule_rescan(rule_id, fresh=True)
    complete = False
    try:
        if high_water is None:
            high_water = repo.get_rule_scan_start_max_message_id(rule_id)
        if observed is None:
            with repo.connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("""SELECT p.message_id FROM deliveries d
                                   JOIN posts p ON p.id=d.post_id WHERE d.rule_id=%s""",
                                (rule_id,))
                    observed = {row["message_id"] for row in cur.fetchall()}
        result = repo.restart_rule_queue(
            rule_id, owner_token=owner, allowed_message_ids=observed,
            include_newer_than_message_id=high_water,
        )
        complete = True
        return result
    finally:
        repo.finish_rule_rescan(rule_id, was_active=was_active, owner_token=owner,
                                queue_rebuilt=complete)


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
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE routing SET rescan_owner_token=NULL WHERE id=%s", (rule_id,))
    assert _restart_from_snapshot(repo, rule_id) == (1,3,3)
    assert all(repo.get_delivery(d) is None for d in deliveries)
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT idempotency_key,expired_at FROM telegram_publication_receipts WHERE rule_id=%s",(rule_id,))
            archived=cur.fetchone()
            assert ":rescan:" in archived["idempotency_key"] and archived["expired_at"] is not None


def test_rescan_legacy_video_history_431_and_atomic_rebuild(repo, monkeypatch):
    _, rule_id, deliveries, source_ids, _, target = seed(repo, 131, count=3)
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM delivery_attempts WHERE rule_id=%s", (rule_id,))
            cur.execute("UPDATE routing SET mode='video',is_active=TRUE WHERE id=%s", (rule_id,))
            cur.executemany("UPDATE deliveries SET status='sent',sent_message_id=%s,target_id_snapshot=%s WHERE id=%s", [(649+i,target,d) for i,d in enumerate(deliveries)])
            cur.execute(
                """INSERT INTO delivery_attempts
                   (delivery_id,rule_id,tenant_id,idempotency_key,operation_kind,status,telegram_method,
                    target_id,source_message_ids_json,sent_message_ids_json,created_at,updated_at)
                   SELECT n,%s,1,'delivery:'||n::text||':target:'||%s||':video_send:v1',
                          'video_send','accepted','video_send',%s,jsonb_build_array(src),
                          jsonb_build_array(dst),TIMESTAMPTZ '2026-11-21 10:00:00+00',TIMESTAMPTZ '2026-11-21 10:00:00+00'
                   FROM unnest(%s::bigint[],%s::bigint[],%s::bigint[]) AS members(n,src,dst)""",
                (rule_id,target,target,deliveries+list(range(9000001,9000429)),
                 source_ids+list(range(1,429)),[649,650,651]+list(range(1,429))),
            )
    with monkeypatch.context() as patch:
        def fail():
            raise RuntimeError("rebuild failed")
        patch.setattr("app.postgres_repository.utc_now_iso", fail)
        with pytest.raises(RuntimeError, match="rebuild failed"):
            _restart_from_snapshot(repo, rule_id)
    assert all(repo.get_delivery(d)["status"]=="sent" for d in deliveries)
    archived, deleted, rebuilt = _restart_from_snapshot(repo, rule_id)
    assert (archived, deleted, rebuilt)==(431,3,3)
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS n FROM delivery_attempts WHERE rule_id=%s AND status='accepted' AND idempotency_key LIKE %s", (rule_id,"%:rescan:%"))
            assert cur.fetchone()["n"]==431
            cur.execute("SELECT id,status FROM deliveries WHERE rule_id=%s", (rule_id,))
            rows=cur.fetchall()
            assert len(rows)==3 and all(row["status"]=="pending" and row["id"] not in deliveries for row in rows)
    assert repo.get_rule(rule_id).is_active


@pytest.mark.parametrize(("raw_ids", "has_anchor"), [
    ("[651]", True),
    ("[651, null]", True),
    ("[651, 652]", True),
    ("null", True),
    ('{"id": 651}', True),
    ("[651]", False),
    ("[651, 652]", False),
])
def test_restart_preserves_historical_ids_without_interpreting_them(
    repo, raw_ids, has_anchor,
):
    """Explicit restart does not claim that any old candidate ID was verified."""
    _, rule_id, deliveries, _, _, target = seed(repo, uuid4().int % 10**12)
    delivery_id = deliveries[0]
    key = f"delivery:{delivery_id}:target:{target}:video_send:v1"
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """UPDATE deliveries SET status='sent', sent_message_id=651,
                   target_id_snapshot=%s WHERE id=%s""",
                (target, delivery_id),
            )
            cur.execute(
                """UPDATE delivery_attempts SET idempotency_key=%s,
                   operation_kind='video_send', status='accepted',
                   telegram_method='video_send', target_id=%s,
                   sent_message_ids_json=%s::jsonb, error_text=NULL,
                   created_at=TIMESTAMPTZ '2026-11-21 10:00:00+00',
                   updated_at=TIMESTAMPTZ '2026-11-21 10:00:00+00'
                   WHERE rule_id=%s""",
                (key, target, raw_ids, rule_id),
            )
            if not has_anchor:
                # The attempt ledger is not FK-cascade-linked to deliveries.
                cur.execute("DELETE FROM deliveries WHERE id=%s", (delivery_id,))
    attempt = repo.get_delivery_attempt_by_idempotency_key(key)
    assert _restart_from_snapshot(repo, rule_id) == (1, int(has_anchor), int(has_anchor))
    archived = repo.get_delivery_attempt_by_idempotency_key(f"{key}:rescan:{attempt['id']}")
    assert archived["status"] == "accepted"
    assert archived["sent_message_ids_json"] == attempt["sent_message_ids_json"]




def test_rule_rescan_confirmation_one_time_per_actor(repo):
    _, rule_id, _, _, _, _ = seed(repo, uuid4().int % 10**12)
    assert repo.issue_rule_rescan_confirmation(rule_id, 321, "first")
    assert not repo.consume_rule_rescan_confirmation(rule_id, 999, "first")
    assert not repo.consume_rule_rescan_confirmation(rule_id, 321, "wrong")
    assert repo.consume_rule_rescan_confirmation(rule_id, 321, "first")
    assert not repo.consume_rule_rescan_confirmation(rule_id, 321, "first")

    assert repo.issue_rule_rescan_confirmation(rule_id, 321, "stale")
    assert repo.issue_rule_rescan_confirmation(rule_id, 321, "current")
    assert not repo.consume_rule_rescan_confirmation(rule_id, 321, "stale")
    assert repo.consume_rule_rescan_confirmation(rule_id, 321, "current")

    assert repo.issue_rule_rescan_confirmation(rule_id, 321, "expired")
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE routing SET rescan_confirmation_until=NOW()-INTERVAL '1 second' WHERE id=%s",
                (rule_id,),
            )
    assert not repo.consume_rule_rescan_confirmation(rule_id, 321, "expired")

    assert repo.issue_rule_rescan_confirmation(rule_id, 321, "locked")
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE routing SET rescan_owner_token='busy' WHERE id=%s", (rule_id,))
    assert not repo.consume_rule_rescan_confirmation(rule_id, 321, "locked")
    assert not repo.issue_rule_rescan_confirmation(rule_id, 321, "replaced")


@pytest.mark.parametrize("with_intent", [True, False])
def test_rescan_archives_video_intent_without_rewriting_outcome(repo, with_intent):
    _, rule_id, deliveries, _, _, target = seed(repo, 132 if with_intent else 13201)
    key=f"delivery:{deliveries[0]}:target:{target}:video_send:v1"
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE deliveries SET status='sent',sent_message_id=651 WHERE id=%s", (deliveries[0],))
            cur.execute("UPDATE delivery_attempts SET idempotency_key=%s,operation_kind='video_send',telegram_method='video_send' WHERE rule_id=%s", (key,rule_id))
    if with_intent:
        repo.get_or_create_mtproto_random_ids(key,0,"legacy-test",1)
    assert repo.mark_delivery_attempt_accepted(key,sent_message_ids=[651],telegram_method="video_send")
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE delivery_attempts SET created_at=TIMESTAMPTZ '2026-11-21 10:00:00+00', updated_at=TIMESTAMPTZ '2026-11-21 10:00:00+00' WHERE rule_id=%s", (rule_id,))
    attempt = repo.get_delivery_attempt_by_idempotency_key(key)
    assert _restart_from_snapshot(repo, rule_id)[0] == 1
    archived = repo.get_delivery_attempt_by_idempotency_key(f"{key}:rescan:{attempt['id']}")
    assert archived["status"] == "accepted"
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS n FROM mtproto_send_intents WHERE attempt_id=%s", (attempt["id"],))
            assert cur.fetchone()["n"] == int(with_intent)


def test_keep_position_replaces_orphan_with_new_pending_delivery(repo):
    _, rule_id, deliveries, source_ids, _, _ = seed(repo, 133)
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE delivery_attempts SET delivery_id=9001000,status='accepted',sent_message_ids_json='[77]'::jsonb WHERE rule_id=%s", (rule_id,))
            cur.execute("UPDATE deliveries SET status='pending' WHERE rule_id=%s", (rule_id,))
    active, owner = repo.begin_rule_rescan(rule_id)
    assert repo.rebuild_rule_keep_position(rule_id, owner_token=owner,
        allowed_message_ids=set(source_ids), include_newer_than_message_id=max(source_ids),
        before_message_id=None)==(1,0,0,0)
    assert repo.get_delivery(deliveries[0]) is None
    assert len(repo.get_rule_queue_logical_items(rule_id)) == 1
    repo.finish_rule_rescan(rule_id,was_active=active,owner_token=owner,queue_rebuilt=True)


def test_fresh_self_loop_keeps_old_echo_evidence_across_replays(repo):
    _, rule_id, deliveries, source_ids, source, _ = seed(repo, 134,count=2)
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM delivery_attempts WHERE rule_id=%s", (rule_id,))
            cur.execute("UPDATE routing SET target_id=source_id WHERE id=%s", (rule_id,))
            cur.execute("UPDATE deliveries SET status='sent',sent_message_id=%s,target_id_snapshot=%s WHERE id=%s", (source_ids[1],source,deliveries[0]))
            cur.execute("UPDATE deliveries SET status='pending' WHERE id=%s", (deliveries[1],))
    for _ in range(2):
        _,_,rebuilt = _restart_from_snapshot(repo, rule_id)
        assert rebuilt==1
        with repo.connect() as conn:
            assert repo._is_self_target_echo_post_for_rule_conn(conn,rule_id=rule_id,
                source_channel=source,source_thread_id=None,message_id=source_ids[1])
            with conn.cursor() as cur:
                assert repo._get_rescan_echo_ids(cur, rule_id) == {source_ids[1]}



def test_rescan_rejects_route_change_and_manual_clear(repo):
    key, rule_id, deliveries, source_ids, source, target = seed(repo, 135)
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM delivery_attempts WHERE rule_id=%s", (rule_id,))
            cur.execute("UPDATE deliveries SET status='pending' WHERE rule_id=%s", (rule_id,))
    active, owner = repo.begin_rule_rescan(rule_id)
    with pytest.raises(RuntimeError, match="пересканируется"):
        repo.clear_rule_deliveries(rule_id)
    with pytest.raises(RuntimeError, match="уже выполняется"):
        _restart_from_snapshot(repo, rule_id)
    with pytest.raises(RuntimeError, match="изменились"):
        repo.rebuild_rule_keep_position(rule_id,owner_token=owner,
            allowed_message_ids=set(source_ids),include_newer_than_message_id=max(source_ids),
            before_message_id=None,expected_scope=(source,None,target,None,"video"))
    assert repo.get_delivery(deliveries[0])["status"]=="pending"
    repo.finish_rule_rescan(rule_id,was_active=active,owner_token=owner)


def test_keep_rebuilds_entire_orphan_album(repo):
    _, rule_id, deliveries, source_ids, source, target = seed(repo, 136,count=3)
    unrelated_source_id = 9001001  # collides numerically with an old delivery ID
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE delivery_attempts SET idempotency_key=%s,delivery_id=9001001,status='accepted',source_message_ids_json=%s::jsonb WHERE rule_id=%s",
                (f"rule:{rule_id}:target:{target}:media_group:group-136:album",str([unrelated_source_id]),rule_id))
            cur.execute("UPDATE deliveries SET status='pending' WHERE rule_id=%s", (rule_id,))
            cur.execute("INSERT INTO posts(message_id,source_channel,content_json,created_at) VALUES(%s,%s,'{}','2026-11-21') RETURNING id",(unrelated_source_id,source))
            post_id=cur.fetchone()["id"]
            cur.execute("INSERT INTO deliveries(rule_id,post_id,status,created_at,tenant_id) VALUES(%s,%s,'pending','2026-11-21',1) RETURNING id",(rule_id,post_id))
            unrelated_delivery_id=cur.fetchone()["id"]
    active, owner = repo.begin_rule_rescan(rule_id)
    assert repo.rebuild_rule_keep_position(rule_id,owner_token=owner,
        allowed_message_ids=set(source_ids)|{unrelated_source_id},include_newer_than_message_id=unrelated_source_id,
        before_message_id=None)==(4,0,0,0)
    assert all(repo.get_delivery(d) is None for d in deliveries+[unrelated_delivery_id])
    assert len(repo.get_rule_queue_logical_items(rule_id)) == 2
    repo.finish_rule_rescan(rule_id,was_active=active,owner_token=owner,queue_rebuilt=True)


def test_keep_does_not_block_on_unanchored_historical_album(repo):
    _, rule_id, _, _, _, target = seed(repo,137,count=2)
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE delivery_attempts SET idempotency_key=%s,delivery_id=9001002,status='accepted',source_message_ids_json='[9001002,9001003]'::jsonb WHERE rule_id=%s",
                (f"rule:{rule_id}:target:{target}:album_sources:9001002,9001003",rule_id))
            cur.execute("UPDATE deliveries SET status='pending' WHERE rule_id=%s", (rule_id,))
    active, owner = repo.begin_rule_rescan(rule_id)
    assert repo.rebuild_rule_keep_position(
        rule_id, owner_token=owner, allowed_message_ids={1001,1002},
        include_newer_than_message_id=1002, before_message_id=None,
    ) == (2,0,0,0)
    repo.finish_rule_rescan(rule_id,was_active=active,owner_token=owner,queue_rebuilt=True)


def test_keep_prune_archives_stale_album_without_preserving_deleted_posts(repo):
    _, rule_id, deliveries, source_ids, source, target = seed(repo,138,count=2)
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE delivery_attempts SET idempotency_key=%s,delivery_id=0,status='accepted',source_message_ids_json=%s::jsonb WHERE rule_id=%s",
                (f"rule:{rule_id}:target:{target}:media_group:group-138:album",str(deliveries),rule_id))
            cur.execute("UPDATE deliveries SET status='pending' WHERE rule_id=%s", (rule_id,))
    active, owner = repo.begin_rule_rescan(rule_id)
    # Both members are absent from the Telegram snapshot. Evidence still survives.
    assert repo.rebuild_rule_keep_position(rule_id,owner_token=owner,
        allowed_message_ids=set(),include_newer_than_message_id=max(source_ids),
        before_message_id=max(source_ids)+1)==(0,2,0,0)
    assert all(repo.get_delivery(d) is None for d in deliveries)
    repo.finish_rule_rescan(rule_id,was_active=active,owner_token=owner,queue_rebuilt=True)


def test_keep_removes_single_orphan_absent_from_snapshot(repo):
    _, rule_id, deliveries, source_ids, _, _ = seed(repo, uuid4().int % 10**12)
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """UPDATE delivery_attempts SET delivery_id=90000000001,status='accepted',
                   source_message_ids_json=jsonb_build_array(%s)
                   WHERE rule_id=%s""", (source_ids[0], rule_id),
            )
            cur.execute("UPDATE deliveries SET status='pending' WHERE rule_id=%s", (rule_id,))
    active, owner = repo.begin_rule_rescan(rule_id)
    _, removed_stale, _, quarantined = repo.rebuild_rule_keep_position(
        rule_id, owner_token=owner, allowed_message_ids=set(),
        include_newer_than_message_id=max(source_ids), before_message_id=None,
    )
    assert removed_stale == 1 and quarantined == 0
    assert repo.get_delivery(deliveries[0]) is None
    repo.finish_rule_rescan(rule_id, was_active=active, owner_token=owner, queue_rebuilt=True)


def test_keep_rebuilds_missing_album_member_even_if_anchor_exists(repo):
    _, rule_id, deliveries, source_ids, _, target = seed(repo, uuid4().int % 10**12, count=2)
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """UPDATE delivery_attempts SET status='accepted',
                   idempotency_key=%s WHERE rule_id=%s""",
                (f"rule:{rule_id}:target:{target}:media_group:group-{str(target)[7:]}:album", rule_id),
            )
            cur.execute("UPDATE deliveries SET status='faulty' WHERE id=%s", (deliveries[0],))
            cur.execute("DELETE FROM deliveries WHERE id=%s", (deliveries[1],))
    active, owner = repo.begin_rule_rescan(rule_id)
    _, _, _, quarantined = repo.rebuild_rule_keep_position(
        rule_id, owner_token=owner, allowed_message_ids=set(source_ids),
        include_newer_than_message_id=max(source_ids), before_message_id=None,
    )
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT d.status FROM deliveries d JOIN posts p ON p.id=d.post_id
                   WHERE d.rule_id=%s AND p.message_id=%s""", (rule_id, source_ids[1]),
            )
            row = cur.fetchone()
    assert row is not None and row["status"] == "pending"
    assert quarantined == 0
    repo.finish_rule_rescan(rule_id, was_active=active, owner_token=owner, queue_rebuilt=True)


def test_keep_prune_before_position_preserves_post_after_snapshot(repo):
    _, rule_id, deliveries, source_ids, source, _ = seed(repo, uuid4().int % 10**12)
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM delivery_attempts WHERE rule_id=%s", (rule_id,))
            cur.execute("UPDATE deliveries SET status='sent' WHERE rule_id=%s", (rule_id,))
            cur.execute(
                """INSERT INTO posts(message_id,source_channel,content_json,created_at)
                   VALUES(2001,%s,'{}','2026-11-21') RETURNING id""", (source,),
            )
            new_post_id = cur.fetchone()["id"]
    active, owner = repo.begin_rule_rescan(rule_id)
    repo.rebuild_rule_keep_position(
        rule_id, owner_token=owner, allowed_message_ids={2001},
        include_newer_than_message_id=max(source_ids), before_message_id=3000,
    )
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status FROM deliveries WHERE rule_id=%s AND post_id=%s",
                (rule_id, new_post_id),
            )
            row = cur.fetchone()
    assert row is not None and row["status"] == "pending"
    repo.finish_rule_rescan(rule_id, was_active=active, owner_token=owner, queue_rebuilt=True)


@pytest.mark.parametrize("extra_type", ["text", "jsonb"])
@pytest.mark.parametrize("self_loop", [False, True])
def test_rescan_snapshot_accepts_both_production_and_ci_audit_schemas(
    repo, extra_type, self_loop,
):
    """The real VM uses JSONB while the repository init DDL creates TEXT.

    A TEMP shadow table reproduces the exact production SQL type without
    changing the shared disposable CI database or masking other tests.
    The non-self-loop case must still parse correctly with zero selected rows.
    """
    _, rule_id, delivery_ids, source_ids, source, target = seed(
        repo, uuid4().int % 10**12,
    )
    mid = source_ids[0] + 41000
    album_ids = [mid + 1, mid + 2]
    with repo.connect() as conn:
        with conn.cursor() as cur:
            if self_loop:
                cur.execute("UPDATE routing SET target_id=source_id WHERE id=%s", (rule_id,))
            cur.execute(
                """UPDATE deliveries SET status='sent',sent_message_id=%s,
                   sent_message_ids_json=%s::jsonb,target_id_snapshot=%s WHERE id=%s""",
                (mid, json.dumps(album_ids), source if self_loop else target, delivery_ids[0]),
            )
        conn.commit()

    with repo.connect() as conn:
        with conn.cursor() as cur:
            # The session-local table shadows audit_log only for this call.
            cur.execute(
                "CREATE TEMP TABLE audit_log (LIKE public.audit_log INCLUDING ALL) ON COMMIT DROP"
            )
            if extra_type == "jsonb":
                cur.execute(
                    "ALTER TABLE pg_temp.audit_log ALTER COLUMN extra_json "
                    "TYPE jsonb USING extra_json::jsonb"
                )
            repo._snapshot_rescan_echo_ids(cur, rule_id)
            cur.execute(
                """SELECT pg_typeof(extra_json)::text AS stored_type,extra_json
                   FROM pg_temp.audit_log
                   WHERE event_type='rule_rescan_echo_snapshot' AND rule_id=%s""",
                (rule_id,),
            )
            rows = cur.fetchall()
            assert len(rows) == int(self_loop)
            if self_loop:
                assert rows[0]["stored_type"] == extra_type
                stored = rows[0]["extra_json"]
                payload = json.loads(stored) if isinstance(stored, str) else stored
                assert set(payload["sent_message_ids"]) == {mid, *album_ids}
        conn.commit()


def test_campaign_send_claim_and_intent_are_durable(repo):
    _, rule_id, _, _, _, target = seed(repo, uuid4().int % 10**12)
    run = repo.create_campaign_run(rule_id=rule_id,saved_post_id=123,run_type="manual",status="sending",show_seconds=60,targets_total=1,started_by=None)
    def placement(run_id):
        return repo.create_campaign_run_message(run_id=run_id,rule_id=rule_id,saved_post_id=123,
            target_kind="main",target_id=str(target),show_seconds=60,delete_after_at="2020-01-01T00:00:00+00:00")
    mid = placement(run)
    other_run = repo.create_campaign_run(rule_id=rule_id,saved_post_id=123,run_type="manual",status="sending",show_seconds=60,targets_total=1,started_by=None)
    other = placement(other_run)
    assert repo.mark_campaign_run_message_sending(mid)
    assert not repo.mark_campaign_run_message_sending(mid)
    assert not repo.mark_campaign_run_message_sending(other)
    ids = repo.get_or_create_campaign_mtproto_random_ids(str(mid),0,"identity",2)
    assert repo.get_or_create_campaign_mtproto_random_ids(str(mid),0,"identity",2)==ids
    assert repo.mark_campaign_run_message_failed(mid,error_text="unknown",accepted_unverified=True)
    assert not repo.mark_campaign_run_message_sending(other)
    assert not repo.mark_campaign_run_message_sent(mid,sent_message_id=1)


def test_campaign_receipt_exact_mapping_and_atomic_timers(repo):
    from datetime import datetime, timedelta, timezone
    _, rule_id, _, _, _, target = seed(repo, uuid4().int % 10**12)
    run = repo.create_campaign_run(rule_id=rule_id,saved_post_id=124,run_type="manual",
        status="sending",show_seconds=60,started_by=None,targets_total=1,
        top_time_enabled_snapshot=True,top_time_seconds_snapshot=7200)
    mid = repo.create_campaign_run_message(run_id=run,rule_id=rule_id,saved_post_id=124,
        target_kind="main",target_id=str(target),show_seconds=60,delete_after_at="2020-01-01T00:00:00+00:00")
    assert repo.mark_campaign_run_message_sending(mid)
    account = uuid4().int % 10**12 + 1
    now = datetime.now(timezone.utc)
    receipt = {"ids":[11,12],"mask":[True,False],"peer_id":int(target),"thread_id":None,
        "account_id":account,"started_at":(now-timedelta(minutes=2)).isoformat(),"method":"telethon_source"}
    assert repo.record_campaign_publication_receipt(mid,receipt)
    assert repo.mark_campaign_run_message_failed(mid,error_text="pending",accepted_unverified=True)
    assert not repo.mark_campaign_run_message_sent(mid,sent_message_id=11)
    repo.update_campaign_run_status(run,status="failed",targets_failed=1,finish=True)
    assert not repo.confirm_campaign_publication(mid,receipt=receipt,published_ids=[22,12],published_at=now)
    repo.record_telegram_publication_mappings(account,int(target),[(11,22)])
    assert not repo.confirm_campaign_publication(mid,receipt=receipt,published_ids=[22,99],published_at=now)
    assert repo.confirm_campaign_publication(mid,receipt=receipt,published_ids=[22,12],published_at=now)
    assert not repo.confirm_campaign_publication(mid,receipt=receipt,published_ids=[22,12],published_at=now)
    row=repo.get_campaign_run_message(mid)
    assert row["send_status"]=="sent" and row["sent_message_ids_json"]==[22,12]
    assert row["sent_at"]==now
    assert row["delete_after_at"]==now+timedelta(seconds=60)
    assert repo.get_campaign_run(run)["status"]=="sent"
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT starts_at,ends_at FROM campaign_top_time_pauses WHERE campaign_run_message_id=%s",(mid,))
            pause=cur.fetchone()
            assert pause and pause["starts_at"]==now and pause["ends_at"]==now+timedelta(seconds=7200)


def test_campaign_manual_review_keeps_old_placement_nonreplayable(repo):
    from datetime import datetime, timedelta, timezone
    _, rule_id, _, _, _, target=seed(repo,uuid4().int%10**12)
    run=repo.create_campaign_run(rule_id=rule_id,saved_post_id=125,run_type="manual",
        status="sending",show_seconds=60,started_by=None,targets_total=1)
    mid=repo.create_campaign_run_message(run_id=run,rule_id=rule_id,saved_post_id=125,
        target_kind="main",target_id=target,show_seconds=60)
    assert repo.mark_campaign_run_message_sending(mid)
    stamp=datetime.now(timezone.utc)-timedelta(minutes=20)
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE routing SET is_active=FALSE WHERE id=%s",(rule_id,))
            cur.execute("UPDATE campaign_run_messages SET updated_at=%s WHERE id=%s",(stamp,mid))
    assert not repo.acknowledge_interrupted_campaign_send(mid,expected_updated_at=stamp+timedelta(seconds=1),operator="test",note="inspected")
    assert repo.acknowledge_interrupted_campaign_send(mid,expected_updated_at=stamp,operator="test",note="inspected")
    assert repo.get_campaign_run_message(mid)["publication_manual_review"]["operator"]=="test"
    assert not repo.mark_campaign_run_message_sending(mid)


def test_inbox_incident_survives_new_connection_and_requires_explicit_review(repo):
    from datetime import timedelta
    account = uuid4().int % 10**12 + 1
    repo.record_publication_inbox_overflow(account,-100123,[7,8])
    other = PostgresRepository()
    try:
        incident = other.get_publication_inbox_incident(account)
        assert incident["last_scheduled_ids"] == [7,8]
        assert incident["acknowledged_at"] is None
        stamp = incident["blocked_at"]
        assert not other.acknowledge_publication_inbox_incident(account,expected_blocked_at=stamp-timedelta(seconds=1),note="reviewed")
        assert other.acknowledge_publication_inbox_incident(account,expected_blocked_at=stamp,note="reviewed")
        assert repo.get_publication_inbox_incident(account)["acknowledged_at"] is not None
        repo.record_publication_inbox_overflow(account,-100123,[9])
        assert other.get_publication_inbox_incident(account)["acknowledged_at"] is None
    finally:
        other.client.close_all_connections()


def test_campaign_scheduled_id_cannot_be_claimed_by_two_placements_or_delivery(repo):
    from datetime import datetime, timezone
    key, rule_id, delivery_ids, source_ids, source, target = seed(repo, uuid4().int % 10**12)
    account = uuid4().int % 10**12 + 1
    receipt = {"ids":[411],"mask":[True],"peer_id":int(target),"thread_id":None,
        "account_id":account,"started_at":datetime.now(timezone.utc).isoformat(),"method":"telethon_source"}
    def placement(post):
        run = repo.create_campaign_run(rule_id=rule_id,saved_post_id=post,run_type="manual",
            status="sending",show_seconds=60,started_by=None,targets_total=1)
        mid = repo.create_campaign_run_message(run_id=run,rule_id=rule_id,saved_post_id=post,
            target_kind="main",target_id=target,show_seconds=60)
        assert repo.mark_campaign_run_message_sending(mid)
        return mid
    first, second = placement(901), placement(902)
    assert repo.record_campaign_publication_receipt(first,receipt)
    assert not repo.record_campaign_publication_receipt(second,receipt)
    assert not repo.register_publication_receipt(key=key,delivery_ids=delivery_ids,rule_id=rule_id,
        account_id=account,peer_id=int(target),thread_id=None,source_channel=source,
        source_message_ids=source_ids,scheduled_ids=[411])
    assert repo.register_publication_receipt(key=key,delivery_ids=delivery_ids,rule_id=rule_id,
        account_id=account,peer_id=int(target),thread_id=None,source_channel=source,
        source_message_ids=source_ids,scheduled_ids=[412])
    assert not repo.record_campaign_publication_receipt(second,{**receipt,"ids":[412]})
    assert repo.get_campaign_run_message(second)["publication_receipt"] is None


@pytest.mark.parametrize("initial_status", ["pending", "sending", "sent", "failed"])
def test_campaign_definite_pre_rpc_failure_is_atomic_and_does_not_touch_claims(repo, initial_status):
    _, rule_id, _, _, _, target = seed(repo, uuid4().int % 10**12)
    run = repo.create_campaign_run(
        rule_id=rule_id, saved_post_id=991, run_type="manual", status="sending",
        show_seconds=60, targets_total=1, started_by=None,
    )
    mid = repo.create_campaign_run_message(
        run_id=run, rule_id=rule_id, saved_post_id=991, target_kind="main",
        target_id=f"@missing_{initial_status}_{uuid4().hex[:10]}",
        show_seconds=60, delete_after_at="2026-12-01T00:00:00+00:00",
    )
    assert mid is not None
    if initial_status != "pending":
        assert repo.mark_campaign_run_message_sending(mid)
        if initial_status == "sent":
            assert repo.mark_campaign_run_message_sent(
                mid, sent_message_id=10001, render_mode="bot_api",
            )
        elif initial_status == "failed":
            assert repo.mark_campaign_run_message_failed(
                mid, error_text="prior failure", render_mode="bot_api",
            )
    before = repo.get_campaign_run_message(mid)
    changed = repo.mark_campaign_run_message_failed_before_send(
        mid, error_text="recipient resolution rejected",
    )
    assert changed is (initial_status == "pending")
    after = repo.get_campaign_run_message(mid)
    if initial_status == "pending":
        assert after["send_status"] == "failed"
        assert after["render_mode"] == "target_resolution_failed"
        assert after["send_error_text"] == "recipient resolution rejected"
        assert after["delete_status"] == "none"
        assert after["delete_after_at"] is None
        assert after["sent_message_id"] is None
    else:
        assert after["send_status"] == before["send_status"]
        assert after["render_mode"] == before["render_mode"]
        assert after["send_error_text"] == before["send_error_text"]
        assert after["delete_after_at"] == before["delete_after_at"]
        assert after["sent_message_id"] == before["sent_message_id"]
    assert after["publication_review_required"] == before["publication_review_required"]
    # The result must be committed, not only visible within the current connection.
    reader = PostgresRepository()
    try:
        assert reader.get_campaign_run_message(mid)["send_status"] == after["send_status"]
    finally:
        reader.client.close_all_connections()



def test_campaign_cancel_before_send_and_delete_after_send_are_disjoint(repo):
    _, rule_id, _, _, _, target = seed(repo, uuid4().int % 10**12)
    def placement(saved_id):
        run = repo.create_campaign_run(rule_id=rule_id, saved_post_id=saved_id,
            run_type="manual", status="sending", show_seconds=60, targets_total=1, started_by=None)
        return repo.create_campaign_run_message(run_id=run, rule_id=rule_id, saved_post_id=saved_id,
            target_kind="main", target_id=str(target), show_seconds=60)
    cancelled = placement(901)
    assert not repo.mark_campaign_run_message_deleted(cancelled)
    assert repo.mark_campaign_run_message_deleted(cancelled, expected_sent=False)
    assert not repo.mark_campaign_run_message_sending(cancelled)
    row = repo.get_campaign_run_message(cancelled)
    assert row["send_status"] == "failed" and row["delete_status"] == "deleted"
    sent = placement(902)
    assert repo.mark_campaign_run_message_sending(sent)
    assert not repo.mark_campaign_run_message_deleted(sent, expected_sent=False)
    assert repo.mark_campaign_run_message_sent(sent, sent_message_id=4321)
    assert not repo.mark_campaign_run_message_deleted(sent, expected_sent=False)
    assert repo.mark_campaign_run_message_deleted(sent)
    unresolved = placement(903)
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE campaign_run_messages SET send_status='failed', render_mode='copy_album_unverified' WHERE id=%s", (unresolved,))
    assert not repo.mark_campaign_run_message_deleted(unresolved, expected_sent=False)



def test_stale_album_release_retires_old_job_before_new_group_claim(repo):
    from app.delivery_idempotency import build_delivery_idempotency_key

    suffix = uuid4().int % 10**12
    seed_key, rule_id, ids, _, source, target = seed(repo, suffix, count=3)
    assert repo.mark_delivery_attempt_failed(seed_key, status="failed_before_send", error_text="test setup")
    old_ids = ids[:2]
    payload = {"rule_id": rule_id, "delivery_id": old_ids[0], "delivery_ids": old_ids}
    job_id = repo.create_job("repost_album", payload, "light")
    key = build_delivery_idempotency_key(operation_kind="album", rule_id=rule_id,
        target_id=target, media_group_id=f"group-{suffix}", source_message_ids=old_ids)
    assert repo.create_delivery_attempt(delivery_id=old_ids[0], rule_id=rule_id,
        tenant_id=1, job_id=job_id, idempotency_key=key, operation_kind="album",
        status="created", target_id=target, source_message_ids=old_ids)
    assert repo.mark_delivery_attempt_sending(key)
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE jobs SET status='processing',locked_by='current' WHERE id=%s", (job_id,))
            cur.execute("UPDATE deliveries SET status='pending' WHERE id=%s", (ids[2],))
            cur.execute("UPDATE routing SET is_active=TRUE, mode='repost', next_run_at=NULL WHERE id=%s", (rule_id,))
    def release(worker, members):
        return repo.release_stale_album_job_members(job_id=job_id, worker_id=worker,
            rule_id=rule_id, delivery_ids=members, idempotency_key=key)
    assert release("current", old_ids) is None  # sending is not safe to release
    assert repo.mark_delivery_attempt_failed(key, status="failed_before_send", error_text="membership changed")
    assert release("stale", old_ids) is None
    assert release("current", ids) is None
    assert [repo.get_delivery(i)["status"] for i in ids] == ["processing", "processing", "pending"]
    assert release("current", old_ids) == 2
    assert [repo.get_delivery(i)["status"] for i in ids] == ["pending"] * 3
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT status, locked_by FROM jobs WHERE id=%s", (job_id,))
            assert cur.fetchone() == {"status": "failed", "locked_by": None}
    new_job = repo.take_due_delivery_and_create_job(rule_id, "2027-01-01T00:00:00+00:00")
    assert new_job["status"] == "created" and new_job["job_id"] != job_id
    assert [repo.get_delivery(i)["status"] for i in ids] == ["processing"] * 3
    assert release("current", old_ids) is None
    assert [repo.get_delivery(i)["status"] for i in ids] == ["processing"] * 3


@pytest.mark.parametrize("job_type", ["video_download", "video_process", "video_delivery", "video_send", "repost_single", "repost_album"])
def test_previously_quarantined_echo_still_fences_retried_job(repo, job_type):
    suffix = uuid4().int % 10**12
    _, rule_id, ids, _, source, _ = seed(repo, suffix, count=2)
    echo_id, sibling = ids
    members = ids if job_type == "repost_album" else [echo_id]
    payload = {"rule_id": rule_id, "delivery_id": echo_id, "delivery_ids": members}
    job_id = repo.create_job(job_type, payload, "light")
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE routing SET target_id=%s WHERE id=%s", (source, rule_id))
            cur.execute("UPDATE deliveries SET status='faulty',error_text='Self-loop: confirmed publication echo' WHERE id=%s", (echo_id,))
            cur.execute("UPDATE jobs SET status='processing',locked_by='retried' WHERE id=%s", (job_id,))
    def guard():
        return repo.quarantine_queued_self_target_echo_deliveries(
            rule_id, members, job_id=job_id, worker_id="retried")
    # A crash after the original quarantine must not make the echo eligible.
    assert repo.quarantine_queued_self_target_echo_deliveries(
        rule_id, members, job_id=job_id, worker_id="stale") is None
    assert guard() == [echo_id]
    assert guard() == [echo_id]
    assert repo.get_delivery(echo_id)["status"] == "faulty"
    # Previously released unrelated album members may now be owned by a new job.
    assert repo.get_delivery(sibling)["status"] == "processing"


def test_same_difference_gap_preserves_acknowledgement(repo):
    account = uuid4().int % 10**12 + 1
    repo.record_publication_inbox_overflow(account, 0, [42])
    incident = repo.get_publication_inbox_incident(account)
    assert repo.acknowledge_publication_inbox_incident(account, expected_blocked_at=incident["blocked_at"], note="reviewed")
    repo.record_publication_inbox_overflow(account, 0, [42])
    same = repo.get_publication_inbox_incident(account)
    assert same["blocked_at"] == incident["blocked_at"] and same["acknowledged_at"] is not None
    repo.record_publication_inbox_overflow(account, 0, [43])
    assert repo.get_publication_inbox_incident(account)["acknowledged_at"] is None


def test_restart_rule_94_legacy_240_preserves_queue_and_history(repo):
    key, rule_id, deliveries, _, source, target = seed(repo, uuid4().int % 10**12, count=5)
    categories = [
        ("accepted", "album", "reupload_album", 98),
        ("sending", "album", "copy_album", 87),
        ("sending", "single", "copy_single", 27),
        ("accepted", "single", "reupload_single", 21),
        ("accepted", "single", "reupload_single", 4),
        ("failed_after_send", "album", "copy_album", 3),
    ]
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM delivery_attempts WHERE idempotency_key=%s", (key,))
            cur.execute("UPDATE routing SET is_active=TRUE WHERE id=%s", (rule_id,))
            cur.execute("UPDATE deliveries SET status='pending' WHERE rule_id=%s", (rule_id,))
            cur.execute("UPDATE deliveries SET status='sent',sent_message_id=900 WHERE id=%s", (deliveries[0],))
            cur.execute("UPDATE deliveries SET status='faulty' WHERE id=%s", (deliveries[1],))
            for index, (status, kind, method, count) in enumerate(categories):
                cur.execute(
                    """INSERT INTO delivery_attempts(
                           delivery_id,rule_id,tenant_id,idempotency_key,operation_kind,
                           status,telegram_method,target_id,created_at,updated_at)
                       SELECT %s,%s,1,%s||':'||n::text,%s,%s,%s,%s,
                              TIMESTAMPTZ '2026-05-03',TIMESTAMPTZ '2026-11-20'
                       FROM generate_series(1,%s) n""",
                    (deliveries[0] if index==4 else -1, rule_id,
                     f"legacy:{rule_id}:{index}", kind, status, method, target, count),
                )
            cur.execute("SELECT post_id FROM deliveries WHERE rule_id=%s ORDER BY post_id", (rule_id,))
            old_posts = cur.fetchall()
    assert _restart_from_snapshot(repo, rule_id) == (240, 5, 5)
    assert _restart_from_snapshot(repo, rule_id) == (0, 5, 5)
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT post_id FROM deliveries WHERE rule_id=%s ORDER BY post_id", (rule_id,))
            assert cur.fetchall() == old_posts
            cur.execute("SELECT id,status,sent_message_id FROM deliveries WHERE rule_id=%s", (rule_id,))
            rows = cur.fetchall()
            assert len(rows)==5 and all(r["status"]=="pending" and r["id"] not in deliveries
                                        and r["sent_message_id"] is None for r in rows)
            cur.execute("""SELECT status,COUNT(*) AS n FROM delivery_attempts
                           WHERE rule_id=%s AND idempotency_key LIKE %s GROUP BY status""",
                        (rule_id, "%:rescan:%"))
            assert {r["status"]:r["n"] for r in cur.fetchall()} == {
                "accepted":123, "sending":114, "failed_after_send":3,
            }
            cur.execute("SELECT COUNT(*) AS n FROM posts WHERE source_channel=%s", (source,))
            assert cur.fetchone()["n"] == 5
    assert repo.get_rule(rule_id).is_active


def test_restart_from_full_snapshot_includes_new_excludes_deleted_and_keeps_live_arrival(repo):
    key, rule_id, deliveries, source_ids, source, target = seed(
        repo, uuid4().int % 10**12, count=3,
    )
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM delivery_attempts WHERE idempotency_key=%s", (key,))
            cur.execute("UPDATE routing SET is_active=TRUE WHERE id=%s", (rule_id,))
            cur.execute("UPDATE deliveries SET status='sent' WHERE rule_id=%s", (rule_id,))
    was_active, owner = repo.begin_rule_rescan(rule_id, fresh=True)
    high_water = repo.get_rule_scan_start_max_message_id(rule_id)
    new_ids = [min(source_ids) - 1, high_water + 1, high_water + 2, high_water + 3]
    try:
        # A cached message is absent, one newly observed ID lies below the
        # watermark, and one cached live candidate was deleted remotely.
        repo.save_post_batch(
            [(mid, source, None, {"media_kind": "video"}, None) for mid in new_ids],
            preserve_existing_deliveries=True,
        )
        observed = set(source_ids[1:] + new_ids[:3])
        assert repo.get_rule_rescan_live_message_ids(rule_id, high_water) == set(new_ids[1:])
        with pytest.raises(RuntimeError, match="появились новые записи"):
            repo.restart_rule_queue(
                rule_id, owner_token=owner, allowed_message_ids=observed,
                include_newer_than_message_id=high_water,
            )
        _, _, rebuilt = repo.restart_rule_queue(
            rule_id, owner_token=owner, allowed_message_ids=observed,
            include_newer_than_message_id=high_water,
            checked_missing_message_ids={new_ids[-1]},
        )
        assert rebuilt == 5
        with repo.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("""SELECT p.message_id,d.status FROM deliveries d
                               JOIN posts p ON p.id=d.post_id WHERE d.rule_id=%s""",
                            (rule_id,))
                assert {r["message_id"]:r["status"] for r in cur.fetchall()} == {
                    mid:"pending" for mid in source_ids[1:] + new_ids[:3]
                }
                cur.execute("SELECT COUNT(*) AS n FROM posts WHERE source_channel=%s", (source,))
                assert cur.fetchone()["n"] == 7
    finally:
        repo.finish_rule_rescan(rule_id, was_active=was_active, owner_token=owner)


@pytest.mark.parametrize("status", ["sending", "accepted", "failed_after_send"])
@pytest.mark.parametrize("old_created", [True, False])
def test_restart_archives_modern_bot_api_attempt_without_intent(repo, status, old_created):
    key, rule_id, deliveries, _, _, _ = seed(repo, uuid4().int % 10**12)
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE deliveries SET status='faulty' WHERE rule_id=%s", (rule_id,))
            cur.execute(
                """UPDATE delivery_attempts SET status=%s,telegram_method='copy_single',
                   created_at=CASE WHEN %s THEN TIMESTAMPTZ '2026-11-21'
                                   ELSE TIMESTAMPTZ '2026-11-22 17:56:18+00' END,
                   updated_at=TIMESTAMPTZ '2026-11-22 17:56:18+00'
                   WHERE idempotency_key=%s""",
                (status, old_created, key),
            )
    attempt = repo.get_delivery_attempt_by_idempotency_key(key)
    assert _restart_from_snapshot(repo, rule_id) == (1,1,1)
    archived = repo.get_delivery_attempt_by_idempotency_key(f"{key}:rescan:{attempt['id']}")
    assert archived["status"] == status
    assert repo.get_delivery(deliveries[0]) is None
    assert len(repo.get_rule_queue_logical_items(rule_id)) == 1
    assert repo.get_rule(rule_id).is_active


def test_restart_scheduler_and_logical_queue_follow_telegram_message_ids(repo):
    key, rule_id, deliveries, source_ids, source, _ = seed(repo, uuid4().int % 10**12)
    high = source_ids[0]
    low = high - 1
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM delivery_attempts WHERE idempotency_key=%s", (key,))
            cur.execute("UPDATE deliveries SET status='pending' WHERE rule_id=%s", (rule_id,))
    was_active, owner = repo.begin_rule_rescan(rule_id, fresh=True)
    try:
        repo.save_post_batch(
            [(low, source, None, {"media_kind": "text"}, None)],
            preserve_existing_deliveries=True,
        )
        assert repo.restart_rule_queue(
            rule_id, owner_token=owner, allowed_message_ids={low, high},
            include_newer_than_message_id=high,
        )[2] == 2
    finally:
        repo.finish_rule_rescan(rule_id, was_active=was_active, owner_token=owner,
                                queue_rebuilt=True)
    assert repo.get_rule_first_pending_message_id(rule_id) == low
    assert [item["first_message_id"] for item in repo.get_rule_queue_logical_items(rule_id)] == [low, high]
    taken = repo.take_due_delivery_and_create_job(rule_id, "9999-01-01T00:00:00+00:00")
    assert taken and taken["status"] == "created"
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT payload_json FROM jobs WHERE id=%s", (taken["job_id"],))
            assert int(cur.fetchone()["payload_json"]["message_id"]) == low
            # Leave no runnable test job in the shared disposable database.
            cur.execute("DELETE FROM jobs WHERE id=%s", (taken["job_id"],))
            cur.execute("UPDATE deliveries SET status='pending' WHERE id=%s", (taken["delivery_id"],))


def test_rescan_release_repeated_after_lost_commit_response(repo):
    key, rule_id, _, _, _, _ = seed(repo, uuid4().int % 10**12)
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM delivery_attempts WHERE idempotency_key=%s", (key,))
            cur.execute("UPDATE deliveries SET status='pending' WHERE rule_id=%s", (rule_id,))
    active, owner = repo.begin_rule_rescan(rule_id)
    repo.finish_rule_rescan(rule_id, was_active=active, owner_token=owner, queue_rebuilt=True)
    repo.finish_rule_rescan(rule_id, was_active=active, owner_token=owner, queue_rebuilt=True)
    active2, owner2 = repo.begin_rule_rescan(rule_id)
    try:
        with pytest.raises(RuntimeError, match="Владелец"):
            repo.finish_rule_rescan(rule_id, was_active=active, owner_token=owner)
    finally:
        repo.finish_rule_rescan(rule_id, was_active=active2, owner_token=owner2)


def test_restart_rolls_back_if_unchecked_arrival_commits_between_guard_and_delete(repo, monkeypatch):
    key, rule_id, deliveries, source_ids, source, _ = seed(repo, uuid4().int % 10**12)
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM delivery_attempts WHERE idempotency_key=%s", (key,))
            cur.execute("UPDATE deliveries SET status='sent' WHERE rule_id=%s", (rule_id,))
    watermark = max(source_ids)
    live_id = watermark + 1
    active, owner = repo.begin_rule_rescan(rule_id, fresh=True)
    guard = repo._assert_rescan_live_snapshot
    checks = 0

    def ingest():
        try:
            repo.save_post(live_id, source, None, {"media_kind": "text"})
        finally:
            repo.client.close_thread_connection()

    def guard_with_arrival(*args):
        nonlocal checks
        guard(*args)
        checks += 1
        if checks == 1:
            # Independent committed transaction after the first guard.
            with ThreadPoolExecutor(max_workers=1) as pool:
                pool.submit(ingest).result(timeout=10)

    monkeypatch.setattr(repo, "_assert_rescan_live_snapshot", guard_with_arrival)
    try:
        with pytest.raises(RuntimeError, match="появились новые записи"):
            repo.restart_rule_queue(
                rule_id, owner_token=owner, allowed_message_ids=set(source_ids),
                include_newer_than_message_id=watermark,
            )
        assert repo.get_delivery(deliveries[0])["status"] == "sent"
        with repo.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("""SELECT d.status FROM deliveries d JOIN posts p ON p.id=d.post_id
                               WHERE d.rule_id=%s AND p.message_id=%s""", (rule_id, live_id))
                assert cur.fetchone()["status"] == "pending"
    finally:
        repo.finish_rule_rescan(rule_id, was_active=active, owner_token=owner)

    
@pytest.mark.parametrize("already_quarantined", [False,True])
def test_keep_rebuild_rule_94_resets_historical_failures_and_preserves_position(repo, already_quarantined):
    key, rule_id, _, _, source, target = seed(repo, uuid4().int % 10**12)
    observed = set()
    album_groups = []
    singles = []
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM delivery_attempts WHERE idempotency_key=%s", (key,))
            cur.execute("DELETE FROM deliveries WHERE rule_id=%s", (rule_id,))
            cur.execute("DELETE FROM posts WHERE source_channel=%s", (source,))
            mid = 1000
            # 47 completed = 131 rows; 185 remaining = 394 rows.
            # 133 remaining albums + 52 single posts match the 240 historical attempts.
            sizes = [3]*42 + [1]*5 + [3]*76 + [2]*57 + [1]*52
            faulty_positions = {47,123,124,125}
            for pos, size in enumerate(sizes):
                group = f"group-{rule_id}-{pos}" if size>1 else None
                if pos>=47 and group:
                    album_groups.append(group)
                status = "sent" if pos<47 else (
                    "faulty" if already_quarantined or pos in faulty_positions else "pending"
                )
                for _ in range(size):
                    mid += 1
                    observed.add(mid)
                    if pos>=47 and not group:
                        singles.append(mid)
                    cur.execute(
                        """INSERT INTO posts(message_id,source_channel,content_json,media_group_id,created_at)
                           VALUES (%s,%s,'{"media_kind":"video"}',%s,'2026-01-01') RETURNING id""",
                        (mid,source,group),
                    )
                    post_id=cur.fetchone()["id"]
                    cur.execute(
                        """INSERT INTO deliveries(rule_id,post_id,status,created_at,tenant_id,error_text)
                           VALUES (%s,%s,%s,'2026-01-01',1,%s)""",
                        (rule_id,post_id,status,
                         "historical_attempt_unverified_no_resend" if status=="faulty" else None),
                    )
            album_statuses = ["accepted"]*98 + ["sending"]*87 + ["failed_after_send"]*3
            single_statuses = ["sending"]*27 + ["accepted"]*25
            for n,status in enumerate(album_statuses):
                group=album_groups[n%len(album_groups)]
                old_key=f"rule:{rule_id}:target:{target}:media_group:{group}:album"
                if n>=len(album_groups):
                    old_key+=f":members:historical-{n}"
                cur.execute(
                    """INSERT INTO delivery_attempts(delivery_id,rule_id,tenant_id,idempotency_key,
                           operation_kind,status,target_id,source_message_ids_json,created_at,updated_at)
                       VALUES(-1,%s,1,%s,'album',%s,%s,'[900001]'::jsonb,
                              TIMESTAMPTZ '2026-05-03',NOW())""",
                    (rule_id,old_key,status,target),
                )
            for n,(mid,status) in enumerate(zip(singles,single_statuses)):
                cur.execute(
                    """INSERT INTO delivery_attempts(delivery_id,rule_id,tenant_id,idempotency_key,
                           operation_kind,status,target_id,source_message_ids_json,created_at,updated_at)
                       VALUES(-1,%s,1,%s,'single',%s,%s,jsonb_build_array(%s::bigint),
                              TIMESTAMPTZ '2026-05-03',NOW())""",
                    (rule_id,f"delivery:old-{rule_id}-{n}:target:{target}:single",status,target,mid),
                )
    before=repo.get_rule_card_snapshot(rule_id)
    assert before["logical_total"]==232
    assert before["logical_current_position"]==48
    assert before["logical_pending"]==(0 if already_quarantined else 181)
    assert before["logical_faulty"]==(185 if already_quarantined else 4)
    was_active, owner = repo.begin_rule_rescan(rule_id)
    anchor=repo.get_rule_first_pending_message_id(rule_id)
    assert anchor==1132
    assert repo.rebuild_rule_keep_position(
        rule_id, owner_token=owner, allowed_message_ids=observed,
        include_newer_than_message_id=max(observed), before_message_id=anchor,
    ) == (394,0,0,0)
    repo.finish_rule_rescan(rule_id,was_active=was_active,owner_token=owner,queue_rebuilt=True)
    assert repo.get_rule_position_info(rule_id)=={
        "total":232,"completed":47,"current_position":48,
    }
    after=repo.get_rule_card_snapshot(rule_id)
    assert after["logical_pending"]==185 and after["logical_faulty"]==0
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT status,COUNT(*) AS n FROM deliveries WHERE rule_id=%s GROUP BY status",(rule_id,))
            assert {r["status"]:r["n"] for r in cur.fetchall()}=={"sent":131,"pending":394}
            cur.execute("SELECT status,COUNT(*) AS n FROM delivery_attempts WHERE rule_id=%s AND idempotency_key LIKE %s GROUP BY status",(rule_id,"%:rescan:%"))
            assert {r["status"]:r["n"] for r in cur.fetchall()}=={
                "accepted":123,"sending":114,"failed_after_send":3,
            }
            cur.execute("SELECT COUNT(*) AS n FROM audit_log WHERE rule_id=%s AND event_type='rule_rescan_delivery_archived'",(rule_id,))
            assert cur.fetchone()["n"]==394


@pytest.mark.parametrize("fresh", [True, False])
def test_rescan_revokes_active_delivery_jobs_and_rebuilds_while_worker_is_live(repo, fresh):
    import asyncio
    from app.delivery_job_fence import (
        DeliveryJobRevoked, enter_delivery_job, leave_delivery_job,
    )
    key, rule_id, delivery_ids, source_ids, source, target = seed(
        repo, uuid4().int % 10**12,
    )
    delivery_id = delivery_ids[0]
    payload = {"rule_id":rule_id,"delivery_id":delivery_id}
    job_id = repo.create_job("repost_single",payload,"light")
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE routing SET is_active=TRUE WHERE id=%s", (rule_id,))
            cur.execute("UPDATE jobs SET status='processing',locked_by='stale' WHERE id=%s", (job_id,))
    async def run():
        # A second context models the user request while the old worker remains live.
        ready = asyncio.Event()
        resume = asyncio.Event()
        async def old_worker():
            token = enter_delivery_job(repo, "repost_single", rule_id, job_id, "stale")
            try:
                await asyncio.to_thread(repo.assert_current_delivery_job)
                ready.set()
                await resume.wait()
                for mutation in (
                    lambda: repo.assert_current_delivery_job(),
                    lambda: repo.touch_rule_after_send(rule_id, 300),
                    lambda: repo.mark_delivery_sent(delivery_id),
                    lambda: repo.create_job("video_send",payload,"heavy"),
                    lambda: repo.mark_delivery_attempt_accepted(key,sent_message_ids=[777]),
                ):
                    with pytest.raises(DeliveryJobRevoked):
                        await asyncio.to_thread(mutation)
            finally:
                leave_delivery_job(token)
        task = asyncio.create_task(old_worker())
        await ready.wait()
        try:
            active, owner = await asyncio.to_thread(repo.begin_rule_rescan, rule_id, fresh=fresh)
            assert not repo.heartbeat_job_processing(job_id,"stale")
            assert not repo.retry_job(job_id,"late",1)
            if fresh:
                assert repo.restart_rule_queue(
                    rule_id,owner_token=owner,allowed_message_ids=set(source_ids),
                    include_newer_than_message_id=max(source_ids),
                )[2] == 1
            else:
                assert repo.rebuild_rule_keep_position(
                    rule_id,owner_token=owner,allowed_message_ids=set(source_ids),
                    include_newer_than_message_id=max(source_ids),before_message_id=source_ids[0],
                ) == (1,0,0,0)
            repo.finish_rule_rescan(rule_id,was_active=active,owner_token=owner,queue_rebuilt=True)
        finally:
            resume.set()
            await task
    asyncio.run(run())
    assert repo.get_delivery(delivery_id) is None
    assert repo.get_job(job_id)["status"] == "failed"
    assert len(repo.get_rule_queue_logical_items(rule_id)) == 1


def test_rescan_archived_unresolved_receipt_does_not_block_manual_rollback(repo):
    seeded = seed(repo, uuid4().int % 10**12)
    key, rule_id, delivery_ids, source_ids, _, _ = seeded
    assert register(repo,seeded,[75001],[True])
    assert _restart_from_snapshot(repo, rule_id)[2] == 1
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM deliveries WHERE rule_id=%s",(rule_id,))
            new_id=cur.fetchone()["id"]
            cur.execute("UPDATE deliveries SET status='sent',sent_at='2026-11-26T12:00:00+00:00' WHERE id=%s",(new_id,))
            cur.execute("SELECT idempotency_key,expired_at FROM telegram_publication_receipts WHERE rule_id=%s",(rule_id,))
            archived=cur.fetchone()
            assert ":rescan:" in archived["idempotency_key"] and archived["expired_at"] is not None
    assert new_id not in delivery_ids
    assert repo.rollback_last_delivery(rule_id) is not None
    assert repo.get_delivery(new_id)["status"] == "pending"
    assert repo.get_publication_receipt(archived["idempotency_key"]) is not None


def test_keep_position_rebuilds_partial_album_and_removes_deleted_sent_member(repo):
    key, rule_id, deliveries, source_ids, _, _ = seed(repo, uuid4().int % 10**12, count=3)
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE deliveries SET status='sent' WHERE id=ANY(%s)",(deliveries[:2],))
    active, owner = repo.begin_rule_rescan(rule_id)
    assert repo.rebuild_rule_keep_position(
        rule_id,owner_token=owner,allowed_message_ids=set(source_ids[1:]),
        include_newer_than_message_id=max(source_ids),before_message_id=source_ids[0],
    ) == (2,1,0,0)
    repo.finish_rule_rescan(rule_id,was_active=active,owner_token=owner,queue_rebuilt=True)
    items = repo.get_rule_queue_logical_items(rule_id)
    assert len(items)==1 and items[0]["message_ids"] == source_ids[1:]
    assert items[0]["pending_count"]==2
    assert all(repo.get_delivery(d) is None for d in deliveries)


def test_rescan_cancellation_is_rule_and_job_type_scoped(repo):
    _, rid, delivery_ids, source_ids, _, _ = seed(repo, uuid4().int % 10**12)
    _, other, other_ids, _, _, _ = seed(repo, uuid4().int % 10**12)
    delivery_job = repo.create_job("repost_single",{"rule_id":rid,"delivery_id":delivery_ids[0]},"light")
    cleanup = repo.create_job("repost_campaign_delete_copy",{"rule_id":rid,"copy_id":1},"light")
    unrelated = repo.create_job("repost_single",{"rule_id":other,"delivery_id":other_ids[0]},"light")
    active, owner = repo.begin_rule_rescan(rid)
    assert repo.restart_rule_queue(
        rid,owner_token=owner,allowed_message_ids=set(source_ids),
        include_newer_than_message_id=max(source_ids),
    )[2] == 1
    repo.finish_rule_rescan(rid,was_active=active,owner_token=owner,queue_rebuilt=True)
    assert repo.get_job(delivery_job)["status"] == "failed"
    assert repo.get_job(cleanup)["status"] == "pending"
    assert repo.get_job(unrelated)["status"] == "pending"
    assert repo.get_delivery(other_ids[0])["status"] == "processing"


def test_keep_position_does_not_rewind_to_old_post_inserted_by_parser(repo):
    _, rid, delivery_ids, source_ids, source, _ = seed(repo, uuid4().int % 10**12)
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE deliveries SET status='pending' WHERE rule_id=%s",(rid,))
    active, owner=repo.begin_rule_rescan(rid)
    anchor=repo.get_rule_first_pending_message_id(rid)
    watermark=repo.get_rule_scan_start_max_message_id(rid)
    earlier=anchor-1
    # This parser API creates a pending delivery for a newly inserted post.
    repo.save_post_batch([(earlier,source,None,{"media_kind":"video"},None)],
                         preserve_existing_deliveries=True)
    assert repo.rebuild_rule_keep_position(
        rid,owner_token=owner,allowed_message_ids={earlier,*source_ids},
        include_newer_than_message_id=watermark,before_message_id=anchor,
    ) == (1,0,1,0)
    repo.finish_rule_rescan(rid,was_active=active,owner_token=owner,queue_rebuilt=True)
    assert [item["first_message_id"] for item in repo.get_rule_queue_logical_items(rid)]==source_ids


@pytest.mark.parametrize("extra_type", ["text", "jsonb"])
def test_rescan_delivery_archive_supports_both_audit_column_types(repo, extra_type):
    _, rid, deliveries, source_ids, _, _ = seed(repo, uuid4().int % 10**12)
    active, owner=repo.begin_rule_rescan(rid,fresh=True)
    try:
        with repo.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("CREATE TEMP TABLE audit_log (LIKE public.audit_log INCLUDING ALL) ON COMMIT PRESERVE ROWS")
                if extra_type=="jsonb":
                    cur.execute("ALTER TABLE pg_temp.audit_log ALTER COLUMN extra_json TYPE jsonb USING extra_json::jsonb")
        assert repo.restart_rule_queue(
            rid,owner_token=owner,allowed_message_ids=set(source_ids),
            include_newer_than_message_id=max(source_ids),
        )[2]==1
        with repo.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("""SELECT extra_json,pg_typeof(extra_json)::text AS stored_type
                               FROM pg_temp.audit_log WHERE rule_id=%s AND event_type='rule_rescan_delivery_archived'""",(rid,))
                archived=cur.fetchone()
                assert archived["stored_type"]==extra_type
                extra=archived["extra_json"]
                payload=json.loads(extra) if isinstance(extra,str) else extra
                assert payload["cycle"]==owner and payload["delivery"]["id"]==deliveries[0]
    finally:
        with repo.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("DROP TABLE IF EXISTS pg_temp.audit_log")
        repo.finish_rule_rescan(rid,was_active=active,owner_token=owner,queue_rebuilt=True)


@pytest.mark.skipif(os.getenv("VIMI_TEST_POSTGRES") != "1", reason="requires disposable PostgreSQL")
def test_emoji_probe_quota_and_fence_across_process_connections():
    assert os.getenv("APP_PG_DB") == "vimi_publication_ci"
    repo = PostgresRepository()
    account = 8745123
    chat = -1008745123
    root = f"video-emoji:{account}"
    try:
        repo.init()

        def reserve(user):
            other = PostgresRepository()
            try:
                return other.reserve_video_emoji_check(account, user, chat)
            finally:
                other.close_all_connections()

        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(reserve, [10, 10, 11, 12]))
        assert results.count("reserved") == 1
        assert results.count("pending_review") == 3
        # Unknown result remains fenced for a fresh repository connection.
        assert reserve(13) == "pending_review"
        repo.release_video_emoji_check(account)
        assert reserve(13) == "quota"
        with repo.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE telegram_transport_cooldowns SET blocked_until=NOW()-interval '1 second' WHERE scope_key=%s", (f"{root}:quota",))
        winner = [10, 10, 11, 12][results.index("reserved")]
        assert reserve(winner) == "quota"
        repo.extend_transport_cooldown(f"mtproto:{account}", 60)
        assert reserve(13) == "flood_wait"
    finally:
        with repo.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM telegram_transport_cooldowns WHERE scope_key LIKE %s OR scope_key=%s", (f"{root}:%", f"mtproto:{account}"))
        repo.close_all_connections()


def test_emoji_validation_rejects_campaign_only_destinations(repo):
    chat_id = -int("100" + str(uuid4().int % 10**12))
    target = str(chat_id)
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO rule_repost_campaign_targets(rule_id,target_id,is_active) VALUES (0,%s,FALSE)", (target,))
            cur.execute("INSERT INTO campaign_scheduled_post_targets(scheduled_post_id,rule_id,target_id,is_active) VALUES (0,0,%s,FALSE)", (target,))
    try:
        assert not repo.is_video_emoji_validation_chat_registered(chat_id)
        for table, enable, disable in (
            ("rule_repost_campaign_targets",
             "UPDATE rule_repost_campaign_targets SET is_active=TRUE WHERE target_id=%s",
             "UPDATE rule_repost_campaign_targets SET is_active=FALSE WHERE target_id=%s"),
            ("campaign_scheduled_post_targets",
             "UPDATE campaign_scheduled_post_targets SET is_active=TRUE WHERE target_id=%s",
             "UPDATE campaign_scheduled_post_targets SET is_active=FALSE WHERE target_id=%s"),
        ):
            with repo.connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(enable, (target,))
            assert repo.is_video_emoji_validation_chat_registered(chat_id), table
            with repo.connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(disable, (target,))
        assert not repo.is_video_emoji_validation_chat_registered(chat_id)
    finally:
        with repo.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM rule_repost_campaign_targets WHERE target_id=%s", (target,))
                cur.execute("DELETE FROM campaign_scheduled_post_targets WHERE target_id=%s", (target,))


def test_emoji_validation_registry_checks_inactive_rules_and_active_channels(repo):
    target = "-100" + str(uuid4().int % 10**12)
    other = target + "1"
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO routing(source_id,target_id,created_date,is_active) VALUES(%s,%s,'2026-01-01',FALSE) RETURNING id", (target, other))
            rule_id = cur.fetchone()["id"]
            cur.execute("INSERT INTO channels(channel_id,channel_type,added_date,is_active) VALUES(%s,'source','2026-01-01',FALSE)", (target,))
    try:
        assert repo.is_video_emoji_validation_chat_registered(int(target))
        assert repo.is_video_emoji_validation_chat_registered(int(other))
        with repo.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM routing WHERE id=%s", (rule_id,))
        assert not repo.is_video_emoji_validation_chat_registered(int(target))
        with repo.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE channels SET is_active=TRUE WHERE channel_id=%s", (target,))
        assert repo.is_video_emoji_validation_chat_registered(int(target))
    finally:
        with repo.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM routing WHERE id=%s", (rule_id,))
                cur.execute("DELETE FROM channels WHERE channel_id=%s", (target,))



@pytest.mark.skipif(os.getenv("VIMI_TEST_POSTGRES") != "1", reason="requires disposable PostgreSQL")
def test_terminal_document_rejection_cannot_be_reacquired():
    assert os.getenv("APP_PG_DB") == "vimi_publication_ci"
    repo = PostgresRepository()
    try:
        repo.init()
        for error, can_retry in (("repost_original_document_invalid_non_retryable", False), ("temporary download failure", True)):
            key = str(uuid4())
            repo.create_delivery_attempt(delivery_id=1, rule_id=1, tenant_id=1,
                                         job_id=None, idempotency_key=key,
                                         operation_kind="single", status="created")
            assert repo.mark_delivery_attempt_sending(key)
            assert repo.mark_delivery_attempt_failed(key, status="failed_before_send", error_text=error)
            assert repo.mark_delivery_attempt_sending(key) is can_retry
            if not can_retry:
                assert not repo.mark_delivery_attempt_failed(key, status="failed_before_send", error_text="late temporary failure")
                assert repo.get_delivery_attempt_by_idempotency_key(key)["error_text"] == error
                assert not repo.mark_delivery_attempt_sending(key)
            with repo.connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM delivery_attempts WHERE idempotency_key=%s", (key,))
    finally:
        repo.client.close_all_connections()



@pytest.mark.skipif(os.getenv("VIMI_TEST_POSTGRES") != "1", reason="requires disposable PostgreSQL")
def test_terminal_album_conflict_requires_overlapping_member():
    from app.delivery_idempotency import build_delivery_idempotency_key

    assert os.getenv("APP_PG_DB") == "vimi_publication_ci"
    repo = PostgresRepository()
    group = str(uuid4())
    args = dict(rule_id=1, target_id="-100123", media_group_id=group)
    def key(members):
        return build_delivery_idempotency_key(operation_kind="album", source_message_ids=members, **args)
    old = key([1, 2])
    try:
        repo.init()
        repo.create_delivery_attempt(delivery_id=1, rule_id=1, tenant_id=1,
                                     job_id=None, idempotency_key=old, target_id="-100123",
                                     operation_kind="album", status="created", source_message_ids=[1, 2])
        assert repo.mark_delivery_attempt_sending(old)
        assert repo.mark_delivery_attempt_failed(old, status="failed_before_send",
                                                error_text="repost_original_document_invalid_non_retryable")
        conflict = repo.find_conflicting_album_attempt(**args, current_key=key([2, 3]), current_delivery_ids=[2, 3])
        assert conflict["idempotency_key"] == old
        assert repo.find_conflicting_album_attempt(**args, current_key=key([3, 4]), current_delivery_ids=[3, 4]) is None
        assert repo.find_conflicting_album_attempt(**args, current_key=old, current_delivery_ids=[1, 2]) is None
    finally:
        with repo.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM delivery_attempts WHERE idempotency_key=%s", (old,))
        repo.client.close_all_connections()


def test_rule_catalog_sql_pagination_and_scope(repo):
    # Use a unique search term so this test does not depend on fixture row counts.
    prefix = str(uuid4())
    with repo.connect() as conn:
        with conn.cursor() as cur:
            ids = []
            for i in range(13):
                cur.execute(
                    """
                    INSERT INTO routing(source_id,target_id,created_date,tenant_id,is_active)
                    VALUES (%s,%s,%s,1,%s) RETURNING id
                    """,
                    (f"{prefix}-src-{i}", f"{prefix}-dst-{i}", "2026-01-01", i % 2 == 0),
                )
                ids.append(cur.fetchone()["id"])
            cur.execute(
                """
                INSERT INTO routing(source_id,target_id,created_date,tenant_id,created_by)
                VALUES (%s,%s,%s,42,777) RETURNING id
                """,
                (f"{prefix}-other", f"{prefix}-other-dst", "2026-01-01"),
            )
            other = cur.fetchone()["id"]
    first = repo.get_rule_catalog_page(1, search=prefix, page=0, selected_ids=[ids[-1], other])
    assert first.total == 13 and first.active == 7
    assert [r.id for r in first.rules] == ids[:10]
    assert first.valid_selected_ids == {ids[-1]}
    last = repo.get_rule_catalog_page(1, search=prefix, page=999)
    assert last.page == 1 and [r.id for r in last.rules] == ids[10:]
    assert repo.get_rule_catalog_page(1, search=f"#{ids[0]}").total == 1
    assert repo.get_rule_catalog_page(1, search=f"#{other}").total == 0
    assert repo.get_rule_catalog_page(1, search=f"# {ids[0]}").total == 1
    # Numeric input without # matches chat metadata, not the rule primary key.
    assert repo.get_rule_catalog_page(1, search=str(ids[0]), selected_ids=[ids[0]]).valid_selected_ids == (
        {ids[0]} if str(ids[0]) in f"{prefix}-src-0" or str(ids[0]) in f"{prefix}-dst-0" else set()
    )
    assert repo.get_rule_catalog_page(1, search="#999999999999999999999999").total == 0
    assert repo.get_rule_catalog_page(1, search=prefix, owner_admin_id=777).total == 14
    assert repo.get_rule_catalog_page(1, search=prefix, active=False).total == 6
    assert repo.get_rule_catalog_page(1, search=prefix + "%").total == 0
    folder = repo.create_rule_folder(1, prefix)
    assert folder
    repo.assign_rules_to_folder(1, [ids[0], ids[1]], folder)
    in_folder = repo.get_rule_catalog_page(1, folder_id=folder, search=prefix)
    assert {r.id for r in in_folder.rules} == {ids[0], ids[1]}
    assert in_folder.assignments[ids[0]]["id"] == folder
    outside = repo.get_rule_catalog_page(1, exclude_folder_id=folder, search=prefix)
    assert outside.total == 11
    assert repo.get_rule_catalog_page(1, search=prefix + "missing").rules == []


def test_postgres_queue_stats_aggregate_matches_physical_counts(repo):
    with repo.connect() as conn:
        with conn.cursor() as cur:
            expected = {}
            for table, column, where in (
                ("posts", "posts", ""), ("deliveries", "deliveries", ""),
                ("deliveries", "pending", "WHERE status='pending'"),
                ("deliveries", "processing", "WHERE status='processing'"),
                ("deliveries", "sent", "WHERE status='sent'"),
                ("deliveries", "faulty", "WHERE status='faulty'"),
                ("routing", "rules", ""), ("routing", "active_rules", "WHERE is_active"),
            ):
                cur.execute(f"SELECT COUNT(*) AS cnt FROM {table} {where}")
                expected[column] = cur.fetchone()["cnt"]
    assert repo.get_queue_stats() == expected




def test_tenant_live_status_matches_canonical_logical_counts(repo):
    tenant = 1000000 + uuid4().int % 100000000
    _, repost, repost_ids, _, _, _ = seed(repo, uuid4().int % 10**12, count=3)
    _, video, video_ids, _, _, _ = seed(repo, uuid4().int % 10**12, count=3)
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE routing SET tenant_id=%s WHERE id=ANY(%s)", (tenant, [repost, video]))
            cur.execute("UPDATE routing SET mode='video' WHERE id=%s", (video,))
            cur.execute("UPDATE deliveries SET status='pending' WHERE id=ANY(%s)", (repost_ids + video_ids,))
            cur.execute(
                "UPDATE posts SET content_json=%s WHERE id=(SELECT post_id FROM deliveries WHERE id=%s)",
                ('null', video_ids[-1]),
            )
            cur.execute("UPDATE deliveries SET status='faulty' WHERE id=%s", (video_ids[0],))
    snapshots = [repo.get_rule_card_snapshot(rule_id) for rule_id in (repost, video)]
    assert all(snapshots)
    totals = repo.get_tenant_queue_stats(tenant)
    assert totals["pending"] == sum(s["logical_pending"] for s in snapshots) == 2
    assert totals["faulty"] == sum(s["logical_faulty"] for s in snapshots) == 1
    assert totals["active_rules"] == 2
