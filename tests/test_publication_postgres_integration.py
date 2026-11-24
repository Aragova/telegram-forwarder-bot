"""Integration tests against an isolated PostgreSQL service, never production."""
import os

import pytest

from app.postgres_repository import PostgresRepository

pytestmark = pytest.mark.skipif(
    os.getenv("VIMI_PUBLICATION_PG_TEST") != "1",
    reason="Requires isolated PostgreSQL CI service",
)


@pytest.fixture(scope="module")
def repo():
    # Refuse any accidental use of an actual production database.
    assert os.environ.get("APP_PG_HOST") == "127.0.0.1"
    assert os.environ.get("APP_PG_DB") == "publication_test"
    assert os.environ.get("APP_PG_USER") == "publication_test"
    repository = PostgresRepository()
    repository.init()
    yield repository
    repository.client.close_thread_connection()


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


def test_real_pg_mixed_album_is_atomic_and_rollback_preserves_evidence(repo):
    seeded=seed(repo,101,count=2)
    key,rule_id,deliveries,source_ids,source,target=seeded
    assert register(repo,seeded,[500,500],[False,True])
    assert repo.get_delivery_attempt_by_idempotency_key(key)["status"]=="accepted"
    assert repo.get_delivery_attempt_by_idempotency_key(key)["sent_message_ids_json"]==[]
    assert not repo.finalize_publication_receipt(key,[500,501])
    assert [repo.get_delivery(d)["status"] for d in deliveries]==["processing","processing"]

    repo.record_telegram_publication_mappings(123,int(target),[(500,501)])
    repo.record_telegram_publication_mappings(123,int(target),[(500,501)])
    assert repo.get_telegram_publication_mappings(123,int(target),[500])==[501]
    assert repo.finalize_publication_receipt(key,[500,501])
    assert [repo.get_delivery(d)["sent_message_id"] for d in deliveries]==[500,501]
    assert repo.get_delivery_attempt_by_idempotency_key(key)["sent_message_ids_json"]==[500,501]
    assert repo.claim_publication_reaction(key)
    assert not repo.claim_publication_reaction(key)
    repo.finish_publication_reaction(key,"done",{"applied":True})

    assert repo.rollback_last_delivery(rule_id)
    assert repo.get_publication_receipt(key) is None
    assert repo.get_delivery_attempt_by_idempotency_key(key) is None
    assert [repo.get_delivery(d)["status"] for d in deliveries]==["pending","pending"]
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT published_ids FROM telegram_publication_receipts WHERE rule_id=%s",(rule_id,))
            assert cur.fetchone()["published_ids"]==[500,501]
    start_attempt(repo,key,rule_id,deliveries,source_ids,target)
    assert register(repo,seeded,[600,601],[True,True])


def test_real_pg_conflicting_mapping_and_deadline_never_finalize(repo):
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
