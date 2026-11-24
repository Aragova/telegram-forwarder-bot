"""Integration tests against an isolated PostgreSQL service, never production."""
import os
import json
from datetime import datetime, timezone

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
    assert repo.get_delivery_attempt_by_idempotency_key(key) is None
    assert [repo.get_delivery(d)["status"] for d in deliveries]==["pending","pending"]
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT published_ids FROM telegram_publication_receipts WHERE rule_id=%s",(rule_id,))
            assert cur.fetchone()["published_ids"]==[500,501]
    start_attempt(repo,key,rule_id,deliveries,source_ids,target)
    assert register(repo,seeded,[600,601],[True,True])
    assert not repo.finalize_publication_receipt(key,[500,501],expected_started_at=started)


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


def test_real_pg_partial_botapi_album_evidence_is_not_published_ids(repo):
    seeded=seed(repo,104,count=2)
    key,_,_,_,_,_=seeded
    assert repo.record_unverified_botapi_album(key,[901,0])
    attempt=repo.get_delivery_attempt_by_idempotency_key(key)
    assert attempt["status"]=="accepted" and attempt["sent_message_ids_json"]==[]
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT extra_json FROM audit_log WHERE event_type='botapi_publication_unverified' AND delivery_id=%s",(attempt["delivery_id"],))
            raw=cur.fetchone()["extra_json"]
            assert (json.loads(raw) if isinstance(raw,str) else raw)["raw_message_ids"]==[901,0]



def test_pending_receipt_does_not_block_other_rules_to_same_target(repo):
    waiting=seed(repo,105)
    assert register(repo,waiting,[88])
    other=seed(repo,106)
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE routing SET target_id=%s WHERE id=%s",(waiting[5],other[1]))
            cur.execute("UPDATE deliveries SET status='pending' WHERE id=ANY(%s)",(other[2],))
            cur.execute("DELETE FROM delivery_attempts WHERE idempotency_key=%s",(other[0],))
    result=repo.take_due_delivery_and_create_job(other[1],datetime.now(timezone.utc).isoformat())
    assert result is not None
    assert result["status"]=="created"
    assert repo.get_delivery(waiting[2][0])["status"]=="processing"


def test_publication_finalize_preserves_scheduler_reserved_slot(repo):
    seeded=seed(repo,107)
    key,rule_id,_,_,_,target=seeded
    reserved="2099-01-01T00:00:00+00:00"
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE routing SET next_run_at=%s WHERE id=%s",(reserved,rule_id))
    assert register(repo,seeded,[123])
    repo.record_telegram_publication_mappings(123,int(target),[(123,456)])
    assert repo.finalize_publication_receipt(key,[456])
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT next_run_at,last_sent_at FROM routing WHERE id=%s",(rule_id,))
            before=cur.fetchone()
    assert before["next_run_at"]==reserved
    assert before["last_sent_at"] is not None
    assert not repo.finalize_publication_receipt(key,[456])
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT next_run_at,last_sent_at FROM routing WHERE id=%s",(rule_id,))
            assert cur.fetchone()==before



def test_rescan_archives_confirmed_receipt_and_releases_logical_key(repo):
    seeded=seed(repo,108,count=2)
    key,rule_id,deliveries,source_ids,source,target=seeded
    assert register(repo,seeded,[10,11])
    repo.record_telegram_publication_mappings(123,int(target),[(10,100),(11,101)])
    assert repo.finalize_publication_receipt(key,[100,101])
    attempt_id=repo.get_delivery_attempt_by_idempotency_key(key)["id"]
    owner="test-rescan-108"
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE routing SET is_active=FALSE,rescan_owner_token=%s WHERE id=%s",(owner,rule_id))
    assert repo.reset_rule_history_for_replay(rule_id,owner_token=owner)==(1,2)
    assert repo.get_publication_receipt(key) is None
    archived_key=f"{key}:rescan:{attempt_id}"
    assert repo.get_publication_receipt(archived_key)["published_ids"]==[100,101]
    assert repo.get_delivery_attempt_by_idempotency_key(archived_key)["status"]=="verified"
    assert repo.get_telegram_publication_mappings(123,int(target),[10,11])==[100,101]
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM posts WHERE source_channel=%s ORDER BY message_id",(source,))
            post_ids=[row["id"] for row in cur.fetchall()]
            new_deliveries=[]
            for post_id in post_ids:
                cur.execute("INSERT INTO deliveries(rule_id,post_id,status,created_at,tenant_id) VALUES (%s,%s,'processing',%s,1) RETURNING id",
                            (rule_id,post_id,"2026-01-01T00:00:00+00:00"))
                new_deliveries.append(cur.fetchone()["id"])
    start_attempt(repo,key,rule_id,new_deliveries,source_ids,target)
    assert register(repo,(key,rule_id,new_deliveries,source_ids,source,target),[12,13])
    assert repo.get_publication_receipt(key)["published_ids"] is None



def test_publication_deadline_is_checked_after_advisory_lock(repo):
    import time
    from concurrent.futures import ThreadPoolExecutor

    seeded = seed(repo,109)
    key, _, deliveries, _, _, target = seeded
    assert register(repo,seeded,[15])
    repo.record_telegram_publication_mappings(123,int(target),[(15,115)])
    lock_key = f"publication:123:{target}"
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE telegram_publication_receipts SET deadline_at=clock_timestamp()+INTERVAL '3 seconds' WHERE idempotency_key=%s",(key,))
        conn.commit()
    with ThreadPoolExecutor(max_workers=1) as pool:
        with repo.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",(lock_key,))
                future = pool.submit(repo.finalize_publication_receipt,key,[115])
                until = time.monotonic()+2
                while time.monotonic() < until:
                    cur.execute("SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND NOT granted) AS waiting")
                    if cur.fetchone()["waiting"]:
                        break
                    time.sleep(0.05)
                else:
                    raise AssertionError("Finalizer never waited on publication lock")
                cur.execute("SELECT pg_sleep(3.2)")
            conn.commit()
        assert future.result(timeout=5) is False
    assert repo.get_delivery(deliveries[0])["status"] == "processing"


def test_real_pg_campaign_accepted_unknown_blocks_clean_channel(repo):
    _,rule_id,_,_,_,target=seed(repo,110)
    saved=repo.create_saved_post(rule_id=rule_id,title="test",content={},
        source_chat_id=None,source_message_id=None,source_media_group_id=None,created_by=None)
    run=repo.create_campaign_run(rule_id=rule_id,saved_post_id=saved,run_type="manual",
        status="sending",show_seconds=10,started_by=None,targets_total=2)
    unknown=repo.create_campaign_run_message(run_id=run,rule_id=rule_id,saved_post_id=saved,
        target_kind="main",target_id=target)
    rejected=repo.create_campaign_run_message(run_id=run,rule_id=rule_id,saved_post_id=saved,
        target_kind="extra",target_id="-10099110")
    assert repo.mark_campaign_run_message_failed(rejected,error_text="rejected")
    assert repo.get_active_campaign_placements_summary_for_rule(rule_id)["placements_total"]==0
    assert repo.mark_campaign_run_message_failed(unknown,error_text="unknown",
        accepted_unverified=True,raw_message_ids=[0])
    summary=repo.get_active_campaign_placements_summary_for_rule(rule_id)
    assert summary["placements_total"]==1
    assert summary["delete_problem_total"]==1
    rows=repo.list_active_campaign_placements_for_rule(rule_id)
    assert len(rows)==1
    assert rows[0]["sent"]==0
    assert rows[0]["accepted_unverified"]==1
    assert rows[0]["active_messages_total"]==1
    assert unknown not in [row["id"] for row in repo.claim_due_campaign_run_messages_for_delete()]
    assert repo.get_campaign_run_message(unknown)["accepted_unverified"] is True
    assert repo.get_campaign_run_message(rejected)["accepted_unverified"] is False
    rows = repo.list_campaign_run_messages(run)
    assert next(row for row in rows if row["id"] == unknown)["accepted_unverified"] is True
    resolved = repo.resolve_campaign_run_delete_failures(run_id=run, rule_id=rule_id, actor_id=1)
    assert resolved["resolved"] == 1 and resolved["remaining_failed"] == 0
    assert repo.get_active_campaign_placements_summary_for_rule(rule_id)["placements_total"] == 0
    assert repo.get_campaign_run_message(unknown)["accepted_unverified"] is True
    assert repo.get_campaign_run_message(unknown)["delete_status"] == "deleted"
    assert repo.get_campaign_run_message(rejected)["delete_status"] == "failed"



def test_rescan_refuses_unverified_botapi_album(repo):
    key, rule_id, deliveries, _, _, _ = seed(repo, 111, count=2)
    assert repo.record_unverified_botapi_album(key, [0, 102])
    owner = "test-rescan-111"
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE routing SET is_active=FALSE,rescan_owner_token=%s WHERE id=%s",
                (owner, rule_id),
            )
            cur.execute("UPDATE deliveries SET status='faulty' WHERE id=ANY(%s)", (deliveries,))
    with pytest.raises(RuntimeError, match="неподтверждённый альбом Bot API"):
        repo.reset_rule_history_for_replay(rule_id, owner_token=owner)
    assert repo.get_delivery_attempt_by_idempotency_key(key)["status"] == "accepted"
    assert all(repo.get_delivery(delivery_id) is not None for delivery_id in deliveries)


def test_self_loop_suppression_keeps_unrelated_album_members(repo):
    _, rule_id, deliveries, source_ids, source, _ = seed(repo, 112, count=3)
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE routing SET target_id=source_id WHERE id=%s", (rule_id,))
            cur.execute("UPDATE deliveries SET status='sent', sent_message_id=%s WHERE id=%s",
                        (source_ids[1], deliveries[0]))
    assert repo.suppress_self_target_echo_deliveries(rule_id, deliveries[1:]) == [deliveries[1]]
    assert repo.get_delivery(deliveries[1])["status"] == "faulty"
    assert repo.get_delivery(deliveries[2])["status"] == "processing"
    assert repo.suppress_self_target_echo_deliveries(rule_id, deliveries[1:]) == [deliveries[1]]


def test_rescan_refuses_expired_mtproto_receipt(repo):
    seeded = seed(repo, 113, count=2)
    key, rule_id, deliveries, _, _, _ = seeded
    assert register(repo, seeded, [20, 21])
    owner = "test-rescan-113"
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE telegram_publication_receipts SET deadline_at=NOW()-INTERVAL '1 second' WHERE idempotency_key=%s", (key,))
            cur.execute("UPDATE routing SET is_active=FALSE,rescan_owner_token=%s WHERE id=%s", (owner, rule_id))
    assert repo.expire_publication_receipt(key, "expired")
    with pytest.raises(RuntimeError, match="неподтверждённая публикация MTProto"):
        repo.reset_rule_history_for_replay(rule_id, owner_token=owner)
    assert repo.get_publication_receipt(key) is not None
    assert all(repo.get_delivery(d) is not None for d in deliveries)


def test_album_receipt_accepts_single_remaining_member(repo):
    seeded = seed(repo, 114, count=2)
    key, rule_id, deliveries, source_ids, source, target = seeded
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE deliveries SET status='faulty' WHERE id=%s", (deliveries[0],))
            cur.execute("UPDATE delivery_attempts SET delivery_id=%s,source_message_ids_json=%s::jsonb WHERE idempotency_key=%s",
                        (deliveries[1], json.dumps([source_ids[1]]), key))
    assert register(repo, (key, rule_id, deliveries[1:], source_ids[1:], source, target), [30])
    repo.record_telegram_publication_mappings(123, int(target), [(30, 130)])
    assert repo.finalize_publication_receipt(key, [130])
    assert repo.get_delivery(deliveries[0])["status"] == "faulty"
    assert repo.get_delivery(deliveries[1])["status"] == "sent"


def test_publication_deadline_is_checked_after_delivery_row_lock(repo):
    import time
    from concurrent.futures import ThreadPoolExecutor

    seeded = seed(repo, 115)
    key, _, deliveries, _, _, target = seeded
    assert register(repo, seeded, [40])
    repo.record_telegram_publication_mappings(123, int(target), [(40, 140)])
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE telegram_publication_receipts SET deadline_at=clock_timestamp()+INTERVAL '3 seconds' WHERE idempotency_key=%s", (key,))
        conn.commit()
    with ThreadPoolExecutor(max_workers=1) as pool:
        with repo.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM deliveries WHERE id=%s FOR UPDATE", (deliveries[0],))
                future = pool.submit(repo.finalize_publication_receipt, key, [140])
                until = time.monotonic() + 2
                while time.monotonic() < until:
                    cur.execute("SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='transactionid' AND NOT granted) AS waiting")
                    if cur.fetchone()["waiting"]:
                        break
                    time.sleep(0.05)
                else:
                    raise AssertionError("Finalizer did not wait for delivery row")
                cur.execute("SELECT pg_sleep(3.2)")
            conn.commit()
        assert future.result(timeout=5) is False
    assert repo.get_delivery(deliveries[0])["status"] == "processing"


def test_publication_reaction_enqueue_and_receipt_complete_atomically(repo):
    seeded = seed(repo, 116)
    key, rule_id, _, _, _, target = seeded
    assert register(repo, seeded, [41])
    repo.record_telegram_publication_mappings(123, int(target), [(41, 141)])
    assert repo.finalize_publication_receipt(key, [141])
    started = repo.get_publication_receipt(key)["send_started_at"]
    assert repo.claim_publication_reaction(key, [141], expected_started_at=started)
    repo.finish_publication_reaction(key, "pending", {"reason": "enqueue_failed"}, expected_started_at=started)
    assert repo.claim_publication_reaction(key, [141], expected_started_at=started)
    kwargs = dict(tenant_id=1, rule_id=rule_id, target_id=target, message_id=141,
                  account_ids=[1], publication_intent=(key, started))
    assert repo.enqueue_reaction_job(**(kwargs | {"message_id": 142})) is None
    job_id = repo.enqueue_reaction_job(**kwargs)
    assert job_id is not None
    receipt = repo.get_publication_receipt(key)
    assert receipt["reaction_state"] == "done"
    # Simulate a lost response after the database commit.
    repo.finish_publication_reaction(key, "pending", {"reason": "enqueue_failed"}, expected_started_at=started)
    assert not repo.claim_publication_reaction(key, [141], expected_started_at=started)
    assert repo.enqueue_reaction_job(**kwargs) is None
    assert repo.get_publication_receipt(key)["reaction_state"] == "done"
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM reaction_jobs WHERE rule_id=%s AND message_id=%s", (rule_id, 141))
            assert [row["id"] for row in cur.fetchall()] == [job_id]


def test_publication_recovery_cursor_and_exact_replay_after_restart(repo):
    import asyncio
    from telethon import functions, types
    from app.publication_difference_recovery import PublicationDifferenceRecovery
    from app.telegram_publication import PublicationInbox
    from app.single_publication_reconciliation import reconcile_pending_publication
    from types import SimpleNamespace
    from telethon import utils

    seeded = seed(repo, 117)
    key, rule_id, deliveries, source_ids, source, _ = seeded
    peer = types.PeerChannel(900117)
    target = str(utils.get_peer_id(peer))
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE routing SET target_id=%s WHERE id=%s", (target, rule_id))
            cur.execute("UPDATE delivery_attempts SET target_id=%s WHERE idempotency_key=%s", (target, key))
    seeded = (key, rule_id, deliveries, source_ids, source, target)
    account = 123
    baseline = datetime.now(timezone.utc).replace(microsecond=0)
    state = types.updates.State(pts=101, qts=0, date=baseline, seq=1, unread_count=0)

    class Client:
        def __init__(self, response):
            self.response = response

        async def __call__(self, request, *, flood_sleep_threshold):
            assert flood_sleep_threshold == 0
            if isinstance(request, functions.updates.GetStateRequest):
                return state
            assert isinstance(request, functions.updates.GetDifferenceRequest)
            assert request.pts == 101 and request.qts == 0
            return self.response

        async def get_entity(self, peer_id):
            assert peer_id == int(target)
            return SimpleNamespace(broadcast=True, megagroup=False)

        async def get_messages(self, entity, ids):
            assert ids == 142
            return SimpleNamespace(id=ids, chat_id=int(target), from_scheduled=True,
                                   post=True, date=datetime.now(timezone.utc))

    async def scenario():
        initial = PublicationDifferenceRecovery(Client(None), PublicationInbox(repo, account))
        await initial.initialize()
        assert register(repo, seeded, [42])
        assert not repo.finalize_publication_receipt(key, [142])
        # The original process never receives the live publication event.
        published = types.UpdateDeleteScheduledMessages(
            peer=peer, messages=[42], sent_messages=[142],
        )
        deletion = types.UpdateDeleteScheduledMessages(
            peer=published.peer, messages=[43],
        )
        result = types.updates.Difference(
            new_messages=[], new_encrypted_messages=[], other_updates=[deletion, published],
            chats=[], users=[], state=types.updates.State(
                pts=102, qts=0, date=baseline, seq=2, unread_count=0,
            ),
        )
        # Fail the first database write after receiving the replay page.
        class FailingRepository:
            def __getattr__(self, name):
                return getattr(repo, name)

            def record_telegram_publication_mappings(self, *_args):
                raise RuntimeError("simulated database outage")

        failed = PublicationDifferenceRecovery(Client(result), PublicationInbox(FailingRepository(), account))
        await failed.recover()
        assert repo.get_publication_recovery_cursor(account)["version"] == 0
        assert repo.get_telegram_publication_mappings(account, int(target), [42]) == []

        client = Client(result)
        client.publication_inbox = PublicationInbox(repo, account)
        client.publication_recovery = PublicationDifferenceRecovery(client, client.publication_inbox)
        await client.publication_recovery.initialize()

        reactions = []

        async def react(**kwargs):
            reactions.append(kwargs["sent_message_id"])
            return {"reason": "disabled"}

        owner = SimpleNamespace(db=repo, telethon=client, _add_reaction_for_rule_if_possible=react)
        outcome = await reconcile_pending_publication(owner, key)
        assert outcome["ok"] and outcome["sent_message_ids"] == [142]
        assert repo.get_telegram_publication_mappings(account, int(target), [42]) == [142]
        assert repo.get_telegram_publication_mappings(account, int(target), [43]) == []
        cursor = repo.get_publication_recovery_cursor(account)
        assert cursor["pts"] == 102 and cursor["version"] == 1
        assert not repo.advance_publication_recovery_cursor(account, 0, 103, 0, baseline)
        assert not repo.finalize_publication_receipt(key, [142])
        with repo.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT status,sent_message_id FROM deliveries WHERE id=%s", (deliveries[0],))
                assert cur.fetchone() == {"status": "sent", "sent_message_id": 142}
        assert (await reconcile_pending_publication(owner, key))["ok"]
        assert repo.get_publication_receipt(key)["reaction_state"] == "done"
        assert reactions == [142]

    asyncio.run(scenario())
