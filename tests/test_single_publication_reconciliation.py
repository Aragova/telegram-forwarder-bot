import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

from app.single_publication_reconciliation import reconcile_pending_single


def test_pending_single_defers_without_second_send_then_finishes_and_reacts_to_final_id():
    class Repo:
        def __init__(self):
            self.row = dict(delivery_ids=[12], rule_id=149, tenant_id=1, sender_account_id=1,
                            target_peer_id=-100, target_thread_id=None, source_channel="-200",
                            source_message_ids=[7], scheduled_ids=[2292], published_ids=None, expired_at=None,
                            send_started_at=datetime.now(timezone.utc), expired=False,
                            reaction_state="pending", dispatch_stale=False)
            self.finalized = 0
            self.fail_finalize = False

        def get_publication_receipt(self, key):
            return dict(self.row)

        def finalize_publication_receipt(self, key, published_ids, *, expected_started_at):
            if self.fail_finalize:
                raise OSError("database unavailable after receipt committed")
            self.finalized += 1
            self.row["published_ids"] = published_ids
            return True

        def claim_publication_reaction(self, key, published_ids, *, expected_started_at):
            self.row["reaction_state"] = "dispatching"
            return True

        def finish_publication_reaction(self, key, state, result, *, expected_started_at):
            self.row["reaction_state"] = state

        def get_rule(self, rule_id):
            return SimpleNamespace(id=rule_id, target_id="-100", target_thread_id=None)

    class Inbox:
        sender_account_id = 1
        mapped = None

        async def lookup_many(self, peer_id, scheduled_ids):
            assert (peer_id, scheduled_ids) == (-100, [2292])
            return [self.mapped] if self.mapped else []

    class Client:
        publication_inbox = Inbox()

        async def get_entity(self, peer):
            return SimpleNamespace(broadcast=True, megagroup=False)

        async def get_messages(self, entity, ids):
            assert ids == 5181
            return SimpleNamespace(id=ids, chat_id=-100, from_scheduled=True,
                                   post=True, date=datetime.now(timezone.utc))

        async def send_file(self, *args, **kwargs):
            raise AssertionError("Never replay accepted submission")

    async def check():
        repo = Repo()
        client = Client()
        reactions = []

        async def react(**kwargs):
            reactions.append(kwargs["sent_message_id"])
            return {"applied": True}

        owner = SimpleNamespace(db=repo, telethon=client, _add_reaction_for_rule_if_possible=react)
        first = await reconcile_pending_single(owner, "key")
        assert first["publication_pending"]
        assert not reactions and repo.finalized == 0
        client.publication_inbox.mapped = 5181
        repo.fail_finalize = True
        failed = await reconcile_pending_single(owner, "key")
        assert failed["publication_pending"] and not failed["retryable"]
        assert not reactions and repo.finalized == 0
        repo.fail_finalize = False
        second = await reconcile_pending_single(owner, "key")
        assert second["ok"] and second["sent_message_ids"] == [5181]
        assert reactions == [5181] and repo.finalized == 1
        third = await reconcile_pending_single(owner, "key")
        assert third["ok"] and reactions == [5181] and repo.finalized == 1

    asyncio.run(check())


def test_publication_key_matches_sender_jobs():
    from app.single_publication_reconciliation import publication_key_for_job
    payload = dict(rule_id=149,delivery_id=12,delivery_ids=[12,13],target_id="-100123",media_group_id="abc")
    assert publication_key_for_job("repost_single",payload) == "delivery:12:target:-100123:single"
    assert publication_key_for_job("repost_album",payload) == "rule:149:target:-100123:media_group:abc:album"
    assert publication_key_for_job("video_send",payload) == publication_key_for_job("video_delivery",payload)
    assert publication_key_for_job("video_download",payload) is None


def test_read_only_reconciliation_and_safe_reaction_retry():
    class Repo:
        def __init__(self):
            self.row = dict(delivery_ids=[12], rule_id=149, tenant_id=1, sender_account_id=1,
                            target_peer_id=-100, target_thread_id=None, source_channel="-200",
                            source_message_ids=[7], scheduled_ids=[2292], published_ids=None,
                            expired_at=None, send_started_at=datetime.now(timezone.utc),
                            expired=False, reaction_state="pending", dispatch_stale=False)
            self.lookups = 0

        def get_publication_receipt(self, key):
            return dict(self.row)

        def get_telegram_publication_mappings(self, account, peer, ids):
            assert (account, peer, ids) == (1, -100, [2292])
            self.lookups += 1
            return [5181]

        def finalize_publication_receipt(self, key, published_ids, *, expected_started_at):
            self.row["published_ids"] = published_ids

        def claim_publication_reaction(self, key, published_ids, *, expected_started_at):
            assert self.row["reaction_state"] == "pending"
            self.row["reaction_state"] = "dispatching"
            return True

        def finish_publication_reaction(self, key, state, result, *, expected_started_at):
            self.row["reaction_state"] = state

        def get_rule(self, rule_id):
            return SimpleNamespace(id=rule_id, target_id="-100", target_thread_id=None)

    class Reader:
        account_id = 2

        async def get_me(self):
            return SimpleNamespace(id=self.account_id)

        async def get_entity(self, peer):
            return SimpleNamespace(broadcast=True, megagroup=False)

        async def get_messages(self, entity, ids):
            return SimpleNamespace(id=ids, chat_id=-100, from_scheduled=True,
                                   post=True, date=datetime.now(timezone.utc))

    async def check():
        repo, reader = Repo(), Reader()
        reactions = []

        async def react(**kwargs):
            assert kwargs["publication_intent"] == ("key", repo.row["send_started_at"])
            reactions.append(kwargs["sent_message_id"])
            if len(reactions) == 1:
                return {"reason": "enqueue_failed"}
            return {"enqueued": True}

        owner = SimpleNamespace(db=repo, telethon=None, telethon_read=reader,
                                _add_reaction_for_rule_if_possible=react)
        assert (await reconcile_pending_single(owner, "key"))["publication_pending"]
        assert repo.lookups == 0 and not reactions
        reader.account_id = 1
        assert (await reconcile_pending_single(owner, "key"))["publication_pending"]
        assert repo.row["published_ids"] == [5181]
        assert repo.row["reaction_state"] == "pending"
        assert (await reconcile_pending_single(owner, "key"))["ok"]
        assert (await reconcile_pending_single(owner, "key"))["ok"]
        assert reactions == [5181, 5181]
        assert not hasattr(reader, "publication_inbox") and owner.telethon is None

    asyncio.run(check())

def test_failed_receipt_lookup_never_authorizes_another_send():
    class Repo:
        def get_publication_receipt(self, key):
            raise OSError("database unavailable")

    result = asyncio.run(reconcile_pending_single(SimpleNamespace(db=Repo()), "key"))
    assert result["publication_pending"] is True
    assert not result["ok"]
    assert not result["retryable"]
