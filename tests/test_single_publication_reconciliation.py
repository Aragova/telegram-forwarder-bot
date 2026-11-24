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

        def get_publication_receipt(self, key):
            return dict(self.row)

        def finalize_publication_receipt(self, key, published_ids, *, expected_started_at):
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
