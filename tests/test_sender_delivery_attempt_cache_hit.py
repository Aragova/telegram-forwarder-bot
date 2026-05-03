import asyncio

from app.sender import SenderService


class _Rule:
    id = 1


class FakeRepo:
    def get_delivery_attempt_by_idempotency_key(self, _key):
        return {"status": "accepted", "sent_message_ids_json": [0]}

    def create_delivery_attempt(self, **_kwargs):
        return 1

    def mark_delivery_attempt_sending(self, *_args, **_kwargs):
        return True

    def get_rule(self, _rule_id):
        return _Rule()

    def touch_rule_after_send(self, *_args, **_kwargs):
        return None


class DummyBot:
    pass


def test_repost_single_cache_hit_ignores_invalid_zero_id():
    service = SenderService(bot=DummyBot(), telethon_client=None, reaction_clients=[], db=FakeRepo())

    async def _should_call(*_args, **_kwargs):
        return True

    service._deliver_single = _should_call  # type: ignore[method-assign]
    ok = asyncio.run(
        service.execute_repost_single_from_job(
            rule_id=1,
            delivery_id=10,
            message_id=11,
            source_channel="@src",
            target_id="-1001",
        )
    )
    assert ok is True
