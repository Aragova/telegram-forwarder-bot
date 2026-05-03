import asyncio

from app.sender import SenderService


class FakeRepo:
    def get_delivery_attempt_by_idempotency_key(self, _key):
        return {"status": "accepted", "sent_message_ids_json": [777]}

    def mark_delivery_sent_with_target_message(self, *args, **kwargs):
        return None


class DummyBot:
    pass


def test_repost_single_cache_hit_skips_send():
    service = SenderService(bot=DummyBot(), telethon_client=None, reaction_clients=[], db=FakeRepo())

    async def _should_not_call(*_args, **_kwargs):
        raise AssertionError("telegram send should not be called on cache hit")

    service._deliver_single = _should_not_call  # type: ignore[method-assign]
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
