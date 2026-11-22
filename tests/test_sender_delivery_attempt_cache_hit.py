import asyncio

from app.sender import SenderService


class _Rule:
    id = 1


class FakeRepo:
    def get_delivery_attempt_by_idempotency_key(self, _key):
        return {"status": "accepted", "sent_message_ids_json": [0]}

    def mark_delivery_faulty(self, delivery_id, error_text):
        self.faulty = (delivery_id, error_text)

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
    repo = FakeRepo()
    service = SenderService(bot=DummyBot(), telethon_client=None, reaction_clients=[], db=repo)

    async def _must_not_resend(*_args, **_kwargs):
        raise AssertionError("Unverified accepted attempts cannot be reposted")

    service._deliver_single = _must_not_resend
    result = asyncio.run(service.execute_repost_single_from_job(
        rule_id=1, delivery_id=10, message_id=11,
        source_channel="@src", target_id="-1001",
    ))
    assert result["ok"] is False
    assert result["retryable"] is True
    assert result["accepted"] is False
    assert not hasattr(repo, "faulty")


def test_repost_single_verified_cache_can_be_completed_without_sending():
    class VerifiedRepo(FakeRepo):
        sent = None

        def get_delivery_attempt_by_idempotency_key(self, _key):
            return {"status": "verified", "sent_message_ids_json": [778]}

        def mark_delivery_sent_with_target_message(self, *args, **kwargs):
            self.sent = (args, kwargs)

    repo = VerifiedRepo()
    service = SenderService(bot=DummyBot(), telethon_client=None, reaction_clients=[], db=repo)

    async def _must_not_resend(*_args, **_kwargs):
        raise AssertionError("Verified cache requires no second Telegram send")

    service._deliver_single = _must_not_resend
    assert asyncio.run(service.execute_repost_single_from_job(
        rule_id=1, delivery_id=10, message_id=11,
        source_channel="@src", target_id="-1001",
    )) is True
    assert repo.sent is not None


def test_repost_single_accepted_cache_does_not_increment_success_on_failed_read():
    class AcceptedRepo(FakeRepo):
        sent = False

        def get_delivery_attempt_by_idempotency_key(self, _key):
            return {"status": "accepted", "sent_message_ids_json": [370]}

        def mark_delivery_sent_with_target_message(self, *_args, **_kwargs):
            self.sent = True

    repo = AcceptedRepo()
    service = SenderService(bot=DummyBot(), telethon_client=None, reaction_clients=[], db=repo)

    async def _unconfirmed(**_kwargs):
        return []

    async def _must_not_resend(*_args, **_kwargs):
        raise AssertionError("Accepted candidate id cannot trigger a new copy")

    service._confirm_target_delivery_message_ids_with_retry = _unconfirmed
    service._deliver_single = _must_not_resend

    result = asyncio.run(service.execute_repost_single_from_job(
        rule_id=1, delivery_id=14, message_id=15,
        source_channel="@src", target_id="-1002",
    ))
    assert result["ok"] is False
    assert result["retryable"] is True
    assert repo.sent is False
    assert not hasattr(repo, "faulty")


def test_repost_single_success_touches_rule_once_at_outer_level():
    class TouchRepo(FakeRepo):
        def __init__(self):
            self.touches = []

        def get_delivery_attempt_by_idempotency_key(self, _key):
            return None

        def touch_rule_after_send(self, rule_id, interval):
            self.touches.append((rule_id, interval))
            return None

    repo = TouchRepo()
    service = SenderService(bot=DummyBot(), telethon_client=None, reaction_clients=[], db=repo)

    async def _deliver(*_args, **_kwargs):
        return True

    service._deliver_single = _deliver  # type: ignore[method-assign]
    ok = asyncio.run(
        service.execute_repost_single_from_job(
            rule_id=1,
            delivery_id=10,
            message_id=11,
            source_channel="@src",
            target_id="-1001",
            interval=3600,
        )
    )
    assert ok is True
    assert repo.touches == [(1, 3600)]


def test_accepted_single_reconciliation_rejects_stale_verified_promotion():
    class RacingRepo(FakeRepo):
        def __init__(self):
            self.sent = False
            self.promoted = 0

        def get_delivery_attempt_by_idempotency_key(self, _key):
            return {"status": "accepted", "sent_message_ids_json": [370]}

        def mark_delivery_attempt_verified(self, *_args, **_kwargs):
            self.promoted += 1
            return False  # Other worker finalized failed_after_send first.

        def mark_delivery_sent_with_target_message(self, *_args, **_kwargs):
            self.sent = True

    repo = RacingRepo()
    service = SenderService(bot=DummyBot(), telethon_client=None, reaction_clients=[], db=repo)

    async def confirmed(**_kwargs):
        return [370]

    async def no_second_send(*_args, **_kwargs):
        raise AssertionError("CAS loser must not send a second Telegram message")

    service._confirm_target_delivery_message_ids_with_retry = confirmed
    service._deliver_single = no_second_send
    result = asyncio.run(service.execute_repost_single_from_job(
        rule_id=1, delivery_id=14, message_id=15, source_channel="@src", target_id="-1002",
    ))
    assert result["ok"] is False
    assert result["retryable"] is True
    assert result["manual_review_required"] is True
    assert repo.promoted == 1
    assert repo.sent is False


def test_accepted_single_reconciliation_marks_sent_only_after_cas_promotion():
    class PromotableRepo(FakeRepo):
        def __init__(self):
            self.sent = None

        def get_delivery_attempt_by_idempotency_key(self, _key):
            return {"status": "accepted", "sent_message_ids_json": [370]}

        def mark_delivery_attempt_verified(self, *_args, **_kwargs):
            return True

        def mark_delivery_sent_with_target_message(self, delivery_id, **kwargs):
            self.sent = (delivery_id, kwargs)

    repo = PromotableRepo()
    service = SenderService(bot=DummyBot(), telethon_client=None, reaction_clients=[], db=repo)
    async def confirmed(**_kwargs):
        return [370]
    service._confirm_target_delivery_message_ids_with_retry = confirmed
    assert asyncio.run(service.execute_repost_single_from_job(
        rule_id=1, delivery_id=14, message_id=15, source_channel="@src", target_id="-1002",
    )) is True
    assert repo.sent[0] == 14
    assert repo.sent[1]["sent_message_id"] == 370
