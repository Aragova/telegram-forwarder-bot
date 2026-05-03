import asyncio

from app.sender import SenderService


class DummyBot:
    pass


class DummyRule:
    id = 1


class Repo:
    def __init__(self):
        self.accepted = []
        self.sent = []

    def get_delivery_attempt_by_idempotency_key(self, _key):
        return None

    def create_delivery_attempt(self, **_kwargs):
        return 1

    def mark_delivery_attempt_sending(self, *_args, **_kwargs):
        return True

    def mark_delivery_attempt_accepted(self, key, *, sent_message_ids, telegram_method=None):
        self.accepted.append((key, sent_message_ids, telegram_method))

    def mark_delivery_attempt_failed(self, *_args, **_kwargs):
        raise AssertionError("must not fail after accepted")

    def get_rule(self, _rule_id):
        return DummyRule()

    def mark_delivery_sent(self, delivery_id, sent_message_id=None, sent_message_ids_json=None, target_id=None, delivery_method=None):
        self.sent.append((delivery_id, sent_message_id, sent_message_ids_json, target_id, delivery_method))

    def touch_rule_after_send(self, *_args, **_kwargs):
        return None


class VideoProcessorStub:
    async def get_video_info(self, *_args, **_kwargs):
        return {"duration": 1}

    async def send_with_retry(self, *_args, **_kwargs):
        class M:
            message_id = 301
        return M()


def test_post_send_safe_swallows_exception():
    s = SenderService(bot=DummyBot(), telethon_client=None, reaction_clients=[], db=Repo())

    async def run():
        res = await s._run_post_send_step_safe(
            step_name="verify",
            rule_id=1,
            delivery_id=1,
            coro_factory=lambda: (_ for _ in ()).throw(RuntimeError("boom")),
        )
        assert res["ok"] is False

    asyncio.run(run())


def test_video_send_verify_failure_after_accepted_is_non_fatal():
    repo = Repo()
    s = SenderService(bot=DummyBot(), telethon_client=None, reaction_clients=[], db=repo)
    s.video_processor = VideoProcessorStub()

    async def _confirm(**_kwargs):
        return []

    async def _react(*_args, **_kwargs):
        raise RuntimeError("reaction fail")

    s._confirm_target_delivery_message_ids_with_retry = _confirm  # type: ignore
    s._add_reaction_if_possible = _react  # type: ignore

    result = asyncio.run(
        s.execute_video_send_from_job(
            delivery_id=7,
            rule_id=1,
            target_id="-1001",
            source_channel="@src",
            message_id=11,
            processed_video_path=__file__,
        )
    )
    assert result.get("ok") is True
    assert repo.accepted
    assert repo.sent


def test_video_delivery_post_send_failure_after_accepted_is_non_fatal():
    repo = Repo()
    s = SenderService(bot=DummyBot(), telethon_client=None, reaction_clients=[], db=repo)

    async def _deliver(*_args, **_kwargs):
        return {"ok": False, "sent_message_ids": [401]}

    s._deliver_single_video = _deliver  # type: ignore

    result = asyncio.run(
        s.execute_video_delivery_from_job(
            rule_id=1,
            delivery_id=8,
            message_id=12,
            source_channel="@src",
            target_id="-1001",
        )
    )
    assert result is True
    assert repo.accepted
    assert repo.sent
