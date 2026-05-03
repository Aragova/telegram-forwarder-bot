import asyncio
from pathlib import Path

from app.sender import SenderService


class _Rule:
    id = 1


class FakeVideoProcessor:
    async def get_video_info(self, *_args, **_kwargs):
        return {"duration": 10}

    async def send_with_retry(self, *_args, **_kwargs):
        return {"message_id": 778}


class FakeRepo:
    def __init__(self):
        self.calls = []
        self.attempt = None

    def get_delivery_attempt_by_idempotency_key(self, _key):
        return self.attempt

    def create_delivery_attempt(self, **kwargs):
        self.calls.append(("create", kwargs))
        return 1

    def mark_delivery_attempt_sending(self, key, **kwargs):
        self.calls.append(("sending", key, kwargs))
        return True

    def mark_delivery_attempt_accepted(self, key, *, sent_message_ids, telegram_method=None):
        self.calls.append(("accepted", key, list(sent_message_ids), telegram_method))
        return True

    def mark_delivery_attempt_failed(self, key, **kwargs):
        self.calls.append(("failed", key, kwargs))
        return True

    def get_rule(self, _rule_id):
        return _Rule()

    def mark_delivery_sent_with_target_message(self, *args, **kwargs):
        self.calls.append(("mark_delivery_sent", args, kwargs))
        return None

    def touch_rule_after_send(self, *_args, **_kwargs):
        return None


class DummyBot:
    pass


def _build_service(repo: FakeRepo) -> SenderService:
    service = SenderService(bot=DummyBot(), telethon_client=None, reaction_clients=[], db=repo)
    service.video_processor = FakeVideoProcessor()
    return service


def test_video_send_cache_hit_skips_actual_send(tmp_path: Path):
    repo = FakeRepo()
    repo.attempt = {"status": "accepted", "sent_message_ids_json": [777]}
    service = _build_service(repo)

    async def _should_not_call(*_args, **_kwargs):
        raise AssertionError("video send should not be called on cache hit")

    service.video_processor.send_with_retry = _should_not_call  # type: ignore[method-assign]
    processed_file = tmp_path / "video.mp4"
    processed_file.write_bytes(b"ok")
    result = asyncio.run(
        service.execute_video_send_from_job(
            delivery_id=10,
            rule_id=1,
            tenant_id=1,
            target_id="-1001",
            processed_video_path=str(processed_file),
            artifact_version=1,
            pipeline_version=1,
        )
    )
    assert result["ok"] is True
    assert result["cache_hit"] is True
    assert result["sent_message_ids"] == [777]


def test_video_send_marks_accepted_before_followup_error(tmp_path: Path):
    repo = FakeRepo()
    service = _build_service(repo)

    async def _raise_after_send(*_args, **_kwargs):
        raise RuntimeError("post-send failure")

    service._confirm_target_delivery_message_ids_with_retry = _raise_after_send  # type: ignore[method-assign]
    processed_file = tmp_path / "video.mp4"
    processed_file.write_bytes(b"ok")

    try:
        asyncio.run(
            service.execute_video_send_from_job(
                delivery_id=11,
                rule_id=1,
                tenant_id=1,
                target_id="-1002",
                message_id=123,
                processed_video_path=str(processed_file),
                artifact_version=1,
                pipeline_version=1,
            )
        )
    except RuntimeError:
        pass

    accepted_calls = [call for call in repo.calls if call[0] == "accepted"]
    assert accepted_calls
    assert accepted_calls[0][2] == [778]
