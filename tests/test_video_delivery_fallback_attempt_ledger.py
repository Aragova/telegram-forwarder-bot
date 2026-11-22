import asyncio

from app.sender import SenderService


class _Rule:
    id = 1


class FakeRepo:
    def __init__(self):
        self.calls = []
        self.attempt = None
        self.faulty = []

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

    def mark_delivery_faulty(self, delivery_id, error_text):
        self.faulty.append((delivery_id, error_text))
        return True

    def get_rule(self, _rule_id):
        return _Rule()

    def mark_delivery_sent_with_target_message(self, *args, **kwargs):
        self.calls.append(("mark_delivery_sent", args, kwargs))

    def touch_rule_after_send(self, *_args, **_kwargs):
        return None


class DummyBot:
    pass


def _build_service(repo: FakeRepo) -> SenderService:
    return SenderService(bot=DummyBot(), telethon_client=None, reaction_clients=[], db=repo)


def test_video_delivery_cache_hit_skips_actual_send():
    repo = FakeRepo()
    repo.attempt = {"status": "verified", "sent_message_ids_json": [777]}
    service = _build_service(repo)

    async def _should_not_call(*_args, **_kwargs):
        raise AssertionError("video_delivery should not send on cache hit")

    service._deliver_single_video = _should_not_call  # type: ignore[method-assign]

    async def _confirm(**kwargs):
        return list(kwargs["candidate_sent_message_ids"])

    async def _react(**_kwargs):
        return {"applied": True}

    service._confirm_target_delivery_message_ids_with_retry = _confirm
    service._add_reaction_for_rule_if_possible = _react
    ok = asyncio.run(
        service.execute_video_delivery_from_job(
            rule_id=1,
            delivery_id=10,
            message_id=20,
            source_channel="-100src",
            target_id="-100dst",
        )
    )
    assert ok is True


def test_legacy_bot_api_rate_limit_is_retryable_and_not_accepted():
    repo = FakeRepo()
    service = _build_service(repo)

    async def _rejected(*_args, **_kwargs):
        return {
            "ok": False, "retryable": True, "accepted": False,
            "rejected_before_send": True, "retry_after_seconds": 49,
            "error_text": "TelegramRetryAfter: retry_after=49",
        }

    service._deliver_single_video = _rejected
    result = asyncio.run(service.execute_video_delivery_from_job(
        rule_id=1, delivery_id=48, message_id=49,
        source_channel="-100src", target_id="-100dst",
    ))
    assert isinstance(result, dict)
    assert result["ok"] is False
    assert result["retryable"] is True
    assert result["accepted"] is False
    assert result["retry_after_seconds"] == 49
    assert any(c[0] == "failed" and c[2]["status"] == "failed_before_send" for c in repo.calls)
    assert not any(c[0] in ("accepted", "mark_delivery_sent") for c in repo.calls)


def test_video_delivery_uses_video_send_key_and_accepts_before_followup_error():
    repo = FakeRepo()
    service = _build_service(repo)

    async def _deliver(*_args, **_kwargs):
        return {"ok": True, "sent_message_ids": [778]}

    def _raise_after(*_args, **_kwargs):
        raise RuntimeError("post-send failure")

    service._deliver_single_video = _deliver  # type: ignore[method-assign]
    service._touch_rule_after_send_sync = _raise_after  # type: ignore[method-assign]

    try:
        asyncio.run(
            service.execute_video_delivery_from_job(
                rule_id=1,
                delivery_id=11,
                message_id=21,
                source_channel="-100src",
                target_id="-100dst",
            )
        )
    except RuntimeError:
        pass

    accepted_calls = [call for call in repo.calls if call[0] == "accepted"]
    assert accepted_calls
    assert accepted_calls[0][2] == [778]
    assert "video_send:v1" in accepted_calls[0][1]


def test_video_delivery_invalid_ids_not_accepted():
    repo = FakeRepo()
    service = _build_service(repo)

    async def _deliver(*_args, **_kwargs):
        return {"ok": True, "sent_message_ids": [0]}

    service._deliver_single_video = _deliver  # type: ignore[method-assign]
    ok = asyncio.run(
        service.execute_video_delivery_from_job(
            rule_id=1,
            delivery_id=12,
            message_id=22,
            source_channel="-100src",
            target_id="-100dst",
        )
    )
    assert ok is True
    accepted_calls = [call for call in repo.calls if call[0] == "accepted"]
    assert not accepted_calls
