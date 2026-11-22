import asyncio
import logging
from pathlib import Path

from aiogram.exceptions import TelegramEntityTooLarge
from aiogram.methods import SendVideo

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

    def mark_delivery_faulty(self, delivery_id, error_text):
        self.calls.append(("faulty", delivery_id, error_text))
        return True

    def mark_delivery_attempt_verified(self, key, *, sent_message_ids):
        self.calls.append(("verified", key, list(sent_message_ids)))
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

    async def _confirm(**kwargs):
        return list(kwargs["candidate_sent_message_ids"])

    async def _react(**_kwargs):
        return {"applied": True}

    service._confirm_target_delivery_message_ids_with_retry = _confirm  # type: ignore[method-assign]
    service._add_reaction_for_rule_if_possible = _react  # type: ignore[method-assign]
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
    mark_sent_calls = [call for call in repo.calls if call[0] == "mark_delivery_sent"]
    assert mark_sent_calls
    _, args, kwargs = mark_sent_calls[0]
    assert args[0] == 10
    assert kwargs["sent_message_id"] == 777
    assert kwargs["sent_message_ids"] == [777]
    assert kwargs["target_id"] == "-1001"
    assert kwargs["delivery_method"] == "video_cache_verified"


def test_accepted_cache_wrong_peer_id_is_never_resent(tmp_path: Path):
    repo = FakeRepo()
    repo.attempt = {"status": "accepted", "sent_message_ids_json": [370]}
    service = _build_service(repo)

    async def _should_not_send(*_args, **_kwargs):
        raise AssertionError("Never resend an accepted video to repair its id")

    async def _unconfirmed(**_kwargs):
        return []

    service.video_processor.send_with_retry = _should_not_send
    service._confirm_target_delivery_message_ids_with_retry = _unconfirmed
    processed_file = tmp_path / "video.mp4"
    processed_file.write_bytes(b"ok")

    result = asyncio.run(
        service.execute_video_send_from_job(
            delivery_id=141, rule_id=141, tenant_id=1,
            target_id="-1004477194308",
            processed_video_path=str(processed_file),
            artifact_version=1, pipeline_version=1,
        )
    )
    assert result["ok"] is False
    assert result["retryable"] is False
    assert result["accepted"] is True
    assert result["manual_review_required"] is True
    assert not any(c[0] == "mark_delivery_sent" for c in repo.calls)
    assert any(c[0] == "faulty" for c in repo.calls)


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


def test_video_send_skips_accept_for_invalid_zero_sent_id(tmp_path: Path):
    repo = FakeRepo()
    service = _build_service(repo)

    class _ZeroVideoProcessor(FakeVideoProcessor):
        async def send_with_retry(self, *_args, **_kwargs):
            return {"message_id": 0}

    service.video_processor = _ZeroVideoProcessor()
    processed_file = tmp_path / "video.mp4"
    processed_file.write_bytes(b"ok")

    result = asyncio.run(
        service.execute_video_send_from_job(
            delivery_id=12,
            rule_id=1,
            tenant_id=1,
            target_id="-1003",
            message_id=123,
            processed_video_path=str(processed_file),
            artifact_version=1,
            pipeline_version=1,
        )
    )

    accepted_calls = [call for call in repo.calls if call[0] == "accepted"]
    assert accepted_calls  # acceptance with unknown target id is retained
    assert accepted_calls[0][2] == []
    assert result["ok"] is False
    assert result["retryable"] is False


def test_video_send_reaction_after_send_passes_rule_id(tmp_path: Path):
    repo = FakeRepo()
    service = _build_service(repo)
    reaction_calls = []

    async def _confirm(**kwargs):
        return list(kwargs["candidate_sent_message_ids"])

    async def _react(*args, **kwargs):
        reaction_calls.append((args, kwargs))

    service._confirm_target_delivery_message_ids_with_retry = _confirm  # type: ignore[method-assign]
    service._add_reaction_for_rule_if_possible = _react  # type: ignore[method-assign]
    processed_file = tmp_path / "video.mp4"
    processed_file.write_bytes(b"ok")

    result = asyncio.run(
        service.execute_video_send_from_job(
            delivery_id=13,
            rule_id=51,
            tenant_id=1,
            target_id="-1004",
            message_id=2914,
            processed_video_path=str(processed_file),
            artifact_version=1,
            pipeline_version=1,
        )
    )

    assert result["ok"] is True
    assert len(reaction_calls) == 1
    args, kwargs = reaction_calls[0]
    assert args == ()
    assert kwargs["rule"].id == 1
    assert kwargs["sent_message_id"] == 778
    assert kwargs["target_id"] == "-1004"
    assert kwargs["delivery_id"] == 13


def test_video_send_extracted_log_uses_payload_source_message_ids(tmp_path: Path, caplog):
    repo = FakeRepo()
    service = _build_service(repo)

    async def _confirm(**kwargs):
        return list(kwargs["candidate_sent_message_ids"])

    async def _react(*_args, **_kwargs):
        return None

    service._confirm_target_delivery_message_ids_with_retry = _confirm  # type: ignore[method-assign]
    service._add_reaction_for_rule_if_possible = _react  # type: ignore[method-assign]
    processed_file = tmp_path / "video.mp4"
    processed_file.write_bytes(b"ok")

    with caplog.at_level(logging.INFO, logger="forwarder"):
        result = asyncio.run(
            service.execute_video_send_from_job(
                delivery_id=14,
                rule_id=51,
                tenant_id=1,
                target_id="-1005",
                message_id=2914,
                processed_video_path=str(processed_file),
                artifact_version=1,
                pipeline_version=1,
            )
        )

    assert result["ok"] is True
    messages = [record.getMessage() for record in caplog.records]
    assert any(
        "DELIVERY_SENT_MESSAGE_IDS_EXTRACTED" in message
        and "source_message_ids=[2914]" in message
        for message in messages
    )


def test_video_send_wrapper_success_result_contract(tmp_path: Path):
    repo = FakeRepo()
    service = _build_service(repo)

    async def _confirm(**kwargs):
        return list(kwargs["candidate_sent_message_ids"])

    async def _react(*_args, **_kwargs):
        return None

    service._confirm_target_delivery_message_ids_with_retry = _confirm  # type: ignore[method-assign]
    service._add_reaction_for_rule_if_possible = _react  # type: ignore[method-assign]
    processed_file = tmp_path / "video.mp4"
    processed_file.write_bytes(b"ok")

    result = asyncio.run(
        service.execute_video_send_from_processed_job(
            delivery_id=15,
            rule_id=1,
            tenant_id=1,
            target_id="-1006",
            message_id=2914,
            processed_video_path=str(processed_file),
            artifact_version=1,
            pipeline_version=1,
        )
    )

    assert result["ok"] is True
    assert result["fallback_to_legacy"] is False


def test_video_send_wrapper_failed_send_result_contract(tmp_path: Path):
    repo = FakeRepo()
    service = _build_service(repo)

    async def _failed_send(*_args, **_kwargs):
        return None

    service.video_processor.send_with_retry = _failed_send  # type: ignore[method-assign]
    processed_file = tmp_path / "video.mp4"
    processed_file.write_bytes(b"ok")

    result = asyncio.run(
        service.execute_video_send_from_processed_job(
            delivery_id=16,
            rule_id=1,
            tenant_id=1,
            target_id="-1007",
            message_id=2914,
            processed_video_path=str(processed_file),
            artifact_version=1,
            pipeline_version=1,
        )
    )

    assert result["ok"] is False
    assert result["retryable"] is False
    assert result["accepted"] is True
    assert result["fallback_to_legacy"] is False



def test_video_send_size_error_is_non_retryable_and_not_accepted(tmp_path: Path):
    repo = FakeRepo()
    service = _build_service(repo)

    async def _reject_too_large(*_args, **_kwargs):
        raise TelegramEntityTooLarge(
            method=SendVideo(chat_id=123, video="video_file_id"),
            message="Request Entity Too Large",
        )

    service.video_processor.send_with_retry = _reject_too_large
    processed_file = tmp_path / "video.mp4"
    processed_file.write_bytes(b"video")

    result = asyncio.run(
        service.execute_video_send_from_processed_job(
            delivery_id=17,
            rule_id=1,
            tenant_id=1,
            target_id="-1007",
            processed_video_path=str(processed_file),
            artifact_version=1,
            pipeline_version=1,
        )
    )

    assert result["ok"] is False
    assert result["retryable"] is False
    assert result["fallback_to_legacy"] is False
    assert "TelegramEntityTooLarge" in result["error_text"]
    assert any(call[0] == "failed" for call in repo.calls)
    assert not any(call[0] == "accepted" for call in repo.calls)
