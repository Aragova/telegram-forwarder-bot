from __future__ import annotations

import asyncio

import pytest
from aiogram.exceptions import TelegramEntityTooLarge, TelegramRetryAfter
from telethon.errors import FloodWaitError
from aiogram.methods import SendVideo

from app.video_processor import VideoProcessor
from app.transport import wrap_bot
from app.transport_policy import TransportRateLimited, build_sender_bot_policy


def test_send_with_retry_does_not_repeat_413(tmp_path):
    class RejectingBot:
        calls = 0

        async def send_video(self, **_kwargs):
            self.calls += 1
            raise TelegramEntityTooLarge(
                method=SendVideo(chat_id=123, video="video_file_id"),
                message="Request Entity Too Large",
            )

    processor = VideoProcessor.__new__(VideoProcessor)
    video = tmp_path / "processed.mp4"
    video.write_bytes(b"video")
    bot = RejectingBot()

    with pytest.raises(TelegramEntityTooLarge):
        asyncio.run(
            processor.send_with_retry(
                bot,
                destination_channel=-100123,
                target_thread_id=None,
                video_path=str(video),
                thumbnail_path=None,
                caption="",
                duration=10,
                max_retries=3,
            )
        )

    assert bot.calls == 1


@pytest.mark.parametrize("wrapped", [False, True])
def test_send_with_retry_propagates_rejected_429_without_second_post(tmp_path, wrapped):
    class ThrottledBot:
        calls = 0

        async def send_video(self, **_kwargs):
            self.calls += 1
            raise TelegramRetryAfter(
                method=SendVideo(chat_id=123, video="video_id"),
                message="Too Many Requests", retry_after=21,
            )

    processor = VideoProcessor.__new__(VideoProcessor)
    clip = tmp_path / "clip.mp4"
    clip.write_bytes(b"video")
    bot = ThrottledBot()

    transport = wrap_bot(bot, label="sender.bot", policy=build_sender_bot_policy()) if wrapped else bot
    expected_error = TransportRateLimited if wrapped else TelegramRetryAfter
    with pytest.raises(expected_error) as err:
        asyncio.run(processor.send_with_retry(
            transport, destination_channel=-100123, target_thread_id=None,
            video_path=str(clip), thumbnail_path=None,
            caption="", duration=10, max_retries=3,
        ))
    assert (err.value.retry_after_seconds if wrapped else err.value.retry_after) == 21
    assert bot.calls == 1


def test_send_with_retry_propagates_telethon_floodwait(tmp_path):
    processor = VideoProcessor.__new__(VideoProcessor)
    clip = tmp_path / "clip.mp4"
    clip.write_bytes(b"video")

    class ThrottledTelethon:
        calls = 0

        async def send_file(self, **_kwargs):
            self.calls += 1
            raise FloodWaitError(request=None, capture=120)

        async def get_entity(self, _):
            return type("Target", (), {"broadcast": True, "megagroup": False})()

        async def get_messages(self, _entity, ids=None, limit=None):
            return []

    client = ThrottledTelethon()
    processor.telethon = client

    async def fake_video_info(*_args, **_kwargs):
        return {"width": 720, "height": 1280, "duration": 30}

    processor.get_video_info = fake_video_info

    with pytest.raises(FloodWaitError) as err:
        asyncio.run(processor.send_with_retry(
            None, destination_channel=-100123, target_thread_id=None,
            video_path=str(clip), thumbnail_path=None,
            caption="", duration=10,
            caption_send_mode="premium", max_retries=3,
        ))
    assert err.value.seconds == 120
    assert client.calls == 1


def test_telethon_missing_ffprobe_metadata_is_pre_send_retryable(tmp_path):
    from app.video_processor import VideoPreSendError

    processor = VideoProcessor.__new__(VideoProcessor)
    processor.telethon = object()
    clip = tmp_path / "processed.mp4"
    clip.write_bytes(b"video")

    async def fake_missing_info(*_args, **_kwargs):
        return None

    processor.get_video_info = fake_missing_info
    with pytest.raises(VideoPreSendError, match="video_info"):
        asyncio.run(processor.send_with_retry(
            None, destination_channel=-100123, target_thread_id=None,
            video_path=str(clip), thumbnail_path=None,
            caption="", duration=10, caption_send_mode="premium", max_retries=3,
        ))

def test_telethon_rate_limit_after_send_is_unknown_acceptance_not_rejection(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from app.telethon_authoritative_resolver import TelethonAuthoritativeMessageResolver

    processor = VideoProcessor.__new__(VideoProcessor)
    clip = tmp_path / "clip.mp4"
    clip.write_bytes(b"video")

    class Client:
        calls = 0

        async def send_file(self, **_kwargs):
            self.calls += 1
            return SimpleNamespace(id=123)

        async def get_entity(self, _):
            return SimpleNamespace(broadcast=True, megagroup=False)

        async def get_messages(self, _entity, ids=None, limit=None):
            return []

    client = Client()
    processor.telethon = client

    async def fake_video_info(*_args, **_kwargs):
        return {"width": 720, "height": 1280, "duration": 30}

    async def rate_limited(*_args, **_kwargs):
        raise TransportRateLimited(
            retry_after_seconds=21, backend="telethon",
            op_name="get_messages", key="sender.telethon.get_messages",
        )

    processor.get_video_info = fake_video_info
    monkeypatch.setattr(TelethonAuthoritativeMessageResolver, "resolve_authoritative_single_message", rate_limited)
    result = asyncio.run(processor.send_with_retry(
        None, destination_channel=-100123, target_thread_id=None,
        video_path=str(clip), thumbnail_path=None,
        caption="", duration=10, caption_send_mode="premium",
    ))
    assert result.transport_accepted
    assert not result.authoritative_resolved
    assert result.returned_candidate_ids == [123]
    assert client.calls == 1


def test_document_invalid_propagates_without_repeat_and_logs_metadata(tmp_path, caplog, monkeypatch):
    from types import SimpleNamespace
    from telethon.errors import DocumentInvalidError

    class RejectingTelethon:
        calls = 0

        async def get_entity(self, _):
            return SimpleNamespace(broadcast=True, megagroup=False)

        async def get_messages(self, _entity, ids=None, limit=None):
            return []

        async def send_file(self, **_kwargs):
            self.calls += 1
            raise DocumentInvalidError(request=None)

    checks = []

    async def decode(path, *, thumbnail=False):
        checks.append((path, thumbnail))
        return {"status": "decoded" if not thumbnail else "decode_failed"}

    monkeypatch.setattr("app.video_processor.decode_artifact", decode)
    processor = VideoProcessor.__new__(VideoProcessor)
    client = RejectingTelethon()
    processor.telethon = client

    async def video_info(*_args, **_kwargs):
        return {"width": 720, "height": 1280, "duration": 30, "fps": 30, "codec_name": "h264"}

    processor.get_video_info = video_info
    clip = tmp_path / "clip.mp4"
    clip.write_bytes(b"video")
    thumb = tmp_path / "thumb.jpg"
    thumb.write_bytes(b"thumb")
    with pytest.raises(DocumentInvalidError):
        asyncio.run(processor.send_with_retry(
            None, destination_channel=-100123, target_thread_id=None,
            video_path=str(clip), thumbnail_path=str(thumb), caption="private",
            duration=30, caption_send_mode="premium", max_retries=3,
        ))
    assert client.calls == 1
    assert "VIDEO_SEND_FAILURE_SNAPSHOT" in caplog.text
    assert "'error_type': 'DocumentInvalidError'" in caplog.text
    assert "'codec_name': 'h264'" in caplog.text
    assert "'video_artifact': {'provided': True, 'bytes': 5" in caplog.text
    assert "'thumbnail_artifact': {'provided': True, 'bytes': 5" in caplog.text
    assert checks == [(str(clip), False), (str(thumb), True)]
    assert "'video_decode': {'status': 'decoded'}" in caplog.text
    assert "'thumbnail_decode': {'status': 'decode_failed'}" in caplog.text
    assert "private" not in caplog.text
