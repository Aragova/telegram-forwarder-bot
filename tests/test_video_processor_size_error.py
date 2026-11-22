from __future__ import annotations

import asyncio

import pytest
from aiogram.exceptions import TelegramEntityTooLarge, TelegramRetryAfter
from aiogram.methods import SendVideo

from app.video_processor import VideoProcessor


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


def test_send_with_retry_propagates_rejected_429_without_second_post(tmp_path):
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

    with pytest.raises(TelegramRetryAfter) as err:
        asyncio.run(processor.send_with_retry(
            bot, destination_channel=-100123, target_thread_id=None,
            video_path=str(clip), thumbnail_path=None,
            caption="", duration=10, max_retries=3,
        ))
    assert err.value.retry_after == 21
    assert bot.calls == 1
