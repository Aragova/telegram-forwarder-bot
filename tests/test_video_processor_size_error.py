from __future__ import annotations

import asyncio

import pytest
from aiogram.exceptions import TelegramEntityTooLarge
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
