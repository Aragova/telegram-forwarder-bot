from __future__ import annotations

import asyncio
import threading
from pathlib import Path

import pytest

from app import sender_telethon_helpers as thumbs


def test_cancel_during_temp_creation_removes_late_file(monkeypatch, tmp_path: Path) -> None:
    started = threading.Event()
    release = threading.Event()
    created = threading.Event()
    image = tmp_path / "late-thumb.jpg"

    def slow_creator(prefix: str) -> Path:
        started.set()
        if not release.wait(timeout=3):
            raise TimeoutError("test creator was not released")
        image.write_bytes(b"temporary image")
        created.set()
        return image

    monkeypatch.setattr(thumbs, "_create_thumb_temp_path", slow_creator)

    async def scenario() -> None:
        outer = asyncio.create_task(thumbs._create_thumb_temp_path_async("test_"))
        assert await asyncio.to_thread(started.wait, 2)
        outer.cancel()
        with pytest.raises(asyncio.CancelledError):
            await outer
        release.set()
        assert await asyncio.to_thread(created.wait, 2)
        for _ in range(50):
            if not image.exists():
                break
            await asyncio.sleep(0.01)
        assert not image.exists(), "late-created thumbnail leaked after cancellation"

    asyncio.run(scenario())
