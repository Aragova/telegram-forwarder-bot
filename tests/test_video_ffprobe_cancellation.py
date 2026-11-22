"""All video ffprobe call sites must clean up subprocesses on cancellation."""
import asyncio
import sys
from pathlib import Path

import pytest

import app.video_processor as video_mod
from app.video_pipeline_stages import VideoPipelineStages
from app.sender_telethon_helpers import SenderTelethonHelpers
from app.video_processor import VideoProcessor
from app.subprocess_safety import communicate_safely


@pytest.mark.parametrize("caller", ["metadata", "validate"])
def test_cancelled_ffprobe_is_reaped_before_worker_slot_is_freed(tmp_path, monkeypatch, caller):
    original_spawn = asyncio.create_subprocess_exec
    started = asyncio.Event()
    processes = []

    async def fake_ffprobe(*args, **kwargs):
        proc = await original_spawn(
            sys.executable, "-c", "import time; time.sleep(60)", **kwargs
        )
        processes.append(proc)
        started.set()
        return proc

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_ffprobe)
    file = tmp_path / "video.mp4"
    file.write_bytes(b"not a real MP4")

    async def scenario():
        if caller == "metadata":
            processor = VideoProcessor.__new__(VideoProcessor)
            processor.video_info_cache = {}
            processor.CACHE_TTL = 3600
            running = asyncio.create_task(processor.get_video_info(str(file), use_cache=False))
        else:
            stages = VideoPipelineStages(object())
            running = asyncio.create_task(stages.validate_mp4_file_for_pipeline(
                file, delivery_id=77, job_id=92, stage="download",
            ))
        await asyncio.wait_for(started.wait(), timeout=3.0)
        running.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(running, timeout=8.0)
        assert processes[0].returncode is not None
        assert await asyncio.wait_for(processes[0].wait(), timeout=0.5) == processes[0].returncode

    asyncio.run(scenario())


def test_probe_timeout_kills_and_reaps_child():
    async def scenario():
        proc = await asyncio.create_subprocess_exec(
            sys.executable, "-c", "import time; time.sleep(60)",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        with pytest.raises(asyncio.TimeoutError):
            await communicate_safely(proc, timeout_seconds=0.01)
        assert proc.returncode is not None

    asyncio.run(scenario())



@pytest.mark.parametrize("caller", ["reupload_probe", "generated_thumbnail"])
def test_telethon_video_probe_and_thumbnail_cleanup_on_cancel(tmp_path, monkeypatch, caller):
    original_spawn = asyncio.create_subprocess_exec
    started = asyncio.Event()
    children = []

    async def fake_spawn(*args, **kwargs):
        proc = await original_spawn(
            sys.executable, "-c", "import time; time.sleep(60)", **kwargs
        )
        children.append(proc)
        started.set()
        return proc

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_spawn)
    source = tmp_path / "video.mp4"
    source.write_bytes(b"dummy")
    helper = SenderTelethonHelpers(owner=object())

    async def scenario():
        if caller == "reupload_probe":
            task = asyncio.create_task(helper._probe_video_file(source))
        else:
            task = asyncio.create_task(helper._generate_video_thumb(source, 10))
        await asyncio.wait_for(started.wait(), timeout=3)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=8)
        assert children[0].returncode is not None

    asyncio.run(scenario())
