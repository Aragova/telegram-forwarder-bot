"""FFmpeg subprocess cleanup must complete when an owning worker is cancelled."""
import asyncio
import sys

import pytest

from app import video_processor as video_module
from app.video_processor import VideoProcessor


def test_ffmpeg_cancel_terminates_child_and_drains_reader_tasks(monkeypatch):
    """Mimic watchdog loss of ownership during a hanging FFmpeg process."""
    original = video_module.asyncio.create_subprocess_exec
    started = asyncio.Event()
    launched = []

    async def fake_ffmpeg_spawn(*command, **kwargs):
        # Run a real, harmless sleeping Python child instead of FFmpeg.
        # The runner appends -progress / -nostats, which Python -c treats as argv.
        proc = await original(
            sys.executable, "-c", "import time; time.sleep(60)", **kwargs
        )
        launched.append(proc)
        started.set()
        return proc

    monkeypatch.setattr(video_module.asyncio, "create_subprocess_exec", fake_ffmpeg_spawn)
    processor = VideoProcessor.__new__(VideoProcessor)

    async def scenario():
        job = asyncio.create_task(
            processor.run_ffmpeg_with_progress(
                ["ffmpeg", "-i", "dummy.mp4"], "cancel regression",
                timeout=600,
            )
        )
        await asyncio.wait_for(started.wait(), timeout=3.0)
        job.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(job, timeout=8.0)
        proc = launched[0]
        assert proc.returncode is not None, "FFmpeg child survived worker cancellation"
        assert await asyncio.wait_for(proc.wait(), timeout=0.5) == proc.returncode
        # Reader/watchdog coroutines must have exited, not outlived the slot.
        leaked = [
            task for task in asyncio.all_tasks()
            if task is not asyncio.current_task()
            and not task.done()
            and getattr(task.get_coro(), "__qualname__", "").endswith(
                ("run_ffmpeg_with_progress.<locals>.read_stdout",
                 "run_ffmpeg_with_progress.<locals>.read_stderr")
            )
        ]
        assert not leaked

    asyncio.run(scenario())


def test_ffmpeg_timeout_terminates_child(monkeypatch):
    original = video_module.asyncio.create_subprocess_exec
    launched = []

    async def spawn(*command, **kwargs):
        proc = await original(
            sys.executable, "-c", "import time; time.sleep(60)", **kwargs
        )
        launched.append(proc)
        return proc

    monkeypatch.setattr(video_module.asyncio, "create_subprocess_exec", spawn)
    processor = VideoProcessor.__new__(VideoProcessor)

    async def scenario():
        result = await asyncio.wait_for(
            processor.run_ffmpeg_with_progress(
                ["ffmpeg", "-i", "dummy.mp4"], "timeout regression",
                timeout=0.05,
            ),
            timeout=8.0,
        )
        assert result is False
        assert launched[0].returncode is not None

    asyncio.run(scenario())



def test_thumbnail_cancel_drains_large_stderr_until_reaped(tmp_path, monkeypatch):
    """SIGTERM handler fills stderr; cancelled communicate must continue draining."""
    import os
    import textwrap

    original = video_module.asyncio.create_subprocess_exec
    children = []
    ready = tmp_path / "ffmpeg_ready.flag"
    program = textwrap.dedent(
        f"""
        import pathlib, signal, sys, time
        def terminate(signum, frame):
            sys.stderr.buffer.write(b'X' * 2_000_000)
            sys.stderr.buffer.flush()
            sys.exit(1)
        signal.signal(signal.SIGTERM, terminate)
        pathlib.Path({str(ready)!r}).write_text('ready')
        while True:
            time.sleep(0.2)
        """
    )

    async def launch(*command, **kwargs):
        child = await original(sys.executable, "-c", program, **kwargs)
        children.append(child)
        return child

    monkeypatch.setattr(video_module.asyncio, "create_subprocess_exec", launch)
    processor = VideoProcessor.__new__(VideoProcessor)
    video = tmp_path / "video.mp4"
    video.write_bytes(b"fake")
    preview = tmp_path / "thumb.jpg"

    async def check():
        task = asyncio.create_task(
            processor.create_thumbnail_fast(str(video), str(preview))
        )
        for _ in range(100):
            if ready.exists():
                break
            await asyncio.sleep(0.02)
        assert ready.exists(), "Subprocess never registered SIGTERM handler"
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=7.0)
        assert children[0].returncode is not None

    asyncio.run(check())



@pytest.mark.parametrize("operation", ["runner", "thumbnail"])
def test_second_cancellation_cannot_orphan_ffmpeg(tmp_path, monkeypatch, operation):
    """A graceful-shutdown cancel during SIGTERM grace must still kill and reap FFmpeg."""
    import textwrap
    from pathlib import Path

    original_spawn = video_module.asyncio.create_subprocess_exec
    ready = tmp_path / f"ready_{operation}.flag"
    program = textwrap.dedent(
        f"""
        import pathlib, signal, time
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        pathlib.Path({str(ready)!r}).write_text('ready')
        while True:
            time.sleep(0.05)
        """
    )
    children = []

    async def launch(*args, **kwargs):
        proc = await original_spawn(sys.executable, "-c", program, **kwargs)
        children.append(proc)
        return proc

    monkeypatch.setattr(video_module.asyncio, "create_subprocess_exec", launch)
    processor = VideoProcessor.__new__(VideoProcessor)

    async def scenario():
        termination_started = asyncio.Event()
        real_terminate = processor._terminate_ffmpeg_process

        async def quick_terminate(proc):
            termination_started.set()
            return await real_terminate(proc, grace_seconds=0.25)

        processor._terminate_ffmpeg_process = quick_terminate
        if operation == "runner":
            task = asyncio.create_task(
                processor.run_ffmpeg_with_progress(
                    ["ffmpeg", "-i", "fake.mp4"], "double cancel regression",
                    timeout=600,
                )
            )
        else:
            video = tmp_path / "source.mp4"
            video.write_bytes(b"dummy")
            task = asyncio.create_task(
                processor.create_thumbnail_fast(str(video), str(tmp_path / "thumb.jpg"))
            )
        for _ in range(150):
            if ready.exists():
                break
            await asyncio.sleep(0.01)
        assert ready.exists(), "Subprocess did not install SIGTERM ignore handler"
        task.cancel()
        await asyncio.wait_for(termination_started.wait(), timeout=2.0)
        task.cancel()  # second cancellation while the SIGTERM grace wait is active
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=3.0)
        assert children[0].returncode is not None, "Child survived repeated cancellation"
        assert await asyncio.wait_for(children[0].wait(), timeout=0.5) == children[0].returncode

    asyncio.run(scenario())
