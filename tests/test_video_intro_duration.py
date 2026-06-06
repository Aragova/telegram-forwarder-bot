import asyncio
from types import SimpleNamespace

from app import video_processor as video_processor_module
from app.video_clip_duration import calculate_center_clip_window
from app.video_processor import VideoProcessor


def _processor(tmp_path, monkeypatch):
    monkeypatch.setattr(video_processor_module.config, "intro_duration", 2)
    processor = VideoProcessor(bot=object())
    processor.temp_dir = str(tmp_path / "temp")
    processor.intros_dir = str(tmp_path / "intros")
    (tmp_path / "temp").mkdir()
    (tmp_path / "intros").mkdir()
    return processor


def test_video_intro_uses_full_ffprobe_duration_not_intro_duration(tmp_path, monkeypatch):
    processor = _processor(tmp_path, monkeypatch)
    intro_path = tmp_path / "intro.mp4"
    intro_path.write_bytes(b"video")

    async def fake_get_video_info(path, use_cache=True):
        assert path == str(intro_path)
        return {"duration": 20.0}

    processor.get_video_info = fake_get_video_info

    effective_intro_duration = asyncio.run(processor._resolve_intro_effective_duration(intro_path))
    main_duration = 60.0

    assert effective_intro_duration == 20.0
    assert main_duration + effective_intro_duration == 80.0


def test_image_intro_uses_config_intro_duration(tmp_path, monkeypatch):
    processor = _processor(tmp_path, monkeypatch)
    intro_path = tmp_path / "intro.jpg"
    intro_path.write_bytes(b"image")

    async def fail_if_called(*_args, **_kwargs):
        raise AssertionError("images must not be probed as video intros")

    processor.get_video_info = fail_if_called

    effective_intro_duration = asyncio.run(processor._resolve_intro_effective_duration(intro_path))
    main_duration = 60.0

    assert effective_intro_duration == 2.0
    assert main_duration + effective_intro_duration == 62.0


def test_custom_main_duration_is_not_reduced_by_video_intro(tmp_path, monkeypatch):
    processor = _processor(tmp_path, monkeypatch)
    intro_path = tmp_path / "intro.mp4"
    intro_path.write_bytes(b"video")

    async def fake_get_video_info(_path, use_cache=True):
        return {"duration": 20.0}

    processor.get_video_info = fake_get_video_info

    clip_window = calculate_center_clip_window(120.0, 45)
    effective_intro_duration = asyncio.run(processor._resolve_intro_effective_duration(intro_path))

    assert clip_window.should_cut is True
    assert clip_window.duration == 45
    assert effective_intro_duration == 20.0
    assert clip_window.duration + effective_intro_duration == 65.0


def test_default_main_duration_is_not_reduced_by_video_intro(tmp_path, monkeypatch):
    processor = _processor(tmp_path, monkeypatch)
    intro_path = tmp_path / "intro.mp4"
    intro_path.write_bytes(b"video")

    async def fake_get_video_info(_path, use_cache=True):
        return {"duration": 20.0}

    processor.get_video_info = fake_get_video_info

    requested_clip = 118
    clip_window = calculate_center_clip_window(600.0, requested_clip)
    effective_intro_duration = asyncio.run(processor._resolve_intro_effective_duration(intro_path))

    assert clip_window.should_cut is True
    assert clip_window.duration == 118
    assert effective_intro_duration == 20.0
    assert clip_window.duration + effective_intro_duration == 138.0


def test_video_intro_duration_falls_back_to_intro_item_then_config(tmp_path, monkeypatch):
    processor = _processor(tmp_path, monkeypatch)
    intro_path = tmp_path / "intro.mp4"
    intro_path.write_bytes(b"video")

    async def failed_ffprobe(_path, use_cache=True):
        return None

    processor.get_video_info = failed_ffprobe

    assert asyncio.run(
        processor._resolve_intro_effective_duration(intro_path, SimpleNamespace(duration=20))
    ) == 20.0
    assert asyncio.run(processor._resolve_intro_effective_duration(intro_path)) == 2.0


def test_video_intro_ffmpeg_command_does_not_trim_video_intro(tmp_path, monkeypatch):
    processor = _processor(tmp_path, monkeypatch)
    source_path = tmp_path / "intro.mp4"
    source_path.write_bytes(b"video")
    output_path = tmp_path / "temp" / "intro_matched_123.mp4"
    captured = {}

    monkeypatch.setattr(video_processor_module.time, "time", lambda: 123)

    async def fake_run_ffmpeg(cmd, *_args, **kwargs):
        captured["cmd"] = cmd
        captured["timeout"] = kwargs.get("timeout")
        output_path.write_bytes(b"processed")
        return True

    processor.run_ffmpeg_with_progress = fake_run_ffmpeg

    result = asyncio.run(
        processor.create_intro_matching_video(
            str(source_path),
            {"target_width": 1280, "target_height": 720, "target_fps": 30},
            20.0,
        )
    )

    assert result == str(output_path)
    assert "-t" not in captured["cmd"]
    assert captured["timeout"] == 120


def test_video_intro_timeout_depends_on_intro_duration(tmp_path, monkeypatch):
    processor = _processor(tmp_path, monkeypatch)
    source_path = tmp_path / "intro.mp4"
    source_path.write_bytes(b"video")
    output_path = tmp_path / "temp" / "intro_matched_123.mp4"
    captured = {}

    monkeypatch.setattr(video_processor_module.time, "time", lambda: 123)

    async def fake_run_ffmpeg(cmd, *_args, **kwargs):
        captured["cmd"] = cmd
        captured["timeout"] = kwargs.get("timeout")
        output_path.write_bytes(b"processed")
        return True

    processor.run_ffmpeg_with_progress = fake_run_ffmpeg

    result = asyncio.run(
        processor.create_intro_matching_video(
            str(source_path),
            {"target_width": 1280, "target_height": 720, "target_fps": 30},
            30.0,
        )
    )

    assert result == str(output_path)
    assert captured["timeout"] == 180
    assert captured["timeout"] > 60


def test_image_intro_ffmpeg_command_keeps_intro_duration_trim(tmp_path, monkeypatch):
    processor = _processor(tmp_path, monkeypatch)
    source_path = tmp_path / "intro.png"
    source_path.write_bytes(b"image")
    output_path = tmp_path / "temp" / "intro_matched_123.mp4"
    captured = {}

    monkeypatch.setattr(video_processor_module.time, "time", lambda: 123)

    async def fake_run_ffmpeg(cmd, *_args, **_kwargs):
        captured["cmd"] = cmd
        output_path.write_bytes(b"processed")
        return True

    processor.run_ffmpeg_with_progress = fake_run_ffmpeg

    result = asyncio.run(
        processor.create_intro_matching_video(
            str(source_path),
            {"target_width": 1280, "target_height": 720, "target_fps": 30},
            2.0,
        )
    )

    assert result == str(output_path)
    assert "-t" in captured["cmd"]
    assert captured["cmd"][captured["cmd"].index("-t") + 1] == "2.0"


def test_video_intro_duration_source_guards():
    source = (video_processor_module.Path(__file__).parents[1] / "app" / "video_processor.py").read_text()

    assert "intro_duration = config.intro_duration" not in source
    assert "_resolve_intro_effective_duration" in source
    assert "effective_intro_duration" in source
    assert "VIDEO_INTRO_DURATION_RESOLVED" in source
    assert "VIDEO_FINAL_DURATION_PLAN" in source
    assert "VIDEO_INTRO_PROCESS_TIMEOUT" in source

    create_intro_source = source.split("async def create_intro_matching_video", 1)[1].split("async def create_thumbnail_fast", 1)[0]
    assert "timeout=60" not in create_intro_source
