"""Real FFmpeg/FFprobe regressions: do not trust nominal VFR metadata alone."""
import asyncio
import shutil
import subprocess

import pytest

from app.video_processor import VideoProcessor


pytestmark = pytest.mark.skipif(
    not shutil.which("ffmpeg") or not shutil.which("ffprobe"),
    reason="FFmpeg and FFprobe required",
)


@pytest.fixture
def processor(tmp_path):
    result = VideoProcessor(bot=object())
    result.temp_dir = str(tmp_path)
    result.intros_dir = str(tmp_path)
    result.min_free_space_gb = 0
    result.target_fps = 30
    return result


def _source(path, rate, *, vfr=False):
    cmd = [
        "ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
        "-f", "lavfi", "-i", f"testsrc2=size=244x432:rate={rate}:duration=2",
    ]
    if vfr:
        # A nominal 29.97 fps stream with much lower average cadence.
        cmd += ["-vf", "select=eq(n\\,1)+not(mod(n\\,2))", "-fps_mode", "vfr"]
    cmd += ["-c:v", "libx264", "-threads", "1", "-pix_fmt", "yuv420p", str(path)]
    subprocess.run(cmd, check=True, timeout=30, capture_output=True)


@pytest.mark.parametrize("rate,vfr,target", [("30000/1001", True, 30), ("15", False, 15)])
def test_normalize_real_video_enforces_selected_profile(tmp_path, processor, rate, vfr, target):
    source = tmp_path / "source.mp4"
    output = tmp_path / "normalized.mp4"
    _source(source, rate, vfr=vfr)

    async def run():
        info = await processor.get_video_info(str(source), use_cache=False)
        profile = processor._get_processing_profile(info)
        assert profile["target_fps"] == target
        if vfr:
            assert info["fps_normalized"] == 30
            assert info["fps_avg"] < 20
            assert not processor._matches_target_fps(info, target)
        assert await processor.normalize_video(
            str(source), str(output), profile, False, info["duration"],
        )
        result = await processor.get_video_info(str(output), use_cache=False)
        assert result["width"] == 720 and result["height"] == 1280
        assert result["fps"] == target
        assert result["fps_avg"] == target
        assert abs(result["duration"] - info["duration"]) < 0.2

    asyncio.run(run())


def test_accurate_cut_and_intro_concat_real_ffmpeg(tmp_path, processor):
    source = tmp_path / "source.mp4"
    intro_source = tmp_path / "intro.mp4"
    cut = tmp_path / "cut.mp4"
    output = tmp_path / "joined.mp4"
    _source(source, "30000/1001", vfr=True)
    _source(intro_source, "15")

    async def run():
        info = await processor.get_video_info(str(source), use_cache=False)
        profile = {"target_width": 244, "target_height": 432, "target_fps": 30}
        assert await processor.cut_video_accurate(
            str(source), str(cut), 0.1, 1.0, info, profile,
        )
        cut_info = await processor.get_video_info(str(cut), use_cache=False)
        assert cut_info["fps"] == 30 and cut_info["fps_avg"] == 30
        intro = await processor.create_intro_matching_video(
            str(intro_source), profile, 2.0,
        )
        assert intro
        intro_info = await processor.get_video_info(intro, use_cache=False)
        assert intro_info["fps"] == 30 and intro_info["fps_avg"] == 30
        assert await processor.concat_videos_safe(
            intro, str(cut), str(output), profile, False, expected_duration=3.0,
        )
        joined = await processor.get_video_info(str(output), use_cache=False)
        assert joined["fps"] == 30 and joined["fps_avg"] == 30
        assert abs(joined["duration"] - (intro_info["duration"] + cut_info["duration"])) < 0.2

    asyncio.run(run())


@pytest.mark.parametrize("raw,expected", [("30000/1001", 29.97002997002997),
    ("15/1", 15), ("0/0", None), ("N/A", None), (None, None), ("-1/1", None)])
def test_ffprobe_rate_parser(raw, expected):
    assert VideoProcessor._parse_frame_rate(raw) == expected


def test_full_processing_preserves_short_vfr_source(tmp_path, processor):
    """Exercise the rule #213 build-only path, not just isolated commands."""
    source = tmp_path / "source.mp4"
    _source(source, "30000/1001", vfr=True)

    async def run():
        before = await processor.get_video_info(str(source), use_cache=False)
        artifact = await processor.build_processed_video(
            input_file_path=str(source), clip_duration_seconds=118,
        )
        assert artifact
        assert artifact["trim_applied"] is False
        assert artifact["processing_summary"]["target_fps"] == 30
        final = await processor.get_video_info(
            artifact["processed_video_path"], use_cache=False,
        )
        assert final["fps"] == 30 and final["fps_avg"] == 30
        assert abs(final["duration"] - before["duration"]) < 0.2
        assert source.is_file()

    asyncio.run(run())
