from app.video_clip_duration import (
    calculate_center_clip_window,
    is_video_clip_duration_in_bounds,
    parse_video_clip_duration_input,
)


def test_parse_video_clip_duration_valid():
    assert parse_video_clip_duration_input("60") == 60
    assert parse_video_clip_duration_input("118") == 118
    assert parse_video_clip_duration_input("1:00") == 60
    assert parse_video_clip_duration_input("1:30") == 90
    assert parse_video_clip_duration_input("01:05") == 65
    assert parse_video_clip_duration_input("2:00") == 120
    assert parse_video_clip_duration_input("10:00") == 600


def test_parse_video_clip_duration_invalid():
    assert parse_video_clip_duration_input("") is None
    assert parse_video_clip_duration_input("abc") is None
    assert parse_video_clip_duration_input("1:2:3") is None
    assert parse_video_clip_duration_input("1:xx") is None
    assert parse_video_clip_duration_input("-10") is None
    assert parse_video_clip_duration_input("0") == 0


def test_video_clip_duration_bounds():
    assert not is_video_clip_duration_in_bounds(9)
    assert is_video_clip_duration_in_bounds(10)
    assert is_video_clip_duration_in_bounds(600)
    assert not is_video_clip_duration_in_bounds(601)


def test_calculate_center_clip_window():
    result = calculate_center_clip_window(600, 60)
    assert result.should_cut is True
    assert result.start_time == 270
    assert result.duration == 60

    result = calculate_center_clip_window(120, 60)
    assert result.should_cut is True
    assert result.start_time == 30
    assert result.duration == 60

    assert calculate_center_clip_window(80, 120).should_cut is False
    assert calculate_center_clip_window(118, 118).should_cut is False

    result = calculate_center_clip_window(119, 118)
    assert result.should_cut is True
    assert result.start_time == 0.5
    assert result.duration == 118
