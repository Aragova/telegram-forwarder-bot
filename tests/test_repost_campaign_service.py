import pytest

from app.repost_campaign_service import (
    format_campaign_show_seconds_ru,
    normalize_campaign_show_seconds,
)


def test_normalize_zero():
    assert normalize_campaign_show_seconds(0) == 0


@pytest.mark.parametrize("value", [60, 900, 3600, 7200, 21600, 43200, 86400, 172800])
def test_normalize_presets(value):
    assert normalize_campaign_show_seconds(value) == value


def test_normalize_too_big():
    with pytest.raises(ValueError):
        normalize_campaign_show_seconds(172801)


def test_normalize_unknown():
    with pytest.raises(ValueError):
        normalize_campaign_show_seconds(61)


def test_format_ru():
    assert format_campaign_show_seconds_ru(0) == "выключено"
    assert format_campaign_show_seconds_ru(60) == "1 минута"
    assert format_campaign_show_seconds_ru(3600) == "1 час"
