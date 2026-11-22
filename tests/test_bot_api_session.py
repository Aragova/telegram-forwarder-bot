from __future__ import annotations

import pytest

from app.bot_api_session import create_bot_api_session


def test_bot_api_session_uses_local_endpoint_and_local_mode():
    session = create_bot_api_session("http://127.0.0.1:8081/")
    assert session.api.is_local is True
    assert session.api.api_url("123456:ABC", "sendVideo") == (
        "http://127.0.0.1:8081/bot123456:ABC/sendVideo"
    )
    assert session.api.file_url("123456:ABC", "video.mp4").startswith(
        "http://127.0.0.1:8081/"
    )


@pytest.mark.parametrize("base", ["", "invalid", "ftp://localhost:8081"])
def test_bot_api_session_rejects_invalid_endpoint(base):
    with pytest.raises(ValueError):
        create_bot_api_session(base)
