from __future__ import annotations

import pytest

from aiogram.methods import GetMe
from aiogram.types import BotFullInfo

from app.bot_api_session import create_bot_api_session
from app.user_ui import build_button


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


def test_aiogram_get_me_uses_bot_api_10_5_bot_full_info():
    # Bot API 10.5 changed getMe from User to BotFullInfo. The target
    # permission checks rely on the bot ID, which must remain available.
    assert GetMe.__returning__ is BotFullInfo
    payload = {
        "id": 8709166092,
        "is_bot": True,
        "first_name": "ViMi",
        "username": "vimi_test_bot",
        "can_join_groups": True,
        "can_read_all_group_messages": False,
        "supports_guest_queries": False,
        "supports_inline_queries": False,
        "needs_inline_query_location": False,
        "can_connect_to_business": False,
        "has_main_web_app": False,
        "has_topics_enabled": False,
        "allows_users_to_create_topics": False,
        "has_topic_sidebar": False,
        "can_manage_bots": False,
        "supports_join_request_queries": False,
        "active_usernames": ["vimi_test_bot"],
    }
    bot_info = BotFullInfo.model_validate(payload)
    assert bot_info.id == payload["id"]
    assert bot_info.username == payload["username"]
    assert bot_info.active_usernames == ["vimi_test_bot"]


@pytest.mark.parametrize("style", ["primary", "success", "danger"])
def test_aiogram_styled_inline_button_serialization(style):
    # Keep styled action buttons in menus working with aiogram 3.34.
    button = build_button(text="Test", callback_data="test", style=style)
    payload = button.model_dump(mode="json", exclude_none=True)
    assert payload["style"] == style
    assert payload["callback_data"] == "test"
