"""Repost caption must not change a rule's original-copy behavior when OFF."""
from __future__ import annotations

import asyncio
import json
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest
from telethon.tl.types import MessageEntityBold, MessageEntityCustomEmoji

from app.repost_caption import caption_override_content, get_repost_caption_override
from app.sender_content_helpers import SenderContentHelpers
from app.sender_reupload_helpers import SenderReuploadHelpers
from app.postgres_repository import PostgresRepository
from app import user_ui


def _rule(**changes):
    data = {
        "mode": "repost",
        "repost_caption": None,
        "repost_caption_entities_json": None,
        "repost_caption_enabled": False,
        "repost_caption_delivery_mode": "copy_first",
    }
    data.update(changes)
    return SimpleNamespace(**data)


@pytest.mark.parametrize("rule", [
    _rule(),
    _rule(repost_caption="Original untouched", repost_caption_enabled=False),
    _rule(repost_caption="", repost_caption_enabled=True),
    _rule(mode="video", repost_caption="Hidden from video", repost_caption_enabled=True),
])
def test_no_override_uses_original_text(rule):
    assert get_repost_caption_override(rule) is None
    assert caption_override_content(get_repost_caption_override(rule)) is None


def test_simple_active_caption_replaces_original_without_entities():
    rule = _rule(
        repost_caption="My own caption 🔥",
        repost_caption_enabled=True,
        repost_caption_delivery_mode="copy_first",
        repost_caption_entities_json='[{"type":"bold","offset":0,"length":2}]',
    )
    assert caption_override_content(get_repost_caption_override(rule)) == {
        "text": "My own caption 🔥",
        "entities": [],
    }


def test_premium_caption_reuses_telegram_entity_builder():
    serialized = json.dumps([
        {"type":"bold","offset":0,"length":5},
        {"type":"custom_emoji","offset":6,"length":2,"custom_emoji_id":"123456789"},
    ])
    rule = _rule(
        repost_caption="Hello 🔥",
        repost_caption_enabled=True,
        repost_caption_delivery_mode="builder_first",
        repost_caption_entities_json=serialized,
    )
    content = caption_override_content(get_repost_caption_override(rule))
    text, entities = SenderContentHelpers(owner=None).build_text_and_entities_from_content(content)
    assert text == "Hello 🔥"
    assert len(entities) == 2
    assert isinstance(entities[0], MessageEntityBold)
    assert isinstance(entities[1], MessageEntityCustomEmoji)
    assert entities[1].document_id == 123456789


@pytest.mark.parametrize("caption,enabled,expected", [
    ("Saved", True, True),
    ("Saved", False, False),
    (None, False, False),
])
def test_custom_caption_enabled_guard(caption, enabled, expected):
    value = get_repost_caption_override(_rule(
        repost_caption=caption, repost_caption_enabled=enabled,
    ))
    assert bool(value) is expected


class _Cursor:
    rowcount = 1

    def __init__(self):
        self.queries = []

    def execute(self, sql, args):
        self.queries.append((" ".join(str(sql).split()), args))


class _Conn:
    def __init__(self):
        self.cur = _Cursor()
        self.commits = 0

    @contextmanager
    def cursor(self):
        yield self.cur

    def commit(self):
        self.commits += 1


def _repo():
    repo = object.__new__(PostgresRepository)
    conn = _Conn()
    @contextmanager
    def connection():
        yield conn
    repo.connect = connection
    return repo, conn


def test_save_enables_caption_and_clear_restores_original():
    repo, conn = _repo()
    assert repo.update_rule_repost_caption(214, "Hello", "[]") is True
    sql, args = conn.cur.queries[-1]
    assert "repost_caption_enabled = %s" in sql
    assert "mode = 'repost'" in sql
    assert args == ("Hello", "[]", True, 214)

    assert repo.update_rule_repost_caption(214, None, None) is True
    sql, args = conn.cur.queries[-1]
    assert args == (None, None, False, 214)


def test_switch_off_preserves_saved_text_for_reenable():
    repo, conn = _repo()
    assert repo.set_rule_repost_caption_enabled(214, False)
    sql, args = conn.cur.queries[-1]
    assert "SET repost_caption_enabled = %s" in sql
    assert "SET repost_caption = " not in sql
    assert args == (False, 214, False)
    assert repo.set_rule_repost_caption_enabled(214, True)
    assert conn.cur.queries[-1][1] == (True, 214, True)


def test_caption_rejects_overlong_input_before_postgres_write():
    repo, conn = _repo()
    assert not repo.update_rule_repost_caption(214, "🔥" * 600)
    assert conn.cur.queries == []


@pytest.mark.parametrize("mode,expected", [
    ("copy_first", True), ("builder_first", True),
    ("auto", False), ("unknown", False),
])
def test_custom_mode_is_separate_from_original_repost_mode(mode, expected):
    repo, conn = _repo()
    assert repo.update_rule_repost_caption_delivery_mode(214, mode) is expected
    if expected:
        assert "repost_caption_delivery_mode" in conn.cur.queries[-1][0]
        assert "SET caption_delivery_mode = " not in conn.cur.queries[-1][0]


def test_repost_card_gets_caption_button_like_video_editor():
    reply = user_ui.build_user_rule_card_keyboard(
        rule_id=214, is_active=True, schedule_mode="interval", mode="repost"
    )
    callbacks = [b.callback_data for row in reply.inline_keyboard for b in row]
    assert "user_rule_repost_caption:214" in callbacks
    assert "user_rule_caption:214" not in callbacks
    extra = user_ui.build_user_rule_extra_keyboard(rule_id=214, mode="repost")
    assert not any(
        b.text == "✍️ Режим подписи" for row in extra.inline_keyboard for b in row
    )


def test_album_telethon_send_receives_override_without_copy():
    class Owner:
        def __init__(self):
            self.captured = []
            self.bot = None
        async def _send_album_via_telethon(self, **kwargs):
            self.captured.append(kwargs)
            return {
                "ok": True, "sent_message_id": 501,
                "sent_message_ids": [501, 502, 503], "sent_count": 3,
                "error_text": None,
            }
    owner = Owner()
    caption = {"text": "New caption", "entities": []}
    messages = [SimpleNamespace(id=10+i) for i in range(3)]
    result = asyncio.run(SenderReuploadHelpers(owner).reupload_album(
        messages, "-1001", None, caption_override=caption,
    ))
    assert result["ok"]
    assert owner.captured[0]["caption_override"] == caption


def test_repost_sender_never_uses_copy_when_caption_active():
    source = Path("app/repost_single_delivery.py").read_text(encoding="utf-8")
    album = Path("app/repost_album_delivery.py").read_text(encoding="utf-8")
    for code in (source, album):
        assert "get_repost_caption_override(rule)" in code
        assert "if custom_caption is not None:" in code
        assert "use_copy_first = False" in code
        assert "caption_override" in code


def test_telethon_album_content_resolver_uses_custom_caption_in_both_paths():
    source = Path("app/sender_telethon_helpers.py").read_text(encoding="utf-8")
    section = source.split("async def send_album_via_telethon(", 1)[1].split(
        "async def verify_album_delivery(", 1
    )[0]
    assert section.count("self.owner._build_text_and_entities_from_content(caption_override)") == 2
    assert section.count('"formatting_entities": formatting_entities or None') == 2
