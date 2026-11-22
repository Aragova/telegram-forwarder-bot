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



def test_album_telethon_uses_custom_caption_and_entities_not_source(monkeypatch):
    """Media send and authoritative resolver must see the replacement text."""
    from app import sender_telethon_helpers as module
    class Client:
        def __init__(self):
            self.sent = None
        async def send_file(self, **kwargs):
            self.sent = kwargs
            return [SimpleNamespace(id=701), SimpleNamespace(id=702)]
    class Resolver:
        def __init__(self, _client):
            pass
        async def get_before_max_message_id(self, *_args):
            return 700
        async def resolve_authoritative_album_messages(self, **kwargs):
            assert kwargs["expected_text"] == "My album 🔥"
            return SimpleNamespace(
                ok=True, authoritative_message_id=701,
                authoritative_message_ids=[701, 702],
                returned_candidate_ids=[701, 702], resolution_method="direct",
            )
    monkeypatch.setattr(module, "TelethonAuthoritativeMessageResolver", Resolver)
    client = Client()
    content_helper = SenderContentHelpers(owner=None)
    owner = SimpleNamespace(
        telethon=client,
        _content_from_message_or_post=lambda **_kwargs: {"text": "Original", "entities": []},
        _build_text_and_entities_from_content=content_helper.build_text_and_entities_from_content,
        _clone_telethon_entities=content_helper.clone_telethon_entities,
    )
    sender = module.SenderTelethonHelpers(owner)
    async def target(_id):
        return "-100123", True
    sender._resolve_target_entity_and_broadcast_flag = target
    override = {"text": "My album 🔥", "entities": [{"type": "bold", "offset": 0, "length": 2}]}
    messages = [
        SimpleNamespace(id=1, media="media-1"),
        SimpleNamespace(id=2, media="media-2"),
    ]
    result = asyncio.run(sender.send_album_via_telethon(
        messages=messages, target_id="-100123", target_thread_id=None,
        caption_override=override,
    ))
    assert result["ok"]
    assert client.sent["file"] == ["media-1", "media-2"]
    assert client.sent["caption"] == "My album 🔥"
    assert isinstance(client.sent["formatting_entities"][0], MessageEntityBold)


def test_custom_text_send_has_caption_instead_of_source():
    from app.sender_reupload_helpers import SenderReuploadHelpers
    class Owner:
        def __init__(self):
            self.called = None
        def _content_from_message_or_post(self, **kwargs):
            return {"text": "OLD TEXT", "entities": []}
        def _build_text_and_entities_from_content(self, content):
            return SenderContentHelpers(owner=None).build_text_and_entities_from_content(content)
        async def _send_text_via_telethon(self, **kwargs):
            self.called = kwargs
            return 801
    owner = Owner()
    msg = SimpleNamespace(id=123, media=None)
    override = {"text": "NEW TEXT", "entities": [{"type": "bold", "offset": 0, "length": 3}]}
    result = asyncio.run(SenderReuploadHelpers(owner).reupload_message(
        msg, "-1001", None, caption_override=override,
    ))
    assert result.authoritative_message_id == 801
    assert owner.called["text"] == "NEW TEXT"
    assert isinstance(owner.called["entities"][0], MessageEntityBold)


def test_bot_api_entities_are_normalized_without_losing_identity():
    from app.delivery_content_helpers import normalize_caption_entities
    entities = normalize_caption_entities([
        {"type": "strikethrough", "offset": 0, "length": 3},
        {"type": "expandable_blockquote", "offset": 0, "length": 4},
        {"type": "text_mention", "offset": 5, "length": 4, "user_id": 123456},
    ])
    assert entities == [
        {"type": "strike", "offset": 0, "length": 3},
        {"type": "expandable_blockquote", "offset": 0, "length": 4},
        {"type": "text_link", "offset": 5, "length": 4, "url": "tg://user?id=123456"},
    ]


def test_premium_telethon_and_botapi_entities_survive_roundtrip():
    from telethon.tl import types as tl
    from app.delivery_content_helpers import normalize_caption_entities
    from app.sender_primitives import _telethon_entities_to_bot
    text = "Abcd text user"
    content = {"text": text, "entities": normalize_caption_entities([
        {"type": "strikethrough", "offset": 0, "length": 4},
        {"type": "spoiler", "offset": 5, "length": 4},
        {"type": "expandable_blockquote", "offset": 0, "length": 9},
        {"type": "text_mention", "offset": 10, "length": 4, "user_id": 123456},
        {"type": "pre", "offset": 0, "length": 4, "language": "python"},
    ])}
    raw_text, telethon_entities = SenderContentHelpers(owner=None).build_text_and_entities_from_content(content)
    assert raw_text == text
    assert any(isinstance(e, tl.MessageEntityStrike) for e in telethon_entities)
    assert any(isinstance(e, tl.MessageEntitySpoiler) for e in telethon_entities)
    assert any(isinstance(e, tl.MessageEntityBlockquote) and e.collapsed for e in telethon_entities)
    assert any(isinstance(e, tl.MessageEntityTextUrl) and e.url == "tg://user?id=123456"
               for e in telethon_entities)
    bot = _telethon_entities_to_bot(telethon_entities)
    types = {e.type for e in bot}
    assert {"strikethrough", "spoiler", "expandable_blockquote", "text_link", "pre"} <= types
    assert next(e for e in bot if e.type == "pre").language == "python"


def test_plain_caption_never_runs_implicit_markdown_in_telethon(monkeypatch):
    from app import sender_telethon_helpers as module
    class Client:
        sent = None
        async def send_message(self, **kwargs):
            self.sent = kwargs
            return SimpleNamespace(id=812)
    class Resolver:
        def __init__(self, _client): pass
        async def get_before_max_message_id(self, *_args): return 811
        async def resolve_authoritative_single_message(self, **kwargs):
            assert kwargs["expected_text"] == "Literal **stars**"
            return SimpleNamespace(ok=True, authoritative_message_id=812,
                                   resolution_method="direct", error_text=None)
    monkeypatch.setattr(module, "TelethonAuthoritativeMessageResolver", Resolver)
    client = Client()
    owner = SimpleNamespace(
        telethon=client,
        _clone_telethon_entities=SenderContentHelpers(owner=None).clone_telethon_entities,
    )
    sender = module.SenderTelethonHelpers(owner)
    async def target(_id): return "-100123", True
    sender._resolve_target_entity_and_broadcast_flag = target
    outcome = asyncio.run(sender.send_text_via_telethon(
        target_id="-100123", target_thread_id=None,
        text="Literal **stars**", entities=[], literal_caption=True,
    ))
    assert outcome.authoritative_message_id == 812
    assert client.sent["message"] == "Literal **stars**"
    assert "parse_mode" in client.sent and client.sent["parse_mode"] is None


def test_original_source_text_is_never_used_by_custom_single_fallback():
    source = Path("app/repost_single_delivery.py").read_text(encoding="utf-8")
    assert "custom_caption if custom_caption is not None" in source
    assert "custom_caption_media_send_failed_no_text_only_downgrade" in source
    assert "media_downgrade_blocked" in source
    assert 'parse_mode=None if custom_caption is not None else "HTML"' in source
    assert "entities\": bot_entities" in source


def test_custom_caption_media_send_disables_markdown_on_both_paths():
    source = Path("app/sender_telethon_helpers.py").read_text(encoding="utf-8")
    single = source.split("async def send_file_via_telethon(", 1)[1].split(
        "async def send_album_via_telethon(", 1
    )[0]
    album = source.split("async def send_album_via_telethon(", 1)[1].split(
        "async def verify_album_delivery(", 1
    )[0]
    assert single.count('send_kwargs["parse_mode"] = None') == 2
    assert album.count('send_kwargs["parse_mode"] = None') == 2



def test_link_preview_not_considered_downloadable_attachment():
    from telethon.tl import types
    from app.sender_primitives import _is_link_preview_only
    link_post = SimpleNamespace(
        id=11,
        media=types.MessageMediaWebPage(webpage=types.WebPageEmpty(id=112)),
    )
    photo_post = SimpleNamespace(id=12, media=object())
    assert _is_link_preview_only(link_post)
    assert not _is_link_preview_only(photo_post)


def test_link_preview_with_custom_caption_uses_text_path_not_media_download():
    from telethon.tl import types
    from app.sender_reupload_helpers import SenderReuploadHelpers
    class Telegram:
        async def download_media(self, *_args, **_kwargs):
            raise AssertionError("A link preview must not be downloaded as a media attachment")
    class Owner:
        def __init__(self):
            self.telethon = Telegram()
            self.sent = None
        def _build_text_and_entities_from_content(self, content):
            return SenderContentHelpers(owner=None).build_text_and_entities_from_content(content)
        async def _send_text_via_telethon(self, **kwargs):
            self.sent = kwargs
            return 941
    owner = Owner()
    post = SimpleNamespace(id=20, media=types.MessageMediaWebPage(
        webpage=types.WebPageEmpty(id=21),
    ))
    outcome = asyncio.run(SenderReuploadHelpers(owner).reupload_message(
        post, "-1001", None,
        caption_override={"text": "Literal **stars**", "entities": []},
    ))
    assert outcome.authoritative_message_id == 941
    assert owner.sent["text"] == "Literal **stars**"
    assert owner.sent["literal_caption"] is True


def test_link_preview_excluded_from_custom_caption_downgrade_block():
    source = Path("app/repost_single_delivery.py").read_text(encoding="utf-8")
    assert "not _is_link_preview_only(message)" in source
    assert "_is_link_preview_only(message)" in Path(
        "app/sender_reupload_helpers.py"
    ).read_text(encoding="utf-8")
