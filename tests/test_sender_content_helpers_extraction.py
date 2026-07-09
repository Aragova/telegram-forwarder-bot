from pathlib import Path
from types import SimpleNamespace
import importlib


from app.sender import SenderService
from app.sender_content_helpers import SenderContentHelpers


def test_sender_content_helpers_extracted_from_sender():
    sender_source = Path("app/sender.py").read_text(encoding="utf-8")
    helper_source = Path("app/sender_content_helpers.py").read_text(encoding="utf-8")

    moved_methods = [
        "def caption_entity_counts",
        "def log_caption_entity_inventory",
        "def content_from_message_or_post",
        "def build_telethon_entities_from_content",
        "def build_text_and_entities_from_content",
        "def clone_telethon_entities",
        "def get_album_primary_text",
        "def get_rule_caption_delivery_mode",
        "def get_rule_video_caption_delivery_mode",
        "def resolve_repost_caption_delivery_strategy",
        "def content_requires_builder",
        "def single_requires_builder",
        "def album_requires_builder",
        "def get_post_row_for_rule_message",
    ]

    for method in moved_methods:
        assert method in helper_source

    heavy_old_blocks = [
        "def _content_from_message_or_post",
        "def _build_telethon_entities_from_content",
        "def _clone_telethon_entities",
        "def _resolve_repost_caption_delivery_strategy",
        "def _album_requires_builder",
    ]

    for method in heavy_old_blocks:
        assert sender_source.count(f"{method}(") == 1


def test_sender_content_helpers_do_not_import_sender():
    source = Path("app/sender_content_helpers.py").read_text(encoding="utf-8")

    forbidden = [
        "from ." "sender import",
        "import app." "sender",
        "import ." "sender",
    ]

    for item in forbidden:
        assert item not in source


class _FakeHelpers:
    def __init__(self, owner):
        self.owner = owner

    def content_from_message_or_post(self, **kwargs):
        return {"method": "content_from_message_or_post", "owner": self.owner, "kwargs": kwargs}

    def clone_telethon_entities(self, entities, text):
        return ["clone_telethon_entities", self.owner, entities, text]

    def resolve_repost_caption_delivery_strategy(self, **kwargs):
        return {"method": "resolve_repost_caption_delivery_strategy", "owner": self.owner, "kwargs": kwargs}

    def album_requires_builder(self, rule, source_channel, message_ids):
        return ("album_requires_builder", self.owner, rule, source_channel, message_ids)


def test_sender_content_wrapper_delegates_content_from_message_or_post(monkeypatch):
    helper_module = importlib.import_module("app.sender_content_helpers")

    monkeypatch.setattr(helper_module, "SenderContentHelpers", _FakeHelpers)
    service = SenderService.__new__(SenderService)

    result = service._content_from_message_or_post(message="msg", post_row={"id": 1})

    assert result == {
        "method": "content_from_message_or_post",
        "owner": service,
        "kwargs": {"message": "msg", "post_row": {"id": 1}},
    }


def test_sender_content_wrapper_delegates_clone_telethon_entities(monkeypatch):
    helper_module = importlib.import_module("app.sender_content_helpers")

    monkeypatch.setattr(helper_module, "SenderContentHelpers", _FakeHelpers)
    service = SenderService.__new__(SenderService)

    assert service._clone_telethon_entities(["entity"], "text") == [
        "clone_telethon_entities",
        service,
        ["entity"],
        "text",
    ]


def test_sender_content_wrapper_delegates_resolve_repost_caption_delivery_strategy(monkeypatch):
    helper_module = importlib.import_module("app.sender_content_helpers")

    monkeypatch.setattr(helper_module, "SenderContentHelpers", _FakeHelpers)
    service = SenderService.__new__(SenderService)
    rule = SimpleNamespace(id=1)

    result = service._resolve_repost_caption_delivery_strategy(
        rule=rule,
        source_channel="src",
        message_ids=[1, 2],
        is_album=True,
    )

    assert result == {
        "method": "resolve_repost_caption_delivery_strategy",
        "owner": service,
        "kwargs": {
            "rule": rule,
            "source_channel": "src",
            "message_ids": [1, 2],
            "is_album": True,
        },
    }


def test_sender_content_wrapper_delegates_album_requires_builder(monkeypatch):
    helper_module = importlib.import_module("app.sender_content_helpers")

    monkeypatch.setattr(helper_module, "SenderContentHelpers", _FakeHelpers)
    service = SenderService.__new__(SenderService)
    rule = SimpleNamespace(id=1)

    assert service._album_requires_builder(rule, "src", [1, 2]) == (
        "album_requires_builder",
        service,
        rule,
        "src",
        [1, 2],
    )


def test_sender_content_from_post_row_dict_smoke():
    helpers = SenderContentHelpers(owner=SimpleNamespace())

    assert helpers.content_from_message_or_post(
        post_row={"content_json": {"text": "abc", "media_kind": "text"}}
    ) == {"text": "abc", "media_kind": "text"}


def test_sender_content_from_post_row_string_smoke():
    helpers = SenderContentHelpers(owner=SimpleNamespace())

    assert helpers.content_from_message_or_post(
        post_row={"content_json": '{"text":"abc","media_kind":"text"}'}
    ) == {"text": "abc", "media_kind": "text"}


def test_sender_caption_modes_smoke():
    helpers = SenderContentHelpers(owner=SimpleNamespace())
    rule = SimpleNamespace(
        caption_delivery_mode="auto",
        video_caption_delivery_mode="builder_first",
    )

    assert helpers.get_rule_caption_delivery_mode(rule) == "auto"
    assert helpers.get_rule_video_caption_delivery_mode(rule) == "builder_first"


def test_sender_content_requires_builder_custom_emoji_smoke():
    helpers = SenderContentHelpers(owner=SimpleNamespace())
    content = {
        "text": "x",
        "entities": [{"type": "custom_emoji", "offset": 0, "length": 1, "custom_emoji_id": "123"}],
    }

    assert helpers.content_requires_builder(content) is True


def test_content_from_message_or_post_merges_live_entities_when_post_row_stale():
    from types import SimpleNamespace
    from telethon import types
    from app.sender_content_helpers import SenderContentHelpers

    helpers = SenderContentHelpers(owner=SimpleNamespace())

    post_row = {
        "content_json": {
            "text": "abc",
            "entities": [],
            "has_media": True,
            "media_kind": "video",
        }
    }

    message = SimpleNamespace(
        raw_text="abc",
        text="abc",
        message="abc",
        entities=[
            types.MessageEntityCustomEmoji(
                offset=0,
                length=1,
                document_id=123,
            )
        ],
        media=object(),
        video=True,
        photo=None,
        gif=None,
        document=None,
        date=None,
    )

    result = helpers.content_from_message_or_post(
        message=message,
        post_row=post_row,
    )

    assert result["text"] == "abc"
    assert result["entities"]
    assert result["entities"][0]["type"] == "custom_emoji"
    assert result["entities"][0]["custom_emoji_id"] == "123"
