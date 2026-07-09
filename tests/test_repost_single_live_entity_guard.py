import asyncio
import logging
from types import SimpleNamespace

from telethon import types

from app.delivery_content_helpers import content_requires_builder
from app.repost_single_delivery import RepostSingleDelivery
from app.sender_content_helpers import SenderContentHelpers


class _Repo:
    def log_delivery_event(self, **_kwargs):
        return None


class _Bot:
    async def send_message(self, **_kwargs):
        return SimpleNamespace(message_id=900)


class _Owner:
    db = _Repo()
    bot = _Bot()

    def __init__(self, caption_mode):
        self.caption_mode = caption_mode
        self.copy_single_called = False
        self.reupload_message_called = False
        self.helpers = SenderContentHelpers(owner=SimpleNamespace())

    def _get_post_id_by_delivery_sync(self, delivery_id):
        return 1000 + int(delivery_id)

    def _resolve_repost_caption_delivery_strategy_sync(self, **_kwargs):
        if self.caption_mode == "builder_first":
            return {"configured_mode": "builder_first", "requires_builder": True, "use_copy_first": False}
        return {"configured_mode": self.caption_mode, "requires_builder": False, "use_copy_first": True}

    def _get_post_row_for_rule_message_sync(self, *_args):
        return {"content_json": {"text": "abc", "entities": [], "has_media": True, "media_kind": "video"}}

    def _is_self_loop_rule(self, *_args):
        return False

    async def _log_delivery_pipeline_step(self, **_kwargs):
        return None

    async def _copy_single_via_bot(self, *_args):
        self.copy_single_called = True
        return {"raw_result": None, "sent_ids": [], "attempted": False, "raw_result_type": "none"}

    async def _fetch_message(self, *_args):
        return SimpleNamespace(
            raw_text="abc",
            text="abc",
            message="abc",
            entities=[types.MessageEntityCustomEmoji(offset=0, length=1, document_id=123)],
            media=object(),
            video=True,
            photo=None,
            gif=None,
            document=None,
            date=None,
        )

    def _content_from_message_or_post(self, **kwargs):
        return self.helpers.content_from_message_or_post(**kwargs)

    def _content_requires_builder(self, content):
        return content_requires_builder(content)

    def _build_text_and_entities_from_content(self, content):
        return self.helpers.build_text_and_entities_from_content(content)

    async def _reupload_message(self, *_args, **_kwargs):
        self.reupload_message_called = True
        return 777

    async def _confirm_target_delivery_message_ids_with_retry(self, **_kwargs):
        return [777] if self.reupload_message_called else []

    async def _add_reaction_for_rule_if_possible(self, **_kwargs):
        return True

    async def _log_delivery_final_success(self, **_kwargs):
        return None

    async def _log_delivery_final_failure(self, **_kwargs):
        return None

    def _mark_delivery_sent_sync(self, *_args, **_kwargs):
        return None


def _deliver(owner):
    return asyncio.run(
        RepostSingleDelivery(owner).deliver(
            SimpleNamespace(id=89), 749972, 60, "-1003546096814", "-1003812542665", None
        )
    )


def test_repost_single_auto_live_custom_emoji_skips_copy_and_uses_reupload(caplog):
    caplog.set_level(logging.INFO, logger="forwarder")
    owner = _Owner("auto")

    assert _deliver(owner) is True

    assert owner.copy_single_called is False
    assert owner.reupload_message_called is True
    assert "SINGLE_LIVE_ENTITY_GUARD_BUILDER_REQUIRED" in caplog.text
    assert "selected_path=builder_first" in caplog.text


def test_repost_single_explicit_copy_first_not_overridden_by_live_guard(caplog):
    caplog.set_level(logging.INFO, logger="forwarder")
    owner = _Owner("copy_first")

    assert _deliver(owner) is True

    assert owner.copy_single_called is True
    assert owner.reupload_message_called is True
    assert "SINGLE_LIVE_ENTITY_GUARD_BUILDER_REQUIRED" not in caplog.text


def test_repost_single_explicit_builder_first_skips_copy():
    owner = _Owner("builder_first")

    assert _deliver(owner) is True

    assert owner.copy_single_called is False
    assert owner.reupload_message_called is True
