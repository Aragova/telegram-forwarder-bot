import asyncio
import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

from app.repost_album_delivery import RepostAlbumDelivery
from app.repost_single_delivery import RepostSingleDelivery


class _Repo:
    def __init__(self):
        self.accepted = []

    def log_delivery_event(self, **_kwargs):
        return None

    def mark_delivery_attempt_accepted(self, key, **kwargs):
        self.accepted.append((key, kwargs))


class _Bot:
    def __init__(self):
        self.send_message = AsyncMock(return_value=SimpleNamespace(message_id=999))


class _SingleOwner:
    def __init__(self, *, self_loop=True, use_copy_first=False, verify_ids=None, self_loop_sequence=None):
        self.db = _Repo()
        self.bot = _Bot()
        self.self_loop = self_loop
        self.self_loop_sequence = list(self_loop_sequence or [])
        self.strategy = {"configured_mode": "auto", "requires_builder": not use_copy_first, "use_copy_first": use_copy_first}
        self.verify_ids = verify_ids if verify_ids is not None else [777]
        self.copy_single = AsyncMock(return_value={"raw_result": None, "sent_ids": [], "attempted": False})
        self.fetch_message = AsyncMock(return_value=SimpleNamespace(raw_text="caption", text="caption", message="caption", entities=[], media=object(), date=None))
        self.reupload_message = AsyncMock(return_value=777)
        self.mark_delivery_sent = Mock()
        self.final_success = AsyncMock()
        self.pipeline_step = AsyncMock()
        self.reaction = AsyncMock(return_value=True)

    def _get_post_id_by_delivery_sync(self, delivery_id):
        return int(delivery_id) + 100

    def _resolve_repost_caption_delivery_strategy_sync(self, **_kwargs):
        return dict(self.strategy)

    def _is_self_loop_rule(self, _rule):
        if self.self_loop_sequence:
            return self.self_loop_sequence.pop(0)
        return self.self_loop

    async def _copy_single_via_bot(self, *args, **kwargs):
        return await self.copy_single(*args, **kwargs)

    async def _fetch_message(self, *args, **kwargs):
        return await self.fetch_message(*args, **kwargs)

    async def _reupload_message(self, *args, **kwargs):
        return await self.reupload_message(*args, **kwargs)

    async def _confirm_target_delivery_message_ids_with_retry(self, **_kwargs):
        return self.verify_ids

    async def _log_delivery_pipeline_step(self, **kwargs):
        await self.pipeline_step(**kwargs)

    async def _log_delivery_final_success(self, **kwargs):
        await self.final_success(**kwargs)

    async def _log_delivery_final_failure(self, **_kwargs):
        return None

    async def _add_reaction_for_rule_if_possible(self, **kwargs):
        return await self.reaction(**kwargs)

    def _get_post_row_for_rule_message_sync(self, *_args):
        return {"content_json": {"text": "caption", "entities": [], "has_media": True, "media_kind": "video"}}

    def _content_from_message_or_post(self, **_kwargs):
        return {"text": "caption", "entities": [], "has_media": True, "media_kind": "video"}

    def _content_requires_builder(self, _content):
        return False

    def _build_text_and_entities_from_content(self, content):
        return content.get("text") or "", []

    def _mark_delivery_sent_sync(self, *args, **kwargs):
        self.mark_delivery_sent(*args, **kwargs)


def _deliver_single(owner):
    return asyncio.run(RepostSingleDelivery(owner).deliver(SimpleNamespace(id=14), 123, 469, "-1001", "-1001", None, idempotency_key="k1"))


def test_single_self_loop_builder_first_noops_without_creating_post():
    owner = _SingleOwner(self_loop=True, use_copy_first=False)

    assert _deliver_single(owner) is True

    owner.copy_single.assert_not_called()
    owner.fetch_message.assert_not_called()
    owner.reupload_message.assert_not_called()
    owner.bot.send_message.assert_not_called()
    owner.mark_delivery_sent.assert_called_once()
    assert owner.mark_delivery_sent.call_args.kwargs["sent_message_id"] == 469
    assert owner.mark_delivery_sent.call_args.kwargs["sent_message_ids"] == [469]
    assert owner.mark_delivery_sent.call_args.kwargs["delivery_method"] == "self_loop_noop_single"
    assert owner.final_success.call_args.kwargs["final_method"] == "self_loop_noop_single"


def test_single_self_loop_copy_first_noops_before_copy():
    owner = _SingleOwner(self_loop=True, use_copy_first=True)

    assert _deliver_single(owner) is True

    owner.copy_single.assert_not_called()
    owner.fetch_message.assert_not_called()
    owner.reupload_message.assert_not_called()
    assert owner.mark_delivery_sent.call_args.kwargs["sent_message_id"] == 469
    assert owner.mark_delivery_sent.call_args.kwargs["delivery_method"] == "self_loop_noop_single"


def test_single_non_self_loop_builder_first_still_reuploads():
    owner = _SingleOwner(self_loop=False, use_copy_first=False, verify_ids=[777])

    assert _deliver_single(owner) is True

    owner.copy_single.assert_not_called()
    owner.fetch_message.assert_called_once()
    owner.reupload_message.assert_called_once()
    assert owner.mark_delivery_sent.call_args.kwargs["delivery_method"] == "reupload_single"


def test_single_self_loop_blocks_text_fallback_after_failed_verify(caplog):
    caplog.set_level(logging.WARNING, logger="forwarder")
    owner = _SingleOwner(self_loop=False, use_copy_first=False, verify_ids=[], self_loop_sequence=[False, True])

    assert _deliver_single(owner) is True

    owner.reupload_message.assert_called_once()
    owner.bot.send_message.assert_not_called()
    assert owner.mark_delivery_sent.call_args.kwargs["sent_message_id"] == 469
    assert owner.mark_delivery_sent.call_args.kwargs["delivery_method"] == "self_loop_noop_single"
    assert "SELF_LOOP_TEXT_FALLBACK_BLOCKED" in caplog.text


class _AlbumOwner:
    def __init__(self):
        self.db = _Repo()
        self.copy_album = AsyncMock()
        self.send_album = AsyncMock()
        self.reupload_album = AsyncMock()
        self.fetch_album = AsyncMock()
        self.mark_album = Mock()
        self.final_success = AsyncMock()
        self.reaction = AsyncMock(return_value=True)

    def _resolve_repost_caption_delivery_strategy_sync(self, **_kwargs):
        return {"configured_mode": "auto", "requires_builder": False, "use_copy_first": True}

    def _is_self_loop_rule(self, _rule):
        return True

    async def _copy_album_via_bot(self, *args, **kwargs):
        return await self.copy_album(*args, **kwargs)

    async def _send_album_via_telethon(self, *args, **kwargs):
        return await self.send_album(*args, **kwargs)

    async def _reupload_album(self, *args, **kwargs):
        return await self.reupload_album(*args, **kwargs)

    async def _fetch_album_messages(self, *args, **kwargs):
        return await self.fetch_album(*args, **kwargs)

    async def _add_reaction_for_rule_if_possible(self, **kwargs):
        return await self.reaction(**kwargs)

    async def _log_delivery_final_success(self, **kwargs):
        await self.final_success(**kwargs)

    def _mark_album_deliveries_sent_sync(self, *args, **kwargs):
        self.mark_album(*args, **kwargs)


def test_album_self_loop_noops_without_creating_album():
    owner = _AlbumOwner()
    rows = [{"delivery_id": 100, "message_id": 10}, {"delivery_id": 101, "message_id": 11}, {"delivery_id": 102, "message_id": 12}]

    ok = asyncio.run(RepostAlbumDelivery(owner).deliver(SimpleNamespace(id=14), rows, "-1001", "-1001", None, idempotency_key="ka"))

    assert ok is True
    owner.copy_album.assert_not_called()
    owner.send_album.assert_not_called()
    owner.reupload_album.assert_not_called()
    owner.fetch_album.assert_not_called()
    owner.mark_album.assert_called_once_with(
        delivery_ids=[100, 101, 102],
        sent_message_ids=[10, 11, 12],
        target_id="-1001",
        delivery_method="self_loop_noop_album",
    )
    assert owner.final_success.call_args.kwargs["final_method"] == "self_loop_noop_album"
