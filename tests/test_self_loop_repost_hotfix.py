import asyncio
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
    def __init__(self, *, self_loop=True, use_copy_first=False, verify_ids=None, reupload_id=777, copy_result=None):
        self.db = _Repo()
        self.bot = _Bot()
        self.self_loop = self_loop
        self.strategy = {"configured_mode": "auto", "requires_builder": not use_copy_first, "use_copy_first": use_copy_first}
        self.verify_ids = verify_ids if verify_ids is not None else [777]
        self.copy_single = AsyncMock(return_value=copy_result or {"raw_result": None, "sent_ids": [], "attempted": False})
        self.fetch_message = AsyncMock(return_value=SimpleNamespace(raw_text="caption", text="caption", message="caption", entities=[], media=object(), date=None))
        self.reupload_message = AsyncMock(return_value=reupload_id)
        self.mark_delivery_sent = Mock()
        self.mark_faulty = Mock()
        self.final_success = AsyncMock()
        self.pipeline_step = AsyncMock()
        self.reaction = AsyncMock(return_value=True)

    def _get_post_id_by_delivery_sync(self, delivery_id): return int(delivery_id) + 100
    def _resolve_repost_caption_delivery_strategy_sync(self, **_kwargs): return dict(self.strategy)
    def _is_self_loop_rule(self, _rule): return self.self_loop
    async def _copy_single_via_bot(self, *args, **kwargs): return await self.copy_single(*args, **kwargs)
    async def _fetch_message(self, *args, **kwargs): return await self.fetch_message(*args, **kwargs)
    async def _reupload_message(self, *args, **kwargs): return await self.reupload_message(*args, **kwargs)
    async def _confirm_target_delivery_message_ids_with_retry(self, **_kwargs): return self.verify_ids
    async def _log_delivery_pipeline_step(self, **kwargs): await self.pipeline_step(**kwargs)
    async def _log_delivery_final_success(self, **kwargs): await self.final_success(**kwargs)
    async def _log_delivery_final_failure(self, **_kwargs): return None
    async def _add_reaction_for_rule_if_possible(self, **kwargs): return await self.reaction(**kwargs)
    async def _run_post_send_step_safe(self, *, coro_factory, **_kwargs):
        return {"ok": True, "result": await coro_factory()}
    def _get_post_row_for_rule_message_sync(self, *_args): return {"content_json": {"text": "caption", "entities": [], "has_media": True}}
    def _content_from_message_or_post(self, **_kwargs): return {"text": "caption", "entities": [], "has_media": True}
    def _content_requires_builder(self, _content): return False
    def _build_text_and_entities_from_content(self, content): return content.get("text") or "", []
    def _mark_delivery_sent_sync(self, *args, **kwargs): self.mark_delivery_sent(*args, **kwargs)
    def _mark_delivery_faulty_sync(self, *args, **kwargs): self.mark_faulty(*args, **kwargs)


def _deliver_single(owner):
    return asyncio.run(RepostSingleDelivery(owner).deliver(SimpleNamespace(id=14), 123, 469, "-1001", "-1001", None, idempotency_key="k1"))


def test_builder_first_self_loop_reuploads_new_post():
    owner = _SingleOwner(self_loop=True, use_copy_first=False, verify_ids=[777])
    assert _deliver_single(owner) is True
    owner.reupload_message.assert_called_once()
    owner.bot.send_message.assert_not_called()
    assert owner.mark_delivery_sent.call_args.kwargs["sent_message_id"] == 777
    assert owner.mark_delivery_sent.call_args.kwargs["delivery_method"] == "reupload_single"
    assert owner.final_success.call_args.kwargs["final_method"] == "reupload_single"


def test_copy_first_self_loop_copies_new_post_without_reupload():
    owner = _SingleOwner(self_loop=True, use_copy_first=True, verify_ids=[700], copy_result={"raw_result": None, "sent_ids": [700], "attempted": True})
    assert _deliver_single(owner) is True
    owner.copy_single.assert_called_once()
    owner.reupload_message.assert_not_called()
    assert owner.mark_delivery_sent.call_args.kwargs["sent_message_id"] == 700


def test_self_loop_copy_explicit_failure_allows_one_reupload():
    owner = _SingleOwner(self_loop=True, use_copy_first=True, verify_ids=[777], copy_result={"raw_result": None, "sent_ids": [], "attempted": True, "error_text": "boom"})
    assert _deliver_single(owner) is True
    owner.copy_single.assert_called_once()
    owner.reupload_message.assert_called_once()
    owner.bot.send_message.assert_not_called()
    assert owner.mark_delivery_sent.call_args.kwargs["sent_message_id"] == 777


def test_reupload_returned_id_verification_failed_marks_self_loop_without_text_fallback():
    owner = _SingleOwner(self_loop=True, use_copy_first=False, verify_ids=[], reupload_id=1147)
    assert _deliver_single(owner) is True
    owner.reupload_message.assert_called_once()
    owner.bot.send_message.assert_not_called()
    assert owner.mark_delivery_sent.call_args.kwargs["sent_message_id"] == 1147
    assert owner.mark_delivery_sent.call_args.kwargs["delivery_method"] == "reupload_single_self_loop_unverified"
    assert owner.final_success.call_args.kwargs["final_method"] == "reupload_single_self_loop_unverified"
    assert owner.db.accepted[-1][1]["sent_message_ids"] == [1147]


def test_reupload_fully_failed_self_loop_no_text_fallback_no_mark_sent():
    owner = _SingleOwner(self_loop=True, use_copy_first=False, verify_ids=[], reupload_id=None)
    assert _deliver_single(owner) is False
    owner.bot.send_message.assert_not_called()
    owner.mark_delivery_sent.assert_not_called()


def test_reaction_uses_new_id():
    owner = _SingleOwner(self_loop=True, use_copy_first=False, verify_ids=[777])
    assert _deliver_single(owner) is True
    assert owner.reaction.call_args.kwargs["sent_message_id"] == 777


class _AlbumOwner:
    def __init__(self, *, verify_ok=True):
        self.db = _Repo()
        self.verify_ok = verify_ok
        self.copy_album = AsyncMock(return_value={"ok": False, "sent_count": 0, "error_text": "copy failed"})
        self.reupload_album = AsyncMock(return_value={"ok": True, "sent_message_id": 100, "sent_message_ids": [100, 101, 102], "sent_count": 3})
        self.fetch_album = AsyncMock(return_value=[SimpleNamespace(id=10), SimpleNamespace(id=11), SimpleNamespace(id=12)])
        self.mark_album = Mock()
        self.mark_many = Mock()
        self.final_success = AsyncMock()
        self.pipeline_step = AsyncMock()
        self.reaction = AsyncMock(return_value=True)

    def _resolve_repost_caption_delivery_strategy_sync(self, **_kwargs): return {"configured_mode": "auto", "requires_builder": True, "use_copy_first": False}
    def _is_self_loop_rule(self, _rule): return True
    async def _copy_album_via_bot(self, *args, **kwargs): return await self.copy_album(*args, **kwargs)
    async def _reupload_album(self, *args, **kwargs): return await self.reupload_album(*args, **kwargs)
    async def _fetch_album_messages(self, *args, **kwargs): return await self.fetch_album(*args, **kwargs)
    async def _verify_album_delivery(self, **_kwargs): return {"ok": self.verify_ok, "sent_message_ids": [100, 101, 102], "first_message_id": 100}
    async def _add_reaction_for_rule_if_possible(self, **kwargs): return await self.reaction(**kwargs)
    async def _log_delivery_final_success(self, **kwargs): await self.final_success(**kwargs)
    async def _log_delivery_final_failure(self, **_kwargs): return None
    async def _log_delivery_pipeline_step(self, **kwargs): await self.pipeline_step(**kwargs)
    def _content_from_message_or_post(self, **_kwargs): return {"text": "", "entities": []}
    def _get_album_primary_text(self, *_args, **_kwargs): return ""
    def _caption_entity_counts(self, _entities): return {}
    def _log_caption_entity_inventory(self, **_kwargs): return None
    def _serialize_pipeline_verify_result(self, value): return value
    async def _select_reaction_message_id(self, target_id, sent_message_ids): return (sent_message_ids[0] if sent_message_ids else None), "first"
    def _mark_album_deliveries_sent_sync(self, *args, **kwargs): self.mark_album(*args, **kwargs)
    def _mark_many_deliveries_sent_sync(self, *args, **kwargs): self.mark_many(*args, **kwargs)


def _deliver_album(owner):
    rows = [{"delivery_id": 100, "message_id": 10}, {"delivery_id": 101, "message_id": 11}, {"delivery_id": 102, "message_id": 12}]
    return asyncio.run(RepostAlbumDelivery(owner).deliver(SimpleNamespace(id=14), rows, "-1001", "-1001", None, idempotency_key="ka"))


def test_album_self_loop_sends_exactly_one_new_album():
    owner = _AlbumOwner(verify_ok=True)
    assert _deliver_album(owner) is True
    owner.reupload_album.assert_called_once()
    owner.mark_album.assert_called_once()
    assert owner.mark_album.call_args.kwargs["sent_message_ids"] == [100, 101, 102]
    assert owner.mark_album.call_args.kwargs["delivery_method"] == "reupload_album_verified"
    assert owner.final_success.call_args.kwargs["sent_message_ids"] == [100, 101, 102]


def test_album_self_loop_failed_verify_marks_accepted_without_second_send():
    owner = _AlbumOwner(verify_ok=False)
    assert _deliver_album(owner) is True
    owner.reupload_album.assert_called_once()
    owner.mark_album.assert_called_once()
    assert owner.mark_album.call_args.kwargs["sent_message_ids"] == [100, 101, 102]
    assert owner.mark_album.call_args.kwargs["delivery_method"] == "reupload_album_self_loop_unverified"
    assert owner.final_success.call_args.kwargs["final_method"] == "reupload_album_self_loop_unverified"


def test_no_runtime_self_loop_noop_strings_in_app():
    from pathlib import Path
    runtime_sources = "\n".join(p.read_text() for p in Path("app").rglob("*.py"))
    assert "self_loop_noop_single" not in runtime_sources
    assert "self_loop_noop_album" not in runtime_sources
    assert "action=noop_mark_sent" not in runtime_sources
