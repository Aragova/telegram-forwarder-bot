import asyncio

from app.sender import SenderService


class DummyBot:
    pass


class DummyRule:
    id = 1


class Repo:
    def __init__(self):
        self.accepted = []
        self.sent = []

    def get_delivery_attempt_by_idempotency_key(self, _key):
        return None

    def create_delivery_attempt(self, **_kwargs):
        return 1

    def mark_delivery_attempt_sending(self, *_args, **_kwargs):
        return True

    def mark_delivery_attempt_accepted(self, key, *, sent_message_ids, telegram_method=None):
        self.accepted.append((key, sent_message_ids, telegram_method))

    def mark_delivery_attempt_failed(self, *_args, **_kwargs):
        raise AssertionError("must not fail after accepted")

    def get_rule(self, _rule_id):
        return DummyRule()

    def mark_delivery_sent(self, delivery_id, sent_message_id=None, sent_message_ids_json=None, target_id=None, delivery_method=None):
        self.sent.append((delivery_id, sent_message_id, sent_message_ids_json, target_id, delivery_method))

    def touch_rule_after_send(self, *_args, **_kwargs):
        return None


class VideoProcessorStub:
    async def get_video_info(self, *_args, **_kwargs):
        return {"duration": 1}

    async def send_with_retry(self, *_args, **_kwargs):
        class M:
            message_id = 301
        return M()


def test_post_send_safe_swallows_exception():
    s = SenderService(bot=DummyBot(), telethon_client=None, reaction_clients=[], db=Repo())

    async def run():
        res = await s._run_post_send_step_safe(
            step_name="verify",
            rule_id=1,
            delivery_id=1,
            coro_factory=lambda: (_ for _ in ()).throw(RuntimeError("boom")),
        )
        assert res["ok"] is False

    asyncio.run(run())


def test_video_send_verify_failure_after_accepted_is_non_fatal():
    repo = Repo()
    s = SenderService(bot=DummyBot(), telethon_client=None, reaction_clients=[], db=repo)
    s.video_processor = VideoProcessorStub()

    async def _confirm(**_kwargs):
        return []

    async def _react(*_args, **_kwargs):
        raise RuntimeError("reaction fail")

    s._confirm_target_delivery_message_ids_with_retry = _confirm  # type: ignore
    s._add_reaction_if_possible = _react  # type: ignore

    result = asyncio.run(
        s.execute_video_send_from_job(
            delivery_id=7,
            rule_id=1,
            target_id="-1001",
            source_channel="@src",
            message_id=11,
            processed_video_path=__file__,
        )
    )
    assert result.get("ok") is True
    assert repo.accepted
    assert repo.sent


def test_video_delivery_post_send_failure_after_accepted_is_non_fatal():
    repo = Repo()
    s = SenderService(bot=DummyBot(), telethon_client=None, reaction_clients=[], db=repo)

    async def _deliver(*_args, **_kwargs):
        return {"ok": False, "sent_message_ids": [401]}

    s._deliver_single_video = _deliver  # type: ignore

    result = asyncio.run(
        s.execute_video_delivery_from_job(
            rule_id=1,
            delivery_id=8,
            message_id=12,
            source_channel="@src",
            target_id="-1001",
        )
    )
    assert result is True
    assert repo.accepted
    assert repo.sent


def test_single_delivery_source_extracted_from_sender():
    sender_source = open("app/sender.py", encoding="utf-8").read()
    runtime_source = open("app/repost_single_delivery.py", encoding="utf-8").read()

    wrapper_start = sender_source.index("    async def _deliver_single(")
    wrapper_end = sender_source.index("    async def _deliver_single_video", wrapper_start)
    wrapper_source = sender_source[wrapper_start:wrapper_end]

    assert len(wrapper_source.splitlines()) <= 20
    assert "RepostSingleDelivery(self).deliver" in wrapper_source
    assert "pipeline_stage=\"copy_single\"" not in wrapper_source
    assert "class RepostSingleDelivery" in runtime_source
    assert "pipeline_stage=\"copy_single\"" in runtime_source
    assert "COPY_SINGLE_TARGET_CONFIRM_OK" in runtime_source


def test_video_single_delivery_runtime_extracted_from_sender():
    sender_source = open("app/sender.py", encoding="utf-8").read()
    runtime_source = open("app/video_single_delivery.py", encoding="utf-8").read()

    wrapper_start = sender_source.index("    async def _deliver_single_video(")
    wrapper_end = sender_source.index("    async def", wrapper_start + 1)
    wrapper_source = sender_source[wrapper_start:wrapper_end]

    assert len(wrapper_source.splitlines()) <= 24
    assert "VideoSingleDelivery(self).deliver" in wrapper_source

    assert "process_video(" not in wrapper_source
    assert "video_processing_started" not in wrapper_source
    assert "video_download_started" not in wrapper_source
    assert "DELIVERY_SENT_MESSAGE_IDS_EXTRACTED" not in wrapper_source
    assert "VIDEO_REACTION" not in wrapper_source

    assert "class VideoSingleDelivery" in runtime_source
    assert "process_video(" in runtime_source
    assert "video_processing_started" in runtime_source
    assert "video_download_started" in runtime_source
    assert "DELIVERY_SENT_MESSAGE_IDS_EXTRACTED" in runtime_source
    assert "method=video_process" in runtime_source or '"video_process"' in runtime_source

    forbidden = [
        "ActiveCanary",
        "active_canary",
        "Rollout",
        "TelegramSendGateway",
        "TargetVerifier",
        "DeliveryFinalizer",
        "DeliveryContext",
        "PipelineResult",
    ]
    for needle in forbidden:
        assert needle not in runtime_source


def test_deliver_single_video_wrapper_delegates_to_video_single_delivery(monkeypatch):
    from app.video_single_delivery import VideoSingleDelivery

    s = SenderService(bot=DummyBot(), telethon_client=None, reaction_clients=[], db=Repo())
    rule = DummyRule()
    calls = []

    async def fake_deliver(
        self,
        got_rule,
        got_delivery_id,
        got_message_id,
        got_source_channel,
        got_target_id,
        got_target_thread_id,
    ):
        calls.append(
            (
                self.owner,
                got_rule,
                got_delivery_id,
                got_message_id,
                got_source_channel,
                got_target_id,
                got_target_thread_id,
            )
        )
        return {"delegated": True}

    monkeypatch.setattr(VideoSingleDelivery, "deliver", fake_deliver)

    result = asyncio.run(
        s._deliver_single_video(
            rule,
            7,
            12,
            "@src",
            "-1001",
            55,
        )
    )

    assert result == {"delegated": True}
    assert calls == [(s, rule, 7, 12, "@src", "-1001", 55)]


def test_repost_album_runtime_extracted_from_sender():
    sender_source = open("app/sender.py", encoding="utf-8").read()
    runtime_source = open("app/repost_album_delivery.py", encoding="utf-8").read()

    wrapper_start = sender_source.index("    async def _deliver_album(")
    wrapper_end = sender_source.index("    async def", wrapper_start + 1)
    wrapper_source = sender_source[wrapper_start:wrapper_end]

    assert len(wrapper_source.splitlines()) <= 24
    assert "RepostAlbumDelivery(self).deliver" in wrapper_source

    assert "copy_album" not in wrapper_source
    assert "reupload_album" not in wrapper_source
    assert "verify_after_copy_album" not in wrapper_source
    assert "verify_after_reupload" not in wrapper_source

    assert "class RepostAlbumDelivery" in runtime_source
    assert "copy_album" in runtime_source
    assert "reupload_album" in runtime_source
    assert "verify_after_copy_album" in runtime_source
    assert "verify_after_reupload" in runtime_source
    assert "_mark_many_deliveries_sent_sync" in runtime_source

    forbidden = [
        "ActiveCanary",
        "active_canary",
        "Rollout",
        "TelegramSendGateway",
        "TargetVerifier",
        "DeliveryFinalizer",
        "DeliveryContext",
    ]
    for needle in forbidden:
        assert needle not in runtime_source


def test_deliver_album_wrapper_delegates_to_repost_album_delivery(monkeypatch):
    from app.repost_album_delivery import RepostAlbumDelivery

    s = SenderService(bot=DummyBot(), telethon_client=None, reaction_clients=[], db=Repo())
    rule = DummyRule()
    album_rows = [{"delivery_id": 1, "message_id": 10}]
    calls = []

    async def fake_deliver(
        self,
        got_rule,
        got_album_rows,
        got_source_channel,
        got_target_id,
        got_target_thread_id,
        idempotency_key=None,
    ):
        calls.append(
            (
                self.owner,
                got_rule,
                got_album_rows,
                got_source_channel,
                got_target_id,
                got_target_thread_id,
                idempotency_key,
            )
        )
        return {"delegated": True}

    monkeypatch.setattr(RepostAlbumDelivery, "deliver", fake_deliver)

    result = asyncio.run(
        s._deliver_album(
            rule,
            album_rows,
            "@src",
            "-1001",
            55,
            idempotency_key="album-key",
        )
    )

    assert result == {"delegated": True}
    assert calls == [(s, rule, album_rows, "@src", "-1001", 55, "album-key")]


def test_execute_repost_single_calls_delivery_and_touches_once():
    repo = Repo()
    touches = []
    s = SenderService(bot=DummyBot(), telethon_client=None, reaction_clients=[], db=repo)

    async def _deliver(rule, delivery_id, message_id, source_channel, target_id, target_thread_id, idempotency_key=None):
        assert rule.id == 1
        assert delivery_id == 9
        assert message_id == 13
        assert idempotency_key
        return True

    def _touch(rule_id, interval):
        touches.append((rule_id, interval))

    s._deliver_single = _deliver  # type: ignore
    s._touch_rule_after_send_sync = _touch  # type: ignore

    ok = asyncio.run(
        s.execute_repost_single_from_job(
            rule_id=1,
            delivery_id=9,
            message_id=13,
            source_channel="@src",
            target_id="-1001",
            interval=77,
        )
    )

    assert ok is True
    assert touches == [(1, 77)]


def test_copy_single_success_marks_sent_reacts_and_does_not_touch_inside_delivery():
    from app.repost_single_delivery import RepostSingleDelivery

    events = []

    class RuntimeRepo:
        def log_delivery_event(self, **kwargs):
            events.append(("log_event", kwargs.get("event_type")))

        def mark_delivery_attempt_accepted(self, key, *, sent_message_ids, telegram_method=None):
            events.append(("accepted", key, tuple(sent_message_ids), telegram_method))

    class RuntimeOwner:
        db = RuntimeRepo()
        bot = DummyBot()

        def _get_post_id_by_delivery_sync(self, delivery_id):
            return 1000 + delivery_id

        def _resolve_repost_caption_delivery_strategy_sync(self, **_kwargs):
            return {"configured_mode": "copy", "requires_builder": False, "use_copy_first": True}

        def _get_post_row_for_rule_message_sync(self, *_args):
            return None

        def _is_self_loop_rule(self, *_args):
            return False

        async def _log_delivery_pipeline_step(self, **kwargs):
            events.append(("pipeline", kwargs.get("pipeline_stage"), kwargs.get("pipeline_result")))

        async def _copy_single_via_bot(self, *_args):
            return {"raw_result": {"message_id": 501}, "sent_ids": [501], "attempted": True, "raw_result_type": "dict"}

        async def _run_post_send_step_safe(self, *, step_name, coro_factory, **_kwargs):
            result = await coro_factory()
            events.append(("post_send", step_name, result))
            return {"ok": True, "result": result}

        async def _confirm_target_delivery_message_ids_with_retry(self, **_kwargs):
            return [501]

        async def _add_reaction_for_rule_if_possible(self, **kwargs):
            events.append(("reaction", kwargs.get("sent_message_id")))
            return True

        async def _log_delivery_final_success(self, **kwargs):
            events.append(("final_success", kwargs.get("final_method")))

        def _mark_delivery_sent_sync(self, delivery_id, **kwargs):
            events.append(("mark_sent", delivery_id, kwargs.get("delivery_method"), kwargs.get("sent_message_ids")))

        def _touch_rule_after_send_sync(self, *_args):
            events.append(("touch",))

    ok = asyncio.run(
        RepostSingleDelivery(RuntimeOwner()).deliver(
            DummyRule(), 9, 13, "@src", "-1001", None, idempotency_key="key-1"
        )
    )

    assert ok is True
    assert ("reaction", 501) in events
    assert ("final_success", "copy_single") in events
    assert ("mark_sent", 9, "copy_single", [501]) in events
    assert not [event for event in events if event[0] == "touch"]


def test_reupload_single_passes_reaction_context():
    from app.repost_single_delivery import RepostSingleDelivery

    rule = DummyRule()
    delivery_id = 91
    message_id = 131
    source_channel = "@src"
    target_id = "-1001"
    reaction_kwargs = {}

    class RuntimeRepo:
        def log_delivery_event(self, **_kwargs):
            return None

    class RuntimeOwner:
        db = RuntimeRepo()
        bot = DummyBot()

        def _get_post_id_by_delivery_sync(self, delivery_id):
            return 1000 + delivery_id

        def _resolve_repost_caption_delivery_strategy_sync(self, **_kwargs):
            return {"configured_mode": "builder", "requires_builder": True, "use_copy_first": False}

        def _get_post_row_for_rule_message_sync(self, *_args):
            return None

        def _is_self_loop_rule(self, *_args):
            return False

        async def _log_delivery_pipeline_step(self, **_kwargs):
            return None

        async def _fetch_message(self, *_args):
            return object()

        def _content_from_message_or_post(self, **_kwargs):
            return "текст"

        def _build_text_and_entities_from_content(self, content):
            return content, []

        async def _reupload_message(self, *_args, **_kwargs):
            return 501

        async def _confirm_target_delivery_message_ids_with_retry(self, **_kwargs):
            return [501]

        async def _add_reaction_for_rule_if_possible(self, **kwargs):
            reaction_kwargs.update(kwargs)
            return True

        async def _log_delivery_final_success(self, **_kwargs):
            return None

        def _mark_delivery_sent_sync(self, *_args, **_kwargs):
            return None

    ok = asyncio.run(
        RepostSingleDelivery(RuntimeOwner()).deliver(
            rule, delivery_id, message_id, source_channel, target_id, None, idempotency_key="key-r2-1"
        )
    )

    assert ok is True
    assert reaction_kwargs["rule"] is rule
    assert reaction_kwargs["target_id"] == target_id
    assert reaction_kwargs["sent_message_id"] == 501
    assert reaction_kwargs["source_channel"] == source_channel
    assert reaction_kwargs["source_message_ids"] == [message_id]
    assert reaction_kwargs["delivery_id"] == delivery_id


def test_text_fallback_passes_reaction_context():
    from app.repost_single_delivery import RepostSingleDelivery

    rule = DummyRule()
    delivery_id = 92
    message_id = 132
    source_channel = "@src"
    target_id = "-1001"
    reaction_kwargs = {}

    class RuntimeRepo:
        def log_delivery_event(self, **_kwargs):
            return None

    class RuntimeBot:
        async def send_message(self, **_kwargs):
            class Sent:
                message_id = 777
            return Sent()

    class RuntimeOwner:
        db = RuntimeRepo()
        bot = RuntimeBot()

        def _get_post_id_by_delivery_sync(self, delivery_id):
            return 1000 + delivery_id

        def _resolve_repost_caption_delivery_strategy_sync(self, **_kwargs):
            return {"configured_mode": "builder", "requires_builder": True, "use_copy_first": False}

        def _get_post_row_for_rule_message_sync(self, *_args):
            return None

        def _is_self_loop_rule(self, *_args):
            return False

        async def _log_delivery_pipeline_step(self, **_kwargs):
            return None

        async def _fetch_message(self, *_args):
            return object()

        def _content_from_message_or_post(self, **_kwargs):
            return "текст"

        def _build_text_and_entities_from_content(self, content):
            return content, []

        async def _reupload_message(self, *_args, **_kwargs):
            return None

        async def _confirm_target_delivery_message_ids_with_retry(self, **_kwargs):
            return []

        async def _add_reaction_for_rule_if_possible(self, **kwargs):
            reaction_kwargs.update(kwargs)
            return True

        async def _log_delivery_final_success(self, **_kwargs):
            return None

        def _mark_delivery_sent_sync(self, *_args, **_kwargs):
            return None

    ok = asyncio.run(
        RepostSingleDelivery(RuntimeOwner()).deliver(
            rule, delivery_id, message_id, source_channel, target_id, None, idempotency_key="key-r2-1-fallback"
        )
    )

    assert ok
    assert reaction_kwargs["rule"] is rule
    assert reaction_kwargs["target_id"] == target_id
    assert reaction_kwargs["sent_message_id"] == 777
    assert reaction_kwargs["source_channel"] == source_channel
    assert reaction_kwargs["source_message_ids"] == [message_id]
    assert reaction_kwargs["delivery_id"] == delivery_id


def test_repost_single_delivery_reaction_calls_keep_full_context_source_guard():
    source = open("app/repost_single_delivery.py", encoding="utf-8").read()

    copy_start = source.index('step_name="reaction_after_copy_single"')
    copy_block = source[copy_start:source.index("await owner._log_delivery_final_success", copy_start)]
    assert 'sent_message_id=authoritative_sent_message_id' in copy_block
    assert 'source_channel=str(source_channel or "")' in copy_block
    assert 'source_message_ids=source_message_ids' in copy_block
    assert 'delivery_id=delivery_id' in copy_block

    reupload_start = source.index('final_method="reupload_single"')
    reupload_call_start = source.rfind("await owner._add_reaction_for_rule_if_possible", 0, reupload_start)
    reupload_block = source[reupload_call_start:reupload_start]
    assert 'sent_message_id=authoritative_sent_message_id' in reupload_block
    assert 'source_channel=str(source_channel or "")' in reupload_block
    assert 'source_message_ids=source_message_ids' in reupload_block
    assert 'delivery_id=delivery_id' in reupload_block

    text_start = source.index('final_method="text_fallback"')
    text_call_start = source.rfind("await owner._add_reaction_for_rule_if_possible", 0, text_start)
    text_block = source[text_call_start:text_start]
    assert 'sent_message_id=sent.message_id' in text_block
    assert 'source_channel=str(source_channel or "")' in text_block
    assert 'source_message_ids=source_message_ids' in text_block
    assert 'delivery_id=delivery_id' in text_block


def test_video_send_runtime_extracted_from_sender():
    sender_source = open("app/sender.py", encoding="utf-8").read()
    runtime_source = open("app/video_send_delivery.py", encoding="utf-8").read()

    wrapper_start = sender_source.index("    async def execute_video_send_from_processed_job(")
    wrapper_end = sender_source.index("    async def _deliver_single(", wrapper_start)
    wrapper_source = sender_source[wrapper_start:wrapper_end]

    assert len(wrapper_source.splitlines()) <= 24
    assert "VideoSendDelivery(self).execute_from_processed_job" in wrapper_source
    assert "send_with_retry" not in wrapper_source
    assert "DELIVERY_ATTEMPT_ACCEPTED" not in wrapper_source
    assert "reaction_after_video_send" not in wrapper_source
    assert "DELIVERY_SENT_MESSAGE_IDS_EXTRACTED" not in wrapper_source

    assert "class VideoSendDelivery" in runtime_source
    assert "execute_from_processed_job" in runtime_source
    assert "send_with_retry" in runtime_source
    assert "DELIVERY_ATTEMPT_ACCEPTED" in runtime_source
    assert "reaction_after_video_send" in runtime_source
    assert "DELIVERY_SENT_MESSAGE_IDS_EXTRACTED" in runtime_source

    forbidden = [
        "ActiveCanary",
        "active_canary",
        "Rollout",
        "Pipeline",
        "TelegramSendGateway",
        "TargetVerifier",
        "DeliveryFinalizer",
        "DeliveryContext",
    ]
    for needle in forbidden:
        assert needle not in runtime_source


def _run_album_delivery_with_entities(entities):
    from app.repost_album_delivery import RepostAlbumDelivery

    events = []

    class RuntimeRepo:
        def mark_many_deliveries_sent(self, delivery_ids):
            events.append(("mark_many", tuple(delivery_ids)))

    class RuntimeOwner(SenderService):
        def __init__(self):
            self.db = RuntimeRepo()
            self.copy_calls = 0
            self.reupload_calls = 0

        def _resolve_repost_caption_delivery_strategy_sync(self, **_kwargs):
            return {"configured_mode": "auto", "requires_builder": False, "use_copy_first": True}

        def _get_album_primary_text(self, *_args, **_kwargs):
            return "caption"

        async def _fetch_album_messages(self, *_args, **_kwargs):
            return [type("Msg", (), {"id": 10})(), type("Msg", (), {"id": 11})()]

        def _content_from_message_or_post(self, message=None, post_row=None):
            if message is not None and getattr(message, "id", None) == 10:
                return {"text": "caption", "entities": entities}
            return {"text": "", "entities": []}

        def _log_caption_entity_inventory(self, **kwargs):
            events.append(("inventory", kwargs.get("entities")))

        def _is_self_loop_rule(self, *_args):
            return False

        async def _log_delivery_pipeline_step(self, **kwargs):
            events.append(("pipeline", kwargs.get("pipeline_stage"), kwargs.get("pipeline_result"), kwargs.get("error_text"), kwargs.get("extra") or {}))

        async def _copy_album_via_bot(self, **_kwargs):
            self.copy_calls += 1
            return {"ok": True, "sent_message_id": 501, "sent_message_ids": [501, 502], "sent_count": 2}

        async def _reupload_album(self, **_kwargs):
            self.reupload_calls += 1
            return {"ok": True, "sent_message_id": 601, "sent_message_ids": [601, 602], "sent_count": 2}

        async def _verify_album_delivery(self, **kwargs):
            return {"ok": True, "first_message_id": (kwargs.get("sent_message_ids") or [None])[0], "sent_message_ids": kwargs.get("sent_message_ids") or []}

        def _serialize_pipeline_verify_result(self, result):
            return result

        async def _run_post_send_step_safe(self, *, coro_factory, **_kwargs):
            return {"ok": True, "result": await coro_factory()}

        async def _add_reaction_for_rule_if_possible(self, **_kwargs):
            return True

        async def _select_reaction_message_id(self, *, sent_message_ids, **_kwargs):
            return ((sent_message_ids or [None])[0], "first_sent_message")

        async def _log_delivery_final_success(self, **kwargs):
            events.append(("final_success", kwargs.get("final_method")))

        def _mark_many_deliveries_sent_sync(self, delivery_ids):
            self.db.mark_many_deliveries_sent(delivery_ids)

    owner = RuntimeOwner()
    ok = asyncio.run(
        RepostAlbumDelivery(owner).deliver(
            DummyRule(),
            [{"delivery_id": 1, "message_id": 10}, {"delivery_id": 2, "message_id": 11}],
            "@src",
            "-1001",
            None,
        )
    )
    return ok, owner, events


def test_album_custom_emoji_forces_telethon_reupload_path():
    ok, owner, _events = _run_album_delivery_with_entities(
        [{"type": "custom_emoji", "offset": 0, "length": 1, "custom_emoji_id": "777"}]
    )

    assert ok is True
    assert owner.copy_calls == 0
    assert owner.reupload_calls == 1


def test_album_custom_emoji_logs_force_telethon(caplog):
    caplog.set_level("INFO", logger="forwarder")

    ok, _owner, events = _run_album_delivery_with_entities(
        [{"type": "custom_emoji", "offset": 0, "length": 1, "custom_emoji_id": "777"}]
    )

    assert ok is True
    assert "ALBUM_CUSTOM_EMOJI_FORCE_TELETHON" in caplog.text
    assert "COPY_ALBUM_CAPTION_POLICY" in caplog.text
    assert "custom_emoji_requires_telethon" in caplog.text
    assert any(
        event[0:3] == ("pipeline", "copy_album", "skipped")
        and event[3] == "copy_album skipped because custom_emoji requires Telethon"
        for event in events
    )


def test_album_without_custom_emoji_keeps_copy_first():
    ok, owner, events = _run_album_delivery_with_entities(
        [{"type": "bold", "offset": 0, "length": 1}]
    )

    assert ok is True
    assert owner.copy_calls == 1
    assert owner.reupload_calls == 0
    assert ("final_success", "copy_album_verified") in events


def test_album_content_entities_preserve_custom_emoji():
    from telethon.tl import types

    s = SenderService(bot=DummyBot(), telethon_client=None, reaction_clients=[], db=Repo())
    content = {
        "text": "A 🔥 caption",
        "entities": [{"type": "custom_emoji", "offset": 2, "length": 2, "custom_emoji_id": "5470177992950946662"}],
    }

    text, entities = s._build_text_and_entities_from_content(content)

    assert text == "A 🔥 caption"
    assert len(entities) == 1
    assert isinstance(entities[0], types.MessageEntityCustomEmoji)
    assert entities[0].document_id == 5470177992950946662
    assert entities[0].offset == 2
    assert entities[0].length == 2


def test_album_builder_keeps_custom_emoji_entities():
    test_album_content_entities_preserve_custom_emoji()


def test_copy_album_copy_first_does_not_override_caption_when_builder_not_required():
    from aiogram.methods import CopyMessages

    calls = []

    class Bot:
        async def __call__(self, method):
            calls.append(method)
            return [type("M", (), {"message_id": 501})()]

    s = SenderService(bot=Bot(), telethon_client=None, reaction_clients=[], db=Repo())

    async def run():
        return await s._copy_album_via_bot("-1001", "-1002", [10, 11], None)

    result = asyncio.run(run())

    assert result["ok"] is True
    assert isinstance(calls[0], CopyMessages)
    assert not hasattr(calls[0], "caption")
    assert not hasattr(calls[0], "caption_entities")


def test_video_pipeline_stages_extracted_from_sender():
    sender_source = open("app/sender.py", encoding="utf-8").read()
    runtime_source = open("app/video_pipeline_stages.py", encoding="utf-8").read()

    assert "class VideoPipelineStages" in runtime_source
    assert "execute_download_from_job" in runtime_source
    assert "execute_process_from_job" in runtime_source
    assert "validate_mp4_file_for_pipeline" in runtime_source
    assert "ffprobe" in runtime_source
    assert "VIDEO DOWNLOAD DONE" in runtime_source
    assert "VIDEO PROCESS DONE" in runtime_source
    assert "VIDEO_PROCESS_CLIP_DURATION" in runtime_source

    download_start = sender_source.index("    async def execute_video_download_from_job(")
    download_end = sender_source.index("    async def", download_start + 1)
    download_wrapper = sender_source[download_start:download_end]

    process_start = sender_source.index("    async def execute_video_process_from_job(")
    process_end = sender_source.index("    async def", process_start + 1)
    process_wrapper = sender_source[process_start:process_end]

    validate_start = sender_source.index("    async def _validate_mp4_file_for_pipeline(")
    validate_end = sender_source.index("    async def", validate_start + 1)
    validate_wrapper = sender_source[validate_start:validate_end]

    assert len(download_wrapper.splitlines()) <= 35
    assert len(process_wrapper.splitlines()) <= 35
    assert len(validate_wrapper.splitlines()) <= 24

    assert "VideoPipelineStages(self).execute_download_from_job" in download_wrapper
    assert "VideoPipelineStages(self).execute_process_from_job" in process_wrapper
    assert "VideoPipelineStages(self).validate_mp4_file_for_pipeline" in validate_wrapper

    for wrapper in [download_wrapper, process_wrapper, validate_wrapper]:
        assert "ffprobe" not in wrapper
        assert "build_processed_video" not in wrapper
        assert "VIDEO FILE VALIDATION FAILED" not in wrapper
        assert "VIDEO PROCESS DONE" not in wrapper

    forbidden = [
        "ActiveCanary",
        "active_canary",
        "Rollout",
        "TelegramSendGateway",
        "TargetVerifier",
        "DeliveryFinalizer",
        "DeliveryContext",
        "PipelineResult",
    ]
    for needle in forbidden:
        assert needle not in runtime_source


def test_execute_video_download_wrapper_delegates_to_video_pipeline_stages(monkeypatch):
    from app.video_pipeline_stages import VideoPipelineStages

    s = SenderService(bot=DummyBot(), telethon_client=None, reaction_clients=[], db=Repo())
    calls = []

    async def fake_execute(self, **kwargs):
        calls.append((self.owner, kwargs))
        return {"delegated": "download"}

    monkeypatch.setattr(VideoPipelineStages, "execute_download_from_job", fake_execute)

    result = asyncio.run(
        s.execute_video_download_from_job(
            job_id=10,
            job_attempt=2,
            rule_id=3,
            delivery_id=4,
            message_id=5,
            source_channel="@src",
            target_id="-100",
            invalid_file_attempts=1,
            extra="value",
        )
    )

    assert result == {"delegated": "download"}
    assert calls == [(
        s,
        {
            "job_id": 10,
            "job_attempt": 2,
            "rule_id": 3,
            "delivery_id": 4,
            "message_id": 5,
            "source_channel": "@src",
            "target_id": "-100",
            "invalid_file_attempts": 1,
            "extra": "value",
        },
    )]


def test_execute_video_process_wrapper_delegates_to_video_pipeline_stages(monkeypatch):
    from app.video_pipeline_stages import VideoPipelineStages

    s = SenderService(bot=DummyBot(), telethon_client=None, reaction_clients=[], db=Repo())
    calls = []

    async def fake_execute(self, **kwargs):
        calls.append((self.owner, kwargs))
        return {"delegated": "process"}

    monkeypatch.setattr(VideoPipelineStages, "execute_process_from_job", fake_execute)

    result = asyncio.run(
        s.execute_video_process_from_job(
            job_id=11,
            job_attempt=3,
            rule_id=4,
            delivery_id=5,
            source_video_path="/tmp/source.mp4",
            artifact_version=1,
            invalid_file_attempts=2,
            extra="value",
        )
    )

    assert result == {"delegated": "process"}
    assert calls == [(
        s,
        {
            "job_id": 11,
            "job_attempt": 3,
            "rule_id": 4,
            "delivery_id": 5,
            "source_video_path": "/tmp/source.mp4",
            "artifact_version": 1,
            "invalid_file_attempts": 2,
            "extra": "value",
        },
    )]


def test_validate_mp4_wrapper_delegates_to_video_pipeline_stages(monkeypatch, tmp_path):
    from app.video_pipeline_stages import VideoPipelineStages

    s = SenderService(bot=DummyBot(), telethon_client=None, reaction_clients=[], db=Repo())
    path = tmp_path / "source.mp4"
    calls = []

    async def fake_validate(self, file_path, **kwargs):
        calls.append((self.owner, file_path, kwargs))
        return True, None

    monkeypatch.setattr(VideoPipelineStages, "validate_mp4_file_for_pipeline", fake_validate)

    result = asyncio.run(
        s._validate_mp4_file_for_pipeline(path, delivery_id=7, job_id=8, stage="download")
    )

    assert result == (True, None)
    assert calls == [(s, path, {"delivery_id": 7, "job_id": 8, "stage": "download"})]


def _run_album_delivery_one_by_one_accepted_unresolved(*, first_resolved: bool):
    from app.repost_album_delivery import RepostAlbumDelivery

    events = []

    class RuntimeRepo:
        pass

    class RuntimeOwner(SenderService):
        def __init__(self):
            self.db = RuntimeRepo()
            self.reupload_calls = 0
            self.one_by_one_calls = 0
            self.reaction_calls = 0
            self.faulty = []

        def _resolve_repost_caption_delivery_strategy_sync(self, **_kwargs):
            return {"configured_mode": "auto", "requires_builder": False, "use_copy_first": False}

        def _is_self_loop_rule(self, *_args):
            return False

        def _content_from_message_or_post(self, message=None, post_row=None):
            return {"text": "caption" if (message is None or getattr(message, "id", None) == 10) else "", "entities": []}

        def _log_caption_entity_inventory(self, **_kwargs):
            pass

        def _caption_entity_counts(self, entities):
            return {"custom_emoji": 0}

        async def _fetch_album_messages(self, *_args, **_kwargs):
            return [type("Msg", (), {"id": 10})(), type("Msg", (), {"id": 11})(), type("Msg", (), {"id": 12})()]

        def _get_album_primary_text(self, *_args, **_kwargs):
            return "caption"

        async def _log_delivery_pipeline_step(self, **kwargs):
            events.append(("pipeline", kwargs.get("pipeline_stage"), kwargs.get("pipeline_result"), kwargs.get("error_text"), kwargs.get("extra") or {}))

        async def _reupload_album(self, **_kwargs):
            self.reupload_calls += 1
            return {"ok": False, "transport_accepted": False, "authoritative_resolved": False, "sent_message_id": None, "sent_message_ids": [], "sent_count": 0, "error_text": "transport_failed"}

        async def _send_album_one_by_one(self, **_kwargs):
            self.one_by_one_calls += 1
            if first_resolved:
                return {
                    "ok": False,
                    "transport_accepted": True,
                    "authoritative_resolved": False,
                    "sent_message_id": None,
                    "sent_message_ids": [],
                    "sent_count": 2,
                    "returned_candidate_id": 1158,
                    "returned_candidate_ids": [1158],
                    "resolved_authoritative_message_ids_before_unresolved": [201],
                    "resolution_method": "unresolved",
                    "error_text": "not_found",
                    "manual_review_required": True,
                    "non_retryable": True,
                    "action": "no_second_send",
                }
            return {
                "ok": False,
                "transport_accepted": True,
                "authoritative_resolved": False,
                "sent_message_id": None,
                "sent_message_ids": [],
                "sent_count": 1,
                "returned_candidate_id": 1158,
                "returned_candidate_ids": [1158],
                "resolved_authoritative_message_ids_before_unresolved": [],
                "resolution_method": "unresolved",
                "error_text": "not_found",
                "manual_review_required": True,
                "non_retryable": True,
                "action": "no_second_send",
            }

        async def _add_reaction_for_rule_if_possible(self, **_kwargs):
            self.reaction_calls += 1
            return True

        async def _log_delivery_final_failure(self, **kwargs):
            events.append(("final_failure", kwargs.get("final_method"), kwargs.get("error_text")))

        def _mark_delivery_faulty_sync(self, delivery_id, error_text):
            self.faulty.append((delivery_id, error_text))

    owner = RuntimeOwner()
    ok = asyncio.run(
        RepostAlbumDelivery(owner).deliver(
            DummyRule(),
            [{"delivery_id": 1, "message_id": 10}, {"delivery_id": 2, "message_id": 11}, {"delivery_id": 3, "message_id": 12}],
            "@src",
            "-1001",
            None,
        )
    )
    return ok, owner, events


def test_album_delivery_one_by_one_second_accepted_unresolved_terminal_faulty():
    ok, owner, events = _run_album_delivery_one_by_one_accepted_unresolved(first_resolved=True)

    assert ok is False
    assert owner.one_by_one_calls == 1
    assert owner.reupload_calls == 2
    assert owner.reaction_calls == 0
    assert owner.faulty == [
        (1, "telethon_one_by_one_send_accepted_target_id_unresolved_non_retryable"),
        (2, "telethon_one_by_one_send_accepted_target_id_unresolved_non_retryable"),
        (3, "telethon_one_by_one_send_accepted_target_id_unresolved_non_retryable"),
    ]
    terminal = [event for event in events if event[1] == "one_by_one_accepted_target_id_unresolved"]
    assert terminal and terminal[0][2] == "terminal_manual_review"
    assert terminal[0][4]["returned_candidate_ids"] == [1158]
    assert terminal[0][4]["resolved_authoritative_message_ids_before_unresolved"] == [201]
    assert not [event for event in events if event[0] == "final_failure" and event[1] == "album_pipeline_final_failure"]


def test_album_delivery_one_by_one_first_accepted_unresolved_terminal_faulty():
    ok, owner, events = _run_album_delivery_one_by_one_accepted_unresolved(first_resolved=False)

    assert ok is False
    assert owner.one_by_one_calls == 1
    assert owner.reaction_calls == 0
    assert len(owner.faulty) == 3
    terminal = [event for event in events if event[1] == "one_by_one_accepted_target_id_unresolved"]
    assert terminal and terminal[0][4]["sent_count"] == 1
    assert terminal[0][4]["resolved_authoritative_message_ids_before_unresolved"] == []
