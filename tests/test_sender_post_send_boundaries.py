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
