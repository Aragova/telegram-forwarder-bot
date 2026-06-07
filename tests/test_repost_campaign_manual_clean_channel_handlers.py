import asyncio
from pathlib import Path
from types import SimpleNamespace

import app.repost_campaign_handlers as handlers


class _FakeLogger:
    def info(self, *args, **kwargs):
        pass

    def warning(self, *args, **kwargs):
        pass

    def exception(self, *args, **kwargs):
        pass


class _FakeRuntime:
    def __init__(self, policy_state):
        self.policy_state = dict(policy_state)
        self.calls = []

    def build_manual_launch_policy_state(self, *, rule_id, force_ignore_clean_channel=False):
        self.calls.append(
            {
                "rule_id": rule_id,
                "force_ignore_clean_channel": force_ignore_clean_channel,
            }
        )
        return dict(self.policy_state)


class _FakeCtx:
    def __init__(self, policy_state, *, new_message=False):
        self.runtime = _FakeRuntime(policy_state)
        self.db = SimpleNamespace()
        self.logger = _FakeLogger()
        self.settings = SimpleNamespace(repost_campaign_admin_test_enabled=True)
        self.new_message = new_message
        self.edits = []
        self.sends = []
        self.answers = []
        self.enqueue_calls = []

    async def run_db(self, func, *args, **kwargs):
        return func(*args, **kwargs)

    def should_answer_new_message_for_callback(self, callback):
        return self.new_message

    async def edit_message_text_safe(self, *, message, text, reply_markup=None, **kwargs):
        self.edits.append({"message": message, "text": text, "reply_markup": reply_markup})

    async def send_message_safe(self, *, chat_id, text, reply_markup=None, **kwargs):
        message = SimpleNamespace(chat=SimpleNamespace(id=chat_id), message_id=999)
        self.sends.append({"chat_id": chat_id, "text": text, "reply_markup": reply_markup, "message": message})
        return message

    async def answer_callback_safe(self, callback, text=None, show_alert=False):
        self.answers.append({"text": text, "show_alert": show_alert})

    async def answer_callback_safe_once(self, callback, text=None, show_alert=False):
        self.answers.append({"text": text, "show_alert": show_alert, "once": True})

    def get_bot(self):
        return None


class _FakeCallback:
    def __init__(self, data="rule_repost_campaign_launch_now_preview:10"):
        self.data = data
        self.from_user = SimpleNamespace(id=777)
        self.message = SimpleNamespace(chat=SimpleNamespace(id=-100), message_id=55)


def _callbacks_from_last_render(ctx):
    rendered = (ctx.edits or ctx.sends)[-1]
    keyboard = rendered["reply_markup"]
    return [button.callback_data for row in keyboard.inline_keyboard for button in row if button.callback_data]


def _texts_from_last_render(ctx):
    rendered = (ctx.edits or ctx.sends)[-1]
    keyboard = rendered["reply_markup"]
    return [button.text for row in keyboard.inline_keyboard for button in row]


def _last_text(ctx):
    return (ctx.edits or ctx.sends)[-1]["text"]


def _policy(action, *, can_launch=False, ok=True):
    return {
        "ok": ok,
        "action": action,
        "can_launch": can_launch,
        "active_placements_total": 1,
        "delete_problem_total": 0,
        "blocking_text": "Чистый канал не позволяет запустить кампанию поверх активной рекламы",
        "warning_text": "Чистый канал выключен. ViMi разрешит запуск поверх активной рекламы.",
        "base_readiness": {"can_launch": True, "will_send_total": 1},
    }


def test_source_guards_manual_clean_channel_handlers_are_wired_only_to_manual_flow():
    handlers_source = Path("app/repost_campaign_handlers.py").read_text(encoding="utf-8")
    for required in (
        "build_repost_campaign_launch_clean_channel_blocked_view",
        "build_repost_campaign_launch_clean_channel_warning_view",
        "rule_repost_campaign_launch_confirm_force",
        "force_ignore_clean_channel=True",
        "force_ignore_clean_channel=False",
    ):
        assert required in handlers_source

    confirm_slice = handlers_source.split('rule_repost_campaign_launch_confirm_force:', 1)[-1]
    assert "launch_campaign_now(" not in confirm_slice
    assert 'action in {"allow", "allow_forced"} and policy_state.get("can_launch") is True' in confirm_slice

    for path in (
        "app/repost_campaign_schedule_service.py",
        "app/repost_campaign_scheduled_post_service.py",
    ):
        source = Path(path).read_text(encoding="utf-8")
        for forbidden in (
            "rule_repost_campaign_launch_confirm_force",
            "build_manual_launch_policy_state",
        ):
            assert forbidden not in source


def test_preview_on_with_active_placements_shows_blocked_view(monkeypatch):
    ctx = _FakeCtx(_policy("block", can_launch=False))
    monkeypatch.setattr(handlers, "build_repost_campaign_runtime", lambda actual_ctx: actual_ctx.runtime)

    result = asyncio.run(
        handlers._render_repost_campaign_manual_launch_policy(
            _FakeCallback(),
            10,
            ctx,
            force_ignore_clean_channel=False,
        )
    )

    assert result["action"] == "block"
    assert "Чистый канал включён" in _last_text(ctx)
    assert "Всё равно запустить" not in _last_text(ctx)
    callbacks = _callbacks_from_last_render(ctx)
    assert "rule_repost_campaign_active_placements:10:0" in callbacks
    assert "rule_repost_campaign_clean_channel:10" in callbacks
    assert "rule_repost_campaign_launch:10" in callbacks
    assert ctx.enqueue_calls == []


def test_preview_off_with_active_placements_shows_warning_view(monkeypatch):
    ctx = _FakeCtx(_policy("confirm_required", can_launch=False))
    monkeypatch.setattr(handlers, "build_repost_campaign_runtime", lambda actual_ctx: actual_ctx.runtime)

    result = asyncio.run(
        handlers._render_repost_campaign_manual_launch_policy(
            _FakeCallback(),
            10,
            ctx,
            force_ignore_clean_channel=False,
        )
    )

    assert result["action"] == "confirm_required"
    assert "Чистый канал выключен" in _last_text(ctx)
    assert any("Всё равно запустить" in text for text in _texts_from_last_render(ctx))
    assert "rule_repost_campaign_launch_confirm_force:10" in _callbacks_from_last_render(ctx)
    assert ctx.enqueue_calls == []


def test_preview_clean_allow_shows_readiness_view(monkeypatch):
    ctx = _FakeCtx(_policy("allow", can_launch=True))
    monkeypatch.setattr(handlers, "build_repost_campaign_runtime", lambda actual_ctx: actual_ctx.runtime)

    result = asyncio.run(
        handlers._render_repost_campaign_manual_launch_policy(
            _FakeCallback(),
            10,
            ctx,
            force_ignore_clean_channel=False,
        )
    )

    assert result["action"] == "allow"
    assert "Предпросмотр запуска" in _last_text(ctx)
    assert "rule_repost_campaign_launch_confirm:10" in _callbacks_from_last_render(ctx)


def test_ordinary_confirm_with_warning_does_not_enqueue(monkeypatch):
    ctx = _FakeCtx(_policy("confirm_required", can_launch=False))
    monkeypatch.setattr(handlers, "build_repost_campaign_runtime", lambda actual_ctx: actual_ctx.runtime)

    asyncio.run(
        handlers._render_repost_campaign_manual_launch_policy_state(
            _FakeCallback("rule_repost_campaign_launch_confirm:10"),
            10,
            ctx,
            policy_state=ctx.runtime.build_manual_launch_policy_state(rule_id=10, force_ignore_clean_channel=False),
        )
    )

    assert "Чистый канал выключен" in _last_text(ctx)
    assert ctx.enqueue_calls == []


def test_ordinary_confirm_with_block_does_not_enqueue(monkeypatch):
    ctx = _FakeCtx(_policy("block", can_launch=False))
    monkeypatch.setattr(handlers, "build_repost_campaign_runtime", lambda actual_ctx: actual_ctx.runtime)

    asyncio.run(
        handlers._render_repost_campaign_manual_launch_policy_state(
            _FakeCallback("rule_repost_campaign_launch_confirm:10"),
            10,
            ctx,
            policy_state=ctx.runtime.build_manual_launch_policy_state(rule_id=10, force_ignore_clean_channel=False),
        )
    )

    assert "Чистый канал включён" in _last_text(ctx)
    assert ctx.enqueue_calls == []


class _FakeEnqueueResult:
    def __init__(self):
        self.created = True
        self.job = {"id": 123, "status": "pending", "rule_id": 10}


class _FakeJobService:
    def __init__(self, *, repo, campaign_runtime, bot=None, logger_=None):
        self.repo = repo
        self.repo.service = self
        self.calls = []

    def enqueue_manual_launch(self, **kwargs):
        self.calls.append(kwargs)
        self.repo.ctx.enqueue_calls.append(kwargs)
        return _FakeEnqueueResult()


def test_ordinary_confirm_with_allow_enqueues_force_false(monkeypatch):
    ctx = _FakeCtx(_policy("allow", can_launch=True))
    ctx.db.ctx = ctx
    monkeypatch.setattr(handlers, "build_repost_campaign_runtime", lambda actual_ctx: actual_ctx.runtime)
    monkeypatch.setattr(handlers, "RepostCampaignLaunchJobService", _FakeJobService)

    asyncio.run(
        handlers._enqueue_repost_campaign_manual_launch_from_callback(
            _FakeCallback("rule_repost_campaign_launch_confirm:10"),
            10,
            ctx,
            force_ignore_clean_channel=False,
        )
    )

    assert ctx.enqueue_calls[-1]["force_ignore_clean_channel"] is False


def test_force_confirm_with_allow_forced_enqueues_force_true(monkeypatch):
    ctx = _FakeCtx(_policy("allow_forced", can_launch=True))
    ctx.db.ctx = ctx
    monkeypatch.setattr(handlers, "build_repost_campaign_runtime", lambda actual_ctx: actual_ctx.runtime)
    monkeypatch.setattr(handlers, "RepostCampaignLaunchJobService", _FakeJobService)

    asyncio.run(
        handlers._enqueue_repost_campaign_manual_launch_from_callback(
            _FakeCallback("rule_repost_campaign_launch_confirm_force:10"),
            10,
            ctx,
            force_ignore_clean_channel=True,
        )
    )

    assert ctx.enqueue_calls[-1]["force_ignore_clean_channel"] is True


def test_force_confirm_still_respects_base_block(monkeypatch):
    ctx = _FakeCtx(_policy("base_block", can_launch=False))
    monkeypatch.setattr(handlers, "build_repost_campaign_runtime", lambda actual_ctx: actual_ctx.runtime)

    asyncio.run(
        handlers._render_repost_campaign_manual_launch_policy(
            _FakeCallback("rule_repost_campaign_launch_confirm_force:10"),
            10,
            ctx,
            force_ignore_clean_channel=True,
        )
    )

    assert "Предпросмотр запуска" in _last_text(ctx)
    assert ctx.enqueue_calls == []


def test_force_confirm_with_block_does_not_enqueue(monkeypatch):
    ctx = _FakeCtx(_policy("block", can_launch=False))
    monkeypatch.setattr(handlers, "build_repost_campaign_runtime", lambda actual_ctx: actual_ctx.runtime)

    asyncio.run(
        handlers._render_repost_campaign_manual_launch_policy(
            _FakeCallback("rule_repost_campaign_launch_confirm_force:10"),
            10,
            ctx,
            force_ignore_clean_channel=True,
        )
    )

    assert "Чистый канал включён" in _last_text(ctx)
    assert ctx.enqueue_calls == []


def test_scheduled_services_remain_untouched_by_manual_handler_wiring():
    schedule_source = Path("app/repost_campaign_schedule_service.py").read_text(encoding="utf-8")
    scheduled_post_source = Path("app/repost_campaign_scheduled_post_service.py").read_text(encoding="utf-8")

    for forbidden in (
        "rule_repost_campaign_launch_confirm_force",
        "force_ignore_clean_channel",
        "build_manual_launch_policy_state",
        "waiting_clean_channel",
    ):
        assert forbidden not in schedule_source

    for forbidden in (
        "rule_repost_campaign_launch_confirm_force",
        "force_ignore_clean_channel",
        "build_manual_launch_policy_state",
        "RepostCampaignPlacementService",
        "waiting_clean_channel",
    ):
        assert forbidden not in scheduled_post_source
