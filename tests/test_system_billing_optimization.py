from types import SimpleNamespace
from unittest.mock import Mock
from app.admin_handlers.system import _billing_home_kb, _build_fixed_prices_text, _build_fixed_plan_kb
from app.scheduler_runtime import _video_tenant_throttled
from app.tenant_fairness_service import TenantFairnessService


def test_billing_menu_and_fixed_price_snapshot():
    repo = Mock()
    repo.get_billing_fixed_prices.side_effect = [
        {"basic": {1: {"amount": 1234}}},
        {"basic": {1: {"amount": "11.5", "display": "$11.5"}}},
    ]
    text = _build_fixed_prices_text(SimpleNamespace(db=repo))
    assert "1234 Stars" in text and "$11.5" in text
    assert [call.args for call in repo.get_billing_fixed_prices.call_args_list] == [("stars",), ("crypto",)]
    callbacks = [b.callback_data for row in _billing_home_kb().inline_keyboard for b in row]
    assert callbacks == ["admin_billing_usd", "admin_fixed_prices_kind:stars",
                         "admin_fixed_prices_kind:crypto", "admin_billing_rates", "admin_billing_home_back"]


def test_fixed_plan_queries_once():
    repo = Mock()
    repo.get_billing_fixed_prices.return_value = {"basic": {1: {"amount": 1234}}}
    kb = _build_fixed_plan_kb(SimpleNamespace(db=repo), "stars", "basic")
    assert "1234 Stars" in kb.inline_keyboard[0][0].text
    repo.get_billing_fixed_prices.assert_called_once_with("stars")


def test_normal_fairness_does_not_read_subscription():
    repo = Mock()
    assert not TenantFairnessService(repo).should_throttle_tenant(tenant_id=1, system_mode="normal")
    repo.get_active_subscription.assert_not_called()


def test_degraded_video_preserves_admission_with_single_snapshot():
    repo = Mock()
    repo.get_tenant_heavy_admission_counts.return_value = {"pending": 300, "processing": 8, "retry": 20}
    repo.get_active_subscription.return_value = {"plan_name": "FREE"}
    assert _video_tenant_throttled(repo, TenantFairnessService(repo), 1, "degraded")
    repo.get_tenant_heavy_admission_counts.assert_called_once_with(1)
    repo.get_tenant_job_counts.assert_not_called()
    repo.get_tenant_processing_counts.assert_not_called()
    repo.get_tenant_retry_counts.assert_not_called()


def test_billing_edit_callbacks_reject_invalid_data_and_keep_cancel():
    import asyncio
    from unittest.mock import AsyncMock
    from aiogram import Dispatcher
    from app.admin_handlers.system import register_admin_system_handlers

    dp = Dispatcher()
    ctx = SimpleNamespace(
        is_admin_callback=AsyncMock(return_value=True),
        answer_callback_safe=AsyncMock(),
        answer_callback_safe_once=AsyncMock(),
        edit_message_text_safe=AsyncMock(),
        user_states={},
    )
    register_admin_system_handlers(dp, ctx)
    handlers = {handler.callback.__name__: handler.callback for handler in dp.callback_query.handlers}
    for name, data in (
        ("handle_billing_usd_edit", "admin_billing_usd_edit:fake:1"),
        ("handle_billing_rate_edit", "admin_billing_rate_edit:XYZ"),
        ("handle_fixed_price_edit", "admin_fixed_price_edit:stars:basic:no"),
    ):
        asyncio.run(handlers[name](SimpleNamespace(data=data, from_user=SimpleNamespace(id=1), message=object())))
        assert not ctx.user_states
    assert ctx.answer_callback_safe.await_count == 3
    asyncio.run(handlers["handle_billing_usd_edit"](SimpleNamespace(
        data="admin_billing_usd_edit:basic:1", from_user=SimpleNamespace(id=1), message=object(),
    )))
    assert ctx.user_states[1]["period_months"] == 1
    kb = ctx.edit_message_text_safe.call_args.kwargs["reply_markup"]
    assert kb.inline_keyboard[0][0].callback_data == "admin_billing_usd_plan:basic"


def test_unknown_heavy_counts_fail_closed():
    repo = Mock()
    service = TenantFairnessService(repo)
    for bad in (None, {}, {"pending": -1, "processing": 0, "retry": 0},
                {"pending": 0, "processing": None, "retry": 0}):
        repo.get_tenant_heavy_admission_counts.return_value = bad
        assert _video_tenant_throttled(repo, service, 1, "degraded")
    repo.get_tenant_heavy_admission_counts.side_effect = RuntimeError("database unavailable")
    assert _video_tenant_throttled(repo, service, 1, "degraded")
    repo.get_active_subscription.assert_not_called()


def test_scheduler_missing_video_snapshot_keeps_other_rules(monkeypatch):
    import asyncio
    from app import scheduler_runtime

    repo = Mock()
    repo.get_all_rules.return_value = [
        SimpleNamespace(id=1, is_active=True, mode="video"),
        SimpleNamespace(id=2, is_active=True, mode="repost"),
    ]
    repo.mark_expired_campaign_top_time_pauses_completed.return_value = 0
    repo.get_due_delivery.return_value = None
    monkeypatch.setattr(scheduler_runtime, "build_worker_load_snapshot", lambda *_: None)
    result = asyncio.run(scheduler_runtime.scheduler_tick(repo))
    assert result == {"created": 0, "duplicates": 0, "checked_rules": 1}
    assert repo.get_due_delivery.call_count == 1
    assert repo.get_due_delivery.call_args.args[0] == 2
