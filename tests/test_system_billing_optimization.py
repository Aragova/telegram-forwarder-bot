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
