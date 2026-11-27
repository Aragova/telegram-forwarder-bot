from pathlib import Path


HANDLERS_SOURCE = Path("app/repost_campaign_handlers.py").read_text(encoding="utf-8")


def test_delete_failures_resolve_callbacks_are_registered():
    source = Path("app/repost_campaign_report_handlers.py").read_text(encoding="utf-8")

    assert "rule_repost_campaign_run_delete_failures_resolve_confirm" in source
    assert "rule_repost_campaign_run_delete_failures_resolve_apply" in source


def test_delete_failures_resolve_apply_calls_runtime_service_method():
    source = Path("app/repost_campaign_report_handlers.py").read_text(encoding="utf-8")
    marker = "async def handle_rule_repost_campaign_run_delete_failures_resolve_apply"
    start = source.index(marker)
    end = source.index("@dp.callback_query", start + 1)
    handler_source = source[start:end]

    assert "runtime.resolve_campaign_run_delete_failures" in handler_source
    assert "delete_campaign_run_now" not in handler_source
