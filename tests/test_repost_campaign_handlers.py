from pathlib import Path


HANDLERS_SOURCE = Path("app/repost_campaign_handlers.py").read_text(encoding="utf-8")


def test_invite_links_callbacks_are_registered():
    for callback_prefix in [
        "rule_repost_campaign_invite_links:",
        "rule_repost_campaign_invite_links_toggle:",
        "rule_repost_campaign_invite_links_destination:",
        "rule_repost_campaign_invite_links_list:",
        "rule_repost_campaign_invite_links_injection:",
    ]:
        assert callback_prefix in HANDLERS_SOURCE


def test_invite_links_toggle_uses_repository_update_and_rerender():
    assert "ctx.db.get_campaign_invite_link_settings" in HANDLERS_SOURCE
    assert "ctx.db.set_campaign_invite_link_settings" in HANDLERS_SOURCE
    assert "enabled = not bool((current or {}).get(\"enabled\"))" in HANDLERS_SOURCE
    assert "_render_repost_campaign_invite_links(callback, rule_id, ctx)" in HANDLERS_SOURCE


def test_invite_links_repository_errors_are_logged_and_user_safe():
    assert "REPOST_CAMPAIGN_INVITE_LINK_SETTINGS_LOOKUP_FAILED" in HANDLERS_SOURCE
    assert "REPOST_CAMPAIGN_INVITE_LINK_SETTINGS_UPDATE_FAILED" in HANDLERS_SOURCE
    assert "⚠️ Не удалось изменить статус функции. Попробуйте ещё раз." in HANDLERS_SOURCE


def test_invite_links_toggle_is_guarded_by_repost_campaign_feature_flag():
    helper_start = HANDLERS_SOURCE.index("async def _ensure_repost_campaign_rule_available")
    helper_end = HANDLERS_SOURCE.index("async def _get_invite_link_settings_for_ui")
    helper_source = HANDLERS_SOURCE[helper_start:helper_end]
    assert "ctx.settings.repost_campaign_admin_test_enabled" in helper_source
    assert "Функция пока выключена" in helper_source

    toggle_start = HANDLERS_SOURCE.index("async def handle_rule_repost_campaign_invite_links_toggle")
    toggle_end = HANDLERS_SOURCE.index("@dp.callback_query", toggle_start + 1)
    toggle_source = HANDLERS_SOURCE[toggle_start:toggle_end]
    assert toggle_source.index("_ensure_repost_campaign_rule_available") < toggle_source.index("ctx.db.set_campaign_invite_link_settings")
