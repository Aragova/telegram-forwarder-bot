from pathlib import Path


HANDLERS_SOURCE = Path("app/repost_campaign_handlers.py").read_text(encoding="utf-8")


def test_invite_links_callbacks_are_registered():
    for callback_prefix in [
        "rule_repost_campaign_invite_links:",
        "rule_repost_campaign_invite_links_toggle:",
        "rule_repost_campaign_invite_links_destination:",
        "rule_repost_campaign_invite_links_list:",
        "rule_repost_campaign_invite_links_injection:",
        "rule_repost_campaign_invite_links_mode:",
        "rule_repost_campaign_invite_links_mode_set:",
        "rule_repost_campaign_invite_links_injection_set:",
        "rule_repost_campaign_invite_links_per_target:",
        "rule_repost_campaign_invite_links_per_target_set:",
        "rule_repost_campaign_invite_links_preview:",
        "rule_repost_campaign_invite_links_preview_set:",
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
    assert "⚠️ Не удалось сохранить настройку. Попробуйте ещё раз." in HANDLERS_SOURCE


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


def test_invite_links_set_callbacks_save_expected_fields():
    for snippet in [
        "link_mode=mode",
        "injection_mode=mode",
        "per_target_links_enabled=(value == \"on\")",
        "preview_required=(value == \"required\")",
        "destination_chat_id=str(chat_id)",
        "destination_chat_title=title",
    ]:
        assert snippet in HANDLERS_SOURCE
    for value in ["join_request", "direct_join", "placeholder", "append_footer", "disabled", "on", "off", "required", "optional"]:
        assert value in HANDLERS_SOURCE


def test_invite_links_set_callbacks_are_guarded_before_repository_update():
    helper_start = HANDLERS_SOURCE.index("async def _save_invite_link_setting")
    helper_end = HANDLERS_SOURCE.index("async def _render_repost_campaign_top_time_settings")
    helper_source = HANDLERS_SOURCE[helper_start:helper_end]
    assert helper_source.index("_ensure_repost_campaign_rule_available") < helper_source.index("ctx.db.set_campaign_invite_link_settings")

    destination_start = HANDLERS_SOURCE.index("async def handle_rule_repost_campaign_invite_links_destination_set")
    destination_end = HANDLERS_SOURCE.index("@dp.callback_query", destination_start + 1)
    destination_source = HANDLERS_SOURCE[destination_start:destination_end]
    assert destination_source.index("_ensure_repost_campaign_rule_available") < destination_source.index("ctx.db.list_rule_repost_campaign_targets")
