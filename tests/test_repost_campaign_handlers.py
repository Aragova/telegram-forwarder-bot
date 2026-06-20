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
