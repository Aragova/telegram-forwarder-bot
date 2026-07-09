from pathlib import Path

from app.repost_campaign_ui import (
    build_repost_campaign_active_placements_view,
    build_repost_campaign_vip_features_view,
)


def _texts_from_keyboard(keyboard):
    return [button.text for row in keyboard.inline_keyboard for button in row]


def _callbacks_from_keyboard(keyboard):
    return [button.callback_data for row in keyboard.inline_keyboard for button in row]


def _placement(run_id: int, *, summary_text: str | None = None):
    return {
        "run_id": run_id,
        "summary_text": summary_text or f"#{run_id} · Запуск сейчас\n✅ Опубликовано: 3\n🧹 Ожидают удаления: 2\n⏳ Автоудаление: 07.06 18:30",
        "details_callback_data": f"rule_repost_campaign_history_detail:10:{run_id}",
        "delete_callback_data": f"rule_repost_campaign_run_delete_confirm:10:{run_id}",
        "report_callback_data": f"rule_repost_campaign_views_report:10:{run_id}",
    }


def _state(name: str, placements=None, **extra):
    placements = list(placements or [])
    base = {
        "ok": True,
        "state": name,
        "placements": placements,
        "placements_total": len(placements),
        "active_total": len(placements),
        "delete_problem_total": 0,
    }
    base.update(extra)
    return base


def test_clean_screen():
    text, keyboard = build_repost_campaign_active_placements_view(
        rule_id=10,
        state=_state("clean", [], active_total=0),
    )

    texts = _texts_from_keyboard(keyboard)
    callbacks = _callbacks_from_keyboard(keyboard)

    assert "Канал чист" in text
    assert "VIP функции" in " ".join(texts)
    assert "К кампании" in " ".join(texts)
    assert "🔄 Обновить" in texts
    assert f"rule_repost_campaign_active_placements:10:0" in callbacks
    assert not any(callback and "run_delete_confirm" in callback for callback in callbacks)


def test_active_screen_with_one_placement():
    placement = _placement(123)
    text, keyboard = build_repost_campaign_active_placements_view(
        rule_id=10,
        state=_state("active", [placement], active_total=1),
    )

    texts = _texts_from_keyboard(keyboard)
    callbacks = _callbacks_from_keyboard(keyboard)

    assert placement["summary_text"] in text
    assert "📄 #123" in texts
    assert "🧹 Удалить #123" in texts
    assert "📊 Отчёт #123" in texts
    assert "rule_repost_campaign_history_detail:10:123" in callbacks
    assert "rule_repost_campaign_run_delete_confirm:10:123" in callbacks
    assert "rule_repost_campaign_views_report:10:123" in callbacks


def test_delete_problem_screen():
    placement = _placement(123, summary_text="#123 · Запуск сейчас\n✅ Опубликовано: 3\n⚠️ Ошибки удаления: 1")
    text, keyboard = build_repost_campaign_active_placements_view(
        rule_id=10,
        state=_state("delete_problem", [placement], active_total=0, delete_problem_total=1),
    )

    callbacks = _callbacks_from_keyboard(keyboard)

    assert "Есть ошибки удаления" in text
    assert "Ошибки удаления" in text
    assert "rule_repost_campaign_run_delete_confirm:10:123" in callbacks


def test_mixed_screen():
    placement = _placement(
        123,
        summary_text="#123 · Запуск сейчас\n✅ Опубликовано: 3\n🧹 Ожидают удаления: 2\n⚠️ Ошибки удаления: 1\n⏳ Автоудаление: 07.06 18:30",
    )
    text, _ = build_repost_campaign_active_placements_view(
        rule_id=10,
        state=_state("mixed", [placement], active_total=1, delete_problem_total=1),
    )

    assert "активные размещения и ошибки удаления" in text
    assert "Ожидают удаления" in text
    assert "Ошибки удаления" in text


def test_multiple_placements_keep_order_and_callbacks():
    placements = [_placement(123), _placement(122), _placement(121)]
    text, keyboard = build_repost_campaign_active_placements_view(
        rule_id=10,
        state=_state("active", placements, active_total=3),
    )

    callbacks = _callbacks_from_keyboard(keyboard)

    for placement in placements:
        assert placement["summary_text"] in text
        run_id = placement["run_id"]
        assert f"rule_repost_campaign_history_detail:10:{run_id}" in callbacks
        assert f"rule_repost_campaign_run_delete_confirm:10:{run_id}" in callbacks
        assert f"rule_repost_campaign_views_report:10:{run_id}" in callbacks
    assert text.index("#123") < text.index("#122") < text.index("#121")


def test_second_page_shows_loaded_prefix_slice():
    placements = [_placement(run_id) for run_id in range(120, 100, -1)]
    text, keyboard = build_repost_campaign_active_placements_view(
        rule_id=10,
        state=_state(
            "active",
            placements,
            active_total=20,
            placements_total=20,
            page=1,
            page_size=10,
        ),
    )

    callbacks = _callbacks_from_keyboard(keyboard)

    assert "#120" not in text
    assert "#111" not in text
    assert "#110" in text
    assert "#101" in text
    assert "Показаны размещения 11–20 из 20." in text
    assert "rule_repost_campaign_history_detail:10:110" in callbacks
    assert "rule_repost_campaign_run_delete_confirm:10:101" in callbacks
    assert "rule_repost_campaign_active_placements:10:0" in callbacks


def test_error_screen():
    text, keyboard = build_repost_campaign_active_placements_view(
        rule_id=10,
        state={"ok": False, "state": "unknown", "error_text": "Traceback: database failed"},
    )

    texts = _texts_from_keyboard(keyboard)

    assert "Не удалось проверить канал" in text
    assert "Traceback" not in text
    assert "database failed" not in text
    assert "🔄 Обновить" in texts


def test_vip_menu_clean_channel_button_changed():
    text, keyboard = build_repost_campaign_vip_features_view(rule_id=10)
    callbacks = _callbacks_from_keyboard(keyboard)

    assert "rule_repost_campaign_clean_channel:10" in callbacks
    assert "rule_repost_campaign_active_placements:10:0" not in callbacks
    assert "ViMi удалит предыдущий активный рекламный пост" not in text
    assert "удалит предыдущий" not in text


def test_no_scheduled_post_service_coupling():
    source = Path("app/repost_campaign_scheduled_post_service.py").read_text(encoding="utf-8")

    assert "active_placements" not in source
    assert "RepostCampaignPlacementService" not in source
    assert "clean_channel_enabled" not in source
    assert "waiting_clean_channel" not in source


def _runtime_method_source(source: str, method_name: str) -> str:
    marker = f"\n    async def {method_name}("
    start = source.find(marker)
    if start < 0:
        marker = f"\n    def {method_name}("
        start = source.find(marker)
    assert start >= 0
    next_def = source.find("\n    def ", start + 1)
    next_async_def = source.find("\n    async def ", start + 1)
    candidates = [pos for pos in (next_def, next_async_def) if pos >= 0]
    end = min(candidates) if candidates else len(source)
    return source[start:end]


def test_handler_source_contains_active_placements_and_launch_flow_is_not_coupled():
    handlers_source = Path("app/repost_campaign_handlers.py").read_text(encoding="utf-8")
    runtime_source = Path("app/repost_campaign_runtime_service.py").read_text(encoding="utf-8")

    assert "rule_repost_campaign_active_placements" in handlers_source
    assert "RepostCampaignPlacementService" in handlers_source
    assert "build_clean_channel_state" not in runtime_source

    launch_source = _runtime_method_source(runtime_source, "launch_campaign_now")
    assert "RepostCampaignPlacementService" not in launch_source
    assert "build_launch_policy_preview" not in launch_source

from app.repost_campaign_ui import (
    build_repost_campaign_run_delete_failures_resolve_confirm_view,
    build_repost_campaign_run_delete_failures_resolve_result_view,
)


def test_delete_failed_placement_shows_resolve_button():
    placement = _placement(130)
    placement.update({"delete_pending": 0, "delete_processing": 0, "delete_failed": 1})
    _, keyboard = build_repost_campaign_active_placements_view(rule_id=10, state=_state("delete_problem", [placement], delete_problem_total=1))

    assert "🧯 Снять проблему #130" in _texts_from_keyboard(keyboard)


def test_pending_only_placement_shows_delete_not_resolve():
    placement = _placement(140)
    placement.update({"delete_pending": 1, "delete_processing": 0, "delete_failed": 0})
    _, keyboard = build_repost_campaign_active_placements_view(rule_id=10, state=_state("active", [placement], active_total=1))
    texts = _texts_from_keyboard(keyboard)

    assert "🧹 Удалить #140" in texts
    assert not any("🧯" in text for text in texts)


def test_mixed_placement_shows_delete_and_resolve():
    placement = _placement(140)
    placement.update({"delete_pending": 1, "delete_processing": 0, "delete_failed": 1})
    _, keyboard = build_repost_campaign_active_placements_view(rule_id=10, state=_state("mixed", [placement], active_total=1, delete_problem_total=1))
    texts = _texts_from_keyboard(keyboard)

    assert "🧹 Удалить #140" in texts
    assert "🧯 Снять проблему #140" in texts


def test_resolve_confirm_screen_warns_telegram_posts_not_deleted():
    text, _ = build_repost_campaign_run_delete_failures_resolve_confirm_view(rule_id=10, run_id=130, details={"summary": {"delete_failed": 1}})

    assert "Публикации в Telegram удаляться не будут" in text


def test_mixed_resolve_confirm_screen_warns_pending_stays_active():
    text, _ = build_repost_campaign_run_delete_failures_resolve_confirm_view(rule_id=10, run_id=130, details={"summary": {"delete_failed": 1, "delete_pending": 1}})

    assert "Ожидающие публикации останутся активными" in text


def test_resolve_result_screen_shows_counts():
    result = {"ok": True, "extra": {"resolved": 5, "remaining_pending": 42, "remaining_processing": 0, "remaining_failed": 0, "still_active": True}}
    text, _ = build_repost_campaign_run_delete_failures_resolve_result_view(rule_id=10, run_id=130, result=result)

    assert "Исправлено ошибок удаления" in text
    assert "Осталось ожидающих удаления" in text
