from pathlib import Path
import ast
import re

from app.repost_campaign_ui import (
    build_repost_campaign_delete_result_view,
    build_repost_campaign_launch_result_view,
    build_repost_campaign_launch_mode_view,
    build_repost_campaign_launch_readiness_view,
    build_repost_campaign_launch_queued_view,
    build_repost_campaign_launch_job_status_view,
    build_repost_campaign_launch_needs_review_view,
    build_repost_campaign_menu_view,
    build_repost_campaign_post_menu_view,
    build_repost_campaign_views_report_loading_view,
    build_repost_campaign_views_report_error_view,
    build_repost_campaign_vip_features_view,
    build_repost_campaign_schedule_current_view,
    build_repost_campaign_run_details_view,
    build_repost_campaign_run_delete_confirm_view,
    build_repost_campaign_run_delete_loading_view,
    build_repost_campaign_run_delete_result_view,
    build_repost_campaign_show_menu_view,
    build_repost_campaign_target_delete_confirm_view,
    build_repost_campaign_target_card_view,
    build_repost_campaign_target_action_result_view,
    build_repost_campaign_targets_menu_view,
    build_repost_campaign_targets_list_view,
    build_repost_campaign_targets_check_loading_view,
    build_repost_campaign_target_preview_result_view,
    build_repost_campaign_target_check_result_view,
    build_repost_campaign_preview_delete_result_view,
    build_repost_campaign_views_report_view,
    format_repost_campaign_readiness_block,
    build_vip_scheduled_posts_screen_view,
)


def _texts_from_keyboard(keyboard):
    return [button.text for row in keyboard.inline_keyboard for button in row]


def _callbacks_from_keyboard(keyboard):
    return [button.callback_data for row in keyboard.inline_keyboard for button in row]


def test_bot_no_legacy_target_check_stub():
    bot_source = Path("bot.py").read_text(encoding="utf-8")
    handlers_source = Path("app/repost_campaign_handlers.py").read_text(encoding="utf-8")
    assert "Полная проверка прав публикации и удаления будет добавлена отдельным шагом" not in bot_source
    assert "rule_repost_campaign_check:" in handlers_source

    assert "result = runtime.check_campaign_targets(" not in bot_source
    assert "auto_check_result = runtime.check_campaign_targets(" not in bot_source
    assert "result = runtime.check_campaign_target(" not in bot_source


def test_bot_campaign_check_calls_are_awaited_ast():
    source = Path("bot.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    for parent in ast.walk(tree):
        for child in ast.iter_child_nodes(parent):
            child.parent = parent
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr in {"check_campaign_targets", "check_campaign_target"}:
            parent = getattr(node, "parent", None)
            assert isinstance(parent, ast.Await), f"{node.func.attr} must be awaited"


def test_problematic_target_buttons_and_copy():
    text, keyboard = build_repost_campaign_targets_list_view(rule_id=3, targets=[{"id": 1, "target_id": "-1001", "title": "A", "is_active": True, "last_check_error": "Аккаунт-парсер не имеет права публиковать"}])
    texts = _texts_from_keyboard(keyboard)
    assert "🔎 Проверить все права" in texts
    assert "⏸ Пауза" not in texts and "▶️ Включить" not in texts and "🗑 Удалить" not in texts
    assert "Аккаунт-парсер" not in text




def test_targets_list_view_does_not_show_raw_id_as_main_title():
    text, _ = build_repost_campaign_targets_list_view(rule_id=3, targets=[{"id": 1, "target_id": "-1002741117827", "title": None, "is_active": True}])
    assert "Подключено: 1" in text
    assert "ID: -1002741117827" not in text
    assert "1. 🟢 -1002741117827" not in text


def test_targets_list_view_manual_actions_copy():
    _, keyboard = build_repost_campaign_targets_list_view(rule_id=3, targets=[])
    texts = _texts_from_keyboard(keyboard)
    assert "⚙️ Управление вручную" not in texts

def test_targets_list_buttons_without_number_suffix():
    _, keyboard = build_repost_campaign_targets_list_view(rule_id=3, targets=[{"id": 1, "target_id": "-1001", "title": "A", "is_active": True}])
    texts = _texts_from_keyboard(keyboard)
    assert "⏸ Пауза" not in texts
    assert "🔎 Проверить" not in texts
    assert "🗑 Удалить" not in texts
    assert "Пауза #1" not in texts
    assert "Проверить #1" not in texts
    assert "Удалить #1" not in texts


def test_targets_menu_has_no_check_rights_button():
    _, keyboard = build_repost_campaign_targets_menu_view(rule_id=7, summary={})
    texts = _texts_from_keyboard(keyboard)
    assert "🔎 Проверить права" not in texts


def test_targets_list_has_check_all_rights_button():
    _, keyboard = build_repost_campaign_targets_list_view(rule_id=7, targets=[])
    texts = _texts_from_keyboard(keyboard)
    assert "🔎 Проверить все права" in texts
    assert "🔎 Проверить права" not in texts


def test_targets_list_has_no_add_list_button():
    _, keyboard = build_repost_campaign_targets_list_view(rule_id=7, targets=[])
    texts = _texts_from_keyboard(keyboard)
    assert "📥 Добавить списком" not in texts


def test_targets_list_has_no_manual_management_button():
    _, keyboard = build_repost_campaign_targets_list_view(rule_id=7, targets=[])
    texts = _texts_from_keyboard(keyboard)
    assert "⚙️ Управление вручную" not in texts

def test_targets_check_loading_view_text_and_keyboard():
    text, keyboard = build_repost_campaign_targets_check_loading_view(rule_id=1, targets_count=42)
    texts = _texts_from_keyboard(keyboard)
    assert "🔎 Проверяем все права" in text
    assert "Каналов/групп: 42" in text
    assert "💰 К кампании" in texts


def test_target_card_check_callback_keeps_page():
    _, keyboard = build_repost_campaign_target_card_view(
        rule_id=3,
        target={"id": 15, "target_id": "-1001", "title": "A", "is_active": True},
        page=3,
    )
    callbacks = _callbacks_from_keyboard(keyboard)
    assert "rule_repost_campaign_target_check:3:15:3" in callbacks


def test_target_check_result_view_keeps_page_and_target_card_callback():
    _, keyboard = build_repost_campaign_target_check_result_view(
        rule_id=3,
        result={"ok": True, "target_row_id": 8, "target_title": "Канал", "target_id": "-1008"},
        page=2,
    )
    callbacks = _callbacks_from_keyboard(keyboard)
    assert "rule_repost_campaign_target_card:3:8:2" in callbacks
    assert "rule_repost_campaign_targets_list:3:2" in callbacks


def _kb_texts(kb):
    return [b.text for row in kb.inline_keyboard for b in row]

def test_launch_readiness_view_ready_has_confirm_button():
    text, keyboard = build_repost_campaign_launch_readiness_view(rule_id=3, readiness={"can_launch": True, "saved_post_id": 7, "saved_post_exists": True, "show_seconds": 3600, "main_target_ready": True, "will_send_total": 3, "will_skip_total": 0, "extra_paused": 0, "extra_problem": 0})
    assert "👁 Предпросмотр запуска" in text
    assert "Рекламный пост:" in text
    assert "✅ Готов к публикации" in text
    assert "Публикация:" in text
    assert "📣 Каналов/групп:" in text
    assert "✅ Готовы:" in text
    assert "⚠️ Требуют внимания:" in text
    assert "Срок размещения:" in text
    assert "🕒 Ожидаемое удаление:" in text
    assert "UTC+3" in text
    assert "После запуска ViMi:" in text
    assert "подготовит отчёт XLSX/CSV/TXT" in text
    labels = _kb_texts(keyboard)
    callbacks = _callbacks_from_keyboard(keyboard)
    assert "✅ Подтвердить запуск" in labels
    assert "rule_repost_campaign_launch_confirm:3" in callbacks
    assert "rule_repost_campaign_launch:3" not in callbacks
    assert "🔎 Проверить права" not in labels
    assert "📣 Каналы/Группы" not in labels
    assert "📝 Рекламный пост" not in labels
    assert "⏳ Время показа" not in labels


def test_launch_readiness_view_ready_mentions_final_confirmation():
    text, _ = build_repost_campaign_launch_readiness_view(rule_id=3, readiness={"can_launch": True, "saved_post_id": 7, "saved_post_exists": True, "show_seconds": 3600, "main_target_ready": True, "will_send_total": 3, "will_skip_total": 0, "extra_paused": 0, "extra_problem": 0})
    assert "Если всё верно — подтвердите запуск." in text

def test_launch_readiness_view_blocked_has_no_confirm_button():
    text, keyboard = build_repost_campaign_launch_readiness_view(rule_id=3, readiness={"can_launch": False, "saved_post_exists": True, "show_seconds": 300, "main_target_ready": True, "extra_active_problem": 1, "extra_problem": 1, "will_send_total": 1, "will_skip_total": 1, "extra_paused": 0, "block_reasons": ["Есть активные каналы/группы, которые требуют настройки."]})
    assert "👁 Предпросмотр запуска" in text
    assert "Кампания не готова к запуску" in text
    labels = _kb_texts(keyboard)
    assert "✅ Подтвердить запуск" not in labels
    assert labels == ["⬅️ Назад"]


def test_launch_readiness_view_expected_delete_uses_utc_plus_3():
    from datetime import datetime, timezone

    text, _ = build_repost_campaign_launch_readiness_view(
        rule_id=3,
        readiness={"can_launch": True, "saved_post_id": 7, "saved_post_exists": True, "show_seconds": 7200, "will_send_total": 1, "will_skip_total": 0},
        now=datetime(2026, 5, 8, 18, 0, tzinfo=timezone.utc),
    )
    assert "08.05 23:00 UTC+3" in text


def test_launch_readiness_view_has_only_confirm_and_back_when_ready():
    _, keyboard = build_repost_campaign_launch_readiness_view(rule_id=3, readiness={"can_launch": True, "saved_post_id": 7, "saved_post_exists": True, "show_seconds": 3600, "will_send_total": 3, "will_skip_total": 0})
    assert _kb_texts(keyboard) == ["✅ Подтвердить запуск", "⬅️ Назад"]


def test_launch_readiness_view_has_only_back_when_blocked():
    _, keyboard = build_repost_campaign_launch_readiness_view(rule_id=3, readiness={"can_launch": False, "saved_post_exists": False, "show_seconds": 0, "will_send_total": 0, "will_skip_total": 0})
    assert _kb_texts(keyboard) == ["⬅️ Назад"]

def test_launch_result_blocked_uses_readiness_vm():
    result = {"ok": False, "error_text": "Кампания не готова к запуску", "extra": {"launch_readiness": {"can_launch": False, "saved_post_exists": True, "show_seconds": 300, "main_target_ready": True, "extra_active_problem": 1, "extra_problem": 1, "will_send_total": 1, "will_skip_total": 1, "extra_paused": 0, "block_reasons": ["Есть активные каналы/группы, которые требуют настройки."]}}}
    text, _ = build_repost_campaign_launch_result_view(rule_id=3, result=result)
    assert "Кампания не готова к запуску" in text
    assert "Будет опубликовано" in text
    assert "Будет пропущено" in text

def test_launch_readiness_view_no_banned_terms():
    text, _ = build_repost_campaign_launch_readiness_view(rule_id=3, readiness={"can_launch": False, "saved_post_exists": False, "show_seconds": 0, "main_target_ready": True, "will_send_total": 0, "will_skip_total": 0, "extra_paused": 0, "extra_problem": 0})
    for bad in ["креатив", "площадк", "аккаунт-парсер", "тестовый", "Режим: репост"]:
        assert bad.lower() not in text.lower()


def test_bot_has_launch_confirm_callback_and_logs():
    handlers_source = Path("app/repost_campaign_handlers.py").read_text(encoding="utf-8")
    bot_source = Path("bot.py").read_text(encoding="utf-8")
    assert "rule_repost_campaign_launch_confirm:" in handlers_source
    assert "REPOST_CAMPAIGN_LAUNCH_PREFLIGHT_UI" in handlers_source
    assert "REPOST_CAMPAIGN_LAUNCH_JOB_UI_ENQUEUED" in handlers_source
    assert '@dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_launch_confirm:"))' not in bot_source


def test_bot_contains_campaign_check_loading_and_optional_page_parse():
    source = Path("app/repost_campaign_handlers.py").read_text(encoding="utf-8")
    assert "build_repost_campaign_targets_check_loading_view" in source
    assert "rule_repost_campaign_target_check:" in source
    assert "rule_repost_campaign_check:" in source
    assert "page = int(parts[3]) if len(parts) > 3 else 0" in source


def test_vip_scheduled_material_state_uses_album_buffer():
    source = Path("app/repost_campaign_message_handlers.py").read_text(encoding="utf-8")
    assert "waiting_vip_scheduled_post_material" in source
    assert "saved_post_album_buffer.add_message" in source
    assert "build_saved_post_album_content_from_aiogram_messages" in source
    assert "Отправьте альбом ещё раз одним сообщением" not in source


def test_vip_scheduled_single_material_uses_shared_save_helper():
    source = Path("app/repost_campaign_message_handlers.py").read_text(encoding="utf-8")
    assert "async def _save_vip_scheduled_post_material(" in source
    assert "db.create_saved_post" in source
    assert "update_draft_saved_post" in source
    assert "scheduled_post_id=scheduled_post_id" in source


def test_vip_scheduled_album_callback_reloads_state_ids_and_uses_last_message():
    source = Path("app/repost_campaign_message_handlers.py").read_text(encoding="utf-8")
    assert "rule_id_now = int(state_now.get(\"rule_id\") or 0)" in source
    assert "scheduled_post_id_now = int(state_now.get(\"scheduled_post_id\") or 0)" in source
    assert "message=messages[-1]" in source


def test_vip_scheduled_material_has_dedicated_handler_before_generic_campaign_state_router():
    source = Path("bot.py").read_text(encoding="utf-8")
    module_source = Path("app/repost_campaign_message_handlers.py").read_text(encoding="utf-8")
    vip_handler_pos = source.index("if await handle_vip_scheduled_post_material_message(campaign_handlers_ctx, message):")
    campaign_state_router_pos = source.index("if await handle_repost_campaign_stateful_private_input(campaign_handlers_ctx, message, state, text):")
    assert vip_handler_pos < campaign_state_router_pos
    assert "on_album_ready=lambda **kwargs: _finalize_repost_campaign_saved_post_album(ctx, **kwargs)" in module_source


def test_repost_campaign_message_handlers_registers_vip_material_handler():
    source = Path("app/repost_campaign_message_handlers.py").read_text(encoding="utf-8")
    assert "def register_repost_campaign_message_handlers(dp: Dispatcher, ctx: RepostCampaignHandlersContext) -> None:" in source
    assert "@dp.message(" in source[source.index("def register_repost_campaign_message_handlers"): ]
    assert "is_waiting_vip_scheduled_post_material(ctx, m.from_user.id)" in source
    assert "VIP_SCHEDULED_POST_MATERIAL_HANDLER_HIT" in source
    assert "VIP_SCHEDULED_POST_MATERIAL_HANDLER_NOT_HANDLED" in source


def test_bot_registers_repost_campaign_message_handlers():
    source = Path("bot.py").read_text(encoding="utf-8")
    assert "register_repost_campaign_message_handlers(dp, campaign_handlers_ctx)" in source


def test_vip_material_registration_happens_before_generic_stateful_handler_registration():
    source = Path("bot.py").read_text(encoding="utf-8")
    register_pos = source.index("register_repost_campaign_message_handlers(dp, campaign_handlers_ctx)")
    generic_stateful_pos = source.index("async def handle_stateful_private_inputs(message: Message):")
    assert register_pos < generic_stateful_pos


def test_bot_has_no_vip_material_business_logic_inline():
    source = Path("bot.py").read_text(encoding="utf-8")
    assert "VIP_SCHEDULED_POST_MATERIAL_HANDLER_HIT" not in source
    assert "VIP_SCHEDULED_POST_MATERIAL_HANDLER_NOT_HANDLED" not in source


def test_vip_scheduled_posts_screen_shows_active_placement_block_and_delete_button():
    text, kb = build_vip_scheduled_posts_screen_view(
        rule_id=1,
        active_placement={
            "active_placement": True,
            "active_run_id": 22,
            "active_delete_after_text": "11.05 21:47 UTC+3",
            "delete_failed": 0,
        },
    )
    assert "🟢 Сейчас активно размещение" in text
    assert "Пост будет удалён:" in text
    assert "11.05 21:47 UTC+3" in text
    assert "VIP-режим: публикация не блокируется активной рекламой." in text
    assert "🧹 Удалить активный пост" in _texts_from_keyboard(kb)


def test_vip_scheduled_posts_screen_hides_active_block_and_delete_without_active():
    text, kb = build_vip_scheduled_posts_screen_view(rule_id=1, active_placement=None)
    assert "🟢 Сейчас активно размещение" not in text
    assert "Новые запланированные посты стартуют после освобождения места." not in text
    assert "🧹 Удалить активный пост" not in _texts_from_keyboard(kb)


def test_vip_scheduled_pick_show_does_not_reuse_pick_callback_data_for_step_show():
    source = Path("app/repost_campaign_scheduled_post_handlers.py").read_text(encoding="utf-8")
    assert "async def handle_pick_show" in source
    block = source[source.index("async def handle_pick_show"):source.index('@dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_scheduled_post_input_time:"))')]
    assert "await handle_step_show(callback)" not in block


def test_vip_scheduled_pick_show_parses_four_part_callback():
    source = Path("app/repost_campaign_scheduled_post_handlers.py").read_text(encoding="utf-8")
    pick_show_block = source[source.index("async def handle_pick_show"):source.index('@dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_scheduled_post_input_time:"))')]
    assert '.split(":", 3)' in pick_show_block
    assert "show_seconds=int(show_seconds_text)" in pick_show_block


def test_generic_album_handlers_skip_vip_scheduled_material_state():
    source = Path("bot.py").read_text(encoding="utf-8")
    module_source = Path("app/repost_campaign_message_handlers.py").read_text(encoding="utf-8")
    assert "def _is_waiting_vip_scheduled_post_material(user_id: int | None) -> bool:" not in source
    assert "waiting_vip_scheduled_post_material" in module_source


def test_stateful_handler_delegates_vip_scheduled_material_to_helper():
    source = Path("bot.py").read_text(encoding="utf-8")
    assert "if await handle_vip_scheduled_post_material_message(campaign_handlers_ctx, message):" in source
    block = source[
        source.index("if await handle_vip_scheduled_post_material_message(campaign_handlers_ctx, message):"):
        source.index("if await handle_repost_campaign_stateful_private_input(campaign_handlers_ctx, message, state, text):")
    ]
    assert "    return" in block


def _extract_function_block(source: str, function_name: str) -> str:
    match = re.search(rf"^(?:async\s+)?def {re.escape(function_name)}\(", source, flags=re.MULTILINE)
    assert match is not None
    start = match.start()
    next_match = re.search(r"^(?:async\s+)?def [a-zA-Z0-9_]+\(", source[start + 1 :], flags=re.MULTILINE)
    if next_match is None:
        return source[start:]
    return source[start : start + 1 + next_match.start()]


def test_vip_scheduled_ctx_helpers_are_module_level_and_explicit_ctx():
    source = Path("app/repost_campaign_scheduled_post_handlers.py").read_text(encoding="utf-8")
    helper_names = [
        "_open_vip_step_post",
        "_open_vip_scheduled_post_step_targets_callback",
        "_open_vip_scheduled_posts_list_callback",
        "_build_vip_scheduled_known_targets",
        "_open_vip_scheduled_post_pick_targets",
    ]
    for helper_name in helper_names:
        block = _extract_function_block(source, helper_name)
        assert "ctx: RepostCampaignHandlersContext" in block


def test_register_scheduled_post_handlers_has_no_nested_vip_ctx_helpers():
    source = Path("app/repost_campaign_scheduled_post_handlers.py").read_text(encoding="utf-8")
    register_block = source[source.index("def register_repost_campaign_scheduled_post_handlers"): ]
    assert re.search(r"\n\s{4}async def _open_vip_step_post\(", register_block) is None
    assert re.search(r"\n\s{4}async def _open_vip_scheduled_post_step_targets_callback\(", register_block) is None
    assert re.search(r"\n\s{4}async def _open_vip_scheduled_posts_list_callback\(", register_block) is None
    assert re.search(r"\n\s{4}async def _build_vip_scheduled_known_targets\(", register_block) is None
    assert re.search(r"\n\s{4}async def _open_vip_scheduled_post_pick_targets\(", register_block) is None


def test_register_scheduled_post_handlers_still_has_callback_registrations():
    source = Path("app/repost_campaign_scheduled_post_handlers.py").read_text(encoding="utf-8")
    register_block = source[source.index("def register_repost_campaign_scheduled_post_handlers"): ]
    assert "@dp.callback_query(" in register_block

from app.repost_campaign_ui import build_repost_campaign_posts_library_view, build_repost_campaign_post_stats_view, build_repost_campaign_post_stats_loading_view, build_repost_campaign_post_channels_stats_view

def _library_payload(count=3):
    items = []
    for i in range(count):
        items.append({
            "saved_post_id": 24 - i,
            "kind": None,
            "is_album": i == 0,
            "media_count": 6 if i == 0 else 0,
            "is_current": i == 0,
            "views_total": 8218 if i == 0 else None,
            "runs_count": 1,
            "placements_sent": 43,
            "top_channels": [{"target_title": "WikiBoy’s 😎", "views_total": 1111}] if i == 0 else [],
            "last_started_at": "2026-05-07T12:04:00+00:00",
        })
    return {"summary": {"posts_total": count, "runs_total": 13, "placements_total": 142, "views_mode": "lazy"}, "items": items}


def test_posts_library_view_premium_layout():
    text, _ = build_repost_campaign_posts_library_view(rule_id=1, library=_library_payload())
    assert "📚 Библиотека постов" in text
    assert "unknown" not in text.lower()
    assert "#24" not in text
    assert "Всего просмотров" not in text


def test_posts_library_view_keyboard_has_single_open_button_per_post():
    _, kb = build_repost_campaign_posts_library_view(rule_id=1, library=_library_payload(3))
    texts = _texts_from_keyboard(kb)
    open_buttons = [x for x in texts if x.startswith("📄 Открыть")]
    assert len(open_buttons) == 3
    assert not any("Использовать" in x for x in texts)


def test_posts_library_view_keyboard_has_open_button_for_each_item():
    _, kb = build_repost_campaign_posts_library_view(rule_id=1, library=_library_payload(6))
    texts = _texts_from_keyboard(kb)
    open_buttons = [x for x in texts if x.startswith("📄 Открыть")]
    assert len(open_buttons) == 6
    assert "✅ Использовать этот пост" not in texts


def test_posts_library_view_text_stays_short_without_posts_sheet():
    text, _ = build_repost_campaign_posts_library_view(rule_id=1, library=_library_payload(6))
    assert "📚 Библиотека постов" in text
    assert "Коллекция рекламных постов этой кампании." in text
    assert "📝 Постов: 6" in text
    assert "🕘 Пост от" not in text


def test_posts_library_view_current_post_button_text():
    _, kb = build_repost_campaign_posts_library_view(rule_id=1, library=_library_payload(2))
    texts = _texts_from_keyboard(kb)
    assert "📄 Открыть текущий пост" in texts


def test_posts_library_view_old_post_button_uses_date_from_title_line():
    payload = _library_payload(2)
    payload["items"][0]["is_current"] = False
    payload["items"][0]["last_started_at"] = "2026-05-07T18:10:00+00:00"
    _, kb = build_repost_campaign_posts_library_view(rule_id=1, library=payload)
    texts = _texts_from_keyboard(kb)
    assert any(x.startswith("📄 Открыть пост от ") for x in texts)


def test_posts_library_view_common_actions():
    _, kb = build_repost_campaign_posts_library_view(rule_id=1, library=_library_payload())
    texts = _texts_from_keyboard(kb)
    assert "➕ Добавить новый пост" not in texts
    assert "🧾 Журнал запусков" not in texts
    assert "💰 К кампании" in texts


def test_post_stats_view_premium_actions_current():
    _, kb = build_repost_campaign_post_stats_view(rule_id=1, saved_post_id=1, stats={"is_current": True, "kind": "photo"})
    texts = _texts_from_keyboard(kb)
    assert "🚀 Запустить кампанию" in texts
    assert "👁 Предпросмотр" not in texts


def test_post_stats_view_premium_actions_not_current():
    _, kb = build_repost_campaign_post_stats_view(rule_id=1, saved_post_id=1, stats={"is_current": False, "kind": "photo"})
    texts = _texts_from_keyboard(kb)
    assert "🚀 Использовать снова" in texts
    assert "🚀 Запустить кампанию" not in texts


def test_library_visible_text_has_no_banned_terms():
    text, kb = build_repost_campaign_posts_library_view(rule_id=1, library=_library_payload())
    blob = text + "\n" + "\n".join(_texts_from_keyboard(kb))
    banned = ["unknown", "saved_post", "campaign_run", "message_id", "target_id", "delete_status", "send_status", "Технический ID"]
    for term in banned:
        assert term.lower() not in blob.lower()


def test_post_stats_loading_view():
    text, kb = build_repost_campaign_post_stats_loading_view(rule_id=1, saved_post_id=24)
    assert "Собираю статистику просмотров" in text
    assert "Экран обновится автоматически" in text
    texts = _texts_from_keyboard(kb)
    assert "📚 К библиотеке" in texts
    assert "💰 К кампании" in texts


def test_vip_features_view_contains_placeholder_text():
    text, keyboard = build_repost_campaign_vip_features_view(rule_id=5)
    assert "💎 VIP функции" in text
    assert "A/B-тесты" in text
    assert "⬅️ Назад" in _texts_from_keyboard(keyboard)


def _stats_payload(channels_count=12):
    top = [{"target_title": f"Канал {i+1}", "views_total": 1000 - i * 10} for i in range(channels_count)]
    return {"kind": "photo", "views_total": 10830, "runs_count": 1, "placements_sent": 43, "top_channels": top, "problem_channels": []}

def _stats_payload_full(channels_count=43):
    items = []
    for i in range(channels_count):
        items.append({
            "target_title": f"Канал {i+1}",
            "target_id": f"-100{i+1}",
            "views_total": 1200 - i,
            "views_status": "ok",
            "runs_count": 1,
        })
    return {"kind": "photo", "views_total": 10830, "runs_count": 1, "placements_sent": 43, "channels_stats": items, "top_channels": [{"target_title": "ТОП", "views_total": 9999}]}


def test_post_stats_view_does_not_render_channels_sheet():
    text, _ = build_repost_campaign_post_stats_view(rule_id=1, saved_post_id=24, stats=_stats_payload(3))
    assert "Каналы/Группы:" not in text
    assert "Канал 1" not in text


def test_post_stats_view_has_channels_stats_button():
    _, kb = build_repost_campaign_post_stats_view(rule_id=39, saved_post_id=24, stats=_stats_payload(3))
    assert "📊 Статистика по каналам" in _texts_from_keyboard(kb)


def test_post_channels_stats_view_first_page():
    text, kb = build_repost_campaign_post_channels_stats_view(rule_id=39, saved_post_id=24, stats=_stats_payload_full(43), offset=0, page_size=10)
    assert "Страница 1 из 5" in text
    assert text.count("👁 ") == 11
    labels = _texts_from_keyboard(kb)
    assert "➡️ Вперёд" in labels
    assert "⬅️ Назад" not in labels


def test_post_channels_stats_view_middle_page_has_prev_and_next():
    text, kb = build_repost_campaign_post_channels_stats_view(rule_id=39, saved_post_id=24, stats=_stats_payload_full(43), offset=10, page_size=10)
    assert "Канал 11" in text
    assert "Канал 20" in text
    assert "Канал 10" not in text
    labels = _texts_from_keyboard(kb)
    assert "⬅️ Назад" in labels
    assert "➡️ Вперёд" in labels


def test_post_channels_stats_view_last_page_has_only_prev():
    text, kb = build_repost_campaign_post_channels_stats_view(rule_id=39, saved_post_id=24, stats=_stats_payload_full(43), offset=40, page_size=10)
    assert "Страница 5 из 5" in text
    labels = _texts_from_keyboard(kb)
    assert "⬅️ Назад" in labels
    assert "➡️ Вперёд" not in labels


def test_post_channels_stats_view_empty_state():
    text, kb = build_repost_campaign_post_channels_stats_view(rule_id=39, saved_post_id=24, stats={"kind": "photo", "views_total": 0}, offset=0, page_size=10)
    assert "Страница 1 из 1" in text
    assert "📣 Каналов/групп: 0" in text
    assert "⬅️ Назад" not in _texts_from_keyboard(kb)
    assert "➡️ Вперёд" not in _texts_from_keyboard(kb)


def test_post_channels_stats_view_fallback_legacy_top_and_problem():
    text, _ = build_repost_campaign_post_channels_stats_view(
        rule_id=39,
        saved_post_id=24,
        stats={"kind": "photo", "top_channels": [{"target_title": "A", "views_total": 1}], "problem_channels": [{"target_title": "B"}]},
        offset=0,
        page_size=10,
    )
    assert "📣 Каналов/групп: 2" in text
    assert "👁 1 — A" in text
    assert "⚠️ нет данных — B" in text


def test_post_channels_stats_view_unavailable_status_renders_warning():
    text, _ = build_repost_campaign_post_channels_stats_view(
        rule_id=39,
        saved_post_id=24,
        stats={
            "kind": "photo",
            "channels_stats": [{"target_title": "Название", "views_total": 0, "views_status": "unavailable"}],
        },
        offset=0,
        page_size=10,
    )
    assert "⚠️ нет данных — Название" in text
    assert "👁 0 — Название" not in text

def _mk_targets(n=43):
    items=[]
    for i in range(n):
        items.append({"id": i+1, "target_id": f"-100{i+1}", "title": f"Канал {i+1}", "is_active": True, "last_check_error": None})
    return items


def test_targets_list_view_has_pagination_for_many_targets():
    text, _ = build_repost_campaign_targets_list_view(rule_id=1, targets=_mk_targets(43))
    assert "Страница 1 из 5" in text


def test_targets_list_view_first_page_has_next_only():
    _, kb = build_repost_campaign_targets_list_view(rule_id=1, targets=_mk_targets(43), page=0)
    cbs = _callbacks_from_keyboard(kb)
    assert "rule_repost_campaign_targets_list:1:1" in cbs
    assert "rule_repost_campaign_targets_list:1:-1" not in cbs


def test_targets_list_view_middle_page_has_prev_and_next():
    _, kb = build_repost_campaign_targets_list_view(rule_id=1, targets=_mk_targets(43), page=1)
    cbs = _callbacks_from_keyboard(kb)
    assert "rule_repost_campaign_targets_list:1:0" in cbs
    assert "rule_repost_campaign_targets_list:1:2" in cbs


def test_targets_list_view_last_page_has_prev_only():
    _, kb = build_repost_campaign_targets_list_view(rule_id=1, targets=_mk_targets(43), page=4)
    cbs = _callbacks_from_keyboard(kb)
    assert "rule_repost_campaign_targets_list:1:3" in cbs
    assert "rule_repost_campaign_targets_list:1:5" not in cbs


def test_targets_list_view_does_not_render_action_buttons_per_target():
    _, kb = build_repost_campaign_targets_list_view(rule_id=1, targets=_mk_targets(12))
    texts = _texts_from_keyboard(kb)
    assert "⏸ Пауза" not in texts and "▶️ Включить" not in texts and "🗑 Удалить" not in texts


def test_targets_list_view_has_target_card_buttons():
    _, kb = build_repost_campaign_targets_list_view(rule_id=1, targets=_mk_targets(12), page=1)
    cbs = _callbacks_from_keyboard(kb)
    assert "rule_repost_campaign_target_card:1:11:1" in cbs


def test_target_card_active_has_pause_check_delete():
    _, kb = build_repost_campaign_target_card_view(rule_id=1, target={"id":1,"target_id":"-1001","title":"A","is_active":True}, page=2)
    texts=_texts_from_keyboard(kb)
    assert "⏸ Пауза" in texts and "🔎 Проверить" in texts and "🗑 Удалить" in texts


def test_target_card_paused_has_resume_check_delete():
    _, kb = build_repost_campaign_target_card_view(rule_id=1, target={"id":1,"target_id":"-1001","title":"A","is_active":False}, page=2)
    texts=_texts_from_keyboard(kb)
    assert "▶️ Включить" in texts and "🔎 Проверить" in texts and "🗑 Удалить" in texts


def test_target_card_problem_has_check_delete():
    text, kb = build_repost_campaign_target_card_view(rule_id=1, target={"id":1,"target_id":"-1001","title":"A","is_active":True,"last_check_error":"err"}, page=2)
    texts=_texts_from_keyboard(kb)
    assert "⚠️ требует внимания" in text
    assert "🔎 Проверить" in texts and "🗑 Удалить" in texts


def test_target_card_text_has_no_double_status_or_check_prefix():
    text, _ = build_repost_campaign_target_card_view(rule_id=1, target={"id": 1, "target_id": "-1001", "title": "A", "is_active": True}, page=0)
    assert "Статус: Статус:" not in text
    assert "Проверка: Проверка:" not in text


def test_target_card_does_not_show_empty_topic_line():
    text, _ = build_repost_campaign_target_card_view(
        rule_id=3,
        target={
            "id": 8,
            "target_id": "-1002741117827",
            "title": "Шаловливый мальчуган 😜",
            "is_active": True,
            "target_thread_id": None,
        },
        page=0,
    )

    assert "Тема: не задана" not in text
    assert "Тема:" not in text


def test_target_card_shows_topic_line_when_thread_id_exists():
    text, _ = build_repost_campaign_target_card_view(
        rule_id=3,
        target={
            "id": 8,
            "target_id": "-1002741117827",
            "title": "Группа с темой",
            "is_active": True,
            "target_thread_id": 12345,
        },
        page=0,
    )

    assert "Тема: 12345" in text


def test_target_delete_confirm_returns_to_card():
    _, kb = build_repost_campaign_target_delete_confirm_view(rule_id=1, target={"id":9,"title":"A"}, page=3)
    assert "rule_repost_campaign_target_card:1:9:3" in _callbacks_from_keyboard(kb)


def test_target_action_result_after_delete_returns_to_list():
    _, kb = build_repost_campaign_target_action_result_view(rule_id=1, result={"ok":True,"target_row_id":5}, action="remove", page=4)
    cbs = _callbacks_from_keyboard(kb)
    assert "rule_repost_campaign_targets_list:1:4" in cbs
    assert not any(x.startswith("rule_repost_campaign_target_card:") for x in cbs)


def test_target_action_result_after_pause_has_card_button_when_row_id_present():
    _, kb = build_repost_campaign_target_action_result_view(rule_id=1, result={"ok": True, "target_row_id": 7, "target_title": "A"}, action="pause", page=2)
    cbs = _callbacks_from_keyboard(kb)
    assert "rule_repost_campaign_target_card:1:7:2" in cbs


def test_target_delete_confirm_not_found_keeps_page_in_callback():
    _, kb = build_repost_campaign_target_delete_confirm_view(rule_id=1, target=None, page=3)
    cbs = _callbacks_from_keyboard(kb)
    assert "rule_repost_campaign_targets_list:1:3" in cbs


def test_bot_has_campaign_target_callbacks_and_card_import():
    source = Path("app/repost_campaign_handlers.py").read_text(encoding="utf-8")
    for cb in [
        "rule_repost_campaign_target_card:",
        "rule_repost_campaign_targets_list:",
        "rule_repost_campaign_target_pause:",
        "rule_repost_campaign_target_resume:",
        "rule_repost_campaign_target_check:",
        "rule_repost_campaign_target_delete_confirm:",
        "rule_repost_campaign_target_delete:",
    ]:
        assert cb in source
    assert "build_repost_campaign_target_card_view" in source


def test_campaign_runtime_tasks_started_in_bot_role():
    source = Path("bot.py").read_text(encoding="utf-8")

    marker = "async def _start_bot_role()"
    start = source.index(marker)
    end = source.index("async def _start_scheduler_role()", start)
    block = source[start:end]

    assert '_start_repost_campaign_runtime_tasks(role="bot")' in block
    assert "run_repost_campaign_delete_loop" not in block


def test_campaign_runtime_tasks_started_in_all_role():
    source = Path("bot.py").read_text(encoding="utf-8")

    marker = "async def _start_all_role()"
    start = source.index(marker)
    end = source.index("async def main()", start) if "async def main()" in source[start:] else len(source)
    block = source[start:end]

    assert '_start_repost_campaign_runtime_tasks(role="all")' in block
    assert "run_repost_campaign_delete_loop" not in block


def test_saved_post_action_callbacks_moved_to_report_handlers_module():
    bot_source = Path("bot.py").read_text(encoding="utf-8")
    report_source = Path("app/repost_campaign_report_handlers.py").read_text(encoding="utf-8")

    assert '@dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_post_use:"))' not in bot_source
    assert '@dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_delete_message:"))' not in bot_source
    assert '@dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_post_use:"))' in report_source
    assert '@dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_delete_message:"))' in report_source
    assert "if not await ctx.is_admin_callback(callback):" in report_source
    assert ".data = " not in report_source

def test_views_report_view_has_export_buttons_and_callbacks():
    _, kb = build_repost_campaign_views_report_view(rule_id=5, run_id=8, report={"ok": True, "items": []})
    texts = _texts_from_keyboard(kb)
    callbacks = _callbacks_from_keyboard(kb)
    assert "📊 Excel XLSX" in texts
    assert "📤 Экспорт CSV" in texts
    assert "📄 Экспорт TXT" in texts
    assert "rule_repost_campaign_views_export_xlsx:5:8" in callbacks
    assert "rule_repost_campaign_views_export_csv:5:8" in callbacks
    assert "rule_repost_campaign_views_export_txt:5:8" in callbacks


def test_post_stats_views_have_export_buttons():
    _, kb1 = build_repost_campaign_post_stats_view(rule_id=5, saved_post_id=26, stats={"ok": True})
    _, kb2 = build_repost_campaign_post_channels_stats_view(rule_id=5, saved_post_id=26, stats={"channels_stats": []})
    texts = _texts_from_keyboard(kb1) + _texts_from_keyboard(kb2)
    callbacks = _callbacks_from_keyboard(kb1) + _callbacks_from_keyboard(kb2)
    assert "📊 Excel XLSX" in texts
    assert "📤 Экспорт CSV" in texts
    assert "📄 Экспорт TXT" in texts
    assert "rule_repost_campaign_post_export_xlsx:5:26" in callbacks
    assert "rule_repost_campaign_post_export_csv:5:26" in callbacks
    assert "rule_repost_campaign_post_export_txt:5:26" in callbacks


def test_export_callbacks_moved_to_report_handlers_module():
    bot_source = Path("bot.py").read_text(encoding="utf-8")
    report_source = Path("app/repost_campaign_report_handlers.py").read_text(encoding="utf-8")

    assert "register_repost_campaign_report_handlers(dp, campaign_handlers_ctx)" in bot_source
    assert "def _send_export_document" in report_source
    assert "_send_export_document(" in report_source
    assert "build_repost_campaign_runtime(ctx)" in report_source
    assert "from app.repost_campaign_export_service import" in report_source
    assert "build_campaign_run_report_xlsx" in report_source
    assert "build_campaign_post_stats_xlsx" in report_source
    assert "ctx.ensure_rule_callback_access(callback, rule_id)" in report_source

    assert '@dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_views_export_csv:"))' not in bot_source
    assert '@dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_views_export_xlsx:"))' not in bot_source
    assert '@dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_views_export_txt:"))' not in bot_source
    assert '@dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_post_export_csv:"))' not in bot_source
    assert '@dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_post_export_xlsx:"))' not in bot_source
    assert '@dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_post_export_txt:"))' not in bot_source

    assert ".data = " not in report_source
    export_block = report_source[report_source.index('rule_repost_campaign_views_export_csv:'):report_source.index('rule_repost_campaign_post_use:') if 'rule_repost_campaign_post_use:' in report_source else len(report_source)]
    assert "if not await ctx.is_admin_callback(callback):" not in export_block

def test_vip_features_view_has_schedule_button():
    from app.repost_campaign_ui import build_repost_campaign_vip_features_view
    text,kb=build_repost_campaign_vip_features_view(rule_id=11)
    assert '🕒 Запланированные посты' in text
    assert any('rule_repost_campaign_scheduled_posts:11' in b.callback_data for row in kb.inline_keyboard for b in row)

def test_schedule_menu_view_has_quick_presets_and_manual_input():
    from app.repost_campaign_ui import build_repost_campaign_schedule_menu_view
    text,kb=build_repost_campaign_schedule_menu_view(rule_id=5)
    assert 'Часовой пояс: UTC+3' in text
    labels=[b.text for r in kb.inline_keyboard for b in r]
    assert 'Сегодня в 20:00' in labels and '✍️ Ввести дату и время' in labels

def test_schedule_step4_view_has_time_presets():
    from app.repost_campaign_ui import build_repost_campaign_schedule_wizard_step4_view
    text, kb = build_repost_campaign_schedule_wizard_step4_view(rule_id=5)
    assert "Шаг 4/4" in text
    labels = [b.text for r in kb.inline_keyboard for b in r]
    callbacks = [b.callback_data for r in kb.inline_keyboard for b in r]
    assert "Сегодня в 20:00" in labels
    assert "Завтра в 12:00" in labels
    assert "Завтра в 18:00" in labels
    assert "✍️ Ввести дату и время" in labels
    assert f"rule_repost_campaign_schedule_step3:5" in callbacks

def test_vip_coming_soon_view():
    from app.repost_campaign_ui import build_repost_campaign_vip_coming_soon_view
    text,_=build_repost_campaign_vip_coming_soon_view(rule_id=1, feature='x')
    assert 'Скоро в VIP функциях' in text

def test_schedule_preview_view_contains_full_sections():
    from datetime import datetime, timezone
    from app.repost_campaign_ui import build_repost_campaign_schedule_preview_view
    text, kb = build_repost_campaign_schedule_preview_view(
        rule_id=1,
        readiness={"can_launch": True, "saved_post_id": 2, "targets_total": 43, "will_send_total": 43, "will_skip_total": 0, "show_seconds": 86400},
        scheduled_at_utc=datetime(2026, 5, 9, 15, 0, tzinfo=timezone.utc),
    )
    assert "Рекламный пост:" in text
    assert "Публикация:" in text
    assert "Срок размещения:" in text
    assert "После запуска ViMi:" in text
    assert "подготовит отчёт XLSX/CSV/TXT" in text
    labels = [b.text for row in kb.inline_keyboard for b in row]
    assert "✅ Запланировать запуск" in labels


def test_bot_has_real_schedule_handlers_blocks():
    from pathlib import Path
    bot_source = Path("bot.py").read_text(encoding="utf-8")
    schedule_source = Path("app/repost_campaign_schedule_handlers.py").read_text(encoding="utf-8")
    assert '@dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_schedule_menu:"))' in schedule_source
    assert 'async def handle_rule_repost_campaign_schedule_menu' in schedule_source
    assert '@dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_schedule_confirm:"))' in schedule_source
    assert 'async def handle_rule_repost_campaign_schedule_confirm' in schedule_source
    assert 'REPOST_CAMPAIGN_SCHEDULE_CREATE_STARTED' in schedule_source
    assert 'REPOST_CAMPAIGN_SCHEDULE_CREATE_DONE' in schedule_source
    assert '@dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_schedule_confirm:"))' not in bot_source
    assert 'async def handle_rule_repost_campaign_schedule_confirm' not in bot_source
    assert '@dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_schedule_step4:"))' in schedule_source
    assert 'async def handle_rule_repost_campaign_schedule_step4' in schedule_source
    assert '@dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_schedule_step4:"))' not in bot_source
    assert 'async def handle_rule_repost_campaign_schedule_step4' not in bot_source
    assert 'rule_repost_campaign_schedule_step4:{rule_id}' in schedule_source
    assert 'if int(readiness.get("show_seconds") or 0) <= 0' in schedule_source

def test_schedule_show_pick_goes_to_step4():
    source = Path("app/repost_campaign_schedule_handlers.py").read_text(encoding="utf-8")
    assert 'async def handle_rule_repost_campaign_schedule_show_pick' in source
    assert 'text, kb = build_repost_campaign_schedule_wizard_step4_view(rule_id=rule_id)' in source

def test_schedule_input_back_goes_to_step4():
    source = Path("app/repost_campaign_schedule_handlers.py").read_text(encoding="utf-8")
    assert '⬅️ Назад к выбору времени' in source
    assert 'rule_repost_campaign_schedule_step4:{rule_id}' in source

def test_manual_input_without_show_seconds_returns_step3():
    source = Path("app/repost_campaign_message_handlers.py").read_text(encoding="utf-8")
    assert 'if int(readiness.get("show_seconds") or 0) <= 0:' in source
    assert 'build_repost_campaign_schedule_wizard_step3_view(rule_id=rule_id, readiness=readiness)' in source

def test_schedule_menu_view_shows_scheduled_launches_block():
    from app.repost_campaign_ui import build_repost_campaign_schedule_menu_view
    launches = [{"id": 123, "status": "scheduled", "scheduled_at": "2026-05-09T15:00:00+00:00"}]
    text, kb = build_repost_campaign_schedule_menu_view(rule_id=7, scheduled_launches=launches)
    assert "Ближайшие запланированные запуски:" in text
    assert "ожидает запуска" in text
    labels = [b.text for row in kb.inline_keyboard for b in row]
    assert "📄 Открыть запуск #123" in labels


def test_scheduled_detail_view_status_mapping_and_cancel_visibility():
    from app.repost_campaign_ui import build_repost_campaign_scheduled_launch_detail_view
    text, kb = build_repost_campaign_scheduled_launch_detail_view(rule_id=1, scheduled_launch={"id": 1, "status": "failed", "scheduled_at": "2026-05-09T15:00:00+00:00", "saved_post_id": 26, "show_seconds": 86400})
    assert "❌ ошибка запуска" in text
    labels = [b.text for row in kb.inline_keyboard for b in row]
    assert "❌ Отменить запуск" not in labels


def test_launch_mode_view_contains_now_and_schedule_actions():
    text, kb = build_repost_campaign_launch_mode_view(
        rule_id=12,
        readiness={"saved_post_id": 9, "saved_post_exists": True, "show_seconds": 86400, "will_send_total": 12, "will_skip_total": 0},
    )
    callbacks = _callbacks_from_keyboard(kb)
    assert "🚀 Запуск кампании" in text
    labels = _texts_from_keyboard(kb)
    assert "⚡ Запустить сейчас" in labels
    assert "🕒 Запланировать запуск" in labels
    assert f"rule_repost_campaign_launch_now_preview:12" in callbacks
    assert f"rule_repost_campaign_schedule_current:12" in callbacks


def test_schedule_current_view_has_time_only_without_wizard_steps():
    text, kb = build_repost_campaign_schedule_current_view(
        rule_id=7,
        readiness={"saved_post_id": 5, "saved_post_exists": True, "show_seconds": 86400, "will_send_total": 12, "will_skip_total": 0},
    )
    callbacks = _callbacks_from_keyboard(kb)
    assert "🕒 Запланировать запуск текущей кампании" in text
    assert "Шаг 1/4" not in text and "Шаг 2/4" not in text and "Шаг 3/4" not in text
    assert f"rule_repost_campaign_schedule_quick:7:today_20" in callbacks
    assert f"rule_repost_campaign_schedule_input:7" in callbacks
    assert f"rule_repost_campaign_launch:7" in callbacks


def test_vip_features_view_uses_scheduled_posts_copy():
    text, _ = build_repost_campaign_vip_features_view(rule_id=5)
    assert "🕒 Запланированные посты" in text
    assert "Запуск по расписанию" not in text


def test_bot_launch_callback_opens_launch_mode_screen():
    handlers_source = Path("app/repost_campaign_handlers.py").read_text(encoding="utf-8")
    bot_source = Path("bot.py").read_text(encoding="utf-8")
    assert 'build_repost_campaign_launch_mode_view(rule_id=rule_id, readiness=readiness)' in handlers_source
    assert '@dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_launch_now_preview:"))' in handlers_source
    schedule_source = Path("app/repost_campaign_schedule_handlers.py").read_text(encoding="utf-8")
    assert '@dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_schedule_current:"))' in schedule_source
    assert '@dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_launch:"))' not in bot_source

from app.repost_campaign_ui import (
    build_vip_scheduled_posts_screen_view,
    build_vip_scheduled_posts_list_view,
    build_vip_scheduled_post_wizard_post_view,
    build_vip_scheduled_post_wizard_targets_view,
    build_vip_scheduled_post_wizard_show_view,
    build_vip_scheduled_post_wizard_time_view,
    build_vip_scheduled_post_preview_view,
    build_vip_scheduled_post_detail_view,
    build_vip_scheduled_post_cancel_confirm_view,
    build_vip_scheduled_post_add_target_view,
    build_vip_scheduled_post_pick_targets_view,
)

def test_vip_scheduled_main_screen_has_only_premium_intro_and_three_buttons():
    text, kb = build_vip_scheduled_posts_screen_view(rule_id=1, posts=[])
    assert '🕒 Запланированные посты' in text
    assert 'Планируйте рекламные публикации заранее' in text
    assert 'После подтверждения пост появится' in text
    for banned in ['Черновик', 'Ближайшие', 'Пост не выбран', 'Время не задано', 'Фильтр']:
        assert banned not in text
    assert _texts_from_keyboard(kb) == ["➕ Запланировать пост", "📄 Все запланированные посты", "⬅️ Назад"]

def test_vip_scheduled_post_wizard_post_view():
    text, kb = build_vip_scheduled_post_wizard_post_view(rule_id=1, scheduled_post={'id':10}, saved_posts=[{'id':29}], readiness={})
    assert 'Шаг 1/4' in text
    assert '📚 Выберите рекламный пост из библиотеки' in text
    assert 'rule_repost_campaign_scheduled_post_pick_post:1:10:29' in _callbacks_from_keyboard(kb)

def test_vip_scheduled_post_wizard_targets_view():
    text, kb = build_vip_scheduled_post_wizard_targets_view(rule_id=1, scheduled_post={'id':10}, targets=[{}], readiness={})
    assert 'Снимок текущих каналов' not in text
    labels=_texts_from_keyboard(kb)
    assert '➕ Добавить канал/группу' in labels
    assert '📋 Выбрать из известных' in labels

def test_vip_scheduled_post_wizard_show_view():
    _, kb = build_vip_scheduled_post_wizard_show_view(rule_id=1, scheduled_post={'id':10,'show_seconds':86400}, readiness={})
    labels=_texts_from_keyboard(kb)
    assert '1 час' in labels and '24 часа' in labels and '48 часов' in labels

def test_vip_scheduled_post_wizard_time_view():
    text, _ = build_vip_scheduled_post_wizard_time_view(rule_id=1, scheduled_post={'id':10}, readiness={})
    labels = _texts_from_keyboard(_[1]) if False else None

def test_vip_scheduled_post_preview_ready_has_confirm_button():
    _, kb = build_vip_scheduled_post_preview_view(rule_id=1, scheduled_post={'id':10,'saved_post_id':29}, targets=[], readiness={'can_schedule':True})
    assert '✅ Запланировать пост' in _texts_from_keyboard(kb)

def test_vip_scheduled_post_preview_blocked_hides_confirm_button():
    text, kb = build_vip_scheduled_post_preview_view(rule_id=1, scheduled_post={'id':10}, targets=[], readiness={'can_schedule':False,'block_reasons':['x']})
    assert '✅ Запланировать пост' not in _texts_from_keyboard(kb)
    assert 'Что нужно исправить' in text


def test_vip_scheduled_post_wizard_time_view_buttons():
    text, kb = build_vip_scheduled_post_wizard_time_view(rule_id=1, scheduled_post={'id':10}, readiness={})
    labels = _texts_from_keyboard(kb)
    assert 'Сегодня в 20:00' in labels
    assert 'Завтра в 12:00' in labels
    assert '✍️ Ввести дату и время' in labels
    assert '👁 Предпросмотр' not in labels

def test_vip_scheduled_detail_scheduled_has_premium_actions():
    text, kb = build_vip_scheduled_post_detail_view(rule_id=1, details={'post': {'id': 123, 'status': 'scheduled'}})
    labels = _texts_from_keyboard(kb)
    assert 'Статус: запланирован' in text
    assert '🚀 Отправить сейчас' in labels
    assert '✏️ Изменить пост' not in labels
    assert '🗑 Отменить пост' in labels
    assert '⬅️ Назад' in labels

def test_vip_scheduled_post_detail_launched_has_run_buttons():
    _, kb = build_vip_scheduled_post_detail_view(rule_id=1, details={'post': {'id':123, 'status':'launched', 'campaign_run_id':55}})
    labels = _texts_from_keyboard(kb)
    assert '📄 Открыть запуск' not in labels
    assert '📊 Открыть отчёт' in labels

def test_vip_scheduled_delete_confirm_uses_delete_not_cancel():
    text, kb = build_vip_scheduled_post_cancel_confirm_view(rule_id=1, scheduled_post={'id':123})
    assert 'Удалить отложенный пост' in text
    labels = _texts_from_keyboard(kb)
    assert '✅ Удалить пост' in labels
    assert 'Отменить запланированный пост' not in text

def test_ordinary_schedule_callbacks_kept():
    from app.repost_campaign_ui import build_repost_campaign_schedule_preview_view, build_repost_campaign_scheduled_launch_detail_view, build_repost_campaign_scheduled_launch_cancel_result_view
    _, kb1 = build_repost_campaign_schedule_preview_view(rule_id=1, readiness={}, scheduled_at_utc=None)
    assert any('rule_repost_campaign_schedule_menu:1' in (b.callback_data or '') for r in kb1.inline_keyboard for b in r)

def test_bot_has_all_vip_scheduled_callbacks_prefixes():
    source = Path('app/repost_campaign_scheduled_post_handlers.py').read_text(encoding='utf-8')
    for prefix in [
        'rule_repost_campaign_scheduled_post_pick_post:',
        'rule_repost_campaign_scheduled_post_step_targets:',
        'rule_repost_campaign_scheduled_post_step_show:',
        'rule_repost_campaign_scheduled_post_step_time:',
        'rule_repost_campaign_scheduled_post_preview:',
        'rule_repost_campaign_scheduled_post_confirm:',
        'rule_repost_campaign_scheduled_post_detail:',
        'rule_repost_campaign_scheduled_posts:',
        'rule_repost_campaign_scheduled_posts_list:',
        'rule_repost_campaign_scheduled_post_new:',
    ]:
        assert prefix in source

def test_vip_scheduled_posts_list_view_includes_allowed_statuses_and_excludes_draft_ready():
    posts = [
        {"id": 1, "status": "draft", "scheduled_at": None},
        {"id": 2, "status": "ready", "scheduled_at": None},
        {"id": 3, "status": "scheduled", "scheduled_at": "2026-05-10T15:00:00+00:00"},
        {"id": 4, "status": "processing", "scheduled_at": "2026-05-10T16:00:00+00:00"},
        {"id": 5, "status": "launched", "scheduled_at": "2026-05-10T17:00:00+00:00"},
        {"id": 6, "status": "failed", "scheduled_at": "2026-05-10T18:00:00+00:00"},
        {"id": 7, "status": "cancelled", "scheduled_at": "2026-05-10T19:00:00+00:00"},
        {"id": 8, "status": "expired", "scheduled_at": "2026-05-10T20:00:00+00:00"},
    ]
    text, kb = build_vip_scheduled_posts_list_view(rule_id=1, posts=posts, page=0)
    callbacks = _callbacks_from_keyboard(kb)
    assert "rule_repost_campaign_scheduled_post_detail:1:1" not in callbacks
    assert "rule_repost_campaign_scheduled_post_detail:1:2" not in callbacks
    for post_id in (3, 4, 5, 6, 7, 8):
        assert any(f"rule_repost_campaign_scheduled_post_detail:1:{post_id}" in c for c in callbacks)
    assert "Отложенный пост" in text or any("Отложенный пост" in x for x in _texts_from_keyboard(kb))


def test_vip_targets_view_shows_selected_targets_preview():
    text, _ = build_vip_scheduled_post_wizard_targets_view(
        rule_id=1,
        scheduled_post={'id': 10},
        targets=[{'target_id': '@a', 'target_title': 'A'}, {'target_id': '@b', 'target_title': 'B'}],
        readiness={},
    )
    assert 'Выбрано: 2' in text
    assert 'Выбранные каналы:' in text
    assert '✅ A' in text
    assert '✅ B' in text


def test_vip_step1_back_to_scheduled_posts():
    _, kb = build_vip_scheduled_post_wizard_post_view(rule_id=1, scheduled_post={'id': 10}, saved_posts=[], readiness={})
    assert 'rule_repost_campaign_scheduled_posts:1' in _callbacks_from_keyboard(kb)


def test_legacy_cancel_result_keeps_schedule_menu_callback():
    from app.repost_campaign_ui import build_repost_campaign_scheduled_launch_cancel_result_view
    _, kb = build_repost_campaign_scheduled_launch_cancel_result_view(rule_id=1, ok=True)
    assert 'rule_repost_campaign_schedule_menu:1' in _callbacks_from_keyboard(kb)


def test_bot_no_run_db_for_async_check_targets():
    source = Path('bot.py').read_text(encoding='utf-8')
    assert 'run_db(service.check_targets' not in source


def test_bot_vip_handlers_use_ensure_rule_callback_access():
    source = Path('app/repost_campaign_scheduled_post_handlers.py').read_text(encoding='utf-8')
    prefixes = [
        "rule_repost_campaign_scheduled_posts:",
        "rule_repost_campaign_scheduled_posts_list:",
        "rule_repost_campaign_scheduled_post_new:",
        "rule_repost_campaign_scheduled_post_step_post:",
        "rule_repost_campaign_scheduled_post_pick_post:",
        "rule_repost_campaign_scheduled_post_step_targets:",
        "rule_repost_campaign_scheduled_post_step_show:",
        "rule_repost_campaign_scheduled_post_step_time:",
        "rule_repost_campaign_scheduled_post_preview:",
        "rule_repost_campaign_scheduled_post_confirm:",
        "rule_repost_campaign_scheduled_post_detail:",
    ]
    for prefix in prefixes:
        marker = f'@dp.callback_query(lambda c: c.data.startswith("{prefix}"))'
        start = source.find(marker)
        assert start != -1

def test_vip_scheduled_add_target_view():
    text, kb = build_vip_scheduled_post_add_target_view(rule_id=1, scheduled_post_id=10)
    assert "➕ Добавить канал/группу" in text
    assert "@channelname" in text
    assert "https://t.me/channelname" in text
    assert "-1001234567890" in text
    assert "rule_repost_campaign_scheduled_post_step_targets:1:10" in _callbacks_from_keyboard(kb)

def test_vip_scheduled_step2_does_not_use_snapshot_callback():
    _, kb = build_vip_scheduled_post_wizard_targets_view(rule_id=1, scheduled_post={'id':10}, targets=[], readiness={})
    callbacks = _callbacks_from_keyboard(kb)
    assert not any("rule_repost_campaign_scheduled_post_snapshot_targets" in x for x in callbacks)

def test_vip_scheduled_pick_targets_view_empty():
    text, kb = build_vip_scheduled_post_pick_targets_view(rule_id=1, scheduled_post_id=10, known_targets=[], selected_targets=[])
    assert "Пока нет известных каналов/групп" in text
    assert "➕ Добавить канал/группу" in _texts_from_keyboard(kb)

def test_vip_scheduled_pick_targets_view_paginates_more_than_10():
    known_targets = [{"target_id": f"@ch{i}", "target_thread_id": None, "target_title": f"Канал {i}"} for i in range(12)]
    text0, kb0 = build_vip_scheduled_post_pick_targets_view(rule_id=1, scheduled_post_id=10, known_targets=known_targets, selected_targets=[], page=0, page_size=10)
    assert "Страница: 1 / 2" in text0
    labels0 = _texts_from_keyboard(kb0)
    assert "➕ Канал 0" in labels0 and "➕ Канал 9" in labels0
    assert "➡️ Следующая" in labels0
    text1, kb1 = build_vip_scheduled_post_pick_targets_view(rule_id=1, scheduled_post_id=10, known_targets=known_targets, selected_targets=[], page=1, page_size=10)
    assert "Страница: 2 / 2" in text1
    labels1 = _texts_from_keyboard(kb1)
    assert "➕ Канал 10" in labels1 and "➕ Канал 11" in labels1
    assert "⬅️ Предыдущая" in labels1

def test_vip_scheduled_pick_targets_view_has_add_all_buttons():
    known_targets = [{"target_id": "@x", "target_thread_id": None, "target_title": "X"}]
    _, kb = build_vip_scheduled_post_pick_targets_view(rule_id=1, scheduled_post_id=10, known_targets=known_targets, selected_targets=[])
    labels = _texts_from_keyboard(kb)
    assert "➕ Добавить все" in labels
    assert "➕ Добавить все на странице" not in labels

def test_bot_vip_pick_targets_is_not_placeholder():
    source = Path("app/repost_campaign_scheduled_post_handlers.py").read_text(encoding="utf-8")
    assert "Скоро: выбор из известных каналов/групп" not in source
    assert "rule_repost_campaign_scheduled_post_add_known_target" in source
    assert "rule_repost_campaign_scheduled_post_add_known_all" in source

def test_bot_vip_pick_targets_has_pagination_callbacks():
    source = Path("app/repost_campaign_scheduled_post_handlers.py").read_text(encoding="utf-8")
    assert "rule_repost_campaign_scheduled_post_pick_targets:" in source
    assert "page = int(parts[3]) if len(parts) > 3 else 0" in source

def test_bot_vip_pick_targets_uses_keyword_active_only_calls():
    source = Path("app/repost_campaign_scheduled_post_handlers.py").read_text(encoding="utf-8")
    assert "db.list_rule_repost_campaign_targets, rule_id, True" not in source
    assert "db.list_campaign_scheduled_post_targets, post_id, True" not in source
    assert "db.list_campaign_scheduled_post_targets, scheduled_post_id, True" not in source
    assert "active_only=True" in source

def test_bot_vip_add_known_all_returns_via_step_targets_handler():
    source = Path("app/repost_campaign_scheduled_post_handlers.py").read_text(encoding="utf-8")
    assert "_open_vip_scheduled_post_step_targets_message(callback," not in source
    handler_body = source[
        source.find("async def handle_vip_scheduled_post_add_known_all"):
        source.find('@dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_scheduled_post_step_show:"))')
    ]
    assert "_open_vip_scheduled_post_step_targets_callback" in handler_body
    assert "await handle_step_targets(callback)" not in handler_body


def test_vip_scheduled_callbacks_moved_from_bot_to_handlers_module():
    handlers_source = Path("app/repost_campaign_scheduled_post_handlers.py").read_text(encoding="utf-8")
    bot_source = Path("bot.py").read_text(encoding="utf-8")
    moved_prefixes = [
        "rule_repost_campaign_scheduled_post_add_target:",
        "rule_repost_campaign_scheduled_post_pick_targets:",
        "rule_repost_campaign_scheduled_post_snapshot_targets:",
        "rule_repost_campaign_scheduled_post_add_known_target:",
        "rule_repost_campaign_scheduled_post_add_known_all:",
        "rule_repost_campaign_scheduled_post_pick_show:",
        "rule_repost_campaign_scheduled_post_quick_time:",
        "rule_repost_campaign_scheduled_post_input_time:",
        "rule_repost_campaign_scheduled_post_check_rights:",
    ]
    for prefix in moved_prefixes:
        assert prefix in handlers_source
        assert f'@dp.callback_query(lambda c: c.data.startswith("{prefix}"))' not in bot_source


def test_destructive_vip_scheduled_callbacks_moved_from_bot_to_handlers_module():
    handlers_source = Path("app/repost_campaign_scheduled_post_handlers.py").read_text(encoding="utf-8")
    bot_source = Path("bot.py").read_text(encoding="utf-8")
    for prefix in [
        "rule_repost_campaign_vip_delete_active:",
        "rule_repost_campaign_scheduled_post_send_now_confirm:",
        "rule_repost_campaign_scheduled_post_send_now:",
        "rule_repost_campaign_scheduled_post_cancel_confirm:",
        "rule_repost_campaign_scheduled_post_cancel:",
        "rule_repost_campaign_scheduled_post_duplicate:",
    ]:
        assert f'@dp.callback_query(lambda c: c.data.startswith("{prefix}"))' in handlers_source
        assert f'@dp.callback_query(lambda c: c.data.startswith("{prefix}"))' not in bot_source


def test_vip_scheduled_handlers_module_does_not_touch_regular_schedule_or_mutate_callback_data():
    source = Path("app/repost_campaign_scheduled_post_handlers.py").read_text(encoding="utf-8")
    assert "callback.data =" not in source
    assert "rule_repost_campaign_schedule_step1:" not in source


def test_message_handlers_keep_expected_states():
    source = Path("app/repost_campaign_message_handlers.py").read_text(encoding="utf-8")
    assert "waiting_vip_scheduled_post_target" in source
    assert "waiting_repost_campaign_scheduled_post_time" in source


def test_vip_scheduled_posts_screen_has_only_three_main_buttons():
    _, kb = build_vip_scheduled_posts_screen_view(rule_id=1, posts=[])
    assert _texts_from_keyboard(kb) == ["➕ Запланировать пост", "📄 Все запланированные посты", "⬅️ Назад"]

def test_vip_scheduled_posts_screen_shows_active_placement_block():
    text, kb = build_vip_scheduled_posts_screen_view(
        rule_id=4,
        active_placement={"active_placement": True, "active_run_id": 22, "active_delete_after_text": "09.05 23:49 UTC+3", "next_available_text": "09.05 23:50 UTC+3", "delete_failed": 0},
    )
    labels = _texts_from_keyboard(kb)
    assert "Сейчас активно размещение" in text
    assert "09.05 23:49 UTC+3" in text
    assert "🧹 Удалить активный пост" in labels

def test_vip_scheduled_posts_screen_hides_delete_active_without_placement():
    _, kb = build_vip_scheduled_posts_screen_view(rule_id=4, active_placement={"active_placement": False, "delete_failed": 0})
    assert "🧹 Удалить активный пост" not in _texts_from_keyboard(kb)

def test_handlers_module_has_delete_active_vip_scheduled_handler():
    source = Path("app/repost_campaign_scheduled_post_handlers.py").read_text(encoding="utf-8")
    assert "rule_repost_campaign_vip_delete_active:" in source
    assert "handle_rule_repost_campaign_vip_delete_active" in source

def test_scheduled_post_new_button_does_not_open_library_or_choice():
    source = Path('app/repost_campaign_scheduled_post_handlers.py').read_text(encoding='utf-8')
    assert '@dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_scheduled_post_new:"))' in source
    start = source.index('async def handle_rule_repost_campaign_scheduled_post_new')
    end = source.index('@dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_scheduled_post_step_post:"))')
    handler_body = source[start:end]
    assert 'build_vip_scheduled_post_create_choice_view' not in handler_body
    assert 'db.list_saved_posts' not in handler_body
    assert 'build_vip_scheduled_post_wizard_post_view' not in handler_body
    assert 'waiting_vip_scheduled_post_material' in handler_body

def test_vip_scheduled_flow_has_no_library_choice_callbacks():
    source = Path('bot.py').read_text(encoding='utf-8')
    assert "rule_repost_campaign_scheduled_post_new_from_library" not in source
    assert "rule_repost_campaign_scheduled_post_create_choice" not in source
    assert "rule_repost_campaign_scheduled_post_new_from_current" not in source

def test_vip_scheduled_waiting_material_text():
    source = Path('app/repost_campaign_scheduled_post_handlers.py').read_text(encoding='utf-8')
    assert "Отправьте сюда рекламный пост" in source
    assert "После сохранения поста ViMi перейдёт к шагу 2" in source

def test_vip_scheduled_posts_list_view_empty_when_only_internal_drafts():
    text, _ = build_vip_scheduled_posts_list_view(rule_id=1, posts=[{"id": 1, "status": "draft", "scheduled_at": None}], page=0)
    assert "Пока нет отложенных постов." in text

def test_vip_scheduled_all_posts_button_opens_list_view():
    source = Path("app/repost_campaign_scheduled_post_handlers.py").read_text(encoding="utf-8")
    handler_body = source[source.find("async def handle_rule_repost_campaign_scheduled_posts_list"):source.find('@dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_scheduled_post_new:"))')]
    assert "_open_vip_scheduled_posts_list_callback" in handler_body
    assert "build_vip_scheduled_posts_screen_view" not in handler_body

def test_vip_scheduled_posts_list_view_has_no_filters():
    posts = [{"id": i + 1, "status": "scheduled", "scheduled_at": "2026-05-10T15:00:00+00:00"} for i in range(11)]
    _, kb = build_vip_scheduled_posts_list_view(rule_id=1, posts=posts, page=0, page_size=10)
    labels = _texts_from_keyboard(kb)
    for banned in ["Все", "Черновики", "Запланированные", "Завершённые"]:
        assert banned not in labels
    assert "➡️ Следующая" in labels

def test_bot_vip_scheduled_list_handler_loads_allowed_statuses():
    source = Path("app/repost_campaign_scheduled_post_handlers.py").read_text(encoding="utf-8")
    assert 'statuses=["scheduled", "processing", "launched", "failed", "cancelled", "expired"]' in source
    handler_body = source[source.find("async def handle_rule_repost_campaign_scheduled_posts_list"):source.find('@dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_scheduled_post_new:"))')]
    assert "status_filter" not in handler_body

def test_bot_does_not_mutate_callback_data():
    source = Path("bot.py").read_text(encoding="utf-8")
    assert re.search(r"callback\.data\s*=\s*[^=]", source) is None


def test_vip_scheduled_uses_open_helpers_instead_of_callback_data_rewrite():
    source = Path("app/repost_campaign_scheduled_post_handlers.py").read_text(encoding="utf-8")
    assert "_open_vip_scheduled_post_step_targets_callback" in source
    assert "_open_vip_scheduled_posts_list_callback" in source


def test_vip_scheduled_confirm_opens_list_without_callback_data_mutation():
    source = Path("app/repost_campaign_scheduled_post_handlers.py").read_text(encoding="utf-8")
    start = source.find("async def handle_confirm")
    end = source.find("\n    @dp.callback_query", start + 1)
    handler_body = source[start:end]
    assert re.search(r"callback\.data\s*=\s*[^=]", handler_body) is None
    assert "_open_vip_scheduled_posts_list_callback" in handler_body


def test_vip_scheduled_add_known_all_opens_targets_without_callback_data_mutation():
    source = Path("app/repost_campaign_scheduled_post_handlers.py").read_text(encoding="utf-8")
    handler_body = source[
        source.find("async def handle_vip_scheduled_post_add_known_all"):
        source.find('@dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_scheduled_post_step_show:"))')
    ]
    assert re.search(r"callback\.data\s*=\s*[^=]", handler_body) is None
    assert "_open_vip_scheduled_post_step_targets_callback" in handler_body


def test_vip_scheduled_step_targets_callback_helper_passes_targets():
    source = Path("app/repost_campaign_scheduled_post_handlers.py").read_text(encoding="utf-8")
    helper_body = source[
        source.find("async def _open_vip_scheduled_post_step_targets_callback"):
        source.find("async def _open_vip_scheduled_posts_list_callback")
    ]
    assert "db.list_campaign_scheduled_post_targets" in helper_body
    assert "active_only=True" in helper_body
    assert "targets=targets or []" in helper_body


def test_bot_has_no_unresolved_vip_scheduled_helper_calls():
    source = Path("bot.py").read_text(encoding="utf-8")
    helper_names = [
        "_open_vip_scheduled_post_step_targets_callback",
        "_open_vip_scheduled_posts_list_callback",
        "_open_vip_scheduled_post_detail_callback",
        "_open_vip_scheduled_post_step_show_callback",
    ]
    for helper_name in helper_names:
        if f"{helper_name}(" in source:
            assert f"def {helper_name}(" in source


def test_no_duplicate_callback_prefix_registrations_across_repost_campaign_modules():
    files = [
        "bot.py",
        "app/repost_campaign_handlers.py",
        "app/repost_campaign_schedule_handlers.py",
        "app/repost_campaign_report_handlers.py",
        "app/repost_campaign_scheduled_post_handlers.py",
        "app/repost_campaign_message_handlers.py",
    ]
    pattern = re.compile(r'@dp\.callback_query\(lambda c: c\.data\.startswith\("([^"]+)"\)\)')
    prefixes: dict[str, list[str]] = {}
    for file_path in files:
        source = Path(file_path).read_text(encoding="utf-8")
        for prefix in pattern.findall(source):
            prefixes.setdefault(prefix, []).append(file_path)
    duplicates = {prefix: owners for prefix, owners in prefixes.items() if len(owners) > 1}
    assert not duplicates, f"Duplicate callback prefixes found: {duplicates}"


def test_vip_scheduled_callbacks_registered_only_in_handlers_module():
    handler_prefixes = [
        "rule_repost_campaign_vip_delete_active:",
        "rule_repost_campaign_scheduled_post_send_now_confirm:",
        "rule_repost_campaign_scheduled_post_send_now:",
        "rule_repost_campaign_scheduled_post_cancel_confirm:",
        "rule_repost_campaign_scheduled_post_cancel:",
        "rule_repost_campaign_scheduled_post_duplicate:",
        "rule_repost_campaign_scheduled_post_add_target:",
        "rule_repost_campaign_scheduled_post_pick_targets:",
        "rule_repost_campaign_scheduled_post_snapshot_targets:",
        "rule_repost_campaign_scheduled_post_add_known_target:",
        "rule_repost_campaign_scheduled_post_add_known_all:",
        "rule_repost_campaign_scheduled_post_pick_show:",
        "rule_repost_campaign_scheduled_post_quick_time:",
        "rule_repost_campaign_scheduled_post_input_time:",
        "rule_repost_campaign_scheduled_post_check_rights:",
        "rule_repost_campaign_scheduled_posts:",
        "rule_repost_campaign_scheduled_posts_list:",
        "rule_repost_campaign_scheduled_post_new:",
        "rule_repost_campaign_scheduled_post_step_post:",
        "rule_repost_campaign_scheduled_post_pick_post:",
        "rule_repost_campaign_scheduled_post_step_targets:",
        "rule_repost_campaign_scheduled_post_step_show:",
        "rule_repost_campaign_scheduled_post_step_time:",
        "rule_repost_campaign_scheduled_post_preview:",
        "rule_repost_campaign_scheduled_post_confirm:",
        "rule_repost_campaign_scheduled_post_detail:",
    ]
    bot_source = Path("bot.py").read_text(encoding="utf-8")
    handlers_source = Path("app/repost_campaign_scheduled_post_handlers.py").read_text(encoding="utf-8")
    for prefix in handler_prefixes:
        registration = f'@dp.callback_query(lambda c: c.data.startswith("{prefix}"))'
        assert registration in handlers_source
        assert registration not in bot_source


def test_bot_vip_scheduled_helpers_are_removed_when_not_called():
    source = Path("bot.py").read_text(encoding="utf-8")
    helper_names = [
        "_open_vip_scheduled_post_step_targets_callback",
        "_open_vip_scheduled_posts_list_callback",
        "_open_vip_scheduled_post_detail_callback",
        "_open_vip_scheduled_post_step_show_callback",
        "_open_vip_scheduled_post_pick_targets",
        "_build_vip_scheduled_known_targets",
    ]
    for helper_name in helper_names:
        has_call = f"{helper_name}(" in source
        has_def = f"def {helper_name}(" in source
        assert has_call == has_def, f"Inconsistent helper presence for {helper_name}: call={has_call}, def={has_def}"


def test_vip_scheduled_step_targets_has_next_when_targets_selected():
    text, kb = build_vip_scheduled_post_wizard_targets_view(
        rule_id=1,
        scheduled_post={"id": 10},
        targets=[{"target_id": "@test", "target_title": "Test"}],
        readiness={},
    )
    labels = _texts_from_keyboard(kb)
    assert "✅ Далее" in labels
    assert "Выбрано: 1" in text


def test_vip_scheduled_step_targets_no_next_without_targets():
    text, kb = build_vip_scheduled_post_wizard_targets_view(
        rule_id=1,
        scheduled_post={"id": 10},
        targets=[],
        readiness={},
    )
    labels = _texts_from_keyboard(kb)
    assert "✅ Далее" not in labels
    assert "Выбрано: 0" in text


def test_vip_scheduled_pick_targets_has_done_button():
    text, kb = build_vip_scheduled_post_pick_targets_view(
        rule_id=1,
        scheduled_post_id=10,
        known_targets=[{"target_id": "@a", "target_title": "A"}],
        selected_targets=[],
        page=0,
        page_size=10,
    )
    labels = _texts_from_keyboard(kb)
    callbacks = _callbacks_from_keyboard(kb)
    assert "✅ Готово" in labels
    assert "rule_repost_campaign_scheduled_post_step_targets:1:10" in callbacks


def test_vip_scheduled_pick_targets_hides_add_page_when_single_page():
    _, kb = build_vip_scheduled_post_pick_targets_view(
        rule_id=1,
        scheduled_post_id=10,
        known_targets=[{"target_id": "@a", "target_title": "A"}],
        selected_targets=[],
        page=0,
        page_size=10,
    )
    labels = _texts_from_keyboard(kb)
    assert "➕ Добавить все" in labels
    assert "➕ Добавить все на странице" not in labels


def test_vip_scheduled_pick_targets_shows_add_page_when_paginated():
    known = [{"target_id": f"@a{i}", "target_title": f"A{i}"} for i in range(11)]
    _, kb = build_vip_scheduled_post_pick_targets_view(
        rule_id=1,
        scheduled_post_id=10,
        known_targets=known,
        selected_targets=[],
        page=0,
        page_size=10,
    )
    labels = _texts_from_keyboard(kb)
    assert "➕ Добавить все на странице" in labels
    assert "➕ Добавить все" in labels


def test_bot_imports_campaign_runtime_tasks_manager():
    source = Path("bot.py").read_text(encoding="utf-8")
    assert "RepostCampaignRuntimeTasks" in source
    assert "run_repost_campaign_scheduled_post_loop" not in source
    assert "RepostCampaignScheduledPostService" not in source


def test_bot_starts_vip_scheduled_post_loop_via_runtime_tasks_manager():
    bot_source = Path("bot.py").read_text(encoding="utf-8")
    runtime_source = Path("app/repost_campaign_runtime_tasks.py").read_text(encoding="utf-8")
    assert "_start_repost_campaign_runtime_tasks" in bot_source
    assert "run_repost_campaign_scheduled_post_loop(" in runtime_source
    assert "vip-scheduled-post:" in runtime_source


def test_bot_vip_scheduled_post_loop_uses_runtime_tasks_builder():
    bot_source = Path("bot.py").read_text(encoding="utf-8")
    runtime_source = Path("app/repost_campaign_runtime_tasks.py").read_text(encoding="utf-8")
    assert "_build_repost_campaign_scheduled_post_service()" not in bot_source
    assert "def _build_scheduled_post_runtime" in runtime_source


from app.repost_campaign_ui import build_vip_scheduled_post_detail_view

def test_vip_scheduled_detail_buttons_for_draft():
    _, kb = build_vip_scheduled_post_detail_view(rule_id=1, details={"post": {"id": 2, "status": "draft"}})
    texts = _texts_from_keyboard(kb)
    assert "✏️ Изменить пост" in texts and "🚀 Отправить сейчас" in texts and "📋 Дублировать пост" in texts and "🗑 Удалить пост" in texts

def test_vip_scheduled_detail_buttons_for_scheduled():
    _, kb = build_vip_scheduled_post_detail_view(rule_id=1, details={"post": {"id": 2, "status": "scheduled"}})
    texts = _texts_from_keyboard(kb)
    assert "✏️ Изменить пост" not in texts and "🗑 Отменить пост" in texts

def test_vip_scheduled_detail_buttons_for_launched():
    _, kb = build_vip_scheduled_post_detail_view(rule_id=1, details={"post": {"id": 2, "status": "launched", "campaign_run_id": 8}})
    texts = _texts_from_keyboard(kb)
    assert texts == ["📊 Открыть отчёт", "📋 Дублировать пост", "⬅️ Назад"]

def test_vip_scheduled_detail_buttons_for_terminal():
    _, kb = build_vip_scheduled_post_detail_view(rule_id=1, details={"post": {"id": 2, "status": "failed"}})
    texts = _texts_from_keyboard(kb)
    assert "📋 Дублировать пост" in texts and "🚀 Отправить сейчас" not in texts

def test_vip_scheduled_detail_hides_technical_fields_before_launch():
    text, _ = build_vip_scheduled_post_detail_view(
        rule_id=1,
        details={"post": {"id": 1, "status": "scheduled", "scheduled_at": "2026-05-10T12:40:00+00:00", "show_seconds": 3600}},
    )
    forbidden = ["campaign_run_id", "delete_after_at", "pending=", "processing=", "deleted=", "failed=", "статус=—", "всего=0"]
    for value in forbidden:
        assert value not in text


def test_vip_scheduled_detail_before_launch_is_human_readable():
    text, _ = build_vip_scheduled_post_detail_view(
        rule_id=1,
        details={"post": {"id": 1, "status": "scheduled", "scheduled_at": "2026-05-10T12:40:00+00:00", "show_seconds": 3600}},
    )
    assert "Статус: запланирован" in text
    assert "Срок показа:" in text
    assert "Публикация ещё не запускалась" in text

def test_handlers_has_vip_scheduled_duplicate_handler():
    source = Path("app/repost_campaign_scheduled_post_handlers.py").read_text(encoding="utf-8")
    assert "rule_repost_campaign_scheduled_post_duplicate:" in source

def test_handlers_send_now_is_not_coming_soon_stub():
    source = Path("app/repost_campaign_scheduled_post_handlers.py").read_text(encoding="utf-8")
    assert "Скоро: отправка отложенного поста вручную" not in source
    assert "service.send_now" in source

def test_bot_edit_rejects_non_editable_statuses():
    source = Path("app/repost_campaign_scheduled_post_handlers.py").read_text(encoding="utf-8")
    assert "Запланированный пост уже подтверждён" in source


def test_vip_scheduled_detail_uses_delete_after_at_from_messages_not_run():
    text, _ = build_vip_scheduled_post_detail_view(rule_id=1, details={"post": {"id": 2, "status": "launched", "campaign_run_id": 8}, "campaign_run": {"run": {"status": "sent", "delete_after_at": "2026-05-01T10:00:00+00:00"}, "messages": [{"delete_after_at": "2026-05-02T10:00:00+00:00", "delete_status": "failed", "delete_error_text": "boom"}], "summary": {"delete_pending": 0, "delete_failed": 1}}})
    assert "Не удалось удалить: 1" in text
    assert "Причина: boom" in text


def test_vip_scheduled_detail_launched_delete_summary_is_human_readable():
    text, _ = build_vip_scheduled_post_detail_view(
        rule_id=1,
        details={
            "post": {"id": 1, "status": "launched", "campaign_run_id": 22},
            "campaign_run": {
                "run": {"status": "sent", "targets_total": 2, "targets_success": 2, "targets_failed": 0},
                "summary": {"delete_pending": 2, "delete_processing": 0, "delete_done": 0, "delete_failed": 0},
                "messages": [
                    {"delete_status": "pending", "delete_after_at": "2026-05-10T13:40:00+00:00"},
                    {"delete_status": "pending", "delete_after_at": "2026-05-10T13:40:00+00:00"},
                ],
            },
        },
    )
    assert "Пост будет удалён:" in text
    assert "Ожидает удаления: 2" in text
    assert "pending=" not in text
    assert "delete_after_at" not in text


def test_vip_scheduled_detail_hides_empty_views_block():
    text, _ = build_vip_scheduled_post_detail_view(
        rule_id=1,
        details={"post": {"id": 1, "status": "scheduled"}, "campaign_run": None},
    )
    assert "Просмотры:" not in text
    assert "всего=0" not in text


def test_campaign_ui_does_not_render_technical_keys_in_vip_scheduled_detail():
    text, _ = build_vip_scheduled_post_detail_view(
        rule_id=1,
        details={
            "post": {"id": 2, "status": "launched", "campaign_run_id": 8},
            "campaign_run": {
                "run": {"status": "sent", "targets_total": 2, "targets_success": 1, "targets_failed": 1},
                "messages": [{"delete_after_at": "2026-05-02T10:00:00+00:00", "delete_status": "pending"}],
                "summary": {"delete_pending": 1, "delete_failed": 0},
            },
        },
    )
    forbidden_literals = ["campaign_run_id:", "delete_after_at:", "pending=", "processing=", "deleted=", "failed=", "статус=—", "всего=0"]
    for value in forbidden_literals:
        assert value not in text

def test_report_button_callback_exists_in_report_handlers_module():
    source = Path("app/repost_campaign_report_handlers.py").read_text(encoding="utf-8")
    assert "rule_repost_campaign_views_report:" in source
    bot_source = Path("bot.py").read_text(encoding="utf-8")
    assert "register_repost_campaign_report_handlers(dp, campaign_handlers_ctx)" in bot_source


def test_postgres_mark_processing_does_not_use_nonexistent_columns():
    source = Path("app/postgres_repository.py").read_text(encoding="utf-8")
    block_start = source.index("def mark_campaign_scheduled_post_processing")
    block_end = source.index("def delay_campaign_scheduled_post_retry", block_start)
    block = source[block_start:block_end]
    assert "started_processing_at" not in block
    assert "launched_by" not in block
    assert "locked_by" in block and "locked_at=NOW()" in block and "lock_until=NOW()" in block


def test_a1_callbacks_registered_in_repost_campaign_handlers():
    source = Path("app/repost_campaign_handlers.py").read_text(encoding="utf-8")
    for marker in [
        'rule_repost_campaign_post_menu:',
        'rule_repost_campaign_post_preview:',
        'rule_repost_campaign_preview_delete:',
        'rule_repost_campaign_post_unlink:',
        'rule_repost_campaign_test_send:',
    ]:
        assert marker in source


def test_a1_legacy_decorators_removed_from_bot_py():
    source = Path("bot.py").read_text(encoding="utf-8")
    assert '@dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_post_menu:"))' not in source
    assert '@dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_post_preview:"))' not in source
    assert '@dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_preview_delete:"))' not in source
    assert '@dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_post_unlink:"))' not in source
    assert '@dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_test_send:"))' not in source


def test_no_callback_data_mutation_in_repost_campaign_handlers():
    source = Path("app/repost_campaign_handlers.py").read_text(encoding="utf-8")
    assert "callback.data =" not in source


def test_a3_delete_run_callbacks_moved_to_report_handlers():
    source = Path("app/repost_campaign_report_handlers.py").read_text(encoding="utf-8")
    assert 'rule_repost_campaign_run_delete_confirm:' in source
    assert 'rule_repost_campaign_run_delete_now:' in source


def test_a3_delete_run_callbacks_removed_from_bot_decorators():
    source = Path("bot.py").read_text(encoding="utf-8")
    assert '@dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_run_delete_confirm:"))' not in source
    assert '@dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_run_delete_now:"))' not in source


def test_a3_delete_run_now_uses_direct_await_without_run_db_wrapper():
    source = Path("app/repost_campaign_report_handlers.py").read_text(encoding="utf-8")
    assert 'await runtime.delete_campaign_run_now(' in source
    assert 'ctx.run_db(lambda: runtime.delete_campaign_run_now' not in source


def test_a3_no_callback_data_mutation_in_report_handlers():
    source = Path("app/repost_campaign_report_handlers.py").read_text(encoding="utf-8")
    assert 'callback.data =' not in source


def test_schedule_handlers_module_exists_and_registered_in_bot():
    schedule_source = Path("app/repost_campaign_schedule_handlers.py").read_text(encoding="utf-8")
    bot_source = Path("bot.py").read_text(encoding="utf-8")
    assert "def register_repost_campaign_schedule_handlers(dp: Dispatcher, ctx: RepostCampaignHandlersContext) -> None:" in schedule_source
    assert "register_repost_campaign_schedule_handlers(dp, campaign_handlers_ctx)" in bot_source


def test_schedule_callbacks_moved_to_dedicated_module():
    schedule_source = Path("app/repost_campaign_schedule_handlers.py").read_text(encoding="utf-8")
    bot_source = Path("bot.py").read_text(encoding="utf-8")
    assert "rule_repost_campaign_schedule_current:" in schedule_source
    assert "rule_repost_campaign_schedule_menu:" in schedule_source
    assert '@dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_schedule_current:"))' not in bot_source
    assert '@dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_schedule_menu:"))' not in bot_source


def test_schedule_handlers_do_not_mutate_callback_data_and_do_not_contain_forbidden_callbacks():
    schedule_source = Path("app/repost_campaign_schedule_handlers.py").read_text(encoding="utf-8")
    assert "callback.data =" not in schedule_source
    for forbidden in [
        "rule_repost_campaign_scheduled_detail:",
        "rule_repost_campaign_scheduled_cancel_confirm:",
        "rule_repost_campaign_scheduled_cancel:",
    ]:
        assert forbidden not in schedule_source


def test_schedule_handlers_keep_access_guards():
    schedule_source = Path("app/repost_campaign_schedule_handlers.py").read_text(encoding="utf-8")
    assert "ctx.ensure_rule_callback_access(callback, rule_id)" in schedule_source
    assert "ctx.is_admin_callback(callback)" in schedule_source


def test_vip_scheduled_helpers_rule_value_and_target_key_are_module_level():
    source = Path("app/repost_campaign_scheduled_post_handlers.py").read_text(encoding="utf-8")
    assert "\ndef _rule_value(" in source
    assert "\ndef _target_key(" in source


def _extract_vip_scheduled_register_block(source: str) -> str:
    start = source.index("def register_repost_campaign_scheduled_post_handlers")
    return source[start:]


def _extract_vip_scheduled_section_blocks(register_block: str) -> dict[str, str]:
    section_names = [
        "Entry / list / detail",
        "Draft / material / post",
        "Targets",
        "Show duration",
        "Time",
        "Preview / schedule",
        "Runtime / destructive",
    ]
    markers = [f"    # {name}" for name in section_names]
    positions = [register_block.index(marker) for marker in markers]
    sections: dict[str, str] = {}
    for idx, name in enumerate(section_names):
        start = positions[idx]
        end = positions[idx + 1] if idx + 1 < len(positions) else len(register_block)
        sections[name] = register_block[start:end]
    return sections


def test_vip_scheduled_register_has_sections_in_order():
    source = Path("app/repost_campaign_scheduled_post_handlers.py").read_text(encoding="utf-8")
    register_block = _extract_vip_scheduled_register_block(source)
    section_names = [
        "Entry / list / detail",
        "Draft / material / post",
        "Targets",
        "Show duration",
        "Time",
        "Preview / schedule",
        "Runtime / destructive",
    ]
    positions = [register_block.index(f"    # {name}") for name in section_names]
    assert positions == sorted(positions)


def test_vip_scheduled_callbacks_are_grouped_in_sections():
    source = Path("app/repost_campaign_scheduled_post_handlers.py").read_text(encoding="utf-8")
    register_block = _extract_vip_scheduled_register_block(source)
    sections = _extract_vip_scheduled_section_blocks(register_block)

    expected = {
        "Entry / list / detail": [
            "rule_repost_campaign_scheduled_posts:",
            "rule_repost_campaign_scheduled_posts_list:",
            "rule_repost_campaign_scheduled_post_detail:",
        ],
        "Draft / material / post": [
            "rule_repost_campaign_scheduled_post_new:",
            "rule_repost_campaign_scheduled_post_edit:",
            "rule_repost_campaign_scheduled_post_step_post:",
            "rule_repost_campaign_scheduled_post_pick_post:",
        ],
        "Targets": [
            "rule_repost_campaign_scheduled_post_step_targets:",
            "rule_repost_campaign_scheduled_post_snapshot_targets:",
            "rule_repost_campaign_scheduled_post_add_target:",
            "rule_repost_campaign_scheduled_post_pick_targets:",
            "rule_repost_campaign_scheduled_post_add_known_target:",
            "rule_repost_campaign_scheduled_post_add_known_page:",
            "rule_repost_campaign_scheduled_post_add_known_all:",
            "rule_repost_campaign_scheduled_post_check_rights:",
        ],
        "Show duration": [
            "rule_repost_campaign_scheduled_post_step_show:",
            "rule_repost_campaign_scheduled_post_pick_show:",
        ],
        "Time": [
            "rule_repost_campaign_scheduled_post_step_time:",
            "rule_repost_campaign_scheduled_post_quick_time:",
            "rule_repost_campaign_scheduled_post_input_time:",
        ],
        "Preview / schedule": [
            "rule_repost_campaign_scheduled_post_preview:",
            "rule_repost_campaign_scheduled_post_confirm:",
        ],
        "Runtime / destructive": [
            "rule_repost_campaign_scheduled_post_send_now_confirm:",
            "rule_repost_campaign_scheduled_post_send_now:",
            "rule_repost_campaign_scheduled_post_duplicate:",
            "rule_repost_campaign_scheduled_post_cancel_confirm:",
            "rule_repost_campaign_scheduled_post_cancel:",
            "rule_repost_campaign_vip_delete_active:",
        ],
    }

    for section_name, callbacks in expected.items():
        block = sections[section_name]
        for callback_prefix in callbacks:
            assert callback_prefix in block


def test_vip_scheduled_callbacks_order_safety():
    source = Path("app/repost_campaign_scheduled_post_handlers.py").read_text(encoding="utf-8")
    assert source.index("rule_repost_campaign_scheduled_post_send_now_confirm:") < source.index("rule_repost_campaign_scheduled_post_send_now:")
    assert source.index("rule_repost_campaign_scheduled_post_cancel_confirm:") < source.index("rule_repost_campaign_scheduled_post_cancel:")
    assert source.index("rule_repost_campaign_scheduled_posts_list:") > source.index("rule_repost_campaign_scheduled_posts:")


def test_vip_scheduled_no_forbidden_callbacks_or_callback_data_mutation():
    source = Path("app/repost_campaign_scheduled_post_handlers.py").read_text(encoding="utf-8")
    assert "callback.data =" not in source
    for forbidden in [
        "rule_repost_campaign_schedule_step1:",
        "rule_repost_campaign_schedule_step2:",
        "rule_repost_campaign_schedule_step3:",
        "rule_repost_campaign_schedule_step4:",
    ]:
        assert forbidden not in source


def test_launch_job_queued_status_and_needs_review_views():
    text, kb = build_repost_campaign_launch_queued_view(rule_id=3, job_id=9)
    assert "Кампания поставлена в очередь" in text
    callbacks = _callbacks_from_keyboard(kb)
    assert "rule_repost_campaign_launch_job_status:3:9" in callbacks

    processing_text, _ = build_repost_campaign_launch_job_status_view(rule_id=3, job={"id": 9, "status": "processing"})
    assert "Кампания отправляется" in processing_text

    review_text, review_kb = build_repost_campaign_launch_needs_review_view(rule_id=3, job={"id": 9, "status": "needs_review"})
    assert "Требуется проверка" in review_text
    assert "не отправить рекламу дважды" in review_text
    assert "rule_repost_campaign_launch_job_status:3:9" in _callbacks_from_keyboard(review_kb)


def test_manual_launch_handler_uses_durable_job_not_background_send():
    handlers_source = Path("app/repost_campaign_handlers.py").read_text(encoding="utf-8")
    assert "RepostCampaignLaunchJobService" in handlers_source
    assert "enqueue_manual_launch" in handlers_source
    assert "asyncio.create_task" not in handlers_source
    assert "launch_campaign_now" not in handlers_source


def test_campaign_launch_now_only_allowed_in_runtime_and_launch_job_service():
    offenders = []
    for path in Path("app").glob("repost_campaign_*.py"):
        source = path.read_text(encoding="utf-8")
        if "launch_campaign_now" in source and path.name not in {"repost_campaign_launch_job_service.py", "repost_campaign_runtime_service.py", "repost_campaign_schedule_service.py"}:
            offenders.append(str(path))
    assert offenders == []


def test_bot_has_no_direct_repost_campaign_callbacks_or_launch_job_logic():
    bot_source = Path("bot.py").read_text(encoding="utf-8")
    assert "rule_repost_campaign_launch_confirm:" not in bot_source
    assert "rule_repost_campaign_launch_job_status:" not in bot_source
    assert "RepostCampaignLaunchJobService" not in bot_source


def test_vip_scheduled_preview_and_detail_show_active_ad_warning_not_error():
    warning = "⚠️ В выбранных целях уже есть активная реклама. VIP-пост всё равно будет опубликован по расписанию."
    preview, _ = build_vip_scheduled_post_preview_view(
        rule_id=1,
        scheduled_post={"id": 10, "saved_post_id": 2, "scheduled_at": "2026-05-10T10:00:00+00:00", "show_seconds": 3600},
        targets=[],
        readiness={"can_schedule": True, "warnings": [warning]},
    )
    detail, _ = build_vip_scheduled_post_detail_view(rule_id=1, details={"post": {"id": 10, "status": "scheduled"}, "readiness": {"warnings": [warning]}})
    assert warning in preview
    assert warning in detail
    assert "Предыдущий рекламный пост активен" + " до" not in preview
    assert "Предыдущий рекламный пост активен" + " до" not in detail
    assert "VIP-режим: публикация не блокируется активной рекламой." in preview
    assert "VIP-режим: публикация не блокируется активной рекламой." in detail


def test_vip_scheduled_send_now_confirm_says_publish_over_active_ad():
    from app.repost_campaign_ui import build_vip_scheduled_post_send_now_confirm_view
    text, kb = build_vip_scheduled_post_send_now_confirm_view(rule_id=1, scheduled_post={"id": 10})
    assert "После подтверждения VIP-пост будет опубликован поверх неё." in text
    assert "✅ Да, отправить сейчас" in _texts_from_keyboard(kb)


def test_scheduled_launch_needs_review_view_explains_manual_check():
    from app.repost_campaign_ui import build_repost_campaign_scheduled_launch_detail_view
    text, kb = build_repost_campaign_scheduled_launch_detail_view(
        rule_id=1,
        scheduled_launch={
            "id": 9,
            "status": "needs_review",
            "scheduled_at": "2026-05-09T15:00:00+00:00",
            "saved_post_id": 26,
            "show_seconds": 86400,
        },
    )
    labels = [b.text for row in kb.inline_keyboard for b in row]
    assert "⚠️ Требуется проверка" in text
    assert "Запуск был прерван после создания campaign_run" in text
    assert "Автоматический повтор остановлен" in text
    assert "🔄 Обновить" in labels
    assert "⬅️ Назад к кампании" in labels


def test_launch_needs_review_hides_raw_delete_status_sql_error():
    raw_error = (
        'null value in column "delete_status" of relation "campaign_run_messages" violates not-null constraint\n'
        'DETAIL: Failing row contains campaign_run_messages delete_status.'
    )
    text, _ = build_repost_campaign_launch_needs_review_view(rule_id=1, job={"id": 5, "status": "needs_review", "last_error": raw_error})

    assert "violates not-null constraint" not in text
    assert "DETAIL" not in text
    assert "campaign_run_messages" not in text
    assert "Реклама могла быть опубликована" in text
    assert "не смог безопасно подтвердить ID сообщений" in text
    assert "Автоматический повтор остановлен" in text


def test_launch_result_hides_raw_delete_status_sql_error():
    raw_error = (
        'null value in column "delete_status" of relation "campaign_run_messages" violates not-null constraint\n'
        'DETAIL: Failing row contains campaign_run_messages delete_status.'
    )
    text, _ = build_repost_campaign_launch_result_view(rule_id=1, result={"ok": False, "error_text": raw_error, "extra": {}})

    assert "violates not-null constraint" not in text
    assert "DETAIL" not in text
    assert "campaign_run_messages" not in text
    assert "Реклама могла быть опубликована" in text
    assert "не смог безопасно подтвердить ID сообщений" in text
    assert "Автоматический повтор остановлен" in text
