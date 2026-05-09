from pathlib import Path
import ast

from app.repost_campaign_ui import (
    build_repost_campaign_delete_result_view,
    build_repost_campaign_launch_result_view,
    build_repost_campaign_launch_mode_view,
    build_repost_campaign_launch_readiness_view,
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
)


def _texts_from_keyboard(keyboard):
    return [button.text for row in keyboard.inline_keyboard for button in row]


def _callbacks_from_keyboard(keyboard):
    return [button.callback_data for row in keyboard.inline_keyboard for button in row]


def test_bot_no_legacy_target_check_stub():
    source = Path("bot.py").read_text(encoding="utf-8")
    assert "Полная проверка прав публикации и удаления будет добавлена отдельным шагом" not in source
    assert "rule_repost_campaign_check:" in source

    assert "result = runtime.check_campaign_targets(" not in source
    assert "auto_check_result = runtime.check_campaign_targets(" not in source
    assert "result = runtime.check_campaign_target(" not in source


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
    source = Path("bot.py").read_text(encoding="utf-8")
    assert "rule_repost_campaign_launch_confirm:" in source
    assert "REPOST_CAMPAIGN_LAUNCH_PREFLIGHT_UI" in source
    assert "REPOST_CAMPAIGN_LAUNCH_CONFIRM_STARTED" in source


def test_bot_contains_campaign_check_loading_and_optional_page_parse():
    source = Path("bot.py").read_text(encoding="utf-8")
    assert "build_repost_campaign_targets_check_loading_view" in source
    assert "rule_repost_campaign_target_check:" in source
    assert "rule_repost_campaign_check:" in source
    assert "page = int(parts[3]) if len(parts) > 3 else 0" in source


def test_vip_scheduled_material_state_uses_album_buffer():
    source = Path("bot.py").read_text(encoding="utf-8")
    assert "waiting_vip_scheduled_post_material" in source
    assert "saved_post_album_buffer.add_message" in source
    assert "build_saved_post_album_content_from_aiogram_messages" in source
    assert "Отправьте альбом ещё раз одним сообщением" not in source


def test_vip_scheduled_single_material_uses_shared_save_helper():
    source = Path("bot.py").read_text(encoding="utf-8")
    assert "async def _save_vip_scheduled_post_material(" in source
    assert "saved_post_id = await _save_vip_scheduled_post_material(" in source
    assert "db.create_saved_post" in source
    assert "service.update_draft_saved_post" in source


def test_vip_scheduled_material_has_dedicated_handler_before_generic_album_handler():
    source = Path("bot.py").read_text(encoding="utf-8")
    vip_handler_pos = source.index("async def handle_vip_scheduled_post_material_message(message: Message):")
    generic_album_pos = source.index("on_album_ready=_finalize_repost_campaign_saved_post_album")
    assert vip_handler_pos < generic_album_pos


def test_generic_album_handlers_skip_vip_scheduled_material_state():
    source = Path("bot.py").read_text(encoding="utf-8")
    assert "def _is_waiting_vip_scheduled_post_material(user_id: int | None) -> bool:" in source
    generic_state_block_pos = source.index("if state.get(\"state\") == \"awaiting_repost_campaign_saved_post\":")
    guard_pos = source.index("if _is_waiting_vip_scheduled_post_material(user_id):", generic_state_block_pos)
    assert guard_pos > generic_state_block_pos


def test_stateful_handler_delegates_vip_scheduled_material_to_helper():
    source = Path("bot.py").read_text(encoding="utf-8")
    assert "if await _handle_vip_scheduled_post_material_message(message):" in source
    assert "    return" in source[source.index("if await _handle_vip_scheduled_post_material_message(message):"):source.index("if state.get(\"state\") == \"awaiting_repost_campaign_saved_post\":")]

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
    source = Path("bot.py").read_text(encoding="utf-8")
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


def test_campaign_delete_loop_runtime_gets_telethon_client_in_bot_role():
    source = Path("bot.py").read_text(encoding="utf-8")

    marker = "async def _start_bot_role()"
    start = source.index(marker)
    end = source.index("async def _start_scheduler_role()", start)
    block = source[start:end]

    assert "run_repost_campaign_delete_loop(runtime=delete_runtime" in block
    assert "delete_runtime = RepostCampaignRuntimeService(" in block
    assert "telethon_client=telethon_client" in block


def test_campaign_delete_loop_runtime_gets_telethon_client_in_all_role():
    source = Path("bot.py").read_text(encoding="utf-8")

    marker = "async def _start_all_role()"
    start = source.index(marker)
    end = source.index("async def main()", start) if "async def main()" in source[start:] else len(source)
    block = source[start:end]

    assert "run_repost_campaign_delete_loop(runtime=delete_runtime" in block
    assert "delete_runtime = RepostCampaignRuntimeService(" in block
    assert "telethon_client=telethon_client" in block


def test_campaign_manual_delete_message_runtime_gets_telethon_client():
    source = Path("bot.py").read_text(encoding="utf-8")

    marker = "async def handle_rule_repost_campaign_delete_message"
    start = source.index(marker)
    end = source.index("async def", start + len(marker))
    block = source[start:end]

    assert "delete_campaign_run_message_now" in block
    assert "runtime = RepostCampaignRuntimeService(" in block
    assert "telethon_client=telethon_client" in block

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


def test_bot_has_export_callbacks_and_runtime_builder_usage():
    source = Path("bot.py").read_text(encoding="utf-8")

    assert "rule_repost_campaign_views_export_xlsx:" in source
    assert "rule_repost_campaign_views_export_csv:" in source
    assert "rule_repost_campaign_views_export_txt:" in source
    assert "rule_repost_campaign_post_export_xlsx:" in source
    assert "rule_repost_campaign_post_export_csv:" in source
    assert "rule_repost_campaign_post_export_txt:" in source
    assert "_build_repost_campaign_runtime()" in source
    assert "from app.repost_campaign_export_service import" in source
    assert "build_campaign_run_report_xlsx" in source
    assert "build_campaign_post_stats_xlsx" in source

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
    source = Path("bot.py").read_text(encoding="utf-8")
    assert '@dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_schedule_menu:"))' in source
    assert 'async def handle_rule_repost_campaign_schedule_menu' in source
    assert '@dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_schedule_confirm:"))' in source
    assert 'async def handle_rule_repost_campaign_schedule_confirm' in source
    assert 'REPOST_CAMPAIGN_SCHEDULE_CREATE_STARTED' in source
    assert 'REPOST_CAMPAIGN_SCHEDULE_CREATE_DONE' in source
    assert 'rule_repost_campaign_schedule_step4:' in source
    assert 'handle_rule_repost_campaign_schedule_step4' in source
    assert 'build_repost_campaign_schedule_wizard_step4_view' in source
    assert 'rule_repost_campaign_schedule_step4:{rule_id}' in source
    assert 'if int(readiness.get("show_seconds") or 0) <= 0' in source

def test_schedule_show_pick_goes_to_step4():
    source = Path("bot.py").read_text(encoding="utf-8")
    assert 'async def handle_rule_repost_campaign_schedule_show_pick' in source
    assert 'text, kb = build_repost_campaign_schedule_wizard_step4_view(rule_id=rule_id)' in source

def test_schedule_input_back_goes_to_step4():
    source = Path("bot.py").read_text(encoding="utf-8")
    assert '⬅️ Назад к выбору времени' in source
    assert 'rule_repost_campaign_schedule_step4:{rule_id}' in source

def test_manual_input_without_show_seconds_returns_step3():
    source = Path("bot.py").read_text(encoding="utf-8")
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
    source = Path("bot.py").read_text(encoding="utf-8")
    assert 'build_repost_campaign_launch_mode_view(rule_id=rule_id, readiness=readiness)' in source
    assert '@dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_launch_now_preview:"))' in source
    assert '@dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_schedule_current:"))' in source

from app.repost_campaign_ui import (
    build_vip_scheduled_posts_screen_view,
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

def test_vip_scheduled_posts_screen_empty_state():
    text, kb = build_vip_scheduled_posts_screen_view(rule_id=1, posts=[])
    assert '🕒 Запланированные посты' in text
    assert 'Шаг 1' in text
    assert '➕ Запланировать пост' in _texts_from_keyboard(kb)
    assert '📄 Все запланированные посты' in _texts_from_keyboard(kb)
    assert '⬅️ Назад' in _texts_from_keyboard(kb)
    assert '📚 Библиотека' not in text
    assert '📝 Черновики' not in text

def test_vip_scheduled_posts_screen_lists_nearest_posts():
    text, kb = build_vip_scheduled_posts_screen_view(rule_id=1, posts=[{'id':123,'status':'scheduled','scheduled_at':'2026-05-10T15:00:00+00:00','show_seconds':86400}])
    assert 'Пост #123' in text
    assert '🟢 Запланирован' in text
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
    assert '🔎 Проверить права' in labels
    assert '➕ Добавить канал/группу' in labels
    assert '📋 Выбрать канал/группу' in labels

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
    assert '👁 Предпросмотр' in labels

def test_vip_scheduled_post_detail_scheduled_has_cancel_and_check_buttons():
    text, kb = build_vip_scheduled_post_detail_view(rule_id=1, details={'post': {'id': 123, 'status': 'scheduled'}})
    labels = _texts_from_keyboard(kb)
    callbacks = _callbacks_from_keyboard(kb)
    assert '🟢 Запланирован' in text
    assert '🔎 Проверить права' in labels
    assert '🗑 Отменить' in labels
    assert 'rule_repost_campaign_scheduled_post_check_rights:1:123' in callbacks
    assert 'rule_repost_campaign_scheduled_post_cancel_confirm:1:123' in callbacks

def test_vip_scheduled_post_detail_launched_has_run_buttons():
    _, kb = build_vip_scheduled_post_detail_view(rule_id=1, details={'post': {'id':123, 'status':'launched', 'campaign_run_id':55}})
    labels = _texts_from_keyboard(kb)
    assert '📄 Открыть запуск' in labels
    assert '📊 Отчёт просмотров' in labels

def test_vip_scheduled_post_cancel_confirm_view():
    text, kb = build_vip_scheduled_post_cancel_confirm_view(rule_id=1, scheduled_post={'id':123})
    assert '🗑 Отменить запланированный пост?' in text
    labels = _texts_from_keyboard(kb)
    assert '✅ Да, отменить' in labels

def test_ordinary_schedule_callbacks_kept():
    from app.repost_campaign_ui import build_repost_campaign_schedule_preview_view, build_repost_campaign_scheduled_launch_detail_view, build_repost_campaign_scheduled_launch_cancel_result_view
    _, kb1 = build_repost_campaign_schedule_preview_view(rule_id=1, readiness={}, scheduled_at_utc=None)
    assert any('rule_repost_campaign_schedule_menu:1' in (b.callback_data or '') for r in kb1.inline_keyboard for b in r)

def test_bot_has_all_vip_scheduled_callbacks_prefixes():
    source = Path('bot.py').read_text(encoding='utf-8')
    for prefix in [
        'rule_repost_campaign_scheduled_post_pick_post:', 'rule_repost_campaign_scheduled_post_step_targets:',
        'rule_repost_campaign_scheduled_post_add_target:', 'rule_repost_campaign_scheduled_post_pick_targets:',
        'rule_repost_campaign_scheduled_post_snapshot_targets:', 'rule_repost_campaign_scheduled_post_check_rights:',
        'rule_repost_campaign_scheduled_post_step_show:', 'rule_repost_campaign_scheduled_post_pick_show:',
        'rule_repost_campaign_scheduled_post_step_time:', 'rule_repost_campaign_scheduled_post_quick_time:',
        'rule_repost_campaign_scheduled_post_input_time:', 'rule_repost_campaign_scheduled_post_preview:',
        'rule_repost_campaign_scheduled_post_confirm:', 'rule_repost_campaign_scheduled_post_detail:',
        'rule_repost_campaign_scheduled_post_cancel_confirm:', 'rule_repost_campaign_scheduled_post_cancel:'
    ]:
        assert prefix in source

def test_vip_detail_view_uses_nested_details_post():
    text, _ = build_vip_scheduled_post_detail_view(rule_id=1, details={'post': {'id': 321, 'status': 'scheduled', 'show_seconds': 3600, 'scheduled_at': '2026-05-10T15:00:00+00:00'}})
    assert '#321' in text
    assert '🟢 Запланирован' in text


def test_vip_targets_view_uses_readiness_counters():
    text, _ = build_vip_scheduled_post_wizard_targets_view(rule_id=1, scheduled_post={'id': 10}, targets=[], readiness={'targets_ready_count': 5, 'targets_warning_count': 2, 'targets_blocked_count': 1})
    assert '✅ Готовы: 5' in text
    assert '⚠️ Требуют проверки: 2' in text
    assert '🔴 Заблокированы: 1' in text


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
    source = Path('bot.py').read_text(encoding='utf-8')
    prefixes = [
        "rule_repost_campaign_scheduled_post_step_post:",
        "rule_repost_campaign_scheduled_post_pick_post:",
        "rule_repost_campaign_scheduled_post_step_targets:",
        "rule_repost_campaign_scheduled_post_add_target:",
        "rule_repost_campaign_scheduled_post_pick_targets:",
        "rule_repost_campaign_scheduled_post_snapshot_targets:",
        "rule_repost_campaign_scheduled_post_step_show:",
        "rule_repost_campaign_scheduled_post_pick_show:",
        "rule_repost_campaign_scheduled_post_step_time:",
        "rule_repost_campaign_scheduled_post_quick_time:",
        "rule_repost_campaign_scheduled_post_input_time:",
        "rule_repost_campaign_scheduled_post_preview:",
        "rule_repost_campaign_scheduled_post_confirm:",
        "rule_repost_campaign_scheduled_post_detail:",
        "rule_repost_campaign_scheduled_post_cancel_confirm:",
        "rule_repost_campaign_scheduled_post_cancel:",
        "rule_repost_campaign_scheduled_post_check_rights:",
    ]
    for prefix in prefixes:
        start = source.find(prefix)
        assert start != -1
        body = source[start:start+1200]
        assert "ensure_rule_callback_access" in body

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
    assert "➕ Добавить все на странице" in labels
    assert "➕ Добавить все" in labels

def test_bot_vip_pick_targets_is_not_placeholder():
    source = Path("bot.py").read_text(encoding="utf-8")
    assert "Скоро: выбор из известных каналов/групп" not in source
    assert "rule_repost_campaign_scheduled_post_add_known_target" in source
    assert "rule_repost_campaign_scheduled_post_add_known_page" in source
    assert "rule_repost_campaign_scheduled_post_add_known_all" in source

def test_bot_vip_pick_targets_has_pagination_callbacks():
    source = Path("bot.py").read_text(encoding="utf-8")
    assert "rule_repost_campaign_scheduled_post_pick_targets:" in source
    assert "page = int(parts[3]) if len(parts) > 3 else 0" in source


def test_vip_scheduled_posts_screen_has_only_three_main_buttons():
    _, kb = build_vip_scheduled_posts_screen_view(rule_id=1, posts=[])
    assert _texts_from_keyboard(kb) == ["➕ Запланировать пост", "📄 Все запланированные посты", "⬅️ Назад"]

def test_scheduled_post_new_button_does_not_open_library_or_choice():
    source = Path('bot.py').read_text(encoding='utf-8')
    assert '@dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_scheduled_post_new:"))' in source
    handler_body = source[source.find('async def handle_rule_repost_campaign_scheduled_post_new'):source.find('async def _open_vip_step_post')]
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
    source = Path('bot.py').read_text(encoding='utf-8')
    assert "Отправьте сюда рекламный пост" in source
    assert "После сохранения поста ViMi перейдёт к шагу 2" in source

def test_vip_scheduled_posts_screen_draft_lines_are_human():
    text, _ = build_vip_scheduled_posts_screen_view(rule_id=1, posts=[{'id':2,'status':'draft'}])
    assert '⚪ Черновик #2' in text
    assert '📝 Пост не выбран' in text
    assert '🕒 Время не задано' in text
    assert 'не указано · Пост #' not in text

def test_vip_scheduled_posts_buttons_use_draft_labels():
    _, kb = build_vip_scheduled_posts_screen_view(rule_id=1, posts=[{'id':2,'status':'draft'}])
    assert _texts_from_keyboard(kb) == ['➕ Запланировать пост', '📄 Все запланированные посты', '⬅️ Назад']
