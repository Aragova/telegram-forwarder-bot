from pathlib import Path
import ast

from app.repost_campaign_ui import (
    build_repost_campaign_delete_result_view,
    build_repost_campaign_launch_result_view,
    build_repost_campaign_launch_readiness_view,
    build_repost_campaign_menu_view,
    build_repost_campaign_post_menu_view,
    build_repost_campaign_views_report_loading_view,
    build_repost_campaign_views_report_error_view,
    build_repost_campaign_vip_features_view,
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
    assert "🚦 Проверка перед запуском" in text
    assert "Кампания готова к запуску" in text
    assert "Будет опубликовано" in text
    labels = _kb_texts(keyboard)
    callbacks = _callbacks_from_keyboard(keyboard)
    assert "🚀 Подтвердить запуск" in labels
    assert "rule_repost_campaign_launch_confirm:3" in callbacks
    assert "rule_repost_campaign_launch:3" not in callbacks


def test_launch_readiness_view_ready_mentions_final_confirmation():
    text, _ = build_repost_campaign_launch_readiness_view(rule_id=3, readiness={"can_launch": True, "saved_post_id": 7, "saved_post_exists": True, "show_seconds": 3600, "main_target_ready": True, "will_send_total": 3, "will_skip_total": 0, "extra_paused": 0, "extra_problem": 0})
    assert "финальная проверка" in text
    assert "После подтверждения" in text

def test_launch_readiness_view_blocked_has_no_confirm_button():
    text, keyboard = build_repost_campaign_launch_readiness_view(rule_id=3, readiness={"can_launch": False, "saved_post_exists": True, "show_seconds": 300, "main_target_ready": True, "extra_active_problem": 1, "extra_problem": 1, "will_send_total": 1, "will_skip_total": 1, "extra_paused": 0, "block_reasons": ["Есть активные каналы/группы, которые требуют настройки."]})
    assert "Нужно проверить каналы/группы" in text
    labels = _kb_texts(keyboard)
    assert "🚀 Подтвердить запуск" not in labels
    assert "🔎 Проверить права" in labels
    assert "📣 Каналы/Группы" in labels

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
    assert "💰 К кампании" in _texts_from_keyboard(keyboard)


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
