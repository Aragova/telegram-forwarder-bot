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
    build_repost_campaign_targets_id_actions_view,
    build_repost_campaign_targets_list_view,
    build_repost_campaign_target_preview_result_view,
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
    assert "🔎 Проверить" in texts and "🗑 Удалить" in texts
    assert "⏸ Пауза" not in texts and "▶️ Включить" not in texts
    assert "Аккаунт-парсер" not in text




def test_targets_list_view_does_not_show_raw_id_as_main_title():
    text, _ = build_repost_campaign_targets_list_view(rule_id=3, targets=[{"id": 1, "target_id": "-1002741117827", "title": None, "is_active": True}])
    assert "Канал/Группа #1" in text
    assert "ID: -1002741117827" not in text
    assert "1. 🟢 -1002741117827" not in text


def test_targets_list_view_manual_actions_copy():
    _, keyboard = build_repost_campaign_targets_list_view(rule_id=3, targets=[])
    texts = _texts_from_keyboard(keyboard)
    assert "⚙️ Управление вручную" in texts
    assert "⚙️ Действия по ID" not in texts


def test_targets_id_actions_view_uses_manual_wording():
    text, keyboard = build_repost_campaign_targets_id_actions_view(rule_id=3)
    texts = _texts_from_keyboard(keyboard)
    assert "⚙️ Управление вручную" in text
    assert "⏸ Поставить на паузу вручную" in texts
    assert "▶️ Включить вручную" in texts
    assert "🗑 Удалить вручную" in texts
    assert "по ID" not in text
    assert all("по ID" not in t for t in texts)


def test_targets_list_buttons_without_number_suffix():
    _, keyboard = build_repost_campaign_targets_list_view(rule_id=3, targets=[{"id": 1, "target_id": "-1001", "title": "A", "is_active": True}])
    texts = _texts_from_keyboard(keyboard)
    assert "⏸ Пауза" in texts
    assert "🔎 Проверить" in texts
    assert "🗑 Удалить" in texts
    assert "Пауза #1" not in texts
    assert "Проверить #1" not in texts
    assert "Удалить #1" not in texts


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
