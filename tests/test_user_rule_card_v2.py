from __future__ import annotations

import asyncio
from types import SimpleNamespace
from pathlib import Path

from app import user_ui
from app.user_handlers import rules as user_rules


def _texts(kb):
    return [[btn.text for btn in row] for row in kb.inline_keyboard]


def _callbacks(kb):
    return [btn.callback_data or "" for row in kb.inline_keyboard for btn in row]


def _base_snapshot(**overrides):
    data = {
        "id": 3,
        "target_title": "Mickey Twink 🍭",
        "target_id": "@target",
        "is_active": True,
        "mode": "repost",
        "schedule_mode": "interval",
        "next_run_at": "2026-04-28T14:54:00+00:00",
        "logical_pending": 153,
        "logical_processing": 0,
        "logical_completed": 67,
        "logical_faulty": 0,
        "logical_current_position": 22,
        "logical_total": 220,
        "caption_delivery_mode": "auto",
        "source_title": "SHOULD_NOT_BE_VISIBLE",
    }
    data.update(overrides)
    return data


def test_repost_card_text_contains_required_blocks():
    text = user_ui.build_user_rule_card_text(_base_snapshot())
    assert "⚙️ Правило #" in text
    assert "👉" in text
    assert "Работает · Репост" in text
    assert "Ждёт до" in text
    assert "В очереди" in text
    assert "В обработке" in text
    assert "Отправлено" in text
    assert "Ошибки" in text
    assert "Позиция" in text
    assert "Подпись" in text
    assert "──────────────" in text


def test_video_card_text_contains_required_blocks():
    text = user_ui.build_user_rule_card_text(_base_snapshot(id=59, mode="video", target_title="Family Club"))
    assert "⚙️ Правило #" in text
    assert "👉" in text
    assert "Работает · Видеоредактор" in text
    assert "Ждёт до" in text
    assert "В очереди" in text
    assert "В обработке" in text
    assert "Отправлено" in text
    assert "Ошибки" in text
    assert "Позиция" in text
    assert "Подпись" not in text


def test_repost_and_video_card_text_do_not_show_source():
    repost = user_ui.build_user_rule_card_text(_base_snapshot(source_title="SECRET SOURCE"))
    video = user_ui.build_user_rule_card_text(_base_snapshot(mode="video", source_title="SECRET SOURCE"))
    assert "SECRET SOURCE" not in repost
    assert "SECRET SOURCE" not in video


def test_repost_interval_keyboard_rows():
    rows = _texts(user_ui.build_user_rule_card_keyboard(rule_id=3, is_active=True, schedule_mode="interval", mode="repost"))
    assert rows[0] == ["🔁 Сделать фиксированным"]
    assert rows[1] == ["⏱ Интервал", "🕓 Время"]
    assert rows[2] == ["⚙️ Дополнительные функции"]
    assert rows[3] == ["🔄 Обновить", "⏸ Выключить"]
    assert rows[4] == ["⬅️ К правилам", "🏠 Главное меню"]


def test_repost_fixed_keyboard_rows():
    rows = _texts(user_ui.build_user_rule_card_keyboard(rule_id=3, is_active=False, schedule_mode="fixed", mode="repost"))
    assert rows[0] == ["🔁 Сделать плавающим"]
    assert rows[1] == ["🕓 Фикс. время", "🕓 Время"]
    assert rows[2] == ["⚙️ Дополнительные функции"]
    assert rows[3] == ["🔄 Обновить", "▶️ Включить"]
    assert rows[4] == ["⬅️ К правилам", "🏠 Главное меню"]


def test_video_keyboards_include_intro_and_caption():
    interval_rows = _texts(user_ui.build_user_rule_card_keyboard(rule_id=59, is_active=True, schedule_mode="interval", mode="video"))
    fixed_rows = _texts(user_ui.build_user_rule_card_keyboard(rule_id=59, is_active=True, schedule_mode="fixed", mode="video"))
    assert "🎬 Заставки" in interval_rows[2]
    assert "✍️ Подпись" in interval_rows[2]
    assert "🎬 Заставки" in fixed_rows[2]
    assert "✍️ Подпись" in fixed_rows[2]


def test_repost_extra_menu_contains_required_actions():
    texts = [x for row in _texts(user_ui.build_user_rule_extra_keyboard(rule_id=3, mode="repost")) for x in row]
    for expected in [
        "🔁 Сменить режим: Видеоредактор",
        "✍️ Режим подписи",
        "⚡ Отправить сейчас",
        "🔢 Начать с номера",
        "🔄 Пересканировать",
        "⏪ Откатить",
        "📜 Логи правила",
        "🗑 Удалить правило",
        "⬅️ Назад к правилу",
    ]:
        assert expected in texts


def test_video_extra_menu_contains_required_actions_and_no_caption_mode():
    texts = [x for row in _texts(user_ui.build_user_rule_extra_keyboard(rule_id=59, mode="video")) for x in row]
    for expected in [
        "🔁 Сменить режим: Репост",
        "⚡ Отправить сейчас",
        "🔢 Начать с номера",
        "🔄 Пересканировать",
        "⏪ Откатить",
        "📜 Логи правила",
        "🗑 Удалить правило",
        "⬅️ Назад к правилу",
    ]:
        assert expected in texts
    assert "✍️ Режим подписи" not in texts


def test_user_card_has_no_admin_global_buttons():
    callbacks = " ".join(_callbacks(user_ui.build_user_rule_card_keyboard(rule_id=7, is_active=True, schedule_mode="interval", mode="repost")))
    for forbidden in ["diagnostics", "system", "queue", "reset", "worker", "scheduler"]:
        assert forbidden not in callbacks


class _DummyCtx:
    def __init__(self, is_admin: bool, is_owned: bool):
        self._is_admin = is_admin
        self._is_owned = is_owned
        self.logger = SimpleNamespace(warning=lambda *args, **kwargs: None)
        self.is_rule_owned_by_user = lambda *_args, **_kwargs: is_owned

    def is_admin_user(self, _user_id):
        return self._is_admin

    async def run_db(self, func, rule_id, user_id):
        return self._is_owned

    async def answer_callback_safe(self, callback, text, show_alert=False):
        callback.answers.append((text, show_alert))


class _DummyCallback:
    def __init__(self, user_id: int):
        self.from_user = SimpleNamespace(id=user_id)
        self.answers = []


def test_non_admin_cannot_open_foreign_rule_card():
    callback = _DummyCallback(100)
    ok = asyncio.run(user_rules.ensure_rule_callback_access(_DummyCtx(False, False), callback, 99))
    assert ok is False
    assert callback.answers[-1][0] == "⛔ Нет доступа к этому правилу"


def test_non_admin_cannot_open_foreign_rule_logs():
    callback = _DummyCallback(100)
    ok = asyncio.run(user_rules.ensure_rule_callback_access(_DummyCtx(False, False), callback, 98))
    assert ok is False
    assert callback.answers[-1][0] == "⛔ Нет доступа к этому правилу"


def test_admin_bypass_for_rule_callbacks():
    callback = _DummyCallback(1)
    ok = asyncio.run(user_rules.ensure_rule_callback_access(_DummyCtx(True, False), callback, 77))
    assert ok is True


def test_navigation_callbacks_exist():
    card_callbacks = _callbacks(user_ui.build_user_rule_card_keyboard(rule_id=1, is_active=True, schedule_mode="interval", mode="repost"))
    extra_callbacks = _callbacks(user_ui.build_user_rule_extra_keyboard(rule_id=1, mode="repost"))
    logs_callbacks = _callbacks(user_ui.build_user_rule_logs_keyboard(rule_id=1, has_logs=True))

    assert any(cb.startswith("user_rule_open:") for cb in extra_callbacks)
    assert "user_rules" in card_callbacks
    assert "user_main" in card_callbacks
    assert any(cb.startswith("user_rule_logs_refresh:") for cb in logs_callbacks)


def test_rule_callbacks_with_rule_id_are_not_admin_only_for_user_flows():
    source = Path("bot.py").read_text(encoding="utf-8")
    for marker in [
        'c.data.startswith("rescan_rule_menu:")',
        'c.data.startswith("rescan_rule_fresh:")',
        'c.data.startswith("rescan_rule_keep:")',
        'c.data.startswith("startpos_prev:")',
        'c.data.startswith("startpos_next:")',
        'c.data.startswith("startpos_apply:")',
        'c.data.startswith("startpos_cancel:")',
    ]:
        idx = source.find(marker)
        assert idx >= 0
        block = source[idx: idx + 800]
        assert "ensure_rule_callback_access(callback, rule_id)" in block
