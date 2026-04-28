from __future__ import annotations

from types import SimpleNamespace
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import bot
import asyncio
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup


class _DummyFromUser:
    def __init__(self, user_id: int):
        self.id = user_id


class _DummyCallback:
    def __init__(self, user_id: int):
        self.from_user = _DummyFromUser(user_id)
        self.data = "test"
        self.answers: list[tuple[str | None, bool]] = []

    async def answer(self, text: str | None = None, show_alert: bool = False):
        self.answers.append((text, show_alert))


def test_user_cannot_open_foreign_rule(monkeypatch):
    callback = _DummyCallback(user_id=111)

    async def _fake_run_db(func, rule_id, user_id):
        return False

    monkeypatch.setattr(bot, "run_db", _fake_run_db)

    allowed = asyncio.run(bot.ensure_rule_callback_access(callback, 77))

    assert allowed is False
    assert callback.answers
    assert callback.answers[-1][0] == "⛔ Нет доступа к этому правилу"


def test_user_can_open_own_rule(monkeypatch):
    callback = _DummyCallback(user_id=111)

    async def _fake_run_db(func, rule_id, user_id):
        return True

    monkeypatch.setattr(bot, "run_db", _fake_run_db)

    allowed = asyncio.run(bot.ensure_rule_callback_access(callback, 77))

    assert allowed is True
    assert callback.answers == []


def test_admin_bypass_not_broken(monkeypatch):
    admin_id = int(bot.settings.admin_id)
    callback = _DummyCallback(user_id=admin_id)

    called = {"run_db": False}

    async def _fake_run_db(*args, **kwargs):
        called["run_db"] = True
        return False

    monkeypatch.setattr(bot, "run_db", _fake_run_db)

    allowed = asyncio.run(bot.ensure_rule_callback_access(callback, 42))

    assert allowed is True
    assert called["run_db"] is False


def test_empty_user_rules_keyboard_has_add_button():
    kb = bot.build_user_rules_keyboard([], page=0)
    texts = [button.text for row in kb.inline_keyboard for button in row]
    assert "➕ Добавить правило" in texts
    assert "⬅️ Назад" in texts


def test_user_rules_keyboard_always_has_add_button():
    rules = [SimpleNamespace(id=1, source_id="s", source_title=None, target_id="t", target_title=None, source_thread_id=None, target_thread_id=None, interval=60, is_active=True)]
    kb = bot.build_user_rules_keyboard(rules, page=0)
    texts = [button.text for row in kb.inline_keyboard for button in row]
    assert "➕ Добавить правило" in texts


def test_user_card_hides_admin_only_buttons():
    base = bot.build_rule_card_keyboard(rule_id=13, is_active=True, schedule_mode="interval", mode="repost")
    user_kb = bot._filter_user_rule_card_keyboard(base, 13)
    callbacks = [button.callback_data or "" for row in user_kb.inline_keyboard for button in row]
    joined = " ".join(callbacks)

    assert "rule_to_main_menu" not in joined
    assert "user_rules" in joined

    def _fake_build_rule_extra_keyboard(_rule_id: int) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="🎛 Режим", callback_data=f"toggle_rule_mode:{_rule_id}")],
                [InlineKeyboardButton(text="⚡ Отправить сейчас", callback_data=f"trigger_now:{_rule_id}")],
                [InlineKeyboardButton(text="↪ Начать с номера", callback_data=f"start_from_number:{_rule_id}")],
                [InlineKeyboardButton(text="🔄 Пересканировать", callback_data=f"rescan_rule_menu:{_rule_id}")],
                [InlineKeyboardButton(text="⏪ Откатить", callback_data=f"rollback:{_rule_id}")],
                [InlineKeyboardButton(text="🧾 Логи", callback_data=f"rule_logs:{_rule_id}")],
                [InlineKeyboardButton(text="🗑 Удалить", callback_data=f"delete_rule:{_rule_id}")],
                [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"rule_card:{_rule_id}")],
            ]
        )

    original = bot.build_rule_extra_keyboard
    bot.build_rule_extra_keyboard = _fake_build_rule_extra_keyboard
    try:
        extra_kb = bot.build_user_rule_extra_keyboard(13)
    finally:
        bot.build_rule_extra_keyboard = original
    extra_callbacks = [button.callback_data or "" for row in extra_kb.inline_keyboard for button in row]
    extra_joined = " ".join(extra_callbacks)
    assert " rule_logs:" not in f" {extra_joined}"
    assert " rescan_rule_menu:" not in f" {extra_joined}"
    assert " rollback:" not in f" {extra_joined}"


def test_user_creation_flow_skips_interval_input_for_non_admin():
    source = Path("bot.py").read_text(encoding="utf-8")
    block_start = source.find("async def handle_user_rule_pick_target_callback")
    assert block_start >= 0
    block = source[block_start:block_start + 2800]
    assert "rule_id = await run_db(_create_rule_sync, state[\"choice\"], 3600, user_id)" in block
    assert "parse_channel_history" in block or "parse_group_history" in block
    assert "db.backfill_rule" in block
    assert "ReplyKeyboardRemove()" in source


def test_user_creation_flow_uses_inline_pick_callbacks():
    source = Path("bot.py").read_text(encoding="utf-8")
    assert "user_rule_pick_source:" in source
    assert "user_rule_pick_target:" in source
