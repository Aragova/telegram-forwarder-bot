from __future__ import annotations

import inspect
from pathlib import Path

from app.postgres_repository import PostgresRepository

ROOT = Path(__file__).resolve().parents[1]


def test_delivery_diagnostics_button_is_in_main_diagnostics_menu_next_to_existing_items() -> None:
    source = (ROOT / "app/keyboards.py").read_text(encoding="utf-8")
    start = source.index("def get_diagnostics_menu")
    end = source.index("def get_system_menu", start)
    menu_source = source[start:end]

    assert "📊 Диагностика доставки" in menu_source
    assert "⚠️ Проблемные доставки" in menu_source
    assert "📊 Журнал системы" in menu_source
    assert "🧯 Зависшие задачи" in menu_source
    assert menu_source.index("⚠️ Проблемные доставки") < menu_source.index("📊 Диагностика доставки") < menu_source.index("🧯 Зависшие задачи") < menu_source.index("📊 Журнал системы")

    bot_source = (ROOT / "bot.py").read_text(encoding="utf-8")
    rule_card_start = bot_source.find("def build_rule_card")
    if rule_card_start != -1:
        next_section = bot_source.find("\ndef ", rule_card_start + 1)
        rule_card_source = bot_source[rule_card_start:next_section if next_section != -1 else len(bot_source)]
        assert "📊 Диагностика доставки" not in rule_card_source


def test_delivery_diagnostics_handler_is_admin_only() -> None:
    source = (ROOT / "app/admin_handlers/diagnostics.py").read_text(encoding="utf-8")
    start = source.index("async def handle_delivery_diagnostics")
    end = source.index("@dp.message(lambda m: m.text == \"📊 Журнал системы\")", start)
    handler_source = source[start:end]

    assert "📊 Диагностика доставки" in source
    assert "ctx.is_admin(message)" in handler_source
    assert "build_delivery_diagnostics_admin_text" in handler_source
    assert 'parse_mode="HTML"' in handler_source


def test_delivery_observability_repository_method_is_read_only_select() -> None:
    source = inspect.getsource(PostgresRepository.get_delivery_observability_rule_metrics)
    upper = source.upper()

    assert "SELECT" in upper
    assert "DELIVERYRULEMETRICS" in upper
    for forbidden in ("UPDATE", "INSERT", "DELETE", "FOR UPDATE", "DROP", "ALTER", "CREATE TABLE"):
        assert forbidden not in upper


def test_forbidden_runtime_files_do_not_contain_delivery_diagnostics_button() -> None:
    for relative_path in ("app/sender.py", "app/worker_runtime.py", "app/video_processor.py"):
        source = (ROOT / relative_path).read_text(encoding="utf-8")
        assert "📊 Диагностика доставки" not in source


def test_stuck_delivery_handlers_are_admin_only_and_confirmation_required() -> None:
    source = (ROOT / "app/admin_handlers/diagnostics.py").read_text(encoding="utf-8")
    start = source.index("async def handle_stuck_deliveries")
    end = source.index('@dp.message(lambda m: m.text == "📊 Журнал системы")', start)
    handler_source = source[start:end]
    assert "ctx.is_admin(message)" in handler_source
    assert "ctx.is_admin_callback(callback)" in handler_source
    assert "stuck_reset_request" in handler_source
    assert "stuck_reset_confirm" in handler_source
    assert handler_source.index("stuck_reset_request") < handler_source.index("recover_stale_delivery_jobs")
    request_block = handler_source[handler_source.index("async def handle_stuck_reset_request"):handler_source.index("async def handle_stuck_reset_confirm")]
    assert "recover_stale_delivery_jobs" not in request_block


def test_stuck_button_not_added_to_rule_card() -> None:
    bot_source = (ROOT / "bot.py").read_text(encoding="utf-8")
    rule_card_start = bot_source.find("def build_rule_card")
    if rule_card_start != -1:
        next_section = bot_source.find("\ndef ", rule_card_start + 1)
        rule_card_source = bot_source[rule_card_start:next_section if next_section != -1 else len(bot_source)]
        assert "🧯 Зависшие задачи" not in rule_card_source


def test_faulty_keyboard_opens_correct_rule_and_preserves_clear() -> None:
    import ast
    from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

    source = (ROOT / "bot.py").read_text(encoding="utf-8")
    node = next(n for n in ast.parse(source).body
                if isinstance(n, ast.FunctionDef) and n.name == "build_faulty_inline_keyboard")
    namespace = {"InlineKeyboardButton": InlineKeyboardButton,
                 "InlineKeyboardMarkup": InlineKeyboardMarkup}
    exec(compile(ast.Module(body=[node], type_ignores=[]), "bot.py", "exec"), namespace)
    build = namespace["build_faulty_inline_keyboard"]
    keyboard = build(1, 3, 12345, rule_id=208)
    buttons = [button for row in keyboard.inline_keyboard for button in row]
    assert buttons[0].text == "📂 Открыть правило"
    assert buttons[0].callback_data == "rule_card:208"
    assert buttons[1].text == "🧹 Очистить ошибку"
    assert buttons[1].callback_data == "faulty_clear:12345:1"
    assert not any((button.callback_data or "").startswith("faulty_ack:") for button in buttons)
    empty = build(0, 1)
    assert [button.callback_data for row in empty.inline_keyboard for button in row] == [
        "faulty_refresh:0", "faulty_back",
    ]


def test_legacy_ack_is_read_only_and_requires_admin() -> None:
    import ast

    for path in ("bot.py", "app/admin_handlers/diagnostics.py"):
        source = (ROOT / path).read_text(encoding="utf-8")
        handlers = [n for n in ast.walk(ast.parse(source))
                    if isinstance(n, ast.AsyncFunctionDef) and n.name == "handle_faulty_ack"]
        assert len(handlers) == 1
        body = ast.get_source_segment(source, handlers[0])
        assert "is_admin_callback" in body
        assert "Обновить" in body
        assert "resolve_problem" not in body
        assert "run_db" not in body



def test_orphan_recovery_instruction_uses_safe_rich_format():
    from app.admin_handlers.diagnostics import _format_orphan_recovery_instruction
    from html.parser import HTMLParser

    for rule_id in (None, 547):
        text = _format_orphan_recovery_instruction(rule_id)
        assert text.startswith("<b>🧯 ")
        assert "<b>Причина</b>" in text
        assert "<b>Что делать</b>" in text
        assert all(f"{i}. " in text for i in (1, 2, 3))
        assert "История сохранится" in text
        assert "не отправит их повторно" in text
        assert len(text.encode("utf-16-le")) // 2 <= 4000
        class Balanced(HTMLParser):
            def __init__(self):
                super().__init__()
                self.tags = []

            def handle_starttag(self, tag, attrs):
                self.tags.append(tag)

            def handle_endtag(self, tag):
                assert self.tags.pop() == tag

        parser = Balanced()
        parser.feed(text)
        assert not parser.tags



def test_orphan_confirmation_is_one_use_and_bound_to_operator():
    from types import SimpleNamespace as NS
    from app.admin_handlers.diagnostics import _OrphanConfirmations, _orphan_page_keyboard
    store = _OrphanConfirmations()
    cb = NS(from_user=NS(id=1), message=NS(chat=NS(id=2), message_id=3), data="")
    token = store.issue(cb, 547, [10, 11])
    cb.data = "stuck_orphan_confirm:" + token
    other = NS(from_user=NS(id=9), message=cb.message, data=cb.data)
    assert store.consume(other) is None
    assert store.consume(cb) == (547, [10, 11])
    assert store.consume(cb) is None
    rows = [{"rule_id": i, "is_active": False} for i in range(1, 11)]
    keyboard = _orphan_page_keyboard(rows, 0)
    assert keyboard.inline_keyboard[-1][-1].callback_data == "stuck_orphan_page:10"
    assert _orphan_page_keyboard([], 10).inline_keyboard[-1][0].callback_data == "stuck_orphan_page:0"


def test_orphan_confirmation_rejects_missing_identity_without_consuming_token():
    from types import SimpleNamespace as NS
    from app.admin_handlers.diagnostics import _OrphanConfirmations

    store = _OrphanConfirmations()
    cb = NS(from_user=NS(id=1), message=NS(chat=NS(id=2), message_id=3), data="")
    token = store.issue(cb, 547, [10])
    cb.data = "stuck_orphan_cancel:" + token
    assert store.consume(NS(from_user=None, message=cb.message, data=cb.data)) is None
    assert store.consume(NS(from_user=cb.from_user, message=None, data=cb.data)) is None
    assert store.consume(cb) == (547, [10])


def test_quarantine_traceback_identifies_operation_without_exception_payload():
    from app.admin_handlers.diagnostics import _safe_quarantine_traceback

    def failing_db_operation():
        raise RuntimeError("private-database-value")

    try:
        failing_db_operation()
    except RuntimeError as exc:
        trace = _safe_quarantine_traceback(exc)
    assert "failing_db_operation" in trace
    assert "test_delivery_diagnostics_admin_menu.py:" in trace
    assert "private-database-value" not in trace
