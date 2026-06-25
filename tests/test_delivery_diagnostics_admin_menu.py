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
    assert menu_source.index("⚠️ Проблемные доставки") < menu_source.index("📊 Диагностика доставки") < menu_source.index("📊 Журнал системы")

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
