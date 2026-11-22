"""Regression: the '🎛 Режим подписи' inline button must be routed and editable."""

import ast
import asyncio
import copy
from pathlib import Path
from types import SimpleNamespace

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup


SOURCE = Path(__file__).resolve().parents[1] / "bot.py"


def _get_function(name, namespace):
    module = ast.parse(SOURCE.read_text(encoding="utf-8"))
    fn = next(
        node for node in module.body
        if isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef)) and node.name == name
    )
    fn = copy.deepcopy(fn)
    fn.decorator_list = []
    compiled = compile(ast.fix_missing_locations(ast.Module(body=[fn], type_ignores=[])), str(SOURCE), "exec")
    exec(compiled, namespace)
    return namespace[name]


def _callback_namespace(*, rule_mode="video", allowed=True, edited=True):
    events = []
    rule = SimpleNamespace(mode=rule_mode)

    async def check_access(callback, rule_id):
        events.append(("access", rule_id))
        return allowed

    async def run_db(fn, *args):
        return fn(*args)

    async def edit_message_text_safe(**kwargs):
        events.append(("edit", kwargs))
        return edited

    async def answer_callback_safe(callback, message, *, show_alert=False):
        events.append(("alert", message, show_alert))

    async def answer_callback_safe_once(callback, *args):
        events.append(("answered", args))

    namespace = {
        "CallbackQuery": object,
        "InlineKeyboardButton": InlineKeyboardButton,
        "InlineKeyboardMarkup": InlineKeyboardMarkup,
        "db": SimpleNamespace(get_rule=lambda rule_id: rule),
        "run_db": run_db,
        "ensure_rule_callback_access": check_access,
        "answer_callback_safe": answer_callback_safe,
        "answer_callback_safe_once": answer_callback_safe_once,
        "edit_message_text_safe": edit_message_text_safe,
        "build_video_caption_mode_menu_text": lambda rule_id: (
            "🎛 <b>Режим подписи</b>\\n⚡ Обычный\\n💎 Премиум"
        ),
        "logger": SimpleNamespace(exception=lambda *args: events.append(("log", args))),
    }
    namespace["build_video_caption_mode_keyboard"] = _get_function(
        "build_video_caption_mode_keyboard", namespace,
    )
    fn = _get_function("handle_video_caption_mode_menu", namespace)
    callback = SimpleNamespace(
        data="video_caption_mode_menu:57",
        message=SimpleNamespace(chat=SimpleNamespace(id=777), message_id=8),
    )
    return fn, callback, events


def test_callback_button_has_registered_handler_and_correct_prefix():
    module = ast.parse(SOURCE.read_text(encoding="utf-8"))
    fn = next(n for n in module.body if isinstance(n, ast.AsyncFunctionDef)
              and n.name == "handle_video_caption_mode_menu")
    assert any("video_caption_mode_menu:" in ast.unparse(d) for d in fn.decorator_list)
    source = SOURCE.read_text(encoding="utf-8")
    assert 'callback_data=f"video_caption_mode_menu:{rule_id}"' in source


def test_video_caption_button_opens_edit_in_place_with_two_choices():
    fn, callback, events = _callback_namespace()
    asyncio.run(fn(callback))
    assert events[0] == ("access", 57)
    edit = next(v[1] for v in events if v[0] == "edit")
    assert edit["message"] is callback.message
    assert edit["parse_mode"] == "HTML"
    assert "Режим подписи" in edit["text"]
    buttons = [
        button.callback_data for row in edit["reply_markup"].inline_keyboard for button in row
    ]
    assert buttons == [
        "set_video_caption_mode_copy_first:57",
        "set_video_caption_mode_builder_first:57",
        "video_caption_menu:57",
    ]
    assert events[-1][0] == "answered"
    assert not any(v[0] == "alert" for v in events)


def test_video_caption_button_denied_for_non_video_rule():
    fn, callback, events = _callback_namespace(rule_mode="repost")
    asyncio.run(fn(callback))
    assert any(v[0] == "alert" and v[2] for v in events)
    assert not any(v[0] == "edit" for v in events)


def test_video_caption_button_reports_failed_edit_without_duplicate_message():
    fn, callback, events = _callback_namespace(edited=False)
    asyncio.run(fn(callback))
    assert len([v for v in events if v[0] == "edit"]) == 1
    assert any(v[0] == "alert" and v[2] for v in events)
    assert not any(v[0] == "answered" for v in events)


def test_video_caption_button_respects_access_check():
    fn, callback, events = _callback_namespace(allowed=False)
    asyncio.run(fn(callback))
    assert events == [("access", 57)]
