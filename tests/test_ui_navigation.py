from __future__ import annotations

import asyncio
import ast
import logging
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from app.ui_navigation import CallbackNavigationMiddleware, edit_result_status


def callback(data, message_id=42):
    return SimpleNamespace(
        data=data, message=SimpleNamespace(
            chat=SimpleNamespace(id=123, type="private"), message_id=message_id,
        ),
    )


def test_navigation_orders_handlers_and_acks_before_waiting_for_database():
    async def scenario():
        started, release, acked = asyncio.Event(), asyncio.Event(), asyncio.Event()
        actions = []

        async def answer(event):
            event._safe_answer_sent = True
            if event.data == "rule_card:1":
                acked.set()
            return True

        middleware = CallbackNavigationMiddleware(answer)

        async def handler(event, _data):
            if event.data == "rf:list:-1:0":
                started.set()
                await release.wait()
            actions.append(event.data)

        first = asyncio.create_task(middleware(handler, callback("rf:list:-1:0"), {}))
        await started.wait()
        second = asyncio.create_task(middleware(handler, callback("rule_card:1"), {}))
        await asyncio.wait_for(acked.wait(), timeout=1)
        # Another message never waits for this screen's database call.
        await asyncio.wait_for(middleware(handler, callback("rf:root", 43), {}), timeout=1)
        assert actions == ["rf:root"]
        release.set()
        await asyncio.gather(first, second)
        assert actions == ["rf:root", "rf:list:-1:0", "rule_card:1"]

    asyncio.run(scenario())


def test_write_actions_are_not_preacknowledged_and_lock_releases_on_error():
    async def scenario():
        answer = AsyncMock(return_value=True)
        middleware = CallbackNavigationMiddleware(answer)
        handler = AsyncMock(side_effect=RuntimeError("failed"))
        with pytest.raises(RuntimeError):
            await middleware(handler, callback("rf:delete:1:0"), {})
        answer.assert_not_awaited()
        handler.side_effect = None
        await asyncio.wait_for(middleware(handler, callback("rf:back:1"), {}), timeout=1)
        answer.assert_awaited_once()

    asyncio.run(scenario())


def load_function(name, namespace):
    tree = ast.parse(Path("bot.py").read_text(encoding="utf-8"))
    node = next(n for n in tree.body if isinstance(n, ast.AsyncFunctionDef) and n.name == name)
    node.decorator_list = []
    exec(compile(ast.Module(body=[node], type_ignores=[]), "bot.py", "exec"), namespace)
    return namespace[name]


@pytest.mark.parametrize("reason, expected", [
    ("same_message_signature", "not_modified"),
    ("message_not_modified", "not_modified"),
    ("chat_retry_after_active", "error"),
    ("retry_after", "error"),
    ("message_edit_throttled", "error"),
])
def test_cached_card_always_attempts_render_and_does_not_hide_failure(reason, expected):
    edit = AsyncMock(return_value=SimpleNamespace(ok=False, reason=reason))
    render = load_function("refresh_rule_card_message", {
        "CallbackQuery": object,
        "build_rule_card_payload_cached": AsyncMock(return_value=("CARD", "keyboard", "cache_hit")),
        "edit_message_text_safe_result": edit, "edit_result_status": edit_result_status,
        "invalidate_rule_card_cache": Mock(), "logger": logging.getLogger(__name__),
    })
    result = asyncio.run(render(callback("rule_card:1"), 1))
    assert result == expected
    edit.assert_awaited_once()


def test_cached_card_by_ids_always_attempts_render():
    edit = AsyncMock(return_value="updated")
    render = load_function("refresh_rule_card_by_ids", {
        "build_rule_card_payload_cached": AsyncMock(return_value=("CARD", "keyboard", "cache_hit")),
        "try_edit_message_text_by_ids_safe": edit,
        "invalidate_rule_card_cache": Mock(), "logger": logging.getLogger(__name__),
    })
    assert asyncio.run(render(chat_id=123, message_id=42, rule_id=1)) == "updated"
    edit.assert_awaited_once()


@pytest.mark.parametrize("early", [True, False])
def test_alert_after_any_ack_uses_deferred_notification(early):
    notify = Mock(return_value=True)
    ns = {
        "CallbackQuery": object, "ui_policy": SimpleNamespace(notify_after_ack=notify),
        "send_message_safe": AsyncMock(), "logger": logging.getLogger(__name__),
    }
    load_function("notify_callback_error", ns)
    load_function("answer_callback_safe", ns)
    once = load_function("answer_callback_safe_once", ns)
    event = callback("rf:apply:1")
    event._safe_answer_sent = True
    event._navigation_ack_sent = early
    assert asyncio.run(once(event, "Нет доступа", show_alert=True))
    notify.assert_called_once_with(chat_id=123, text="Нет доступа")
    assert not asyncio.run(once(event))
    notify.assert_called_once()


def test_rule_card_invalidation_during_render_prevents_stale_cache():
    import time

    tree = ast.parse(Path("bot.py").read_text(encoding="utf-8"))
    names = {"build_rule_card_payload_cached", "invalidate_rule_card_cache",
             "_get_cached_rule_card", "_get_rule_card_build_lock"}
    nodes = [n for n in tree.body
             if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name in names]

    async def scenario():
        started, release = asyncio.Event(), asyncio.Event()
        renders = 0

        async def render(rule_id):
            nonlocal renders
            renders += 1
            if renders == 1:
                started.set()
                await release.wait()
                return {"text": "paused", "reply_markup": "enable"}
            return {"text": "active", "reply_markup": "disable"}

        ns = {"asyncio": asyncio, "time": time, "InlineKeyboardMarkup": object,
              "RuleCardCacheEntry": SimpleNamespace, "RULE_CARD_CACHE_TTL_SEC": 15,
              "rule_card_cache": {}, "rule_card_build_locks": {},
              "rule_card_render_tokens": {}, "_build_rule_card_render_async": render}
        exec(compile(ast.Module(body=nodes, type_ignores=[]), "bot.py", "exec"), ns)
        build = ns["build_rule_card_payload_cached"]
        first = asyncio.create_task(build(94))
        await asyncio.wait_for(started.wait(), timeout=1)
        ns["invalidate_rule_card_cache"](94)
        release.set()
        assert await first == ("paused", "enable", "rebuilt")
        assert not ns["rule_card_cache"]
        assert not ns["rule_card_render_tokens"]
        assert await build(94) == ("active", "disable", "rebuilt")
        assert await build(94) == ("active", "disable", "cache_hit")
        assert renders == 2
        assert not ns["rule_card_render_tokens"]

    asyncio.run(scenario())
