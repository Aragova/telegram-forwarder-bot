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
