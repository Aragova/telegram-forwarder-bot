"""Order private-chat callbacks and serialize edits without a global UI lock."""
from __future__ import annotations

import asyncio
from weakref import WeakValueDictionary


def message_key(chat_id: int | str, message_id: int) -> tuple[int | str, int]:
    try:
        chat_id = int(chat_id)
    except (ValueError, TypeError):
        pass
    return chat_id, int(message_id)


class MessageLocks:
    """Active holders/waiters own the locks; idle keys are not retained."""

    def __init__(self) -> None:
        self._locks: WeakValueDictionary = WeakValueDictionary()

    def get(self, chat_id: int | str, message_id: int) -> asyncio.Lock:
        key = (asyncio.get_running_loop(), *message_key(chat_id, message_id))
        lock = self._locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[key] = lock
        return lock


callback_locks = MessageLocks()
edit_locks = MessageLocks()

_FOLDER_NAVIGATION = frozenset({
    "root", "folders", "list", "open", "open_search", "back", "results",
    "folder_settings", "move", "move_page", "move_back", "fill", "batch",
    "bulk_to", "noop",
})
_RULE_NAVIGATION = frozenset({
    "rule_card", "rule_refresh", "rule_extra_menu", "rule_to_list",
    "rules_page", "rules_back", "user_rules", "user_rules_page",
    "user_rule_open", "user_rule_refresh", "user_rule_extra",
})


def is_navigation(data: str | None) -> bool:
    parts = (data or "").split(":")
    if parts[0] == "rf":
        return len(parts) > 1 and parts[1] in _FOLDER_NAVIGATION
    return parts[0] in _RULE_NAVIGATION


async def ack_navigation(callback, answer_once) -> None:
    if not getattr(callback, "_safe_answer_sent", False) and await answer_once(callback):
        setattr(callback, "_navigation_ack_sent", True)


class CallbackNavigationMiddleware:
    """FIFO within a message, independent progress for other messages.

    ACK read-only navigation while it waits; do not pre-ACK write actions,
    which can require confirmation or an alert. Handlers retain their checks.
    """

    def __init__(self, answer_once) -> None:
        self._answer_once = answer_once

    async def __call__(self, handler, event, data):
        message = getattr(event, "message", None)
        chat = getattr(message, "chat", None)
        message_id = getattr(message, "message_id", None)
        if getattr(chat, "type", None) != "private" or message_id is None:
            return await handler(event, data)
        lock = callback_locks.get(chat.id, message_id)
        ack_task = None
        if is_navigation(getattr(event, "data", None)):
            ack_task = asyncio.create_task(ack_navigation(event, self._answer_once))
        try:
            async with lock:
                if ack_task is not None:
                    await ack_task
                return await handler(event, data)
        finally:
            if ack_task is not None and not ack_task.done():
                ack_task.cancel()
                await asyncio.gather(ack_task, return_exceptions=True)


def edit_result_status(result) -> str:
    """Only a confirmed edit or confirmed identical screen is successful."""
    if result.ok:
        return "updated"
    if result.reason in {"message_not_modified", "same_message_signature"}:
        return "not_modified"
    if result.reason in {
        "message_id_invalid", "message_to_edit_not_found",
        "message_cant_be_edited", "no_text_to_edit",
        "message_is_none", "message_identifiers_missing",
    }:
        return "gone"
    return "failed"
