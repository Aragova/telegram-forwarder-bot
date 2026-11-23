"""Experimental in-message rule buttons (Telegram Bot API >= 10.3).

Keep the existing inline-keyboard catalog as the authoritative fallback.
No rule, queue, persistence, or scheduler logic lives in this module.
"""

from __future__ import annotations

import logging
import os
from html import escape
from typing import Any, Callable

import httpx
from aiogram.types import InlineKeyboardMarkup

_RULE_CALLBACKS = frozenset({"open", "open_search", "pick", "fill_pick"})
# Two buttons per row if both labels are short. Otherwise one full-width button.
_TWO_COLUMN_MAX_CHARS = 27
_LOGGER = logging.getLogger(__name__)


def rich_rule_list_enabled(*, admin: bool = False) -> bool:
    """Deployment switch: admin (default), all (1), or classic (0).

    Keep the rollout limited to the owner until the test bots confirm that
    Telegram clients and the locally hosted Bot API support Rich messages.
    """
    mode = os.getenv("VIMI_RICH_RULE_LIST", "admin").strip().casefold()
    if mode in {"1", "true", "on", "yes", "all"}:
        return True
    if mode in {"admin", "owner", "test"}:
        return bool(admin)
    return False


def _rule_id_for_callback(callback_data: str | None) -> int | None:
    parts = str(callback_data or "").split(":")
    if len(parts) < 3 or parts[0] != "rf" or parts[1] not in _RULE_CALLBACKS:
        return None
    try:
        if parts[1] in {"open", "open_search"}:
            return int(parts[2])
        return int(parts[-1])
    except ValueError:
        return None


def _label_for_rule(rule: Any, *, selected: bool | None, in_folder: bool) -> str:
    rule_id = int(rule.id)
    name = str(rule.target_title or rule.target_id or rule.source_title or rule.source_id)
    state = "🟢" if bool(rule.is_active) else "⏸"
    folder_marker = "📁" if in_folder else ""
    checked = ("✅ " if selected else "☐ ") if selected is not None else ""
    return f"{checked}{state}{folder_marker} #{rule_id} {name}"


def _rich_button(label: str, callback_data: str) -> str:
    # Unlike an inline keyboard, these are Bot API 10.3 RichMessageButton
    # objects embedded directly in the rich message HTML.
    return (
        '<tg-button type="callback_data" style="primary" '
        f'data="{escape(callback_data, quote=True)}">'
        f"{escape(label)}</tg-button>"
    )


def _button_rows(buttons: list[tuple[str, str]]) -> list[str]:
    chunks: list[list[str]] = []
    pending: list[str] = []
    for label, callback_data in buttons:
        button = _rich_button(label, callback_data)
        if len(label) > _TWO_COLUMN_MAX_CHARS:
            if pending:
                chunks.append(pending)
                pending = []
            chunks.append([button])
        else:
            pending.append(button)
            if len(pending) == 2:
                chunks.append(pending)
                pending = []
    if pending:
        chunks.append(pending)
    return [
        '<tg-button-row align="center">' + "".join(chunk) + "</tg-button-row>"
        for chunk in chunks
    ]


def rich_catalog_payload(
    text: str,
    keyboard: InlineKeyboardMarkup,
    rules: list[Any],
    *,
    assigned: dict[int, dict[str, Any]] | None = None,
    selected: set[int] | None = None,
) -> tuple[str, InlineKeyboardMarkup] | None:
    """Turn ONLY rule-number buttons into rich buttons; leave navigation intact.

    Caller supplies the existing catalog view. Its callbacks remain the
    source of truth, so page/search/folder/bulk routing cannot drift.
    """
    by_id = {int(rule.id): rule for rule in rules}
    folder_map = assigned or {}
    button_specs: list[tuple[str, str]] = []
    footer_rows = []

    for row in keyboard.inline_keyboard:
        remaining = []
        for button in row:
            callback_data = button.callback_data or ""
            rule_id = _rule_id_for_callback(callback_data)
            if rule_id is None:
                remaining.append(button)
                continue
            rule = by_id.get(rule_id)
            if rule is None:
                # Do not silently create a button with the wrong rule ID.
                return None
            button_specs.append((
                _label_for_rule(
                    rule,
                    selected=(rule_id in selected) if selected is not None else None,
                    in_folder=rule_id in folder_map,
                ),
                callback_data,
            ))
        if remaining:
            footer_rows.append(remaining)

    if not button_specs:
        return None

    # Existing view already escapes its labels and formatted headings.
    # Everything after its blank header separator consists of route lines,
    # which are replaced by named buttons instead of repeated text.
    header = text.split("\n\n", 1)[0]
    heading_lines = [line for line in header.splitlines() if line.strip()]
    blocks = [f"<p>{line}</p>" for line in heading_lines]
    blocks.extend(_button_rows(button_specs))
    return "\n".join(blocks), InlineKeyboardMarkup(inline_keyboard=footer_rows)


def rich_edit_payload(
    *,
    chat_id: int,
    message_id: int,
    html: str,
    footer: InlineKeyboardMarkup,
) -> dict[str, Any]:
    return {
        "chat_id": chat_id,
        "message_id": message_id,
        "rich_message": {"html": html},
        "reply_markup": footer.model_dump(mode="json", exclude_none=True),
    }


async def try_edit_rich_catalog(
    *,
    bot: Any,
    message: Any,
    html: str,
    footer: InlineKeyboardMarkup,
    logger: Any = None,
    client_factory: Callable[..., Any] | None = None,
) -> bool:
    """Use configured LOCAL Bot API endpoint. Never log the bot token or URL.

    On an unsupported Bot API version / network failure return False, allowing
    the existing safe edit_message_text_safe() implementation to take over.
    """
    log = logger or _LOGGER
    if bot is None or message is None:
        return False
    try:
        url = bot.session.api.api_url(token=bot.token, method="editMessageText")
        payload = rich_edit_payload(
            chat_id=int(message.chat.id),
            message_id=int(message.message_id),
            html=html,
            footer=footer,
        )
        factory = client_factory or httpx.AsyncClient
        async with factory(timeout=10.0, trust_env=False) as client:
            response = await client.post(url, json=payload)
            data = response.json()
        if data.get("ok") is True:
            return True
        description = str(data.get("description") or "").lower()
        if "message is not modified" in description:
            return True
        log.warning(
            "RICH_RULE_CATALOG_FALLBACK | api_code=%s status=%s",
            data.get("error_code"), response.status_code,
        )
    except Exception as exc:
        # The exception text may contain the bot-token URL. Do not log it.
        log.warning(
            "RICH_RULE_CATALOG_FALLBACK | exception_type=%s",
            type(exc).__name__,
        )
    return False
