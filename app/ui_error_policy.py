from __future__ import annotations

import logging
from time import monotonic
from dataclasses import dataclass
from typing import Any

from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError, TelegramRetryAfter


logger = logging.getLogger("forwarder.ui")


@dataclass(slots=True)
class UIActionResult:
    ok: bool
    skipped: bool = False
    reason: str | None = None
    result: Any = None


class UIErrorPolicy:
    """
    Единая SaaS-политика UI-ошибок Telegram для bot.py.

    Задачи:
    - не падать на устаревших callback query
    - не падать на MESSAGE_ID_INVALID / message is not modified / can't be edited
    - одинаково логировать все UI-сбои
    - вернуть единый контракт результата
    """

    MESSAGE_EDIT_THROTTLE_SECONDS = 0.5

    def __init__(self, bot) -> None:
        self.bot = bot
        self._chat_retry_after_until: dict[int | str, float] = {}
        self._last_edit_at: dict[tuple[int | str, int], float] = {}
        self._last_edit_signature: dict[tuple[int | str, int], tuple[str, str]] = {}

    # =========================================================
    # CLASSIFICATION
    # =========================================================

    def _classify_bad_request(self, exc: TelegramBadRequest) -> tuple[str, bool]:
        """
        Возвращает:
        - reason
        - suppress_exception (True = гасим ошибку и не валим хендлер)
        """
        text = str(exc).lower()

        # callback
        if "query is too old" in text:
            return "callback_query_too_old", True

        if "query id is invalid" in text:
            return "callback_query_invalid", True

        # edit
        if "message is not modified" in text:
            return "message_not_modified", True

        if "message to edit not found" in text:
            return "message_to_edit_not_found", True

        if "message can't be edited" in text:
            return "message_cant_be_edited", True

        if "message_id_invalid" in text:
            return "message_id_invalid", True

        # delete
        if "message to delete not found" in text:
            return "message_to_delete_not_found", True

        if "message can't be deleted" in text:
            return "message_cant_be_deleted", True

        # send/edit common
        if "chat not found" in text:
            return "chat_not_found", True

        if "there is no text in the message to edit" in text:
            return "no_text_to_edit", True

        return "telegram_bad_request_unknown", False

    def _log_suppressed(
        self,
        *,
        action: str,
        reason: str,
        details: dict | None = None,
        exc: Exception | None = None,
    ) -> None:
        payload = details or {}
        if exc:
            logger.warning(
                "UI_POLICY | SUPPRESSED | action=%s | reason=%s | details=%s | error=%s",
                action,
                reason,
                payload,
                exc,
            )
        else:
            logger.warning(
                "UI_POLICY | SUPPRESSED | action=%s | reason=%s | details=%s",
                action,
                reason,
                payload,
            )

    def _log_error(
        self,
        *,
        action: str,
        details: dict | None = None,
        exc: Exception,
    ) -> None:
        payload = details or {}
        logger.exception(
            "UI_POLICY | ERROR | action=%s | details=%s | error=%s",
            action,
            payload,
            exc,
        )

    def _chat_cooldown_left(self, chat_id: int | str | None) -> float:
        if chat_id is None:
            return 0.0

        until = self._chat_retry_after_until.get(chat_id)
        if until is None:
            return 0.0

        left = until - monotonic()
        if left <= 0:
            self._chat_retry_after_until.pop(chat_id, None)
            return 0.0

        return left

    def _set_chat_retry_after(
        self,
        *,
        chat_id: int | str | None,
        retry_after: int | float | None,
    ) -> None:
        if chat_id is None or retry_after is None:
            return

        try:
            retry_after_seconds = float(retry_after)
        except (TypeError, ValueError):
            return

        if retry_after_seconds <= 0:
            return

        until = monotonic() + retry_after_seconds + 3
        current_until = self._chat_retry_after_until.get(chat_id, 0.0)
        if until > current_until:
            self._chat_retry_after_until[chat_id] = until

    def _build_message_signature(self, *, text: str, reply_markup) -> tuple[str, str]:
        return (text, repr(reply_markup))

    # =========================================================
    # CORE EXECUTOR
    # =========================================================

    async def _execute(
        self,
        *,
        action: str,
        func,
        details: dict | None = None,
        chat_id: int | str | None = None,
    ) -> UIActionResult:
        payload = details or {}
        cooldown_left = self._chat_cooldown_left(chat_id)
        if cooldown_left > 0:
            cooldown_details = {
                **payload,
                "cooldown_left": round(cooldown_left, 3),
            }
            self._log_suppressed(
                action=action,
                reason="chat_retry_after_active",
                details=cooldown_details,
            )
            return UIActionResult(
                ok=False,
                skipped=True,
                reason="chat_retry_after_active",
                result=None,
            )

        try:
            result = await func()
            return UIActionResult(ok=True, skipped=False, reason=None, result=result)

        except TelegramRetryAfter as exc:
            # Для UI не устраиваем длинных ожиданий.
            # Просто гасим и ставим cooldown на чат, чтобы не убивать UX-хендлер.
            retry_after = getattr(exc, "retry_after", None)
            self._set_chat_retry_after(chat_id=chat_id, retry_after=retry_after)
            retry_details = {
                **payload,
                "retry_after": retry_after,
                "safety_seconds": 3,
            }
            self._log_suppressed(
                action=action,
                reason="retry_after",
                details=retry_details,
                exc=exc,
            )
            return UIActionResult(ok=False, skipped=True, reason="retry_after", result=None)

        except TelegramForbiddenError as exc:
            self._log_suppressed(
                action=action,
                reason="forbidden",
                details=details,
                exc=exc,
            )
            return UIActionResult(ok=False, skipped=True, reason="forbidden", result=None)

        except TelegramBadRequest as exc:
            reason, suppress = self._classify_bad_request(exc)

            if suppress:
                self._log_suppressed(
                    action=action,
                    reason=reason,
                    details=details,
                    exc=exc,
                )
                return UIActionResult(ok=False, skipped=True, reason=reason, result=None)

            self._log_error(
                action=action,
                details=details,
                exc=exc,
            )
            return UIActionResult(ok=False, skipped=False, reason=reason, result=None)

        except Exception as exc:
            self._log_error(
                action=action,
                details=details,
                exc=exc,
            )
            return UIActionResult(ok=False, skipped=False, reason="unexpected_error", result=None)

    # =========================================================
    # CALLBACK
    # =========================================================

    async def answer_callback(
        self,
        callback,
        text: str | None = None,
        show_alert: bool = False,
    ) -> UIActionResult:
        callback_data = None
        try:
            callback_data = callback.data
        except Exception:
            callback_data = None

        return await self._execute(
            action="callback.answer",
            details={
                "callback_data": callback_data,
                "show_alert": show_alert,
                "text": text,
            },
            func=lambda: callback.answer(text=text, show_alert=show_alert),
        )

    # =========================================================
    # EDIT
    # =========================================================

    async def edit_text(
        self,
        *,
        chat_id: int | str,
        message_id: int,
        text: str,
        reply_markup=None,
        parse_mode: str | None = None,
        disable_web_page_preview: bool | None = None,
    ) -> UIActionResult:
        edit_key = (chat_id, message_id)
        details = {
            "chat_id": chat_id,
            "message_id": message_id,
        }
        cooldown_left = self._chat_cooldown_left(chat_id)
        if cooldown_left > 0:
            self._log_suppressed(
                action="bot.edit_message_text",
                reason="chat_retry_after_active",
                details={
                    **details,
                    "cooldown_left": round(cooldown_left, 3),
                },
            )
            return UIActionResult(
                ok=False,
                skipped=True,
                reason="chat_retry_after_active",
                result=None,
            )

        signature = self._build_message_signature(text=text, reply_markup=reply_markup)
        if self._last_edit_signature.get(edit_key) == signature:
            self._log_suppressed(
                action="bot.edit_message_text",
                reason="same_message_signature",
                details=details,
            )
            return UIActionResult(
                ok=False,
                skipped=True,
                reason="same_message_signature",
                result=None,
            )

        last_edit_at = self._last_edit_at.get(edit_key)
        now = monotonic()
        elapsed_since_last_edit = now - last_edit_at if last_edit_at is not None else None
        if (
            elapsed_since_last_edit is not None
            and elapsed_since_last_edit < self.MESSAGE_EDIT_THROTTLE_SECONDS
        ):
            self._log_suppressed(
                action="bot.edit_message_text",
                reason="message_edit_throttled",
                details={
                    **details,
                    "throttle_left": round(
                        self.MESSAGE_EDIT_THROTTLE_SECONDS - elapsed_since_last_edit, 3
                    ),
                },
            )
            return UIActionResult(
                ok=False,
                skipped=True,
                reason="message_edit_throttled",
                result=None,
            )

        result = await self._execute(
            action="bot.edit_message_text",
            details=details,
            func=lambda: self.bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=text,
                reply_markup=reply_markup,
                parse_mode=parse_mode,
                disable_web_page_preview=disable_web_page_preview,
            ),
            chat_id=chat_id,
        )
        if result.ok:
            self._last_edit_at[edit_key] = monotonic()
            self._last_edit_signature[edit_key] = signature
        return result

    async def edit_text_from_message(
        self,
        *,
        message,
        text: str,
        reply_markup=None,
        parse_mode: str | None = None,
        disable_web_page_preview: bool | None = None,
    ) -> UIActionResult:
        if not message:
            return UIActionResult(
                ok=False,
                skipped=True,
                reason="message_is_none",
                result=None,
            )

        chat_id = getattr(getattr(message, "chat", None), "id", None)
        message_id = getattr(message, "message_id", None)

        if chat_id is None or message_id is None:
            return UIActionResult(
                ok=False,
                skipped=True,
                reason="message_identifiers_missing",
                result=None,
            )

        return await self.edit_text(
            chat_id=chat_id,
            message_id=message_id,
            text=text,
            reply_markup=reply_markup,
            parse_mode=parse_mode,
            disable_web_page_preview=disable_web_page_preview,
        )

    # =========================================================
    # SEND
    # =========================================================

    async def send_message(
        self,
        *,
        chat_id: int | str,
        text: str,
        reply_markup=None,
        parse_mode: str | None = None,
        disable_web_page_preview: bool | None = None,
        message_thread_id: int | None = None,
    ) -> UIActionResult:
        return await self._execute(
            action="bot.send_message",
            details={
                "chat_id": chat_id,
                "message_thread_id": message_thread_id,
            },
            func=lambda: self.bot.send_message(
                chat_id=chat_id,
                text=text,
                reply_markup=reply_markup,
                parse_mode=parse_mode,
                disable_web_page_preview=disable_web_page_preview,
                message_thread_id=message_thread_id,
            ),
            chat_id=chat_id,
        )

    # =========================================================
    # DELETE
    # =========================================================

    async def delete_message(
        self,
        *,
        chat_id: int | str,
        message_id: int,
    ) -> UIActionResult:
        return await self._execute(
            action="bot.delete_message",
            details={
                "chat_id": chat_id,
                "message_id": message_id,
            },
            func=lambda: self.bot.delete_message(
                chat_id=chat_id,
                message_id=message_id,
            ),
            chat_id=chat_id,
        )

    async def delete_from_message(
        self,
        *,
        message,
    ) -> UIActionResult:
        if not message:
            return UIActionResult(
                ok=False,
                skipped=True,
                reason="message_is_none",
                result=None,
            )

        chat_id = getattr(getattr(message, "chat", None), "id", None)
        message_id = getattr(message, "message_id", None)

        if chat_id is None or message_id is None:
            return UIActionResult(
                ok=False,
                skipped=True,
                reason="message_identifiers_missing",
                result=None,
            )

        return await self.delete_message(
            chat_id=chat_id,
            message_id=message_id,
        )
