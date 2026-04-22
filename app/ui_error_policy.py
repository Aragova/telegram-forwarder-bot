from __future__ import annotations

import logging
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

    def __init__(self, bot) -> None:
        self.bot = bot

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

    # =========================================================
    # CORE EXECUTOR
    # =========================================================

    async def _execute(
        self,
        *,
        action: str,
        func,
        details: dict | None = None,
    ) -> UIActionResult:
        try:
            result = await func()
            return UIActionResult(ok=True, skipped=False, reason=None, result=result)

        except TelegramRetryAfter as exc:
            # Для UI не устраиваем длинных ожиданий.
            # Просто гасим, чтобы не убивать UX-хендлер.
            self._log_suppressed(
                action=action,
                reason="retry_after",
                details=details,
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
        return await self._execute(
            action="bot.edit_message_text",
            details={
                "chat_id": chat_id,
                "message_id": message_id,
            },
            func=lambda: self.bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=text,
                reply_markup=reply_markup,
                parse_mode=parse_mode,
                disable_web_page_preview=disable_web_page_preview,
            ),
        )

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
