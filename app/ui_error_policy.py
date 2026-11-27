from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Any

from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError, TelegramRetryAfter

from app.background_tasks import start_background_task
from app.telegram_flood_locks import TelegramFloodLockStore
from app.ui_navigation import edit_locks, message_key


logger = logging.getLogger("forwarder.ui")

RETRY_AFTER_SAFETY_SECONDS = 3


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
    EDIT_WAIT_BUDGET_SECONDS = 5.0
    ALERT_MAX_CHATS = 256
    ALERT_MAX_PER_CHAT = 16
    ALERT_SEND_TIMEOUT_SECONDS = 15.0

    def __init__(
        self, bot, flood_lock_store: TelegramFloodLockStore | None = None
    ) -> None:
        self.bot = bot
        self._flood_lock_store = flood_lock_store or TelegramFloodLockStore()
        self._chat_retry_after_until: dict[str, float] = {}
        self._last_edit_at: dict[tuple[int | str, int], float] = {}
        self._last_edit_signature: dict[tuple[int | str, int], tuple[Any, ...]] = {}
        self._pending_alerts: dict[str, dict[str, None]] = {}
        self._alert_tasks: dict[str, asyncio.Task] = {}
        self._load_persistent_chat_locks()

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

    def _chat_key(self, chat_id: int | str | None) -> str | None:
        if chat_id is None:
            return None
        return str(chat_id)

    def _load_persistent_chat_locks(self) -> None:
        try:
            locks = self._flood_lock_store.load_active_locks(now_epoch=time.time())
        except Exception as exc:
            logger.warning(
                "UI_FLOOD_LOCKS | STORAGE_WARNING | path=%s | error=%s",
                getattr(self._flood_lock_store, "path", "unknown"),
                exc,
            )
            return

        now_epoch = time.time()
        now_monotonic = time.monotonic()
        for chat_id, lock in locks.items():
            remaining = lock.until_epoch - now_epoch
            if remaining <= 0:
                continue
            self._chat_retry_after_until[str(chat_id)] = now_monotonic + remaining
            logger.info(
                "UI_FLOOD_LOCKS | RESTORED | chat_id=%s | remaining=%s",
                chat_id,
                round(remaining, 3),
            )

    def chat_cooldown_left(self, chat_id: int | str | None) -> float:
        """Read the remaining chat flood lock without sending a Telegram request."""
        return self._chat_cooldown_left(chat_id)

    def _chat_cooldown_left(self, chat_id: int | str | None) -> float:
        key = self._chat_key(chat_id)
        if key is None:
            return 0.0

        until = self._chat_retry_after_until.get(key)
        if until is not None:
            left = until - time.monotonic()
            if left > 0:
                return left
            self._chat_retry_after_until.pop(key, None)

        try:
            remaining = self._flood_lock_store.get_remaining_seconds(key)
        except Exception as exc:
            logger.warning(
                "UI_FLOOD_LOCKS | STORAGE_WARNING | path=%s | error=%s",
                getattr(self._flood_lock_store, "path", "unknown"),
                exc,
            )
            return 0.0

        if remaining <= 0:
            return 0.0

        self._chat_retry_after_until[key] = time.monotonic() + remaining
        logger.info(
            "UI_FLOOD_LOCKS | RESTORED | chat_id=%s | remaining=%s",
            key,
            round(remaining, 3),
        )
        return remaining

    def _set_chat_retry_after(
        self,
        *,
        chat_id: int | str | None,
        retry_after: int | float | None,
        action: str,
    ) -> None:
        key = self._chat_key(chat_id)
        if key is None or retry_after is None:
            return

        try:
            retry_after_seconds = float(retry_after)
        except (TypeError, ValueError):
            return

        if retry_after_seconds <= 0:
            return

        until = time.monotonic() + retry_after_seconds + RETRY_AFTER_SAFETY_SECONDS
        current_until = self._chat_retry_after_until.get(key, 0.0)
        if until > current_until:
            self._chat_retry_after_until[key] = until

        try:
            self._flood_lock_store.set_lock(
                key,
                retry_after_seconds=retry_after_seconds,
                method=action,
                safety_seconds=RETRY_AFTER_SAFETY_SECONDS,
            )
            self._flood_lock_store.clear_expired()
        except Exception as exc:
            logger.warning(
                "UI_FLOOD_LOCKS | STORAGE_WARNING | path=%s | error=%s",
                getattr(self._flood_lock_store, "path", "unknown"),
                exc,
            )

    def invalidate_edit_cache(self, *, chat_id: int | str, message_id: int) -> None:
        """Clear stale edit state after an externally executed Rich edit.

        Without this, the next navigation to a previously seen screen can be
        dropped as same_message_signature, even though the message changed.
        """
        key = message_key(chat_id, message_id)
        self._last_edit_signature.pop(key, None)
        self._last_edit_at.pop(key, None)

    def _without_none(self, values: dict[str, Any]) -> dict[str, Any]:
        return {key: value for key, value in values.items() if value is not None}

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
            self._set_chat_retry_after(
                chat_id=chat_id, retry_after=retry_after, action=action
            )
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

    async def edit_operation(
        self, *, chat_id: int | str, message_id: int, action: str,
        signature: tuple[Any, ...] | None, func,
    ) -> UIActionResult:
        """Serialize all screen writes, wait for short rate limits, never report
        a skipped distinct screen as already rendered.
        """
        key = message_key(chat_id, message_id)
        details = {"chat_id": chat_id, "message_id": message_id}
        async with edit_locks.get(chat_id, message_id):
            if signature is not None and self._last_edit_signature.get(key) == signature:
                return UIActionResult(ok=False, skipped=True, reason="same_message_signature")

            deadline = time.monotonic() + self.EDIT_WAIT_BUDGET_SECONDS
            previous = None
            for attempt in range(2):
                last_edit = self._last_edit_at.get(key)
                throttle = (
                    self.MESSAGE_EDIT_THROTTLE_SECONDS - (time.monotonic() - last_edit)
                    if last_edit is not None else 0.0
                )
                cooldown = self._chat_cooldown_left(chat_id)
                delay = max(0.0, throttle, cooldown)
                if delay > max(0.0, deadline - time.monotonic()):
                    reason = "chat_retry_after_active" if cooldown > 0 else "message_edit_throttled"
                    self._log_suppressed(action=action, reason=reason, details=details)
                    return previous or UIActionResult(ok=False, skipped=True, reason=reason)
                if delay > 0:
                    await asyncio.sleep(delay)

                # A request with unknown outcome cannot leave a previous screen
                # marked as confirmed. Only success / not-modified restores it.
                self._last_edit_signature.pop(key, None)
                result = await self._execute(action=action, details=details, func=func, chat_id=chat_id)
                if result.ok and result.result is False:
                    return UIActionResult(ok=False, reason="edit_rejected")
                if result.ok or result.reason == "message_not_modified":
                    self._last_edit_at[key] = time.monotonic()
                    if signature is not None:
                        self._last_edit_signature[key] = signature
                    return result
                if attempt == 0 and result.reason in {"retry_after", "chat_retry_after_active"}:
                    previous = result
                    continue
                return result
            return previous or UIActionResult(ok=False, reason="edit_rejected")

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
        return await self.edit_operation(
            chat_id=chat_id, message_id=message_id, action="bot.edit_message_text",
            signature=("text", text, repr(reply_markup), parse_mode, disable_web_page_preview),
            func=lambda: self.bot.edit_message_text(
                chat_id=chat_id, message_id=message_id, text=text,
                reply_markup=reply_markup, parse_mode=parse_mode,
                disable_web_page_preview=disable_web_page_preview,
            ),
        )

    async def edit_message_reply_markup(
        self, *, chat_id: int | str, message_id: int, reply_markup=None,
    ) -> UIActionResult:
        return await self.edit_operation(
            chat_id=chat_id, message_id=message_id, action="bot.edit_message_reply_markup",
            signature=None,
            func=lambda: self.bot.edit_message_reply_markup(
                chat_id=chat_id, message_id=message_id, reply_markup=reply_markup,
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

    def notify_after_ack(self, *, chat_id: int | str, text: str) -> bool:
        """Accept a private-chat alert for delivery without blocking its handler.

        One tracked task per chat; duplicate pending texts coalesce. Flood
        cooldowns are rechecked on every wake and every send. No delivery
        promise is made across process shutdown.
        """
        key = str(chat_id)
        pending = self._pending_alerts.get(key)
        if pending is None:
            if len(self._pending_alerts) >= self.ALERT_MAX_CHATS:
                logger.warning("UI_ALERT_CAPACITY | chat_id=%s", key)
                return False
            pending = {}
            self._pending_alerts[key] = pending
        if text in pending:
            return True
        if len(pending) >= self.ALERT_MAX_PER_CHAT:
            logger.warning("UI_ALERT_CHAT_CAPACITY | chat_id=%s", key)
            return False
        pending[text] = None
        if key not in self._alert_tasks:
            task = start_background_task(self._deliver_alerts(chat_id, key), name="ui-deferred-alert")
            self._alert_tasks[key] = task
            task.add_done_callback(lambda done: self._finish_alert_task(key, done))
        return True

    def _finish_alert_task(self, key: str, task: asyncio.Task) -> None:
        if self._alert_tasks.get(key) is task:
            self._alert_tasks.pop(key, None)
            self._pending_alerts.pop(key, None)

    async def _deliver_alerts(self, chat_id: int | str, key: str) -> None:
        pending = self._pending_alerts[key]
        try:
            while pending:
                cooldown = self._chat_cooldown_left(chat_id)
                if cooldown > 0:
                    await asyncio.sleep(cooldown)
                    continue
                text = next(iter(pending))
                try:
                    async with asyncio.timeout(self.ALERT_SEND_TIMEOUT_SECONDS):
                        result = await self.send_message(chat_id=chat_id, text=text)
                except TimeoutError:
                    # Unknown send outcome: do not create a duplicate warning.
                    logger.warning("UI_ALERT_TIMEOUT | chat_id=%s", key)
                    pending.pop(text, None)
                    continue
                if result.reason in {"retry_after", "chat_retry_after_active"}:
                    # Retry only when a positive cooldown was actually recorded.
                    if self._chat_cooldown_left(chat_id) > 0:
                        continue
                if not result.ok:
                    logger.warning("UI_ALERT_UNDELIVERED | chat_id=%s | reason=%s", key, result.reason)
                pending.pop(text, None)
        finally:
            # Remove synchronously before returning, so a new alert cannot be
            # appended to a finished task's queue before its done callback.
            current = asyncio.current_task()
            if current is not None:
                self._finish_alert_task(key, current)

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

    async def send_photo(
        self,
        *,
        chat_id: int | str,
        photo,
        caption: str | None = None,
        reply_markup=None,
        parse_mode: str | None = None,
        message_thread_id: int | None = None,
    ) -> UIActionResult:
        kwargs = self._without_none(
            {
                "chat_id": chat_id,
                "photo": photo,
                "caption": caption,
                "reply_markup": reply_markup,
                "parse_mode": parse_mode,
                "message_thread_id": message_thread_id,
            }
        )
        return await self._execute(
            action="bot.send_photo",
            details={
                "chat_id": chat_id,
                "message_thread_id": message_thread_id,
            },
            func=lambda: self.bot.send_photo(**kwargs),
            chat_id=chat_id,
        )

    async def send_video(
        self,
        *,
        chat_id: int | str,
        video,
        caption: str | None = None,
        reply_markup=None,
        parse_mode: str | None = None,
        supports_streaming: bool | None = None,
        duration: int | None = None,
        width: int | None = None,
        height: int | None = None,
        thumbnail=None,
        message_thread_id: int | None = None,
    ) -> UIActionResult:
        kwargs = self._without_none(
            {
                "chat_id": chat_id,
                "video": video,
                "caption": caption,
                "reply_markup": reply_markup,
                "parse_mode": parse_mode,
                "supports_streaming": supports_streaming,
                "duration": duration,
                "width": width,
                "height": height,
                "thumbnail": thumbnail,
                "message_thread_id": message_thread_id,
            }
        )
        return await self._execute(
            action="bot.send_video",
            details={
                "chat_id": chat_id,
                "message_thread_id": message_thread_id,
            },
            func=lambda: self.bot.send_video(**kwargs),
            chat_id=chat_id,
        )

    async def send_document(
        self,
        *,
        chat_id: int | str,
        document,
        caption: str | None = None,
        reply_markup=None,
        parse_mode: str | None = None,
        message_thread_id: int | None = None,
    ) -> UIActionResult:
        kwargs = self._without_none(
            {
                "chat_id": chat_id,
                "document": document,
                "caption": caption,
                "reply_markup": reply_markup,
                "parse_mode": parse_mode,
                "message_thread_id": message_thread_id,
            }
        )
        return await self._execute(
            action="bot.send_document",
            details={
                "chat_id": chat_id,
                "message_thread_id": message_thread_id,
            },
            func=lambda: self.bot.send_document(**kwargs),
            chat_id=chat_id,
        )

    async def copy_message(
        self,
        *,
        chat_id: int | str,
        from_chat_id: int | str,
        message_id: int,
        caption: str | None = None,
        reply_markup=None,
        parse_mode: str | None = None,
        message_thread_id: int | None = None,
    ) -> UIActionResult:
        kwargs = self._without_none(
            {
                "chat_id": chat_id,
                "from_chat_id": from_chat_id,
                "message_id": message_id,
                "caption": caption,
                "reply_markup": reply_markup,
                "parse_mode": parse_mode,
                "message_thread_id": message_thread_id,
            }
        )
        return await self._execute(
            action="bot.copy_message",
            details={
                "chat_id": chat_id,
                "from_chat_id": from_chat_id,
                "message_id": message_id,
                "message_thread_id": message_thread_id,
            },
            func=lambda: self.bot.copy_message(**kwargs),
            chat_id=chat_id,
        )

    async def forward_message(
        self,
        *,
        chat_id: int | str,
        from_chat_id: int | str,
        message_id: int,
        message_thread_id: int | None = None,
    ) -> UIActionResult:
        kwargs = self._without_none(
            {
                "chat_id": chat_id,
                "from_chat_id": from_chat_id,
                "message_id": message_id,
                "message_thread_id": message_thread_id,
            }
        )
        return await self._execute(
            action="bot.forward_message",
            details={
                "chat_id": chat_id,
                "from_chat_id": from_chat_id,
                "message_id": message_id,
                "message_thread_id": message_thread_id,
            },
            func=lambda: self.bot.forward_message(**kwargs),
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
        async with edit_locks.get(chat_id, message_id):
            result = await self._execute(
                action="bot.delete_message",
                details={"chat_id": chat_id, "message_id": message_id},
                func=lambda: self.bot.delete_message(chat_id=chat_id, message_id=message_id),
                chat_id=chat_id,
            )
            self.invalidate_edit_cache(chat_id=chat_id, message_id=message_id)
            return result

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
