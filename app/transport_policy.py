from __future__ import annotations

import asyncio
import logging
import random
import re
import time
from dataclasses import dataclass

from app.transport_operation import TransportOperationKind, classify_transport_operation


logger = logging.getLogger("forwarder.transport")


def _transport_label_from_key(key: str, op_name: str) -> str:
    suffix = f".{op_name}"
    if key.endswith(suffix):
        return key[: -len(suffix)] or key
    return key


def _safe_error_text(exc: Exception, *, max_len: int = 160) -> str:
    text = str(exc).replace("\n", " ").replace("\r", " ")
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) > max_len:
        return f"{text[: max_len - 3]}..."
    return text


def _format_transport_log_context(**fields) -> str:
    return " | ".join(f"{name}={value}" for name, value in fields.items() if value is not None)


@dataclass(slots=True)
class RetryDecision:
    should_retry: bool
    delay: float = 0.0
    reason: str = ""


class TransportRateLimited(Exception):
    def __init__(
        self,
        *,
        retry_after_seconds: int,
        backend: str,
        op_name: str,
        key: str,
    ) -> None:
        self.retry_after_seconds = int(retry_after_seconds)
        self.backend = backend
        self.op_name = op_name
        self.key = key
        super().__init__(
            f"transport rate limited: backend={backend} op={op_name} "
            f"key={key} retry_after={self.retry_after_seconds}"
        )


class TransportPolicy:
    """
    Единая политика транспорта для КОНКРЕТНОГО клиента,
    а не глобальная singleton-помойка на весь проект.
    """

    def __init__(
        self,
        *,
        max_attempts: int = 2,
        min_interval_sec: float = 0.0,
        max_concurrency: int = 16,
        base_backoff_sec: float = 0.8,
        max_backoff_sec: float = 8.0,
        jitter_sec: float = 0.25,
        retry_unknown_errors: bool = False,
        long_retry_after_threshold_sec: int = 30,
        retry_non_idempotent_writes: bool = True,
        retry_unknown_operations: bool = True,
    ) -> None:
        self.max_attempts = max(1, int(max_attempts))
        self.min_interval_sec = max(0.0, float(min_interval_sec))
        self.base_backoff_sec = max(0.1, float(base_backoff_sec))
        self.max_backoff_sec = max(self.base_backoff_sec, float(max_backoff_sec))
        self.jitter_sec = max(0.0, float(jitter_sec))
        self.retry_unknown_errors = bool(retry_unknown_errors)
        self.long_retry_after_threshold_sec = max(1, int(long_retry_after_threshold_sec))
        self.retry_non_idempotent_writes = bool(retry_non_idempotent_writes)
        self.retry_unknown_operations = bool(retry_unknown_operations)

        self._semaphore = asyncio.Semaphore(max(1, int(max_concurrency)))
        self._rate_lock = asyncio.Lock()
        self._last_call_by_key: dict[str, float] = {}

    def classify_operation(
        self,
        *,
        backend: str,
        op_name: str,
        explicit_kind: TransportOperationKind | str | None = None,
    ) -> TransportOperationKind:
        return classify_transport_operation(
            backend=backend,
            op_name=op_name,
            explicit_kind=explicit_kind,
        )

    async def execute(
        self,
        *,
        backend: str,
        key: str,
        op_name: str,
        func,
        operation_kind: TransportOperationKind | str | None = None,
    ):
        _operation_kind = self.classify_operation(
            backend=backend,
            op_name=op_name,
            explicit_kind=operation_kind,
        )
        label = _transport_label_from_key(key, op_name)
        last_error = None

        for attempt in range(1, self.max_attempts + 1):
            attempt_started_at = time.monotonic()
            try:
                async with self._semaphore:
                    await self._wait_rate_slot(key)

                    result = await func()
                    elapsed_ms = int((time.monotonic() - attempt_started_at) * 1000)

                    logger.debug(
                        "TRANSPORT | CALL_OK | %s",
                        _format_transport_log_context(
                            backend=backend,
                            label=label,
                            op=op_name,
                            operation_kind=_operation_kind.value,
                            attempt=attempt,
                            max_attempts=self.max_attempts,
                            elapsed_ms=elapsed_ms,
                            decision="success",
                        ),
                    )
                    return result

            except TransportRateLimited:
                raise
            except Exception as exc:
                last_error = exc
                elapsed_ms = int((time.monotonic() - attempt_started_at) * 1000)
                decision = self._classify_error(
                    backend=backend,
                    label=label,
                    op_name=op_name,
                    key=key,
                    operation_kind=_operation_kind,
                    attempt=attempt,
                    elapsed_ms=elapsed_ms,
                    exc=exc,
                )

                decision = self.should_retry_operation(
                    operation_kind=_operation_kind,
                    decision=decision,
                )

                decision_name = self._failure_decision_name(
                    operation_kind=_operation_kind,
                    decision=decision,
                    attempt=attempt,
                )
                event = self._failure_event_name(decision_name=decision_name, decision=decision, attempt=attempt)

                logger.warning(
                    "TRANSPORT | %s | %s",
                    event,
                    _format_transport_log_context(
                        backend=backend,
                        label=label,
                        op=op_name,
                        operation_kind=_operation_kind.value,
                        attempt=attempt,
                        max_attempts=self.max_attempts,
                        elapsed_ms=elapsed_ms,
                        error_type=exc.__class__.__name__,
                        error_text=_safe_error_text(exc),
                        retryable=decision.should_retry,
                        retry_after=self._extract_wait_seconds(exc, str(exc).lower()),
                        sleep_seconds=f"{decision.delay:.2f}" if decision.should_retry else None,
                        decision=decision_name,
                    ),
                )

                if not decision.should_retry or attempt >= self.max_attempts:
                    raise

                await asyncio.sleep(decision.delay)

        if last_error:
            raise last_error

    def should_retry_operation(
        self,
        *,
        operation_kind: TransportOperationKind,
        decision: RetryDecision,
    ) -> RetryDecision:
        if not decision.should_retry:
            return decision

        if (
            operation_kind == TransportOperationKind.NON_IDEMPOTENT_WRITE
            and not self.retry_non_idempotent_writes
        ):
            return RetryDecision(
                should_retry=False,
                delay=0.0,
                reason=f"{decision.reason}:non_idempotent_write_auto_retry_disabled",
            )

        if operation_kind == TransportOperationKind.UNKNOWN and not self.retry_unknown_operations:
            return RetryDecision(
                should_retry=False,
                delay=0.0,
                reason=f"{decision.reason}:unknown_operation_auto_retry_disabled",
            )

        return decision

    async def _wait_rate_slot(self, key: str) -> None:
        if self.min_interval_sec <= 0:
            return

        async with self._rate_lock:
            now = time.monotonic()
            last_ts = self._last_call_by_key.get(key, 0.0)
            wait_for = self.min_interval_sec - (now - last_ts)

            if wait_for > 0:
                await asyncio.sleep(wait_for)

            self._last_call_by_key[key] = time.monotonic()

    def _classify_error(
        self,
        *,
        backend: str,
        op_name: str,
        key: str,
        attempt: int,
        exc: Exception,
        label: str | None = None,
        operation_kind: TransportOperationKind | None = None,
        elapsed_ms: int = 0,
    ) -> RetryDecision:
        label = label or _transport_label_from_key(key, op_name)
        operation_kind = operation_kind or self.classify_operation(backend=backend, op_name=op_name)
        text = f"{exc}".lower()
        class_name = exc.__class__.__name__.lower()

        flood_seconds = self._extract_wait_seconds(exc, text)
        if flood_seconds is not None:
            if flood_seconds > self.long_retry_after_threshold_sec:
                logger.warning(
                    "TRANSPORT | RATE_LIMITED | %s",
                    _format_transport_log_context(
                        backend=backend,
                        label=label,
                        op=op_name,
                        operation_kind=operation_kind.value,
                        attempt=attempt,
                        max_attempts=self.max_attempts,
                        elapsed_ms=elapsed_ms,
                        retry_after=flood_seconds,
                        threshold=self.long_retry_after_threshold_sec,
                        decision="rate_limited",
                        error_type=exc.__class__.__name__,
                        error_text=_safe_error_text(exc),
                    ),
                )
                raise TransportRateLimited(
                    retry_after_seconds=flood_seconds,
                    backend=backend,
                    op_name=op_name,
                    key=key,
                ) from exc

            delay = max(1.0, float(flood_seconds) + 0.5)
            return RetryDecision(
                should_retry=True,
                delay=delay,
                reason="flood_wait_or_retry_after",
            )

        transient_markers = (
            "timeout",
            "timed out",
            "connection reset",
            "connection aborted",
            "connection refused",
            "server disconnected",
            "network is unreachable",
            "temporary failure",
            "temporarily unavailable",
            "bad gateway",
            "gateway timeout",
            "service unavailable",
            "internal server error",
            "too many requests",
            "retry later",
            "429",
        )

        transient_classes = (
            "timeouterror",
            "clientconnectorerror",
            "serverdisconnectederror",
            "networkerror",
        )

        if any(marker in text for marker in transient_markers) or class_name in transient_classes:
            return RetryDecision(
                should_retry=True,
                delay=self._exp_backoff(attempt),
                reason="transient_error",
            )

        non_retry_markers = (
            "message_id_invalid",
            "message is not modified",
            "chat not found",
            "message to edit not found",
            "message can't be edited",
            "message can't be deleted",
            "entity not found",
            "peer id invalid",
            "username not occupied",
            "have no rights",
            "forbidden",
            "can't parse entities",
            "wrong file identifier",
            "file reference expired",
        )

        if any(marker in text for marker in non_retry_markers):
            return RetryDecision(
                should_retry=False,
                delay=0.0,
                reason="non_retry_business_error",
            )

        if self.retry_unknown_errors and attempt == 1:
            return RetryDecision(
                should_retry=True,
                delay=self._exp_backoff(attempt),
                reason="unknown_first_retry",
            )

        return RetryDecision(
            should_retry=False,
            delay=0.0,
            reason="unknown_non_retry",
        )

    def _failure_decision_name(
        self,
        *,
        operation_kind: TransportOperationKind,
        decision: RetryDecision,
        attempt: int,
    ) -> str:
        if decision.should_retry and attempt < self.max_attempts:
            return "retry"
        if decision.should_retry and attempt >= self.max_attempts:
            return "max_attempts_exceeded"
        if (
            operation_kind == TransportOperationKind.NON_IDEMPOTENT_WRITE
            and "non_idempotent_write_auto_retry_disabled" in decision.reason
        ):
            return "no_retry_non_idempotent_write"
        if (
            operation_kind == TransportOperationKind.UNKNOWN
            and "unknown_operation_auto_retry_disabled" in decision.reason
        ):
            return "no_retry_unknown_operation"
        if decision.reason == "non_retry_business_error":
            return "non_retryable_error"
        return "raise"

    def _failure_event_name(self, *, decision_name: str, decision: RetryDecision, attempt: int) -> str:
        if decision_name == "retry" and decision.should_retry and attempt < self.max_attempts:
            return "CALL_RETRY"
        if decision_name.startswith("no_retry_"):
            return "RETRY_SKIPPED"
        if decision_name == "non_retryable_error":
            return "CALL_FAILED_NON_RETRYABLE"
        return "CALL_FAILED"

    def _exp_backoff(self, attempt: int) -> float:
        delay = min(
            self.base_backoff_sec * (2 ** max(0, attempt - 1)),
            self.max_backoff_sec,
        )
        if self.jitter_sec > 0:
            delay += random.uniform(0.0, self.jitter_sec)
        return delay

    def _extract_wait_seconds(self, exc: Exception, text: str) -> int | None:
        for attr_name in ("seconds", "retry_after", "timeout"):
            value = getattr(exc, attr_name, None)
            if isinstance(value, (int, float)) and value > 0:
                return int(value)

        patterns = [
            r"flood wait.*?(\d+)",
            r"retry after.*?(\d+)",
            r"too many requests.*?(\d+)",
            r"wait of (\d+)",
        ]

        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                try:
                    return int(match.group(1))
                except Exception:
                    pass

        return None


def build_sender_bot_policy() -> TransportPolicy:
    return TransportPolicy(
        max_attempts=2,
        min_interval_sec=0.0,
        max_concurrency=24,
        base_backoff_sec=0.7,
        max_backoff_sec=5.0,
        jitter_sec=0.2,
        retry_unknown_errors=False,
        retry_non_idempotent_writes=False,
        retry_unknown_operations=False,
    )


def build_sender_telethon_policy() -> TransportPolicy:
    return TransportPolicy(
        max_attempts=2,
        min_interval_sec=0.0,
        max_concurrency=1,
        base_backoff_sec=0.8,
        max_backoff_sec=8.0,
        jitter_sec=0.25,
        retry_unknown_errors=False,
        retry_non_idempotent_writes=False,
        retry_unknown_operations=False,
    )


def build_reaction_policy() -> TransportPolicy:
    return TransportPolicy(
        max_attempts=1,
        min_interval_sec=0.0,
        max_concurrency=32,
        base_backoff_sec=0.0,
        max_backoff_sec=0.0,
        jitter_sec=0.0,
        retry_unknown_errors=False,
    )


def build_video_bot_policy() -> TransportPolicy:
    return TransportPolicy(
        max_attempts=2,
        min_interval_sec=0.0,
        max_concurrency=8,
        base_backoff_sec=0.8,
        max_backoff_sec=6.0,
        jitter_sec=0.2,
        retry_unknown_errors=False,
    )


def build_video_telethon_policy() -> TransportPolicy:
    return TransportPolicy(
        max_attempts=2,
        min_interval_sec=0.0,
        max_concurrency=1,
        base_backoff_sec=0.8,
        max_backoff_sec=8.0,
        jitter_sec=0.2,
        retry_unknown_errors=False,
    )
