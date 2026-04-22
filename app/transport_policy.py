from __future__ import annotations

import asyncio
import logging
import random
import re
import time
from dataclasses import dataclass


logger = logging.getLogger("forwarder.transport")


@dataclass(slots=True)
class RetryDecision:
    should_retry: bool
    delay: float = 0.0
    reason: str = ""


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
    ) -> None:
        self.max_attempts = max(1, int(max_attempts))
        self.min_interval_sec = max(0.0, float(min_interval_sec))
        self.base_backoff_sec = max(0.1, float(base_backoff_sec))
        self.max_backoff_sec = max(self.base_backoff_sec, float(max_backoff_sec))
        self.jitter_sec = max(0.0, float(jitter_sec))
        self.retry_unknown_errors = bool(retry_unknown_errors)

        self._semaphore = asyncio.Semaphore(max(1, int(max_concurrency)))
        self._rate_lock = asyncio.Lock()
        self._last_call_by_key: dict[str, float] = {}

    async def execute(
        self,
        *,
        backend: str,
        key: str,
        op_name: str,
        func,
    ):
        last_error = None

        for attempt in range(1, self.max_attempts + 1):
            try:
                async with self._semaphore:
                    await self._wait_rate_slot(key)

                    started_at = time.monotonic()
                    result = await func()
                    elapsed = time.monotonic() - started_at

                    logger.debug(
                        "TRANSPORT | OK | backend=%s | op=%s | key=%s | attempt=%s | elapsed=%.3f",
                        backend,
                        op_name,
                        key,
                        attempt,
                        elapsed,
                    )
                    return result

            except Exception as exc:
                last_error = exc
                decision = self._classify_error(
                    backend=backend,
                    op_name=op_name,
                    attempt=attempt,
                    exc=exc,
                )

                logger.warning(
                    "TRANSPORT | ERROR | backend=%s | op=%s | key=%s | attempt=%s/%s | retry=%s | delay=%.2f | reason=%s | error=%s",
                    backend,
                    op_name,
                    key,
                    attempt,
                    self.max_attempts,
                    decision.should_retry,
                    decision.delay,
                    decision.reason,
                    exc,
                )

                if not decision.should_retry or attempt >= self.max_attempts:
                    raise

                await asyncio.sleep(decision.delay)

        if last_error:
            raise last_error

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
        attempt: int,
        exc: Exception,
    ) -> RetryDecision:
        text = f"{exc}".lower()
        class_name = exc.__class__.__name__.lower()

        flood_seconds = self._extract_wait_seconds(exc, text)
        if flood_seconds is not None:
            delay = min(max(1.0, float(flood_seconds) + 0.5), self.max_backoff_sec)
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
