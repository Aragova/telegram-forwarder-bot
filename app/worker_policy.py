from __future__ import annotations

import logging
from dataclasses import dataclass


logger = logging.getLogger("forwarder.worker")


@dataclass(slots=True)
class WorkerLoopDecision:
    sleep_seconds: float
    reason: str
    level: str = "info"


class WorkerPolicy:
    """
    Единая политика цикла rule_worker.

    Здесь централизуем:
    - паузы после успешной обработки
    - паузы при пустой очереди
    - паузы при рабочих ошибках
    - паузы при критических сбоях воркера
    """

    def __init__(
        self,
        *,
        idle_sleep_sec: float = 2.0,
        success_sleep_sec: float = 0.15,
        handled_error_sleep_sec: float = 3.0,
        crash_sleep_sec: float = 5.0,
        inactive_rule_sleep_sec: float = 5.0,
    ) -> None:
        self.idle_sleep_sec = max(0.05, float(idle_sleep_sec))
        self.success_sleep_sec = max(0.0, float(success_sleep_sec))
        self.handled_error_sleep_sec = max(0.5, float(handled_error_sleep_sec))
        self.crash_sleep_sec = max(1.0, float(crash_sleep_sec))
        self.inactive_rule_sleep_sec = max(1.0, float(inactive_rule_sleep_sec))

    def on_rule_missing(self) -> WorkerLoopDecision:
        return WorkerLoopDecision(
            sleep_seconds=self.inactive_rule_sleep_sec,
            reason="rule_missing",
            level="warning",
        )

    def on_rule_inactive(self) -> WorkerLoopDecision:
        return WorkerLoopDecision(
            sleep_seconds=self.inactive_rule_sleep_sec,
            reason="rule_inactive",
            level="debug",
        )

    def on_nothing_processed(self) -> WorkerLoopDecision:
        return WorkerLoopDecision(
            sleep_seconds=self.idle_sleep_sec,
            reason="idle_no_due_delivery",
            level="debug",
        )

    def on_processed_success(self) -> WorkerLoopDecision:
        return WorkerLoopDecision(
            sleep_seconds=self.success_sleep_sec,
            reason="processed_success",
            level="debug",
        )

    def on_processed_failure(self) -> WorkerLoopDecision:
        return WorkerLoopDecision(
            sleep_seconds=self.handled_error_sleep_sec,
            reason="processed_failure",
            level="warning",
        )

    def on_worker_crash(self) -> WorkerLoopDecision:
        return WorkerLoopDecision(
            sleep_seconds=self.crash_sleep_sec,
            reason="worker_crash",
            level="error",
        )

    def log_decision(
        self,
        *,
        rule_id: int,
        decision: WorkerLoopDecision,
        extra: str | None = None,
    ) -> None:
        text = (
            f"WORKER_POLICY | rule_id={rule_id} | reason={decision.reason} "
            f"| sleep={decision.sleep_seconds:.2f}s"
        )
        if extra:
            text += f" | {extra}"

        if decision.level == "debug":
            logger.debug(text)
        elif decision.level == "warning":
            logger.warning(text)
        elif decision.level == "error":
            logger.error(text)
        else:
            logger.info(text)


def build_worker_policy() -> WorkerPolicy:
    return WorkerPolicy(
        idle_sleep_sec=2.0,
        success_sleep_sec=0.15,
        handled_error_sleep_sec=3.0,
        crash_sleep_sec=5.0,
        inactive_rule_sleep_sec=5.0,
    )
