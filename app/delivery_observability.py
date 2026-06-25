"""Runtime-neutral delivery diagnostics foundation for Sender Architecture v2."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from inspect import isawaitable
from typing import Any


class DeliveryHealthStatus(str, Enum):
    OK = "ok"
    WARNING = "warning"
    CRITICAL = "critical"
    UNKNOWN = "unknown"


class DeliveryDiagnosticSignal(str, Enum):
    STUCK_PROCESSING = "stuck_processing"
    QUEUE_LAG = "queue_lag"
    FAULTY_DELIVERIES = "faulty_deliveries"
    RATE_LIMITED = "rate_limited"
    EMPTY_QUEUE = "empty_queue"
    NO_DATA = "no_data"


@dataclass(frozen=True, slots=True)
class DeliveryObservabilityConfig:
    stale_processing_after_seconds: int = 900
    queue_lag_warning_seconds: int = 600
    queue_lag_critical_seconds: int = 1800
    faulty_warning_count: int = 1
    faulty_critical_count: int = 10
    rate_limited_warning_count: int = 1
    max_problem_rules: int = 10


_COUNT_FIELDS = (
    "pending_count",
    "processing_count",
    "sent_count",
    "faulty_count",
    "deferred_count",
    "rate_limited_count",
)
_AGE_FIELDS = (
    "oldest_pending_age_seconds",
    "oldest_processing_age_seconds",
    "next_run_in_seconds",
)
_KNOWN_FIELDS = (
    "rule_id",
    "source_id",
    "target_id",
    *_COUNT_FIELDS,
    *_AGE_FIELDS,
    "last_error_type",
    "last_error_text",
)


def sanitize_diagnostic_text(value: object | None, *, max_length: int = 500) -> str | None:
    if value is None:
        return None
    text = str(value)
    for secret in ("SECRET_TOKEN", "PRIVATE_TOKEN"):
        text = text.replace(secret, "<redacted>")
    sanitized_chars = []
    for char in text:
        sanitized_chars.append(" " if ord(char) < 32 or char == "\x7f" else char)
    text = "".join(sanitized_chars)
    parts = []
    for part in text.split(" "):
        if _looks_like_bot_token(part):
            parts.append("<redacted>")
        else:
            parts.append(part)
    text = " ".join(parts)
    if max_length < 0:
        max_length = 0
    if len(text) > max_length:
        text = text[:max_length]
    return text


def _looks_like_bot_token(value: str) -> bool:
    token = value.strip(".,;:()[]{}<>\"'")
    if ":" not in token:
        return False
    left, right = token.split(":", 1)
    return left.isdigit() and len(left) >= 5 and len(right) >= 20


def _safe_count(value: object) -> int:
    if value is None:
        return 0
    try:
        count = int(float(value)) if isinstance(value, str) else int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError, OverflowError):
        return 0
    return max(count, 0)


def _safe_age(value: object) -> int | float | None:
    if value is None:
        return None
    try:
        age = float(value) if isinstance(value, str) else value
        if not isinstance(age, (int, float)):
            return None
        if age < 0:
            return None
        return int(age) if float(age).is_integer() else float(age)
    except (TypeError, ValueError, OverflowError):
        return None


@dataclass(frozen=True, slots=True)
class DeliveryRuleMetrics:
    rule_id: int | str | None
    source_id: int | str | None = None
    target_id: int | str | None = None

    pending_count: int = 0
    processing_count: int = 0
    sent_count: int = 0
    faulty_count: int = 0
    deferred_count: int = 0
    rate_limited_count: int = 0

    oldest_pending_age_seconds: int | float | None = None
    oldest_processing_age_seconds: int | float | None = None
    next_run_in_seconds: int | float | None = None

    last_error_type: str | None = None
    last_error_text: str | None = None

    @property
    def has_pending(self) -> bool:
        return self.pending_count > 0

    @property
    def has_processing(self) -> bool:
        return self.processing_count > 0

    @property
    def has_faulty(self) -> bool:
        return self.faulty_count > 0

    @property
    def has_deferred(self) -> bool:
        return self.deferred_count > 0

    def to_log_context(self) -> dict[str, object]:
        return {
            "rule_id": self.rule_id,
            "source_id": self.source_id,
            "target_id": self.target_id,
            "pending_count": self.pending_count,
            "processing_count": self.processing_count,
            "sent_count": self.sent_count,
            "faulty_count": self.faulty_count,
            "deferred_count": self.deferred_count,
            "rate_limited_count": self.rate_limited_count,
            "oldest_pending_age_seconds": self.oldest_pending_age_seconds,
            "oldest_processing_age_seconds": self.oldest_processing_age_seconds,
            "next_run_in_seconds": self.next_run_in_seconds,
            "last_error_type": sanitize_diagnostic_text(self.last_error_type, max_length=120),
            "last_error_text": sanitize_diagnostic_text(self.last_error_text),
        }


@dataclass(frozen=True, slots=True)
class DeliveryDiagnosticsSnapshot:
    status: DeliveryHealthStatus
    signals: tuple[DeliveryDiagnosticSignal, ...]

    total_rules: int
    total_pending: int
    total_processing: int
    total_sent: int
    total_faulty: int
    total_deferred: int
    total_rate_limited: int

    stuck_processing_count: int
    queue_lag_seconds: int | float | None

    problem_rules: tuple[DeliveryRuleMetrics, ...]

    generated_at: datetime | None = None
    reason: str | None = None

    @property
    def ok(self) -> bool:
        return self.status == DeliveryHealthStatus.OK

    @property
    def is_warning(self) -> bool:
        return self.status == DeliveryHealthStatus.WARNING

    @property
    def is_critical(self) -> bool:
        return self.status == DeliveryHealthStatus.CRITICAL

    @property
    def has_problem_rules(self) -> bool:
        return bool(self.problem_rules)

    def has_signal(self, signal: DeliveryDiagnosticSignal) -> bool:
        return signal in self.signals

    def log_label(self) -> str:
        return f"delivery_diagnostics:{self.status.value}"

    def to_log_context(self) -> dict[str, object]:
        return {
            "status": self.status.value,
            "signals": tuple(signal.value for signal in self.signals),
            "total_rules": self.total_rules,
            "total_pending": self.total_pending,
            "total_processing": self.total_processing,
            "total_sent": self.total_sent,
            "total_faulty": self.total_faulty,
            "total_deferred": self.total_deferred,
            "total_rate_limited": self.total_rate_limited,
            "stuck_processing_count": self.stuck_processing_count,
            "queue_lag_seconds": self.queue_lag_seconds,
            "problem_rules": tuple(rule.to_log_context() for rule in self.problem_rules),
            "generated_at": self.generated_at.isoformat() if self.generated_at else None,
            "reason": sanitize_diagnostic_text(self.reason, max_length=120),
        }

    def to_admin_text(self) -> str:
        if self.has_signal(DeliveryDiagnosticSignal.NO_DATA):
            return "📊 Диагностика доставки\n\nНет данных для диагностики."
        lines = [
            "📊 Диагностика доставки",
            "",
            f"Статус: {self.status.name}",
            f"В очереди: {self.total_pending}",
            f"В обработке: {self.total_processing}",
            f"Отправлено: {self.total_sent}",
            f"Ошибок: {self.total_faulty}",
            f"Rate-limit/deferred: {self.total_rate_limited + self.total_deferred}",
        ]
        if self.queue_lag_seconds is not None:
            lines.append(f"Максимальный lag: {self.queue_lag_seconds}s")
        if self.reason:
            lines.append(f"Причина: {sanitize_diagnostic_text(self.reason, max_length=120)}")
        if self.problem_rules:
            lines.extend(["", "Проблемные правила:"])
            for rule in self.problem_rules:
                lag = rule.oldest_pending_age_seconds
                bits = [f"pending={rule.pending_count}", f"faulty={rule.faulty_count}"]
                if rule.processing_count:
                    bits.append(f"processing={rule.processing_count}")
                if rule.rate_limited_count or rule.deferred_count:
                    bits.append(f"rate/deferred={rule.rate_limited_count + rule.deferred_count}")
                if lag is not None:
                    bits.append(f"lag={lag}s")
                lines.append(f"• rule {rule.rule_id}: " + " ".join(bits))
        return "\n".join(lines)


def normalize_delivery_rule_metrics(value: object) -> DeliveryRuleMetrics:
    if isinstance(value, DeliveryRuleMetrics):
        return value
    data: dict[str, object] = {}
    if isinstance(value, Mapping):
        data = {key: value.get(key) for key in _KNOWN_FIELDS}
    else:
        for key in _KNOWN_FIELDS:
            try:
                if hasattr(value, key):
                    data[key] = getattr(value, key)
            except Exception:
                data[key] = None
    kwargs: dict[str, Any] = {"rule_id": data.get("rule_id")}
    kwargs["source_id"] = data.get("source_id")
    kwargs["target_id"] = data.get("target_id")
    for field in _COUNT_FIELDS:
        kwargs[field] = _safe_count(data.get(field))
    for field in _AGE_FIELDS:
        kwargs[field] = _safe_age(data.get(field))
    kwargs["last_error_type"] = sanitize_diagnostic_text(data.get("last_error_type"), max_length=120)
    kwargs["last_error_text"] = sanitize_diagnostic_text(data.get("last_error_text"))
    return DeliveryRuleMetrics(**kwargs)


class DeliveryObservabilityService:
    def __init__(
        self,
        *,
        metrics_provider: object | None = None,
        config: DeliveryObservabilityConfig | None = None,
    ) -> None:
        self.metrics_provider = metrics_provider
        self.config = config or DeliveryObservabilityConfig()

    def build_snapshot(
        self,
        rule_metrics: Iterable[DeliveryRuleMetrics],
        *,
        generated_at: datetime | None = None,
    ) -> DeliveryDiagnosticsSnapshot:
        metrics = tuple(normalize_delivery_rule_metrics(item) for item in rule_metrics)
        if not metrics:
            return self._unknown_snapshot(
                generated_at=generated_at,
                reason="no_data",
                signals=(DeliveryDiagnosticSignal.NO_DATA,),
            )
        total_pending = sum(item.pending_count for item in metrics)
        total_processing = sum(item.processing_count for item in metrics)
        total_sent = sum(item.sent_count for item in metrics)
        total_faulty = sum(item.faulty_count for item in metrics)
        total_deferred = sum(item.deferred_count for item in metrics)
        total_rate_limited = sum(item.rate_limited_count for item in metrics)
        stuck_processing_count = sum(1 for item in metrics if self._is_stuck_processing(item))
        queue_lag_seconds = max(
            (item.oldest_pending_age_seconds for item in metrics if item.oldest_pending_age_seconds is not None),
            default=None,
        )
        signals = self._build_signals(
            total_pending=total_pending,
            total_processing=total_processing,
            total_faulty=total_faulty,
            total_deferred=total_deferred,
            total_rate_limited=total_rate_limited,
            stuck_processing_count=stuck_processing_count,
            queue_lag_seconds=queue_lag_seconds,
        )
        status = self._build_status(total_faulty, total_deferred, total_rate_limited, stuck_processing_count)
        problem_rules = self._select_problem_rules(metrics)
        return DeliveryDiagnosticsSnapshot(
            status=status,
            signals=signals,
            total_rules=len(metrics),
            total_pending=total_pending,
            total_processing=total_processing,
            total_sent=total_sent,
            total_faulty=total_faulty,
            total_deferred=total_deferred,
            total_rate_limited=total_rate_limited,
            stuck_processing_count=stuck_processing_count,
            queue_lag_seconds=queue_lag_seconds,
            problem_rules=problem_rules,
            generated_at=generated_at,
        )

    async def collect_snapshot(self, *, generated_at: datetime | None = None) -> DeliveryDiagnosticsSnapshot:
        provider = self.metrics_provider
        if provider is None:
            return self._unknown_snapshot(generated_at=generated_at, reason="metrics_provider_not_configured")
        try:
            if hasattr(provider, "get_delivery_rule_metrics"):
                result = provider.get_delivery_rule_metrics()  # type: ignore[attr-defined]
            elif callable(provider):
                result = provider()
            else:
                return self._unknown_snapshot(generated_at=generated_at, reason="metrics_provider_not_configured")
            if isawaitable(result):
                result = await result
            return self.build_snapshot(result or (), generated_at=generated_at)
        except Exception as exc:
            return self._unknown_snapshot(
                generated_at=generated_at,
                reason="metrics_provider_failed",
                error_text=sanitize_diagnostic_text(exc),
            )

    def _unknown_snapshot(
        self,
        *,
        generated_at: datetime | None,
        reason: str,
        signals: tuple[DeliveryDiagnosticSignal, ...] = (),
        error_text: str | None = None,
    ) -> DeliveryDiagnosticsSnapshot:
        reason_text = reason if error_text is None else f"{reason}: {error_text}"
        return DeliveryDiagnosticsSnapshot(
            status=DeliveryHealthStatus.UNKNOWN,
            signals=signals,
            total_rules=0,
            total_pending=0,
            total_processing=0,
            total_sent=0,
            total_faulty=0,
            total_deferred=0,
            total_rate_limited=0,
            stuck_processing_count=0,
            queue_lag_seconds=None,
            problem_rules=(),
            generated_at=generated_at,
            reason=sanitize_diagnostic_text(reason_text, max_length=500),
        )

    def _build_status(
        self,
        total_faulty: int,
        total_deferred: int,
        total_rate_limited: int,
        stuck_processing_count: int,
    ) -> DeliveryHealthStatus:
        if total_faulty >= self.config.faulty_critical_count:
            return DeliveryHealthStatus.CRITICAL
        if stuck_processing_count > 0:
            return DeliveryHealthStatus.CRITICAL
        if total_faulty >= self.config.faulty_warning_count:
            return DeliveryHealthStatus.WARNING
        if total_rate_limited >= self.config.rate_limited_warning_count or total_deferred > 0:
            return DeliveryHealthStatus.WARNING
        return DeliveryHealthStatus.OK

    def _build_signals(
        self,
        *,
        total_pending: int,
        total_processing: int,
        total_faulty: int,
        total_deferred: int,
        total_rate_limited: int,
        stuck_processing_count: int,
        queue_lag_seconds: int | float | None,
    ) -> tuple[DeliveryDiagnosticSignal, ...]:
        signals = []
        if stuck_processing_count > 0:
            signals.append(DeliveryDiagnosticSignal.STUCK_PROCESSING)
        if queue_lag_seconds is not None and queue_lag_seconds >= self.config.queue_lag_warning_seconds:
            signals.append(DeliveryDiagnosticSignal.QUEUE_LAG)
        if total_faulty > 0:
            signals.append(DeliveryDiagnosticSignal.FAULTY_DELIVERIES)
        if total_rate_limited > 0 or total_deferred > 0:
            signals.append(DeliveryDiagnosticSignal.RATE_LIMITED)
        if total_pending == 0 and total_processing == 0 and total_faulty == 0:
            signals.append(DeliveryDiagnosticSignal.EMPTY_QUEUE)
        return tuple(signals)

    def _select_problem_rules(self, metrics: tuple[DeliveryRuleMetrics, ...]) -> tuple[DeliveryRuleMetrics, ...]:
        # The snapshot keeps a compact rule sample for the admin formatter:
        # real problem rules first, then the largest planned queues as context.
        problem = [item for item in metrics if self._is_problem_rule(item)]
        backlog = [item for item in metrics if item.pending_count > 0 and item not in problem]
        problem.sort(key=self._problem_sort_key, reverse=True)
        backlog.sort(key=lambda item: item.pending_count, reverse=True)
        limit = max(self.config.max_problem_rules, 0)
        return tuple((problem + backlog)[:limit])

    def _is_problem_rule(self, item: DeliveryRuleMetrics) -> bool:
        return (
            item.faulty_count > 0
            or item.rate_limited_count > 0
            or item.deferred_count > 0
            or self._is_stuck_processing(item)
        )

    def _problem_sort_key(self, item: DeliveryRuleMetrics) -> tuple[int, int, int, float, int]:
        return (
            item.faulty_count,
            1 if self._is_stuck_processing(item) else 0,
            item.processing_count,
            float(item.oldest_pending_age_seconds or 0),
            item.pending_count,
        )

    def _is_stuck_processing(self, item: DeliveryRuleMetrics) -> bool:
        return (
            item.processing_count > 0
            and item.oldest_processing_age_seconds is not None
            and item.oldest_processing_age_seconds > self.config.stale_processing_after_seconds
        )

    def _has_queue_lag(self, item: DeliveryRuleMetrics) -> bool:
        return (
            item.oldest_pending_age_seconds is not None
            and item.oldest_pending_age_seconds >= self.config.queue_lag_warning_seconds
        )
