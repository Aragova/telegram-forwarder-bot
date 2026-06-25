"""Admin-safe Russian formatter for delivery diagnostics snapshots."""

from __future__ import annotations

from app.delivery_observability import (
    DeliveryDiagnosticSignal,
    DeliveryDiagnosticsSnapshot,
    DeliveryRuleMetrics,
    sanitize_diagnostic_text,
)

_MAX_TEXT_LENGTH = 4000
_MAX_PROBLEM_RULES = 8

_SIGNAL_RU = {
    DeliveryDiagnosticSignal.STUCK_PROCESSING: "зависшие processing",
    DeliveryDiagnosticSignal.QUEUE_LAG: "задержка очереди",
    DeliveryDiagnosticSignal.FAULTY_DELIVERIES: "ошибки доставок",
    DeliveryDiagnosticSignal.RATE_LIMITED: "rate limit / deferred",
    DeliveryDiagnosticSignal.EMPTY_QUEUE: "очередь пуста",
    DeliveryDiagnosticSignal.NO_DATA: "нет данных",
}


def _duration_ru(seconds: int | float | None) -> str:
    if seconds is None:
        return "нет данных"
    seconds = max(0, int(seconds))
    if seconds < 60:
        return f"{seconds} сек"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes} мин"
    hours = minutes // 60
    minutes = minutes % 60
    if hours < 24:
        return f"{hours} ч {minutes} мин"
    days = hours // 24
    hours = hours % 24
    return f"{days} д {hours} ч"


def _safe_error(rule: DeliveryRuleMetrics) -> str | None:
    error_type = sanitize_diagnostic_text(rule.last_error_type, max_length=80)
    error_text = sanitize_diagnostic_text(rule.last_error_text, max_length=160)
    if error_type and error_text:
        return f"{error_type}: {error_text}"
    return error_type or error_text


def _problem_rule_line(index: int, rule: DeliveryRuleMetrics) -> str:
    parts = [
        f"rule_id={rule.rule_id}",
        f"pending={rule.pending_count}",
        f"processing={rule.processing_count}",
        f"faulty={rule.faulty_count}",
    ]
    if rule.deferred_count or rule.rate_limited_count:
        parts.append(f"deferred/rate-limit={rule.deferred_count + rule.rate_limited_count}")
    if rule.oldest_pending_age_seconds is not None:
        parts.append(f"lag={_duration_ru(rule.oldest_pending_age_seconds)}")
    if rule.oldest_processing_age_seconds is not None:
        parts.append(f"processing age={_duration_ru(rule.oldest_processing_age_seconds)}")
    error = _safe_error(rule)
    if error:
        parts.append(f"последняя ошибка: {error}")
    return f"{index}) " + " | ".join(parts)


def format_delivery_diagnostics_admin_text(snapshot: DeliveryDiagnosticsSnapshot) -> str:
    signals = ", ".join(_SIGNAL_RU.get(signal, signal.value) for signal in snapshot.signals) or "нет"
    reason = sanitize_diagnostic_text(snapshot.reason, max_length=160)
    lines = [
        "📊 Диагностика доставки",
        "",
        f"Статус: {snapshot.status.name}",
        f"Причина: {reason or signals}",
        "",
        f"Всего правил: {snapshot.total_rules}",
        f"В очереди: {snapshot.total_pending}",
        f"В обработке: {snapshot.total_processing}",
        f"Отправлено: {snapshot.total_sent}",
        f"Ошибки: {snapshot.total_faulty}",
        f"Deferred/rate-limit: {snapshot.total_deferred + snapshot.total_rate_limited}",
        "",
        f"Зависшие processing: {snapshot.stuck_processing_count}",
        f"Самая старая pending: {_duration_ru(snapshot.queue_lag_seconds)}",
    ]
    oldest_processing = max(
        (rule.oldest_processing_age_seconds for rule in snapshot.problem_rules if rule.oldest_processing_age_seconds is not None),
        default=None,
    )
    lines.append(f"Самая старая processing: {_duration_ru(oldest_processing)}")

    if snapshot.problem_rules:
        lines.extend(["", "Проблемные правила:"])
        for index, rule in enumerate(snapshot.problem_rules[:_MAX_PROBLEM_RULES], start=1):
            lines.append(_problem_rule_line(index, rule))
        if len(snapshot.problem_rules) > _MAX_PROBLEM_RULES:
            lines.append(f"…ещё {len(snapshot.problem_rules) - _MAX_PROBLEM_RULES} правил скрыто")
    else:
        lines.extend(["", "Проблемные правила: нет"])

    text = "\n".join(lines)
    forbidden_markers = ("content_json", "caption", "Traceback", "SECRET_TOKEN")
    for marker in forbidden_markers:
        text = text.replace(marker, "<скрыто>")
    if len(text) > _MAX_TEXT_LENGTH:
        text = text[: _MAX_TEXT_LENGTH - 1] + "…"
    return text
