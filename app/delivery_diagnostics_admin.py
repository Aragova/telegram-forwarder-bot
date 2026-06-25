"""Admin-safe Russian formatter for delivery diagnostics snapshots."""

from __future__ import annotations

from app.delivery_observability import (
    DeliveryDiagnosticSignal,
    DeliveryDiagnosticsSnapshot,
    DeliveryHealthStatus,
    DeliveryRuleMetrics,
    sanitize_diagnostic_text,
)

_MAX_TEXT_LENGTH = 4000
_MAX_RULES = 8


def _duration_ru(seconds: int | float | None) -> str:
    if seconds is None:
        return "нет данных"
    seconds = max(0, int(seconds))
    if seconds < 60:
        return f"{seconds} сек"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes} мин"
    hours, minutes = divmod(minutes, 60)
    if hours < 24:
        return f"{hours} ч {minutes} мин"
    days, hours = divmod(hours, 24)
    return f"{days} д {hours} ч"


def _status_line(snapshot: DeliveryDiagnosticsSnapshot) -> str:
    if snapshot.status == DeliveryHealthStatus.CRITICAL and snapshot.stuck_processing_count:
        return "🔴 Состояние: есть зависшие задачи"
    if snapshot.status == DeliveryHealthStatus.CRITICAL:
        return "🔴 Состояние: требуется внимание администратора"
    if snapshot.status == DeliveryHealthStatus.WARNING:
        return "🟡 Состояние: есть предупреждения"
    if snapshot.status == DeliveryHealthStatus.UNKNOWN:
        return "⚪ Состояние: диагностика временно недоступна"
    return "🟢 Состояние: очередь работает"


def _safe_error(rule: DeliveryRuleMetrics) -> str | None:
    error_type = sanitize_diagnostic_text(rule.last_error_type, max_length=80)
    error_text = sanitize_diagnostic_text(rule.last_error_text, max_length=160)
    if error_type and error_text:
        return f"{error_type}: {error_text}"
    return error_type or error_text


def _stuck_rules(snapshot: DeliveryDiagnosticsSnapshot) -> list[DeliveryRuleMetrics]:
    return [r for r in snapshot.problem_rules if r.processing_count > 0 and r.oldest_processing_age_seconds is not None]


def _error_rules(snapshot: DeliveryDiagnosticsSnapshot) -> list[DeliveryRuleMetrics]:
    return [r for r in snapshot.problem_rules if r.faulty_count > 0 or r.deferred_count > 0 or r.rate_limited_count > 0]


def _largest_queues(snapshot: DeliveryDiagnosticsSnapshot) -> list[DeliveryRuleMetrics]:
    rules = [r for r in snapshot.problem_rules if r.pending_count > 0]
    rules.sort(key=lambda r: r.pending_count, reverse=True)
    return rules[:_MAX_RULES]


def _append_stuck_section(lines: list[str], snapshot: DeliveryDiagnosticsSnapshot) -> None:
    lines.extend(["", "🧯 Зависшие задачи:"])
    rules = _stuck_rules(snapshot)
    if not rules and snapshot.stuck_processing_count <= 0:
        lines.append("• Не найдено")
        return
    if not rules:
        lines.append(f"• Найдено: {snapshot.stuck_processing_count}")
        return
    for index, rule in enumerate(rules[:_MAX_RULES], start=1):
        count = rule.processing_count
        word = "задача" if count == 1 else "задачи"
        lines.append(f"{index}) Правило #{rule.rule_id} — зависло {count} {word}, старшая висит {_duration_ru(rule.oldest_processing_age_seconds)}")


def _append_error_section(lines: list[str], snapshot: DeliveryDiagnosticsSnapshot) -> None:
    rules = _error_rules(snapshot)
    lines.extend(["", "⚠️ Правила с ошибками:"])
    if not rules:
        lines.append("• Не найдено")
        return
    for index, rule in enumerate(rules[:_MAX_RULES], start=1):
        details = [f"С ошибкой: {rule.faulty_count}"]
        delayed = rule.deferred_count + rule.rate_limited_count
        if delayed:
            details.append(f"Задержано Telegram/rate-limit: {delayed}")
        error = _safe_error(rule)
        if error:
            details.append(f"последняя ошибка: {error}")
        lines.append(f"{index}) Правило #{rule.rule_id} — " + "; ".join(details))


def _append_largest_queues(lines: list[str], snapshot: DeliveryDiagnosticsSnapshot) -> None:
    lines.extend(["", "📌 Самые большие очереди по правилам:"])
    rules = _largest_queues(snapshot)
    if not rules:
        lines.append("• Нет данных по большим очередям")
        return
    for index, rule in enumerate(rules, start=1):
        age = ""
        if rule.oldest_pending_age_seconds is not None:
            age = f" — старшая ждёт {_duration_ru(rule.oldest_pending_age_seconds)}"
        lines.append(f"{index}) Правило #{rule.rule_id} — ждут своего времени: {rule.pending_count}{age}")


def format_delivery_diagnostics_admin_text(snapshot: DeliveryDiagnosticsSnapshot) -> str:
    reason = sanitize_diagnostic_text(snapshot.reason, max_length=160)
    lines = ["📊 Диагностика доставки", "", _status_line(snapshot)]
    if snapshot.status == DeliveryHealthStatus.OK:
        lines.append("Посты ждут своего времени по интервалам правил.")
    elif snapshot.status == DeliveryHealthStatus.UNKNOWN:
        lines.append(f"Не удалось получить данные диагностики: {reason or 'причина неизвестна'}.")

    lines.extend([
        "",
        "📦 Очередь:",
        f"• Ждут своего времени: {snapshot.total_pending}",
        f"• Сейчас обрабатываются: {snapshot.total_processing}",
        f"• Уже отправлено: {snapshot.total_sent}",
        f"• С ошибкой: {snapshot.total_faulty}",
        f"• Задержано Telegram/rate-limit: {snapshot.total_deferred + snapshot.total_rate_limited}",
    ])
    if snapshot.queue_lag_seconds is not None:
        lines.append(f"• Старшие посты в плановой очереди ждут: {_duration_ru(snapshot.queue_lag_seconds)}")

    _append_stuck_section(lines, snapshot)
    _append_error_section(lines, snapshot)
    _append_largest_queues(lines, snapshot)

    lines.extend(["", "Вывод:"])
    if snapshot.stuck_processing_count > 0:
        lines.append('есть зависшие задачи. Нажмите "🧯 Зависшие задачи", чтобы посмотреть и вручную вернуть их в очередь.')
    elif snapshot.total_faulty == 0 and snapshot.total_deferred + snapshot.total_rate_limited == 0:
        lines.append("ошибок и зависших задач нет. Большая очередь — это плановые доставки, которые будут отправляться по интервалам правил.")
    else:
        lines.append("есть предупреждения по ошибкам или задержкам Telegram/rate-limit; плановая очередь сама по себе не является ошибкой.")

    text = "\n".join(lines)
    for marker in ("content_json", "caption", "Traceback", "SECRET_TOKEN"):
        text = text.replace(marker, "<скрыто>")
    for technical in ("pending", "processing", "faulty", "rule_id", "lag", "Deferred/rate-limit"):
        text = text.replace(technical, "<скрыто>")
    if len(text) > _MAX_TEXT_LENGTH:
        text = text[: _MAX_TEXT_LENGTH - 1] + "…"
    return text
