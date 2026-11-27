"""Admin-safe Russian formatter for delivery diagnostics snapshots."""

from __future__ import annotations

from html import escape

from app.delivery_observability import (
    DeliveryDiagnosticSignal,
    DeliveryDiagnosticsSnapshot,
    DeliveryObservabilityConfig,
    DeliveryHealthStatus,
    DeliveryRuleMetrics,
)
from app.delivery_error_text import DeliveryError, describe_delivery_error

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


def _safe_error(rule: DeliveryRuleMetrics) -> DeliveryError | None:
    if not rule.last_error_type and not rule.last_error_text:
        return None
    return describe_delivery_error(rule.last_error_text, rule.last_error_type)


def _stuck_rules(snapshot: DeliveryDiagnosticsSnapshot) -> list[DeliveryRuleMetrics]:
    stale_seconds = DeliveryObservabilityConfig().stale_processing_after_seconds
    return [
        r
        for r in snapshot.problem_rules
        if r.processing_count > 0
        and r.oldest_processing_age_seconds is not None
        and r.oldest_processing_age_seconds > stale_seconds
    ]


def _error_rules(snapshot: DeliveryDiagnosticsSnapshot) -> list[DeliveryRuleMetrics]:
    return [r for r in snapshot.problem_rules if r.faulty_count > 0 or r.deferred_count > 0 or r.rate_limited_count > 0]


def _largest_queues(snapshot: DeliveryDiagnosticsSnapshot) -> list[DeliveryRuleMetrics]:
    rules = [r for r in snapshot.problem_rules if r.pending_count > 0]
    rules.sort(key=lambda r: r.pending_count, reverse=True)
    return rules[:_MAX_RULES]


def _append_stuck_section(lines: list[str], snapshot: DeliveryDiagnosticsSnapshot) -> None:
    lines.extend(["", "<b>🧯 Зависшие задачи:</b>"])
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
        lines.append(f"{index}) Правило #{escape(str(rule.rule_id)[:64])} — зависло {count} {word}, старшая висит {_duration_ru(rule.oldest_processing_age_seconds)}")


def _append_error_section(lines: list[str], snapshot: DeliveryDiagnosticsSnapshot) -> None:
    rules = _error_rules(snapshot)
    lines.extend(["", "<b>⚠️ Правила с ошибками:</b>"])
    if not rules:
        lines.append("• Не найдено")
        return
    for index, rule in enumerate(rules[:_MAX_RULES], start=1):
        block = [
            f"<b>{index}) Правило #{escape(str(rule.rule_id)[:64])}</b>",
            f"❌ С ошибкой: {rule.faulty_count}",
        ]
        delayed = rule.deferred_count + rule.rate_limited_count
        if delayed:
            block.append(f"⏳ Отложено из-за ограничений Telegram: {delayed}")
        error = _safe_error(rule)
        if error:
            block.extend([f"📌 <b>{escape(error.title)}</b>", "", error.html()])
        lines.append("\n".join(block))


def _append_largest_queues(lines: list[str], snapshot: DeliveryDiagnosticsSnapshot) -> None:
    lines.extend(["", "<b>📌 Самые большие очереди по правилам:</b>"])
    rules = _largest_queues(snapshot)
    if not rules:
        lines.append("• Нет данных по большим очередям")
        return
    for index, rule in enumerate(rules, start=1):
        age = ""
        if rule.oldest_pending_age_seconds is not None:
            age = f" — старшая ждёт {_duration_ru(rule.oldest_pending_age_seconds)}"
        lines.append(f"{index}) Правило #{escape(str(rule.rule_id)[:64])} — ждут своего времени: {rule.pending_count}{age}")


def format_delivery_diagnostics_admin_text(snapshot: DeliveryDiagnosticsSnapshot) -> str:
    reason = describe_delivery_error(snapshot.reason).reason
    lines = ["<b>📊 Диагностика доставки</b>", "", f"<b>{_status_line(snapshot)}</b>"]
    if snapshot.status == DeliveryHealthStatus.OK:
        lines.append("Посты ждут своего времени по интервалам правил.")
    elif snapshot.status == DeliveryHealthStatus.UNKNOWN:
        lines.append(f"Причина: {escape(reason)}")

    lines.extend([
        "",
        "<b>📦 Очередь:</b>",
        f"🕒 Ждут своего времени: <b>{snapshot.total_pending}</b>",
        f"🔄 Сейчас обрабатываются: <b>{snapshot.total_processing}</b>",
        f"✅ Уже отправлено: <b>{snapshot.total_sent}</b>",
        f"❌ С ошибкой: <b>{snapshot.total_faulty}</b>",
        f"⏳ Отложено из-за ограничений Telegram: <b>{snapshot.total_deferred + snapshot.total_rate_limited}</b>",
    ])
    if snapshot.queue_lag_seconds is not None:
        lines.append(f"• Старшие посты в плановой очереди ждут: {_duration_ru(snapshot.queue_lag_seconds)}")

    # Reserve the conclusion and fit whole blocks instead of slicing HTML or
    # cutting a safety instruction halfway through.
    sections: list[str] = []
    _append_stuck_section(sections, snapshot)
    _append_error_section(sections, snapshot)
    _append_largest_queues(sections, snapshot)

    conclusion = ["", "<b>💡 Вывод:</b>"]
    if snapshot.stuck_processing_count > 0:
        conclusion.append('Есть зависшие задачи. Нажмите «🧯 Зависшие задачи» для проверки. Возврат в очередь возможен после сверки с получателем.')
    elif snapshot.total_faulty == 0 and snapshot.total_deferred + snapshot.total_rate_limited == 0:
        conclusion.append("Ошибок и зависших задач нет. Большая очередь — это плановые доставки, которые будут отправляться по интервалам правил.")
    else:
        conclusion.append("Есть предупреждения по ошибкам или ограничениям Telegram. Плановая очередь сама по себе не является ошибкой.")

    omitted = "… Остальные записи смотрите в разделе «Проблемные доставки»."
    for block in sections:
        separator = [""] if "\n" in block else []
        candidate = "\n".join([*lines, *separator, block, "", omitted, *conclusion])
        # UTF-16 length is conservative for Telegram's message limit, including
        # markup and astral emoji.
        if len(candidate.encode("utf-16-le")) // 2 > _MAX_TEXT_LENGTH:
            lines.extend(["", omitted])
            break
        lines.extend([*separator, block])
    return "\n".join([*lines, *conclusion])
