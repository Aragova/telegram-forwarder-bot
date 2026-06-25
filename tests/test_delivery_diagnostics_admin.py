from __future__ import annotations

from app.delivery_diagnostics_admin import format_delivery_diagnostics_admin_text
from app.delivery_observability import DeliveryDiagnosticSignal, DeliveryDiagnosticsSnapshot, DeliveryHealthStatus, DeliveryRuleMetrics


def _snapshot(status=DeliveryHealthStatus.OK, signals=(), problem_rules=(), **overrides):
    data = dict(status=status, signals=tuple(signals), total_rules=1, total_pending=0, total_processing=0, total_sent=10, total_faulty=0, total_deferred=0, total_rate_limited=0, stuck_processing_count=0, queue_lag_seconds=None, problem_rules=tuple(problem_rules), reason=None)
    data.update(overrides)
    return DeliveryDiagnosticsSnapshot(**data)


def test_ok_snapshot_text_is_human_readable_russian_and_short() -> None:
    text = format_delivery_diagnostics_admin_text(_snapshot(total_pending=7, total_processing=2, total_faulty=1))
    assert "📊 Диагностика доставки" in text
    assert "Ждут своего времени: 7" in text
    assert "Сейчас обрабатываются: 2" in text
    assert "С ошибкой: 1" in text
    assert "Посты ждут своего времени по интервалам правил" in text
    assert len(text) <= 4000
    for forbidden in ("pending", "processing", "faulty", "rule_id", "lag"):
        assert forbidden not in text


def test_critical_snapshot_splits_stuck_errors_and_largest_queues() -> None:
    stuck = DeliveryRuleMetrics(rule_id=15, pending_count=120, processing_count=1, faulty_count=0, oldest_pending_age_seconds=1020, oldest_processing_age_seconds=5400)
    faulty = DeliveryRuleMetrics(rule_id=16, pending_count=3, faulty_count=3, last_error_type="TelegramRetryAfter")
    text = format_delivery_diagnostics_admin_text(_snapshot(status=DeliveryHealthStatus.CRITICAL, signals=(DeliveryDiagnosticSignal.STUCK_PROCESSING,), total_pending=123, total_processing=1, total_faulty=3, stuck_processing_count=1, queue_lag_seconds=1020, problem_rules=(stuck, faulty)))
    assert "🔴 Состояние: есть зависшие задачи" in text
    assert "Правило #15" in text
    assert "старшая висит 1 ч 30 мин" in text
    assert "⚠️ Правила с ошибками" in text
    assert "📌 Самые большие очереди по правилам" in text
    assert "TelegramRetryAfter" in text


def test_big_planned_queue_is_not_described_as_error() -> None:
    rule = DeliveryRuleMetrics(rule_id=49, pending_count=55666, oldest_pending_age_seconds=95 * 24 * 3600)
    text = format_delivery_diagnostics_admin_text(_snapshot(total_pending=55666, total_processing=0, total_faulty=0, total_rate_limited=0, stuck_processing_count=0, queue_lag_seconds=95 * 24 * 3600, problem_rules=(rule,)))
    assert "🟢 Состояние: очередь работает" in text
    assert "Не найдено" in text
    assert "Большая очередь — это плановые доставки" in text
    assert "Правило #49 — ждут своего времени: 55666" in text
    assert "CRITICAL" not in text


def test_unknown_snapshot_text_is_safe() -> None:
    text = format_delivery_diagnostics_admin_text(_snapshot(status=DeliveryHealthStatus.UNKNOWN, signals=(DeliveryDiagnosticSignal.NO_DATA,), reason="Traceback SECRET_TOKEN content_json caption"))
    assert "диагностика временно недоступна" in text
    assert "<скрыто>" in text
    for forbidden in ("SECRET_TOKEN", "content_json", "caption", "Traceback"):
        assert forbidden not in text
    assert len(text) <= 4000
