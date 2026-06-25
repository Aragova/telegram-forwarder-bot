from __future__ import annotations

from app.delivery_diagnostics_admin import format_delivery_diagnostics_admin_text
from app.delivery_observability import (
    DeliveryDiagnosticSignal,
    DeliveryDiagnosticsSnapshot,
    DeliveryHealthStatus,
    DeliveryRuleMetrics,
)


def _snapshot(status=DeliveryHealthStatus.OK, signals=(), problem_rules=(), **overrides):
    data = dict(
        status=status,
        signals=tuple(signals),
        total_rules=1,
        total_pending=0,
        total_processing=0,
        total_sent=10,
        total_faulty=0,
        total_deferred=0,
        total_rate_limited=0,
        stuck_processing_count=0,
        queue_lag_seconds=None,
        problem_rules=tuple(problem_rules),
        reason=None,
    )
    data.update(overrides)
    return DeliveryDiagnosticsSnapshot(**data)


def test_ok_snapshot_text_is_russian_and_short() -> None:
    text = format_delivery_diagnostics_admin_text(_snapshot())

    assert "📊 Диагностика доставки" in text
    assert "Статус: OK" in text
    assert "Проблемные правила: нет" in text
    assert len(text) <= 4000


def test_warning_snapshot_contains_pending_processing_faulty() -> None:
    text = format_delivery_diagnostics_admin_text(
        _snapshot(
            status=DeliveryHealthStatus.WARNING,
            signals=(DeliveryDiagnosticSignal.FAULTY_DELIVERIES,),
            total_pending=7,
            total_processing=2,
            total_faulty=1,
        )
    )

    assert "В очереди: 7" in text
    assert "В обработке: 2" in text
    assert "Ошибки: 1" in text


def test_critical_snapshot_contains_stuck_lag_problem_rules() -> None:
    rule = DeliveryRuleMetrics(
        rule_id=15,
        pending_count=120,
        processing_count=1,
        faulty_count=3,
        oldest_pending_age_seconds=1020,
        oldest_processing_age_seconds=540,
        last_error_type="TelegramRetryAfter",
    )
    text = format_delivery_diagnostics_admin_text(
        _snapshot(
            status=DeliveryHealthStatus.CRITICAL,
            signals=(DeliveryDiagnosticSignal.STUCK_PROCESSING, DeliveryDiagnosticSignal.QUEUE_LAG),
            total_pending=120,
            total_processing=1,
            total_faulty=3,
            stuck_processing_count=1,
            queue_lag_seconds=1020,
            problem_rules=(rule,),
        )
    )

    assert "Статус: CRITICAL" in text
    assert "Зависшие processing: 1" in text
    assert "Самая старая pending: 17 мин" in text
    assert "rule_id=15" in text
    assert "TelegramRetryAfter" in text


def test_unknown_snapshot_text_is_safe() -> None:
    text = format_delivery_diagnostics_admin_text(
        _snapshot(status=DeliveryHealthStatus.UNKNOWN, signals=(DeliveryDiagnosticSignal.NO_DATA,), reason="Traceback SECRET_TOKEN content_json caption")
    )

    assert "Статус: UNKNOWN" in text
    assert "<скрыто>" in text
    for forbidden in ("SECRET_TOKEN", "content_json", "caption", "Traceback"):
        assert forbidden not in text
    assert len(text) <= 4000
