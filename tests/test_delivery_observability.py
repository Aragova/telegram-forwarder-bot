import asyncio

from app.delivery_observability import (
    DeliveryDiagnosticSignal,
    DeliveryHealthStatus,
    DeliveryObservabilityConfig,
    DeliveryObservabilityService,
    DeliveryRuleMetrics,
    normalize_delivery_rule_metrics,
    sanitize_diagnostic_text,
)


def test_config_defaults_are_positive():
    config = DeliveryObservabilityConfig()
    assert config.stale_processing_after_seconds > 0
    assert config.queue_lag_warning_seconds > 0
    assert config.queue_lag_critical_seconds > config.queue_lag_warning_seconds
    assert config.faulty_warning_count > 0
    assert config.faulty_critical_count >= config.faulty_warning_count
    assert config.rate_limited_warning_count > 0
    assert config.max_problem_rules > 0


def test_sanitize_diagnostic_text_removes_controls_truncates_and_redacts():
    assert sanitize_diagnostic_text(None) is None
    text = sanitize_diagnostic_text("line1\nSECRET_TOKEN\t123456:abcdefghijklmnopqrstuvwxyz")
    assert "\n" not in text
    assert "\t" not in text
    assert "SECRET_TOKEN" not in text
    assert "<redacted>" in text
    assert sanitize_diagnostic_text("x" * 20, max_length=5) == "xxxxx"


def test_rule_metrics_safe_logs_and_properties():
    rule = DeliveryRuleMetrics(
        rule_id=1,
        pending_count=1,
        processing_count=2,
        faulty_count=3,
        deferred_count=4,
        last_error_text="Traceback... SECRET_TOKEN boom",
    )
    context = rule.to_log_context()
    assert rule.has_pending
    assert rule.has_processing
    assert rule.has_faulty
    assert rule.has_deferred
    assert context["pending_count"] == 1
    assert "SECRET_TOKEN" not in str(context)


def test_empty_metrics_unknown_no_data_admin_text():
    snapshot = DeliveryObservabilityService().build_snapshot([])
    assert snapshot.status == DeliveryHealthStatus.UNKNOWN
    assert snapshot.has_signal(DeliveryDiagnosticSignal.NO_DATA)
    assert "Нет данных" in snapshot.to_admin_text()


def test_healthy_queue_is_ok():
    snapshot = DeliveryObservabilityService().build_snapshot([
        DeliveryRuleMetrics(rule_id=1, pending_count=2, oldest_pending_age_seconds=10)
    ])
    assert snapshot.status == DeliveryHealthStatus.OK
    assert snapshot.ok
    assert snapshot.problem_rules == ()


def test_faulty_warning_and_critical():
    service = DeliveryObservabilityService(config=DeliveryObservabilityConfig(faulty_critical_count=3))
    warning = service.build_snapshot([DeliveryRuleMetrics(rule_id=1, faulty_count=1)])
    critical = service.build_snapshot([DeliveryRuleMetrics(rule_id=1, faulty_count=3)])
    assert warning.status == DeliveryHealthStatus.WARNING
    assert warning.has_signal(DeliveryDiagnosticSignal.FAULTY_DELIVERIES)
    assert critical.status == DeliveryHealthStatus.CRITICAL


def test_stuck_processing_is_critical():
    snapshot = DeliveryObservabilityService().build_snapshot([
        DeliveryRuleMetrics(rule_id=1, processing_count=1, oldest_processing_age_seconds=901)
    ])
    assert snapshot.status == DeliveryHealthStatus.CRITICAL
    assert snapshot.has_signal(DeliveryDiagnosticSignal.STUCK_PROCESSING)
    assert snapshot.stuck_processing_count == 1


def test_queue_lag_warning_and_critical():
    service = DeliveryObservabilityService()
    warning = service.build_snapshot([DeliveryRuleMetrics(rule_id=1, pending_count=1, oldest_pending_age_seconds=600)])
    critical = service.build_snapshot([DeliveryRuleMetrics(rule_id=1, pending_count=1, oldest_pending_age_seconds=1800)])
    assert warning.status == DeliveryHealthStatus.WARNING
    assert warning.has_signal(DeliveryDiagnosticSignal.QUEUE_LAG)
    assert critical.status == DeliveryHealthStatus.CRITICAL


def test_rate_limited_or_deferred_warning():
    service = DeliveryObservabilityService()
    limited = service.build_snapshot([DeliveryRuleMetrics(rule_id=1, rate_limited_count=1)])
    deferred = service.build_snapshot([DeliveryRuleMetrics(rule_id=1, deferred_count=1)])
    assert limited.status == DeliveryHealthStatus.WARNING
    assert deferred.status == DeliveryHealthStatus.WARNING
    assert limited.has_signal(DeliveryDiagnosticSignal.RATE_LIMITED)
    assert deferred.has_signal(DeliveryDiagnosticSignal.RATE_LIMITED)


def test_problem_rules_selection_sorting_and_limit():
    service = DeliveryObservabilityService(config=DeliveryObservabilityConfig(max_problem_rules=2))
    snapshot = service.build_snapshot([
        DeliveryRuleMetrics(rule_id="healthy", pending_count=1),
        DeliveryRuleMetrics(rule_id="lag", pending_count=5, oldest_pending_age_seconds=700),
        DeliveryRuleMetrics(rule_id="stuck", processing_count=2, oldest_processing_age_seconds=1000),
        DeliveryRuleMetrics(rule_id="faulty", faulty_count=4),
    ])
    assert [rule.rule_id for rule in snapshot.problem_rules] == ["faulty", "stuck"]


def test_totals_aggregation():
    snapshot = DeliveryObservabilityService().build_snapshot([
        DeliveryRuleMetrics(rule_id=1, pending_count=1, processing_count=2, sent_count=3, faulty_count=4, deferred_count=5, rate_limited_count=6),
        DeliveryRuleMetrics(rule_id=2, pending_count=10, processing_count=20, sent_count=30, faulty_count=40, deferred_count=50, rate_limited_count=60),
    ])
    assert snapshot.total_rules == 2
    assert snapshot.total_pending == 11
    assert snapshot.total_processing == 22
    assert snapshot.total_sent == 33
    assert snapshot.total_faulty == 44
    assert snapshot.total_deferred == 55
    assert snapshot.total_rate_limited == 66


def test_normalize_from_mapping():
    metrics = normalize_delivery_rule_metrics({
        "rule_id": "1",
        "pending_count": "2",
        "processing_count": None,
        "sent_count": -1,
        "faulty_count": "bad",
        "deferred_count": 3.7,
        "unknown": "ignored",
    })
    assert metrics.rule_id == "1"
    assert metrics.pending_count == 2
    assert metrics.processing_count == 0
    assert metrics.sent_count == 0
    assert metrics.faulty_count == 0
    assert metrics.deferred_count == 3


def test_normalize_from_object_attributes():
    class Row:
        rule_id = 5
        pending_count = "6"
        oldest_pending_age_seconds = "7.5"

    metrics = normalize_delivery_rule_metrics(Row())
    assert metrics.rule_id == 5
    assert metrics.pending_count == 6
    assert metrics.oldest_pending_age_seconds == 7.5


def test_collect_snapshot_provider_missing():
    snapshot = asyncio.run(DeliveryObservabilityService().collect_snapshot())
    assert snapshot.status == DeliveryHealthStatus.UNKNOWN
    assert snapshot.reason == "metrics_provider_not_configured"


def test_collect_snapshot_provider_method_sync_and_async():
    class SyncProvider:
        def get_delivery_rule_metrics(self):
            return [{"rule_id": 1, "pending_count": 1}]

    class AsyncProvider:
        async def get_delivery_rule_metrics(self):
            return [{"rule_id": 2, "pending_count": 2}]

    sync_snapshot = asyncio.run(DeliveryObservabilityService(metrics_provider=SyncProvider()).collect_snapshot())
    async_snapshot = asyncio.run(DeliveryObservabilityService(metrics_provider=AsyncProvider()).collect_snapshot())
    assert sync_snapshot.total_pending == 1
    assert async_snapshot.total_pending == 2


def test_collect_snapshot_callable_sync_and_async():
    def sync_provider():
        return [{"rule_id": 1, "sent_count": 1}]

    async def async_provider():
        return [{"rule_id": 2, "sent_count": 2}]

    assert asyncio.run(DeliveryObservabilityService(metrics_provider=sync_provider).collect_snapshot()).total_sent == 1
    assert asyncio.run(DeliveryObservabilityService(metrics_provider=async_provider).collect_snapshot()).total_sent == 2


def test_provider_exception_is_safe():
    def provider():
        raise RuntimeError("SECRET_TOKEN boom")

    snapshot = asyncio.run(DeliveryObservabilityService(metrics_provider=provider).collect_snapshot())
    assert snapshot.status == DeliveryHealthStatus.UNKNOWN
    assert snapshot.reason.startswith("metrics_provider_failed")
    assert "SECRET_TOKEN" not in str(snapshot.to_log_context())
    assert "SECRET_TOKEN" not in snapshot.to_admin_text()


def test_snapshot_safe_logs_label_and_admin_text():
    snapshot = DeliveryObservabilityService().build_snapshot([
        DeliveryRuleMetrics(rule_id=15, pending_count=10, faulty_count=2, oldest_pending_age_seconds=900, last_error_text="PRIVATE_TOKEN")
    ])
    context = snapshot.to_log_context()
    admin_text = snapshot.to_admin_text()
    assert snapshot.log_label() == "delivery_diagnostics:warning"
    assert "PRIVATE_TOKEN" not in str(context)
    assert "PRIVATE_TOKEN" not in admin_text
    assert "Статус" in admin_text
    assert "Проблемные правила" in admin_text
