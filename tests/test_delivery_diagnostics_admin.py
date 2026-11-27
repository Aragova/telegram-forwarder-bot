from __future__ import annotations

from html.parser import HTMLParser

from app.delivery_diagnostics_admin import format_delivery_diagnostics_admin_text
from app.delivery_observability import DeliveryDiagnosticSignal, DeliveryDiagnosticsSnapshot, DeliveryHealthStatus, DeliveryRuleMetrics


def _snapshot(status=DeliveryHealthStatus.OK, signals=(), problem_rules=(), **overrides):
    data = dict(status=status, signals=tuple(signals), total_rules=1, total_pending=0, total_processing=0, total_sent=10, total_faulty=0, total_deferred=0, total_rate_limited=0, stuck_processing_count=0, queue_lag_seconds=None, problem_rules=tuple(problem_rules), reason=None)
    data.update(overrides)
    return DeliveryDiagnosticsSnapshot(**data)


def test_ok_snapshot_text_is_human_readable_russian_and_short() -> None:
    text = format_delivery_diagnostics_admin_text(_snapshot(total_pending=7, total_processing=2, total_faulty=1))
    assert "📊 Диагностика доставки" in text
    assert "Ждут своего времени: <b>7</b>" in text
    assert "Сейчас обрабатываются: <b>2</b>" in text
    assert "С ошибкой: <b>1</b>" in text
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
    assert "временно ограничил частоту запросов" in text
    assert "TelegramRetryAfter" not in text


def test_big_planned_queue_is_not_described_as_error() -> None:
    rule = DeliveryRuleMetrics(rule_id=49, pending_count=55666, oldest_pending_age_seconds=95 * 24 * 3600)
    text = format_delivery_diagnostics_admin_text(_snapshot(total_pending=55666, total_processing=0, total_faulty=0, total_rate_limited=0, stuck_processing_count=0, queue_lag_seconds=95 * 24 * 3600, problem_rules=(rule,)))
    assert "🟢 Состояние: очередь работает" in text
    assert "Не найдено" in text
    assert "Большая очередь — это плановые доставки" in text
    assert "Правило #49 — ждут своего времени: 55666" in text
    assert "CRITICAL" not in text


def test_young_processing_is_not_shown_as_stuck() -> None:
    rule = DeliveryRuleMetrics(rule_id=77, pending_count=4000, processing_count=1, oldest_processing_age_seconds=30)
    text = format_delivery_diagnostics_admin_text(_snapshot(total_pending=4000, total_processing=1, stuck_processing_count=0, problem_rules=(rule,)))
    stuck_section = text.split("🧯 Зависшие задачи:", 1)[1].split("⚠️ Правила с ошибками:", 1)[0]
    assert "Не найдено" in stuck_section
    assert "Правило #77" not in stuck_section
    assert "Правило #77 — ждут своего времени: 4000" in text


def test_unknown_snapshot_text_is_safe() -> None:
    text = format_delivery_diagnostics_admin_text(_snapshot(status=DeliveryHealthStatus.UNKNOWN, signals=(DeliveryDiagnosticSignal.NO_DATA,), reason="Traceback SECRET_TOKEN content_json caption"))
    assert "диагностика временно недоступна" in text
    assert "Не удалось определить" not in text
    assert "нет точного описания" in text
    assert "Состояние доставки неизвестно. Повторите диагностику позже." in text
    assert "Ошибок и зависших задач нет" not in text
    for forbidden in ("SECRET_TOKEN", "content_json", "caption", "Traceback"):
        assert forbidden not in text
    assert len(text) <= 4000


def test_rescan_error_has_instruction_without_raw_code() -> None:
    rule = DeliveryRuleMetrics(rule_id=208, faulty_count=10,
        last_error_text="album_member_set_changed_manual_review_no_resend")
    text = format_delivery_diagnostics_admin_text(_snapshot(
        status=DeliveryHealthStatus.CRITICAL, total_faulty=10, problem_rules=(rule,)))
    assert "Пересканировать" in text
    assert "Начать заново" in text
    assert "возможен дубль" in text
    assert "album_member_set_changed" not in text
    assert "🔴 <b>Причина</b>\n" in text
    assert "\n\n🛠 <b>Что делать:</b>\n1. Выключите правило." in text
    assert "\n5. Затем включите правило." in text
    assert "\n\n⚠️ При выборе уже опубликованного поста" in text


def test_large_diagnostics_keeps_complete_html_and_safety_instructions() -> None:
    rules = tuple(DeliveryRuleMetrics(
        rule_id=i, faulty_count=10, pending_count=50,
        last_error_text="album_member_set_changed_manual_review_no_resend",
    ) for i in range(1, 9))
    text = format_delivery_diagnostics_admin_text(_snapshot(
        status=DeliveryHealthStatus.CRITICAL, total_faulty=80, problem_rules=rules))
    assert len(text.encode("utf-16-le")) // 2 <= 4000
    assert "Остальные записи" in text
    assert text.count("1. Выключите правило.") == text.count("5. Затем включите правило.")
    assert "<b>💡 Вывод:</b>" in text

    class BalancedTags(HTMLParser):
        def __init__(self):
            super().__init__()
            self.stack = []

        def handle_starttag(self, tag, attrs):
            self.stack.append(tag)

        def handle_endtag(self, tag):
            assert self.stack.pop() == tag

    parser = BalancedTags()
    parser.feed(text)
    assert not parser.stack


def test_diagnostic_rule_label_is_escaped() -> None:
    rule = DeliveryRuleMetrics(rule_id="<b>1</b>", faulty_count=1)
    text = format_delivery_diagnostics_admin_text(_snapshot(
        status=DeliveryHealthStatus.WARNING, total_faulty=1, problem_rules=(rule,)))
    assert "<b>1) Правило #&lt;b&gt;1&lt;/b&gt;</b>" in text
    assert "Правило #<b>1</b>" not in text
