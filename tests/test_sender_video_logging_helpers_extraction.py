import asyncio
import logging
from types import SimpleNamespace

import pytest

from app.sender import SenderService
from app.sender_video_logging_helpers import SenderVideoLoggingHelpers


class FakeDb:
    def __init__(self):
        self.video_events = []
        self.faulty = []
        self.sent = []

    def log_video_event(self, **kwargs):
        self.video_events.append(kwargs)

    def mark_delivery_faulty(self, delivery_id, error_text):
        self.faulty.append((delivery_id, error_text))

    def mark_delivery_sent(self, delivery_id):
        self.sent.append(delivery_id)


def make_owner(db=None):
    return SimpleNamespace(db=db or FakeDb())


def test_sender_video_logging_helpers_extracted_from_sender():
    from pathlib import Path

    sender_source = Path("app/sender.py").read_text(encoding="utf-8")
    helper_source = Path("app/sender_video_logging_helpers.py").read_text(encoding="utf-8")

    assert "def schedule_video_event_log" in helper_source
    assert "def log_video_event_sync" in helper_source
    assert "def finalize_video_failure_sync" in helper_source
    assert "def finalize_video_success_sync" in helper_source
    assert "def stage_name_ru" in helper_source
    assert "def log_human_video_event" in helper_source

    assert sender_source.count("def _schedule_video_event_log") == 1
    assert sender_source.count("def _log_video_event_sync") == 1
    assert sender_source.count("def _finalize_video_failure_sync") == 1
    assert sender_source.count("def _finalize_video_success_sync") == 1
    assert sender_source.count("def _stage_name_ru") == 1
    assert sender_source.count("def _log_human_video_event") == 1


def test_sender_video_logging_helpers_do_not_import_sender():
    from pathlib import Path

    source = Path("app/sender_video_logging_helpers.py").read_text(encoding="utf-8")

    forbidden = [
        "from ." + "sender import",
        "import app." + "sender",
        "import ." + "sender",
    ]

    for item in forbidden:
        assert item not in source


class StubVideoHelpers:
    calls = []

    def __init__(self, owner):
        self.owner = owner

    def _record(self, name, **kwargs):
        self.calls.append((name, self.owner, kwargs))
        return {"name": name, "kwargs": kwargs}

    def schedule_video_event_log(self, **kwargs):
        return self._record("schedule_video_event_log", **kwargs)

    def log_video_event_sync(self, **kwargs):
        return self._record("log_video_event_sync", **kwargs)

    def finalize_video_failure_sync(self, **kwargs):
        return self._record("finalize_video_failure_sync", **kwargs)

    def finalize_video_success_sync(self, **kwargs):
        return self._record("finalize_video_success_sync", **kwargs)

    def stage_name_ru(self, stage):
        self.calls.append(("stage_name_ru", self.owner, {"stage": stage}))
        return "этап"

    def log_human_video_event(self, **kwargs):
        return self._record("log_human_video_event", **kwargs)


@pytest.fixture
def patched_video_helpers(monkeypatch):
    StubVideoHelpers.calls = []
    monkeypatch.setattr("app.sender_video_logging_helpers.SenderVideoLoggingHelpers", StubVideoHelpers)
    return StubVideoHelpers.calls


def test_sender_wrapper_delegates_schedule_video_event_log(patched_video_helpers):
    service = object.__new__(SenderService)
    kwargs = dict(event_type="e", delivery_id=1, rule_id=2, post_id=None, status=None, error_text="err", extra={"x": 1})
    assert service._schedule_video_event_log(**kwargs)["name"] == "schedule_video_event_log"
    assert patched_video_helpers == [("schedule_video_event_log", service, kwargs)]


def test_sender_wrapper_delegates_log_video_event_sync(patched_video_helpers):
    service = object.__new__(SenderService)
    kwargs = dict(event_type="e", delivery_id=1, rule_id=2, post_id=3, status="s", error_text=None, extra=None)
    service._log_video_event_sync(**kwargs)
    assert patched_video_helpers == [("log_video_event_sync", service, kwargs)]


def test_sender_wrapper_delegates_finalize_video_failure_sync(patched_video_helpers):
    service = object.__new__(SenderService)
    kwargs = dict(delivery_id=1, rule_id=2, post_id=None, source_channel="src", target_id="dst", target_thread_id=None, source_message_id=10, error_text="err", fallback_mode=None, caption_delivery_mode="copy", selected_mode=None, caption_requires_premium=False)
    service._finalize_video_failure_sync(**kwargs)
    assert patched_video_helpers == [("finalize_video_failure_sync", service, kwargs)]


def test_sender_wrapper_delegates_finalize_video_success_sync(patched_video_helpers):
    service = object.__new__(SenderService)
    kwargs = dict(delivery_id=1, rule_id=2, post_id=3, source_channel="src", target_id="dst", target_thread_id=4, source_message_id=10, sent_message_id=20, fallback_mode="none", caption_delivery_mode="copy", selected_mode="botapi", caption_requires_premium=False, candidate_sent_message_ids=[20], valid_sent_message_ids=[20])
    service._finalize_video_success_sync(**kwargs)
    assert patched_video_helpers == [("finalize_video_success_sync", service, kwargs)]


def test_sender_wrapper_delegates_stage_name_ru(patched_video_helpers):
    service = object.__new__(SenderService)
    assert service._stage_name_ru("download") == "этап"
    assert patched_video_helpers == [("stage_name_ru", service, {"stage": "download"})]


def test_sender_wrapper_delegates_log_human_video_event(patched_video_helpers):
    service = object.__new__(SenderService)
    kwargs = dict(event_type="video_stage_started", status=None, error_text=None, extra={"stage": "download"})
    service._log_human_video_event(**kwargs)
    assert patched_video_helpers == [("log_human_video_event", service, kwargs)]


def test_log_video_event_sync_smoke():
    db = FakeDb()
    helper = SenderVideoLoggingHelpers(make_owner(db))

    helper.log_video_event_sync(
        event_type="video_download_completed",
        delivery_id=1,
        rule_id=2,
        post_id=3,
        status="completed",
        error_text=None,
        extra={"stage": "download"},
    )

    assert db.video_events == [{
        "event_type": "video_download_completed",
        "delivery_id": 1,
        "rule_id": 2,
        "post_id": 3,
        "status": "completed",
        "error_text": None,
        "extra": {"stage": "download"},
    }]


def test_schedule_video_event_log_no_running_loop_smoke():
    db = FakeDb()
    helper = SenderVideoLoggingHelpers(make_owner(db))
    helper.schedule_video_event_log(event_type="e", delivery_id=1, rule_id=2, post_id=3, status="s", extra={"x": 1})
    assert db.video_events == []


def test_schedule_video_event_log_schedules_run_db_inside_running_loop(monkeypatch):
    async def fake_run_db(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    async def run_case():
        monkeypatch.setattr("app.sender_video_logging_helpers.run_db", fake_run_db)
        db = FakeDb()
        helper = SenderVideoLoggingHelpers(make_owner(db))
        helper.schedule_video_event_log(event_type="e", delivery_id=1, rule_id=2, post_id=3, status="s", extra={"x": 1})
        await asyncio.sleep(0)
        assert db.video_events[0]["event_type"] == "e"
        assert db.video_events[0]["status"] == "s"
        assert db.video_events[0]["extra"] == {"x": 1}

    asyncio.run(run_case())


def test_finalize_video_failure_sync_smoke():
    db = FakeDb()
    helper = SenderVideoLoggingHelpers(make_owner(db))
    helper.finalize_video_failure_sync(delivery_id=1, rule_id=2, post_id=3, source_channel="src", target_id="dst", target_thread_id=None, source_message_id=10, error_text="boom", fallback_mode="x", caption_delivery_mode=None, selected_mode="video", caption_requires_premium=False)
    event = db.video_events[0]
    assert event["event_type"] == "video_processing_failed"
    assert event["status"] == "faulty"
    assert event["error_text"] == "boom"
    assert event["extra"] == {"source_channel": "src", "target_id": "dst", "target_thread_id": None, "source_message_id": 10, "fallback_mode": "x", "selected_mode": "video", "caption_requires_premium": False}
    assert db.faulty == [(1, "boom")]


def test_finalize_video_success_sync_smoke():
    db = FakeDb()
    helper = SenderVideoLoggingHelpers(make_owner(db))
    helper.finalize_video_success_sync(delivery_id=1, rule_id=2, post_id=3, source_channel="src", target_id="dst", target_thread_id=4, source_message_id=10, sent_message_id=50, fallback_mode="none", caption_delivery_mode="copy", selected_mode="botapi", caption_requires_premium=False, candidate_sent_message_ids=[50, "51", "bad"], valid_sent_message_ids=[50])
    event = db.video_events[0]
    assert event["event_type"] == "video_processing_completed"
    assert event["status"] == "sent"
    assert event["extra"]["sent_message_id"] == 50
    assert event["extra"]["candidate_sent_message_ids"] == [50, 51]
    assert event["extra"]["valid_sent_message_ids"] == [50]
    assert db.sent == [1]


def test_finalize_video_success_sync_optional_ids_default_to_empty():
    db = FakeDb()
    helper = SenderVideoLoggingHelpers(make_owner(db))
    helper.finalize_video_success_sync(delivery_id=1, rule_id=2, post_id=3, source_channel="src", target_id="dst", target_thread_id=None, source_message_id=10, sent_message_id=None, fallback_mode="none", caption_delivery_mode="copy", selected_mode="botapi", caption_requires_premium=False)
    event = db.video_events[0]
    assert event["extra"]["candidate_sent_message_ids"] == []
    assert event["extra"]["valid_sent_message_ids"] == []


def test_stage_name_ru_mapping_smoke():
    helper = SenderVideoLoggingHelpers(make_owner())
    assert helper.stage_name_ru("download") == "скачивание"
    assert helper.stage_name_ru("send") == "отправка"
    assert helper.stage_name_ru("unknown_stage") == "unknown_stage"
    assert helper.stage_name_ru(None) == "неизвестный этап"


def test_log_human_video_event_smoke(caplog):
    helper = SenderVideoLoggingHelpers(make_owner())
    with caplog.at_level(logging.INFO, logger="forwarder"):
        helper.log_human_video_event(event_type="video_stage_started", extra={"stage": "download"})
        helper.log_human_video_event(event_type="video_stage_completed", extra={"stage": "download", "file_size_mb": 1.25})
        helper.log_human_video_event(event_type="video_stage_completed", extra={"stage": "trim"})
        helper.log_human_video_event(event_type="video_stage_failed", error_text="boom", extra={"stage": "probe"})
        helper.log_human_video_event(event_type="video_ffmpeg_progress", extra={"stage": "trim", "percent": 50})
        helper.log_human_video_event(event_type="video_send_retry", extra={"attempt": 2, "max_retries": 3})

    text = caplog.text
    assert "▶️ Начат этап" in text
    assert "✅ Скачивание завершено" in text
    assert "✅ Завершён этап" in text
    assert "❌ Ошибка на этапе" in text
    assert "🎬" in text
    assert "🔁 Повторная попытка отправки" in text
