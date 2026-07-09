import asyncio

import pytest
from types import SimpleNamespace

from app.sender import SenderService
from app.sender_delivery_logging_helpers import SenderDeliveryLoggingHelpers


class FakeDb:
    def __init__(self):
        self.delivery_events = []
        self.faulty = []
        self.post_ids = {}

    def get_post_id_by_delivery(self, delivery_id):
        return self.post_ids.get(delivery_id)

    def log_delivery_event(self, **kwargs):
        self.delivery_events.append(kwargs)

    def mark_delivery_faulty(self, delivery_id, error_text):
        self.faulty.append((delivery_id, error_text))


def make_owner(db=None):
    return SimpleNamespace(db=db or FakeDb())


def test_sender_delivery_logging_helpers_extracted_from_sender():
    from pathlib import Path

    sender_source = Path("app/sender.py").read_text(encoding="utf-8")
    helper_source = Path("app/sender_delivery_logging_helpers.py").read_text(encoding="utf-8")

    assert "def serialize_pipeline_verify_result" in helper_source
    assert "def log_delivery_pipeline_step_sync" in helper_source
    assert "def log_delivery_pipeline_step" in helper_source
    assert "def log_delivery_final_success_sync" in helper_source
    assert "def log_delivery_final_success" in helper_source
    assert "def log_delivery_final_failure_sync" in helper_source
    assert "def log_delivery_final_failure" in helper_source

    assert sender_source.count("def _serialize_pipeline_verify_result") == 1
    assert sender_source.count("def _log_delivery_pipeline_step_sync") == 1
    assert sender_source.count("async def _log_delivery_pipeline_step") == 1
    assert sender_source.count("def _log_delivery_final_success_sync") == 1
    assert sender_source.count("async def _log_delivery_final_success") == 1
    assert sender_source.count("def _log_delivery_final_failure_sync") == 1
    assert sender_source.count("async def _log_delivery_final_failure") == 1


def test_sender_delivery_logging_helpers_do_not_import_sender():
    from pathlib import Path

    source = Path("app/sender_delivery_logging_helpers.py").read_text(encoding="utf-8")

    forbidden = [
        "from ." + "sender import",
        "import app." + "sender",
        "import ." + "sender",
    ]

    for item in forbidden:
        assert item not in source


class StubDeliveryHelpers:
    calls = []

    def __init__(self, owner):
        self.owner = owner

    def _record(self, name, **kwargs):
        self.calls.append((name, self.owner, kwargs))
        return {"name": name, "kwargs": kwargs}

    def serialize_pipeline_verify_result(self, verify_result):
        self.calls.append(("serialize_pipeline_verify_result", self.owner, {"verify_result": verify_result}))
        return {"ok": True}

    def log_delivery_pipeline_step_sync(self, **kwargs):
        return self._record("log_delivery_pipeline_step_sync", **kwargs)

    async def log_delivery_pipeline_step(self, **kwargs):
        return self._record("log_delivery_pipeline_step", **kwargs)

    def log_delivery_final_success_sync(self, **kwargs):
        return self._record("log_delivery_final_success_sync", **kwargs)

    async def log_delivery_final_success(self, **kwargs):
        return self._record("log_delivery_final_success", **kwargs)

    def log_delivery_final_failure_sync(self, **kwargs):
        return self._record("log_delivery_final_failure_sync", **kwargs)

    async def log_delivery_final_failure(self, **kwargs):
        return self._record("log_delivery_final_failure", **kwargs)


@pytest.fixture
def patched_delivery_helpers(monkeypatch):
    StubDeliveryHelpers.calls = []
    monkeypatch.setattr("app.sender_delivery_logging_helpers.SenderDeliveryLoggingHelpers", StubDeliveryHelpers)
    return StubDeliveryHelpers.calls


def test_sender_wrapper_delegates_serialize_pipeline_verify_result(patched_delivery_helpers):
    service = object.__new__(SenderService)
    assert service._serialize_pipeline_verify_result({"ok": True}) == {"ok": True}
    assert patched_delivery_helpers == [("serialize_pipeline_verify_result", service, {"verify_result": {"ok": True}})]


def test_sender_wrapper_delegates_log_delivery_pipeline_step_sync(patched_delivery_helpers):
    service = object.__new__(SenderService)
    kwargs = dict(rule_id=1, delivery_ids=[2], event_type="e", pipeline_stage="copy", pipeline_result="ok", source_channel="src", target_id="dst", source_message_ids=[3], error_text=None, extra={"x": 1})
    service._log_delivery_pipeline_step_sync(**kwargs)
    assert patched_delivery_helpers == [("log_delivery_pipeline_step_sync", service, kwargs)]


def test_sender_wrapper_delegates_log_delivery_pipeline_step(patched_delivery_helpers):
    service = object.__new__(SenderService)
    kwargs = dict(rule_id=1, delivery_ids=[2], event_type="e", pipeline_stage="copy", pipeline_result="ok", source_channel="src", target_id="dst", source_message_ids=[3], error_text="err", extra=None)
    asyncio.run(service._log_delivery_pipeline_step(**kwargs))
    assert patched_delivery_helpers == [("log_delivery_pipeline_step", service, kwargs)]


def test_sender_wrapper_delegates_log_delivery_final_success_sync(patched_delivery_helpers):
    service = object.__new__(SenderService)
    kwargs = dict(rule_id=1, delivery_ids=[2], final_method="copy", source_channel="src", target_id="dst", source_message_ids=[3], sent_message_id=4, sent_message_ids=[4], reaction_message_id=4, verify_result={"ok": True}, extra=None)
    service._log_delivery_final_success_sync(**kwargs)
    assert patched_delivery_helpers == [("log_delivery_final_success_sync", service, kwargs)]


def test_sender_wrapper_delegates_log_delivery_final_success(patched_delivery_helpers):
    service = object.__new__(SenderService)
    kwargs = dict(rule_id=1, delivery_ids=[2], final_method="copy", source_channel="src", target_id="dst", source_message_ids=[3], sent_message_id=None, sent_message_ids=None, reaction_message_id=None, verify_result=None, extra={"x": 1})
    asyncio.run(service._log_delivery_final_success(**kwargs))
    assert patched_delivery_helpers == [("log_delivery_final_success", service, kwargs)]


def test_sender_wrapper_delegates_log_delivery_final_failure_sync(patched_delivery_helpers):
    service = object.__new__(SenderService)
    kwargs = dict(rule_id=1, delivery_ids=[2], final_method="copy", source_channel="src", target_id="dst", source_message_ids=[3], error_text="err", attempts_debug=[], extra=None)
    service._log_delivery_final_failure_sync(**kwargs)
    assert patched_delivery_helpers == [("log_delivery_final_failure_sync", service, kwargs)]


def test_sender_wrapper_delegates_log_delivery_final_failure(patched_delivery_helpers):
    service = object.__new__(SenderService)
    kwargs = dict(rule_id=1, delivery_ids=[2], final_method="copy", source_channel="src", target_id="dst", source_message_ids=[3], error_text="err", attempts_debug=None, extra={"x": 1})
    asyncio.run(service._log_delivery_final_failure(**kwargs))
    assert patched_delivery_helpers == [("log_delivery_final_failure", service, kwargs)]


def test_serialize_pipeline_verify_result_smoke():
    helper = SenderDeliveryLoggingHelpers(make_owner())
    assert helper.serialize_pipeline_verify_result({"ok": True, "error_text": None, "grouped_id": "abc", "count": 2, "first_message_id": 100, "ignored": "x"}) == {"ok": True, "error_text": None, "grouped_id": "abc", "count": 2, "first_message_id": 100}
    assert helper.serialize_pipeline_verify_result(None) == {"ok": False, "error_text": None, "grouped_id": None, "count": None, "first_message_id": None}


def test_log_delivery_pipeline_step_sync_smoke():
    db = FakeDb(); db.post_ids = {1: 11, 2: 22}
    helper = SenderDeliveryLoggingHelpers(make_owner(db))
    helper.log_delivery_pipeline_step_sync(rule_id=10, delivery_ids=[1, 2], event_type="delivery_pipeline_step", pipeline_stage="copy", pipeline_result="ok", source_channel="-100src", target_id="-100dst", source_message_ids=[100], error_text=None, extra={"method": "copy_single"})
    assert len(db.delivery_events) == 2
    assert [e["post_id"] for e in db.delivery_events] == [11, 22]
    assert all(e["status"] == "processing" for e in db.delivery_events)
    assert all(e["event_type"] == "delivery_pipeline_step" for e in db.delivery_events)
    assert db.delivery_events[0]["extra"] | {} == {"pipeline_stage": "copy", "pipeline_result": "ok", "source_channel": "-100src", "target_id": "-100dst", "source_message_ids": [100], "method": "copy_single"}
    assert db.faulty == []


def test_log_delivery_pipeline_step_sync_warning_with_error_text(caplog):
    db = FakeDb()
    helper = SenderDeliveryLoggingHelpers(make_owner(db))
    with caplog.at_level("WARNING", logger="forwarder"):
        helper.log_delivery_pipeline_step_sync(rule_id=10, delivery_ids=[1], event_type="delivery_pipeline_step", pipeline_stage="copy", pipeline_result="failed", source_channel="-100src", target_id="-100dst", source_message_ids=[100], error_text="copy failed")
    assert db.delivery_events[0]["error_text"] == "copy failed"
    assert "ПРАВИЛО 10" in caplog.text
    assert "ШАГ copy → FAILED" in caplog.text


def test_log_delivery_pipeline_step_async_wrapper_smoke(monkeypatch):
    async def fake_run_db(fn, *args, **kwargs):
        return fn(*args, **kwargs)
    monkeypatch.setattr("app.sender_delivery_logging_helpers.run_db", fake_run_db)
    db = FakeDb()
    helper = SenderDeliveryLoggingHelpers(make_owner(db))
    asyncio.run(helper.log_delivery_pipeline_step(rule_id=10, delivery_ids=[1], event_type="delivery_pipeline_step", pipeline_stage="copy", pipeline_result="ok", source_channel="src", target_id="dst", source_message_ids=[100]))
    assert db.delivery_events[0]["status"] == "processing"


def test_log_delivery_final_success_sync_smoke():
    db = FakeDb()
    helper = SenderDeliveryLoggingHelpers(make_owner(db))
    helper.log_delivery_final_success_sync(rule_id=10, delivery_ids=[1, 2], final_method="copy_single", source_channel="-100src", target_id="-100dst", source_message_ids=[100], sent_message_id=200, sent_message_ids=[200], reaction_message_id=200, verify_result={"ok": True, "grouped_id": None, "count": 1, "first_message_id": 200}, extra={"custom": "value"})
    assert len(db.delivery_events) == 2
    event = db.delivery_events[0]
    assert event["event_type"] == "delivery_sent"
    assert event["status"] == "sent"
    assert db.faulty == []
    assert event["extra"]["final_method"] == "copy_single"
    assert event["extra"]["sent_message_id"] == 200
    assert event["extra"]["sent_message_ids"] == [200]
    assert event["extra"]["first_sent_message_id"] == 200
    assert event["extra"]["reaction_message_id"] == 200
    assert event["extra"]["verify_ok"] is True
    assert event["extra"]["verify_count"] == 1
    assert event["extra"]["verify_first_message_id"] == 200
    assert event["extra"]["custom"] == "value"


def test_log_delivery_final_success_sync_fallback_sent_ids():
    db = FakeDb()
    helper = SenderDeliveryLoggingHelpers(make_owner(db))
    kwargs = dict(rule_id=10, delivery_ids=[1], final_method="copy", source_channel="src", target_id="dst", source_message_ids=[100])
    helper.log_delivery_final_success_sync(**kwargs, sent_message_id=201, sent_message_ids=None, reaction_message_id=None)
    assert db.delivery_events[-1]["extra"]["sent_message_ids"] == [201]
    assert db.delivery_events[-1]["extra"]["reaction_message_id"] == 201
    helper.log_delivery_final_success_sync(**kwargs, sent_message_id=None, sent_message_ids=None, reaction_message_id=None)
    assert db.delivery_events[-1]["extra"]["sent_message_ids"] == []
    assert db.delivery_events[-1]["extra"]["reaction_message_id"] is None


def test_log_delivery_final_success_async_wrapper_smoke(monkeypatch):
    async def fake_run_db(fn, *args, **kwargs):
        return fn(*args, **kwargs)
    monkeypatch.setattr("app.sender_delivery_logging_helpers.run_db", fake_run_db)
    db = FakeDb()
    helper = SenderDeliveryLoggingHelpers(make_owner(db))
    asyncio.run(helper.log_delivery_final_success(rule_id=10, delivery_ids=[1], final_method="copy", source_channel="src", target_id="dst", source_message_ids=[100]))
    assert db.delivery_events[0]["event_type"] == "delivery_sent"
    assert db.delivery_events[0]["status"] == "sent"


def test_log_delivery_final_failure_sync_smoke(caplog):
    db = FakeDb()
    helper = SenderDeliveryLoggingHelpers(make_owner(db))
    with caplog.at_level("ERROR", logger="forwarder"):
        helper.log_delivery_final_failure_sync(rule_id=10, delivery_ids=[1, 2], final_method="reupload_single", source_channel="-100src", target_id="-100dst", source_message_ids=[100], error_text="final failed", attempts_debug=[{"method": "copy", "ok": False}], extra={"custom": "value"})
    assert len(db.delivery_events) == 2
    event = db.delivery_events[0]
    assert event["event_type"] == "delivery_failed"
    assert event["status"] == "faulty"
    assert event["error_text"] == "final failed"
    assert event["extra"]["final_method"] == "reupload_single"
    assert event["extra"]["attempts"] == [{"method": "copy", "ok": False}]
    assert event["extra"]["custom"] == "value"
    assert db.faulty == [(1, "final failed"), (2, "final failed")]
    assert "ИТОГ → ОШИБКА" in caplog.text


def test_log_delivery_final_failure_async_wrapper_smoke(monkeypatch):
    async def fake_run_db(fn, *args, **kwargs):
        return fn(*args, **kwargs)
    monkeypatch.setattr("app.sender_delivery_logging_helpers.run_db", fake_run_db)
    db = FakeDb()
    helper = SenderDeliveryLoggingHelpers(make_owner(db))
    asyncio.run(helper.log_delivery_final_failure(rule_id=10, delivery_ids=[1], final_method="copy", source_channel="src", target_id="dst", source_message_ids=[100], error_text="err"))
    assert db.delivery_events[0]["event_type"] == "delivery_failed"
    assert db.delivery_events[0]["status"] == "faulty"
    assert db.faulty == [(1, "err")]
