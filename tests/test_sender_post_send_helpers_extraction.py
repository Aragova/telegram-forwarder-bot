from __future__ import annotations

import asyncio
import importlib
import logging
from pathlib import Path
from types import SimpleNamespace


def _helper(owner=None):
    module = importlib.import_module("app.sender_post_send_helpers")
    return module.SenderPostSendHelpers(owner or SimpleNamespace(telethon=SimpleNamespace(get_messages=lambda *a, **k: None)))


def test_sender_post_send_helpers_extracted_from_sender():
    sender_source = Path("app/sender.py").read_text(encoding="utf-8")
    helper_source = Path("app/sender_post_send_helpers.py").read_text(encoding="utf-8")

    assert "def extract_sent_message_id" in helper_source
    assert "def extract_sent_message_ids" in helper_source
    assert "def validate_reaction_target_message" in helper_source
    assert "def validate_sent_message_ids_for_delivery" in helper_source
    assert "def confirm_target_delivery_message_ids" in helper_source
    assert "def confirm_target_delivery_message_ids_with_retry" in helper_source
    assert "def run_post_send_step_safe" in helper_source

    assert sender_source.count("def _extract_sent_message_id(") == 1
    assert sender_source.count("def _extract_sent_message_ids") == 1
    assert sender_source.count("def _validate_reaction_target_message") == 1
    assert sender_source.count("def _validate_sent_message_ids_for_delivery") == 1
    assert sender_source.count("def _confirm_target_delivery_message_ids(") == 1
    assert sender_source.count("def _confirm_target_delivery_message_ids_with_retry") == 1
    assert sender_source.count("def _run_post_send_step_safe") == 1


def test_sender_post_send_helpers_do_not_import_sender():
    source = Path("app/sender_post_send_helpers.py").read_text(encoding="utf-8")

    forbidden = [
        "from ." + "sender import",
        "import app." + "sender",
        "import ." + "sender",
    ]

    for item in forbidden:
        assert item not in source


def _patch_helper(monkeypatch, methods):
    helper_module = importlib.import_module("app.sender_post_send_helpers")
    calls = []

    class FakeHelpers:
        def __init__(self, owner):
            calls.append(("init", owner))

    for name, value in methods.items():
        setattr(FakeHelpers, name, value)

    monkeypatch.setattr(helper_module, "SenderPostSendHelpers", FakeHelpers)
    SenderService = importlib.import_module("app.sender").SenderService
    return SenderService.__new__(SenderService), calls


def test_sender_wrapper_delegates_extract_sent_message_id(monkeypatch):
    def extract_sent_message_id(self, sent_msg):
        calls.append(("extract_one", sent_msg))
        return 42

    service, calls = _patch_helper(monkeypatch, {"extract_sent_message_id": extract_sent_message_id})

    assert service._extract_sent_message_id({"message_id": 1}) == 42
    assert calls == [("init", service), ("extract_one", {"message_id": 1})]


def test_sender_wrapper_delegates_extract_sent_message_ids(monkeypatch):
    def extract_sent_message_ids(self, sent_result):
        calls.append(("extract_many", sent_result))
        return [1, 2]

    service, calls = _patch_helper(monkeypatch, {"extract_sent_message_ids": extract_sent_message_ids})

    assert service._extract_sent_message_ids([1]) == [1, 2]
    assert calls == [("init", service), ("extract_many", [1])]


def test_sender_wrapper_delegates_validate_reaction_target_message(monkeypatch):
    async def validate_reaction_target_message(self, **kwargs):
        calls.append(("validate_reaction", kwargs))
        return 9

    service, calls = _patch_helper(monkeypatch, {"validate_reaction_target_message": validate_reaction_target_message})

    result = asyncio.run(service._validate_reaction_target_message(rule_id=1, source_channel="s", target_id="t", source_message_ids=[2], sent_message_id=3, delivery_id=4, max_age_seconds=5))

    assert result == 9
    assert calls == [("init", service), ("validate_reaction", {"rule_id": 1, "source_channel": "s", "target_id": "t", "source_message_ids": [2], "sent_message_id": 3, "delivery_id": 4, "max_age_seconds": 5})]


def test_sender_wrapper_delegates_validate_sent_message_ids_for_delivery(monkeypatch):
    async def validate_sent_message_ids_for_delivery(self, **kwargs):
        calls.append(("validate_ids", kwargs))
        return [10]

    service, calls = _patch_helper(monkeypatch, {"validate_sent_message_ids_for_delivery": validate_sent_message_ids_for_delivery})

    result = asyncio.run(service._validate_sent_message_ids_for_delivery(rule_id=1, delivery_id=2, source_channel="s", target_id="t", source_message_ids=[3], candidate_sent_message_ids=[4], method="m", max_age_seconds=6))

    assert result == [10]
    assert calls == [("init", service), ("validate_ids", {"rule_id": 1, "delivery_id": 2, "source_channel": "s", "target_id": "t", "source_message_ids": [3], "candidate_sent_message_ids": [4], "method": "m", "max_age_seconds": 6})]


def test_sender_wrapper_delegates_confirm_target_delivery_message_ids(monkeypatch):
    async def confirm_target_delivery_message_ids(self, **kwargs):
        calls.append(("confirm", kwargs))
        return [44]

    service, calls = _patch_helper(monkeypatch, {"confirm_target_delivery_message_ids": confirm_target_delivery_message_ids})

    result = asyncio.run(service._confirm_target_delivery_message_ids(rule_id=1, delivery_id=2, source_channel="s", target_id="t", source_message_ids=[3], candidate_sent_message_ids=[4], method="m", max_age_seconds=6))

    assert result == [44]
    assert calls == [("init", service), ("confirm", {"rule_id": 1, "delivery_id": 2, "source_channel": "s", "target_id": "t", "source_message_ids": [3], "candidate_sent_message_ids": [4], "method": "m", "max_age_seconds": 6})]


def test_sender_wrapper_delegates_confirm_target_delivery_message_ids_with_retry(monkeypatch):
    async def confirm_target_delivery_message_ids_with_retry(self, **kwargs):
        calls.append(("retry", kwargs))
        return [77]

    service, calls = _patch_helper(monkeypatch, {"confirm_target_delivery_message_ids_with_retry": confirm_target_delivery_message_ids_with_retry})

    result = asyncio.run(service._confirm_target_delivery_message_ids_with_retry(rule_id=1, extra="x"))

    assert result == [77]
    assert calls == [("init", service), ("retry", {"rule_id": 1, "extra": "x"})]


def test_sender_wrapper_delegates_run_post_send_step_safe(monkeypatch):
    async def run_post_send_step_safe(self, **kwargs):
        calls.append(("safe", kwargs))
        return {"ok": True}

    service, calls = _patch_helper(monkeypatch, {"run_post_send_step_safe": run_post_send_step_safe})

    async def coro():
        return None

    result = asyncio.run(service._run_post_send_step_safe(step_name="s", rule_id=1, delivery_id=2, idempotency_key="k", accepted_sent_message_ids=[3], coro_factory=coro))

    assert result == {"ok": True}
    assert calls == [("init", service), ("safe", {"step_name": "s", "rule_id": 1, "delivery_id": 2, "idempotency_key": "k", "accepted_sent_message_ids": [3], "coro_factory": coro})]


def test_extract_sent_message_ids_smoke_cases():
    helper = _helper()
    cases = [
        (None, []),
        (SimpleNamespace(message_id=123), [123]),
        (SimpleNamespace(id=124), [124]),
        ({"message_id": 125}, [125]),
        ({"id": 126}, [126]),
        ({"message": {"message_id": 127}}, [127]),
        ({"result": {"id": 128}}, [128]),
        ({"data": {"message_id": 129}}, [129]),
        ([SimpleNamespace(message_id=130), {"message_id": 131}], [130, 131]),
        ((SimpleNamespace(message_id=132), {"id": 133}), [132, 133]),
    ]

    for value, expected in cases:
        assert helper.extract_sent_message_ids(value) == expected


def test_extract_sent_message_id_returns_first_id():
    helper = _helper()

    assert helper.extract_sent_message_id([{"message_id": 10}, {"message_id": 11}]) == 10


def test_validate_sent_message_ids_for_delivery_success_smoke(monkeypatch):
    helper = _helper()

    async def validate_reaction_target_message(**kwargs):
        sent_message_id = kwargs["sent_message_id"]
        return sent_message_id if sent_message_id in {10, 12} else None

    monkeypatch.setattr(helper, "validate_reaction_target_message", validate_reaction_target_message)

    result = asyncio.run(helper.validate_sent_message_ids_for_delivery(rule_id=1, delivery_id=2, source_channel="s", target_id="t", source_message_ids=[3], candidate_sent_message_ids=["10", "bad", 11, 12], method="m"))

    assert result == [10, 12]


def test_validate_sent_message_ids_for_delivery_empty_candidates(caplog):
    helper = _helper()
    caplog.set_level(logging.WARNING, logger="forwarder")

    result = asyncio.run(helper.validate_sent_message_ids_for_delivery(rule_id=1, delivery_id=2, source_channel="s", target_id="t", source_message_ids=[3], candidate_sent_message_ids=[], method="m"))

    assert result == []
    assert "DELIVERY_SENT_MESSAGE_IDS_VALIDATE_EMPTY" in caplog.text
    assert "no_candidate_ids" in caplog.text


def test_confirm_target_delivery_message_ids_no_get_messages(caplog):
    helper = _helper(SimpleNamespace(telethon=SimpleNamespace()))
    caplog.set_level(logging.WARNING, logger="forwarder")

    result = asyncio.run(helper.confirm_target_delivery_message_ids(rule_id=1, delivery_id=2, source_channel="s", target_id="t", source_message_ids=[3], candidate_sent_message_ids=["40", "bad", 41], method="m"))

    assert result == [40, 41]
    assert "DELIVERY_TARGET_CONFIRM_SKIPPED_NO_GET_MESSAGES" in caplog.text


def test_confirm_target_delivery_message_ids_no_candidates(caplog):
    helper = _helper()
    caplog.set_level(logging.WARNING, logger="forwarder")

    result = asyncio.run(helper.confirm_target_delivery_message_ids(rule_id=1, delivery_id=2, source_channel="s", target_id="t", source_message_ids=[3], candidate_sent_message_ids=[], method="m"))

    assert result == []
    assert "DELIVERY_TARGET_CONFIRM_FAILED" in caplog.text
    assert "no_candidate_ids" in caplog.text


def test_confirm_target_delivery_message_ids_valid_ids(monkeypatch, caplog):
    helper = _helper()
    caplog.set_level(logging.INFO, logger="forwarder")

    async def validate_sent_message_ids_for_delivery(**kwargs):
        return [44]

    monkeypatch.setattr(helper, "validate_sent_message_ids_for_delivery", validate_sent_message_ids_for_delivery)

    result = asyncio.run(helper.confirm_target_delivery_message_ids(rule_id=1, delivery_id=2, source_channel="s", target_id="t", source_message_ids=[3], candidate_sent_message_ids=[44], method="m"))

    assert result == [44]
    assert "DELIVERY_TARGET_CONFIRM_OK" in caplog.text


def test_confirm_target_delivery_message_ids_with_retry_retries_then_succeeds(monkeypatch, caplog):
    helper = _helper()
    calls = []
    caplog.set_level(logging.INFO, logger="forwarder")

    async def confirm_target_delivery_message_ids(**kwargs):
        calls.append(kwargs)
        return [] if len(calls) == 1 else [77]

    async def fake_sleep(delay):
        calls.append(("sleep", delay))

    monkeypatch.setattr(helper, "confirm_target_delivery_message_ids", confirm_target_delivery_message_ids)
    monkeypatch.setattr(importlib.import_module("app.sender_post_send_helpers").asyncio, "sleep", fake_sleep)

    result = asyncio.run(helper.confirm_target_delivery_message_ids_with_retry(rule_id=1, delivery_id=2, candidate_sent_message_ids=[77]))

    assert result == [77]
    assert len([call for call in calls if isinstance(call, dict)]) == 2
    assert "DELIVERY_TARGET_CONFIRM_RETRY" in caplog.text


def test_run_post_send_step_safe_success():
    helper = _helper()

    async def coro():
        return {"done": True}

    result = asyncio.run(helper.run_post_send_step_safe(step_name="s", rule_id=1, delivery_id=2, coro_factory=coro))

    assert result == {"ok": True, "result": {"done": True}}


def test_run_post_send_step_safe_exception_is_non_fatal(caplog):
    helper = _helper()
    caplog.set_level(logging.WARNING, logger="forwarder")

    async def coro():
        raise RuntimeError("boom")

    result = asyncio.run(helper.run_post_send_step_safe(step_name="s", rule_id=1, delivery_id=2, coro_factory=coro))

    assert result == {"ok": False, "error": "boom"}
    assert "POST_SEND_STEP_FAILED_NON_FATAL" in caplog.text
