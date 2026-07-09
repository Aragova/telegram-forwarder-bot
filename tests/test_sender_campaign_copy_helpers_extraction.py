from __future__ import annotations

import asyncio
import importlib
from pathlib import Path
from types import SimpleNamespace


class FakeBot:
    def __init__(self, *, copy_message_id=444, delete_errors=None):
        self.copy_message_id = copy_message_id
        self.delete_errors = delete_errors or {}
        self.copy_message_calls = []
        self.delete_message_calls = []

    async def copy_message(self, **kwargs):
        self.copy_message_calls.append(kwargs)
        return SimpleNamespace(message_id=self.copy_message_id)

    async def delete_message(self, **kwargs):
        self.delete_message_calls.append(kwargs)
        error = self.delete_errors.get(kwargs.get("message_id"))
        if error:
            raise error
        return True


class FakeDb:
    def __init__(self, *, copy_row=None, delivery=None, rule=None):
        self.copy_row = copy_row
        self.delivery = delivery
        self.rule = rule or SimpleNamespace(repost_campaign_show_seconds=0)
        self.calls = []

    def get_delivery_campaign_copy(self, copy_id):
        self.calls.append(("get_delivery_campaign_copy", copy_id))
        return self.copy_row

    def get_delivery(self, delivery_id):
        self.calls.append(("get_delivery", delivery_id))
        return self.delivery

    def get_rule(self, rule_id):
        self.calls.append(("get_rule", rule_id))
        return self.rule

    def mark_delivery_campaign_copy_send_failed(self, copy_id, text):
        self.calls.append(("mark_delivery_campaign_copy_send_failed", copy_id, text))

    def mark_delivery_campaign_copy_processing(self, copy_id):
        self.calls.append(("mark_delivery_campaign_copy_processing", copy_id))

    def mark_delivery_campaign_copy_sent(self, copy_id, **kwargs):
        self.calls.append(("mark_delivery_campaign_copy_sent", copy_id, kwargs))

    def mark_delivery_campaign_copy_delete_skipped(self, copy_id, text):
        self.calls.append(("mark_delivery_campaign_copy_delete_skipped", copy_id, text))

    def mark_delivery_campaign_copy_delete_processing(self, copy_id):
        self.calls.append(("mark_delivery_campaign_copy_delete_processing", copy_id))

    def mark_delivery_campaign_copy_delete_failed(self, copy_id, text):
        self.calls.append(("mark_delivery_campaign_copy_delete_failed", copy_id, text))

    def mark_delivery_campaign_copy_deleted(self, copy_id):
        self.calls.append(("mark_delivery_campaign_copy_deleted", copy_id))


def _helper(owner):
    return importlib.import_module("app.sender_campaign_copy_helpers").SenderCampaignCopyHelpers(owner)


def test_sender_campaign_copy_helpers_extracted_from_sender():
    sender_source = Path("app/sender.py").read_text(encoding="utf-8")
    helper_source = Path("app/sender_campaign_copy_helpers.py").read_text(encoding="utf-8")

    assert "def execute_send_copy_from_job" in helper_source
    assert "def execute_delete_copy_from_job" in helper_source

    assert sender_source.count("def execute_repost_campaign_send_copy_from_job") == 1
    assert sender_source.count("def execute_repost_campaign_delete_copy_from_job") == 1


def test_sender_campaign_copy_helpers_do_not_import_sender():
    source = Path("app/sender_campaign_copy_helpers.py").read_text(encoding="utf-8")

    forbidden = [
        "from ." + "sender import",
        "import app." + "sender",
        "import ." + "sender",
    ]

    for item in forbidden:
        assert item not in source


def test_sender_wrapper_delegates_repost_campaign_send_copy_from_job(monkeypatch):
    helper_module = importlib.import_module("app.sender_campaign_copy_helpers")
    SenderService = importlib.import_module("app.sender").SenderService
    calls = []

    class FakeHelpers:
        def __init__(self, owner):
            calls.append(("init", owner))

        async def execute_send_copy_from_job(self, **kwargs):
            calls.append(("send", kwargs))
            return {"ok": "send"}

    monkeypatch.setattr(helper_module, "SenderCampaignCopyHelpers", FakeHelpers)
    service = SenderService.__new__(SenderService)

    result = asyncio.run(service.execute_repost_campaign_send_copy_from_job(copy_id=7, extra="x"))

    assert result == {"ok": "send"}
    assert calls == [("init", service), ("send", {"copy_id": 7, "extra": "x"})]


def test_sender_wrapper_delegates_repost_campaign_delete_copy_from_job(monkeypatch):
    helper_module = importlib.import_module("app.sender_campaign_copy_helpers")
    SenderService = importlib.import_module("app.sender").SenderService
    calls = []

    class FakeHelpers:
        def __init__(self, owner):
            calls.append(("init", owner))

        async def execute_delete_copy_from_job(self, **kwargs):
            calls.append(("delete", kwargs))
            return {"ok": "delete"}

    monkeypatch.setattr(helper_module, "SenderCampaignCopyHelpers", FakeHelpers)
    service = SenderService.__new__(SenderService)

    result = asyncio.run(service.execute_repost_campaign_delete_copy_from_job(copy_id=8, extra="y"))

    assert result == {"ok": "delete"}
    assert calls == [("init", service), ("delete", {"copy_id": 8, "extra": "y"})]


def test_send_copy_missing_copy_row():
    owner = SimpleNamespace(db=FakeDb(copy_row=None), bot=FakeBot())
    result = asyncio.run(_helper(owner).execute_send_copy_from_job(copy_id=1))
    assert result == {"ok": False, "retryable": False, "error_text": "Копия кампании не найдена"}


def test_send_copy_already_sent():
    owner = SimpleNamespace(db=FakeDb(copy_row={"send_status": "sent"}), bot=FakeBot())
    result = asyncio.run(_helper(owner).execute_send_copy_from_job(copy_id=1))
    assert result == {"ok": True, "already_sent": True}


def test_send_copy_missing_delivery_marks_failed():
    db = FakeDb(copy_row={"send_status": "pending", "delivery_id": 123}, delivery=None)
    owner = SimpleNamespace(db=db, bot=FakeBot())
    result = asyncio.run(_helper(owner).execute_send_copy_from_job(copy_id=5))
    assert ("mark_delivery_campaign_copy_send_failed", 5, "Delivery не найден") in db.calls
    assert result == {"ok": False, "retryable": False, "error_text": "Delivery не найден"}


def test_send_copy_album_unsupported():
    db = FakeDb(copy_row={"send_status": "pending", "delivery_id": 123}, delivery={"delivery_method": "album"})
    owner = SimpleNamespace(db=db, bot=FakeBot())
    result = asyncio.run(_helper(owner).execute_send_copy_from_job(copy_id=5))
    assert result == {"ok": False, "retryable": False, "error_text": "Кампании для альбомов пока не поддерживаются в MVP"}


def test_send_copy_success_smoke(monkeypatch):
    fixed_iso = "2026-07-09T00:01:00+00:00"
    repost_campaign_service = importlib.import_module("app.repost_campaign_service")
    job_service = importlib.import_module("app.job_service")
    enqueue_calls = []

    monkeypatch.setattr(repost_campaign_service, "build_campaign_delete_after_iso", lambda show_seconds: fixed_iso)

    def fake_enqueue(db, copy_id, *, run_at):
        enqueue_calls.append((db, copy_id, run_at))

    monkeypatch.setattr(job_service, "enqueue_repost_campaign_delete_copy", fake_enqueue)

    copy_row = {"send_status": "pending", "delivery_id": 11, "rule_id": 22, "target_id": "-100target"}
    delivery = {"source_channel": "-100source", "message_id": 333, "delivery_method": "single"}
    db = FakeDb(copy_row=copy_row, delivery=delivery, rule=SimpleNamespace(repost_campaign_show_seconds=60))
    bot = FakeBot(copy_message_id=444)
    owner = SimpleNamespace(db=db, bot=bot)

    result = asyncio.run(_helper(owner).execute_send_copy_from_job(copy_id=9))

    assert bot.copy_message_calls == [{"chat_id": "-100target", "from_chat_id": "-100source", "message_id": 333}]
    assert ("mark_delivery_campaign_copy_processing", 9) in db.calls
    assert ("mark_delivery_campaign_copy_sent", 9, {"sent_message_id": 444, "sent_message_ids": [444], "delivery_method": "copy_single", "delete_after_at": fixed_iso}) in db.calls
    assert enqueue_calls == [(db, 9, fixed_iso)]
    assert result == {"ok": True, "copy_id": 9, "sent_message_ids": [444]}


def test_delete_copy_missing_copy_row():
    owner = SimpleNamespace(db=FakeDb(copy_row=None), bot=FakeBot())
    result = asyncio.run(_helper(owner).execute_delete_copy_from_job(copy_id=1))
    assert result == {"ok": False, "retryable": False, "error_text": "Копия кампании не найдена"}


import pytest


@pytest.mark.parametrize("status", ["deleted", "skipped"])
def test_delete_copy_already_deleted_or_skipped(status):
    owner = SimpleNamespace(db=FakeDb(copy_row={"delete_status": status}), bot=FakeBot())
    result = asyncio.run(_helper(owner).execute_delete_copy_from_job(copy_id=1))
    assert result == {"ok": True, "already_done": True}


def test_delete_copy_no_message_ids():
    db = FakeDb(copy_row={"delete_status": "pending", "sent_message_ids": [], "sent_message_id": None})
    owner = SimpleNamespace(db=db, bot=FakeBot())
    result = asyncio.run(_helper(owner).execute_delete_copy_from_job(copy_id=6))
    assert ("mark_delivery_campaign_copy_delete_skipped", 6, "Нет message_id для удаления") in db.calls
    assert result == {"ok": True}


def test_delete_copy_success_smoke():
    db = FakeDb(copy_row={"delete_status": "pending", "sent_message_ids": [10, 11], "target_id": "-100target"})
    bot = FakeBot()
    owner = SimpleNamespace(db=db, bot=bot)
    result = asyncio.run(_helper(owner).execute_delete_copy_from_job(copy_id=6))
    assert bot.delete_message_calls == [{"chat_id": "-100target", "message_id": 10}, {"chat_id": "-100target", "message_id": 11}]
    assert ("mark_delivery_campaign_copy_delete_processing", 6) in db.calls
    assert ("mark_delivery_campaign_copy_deleted", 6) in db.calls
    assert result == {"ok": True}


def test_delete_copy_not_found_is_ignored():
    db = FakeDb(copy_row={"delete_status": "pending", "sent_message_ids": [10, 11], "target_id": "-100target"})
    bot = FakeBot(delete_errors={10: RuntimeError("message to delete not found")})
    owner = SimpleNamespace(db=db, bot=bot)
    result = asyncio.run(_helper(owner).execute_delete_copy_from_job(copy_id=6))
    assert result == {"ok": True}
    assert ("mark_delivery_campaign_copy_deleted", 6) in db.calls
    assert not any(call[0] == "mark_delivery_campaign_copy_delete_failed" for call in db.calls)


def test_delete_copy_retry_after_failure():
    db = FakeDb(copy_row={"delete_status": "pending", "sent_message_ids": [10], "target_id": "-100target"})
    owner = SimpleNamespace(db=db, bot=FakeBot(delete_errors={10: RuntimeError("Retry after 10")}))
    result = asyncio.run(_helper(owner).execute_delete_copy_from_job(copy_id=6))
    assert ("mark_delivery_campaign_copy_delete_failed", 6, "Retry after 10") in db.calls
    assert result["ok"] is False
    assert result["retryable"] is True
    assert "Retry after 10" in result["error_text"]


def test_delete_copy_non_retryable_failure():
    db = FakeDb(copy_row={"delete_status": "pending", "sent_message_ids": [10], "target_id": "-100target"})
    owner = SimpleNamespace(db=db, bot=FakeBot(delete_errors={10: RuntimeError("Forbidden")}))
    result = asyncio.run(_helper(owner).execute_delete_copy_from_job(copy_id=6))
    assert ("mark_delivery_campaign_copy_delete_failed", 6, "Forbidden") in db.calls
    assert result["ok"] is False
    assert result["retryable"] is False
    assert "Forbidden" in result["error_text"]
