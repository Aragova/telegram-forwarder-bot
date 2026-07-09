from __future__ import annotations

import asyncio
import importlib
import logging
from pathlib import Path
from types import SimpleNamespace


class FakeGetMessagesTelethon:
    def __init__(self, *, exc: Exception | None = None):
        self.calls = []
        self.exc = exc

    async def get_messages(self, entity, ids):
        self.calls.append((entity, ids))
        if self.exc:
            raise self.exc
        return {"ok": True}


class FakeDownloadTelethon:
    def __init__(self, *, mode: str):
        self.mode = mode
        self.calls = []

    async def download_media(self, message, file, progress_callback):
        self.calls.append((message, file, progress_callback))
        if self.mode == "exception":
            raise RuntimeError("download boom")
        if self.mode == "none":
            return None
        path = Path(file)
        if self.mode == "empty":
            path.write_bytes(b"")
        else:
            progress_callback(100, 100)
            path.write_bytes(b"video-bytes")
        return str(path)


def _owner(telethon):
    scheduled_events = []
    logged_events = []

    def _schedule_video_event_log(**kwargs):
        scheduled_events.append(kwargs)

    def _log_video_event_sync(**kwargs):
        logged_events.append(kwargs)

    return SimpleNamespace(
        telethon=telethon,
        scheduled_events=scheduled_events,
        logged_events=logged_events,
        _schedule_video_event_log=_schedule_video_event_log,
        _log_video_event_sync=_log_video_event_sync,
    )


def test_sender_fetch_download_helpers_extracted_from_sender():
    sender_source = Path("app/sender.py").read_text(encoding="utf-8")
    helper_source = Path("app/sender_fetch_download_helpers.py").read_text(encoding="utf-8")

    assert "def fetch_message" in helper_source
    assert "def download_video_source" in helper_source

    assert sender_source.count("def _fetch_message") == 1
    assert sender_source.count("def _download_video_source") == 1


def test_sender_fetch_download_helpers_do_not_import_sender():
    source = Path("app/sender_fetch_download_helpers.py").read_text(encoding="utf-8")

    forbidden = [
        "from ." + "sender import",
        "import app." + "sender",
        "import ." + "sender",
    ]

    for item in forbidden:
        assert item not in source


def test_sender_wrapper_delegates_fetch_message(monkeypatch):
    helper_module = importlib.import_module("app.sender_fetch_download_helpers")
    SenderService = importlib.import_module("app." + "sender").SenderService

    calls = []

    class FakeHelpers:
        def __init__(self, owner):
            calls.append(("init", owner))

        async def fetch_message(self, source_channel, message_id):
            calls.append(("fetch_message", source_channel, message_id))
            return {"ok": "fetch"}

    monkeypatch.setattr(helper_module, "SenderFetchDownloadHelpers", FakeHelpers)
    service = SenderService.__new__(SenderService)

    result = asyncio.run(service._fetch_message("src", 55))

    assert result == {"ok": "fetch"}
    assert calls == [("init", service), ("fetch_message", "src", 55)]


def test_sender_wrapper_delegates_download_video_source(monkeypatch):
    helper_module = importlib.import_module("app.sender_fetch_download_helpers")
    SenderService = importlib.import_module("app." + "sender").SenderService

    calls = []

    class FakeHelpers:
        def __init__(self, owner):
            calls.append(("init", owner))

        async def download_video_source(self, message, **kwargs):
            calls.append(("download_video_source", message, kwargs))
            return Path("/tmp/video.mp4")

    monkeypatch.setattr(helper_module, "SenderFetchDownloadHelpers", FakeHelpers)
    service = SenderService.__new__(SenderService)
    message = SimpleNamespace(id=55)

    result = asyncio.run(
        service._download_video_source(
            message,
            delivery_id=1,
            rule_id=2,
            post_id=3,
            source_channel="src",
            target_id="dst",
            source_message_id=55,
        )
    )

    assert result == Path("/tmp/video.mp4")
    assert calls == [
        (
            "init",
            service,
        ),
        (
            "download_video_source",
            message,
            {
                "delivery_id": 1,
                "rule_id": 2,
                "post_id": 3,
                "source_channel": "src",
                "target_id": "dst",
                "source_message_id": 55,
            },
        ),
    ]


def test_fetch_message_numeric_channel_smoke():
    SenderFetchDownloadHelpers = importlib.import_module("app." + "sender_fetch_download_helpers").SenderFetchDownloadHelpers

    telethon = FakeGetMessagesTelethon()
    result = asyncio.run(
        SenderFetchDownloadHelpers(SimpleNamespace(telethon=telethon)).fetch_message("-100123", 55)
    )

    assert result == {"ok": True}
    assert telethon.calls == [(-100123, 55)]


def test_fetch_message_non_numeric_channel_smoke():
    SenderFetchDownloadHelpers = importlib.import_module("app." + "sender_fetch_download_helpers").SenderFetchDownloadHelpers

    telethon = FakeGetMessagesTelethon()
    result = asyncio.run(
        SenderFetchDownloadHelpers(SimpleNamespace(telethon=telethon)).fetch_message("@source_channel", 55)
    )

    assert result == {"ok": True}
    assert telethon.calls == [("@source_channel", 55)]


def test_fetch_message_exception_smoke(caplog):
    SenderFetchDownloadHelpers = importlib.import_module("app." + "sender_fetch_download_helpers").SenderFetchDownloadHelpers

    telethon = FakeGetMessagesTelethon(exc=RuntimeError("boom"))

    with caplog.at_level(logging.WARNING, logger="forwarder"):
        result = asyncio.run(
            SenderFetchDownloadHelpers(SimpleNamespace(telethon=telethon)).fetch_message("@source_channel", 55)
        )

    assert result is None
    assert "Telethon не смог получить сообщение" in caplog.text


def test_download_video_source_success_smoke(monkeypatch, tmp_path):
    helper_module = importlib.import_module("app." + "sender_fetch_download_helpers")
    SenderFetchDownloadHelpers = helper_module.SenderFetchDownloadHelpers

    async def fake_run_db(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    monkeypatch.setattr(helper_module, "run_db", fake_run_db)
    monkeypatch.setattr(helper_module.settings, "base_dir_raw", str(tmp_path))
    monkeypatch.setattr(helper_module.settings, "media_cache_dir", "cache")

    telethon = FakeDownloadTelethon(mode="success")
    owner = _owner(telethon)
    message = SimpleNamespace(id=55, file=SimpleNamespace(ext="mp4"))

    result = asyncio.run(
        SenderFetchDownloadHelpers(owner).download_video_source(
            message,
            delivery_id=1,
            rule_id=2,
            post_id=3,
            source_channel="src",
            target_id="dst",
            source_message_id=55,
        )
    )

    assert isinstance(result, Path)
    assert result.exists()
    assert telethon.calls[0][1] == str(result)
    assert any(event["event_type"] == "video_download_progress" for event in owner.scheduled_events)
    assert any(event["event_type"] == "video_download_completed" for event in owner.logged_events)
    assert owner.scheduled_events[-1]["extra"]["stage"] == "download"
    assert owner.logged_events[-1]["extra"]["stage"] == "download"


def test_download_video_source_no_path_smoke(monkeypatch, tmp_path, caplog):
    helper_module = importlib.import_module("app." + "sender_fetch_download_helpers")
    SenderFetchDownloadHelpers = helper_module.SenderFetchDownloadHelpers

    monkeypatch.setattr(helper_module.settings, "base_dir_raw", str(tmp_path))
    monkeypatch.setattr(helper_module.settings, "media_cache_dir", "cache")
    owner = _owner(FakeDownloadTelethon(mode="none"))

    with caplog.at_level(logging.WARNING, logger="forwarder"):
        result = asyncio.run(SenderFetchDownloadHelpers(owner).download_video_source(SimpleNamespace(id=55)))

    assert result is None
    assert "путь не получен" in caplog.text


def test_download_video_source_empty_file_smoke(monkeypatch, tmp_path, caplog):
    helper_module = importlib.import_module("app." + "sender_fetch_download_helpers")
    SenderFetchDownloadHelpers = helper_module.SenderFetchDownloadHelpers

    monkeypatch.setattr(helper_module.settings, "base_dir_raw", str(tmp_path))
    monkeypatch.setattr(helper_module.settings, "media_cache_dir", "cache")
    owner = _owner(FakeDownloadTelethon(mode="empty"))

    with caplog.at_level(logging.WARNING, logger="forwarder"):
        result = asyncio.run(SenderFetchDownloadHelpers(owner).download_video_source(SimpleNamespace(id=55)))

    created_path = Path(owner.telethon.calls[0][1])
    assert result is None
    assert not created_path.exists()
    assert "файл пустой" in caplog.text


def test_download_video_source_exception_smoke(monkeypatch, tmp_path):
    helper_module = importlib.import_module("app." + "sender_fetch_download_helpers")
    SenderFetchDownloadHelpers = helper_module.SenderFetchDownloadHelpers

    async def fake_run_db(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    monkeypatch.setattr(helper_module, "run_db", fake_run_db)
    monkeypatch.setattr(helper_module.settings, "base_dir_raw", str(tmp_path))
    monkeypatch.setattr(helper_module.settings, "media_cache_dir", "cache")
    owner = _owner(FakeDownloadTelethon(mode="exception"))

    result = asyncio.run(
        SenderFetchDownloadHelpers(owner).download_video_source(
            SimpleNamespace(id=55),
            delivery_id=1,
            rule_id=2,
            post_id=3,
            source_channel="src",
            target_id="dst",
            source_message_id=55,
        )
    )

    assert result is None
    assert any(event["event_type"] == "video_download_failed" for event in owner.logged_events)
    failed_event = owner.logged_events[-1]
    assert "download boom" in failed_event["error_text"]
    assert failed_event["extra"]["stage"] == "download"
