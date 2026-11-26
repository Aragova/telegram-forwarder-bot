"""Regression suite for the delivery gaps left after PR #522.

These tests simulate ambiguous Telegram responses; they never send to Telegram.
"""
from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

from app.postgres_repository import PostgresRepository
from app.sender_reupload_helpers import SenderReuploadHelpers
from app.sender_telethon_helpers import SenderTelethonHelpers
from app.telethon_authoritative_resolver import telethon_transport_failed
from app.video_pipeline_stages import VideoPipelineStages


class FakeBot:
    def __init__(self):
        self.calls = []

    async def send_message(self, **kwargs):
        self.calls.append(("text", kwargs))
        return SimpleNamespace(message_id=23)

    async def send_photo(self, **kwargs):
        self.calls.append(("photo", kwargs))
        return SimpleNamespace(message_id=24)

    async def send_video(self, **kwargs):
        self.calls.append(("video", kwargs))
        return SimpleNamespace(message_id=25)

    async def send_document(self, **kwargs):
        self.calls.append(("document", kwargs))
        return SimpleNamespace(message_id=26)

    async def send_media_group(self, **kwargs):
        self.calls.append(("album", kwargs))
        return [SimpleNamespace(message_id=27)]


def _owner(bot, *, text_outcome=None, file_outcome=None, album_outcome=None, video_path=None):
    async def send_text(**kwargs):
        return text_outcome

    async def send_file(**kwargs):
        return file_outcome

    async def send_album(**kwargs):
        return album_outcome

    class Downloader:
        async def download_media(self, *args, **kwargs):
            return str(video_path) if video_path else None

    return SimpleNamespace(
        bot=bot,
        telethon=Downloader(),
        _content_from_message_or_post=lambda **kwargs: {"text": "caption", "entities": []},
        _build_text_and_entities_from_content=lambda content: (content["text"], content["entities"]),
        _send_text_via_telethon=send_text,
        _send_file_via_telethon=send_file,
        _send_album_via_telethon=send_album,
    )


def test_text_unknown_mtproto_result_never_falls_through_to_botapi():
    bot = FakeBot()
    owner = _owner(bot, text_outcome=telethon_transport_failed("connection reset after send"))
    result = asyncio.run(SenderReuploadHelpers(owner).reupload_message(
        SimpleNamespace(media=None), "target", None,
    ))
    assert result.transport_attempted
    assert not result.transport_accepted
    assert bot.calls == []


def test_media_unknown_mtproto_result_never_falls_through_to_botapi(tmp_path):
    path = tmp_path / "video.mp4"
    path.write_bytes(b"not a real video - no actual telegram call")
    bot = FakeBot()
    owner = _owner(
        bot, video_path=path,
        file_outcome=telethon_transport_failed("timeout after send_file"),
    )
    result = asyncio.run(SenderReuploadHelpers(owner).reupload_message(
        SimpleNamespace(media=object(), video=True), "target", None,
    ))
    assert result.transport_attempted
    assert bot.calls == []
    assert path.exists()  # no download or cleanup on ambiguous original send


def test_album_unknown_mtproto_result_never_falls_through_to_botapi():
    bot = FakeBot()
    outcome = telethon_transport_failed("network reset").to_reupload_album_result(sent_count=0)
    owner = _owner(bot, album_outcome=outcome)
    result = asyncio.run(SenderReuploadHelpers(owner).reupload_album(
        [SimpleNamespace(media=object()), SimpleNamespace(media=object())],
        "target", None,
    ))
    assert result["transport_attempted"] is True
    assert bot.calls == []


def _real_telethon_owner():
    class Client:
        def __init__(self):
            self.send_count = 0

        async def get_messages(self, *args, **kwargs):
            return []

        async def send_file(self, **kwargs):
            self.send_count += 1
            raise ConnectionResetError("Telegram may have accepted this send")

    client = Client()
    owner = SimpleNamespace(
        telethon=client,
        _content_from_message_or_post=lambda **kwargs: {"text": "", "entities": []},
        _build_text_and_entities_from_content=lambda content: (content["text"], content["entities"]),
        _clone_telethon_entities=lambda entities, text: [],
    )
    sender = SenderTelethonHelpers(owner)

    async def peer(*args, **kwargs):
        return "target", False

    sender._resolve_target_entity_and_broadcast_flag = peer
    return client, sender


def test_telethon_file_no_second_send_after_exception(tmp_path):
    client, sender = _real_telethon_owner()
    media_file = tmp_path / "media.bin"
    media_file.write_bytes(b"sample")
    result = asyncio.run(sender.send_file_via_telethon(
        target_id="target", target_thread_id=None,
        message=SimpleNamespace(media=object(), id=10),
        file_path=media_file, force_document=True,
    ))
    assert client.send_count == 1
    assert result.transport_attempted
    assert "original_media_send_uncertain" in (result.error_text or "")


def test_telethon_album_no_second_send_after_exception():
    client, sender = _real_telethon_owner()
    messages = [SimpleNamespace(media=object(), id=10),
                SimpleNamespace(media=object(), id=11)]
    result = asyncio.run(sender.send_album_via_telethon(
        target_id="target", target_thread_id=None, messages=messages,
    ))
    assert client.send_count == 1
    assert result["transport_attempted"] is True
    assert "original_album_send_uncertain" in result["error_text"]


def test_self_loop_echo_guard_uses_verified_receipts_and_all_album_ids():
    import inspect

    source = inspect.getsource(PostgresRepository._is_self_target_echo_post_for_rule_conn)
    assert "d.sent_message_id = %s" in source
    assert "d.sent_message_ids_json" in source
    assert "a.status = 'verified'" in source
    assert "a.sent_message_ids_json" in source
    assert "a.target_id::text = r.target_id::text" in source
    assert "r.source_id::text = r.target_id::text" in source
    assert "r.source_thread_id IS NOT DISTINCT FROM r.target_thread_id" in source


def test_policy_skip_does_not_use_error_text_field():
    source = Path("app/repost_single_delivery.py").read_text(encoding="utf-8")
    album = Path("app/repost_album_delivery.py").read_text(encoding="utf-8")
    assert "pipeline_result=\"skipped\"" in source
    assert 'error_text=None,  # Policy skip is not a delivery failure.' in source
    assert 'error_text=None,  # Policy skip; the reason is retained in extra.' in album


def test_faulty_card_distinguishes_unverified_send_from_generic_failure():
    source = Path("bot.py").read_text(encoding="utf-8")
    assert 'error_type = "🧭 Отправка требует сверки с Telegram"' in source
    assert 'created_at_text == "дата неизвестна"' in source


def test_video_ffprobe_missing_is_transient_and_preserves_source(tmp_path, monkeypatch):
    path = tmp_path / "video.mp4"
    path.write_bytes(b"content")

    async def absent(*args, **kwargs):
        raise FileNotFoundError("ffprobe missing")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", absent)
    stages = VideoPipelineStages(owner=object())
    ok, reason = asyncio.run(stages.validate_mp4_file_for_pipeline(
        path, delivery_id=1, job_id=2, stage="download",
    ))
    assert ok is False
    assert str(reason).startswith("временная ошибка проверки видео:")
    assert path.exists()


def test_video_without_video_stream_is_corrupt(tmp_path, monkeypatch):
    from app import video_pipeline_stages as stages_mod

    path = tmp_path / "video.mp4"
    path.write_bytes(b"data")

    async def probe(*args, **kwargs):
        return SimpleNamespace(returncode=0)

    async def communicate(_process):
        output = {"format": {"duration": "1.0"}, "streams": [
            {"codec_type": "audio", "codec_name": "aac"},
        ]}
        return json.dumps(output).encode(), b""

    monkeypatch.setattr(asyncio, "create_subprocess_exec", probe)
    monkeypatch.setattr(stages_mod, "communicate_safely", communicate)
    ok, reason = asyncio.run(VideoPipelineStages(object()).validate_mp4_file_for_pipeline(
        path, delivery_id=3, job_id=4, stage="process",
    ))
    assert not ok
    assert "видеопоток отсутствует" in str(reason)
    assert not path.exists()


def test_video_download_transient_probe_error_does_not_mark_file_corrupt(tmp_path):
    path = tmp_path / "video.mp4"
    path.write_bytes(b"data")

    class Owner:
        async def _fetch_message(self, *_):
            return SimpleNamespace(id=1)

        async def _download_video_source(self, *args, **kwargs):
            return path

        async def _validate_mp4_file_for_pipeline(self, *args, **kwargs):
            return False, "временная ошибка проверки видео: FileNotFoundError"

    result = asyncio.run(VideoPipelineStages(Owner()).execute_download_from_job(
        rule_id=1, delivery_id=2, message_id=3,
        source_channel="-1001", target_id="-1002", job_attempt=1,
    ))
    assert result["retryable"] is True
    assert result["transient_validation_failure"] is True
    assert not result.get("invalid_source_file")
    assert path.exists()


def test_album_sent_paths_persist_target_message_ids():
    source = Path("app/repost_album_delivery.py").read_text(encoding="utf-8")
    assert "copy_album_uncertain_no_fallback" in source
    assert "REUPLOAD_ALBUM_MT_PROTO_UNCERTAIN" in source
    for method in (
        "copy_album_verified",
        "copy_album_retry_verified",
        "reupload_album_verified",
        "reupload_album_retry_verified",
        "one_by_one_fallback",
    ):
        assert f'delivery_method="{method}"' in source
    assert source.count('if hasattr(owner, "_mark_album_deliveries_sent_sync")') >= 5


def test_botapi_copy_transport_uncertain_requires_manual_confirmation():
    source = Path("app/repost_single_delivery.py").read_text(encoding="utf-8")
    album = Path("app/repost_album_delivery.py").read_text(encoding="utf-8")
    helper = Path("app/sender_botapi_copy_helpers.py").read_text(encoding="utf-8")
    assert 'copy_result.get("definitely_rejected")' in source
    assert "copy_album_uncertain_no_fallback" in album
    assert "TelegramBadRequest" in helper


def test_missing_download_before_send_does_not_claim_telegram_write():
    """A failed source download is safely retryable, not an accepted send."""
    bot = FakeBot()
    owner = _owner(bot, video_path=None)
    result = asyncio.run(SenderReuploadHelpers(owner).reupload_message(
        SimpleNamespace(media=object(), video=True), "target", None,
    ))
    assert result.transport_attempted is False
    assert result.transport_accepted is False
    assert result.error_text == "download_failed_before_send"
    assert bot.calls == []


def test_invalid_album_input_before_send_is_not_ambiguous():
    client, sender = _real_telethon_owner()
    result = asyncio.run(sender.send_album_via_telethon(
        target_id="target", target_thread_id=None,
        messages=[SimpleNamespace(media=None, id=10)],
    ))
    assert result["transport_attempted"] is False
    assert client.send_count == 0


def test_definitely_rejected_original_media_without_fallback_is_pre_send_recoverable():
    from telethon.errors import BadRequestError

    class Client:
        def __init__(self):
            self.calls = 0

        async def get_messages(self, *args, **kwargs):
            return []

        async def send_file(self, **kwargs):
            self.calls += 1
            raise BadRequestError(None, "FILE_REFERENCE_EXPIRED")

    client = Client()
    owner = SimpleNamespace(
        telethon=client,
        _content_from_message_or_post=lambda **kwargs: {"text": "", "entities": []},
        _build_text_and_entities_from_content=lambda content: ("", []),
        _clone_telethon_entities=lambda entities, text: [],
    )
    sender = SenderTelethonHelpers(owner)

    async def peer(*args, **kwargs):
        return "target", False

    sender._resolve_target_entity_and_broadcast_flag = peer
    result = asyncio.run(sender.send_file_via_telethon(
        target_id="target", target_thread_id=None,
        message=SimpleNamespace(media=object(), id=10),
        file_path=None, force_document=True,
    ))
    assert client.calls == 1
    assert result.transport_attempted is False
    assert result.error_text == "telethon_original_media_failed_no_file_path"



def test_custom_caption_document_invalid_is_a_recoverable_reference_rejection():
    from telethon.errors import DocumentInvalidError

    client, sender = _real_telethon_owner()

    async def reject(**kwargs):
        client.send_count += 1
        raise DocumentInvalidError(request=None)

    client.send_file = reject
    result = asyncio.run(sender.send_file_via_telethon(
        target_id="target", target_thread_id=None,
        message=SimpleNamespace(media=object(), id=10),
        caption_override={"text": "new", "entities": []},
    ))
    assert client.send_count == 1
    assert result.transport_attempted is False
    assert result.error_text == "repost_original_document_invalid"
