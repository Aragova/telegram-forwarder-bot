from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import asyncio
from telethon.tl import types as tl_types

from app.sender_telethon_helpers import SenderTelethonHelpers


class _Telethon:
    def __init__(self, *, original_ok=False):
        self.calls = []
        self.download_media = AsyncMock(return_value=None)
        self._original_ok = original_ok

    async def send_file(self, **kwargs):
        self.calls.append(kwargs)
        if len(self.calls) == 1 and not isinstance(kwargs.get("file"), str):
            if self._original_ok:
                return SimpleNamespace(id=10)
            raise RuntimeError("original failed")
        return SimpleNamespace(id=777)


class _Owner:
    def __init__(self, telethon):
        self.telethon = telethon

    def _content_from_message_or_post(self, **_kwargs):
        return {"text": "caption", "entities": []}

    def _build_text_and_entities_from_content(self, content):
        return content.get("text") or "", content.get("entities") or []

    def _clone_telethon_entities(self, entities, text):
        return list(entities or [])



def _jpeg_bytes(width=100, height=100, payload_size=0):
    return (
        b"\xff\xd8"
        b"\xff\xe0\x00\x10JFIF\x00\x01\x01\x00\x00\x01\x00\x01\x00\x00"
        + b"\xff\xc0\x00\x11\x08"
        + int(height).to_bytes(2, "big")
        + int(width).to_bytes(2, "big")
        + b"\x03\x01\x11\x00\x02\x11\x00\x03\x11\x00"
        + (b"0" * payload_size)
        + b"\xff\xd9"
    )

def _message(attr=None, thumbs=None):
    document = SimpleNamespace(attributes=[attr] if attr else [], thumbs=thumbs or [])
    return SimpleNamespace(media=SimpleNamespace(document=document), video=True)



def test_download_source_thumb_uses_message_and_selected_photosize(tmp_path):
    message = _message(
        thumbs=[
            tl_types.PhotoPathSize(type="i", bytes=b"x"),
            tl_types.VideoSize(type="v", w=640, h=360, size=1000),
            tl_types.PhotoSize(type="s", w=90, h=90, size=100),
            tl_types.PhotoSize(type="m", w=320, h=180, size=200),
        ]
    )
    telethon = _Telethon()

    async def fake_download_media(media, *, file, thumb=None):
        fake_download_media.media = media
        fake_download_media.thumb = thumb
        Path(file).write_bytes(_jpeg_bytes(320, 180))
        return file

    telethon.download_media = fake_download_media
    helper = SenderTelethonHelpers(_Owner(telethon))

    path, source = asyncio.run(helper._download_source_video_thumb(message))

    assert source == "telegram"
    assert fake_download_media.media is message
    assert isinstance(fake_download_media.thumb, tl_types.PhotoSize)
    assert fake_download_media.thumb.type == "m"
    Path(path).unlink(missing_ok=True)

def test_video_metadata_from_source_document_attribute(tmp_path, monkeypatch):
    video = tmp_path / "v.mp4"
    video.write_bytes(b"video")
    attr = tl_types.DocumentAttributeVideo(duration=1050, w=1920, h=1080, supports_streaming=True)
    telethon = _Telethon()
    helper = SenderTelethonHelpers(_Owner(telethon))
    monkeypatch.setattr(helper, "_probe_video_file", AsyncMock(return_value=None))
    monkeypatch.setattr(helper, "_download_source_video_thumb", AsyncMock(return_value=(None, "none")))
    monkeypatch.setattr(helper, "_generate_video_thumb", AsyncMock(return_value=(None, "none", 2.0)))

    assert asyncio.run(helper.send_file_via_telethon(target_id="1", target_thread_id=None, message=_message(attr), file_path=video)) == 777
    sent_attr = telethon.calls[-1]["attributes"][0]
    assert sent_attr.duration == 1050
    assert sent_attr.w == 1920
    assert sent_attr.h == 1080
    assert sent_attr.supports_streaming is True


def test_invalid_source_attribute_falls_back_to_ffprobe(tmp_path, monkeypatch):
    video = tmp_path / "v.mp4"
    video.write_bytes(b"video")
    attr = tl_types.DocumentAttributeVideo(duration=0, w=0, h=0, supports_streaming=False)
    telethon = _Telethon()
    helper = SenderTelethonHelpers(_Owner(telethon))
    probe = AsyncMock(return_value={"duration": 17, "width": 640, "height": 360, "codec": "h264", "has_video": True})
    monkeypatch.setattr(helper, "_probe_video_file", probe)
    monkeypatch.setattr(helper, "_download_source_video_thumb", AsyncMock(return_value=(None, "none")))
    monkeypatch.setattr(helper, "_generate_video_thumb", AsyncMock(return_value=(None, "none", 2.0)))

    asyncio.run(helper.send_file_via_telethon(target_id="1", target_thread_id=None, message=_message(attr), file_path=video))
    sent_attr = telethon.calls[-1]["attributes"][0]
    assert probe.await_count == 1
    assert (sent_attr.duration, sent_attr.w, sent_attr.h) == (17, 640, 360)


def test_source_thumbnail_is_passed_to_send_file(tmp_path, monkeypatch):
    video = tmp_path / "v.mp4"
    video.write_bytes(b"video")
    thumb = tmp_path / "thumb.jpg"
    thumb.write_bytes(_jpeg_bytes(100, 100))
    telethon = _Telethon()
    helper = SenderTelethonHelpers(_Owner(telethon))
    monkeypatch.setattr(helper, "_probe_video_file", AsyncMock(return_value={"duration": 10, "width": 100, "height": 100}))
    monkeypatch.setattr(helper, "_download_source_video_thumb", AsyncMock(return_value=(thumb, "telegram")))

    asyncio.run(helper.send_file_via_telethon(target_id="1", target_thread_id=None, message=_message(), file_path=video))
    assert telethon.calls[-1]["thumb"] == str(thumb)
    assert not thumb.exists()


def test_generated_thumbnail_exists_during_send_and_removed(tmp_path, monkeypatch):
    video = tmp_path / "v.mp4"
    video.write_bytes(b"video")
    thumb = tmp_path / "generated.jpg"
    thumb.write_bytes(_jpeg_bytes(100, 100))
    telethon = _Telethon()
    helper = SenderTelethonHelpers(_Owner(telethon))
    monkeypatch.setattr(helper, "_probe_video_file", AsyncMock(return_value={"duration": 20, "width": 100, "height": 100}))
    monkeypatch.setattr(helper, "_download_source_video_thumb", AsyncMock(return_value=(None, "none")))
    gen = AsyncMock(return_value=(thumb, "generated_ffmpeg", 2.0))
    monkeypatch.setattr(helper, "_generate_video_thumb", gen)

    asyncio.run(helper.send_file_via_telethon(target_id="1", target_thread_id=None, message=_message(), file_path=video))
    assert telethon.calls[-1]["thumb"] == str(thumb)
    assert gen.await_count == 1
    assert not thumb.exists()


def test_seek_seconds_positive_when_duration_allows(tmp_path, monkeypatch):
    video = tmp_path / "v.mp4"
    video.write_bytes(b"video")
    helper = SenderTelethonHelpers(_Owner(_Telethon()))

    async def fake_exec(*cmd, **_kwargs):
        class Proc:
            returncode = 0
            async def communicate(self):
                Path(cmd[-1]).write_bytes(_jpeg_bytes(320, 180))
                return b"", b""
        fake_exec.cmd = cmd
        return Proc()

    monkeypatch.setattr("asyncio.create_subprocess_exec", fake_exec)
    thumb, _source, seek = asyncio.run(helper._generate_video_thumb(video, 20))
    assert seek and seek > 0
    assert "-ss" in fake_exec.cmd
    Path(thumb).unlink(missing_ok=True)



def test_generated_thumb_contract_jpeg_320_and_under_20kb(tmp_path, monkeypatch):
    video = tmp_path / "v.mp4"
    video.write_bytes(b"video")
    helper = SenderTelethonHelpers(_Owner(_Telethon()))

    async def fake_exec(*cmd, **_kwargs):
        class Proc:
            returncode = 0
            async def communicate(self):
                Path(cmd[-1]).write_bytes(_jpeg_bytes(320, 180, payload_size=512))
                return b"", b""
        fake_exec.cmd = cmd
        return Proc()

    monkeypatch.setattr("asyncio.create_subprocess_exec", fake_exec)
    thumb, source, _seek = asyncio.run(helper._generate_video_thumb(video, 20))

    assert source == "generated_ffmpeg"
    assert thumb.read_bytes().startswith(b"\xff\xd8")
    assert thumb.stat().st_size < 20 * 1024
    assert helper._jpeg_dimensions(thumb) == (320, 180)
    assert "scale=320:320:force_original_aspect_ratio=decrease" in fake_exec.cmd
    thumb.unlink(missing_ok=True)

def test_non_video_behaviour_unchanged(tmp_path, monkeypatch):
    file_path = tmp_path / "image.jpg"
    file_path.write_bytes(b"img")
    telethon = _Telethon()
    helper = SenderTelethonHelpers(_Owner(telethon))
    probe = AsyncMock()
    gen = AsyncMock()
    monkeypatch.setattr(helper, "_probe_video_file", probe)
    monkeypatch.setattr(helper, "_generate_video_thumb", gen)
    message = SimpleNamespace(media=SimpleNamespace(document=SimpleNamespace(attributes=[], thumbs=[])), photo=True)

    asyncio.run(helper.send_file_via_telethon(target_id="1", target_thread_id=None, message=message, file_path=file_path))
    assert "attributes" not in telethon.calls[-1]
    assert "thumb" not in telethon.calls[-1]
    probe.assert_not_awaited()
    gen.assert_not_awaited()


def test_original_media_success_does_not_use_file_path_fallback(tmp_path, monkeypatch):
    video = tmp_path / "v.mp4"
    video.write_bytes(b"video")
    telethon = _Telethon(original_ok=True)
    helper = SenderTelethonHelpers(_Owner(telethon))
    probe = AsyncMock()
    monkeypatch.setattr(helper, "_probe_video_file", probe)

    assert asyncio.run(helper.send_file_via_telethon(target_id="1", target_thread_id=None, message=_message(), file_path=video)) == 10
    assert len(telethon.calls) == 1
    probe.assert_not_awaited()
