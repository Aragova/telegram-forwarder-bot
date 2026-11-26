import asyncio
from types import SimpleNamespace

import pytest

from app.video_send_diagnostics import artifact_details, decode_artifact, persist_video_failure


def test_artifact_details_are_bounded_structural_hints(tmp_path):
    video = tmp_path / "video.mp4"
    video.write_bytes(b"\x00\x00\x00\x18ftypisom" + b"secret payload")
    thumb = tmp_path / "thumb.jpg"
    thumb.write_bytes(
        b"\xff\xd8\xff\xe0\x00\x04xx"
        b"\xff\xc0\x00\x0b\x08\x04\x00\x02\x40\x01\x01\x11\x00\xff\xd9"
    )
    assert artifact_details(video)["mp4_ftyp_at_offset_4"]
    data = artifact_details(thumb, thumbnail=True)
    assert data["jpeg_soi"] and data["jpeg_eoi"]
    assert (data["width"], data["height"]) == (576, 1024)
    assert "secret payload" not in repr(artifact_details(video))
    assert artifact_details(None) == {"provided": False}
    assert artifact_details(tmp_path / "missing")["read_error"] == "FileNotFoundError"


@pytest.mark.parametrize("thumbnail", [False, True])
@pytest.mark.parametrize("outcome", ["decoded", "decode_failed", "timeout_inconclusive", "unavailable"])
def test_decoder_diagnostic_is_time_bounded_and_reports_exact_scope(tmp_path, monkeypatch, thumbnail, outcome):
    artifact = tmp_path / ("thumb.jpg" if thumbnail else "video.mp4")
    artifact.write_bytes(b"data")

    async def spawn(*args, **kwargs):
        assert "-xerror" in args
        assert "-nostdin" in args
        assert args[args.index("-f") + 1] == ("mjpeg" if thumbnail else "mov")
        assert ("-frames:v" in args) == thumbnail
        if outcome == "unavailable":
            raise FileNotFoundError("ffmpeg")
        return SimpleNamespace(returncode=1 if outcome == "decode_failed" else 0)

    async def communicate(_process, *, timeout_seconds):
        assert timeout_seconds == (3 if thumbnail else 20)
        if outcome == "timeout_inconclusive":
            raise asyncio.TimeoutError
        return b"", str(artifact).encode() + b" invalid data"

    monkeypatch.setattr("app.video_send_diagnostics.asyncio.create_subprocess_exec", spawn)
    monkeypatch.setattr("app.video_send_diagnostics.communicate_safely", communicate)
    result = asyncio.run(decode_artifact(artifact, thumbnail=thumbnail))
    assert result["status"] == outcome
    if outcome in {"decoded", "decode_failed"}:
        assert str(artifact) not in result["stderr_tail"]
        assert result["scope"] == ("first_frame" if thumbnail else "full_video_and_audio")


def test_missing_decode_does_not_start_process(tmp_path):
    assert asyncio.run(decode_artifact(tmp_path / "missing.mp4")) == {"status": "missing"}


@pytest.mark.parametrize(("trace", "expected"), [
    ({"phase": "telethon_send_file", "rpc_state": "rejected"}, "send_media_rpc"),
    ({"phase": "telethon_send_file", "rpc_state": "response_received"}, "send_file_after_rpc_response"),
    ({"phase": "telethon_send_file", "upload_parts_failed": 1}, "file_upload"),
    ({"phase": "telethon_send_file", "upload_parts_rejected": 1}, "file_upload"),
    ({"phase": "telethon_send_file"}, "send_file_before_observed_publication"),
    ({"phase": "target_message_resolution"}, "target_message_resolution"),
    ({}, "unknown"),
])
def test_failure_stage_is_based_on_observed_rpc_not_assumed(trace, expected):
    events = []
    owner = SimpleNamespace(
        db=SimpleNamespace(log_video_event=lambda **_kwargs: None),
        _log_video_event_sync=lambda **kwargs: events.append(kwargs),
    )
    intent = SimpleNamespace(video_diagnostics=trace, key="delivery:1", possible_write=False,
                             uncertain=False, blocked=False)
    asyncio.run(persist_video_failure(owner, intent, delivery_id=1, rule_id=2, error_text="failure"))
    assert events[0]["extra"]["failure_stage"] == expected


def test_rpc_diagnostics_include_custom_emoji_document_id_not_caption():
    from telethon import types
    from app.video_send_diagnostics import media_request_details

    request = SimpleNamespace(
        media=None, message="private caption",
        entities=[types.MessageEntityCustomEmoji(offset=0, length=2, document_id=5319302290127991242),
                  types.MessageEntityBold(offset=2, length=3)],
    )
    snapshot = media_request_details(request)
    assert snapshot["caption_entities"][0]["document_id"] == 5319302290127991242
    assert "document_id" not in snapshot["caption_entities"][1]
    assert "private caption" not in repr(snapshot)
