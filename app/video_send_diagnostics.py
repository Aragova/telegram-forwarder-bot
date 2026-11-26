"""Small, privacy-safe failure snapshots for video publication.

No file content, captions, access hashes, auth data or upload IDs are retained.
Diagnostics do not change media or authorize a retry.
"""
from __future__ import annotations

import logging
from pathlib import Path

from .runtime_utils import run_db

logger = logging.getLogger("forwarder")

VIDEO_METADATA_KEYS = (
    "file_size", "width", "height", "duration", "fps", "fps_avg",
    "codec_name", "pix_fmt", "sar", "has_audio", "audio_sample_rate",
    "audio_channels", "audio_codec_name", "format_name", "video_profile",
    "video_level", "stream_count",
)
_JPEG_SOF = {0xC0, 0xC1, 0xC2, 0xC3, 0xC5, 0xC6, 0xC7,
             0xC9, 0xCA, 0xCB, 0xCD, 0xCE, 0xCF}


def artifact_details(path, *, thumbnail=False):
    """Read at most 64 KiB; structural hints, not a full integrity check."""
    if not path:
        return {"provided": False}
    result = {"provided": True}
    try:
        with Path(path).open("rb") as stream:
            stream.seek(0, 2)
            result["bytes"] = stream.tell()
            stream.seek(0)
            data = stream.read(65536 if thumbnail else 16)
            if not thumbnail:
                result["mp4_ftyp_at_offset_4"] = data[4:8] == b"ftyp"
                return result
            result["jpeg_soi"] = data.startswith(b"\xff\xd8")
            if result["bytes"] >= 2:
                stream.seek(-2, 2)
                result["jpeg_eoi"] = stream.read(2) == b"\xff\xd9"
            if result["jpeg_soi"]:
                pos = 2
                while pos + 4 <= len(data) and data[pos] == 0xFF:
                    marker = data[pos + 1]
                    pos += 2
                    if marker == 0xFF:
                        pos -= 1
                        continue
                    if marker in {0xD9, 0xDA}:
                        break
                    if marker == 0x01 or 0xD0 <= marker <= 0xD8:
                        continue
                    length = int.from_bytes(data[pos:pos + 2], "big")
                    if length < 2 or pos + length > len(data):
                        break
                    if marker in _JPEG_SOF and length >= 7:
                        result["height"] = int.from_bytes(data[pos + 3:pos + 5], "big")
                        result["width"] = int.from_bytes(data[pos + 5:pos + 7], "big")
                        break
                    pos += length
    except (OSError, ValueError) as exc:
        result["read_error"] = type(exc).__name__
    return result


def rpc_error_details(exc):
    request = getattr(exc, "request", None)
    return {
        "error_type": type(exc).__name__,
        "rpc_code": getattr(exc, "code", None),
        "error_request_type": type(request).__name__ if request is not None else None,
    }


def media_request_details(request):
    """Extract only the structural fields of the actual SendMediaRequest."""
    media = getattr(request, "media", None)
    file = getattr(media, "file", None)
    thumb = getattr(media, "thumb", None)
    attributes = getattr(media, "attributes", None) or []
    video = next((a for a in attributes if type(a).__name__ == "DocumentAttributeVideo"), None)
    return {
        "method": type(request).__name__,
        "media_type": type(media).__name__,
        "mime_type": getattr(media, "mime_type", None),
        "file_type": type(file).__name__ if file is not None else None,
        "file_parts": getattr(file, "parts", None),
        "thumb_type": type(thumb).__name__ if thumb is not None else None,
        "thumb_parts": getattr(thumb, "parts", None),
        "attribute_types": [type(a).__name__ for a in attributes],
        "video_attributes": {key: getattr(video, key, None) for key in
                             ("duration", "w", "h", "supports_streaming", "round_message")} if video else None,
        "caption_entity_types": [type(e).__name__ for e in (getattr(request, "entities", None) or [])],
    }


def record_error(trace, exc):
    trace.update(rpc_error_details(exc))


async def persist_video_failure(owner, intent, *, delivery_id, rule_id, error_text, stage=None):
    """Keep failure evidence in the existing audit table; logging failure is nonfatal."""
    if intent.video_diagnostics is None:
        return
    snapshot = dict(intent.video_diagnostics)
    snapshot.update({
        "idempotency_key": intent.key,
        "possible_write": intent.possible_write,
        "uncertain": intent.uncertain,
        "blocked": intent.blocked,
    })
    if stage:
        snapshot["failure_stage"] = stage
    logger.warning("VIDEO_SEND_DIAGNOSTICS | delivery_id=%s | rule_id=%s | error=%s | details=%s",
                   delivery_id, rule_id, error_text, snapshot)
    if not callable(getattr(owner.db, "log_video_event", None)):
        return
    try:
        await run_db(
            owner._log_video_event_sync, event_type="video_send_diagnostics",
            delivery_id=delivery_id, rule_id=rule_id, post_id=None,
            status="failed", error_text=error_text, extra=snapshot,
        )
    except Exception:
        logger.exception("VIDEO_SEND_DIAGNOSTICS_PERSIST_FAILED | delivery_id=%s", delivery_id)
