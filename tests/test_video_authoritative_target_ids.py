"""Regression for Telethon returning an unrelated peer's numeric message id.

The message accepted into the target channel has its own id; reactions must
only use that authoritative target id, and ambiguity must never cause a resend.
"""
import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

from app.video_processor import VideoProcessor
from app.telethon_authoritative_resolver import TelethonSendOutcome


TARGET = -1004477194308

def _video_message(mid, *, document_id=123, caption="caption"):
    return SimpleNamespace(
        id=mid, chat_id=TARGET, post=True, out=False,
        date=datetime.now(timezone.utc), message=caption,
        media=SimpleNamespace(document=SimpleNamespace(
            id=document_id, size=1234, mime_type="video/mp4", attributes=[]
        )),
    )


class FakeTelethon:
    def __init__(self, *, history, fail_send=False):
        self.history = history
        self.fail_send = fail_send
        self.send_file_calls = 0

    async def get_entity(self, _entity):
        return SimpleNamespace(id=4477194308, broadcast=True, megagroup=False)

    async def get_messages(self, _entity, ids=None, limit=None):
        if limit == 1:
            return [_video_message(2182, document_id=999)]
        if limit:
            return self.history
        return next((msg for msg in self.history if msg.id == ids), None)

    async def send_file(self, **_kwargs):
        self.send_file_calls += 1
        if self.fail_send:
            raise RuntimeError("connection lost after request")
        # Different source peer id returned by Telethon, same media document.
        return SimpleNamespace(
            id=370, message="caption",
            media=SimpleNamespace(document=SimpleNamespace(
                id=123, size=1234, mime_type="video/mp4", attributes=[]
            )),
        )


def _processor(fake):
    processor = VideoProcessor(bot=SimpleNamespace(session=None), telethon_client=fake)

    async def video_info(*_args, **_kwargs):
        return {"width": 720, "height": 1280, "duration": 30}

    processor.get_video_info = video_info
    return processor


def test_premium_video_resolves_actual_target_id_not_returned_370(tmp_path):
    fake = FakeTelethon(history=[_video_message(2183)])
    processor = _processor(fake)
    result = asyncio.run(processor._send_video_via_telethon(
        TARGET, None, str(tmp_path / "video.mp4"), None, "caption",
    ))
    assert isinstance(result, TelethonSendOutcome)
    assert result.transport_accepted is True
    assert result.authoritative_resolved is True
    assert result.returned_candidate_id == 370
    assert result.authoritative_message_id == 2183
    assert fake.send_file_calls == 1


def test_premium_video_does_not_match_different_media_with_same_caption(tmp_path):
    fake = FakeTelethon(history=[_video_message(2183, document_id=999)])
    result = asyncio.run(_processor(fake)._send_video_via_telethon(
        TARGET, None, str(tmp_path / "video.mp4"), None, "caption",
    ))
    assert result.authoritative_resolved is False
    assert result.authoritative_message_id is None
    assert fake.send_file_calls == 1


def test_strong_resolver_retries_reads_until_video_is_visible():
    from app.telethon_authoritative_resolver import TelethonAuthoritativeMessageResolver

    class DelayedHistory(FakeTelethon):
        history_reads = 0

        async def get_messages(self, entity, ids=None, limit=None):
            if limit == 50:
                self.history_reads += 1
                if self.history_reads < 3:
                    return []
            return await super().get_messages(entity, ids=ids, limit=limit)

    fake = DelayedHistory(history=[_video_message(2183)])
    sent = asyncio.run(fake.send_file())
    async def check():
        resolver = TelethonAuthoritativeMessageResolver(fake)
        return await resolver.resolve_authoritative_single_message(
            target_entity=TARGET, target_id=TARGET, sent=sent,
            expected_text="caption",
            before_max_message_id=2182,
            send_started_at=datetime.now(timezone.utc),
            send_finished_at=datetime.now(timezone.utc),
            target_is_broadcast_channel=True,
            require_strong_media_match=True,
            history_poll_delays=(0, 0, 0, 0),
        )
    result = asyncio.run(check())
    assert result.ok is True
    assert result.authoritative_message_id == 2183
    assert fake.history_reads == 3



def test_premium_video_rejects_ambiguous_identical_posts(tmp_path):
    fake = FakeTelethon(history=[_video_message(2183), _video_message(2184)])
    result = asyncio.run(_processor(fake)._send_video_via_telethon(
        TARGET, None, str(tmp_path / "video.mp4"), None, "caption",
    ))
    assert result.transport_accepted is True
    assert result.authoritative_resolved is False
    assert result.authoritative_message_id is None
    assert fake.send_file_calls == 1


def test_premium_video_send_exception_never_reuploads(tmp_path):
    video = tmp_path / "clip.mp4"
    video.write_bytes(b"not-a-real-mp4")
    fake = FakeTelethon(history=[], fail_send=True)
    result = asyncio.run(_processor(fake).send_with_retry(
        SimpleNamespace(session=None), TARGET, None, str(video), None,
        "caption", 30, caption_send_mode="premium", max_retries=3,
    ))
    assert isinstance(result, TelethonSendOutcome)
    assert result.authoritative_resolved is False
    assert result.transport_accepted is True  # conservatively unknown
    assert fake.send_file_calls == 1
