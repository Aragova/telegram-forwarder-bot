import asyncio
from types import SimpleNamespace

from app.saved_post_album_service import SavedPostAlbumCaptureBuffer


def test_album_buffer_collects_and_cleans_bucket():
    calls = []

    async def _on_ready(**kwargs):
        calls.append(kwargs)

    async def _run():
        buf = SavedPostAlbumCaptureBuffer(delay_seconds=0.01)
        await buf.add_message(admin_id=1, message=SimpleNamespace(media_group_id="g1", message_id=10), on_album_ready=_on_ready)
        await buf.add_message(admin_id=1, message=SimpleNamespace(media_group_id="g1", message_id=11), on_album_ready=_on_ready)
        await asyncio.sleep(0.03)
        assert len(calls) == 1
        assert len(calls[0]["messages"]) == 2
        assert buf._buckets == {}

    asyncio.run(_run())


def test_album_buffer_ignores_duplicates():
    calls = []
    async def _on_ready(**kwargs):
        calls.append(kwargs)
    async def _run():
        buf = SavedPostAlbumCaptureBuffer(delay_seconds=0.01)
        msg = SimpleNamespace(media_group_id="g1", message_id=10)
        await buf.add_message(admin_id=1, message=msg, on_album_ready=_on_ready)
        await buf.add_message(admin_id=1, message=msg, on_album_ready=_on_ready)
        await asyncio.sleep(0.03)
        assert len(calls[0]["messages"]) == 1
    asyncio.run(_run())
