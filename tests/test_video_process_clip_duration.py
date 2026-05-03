import asyncio

from app.sender import SenderService


class _Bot:
    pass


class _Rule:
    id = 50
    mode = "video"
    video_add_intro = False

    def __init__(self, clip_duration_seconds):
        self.video_clip_duration_seconds = clip_duration_seconds


class _Repo:
    def __init__(self, clip_duration_seconds):
        self.rule = _Rule(clip_duration_seconds)

    def get_rule(self, _rule_id):
        return self.rule


class _VideoProcessor:
    def __init__(self):
        self.received_clip_duration_seconds = None

    async def build_processed_video(self, **kwargs):
        self.received_clip_duration_seconds = kwargs.get("clip_duration_seconds")
        return {"processed_video_path": __file__, "thumbnail_path": None}



def _build_sender(clip_duration_seconds):
    sender = SenderService(bot=_Bot(), telethon_client=None, reaction_clients=[], db=_Repo(clip_duration_seconds))
    sender.video_processor = _VideoProcessor()
    return sender


def test_video_process_passes_rule_clip_duration(tmp_path):
    source = tmp_path / "source.mp4"
    source.write_bytes(b"ok")

    sender = _build_sender(60)

    async def _run():
        sender._validate_mp4_file_for_pipeline = lambda *_a, **_k: asyncio.sleep(0, result=(True, None))  # type: ignore[method-assign]
        sender._get_rule_intro_items_sync = lambda _rule: (None, None)  # type: ignore[method-assign]
        result = await sender.execute_video_process_from_job(rule_id=50, delivery_id=500, source_video_path=str(source))
        assert result["ok"] is True

    asyncio.run(_run())

    assert sender.video_processor.received_clip_duration_seconds == 60


def test_video_process_uses_fallback_clip_duration_118(tmp_path):
    source = tmp_path / "source.mp4"
    source.write_bytes(b"ok")

    sender = _build_sender(None)

    async def _run():
        sender._validate_mp4_file_for_pipeline = lambda *_a, **_k: asyncio.sleep(0, result=(True, None))  # type: ignore[method-assign]
        sender._get_rule_intro_items_sync = lambda _rule: (None, None)  # type: ignore[method-assign]
        result = await sender.execute_video_process_from_job(rule_id=50, delivery_id=501, source_video_path=str(source))
        assert result["ok"] is True

    asyncio.run(_run())

    assert sender.video_processor.received_clip_duration_seconds == 118
