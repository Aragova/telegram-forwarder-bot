from __future__ import annotations

from app.sender import SenderService


class DummyBot: pass
class DummyRepo: pass


def test_sender_service_accepts_optional_repost_single_rollout_probe_none() -> None:
    service = SenderService(bot=DummyBot(), telethon_client=None, reaction_clients=[], db=DummyRepo())
    assert service.repost_single_rollout_probe is None


def test_sender_service_stores_injected_repost_single_rollout_probe() -> None:
    probe = object()
    service = SenderService(bot=DummyBot(), telethon_client=None, reaction_clients=[], db=DummyRepo(), repost_single_rollout_probe=probe)  # type: ignore[arg-type]
    assert service.repost_single_rollout_probe is probe
