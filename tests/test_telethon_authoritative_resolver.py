import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

from app.telethon_authoritative_resolver import TelethonAuthoritativeMessageResolver


class FakeTelethon:
    def __init__(self, by_id=None, history=None):
        self.by_id = by_id or {}
        self.history = history or []
        self.send_file_calls = 0

    async def get_messages(self, entity, ids=None, limit=None):
        if limit:
            return self.history[:limit]
        return self.by_id.get(ids)

    async def send_file(self, **kwargs):
        self.send_file_calls += 1
        return SimpleNamespace(id=1157, message=kwargs.get("caption") or "", media=SimpleNamespace(document=SimpleNamespace(id=1, size=10, mime_type="video/mp4", attributes=[])))


def _msg(mid, text="caption", doc_id=1):
    return SimpleNamespace(id=mid, message=text, date=datetime.now(timezone.utc), media=SimpleNamespace(document=SimpleNamespace(id=doc_id, size=10, mime_type="video/mp4", attributes=[])))


def test_returned_candidate_verified_without_history():
    sent = _msg(2883)
    telethon = FakeTelethon(by_id={2883: sent}, history=[])
    resolver = TelethonAuthoritativeMessageResolver(telethon)
    result = asyncio.run(resolver.resolve_authoritative_single_message(target_entity=-100, target_id=-100, sent=sent, expected_text="caption", before_max_message_id=2882, send_started_at=datetime.now(timezone.utc), send_finished_at=datetime.now(timezone.utc)))
    assert result.ok is True
    assert result.authoritative_message_id == 2883
    assert result.resolution_method == "returned_candidate_verified"


def test_production_regression_returned_1157_resolves_to_2883():
    sent = _msg(1157)
    telethon = FakeTelethon(by_id={1157: None}, history=[_msg(2883)])
    resolver = TelethonAuthoritativeMessageResolver(telethon)
    result = asyncio.run(resolver.resolve_authoritative_single_message(target_entity=-1002693516250, target_id=-1002693516250, sent=sent, expected_text="caption", before_max_message_id=2882, send_started_at=datetime.now(timezone.utc), send_finished_at=datetime.now(timezone.utc)))
    assert result.ok is True
    assert result.returned_candidate_id == 1157
    assert result.authoritative_message_id == 2883
    assert result.resolution_method == "target_history_media_fingerprint"


def test_ambiguous_history_is_unresolved_and_does_not_resend():
    sent = _msg(1157)
    telethon = FakeTelethon(by_id={1157: None}, history=[_msg(2883), _msg(2884)])
    resolver = TelethonAuthoritativeMessageResolver(telethon)
    result = asyncio.run(resolver.resolve_authoritative_single_message(target_entity=-100, target_id=-100, sent=sent, expected_text="caption", before_max_message_id=2882, send_started_at=datetime.now(timezone.utc), send_finished_at=datetime.now(timezone.utc)))
    assert result.ok is False
    assert result.authoritative_message_id is None
    assert telethon.send_file_calls == 0


def test_self_loop_source_id_collision_blocked_then_history_used():
    sent = _msg(523)
    telethon = FakeTelethon(by_id={523: sent}, history=[_msg(2882)])
    resolver = TelethonAuthoritativeMessageResolver(telethon)
    result = asyncio.run(resolver.resolve_authoritative_single_message(target_entity=-100, target_id=-100, sent=sent, expected_text="caption", before_max_message_id=2881, send_started_at=datetime.now(timezone.utc), send_finished_at=datetime.now(timezone.utc), source_message_ids={523}))
    assert result.ok is True
    assert result.authoritative_message_id == 2882
