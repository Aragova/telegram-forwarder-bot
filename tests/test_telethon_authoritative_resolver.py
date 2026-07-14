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


def test_cross_channel_same_numeric_id_is_allowed_without_self_loop_guard():
    sent = _msg(100)
    telethon = FakeTelethon(by_id={100: sent}, history=[])
    resolver = TelethonAuthoritativeMessageResolver(telethon)
    result = asyncio.run(resolver.resolve_authoritative_single_message(target_entity=-200, target_id=-200, sent=sent, expected_text="caption", before_max_message_id=99, send_started_at=datetime.now(timezone.utc), send_finished_at=datetime.now(timezone.utc), source_message_ids=None))
    assert result.ok is True
    assert result.authoritative_message_id == 100


def test_true_self_loop_same_numeric_id_is_blocked():
    sent = _msg(100)
    telethon = FakeTelethon(by_id={100: sent}, history=[])
    resolver = TelethonAuthoritativeMessageResolver(telethon)
    result = asyncio.run(resolver.resolve_authoritative_single_message(target_entity=-100, target_id=-100, sent=sent, expected_text="caption", before_max_message_id=99, send_started_at=datetime.now(timezone.utc), send_finished_at=datetime.now(timezone.utc), source_message_ids={100}))
    assert result.ok is False
    assert result.authoritative_message_id is None


def test_album_sent_count_mismatch_is_unresolved():
    telethon = FakeTelethon(by_id={}, history=[])
    resolver = TelethonAuthoritativeMessageResolver(telethon)
    result = asyncio.run(resolver.resolve_authoritative_album_messages(target_entity=-100, target_id=-100, sent_messages=[_msg(10)], expected_messages=[_msg(1), _msg(2)], expected_text="caption", before_max_message_id=9, send_started_at=datetime.now(timezone.utc), send_finished_at=datetime.now(timezone.utc)))
    assert result.ok is False
    assert result.error_text == "album_sent_count_mismatch"


def test_album_duplicate_authoritative_ids_are_unresolved():
    telethon = FakeTelethon(by_id={10: _msg(20), 11: _msg(20)}, history=[])
    resolver = TelethonAuthoritativeMessageResolver(telethon)
    result = asyncio.run(resolver.resolve_authoritative_album_messages(target_entity=-100, target_id=-100, sent_messages=[_msg(10), _msg(11)], expected_messages=[_msg(1), _msg(2)], expected_text="caption", before_max_message_id=9, send_started_at=datetime.now(timezone.utc), send_finished_at=datetime.now(timezone.utc)))
    assert result.ok is False
    assert result.error_text == "album_authoritative_ids_not_unique"


def test_history_ignores_matching_non_outbound_message():
    sent = _msg(1157)
    foreign = _msg(2883)
    foreign.out = False
    own = _msg(2884)
    own.out = True
    telethon = FakeTelethon(by_id={1157: None}, history=[foreign, own])
    resolver = TelethonAuthoritativeMessageResolver(telethon)
    result = asyncio.run(resolver.resolve_authoritative_single_message(target_entity=-100, target_id=-100, sent=sent, expected_text="caption", before_max_message_id=2882, send_started_at=datetime.now(timezone.utc), send_finished_at=datetime.now(timezone.utc)))
    assert result.ok is True
    assert result.authoritative_message_id == 2884
