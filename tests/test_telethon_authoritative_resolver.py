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


def _thread_msg(mid, *, thread_id=None, post=False, out=True, text="caption", doc_id=1):
    msg = _msg(mid, text=text, doc_id=doc_id)
    msg.post = post
    msg.out = out
    if thread_id is not None:
        msg.reply_to = SimpleNamespace(reply_to_top_id=thread_id)
    return msg


def test_supergroup_candidate_post_false_is_accepted():
    sent = _thread_msg(300, post=False, out=True)
    telethon = FakeTelethon(by_id={300: sent}, history=[])
    resolver = TelethonAuthoritativeMessageResolver(telethon)
    result = asyncio.run(resolver.resolve_authoritative_single_message(target_entity=-100, target_id=-100, sent=sent, expected_text="caption", before_max_message_id=299, send_started_at=datetime.now(timezone.utc), send_finished_at=datetime.now(timezone.utc)))
    assert result.ok is True
    assert result.authoritative_message_id == 300


def test_supergroup_history_post_false_is_accepted():
    sent = _msg(1157)
    telethon = FakeTelethon(by_id={1157: None}, history=[_thread_msg(301, post=False, out=True)])
    resolver = TelethonAuthoritativeMessageResolver(telethon)
    result = asyncio.run(resolver.resolve_authoritative_single_message(target_entity=-100, target_id=-100, sent=sent, expected_text="caption", before_max_message_id=300, send_started_at=datetime.now(timezone.utc), send_finished_at=datetime.now(timezone.utc)))
    assert result.ok is True
    assert result.authoritative_message_id == 301


def test_history_message_from_other_topic_is_not_selected():
    sent = _msg(1157)
    telethon = FakeTelethon(by_id={1157: None}, history=[_thread_msg(302, thread_id=20, post=False, out=True)])
    resolver = TelethonAuthoritativeMessageResolver(telethon)
    result = asyncio.run(resolver.resolve_authoritative_single_message(target_entity=-100, target_id=-100, sent=sent, expected_text="caption", before_max_message_id=301, send_started_at=datetime.now(timezone.utc), send_finished_at=datetime.now(timezone.utc), target_thread_id=10))
    assert result.ok is False
    assert result.authoritative_message_id is None


def test_history_message_from_expected_topic_is_selected():
    sent = _msg(1157)
    telethon = FakeTelethon(by_id={1157: None}, history=[_thread_msg(303, thread_id=10, post=False, out=True)])
    resolver = TelethonAuthoritativeMessageResolver(telethon)
    result = asyncio.run(resolver.resolve_authoritative_single_message(target_entity=-100, target_id=-100, sent=sent, expected_text="caption", before_max_message_id=302, send_started_at=datetime.now(timezone.utc), send_finished_at=datetime.now(timezone.utc), target_thread_id=10))
    assert result.ok is True
    assert result.authoritative_message_id == 303


def test_self_loop_text_candidate_collision_uses_history():
    sent = SimpleNamespace(id=100, message="hello")
    history = [SimpleNamespace(id=101, message="hello", out=True)]
    telethon = FakeTelethon(by_id={100: SimpleNamespace(id=100, message="hello", out=True)}, history=history)
    resolver = TelethonAuthoritativeMessageResolver(telethon)
    result = asyncio.run(resolver.resolve_authoritative_single_message(target_entity=-100, target_id=-100, sent=sent, expected_text="hello", before_max_message_id=99, send_started_at=datetime.now(timezone.utc), send_finished_at=datetime.now(timezone.utc), source_message_ids={100}))
    assert result.ok is True
    assert result.authoritative_message_id == 101


def test_unique_new_outbound_production_regression_1158_to_2884():
    now = datetime.now(timezone.utc)
    sent = _msg(1158, text="wrong returned", doc_id=999)
    history = [_msg(2884, text="real target", doc_id=42)]
    history[0].out = True
    telethon = FakeTelethon(by_id={1158: None}, history=history)
    resolver = TelethonAuthoritativeMessageResolver(telethon)

    result = asyncio.run(resolver.resolve_authoritative_single_message(
        target_entity=-1002693516250,
        target_id=-1002693516250,
        sent=sent,
        expected_message=_msg(525, text="source caption", doc_id=777),
        expected_text="source caption",
        before_max_message_id=2883,
        send_started_at=now,
        send_finished_at=now,
        source_message_ids={525},
    ))

    assert result.ok is True
    assert result.returned_candidate_id == 1158
    assert result.authoritative_message_id == 2884
    assert result.resolution_method == "target_history_unique_new_outbound"


def test_damaged_sent_fingerprint_still_uses_unique_history_candidate():
    now = datetime.now(timezone.utc)
    sent = _msg(1158, text="telethon damaged", doc_id=999999)
    target = _msg(2884, text="uploaded target", doc_id=123)
    target.out = True
    telethon = FakeTelethon(by_id={1158: None}, history=[target])
    resolver = TelethonAuthoritativeMessageResolver(telethon)

    result = asyncio.run(resolver.resolve_authoritative_single_message(
        target_entity=-100,
        target_id=-100,
        sent=sent,
        expected_message=_msg(525, text="source caption", doc_id=456),
        expected_text="source caption",
        before_max_message_id=2883,
        send_started_at=now,
        send_finished_at=now,
    ))

    assert result.ok is True
    assert result.authoritative_message_id == 2884
    assert result.resolution_method == "target_history_unique_new_outbound"


def test_self_loop_source_collision_excludes_source_and_selects_unique_new_outbound():
    now = datetime.now(timezone.utc)
    sent = _msg(1158, text="wrong", doc_id=999)
    source = _msg(525, text="source caption", doc_id=777)
    source.out = True
    target = _msg(2884, text="target caption", doc_id=123)
    target.out = True
    telethon = FakeTelethon(by_id={1158: None}, history=[target, source])
    resolver = TelethonAuthoritativeMessageResolver(telethon)

    result = asyncio.run(resolver.resolve_authoritative_single_message(
        target_entity=-1002693516250,
        target_id=-1002693516250,
        sent=sent,
        expected_message=source,
        expected_text="source caption",
        before_max_message_id=500,
        send_started_at=now,
        send_finished_at=now,
        source_message_ids={525},
    ))

    assert result.ok is True
    assert result.authoritative_message_id == 2884
    assert result.resolution_method == "target_history_unique_new_outbound"


def test_two_unique_fallback_candidates_are_unresolved():
    now = datetime.now(timezone.utc)
    sent = _msg(1158, text="wrong", doc_id=999)
    one = _msg(2884, text="target one", doc_id=1)
    two = _msg(2885, text="target two", doc_id=2)
    one.out = two.out = True
    telethon = FakeTelethon(by_id={1158: None}, history=[two, one])
    resolver = TelethonAuthoritativeMessageResolver(telethon)

    result = asyncio.run(resolver.resolve_authoritative_single_message(
        target_entity=-100,
        target_id=-100,
        sent=sent,
        expected_message=_msg(525, text="source caption", doc_id=777),
        expected_text="source caption",
        before_max_message_id=2883,
        send_started_at=now,
        send_finished_at=now,
    ))

    assert result.ok is False
    assert result.authoritative_message_id is None
    assert result.resolution_method == "unresolved"


def test_unique_fallback_rejects_wrong_media_kind():
    now = datetime.now(timezone.utc)
    sent = _msg(1158, text="wrong", doc_id=999)
    text_post = SimpleNamespace(id=2884, message="target text", date=now, out=True)
    telethon = FakeTelethon(by_id={1158: None}, history=[text_post])
    resolver = TelethonAuthoritativeMessageResolver(telethon)

    result = asyncio.run(resolver.resolve_authoritative_single_message(
        target_entity=-100,
        target_id=-100,
        sent=sent,
        expected_message=_msg(525, text="source caption", doc_id=777),
        expected_text="source caption",
        before_max_message_id=2883,
        send_started_at=now,
        send_finished_at=now,
    ))

    assert result.ok is False
    assert result.authoritative_message_id is None


def test_unique_fallback_rejects_wrong_thread():
    now = datetime.now(timezone.utc)
    sent = _msg(1158, text="wrong", doc_id=999)
    other_topic = _thread_msg(2884, thread_id=20, post=False, out=True, text="target")
    telethon = FakeTelethon(by_id={1158: None}, history=[other_topic])
    resolver = TelethonAuthoritativeMessageResolver(telethon)

    result = asyncio.run(resolver.resolve_authoritative_single_message(
        target_entity=-100,
        target_id=-100,
        sent=sent,
        expected_message=_msg(525, text="source caption", doc_id=777),
        expected_text="source caption",
        before_max_message_id=2883,
        send_started_at=now,
        send_finished_at=now,
        target_thread_id=10,
    ))

    assert result.ok is False
    assert result.authoritative_message_id is None


def test_broadcast_history_post_out_false_resolves_unique_new_outbound():
    now = datetime.now(timezone.utc)
    sent = _msg(1159, text="returned wrong", doc_id=999)
    target = _msg(2885, text="real broadcast post", doc_id=123)
    target.post = True
    target.out = False
    telethon = FakeTelethon(by_id={1159: None}, history=[target])
    resolver = TelethonAuthoritativeMessageResolver(telethon)

    result = asyncio.run(resolver.resolve_authoritative_single_message(
        target_entity=-1002693516250,
        target_id=-1002693516250,
        sent=sent,
        expected_message=_msg(526, text="source caption", doc_id=777),
        expected_text="source caption",
        before_max_message_id=2884,
        send_started_at=now,
        send_finished_at=now,
        source_message_ids={526},
        target_is_broadcast_channel=True,
    ))

    assert result.ok is True
    assert result.returned_candidate_id == 1159
    assert result.authoritative_message_id == 2885
    assert result.resolution_method == "target_history_unique_new_outbound"


def test_broadcast_history_requires_channel_post():
    now = datetime.now(timezone.utc)
    sent = _msg(1159, text="returned wrong", doc_id=999)
    target = _msg(2885, text="real broadcast post", doc_id=123)
    target.post = False
    target.out = False
    telethon = FakeTelethon(by_id={1159: None}, history=[target])
    resolver = TelethonAuthoritativeMessageResolver(telethon)

    result = asyncio.run(resolver.resolve_authoritative_single_message(
        target_entity=-1002693516250,
        target_id=-1002693516250,
        sent=sent,
        expected_message=_msg(526, text="source caption", doc_id=777),
        expected_text="source caption",
        before_max_message_id=2884,
        send_started_at=now,
        send_finished_at=now,
        target_is_broadcast_channel=True,
    ))

    assert result.ok is False
    assert result.authoritative_message_id is None


def test_group_history_out_false_is_rejected():
    now = datetime.now(timezone.utc)
    sent = _msg(1159, text="returned wrong", doc_id=999)
    foreign = _thread_msg(2885, post=False, out=False, text="foreign", doc_id=123)
    telethon = FakeTelethon(by_id={1159: None}, history=[foreign])
    resolver = TelethonAuthoritativeMessageResolver(telethon)

    result = asyncio.run(resolver.resolve_authoritative_single_message(
        target_entity=-100,
        target_id=-100,
        sent=sent,
        expected_message=_msg(526, text="source caption", doc_id=777),
        expected_text="source caption",
        before_max_message_id=2884,
        send_started_at=now,
        send_finished_at=now,
        target_is_broadcast_channel=False,
    ))

    assert result.ok is False
    assert result.authoritative_message_id is None


def test_broadcast_two_new_posts_are_unresolved_no_guess():
    now = datetime.now(timezone.utc)
    sent = _msg(1159, text="returned wrong", doc_id=999)
    one = _msg(2885, text="one", doc_id=123)
    two = _msg(2886, text="two", doc_id=124)
    one.post = two.post = True
    one.out = two.out = False
    telethon = FakeTelethon(by_id={1159: None}, history=[two, one])
    resolver = TelethonAuthoritativeMessageResolver(telethon)

    result = asyncio.run(resolver.resolve_authoritative_single_message(
        target_entity=-1002693516250,
        target_id=-1002693516250,
        sent=sent,
        expected_message=_msg(526, text="source caption", doc_id=777),
        expected_text="source caption",
        before_max_message_id=2884,
        send_started_at=now,
        send_finished_at=now,
        target_is_broadcast_channel=True,
    ))

    assert result.ok is False
    assert result.authoritative_message_id is None
    assert result.resolution_method == "unresolved"
    assert telethon.send_file_calls == 0
