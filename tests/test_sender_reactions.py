import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from app.sender import SenderService


class FakeTelethon:
    def __init__(self, message=None):
        self.message = message

    async def get_messages(self, entity, ids):
        return self.message


def _service(message=None):
    svc = SenderService.__new__(SenderService)
    svc.telethon = FakeTelethon(message)
    svc.reaction_clients = []
    svc.db = None
    return svc


def test_extract_sent_message_ids_variants():
    svc = _service()
    assert svc._extract_sent_message_ids(SimpleNamespace(message_id=11)) == [11]
    assert svc._extract_sent_message_ids(SimpleNamespace(id=12)) == [12]
    assert svc._extract_sent_message_ids([SimpleNamespace(id=1), SimpleNamespace(message_id=2)]) == [1, 2]
    assert svc._extract_sent_message_ids((SimpleNamespace(id=3), SimpleNamespace(id=4))) == [3, 4]
    assert svc._extract_sent_message_ids({"message_id": 5}) == [5]
    assert svc._extract_sent_message_ids({"id": 6}) == [6]
    assert svc._extract_sent_message_ids(None) == []
    assert svc._extract_sent_message_ids(object()) == []


def test_validate_reaction_target_message():
    now = datetime.now(timezone.utc)
    fresh = SimpleNamespace(id=100, date=now)
    stale = SimpleNamespace(id=100, date=now - timedelta(seconds=1000))

    svc = _service(fresh)
    assert asyncio.run(svc._validate_reaction_target_message(rule_id=1, source_channel='a', target_id='b', source_message_ids=[1], sent_message_id=None)) is None

    svc = _service(None)
    assert asyncio.run(svc._validate_reaction_target_message(rule_id=1, source_channel='a', target_id='b', source_message_ids=[1], sent_message_id=10)) is None

    svc = _service(stale)
    assert asyncio.run(svc._validate_reaction_target_message(rule_id=1, source_channel='a', target_id='b', source_message_ids=[1], sent_message_id=10, max_age_seconds=300)) is None

    svc = _service(fresh)
    assert asyncio.run(svc._validate_reaction_target_message(rule_id=1, source_channel='-100', target_id='-100', source_message_ids=[10], sent_message_id=10)) is None

    svc = _service(fresh)
    assert asyncio.run(svc._validate_reaction_target_message(rule_id=1, source_channel='a', target_id='b', source_message_ids=[1], sent_message_id=10)) == 10


def test_confirm_reaction_without_reactions_returns_strict_false():
    svc = _service(SimpleNamespace(reactions=None))
    confirmed = asyncio.run(svc._confirm_reaction(svc.telethon, entity="chat", message_id=1, emoji="🔥"))
    assert confirmed is False
    assert not confirmed


def test_validate_sent_message_ids_for_delivery_stale_is_skipped():
    now = datetime.now(timezone.utc)
    stale = SimpleNamespace(id=777, date=now - timedelta(seconds=1000))
    svc = _service(stale)

    validated = asyncio.run(
        svc._validate_sent_message_ids_for_delivery(
            rule_id=15,
            delivery_id=460298,
            source_channel="-1002546799428",
            target_id="-1002546799428",
            source_message_ids=[299, 300, 301, 302],
            candidate_sent_message_ids=[777],
            method="reupload_album_unverified_success",
            max_age_seconds=300,
        )
    )
    assert validated == []


def test_validate_sent_message_ids_for_delivery_fresh_ok():
    now = datetime.now(timezone.utc)
    fresh = SimpleNamespace(id=888, date=now)
    svc = _service(fresh)

    validated = asyncio.run(
        svc._validate_sent_message_ids_for_delivery(
            rule_id=15,
            delivery_id=460298,
            source_channel="-100",
            target_id="-200",
            source_message_ids=[1, 2],
            candidate_sent_message_ids=[888],
            method="reupload_album_unverified_success",
        )
    )
    assert validated == [888]


def test_validate_sent_message_ids_for_delivery_self_target_source_id_blocked():
    now = datetime.now(timezone.utc)
    fresh = SimpleNamespace(id=300, date=now)
    svc = _service(fresh)

    validated = asyncio.run(
        svc._validate_sent_message_ids_for_delivery(
            rule_id=15,
            delivery_id=460298,
            source_channel="-1002546799428",
            target_id="-1002546799428",
            source_message_ids=[299, 300, 301, 302],
            candidate_sent_message_ids=[300],
            method="reupload_album_unverified_success",
        )
    )
    assert validated == []


def test_validate_sent_message_ids_for_delivery_passes_full_context():
    svc = _service(SimpleNamespace(id=901, date=datetime.now(timezone.utc)))
    captured = {}

    async def _fake_validate(**kwargs):
        captured.update(kwargs)
        return kwargs["sent_message_id"]

    svc._validate_reaction_target_message = _fake_validate
    validated = asyncio.run(
        svc._validate_sent_message_ids_for_delivery(
            rule_id=15,
            delivery_id=460298,
            source_channel="-1002546799428",
            target_id="-1002546799428",
            source_message_ids=[299, 300],
            candidate_sent_message_ids=[901],
            method="reupload_album_unverified_success",
        )
    )
    assert validated == [901]
    assert captured["delivery_id"] == 460298
    assert captured["source_channel"] == "-1002546799428"
    assert captured["source_message_ids"] == [299, 300]

class RetryTelethon:
    def __init__(self, messages):
        self.messages = list(messages)

    async def get_messages(self, entity, ids):
        if self.messages:
            return self.messages.pop(0)
        return None


def test_confirm_target_delivery_message_ids_rejects_missing():
    svc = _service(None)
    validated = asyncio.run(
        svc._confirm_target_delivery_message_ids(
            rule_id=1,
            delivery_id=2,
            source_channel="-100",
            target_id="-200",
            source_message_ids=[10],
            candidate_sent_message_ids=[55],
            method="reupload_single",
        )
    )
    assert validated == []


def test_confirm_target_delivery_message_ids_self_target_rejects_source_id():
    now = datetime.now(timezone.utc)
    svc = _service(SimpleNamespace(id=10, date=now))
    validated = asyncio.run(
        svc._confirm_target_delivery_message_ids(
            rule_id=1,
            delivery_id=2,
            source_channel="-100",
            target_id="-100",
            source_message_ids=[10],
            candidate_sent_message_ids=[10],
            method="reupload_single",
        )
    )
    assert validated == []


def test_confirm_target_delivery_message_ids_with_retry_eventual_success():
    now = datetime.now(timezone.utc)
    svc = _service()
    svc.telethon = RetryTelethon([None, SimpleNamespace(id=77, date=now)])

    validated = asyncio.run(
        svc._confirm_target_delivery_message_ids_with_retry(
            rule_id=1,
            delivery_id=2,
            source_channel="-100",
            target_id="-200",
            source_message_ids=[10],
            candidate_sent_message_ids=[77],
            method="reupload_single",
        )
    )
    assert validated == [77]
