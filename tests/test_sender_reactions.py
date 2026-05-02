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
