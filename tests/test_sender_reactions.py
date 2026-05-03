import asyncio
import inspect
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
    assert svc._extract_sent_message_ids({"result": {"message_id": 7}}) == [7]
    assert svc._extract_sent_message_ids(SimpleNamespace(result=SimpleNamespace(message_id=8))) == [8]
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


def test_mark_delivery_sent_sync_uses_passed_sent_message_id():
    class FakeDb:
        def __init__(self):
            self.captured = None

        def mark_delivery_sent_with_target_message(self, delivery_id, **kwargs):
            self.captured = (delivery_id, kwargs)

    svc = _service()
    svc.db = FakeDb()
    svc._mark_delivery_sent_sync(
        99,
        sent_message_id=12345,
        sent_message_ids=[12345, 12346],
        target_id="-1001",
        delivery_method="reupload_single",
    )
    assert svc.db.captured is not None
    delivery_id, kwargs = svc.db.captured
    assert delivery_id == 99
    assert kwargs["sent_message_id"] == 12345
    assert kwargs["sent_message_ids"] == [12345, 12346]
    assert kwargs["target_id"] == "-1001"
    assert kwargs["delivery_method"] == "reupload_single"


def test_mark_delivery_sent_sync_has_no_authoritative_sent_message_id_reference():
    source = inspect.getsource(SenderService._mark_delivery_sent_sync)
    assert "authoritative_sent_message_id" not in source


def test_copy_single_path_does_not_reference_unbound_valid_sent_message_ids():
    source = inspect.getsource(SenderService._deliver_single)
    copy_single_block_start = source.index("if use_copy_first:")
    mark_sent_call = source.index("delivery_method=\"copy_single\"")
    block = source[copy_single_block_start:mark_sent_call]
    assert "pipeline_result=\"ok\" if valid_sent_message_ids else \"failed\"" not in block
    assert "sent_message_ids=valid_sent_message_ids" not in block


def test_copy_single_uncertain_result_stops_without_reupload():
    source = inspect.getsource(SenderService._deliver_single)
    assert "COPY_SINGLE_UNCERTAIN_NO_FALLBACK" in source
    assert "if copy_result.get(\"attempted\")" in source
    assert "return False" in source[source.index("COPY_SINGLE_UNCERTAIN_NO_FALLBACK"):source.index("COPY_TO_REUPLOAD_FALLBACK_ALLOWED")]


def test_copy_single_has_confirmation_gate_before_mark_sent():
    source = inspect.getsource(SenderService._deliver_single)
    confirm_call_pos = source.index("method=\"copy_single\"")
    mark_sent_pos = source.index("delivery_method=\"copy_single\"")
    assert confirm_call_pos < mark_sent_pos


def test_copy_single_fallback_allowed_only_when_not_attempted():
    source = inspect.getsource(SenderService._deliver_single)
    allowed_pos = source.index("COPY_TO_REUPLOAD_FALLBACK_ALLOWED")
    attempted_guard_pos = source.rfind("if copy_result.get(\"attempted\")", 0, allowed_pos)
    assert attempted_guard_pos != -1
