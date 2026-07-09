from types import SimpleNamespace

import pytest

from app.sender import SenderService
from app.sender_state_sync_helpers import SenderStateSyncHelpers


class FakeDb:
    def __init__(self):
        self.sent_with_target = []
        self.sent = []
        self.many_sent = []
        self.faulty = []
        self.post_ids = {}
        self.intros = {}

    def mark_delivery_sent_with_target_message(self, delivery_id, **kwargs):
        self.sent_with_target.append((delivery_id, kwargs))

    def mark_delivery_sent(self, delivery_id):
        self.sent.append(delivery_id)

    def mark_many_deliveries_sent(self, delivery_ids):
        self.many_sent.append(delivery_ids)

    def mark_delivery_faulty(self, delivery_id, error_text):
        self.faulty.append((delivery_id, error_text))

    def get_post_id_by_delivery(self, delivery_id):
        return self.post_ids.get(delivery_id)

    def get_intro_by_id(self, intro_id):
        return self.intros.get(intro_id)


class FakeDbWithoutRich:
    def __init__(self):
        self.sent = []
        self.many_sent = []

    def mark_delivery_sent(self, delivery_id):
        self.sent.append(delivery_id)

    def mark_many_deliveries_sent(self, delivery_ids):
        self.many_sent.append(delivery_ids)


def make_owner(db=None):
    return SimpleNamespace(db=db or FakeDb())


def test_sender_state_sync_helpers_extracted_from_sender():
    from pathlib import Path

    sender_source = Path("app/sender.py").read_text(encoding="utf-8")
    helper_source = Path("app/sender_state_sync_helpers.py").read_text(encoding="utf-8")

    assert "def get_rule_intro_items_sync" in helper_source
    assert "def get_rule_intro_items" in helper_source
    assert "def mark_delivery_sent_sync" in helper_source
    assert "def mark_many_deliveries_sent_sync" in helper_source
    assert "def mark_album_deliveries_sent_sync" in helper_source
    assert "def mark_delivery_faulty_sync" in helper_source
    assert "def get_post_id_by_delivery_sync" in helper_source
    assert "def get_post_id_by_delivery" in helper_source

    assert sender_source.count("def _get_rule_intro_items_sync") == 1
    assert sender_source.count("def _get_rule_intro_items(") == 1
    assert sender_source.count("def _mark_delivery_sent_sync") == 1
    assert sender_source.count("def _mark_many_deliveries_sent_sync") == 1
    assert sender_source.count("def _mark_album_deliveries_sent_sync") == 1
    assert sender_source.count("def _mark_delivery_faulty_sync") == 1
    assert sender_source.count("def _get_post_id_by_delivery_sync") == 1
    assert sender_source.count("def _get_post_id_by_delivery(") == 1


def test_sender_state_sync_helpers_do_not_import_sender():
    from pathlib import Path

    source = Path("app/sender_state_sync_helpers.py").read_text(encoding="utf-8")

    forbidden = [
        "from ." + "sender import",
        "import app." + "sender",
        "import ." + "sender",
    ]

    for item in forbidden:
        assert item not in source


class StubStateSyncHelpers:
    calls = []

    def __init__(self, owner):
        self.owner = owner

    def _record(self, name, *args, **kwargs):
        self.calls.append((name, self.owner, args, kwargs))
        return {"name": name, "args": args, "kwargs": kwargs}

    def get_rule_intro_items_sync(self, *args, **kwargs):
        return self._record("get_rule_intro_items_sync", *args, **kwargs)

    def get_rule_intro_items(self, *args, **kwargs):
        return self._record("get_rule_intro_items", *args, **kwargs)

    def mark_delivery_sent_sync(self, *args, **kwargs):
        return self._record("mark_delivery_sent_sync", *args, **kwargs)

    def mark_many_deliveries_sent_sync(self, *args, **kwargs):
        return self._record("mark_many_deliveries_sent_sync", *args, **kwargs)

    def mark_album_deliveries_sent_sync(self, *args, **kwargs):
        return self._record("mark_album_deliveries_sent_sync", *args, **kwargs)

    def mark_delivery_faulty_sync(self, *args, **kwargs):
        return self._record("mark_delivery_faulty_sync", *args, **kwargs)

    def get_post_id_by_delivery_sync(self, *args, **kwargs):
        return self._record("get_post_id_by_delivery_sync", *args, **kwargs)

    def get_post_id_by_delivery(self, *args, **kwargs):
        return self._record("get_post_id_by_delivery", *args, **kwargs)


@pytest.fixture
def patched_state_sync_helpers(monkeypatch):
    StubStateSyncHelpers.calls = []
    monkeypatch.setattr("app.sender_state_sync_helpers.SenderStateSyncHelpers", StubStateSyncHelpers)
    return StubStateSyncHelpers.calls


def test_sender_wrapper_delegates_get_rule_intro_items_sync(patched_state_sync_helpers):
    service = object.__new__(SenderService); rule = object()
    assert service._get_rule_intro_items_sync(rule)["name"] == "get_rule_intro_items_sync"
    assert patched_state_sync_helpers == [("get_rule_intro_items_sync", service, (rule,), {})]


def test_sender_wrapper_delegates_get_rule_intro_items(patched_state_sync_helpers):
    service = object.__new__(SenderService); rule = object()
    service._get_rule_intro_items(rule)
    assert patched_state_sync_helpers == [("get_rule_intro_items", service, (rule,), {})]


def test_sender_wrapper_delegates_mark_delivery_sent_sync(patched_state_sync_helpers):
    service = object.__new__(SenderService)
    kwargs = dict(sent_message_id=20, sent_message_ids=[20, 21], target_id="-100target", delivery_method="copy_single")
    service._mark_delivery_sent_sync(10, **kwargs)
    assert patched_state_sync_helpers == [("mark_delivery_sent_sync", service, (10,), kwargs)]


def test_sender_wrapper_delegates_mark_many_deliveries_sent_sync(patched_state_sync_helpers):
    service = object.__new__(SenderService)
    service._mark_many_deliveries_sent_sync([1, 2, 3])
    assert patched_state_sync_helpers == [("mark_many_deliveries_sent_sync", service, ([1, 2, 3],), {})]


def test_sender_wrapper_delegates_mark_album_deliveries_sent_sync(patched_state_sync_helpers):
    service = object.__new__(SenderService)
    kwargs = dict(sent_message_ids=[101], target_id="-100target", delivery_method="copy_album")
    service._mark_album_deliveries_sent_sync([1, 2], **kwargs)
    assert patched_state_sync_helpers == [("mark_album_deliveries_sent_sync", service, ([1, 2],), kwargs)]


def test_sender_wrapper_delegates_mark_delivery_faulty_sync(patched_state_sync_helpers):
    service = object.__new__(SenderService)
    service._mark_delivery_faulty_sync(10, "boom")
    assert patched_state_sync_helpers == [("mark_delivery_faulty_sync", service, (10, "boom"), {})]


def test_sender_wrapper_delegates_get_post_id_by_delivery_sync(patched_state_sync_helpers):
    service = object.__new__(SenderService)
    service._get_post_id_by_delivery_sync(10)
    assert patched_state_sync_helpers == [("get_post_id_by_delivery_sync", service, (10,), {})]


def test_sender_wrapper_delegates_get_post_id_by_delivery(patched_state_sync_helpers):
    service = object.__new__(SenderService)
    service._get_post_id_by_delivery(10)
    assert patched_state_sync_helpers == [("get_post_id_by_delivery", service, (10,), {})]


def test_mark_delivery_sent_sync_uses_rich_method_when_available():
    db = FakeDb(); helper = SenderStateSyncHelpers(make_owner(db))
    helper.mark_delivery_sent_sync(10, sent_message_id=20, sent_message_ids=[20, 21], target_id="-100target", delivery_method="copy_single")
    assert db.sent_with_target == [(10, {"sent_message_id": 20, "sent_message_ids": [20, 21], "target_id": "-100target", "delivery_method": "copy_single"})]
    assert db.sent == []


def test_mark_delivery_sent_sync_fallback():
    db = FakeDbWithoutRich(); helper = SenderStateSyncHelpers(make_owner(db))
    helper.mark_delivery_sent_sync(10)
    assert db.sent == [10]


def test_mark_many_deliveries_sent_sync():
    db = FakeDb(); helper = SenderStateSyncHelpers(make_owner(db))
    helper.mark_many_deliveries_sent_sync([1, 2, 3])
    assert db.many_sent == [[1, 2, 3]]


def test_mark_album_deliveries_sent_sync_raises_on_empty_ids():
    helper = SenderStateSyncHelpers(make_owner())
    with pytest.raises(RuntimeError, match="Не удалось определить deliveries альбома"):
        helper.mark_album_deliveries_sent_sync([])


def test_mark_album_deliveries_sent_sync_rich_marking():
    db = FakeDb(); helper = SenderStateSyncHelpers(make_owner(db))
    helper.mark_album_deliveries_sent_sync([1, 2, 3], sent_message_ids=[101, 102], target_id="-100target", delivery_method="copy_album")
    assert db.sent_with_target == [
        (1, {"sent_message_id": 101, "sent_message_ids": [101, 102], "target_id": "-100target", "delivery_method": "copy_album"}),
        (2, {"sent_message_id": 102, "sent_message_ids": [101, 102], "target_id": "-100target", "delivery_method": "copy_album"}),
        (3, {"sent_message_id": 101, "sent_message_ids": [101, 102], "target_id": "-100target", "delivery_method": "copy_album"}),
    ]
    assert db.many_sent == []


def test_mark_album_deliveries_sent_sync_fallback_to_many_sent_without_valid_ids():
    db = FakeDb(); helper = SenderStateSyncHelpers(make_owner(db))
    helper.mark_album_deliveries_sent_sync([1, 2], sent_message_ids=[])
    assert db.many_sent == [[1, 2]]


def test_mark_album_deliveries_sent_sync_fallback_to_many_sent_without_rich_method():
    db = FakeDbWithoutRich(); helper = SenderStateSyncHelpers(make_owner(db))
    helper.mark_album_deliveries_sent_sync([1, 2], sent_message_ids=[101])
    assert db.many_sent == [[1, 2]]


def test_mark_delivery_faulty_sync():
    db = FakeDb(); helper = SenderStateSyncHelpers(make_owner(db))
    helper.mark_delivery_faulty_sync(10, "boom")
    assert db.faulty == [(10, "boom")]


def test_get_post_id_by_delivery():
    db = FakeDb(); db.post_ids = {10: 99}
    helper = SenderStateSyncHelpers(make_owner(db))
    assert helper.get_post_id_by_delivery(10) == 99
    assert helper.get_post_id_by_delivery_sync(10) == 99


def test_get_rule_intro_items():
    db = FakeDb(); db.intros = {1: SimpleNamespace(id=1), 2: SimpleNamespace(id=2)}
    helper = SenderStateSyncHelpers(make_owner(db))
    rule = SimpleNamespace(video_intro_horizontal_id=1, video_intro_vertical_id=2)
    horizontal, vertical = helper.get_rule_intro_items(rule)
    assert horizontal.id == 1
    assert vertical.id == 2


def test_get_rule_intro_items_handles_exceptions():
    class RaisingDb(FakeDb):
        def get_intro_by_id(self, intro_id):
            if intro_id == 1:
                raise RuntimeError("boom")
            return SimpleNamespace(id=intro_id)

    helper = SenderStateSyncHelpers(make_owner(RaisingDb()))
    rule = SimpleNamespace(video_intro_horizontal_id=1, video_intro_vertical_id=2)
    horizontal, vertical = helper.get_rule_intro_items(rule)
    assert horizontal is None
    assert vertical.id == 2
