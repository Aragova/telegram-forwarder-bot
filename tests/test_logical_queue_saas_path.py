from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
import sys
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.postgres_repository import PostgresRepository


class _FakeCursor:
    def __init__(self, script: dict):
        self.script = script
        self._fetchall = []
        self._fetchone = None
        self.executemany_calls: list[tuple[str, list[tuple]]] = []
        self.execute_calls: list[tuple[str, tuple | None]] = []
        self.rowcount = 1
        self.script = script

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, query, params=None):
        q = " ".join(str(query).split())
        self.execute_calls.append((q, params))

        if "rescan_owner_token IS NULL AND rescan_operation IS NULL FOR UPDATE" in q:
            blocked = self.script.get("rescan_owned") or self.script.get("rescan_retry_pending")
            self._fetchone = None if blocked else {"id": params[0], "mode": "repost"}
            return

        if "SELECT 1 FROM telegram_publication_receipts" in q:
            self._fetchone = self.script.get("unfinished_receipt")
            return

        if ("FROM deliveries d JOIN posts p ON p.id = d.post_id "
                "JOIN routing r ON r.id = d.rule_id WHERE d.rule_id = %s "
                "ORDER BY CASE WHEN r.rescan_queue_built THEN p.message_id ELSE p.id END, p.id ASC") in q:
            self._fetchall = self.script.get("rows", [])
            return

        if "FROM deliveries WHERE id = ANY(%s) ORDER BY id ASC" in q:
            self._fetchall = self.script.get("current_rows", [])
            return

        if "DELETE FROM delivery_attempts WHERE delivery_id = ANY(%s)" in q:
            self.rowcount = int(self.script.get("deleted_attempts_count", 0))
            return

        self._fetchall = []
        self._fetchone = None

    def executemany(self, query, seq):
        q = " ".join(str(query).split())
        payload = list(seq)
        self.executemany_calls.append((q, payload))

    def fetchall(self):
        return self._fetchall

    def fetchone(self):
        return self._fetchone


class _FakeConn:
    def __init__(self, script: dict):
        self.cursor_obj = _FakeCursor(script)
        self.committed = False

    @contextmanager
    def cursor(self):
        yield self.cursor_obj

    def commit(self):
        self.committed = True


class _RepoForTests(PostgresRepository):
    def __init__(self, script: dict | None = None):
        self.client = None
        self._script = script or {}
        self.fake_conn = _FakeConn(self._script)
        self._rule = SimpleNamespace(mode="repost")

    @contextmanager
    def connect(self):
        yield self.fake_conn

    def get_rule(self, rule_id: int):
        return self._rule


def _mk_row(
    delivery_id: int,
    *,
    post_id: int,
    message_id: int,
    status: str,
    source_channel: str = "src",
    source_thread_id: int | None = None,
    media_group_id: str | None = None,
    media_kind: str = "text",
):
    return {
        "delivery_id": delivery_id,
        "status": status,
        "sent_at": None,
        "error_text": None,
        "attempt_count": 0,
        "post_id": post_id,
        "message_id": message_id,
        "source_channel": source_channel,
        "source_thread_id": source_thread_id,
        "media_group_id": media_group_id,
        "content_json": {"media_kind": media_kind},
        "created_at": "2026-01-01T00:00:00+00:00",
    }


def test_repost_album_is_single_logical_item():
    repo = _RepoForTests()
    rows = [
        _mk_row(1, post_id=101, message_id=201, status="pending", media_group_id="alb-1"),
        _mk_row(2, post_id=102, message_id=202, status="pending", media_group_id="alb-1"),
        _mk_row(3, post_id=103, message_id=203, status="pending"),
    ]

    items = repo._build_rule_logical_items_from_rows(rule_id=10, rows=rows, mode="repost")

    assert len(items) == 2
    assert items[0]["kind"] == "album"
    assert items[0]["count"] == 2
    assert items[1]["kind"] == "single"
    assert items[1]["count"] == 1


def test_video_album_does_not_collapse_and_skips_non_video():
    repo = _RepoForTests()
    rows = [
        _mk_row(1, post_id=101, message_id=201, status="pending", media_group_id="alb-1", media_kind="video"),
        _mk_row(2, post_id=102, message_id=202, status="pending", media_group_id="alb-1", media_kind="video"),
        _mk_row(3, post_id=103, message_id=203, status="pending", media_group_id="alb-1", media_kind="photo"),
    ]

    items = repo._build_rule_logical_items_from_rows(rule_id=20, rows=rows, mode="video")

    assert len(items) == 2
    assert all(item["kind"] == "video_single" for item in items)
    assert [item["first_post_id"] for item in items] == [101, 102]


def test_rollback_repost_rolls_back_whole_album():
    script = {
        "rows": [
            _mk_row(1, post_id=101, message_id=201, status="sent", media_group_id="alb-1"),
            _mk_row(2, post_id=102, message_id=202, status="sent", media_group_id="alb-1"),
            _mk_row(3, post_id=103, message_id=203, status="pending"),
        ],
        "current_rows": [
            {"id": 1, "post_id": 101, "status": "sent", "sent_at": "x", "error_text": None, "attempt_count": 1, "sent_message_id": 9001, "sent_message_ids_json": "[9001,9002]", "target_id_snapshot": "target-a", "delivery_method": "copy"},
            {"id": 2, "post_id": 102, "status": "sent", "sent_at": "x", "error_text": None, "attempt_count": 1, "sent_message_id": 9002, "sent_message_ids_json": "[9001,9002]", "target_id_snapshot": "target-a", "delivery_method": "copy"},
        ],
        "deleted_attempts_count": 2,
    }
    repo = _RepoForTests(script=script)
    repo._rule = SimpleNamespace(mode="repost")

    result = repo.rollback_last_delivery(rule_id=30, admin_id=1)

    assert result is not None
    assert result["rolled_back_delivery_ids"] == [1, 2]

    updates = [
        call for call in repo.fake_conn.cursor_obj.executemany_calls
        if "UPDATE deliveries SET status = 'pending'" in call[0]
    ]
    assert len(updates) == 1
    assert "sent_message_id = NULL" in updates[0][0]
    assert "sent_message_ids_json = NULL" in updates[0][0]
    assert "delivery_method = NULL" in updates[0][0]
    assert [x[0] for x in updates[0][1]] == [1, 2]

    delete_calls = [
        call for call in repo.fake_conn.cursor_obj.execute_calls
        if "DELETE FROM delivery_attempts WHERE delivery_id = ANY(%s)" in call[0]
    ]
    assert len(delete_calls) == 1
    assert delete_calls[0][1] == ([1, 2],)
    assert repo.fake_conn.committed is True


def test_start_from_position_marks_previous_logical_items_sent():
    repo = _RepoForTests()
    repo._get_rule_pending_logical_items_conn = lambda conn, rule_id, mode: [
        {"position": 1, "kind": "album", "delivery_ids": [1, 2], "first_post_id": 101, "first_message_id": 201, "count": 2},
        {"position": 2, "kind": "single", "delivery_ids": [3], "first_post_id": 103, "first_message_id": 203, "count": 1},
        {"position": 3, "kind": "single", "delivery_ids": [4], "first_post_id": 104, "first_message_id": 204, "count": 1},
    ]

    selected = repo.set_rule_start_from_position(rule_id=40, position=3)

    assert selected is not None
    assert selected["position"] == 3

    updates = [
        call for call in repo.fake_conn.cursor_obj.executemany_calls
        if "UPDATE deliveries SET status = 'sent'" in call[0]
    ]
    assert len(updates) == 1
    assert [x[1] for x in updates[0][1]] == [1, 2, 3]
    assert repo.fake_conn.committed is True

def test_rollback_refuses_unfinished_publication():
    repo = _RepoForTests(script={"unfinished_receipt": {"exists": 1}})
    assert repo.rollback_last_delivery(rule_id=30, admin_id=1) is None
    assert not repo.fake_conn.cursor_obj.executemany_calls


def test_manual_queue_mutations_refused_during_rescan():
    script = {
        "rows": [
            _mk_row(1, post_id=101, message_id=201, status="sent"),
            _mk_row(2, post_id=102, message_id=202, status="pending"),
            _mk_row(3, post_id=103, message_id=203, status="pending"),
        ],
        "current_rows": [
            {"id": 1, "post_id": 101, "status": "sent", "sent_at": "x",
             "error_text": None, "attempt_count": 1, "sent_message_id": 901,
             "sent_message_ids_json": "[901]", "target_id_snapshot": "target",
             "delivery_method": "copy"},
        ],
    }
    def setup(owned, retry_pending=False):
        repo = _RepoForTests(script={**script, "rescan_owned": owned, "rescan_retry_pending": retry_pending})
        repo._get_rule_pending_logical_items_conn = lambda conn, rule_id, mode: [
            {"position": 1, "kind": "single", "delivery_ids": [2],
             "first_post_id": 102, "first_message_id": 202, "count": 1},
            {"position": 2, "kind": "single", "delivery_ids": [3],
             "first_post_id": 103, "first_message_id": 203, "count": 1},
        ]
        return repo

    owned = setup(True)
    assert owned.rollback_last_delivery(30) is None
    assert owned.set_rule_start_from_position(30, 2) is None
    assert not owned.fake_conn.cursor_obj.executemany_calls

    retry_pending = setup(False, retry_pending=True)
    assert retry_pending.rollback_last_delivery(30) is None
    assert retry_pending.set_rule_start_from_position(30, 2) is None
    assert not retry_pending.fake_conn.cursor_obj.executemany_calls

    available = setup(False)
    assert available.rollback_last_delivery(30) is not None
    assert available.set_rule_start_from_position(30, 2) is not None
    assert available.fake_conn.cursor_obj.executemany_calls


def test_rollback_uses_latest_album_member_timestamp_not_queue_position():
    rows = [
        _mk_row(1, post_id=101, message_id=201, status="sent", media_group_id="album"),
        _mk_row(2, post_id=102, message_id=202, status="sent", media_group_id="album"),
        _mk_row(3, post_id=103, message_id=203, status="sent"),
    ]
    for row, ts in zip(rows, ["2026-01-01T00:00:00+00:00",
                              "2026-01-03T00:00:00+00:00",
                              "2026-01-02T00:00:00+00:00"]):
        row["sent_at"] = ts
    current = [
        {"id": row["delivery_id"], "post_id": row["post_id"], "status": "sent",
         "sent_at": row["sent_at"], "error_text": None, "attempt_count": 1,
         "sent_message_id": 9000 + row["delivery_id"], "sent_message_ids_json": "[9001,9002]",
         "target_id_snapshot": "target-a", "delivery_method": "copy"}
        for row in rows[:2]
    ]
    repo = _RepoForTests(script={"rows": rows, "current_rows": current, "deleted_attempts_count": 2})
    repo._rule = SimpleNamespace(mode="repost")
    result = repo.rollback_last_delivery(rule_id=30, admin_id=1)
    assert result["rolled_back_delivery_ids"] == [1, 2]


def test_compact_logical_projection_matches_canonical_builder():
    repo = _RepoForTests()
    rows = [
        _mk_row(1, post_id=11, message_id=20, status="sent", media_group_id="a", media_kind="video"),
        _mk_row(2, post_id=12, message_id=21, status="pending"),
        _mk_row(3, post_id=13, message_id=22, status="faulty", media_group_id="a", media_kind="video"),
        _mk_row(4, post_id=14, message_id=23, status="processing", media_group_id="a", source_thread_id=9, media_kind="video"),
        _mk_row(5, post_id=15, message_id=24, status="sent", media_kind="video"),
    ]
    for mode in ("repost", "video"):
        full = repo._build_rule_logical_items_from_rows(1, rows, mode)
        projected = (
            {key: row[key] for key in ("status", "post_id", "source_channel",
                                       "source_thread_id", "media_group_id", "content_json")}
            for row in rows
        )
        compact = repo._build_rule_logical_items_from_rows(1, projected, mode, compact=True)
        assert repo._summarize_rule_logical_items(compact) == repo._summarize_rule_logical_items(full)
        assert all("content_json" not in item and "delivery_ids" not in item for item in compact)


def test_non_object_video_json_does_not_break_other_publications():
    repo = _RepoForTests()
    rows = [
        _mk_row(i, post_id=i, message_id=i, status="pending", media_kind="video")
        for i in range(1, 8)
    ]
    for row, raw in zip(rows, ("null", "[]", '"video"', "42", "true", "broken")):
        row["content_json"] = raw
    for compact in (False, True):
        items = repo._build_rule_logical_items_from_rows(1, iter(rows), "video", compact=compact)
        assert repo._summarize_rule_logical_items(items)["pending"] == 1
