from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from app.db import Database, USER_TZ, utc_now_iso


def _init_db(tmp_path):
    db = Database(str(tmp_path / "guardrails.sqlite3"))
    db.init()
    return db


def _seed_rule_with_backlog(
    db: Database,
    *,
    mode: str,
    schedule_mode: str,
    fixed_times_json: str | None,
    interval: int = 3600,
    items: int = 2,
) -> int:
    now_iso = utc_now_iso()
    with db.connect() as conn:
        cur = conn.execute(
            """
            INSERT INTO routing(
                source_id, source_thread_id, target_id, target_thread_id,
                interval, schedule_mode, fixed_times_json, mode,
                created_by, created_date, is_active, next_run_at
            ) VALUES (?, NULL, ?, NULL, ?, ?, ?, ?, 1, ?, 1, ?)
            """,
            ("src", "dst", interval, schedule_mode, fixed_times_json, mode, now_iso, now_iso),
        )
        rule_id = int(cur.lastrowid)

        for idx in range(items):
            content_json = {"media_kind": "video"} if mode == "video" else {"text": f"msg-{idx}"}
            post_cur = conn.execute(
                """
                INSERT INTO posts(message_id, source_channel, source_thread_id, content_json, media_group_id, created_at)
                VALUES (?, ?, NULL, ?, NULL, ?)
                """,
                (1000 + idx, "src", json.dumps(content_json), now_iso),
            )
            post_id = int(post_cur.lastrowid)
            conn.execute(
                """
                INSERT INTO deliveries(rule_id, post_id, status, created_at)
                VALUES (?, ?, 'pending', ?)
                """,
                (rule_id, post_id, now_iso),
            )
        conn.commit()

    return rule_id


def _next_local_slot_json(minutes_ahead: int = 10) -> str:
    local_dt = datetime.now(timezone.utc).astimezone(USER_TZ) + timedelta(minutes=minutes_ahead)
    return json.dumps([f"{local_dt.hour:02d}:{local_dt.minute:02d}"])


def test_backlog_is_rate_limited_for_all_mode_combinations(tmp_path) -> None:
    db = _init_db(tmp_path)

    scenarios = [
        ("repost", "interval", None),
        ("repost", "fixed", _next_local_slot_json()),
        ("video", "interval", None),
        ("video", "fixed", _next_local_slot_json()),
    ]

    for mode, schedule_mode, fixed_times_json in scenarios:
        rule_id = _seed_rule_with_backlog(
            db,
            mode=mode,
            schedule_mode=schedule_mode,
            fixed_times_json=fixed_times_json,
            interval=3600,
            items=2,
        )

        due_iso = utc_now_iso()
        first = db.take_due_delivery(rule_id, due_iso)
        assert first is not None

        db.mark_delivery_sent(int(first["delivery_id"]))
        db.touch_rule_after_send(rule_id, 3600)

        next_rule = db.get_rule(rule_id)
        assert next_rule is not None
        assert next_rule.next_run_at is not None

        second = db.take_due_delivery(rule_id, (datetime.now(timezone.utc) + timedelta(seconds=1)).isoformat())
        assert second is None, f"second delivery must wait by schedule for scenario={mode}+{schedule_mode}"


def test_fixed_invalid_payload_falls_back_safely(tmp_path) -> None:
    db = _init_db(tmp_path)
    invalid_values = [None, "[]", "{broken", json.dumps(["99:99", "trash", ""]) ]

    for raw_fixed in invalid_values:
        rule_id = _seed_rule_with_backlog(
            db,
            mode="repost",
            schedule_mode="fixed",
            fixed_times_json=raw_fixed,
            interval=1800,
            items=2,
        )
        first = db.take_due_delivery(rule_id, utc_now_iso())
        assert first is not None
        db.mark_delivery_sent(int(first["delivery_id"]))
        db.touch_rule_after_send(rule_id, 1800)

        rule = db.get_rule(rule_id)
        assert rule is not None
        assert rule.next_run_at is not None


def test_take_due_delivery_repairs_null_next_run_when_backlog_exists(tmp_path) -> None:
    db = _init_db(tmp_path)
    rule_id = _seed_rule_with_backlog(
        db,
        mode="video",
        schedule_mode="interval",
        fixed_times_json=None,
        interval=7200,
        items=2,
    )

    with db.connect() as conn:
        conn.execute("UPDATE routing SET next_run_at = NULL WHERE id = ?", (rule_id,))
        conn.commit()

    taken = db.take_due_delivery(rule_id, utc_now_iso())
    assert taken is None

    repaired = db.get_rule(rule_id)
    assert repaired is not None
    assert repaired.next_run_at is not None


def test_only_one_logical_item_per_allowed_slot_with_repeated_ticks_and_worker(tmp_path) -> None:
    db = _init_db(tmp_path)
    rule_id = _seed_rule_with_backlog(
        db,
        mode="repost",
        schedule_mode="interval",
        fixed_times_json=None,
        interval=7200,
        items=3,
    )

    first_due = utc_now_iso()
    first = db.take_due_delivery(rule_id, first_due)
    assert first is not None
    first_delivery_id = int(first["delivery_id"])

    # repeated scheduler ticks while first logical item in processing must not take more
    for _ in range(3):
        assert db.take_due_delivery(rule_id, first_due) is None

    # worker completes first item and may be polled repeatedly in the same slot
    db.mark_delivery_sent(first_delivery_id)
    db.touch_rule_after_send(rule_id, 7200)
    db.touch_rule_after_send(rule_id, 7200)

    # repeated scheduler/worker activity before due still must not drain backlog
    one_hour_later = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
    for _ in range(3):
        assert db.take_due_delivery(rule_id, one_hour_later) is None

    # next slot: exactly one next logical item can be taken
    after_due = (datetime.now(timezone.utc) + timedelta(hours=2, seconds=1)).isoformat()
    second = db.take_due_delivery(rule_id, after_due)
    assert second is not None
    second_delivery_id = int(second["delivery_id"])
    assert second_delivery_id != first_delivery_id

    with db.connect() as conn:
        processing_cnt = int(conn.execute("SELECT COUNT(*) AS cnt FROM deliveries WHERE rule_id = ? AND status = 'processing'", (rule_id,)).fetchone()["cnt"])
        pending_cnt = int(conn.execute("SELECT COUNT(*) AS cnt FROM deliveries WHERE rule_id = ? AND status = 'pending'", (rule_id,)).fetchone()["cnt"])
    assert processing_cnt == 1
    assert pending_cnt == 1
