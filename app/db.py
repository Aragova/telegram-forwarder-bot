from __future__ import annotations
import sqlite3, json
import logging
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any
from .config import settings


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_fixed_times(times: list[str]) -> list[str]:
    normalized = []

    for raw in times:
        value = raw.strip()
        if not value:
            continue

        parts = value.split(":")
        if len(parts) != 2:
            continue

        try:
            hour = int(parts[0])
            minute = int(parts[1])
        except Exception:
            continue

        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            continue

        normalized.append(f"{hour:02d}:{minute:02d}")

    return sorted(set(normalized))

logger = logging.getLogger("forwarder.db")
USER_TZ = timezone(timedelta(hours=3))
GLOBAL_INTERVAL_GAP_SECONDS = 180  # 3 минуты между плавающими правилами

def get_next_fixed_run_utc(fixed_times: list[str], now_utc: datetime | None = None) -> str | None:
    if not fixed_times:
        return None

    now_utc = now_utc or datetime.now(timezone.utc)
    now_local = now_utc.astimezone(USER_TZ)

    candidates: list[datetime] = []

    for day_shift in (0, 1, 2):
        base_day = now_local + timedelta(days=day_shift)

        for time_str in fixed_times:
            hour, minute = map(int, time_str.split(":"))

            local_dt = base_day.replace(
                hour=hour,
                minute=minute,
                second=0,
                microsecond=0,
            )

            if local_dt <= now_local:
                continue

            candidates.append(local_dt.astimezone(timezone.utc))

    if not candidates:
        return None

    return min(candidates).isoformat()

@dataclass(slots=True)
class Rule:
    id: int
    source_id: str
    source_thread_id: int | None
    target_id: str
    target_thread_id: int | None

    interval: int
    schedule_mode: str
    fixed_times_json: str | None

    is_active: bool
    created_date: str
    next_run_at: str | None
    last_sent_at: str | None

    source_title: str | None = None
    target_title: str | None = None
    mode: str = "repost"

    video_trim_seconds: int = 120
    video_add_intro: bool = False

    video_intro_horizontal: str | None = None
    video_intro_vertical: str | None = None
    video_intro_horizontal_id: int | None = None
    video_intro_vertical_id: int | None = None

    video_caption: str | None = None
    video_caption_entities_json: str | None = None
    caption_delivery_mode: str = "auto"
    video_caption_delivery_mode: str = "auto"

    def fixed_times(self) -> list[str]:
        if not self.fixed_times_json:
            return []
        try:
            return json.loads(self.fixed_times_json)
        except Exception:
            return []

@dataclass(slots=True)
class IntroItem:
    id: int
    display_name: str
    file_name: str
    file_path: str
    duration: int
    created_at: str

class Database:
    def reset_queue_for_source(self, source_id: str, source_thread_id: int | None = None) -> int:
        with self.connect() as conn:
            if source_thread_id is None:
                cur = conn.execute("""
                    UPDATE deliveries
                    SET status = 'pending',
                        error_text = NULL,
                        attempt_count = 0,
                        sent_at = NULL
                    WHERE post_id IN (
                        SELECT id
                        FROM posts
                        WHERE source_channel = ?
                          AND source_thread_id IS NULL
                    )
                      AND status = 'sent'
                """, (str(source_id),))
            else:
                cur = conn.execute("""
                    UPDATE deliveries
                    SET status = 'pending',
                        error_text = NULL,
                        attempt_count = 0,
                        sent_at = NULL
                    WHERE post_id IN (
                        SELECT id
                        FROM posts
                        WHERE source_channel = ?
                          AND source_thread_id = ?
                    )
                      AND status = 'sent'
                """, (str(source_id), source_thread_id))
            conn.commit()
            return cur.rowcount
    def get_post(
        self,
        source_channel: str,
        source_thread_id: int | None,
        message_id: int,
    ) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT *
                FROM posts
                WHERE source_channel = ?
                  AND (
                        (source_thread_id IS NULL AND ? IS NULL)
                        OR source_thread_id = ?
                  )
                  AND message_id = ?
                LIMIT 1
                """,
                (
                    str(source_channel),
                    source_thread_id,
                    source_thread_id,
                    int(message_id),
                ),
            ).fetchone()

        return dict(row) if row else None

    def reset_all_queue(self) -> tuple[int, int]:
        with self.connect() as conn:
            faulty = conn.execute(
                "SELECT COUNT(*) AS cnt FROM deliveries WHERE status = 'faulty'"
            ).fetchone()["cnt"]
    
            cur = conn.execute("""
                UPDATE deliveries
                SET status = 'pending',
                    error_text = NULL,
                    attempt_count = 0,
                    sent_at = NULL
                WHERE status = 'sent'
            """)
            conn.commit()
            return cur.rowcount, faulty
    def __init__(self, path: str | None = None) -> None:
        self.path = path or settings.db_path

    def rollback_last_delivery(self, rule_id: int, admin_id: int | None = None):
        """
        Откат последнего УЖЕ ОТПРАВЛЕННОГО ЛОГИЧЕСКОГО элемента правила.

        ВАЖНО:
        - работает не по одной delivery-строке, а по логическому элементу
        (single / album / video_single)
        - использует тот же builder, что queue / position / start-from-position
        - возвращает весь логический элемент обратно в pending
        - сбрасывает sent_at / error_text / attempt_count
        - двигает next_run_at на сейчас
        """
        try:
            rule = self.get_rule(rule_id)
            if not rule:
                logger.warning(
                    "rollback_last_delivery: правило не найдено, rule_id=%s",
                    rule_id,
                )
                return None

            mode = (getattr(rule, "mode", "repost") or "repost").strip().lower()
            now_iso = utc_now_iso()

            with self.connect() as conn:
                rows = conn.execute("""
                    SELECT
                        d.id AS delivery_id,
                        d.status,
                        d.sent_at,
                        p.id AS post_id,
                        p.message_id,
                        p.source_channel,
                        p.source_thread_id,
                        p.media_group_id,
                        p.content_json,
                        p.created_at
                    FROM deliveries d
                    JOIN posts p ON p.id = d.post_id
                    WHERE d.rule_id = ?
                    ORDER BY p.id ASC
                """, (rule_id,)).fetchall()

                if not rows:
                    logger.info(
                        "rollback_last_delivery: у правила нет deliveries, rule_id=%s",
                        rule_id,
                    )
                    return None

                logical_items = self._build_rule_logical_items_from_rows(
                    rule_id=rule_id,
                    rows=rows,
                    mode=mode,
                )

                if not logical_items:
                    logger.info(
                        "rollback_last_delivery: builder вернул пустой список, rule_id=%s, mode=%s",
                        rule_id,
                        mode,
                    )
                    return None

                completed_items = [item for item in logical_items if item.get("is_done")]
                if not completed_items:
                    logger.info(
                        "rollback_last_delivery: нет завершённых логических элементов для отката, rule_id=%s",
                        rule_id,
                    )
                    return None

                selected = completed_items[-1]
                delivery_ids = [int(x) for x in selected["delivery_ids"]]
                post_ids = [int(x) for x in selected["post_ids"]]
                message_ids = [int(x) for x in selected["message_ids"]]

                placeholders = ",".join("?" for _ in delivery_ids)
                current_rows = conn.execute(f"""
                    SELECT
                        id,
                        post_id,
                        status,
                        sent_at,
                        error_text,
                        attempt_count
                    FROM deliveries
                    WHERE id IN ({placeholders})
                    ORDER BY id ASC
                """, delivery_ids).fetchall()

                if not current_rows:
                    logger.warning(
                        "rollback_last_delivery: не удалось перечитать deliveries для отката, rule_id=%s, delivery_ids=%s",
                        rule_id,
                        delivery_ids,
                    )
                    return None

                old_value = []
                for row in current_rows:
                    old_value.append({
                        "delivery_id": int(row["id"]),
                        "post_id": int(row["post_id"]),
                        "status": row["status"],
                        "sent_at": row["sent_at"],
                        "error_text": row["error_text"],
                        "attempt_count": row["attempt_count"],
                    })

                conn.executemany("""
                    UPDATE deliveries
                    SET status = 'pending',
                        sent_at = NULL,
                        error_text = NULL,
                        attempt_count = 0
                    WHERE id = ?
                """, [(delivery_id,) for delivery_id in delivery_ids])

                conn.execute("""
                    UPDATE routing
                    SET next_run_at = ?
                    WHERE id = ?
                """, (now_iso, rule_id))

                conn.execute("""
                    INSERT INTO audit_log(
                        created_at,
                        event_type,
                        rule_id,
                        delivery_id,
                        post_id,
                        admin_id,
                        status,
                        old_value_json,
                        new_value_json,
                        extra_json
                    )
                    VALUES(?,?,?,?,?,?,?,?,?,?)
                """, (
                    now_iso,
                    "delivery_rolled_back",
                    rule_id,
                    delivery_ids[0] if delivery_ids else None,
                    selected["first_post_id"],
                    admin_id,
                    "pending",
                    json.dumps(old_value, ensure_ascii=False),
                    json.dumps({
                        "status": "pending",
                        "sent_at": None,
                        "error_text": None,
                        "attempt_count": 0,
                        "next_run_at": now_iso,
                    }, ensure_ascii=False),
                    json.dumps({
                        "action": "rollback_last_delivery",
                        "mode": selected.get("mode"),
                        "kind": selected.get("kind"),
                        "position": selected.get("position"),
                        "count": selected.get("count"),
                        "first_post_id": selected.get("first_post_id"),
                        "first_message_id": selected.get("first_message_id"),
                        "media_group_id": selected.get("media_group_id"),
                        "rolled_back_delivery_ids": delivery_ids,
                        "rolled_back_post_ids": post_ids,
                        "rolled_back_message_ids": message_ids,
                        "rolled_back_count": len(delivery_ids),
                    }, ensure_ascii=False),
                ))

                conn.commit()

            logger.info(
                "rollback_last_delivery: rule_id=%s, mode=%s, kind=%s, position=%s, rolled_back_count=%s",
                rule_id,
                selected.get("mode"),
                selected.get("kind"),
                selected.get("position"),
                len(delivery_ids),
            )

            return {
                "rule_id": rule_id,
                "mode": selected.get("mode"),
                "kind": selected.get("kind"),
                "position": selected.get("position"),
                "count": selected.get("count"),
                "first_post_id": selected.get("first_post_id"),
                "first_message_id": selected.get("first_message_id"),
                "media_group_id": selected.get("media_group_id"),
                "next_run_at": now_iso,
                "rolled_back_delivery_ids": delivery_ids,
                "rolled_back_post_ids": post_ids,
                "rolled_back_message_ids": message_ids,
                "rolled_back_count": len(delivery_ids),
            }

        except Exception as exc:
            logger.exception(
                "rollback_last_delivery: авария отката, rule_id=%s, error=%s",
                rule_id,
                exc,
            )
            return None

    def _build_rule_logical_items_from_rows(
        self,
        rule_id: int,
        rows,
        mode: str | None = None,
    ) -> list[dict[str, Any]]:
        """
        Единый builder логических элементов для правила.

        ВАЖНО:
        - repost:
            * альбом = 1 логический элемент
            * одиночный пост = 1 логический элемент
        - video:
            * каждое видео = 1 логический элемент
            * альбомы не схлопываются
            * не-видео элементы исключаются

        Ожидает rows с полями:
            delivery_id
            status
            post_id
            message_id
            source_channel
            source_thread_id
            media_group_id
            content_json
            created_at (желательно, но не обязательно)
            sent_at    (желательно, но не обязательно)

        Возвращает список logical items в стабильном порядке.
        """
        items: list[dict[str, Any]] = []

        normalized_mode = (mode or "repost").strip().lower()
        if normalized_mode not in ("repost", "video"):
            logger.warning(
                "_build_rule_logical_items_from_rows: неизвестный mode='%s' у rule_id=%s, fallback -> repost",
                normalized_mode,
                rule_id,
            )
            normalized_mode = "repost"

        try:
            raw_rows = list(rows or [])
            if not raw_rows:
                logger.info(
                    "_build_rule_logical_items_from_rows: пустой набор rows, rule_id=%s, mode=%s",
                    rule_id,
                    normalized_mode,
                )
                return []

            # =========================================================
            # VIDEO-РЕЖИМ
            # =========================================================
            if normalized_mode == "video":
                skipped_non_video = 0
                broken_content_json = 0

                for row in raw_rows:
                    try:
                        content = json.loads(row["content_json"]) if row["content_json"] else {}
                    except Exception:
                        content = {}
                        broken_content_json += 1

                    media_kind = str(content.get("media_kind") or "text").strip().lower()
                    if media_kind != "video":
                        skipped_non_video += 1
                        continue

                    status = str(row["status"] or "").strip().lower()

                    items.append({
                        "kind": "video_single",
                        "position": len(items) + 1,
                        "rule_id": rule_id,
                        "mode": normalized_mode,
                        "source_channel": row["source_channel"],
                        "source_thread_id": row["source_thread_id"],
                        "media_group_id": row["media_group_id"],
                        "delivery_ids": [int(row["delivery_id"])],
                        "post_ids": [int(row["post_id"])],
                        "message_ids": [int(row["message_id"])],
                        "first_post_id": int(row["post_id"]),
                        "first_message_id": int(row["message_id"]),
                        "content_json": row["content_json"],
                        "created_at": row["created_at"] if "created_at" in row.keys() else None,
                        "sent_at": row["sent_at"] if "sent_at" in row.keys() else None,
                        "count": 1,
                        "statuses": [status],
                        "pending_count": 1 if status == "pending" else 0,
                        "processing_count": 1 if status == "processing" else 0,
                        "sent_count": 1 if status == "sent" else 0,
                        "faulty_count": 1 if status == "faulty" else 0,
                        "is_done": status == "sent",
                    })

                logger.info(
                    "_build_rule_logical_items_from_rows: built video items, rule_id=%s, raw_rows=%s, items=%s, skipped_non_video=%s, broken_content_json=%s",
                    rule_id,
                    len(raw_rows),
                    len(items),
                    skipped_non_video,
                    broken_content_json,
                )
                return items

            # =========================================================
            # REPOST-РЕЖИМ
            # =========================================================
            groups: dict[str, dict[str, Any]] = {}
            order_keys: list[str] = []

            for row in raw_rows:
                media_group_id = row["media_group_id"]

                if media_group_id:
                    group_key = f"album::{row['source_channel']}::{row['source_thread_id']}::{media_group_id}"
                    kind = "album"
                else:
                    group_key = f"single::{int(row['post_id'])}"
                    kind = "single"

                if group_key not in groups:
                    groups[group_key] = {
                        "kind": kind,
                        "rule_id": rule_id,
                        "mode": normalized_mode,
                        "source_channel": row["source_channel"],
                        "source_thread_id": row["source_thread_id"],
                        "media_group_id": media_group_id,
                        "delivery_ids": [],
                        "post_ids": [],
                        "message_ids": [],
                        "first_post_id": int(row["post_id"]),
                        "first_message_id": int(row["message_id"]),
                        "content_json": row["content_json"],
                        "created_at": row["created_at"] if "created_at" in row.keys() else None,
                        "sent_at": row["sent_at"] if "sent_at" in row.keys() else None,
                        "statuses": [],
                        "pending_count": 0,
                        "processing_count": 0,
                        "sent_count": 0,
                        "faulty_count": 0,
                    }
                    order_keys.append(group_key)

                item = groups[group_key]

                delivery_id = int(row["delivery_id"])
                post_id = int(row["post_id"])
                message_id = int(row["message_id"])
                status = str(row["status"] or "").strip().lower()

                item["delivery_ids"].append(delivery_id)
                item["post_ids"].append(post_id)
                item["message_ids"].append(message_id)
                item["statuses"].append(status)

                if status == "pending":
                    item["pending_count"] += 1
                elif status == "processing":
                    item["processing_count"] += 1
                elif status == "sent":
                    item["sent_count"] += 1
                elif status == "faulty":
                    item["faulty_count"] += 1

            for group_key in order_keys:
                item = groups[group_key]
                item["count"] = len(item["delivery_ids"])
                item["is_done"] = item["count"] > 0 and item["sent_count"] == item["count"]
                item["position"] = len(items) + 1
                items.append(item)

            logger.info(
                "_build_rule_logical_items_from_rows: built repost items, rule_id=%s, raw_rows=%s, items=%s",
                rule_id,
                len(raw_rows),
                len(items),
            )
            return items

        except Exception as exc:
            logger.exception(
                "_build_rule_logical_items_from_rows: авария сборки logical items, rule_id=%s, mode=%s, error=%s",
                rule_id,
                normalized_mode,
                exc,
            )
            return []

    def is_album_already_sent(
        self,
        rule_id: int,
        source_channel: str,
        source_thread_id: int | None,
        media_group_id: str,
    ) -> bool:
        with self.connect() as conn:
            row = conn.execute("""
                SELECT 1
                FROM deliveries d
                JOIN posts p ON p.id = d.post_id
                WHERE d.rule_id = ?
                AND d.status = 'sent'
                AND p.source_channel = ?
                AND (
                        (p.source_thread_id IS NULL AND ? IS NULL)
                        OR p.source_thread_id = ?
                    )
                AND p.media_group_id = ?
                LIMIT 1
            """, (
                rule_id,
                str(source_channel),
                source_thread_id,
                source_thread_id,
                media_group_id,
            )).fetchone()

            return row is not None

    def get_post_id_by_delivery(self, delivery_id: int) -> int | None:
        with self.connect() as conn:
            row = conn.execute("""
                SELECT post_id
                FROM deliveries
                WHERE id = ?
                LIMIT 1
            """, (int(delivery_id),)).fetchone()

            if not row:
                return None

            return int(row["post_id"])

    def log_event(
        self,
        event_type: str,
        rule_id: int | None = None,
        delivery_id: int | None = None,
        post_id: int | None = None,
        admin_id: int | None = None,
        source_id: str | None = None,
        source_thread_id: int | None = None,
        target_id: str | None = None,
        target_thread_id: int | None = None,
        status: str | None = None,
        error_text: str | None = None,
        old_value: dict[str, Any] | None = None,
        new_value: dict[str, Any] | None = None,
        extra: dict[str, Any] | None = None,
    ) -> None:
        with self.connect() as conn:
            conn.execute("""
                INSERT INTO audit_log(
                    created_at,
                    event_type,
                    rule_id,
                    delivery_id,
                    post_id,
                    admin_id,
                    source_id,
                    source_thread_id,
                    target_id,
                    target_thread_id,
                    status,
                    error_text,
                    old_value_json,
                    new_value_json,
                    extra_json
                )
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                utc_now_iso(),
                event_type,
                rule_id,
                delivery_id,
                post_id,
                admin_id,
                str(source_id) if source_id is not None else None,
                source_thread_id,
                str(target_id) if target_id is not None else None,
                target_thread_id,
                status,
                (error_text[:1000] if error_text else None),
                json.dumps(old_value, ensure_ascii=False) if old_value is not None else None,
                json.dumps(new_value, ensure_ascii=False) if new_value is not None else None,
                json.dumps(extra, ensure_ascii=False) if extra is not None else None,
            ))
            conn.commit()

    def get_audit_for_rule(self, rule_id: int, limit: int = 50):
        with self.connect() as conn:
            return conn.execute("""
                SELECT *
                FROM audit_log
                WHERE rule_id = ?
                ORDER BY id DESC
                LIMIT ?
            """, (rule_id, limit)).fetchall()

    def get_audit_for_delivery(self, delivery_id: int, limit: int = 50):
        with self.connect() as conn:
            return conn.execute("""
                SELECT *
                FROM audit_log
                WHERE delivery_id = ?
                ORDER BY id DESC
                LIMIT ?
            """, (delivery_id, limit)).fetchall()

    def get_recent_audit(self, limit: int = 100):
        with self.connect() as conn:
            return conn.execute("""
                SELECT *
                FROM audit_log
                ORDER BY id DESC
                LIMIT ?
            """, (limit,)).fetchall()

    def get_recent_video_audit(self, limit: int = 200):
        with self.connect() as conn:
            return conn.execute("""
                SELECT *
                FROM audit_log
                WHERE event_type LIKE 'video_%'
                OR json_extract(extra_json, '$.mode') = 'video'
                ORDER BY id DESC
                LIMIT ?
            """, (limit,)).fetchall()

    def get_video_audit_for_delivery(self, delivery_id: int, limit: int = 100):
        with self.connect() as conn:
            return conn.execute("""
                SELECT *
                FROM audit_log
                WHERE delivery_id = ?
                AND (
                        event_type LIKE 'video_%'
                        OR json_extract(extra_json, '$.mode') = 'video'
                    )
                ORDER BY id DESC
                LIMIT ?
            """, (delivery_id, limit)).fetchall()

    def log_rule_change(
        self,
        event_type: str,
        rule_id: int,
        admin_id: int,
        old_value: dict[str, Any] | None = None,
        new_value: dict[str, Any] | None = None,
        extra: dict[str, Any] | None = None,
    ) -> None:
        rule = self.get_rule(rule_id)

        self.log_event(
            event_type=event_type,
            rule_id=rule_id,
            admin_id=admin_id,
            source_id=rule.source_id if rule else None,
            source_thread_id=rule.source_thread_id if rule else None,
            target_id=rule.target_id if rule else None,
            target_thread_id=rule.target_thread_id if rule else None,
            old_value=old_value,
            new_value=new_value,
            extra=extra,
        )

    def log_delivery_event(
        self,
        event_type: str,
        delivery_id: int,
        rule_id: int | None = None,
        post_id: int | None = None,
        status: str | None = None,
        error_text: str | None = None,
        extra: dict[str, Any] | None = None,
    ) -> None:
        source_id = None
        source_thread_id = None
        target_id = None
        target_thread_id = None

        if rule_id is not None:
            rule = self.get_rule(rule_id)
            if rule:
                source_id = rule.source_id
                source_thread_id = rule.source_thread_id
                target_id = rule.target_id
                target_thread_id = rule.target_thread_id

        self.log_event(
            event_type=event_type,
            rule_id=rule_id,
            delivery_id=delivery_id,
            post_id=post_id,
            source_id=source_id,
            source_thread_id=source_thread_id,
            target_id=target_id,
            target_thread_id=target_thread_id,
            status=status,
            error_text=error_text,
            extra=extra,
        )

    def log_video_event(
        self,
        event_type: str,
        rule_id: int,
        delivery_id: int | None = None,
        post_id: int | None = None,
        status: str | None = None,
        error_text: str | None = None,
        extra: dict[str, Any] | None = None,
    ) -> None:
        if extra is None:
            extra = {}

        # всегда помечаем что это видео
        extra["mode"] = "video"

        self.log_delivery_event(
            event_type=event_type,
            delivery_id=delivery_id,
            rule_id=rule_id,
            post_id=post_id,
            status=status,
            error_text=error_text,
            extra=extra,
        )

    def register_problem(
        self,
        problem_key: str,
        problem_type: str,
        rule_id: int | None = None,
        delivery_id: int | None = None,
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        now_iso = utc_now_iso()

        with self.connect() as conn:
            row = conn.execute("""
                SELECT *
                FROM problem_state
                WHERE problem_key = ?
                LIMIT 1
            """, (problem_key,)).fetchone()

            if row:
                conn.execute("""
                    UPDATE problem_state
                    SET last_seen_at = ?,
                        hit_count = hit_count + 1,
                        is_active = 1
                    WHERE problem_key = ?
                """, (now_iso, problem_key))

                conn.commit()

                updated = conn.execute("""
                    SELECT *
                    FROM problem_state
                    WHERE problem_key = ?
                    LIMIT 1
                """, (problem_key,)).fetchone()

                return dict(updated)

            conn.execute("""
                INSERT INTO problem_state(
                    problem_key,
                    problem_type,
                    rule_id,
                    delivery_id,
                    first_seen_at,
                    last_seen_at,
                    last_notified_at,
                    hit_count,
                    is_active,
                    is_muted,
                    extra_json
                )
                VALUES(?,?,?,?,?,?,?,?,?,?,?)
            """, (
                problem_key,
                problem_type,
                rule_id,
                delivery_id,
                now_iso,
                now_iso,
                None,
                1,
                1,
                0,
                json.dumps(extra, ensure_ascii=False) if extra is not None else None,
            ))

            conn.commit()

            created = conn.execute("""
                SELECT *
                FROM problem_state
                WHERE problem_key = ?
                LIMIT 1
            """, (problem_key,)).fetchone()

            return dict(created)

    def mark_problem_notified(self, problem_key: str) -> bool:
        with self.connect() as conn:
            cur = conn.execute("""
                UPDATE problem_state
                SET last_notified_at = ?
                WHERE problem_key = ?
            """, (utc_now_iso(), problem_key))
            conn.commit()
            return cur.rowcount > 0

    def resolve_problem(self, problem_key: str) -> bool:
        with self.connect() as conn:
            cur = conn.execute("""
                UPDATE problem_state
                SET is_active = 0
                WHERE problem_key = ?
            """, (problem_key,))
            conn.commit()
            return cur.rowcount > 0

    def mute_problem(self, problem_key: str, muted: bool = True) -> bool:
        with self.connect() as conn:
            cur = conn.execute("""
                UPDATE problem_state
                SET is_muted = ?
                WHERE problem_key = ?
            """, (1 if muted else 0, problem_key))
            conn.commit()
            return cur.rowcount > 0

    def get_problem_state(self, problem_key: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute("""
                SELECT *
                FROM problem_state
                WHERE problem_key = ?
                LIMIT 1
            """, (problem_key,)).fetchone()

            return dict(row) if row else None

    @contextmanager
    def connect(self):
        conn = sqlite3.connect(
            self.path,
            timeout=60,
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row

        # Базовая защита и стабильность
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA busy_timeout = 60000")
        conn.execute("PRAGMA synchronous = NORMAL")
        conn.execute("PRAGMA temp_store = MEMORY")
        conn.execute("PRAGMA cache_size = -32000")   # ~32MB page cache
        conn.execute("PRAGMA wal_autocheckpoint = 1000")

        try:
            yield conn
        finally:
            conn.close()

    def integrity_check(self) -> tuple[bool, str]:
        with self.connect() as conn:
            row = conn.execute("PRAGMA integrity_check").fetchone()
            if not row:
                return False, "integrity_check не вернул результат"

            result = str(row[0]).strip().lower()
            if result == "ok":
                return True, "ok"

            return False, result

    def backup_database(self, backup_path: str) -> bool:
        try:
            with self.connect() as conn:
                dst = sqlite3.connect(backup_path)
                try:
                    conn.backup(dst)
                    dst.commit()
                finally:
                    dst.close()
            return True
        except Exception:
            return False

    def optimize_database(self) -> None:
        with self.connect() as conn:
            conn.execute("PRAGMA optimize")
            conn.commit()

    def _find_next_interval_slot(
        self,
        conn,
        base_dt: datetime,
        exclude_rule_id: int | None = None,
    ) -> str:
        candidate = base_dt

        while True:
            rows = conn.execute("""
                SELECT id, next_run_at
                FROM routing
                WHERE is_active = 1
                AND schedule_mode = 'interval'
                AND next_run_at IS NOT NULL
                ORDER BY next_run_at ASC, id ASC
            """).fetchall()

            conflict_found = False

            for row in rows:
                other_rule_id = int(row["id"])
                if exclude_rule_id is not None and other_rule_id == exclude_rule_id:
                    continue

                try:
                    other_dt = datetime.fromisoformat(row["next_run_at"])
                except Exception:
                    continue

                delta = abs((other_dt - candidate).total_seconds())

                if delta < GLOBAL_INTERVAL_GAP_SECONDS:
                    candidate = other_dt + timedelta(seconds=GLOBAL_INTERVAL_GAP_SECONDS)
                    conflict_found = True
                    break

            if not conflict_found:
                return candidate.isoformat()

    def init(self):
        with self.connect() as conn:
            c = conn.cursor()
            c.execute("""
            CREATE TABLE IF NOT EXISTS channels(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_id TEXT NOT NULL,
                thread_id INTEGER DEFAULT NULL,
                channel_type TEXT NOT NULL CHECK(channel_type IN ('source','target')),
                title TEXT,
                added_by INTEGER,
                added_date TEXT NOT NULL,
                is_active BOOLEAN NOT NULL DEFAULT 1,
                UNIQUE(channel_id, thread_id, channel_type)
            )""")
            c.execute("""
            CREATE TABLE IF NOT EXISTS posts(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_id INTEGER NOT NULL,
                source_channel TEXT NOT NULL,
                source_thread_id INTEGER DEFAULT NULL,
                content_json TEXT NOT NULL,
                media_group_id TEXT DEFAULT NULL,
                created_at TEXT NOT NULL,
                is_faulty BOOLEAN NOT NULL DEFAULT 0,
                UNIQUE(message_id, source_channel, source_thread_id)
            )""")
            c.execute("""
            CREATE TABLE IF NOT EXISTS routing(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_id TEXT NOT NULL,
                source_thread_id INTEGER DEFAULT NULL,
                target_id TEXT NOT NULL,
                target_thread_id INTEGER DEFAULT NULL,
                interval INTEGER NOT NULL DEFAULT 3600,

                schedule_mode TEXT NOT NULL DEFAULT 'interval',
                fixed_times_json TEXT DEFAULT NULL,

                mode TEXT NOT NULL DEFAULT 'repost',

                video_trim_seconds INTEGER DEFAULT 120,
                video_add_intro BOOLEAN DEFAULT 0,
                video_intro_horizontal TEXT DEFAULT NULL,
                video_intro_vertical TEXT DEFAULT NULL,
                video_caption TEXT DEFAULT NULL,
                video_caption_entities_json TEXT DEFAULT NULL,
                caption_delivery_mode TEXT NOT NULL DEFAULT 'auto',
                video_caption_delivery_mode TEXT NOT NULL DEFAULT 'auto',

                created_by INTEGER,
                created_date TEXT NOT NULL,
                is_active BOOLEAN NOT NULL DEFAULT 1,
                next_run_at TEXT DEFAULT NULL,
                last_sent_at TEXT DEFAULT NULL,

                UNIQUE(source_id, source_thread_id, target_id, target_thread_id)
            )
            """)

            c.execute("""
            CREATE TABLE IF NOT EXISTS problem_state(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                problem_key TEXT NOT NULL UNIQUE,
                problem_type TEXT NOT NULL,
                rule_id INTEGER DEFAULT NULL,
                delivery_id INTEGER DEFAULT NULL,
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                last_notified_at TEXT DEFAULT NULL,
                hit_count INTEGER NOT NULL DEFAULT 1,
                is_active BOOLEAN NOT NULL DEFAULT 1,
                is_muted BOOLEAN NOT NULL DEFAULT 0,
                extra_json TEXT DEFAULT NULL
            )""")
            c.execute("""
                CREATE INDEX IF NOT EXISTS idx_problem_state_active
                ON problem_state(is_active, is_muted, last_seen_at)
            """)

            c.execute("""
                CREATE INDEX IF NOT EXISTS idx_problem_state_rule
                ON problem_state(rule_id, is_active, is_muted)
            """)

            c.execute("""
            CREATE TABLE IF NOT EXISTS intros(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                display_name TEXT NOT NULL UNIQUE,
                file_name TEXT NOT NULL,
                file_path TEXT NOT NULL,
                duration INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL
            )""")

            c.execute("""
                CREATE INDEX IF NOT EXISTS idx_intros_created_at
                ON intros(created_at)
            """)

            # миграция (если обновляешь существующую БД)
            try:
                c.execute("ALTER TABLE routing ADD COLUMN mode TEXT NOT NULL DEFAULT 'repost'")
            except Exception:
                pass

            try:
                c.execute("ALTER TABLE routing ADD COLUMN video_trim_seconds INTEGER DEFAULT 120")
            except Exception:
                pass

            try:
                c.execute("ALTER TABLE routing ADD COLUMN video_add_intro BOOLEAN DEFAULT 0")
            except Exception:
                pass

            try:
                c.execute("ALTER TABLE routing ADD COLUMN video_intro_horizontal TEXT DEFAULT NULL")
            except Exception:
                pass

            try:
                c.execute("ALTER TABLE routing ADD COLUMN video_intro_vertical TEXT DEFAULT NULL")
            except Exception:
                pass

            try:
                c.execute("ALTER TABLE routing ADD COLUMN video_caption TEXT DEFAULT NULL")
            except Exception:
                pass

            try:
                c.execute("ALTER TABLE routing ADD COLUMN video_caption_entities_json TEXT DEFAULT NULL")
            except Exception:
                pass
            try:
                c.execute("ALTER TABLE routing ADD COLUMN caption_delivery_mode TEXT NOT NULL DEFAULT 'auto'")
            except Exception:
                pass
            try:
                c.execute("ALTER TABLE routing ADD COLUMN video_caption_delivery_mode TEXT NOT NULL DEFAULT 'auto'")
            except Exception:
                pass
            try:
                c.execute("ALTER TABLE routing ADD COLUMN schedule_mode TEXT NOT NULL DEFAULT 'interval'")
            except Exception:
                pass

            try:
                c.execute("ALTER TABLE routing ADD COLUMN fixed_times_json TEXT DEFAULT NULL")
            except Exception:
                pass
            c.execute("""
            CREATE TABLE IF NOT EXISTS deliveries(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                rule_id INTEGER NOT NULL,
                post_id INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','processing','sent','faulty')),
                error_text TEXT DEFAULT NULL,
                attempt_count INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                sent_at TEXT DEFAULT NULL,
                UNIQUE(rule_id, post_id),
                FOREIGN KEY(rule_id) REFERENCES routing(id) ON DELETE CASCADE,
                FOREIGN KEY(post_id) REFERENCES posts(id) ON DELETE CASCADE
            )""")
            c.execute("""
            CREATE TABLE IF NOT EXISTS audit_log(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                event_type TEXT NOT NULL,

                rule_id INTEGER DEFAULT NULL,
                delivery_id INTEGER DEFAULT NULL,
                post_id INTEGER DEFAULT NULL,

                admin_id INTEGER DEFAULT NULL,

                source_id TEXT DEFAULT NULL,
                source_thread_id INTEGER DEFAULT NULL,
                target_id TEXT DEFAULT NULL,
                target_thread_id INTEGER DEFAULT NULL,

                status TEXT DEFAULT NULL,
                error_text TEXT DEFAULT NULL,

                old_value_json TEXT DEFAULT NULL,
                new_value_json TEXT DEFAULT NULL,
                extra_json TEXT DEFAULT NULL
            )""")
            c.execute("""
                CREATE INDEX IF NOT EXISTS idx_audit_log_created_at
                ON audit_log(created_at)
            """)

            c.execute("""
                CREATE INDEX IF NOT EXISTS idx_audit_log_rule_id
                ON audit_log(rule_id, created_at)
            """)

            c.execute("""
                CREATE INDEX IF NOT EXISTS idx_audit_log_delivery_id
                ON audit_log(delivery_id, created_at)
            """)

            c.execute("""
                CREATE INDEX IF NOT EXISTS idx_audit_log_event_type
                ON audit_log(event_type, created_at)
            """)

            try:
                audit_sql_row = conn.execute("""
                    SELECT sql
                    FROM sqlite_master
                    WHERE type = 'table' AND name = 'audit_log'
                """).fetchone()

                audit_sql = (audit_sql_row["sql"] or "") if audit_sql_row else ""

                if "FOREIGN KEY(rule_id)" in audit_sql or "FOREIGN KEY(delivery_id)" in audit_sql or "FOREIGN KEY(post_id)" in audit_sql:
                    c.execute("ALTER TABLE audit_log RENAME TO audit_log_old")

                    c.execute("""
                    CREATE TABLE audit_log(
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        created_at TEXT NOT NULL,
                        event_type TEXT NOT NULL,

                        rule_id INTEGER DEFAULT NULL,
                        delivery_id INTEGER DEFAULT NULL,
                        post_id INTEGER DEFAULT NULL,

                        admin_id INTEGER DEFAULT NULL,

                        source_id TEXT DEFAULT NULL,
                        source_thread_id INTEGER DEFAULT NULL,
                        target_id TEXT DEFAULT NULL,
                        target_thread_id INTEGER DEFAULT NULL,

                        status TEXT DEFAULT NULL,
                        error_text TEXT DEFAULT NULL,

                        old_value_json TEXT DEFAULT NULL,
                        new_value_json TEXT DEFAULT NULL,
                        extra_json TEXT DEFAULT NULL
                    )""")

                    c.execute("""
                        INSERT INTO audit_log(
                            id,
                            created_at,
                            event_type,
                            rule_id,
                            delivery_id,
                            post_id,
                            admin_id,
                            source_id,
                            source_thread_id,
                            target_id,
                            target_thread_id,
                            status,
                            error_text,
                            old_value_json,
                            new_value_json,
                            extra_json
                        )
                        SELECT
                            id,
                            created_at,
                            event_type,
                            rule_id,
                            delivery_id,
                            post_id,
                            admin_id,
                            source_id,
                            source_thread_id,
                            target_id,
                            target_thread_id,
                            status,
                            error_text,
                            old_value_json,
                            new_value_json,
                            extra_json
                        FROM audit_log_old
                    """)

                    c.execute("DROP TABLE audit_log_old")

                    c.execute("""
                        CREATE INDEX IF NOT EXISTS idx_audit_log_created_at
                        ON audit_log(created_at)
                    """)

                    c.execute("""
                        CREATE INDEX IF NOT EXISTS idx_audit_log_rule_id
                        ON audit_log(rule_id, created_at)
                    """)

                    c.execute("""
                        CREATE INDEX IF NOT EXISTS idx_audit_log_delivery_id
                        ON audit_log(delivery_id, created_at)
                    """)

                    c.execute("""
                        CREATE INDEX IF NOT EXISTS idx_audit_log_event_type
                        ON audit_log(event_type, created_at)
                    """)
            except Exception:
                pass

            c.execute("CREATE INDEX IF NOT EXISTS idx_deliveries_rule_status ON deliveries(rule_id, status, post_id)")

            c.execute("""
                CREATE INDEX IF NOT EXISTS idx_posts_source_lookup
                ON posts(source_channel, source_thread_id, id)
            """)

            c.execute("""
                CREATE INDEX IF NOT EXISTS idx_posts_album_lookup
                ON posts(source_channel, media_group_id, message_id)
            """)

            c.execute("""
                CREATE INDEX IF NOT EXISTS idx_routing_source_lookup
                ON routing(source_id, source_thread_id, is_active)
            """)

            c.execute("""
                CREATE INDEX IF NOT EXISTS idx_routing_next_run
                ON routing(is_active, next_run_at)
            """)

            c.execute("""
                CREATE INDEX IF NOT EXISTS idx_deliveries_post_id
                ON deliveries(post_id)
            """)

            try:
                table_sql = conn.execute("""
                    SELECT sql
                    FROM sqlite_master
                    WHERE type = 'table' AND name = 'deliveries'
                """).fetchone()

                if table_sql and "processing" not in (table_sql["sql"] or ""):
                    c.execute("ALTER TABLE deliveries RENAME TO deliveries_old")

                    c.execute("""
                    CREATE TABLE deliveries(
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        rule_id INTEGER NOT NULL,
                        post_id INTEGER NOT NULL,
                        status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','processing','sent','faulty')),
                        error_text TEXT DEFAULT NULL,
                        attempt_count INTEGER NOT NULL DEFAULT 0,
                        created_at TEXT NOT NULL,
                        sent_at TEXT DEFAULT NULL,
                        UNIQUE(rule_id, post_id),
                        FOREIGN KEY(rule_id) REFERENCES routing(id) ON DELETE CASCADE,
                        FOREIGN KEY(post_id) REFERENCES posts(id) ON DELETE CASCADE
                    )""")

                    c.execute("""
                        INSERT INTO deliveries(id, rule_id, post_id, status, error_text, attempt_count, created_at, sent_at)
                        SELECT id, rule_id, post_id, status, error_text, attempt_count, created_at, sent_at
                        FROM deliveries_old
                    """)

                    c.execute("DROP TABLE deliveries_old")
                    c.execute("CREATE INDEX IF NOT EXISTS idx_deliveries_rule_status ON deliveries(rule_id, status, post_id)")
            except Exception:
                pass
            # --- MIGRATION: intro_id columns ---
            try:
                c.execute("ALTER TABLE routing ADD COLUMN video_intro_horizontal_id INTEGER")
            except sqlite3.OperationalError:
                pass

            try:
                c.execute("ALTER TABLE routing ADD COLUMN video_intro_vertical_id INTEGER")
            except sqlite3.OperationalError:
                pass
            conn.commit()
    def add_channel(self, channel_id: str, thread_id: int | None, channel_type: str, title: str, added_by: int) -> bool:
        with self.connect() as conn:
            cur = conn.execute("INSERT OR IGNORE INTO channels(channel_id,thread_id,channel_type,title,added_by,added_date,is_active) VALUES(?,?,?,?,?,?,1)", (str(channel_id), thread_id, channel_type, title, added_by, utc_now_iso()))
            conn.commit()
            return cur.rowcount > 0
    def remove_channel(self, channel_id: str, thread_id: int | None, channel_type: str | None = None) -> bool:
        with self.connect() as conn:
            params = [str(channel_id)]
            q = "DELETE FROM channels WHERE channel_id = ?"
            if thread_id is None:
                q += " AND thread_id IS NULL"
            else:
                q += " AND thread_id = ?"
                params.append(thread_id)
            if channel_type:
                q += " AND channel_type = ?"
                params.append(channel_type)
            cur = conn.execute(q, params)
            if channel_type in (None, "source"):
                if thread_id is None:
                    conn.execute("DELETE FROM posts WHERE source_channel = ? AND source_thread_id IS NULL", (str(channel_id),))
                else:
                    conn.execute("DELETE FROM posts WHERE source_channel = ? AND source_thread_id = ?", (str(channel_id), thread_id))
            conn.execute("""
                DELETE FROM routing
                WHERE (source_id = ? AND ((source_thread_id IS NULL AND ? IS NULL) OR source_thread_id = ?))
                   OR (target_id = ? AND ((target_thread_id IS NULL AND ? IS NULL) OR target_thread_id = ?))
            """, (str(channel_id), thread_id, thread_id, str(channel_id), thread_id, thread_id))
            conn.commit()
            return cur.rowcount > 0

    def update_rule_fixed_times(self, rule_id: int, times: list[str]) -> bool:
        normalized = normalize_fixed_times(times)

        if not normalized:
            return False

        next_run = get_next_fixed_run_utc(normalized)

        with self.connect() as conn:
            cur = conn.execute("""
                UPDATE routing
                SET schedule_mode = 'fixed',
                    fixed_times_json = ?,
                    next_run_at = ?
                WHERE id = ?
            """, (json.dumps(normalized), next_run, rule_id))

            conn.commit()
            return cur.rowcount > 0

    def set_rule_intro_horizontal(self, rule_id: int, intro_id: int | None) -> bool:
        with self.connect() as conn:
            cur = conn.execute("""
                UPDATE routing
                SET video_intro_horizontal_id = ?
                WHERE id = ?
            """, (intro_id, rule_id))
            conn.commit()
            return cur.rowcount > 0


    def set_rule_intro_vertical(self, rule_id: int, intro_id: int | None) -> bool:
        with self.connect() as conn:
            cur = conn.execute("""
                UPDATE routing
                SET video_intro_vertical_id = ?
                WHERE id = ?
            """, (intro_id, rule_id))
            conn.commit()
            return cur.rowcount > 0

    def set_rule_add_intro(self, rule_id: int, enabled: bool) -> bool:
        with self.connect() as conn:
            cur = conn.execute("""
                UPDATE routing
                SET video_add_intro = ?
                WHERE id = ?
            """, (1 if enabled else 0, rule_id))
            conn.commit()
            return cur.rowcount > 0

    def get_intro_by_id(self, intro_id: int) -> IntroItem | None:
        return self.get_intro(intro_id)

    def set_rule_interval_mode(self, rule_id: int, interval: int) -> bool:
        with self.connect() as conn:
            base_dt = datetime.fromtimestamp(
                datetime.now(timezone.utc).timestamp() + max(interval, 1),
                tz=timezone.utc,
            )
            next_run_iso = self._find_next_interval_slot(conn, base_dt, exclude_rule_id=rule_id)

            cur = conn.execute("""
                UPDATE routing
                SET schedule_mode = 'interval',
                    interval = ?,
                    fixed_times_json = NULL,
                    next_run_at = ?
                WHERE id = ?
            """, (interval, next_run_iso, rule_id))

            conn.commit()
            return cur.rowcount > 0

    def channel_exists(self, channel_id: str, thread_id: int | None, channel_type: str | None = None) -> bool:
        with self.connect() as conn:
            q = "SELECT 1 FROM channels WHERE channel_id = ?"
            params = [str(channel_id)]
            if thread_id is None: q += " AND thread_id IS NULL"
            else:
                q += " AND thread_id = ?"
                params.append(thread_id)
            if channel_type:
                q += " AND channel_type = ?"
                params.append(channel_type)
            return conn.execute(q, params).fetchone() is not None
    def get_channels(self, channel_type: str | None = None):
        with self.connect() as conn:
            if channel_type:
                return conn.execute("SELECT channel_id,thread_id,title,channel_type FROM channels WHERE channel_type = ? AND is_active = 1 ORDER BY added_date", (channel_type,)).fetchall()
            return conn.execute("SELECT channel_id,thread_id,title,channel_type FROM channels WHERE is_active = 1 ORDER BY channel_type, added_date").fetchall()
    def add_intro(
        self,
        display_name: str,
        file_name: str,
        file_path: str,
        duration: int,
    ) -> int | None:
        with self.connect() as conn:
            cur = conn.execute("""
                INSERT OR IGNORE INTO intros(
                    display_name,
                    file_name,
                    file_path,
                    duration,
                    created_at
                )
                VALUES(?,?,?,?,?)
            """, (
                display_name.strip(),
                file_name,
                file_path,
                int(duration or 0),
                utc_now_iso(),
            ))
            conn.commit()

            if cur.rowcount == 0:
                return None

            return int(cur.lastrowid)

    def get_intros(self) -> list[IntroItem]:
        with self.connect() as conn:
            rows = conn.execute("""
                SELECT id, display_name, file_name, file_path, duration, created_at
                FROM intros
                ORDER BY created_at DESC, id DESC
            """).fetchall()

            items: list[IntroItem] = []
            for row in rows:
                items.append(
                    IntroItem(
                        id=int(row["id"]),
                        display_name=row["display_name"],
                        file_name=row["file_name"],
                        file_path=row["file_path"],
                        duration=int(row["duration"] or 0),
                        created_at=row["created_at"],
                    )
                )

            return items

    def get_intro(self, intro_id: int) -> IntroItem | None:
        with self.connect() as conn:
            row = conn.execute("""
                SELECT id, display_name, file_name, file_path, duration, created_at
                FROM intros
                WHERE id = ?
                LIMIT 1
            """, (intro_id,)).fetchone()

            if not row:
                return None

            return IntroItem(
                id=int(row["id"]),
                display_name=row["display_name"],
                file_name=row["file_name"],
                file_path=row["file_path"],
                duration=int(row["duration"] or 0),
                created_at=row["created_at"],
            )

    def delete_intro(self, intro_id: int) -> bool:
        with self.connect() as conn:
            cur = conn.execute("""
                DELETE FROM intros
                WHERE id = ?
            """, (intro_id,))
            conn.commit()
            return cur.rowcount > 0

    def save_post(self, message_id: int, source_channel: str, source_thread_id: int | None, content: dict[str, Any], media_group_id: str | None = None) -> int:
        with self.connect() as conn:
            cur = conn.execute("INSERT OR IGNORE INTO posts(message_id,source_channel,source_thread_id,content_json,media_group_id,created_at,is_faulty) VALUES(?,?,?,?,?,?,0)", (message_id, str(source_channel), source_thread_id, json.dumps(content, ensure_ascii=False), media_group_id, utc_now_iso()))
            if cur.rowcount == 0:
                row = conn.execute("SELECT id FROM posts WHERE message_id = ? AND source_channel = ? AND ((source_thread_id IS NULL AND ? IS NULL) OR source_thread_id = ?)", (message_id, str(source_channel), source_thread_id, source_thread_id)).fetchone()
                post_id = int(row["id"])
            else:
                post_id = int(cur.lastrowid)
            self._create_deliveries_for_post_conn(conn, post_id, str(source_channel), source_thread_id)
            conn.commit()
            return post_id

    def get_next_scheduled_rule(self):
        with self.connect() as conn:
            return conn.execute("""
                SELECT
                    r.id,
                    r.next_run_at,
                    r.interval,
                    r.schedule_mode,
                    r.fixed_times_json,
                    r.is_active,
                    r.source_id,
                    r.source_thread_id,
                    r.target_id,
                    r.target_thread_id,
                    s.title AS source_title,
                    t.title AS target_title
                FROM routing r
                LEFT JOIN channels s
                    ON s.channel_id = r.source_id
                AND ((s.thread_id IS NULL AND r.source_thread_id IS NULL) OR s.thread_id = r.source_thread_id)
                AND s.channel_type = 'source'
                LEFT JOIN channels t
                    ON t.channel_id = r.target_id
                AND ((t.thread_id IS NULL AND r.target_thread_id IS NULL) OR t.thread_id = r.target_thread_id)
                AND t.channel_type = 'target'
                WHERE r.is_active = 1
                AND r.next_run_at IS NOT NULL
                AND EXISTS (
                    SELECT 1
                    FROM deliveries d
                    WHERE d.rule_id = r.id
                      AND d.status = 'pending'
                )
                ORDER BY r.next_run_at ASC
                LIMIT 1
            """).fetchone()

    def save_post_batch(self, posts_data):
        count = 0
        with self.connect() as conn:
            for message_id, source_channel, source_thread_id, content, media_group_id in posts_data:
                cur = conn.execute("INSERT OR IGNORE INTO posts(message_id,source_channel,source_thread_id,content_json,media_group_id,created_at,is_faulty) VALUES(?,?,?,?,?,?,0)", (message_id, str(source_channel), source_thread_id, json.dumps(content, ensure_ascii=False), media_group_id, utc_now_iso()))
                if cur.rowcount == 0:
                    row = conn.execute("SELECT id FROM posts WHERE message_id = ? AND source_channel = ? AND ((source_thread_id IS NULL AND ? IS NULL) OR source_thread_id = ?)", (message_id, str(source_channel), source_thread_id, source_thread_id)).fetchone()
                    post_id = int(row["id"])
                else:
                    post_id = int(cur.lastrowid); count += 1
                self._create_deliveries_for_post_conn(conn, post_id, str(source_channel), source_thread_id)
            conn.commit()
            return count
    def delete_channel_posts(self, channel_id: str, thread_id: int | None = None) -> int:
        with self.connect() as conn:
            if thread_id is None: cur = conn.execute("DELETE FROM posts WHERE source_channel = ? AND source_thread_id IS NULL", (str(channel_id),))
            else: cur = conn.execute("DELETE FROM posts WHERE source_channel = ? AND source_thread_id = ?", (str(channel_id), thread_id))
            conn.commit(); return cur.rowcount

    def add_rule(self, source_id: str, source_thread_id: int | None, target_id: str, target_thread_id: int | None, interval: int, created_by: int) -> int | None:
        with self.connect() as conn:
            base_dt = datetime.now(timezone.utc)
            next_run_iso = self._find_next_interval_slot(conn, base_dt)

            cur = conn.execute("""
                INSERT INTO routing(
                    source_id, source_thread_id, target_id, target_thread_id,
                    interval, created_by, created_date, is_active, next_run_at, last_sent_at
                )
                VALUES(?,?,?,?,?,?,?,?,?,NULL)
            """, (
                str(source_id),
                source_thread_id,
                str(target_id),
                target_thread_id,
                interval,
                created_by,
                utc_now_iso(),
                0,
                next_run_iso,
            ))

            if cur.rowcount == 0:
                return None

            rule_id = int(cur.lastrowid)
            conn.commit()
            return rule_id

    def remove_rule(self, rule_id: int) -> bool:
        with self.connect() as conn:
            cur = conn.execute(
                "DELETE FROM routing WHERE id = ?",
                (rule_id,),
            )
            conn.commit()
            return cur.rowcount > 0
    def delete_rule_with_audit(self, rule_id: int, admin_id: int) -> bool:
        with self.connect() as conn:
            row = conn.execute("""
                SELECT *
                FROM routing
                WHERE id = ?
                LIMIT 1
            """, (rule_id,)).fetchone()

            if not row:
                return False

            fixed_times = []
            try:
                fixed_times = json.loads(row["fixed_times_json"]) if row["fixed_times_json"] else []
            except Exception:
                fixed_times = []

            conn.execute("""
                INSERT INTO audit_log(
                    created_at,
                    event_type,
                    rule_id,
                    admin_id,
                    source_id,
                    source_thread_id,
                    target_id,
                    target_thread_id,
                    old_value_json
                )
                VALUES(?,?,?,?,?,?,?,?,?)
            """, (
                utc_now_iso(),
                "rule_deleted",
                rule_id,
                admin_id,
                row["source_id"],
                row["source_thread_id"],
                row["target_id"],
                row["target_thread_id"],
                json.dumps({
                    "is_active": bool(row["is_active"]),
                    "interval": row["interval"],
                    "schedule_mode": row["schedule_mode"],
                    "fixed_times": fixed_times,
                    "next_run_at": row["next_run_at"],
                }, ensure_ascii=False),
            ))

            cur = conn.execute(
                "DELETE FROM routing WHERE id = ?",
                (rule_id,),
            )

            conn.commit()
            return cur.rowcount > 0
    def get_all_rules(self):
        with self.connect() as conn:
            rows = conn.execute("""
                SELECT r.*, s.title AS source_title, t.title AS target_title
                FROM routing r
                LEFT JOIN channels s ON s.channel_id = r.source_id
                    AND ((s.thread_id IS NULL AND r.source_thread_id IS NULL) OR s.thread_id = r.source_thread_id)
                    AND s.channel_type = 'source'
                LEFT JOIN channels t ON t.channel_id = r.target_id
                    AND ((t.thread_id IS NULL AND r.target_thread_id IS NULL) OR t.thread_id = r.target_thread_id)
                    AND t.channel_type = 'target'
                ORDER BY r.created_date, r.id
            """).fetchall()

            rules = []

            for row in rows:
                data = dict(row)

                rule = Rule(
                    id=data["id"],
                    source_id=data["source_id"],
                    source_thread_id=data["source_thread_id"],
                    target_id=data["target_id"],
                    target_thread_id=data["target_thread_id"],

                    interval=data["interval"],
                    schedule_mode=data.get("schedule_mode", "interval"),
                    fixed_times_json=data.get("fixed_times_json"),

                    is_active=bool(data["is_active"]),
                    created_date=data["created_date"],
                    next_run_at=data["next_run_at"],
                    last_sent_at=data["last_sent_at"],
                    source_title=data.get("source_title"),
                    target_title=data.get("target_title"),

                    mode=data.get("mode", "repost"),

                    video_trim_seconds=data.get("video_trim_seconds", 120),
                    video_add_intro=bool(data.get("video_add_intro", 0)),

                    video_intro_horizontal=data.get("video_intro_horizontal"),
                    video_intro_vertical=data.get("video_intro_vertical"),
                    video_intro_horizontal_id=data.get("video_intro_horizontal_id"),
                    video_intro_vertical_id=data.get("video_intro_vertical_id"),

                    video_caption=data.get("video_caption"),
                    video_caption_entities_json=data.get("video_caption_entities_json"),
                    caption_delivery_mode=data.get("caption_delivery_mode", "auto"),
                    video_caption_delivery_mode=data.get("video_caption_delivery_mode", "auto"),
                )

                rules.append(rule)

            return rules

    def get_rule(self, rule_id: int):
        with self.connect() as conn:
            row = conn.execute("""
                SELECT r.*, s.title AS source_title, t.title AS target_title
                FROM routing r
                LEFT JOIN channels s ON s.channel_id = r.source_id
                    AND ((s.thread_id IS NULL AND r.source_thread_id IS NULL) OR s.thread_id = r.source_thread_id)
                    AND s.channel_type = 'source'
                LEFT JOIN channels t ON t.channel_id = r.target_id
                    AND ((t.thread_id IS NULL AND r.target_thread_id IS NULL) OR t.thread_id = r.target_thread_id)
                    AND t.channel_type = 'target'
                WHERE r.id = ?
            """, (rule_id,)).fetchone()

            if not row:
                return None

            data = dict(row)
            return Rule(
                id=data["id"],
                source_id=data["source_id"],
                source_thread_id=data["source_thread_id"],
                target_id=data["target_id"],
                target_thread_id=data["target_thread_id"],

                interval=data["interval"],
                schedule_mode=data.get("schedule_mode", "interval"),
                fixed_times_json=data.get("fixed_times_json"),

                is_active=bool(data["is_active"]),
                created_date=data["created_date"],
                next_run_at=data["next_run_at"],
                last_sent_at=data["last_sent_at"],
                source_title=data.get("source_title"),
                target_title=data.get("target_title"),

                mode=data.get("mode", "repost"),

                video_trim_seconds=data.get("video_trim_seconds", 120),
                video_add_intro=bool(data.get("video_add_intro", 0)),

                video_intro_horizontal=data.get("video_intro_horizontal"),
                video_intro_vertical=data.get("video_intro_vertical"),
                video_intro_horizontal_id=data.get("video_intro_horizontal_id"),
                video_intro_vertical_id=data.get("video_intro_vertical_id"),

                video_caption=data.get("video_caption"),
                video_caption_entities_json=data.get("video_caption_entities_json"),

               caption_delivery_mode=data.get("caption_delivery_mode", "auto"),
               video_caption_delivery_mode=data.get("video_caption_delivery_mode", "auto"),
            )

    def set_rule_active(self, rule_id: int, is_active: bool) -> bool:
        with self.connect() as conn:
            cur = conn.execute("UPDATE routing SET is_active = ? WHERE id = ?", (1 if is_active else 0, rule_id)); conn.commit(); return cur.rowcount > 0

    def activate_rule_with_backfill(self, rule_id: int) -> bool:
        with self.connect() as conn:
            row = conn.execute("""
                SELECT id, source_id, source_thread_id
                FROM routing
                WHERE id = ?
                LIMIT 1
            """, (rule_id,)).fetchone()

            if not row:
                return False

            cur = conn.execute("""
                UPDATE routing
                SET is_active = 1
                WHERE id = ?
            """, (rule_id,))

            if cur.rowcount == 0:
                conn.commit()
                return False

            self._backfill_deliveries_for_rule_conn(
                conn,
                int(row["id"]),
                str(row["source_id"]),
                row["source_thread_id"],
            )

            conn.commit()
            return True

    def get_rule_source_scope(self, rule_id: int) -> tuple[str, int | None] | None:
        with self.connect() as conn:
            row = conn.execute("""
                SELECT source_id, source_thread_id
                FROM routing
                WHERE id = ?
                LIMIT 1
            """, (rule_id,)).fetchone()

            if not row:
                return None

            return str(row["source_id"]), row["source_thread_id"]


    def clear_rule_deliveries(self, rule_id: int) -> int:
        with self.connect() as conn:
            cur = conn.execute("""
                DELETE FROM deliveries
                WHERE rule_id = ?
            """, (rule_id,))
            conn.commit()
            return cur.rowcount


    def backfill_rule(self, rule_id: int) -> int:
        with self.connect() as conn:
            row = conn.execute("""
                SELECT id, source_id, source_thread_id
                FROM routing
                WHERE id = ?
                LIMIT 1
            """, (rule_id,)).fetchone()

            if not row:
                return 0

            inserted = self._backfill_deliveries_for_rule_conn(
                conn,
                int(row["id"]),
                str(row["source_id"]),
                row["source_thread_id"],
            )
            conn.commit()
            return inserted

    def get_rule_first_pending_message_id(self, rule_id: int) -> int | None:
        with self.connect() as conn:
            row = conn.execute("""
                SELECT p.message_id
                FROM deliveries d
                JOIN posts p ON p.id = d.post_id
                WHERE d.rule_id = ?
                AND d.status = 'pending'
                ORDER BY p.id ASC
                LIMIT 1
            """, (rule_id,)).fetchone()

            if not row:
                return None

            return int(row["message_id"])


    def get_rule_sent_message_ids(self, rule_id: int) -> list[int]:
        with self.connect() as conn:
            rows = conn.execute("""
                SELECT p.message_id
                FROM deliveries d
                JOIN posts p ON p.id = d.post_id
                WHERE d.rule_id = ?
                AND d.status = 'sent'
                ORDER BY p.id ASC
            """, (rule_id,)).fetchall()

            return [int(row["message_id"]) for row in rows]


    def mark_rule_messages_sent(
        self,
        rule_id: int,
        source_channel: str,
        source_thread_id: int | None,
        message_ids: list[int],
    ) -> int:
        if not message_ids:
            return 0

        with self.connect() as conn:
            placeholders = ",".join("?" for _ in message_ids)

            if source_thread_id is None:
                sql = f"""
                    UPDATE deliveries
                    SET status = 'sent',
                        sent_at = ?,
                        error_text = NULL
                    WHERE rule_id = ?
                    AND post_id IN (
                        SELECT id
                        FROM posts
                        WHERE source_channel = ?
                            AND source_thread_id IS NULL
                            AND message_id IN ({placeholders})
                    )
                """
                params = [utc_now_iso(), rule_id, str(source_channel), *message_ids]
            else:
                sql = f"""
                    UPDATE deliveries
                    SET status = 'sent',
                        sent_at = ?,
                        error_text = NULL
                    WHERE rule_id = ?
                    AND post_id IN (
                        SELECT id
                        FROM posts
                        WHERE source_channel = ?
                            AND source_thread_id = ?
                            AND message_id IN ({placeholders})
                    )
                """
                params = [utc_now_iso(), rule_id, str(source_channel), source_thread_id, *message_ids]

            cur = conn.execute(sql, params)
            conn.commit()
            return cur.rowcount


    def drop_rule_pending_before_message(
        self,
        rule_id: int,
        source_channel: str,
        source_thread_id: int | None,
        message_id: int,
    ) -> int:
        with self.connect() as conn:
            if source_thread_id is None:
                cur = conn.execute("""
                    DELETE FROM deliveries
                    WHERE rule_id = ?
                    AND status = 'pending'
                    AND post_id IN (
                        SELECT id
                        FROM posts
                        WHERE source_channel = ?
                            AND source_thread_id IS NULL
                            AND message_id < ?
                    )
                """, (rule_id, str(source_channel), int(message_id)))
            else:
                cur = conn.execute("""
                    DELETE FROM deliveries
                    WHERE rule_id = ?
                    AND status = 'pending'
                    AND post_id IN (
                        SELECT id
                        FROM posts
                        WHERE source_channel = ?
                            AND source_thread_id = ?
                            AND message_id < ?
                    )
                """, (rule_id, str(source_channel), source_thread_id, int(message_id)))

            conn.commit()
            return cur.rowcount


    def get_rule_next_run_at(self, rule_id: int) -> str | None:
        with self.connect() as conn:
            row = conn.execute("""
                SELECT next_run_at
                FROM routing
                WHERE id = ?
                LIMIT 1
            """, (rule_id,)).fetchone()

            if not row:
                return None

            return row["next_run_at"]

    def get_rule_sent_count(self, rule_id: int) -> int:
        with self.connect() as conn:
            row = conn.execute("""
                SELECT COUNT(*) AS cnt
                FROM deliveries
                WHERE rule_id = ?
                AND status = 'sent'
            """, (rule_id,)).fetchone()

            return int(row["cnt"] or 0) if row else 0



    def drop_rule_first_n_deliveries(self, rule_id: int, count: int) -> int:
        if count <= 0:
            return 0

        with self.connect() as conn:
            rows = conn.execute("""
                SELECT d.id
                FROM deliveries d
                JOIN posts p ON p.id = d.post_id
                WHERE d.rule_id = ?
                ORDER BY p.id ASC
                LIMIT ?
            """, (rule_id, int(count))).fetchall()

            ids = [int(row["id"]) for row in rows]
            if not ids:
                return 0

            placeholders = ",".join("?" for _ in ids)
            cur = conn.execute(f"""
                DELETE FROM deliveries
                WHERE id IN ({placeholders})
            """, ids)

            conn.commit()
            return cur.rowcount

    def update_rule_mode(self, rule_id: int, mode: str) -> bool:
        if mode not in ("repost", "video"):
            return False

        with self.connect() as conn:
            cur = conn.execute("""
                UPDATE routing
                SET mode = ?
                WHERE id = ?
            """, (mode, rule_id))
            conn.commit()
            return cur.rowcount > 0

    def update_rule_caption_delivery_mode(self, rule_id: int, mode: str) -> bool:
        normalized = (mode or "auto").strip().lower()

        if normalized not in ("copy_first", "builder_first", "auto"):
            normalized = "auto"

        with self.connect() as conn:
            cur = conn.execute(
                """
                UPDATE routing
                SET caption_delivery_mode = ?
                WHERE id = ?
                """,
                (normalized, int(rule_id)),
            )
            conn.commit()
            return cur.rowcount > 0

    def update_rule_video_caption_delivery_mode(
        self,
        rule_id: int,
        video_caption_delivery_mode: str,
    ) -> bool:
        allowed = {"copy_first", "builder_first", "auto"}
        normalized = (video_caption_delivery_mode or "").strip().lower()

        if normalized not in allowed:
            return False

        with self.connect() as conn:
            cur = conn.execute("""
                UPDATE routing
                SET video_caption_delivery_mode = ?
                WHERE id = ?
            """, (normalized, rule_id))
            conn.commit()
            return cur.rowcount > 0

    def update_rule_video_caption(
        self,
        rule_id: int,
        caption: str | None,
        caption_entities_json: str | None = None,
    ) -> bool:
        with self.connect() as conn:
            cur = conn.execute("""
                UPDATE routing
                SET video_caption = ?,
                    video_caption_entities_json = ?
                WHERE id = ?
            """, (caption, caption_entities_json, rule_id))
            conn.commit()
            return cur.rowcount > 0

    def update_rule_video_intro_horizontal(self, rule_id: int, path: str | None) -> bool:
        with self.connect() as conn:
            cur = conn.execute("""
                UPDATE routing
                SET video_intro_horizontal = ?
                WHERE id = ?
            """, (path, rule_id))
            conn.commit()
            return cur.rowcount > 0

    def update_rule_video_intro_vertical(self, rule_id: int, path: str | None) -> bool:
        with self.connect() as conn:
            cur = conn.execute("""
                UPDATE routing
                SET video_intro_vertical = ?
                WHERE id = ?
            """, (path, rule_id))
            conn.commit()
            return cur.rowcount > 0



    def set_rule_video_intro_enabled(self, rule_id: int, enabled: bool) -> bool:
        with self.connect() as conn:
            cur = conn.execute("""
                UPDATE routing
                SET video_add_intro = ?
                WHERE id = ?
            """, (1 if enabled else 0, rule_id))
            conn.commit()
            return cur.rowcount > 0

    def update_rule_video_trim(self, rule_id: int, seconds: int) -> bool:
        with self.connect() as conn:
            cur = conn.execute("""
                UPDATE routing
                SET video_trim_seconds = ?
                WHERE id = ?
            """, (seconds, rule_id))
            conn.commit()
            return cur.rowcount > 0

    def touch_rule_after_send(self, rule_id: int, interval: int) -> None:
        now_iso = utc_now_iso()
        now_dt = datetime.now(timezone.utc)

        with self.connect() as conn:
            row = conn.execute("""
                SELECT schedule_mode, fixed_times_json, interval
                FROM routing
                WHERE id = ?
            """, (rule_id,)).fetchone()

            if not row:
                return

            pending_row = conn.execute("""
                SELECT COUNT(*) AS cnt
                FROM deliveries
                WHERE rule_id = ?
                  AND status = 'pending'
            """, (rule_id,)).fetchone()

            pending_count = int(pending_row["cnt"] or 0) if pending_row else 0

            if pending_count <= 0:
                next_run_iso = None
            else:
                schedule_mode = row["schedule_mode"] or "interval"

                if schedule_mode == "fixed":
                    fixed_times_json = row["fixed_times_json"]
                    try:
                        fixed_times = json.loads(fixed_times_json) if fixed_times_json else []
                    except Exception:
                        fixed_times = []

                    next_run_iso = get_next_fixed_run_utc(fixed_times, now_dt)
                else:
                    actual_interval = int(row["interval"] or interval or 0)
                    base_dt = datetime.fromtimestamp(
                        now_dt.timestamp() + max(actual_interval, 1),
                        tz=timezone.utc,
                    )
                    next_run_iso = self._find_next_interval_slot(conn, base_dt, exclude_rule_id=rule_id)

            conn.execute("""
                UPDATE routing
                SET last_sent_at = ?, next_run_at = ?
                WHERE id = ?
            """, (now_iso, next_run_iso, rule_id))
            conn.commit()

    def get_due_delivery(self, rule_id: int, due_iso: str):
        with self.connect() as conn:
            return conn.execute("""
                SELECT d.id AS delivery_id, p.message_id, p.source_channel, p.source_thread_id, p.content_json, p.media_group_id, r.target_id, r.target_thread_id, r.interval
                FROM deliveries d
                JOIN posts p ON p.id = d.post_id
                JOIN routing r ON r.id = d.rule_id
                WHERE d.rule_id = ? AND d.status = 'pending' AND r.is_active = 1 AND (r.next_run_at IS NULL OR r.next_run_at <= ?)
                ORDER BY p.id ASC LIMIT 1
            """, (rule_id, due_iso)).fetchone()

    def take_due_delivery(self, rule_id: int, due_iso: str):
        with self.connect() as conn:
            # --- получаем режим правила ---
            rule_row = conn.execute("""
                SELECT mode
                FROM routing
                WHERE id = ?
                LIMIT 1
            """, (rule_id,)).fetchone()

            if not rule_row:
                return None

            rule_mode = (rule_row["mode"] or "repost").strip().lower()

            # --- выбираем следующую задачу ---
            row = conn.execute("""
                SELECT
                    d.id AS delivery_id,
                    p.id AS post_id,
                    p.message_id,
                    p.source_channel,
                    p.source_thread_id,
                    p.content_json,
                    p.media_group_id,
                    r.target_id,
                    r.target_thread_id,
                    r.interval
                FROM deliveries d
                JOIN routing r ON r.id = d.rule_id
                JOIN posts p ON p.id = d.post_id
                WHERE d.rule_id = ?
                AND d.status = 'pending'
                AND r.is_active = 1
                AND (r.next_run_at IS NULL OR r.next_run_at <= ?)

                -- ВАЖНО: фильтр для video режима
                AND (
                    ? != 'video'
                    OR json_extract(p.content_json, '$.media_kind') = 'video'
                )

                -- не брать, если уже есть processing
                AND NOT EXISTS (
                    SELECT 1
                    FROM deliveries d_rule
                    WHERE d_rule.rule_id = d.rule_id
                    AND d_rule.status = 'processing'
                )

                -- защита от повторного альбома только для repost
                AND NOT EXISTS (
                    SELECT 1
                    FROM deliveries d_sent
                    JOIN posts p_sent ON p_sent.id = d_sent.post_id
                    WHERE d_sent.rule_id = d.rule_id
                    AND d_sent.status = 'sent'
                    AND p.media_group_id IS NOT NULL
                    AND p_sent.source_channel = p.source_channel
                    AND (
                            (p_sent.source_thread_id IS NULL AND p.source_thread_id IS NULL)
                            OR p_sent.source_thread_id = p.source_thread_id
                        )
                    AND p_sent.media_group_id = p.media_group_id
                    AND ? = 'repost'
                )

                -- не бомбить один target
                AND NOT EXISTS (
                    SELECT 1
                    FROM deliveries d_target
                    JOIN routing r_target ON r_target.id = d_target.rule_id
                    WHERE d_target.status = 'processing'
                    AND r_target.target_id = r.target_id
                    AND (
                            (r_target.target_thread_id IS NULL AND r.target_thread_id IS NULL)
                            OR
                            (r_target.target_thread_id = r.target_thread_id)
                        )
                )

                ORDER BY p.id ASC
                LIMIT 1
            """, (rule_id, due_iso, rule_mode, rule_mode)).fetchone()

            if not row:
                return None

            delivery_id = int(row["delivery_id"])
            source_channel = str(row["source_channel"])
            media_group_id = row["media_group_id"]

            # --- логика захвата ---
            if media_group_id and rule_mode != "video":
                # REPOST режим — берём весь альбом
                source_thread_id = row["source_thread_id"]

                album_rows = conn.execute("""
                    SELECT d.id AS delivery_id
                    FROM deliveries d
                    JOIN posts p ON p.id = d.post_id
                    WHERE d.rule_id = ?
                    AND d.status = 'pending'
                    AND p.source_channel = ?
                    AND (
                            (p.source_thread_id IS NULL AND ? IS NULL)
                            OR p.source_thread_id = ?
                        )
                    AND p.media_group_id = ?
                    ORDER BY p.message_id ASC
                """, (
                    rule_id,
                    source_channel,
                    source_thread_id,
                    source_thread_id,
                    str(media_group_id),
                )).fetchall()

                delivery_ids = [int(r["delivery_id"]) for r in album_rows]

                if not delivery_ids:
                    conn.commit()
                    return None

                conn.executemany("""
                    UPDATE deliveries
                    SET status = 'processing'
                    WHERE id = ? AND status = 'pending'
                """, [(d_id,) for d_id in delivery_ids])

                # проверка что всё захватили
                processing_count_row = conn.execute(f"""
                    SELECT COUNT(*) AS cnt
                    FROM deliveries
                    WHERE id IN ({",".join(["?"] * len(delivery_ids))})
                    AND status = 'processing'
                """, delivery_ids).fetchone()

                processing_count = int(processing_count_row["cnt"] or 0) if processing_count_row else 0

                if processing_count != len(delivery_ids):
                    conn.commit()
                    return None

            else:
                # VIDEO режим — всегда 1 элемент
                cur = conn.execute("""
                    UPDATE deliveries
                    SET status = 'processing'
                    WHERE id = ? AND status = 'pending'
                """, (delivery_id,))

                if cur.rowcount == 0:
                    conn.commit()
                    return None

            # --- возвращаем задачу ---
            taken = conn.execute("""
                SELECT
                    d.id AS delivery_id,
                    p.message_id,
                    p.source_channel,
                    p.source_thread_id,
                    p.content_json,
                    p.media_group_id,
                    r.target_id,
                    r.target_thread_id,
                    r.interval
                FROM deliveries d
                JOIN posts p ON p.id = d.post_id
                JOIN routing r ON r.id = d.rule_id
                WHERE d.id = ?
                LIMIT 1
            """, (delivery_id,)).fetchone()

            conn.commit()
            return taken

    def get_album_pending_for_rule(
        self,
        rule_id: int,
        source_channel: str,
        source_thread_id: int | None,
        media_group_id: str,
    ):
        with self.connect() as conn:
            return conn.execute("""
                SELECT d.id AS delivery_id, p.message_id
                FROM deliveries d
                JOIN posts p ON p.id = d.post_id
                WHERE d.rule_id = ?
                AND d.status IN ('pending', 'processing')
                AND p.source_channel = ?
                AND (
                        (p.source_thread_id IS NULL AND ? IS NULL)
                        OR p.source_thread_id = ?
                    )
                AND p.media_group_id = ?
                ORDER BY p.message_id
            """, (
                rule_id,
                str(source_channel),
                source_thread_id,
                source_thread_id,
                media_group_id,
            )).fetchall()
    def mark_delivery_sent(self, delivery_id: int): 
        with self.connect() as conn: conn.execute("UPDATE deliveries SET status = 'sent', sent_at = ?, error_text = NULL WHERE id = ?", (utc_now_iso(), delivery_id)); conn.commit()
    def mark_many_deliveries_sent(self, delivery_ids):
        if not delivery_ids: return
        with self.connect() as conn:
            conn.executemany("UPDATE deliveries SET status = 'sent', sent_at = ?, error_text = NULL WHERE id = ?", [(utc_now_iso(), d) for d in delivery_ids]); conn.commit()
    def mark_delivery_faulty(self, delivery_id: int, error_text: str):
        with self.connect() as conn: conn.execute("UPDATE deliveries SET status = 'faulty', error_text = ?, attempt_count = attempt_count + 1 WHERE id = ?", (error_text[:1000], delivery_id)); conn.commit()

    def mark_delivery_pending(self, delivery_id: int):
        with self.connect() as conn:
            conn.execute("""
                UPDATE deliveries
                SET status = 'pending'
                WHERE id = ?
            """, (delivery_id,))
            conn.commit()

    def reset_stuck_processing(self) -> int:
        with self.connect() as conn:
            cur = conn.execute("""
                UPDATE deliveries
                SET status = 'pending'
                WHERE status = 'processing'
            """)
            conn.commit()
            return cur.rowcount

    def backfill_deliveries_all(self) -> int:
        total = 0
        with self.connect() as conn:
            for rule in conn.execute("SELECT id, source_id, source_thread_id FROM routing").fetchall():
                total += self._backfill_deliveries_for_rule_conn(conn, int(rule["id"]), rule["source_id"], rule["source_thread_id"])
            conn.commit()
        return total
    def reset_all_deliveries(self):
        with self.connect() as conn:
            cur = conn.execute("""
                UPDATE deliveries
                SET status = 'pending',
                    error_text = NULL,
                    sent_at = NULL
                WHERE status IN ('processing', 'sent', 'faulty')
            """)
            faulty = conn.execute("SELECT COUNT(*) AS cnt FROM deliveries WHERE status = 'faulty'").fetchone()["cnt"]
            conn.commit()
            return cur.rowcount, int(faulty)
    def reset_source_deliveries(self, source_id: str, source_thread_id: int | None = None):
        with self.connect() as conn:
            if source_thread_id is None:
                cur = conn.execute("UPDATE deliveries SET status='pending', error_text=NULL, sent_at=NULL WHERE post_id IN (SELECT id FROM posts WHERE source_channel = ? AND source_thread_id IS NULL)", (str(source_id),))
            else:
                cur = conn.execute("UPDATE deliveries SET status='pending', error_text=NULL, sent_at=NULL WHERE post_id IN (SELECT id FROM posts WHERE source_channel = ? AND source_thread_id = ?)", (str(source_id), source_thread_id))
            conn.commit(); return cur.rowcount
    def get_queue_stats(self):
        with self.connect() as conn:
            return {
                "posts": int(conn.execute("SELECT COUNT(*) FROM posts").fetchone()[0]),
                "deliveries": int(conn.execute("SELECT COUNT(*) FROM deliveries").fetchone()[0]),
                "pending": int(conn.execute("SELECT COUNT(*) FROM deliveries WHERE status='pending'").fetchone()[0]),
                "sent": int(conn.execute("SELECT COUNT(*) FROM deliveries WHERE status='sent'").fetchone()[0]),
                "faulty": int(conn.execute("SELECT COUNT(*) FROM deliveries WHERE status='faulty'").fetchone()[0]),
                "rules": int(conn.execute("SELECT COUNT(*) FROM routing").fetchone()[0]),
                "active_rules": int(conn.execute("SELECT COUNT(*) FROM routing WHERE is_active = 1").fetchone()[0]),
            }
    def get_faulty_deliveries(self, limit: int = 20):
        with self.connect() as conn:
            return conn.execute("""
                SELECT
                    d.id,
                    d.rule_id,
                    d.post_id,
                    d.status,
                    d.error_text,
                    d.attempt_count,
                    d.created_at,
                    d.sent_at,

                    p.message_id,
                    p.source_channel,
                    p.source_thread_id,
                    p.media_group_id,
                    p.content_json,

                    r.target_id,
                    r.target_thread_id,

                    s.title AS source_title,
                    t.title AS target_title,

                    (
                        SELECT COUNT(*)
                        FROM deliveries d2
                        WHERE d2.rule_id = d.rule_id
                        AND d2.error_text = d.error_text
                        AND d2.status = 'faulty'
                    ) AS same_error_count,

                    (
                        SELECT a.created_at
                        FROM audit_log a
                        WHERE a.delivery_id = d.id
                        AND a.status IN ('faulty', 'failed')
                        ORDER BY a.id DESC
                        LIMIT 1
                    ) AS fault_created_at

                FROM deliveries d
                JOIN posts p ON p.id = d.post_id
                JOIN routing r ON r.id = d.rule_id
                LEFT JOIN channels s
                    ON s.channel_id = r.source_id
                AND ((s.thread_id IS NULL AND r.source_thread_id IS NULL) OR s.thread_id = r.source_thread_id)
                AND s.channel_type = 'source'
                LEFT JOIN channels t
                    ON t.channel_id = r.target_id
                AND ((t.thread_id IS NULL AND r.target_thread_id IS NULL) OR t.thread_id = r.target_thread_id)
                AND t.channel_type = 'target'
                WHERE d.status = 'faulty'
                    AND (d.error_text IS NULL OR d.error_text NOT LIKE 'Self-loop:%')
                ORDER BY d.id DESC
                LIMIT ?
            """, (limit,)).fetchall()

    def get_delivery(self, delivery_id: int):
        with self.connect() as conn:
            return conn.execute("""
                SELECT
                    d.id,
                    d.rule_id,
                    d.post_id,
                    d.status,
                    d.error_text,
                    d.attempt_count,
                    d.created_at,
                    d.sent_at,

                    p.message_id,
                    p.source_channel,
                    p.source_thread_id,
                    p.media_group_id,
                    p.content_json,

                    r.target_id,
                    r.target_thread_id,

                    s.title AS source_title,
                    t.title AS target_title

                FROM deliveries d
                JOIN posts p ON p.id = d.post_id
                JOIN routing r ON r.id = d.rule_id
                LEFT JOIN channels s
                    ON s.channel_id = r.source_id
                AND ((s.thread_id IS NULL AND r.source_thread_id IS NULL) OR s.thread_id = r.source_thread_id)
                AND s.channel_type = 'source'
                LEFT JOIN channels t
                    ON t.channel_id = r.target_id
                AND ((t.thread_id IS NULL AND r.target_thread_id IS NULL) OR t.thread_id = r.target_thread_id)
                AND t.channel_type = 'target'
                WHERE d.id = ?
                LIMIT 1
            """, (delivery_id,)).fetchone()

    def clear_faulty_delivery_log(self, delivery_id: int, admin_id: int | None = None) -> bool:
        with self.connect() as conn:
            row = conn.execute("""
                SELECT id, rule_id, post_id, status, error_text
                FROM deliveries
                WHERE id = ?
                AND status = 'faulty'
                LIMIT 1
            """, (delivery_id,)).fetchone()

            if not row:
                return False

            now_iso = utc_now_iso()

            conn.execute("""
                INSERT INTO audit_log(
                    created_at,
                    event_type,
                    rule_id,
                    delivery_id,
                    post_id,
                    admin_id,
                    status,
                    error_text,
                    extra_json
                )
                VALUES(?,?,?,?,?,?,?,?,?)
            """, (
                now_iso,
                "faulty_log_cleared",
                row["rule_id"],
                row["id"],
                row["post_id"],
                admin_id,
                "cleared",
                row["error_text"],
                json.dumps({
                    "action": "clear_faulty_delivery_log"
                }, ensure_ascii=False),
            ))

            cur = conn.execute("""
                DELETE FROM deliveries
                WHERE id = ?
                AND status = 'faulty'
            """, (delivery_id,))

            conn.commit()
            return cur.rowcount > 0

    def get_faulty_delivery_by_id(self, delivery_id: int):
        with self.connect() as conn:
            return conn.execute("""
                SELECT
                    d.id,
                    d.rule_id,
                    d.post_id,
                    d.status,
                    d.error_text,
                    d.attempt_count,
                    d.created_at,
                    d.sent_at,

                    p.message_id,
                    p.source_channel,
                    p.source_thread_id,
                    p.media_group_id,
                    p.content_json,

                    r.target_id,
                    r.target_thread_id,

                    s.title AS source_title,
                    t.title AS target_title

                FROM deliveries d
                JOIN posts p ON p.id = d.post_id
                JOIN routing r ON r.id = d.rule_id
                LEFT JOIN channels s
                    ON s.channel_id = r.source_id
                AND ((s.thread_id IS NULL AND r.source_thread_id IS NULL) OR s.thread_id = r.source_thread_id)
                AND s.channel_type = 'source'
                LEFT JOIN channels t
                    ON t.channel_id = r.target_id
                AND ((t.thread_id IS NULL AND r.target_thread_id IS NULL) OR t.thread_id = r.target_thread_id)
                AND t.channel_type = 'target'
                WHERE d.id = ?
                LIMIT 1
            """, (delivery_id,)).fetchone()

    def get_rule_stats(self):
        with self.connect() as conn:
            return conn.execute("""
                SELECT
                    r.id,
                    r.source_id,
                    r.source_thread_id,
                    r.target_id,
                    r.target_thread_id,
                    r.interval,
                    r.schedule_mode,
                    r.fixed_times_json,
                    r.mode,

                    r.video_trim_seconds,
                    r.video_add_intro,
                    r.video_intro_horizontal,
                    r.video_intro_vertical,
                    r.video_intro_horizontal_id,
                    r.video_intro_vertical_id,
                    r.video_caption,
                    r.video_caption_entities_json,

                    r.is_active,
                    r.next_run_at,
                    s.title AS source_title,
                    t.title AS target_title,
                    SUM(CASE WHEN d.status = 'pending' THEN 1 ELSE 0 END) AS pending,
                    SUM(CASE WHEN d.status = 'processing' THEN 1 ELSE 0 END) AS processing,
                    SUM(CASE WHEN d.status = 'sent' THEN 1 ELSE 0 END) AS sent,
                    SUM(CASE WHEN d.status = 'faulty' THEN 1 ELSE 0 END) AS faulty
                FROM routing r
                LEFT JOIN deliveries d ON d.rule_id = r.id
                LEFT JOIN channels s ON s.channel_id = r.source_id AND ((s.thread_id IS NULL AND r.source_thread_id IS NULL) OR s.thread_id = r.source_thread_id) AND s.channel_type = 'source'
                LEFT JOIN channels t ON t.channel_id = r.target_id AND ((t.thread_id IS NULL AND r.target_thread_id IS NULL) OR t.thread_id = r.target_thread_id) AND t.channel_type = 'target'
                GROUP BY r.id ORDER BY r.created_date, r.id
            """).fetchall()
    def get_rule_faulty_count(self, rule_id: int) -> int:
        with self.connect() as conn:
            row = conn.execute("""
                SELECT COUNT(*)
                FROM deliveries
                WHERE rule_id = ?
                AND status = 'faulty'
            """, (rule_id,)).fetchone()

            return int(row[0]) if row else 0

    def _backfill_deliveries_for_rule_conn(self, conn, rule_id: int, source_id: str, source_thread_id: int | None):
        rows = conn.execute("SELECT id FROM posts WHERE source_channel = ? AND " + ("source_thread_id IS NULL" if source_thread_id is None else "source_thread_id = ?") + " ORDER BY id", (str(source_id),) if source_thread_id is None else (str(source_id), source_thread_id)).fetchall()
        inserted = 0
        for row in rows:
            cur = conn.execute("INSERT OR IGNORE INTO deliveries(rule_id, post_id, status, created_at) VALUES(?,?,'pending',?)", (rule_id, int(row["id"]), utc_now_iso())); inserted += cur.rowcount
        return inserted
    def _create_deliveries_for_post_conn(self, conn, post_id: int, source_channel: str, source_thread_id: int | None):
        rules = conn.execute("SELECT id FROM routing WHERE source_id = ? AND " + ("source_thread_id IS NULL" if source_thread_id is None else "source_thread_id = ?"), (str(source_channel),) if source_thread_id is None else (str(source_channel), source_thread_id)).fetchall()
        for rule in rules:
            conn.execute("INSERT OR IGNORE INTO deliveries(rule_id, post_id, status, created_at) VALUES(?,?,'pending',?)", (int(rule["id"]), post_id, utc_now_iso()))

    def update_rule_interval(self, rule_id: int, new_interval: int):
        with self.connect() as conn:
            base_dt = datetime.fromtimestamp(
                datetime.now(timezone.utc).timestamp() + max(new_interval, 1),
                tz=timezone.utc,
            )
            next_run_iso = self._find_next_interval_slot(conn, base_dt, exclude_rule_id=rule_id)

            conn.execute("""
                UPDATE routing
                SET schedule_mode = 'interval',
                    interval = ?,
                    next_run_at = ?
                WHERE id = ?
            """, (new_interval, next_run_iso, rule_id))
            conn.commit()

    def update_rule_next_run_at(self, rule_id: int, next_run_iso: str) -> bool:
        with self.connect() as conn:
            cur = conn.execute("""
                UPDATE routing
                SET next_run_at = ?
                WHERE id = ?
            """, (next_run_iso, rule_id))
            conn.commit()
            return cur.rowcount > 0
    def trigger_rule_now(self, rule_id: int) -> bool:
        with self.connect() as conn:
            cur = conn.execute("""
                UPDATE routing
                SET next_run_at = ?
                WHERE id = ? AND is_active = 1
            """, (utc_now_iso(), rule_id))
            conn.commit()
            return cur.rowcount > 0

    def get_rule_queue_logical_items(self, rule_id: int) -> list[dict[str, Any]]:
        """
        Возвращает логическую очередь правила.

        ВАЖНО:
        - использует единый builder
        - считает только pending-элементы
        - repost/video обрабатываются единообразно
        """
        try:
            rule = self.get_rule(rule_id)
            if not rule:
                logger.warning(
                    "get_rule_queue_logical_items: правило не найдено, rule_id=%s",
                    rule_id,
                )
                return []

            mode = (getattr(rule, "mode", "repost") or "repost").strip().lower()

            with self.connect() as conn:
                rows = conn.execute("""
                    SELECT
                        d.id AS delivery_id,
                        d.status,
                        d.sent_at,
                        p.id AS post_id,
                        p.message_id,
                        p.source_channel,
                        p.source_thread_id,
                        p.media_group_id,
                        p.content_json,
                        p.created_at
                    FROM deliveries d
                    JOIN posts p ON p.id = d.post_id
                    WHERE d.rule_id = ?
                    AND d.status = 'pending'
                    ORDER BY p.id ASC
                """, (rule_id,)).fetchall()

            items = self._build_rule_logical_items_from_rows(
                rule_id=rule_id,
                rows=rows,
                mode=mode,
            )

            logger.info(
                "get_rule_queue_logical_items: rule_id=%s, mode=%s, raw_rows=%s, logical_items=%s",
                rule_id,
                mode,
                len(rows),
                len(items),
            )
            return items

        except Exception as exc:
            logger.exception(
                "get_rule_queue_logical_items: авария расчёта очереди, rule_id=%s, error=%s",
                rule_id,
                exc,
            )
            return []

    def get_rule_queue_item_by_position(self, rule_id: int, position: int) -> dict[str, Any] | None:
        items = self.get_rule_queue_logical_items(rule_id)
        if not items:
            return None

        position = max(1, min(position, len(items)))
        return items[position - 1]

    def get_rule_queue_item_shifted(self, rule_id: int, current_position: int, shift: int) -> dict[str, Any] | None:
        items = self.get_rule_queue_logical_items(rule_id)
        if not items:
            return None

        new_position = current_position + shift
        new_position = max(1, min(new_position, len(items)))
        return items[new_position - 1]

    def set_rule_start_from_position(self, rule_id: int, position: int) -> dict[str, Any] | None:
        """
        Сдвигает точку старта по ЛОГИЧЕСКОЙ очереди правила.

        ВАЖНО:
        - использует тот же builder, что и queue/position/card
        - работает только по pending logical items
        - для repost альбом считается одним элементом
        - для video каждое видео считается отдельным элементом
        """
        try:
            items = self.get_rule_queue_logical_items(rule_id)
            if not items:
                logger.warning(
                    "set_rule_start_from_position: логическая очередь пуста, rule_id=%s",
                    rule_id,
                )
                return None

            position = max(1, min(position, len(items)))
            selected = items[position - 1]

            ids_to_mark_sent: list[int] = []
            for item in items:
                if int(item["position"]) >= position:
                    break
                ids_to_mark_sent.extend(int(delivery_id) for delivery_id in item["delivery_ids"])

            now_iso = utc_now_iso()

            with self.connect() as conn:
                if ids_to_mark_sent:
                    conn.executemany("""
                        UPDATE deliveries
                        SET status = 'sent',
                            sent_at = ?,
                            error_text = NULL
                        WHERE id = ?
                    """, [(now_iso, delivery_id) for delivery_id in ids_to_mark_sent])

                conn.execute("""
                    UPDATE routing
                    SET next_run_at = ?
                    WHERE id = ?
                """, (now_iso, rule_id))

                conn.execute("""
                    INSERT INTO audit_log(
                        created_at,
                        event_type,
                        rule_id,
                        post_id,
                        status,
                        new_value_json,
                        extra_json
                    )
                    VALUES(?,?,?,?,?,?,?)
                """, (
                    now_iso,
                    "rule_start_position_changed",
                    rule_id,
                    selected["first_post_id"],
                    "pending",
                    json.dumps({
                        "position": selected["position"],
                        "kind": selected["kind"],
                        "mode": selected.get("mode"),
                        "first_post_id": selected["first_post_id"],
                        "first_message_id": selected["first_message_id"],
                        "delivery_ids": selected["delivery_ids"],
                        "count": selected.get("count"),
                    }, ensure_ascii=False),
                    json.dumps({
                        "skipped_delivery_ids": ids_to_mark_sent,
                        "skipped_logical_items_count": max(position - 1, 0),
                    }, ensure_ascii=False),
                ))

                conn.commit()

            logger.info(
                "set_rule_start_from_position: rule_id=%s, selected_position=%s, selected_kind=%s, skipped_delivery_ids=%s",
                rule_id,
                selected["position"],
                selected["kind"],
                len(ids_to_mark_sent),
            )
            return selected

        except Exception as exc:
            logger.exception(
                "set_rule_start_from_position: авария смены точки старта, rule_id=%s, position=%s, error=%s",
                rule_id,
                position,
                exc,
            )
            return None

    def get_rule_position_info(self, rule_id: int) -> dict[str, Any]:
        """
        Возвращает информацию о логической позиции правила.

        Логика полностью синхронизирована с get_rule_queue_logical_items()
        через единый builder.

        Возвращает:
        {
            "total": int,
            "current_position": int | None,
            "completed": int,
        }
        """
        empty_result = {
            "total": 0,
            "current_position": None,
            "completed": 0,
        }

        try:
            rule = self.get_rule(rule_id)
            if not rule:
                logger.warning(
                    "get_rule_position_info: правило не найдено, rule_id=%s",
                    rule_id,
                )
                return empty_result

            mode = (getattr(rule, "mode", "repost") or "repost").strip().lower()

            with self.connect() as conn:
                rows = conn.execute("""
                    SELECT
                        d.id AS delivery_id,
                        d.status,
                        d.sent_at,
                        p.id AS post_id,
                        p.message_id,
                        p.source_channel,
                        p.source_thread_id,
                        p.media_group_id,
                        p.content_json,
                        p.created_at
                    FROM deliveries d
                    JOIN posts p ON p.id = d.post_id
                    WHERE d.rule_id = ?
                    ORDER BY p.id ASC
                """, (rule_id,)).fetchall()

            if not rows:
                logger.info(
                    "get_rule_position_info: у правила нет deliveries, rule_id=%s, mode=%s",
                    rule_id,
                    mode,
                )
                return empty_result

            logical_items = self._build_rule_logical_items_from_rows(
                rule_id=rule_id,
                rows=rows,
                mode=mode,
            )

            total = len(logical_items)
            if total <= 0:
                logger.info(
                    "get_rule_position_info: после builder очередь пуста, rule_id=%s, mode=%s, raw_rows=%s",
                    rule_id,
                    mode,
                    len(rows),
                )
                return empty_result

            completed = sum(1 for item in logical_items if item.get("is_done"))
            current_position = None

            for item in logical_items:
                if not item.get("is_done"):
                    current_position = int(item["position"])
                    break

            if current_position is None:
                current_position = total

            result = {
                "total": total,
                "current_position": current_position,
                "completed": completed,
            }

            logger.info(
                "get_rule_position_info: rule_id=%s, mode=%s, raw_rows=%s, total=%s, completed=%s, current_position=%s",
                rule_id,
                mode,
                len(rows),
                total,
                completed,
                current_position,
            )
            return result

        except Exception as exc:
            logger.exception(
                "get_rule_position_info: авария расчёта позиции, rule_id=%s, error=%s",
                rule_id,
                exc,
            )
            return empty_result

SqliteRepository = Database
