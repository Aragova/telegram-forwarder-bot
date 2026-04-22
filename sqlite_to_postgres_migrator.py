from __future__ import annotations

import json
from typing import Any

from psycopg.types.json import Jsonb

from app.db import Database
from app.postgres_client import PostgresClient
from app.postgres_repository import PostgresRepository


class SQLiteToPostgresMigrator:
    def __init__(self) -> None:
        self.sqlite = Database()
        self.pg_client = PostgresClient()
        self.pg_repo = PostgresRepository()

    def ensure_ready(self) -> None:
        self.pg_repo.ensure_configured()
        self.pg_repo.init()

    def _jsonable(self, value: Any) -> str | None:
        if value is None:
            return None
        if isinstance(value, str):
            return value
        return json.dumps(value, ensure_ascii=False)

    def migrate_channels(self) -> int:
        with self.sqlite.connect() as conn:
            rows = conn.execute("""
                SELECT id, channel_id, thread_id, channel_type, title, added_by, added_date, is_active
                FROM channels
                ORDER BY id
            """).fetchall()

        if not rows:
            return 0

        params = []
        for row in rows:
            params.append((
                int(row["id"]),
                str(row["channel_id"]),
                row["thread_id"],
                row["channel_type"],
                row["title"],
                row["added_by"],
                row["added_date"],
                bool(row["is_active"]),
            ))

        self.pg_client.executemany("""
            INSERT INTO channels(
                id, channel_id, thread_id, channel_type, title, added_by, added_date, is_active
            )
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (id) DO NOTHING
        """, params)

        return len(params)

    def migrate_posts(self) -> int:
        with self.sqlite.connect() as conn:
            rows = conn.execute("""
                SELECT id, message_id, source_channel, source_thread_id, content_json, media_group_id, created_at, is_faulty
                FROM posts
                ORDER BY id
            """).fetchall()

        if not rows:
            return 0

        params = []
        for row in rows:
            params.append((
                int(row["id"]),
                int(row["message_id"]),
                str(row["source_channel"]),
                row["source_thread_id"],
                Jsonb(json.loads(row["content_json"]) if row["content_json"] else {}),
                row["media_group_id"],
                row["created_at"],
                bool(row["is_faulty"]),
            ))

        self.pg_client.executemany("""
            INSERT INTO posts(
                id, message_id, source_channel, source_thread_id, content_json, media_group_id, created_at, is_faulty
            )
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (id) DO NOTHING
        """, params)

        return len(params)

    def migrate_routing(self) -> int:
        with self.sqlite.connect() as conn:
            rows = conn.execute("""
                SELECT
                    id,
                    source_id,
                    source_thread_id,
                    target_id,
                    target_thread_id,
                    interval,
                    schedule_mode,
                    fixed_times_json,
                    mode,
                    video_trim_seconds,
                    video_add_intro,
                    video_intro_horizontal,
                    video_intro_vertical,
                    video_caption,
                    video_caption_entities_json,
                    caption_delivery_mode,
                    video_caption_delivery_mode,
                    video_intro_horizontal_id,
                    video_intro_vertical_id,
                    created_by,
                    created_date,
                    is_active,
                    next_run_at,
                    last_sent_at
                FROM routing
                ORDER BY id
            """).fetchall()

        if not rows:
            return 0

        params = []
        for row in rows:
            params.append((
                int(row["id"]),
                str(row["source_id"]),
                row["source_thread_id"],
                str(row["target_id"]),
                row["target_thread_id"],
                int(row["interval"]),
                row["schedule_mode"] or "interval",
                Jsonb(json.loads(row["fixed_times_json"])) if row["fixed_times_json"] else None,
                row["mode"] or "repost",
                int(row["video_trim_seconds"] or 120),
                bool(row["video_add_intro"] or 0),
                row["video_intro_horizontal"],
                row["video_intro_vertical"],
                row["video_caption"],
                Jsonb(json.loads(row["video_caption_entities_json"])) if row["video_caption_entities_json"] else None,
                (row["caption_delivery_mode"] or "auto"),
                (row["video_caption_delivery_mode"] or "auto"),
                row["video_intro_horizontal_id"],
                row["video_intro_vertical_id"],
                row["created_by"],
                row["created_date"],
                bool(row["is_active"]),
                row["next_run_at"],
                row["last_sent_at"],
            ))

        self.pg_client.executemany("""
            INSERT INTO routing(
                id,
                source_id,
                source_thread_id,
                target_id,
                target_thread_id,
                interval,
                schedule_mode,
                fixed_times_json,
                mode,
                video_trim_seconds,
                video_add_intro,
                video_intro_horizontal,
                video_intro_vertical,
                video_caption,
                video_caption_entities_json,
                caption_delivery_mode,
                video_caption_delivery_mode,
                video_intro_horizontal_id,
                video_intro_vertical_id,
                created_by,
                created_date,
                is_active,
                next_run_at,
                last_sent_at
            )
            VALUES(
                %s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,
                %s,%s,%s,%s
            )
            ON CONFLICT (id) DO NOTHING
        """, params)

        return len(params)

    def migrate_intros(self) -> int:
        with self.sqlite.connect() as conn:
            rows = conn.execute("""
                SELECT id, display_name, file_name, file_path, duration, created_at
                FROM intros
                ORDER BY id
            """).fetchall()

        if not rows:
            return 0

        params = []
        for row in rows:
            params.append((
                int(row["id"]),
                row["display_name"],
                row["file_name"],
                row["file_path"],
                int(row["duration"] or 0),
                row["created_at"],
            ))

        self.pg_client.executemany("""
            INSERT INTO intros(
                id, display_name, file_name, file_path, duration, created_at
            )
            VALUES(%s,%s,%s,%s,%s,%s)
            ON CONFLICT (id) DO NOTHING
        """, params)

        return len(params)

    def migrate_deliveries(self) -> int:
        with self.sqlite.connect() as conn:
            rows = conn.execute("""
                SELECT id, rule_id, post_id, status, error_text, attempt_count, created_at, sent_at
                FROM deliveries
                ORDER BY id
            """).fetchall()

        if not rows:
            return 0

        params = []
        for row in rows:
            params.append((
                int(row["id"]),
                int(row["rule_id"]),
                int(row["post_id"]),
                row["status"],
                row["error_text"],
                int(row["attempt_count"] or 0),
                row["created_at"],
                row["sent_at"],
            ))

        self.pg_client.executemany("""
            INSERT INTO deliveries(
                id, rule_id, post_id, status, error_text, attempt_count, created_at, sent_at
            )
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (id) DO NOTHING
        """, params)

        return len(params)

    def migrate_problem_state(self) -> int:
        with self.sqlite.connect() as conn:
            rows = conn.execute("""
                SELECT
                    id,
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
                FROM problem_state
                ORDER BY id
            """).fetchall()

        if not rows:
            return 0

        params = []
        for row in rows:
            params.append((
                int(row["id"]),
                row["problem_key"],
                row["problem_type"],
                row["rule_id"],
                row["delivery_id"],
                row["first_seen_at"],
                row["last_seen_at"],
                row["last_notified_at"],
                int(row["hit_count"] or 0),
                bool(row["is_active"]),
                bool(row["is_muted"]),
                Jsonb(json.loads(row["extra_json"])) if row["extra_json"] else None,
            ))

        self.pg_client.executemany("""
            INSERT INTO problem_state(
                id,
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
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (id) DO NOTHING
        """, params)

        return len(params)

    def migrate_audit_log(self) -> int:
        with self.sqlite.connect() as conn:
            rows = conn.execute("""
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
                FROM audit_log
                ORDER BY id
            """).fetchall()

        if not rows:
            return 0

        params = []
        for row in rows:
            params.append((
                int(row["id"]),
                row["created_at"],
                row["event_type"],
                row["rule_id"],
                row["delivery_id"],
                row["post_id"],
                row["admin_id"],
                row["source_id"],
                row["source_thread_id"],
                row["target_id"],
                row["target_thread_id"],
                row["status"],
                row["error_text"],
                Jsonb(json.loads(row["old_value_json"])) if row["old_value_json"] else None,
                Jsonb(json.loads(row["new_value_json"])) if row["new_value_json"] else None,
                Jsonb(json.loads(row["extra_json"])) if row["extra_json"] else None,
            ))

        self.pg_client.executemany("""
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
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (id) DO NOTHING
        """, params)

        return len(params)

    def fix_fixed_rules_next_run(self) -> int:
        import json
        from app.db import get_next_fixed_run_utc

        rows = self.pg_client.fetchall("""
            SELECT id, fixed_times_json
            FROM routing
            WHERE schedule_mode = 'fixed'
            ORDER BY id
        """)

        updated = 0

        for row in rows:
            rule_id = int(row["id"])
            raw = row["fixed_times_json"]

            try:
                if raw is None:
                    times = []
                elif isinstance(raw, str):
                    times = json.loads(raw)
                else:
                    times = raw
            except Exception:
                times = []

            if not times:
                continue

            next_run = get_next_fixed_run_utc(times)

            self.pg_client.execute("""
                UPDATE routing
                SET next_run_at = %s
                WHERE id = %s
            """, (next_run, rule_id))

            updated += 1

        return updated

    def run(self) -> dict[str, int]:
        self.ensure_ready()

        result = {
            "channels": self.migrate_channels(),
            "posts": self.migrate_posts(),
            "routing": self.migrate_routing(),
            "intros": self.migrate_intros(),
            "deliveries": self.migrate_deliveries(),
            "problem_state": self.migrate_problem_state(),
            "audit_log": self.migrate_audit_log(),
        }

        print("🔧 Fixing PostgreSQL sequences...")

        self.pg_client.execute("""
        SELECT setval('channels_id_seq', COALESCE((SELECT MAX(id) FROM channels), 1), true);
        """)

        self.pg_client.execute("""
        SELECT setval('posts_id_seq', COALESCE((SELECT MAX(id) FROM posts), 1), true);
        """)

        self.pg_client.execute("""
        SELECT setval('routing_id_seq', COALESCE((SELECT MAX(id) FROM routing), 1), true);
        """)

        self.pg_client.execute("""
        SELECT setval('intros_id_seq', COALESCE((SELECT MAX(id) FROM intros), 1), true);
        """)

        self.pg_client.execute("""
        SELECT setval('deliveries_id_seq', COALESCE((SELECT MAX(id) FROM deliveries), 1), true);
        """)

        self.pg_client.execute("""
        SELECT setval('problem_state_id_seq', COALESCE((SELECT MAX(id) FROM problem_state), 1), true);
        """)

        self.pg_client.execute("""
        SELECT setval('audit_log_id_seq', COALESCE((SELECT MAX(id) FROM audit_log), 1), true);
        """)

        print("✅ PostgreSQL sequences synced")

        fixed_updated = self.fix_fixed_rules_next_run()
        result["fixed_rules_next_run"] = fixed_updated

        return result


def main() -> None:
    migrator = SQLiteToPostgresMigrator()
    result = migrator.run()

    print("MIGRATION_OK")
    for key, value in result.items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    main()
