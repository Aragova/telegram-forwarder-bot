# build_pg_repo.py
from pathlib import Path

code = r'''
# ================= FULL POSTGRES REPOSITORY (1:1 STYLE WITH SQLITE) =================
# Сгенерирован автоматически. Заменяет app/postgres_repository.py

from __future__ import annotations
import logging
from typing import Any, List, Dict, Optional

from app.postgres_client import PostgresClient
from app.repository import RepositoryProtocol

logger = logging.getLogger("forwarder.postgres")


class PostgresRepository(RepositoryProtocol):

    def __init__(self):
        self.client = PostgresClient()

    # ================= INIT =================

    def ensure_configured(self):
        self.client.connect()

    def init(self):
        self.client.execute_script("""
        CREATE TABLE IF NOT EXISTS routing (
            id SERIAL PRIMARY KEY,
            source_id TEXT NOT NULL,
            source_thread_id INTEGER,
            target_id TEXT NOT NULL,
            target_thread_id INTEGER,
            is_active BOOLEAN DEFAULT TRUE,
            next_run_at TIMESTAMPTZ
        );

        CREATE TABLE IF NOT EXISTS posts (
            id SERIAL PRIMARY KEY,
            message_id INTEGER NOT NULL,
            source_channel TEXT NOT NULL,
            source_thread_id INTEGER,
            media_group_id TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS deliveries (
            id SERIAL PRIMARY KEY,
            rule_id INTEGER NOT NULL,
            post_id INTEGER NOT NULL,
            status TEXT DEFAULT 'pending'
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_posts_unique_no_thread
        ON posts(message_id, source_channel)
        WHERE source_thread_id IS NULL;

        CREATE UNIQUE INDEX IF NOT EXISTS idx_posts_unique_with_thread
        ON posts(message_id, source_channel, source_thread_id)
        WHERE source_thread_id IS NOT NULL;
        """)

    # ================= POSTS =================

    def save_post_batch(self, posts_data):
        count = 0
        for msg_id, source_channel, thread_id, content, media_group_id in posts_data:

            self.client.execute("""
                INSERT INTO posts (message_id, source_channel, source_thread_id, media_group_id)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (msg_id, source_channel, thread_id, media_group_id))

            post = self.client.fetchone("""
                SELECT id FROM posts
                WHERE message_id = %s
                  AND source_channel = %s
                  AND (
                    (source_thread_id IS NULL AND %s IS NULL)
                    OR source_thread_id = %s
                  )
            """, (msg_id, source_channel, thread_id, thread_id))

            if not post:
                continue

            self._create_deliveries_for_post(post["id"], source_channel, thread_id)
            count += 1

        return count

    # ================= DELIVERY =================

    def _create_deliveries_for_post(self, post_id, source_channel, thread_id):

        if thread_id is None:
            rules = self.client.fetchall("""
                SELECT id FROM routing
                WHERE source_id = %s
                  AND source_thread_id IS NULL
                  AND is_active = TRUE
            """, (source_channel,))
        else:
            rules = self.client.fetchall("""
                SELECT id FROM routing
                WHERE source_id = %s
                  AND source_thread_id = %s
                  AND is_active = TRUE
            """, (source_channel, thread_id))

        for r in rules:
            self.client.execute("""
                INSERT INTO deliveries(rule_id, post_id, status)
                VALUES(%s, %s, 'pending')
                ON CONFLICT DO NOTHING
            """, (r["id"], post_id))

    def clear_rule_deliveries(self, rule_id):
        self.client.execute("DELETE FROM deliveries WHERE rule_id = %s", (rule_id,))
        return True

    def backfill_rule(self, rule_id):

        rule = self.get_rule(rule_id)
        if not rule:
            return 0

        if rule["source_thread_id"] is None:
            posts = self.client.fetchall("""
                SELECT id FROM posts
                WHERE source_channel = %s
                  AND source_thread_id IS NULL
                ORDER BY id
            """, (rule["source_id"],))
        else:
            posts = self.client.fetchall("""
                SELECT id FROM posts
                WHERE source_channel = %s
                  AND source_thread_id = %s
                ORDER BY id
            """, (rule["source_id"], rule["source_thread_id"]))

        for p in posts:
            self.client.execute("""
                INSERT INTO deliveries(rule_id, post_id, status)
                VALUES(%s, %s, 'pending')
                ON CONFLICT DO NOTHING
            """, (rule_id, p["id"]))

        return len(posts)

    # ================= RULE =================

    def get_rule(self, rule_id):
        return self.client.fetchone("SELECT * FROM routing WHERE id = %s", (rule_id,))

    def activate_rule_with_backfill(self, rule_id):
        self.client.execute("UPDATE routing SET is_active = TRUE WHERE id = %s", (rule_id,))
        self.clear_rule_deliveries(rule_id)
        self.backfill_rule(rule_id)
        return True

    def set_rule_active(self, rule_id, active):
        self.client.execute("UPDATE routing SET is_active = %s WHERE id = %s", (active, rule_id))
        return True

    # ================= QUEUE =================

    def get_rule_queue_logical_items(self, rule_id):

        rows = self.client.fetchall("""
            SELECT p.*
            FROM deliveries d
            JOIN posts p ON p.id = d.post_id
            WHERE d.rule_id = %s AND d.status = 'pending'
            ORDER BY p.id
        """, (rule_id,))

        grouped = {}
        for r in rows:
            key = r["media_group_id"] or f"single:{r['id']}"
            grouped.setdefault(key, []).append(r)

        result = []
        pos = 1
        for k, items in grouped.items():
            result.append({
                "position": pos,
                "items": items
            })
            pos += 1

        return result

    def get_rule_position_info(self, rule_id):
        items = self.get_rule_queue_logical_items(rule_id)
        return {"current_position": 1 if items else 0, "total": len(items)}

    def set_rule_start_from_position(self, rule_id, position):

        items = self.get_rule_queue_logical_items(rule_id)

        if not items or position < 1 or position > len(items):
            return None

        target = items[position - 1]

        message_ids = [i["message_id"] for i in target["items"]]

        self.mark_rule_messages_sent(
            rule_id,
            target["items"][0]["source_channel"],
            target["items"][0]["source_thread_id"],
            message_ids
        )

        return {"position": position}

    def mark_rule_messages_sent(self, rule_id, source_channel, thread_id, message_ids):

        if thread_id is None:
            self.client.execute("""
                UPDATE deliveries d
                SET status = 'sent'
                FROM posts p
                WHERE d.post_id = p.id
                  AND d.rule_id = %s
                  AND p.source_channel = %s
                  AND p.source_thread_id IS NULL
                  AND p.message_id = ANY(%s)
            """, (rule_id, source_channel, message_ids))
        else:
            self.client.execute("""
                UPDATE deliveries d
                SET status = 'sent'
                FROM posts p
                WHERE d.post_id = p.id
                  AND d.rule_id = %s
                  AND p.source_channel = %s
                  AND p.source_thread_id = %s
                  AND p.message_id = ANY(%s)
            """, (rule_id, source_channel, thread_id, message_ids))

    # ================= STATS =================

    def get_rule_stats(self):
        return self.client.fetchall("""
            SELECT
                r.id,
                r.source_id,
                r.target_id,
                r.is_active,
                COUNT(*) FILTER (WHERE d.status = 'pending') AS pending,
                COUNT(*) FILTER (WHERE d.status = 'sent') AS sent
            FROM routing r
            LEFT JOIN deliveries d ON d.rule_id = r.id
            GROUP BY r.id
            ORDER BY r.id
        """)
'''

Path("app/postgres_repository_full.py").write_text(code)
print("ГОТОВО: app/postgres_repository_full.py создан")
