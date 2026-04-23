from __future__ import annotations

import json
import logging
from contextlib import contextmanager
from dataclasses import asdict
from datetime import datetime, timezone, timedelta
from typing import Any
from app.repository_models import (
    GLOBAL_INTERVAL_GAP_SECONDS,
    USER_TZ,
    IntroItem,
    Rule,
    get_next_fixed_run_utc,
    normalize_fixed_times,
    utc_now_iso,
)
from app.postgres_client import PostgresClient
from app.repository import RepositoryProtocol

logger = logging.getLogger("forwarder.postgres")


def _json_dumps(value: Any) -> str | None:
    if value is None:
        return None

    def _default(obj):
        from dataclasses import asdict, is_dataclass
        from datetime import date, datetime, time, timedelta

        if isinstance(obj, (datetime, date, time)):
            return obj.isoformat()

        if isinstance(obj, timedelta):
            return obj.total_seconds()

        if is_dataclass(obj):
            return asdict(obj)

        return str(obj)

    return json.dumps(value, ensure_ascii=False, default=_default)

def _safe_json_loads(raw: Any, default: Any) -> Any:
    if raw is None:
        return default

    if isinstance(raw, (dict, list)):
        return raw

    if isinstance(raw, (bytes, bytearray)):
        try:
            raw = raw.decode("utf-8", errors="ignore")
        except Exception:
            return default

    if isinstance(raw, str):
        raw = raw.strip()
        if not raw:
            return default
        try:
            return json.loads(raw)
        except Exception:
            return default

    return default


class PostgresRepository(RepositoryProtocol):
    def __init__(self) -> None:
        self.client = PostgresClient()

    # =========================================================
    # LOW LEVEL
    # =========================================================

    def ensure_configured(self) -> None:
        self.client.ensure_driver()
        if not self.client.is_configured():
            raise RuntimeError(
                "PostgreSQL не настроен: проверь APP_PG_HOST, APP_PG_PORT, APP_PG_DB, APP_PG_USER"
            )

    @contextmanager
    def connect(self):
        self.ensure_configured()
        with self.client.connect() as conn:
            yield conn

    def _row_to_rule(self, row) -> Rule:
        data = dict(row)

        return Rule(
            id=int(data["id"]),
            source_id=str(data["source_id"]),
            source_thread_id=data.get("source_thread_id"),
            target_id=str(data["target_id"]),
            target_thread_id=data.get("target_thread_id"),
            interval=int(data.get("interval") or 0),
            schedule_mode=data.get("schedule_mode", "interval"),
            fixed_times_json=data.get("fixed_times_json"),
            is_active=bool(data.get("is_active")),
            created_date=str(data.get("created_date") or ""),
            next_run_at=(data["next_run_at"].isoformat() if data.get("next_run_at") is not None and hasattr(data.get("next_run_at"), "isoformat") else data.get("next_run_at")),
            last_sent_at=(data["last_sent_at"].isoformat() if data.get("last_sent_at") is not None and hasattr(data.get("last_sent_at"), "isoformat") else data.get("last_sent_at")),
            source_title=data.get("source_title"),
            target_title=data.get("target_title"),
            mode=data.get("mode", "repost"),
            video_trim_seconds=int(data.get("video_trim_seconds") or 120),
            video_add_intro=bool(data.get("video_add_intro") or 0),
            video_intro_horizontal=data.get("video_intro_horizontal"),
            video_intro_vertical=data.get("video_intro_vertical"),
            video_intro_horizontal_id=data.get("video_intro_horizontal_id"),
            video_intro_vertical_id=data.get("video_intro_vertical_id"),
            video_caption=data.get("video_caption"),
            video_caption_entities_json=data.get("video_caption_entities_json"),
            caption_delivery_mode=data.get("caption_delivery_mode", "auto"),
            video_caption_delivery_mode=data.get("video_caption_delivery_mode", "auto"),
        )

    def _find_next_interval_slot(
        self,
        conn,
        base_dt: datetime,
        exclude_rule_id: int | None = None,
    ) -> str:
        candidate = base_dt

        while True:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, next_run_at
                    FROM routing
                    WHERE is_active = TRUE
                      AND schedule_mode = 'interval'
                      AND next_run_at IS NOT NULL
                    ORDER BY next_run_at ASC, id ASC
                    """
                )
                rows = cur.fetchall()

            conflict_found = False

            for row in rows:
                other_rule_id = int(row["id"])
                if exclude_rule_id is not None and other_rule_id == exclude_rule_id:
                    continue

                try:
                    other_dt = datetime.fromisoformat(str(row["next_run_at"]))
                except Exception:
                    continue

                delta = abs((other_dt - candidate).total_seconds())
                if delta < GLOBAL_INTERVAL_GAP_SECONDS:
                    candidate = other_dt + timedelta(seconds=GLOBAL_INTERVAL_GAP_SECONDS)
                    conflict_found = True
                    break

            if not conflict_found:
                return candidate.isoformat()

    def _compute_next_run_after_send(
        self,
        conn,
        *,
        rule_id: int,
        schedule_mode: str,
        fixed_times_json: str | None,
        interval_value: int,
        now_dt: datetime,
    ) -> tuple[str | None, str]:
        mode = (schedule_mode or "interval").strip().lower()
        if mode == "fixed":
            fixed_times = normalize_fixed_times(_safe_json_loads(fixed_times_json, []))
            next_run_iso = get_next_fixed_run_utc(fixed_times, now_dt)
            if next_run_iso is not None:
                return next_run_iso, "fixed"

            fallback_interval = int(interval_value or 0)
            fallback_base_dt = datetime.fromtimestamp(
                now_dt.timestamp() + max(fallback_interval, 1),
                tz=timezone.utc,
            )
            return self._find_next_interval_slot(conn, fallback_base_dt, exclude_rule_id=rule_id), "fixed_fallback_interval"

        actual_interval = int(interval_value or 0)
        base_dt = datetime.fromtimestamp(
            now_dt.timestamp() + max(actual_interval, 1),
            tz=timezone.utc,
        )
        return self._find_next_interval_slot(conn, base_dt, exclude_rule_id=rule_id), "interval"

    def _compute_next_run_for_rule_conn(self, conn, rule_id: int) -> str | None:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT schedule_mode, fixed_times_json, interval
                FROM routing
                WHERE id = %s
                LIMIT 1
                """,
                (rule_id,),
            )
            row = cur.fetchone()

        if not row:
            return None

        schedule_mode = row["schedule_mode"] or "interval"

        if schedule_mode == "fixed":
            fixed_times = _safe_json_loads(row.get("fixed_times_json"), [])
            fixed_times = normalize_fixed_times(fixed_times)
            if not fixed_times:
                return None
            return get_next_fixed_run_utc(fixed_times)

        interval = int(row.get("interval") or 0)
        base_dt = datetime.fromtimestamp(
            datetime.now(timezone.utc).timestamp() + max(interval, 1),
            tz=timezone.utc,
        )
        return self._find_next_interval_slot(conn, base_dt, exclude_rule_id=rule_id)

    def _fetch_inserted_id(self, conn, sql: str, params: tuple[Any, ...]) -> int | None:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            if not row:
                return None
            return int(row["id"])

    def _ensure_tenant_for_admin_conn(self, conn, admin_id: int) -> int:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id
                FROM tenants
                WHERE owner_admin_id = %s
                LIMIT 1
                """,
                (int(admin_id),),
            )
            row = cur.fetchone()
            if row:
                return int(row["id"])

            cur.execute(
                """
                INSERT INTO tenants(name, owner_admin_id, created_at, is_active)
                VALUES(%s, %s, %s, TRUE)
                RETURNING id
                """,
                (f"tenant-{int(admin_id)}", int(admin_id), utc_now_iso()),
            )
            created = cur.fetchone()
            tenant_id = int(created["id"]) if created else 1

            cur.execute(
                """
                INSERT INTO tenant_users(tenant_id, telegram_id, role, created_at)
                VALUES(%s, %s, 'owner', %s)
                ON CONFLICT(tenant_id, telegram_id) DO NOTHING
                """,
                (tenant_id, int(admin_id), utc_now_iso()),
            )
            cur.execute(
                """
                INSERT INTO subscriptions(tenant_id, plan_id, status, started_at, expires_at, created_at)
                SELECT %s, p.id, 'active', %s, NULL, %s
                FROM plans p
                WHERE p.name = 'FREE'
                  AND NOT EXISTS (
                    SELECT 1
                    FROM subscriptions s
                    WHERE s.tenant_id = %s
                      AND s.status IN ('active', 'trial')
                  )
                LIMIT 1
                """,
                (tenant_id, utc_now_iso(), utc_now_iso(), tenant_id),
            )
            return tenant_id

    # =========================================================
    # INIT / MIGRATIONS
    # =========================================================

    def init(self) -> None:
        self.ensure_configured()

        init_sql = """
        CREATE TABLE IF NOT EXISTS channels(
            id BIGSERIAL PRIMARY KEY,
            channel_id TEXT NOT NULL,
            thread_id BIGINT NULL,
            channel_type TEXT NOT NULL CHECK(channel_type IN ('source','target')),
            title TEXT NULL,
            added_by BIGINT NULL,
            added_date TEXT NOT NULL,
            is_active BOOLEAN NOT NULL DEFAULT TRUE
        );

        CREATE TABLE IF NOT EXISTS posts(
            id BIGSERIAL PRIMARY KEY,
            message_id BIGINT NOT NULL,
            source_channel TEXT NOT NULL,
            source_thread_id BIGINT NULL,
            content_json TEXT NOT NULL,
            media_group_id TEXT NULL,
            created_at TEXT NOT NULL,
            is_faulty BOOLEAN NOT NULL DEFAULT FALSE
        );

        CREATE TABLE IF NOT EXISTS routing(
            id BIGSERIAL PRIMARY KEY,
            source_id TEXT NOT NULL,
            source_thread_id BIGINT NULL,
            target_id TEXT NOT NULL,
            target_thread_id BIGINT NULL,
            interval BIGINT NOT NULL DEFAULT 3600,

            schedule_mode TEXT NOT NULL DEFAULT 'interval',
            fixed_times_json TEXT NULL,

            mode TEXT NOT NULL DEFAULT 'repost',

            video_trim_seconds BIGINT DEFAULT 120,
            video_add_intro BOOLEAN DEFAULT FALSE,
            video_intro_horizontal TEXT NULL,
            video_intro_vertical TEXT NULL,
            video_caption TEXT NULL,
            video_caption_entities_json TEXT NULL,
            caption_delivery_mode TEXT NOT NULL DEFAULT 'auto',
            video_caption_delivery_mode TEXT NOT NULL DEFAULT 'auto',

            created_by BIGINT NULL,
            created_date TEXT NOT NULL,
            is_active BOOLEAN NOT NULL DEFAULT TRUE,
            next_run_at TEXT NULL,
            last_sent_at TEXT NULL,

            video_intro_horizontal_id BIGINT NULL,
            video_intro_vertical_id BIGINT NULL
        );

        CREATE TABLE IF NOT EXISTS problem_state(
            id BIGSERIAL PRIMARY KEY,
            problem_key TEXT NOT NULL UNIQUE,
            problem_type TEXT NOT NULL,
            rule_id BIGINT NULL,
            delivery_id BIGINT NULL,
            first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            last_notified_at TEXT NULL,
            hit_count BIGINT NOT NULL DEFAULT 1,
            is_active BOOLEAN NOT NULL DEFAULT TRUE,
            is_muted BOOLEAN NOT NULL DEFAULT FALSE,
            extra_json TEXT NULL
        );

        CREATE TABLE IF NOT EXISTS runtime_heartbeat(
            role TEXT PRIMARY KEY,
            last_seen_at TIMESTAMP NOT NULL
        );

        CREATE TABLE IF NOT EXISTS intros(
            id BIGSERIAL PRIMARY KEY,
            display_name TEXT NOT NULL UNIQUE,
            file_name TEXT NOT NULL,
            file_path TEXT NOT NULL,
            duration BIGINT NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS deliveries(
            id BIGSERIAL PRIMARY KEY,
            rule_id BIGINT NOT NULL,
            post_id BIGINT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','processing','sent','faulty')),
            error_text TEXT NULL,
            attempt_count BIGINT NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            sent_at TEXT NULL
        );

        CREATE TABLE IF NOT EXISTS jobs(
            id BIGSERIAL PRIMARY KEY,
            job_type TEXT NOT NULL,
            payload_json JSONB NOT NULL,
            dedup_key TEXT NULL,
            status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','leased','processing','done','retry','failed')),
            priority INT NOT NULL DEFAULT 100,
            queue TEXT NOT NULL DEFAULT 'default',
            attempts INT NOT NULL DEFAULT 0,
            max_attempts INT NOT NULL DEFAULT 3,
            lease_until TIMESTAMPTZ NULL,
            locked_by TEXT NULL,
            run_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            error_text TEXT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS audit_log(
            id BIGSERIAL PRIMARY KEY,
            created_at TEXT NOT NULL,
            event_type TEXT NOT NULL,

            rule_id BIGINT NULL,
            delivery_id BIGINT NULL,
            post_id BIGINT NULL,

            admin_id BIGINT NULL,

            source_id TEXT NULL,
            source_thread_id BIGINT NULL,
            target_id TEXT NULL,
            target_thread_id BIGINT NULL,

            status TEXT NULL,
            error_text TEXT NULL,

            old_value_json TEXT NULL,
            new_value_json TEXT NULL,
            extra_json TEXT NULL
        );

        CREATE TABLE IF NOT EXISTS tenants(
            id BIGSERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            owner_admin_id BIGINT NOT NULL UNIQUE,
            created_at TEXT NOT NULL,
            is_active BOOLEAN NOT NULL DEFAULT TRUE
        );

        CREATE TABLE IF NOT EXISTS tenant_users(
            id BIGSERIAL PRIMARY KEY,
            tenant_id BIGINT NOT NULL,
            telegram_id BIGINT NOT NULL,
            role TEXT NOT NULL CHECK(role IN ('owner','admin','viewer')),
            created_at TEXT NOT NULL,
            UNIQUE(tenant_id, telegram_id)
        );

        CREATE TABLE IF NOT EXISTS plans(
            id BIGSERIAL PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            max_rules BIGINT NOT NULL,
            max_workers BIGINT NOT NULL,
            max_jobs_per_day BIGINT NOT NULL,
            max_video_per_day BIGINT NOT NULL,
            max_storage_mb BIGINT NOT NULL,
            priority_level BIGINT NOT NULL,
            price NUMERIC(10,2) NOT NULL,
            is_active BOOLEAN NOT NULL DEFAULT TRUE
        );

        CREATE TABLE IF NOT EXISTS subscriptions(
            id BIGSERIAL PRIMARY KEY,
            tenant_id BIGINT NOT NULL,
            plan_id BIGINT NOT NULL,
            status TEXT NOT NULL CHECK(status IN ('trial','active','grace','expired','canceled')),
            started_at TEXT NOT NULL,
            expires_at TEXT NULL,
            grace_started_at TEXT NULL,
            grace_ends_at TEXT NULL,
            canceled_at TEXT NULL,
            pending_plan_id BIGINT NULL,
            current_period_start TEXT NULL,
            current_period_end TEXT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NULL
        );

        CREATE TABLE IF NOT EXISTS usage_stats(
            tenant_id BIGINT NOT NULL,
            date TEXT NOT NULL,
            jobs_count BIGINT NOT NULL DEFAULT 0,
            video_count BIGINT NOT NULL DEFAULT 0,
            storage_used_mb BIGINT NOT NULL DEFAULT 0,
            api_calls BIGINT NOT NULL DEFAULT 0,
            PRIMARY KEY(tenant_id, date)
        );

        CREATE TABLE IF NOT EXISTS subscription_history(
            id BIGSERIAL PRIMARY KEY,
            tenant_id BIGINT NOT NULL,
            old_plan_id BIGINT NULL,
            new_plan_id BIGINT NULL,
            old_status TEXT NULL,
            new_status TEXT NULL,
            changed_at TEXT NOT NULL,
            changed_by TEXT NULL,
            reason TEXT NULL,
            effective_from TEXT NULL,
            effective_to TEXT NULL
        );

        CREATE TABLE IF NOT EXISTS billing_events(
            id BIGSERIAL PRIMARY KEY,
            tenant_id BIGINT NOT NULL,
            event_type TEXT NOT NULL,
            event_source TEXT NULL,
            amount NUMERIC(12,2) NULL,
            currency TEXT NULL,
            metadata_json TEXT NULL,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS invoices(
            id BIGSERIAL PRIMARY KEY,
            tenant_id BIGINT NOT NULL,
            subscription_id BIGINT NULL,
            period_start TEXT NOT NULL,
            period_end TEXT NOT NULL,
            status TEXT NOT NULL CHECK(status IN ('draft','open','paid','void','uncollectible')),
            subtotal NUMERIC(12,2) NOT NULL DEFAULT 0,
            total NUMERIC(12,2) NOT NULL DEFAULT 0,
            currency TEXT NOT NULL DEFAULT 'USD',
            external_provider TEXT NULL,
            external_reference TEXT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            due_at TEXT NULL,
            paid_at TEXT NULL
        );

        CREATE TABLE IF NOT EXISTS invoice_items(
            id BIGSERIAL PRIMARY KEY,
            invoice_id BIGINT NOT NULL,
            item_type TEXT NOT NULL CHECK(item_type IN ('base_plan','extra_jobs','extra_video','storage_overage','adjustment')),
            description TEXT NOT NULL,
            quantity BIGINT NOT NULL DEFAULT 1,
            unit_price NUMERIC(12,2) NOT NULL DEFAULT 0,
            amount NUMERIC(12,2) NOT NULL DEFAULT 0,
            metadata_json TEXT NULL
        );

        ALTER TABLE routing ADD COLUMN IF NOT EXISTS mode TEXT NOT NULL DEFAULT 'repost';
        ALTER TABLE routing ADD COLUMN IF NOT EXISTS video_trim_seconds BIGINT DEFAULT 120;
        ALTER TABLE routing ADD COLUMN IF NOT EXISTS video_add_intro BOOLEAN DEFAULT FALSE;
        ALTER TABLE routing ADD COLUMN IF NOT EXISTS video_intro_horizontal TEXT NULL;
        ALTER TABLE routing ADD COLUMN IF NOT EXISTS video_intro_vertical TEXT NULL;
        ALTER TABLE routing ADD COLUMN IF NOT EXISTS video_caption TEXT NULL;
        ALTER TABLE routing ADD COLUMN IF NOT EXISTS video_caption_entities_json TEXT NULL;
        ALTER TABLE routing ADD COLUMN IF NOT EXISTS caption_delivery_mode TEXT NOT NULL DEFAULT 'auto';
        ALTER TABLE routing ADD COLUMN IF NOT EXISTS video_caption_delivery_mode TEXT NOT NULL DEFAULT 'auto';
        ALTER TABLE routing ADD COLUMN IF NOT EXISTS schedule_mode TEXT NOT NULL DEFAULT 'interval';
        ALTER TABLE routing ADD COLUMN IF NOT EXISTS fixed_times_json TEXT NULL;
        ALTER TABLE routing ADD COLUMN IF NOT EXISTS video_intro_horizontal_id BIGINT NULL;
        ALTER TABLE routing ADD COLUMN IF NOT EXISTS video_intro_vertical_id BIGINT NULL;
        ALTER TABLE jobs ADD COLUMN IF NOT EXISTS dedup_key TEXT NULL;
        ALTER TABLE routing ADD COLUMN IF NOT EXISTS tenant_id BIGINT NULL;
        ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS tenant_id BIGINT NULL;
        ALTER TABLE audit_log ADD COLUMN IF NOT EXISTS tenant_id BIGINT NULL;
        ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS grace_started_at TEXT NULL;
        ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS grace_ends_at TEXT NULL;
        ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS canceled_at TEXT NULL;
        ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS pending_plan_id BIGINT NULL;
        ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS current_period_start TEXT NULL;
        ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS current_period_end TEXT NULL;
        ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS updated_at TEXT NULL;
        """

        self.client.execute_script(init_sql)

        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_channels_unique
                    ON channels(channel_id, COALESCE(thread_id, -1), channel_type)
                    """
                )
                cur.execute(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_posts_unique_no_thread
                    ON posts(message_id, source_channel)
                    WHERE source_thread_id IS NULL
                    """
                )
                cur.execute(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_posts_unique_with_thread
                    ON posts(message_id, source_channel, source_thread_id)
                    WHERE source_thread_id IS NOT NULL
                    """
                )
                cur.execute(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_routing_unique
                    ON routing(source_id, COALESCE(source_thread_id, -1), target_id, COALESCE(target_thread_id, -1))
                    """
                )
                cur.execute(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_deliveries_unique
                    ON deliveries(rule_id, post_id)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_problem_state_active
                    ON problem_state(is_active, is_muted, last_seen_at)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_problem_state_rule
                    ON problem_state(rule_id, is_active, is_muted)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_intros_created_at
                    ON intros(created_at)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_audit_log_created_at
                    ON audit_log(created_at)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_audit_log_rule_id
                    ON audit_log(rule_id, created_at)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_audit_log_delivery_id
                    ON audit_log(delivery_id, created_at)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_audit_log_event_type
                    ON audit_log(event_type, created_at)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_deliveries_rule_status
                    ON deliveries(rule_id, status, post_id)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_posts_source_lookup
                    ON posts(source_channel, source_thread_id, id)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_posts_album_lookup
                    ON posts(source_channel, media_group_id, message_id)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_routing_source_lookup
                    ON routing(source_id, source_thread_id, is_active)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_routing_next_run
                    ON routing(is_active, next_run_at)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_deliveries_post_id
                    ON deliveries(post_id)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_jobs_status_run_at
                    ON jobs(status, run_at)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_subscription_history_tenant_changed
                    ON subscription_history(tenant_id, changed_at)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_billing_events_tenant_created
                    ON billing_events(tenant_id, created_at)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_invoices_tenant_status
                    ON invoices(tenant_id, status, created_at)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_invoice_items_invoice_id
                    ON invoice_items(invoice_id)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_jobs_queue_status_run_at
                    ON jobs(queue, status, run_at)
                    """
                )
                cur.execute(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_active_dedup_key_unique
                    ON jobs(dedup_key)
                    WHERE dedup_key IS NOT NULL
                      AND status IN ('pending', 'leased', 'processing', 'retry')
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_routing_tenant_id
                    ON routing(tenant_id, id)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_deliveries_tenant_id
                    ON deliveries(tenant_id, id)
                    """
                )
            conn.commit()

        self._ensure_default_plans()

    def _ensure_default_plans(self) -> None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                for row in [
                    ("FREE", 3, 1, 100, 10, 512, 1, "0.00"),
                    ("BASIC", 15, 2, 1000, 100, 5120, 2, "19.99"),
                    ("PRO", 100, 4, 10000, 1000, 51200, 3, "99.99"),
                ]:
                    cur.execute(
                        """
                        INSERT INTO plans(
                            name,
                            max_rules,
                            max_workers,
                            max_jobs_per_day,
                            max_video_per_day,
                            max_storage_mb,
                            priority_level,
                            price,
                            is_active
                        )
                        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, TRUE)
                        ON CONFLICT(name) DO NOTHING
                        """,
                        row,
                    )
            conn.commit()

    # =========================================================
    # SERVICE / MAINTENANCE
    # =========================================================

    def integrity_check(self) -> tuple[bool, str]:
        try:
            ok = self.client.ping()
            return (True, "ok") if ok else (False, "ping failed")
        except Exception as exc:
            return False, str(exc)

    def backup_database(self, backup_path: str) -> bool:
        logger.warning("backup_database для PostgreSQL не реализован через репозиторий. backup_path=%s", backup_path)
        return False

    def optimize_database(self) -> None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("ANALYZE")
            conn.commit()

    def reset_stuck_processing(self) -> int:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE deliveries
                    SET status = 'pending'
                    WHERE status = 'processing'
                    """
                )
                count = cur.rowcount
            conn.commit()
            return int(count or 0)

    # =========================================================
    # CHANNELS
    # =========================================================

    def add_channel(
        self,
        channel_id: str,
        thread_id: int | None,
        channel_type: str,
        title: str,
        added_by: int,
    ) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO channels(channel_id, thread_id, channel_type, title, added_by, added_date, is_active)
                    VALUES(%s, %s, %s, %s, %s, %s, TRUE)
                    ON CONFLICT DO NOTHING
                    """,
                    (str(channel_id), thread_id, channel_type, title, added_by, utc_now_iso()),
                )
                created = cur.rowcount > 0
            conn.commit()
            return created

    def remove_channel(
        self,
        channel_id: str,
        thread_id: int | None,
        channel_type: str | None = None,
    ) -> bool:
        channel_id = str(channel_id)

        with self.connect() as conn:
            with conn.cursor() as cur:
                # 1. Удаляем запись из channels
                params: list[Any] = [channel_id]
                q = "DELETE FROM channels WHERE channel_id = %s"

                if thread_id is None:
                    q += " AND thread_id IS NULL"
                else:
                    q += " AND thread_id = %s"
                    params.append(thread_id)

                if channel_type:
                    q += " AND channel_type = %s"
                    params.append(channel_type)

                cur.execute(q, tuple(params))
                removed = cur.rowcount > 0

                # 2. Если удаляли source — чистим posts этого источника
                if channel_type in (None, "source"):
                    if thread_id is None:
                        cur.execute(
                            """
                            DELETE FROM posts
                            WHERE source_channel = %s
                            AND source_thread_id IS NULL
                            """,
                            (channel_id,),
                        )
                    else:
                        cur.execute(
                            """
                            DELETE FROM posts
                            WHERE source_channel = %s
                            AND source_thread_id = %s
                            """,
                            (channel_id, thread_id),
                        )

                    # 3. Чистим deliveries без posts
                    cur.execute(
                        """
                        DELETE FROM deliveries d
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM posts p
                            WHERE p.id = d.post_id
                        )
                        """
                    )

                # 4. Чистим routing
                if thread_id is None:
                    cur.execute(
                        """
                        DELETE FROM routing
                        WHERE (source_id = %s AND source_thread_id IS NULL)
                        OR (target_id = %s AND target_thread_id IS NULL)
                        """,
                        (channel_id, channel_id),
                    )
                else:
                    cur.execute(
                        """
                        DELETE FROM routing
                        WHERE (source_id = %s AND source_thread_id = %s)
                        OR (target_id = %s AND target_thread_id = %s)
                        """,
                        (channel_id, thread_id, channel_id, thread_id),
                    )

                # 5. Чистим deliveries без routing
                cur.execute(
                    """
                    DELETE FROM deliveries d
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM routing r
                        WHERE r.id = d.rule_id
                    )
                    """
                )

            conn.commit()
            return removed

    def channel_exists(
        self,
        channel_id: str,
        thread_id: int | None,
        channel_type: str | None = None,
    ) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                params: list[Any] = [str(channel_id)]
                q = "SELECT 1 FROM channels WHERE channel_id = %s"

                if thread_id is None:
                    q += " AND thread_id IS NULL"
                else:
                    q += " AND thread_id = %s"
                    params.append(thread_id)

                if channel_type:
                    q += " AND channel_type = %s"
                    params.append(channel_type)

                q += " LIMIT 1"
                cur.execute(q, tuple(params))
                return cur.fetchone() is not None

    def get_channels(self, channel_type: str | None = None):
        with self.connect() as conn:
            with conn.cursor() as cur:
                if channel_type:
                    cur.execute(
                        """
                        SELECT channel_id, thread_id, title, channel_type
                        FROM channels
                        WHERE channel_type = %s
                          AND is_active = TRUE
                        ORDER BY added_date
                        """,
                        (channel_type,),
                    )
                else:
                    cur.execute(
                        """
                        SELECT channel_id, thread_id, title, channel_type
                        FROM channels
                        WHERE is_active = TRUE
                        ORDER BY channel_type, added_date
                        """
                    )
                return cur.fetchall()

    def get_post(self, source_channel: str, source_thread_id: int | None, message_id: int):
        with self.connect() as conn:
            with conn.cursor() as cur:
                if source_thread_id is None:
                    cur.execute(
                        """
                        SELECT *
                        FROM posts
                        WHERE source_channel = %s
                        AND source_thread_id IS NULL
                        AND message_id = %s
                        LIMIT 1
                        """,
                        (str(source_channel), int(message_id)),
                    )
                else:
                    cur.execute(
                        """
                        SELECT *
                        FROM posts
                        WHERE source_channel = %s
                        AND source_thread_id = %s
                        AND message_id = %s
                        LIMIT 1
                        """,
                        (str(source_channel), int(source_thread_id), int(message_id)),
                    )
                return cur.fetchone()

    def update_rule_caption_delivery_mode(
        self,
        rule_id: int,
        caption_delivery_mode: str,
    ) -> bool:
        allowed = {"copy_first", "builder_first", "auto"}
        normalized = (caption_delivery_mode or "").strip().lower()

        if normalized not in allowed:
            return False

        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE routing
                    SET caption_delivery_mode = %s
                    WHERE id = %s
                    """,
                    (normalized, rule_id),
                )
                updated = cur.rowcount > 0
            conn.commit()
            return updated

    def rollback_last_delivery(self, rule_id: int, admin_id: int | None = None):
        """
        Откат последнего УЖЕ ОТПРАВЛЕННОГО ЛОГИЧЕСКОГО элемента правила.

        ВАЖНО:
        - работает не по одной delivery-строке, а по логическому элементу
        - использует тот же builder, что queue / position / start-from-position
        - возвращает весь logical item обратно в pending
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
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT
                            d.id AS delivery_id,
                            d.status,
                            d.sent_at,
                            d.error_text,
                            d.attempt_count,
                            p.id AS post_id,
                            p.message_id,
                            p.source_channel,
                            p.source_thread_id,
                            p.media_group_id,
                            p.content_json,
                            p.created_at
                        FROM deliveries d
                        JOIN posts p ON p.id = d.post_id
                        WHERE d.rule_id = %s
                        ORDER BY p.id ASC
                        """,
                        (rule_id,),
                    )
                    rows = cur.fetchall()

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

                    cur.execute(
                        """
                        SELECT
                            id,
                            post_id,
                            status,
                            sent_at,
                            error_text,
                            attempt_count
                        FROM deliveries
                        WHERE id = ANY(%s)
                        ORDER BY id ASC
                        """,
                        (delivery_ids,),
                    )
                    current_rows = cur.fetchall()

                    if not current_rows:
                        logger.warning(
                            "rollback_last_delivery: не удалось перечитать deliveries для отката, rule_id=%s, delivery_ids=%s",
                            rule_id,
                            delivery_ids,
                        )
                        return None

                    old_value = []
                    for row in current_rows:
                        old_value.append(
                            {
                                "delivery_id": int(row["id"]),
                                "post_id": int(row["post_id"]),
                                "status": row["status"],
                                "sent_at": row["sent_at"],
                                "error_text": row["error_text"],
                                "attempt_count": row["attempt_count"],
                            }
                        )

                    cur.executemany(
                        """
                        UPDATE deliveries
                        SET status = 'pending',
                            sent_at = NULL,
                            error_text = NULL,
                            attempt_count = 0
                        WHERE id = %s
                        """,
                        [(delivery_id,) for delivery_id in delivery_ids],
                    )

                    cur.execute(
                        """
                        UPDATE routing
                        SET next_run_at = %s
                        WHERE id = %s
                        """,
                        (now_iso, rule_id),
                    )

                    cur.execute(
                        """
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
                        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            now_iso,
                            "delivery_rolled_back",
                            rule_id,
                            delivery_ids[0] if delivery_ids else None,
                            selected["first_post_id"],
                            admin_id,
                            "pending",
                            _json_dumps(old_value),
                            _json_dumps(
                                {
                                    "status": "pending",
                                    "sent_at": None,
                                    "error_text": None,
                                    "attempt_count": 0,
                                    "next_run_at": now_iso,
                                }
                            ),
                            _json_dumps(
                                {
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
                                }
                            ),
                        ),
                    )

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

    # =========================================================
    # INTROS
    # =========================================================

    def add_intro(
        self,
        display_name: str,
        file_name: str,
        file_path: str,
        duration: int,
    ) -> int | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO intros(display_name, file_name, file_path, duration, created_at)
                    VALUES(%s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                    RETURNING id
                    """,
                    (
                        display_name.strip(),
                        file_name,
                        file_path,
                        int(duration or 0),
                        utc_now_iso(),
                    ),
                )
                row = cur.fetchone()
            conn.commit()
            return int(row["id"]) if row else None

    def get_intros(self) -> list[IntroItem]:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, display_name, file_name, file_path, duration, created_at
                    FROM intros
                    ORDER BY created_at DESC, id DESC
                    """
                )
                rows = cur.fetchall()

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

    def get_intro(self, intro_id: int):
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, display_name, file_name, file_path, duration, created_at
                    FROM intros
                    WHERE id = %s
                    LIMIT 1
                    """,
                    (intro_id,),
                )
                row = cur.fetchone()

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

    def get_intro_by_id(self, intro_id: int):
        return self.get_intro(intro_id)

    def delete_intro(self, intro_id: int) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM intros WHERE id = %s", (intro_id,))
                deleted = cur.rowcount > 0
            conn.commit()
            return deleted

    # =========================================================
    # POSTS / QUEUE RAW
    # =========================================================

    def _create_deliveries_for_post_conn(
        self,
        conn,
        post_id: int,
        source_channel: str,
        source_thread_id: int | None,
    ) -> None:
        with conn.cursor() as cur:
            if source_thread_id is None:
                cur.execute(
                    """
                    SELECT id
                    FROM routing
                    WHERE source_id = %s
                      AND source_thread_id IS NULL
                    """,
                    (str(source_channel),),
                )
            else:
                cur.execute(
                    """
                    SELECT id
                    FROM routing
                    WHERE source_id = %s
                      AND source_thread_id = %s
                    """,
                    (str(source_channel), source_thread_id),
                )
            rules = cur.fetchall()

            now_iso = utc_now_iso()
            for rule in rules:
                cur.execute(
                    """
                    INSERT INTO deliveries(rule_id, post_id, status, created_at, tenant_id)
                    VALUES(%s, %s, 'pending', %s, (SELECT tenant_id FROM routing WHERE id = %s))
                    ON CONFLICT DO NOTHING
                    """,
                    (int(rule["id"]), int(post_id), now_iso, int(rule["id"])),
                )

    def _backfill_deliveries_for_rule_conn(
        self,
        conn,
        rule_id: int,
        source_id: str,
        source_thread_id: int | None,
    ) -> int:
        with conn.cursor() as cur:
            if source_thread_id is None:
                cur.execute(
                    """
                    SELECT id
                    FROM posts
                    WHERE source_channel = %s
                      AND source_thread_id IS NULL
                    ORDER BY id
                    """,
                    (str(source_id),),
                )
            else:
                cur.execute(
                    """
                    SELECT id
                    FROM posts
                    WHERE source_channel = %s
                      AND source_thread_id = %s
                    ORDER BY id
                    """,
                    (str(source_id), source_thread_id),
                )
            rows = cur.fetchall()

            inserted = 0
            now_iso = utc_now_iso()
            for row in rows:
                cur.execute(
                    """
                    INSERT INTO deliveries(rule_id, post_id, status, created_at, tenant_id)
                    VALUES(%s, %s, 'pending', %s, (SELECT tenant_id FROM routing WHERE id = %s))
                    ON CONFLICT DO NOTHING
                    """,
                    (int(rule_id), int(row["id"]), now_iso, int(rule_id)),
                )
                inserted += int(cur.rowcount or 0)

        return inserted

    def save_post(
        self,
        message_id: int,
        source_channel: str,
        source_thread_id: int | None,
        content: dict[str, Any],
        media_group_id: str | None = None,
    ) -> int:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO posts(
                        message_id,
                        source_channel,
                        source_thread_id,
                        content_json,
                        media_group_id,
                        created_at,
                        is_faulty
                    )
                    VALUES(%s, %s, %s, %s, %s, %s, FALSE)
                    ON CONFLICT DO NOTHING
                    """,
                    (
                        int(message_id),
                        str(source_channel),
                        source_thread_id,
                        _json_dumps(content) or "{}",
                        media_group_id,
                        utc_now_iso(),
                    ),
                )
                inserted = cur.rowcount > 0

                if source_thread_id is None:
                    cur.execute(
                        """
                        SELECT id
                        FROM posts
                        WHERE message_id = %s
                          AND source_channel = %s
                          AND source_thread_id IS NULL
                        LIMIT 1
                        """,
                        (int(message_id), str(source_channel)),
                    )
                else:
                    cur.execute(
                        """
                        SELECT id
                        FROM posts
                        WHERE message_id = %s
                          AND source_channel = %s
                          AND source_thread_id = %s
                        LIMIT 1
                        """,
                        (int(message_id), str(source_channel), source_thread_id),
                    )
                row = cur.fetchone()

                if not row:
                    raise RuntimeError("Не удалось сохранить или найти post в PostgreSQL")

                post_id = int(row["id"])
                self._create_deliveries_for_post_conn(conn, post_id, str(source_channel), source_thread_id)

            conn.commit()
            return post_id

    def save_post_batch(self, posts_data):
        count = 0
        with self.connect() as conn:
            with conn.cursor() as cur:
                for message_id, source_channel, source_thread_id, content, media_group_id in posts_data:
                    cur.execute(
                        """
                        INSERT INTO posts(
                            message_id,
                            source_channel,
                            source_thread_id,
                            content_json,
                            media_group_id,
                            created_at,
                            is_faulty
                        )
                        VALUES(%s, %s, %s, %s, %s, %s, FALSE)
                        ON CONFLICT DO NOTHING
                        """,
                        (
                            int(message_id),
                            str(source_channel),
                            source_thread_id,
                            _json_dumps(content) or "{}",
                            media_group_id,
                            utc_now_iso(),
                        ),
                    )
                    inserted = cur.rowcount > 0

                    if source_thread_id is None:
                        cur.execute(
                            """
                            SELECT id
                            FROM posts
                            WHERE message_id = %s
                              AND source_channel = %s
                              AND source_thread_id IS NULL
                            LIMIT 1
                            """,
                            (int(message_id), str(source_channel)),
                        )
                    else:
                        cur.execute(
                            """
                            SELECT id
                            FROM posts
                            WHERE message_id = %s
                              AND source_channel = %s
                              AND source_thread_id = %s
                            LIMIT 1
                            """,
                            (int(message_id), str(source_channel), source_thread_id),
                        )
                    row = cur.fetchone()
                    if not row:
                        continue

                    post_id = int(row["id"])
                    if inserted:
                        count += 1
                    self._create_deliveries_for_post_conn(conn, post_id, str(source_channel), source_thread_id)

            conn.commit()
            return count

    def delete_channel_posts(self, channel_id: str, thread_id: int | None = None) -> int:
        with self.connect() as conn:
            with conn.cursor() as cur:
                if thread_id is None:
                    cur.execute(
                        """
                        DELETE FROM posts
                        WHERE source_channel = %s
                          AND source_thread_id IS NULL
                        """,
                        (str(channel_id),),
                    )
                else:
                    cur.execute(
                        """
                        DELETE FROM posts
                        WHERE source_channel = %s
                          AND source_thread_id = %s
                        """,
                        (str(channel_id), thread_id),
                    )
                deleted = int(cur.rowcount or 0)

                # Чистим orphan deliveries вручную, чтобы поведение было стабильным
                cur.execute(
                    """
                    DELETE FROM deliveries d
                    WHERE NOT EXISTS (
                        SELECT 1 FROM posts p WHERE p.id = d.post_id
                    )
                    """
                )
            conn.commit()
            return deleted

    def get_post_id_by_delivery(self, delivery_id: int) -> int | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT post_id
                    FROM deliveries
                    WHERE id = %s
                    LIMIT 1
                    """,
                    (int(delivery_id),),
                )
                row = cur.fetchone()
                if not row:
                    return None
                return int(row["post_id"])

    def is_album_already_sent(
        self,
        rule_id: int,
        source_channel: str,
        source_thread_id: int | None,
        media_group_id: str,
    ) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                if source_thread_id is None:
                    cur.execute(
                        """
                        SELECT 1
                        FROM deliveries d
                        JOIN posts p ON p.id = d.post_id
                        WHERE d.rule_id = %s
                          AND d.status = 'sent'
                          AND p.source_channel = %s
                          AND p.source_thread_id IS NULL
                          AND p.media_group_id = %s
                        LIMIT 1
                        """,
                        (rule_id, str(source_channel), media_group_id),
                    )
                else:
                    cur.execute(
                        """
                        SELECT 1
                        FROM deliveries d
                        JOIN posts p ON p.id = d.post_id
                        WHERE d.rule_id = %s
                          AND d.status = 'sent'
                          AND p.source_channel = %s
                          AND p.source_thread_id = %s
                          AND p.media_group_id = %s
                        LIMIT 1
                        """,
                        (rule_id, str(source_channel), source_thread_id, media_group_id),
                    )
                return cur.fetchone() is not None

    # =========================================================
    # RULES
    # =========================================================

    def add_rule(
        self,
        source_id: str,
        source_thread_id: int | None,
        target_id: str,
        target_thread_id: int | None,
        interval: int,
        created_by: int,
    ) -> int | None:
        with self.connect() as conn:
            tenant_id = self._ensure_tenant_for_admin_conn(conn, int(created_by))
            base_dt = datetime.fromtimestamp(
                datetime.now(timezone.utc).timestamp() + max(int(interval), 1),
                tz=timezone.utc,
            )
            next_run_iso = self._find_next_interval_slot(conn, base_dt)

            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO routing(
                        source_id,
                        source_thread_id,
                        target_id,
                        target_thread_id,
                        interval,
                        created_by,
                        created_date,
                        is_active,
                        next_run_at,
                        last_sent_at,
                        schedule_mode,
                        fixed_times_json,
                        mode,
                        video_trim_seconds,
                        video_add_intro,
                        video_intro_horizontal,
                        video_intro_vertical,
                        video_caption,
                        video_caption_entities_json,
                        video_intro_horizontal_id,
                        video_intro_vertical_id,
                        tenant_id
                    )
                    VALUES(
                        %s, %s, %s, %s,
                        %s, %s, %s, FALSE, %s, NULL,
                        'interval', NULL, 'repost',
                        120, FALSE, NULL, NULL, NULL, NULL, NULL, NULL, %s
                    )
                    ON CONFLICT DO NOTHING
                    RETURNING id
                    """,
                    (
                        str(source_id),
                        source_thread_id,
                        str(target_id),
                        target_thread_id,
                        int(interval),
                        created_by,
                        utc_now_iso(),
                        next_run_iso,
                        tenant_id,
                    ),
                )
                row = cur.fetchone()
            conn.commit()
            return int(row["id"]) if row else None

    def remove_rule(self, rule_id: int) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM routing WHERE id = %s", (rule_id,))
                deleted = cur.rowcount > 0
                cur.execute("DELETE FROM deliveries WHERE rule_id = %s", (rule_id,))
            conn.commit()
            return deleted

    def delete_rule_with_audit(self, rule_id: int, admin_id: int) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM routing
                    WHERE id = %s
                    LIMIT 1
                    """,
                    (rule_id,),
                )
                row = cur.fetchone()

                if not row:
                    conn.commit()
                    return False

                fixed_times = _safe_json_loads(row.get("fixed_times_json"), [])

                cur.execute(
                    """
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
                    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        utc_now_iso(),
                        "rule_deleted",
                        rule_id,
                        admin_id,
                        row["source_id"],
                        row["source_thread_id"],
                        row["target_id"],
                        row["target_thread_id"],
                        _json_dumps(
                            {
                                "is_active": bool(row["is_active"]),
                                "interval": row["interval"],
                                "schedule_mode": row["schedule_mode"],
                                "fixed_times": fixed_times,
                                "next_run_at": row["next_run_at"],
                            }
                        ),
                    ),
                )

                cur.execute("DELETE FROM routing WHERE id = %s", (rule_id,))
                deleted = cur.rowcount > 0
                cur.execute("DELETE FROM deliveries WHERE rule_id = %s", (rule_id,))
            conn.commit()
            return deleted

    def get_all_rules(self):
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        r.*,
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
                    ORDER BY r.created_date, r.id
                    """
                )
                rows = cur.fetchall()

        return [self._row_to_rule(row) for row in rows]

    def get_rule(self, rule_id: int):
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        r.*,
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
                    WHERE r.id = %s
                    LIMIT 1
                    """,
                    (rule_id,),
                )
                row = cur.fetchone()

        if not row:
            return None

        rule = self._row_to_rule(row)

        if rule.schedule_mode == "fixed" and rule.is_active and not rule.next_run_at:
            with self.connect() as conn:
                next_run_iso = self._compute_next_run_for_rule_conn(conn, rule.id)
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE routing
                        SET next_run_at = %s
                        WHERE id = %s
                        """,
                        (next_run_iso, rule.id),
                    )
                conn.commit()

            row = dict(row)
            row["next_run_at"] = next_run_iso
            rule = self._row_to_rule(row)

        return rule

    def set_rule_active(self, rule_id: int, is_active: bool) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE routing SET is_active = %s WHERE id = %s",
                    (bool(is_active), rule_id),
                )
                updated = cur.rowcount > 0

                if updated and bool(is_active):
                    next_run_iso = self._compute_next_run_for_rule_conn(conn, rule_id)
                    cur.execute(
                        """
                        UPDATE routing
                        SET next_run_at = %s
                        WHERE id = %s
                        """,
                        (next_run_iso, rule_id),
                    )

            conn.commit()
            return updated

    def activate_rule_with_backfill(self, rule_id: int) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, source_id, source_thread_id
                    FROM routing
                    WHERE id = %s
                    LIMIT 1
                    """,
                    (rule_id,),
                )
                row = cur.fetchone()

                if not row:
                    conn.commit()
                    return False

                cur.execute(
                    """
                    UPDATE routing
                    SET is_active = TRUE
                    WHERE id = %s
                    """,
                    (rule_id,),
                )

                if cur.rowcount == 0:
                    conn.commit()
                    return False

            self._backfill_deliveries_for_rule_conn(
                conn,
                int(row["id"]),
                str(row["source_id"]),
                row["source_thread_id"],
            )

            next_run_iso = self._compute_next_run_for_rule_conn(conn, rule_id)

            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE routing
                    SET next_run_at = %s
                    WHERE id = %s
                    """,
                    (next_run_iso, rule_id),
                )

            conn.commit()
            return True

    def get_rule_source_scope(self, rule_id: int) -> tuple[str, int | None] | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT source_id, source_thread_id
                    FROM routing
                    WHERE id = %s
                    LIMIT 1
                    """,
                    (rule_id,),
                )
                row = cur.fetchone()

        if not row:
            return None

        return str(row["source_id"]), row["source_thread_id"]

    def clear_rule_deliveries(self, rule_id: int) -> int:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM deliveries WHERE rule_id = %s", (rule_id,))
                count = int(cur.rowcount or 0)
            conn.commit()
            return count

    def backfill_rule(self, rule_id: int) -> int:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, source_id, source_thread_id
                    FROM routing
                    WHERE id = %s
                    LIMIT 1
                    """,
                    (rule_id,),
                )
                row = cur.fetchone()

                if not row:
                    conn.commit()
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
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT p.message_id
                    FROM deliveries d
                    JOIN posts p ON p.id = d.post_id
                    WHERE d.rule_id = %s
                      AND d.status = 'pending'
                    ORDER BY p.id ASC
                    LIMIT 1
                    """,
                    (rule_id,),
                )
                row = cur.fetchone()

        if not row:
            return None
        return int(row["message_id"])

    def get_rule_sent_message_ids(self, rule_id: int) -> list[int]:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT p.message_id
                    FROM deliveries d
                    JOIN posts p ON p.id = d.post_id
                    WHERE d.rule_id = %s
                      AND d.status = 'sent'
                    ORDER BY p.id ASC
                    """,
                    (rule_id,),
                )
                rows = cur.fetchall()

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
            with conn.cursor() as cur:
                if source_thread_id is None:
                    cur.execute(
                        """
                        UPDATE deliveries d
                        SET status = 'sent',
                            sent_at = %s,
                            error_text = NULL
                        FROM posts p
                        WHERE d.post_id = p.id
                          AND d.rule_id = %s
                          AND p.source_channel = %s
                          AND p.source_thread_id IS NULL
                          AND p.message_id = ANY(%s)
                        """,
                        (utc_now_iso(), rule_id, str(source_channel), message_ids),
                    )
                else:
                    cur.execute(
                        """
                        UPDATE deliveries d
                        SET status = 'sent',
                            sent_at = %s,
                            error_text = NULL
                        FROM posts p
                        WHERE d.post_id = p.id
                          AND d.rule_id = %s
                          AND p.source_channel = %s
                          AND p.source_thread_id = %s
                          AND p.message_id = ANY(%s)
                        """,
                        (utc_now_iso(), rule_id, str(source_channel), source_thread_id, message_ids),
                    )
                count = int(cur.rowcount or 0)
            conn.commit()
            return count

    def drop_rule_pending_before_message(
        self,
        rule_id: int,
        source_channel: str,
        source_thread_id: int | None,
        message_id: int,
    ) -> int:
        with self.connect() as conn:
            with conn.cursor() as cur:
                if source_thread_id is None:
                    cur.execute(
                        """
                        DELETE FROM deliveries d
                        USING posts p
                        WHERE d.post_id = p.id
                          AND d.rule_id = %s
                          AND d.status = 'pending'
                          AND p.source_channel = %s
                          AND p.source_thread_id IS NULL
                          AND p.message_id < %s
                        """,
                        (rule_id, str(source_channel), int(message_id)),
                    )
                else:
                    cur.execute(
                        """
                        DELETE FROM deliveries d
                        USING posts p
                        WHERE d.post_id = p.id
                          AND d.rule_id = %s
                          AND d.status = 'pending'
                          AND p.source_channel = %s
                          AND p.source_thread_id = %s
                          AND p.message_id < %s
                        """,
                        (rule_id, str(source_channel), source_thread_id, int(message_id)),
                    )
                count = int(cur.rowcount or 0)
            conn.commit()
            return count

    def get_rule_next_run_at(self, rule_id: int) -> str | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT next_run_at
                    FROM routing
                    WHERE id = %s
                    LIMIT 1
                    """,
                    (rule_id,),
                )
                row = cur.fetchone()

        if not row:
            return None
        return row["next_run_at"]

    def get_rule_sent_count(self, rule_id: int) -> int:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*) AS cnt
                    FROM deliveries
                    WHERE rule_id = %s
                      AND status = 'sent'
                    """,
                    (rule_id,),
                )
                row = cur.fetchone()
        return int(row["cnt"] or 0) if row else 0

    def drop_rule_first_n_deliveries(self, rule_id: int, count: int) -> int:
        if count <= 0:
            return 0

        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT d.id
                    FROM deliveries d
                    JOIN posts p ON p.id = d.post_id
                    WHERE d.rule_id = %s
                    ORDER BY p.id ASC
                    LIMIT %s
                    """,
                    (rule_id, int(count)),
                )
                rows = cur.fetchall()

                ids = [int(row["id"]) for row in rows]
                if not ids:
                    conn.commit()
                    return 0

                cur.execute(
                    """
                    DELETE FROM deliveries
                    WHERE id = ANY(%s)
                    """,
                    (ids,),
                )
                deleted = int(cur.rowcount or 0)
            conn.commit()
            return deleted

    def update_rule_interval(self, rule_id: int, new_interval: int) -> bool:
        with self.connect() as conn:
            base_dt = datetime.fromtimestamp(
                datetime.now(timezone.utc).timestamp() + max(int(new_interval), 1),
                tz=timezone.utc,
            )
            next_run_iso = self._find_next_interval_slot(
                conn,
                base_dt,
                exclude_rule_id=rule_id,
            )

            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE routing
                    SET schedule_mode = 'interval',
                        interval = %s,
                        next_run_at = %s
                    WHERE id = %s
                    """,
                    (int(new_interval), next_run_iso, rule_id),
                )
                updated = cur.rowcount > 0

            conn.commit()

            logger.info(
                "update_rule_interval: rule_id=%s, new_interval=%s, next_run_at=%s, updated=%s",
                rule_id,
                new_interval,
                next_run_iso,
                updated,
            )
            return updated

    def update_rule_next_run_at(self, rule_id: int, next_run_iso: str) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE routing
                    SET next_run_at = %s
                    WHERE id = %s
                    """,
                    (next_run_iso, rule_id),
                )
                updated = cur.rowcount > 0
            conn.commit()
            return updated

    def trigger_rule_now(self, rule_id: int) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE routing
                    SET next_run_at = %s
                    WHERE id = %s AND is_active = TRUE
                    """,
                    (utc_now_iso(), rule_id),
                )
                updated = cur.rowcount > 0
            conn.commit()
            return updated

    def update_rule_fixed_times(self, rule_id: int, times: list[str]) -> bool:
        normalized = normalize_fixed_times(times)
        if not normalized:
            return False

        next_run = get_next_fixed_run_utc(normalized)

        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE routing
                    SET schedule_mode = 'fixed',
                        fixed_times_json = %s,
                        next_run_at = %s
                    WHERE id = %s
                    """,
                    (_json_dumps(normalized), next_run, rule_id),
                )
                updated = cur.rowcount > 0

                if updated:
                    cur.execute(
                        """
                        UPDATE routing
                        SET next_run_at = %s
                        WHERE id = %s
                        AND is_active = TRUE
                    """,
                        (next_run, rule_id),
                    )

            conn.commit()
            return updated

    def set_rule_interval_mode(self, rule_id: int, interval: int) -> bool:
        with self.connect() as conn:
            base_dt = datetime.fromtimestamp(
                datetime.now(timezone.utc).timestamp() + max(interval, 1),
                tz=timezone.utc,
            )
            next_run_iso = self._find_next_interval_slot(conn, base_dt, exclude_rule_id=rule_id)

            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE routing
                    SET schedule_mode = 'interval',
                        interval = %s,
                        fixed_times_json = NULL,
                        next_run_at = %s
                    WHERE id = %s
                    """,
                    (interval, next_run_iso, rule_id),
                )
                updated = cur.rowcount > 0
            conn.commit()
            return updated

    def update_rule_mode(self, rule_id: int, mode: str) -> bool:
        if mode not in ("repost", "video"):
            return False

        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE routing
                    SET mode = %s
                    WHERE id = %s
                    """,
                    (mode, rule_id),
                )
                updated = cur.rowcount > 0
            conn.commit()
            return updated

    def update_rule_video_caption(
        self,
        rule_id: int,
        caption: str | None,
        caption_entities_json: str | None = None,
    ) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE routing
                    SET video_caption = %s,
                        video_caption_entities_json = %s
                    WHERE id = %s
                    """,
                    (caption, caption_entities_json, rule_id),
                )
                updated = cur.rowcount > 0
            conn.commit()
            return updated

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
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE routing
                    SET video_caption_delivery_mode = %s
                    WHERE id = %s
                    """,
                    (normalized, rule_id),
                )
                updated = cur.rowcount > 0

            conn.commit()

        return updated

    def update_rule_video_intro_horizontal(self, rule_id: int, path: str | None) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE routing
                    SET video_intro_horizontal = %s
                    WHERE id = %s
                    """,
                    (path, rule_id),
                )
                updated = cur.rowcount > 0
            conn.commit()
            return updated

    def update_rule_video_intro_vertical(self, rule_id: int, path: str | None) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE routing
                    SET video_intro_vertical = %s
                    WHERE id = %s
                    """,
                    (path, rule_id),
                )
                updated = cur.rowcount > 0
            conn.commit()
            return updated

    def set_rule_video_intro_enabled(self, rule_id: int, enabled: bool) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE routing
                    SET video_add_intro = %s
                    WHERE id = %s
                    """,
                    (bool(enabled), rule_id),
                )
                updated = cur.rowcount > 0
            conn.commit()
            return updated

    def update_rule_video_trim(self, rule_id: int, seconds: int) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE routing
                    SET video_trim_seconds = %s
                    WHERE id = %s
                    """,
                    (int(seconds), rule_id),
                )
                updated = cur.rowcount > 0
            conn.commit()
            return updated

    def set_rule_intro_horizontal(self, rule_id: int, intro_id: int | None) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE routing
                    SET video_intro_horizontal_id = %s
                    WHERE id = %s
                    """,
                    (intro_id, rule_id),
                )
                updated = cur.rowcount > 0
            conn.commit()
            return updated

    def set_rule_intro_vertical(self, rule_id: int, intro_id: int | None) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE routing
                    SET video_intro_vertical_id = %s
                    WHERE id = %s
                    """,
                    (intro_id, rule_id),
                )
                updated = cur.rowcount > 0
            conn.commit()
            return updated

    def set_rule_add_intro(self, rule_id: int, enabled: bool) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE routing
                    SET video_add_intro = %s
                    WHERE id = %s
                    """,
                    (bool(enabled), rule_id),
                )
                updated = cur.rowcount > 0
            conn.commit()
            return updated

    def touch_rule_after_send(self, rule_id: int, interval: int) -> None:
        now_iso = utc_now_iso()
        now_dt = datetime.now(timezone.utc)

        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT mode, schedule_mode, fixed_times_json, interval, next_run_at
                    FROM routing
                    WHERE id = %s
                    """,
                    (rule_id,),
                )
                row = cur.fetchone()
                if not row:
                    conn.commit()
                    return

                cur.execute(
                    """
                    SELECT COUNT(*) AS cnt
                    FROM deliveries
                    WHERE rule_id = %s
                      AND status = 'pending'
                    """,
                    (rule_id,),
                )
                pending_row = cur.fetchone()
                pending_count = int(pending_row["cnt"] or 0) if pending_row else 0

                schedule_mode = row["schedule_mode"] or "interval"
                rule_mode = (row.get("mode") or "repost").strip().lower()
                if pending_count <= 0:
                    next_run_iso = None
                else:
                    actual_interval = int(row["interval"] or interval or 0)
                    next_run_iso, resolution = self._compute_next_run_after_send(
                        conn,
                        rule_id=rule_id,
                        schedule_mode=str(schedule_mode),
                        fixed_times_json=row["fixed_times_json"],
                        interval_value=actual_interval,
                        now_dt=now_dt,
                    )
                    if resolution == "fixed_fallback_interval":
                        logger.warning(
                            "RULE NEXT RUN FIXED FALLBACK | rule_id=%s | fixed_times пустые/некорректные, используем interval=%s | next_run_at=%s",
                            rule_id,
                            actual_interval,
                            next_run_iso,
                        )

                cur.execute(
                    """
                    UPDATE routing
                    SET last_sent_at = %s,
                        next_run_at = %s
                    WHERE id = %s
                    """,
                    (now_iso, next_run_iso, rule_id),
                )
                logger.info(
                    "RULE NEXT RUN UPDATED | rule_id=%s | mode=%s | schedule_mode=%s | interval=%s | pending=%s | last_sent_at=%s | next_run_at=%s",
                    rule_id,
                    rule_mode,
                    schedule_mode,
                    int(row["interval"] or interval or 0),
                    pending_count,
                    now_iso,
                    next_run_iso,
                )
            conn.commit()

    # =========================================================
    # DUE DELIVERY / TAKING
    # =========================================================

    def get_due_delivery(self, rule_id: int, due_iso: str):
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        d.id AS delivery_id,
                        p.message_id,
                        p.source_channel,
                        p.source_thread_id,
                        p.content_json,
                        p.media_group_id,
                        r.target_id,
                        r.target_thread_id,
                        r.interval,
                        COALESCE(r.tenant_id, d.tenant_id, 1) AS tenant_id
                    FROM deliveries d
                    JOIN posts p ON p.id = d.post_id
                    JOIN routing r ON r.id = d.rule_id
                    WHERE d.rule_id = %s
                      AND d.status = 'pending'
                      AND r.is_active = TRUE
                      AND (r.next_run_at IS NULL OR r.next_run_at <= %s)
                    ORDER BY p.id ASC
                    LIMIT 1
                    """,
                    (rule_id, due_iso),
                )
                return cur.fetchone()

    def take_due_delivery(self, rule_id: int, due_iso: str):
        with self.connect() as conn:
            with conn.cursor() as cur:
                # --- получаем режим правила ---
                cur.execute(
                    """
                    SELECT mode, schedule_mode, fixed_times_json, interval, next_run_at
                    FROM routing
                    WHERE id = %s
                    LIMIT 1
                    """,
                    (rule_id,),
                )
                rule_row = cur.fetchone()

                if not rule_row:
                    conn.commit()
                    return None

                rule_mode = (rule_row["mode"] or "repost").strip().lower()
                schedule_mode = (rule_row["schedule_mode"] or "interval").strip().lower()
                next_run_at_before = rule_row["next_run_at"]

                if next_run_at_before is None:
                    cur.execute(
                        """
                        SELECT COUNT(*) AS cnt
                        FROM deliveries
                        WHERE rule_id = %s
                          AND status = 'pending'
                        """,
                        (rule_id,),
                    )
                    pending_row = cur.fetchone()
                    pending_count = int(pending_row["cnt"] or 0) if pending_row else 0
                    if pending_count > 0:
                        next_run_iso, resolution = self._compute_next_run_after_send(
                            conn,
                            rule_id=rule_id,
                            schedule_mode=schedule_mode,
                            fixed_times_json=rule_row["fixed_times_json"],
                            interval_value=int(rule_row["interval"] or 0),
                            now_dt=datetime.now(timezone.utc),
                        )
                        cur.execute(
                            """
                            UPDATE routing
                            SET next_run_at = %s
                            WHERE id = %s
                            """,
                            (next_run_iso, rule_id),
                        )
                        conn.commit()
                        logger.warning(
                            "TAKE DUE DELIVERY GUARD | rule_id=%s | pending=%s | schedule_mode=%s | previous_next_run_at=NULL | repaired_next_run_at=%s | resolution=%s",
                            rule_id,
                            pending_count,
                            schedule_mode,
                            next_run_iso,
                            resolution,
                        )
                        return None

                # --- выбираем следующую задачу ---
                cur.execute(
                    """
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
                        r.interval,
                        COALESCE(r.tenant_id, d.tenant_id, 1) AS tenant_id
                    FROM deliveries d
                    JOIN routing r ON r.id = d.rule_id
                    JOIN posts p ON p.id = d.post_id
                    WHERE d.rule_id = %s
                    AND d.status = 'pending'
                    AND r.is_active = TRUE
                    AND (r.next_run_at IS NULL OR r.next_run_at <= %s)

                    AND (
                            %s != 'video'
                            OR COALESCE((p.content_json::jsonb ->> 'media_kind'), '') = 'video'
                    )

                    AND NOT EXISTS (
                        SELECT 1
                        FROM deliveries d_rule
                        WHERE d_rule.rule_id = d.rule_id
                            AND d_rule.status = 'processing'
                    )

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
                            AND %s = 'repost'
                    )

                    AND NOT EXISTS (
                        SELECT 1
                        FROM deliveries d_target
                        JOIN routing r_target ON r_target.id = d_target.rule_id
                        WHERE d_target.status = 'processing'
                            AND r_target.target_id = r.target_id
                            AND (
                                (r_target.target_thread_id IS NULL AND r.target_thread_id IS NULL)
                                OR r_target.target_thread_id = r.target_thread_id
                            )
                    )

                    ORDER BY p.id ASC
                    LIMIT 1
                    """,
                    (rule_id, due_iso, rule_mode, rule_mode),
                )
                row = cur.fetchone()

                if not row:
                    if next_run_at_before:
                        logger.debug(
                            "TAKE DUE DELIVERY SKIP | rule_id=%s | due_iso=%s | next_run_at=%s",
                            rule_id,
                            due_iso,
                            next_run_at_before,
                        )
                    conn.commit()
                    return None

                delivery_id = int(row["delivery_id"])
                source_channel = str(row["source_channel"])
                media_group_id = row["media_group_id"]

                # --- логика захвата ---
                if media_group_id and rule_mode != "video":
                    # REPOST режим — берём весь альбом
                    source_thread_id = row["source_thread_id"]

                    if source_thread_id is None:
                        cur.execute(
                            """
                            SELECT d.id AS delivery_id
                            FROM deliveries d
                            JOIN posts p ON p.id = d.post_id
                            WHERE d.rule_id = %s
                            AND d.status = 'pending'
                            AND p.source_channel = %s
                            AND p.source_thread_id IS NULL
                            AND p.media_group_id = %s
                            ORDER BY p.message_id ASC
                            """,
                            (rule_id, source_channel, str(media_group_id)),
                        )
                    else:
                        cur.execute(
                            """
                            SELECT d.id AS delivery_id
                            FROM deliveries d
                            JOIN posts p ON p.id = d.post_id
                            WHERE d.rule_id = %s
                            AND d.status = 'pending'
                            AND p.source_channel = %s
                            AND p.source_thread_id = %s
                            AND p.media_group_id = %s
                            ORDER BY p.message_id ASC
                            """,
                            (rule_id, source_channel, source_thread_id, str(media_group_id)),
                        )

                    album_rows = cur.fetchall()
                    delivery_ids = [int(r["delivery_id"]) for r in album_rows]

                    if not delivery_ids:
                        conn.commit()
                        return None

                    cur.executemany(
                        """
                        UPDATE deliveries
                        SET status = 'processing'
                        WHERE id = %s
                        AND status = 'pending'
                        """,
                        [(d_id,) for d_id in delivery_ids],
                    )

                    cur.execute(
                        """
                        SELECT COUNT(*) AS cnt
                        FROM deliveries
                        WHERE id = ANY(%s)
                        AND status = 'processing'
                        """,
                        (delivery_ids,),
                    )
                    processing_count_row = cur.fetchone()
                    processing_count = int(processing_count_row["cnt"] or 0) if processing_count_row else 0

                    if processing_count != len(delivery_ids):
                        conn.commit()
                        return None

                else:
                    # VIDEO режим — всегда 1 элемент
                    cur.execute(
                        """
                        UPDATE deliveries
                        SET status = 'processing'
                        WHERE id = %s
                        AND status = 'pending'
                        """,
                        (delivery_id,),
                    )

                    if cur.rowcount == 0:
                        conn.commit()
                        return None

                # --- возвращаем задачу ---
                cur.execute(
                    """
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
                    WHERE d.id = %s
                    LIMIT 1
                    """,
                    (delivery_id,),
                )
                taken = cur.fetchone()
                logger.info(
                    "TAKE DUE DELIVERY | rule_id=%s | delivery_id=%s | due_iso=%s | next_run_at_before=%s",
                    rule_id,
                    delivery_id,
                    due_iso,
                    next_run_at_before,
                )

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
            with conn.cursor() as cur:
                if source_thread_id is None:
                    cur.execute(
                        """
                        SELECT d.id AS delivery_id, p.message_id
                        FROM deliveries d
                        JOIN posts p ON p.id = d.post_id
                        WHERE d.rule_id = %s
                          AND d.status IN ('pending', 'processing')
                          AND p.source_channel = %s
                          AND p.source_thread_id IS NULL
                          AND p.media_group_id = %s
                        ORDER BY p.message_id
                        """,
                        (rule_id, str(source_channel), media_group_id),
                    )
                else:
                    cur.execute(
                        """
                        SELECT d.id AS delivery_id, p.message_id
                        FROM deliveries d
                        JOIN posts p ON p.id = d.post_id
                        WHERE d.rule_id = %s
                          AND d.status IN ('pending', 'processing')
                          AND p.source_channel = %s
                          AND p.source_thread_id = %s
                          AND p.media_group_id = %s
                        ORDER BY p.message_id
                        """,
                        (rule_id, str(source_channel), source_thread_id, media_group_id),
                    )
                return cur.fetchall()

    # =========================================================
    # DELIVERY STATUS
    # =========================================================

    def mark_delivery_sent(self, delivery_id: int):
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE deliveries
                    SET status = 'sent',
                        sent_at = %s,
                        error_text = NULL
                    WHERE id = %s
                    """,
                    (utc_now_iso(), delivery_id),
                )
            conn.commit()

    def mark_many_deliveries_sent(self, delivery_ids):
        if not delivery_ids:
            return

        now_iso = utc_now_iso()
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    UPDATE deliveries
                    SET status = 'sent',
                        sent_at = %s,
                        error_text = NULL
                    WHERE id = %s
                    """,
                    [(now_iso, int(d)) for d in delivery_ids],
                )
            conn.commit()

    def mark_delivery_faulty(self, delivery_id: int, error_text: str):
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE deliveries
                    SET status = 'faulty',
                        error_text = %s,
                        attempt_count = attempt_count + 1
                    WHERE id = %s
                    """,
                    ((error_text or "")[:1000], delivery_id),
                )
            conn.commit()

    def mark_delivery_pending(self, delivery_id: int):
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE deliveries
                    SET status = 'pending'
                    WHERE id = %s
                    """,
                    (delivery_id,),
                )
            conn.commit()

    def backfill_deliveries_all(self) -> int:
        total = 0
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id, source_id, source_thread_id FROM routing")
                rules = cur.fetchall()

            for rule in rules:
                total += self._backfill_deliveries_for_rule_conn(
                    conn,
                    int(rule["id"]),
                    str(rule["source_id"]),
                    rule["source_thread_id"],
                )
            conn.commit()
        return total

    def reset_all_deliveries(self):
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE deliveries
                    SET status = 'pending',
                        error_text = NULL,
                        sent_at = NULL
                    WHERE status IN ('processing', 'sent', 'faulty')
                    """
                )
                changed = int(cur.rowcount or 0)

                cur.execute("SELECT COUNT(*) AS cnt FROM deliveries WHERE status = 'faulty'")
                faulty_row = cur.fetchone()
                faulty = int(faulty_row["cnt"] or 0) if faulty_row else 0

            conn.commit()
            return changed, faulty

    def reset_source_deliveries(self, source_id: str, source_thread_id: int | None = None):
        with self.connect() as conn:
            with conn.cursor() as cur:
                if source_thread_id is None:
                    cur.execute(
                        """
                        UPDATE deliveries
                        SET status = 'pending',
                            error_text = NULL,
                            sent_at = NULL
                        WHERE post_id IN (
                            SELECT id
                            FROM posts
                            WHERE source_channel = %s
                              AND source_thread_id IS NULL
                        )
                        """,
                        (str(source_id),),
                    )
                else:
                    cur.execute(
                        """
                        UPDATE deliveries
                        SET status = 'pending',
                            error_text = NULL,
                            sent_at = NULL
                        WHERE post_id IN (
                            SELECT id
                            FROM posts
                            WHERE source_channel = %s
                              AND source_thread_id = %s
                        )
                        """,
                        (str(source_id), source_thread_id),
                    )
                changed = int(cur.rowcount or 0)
            conn.commit()
            return changed

    def reset_queue_for_source(self, source_id: str, source_thread_id: int | None = None) -> int:
        with self.connect() as conn:
            with conn.cursor() as cur:
                if source_thread_id is None:
                    cur.execute(
                        """
                        UPDATE deliveries
                        SET status = 'pending',
                            error_text = NULL,
                            attempt_count = 0,
                            sent_at = NULL
                        WHERE post_id IN (
                            SELECT id
                            FROM posts
                            WHERE source_channel = %s
                              AND source_thread_id IS NULL
                        )
                          AND status = 'sent'
                        """,
                        (str(source_id),),
                    )
                else:
                    cur.execute(
                        """
                        UPDATE deliveries
                        SET status = 'pending',
                            error_text = NULL,
                            attempt_count = 0,
                            sent_at = NULL
                        WHERE post_id IN (
                            SELECT id
                            FROM posts
                            WHERE source_channel = %s
                              AND source_thread_id = %s
                        )
                          AND status = 'sent'
                        """,
                        (str(source_id), source_thread_id),
                    )
                changed = int(cur.rowcount or 0)
            conn.commit()
            return changed

    def reset_all_queue(self) -> tuple[int, int]:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) AS cnt FROM deliveries WHERE status = 'faulty'")
                faulty_row = cur.fetchone()
                faulty = int(faulty_row["cnt"] or 0) if faulty_row else 0

                cur.execute(
                    """
                    UPDATE deliveries
                    SET status = 'pending',
                        error_text = NULL,
                        attempt_count = 0,
                        sent_at = NULL
                    WHERE status = 'sent'
                    """
                )
                changed = int(cur.rowcount or 0)
            conn.commit()
            return changed, faulty

    # =========================================================
    # STATS / DIAGNOSTICS
    # =========================================================

    def get_queue_stats(self):
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) AS cnt FROM posts")
                posts = int(cur.fetchone()["cnt"])

                cur.execute("SELECT COUNT(*) AS cnt FROM deliveries")
                deliveries = int(cur.fetchone()["cnt"])

                cur.execute("SELECT COUNT(*) AS cnt FROM deliveries WHERE status = 'pending'")
                pending = int(cur.fetchone()["cnt"])

                cur.execute("SELECT COUNT(*) AS cnt FROM deliveries WHERE status = 'processing'")
                processing = int(cur.fetchone()["cnt"])

                cur.execute("SELECT COUNT(*) AS cnt FROM deliveries WHERE status = 'sent'")
                sent = int(cur.fetchone()["cnt"])

                cur.execute("SELECT COUNT(*) AS cnt FROM deliveries WHERE status = 'faulty'")
                faulty = int(cur.fetchone()["cnt"])

                cur.execute("SELECT COUNT(*) AS cnt FROM routing")
                rules = int(cur.fetchone()["cnt"])

                cur.execute("SELECT COUNT(*) AS cnt FROM routing WHERE is_active = TRUE")
                active_rules = int(cur.fetchone()["cnt"])

        return {
            "posts": posts,
            "deliveries": deliveries,
            "pending": pending,
            "processing": processing,
            "sent": sent,
            "faulty": faulty,
            "rules": rules,
            "active_rules": active_rules,
        }

    def get_faulty_deliveries(self, limit: int = 20):
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
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
                        COALESCE(r.tenant_id, d.tenant_id, 1) AS tenant_id,

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
                      AND (d.error_text IS NULL OR d.error_text NOT LIKE 'Self-loop:%%')
                    ORDER BY d.id DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
                return cur.fetchall()

    def get_processing_album_for_rule(
        self,
        rule_id: int,
        source_channel: str,
        source_thread_id: int | None,
        media_group_id: str,
    ):
        return self.get_album_pending_for_rule(
            rule_id=rule_id,
            source_channel=source_channel,
            source_thread_id=source_thread_id,
            media_group_id=media_group_id,
        )

    # =========================================================
    # JOB QUEUE
    # =========================================================

    def create_job(
        self,
        job_type: str,
        payload: dict[str, Any],
        queue: str,
        priority: int = 100,
        run_at: str | None = None,
        dedup_key: str | None = None,
    ) -> int | None:
        queue_name = (queue or "default").strip().lower()
        if queue_name not in {"light", "heavy"}:
            queue_name = "default"

        with self.connect() as conn:
            with conn.cursor() as cur:
                if dedup_key:
                    cur.execute(
                        """
                        SELECT id
                        FROM jobs
                        WHERE dedup_key = %s
                          AND status IN ('pending', 'leased', 'processing', 'retry')
                        ORDER BY id DESC
                        LIMIT 1
                        """,
                        (dedup_key,),
                    )
                    existing = cur.fetchone()
                    if existing:
                        conn.commit()
                        return None

                try:
                    cur.execute(
                        """
                        INSERT INTO jobs(job_type, payload_json, dedup_key, status, priority, queue, run_at, created_at, updated_at)
                        VALUES(%s, %s::jsonb, %s, 'pending', %s, %s, COALESCE(%s::timestamptz, NOW()), NOW(), NOW())
                        RETURNING id
                        """,
                        (job_type, _json_dumps(payload) or "{}", dedup_key, int(priority), queue_name, run_at),
                    )
                    row = cur.fetchone()
                except Exception as exc:
                    if "duplicate key value violates unique constraint" not in str(exc).lower():
                        raise
                    conn.rollback()
                    logger.info("Scheduler пропустил дубль задачи по dedup_key=%s", dedup_key)
                    return None
            conn.commit()
            return int(row["id"]) if row else None

    def get_active_job_by_dedup_key(self, dedup_key: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM jobs
                    WHERE dedup_key = %s
                      AND status IN ('pending', 'leased', 'processing', 'retry')
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (dedup_key,),
                )
                row = cur.fetchone()
            conn.commit()
            if not row:
                return None
            item = dict(row)
            item["payload_json"] = _safe_json_loads(item.get("payload_json"), {})
            return item

    def lease_jobs(self, queue: str, worker_id: str, limit: int = 1, lease_seconds: int = 30) -> list[dict]:
        queue_name = (queue or "").strip().lower()
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    WITH picked AS (
                        SELECT id
                        FROM jobs
                        WHERE queue = %s
                          AND status IN ('pending', 'retry')
                          AND run_at <= NOW()
                          AND (lease_until IS NULL OR lease_until < NOW())
                        ORDER BY priority ASC, run_at ASC, created_at ASC
                        FOR UPDATE SKIP LOCKED
                        LIMIT %s
                    )
                    UPDATE jobs j
                    SET status = 'leased',
                        locked_by = %s,
                        lease_until = NOW() + make_interval(secs => %s),
                        updated_at = NOW()
                    FROM picked
                    WHERE j.id = picked.id
                    RETURNING j.*
                    """,
                    (queue_name, max(1, int(limit)), worker_id, max(1, int(lease_seconds))),
                )
                rows = cur.fetchall() or []
            conn.commit()
        return [dict(r) for r in rows]

    def mark_job_processing(self, job_id: int, worker_id: str) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE jobs
                    SET status = 'processing',
                        updated_at = NOW()
                    WHERE id = %s
                      AND status = 'leased'
                      AND locked_by = %s
                    """,
                    (int(job_id), worker_id),
                )
                ok = cur.rowcount > 0
            conn.commit()
            return ok

    def complete_job(self, job_id: int) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE jobs
                    SET status = 'done',
                        lease_until = NULL,
                        locked_by = NULL,
                        error_text = NULL,
                        updated_at = NOW()
                    WHERE id = %s
                    """,
                    (int(job_id),),
                )
                ok = cur.rowcount > 0
            conn.commit()
            return ok

    def retry_job(self, job_id: int, error_text: str, delay_seconds: int) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE jobs
                    SET status = 'retry',
                        attempts = attempts + 1,
                        lease_until = NULL,
                        locked_by = NULL,
                        run_at = NOW() + make_interval(secs => %s),
                        error_text = %s,
                        updated_at = NOW()
                    WHERE id = %s
                    """,
                    (max(1, int(delay_seconds)), (error_text or "")[:1000], int(job_id)),
                )
                ok = cur.rowcount > 0
            conn.commit()
            return ok

    def fail_job(self, job_id: int, error_text: str) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE jobs
                    SET status = 'failed',
                        attempts = attempts + 1,
                        lease_until = NULL,
                        locked_by = NULL,
                        error_text = %s,
                        updated_at = NOW()
                    WHERE id = %s
                    """,
                    ((error_text or "")[:1000], int(job_id)),
                )
                ok = cur.rowcount > 0
            conn.commit()
            return ok

    def get_job(self, job_id: int) -> dict | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM jobs
                    WHERE id = %s
                    LIMIT 1
                    """,
                    (int(job_id),),
                )
                row = cur.fetchone()
            conn.commit()
            if not row:
                return None
            item = dict(row)
            item["payload_json"] = _safe_json_loads(item.get("payload_json"), {})
            return item

    def get_job_status_counts(self) -> dict[str, int]:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT status, COUNT(*) AS cnt
                    FROM jobs
                    GROUP BY status
                    """
                )
                rows = cur.fetchall() or []
            conn.commit()

        counts = {status: 0 for status in ("pending", "leased", "processing", "retry", "failed", "done")}
        for row in rows:
            status = str(row.get("status") or "")
            if status in counts:
                counts[status] = int(row.get("cnt") or 0)
        return counts

    def get_video_stage_job_counts(self) -> dict[str, dict[str, int]]:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT job_type, status, COUNT(*) AS cnt
                    FROM jobs
                    WHERE job_type IN ('video_download', 'video_process', 'video_send', 'video_delivery')
                    GROUP BY job_type, status
                    """
                )
                rows = cur.fetchall() or []
            conn.commit()

        status_keys = ("pending", "leased", "processing", "retry", "failed", "done")
        result = {
            "video_download": {k: 0 for k in status_keys},
            "video_process": {k: 0 for k in status_keys},
            "video_send": {k: 0 for k in status_keys},
            "video_delivery": {k: 0 for k in status_keys},
        }
        for row in rows:
            job_type = str(row.get("job_type") or "")
            status = str(row.get("status") or "")
            if job_type in result and status in result[job_type]:
                result[job_type][status] = int(row.get("cnt") or 0)
        return result

    def get_job_queue_counts(self) -> dict[str, int]:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        queue,
                        status,
                        COUNT(*) AS cnt
                    FROM jobs
                    GROUP BY queue, status
                    """
                )
                rows = cur.fetchall() or []
            conn.commit()

        result = {
            "light_pending": 0,
            "light_processing": 0,
            "light_retry": 0,
            "heavy_pending": 0,
            "heavy_processing": 0,
            "heavy_retry": 0,
        }
        for row in rows:
            queue = str(row.get("queue") or "").strip().lower()
            status = str(row.get("status") or "").strip().lower()
            if queue not in {"light", "heavy"}:
                continue
            key = f"{queue}_{status}"
            if key in result:
                result[key] = int(row.get("cnt") or 0)
        return result

    def get_oldest_pending_job_ages(self) -> dict[str, int]:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        queue,
                        EXTRACT(EPOCH FROM (NOW() - MIN(created_at)))::BIGINT AS age_sec
                    FROM jobs
                    WHERE status = 'pending'
                    GROUP BY queue
                    """
                )
                rows = cur.fetchall() or []
            conn.commit()

        result = {"light": 0, "heavy": 0}
        for row in rows:
            queue = str(row.get("queue") or "").strip().lower()
            if queue not in result:
                continue
            result[queue] = max(int(row.get("age_sec") or 0), 0)
        return result

    def get_expired_leased_jobs(self, limit: int = 100) -> list[dict[str, Any]]:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM jobs
                    WHERE status = 'leased'
                      AND lease_until IS NOT NULL
                      AND lease_until < NOW()
                    ORDER BY lease_until ASC
                    LIMIT %s
                    """,
                    (max(1, int(limit)),),
                )
                rows = cur.fetchall() or []
            conn.commit()
        return [dict(r) for r in rows]

    def requeue_expired_leases(self, delay_seconds: int = 15) -> int:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE jobs
                    SET status = 'retry',
                        attempts = attempts + 1,
                        run_at = NOW() + make_interval(secs => %s),
                        locked_by = NULL,
                        lease_until = NULL,
                        error_text = 'Истёк lease задачи, задача возвращена в очередь',
                        updated_at = NOW()
                    WHERE status = 'leased'
                      AND lease_until IS NOT NULL
                      AND lease_until < NOW()
                    """,
                    (max(1, int(delay_seconds)),),
                )
                count = cur.rowcount or 0
            conn.commit()
        return int(count)

    def get_stuck_processing_jobs(self, stuck_seconds: int = 600, limit: int = 100) -> list[dict[str, Any]]:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM jobs
                    WHERE status = 'processing'
                      AND updated_at < NOW() - make_interval(secs => %s)
                    ORDER BY updated_at ASC
                    LIMIT %s
                    """,
                    (max(1, int(stuck_seconds)), max(1, int(limit))),
                )
                rows = cur.fetchall() or []
            conn.commit()
        return [dict(r) for r in rows]

    def requeue_stuck_processing_jobs(self, stuck_seconds: int = 600, delay_seconds: int = 15) -> int:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE jobs
                    SET status = 'retry',
                        attempts = attempts + 1,
                        run_at = NOW() + make_interval(secs => %s),
                        locked_by = NULL,
                        lease_until = NULL,
                        error_text = 'Watchdog: задача зависла в processing и возвращена в retry',
                        updated_at = NOW()
                    WHERE status = 'processing'
                      AND updated_at < NOW() - make_interval(secs => %s)
                    """,
                    (max(1, int(delay_seconds)), max(1, int(stuck_seconds))),
                )
                count = cur.rowcount or 0
            conn.commit()
        return int(count)

    def get_delivery(self, delivery_id: int):
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
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
                    WHERE d.id = %s
                    LIMIT 1
                    """,
                    (delivery_id,),
                )
                return cur.fetchone()

    def clear_faulty_delivery_log(self, delivery_id: int, admin_id: int | None = None) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, rule_id, post_id, status, error_text
                    FROM deliveries
                    WHERE id = %s
                      AND status = 'faulty'
                    LIMIT 1
                    """,
                    (delivery_id,),
                )
                row = cur.fetchone()

                if not row:
                    conn.commit()
                    return False

                now_iso = utc_now_iso()

                cur.execute(
                    """
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
                    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        now_iso,
                        "faulty_log_cleared",
                        row["rule_id"],
                        row["id"],
                        row["post_id"],
                        admin_id,
                        "cleared",
                        row["error_text"],
                        _json_dumps({"action": "clear_faulty_delivery_log"}),
                    ),
                )

                cur.execute(
                    """
                    DELETE FROM deliveries
                    WHERE id = %s
                      AND status = 'faulty'
                    """,
                    (delivery_id,),
                )
                deleted = cur.rowcount > 0
            conn.commit()
            return deleted

    def get_faulty_delivery_by_id(self, delivery_id: int):
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
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
                    WHERE d.id = %s
                    LIMIT 1
                    """,
                    (delivery_id,),
                )
                return cur.fetchone()

    def get_rule_stats(self):
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, fixed_times_json
                    FROM routing
                    WHERE schedule_mode = 'fixed'
                    AND is_active = TRUE
                    AND next_run_at IS NULL
                    """
                )
                fixed_rows = cur.fetchall()

                for fixed_row in fixed_rows:
                    fixed_times = _safe_json_loads(fixed_row.get("fixed_times_json"), [])
                    fixed_times = normalize_fixed_times(fixed_times)
                    if not fixed_times:
                        continue

                    next_run_iso = get_next_fixed_run_utc(fixed_times)

                    cur.execute(
                        """
                        UPDATE routing
                        SET next_run_at = %s
                        WHERE id = %s
                        """,
                        (next_run_iso, int(fixed_row["id"])),
                    )

            conn.commit()

        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
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
                        CAST(r.next_run_at AS TEXT) AS next_run_at,
                        s.title AS source_title,
                        t.title AS target_title,
                        COALESCE(SUM(CASE WHEN d.status = 'pending' THEN 1 ELSE 0 END), 0) AS pending,
                        COALESCE(SUM(CASE WHEN d.status = 'processing' THEN 1 ELSE 0 END), 0) AS processing,
                        COALESCE(SUM(CASE WHEN d.status = 'sent' THEN 1 ELSE 0 END), 0) AS sent,
                        COALESCE(SUM(CASE WHEN d.status = 'faulty' THEN 1 ELSE 0 END), 0) AS faulty
                    FROM routing r
                    LEFT JOIN deliveries d ON d.rule_id = r.id
                    LEFT JOIN channels s
                    ON s.channel_id = r.source_id
                    AND ((s.thread_id IS NULL AND r.source_thread_id IS NULL) OR s.thread_id = r.source_thread_id)
                    AND s.channel_type = 'source'
                    LEFT JOIN channels t
                    ON t.channel_id = r.target_id
                    AND ((t.thread_id IS NULL AND r.target_thread_id IS NULL) OR t.thread_id = r.target_thread_id)
                    AND t.channel_type = 'target'
                    GROUP BY
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
                        s.title,
                        t.title,
                        r.created_date
                    ORDER BY r.created_date, r.id
                    """
                )
                return cur.fetchall()

    def get_rule_card_snapshot(self, rule_id: int) -> dict[str, Any] | None:
        """
        Быстрый snapshot для карточки одного правила.

        Что делает:
        - читает только одно правило
        - одним отдельным запросом читает deliveries/posts только этого правила
        - ОДИН раз собирает logical items
        - возвращает уже готовые данные для build_rule_card_text()

        Это основной SaaS-путь для rule_card / rule_refresh.
        """
        try:
            rule = self.get_rule(rule_id)
            if not rule:
                logger.warning("get_rule_card_snapshot: правило не найдено, rule_id=%s", rule_id)
                return None

            mode = (getattr(rule, "mode", "repost") or "repost").strip().lower()

            with self.connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT
                            d.id AS delivery_id,
                            d.status,
                            d.sent_at,
                            d.error_text,
                            d.attempt_count,
                            p.id AS post_id,
                            p.message_id,
                            p.source_channel,
                            p.source_thread_id,
                            p.media_group_id,
                            p.content_json,
                            p.created_at
                        FROM deliveries d
                        JOIN posts p ON p.id = d.post_id
                        WHERE d.rule_id = %s
                        ORDER BY p.id ASC
                        """,
                        (rule_id,),
                    )
                    rows = cur.fetchall()

            logical_items = self._build_rule_logical_items_from_rows(
                rule_id=rule_id,
                rows=rows,
                mode=mode,
            )
            logical_summary = self._summarize_rule_logical_items(logical_items)

            physical_pending = 0
            physical_processing = 0
            physical_sent = 0
            physical_faulty = 0

            for row in rows:
                status = str(row["status"] or "").strip().lower()
                if status == "pending":
                    physical_pending += 1
                elif status == "processing":
                    physical_processing += 1
                elif status == "sent":
                    physical_sent += 1
                elif status == "faulty":
                    physical_faulty += 1

            logical_total = logical_summary["total"]
            logical_completed = logical_summary["completed"]
            logical_pending = logical_summary["pending"]
            logical_processing = logical_summary["processing"]
            logical_faulty = logical_summary["faulty"]
            logical_current_position = logical_summary["current_position"]

            snapshot = {
                "id": rule.id,
                "source_id": rule.source_id,
                "source_thread_id": rule.source_thread_id,
                "target_id": rule.target_id,
                "target_thread_id": rule.target_thread_id,
                "interval": rule.interval,
                "schedule_mode": rule.schedule_mode,
                "fixed_times_json": rule.fixed_times_json,
                "mode": rule.mode,
                "video_trim_seconds": rule.video_trim_seconds,
                "video_add_intro": rule.video_add_intro,
                "video_intro_horizontal": rule.video_intro_horizontal,
                "video_intro_vertical": rule.video_intro_vertical,
                "video_intro_horizontal_id": rule.video_intro_horizontal_id,
                "video_intro_vertical_id": rule.video_intro_vertical_id,
                "video_caption": rule.video_caption,
                "video_caption_entities_json": rule.video_caption_entities_json,
                "caption_delivery_mode": getattr(rule, "caption_delivery_mode", "auto"),
                "video_caption_delivery_mode": getattr(rule, "video_caption_delivery_mode", "auto"),
                "is_active": bool(rule.is_active),
                "next_run_at": rule.next_run_at,
                "last_sent_at": rule.last_sent_at,
                "source_title": rule.source_title,
                "target_title": rule.target_title,
                "pending": physical_pending,
                "processing": physical_processing,
                "sent": physical_sent,
                "faulty": physical_faulty,
                "logical_pending": logical_pending,
                "logical_processing": logical_processing,
                "logical_completed": logical_completed,
                "logical_faulty": logical_faulty,
                "logical_total": logical_total,
                "logical_current_position": logical_current_position,
            }

            logger.info(
                "get_rule_card_snapshot: rule_id=%s, mode=%s, raw_rows=%s, logical_total=%s, logical_pending=%s, logical_processing=%s, logical_completed=%s, logical_faulty=%s, logical_current_position=%s",
                rule_id,
                mode,
                len(rows),
                logical_total,
                logical_pending,
                logical_processing,
                logical_completed,
                logical_faulty,
                logical_current_position,
            )

            return snapshot

        except Exception as exc:
            logger.exception(
                "get_rule_card_snapshot: авария snapshot rule_id=%s error=%s",
                rule_id,
                exc,
            )
            return None

    def _summarize_rule_logical_items(self, logical_items) -> dict[str, Any]:
        """
        Единая сводка по logical items для snapshot/position.
        """
        items = list(logical_items or [])
        total = len(items)
        completed = sum(1 for item in items if item.get("is_done"))
        processing = sum(
            1
            for item in items
            if (not item.get("is_done")) and int(item.get("processing_count") or 0) > 0
        )
        faulty = sum(
            1
            for item in items
            if (not item.get("is_done"))
            and int(item.get("processing_count") or 0) <= 0
            and int(item.get("pending_count") or 0) <= 0
            and int(item.get("faulty_count") or 0) > 0
        )
        pending = max(total - completed - processing - faulty, 0)

        current_position = None
        for item in items:
            if not item.get("is_done"):
                current_position = int(item["position"])
                break

        if current_position is None and total > 0:
            current_position = total

        return {
            "total": total,
            "completed": completed,
            "pending": pending,
            "processing": processing,
            "faulty": faulty,
            "current_position": current_position,
        }

    def get_rule_faulty_count(self, rule_id: int) -> int:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*) AS cnt
                    FROM deliveries
                    WHERE rule_id = %s
                      AND status = 'faulty'
                    """,
                    (rule_id,),
                )
                row = cur.fetchone()
        return int(row["cnt"] or 0) if row else 0

    def get_next_scheduled_rule(self):
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        r.id,
                        CAST(r.next_run_at AS TEXT) AS next_run_at,
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
                    WHERE r.is_active = TRUE
                      AND r.next_run_at IS NOT NULL
                      AND EXISTS (
                          SELECT 1
                          FROM deliveries d
                          WHERE d.rule_id = r.id
                            AND d.status = 'pending'
                      )
                    ORDER BY r.next_run_at ASC
                    LIMIT 1
                    """
                )
                return cur.fetchone()

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
                        content = _safe_json_loads(row["content_json"], {})
                    except Exception:
                        content = {}
                        broken_content_json += 1

                    media_kind = str(content.get("media_kind") or "text").strip().lower()
                    if media_kind != "video":
                        skipped_non_video += 1
                        continue

                    status = str(row["status"] or "").strip().lower()

                    items.append(
                        {
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
                        }
                    )

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

    # =========================================================
    # QUEUE LOGICAL VIEW
    # =========================================================

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
                with conn.cursor() as cur:
                    cur.execute(
                        """
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
                        WHERE d.rule_id = %s
                        AND d.status = 'pending'
                        ORDER BY p.id ASC
                        """,
                        (rule_id,),
                    )
                    rows = cur.fetchall()

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

    def get_rule_queue_item_shifted(
        self,
        rule_id: int,
        current_position: int,
        shift: int,
    ) -> dict[str, Any] | None:
        items = self.get_rule_queue_logical_items(rule_id)
        if not items:
            return None

        new_position = current_position + shift
        new_position = max(1, min(new_position, len(items)))
        return items[new_position - 1]

    def set_rule_start_from_position(
        self,
        rule_id: int,
        position: int,
    ) -> dict[str, Any] | None:
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
                with conn.cursor() as cur:
                    if ids_to_mark_sent:
                        cur.executemany(
                            """
                            UPDATE deliveries
                            SET status = 'sent',
                                sent_at = %s,
                                error_text = NULL
                            WHERE id = %s
                            """,
                            [(now_iso, delivery_id) for delivery_id in ids_to_mark_sent],
                        )

                    cur.execute(
                        """
                        UPDATE routing
                        SET next_run_at = %s
                        WHERE id = %s
                        """,
                        (now_iso, rule_id),
                    )

                    cur.execute(
                        """
                        INSERT INTO audit_log(
                            created_at,
                            event_type,
                            rule_id,
                            post_id,
                            status,
                            new_value_json,
                            extra_json
                        )
                        VALUES(%s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            now_iso,
                            "rule_start_position_changed",
                            rule_id,
                            selected["first_post_id"],
                            "pending",
                            _json_dumps(
                                {
                                    "position": selected["position"],
                                    "kind": selected["kind"],
                                    "mode": selected.get("mode"),
                                    "first_post_id": selected["first_post_id"],
                                    "first_message_id": selected["first_message_id"],
                                    "delivery_ids": selected["delivery_ids"],
                                    "count": selected.get("count"),
                                }
                            ),
                            _json_dumps(
                                {
                                    "skipped_delivery_ids": ids_to_mark_sent,
                                    "skipped_logical_items_count": max(position - 1, 0),
                                }
                            ),
                        ),
                    )

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
                with conn.cursor() as cur:
                    cur.execute(
                        """
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
                        WHERE d.rule_id = %s
                        ORDER BY p.id ASC
                        """,
                        (rule_id,),
                    )
                    rows = cur.fetchall()

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
            logical_summary = self._summarize_rule_logical_items(logical_items)

            total = logical_summary["total"]
            if total <= 0:
                logger.info(
                    "get_rule_position_info: после builder очередь пуста, rule_id=%s, mode=%s, raw_rows=%s",
                    rule_id,
                    mode,
                    len(rows),
                )
                return empty_result

            completed = logical_summary["completed"]
            current_position = logical_summary["current_position"]

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

    # =========================================================
    # AUDIT
    # =========================================================

    def create_tenant(self, owner_admin_id: int, name: str) -> int | None:
        with self.connect() as conn:
            tenant_id = self._ensure_tenant_for_admin_conn(conn, int(owner_admin_id))
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE tenants
                    SET name = COALESCE(NULLIF(%s, ''), name)
                    WHERE id = %s
                    """,
                    (name, tenant_id),
                )
            conn.commit()
            return tenant_id

    def get_tenant_by_admin(self, admin_id: int) -> dict[str, Any] | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, name, owner_admin_id, created_at, is_active
                    FROM tenants
                    WHERE owner_admin_id = %s
                    LIMIT 1
                    """,
                    (int(admin_id),),
                )
                row = cur.fetchone()
        return dict(row) if row else None

    def get_default_tenant(self) -> dict[str, Any] | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, name, owner_admin_id, created_at, is_active
                    FROM tenants
                    ORDER BY id ASC
                    LIMIT 1
                    """
                )
                row = cur.fetchone()
        return dict(row) if row else None

    def get_tenant_by_id(self, tenant_id: int) -> dict[str, Any] | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, name, owner_admin_id, created_at, is_active
                    FROM tenants
                    WHERE id = %s
                    LIMIT 1
                    """,
                    (int(tenant_id),),
                )
                row = cur.fetchone()
        return dict(row) if row else None

    def set_tenant_active(self, tenant_id: int, is_active: bool) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE tenants SET is_active = %s WHERE id = %s",
                    (bool(is_active), int(tenant_id)),
                )
                updated = cur.rowcount > 0
            conn.commit()
            return updated

    def add_tenant_user(self, tenant_id: int, telegram_id: int, role: str) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO tenant_users(tenant_id, telegram_id, role, created_at)
                    VALUES(%s, %s, %s, %s)
                    ON CONFLICT(tenant_id, telegram_id)
                    DO UPDATE SET role = EXCLUDED.role
                    """,
                    (int(tenant_id), int(telegram_id), str(role), utc_now_iso()),
                )
                ok = cur.rowcount > 0
            conn.commit()
            return ok

    def get_tenant_user_role(self, tenant_id: int, telegram_id: int) -> str | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT role
                    FROM tenant_users
                    WHERE tenant_id = %s
                      AND telegram_id = %s
                    LIMIT 1
                    """,
                    (int(tenant_id), int(telegram_id)),
                )
                row = cur.fetchone()
        return str(row["role"]) if row else None

    def get_plan_by_name(self, plan_name: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM plans
                    WHERE name = %s
                      AND is_active = TRUE
                    LIMIT 1
                    """,
                    (str(plan_name).upper(),),
                )
                row = cur.fetchone()
        return dict(row) if row else None

    def assign_subscription(self, tenant_id: int, plan_id: int, *, status: str = "active", expires_at: str | None = None) -> int | None:
        now_iso = utc_now_iso()
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE subscriptions
                    SET status = 'expired'
                    WHERE tenant_id = %s
                      AND status IN ('active', 'trial', 'grace')
                    """,
                    (int(tenant_id),),
                )
                cur.execute(
                    """
                    INSERT INTO subscriptions(
                        tenant_id, plan_id, status, started_at, expires_at, created_at, updated_at, current_period_start, current_period_end
                    )
                    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (int(tenant_id), int(plan_id), status, now_iso, expires_at, now_iso, now_iso, now_iso, expires_at),
                )
                row = cur.fetchone()
            conn.commit()
            return int(row["id"]) if row else None

    def get_active_subscription(self, tenant_id: int) -> dict[str, Any] | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT s.*, p.name AS plan_name, p.max_rules, p.max_workers, p.max_jobs_per_day, p.max_video_per_day, p.max_storage_mb, p.priority_level
                    FROM subscriptions s
                    JOIN plans p ON p.id = s.plan_id
                    WHERE s.tenant_id = %s
                      AND s.status IN ('active', 'trial', 'grace')
                    ORDER BY s.id DESC
                    LIMIT 1
                    """,
                    (int(tenant_id),),
                )
                row = cur.fetchone()
        return dict(row) if row else None

    def get_latest_subscription(self, tenant_id: int) -> dict[str, Any] | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT s.*, p.name AS plan_name, p.max_rules, p.max_workers, p.max_jobs_per_day, p.max_video_per_day, p.max_storage_mb, p.priority_level, p.price
                    FROM subscriptions s
                    JOIN plans p ON p.id = s.plan_id
                    WHERE s.tenant_id = %s
                    ORDER BY s.id DESC
                    LIMIT 1
                    """,
                    (int(tenant_id),),
                )
                row = cur.fetchone()
        return dict(row) if row else None

    def expire_subscription(self, tenant_id: int) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE subscriptions
                    SET status = 'expired'
                    WHERE tenant_id = %s
                      AND status IN ('active', 'trial', 'grace')
                    """,
                    (int(tenant_id),),
                )
                updated = cur.rowcount > 0
            conn.commit()
            return updated

    def set_subscription_status(self, subscription_id: int, new_status: str) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE subscriptions
                    SET status = %s,
                        updated_at = %s,
                        canceled_at = CASE WHEN %s = 'canceled' THEN %s ELSE canceled_at END
                    WHERE id = %s
                    """,
                    (str(new_status), utc_now_iso(), str(new_status), utc_now_iso(), int(subscription_id)),
                )
                updated = cur.rowcount > 0
            conn.commit()
            return updated

    def set_subscription_grace_window(self, subscription_id: int, grace_started_at: str, grace_ends_at: str) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE subscriptions
                    SET grace_started_at = %s,
                        grace_ends_at = %s,
                        updated_at = %s
                    WHERE id = %s
                    """,
                    (grace_started_at, grace_ends_at, utc_now_iso(), int(subscription_id)),
                )
                updated = cur.rowcount > 0
            conn.commit()
            return updated

    def set_subscription_pending_plan(self, subscription_id: int, pending_plan_id: int) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE subscriptions
                    SET pending_plan_id = %s,
                        updated_at = %s
                    WHERE id = %s
                    """,
                    (int(pending_plan_id), utc_now_iso(), int(subscription_id)),
                )
                updated = cur.rowcount > 0
            conn.commit()
            return updated

    def replace_subscription_plan(self, subscription_id: int, plan_id: int) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE subscriptions
                    SET plan_id = %s,
                        pending_plan_id = NULL,
                        updated_at = %s
                    WHERE id = %s
                    """,
                    (int(plan_id), utc_now_iso(), int(subscription_id)),
                )
                updated = cur.rowcount > 0
            conn.commit()
            return updated

    def bump_usage(self, tenant_id: int, *, jobs_delta: int = 0, video_delta: int = 0, storage_delta_mb: int = 0, api_calls_delta: int = 0) -> None:
        day = datetime.now(timezone.utc).date().isoformat()
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO usage_stats(tenant_id, date, jobs_count, video_count, storage_used_mb, api_calls)
                    VALUES(%s, %s, %s, %s, %s, %s)
                    ON CONFLICT(tenant_id, date)
                    DO UPDATE SET
                        jobs_count = usage_stats.jobs_count + EXCLUDED.jobs_count,
                        video_count = usage_stats.video_count + EXCLUDED.video_count,
                        storage_used_mb = usage_stats.storage_used_mb + EXCLUDED.storage_used_mb,
                        api_calls = usage_stats.api_calls + EXCLUDED.api_calls
                    """,
                    (int(tenant_id), day, int(jobs_delta), int(video_delta), int(storage_delta_mb), int(api_calls_delta)),
                )
            conn.commit()

    def get_usage_for_date(self, tenant_id: int, day: str) -> dict[str, Any]:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT tenant_id, date, jobs_count, video_count, storage_used_mb, api_calls
                    FROM usage_stats
                    WHERE tenant_id = %s
                      AND date = %s
                    LIMIT 1
                    """,
                    (int(tenant_id), day),
                )
                row = cur.fetchone()
        return dict(row) if row else {"tenant_id": int(tenant_id), "date": day, "jobs_count": 0, "video_count": 0, "storage_used_mb": 0, "api_calls": 0}

    def get_usage_for_period(self, tenant_id: int, date_from: str, date_to: str) -> dict[str, Any]:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        COALESCE(SUM(jobs_count), 0) AS jobs_count,
                        COALESCE(SUM(video_count), 0) AS video_count,
                        COALESCE(MAX(storage_used_mb), 0) AS storage_used_mb,
                        COALESCE(SUM(api_calls), 0) AS api_calls
                    FROM usage_stats
                    WHERE tenant_id = %s
                      AND date >= %s
                      AND date <= %s
                    """,
                    (int(tenant_id), str(date_from), str(date_to)),
                )
                row = cur.fetchone() or {}
        return {
            "tenant_id": int(tenant_id),
            "date_from": str(date_from),
            "date_to": str(date_to),
            "jobs_count": int(row.get("jobs_count") or 0),
            "video_count": int(row.get("video_count") or 0),
            "storage_used_mb": int(row.get("storage_used_mb") or 0),
            "api_calls": int(row.get("api_calls") or 0),
        }

    def reset_usage_for_day(self, day: str) -> int:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE usage_stats
                    SET jobs_count = 0,
                        video_count = 0
                    WHERE date = %s
                    """,
                    (day,),
                )
                count = cur.rowcount or 0
            conn.commit()
            return int(count)

    def add_subscription_history(
        self,
        *,
        tenant_id: int,
        old_plan_id: int | None,
        new_plan_id: int | None,
        old_status: str | None,
        new_status: str | None,
        changed_by: str,
        reason: str,
        effective_from: str | None,
        effective_to: str | None = None,
    ) -> int | None:
        with self.connect() as conn:
            row_id = self._fetch_inserted_id(
                conn,
                """
                INSERT INTO subscription_history(
                    tenant_id, old_plan_id, new_plan_id, old_status, new_status,
                    changed_at, changed_by, reason, effective_from, effective_to
                )
                VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    int(tenant_id),
                    old_plan_id,
                    new_plan_id,
                    old_status,
                    new_status,
                    utc_now_iso(),
                    changed_by,
                    reason,
                    effective_from,
                    effective_to,
                ),
            )
            conn.commit()
            return row_id

    def get_subscription_history(self, tenant_id: int, limit: int = 20) -> list[dict[str, Any]]:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM subscription_history
                    WHERE tenant_id = %s
                    ORDER BY id DESC
                    LIMIT %s
                    """,
                    (int(tenant_id), int(limit)),
                )
                rows = cur.fetchall() or []
        return [dict(r) for r in rows]

    def create_billing_event(
        self,
        tenant_id: int,
        event_type: str,
        *,
        event_source: str | None = None,
        amount: float | None = None,
        currency: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> int | None:
        with self.connect() as conn:
            row_id = self._fetch_inserted_id(
                conn,
                """
                INSERT INTO billing_events(tenant_id, event_type, event_source, amount, currency, metadata_json, created_at)
                VALUES(%s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    int(tenant_id),
                    str(event_type),
                    event_source,
                    amount,
                    currency,
                    _json_dumps(metadata),
                    utc_now_iso(),
                ),
            )
            conn.commit()
            return row_id

    def get_billing_events(self, tenant_id: int, limit: int = 20) -> list[dict[str, Any]]:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM billing_events
                    WHERE tenant_id = %s
                    ORDER BY id DESC
                    LIMIT %s
                    """,
                    (int(tenant_id), int(limit)),
                )
                rows = cur.fetchall() or []
        result: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["metadata_json"] = _safe_json_loads(item.get("metadata_json"), {})
            result.append(item)
        return result

    def create_invoice(
        self,
        *,
        tenant_id: int,
        subscription_id: int,
        period_start: str,
        period_end: str,
        status: str,
        currency: str,
        due_at: str | None,
    ) -> int | None:
        now_iso = utc_now_iso()
        with self.connect() as conn:
            row_id = self._fetch_inserted_id(
                conn,
                """
                INSERT INTO invoices(
                    tenant_id, subscription_id, period_start, period_end, status,
                    subtotal, total, currency, created_at, updated_at, due_at
                )
                VALUES(%s, %s, %s, %s, %s, 0, 0, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    int(tenant_id),
                    int(subscription_id),
                    str(period_start),
                    str(period_end),
                    str(status),
                    str(currency).upper(),
                    now_iso,
                    now_iso,
                    due_at,
                ),
            )
            conn.commit()
            return row_id

    def add_invoice_item(
        self,
        invoice_id: int,
        *,
        item_type: str,
        description: str,
        quantity: int,
        unit_price: float,
        amount: float,
        metadata: dict[str, Any] | None = None,
    ) -> int | None:
        with self.connect() as conn:
            row_id = self._fetch_inserted_id(
                conn,
                """
                INSERT INTO invoice_items(invoice_id, item_type, description, quantity, unit_price, amount, metadata_json)
                VALUES(%s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    int(invoice_id),
                    str(item_type),
                    str(description),
                    int(quantity),
                    unit_price,
                    amount,
                    _json_dumps(metadata or {}),
                ),
            )
            conn.commit()
            return row_id

    def recalculate_invoice_totals(self, invoice_id: int) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    WITH sums AS (
                        SELECT COALESCE(SUM(amount), 0) AS amount_sum
                        FROM invoice_items
                        WHERE invoice_id = %s
                    )
                    UPDATE invoices
                    SET subtotal = sums.amount_sum,
                        total = sums.amount_sum,
                        updated_at = %s
                    FROM sums
                    WHERE id = %s
                    """,
                    (int(invoice_id), utc_now_iso(), int(invoice_id)),
                )
                updated = cur.rowcount > 0
            conn.commit()
            return updated

    def set_invoice_status(
        self,
        invoice_id: int,
        status: str,
        *,
        updated_at: str | None = None,
        paid_at: str | None = None,
        external_reference: str | None = None,
    ) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE invoices
                    SET status = %s,
                        updated_at = %s,
                        paid_at = COALESCE(%s, paid_at),
                        external_reference = COALESCE(%s, external_reference)
                    WHERE id = %s
                    """,
                    (str(status), updated_at or utc_now_iso(), paid_at, external_reference, int(invoice_id)),
                )
                updated = cur.rowcount > 0
            conn.commit()
            return updated

    def get_invoice(self, invoice_id: int) -> dict[str, Any] | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM invoices WHERE id = %s LIMIT 1", (int(invoice_id),))
                row = cur.fetchone()
        return dict(row) if row else None

    def get_last_invoice(self, tenant_id: int) -> dict[str, Any] | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM invoices
                    WHERE tenant_id = %s
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (int(tenant_id),),
                )
                row = cur.fetchone()
        return dict(row) if row else None

    def count_open_invoices(self, tenant_id: int) -> int:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*) AS cnt
                    FROM invoices
                    WHERE tenant_id = %s
                      AND status IN ('draft', 'open', 'uncollectible')
                    """,
                    (int(tenant_id),),
                )
                row = cur.fetchone()
        return int((row or {}).get("cnt") or 0)

    def count_rules_for_tenant(self, tenant_id: int) -> int:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*) AS cnt
                    FROM routing
                    WHERE COALESCE(tenant_id, 1) = %s
                    """,
                    (int(tenant_id),),
                )
                row = cur.fetchone()
        return int(row["cnt"] or 0) if row else 0

    def get_rule_tenant_id(self, rule_id: int) -> int:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COALESCE(tenant_id, 1) AS tenant_id
                    FROM routing
                    WHERE id = %s
                    LIMIT 1
                    """,
                    (int(rule_id),),
                )
                row = cur.fetchone()
        return int(row["tenant_id"]) if row else 1

    def get_saas_health_snapshot(self) -> dict[str, Any]:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) AS cnt FROM tenants WHERE is_active = TRUE")
                active = int((cur.fetchone() or {}).get("cnt") or 0)
                cur.execute("SELECT COUNT(*) AS cnt FROM tenants WHERE is_active = FALSE")
                blocked = int((cur.fetchone() or {}).get("cnt") or 0)
                cur.execute("SELECT COUNT(*) AS cnt FROM subscriptions WHERE status = 'active'")
                subscriptions_active = int((cur.fetchone() or {}).get("cnt") or 0)
                cur.execute("SELECT COUNT(*) AS cnt FROM subscriptions WHERE status = 'grace'")
                subscriptions_in_grace = int((cur.fetchone() or {}).get("cnt") or 0)
                cur.execute("SELECT COUNT(*) AS cnt FROM subscriptions WHERE status = 'expired'")
                subscriptions_expired = int((cur.fetchone() or {}).get("cnt") or 0)
                cur.execute("SELECT COUNT(*) AS cnt FROM invoices WHERE status IN ('draft', 'open', 'uncollectible')")
                invoices_open = int((cur.fetchone() or {}).get("cnt") or 0)
                cur.execute(
                    """
                    SELECT COUNT(DISTINCT u.tenant_id) AS cnt
                    FROM usage_stats u
                    JOIN subscriptions s ON s.tenant_id = u.tenant_id
                    JOIN plans p ON p.id = s.plan_id
                    WHERE u.date = %s
                      AND s.status IN ('active', 'trial')
                      AND (u.jobs_count >= p.max_jobs_per_day OR u.video_count >= p.max_video_per_day)
                    """,
                    (datetime.now(timezone.utc).date().isoformat(),),
                )
                over = int((cur.fetchone() or {}).get("cnt") or 0)
                cur.execute(
                    """
                    SELECT COUNT(DISTINCT tenant_id) AS cnt
                    FROM billing_events
                    WHERE event_type IN ('invoice_marked_void', 'subscription_expired')
                    """
                )
                billing_issues = int((cur.fetchone() or {}).get("cnt") or 0)
                cur.execute(
                    """
                    SELECT COUNT(DISTINCT tenant_id) AS cnt
                    FROM billing_events
                    WHERE event_type = 'usage_threshold_reached'
                    """
                )
                overage_candidates = int((cur.fetchone() or {}).get("cnt") or 0)
        return {
            "tenants_active": active,
            "tenants_blocked": blocked,
            "tenants_over_limits": over,
            "subscriptions_active": subscriptions_active,
            "subscriptions_in_grace": subscriptions_in_grace,
            "subscriptions_expired": subscriptions_expired,
            "invoices_open": invoices_open,
            "tenants_with_billing_issues": billing_issues,
            "tenants_with_overage_candidates": overage_candidates,
        }

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
        tenant_id: int | None = None,
    ) -> None:
        resolved_tenant_id = tenant_id
        if resolved_tenant_id is None and rule_id is not None:
            resolved_tenant_id = self.get_rule_tenant_id(int(rule_id))
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
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
                        extra_json,
                        tenant_id
                    )
                    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
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
                        _json_dumps(old_value),
                        _json_dumps(new_value),
                        _json_dumps(extra),
                        resolved_tenant_id,
                    ),
                )
            conn.commit()

    def get_audit_for_rule(self, rule_id: int, limit: int = 50):
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM audit_log
                    WHERE rule_id = %s
                    ORDER BY id DESC
                    LIMIT %s
                    """,
                    (rule_id, limit),
                )
                return cur.fetchall()

    def get_audit_for_delivery(self, delivery_id: int, limit: int = 50):
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM audit_log
                    WHERE delivery_id = %s
                    ORDER BY id DESC
                    LIMIT %s
                    """,
                    (delivery_id, limit),
                )
                return cur.fetchall()

    def get_recent_audit(self, limit: int = 100):
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM audit_log
                    ORDER BY id DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
                return cur.fetchall()

    def get_recent_video_audit(self, limit: int = 200):
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM audit_log
                    WHERE event_type LIKE 'video_%%'
                       OR COALESCE((extra_json::jsonb ->> 'mode'), '') = 'video'
                    ORDER BY id DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
                return cur.fetchall()

    def get_video_audit_for_delivery(self, delivery_id: int, limit: int = 100):
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM audit_log
                    WHERE delivery_id = %s
                      AND (
                        event_type LIKE 'video_%%'
                        OR COALESCE((extra_json::jsonb ->> 'mode'), '') = 'video'
                      )
                    ORDER BY id DESC
                    LIMIT %s
                    """,
                    (delivery_id, limit),
                )
                return cur.fetchall()

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

    # =========================================================
    # PROBLEMS
    # =========================================================

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
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM problem_state
                    WHERE problem_key = %s
                    LIMIT 1
                    """,
                    (problem_key,),
                )
                row = cur.fetchone()

                if row:
                    cur.execute(
                        """
                        UPDATE problem_state
                        SET last_seen_at = %s,
                            hit_count = hit_count + 1,
                            is_active = TRUE
                        WHERE problem_key = %s
                        """,
                        (now_iso, problem_key),
                    )
                    cur.execute(
                        """
                        SELECT *
                        FROM problem_state
                        WHERE problem_key = %s
                        LIMIT 1
                        """,
                        (problem_key,),
                    )
                    updated = cur.fetchone()
                    conn.commit()
                    return dict(updated)

                cur.execute(
                    """
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
                    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        problem_key,
                        problem_type,
                        rule_id,
                        delivery_id,
                        now_iso,
                        now_iso,
                        None,
                        1,
                        True,
                        False,
                        _json_dumps(extra),
                    ),
                )

                cur.execute(
                    """
                    SELECT *
                    FROM problem_state
                    WHERE problem_key = %s
                    LIMIT 1
                    """,
                    (problem_key,),
                )
                created = cur.fetchone()
            conn.commit()
            return dict(created)

    def mark_problem_notified(self, problem_key: str) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE problem_state
                    SET last_notified_at = %s
                    WHERE problem_key = %s
                    """,
                    (utc_now_iso(), problem_key),
                )
                updated = cur.rowcount > 0
            conn.commit()
            return updated

    def resolve_problem(self, problem_key: str) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE problem_state
                    SET is_active = FALSE
                    WHERE problem_key = %s
                    """,
                    (problem_key,),
                )
                updated = cur.rowcount > 0
            conn.commit()
            return updated

    def mute_problem(self, problem_key: str, muted: bool = True) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE problem_state
                    SET is_muted = %s
                    WHERE problem_key = %s
                    """,
                    (bool(muted), problem_key),
                )
                updated = cur.rowcount > 0
            conn.commit()
            return updated

    def get_problem_state(self, problem_key: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM problem_state
                    WHERE problem_key = %s
                    LIMIT 1
                    """,
                    (problem_key,),
                )
                row = cur.fetchone()

        return dict(row) if row else None

    def update_runtime_heartbeat(self, role: str) -> None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO runtime_heartbeat (role, last_seen_at)
                    VALUES (%s, NOW())
                    ON CONFLICT (role) DO UPDATE SET last_seen_at = EXCLUDED.last_seen_at
                    """,
                    (str(role),),
                )
            conn.commit()

    def get_runtime_heartbeats(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT role, last_seen_at
                    FROM runtime_heartbeat
                    """
                )
                rows = cur.fetchall()
        return [dict(row) for row in rows]

    def count_recent_errors(self, minutes: int = 5) -> int:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*) AS cnt
                    FROM deliveries
                    WHERE status = 'faulty'
                      AND CAST(created_at AS timestamptz) >= NOW() - (%s * INTERVAL '1 minute')

                    """,
                    (max(int(minutes), 1),),
                )
                row = cur.fetchone()
        return int(row["cnt"] if row else 0)
