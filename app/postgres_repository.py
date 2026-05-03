from __future__ import annotations

import json
import logging
import re
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
from app.tenant_repository import TenantRepository
from app.subscription_repository import SubscriptionRepository
from app.billing_repository import BillingRepository
from app.payment_repository import PaymentRepository
from app.usage_repository import UsageRepository
from app.tenant_fairness_service import TenantFairnessService
from app.job_service import (
    JOB_PRIORITY_BY_TYPE,
    JOB_QUEUE_BY_TYPE,
    JOB_TYPE_REPOST_ALBUM,
    JOB_TYPE_REPOST_SINGLE,
    JOB_TYPE_VIDEO_DOWNLOAD,
    VIDEO_ARTIFACT_VERSION,
    VIDEO_PIPELINE_VERSION,
    build_dedup_key_for_album,
    build_dedup_key_for_single,
    build_dedup_key_for_video,
)

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


_JOB_TENANT_SQL = "COALESCE(NULLIF(j.payload_json->>'tenant_id', '')::BIGINT, 1)"


class PostgresRepository(RepositoryProtocol):
    def __init__(self) -> None:
        self.client = PostgresClient()
        self.tenant_repo = TenantRepository(self)
        self.subscription_repo = SubscriptionRepository(self)
        self.billing_repo = BillingRepository(self)
        self.payment_repo = PaymentRepository(self)
        self.usage_repo = UsageRepository(self)

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
            sent_at TEXT NULL,
            sent_message_id BIGINT NULL,
            sent_message_ids_json JSONB NULL,
            target_id_snapshot TEXT NULL,
            delivery_method TEXT NULL
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

        CREATE TABLE IF NOT EXISTS payment_intents(
            id BIGSERIAL PRIMARY KEY,
            tenant_id BIGINT NOT NULL,
            invoice_id BIGINT NOT NULL,
            provider TEXT NOT NULL CHECK(provider IN (
                'telegram_stars','telegram_payments','paypal','card_provider','sbp_provider',
                'crypto_manual','tribute','lava_top','manual_bank_card'
            )),
            status TEXT NOT NULL CHECK(status IN ('created','pending','waiting_confirmation','checkout_opened','paid','failed','expired','canceled')),
            amount NUMERIC(12,2) NOT NULL DEFAULT 0,
            currency TEXT NOT NULL DEFAULT 'USD',
            external_payment_id TEXT NULL,
            external_checkout_url TEXT NULL,
            provider_payload_json TEXT NULL,
            confirmation_payload_json TEXT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            expires_at TEXT NULL,
            paid_at TEXT NULL,
            failed_at TEXT NULL,
            error_text TEXT NULL
        );

        CREATE TABLE IF NOT EXISTS reaction_accounts(
            id BIGSERIAL PRIMARY KEY,
            tenant_id BIGINT NOT NULL,
            session_name TEXT NOT NULL,
            telegram_user_id BIGINT NULL,
            username TEXT NULL,
            phone_hint TEXT NULL,
            is_premium BOOLEAN NOT NULL DEFAULT FALSE,
            status TEXT NOT NULL DEFAULT 'active' CHECK(status IN ('active','disabled','auth_required','flood_wait','limited','error')),
            fixed_reactions_json TEXT NULL,
            last_checked_at TEXT NULL,
            last_error TEXT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NULL
        );

        CREATE TABLE IF NOT EXISTS rule_reaction_settings(
            id BIGSERIAL PRIMARY KEY,
            tenant_id BIGINT NOT NULL,
            rule_id BIGINT NOT NULL,
            enabled BOOLEAN NOT NULL DEFAULT FALSE,
            mode TEXT NOT NULL DEFAULT 'premium_then_normal' CHECK(mode IN ('off','premium_only','normal_only','premium_then_normal','random_pool')),
            preset_json TEXT NULL,
            max_accounts_per_post BIGINT NOT NULL DEFAULT 3,
            delay_min_sec BIGINT NOT NULL DEFAULT 3,
            delay_max_sec BIGINT NOT NULL DEFAULT 30,
            premium_first BOOLEAN NOT NULL DEFAULT TRUE,
            stop_after_premium_success BOOLEAN NOT NULL DEFAULT FALSE,
            created_at TEXT NOT NULL,
            updated_at TEXT NULL
        );

        CREATE TABLE IF NOT EXISTS reaction_jobs(
            id BIGSERIAL PRIMARY KEY,
            tenant_id BIGINT NOT NULL,
            rule_id BIGINT NULL,
            delivery_id BIGINT NULL,
            target_id TEXT NOT NULL,
            message_id BIGINT NOT NULL,
            account_id BIGINT NULL,
            reaction_payload_json TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','leased','processing','done','retry','failed','skipped')),
            attempts BIGINT NOT NULL DEFAULT 0,
            max_attempts BIGINT NOT NULL DEFAULT 3,
            run_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            lease_until TIMESTAMPTZ NULL,
            locked_by TEXT NULL,
            error_text TEXT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS reaction_events(
            id BIGSERIAL PRIMARY KEY,
            reaction_job_id BIGINT NULL,
            tenant_id BIGINT NULL,
            rule_id BIGINT NULL,
            delivery_id BIGINT NULL,
            account_id BIGINT NULL,
            event_type TEXT NOT NULL,
            status TEXT NULL,
            error_text TEXT NULL,
            extra_json TEXT NULL,
            created_at TEXT NOT NULL
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
        ALTER TABLE channels ADD COLUMN IF NOT EXISTS tenant_id BIGINT NULL;
        ALTER TABLE routing ADD COLUMN IF NOT EXISTS tenant_id BIGINT NULL;
        ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS tenant_id BIGINT NULL;
        ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS sent_message_id BIGINT NULL;
        ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS sent_message_ids_json JSONB NULL;
        ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS target_id_snapshot TEXT NULL;
        ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS delivery_method TEXT NULL;
        ALTER TABLE audit_log ADD COLUMN IF NOT EXISTS tenant_id BIGINT NULL;
        ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS grace_started_at TEXT NULL;
        ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS grace_ends_at TEXT NULL;
        ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS canceled_at TEXT NULL;
        ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS pending_plan_id BIGINT NULL;
        ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS current_period_start TEXT NULL;
        ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS current_period_end TEXT NULL;
        ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS updated_at TEXT NULL;
        ALTER TABLE reaction_jobs ADD COLUMN IF NOT EXISTS attempt_count INTEGER NOT NULL DEFAULT 0;
        ALTER TABLE reaction_jobs ADD COLUMN IF NOT EXISTS not_before TIMESTAMPTZ NULL;
        ALTER TABLE reaction_jobs ADD COLUMN IF NOT EXISTS locked_at TIMESTAMPTZ NULL;
        ALTER TABLE reaction_jobs ADD COLUMN IF NOT EXISTS account_ids_json TEXT NULL;
        ALTER TABLE reaction_jobs ADD COLUMN IF NOT EXISTS result_json TEXT NULL;
        ALTER TABLE reaction_jobs ADD COLUMN IF NOT EXISTS processed_at TIMESTAMPTZ NULL;
        """

        self.client.execute_script(init_sql)

        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE channels
                    SET tenant_id = 1
                    WHERE tenant_id IS NULL
                    """
                )
                cur.execute(
                    """
                    DROP INDEX IF EXISTS idx_channels_unique
                    """
                )
                cur.execute(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_channels_unique
                    ON channels(COALESCE(tenant_id, 1), channel_id, COALESCE(thread_id, -1), channel_type)
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
                    CREATE INDEX IF NOT EXISTS idx_payment_intents_invoice_id
                    ON payment_intents(invoice_id, status, id)
                    """
                )
                cur.execute(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_payment_intents_external_id
                    ON payment_intents(external_payment_id)
                    WHERE external_payment_id IS NOT NULL
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
                    CREATE INDEX IF NOT EXISTS idx_reaction_accounts_tenant_status
                    ON reaction_accounts(tenant_id, status)
                    """
                )
                cur.execute(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_reaction_accounts_tenant_session
                    ON reaction_accounts(tenant_id, session_name)
                    """
                )
                cur.execute(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_rule_reaction_settings_tenant_rule
                    ON rule_reaction_settings(tenant_id, rule_id)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_rule_reaction_settings_rule
                    ON rule_reaction_settings(rule_id)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_reaction_jobs_status_run_at
                    ON reaction_jobs(status, run_at)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_reaction_jobs_tenant_status
                    ON reaction_jobs(tenant_id, status)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_reaction_jobs_delivery
                    ON reaction_jobs(delivery_id)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_reaction_jobs_status_not_before
                    ON reaction_jobs(status, not_before, id)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_reaction_jobs_tenant_rule
                    ON reaction_jobs(tenant_id, rule_id, id)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_reaction_jobs_locked
                    ON reaction_jobs(status, locked_at)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_reaction_events_tenant_created
                    ON reaction_events(tenant_id, created_at)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_reaction_events_job
                    ON reaction_events(reaction_job_id)
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

        self._ensure_payment_intents_status_constraint()
        self._ensure_reaction_jobs_status_constraint()
        self._ensure_default_plans()

    def _ensure_payment_intents_status_constraint(self) -> None:
        required_statuses = {
            "created",
            "pending",
            "waiting_confirmation",
            "checkout_opened",
            "paid",
            "failed",
            "canceled",
        }
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT pg_get_constraintdef(c.oid) AS def
                    FROM pg_constraint c
                    JOIN pg_class t ON t.oid = c.conrelid
                    JOIN pg_namespace n ON n.oid = t.relnamespace
                    WHERE c.contype = 'c'
                      AND c.conname = 'payment_intents_status_check'
                      AND t.relname = 'payment_intents'
                    LIMIT 1
                    """
                )
                row = cur.fetchone()
                existing_statuses: set[str] = set()
                if row and row.get("def"):
                    existing_statuses = set(re.findall(r"'([^']+)'", str(row["def"])))

                merged_statuses = sorted(existing_statuses | required_statuses)
                if existing_statuses and "checkout_opened" in existing_statuses:
                    return

                quoted = ", ".join(f"'{status}'" for status in merged_statuses)
                cur.execute("ALTER TABLE payment_intents DROP CONSTRAINT IF EXISTS payment_intents_status_check")
                cur.execute(
                    f"""
                    ALTER TABLE payment_intents
                    ADD CONSTRAINT payment_intents_status_check
                    CHECK (status IN ({quoted}))
                    """
                )
            conn.commit()


    def _ensure_reaction_jobs_status_constraint(self) -> None:
        statuses = ["pending", "leased", "processing", "done", "retry", "failed", "skipped"]
        quoted = ", ".join(f"'{status}'" for status in statuses)

        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("ALTER TABLE reaction_jobs DROP CONSTRAINT IF EXISTS reaction_jobs_status_check")
                cur.execute(
                    f"""
                    ALTER TABLE reaction_jobs
                    ADD CONSTRAINT reaction_jobs_status_check
                    CHECK (status IN ({quoted}))
                    """
                )
            conn.commit()

        logger.info("REACTION_JOBS_STATUS_CONSTRAINT_SYNCED")

    def _ensure_default_plans(self) -> None:
        self.subscription_repo.ensure_default_plans()

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
    # SAAS REACTIONS FOUNDATION
    # =========================================================

    def create_reaction_account(
        self,
        *,
        tenant_id: int,
        session_name: str,
        telegram_user_id: int | None = None,
        username: str | None = None,
        phone_hint: str | None = None,
        is_premium: bool = False,
        fixed_reactions: list[str] | None = None,
        status: str = "active",
    ) -> int | None:
        now_iso = utc_now_iso()
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO reaction_accounts(
                        tenant_id, session_name, telegram_user_id, username, phone_hint,
                        is_premium, status, fixed_reactions_json, created_at
                    )
                    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    RETURNING id
                    """,
                    (tenant_id, session_name, telegram_user_id, username, phone_hint, is_premium, status, _json_dumps(fixed_reactions or []), now_iso),
                )
                row = cur.fetchone()
            conn.commit()
        return int(row["id"]) if row and row.get("id") is not None else None

    def list_reaction_accounts_for_tenant(self, tenant_id: int, active_only: bool = False) -> list[dict[str, Any]]:
        with self.connect() as conn:
            with conn.cursor() as cur:
                if active_only:
                    cur.execute(
                        """
                        SELECT * FROM reaction_accounts
                        WHERE tenant_id = %s AND status = 'active'
                        ORDER BY id ASC
                        """,
                        (tenant_id,),
                    )
                else:
                    cur.execute(
                        """
                        SELECT * FROM reaction_accounts
                        WHERE tenant_id = %s
                        ORDER BY id ASC
                        """,
                        (tenant_id,),
                    )
                rows = cur.fetchall() or []
        return [dict(row) for row in rows]

    def set_reaction_account_status(self, account_id: int, tenant_id: int, status: str, last_error: str | None = None) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE reaction_accounts
                    SET status = %s, last_error = %s, updated_at = %s
                    WHERE id = %s AND tenant_id = %s
                    """,
                    (status, last_error, utc_now_iso(), account_id, tenant_id),
                )
                updated = cur.rowcount > 0
            conn.commit()
        return bool(updated)

    def get_reaction_account_for_tenant(self, tenant_id: int, account_id: int) -> dict[str, Any] | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM reaction_accounts
                    WHERE tenant_id = %s AND id = %s
                    LIMIT 1
                    """,
                    (tenant_id, account_id),
                )
                row = cur.fetchone()
        return dict(row) if row else None

    def update_reaction_account_status_for_tenant(
        self,
        tenant_id: int,
        account_id: int,
        status: str,
        last_error: str | None = None,
    ) -> bool:
        allowed_statuses = {"active", "disabled", "auth_required", "flood_wait", "limited", "error"}
        status_value = str(status or "").strip().lower()
        if status_value not in allowed_statuses:
            return False
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE reaction_accounts
                    SET status = %s,
                        last_error = %s,
                        updated_at = %s
                    WHERE tenant_id = %s AND id = %s
                    """,
                    (status_value, last_error, utc_now_iso(), tenant_id, account_id),
                )
                updated = cur.rowcount > 0
            conn.commit()
        return bool(updated)

    def update_reaction_account_fixed_reactions_for_tenant(
        self,
        tenant_id: int,
        account_id: int,
        fixed_reactions: list[str],
    ) -> bool:
        now_iso = utc_now_iso()
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE reaction_accounts
                    SET fixed_reactions_json = %s,
                        updated_at = %s
                    WHERE tenant_id = %s
                      AND id = %s
                    """,
                    (_json_dumps(fixed_reactions or []), now_iso, tenant_id, account_id),
                )
                updated = cur.rowcount > 0
            conn.commit()
        return bool(updated)

    def delete_reaction_account_for_tenant(self, tenant_id: int, account_id: int) -> dict[str, Any] | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM reaction_accounts
                    WHERE tenant_id = %s AND id = %s
                    LIMIT 1
                    """,
                    (tenant_id, account_id),
                )
                row = cur.fetchone()
                if not row:
                    return None
                deleted_row = dict(row)
                cur.execute(
                    """
                    DELETE FROM reaction_accounts
                    WHERE tenant_id = %s AND id = %s
                    """,
                    (tenant_id, account_id),
                )
            conn.commit()
        return deleted_row

    def get_active_reaction_account_by_telegram_user_for_tenant(self, tenant_id: int, telegram_user_id: int) -> dict[str, Any] | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        id,
                        tenant_id,
                        session_name,
                        telegram_user_id,
                        username,
                        phone_hint,
                        is_premium,
                        status,
                        fixed_reactions_json,
                        created_at,
                        updated_at
                    FROM reaction_accounts
                    WHERE tenant_id = %s
                      AND telegram_user_id = %s
                      AND status = 'active'
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (tenant_id, telegram_user_id),
                )
                row = cur.fetchone()
        return dict(row) if row else None

    def upsert_reaction_account_for_tenant(
        self,
        *,
        tenant_id: int,
        session_name: str,
        telegram_user_id: int | None = None,
        username: str | None = None,
        phone_hint: str | None = None,
        is_premium: bool = False,
        fixed_reactions: list[str] | None = None,
    ) -> int | None:
        now_iso = utc_now_iso()
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO reaction_accounts(
                        tenant_id, session_name, telegram_user_id, username, phone_hint,
                        is_premium, status, fixed_reactions_json, created_at, updated_at
                    )
                    VALUES(%s,%s,%s,%s,%s,%s,'active',%s,%s,%s)
                    ON CONFLICT (tenant_id, session_name)
                    DO UPDATE SET
                        telegram_user_id = EXCLUDED.telegram_user_id,
                        username = EXCLUDED.username,
                        phone_hint = EXCLUDED.phone_hint,
                        is_premium = EXCLUDED.is_premium,
                        status = 'active',
                        fixed_reactions_json = COALESCE(reaction_accounts.fixed_reactions_json, EXCLUDED.fixed_reactions_json),
                        last_error = NULL,
                        updated_at = EXCLUDED.updated_at
                    RETURNING id
                    """,
                    (tenant_id, session_name, telegram_user_id, username, phone_hint, is_premium, _json_dumps(fixed_reactions or []), now_iso, now_iso),
                )
                row = cur.fetchone()
            conn.commit()
        return int(row["id"]) if row and row.get("id") is not None else None

    def upsert_rule_reaction_settings(self, *, tenant_id: int, rule_id: int, enabled: bool, mode: str, preset: dict[str, Any] | list[Any] | None = None, max_accounts_per_post: int = 3, delay_min_sec: int = 3, delay_max_sec: int = 30, premium_first: bool = True, stop_after_premium_success: bool = False) -> int | None:
        now_iso = utc_now_iso()
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO rule_reaction_settings(
                        tenant_id, rule_id, enabled, mode, preset_json, max_accounts_per_post,
                        delay_min_sec, delay_max_sec, premium_first, stop_after_premium_success,
                        created_at, updated_at
                    )
                    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT(tenant_id, rule_id)
                    DO UPDATE SET
                        enabled = EXCLUDED.enabled,
                        mode = EXCLUDED.mode,
                        preset_json = EXCLUDED.preset_json,
                        max_accounts_per_post = EXCLUDED.max_accounts_per_post,
                        delay_min_sec = EXCLUDED.delay_min_sec,
                        delay_max_sec = EXCLUDED.delay_max_sec,
                        premium_first = EXCLUDED.premium_first,
                        stop_after_premium_success = EXCLUDED.stop_after_premium_success,
                        updated_at = EXCLUDED.updated_at
                    RETURNING id
                    """,
                    (tenant_id, rule_id, enabled, mode, _json_dumps(preset), max_accounts_per_post, delay_min_sec, delay_max_sec, premium_first, stop_after_premium_success, now_iso, now_iso),
                )
                row = cur.fetchone()
            conn.commit()
        return int(row["id"]) if row and row.get("id") is not None else None

    def get_rule_reaction_settings_for_tenant(self, tenant_id: int, rule_id: int) -> dict[str, Any] | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT * FROM rule_reaction_settings
                    WHERE tenant_id = %s AND rule_id = %s
                    LIMIT 1
                    """,
                    (tenant_id, rule_id),
                )
                row = cur.fetchone()
        return dict(row) if row else None

    def enqueue_reaction_job(self, *, tenant_id: int, rule_id: int, target_id: str, message_id: int, account_ids: list[int] | None = None, not_before: str | None = None, max_attempts: int = 3, reaction_payload: dict[str, Any] | None = None, delivery_id: int | None = None, account_id: int | None = None, run_at: str | None = None) -> int | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO reaction_jobs(
                        tenant_id, rule_id, delivery_id, target_id, message_id, account_id,
                        reaction_payload_json, status, max_attempts, run_at, account_ids_json, not_before
                    )
                    VALUES(%s,%s,%s,%s,%s,%s,%s,'pending',%s,COALESCE(%s, NOW()),%s,%s)
                    RETURNING id
                    """,
                    (tenant_id, rule_id, delivery_id, target_id, message_id, account_id, _json_dumps(reaction_payload) or "{}", max_attempts, run_at, _json_dumps(account_ids), not_before),
                )
                row = cur.fetchone()
            conn.commit()
        return int(row["id"]) if row and row.get("id") is not None else None

    def lease_due_reaction_job(self, *, worker_id: str, lock_timeout_seconds: int = 300) -> dict[str, Any] | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    WITH candidate AS (
                        SELECT id
                        FROM reaction_jobs
                        WHERE (
                            status = 'pending'
                            AND (not_before IS NULL OR not_before <= NOW())
                        )
                        OR (
                            status = 'processing'
                            AND locked_at < NOW() - make_interval(secs => %s)
                            AND attempt_count < max_attempts
                        )
                        ORDER BY id
                        FOR UPDATE SKIP LOCKED
                        LIMIT 1
                    )
                    UPDATE reaction_jobs j
                    SET status='processing',
                        locked_at=NOW(),
                        locked_by=%s,
                        attempt_count=COALESCE(attempt_count, 0)+1,
                        updated_at=NOW()
                    FROM candidate
                    WHERE j.id = candidate.id
                    RETURNING j.*
                    """,
                    (int(lock_timeout_seconds), worker_id),
                )
                row = cur.fetchone()
            conn.commit()
        return dict(row) if row else None

    def mark_reaction_job_done(self, *, job_id: int, result: dict) -> None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE reaction_jobs SET status='done', processed_at=NOW(), result_json=%s, error_text=NULL, updated_at=NOW() WHERE id=%s",
                    (_json_dumps(result) or "{}", job_id),
                )
            conn.commit()

    def mark_reaction_job_failed(self, *, job_id: int, error_text: str, result: dict | None = None, retry_after_seconds: int | None = None) -> None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                if retry_after_seconds is None:
                    cur.execute(
                        "UPDATE reaction_jobs SET status='failed', processed_at=NOW(), result_json=%s, error_text=%s, updated_at=NOW() WHERE id=%s",
                        (_json_dumps(result), error_text, job_id),
                    )
                else:
                    cur.execute(
                        "UPDATE reaction_jobs SET status='pending', not_before=NOW() + make_interval(secs => %s), result_json=%s, error_text=%s, updated_at=NOW() WHERE id=%s",
                        (int(retry_after_seconds), _json_dumps(result), error_text, job_id),
                    )
            conn.commit()

    def mark_reaction_job_skipped(self, *, job_id: int, result: dict) -> None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE reaction_jobs SET status='skipped', processed_at=NOW(), result_json=%s, updated_at=NOW() WHERE id=%s",
                    (_json_dumps(result) or "{}", job_id),
                )
            conn.commit()

    def log_reaction_event(self, *, event_type: str, reaction_job_id: int | None = None, tenant_id: int | None = None, rule_id: int | None = None, delivery_id: int | None = None, account_id: int | None = None, status: str | None = None, error_text: str | None = None, extra: dict[str, Any] | list[Any] | None = None) -> int | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO reaction_events(
                        reaction_job_id, tenant_id, rule_id, delivery_id, account_id,
                        event_type, status, error_text, extra_json, created_at
                    )
                    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    RETURNING id
                    """,
                    (reaction_job_id, tenant_id, rule_id, delivery_id, account_id, event_type, status, error_text, _json_dumps(extra), utc_now_iso()),
                )
                row = cur.fetchone()
            conn.commit()
        return int(row["id"]) if row and row.get("id") is not None else None

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
            tenant_id = self._ensure_tenant_for_admin_conn(conn, int(added_by))
        return self.add_channel_for_tenant(
            tenant_id=tenant_id,
            channel_id=channel_id,
            thread_id=thread_id,
            channel_type=channel_type,
            title=title,
            added_by=added_by,
        )

    def add_channel_for_tenant(
        self,
        tenant_id: int,
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
                    INSERT INTO channels(
                        channel_id,
                        thread_id,
                        channel_type,
                        title,
                        added_by,
                        added_date,
                        is_active,
                        tenant_id
                    )
                    VALUES(%s, %s, %s, %s, %s, %s, TRUE, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    (str(channel_id), thread_id, channel_type, title, added_by, utc_now_iso(), int(tenant_id)),
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
        - двигает next_run_at с безопасной задержкой
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
            next_retry_iso = datetime.now(timezone.utc) + timedelta(seconds=7)
            next_retry_iso_str = next_retry_iso.isoformat()

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
                        (next_retry_iso_str, rule_id),
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
                                    "next_run_at": next_retry_iso_str,
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
                "next_run_at": next_retry_iso_str,
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

    def get_rules_for_tenant(self, tenant_id: int):
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
                     AND COALESCE(s.tenant_id, 1) = COALESCE(r.tenant_id, 1)
                    LEFT JOIN channels t
                      ON t.channel_id = r.target_id
                     AND ((t.thread_id IS NULL AND r.target_thread_id IS NULL) OR t.thread_id = r.target_thread_id)
                     AND t.channel_type = 'target'
                     AND COALESCE(t.tenant_id, 1) = COALESCE(r.tenant_id, 1)
                    WHERE COALESCE(r.tenant_id, 1) = %s
                    ORDER BY r.created_date, r.id
                    """,
                    (int(tenant_id),),
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

    def get_last_sent_post_for_reaction_test(self, tenant_id: int, rule_id: int) -> dict[str, Any] | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        target_id,
                        COALESCE(
                            NULLIF(extra_json::json->>'reaction_message_id', '')::BIGINT,
                            NULLIF(extra_json::json->>'sent_message_id', '')::BIGINT
                        ) AS message_id
                    FROM audit_log
                    WHERE tenant_id = %s
                      AND rule_id = %s
                      AND event_type = 'delivery_sent'
                      AND target_id IS NOT NULL
                    ORDER BY created_at DESC, id DESC
                    LIMIT 1
                    """,
                    (tenant_id, rule_id),
                )
                row = cur.fetchone()
        if not row:
            return None
        message_id = row.get("message_id")
        if not message_id:
            return None
        return {
            "target_id": str(row["target_id"]),
            "message_id": int(message_id),
        }

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
                next_run_iso = row.get("next_run_at")

                cur.execute(
                    """
                    UPDATE routing
                    SET last_sent_at = %s,
                        next_run_at = COALESCE(next_run_at, %s)
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

    def take_due_delivery_and_create_job(self, rule_id: int, due_iso: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT mode, schedule_mode, interval
                    FROM routing
                    WHERE id = %s
                    FOR UPDATE
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
                interval = int(rule_row["interval"] or 0)

                cur.execute(
                    """
                    SELECT
                        d.id AS delivery_id,
                        d.rule_id,
                        p.id AS post_id,
                        p.message_id,
                        p.source_channel,
                        p.source_thread_id,
                        p.content_json,
                        p.media_group_id,
                        r.target_id,
                        r.target_thread_id,
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
                    FOR UPDATE OF d SKIP LOCKED
                    LIMIT 1
                    """,
                    (rule_id, due_iso, rule_mode, rule_mode),
                )
                due_row = cur.fetchone()
                if not due_row:
                    conn.commit()
                    return None

                delivery_id = int(due_row["delivery_id"])
                tenant_id = int(due_row["tenant_id"] or 1)
                media_group_id = due_row.get("media_group_id")

                payload: dict[str, Any] = {
                    "rule_id": int(due_row["rule_id"]),
                    "delivery_id": delivery_id,
                    "tenant_id": tenant_id,
                    "message_id": int(due_row["message_id"]),
                    "source_channel": str(due_row["source_channel"]),
                    "source_thread_id": due_row["source_thread_id"],
                    "target_id": str(due_row["target_id"]),
                    "target_thread_id": due_row["target_thread_id"],
                    "mode": rule_mode,
                    "interval": interval,
                    "schedule_mode": schedule_mode,
                    "media_group_id": media_group_id,
                }

                job_type = JOB_TYPE_REPOST_SINGLE
                dedup_key = build_dedup_key_for_single(delivery_id)
                delivery_ids_to_take = [delivery_id]

                if rule_mode == "video":
                    job_type = JOB_TYPE_VIDEO_DOWNLOAD
                    dedup_key = build_dedup_key_for_video(delivery_id)
                    payload["job_type"] = JOB_TYPE_VIDEO_DOWNLOAD
                    payload["attempt_stage"] = "download"
                    payload["source_video_path"] = None
                    payload["processed_video_path"] = None
                    payload["thumbnail_path"] = None
                    payload["cleanup_paths"] = []
                    payload["video_file_path"] = None
                    payload["processed_file_path"] = None
                    payload["artifact_version"] = VIDEO_ARTIFACT_VERSION
                    payload["pipeline_version"] = VIDEO_PIPELINE_VERSION
                elif media_group_id:
                    if due_row["source_thread_id"] is None:
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
                            FOR UPDATE OF d SKIP LOCKED
                            """,
                            (rule_id, str(due_row["source_channel"]), str(media_group_id)),
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
                            FOR UPDATE OF d SKIP LOCKED
                            """,
                            (rule_id, str(due_row["source_channel"]), due_row["source_thread_id"], str(media_group_id)),
                        )

                    album_rows = cur.fetchall() or []
                    delivery_ids_to_take = [int(item["delivery_id"]) for item in album_rows]
                    if not delivery_ids_to_take or delivery_id not in delivery_ids_to_take:
                        conn.commit()
                        return None

                    job_type = JOB_TYPE_REPOST_ALBUM
                    dedup_key = build_dedup_key_for_album(int(due_row["rule_id"]), str(media_group_id), delivery_ids_to_take)
                    payload["job_type"] = JOB_TYPE_REPOST_ALBUM
                    payload["delivery_ids"] = delivery_ids_to_take
                    payload["media_group_id"] = media_group_id
                else:
                    payload["job_type"] = JOB_TYPE_REPOST_SINGLE

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
                    return {
                        "status": "duplicate",
                        "delivery_id": delivery_id,
                        "tenant_id": tenant_id,
                        "dedup_key": dedup_key,
                    }

                try:
                    cur.execute(
                        """
                        INSERT INTO jobs(job_type, payload_json, dedup_key, status, priority, queue, run_at, created_at, updated_at)
                        VALUES(%s, %s::jsonb, %s, 'pending', %s, %s, NOW(), NOW(), NOW())
                        RETURNING id
                        """,
                        (
                            job_type,
                            _json_dumps(payload) or "{}",
                            dedup_key,
                            int(JOB_PRIORITY_BY_TYPE.get(job_type, 100)),
                            str(JOB_QUEUE_BY_TYPE.get(job_type, "default")),
                        ),
                    )
                    job_row = cur.fetchone()
                except Exception as exc:
                    if "duplicate key value violates unique constraint" not in str(exc).lower():
                        raise
                    conn.rollback()
                    return {
                        "status": "duplicate",
                        "delivery_id": delivery_id,
                        "tenant_id": tenant_id,
                        "dedup_key": dedup_key,
                    }

                cur.executemany(
                    """
                    UPDATE deliveries
                    SET status = 'processing'
                    WHERE id = %s
                      AND status = 'pending'
                    """,
                    [(item_id,) for item_id in delivery_ids_to_take],
                )
                cur.execute(
                    """
                    SELECT COUNT(*) AS cnt
                    FROM deliveries
                    WHERE id = ANY(%s)
                      AND status = 'processing'
                    """,
                    (delivery_ids_to_take,),
                )
                taken_count_row = cur.fetchone()
                taken_count = int(taken_count_row["cnt"] or 0) if taken_count_row else 0
                if taken_count != len(delivery_ids_to_take):
                    conn.rollback()
                    return None

                reserved_next_run_at = None
                if schedule_mode == "fixed":
                    reserved_next_run_at = self._compute_next_run_for_rule_conn(conn, int(rule_id))
                else:
                    reserve_from = datetime.now(timezone.utc) + timedelta(seconds=max(interval, 1))
                    reserved_next_run_at = self._find_next_interval_slot(conn, reserve_from, exclude_rule_id=int(rule_id))
                cur.execute(
                    """
                    UPDATE routing
                    SET next_run_at = %s
                    WHERE id = %s
                    """,
                    (reserved_next_run_at, int(rule_id)),
                )

            conn.commit()
            return {
                "status": "created",
                "job_id": int(job_row["id"]) if job_row else None,
                "delivery_id": delivery_id,
                "tenant_id": tenant_id,
                "dedup_key": dedup_key,
                "job_type": job_type,
                "reserved_next_run_at": reserved_next_run_at,
            }

    # =========================================================
    # DELIVERY STATUS
    # =========================================================

    def mark_delivery_sent_with_target_message(self, delivery_id: int, *, sent_message_id: int | None, sent_message_ids: list[int] | None, target_id: str | None, delivery_method: str | None):
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE deliveries
                    SET status = 'sent',
                        sent_at = %s,
                        error_text = NULL,
                        sent_message_id = %s,
                        sent_message_ids_json = %s,
                        target_id_snapshot = %s,
                        delivery_method = %s
                    WHERE id = %s
                    """,
                    (utc_now_iso(), sent_message_id, _json_dumps(sent_message_ids or []), target_id, delivery_method, delivery_id),
                )
            conn.commit()

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

    def get_channels_for_tenant(self, tenant_id: int, channel_type: str | None = None):
        with self.connect() as conn:
            with conn.cursor() as cur:
                if channel_type:
                    cur.execute(
                        """
                        SELECT channel_id, thread_id, title, channel_type
                        FROM channels
                        WHERE channel_type = %s
                          AND is_active = TRUE
                          AND COALESCE(tenant_id, 1) = %s
                        ORDER BY added_date
                        """,
                        (channel_type, int(tenant_id)),
                    )
                else:
                    cur.execute(
                        """
                        SELECT channel_id, thread_id, title, channel_type
                        FROM channels
                        WHERE is_active = TRUE
                          AND COALESCE(tenant_id, 1) = %s
                        ORDER BY channel_type, added_date
                        """,
                        (int(tenant_id),),
                    )
                return cur.fetchall()

    def remove_channel_for_tenant(
        self,
        tenant_id: int,
        channel_id: str,
        thread_id: int | None,
        channel_type: str | None = None,
    ) -> bool:
        channel_id = str(channel_id)

        with self.connect() as conn:
            with conn.cursor() as cur:
                params: list[Any] = [channel_id, int(tenant_id)]
                q = "DELETE FROM channels WHERE channel_id = %s AND COALESCE(tenant_id, 1) = %s"

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
                if not removed:
                    conn.commit()
                    return False

                if thread_id is None:
                    cur.execute(
                        """
                        DELETE FROM routing
                        WHERE COALESCE(tenant_id, 1) = %s
                          AND ((source_id = %s AND source_thread_id IS NULL)
                            OR (target_id = %s AND target_thread_id IS NULL))
                        """,
                        (int(tenant_id), channel_id, channel_id),
                    )
                else:
                    cur.execute(
                        """
                        DELETE FROM routing
                        WHERE COALESCE(tenant_id, 1) = %s
                          AND ((source_id = %s AND source_thread_id = %s)
                            OR (target_id = %s AND target_thread_id = %s))
                        """,
                        (int(tenant_id), channel_id, thread_id, channel_id, thread_id),
                    )

                if channel_type in (None, "source"):
                    if thread_id is None:
                        cur.execute(
                            """
                            DELETE FROM posts
                            WHERE source_channel = %s
                              AND source_thread_id IS NULL
                              AND NOT EXISTS (
                                SELECT 1
                                FROM channels c
                                WHERE c.channel_type = 'source'
                                  AND c.channel_id = posts.source_channel
                                  AND c.thread_id IS NULL
                                  AND c.is_active = TRUE
                              )
                            """,
                            (channel_id,),
                        )
                    else:
                        cur.execute(
                            """
                            DELETE FROM posts
                            WHERE source_channel = %s
                              AND source_thread_id = %s
                              AND NOT EXISTS (
                                SELECT 1
                                FROM channels c
                                WHERE c.channel_type = 'source'
                                  AND c.channel_id = posts.source_channel
                                  AND c.thread_id = posts.source_thread_id
                                  AND c.is_active = TRUE
                              )
                            """,
                            (channel_id, thread_id),
                        )

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
            return True

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

    def lease_jobs_for_tenant(
        self,
        tenant_id: int,
        queue: str,
        worker_id: str,
        limit: int = 1,
        lease_seconds: int = 30,
    ) -> list[dict]:
        queue_name = (queue or "").strip().lower()
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    WITH picked AS (
                        SELECT j.id
                        FROM jobs j
                        WHERE j.queue = %s
                          AND {_JOB_TENANT_SQL} = %s
                          AND j.status IN ('pending', 'retry')
                          AND j.run_at <= NOW()
                          AND (j.lease_until IS NULL OR j.lease_until < NOW())
                        ORDER BY j.priority ASC, j.run_at ASC, j.created_at ASC
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
                    (
                        queue_name,
                        int(tenant_id),
                        max(1, int(limit)),
                        worker_id,
                        max(1, int(lease_seconds)),
                    ),
                )
                rows = cur.fetchall() or []
            conn.commit()
        return [dict(r) for r in rows]

    def lease_fair_jobs(self, queue: str, worker_id: str, limit: int = 1, lease_seconds: int = 30) -> list[dict]:
        queue_name = (queue or "").strip().lower()
        target_limit = max(1, int(limit))
        fair_service = TenantFairnessService(self)
        leased: list[dict] = []
        picked_tenant_ids: set[int] = set()

        for _ in range(target_limit):
            pending_map = self.get_tenant_job_counts(queue=queue_name)
            if not pending_map:
                break

            processing_map = self.get_tenant_processing_counts(queue=queue_name)
            retry_map = self.get_tenant_retry_counts(queue=queue_name)
            oldest_map = self.get_tenant_oldest_pending_ages(queue=queue_name)
            candidates: list[tuple[float, int]] = []

            for tenant_id_raw, pending_count in pending_map.items():
                tenant_id = int(tenant_id_raw)
                if int(pending_count) <= 0:
                    continue
                processing = int(processing_map.get(tenant_id) or 0)
                retry = int(retry_map.get(tenant_id) or 0)
                oldest_age = int(oldest_map.get(tenant_id) or 0)
                score = fair_service.compute_fairness_score(
                    tenant_id=tenant_id,
                    pending=int(pending_count),
                    processing=processing,
                    retry=retry,
                    oldest_pending_age_sec=oldest_age,
                )
                if tenant_id in picked_tenant_ids:
                    score -= 20.0
                candidates.append((score, tenant_id))

            if not candidates:
                break

            candidates.sort(key=lambda item: (-item[0], item[1]))
            selected_tenant = int(candidates[0][1])
            batch = self.lease_jobs_for_tenant(
                selected_tenant,
                queue_name,
                worker_id,
                limit=1,
                lease_seconds=lease_seconds,
            )
            if not batch:
                # tenant мог быть перехвачен конкурентным worker; пробуем следующий слот
                continue
            leased.extend(batch)
            picked_tenant_ids.add(selected_tenant)

        return leased

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

    def get_tenant_job_counts(self, queue: str | None = None) -> dict[int, int]:
        queue_name = str(queue or "").strip().lower()
        use_queue_filter = queue_name in {"light", "heavy"}
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                        {_JOB_TENANT_SQL} AS tenant_id,
                        COUNT(*) AS cnt
                    FROM jobs j
                    WHERE j.status IN ('pending', 'retry')
                      AND (%s = FALSE OR j.queue = %s)
                    GROUP BY {_JOB_TENANT_SQL}
                    """,
                    (use_queue_filter, queue_name),
                )
                rows = cur.fetchall() or []
            conn.commit()
        return {int(row["tenant_id"]): int(row.get("cnt") or 0) for row in rows}

    def get_tenant_oldest_pending_ages(self, queue: str | None = None) -> dict[int, int]:
        queue_name = str(queue or "").strip().lower()
        use_queue_filter = queue_name in {"light", "heavy"}
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                        {_JOB_TENANT_SQL} AS tenant_id,
                        EXTRACT(EPOCH FROM (NOW() - MIN(j.created_at)))::BIGINT AS age_sec
                    FROM jobs j
                    WHERE j.status IN ('pending', 'retry')
                      AND (%s = FALSE OR j.queue = %s)
                    GROUP BY {_JOB_TENANT_SQL}
                    """,
                    (use_queue_filter, queue_name),
                )
                rows = cur.fetchall() or []
            conn.commit()
        return {int(row["tenant_id"]): max(int(row.get("age_sec") or 0), 0) for row in rows}

    def get_tenant_processing_counts(self, queue: str | None = None) -> dict[int, int]:
        queue_name = str(queue or "").strip().lower()
        use_queue_filter = queue_name in {"light", "heavy"}
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                        {_JOB_TENANT_SQL} AS tenant_id,
                        COUNT(*) AS cnt
                    FROM jobs j
                    WHERE j.status IN ('leased', 'processing')
                      AND (%s = FALSE OR j.queue = %s)
                    GROUP BY {_JOB_TENANT_SQL}
                    """,
                    (use_queue_filter, queue_name),
                )
                rows = cur.fetchall() or []
            conn.commit()
        return {int(row["tenant_id"]): int(row.get("cnt") or 0) for row in rows}

    def get_tenant_retry_counts(self, queue: str | None = None) -> dict[int, int]:
        queue_name = str(queue or "").strip().lower()
        use_queue_filter = queue_name in {"light", "heavy"}
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                        {_JOB_TENANT_SQL} AS tenant_id,
                        COUNT(*) AS cnt
                    FROM jobs j
                    WHERE j.status = 'retry'
                      AND (%s = FALSE OR j.queue = %s)
                    GROUP BY {_JOB_TENANT_SQL}
                    """,
                    (use_queue_filter, queue_name),
                )
                rows = cur.fetchall() or []
            conn.commit()
        return {int(row["tenant_id"]): int(row.get("cnt") or 0) for row in rows}

    def get_tenant_throughput_snapshot(self, *, window_minutes: int = 15, queue: str | None = None) -> dict[int, int]:
        queue_name = str(queue or "").strip().lower()
        use_queue_filter = queue_name in {"light", "heavy"}
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                        {_JOB_TENANT_SQL} AS tenant_id,
                        COUNT(*) AS cnt
                    FROM jobs j
                    WHERE j.status = 'done'
                      AND j.updated_at >= NOW() - make_interval(mins => %s)
                      AND (%s = FALSE OR j.queue = %s)
                    GROUP BY {_JOB_TENANT_SQL}
                    """,
                    (max(1, int(window_minutes)), use_queue_filter, queue_name),
                )
                rows = cur.fetchall() or []
            conn.commit()
        return {int(row["tenant_id"]): int(row.get("cnt") or 0) for row in rows}

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

    def get_recoverable_summary_for_tenant(self, tenant_id: int) -> dict[str, Any]:
        return self.usage_repo.get_recoverable_summary_for_tenant(tenant_id=tenant_id)

    def recover_blocked_jobs_for_tenant(self, tenant_id: int) -> int:
        return self.usage_repo.recover_blocked_jobs_for_tenant(tenant_id=tenant_id)

    def recover_pending_deliveries_for_tenant(self, tenant_id: int) -> int:
        return self.usage_repo.recover_pending_deliveries_for_tenant(tenant_id=tenant_id)

    def get_recent_limit_events_for_tenant(self, tenant_id: int, limit: int = 10) -> list[dict[str, Any]]:
        return self.usage_repo.get_recent_limit_events_for_tenant(tenant_id=tenant_id, limit=limit)

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
        return self.tenant_repo.create_tenant(owner_admin_id=owner_admin_id, name=name)

    def get_tenant_by_admin(self, admin_id: int) -> dict[str, Any] | None:
        return self.tenant_repo.get_tenant_by_admin(admin_id=admin_id)

    def get_default_tenant(self) -> dict[str, Any] | None:
        return self.tenant_repo.get_default_tenant()

    def get_tenant_by_id(self, tenant_id: int) -> dict[str, Any] | None:
        return self.tenant_repo.get_tenant_by_id(tenant_id=tenant_id)

    def set_tenant_active(self, tenant_id: int, is_active: bool) -> bool:
        return self.tenant_repo.set_tenant_active(tenant_id=tenant_id, is_active=is_active)

    def add_tenant_user(self, tenant_id: int, telegram_id: int, role: str) -> bool:
        return self.tenant_repo.add_tenant_user(tenant_id=tenant_id, telegram_id=telegram_id, role=role)

    def get_tenant_user_role(self, tenant_id: int, telegram_id: int) -> str | None:
        return self.tenant_repo.get_tenant_user_role(tenant_id=tenant_id, telegram_id=telegram_id)

    def get_plan_by_name(self, plan_name: str) -> dict[str, Any] | None:
        return self.subscription_repo.get_plan_by_name(plan_name=plan_name)

    def assign_subscription(self, tenant_id: int, plan_id: int, *, status: str = "active", expires_at: str | None = None) -> int | None:
        return self.subscription_repo.assign_subscription(tenant_id=tenant_id, plan_id=plan_id, status=status, expires_at=expires_at)

    def get_active_subscription(self, tenant_id: int) -> dict[str, Any] | None:
        return self.subscription_repo.get_active_subscription(tenant_id=tenant_id)

    def get_latest_subscription(self, tenant_id: int) -> dict[str, Any] | None:
        return self.subscription_repo.get_latest_subscription(tenant_id=tenant_id)

    def get_subscription_by_id(self, subscription_id: int) -> dict[str, Any] | None:
        return self.subscription_repo.get_subscription_by_id(subscription_id=subscription_id)

    def expire_subscription(self, tenant_id: int) -> bool:
        return self.subscription_repo.expire_subscription(tenant_id=tenant_id)

    def set_subscription_status(self, subscription_id: int, new_status: str) -> bool:
        return self.subscription_repo.set_subscription_status(subscription_id=subscription_id, new_status=new_status)

    def set_subscription_grace_window(self, subscription_id: int, grace_started_at: str, grace_ends_at: str) -> bool:
        return self.subscription_repo.set_subscription_grace_window(
            subscription_id=subscription_id,
            grace_started_at=grace_started_at,
            grace_ends_at=grace_ends_at,
        )

    def set_subscription_pending_plan(self, subscription_id: int, pending_plan_id: int) -> bool:
        return self.subscription_repo.set_subscription_pending_plan(subscription_id=subscription_id, pending_plan_id=pending_plan_id)

    def replace_subscription_plan(self, subscription_id: int, plan_id: int) -> bool:
        return self.subscription_repo.replace_subscription_plan(subscription_id=subscription_id, plan_id=plan_id)

    def bump_usage(self, tenant_id: int, *, jobs_delta: int = 0, video_delta: int = 0, storage_delta_mb: int = 0, api_calls_delta: int = 0) -> None:
        self.usage_repo.bump_usage(
            tenant_id=tenant_id,
            jobs_delta=jobs_delta,
            video_delta=video_delta,
            storage_delta_mb=storage_delta_mb,
            api_calls_delta=api_calls_delta,
        )

    def get_usage_for_date(self, tenant_id: int, day: str) -> dict[str, Any]:
        return self.usage_repo.get_usage_for_date(tenant_id=tenant_id, day=day)

    def get_usage_for_period(self, tenant_id: int, date_from: str, date_to: str) -> dict[str, Any]:
        return self.usage_repo.get_usage_for_period(tenant_id=tenant_id, date_from=date_from, date_to=date_to)

    def build_billing_usage_data(self, tenant_id: int, period_start: str, period_end: str) -> dict[str, Any]:
        return self.usage_repo.build_billing_usage_data(tenant_id=tenant_id, period_start=period_start, period_end=period_end)

    def reset_usage_for_day(self, day: str) -> int:
        return self.usage_repo.reset_usage_for_day(day=day)

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
        return self.subscription_repo.add_subscription_history(
            tenant_id=tenant_id,
            old_plan_id=old_plan_id,
            new_plan_id=new_plan_id,
            old_status=old_status,
            new_status=new_status,
            changed_by=changed_by,
            reason=reason,
            effective_from=effective_from,
            effective_to=effective_to,
        )

    def get_subscription_history(self, tenant_id: int, limit: int = 20) -> list[dict[str, Any]]:
        return self.subscription_repo.get_subscription_history(tenant_id=tenant_id, limit=limit)

    def get_subscriptions_due_for_billing(self, due_before: str, limit: int = 100) -> list[dict[str, Any]]:
        return self.subscription_repo.get_subscriptions_due_for_billing(due_before=due_before, limit=limit)

    def update_billing_period(self, subscription_id: int, period_start: str, period_end: str) -> bool:
        return self.subscription_repo.update_billing_period(
            subscription_id=subscription_id,
            period_start=period_start,
            period_end=period_end,
        )

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
        return self.billing_repo.create_billing_event(
            tenant_id=tenant_id,
            event_type=event_type,
            event_source=event_source,
            amount=amount,
            currency=currency,
            metadata=metadata,
        )

    def get_billing_events(self, tenant_id: int, limit: int = 20) -> list[dict[str, Any]]:
        return self.billing_repo.get_billing_events(tenant_id=tenant_id, limit=limit)

    def get_billing_exchange_rates(self) -> dict[str, float]:
        return self.billing_repo.get_billing_exchange_rates()

    def set_billing_exchange_rate(self, *, currency: str, new_value: float, admin_id: int | None = None) -> bool:
        return self.billing_repo.set_billing_exchange_rate(currency=currency, new_value=new_value, admin_id=admin_id)

    def get_billing_usd_prices(self) -> dict[str, dict[int, float]]:
        return self.billing_repo.get_billing_usd_prices()

    def set_billing_usd_price(self, *, tariff_code: str, period_months: int, new_price: float, admin_id: int | None = None) -> bool:
        return self.billing_repo.set_billing_usd_price(
            tariff_code=tariff_code,
            period_months=period_months,
            new_price=new_price,
            admin_id=admin_id,
        )

    def get_billing_fixed_prices(self, kind: str) -> dict[str, dict[int, dict[str, Any]]]:
        return self.billing_repo.get_billing_fixed_prices(kind)

    def set_billing_fixed_price(self, *, kind: str, tariff_code: str, period_months: int, value: Any, admin_id: int | None = None) -> bool:
        return self.billing_repo.set_billing_fixed_price(
            kind=kind,
            tariff_code=tariff_code,
            period_months=period_months,
            value=value,
            admin_id=admin_id,
        )

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
        return self.billing_repo.create_invoice(
            tenant_id=tenant_id,
            subscription_id=subscription_id,
            period_start=period_start,
            period_end=period_end,
            status=status,
            currency=currency,
            due_at=due_at,
        )

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
        return self.billing_repo.add_invoice_item(
            invoice_id=invoice_id,
            item_type=item_type,
            description=description,
            quantity=quantity,
            unit_price=unit_price,
            amount=amount,
            metadata=metadata,
        )

    def recalculate_invoice_totals(self, invoice_id: int) -> bool:
        return self.billing_repo.recalculate_invoice_totals(invoice_id=invoice_id)

    def set_invoice_status(
        self,
        invoice_id: int,
        status: str,
        *,
        updated_at: str | None = None,
        paid_at: str | None = None,
        external_reference: str | None = None,
    ) -> bool:
        return self.billing_repo.set_invoice_status(
            invoice_id=invoice_id,
            status=status,
            updated_at=updated_at,
            paid_at=paid_at,
            external_reference=external_reference,
        )

    def get_invoice(self, invoice_id: int) -> dict[str, Any] | None:
        return self.billing_repo.get_invoice(invoice_id=invoice_id)

    def get_last_invoice(self, tenant_id: int) -> dict[str, Any] | None:
        return self.billing_repo.get_last_invoice(tenant_id=tenant_id)

    def list_invoices_for_tenant(self, tenant_id: int, limit: int = 10) -> list[dict[str, Any]]:
        return self.billing_repo.list_invoices_for_tenant(tenant_id=tenant_id, limit=limit)

    def count_open_invoices(self, tenant_id: int) -> int:
        return self.billing_repo.count_open_invoices(tenant_id=tenant_id)

    def get_invoice_for_period(self, tenant_id: int, period_start: str, period_end: str) -> dict[str, Any] | None:
        return self.billing_repo.get_invoice_for_period(
            tenant_id=tenant_id,
            period_start=period_start,
            period_end=period_end,
        )

    def list_invoice_items(self, invoice_id: int) -> list[dict[str, Any]]:
        return self.billing_repo.list_invoice_items(invoice_id=invoice_id)

    def count_invoices_by_status(self, status: str) -> int:
        return self.billing_repo.count_invoices_by_status(status=status)

    def get_billing_periods_due(self, now_iso: str) -> int:
        return self.billing_repo.get_billing_periods_due(now_iso=now_iso)

    def count_tenants_with_overage_current_period(self) -> int:
        return self.billing_repo.count_tenants_with_overage_current_period()

    def create_payment_intent(
        self,
        *,
        tenant_id: int,
        invoice_id: int,
        provider: str,
        status: str,
        amount: float,
        currency: str,
        expires_at: str | None = None,
    ) -> int | None:
        return self.payment_repo.create_payment_intent(
            tenant_id=tenant_id,
            invoice_id=invoice_id,
            provider=provider,
            status=status,
            amount=amount,
            currency=currency,
            expires_at=expires_at,
        )

    def get_payment_intent(self, payment_intent_id: int) -> dict[str, Any] | None:
        return self.payment_repo.get_payment_intent(payment_intent_id=payment_intent_id)

    def get_payment_intent_by_invoice(self, invoice_id: int) -> dict[str, Any] | None:
        return self.payment_repo.get_payment_intent_by_invoice(invoice_id=invoice_id)

    def get_active_payment_intent_for_invoice_provider(self, invoice_id: int, provider: str) -> dict[str, Any] | None:
        return self.payment_repo.get_active_payment_intent_for_invoice_provider(invoice_id=invoice_id, provider=provider)

    def get_active_manual_payment_intent_for_invoice(self, invoice_id: int) -> dict[str, Any] | None:
        return self.payment_repo.get_active_manual_payment_intent_for_invoice(invoice_id=invoice_id)

    def get_payment_intent_by_external_id(self, external_payment_id: str) -> dict[str, Any] | None:
        return self.payment_repo.get_payment_intent_by_external_id(external_payment_id=external_payment_id)

    def update_payment_intent_status(self, payment_intent_id: int, status: str, *, error_text: str | None = None) -> bool:
        return self.payment_repo.update_payment_intent_status(payment_intent_id=payment_intent_id, status=status, error_text=error_text)

    def attach_checkout_url(self, payment_intent_id: int, checkout_url: str, *, external_payment_id: str | None = None) -> bool:
        return self.payment_repo.attach_checkout_url(
            payment_intent_id=payment_intent_id,
            checkout_url=checkout_url,
            external_payment_id=external_payment_id,
        )

    def attach_provider_payload(self, payment_intent_id: int, payload: dict[str, Any]) -> bool:
        return self.payment_repo.attach_provider_payload(payment_intent_id=payment_intent_id, payload=payload)

    def attach_external_payment_id(self, payment_intent_id: int, external_payment_id: str) -> bool:
        return self.payment_repo.attach_external_payment_id(payment_intent_id=payment_intent_id, external_payment_id=external_payment_id)

    def mark_payment_paid(self, payment_intent_id: int, *, confirmation_payload: dict[str, Any] | None = None) -> bool:
        return self.payment_repo.mark_payment_paid(payment_intent_id=payment_intent_id, confirmation_payload=confirmation_payload)

    def mark_payment_failed(self, payment_intent_id: int, error_text: str, *, payload: dict[str, Any] | None = None) -> bool:
        return self.payment_repo.mark_payment_failed(payment_intent_id=payment_intent_id, error_text=error_text, payload=payload)

    def list_payment_intents_for_tenant(self, tenant_id: int, limit: int = 20) -> list[dict[str, Any]]:
        return self.payment_repo.list_payment_intents_for_tenant(tenant_id=tenant_id, limit=limit)

    def attach_confirmation_payload(self, payment_intent_id: int, payload: dict[str, Any]) -> bool:
        return self.payment_repo.attach_confirmation_payload(payment_intent_id=payment_intent_id, payload=payload)

    def count_rules_for_tenant(self, tenant_id: int) -> int:
        return self.usage_repo.count_rules_for_tenant(tenant_id=tenant_id)

    def get_rule_tenant_id(self, rule_id: int) -> int:
        return self.usage_repo.get_rule_tenant_id(rule_id=rule_id)

    def get_saas_health_snapshot(self) -> dict[str, Any]:
        return self.usage_repo.get_saas_health_snapshot()

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
