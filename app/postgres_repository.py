from __future__ import annotations

import json
import logging
import re
import uuid
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
from app.rescan_safety import RescanBlocked
from app.repository import RepositoryProtocol
from app.rule_folders_repository import RuleFoldersRepositoryMixin
from app.repost_campaign_top_time import normalize_repost_campaign_top_time_settings
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

CAMPAIGN_INVITE_LINK_DEFAULT_APPEND_TEMPLATE = "👉 Подписаться: {invite_link}"
CAMPAIGN_INVITE_LINK_ALLOWED_LINK_MODES = {"join_request", "direct_join"}
CAMPAIGN_INVITE_LINK_ALLOWED_INJECTION_MODES = {"placeholder", "append_footer", "disabled"}
CAMPAIGN_INVITE_LINK_ALLOWED_STATUSES = {"active", "archived", "revoked", "created_not_sent", "failed"}
CAMPAIGN_INVITE_LINK_ALLOWED_EVENT_TYPES = {
    "join_request_created",
    "member_joined",
    "member_left",
    "member_kicked",
    "member_unknown",
}


def _default_campaign_invite_link_settings(rule_id: int) -> dict[str, Any]:
    return {
        "rule_id": int(rule_id),
        "enabled": False,
        "destination_chat_id": None,
        "destination_chat_title": None,
        "link_mode": "join_request",
        "injection_mode": "placeholder",
        "append_template": CAMPAIGN_INVITE_LINK_DEFAULT_APPEND_TEMPLATE,
        "per_target_links_enabled": True,
        "preview_required": True,
        "preview_checked_at": None,
        "preview_checked_by": None,
    }


class PostgresRepository(RuleFoldersRepositoryMixin, RepositoryProtocol):
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

    def close_all_connections(self) -> None:
        self.client.close_all_connections()

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
            video_clip_duration_seconds=int(data.get("video_clip_duration_seconds") or 118),
            video_add_intro=bool(data.get("video_add_intro") or 0),
            video_intro_horizontal=data.get("video_intro_horizontal"),
            video_intro_vertical=data.get("video_intro_vertical"),
            video_intro_horizontal_id=data.get("video_intro_horizontal_id"),
            video_intro_vertical_id=data.get("video_intro_vertical_id"),
            video_caption=data.get("video_caption"),
            video_caption_entities_json=data.get("video_caption_entities_json"),
            repost_caption=data.get("repost_caption"),
            repost_caption_entities_json=data.get("repost_caption_entities_json"),
            repost_caption_enabled=bool(data.get("repost_caption_enabled", False)),
            repost_caption_delivery_mode=data.get("repost_caption_delivery_mode") or "copy_first",
            caption_delivery_mode=data.get("caption_delivery_mode", "auto"),
            video_caption_delivery_mode=data.get("video_caption_delivery_mode", "copy_first"),
            repost_campaign_enabled=bool(data.get("repost_campaign_enabled") or False),
            repost_campaign_clean_channel_enabled=bool(
                data.get("repost_campaign_clean_channel_enabled")
                if data.get("repost_campaign_clean_channel_enabled") is not None
                else True
            ),
            repost_campaign_top_time_enabled=bool(data.get("repost_campaign_top_time_enabled") or False),
            repost_campaign_top_time_seconds=int(data.get("repost_campaign_top_time_seconds") or 0),
            repost_campaign_show_seconds=int(data.get("repost_campaign_show_seconds") or 0),
            repost_campaign_saved_post_id=(int(data.get("repost_campaign_saved_post_id")) if data.get("repost_campaign_saved_post_id") is not None else None),
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
            video_clip_duration_seconds INTEGER DEFAULT 118,
            video_add_intro BOOLEAN DEFAULT FALSE,
            video_intro_horizontal TEXT NULL,
            video_intro_vertical TEXT NULL,
            video_caption TEXT NULL,
            video_caption_entities_json TEXT NULL,
            repost_caption TEXT NULL,
            repost_caption_entities_json TEXT NULL,
            repost_caption_enabled BOOLEAN NOT NULL DEFAULT FALSE,
            repost_caption_delivery_mode TEXT NOT NULL DEFAULT 'copy_first',
            caption_delivery_mode TEXT NOT NULL DEFAULT 'auto',
            video_caption_delivery_mode TEXT NOT NULL DEFAULT 'copy_first',

            created_by BIGINT NULL,
            created_date TEXT NOT NULL,
            is_active BOOLEAN NOT NULL DEFAULT TRUE,
            rescan_owner_token TEXT NULL,
            rescan_started_at TIMESTAMPTZ NULL,
            rescan_queue_built BOOLEAN NOT NULL DEFAULT FALSE,
            rescan_confirmation_token TEXT NULL,
            rescan_confirmation_actor BIGINT NULL,
            rescan_confirmation_until TIMESTAMPTZ NULL,
            next_run_at TEXT NULL,
            last_sent_at TEXT NULL,

            video_intro_horizontal_id BIGINT NULL,
            video_intro_vertical_id BIGINT NULL,

            repost_campaign_enabled BOOLEAN NOT NULL DEFAULT FALSE,
            repost_campaign_clean_channel_enabled BOOLEAN NOT NULL DEFAULT TRUE,
            repost_campaign_top_time_enabled BOOLEAN NOT NULL DEFAULT FALSE,
            repost_campaign_top_time_seconds INTEGER NOT NULL DEFAULT 0,
            repost_campaign_show_seconds BIGINT NOT NULL DEFAULT 0,
            repost_campaign_saved_post_id BIGINT NULL
        );

        -- Organisation only: never used by worker/scheduler logic.
        CREATE TABLE IF NOT EXISTS rule_folders (
            id BIGSERIAL PRIMARY KEY,
            tenant_id BIGINT NOT NULL,
            name TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_rule_folders_tenant_name
            ON rule_folders (tenant_id, lower(name));
        CREATE TABLE IF NOT EXISTS rule_folder_assignments (
            rule_id BIGINT PRIMARY KEY REFERENCES routing(id) ON DELETE CASCADE,
            tenant_id BIGINT NOT NULL,
            folder_id BIGINT NOT NULL REFERENCES rule_folders(id) ON DELETE CASCADE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_rule_folder_assignments_folder
            ON rule_folder_assignments (tenant_id, folder_id);

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
            created_at TEXT NOT NULL,
            rule_id BIGINT NULL,
            tenant_id BIGINT NOT NULL DEFAULT 1,
            created_by BIGINT NULL,
            media_kind TEXT NULL,
            bank TEXT NULL,
            status TEXT NOT NULL DEFAULT 'active',
            deleted_at TIMESTAMPTZ NULL,
            updated_at TIMESTAMPTZ NULL
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
        CREATE TABLE IF NOT EXISTS delivery_attempts (
            id BIGSERIAL PRIMARY KEY,
            delivery_id BIGINT NOT NULL,
            rule_id BIGINT NOT NULL,
            tenant_id BIGINT NOT NULL DEFAULT 1,
            job_id BIGINT NULL,
            idempotency_key TEXT NOT NULL,
            operation_kind TEXT NOT NULL,
            status TEXT NOT NULL CHECK(status IN ('created','sending','accepted','verified','failed_before_send','failed_after_send')),
            telegram_method TEXT NULL,
            target_id TEXT NULL,
            source_message_ids_json JSONB NULL,
            sent_message_ids_json JSONB NULL,
            error_text TEXT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE(idempotency_key)
        );

        CREATE TABLE IF NOT EXISTS telegram_transport_cooldowns (
            scope_key TEXT PRIMARY KEY,
            blocked_until TIMESTAMPTZ NOT NULL
        );

        CREATE TABLE IF NOT EXISTS mtproto_send_intents (
            attempt_id BIGINT NOT NULL REFERENCES delivery_attempts(id) ON DELETE CASCADE,
            ordinal INTEGER NOT NULL CHECK(ordinal >= 0),
            fingerprint TEXT NOT NULL,
            random_ids BIGINT[] NOT NULL CHECK(cardinality(random_ids) BETWEEN 1 AND 100),
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY(attempt_id, ordinal)
        );

        -- MTProto scheduled IDs and published IDs are distinct namespaces.
        CREATE TABLE IF NOT EXISTS telegram_publication_mappings (
            sender_account_id BIGINT NOT NULL CHECK(sender_account_id > 0),
            target_peer_id BIGINT NOT NULL CHECK(target_peer_id <> 0),
            scheduled_id BIGINT NOT NULL CHECK(scheduled_id > 0),
            published_id BIGINT NOT NULL CHECK(published_id > 0),
            first_observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (sender_account_id, target_peer_id, scheduled_id, published_id)
        );
        CREATE INDEX IF NOT EXISTS idx_telegram_publication_mappings_published
            ON telegram_publication_mappings(sender_account_id, target_peer_id, published_id);

        -- Independent read-only difference cursor; never advance it before
        -- the exact publication mappings returned by Telegram are durable.
        CREATE TABLE IF NOT EXISTS telegram_publication_inbox_incidents (
            sender_account_id BIGINT PRIMARY KEY CHECK(sender_account_id>0),
            blocked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            last_peer_id BIGINT NULL,
            last_scheduled_ids BIGINT[] NULL,
            acknowledged_at TIMESTAMPTZ NULL,
            review_note TEXT NULL
        );
        CREATE TABLE IF NOT EXISTS telegram_publication_recovery_cursor (
            sender_account_id BIGINT PRIMARY KEY CHECK(sender_account_id > 0),
            pts BIGINT NOT NULL CHECK(pts >= 0),
            qts BIGINT NOT NULL CHECK(qts >= 0),
            state_date TIMESTAMPTZ NOT NULL,
            version BIGINT NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS telegram_publication_receipts (
            idempotency_key TEXT PRIMARY KEY,
            rule_id BIGINT NOT NULL,
            tenant_id BIGINT NOT NULL,
            sender_account_id BIGINT NOT NULL,
            target_peer_id BIGINT NOT NULL,
            target_thread_id BIGINT NULL,
            source_channel TEXT NOT NULL,
            source_thread_id BIGINT NULL,
            delivery_ids BIGINT[] NOT NULL,
            source_message_ids BIGINT[] NOT NULL,
            scheduled_ids BIGINT[] NOT NULL,
            pending_mask BOOLEAN[] NOT NULL,
            send_started_at TIMESTAMPTZ NOT NULL,
            deadline_at TIMESTAMPTZ NOT NULL DEFAULT (NOW() + INTERVAL '24 hours'),
            published_ids BIGINT[] NULL,
            expired_at TIMESTAMPTZ NULL,
            reaction_state TEXT NOT NULL DEFAULT 'pending'
                CHECK(reaction_state IN ('pending','dispatching','done','uncertain')),
            reaction_result JSONB NULL,
            reaction_updated_at TIMESTAMPTZ NULL,
            CHECK(cardinality(delivery_ids) BETWEEN 1 AND 10),
            CHECK(cardinality(delivery_ids) = cardinality(source_message_ids)),
            CHECK(cardinality(delivery_ids) = cardinality(scheduled_ids)),
            CHECK(cardinality(delivery_ids) = cardinality(pending_mask)),
            CHECK(published_ids IS NULL OR cardinality(published_ids) = cardinality(delivery_ids))
        );
        ALTER TABLE telegram_publication_receipts ADD COLUMN IF NOT EXISTS manual_review JSONB NULL;
        CREATE INDEX IF NOT EXISTS idx_telegram_publication_receipts_rule
            ON telegram_publication_receipts(rule_id);
        CREATE INDEX IF NOT EXISTS idx_telegram_publication_manual_review_rule
            ON telegram_publication_receipts(rule_id) WHERE manual_review IS NOT NULL;
        CREATE INDEX IF NOT EXISTS idx_telegram_publication_receipt_deliveries
            ON telegram_publication_receipts USING GIN(delivery_ids);
        CREATE TABLE IF NOT EXISTS telegram_publication_claims (
            sender_account_id BIGINT NOT NULL,
            target_peer_id BIGINT NOT NULL,
            scheduled_id BIGINT NOT NULL CHECK(scheduled_id > 0),
            idempotency_key TEXT NOT NULL REFERENCES telegram_publication_receipts(idempotency_key) ON UPDATE CASCADE,
            PRIMARY KEY (sender_account_id, target_peer_id, scheduled_id)
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

        ALTER TABLE routing ADD COLUMN IF NOT EXISTS rescan_owner_token TEXT NULL;
        ALTER TABLE routing ADD COLUMN IF NOT EXISTS rescan_started_at TIMESTAMPTZ NULL;
        ALTER TABLE routing ADD COLUMN IF NOT EXISTS rescan_queue_built BOOLEAN NOT NULL DEFAULT FALSE;
        ALTER TABLE routing ADD COLUMN IF NOT EXISTS rescan_confirmation_token TEXT NULL;
        ALTER TABLE routing ADD COLUMN IF NOT EXISTS rescan_confirmation_actor BIGINT NULL;
        ALTER TABLE routing ADD COLUMN IF NOT EXISTS rescan_confirmation_until TIMESTAMPTZ NULL;
        ALTER TABLE routing ADD COLUMN IF NOT EXISTS mode TEXT NOT NULL DEFAULT 'repost';
        ALTER TABLE routing ADD COLUMN IF NOT EXISTS video_trim_seconds BIGINT DEFAULT 120;
        ALTER TABLE routing ADD COLUMN IF NOT EXISTS video_clip_duration_seconds INTEGER DEFAULT 118;
        ALTER TABLE routing ADD COLUMN IF NOT EXISTS video_add_intro BOOLEAN DEFAULT FALSE;
        ALTER TABLE routing ADD COLUMN IF NOT EXISTS video_intro_horizontal TEXT NULL;
        ALTER TABLE routing ADD COLUMN IF NOT EXISTS video_intro_vertical TEXT NULL;
        ALTER TABLE routing ADD COLUMN IF NOT EXISTS video_caption TEXT NULL;
        ALTER TABLE routing ADD COLUMN IF NOT EXISTS video_caption_entities_json TEXT NULL;
        ALTER TABLE routing ADD COLUMN IF NOT EXISTS repost_caption TEXT NULL;
        ALTER TABLE routing ADD COLUMN IF NOT EXISTS repost_caption_entities_json TEXT NULL;
        ALTER TABLE routing ADD COLUMN IF NOT EXISTS repost_caption_enabled BOOLEAN NOT NULL DEFAULT FALSE;
        ALTER TABLE routing ADD COLUMN IF NOT EXISTS repost_caption_delivery_mode TEXT NOT NULL DEFAULT 'copy_first';
        ALTER TABLE routing ADD COLUMN IF NOT EXISTS caption_delivery_mode TEXT NOT NULL DEFAULT 'auto';
        ALTER TABLE routing ADD COLUMN IF NOT EXISTS video_caption_delivery_mode TEXT NOT NULL DEFAULT 'copy_first';
        ALTER TABLE routing ALTER COLUMN video_caption_delivery_mode SET DEFAULT 'copy_first';
        -- Preserve the old effective transport for existing auto video rules.
        UPDATE routing
           SET video_caption_delivery_mode = CASE
               WHEN video_caption_entities_json IS NOT NULL
                AND BTRIM(video_caption_entities_json::text) NOT IN ('', '[]', 'null', '{}')
               THEN 'builder_first'
               ELSE 'copy_first'
           END
         WHERE mode = 'video'
           AND (video_caption_delivery_mode IS NULL
                OR LOWER(BTRIM(video_caption_delivery_mode)) NOT IN ('copy_first', 'builder_first'));
        ALTER TABLE routing ADD COLUMN IF NOT EXISTS schedule_mode TEXT NOT NULL DEFAULT 'interval';
        ALTER TABLE routing ADD COLUMN IF NOT EXISTS fixed_times_json TEXT NULL;
        ALTER TABLE routing ADD COLUMN IF NOT EXISTS video_intro_horizontal_id BIGINT NULL;
        ALTER TABLE routing ADD COLUMN IF NOT EXISTS video_intro_vertical_id BIGINT NULL;
        ALTER TABLE routing ADD COLUMN IF NOT EXISTS repost_campaign_enabled BOOLEAN NOT NULL DEFAULT FALSE;
        ALTER TABLE routing ADD COLUMN IF NOT EXISTS repost_campaign_clean_channel_enabled BOOLEAN NOT NULL DEFAULT TRUE;
        ALTER TABLE routing ADD COLUMN IF NOT EXISTS repost_campaign_top_time_enabled BOOLEAN NOT NULL DEFAULT FALSE;
        ALTER TABLE routing ADD COLUMN IF NOT EXISTS repost_campaign_top_time_seconds INTEGER NOT NULL DEFAULT 0;
        ALTER TABLE routing ADD COLUMN IF NOT EXISTS repost_campaign_show_seconds BIGINT NOT NULL DEFAULT 0;
        ALTER TABLE routing ADD COLUMN IF NOT EXISTS repost_campaign_saved_post_id BIGINT NULL;
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
        CREATE TABLE IF NOT EXISTS rule_repost_campaign_targets (
            id BIGSERIAL PRIMARY KEY,
            rule_id BIGINT NOT NULL,
            target_id TEXT NOT NULL,
            target_thread_id BIGINT NULL,
            title TEXT NULL,
            is_active BOOLEAN NOT NULL DEFAULT TRUE,
            can_post BOOLEAN NULL,
            can_delete BOOLEAN NULL,
            publish_status TEXT NULL,
            delete_permission_status TEXT NULL,
            publish_error_text TEXT NULL,
            delete_error_text TEXT NULL,
            check_source TEXT NULL,
            last_check_at TIMESTAMPTZ NULL,
            last_check_error TEXT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NULL,
            created_by BIGINT NULL
        );
        ALTER TABLE rule_repost_campaign_targets ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NULL;
        ALTER TABLE rule_repost_campaign_targets ADD COLUMN IF NOT EXISTS publish_status TEXT NULL;
        ALTER TABLE rule_repost_campaign_targets ADD COLUMN IF NOT EXISTS delete_permission_status TEXT NULL;
        ALTER TABLE rule_repost_campaign_targets ADD COLUMN IF NOT EXISTS publish_error_text TEXT NULL;
        ALTER TABLE rule_repost_campaign_targets ADD COLUMN IF NOT EXISTS delete_error_text TEXT NULL;
        ALTER TABLE rule_repost_campaign_targets ADD COLUMN IF NOT EXISTS check_source TEXT NULL;
        CREATE UNIQUE INDEX IF NOT EXISTS idx_rule_repost_campaign_targets_unique
        ON rule_repost_campaign_targets(rule_id, target_id, COALESCE(target_thread_id, -1));
        CREATE INDEX IF NOT EXISTS idx_rule_repost_campaign_targets_rule
        ON rule_repost_campaign_targets(rule_id, is_active);



        CREATE TABLE IF NOT EXISTS saved_posts (
            id BIGSERIAL PRIMARY KEY,
            tenant_id BIGINT NOT NULL DEFAULT 1,
            rule_id BIGINT NULL,
            title TEXT NULL,
            status TEXT NOT NULL DEFAULT 'ready',
            post_type TEXT NOT NULL DEFAULT 'single',
            content_json JSONB NOT NULL,
            source_chat_id TEXT NULL,
            source_message_id BIGINT NULL,
            source_media_group_id TEXT NULL,
            created_by BIGINT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NULL,
            archived_at TIMESTAMPTZ NULL
        );
        CREATE INDEX IF NOT EXISTS idx_saved_posts_tenant_status
        ON saved_posts(tenant_id, status, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_saved_posts_rule_status
        ON saved_posts(rule_id, status, created_at DESC);

        CREATE TABLE IF NOT EXISTS delivery_campaign_copies (
            id BIGSERIAL PRIMARY KEY, delivery_id BIGINT NOT NULL, rule_id BIGINT NOT NULL,
            target_kind TEXT NOT NULL DEFAULT 'extra', target_id TEXT NOT NULL, target_thread_id BIGINT NULL, target_title TEXT NULL,
            send_status TEXT NOT NULL DEFAULT 'pending', send_error_text TEXT NULL, sent_at TIMESTAMPTZ NULL, sent_message_id BIGINT NULL, sent_message_ids_json JSONB NULL, delivery_method TEXT NULL,
            delete_after_at TIMESTAMPTZ NULL, delete_status TEXT NOT NULL DEFAULT 'none', deleted_at TIMESTAMPTZ NULL, delete_attempt_count BIGINT NOT NULL DEFAULT 0, delete_error_text TEXT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_delivery_campaign_copies_unique
        ON delivery_campaign_copies(delivery_id, target_id, COALESCE(target_thread_id, -1));
        CREATE INDEX IF NOT EXISTS idx_delivery_campaign_copies_delivery ON delivery_campaign_copies(delivery_id);
        CREATE INDEX IF NOT EXISTS idx_delivery_campaign_copies_rule_status ON delivery_campaign_copies(rule_id, send_status, delete_status);
        CREATE INDEX IF NOT EXISTS idx_delivery_campaign_copies_delete ON delivery_campaign_copies(delete_status, delete_after_at);

        CREATE TABLE IF NOT EXISTS campaign_runs (
            id BIGSERIAL PRIMARY KEY,
            tenant_id BIGINT NOT NULL DEFAULT 1,
            rule_id BIGINT NOT NULL,
            saved_post_id BIGINT NOT NULL,
            run_type TEXT NOT NULL DEFAULT 'test'
                CHECK (run_type IN ('test','manual','scheduled','retry')),
            status TEXT NOT NULL DEFAULT 'created'
                CHECK (status IN ('created','sending','partial','sent','failed','cancelled')),
            render_mode TEXT NULL,
            show_seconds BIGINT NOT NULL DEFAULT 0,
            targets_total BIGINT NOT NULL DEFAULT 0,
            targets_success BIGINT NOT NULL DEFAULT 0,
            targets_failed BIGINT NOT NULL DEFAULT 0,
            started_by BIGINT NULL,
            started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            finished_at TIMESTAMPTZ NULL,
            error_text TEXT NULL,
            report_json JSONB NULL,
            scheduled_post_id BIGINT NULL,
            top_time_enabled_snapshot BOOLEAN NOT NULL DEFAULT FALSE,
            top_time_seconds_snapshot INTEGER NOT NULL DEFAULT 0,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_campaign_runs_rule_created
        ON campaign_runs(rule_id, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_campaign_runs_saved_post
        ON campaign_runs(saved_post_id, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_campaign_runs_status
        ON campaign_runs(status, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_campaign_runs_rule_basic
        ON campaign_runs(rule_id, scheduled_post_id, run_type, created_at DESC);

        CREATE TABLE IF NOT EXISTS campaign_scheduled_launches (
            id BIGSERIAL PRIMARY KEY,
            tenant_id BIGINT NOT NULL DEFAULT 1,
            rule_id BIGINT NOT NULL,
            saved_post_id BIGINT NOT NULL,
            show_seconds BIGINT NOT NULL DEFAULT 0,
            scheduled_at TIMESTAMPTZ NOT NULL,
            timezone_offset_minutes INT NOT NULL DEFAULT 180,
            timezone_label TEXT NOT NULL DEFAULT 'UTC+3',
            status TEXT NOT NULL DEFAULT 'scheduled'
                CHECK (status IN ('scheduled','processing','waiting_clean_channel','launched','failed','needs_review','cancelled','expired')),
            campaign_run_id BIGINT NULL,
            created_by BIGINT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            locked_at TIMESTAMPTZ NULL,
            locked_by TEXT NULL,
            launched_at TIMESTAMPTZ NULL,
            cancelled_at TIMESTAMPTZ NULL,
            cancelled_by BIGINT NULL,
            failed_at TIMESTAMPTZ NULL,
            error_text TEXT NULL,
            preview_json JSONB NULL,
            top_time_enabled_snapshot BOOLEAN NOT NULL DEFAULT FALSE,
            top_time_seconds_snapshot INTEGER NOT NULL DEFAULT 0,
            clean_channel_next_retry_at TIMESTAMPTZ NULL,
            clean_channel_wait_attempt_count INTEGER NOT NULL DEFAULT 0,
            clean_channel_last_wait_at TIMESTAMPTZ NULL,
            clean_channel_last_reason TEXT NULL,
            clean_channel_policy_json JSONB NULL
        );
        CREATE INDEX IF NOT EXISTS idx_campaign_scheduled_launches_due
        ON campaign_scheduled_launches(status, scheduled_at);
        CREATE INDEX IF NOT EXISTS idx_campaign_scheduled_launches_rule_status
        ON campaign_scheduled_launches(rule_id, status, scheduled_at DESC);
        CREATE INDEX IF NOT EXISTS idx_campaign_scheduled_launches_campaign_run
        ON campaign_scheduled_launches(campaign_run_id);

        CREATE TABLE IF NOT EXISTS repost_campaign_launch_jobs (
            id BIGSERIAL PRIMARY KEY,
            rule_id BIGINT NOT NULL,
            admin_id BIGINT NULL,
            status TEXT NOT NULL DEFAULT 'pending'
                CHECK (status IN ('pending','processing','sent','failed','needs_review','cancelled')),
            payload_json JSONB NULL,
            result_json JSONB NULL,
            campaign_run_id BIGINT NULL,
            progress_chat_id TEXT NULL,
            progress_message_id BIGINT NULL,
            attempts INT NOT NULL DEFAULT 0,
            max_attempts INT NOT NULL DEFAULT 3,
            locked_by TEXT NULL,
            locked_until TIMESTAMPTZ NULL,
            last_error TEXT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            started_at TIMESTAMPTZ NULL,
            finished_at TIMESTAMPTZ NULL
        );
        CREATE INDEX IF NOT EXISTS idx_repost_campaign_launch_jobs_status_created
        ON repost_campaign_launch_jobs(status, created_at);
        CREATE INDEX IF NOT EXISTS idx_repost_campaign_launch_jobs_rule
        ON repost_campaign_launch_jobs(rule_id);
        CREATE INDEX IF NOT EXISTS idx_repost_campaign_launch_jobs_locked_until
        ON repost_campaign_launch_jobs(locked_until);
        CREATE INDEX IF NOT EXISTS idx_repost_campaign_launch_jobs_campaign_run
        ON repost_campaign_launch_jobs(campaign_run_id);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_repost_campaign_launch_jobs_active_rule
        ON repost_campaign_launch_jobs(rule_id)
        WHERE status IN ('pending','processing');

        CREATE TABLE IF NOT EXISTS campaign_invite_link_settings (
            rule_id BIGINT PRIMARY KEY,
            enabled BOOLEAN NOT NULL DEFAULT FALSE,
            destination_chat_id TEXT,
            destination_chat_title TEXT,
            link_mode TEXT NOT NULL DEFAULT 'join_request',
            injection_mode TEXT NOT NULL DEFAULT 'placeholder',
            append_template TEXT NOT NULL DEFAULT '👉 Подписаться: {invite_link}',
            per_target_links_enabled BOOLEAN NOT NULL DEFAULT TRUE,
            preview_required BOOLEAN NOT NULL DEFAULT TRUE,
            preview_checked_at TIMESTAMPTZ,
            preview_checked_by BIGINT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS campaign_invite_links (
            id BIGSERIAL PRIMARY KEY,
            rule_id BIGINT NOT NULL,
            campaign_run_id BIGINT,
            campaign_run_message_id BIGINT,
            saved_post_id BIGINT,
            destination_chat_id TEXT NOT NULL,
            destination_chat_title TEXT,
            ad_target_id TEXT,
            ad_target_thread_id BIGINT,
            ad_target_title TEXT,
            link_mode TEXT NOT NULL,
            invite_link TEXT NOT NULL,
            invite_link_name TEXT,
            invite_link_hash TEXT NOT NULL,
            creates_join_request BOOLEAN NOT NULL DEFAULT TRUE,
            status TEXT NOT NULL DEFAULT 'active',
            created_by BIGINT,
            telegram_payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            revoked_at TIMESTAMPTZ,
            archived_at TIMESTAMPTZ
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_campaign_invite_links_hash
        ON campaign_invite_links(invite_link_hash);
        CREATE INDEX IF NOT EXISTS idx_campaign_invite_links_rule
        ON campaign_invite_links(rule_id, status);
        CREATE INDEX IF NOT EXISTS idx_campaign_invite_links_run
        ON campaign_invite_links(campaign_run_id);
        CREATE INDEX IF NOT EXISTS idx_campaign_invite_links_run_message
        ON campaign_invite_links(campaign_run_message_id);
        CREATE INDEX IF NOT EXISTS idx_campaign_invite_links_ad_target
        ON campaign_invite_links(rule_id, ad_target_id, ad_target_thread_id);

        CREATE TABLE IF NOT EXISTS campaign_invite_link_events (
            id BIGSERIAL PRIMARY KEY,
            invite_link_id BIGINT NOT NULL,
            rule_id BIGINT NOT NULL,
            campaign_run_id BIGINT,
            campaign_run_message_id BIGINT,
            destination_chat_id TEXT NOT NULL,
            ad_target_id TEXT,
            ad_target_thread_id BIGINT,
            event_type TEXT NOT NULL,
            telegram_user_id_hash TEXT NOT NULL,
            telegram_user_payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
            telegram_update_id BIGINT,
            event_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            raw_update_json JSONB NOT NULL DEFAULT '{}'::jsonb
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_campaign_invite_link_events_dedup
        ON campaign_invite_link_events(invite_link_id, event_type, telegram_user_id_hash);
        CREATE INDEX IF NOT EXISTS idx_campaign_invite_link_events_rule
        ON campaign_invite_link_events(rule_id, event_at DESC);
        CREATE INDEX IF NOT EXISTS idx_campaign_invite_link_events_run
        ON campaign_invite_link_events(campaign_run_id, event_at DESC);
        CREATE INDEX IF NOT EXISTS idx_campaign_invite_link_events_link
        ON campaign_invite_link_events(invite_link_id, event_at DESC);

        CREATE TABLE IF NOT EXISTS campaign_scheduled_posts (
            id BIGSERIAL PRIMARY KEY, tenant_id BIGINT NOT NULL DEFAULT 1, rule_id BIGINT NOT NULL, saved_post_id BIGINT NULL, title TEXT NULL,
            status TEXT NOT NULL DEFAULT 'draft' CHECK (status IN ('draft','ready','scheduled','processing','launched','failed','needs_review','cancelled','expired')),
            scheduled_at TIMESTAMPTZ NULL, timezone_offset_minutes INT NOT NULL DEFAULT 180, timezone_label TEXT NOT NULL DEFAULT 'UTC+3', show_seconds BIGINT NULL,
            targets_total BIGINT NOT NULL DEFAULT 0, targets_ready BIGINT NOT NULL DEFAULT 0, targets_with_warnings BIGINT NOT NULL DEFAULT 0, targets_blocked BIGINT NOT NULL DEFAULT 0,
            attempt_count BIGINT NOT NULL DEFAULT 0, next_retry_at TIMESTAMPTZ NULL, campaign_run_id BIGINT NULL, created_by BIGINT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NULL, scheduled_by BIGINT NULL, scheduled_at_created TIMESTAMPTZ NULL, launched_at TIMESTAMPTZ NULL, cancelled_at TIMESTAMPTZ NULL, cancelled_by BIGINT NULL,
            cancel_reason TEXT NULL, failed_at TIMESTAMPTZ NULL, error_text TEXT NULL, locked_by TEXT NULL, locked_at TIMESTAMPTZ NULL, lock_until TIMESTAMPTZ NULL,
            post_snapshot_json JSONB NULL, readiness_snapshot_json JSONB NULL, preview_json JSONB NULL, metadata_json JSONB NULL
        );
        CREATE INDEX IF NOT EXISTS idx_campaign_scheduled_posts_rule_status ON campaign_scheduled_posts(rule_id, status, scheduled_at);
        CREATE INDEX IF NOT EXISTS idx_campaign_scheduled_posts_tenant_status ON campaign_scheduled_posts(tenant_id, status, scheduled_at);
        CREATE INDEX IF NOT EXISTS idx_campaign_scheduled_posts_due ON campaign_scheduled_posts(status, COALESCE(next_retry_at, scheduled_at), scheduled_at) WHERE status = 'scheduled';
        CREATE INDEX IF NOT EXISTS idx_campaign_scheduled_posts_run ON campaign_scheduled_posts(campaign_run_id) WHERE campaign_run_id IS NOT NULL;
        CREATE INDEX IF NOT EXISTS idx_campaign_scheduled_posts_saved_post ON campaign_scheduled_posts(saved_post_id);
        CREATE TABLE IF NOT EXISTS campaign_scheduled_post_targets (
            id BIGSERIAL PRIMARY KEY, scheduled_post_id BIGINT NOT NULL, rule_id BIGINT NOT NULL, target_kind TEXT NOT NULL DEFAULT 'extra' CHECK (target_kind IN ('main', 'extra')),
            target_id TEXT NOT NULL, target_thread_id BIGINT NULL, target_title TEXT NULL, is_active BOOLEAN NOT NULL DEFAULT TRUE, can_publish BOOLEAN NULL, can_delete BOOLEAN NULL,
            publish_status TEXT NULL, delete_status TEXT NULL, publish_error_text TEXT NULL, delete_error_text TEXT NULL, check_source TEXT NULL, checked_at TIMESTAMPTZ NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), updated_at TIMESTAMPTZ NULL, metadata_json JSONB NULL
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_campaign_scheduled_post_targets_unique ON campaign_scheduled_post_targets(scheduled_post_id, target_id, COALESCE(target_thread_id, -1));
        CREATE INDEX IF NOT EXISTS idx_campaign_scheduled_post_targets_post ON campaign_scheduled_post_targets(scheduled_post_id, is_active);
        CREATE INDEX IF NOT EXISTS idx_campaign_scheduled_post_targets_rule ON campaign_scheduled_post_targets(rule_id);
        CREATE TABLE IF NOT EXISTS campaign_scheduled_post_checks (
            id BIGSERIAL PRIMARY KEY, scheduled_post_id BIGINT NOT NULL, target_row_id BIGINT NULL, rule_id BIGINT NOT NULL, target_id TEXT NOT NULL, target_thread_id BIGINT NULL,
            check_type TEXT NOT NULL CHECK (check_type IN ('publish', 'delete', 'full')),
            status TEXT NOT NULL CHECK (status IN ('confirmed', 'denied', 'unknown', 'failed')),
            source TEXT NULL, error_text TEXT NULL, details_json JSONB NULL, checked_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_campaign_scheduled_post_checks_post ON campaign_scheduled_post_checks(scheduled_post_id, checked_at DESC);
        CREATE INDEX IF NOT EXISTS idx_campaign_scheduled_post_checks_target ON campaign_scheduled_post_checks(target_row_id, checked_at DESC);
        CREATE INDEX IF NOT EXISTS idx_campaign_scheduled_post_checks_status ON campaign_scheduled_post_checks(status, checked_at DESC);
        CREATE TABLE IF NOT EXISTS campaign_scheduled_post_events (
            id BIGSERIAL PRIMARY KEY, scheduled_post_id BIGINT NOT NULL, rule_id BIGINT NOT NULL, event_type TEXT NOT NULL, status_from TEXT NULL, status_to TEXT NULL,
            actor_id BIGINT NULL, worker_id TEXT NULL, message TEXT NULL, error_text TEXT NULL, extra_json JSONB NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_campaign_scheduled_post_events_post ON campaign_scheduled_post_events(scheduled_post_id, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_campaign_scheduled_post_events_rule ON campaign_scheduled_post_events(rule_id, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_campaign_scheduled_post_events_type ON campaign_scheduled_post_events(event_type, created_at DESC);

        CREATE TABLE IF NOT EXISTS campaign_run_messages (
            id BIGSERIAL PRIMARY KEY,
            run_id BIGINT NOT NULL,
            rule_id BIGINT NOT NULL,
            saved_post_id BIGINT NOT NULL,
            target_kind TEXT NOT NULL DEFAULT 'main'
                CHECK (target_kind IN ('main','extra')),
            target_id TEXT NOT NULL,
            target_thread_id BIGINT NULL,
            target_title TEXT NULL,
            send_status TEXT NOT NULL DEFAULT 'pending'
                CHECK (send_status IN ('pending','sending','sent','failed')),
            render_mode TEXT NULL,
            sent_message_id BIGINT NULL,
            sent_message_ids_json JSONB NULL,
            sent_at TIMESTAMPTZ NULL,
            send_error_text TEXT NULL,
            show_seconds BIGINT NOT NULL DEFAULT 0,
            delete_after_at TIMESTAMPTZ NULL,
            delete_status TEXT NOT NULL DEFAULT 'none'
                CHECK (delete_status IN ('none','pending','processing','deleted','failed')),
            deleted_at TIMESTAMPTZ NULL,
            delete_error_text TEXT NULL,
            delete_attempt_count BIGINT NOT NULL DEFAULT 0,
            views_final_count BIGINT NULL,
            views_final_status TEXT NULL,
            views_final_collected_at TIMESTAMPTZ NULL,
            views_final_error_text TEXT NULL,
            views_final_attempt_count BIGINT NOT NULL DEFAULT 0,
            views_final_next_retry_at TIMESTAMPTZ NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        ALTER TABLE campaign_run_messages ADD COLUMN IF NOT EXISTS publication_review_required BOOLEAN NOT NULL DEFAULT FALSE;
        ALTER TABLE campaign_run_messages ADD COLUMN IF NOT EXISTS publication_receipt JSONB NULL;
        ALTER TABLE campaign_run_messages ADD COLUMN IF NOT EXISTS publication_raw_ids JSONB NULL;
        ALTER TABLE campaign_run_messages ADD COLUMN IF NOT EXISTS publication_next_check_at TIMESTAMPTZ NULL;
        ALTER TABLE campaign_run_messages ADD COLUMN IF NOT EXISTS publication_manual_review JSONB NULL;
        CREATE INDEX IF NOT EXISTS idx_campaign_publication_review
            ON campaign_run_messages(rule_id, saved_post_id, target_id)
            WHERE send_status='sending' OR publication_review_required;
        CREATE INDEX IF NOT EXISTS idx_campaign_publication_pending
            ON campaign_run_messages(id)
            WHERE publication_receipt IS NOT NULL AND publication_review_required;
        CREATE TABLE IF NOT EXISTS campaign_publication_claims (
            sender_account_id BIGINT NOT NULL,
            target_peer_id BIGINT NOT NULL,
            scheduled_id BIGINT NOT NULL CHECK(scheduled_id > 0),
            campaign_message_id BIGINT NOT NULL REFERENCES campaign_run_messages(id),
            PRIMARY KEY(sender_account_id,target_peer_id,scheduled_id)
        );
        CREATE TABLE IF NOT EXISTS campaign_mtproto_send_intents (
            message_id BIGINT NOT NULL REFERENCES campaign_run_messages(id) ON DELETE CASCADE,
            ordinal INTEGER NOT NULL CHECK(ordinal>=0),
            fingerprint TEXT NOT NULL,
            random_ids BIGINT[] NOT NULL CHECK(cardinality(random_ids) BETWEEN 1 AND 10),
            PRIMARY KEY(message_id, ordinal)
        );
        CREATE INDEX IF NOT EXISTS idx_campaign_run_messages_run
        ON campaign_run_messages(run_id);
        CREATE INDEX IF NOT EXISTS idx_campaign_run_messages_rule_status
        ON campaign_run_messages(rule_id, send_status, delete_status);
        CREATE INDEX IF NOT EXISTS idx_campaign_run_messages_delete
        ON campaign_run_messages(delete_status, delete_after_at);
        CREATE INDEX IF NOT EXISTS idx_campaign_run_messages_active_by_rule
        ON campaign_run_messages(rule_id, delete_status, send_status, delete_after_at);
        CREATE INDEX IF NOT EXISTS idx_campaign_run_messages_active_by_run
        ON campaign_run_messages(run_id, delete_status, send_status);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_campaign_run_messages_unique_target
        ON campaign_run_messages(run_id, target_kind, target_id, COALESCE(target_thread_id, -1));
        CREATE UNIQUE INDEX IF NOT EXISTS idx_campaign_run_messages_unique_target_per_run
        ON campaign_run_messages(run_id, target_id, COALESCE(target_thread_id, -1));
        CREATE TABLE IF NOT EXISTS campaign_top_time_pauses (
            id BIGSERIAL PRIMARY KEY,
            rule_id BIGINT NOT NULL,
            campaign_run_id BIGINT NOT NULL,
            campaign_run_message_id BIGINT NOT NULL,
            target_id TEXT NOT NULL,
            target_thread_id BIGINT NULL,
            target_title TEXT NULL,
            starts_at TIMESTAMPTZ NOT NULL,
            ends_at TIMESTAMPTZ NOT NULL,
            status TEXT NOT NULL DEFAULT 'active',
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            completed_at TIMESTAMPTZ NULL,
            cancelled_at TIMESTAMPTZ NULL,
            cancel_reason TEXT NULL
        );
        ALTER TABLE campaign_top_time_pauses
        ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
        CREATE UNIQUE INDEX IF NOT EXISTS idx_campaign_top_time_pauses_run_message_unique
        ON campaign_top_time_pauses (campaign_run_message_id);
        CREATE INDEX IF NOT EXISTS idx_campaign_top_time_pauses_active_target
        ON campaign_top_time_pauses (target_id, target_thread_id, ends_at)
        WHERE status = 'active';
        CREATE INDEX IF NOT EXISTS idx_campaign_top_time_pauses_active_due
        ON campaign_top_time_pauses (ends_at)
        WHERE status = 'active';

        """

        self.client.execute_script(init_sql)

        with self.connect() as conn:
            with conn.cursor() as cur:
                for _sql in [
                    "ALTER TABLE intros ADD COLUMN IF NOT EXISTS rule_id BIGINT NULL",
                    "ALTER TABLE intros ADD COLUMN IF NOT EXISTS tenant_id BIGINT NOT NULL DEFAULT 1",
                    "ALTER TABLE intros ADD COLUMN IF NOT EXISTS created_by BIGINT NULL",
                    "ALTER TABLE intros ADD COLUMN IF NOT EXISTS media_kind TEXT NULL",
                    "ALTER TABLE intros ADD COLUMN IF NOT EXISTS bank TEXT NULL",
                    "ALTER TABLE intros ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'active'",
                    "ALTER TABLE intros ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ NULL",
                    "ALTER TABLE intros ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NULL",
                ]:
                    cur.execute(_sql)
                cur.execute("ALTER TABLE intros DROP CONSTRAINT IF EXISTS intros_status_check")
                cur.execute("ALTER TABLE intros ADD CONSTRAINT intros_status_check CHECK (status IN ('active', 'deleted'))")
                cur.execute("ALTER TABLE intros DROP CONSTRAINT IF EXISTS intros_display_name_key")
                cur.execute("ALTER TABLE intros DROP CONSTRAINT IF EXISTS intros_bank_check")
                cur.execute("ALTER TABLE intros ADD CONSTRAINT intros_bank_check CHECK (bank IS NULL OR bank IN ('horizontal', 'vertical'))")

                for _sql in [
                    "ALTER TABLE campaign_invite_link_settings ADD COLUMN IF NOT EXISTS enabled BOOLEAN NOT NULL DEFAULT FALSE",
                    "ALTER TABLE campaign_invite_link_settings ADD COLUMN IF NOT EXISTS destination_chat_id TEXT NULL",
                    "ALTER TABLE campaign_invite_link_settings ADD COLUMN IF NOT EXISTS destination_chat_title TEXT NULL",
                    "ALTER TABLE campaign_invite_link_settings ADD COLUMN IF NOT EXISTS link_mode TEXT NOT NULL DEFAULT 'join_request'",
                    "ALTER TABLE campaign_invite_link_settings ADD COLUMN IF NOT EXISTS injection_mode TEXT NOT NULL DEFAULT 'placeholder'",
                    "ALTER TABLE campaign_invite_link_settings ADD COLUMN IF NOT EXISTS append_template TEXT NOT NULL DEFAULT '👉 Подписаться: {invite_link}'",
                    "ALTER TABLE campaign_invite_link_settings ADD COLUMN IF NOT EXISTS per_target_links_enabled BOOLEAN NOT NULL DEFAULT TRUE",
                    "ALTER TABLE campaign_invite_link_settings ADD COLUMN IF NOT EXISTS preview_required BOOLEAN NOT NULL DEFAULT TRUE",
                    "ALTER TABLE campaign_invite_link_settings ADD COLUMN IF NOT EXISTS preview_checked_at TIMESTAMPTZ NULL",
                    "ALTER TABLE campaign_invite_link_settings ADD COLUMN IF NOT EXISTS preview_checked_by BIGINT NULL",
                    "ALTER TABLE campaign_invite_link_settings ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()",
                    "ALTER TABLE campaign_invite_link_settings ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()",
                ]:
                    cur.execute(_sql)

                for _sql in [
                    "ALTER TABLE campaign_invite_links ADD COLUMN IF NOT EXISTS rule_id BIGINT NOT NULL",
                    "ALTER TABLE campaign_invite_links ADD COLUMN IF NOT EXISTS campaign_run_id BIGINT NULL",
                    "ALTER TABLE campaign_invite_links ADD COLUMN IF NOT EXISTS campaign_run_message_id BIGINT NULL",
                    "ALTER TABLE campaign_invite_links ADD COLUMN IF NOT EXISTS saved_post_id BIGINT NULL",
                    "ALTER TABLE campaign_invite_links ADD COLUMN IF NOT EXISTS destination_chat_id TEXT NOT NULL",
                    "ALTER TABLE campaign_invite_links ADD COLUMN IF NOT EXISTS destination_chat_title TEXT NULL",
                    "ALTER TABLE campaign_invite_links ADD COLUMN IF NOT EXISTS ad_target_id TEXT NULL",
                    "ALTER TABLE campaign_invite_links ADD COLUMN IF NOT EXISTS ad_target_thread_id BIGINT NULL",
                    "ALTER TABLE campaign_invite_links ADD COLUMN IF NOT EXISTS ad_target_title TEXT NULL",
                    "ALTER TABLE campaign_invite_links ADD COLUMN IF NOT EXISTS link_mode TEXT NOT NULL",
                    "ALTER TABLE campaign_invite_links ADD COLUMN IF NOT EXISTS invite_link TEXT NOT NULL",
                    "ALTER TABLE campaign_invite_links ADD COLUMN IF NOT EXISTS invite_link_name TEXT NULL",
                    "ALTER TABLE campaign_invite_links ADD COLUMN IF NOT EXISTS invite_link_hash TEXT NOT NULL",
                    "ALTER TABLE campaign_invite_links ADD COLUMN IF NOT EXISTS creates_join_request BOOLEAN NOT NULL DEFAULT TRUE",
                    "ALTER TABLE campaign_invite_links ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'active'",
                    "ALTER TABLE campaign_invite_links ADD COLUMN IF NOT EXISTS created_by BIGINT NULL",
                    "ALTER TABLE campaign_invite_links ADD COLUMN IF NOT EXISTS telegram_payload_json JSONB NOT NULL DEFAULT '{}'::jsonb",
                    "ALTER TABLE campaign_invite_links ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()",
                    "ALTER TABLE campaign_invite_links ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()",
                    "ALTER TABLE campaign_invite_links ADD COLUMN IF NOT EXISTS revoked_at TIMESTAMPTZ NULL",
                    "ALTER TABLE campaign_invite_links ADD COLUMN IF NOT EXISTS archived_at TIMESTAMPTZ NULL",
                    "ALTER TABLE campaign_invite_link_events ADD COLUMN IF NOT EXISTS invite_link_id BIGINT NOT NULL",
                    "ALTER TABLE campaign_invite_link_events ADD COLUMN IF NOT EXISTS rule_id BIGINT NOT NULL",
                    "ALTER TABLE campaign_invite_link_events ADD COLUMN IF NOT EXISTS campaign_run_id BIGINT NULL",
                    "ALTER TABLE campaign_invite_link_events ADD COLUMN IF NOT EXISTS campaign_run_message_id BIGINT NULL",
                    "ALTER TABLE campaign_invite_link_events ADD COLUMN IF NOT EXISTS destination_chat_id TEXT NOT NULL",
                    "ALTER TABLE campaign_invite_link_events ADD COLUMN IF NOT EXISTS ad_target_id TEXT NULL",
                    "ALTER TABLE campaign_invite_link_events ADD COLUMN IF NOT EXISTS ad_target_thread_id BIGINT NULL",
                    "ALTER TABLE campaign_invite_link_events ADD COLUMN IF NOT EXISTS event_type TEXT NOT NULL",
                    "ALTER TABLE campaign_invite_link_events ADD COLUMN IF NOT EXISTS telegram_user_id_hash TEXT NOT NULL",
                    "ALTER TABLE campaign_invite_link_events ADD COLUMN IF NOT EXISTS telegram_user_payload_json JSONB NOT NULL DEFAULT '{}'::jsonb",
                    "ALTER TABLE campaign_invite_link_events ADD COLUMN IF NOT EXISTS telegram_update_id BIGINT NULL",
                    "ALTER TABLE campaign_invite_link_events ADD COLUMN IF NOT EXISTS event_at TIMESTAMPTZ NOT NULL DEFAULT NOW()",
                    "ALTER TABLE campaign_invite_link_events ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()",
                    "ALTER TABLE campaign_invite_link_events ADD COLUMN IF NOT EXISTS raw_update_json JSONB NOT NULL DEFAULT '{}'::jsonb",
                ]:
                    cur.execute(_sql)

                cur.execute(
                    """
                    ALTER TABLE campaign_run_messages ADD COLUMN IF NOT EXISTS views_final_count BIGINT NULL
                    """
                )
                cur.execute(
                    """
                    ALTER TABLE campaign_run_messages ADD COLUMN IF NOT EXISTS views_final_status TEXT NULL
                    """
                )
                cur.execute(
                    """
                    ALTER TABLE campaign_run_messages ADD COLUMN IF NOT EXISTS views_final_collected_at TIMESTAMPTZ NULL
                    """
                )
                cur.execute(
                    """
                    ALTER TABLE campaign_run_messages ADD COLUMN IF NOT EXISTS views_final_error_text TEXT NULL
                    """
                )
                cur.execute(
                    """
                    ALTER TABLE campaign_run_messages ADD COLUMN IF NOT EXISTS views_final_attempt_count BIGINT NOT NULL DEFAULT 0
                    """
                )
                cur.execute(
                    """
                    ALTER TABLE campaign_run_messages ADD COLUMN IF NOT EXISTS views_final_next_retry_at TIMESTAMPTZ NULL
                    """
                )

                cur.execute("ALTER TABLE campaign_runs ADD COLUMN IF NOT EXISTS scheduled_post_id BIGINT NULL")
                cur.execute("ALTER TABLE campaign_runs ADD COLUMN IF NOT EXISTS top_time_enabled_snapshot BOOLEAN NOT NULL DEFAULT FALSE")
                cur.execute("ALTER TABLE campaign_runs ADD COLUMN IF NOT EXISTS top_time_seconds_snapshot INTEGER NOT NULL DEFAULT 0")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_campaign_runs_scheduled_post ON campaign_runs(scheduled_post_id)")
                for _sql in [
                    "ALTER TABLE campaign_scheduled_launches ADD COLUMN IF NOT EXISTS top_time_enabled_snapshot BOOLEAN NOT NULL DEFAULT FALSE",
                    "ALTER TABLE campaign_scheduled_launches ADD COLUMN IF NOT EXISTS top_time_seconds_snapshot INTEGER NOT NULL DEFAULT 0",
                    "ALTER TABLE campaign_scheduled_launches ADD COLUMN IF NOT EXISTS clean_channel_next_retry_at TIMESTAMPTZ NULL",
                    "ALTER TABLE campaign_scheduled_launches ADD COLUMN IF NOT EXISTS clean_channel_wait_attempt_count INTEGER NOT NULL DEFAULT 0",
                    "ALTER TABLE campaign_scheduled_launches ADD COLUMN IF NOT EXISTS clean_channel_last_wait_at TIMESTAMPTZ NULL",
                    "ALTER TABLE campaign_scheduled_launches ADD COLUMN IF NOT EXISTS clean_channel_last_reason TEXT NULL",
                    "ALTER TABLE campaign_scheduled_launches ADD COLUMN IF NOT EXISTS clean_channel_policy_json JSONB NULL",
                ]:
                    cur.execute(_sql)
                cur.execute("CREATE INDEX IF NOT EXISTS idx_campaign_scheduled_launches_waiting_clean_channel_due ON campaign_scheduled_launches (clean_channel_next_retry_at, id) WHERE status = 'waiting_clean_channel'")
                cur.execute("ALTER TABLE campaign_scheduled_launches DROP CONSTRAINT IF EXISTS campaign_scheduled_launches_status_check")
                cur.execute("ALTER TABLE campaign_scheduled_launches ADD CONSTRAINT campaign_scheduled_launches_status_check CHECK (status IN ('scheduled','processing','waiting_clean_channel','launched','failed','needs_review','cancelled','expired'))")
                cur.execute("ALTER TABLE campaign_scheduled_posts DROP CONSTRAINT IF EXISTS campaign_scheduled_posts_status_check")
                cur.execute("ALTER TABLE campaign_scheduled_posts ADD CONSTRAINT campaign_scheduled_posts_status_check CHECK (status IN ('draft','ready','scheduled','processing','launched','failed','needs_review','cancelled','expired'))")
                for _sql in [
                    "ALTER TABLE campaign_scheduled_posts ADD COLUMN IF NOT EXISTS tenant_id BIGINT NOT NULL DEFAULT 1",
                    "ALTER TABLE campaign_scheduled_posts ADD COLUMN IF NOT EXISTS rule_id BIGINT NOT NULL DEFAULT 0",
                    "ALTER TABLE campaign_scheduled_posts ADD COLUMN IF NOT EXISTS saved_post_id BIGINT NULL",
                    "ALTER TABLE campaign_scheduled_posts ADD COLUMN IF NOT EXISTS title TEXT NULL",
                    "ALTER TABLE campaign_scheduled_posts ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'draft'",
                    "ALTER TABLE campaign_scheduled_posts ADD COLUMN IF NOT EXISTS scheduled_at TIMESTAMPTZ NULL",
                    "ALTER TABLE campaign_scheduled_posts ADD COLUMN IF NOT EXISTS timezone_offset_minutes INT NOT NULL DEFAULT 180",
                    "ALTER TABLE campaign_scheduled_posts ADD COLUMN IF NOT EXISTS timezone_label TEXT NOT NULL DEFAULT 'UTC+3'",
                    "ALTER TABLE campaign_scheduled_posts ADD COLUMN IF NOT EXISTS show_seconds BIGINT NULL",
                    "ALTER TABLE campaign_scheduled_posts ADD COLUMN IF NOT EXISTS targets_total BIGINT NOT NULL DEFAULT 0",
                    "ALTER TABLE campaign_scheduled_posts ADD COLUMN IF NOT EXISTS targets_ready BIGINT NOT NULL DEFAULT 0",
                    "ALTER TABLE campaign_scheduled_posts ADD COLUMN IF NOT EXISTS targets_with_warnings BIGINT NOT NULL DEFAULT 0",
                    "ALTER TABLE campaign_scheduled_posts ADD COLUMN IF NOT EXISTS targets_blocked BIGINT NOT NULL DEFAULT 0",
                    "ALTER TABLE campaign_scheduled_posts ADD COLUMN IF NOT EXISTS attempt_count BIGINT NOT NULL DEFAULT 0",
                    "ALTER TABLE campaign_scheduled_posts ADD COLUMN IF NOT EXISTS next_retry_at TIMESTAMPTZ NULL",
                    "ALTER TABLE campaign_scheduled_posts ADD COLUMN IF NOT EXISTS campaign_run_id BIGINT NULL",
                    "ALTER TABLE campaign_scheduled_posts ADD COLUMN IF NOT EXISTS created_by BIGINT NULL",
                    "ALTER TABLE campaign_scheduled_posts ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()",
                    "ALTER TABLE campaign_scheduled_posts ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NULL",
                    "ALTER TABLE campaign_scheduled_posts ADD COLUMN IF NOT EXISTS scheduled_by BIGINT NULL",
                    "ALTER TABLE campaign_scheduled_posts ADD COLUMN IF NOT EXISTS scheduled_at_created TIMESTAMPTZ NULL",
                    "ALTER TABLE campaign_scheduled_posts ADD COLUMN IF NOT EXISTS launched_at TIMESTAMPTZ NULL",
                    "ALTER TABLE campaign_scheduled_posts ADD COLUMN IF NOT EXISTS cancelled_at TIMESTAMPTZ NULL",
                    "ALTER TABLE campaign_scheduled_posts ADD COLUMN IF NOT EXISTS cancelled_by BIGINT NULL",
                    "ALTER TABLE campaign_scheduled_posts ADD COLUMN IF NOT EXISTS cancel_reason TEXT NULL",
                    "ALTER TABLE campaign_scheduled_posts ADD COLUMN IF NOT EXISTS failed_at TIMESTAMPTZ NULL",
                    "ALTER TABLE campaign_scheduled_posts ADD COLUMN IF NOT EXISTS error_text TEXT NULL",
                    "ALTER TABLE campaign_scheduled_posts ADD COLUMN IF NOT EXISTS locked_by TEXT NULL",
                    "ALTER TABLE campaign_scheduled_posts ADD COLUMN IF NOT EXISTS locked_at TIMESTAMPTZ NULL",
                    "ALTER TABLE campaign_scheduled_posts ADD COLUMN IF NOT EXISTS lock_until TIMESTAMPTZ NULL",
                    "ALTER TABLE campaign_scheduled_posts ADD COLUMN IF NOT EXISTS post_snapshot_json JSONB NULL",
                    "ALTER TABLE campaign_scheduled_posts ADD COLUMN IF NOT EXISTS readiness_snapshot_json JSONB NULL",
                    "ALTER TABLE campaign_scheduled_posts ADD COLUMN IF NOT EXISTS preview_json JSONB NULL",
                    "ALTER TABLE campaign_scheduled_posts ADD COLUMN IF NOT EXISTS metadata_json JSONB NULL",
                ]:
                    cur.execute(_sql)
                cur.execute(
                    """
                    UPDATE routing
                    SET video_clip_duration_seconds = 118
                    WHERE video_clip_duration_seconds IS NULL
                    """
                )
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
                    CREATE INDEX IF NOT EXISTS idx_intros_rule_status_created
                    ON intros(rule_id, status, created_at DESC)
                    """
                )
                cur.execute("DROP INDEX IF EXISTS uq_intros_rule_display_name_active")
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_intros_rule_bank_status_created
                    ON intros(rule_id, bank, status, created_at DESC)
                    """
                )
                cur.execute(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS uq_intros_rule_bank_display_name_active
                    ON intros(rule_id, bank, lower(display_name))
                    WHERE rule_id IS NOT NULL AND status = 'active' AND deleted_at IS NULL
                    """
                )
                cur.execute(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS uq_intros_global_display_name_active
                    ON intros(lower(display_name))
                    WHERE rule_id IS NULL AND status = 'active' AND deleted_at IS NULL
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
                    CREATE INDEX IF NOT EXISTS idx_delivery_attempts_delivery_id
                    ON delivery_attempts(delivery_id, created_at)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_delivery_attempts_rule_id
                    ON delivery_attempts(rule_id, created_at)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_delivery_attempts_status
                    ON delivery_attempts(status, updated_at)
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
        try:
            stats = self.migrate_legacy_rule_intro_assignments()
            logger.info("INTRO_LEGACY_ASSIGNMENTS_MIGRATED | stats=%s", stats)
            bank_stats = self.migrate_rule_intro_banks()
            logger.info("INTRO_BANK_MIGRATION_DONE | stats=%s", bank_stats)
        except Exception:
            logger.exception("INTRO_LEGACY_ASSIGNMENTS_MIGRATION_FAILED")

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


    def get_stuck_processing_deliveries(
        self,
        *,
        older_than_seconds: int,
        limit: int = 50,
        rule_id: int | None = None,
    ) -> list[dict[str, Any]]:
        safe_limit = max(1, min(int(limit), 50))
        safe_seconds = max(1, int(older_than_seconds))
        params: list[Any] = [safe_seconds]
        rule_filter = ""
        if rule_id is not None:
            rule_filter = "AND d.rule_id = %s"
            params.append(int(rule_id))
        params.append(safe_limit)
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                        d.id AS delivery_id,
                        d.rule_id,
                        d.post_id,
                        r.source_id,
                        r.target_id,
                        EXTRACT(EPOCH FROM (NOW() - processing_clock.processing_job_created_at)) AS age_seconds,
                        processing_clock.processing_job_created_at AS created_at,
                        d.error_text
                    FROM deliveries d
                    JOIN routing r ON r.id = d.rule_id
                    JOIN LATERAL (
                        SELECT MIN(j.created_at) AS processing_job_created_at
                        FROM jobs j
                        WHERE j.status IN ('pending', 'leased', 'processing', 'retry')
                          AND (
                              (
                                  (j.payload_json ->> 'delivery_id') ~ '^[0-9]+$'
                                  AND (j.payload_json ->> 'delivery_id')::bigint = d.id
                              )
                              OR EXISTS (
                                  SELECT 1
                                  FROM jsonb_array_elements_text(COALESCE(j.payload_json -> 'delivery_ids', '[]'::jsonb)) AS delivery_ids(value)
                                  WHERE delivery_ids.value ~ '^[0-9]+$'
                                    AND delivery_ids.value::bigint = d.id
                              )
                          )
                    ) AS processing_clock ON TRUE
                    WHERE d.status = 'processing'
                      AND processing_clock.processing_job_created_at IS NOT NULL
                      AND processing_clock.processing_job_created_at < NOW() - make_interval(secs => %s)
                      {rule_filter}
                    ORDER BY processing_clock.processing_job_created_at ASC, d.id ASC
                    LIMIT %s
                    """,
                    tuple(params),
                )
                rows = cur.fetchall() or []
        return [dict(row) for row in rows]

    def reset_stuck_processing_deliveries_to_pending(
        self,
        *,
        older_than_seconds: int,
        limit: int = 50,
        rule_id: int | None = None,
        admin_id: int | None = None,
    ) -> dict[str, Any]:
        safe_limit = max(1, min(int(limit), 50))
        safe_seconds = max(1, int(older_than_seconds))
        params: list[Any] = [safe_seconds]
        rule_filter = ""
        if rule_id is not None:
            rule_filter = "AND d.rule_id = %s"
            params.append(int(rule_id))
        params.append(safe_limit)
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    WITH candidates AS (
                        SELECT d.id
                        FROM deliveries d
                        JOIN LATERAL (
                            SELECT MIN(j.created_at) AS processing_job_created_at
                            FROM jobs j
                            WHERE j.status IN ('pending', 'leased', 'processing', 'retry')
                              AND (
                                  (
                                      (j.payload_json ->> 'delivery_id') ~ '^[0-9]+$'
                                      AND (j.payload_json ->> 'delivery_id')::bigint = d.id
                                  )
                                  OR EXISTS (
                                      SELECT 1
                                      FROM jsonb_array_elements_text(COALESCE(j.payload_json -> 'delivery_ids', '[]'::jsonb)) AS delivery_ids(value)
                                      WHERE delivery_ids.value ~ '^[0-9]+$'
                                        AND delivery_ids.value::bigint = d.id
                                  )
                              )
                        ) AS processing_clock ON TRUE
                        WHERE d.status = 'processing'
                          AND processing_clock.processing_job_created_at IS NOT NULL
                          AND processing_clock.processing_job_created_at < NOW() - make_interval(secs => %s)
                          AND d.sent_at IS NULL
                          AND d.sent_message_id IS NULL
                          AND (d.sent_message_ids_json IS NULL OR d.sent_message_ids_json = '[]'::jsonb)
                          {rule_filter}
                        ORDER BY processing_clock.processing_job_created_at ASC, d.id ASC
                        LIMIT %s
                    )
                    UPDATE deliveries d
                    SET status = 'pending',
                        error_text = NULL
                    FROM candidates
                    WHERE d.id = candidates.id
                      AND d.status = 'processing'
                    RETURNING d.id
                    """,
                    tuple(params),
                )
                rows = cur.fetchall() or []
                delivery_ids = [int(row["id"]) for row in rows]
            conn.commit()
        logger.warning(
            "manual_stuck_processing_reset: admin_id=%s reset_count=%s delivery_ids=%s",
            admin_id,
            len(delivery_ids),
            delivery_ids,
        )
        return {"updated_count": len(delivery_ids), "delivery_ids": delivery_ids, "limit": safe_limit}

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

    def enqueue_reaction_job(self, *, tenant_id: int, rule_id: int, target_id: str, message_id: int, account_ids: list[int] | None = None, not_before: str | None = None, max_attempts: int = 3, reaction_payload: dict[str, Any] | None = None, delivery_id: int | None = None, account_id: int | None = None, run_at: str | None = None, publication_intent: tuple[str, datetime] | None = None) -> int | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                if publication_intent is not None:
                    key, started_at = publication_intent
                    try:
                        peer_id = int(target_id)
                    except (TypeError, ValueError):
                        return None
                    cur.execute(
                        """
                        SELECT 1 FROM telegram_publication_receipts
                        WHERE idempotency_key=%s AND send_started_at=%s
                          AND reaction_state='dispatching' AND tenant_id=%s AND rule_id=%s
                          AND target_peer_id=%s AND %s=ANY(published_ids)
                        FOR UPDATE
                        """,
                        (key, started_at, tenant_id, rule_id, peer_id, message_id),
                    )
                    if not cur.fetchone():
                        return None
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
                if publication_intent is not None:
                    if not row or row.get("id") is None:
                        raise RuntimeError("Reaction queue insert returned no ID")
                    cur.execute(
                        """
                        UPDATE telegram_publication_receipts
                        SET reaction_state='done', reaction_result=%s::jsonb, reaction_updated_at=NOW()
                        WHERE idempotency_key=%s AND send_started_at=%s AND reaction_state='dispatching'
                        """,
                        (_json_dumps({"enqueued": True, "job_id": int(row["id"]), "reason": "job_enqueued"}),
                         key, started_at),
                    )
                    if cur.rowcount != 1:
                        raise RuntimeError("Publication reaction intent changed")
            conn.commit()
        return int(row["id"]) if row and row.get("id") is not None else None

    def lease_due_reaction_job(self, *, worker_id: str, lock_timeout_seconds: int = 300) -> dict[str, Any] | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    WITH exhausted AS (
                        SELECT id FROM reaction_jobs
                        WHERE attempt_count>=max_attempts AND (status='pending' OR
                            (status='processing' AND locked_at<NOW()-make_interval(secs=>%s)))
                        ORDER BY id LIMIT 100 FOR UPDATE SKIP LOCKED
                    )
                    UPDATE reaction_jobs SET status='failed', locked_by=NULL, locked_at=NULL,
                        error_text='reaction retry budget exhausted', processed_at=NOW(), updated_at=NOW()
                    FROM exhausted WHERE reaction_jobs.id=exhausted.id
                    """,
                    (int(lock_timeout_seconds),),
                )
                cur.execute(
                    """
                    WITH candidate AS (
                        SELECT id
                        FROM reaction_jobs
                        WHERE (
                            status = 'pending'
                            AND attempt_count < max_attempts
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

    def heartbeat_reaction_job(self, *, job_id: int, worker_id: str) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE reaction_jobs SET locked_at=NOW(),updated_at=NOW() WHERE id=%s AND locked_by=%s AND status='processing'", (job_id, worker_id))
                updated = cur.rowcount == 1
            conn.commit()
        return updated

    def mark_reaction_job_done(self, *, job_id: int, worker_id: str, result: dict) -> bool:
        return self._finish_reaction_job(job_id, worker_id, "done", result, None, None)

    def mark_reaction_job_failed(self, *, job_id: int, worker_id: str, error_text: str, result: dict | None = None, retry_after_seconds: int | None = None) -> bool:
        return self._finish_reaction_job(job_id, worker_id, "failed", result, error_text, retry_after_seconds)

    def mark_reaction_job_skipped(self, *, job_id: int, worker_id: str, result: dict) -> bool:
        return self._finish_reaction_job(job_id, worker_id, "skipped", result, None, None)

    def _finish_reaction_job(self, job_id, worker_id, status, result, error, retry_after) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE reaction_jobs SET
                      status=CASE WHEN %s::integer IS NOT NULL AND attempt_count<max_attempts THEN 'pending' ELSE %s END,
                      not_before=CASE WHEN %s::integer IS NOT NULL AND attempt_count<max_attempts
                          THEN NOW()+make_interval(secs=>%s::integer) ELSE NULL END,
                      processed_at=CASE WHEN %s::integer IS NOT NULL AND attempt_count<max_attempts THEN NULL ELSE NOW() END,
                      result_json=%s,error_text=%s,locked_by=NULL,locked_at=NULL,updated_at=NOW()
                    WHERE id=%s AND locked_by=%s AND status='processing'
                    """,
                    (retry_after,status,retry_after,retry_after,retry_after,_json_dumps(result),error,job_id,worker_id),
                )
                updated = cur.rowcount == 1
            conn.commit()
        return updated

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

    def refresh_channel_title(self, channel_id: str, title: str) -> int:
        """Update changed chat names in all saved rows, including forum groups.

        Updating `channels.title` is sufficient: all rule views join this
        table and immediately pick up the new text on their next rendering.
        No routing, deliveries, scheduler or queue state is modified.
        """
        cleaned = str(title or "").strip()
        if not cleaned:
            return 0
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE channels
                    SET title = %s
                    WHERE channel_id = %s
                      AND title IS DISTINCT FROM %s
                    """,
                    (cleaned, str(channel_id), cleaned),
                )
                changed = int(cur.rowcount or 0)
            conn.commit()
        return changed

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
                    cur.execute("SELECT id FROM routing WHERE id=%s AND rescan_owner_token IS NULL FOR UPDATE", (rule_id,))
                    if not cur.fetchone():
                        return None
                    cur.execute("""SELECT 1 FROM telegram_publication_receipts
                        WHERE rule_id=%s AND (published_ids IS NULL OR reaction_state IN ('pending','dispatching')) LIMIT 1""",
                        (rule_id,))
                    if cur.fetchone():
                        return None
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
                            attempt_count,
                            sent_message_id,
                            sent_message_ids_json,
                            target_id_snapshot,
                            delivery_method
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
                                "sent_message_id": row["sent_message_id"],
                                "sent_message_ids_json": row["sent_message_ids_json"],
                                "target_id_snapshot": row["target_id_snapshot"],
                                "delivery_method": row["delivery_method"],
                            }
                        )

                    cur.executemany(
                        """
                        UPDATE deliveries
                        SET status = 'pending',
                            sent_at = NULL,
                            error_text = NULL,
                            attempt_count = 0,
                            sent_message_id = NULL,
                            sent_message_ids_json = NULL,
                            target_id_snapshot = NULL,
                            delivery_method = NULL
                        WHERE id = %s
                        """,
                        [(delivery_id,) for delivery_id in delivery_ids],
                    )

                    cur.execute(
                        """
                        UPDATE telegram_publication_receipts
                        SET idempotency_key='archived:' || %s || ':' || idempotency_key
                        WHERE rule_id=%s AND delivery_ids && %s::bigint[]
                          AND published_ids IS NOT NULL AND idempotency_key NOT LIKE 'archived:%%'
                        """,
                        (uuid.uuid4().hex, rule_id, delivery_ids),
                    )
                    cur.execute(
                        """
                        DELETE FROM delivery_attempts
                        WHERE delivery_id = ANY(%s)
                        """,
                        (delivery_ids,),
                    )
                    deleted_attempts_count = int(cur.rowcount or 0)

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
                                    "sent_message_id": None,
                                    "sent_message_ids_json": None,
                                    "target_id_snapshot": None,
                                    "delivery_method": None,
                                    "deleted_delivery_attempts_count": deleted_attempts_count,
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
                                    "reset_send_state": True,
                                    "deleted_delivery_attempts_count": deleted_attempts_count,
                                }
                            ),
                        ),
                    )

                conn.commit()

            logger.info(
                "rollback_last_delivery: reset send state and deleted attempts, rule_id=%s, delivery_ids=%s, deleted_attempts_count=%s",
                rule_id,
                delivery_ids,
                deleted_attempts_count,
            )
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

    def _intro_item_from_row(self, row) -> IntroItem:
        data = dict(row)

        def _optional_int(value):
            return int(value) if value is not None else None

        def _optional_str(value):
            if value is None:
                return None
            return value.isoformat() if hasattr(value, "isoformat") else str(value)

        return IntroItem(
            id=int(data["id"]),
            display_name=data["display_name"],
            file_name=data["file_name"],
            file_path=data["file_path"],
            duration=int(data.get("duration") or 0),
            created_at=_optional_str(data.get("created_at")),
            rule_id=_optional_int(data.get("rule_id")),
            tenant_id=_optional_int(data.get("tenant_id")),
            created_by=_optional_int(data.get("created_by")),
            media_kind=data.get("media_kind"),
            bank=data.get("bank"),
            status=data.get("status") or "active",
            deleted_at=_optional_str(data.get("deleted_at")),
            updated_at=_optional_str(data.get("updated_at")),
        )

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
                    SELECT
                        id, display_name, file_name, file_path, duration, created_at,
                        rule_id, tenant_id, created_by, media_kind, bank, status, deleted_at, updated_at
                    FROM intros
                    WHERE status = 'active'
                      AND deleted_at IS NULL
                    ORDER BY created_at DESC, id DESC
                    """
                )
                rows = cur.fetchall()

        return [self._intro_item_from_row(row) for row in rows]

    def get_intro(self, intro_id: int):
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        id, display_name, file_name, file_path, duration, created_at,
                        rule_id, tenant_id, created_by, media_kind, bank, status, deleted_at, updated_at
                    FROM intros
                    WHERE id = %s
                      AND status = 'active'
                      AND deleted_at IS NULL
                    LIMIT 1
                    """,
                    (intro_id,),
                )
                row = cur.fetchone()

        if not row:
            return None

        return self._intro_item_from_row(row)

    def get_intro_by_id(self, intro_id: int):
        return self.get_intro(intro_id)

    def delete_intro(self, intro_id: int) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM intros WHERE id = %s", (intro_id,))
                deleted = cur.rowcount > 0
            conn.commit()
            return deleted

    def add_rule_intro(
        self,
        rule_id: int,
        display_name: str,
        file_name: str,
        file_path: str,
        duration: int = 0,
        *,
        created_by: int | None = None,
        media_kind: str | None = None,
        tenant_id: int = 1,
        bank: str | None = None,
    ) -> int:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO intros(
                        rule_id, tenant_id, created_by, display_name, file_name,
                        file_path, duration, media_kind, bank, status, created_at, updated_at
                    )
                    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, 'active', %s, NOW())
                    RETURNING id
                    """,
                    (
                        int(rule_id),
                        int(tenant_id),
                        created_by,
                        display_name.strip(),
                        file_name,
                        file_path,
                        int(duration or 0),
                        media_kind,
                        bank,
                        utc_now_iso(),
                    ),
                )
                row = cur.fetchone()
            conn.commit()

        return int(row["id"])

    def list_rule_intros(
        self,
        rule_id: int,
        *,
        bank: str | None = None,
        include_deleted: bool = False,
    ) -> list[IntroItem]:
        status_filter = "" if include_deleted else "AND status = 'active' AND deleted_at IS NULL"
        bank_filter = ""
        params: list[Any] = [int(rule_id)]
        if bank is not None:
            bank_filter = "AND bank = %s"
            params.append(bank)
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                        id, display_name, file_name, file_path, duration, created_at,
                        rule_id, tenant_id, created_by, media_kind, bank, status, deleted_at, updated_at
                    FROM intros
                    WHERE rule_id = %s
                      {bank_filter}
                      {status_filter}
                    ORDER BY created_at DESC, id DESC
                    """,
                    tuple(params),
                )
                rows = cur.fetchall()

        return [self._intro_item_from_row(row) for row in rows]

    def get_rule_intro(
        self,
        rule_id: int,
        intro_id: int,
        *,
        bank: str | None = None,
        include_deleted: bool = False,
    ) -> IntroItem | None:
        status_filter = "" if include_deleted else "AND status = 'active' AND deleted_at IS NULL"
        bank_filter = ""
        params: list[Any] = [int(intro_id), int(rule_id)]
        if bank is not None:
            bank_filter = "AND bank = %s"
            params.append(bank)
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                        id, display_name, file_name, file_path, duration, created_at,
                        rule_id, tenant_id, created_by, media_kind, bank, status, deleted_at, updated_at
                    FROM intros
                    WHERE id = %s
                      AND rule_id = %s
                      {bank_filter}
                      {status_filter}
                    LIMIT 1
                    """,
                    tuple(params),
                )
                row = cur.fetchone()

        if not row:
            return None

        return self._intro_item_from_row(row)

    def soft_delete_rule_intro(self, rule_id: int, intro_id: int) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE intros
                    SET
                        status = 'deleted',
                        deleted_at = NOW(),
                        updated_at = NOW()
                    WHERE id = %s
                      AND rule_id = %s
                      AND status = 'active'
                      AND deleted_at IS NULL
                    RETURNING id
                    """,
                    (int(intro_id), int(rule_id)),
                )
                row = cur.fetchone()
            conn.commit()

        return row is not None

    def _rule_intro_display_name_exists(self, conn, rule_id: int, display_name: str, bank: str | None = None) -> bool:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id
                FROM intros
                WHERE rule_id = %s
                  AND bank IS NOT DISTINCT FROM %s
                  AND lower(display_name) = lower(%s)
                  AND status = 'active'
                  AND deleted_at IS NULL
                LIMIT 1
                """,
                (int(rule_id), bank, display_name.strip()),
            )
            return cur.fetchone() is not None

    def copy_intro_to_rule(
        self,
        rule_id: int,
        intro_id: int,
        *,
        created_by: int | None = None,
        tenant_id: int = 1,
        bank: str | None = None,
    ) -> int | None:
        source = self.get_intro(intro_id)
        if source is None:
            return None

        base_name = source.display_name
        candidate = base_name
        counter = 2

        with self.connect() as conn:
            copy_bank = bank if bank is not None else source.bank
            while self._rule_intro_display_name_exists(conn, rule_id, candidate, copy_bank):
                candidate = f"{base_name}_{counter}"
                counter += 1

            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO intros(
                        rule_id, tenant_id, created_by, display_name, file_name,
                        file_path, duration, media_kind, bank, status, created_at, updated_at
                    )
                    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, 'active', %s, NOW())
                    RETURNING id
                    """,
                    (
                        int(rule_id),
                        int(tenant_id),
                        created_by,
                        candidate,
                        source.file_name,
                        source.file_path,
                        int(source.duration or 0),
                        source.media_kind,
                        copy_bank,
                        utc_now_iso(),
                    ),
                )
                row = cur.fetchone()
            conn.commit()

        return int(row["id"]) if row else None

    def _find_existing_rule_intro_copy(self, rule_id: int, source: IntroItem, bank: str | None = None) -> int | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id
                    FROM intros
                    WHERE rule_id = %s
                      AND file_path = %s
                      AND display_name = %s
                      AND bank IS NOT DISTINCT FROM %s
                      AND status = 'active'
                      AND deleted_at IS NULL
                    ORDER BY id ASC
                    LIMIT 1
                    """,
                    (int(rule_id), source.file_path, source.display_name, bank),
                )
                row = cur.fetchone()

        return int(row["id"]) if row else None

    def migrate_legacy_rule_intro_assignments(self) -> dict[str, int]:
        stats = {
            "rules_scanned": 0,
            "assignments_migrated": 0,
            "intro_copies_created": 0,
            "missing_intro_skipped": 0,
        }
        logger.info("INTRO_LEGACY_ASSIGNMENTS_MIGRATION_STARTED")

        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, tenant_id, video_intro_horizontal_id, video_intro_vertical_id
                    FROM routing
                    WHERE video_intro_horizontal_id IS NOT NULL
                       OR video_intro_vertical_id IS NOT NULL
                    ORDER BY id ASC
                    """
                )
                rows = cur.fetchall()

        stats["rules_scanned"] = len(rows)
        copied_by_source: dict[tuple[int, int], tuple[int, bool]] = {}

        for row in rows:
            rule_id = int(row["id"])
            tenant_id = int(row.get("tenant_id") or 1) if hasattr(row, "get") or isinstance(row, dict) else 1
            for field, column in (
                ("horizontal", "video_intro_horizontal_id"),
                ("vertical", "video_intro_vertical_id"),
            ):
                old_intro_id = row.get(column) if hasattr(row, "get") else row[column]
                if old_intro_id is None:
                    continue

                source = self.get_intro(int(old_intro_id))
                if source is None:
                    stats["missing_intro_skipped"] += 1
                    continue
                if source.rule_id == rule_id:
                    continue

                cache_key = (rule_id, int(old_intro_id))
                cached = copied_by_source.get(cache_key)
                if cached is not None:
                    new_intro_id, created = cached
                else:
                    existing_id = self._find_existing_rule_intro_copy(rule_id, source)
                    if existing_id is not None:
                        new_intro_id = existing_id
                        created = False
                    else:
                        new_intro_id = self.copy_intro_to_rule(
                            rule_id=rule_id,
                            intro_id=int(old_intro_id),
                            tenant_id=tenant_id or source.tenant_id or 1,
                        )
                        if new_intro_id is None:
                            stats["missing_intro_skipped"] += 1
                            continue
                        created = True
                    copied_by_source[cache_key] = (int(new_intro_id), created)

                if created:
                    stats["intro_copies_created"] += 1
                    copied_by_source[cache_key] = (int(new_intro_id), False)

                update_method = self.set_rule_intro_horizontal if field == "horizontal" else self.set_rule_intro_vertical
                if update_method(rule_id, int(new_intro_id)):
                    stats["assignments_migrated"] += 1
                    logger.info(
                        "INTRO_LEGACY_ASSIGNMENT_MIGRATED | rule_id=%s | old_intro_id=%s | new_intro_id=%s | field=%s",
                        rule_id,
                        old_intro_id,
                        new_intro_id,
                        field,
                    )

        return stats

    def migrate_rule_intro_banks(self) -> dict[str, int]:
        stats = {
            "rules_scanned": 0,
            "intro_banks_set": 0,
            "intro_copies_created": 0,
            "routing_assignments_updated": 0,
            "legacy_unassigned_copied": 0,
        }
        logger.info("INTRO_BANK_MIGRATION_STARTED")

        try:
            with self.connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT id, tenant_id, video_intro_horizontal_id, video_intro_vertical_id
                        FROM routing
                        WHERE video_intro_horizontal_id IS NOT NULL
                           OR video_intro_vertical_id IS NOT NULL
                        ORDER BY id ASC
                        """
                    )
                    rules = cur.fetchall()
            stats["rules_scanned"] = len(rules)

            for rule in rules:
                rule_id = int(rule["id"])
                tenant_id = int(rule.get("tenant_id") or 1) if hasattr(rule, "get") else 1
                horizontal_id = rule.get("video_intro_horizontal_id") if hasattr(rule, "get") else rule["video_intro_horizontal_id"]
                vertical_id = rule.get("video_intro_vertical_id") if hasattr(rule, "get") else rule["video_intro_vertical_id"]

                if horizontal_id and vertical_id and int(horizontal_id) == int(vertical_id):
                    intro = self.get_rule_intro(rule_id, int(horizontal_id), include_deleted=False)
                    if intro and intro.bank is None:
                        with self.connect() as conn:
                            with conn.cursor() as cur:
                                cur.execute(
                                    """
                                    UPDATE intros
                                    SET bank = 'horizontal', updated_at = NOW()
                                    WHERE id = %s AND rule_id = %s AND bank IS NULL
                                    """,
                                    (int(horizontal_id), rule_id),
                                )
                                if cur.rowcount:
                                    stats["intro_banks_set"] += 1
                            conn.commit()
                        new_vertical_id = self.copy_intro_to_rule(
                            rule_id=rule_id,
                            intro_id=int(horizontal_id),
                            tenant_id=tenant_id or intro.tenant_id or 1,
                            bank="vertical",
                        )
                        if new_vertical_id:
                            if self.set_rule_intro_vertical(rule_id, int(new_vertical_id)):
                                stats["routing_assignments_updated"] += 1
                            stats["intro_copies_created"] += 1
                    continue

                for bank, intro_id in (("horizontal", horizontal_id), ("vertical", vertical_id)):
                    if not intro_id:
                        continue
                    intro = self.get_rule_intro(rule_id, int(intro_id), include_deleted=False)
                    if intro and intro.bank is None:
                        with self.connect() as conn:
                            with conn.cursor() as cur:
                                cur.execute(
                                    """
                                    UPDATE intros
                                    SET bank = %s, updated_at = NOW()
                                    WHERE id = %s AND rule_id = %s AND bank IS NULL
                                    """,
                                    (bank, int(intro_id), rule_id),
                                )
                                if cur.rowcount:
                                    stats["intro_banks_set"] += 1
                            conn.commit()

            with self.connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT i.id, i.rule_id, i.tenant_id
                        FROM intros i
                        WHERE i.rule_id IS NOT NULL
                          AND i.bank IS NULL
                          AND i.status = 'active'
                          AND i.deleted_at IS NULL
                          AND NOT EXISTS (
                              SELECT 1 FROM routing r
                              WHERE r.id = i.rule_id
                                AND (r.video_intro_horizontal_id = i.id OR r.video_intro_vertical_id = i.id)
                          )
                        ORDER BY i.id ASC
                        """
                    )
                    unassigned = cur.fetchall()

            for row in unassigned:
                intro_id = int(row["id"])
                rule_id = int(row["rule_id"])
                tenant_id = int(row.get("tenant_id") or 1) if hasattr(row, "get") else 1
                source = self.get_rule_intro(rule_id, intro_id, include_deleted=False)
                if source is None:
                    continue
                for bank in ("horizontal", "vertical"):
                    existing_id = self._find_existing_rule_intro_copy(rule_id, source, bank)
                    if existing_id is not None:
                        continue
                    new_intro_id = self.copy_intro_to_rule(
                        rule_id=rule_id,
                        intro_id=intro_id,
                        tenant_id=tenant_id,
                        bank=bank,
                    )
                    if new_intro_id:
                        stats["intro_copies_created"] += 1
                        stats["legacy_unassigned_copied"] += 1

            logger.info("INTRO_BANK_MIGRATION_DONE | stats=%s", stats)
            return stats
        except Exception:
            logger.exception("INTRO_BANK_MIGRATION_FAILED")
            return stats

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

            message_id = self._get_post_message_id_conn(conn, post_id)
            now_iso = utc_now_iso()
            for rule in rules:
                if self._is_self_target_echo_post_for_rule_conn(
                    conn,
                    rule_id=int(rule["id"]),
                    source_channel=str(source_channel),
                    source_thread_id=source_thread_id,
                    message_id=message_id,
                ):
                    self._log_self_target_echo_delivery_skipped(
                        rule_id=int(rule["id"]),
                        source_channel=str(source_channel),
                        source_thread_id=source_thread_id,
                        message_id=message_id,
                    )
                    continue
                cur.execute(
                    """
                    INSERT INTO deliveries(rule_id, post_id, status, created_at, tenant_id)
                    VALUES(%s, %s, 'pending', %s, (SELECT tenant_id FROM routing WHERE id = %s))
                    ON CONFLICT DO NOTHING
                    """,
                    (int(rule["id"]), int(post_id), now_iso, int(rule["id"])),
                )

    def _get_post_message_id_conn(self, conn, post_id: int) -> int:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT message_id
                FROM posts
                WHERE id = %s
                LIMIT 1
                """,
                (int(post_id),),
            )
            row = cur.fetchone()

        if not row:
            raise RuntimeError("Не удалось найти post для создания delivery")

        return int(row["message_id"])

    def _is_self_target_echo_post_for_rule_conn(
        self,
        conn,
        *,
        rule_id: int,
        source_channel: str,
        source_thread_id: int | None,
        message_id: int,
    ) -> bool:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1
                FROM routing r
                JOIN deliveries d ON d.rule_id = r.id
                WHERE r.id = %s
                  AND r.source_id::text = r.target_id::text
                  AND r.source_id::text = %s
                  AND COALESCE(r.source_thread_id, -1) = COALESCE(%s, -1)
                  AND r.source_thread_id IS NOT DISTINCT FROM r.target_thread_id
                  AND (
                    (
                      d.status = 'sent'
                      AND (
                        d.sent_message_id = %s
                        OR COALESCE(d.sent_message_ids_json, '[]'::jsonb)
                           @> jsonb_build_array(%s::bigint)
                      )
                    )
                    OR EXISTS (
                      SELECT 1 FROM delivery_attempts a
                      WHERE a.delivery_id = d.id
                        AND a.rule_id = r.id
                        AND a.status = 'verified'
                        AND a.target_id::text = r.target_id::text
                        AND COALESCE(a.sent_message_ids_json, '[]'::jsonb)
                            @> jsonb_build_array(%s::bigint)
                    )
                  )
                LIMIT 1
                """,
                (int(rule_id), str(source_channel), source_thread_id,
                 int(message_id), int(message_id), int(message_id)),
            )
            row = cur.fetchone()

            if row is not None:
                return True
            # Exact mapping may arrive before finalization; archived receipts
            # remain evidence of this rule's own publications.
            cur.execute(
                """
                SELECT 1 FROM telegram_publication_receipts pr
                JOIN routing rr ON rr.id=pr.rule_id
                WHERE pr.rule_id=%s AND pr.source_channel=pr.target_peer_id::text
                  AND rr.source_id::text=pr.source_channel AND rr.target_id::text=pr.source_channel
                  AND COALESCE(rr.tenant_id,1)=pr.tenant_id
                  AND rr.source_thread_id IS NOT DISTINCT FROM pr.source_thread_id
                  AND rr.target_thread_id IS NOT DISTINCT FROM pr.target_thread_id
                  AND pr.source_channel=%s AND pr.target_thread_id IS NOT DISTINCT FROM %s
                  AND pr.source_thread_id IS NOT DISTINCT FROM pr.target_thread_id
                  AND (%s=ANY(pr.published_ids)
                    OR EXISTS (
                        SELECT 1 FROM unnest(pr.scheduled_ids,pr.pending_mask) AS member(mid,pending)
                        WHERE member.pending IS FALSE AND member.mid=%s
                    )
                    OR EXISTS (
                        SELECT 1 FROM telegram_publication_claims c
                        JOIN telegram_publication_mappings m
                          ON m.sender_account_id=c.sender_account_id
                         AND m.target_peer_id=c.target_peer_id AND m.scheduled_id=c.scheduled_id
                        WHERE c.idempotency_key=pr.idempotency_key AND m.published_id=%s
                          AND NOT EXISTS (
                            SELECT 1 FROM telegram_publication_mappings x
                            WHERE x.sender_account_id=m.sender_account_id AND x.target_peer_id=m.target_peer_id
                              AND ((x.scheduled_id=m.scheduled_id AND x.published_id<>m.published_id)
                                OR (x.published_id=m.published_id AND x.scheduled_id<>m.scheduled_id))
                          )
                    ))
                UNION ALL
                SELECT 1 FROM audit_log a
                JOIN routing rr ON rr.id=a.rule_id
                WHERE a.rule_id=%s AND a.event_type='rule_rescan_echo_snapshot'
                  AND COALESCE(rr.tenant_id,1)=a.tenant_id
                  AND a.source_id=rr.source_id AND a.target_id=rr.target_id
                  AND a.source_id=a.target_id AND a.source_id=%s
                  AND a.source_thread_id IS NOT DISTINCT FROM rr.source_thread_id
                  AND a.target_thread_id IS NOT DISTINCT FROM rr.target_thread_id
                  AND a.source_thread_id IS NOT DISTINCT FROM a.target_thread_id
                  AND a.source_thread_id IS NOT DISTINCT FROM %s
                  AND CASE WHEN a.event_type='rule_rescan_echo_snapshot'
                    THEN a.extra_json::jsonb->'sent_message_ids' @> jsonb_build_array(%s::bigint)
                    ELSE FALSE END
                LIMIT 1
                """, (rule_id,str(source_channel),source_thread_id,message_id,message_id,message_id,
                      rule_id,str(source_channel),source_thread_id,message_id),
            )
            return cur.fetchone() is not None

    def _log_self_target_echo_delivery_skipped(
        self,
        *,
        rule_id: int,
        source_channel: str,
        source_thread_id: int | None,
        message_id: int,
    ) -> None:
        logger.warning(
            "SELF_TARGET_ECHO_DELIVERY_SKIPPED | rule_id=%s | source_channel=%s | source_thread_id=%s | message_id=%s | reason=message_is_previous_sent_output",
            rule_id,
            source_channel,
            source_thread_id,
            message_id,
        )

    def _backfill_deliveries_for_rule_conn(
        self,
        conn,
        rule_id: int,
        source_id: str,
        source_thread_id: int | None,
        *,
        allowed_message_ids: set[int] | None = None,
        include_newer_than_message_id: int | None = None,
    ) -> int:
        with conn.cursor() as cur:
            if source_thread_id is None:
                cur.execute(
                    """
                    SELECT id, message_id
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
                    SELECT id, message_id
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
                if (allowed_message_ids is not None
                        and int(row["message_id"]) not in allowed_message_ids
                        and not (
                            include_newer_than_message_id is not None
                            and int(row["message_id"]) > include_newer_than_message_id
                        )):
                    continue
                if self._is_self_target_echo_post_for_rule_conn(
                    conn,
                    rule_id=int(rule_id),
                    source_channel=str(source_id),
                    source_thread_id=source_thread_id,
                    message_id=int(row["message_id"]),
                ):
                    self._log_self_target_echo_delivery_skipped(
                        rule_id=int(rule_id),
                        source_channel=str(source_id),
                        source_thread_id=source_thread_id,
                        message_id=int(row["message_id"]),
                    )
                    continue
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

    def save_post_batch(self, posts_data, *, preserve_existing_deliveries: bool = False):
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
                        if preserve_existing_deliveries:
                            raise RuntimeError("Не удалось сохранить сообщение при пересканировании")
                        continue

                    post_id = int(row["id"])
                    if inserted:
                        count += 1
                    else:
                        # History rescans refresh metadata without replacing posts
                        # or discarding per-rule delivery/attempt identities.
                        cur.execute(
                            """
                            UPDATE posts
                            SET content_json = %s, media_group_id = %s, is_faulty = FALSE
                            WHERE id = %s
                              AND (
                                  content_json IS DISTINCT FROM %s
                                  OR media_group_id IS DISTINCT FROM %s
                                  OR is_faulty = TRUE
                              )
                            """,
                            (
                                _json_dumps(content) or "{}", media_group_id, post_id,
                                _json_dumps(content) or "{}", media_group_id,
                            ),
                        )
                    if inserted or not preserve_existing_deliveries:
                        self._create_deliveries_for_post_conn(
                            conn, post_id, str(source_channel), source_thread_id,
                        )

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
                        video_clip_duration_seconds,
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
                        120, 118, FALSE, NULL, NULL, NULL, NULL, NULL, NULL, %s
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
                    "UPDATE routing SET is_active = %s WHERE id = %s AND rescan_owner_token IS NULL",
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
                # Atomically read the authoritative queue marker at the same
                # moment activation is allowed. A preceding SELECT could
                # race with another process finishing a rescan.
                cur.execute(
                    """
                    UPDATE routing
                    SET is_active = TRUE
                    WHERE id = %s AND rescan_owner_token IS NULL
                    RETURNING id, source_id, source_thread_id, rescan_queue_built
                    """,
                    (int(rule_id),),
                )
                row = cur.fetchone()
                if not row:
                    conn.commit()
                    return False

            # A completed, observed-ID-filtered rescan has already built its
            # authoritative queue. An unfiltered backfill would reintroduce
            # messages deleted from Telegram when this rule is re-enabled.
            if not bool(row["rescan_queue_built"]):
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

    def get_rule_scan_start_max_message_id(self, rule_id: int) -> int:
        """Cached high-water mark at scan start; live IDs increase monotonically.

        A message arriving after this snapshot must not be erased just because
        the finite Telegram history iterator did not observe it.
        """
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COALESCE(MAX(p.message_id), 0) AS max_message_id
                    FROM routing r
                    LEFT JOIN posts p
                      ON p.source_channel = r.source_id
                     AND p.source_thread_id IS NOT DISTINCT FROM r.source_thread_id
                    WHERE r.id = %s
                    """,
                    (int(rule_id),),
                )
                row = cur.fetchone()
        return int(row["max_message_id"] or 0) if row else 0

    def _assert_rule_replay_safe(self, cur, rule_id: int) -> None:
        """Allow explicitly confirmed replay of historical accepted video records.

        Candidate IDs are not proof of publication. Legacy rows are archived
        without promotion to verified, only when no modern receipt/intent or
        unfinished delivery exists. The #507 merge cutoff is not proof of send.
        """
        cur.execute(
            """
            SELECT COUNT(*) AS unresolved, MIN(a.id) AS first_attempt_id
            FROM delivery_attempts a
            JOIN routing r ON r.id=a.rule_id
            LEFT JOIN deliveries d ON d.id=a.delivery_id
            WHERE a.rule_id=%s
              AND a.status IN ('sending','accepted','failed_after_send')
              AND a.idempotency_key NOT LIKE '%%:rescan:%%'
              AND a.idempotency_key NOT LIKE 'archived:%%'
              AND NOT COALESCE((
                a.status='accepted'
                AND a.operation_kind='video_send'
                AND a.telegram_method='video_send'
                AND a.error_text IS NULL
                AND a.updated_at < TIMESTAMPTZ '2026-11-22 17:56:18+00'
                AND a.target_id=r.target_id
                AND a.tenant_id=COALESCE(r.tenant_id,1)
                AND a.idempotency_key=('delivery:' || a.delivery_id::text
                    || ':target:' || a.target_id || ':video_send:v1')
                AND CASE
                    WHEN jsonb_typeof(a.sent_message_ids_json)='array'
                    THEN jsonb_array_length(a.sent_message_ids_json)=1
                    ELSE FALSE
                END
                AND jsonb_typeof(a.sent_message_ids_json->0)='number'
                AND a.sent_message_ids_json->>0 ~ '^[1-9][0-9]*$'
                AND (d.id IS NULL OR (
                    d.rule_id=a.rule_id AND d.status='sent'
                    AND d.sent_message_id>0
                    AND a.sent_message_ids_json=jsonb_build_array(d.sent_message_id)
                    AND COALESCE(d.target_id_snapshot,a.target_id)=a.target_id
                ))
                AND NOT EXISTS (
                    SELECT 1 FROM mtproto_send_intents i WHERE i.attempt_id=a.id
                )
                AND NOT EXISTS (
                    SELECT 1 FROM telegram_publication_receipts pr
                    WHERE pr.idempotency_key=a.idempotency_key
                )
              ), FALSE)
            """, (int(rule_id),),
        )
        blocked = cur.fetchone()
        if blocked["unresolved"]:
            raise RescanBlocked(
                "Есть отправка с неизвестным результатом; "
                f"записей: {blocked['unresolved']}, "
                f"первая попытка: {blocked.get('first_attempt_id')}. "
                "Требуется ручная проверка."
            )
        cur.execute(
            """SELECT idempotency_key FROM telegram_publication_receipts
                WHERE rule_id=%s AND (published_ids IS NULL OR reaction_state IN ('pending','dispatching'))
                ORDER BY idempotency_key LIMIT 1""", (int(rule_id),),
        )
        if cur.fetchone():
            raise RescanBlocked("Есть незавершённая публикация или реакция; повтор запрещён")

    def _assert_keep_rescan_safe(self, cur, rule_id: int) -> None:
        cur.execute(
            """
            SELECT COUNT(*) AS unresolved, MIN(a.id) AS first_attempt_id
            FROM delivery_attempts a
            WHERE a.rule_id=%s AND a.status IN ('sending','accepted','verified','failed_after_send')
              AND a.idempotency_key NOT LIKE '%%:rescan:%%' AND a.idempotency_key NOT LIKE 'archived:%%'
              AND NOT EXISTS (SELECT 1 FROM deliveries d WHERE d.id=a.delivery_id AND d.rule_id=a.rule_id)
              AND NOT COALESCE((
                CASE WHEN a.operation_kind='album' THEN
                    EXISTS (SELECT 1 FROM telegram_publication_receipts pr
                        WHERE pr.idempotency_key=a.idempotency_key AND pr.rule_id=a.rule_id)
                    OR (a.target_id IS NOT NULL
                        AND starts_with(a.idempotency_key,'rule:'||a.rule_id::text||':target:'||a.target_id||':media_group:')
                        AND right(a.idempotency_key,6)=':album'
                        AND length(a.idempotency_key)>length('rule:'||a.rule_id::text||':target:'||a.target_id||':media_group::album'))
                WHEN a.operation_kind IN ('single','video_send') THEN
                    jsonb_typeof(a.source_message_ids_json)='array'
                    AND a.source_message_ids_json <> '[]'::jsonb
                    AND NOT EXISTS (SELECT 1 FROM jsonb_array_elements(
                        CASE WHEN jsonb_typeof(a.source_message_ids_json)='array'
                             THEN a.source_message_ids_json ELSE '[]'::jsonb END) AS mid(value)
                        WHERE jsonb_typeof(mid.value)<>'number' OR mid.value::text !~ '^[1-9][0-9]*$')
                ELSE FALSE END
              ),FALSE)
            """, (int(rule_id),),
        )
        blocked = cur.fetchone()
        if blocked["unresolved"]:
            raise RescanBlocked(
                f"У старых отправок нет точной привязки к источнику: {blocked['unresolved']}; "
                f"первая попытка: {blocked.get('first_attempt_id')}. Требуется ручная проверка."
            )

    def issue_rule_rescan_confirmation(self, rule_id: int, actor_id: int, token: str) -> bool:
        """Replace any old confirmation; bind it to a rule and its requesting actor."""
        if not token or len(token) > 32 or not actor_id:
            return False
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """UPDATE routing
                       SET rescan_confirmation_token=%s, rescan_confirmation_actor=%s,
                           rescan_confirmation_until=NOW() + INTERVAL '15 minutes'
                       WHERE id=%s AND rescan_owner_token IS NULL
                       RETURNING id""",
                    (str(token), int(actor_id), int(rule_id)),
                )
                issued = cur.fetchone() is not None
            conn.commit()
        return issued

    def consume_rule_rescan_confirmation(self, rule_id: int, actor_id: int, token: str) -> bool:
        """Atomic one-shot confirmation even across workers or restarts."""
        if not token or not actor_id:
            return False
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """UPDATE routing
                       SET rescan_confirmation_token=NULL, rescan_confirmation_actor=NULL,
                           rescan_confirmation_until=NULL
                       WHERE id=%s AND rescan_confirmation_token=%s
                         AND rescan_confirmation_actor=%s
                         AND rescan_confirmation_until>NOW()
                         AND rescan_owner_token IS NULL
                       RETURNING id""",
                    (int(rule_id), str(token), int(actor_id)),
                )
                consumed = cur.fetchone() is not None
            conn.commit()
        return consumed

    def begin_rule_rescan(self, rule_id: int, *, fresh: bool = False) -> tuple[bool, str]:
        """Acquire a PostgreSQL-backed exclusive rescan owner before parsing.

        Ownership survives individual DB transactions and excludes independent
        handlers/processes. Crashed scans remain paused and require explicit
        operator recovery rather than risking an unverified automatic replay.
        """
        owner_token = uuid.uuid4().hex
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT is_active, rescan_owner_token FROM routing WHERE id = %s FOR UPDATE",
                    (int(rule_id),),
                )
                rule = cur.fetchone()
                if rule is None:
                    raise RescanBlocked(f"Правило #{rule_id} не найдено")
                if rule["rescan_owner_token"] is not None:
                    raise RescanBlocked(
                        "Пересканирование этого правила уже выполняется. "
                        "Повторный запуск запрещён до завершения текущего."
                    )
                cur.execute(
                    """
                    SELECT EXISTS(
                        SELECT 1 FROM deliveries
                        WHERE rule_id = %s AND status = 'processing'
                    ) OR EXISTS(
                        SELECT 1 FROM jobs
                        WHERE payload_json->>'rule_id' = %s
                          AND status IN ('pending', 'leased', 'processing', 'retry')
                    ) AS busy
                    """,
                    (int(rule_id), str(rule_id)),
                )
                if cur.fetchone()["busy"]:
                    raise RescanBlocked(
                        "У правила есть незавершённые доставки или задачи; "
                        "пересканирование отложено во избежание дубликатов"
                    )
                if fresh:
                    self._assert_rule_replay_safe(cur, rule_id)
                else:
                    self._assert_keep_rescan_safe(cur, rule_id)
                cur.execute(
                    """
                    UPDATE routing
                    SET is_active = FALSE, rescan_owner_token = %s,
                        rescan_started_at = NOW()
                    WHERE id = %s AND rescan_owner_token IS NULL
                    """,
                    (owner_token, int(rule_id)),
                )
                if cur.rowcount != 1:
                    raise RescanBlocked("Другое пересканирование получило блокировку")
            conn.commit()
        return bool(rule["is_active"]), owner_token

    def finish_rule_rescan(
        self, rule_id: int, *, was_active: bool, owner_token: str,
        queue_rebuilt: bool = False,
    ) -> None:
        """Only the original owner can resume or release a rescan."""
        if not owner_token:
            raise ValueError("Missing rescan owner")
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE routing
                    SET is_active = %s, rescan_owner_token = NULL,
                        rescan_started_at = NULL,
                        rescan_queue_built = rescan_queue_built OR %s
                    WHERE id = %s AND rescan_owner_token = %s
                    """,
                    (bool(was_active), bool(queue_rebuilt), int(rule_id), str(owner_token)),
                )
                if cur.rowcount != 1:
                    raise RuntimeError(
                        "Владелец пересканирования изменился; нельзя менять статус правила"
                    )
            conn.commit()

    def _snapshot_rescan_echo_ids(self, cur, rule_id: int) -> None:
        """Keep existing self-loop output evidence when its delivery anchors are removed."""
        cur.execute(
            """
            INSERT INTO audit_log(created_at,event_type,rule_id,tenant_id,source_id,
                                  source_thread_id,target_id,target_thread_id,extra_json)
            SELECT %s,'rule_rescan_echo_snapshot',r.id,COALESCE(r.tenant_id,1),
                   r.source_id,r.source_thread_id,r.target_id,r.target_thread_id,
                   (jsonb_populate_record(
                       NULL::audit_log,
                       jsonb_build_object(
                           'extra_json',
                           jsonb_build_object('sent_message_ids', jsonb_agg(DISTINCT e.mid))
                       )
                   )).extra_json
            FROM routing r
            JOIN deliveries d ON d.rule_id=r.id
            CROSS JOIN LATERAL (
                SELECT to_jsonb(d.sent_message_id) AS mid WHERE d.status='sent' AND d.sent_message_id>0
                UNION
                SELECT value FROM jsonb_array_elements(
                    CASE WHEN d.status='sent' AND jsonb_typeof(d.sent_message_ids_json)='array'
                         THEN d.sent_message_ids_json ELSE '[]'::jsonb END)
                UNION
                SELECT value FROM delivery_attempts a
                CROSS JOIN LATERAL jsonb_array_elements(
                    CASE WHEN jsonb_typeof(a.sent_message_ids_json)='array'
                         THEN a.sent_message_ids_json ELSE '[]'::jsonb END)
                WHERE a.delivery_id=d.id AND a.rule_id=r.id AND a.status='verified'
                  AND a.target_id=r.target_id
            ) e
            WHERE r.id=%s AND r.source_id=r.target_id
              AND r.source_thread_id IS NOT DISTINCT FROM r.target_thread_id
              AND COALESCE(d.target_id_snapshot,r.target_id)=r.target_id
              AND jsonb_typeof(e.mid)='number' AND e.mid::text ~ '^[1-9][0-9]*$'
            GROUP BY r.id
            """, (utc_now_iso(),int(rule_id)),
        )

    def _lock_rescan_commit(self, cur, rule_id: int, owner_token: str, expected_scope):
        cur.execute(
            "SELECT is_active, rescan_owner_token, source_id, source_thread_id, "
            "target_id, target_thread_id, mode FROM routing WHERE id = %s FOR UPDATE",
            (int(rule_id),),
        )
        rule = cur.fetchone()
        if (rule is None or rule["is_active"] or not owner_token
                or rule["rescan_owner_token"] != owner_token):
            raise RescanBlocked("Для фиксации изменений необходимо владеть остановленным процессом пересканирования")
        if expected_scope is not None and expected_scope != (
            str(rule["source_id"]), rule["source_thread_id"],
            str(rule["target_id"]), rule["target_thread_id"], rule["mode"],
        ):
            raise RescanBlocked("Настройки источника или получателя изменились; повторите пересканирование")
        cur.execute(
            """SELECT EXISTS(SELECT 1 FROM deliveries
                   WHERE rule_id=%s AND status='processing')
                OR EXISTS(SELECT 1 FROM jobs WHERE payload_json->>'rule_id'=%s
                   AND status IN ('pending','leased','processing','retry')) AS busy""",
            (int(rule_id), str(rule_id)),
        )
        if cur.fetchone()["busy"]:
            raise RescanBlocked("Остались активные задачи; пересканирование отменено")
        return rule

    def reset_rule_history_for_replay(
        self, rule_id: int, *, owner_token: str,
        allowed_message_ids: set[int], include_newer_than_message_id: int,
        expected_scope: tuple | None = None,
    ) -> tuple[int, int, int]:
        """Explicit 'start over': archive old attempt keys, then reset only this rule.

        Retain original attempt rows and Telegram receipts for audit. A new
        replay must use new idempotency keys, otherwise accepted historical
        writes are incorrectly counted as fresh deliveries.
        """
        with self.connect() as conn:
            with conn.cursor() as cur:
                rule = self._lock_rescan_commit(cur, rule_id, owner_token, expected_scope)
                self._assert_rule_replay_safe(cur, rule_id)
                self._snapshot_rescan_echo_ids(cur, rule_id)
                cur.execute("""
                    UPDATE telegram_publication_receipts r
                    SET idempotency_key=r.idempotency_key || ':rescan:' || a.id::text
                    FROM delivery_attempts a
                    WHERE r.rule_id=%s AND a.idempotency_key=r.idempotency_key
                      AND r.published_ids IS NOT NULL
                      AND r.idempotency_key NOT LIKE '%%:rescan:%%'
                    """, (int(rule_id),))

                # Unique suffix incorporates immutable attempt primary key.
                # Previously archived rows are excluded, including on repeat reset.
                cur.execute(
                    """
                    UPDATE delivery_attempts
                    SET idempotency_key = idempotency_key || ':rescan:' || id::text,
                        updated_at = NOW()
                    WHERE rule_id = %s
                      AND idempotency_key NOT LIKE '%%:rescan:%%'
                    """,
                    (int(rule_id),),
                )
                archived = int(cur.rowcount or 0)
                cur.execute("DELETE FROM deliveries WHERE rule_id = %s", (int(rule_id),))
                deleted = int(cur.rowcount or 0)
                rebuilt = self._backfill_deliveries_for_rule_conn(
                    conn, int(rule_id), str(rule["source_id"]), rule["source_thread_id"],
                    allowed_message_ids=allowed_message_ids,
                    include_newer_than_message_id=include_newer_than_message_id,
                )
                cur.execute("UPDATE routing SET rescan_queue_built=TRUE,next_run_at=%s WHERE id=%s", (utc_now_iso(),rule_id))
            conn.commit()
        return archived, deleted, rebuilt

    def clear_rule_deliveries(self, rule_id: int) -> int:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM routing WHERE id=%s AND rescan_owner_token IS NULL FOR UPDATE", (rule_id,))
                if not cur.fetchone():
                    raise RescanBlocked("Правило недоступно или пересканируется")
                cur.execute("""SELECT 1 FROM telegram_publication_receipts
                    WHERE rule_id=%s AND (published_ids IS NULL OR reaction_state IN ('pending','dispatching')) LIMIT 1""", (rule_id,))
                if cur.fetchone():
                    raise RuntimeError("Есть незавершённая публикация или реакция")
                cur.execute("DELETE FROM deliveries WHERE rule_id = %s", (rule_id,))
                count = int(cur.rowcount or 0)
            conn.commit()
            return count

    def backfill_rule(
        self, rule_id: int, *, allowed_message_ids: set[int] | None = None,
        include_newer_than_message_id: int | None = None,
    ) -> int:
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
                allowed_message_ids=allowed_message_ids,
                include_newer_than_message_id=include_newer_than_message_id,
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

    def _prune_rule_pending_conn(
        self, conn, rule_id: int, source_channel: str, source_thread_id: int | None,
        *, seen_message_ids: set[int] | None = None, scan_start_max_message_id: int = 0,
        before_message_id: int | None = None,
        before_snapshot_max_message_id: int | None = None,
    ) -> int:
        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM deliveries d USING posts p
                WHERE d.post_id = p.id
                  AND d.rule_id = %s AND d.status = 'pending'
                  AND p.source_channel = %s
                  AND p.source_thread_id IS NOT DISTINCT FROM %s
                  AND NOT EXISTS (
                      SELECT 1 FROM delivery_attempts a
                      LEFT JOIN deliveries ad ON ad.id=a.delivery_id AND ad.rule_id=a.rule_id
                      LEFT JOIN posts ap ON ap.id=ad.post_id
                      WHERE a.rule_id=d.rule_id
                        AND a.status IN ('sending','accepted','verified','failed_after_send')
                        AND a.idempotency_key NOT LIKE '%%:rescan:%%'
                        AND a.idempotency_key NOT LIKE 'archived:%%'
                        AND (a.delivery_id=d.id
                            OR (a.operation_kind IN ('single','video_send')
                                AND a.source_message_ids_json @> jsonb_build_array(p.message_id))
                            OR (a.operation_kind='album' AND p.media_group_id IS NOT NULL AND (
                                a.idempotency_key=('rule:'||a.rule_id::text||':target:'||a.target_id
                                    ||':media_group:'||p.media_group_id||':album')
                                OR (ap.media_group_id=p.media_group_id
                                    AND ap.source_channel=p.source_channel
                                    AND ap.source_thread_id IS NOT DISTINCT FROM p.source_thread_id)
                            )))
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM telegram_publication_receipts pr
                      WHERE pr.rule_id=d.rule_id AND d.id=ANY(pr.delivery_ids)
                        AND pr.published_ids IS NULL
                  )
                  AND (
                      (%s::bigint IS NOT NULL AND p.message_id < %s::bigint
                          AND (%s::bigint IS NULL OR p.message_id <= %s::bigint))
                      OR (%s::bigint IS NULL AND NOT (p.message_id = ANY(%s::bigint[]))
                          AND p.message_id <= %s)
                  )
                """,
                (int(rule_id), str(source_channel), source_thread_id,
                 before_message_id, before_message_id,
                 before_snapshot_max_message_id, before_snapshot_max_message_id,
                 before_message_id, sorted(seen_message_ids or set()),
                 int(scan_start_max_message_id)),
            )
            return int(cur.rowcount or 0)

    def drop_rule_pending_before_message(
        self, rule_id: int, source_channel: str, source_thread_id: int | None, message_id: int,
    ) -> int:
        with self.connect() as conn:
            count = self._prune_rule_pending_conn(
                conn, rule_id, source_channel, source_thread_id, before_message_id=message_id,
            )
            conn.commit()
            return count

    def drop_rule_pending_not_in_message_ids(
        self, rule_id: int, *, source_channel: str, source_thread_id: int | None,
        seen_message_ids: set[int], scan_start_max_message_id: int,
    ) -> int:
        """Prune stale snapshot posts, never new arrivals after scan started."""
        with self.connect() as conn:
            count = self._prune_rule_pending_conn(
                conn, rule_id, source_channel, source_thread_id,
                seen_message_ids=seen_message_ids, scan_start_max_message_id=scan_start_max_message_id,
            )
            conn.commit()
            return count

    def _quarantine_rescan_orphans(self, cur, rule_id: int) -> int:
        """A lost delivery ID must not turn an accepted write into a fresh send."""
        cur.execute(
            """
            UPDATE deliveries d SET status='faulty',error_text='historical_attempt_unverified_no_resend'
            FROM posts p, routing r
            WHERE d.rule_id=%s AND r.id=d.rule_id AND d.post_id=p.id AND d.status='pending'
              AND EXISTS (
                SELECT 1 FROM posts member
                WHERE member.source_channel=p.source_channel
                  AND member.source_thread_id IS NOT DISTINCT FROM p.source_thread_id
                  AND (member.id=p.id OR (r.mode='repost' AND p.media_group_id IS NOT NULL
                       AND member.media_group_id=p.media_group_id))
                  AND (
                    EXISTS (
                        SELECT 1 FROM delivery_attempts a
                        WHERE a.rule_id=d.rule_id AND a.status IN ('sending','accepted','verified','failed_after_send')
                          AND a.idempotency_key NOT LIKE '%%:rescan:%%' AND a.idempotency_key NOT LIKE 'archived:%%'
                          AND (
                            (a.operation_kind IN ('single','video_send')
                             AND a.source_message_ids_json @> jsonb_build_array(member.message_id))
                            OR (a.operation_kind='album' AND member.media_group_id IS NOT NULL
                                AND a.idempotency_key=('rule:'||a.rule_id::text||':target:'||a.target_id
                                    ||':media_group:'||member.media_group_id||':album'))
                          )
                          AND (a.operation_kind='album' OR NOT EXISTS (
                              SELECT 1 FROM deliveries old
                              WHERE old.id=a.delivery_id AND old.rule_id=a.rule_id
                                AND old.post_id=member.id
                          ))
                    )
                    OR EXISTS (
                        SELECT 1 FROM telegram_publication_receipts pr
                        WHERE pr.rule_id=d.rule_id AND pr.source_channel=p.source_channel
                          AND pr.source_thread_id IS NOT DISTINCT FROM p.source_thread_id
                          AND pr.idempotency_key NOT LIKE '%%:rescan:%%' AND pr.idempotency_key NOT LIKE 'archived:%%'
                          AND member.message_id=ANY(pr.source_message_ids)
                          AND NOT EXISTS (SELECT 1 FROM deliveries old WHERE old.id=ANY(pr.delivery_ids)
                              AND old.rule_id=d.rule_id AND old.post_id=member.id)
                    )
                  )
              )
            """, (int(rule_id),),
        )
        return int(cur.rowcount or 0)

    def rebuild_rule_keep_position(
        self, rule_id: int, *, owner_token: str, allowed_message_ids: set[int],
        include_newer_than_message_id: int, before_message_id: int | None,
        expected_scope: tuple | None = None,
    ) -> tuple[int, int, int, int]:
        with self.connect() as conn:
            with conn.cursor() as cur:
                rule = self._lock_rescan_commit(cur, rule_id, owner_token, expected_scope)
                self._assert_keep_rescan_safe(cur, rule_id)
                source, thread = str(rule["source_id"]), rule["source_thread_id"]
                rebuilt = self._backfill_deliveries_for_rule_conn(
                    conn, rule_id, source, thread, allowed_message_ids=allowed_message_ids,
                    include_newer_than_message_id=include_newer_than_message_id,
                )
                stale = self._prune_rule_pending_conn(
                    conn, rule_id, source, thread, seen_message_ids=allowed_message_ids,
                    scan_start_max_message_id=include_newer_than_message_id,
                )
                before = 0 if before_message_id is None else self._prune_rule_pending_conn(
                    conn, rule_id, source, thread, before_message_id=before_message_id,
                    before_snapshot_max_message_id=include_newer_than_message_id,
                )
                quarantined = self._quarantine_rescan_orphans(cur, rule_id)
                cur.execute("UPDATE routing SET rescan_queue_built=TRUE WHERE id=%s", (rule_id,))
            conn.commit()
        return rebuilt, stale, before, quarantined

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

    def update_rule_repost_caption(
        self,
        rule_id: int,
        caption: str | None,
        caption_entities_json: str | None = None,
    ) -> bool:
        # Do not implicitly switch the caption on for empty input. Clearing
        # always restores original text; a valid save enables the override.
        value = str(caption) if caption and str(caption).strip() else None
        if value is not None and len(value.encode("utf-16-le")) // 2 > 1024:
            return False
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE routing
                    SET repost_caption = %s,
                        repost_caption_entities_json = %s,
                        repost_caption_enabled = %s
                    WHERE id = %s AND mode = 'repost'
                    """,
                    (value, caption_entities_json if value is not None else None,
                     bool(value), int(rule_id)),
                )
                updated = cur.rowcount == 1
            conn.commit()
        return updated

    def set_rule_repost_caption_enabled(self, rule_id: int, enabled: bool) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE routing
                    SET repost_caption_enabled = %s
                    WHERE id = %s AND mode = 'repost'
                      AND (NOT %s OR NULLIF(BTRIM(repost_caption), '') IS NOT NULL)
                    """,
                    (bool(enabled), int(rule_id), bool(enabled)),
                )
                updated = cur.rowcount == 1
            conn.commit()
        return updated

    def update_rule_repost_caption_delivery_mode(self, rule_id: int, mode: str) -> bool:
        normalized = str(mode or "").strip().lower()
        if normalized not in ("copy_first", "builder_first"):
            return False
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE routing SET repost_caption_delivery_mode = %s
                    WHERE id = %s AND mode = 'repost'
                    """,
                    (normalized, int(rule_id)),
                )
                updated = cur.rowcount == 1
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
        allowed = {"copy_first", "builder_first"}
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

    def update_rule_video_clip_duration_seconds(self, rule_id: int, seconds: int) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE routing
                    SET video_clip_duration_seconds = %s
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
                    SELECT mode, schedule_mode, interval, target_id, target_thread_id
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

                destination_lock = f"delivery-target:{rule_row['target_id']}:{rule_row['target_thread_id']}"
                cur.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", (destination_lock,))
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
                              AND NOT EXISTS (
                                  SELECT 1 FROM telegram_publication_receipts pr
                                  JOIN delivery_attempts pa ON pa.idempotency_key=pr.idempotency_key
                                  WHERE pr.delivery_ids @> ARRAY[d_target.id]::bigint[]
                                    AND pr.published_ids IS NULL AND pr.expired_at IS NULL
                                    AND pa.status='accepted'
                              )
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

                # The source update can be queued before its publication receipt
                # is mapped. Recheck under the rule/delivery locks before enqueueing.
                if (str(due_row["source_channel"]) == str(due_row["target_id"])
                        and due_row["source_thread_id"] == due_row["target_thread_id"]):
                    cur.execute(
                        """SELECT d.id, p.message_id FROM deliveries d
                           JOIN posts p ON p.id=d.post_id WHERE d.id=ANY(%s)""",
                        (delivery_ids_to_take,),
                    )
                    echo_ids = []
                    for item in cur.fetchall():
                        if self._is_self_target_echo_post_for_rule_conn(
                            conn, rule_id=int(rule_id), source_channel=str(due_row["source_channel"]),
                            source_thread_id=due_row["source_thread_id"], message_id=int(item["message_id"]),
                        ):
                            echo_ids.append(int(item["id"]))
                            self._log_self_target_echo_delivery_skipped(
                                rule_id=int(rule_id), source_channel=str(due_row["source_channel"]),
                                source_thread_id=due_row["source_thread_id"], message_id=int(item["message_id"]),
                            )
                    if echo_ids:
                        cur.execute(
                            """UPDATE deliveries SET status='faulty', error_text='self_target_echo'
                               WHERE id=ANY(%s) AND status='pending'""", (echo_ids,),
                        )
                        conn.commit()
                        return None

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

    def get_transport_cooldown(self, scope_key: str) -> int:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT GREATEST(0, CEIL(EXTRACT(EPOCH FROM blocked_until-clock_timestamp())))::integer AS seconds
                       FROM telegram_transport_cooldowns WHERE scope_key=%s""",
                    (scope_key,),
                )
                row = cur.fetchone()
                return int(row["seconds"]) if row else 0

    def extend_transport_cooldown(self, scope_key: str, delay: int) -> None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO telegram_transport_cooldowns(scope_key,blocked_until)
                       VALUES (%s,clock_timestamp()+make_interval(secs=>%s))
                       ON CONFLICT (scope_key) DO UPDATE SET blocked_until =
                       GREATEST(telegram_transport_cooldowns.blocked_until,EXCLUDED.blocked_until)""",
                    (scope_key, max(1, int(delay))),
                )

    def get_or_create_mtproto_random_ids(self, key: str, ordinal: int,
                                         fingerprint: str, count: int) -> list[int]:
        """Persist a request identity under the currently claimed ledger row."""
        import secrets

        if not 1 <= count <= 100 or ordinal < 0:
            raise ValueError("Invalid MTProto send identity")
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id FROM delivery_attempts WHERE idempotency_key=%s AND status='sending' FOR UPDATE",
                    (key,),
                )
                row = cur.fetchone()
                if not row:
                    raise RuntimeError("No claimed MTProto delivery attempt")
                attempt_id = row["id"]
                ids = set()
                while len(ids) < count:
                    ids.add(secrets.randbits(63) or 1)
                cur.execute(
                    """
                    INSERT INTO mtproto_send_intents(attempt_id,ordinal,fingerprint,random_ids)
                    VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING
                    """,
                    (attempt_id, ordinal, fingerprint, sorted(ids)),
                )
                cur.execute(
                    "SELECT fingerprint,random_ids FROM mtproto_send_intents WHERE attempt_id=%s AND ordinal=%s",
                    (attempt_id, ordinal),
                )
                stored = cur.fetchone()
                if stored["fingerprint"] != fingerprint or len(stored["random_ids"]) != count:
                    raise ValueError("MTProto publication identity mismatch")
                return list(stored["random_ids"])

    def record_publication_inbox_overflow(self, account_id: int, peer_id: int, scheduled_ids: list[int]) -> None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO telegram_publication_inbox_incidents(sender_account_id,last_peer_id,last_scheduled_ids)
                    VALUES (%s,%s,%s)
                    ON CONFLICT(sender_account_id) DO UPDATE SET blocked_at=NOW(),
                        last_peer_id=EXCLUDED.last_peer_id,last_scheduled_ids=EXCLUDED.last_scheduled_ids,
                        acknowledged_at=NULL,review_note=NULL
                    """,(account_id,peer_id,scheduled_ids))

    def get_publication_inbox_incident(self, account_id: int) -> dict[str, Any] | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM telegram_publication_inbox_incidents WHERE sender_account_id=%s",(account_id,))
                row=cur.fetchone()
                return dict(row) if row else None

    def acknowledge_publication_inbox_incident(self, account_id: int, *, expected_blocked_at: datetime, note: str) -> bool:
        """Offline operator review, not a claim that missing events were recovered."""
        if not note.strip() or len(note)>1000:
            raise ValueError("Review note is required")
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE telegram_publication_inbox_incidents
                    SET acknowledged_at=NOW(),review_note=%s
                    WHERE sender_account_id=%s AND blocked_at=%s AND acknowledged_at IS NULL
                    """,(note,account_id,expected_blocked_at))
                return cur.rowcount==1

    def get_publication_recovery_cursor(self, account_id: int) -> dict[str, Any] | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT pts,qts,state_date,version FROM telegram_publication_recovery_cursor WHERE sender_account_id=%s",
                    (account_id,),
                )
                row = cur.fetchone()
                return dict(row) if row is not None else None

    def initialize_publication_recovery_cursor(
        self, account_id: int, pts: int, qts: int, state_date: datetime,
    ) -> None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO telegram_publication_recovery_cursor(sender_account_id,pts,qts,state_date)
                    VALUES (%s,%s,%s,%s) ON CONFLICT(sender_account_id) DO NOTHING
                    """,
                    (account_id, pts, qts, state_date),
                )
            conn.commit()

    def advance_publication_recovery_cursor(
        self, account_id: int, version: int, pts: int, qts: int, state_date: datetime,
    ) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE telegram_publication_recovery_cursor
                    SET pts=%s,qts=%s,state_date=%s,version=version+1
                    WHERE sender_account_id=%s AND version=%s
                      AND pts<=%s AND qts<=%s AND state_date<=%s
                    """,
                    (pts,qts,state_date,account_id,version,pts,qts,state_date),
                )
                changed = cur.rowcount == 1
            conn.commit()
        return changed

    def record_telegram_publication_mappings(
        self, sender_account_id: int, target_peer_id: int,
        pairs: list[tuple[int, int]],
    ) -> None:
        """Persist exact Telegram mappings, including updates before receipts."""
        from app.telegram_publication import publication_pairs

        validated = publication_pairs([p[0] for p in pairs], [p[1] for p in pairs])
        if not validated or validated != pairs:
            raise ValueError("Invalid scheduled-publication mapping")
        with self.connect() as conn:
            with conn.cursor() as cur:
                # Serialize exact evidence against the same peer's finalizer.
                cur.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                            (f"publication:{sender_account_id}:{target_peer_id}",))
                # All items in an update are committed together.
                cur.executemany(
                    """
                    INSERT INTO telegram_publication_mappings
                        (sender_account_id, target_peer_id, scheduled_id, published_id)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (sender_account_id, target_peer_id, scheduled_id, published_id)
                    DO NOTHING
                    """,
                    [(sender_account_id, target_peer_id, old, new) for old, new in sorted(validated)],
                )
            conn.commit()

    def get_telegram_publication_mapping(
        self, sender_account_id: int, target_peer_id: int, scheduled_id: int,
    ) -> int | None:
        """Fail closed on contradictory evidence in either ID namespace."""
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT m.published_id
                    FROM telegram_publication_mappings m
                    WHERE m.sender_account_id = %s AND m.target_peer_id = %s
                      AND m.scheduled_id = %s
                      AND NOT EXISTS (
                        SELECT 1 FROM telegram_publication_mappings other
                        WHERE other.sender_account_id = m.sender_account_id
                          AND other.target_peer_id = m.target_peer_id
                          AND (
                            (other.scheduled_id = m.scheduled_id AND other.published_id <> m.published_id)
                            OR (other.published_id = m.published_id AND other.scheduled_id <> m.scheduled_id)
                          )
                      )
                    """,
                    (sender_account_id, target_peer_id, scheduled_id),
                )
                row = cur.fetchone()
                return int(row["published_id"]) if row else None

    def get_telegram_publication_mappings(
        self, sender_account_id: int, target_peer_id: int, scheduled_ids: list[int],
    ) -> list[int]:
        """Read all conflict-free mappings from one database snapshot."""
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT m.scheduled_id,m.published_id
                    FROM telegram_publication_mappings m
                    WHERE m.sender_account_id=%s AND m.target_peer_id=%s
                      AND m.scheduled_id=ANY(%s)
                      AND NOT EXISTS (
                          SELECT 1 FROM telegram_publication_mappings x
                          WHERE x.sender_account_id=m.sender_account_id AND x.target_peer_id=m.target_peer_id
                            AND ((x.scheduled_id=m.scheduled_id AND x.published_id<>m.published_id)
                              OR (x.published_id=m.published_id AND x.scheduled_id<>m.scheduled_id))
                      )
                    """,(sender_account_id,target_peer_id,scheduled_ids),
                )
                rows = {r["scheduled_id"]:r["published_id"] for r in cur.fetchall()}
                return [rows[mid] for mid in scheduled_ids] if all(mid in rows for mid in scheduled_ids) else []

    def register_publication_receipt(
        self, *, key: str, delivery_ids: list[int], rule_id: int,
        account_id: int, peer_id: int, thread_id: int | None,
        source_channel: str, source_message_ids: list[int], scheduled_ids: list[int],
        pending_mask: list[bool] | None = None,
    ) -> bool:
        """Register an ordered MTProto receipt and reserve each scheduled ID once."""
        from app.telegram_publication import publication_candidate_mask, publication_pairs

        mask = publication_candidate_mask(scheduled_ids,pending_mask)

        if (account_id <= 0 or peer_id == 0 or not 1 <= len(delivery_ids) <= 10
                or not publication_pairs(delivery_ids, source_message_ids)
                or len(delivery_ids) != len(scheduled_ids) or not mask):
            return False
        pending_ids = [mid for mid,flag in zip(scheduled_ids,mask) if flag]
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT target_id,target_thread_id,source_id,source_thread_id,tenant_id FROM routing WHERE id=%s FOR UPDATE",
                    (rule_id,),
                )
                rule = cur.fetchone()
                if (not rule or str(rule["target_id"]) != str(peer_id)
                        or rule["target_thread_id"] != thread_id
                        or str(rule["source_id"]) != source_channel):
                    return False
                cur.execute(
                    """
                    SELECT delivery_id,operation_kind,source_message_ids_json,created_at
                    FROM delivery_attempts
                    WHERE idempotency_key=%s AND rule_id=%s AND target_id=%s
                      AND status IN ('sending','accepted') FOR UPDATE
                    """,
                    (key, rule_id, str(peer_id)),
                )
                attempt = cur.fetchone()
                if not attempt:
                    return False
                kind = attempt["operation_kind"]
                recorded = _safe_json_loads(attempt["source_message_ids_json"], [])
                if kind == "album":
                    # Historical album ledger uses delivery IDs rather than source IDs.
                    # Verify both cardinality and actual source membership below.
                    if (int(attempt["delivery_id"]) not in [0, *delivery_ids]
                            or sorted(recorded) not in [sorted(delivery_ids), sorted(source_message_ids)]):
                        return False
                elif (kind not in {"single", "video_send"} or len(delivery_ids) != 1
                      or attempt["delivery_id"] != delivery_ids[0] or recorded != source_message_ids):
                    return False
                cur.execute(
                    """
                    SELECT d.id,p.message_id,p.media_group_id
                    FROM deliveries d JOIN posts p ON p.id=d.post_id
                    WHERE d.id=ANY(%s) AND d.rule_id=%s AND d.status IN ('pending','processing')
                      AND p.source_channel=%s AND p.source_thread_id IS NOT DISTINCT FROM %s
                    ORDER BY d.id FOR UPDATE OF d
                    """,
                    (delivery_ids, rule_id, source_channel, rule["source_thread_id"]),
                )
                rows = cur.fetchall()
                if {r["id"]: r["message_id"] for r in rows} != dict(zip(delivery_ids, source_message_ids)):
                    return False
                if kind == "album":
                    groups = {r["media_group_id"] for r in rows}
                    if len(groups) != 1 or not next(iter(groups)):
                        return False
                # Serialize claims across ordinary deliveries and campaign placements.
                cur.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                            (f"publication:{account_id}:{peer_id}",))
                cur.execute("""
                    SELECT 1 FROM campaign_publication_claims
                    WHERE sender_account_id=%s AND target_peer_id=%s
                      AND scheduled_id=ANY(%s::bigint[]) LIMIT 1
                    """, (account_id,peer_id,pending_ids))
                if cur.fetchone():
                    return False
                cur.execute(
                    """
                    INSERT INTO telegram_publication_receipts
                    (idempotency_key,rule_id,tenant_id,sender_account_id,target_peer_id,
                     target_thread_id,source_channel,source_thread_id,delivery_ids,source_message_ids,
                     scheduled_ids,pending_mask,send_started_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT DO NOTHING RETURNING idempotency_key
                    """,
                    (key, rule_id, rule["tenant_id"] or 1, account_id, peer_id, thread_id,
                     source_channel, rule["source_thread_id"], delivery_ids, source_message_ids,
                     scheduled_ids, mask, attempt["created_at"]),
                )
                if not cur.fetchone():
                    # A repeated identical registration may follow a lost commit response.
                    cur.execute(
                        """SELECT 1 FROM telegram_publication_receipts
                           WHERE idempotency_key=%s AND rule_id=%s AND tenant_id=%s
                             AND sender_account_id=%s AND target_peer_id=%s
                             AND target_thread_id IS NOT DISTINCT FROM %s
                             AND source_channel=%s AND source_thread_id IS NOT DISTINCT FROM %s
                             AND delivery_ids=%s::bigint[] AND source_message_ids=%s::bigint[]
                             AND scheduled_ids=%s::bigint[] AND pending_mask=%s::boolean[] AND expired_at IS NULL""",
                        (key,rule_id,rule['tenant_id'] or 1,account_id,peer_id,thread_id,
                         source_channel,rule['source_thread_id'],delivery_ids,source_message_ids,scheduled_ids,mask),
                    )
                    return cur.fetchone() is not None
                cur.execute(
                    """
                    INSERT INTO telegram_publication_claims
                    (sender_account_id,target_peer_id,scheduled_id,idempotency_key)
                    SELECT %s,%s,member.scheduled_id,%s
                    FROM unnest(%s::bigint[]) AS member(scheduled_id)
                    ORDER BY member.scheduled_id
                    ON CONFLICT DO NOTHING
                    """,
                    (account_id,peer_id,key,pending_ids),
                )
                if cur.rowcount != len(pending_ids):
                    conn.rollback()
                    return False
                cur.execute(
                    """
                    UPDATE delivery_attempts SET status='accepted', telegram_method='mtproto_pending_publication',
                        sent_message_ids_json='[]'::jsonb,error_text=NULL,updated_at=NOW()
                    WHERE idempotency_key=%s
                    """,
                    (key,),
                )
            conn.commit()
        return True

    def get_publication_receipt(self, key: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT *, deadline_at<=NOW() AS expired,
                        reaction_updated_at<NOW()-INTERVAL '15 minutes' AS dispatch_stale
                    FROM telegram_publication_receipts WHERE idempotency_key=%s
                    """,
                    (key,),
                )
                row = cur.fetchone()
                return dict(row) if row is not None else None

    def has_unfinished_publication_reaction(self, delivery_id: int) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT 1 FROM telegram_publication_receipts r
                    JOIN delivery_attempts a ON a.idempotency_key=r.idempotency_key
                    WHERE r.delivery_ids @> ARRAY[%s]::bigint[] AND r.published_ids IS NOT NULL
                      AND r.reaction_state IN ('pending','dispatching') AND a.status='verified'
                    LIMIT 1
                    """,
                    (delivery_id,),
                )
                return cur.fetchone() is not None

    def finalize_publication_receipt(self, key: str, published_ids: list[int], *, expected_started_at: datetime | None = None) -> bool:
        """Commit all mapped IDs, the attempt, and post-send intent atomically."""
        from app.telegram_publication import publication_pairs

        receipt = self.get_publication_receipt(key)
        if not receipt or not publication_pairs(published_ids,published_ids) or (expected_started_at is not None and receipt["send_started_at"] != expected_started_at):
            return False
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT target_id,target_thread_id,source_id,source_thread_id,tenant_id FROM routing WHERE id=%s FOR UPDATE",
                    (receipt["rule_id"],),
                )
                rule = cur.fetchone()
                if not rule:
                    return False
                cur.execute(
                    """
                    SELECT id FROM delivery_attempts WHERE idempotency_key=%s AND rule_id=%s
                      AND status='accepted' AND telegram_method='mtproto_pending_publication'
                    FOR UPDATE
                    """,
                    (key,receipt["rule_id"]),
                )
                if not cur.fetchone():
                    return False
                cur.execute("SELECT *,deadline_at<=clock_timestamp() AS expired FROM telegram_publication_receipts WHERE idempotency_key=%s FOR UPDATE",(key,))
                pending = cur.fetchone()
                if not pending or pending["published_ids"] is not None or pending["expired_at"] is not None or pending["expired"]:
                    return False
                if pending["send_started_at"] != receipt["send_started_at"] or pending["rule_id"] != receipt["rule_id"]:
                    return False
                receipt = pending
                if len(receipt["scheduled_ids"]) != len(published_ids):
                    return False
                if (not rule or str(rule["target_id"]) != str(receipt["target_peer_id"])
                        or rule["target_thread_id"] != receipt["target_thread_id"]
                        or str(rule["source_id"]) != receipt["source_channel"]
                        or rule["source_thread_id"] != receipt["source_thread_id"]
                        or (rule["tenant_id"] or 1) != receipt["tenant_id"]):
                    return False
                if str(receipt["target_peer_id"]) == receipt["source_channel"] and set(published_ids) & set(receipt["source_message_ids"]):
                    return False
                # Serialize with inbox writes for the same peer before checking
                # every mapping. A conflicting event already recorded blocks promotion.
                cur.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                            (f'publication:{receipt["sender_account_id"]}:{receipt["target_peer_id"]}',))
                for scheduled_id,published_id,flag in zip(receipt["scheduled_ids"],published_ids,receipt["pending_mask"]):
                    if not flag:
                        if scheduled_id != published_id:
                            return False
                        continue
                    cur.execute(
                        """
                        SELECT m.published_id FROM telegram_publication_mappings m
                        WHERE m.sender_account_id=%s AND m.target_peer_id=%s
                          AND m.scheduled_id=%s AND m.published_id=%s
                          AND NOT EXISTS (
                              SELECT 1 FROM telegram_publication_mappings x
                              WHERE x.sender_account_id=m.sender_account_id AND x.target_peer_id=m.target_peer_id
                                AND ((x.scheduled_id=m.scheduled_id AND x.published_id<>m.published_id)
                                  OR (x.published_id=m.published_id AND x.scheduled_id<>m.scheduled_id))
                          )
                        """,
                        (receipt["sender_account_id"],receipt["target_peer_id"],scheduled_id,published_id),
                    )
                    if not cur.fetchone():
                        return False
                # Lock the complete delivery vector before the final deadline check.
                cur.execute(
                    "SELECT id FROM deliveries WHERE rule_id=%s AND id=ANY(%s) ORDER BY id FOR UPDATE",
                    (receipt["rule_id"], receipt["delivery_ids"]),
                )
                if [row["id"] for row in cur.fetchall()] != sorted(receipt["delivery_ids"]):
                    return False
                # NOW() is fixed at transaction start; locks above may have
                # delayed this transaction beyond the publication deadline.
                cur.execute("SELECT clock_timestamp() >= %s AS expired",(receipt["deadline_at"],))
                if cur.fetchone()["expired"]:
                    return False
                ids = _json_dumps(published_ids)
                now = utc_now_iso()
                for delivery_id,source_id,published_id in sorted(zip(receipt["delivery_ids"],receipt["source_message_ids"],published_ids)):
                    cur.execute(
                        """
                        UPDATE deliveries d SET status='sent',sent_at=%s,error_text=NULL,
                            sent_message_id=%s,sent_message_ids_json=%s::jsonb,
                            target_id_snapshot=%s,delivery_method='mtproto_scheduled_mapping'
                        FROM posts p
                        WHERE d.id=%s AND d.rule_id=%s AND d.status IN ('pending','processing')
                          AND p.id=d.post_id AND p.message_id=%s AND p.source_channel=%s
                          AND p.source_thread_id IS NOT DISTINCT FROM %s
                        """,
                        (now,published_id,ids,str(receipt["target_peer_id"]),delivery_id,
                         receipt["rule_id"],source_id,receipt["source_channel"],receipt["source_thread_id"]),
                    )
                    if cur.rowcount != 1:
                        conn.rollback()
                        return False
                cur.execute(
                    """
                    UPDATE delivery_attempts SET status='verified',sent_message_ids_json=%s::jsonb,
                        error_text=NULL,updated_at=NOW() WHERE idempotency_key=%s
                    """,
                    (ids,key),
                )
                cur.execute("UPDATE telegram_publication_receipts SET published_ids=%s WHERE idempotency_key=%s",(published_ids,key))
                cur.execute("UPDATE routing SET last_sent_at=%s WHERE id=%s",(now,receipt["rule_id"]))
            conn.commit()
        return True

    def expire_publication_receipt(self, key: str, reason: str, *, expected_started_at: datetime | None = None) -> bool:
        """Retain the accepted attempt and atomically end an unresolved receipt."""
        receipt = self.get_publication_receipt(key)
        if not receipt or (expected_started_at is not None and receipt['send_started_at'] != expected_started_at):
            return False
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM delivery_attempts WHERE idempotency_key=%s AND status='accepted' FOR UPDATE",(key,))
                if not cur.fetchone():
                    return False
                cur.execute(
                    """
                    UPDATE telegram_publication_receipts SET expired_at=NOW()
                    WHERE idempotency_key=%s AND published_ids IS NULL AND expired_at IS NULL
                      AND send_started_at=%s AND deadline_at<=NOW() RETURNING delivery_ids
                    """,
                    (key,receipt['send_started_at']),
                )
                row = cur.fetchone()
                if not row:
                    return False
                cur.execute(
                    """
                    UPDATE deliveries SET status='faulty',error_text=%s,attempt_count=attempt_count+1
                    WHERE id=ANY(%s) AND rule_id=%s AND status IN ('pending','processing')
                    """,
                    (reason[:1000],row["delivery_ids"],receipt["rule_id"]),
                )
            conn.commit()
        return True

    def acknowledge_expired_publication_receipt(self, key: str, *, operator: str, note: str) -> bool:
        """Operator quarantine: retain accepted evidence, never invent published IDs."""
        operator, note = operator.strip(), note.strip()
        if not operator or len(operator)>128 or not note or len(note)>1000:
            raise ValueError("Operator and manual review note are required")
        receipt = self.get_publication_receipt(key)
        if not receipt:
            return False
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT is_active,rescan_owner_token FROM routing WHERE id=%s FOR UPDATE",
                    (receipt["rule_id"],),
                )
                rule = cur.fetchone()
                if not rule or rule["is_active"] or rule["rescan_owner_token"] is not None:
                    return False
                cur.execute(
                    """
                    SELECT 1 FROM delivery_attempts
                    WHERE idempotency_key=%s AND rule_id=%s AND status='accepted'
                      AND telegram_method='mtproto_pending_publication' FOR UPDATE
                    """,
                    (key,receipt["rule_id"]),
                )
                if not cur.fetchone():
                    return False
                cur.execute(
                    """
                    UPDATE telegram_publication_receipts r
                    SET manual_review=jsonb_build_object('operator',%s::text,'note',%s::text,'reviewed_at',NOW())
                    WHERE r.idempotency_key=%s AND r.rule_id=%s AND r.send_started_at=%s
                      AND r.expired_at IS NOT NULL AND r.published_ids IS NULL AND r.manual_review IS NULL
                      AND EXISTS (
                          SELECT 1 FROM routing rr WHERE rr.id=r.rule_id
                            AND COALESCE(rr.tenant_id,1)=r.tenant_id
                            AND rr.source_id::text=r.source_channel
                            AND rr.source_thread_id IS NOT DISTINCT FROM r.source_thread_id
                            AND rr.target_id::text=r.target_peer_id::text
                            AND rr.target_thread_id IS NOT DISTINCT FROM r.target_thread_id
                      )
                      AND NOT EXISTS (
                          SELECT 1 FROM jobs j WHERE j.payload_json->>'rule_id'=r.rule_id::text
                            AND j.status IN ('pending','leased','processing','retry')
                      )
                      AND NOT EXISTS (
                          SELECT 1 FROM deliveries d WHERE d.rule_id=r.rule_id AND d.status='processing'
                      )
                      AND NOT EXISTS (
                          SELECT 1 FROM unnest(r.delivery_ids,r.source_message_ids) AS member(delivery_id,message_id)
                          WHERE NOT EXISTS (
                              SELECT 1 FROM deliveries d JOIN posts p ON p.id=d.post_id
                              WHERE d.rule_id=r.rule_id AND d.id=member.delivery_id AND d.status='faulty'
                                AND p.message_id=member.message_id AND p.source_channel=r.source_channel
                                AND p.source_thread_id IS NOT DISTINCT FROM r.source_thread_id
                          )
                      )
                    """,
                    (operator,note,key,receipt["rule_id"],receipt["send_started_at"]),
                )
                updated = cur.rowcount == 1
            conn.commit()
        return updated


    def claim_publication_reaction(self, key: str, published_ids: list[int], *, expected_started_at: datetime) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE telegram_publication_receipts r
                    SET reaction_state='dispatching',reaction_updated_at=NOW()
                    WHERE r.idempotency_key=%s AND r.published_ids=%s::bigint[]
                      AND r.send_started_at=%s AND r.reaction_state='pending'
                      AND EXISTS (SELECT 1 FROM delivery_attempts a WHERE a.idempotency_key=r.idempotency_key
                                  AND a.status='verified' AND a.sent_message_ids_json=to_jsonb(r.published_ids))
                      AND NOT EXISTS (
                          SELECT 1 FROM unnest(r.delivery_ids,r.published_ids) AS pair(delivery_id,published_id)
                          WHERE NOT EXISTS (SELECT 1 FROM deliveries d WHERE d.id=pair.delivery_id
                            AND d.status='sent' AND d.sent_message_id=pair.published_id
                            AND d.target_id_snapshot=r.target_peer_id::text)
                      )
                    """,
                    (key,published_ids,expected_started_at),
                )
                claimed = cur.rowcount == 1
            conn.commit()
        return claimed

    def finish_publication_reaction(self, key: str, state: str, result: dict[str, Any], *, expected_started_at: datetime, expected_reaction_updated_at: datetime | None = None) -> None:
        if state not in {"done","uncertain"} and not (state == "pending" and result.get("reason") == "enqueue_failed"):
            raise ValueError("Invalid reaction outcome")
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE telegram_publication_receipts
                    SET reaction_state=%s,reaction_result=%s::jsonb,reaction_updated_at=NOW()
                    WHERE idempotency_key=%s AND send_started_at=%s AND reaction_state='dispatching'
                      AND (%s::timestamptz IS NULL OR reaction_updated_at=%s::timestamptz)
                    """,
                    (state,_json_dumps(result),key,expected_started_at,expected_reaction_updated_at,expected_reaction_updated_at),
                )
            conn.commit()

    def get_delivery_attempt_by_idempotency_key(self, idempotency_key: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM delivery_attempts WHERE idempotency_key = %s", (str(idempotency_key or ""),))
                return cur.fetchone()

    def create_delivery_attempt(self, *, delivery_id: int, rule_id: int, tenant_id: int, job_id: int | None, idempotency_key: str, operation_kind: str, status: str, telegram_method: str | None = None, target_id: str | None = None, source_message_ids: list[int] | None = None, sent_message_ids: list[int] | None = None, error_text: str | None = None) -> int | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO delivery_attempts(
                        delivery_id, rule_id, tenant_id, job_id, idempotency_key, operation_kind, status, telegram_method, target_id,
                        source_message_ids_json, sent_message_ids_json, error_text, created_at, updated_at
                    )
                    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s, NOW(), NOW())
                    ON CONFLICT(idempotency_key) DO NOTHING
                    RETURNING id
                    """,
                    (delivery_id, rule_id, tenant_id, job_id, str(idempotency_key), str(operation_kind), str(status), telegram_method, target_id, _json_dumps(source_message_ids), _json_dumps(sent_message_ids), (error_text or None)),
                )
                row = cur.fetchone()
                if row:
                    conn.commit()
                    return int(row["id"])
                cur.execute("SELECT id FROM delivery_attempts WHERE idempotency_key = %s", (str(idempotency_key),))
                existing = cur.fetchone()
            conn.commit()
            return int(existing["id"]) if existing else None

    def mark_delivery_attempt_sending(self, idempotency_key: str, *, job_id: int | None = None, telegram_method: str | None = None) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE delivery_attempts
                    SET status = 'sending', job_id = COALESCE(%s, job_id), telegram_method = COALESCE(%s, telegram_method), updated_at = NOW()
                    WHERE idempotency_key = %s
                      AND status IN ('created', 'failed_before_send')
                    """,
                    (job_id, telegram_method, str(idempotency_key)),
                )
                updated = int(cur.rowcount or 0) > 0
            conn.commit()
            return updated

    def mark_delivery_attempt_accepted(self, idempotency_key: str, *, sent_message_ids: list[int], telegram_method: str | None = None) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE delivery_attempts
                    SET status = 'accepted', sent_message_ids_json = %s::jsonb, telegram_method = COALESCE(%s, telegram_method), updated_at = NOW(), error_text = NULL
                    WHERE idempotency_key = %s
                      AND status IN ('sending', 'accepted')
                      AND telegram_method IS DISTINCT FROM 'mtproto_pending_publication'
                    """,
                    (_json_dumps([int(x) for x in (sent_message_ids or [])]), telegram_method, str(idempotency_key)),
                )
                updated = int(cur.rowcount or 0) > 0
            conn.commit()
            return updated

    def mark_delivery_attempt_verified(self, idempotency_key: str, *, sent_message_ids: list[int]) -> bool:
        """Promote a target-confirmed delivery; never overwrite a verified id with a raw candidate."""
        ids = [int(x) for x in sent_message_ids if int(x) > 0]
        if not ids:
            return False
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE delivery_attempts
                    SET status = 'verified', sent_message_ids_json = %s::jsonb, error_text = NULL, updated_at = NOW()
                    WHERE idempotency_key = %s
                      AND telegram_method IS DISTINCT FROM 'mtproto_pending_publication'
                      AND (
                          status = 'accepted'
                          OR (status = 'verified' AND sent_message_ids_json = %s::jsonb)
                      )
                    """,
                    (_json_dumps(ids), str(idempotency_key), _json_dumps(ids)),
                )
                updated = int(cur.rowcount or 0) > 0
            conn.commit()
            return updated

    def get_pending_album_ledger_candidates(self, rule_id: int) -> list[dict[str, Any]]:
        """Read-only candidates; never infer success from accepted ledger alone."""
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT d.id AS delivery_id, d.status AS delivery_status,
                           p.message_id, p.source_channel, p.source_thread_id,
                           p.media_group_id, r.target_id, r.target_thread_id,
                           a.id AS attempt_id, a.idempotency_key, a.status AS attempt_status,
                           a.sent_message_ids_json
                    FROM deliveries d
                    JOIN posts p ON p.id = d.post_id
                    JOIN routing r ON r.id = d.rule_id
                    JOIN delivery_attempts a ON a.idempotency_key =
                        ('rule:' || r.id::text || ':target:' || r.target_id
                         || ':media_group:' || p.media_group_id || ':album')
                    WHERE d.rule_id = %s AND d.status = 'pending'
                      AND r.mode = 'repost'
                      AND p.media_group_id IS NOT NULL
                      AND a.operation_kind = 'album'
                      AND a.status IN ('accepted', 'verified')
                    ORDER BY p.id, p.message_id
                    """,
                    (int(rule_id),),
                )
                return list(cur.fetchall())

    def reconcile_confirmed_album_ledger(
        self, *, rule_id: int, attempt_id: int, source_channel: str,
        source_thread_id: int | None, media_group_id: str,
        source_message_ids: list[int], target_message_ids: list[int],
    ) -> int:
        """Atomically promote one *externally verified* complete album.

        Telegram reading and media/caption checks happen before this call.
        All DB state and ownership are rechecked under row locks. Never
        modifies processing/leased jobs, and never resends a Telegram post.
        """
        from app.delivery_idempotency import build_delivery_idempotency_key

        source_ids = [int(x) for x in source_message_ids]
        target_ids = [int(x) for x in target_message_ids]
        if (not source_ids or len(source_ids) != len(target_ids)
                or len(set(source_ids)) != len(source_ids)
                or len(set(target_ids)) != len(target_ids)
                or any(x <= 0 for x in source_ids + target_ids)):
            raise ValueError("Invalid/partial album IDs")
        if source_ids != sorted(source_ids) or target_ids != sorted(target_ids):
            raise ValueError("Album IDs must be in source/target order")

        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT target_id, mode, tenant_id FROM routing WHERE id = %s FOR UPDATE",
                    (int(rule_id),),
                )
                rule = cur.fetchone()
                if not rule or str(rule["mode"]) != "repost":
                    raise RuntimeError("Rule missing or not in repost mode")
                target_id = str(rule["target_id"])
                key = build_delivery_idempotency_key(
                    operation_kind="album", rule_id=rule_id,
                    target_id=target_id, media_group_id=media_group_id,
                )
                cur.execute(
                    """
                    SELECT id, status, operation_kind, target_id, idempotency_key,
                           delivery_id, source_message_ids_json, sent_message_ids_json
                    FROM delivery_attempts WHERE id = %s FOR UPDATE
                    """,
                    (int(attempt_id),),
                )
                attempt = cur.fetchone()
                if not attempt or attempt["idempotency_key"] != key:
                    raise RuntimeError("Attempt key changed; reconciliation aborted")
                if (attempt["status"] not in ("accepted", "verified")
                        or attempt["operation_kind"] != "album"
                        or str(attempt["target_id"]) != target_id):
                    raise RuntimeError("Attempt is not a recoverable album")
                expected = _safe_json_loads(attempt["sent_message_ids_json"], [])
                if expected != target_ids:
                    raise RuntimeError("Accepted Telegram IDs changed; reconciliation aborted")

                cur.execute(
                    """
                    SELECT EXISTS(
                        SELECT 1 FROM jobs
                        WHERE payload_json->>'rule_id' = %s
                          AND status IN ('pending', 'leased', 'processing', 'retry')
                    ) AS busy
                    """,
                    (str(rule_id),),
                )
                if cur.fetchone()["busy"]:
                    raise RuntimeError("Rule has a live job; no ledger recovery while sending")
                cur.execute(
                    """
                    SELECT d.id, d.status, p.message_id
                    FROM deliveries d JOIN posts p ON p.id = d.post_id
                    WHERE d.rule_id = %s
                      AND p.source_channel = %s
                      AND p.source_thread_id IS NOT DISTINCT FROM %s
                      AND p.media_group_id = %s
                    ORDER BY p.message_id
                    FOR UPDATE OF d
                    """,
                    (int(rule_id), str(source_channel), source_thread_id, str(media_group_id)),
                )
                rows = cur.fetchall()
                if ([int(x["message_id"]) for x in rows] != source_ids
                        or any(x["status"] != "pending" for x in rows)):
                    raise RuntimeError("Album changed or partially processed; no mutation")

                delivery_ids = [int(x["id"]) for x in rows]
                original_delivery_id = int(attempt.get("delivery_id") or 0)
                original_ids = _safe_json_loads(attempt.get("source_message_ids_json"), [])
                cur.execute(
                    """
                    UPDATE delivery_attempts
                    SET status = 'verified', delivery_id = %s,
                        source_message_ids_json = %s::jsonb,
                        updated_at = NOW(), error_text = NULL
                    WHERE id = %s
                      AND status IN ('accepted', 'verified')
                    """,
                    (delivery_ids[0], _json_dumps(delivery_ids), int(attempt_id)),
                )
                if cur.rowcount != 1:
                    raise RuntimeError("Attempt promotion lost a race")
                for did, tid in zip(delivery_ids, target_ids):
                    cur.execute(
                        """
                        UPDATE deliveries
                        SET status = 'sent', sent_at = NOW(), error_text = NULL,
                            sent_message_id = %s, sent_message_ids_json = %s::jsonb,
                            target_id_snapshot = %s,
                            delivery_method = 'reconciled_verified_album_ledger'
                        WHERE id = %s AND status = 'pending'
                        """,
                        (int(tid), _json_dumps(target_ids), target_id, int(did)),
                    )
                    if cur.rowcount != 1:
                        raise RuntimeError("Album row changed concurrently; rolled back")
                # Retain old->new delivery-ID provenance in the *same* commit.
                cur.execute(
                    """
                    INSERT INTO audit_log (
                        created_at, event_type, rule_id, delivery_id,
                        source_id, source_thread_id, target_id, status,
                        old_value_json, new_value_json, extra_json, tenant_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        utc_now_iso(), "album_ledger_reconciled", int(rule_id),
                        delivery_ids[0], str(source_channel), source_thread_id,
                        target_id, "verified",
                        _json_dumps({
                            "attempt_id": int(attempt_id),
                            "old_delivery_id": original_delivery_id,
                            "old_delivery_ids": original_ids,
                        }),
                        _json_dumps({
                            "new_delivery_ids": delivery_ids,
                            "target_message_ids": target_ids,
                        }),
                        _json_dumps({"media_group_id": str(media_group_id),
                                     "verification": "telegram_group_media_caption"}),
                        int(rule.get("tenant_id") or 1),
                    ),
                )
            conn.commit()
        return len(delivery_ids)

    def mark_delivery_attempt_failed(self, idempotency_key: str, *, status: str, error_text: str) -> bool:
        normalized = str(status or "")
        if normalized not in {"failed_before_send", "failed_after_send"}:
            return False
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE delivery_attempts
                    SET status = %s, error_text = %s, updated_at = NOW()
                    WHERE idempotency_key = %s
                      AND status NOT IN ('accepted', 'verified')
                      AND (status <> 'failed_after_send' OR %s = 'failed_after_send')
                    """,
                    (normalized, (error_text or "")[:1000], str(idempotency_key), normalized),
                )
                updated = int(cur.rowcount or 0) > 0
            conn.commit()
            return updated

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

    def mark_delivery_faulty_if_attempt_unverified(
        self, delivery_id: int, idempotency_key: str, error_text: str
    ) -> bool:
        """Fenced failure: a second worker cannot downgrade a verified/sent delivery.

        Serialize on the attempt row before checking its state, so a concurrent
        verified transition cannot race a stale confirm-failure write.
        """
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT status FROM delivery_attempts WHERE idempotency_key = %s AND telegram_method IS DISTINCT FROM 'mtproto_pending_publication' FOR UPDATE",
                    (str(idempotency_key),),
                )
                attempt = cur.fetchone()
                if not attempt or str(attempt.get("status") or "") not in {"sending", "accepted", "failed_after_send"}:
                    return False
                cur.execute(
                    """
                    UPDATE deliveries
                    SET status = 'faulty', error_text = %s, attempt_count = attempt_count + 1
                    WHERE id = %s AND status NOT IN ('sent', 'faulty')
                    """,
                    ((error_text or "")[:1000], int(delivery_id)),
                )
                updated = cur.rowcount > 0
            conn.commit()
            return updated

    def finalize_orphaned_repost_attempt(
        self, *, idempotency_key: str, job_id: int, worker_id: str, rule_id: int,
        delivery_ids: list[int], error_text: str,
    ) -> int:
        """Terminal, fenced manual review when no other live repost job owns the write.

        Never re-send on an unknown response. The row lock orders this
        transition against mark_delivery_attempt_verified.
        """
        ids = [int(x) for x in delivery_ids if int(x) > 0]
        if not ids:
            return 0
        with self.connect() as conn:
            with conn.cursor() as cur:
                # Serialize with job lease recovery/finalization. The caller
                # must still own this exact processing job, not merely its ID.
                cur.execute(
                    "SELECT status, locked_by FROM jobs WHERE id = %s FOR UPDATE",
                    (int(job_id),),
                )
                current_job = cur.fetchone()
                if not current_job or current_job.get("status") != "processing" or current_job.get("locked_by") != str(worker_id):
                    return 0
                cur.execute(
                    "SELECT status FROM delivery_attempts WHERE idempotency_key = %s AND telegram_method IS DISTINCT FROM 'mtproto_pending_publication' FOR UPDATE",
                    (str(idempotency_key),),
                )
                attempt = cur.fetchone()
                if not attempt or str(attempt.get("status") or "") not in {"sending", "accepted", "failed_after_send"}:
                    return 0

                # Another worker may still be sending or verifying this
                # same rule/delivery. Never mark its live work as faulty.
                cur.execute(
                    """
                    SELECT EXISTS (
                        SELECT 1 FROM jobs j
                        WHERE j.id <> %s
                          AND (
                              (j.status = 'leased' AND j.lease_until > NOW())
                              OR (j.status = 'processing' AND j.updated_at > NOW() - INTERVAL '60 seconds')
                          )
                          AND j.job_type IN ('repost_single', 'repost_album')
                          AND j.payload_json->>'rule_id' = %s
                          AND (
                              j.payload_json->>'delivery_id' = ANY(%s)
                              OR EXISTS (
                                  SELECT 1
                                  FROM jsonb_array_elements_text(
                                      CASE WHEN jsonb_typeof(j.payload_json->'delivery_ids') = 'array'
                                           THEN j.payload_json->'delivery_ids'
                                           ELSE '[]'::jsonb END
                                  ) AS member(id)
                                  WHERE member.id = ANY(%s)
                              )
                          )
                    ) AS has_live_owner
                    """,
                    (int(job_id), str(rule_id), [str(x) for x in ids], [str(x) for x in ids]),
                )
                owner = cur.fetchone()
                if owner and bool(owner.get("has_live_owner")):
                    return 0

                cur.execute(
                    """
                    UPDATE deliveries
                    SET status='faulty',
                        error_text=%s,
                        attempt_count=attempt_count+1
                    WHERE id = ANY(%s)
                      AND rule_id=%s
                      AND status='processing'
                    """,
                    ((error_text or "")[:1000], ids, int(rule_id)),
                )
                marked = int(cur.rowcount or 0)
                if marked:
                    cur.execute(
                        """
                        UPDATE delivery_attempts
                        SET status='failed_after_send', error_text=%s, updated_at=NOW()
                        WHERE idempotency_key=%s AND status IN ('sending', 'accepted', 'failed_after_send')
                        """,
                        ((error_text or "")[:1000], str(idempotency_key)),
                    )
            conn.commit()
            return marked

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

    def count_uncleared_faulty_deliveries(self) -> int:
        """Count delivery IDs still visible in the admin faulty-delivery list."""
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*) AS cnt
                    FROM deliveries d
                    JOIN posts p ON p.id = d.post_id
                    JOIN routing r ON r.id = d.rule_id
                    WHERE d.status = 'faulty'
                      AND (d.error_text IS NULL OR (
                          d.error_text NOT LIKE 'Self-loop:%'
                          AND d.error_text <> 'self_target_echo'
                      ))
                      AND NOT EXISTS (
                          SELECT 1 FROM audit_log cleared
                          WHERE cleared.delivery_id = d.id
                            AND cleared.event_type = 'faulty_log_cleared'
                      )
                    """
                )
                return int(cur.fetchone()["cnt"])

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
                     AND COALESCE(s.tenant_id, 1) = COALESCE(r.tenant_id, d.tenant_id, 1)
                     AND ((s.thread_id IS NULL AND r.source_thread_id IS NULL) OR s.thread_id = r.source_thread_id)
                     AND s.channel_type = 'source'
                    LEFT JOIN channels t
                      ON t.channel_id = r.target_id
                     AND COALESCE(t.tenant_id, 1) = COALESCE(r.tenant_id, d.tenant_id, 1)
                     AND ((t.thread_id IS NULL AND r.target_thread_id IS NULL) OR t.thread_id = r.target_thread_id)
                     AND t.channel_type = 'target'
                    WHERE d.status = 'faulty'
                      AND (d.error_text IS NULL OR (
                          d.error_text NOT LIKE 'Self-loop:%%'
                          AND d.error_text <> 'self_target_echo'
                      ))
                      AND NOT EXISTS (
                          SELECT 1 FROM audit_log cleared
                          WHERE cleared.delivery_id = d.id
                            AND cleared.event_type = 'faulty_log_cleared'
                      )
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

    def heartbeat_job_processing(self, job_id: int, worker_id: str) -> bool:
        """Refresh liveness only for the execution that still owns the processing job."""
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE jobs
                       SET updated_at = NOW()
                     WHERE id = %s AND status = 'processing' AND locked_by = %s
                    """,
                    (int(job_id), str(worker_id)),
                )
                owned = cur.rowcount > 0
            conn.commit()
        return owned

    def finalize_owned_job(
        self,
        job_id: int,
        worker_id: str,
        action: str,
        error_text: str = "",
        delay_seconds: int = 0,
    ) -> bool:
        """Fence stale tasks: an earlier execution must not overwrite the new owner's status."""
        if action == "complete":
            sql = """
                UPDATE jobs SET status = 'done', lease_until = NULL, locked_by = NULL,
                                error_text = NULL, updated_at = NOW()
                 WHERE id = %s AND status = 'processing' AND locked_by = %s
            """
            params = (int(job_id), str(worker_id))
        elif action in {"retry", "defer"}:
            sql = """
                UPDATE jobs SET status = 'retry', attempts = attempts + %s,
                                lease_until = NULL, locked_by = NULL,
                                run_at = NOW() + make_interval(secs => %s),
                                error_text = %s, updated_at = NOW()
                 WHERE id = %s AND status = 'processing' AND locked_by = %s
            """
            params = (
                1 if action == "retry" else 0, max(1, int(delay_seconds)),
                (error_text or "")[:1000], int(job_id), str(worker_id),
            )
        elif action == "fail":
            sql = """
                UPDATE jobs SET status = 'failed', attempts = attempts + 1,
                                lease_until = NULL, locked_by = NULL,
                                error_text = %s, updated_at = NOW()
                 WHERE id = %s AND status = 'processing' AND locked_by = %s
            """
            params = ((error_text or "")[:1000], int(job_id), str(worker_id))
        else:
            raise ValueError(f"Unsupported owned job action: {action}")
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                updated = cur.rowcount > 0
            conn.commit()
        return updated

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

    def defer_job(self, job_id: int, error_text: str, delay_seconds: int) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE jobs
                    SET status = 'retry',
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
                    FROM deliveries d
                    WHERE d.id = %s
                      AND d.status = 'faulty'
                      AND NOT EXISTS (
                          SELECT 1 FROM audit_log cleared
                          WHERE cleared.delivery_id = d.id
                            AND cleared.event_type = 'faulty_log_cleared'
                      )
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

                # This is an acknowledgement, not a delivery state change.
                # Keep the send/attempt ledger for safe incident reconciliation.
            conn.commit()
            return True

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
                        r.video_clip_duration_seconds,
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
                        r.video_clip_duration_seconds,
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


    def get_delivery_observability_rule_metrics(self) -> list[DeliveryRuleMetrics]:
        """Return read-only delivery diagnostics metrics grouped by rule."""
        from app.delivery_observability import DeliveryRuleMetrics, sanitize_diagnostic_text

        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        r.id AS rule_id,
                        r.source_id AS source_id,
                        r.target_id AS target_id,
                        COALESCE(COUNT(d.id) FILTER (WHERE d.status = 'pending'), 0) AS pending_count,
                        COALESCE(COUNT(d.id) FILTER (WHERE d.status = 'processing'), 0) AS processing_count,
                        COALESCE(COUNT(d.id) FILTER (WHERE d.status = 'sent'), 0) AS sent_count,
                        COALESCE(COUNT(d.id) FILTER (WHERE d.status = 'faulty'), 0) AS faulty_count,
                        0 AS deferred_count,
                        COALESCE(COUNT(d.id) FILTER (
                            WHERE d.status = 'faulty'
                              AND d.error_text IS NOT NULL
                              AND (
                                  d.error_text ILIKE '%%retryafter%%'
                                  OR d.error_text ILIKE '%%retry after%%'
                                  OR d.error_text ILIKE '%%rate limit%%'
                                  OR d.error_text ILIKE '%%too many requests%%'
                              )
                        ), 0) AS rate_limited_count,
                        EXTRACT(EPOCH FROM (NOW() - MIN(d.created_at::timestamptz) FILTER (WHERE d.status = 'pending'))) AS oldest_pending_age_seconds,
                        EXTRACT(EPOCH FROM (NOW() - MIN(processing_clock.processing_job_created_at) FILTER (WHERE d.status = 'processing'))) AS oldest_processing_age_seconds,
                        EXTRACT(EPOCH FROM (r.next_run_at::timestamptz - NOW())) AS next_run_in_seconds,
                        last_fault.error_text AS last_error_text
                    FROM routing r
                    LEFT JOIN deliveries d ON d.rule_id = r.id
                    LEFT JOIN LATERAL (
                        SELECT MIN(j.created_at) AS processing_job_created_at
                        FROM jobs j
                        WHERE d.status = 'processing'
                          AND j.status IN ('pending', 'leased', 'processing', 'retry')
                          AND (
                              (
                                  (j.payload_json ->> 'delivery_id') ~ '^[0-9]+$'
                                  AND (j.payload_json ->> 'delivery_id')::bigint = d.id
                              )
                              OR EXISTS (
                                  SELECT 1
                                  FROM jsonb_array_elements_text(COALESCE(j.payload_json -> 'delivery_ids', '[]'::jsonb)) AS delivery_ids(value)
                                  WHERE delivery_ids.value ~ '^[0-9]+$'
                                    AND delivery_ids.value::bigint = d.id
                              )
                          )
                    ) AS processing_clock ON TRUE
                    LEFT JOIN LATERAL (
                        SELECT fd.error_text
                        FROM deliveries fd
                        WHERE fd.rule_id = r.id
                          AND fd.status = 'faulty'
                          AND fd.error_text IS NOT NULL
                        ORDER BY fd.id DESC
                        LIMIT 1
                    ) AS last_fault ON TRUE
                    GROUP BY r.id, r.source_id, r.target_id, r.next_run_at, last_fault.error_text
                    ORDER BY r.id
                    """
                )
                rows = cur.fetchall()

        metrics: list[DeliveryRuleMetrics] = []
        for row in rows:
            last_error_text = sanitize_diagnostic_text(row.get("last_error_text"), max_length=500)
            last_error_type = None
            if last_error_text:
                last_error_type = sanitize_diagnostic_text(last_error_text.split(":", 1)[0], max_length=120)
            metrics.append(
                DeliveryRuleMetrics(
                    rule_id=row.get("rule_id"),
                    source_id=row.get("source_id"),
                    target_id=row.get("target_id"),
                    pending_count=int(row.get("pending_count") or 0),
                    processing_count=int(row.get("processing_count") or 0),
                    sent_count=int(row.get("sent_count") or 0),
                    faulty_count=int(row.get("faulty_count") or 0),
                    deferred_count=int(row.get("deferred_count") or 0),
                    rate_limited_count=int(row.get("rate_limited_count") or 0),
                    oldest_pending_age_seconds=row.get("oldest_pending_age_seconds"),
                    oldest_processing_age_seconds=row.get("oldest_processing_age_seconds"),
                    next_run_in_seconds=row.get("next_run_in_seconds"),
                    last_error_type=last_error_type,
                    last_error_text=last_error_text,
                )
            )
        return metrics

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

            active_top_time_pause = self.get_active_campaign_top_time_pause_for_target(
                target_id=str(rule.target_id),
                target_thread_id=rule.target_thread_id,
            ) if rule.target_id else None

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
                "video_clip_duration_seconds": getattr(rule, "video_clip_duration_seconds", 118),
                "video_add_intro": rule.video_add_intro,
                "video_intro_horizontal": rule.video_intro_horizontal,
                "video_intro_vertical": rule.video_intro_vertical,
                "video_intro_horizontal_id": rule.video_intro_horizontal_id,
                "video_intro_vertical_id": rule.video_intro_vertical_id,
                "video_caption": rule.video_caption,
                "video_caption_entities_json": rule.video_caption_entities_json,
                "repost_caption": rule.repost_caption,
                "repost_caption_enabled": rule.repost_caption_enabled,
                "repost_caption_delivery_mode": rule.repost_caption_delivery_mode,
                "caption_delivery_mode": getattr(rule, "caption_delivery_mode", "auto"),
                "video_caption_delivery_mode": getattr(rule, "video_caption_delivery_mode", "copy_first"),
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
                "active_top_time_pause": active_top_time_pause,
                "top_time_pause_ends_at": (active_top_time_pause or {}).get("ends_at"),
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

    def _get_rule_pending_logical_items_conn(self, conn, rule_id: int, mode: str) -> list[dict[str, Any]]:
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

        return self._build_rule_logical_items_from_rows(rule_id=rule_id, rows=rows, mode=mode)

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
                items = self._get_rule_pending_logical_items_conn(conn, rule_id, mode)

            logger.info(
                "get_rule_queue_logical_items: rule_id=%s, mode=%s, logical_items=%s",
                rule_id,
                mode,
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
            with self.connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT mode FROM routing WHERE id=%s AND rescan_owner_token IS NULL FOR UPDATE", (rule_id,))
                    rule = cur.fetchone()
                    if not rule:
                        return None
                    items = self._get_rule_pending_logical_items_conn(conn, rule_id, rule['mode'])
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
                    if ids_to_mark_sent:
                        cur.executemany(
                            """
                            UPDATE deliveries
                            SET status = 'sent',
                                sent_at = %s,
                                error_text = NULL
                            WHERE id = %s AND status='pending'
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


    def create_saved_post(self, *, rule_id: int | None, title: str | None, content: dict[str, Any], source_chat_id: str | None, source_message_id: int | None, source_media_group_id: str | None, created_by: int | None) -> int | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO saved_posts(rule_id,title,content_json,source_chat_id,source_message_id,source_media_group_id,created_by)
                    VALUES (%s,%s,%s::jsonb,%s,%s,%s,%s)
                    RETURNING id
                """, (rule_id, title, json.dumps(content, ensure_ascii=False), source_chat_id, source_message_id, source_media_group_id, created_by))
                row = cur.fetchone()
                return int(row["id"]) if row else None

    def get_saved_post(self, saved_post_id: int) -> dict[str, Any] | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM saved_posts WHERE id=%s AND archived_at IS NULL", (int(saved_post_id),))
                return cur.fetchone()

    def list_saved_posts(self, *, rule_id: int | None = None, limit: int = 20) -> list[dict[str, Any]]:
        with self.connect() as conn:
            with conn.cursor() as cur:
                if rule_id is None:
                    cur.execute("SELECT * FROM saved_posts WHERE archived_at IS NULL AND status='ready' ORDER BY created_at DESC LIMIT %s", (int(limit),))
                else:
                    cur.execute("SELECT * FROM saved_posts WHERE archived_at IS NULL AND status='ready' AND rule_id=%s ORDER BY created_at DESC LIMIT %s", (int(rule_id), int(limit)))
                return cur.fetchall() or []

    def update_saved_post_content(self, saved_post_id: int, *, title: str | None, content: dict[str, Any]) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE saved_posts SET title=%s, content_json=%s::jsonb, updated_at=NOW() WHERE id=%s", (title, json.dumps(content, ensure_ascii=False), int(saved_post_id)))
                return cur.rowcount > 0

    def archive_saved_post(self, saved_post_id: int) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE saved_posts SET archived_at=NOW(), status='archived', updated_at=NOW() WHERE id=%s", (int(saved_post_id),))
                return cur.rowcount > 0


    def get_rule_repost_campaign_top_time_settings(self, rule_id: int) -> dict[str, Any]:
        default = {"rule_id": int(rule_id), "enabled": False, "seconds": 0}
        try:
            with self.connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT id, repost_campaign_top_time_enabled, repost_campaign_top_time_seconds
                        FROM routing
                        WHERE id = %s
                        LIMIT 1
                        """,
                        (int(rule_id),),
                    )
                    row = cur.fetchone()
            if not row:
                return default
            data = dict(row)
            return {
                "rule_id": int(data.get("id") or rule_id),
                "enabled": bool(data.get("repost_campaign_top_time_enabled") or False),
                "seconds": int(data.get("repost_campaign_top_time_seconds") or 0),
            }
        except Exception as exc:
            logger.exception("REPOST_CAMPAIGN_TOP_TIME_SETTINGS_GET_FAILED | rule_id=%s | error=%s", rule_id, exc)
            return default

    def set_rule_repost_campaign_top_time_settings(
        self,
        rule_id: int,
        *,
        enabled: bool,
        seconds: int,
        actor_id: int | None = None,
    ) -> bool:
        allowed_seconds = {900, 1800, 3600, 7200, 10800, 21600, 43200}
        normalized_enabled = bool(enabled)
        normalized_seconds = int(seconds or 0)
        if not normalized_enabled:
            normalized_seconds = 0
        elif normalized_seconds not in allowed_seconds:
            normalized_seconds = 7200
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE routing
                    SET
                        repost_campaign_top_time_enabled = %s,
                        repost_campaign_top_time_seconds = %s
                    WHERE id = %s
                    """,
                    (normalized_enabled, normalized_seconds, int(rule_id)),
                )
                updated = int(cur.rowcount or 0) > 0
            conn.commit()
            return updated

    def get_campaign_invite_link_settings(self, rule_id: int) -> dict[str, Any]:
        default = _default_campaign_invite_link_settings(rule_id)
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM campaign_invite_link_settings
                    WHERE rule_id = %s
                    LIMIT 1
                    """,
                    (int(rule_id),),
                )
                row = cur.fetchone()
        if not row:
            return default

        data = dict(row)
        result = {**default, **data}
        result["rule_id"] = int(result.get("rule_id") or rule_id)
        result["enabled"] = bool(result.get("enabled"))
        result["per_target_links_enabled"] = bool(result.get("per_target_links_enabled"))
        result["preview_required"] = bool(result.get("preview_required"))
        return result

    def set_campaign_invite_link_settings(
        self,
        rule_id: int,
        *,
        enabled: bool | None = None,
        destination_chat_id: str | None = None,
        destination_chat_title: str | None = None,
        link_mode: str | None = None,
        injection_mode: str | None = None,
        append_template: str | None = None,
        per_target_links_enabled: bool | None = None,
        preview_required: bool | None = None,
        actor_id: int | None = None,
    ) -> bool:
        if link_mode is not None and link_mode not in CAMPAIGN_INVITE_LINK_ALLOWED_LINK_MODES:
            return False
        if injection_mode is not None and injection_mode not in CAMPAIGN_INVITE_LINK_ALLOWED_INJECTION_MODES:
            return False

        reset_preview = any(
            value is not None
            for value in (destination_chat_id, destination_chat_title, link_mode, injection_mode, append_template)
        )
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO campaign_invite_link_settings(rule_id)
                    VALUES (%s)
                    ON CONFLICT (rule_id) DO NOTHING
                    """,
                    (int(rule_id),),
                )
                cur.execute(
                    """
                    UPDATE campaign_invite_link_settings
                    SET
                        enabled = COALESCE(%s, enabled),
                        destination_chat_id = COALESCE(%s, destination_chat_id),
                        destination_chat_title = COALESCE(%s, destination_chat_title),
                        link_mode = COALESCE(%s, link_mode),
                        injection_mode = COALESCE(%s, injection_mode),
                        append_template = COALESCE(%s, append_template),
                        per_target_links_enabled = COALESCE(%s, per_target_links_enabled),
                        preview_required = COALESCE(%s, preview_required),
                        preview_checked_at = CASE WHEN %s THEN NULL ELSE preview_checked_at END,
                        preview_checked_by = CASE WHEN %s THEN NULL ELSE preview_checked_by END,
                        updated_at = NOW()
                    WHERE rule_id = %s
                    """,
                    (
                        enabled,
                        destination_chat_id,
                        destination_chat_title,
                        link_mode,
                        injection_mode,
                        append_template,
                        per_target_links_enabled,
                        preview_required,
                        reset_preview,
                        reset_preview,
                        int(rule_id),
                    ),
                )
                updated = cur.rowcount > 0
            conn.commit()
        return updated

    def mark_campaign_invite_link_preview_checked(
        self,
        rule_id: int,
        *,
        actor_id: int | None = None,
        checked_at: datetime | None = None,
    ) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO campaign_invite_link_settings(rule_id, preview_checked_at, preview_checked_by)
                    VALUES (%s, COALESCE(%s, NOW()), %s)
                    ON CONFLICT (rule_id) DO UPDATE SET
                        preview_checked_at = COALESCE(EXCLUDED.preview_checked_at, NOW()),
                        preview_checked_by = EXCLUDED.preview_checked_by,
                        updated_at = NOW()
                    """,
                    (int(rule_id), checked_at, actor_id),
                )
                updated = cur.rowcount > 0
            conn.commit()
        return updated

    def create_campaign_invite_link_record(
        self,
        *,
        rule_id: int,
        destination_chat_id: str,
        invite_link: str,
        invite_link_hash: str,
        link_mode: str,
        creates_join_request: bool,
        destination_chat_title: str | None = None,
        campaign_run_id: int | None = None,
        campaign_run_message_id: int | None = None,
        saved_post_id: int | None = None,
        ad_target_id: str | None = None,
        ad_target_thread_id: int | None = None,
        ad_target_title: str | None = None,
        invite_link_name: str | None = None,
        telegram_payload_json: dict | None = None,
        created_by: int | None = None,
        status: str = "active",
    ) -> int | None:
        if link_mode not in CAMPAIGN_INVITE_LINK_ALLOWED_LINK_MODES:
            return None
        if status not in CAMPAIGN_INVITE_LINK_ALLOWED_STATUSES:
            return None
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO campaign_invite_links(
                        rule_id, campaign_run_id, campaign_run_message_id, saved_post_id,
                        destination_chat_id, destination_chat_title, ad_target_id, ad_target_thread_id,
                        ad_target_title, link_mode, invite_link, invite_link_name, invite_link_hash,
                        creates_join_request, status, created_by, telegram_payload_json
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                    ON CONFLICT (invite_link_hash) DO UPDATE SET invite_link_hash = EXCLUDED.invite_link_hash
                    RETURNING id
                    """,
                    (
                        int(rule_id), campaign_run_id, campaign_run_message_id, saved_post_id,
                        str(destination_chat_id), destination_chat_title, ad_target_id, ad_target_thread_id,
                        ad_target_title, link_mode, invite_link, invite_link_name, invite_link_hash,
                        bool(creates_join_request), status, created_by, _json_dumps(telegram_payload_json or {}),
                    ),
                )
                row = cur.fetchone()
            conn.commit()
        return int(row["id"]) if row else None

    def get_campaign_invite_link(self, invite_link_id: int) -> dict[str, Any] | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM campaign_invite_links WHERE id = %s LIMIT 1", (int(invite_link_id),))
                row = cur.fetchone()
        return dict(row) if row else None

    def get_campaign_invite_link_by_hash(self, invite_link_hash: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM campaign_invite_links WHERE invite_link_hash = %s LIMIT 1", (invite_link_hash,))
                row = cur.fetchone()
        return dict(row) if row else None

    def list_campaign_invite_links_for_rule(
        self,
        rule_id: int,
        *,
        statuses: list[str] | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        safe_limit = max(1, min(int(limit or 50), 100))
        with self.connect() as conn:
            with conn.cursor() as cur:
                if statuses is None:
                    cur.execute(
                        """
                        SELECT * FROM campaign_invite_links
                        WHERE rule_id = %s
                        ORDER BY created_at DESC, id DESC
                        LIMIT %s
                        """,
                        (int(rule_id), safe_limit),
                    )
                else:
                    cur.execute(
                        """
                        SELECT * FROM campaign_invite_links
                        WHERE rule_id = %s AND status = ANY(%s)
                        ORDER BY created_at DESC, id DESC
                        LIMIT %s
                        """,
                        (int(rule_id), list(statuses), safe_limit),
                    )
                rows = cur.fetchall()
        return [dict(row) for row in rows]

    def archive_campaign_invite_link(self, invite_link_id: int, *, actor_id: int | None = None) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE campaign_invite_links
                    SET status = 'archived', archived_at = NOW(), updated_at = NOW()
                    WHERE id = %s
                    """,
                    (int(invite_link_id),),
                )
                updated = int(cur.rowcount or 0) > 0
            conn.commit()
        return updated

    def mark_campaign_invite_link_revoked(
        self,
        invite_link_id: int,
        *,
        revoked_at: datetime | None = None,
        telegram_payload_json: dict | None = None,
    ) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                if telegram_payload_json is None:
                    cur.execute(
                        """
                        UPDATE campaign_invite_links
                        SET status = 'revoked', revoked_at = COALESCE(%s, NOW()), updated_at = NOW()
                        WHERE id = %s
                        """,
                        (revoked_at, int(invite_link_id)),
                    )
                else:
                    cur.execute(
                        """
                        UPDATE campaign_invite_links
                        SET status = 'revoked', revoked_at = COALESCE(%s, NOW()), updated_at = NOW(),
                            telegram_payload_json = %s::jsonb
                        WHERE id = %s
                        """,
                        (revoked_at, _json_dumps(telegram_payload_json), int(invite_link_id)),
                    )
                updated = int(cur.rowcount or 0) > 0
            conn.commit()
        return updated

    def bind_campaign_invite_link_to_run_message(
        self,
        invite_link_id: int,
        *,
        campaign_run_message_id: int,
        campaign_run_id: int,
    ) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE campaign_invite_links
                    SET campaign_run_message_id = %s, campaign_run_id = %s, updated_at = NOW()
                    WHERE id = %s
                    """,
                    (int(campaign_run_message_id), int(campaign_run_id), int(invite_link_id)),
                )
                updated = int(cur.rowcount or 0) > 0
            conn.commit()
        return updated

    def get_latest_campaign_invite_link_event_for_user(
        self,
        *,
        destination_chat_id: str,
        telegram_user_id_hash: str,
        event_types: list[str] | None = None,
    ) -> dict[str, Any] | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                if event_types is None:
                    cur.execute(
                        """
                        SELECT
                            id, invite_link_id, rule_id, campaign_run_id, campaign_run_message_id,
                            destination_chat_id, ad_target_id, ad_target_thread_id, event_type,
                            telegram_user_id_hash, event_at
                        FROM campaign_invite_link_events
                        WHERE destination_chat_id = %s AND telegram_user_id_hash = %s
                        ORDER BY event_at DESC, id DESC
                        LIMIT 1
                        """,
                        (str(destination_chat_id), telegram_user_id_hash),
                    )
                else:
                    cur.execute(
                        """
                        SELECT
                            id, invite_link_id, rule_id, campaign_run_id, campaign_run_message_id,
                            destination_chat_id, ad_target_id, ad_target_thread_id, event_type,
                            telegram_user_id_hash, event_at
                        FROM campaign_invite_link_events
                        WHERE destination_chat_id = %s
                          AND telegram_user_id_hash = %s
                          AND event_type = ANY(%s)
                        ORDER BY event_at DESC, id DESC
                        LIMIT 1
                        """,
                        (str(destination_chat_id), telegram_user_id_hash, list(event_types)),
                    )
                row = cur.fetchone()
        return dict(row) if row else None

    def create_campaign_invite_link_event(
        self,
        *,
        invite_link_id: int,
        rule_id: int,
        destination_chat_id: str,
        event_type: str,
        telegram_user_id_hash: str,
        campaign_run_id: int | None = None,
        campaign_run_message_id: int | None = None,
        ad_target_id: str | None = None,
        ad_target_thread_id: int | None = None,
        telegram_update_id: int | None = None,
        telegram_user_payload_json: dict | None = None,
        raw_update_json: dict | None = None,
        event_at: datetime | None = None,
    ) -> int | None:
        if event_type not in CAMPAIGN_INVITE_LINK_ALLOWED_EVENT_TYPES:
            return None
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO campaign_invite_link_events(
                        invite_link_id, rule_id, campaign_run_id, campaign_run_message_id,
                        destination_chat_id, ad_target_id, ad_target_thread_id, event_type,
                        telegram_user_id_hash, telegram_user_payload_json, telegram_update_id,
                        event_at, raw_update_json
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, COALESCE(%s, NOW()), %s::jsonb)
                    ON CONFLICT (invite_link_id, event_type, telegram_user_id_hash) DO UPDATE
                    SET telegram_user_id_hash = EXCLUDED.telegram_user_id_hash
                    RETURNING id
                    """,
                    (
                        int(invite_link_id), int(rule_id), campaign_run_id, campaign_run_message_id,
                        str(destination_chat_id), ad_target_id, ad_target_thread_id, event_type,
                        telegram_user_id_hash, _json_dumps(telegram_user_payload_json or {}), telegram_update_id,
                        event_at, _json_dumps(raw_update_json or {}),
                    ),
                )
                row = cur.fetchone()
            conn.commit()
        return int(row["id"]) if row else None

    def get_campaign_invite_link_stats_for_rule(self, rule_id: int) -> dict[str, Any]:
        result = {
            "rule_id": int(rule_id),
            "links_total": 0,
            "links_active": 0,
            "join_requests_total": 0,
            "joins_total": 0,
            "left_total": 0,
            "kicked_total": 0,
            "unknown_total": 0,
            "unique_users_total": 0,
        }
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        COUNT(*) AS links_total,
                        COUNT(*) FILTER (WHERE status = 'active') AS links_active
                    FROM campaign_invite_links
                    WHERE rule_id = %s
                    """,
                    (int(rule_id),),
                )
                link_row = cur.fetchone() or {}
                cur.execute(
                    """
                    SELECT
                        COUNT(*) FILTER (WHERE event_type = 'join_request_created') AS join_requests_total,
                        COUNT(*) FILTER (WHERE event_type = 'member_joined') AS joins_total,
                        COUNT(*) FILTER (WHERE event_type = 'member_left') AS left_total,
                        COUNT(*) FILTER (WHERE event_type = 'member_kicked') AS kicked_total,
                        COUNT(*) FILTER (WHERE event_type = 'member_unknown') AS unknown_total,
                        COUNT(DISTINCT telegram_user_id_hash) AS unique_users_total
                    FROM campaign_invite_link_events
                    WHERE rule_id = %s
                    """,
                    (int(rule_id),),
                )
                event_row = cur.fetchone() or {}
        for key in result:
            if key in link_row:
                result[key] = int(link_row.get(key) or 0)
            if key in event_row:
                result[key] = int(event_row.get(key) or 0)
        return result


    def get_campaign_invite_link_stats(self, invite_link_id: int) -> dict[str, Any]:
        result = {
            "invite_link_id": int(invite_link_id),
            "join_requests_total": 0,
            "joins_total": 0,
            "left_total": 0,
            "kicked_total": 0,
            "unknown_total": 0,
            "unique_users_total": 0,
        }
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        COUNT(*) FILTER (WHERE event_type = 'join_request_created') AS join_requests_total,
                        COUNT(*) FILTER (WHERE event_type = 'member_joined') AS joins_total,
                        COUNT(*) FILTER (WHERE event_type = 'member_left') AS left_total,
                        COUNT(*) FILTER (WHERE event_type = 'member_kicked') AS kicked_total,
                        COUNT(*) FILTER (WHERE event_type = 'member_unknown') AS unknown_total,
                        COUNT(DISTINCT telegram_user_id_hash) AS unique_users_total
                    FROM campaign_invite_link_events
                    WHERE invite_link_id = %s
                    """,
                    (int(invite_link_id),),
                )
                row = cur.fetchone() or {}
        for key in result:
            if key in row:
                result[key] = int(row.get(key) or 0)
        return result

    def get_rule_repost_campaign_clean_channel_settings(self, rule_id: int) -> dict[str, Any]:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, repost_campaign_clean_channel_enabled
                    FROM routing
                    WHERE id = %s
                    LIMIT 1
                    """,
                    (int(rule_id),),
                )
                row = cur.fetchone()

        if not row:
            return {
                "ok": False,
                "rule_id": int(rule_id),
                "enabled": True,
                "error_text": "Правило не найдено",
            }

        data = dict(row)
        raw_enabled = data.get("repost_campaign_clean_channel_enabled")
        return {
            "ok": True,
            "rule_id": int(data.get("id") or rule_id),
            "enabled": bool(raw_enabled if raw_enabled is not None else True),
        }

    def set_rule_repost_campaign_clean_channel_enabled(
        self,
        rule_id: int,
        enabled: bool,
        actor_id: int | None = None,
    ) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE routing
                    SET repost_campaign_clean_channel_enabled = %s
                    WHERE id = %s
                    """,
                    (bool(enabled), int(rule_id)),
                )
                updated = cur.rowcount > 0
            conn.commit()
        return updated

    def set_rule_repost_campaign_saved_post(self, rule_id: int, saved_post_id: int | None) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE routing SET repost_campaign_saved_post_id=%s WHERE id=%s", (saved_post_id, int(rule_id)))
                return cur.rowcount > 0

    def create_campaign_run(self, *, rule_id: int, saved_post_id: int, run_type: str, status: str, show_seconds: int, started_by: int | None, render_mode: str | None = None, targets_total: int = 0, scheduled_post_id: int | None = None, top_time_enabled_snapshot: bool = False, top_time_seconds_snapshot: int = 0) -> int | None:
        top_time = normalize_repost_campaign_top_time_settings(
            enabled=bool(top_time_enabled_snapshot),
            seconds=int(top_time_seconds_snapshot or 0),
        )
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO campaign_runs(tenant_id, rule_id, saved_post_id, run_type, status, render_mode, show_seconds, targets_total, started_by, scheduled_post_id, top_time_enabled_snapshot, top_time_seconds_snapshot)
                    VALUES (1, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (int(rule_id), int(saved_post_id), str(run_type), str(status), render_mode, int(show_seconds), int(targets_total), started_by, scheduled_post_id, bool(top_time["enabled"]), int(top_time["seconds"])),
                )
                row = cur.fetchone()
            conn.commit()
            return int(row["id"]) if row else None

    def create_campaign_scheduled_launch(self, *, rule_id: int, saved_post_id: int, show_seconds: int, scheduled_at: str, timezone_offset_minutes: int = 180, timezone_label: str = "UTC+3", created_by: int | None = None, preview: dict[str, Any] | None = None, top_time_enabled_snapshot: bool = False, top_time_seconds_snapshot: int = 0) -> int | None:
        top_time = normalize_repost_campaign_top_time_settings(
            enabled=bool(top_time_enabled_snapshot),
            seconds=int(top_time_seconds_snapshot or 0),
        )
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("""SELECT id FROM campaign_scheduled_launches WHERE rule_id=%s AND saved_post_id=%s AND scheduled_at=%s AND created_by IS NOT DISTINCT FROM %s AND status='scheduled' ORDER BY id DESC LIMIT 1""", (int(rule_id), int(saved_post_id), scheduled_at, created_by))
                row = cur.fetchone()
                if row:
                    return int(row['id'])
                cur.execute("""INSERT INTO campaign_scheduled_launches(tenant_id,rule_id,saved_post_id,show_seconds,scheduled_at,timezone_offset_minutes,timezone_label,status,created_by,preview_json,top_time_enabled_snapshot,top_time_seconds_snapshot) VALUES (1,%s,%s,%s,%s,%s,%s,'scheduled',%s,%s::jsonb,%s,%s) RETURNING id""", (int(rule_id), int(saved_post_id), int(show_seconds), scheduled_at, int(timezone_offset_minutes), str(timezone_label), created_by, _json_dumps(preview), bool(top_time["enabled"]), int(top_time["seconds"])))
                row = cur.fetchone()
            conn.commit()
            return int(row['id']) if row else None

    def _normalize_campaign_scheduled_launch_row(self, row: dict[str, Any] | None) -> dict[str, Any] | None:
        if not row:
            return None
        d = dict(row)
        d["preview_json"] = _safe_json_loads(d.get("preview_json"), {})
        d["clean_channel_policy_json"] = _safe_json_loads(d.get("clean_channel_policy_json"), None)
        return d

    def get_campaign_scheduled_launch(self, scheduled_launch_id: int) -> dict[str, Any] | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM campaign_scheduled_launches WHERE id=%s", (int(scheduled_launch_id),))
                return self._normalize_campaign_scheduled_launch_row(cur.fetchone())

    def list_rule_campaign_scheduled_launches(self, rule_id: int, *, statuses: list[str] | None = None, limit: int = 20) -> list[dict[str, Any]]:
        with self.connect() as conn:
            with conn.cursor() as cur:
                if statuses:
                    cur.execute("SELECT * FROM campaign_scheduled_launches WHERE rule_id=%s AND status = ANY(%s) ORDER BY scheduled_at DESC, id DESC LIMIT %s", (int(rule_id), statuses, int(limit)))
                else:
                    cur.execute("SELECT * FROM campaign_scheduled_launches WHERE rule_id=%s ORDER BY scheduled_at DESC, id DESC LIMIT %s", (int(rule_id), int(limit)))
                return [self._normalize_campaign_scheduled_launch_row(r) for r in (cur.fetchall() or [])]

    def list_due_campaign_scheduled_launches(self, *, now_iso: str, limit: int = 10) -> list[dict[str, Any]]:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM campaign_scheduled_launches
                    WHERE (status = 'scheduled' AND scheduled_at <= %s)
                       OR (status = 'waiting_clean_channel'
                           AND COALESCE(clean_channel_next_retry_at, scheduled_at) <= %s
                           AND campaign_run_id IS NULL)
                    ORDER BY COALESCE(clean_channel_next_retry_at, scheduled_at) ASC, id ASC
                    LIMIT %s
                    """,
                    (now_iso, now_iso, int(limit)),
                )
                return [self._normalize_campaign_scheduled_launch_row(r) for r in (cur.fetchall() or [])]

    def claim_due_campaign_scheduled_launches(self, *, now_iso: str, worker_id: str, limit: int = 5) -> list[dict[str, Any]]:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    WITH picked AS (
                        SELECT id
                        FROM campaign_scheduled_launches
                        WHERE (status = 'scheduled' AND scheduled_at <= %s)
                           OR (status = 'waiting_clean_channel'
                               AND COALESCE(clean_channel_next_retry_at, scheduled_at) <= %s
                               AND campaign_run_id IS NULL)
                        ORDER BY COALESCE(clean_channel_next_retry_at, scheduled_at) ASC, id ASC
                        FOR UPDATE SKIP LOCKED
                        LIMIT %s
                    )
                    UPDATE campaign_scheduled_launches s
                    SET status = 'processing', locked_at = NOW(), locked_by = %s, updated_at = NOW()
                    FROM picked
                    WHERE s.id = picked.id
                    RETURNING s.*
                    """,
                    (now_iso, now_iso, int(limit), worker_id),
                )
                rows = [self._normalize_campaign_scheduled_launch_row(r) for r in (cur.fetchall() or [])]
            conn.commit()
            return rows

    def set_campaign_scheduled_launch_campaign_run_id(self, scheduled_launch_id: int, campaign_run_id: int) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE campaign_scheduled_launches SET campaign_run_id=%s, updated_at=NOW() WHERE id=%s AND status='processing'", (int(campaign_run_id), int(scheduled_launch_id)))
                ok=cur.rowcount>0
            conn.commit(); return ok

    def mark_campaign_scheduled_launch_launched(self, scheduled_launch_id: int, *, campaign_run_id: int) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE campaign_scheduled_launches SET status='launched', campaign_run_id=%s, launched_at=NOW(), locked_at=NULL, locked_by=NULL, updated_at=NOW() WHERE id=%s AND status='processing'", (int(campaign_run_id), int(scheduled_launch_id)))
                ok=cur.rowcount>0
            conn.commit(); return ok

    def mark_campaign_scheduled_launch_failed(self, scheduled_launch_id: int, *, error_text: str) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE campaign_scheduled_launches SET status='failed', failed_at=NOW(), error_text=%s, locked_at=NULL, locked_by=NULL, updated_at=NOW() WHERE id=%s AND status IN ('processing','scheduled')", (error_text, int(scheduled_launch_id)))
                ok=cur.rowcount>0
            conn.commit(); return ok

    def mark_campaign_scheduled_launch_needs_review(self, scheduled_launch_id: int, *, error_text: str, campaign_run_id: int | None = None) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE campaign_scheduled_launches SET status='needs_review', failed_at=NOW(), error_text=%s, campaign_run_id=COALESCE(%s, campaign_run_id), locked_at=NULL, locked_by=NULL, updated_at=NOW() WHERE id=%s AND status='processing'", (error_text, campaign_run_id, int(scheduled_launch_id)))
                ok=cur.rowcount>0
            conn.commit(); return ok

    def _clean_channel_wait_reason(self, reason: str | None) -> str:
        safe_reason = (reason or "").strip()
        if not safe_reason or "Traceback" in safe_reason or "Traceback (most recent call last)" in safe_reason:
            return "Чистый канал занят активной рекламой"
        return safe_reason[:500]

    def mark_campaign_scheduled_launch_waiting_clean_channel(
        self,
        scheduled_launch_id: int,
        *,
        next_retry_at: str,
        reason: str | None = None,
        policy_snapshot: dict[str, Any] | None = None,
    ) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE campaign_scheduled_launches
                    SET status = 'waiting_clean_channel',
                        clean_channel_next_retry_at = %s,
                        clean_channel_wait_attempt_count = COALESCE(clean_channel_wait_attempt_count, 0) + 1,
                        clean_channel_last_wait_at = NOW(),
                        clean_channel_last_reason = %s,
                        clean_channel_policy_json = %s::jsonb,
                        updated_at = NOW(),
                        locked_at = NULL,
                        locked_by = NULL
                    WHERE id = %s
                      AND status IN ('scheduled', 'processing', 'waiting_clean_channel')
                      AND campaign_run_id IS NULL
                    RETURNING id
                    """,
                    (
                        next_retry_at,
                        self._clean_channel_wait_reason(reason),
                        _json_dumps(policy_snapshot),
                        int(scheduled_launch_id),
                    ),
                )
                row = cur.fetchone()
            conn.commit()
        return bool(row)

    def mark_campaign_scheduled_launch_scheduled_again(
        self,
        scheduled_launch_id: int,
        *,
        next_retry_at: str | None = None,
        reason: str | None = None,
    ) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE campaign_scheduled_launches
                    SET status = 'scheduled',
                        clean_channel_next_retry_at = %s,
                        clean_channel_last_reason = %s,
                        updated_at = NOW(),
                        locked_at = NULL,
                        locked_by = NULL
                    WHERE id = %s
                      AND status = 'waiting_clean_channel'
                      AND campaign_run_id IS NULL
                    RETURNING id
                    """,
                    (next_retry_at, self._clean_channel_wait_reason(reason) if reason is not None else None, int(scheduled_launch_id)),
                )
                row = cur.fetchone()
            conn.commit()
        return bool(row)

    def cancel_campaign_scheduled_launch(self, scheduled_launch_id: int, *, cancelled_by: int | None = None) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE campaign_scheduled_launches SET status='cancelled', cancelled_at=NOW(), cancelled_by=%s, updated_at=NOW() WHERE id=%s AND status IN ('scheduled','waiting_clean_channel')", (cancelled_by, int(scheduled_launch_id)))
                ok=cur.rowcount>0
            conn.commit(); return ok

    def reset_stuck_campaign_scheduled_launches(self, *, stuck_seconds: int = 300) -> dict[str, int]:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE campaign_scheduled_launches SET status='scheduled', locked_at=NULL, locked_by=NULL, updated_at=NOW() WHERE status='processing' AND campaign_run_id IS NULL AND locked_at < NOW() - (%s * INTERVAL '1 second')", (int(stuck_seconds),))
                requeued=int(cur.rowcount)
                cur.execute("UPDATE campaign_scheduled_launches SET status='needs_review', error_text=COALESCE(error_text, 'Запуск завис после создания campaign_run'), locked_at=NULL, locked_by=NULL, updated_at=NOW() WHERE status='processing' AND campaign_run_id IS NOT NULL AND locked_at < NOW() - (%s * INTERVAL '1 second')", (int(stuck_seconds),))
                needs_review=int(cur.rowcount)
            conn.commit()
        if requeued: logger.info("CAMPAIGN_SCHEDULE_STUCK_RESET count=%s", requeued)
        if needs_review: logger.warning("CAMPAIGN_SCHEDULE_NEEDS_REVIEW | reason=stale_processing_with_campaign_run | count=%s", needs_review)
        return {"requeued": requeued, "needs_review": needs_review}

    def _normalize_campaign_scheduled_post_row(self, row: dict[str, Any] | None) -> dict[str, Any] | None:
        if not row:
            return None
        d = dict(row)
        d["post_snapshot_json"] = _safe_json_loads(d.get("post_snapshot_json"), {})
        d["readiness_snapshot_json"] = _safe_json_loads(d.get("readiness_snapshot_json"), {})
        d["preview_json"] = _safe_json_loads(d.get("preview_json"), {})
        d["metadata_json"] = _safe_json_loads(d.get("metadata_json"), {})
        return d


    def create_campaign_scheduled_post_draft(self, *, rule_id: int, tenant_id: int = 1, created_by: int | None = None, title: str | None = None, metadata: dict[str, Any] | None = None) -> int | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO campaign_scheduled_posts(tenant_id, rule_id, created_by, title, metadata_json) VALUES (%s,%s,%s,%s,%s::jsonb) RETURNING id", (int(tenant_id), int(rule_id), created_by, title, _json_dumps(metadata)))
                row = cur.fetchone()
            conn.commit()
        if row: logger.info("VIP_SCHEDULED_POST_DRAFT_CREATED id=%s rule_id=%s", row['id'], rule_id)
        return int(row['id']) if row else None

    def get_campaign_scheduled_post(self, scheduled_post_id: int) -> dict[str, Any] | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM campaign_scheduled_posts WHERE id=%s", (int(scheduled_post_id),))
                return self._normalize_campaign_scheduled_post_row(cur.fetchone())

    def update_campaign_scheduled_post(self, scheduled_post_id: int, **kwargs) -> bool:
        mapping={"saved_post_id":"saved_post_id=%s","title":"title=%s","show_seconds":"show_seconds=%s","scheduled_at":"scheduled_at=%s","timezone_offset_minutes":"timezone_offset_minutes=%s","timezone_label":"timezone_label=%s","status":"status=%s","targets_total":"targets_total=%s","targets_ready":"targets_ready=%s","targets_with_warnings":"targets_with_warnings=%s","targets_blocked":"targets_blocked=%s","error_text":"error_text=%s","post_snapshot":"post_snapshot_json=%s::jsonb","readiness_snapshot":"readiness_snapshot_json=%s::jsonb","preview":"preview_json=%s::jsonb","metadata":"metadata_json=%s::jsonb"}
        set_parts=[]; params=[]
        for k,sql in mapping.items():
            if k in kwargs and kwargs[k] is not None:
                set_parts.append(sql); params.append(_json_dumps(kwargs[k]) if k in {"post_snapshot","readiness_snapshot","preview","metadata"} else kwargs[k])
        if not set_parts: return False
        set_parts.append("updated_at=NOW()"); params.append(int(scheduled_post_id))
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(f"UPDATE campaign_scheduled_posts SET {', '.join(set_parts)} WHERE id=%s", tuple(params)); ok=cur.rowcount>0
            conn.commit()
        if ok: logger.info("VIP_SCHEDULED_POST_UPDATED id=%s", scheduled_post_id)
        return ok

    def list_campaign_scheduled_posts(self, *, rule_id: int, statuses: list[str] | None = None, limit: int = 20) -> list[dict[str, Any]]:
        with self.connect() as conn:
            with conn.cursor() as cur:
                if statuses: cur.execute("SELECT * FROM campaign_scheduled_posts WHERE rule_id=%s AND status = ANY(%s) ORDER BY scheduled_at DESC NULLS LAST, id DESC LIMIT %s", (int(rule_id), statuses, int(limit)))
                else: cur.execute("SELECT * FROM campaign_scheduled_posts WHERE rule_id=%s ORDER BY scheduled_at DESC NULLS LAST, id DESC LIMIT %s", (int(rule_id), int(limit)))
                return [self._normalize_campaign_scheduled_post_row(r) for r in (cur.fetchall() or [])]

    def list_campaign_scheduled_posts_for_tenant(self, *, tenant_id: int, statuses: list[str] | None = None, limit: int = 50) -> list[dict[str, Any]]:
        with self.connect() as conn:
            with conn.cursor() as cur:
                if statuses: cur.execute("SELECT * FROM campaign_scheduled_posts WHERE tenant_id=%s AND status = ANY(%s) ORDER BY scheduled_at DESC NULLS LAST, id DESC LIMIT %s", (int(tenant_id), statuses, int(limit)))
                else: cur.execute("SELECT * FROM campaign_scheduled_posts WHERE tenant_id=%s ORDER BY scheduled_at DESC NULLS LAST, id DESC LIMIT %s", (int(tenant_id), int(limit)))
                return [self._normalize_campaign_scheduled_post_row(r) for r in (cur.fetchall() or [])]


    def replace_campaign_scheduled_post_targets(self, *, scheduled_post_id: int, rule_id: int, targets: list[dict[str, Any]]) -> int:
        active = 0
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM campaign_scheduled_post_targets WHERE scheduled_post_id=%s", (int(scheduled_post_id),))
                for t in targets:
                    cur.execute("""INSERT INTO campaign_scheduled_post_targets(scheduled_post_id, rule_id, target_kind, target_id, target_thread_id, target_title, is_active, metadata_json)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s::jsonb) ON CONFLICT (scheduled_post_id, target_id, COALESCE(target_thread_id, -1)) DO UPDATE SET target_title=EXCLUDED.target_title, is_active=EXCLUDED.is_active, updated_at=NOW()""",
                    (int(scheduled_post_id), int(rule_id), t.get('target_kind','extra'), str(t.get('target_id')), t.get('target_thread_id'), t.get('target_title'), bool(t.get('is_active', True)), _json_dumps(t.get('metadata'))))
                    if bool(t.get('is_active', True)): active += 1
                cur.execute("UPDATE campaign_scheduled_posts SET targets_total=%s, updated_at=NOW() WHERE id=%s", (int(active), int(scheduled_post_id)))
            conn.commit()
        logger.info("VIP_SCHEDULED_POST_TARGETS_REPLACED id=%s active=%s", scheduled_post_id, active)
        return active

    def list_campaign_scheduled_post_targets(self, scheduled_post_id: int, *, active_only: bool = True) -> list[dict[str, Any]]:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM campaign_scheduled_post_targets WHERE scheduled_post_id=%s" + (" AND is_active=TRUE" if active_only else "") + " ORDER BY id ASC", (int(scheduled_post_id),))
                rows=[dict(r) for r in (cur.fetchall() or [])]
                for r in rows: r['metadata_json']=_safe_json_loads(r.get('metadata_json'), {})
                return rows

    def update_campaign_scheduled_post_target_check_result(
        self,
        target_row_id: int,
        *,
        can_publish: bool | None = None,
        can_delete: bool | None = None,
        publish_status: str | None = None,
        delete_status: str | None = None,
        publish_error_text: str | None = None,
        delete_error_text: str | None = None,
        check_source: str | None = None,
    ) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE campaign_scheduled_post_targets
                    SET can_publish=%s,
                        can_delete=%s,
                        publish_status=%s,
                        delete_status=%s,
                        publish_error_text=%s,
                        delete_error_text=%s,
                        check_source=%s,
                        checked_at=NOW(),
                        updated_at=NOW()
                    WHERE id=%s
                    """,
                    (
                        can_publish,
                        can_delete,
                        publish_status,
                        delete_status,
                        publish_error_text,
                        delete_error_text,
                        check_source,
                        int(target_row_id),
                    ),
                )
                ok = cur.rowcount > 0
            conn.commit()
        return ok
    def log_campaign_scheduled_post_check(self, *, scheduled_post_id: int, rule_id: int, target_id: str, target_thread_id: int | None, check_type: str, status: str, target_row_id: int | None = None, source: str | None = None, error_text: str | None = None, details: dict[str, Any] | None = None) -> int | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO campaign_scheduled_post_checks(scheduled_post_id,target_row_id,rule_id,target_id,target_thread_id,check_type,status,source,error_text,details_json) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb) RETURNING id", (int(scheduled_post_id), target_row_id, int(rule_id), target_id, target_thread_id, check_type, status, source, error_text, _json_dumps(details)))
                row=cur.fetchone()
            conn.commit(); return int(row['id']) if row else None
    def list_campaign_scheduled_post_checks(self, scheduled_post_id: int, *, limit: int = 50) -> list[dict[str, Any]]:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM campaign_scheduled_post_checks WHERE scheduled_post_id=%s ORDER BY checked_at DESC, id DESC LIMIT %s", (int(scheduled_post_id), int(limit)))
                rows=[dict(r) for r in (cur.fetchall() or [])]
                for r in rows: r['details_json']=_safe_json_loads(r.get('details_json'), {})
                return rows

    def log_campaign_scheduled_post_event(self, *, scheduled_post_id: int, rule_id: int, event_type: str, status_from: str | None = None, status_to: str | None = None, actor_id: int | None = None, worker_id: str | None = None, message: str | None = None, error_text: str | None = None, extra: dict[str, Any] | None = None) -> int | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO campaign_scheduled_post_events(scheduled_post_id,rule_id,event_type,status_from,status_to,actor_id,worker_id,message,error_text,extra_json) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb) RETURNING id", (int(scheduled_post_id), int(rule_id), event_type, status_from, status_to, actor_id, worker_id, message, error_text, _json_dumps(extra)))
                row=cur.fetchone()
            conn.commit(); return int(row['id']) if row else None
    def list_campaign_scheduled_post_events(self, scheduled_post_id: int, *, limit: int = 50) -> list[dict[str, Any]]:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM campaign_scheduled_post_events WHERE scheduled_post_id=%s ORDER BY created_at DESC, id DESC LIMIT %s", (int(scheduled_post_id), int(limit)))
                rows=[dict(r) for r in (cur.fetchall() or [])]
                for r in rows: r['extra_json']=_safe_json_loads(r.get('extra_json'), {})
                return rows

    def schedule_campaign_scheduled_post(self, scheduled_post_id: int, *, scheduled_by: int | None = None) -> bool:
        return self._update_campaign_scheduled_post_status(scheduled_post_id, "status='scheduled', scheduled_by=%s, scheduled_at_created=NOW(), updated_at=NOW()", (scheduled_by,), "status IN ('draft','ready')", "VIP_SCHEDULED_POST_SCHEDULED")
    def cancel_campaign_scheduled_post(self, scheduled_post_id: int, *, cancelled_by: int | None = None, reason: str | None = None) -> bool:
        return self._update_campaign_scheduled_post_status(scheduled_post_id, "status='cancelled', cancelled_at=NOW(), cancelled_by=%s, cancel_reason=%s, updated_at=NOW()", (cancelled_by, reason), "status IN ('draft','ready','scheduled')", "VIP_SCHEDULED_POST_CANCELLED")
    def mark_campaign_scheduled_post_launched(self, scheduled_post_id: int, *, campaign_run_id: int) -> bool:
        return self._update_campaign_scheduled_post_status(scheduled_post_id, "status='launched', campaign_run_id=%s, launched_at=NOW(), updated_at=NOW()", (int(campaign_run_id),), "status='processing'", "VIP_SCHEDULED_POST_MARKED_LAUNCHED")
    def mark_campaign_scheduled_post_failed(self, scheduled_post_id: int, *, error_text: str, campaign_run_id: int | None = None) -> bool:
        return self._update_campaign_scheduled_post_status(scheduled_post_id, "status='failed', failed_at=NOW(), error_text=%s, campaign_run_id=COALESCE(%s, campaign_run_id), locked_by=NULL, locked_at=NULL, lock_until=NULL, updated_at=NOW()", (error_text, campaign_run_id), "status IN ('scheduled','processing')", "VIP_SCHEDULED_POST_MARKED_FAILED")
    def mark_campaign_scheduled_post_needs_review(self, scheduled_post_id: int, *, error_text: str, campaign_run_id: int | None = None) -> bool:
        return self._update_campaign_scheduled_post_status(scheduled_post_id, "status='needs_review', failed_at=NOW(), error_text=%s, campaign_run_id=COALESCE(%s, campaign_run_id), locked_by=NULL, locked_at=NULL, lock_until=NULL, updated_at=NOW()", (error_text, campaign_run_id), "status='processing'", "VIP_SCHEDULED_POST_NEEDS_REVIEW")
    def set_campaign_scheduled_post_campaign_run_id(self, scheduled_post_id: int, campaign_run_id: int) -> bool:
        return self._update_campaign_scheduled_post_status(scheduled_post_id, "campaign_run_id=%s, updated_at=NOW()", (int(campaign_run_id),), "status='processing' AND campaign_run_id IS NULL", "VIP_SCHEDULED_POST_RUN_ID_SET")
    def mark_campaign_scheduled_post_processing(self, scheduled_post_id: int, *, actor_id: int | None = None) -> bool:
        lock_owner = f"send-now:{actor_id}" if actor_id is not None else "send-now"
        return self._update_campaign_scheduled_post_status(
            scheduled_post_id,
            "status='processing', locked_by=%s, locked_at=NOW(), lock_until=NOW() + INTERVAL '5 minutes', updated_at=NOW()",
            (lock_owner,),
            "status IN ('draft','ready','scheduled') AND campaign_run_id IS NULL",
            "VIP_SCHEDULED_POST_MARKED_PROCESSING",
        )
    def reset_campaign_scheduled_post_after_send_now_failure(self, scheduled_post_id: int, *, status: str, error_text: str) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE campaign_scheduled_posts
                    SET status=%s,
                        error_text=%s,
                        locked_by=NULL,
                        locked_at=NULL,
                        lock_until=NULL,
                        updated_at=NOW()
                    WHERE id=%s
                      AND status='processing'
                      AND campaign_run_id IS NULL
                    """,
                    (status, error_text, int(scheduled_post_id)),
                )
                ok = cur.rowcount > 0
            conn.commit()
        if ok:
            logger.info("VIP_SCHEDULED_POST_RESET_AFTER_SEND_NOW_FAILURE id=%s", scheduled_post_id)
        return ok
    def delay_campaign_scheduled_post_retry(self, scheduled_post_id: int, *, next_retry_at: str, error_text: str | None = None) -> bool:
        return self._update_campaign_scheduled_post_status(scheduled_post_id, "status='scheduled', attempt_count=attempt_count+1, next_retry_at=%s, error_text=%s, locked_by=NULL, locked_at=NULL, lock_until=NULL, updated_at=NOW()", (next_retry_at, error_text), "status='processing'", None)
    def _update_campaign_scheduled_post_status(self, scheduled_post_id: int, set_sql: str, params: tuple[Any, ...], where_sql: str, log_code: str | None) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(f"UPDATE campaign_scheduled_posts SET {set_sql} WHERE id=%s AND {where_sql}", (*params, int(scheduled_post_id))); ok=cur.rowcount>0
            conn.commit()
        if ok and log_code: logger.info("%s id=%s", log_code, scheduled_post_id)
        return ok

    def delay_campaign_scheduled_post_until(self, scheduled_post_id: int, *, next_retry_at: str, error_text: str | None = None) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE campaign_scheduled_posts
                    SET status='scheduled',
                        next_retry_at=%s,
                        error_text=%s,
                        locked_by=NULL,
                        locked_at=NULL,
                        lock_until=NULL,
                        updated_at=NOW()
                    WHERE id=%s
                    """,
                    (next_retry_at, error_text, int(scheduled_post_id)),
                )
                ok = cur.rowcount > 0
            conn.commit()
        return ok

    def claim_due_campaign_scheduled_posts(self, *, now_iso: str, worker_id: str, limit: int = 5) -> list[dict[str, Any]]:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("""WITH picked AS (SELECT id FROM campaign_scheduled_posts WHERE status='scheduled' AND campaign_run_id IS NULL AND scheduled_at IS NOT NULL AND COALESCE(next_retry_at, scheduled_at) <= %s ORDER BY COALESCE(next_retry_at, scheduled_at) ASC, scheduled_at ASC, id ASC FOR UPDATE SKIP LOCKED LIMIT %s) UPDATE campaign_scheduled_posts p SET status='processing', locked_by=%s, locked_at=NOW(), lock_until=NOW() + INTERVAL '5 minutes', updated_at=NOW() FROM picked WHERE p.id = picked.id RETURNING p.*""", (now_iso, int(limit), worker_id))
                rows=[self._normalize_campaign_scheduled_post_row(r) for r in (cur.fetchall() or [])]
            conn.commit()
        if rows: logger.info("VIP_SCHEDULED_POST_CLAIMED count=%s worker=%s", len(rows), worker_id)
        return rows
    def reset_stuck_campaign_scheduled_posts(self, *, stuck_seconds: int = 300) -> dict[str, int]:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE campaign_scheduled_posts SET status='scheduled', locked_by=NULL, locked_at=NULL, lock_until=NULL, updated_at=NOW() WHERE status='processing' AND campaign_run_id IS NULL AND (lock_until < NOW() OR locked_at < NOW() - (%s * INTERVAL '1 second'))", (int(stuck_seconds),)); requeued=int(cur.rowcount)
                cur.execute("UPDATE campaign_scheduled_posts SET status='needs_review', error_text=COALESCE(error_text, 'Запуск завис после создания campaign_run'), locked_by=NULL, locked_at=NULL, lock_until=NULL, updated_at=NOW() WHERE status='processing' AND campaign_run_id IS NOT NULL AND (lock_until < NOW() OR locked_at < NOW() - (%s * INTERVAL '1 second'))", (int(stuck_seconds),)); needs_review=int(cur.rowcount)
            conn.commit()
        if requeued: logger.info("VIP_SCHEDULED_POST_STUCK_RESET count=%s", requeued)
        if needs_review: logger.warning("VIP_SCHEDULED_POST_NEEDS_REVIEW | reason=stale_processing_with_campaign_run | count=%s", needs_review)
        return {"requeued": requeued, "needs_review": needs_review}

    def _normalize_repost_campaign_launch_job_row(self, row) -> dict[str, Any] | None:
        if not row:
            return None
        item = dict(row)
        item["payload_json"] = _safe_json_loads(item.get("payload_json"), {})
        item["result_json"] = _safe_json_loads(item.get("result_json"), None)
        return item

    def create_repost_campaign_launch_job(
        self,
        *,
        rule_id: int,
        admin_id: int | None,
        progress_chat_id: int | str | None,
        progress_message_id: int | None,
        payload_json: dict[str, Any] | None = None,
        max_attempts: int = 3,
    ) -> dict[str, Any]:
        existing = self.get_active_repost_campaign_launch_job_for_rule(rule_id)
        if existing:
            return existing
        payload = dict(payload_json or {})
        with self.connect() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(
                        """
                        INSERT INTO repost_campaign_launch_jobs(
                            rule_id, admin_id, status, payload_json, progress_chat_id,
                            progress_message_id, max_attempts, created_at, updated_at
                        )
                        VALUES(%s, %s, 'pending', %s::jsonb, %s, %s, %s, NOW(), NOW())
                        RETURNING *
                        """,
                        (
                            int(rule_id),
                            admin_id,
                            _json_dumps(payload),
                            str(progress_chat_id) if progress_chat_id is not None else None,
                            progress_message_id,
                            max(1, int(max_attempts)),
                        ),
                    )
                    row = cur.fetchone()
                    conn.commit()
                    return self._normalize_repost_campaign_launch_job_row(row) or {}
                except Exception as exc:
                    if "duplicate key value violates unique constraint" not in str(exc).lower():
                        raise
                    conn.rollback()
        existing = self.get_active_repost_campaign_launch_job_for_rule(rule_id)
        if existing:
            return existing
        raise RuntimeError("Не удалось создать durable job запуска рекламной кампании")

    def get_active_repost_campaign_launch_job_for_rule(self, rule_id: int) -> dict[str, Any] | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM repost_campaign_launch_jobs
                    WHERE rule_id=%s AND status IN ('pending','processing')
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (int(rule_id),),
                )
                row = cur.fetchone()
            conn.commit()
        return self._normalize_repost_campaign_launch_job_row(row)

    def get_repost_campaign_launch_job(self, job_id: int) -> dict[str, Any] | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM repost_campaign_launch_jobs WHERE id=%s", (int(job_id),))
                row = cur.fetchone()
            conn.commit()
        return self._normalize_repost_campaign_launch_job_row(row)

    def get_due_repost_campaign_launch_jobs(self, limit: int = 5) -> list[dict[str, Any]]:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM repost_campaign_launch_jobs
                    WHERE status='pending'
                    ORDER BY created_at ASC, id ASC
                    LIMIT %s
                    """,
                    (max(1, int(limit)),),
                )
                rows = cur.fetchall() or []
            conn.commit()
        return [self._normalize_repost_campaign_launch_job_row(r) or {} for r in rows]

    def lease_repost_campaign_launch_job(self, job_id: int, worker_id: str, lock_ttl_seconds: int) -> dict[str, Any] | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE repost_campaign_launch_jobs
                    SET status='processing',
                        locked_by=%s,
                        locked_until=NOW() + make_interval(secs => %s),
                        attempts=attempts + 1,
                        started_at=COALESCE(started_at, NOW()),
                        updated_at=NOW()
                    WHERE id=%s
                      AND status='pending'
                    RETURNING *
                    """,
                    (worker_id, max(1, int(lock_ttl_seconds)), int(job_id)),
                )
                row = cur.fetchone()
            conn.commit()
        return self._normalize_repost_campaign_launch_job_row(row)

    def mark_repost_campaign_launch_job_processing(self, job_id: int, *, worker_id: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE repost_campaign_launch_jobs
                    SET status='processing', locked_by=%s, started_at=COALESCE(started_at, NOW()), updated_at=NOW()
                    WHERE id=%s AND status='processing'
                    RETURNING *
                    """,
                    (worker_id, int(job_id)),
                )
                row = cur.fetchone()
            conn.commit()
        return self._normalize_repost_campaign_launch_job_row(row)

    def set_repost_campaign_launch_job_campaign_run_id(self, job_id: int, campaign_run_id: int) -> dict[str, Any] | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE repost_campaign_launch_jobs
                    SET campaign_run_id=%s, updated_at=NOW()
                    WHERE id=%s AND status='processing'
                    RETURNING *
                    """,
                    (int(campaign_run_id), int(job_id)),
                )
                row = cur.fetchone()
            conn.commit()
        return self._normalize_repost_campaign_launch_job_row(row)

    def mark_repost_campaign_launch_job_sent(self, job_id: int, *, campaign_run_id: int | None, result_json: dict[str, Any] | None) -> dict[str, Any] | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE repost_campaign_launch_jobs
                    SET status='sent', campaign_run_id=%s, result_json=%s::jsonb, locked_by=NULL, locked_until=NULL, updated_at=NOW(), finished_at=NOW()
                    WHERE id=%s AND status='processing'
                    RETURNING *
                    """,
                    (campaign_run_id, _json_dumps(result_json or {}), int(job_id)),
                )
                row = cur.fetchone()
            conn.commit()
        return self._normalize_repost_campaign_launch_job_row(row)

    def mark_repost_campaign_launch_job_failed(self, job_id: int, *, last_error: str, result_json: dict[str, Any] | None = None, retryable: bool = False) -> dict[str, Any] | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE repost_campaign_launch_jobs
                    SET status = CASE WHEN %s AND campaign_run_id IS NULL AND attempts < max_attempts THEN 'pending' ELSE 'failed' END,
                        last_error=%s,
                        result_json=COALESCE(%s::jsonb, result_json),
                        locked_by=NULL,
                        locked_until=NULL,
                        updated_at=NOW(),
                        finished_at=CASE WHEN %s AND campaign_run_id IS NULL AND attempts < max_attempts THEN finished_at ELSE NOW() END
                    WHERE id=%s AND status='processing'
                    RETURNING *
                    """,
                    (bool(retryable), last_error, _json_dumps(result_json) if result_json is not None else None, bool(retryable), int(job_id)),
                )
                row = cur.fetchone()
            conn.commit()
        return self._normalize_repost_campaign_launch_job_row(row)

    def mark_repost_campaign_launch_job_needs_review(self, job_id: int, *, last_error: str, campaign_run_id: int | None = None, result_json: dict[str, Any] | None = None) -> dict[str, Any] | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE repost_campaign_launch_jobs
                    SET status='needs_review',
                        last_error=%s,
                        campaign_run_id=COALESCE(%s, campaign_run_id),
                        result_json=COALESCE(%s::jsonb, result_json),
                        locked_by=NULL,
                        locked_until=NULL,
                        updated_at=NOW(),
                        finished_at=NOW()
                    WHERE id=%s AND status IN ('pending','processing')
                    RETURNING *
                    """,
                    (last_error, campaign_run_id, _json_dumps(result_json) if result_json is not None else None, int(job_id)),
                )
                row = cur.fetchone()
            conn.commit()
        return self._normalize_repost_campaign_launch_job_row(row)

    def mark_repost_campaign_launch_job_cancelled(self, job_id: int, *, reason: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE repost_campaign_launch_jobs
                    SET status='cancelled', last_error=%s, locked_by=NULL, locked_until=NULL, updated_at=NOW(), finished_at=NOW()
                    WHERE id=%s AND status IN ('pending','processing')
                    RETURNING *
                    """,
                    (reason, int(job_id)),
                )
                row = cur.fetchone()
            conn.commit()
        return self._normalize_repost_campaign_launch_job_row(row)

    def recover_stale_repost_campaign_launch_jobs(self, *, worker_id: str | None = None) -> dict[str, int]:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE repost_campaign_launch_jobs
                    SET status='needs_review',
                        last_error='Состояние запуска неизвестно после рестарта',
                        locked_by=NULL,
                        locked_until=NULL,
                        updated_at=NOW(),
                        finished_at=NOW()
                    WHERE status='processing'
                      AND locked_until IS NOT NULL
                      AND locked_until < NOW()
                      AND campaign_run_id IS NOT NULL
                    """
                )
                needs_review = int(cur.rowcount or 0)
                cur.execute(
                    """
                    UPDATE repost_campaign_launch_jobs
                    SET status='failed',
                        last_error=COALESCE(last_error, 'Превышено число попыток запуска кампании'),
                        locked_by=NULL,
                        locked_until=NULL,
                        updated_at=NOW(),
                        finished_at=NOW()
                    WHERE status='processing'
                      AND locked_until IS NOT NULL
                      AND locked_until < NOW()
                      AND campaign_run_id IS NULL
                      AND attempts >= max_attempts
                    """
                )
                failed = int(cur.rowcount or 0)
                cur.execute(
                    """
                    UPDATE repost_campaign_launch_jobs
                    SET status='pending',
                        locked_by=NULL,
                        locked_until=NULL,
                        updated_at=NOW()
                    WHERE status='processing'
                      AND locked_until IS NOT NULL
                      AND locked_until < NOW()
                      AND campaign_run_id IS NULL
                      AND attempts < max_attempts
                    """
                )
                requeued = int(cur.rowcount or 0)
            conn.commit()
        return {"requeued": requeued, "needs_review": needs_review, "failed": failed}

    def update_campaign_run_status(self, run_id: int, *, status: str, render_mode: str | None = None, targets_success: int | None = None, targets_failed: int | None = None, error_text: str | None = None, report: dict[str, Any] | None = None, finish: bool = False) -> bool:
        set_parts = ["status=%s", "updated_at=NOW()"]
        params: list[Any] = [str(status)]
        if render_mode is not None:
            set_parts.append("render_mode=%s")
            params.append(render_mode)
        if targets_success is not None:
            set_parts.append("targets_success=%s")
            params.append(int(targets_success))
        if targets_failed is not None:
            set_parts.append("targets_failed=%s")
            params.append(int(targets_failed))
        if error_text is not None:
            set_parts.append("error_text=%s")
            params.append(error_text)
        if report is not None:
            set_parts.append("report_json=%s::jsonb")
            params.append(_json_dumps(report))
        if finish:
            set_parts.append("finished_at=NOW()")
        params.append(int(run_id))
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(f"UPDATE campaign_runs SET {', '.join(set_parts)} WHERE id=%s", tuple(params))
            conn.commit()
            return True

    def create_campaign_run_message(self, *, run_id: int, rule_id: int, saved_post_id: int, target_kind: str, target_id: str, target_thread_id: int | None = None, target_title: str | None = None, show_seconds: int = 0, delete_after_at: str | None = None) -> int | None:
        delete_status = "pending" if delete_after_at else "none"
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO campaign_run_messages(run_id, rule_id, saved_post_id, target_kind, target_id, target_thread_id, target_title, show_seconds, delete_after_at, delete_status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                    RETURNING id
                    """,
                    (int(run_id), int(rule_id), int(saved_post_id), str(target_kind), str(target_id), target_thread_id, target_title, int(show_seconds), delete_after_at, delete_status),
                )
                row = cur.fetchone()
            conn.commit()
            return int(row["id"]) if row else None

    def mark_campaign_run_message_failed_before_send(self, message_id: int, *, error_text: str) -> bool:
        """Record a definite pre-RPC rejection without touching claimed sends."""
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE campaign_run_messages
                    SET send_status='failed',
                        render_mode='target_resolution_failed',
                        send_error_text=%s,
                        delete_after_at=NULL,
                        delete_status='none',
                        delete_error_text=NULL,
                        updated_at=NOW()
                    WHERE id=%s AND send_status='pending'
                      AND sent_message_id IS NULL
                      AND publication_receipt IS NULL
                      AND NOT publication_review_required
                    """, (error_text, int(message_id)))
                return cur.rowcount == 1

    def mark_campaign_run_message_sending(self, message_id: int, *, render_mode: str | None = None,
                                         canonical_target_id: int | None = None) -> bool:
        """Claim exactly once; unresolved placements fence subsequent launches."""
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT rule_id,saved_post_id,target_id,target_thread_id FROM campaign_run_messages WHERE id=%s", (int(message_id),))
                row = cur.fetchone()
                if not row:
                    return False
                # Canonicalize aliases before the first write; subsequent delete and
                # receipt reconciliation must not resolve a mutable username again.
                original_target = row["target_id"]
                target = str(canonical_target_id) if canonical_target_id is not None else original_target
                if canonical_target_id is not None and (type(canonical_target_id) is not int or canonical_target_id == 0):
                    return False
                lock_key = f"campaign:{row['rule_id']}:{row['saved_post_id']}:{target}:{row['target_thread_id']}"
                cur.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", (lock_key,))
                cur.execute("""
                    UPDATE campaign_run_messages m SET send_status='sending',render_mode=%s,target_id=%s,updated_at=NOW()
                    WHERE m.id=%s AND m.send_status='pending' AND m.target_id=%s
                      AND NOT EXISTS (
                        SELECT 1 FROM campaign_run_messages same_run
                        WHERE same_run.run_id=m.run_id AND same_run.id<>m.id
                          AND same_run.target_id=%s
                          AND same_run.target_thread_id IS NOT DISTINCT FROM m.target_thread_id
                      )
                      AND NOT EXISTS (
                        SELECT 1 FROM campaign_run_messages p
                        WHERE p.id<>m.id AND p.rule_id=m.rule_id AND p.saved_post_id=m.saved_post_id
                          AND p.target_id IN (%s,%s) AND p.target_thread_id IS NOT DISTINCT FROM m.target_thread_id
                          AND (p.send_status='sending' OR p.publication_review_required
                            OR (p.send_status='failed' AND p.render_mode LIKE '%%_unverified'
                                AND p.publication_manual_review IS NULL)
                            OR (p.send_status='sent' AND p.sent_at >=
                                (SELECT started_at FROM campaign_runs WHERE id=m.run_id)))
                      )
                    """, (render_mode,target,int(message_id),original_target,target,target,original_target))
                if cur.rowcount == 1:
                    return True
                cur.execute("""
                    UPDATE campaign_run_messages SET send_status='failed',delete_status='none',
                        send_error_text='Предыдущая публикация не подтверждена или уже обрабатывается',updated_at=NOW()
                    WHERE id=%s AND send_status='pending'
                    """, (int(message_id),))
                return False

    def acknowledge_interrupted_campaign_send(
        self, message_id: int, *, expected_updated_at: datetime, operator: str, note: str,
    ) -> bool:
        """Release only an inspected abandoned placement, never replay its RPC."""
        operator, note = operator.strip(), note.strip()
        if not operator or len(operator)>128 or not note or len(note)>1000:
            raise ValueError("Operator and review note are required")
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT rule_id,saved_post_id,target_id,target_thread_id FROM campaign_run_messages WHERE id=%s",(int(message_id),))
                row=cur.fetchone()
                if not row:
                    return False
                cur.execute("SELECT is_active,rescan_owner_token FROM routing WHERE id=%s FOR UPDATE",(row["rule_id"],))
                rule=cur.fetchone()
                if not rule or rule["is_active"] or rule["rescan_owner_token"] is not None:
                    return False
                lock_key=f"campaign:{row['rule_id']}:{row['saved_post_id']}:{row['target_id']}:{row['target_thread_id']}"
                cur.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",(lock_key,))
                cur.execute("""
                    UPDATE campaign_run_messages m SET send_status='failed',delete_status='none',
                        publication_review_required=FALSE,
                        publication_manual_review=jsonb_build_object('operator',%s::text,'note',%s::text,'reviewed_at',NOW()),
                        send_error_text='Прерванная публикация проверена вручную; повторная отправка запрещена',
                        delete_error_text=NULL,updated_at=NOW()
                    WHERE m.id=%s AND m.updated_at=%s AND m.updated_at<NOW()-INTERVAL '15 minutes'
                      AND (m.send_status='sending' OR m.publication_review_required OR
                           (m.send_status='failed' AND m.render_mode LIKE '%%_unverified'))
                      AND m.publication_manual_review IS NULL
                      AND m.sent_message_id IS NULL
                      AND COALESCE(m.sent_message_ids_json,'[]'::jsonb) IN ('[]'::jsonb,'null'::jsonb)
                      AND (m.publication_receipt IS NULL OR
                           (m.publication_receipt->>'started_at')::timestamptz<=NOW()-INTERVAL '24 hours')
                      AND NOT EXISTS(SELECT 1 FROM repost_campaign_launch_jobs j
                                     WHERE j.rule_id=m.rule_id AND j.status IN ('pending','processing'))
                      AND NOT EXISTS(SELECT 1 FROM campaign_scheduled_posts j WHERE j.rule_id=m.rule_id AND j.status='processing')
                      AND NOT EXISTS(SELECT 1 FROM campaign_scheduled_launches j WHERE j.rule_id=m.rule_id AND j.status='processing')
                    """,(operator,note,int(message_id),expected_updated_at))
                return cur.rowcount==1

    def get_or_create_campaign_mtproto_random_ids(self, key: str, ordinal: int,
                                                   fingerprint: str, count: int) -> list[int]:
        """Same identity contract as deliveries, without changing their FK/schema."""
        import secrets
        if not 1 <= count <= 10 or ordinal < 0:
            raise ValueError("Invalid campaign send identity")
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM campaign_run_messages WHERE id=%s AND send_status='sending' FOR UPDATE", (int(key),))
                if not cur.fetchone():
                    raise RuntimeError("No claimed campaign publication")
                ids = set()
                while len(ids) < count:
                    ids.add(secrets.randbits(63) or 1)
                cur.execute("""
                    INSERT INTO campaign_mtproto_send_intents(message_id,ordinal,fingerprint,random_ids)
                    VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING
                    """, (int(key),ordinal,fingerprint,sorted(ids)))
                cur.execute("SELECT fingerprint,random_ids FROM campaign_mtproto_send_intents WHERE message_id=%s AND ordinal=%s", (int(key),ordinal))
                stored = cur.fetchone()
                if stored["fingerprint"] != fingerprint or len(stored["random_ids"]) != count:
                    raise ValueError("Campaign publication identity mismatch")
                return list(stored["random_ids"])

    def record_campaign_publication_receipt(self, message_id: int, receipt: dict[str, Any]) -> bool:
        from app.telegram_publication import publication_candidate_mask
        if not publication_candidate_mask(receipt["ids"],receipt["mask"]):
            return False
        if type(receipt["account_id"]) is not int or receipt["account_id"]<=0:
            return False
        if type(receipt["peer_id"]) is not int or receipt["peer_id"]==0:
            return False
        started = datetime.fromisoformat(receipt["started_at"])
        if started.tzinfo is None:
            return False
        pending_ids = [mid for mid,flag in zip(receipt["ids"],receipt["mask"]) if flag]
        account,peer = receipt["account_id"],receipt["peer_id"]
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                            (f"publication:{account}:{peer}",))
                cur.execute("""
                    SELECT 1 FROM telegram_publication_claims
                    WHERE sender_account_id=%s AND target_peer_id=%s
                      AND scheduled_id=ANY(%s::bigint[]) LIMIT 1
                    """,(account,peer,pending_ids))
                if cur.fetchone():
                    return False
                cur.execute("""
                    UPDATE campaign_run_messages SET publication_receipt=%s::jsonb,
                        publication_review_required=TRUE, updated_at=NOW()
                    WHERE id=%s AND send_status='sending' AND publication_receipt IS NULL
                      AND target_id=%s AND target_thread_id IS NOT DISTINCT FROM %s
                    """, (_json_dumps(receipt),int(message_id),str(receipt["peer_id"]),receipt["thread_id"]))
                if cur.rowcount != 1:
                    return False
                cur.execute("""
                    INSERT INTO campaign_publication_claims
                    (sender_account_id,target_peer_id,scheduled_id,campaign_message_id)
                    SELECT %s,%s,scheduled_id,%s
                    FROM unnest(%s::bigint[]) AS member(scheduled_id)
                    ON CONFLICT DO NOTHING
                    """,(account,peer,int(message_id),pending_ids))
                if cur.rowcount != len(pending_ids):
                    conn.rollback()
                    return False
                return True

    def list_pending_campaign_publications(self, *, account_id: int, limit: int = 30) -> list[dict[str, Any]]:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT m.* FROM campaign_run_messages m JOIN campaign_runs r ON r.id=m.run_id
                    WHERE m.publication_review_required AND m.publication_receipt IS NOT NULL
                      AND m.send_status IN ('sending','failed')
                      AND (r.status<>'sending' OR m.updated_at < NOW()-INTERVAL '15 minutes')
                      AND (m.publication_receipt->>'started_at')::timestamptz >= NOW()-INTERVAL '24 hours'
                      AND m.publication_receipt->>'account_id'=%s
                      AND (m.publication_next_check_at IS NULL OR m.publication_next_check_at<=NOW())
                    ORDER BY m.publication_next_check_at NULLS FIRST,m.id LIMIT %s
                    """, (str(account_id),max(1,min(int(limit),100))))
                return [dict(row) for row in cur.fetchall()]

    def defer_campaign_publication_check(self, message_id: int) -> None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE campaign_run_messages SET publication_next_check_at=NOW()+INTERVAL '30 seconds'
                    WHERE id=%s AND publication_review_required
                    """, (int(message_id),))

    def confirm_campaign_publication(self, message_id: int, *, receipt: dict[str, Any],
                                     published_ids: list[int], published_at: datetime) -> bool:
        from app.telegram_publication import publication_pairs, publication_candidate_mask
        if (not publication_pairs(published_ids,published_ids) or len(published_ids)!=len(receipt["ids"])
                or not publication_candidate_mask(receipt["ids"],receipt["mask"])):
            return False
        started_at = datetime.fromisoformat(receipt["started_at"])
        if published_at.tzinfo is None or published_at < started_at.replace(microsecond=0):
            return False
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                            (f'publication:{receipt["account_id"]}:{receipt["peer_id"]}',))
                for old,new,pending in zip(receipt["ids"],published_ids,receipt["mask"]):
                    if not pending:
                        if old != new:
                            return False
                        continue
                    cur.execute("""
                        SELECT 1 FROM telegram_publication_mappings m
                        JOIN campaign_publication_claims c USING(sender_account_id,target_peer_id,scheduled_id)
                        WHERE m.sender_account_id=%s AND m.target_peer_id=%s
                          AND m.scheduled_id=%s AND m.published_id=%s
                          AND c.campaign_message_id=%s
                          AND NOT EXISTS (
                            SELECT 1 FROM telegram_publication_mappings x
                            WHERE x.sender_account_id=m.sender_account_id AND x.target_peer_id=m.target_peer_id
                              AND ((x.scheduled_id=m.scheduled_id AND x.published_id<>m.published_id)
                                OR (x.published_id=m.published_id AND x.scheduled_id<>m.scheduled_id))
                          )
                        """, (receipt["account_id"],receipt["peer_id"],old,new,int(message_id)))
                    if not cur.fetchone():
                        return False
                cur.execute("""
                    UPDATE campaign_run_messages SET send_status='sent',sent_message_id=%s,
                        sent_message_ids_json=%s::jsonb,sent_at=%s,render_mode=%s,
                        publication_review_required=FALSE,send_error_text=NULL,delete_error_text=NULL,
                        delete_after_at=CASE WHEN delete_after_at IS NOT NULL
                            THEN %s::timestamptz+make_interval(secs=>show_seconds::double precision) ELSE NULL END,
                        delete_status=CASE WHEN delete_after_at IS NOT NULL THEN 'pending' ELSE 'none' END,
                        updated_at=NOW()
                    WHERE id=%s AND publication_receipt=%s::jsonb AND publication_review_required
                        AND send_status IN ('sending','failed')
                        AND target_id=%s AND target_thread_id IS NOT DISTINCT FROM %s
                        AND clock_timestamp() < (publication_receipt->>'started_at')::timestamptz+INTERVAL '24 hours'
                    RETURNING *
                    """, (published_ids[0],_json_dumps(published_ids),published_at,receipt["method"],
                          published_at,int(message_id),_json_dumps(receipt),str(receipt["peer_id"]),receipt["thread_id"]))
                row = cur.fetchone()
                if not row:
                    return False
                self._persist_campaign_top_time_conn(cur,row["id"])
                cur.execute("""
                    UPDATE campaign_runs r SET
                      targets_success=(SELECT COUNT(*) FROM campaign_run_messages WHERE run_id=r.id AND send_status='sent'),
                      targets_failed=(SELECT COUNT(*) FROM campaign_run_messages WHERE run_id=r.id AND send_status='failed'),
                      status=CASE WHEN NOT EXISTS(SELECT 1 FROM campaign_run_messages WHERE run_id=r.id AND send_status<>'sent')
                                  AND r.targets_total=(SELECT COUNT(*) FROM campaign_run_messages WHERE run_id=r.id)
                                  THEN 'sent' ELSE 'partial' END,updated_at=NOW()
                    WHERE r.id=%s AND (r.status IN ('failed','partial','sent')
                        OR (r.status='sending' AND r.targets_total=(SELECT COUNT(*) FROM campaign_run_messages WHERE run_id=r.id)
                            AND NOT EXISTS(SELECT 1 FROM campaign_run_messages WHERE run_id=r.id AND send_status IN ('pending','sending'))))
                    """, (row["run_id"],))
                return True

    def _persist_campaign_top_time_conn(self, cur, message_id: int) -> None:
        cur.execute("""
            INSERT INTO campaign_top_time_pauses(rule_id,campaign_run_id,campaign_run_message_id,
                target_id,target_thread_id,target_title,starts_at,ends_at,status)
            SELECT m.rule_id,m.run_id,m.id,m.target_id,m.target_thread_id,m.target_title,
                m.sent_at,m.sent_at+make_interval(secs=>r.top_time_seconds_snapshot::double precision),'active'
            FROM campaign_run_messages m JOIN campaign_runs r ON r.id=m.run_id
            WHERE m.id=%s AND m.send_status='sent' AND r.top_time_enabled_snapshot
              AND r.top_time_seconds_snapshot>0 AND m.sent_at IS NOT NULL
            ON CONFLICT(campaign_run_message_id) DO NOTHING
            """, (int(message_id),))

    def mark_campaign_run_message_sent(self, message_id: int, *, sent_message_id: int | None, sent_message_ids: list[int] | None = None, render_mode: str | None = None) -> bool:
        from app.telegram_publication import publication_pairs
        ids = sent_message_ids if sent_message_ids is not None else [sent_message_id]
        if not publication_pairs(ids,ids) or sent_message_id != ids[0]:
            return False
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE campaign_run_messages SET send_status='sent',sent_message_id=%s,
                        sent_message_ids_json=%s::jsonb,render_mode=%s,sent_at=NOW(),
                        delete_after_at=CASE WHEN delete_after_at IS NOT NULL
                            THEN NOW()+make_interval(secs=>show_seconds::double precision) ELSE NULL END,
                        updated_at=NOW()
                    WHERE id=%s AND send_status='sending' AND NOT publication_review_required
                    """, (sent_message_id,_json_dumps(ids),render_mode,int(message_id)))
                if cur.rowcount != 1:
                    return False
                self._persist_campaign_top_time_conn(cur,message_id)
                return True

    def mark_campaign_run_message_failed(
        self,
        message_id: int,
        *,
        error_text: str,
        render_mode: str | None = None,
        delete_status: str | None = "failed",
        delete_error_text: str | None = None,
        accepted_unverified: bool = False,
        raw_message_ids: list[int] | None = None,
    ) -> bool:
        safe_delete_status = str(delete_status or "failed")
        if safe_delete_status not in {"none", "pending", "processing", "deleted", "failed"}:
            safe_delete_status = "failed"
        safe_delete_error_text = delete_error_text
        if delete_status is None and not safe_delete_error_text:
            safe_delete_error_text = "ID публикации не подтверждён. Автоматическое удаление невозможно, требуется ручная проверка."
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE campaign_run_messages
                    SET send_status='failed',
                        send_error_text=%s,
                        render_mode=%s,
                        delete_status=%s,
                        delete_error_text=%s,
                        publication_review_required=publication_review_required OR %s,
                        publication_raw_ids=%s::jsonb,
                        updated_at=NOW()
                    WHERE id=%s AND send_status='sending'
                    """,
                    (error_text, render_mode, safe_delete_status, safe_delete_error_text,
                     accepted_unverified, _json_dumps(raw_message_ids), int(message_id)),
                )
                return cur.rowcount == 1

    def create_campaign_top_time_pause(
        self,
        *,
        rule_id: int,
        campaign_run_id: int,
        campaign_run_message_id: int,
        target_id: str,
        target_thread_id: int | None,
        target_title: str | None,
        starts_at: str,
        ends_at: str,
    ) -> int | None:
        try:
            with self.connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO campaign_top_time_pauses (
                            rule_id, campaign_run_id, campaign_run_message_id, target_id,
                            target_thread_id, target_title, starts_at, ends_at, status
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'active')
                        ON CONFLICT (campaign_run_message_id)
                        DO UPDATE SET campaign_run_message_id = EXCLUDED.campaign_run_message_id
                        RETURNING id
                        """,
                        (
                            int(rule_id), int(campaign_run_id), int(campaign_run_message_id),
                            str(target_id), target_thread_id, target_title, starts_at, ends_at,
                        ),
                    )
                    row = cur.fetchone()
                conn.commit()
                return int(row["id"]) if row else None
        except Exception:
            logger.exception("Failed to create campaign top-time pause")
            return None

    def get_campaign_top_time_pause(self, pause_id: int) -> dict[str, Any] | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM campaign_top_time_pauses WHERE id = %s LIMIT 1", (int(pause_id),))
                row = cur.fetchone()
                return dict(row) if row else None

    def get_campaign_top_time_pause_by_run_message(self, campaign_run_message_id: int) -> dict[str, Any] | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM campaign_top_time_pauses WHERE campaign_run_message_id=%s LIMIT 1", (int(campaign_run_message_id),))
                row = cur.fetchone()
                return dict(row) if row else None

    def list_active_campaign_top_time_pauses_for_rule(self, rule_id: int, *, limit: int = 50) -> list[dict[str, Any]]:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM campaign_top_time_pauses
                    WHERE rule_id = %s
                      AND status = 'active'
                      AND ends_at > NOW()
                    ORDER BY ends_at ASC, id ASC
                    LIMIT %s
                    """,
                    (int(rule_id), int(limit)),
                )
                return [dict(row) for row in (cur.fetchall() or [])]

    def list_campaign_top_time_pauses_for_run(self, campaign_run_id: int, *, limit: int = 100) -> list[dict[str, Any]]:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM campaign_top_time_pauses
                    WHERE campaign_run_id = %s
                    ORDER BY
                        CASE status
                            WHEN 'active' THEN 0
                            WHEN 'completed' THEN 1
                            WHEN 'cancelled' THEN 2
                            ELSE 3
                        END,
                        ends_at DESC,
                        id DESC
                    LIMIT %s
                    """,
                    (int(campaign_run_id), int(limit)),
                )
                return [dict(row) for row in (cur.fetchall() or [])]

    def get_active_campaign_top_time_pause_for_target(
        self,
        *,
        target_id: str,
        target_thread_id: int | None = None,
        at_iso: str | None = None,
    ) -> dict[str, Any] | None:
        at_expr = at_iso or datetime.now(timezone.utc).isoformat()
        with self.connect() as conn:
            with conn.cursor() as cur:
                if target_thread_id is None:
                    cur.execute(
                        """
                        SELECT *
                        FROM campaign_top_time_pauses
                        WHERE target_id = %s
                          AND target_thread_id IS NULL
                          AND status = 'active'
                          AND starts_at <= %s::timestamptz
                          AND ends_at > %s::timestamptz
                        ORDER BY ends_at DESC, id DESC
                        LIMIT 1
                        """,
                        (str(target_id), at_expr, at_expr),
                    )
                else:
                    cur.execute(
                        """
                        SELECT *
                        FROM campaign_top_time_pauses
                        WHERE target_id = %s
                          AND target_thread_id = %s
                          AND status = 'active'
                          AND starts_at <= %s::timestamptz
                          AND ends_at > %s::timestamptz
                        ORDER BY ends_at DESC, id DESC
                        LIMIT 1
                        """,
                        (str(target_id), int(target_thread_id), at_expr, at_expr),
                    )
                row = cur.fetchone()
                return dict(row) if row else None

    def cancel_campaign_top_time_pause(
        self,
        pause_id: int,
        *,
        cancelled_at: str | None = None,
        cancel_reason: str | None = None,
        actor_id: int | None = None,
    ) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE campaign_top_time_pauses
                    SET
                        status = 'cancelled',
                        cancelled_at = COALESCE(%s::timestamptz, NOW()),
                        updated_at = NOW(),
                        cancel_reason = COALESCE(%s, 'manual_admin_cancel')
                    WHERE id = %s
                      AND status = 'active'
                    RETURNING id
                    """,
                    (cancelled_at, cancel_reason, int(pause_id)),
                )
                row = cur.fetchone()
            conn.commit()
            return bool(row)


    def mark_expired_campaign_top_time_pauses_completed(
        self,
        *,
        now_iso: str | None = None,
        limit: int = 500,
    ) -> int:
        safe_limit = max(1, min(int(limit or 500), 5000))
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    WITH due AS (
                        SELECT id
                        FROM campaign_top_time_pauses
                        WHERE status = 'active'
                          AND ends_at <= COALESCE(%s::timestamptz, NOW())
                        ORDER BY ends_at ASC, id ASC
                        LIMIT %s
                    )
                    UPDATE campaign_top_time_pauses p
                    SET status = 'completed',
                        completed_at = COALESCE(%s::timestamptz, NOW()),
                        updated_at = NOW()
                    FROM due
                    WHERE p.id = due.id
                    RETURNING p.id
                    """,
                    (now_iso, safe_limit, now_iso),
                )
                rows = cur.fetchall() or []
            conn.commit()
            return len(rows)

    def get_campaign_run(self, run_id: int) -> dict[str, Any] | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM campaign_runs WHERE id=%s", (int(run_id),))
                row = cur.fetchone()
                return dict(row) if row else None

    def get_campaign_run_message(self, message_id: int) -> dict[str, Any] | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM campaign_run_messages
                    WHERE id = %s
                    LIMIT 1
                    """,
                    (int(message_id),),
                )
                row = cur.fetchone()
                return dict(row) if row else None

    def list_campaign_runs_for_rule(self, rule_id: int, *, limit: int = 20) -> list[dict[str, Any]]:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM campaign_runs WHERE rule_id=%s ORDER BY created_at DESC LIMIT %s", (int(rule_id), int(limit)))
                return [dict(row) for row in (cur.fetchall() or [])]

    def list_campaign_run_messages(self, run_id: int) -> list[dict[str, Any]]:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM campaign_run_messages WHERE run_id=%s ORDER BY id ASC", (int(run_id),))
                return [dict(row) for row in (cur.fetchall() or [])]

    def resolve_campaign_run_delete_failures(
        self,
        *,
        run_id: int,
        rule_id: int,
        actor_id: int | None = None,
        reason: str | None = None,
    ) -> dict[str, int]:
        _ = actor_id, reason
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE campaign_run_messages
                    SET delete_status = 'deleted',
                        deleted_at = NOW(),
                        delete_error_text = '[manual_resolved_orphaned_target] ' || COALESCE(delete_error_text, ''),
                        updated_at = NOW()
                    WHERE run_id = %s
                      AND rule_id = %s
                      AND send_status = 'sent'
                      AND delete_status = 'failed'
                    """,
                    (int(run_id), int(rule_id)),
                )
                resolved = int(cur.rowcount or 0)
                cur.execute(
                    """
                    SELECT
                        COUNT(*) FILTER (WHERE delete_status = 'pending') AS remaining_pending,
                        COUNT(*) FILTER (WHERE delete_status = 'processing') AS remaining_processing,
                        COUNT(*) FILTER (WHERE delete_status = 'failed') AS remaining_failed
                    FROM campaign_run_messages
                    WHERE run_id = %s
                      AND rule_id = %s
                      AND send_status = 'sent'
                    """,
                    (int(run_id), int(rule_id)),
                )
                row = dict(cur.fetchone() or {})
            conn.commit()
        return {
            "resolved": resolved,
            "remaining_pending": int(row.get("remaining_pending") or 0),
            "remaining_processing": int(row.get("remaining_processing") or 0),
            "remaining_failed": int(row.get("remaining_failed") or 0),
        }

    def list_active_campaign_placements_for_rule(
        self,
        rule_id: int,
        *,
        limit: int = 20,
        basic_only: bool = True,
    ) -> list[dict[str, Any]]:
        where_parts = [
            "r.rule_id = %s",
            "m.send_status = 'sent'",
            "m.delete_status IN ('pending', 'processing', 'failed')",
        ]
        params: list[Any] = [int(rule_id)]
        if basic_only:
            where_parts.append("r.scheduled_post_id IS NULL")
            where_parts.append("r.run_type IN ('manual', 'scheduled')")
        params.append(int(limit))

        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                        r.id AS run_id,
                        r.rule_id,
                        r.saved_post_id,
                        r.run_type,
                        r.status AS run_status,
                        r.scheduled_post_id,
                        r.created_at,
                        r.started_at,
                        r.finished_at,
                        r.targets_total,
                        r.targets_success,
                        r.targets_failed,
                        COUNT(m.id) FILTER (WHERE m.send_status = 'sent') AS sent,
                        COUNT(m.id) FILTER (WHERE m.delete_status = 'pending') AS delete_pending,
                        COUNT(m.id) FILTER (WHERE m.delete_status = 'processing') AS delete_processing,
                        COUNT(m.id) FILTER (WHERE m.delete_status = 'failed') AS delete_failed,
                        MIN(m.delete_after_at) AS delete_after_at_min,
                        MAX(m.delete_after_at) AS delete_after_at_max,
                        MAX(m.sent_at) AS last_sent_at
                    FROM campaign_runs r
                    JOIN campaign_run_messages m ON m.run_id = r.id
                    WHERE {' AND '.join(where_parts)}
                    GROUP BY r.id
                    ORDER BY
                        CASE
                            WHEN COUNT(m.id) FILTER (WHERE m.delete_status = 'failed') > 0 THEN 0
                            ELSE 1
                        END ASC,
                        MIN(m.delete_after_at) ASC NULLS LAST,
                        r.id DESC
                    LIMIT %s
                    """,
                    tuple(params),
                )
                rows = [dict(row) for row in (cur.fetchall() or [])]

        for row in rows:
            delete_pending = int(row.get("delete_pending") or 0)
            delete_processing = int(row.get("delete_processing") or 0)
            delete_failed = int(row.get("delete_failed") or 0)
            active_count = delete_pending + delete_processing
            if delete_failed > 0 and active_count > 0:
                placement_status = "mixed"
            elif delete_failed > 0:
                placement_status = "delete_problem"
            elif active_count > 0:
                placement_status = "active"
            else:
                placement_status = "clean"
            row["sent"] = int(row.get("sent") or 0)
            row["active_messages_total"] = row["sent"]
            row["delete_pending"] = delete_pending
            row["delete_processing"] = delete_processing
            row["delete_failed"] = delete_failed
            row["placement_status"] = placement_status

        return rows

    def get_active_campaign_placements_summary_for_rule(
        self,
        rule_id: int,
        *,
        basic_only: bool = True,
    ) -> dict[str, Any]:
        where_parts = [
            "r.rule_id = %s",
            "m.send_status = 'sent'",
            "m.delete_status IN ('pending', 'processing', 'failed')",
        ]
        params: list[Any] = [int(rule_id)]
        if basic_only:
            where_parts.append("r.scheduled_post_id IS NULL")
            where_parts.append("r.run_type IN ('manual', 'scheduled')")

        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    WITH placements AS (
                        SELECT
                            r.id AS run_id,
                            COUNT(m.id) FILTER (WHERE m.delete_status = 'pending') AS delete_pending,
                            COUNT(m.id) FILTER (WHERE m.delete_status = 'processing') AS delete_processing,
                            COUNT(m.id) FILTER (WHERE m.delete_status = 'failed') AS delete_failed
                        FROM campaign_runs r
                        JOIN campaign_run_messages m ON m.run_id = r.id
                        WHERE {' AND '.join(where_parts)}
                        GROUP BY r.id
                    )
                    SELECT
                        COUNT(*) AS placements_total,
                        COUNT(*) FILTER (WHERE delete_pending + delete_processing > 0) AS active_total,
                        COUNT(*) FILTER (WHERE delete_failed > 0) AS delete_problem_total,
                        COUNT(*) FILTER (WHERE delete_failed > 0 AND delete_pending + delete_processing > 0) AS mixed_total,
                        COALESCE(SUM(delete_pending), 0) AS delete_pending_total,
                        COALESCE(SUM(delete_processing), 0) AS delete_processing_total,
                        COALESCE(SUM(delete_failed), 0) AS delete_failed_total
                    FROM placements
                    """,
                    tuple(params),
                )
                row = dict(cur.fetchone() or {})

        return {
            "rule_id": int(rule_id),
            "basic_only": bool(basic_only),
            "placements_total": int(row.get("placements_total") or 0),
            "active_total": int(row.get("active_total") or 0),
            "delete_problem_total": int(row.get("delete_problem_total") or 0),
            "mixed_total": int(row.get("mixed_total") or 0),
            "delete_pending_total": int(row.get("delete_pending_total") or 0),
            "delete_processing_total": int(row.get("delete_processing_total") or 0),
            "delete_failed_total": int(row.get("delete_failed_total") or 0),
        }

    def claim_due_campaign_run_messages_for_delete(self, *, limit: int = 50) -> list[dict[str, Any]]:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    WITH due AS (
                        SELECT id
                        FROM campaign_run_messages
                        WHERE send_status = 'sent'
                          AND delete_status = 'pending'
                          AND delete_after_at IS NOT NULL
                          AND delete_after_at <= NOW()
                          AND sent_message_id IS NOT NULL
                          AND NOT (
                            views_final_status = 'failed'
                            AND views_final_next_retry_at IS NOT NULL
                            AND views_final_next_retry_at > NOW()
                          )
                        ORDER BY delete_after_at ASC, id ASC
                        LIMIT %s
                        FOR UPDATE SKIP LOCKED
                    )
                    UPDATE campaign_run_messages m
                    SET delete_status = 'processing',
                        delete_attempt_count = COALESCE(delete_attempt_count, 0) + 1,
                        updated_at = NOW()
                    FROM due
                    WHERE m.id = due.id
                    RETURNING m.*
                    """,
                    (int(limit),),
                )
                rows = [dict(row) for row in (cur.fetchall() or [])]
            conn.commit()
            return rows

    def mark_campaign_run_message_views_processing(self, message_id: int) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE campaign_run_messages
                    SET views_final_status = 'processing',
                        views_final_attempt_count = COALESCE(views_final_attempt_count, 0) + 1,
                        updated_at = NOW()
                    WHERE id = %s
                    """,
                    (int(message_id),),
                )
                updated = cur.rowcount > 0
            conn.commit()
            return updated

    def mark_campaign_run_message_views_collected(self, message_id: int, *, views_count: int, collected_at: str) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE campaign_run_messages
                    SET views_final_count = %s,
                        views_final_status = 'collected',
                        views_final_collected_at = %s,
                        views_final_error_text = NULL,
                        views_final_next_retry_at = NULL,
                        updated_at = NOW()
                    WHERE id = %s
                    """,
                    (int(views_count), collected_at, int(message_id)),
                )
                updated = cur.rowcount > 0
            conn.commit()
            return updated

    def mark_campaign_run_message_views_unavailable(self, message_id: int, *, error_text: str | None, collected_at: str) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE campaign_run_messages
                    SET views_final_count = 0,
                        views_final_status = 'unavailable',
                        views_final_collected_at = %s,
                        views_final_error_text = %s,
                        views_final_next_retry_at = NULL,
                        updated_at = NOW()
                    WHERE id = %s
                    """,
                    (collected_at, error_text, int(message_id)),
                )
                updated = cur.rowcount > 0
            conn.commit()
            return updated

    def mark_campaign_run_message_views_failed(self, message_id: int, *, error_text: str, next_retry_at: str) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE campaign_run_messages
                    SET views_final_status = 'failed',
                        views_final_error_text = %s,
                        views_final_next_retry_at = %s,
                        updated_at = NOW()
                    WHERE id = %s
                    """,
                    (error_text, next_retry_at, int(message_id)),
                )
                updated = cur.rowcount > 0
            conn.commit()
            return updated

    def mark_campaign_run_message_deleted(self, message_id: int) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE campaign_run_messages
                    SET delete_status = 'deleted',
                        deleted_at = NOW(),
                        delete_error_text = NULL,
                        updated_at = NOW()
                    WHERE id = %s
                    """,
                    (int(message_id),),
                )
                updated = cur.rowcount > 0
            conn.commit()
            return updated

    def mark_campaign_run_message_delete_failed(self, message_id: int, *, error_text: str) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE campaign_run_messages
                    SET delete_status = 'failed',
                        delete_error_text = %s,
                        updated_at = NOW()
                    WHERE id = %s
                    """,
                    (error_text, int(message_id)),
                )
                updated = cur.rowcount > 0
            conn.commit()
            return updated

    def reset_stuck_campaign_delete_processing(self, *, stuck_seconds: int = 300) -> int:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE campaign_run_messages
                    SET delete_status = 'pending',
                        updated_at = NOW()
                    WHERE delete_status = 'processing'
                      AND updated_at <= NOW() - (%s * INTERVAL '1 second')
                    """,
                    (int(stuck_seconds),),
                )
                count = int(cur.rowcount)
            conn.commit()
            return count

    def update_rule_repost_campaign_settings(self, rule_id: int, *, enabled: bool, show_seconds: int) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE routing SET repost_campaign_enabled=%s, repost_campaign_show_seconds=%s WHERE id=%s", (bool(enabled), int(show_seconds), int(rule_id)))
            conn.commit()
            return True

    def get_rule_repost_campaign_summary(self, rule_id: int) -> dict[str, Any]:
        default = {
            "enabled": False,
            "show_seconds": 0,
            "top_time_enabled": False,
            "top_time_seconds": 0,
            "targets_total": 0,
            "targets_active": 0,
            "targets_ready": 0,
            "targets_with_errors": 0,
        }
        try:
            with self.connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT
                            COALESCE(repost_campaign_enabled, FALSE) AS enabled,
                            COALESCE(repost_campaign_show_seconds, 0) AS show_seconds,
                            COALESCE(repost_campaign_top_time_enabled, FALSE) AS top_time_enabled,
                            COALESCE(repost_campaign_top_time_seconds, 0) AS top_time_seconds
                        FROM routing
                        WHERE id = %s
                        LIMIT 1
                        """,
                        (int(rule_id),),
                    )
                    rule_row = cur.fetchone()
                    if not rule_row:
                        return default
                    cur.execute(
                        """
                        SELECT
                            COUNT(*) AS targets_total,
                            COUNT(*) FILTER (WHERE is_active = TRUE) AS targets_active,
                            COUNT(*) FILTER (
                                WHERE is_active = TRUE
                                  AND COALESCE(can_post, TRUE) = TRUE
                                  AND COALESCE(can_delete, TRUE) = TRUE
                                  AND (last_check_error IS NULL OR last_check_error = '')
                            ) AS targets_ready,
                            COUNT(*) FILTER (
                                WHERE is_active = TRUE
                                  AND (
                                      COALESCE(can_post, TRUE) = FALSE
                                      OR COALESCE(can_delete, TRUE) = FALSE
                                      OR (last_check_error IS NOT NULL AND last_check_error <> '')
                                  )
                            ) AS targets_with_errors
                        FROM rule_repost_campaign_targets
                        WHERE rule_id = %s
                        """,
                        (int(rule_id),),
                    )
                    targets_row = cur.fetchone() or {}
            return {
                "enabled": bool(rule_row["enabled"]),
                "show_seconds": int(rule_row["show_seconds"] or 0),
                "top_time_enabled": bool(rule_row.get("top_time_enabled")),
                "top_time_seconds": int(rule_row.get("top_time_seconds") or 0),
                "targets_total": int(targets_row.get("targets_total") or 0),
                "targets_active": int(targets_row.get("targets_active") or 0),
                "targets_ready": int(targets_row.get("targets_ready") or 0),
                "targets_with_errors": int(targets_row.get("targets_with_errors") or 0),
            }
        except Exception as exc:
            logger.exception(
                "REPOST_CAMPAIGN_SUMMARY_FAILED | rule_id=%s | error=%s",
                rule_id,
                exc,
            )
            return default

    def add_rule_repost_campaign_target(self, *, rule_id: int, target_id: str, target_thread_id: int | None, title: str | None, created_by: int | None) -> int | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("""INSERT INTO rule_repost_campaign_targets(rule_id,target_id,target_thread_id,title,created_by) VALUES (%s,%s,%s,%s,%s) ON CONFLICT (rule_id,target_id,COALESCE(target_thread_id,-1)) DO NOTHING RETURNING id""", (int(rule_id), str(target_id), target_thread_id, title, created_by))
                row=cur.fetchone()
            conn.commit(); return int(row['id']) if row else None

    def list_rule_repost_campaign_targets(self, rule_id: int, *, active_only: bool = False) -> list[dict[str, Any]]:
        with self.connect() as conn:
            with conn.cursor() as cur:
                if active_only: cur.execute("SELECT * FROM rule_repost_campaign_targets WHERE rule_id=%s AND is_active=TRUE ORDER BY id", (int(rule_id),))
                else: cur.execute("SELECT * FROM rule_repost_campaign_targets WHERE rule_id=%s ORDER BY id", (int(rule_id),))
                return [dict(x) for x in cur.fetchall() or []]

    def remove_rule_repost_campaign_target(self, row_id: int) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    DELETE FROM rule_repost_campaign_targets
                    WHERE id = %s
                    RETURNING id
                    """,
                    (int(row_id),),
                )
                row = cur.fetchone()
            conn.commit()
            return row is not None

    def set_rule_repost_campaign_target_active(self, row_id: int, is_active: bool) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE rule_repost_campaign_targets
                    SET is_active = %s,
                        updated_at = NOW()
                    WHERE id = %s
                    RETURNING id
                    """,
                    (bool(is_active), int(row_id)),
                )
                row = cur.fetchone()
            conn.commit()
            return row is not None

    def update_rule_repost_campaign_target_check_result(
        self,
        row_id: int,
        *,
        title: str | None = None,
        last_check_error: str | None = None,
        can_post: bool | None = None,
        can_delete: bool | None = None,
        publish_status: str | None = None,
        delete_permission_status: str | None = None,
        publish_error_text: str | None = None,
        delete_error_text: str | None = None,
        check_source: str | None = None,
        check_status_json: dict | None = None,
    ) -> bool:
        _ = check_status_json
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE rule_repost_campaign_targets
                    SET title = COALESCE(%s, title),
                        can_post = %s,
                        can_delete = %s,
                        publish_status = %s,
                        delete_permission_status = %s,
                        publish_error_text = %s,
                        delete_error_text = %s,
                        check_source = %s,
                        last_check_at = NOW(),
                        last_check_error = %s,
                        updated_at = NOW()
                    WHERE id = %s
                    RETURNING id
                    """,
                    (title, can_post, can_delete, publish_status, delete_permission_status, publish_error_text, delete_error_text, check_source, last_check_error, int(row_id)),
                )
                row = cur.fetchone()
            conn.commit()
            return row is not None

    def create_delivery_campaign_copy(self, *, delivery_id: int, rule_id: int, target_id: str, target_thread_id: int | None, target_title: str | None) -> int | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("""INSERT INTO delivery_campaign_copies(delivery_id,rule_id,target_id,target_thread_id,target_title) VALUES (%s,%s,%s,%s,%s) ON CONFLICT (delivery_id,target_id,COALESCE(target_thread_id,-1)) DO NOTHING RETURNING id""", (int(delivery_id), int(rule_id), str(target_id), target_thread_id, target_title))
                row=cur.fetchone()
            conn.commit(); return int(row['id']) if row else None

    def get_delivery_campaign_copy(self, copy_id: int) -> dict[str, Any] | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT *, COALESCE(sent_message_ids_json, '[]'::jsonb) AS sent_message_ids_json FROM delivery_campaign_copies WHERE id=%s", (int(copy_id),)); r=cur.fetchone()
                if not r: return None
                d=dict(r); d['sent_message_ids']=_safe_json_loads(d.get('sent_message_ids_json'), [])
                return d

    def mark_delivery_campaign_copy_processing(self, copy_id: int) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE delivery_campaign_copies SET send_status='processing',updated_at=NOW() WHERE id=%s AND (send_status='pending' OR (send_status='failed' AND send_error_text LIKE 'rejected:%%'))", (int(copy_id),))
                return cur.rowcount == 1

    def mark_delivery_campaign_copy_unverified(self, copy_id: int, reason: str) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE delivery_campaign_copies SET send_status='unverified',send_error_text=%s,delete_status='none',updated_at=NOW() WHERE id=%s AND send_status='processing'", (reason,int(copy_id)))
                return cur.rowcount == 1

    def mark_delivery_campaign_copy_sent(self, copy_id: int, *, sent_message_id: int | None, sent_message_ids: list[int] | None, delivery_method: str | None, delete_after_at: str | None) -> bool:
        from app.telegram_publication import publication_pairs
        if not sent_message_ids or not publication_pairs(sent_message_ids,sent_message_ids) or sent_message_id!=sent_message_ids[0]:
            return False
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE delivery_campaign_copies SET send_status='sent', sent_at=NOW(), sent_message_id=%s, sent_message_ids_json=%s::jsonb, delivery_method=%s, delete_after_at=%s, delete_status='scheduled', updated_at=NOW() WHERE id=%s AND send_status='processing'", (sent_message_id, _json_dumps(sent_message_ids or []), delivery_method, delete_after_at, int(copy_id)))
                return cur.rowcount == 1

    def _update_copy_status(self, copy_id: int, field: str, value: str) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(f"UPDATE delivery_campaign_copies SET {field}=%s, updated_at=NOW() WHERE id=%s", (value, int(copy_id)))
            conn.commit(); return True

    def list_campaign_copies_missing_delete_job(self, *, limit: int = 50) -> list[dict[str, Any]]:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT c.id,c.delete_after_at FROM delivery_campaign_copies c
                    WHERE c.send_status='sent' AND c.delete_status='scheduled'
                      AND c.sent_message_id>0 AND c.delete_after_at IS NOT NULL
                      AND NOT EXISTS (
                        SELECT 1 FROM jobs j WHERE j.job_type='repost_campaign_delete_copy'
                          AND j.payload_json->>'copy_id'=c.id::text
                          AND j.status IN ('pending','leased','processing','retry','failed')
                      )
                    ORDER BY c.delete_after_at,c.id LIMIT %s
                    """, (max(1,min(limit,100)),))
                return [dict(row) for row in cur.fetchall()]

    def mark_delivery_campaign_copy_send_failed(self, copy_id: int, error_text: str) -> bool:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE delivery_campaign_copies SET send_status='failed',send_error_text=%s,updated_at=NOW() WHERE id=%s AND send_status IN ('pending','processing')", (error_text,int(copy_id)))
                return cur.rowcount == 1
    def mark_delivery_campaign_copy_delete_processing(self, copy_id: int) -> bool: return self._update_copy_status(copy_id,'delete_status','processing')
    def mark_delivery_campaign_copy_deleted(self, copy_id: int) -> bool: return self._update_copy_status(copy_id,'delete_status','deleted')
    def mark_delivery_campaign_copy_delete_failed(self, copy_id: int, error_text: str) -> bool: return self._update_copy_status(copy_id,'delete_status','failed')
    def mark_delivery_campaign_copy_delete_skipped(self, copy_id: int, reason: str) -> bool: return self._update_copy_status(copy_id,'delete_status','skipped')
