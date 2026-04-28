from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from app.repository_split_base import RepositorySplitBase

logger = logging.getLogger("forwarder.repository")


class UsageRepository(RepositorySplitBase):
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
        if row:
            return dict(row)
        return {"tenant_id": int(tenant_id), "date": day, "jobs_count": 0, "video_count": 0, "storage_used_mb": 0, "api_calls": 0}

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

    def build_billing_usage_data(self, tenant_id: int, period_start: str, period_end: str) -> dict[str, Any]:
        usage = self.get_usage_for_period(int(tenant_id), str(period_start), str(period_end))
        return {
            "tenant_id": int(tenant_id),
            "period_start": str(period_start),
            "period_end": str(period_end),
            "jobs_count": int(usage.get("jobs_count") or 0),
            "video_count": int(usage.get("video_count") or 0),
            "storage_used_mb": int(usage.get("storage_used_mb") or 0),
            "api_calls": int(usage.get("api_calls") or 0),
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
                cur.execute("SELECT COUNT(*) AS cnt FROM invoices WHERE status = 'draft'")
                invoices_draft = int((cur.fetchone() or {}).get("cnt") or 0)
                cur.execute("SELECT COUNT(*) AS cnt FROM invoices WHERE status = 'open'")
                invoices_open_exact = int((cur.fetchone() or {}).get("cnt") or 0)
                cur.execute("SELECT COUNT(*) AS cnt FROM invoices WHERE status = 'paid'")
                invoices_paid = int((cur.fetchone() or {}).get("cnt") or 0)
                cur.execute(
                    """
                    SELECT COUNT(*) AS cnt
                    FROM subscriptions
                    WHERE status IN ('active', 'trial', 'grace')
                      AND current_period_end IS NOT NULL
                      AND current_period_end <= %s
                    """,
                    (datetime.now(timezone.utc).isoformat(),),
                )
                billing_periods_due = int((cur.fetchone() or {}).get("cnt") or 0)
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
                cur.execute(
                    """
                    SELECT COUNT(*) AS cnt
                    FROM billing_events
                    WHERE event_type = 'invoice_generation_error'
                    """
                )
                billing_generation_errors = int((cur.fetchone() or {}).get("cnt") or 0)
                cur.execute(
                    """
                    WITH usage_period AS (
                        SELECT
                            s.tenant_id,
                            COALESCE(SUM(u.jobs_count), 0) AS jobs_count,
                            COALESCE(SUM(u.video_count), 0) AS video_count,
                            COALESCE(MAX(u.storage_used_mb), 0) AS storage_used_mb,
                            p.max_jobs_per_day,
                            p.max_video_per_day,
                            p.max_storage_mb
                        FROM subscriptions s
                        JOIN plans p ON p.id = s.plan_id
                        LEFT JOIN usage_stats u
                          ON u.tenant_id = s.tenant_id
                         AND s.current_period_start IS NOT NULL
                         AND s.current_period_end IS NOT NULL
                         AND u.date >= s.current_period_start
                         AND u.date <= s.current_period_end
                        WHERE s.status IN ('active', 'trial', 'grace')
                        GROUP BY s.tenant_id, p.max_jobs_per_day, p.max_video_per_day, p.max_storage_mb
                    )
                    SELECT COUNT(*) AS cnt
                    FROM usage_period
                    WHERE (max_jobs_per_day > 0 AND jobs_count > max_jobs_per_day)
                       OR (max_video_per_day > 0 AND video_count > max_video_per_day)
                       OR (max_storage_mb > 0 AND storage_used_mb > max_storage_mb)
                    """
                )
                tenants_with_overage_current_period = int((cur.fetchone() or {}).get("cnt") or 0)
        return {
            "tenants_active": active,
            "tenants_blocked": blocked,
            "tenants_over_limits": over,
            "subscriptions_active": subscriptions_active,
            "subscriptions_in_grace": subscriptions_in_grace,
            "subscriptions_expired": subscriptions_expired,
            "invoices_open": invoices_open,
            "invoices_draft": invoices_draft,
            "invoices_open_exact": invoices_open_exact,
            "invoices_paid": invoices_paid,
            "billing_periods_due": billing_periods_due,
            "billing_generation_errors": billing_generation_errors,
            "tenants_with_overage_current_period": tenants_with_overage_current_period,
            "tenants_with_billing_issues": billing_issues,
            "tenants_with_overage_candidates": overage_candidates,
        }

    def get_recoverable_summary_for_tenant(self, tenant_id: int) -> dict[str, Any]:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*) AS cnt
                    FROM routing
                    WHERE COALESCE(tenant_id, 1) = %s
                      AND is_active = TRUE
                    """,
                    (int(tenant_id),),
                )
                active_rules_count = int((cur.fetchone() or {}).get("cnt") or 0)
                cur.execute(
                    """
                    SELECT COUNT(*) AS cnt
                    FROM deliveries d
                    JOIN routing r ON r.id = d.rule_id
                    WHERE COALESCE(r.tenant_id, 1) = %s
                      AND d.status = 'pending'
                    """,
                    (int(tenant_id),),
                )
                pending_deliveries_count = int((cur.fetchone() or {}).get("cnt") or 0)
                cur.execute(
                    """
                    SELECT
                        COALESCE(SUM(CASE WHEN j.job_type LIKE 'video_%%' THEN 1 ELSE 0 END), 0) AS video_cnt,
                        COALESCE(SUM(CASE WHEN j.job_type NOT LIKE 'video_%%' THEN 1 ELSE 0 END), 0) AS non_video_cnt
                    FROM jobs j
                    WHERE COALESCE(NULLIF(j.payload_json->>'tenant_id', '')::BIGINT, 1) = %s
                      AND j.status = 'failed'
                      AND (
                          COALESCE(j.error_text, '') ILIKE '%%лимит%%'
                          OR COALESCE(j.error_text, '') ILIKE '%%подписка неактивна%%'
                          OR COALESCE(j.error_text, '') ILIKE '%%subscription%%'
                          OR COALESCE(j.error_text, '') ILIKE '%%limit%%'
                      )
                      AND COALESCE(j.error_text, '') NOT ILIKE '%%ffmpeg%%'
                      AND COALESCE(j.error_text, '') NOT ILIKE '%%telegram%%'
                      AND COALESCE(j.error_text, '') NOT ILIKE '%%invalid source%%'
                      AND COALESCE(j.error_text, '') NOT ILIKE '%%access%%'
                    """,
                    (int(tenant_id),),
                )
                row = cur.fetchone() or {}
                failed_limit_video_jobs_count = int(row.get("video_cnt") or 0)
                failed_limit_jobs_count = int(row.get("non_video_cnt") or 0)
                cur.execute(
                    """
                    SELECT id, event_type, created_at, metadata_json
                    FROM billing_events
                    WHERE tenant_id = %s
                      AND event_type IN ('limit_rule_blocked', 'limit_job_blocked', 'limit_video_blocked', 'subscription_blocked_action')
                    ORDER BY id DESC
                    LIMIT 10
                    """,
                    (int(tenant_id),),
                )
                limit_rows = cur.fetchall() or []
        return {
            "active_rules_count": active_rules_count,
            "pending_deliveries_count": pending_deliveries_count,
            "failed_limit_jobs_count": failed_limit_jobs_count,
            "failed_limit_video_jobs_count": failed_limit_video_jobs_count,
            "last_blocked_events": [dict(row) for row in limit_rows],
        }

    def recover_blocked_jobs_for_tenant(self, tenant_id: int) -> int:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT j.id, COALESCE(j.error_text, '') AS error_text
                    FROM jobs j
                    WHERE COALESCE(NULLIF(j.payload_json->>'tenant_id', '')::BIGINT, 1) = %s
                      AND j.status = 'failed'
                    """,
                    (int(tenant_id),),
                )
                rows = cur.fetchall() or []
                recover_ids: list[int] = []
                for row in rows:
                    job_id = int(row["id"])
                    error_text = str(row.get("error_text") or "")
                    lowered = error_text.lower()
                    has_limit_marker = (
                        "лимит" in lowered
                        or "подписка неактивна" in lowered
                        or "subscription" in lowered
                        or "limit" in lowered
                    )
                    has_excluded_marker = (
                        "ffmpeg" in lowered
                        or "telegram" in lowered
                        or "invalid source" in lowered
                        or "access" in lowered
                    )
                    if has_limit_marker and not has_excluded_marker:
                        recover_ids.append(job_id)
                    else:
                        logger.info("skipped non-limit failed job job_id=%s reason=%s", job_id, (error_text or "нет текста ошибки")[:160])
                if recover_ids:
                    cur.execute(
                        """
                        UPDATE jobs
                        SET status = 'retry',
                            run_at = NOW(),
                            locked_by = NULL,
                            lease_until = NULL,
                            updated_at = NOW(),
                            error_text = NULL
                        WHERE id = ANY(%s)
                        """,
                        (recover_ids,),
                    )
                    recovered = int(cur.rowcount or 0)
                else:
                    recovered = 0
            conn.commit()
        return recovered

    def recover_pending_deliveries_for_tenant(self, tenant_id: int) -> int:
        now_iso = datetime.now(timezone.utc).isoformat()
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE routing
                    SET next_run_at = %s
                    WHERE COALESCE(tenant_id, 1) = %s
                      AND is_active = TRUE
                    """,
                    (now_iso, int(tenant_id)),
                )
                updated_rules = int(cur.rowcount or 0)
            conn.commit()
        return updated_rules

    def get_recent_limit_events_for_tenant(self, tenant_id: int, limit: int = 10) -> list[dict[str, Any]]:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, event_type, event_source, metadata_json, created_at
                    FROM billing_events
                    WHERE tenant_id = %s
                      AND event_type IN ('limit_rule_blocked', 'limit_job_blocked', 'limit_video_blocked', 'subscription_blocked_action')
                    ORDER BY id DESC
                    LIMIT %s
                    """,
                    (int(tenant_id), int(limit)),
                )
                rows = cur.fetchall() or []
        return [dict(row) for row in rows]
