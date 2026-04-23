from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.repository_split_base import RepositorySplitBase


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
