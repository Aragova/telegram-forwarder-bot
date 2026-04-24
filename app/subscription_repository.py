from __future__ import annotations

from typing import Any

from app.repository_models import utc_now_iso
from app.repository_split_base import RepositorySplitBase


class SubscriptionRepository(RepositorySplitBase):
    def ensure_default_plans(self) -> None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                for row in [
                    ("FREE", 3, 1, 100, 10, 512, 1, "0.00"),
                    ("BASIC", 15, 2, 1000, 100, 5120, 2, "19.99"),
                    ("PRO", 100, 4, 10000, 1000, 51200, 3, "99.99"),
                    ("OWNER", 0, 32, 0, 0, 0, 10, "0.00"),
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
            row_id = self.fetch_inserted_id(
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
