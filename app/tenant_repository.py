from __future__ import annotations

from typing import Any

from app.repository_models import utc_now_iso
from app.repository_split_base import RepositorySplitBase


class TenantRepository(RepositorySplitBase):
    def create_tenant(self, owner_admin_id: int, name: str) -> int | None:
        with self.connect() as conn:
            tenant_id = self.ensure_tenant_for_admin_conn(conn, int(owner_admin_id))
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
