from __future__ import annotations

from typing import Any


class TenantService:
    def __init__(self, repo) -> None:
        self._repo = repo

    def create_tenant(self, admin_id: int, name: str | None = None) -> int | None:
        tenant_name = (name or f"tenant-{int(admin_id)}").strip() or f"tenant-{int(admin_id)}"
        return self._repo.create_tenant(int(admin_id), tenant_name)

    def get_tenant_by_admin(self, admin_id: int) -> dict[str, Any] | None:
        return self._repo.get_tenant_by_admin(int(admin_id))

    def ensure_tenant_exists(self, admin_id: int) -> dict[str, Any]:
        tenant = self.get_tenant_by_admin(admin_id)
        if tenant:
            return tenant

        self.create_tenant(admin_id)
        tenant = self.get_tenant_by_admin(admin_id)
        if tenant:
            return tenant

        fallback = self._repo.get_default_tenant()
        if fallback:
            return fallback

        self.create_tenant(admin_id=0, name="default")
        return self._repo.get_default_tenant() or {
            "id": 1,
            "name": "default",
            "owner_admin_id": 0,
            "is_active": True,
        }

    def disable_tenant(self, tenant_id: int) -> bool:
        return bool(self._repo.set_tenant_active(int(tenant_id), False))
