from __future__ import annotations


class UserService:
    def __init__(self, repo) -> None:
        self._repo = repo

    def add_user_to_tenant(self, tenant_id: int, telegram_id: int, role: str = "viewer") -> bool:
        return bool(self._repo.add_tenant_user(int(tenant_id), int(telegram_id), role))

    def get_user_role(self, tenant_id: int, telegram_id: int) -> str | None:
        return self._repo.get_tenant_user_role(int(tenant_id), int(telegram_id))

    def check_user_access(self, tenant_id: int, telegram_id: int, min_role: str = "viewer") -> bool:
        current_role = self.get_user_role(tenant_id, telegram_id)
        if not current_role:
            return False

        order = {"viewer": 1, "admin": 2, "owner": 3}
        return order.get(current_role, 0) >= order.get(min_role, 1)
