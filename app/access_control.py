from __future__ import annotations

from app.config import settings


def is_admin_user(user_id: int | None) -> bool:
    if user_id is None:
        return False
    return int(user_id) == int(settings.admin_id)


def get_current_tenant_for_user(user_id: int, tenant_service) -> int:
    tenant = tenant_service.ensure_tenant_exists(int(user_id))
    return int(tenant.get("id") or 1)


def ensure_user_tenant(user_id: int, tenant_service) -> int:
    return get_current_tenant_for_user(user_id, tenant_service)


def is_rule_owned_by_user(rule_id: int, user_id: int, db, tenant_service) -> bool:
    if is_admin_user(user_id):
        return True
    if not hasattr(db, "get_rule_tenant_id"):
        return False
    return int(db.get_rule_tenant_id(int(rule_id)) or 1) == ensure_user_tenant(int(user_id), tenant_service)


def is_channel_owned_by_user(
    channel_id: str,
    thread_id: int | None,
    channel_type: str,
    user_id: int,
    db,
    tenant_service,
) -> bool:
    if is_admin_user(user_id):
        return True
    tenant_id = ensure_user_tenant(int(user_id), tenant_service)
    if not hasattr(db, "get_channels_for_tenant"):
        return False
    rows = db.get_channels_for_tenant(tenant_id, channel_type)
    return any(
        str(row["channel_id"]) == str(channel_id)
        and ((row["thread_id"] is None and thread_id is None) or int(row["thread_id"]) == int(thread_id or 0))
        for row in rows
    )
