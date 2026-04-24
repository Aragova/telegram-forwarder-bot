from __future__ import annotations

import logging
from datetime import datetime, timezone

from app.subscription_service import SubscriptionService
from app.tenant_service import TenantService

logger = logging.getLogger("forwarder.saas")


def _is_expired(expires_at: str | None) -> bool:
    if not expires_at:
        return False
    try:
        dt = datetime.fromisoformat(str(expires_at).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt < datetime.now(timezone.utc)
    except Exception:
        return False


def _has_active_subscription(subscription: dict | None) -> bool:
    if not subscription:
        return False
    if str(subscription.get("status") or "").strip().lower() not in {"active", "trial", "grace"}:
        return False
    if _is_expired(subscription.get("expires_at")):
        return False
    return True


def _plan_name(subscription: dict | None) -> str:
    return str((subscription or {}).get("plan_name") or "").strip().upper()


def ensure_owner_and_default_tenant_bootstrap(repo, admin_id: int) -> None:
    tenant_service = TenantService(repo)
    subscription_service = SubscriptionService(repo)

    owner_tenant = tenant_service.ensure_tenant_exists(int(admin_id))
    owner_tenant_id = int(owner_tenant.get("id") or 1)

    if not bool(owner_tenant.get("is_active", True)):
        repo.set_tenant_active(owner_tenant_id, True)

    owner_sub = subscription_service.get_active_subscription(owner_tenant_id)
    owner_plan = _plan_name(owner_sub)
    if (not _has_active_subscription(owner_sub)) or owner_plan not in {"OWNER", "PRO"}:
        target_plan = "OWNER" if repo.get_plan_by_name("OWNER") else "PRO"
        subscription_service.assign_plan(owner_tenant_id, target_plan, status="active", expires_at=None)
        logger.warning("SaaS bootstrap: owner tenant активирован | tenant_id=%s | plan=%s", owner_tenant_id, target_plan)

    default_tenant = repo.get_tenant_by_id(1) if hasattr(repo, "get_tenant_by_id") else None
    if not default_tenant and hasattr(repo, "get_default_tenant"):
        default_tenant = repo.get_default_tenant()
    if not default_tenant:
        return

    default_tenant_id = int(default_tenant.get("id") or 1)
    if not bool(default_tenant.get("is_active", True)):
        repo.set_tenant_active(default_tenant_id, True)

    default_sub = subscription_service.get_active_subscription(default_tenant_id)
    if not _has_active_subscription(default_sub):
        default_plan = "OWNER" if default_tenant_id == owner_tenant_id and repo.get_plan_by_name("OWNER") else "PRO"
        subscription_service.assign_plan(default_tenant_id, default_plan, status="active", expires_at=None)
        logger.warning(
            "SaaS bootstrap: default tenant получил активную подписку | tenant_id=%s | plan=%s",
            default_tenant_id,
            default_plan,
        )
