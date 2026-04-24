from __future__ import annotations


class LimitExceededError(RuntimeError):
    pass


class LimitService:
    def __init__(self, repo, subscription_service, usage_service) -> None:
        self._repo = repo
        self._subscription_service = subscription_service
        self._usage_service = usage_service

    def _get_plan_limits(self, tenant_id: int) -> dict:
        sub = self._subscription_service.get_active_subscription(int(tenant_id))
        if not sub:
            return {}
        return dict(sub)

    @staticmethod
    def _is_unlimited_plan(limits: dict) -> bool:
        return str(limits.get("plan_name") or "").strip().upper() == "OWNER"

    def can_create_rule(self, tenant_id: int) -> tuple[bool, str | None]:
        limits = self._get_plan_limits(tenant_id)
        if self._is_unlimited_plan(limits):
            return True, None
        max_rules = int(limits.get("max_rules") or 0)
        if max_rules <= 0:
            return True, None
        if not hasattr(self._repo, "count_rules_for_tenant"):
            return True, None
        current = int(self._repo.count_rules_for_tenant(int(tenant_id)) or 0)
        if current >= max_rules:
            return False, f"Достигнут лимит правил: {max_rules}"
        return True, None

    def can_enqueue_job(self, tenant_id: int) -> tuple[bool, str | None]:
        limits = self._get_plan_limits(tenant_id)
        if self._is_unlimited_plan(limits):
            return True, None
        if not self._subscription_service.is_subscription_active(int(tenant_id)):
            return False, "Подписка неактивна"
        max_jobs = int(limits.get("max_jobs_per_day") or 0)
        if max_jobs <= 0:
            return True, None
        usage = self._usage_service.get_today_usage(int(tenant_id)) or {}
        jobs_count = int(usage.get("jobs_count") or 0)
        if jobs_count >= max_jobs:
            return False, f"Достигнут дневной лимит задач: {max_jobs}"
        return True, None

    def can_process_video(self, tenant_id: int) -> tuple[bool, str | None]:
        limits = self._get_plan_limits(tenant_id)
        if self._is_unlimited_plan(limits):
            return True, None
        if not self._subscription_service.is_subscription_active(int(tenant_id)):
            return False, "Подписка неактивна"
        max_video = int(limits.get("max_video_per_day") or 0)
        if max_video <= 0:
            return True, None
        usage = self._usage_service.get_today_usage(int(tenant_id)) or {}
        video_count = int(usage.get("video_count") or 0)
        if video_count >= max_video:
            return False, f"Лимит видео задач превышен ({max_video} в день)"
        return True, None

    def check_limits_or_raise(self, *, tenant_id: int, action: str) -> None:
        if action == "rule":
            ok, reason = self.can_create_rule(tenant_id)
        elif action == "job":
            ok, reason = self.can_enqueue_job(tenant_id)
        elif action == "video":
            ok, reason = self.can_process_video(tenant_id)
        else:
            ok, reason = True, None
        if not ok:
            raise LimitExceededError(reason or "Лимит превышен")
