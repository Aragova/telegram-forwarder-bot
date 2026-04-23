from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any


class UsageService:
    def __init__(self, repo) -> None:
        self._repo = repo

    def increment_jobs(self, tenant_id: int, amount: int = 1) -> None:
        if hasattr(self._repo, "bump_usage"):
            self._repo.bump_usage(int(tenant_id), jobs_delta=int(amount))

    def increment_video(self, tenant_id: int, amount: int = 1) -> None:
        if hasattr(self._repo, "bump_usage"):
            self._repo.bump_usage(int(tenant_id), video_delta=int(amount))

    def get_today_usage(self, tenant_id: int):
        if not hasattr(self._repo, "get_usage_for_date"):
            return {"jobs_count": 0, "video_count": 0, "storage_used_mb": 0, "api_calls": 0}
        return self._repo.get_usage_for_date(int(tenant_id), date.today().isoformat())

    def reset_daily_usage(self) -> int:
        if not hasattr(self._repo, "reset_usage_for_day"):
            return 0
        return int(self._repo.reset_usage_for_day(date.today().isoformat()) or 0)

    def build_usage_snapshot(self, tenant_id: int, date_from: str, date_to: str) -> dict[str, Any]:
        if hasattr(self._repo, "get_usage_for_period"):
            usage = self._repo.get_usage_for_period(int(tenant_id), str(date_from), str(date_to))
        else:
            usage = {"jobs_count": 0, "video_count": 0, "storage_used_mb": 0, "api_calls": 0}
        sub = self._repo.get_active_subscription(int(tenant_id)) if hasattr(self._repo, "get_active_subscription") else None
        plan_name = str((sub or {}).get("plan_name") or "UNKNOWN")
        max_jobs = int((sub or {}).get("max_jobs_per_day") or 0)
        max_video = int((sub or {}).get("max_video_per_day") or 0)
        max_storage = int((sub or {}).get("max_storage_mb") or 0)
        jobs = int(usage.get("jobs_count") or 0)
        video = int(usage.get("video_count") or 0)
        storage = int(usage.get("storage_used_mb") or 0)
        over = {
            "jobs": max_jobs > 0 and jobs > max_jobs,
            "video": max_video > 0 and video > max_video,
            "storage": max_storage > 0 and storage > max_storage,
        }
        return {
            "tenant_id": int(tenant_id),
            "date_from": str(date_from),
            "date_to": str(date_to),
            "plan_name": plan_name,
            "active_subscription": sub,
            "jobs_count": jobs,
            "video_count": video,
            "storage_used_mb": storage,
            "api_calls": int(usage.get("api_calls") or 0),
            "over_limit_indicators": over,
            "overage_candidates": [k for k, is_over in over.items() if is_over],
        }
