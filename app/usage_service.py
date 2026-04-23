from __future__ import annotations

from datetime import date, datetime, timezone


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
