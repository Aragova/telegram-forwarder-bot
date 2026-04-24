from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(slots=True, frozen=True)
class TenantFairnessSnapshot:
    tenant_id: int
    weight: int
    priority_level: int
    pending: int
    processing: int
    retry: int
    oldest_pending_age_sec: int
    heavy_pending: int
    light_pending: int
    score: float
    throttled: bool

    def as_dict(self) -> dict[str, Any]:
        return {
            "tenant_id": self.tenant_id,
            "weight": self.weight,
            "priority_level": self.priority_level,
            "pending": self.pending,
            "processing": self.processing,
            "retry": self.retry,
            "oldest_pending_age_sec": self.oldest_pending_age_sec,
            "heavy_pending": self.heavy_pending,
            "light_pending": self.light_pending,
            "score": self.score,
            "throttled": self.throttled,
        }


class TenantFairnessService:
    _PLAN_WEIGHT_MAP = {
        "FREE": 1,
        "BASIC": 2,
        "PRO": 4,
        "OWNER": 8,
    }

    def __init__(self, repo) -> None:
        self._repo = repo

    def get_tenant_priority_level(self, tenant_id: int) -> int:
        if hasattr(self._repo, "get_active_subscription"):
            sub = self._repo.get_active_subscription(int(tenant_id))
            if isinstance(sub, dict):
                return max(1, int(sub.get("priority_level") or 1))
        return 1

    def get_tenant_weight(self, tenant_id: int) -> int:
        sub = self._repo.get_active_subscription(int(tenant_id)) if hasattr(self._repo, "get_active_subscription") else None
        plan_name = str((sub or {}).get("plan_name") or "").strip().upper()
        if plan_name in self._PLAN_WEIGHT_MAP:
            return self._PLAN_WEIGHT_MAP[plan_name]

        priority_level = max(1, int((sub or {}).get("priority_level") or 1))
        return min(max(priority_level, 1), 8)

    def should_throttle_tenant(
        self,
        *,
        tenant_id: int,
        system_mode: str = "normal",
        queue: str | None = None,
        tenant_pending: int | None = None,
        tenant_processing: int | None = None,
        tenant_retry: int | None = None,
        tenant_heavy_pending: int | None = None,
    ) -> bool:
        pending = int(tenant_pending if tenant_pending is not None else 0)
        processing = int(tenant_processing if tenant_processing is not None else 0)
        retry = int(tenant_retry if tenant_retry is not None else 0)
        heavy_pending = int(tenant_heavy_pending if tenant_heavy_pending is not None else 0)
        weight = self.get_tenant_weight(int(tenant_id))

        mode = str(system_mode or "normal").strip().lower()
        queue_name = str(queue or "").strip().lower()

        if mode == "normal":
            return False

        base_backlog_limit = 120 if mode == "degraded" else 80
        backlog_limit = max(30, int(base_backlog_limit * max(weight, 1)))
        heavy_limit = max(20, int(40 * max(weight, 1)))
        processing_limit = max(2, int(2 * max(weight, 1)))
        retry_limit = max(3, int(3 * max(weight, 1)))

        backlog_over = pending >= backlog_limit
        heavy_over = heavy_pending >= heavy_limit
        retry_storm = retry >= retry_limit
        processing_over = processing >= processing_limit
        severe_backlog = pending >= backlog_limit * 2 or heavy_pending >= heavy_limit * 2

        if queue_name == "light":
            return bool(backlog_over and processing_over and retry_storm)

        return bool(severe_backlog or ((heavy_over or backlog_over) and (retry_storm or processing_over)))

    def build_tenant_fairness_snapshot(self, queue: str | None = None, *, system_mode: str = "normal") -> list[dict[str, Any]]:
        pending_map = self._repo.get_tenant_job_counts(queue=queue) if hasattr(self._repo, "get_tenant_job_counts") else {}
        processing_map = self._repo.get_tenant_processing_counts(queue=queue) if hasattr(self._repo, "get_tenant_processing_counts") else {}
        retry_map = self._repo.get_tenant_retry_counts(queue=queue) if hasattr(self._repo, "get_tenant_retry_counts") else {}
        oldest_map = self._repo.get_tenant_oldest_pending_ages(queue=queue) if hasattr(self._repo, "get_tenant_oldest_pending_ages") else {}
        heavy_pending_map = self._repo.get_tenant_job_counts(queue="heavy") if hasattr(self._repo, "get_tenant_job_counts") else {}
        light_pending_map = self._repo.get_tenant_job_counts(queue="light") if hasattr(self._repo, "get_tenant_job_counts") else {}

        tenant_ids: set[int] = set()
        for source in (pending_map, processing_map, retry_map, oldest_map, heavy_pending_map, light_pending_map):
            tenant_ids.update(int(x) for x in source.keys())

        result: list[TenantFairnessSnapshot] = []
        for tenant_id in tenant_ids:
            pending = int(pending_map.get(tenant_id) or 0)
            processing = int(processing_map.get(tenant_id) or 0)
            retry = int(retry_map.get(tenant_id) or 0)
            oldest_age = int(oldest_map.get(tenant_id) or 0)
            heavy_pending = int(heavy_pending_map.get(tenant_id) or 0)
            light_pending = int(light_pending_map.get(tenant_id) or 0)
            weight = self.get_tenant_weight(tenant_id)
            priority_level = self.get_tenant_priority_level(tenant_id)
            score = self.compute_fairness_score(
                tenant_id=tenant_id,
                pending=pending,
                processing=processing,
                retry=retry,
                oldest_pending_age_sec=oldest_age,
            )
            throttled = self.should_throttle_tenant(
                tenant_id=tenant_id,
                system_mode=system_mode,
                queue=queue,
                tenant_pending=pending,
                tenant_processing=processing,
                tenant_retry=retry,
                tenant_heavy_pending=heavy_pending,
            )
            result.append(
                TenantFairnessSnapshot(
                    tenant_id=tenant_id,
                    weight=weight,
                    priority_level=priority_level,
                    pending=pending,
                    processing=processing,
                    retry=retry,
                    oldest_pending_age_sec=oldest_age,
                    heavy_pending=heavy_pending,
                    light_pending=light_pending,
                    score=score,
                    throttled=throttled,
                )
            )

        result.sort(key=lambda item: (item.throttled, -item.score, -item.pending, -item.oldest_pending_age_sec, item.tenant_id))
        return [item.as_dict() for item in result]

    def compute_fairness_score(
        self,
        *,
        tenant_id: int,
        pending: int,
        processing: int,
        retry: int,
        oldest_pending_age_sec: int,
    ) -> float:
        weight = self.get_tenant_weight(int(tenant_id))
        pending_score = min(float(max(pending, 0)), 1000.0)
        age_score = min(float(max(oldest_pending_age_sec, 0)) / 10.0, 200.0)
        weight_bonus = float(weight) * 8.0
        processing_penalty = float(max(processing, 0)) * 12.0
        retry_penalty = float(max(retry, 0)) * 4.0
        return pending_score + age_score + weight_bonus - processing_penalty - retry_penalty
