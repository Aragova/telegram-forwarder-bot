from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.repository import RepositoryProtocol
from app.worker_load_service import build_worker_load_snapshot
from app.worker_resource_policy import POLICY
from app.worker_runtime import get_worker_runtime_metrics_snapshot_sync
from app.tenant_fairness_service import TenantFairnessService


def update_heartbeat(repo: RepositoryProtocol, role: str) -> None:
    repo.update_runtime_heartbeat(role)


def get_system_health(repo: RepositoryProtocol) -> dict[str, Any]:
    rows = repo.get_runtime_heartbeats()
    now = datetime.now(timezone.utc)

    status = {
        "bot": "down",
        "scheduler": "down",
        "worker": "down",
    }

    for row in rows:
        role = str(row.get("role") or "")
        if role not in status:
            continue

        last_seen_at = row.get("last_seen_at")
        if not isinstance(last_seen_at, datetime):
            continue

        if last_seen_at.tzinfo is None:
            last_seen_at = last_seen_at.replace(tzinfo=timezone.utc)

        delta = (now - last_seen_at).total_seconds()
        if delta < 15:
            status[role] = "ok"

    queue = repo.get_queue_stats()
    errors = repo.count_recent_errors(minutes=5)
    job_counts = repo.get_job_status_counts() if hasattr(repo, "get_job_status_counts") else {}
    video_stage_counts = repo.get_video_stage_job_counts() if hasattr(repo, "get_video_stage_job_counts") else {}
    expired_count = len(repo.get_expired_leased_jobs()) if hasattr(repo, "get_expired_leased_jobs") else 0
    stuck_count = len(repo.get_stuck_processing_jobs(600)) if hasattr(repo, "get_stuck_processing_jobs") else 0
    load = build_worker_load_snapshot(repo, POLICY)
    throughput = get_worker_runtime_metrics_snapshot_sync()
    saas = repo.get_saas_health_snapshot() if hasattr(repo, "get_saas_health_snapshot") else {}
    usage_snapshot = repo.get_usage_for_date(1, datetime.now(timezone.utc).date().isoformat()) if hasattr(repo, "get_usage_for_date") else {}
    fairness_service = TenantFairnessService(repo)
    fairness_snapshot = fairness_service.build_tenant_fairness_snapshot(system_mode=load.mode)
    throttled_count = sum(1 for row in fairness_snapshot if bool(row.get("throttled")))

    return {
        "roles": status,
        "pending": int(queue.get("pending") or 0),
        "processing": int(queue.get("processing") or 0),
        "errors": int(errors),
        "jobs": {
            "pending": int(job_counts.get("pending") or 0),
            "leased": int(job_counts.get("leased") or 0),
            "processing": int(job_counts.get("processing") or 0),
            "retry": int(job_counts.get("retry") or 0),
            "failed": int(job_counts.get("failed") or 0),
            "expired_leased": int(expired_count),
            "stuck_processing": int(stuck_count),
        },
        "system_mode": load.mode,
        "load": {
            "backlog_light": load.light_pending,
            "backlog_heavy": load.heavy_pending,
            "processing_light": load.light_processing,
            "processing_heavy": load.heavy_processing,
            "oldest_pending_age_light_sec": load.oldest_pending_age_light_sec,
            "oldest_pending_age_heavy_sec": load.oldest_pending_age_heavy_sec,
            "active_light_slots": min(load.light_processing, POLICY.light_max_concurrency),
            "active_heavy_stage_slots": {
                "download": min(load.pending_video_download + load.heavy_processing, POLICY.heavy_download_max_concurrency),
                "process": min(load.pending_video_process + load.heavy_processing, POLICY.heavy_process_max_concurrency),
                "send": min(load.pending_video_send + load.heavy_processing, POLICY.heavy_send_max_concurrency),
            },
            "retry_storm_warning": load.retry_storm_warning,
        },
        "throughput": throughput,
        "tenant_fairness": {
            "tenant_fairness_mode": load.mode,
            "tenants_with_pending_jobs": len([row for row in fairness_snapshot if int(row.get("pending") or 0) > 0]),
            "top_tenants_by_backlog": sorted(
                [{"tenant_id": int(row.get("tenant_id") or 0), "pending": int(row.get("pending") or 0)} for row in fairness_snapshot],
                key=lambda item: (-item["pending"], item["tenant_id"]),
            )[:5],
            "top_tenants_by_oldest_pending": sorted(
                [{"tenant_id": int(row.get("tenant_id") or 0), "oldest_pending_age_sec": int(row.get("oldest_pending_age_sec") or 0)} for row in fairness_snapshot],
                key=lambda item: (-item["oldest_pending_age_sec"], item["tenant_id"]),
            )[:5],
            "top_tenants_by_processing": sorted(
                [{"tenant_id": int(row.get("tenant_id") or 0), "processing": int(row.get("processing") or 0)} for row in fairness_snapshot],
                key=lambda item: (-item["processing"], item["tenant_id"]),
            )[:5],
            "throttled_tenants_count": throttled_count,
        },
        "video_stages": video_stage_counts,
        "saas": {
            "tenants_active": int(saas.get("tenants_active") or 0),
            "tenants_blocked": int(saas.get("tenants_blocked") or 0),
            "tenants_over_limits": int(saas.get("tenants_over_limits") or 0),
            "subscriptions_active": int(saas.get("subscriptions_active") or 0),
            "subscriptions_in_grace": int(saas.get("subscriptions_in_grace") or 0),
            "subscriptions_expired": int(saas.get("subscriptions_expired") or 0),
            "invoices_open": int(saas.get("invoices_open") or 0),
            "invoices_draft": int(saas.get("invoices_draft") or 0),
            "invoices_open_exact": int(saas.get("invoices_open_exact") or 0),
            "invoices_paid": int(saas.get("invoices_paid") or 0),
            "billing_periods_due": int(saas.get("billing_periods_due") or 0),
            "billing_generation_errors": int(saas.get("billing_generation_errors") or 0),
            "tenants_with_overage_current_period": int(saas.get("tenants_with_overage_current_period") or 0),
            "tenants_with_billing_issues": int(saas.get("tenants_with_billing_issues") or 0),
            "tenants_with_overage_candidates": int(saas.get("tenants_with_overage_candidates") or 0),
            "usage_snapshot": usage_snapshot,
        },
    }
