from __future__ import annotations

import asyncio
import json
import logging

from .config import settings
from .tenant_reaction_executor import TenantReactionExecutor

logger = logging.getLogger("forwarder")


async def run_db(callable_obj, *args, **kwargs):
    return await asyncio.to_thread(callable_obj, *args, **kwargs)


class ReactionWorker:
    def __init__(self, *, db, worker_id: str, poll_interval_sec: float = 2.0, lock_timeout_seconds: int = 300):
        self.db = db
        self.worker_id = worker_id
        self.poll_interval_sec = poll_interval_sec
        self.lock_timeout_seconds = lock_timeout_seconds

    async def run_forever(self) -> None:
        logger.info("REACTION_WORKER_STARTED | worker_id=%s", self.worker_id)
        while True:
            try:
                processed = await self.process_once()
                if not processed:
                    await asyncio.sleep(self.poll_interval_sec)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("REACTION_JOB_PROCESS_FAILED | worker_id=%s", self.worker_id)
                await asyncio.sleep(self.poll_interval_sec)

    async def process_once(self) -> bool:
        job = await run_db(self.db.lease_due_reaction_job, worker_id=self.worker_id, lock_timeout_seconds=self.lock_timeout_seconds)
        if not job:
            return False

        job_id = int(job.get("id") or 0)
        tenant_id = int(job.get("tenant_id") or 0)
        rule_id = int(job.get("rule_id") or 0)

        logger.info("REACTION_JOB_LEASED | job_id=%s | tenant_id=%s | rule_id=%s | worker_id=%s | attempt=%s", job_id, tenant_id, rule_id, self.worker_id, job.get("attempt_count"))

        try:
            accounts = await run_db(self.db.list_reaction_accounts_for_tenant, tenant_id, True)
            account_ids_json = job.get("account_ids_json")
            if account_ids_json:
                allowed_ids = set(int(v) for v in (json.loads(account_ids_json) or []) if str(v).isdigit())
                accounts = [a for a in accounts if int(a.get("id") or 0) in allowed_ids]

            if not accounts:
                result = {"reason": "no_active_accounts_for_job", "attempted": 0, "confirmed": 0, "failed": 0, "skipped": 1}
                await run_db(self.db.mark_reaction_job_skipped, job_id=job_id, result=result)
                await run_db(self.db.log_reaction_event, tenant_id=tenant_id, rule_id=rule_id, reaction_job_id=job_id, event_type="reaction_job_skipped", status="skipped", extra=result)
                logger.info("REACTION_JOB_SKIPPED | job_id=%s | tenant_id=%s | rule_id=%s | reason=no_active_accounts_for_job", job_id, tenant_id, rule_id)
                return True

            executor = TenantReactionExecutor(api_id=settings.api_id, api_hash=settings.api_hash, base_dir=settings.base_dir)
            result = await executor.add_reactions(tenant_id=tenant_id, accounts=accounts, target_id=job.get("target_id"), message_id=int(job.get("message_id") or 0), rule_id=rule_id)

            confirmed = int(result.get("confirmed") or 0)
            attempted = int(result.get("attempted") or 0)

            if confirmed > 0:
                await run_db(self.db.mark_reaction_job_done, job_id=job_id, result=result)
                await run_db(self.db.log_reaction_event, tenant_id=tenant_id, rule_id=rule_id, reaction_job_id=job_id, event_type="reaction_job_done", status="done", extra=result)
                logger.info("REACTION_JOB_DONE | job_id=%s | tenant_id=%s | rule_id=%s", job_id, tenant_id, rule_id)
            elif attempted == 0:
                await run_db(self.db.mark_reaction_job_skipped, job_id=job_id, result=result)
                await run_db(self.db.log_reaction_event, tenant_id=tenant_id, rule_id=rule_id, reaction_job_id=job_id, event_type="reaction_job_skipped", status="skipped", extra=result)
                logger.info("REACTION_JOB_SKIPPED | job_id=%s | tenant_id=%s | rule_id=%s", job_id, tenant_id, rule_id)
            else:
                attempts = int(job.get("attempt_count") or 0)
                max_attempts = int(job.get("max_attempts") or 0)
                retry_after = 30 if attempts < max_attempts else None
                await run_db(self.db.mark_reaction_job_failed, job_id=job_id, error_text="tenant_reaction_not_confirmed", result=result, retry_after_seconds=retry_after)
                await run_db(self.db.log_reaction_event, tenant_id=tenant_id, rule_id=rule_id, reaction_job_id=job_id, event_type="reaction_job_failed", status="pending" if retry_after else "failed", error_text="tenant_reaction_not_confirmed", extra=result)
                if retry_after:
                    logger.info("REACTION_JOB_RETRY_SCHEDULED | job_id=%s | tenant_id=%s | rule_id=%s", job_id, tenant_id, rule_id)
                else:
                    logger.info("REACTION_JOB_FAILED | job_id=%s | tenant_id=%s | rule_id=%s", job_id, tenant_id, rule_id)
        except Exception as exc:
            logger.exception("REACTION_JOB_PROCESS_FAILED | job_id=%s | tenant_id=%s | rule_id=%s", job_id, tenant_id, rule_id)
            await run_db(self.db.mark_reaction_job_failed, job_id=job_id, error_text=str(exc), result={"error_type": exc.__class__.__name__}, retry_after_seconds=30)

        return True
