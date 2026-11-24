"""Reaction worker must stop on lease loss and avoid stale audit writes."""
import asyncio
from unittest.mock import Mock

from app.reaction_worker import ReactionWorker


def test_stale_reaction_completion_does_not_write_audit():
    db = Mock()
    db.lease_due_reaction_job.return_value = {"id": 1, "tenant_id": 2, "rule_id": 3}
    db.list_reaction_accounts_for_tenant.return_value = []
    db.mark_reaction_job_skipped.return_value = False
    worker = ReactionWorker(db=db, worker_id="worker")
    assert asyncio.run(worker.process_once())
    token = db.lease_due_reaction_job.call_args.kwargs["worker_id"]
    assert token.startswith("worker:")
    assert db.mark_reaction_job_skipped.call_args.kwargs["worker_id"] == token
    db.log_reaction_event.assert_not_called()


def test_lease_loss_cancels_reaction_processing(monkeypatch):
    async def scenario():
        db = Mock()
        db.lease_due_reaction_job.return_value = {"id": 1}
        db.heartbeat_reaction_job.return_value = False
        worker = ReactionWorker(db=db, worker_id="worker", lock_timeout_seconds=0.1)
        cancelled = asyncio.Event()

        async def process(*args):
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()

        monkeypatch.setattr(worker, "_process_leased", process)
        assert await asyncio.wait_for(worker.process_once(), timeout=3)
        assert cancelled.is_set()

    asyncio.run(scenario())
